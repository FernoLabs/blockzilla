use std::{
    fs::File,
    io::{self, Read},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
};

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

        let mut bytes = vec![0u8; length];
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
        Ok(bytes)
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
        self.chunk = self
            .source
            .read_range(self.object, self.position, length)
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
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn local_source_reads_exact_ranges_and_rejects_traversal() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join("object.bin"), b"0123456789").unwrap();
        let source = LocalRangeSource::new(directory.path());
        assert_eq!(source.size("object.bin").unwrap(), Some(10));
        assert_eq!(source.read_range("object.bin", 3, 4).unwrap(), b"3456");
        assert!(source.read_range("../object.bin", 0, 1).is_err());
        assert!(source.read_range("object.bin", 9, 2).is_err());
    }
}
