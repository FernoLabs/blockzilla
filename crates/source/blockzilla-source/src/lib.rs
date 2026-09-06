//! Shared byte-range contract and object-name validation.

use std::sync::Arc;
use thiserror::Error;

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

    /// Read an exact range into an already initialized destination slice.
    ///
    /// The default keeps custom sources compatible, but it allocates one
    /// temporary response. Local, cached, and HTTP sources override this method
    /// so callers can fill their final storage directly.
    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        destination: &mut [u8],
    ) -> SourceResult<()> {
        let expected = destination.len();
        let bytes = self.read_range(object, offset, expected)?;
        if bytes.len() != expected {
            return Err(SourceError::ShortRead {
                object: object.to_owned(),
                expected,
                actual: bytes.len(),
            });
        }
        destination.copy_from_slice(&bytes);
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

    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        destination: &mut [u8],
    ) -> SourceResult<()> {
        (**self).read_range_into_slice(object, offset, destination)
    }
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("invalid object name: {0}")]
    InvalidName(String),

    #[error("I/O error for {object}: {source}")]
    Io {
        object: String,
        #[source]
        source: std::io::Error,
    },

    #[error("object {0} does not exist")]
    NotFound(String),

    #[error("short range read for {object}: got {actual} bytes, expected {expected}")]
    ShortRead {
        object: String,
        expected: usize,
        actual: usize,
    },

    #[error(
        "range for {object} is outside the object: offset={offset}, length={length}, size={size}"
    )]
    OutOfBounds {
        object: String,
        offset: u64,
        length: usize,
        size: u64,
    },

    #[error("remote source protocol error: {0}")]
    Protocol(String),
}

pub fn validate_object_name(name: &str) -> std::result::Result<(), &'static str> {
    if name.is_empty() {
        return Err("file name is empty");
    }
    if name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err("file name must be one safe path component");
    }
    if name
        .bytes()
        .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err("file name contains a control character");
    }
    Ok(())
}
