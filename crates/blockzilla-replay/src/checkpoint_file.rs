//! Crash-safe files for deterministic launch replay checkpoints.
//!
//! The checksum embedded by the codec detects accidental corruption. Public
//! resume additionally requires a caller-supplied SHA-256 of the complete file;
//! that value must come from trusted metadata outside the checkpoint itself.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    LaunchReplay,
    checkpoint::{FrozenCheckpointMetadata, MAX_CHECKPOINT_BYTES},
};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error)]
pub(crate) enum LaunchCheckpointFileError {
    #[error("checkpoint path {path} has no file name")]
    InvalidPath { path: PathBuf },
    #[error("checkpoint file {path} is not a regular file")]
    NotRegularFile { path: PathBuf },
    #[error("checkpoint file {path} exceeds the {MAX_CHECKPOINT_BYTES}-byte bound")]
    TooLarge { path: PathBuf },
    #[error("checkpoint file SHA-256 mismatch for {path}: expected {expected}, found {found}")]
    DigestMismatch {
        path: PathBuf,
        expected: String,
        found: String,
    },
    #[error("{operation} checkpoint path {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("decode frozen checkpoint {path}: {message}")]
    Decode { path: PathBuf, message: String },
    #[error("could not reserve a unique temporary checkpoint file beside {path}")]
    TemporaryNameExhausted { path: PathBuf },
}

pub(crate) struct TrustedFrozenCheckpoint {
    pub(crate) replay: LaunchReplay,
    pub(crate) metadata: FrozenCheckpointMetadata,
    pub(crate) file_sha256: [u8; 32],
}

pub(crate) fn read_trusted_frozen_checkpoint(
    path: &Path,
    expected_file_sha256: [u8; 32],
) -> Result<TrustedFrozenCheckpoint, LaunchCheckpointFileError> {
    let mut file = File::open(path).map_err(|source| io_error("open", path, source))?;
    let metadata = file
        .metadata()
        .map_err(|source| io_error("inspect", path, source))?;
    if !metadata.is_file() {
        return Err(LaunchCheckpointFileError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    if metadata.len() > MAX_CHECKPOINT_BYTES {
        return Err(LaunchCheckpointFileError::TooLarge {
            path: path.to_path_buf(),
        });
    }
    let capacity =
        usize::try_from(metadata.len()).map_err(|_| LaunchCheckpointFileError::TooLarge {
            path: path.to_path_buf(),
        })?;
    let mut bytes = Vec::with_capacity(capacity);
    Read::by_ref(&mut file)
        .take(MAX_CHECKPOINT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| io_error("read", path, source))?;
    if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
        return Err(LaunchCheckpointFileError::TooLarge {
            path: path.to_path_buf(),
        });
    }

    let file_sha256: [u8; 32] = Sha256::digest(&bytes).into();
    if file_sha256 != expected_file_sha256 {
        return Err(LaunchCheckpointFileError::DigestMismatch {
            path: path.to_path_buf(),
            expected: hex(&expected_file_sha256),
            found: hex(&file_sha256),
        });
    }
    let (replay, metadata) =
        LaunchReplay::restore_frozen_checkpoint(&bytes, false).map_err(|error| {
            LaunchCheckpointFileError::Decode {
                path: path.to_path_buf(),
                message: error.to_string(),
            }
        })?;
    Ok(TrustedFrozenCheckpoint {
        replay,
        metadata,
        file_sha256,
    })
}

/// Durably publish a complete checkpoint without exposing a partial target.
///
/// The temporary file is created in the target directory, flushed, renamed,
/// and followed by a directory flush on Unix. Therefore old-or-new contents
/// survive a crash on filesystems that honor the usual fsync/rename contract.
pub(crate) fn publish_frozen_checkpoint(
    path: &Path,
    bytes: &[u8],
) -> Result<[u8; 32], LaunchCheckpointFileError> {
    if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
        return Err(LaunchCheckpointFileError::TooLarge {
            path: path.to_path_buf(),
        });
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| LaunchCheckpointFileError::InvalidPath {
            path: path.to_path_buf(),
        })?;
    let mut temporary = None;
    for _ in 0..128 {
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".{file_name}.blockzilla-checkpoint.tmp.{}.{}",
            std::process::id(),
            sequence
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(source) => return Err(io_error("create temporary", &candidate, source)),
        }
    }
    let Some((temporary_path, mut temporary_file)) = temporary else {
        return Err(LaunchCheckpointFileError::TemporaryNameExhausted {
            path: path.to_path_buf(),
        });
    };

    let publish = (|| {
        temporary_file
            .write_all(bytes)
            .map_err(|source| io_error("write temporary", &temporary_path, source))?;
        temporary_file
            .sync_all()
            .map_err(|source| io_error("flush temporary", &temporary_path, source))?;
        drop(temporary_file);
        fs::rename(&temporary_path, path)
            .map_err(|source| io_error("rename temporary", path, source))?;
        sync_parent_directory(parent)?;
        Ok::<(), LaunchCheckpointFileError>(())
    })();
    if publish.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    publish?;
    Ok(Sha256::digest(bytes).into())
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> Result<(), LaunchCheckpointFileError> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("flush parent directory", parent, source))
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> Result<(), LaunchCheckpointFileError> {
    Ok(())
}

fn io_error(
    operation: &'static str,
    path: &Path,
    source: std::io::Error,
) -> LaunchCheckpointFileError {
    LaunchCheckpointFileError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary_directory(label: &str) -> PathBuf {
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "blockzilla-checkpoint-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn atomic_publication_replaces_only_with_complete_bytes_and_cleans_temporary_file() {
        let directory = temporary_directory("atomic");
        let checkpoint = directory.join("epoch.chk");
        let old = b"old-complete-checkpoint";
        let new = b"new-complete-checkpoint-with-more-bytes";

        assert_eq!(
            publish_frozen_checkpoint(&checkpoint, old).unwrap(),
            <[u8; 32]>::from(Sha256::digest(old))
        );
        assert_eq!(fs::read(&checkpoint).unwrap(), old);
        assert_eq!(
            publish_frozen_checkpoint(&checkpoint, new).unwrap(),
            <[u8; 32]>::from(Sha256::digest(new))
        );
        assert_eq!(fs::read(&checkpoint).unwrap(), new);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn trusted_read_rejects_wrong_whole_file_digest_before_codec_decode() {
        let directory = temporary_directory("digest");
        let checkpoint = directory.join("corrupt.chk");
        let corrupt = b"not-a-valid-frozen-checkpoint";
        let actual = publish_frozen_checkpoint(&checkpoint, corrupt).unwrap();

        assert!(matches!(
            read_trusted_frozen_checkpoint(&checkpoint, [0; 32]),
            Err(LaunchCheckpointFileError::DigestMismatch { .. })
        ));
        assert!(matches!(
            read_trusted_frozen_checkpoint(&checkpoint, actual),
            Err(LaunchCheckpointFileError::Decode { .. })
        ));

        fs::remove_dir_all(directory).unwrap();
    }
}
