//! Shared exclusion for Archive V2 control-file publication.
//!
//! The gateway manifest publisher and every in-place control-file transition
//! must use this lock. Keeping one SDK-owned pathname and inode check prevents
//! two tools from using similar-looking but independent lock protocols.

use std::{
    os::fd::OwnedFd,
    path::{Path, PathBuf},
};

use rustix::fs::{FileType, FlockOperation, Mode, OFlags};

use crate::{Error, Result};

pub const ARCHIVE_V2_PUBLICATION_LOCK_FILE: &str = ".archive-v2-manifest-publish.lock";

/// Exclusive guard for Archive V2 manifest and wire-profile control files.
///
/// Dropping this value releases the operating-system lock.
#[derive(Debug)]
pub struct ArchiveV2PublicationLock {
    root: PathBuf,
    path: PathBuf,
    file: OwnedFd,
    device: u128,
    inode: u128,
}

impl ArchiveV2PublicationLock {
    /// Canonical archive directory protected by this guard.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Prove that the held descriptor and the lock pathname still identify
    /// the same regular file.
    pub fn recheck(&self) -> Result<()> {
        let opened = rustix::fs::fstat(&self.file).map_err(lock_io)?;
        let current = rustix::fs::lstat(&self.path).map_err(lock_io)?;
        if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile
            || FileType::from_raw_mode(current.st_mode) != FileType::RegularFile
            || identity_component(opened.st_dev) != Some(self.device)
            || identity_component(opened.st_ino) != Some(self.inode)
            || identity_component(current.st_dev) != Some(self.device)
            || identity_component(current.st_ino) != Some(self.inode)
        {
            return Err(Error::PublicationLock(
                "publication lock path changed while the lock was held".into(),
            ));
        }
        Ok(())
    }
}

/// Acquire the one shared Archive V2 control-file publication lock.
pub fn acquire_archive_v2_publication_lock(root: &Path) -> Result<ArchiveV2PublicationLock> {
    let metadata = std::fs::symlink_metadata(root).map_err(lock_std_io)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::PublicationLock(
            "archive root must be a real directory".into(),
        ));
    }
    let canonical_root = root.canonicalize().map_err(lock_std_io)?;
    if canonical_root != root {
        return Err(Error::PublicationLock(
            "archive root must already be canonical".into(),
        ));
    }

    let path = canonical_root.join(ARCHIVE_V2_PUBLICATION_LOCK_FILE);
    let file = rustix::fs::open(
        &path,
        OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::from_raw_mode(0o600),
    )
    .map_err(lock_io)?;
    let opened = rustix::fs::fstat(&file).map_err(lock_io)?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile {
        return Err(Error::PublicationLock(
            "publication lock is not a regular file".into(),
        ));
    }
    rustix::fs::flock(&file, FlockOperation::LockExclusive).map_err(lock_io)?;

    let guard = ArchiveV2PublicationLock {
        root: canonical_root,
        path,
        file,
        device: identity_component(opened.st_dev)
            .ok_or_else(|| Error::PublicationLock("publication lock device is invalid".into()))?,
        inode: identity_component(opened.st_ino)
            .ok_or_else(|| Error::PublicationLock("publication lock inode is invalid".into()))?,
    };
    guard.recheck()?;
    Ok(guard)
}

fn lock_io(error: rustix::io::Errno) -> Error {
    Error::PublicationLock(std::io::Error::from(error).to_string())
}

fn lock_std_io(error: std::io::Error) -> Error {
    Error::PublicationLock(error.to_string())
}

fn identity_component<T: TryInto<u128>>(value: T) -> Option<u128> {
    value.try_into().ok()
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, thread};

    use super::*;

    #[test]
    fn guard_is_shared_and_rechecks_its_inode() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let guard = acquire_archive_v2_publication_lock(&root).unwrap();
        guard.recheck().unwrap();

        let replacement = root.join("replacement.lock");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&replacement)
            .unwrap();
        std::fs::rename(&replacement, &guard.path).unwrap();
        assert!(guard.recheck().is_err());
    }

    #[test]
    fn second_caller_waits_for_the_same_lock() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let first = acquire_archive_v2_publication_lock(&root).unwrap();
        let second_root = root.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let join = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let second = acquire_archive_v2_publication_lock(&second_root).unwrap();
            locked_tx.send(()).unwrap();
            second
        });
        started_rx.recv().unwrap();
        assert!(
            locked_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err()
        );
        drop(first);
        let second = join.join().unwrap();
        second.recheck().unwrap();
    }
}
