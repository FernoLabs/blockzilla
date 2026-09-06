//! Small Unix directory capability used for local custody state.
//!
//! Once opened, every child operation is relative to the held directory file
//! descriptor. Renaming or replacing the pathname used to obtain the handle
//! therefore cannot redirect a later read, publication, cleanup, or fsync.

use super::{Result, UploaderError};
use rustix::fs::{AtFlags, Dir, Mode, OFlags};
use rustix::io::FdFlags;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const MAX_ANCESTOR_DEPTH: usize = 1024;

pub(crate) struct DirectoryHandle {
    file: File,
    display_path: PathBuf,
    device: u64,
    inode: u64,
}

impl DirectoryHandle {
    pub(crate) fn open_existing(path: &Path, label: &str) -> Result<Self> {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let before = fs::symlink_metadata(&absolute)
            .map_err(|error| config(format!("cannot inspect {label}: {error}")))?;
        if before.file_type().is_symlink() || !before.is_dir() {
            return Err(config(format!(
                "{label} must be a directory, not a symlink"
            )));
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&absolute)
            .map_err(|error| config(format!("cannot open {label} safely: {error}")))?;
        let opened = file.metadata()?;
        if !opened.is_dir() || opened.dev() != before.dev() || opened.ino() != before.ino() {
            return Err(config(format!("{label} changed while opening")));
        }
        Ok(Self::from_file(file, absolute, opened))
    }

    pub(crate) fn open_or_create(path: &Path, label: &str) -> Result<Self> {
        fs::create_dir_all(path)
            .map_err(|error| config(format!("cannot create {label}: {error}")))?;
        Self::open_existing(path, label)
    }

    fn from_file(file: File, display_path: PathBuf, metadata: Metadata) -> Self {
        Self {
            file,
            display_path,
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }

    pub(crate) fn try_clone(&self) -> Result<Self> {
        let file = self.file.try_clone()?;
        let metadata = file.metadata()?;
        if !metadata.is_dir() || metadata.dev() != self.device || metadata.ino() != self.inode {
            return Err(protocol("directory capability changed while cloning"));
        }
        Ok(Self::from_file(file, self.display_path.clone(), metadata))
    }

    pub(crate) fn verify(&self) -> Result<()> {
        let metadata = self.file.metadata()?;
        if !metadata.is_dir() || metadata.dev() != self.device || metadata.ino() != self.inode {
            return Err(protocol("directory capability changed unexpectedly"));
        }
        Ok(())
    }

    pub(crate) fn verify_path_binding(&self, label: &str) -> Result<()> {
        self.verify()?;
        let current = fs::symlink_metadata(&self.display_path)
            .map_err(|error| protocol(format!("{label} path is unavailable: {error}")))?;
        if current.file_type().is_symlink()
            || !current.is_dir()
            || current.dev() != self.device
            || current.ino() != self.inode
        {
            return Err(protocol(format!("{label} path was replaced")));
        }
        Ok(())
    }

    pub(crate) fn identity(&self) -> (u64, u64) {
        (self.device, self.inode)
    }

    pub(crate) fn metadata(&self) -> Result<Metadata> {
        self.verify()?;
        Ok(self.file.metadata()?)
    }

    pub(crate) fn require_private_owner(&self, label: &str) -> Result<()> {
        let metadata = self.metadata()?;
        if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o022 != 0 {
            return Err(config(format!(
                "{label} must be owned by the effective user and not group/world writable"
            )));
        }
        Ok(())
    }

    pub(crate) fn contains_ancestor(&self, identity: (u64, u64)) -> Result<bool> {
        let mut current = self.open_directory_relative(OsStr::new("."))?;
        for _ in 0..MAX_ANCESTOR_DEPTH {
            let metadata = current.metadata()?;
            if (metadata.dev(), metadata.ino()) == identity {
                return Ok(true);
            }
            let parent = openat_file(
                &current,
                OsStr::new(".."),
                OFlags::RDONLY | OFlags::DIRECTORY,
                0,
            )?;
            let parent_metadata = parent.metadata()?;
            if parent_metadata.dev() == metadata.dev() && parent_metadata.ino() == metadata.ino() {
                return Ok(false);
            }
            current = parent;
        }
        Err(protocol("directory ancestry exceeds the safety limit"))
    }

    pub(crate) fn open_regular_optional(&self, name: &OsStr, label: &str) -> Result<Option<File>> {
        validate_child_name(name)?;
        let file = match openat_file(&self.file, name, OFlags::RDONLY | OFlags::NONBLOCK, 0) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(config(format!("cannot open {label} safely: {error}")));
            }
        };
        if !file.metadata()?.is_file() {
            return Err(config(format!(
                "{label} must be a regular file, not a symlink"
            )));
        }
        Ok(Some(file))
    }

    pub(crate) fn read_regular_optional(
        &self,
        name: &OsStr,
        maximum: usize,
        label: &str,
    ) -> Result<Option<Vec<u8>>> {
        let Some(file) = self.open_regular_optional(name, label)? else {
            return Ok(None);
        };
        read_stable_file_after_snapshot(file, maximum, label, |_| Ok(())).map(Some)
    }

    #[cfg(test)]
    fn read_regular_after_snapshot<F>(
        &self,
        name: &OsStr,
        maximum: usize,
        label: &str,
        after_snapshot: F,
    ) -> Result<Vec<u8>>
    where
        F: FnOnce(&Metadata) -> Result<()>,
    {
        let file = self
            .open_regular_optional(name, label)?
            .ok_or_else(|| config(format!("{label} is missing")))?;
        read_stable_file_after_snapshot(file, maximum, label, after_snapshot)
    }

    pub(crate) fn read_regular(
        &self,
        name: &OsStr,
        maximum: usize,
        label: &str,
    ) -> Result<Vec<u8>> {
        self.read_regular_optional(name, maximum, label)?
            .ok_or_else(|| config(format!("{label} is missing")))
    }

    pub(crate) fn open_lock(&self, name: &OsStr, mode: u32, label: &str) -> Result<File> {
        validate_child_name(name)?;
        let file = openat_file(
            &self.file,
            name,
            OFlags::RDWR | OFlags::CREATE | OFlags::NONBLOCK,
            mode,
        )
        .map_err(|error| config(format!("cannot open {label} safely: {error}")))?;
        if !file.metadata()?.is_file() {
            return Err(config(format!("{label} must be a regular file")));
        }
        Ok(file)
    }

    pub(crate) fn create_exclusive(&self, name: &OsStr, mode: u32) -> Result<File> {
        validate_child_name(name)?;
        Ok(openat_file(
            &self.file,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
            mode,
        )?)
    }

    pub(crate) fn require_same_inode(
        &self,
        name: &OsStr,
        expected: (u64, u64),
        label: &str,
    ) -> Result<()> {
        let current = self
            .open_regular_optional(name, label)?
            .ok_or_else(|| protocol(format!("{label} disappeared")))?
            .metadata()?;
        if (current.dev(), current.ino()) != expected {
            return Err(protocol(format!("{label} was replaced")));
        }
        Ok(())
    }

    pub(crate) fn rename_same_inode(
        &self,
        source: &OsStr,
        destination: &OsStr,
        expected: (u64, u64),
        label: &str,
    ) -> Result<()> {
        self.require_same_inode(source, expected, label)?;
        validate_child_name(source)?;
        validate_child_name(destination)?;
        rustix::fs::renameat(&self.file, source, &self.file, destination)
            .map_err(io::Error::from)?;
        self.require_same_inode(destination, expected, label)?;
        Ok(())
    }

    /// Atomically publish `source` at `destination` without replacement.
    /// Returns false when the destination already exists.
    pub(crate) fn link_same_inode_no_replace(
        &self,
        source: &OsStr,
        destination: &OsStr,
        expected: (u64, u64),
        label: &str,
    ) -> Result<bool> {
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
            Err(error) => Err(io::Error::from(error).into()),
        }
    }

    pub(crate) fn unlink(&self, name: &OsStr) -> Result<bool> {
        validate_child_name(name)?;
        match rustix::fs::unlinkat(&self.file, name, AtFlags::empty()) {
            Ok(()) => Ok(true),
            Err(error) if error == rustix::io::Errno::NOENT => Ok(false),
            Err(error) => Err(io::Error::from(error).into()),
        }
    }

    /// Remove a temporary name only when it still resolves to the exact file
    /// created by this operation. A collision or replacement is left intact.
    pub(crate) fn unlink_if_same_inode(
        &self,
        name: &OsStr,
        expected: (u64, u64),
        label: &str,
    ) -> Result<bool> {
        let Some(current) = self.open_regular_optional(name, label)? else {
            return Ok(false);
        };
        let current = current.metadata()?;
        if (current.dev(), current.ino()) != expected {
            return Err(protocol(format!("{label} changed before cleanup")));
        }
        self.unlink(name)
    }

    pub(crate) fn sync(&self) -> Result<()> {
        self.verify()?;
        self.file.sync_all()?;
        Ok(())
    }

    pub(crate) fn entry_names(&self, maximum: usize, label: &str) -> Result<Vec<OsString>> {
        if maximum == 0 {
            return Err(config(format!("{label} entry limit must be positive")));
        }
        self.verify()?;
        let mut stream = Dir::read_from(&self.file).map_err(io::Error::from)?;
        let stream_fd = stream.fd().map_err(io::Error::from)?;
        if !rustix::io::fcntl_getfd(stream_fd)
            .map_err(io::Error::from)?
            .contains(FdFlags::CLOEXEC)
        {
            return Err(protocol("directory stream is missing close-on-exec"));
        }
        let mut names = Vec::new();
        for entry in &mut stream {
            let entry = entry.map_err(io::Error::from)?;
            let bytes = entry.file_name().to_bytes();
            if matches!(bytes, b"." | b"..") {
                continue;
            }
            if names.len() >= maximum {
                return Err(config(format!("{label} contains too many entries")));
            }
            names.push(OsString::from_vec(bytes.to_vec()));
        }
        names.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        self.verify()?;
        Ok(names)
    }

    fn open_directory_relative(&self, name: &OsStr) -> Result<File> {
        let file = openat_file(&self.file, name, OFlags::RDONLY | OFlags::DIRECTORY, 0)?;
        if !file.metadata()?.is_dir() {
            return Err(protocol(
                "directory capability traversal returned a non-directory",
            ));
        }
        Ok(file)
    }
}

fn read_stable_file_after_snapshot<F>(
    mut file: File,
    maximum: usize,
    label: &str,
    after_snapshot: F,
) -> Result<Vec<u8>>
where
    F: FnOnce(&Metadata) -> Result<()>,
{
    let before = file.metadata()?;
    if before.len() == 0 || before.len() > maximum as u64 {
        return Err(config(format!("{label} has an invalid size")));
    }
    after_snapshot(&before)?;
    let mut bytes = Vec::with_capacity(before.len() as usize);
    Read::by_ref(&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    if bytes.len() != before.len() as usize || !same_file_identity(&before, &after) {
        return Err(config(format!("{label} changed while reading")));
    }
    Ok(bytes)
}

fn openat_file(directory: &File, name: &OsStr, flags: OFlags, mode: u32) -> io::Result<File> {
    let descriptor = rustix::fs::openat(
        directory,
        name,
        flags | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::from_raw_mode(mode as _),
    )
    .map_err(io::Error::from)?;
    Ok(File::from(descriptor))
}

fn validate_child_name(name: &OsStr) -> Result<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || matches!(bytes, b"." | b"..")
        || bytes.contains(&b'/')
        || bytes.contains(&0)
    {
        return Err(config("unsafe directory child name"));
    }
    Ok(())
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

fn config(message: impl Into<String>) -> UploaderError {
    UploaderError::Config(message.into())
}

fn protocol(message: impl Into<String>) -> UploaderError {
    UploaderError::Protocol(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustix::fs::{Timespec, Timestamps};
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn held_directory_cannot_be_redirected_by_path_replacement() {
        let temporary = tempfile::tempdir().unwrap();
        let requested = temporary.path().join("authority");
        let moved = temporary.path().join("authority-held");
        let replacement = temporary.path().join("replacement");
        fs::create_dir(&requested).unwrap();
        fs::create_dir(&replacement).unwrap();
        let directory = DirectoryHandle::open_existing(&requested, "authority").unwrap();

        fs::rename(&requested, &moved).unwrap();
        symlink(&replacement, &requested).unwrap();
        let mut file = directory
            .create_exclusive(OsStr::new("state.json"), 0o600)
            .unwrap();
        file.write_all(b"held\n").unwrap();
        file.sync_all().unwrap();
        directory.sync().unwrap();

        assert_eq!(fs::read(moved.join("state.json")).unwrap(), b"held\n");
        assert!(!replacement.join("state.json").exists());
        assert!(directory.verify_path_binding("authority").is_err());
    }

    #[test]
    fn child_symlinks_are_never_followed() {
        let temporary = tempfile::tempdir().unwrap();
        let directory = DirectoryHandle::open_existing(temporary.path(), "authority").unwrap();
        fs::write(temporary.path().join("target"), b"secret").unwrap();
        symlink("target", temporary.path().join("link")).unwrap();
        assert!(
            directory
                .read_regular(OsStr::new("link"), 1024, "state")
                .is_err()
        );
        assert!(
            directory
                .create_exclusive(OsStr::new("link"), 0o600)
                .is_err()
        );
    }

    #[test]
    fn stable_reader_detects_same_length_mutation_with_restored_mtime() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("state");
        fs::write(&path, b"original").unwrap();
        let directory = DirectoryHandle::open_existing(temporary.path(), "authority").unwrap();
        let result =
            directory.read_regular_after_snapshot(OsStr::new("state"), 64, "state", |before| {
                thread::sleep(Duration::from_millis(5));
                let mut writer = OpenOptions::new().write(true).open(&path)?;
                writer.write_all(b"mutated!")?;
                writer.flush()?;
                rustix::fs::futimens(
                    &writer,
                    &Timestamps {
                        last_access: Timespec {
                            tv_sec: before.atime(),
                            tv_nsec: before.atime_nsec() as _,
                        },
                        last_modification: Timespec {
                            tv_sec: before.mtime(),
                            tv_nsec: before.mtime_nsec() as _,
                        },
                    },
                )
                .map_err(io::Error::from)?;
                Ok(())
            });
        assert!(result.is_err());
    }

    #[test]
    fn directory_stream_is_cloexec_bounded_and_private_owner_gated() {
        let temporary = tempfile::tempdir().unwrap();
        fs::write(temporary.path().join("entry"), b"x").unwrap();
        let directory = DirectoryHandle::open_existing(temporary.path(), "authority").unwrap();
        assert_eq!(directory.entry_names(1, "authority").unwrap().len(), 1);
        assert!(directory.entry_names(0, "authority").is_err());
        directory.require_private_owner("authority").unwrap();

        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o770)).unwrap();
        assert!(directory.require_private_owner("authority").is_err());
    }

    #[test]
    fn replaced_temporary_inode_is_never_published_or_cleaned_up() {
        let temporary = tempfile::tempdir().unwrap();
        let directory = DirectoryHandle::open_existing(temporary.path(), "authority").unwrap();
        let mut file = directory
            .create_exclusive(OsStr::new("temporary"), 0o600)
            .unwrap();
        file.write_all(b"intended").unwrap();
        file.sync_all().unwrap();
        let metadata = file.metadata().unwrap();
        let identity = (metadata.dev(), metadata.ino());

        fs::rename(
            temporary.path().join("temporary"),
            temporary.path().join("stolen"),
        )
        .unwrap();
        fs::write(temporary.path().join("temporary"), b"replacement").unwrap();
        assert!(
            directory
                .rename_same_inode(
                    OsStr::new("temporary"),
                    OsStr::new("published"),
                    identity,
                    "temporary",
                )
                .is_err()
        );
        assert!(!temporary.path().join("published").exists());
        assert_eq!(
            fs::read(temporary.path().join("temporary")).unwrap(),
            b"replacement"
        );
        assert!(
            directory
                .unlink_if_same_inode(OsStr::new("temporary"), identity, "temporary")
                .is_err()
        );
        assert!(temporary.path().join("temporary").exists());
    }
}
