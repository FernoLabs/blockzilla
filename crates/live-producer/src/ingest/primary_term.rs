//! Crash-safe primary fencing terms.
//!
//! A primary must acquire this store once when it starts and retain the returned guard for its
//! entire lifetime. Acquisition holds an exclusive lock on a stable sidecar file, advances the
//! persisted term, and returns only after the replacement state and its parent directory have
//! been synced. The state file itself is never locked because it is atomically replaced.

use anyhow::{Context, Result, ensure};
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

const TERM_STATE_MAGIC: &[u8; 8] = b"BZTERM01";
const TERM_STATE_VERSION: u16 = 1;
const TERM_STATE_RESERVED: u16 = 0;
const TERM_STATE_PREFIX_LEN: usize = 8 + 2 + 2 + 8;
const TERM_STATE_LEN: usize = TERM_STATE_PREFIX_LEN + 4;
const LOCK_SUFFIX: &str = ".lock";
const TEMP_SUFFIX: &str = ".tmp";

/// Persistent source of monotonically increasing primary fencing terms.
#[derive(Clone, Debug)]
pub struct PrimaryTermStore {
    state_path: PathBuf,
}

impl PrimaryTermStore {
    pub fn new(state_path: impl Into<PathBuf>) -> Self {
        Self {
            state_path: state_path.into(),
        }
    }

    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    /// Lock this primary identity and durably advance its fencing term.
    ///
    /// The returned guard owns the sidecar lock. Dropping it releases the primary identity so a
    /// later process can acquire a strictly larger term. A leftover temporary file is never
    /// guessed at or automatically removed: it requires explicit operator inspection.
    pub fn acquire(&self) -> Result<PrimaryTermGuard> {
        let parent = durable_parent(&self.state_path);
        ensure_secure_parent(&parent)?;

        let lock_path = sidecar_path(&self.state_path, LOCK_SUFFIX)?;
        let temp_path = sidecar_path(&self.state_path, TEMP_SUFFIX)?;
        let lock_file = open_and_lock_sidecar(&lock_path, &parent)?;

        ensure_path_absent(&temp_path).with_context(|| {
            format!(
                "refusing to advance primary term while temporary state exists: {}",
                temp_path.display()
            )
        })?;

        let current = read_current_term(&self.state_path)?;
        let next = current
            .checked_add(1)
            .context("primary fencing term overflow")?;
        let next = NonZeroU64::new(next).context("primary fencing term must be nonzero")?;

        persist_term(&self.state_path, &temp_path, &parent, next)?;

        Ok(PrimaryTermGuard {
            term: next,
            _lock_file: lock_file,
        })
    }
}

/// Proof that this process exclusively owns a freshly persisted primary fencing term.
///
/// Keep this value alive for as long as the associated primary endpoint is serving requests.
#[must_use = "dropping the guard releases the primary fencing lock"]
pub struct PrimaryTermGuard {
    term: NonZeroU64,
    _lock_file: File,
}

impl PrimaryTermGuard {
    pub fn term(&self) -> NonZeroU64 {
        self.term
    }
}

impl fmt::Debug for PrimaryTermGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrimaryTermGuard")
            .field("term", &self.term)
            .field("lock", &"held")
            .finish()
    }
}

fn open_and_lock_sidecar(path: &Path, parent: &Path) -> Result<File> {
    let (file, created) = open_secure_read_write(path, true)
        .with_context(|| format!("open primary term lock sidecar {}", path.display()))?;
    try_lock_exclusive(&file, path)?;

    if created {
        set_private_mode(&file, path)?;
    }
    validate_secure_regular_file(&file, path)?;
    verify_open_file_is_path(&file, path)?;

    // Sync unconditionally. If a creator crashed between link creation and parent fsync, a later
    // owner repairs that durability gap before it relies on the stable lock inode.
    file.sync_all()
        .with_context(|| format!("sync primary term lock sidecar {}", path.display()))?;
    sync_directory(parent)?;
    Ok(file)
}

fn read_current_term(path: &Path) -> Result<u64> {
    let mut file = match open_secure_read_only(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(0),
        Err(err) => {
            return Err(err).with_context(|| format!("open primary term state {}", path.display()));
        }
    };
    validate_secure_regular_file(&file, path)?;

    let length = file
        .metadata()
        .with_context(|| format!("inspect primary term state {}", path.display()))?
        .len();
    ensure!(
        length == TERM_STATE_LEN as u64,
        "invalid primary term state length in {}",
        path.display()
    );

    let mut encoded = [0u8; TERM_STATE_LEN];
    file.read_exact(&mut encoded)
        .with_context(|| format!("read primary term state {}", path.display()))?;
    decode_term_state(&encoded)
        .with_context(|| format!("validate primary term state {}", path.display()))
}

fn persist_term(
    state_path: &Path,
    temp_path: &Path,
    parent: &Path,
    term: NonZeroU64,
) -> Result<()> {
    // `ensure_path_absent` was already called while holding the lock. `create_new` closes the
    // remaining race and also refuses symlinks. An error after creation intentionally leaves the
    // temporary file in place so the next acquisition fails closed instead of guessing how far a
    // failed write progressed.
    let (mut temp, created) = open_secure_read_write(temp_path, true).with_context(|| {
        format!(
            "create primary term temporary state {}",
            temp_path.display()
        )
    })?;
    ensure!(created, "primary term temporary state unexpectedly existed");
    set_private_mode(&temp, temp_path)?;
    validate_secure_regular_file(&temp, temp_path)?;

    temp.write_all(&encode_term_state(term))
        .with_context(|| format!("write primary term temporary state {}", temp_path.display()))?;
    temp.sync_all()
        .with_context(|| format!("sync primary term temporary state {}", temp_path.display()))?;
    drop(temp);

    fs::rename(temp_path, state_path).with_context(|| {
        format!(
            "atomically install primary term state {}",
            state_path.display()
        )
    })?;
    sync_directory(parent)
}

fn encode_term_state(term: NonZeroU64) -> [u8; TERM_STATE_LEN] {
    let mut encoded = [0u8; TERM_STATE_LEN];
    encoded[..8].copy_from_slice(TERM_STATE_MAGIC);
    encoded[8..10].copy_from_slice(&TERM_STATE_VERSION.to_le_bytes());
    encoded[10..12].copy_from_slice(&TERM_STATE_RESERVED.to_le_bytes());
    encoded[12..20].copy_from_slice(&term.get().to_le_bytes());
    let mut checksum = Crc32c::new();
    checksum.update(&encoded[..TERM_STATE_PREFIX_LEN]);
    encoded[TERM_STATE_PREFIX_LEN..].copy_from_slice(&checksum.finish().to_le_bytes());
    encoded
}

fn decode_term_state(encoded: &[u8; TERM_STATE_LEN]) -> Result<u64> {
    ensure!(
        &encoded[..8] == TERM_STATE_MAGIC,
        "invalid primary term state magic"
    );
    ensure!(
        u16::from_le_bytes(encoded[8..10].try_into().expect("fixed version slice"))
            == TERM_STATE_VERSION,
        "unsupported primary term state version"
    );
    ensure!(
        u16::from_le_bytes(encoded[10..12].try_into().expect("fixed reserved slice"))
            == TERM_STATE_RESERVED,
        "invalid primary term state reserved bits"
    );

    let mut checksum = Crc32c::new();
    checksum.update(&encoded[..TERM_STATE_PREFIX_LEN]);
    let stored_checksum = u32::from_le_bytes(
        encoded[TERM_STATE_PREFIX_LEN..]
            .try_into()
            .expect("fixed checksum slice"),
    );
    ensure!(
        checksum.finish() == stored_checksum,
        "primary term state checksum mismatch"
    );

    let term = u64::from_le_bytes(encoded[12..20].try_into().expect("fixed term slice"));
    ensure!(term != 0, "primary term state contains zero");
    Ok(term)
}

fn sidecar_path(state_path: &Path, suffix: &str) -> Result<PathBuf> {
    let file_name = state_path.file_name().with_context(|| {
        format!(
            "primary term state path has no file name: {}",
            state_path.display()
        )
    })?;
    let mut sidecar_name = OsString::from(file_name);
    sidecar_name.push(suffix);
    Ok(state_path.with_file_name(sidecar_name))
}

fn durable_parent(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

fn ensure_secure_parent(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect primary term parent directory {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "primary term parent is not a real directory: {}",
        path.display()
    );
    Ok(())
}

fn ensure_path_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("inspect sidecar path {}", path.display())),
        Ok(_) => anyhow::bail!("sidecar path already exists: {}", path.display()),
    }
}

fn open_secure_read_write(path: &Path, create_if_missing: bool) -> io::Result<(File, bool)> {
    if create_if_missing {
        match open_read_write_descriptor(path, true) {
            Ok(file) => return Ok((file, true)),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
    }
    open_read_write_descriptor(path, false).map(|file| (file, false))
}

fn open_read_write_descriptor(path: &Path, create_new: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create_new {
        options.create_new(true);
    }
    #[cfg(unix)]
    {
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options.open(path)
}

fn open_secure_read_only(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options.open(path)
}

fn validate_secure_regular_file(file: &File, path: &Path) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect secure file {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "secure file is not a regular file: {}",
        path.display()
    );
    #[cfg(unix)]
    ensure!(
        metadata.permissions().mode() & 0o7777 == 0o600,
        "secure file permissions must be 0600: {}",
        path.display()
    );
    Ok(())
}

#[cfg(unix)]
fn set_private_mode(file: &File, path: &Path) -> Result<()> {
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .with_context(|| format!("set secure file permissions {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_mode(_file: &File, _path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn verify_open_file_is_path(file: &File, path: &Path) -> Result<()> {
    let opened = file
        .metadata()
        .with_context(|| format!("inspect locked sidecar {}", path.display()))?;
    let linked = fs::symlink_metadata(path)
        .with_context(|| format!("inspect linked lock sidecar {}", path.display()))?;
    ensure!(
        linked.file_type().is_file()
            && opened.dev() == linked.dev()
            && opened.ino() == linked.ino(),
        "primary term lock sidecar changed during acquisition: {}",
        path.display()
    );
    Ok(())
}

#[cfg(not(unix))]
fn verify_open_file_is_path(_file: &File, path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect linked lock sidecar {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "primary term lock sidecar is not a regular file: {}",
        path.display()
    );
    Ok(())
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File, path: &Path) -> Result<()> {
    // SAFETY: the descriptor remains owned by `PrimaryTermGuard` for the entire lock lifetime.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
            .with_context(|| format!("lock primary term sidecar {}", path.display()))
    }
}

#[cfg(not(unix))]
fn try_lock_exclusive(file: &File, path: &Path) -> Result<()> {
    file.try_lock()
        .with_context(|| format!("lock primary term sidecar {}", path.display()))
}

fn sync_directory(path: &Path) -> Result<()> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let directory = options
        .open(path)
        .with_context(|| format!("open primary term directory for sync {}", path.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("sync primary term directory {}", path.display()))
}

#[derive(Clone, Copy)]
struct Crc32c(u32);

impl Crc32c {
    fn new() -> Self {
        Self(!0)
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = (self.0 & 1).wrapping_neg();
                self.0 = (self.0 >> 1) ^ (0x82f6_3b78 & mask);
            }
        }
    }

    fn finish(self) -> u32 {
        !self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(unix)]
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let sequence = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "blockzilla-primary-term-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path).expect("create test directory");
            Self(path)
        }

        fn join(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_state(path: &Path, term: u64) {
        let term = NonZeroU64::new(term).expect("nonzero test term");
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(path).expect("open test state");
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .expect("set test state mode");
        file.write_all(&encode_term_state(term))
            .expect("write test state");
        file.sync_all().expect("sync test state");
    }

    #[test]
    fn acquisition_is_exclusive_and_monotonic() {
        let directory = TestDirectory::new();
        let state_path = directory.join("primary.term");
        let store = PrimaryTermStore::new(&state_path);

        let first = store.acquire().expect("acquire first term");
        assert_eq!(first.term().get(), 1);
        assert!(store.acquire().is_err(), "sidecar lock must remain held");
        drop(first);

        let second = store.acquire().expect("acquire second term");
        assert_eq!(second.term().get(), 2);
        assert!(!sidecar_path(&state_path, TEMP_SUFFIX).unwrap().exists());
    }

    #[cfg(unix)]
    #[test]
    fn state_and_stable_lock_are_private_regular_files() {
        let directory = TestDirectory::new();
        let state_path = directory.join("primary.term");
        let lock_path = sidecar_path(&state_path, LOCK_SUFFIX).unwrap();
        let store = PrimaryTermStore::new(&state_path);

        let first = store.acquire().expect("acquire first term");
        let initial_lock = fs::metadata(&lock_path).expect("initial lock metadata");
        assert_eq!(initial_lock.permissions().mode() & 0o7777, 0o600);
        assert_eq!(
            fs::metadata(&state_path)
                .expect("state metadata")
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        drop(first);

        drop(store.acquire().expect("acquire next term"));
        let later_lock = fs::metadata(&lock_path).expect("later lock metadata");
        assert_eq!(initial_lock.dev(), later_lock.dev());
        assert_eq!(initial_lock.ino(), later_lock.ino());
    }

    #[test]
    fn checksum_corruption_fails_closed_without_advancing() {
        let directory = TestDirectory::new();
        let state_path = directory.join("primary.term");
        let store = PrimaryTermStore::new(&state_path);
        drop(store.acquire().expect("seed state"));

        let mut bytes = fs::read(&state_path).expect("read state");
        bytes[12] ^= 0x80;
        fs::write(&state_path, bytes).expect("corrupt state");

        assert!(store.acquire().is_err());
        assert!(store.acquire().is_err());
    }

    #[test]
    fn term_overflow_fails_closed() {
        let directory = TestDirectory::new();
        let state_path = directory.join("primary.term");
        let store = PrimaryTermStore::new(&state_path);
        drop(store.acquire().expect("create stable lock"));
        write_state(&state_path, u64::MAX);

        assert!(store.acquire().is_err());
        assert_eq!(read_current_term(&state_path).unwrap(), u64::MAX);
    }

    #[test]
    fn pre_rename_crash_temp_fails_closed_until_explicit_recovery() {
        let directory = TestDirectory::new();
        let state_path = directory.join("primary.term");
        let temp_path = sidecar_path(&state_path, TEMP_SUFFIX).unwrap();
        let store = PrimaryTermStore::new(&state_path);
        drop(store.acquire().expect("seed state"));
        let original = fs::read(&state_path).expect("read original state");

        write_state(&temp_path, 2);
        assert!(store.acquire().is_err());
        assert_eq!(fs::read(&state_path).unwrap(), original);

        fs::remove_file(&temp_path).expect("operator removes inspected temp");
        let recovered = store.acquire().expect("acquire after explicit recovery");
        assert_eq!(recovered.term().get(), 2);
    }

    #[test]
    fn post_rename_crash_never_reuses_installed_term() {
        let directory = TestDirectory::new();
        let state_path = directory.join("primary.term");
        let temp_path = sidecar_path(&state_path, TEMP_SUFFIX).unwrap();
        let store = PrimaryTermStore::new(&state_path);
        drop(store.acquire().expect("seed state"));

        // Simulate a process that completed its atomic rename and then crashed before returning a
        // guard. Skipping a term is safe; reusing it would not be.
        write_state(&temp_path, 2);
        fs::rename(&temp_path, &state_path).expect("simulate completed rename");

        let recovered = store.acquire().expect("recover post-rename state");
        assert_eq!(recovered.term().get(), 3);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_state_lock_and_temp_all_fail_closed() {
        let directory = TestDirectory::new();
        let target = directory.join("target");
        fs::write(&target, b"untrusted").unwrap();

        let state_symlink = directory.join("state-link.term");
        symlink(&target, &state_symlink).unwrap();
        assert!(PrimaryTermStore::new(&state_symlink).acquire().is_err());

        let lock_state = directory.join("lock-link.term");
        let lock_path = sidecar_path(&lock_state, LOCK_SUFFIX).unwrap();
        symlink(&target, &lock_path).unwrap();
        assert!(PrimaryTermStore::new(&lock_state).acquire().is_err());

        let temp_state = directory.join("temp-link.term");
        let temp_store = PrimaryTermStore::new(&temp_state);
        drop(temp_store.acquire().expect("seed temp-link state"));
        let temp_path = sidecar_path(&temp_state, TEMP_SUFFIX).unwrap();
        symlink(&target, &temp_path).unwrap();
        assert!(temp_store.acquire().is_err());
        assert_eq!(fs::read(&target).unwrap(), b"untrusted");
    }

    #[cfg(unix)]
    #[test]
    fn permissive_existing_files_fail_closed() {
        let directory = TestDirectory::new();
        let state_path = directory.join("primary.term");
        write_state(&state_path, 41);
        fs::set_permissions(&state_path, fs::Permissions::from_mode(0o640)).unwrap();

        assert!(PrimaryTermStore::new(&state_path).acquire().is_err());
        assert_eq!(
            read_current_term(&state_path).unwrap_err().to_string(),
            format!(
                "secure file permissions must be 0600: {}",
                state_path.display()
            )
        );
    }

    #[test]
    fn invalid_magic_version_reserved_and_zero_are_rejected() {
        let directory = TestDirectory::new();
        let state_path = directory.join("primary.term");
        let store = PrimaryTermStore::new(&state_path);
        drop(store.acquire().expect("create stable lock"));

        for index in [0usize, 8, 10, 12] {
            let mut bytes = encode_term_state(NonZeroU64::new(7).unwrap());
            bytes[index] ^= 1;
            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&state_path)
                .unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_all().unwrap();
            assert!(store.acquire().is_err(), "corruption at byte {index}");
        }

        let mut zero = encode_term_state(NonZeroU64::new(7).unwrap());
        zero[12..20].copy_from_slice(&0u64.to_le_bytes());
        let mut checksum = Crc32c::new();
        checksum.update(&zero[..TERM_STATE_PREFIX_LEN]);
        zero[TERM_STATE_PREFIX_LEN..].copy_from_slice(&checksum.finish().to_le_bytes());
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&state_path)
            .unwrap();
        file.write_all(&zero).unwrap();
        file.sync_all().unwrap();
        assert!(store.acquire().is_err());
    }
}
