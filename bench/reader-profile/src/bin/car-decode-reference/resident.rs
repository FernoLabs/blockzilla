//! Benchmark-only read-only CAR mapping pinned in physical memory.
//!
//! This source is for an operator-controlled, immutable local cache. Read-only
//! permissions and an advisory lock do not prevent a privileged process from
//! changing the inode. The operator must keep the mapped file unchanged for the
//! source's lifetime. Stat checks detect changes but cannot make concurrent file
//! modification safe. No network or export work is performed by this module.

#[cfg(unix)]
mod platform {
    use anyhow::{Context, Result, ensure};
    use blockzilla_source::{RangeSource, SourceError, SourceResult, validate_object_name};
    use memmap2::{Mmap, MmapOptions};
    use serde_json::{Value, json};
    use std::{
        fs::{File, Metadata, OpenOptions},
        io,
        os::{
            fd::AsRawFd,
            unix::fs::{MetadataExt, OpenOptionsExt},
        },
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::Instant,
    };

    // Residency checks use at most this many one-byte page states at once.
    // On a 4 KiB-page host one batch checks 4 GiB of the mapping.
    const RESIDENCY_BATCH_PAGES: usize = 1 << 20;
    const MAX_RANGE_BYTES: usize = 128 << 20;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct FileIdentity {
        device: u64,
        inode: u64,
        bytes: u64,
        modified_ns: i64,
        changed_ns: i64,
    }

    impl FileIdentity {
        fn from_value(value: &Value) -> Result<Self> {
            Ok(Self {
                device: value["st_dev"].as_u64().context("resident stat st_dev")?,
                inode: value["st_ino"].as_u64().context("resident stat st_ino")?,
                bytes: value["st_size"].as_u64().context("resident stat st_size")?,
                modified_ns: value["st_mtime_ns"]
                    .as_i64()
                    .context("resident stat st_mtime_ns")?,
                changed_ns: value["st_ctime_ns"]
                    .as_i64()
                    .context("resident stat st_ctime_ns")?,
            })
        }

        fn from_metadata(metadata: &Metadata) -> Result<Self> {
            fn nanos(seconds: i64, nanoseconds: i64) -> Result<i64> {
                ensure!(
                    (0..1_000_000_000).contains(&nanoseconds),
                    "invalid file timestamp"
                );
                seconds
                    .checked_mul(1_000_000_000)
                    .and_then(|value| value.checked_add(nanoseconds))
                    .context("file timestamp overflow")
            }
            ensure!(metadata.is_file(), "resident CAR must be a regular file");
            ensure!(
                metadata.mode() & 0o222 == 0,
                "resident CAR must have no write permission bits"
            );
            Ok(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
                bytes: metadata.len(),
                modified_ns: nanos(metadata.mtime(), metadata.mtime_nsec())?,
                changed_ns: nanos(metadata.ctime(), metadata.ctime_nsec())?,
            })
        }

        fn receipt(&self) -> Value {
            json!({"st_dev":self.device, "st_ino":self.inode, "st_size":self.bytes,
                "st_mtime_ns":self.modified_ns, "st_ctime_ns":self.changed_ns})
        }
    }

    pub(crate) struct ResidentSource {
        path: PathBuf,
        object: String,
        // Keep the open descriptor and its advisory lock alive with the mapping.
        file: File,
        mapping: Mmap,
        identity: FileIdentity,
        page_bytes: usize,
        lock_seconds: f64,
        read_calls: AtomicU64,
        read_bytes: AtomicU64,
    }

    impl ResidentSource {
        /// Open an already verified, read-only cache file and lock all its pages.
        /// `expected_stat` is the cache manifest file record's `stat` object.
        /// The caller must ensure that no process changes this file while mapped.
        pub(crate) fn open(path: &Path, expected_stat: &Value) -> Result<Self> {
            let expected = FileIdentity::from_value(expected_stat)?;
            ensure!(expected.bytes > 0, "resident CAR must not be empty");
            let bytes = usize::try_from(expected.bytes)
                .context("resident CAR does not fit this platform")?;
            ensure!(
                bytes <= isize::MAX as usize,
                "resident CAR exceeds the mapping address limit"
            );
            let object = path
                .file_name()
                .and_then(|name| name.to_str())
                .context("resident CAR needs a UTF-8 file name")?
                .to_owned();
            validate_object_name(&object).map_err(anyhow::Error::msg)?;
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)
                .with_context(|| format!("open resident CAR {}", path.display()))?;
            // SAFETY: the descriptor belongs to the live File. This only takes an
            // advisory lock; it does not change the file bytes or permissions.
            ensure!(
                unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) } == 0,
                "cannot acquire resident CAR shared file lock: {}",
                io::Error::last_os_error()
            );
            ensure!(
                FileIdentity::from_metadata(&file.metadata()?)? == expected,
                "resident CAR descriptor identity differs from the cache manifest"
            );
            ensure!(
                FileIdentity::from_metadata(&std::fs::symlink_metadata(path)?)? == expected,
                "resident CAR path identity differs from the cache manifest"
            );
            // SAFETY: the immutable-cache contract is explicit above. The fd is
            // read-only, the file has no write bits, its identity matches the
            // verified manifest, and a shared lock excludes cooperating writers.
            // The caller must keep non-cooperating writers away for this lifetime.
            let mapping = unsafe { MmapOptions::new().len(bytes).map(&file) }
                .context("map complete resident CAR")?;
            let started = Instant::now();
            mapping.lock().context("mlock of the complete CAR failed; resident mode requires enough RAM and a sufficient process memlock limit")?;
            let lock_seconds = started.elapsed().as_secs_f64();
            // SAFETY: sysconf has no pointer arguments and does not mutate Rust data.
            let page_bytes = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
                .context("read system page size")?;
            ensure!(page_bytes > 0, "invalid system page size");
            let source = Self {
                path: path.to_owned(),
                object,
                file,
                mapping,
                identity: expected,
                page_bytes,
                lock_seconds,
                read_calls: AtomicU64::new(0),
                read_bytes: AtomicU64::new(0),
            };
            source.validate_unchanged()?;
            source.residency()?;
            Ok(source)
        }

        pub(crate) fn object(&self) -> &str {
            &self.object
        }

        pub(crate) fn identity(&self) -> Value {
            json!({"source_kind":"locked-readonly-file-mmap", "path":self.path,
                "object":self.object, "bytes":self.identity.bytes, "stat":self.identity.receipt(),
                "read_only":true, "mlock_succeeded":true, "lock_seconds":self.lock_seconds,
                "identity_limit":"metadata matches the verified immutable cache; this source does not hash the full CAR again",
                "immutability_limit":"file must remain unchanged; readonly permission and shared advisory lock do not exclude privileged or non-cooperating writers"})
        }

        pub(crate) fn validate_unchanged(&self) -> Result<()> {
            ensure!(
                FileIdentity::from_metadata(&self.file.metadata()?)? == self.identity,
                "resident CAR descriptor identity changed"
            );
            ensure!(
                FileIdentity::from_metadata(&std::fs::symlink_metadata(&self.path)?)?
                    == self.identity,
                "resident CAR path identity changed"
            );
            Ok(())
        }

        /// Check every mapped page in bounded batches. Any missing page is fatal.
        /// The full mapping remains mlocked until this source is dropped.
        pub(crate) fn residency(&self) -> Result<Value> {
            self.validate_unchanged()?;
            let pages = self.mapping.len().div_ceil(self.page_bytes);
            let mut states = vec![0u8; pages.min(RESIDENCY_BATCH_PAGES)];
            let mut first_page = 0usize;
            let mut resident_pages = 0usize;
            while first_page < pages {
                let count = states.len().min(pages - first_page);
                let offset = first_page
                    .checked_mul(self.page_bytes)
                    .context("residency offset overflow")?;
                let length = count
                    .checked_mul(self.page_bytes)
                    .context("residency length overflow")?
                    .min(self.mapping.len() - offset);
                // SAFETY: the address is page-aligned within a live map; length
                // stays within it. states has one writable byte per queried page.
                let result = unsafe {
                    libc::mincore(
                        self.mapping.as_ptr().add(offset) as *mut libc::c_void,
                        length,
                        states.as_mut_ptr().cast(),
                    )
                };
                ensure!(
                    result == 0,
                    "resident CAR mincore failed: {}",
                    io::Error::last_os_error()
                );
                resident_pages += states[..count]
                    .iter()
                    .filter(|state| **state & 1 != 0)
                    .count();
                first_page += count;
            }
            ensure!(
                resident_pages == pages,
                "resident CAR lost residency: {resident_pages} of {pages} pages are resident"
            );
            self.validate_unchanged()?;
            Ok(
                json!({"mapped_bytes":self.mapping.len(), "page_bytes":self.page_bytes,
                "total_pages":pages, "resident_pages":resident_pages,
                "all_pages_resident":true, "mlock_succeeded":true,
                "check":"mincore for every page of the complete CAR mapping"}),
            )
        }

        pub(crate) fn stats(&self) -> Value {
            json!({"read_calls":self.read_calls.load(Ordering::Relaxed),
                "read_bytes":self.read_bytes.load(Ordering::Relaxed),
                "read_method":"memory copy from mlocked readonly mapping", "http_requests":0})
        }

        fn checked_range(
            &self,
            object: &str,
            offset: u64,
            length: usize,
        ) -> SourceResult<std::ops::Range<usize>> {
            validate_object_name(object)
                .map_err(|_| SourceError::InvalidName(object.to_owned()))?;
            if object != self.object {
                return Err(SourceError::NotFound(object.to_owned()));
            }
            let out_of_bounds = || SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size: self.identity.bytes,
            };
            let end = offset
                .checked_add(u64::try_from(length).map_err(|_| out_of_bounds())?)
                .filter(|end| *end <= self.identity.bytes)
                .ok_or_else(out_of_bounds)?;
            if length > MAX_RANGE_BYTES {
                return Err(SourceError::Protocol(
                    "resident range exceeds the 128 MiB copy limit".into(),
                ));
            }
            self.validate_unchanged()
                .map_err(|error| SourceError::Protocol(format!("{error:#}")))?;
            let start = usize::try_from(offset).map_err(|_| out_of_bounds())?;
            let end = usize::try_from(end).map_err(|_| out_of_bounds())?;
            Ok(start..end)
        }

        fn copy(&self, range: std::ops::Range<usize>, destination: &mut [u8]) -> SourceResult<()> {
            destination.copy_from_slice(&self.mapping[range]);
            self.validate_unchanged()
                .map_err(|error| SourceError::Protocol(format!("{error:#}")))?;
            self.read_calls.fetch_add(1, Ordering::Relaxed);
            self.read_bytes
                .fetch_add(destination.len() as u64, Ordering::Relaxed);
            Ok(())
        }
    }

    impl RangeSource for ResidentSource {
        fn recommended_read_concurrency(&self) -> usize {
            128
        }

        fn size(&self, object: &str) -> SourceResult<Option<u64>> {
            validate_object_name(object)
                .map_err(|_| SourceError::InvalidName(object.to_owned()))?;
            if object != self.object {
                return Ok(None);
            }
            self.validate_unchanged()
                .map_err(|error| SourceError::Protocol(format!("{error:#}")))?;
            Ok(Some(self.identity.bytes))
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
            destination: &mut Vec<u8>,
        ) -> SourceResult<()> {
            let range = self.checked_range(object, offset, length)?;
            if destination.len() < length {
                destination
                    .try_reserve_exact(length - destination.len())
                    .map_err(|error| SourceError::Io {
                        object: object.to_owned(),
                        source: io::Error::other(format!(
                            "resident range allocation failed: {error}"
                        )),
                    })?;
            }
            destination.resize(length, 0);
            self.copy(range, destination)
        }

        fn read_range_into_slice(
            &self,
            object: &str,
            offset: u64,
            destination: &mut [u8],
        ) -> SourceResult<()> {
            let range = self.checked_range(object, offset, destination.len())?;
            self.copy(range, destination)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::os::unix::fs::{PermissionsExt, symlink};

        static NEXT: AtomicU64 = AtomicU64::new(0);

        struct Fixture {
            path: PathBuf,
            bytes: Vec<u8>,
            stat: Value,
        }
        impl Fixture {
            fn new() -> Self {
                let path = std::env::temp_dir().join(format!(
                    "resident-car-{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                // Non-page-aligned length also tests the final mincore page.
                let bytes: Vec<_> = (0..(16_384 + 17)).map(|n| (n % 251) as u8).collect();
                std::fs::write(&path, &bytes).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
                let stat = FileIdentity::from_metadata(&std::fs::metadata(&path).unwrap())
                    .unwrap()
                    .receipt();
                Self { path, bytes, stat }
            }
        }
        impl Drop for Fixture {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.path);
            }
        }

        #[test]
        fn locked_source_reads_exact_ranges_and_retains_reusable_storage() {
            let fixture = Fixture::new();
            let source = ResidentSource::open(&fixture.path, &fixture.stat)
                .expect("small mmap mlock must succeed");
            let object = source.object();
            assert_eq!(
                source.size(object).unwrap(),
                Some(fixture.bytes.len() as u64)
            );
            assert_eq!(source.size("missing.car").unwrap(), None);
            assert!(source.size("../escape").is_err());
            assert_eq!(
                source.read_range(object, 0, 1024).unwrap(),
                fixture.bytes[..1024]
            );
            let mut bytes = Vec::with_capacity(512);
            let pointer = bytes.as_ptr();
            source
                .read_range_into(object, 250, 300, &mut bytes)
                .unwrap();
            assert_eq!(bytes, fixture.bytes[250..550]);
            source
                .read_range_into(object, fixture.bytes.len() as u64 - 17, 17, &mut bytes)
                .unwrap();
            assert_eq!(bytes, fixture.bytes[fixture.bytes.len() - 17..]);
            assert_eq!(pointer, bytes.as_ptr());
            source
                .read_range_into_slice(object, fixture.bytes.len() as u64, &mut [])
                .unwrap();
            assert_eq!(source.stats()["read_calls"], 4);
            let resident = source.residency().unwrap();
            assert_eq!(resident["all_pages_resident"], true);
            assert_eq!(resident["total_pages"], resident["resident_pages"]);
            assert_eq!(resident["mapped_bytes"], fixture.bytes.len());
            source.validate_unchanged().unwrap();
        }

        #[test]
        fn invalid_ranges_fail_before_allocating_or_recording_a_read() {
            let fixture = Fixture::new();
            let source = ResidentSource::open(&fixture.path, &fixture.stat).unwrap();
            for (offset, length) in [
                (0, usize::MAX),
                (u64::MAX, 1),
                (fixture.bytes.len() as u64, 1),
                (fixture.bytes.len() as u64 + 1, 0),
            ] {
                let mut output = vec![7; 4];
                assert!(
                    source
                        .read_range_into(source.object(), offset, length, &mut output)
                        .is_err()
                );
                assert_eq!(output, [7; 4]);
            }
            assert!(source.read_range("missing.car", 0, 1).is_err());
            assert!(source.read_range("../escape", 0, 1).is_err());
            assert_eq!(source.stats()["read_calls"], 0);
        }

        #[test]
        fn opening_requires_exact_manifest_identity_and_readonly_regular_file() {
            let fixture = Fixture::new();
            for field in ["st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"] {
                let mut stat = fixture.stat.clone();
                stat[field] = json!(stat[field].as_u64().unwrap() + 1);
                assert!(ResidentSource::open(&fixture.path, &stat).is_err());
            }
            assert!(ResidentSource::open(&fixture.path, &json!({})).is_err());
            std::fs::set_permissions(&fixture.path, std::fs::Permissions::from_mode(0o644))
                .unwrap();
            assert!(ResidentSource::open(&fixture.path, &fixture.stat).is_err());
        }

        #[test]
        fn symlinks_and_path_replacement_are_rejected_without_touching_mapped_data() {
            let fixture = Fixture::new();
            let link = fixture.path.with_extension("link");
            symlink(&fixture.path, &link).unwrap();
            assert!(ResidentSource::open(&link, &fixture.stat).is_err());
            std::fs::remove_file(&link).unwrap();
            let source = ResidentSource::open(&fixture.path, &fixture.stat).unwrap();
            // Renaming does not change or truncate the mapped inode. Validation
            // must reject the replacement path before the next memory copy.
            let retained = fixture.path.with_extension("retained");
            std::fs::rename(&fixture.path, &retained).unwrap();
            std::fs::write(&fixture.path, b"replacement").unwrap();
            std::fs::set_permissions(&fixture.path, std::fs::Permissions::from_mode(0o444))
                .unwrap();
            assert!(source.validate_unchanged().is_err());
            assert!(source.read_range(source.object(), 0, 1).is_err());
            assert_eq!(source.stats()["read_calls"], 0);
            drop(source);
            std::fs::remove_file(retained).unwrap();
        }
    }
}

#[cfg(not(unix))]
mod platform {
    use anyhow::{Result, bail};
    use blockzilla_source::{RangeSource, SourceError, SourceResult};
    use serde_json::Value;
    use std::path::Path;

    const UNSUPPORTED: &str =
        "resident CAR profiling requires Unix mmap, mlock, mincore and file identity support";

    pub(crate) struct ResidentSource;
    impl ResidentSource {
        pub(crate) fn open(_: &Path, _: &Value) -> Result<Self> {
            bail!(UNSUPPORTED)
        }
        pub(crate) fn object(&self) -> &str {
            ""
        }
        pub(crate) fn identity(&self) -> Value {
            Value::Null
        }
        pub(crate) fn stats(&self) -> Value {
            Value::Null
        }
        pub(crate) fn validate_unchanged(&self) -> Result<()> {
            bail!(UNSUPPORTED)
        }
        pub(crate) fn residency(&self) -> Result<Value> {
            bail!(UNSUPPORTED)
        }
    }
    impl RangeSource for ResidentSource {
        fn size(&self, _: &str) -> SourceResult<Option<u64>> {
            Err(SourceError::Protocol(UNSUPPORTED.into()))
        }
        fn read_range(&self, _: &str, _: u64, _: usize) -> SourceResult<Vec<u8>> {
            Err(SourceError::Protocol(UNSUPPORTED.into()))
        }
    }
}

pub(super) use platform::ResidentSource;
