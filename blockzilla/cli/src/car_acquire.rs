use crate::car_preflight::{CarPreflightConfig, CarPreflightReceipt, preflight_car};
use anyhow::{Context, Result};
use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    io,
    num::NonZeroU64,
    path::{Component, Path},
    process::Command,
    thread,
    time::Duration,
};
use tracing::{info, warn};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

pub(crate) const ACQUISITION_LOCK_FD_ENV: &str = "BLOCKZILLA_ACQUISITION_LOCK_FD";
const MEMORY_POLL_INTERVAL: Duration = Duration::from_secs(10);
pub(crate) const PROGRESS_FILE_ENV: &str = "BLOCKZILLA_PROGRESS_FILE";

#[derive(Debug, Clone)]
pub(crate) struct CarAcquireConfig<'a> {
    pub url: &'a str,
    pub part: &'a Path,
    pub canonical: &'a Path,
    pub alternate: &'a Path,
    pub epoch: u64,
    pub receipt: &'a Path,
    pub progress_json: Option<&'a Path>,
    pub expected_bytes: Option<NonZeroU64>,
    pub aria2c: &'a Path,
    pub max_attempts: u8,
    pub required_memory_mib: u64,
    pub io_buffer_bytes: usize,
}

/// Acquire one CAR through aria2c, validate it, and publish it without replacement.
///
/// The scheduler owns cross-process admission and passes an inherited epoch lock.
/// This helper owns the child/retry and durability boundary. Failed attempts keep
/// the regular `.part` file so an explicit scheduler retry can resume it.
pub(crate) fn acquire_car(config: CarAcquireConfig<'_>) -> Result<()> {
    validate_config(&config)?;
    validate_inherited_lock()?;

    let part_parent = parent_or_dot(config.part);
    fs::create_dir_all(part_parent)
        .with_context(|| format!("create CAR download directory {}", part_parent.display()))?;
    ensure_publish_targets_absent(config.canonical, config.alternate)?;
    ensure_resumable_part(config.part)?;

    run_aria2c_with_retries(&config)?;
    let part_metadata = regular_nonempty_metadata(config.part)?;
    validate_expected_bytes(&config, &part_metadata)?;
    sync_file(config.part)?;
    // Another controller or operator may have published this epoch while the
    // transfer was running. Refuse before waiting for memory or replacing a
    // shared preflight receipt.
    ensure_publish_targets_absent(config.canonical, config.alternate)?;

    wait_for_memory_gate(config.required_memory_mib, MEMORY_POLL_INTERVAL)?;
    ensure_publish_targets_absent(config.canonical, config.alternate)?;
    let receipt = preflight_car(CarPreflightConfig {
        input: config.part,
        epoch: config.epoch,
        receipt: config.receipt,
        io_buffer_bytes: config.io_buffer_bytes,
        progress_json: config.progress_json,
    })?;
    validate_preflight_receipt(&config, &receipt, &part_metadata)?;
    sync_file(config.receipt)?;

    // Recheck both names and the source fingerprint immediately before the
    // no-replace operation. The final primitive still enforces no-clobber, so
    // this check is diagnostic rather than the race-safety boundary.
    ensure_publish_targets_absent(config.canonical, config.alternate)?;
    validate_preflight_receipt(&config, &receipt, &regular_nonempty_metadata(config.part)?)?;
    publish_noclobber(config.part, config.canonical).with_context(|| {
        format!(
            "publish validated CAR without replacement {} -> {}",
            config.part.display(),
            config.canonical.display()
        )
    })?;

    // Make the successful publication durable before any diagnostic check can
    // return. Scheduler reconciliation treats a valid canonical file+receipt
    // as authoritative even when the helper is interrupted after the rename.
    sync_file(config.canonical)?;
    sync_directory(parent_or_dot(config.canonical))?;
    if parent_or_dot(config.part) != parent_or_dot(config.canonical) {
        sync_directory(parent_or_dot(config.part))?;
    }
    let canonical_metadata = regular_nonempty_metadata(config.canonical)?;
    validate_preflight_receipt(&config, &receipt, &canonical_metadata)?;
    ensure_absent(config.alternate, "alternate canonical CAR")?;
    anyhow::ensure!(
        !path_exists_no_follow(config.part)?,
        "CAR part still exists after publication: {}",
        config.part.display()
    );
    info!(
        "CAR acquisition complete: epoch={} bytes={} canonical={} receipt={}",
        config.epoch,
        canonical_metadata.len(),
        config.canonical.display(),
        config.receipt.display(),
    );
    Ok(())
}

fn validate_config(config: &CarAcquireConfig<'_>) -> Result<()> {
    let inherited_progress = config
        .progress_json
        .is_none()
        .then(|| std::env::var_os(PROGRESS_FILE_ENV))
        .flatten()
        .map(std::path::PathBuf::from);
    validate_config_with_inherited_progress(config, inherited_progress.as_deref())
}

fn validate_config_with_inherited_progress(
    config: &CarAcquireConfig<'_>,
    inherited_progress: Option<&Path>,
) -> Result<()> {
    anyhow::ensure!(!config.url.is_empty(), "CAR source URL must not be empty");
    anyhow::ensure!(
        config.max_attempts > 0,
        "CAR download attempts must be positive"
    );
    anyhow::ensure!(
        config.io_buffer_bytes > 0,
        "CAR preflight I/O buffer must be positive"
    );
    anyhow::ensure!(
        !config.aria2c.as_os_str().is_empty(),
        "aria2c executable must not be empty"
    );
    // `preflight_car` inherits BLOCKZILLA_PROGRESS_FILE when no explicit path
    // is supplied. Validate that effective path too so a direct helper
    // invocation cannot use ambient state to clobber an acquisition artifact.
    let mut artifacts = vec![
        ("part", config.part),
        ("canonical", config.canonical),
        ("alternate", config.alternate),
        ("receipt", config.receipt),
    ];
    if let Some(progress) = config.progress_json.or(inherited_progress) {
        artifacts.push(("progress", progress));
    }
    validate_distinct_artifact_paths("CAR acquisition", &artifacts)
}

pub(crate) fn validate_distinct_artifact_paths(
    operation: &str,
    artifacts: &[(&str, &Path)],
) -> Result<()> {
    let compared = artifacts
        .iter()
        .map(|(name, path)| Ok((*name, *path, comparison_path(operation, path)?)))
        .collect::<Result<Vec<_>>>()?;
    for (left_index, (left_name, left_path, left)) in compared.iter().enumerate() {
        for (right_name, right_path, right) in &compared[left_index + 1..] {
            anyhow::ensure!(
                left != right,
                "{operation} {left_name} and {right_name} paths resolve to the same target: {} and {}",
                left_path.display(),
                right_path.display()
            );
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct ComparisonPath {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    existing_prefix: std::path::PathBuf,
    missing_suffix: Vec<OsString>,
}

/// Resolve a possibly not-yet-created artifact without following a future
/// path component. Existing prefixes are canonicalized, and Unix comparison
/// uses their filesystem identity so bind-mount aliases are caught too.
fn comparison_path(operation: &str, path: &Path) -> Result<ComparisonPath> {
    anyhow::ensure!(
        path.file_name().is_some(),
        "{operation} artifact path has no file name: {}",
        path.display()
    );
    anyhow::ensure!(
        !path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir)),
        "{operation} artifact path contains dot traversal: {}",
        path.display()
    );

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .with_context(|| format!("resolve current directory for {operation} path validation"))?
            .join(path)
    };
    let mut cursor = absolute.as_path();
    let mut missing_suffix = Vec::new();
    loop {
        match fs::symlink_metadata(cursor) {
            Ok(_) => {
                let existing_prefix = fs::canonicalize(cursor).with_context(|| {
                    format!(
                        "canonicalize existing {operation} path prefix {}",
                        cursor.display()
                    )
                })?;
                let metadata = fs::metadata(&existing_prefix).with_context(|| {
                    format!(
                        "inspect existing {operation} path prefix {}",
                        existing_prefix.display()
                    )
                })?;
                missing_suffix.reverse();
                return Ok(ComparisonPath {
                    #[cfg(unix)]
                    device: metadata.dev(),
                    #[cfg(unix)]
                    inode: metadata.ino(),
                    #[cfg(not(unix))]
                    existing_prefix,
                    missing_suffix,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = cursor.file_name().ok_or_else(|| {
                    anyhow::anyhow!(
                        "{operation} path has no existing ancestor: {}",
                        path.display()
                    )
                })?;
                missing_suffix.push(name.to_os_string());
                cursor = cursor.parent().ok_or_else(|| {
                    anyhow::anyhow!(
                        "{operation} path has no existing ancestor: {}",
                        path.display()
                    )
                })?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "inspect existing {operation} path prefix {}",
                        cursor.display()
                    )
                });
            }
        }
    }
}

fn validate_expected_bytes(config: &CarAcquireConfig<'_>, metadata: &fs::Metadata) -> Result<()> {
    if let Some(expected) = config.expected_bytes {
        anyhow::ensure!(
            metadata.len() == expected.get(),
            "downloaded CAR size mismatch for epoch {}: expected {} bytes, got {}",
            config.epoch,
            expected,
            metadata.len()
        );
    }
    Ok(())
}

fn run_aria2c_with_retries(config: &CarAcquireConfig<'_>) -> Result<()> {
    let part_parent = parent_or_dot(config.part);
    let part_name = config
        .part
        .file_name()
        .context("validated CAR part path lost its file name")?;
    let mut failures = Vec::with_capacity(usize::from(config.max_attempts));

    for attempt in 1..=config.max_attempts {
        ensure_publish_targets_absent(config.canonical, config.alternate)?;
        ensure_resumable_part(config.part)?;
        info!(
            "Starting aria2c CAR download: epoch={} attempt={}/{} part={}",
            config.epoch,
            attempt,
            config.max_attempts,
            config.part.display(),
        );
        let result = Command::new(config.aria2c)
            .args([
                OsStr::new("--continue=true"),
                OsStr::new("--allow-overwrite=true"),
                OsStr::new("--auto-file-renaming=false"),
                OsStr::new("--file-allocation=none"),
                OsStr::new("--max-connection-per-server=4"),
                OsStr::new("--split=4"),
                OsStr::new("--min-split-size=64M"),
                OsStr::new("--dir"),
            ])
            .arg(part_parent)
            .arg("--out")
            .arg(part_name)
            .arg(config.url)
            .status();

        let failure = match result {
            Ok(status) if status.success() => match regular_nonempty_metadata(config.part) {
                Ok(_) => return Ok(()),
                Err(error) => {
                    format!("aria2c reported success but part validation failed: {error:#}")
                }
            },
            Ok(status) => format!("aria2c exited with {status}"),
            Err(error) => format!("could not start aria2c: {error}"),
        };
        warn!(
            "CAR download attempt failed: epoch={} attempt={}/{} error={}",
            config.epoch, attempt, config.max_attempts, failure,
        );
        failures.push(failure);
    }

    anyhow::bail!(
        "aria2c failed to acquire epoch {} after {} attempt(s): {}",
        config.epoch,
        config.max_attempts,
        failures.join("; ")
    )
}

fn wait_for_memory_gate(required_memory_mib: u64, poll_interval: Duration) -> Result<()> {
    if required_memory_mib == 0 {
        return Ok(());
    }
    let required_kib = required_memory_mib
        .checked_mul(1024)
        .context("required preflight memory overflows KiB")?;
    let mut reported_wait = false;
    loop {
        let meminfo = match fs::read_to_string("/proc/meminfo") {
            Ok(meminfo) => meminfo,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                info!("/proc/meminfo is unavailable; skipping Linux CAR preflight memory gate");
                return Ok(());
            }
            Err(error) => return Err(error).context("read /proc/meminfo for CAR memory gate"),
        };
        let available_kib = parse_mem_available_kib(&meminfo)
            .context("/proc/meminfo has no valid MemAvailable value")?;
        if available_kib >= required_kib {
            if reported_wait {
                info!(
                    "CAR preflight memory gate opened: available_mib={} required_mib={}",
                    available_kib / 1024,
                    required_memory_mib,
                );
            }
            return Ok(());
        }
        if !reported_wait {
            info!(
                "Waiting for CAR preflight memory gate: available_mib={} required_mib={}",
                available_kib / 1024,
                required_memory_mib,
            );
            reported_wait = true;
        }
        thread::sleep(poll_interval);
    }
}

fn parse_mem_available_kib(meminfo: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        line.strip_prefix("MemAvailable:")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

fn validate_preflight_receipt(
    config: &CarAcquireConfig<'_>,
    receipt: &CarPreflightReceipt,
    source_metadata: &fs::Metadata,
) -> Result<()> {
    let modified = source_metadata
        .modified()
        .with_context(|| format!("read CAR mtime for epoch {}", config.epoch))?
        .duration_since(std::time::UNIX_EPOCH)
        .with_context(|| format!("CAR mtime predates Unix epoch for epoch {}", config.epoch))?;
    anyhow::ensure!(
        receipt.structurally_valid && receipt.clean_eof && receipt.eligible_for_compaction,
        "CAR preflight did not make epoch {} eligible for compaction",
        config.epoch
    );
    anyhow::ensure!(
        receipt.epoch == config.epoch,
        "CAR preflight epoch mismatch"
    );
    anyhow::ensure!(
        receipt.source_path == config.part,
        "CAR preflight source path changed: expected={} actual={}",
        config.part.display(),
        receipt.source_path.display()
    );
    anyhow::ensure!(
        receipt.source_bytes == source_metadata.len()
            && receipt.source_modified_unix_secs == modified.as_secs()
            && receipt.source_modified_subsec_nanos == modified.subsec_nanos(),
        "CAR source fingerprint changed after preflight for epoch {}",
        config.epoch
    );
    Ok(())
}

fn ensure_publish_targets_absent(canonical: &Path, alternate: &Path) -> Result<()> {
    ensure_absent(canonical, "canonical CAR")?;
    ensure_absent(alternate, "alternate canonical CAR")
}

fn ensure_absent(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => anyhow::bail!("refusing to replace existing {label}: {}", path.display()),
        Err(error) => Err(error).with_context(|| format!("inspect {label} {}", path.display())),
    }
}

fn path_exists_no_follow(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

fn ensure_resumable_part(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => anyhow::bail!(
            "CAR download part is not a regular file and cannot be resumed: {}",
            path.display()
        ),
        Err(error) => Err(error).with_context(|| format!("inspect CAR part {}", path.display())),
    }
}

fn regular_nonempty_metadata(path: &Path) -> Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect downloaded CAR {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file(),
        "downloaded CAR is not a regular file: {}",
        path.display()
    );
    anyhow::ensure!(
        metadata.len() > 0,
        "downloaded CAR is empty: {}",
        path.display()
    );
    Ok(metadata)
}

fn sync_file(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open {} for fsync", path.display()))?
        .sync_all()
        .with_context(|| format!("fsync {}", path.display()))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for fsync", path.display()))?
        .sync_all()
        .with_context(|| format!("fsync directory {}", path.display()))
}

fn parent_or_dot(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[cfg(target_os = "linux")]
fn publish_noclobber(source: &Path, target: &Path) -> Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let source_c = CString::new(source.as_os_str().as_bytes())
        .with_context(|| format!("CAR part path contains NUL: {}", source.display()))?;
    let target_c = CString::new(target.as_os_str().as_bytes())
        .with_context(|| format!("canonical CAR path contains NUL: {}", target.display()))?;
    // Call the kernel directly instead of linking the glibc `renameat2`
    // wrapper. The syscall has been available since Linux 3.15, while the
    // wrapper was added to glibc much later; using the wrapper needlessly
    // raised the minimum glibc version of an otherwise portable binary.
    // SAFETY: both C strings remain live and NUL-terminated for the call, and
    // the remaining arguments match the kernel's renameat2 ABI.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD as libc::c_long,
            source_c.as_ptr(),
            libc::AT_FDCWD as libc::c_long,
            target_c.as_ptr(),
            libc::RENAME_NOREPLACE as libc::c_long,
        )
    };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(libc::ENOSYS) | Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
    ) {
        return publish_noclobber_hard_link(source, target);
    }
    Err(error.into())
}

#[cfg(not(target_os = "linux"))]
fn publish_noclobber(source: &Path, target: &Path) -> Result<()> {
    publish_noclobber_hard_link(source, target)
}

fn publish_noclobber_hard_link(source: &Path, target: &Path) -> Result<()> {
    fs::hard_link(source, target).with_context(|| {
        format!(
            "create no-clobber canonical CAR link {} -> {}",
            source.display(),
            target.display()
        )
    })?;
    // Make the canonical name durable before removing the resumable name. If
    // removal fails, both names intentionally remain and the caller fails
    // closed without deleting the now-durable canonical object.
    sync_file(target)?;
    sync_directory(parent_or_dot(target))?;
    fs::remove_file(source)
        .with_context(|| format!("remove published CAR part {}", source.display()))?;
    Ok(())
}

fn validate_inherited_lock() -> Result<()> {
    let Some(raw) = std::env::var_os(ACQUISITION_LOCK_FD_ENV) else {
        return Ok(());
    };
    let fd = raw
        .to_str()
        .context("acquisition lock fd is not UTF-8")?
        .parse::<libc::c_int>()
        .context("acquisition lock fd is not an integer")?;
    // SAFETY: fcntl does not dereference memory and only inspects the supplied fd.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error()).context("read acquisition lock fd flags");
    }
    // Keep the lock inheritable. If this supervisor is hard-killed, its aria2c
    // child continues to own the epoch until that writer exits; a restarted
    // scheduler therefore cannot launch a second writer for the same `.part`.
    anyhow::ensure!(
        flags & libc::FD_CLOEXEC == 0,
        "inherited acquisition lock would close before aria2c exec"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use minicbor::Encoder;
    use std::{
        io::Write,
        os::{fd::AsRawFd, unix::fs::PermissionsExt},
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn parses_memavailable_without_accepting_other_memory_fields() {
        assert_eq!(
            parse_mem_available_kib(
                "MemTotal:       1000 kB\nMemFree:          20 kB\nMemAvailable:    700 kB\n"
            ),
            Some(700)
        );
        assert_eq!(parse_mem_available_kib("MemFree: 700 kB\n"), None);
        assert_eq!(parse_mem_available_kib("MemAvailable: nope kB\n"), None);
    }

    #[test]
    fn caller_expected_bytes_are_checked_exactly() {
        let root = test_root("expected-bytes");
        fs::create_dir_all(&root).unwrap();
        let part = root.join("epoch-7.car.part");
        fs::write(&part, b"four").unwrap();
        let canonical = root.join("epoch-7.car");
        let alternate = root.join("epoch-7.car.zst");
        let receipt = root.join("epoch-7.receipt.json");
        let metadata = fs::metadata(&part).unwrap();
        let mut config = CarAcquireConfig {
            url: "https://example.invalid/epoch-7.car",
            part: &part,
            canonical: &canonical,
            alternate: &alternate,
            epoch: 7,
            receipt: &receipt,
            progress_json: None,
            expected_bytes: NonZeroU64::new(4),
            aria2c: Path::new("aria2c"),
            max_attempts: 1,
            required_memory_mib: 0,
            io_buffer_bytes: 1,
        };
        validate_expected_bytes(&config, &metadata).unwrap();
        config.expected_bytes = NonZeroU64::new(5);
        let error = validate_expected_bytes(&config, &metadata).unwrap_err();
        assert!(format!("{error:#}").contains("expected 5 bytes, got 4"));
        config.expected_bytes = None;
        validate_expected_bytes(&config, &metadata).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn progress_path_cannot_alias_any_acquisition_artifact() {
        let root = test_root("path-validation");
        let part = root.join("epoch-7.car.part");
        let canonical = root.join("epoch-7.car");
        let alternate = root.join("epoch-7.car.zst");
        let receipt = root.join("epoch-7.receipt.json");
        let distinct_progress = root.join("epoch-7.progress.json");

        for aliased in [&part, &canonical, &alternate, &receipt] {
            let config = CarAcquireConfig {
                url: "https://example.invalid/epoch-7.car",
                part: &part,
                canonical: &canonical,
                alternate: &alternate,
                epoch: 7,
                receipt: &receipt,
                progress_json: Some(aliased),
                expected_bytes: None,
                aria2c: Path::new("aria2c"),
                max_attempts: 1,
                required_memory_mib: 0,
                io_buffer_bytes: 1,
            };
            let explicit_error =
                validate_config_with_inherited_progress(&config, None).unwrap_err();
            assert!(format!("{explicit_error:#}").contains("progress"));

            let inherited_config = CarAcquireConfig {
                progress_json: None,
                ..config
            };
            let inherited_error =
                validate_config_with_inherited_progress(&inherited_config, Some(aliased))
                    .unwrap_err();
            assert!(format!("{inherited_error:#}").contains("progress"));
        }

        let acquire_error = acquire_car(CarAcquireConfig {
            url: "https://example.invalid/epoch-7.car",
            part: &part,
            canonical: &canonical,
            alternate: &alternate,
            epoch: 7,
            receipt: &receipt,
            progress_json: Some(&part),
            expected_bytes: None,
            aria2c: Path::new("aria2c"),
            max_attempts: 1,
            required_memory_mib: 0,
            io_buffer_bytes: 1,
        })
        .unwrap_err();
        assert!(format!("{acquire_error:#}").contains("progress"));
        assert!(!root.exists(), "invalid config mutated the filesystem");

        validate_config(&CarAcquireConfig {
            url: "https://example.invalid/epoch-7.car",
            part: &part,
            canonical: &canonical,
            alternate: &alternate,
            epoch: 7,
            receipt: &receipt,
            progress_json: Some(&distinct_progress),
            expected_bytes: None,
            aria2c: Path::new("aria2c"),
            max_attempts: 1,
            required_memory_mib: 0,
            io_buffer_bytes: 1,
        })
        .unwrap();

        let traversing_progress = root.join("subdir/../epoch-7.car.part");
        let traversal_error = validate_config(&CarAcquireConfig {
            url: "https://example.invalid/epoch-7.car",
            part: &part,
            canonical: &canonical,
            alternate: &alternate,
            epoch: 7,
            receipt: &receipt,
            progress_json: Some(&traversing_progress),
            expected_bytes: None,
            aria2c: Path::new("aria2c"),
            max_attempts: 1,
            required_memory_mib: 0,
            io_buffer_bytes: 1,
        })
        .unwrap_err();
        assert!(format!("{traversal_error:#}").contains("dot traversal"));
    }

    #[cfg(unix)]
    #[test]
    fn progress_path_cannot_alias_through_a_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let root = test_root("symlinked-path-validation");
        let real = root.join("real");
        let alias = root.join("alias");
        fs::create_dir_all(&real).unwrap();
        symlink(&real, &alias).unwrap();
        let part = real.join("epoch-7.car.part");
        let canonical = real.join("epoch-7.car");
        let alternate = real.join("epoch-7.car.zst");
        let receipt = real.join("epoch-7.receipt.json");
        let progress = alias.join("epoch-7.car.part");

        let error = validate_config(&CarAcquireConfig {
            url: "https://example.invalid/epoch-7.car",
            part: &part,
            canonical: &canonical,
            alternate: &alternate,
            epoch: 7,
            receipt: &receipt,
            progress_json: Some(&progress),
            expected_bytes: None,
            aria2c: Path::new("aria2c"),
            max_attempts: 1,
            required_memory_mib: 0,
            io_buffer_bytes: 1,
        })
        .unwrap_err();
        assert!(format!("{error:#}").contains("resolve to the same target"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn no_clobber_publication_preserves_existing_target() {
        let root = test_root("no-clobber");
        fs::create_dir_all(&root).unwrap();
        let part = root.join("epoch-7.car.part");
        let canonical = root.join("epoch-7.car");
        fs::write(&part, b"candidate").unwrap();
        fs::write(&canonical, b"existing").unwrap();

        assert!(publish_noclobber(&part, &canonical).is_err());
        assert_eq!(fs::read(&canonical).unwrap(), b"existing");
        assert_eq!(fs::read(&part).unwrap(), b"candidate");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn inherited_scheduler_lock_remains_owned_by_aria2_exec() {
        let root = test_root("cloexec");
        fs::create_dir_all(&root).unwrap();
        let lock = File::create(root.join("lock")).unwrap();
        let fd = lock.as_raw_fd();
        // SAFETY: fd is live for the duration of the test.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert!(flags >= 0);
        // SAFETY: fd remains live.
        assert_eq!(
            unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0
        );
        // SAFETY: this test is single-threaded with respect to the unique key.
        unsafe { std::env::set_var(ACQUISITION_LOCK_FD_ENV, fd.to_string()) };
        validate_inherited_lock().unwrap();
        // SAFETY: undo the test-only process environment mutation.
        unsafe { std::env::remove_var(ACQUISITION_LOCK_FD_ENV) };
        // SAFETY: fd remains live.
        let updated = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_eq!(updated & libc::FD_CLOEXEC, 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_canonical_publication_stops_before_preflight_receipt() {
        let epoch = 9;
        let root = test_root("concurrent-canonical");
        let downloads = root.join(".downloads");
        fs::create_dir_all(&downloads).unwrap();
        let source = root.join("source.car");
        let part = downloads.join(format!("epoch-{epoch}.car.part"));
        let canonical = root.join(format!("epoch-{epoch}.car"));
        let alternate = root.join(format!("epoch-{epoch}.car.zst"));
        let receipt = root.join("state/preflight.json");
        fs::write(&source, test_car(epoch)).unwrap();

        let fake_aria2c = root.join("aria2c");
        let mut script = File::create(&fake_aria2c).unwrap();
        script
            .write_all(
                br#"#!/bin/sh
set -eu
dir=
out=
source=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dir) dir=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    --*) shift ;;
    *) source=$1; shift ;;
  esac
done
cp "$source" "$dir/$out"
printf competing > "$FAKE_CANONICAL"
"#,
            )
            .unwrap();
        drop(script);
        fs::set_permissions(&fake_aria2c, fs::Permissions::from_mode(0o700)).unwrap();

        // SAFETY: this test is single-threaded with respect to this unique key.
        unsafe { std::env::set_var("FAKE_CANONICAL", &canonical) };
        let result = acquire_car(CarAcquireConfig {
            url: source.to_str().unwrap(),
            part: &part,
            canonical: &canonical,
            alternate: &alternate,
            epoch,
            receipt: &receipt,
            progress_json: None,
            expected_bytes: None,
            aria2c: &fake_aria2c,
            max_attempts: 1,
            required_memory_mib: 0,
            io_buffer_bytes: 64 * 1024,
        });
        // SAFETY: undo the test-only process environment mutation.
        unsafe { std::env::remove_var("FAKE_CANONICAL") };

        let error = result.unwrap_err();
        assert!(format!("{error:#}").contains("refusing to replace existing canonical CAR"));
        assert_eq!(fs::read(&canonical).unwrap(), b"competing");
        assert!(!receipt.exists());
        assert!(part.is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn aria2_retry_preflight_and_publication_are_one_fail_closed_flow() {
        let epoch = 7;
        let root = test_root("flow");
        let downloads = root.join(".downloads");
        fs::create_dir_all(&downloads).unwrap();
        let source = root.join("source.car");
        let part = downloads.join(format!("epoch-{epoch}.car.part"));
        let canonical = root.join(format!("epoch-{epoch}.car"));
        let alternate = root.join(format!("epoch-{epoch}.car.zst"));
        let receipt = root.join("state/preflight.json");
        let progress = root.join("state/progress.json");
        fs::write(&source, test_car(epoch)).unwrap();

        let attempts = root.join("attempts");
        let fake_aria2c = root.join("aria2c");
        let mut script = File::create(&fake_aria2c).unwrap();
        script
            .write_all(
                br#"#!/bin/sh
set -eu
count=0
if [ -f "$FAKE_ARIA_ATTEMPTS" ]; then count=$(cat "$FAKE_ARIA_ATTEMPTS"); fi
count=$((count + 1))
echo "$count" > "$FAKE_ARIA_ATTEMPTS"
if [ "$count" -lt 2 ]; then exit 7; fi
dir=
out=
source=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dir) dir=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    --*) shift ;;
    *) source=$1; shift ;;
  esac
done
cp "$source" "$dir/$out"
"#,
            )
            .unwrap();
        drop(script);
        fs::set_permissions(&fake_aria2c, fs::Permissions::from_mode(0o700)).unwrap();

        // SAFETY: this test is single-threaded with respect to this unique key;
        // the child is the only reader and the value is removed before return.
        unsafe { std::env::set_var("FAKE_ARIA_ATTEMPTS", &attempts) };
        let result = acquire_car(CarAcquireConfig {
            url: source.to_str().unwrap(),
            part: &part,
            canonical: &canonical,
            alternate: &alternate,
            epoch,
            receipt: &receipt,
            progress_json: Some(&progress),
            expected_bytes: NonZeroU64::new(fs::metadata(&source).unwrap().len()),
            aria2c: &fake_aria2c,
            max_attempts: 3,
            required_memory_mib: 0,
            io_buffer_bytes: 64 * 1024,
        });
        // SAFETY: undo the test-only process environment mutation.
        unsafe { std::env::remove_var("FAKE_ARIA_ATTEMPTS") };
        result.unwrap();

        assert_eq!(fs::read_to_string(attempts).unwrap().trim(), "2");
        assert_eq!(fs::read(&canonical).unwrap(), fs::read(&source).unwrap());
        assert!(!part.exists());
        assert!(!alternate.exists());
        assert!(receipt.is_file());
        assert!(progress.is_file());
        let persisted: CarPreflightReceipt =
            serde_json::from_slice(&fs::read(receipt).unwrap()).unwrap();
        assert!(persisted.structurally_valid);
        assert!(persisted.clean_eof);
        assert!(persisted.eligible_for_compaction);
        fs::remove_dir_all(root).unwrap();
    }

    fn test_car(epoch: u64) -> Vec<u8> {
        let slot = epoch * crate::SLOTS_PER_EPOCH + 1;
        let mut car = vec![1, 0x80];
        let mut entry = Encoder::new(Vec::new());
        entry.array(4).unwrap();
        entry.u8(1).unwrap();
        entry.u64(1).unwrap();
        entry.bytes(&[1; 32]).unwrap();
        entry.array(0).unwrap();
        push_car_entry(&mut car, &entry.into_writer());

        let mut block = Encoder::new(Vec::new());
        block.array(5).unwrap();
        block.u8(2).unwrap();
        block.u64(slot).unwrap();
        block.array(1).unwrap();
        block.array(2).unwrap();
        block.i64(0).unwrap();
        block.i64(1).unwrap();
        block.array(1).unwrap();
        block.null().unwrap();
        block.array(3).unwrap();
        block.u64(slot - 1).unwrap();
        block.i64(1_700_000_000).unwrap();
        block.u64(slot).unwrap();
        push_car_entry(&mut car, &block.into_writer());
        car
    }

    fn push_car_entry(car: &mut Vec<u8>, payload: &[u8]) {
        push_uvarint(car, (36 + payload.len()) as u64);
        car.extend_from_slice(&[0; 36]);
        car.extend_from_slice(payload);
    }

    fn push_uvarint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn test_root(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-car-acquire-{label}-{}-{unique}",
            std::process::id()
        ))
    }
}
