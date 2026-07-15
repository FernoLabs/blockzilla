use anyhow::{Context, Result};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_PUBKEY_HOT_SEED_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};
use tracing::info;

use crate::archive_v2;

pub(crate) const FIRST_SEEN_SCAN_COMPLETE_FILE: &str = "archive-v2-first-seen-scan-complete.v1";
const FIRST_SEEN_SCAN_COMPLETE_MAGIC: &str = "blockzilla-first-seen-scan-complete-v1";
const DEFAULT_FINALIZER_LOCK_FILE: &str = "blockzilla-first-seen-finalizer.lock";

/// Facts persisted after the large in-memory first-seen registry has been
/// written, but before the memory-intensive MPHF is constructed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FirstSeenScanMarker {
    pub(crate) registry_keys: u64,
    pub(crate) references: u64,
    pub(crate) include_access: bool,
}

impl FirstSeenScanMarker {
    pub(crate) fn new(registry_keys: usize, references: u64, include_access: bool) -> Result<Self> {
        let registry_keys =
            u64::try_from(registry_keys).context("first-seen registry key count exceeds u64")?;
        anyhow::ensure!(
            registry_keys <= u64::from(u32::MAX),
            "first-seen registry key count {registry_keys} exceeds compact ID space"
        );
        Ok(Self {
            registry_keys,
            references,
            include_access,
        })
    }

    fn encode(self) -> String {
        format!(
            "{FIRST_SEEN_SCAN_COMPLETE_MAGIC}\nregistry_keys={}\nreferences={}\ninclude_access={}\n",
            self.registry_keys,
            self.references,
            u8::from(self.include_access),
        )
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let text = std::str::from_utf8(bytes).context("scan-complete marker is not UTF-8")?;
        let mut lines = text.lines();
        anyhow::ensure!(
            lines.next() == Some(FIRST_SEEN_SCAN_COMPLETE_MAGIC),
            "unsupported first-seen scan-complete marker"
        );

        let mut registry_keys = None;
        let mut references = None;
        let mut include_access = None;
        for line in lines {
            let Some((name, value)) = line.split_once('=') else {
                anyhow::bail!("malformed first-seen scan-complete marker line: {line}");
            };
            match name {
                "registry_keys" => {
                    registry_keys = Some(
                        value
                            .parse::<u64>()
                            .context("invalid marker registry_keys")?,
                    );
                }
                "references" => {
                    references = Some(value.parse::<u64>().context("invalid marker references")?);
                }
                "include_access" => {
                    include_access = Some(match value {
                        "0" => false,
                        "1" => true,
                        _ => anyhow::bail!("invalid marker include_access value {value}"),
                    });
                }
                _ => anyhow::bail!("unknown first-seen scan-complete marker field {name}"),
            }
        }
        let marker = Self {
            registry_keys: registry_keys.context("marker is missing registry_keys")?,
            references: references.context("marker is missing references")?,
            include_access: include_access.context("marker is missing include_access")?,
        };
        anyhow::ensure!(
            marker.registry_keys <= u64::from(u32::MAX),
            "marker registry key count {} exceeds compact ID space",
            marker.registry_keys
        );
        Ok(marker)
    }
}

/// An advisory lock guard. Scan-only jobs hold a shared lock; the finalizer
/// holds an exclusive lock. This permits multiple scans while ensuring no MPHF
/// build overlaps a scan or another MPHF build on the same machine.
pub(crate) struct FinalizationLock {
    _file: File,
}

pub(crate) fn acquire_scan_lock(lock_path: Option<&Path>) -> Result<FinalizationLock> {
    acquire_lock(lock_path, false)
}

pub(crate) fn acquire_inline_build_lock(lock_path: Option<&Path>) -> Result<FinalizationLock> {
    acquire_lock(lock_path, true)
}

fn acquire_finalizer_lock(lock_path: Option<&Path>) -> Result<FinalizationLock> {
    acquire_lock(lock_path, true)
}

fn default_lock_path() -> PathBuf {
    std::env::temp_dir().join(DEFAULT_FINALIZER_LOCK_FILE)
}

fn acquire_lock(lock_path: Option<&Path>, exclusive: bool) -> Result<FinalizationLock> {
    let path = lock_path
        .map(Path::to_path_buf)
        .unwrap_or_else(default_lock_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create finalizer lock dir {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("open first-seen finalizer lock {}", path.display()))?;
    info!(
        "Waiting for first-seen {} lock: {}",
        if exclusive {
            "exclusive finalizer"
        } else {
            "shared scan"
        },
        path.display()
    );
    lock_file(&file, exclusive)
        .with_context(|| format!("lock first-seen finalizer guard {}", path.display()))?;
    info!(
        "Acquired first-seen {} lock: {}",
        if exclusive {
            "exclusive finalizer"
        } else {
            "shared scan"
        },
        path.display()
    );
    Ok(FinalizationLock { _file: file })
}

#[cfg(unix)]
fn lock_file(file: &File, exclusive: bool) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let operation = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    // SAFETY: `file` owns a valid file descriptor for the duration of this
    // call. flock does not dereference process memory.
    let result = unsafe { libc::flock(file.as_raw_fd(), operation) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn lock_file(_file: &File, _exclusive: bool) -> std::io::Result<()> {
    Ok(())
}

pub(crate) fn first_seen_temp_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("first-seen");
    path.with_file_name(format!("{name}.prehot.tmp"))
}

/// Atomically publishes the scan marker. Call this only after all scan outputs,
/// registry files, the next-epoch seed, manifest temp, and metadata temp have
/// been flushed and closed.
pub(crate) fn write_scan_complete_marker(
    output_dir: &Path,
    marker: FirstSeenScanMarker,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    // The marker is a crash-recovery contract, not merely a process-lifetime
    // flag. Flush candidate data and directory entries before publishing it so
    // a surviving marker never points at writes that only lived in page cache.
    sync_candidate_files(output_dir)?;
    sync_directory(output_dir)?;
    let path = output_dir.join(FIRST_SEEN_SCAN_COMPLETE_FILE);
    let tmp_path = path.with_extension("v1.tmp");
    if tmp_path.exists() {
        std::fs::remove_file(&tmp_path)
            .with_context(|| format!("remove stale marker temp {}", tmp_path.display()))?;
    }

    let mut file = File::create(&tmp_path)
        .with_context(|| format!("create scan-complete marker {}", tmp_path.display()))?;
    file.write_all(marker.encode().as_bytes())
        .with_context(|| format!("write scan-complete marker {}", tmp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync scan-complete marker {}", tmp_path.display()))?;
    drop(file);
    std::fs::rename(&tmp_path, &path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    sync_directory(output_dir)?;
    info!(
        "First-seen scan complete and awaiting MPHF finalization: keys={} refs={} marker={}",
        marker.registry_keys,
        marker.references,
        path.display()
    );
    Ok(())
}

pub(crate) fn sync_candidate_files(output_dir: &Path) -> Result<()> {
    for entry in std::fs::read_dir(output_dir)
        .with_context(|| format!("read scan output dir {}", output_dir.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", output_dir.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat scan output {}", entry.path().display()))?;
        if !file_type.is_file() {
            continue;
        }
        File::open(entry.path())
            .with_context(|| format!("open scan output {} for sync", entry.path().display()))?
            .sync_all()
            .with_context(|| format!("sync scan output {}", entry.path().display()))?;
    }
    Ok(())
}

pub(crate) fn finalize_first_seen_scan(output_dir: &Path, lock_path: Option<&Path>) -> Result<()> {
    let _lock = acquire_finalizer_lock(lock_path)?;
    let marker_path = output_dir.join(FIRST_SEEN_SCAN_COMPLETE_FILE);
    let meta_path = output_dir.join(ARCHIVE_V2_META_FILE);
    let registry_index_path = output_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    let manifest_path = output_dir.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE);

    if meta_path.is_file() && !marker_path.exists() {
        anyhow::ensure!(
            crate::file_nonempty(&registry_index_path) && crate::file_nonempty(&manifest_path),
            "published metadata exists but final first-seen sidecars are incomplete in {}",
            output_dir.display()
        );
        info!(
            "First-seen archive is already finalized: {}",
            output_dir.display()
        );
        return Ok(());
    }

    if !marker_path.exists() {
        recover_unmarked_scan(output_dir)?;
    }

    let marker = read_marker(&marker_path)?;
    if meta_path.is_file() {
        // Metadata is always renamed last. A surviving marker therefore means
        // the process exited between publishing metadata and cleaning up the
        // marker; no expensive work needs to be repeated.
        anyhow::ensure!(
            crate::file_nonempty(&registry_index_path) && crate::file_nonempty(&manifest_path),
            "published metadata exists but final first-seen sidecars are incomplete in {}",
            output_dir.display()
        );
        std::fs::remove_file(&marker_path)
            .with_context(|| format!("remove completed marker {}", marker_path.display()))?;
        sync_directory(output_dir)?;
        info!(
            "Cleaned completed first-seen finalization marker: {}",
            output_dir.display()
        );
        return Ok(());
    }

    validate_scan_outputs(output_dir, marker)?;
    let registry_path = output_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    info!(
        "Finalizing first-seen scan under exclusive lock: keys={} refs={} output={}",
        marker.registry_keys,
        marker.references,
        output_dir.display()
    );
    // Always rebuild when a scan marker is present. This safely replaces an
    // incomplete index left by an interrupted prior finalization.
    archive_v2::build_registry_index(&registry_path, Some(&registry_index_path), true)?;

    let manifest_tmp = first_seen_temp_path(&manifest_path);
    if !manifest_path.exists() {
        std::fs::rename(&manifest_tmp, &manifest_path).with_context(|| {
            format!(
                "rename {} to {}",
                manifest_tmp.display(),
                manifest_path.display()
            )
        })?;
        sync_directory(output_dir)?;
    }
    let meta_tmp = first_seen_temp_path(&meta_path);
    std::fs::rename(&meta_tmp, &meta_path)
        .with_context(|| format!("rename {} to {}", meta_tmp.display(), meta_path.display()))?;
    sync_directory(output_dir)?;
    std::fs::remove_file(&marker_path)
        .with_context(|| format!("remove completed marker {}", marker_path.display()))?;
    sync_directory(output_dir)?;
    info!(
        "First-seen MPHF finalization complete; metadata published last: {}",
        output_dir.display()
    );
    Ok(())
}

fn recover_unmarked_scan(output_dir: &Path) -> Result<()> {
    let manifest_path = output_dir.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE);
    let manifest_candidate = if manifest_path.is_file() {
        manifest_path
    } else {
        first_seen_temp_path(&manifest_path)
    };
    let text = std::fs::read_to_string(&manifest_candidate).with_context(|| {
        format!(
            "no scan marker; read recoverable first-seen manifest {}",
            manifest_candidate.display()
        )
    })?;
    let registry_keys = manifest_field(&text, "registry_keys")
        .context("recoverable first-seen manifest is missing registry_keys")?
        .parse::<usize>()
        .context("invalid recoverable first-seen registry_keys")?;
    let references = manifest_field(&text, "references")
        .context("recoverable first-seen manifest is missing references")?
        .parse::<u64>()
        .context("invalid recoverable first-seen references")?;
    let access_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let access_index_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    anyhow::ensure!(
        access_path.exists() == access_index_path.exists(),
        "unmarked first-seen candidate has only one access sidecar: {} {}",
        access_path.display(),
        access_index_path.display(),
    );
    let marker = FirstSeenScanMarker::new(registry_keys, references, access_path.is_file())?;
    validate_scan_outputs(output_dir, marker)?;
    write_scan_complete_marker(output_dir, marker)?;
    info!(
        "Recovered complete unmarked first-seen scan: {}",
        output_dir.display()
    );
    Ok(())
}

fn read_marker(path: &Path) -> Result<FirstSeenScanMarker> {
    let mut bytes = Vec::new();
    File::open(path)
        .with_context(|| format!("open first-seen scan marker {}", path.display()))?
        .take(64 << 10)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read first-seen scan marker {}", path.display()))?;
    FirstSeenScanMarker::decode(&bytes)
        .with_context(|| format!("parse first-seen scan marker {}", path.display()))
}

fn validate_scan_outputs(output_dir: &Path, marker: FirstSeenScanMarker) -> Result<()> {
    let registry_path = output_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let expected_registry_bytes = marker
        .registry_keys
        .checked_mul(32)
        .context("first-seen registry byte size overflow")?;
    let registry_bytes = std::fs::metadata(&registry_path)
        .with_context(|| format!("stat registry {}", registry_path.display()))?
        .len();
    anyhow::ensure!(
        registry_bytes == expected_registry_bytes,
        "first-seen registry size {} != marker size {} ({} keys): {}",
        registry_bytes,
        expected_registry_bytes,
        marker.registry_keys,
        registry_path.display()
    );

    for name in [
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_POH_FILE,
        ARCHIVE_V2_SHREDDING_FILE,
    ] {
        let path = output_dir.join(name);
        anyhow::ensure!(
            crate::file_nonempty(&path),
            "first-seen scan output is missing or empty: {}",
            path.display()
        );
    }
    // These are valid empty files for a bounded fixture with no signatures or
    // compact vote hashes, or when --first-seen-seed-keys=0, but the scan must
    // still have created them.
    for name in [
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ] {
        let path = output_dir.join(name);
        anyhow::ensure!(
            path.is_file(),
            "first-seen scan output is missing: {}",
            path.display()
        );
    }
    if marker.include_access {
        for name in [
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ] {
            let path = output_dir.join(name);
            anyhow::ensure!(
                crate::file_nonempty(&path),
                "first-seen access output is missing or empty: {}",
                path.display()
            );
        }
    }

    let meta_path = output_dir.join(ARCHIVE_V2_META_FILE);
    let meta_tmp = first_seen_temp_path(&meta_path);
    anyhow::ensure!(
        crate::file_nonempty(&meta_tmp),
        "first-seen metadata temp is missing or empty: {}",
        meta_tmp.display()
    );
    let manifest_path = output_dir.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE);
    let manifest_candidate = if manifest_path.exists() {
        manifest_path
    } else {
        first_seen_temp_path(&manifest_path)
    };
    let audit = validate_manifest(&manifest_candidate, marker)?;
    validate_registry_counts_audit(output_dir, marker, audit)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ManifestReferenceAudit {
    low: u128,
    high: u128,
}

fn validate_manifest(path: &Path, marker: FirstSeenScanMarker) -> Result<ManifestReferenceAudit> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read first-seen manifest {}", path.display()))?;
    let registry_keys = manifest_field(&text, "registry_keys")
        .context("first-seen manifest is missing registry_keys")?
        .parse::<u64>()
        .context("invalid first-seen manifest registry_keys")?;
    let references = manifest_field(&text, "references")
        .context("first-seen manifest is missing references")?
        .parse::<u64>()
        .context("invalid first-seen manifest references")?;
    anyhow::ensure!(
        registry_keys == marker.registry_keys && references == marker.references,
        "first-seen manifest facts keys={registry_keys} refs={references} do not match marker keys={} refs={}",
        marker.registry_keys,
        marker.references
    );
    let fingerprint = manifest_field(&text, "fingerprint");
    let audit_kind = manifest_field(&text, "reference_audit");
    let audit_low = manifest_field(&text, "reference_audit_low");
    let audit_high = manifest_field(&text, "reference_audit_high");
    anyhow::ensure!(
        fingerprint == Some("gxhash128_random_seed_full_key_v1"),
        "unsupported first-seen fingerprint manifest value {:?}",
        fingerprint,
    );
    anyhow::ensure!(
        audit_kind == Some("wrapping_u128_halves_le_v1"),
        "unsupported first-seen reference audit manifest value {:?}",
        audit_kind,
    );
    let low = u128::from_str_radix(
        audit_low.context("first-seen manifest is missing reference_audit_low")?,
        16,
    )
    .context("invalid first-seen manifest reference_audit_low")?;
    let high = u128::from_str_radix(
        audit_high.context("first-seen manifest is missing reference_audit_high")?,
        16,
    )
    .context("invalid first-seen manifest reference_audit_high")?;
    Ok(ManifestReferenceAudit { low, high })
}

fn manifest_field<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    text.lines().find_map(|line| {
        line.strip_prefix(name)
            .and_then(|value| value.strip_prefix('='))
    })
}

fn validate_registry_counts_audit(
    output_dir: &Path,
    marker: FirstSeenScanMarker,
    expected: ManifestReferenceAudit,
) -> Result<()> {
    let registry_path = output_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let counts_path = output_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE);
    let mut registry = BufReader::with_capacity(
        8 << 20,
        File::open(&registry_path)
            .with_context(|| format!("open registry audit {}", registry_path.display()))?,
    );
    let mut counts = BufReader::with_capacity(
        8 << 20,
        File::open(&counts_path)
            .with_context(|| format!("open registry counts audit {}", counts_path.display()))?,
    );
    let mut actual = ManifestReferenceAudit { low: 0, high: 0 };
    let mut rows = 0u64;
    let mut references = 0u64;
    while let Some(count) = blockzilla_format::framed::read_u32_varint(&mut counts)
        .with_context(|| format!("read registry counts audit {}", counts_path.display()))?
    {
        anyhow::ensure!(
            rows < marker.registry_keys,
            "registry counts has more than {} marker rows",
            marker.registry_keys,
        );
        let mut key = [0u8; 32];
        registry.read_exact(&mut key).with_context(|| {
            format!(
                "read registry audit {} row {}",
                registry_path.display(),
                rows + 1,
            )
        })?;
        references = references
            .checked_add(u64::from(count))
            .context("registry reference audit sum overflow")?;
        let low = u128::from_le_bytes(key[..16].try_into().unwrap());
        let high = u128::from_le_bytes(key[16..].try_into().unwrap());
        actual.low = actual.low.wrapping_add(low.wrapping_mul(u128::from(count)));
        actual.high = actual
            .high
            .wrapping_add(high.wrapping_mul(u128::from(count)));
        rows += 1;
    }
    anyhow::ensure!(
        rows == marker.registry_keys,
        "registry counts rows {} != marker keys {}",
        rows,
        marker.registry_keys,
    );
    let mut trailing = [0u8; 1];
    anyhow::ensure!(
        registry.read(&mut trailing)? == 0,
        "registry has trailing keys beyond {} count rows",
        rows,
    );
    anyhow::ensure!(
        references == marker.references,
        "registry counts references {} != marker references {}",
        references,
        marker.references,
    );
    anyhow::ensure!(
        actual == expected,
        "registry reference audit does not match manifest: actual_low={:032x} expected_low={:032x} actual_high={:032x} expected_high={:032x}",
        actual.low,
        expected.low,
        actual.high,
        expected.high,
    );
    Ok(())
}

#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open output directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync output directory {}", path.display()))
}

#[cfg(not(unix))]
pub(crate) fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::KeyIndex;

    fn test_audit(keys: &[[u8; 32]], counts: &[u32]) -> ManifestReferenceAudit {
        let mut audit = ManifestReferenceAudit { low: 0, high: 0 };
        for (key, &count) in keys.iter().zip(counts) {
            let low = u128::from_le_bytes(key[..16].try_into().unwrap());
            let high = u128::from_le_bytes(key[16..].try_into().unwrap());
            audit.low = audit.low.wrapping_add(low.wrapping_mul(u128::from(count)));
            audit.high = audit
                .high
                .wrapping_add(high.wrapping_mul(u128::from(count)));
        }
        audit
    }

    fn test_manifest(keys: u64, references: u64, audit: ManifestReferenceAudit) -> String {
        format!(
            "version=1\nfingerprint=gxhash128_random_seed_full_key_v1\nreference_audit=wrapping_u128_halves_le_v1\nregistry_keys={keys}\nreferences={references}\nreference_audit_low={:032x}\nreference_audit_high={:032x}\n",
            audit.low, audit.high,
        )
    }

    #[test]
    fn scan_marker_round_trips_and_rejects_unknown_versions() {
        let marker = FirstSeenScanMarker::new(31_912_867, 10_003_134_293, false).unwrap();
        assert_eq!(
            FirstSeenScanMarker::decode(marker.encode().as_bytes()).unwrap(),
            marker
        );
        let error = FirstSeenScanMarker::decode(
            b"blockzilla-first-seen-scan-complete-v2\nregistry_keys=1\nreferences=2\ninclude_access=0\n",
        )
        .unwrap_err();
        assert!(error.to_string().contains("unsupported"));
    }

    #[test]
    fn manifest_validation_checks_marker_facts() {
        let root = std::env::temp_dir().join(format!(
            "blockzilla-first-seen-marker-test-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("manifest");
        std::fs::write(&path, "version=1\nregistry_keys=7\nreferences=11\n").unwrap();
        let marker = FirstSeenScanMarker::new(7, 11, false).unwrap();
        let error = validate_manifest(&path, marker).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported first-seen fingerprint")
        );
        std::fs::write(
            &path,
            test_manifest(7, 11, ManifestReferenceAudit { low: 0, high: 0 }),
        )
        .unwrap();
        validate_manifest(&path, marker).unwrap();
        let error =
            validate_manifest(&path, FirstSeenScanMarker::new(8, 11, false).unwrap()).unwrap_err();
        assert!(error.to_string().contains("do not match"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn registry_count_audit_rechecks_manifest_before_finalizing() {
        let root = std::env::temp_dir().join(format!(
            "blockzilla-first-seen-audit-test-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let keys = [[1u8; 32], [2u8; 32]];
        std::fs::write(root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), keys.concat()).unwrap();
        let mut count_bytes = Vec::new();
        blockzilla_format::framed::write_u32_varint(&mut count_bytes, 1).unwrap();
        blockzilla_format::framed::write_u32_varint(&mut count_bytes, 2).unwrap();
        let counts_path = root.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE);
        std::fs::write(&counts_path, &count_bytes).unwrap();

        let expected = test_audit(&keys, &[1, 2]);
        let marker = FirstSeenScanMarker::new(2, 3, false).unwrap();
        validate_registry_counts_audit(&root, marker, expected).unwrap();

        count_bytes.clear();
        blockzilla_format::framed::write_u32_varint(&mut count_bytes, 1).unwrap();
        blockzilla_format::framed::write_u32_varint(&mut count_bytes, 1).unwrap();
        std::fs::write(&counts_path, &count_bytes).unwrap();
        let error = validate_registry_counts_audit(&root, marker, expected).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("references 2 != marker references 3")
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn finalizer_builds_index_and_publishes_metadata_last() {
        let root = std::env::temp_dir().join(format!(
            "blockzilla-first-seen-finalize-test-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let first_key = [1u8; 32];
        let second_key = [2u8; 32];
        let mut registry = Vec::with_capacity(64);
        registry.extend_from_slice(&first_key);
        registry.extend_from_slice(&second_key);
        std::fs::write(root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), registry).unwrap();
        for name in [
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_POH_FILE,
            ARCHIVE_V2_SHREDDING_FILE,
        ] {
            std::fs::write(root.join(name), [1]).unwrap();
        }
        for name in [
            ARCHIVE_V2_SIGNATURES_FILE,
            ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
            ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
        ] {
            std::fs::write(root.join(name), []).unwrap();
        }
        let mut count_bytes = Vec::new();
        blockzilla_format::framed::write_u32_varint(&mut count_bytes, 1).unwrap();
        blockzilla_format::framed::write_u32_varint(&mut count_bytes, 2).unwrap();
        std::fs::write(
            root.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
            count_bytes,
        )
        .unwrap();

        let meta_path = root.join(ARCHIVE_V2_META_FILE);
        std::fs::write(first_seen_temp_path(&meta_path), [1]).unwrap();
        let manifest_path = root.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE);
        let audit = test_audit(&[first_key, second_key], &[1, 2]);
        std::fs::write(
            first_seen_temp_path(&manifest_path),
            test_manifest(2, 3, audit),
        )
        .unwrap();

        let lock_path = root.join("finalizer.lock");
        // A kill after the manifest was flushed but before marker publication
        // is recovered and revalidated automatically.
        finalize_first_seen_scan(&root, Some(&lock_path)).unwrap();
        assert!(meta_path.is_file());
        assert!(manifest_path.is_file());
        assert!(!root.join(FIRST_SEEN_SCAN_COMPLETE_FILE).exists());
        let index = KeyIndex::load(&root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE)).unwrap();
        assert_eq!(index.lookup(&first_key), Some(1));
        assert_eq!(index.lookup(&second_key), Some(2));

        // Finalization is safe to retry after the metadata-last commit.
        finalize_first_seen_scan(&root, Some(&lock_path)).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
