//! Crash-recoverable, bounded-memory raw ingress spool.
//!
//! This is the first durability boundary for every source. [`SpoolWriter::append_and_sync`] does
//! not return until the complete frame is visible to the filesystem and `sync_data` succeeds.
//! Upstream cursors and primary receipts may advance only after that return value is obtained.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use super::{
    dedup::{ContentDigest, IngressRecordMeta, compute_content_digest},
    replication::DurableGcAuthorization,
};

const SEGMENT_MAGIC: &[u8; 8] = b"BZIWAL01";
const FRAME_MAGIC: &[u8; 4] = b"BZIF";
const COMMIT_MAGIC: &[u8; 4] = b"CMIT";
const FRAME_VERSION: u16 = 1;
const SEGMENT_HEADER_LEN: u64 = SEGMENT_MAGIC.len() as u64;
const FRAME_FIXED_LEN: u64 = 4 + 2 + 4 + 8 + 4;
const FRAME_TRAILER_LEN: u64 = 4 + 4;
const MAX_METADATA_BYTES: usize = 64 * 1024;
const RECOVERY_BUFFER_BYTES: usize = 64 * 1024;
const RETIRED_PREFIX_MARKER_FILE: &str = ".retired-prefix.v1.json";
const RETIRED_PREFIX_MARKER_MAX_BYTES: u64 = 64 * 1024;
const RETENTION_LOCK_FILE: &str = ".retention.lock";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpoolRetiredPrefixTail {
    location: SpoolLocation,
    metadata: IngressRecordMeta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpoolRetiredPrefixMarker {
    schema_version: u32,
    identity: SpoolJournalIdentity,
    first_retained_segment_id: u64,
    acknowledged_through_sequence: u64,
    acknowledged_through_content_digest: ContentDigest,
    acknowledgement_anchor: SpoolLocation,
    /// Present in schema v2. It binds a transcoded ACK to the exact physical local frame.
    #[serde(default)]
    acknowledgement_anchor_metadata: Option<IngressRecordMeta>,
    /// Present in schema v2. It proves the removed segment's complete tail and the next retained
    /// segment must continue directly from it.
    #[serde(default)]
    retired_tail: Option<SpoolRetiredPrefixTail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpoolSegmentRetirementOutcome {
    Busy,
    NothingToRetire,
    Retired(PathBuf),
}

#[derive(Debug, Clone, Copy)]
enum RetentionLockMode {
    Shared,
    ExclusiveNonblocking,
}

#[derive(Debug)]
struct RetentionGuard {
    _file: File,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolOptions {
    /// Rotate before appending a frame that would exceed this target. One large record may exceed
    /// the target but can never exceed `max_record_bytes`.
    pub segment_target_bytes: u64,
    /// Hard safety bound applied before allocating/serializing a record.
    pub max_record_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolJournalIdentity {
    pub cluster_id: String,
    pub origin_node_id: String,
    pub source_id: String,
    pub journal_id: [u8; 16],
}

/// Unix ownership boundary required before a pull source may retire raw-spool segments.
///
/// The recorder owns its segment files, while the root control UID owns every identity
/// directory, the sticky/setgid journal leaf, and the retention control files. This prevents the
/// unprivileged recorder UID from replacing a trusted prefix marker or lock through ordinary
/// directory writes. Non-root control is intentionally unsupported until segment files are made
/// safely group-readable and covered by cross-UID tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolGcNamespacePolicy {
    pub control_uid: u32,
    pub recorder_gid: u32,
}

impl Default for SpoolOptions {
    fn default() -> Self {
        Self {
            segment_target_bytes: 256 * 1024 * 1024,
            max_record_bytes: 128 * 1024 * 1024,
        }
    }
}

impl SpoolOptions {
    pub fn validate(self) -> Result<Self> {
        ensure!(
            self.segment_target_bytes > SEGMENT_HEADER_LEN,
            "segment target must be larger than the segment header"
        );
        ensure!(
            self.max_record_bytes > 0,
            "max record bytes must be non-zero"
        );
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolLocation {
    pub segment_id: u64,
    pub frame_offset: u64,
    pub frame_len: u64,
}

/// Exact logical-file growth and durable location of a prospective spool append.
///
/// The projection applies the same validation, serialization, and segment-rotation decision as
/// [`SpoolWriter::append_and_sync`]. It does not write anything and remains valid only until the
/// writer is otherwise mutated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolAppendProjection {
    pub location: SpoolLocation,
    pub additional_bytes: u64,
}

/// Result of one group commit. Every returned record crossed the same final `sync_data` boundary;
/// callers must not publish progress for any member until this value is returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableSpoolBatch {
    pub(crate) records: Vec<DurableSpoolRecord>,
    pub(crate) additional_bytes: u64,
}

/// Proof that one raw event has crossed the local filesystem durability boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableSpoolRecord {
    location: SpoolLocation,
    metadata: IngressRecordMeta,
}

impl DurableSpoolRecord {
    /// Reconstruct a durability witness from a checksummed spool read that is independently bound
    /// to a synced commit journal. This is crate-private so arbitrary callers cannot promote an
    /// uncommitted read into deletion authority.
    pub(crate) fn from_verified_committed_read(
        location: SpoolLocation,
        metadata: IngressRecordMeta,
    ) -> Self {
        Self { location, metadata }
    }

    pub fn location(&self) -> SpoolLocation {
        self.location
    }

    pub fn metadata(&self) -> &IngressRecordMeta {
        &self.metadata
    }
}

/// One checksummed record read back from a spool segment.
///
/// Readers only retain a single payload at a time, so replay remains bounded by
/// [`SpoolOptions::max_record_bytes`] rather than the total journal size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolRecord {
    pub location: SpoolLocation,
    pub metadata: IngressRecordMeta,
    pub payload: Vec<u8>,
}

/// Result of a lock-free, read-only snapshot of a receiver spool's durable prefix.
///
/// The caller supplies the independently recovered receiver-progress sequence. This reader never
/// promotes a merely visible active-segment tail into durability: it exposes records only through
/// that already-synced progress boundary. It does not take the writer lock and never creates,
/// truncates, renames, or removes a source file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolCommittedSnapshotReport {
    pub records: u64,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub durable_through_sequence: u64,
    pub reached_durable_tail: bool,
}

/// Resolve the canonical journal directory for one spool identity without touching storage.
pub fn spool_journal_dir_path(
    spool_root: impl AsRef<Path>,
    identity: &SpoolJournalIdentity,
) -> Result<PathBuf> {
    validate_path_component(&identity.cluster_id, "cluster id")?;
    validate_path_component(&identity.origin_node_id, "origin node id")?;
    validate_path_component(&identity.source_id, "source id")?;
    Ok(spool_root
        .as_ref()
        .join(&identity.cluster_id)
        .join(&identity.origin_node_id)
        .join(&identity.source_id)
        .join(hex_journal_id(identity.journal_id)))
}

/// Validate the privilege-separated namespace required by destructive spool retention.
///
/// This deliberately requires an already-created retention lock. A recorder may lazily create the
/// cooperative lock while GC is disabled, but that state can never authorize deletion: an
/// operator must first replace it with a control-UID-owned file and protect the full identity path.
pub fn validate_spool_gc_namespace(
    spool_root: impl AsRef<Path>,
    identity: &SpoolJournalIdentity,
    policy: SpoolGcNamespacePolicy,
) -> Result<()> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (spool_root.as_ref(), identity, policy);
        bail!("raw-spool GC namespace protection requires Linux")
    }

    #[cfg(target_os = "linux")]
    {
        // SAFETY: `geteuid` has no preconditions and reads the current process credential.
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            effective_uid == policy.control_uid,
            "raw-spool GC control UID does not match the effective process UID"
        );
        ensure!(
            policy.control_uid == 0,
            "raw-spool GC currently requires the root control UID"
        );
        ensure!(
            policy.recorder_gid != 0,
            "raw-spool GC recorder group must be unprivileged"
        );
        let spool_root = spool_root.as_ref();
        ensure!(
            spool_root.is_absolute() && spool_root != Path::new("/"),
            "raw-spool GC root must be absolute and non-root"
        );
        for path in spool_root.ancestors().skip(1) {
            let metadata = fs::symlink_metadata(path).with_context(|| {
                format!("inspect raw-spool GC anchor directory {}", path.display())
            })?;
            let mode = metadata.mode();
            let recorder_can_traverse =
                mode & 0o001 != 0 || (metadata.gid() == policy.recorder_gid && mode & 0o010 != 0);
            let replacement_is_blocked = mode & 0o022 == 0 || mode & 0o1000 != 0;
            ensure!(
                metadata.is_dir()
                    && !metadata.file_type().is_symlink()
                    && (metadata.uid() == 0 || metadata.uid() == policy.control_uid)
                    && recorder_can_traverse
                    && replacement_is_blocked,
                "raw-spool GC anchor ownership or permissions are unsafe: {}",
                path.display()
            );
        }
        let journal_dir = spool_journal_dir_path(spool_root, identity)?;
        let protected_directories = [
            spool_root.to_path_buf(),
            spool_root.join(&identity.cluster_id),
            spool_root
                .join(&identity.cluster_id)
                .join(&identity.origin_node_id),
            spool_root
                .join(&identity.cluster_id)
                .join(&identity.origin_node_id)
                .join(&identity.source_id),
        ];
        for path in protected_directories {
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("inspect protected spool directory {}", path.display()))?;
            ensure!(
                metadata.is_dir()
                    && !metadata.file_type().is_symlink()
                    && metadata.uid() == policy.control_uid
                    && metadata.gid() == policy.recorder_gid
                    && metadata.mode() & 0o700 == 0o700
                    && metadata.mode() & 0o050 == 0o050
                    && metadata.mode() & 0o022 == 0,
                "protected spool directory ownership or permissions are unsafe: {}",
                path.display()
            );
        }

        let journal_metadata = fs::symlink_metadata(&journal_dir).with_context(|| {
            format!("inspect protected spool journal {}", journal_dir.display())
        })?;
        ensure!(
            journal_metadata.is_dir()
                && !journal_metadata.file_type().is_symlink()
                && journal_metadata.uid() == policy.control_uid
                && journal_metadata.gid() == policy.recorder_gid
                && journal_metadata.mode() & 0o7777 == 0o3770,
            "spool journal must be control-owned mode 03770: {}",
            journal_dir.display()
        );

        validate_gc_control_file(
            &journal_dir.join(RETENTION_LOCK_FILE),
            policy,
            "retention lock",
            false,
        )?;
        validate_gc_control_file(
            &journal_dir.join(RETIRED_PREFIX_MARKER_FILE),
            policy,
            "retired-prefix marker",
            true,
        )?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn validate_gc_control_file(
    path: &Path,
    policy: SpoolGcNamespacePolicy,
    label: &str,
    optional: bool,
) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if optional && error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect raw-spool {label} {}", path.display()));
        }
    };
    validate_gc_control_metadata(&metadata, policy, label, path)
}

#[cfg(target_os = "linux")]
fn validate_gc_control_metadata(
    metadata: &fs::Metadata,
    policy: SpoolGcNamespacePolicy,
    label: &str,
    path: &Path,
) -> Result<()> {
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.nlink() == 1
            && metadata.uid() == policy.control_uid
            && metadata.gid() == policy.recorder_gid
            && metadata.mode() & 0o7777 == 0o640,
        "raw-spool {label} ownership or permissions are unsafe: {}",
        path.display()
    );
    Ok(())
}

/// Read-only validation result that keeps the journal's exclusive writer lock held.
///
/// Holding the lock makes the reported durable tail stable for the lifetime of this value. The
/// audit never creates, truncates, or writes journal files. An incomplete frame is tolerated only
/// in the final (active) segment and is reported through [`Self::incomplete_tail_bytes`].
#[derive(Debug)]
pub struct LockedSpoolAudit {
    journal_dir: PathBuf,
    last_record: Option<DurableSpoolRecord>,
    incomplete_tail_bytes: u64,
    _retention_guard: RetentionGuard,
    _journal_lock: File,
}

impl LockedSpoolAudit {
    /// Non-blockingly lock and validate an existing spool journal without mutating it.
    pub fn open(
        spool_root: impl AsRef<Path>,
        identity: SpoolJournalIdentity,
        options: SpoolOptions,
    ) -> Result<Self> {
        let options = options.validate()?;
        validate_path_component(&identity.cluster_id, "cluster id")?;
        validate_path_component(&identity.origin_node_id, "origin node id")?;
        validate_path_component(&identity.source_id, "source id")?;
        let journal_dir = spool_root
            .as_ref()
            .join(&identity.cluster_id)
            .join(&identity.origin_node_id)
            .join(&identity.source_id)
            .join(hex_journal_id(identity.journal_id));

        let retention_guard = acquire_retention_guard(&journal_dir, RetentionLockMode::Shared)?
            .context("shared spool retention lock unexpectedly unavailable")?;

        let lock_path = journal_dir.join("writer.lock");
        let journal_lock = open_regular_file_read_only(&lock_path)?;
        try_lock_exclusive(&journal_lock, &lock_path)?;

        let segment_ids = segment_ids(&journal_dir, &identity, options.max_record_bytes)?;
        ensure!(
            !segment_ids.is_empty(),
            "spool journal has no segments: {}",
            journal_dir.display()
        );

        let mut last_record: Option<DurableSpoolRecord> = None;
        let mut incomplete_tail_bytes = 0;
        for (index, segment_id) in segment_ids.iter().copied().enumerate() {
            let path = segment_path(&journal_dir, segment_id);
            let mut file = open_regular_file_read_only(&path)?;
            let file_len = file.metadata()?.len();
            let recovered = recover_segment(
                &mut file,
                &path,
                segment_id,
                options.max_record_bytes,
                &identity,
            )?;
            let incomplete_bytes = file_len
                .checked_sub(recovered.valid_len)
                .context("spool recovery length exceeds segment length")?;
            let is_final_segment = index + 1 == segment_ids.len();
            ensure!(
                is_final_segment || incomplete_bytes == 0,
                "sealed spool segment has an incomplete tail: {}",
                path.display()
            );

            if let (Some(previous), Some(first)) =
                (last_record.as_ref(), recovered.first_record.as_ref())
            {
                ensure_record_follows(previous, first)?;
            }
            if recovered.last_record.is_some() {
                last_record = recovered.last_record;
            }
            if is_final_segment {
                incomplete_tail_bytes = incomplete_bytes;
            }
        }

        Ok(Self {
            journal_dir,
            last_record,
            incomplete_tail_bytes,
            _retention_guard: retention_guard,
            _journal_lock: journal_lock,
        })
    }

    pub fn journal_dir(&self) -> &Path {
        &self.journal_dir
    }

    pub fn last_record(&self) -> Option<&DurableSpoolRecord> {
        self.last_record.as_ref()
    }

    pub fn incomplete_tail_bytes(&self) -> u64 {
        self.incomplete_tail_bytes
    }
}

#[derive(Debug)]
pub struct SpoolWriter {
    journal_dir: PathBuf,
    identity: SpoolJournalIdentity,
    options: SpoolOptions,
    segment_id: u64,
    segment_len: u64,
    writer: BufWriter<File>,
    _journal_lock: File,
    last_record: Option<DurableSpoolRecord>,
    poisoned: bool,
}

impl SpoolWriter {
    /// Open a writer after validating every sealed segment in the journal.
    ///
    /// This is the conservative path for offline tasks. Latency grows with the complete spool,
    /// because every historical payload checksum is recomputed before appending is allowed.
    pub fn open(
        spool_root: impl AsRef<Path>,
        identity: SpoolJournalIdentity,
        options: SpoolOptions,
    ) -> Result<Self> {
        let options = options.validate()?;
        validate_path_component(&identity.cluster_id, "cluster id")?;
        validate_path_component(&identity.origin_node_id, "origin node id")?;
        validate_path_component(&identity.source_id, "source id")?;
        let journal_dir = spool_root
            .as_ref()
            .join(&identity.cluster_id)
            .join(&identity.origin_node_id)
            .join(&identity.source_id)
            .join(hex_journal_id(identity.journal_id));
        create_dir_all_durable(&journal_dir)?;

        let lock_path = journal_dir.join("writer.lock");
        let (journal_lock, lock_created) = open_regular_file(&lock_path, true)?;
        try_lock_exclusive(&journal_lock, &lock_path)?;
        if lock_created {
            sync_directory(&journal_dir)?;
        }

        let _retention_guard = acquire_retention_guard(&journal_dir, RetentionLockMode::Shared)?
            .context("shared spool retention lock unexpectedly unavailable")?;
        let segment_ids = segment_ids(&journal_dir, &identity, options.max_record_bytes)?;
        let mut last_record = None;
        if segment_ids.len() > 1 {
            for segment_id in &segment_ids[..segment_ids.len() - 1] {
                let sealed_last = validate_sealed_segment(
                    &journal_dir,
                    *segment_id,
                    options.max_record_bytes,
                    &identity,
                )?;
                if sealed_last.is_some() {
                    if let (Some(previous), Some(next)) = (&last_record, &sealed_last) {
                        ensure_record_follows(previous, next)?;
                    }
                    last_record = sealed_last;
                }
            }
        }
        let segment_id = segment_ids.last().copied().unwrap_or(0);
        let (writer, segment_len, active_last_record) = open_and_recover_segment(
            &journal_dir,
            segment_id,
            options.max_record_bytes,
            &identity,
        )?;
        if active_last_record.is_some() {
            if let (Some(previous), Some(next)) = (&last_record, &active_last_record) {
                ensure_record_follows(previous, next)?;
            }
            last_record = active_last_record;
        }
        sync_directory(&journal_dir)?;
        Ok(Self {
            journal_dir,
            identity,
            options,
            segment_id,
            segment_len,
            writer,
            _journal_lock: journal_lock,
            last_record,
            poisoned: false,
        })
    }

    /// Open a live writer from a handoff-journal checkpoint without rescanning sealed history.
    ///
    /// The checkpoint is an already-synced handoff row. Its exact WAL frame is checksummed here,
    /// then only the current segment is recovered. A full [`LockedSpoolAudit`] remains required
    /// for offline validation before materialization or deletion of source data.
    pub fn open_from_checkpoint(
        spool_root: impl AsRef<Path>,
        identity: SpoolJournalIdentity,
        options: SpoolOptions,
        checkpoint: Option<SpoolLocation>,
    ) -> Result<Self> {
        let options = options.validate()?;
        validate_path_component(&identity.cluster_id, "cluster id")?;
        validate_path_component(&identity.origin_node_id, "origin node id")?;
        validate_path_component(&identity.source_id, "source id")?;
        let journal_dir = spool_root
            .as_ref()
            .join(&identity.cluster_id)
            .join(&identity.origin_node_id)
            .join(&identity.source_id)
            .join(hex_journal_id(identity.journal_id));
        create_dir_all_durable(&journal_dir)?;

        let lock_path = journal_dir.join("writer.lock");
        let (journal_lock, lock_created) = open_regular_file(&lock_path, true)?;
        try_lock_exclusive(&journal_lock, &lock_path)?;
        if lock_created {
            sync_directory(&journal_dir)?;
        }

        let _retention_guard = acquire_retention_guard(&journal_dir, RetentionLockMode::Shared)?
            .context("shared spool retention lock unexpectedly unavailable")?;
        let segment_ids = segment_ids(&journal_dir, &identity, options.max_record_bytes)?;
        ensure!(
            checkpoint.is_some() || segment_ids.len() <= 1,
            "a handoff checkpoint is required to resume a multi-segment spool; run the offline raw-spool audit"
        );
        let active_segment_id = segment_ids.last().copied().unwrap_or(0);
        let checkpoint_record = checkpoint
            .map(|location| {
                ensure!(
                    segment_ids.binary_search(&location.segment_id).is_ok(),
                    "handoff checkpoint references missing spool segment {}",
                    location.segment_id
                );
                ensure!(
                    active_segment_id == location.segment_id
                        || active_segment_id == location.segment_id.saturating_add(1),
                    "active spool segment {} is not adjacent to checkpoint segment {}",
                    active_segment_id,
                    location.segment_id
                );
                let stored = read_spool_record(&journal_dir, location, options.max_record_bytes)
                    .context("validate handoff checkpoint WAL frame")?;
                ensure_record_matches_identity(&stored.metadata, &identity)?;
                if active_segment_id != location.segment_id {
                    let checkpoint_end = location
                        .frame_offset
                        .checked_add(location.frame_len)
                        .context("handoff checkpoint frame end overflow")?;
                    let checkpoint_len =
                        fs::metadata(segment_path(&journal_dir, location.segment_id))?.len();
                    ensure!(
                        checkpoint_end == checkpoint_len,
                        "handoff checkpoint does not end at sealed segment boundary"
                    );
                }
                Ok(DurableSpoolRecord {
                    location: stored.location,
                    metadata: stored.metadata,
                })
            })
            .transpose()?;

        let (writer, segment_len, recovered_last) = open_and_recover_segment(
            &journal_dir,
            active_segment_id,
            options.max_record_bytes,
            &identity,
        )?;
        let last_record = match (checkpoint_record, recovered_last) {
            (None, recovered_last) => recovered_last,
            (Some(checkpoint), recovered_last) => {
                if active_segment_id != checkpoint.location.segment_id {
                    if let Some(last) = recovered_last.as_ref() {
                        ensure_record_follows(&checkpoint, last)?;
                    }
                }
                if let Some(last) = recovered_last.as_ref() {
                    let checkpoint_sequence = checkpoint.metadata.observation.sequence;
                    let maximum_sequence = checkpoint_sequence
                        .checked_add(1)
                        .context("handoff checkpoint sequence overflow")?;
                    ensure!(
                        last.metadata.observation.sequence >= checkpoint_sequence
                            && last.metadata.observation.sequence <= maximum_sequence,
                        "WAL is more than one frame ahead of handoff checkpoint"
                    );
                }
                recovered_last.or(Some(checkpoint))
            }
        };
        sync_directory(&journal_dir)?;
        Ok(Self {
            journal_dir,
            identity,
            options,
            segment_id: active_segment_id,
            segment_len,
            writer,
            _journal_lock: journal_lock,
            last_record,
            poisoned: false,
        })
    }

    pub fn journal_dir(&self) -> &Path {
        &self.journal_dir
    }

    pub fn current_segment_id(&self) -> u64 {
        self.segment_id
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Last complete, checksummed record recovered or appended in this journal.
    pub fn last_record(&self) -> Option<&DurableSpoolRecord> {
        self.last_record.as_ref()
    }

    /// Read and validate one durable record without scanning or retaining the rest of the spool.
    pub fn read_record(&self, record: &DurableSpoolRecord) -> Result<SpoolRecord> {
        let loaded = read_spool_record(
            &self.journal_dir,
            record.location,
            self.options.max_record_bytes,
        )?;
        ensure!(
            loaded.metadata == record.metadata,
            "spool record metadata changed at segment {} offset {}",
            record.location.segment_id,
            record.location.frame_offset
        );
        Ok(loaded)
    }

    /// Project one append without changing the journal.
    pub fn project_append(
        &self,
        metadata: &IngressRecordMeta,
        payload: &[u8],
    ) -> Result<SpoolAppendProjection> {
        let prepared = self.prepare_append(metadata, payload)?;
        if self.should_rotate(prepared.frame_len) {
            let segment_id = self
                .segment_id
                .checked_add(1)
                .context("spool segment id overflow")?;
            let additional_bytes = SEGMENT_HEADER_LEN
                .checked_add(prepared.frame_len)
                .context("projected spool append length overflow")?;
            Ok(SpoolAppendProjection {
                location: SpoolLocation {
                    segment_id,
                    frame_offset: SEGMENT_HEADER_LEN,
                    frame_len: prepared.frame_len,
                },
                additional_bytes,
            })
        } else {
            Ok(SpoolAppendProjection {
                location: SpoolLocation {
                    segment_id: self.segment_id,
                    frame_offset: self.segment_len,
                    frame_len: prepared.frame_len,
                },
                additional_bytes: prepared.frame_len,
            })
        }
    }

    /// Project the exact journal growth of a group commit without writing it.
    pub(crate) fn project_batch_additional_bytes(
        &self,
        records: &[(IngressRecordMeta, Vec<u8>)],
    ) -> Result<u64> {
        ensure!(!records.is_empty(), "spool append batch must not be empty");
        let mut previous = self.last_record.as_ref().map(DurableSpoolRecord::metadata);
        let mut segment_len = self.segment_len;
        let mut additional_bytes = 0u64;
        for (metadata, payload) in records {
            let prepared = self.prepare_append_after(previous, metadata, payload)?;
            if segment_len > SEGMENT_HEADER_LEN
                && segment_len.saturating_add(prepared.frame_len)
                    > self.options.segment_target_bytes
            {
                additional_bytes = additional_bytes
                    .checked_add(SEGMENT_HEADER_LEN)
                    .context("projected spool batch growth overflow")?;
                segment_len = SEGMENT_HEADER_LEN;
            }
            additional_bytes = additional_bytes
                .checked_add(prepared.frame_len)
                .context("projected spool batch growth overflow")?;
            segment_len = segment_len
                .checked_add(prepared.frame_len)
                .context("projected spool batch segment length overflow")?;
            previous = Some(metadata);
        }
        Ok(additional_bytes)
    }

    /// Append one complete event and sync it before returning a durability token.
    pub fn append_and_sync(
        &mut self,
        metadata: IngressRecordMeta,
        payload: &[u8],
    ) -> Result<DurableSpoolRecord> {
        let prepared = self.prepare_append(&metadata, payload)?;
        let PreparedSpoolAppend {
            metadata_bytes,
            metadata_len,
            frame_len,
        } = prepared;

        // From this point any error is ambiguous: bytes may have reached the file or stable
        // storage. Keep the writer fail-stop until the journal is reopened and recovered.
        self.poisoned = true;

        if self.should_rotate(frame_len) {
            self.rotate()?;
        }

        let durable = self.write_prepared_frame(
            metadata,
            payload,
            PreparedSpoolAppend {
                metadata_bytes,
                metadata_len,
                frame_len,
            },
        )?;
        self.writer.flush().context("flush spool segment")?;
        self.writer
            .get_ref()
            .sync_data()
            .context("sync spool segment")?;
        self.poisoned = false;
        self.last_record = Some(durable.clone());
        Ok(durable)
    }

    /// Append a bounded group and cross one durability boundary for the whole group. Rotation
    /// still syncs the sealed segment before opening the next one.
    pub(crate) fn append_batch_and_sync(
        &mut self,
        records: Vec<(IngressRecordMeta, Vec<u8>)>,
    ) -> Result<DurableSpoolBatch> {
        ensure!(!records.is_empty(), "spool append batch must not be empty");
        let additional_bytes = self.project_batch_additional_bytes(&records)?;
        let mut previous = self.last_record.as_ref().map(DurableSpoolRecord::metadata);
        let mut prepared = Vec::with_capacity(records.len());
        for (metadata, payload) in &records {
            prepared.push(self.prepare_append_after(previous, metadata, payload)?);
            previous = Some(metadata);
        }

        self.poisoned = true;
        let mut durable_records = Vec::with_capacity(records.len());
        for ((metadata, payload), prepared) in records.into_iter().zip(prepared) {
            if self.should_rotate(prepared.frame_len) {
                self.rotate()?;
            }
            let durable = self.write_prepared_frame(metadata, &payload, prepared)?;
            self.last_record = Some(durable.clone());
            durable_records.push(durable);
        }
        self.writer.flush().context("flush spool append batch")?;
        self.writer
            .get_ref()
            .sync_data()
            .context("sync spool append batch")?;
        self.poisoned = false;
        Ok(DurableSpoolBatch {
            records: durable_records,
            additional_bytes,
        })
    }

    fn prepare_append(
        &self,
        metadata: &IngressRecordMeta,
        payload: &[u8],
    ) -> Result<PreparedSpoolAppend> {
        self.prepare_append_after(
            self.last_record.as_ref().map(DurableSpoolRecord::metadata),
            metadata,
            payload,
        )
    }

    fn prepare_append_after(
        &self,
        previous: Option<&IngressRecordMeta>,
        metadata: &IngressRecordMeta,
        payload: &[u8],
    ) -> Result<PreparedSpoolAppend> {
        ensure!(
            !self.poisoned,
            "spool writer is poisoned; reopen it to recover before appending"
        );
        ensure!(
            metadata.cluster_id == self.identity.cluster_id,
            "metadata cluster id {:?} does not match spool cluster {:?}",
            metadata.cluster_id,
            self.identity.cluster_id
        );
        ensure!(
            metadata.observation.origin_node_id == self.identity.origin_node_id,
            "metadata origin node id {:?} does not match spool origin {:?}",
            metadata.observation.origin_node_id,
            self.identity.origin_node_id
        );
        ensure!(
            metadata.source_id == self.identity.source_id,
            "metadata source id {:?} does not match spool source {:?}",
            metadata.source_id,
            self.identity.source_id
        );
        ensure!(
            metadata.observation.journal_id == self.identity.journal_id,
            "metadata journal id does not match spool journal"
        );
        ensure!(
            metadata.payload_len == payload.len() as u64,
            "metadata payload length {} does not match actual payload length {}",
            metadata.payload_len,
            payload.len()
        );
        ensure!(
            metadata.payload_len <= self.options.max_record_bytes,
            "ingress record {} bytes exceeds configured maximum {}",
            metadata.payload_len,
            self.options.max_record_bytes
        );
        ensure!(
            metadata.content_digest
                == compute_content_digest(
                    &metadata.cluster_id,
                    &metadata.logical_key,
                    metadata.payload_format_version,
                    payload,
                ),
            "metadata content digest does not match canonical payload digest"
        );
        if let Some(previous) = previous {
            ensure_observation_follows(previous, metadata)?;
        }
        let metadata_bytes = serde_json::to_vec(&metadata).context("encode ingress metadata")?;
        ensure!(
            metadata_bytes.len() <= MAX_METADATA_BYTES,
            "ingress metadata exceeds {} bytes",
            MAX_METADATA_BYTES
        );
        let metadata_len =
            u32::try_from(metadata_bytes.len()).context("ingress metadata length exceeds u32")?;
        let frame_len = FRAME_FIXED_LEN
            .checked_add(metadata_bytes.len() as u64)
            .and_then(|len| len.checked_add(payload.len() as u64))
            .and_then(|len| len.checked_add(FRAME_TRAILER_LEN))
            .context("spool frame length overflow")?;
        Ok(PreparedSpoolAppend {
            metadata_bytes,
            metadata_len,
            frame_len,
        })
    }

    fn write_prepared_frame(
        &mut self,
        metadata: IngressRecordMeta,
        payload: &[u8],
        prepared: PreparedSpoolAppend,
    ) -> Result<DurableSpoolRecord> {
        let PreparedSpoolAppend {
            metadata_bytes,
            metadata_len,
            frame_len,
        } = prepared;
        let frame_offset = self.segment_len;
        let version_bytes = FRAME_VERSION.to_le_bytes();
        let metadata_len_bytes = metadata_len.to_le_bytes();
        let payload_len_bytes = metadata.payload_len.to_le_bytes();
        let mut header_crc = Crc32c::new();
        header_crc.update(FRAME_MAGIC);
        header_crc.update(&version_bytes);
        header_crc.update(&metadata_len_bytes);
        header_crc.update(&payload_len_bytes);
        let mut payload_crc = Crc32c::new();
        payload_crc.update(&metadata_bytes);
        payload_crc.update(payload);

        self.writer
            .write_all(FRAME_MAGIC)
            .context("write spool frame magic")?;
        self.writer
            .write_all(&version_bytes)
            .context("write spool frame version")?;
        self.writer
            .write_all(&metadata_len_bytes)
            .context("write spool metadata length")?;
        self.writer
            .write_all(&payload_len_bytes)
            .context("write spool payload length")?;
        self.writer
            .write_all(&header_crc.finish().to_le_bytes())
            .context("write spool frame header checksum")?;
        self.writer
            .write_all(&metadata_bytes)
            .context("write spool metadata")?;
        self.writer
            .write_all(payload)
            .context("write spool payload")?;
        self.writer
            .write_all(&payload_crc.finish().to_le_bytes())
            .context("write spool frame checksum")?;
        self.writer
            .write_all(COMMIT_MAGIC)
            .context("write spool commit marker")?;
        self.segment_len = self
            .segment_len
            .checked_add(frame_len)
            .context("spool segment length overflow")?;
        Ok(DurableSpoolRecord {
            location: SpoolLocation {
                segment_id: self.segment_id,
                frame_offset,
                frame_len,
            },
            metadata,
        })
    }

    fn should_rotate(&self, frame_len: u64) -> bool {
        self.segment_len > SEGMENT_HEADER_LEN
            && self.segment_len.saturating_add(frame_len) > self.options.segment_target_bytes
    }

    fn rotate(&mut self) -> Result<()> {
        self.writer.flush().context("flush spool before rotation")?;
        self.writer
            .get_ref()
            .sync_data()
            .context("sync spool before rotation")?;
        let new_segment_id = self
            .segment_id
            .checked_add(1)
            .context("spool segment id overflow")?;
        let (writer, segment_len, last_record) = open_and_recover_segment(
            &self.journal_dir,
            new_segment_id,
            self.options.max_record_bytes,
            &self.identity,
        )?;
        sync_directory(&self.journal_dir)?;
        self.segment_id = new_segment_id;
        self.writer = writer;
        self.segment_len = segment_len;
        if last_record.is_some() {
            self.last_record = last_record;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PreparedSpoolAppend {
    metadata_bytes: Vec<u8>,
    metadata_len: u32,
    frame_len: u64,
}

fn open_and_recover_segment(
    journal_dir: &Path,
    segment_id: u64,
    max_record_bytes: u64,
    identity: &SpoolJournalIdentity,
) -> Result<(BufWriter<File>, u64, Option<DurableSpoolRecord>)> {
    let path = segment_path(journal_dir, segment_id);
    let (mut file, _created) = open_regular_file(&path, true)?;
    initialize_segment_header(&mut file, &path)?;
    let file_len = file.metadata()?.len();
    let recovered = recover_segment(&mut file, &path, segment_id, max_record_bytes, identity)?;
    let valid_len = recovered.valid_len;
    if valid_len != file_len {
        file.set_len(valid_len)
            .with_context(|| format!("truncate spool segment {}", path.display()))?;
        file.sync_data()
            .with_context(|| format!("sync recovered spool segment {}", path.display()))?;
    }
    file.seek(SeekFrom::End(0))
        .with_context(|| format!("seek spool segment {}", path.display()))?;
    Ok((BufWriter::new(file), valid_len, recovered.last_record))
}

fn initialize_segment_header(file: &mut File, path: &Path) -> Result<()> {
    let len = file.metadata()?.len();
    if len >= SEGMENT_HEADER_LEN {
        return Ok(());
    }
    let mut existing = vec![0u8; len as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut existing)?;
    ensure!(
        SEGMENT_MAGIC.starts_with(&existing),
        "refusing non-spool file with invalid partial header: {}",
        path.display()
    );
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(SEGMENT_MAGIC)
        .with_context(|| format!("write spool segment header {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("sync spool segment header {}", path.display()))
}

fn validate_sealed_segment(
    journal_dir: &Path,
    segment_id: u64,
    max_record_bytes: u64,
    identity: &SpoolJournalIdentity,
) -> Result<Option<DurableSpoolRecord>> {
    let path = segment_path(journal_dir, segment_id);
    let (mut file, created) = open_regular_file(&path, false)?;
    ensure!(!created, "sealed spool segment unexpectedly created");
    let file_len = file.metadata()?.len();
    let recovered = recover_segment(&mut file, &path, segment_id, max_record_bytes, identity)?;
    let valid_len = recovered.valid_len;
    ensure!(
        valid_len == file_len,
        "sealed spool segment has an incomplete tail: {}",
        path.display()
    );
    Ok(recovered.last_record)
}

#[derive(Debug)]
struct RecoveredSegment {
    valid_len: u64,
    first_record: Option<DurableSpoolRecord>,
    last_record: Option<DurableSpoolRecord>,
    exact_sequence_contiguous: bool,
}

fn recover_segment(
    file: &mut File,
    path: &Path,
    segment_id: u64,
    max_record_bytes: u64,
    identity: &SpoolJournalIdentity,
) -> Result<RecoveredSegment> {
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("seek spool segment {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut segment_magic = [0u8; 8];
    reader
        .read_exact(&mut segment_magic)
        .with_context(|| format!("read spool segment header {}", path.display()))?;
    ensure!(
        &segment_magic == SEGMENT_MAGIC,
        "invalid spool segment header in {}",
        path.display()
    );

    let mut valid_len = SEGMENT_HEADER_LEN;
    let mut first_record = None;
    let mut last_record = None;
    let mut exact_sequence_contiguous = true;
    loop {
        let frame_offset = valid_len;
        let mut frame_magic = [0u8; 4];
        if !read_exact_or_incomplete_tail(&mut reader, &mut frame_magic, path)? {
            break;
        }
        ensure!(
            &frame_magic == FRAME_MAGIC,
            "corrupt spool frame magic at {} in {}",
            frame_offset,
            path.display()
        );

        let mut version_bytes = [0u8; 2];
        if !read_exact_or_incomplete_tail(&mut reader, &mut version_bytes, path)? {
            break;
        }
        let version = u16::from_le_bytes(version_bytes);
        ensure!(
            version == FRAME_VERSION,
            "unsupported spool frame version {version} at {} in {}",
            frame_offset,
            path.display()
        );

        let mut metadata_len_bytes = [0u8; 4];
        if !read_exact_or_incomplete_tail(&mut reader, &mut metadata_len_bytes, path)? {
            break;
        }
        let metadata_len = u32::from_le_bytes(metadata_len_bytes) as usize;
        ensure!(
            metadata_len <= MAX_METADATA_BYTES,
            "spool metadata length {} exceeds maximum at {} in {}",
            metadata_len,
            frame_offset,
            path.display()
        );

        let mut payload_len_bytes = [0u8; 8];
        if !read_exact_or_incomplete_tail(&mut reader, &mut payload_len_bytes, path)? {
            break;
        }
        let payload_len = u64::from_le_bytes(payload_len_bytes);

        let mut expected_header_crc_bytes = [0u8; 4];
        if !read_exact_or_incomplete_tail(&mut reader, &mut expected_header_crc_bytes, path)? {
            break;
        }
        let mut header_crc = Crc32c::new();
        header_crc.update(&frame_magic);
        header_crc.update(&version_bytes);
        header_crc.update(&metadata_len_bytes);
        header_crc.update(&payload_len_bytes);
        ensure!(
            header_crc.finish() == u32::from_le_bytes(expected_header_crc_bytes),
            "spool frame header checksum mismatch at {} in {}",
            frame_offset,
            path.display()
        );

        ensure!(
            payload_len <= max_record_bytes,
            "spool payload length {} exceeds configured maximum at {} in {}",
            payload_len,
            frame_offset,
            path.display()
        );

        let mut metadata_bytes = vec![0u8; metadata_len];
        if !read_exact_or_incomplete_tail(&mut reader, &mut metadata_bytes, path)? {
            break;
        }
        let metadata: IngressRecordMeta =
            serde_json::from_slice(&metadata_bytes).with_context(|| {
                format!(
                    "decode spool metadata at {} in {}",
                    frame_offset,
                    path.display()
                )
            })?;
        ensure!(
            metadata.cluster_id == identity.cluster_id
                && metadata.observation.origin_node_id == identity.origin_node_id
                && metadata.source_id == identity.source_id
                && metadata.observation.journal_id == identity.journal_id,
            "spool record identity does not match journal path at {} in {}",
            frame_offset,
            path.display()
        );
        ensure!(
            metadata.payload_len == payload_len,
            "spool metadata/payload length mismatch at {} in {}",
            frame_offset,
            path.display()
        );

        let mut crc = Crc32c::new();
        crc.update(&metadata_bytes);
        let mut remaining = payload_len;
        let mut buffer = [0u8; RECOVERY_BUFFER_BYTES];
        while remaining > 0 {
            let chunk = remaining.min(buffer.len() as u64) as usize;
            if !read_exact_or_incomplete_tail(&mut reader, &mut buffer[..chunk], path)? {
                return Ok(RecoveredSegment {
                    valid_len,
                    first_record,
                    last_record,
                    exact_sequence_contiguous,
                });
            }
            crc.update(&buffer[..chunk]);
            remaining -= chunk as u64;
        }

        let mut expected_crc_bytes = [0u8; 4];
        if !read_exact_or_incomplete_tail(&mut reader, &mut expected_crc_bytes, path)? {
            break;
        }
        let expected_crc = u32::from_le_bytes(expected_crc_bytes);
        ensure!(
            crc.finish() == expected_crc,
            "spool checksum mismatch at {} in {}",
            frame_offset,
            path.display()
        );

        let mut commit_magic = [0u8; 4];
        if !read_exact_or_incomplete_tail(&mut reader, &mut commit_magic, path)? {
            break;
        }
        ensure!(
            &commit_magic == COMMIT_MAGIC,
            "missing spool commit marker at {} in {}",
            frame_offset,
            path.display()
        );

        let frame_len = FRAME_FIXED_LEN
            .checked_add(metadata_len as u64)
            .and_then(|len| len.checked_add(payload_len))
            .and_then(|len| len.checked_add(FRAME_TRAILER_LEN))
            .context("spool recovery frame length overflow")?;
        valid_len = valid_len
            .checked_add(FRAME_FIXED_LEN)
            .and_then(|len| len.checked_add(metadata_len as u64))
            .and_then(|len| len.checked_add(payload_len))
            .and_then(|len| len.checked_add(FRAME_TRAILER_LEN))
            .context("spool recovery length overflow")?;
        let recovered_record = DurableSpoolRecord {
            location: SpoolLocation {
                segment_id,
                frame_offset,
                frame_len,
            },
            metadata,
        };
        if let Some(previous) = last_record.as_ref() {
            ensure_record_follows(previous, &recovered_record)?;
            if previous.metadata.observation.sequence.checked_add(1)
                != Some(recovered_record.metadata.observation.sequence)
            {
                exact_sequence_contiguous = false;
            }
        }
        if first_record.is_none() {
            first_record = Some(recovered_record.clone());
        }
        last_record = Some(recovered_record);
    }
    Ok(RecoveredSegment {
        valid_len,
        first_record,
        last_record,
        exact_sequence_contiguous,
    })
}

fn ensure_record_follows(previous: &DurableSpoolRecord, next: &DurableSpoolRecord) -> Result<()> {
    ensure_observation_follows(&previous.metadata, &next.metadata).with_context(|| {
        format!(
            "invalid observation at segment {} offset {}",
            next.location.segment_id, next.location.frame_offset
        )
    })
}

fn ensure_record_matches_identity(
    metadata: &IngressRecordMeta,
    identity: &SpoolJournalIdentity,
) -> Result<()> {
    ensure!(
        metadata.cluster_id == identity.cluster_id
            && metadata.observation.origin_node_id == identity.origin_node_id
            && metadata.source_id == identity.source_id
            && metadata.observation.journal_id == identity.journal_id,
        "handoff checkpoint identity does not match spool journal"
    );
    Ok(())
}

fn ensure_observation_follows(
    previous: &IngressRecordMeta,
    next: &IngressRecordMeta,
) -> Result<()> {
    ensure!(
        next.observation.sequence >= previous.observation.sequence,
        "observation sequence {} moved backward from {}",
        next.observation.sequence,
        previous.observation.sequence
    );
    if next.observation.sequence == previous.observation.sequence {
        ensure!(
            next == previous,
            "observation sequence {} was reused with different metadata/content",
            next.observation.sequence
        );
    }
    Ok(())
}

/// Read one independently checksummed spool frame by durable location.
///
/// This is the replay primitive used by source adapters. It never scans neighboring records and
/// allocates at most one configured payload plus its small JSON metadata envelope.
pub fn read_spool_record(
    journal_dir: impl AsRef<Path>,
    location: SpoolLocation,
    max_record_bytes: u64,
) -> Result<SpoolRecord> {
    let journal_dir = journal_dir.as_ref();
    let path = segment_path(journal_dir, location.segment_id);
    let mut file = open_regular_file_read_only(&path)?;
    file.seek(SeekFrom::Start(location.frame_offset))
        .with_context(|| format!("seek spool frame in {}", path.display()))?;

    let header =
        read_spool_frame_header(&mut file, &path, location.frame_offset, max_record_bytes)?;
    read_spool_frame_body(&mut file, &path, location, header)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpoolFrameHeader {
    metadata_len: usize,
    payload_len: u64,
    frame_len: u64,
}

/// Read and validate a frame header at the reader's current position.
///
/// Keeping this separate from the body lets snapshot scans determine whether a complete frame was
/// visible in their snapshotted segment length before allocating its metadata or payload. The
/// reader is left immediately after the fixed-width header on success.
fn read_spool_frame_header<R: Read>(
    reader: &mut R,
    path: &Path,
    frame_offset: u64,
    max_record_bytes: u64,
) -> Result<SpoolFrameHeader> {
    let mut frame_magic = [0u8; 4];
    reader
        .read_exact(&mut frame_magic)
        .with_context(|| format!("read spool frame magic in {}", path.display()))?;
    ensure!(
        &frame_magic == FRAME_MAGIC,
        "corrupt spool frame magic at {} in {}",
        frame_offset,
        path.display()
    );
    let mut version_bytes = [0u8; 2];
    let mut metadata_len_bytes = [0u8; 4];
    let mut payload_len_bytes = [0u8; 8];
    let mut expected_header_crc_bytes = [0u8; 4];
    reader.read_exact(&mut version_bytes)?;
    reader.read_exact(&mut metadata_len_bytes)?;
    reader.read_exact(&mut payload_len_bytes)?;
    reader.read_exact(&mut expected_header_crc_bytes)?;
    ensure!(
        u16::from_le_bytes(version_bytes) == FRAME_VERSION,
        "unsupported spool frame version at {} in {}",
        frame_offset,
        path.display()
    );
    let metadata_len = u32::from_le_bytes(metadata_len_bytes) as usize;
    let payload_len = u64::from_le_bytes(payload_len_bytes);
    ensure!(
        metadata_len <= MAX_METADATA_BYTES,
        "spool metadata length exceeds maximum at {} in {}",
        frame_offset,
        path.display()
    );
    ensure!(
        payload_len <= max_record_bytes,
        "spool payload length {} exceeds configured maximum {}",
        payload_len,
        max_record_bytes
    );
    let frame_len = FRAME_FIXED_LEN
        .checked_add(metadata_len as u64)
        .and_then(|len| len.checked_add(payload_len))
        .and_then(|len| len.checked_add(FRAME_TRAILER_LEN))
        .context("spool replay frame length overflow")?;

    let mut header_crc = Crc32c::new();
    header_crc.update(&frame_magic);
    header_crc.update(&version_bytes);
    header_crc.update(&metadata_len_bytes);
    header_crc.update(&payload_len_bytes);
    ensure!(
        header_crc.finish() == u32::from_le_bytes(expected_header_crc_bytes),
        "spool frame header checksum mismatch at {} in {}",
        frame_offset,
        path.display()
    );

    Ok(SpoolFrameHeader {
        metadata_len,
        payload_len,
        frame_len,
    })
}

/// Read and validate a frame body at the reader's current position.
///
/// `header` must have been produced by [`read_spool_frame_header`] from the immediately preceding
/// bytes. On success the reader is positioned exactly at the next frame boundary.
fn read_spool_frame_body<R: Read>(
    reader: &mut R,
    path: &Path,
    location: SpoolLocation,
    header: SpoolFrameHeader,
) -> Result<SpoolRecord> {
    let SpoolFrameHeader {
        metadata_len,
        payload_len,
        frame_len,
    } = header;
    ensure!(
        frame_len == location.frame_len,
        "spool frame length mismatch at {} in {}: journal {}, encoded {}",
        location.frame_offset,
        path.display(),
        location.frame_len,
        frame_len
    );

    let mut metadata_bytes = vec![0u8; metadata_len];
    reader.read_exact(&mut metadata_bytes)?;
    let metadata: IngressRecordMeta =
        serde_json::from_slice(&metadata_bytes).with_context(|| {
            format!(
                "decode spool metadata at {} in {}",
                location.frame_offset,
                path.display()
            )
        })?;
    ensure!(
        metadata.payload_len == payload_len,
        "spool metadata/payload length mismatch at {} in {}",
        location.frame_offset,
        path.display()
    );
    let payload_len_usize = usize::try_from(payload_len).context("spool payload exceeds usize")?;
    let mut payload = vec![0u8; payload_len_usize];
    reader.read_exact(&mut payload)?;
    let mut expected_crc_bytes = [0u8; 4];
    let mut commit_magic = [0u8; 4];
    reader.read_exact(&mut expected_crc_bytes)?;
    reader.read_exact(&mut commit_magic)?;
    let mut crc = Crc32c::new();
    crc.update(&metadata_bytes);
    crc.update(&payload);
    ensure!(
        crc.finish() == u32::from_le_bytes(expected_crc_bytes),
        "spool checksum mismatch at {} in {}",
        location.frame_offset,
        path.display()
    );
    ensure!(
        &commit_magic == COMMIT_MAGIC,
        "missing spool commit marker at {} in {}",
        location.frame_offset,
        path.display()
    );
    ensure!(
        metadata.content_digest
            == compute_content_digest(
                &metadata.cluster_id,
                &metadata.logical_key,
                metadata.payload_format_version,
                &payload,
            ),
        "spool content digest mismatch at {} in {}",
        location.frame_offset,
        path.display()
    );
    Ok(SpoolRecord {
        location,
        metadata,
        payload,
    })
}

/// Visit a bounded suffix of the receiver spool through an independently durable progress cursor.
///
/// `after` is the last source location already materialized by the caller. When present, the
/// referenced frame is fully revalidated before the scan skips past it. `durable_through_sequence`
/// must come from [`crate::ingest::read_receiver_durable_progress`], not from a slot, heartbeat,
/// R2 upload, or indexer watermark. The callback sees exact compressed bytes one record at a time.
///
/// This function is deliberately compatible with an active writer. Segment names and lengths are
/// snapshotted read-only; an incomplete visible writer tail is ignored unless the supplied durable
/// progress cursor says that record must already exist, which is treated as corruption. Each
/// segment descriptor stays open while its frames are validated sequentially.
pub fn read_spool_committed_snapshot_after<F>(
    spool_root: impl AsRef<Path>,
    identity: SpoolJournalIdentity,
    max_record_bytes: u64,
    after: Option<SpoolLocation>,
    durable_through_sequence: u64,
    max_records: usize,
    mut visit: F,
) -> Result<SpoolCommittedSnapshotReport>
where
    F: FnMut(SpoolRecord) -> Result<()>,
{
    ensure!(
        max_record_bytes > 0,
        "spool snapshot record-byte limit must be non-zero"
    );
    ensure!(
        max_records > 0,
        "spool snapshot record limit must be non-zero"
    );
    let journal_dir = spool_journal_dir_path(spool_root, &identity)?;
    let _retention_guard = acquire_retention_guard(&journal_dir, RetentionLockMode::Shared)?
        .context("shared spool retention lock unexpectedly unavailable")?;
    let segment_ids = segment_ids(&journal_dir, &identity, max_record_bytes)?;
    ensure!(
        !segment_ids.is_empty(),
        "spool journal has no segments: {}",
        journal_dir.display()
    );

    let mut previous = match after {
        Some(location) => {
            let record = read_spool_record(&journal_dir, location, max_record_bytes)
                .context("validate receiver spool resume location")?;
            ensure_spool_record_identity(&record, &identity)?;
            ensure!(
                record.metadata.observation.sequence <= durable_through_sequence,
                "receiver spool resume location is beyond durable progress"
            );
            Some(record.metadata)
        }
        None => None,
    };
    if previous
        .as_ref()
        .is_some_and(|metadata| metadata.observation.sequence == durable_through_sequence)
    {
        return Ok(SpoolCommittedSnapshotReport {
            records: 0,
            first_sequence: None,
            last_sequence: previous
                .as_ref()
                .map(|metadata| metadata.observation.sequence),
            durable_through_sequence,
            reached_durable_tail: true,
        });
    }

    let mut report = SpoolCommittedSnapshotReport {
        records: 0,
        first_sequence: None,
        last_sequence: previous
            .as_ref()
            .map(|metadata| metadata.observation.sequence),
        durable_through_sequence,
        reached_durable_tail: false,
    };
    let mut stopped_at_limit = false;
    for (segment_index, segment_id) in segment_ids.iter().copied().enumerate() {
        if after.is_some_and(|location| segment_id < location.segment_id) {
            continue;
        }
        let path = segment_path(&journal_dir, segment_id);
        let file = open_regular_file_read_only(&path)?;
        let snapshot_len = file
            .metadata()
            .with_context(|| format!("inspect spool segment {}", path.display()))?
            .len();
        // A raw-shred journal contains hundreds of millions of small frames. Buffer the one
        // descriptor we keep open per segment so the fixed header/body reads below do not turn
        // into several kernel reads for every shred.
        let mut file = BufReader::with_capacity(1024 * 1024, file);
        ensure!(
            snapshot_len >= SEGMENT_HEADER_LEN,
            "spool segment is shorter than its header: {}",
            path.display()
        );
        let mut segment_magic = [0u8; 8];
        file.read_exact(&mut segment_magic)
            .with_context(|| format!("read spool segment header {}", path.display()))?;
        ensure!(
            &segment_magic == SEGMENT_MAGIC,
            "invalid spool segment header in {}",
            path.display()
        );
        let mut offset = match after.filter(|location| location.segment_id == segment_id) {
            Some(location) => location
                .frame_offset
                .checked_add(location.frame_len)
                .context("receiver spool resume location overflow")?,
            None => SEGMENT_HEADER_LEN,
        };
        ensure!(
            offset >= SEGMENT_HEADER_LEN && offset <= snapshot_len,
            "receiver spool resume location is outside {}",
            path.display()
        );
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("seek spool snapshot {}", path.display()))?;

        while offset < snapshot_len && report.records < max_records as u64 {
            let remaining = snapshot_len - offset;
            if remaining < FRAME_FIXED_LEN {
                let is_final_segment = segment_index + 1 == segment_ids.len();
                ensure!(
                    is_final_segment,
                    "sealed spool segment has an incomplete frame header: {}",
                    path.display()
                );
                break;
            }

            let header = read_spool_frame_header(&mut file, &path, offset, max_record_bytes)?;
            let frame_end = offset
                .checked_add(header.frame_len)
                .context("spool snapshot offset overflow")?;
            if frame_end > snapshot_len {
                let is_final_segment = segment_index + 1 == segment_ids.len();
                ensure!(
                    is_final_segment,
                    "sealed spool segment has an incomplete frame: {}",
                    path.display()
                );
                break;
            }

            let location = SpoolLocation {
                segment_id,
                frame_offset: offset,
                frame_len: header.frame_len,
            };
            let record = read_spool_frame_body(&mut file, &path, location, header)?;
            ensure_spool_record_identity(&record, &identity)?;
            if let Some(previous) = previous.as_ref() {
                ensure_observation_follows(previous, &record.metadata)?;
            }
            let sequence = record.metadata.observation.sequence;
            ensure!(
                sequence <= durable_through_sequence,
                "receiver spool record {sequence} is beyond supplied durable progress {durable_through_sequence}"
            );
            report.first_sequence.get_or_insert(sequence);
            report.last_sequence = Some(sequence);
            let metadata = record.metadata.clone();
            visit(record).with_context(|| {
                format!(
                    "visit receiver spool sequence {sequence} at {}",
                    path.display()
                )
            })?;
            report.records = report
                .records
                .checked_add(1)
                .context("spool snapshot record count overflow")?;
            previous = Some(metadata);
            offset = frame_end;
            if sequence == durable_through_sequence {
                report.reached_durable_tail = true;
                return Ok(report);
            }
        }
        if report.records == max_records as u64 {
            stopped_at_limit = true;
            break;
        }
    }

    if !stopped_at_limit {
        ensure!(
            report.last_sequence == Some(durable_through_sequence),
            "receiver progress is durable through sequence {durable_through_sequence}, but the spool snapshot ended at {:?}",
            report.last_sequence
        );
        report.reached_durable_tail = true;
    }
    Ok(report)
}

fn ensure_spool_record_identity(
    record: &SpoolRecord,
    identity: &SpoolJournalIdentity,
) -> Result<()> {
    ensure!(
        record.metadata.cluster_id == identity.cluster_id
            && record.metadata.observation.origin_node_id == identity.origin_node_id
            && record.metadata.source_id == identity.source_id
            && record.metadata.observation.journal_id == identity.journal_id,
        "spool record identity does not match requested journal"
    );
    Ok(())
}

fn read_exact_or_incomplete_tail<R: Read>(
    reader: &mut R,
    output: &mut [u8],
    path: &Path,
) -> Result<bool> {
    match reader.read_exact(output) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(err) => Err(err).with_context(|| format!("read spool segment {}", path.display())),
    }
}

/// Retire at most one fully validated sealed segment strictly before a durable ACK capability.
///
/// The ACK WAL capability, retention lock, complete candidate scan, boundary proof, and durable
/// prefix marker are all required before unlinking. The acknowledged physical frame remains
/// retained as a restart anchor. A pass is deliberately bounded to one segment.
#[cfg(test)]
pub(crate) fn retire_one_spool_segment_before_ack(
    spool_root: impl AsRef<Path>,
    identity: &SpoolJournalIdentity,
    max_record_bytes: u64,
    authorization: &DurableGcAuthorization,
) -> Result<SpoolSegmentRetirementOutcome> {
    retire_one_spool_segment_before_ack_inner(
        spool_root,
        identity,
        max_record_bytes,
        authorization,
        None,
    )
}

/// Production retention variant which revalidates the privilege-separated namespace while the
/// exclusive retention lock is held and accepts only a control-owned prefix marker.
pub(crate) fn retire_one_spool_segment_before_ack_in_namespace(
    spool_root: impl AsRef<Path>,
    identity: &SpoolJournalIdentity,
    max_record_bytes: u64,
    authorization: &DurableGcAuthorization,
    namespace_policy: SpoolGcNamespacePolicy,
) -> Result<SpoolSegmentRetirementOutcome> {
    retire_one_spool_segment_before_ack_inner(
        spool_root,
        identity,
        max_record_bytes,
        authorization,
        Some(namespace_policy),
    )
}

fn retire_one_spool_segment_before_ack_inner(
    spool_root: impl AsRef<Path>,
    identity: &SpoolJournalIdentity,
    max_record_bytes: u64,
    authorization: &DurableGcAuthorization,
    namespace_policy: Option<SpoolGcNamespacePolicy>,
) -> Result<SpoolSegmentRetirementOutcome> {
    ensure!(
        max_record_bytes > 0,
        "spool retirement record-byte limit must be non-zero"
    );
    let spool_root = spool_root.as_ref();
    let journal_dir = spool_journal_dir_path(spool_root, identity)?;
    let Some(_retention_guard) =
        acquire_retention_guard(&journal_dir, RetentionLockMode::ExclusiveNonblocking)?
    else {
        return Ok(SpoolSegmentRetirementOutcome::Busy);
    };
    if let Some(policy) = namespace_policy {
        validate_spool_gc_namespace(spool_root, identity, policy)?;
    }

    let ack = authorization.ack();
    ensure!(
        ack.stream.cluster_id == identity.cluster_id
            && ack.stream.origin_node_id == identity.origin_node_id
            && ack.stream.source_id == identity.source_id
            && ack.stream.journal_id == identity.journal_id,
        "spool ACK retirement authorization belongs to a different journal"
    );
    let acknowledgement_anchor = authorization.local_location();
    let anchor = read_spool_record(&journal_dir, acknowledgement_anchor, max_record_bytes)
        .context("read spool ACK retirement anchor")?;
    ensure_spool_record_identity(&anchor, identity)?;
    ensure!(
        anchor.metadata == *authorization.local_metadata()
            && anchor.metadata.observation.sequence == ack.through_sequence,
        "spool ACK retirement anchor no longer matches its durable ACK-WAL binding"
    );

    let segment_ids = segment_ids(&journal_dir, identity, max_record_bytes)?;
    let oldest = *segment_ids
        .first()
        .context("spool ACK retirement journal has no segments")?;
    ensure!(
        segment_ids
            .binary_search(&acknowledgement_anchor.segment_id)
            .is_ok(),
        "spool ACK retirement anchor segment is not retained"
    );
    if oldest >= acknowledgement_anchor.segment_id {
        return Ok(SpoolSegmentRetirementOutcome::NothingToRetire);
    }

    let successor_id = oldest
        .checked_add(1)
        .context("spool retirement successor id overflow")?;
    ensure!(
        segment_ids.binary_search(&successor_id).is_ok(),
        "spool retirement candidate has no contiguous retained successor"
    );
    let candidate = recover_complete_segment(
        &journal_dir,
        oldest,
        max_record_bytes,
        identity,
        "spool retirement candidate",
    )?;
    let candidate_tail = candidate
        .last_record
        .context("cannot retire an empty spool segment")?;
    ensure!(
        candidate_tail.metadata.observation.sequence <= ack.through_sequence,
        "durable ACK does not cover the complete spool retirement candidate"
    );
    let successor_first =
        read_first_spool_record(&journal_dir, successor_id, max_record_bytes, identity)?;
    ensure_exact_retention_successor(&candidate_tail, &successor_first)
        .context("spool retirement candidate/successor boundary is discontinuous")?;

    let marker = SpoolRetiredPrefixMarker {
        schema_version: 2,
        identity: identity.clone(),
        first_retained_segment_id: successor_id,
        acknowledged_through_sequence: ack.through_sequence,
        acknowledged_through_content_digest: ack.through_content_digest,
        acknowledgement_anchor,
        acknowledgement_anchor_metadata: Some(anchor.metadata),
        retired_tail: Some(SpoolRetiredPrefixTail {
            location: candidate_tail.location,
            metadata: candidate_tail.metadata,
        }),
    };
    if let Some(previous) = read_retired_prefix_marker_with_policy(&journal_dir, namespace_policy)?
    {
        ensure!(
            previous.identity == marker.identity,
            "spool retired-prefix marker identity changed"
        );
        ensure!(
            previous.acknowledged_through_sequence <= marker.acknowledged_through_sequence,
            "spool retired-prefix marker cannot move backward"
        );
        // Schema v1 stored the ACK-anchor segment in `first_retained_segment_id`, not the actual
        // first retained segment. During a crash or bounded one-segment GC pass, valid predecessor
        // segments may therefore remain below that value. The complete candidate scan and
        // candidate/successor continuity proof above safely establish the first exact schema-v2
        // boundary; only subsequent schema-v2 markers have a monotonic first-retained meaning.
        if previous.schema_version == 2 {
            ensure!(
                previous.first_retained_segment_id <= marker.first_retained_segment_id,
                "schema-v2 spool retired-prefix marker cannot move backward"
            );
        }
    }
    write_retired_prefix_marker(&journal_dir, &marker)?;

    let retired = segment_path(&journal_dir, oldest);
    fs::remove_file(&retired)
        .with_context(|| format!("retire acknowledged spool segment {}", retired.display()))?;
    sync_directory(&journal_dir)?;
    Ok(SpoolSegmentRetirementOutcome::Retired(retired))
}

fn read_retired_prefix_marker(journal_dir: &Path) -> Result<Option<SpoolRetiredPrefixMarker>> {
    read_retired_prefix_marker_with_policy(journal_dir, None)
}

fn read_retired_prefix_marker_with_policy(
    journal_dir: &Path,
    namespace_policy: Option<SpoolGcNamespacePolicy>,
) -> Result<Option<SpoolRetiredPrefixMarker>> {
    let path = journal_dir.join(RETIRED_PREFIX_MARKER_FILE);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("inspect spool retired-prefix marker {}", path.display())
            });
        }
    };
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "spool retired-prefix marker is not a regular file: {}",
        path.display()
    );
    #[cfg(target_os = "linux")]
    if let Some(policy) = namespace_policy {
        validate_gc_control_metadata(&metadata, policy, "retired-prefix marker", &path)?;
    }
    #[cfg(not(target_os = "linux"))]
    ensure!(
        namespace_policy.is_none(),
        "raw-spool GC marker protection requires Linux"
    );
    ensure!(
        metadata.len() <= RETIRED_PREFIX_MARKER_MAX_BYTES,
        "spool retired-prefix marker exceeds maximum size"
    );
    let mut file = open_regular_file_read_only(&path)?;
    let opened = file.metadata()?;
    #[cfg(target_os = "linux")]
    if let Some(policy) = namespace_policy {
        validate_gc_control_metadata(&opened, policy, "opened retired-prefix marker", &path)?;
    }
    ensure!(
        opened.len() <= RETIRED_PREFIX_MARKER_MAX_BYTES,
        "opened spool retired-prefix marker exceeds maximum size"
    );
    #[cfg(unix)]
    ensure!(
        opened.dev() == metadata.dev() && opened.ino() == metadata.ino() && opened.nlink() == 1,
        "spool retired-prefix marker changed while it was opened"
    );
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    (&mut file)
        .take(RETIRED_PREFIX_MARKER_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= RETIRED_PREFIX_MARKER_MAX_BYTES,
        "spool retired-prefix marker exceeds maximum size while reading"
    );
    #[cfg(unix)]
    {
        let linked_after = fs::symlink_metadata(&path)?;
        #[cfg(target_os = "linux")]
        if let Some(policy) = namespace_policy {
            validate_gc_control_metadata(
                &linked_after,
                policy,
                "linked retired-prefix marker",
                &path,
            )?;
        }
        ensure!(
            opened.dev() == linked_after.dev() && opened.ino() == linked_after.ino(),
            "spool retired-prefix marker was replaced while it was read"
        );
    }
    let marker: SpoolRetiredPrefixMarker =
        serde_json::from_slice(&bytes).context("decode spool retired-prefix marker")?;
    match marker.schema_version {
        1 => ensure!(
            marker.first_retained_segment_id == marker.acknowledgement_anchor.segment_id
                && marker.acknowledgement_anchor_metadata.is_none()
                && marker.retired_tail.is_none(),
            "invalid legacy spool retired-prefix marker"
        ),
        2 => {
            let anchor_metadata = marker
                .acknowledgement_anchor_metadata
                .as_ref()
                .context("schema-v2 spool retired-prefix marker lacks anchor metadata")?;
            let retired_tail = marker
                .retired_tail
                .as_ref()
                .context("schema-v2 spool retired-prefix marker lacks retired tail")?;
            ensure!(
                anchor_metadata.observation.sequence == marker.acknowledged_through_sequence
                    && retired_tail
                        .location
                        .segment_id
                        .checked_add(1)
                        .is_some_and(|next| next == marker.first_retained_segment_id)
                    && marker.first_retained_segment_id <= marker.acknowledgement_anchor.segment_id,
                "invalid schema-v2 spool retired-prefix marker"
            );
        }
        version => bail!("unsupported spool retired-prefix marker schema {version}"),
    }
    Ok(Some(marker))
}

fn write_retired_prefix_marker(
    journal_dir: &Path,
    marker: &SpoolRetiredPrefixMarker,
) -> Result<()> {
    let path = journal_dir.join(RETIRED_PREFIX_MARKER_FILE);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_nanos();
    let temporary = journal_dir.join(format!(
        ".retired-prefix.{}.{}.tmp",
        std::process::id(),
        nonce
    ));
    let result = (|| -> Result<()> {
        let (mut file, created) = open_regular_file(&temporary, true)?;
        ensure!(
            created,
            "spool retired-prefix temporary file already exists"
        );
        serde_json::to_writer(&mut file, marker).context("encode spool retired-prefix marker")?;
        file.write_all(b"\n")?;
        // The pull source normally runs as root while the recorder runs as the unprivileged app
        // user. The marker contains no secret material and must remain readable across that
        // process boundary.
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(0o640))
            .context("make spool retired-prefix marker readable by the recorder")?;
        file.sync_all()?;
        fs::rename(&temporary, &path).with_context(|| {
            format!(
                "publish spool retired-prefix marker {} from {}",
                path.display(),
                temporary.display()
            )
        })?;
        sync_directory(journal_dir)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn segment_ids(
    journal_dir: &Path,
    identity: &SpoolJournalIdentity,
    max_record_bytes: u64,
) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(journal_dir)
        .with_context(|| format!("list spool journal {}", journal_dir.display()))?
    {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(id_text) = name
            .strip_prefix("segment-")
            .and_then(|name| name.strip_suffix(".wal"))
        else {
            continue;
        };
        ensure!(
            id_text.len() == 20 && id_text.bytes().all(|byte| byte.is_ascii_digit()),
            "non-canonical spool segment name in {}: {}",
            journal_dir.display(),
            name
        );
        let id = id_text
            .parse::<u64>()
            .with_context(|| format!("parse spool segment id from {name}"))?;
        ensure!(id != u64::MAX, "spool segment id space exhausted");
        ids.push(id);
    }
    ids.sort_unstable();
    ids.dedup();
    for pair in ids.windows(2) {
        ensure!(
            pair[1] == pair[0] + 1,
            "non-consecutive spool segments {} then {} in {}",
            pair[0],
            pair[1],
            journal_dir.display()
        );
    }

    let retired =
        validate_retired_prefix_marker_anchor(journal_dir, identity, max_record_bytes, &ids)?;
    ensure!(
        !ids.is_empty() || retired.is_none(),
        "spool retired-prefix marker exists but no restart anchor segment remains in {}",
        journal_dir.display()
    );
    if let Some(first) = ids.first() {
        match retired {
            None => ensure!(
                *first == 0,
                "spool segment sequence starts at {}, expected 0 in {}",
                first,
                journal_dir.display()
            ),
            Some(marker) if marker.schema_version == 1 => ensure!(
                *first <= marker.first_retained_segment_id
                    && ids.binary_search(&marker.first_retained_segment_id).is_ok(),
                "legacy spool segment prefix is not covered by its durable retirement marker"
            ),
            Some(marker) => {
                let retired_id = marker
                    .retired_tail
                    .as_ref()
                    .context("schema-v2 marker lacks retired tail")?
                    .location
                    .segment_id;
                ensure!(
                    *first == marker.first_retained_segment_id || *first == retired_id,
                    "spool segment prefix is not covered by its schema-v2 retirement marker"
                );
            }
        }
    }
    Ok(ids)
}

fn validate_retired_prefix_marker_anchor(
    journal_dir: &Path,
    identity: &SpoolJournalIdentity,
    max_record_bytes: u64,
    segment_ids: &[u64],
) -> Result<Option<SpoolRetiredPrefixMarker>> {
    let Some(marker) = read_retired_prefix_marker(journal_dir)? else {
        return Ok(None);
    };
    ensure!(
        marker.identity == *identity,
        "spool retired-prefix marker identity does not match journal"
    );
    ensure!(
        segment_ids
            .binary_search(&marker.acknowledgement_anchor.segment_id)
            .is_ok(),
        "spool retired-prefix ACK anchor segment is not retained"
    );
    let anchor = read_spool_record(journal_dir, marker.acknowledgement_anchor, max_record_bytes)
        .context("validate spool retired-prefix restart anchor")?;
    ensure_spool_record_identity(&anchor, identity)?;
    ensure!(
        anchor.metadata.observation.sequence == marker.acknowledged_through_sequence,
        "spool retired-prefix restart anchor sequence does not match marker"
    );
    if marker.schema_version == 1 {
        ensure!(
            anchor.metadata.content_digest == marker.acknowledged_through_content_digest,
            "legacy spool retired-prefix restart anchor digest does not match marker"
        );
        return Ok(Some(marker));
    }

    ensure!(
        marker.acknowledgement_anchor_metadata.as_ref() == Some(&anchor.metadata),
        "spool retired-prefix physical ACK anchor metadata does not match marker"
    );
    let retired_tail = marker
        .retired_tail
        .as_ref()
        .context("schema-v2 spool retired-prefix marker lacks retired tail")?;
    let first_retained = read_first_spool_record(
        journal_dir,
        marker.first_retained_segment_id,
        max_record_bytes,
        identity,
    )?;
    let retired = DurableSpoolRecord {
        location: retired_tail.location,
        metadata: retired_tail.metadata.clone(),
    };
    ensure_exact_retention_successor(&retired, &first_retained)
        .context("spool retired-prefix boundary is discontinuous")?;

    if segment_ids.first().copied() == Some(retired_tail.location.segment_id) {
        let recovered = recover_complete_segment(
            journal_dir,
            retired_tail.location.segment_id,
            max_record_bytes,
            identity,
            "published-but-not-yet-unlinked retirement candidate",
        )?;
        ensure!(
            recovered.last_record.as_ref() == Some(&retired),
            "published spool retirement candidate tail changed before unlink"
        );
    }
    Ok(Some(marker))
}

fn recover_complete_segment(
    journal_dir: &Path,
    segment_id: u64,
    max_record_bytes: u64,
    identity: &SpoolJournalIdentity,
    label: &str,
) -> Result<RecoveredSegment> {
    let path = segment_path(journal_dir, segment_id);
    let mut file = open_regular_file_read_only(&path)?;
    let file_len = file.metadata()?.len();
    let recovered = recover_segment(&mut file, &path, segment_id, max_record_bytes, identity)
        .with_context(|| format!("validate {label}"))?;
    ensure!(
        recovered.valid_len == file_len,
        "{label} has an incomplete tail: {}",
        path.display()
    );
    ensure!(
        recovered.exact_sequence_contiguous,
        "{label} contains a non-contiguous observation sequence"
    );
    Ok(recovered)
}

fn ensure_exact_retention_successor(
    previous: &DurableSpoolRecord,
    next: &DurableSpoolRecord,
) -> Result<()> {
    ensure_record_follows(previous, next)?;
    ensure!(
        previous.metadata.observation.sequence.checked_add(1)
            == Some(next.metadata.observation.sequence),
        "retained observation sequence {} does not immediately follow {}",
        next.metadata.observation.sequence,
        previous.metadata.observation.sequence
    );
    Ok(())
}

fn read_first_spool_record(
    journal_dir: &Path,
    segment_id: u64,
    max_record_bytes: u64,
    identity: &SpoolJournalIdentity,
) -> Result<DurableSpoolRecord> {
    let path = segment_path(journal_dir, segment_id);
    let mut file = open_regular_file_read_only(&path)?;
    let mut segment_magic = [0u8; 8];
    file.read_exact(&mut segment_magic)
        .with_context(|| format!("read retained spool segment header {}", path.display()))?;
    ensure!(
        &segment_magic == SEGMENT_MAGIC,
        "invalid retained spool segment header in {}",
        path.display()
    );
    let header = read_spool_frame_header(&mut file, &path, SEGMENT_HEADER_LEN, max_record_bytes)?;
    let location = SpoolLocation {
        segment_id,
        frame_offset: SEGMENT_HEADER_LEN,
        frame_len: header.frame_len,
    };
    let record = read_spool_frame_body(&mut file, &path, location, header)?;
    ensure_spool_record_identity(&record, identity)?;
    Ok(DurableSpoolRecord::from_verified_committed_read(
        record.location,
        record.metadata,
    ))
}

fn segment_path(journal_dir: &Path, segment_id: u64) -> PathBuf {
    journal_dir.join(format!("segment-{segment_id:020}.wal"))
}

fn validate_path_component(value: &str, label: &str) -> Result<()> {
    ensure!(!value.is_empty(), "{label} must not be empty");
    ensure!(value.len() <= 64, "{label} exceeds 64 bytes");
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "{label} contains unsafe path characters: {value}"
    );
    ensure!(value != "." && value != "..", "{label} must not be {value}");
    Ok(())
}

fn hex_journal_id(journal_id: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(32);
    for byte in journal_id {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn acquire_retention_guard(
    journal_dir: &Path,
    mode: RetentionLockMode,
) -> Result<Option<RetentionGuard>> {
    let path = journal_dir.join(RETENTION_LOCK_FILE);
    let linked_before = match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "spool retention lock is not a regular file: {}",
                path.display()
            );
            Some(metadata)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect spool retention lock {}", path.display()));
        }
    };
    let created = linked_before.is_none();
    let mut options = OpenOptions::new();
    if matches!(mode, RetentionLockMode::Shared) && !created {
        options.read(true);
    } else {
        options.read(true).write(true).create(true);
    }
    #[cfg(unix)]
    options
        .mode(0o644)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(&path)
        .with_context(|| format!("open spool retention lock {}", path.display()))?;
    if created {
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(0o644))?;
        file.sync_all()?;
        sync_directory(journal_dir)?;
    }
    let opened = file.metadata()?;
    ensure!(
        opened.is_file(),
        "spool retention lock is not a file: {}",
        path.display()
    );
    let linked_after = fs::symlink_metadata(&path)?;
    #[cfg(unix)]
    {
        ensure!(
            opened.dev() == linked_after.dev() && opened.ino() == linked_after.ino(),
            "spool retention lock changed while it was opened"
        );
        if let Some(linked_before) = linked_before.as_ref() {
            ensure!(
                opened.dev() == linked_before.dev() && opened.ino() == linked_before.ino(),
                "spool retention lock was replaced while it was opened"
            );
        }
        let journal_owner = fs::metadata(journal_dir)?.uid();
        ensure!(
            (opened.uid() == 0 || opened.uid() == journal_owner)
                && opened.mode() & 0o022 == 0
                && opened.nlink() == 1,
            "spool retention lock ownership, permissions, or link count are unsafe"
        );
        let operation = match mode {
            RetentionLockMode::Shared => libc::LOCK_SH,
            RetentionLockMode::ExclusiveNonblocking => libc::LOCK_EX | libc::LOCK_NB,
        };
        // SAFETY: the guard owns this descriptor for the complete lock lifetime.
        let result = unsafe { libc::flock(file.as_raw_fd(), operation) };
        if result != 0 {
            let error = io::Error::last_os_error();
            if matches!(mode, RetentionLockMode::ExclusiveNonblocking)
                && error.kind() == ErrorKind::WouldBlock
            {
                return Ok(None);
            }
            return Err(error).context("acquire spool retention lock");
        }
    }
    #[cfg(not(unix))]
    match mode {
        RetentionLockMode::Shared => file.lock().context("acquire spool retention lock")?,
        RetentionLockMode::ExclusiveNonblocking => {
            if let Err(error) = file.try_lock() {
                if error.kind() == ErrorKind::WouldBlock {
                    return Ok(None);
                }
                return Err(error).context("acquire spool retention lock");
            }
        }
    }
    Ok(Some(RetentionGuard { _file: file }))
}

fn open_regular_file(path: &Path, create_if_missing: bool) -> Result<(File, bool)> {
    let (file, created) = if create_if_missing {
        match open_file_descriptor(path, true) {
            Ok(file) => (file, true),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => (
                open_file_descriptor(path, false)
                    .with_context(|| format!("open existing spool file {}", path.display()))?,
                false,
            ),
            Err(err) => {
                return Err(err).with_context(|| format!("create spool file {}", path.display()));
            }
        }
    } else {
        (
            open_file_descriptor(path, false)
                .with_context(|| format!("open spool file {}", path.display()))?,
            false,
        )
    };
    ensure!(
        file.metadata()?.file_type().is_file(),
        "spool path is not a regular file: {}",
        path.display()
    );
    Ok((file, created))
}

fn open_regular_file_read_only(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options
        .open(path)
        .with_context(|| format!("open spool file read-only {}", path.display()))?;
    ensure!(
        file.metadata()?.file_type().is_file(),
        "spool path is not a regular file: {}",
        path.display()
    );
    Ok(file)
}

fn open_file_descriptor(path: &Path, create_new: bool) -> io::Result<File> {
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

#[cfg(unix)]
fn try_lock_exclusive(file: &File, path: &Path) -> Result<()> {
    // SAFETY: `file` owns a valid descriptor for the duration of this call. The lock remains held
    // by its owning writer or audit guard until that value is dropped, and the OS releases it
    // after a crash.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
            .with_context(|| format!("lock spool journal {}", path.display()))
    }
}

#[cfg(not(unix))]
fn try_lock_exclusive(file: &File, path: &Path) -> Result<()> {
    file.try_lock()
        .with_context(|| format!("lock spool journal {}", path.display()))
}

fn sync_directory(path: &Path) -> Result<()> {
    let directory = File::open(path)
        .with_context(|| format!("open spool directory for sync {}", path.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("sync spool directory {}", path.display()))
}

fn create_dir_all_durable(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut cursor = path;
    while !cursor.exists() {
        missing.push(cursor.to_path_buf());
        cursor = cursor.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "spool directory has no existing ancestor: {}",
                path.display()
            )
        })?;
    }
    fs::create_dir_all(path)
        .with_context(|| format!("create spool directory {}", path.display()))?;
    // Persist every newly-created directory entry from the highest missing ancestor downward.
    for created in missing.iter().rev() {
        if let Some(parent) = created.parent() {
            sync_directory(parent)?;
        }
    }
    sync_directory(path)
}

#[derive(Debug, Clone, Copy)]
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
    use crate::ingest::dedup::{ContentDigest, LogicalKey, ObservationId};
    use crate::ingest::replication::{
        CumulativeAckSignatureVerifier, CumulativeAckWal, CumulativePrimaryAck,
        ExpectedCumulativeAck, REPLICATION_PROTOCOL_VERSION, ReceiptDisposition,
        ReplicationStreamId, verify_cumulative_ack,
    };

    struct AcceptCumulativeAckSignature;

    impl CumulativeAckSignatureVerifier for AcceptCumulativeAckSignature {
        fn verify_cumulative_ack_signature(
            &self,
            _key_id: &str,
            _signing_bytes: &[u8],
            _signature: &[u8],
        ) -> bool {
            true
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-spool-{label}-{}-{unique}",
            std::process::id()
        ))
    }

    #[cfg(target_os = "linux")]
    fn chown_test_path(path: &Path, uid: u32, gid: u32) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `path` is a live NUL-terminated pathname for this synchronous syscall.
        let result = unsafe { libc::chown(path.as_ptr(), uid, gid) };
        assert_eq!(
            result,
            0,
            "chown test path failed: {}",
            io::Error::last_os_error()
        );
    }

    #[cfg(target_os = "linux")]
    fn protect_test_gc_namespace(
        spool_root: &Path,
        identity: &SpoolJournalIdentity,
    ) -> SpoolGcNamespacePolicy {
        let policy = SpoolGcNamespacePolicy {
            control_uid: 0,
            recorder_gid: 10001,
        };
        for path in [
            spool_root.to_path_buf(),
            spool_root.join(&identity.cluster_id),
            spool_root
                .join(&identity.cluster_id)
                .join(&identity.origin_node_id),
            spool_root
                .join(&identity.cluster_id)
                .join(&identity.origin_node_id)
                .join(&identity.source_id),
        ] {
            chown_test_path(&path, policy.control_uid, policy.recorder_gid);
            fs::set_permissions(path, fs::Permissions::from_mode(0o750)).unwrap();
        }
        let journal_dir = spool_journal_dir_path(spool_root, identity).unwrap();
        chown_test_path(&journal_dir, policy.control_uid, policy.recorder_gid);
        fs::set_permissions(&journal_dir, fs::Permissions::from_mode(0o3770)).unwrap();
        chown_test_path(
            &journal_dir.join(RETENTION_LOCK_FILE),
            policy.control_uid,
            policy.recorder_gid,
        );
        fs::set_permissions(
            journal_dir.join(RETENTION_LOCK_FILE),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();
        policy
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn destructive_gc_requires_a_precreated_protected_namespace() {
        let root = temp_root("gc-namespace");
        let identity = journal_identity();
        let spool = SpoolWriter::open(
            &root,
            identity.clone(),
            SpoolOptions {
                segment_target_bytes: 1024,
                max_record_bytes: 1024,
            },
        )
        .unwrap();
        drop(spool);
        // The ordinary workspace job runs as an unprivileged Ubuntu user; prove it cannot enable
        // root-only GC there. The root Docker gate exercises the complete positive namespace.
        // SAFETY: credential accessors have no preconditions and do not mutate process state.
        let effective_uid = unsafe { libc::geteuid() };
        let effective_gid = unsafe { libc::getegid() };
        if effective_uid != 0 {
            let rejected = SpoolGcNamespacePolicy {
                control_uid: effective_uid,
                recorder_gid: effective_gid,
            };
            assert!(validate_spool_gc_namespace(&root, &identity, rejected).is_err());
            fs::remove_dir_all(root).unwrap();
            return;
        }
        let policy = protect_test_gc_namespace(&root, &identity);
        validate_spool_gc_namespace(&root, &identity, policy).unwrap();

        let journal_dir = spool_journal_dir_path(&root, &identity).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o710)).unwrap();
        assert!(validate_spool_gc_namespace(&root, &identity, policy).is_err());
        fs::set_permissions(&root, fs::Permissions::from_mode(0o750)).unwrap();

        fs::set_permissions(
            journal_dir.join(RETENTION_LOCK_FILE),
            fs::Permissions::from_mode(0o400),
        )
        .unwrap();
        assert!(validate_spool_gc_namespace(&root, &identity, policy).is_err());
        fs::set_permissions(
            journal_dir.join(RETENTION_LOCK_FILE),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();

        fs::set_permissions(
            journal_dir.join(RETENTION_LOCK_FILE),
            fs::Permissions::from_mode(0o440),
        )
        .unwrap();
        assert!(validate_spool_gc_namespace(&root, &identity, policy).is_err());
        fs::set_permissions(
            journal_dir.join(RETENTION_LOCK_FILE),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();

        fs::set_permissions(
            journal_dir.join(RETENTION_LOCK_FILE),
            fs::Permissions::from_mode(0o660),
        )
        .unwrap();
        assert!(validate_spool_gc_namespace(&root, &identity, policy).is_err());
        fs::set_permissions(
            journal_dir.join(RETENTION_LOCK_FILE),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();

        fs::set_permissions(&journal_dir, fs::Permissions::from_mode(0o0770)).unwrap();
        assert!(validate_spool_gc_namespace(&root, &identity, policy).is_err());
        fs::set_permissions(&journal_dir, fs::Permissions::from_mode(0o3770)).unwrap();

        fs::remove_file(journal_dir.join(RETENTION_LOCK_FILE)).unwrap();
        assert!(validate_spool_gc_namespace(&root, &identity, policy).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn destructive_gc_rejects_a_replaceable_spool_root_anchor() {
        let anchor = temp_root("gc-replaceable-anchor");
        let root = anchor.join("spool");
        let identity = journal_identity();
        let spool = SpoolWriter::open(
            &root,
            identity.clone(),
            SpoolOptions {
                segment_target_bytes: 1024,
                max_record_bytes: 1024,
            },
        )
        .unwrap();
        drop(spool);
        // SAFETY: credential accessors have no preconditions and do not mutate process state.
        let effective_uid = unsafe { libc::geteuid() };
        let effective_gid = unsafe { libc::getegid() };
        if effective_uid != 0 {
            let rejected = SpoolGcNamespacePolicy {
                control_uid: effective_uid,
                recorder_gid: effective_gid,
            };
            assert!(validate_spool_gc_namespace(&root, &identity, rejected).is_err());
            fs::remove_dir_all(anchor).unwrap();
            return;
        }
        let policy = protect_test_gc_namespace(&root, &identity);

        chown_test_path(&anchor, policy.control_uid, policy.recorder_gid);
        fs::set_permissions(&anchor, fs::Permissions::from_mode(0o770)).unwrap();
        assert!(validate_spool_gc_namespace(&root, &identity, policy).is_err());
        fs::set_permissions(&anchor, fs::Permissions::from_mode(0o750)).unwrap();
        validate_spool_gc_namespace(&root, &identity, policy).unwrap();
        fs::remove_dir_all(anchor).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "invoked by the root-only cross-UID retention integration test"]
    fn cross_uid_recorder_process_helper() {
        let Ok(root) = std::env::var("HIVEZILLA_CROSS_UID_SPOOL_ROOT") else {
            return;
        };
        let action = std::env::var("HIVEZILLA_CROSS_UID_ACTION").unwrap();
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let mut spool =
            SpoolWriter::open(PathBuf::from(root), journal_identity(), options).unwrap();
        match action.as_str() {
            "write" => {
                for sequence in 1..=3 {
                    let payload = vec![sequence as u8; 64];
                    spool
                        .append_and_sync(metadata(sequence, &payload), &payload)
                        .unwrap();
                }
            }
            "reopen" => {
                let payload = vec![5; 64];
                spool
                    .append_and_sync(metadata(5, &payload), &payload)
                    .unwrap();
            }
            "hold" => {
                let ready = PathBuf::from(std::env::var("HIVEZILLA_CROSS_UID_READY").unwrap());
                let stop = PathBuf::from(std::env::var("HIVEZILLA_CROSS_UID_STOP").unwrap());
                fs::write(&ready, b"ready\n").unwrap();
                while !stop.exists() {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                let payload = vec![4; 64];
                spool
                    .append_and_sync(metadata(4, &payload), &payload)
                    .unwrap();
            }
            other => panic!("unexpected cross-UID helper action {other}"),
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn root_gc_and_uid_10001_recorder_coexist_across_retirement_and_reopen() {
        use std::os::unix::process::CommandExt;
        use std::process::Command;

        // SAFETY: credential accessors have no preconditions and do not mutate process state.
        if unsafe { libc::geteuid() } != 0 {
            return;
        }
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let anchor = PathBuf::from("/srv").join(format!(
            "hivezilla-cross-uid-{}-{unique}",
            std::process::id()
        ));
        let root = anchor.join("spool");
        fs::create_dir_all(&root).unwrap();
        chown_test_path(&anchor, 0, 10001);
        fs::set_permissions(&anchor, fs::Permissions::from_mode(0o750)).unwrap();
        chown_test_path(&root, 10001, 10001);
        fs::set_permissions(&root, fs::Permissions::from_mode(0o770)).unwrap();

        let run_recorder = |action: &str| {
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--ignored")
                .arg("--exact")
                .arg("ingest::spool::tests::cross_uid_recorder_process_helper")
                .arg("--nocapture")
                .env("HIVEZILLA_CROSS_UID_SPOOL_ROOT", &root)
                .env("HIVEZILLA_CROSS_UID_ACTION", action)
                .uid(10001)
                .gid(10001)
                .status()
                .unwrap();
            assert!(status.success(), "UID 10001 recorder helper failed");
        };
        run_recorder("write");

        let identity = journal_identity();
        let policy = protect_test_gc_namespace(&root, &identity);
        validate_spool_gc_namespace(&root, &identity, policy).unwrap();
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let audit = LockedSpoolAudit::open(&root, identity.clone(), options).unwrap();
        let ack_anchor = audit.last_record().unwrap().clone();
        drop(audit);
        let authorization = durable_gc_authorization(&root, &identity, &ack_anchor);

        let coordination = anchor.join("coordination");
        fs::create_dir(&coordination).unwrap();
        chown_test_path(&coordination, 0, 10001);
        fs::set_permissions(&coordination, fs::Permissions::from_mode(0o770)).unwrap();
        let ready = coordination.join("ready");
        let stop = coordination.join("stop");
        let mut holding_recorder = Command::new(std::env::current_exe().unwrap());
        let mut holding_recorder = holding_recorder
            .arg("--ignored")
            .arg("--exact")
            .arg("ingest::spool::tests::cross_uid_recorder_process_helper")
            .arg("--nocapture")
            .env("HIVEZILLA_CROSS_UID_SPOOL_ROOT", &root)
            .env("HIVEZILLA_CROSS_UID_ACTION", "hold")
            .env("HIVEZILLA_CROSS_UID_READY", &ready)
            .env("HIVEZILLA_CROSS_UID_STOP", &stop)
            .uid(10001)
            .gid(10001)
            .spawn()
            .unwrap();
        for _ in 0..500 {
            if ready.exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(ready.exists(), "UID 10001 recorder did not become ready");
        let journal_dir = spool_journal_dir_path(&root, &identity).unwrap();
        let retained_before =
            segment_ids(&journal_dir, &identity, options.max_record_bytes).unwrap();
        let retired = retire_one_spool_segment_before_ack_in_namespace(
            &root,
            &identity,
            options.max_record_bytes,
            &authorization,
            policy,
        )
        .unwrap();
        assert!(matches!(retired, SpoolSegmentRetirementOutcome::Retired(_)));
        assert_eq!(
            segment_ids(&journal_dir, &identity, options.max_record_bytes)
                .unwrap()
                .len(),
            retained_before.len() - 1
        );
        fs::write(&stop, b"stop\n").unwrap();
        assert!(holding_recorder.wait().unwrap().success());

        validate_spool_gc_namespace(&root, &identity, policy).unwrap();
        let audit = LockedSpoolAudit::open(&root, identity.clone(), options).unwrap();
        assert_eq!(
            audit.last_record().unwrap().metadata().observation.sequence,
            4
        );
        drop(audit);

        run_recorder("reopen");
        let audit = LockedSpoolAudit::open(&root, identity, options).unwrap();
        assert_eq!(
            audit.last_record().unwrap().metadata().observation.sequence,
            5
        );
        drop(audit);
        fs::remove_dir_all(anchor).unwrap();
    }

    fn directory_file_bytes(path: &Path) -> u64 {
        fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum()
    }

    fn metadata(sequence: u64, payload: &[u8]) -> IngressRecordMeta {
        IngressRecordMeta::from_payload(
            "solana-mainnet".to_string(),
            ObservationId {
                origin_node_id: "node-a".to_string(),
                journal_id: [7; 16],
                sequence,
            },
            "grpc-a".to_string(),
            LogicalKey::Block {
                slot: 42 + sequence,
                blockhash: [sequence as u8; 32],
            },
            1,
            payload,
        )
    }

    fn journal_identity() -> SpoolJournalIdentity {
        SpoolJournalIdentity {
            cluster_id: "solana-mainnet".to_string(),
            origin_node_id: "node-a".to_string(),
            source_id: "grpc-a".to_string(),
            journal_id: [7; 16],
        }
    }

    fn durable_gc_authorization(
        root: &Path,
        identity: &SpoolJournalIdentity,
        anchor: &DurableSpoolRecord,
    ) -> DurableGcAuthorization {
        let stream = ReplicationStreamId {
            cluster_id: identity.cluster_id.clone(),
            origin_node_id: identity.origin_node_id.clone(),
            source_id: identity.source_id.clone(),
            journal_id: identity.journal_id,
        };
        let ack = CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: stream.clone(),
            primary_id: "test-primary".to_string(),
            primary_term: 1,
            through_sequence: anchor.metadata().observation.sequence,
            through_content_digest: anchor.metadata().content_digest,
            rolling_chain_digest: ContentDigest([9; 32]),
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: 1,
            signing_key_id: "test-key".to_string(),
            signature: vec![1; 64],
        };
        let expected = ExpectedCumulativeAck {
            stream: &ack.stream,
            primary_id: &ack.primary_id,
            minimum_primary_term: 1,
            through_sequence: ack.through_sequence,
            through_content_digest: ack.through_content_digest,
            rolling_chain_digest: ack.rolling_chain_digest,
        };
        let verified =
            verify_cumulative_ack(ack.clone(), expected, &AcceptCumulativeAckSignature).unwrap();
        let ack_wal_path = root.join("test-cumulative-acks.wal");
        let mut ack_wal = CumulativeAckWal::open(&ack_wal_path).unwrap();
        ack_wal.commit_verified(verified, anchor).unwrap();
        drop(ack_wal);

        let recovered = CumulativeAckWal::open(&ack_wal_path).unwrap();
        let authorization = recovered
            .durable_gc_authorization(&stream)
            .unwrap()
            .unwrap();
        drop(recovered);
        authorization
    }

    fn write_exact_legacy_retired_prefix_marker(
        journal_dir: &Path,
        identity: &SpoolJournalIdentity,
        anchor: &DurableSpoolRecord,
    ) {
        let marker = serde_json::json!({
            "schema_version": 1,
            "identity": identity,
            "first_retained_segment_id": anchor.location().segment_id,
            "acknowledged_through_sequence": anchor.metadata().observation.sequence,
            "acknowledged_through_content_digest": anchor.metadata().content_digest,
            "acknowledgement_anchor": anchor.location(),
        });
        let path = journal_dir.join(RETIRED_PREFIX_MARKER_FILE);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        serde_json::to_writer(&mut file, &marker).unwrap();
        file.write_all(b"\n").unwrap();
        file.sync_all().unwrap();
        sync_directory(journal_dir).unwrap();
    }

    #[test]
    fn appends_syncs_recovers_and_continues() {
        let root = temp_root("recover");
        let options = SpoolOptions {
            segment_target_bytes: 1024 * 1024,
            max_record_bytes: 1024,
        };
        let first_location = {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            spool
                .append_and_sync(metadata(1, b"first"), b"first")
                .unwrap()
                .location
        };
        let second_location = {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            spool
                .append_and_sync(metadata(2, b"second"), b"second")
                .unwrap()
                .location
        };
        assert_eq!(first_location.segment_id, second_location.segment_id);
        assert_eq!(
            second_location.frame_offset,
            first_location.frame_offset + first_location.frame_len
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn append_projection_matches_same_segment_and_rotation_growth() {
        let root = temp_root("append-projection");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();

        let first_metadata = metadata(1, &[1; 64]);
        let first = spool.project_append(&first_metadata, &[1; 64]).unwrap();
        let segment_zero_before = fs::metadata(segment_path(spool.journal_dir(), 0))
            .unwrap()
            .len();
        let first_durable = spool.append_and_sync(first_metadata, &[1; 64]).unwrap();
        let segment_zero_after = fs::metadata(segment_path(spool.journal_dir(), 0))
            .unwrap()
            .len();
        assert_eq!(first.location, first_durable.location());
        assert_eq!(
            segment_zero_after - segment_zero_before,
            first.additional_bytes
        );

        let second_metadata = metadata(2, &[2; 64]);
        let second = spool.project_append(&second_metadata, &[2; 64]).unwrap();
        assert_eq!(second.location.segment_id, 1);
        assert_eq!(second.location.frame_offset, SEGMENT_HEADER_LEN);
        let second_durable = spool.append_and_sync(second_metadata, &[2; 64]).unwrap();
        assert_eq!(second.location, second_durable.location());
        assert_eq!(
            fs::metadata(segment_path(spool.journal_dir(), 1))
                .unwrap()
                .len(),
            second.additional_bytes
        );

        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn group_commit_matches_projection_across_rotation_and_reopens_at_tail() {
        let root = temp_root("group-commit");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let records = vec![
            (metadata(1, &[1; 64]), vec![1; 64]),
            (metadata(2, &[2; 64]), vec![2; 64]),
            (metadata(3, &[3; 64]), vec![3; 64]),
        ];
        let projected = spool.project_batch_additional_bytes(&records).unwrap();
        let before = directory_file_bytes(spool.journal_dir());
        let committed = spool.append_batch_and_sync(records).unwrap();
        assert_eq!(committed.records.len(), 3);
        assert_eq!(committed.additional_bytes, projected);
        assert_eq!(
            directory_file_bytes(spool.journal_dir()) - before,
            projected
        );
        let expected_tail = committed.records.last().unwrap().clone();
        drop(spool);

        let reopened = SpoolWriter::open(&root, identity, options).unwrap();
        assert_eq!(reopened.last_record(), Some(&expected_tail));
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reads_back_one_frame_with_matching_length_and_digest() {
        let root = temp_root("read-record");
        let options = SpoolOptions {
            segment_target_bytes: 1024 * 1024,
            max_record_bytes: 1024,
        };
        let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        let durable = spool
            .append_and_sync(metadata(1, b"payload"), b"payload")
            .unwrap();
        let loaded = spool.read_record(&durable).unwrap();
        assert_eq!(loaded.location, durable.location());
        assert_eq!(loaded.metadata, *durable.metadata());
        assert_eq!(loaded.payload, b"payload");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_snapshot_never_exposes_a_frame_beyond_supplied_durable_progress() {
        let root = temp_root("read-durable-prefix");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        let first = spool
            .append_and_sync(metadata(1, b"first"), b"first")
            .unwrap();
        spool
            .append_and_sync(
                metadata(2, b"visible-but-not-progress-covered"),
                b"visible-but-not-progress-covered",
            )
            .unwrap();
        let bytes_before = fs::read_dir(spool.journal_dir())
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("segment-") && name.ends_with(".wal"))
            })
            .map(|entry| entry.metadata().unwrap().len())
            .sum::<u64>();

        let mut observed = Vec::new();
        let report = read_spool_committed_snapshot_after(
            &root,
            journal_identity(),
            options.max_record_bytes,
            None,
            1,
            8,
            |record| {
                observed.push(record.metadata.observation.sequence);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(observed, [1]);
        assert_eq!(report.last_sequence, Some(1));
        assert!(report.reached_durable_tail);
        assert_eq!(
            fs::read_dir(spool.journal_dir())
                .unwrap()
                .map(Result::unwrap)
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with("segment-") && name.ends_with(".wal"))
                })
                .map(|entry| entry.metadata().unwrap().len())
                .sum::<u64>(),
            bytes_before
        );

        observed.clear();
        let report = read_spool_committed_snapshot_after(
            &root,
            journal_identity(),
            options.max_record_bytes,
            Some(first.location()),
            2,
            8,
            |record| {
                observed.push(record.metadata.observation.sequence);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(observed, [2]);
        assert!(report.reached_durable_tail);
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_snapshot_resumes_and_scans_adjacent_frames_in_one_open_segment() {
        let root = temp_root("read-adjacent-durable-frames");
        let options = SpoolOptions {
            segment_target_bytes: 1024 * 1024,
            max_record_bytes: 1024,
        };
        let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        let first = spool
            .append_and_sync(metadata(1, b"first"), b"first")
            .unwrap();
        let second = spool
            .append_and_sync(metadata(2, b"second"), b"second")
            .unwrap();
        let third = spool
            .append_and_sync(metadata(3, b"third"), b"third")
            .unwrap();
        let fourth = spool
            .append_and_sync(metadata(4, b"fourth"), b"fourth")
            .unwrap();
        assert_eq!(
            [
                first.location(),
                second.location(),
                third.location(),
                fourth.location()
            ]
            .map(|location| location.segment_id),
            [0, 0, 0, 0]
        );
        assert_eq!(
            third.location().frame_offset,
            second.location().frame_offset + second.location().frame_len
        );

        let mut observed = Vec::new();
        let report = read_spool_committed_snapshot_after(
            &root,
            journal_identity(),
            options.max_record_bytes,
            Some(second.location()),
            4,
            8,
            |record| {
                observed.push((
                    record.metadata.observation.sequence,
                    record.payload,
                    record.location,
                ));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            observed,
            [
                (3, b"third".to_vec(), third.location()),
                (4, b"fourth".to_vec(), fourth.location()),
            ]
        );
        assert_eq!(report.records, 2);
        assert_eq!(report.first_sequence, Some(3));
        assert_eq!(report.last_sequence, Some(4));
        assert!(report.reached_durable_tail);
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_snapshot_sequential_reader_rechecks_content_digest() {
        let root = temp_root("read-adjacent-content-digest");
        let options = SpoolOptions {
            segment_target_bytes: 1024 * 1024,
            max_record_bytes: 1024,
        };
        let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        spool
            .append_and_sync(metadata(1, b"first"), b"first")
            .unwrap();
        let second = spool
            .append_and_sync(metadata(2, b"second"), b"second")
            .unwrap();
        spool
            .append_and_sync(metadata(3, b"third"), b"third")
            .unwrap();
        let journal_dir = spool.journal_dir().to_path_buf();
        drop(spool);

        // Change the payload and update the frame CRC so only the independently recomputed
        // canonical content digest can detect the corruption.
        let metadata_bytes = serde_json::to_vec(second.metadata()).unwrap();
        let mut changed_payload = b"second".to_vec();
        changed_payload[0] ^= 1;
        let payload_offset = second.location().frame_offset
            + FRAME_FIXED_LEN
            + u64::try_from(metadata_bytes.len()).unwrap();
        let mut crc = Crc32c::new();
        crc.update(&metadata_bytes);
        crc.update(&changed_payload);
        let mut segment = OpenOptions::new()
            .write(true)
            .open(segment_path(&journal_dir, second.location().segment_id))
            .unwrap();
        segment.seek(SeekFrom::Start(payload_offset)).unwrap();
        segment.write_all(&changed_payload).unwrap();
        segment.write_all(&crc.finish().to_le_bytes()).unwrap();
        segment.sync_data().unwrap();
        drop(segment);

        let mut observed = Vec::new();
        let error = read_spool_committed_snapshot_after(
            &root,
            journal_identity(),
            options.max_record_bytes,
            None,
            3,
            8,
            |record| {
                observed.push(record.metadata.observation.sequence);
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(observed, [1]);
        assert!(
            format!("{error:#}").contains("spool content digest mismatch"),
            "{error:#}"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_reused_observation_sequence_before_writing() {
        let root = temp_root("duplicate-sequence");
        let options = SpoolOptions {
            segment_target_bytes: 1024 * 1024,
            max_record_bytes: 1024,
        };
        let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        spool
            .append_and_sync(metadata(1, b"first"), b"first")
            .unwrap();
        let before = fs::metadata(segment_path(spool.journal_dir(), 0))
            .unwrap()
            .len();
        let error = spool
            .append_and_sync(metadata(1, b"duplicate"), b"duplicate")
            .unwrap_err();
        assert!(error.to_string().contains("reused with different"));
        assert_eq!(
            fs::metadata(segment_path(spool.journal_dir(), 0))
                .unwrap()
                .len(),
            before
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_non_consecutive_segment_files() {
        let root = temp_root("segment-gap");
        let options = SpoolOptions {
            segment_target_bytes: 1024 * 1024,
            max_record_bytes: 1024,
        };
        let journal_dir = {
            let spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            spool.journal_dir().to_path_buf()
        };
        fs::copy(segment_path(&journal_dir, 0), segment_path(&journal_dir, 2)).unwrap();
        let error = SpoolWriter::open(&root, journal_identity(), options).unwrap_err();
        assert!(error.to_string().contains("non-consecutive spool segments"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn incomplete_crash_tail_is_truncated_on_open() {
        let root = temp_root("tail");
        let options = SpoolOptions {
            segment_target_bytes: 1024 * 1024,
            max_record_bytes: 1024,
        };
        let (segment_path, valid_len) = {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            spool
                .append_and_sync(metadata(1, b"first"), b"first")
                .unwrap();
            let path = segment_path(spool.journal_dir(), spool.current_segment_id());
            let valid_len = fs::metadata(&path).unwrap().len();
            (path, valid_len)
        };
        let mut file = OpenOptions::new().append(true).open(&segment_path).unwrap();
        file.write_all(FRAME_MAGIC).unwrap();
        file.write_all(&FRAME_VERSION.to_le_bytes()).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let _spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        assert_eq!(fs::metadata(segment_path).unwrap().len(), valid_len);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn locked_audit_reports_matching_durable_tail_across_segments() {
        let root = temp_root("audit-matching");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let expected = {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            spool
                .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
                .unwrap();
            spool
                .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
                .unwrap()
        };

        let audit = LockedSpoolAudit::open(&root, journal_identity(), options).unwrap();
        assert_eq!(audit.last_record(), Some(&expected));
        assert_eq!(audit.incomplete_tail_bytes(), 0);
        assert!(audit.journal_dir().ends_with(hex_journal_id([7; 16])));

        let error = SpoolWriter::open(&root, journal_identity(), options).unwrap_err();
        assert!(error.to_string().contains("lock spool journal"));
        drop(audit);
        drop(SpoolWriter::open(&root, journal_identity(), options).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn locked_audit_reports_incomplete_active_tail_without_truncating() {
        let root = temp_root("audit-incomplete-active");
        let options = SpoolOptions {
            segment_target_bytes: 1024 * 1024,
            max_record_bytes: 1024,
        };
        let (active_path, expected) = {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            let expected = spool
                .append_and_sync(metadata(1, b"first"), b"first")
                .unwrap();
            (
                segment_path(spool.journal_dir(), spool.current_segment_id()),
                expected,
            )
        };
        let mut file = OpenOptions::new().append(true).open(&active_path).unwrap();
        file.write_all(FRAME_MAGIC).unwrap();
        file.write_all(&FRAME_VERSION.to_le_bytes()).unwrap();
        file.sync_data().unwrap();
        drop(file);
        let length_with_tail = fs::metadata(&active_path).unwrap().len();

        let audit = LockedSpoolAudit::open(&root, journal_identity(), options).unwrap();
        assert_eq!(audit.last_record(), Some(&expected));
        assert_eq!(audit.incomplete_tail_bytes(), 6);
        assert_eq!(fs::metadata(&active_path).unwrap().len(), length_with_tail);
        drop(audit);
        assert_eq!(fs::metadata(&active_path).unwrap().len(), length_with_tail);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn locked_audit_rejects_an_active_writer() {
        let root = temp_root("audit-active-writer");
        let options = SpoolOptions {
            segment_target_bytes: 1024,
            max_record_bytes: 1024,
        };
        let writer = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        let error = LockedSpoolAudit::open(&root, journal_identity(), options).unwrap_err();
        assert!(error.to_string().contains("lock spool journal"));
        drop(writer);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn locked_audit_rejects_incomplete_non_final_segment_without_truncating() {
        let root = temp_root("audit-incomplete-sealed");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let sealed_path = {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            let first = spool
                .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
                .unwrap();
            spool
                .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
                .unwrap();
            segment_path(spool.journal_dir(), first.location.segment_id)
        };
        let mut file = OpenOptions::new().append(true).open(&sealed_path).unwrap();
        file.write_all(FRAME_MAGIC).unwrap();
        file.sync_data().unwrap();
        drop(file);
        let length_with_tail = fs::metadata(&sealed_path).unwrap().len();

        let error = LockedSpoolAudit::open(&root, journal_identity(), options).unwrap_err();
        assert!(error.to_string().contains("incomplete tail"));
        assert_eq!(fs::metadata(&sealed_path).unwrap().len(), length_with_tail);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn locked_audit_validates_observation_order_across_segments() {
        let root = temp_root("audit-cross-order");
        let donor_root = temp_root("audit-cross-order-donor");
        let options = SpoolOptions {
            segment_target_bytes: 1024 * 1024,
            max_record_bytes: 1024,
        };
        let target_dir = {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            spool
                .append_and_sync(metadata(5, b"later"), b"later")
                .unwrap();
            spool.journal_dir().to_path_buf()
        };
        let donor_segment = {
            let mut spool = SpoolWriter::open(&donor_root, journal_identity(), options).unwrap();
            spool
                .append_and_sync(metadata(1, b"earlier"), b"earlier")
                .unwrap();
            segment_path(spool.journal_dir(), 0)
        };
        fs::copy(donor_segment, segment_path(&target_dir, 1)).unwrap();

        let error = LockedSpoolAudit::open(&root, journal_identity(), options).unwrap_err();
        assert!(format!("{error:#}").contains("moved backward"));
        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(donor_root).unwrap();
    }

    #[test]
    fn rotates_without_holding_old_payloads_in_memory() {
        let root = temp_root("rotate");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        let first = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        let second = spool
            .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
            .unwrap();
        assert!(second.location.segment_id > first.location.segment_id);
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn acknowledged_prefix_retirement_keeps_a_restart_anchor() {
        let root = temp_root("retire-acked-prefix");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let first = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        let second = spool
            .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
            .unwrap();
        let anchor = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        assert_eq!(first.location().segment_id, 0);
        assert_eq!(second.location().segment_id, 1);
        assert_eq!(anchor.location().segment_id, 2);
        let authorization = durable_gc_authorization(&root, &identity, &anchor);

        let retired = retire_one_spool_segment_before_ack(
            &root,
            &identity,
            options.max_record_bytes,
            &authorization,
        )
        .unwrap();
        let expected_retired = segment_path(spool.journal_dir(), 0);
        assert_eq!(
            retired,
            SpoolSegmentRetirementOutcome::Retired(expected_retired.clone())
        );
        assert!(!expected_retired.exists());
        assert!(segment_path(spool.journal_dir(), 1).exists());
        assert!(segment_path(spool.journal_dir(), 2).exists());
        #[cfg(unix)]
        {
            let marker_mode = fs::metadata(spool.journal_dir().join(RETIRED_PREFIX_MARKER_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(marker_mode, 0o640);
        }

        let retired = retire_one_spool_segment_before_ack(
            &root,
            &identity,
            options.max_record_bytes,
            &authorization,
        )
        .unwrap();
        assert_eq!(
            retired,
            SpoolSegmentRetirementOutcome::Retired(segment_path(spool.journal_dir(), 1))
        );
        assert_eq!(
            retire_one_spool_segment_before_ack(
                &root,
                &identity,
                options.max_record_bytes,
                &authorization,
            )
            .unwrap(),
            SpoolSegmentRetirementOutcome::NothingToRetire
        );

        // The active writer continues from the retained anchor, and a cold restart accepts the
        // explicitly authorized non-zero segment prefix.
        let fourth = spool
            .append_and_sync(metadata(4, &[4; 64]), &[4; 64])
            .unwrap();
        drop(spool);
        let reopened = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        assert_eq!(reopened.last_record(), Some(&fourth));
        drop(reopened);

        let mut sequences = Vec::new();
        let report = read_spool_committed_snapshot_after(
            &root,
            identity,
            options.max_record_bytes,
            Some(anchor.location()),
            4,
            8,
            |record| {
                sequences.push(record.metadata.observation.sequence);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(sequences, [4]);
        assert!(report.reached_durable_tail);

        // A durable retirement marker must never permit the same journal identity to restart from
        // sequence zero after every retained segment has disappeared.
        let journal_dir = spool_journal_dir_path(&root, &journal_identity()).unwrap();
        for segment_id in
            segment_ids(&journal_dir, &journal_identity(), options.max_record_bytes).unwrap()
        {
            fs::remove_file(segment_path(&journal_dir, segment_id)).unwrap();
        }
        let error = match SpoolWriter::open(&root, journal_identity(), options) {
            Ok(_) => panic!("retired journal without its anchor must fail closed"),
            Err(error) => error,
        };
        assert!(
            format!("{error:#}").contains("retired-prefix")
                && format!("{error:#}").contains("anchor"),
            "{error:#}"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retired_prefix_marker_fails_closed_when_its_anchor_is_truncated() {
        let root = temp_root("retired-anchor-truncated");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        spool
            .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
            .unwrap();
        let anchor = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        let authorization = durable_gc_authorization(&root, &identity, &anchor);
        retire_one_spool_segment_before_ack(
            &root,
            &identity,
            options.max_record_bytes,
            &authorization,
        )
        .unwrap();
        drop(spool);

        OpenOptions::new()
            .write(true)
            .open(segment_path(
                &spool_journal_dir_path(&root, &identity).unwrap(),
                anchor.location().segment_id,
            ))
            .unwrap()
            .set_len(SEGMENT_HEADER_LEN)
            .unwrap();
        let error = SpoolWriter::open(&root, identity, options).unwrap_err();
        assert!(
            format!("{error:#}").contains("validate spool retired-prefix restart anchor"),
            "{error:#}"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_schema_one_marker_reopens_the_live_nonzero_prefix() {
        let root = temp_root("legacy-retired-prefix");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let first = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        let second = spool
            .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
            .unwrap();
        let anchor = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        assert_eq!(
            [first.location(), second.location(), anchor.location()]
                .map(|location| location.segment_id),
            [0, 1, 2]
        );

        let journal_dir = spool.journal_dir().to_path_buf();
        write_exact_legacy_retired_prefix_marker(&journal_dir, &identity, &anchor);
        fs::remove_file(segment_path(&journal_dir, 0)).unwrap();
        fs::remove_file(segment_path(&journal_dir, 1)).unwrap();
        sync_directory(&journal_dir).unwrap();
        drop(spool);

        let mut reopened = SpoolWriter::open(&root, identity, options).unwrap();
        assert_eq!(reopened.last_record(), Some(&anchor));
        let fourth = reopened
            .append_and_sync(metadata(4, &[4; 64]), &[4; 64])
            .unwrap();
        assert_eq!(reopened.last_record(), Some(&fourth));
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_schema_one_crash_prefix_migrates_to_an_exact_schema_two_boundary() {
        let root = temp_root("legacy-retired-prefix-migration");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let zero = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        let one = spool
            .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
            .unwrap();
        let two = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        let anchor = spool
            .append_and_sync(metadata(4, &[4; 64]), &[4; 64])
            .unwrap();
        assert_eq!(
            [
                zero.location(),
                one.location(),
                two.location(),
                anchor.location()
            ]
            .map(|location| location.segment_id),
            [0, 1, 2, 3]
        );
        let authorization = durable_gc_authorization(&root, &identity, &anchor);
        let journal_dir = spool.journal_dir().to_path_buf();
        // Legacy GC published its ACK anchor in `first_retained_segment_id` before deleting one
        // predecessor. The exact schema-1 bytes do not contain the later optional schema-2 keys.
        write_exact_legacy_retired_prefix_marker(&journal_dir, &identity, &anchor);
        fs::remove_file(segment_path(&journal_dir, zero.location().segment_id)).unwrap();
        sync_directory(&journal_dir).unwrap();

        assert_eq!(
            retire_one_spool_segment_before_ack(
                &root,
                &identity,
                options.max_record_bytes,
                &authorization,
            )
            .unwrap(),
            SpoolSegmentRetirementOutcome::Retired(segment_path(
                &journal_dir,
                one.location().segment_id
            ))
        );
        let marker = read_retired_prefix_marker(&journal_dir).unwrap().unwrap();
        assert_eq!(marker.schema_version, 2);
        assert_eq!(marker.first_retained_segment_id, two.location().segment_id);
        drop(spool);

        let reopened = SpoolWriter::open(&root, identity, options).unwrap();
        assert_eq!(reopened.last_record(), Some(&anchor));
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_retirement_candidate_is_never_marked_or_deleted() {
        let root = temp_root("retire-corrupt-candidate");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let first = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        spool
            .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
            .unwrap();
        let anchor = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        let authorization = durable_gc_authorization(&root, &identity, &anchor);
        let journal_dir = spool.journal_dir().to_path_buf();
        let candidate = segment_path(&journal_dir, first.location().segment_id);
        let mut file = OpenOptions::new().write(true).open(&candidate).unwrap();
        file.seek(SeekFrom::Start(
            first.location().frame_offset + FRAME_FIXED_LEN,
        ))
        .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let error = retire_one_spool_segment_before_ack(
            &root,
            &identity,
            options.max_record_bytes,
            &authorization,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("validate spool retirement candidate"),
            "{error:#}"
        );
        assert!(candidate.exists());
        assert!(!journal_dir.join(RETIRED_PREFIX_MARKER_FILE).exists());
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn incomplete_retirement_candidate_is_never_marked_or_deleted() {
        let root = temp_root("retire-incomplete-candidate");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let first = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        spool
            .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
            .unwrap();
        let anchor = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        let authorization = durable_gc_authorization(&root, &identity, &anchor);
        let journal_dir = spool.journal_dir().to_path_buf();
        let candidate = segment_path(&journal_dir, first.location().segment_id);
        let mut file = OpenOptions::new().append(true).open(&candidate).unwrap();
        file.write_all(FRAME_MAGIC).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let error = retire_one_spool_segment_before_ack(
            &root,
            &identity,
            options.max_record_bytes,
            &authorization,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("incomplete tail"),
            "{error:#}"
        );
        assert!(candidate.exists());
        assert!(!journal_dir.join(RETIRED_PREFIX_MARKER_FILE).exists());
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retirement_rejects_a_checksummed_sequence_gap_at_the_segment_boundary() {
        let root = temp_root("retire-sequence-gap");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let candidate = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        let successor = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        let anchor = spool
            .append_and_sync(metadata(4, &[4; 64]), &[4; 64])
            .unwrap();
        assert_eq!(candidate.location().segment_id, 0);
        assert_eq!(successor.location().segment_id, 1);
        let authorization = durable_gc_authorization(&root, &identity, &anchor);
        let journal_dir = spool.journal_dir().to_path_buf();

        let error = retire_one_spool_segment_before_ack(
            &root,
            &identity,
            options.max_record_bytes,
            &authorization,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("does not immediately follow"),
            "{error:#}"
        );
        assert!(segment_path(&journal_dir, candidate.location().segment_id).exists());
        assert!(!journal_dir.join(RETIRED_PREFIX_MARKER_FILE).exists());
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retirement_rejects_a_checksummed_sequence_gap_inside_the_candidate() {
        let root = temp_root("retire-internal-sequence-gap");
        let options = SpoolOptions {
            segment_target_bytes: 2048,
            max_record_bytes: 8192,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let first = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        let gap = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        let anchor_payload = vec![4; 2048];
        let anchor = spool
            .append_and_sync(metadata(4, &anchor_payload), &anchor_payload)
            .unwrap();
        assert_eq!(first.location().segment_id, gap.location().segment_id);
        assert!(anchor.location().segment_id > gap.location().segment_id);
        let authorization = durable_gc_authorization(&root, &identity, &anchor);
        let journal_dir = spool.journal_dir().to_path_buf();

        let error = retire_one_spool_segment_before_ack(
            &root,
            &identity,
            options.max_record_bytes,
            &authorization,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("non-contiguous observation sequence"),
            "{error:#}"
        );
        assert!(segment_path(&journal_dir, first.location().segment_id).exists());
        assert!(!journal_dir.join(RETIRED_PREFIX_MARKER_FILE).exists());
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn locked_audit_reader_makes_gc_return_busy_without_mutation() {
        let root = temp_root("retire-reader-busy");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let first = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        spool
            .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
            .unwrap();
        let anchor = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        let authorization = durable_gc_authorization(&root, &identity, &anchor);
        let journal_dir = spool.journal_dir().to_path_buf();
        drop(spool);
        let audit = LockedSpoolAudit::open(&root, identity.clone(), options).unwrap();

        assert_eq!(
            retire_one_spool_segment_before_ack(
                &root,
                &identity,
                options.max_record_bytes,
                &authorization,
            )
            .unwrap(),
            SpoolSegmentRetirementOutcome::Busy
        );
        assert!(segment_path(&journal_dir, first.location().segment_id).exists());
        assert!(!journal_dir.join(RETIRED_PREFIX_MARKER_FILE).exists());
        drop(audit);
        assert_eq!(
            retire_one_spool_segment_before_ack(
                &root,
                &identity,
                options.max_record_bytes,
                &authorization,
            )
            .unwrap(),
            SpoolSegmentRetirementOutcome::Retired(segment_path(
                &journal_dir,
                first.location().segment_id
            ))
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restart_accepts_marker_published_before_candidate_unlink() {
        let root = temp_root("retire-marker-before-unlink");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let identity = journal_identity();
        let mut spool = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        let candidate_tail = spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        let successor = spool
            .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
            .unwrap();
        let anchor = spool
            .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
            .unwrap();
        let authorization = durable_gc_authorization(&root, &identity, &anchor);
        let journal_dir = spool.journal_dir().to_path_buf();
        write_retired_prefix_marker(
            &journal_dir,
            &SpoolRetiredPrefixMarker {
                schema_version: 2,
                identity: identity.clone(),
                first_retained_segment_id: successor.location().segment_id,
                acknowledged_through_sequence: anchor.metadata().observation.sequence,
                acknowledged_through_content_digest: anchor.metadata().content_digest,
                acknowledgement_anchor: anchor.location(),
                acknowledgement_anchor_metadata: Some(anchor.metadata().clone()),
                retired_tail: Some(SpoolRetiredPrefixTail {
                    location: candidate_tail.location(),
                    metadata: candidate_tail.metadata().clone(),
                }),
            },
        )
        .unwrap();
        drop(spool);

        let reopened = SpoolWriter::open(&root, identity.clone(), options).unwrap();
        assert_eq!(reopened.last_record(), Some(&anchor));
        drop(reopened);
        assert_eq!(
            retire_one_spool_segment_before_ack(
                &root,
                &identity,
                options.max_record_bytes,
                &authorization,
            )
            .unwrap(),
            SpoolSegmentRetirementOutcome::Retired(segment_path(
                &journal_dir,
                candidate_tail.location().segment_id
            ))
        );
        let reopened = SpoolWriter::open(&root, identity, options).unwrap();
        assert_eq!(reopened.last_record(), Some(&anchor));
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reopening_validates_sealed_older_segments() {
        let root = temp_root("sealed-corruption");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let older_segment = {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            let first = spool
                .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
                .unwrap();
            spool
                .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
                .unwrap();
            segment_path(spool.journal_dir(), first.location.segment_id)
        };
        let original_len = fs::metadata(&older_segment).unwrap().len();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&older_segment)
            .unwrap();
        file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_data().unwrap();
        drop(file);

        assert!(SpoolWriter::open(&root, journal_identity(), options).is_err());
        assert_eq!(fs::metadata(older_segment).unwrap().len(), original_len);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_checkpoint_recovery_skips_sealed_history_but_offline_audit_checks_it() {
        let root = temp_root("checkpoint-skips-history");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let (older_segment, checkpoint) = {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            let first = spool
                .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
                .unwrap();
            spool
                .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
                .unwrap();
            let checkpoint = spool
                .append_and_sync(metadata(3, &[3; 64]), &[3; 64])
                .unwrap();
            (
                segment_path(spool.journal_dir(), first.location.segment_id),
                checkpoint,
            )
        };
        assert!(checkpoint.location.segment_id > 0);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&older_segment)
            .unwrap();
        file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_data().unwrap();
        drop(file);

        let live = SpoolWriter::open_from_checkpoint(
            &root,
            journal_identity(),
            options,
            Some(checkpoint.location()),
        )
        .unwrap();
        assert_eq!(live.last_record(), Some(&checkpoint));
        drop(live);

        let error = LockedSpoolAudit::open(&root, journal_identity(), options).unwrap_err();
        assert!(format!("{error:#}").contains("corrupt spool frame magic"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_checkpoint_is_required_for_multi_segment_resume() {
        let root = temp_root("checkpoint-required");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        {
            let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            spool
                .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
                .unwrap();
            spool
                .append_and_sync(metadata(2, &[2; 64]), &[2; 64])
                .unwrap();
        }
        let error = SpoolWriter::open_from_checkpoint(&root, journal_identity(), options, None)
            .unwrap_err();
        assert!(error.to_string().contains("handoff checkpoint is required"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_a_second_writer_for_the_same_journal() {
        let root = temp_root("lock");
        let options = SpoolOptions {
            segment_target_bytes: 1024,
            max_record_bytes: 1024,
        };
        let first = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        let error = SpoolWriter::open(&root, journal_identity(), options).unwrap_err();
        assert!(error.to_string().contains("lock spool journal"));
        drop(first);
        let reopened = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn refuses_to_follow_a_segment_symlink() {
        use std::os::unix::fs::symlink;

        let root = temp_root("symlink");
        let options = SpoolOptions {
            segment_target_bytes: 1024,
            max_record_bytes: 1024,
        };
        let segment = {
            let spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
            segment_path(spool.journal_dir(), spool.current_segment_id())
        };
        fs::remove_file(&segment).unwrap();
        let canary = temp_root("segment-canary");
        fs::write(&canary, b"do-not-touch").unwrap();
        symlink(&canary, &segment).unwrap();

        assert!(SpoolWriter::open(&root, journal_identity(), options).is_err());
        assert_eq!(fs::read(&canary).unwrap(), b"do-not-touch");
        fs::remove_dir_all(root).unwrap();
        fs::remove_file(canary).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn io_failure_poisons_writer_until_reopen() {
        let root = temp_root("poison");
        let options = SpoolOptions {
            segment_target_bytes: 200,
            max_record_bytes: 1024,
        };
        let mut spool = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        spool
            .append_and_sync(metadata(1, &[1; 64]), &[1; 64])
            .unwrap();
        let journal_dir = spool.journal_dir().to_path_buf();
        let detached_journal_dir = journal_dir.with_extension("detached");
        let next = metadata(2, &[2; 64]);
        assert_eq!(
            spool
                .project_append(&next, &[2; 64])
                .unwrap()
                .location
                .segment_id,
            spool.current_segment_id() + 1
        );

        // Replacing the open journal path with a regular file is a deterministic failure
        // injection on Unix, including when the tests run as root in a container. The pending
        // rotation can sync the open segment but cannot create a child below the sentinel.
        fs::rename(&journal_dir, &detached_journal_dir).unwrap();
        fs::write(&journal_dir, b"not-a-directory").unwrap();
        let result = spool.append_and_sync(next, &[2; 64]);
        fs::remove_file(&journal_dir).unwrap();
        fs::rename(&detached_journal_dir, &journal_dir).unwrap();

        assert!(result.is_err());
        assert!(spool.is_poisoned());
        assert!(
            spool
                .append_and_sync(metadata(3, b"event"), b"event")
                .unwrap_err()
                .to_string()
                .contains("poisoned")
        );
        drop(spool);

        let reopened = SpoolWriter::open(&root, journal_identity(), options).unwrap();
        assert!(!reopened.is_poisoned());
        assert_eq!(
            reopened
                .last_record()
                .map(|record| record.metadata.observation.sequence),
            Some(1)
        );
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_metadata_payload_length_mismatch() {
        let root = temp_root("length");
        let mut spool = SpoolWriter::open(
            &root,
            journal_identity(),
            SpoolOptions {
                segment_target_bytes: 1024,
                max_record_bytes: 1024,
            },
        )
        .unwrap();
        let err = spool
            .append_and_sync(metadata(1, b"three"), b"four")
            .unwrap_err();
        assert!(err.to_string().contains("does not match"));
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_metadata_for_a_different_source_or_journal() {
        let root = temp_root("identity");
        let mut spool = SpoolWriter::open(
            &root,
            journal_identity(),
            SpoolOptions {
                segment_target_bytes: 1024,
                max_record_bytes: 1024,
            },
        )
        .unwrap();
        let mut wrong_source = metadata(1, b"event");
        wrong_source.source_id = "grpc-b".to_string();
        assert!(
            spool
                .append_and_sync(wrong_source, b"event")
                .unwrap_err()
                .to_string()
                .contains("source id")
        );

        let mut wrong_journal = metadata(2, b"event");
        wrong_journal.observation.journal_id = [8; 16];
        assert!(
            spool
                .append_and_sync(wrong_journal, b"event")
                .unwrap_err()
                .to_string()
                .contains("journal id")
        );
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recomputes_and_rejects_a_false_content_digest() {
        let root = temp_root("digest");
        let mut spool = SpoolWriter::open(
            &root,
            journal_identity(),
            SpoolOptions {
                segment_target_bytes: 1024,
                max_record_bytes: 1024,
            },
        )
        .unwrap();
        let mut false_digest = metadata(1, b"event");
        false_digest.content_digest = ContentDigest([0; 32]);
        assert!(
            spool
                .append_and_sync(false_digest, b"event")
                .unwrap_err()
                .to_string()
                .contains("canonical payload digest")
        );
        assert!(!spool.is_poisoned());
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn crc32c_matches_standard_check_value() {
        let mut crc = Crc32c::new();
        crc.update(b"123456789");
        assert_eq!(crc.finish(), 0xe306_9283);
    }
}
