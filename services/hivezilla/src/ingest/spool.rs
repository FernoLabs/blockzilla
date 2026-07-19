//! Crash-recoverable, bounded-memory raw ingress spool.
//!
//! This is the first durability boundary for every source. [`SpoolWriter::append_and_sync`] does
//! not return until the complete frame is visible to the filesystem and `sync_data` succeeds.
//! Upstream cursors and primary receipts may advance only after that return value is obtained.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use super::dedup::{IngressRecordMeta, compute_content_digest};

const SEGMENT_MAGIC: &[u8; 8] = b"BZIWAL01";
const FRAME_MAGIC: &[u8; 4] = b"BZIF";
const COMMIT_MAGIC: &[u8; 4] = b"CMIT";
const FRAME_VERSION: u16 = 1;
const SEGMENT_HEADER_LEN: u64 = SEGMENT_MAGIC.len() as u64;
const FRAME_FIXED_LEN: u64 = 4 + 2 + 4 + 8 + 4;
const FRAME_TRAILER_LEN: u64 = 4 + 4;
const MAX_METADATA_BYTES: usize = 64 * 1024;
const RECOVERY_BUFFER_BYTES: usize = 64 * 1024;

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

        let lock_path = journal_dir.join("writer.lock");
        let journal_lock = open_regular_file_read_only(&lock_path)?;
        try_lock_exclusive(&journal_lock, &lock_path)?;

        let segment_ids = segment_ids(&journal_dir)?;
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

        let segment_ids = segment_ids(&journal_dir)?;
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

        let segment_ids = segment_ids(&journal_dir)?;
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
        self.writer.flush().context("flush spool segment")?;
        self.writer
            .get_ref()
            .sync_data()
            .context("sync spool segment")?;

        self.segment_len = self
            .segment_len
            .checked_add(frame_len)
            .context("spool segment length overflow")?;
        self.poisoned = false;
        let durable = DurableSpoolRecord {
            location: SpoolLocation {
                segment_id: self.segment_id,
                frame_offset,
                frame_len,
            },
            metadata,
        };
        self.last_record = Some(durable.clone());
        Ok(durable)
    }

    fn prepare_append(
        &self,
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
        if let Some(previous) = self.last_record.as_ref() {
            ensure_observation_follows(&previous.metadata, &metadata)?;
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

    let mut frame_magic = [0u8; 4];
    file.read_exact(&mut frame_magic)
        .with_context(|| format!("read spool frame magic in {}", path.display()))?;
    ensure!(
        &frame_magic == FRAME_MAGIC,
        "corrupt spool frame magic at {} in {}",
        location.frame_offset,
        path.display()
    );
    let mut version_bytes = [0u8; 2];
    let mut metadata_len_bytes = [0u8; 4];
    let mut payload_len_bytes = [0u8; 8];
    let mut expected_header_crc_bytes = [0u8; 4];
    file.read_exact(&mut version_bytes)?;
    file.read_exact(&mut metadata_len_bytes)?;
    file.read_exact(&mut payload_len_bytes)?;
    file.read_exact(&mut expected_header_crc_bytes)?;
    ensure!(
        u16::from_le_bytes(version_bytes) == FRAME_VERSION,
        "unsupported spool frame version at {} in {}",
        location.frame_offset,
        path.display()
    );
    let metadata_len = u32::from_le_bytes(metadata_len_bytes) as usize;
    let payload_len = u64::from_le_bytes(payload_len_bytes);
    ensure!(
        metadata_len <= MAX_METADATA_BYTES,
        "spool metadata length exceeds maximum at {} in {}",
        location.frame_offset,
        path.display()
    );
    ensure!(
        payload_len <= max_record_bytes,
        "spool payload length {} exceeds configured maximum {}",
        payload_len,
        max_record_bytes
    );
    let expected_frame_len = FRAME_FIXED_LEN
        .checked_add(metadata_len as u64)
        .and_then(|len| len.checked_add(payload_len))
        .and_then(|len| len.checked_add(FRAME_TRAILER_LEN))
        .context("spool replay frame length overflow")?;
    ensure!(
        expected_frame_len == location.frame_len,
        "spool frame length mismatch at {} in {}: journal {}, encoded {}",
        location.frame_offset,
        path.display(),
        location.frame_len,
        expected_frame_len
    );

    let mut header_crc = Crc32c::new();
    header_crc.update(&frame_magic);
    header_crc.update(&version_bytes);
    header_crc.update(&metadata_len_bytes);
    header_crc.update(&payload_len_bytes);
    ensure!(
        header_crc.finish() == u32::from_le_bytes(expected_header_crc_bytes),
        "spool frame header checksum mismatch at {} in {}",
        location.frame_offset,
        path.display()
    );

    let mut metadata_bytes = vec![0u8; metadata_len];
    file.read_exact(&mut metadata_bytes)?;
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
    file.read_exact(&mut payload)?;
    let mut expected_crc_bytes = [0u8; 4];
    let mut commit_magic = [0u8; 4];
    file.read_exact(&mut expected_crc_bytes)?;
    file.read_exact(&mut commit_magic)?;
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
/// progress cursor says that record must already exist, which is treated as corruption.
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
    let segment_ids = segment_ids(&journal_dir)?;
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
        let mut file = open_regular_file_read_only(&path)?;
        let snapshot_len = file
            .metadata()
            .with_context(|| format!("inspect spool segment {}", path.display()))?
            .len();
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

            let mut frame_magic = [0u8; 4];
            let mut version_bytes = [0u8; 2];
            let mut metadata_len_bytes = [0u8; 4];
            let mut payload_len_bytes = [0u8; 8];
            let mut expected_header_crc_bytes = [0u8; 4];
            file.read_exact(&mut frame_magic)?;
            file.read_exact(&mut version_bytes)?;
            file.read_exact(&mut metadata_len_bytes)?;
            file.read_exact(&mut payload_len_bytes)?;
            file.read_exact(&mut expected_header_crc_bytes)?;
            ensure!(
                &frame_magic == FRAME_MAGIC,
                "corrupt spool frame magic at {offset} in {}",
                path.display()
            );
            ensure!(
                u16::from_le_bytes(version_bytes) == FRAME_VERSION,
                "unsupported spool frame version at {offset} in {}",
                path.display()
            );
            let metadata_len = u32::from_le_bytes(metadata_len_bytes) as usize;
            let payload_len = u64::from_le_bytes(payload_len_bytes);
            ensure!(
                metadata_len <= MAX_METADATA_BYTES,
                "spool metadata length exceeds maximum at {offset} in {}",
                path.display()
            );
            ensure!(
                payload_len <= max_record_bytes,
                "spool payload length {payload_len} exceeds configured maximum {}",
                max_record_bytes
            );
            let mut header_crc = Crc32c::new();
            header_crc.update(&frame_magic);
            header_crc.update(&version_bytes);
            header_crc.update(&metadata_len_bytes);
            header_crc.update(&payload_len_bytes);
            ensure!(
                header_crc.finish() == u32::from_le_bytes(expected_header_crc_bytes),
                "spool frame header checksum mismatch at {offset} in {}",
                path.display()
            );
            let frame_len = FRAME_FIXED_LEN
                .checked_add(metadata_len as u64)
                .and_then(|length| length.checked_add(payload_len))
                .and_then(|length| length.checked_add(FRAME_TRAILER_LEN))
                .context("spool snapshot frame length overflow")?;
            let frame_end = offset
                .checked_add(frame_len)
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
                frame_len,
            };
            let record = read_spool_record(&journal_dir, location, max_record_bytes)?;
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
            file.seek(SeekFrom::Start(offset))
                .with_context(|| format!("seek next spool snapshot frame {}", path.display()))?;
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

fn segment_ids(journal_dir: &Path) -> Result<Vec<u64>> {
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
    if let Some(first) = ids.first() {
        ensure!(
            *first == 0,
            "spool segment sequence starts at {}, expected 0 in {}",
            first,
            journal_dir.display()
        );
    }
    for pair in ids.windows(2) {
        ensure!(
            pair[1] == pair[0] + 1,
            "non-consecutive spool segments {} then {} in {}",
            pair[0],
            pair[1],
            journal_dir.display()
        );
    }
    Ok(ids)
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
        use std::os::unix::fs::PermissionsExt;

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
        fs::set_permissions(&journal_dir, fs::Permissions::from_mode(0o500)).unwrap();
        let result = spool.append_and_sync(metadata(2, &[2; 64]), &[2; 64]);
        fs::set_permissions(&journal_dir, fs::Permissions::from_mode(0o700)).unwrap();

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
