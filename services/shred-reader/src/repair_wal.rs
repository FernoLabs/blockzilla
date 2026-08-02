//! Dedicated segmented WAL for accepted repair shreds.
//!
//! Segment zero remains the legacy `.repair.wal` file so existing provenance is read without a
//! migration. Once it reaches the configured per-segment target, the writer seals it and appends
//! to an adjacent, monotonically numbered segment. Sealed segments are immutable and are never
//! deleted by this writer; retirement requires a future durable consumer acknowledgement.
//!
//! Every frame retains the v2 body encoding and CRC. Rolled segments add a v3 header that binds
//! their id and first global sequence to the preceding segment's terminal sequence and chain
//! digest. Startup validates the complete retained chain before opening the highest segment for
//! append. A configuration mistake or missing/corrupt segment therefore fails closed without
//! touching the raw-shred journal or replication ACK state.

use std::{
    fs::{File, OpenOptions, TryLockError},
    io::{self, ErrorKind, Read, Seek, SeekFrom, Write},
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::{ffi::CString, mem::MaybeUninit, os::unix::ffi::OsStrExt};

use sha2::{Digest, Sha256};
use solana_hash::Hash;
use solana_keypair::Signature;
use solana_pubkey::Pubkey;

use crate::repair_wire::{RepairNonce, ShredRepairRequest};

const LEGACY_FILE_HEADER: &[u8; 16] = b"SHRED-REPAIR\0\0\x02\0";
const SEGMENT_FILE_MAGIC: &[u8; 16] = b"SHRED-REPAIR\0\0\x03\0";
const SEGMENT_ID_BYTES: usize = 8;
const SEGMENT_SEQUENCE_BYTES: usize = 8;
const SEGMENT_DIGEST_BYTES: usize = 32;
const SEGMENT_HEADER_BODY_BYTES: usize = SEGMENT_FILE_MAGIC.len()
    + SEGMENT_ID_BYTES
    + SEGMENT_SEQUENCE_BYTES
    + SEGMENT_SEQUENCE_BYTES
    + SEGMENT_DIGEST_BYTES;
const SEGMENT_HEADER_BYTES: usize = SEGMENT_HEADER_BODY_BYTES + 4;
const V3_SEAL_MAGIC: &[u8; 16] = b"SHRED-V3-SEAL\0\x01\0";
const V3_SEAL_BODY_BYTES: usize =
    V3_SEAL_MAGIC.len() + SEGMENT_SEQUENCE_BYTES + SEGMENT_DIGEST_BYTES;
const V3_SEAL_BYTES: usize = V3_SEAL_BODY_BYTES + 4;
const V3_HEAD_MAGIC: &[u8; 16] = b"SHRED-V3-HEAD\0\x01\0";
const V3_HEAD_BODY_BYTES: usize = V3_HEAD_MAGIC.len()
    + SEGMENT_ID_BYTES
    + SEGMENT_SEQUENCE_BYTES
    + SEGMENT_SEQUENCE_BYTES
    + SEGMENT_DIGEST_BYTES;
const V3_HEAD_BYTES: usize = V3_HEAD_BODY_BYTES + 4;
const NO_PREVIOUS_SEQUENCE: u64 = u64::MAX;
const REPAIR_WAL_SUFFIX: &str = ".repair.wal";
const SEGMENT_MARKER: &str = ".segment-";
const SEGMENT_ID_WIDTH: usize = 20;
const FRAME_PREFIX_BYTES: usize = 4;
const FRAME_CHECKSUM_BYTES: usize = 4;
const MIN_BODY_BYTES: usize = 8;
const MAX_FRAME_BODY_BYTES: usize = 16 * 1024;
const MAX_FRAME_BYTES: usize = FRAME_PREFIX_BYTES + MAX_FRAME_BODY_BYTES + FRAME_CHECKSUM_BYTES;
pub const MIN_REPAIR_WAL_FILE_BYTES: u64 = (SEGMENT_HEADER_BYTES + MAX_FRAME_BYTES) as u64;
const MAX_STAGING_NAME_ATTEMPTS: usize = 1_024;
static NEXT_STAGING_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairWalFsyncPolicy {
    EveryRecord,
    Batch {
        max_unsynced_records: NonZeroU64,
        max_unsynced_age: Duration,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairWalConfig {
    pub path: PathBuf,
    pub fsync: RepairWalFsyncPolicy,
    /// Target cap for each repair-only segment. A frame that would cross the target is written to
    /// a fresh segment; a single frame can never exceed this value.
    pub max_file_bytes: u64,
    /// Hard aggregate ceiling across the base file and every numbered segment.
    pub max_retained_bytes: u64,
    /// Bytes that must remain filesystem-available after each admitted append or rollover.
    pub filesystem_reserve_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairWalInspection {
    pub retained_bytes: u64,
    pub active_segment_bytes: u64,
    pub segment_count: u64,
    pub active_segment_id: u64,
    pub filesystem_available_bytes: Option<u64>,
    pub v3_sealed: bool,
    pub validation_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairProvenance {
    pub received_at_unix_ms: u64,
    pub nonce: RepairNonce,
    pub request: ShredRepairRequest,
    pub peer_addr: String,
    pub peer_pubkey: Pubkey,
    pub shred_slot: u64,
    pub shred_index: u32,
    pub fec_set_index: u32,
    pub shred_version: u16,
    pub expected_slot_leader: Pubkey,
    pub fec_merkle_root: Hash,
    pub trust_anchor_fec_set_index: u32,
    pub learned_chained_merkle_root: bool,
    pub chained_merkle_root: Option<Hash>,
    pub leader_signature: Signature,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairWalAppend {
    pub sequence: u64,
    pub frame_bytes: usize,
    pub synced: bool,
    pub rolled_segment: bool,
    pub segment_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairWalEntry {
    pub sequence: u64,
    pub provenance: RepairProvenance,
    pub shred_payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairWalSegment {
    pub id: u64,
    pub path: PathBuf,
    pub first_sequence: u64,
    pub last_sequence: Option<u64>,
    pub file_bytes: u64,
    pub chain_digest: Hash,
    pub sealed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SegmentHeader {
    id: u64,
    first_sequence: u64,
    previous_last_sequence: u64,
    previous_chain_digest: Hash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct V3Seal {
    next_sequence: u64,
    base_chain_digest: Hash,
}

/// Durable high-water checkpoint for the complete accepted v3 generation.
///
/// `active_file_len` and `chain_digest` make a clean frame-boundary truncation detectable even
/// when every remaining frame and segment header is otherwise internally valid.
#[derive(Clone, Debug, Eq, PartialEq)]
struct V3Head {
    active_segment_id: u64,
    active_file_len: u64,
    next_sequence: u64,
    chain_digest: Hash,
}

#[derive(Debug)]
struct RecoveredSegment {
    descriptor: RepairWalSegment,
    valid_len: u64,
    next_sequence: u64,
}

#[derive(Debug)]
struct ValidatedChain {
    base: RepairWalSegment,
    active: RecoveredSegment,
    retained_bytes: u64,
    segments: Vec<RepairWalSegment>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V3HeadRelation {
    Exact,
    WalAhead,
}

#[derive(Debug)]
pub struct RepairWal {
    file: File,
    path: PathBuf,
    writer_lock: File,
    legacy_lock: Option<File>,
    fsync: RepairWalFsyncPolicy,
    max_file_bytes: u64,
    max_retained_bytes: u64,
    filesystem_reserve_bytes: u64,
    filesystem_available_bytes: u64,
    base_sealed: bool,
    active_segment_id: u64,
    next_sequence: u64,
    file_len: u64,
    retained_bytes: u64,
    segment_count: u64,
    rollovers: u64,
    syncs: u64,
    chain_digest: Hash,
    durable_through_sequence: Option<u64>,
    unsynced_records: u64,
    last_sync_at: Instant,
}

impl RepairWal {
    pub fn open(config: RepairWalConfig, now: Instant) -> io::Result<Self> {
        validate_repair_wal_config(&config)?;
        std::fs::create_dir_all(parent_directory(&config.path))?;

        reject_symlink(&writer_lock_path(&config.path), "repair WAL writer lock")?;
        let writer_lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(writer_lock_path(&config.path))?;
        lock_file(&writer_lock, &config.path)?;

        let seal = read_v3_seal(&config.path)?;
        let head = read_v3_head(&config.path)?;
        let mut segment_paths = discover_segment_paths(&config.path)?;
        if segment_paths.is_empty() {
            if seal.is_some() || head.is_some() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "repair WAL v3 control state exists but every segment is missing; refusing sequence rollback",
                ));
            }
            let available = filesystem_available_bytes(&config.path)?;
            ensure_capacity(
                0,
                LEGACY_FILE_HEADER.len() as u64,
                config.max_retained_bytes,
                LEGACY_FILE_HEADER.len() as u64,
                available,
                config.filesystem_reserve_bytes,
            )?;
            reject_symlink(&config.path, "repair WAL")?;
            let mut file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(&config.path)?;
            lock_file(&file, &config.path)?;
            file.write_all(LEGACY_FILE_HEADER)?;
            file.sync_data()?;
            sync_parent_directory(&config.path)?;
            let chain_digest = initial_chain_digest(LEGACY_FILE_HEADER);
            let filesystem_available_bytes = filesystem_available_bytes(&config.path)?;
            return Ok(Self {
                file,
                path: config.path,
                writer_lock,
                legacy_lock: None,
                fsync: config.fsync,
                max_file_bytes: config.max_file_bytes,
                max_retained_bytes: config.max_retained_bytes,
                filesystem_reserve_bytes: config.filesystem_reserve_bytes,
                filesystem_available_bytes,
                base_sealed: false,
                active_segment_id: 0,
                next_sequence: 0,
                file_len: LEGACY_FILE_HEADER.len() as u64,
                retained_bytes: LEGACY_FILE_HEADER.len() as u64,
                segment_count: 1,
                rollovers: 0,
                syncs: 0,
                chain_digest,
                durable_through_sequence: None,
                unsynced_records: 0,
                last_sync_at: now,
            });
        }

        validate_segment_path_sequence(&config.path, &segment_paths)?;
        let base_must_be_sealed = seal.is_some() || head.is_some() || segment_paths.len() > 1;

        // The file lock excludes a cooperative old writer while this process is running. Once a
        // v3 transition is sealed, persistent read-only permissions enforce the post-exit gate.
        let mut legacy_options = OpenOptions::new();
        legacy_options.read(true);
        if !base_must_be_sealed {
            legacy_options.append(true);
        }
        let mut legacy_file = legacy_options.open(&config.path)?;
        lock_file(&legacy_file, &config.path)?;
        if segment_paths.len() == 1
            && seal.is_none()
            && head.is_none()
            && legacy_file.metadata()?.len() == 0
        {
            let available = filesystem_available_bytes(&config.path)?;
            ensure_capacity(
                0,
                LEGACY_FILE_HEADER.len() as u64,
                config.max_retained_bytes,
                LEGACY_FILE_HEADER.len() as u64,
                available,
                config.filesystem_reserve_bytes,
            )?;
            legacy_file.write_all(LEGACY_FILE_HEADER)?;
            legacy_file.sync_data()?;
            sync_parent_directory(&config.path)?;
            let filesystem_available_bytes = filesystem_available_bytes(&config.path)?;
            return Ok(Self {
                file: legacy_file,
                path: config.path,
                writer_lock,
                legacy_lock: None,
                fsync: config.fsync,
                max_file_bytes: config.max_file_bytes,
                max_retained_bytes: config.max_retained_bytes,
                filesystem_reserve_bytes: config.filesystem_reserve_bytes,
                filesystem_available_bytes,
                base_sealed: false,
                active_segment_id: 0,
                next_sequence: 0,
                file_len: LEGACY_FILE_HEADER.len() as u64,
                retained_bytes: LEGACY_FILE_HEADER.len() as u64,
                segment_count: 1,
                rollovers: 0,
                syncs: 0,
                chain_digest: initial_chain_digest(LEGACY_FILE_HEADER),
                durable_through_sequence: None,
                unsynced_records: 0,
                last_sync_at: now,
            });
        }
        let mut legacy_file = Some(legacy_file);
        let validated = validate_segment_chain(&segment_paths)?;
        let active = validated.active;
        let retained_bytes = validated.retained_bytes;
        let terminal_head = v3_head_from_recovered(&active);
        let (base_sealed, head_relation) = match (seal.as_ref(), head.as_ref()) {
            (None, None) if segment_paths.len() == 1 => (false, None),
            (None, None) => {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "segmented repair WAL is missing its v3 seal and durable head checkpoint",
                ));
            }
            (Some(_), None) => {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "repair WAL v3 seal exists without its required durable head checkpoint",
                ));
            }
            (None, Some(head)) => {
                if segment_paths.len() != 1 {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "repair WAL head-without-seal transition contains numbered segments",
                    ));
                }
                validate_v3_head_exact(head, &terminal_head)?;
                let available = filesystem_available_bytes(&config.path)?;
                ensure_filesystem_reserve(
                    V3_SEAL_BYTES as u64,
                    available,
                    config.filesystem_reserve_bytes,
                )?;
                // A durable head is published before the seal. Seeing only the head is the one
                // recoverable transition state; finish it without allowing another legacy append.
                create_v3_seal(&config.path, &validated.base)?;
                (true, Some(V3HeadRelation::Exact))
            }
            (Some(seal), Some(head)) => {
                validate_v3_seal(seal, &validated.base)?;
                let relation = validate_v3_head_prefix(head, &terminal_head, &validated.segments)?;
                (true, Some(relation))
            }
        };
        if base_sealed {
            seal_legacy_file(
                legacy_file
                    .as_ref()
                    .expect("legacy file is locked during validation"),
                &config.path,
            )?;
        }
        if retained_bytes >= config.max_retained_bytes {
            return Err(io::Error::new(
                ErrorKind::StorageFull,
                format!(
                    "repair WAL retains {retained_bytes} bytes, at or above the configured hard ceiling {}",
                    config.max_retained_bytes
                ),
            ));
        }
        let active_path = active.descriptor.path.clone();
        let active_id = active.descriptor.id;
        let mut file = if active_id == 0 {
            legacy_file
                .take()
                .expect("legacy file is available for segment zero")
        } else {
            let active_file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(&active_path)?;
            lock_file(&active_file, &active_path)?;
            active_file
        };
        // No unproven tail is ever truncated. Validation above either proved every complete frame
        // or failed without changing a byte. Sync writable active files to establish the advertised
        // startup durability boundary; a sealed base was synced before its persistent seal.
        if active_id != 0 || !base_sealed {
            file.sync_data()?;
        }
        if head_relation == Some(V3HeadRelation::WalAhead) {
            // The prior process may have crashed after syncing WAL bytes but before publishing the
            // corresponding head/ACK. Preserve the fully validated tail, sync it again, then move
            // the durable checkpoint forward. No byte is truncated and no sequence is reused.
            file.sync_data()?;
            ensure_filesystem_reserve(
                V3_HEAD_BYTES as u64,
                filesystem_available_bytes(&config.path)?,
                config.filesystem_reserve_bytes,
            )?;
            publish_v3_head(&config.path, &terminal_head)?;
        }
        file.seek(SeekFrom::End(0))?;
        segment_paths.clear();
        let filesystem_available_bytes = filesystem_available_bytes(&config.path)?;

        Ok(Self {
            file,
            path: config.path,
            writer_lock,
            legacy_lock: legacy_file,
            fsync: config.fsync,
            max_file_bytes: config.max_file_bytes,
            max_retained_bytes: config.max_retained_bytes,
            filesystem_reserve_bytes: config.filesystem_reserve_bytes,
            filesystem_available_bytes,
            base_sealed,
            active_segment_id: active_id,
            next_sequence: active.next_sequence,
            file_len: active.valid_len,
            retained_bytes,
            segment_count: (active_id + 1),
            // Process-lifetime event counter; retained history is exposed by segment_count/id.
            rollovers: 0,
            syncs: 0,
            chain_digest: active.descriptor.chain_digest,
            durable_through_sequence: active.next_sequence.checked_sub(1),
            unsynced_records: 0,
            last_sync_at: now,
        })
    }

    /// Performs a read-only best-effort inventory before repair transport initialization. Exact
    /// segment bytes are reported even when frame validation fails, allowing hard-cap alerts to
    /// remain truthful for an oversized or corrupt generation.
    pub fn inspect(path: &Path) -> io::Result<RepairWalInspection> {
        validate_repair_wal_path(path)?;
        let segment_paths = discover_segment_paths(path)?;
        let available = filesystem_available_bytes(path).ok();
        let seal = read_v3_seal(path);
        let head = read_v3_head(path);
        if segment_paths.is_empty() {
            let v3_sealed = seal.as_ref().is_ok_and(Option::is_some);
            let validation_error = match (seal, head) {
                (Ok(None), Ok(None)) => None,
                (Err(error), _) | (_, Err(error)) => Some(error.to_string()),
                _ => Some(
                    "repair WAL v3 control state exists but every segment is missing; refusing sequence rollback"
                        .to_owned(),
                ),
            };
            return Ok(RepairWalInspection {
                retained_bytes: 0,
                active_segment_bytes: 0,
                segment_count: 0,
                active_segment_id: 0,
                filesystem_available_bytes: available,
                v3_sealed,
                validation_error,
            });
        }

        let active_segment_id = segment_paths.last().map_or(0, |(id, _)| *id);
        let active_segment_bytes = segment_paths
            .last()
            .map(|(_, path)| std::fs::metadata(path).map(|metadata| metadata.len()))
            .transpose()?
            .unwrap_or(0);
        let retained_bytes = segment_paths.iter().try_fold(0u64, |total, (_, path)| {
            total
                .checked_add(std::fs::metadata(path)?.len())
                .ok_or_else(|| io::Error::new(ErrorKind::StorageFull, "repair WAL size overflow"))
        })?;
        let v3_sealed = seal.as_ref().is_ok_and(Option::is_some);
        let validation = validate_segment_path_sequence(path, &segment_paths)
            .and_then(|()| validate_segment_chain(&segment_paths))
            .and_then(|validated| {
                validate_v3_control_exact_with_values(segment_paths.len(), &validated, seal, head)
            });
        Ok(RepairWalInspection {
            retained_bytes,
            active_segment_bytes,
            segment_count: segment_paths.len() as u64,
            active_segment_id,
            filesystem_available_bytes: available,
            v3_sealed,
            validation_error: validation.err().map(|error| error.to_string()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    pub fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    pub fn segment_count(&self) -> u64 {
        self.segment_count
    }

    pub fn active_segment_id(&self) -> u64 {
        self.active_segment_id
    }

    pub fn rollovers(&self) -> u64 {
        self.rollovers
    }

    pub fn syncs(&self) -> u64 {
        self.syncs
    }

    /// Inclusive global sequence known to have crossed this writer's latest `sync_data` boundary.
    pub fn durable_through_sequence(&self) -> Option<u64> {
        self.durable_through_sequence
    }

    pub fn max_file_bytes(&self) -> u64 {
        self.max_file_bytes
    }

    pub fn max_retained_bytes(&self) -> u64 {
        self.max_retained_bytes
    }

    pub fn filesystem_reserve_bytes(&self) -> u64 {
        self.filesystem_reserve_bytes
    }

    pub fn filesystem_available_bytes(&self) -> u64 {
        self.filesystem_available_bytes
    }

    pub fn v3_sealed(&self) -> bool {
        self.base_sealed
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.max_file_bytes.saturating_sub(self.file_len)
    }

    pub fn append(
        &mut self,
        provenance: &RepairProvenance,
        shred_payload: &[u8],
        now: Instant,
    ) -> io::Result<RepairWalAppend> {
        let sequence = self.next_sequence;
        let body = encode_body(sequence, provenance, shred_payload)?;
        if body.len() > MAX_FRAME_BODY_BYTES {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "repair WAL frame body is {} bytes; maximum is {MAX_FRAME_BODY_BYTES}",
                    body.len()
                ),
            ));
        }
        let body_len = u32::try_from(body.len()).map_err(|_| {
            io::Error::new(ErrorKind::InvalidData, "repair WAL frame length overflow")
        })?;
        let frame = encode_frame(body_len, &body);
        let frame_bytes = frame.len();
        let frame_bytes_u64 = frame_bytes as u64;
        let exceeds_active_target =
            self.file_len.checked_add(frame_bytes_u64).ok_or_else(|| {
                io::Error::new(ErrorKind::StorageFull, "repair WAL size overflow")
            })? > self.max_file_bytes;
        let must_roll = exceeds_active_target || (self.active_segment_id == 0 && self.base_sealed);
        let mut rolled_segment = false;
        if must_roll {
            let retained_required = (SEGMENT_HEADER_BYTES as u64)
                .checked_add(frame_bytes_u64)
                .ok_or_else(|| {
                    io::Error::new(ErrorKind::StorageFull, "repair WAL size overflow")
                })?;
            let seal_required =
                u64::from(self.active_segment_id == 0 && !self.base_sealed) * V3_SEAL_BYTES as u64;
            // Replacing an existing head temporarily consumes one extra head inode. The first v3
            // transition publishes a base head and later replaces it with the new-segment head, so
            // reserve two head records until the complete append/ACK sequence is finished.
            let head_required = if self.active_segment_id == 0 && !self.base_sealed {
                (V3_HEAD_BYTES as u64).saturating_mul(2)
            } else {
                V3_HEAD_BYTES as u64
            };
            self.ensure_admission(
                retained_required,
                retained_required
                    .saturating_add(seal_required)
                    .saturating_add(head_required),
            )?;
            self.roll_segment(now)?;
            rolled_segment = true;
        } else {
            let head_required = u64::from(self.base_sealed) * V3_HEAD_BYTES as u64;
            self.ensure_admission(
                frame_bytes_u64,
                frame_bytes_u64.saturating_add(head_required),
            )?;
        }
        let next_file_len = self
            .file_len
            .checked_add(frame_bytes_u64)
            .ok_or_else(|| io::Error::new(ErrorKind::StorageFull, "repair WAL size overflow"))?;
        if next_file_len > self.max_file_bytes {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "repair WAL frame ({frame_bytes} bytes) cannot fit the configured {}-byte segment",
                    self.max_file_bytes
                ),
            ));
        }

        self.file.write_all(&frame)?;
        self.chain_digest = extend_chain_digest(self.chain_digest.clone(), &frame);
        self.file_len = next_file_len;
        self.retained_bytes = self
            .retained_bytes
            .checked_add(frame_bytes_u64)
            .ok_or_else(|| io::Error::new(ErrorKind::StorageFull, "repair WAL size overflow"))?;
        self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidData, "repair WAL sequence exhausted")
        })?;
        self.unsynced_records = self.unsynced_records.saturating_add(1);

        let synced = self.sync_due(now)?;
        Ok(RepairWalAppend {
            sequence,
            frame_bytes,
            synced,
            rolled_segment,
            segment_id: self.active_segment_id,
        })
    }

    pub fn flush_and_sync(&mut self, now: Instant) -> io::Result<()> {
        self.file.flush()?;
        self.file.sync_data()?;
        if self.base_sealed {
            publish_v3_head(&self.path, &current_v3_head(self))?;
        }
        self.syncs = self.syncs.saturating_add(1);
        self.durable_through_sequence = self.next_sequence.checked_sub(1);
        self.unsynced_records = 0;
        self.last_sync_at = now;
        Ok(())
    }

    /// Enforces the time side of a batch fsync policy even when no newer record arrives.
    pub fn sync_if_due(&mut self, now: Instant) -> io::Result<bool> {
        if self.unsynced_records == 0 {
            return Ok(false);
        }
        self.sync_due(now)
    }

    pub fn read_all(path: &Path) -> io::Result<Vec<RepairWalEntry>> {
        Self::read_through(path, None)
    }

    /// Reads the validated segment chain through an optional inclusive durable sequence.
    /// Supplying a boundary lets a live read-only consumer ignore a concurrently visible active
    /// tail that has not crossed the writer's advertised fsync boundary.
    pub fn read_through(
        path: &Path,
        durable_through_sequence: Option<u64>,
    ) -> io::Result<Vec<RepairWalEntry>> {
        read_entries(path, durable_through_sequence)
    }

    fn sync_due(&mut self, now: Instant) -> io::Result<bool> {
        let due = match self.fsync {
            RepairWalFsyncPolicy::EveryRecord => true,
            RepairWalFsyncPolicy::Batch {
                max_unsynced_records,
                max_unsynced_age,
            } => {
                self.unsynced_records >= max_unsynced_records.get()
                    || elapsed(now, self.last_sync_at) >= max_unsynced_age
            }
        };
        if due {
            self.flush_and_sync(now)?;
        }
        Ok(due)
    }

    fn roll_segment(&mut self, now: Instant) -> io::Result<()> {
        if self.unsynced_records != 0 {
            self.flush_and_sync(now)?;
        }
        if self.active_segment_id == 0 && !self.base_sealed {
            // Head-before-seal makes seal-without-head impossible. A crash after this checkpoint
            // but before the seal is a recognized transition state that startup can finish.
            publish_v3_head(&self.path, &current_v3_head(self))?;
            create_v3_seal_from_values(&self.path, self.next_sequence, &self.chain_digest)?;
            seal_legacy_file(&self.file, &self.path)?;
            self.base_sealed = true;
        }
        let id = self.active_segment_id.checked_add(1).ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidData, "repair WAL segment id exhausted")
        })?;
        let previous_last_sequence = self.next_sequence.checked_sub(1).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "cannot roll an empty repair WAL segment",
            )
        })?;
        let header = SegmentHeader {
            id,
            first_sequence: self.next_sequence,
            previous_last_sequence,
            previous_chain_digest: self.chain_digest.clone(),
        };
        let header_bytes = encode_segment_header(&header);
        let path = segment_path(&self.path, id)?;
        let file = create_segment_file(&path, &header_bytes)?;

        let old_file = std::mem::replace(&mut self.file, file);
        if self.active_segment_id == 0 {
            self.legacy_lock = Some(old_file);
        } else {
            let _ = File::unlock(&old_file);
        }
        self.active_segment_id = id;
        self.file_len = header_bytes.len() as u64;
        self.retained_bytes = self
            .retained_bytes
            .checked_add(header_bytes.len() as u64)
            .ok_or_else(|| io::Error::new(ErrorKind::StorageFull, "repair WAL size overflow"))?;
        self.segment_count = self.segment_count.saturating_add(1);
        self.rollovers = self.rollovers.saturating_add(1);
        self.syncs = self.syncs.saturating_add(1);
        self.chain_digest = initial_chain_digest(&header_bytes);
        self.unsynced_records = 0;
        self.last_sync_at = now;
        // The empty new segment header is already durable. Publish its coordinates before any
        // frame can be accepted in it; if publication fails, startup proves this WAL-ahead state.
        publish_v3_head(&self.path, &current_v3_head(self))?;
        Ok(())
    }

    fn ensure_admission(
        &mut self,
        retained_required: u64,
        filesystem_required: u64,
    ) -> io::Result<()> {
        let available = filesystem_available_bytes(&self.path)?;
        ensure_capacity(
            self.retained_bytes,
            retained_required,
            self.max_retained_bytes,
            filesystem_required,
            available,
            self.filesystem_reserve_bytes,
        )?;
        self.filesystem_available_bytes = available.saturating_sub(filesystem_required);
        Ok(())
    }
}

impl Drop for RepairWal {
    fn drop(&mut self) {
        if self.unsynced_records != 0 {
            let _ = self.file.flush();
            let _ = self.file.sync_data();
        }
        let _ = File::unlock(&self.file);
        if let Some(file) = &self.legacy_lock {
            let _ = File::unlock(file);
        }
        let _ = File::unlock(&self.writer_lock);
    }
}

fn validate_repair_wal_config(config: &RepairWalConfig) -> io::Result<()> {
    validate_repair_wal_path(&config.path)?;
    if config.max_file_bytes < MIN_REPAIR_WAL_FILE_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("repair WAL max_file_bytes must be at least {MIN_REPAIR_WAL_FILE_BYTES}"),
        ));
    }
    if config.max_retained_bytes < config.max_file_bytes {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "repair WAL max_retained_bytes must be at least max_file_bytes",
        ));
    }
    if config.filesystem_reserve_bytes == 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "repair WAL filesystem_reserve_bytes must be nonzero",
        ));
    }
    Ok(())
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn validate_segment_path_sequence(path: &Path, segments: &[(u64, PathBuf)]) -> io::Result<()> {
    if segments.first().map(|(id, _)| *id) != Some(0) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("repair WAL segment zero is missing ({})", path.display()),
        ));
    }
    for (expected, (actual, _)) in segments.iter().enumerate() {
        let expected = expected as u64;
        if *actual != expected {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("repair WAL segment id discontinuity: expected {expected}, got {actual}"),
            ));
        }
    }
    Ok(())
}

fn ensure_capacity(
    retained_bytes: u64,
    retained_required: u64,
    max_retained_bytes: u64,
    filesystem_required: u64,
    filesystem_available: u64,
    filesystem_reserve: u64,
) -> io::Result<()> {
    let next_retained = retained_bytes
        .checked_add(retained_required)
        .ok_or_else(|| io::Error::new(ErrorKind::StorageFull, "repair WAL size overflow"))?;
    if next_retained > max_retained_bytes {
        return Err(io::Error::new(
            ErrorKind::StorageFull,
            format!(
                "repair WAL hard retained-byte ceiling would be exceeded: {retained_bytes} + {retained_required} > {max_retained_bytes}"
            ),
        ));
    }
    ensure_filesystem_reserve(
        filesystem_required,
        filesystem_available,
        filesystem_reserve,
    )
}

fn ensure_filesystem_reserve(required: u64, available: u64, reserve: u64) -> io::Result<()> {
    let required_with_reserve = required.checked_add(reserve).ok_or_else(|| {
        io::Error::new(
            ErrorKind::StorageFull,
            "repair WAL filesystem reserve calculation overflow",
        )
    })?;
    if available < required_with_reserve {
        return Err(io::Error::new(
            ErrorKind::StorageFull,
            format!(
                "repair WAL admission would breach filesystem reserve: available {available}, write {required}, required reserve {reserve}"
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn filesystem_available_bytes(path: &Path) -> io::Result<u64> {
    let parent = parent_directory(path);
    let path = CString::new(parent.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "repair WAL parent contains a NUL byte: {}",
                parent.display()
            ),
        )
    })?;
    let mut status = MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL terminated and `status` points to writable, correctly sized storage.
    let result = unsafe { libc::statvfs(path.as_ptr(), status.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: statvfs returned success and initialized the output structure.
    let status = unsafe { status.assume_init() };
    (status.f_bavail as u64)
        .checked_mul(status.f_frsize as u64)
        .ok_or_else(|| io::Error::new(ErrorKind::StorageFull, "filesystem size overflow"))
}

#[cfg(not(unix))]
fn filesystem_available_bytes(_path: &Path) -> io::Result<u64> {
    Err(io::Error::new(
        ErrorKind::Unsupported,
        "repair WAL filesystem reserve requires statvfs support",
    ))
}

fn validate_repair_wal_path(path: &Path) -> io::Result<()> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "repair WAL path has no UTF-8 file name",
        ));
    };
    let Some(stem) = name.strip_suffix(REPAIR_WAL_SUFFIX) else {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "repair WAL path must end in .repair.wal (refusing {})",
                path.display()
            ),
        ));
    };
    if stem.is_empty() || stem.contains(SEGMENT_MARKER) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "repair WAL base name must be nonempty and must not contain {SEGMENT_MARKER:?}"
            ),
        ));
    }
    Ok(())
}

fn repair_wal_stem(path: &Path) -> io::Result<&str> {
    validate_repair_wal_path(path)?;
    Ok(path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(REPAIR_WAL_SUFFIX))
        .expect("validated repair WAL path has a stem"))
}

fn writer_lock_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("validated repair WAL path has a UTF-8 file name");
    path.with_file_name(format!("{name}.writer.lock"))
}

fn v3_seal_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("validated repair WAL path has a UTF-8 file name");
    path.with_file_name(format!("{name}.v3-seal"))
}

fn v3_head_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("validated repair WAL path has a UTF-8 file name");
    path.with_file_name(format!("{name}.v3-head"))
}

fn segment_path(path: &Path, id: u64) -> io::Result<PathBuf> {
    if id == 0 {
        return Ok(path.to_path_buf());
    }
    let stem = repair_wal_stem(path)?;
    Ok(path.with_file_name(format!(
        "{stem}{SEGMENT_MARKER}{id:0SEGMENT_ID_WIDTH$}{REPAIR_WAL_SUFFIX}"
    )))
}

fn discover_segment_paths(path: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    validate_repair_wal_path(path)?;
    let stem = repair_wal_stem(path)?;
    let prefix = format!("{stem}{SEGMENT_MARKER}");
    let parent = parent_directory(path);
    let mut segments = Vec::new();
    if path.exists() {
        reject_symlink(path, "repair WAL segment zero")?;
        segments.push((0, path.to_path_buf()));
    }
    if !parent.exists() {
        return Ok(segments);
    }
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(id) = name
            .strip_prefix(&prefix)
            .and_then(|rest| rest.strip_suffix(REPAIR_WAL_SUFFIX))
        else {
            continue;
        };
        if id.len() != SEGMENT_ID_WIDTH || !id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("malformed repair WAL segment name {name:?}"),
            ));
        }
        let id = id.parse::<u64>().map_err(|error| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid repair WAL segment id in {name:?}: {error}"),
            )
        })?;
        if id == 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "repair WAL segment zero must use the configured legacy base path",
            ));
        }
        reject_symlink(&entry.path(), "repair WAL segment")?;
        segments.push((id, entry.path()));
    }
    segments.sort_by_key(|(id, _)| *id);
    Ok(segments)
}

fn reject_symlink(path: &Path, kind: &str) -> io::Result<()> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("{kind} must not be a symbolic link: {}", path.display()),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn lock_file(file: &File, path: &Path) -> io::Result<()> {
    file.try_lock().map_err(|error| match error {
        TryLockError::Error(error) => io::Error::new(
            error.kind(),
            format!("cannot lock repair WAL ({}): {error}", path.display()),
        ),
        TryLockError::WouldBlock => io::Error::new(
            ErrorKind::WouldBlock,
            format!(
                "repair WAL is already open by another writer ({})",
                path.display()
            ),
        ),
    })
}

fn sync_parent_directory(path: &Path) -> io::Result<()> {
    File::open(parent_directory(path))?.sync_all()
}

/// Publishes a complete segment header without ever replacing an existing segment path.
///
/// The staging inode lives in the destination directory, is fully written and synced first, and
/// is then installed with `hard_link`, whose create-if-absent semantics are atomic. A crash or I/O
/// error before publication can therefore leave only an ignored staging name; a restart still sees
/// the preceding full segment and can retry rotation. A competing or pre-existing destination
/// makes `hard_link` fail instead of silently overwriting provenance.
fn create_segment_file(path: &Path, header: &[u8]) -> io::Result<File> {
    reject_symlink(path, "repair WAL segment")?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "non-UTF-8 segment filename"))?;

    for _ in 0..MAX_STAGING_NAME_ATTEMPTS {
        let ticket = NEXT_STAGING_ID.fetch_add(1, Ordering::Relaxed);
        let staging = path.with_file_name(format!(
            ".{file_name}.creating-{}-{ticket:020}",
            std::process::id()
        ));
        let mut file = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&staging)
        {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };

        let publish: io::Result<()> = (|| {
            lock_file(&file, path)?;
            file.write_all(header)?;
            file.sync_data()?;
            // Refuse a symlink that appeared after the initial check; hard_link itself also
            // refuses to replace any kind of existing destination.
            reject_symlink(path, "repair WAL segment")?;
            std::fs::hard_link(&staging, path)?;
            sync_parent_directory(path)?;
            Ok(())
        })();

        // Staging files never contain accepted records, only a not-yet-published header. Cleanup
        // is best effort: if it fails, the exact segment discovery pattern deliberately ignores
        // the staging name and the durable published link remains authoritative.
        let _ = std::fs::remove_file(&staging);
        publish?;
        file.seek(SeekFrom::End(0))?;
        return Ok(file);
    }

    Err(io::Error::new(
        ErrorKind::AlreadyExists,
        format!(
            "cannot allocate a unique repair WAL staging name for {}",
            path.display()
        ),
    ))
}

/// Atomically replaces a small control checkpoint in the WAL directory.
///
/// The temporary inode is fully written, made read-only, and synced before rename. The parent
/// directory sync is the final publication boundary and must complete before an accepted-repair
/// ACK may be returned.
fn replace_control_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    reject_symlink(path, "repair WAL control file")?;
    if let Ok(metadata) = path.symlink_metadata()
        && !metadata.is_file()
    {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL control path is not a regular file: {}",
                path.display()
            ),
        ));
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "non-UTF-8 control filename"))?;

    for _ in 0..MAX_STAGING_NAME_ATTEMPTS {
        let ticket = NEXT_STAGING_ID.fetch_add(1, Ordering::Relaxed);
        let staging = path.with_file_name(format!(
            ".{file_name}.checkpoint-{}-{ticket:020}",
            std::process::id()
        ));
        let mut file = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&staging)
        {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };

        let publish: io::Result<()> = (|| {
            file.write_all(bytes)?;
            let mut permissions = file.metadata()?.permissions();
            permissions.set_readonly(true);
            file.set_permissions(permissions)?;
            file.sync_all()?;
            reject_symlink(path, "repair WAL control file")?;
            std::fs::rename(&staging, path)?;
            sync_parent_directory(path)
        })();
        let _ = std::fs::remove_file(&staging);
        return publish;
    }

    Err(io::Error::new(
        ErrorKind::AlreadyExists,
        format!(
            "cannot allocate a unique repair WAL control checkpoint for {}",
            path.display()
        ),
    ))
}

fn encode_v3_seal(seal: &V3Seal) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(V3_SEAL_BYTES);
    bytes.extend_from_slice(V3_SEAL_MAGIC);
    bytes.extend_from_slice(&seal.next_sequence.to_le_bytes());
    bytes.extend_from_slice(seal.base_chain_digest.as_ref());
    debug_assert_eq!(bytes.len(), V3_SEAL_BODY_BYTES);
    bytes.extend_from_slice(&crc32(&bytes).to_le_bytes());
    bytes
}

fn read_v3_seal(path: &Path) -> io::Result<Option<V3Seal>> {
    let path = v3_seal_path(path);
    reject_symlink(&path, "repair WAL v3 seal")?;
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path)?;
    if bytes.len() != V3_SEAL_BYTES || &bytes[..V3_SEAL_MAGIC.len()] != V3_SEAL_MAGIC {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("{} is not a valid repair WAL v3 seal", path.display()),
        ));
    }
    let expected = u32::from_le_bytes(
        bytes[V3_SEAL_BODY_BYTES..]
            .try_into()
            .expect("fixed v3 seal checksum"),
    );
    let actual = crc32(&bytes[..V3_SEAL_BODY_BYTES]);
    if actual != expected {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL v3 seal checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
        ));
    }
    let mut decoder = Decoder::new(&bytes[V3_SEAL_MAGIC.len()..V3_SEAL_BODY_BYTES]);
    let seal = V3Seal {
        next_sequence: decoder.u64()?,
        base_chain_digest: Hash::new_from_array(decoder.array()?),
    };
    debug_assert!(decoder.is_empty());
    Ok(Some(seal))
}

fn validate_v3_seal(seal: &V3Seal, base: &RepairWalSegment) -> io::Result<()> {
    let expected_next_sequence = base
        .last_sequence
        .and_then(|sequence| sequence.checked_add(1))
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "repair WAL v3 seal cannot bind an empty or exhausted legacy base",
            )
        })?;
    if seal.next_sequence != expected_next_sequence || seal.base_chain_digest != base.chain_digest {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL legacy base no longer matches its persistent v3 seal",
        ));
    }
    Ok(())
}

fn create_v3_seal(path: &Path, base: &RepairWalSegment) -> io::Result<()> {
    let next_sequence = base
        .last_sequence
        .and_then(|sequence| sequence.checked_add(1))
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "cannot seal an empty or exhausted repair WAL legacy base",
            )
        })?;
    create_v3_seal_from_values(path, next_sequence, &base.chain_digest)
}

fn create_v3_seal_from_values(
    path: &Path,
    next_sequence: u64,
    base_chain_digest: &Hash,
) -> io::Result<()> {
    if next_sequence == 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "cannot seal an empty repair WAL legacy base",
        ));
    }
    let seal = V3Seal {
        next_sequence,
        base_chain_digest: base_chain_digest.clone(),
    };
    let seal_path = v3_seal_path(path);
    let file = create_segment_file(&seal_path, &encode_v3_seal(&seal))?;
    make_file_readonly(&file, &seal_path)?;
    Ok(())
}

fn encode_v3_head(head: &V3Head) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(V3_HEAD_BYTES);
    bytes.extend_from_slice(V3_HEAD_MAGIC);
    bytes.extend_from_slice(&head.active_segment_id.to_le_bytes());
    bytes.extend_from_slice(&head.active_file_len.to_le_bytes());
    bytes.extend_from_slice(&head.next_sequence.to_le_bytes());
    bytes.extend_from_slice(head.chain_digest.as_ref());
    debug_assert_eq!(bytes.len(), V3_HEAD_BODY_BYTES);
    bytes.extend_from_slice(&crc32(&bytes).to_le_bytes());
    bytes
}

fn read_v3_head(path: &Path) -> io::Result<Option<V3Head>> {
    let path = v3_head_path(path);
    reject_symlink(&path, "repair WAL v3 head")?;
    if !path.exists() {
        return Ok(None);
    }
    let metadata = path.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("{} is not a regular repair WAL v3 head", path.display()),
        ));
    }
    let bytes = std::fs::read(&path)?;
    if bytes.len() != V3_HEAD_BYTES || &bytes[..V3_HEAD_MAGIC.len()] != V3_HEAD_MAGIC {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("{} is not a valid repair WAL v3 head", path.display()),
        ));
    }
    let expected = u32::from_le_bytes(
        bytes[V3_HEAD_BODY_BYTES..]
            .try_into()
            .expect("fixed v3 head checksum"),
    );
    let actual = crc32(&bytes[..V3_HEAD_BODY_BYTES]);
    if actual != expected {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL v3 head checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
        ));
    }
    let mut decoder = Decoder::new(&bytes[V3_HEAD_MAGIC.len()..V3_HEAD_BODY_BYTES]);
    let head = V3Head {
        active_segment_id: decoder.u64()?,
        active_file_len: decoder.u64()?,
        next_sequence: decoder.u64()?,
        chain_digest: Hash::new_from_array(decoder.array()?),
    };
    debug_assert!(decoder.is_empty());
    Ok(Some(head))
}

fn publish_v3_head(path: &Path, head: &V3Head) -> io::Result<()> {
    replace_control_file(&v3_head_path(path), &encode_v3_head(head))
}

fn v3_head_from_recovered(active: &RecoveredSegment) -> V3Head {
    V3Head {
        active_segment_id: active.descriptor.id,
        active_file_len: active.valid_len,
        next_sequence: active.next_sequence,
        chain_digest: active.descriptor.chain_digest,
    }
}

fn current_v3_head(wal: &RepairWal) -> V3Head {
    V3Head {
        active_segment_id: wal.active_segment_id,
        active_file_len: wal.file_len,
        next_sequence: wal.next_sequence,
        chain_digest: wal.chain_digest,
    }
}

fn validate_v3_head_exact(head: &V3Head, terminal: &V3Head) -> io::Result<()> {
    if head != terminal {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL durable head does not match the validated terminal state (head segment {}, length {}, next sequence {}; WAL segment {}, length {}, next sequence {})",
                head.active_segment_id,
                head.active_file_len,
                head.next_sequence,
                terminal.active_segment_id,
                terminal.active_file_len,
                terminal.next_sequence,
            ),
        ));
    }
    Ok(())
}

/// Classifies an exact durable head or a fully provable WAL-ahead crash tail. A head that points
/// beyond, outside, or to different bytes in the validated chain always fails closed.
fn validate_v3_head_prefix(
    head: &V3Head,
    terminal: &V3Head,
    segments: &[RepairWalSegment],
) -> io::Result<V3HeadRelation> {
    if head.active_segment_id > terminal.active_segment_id {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL is behind its durable head: highest segment {}, head segment {}",
                terminal.active_segment_id, head.active_segment_id
            ),
        ));
    }
    let index = usize::try_from(head.active_segment_id).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL head segment id exceeds usize",
        )
    })?;
    let descriptor = segments.get(index).ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL durable head references missing segment {}",
                head.active_segment_id
            ),
        )
    })?;
    validate_v3_head_segment_prefix(head, descriptor, segments)?;
    if head == terminal {
        Ok(V3HeadRelation::Exact)
    } else {
        Ok(V3HeadRelation::WalAhead)
    }
}

fn validate_v3_head_segment_prefix(
    head: &V3Head,
    descriptor: &RepairWalSegment,
    segments: &[RepairWalSegment],
) -> io::Result<()> {
    if head.active_file_len > descriptor.file_bytes {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL segment {} is truncated behind durable head length {} (actual {})",
                head.active_segment_id, head.active_file_len, descriptor.file_bytes
            ),
        ));
    }
    let mut file = File::open(&descriptor.path)?;
    let header_bytes = if descriptor.id == 0 {
        legacy_header(&mut file, &descriptor.path)?
    } else {
        let (header, bytes) = decode_segment_header(&mut file, &descriptor.path)?;
        let previous = segments
            .get(descriptor.id.saturating_sub(1) as usize)
            .ok_or_else(|| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    "repair WAL head predecessor missing",
                )
            })?;
        validate_segment_header(
            &header,
            descriptor.id,
            descriptor.first_sequence,
            Some(previous),
        )?;
        bytes
    };
    let minimum_len = header_bytes.len() as u64;
    if head.active_file_len < minimum_len {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL durable head ends inside its segment header",
        ));
    }
    let mut position = minimum_len;
    let mut next_sequence = descriptor.first_sequence;
    let mut chain_digest = initial_chain_digest(&header_bytes);
    while position < head.active_file_len {
        let frame = read_frame(&mut file)?.ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "repair WAL ended before its durable head boundary",
            )
        })?;
        position = file.stream_position()?;
        if position > head.active_file_len {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "repair WAL durable head ends inside a frame",
            ));
        }
        let sequence = frame_sequence(&frame.body);
        if sequence != next_sequence {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "repair WAL sequence discontinuity before durable head: expected {next_sequence}, got {sequence}"
                ),
            ));
        }
        next_sequence = next_sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidData, "repair WAL sequence overflow")
        })?;
        chain_digest = extend_chain_digest(chain_digest, &frame.encoded);
    }
    if position != head.active_file_len
        || next_sequence != head.next_sequence
        || chain_digest != head.chain_digest
    {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL durable head coordinates or chain digest do not match its exact prefix",
        ));
    }
    Ok(())
}

fn validate_v3_control_exact(
    path: &Path,
    segment_count: usize,
    validated: &ValidatedChain,
) -> io::Result<()> {
    validate_v3_control_exact_with_values(
        segment_count,
        validated,
        read_v3_seal(path),
        read_v3_head(path),
    )
}

fn validate_v3_control_exact_with_values(
    segment_count: usize,
    validated: &ValidatedChain,
    seal: io::Result<Option<V3Seal>>,
    head: io::Result<Option<V3Head>>,
) -> io::Result<()> {
    let seal = seal?;
    let head = head?;
    match (seal.as_ref(), head.as_ref()) {
        (None, None) if segment_count == 1 => Ok(()),
        (None, None) => Err(io::Error::new(
            ErrorKind::InvalidData,
            "segmented repair WAL is missing its v3 seal and durable head checkpoint",
        )),
        (Some(_), None) => Err(io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL v3 seal exists without its required durable head checkpoint",
        )),
        (None, Some(_)) => Err(io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL has a transitional v3 head without a seal; only the writer may finish this transition",
        )),
        (Some(seal), Some(head)) => {
            validate_v3_seal(seal, &validated.base)?;
            validate_v3_head_exact(head, &v3_head_from_recovered(&validated.active))
        }
    }
}

fn make_file_readonly(file: &File, path: &Path) -> io::Result<()> {
    let mut permissions = file.metadata()?.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        file.set_permissions(permissions)?;
        file.sync_all()?;
        sync_parent_directory(path)?;
    }
    Ok(())
}

fn seal_legacy_file(file: &File, path: &Path) -> io::Result<()> {
    make_file_readonly(file, path)
}

fn encode_segment_header(header: &SegmentHeader) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(SEGMENT_HEADER_BYTES);
    bytes.extend_from_slice(SEGMENT_FILE_MAGIC);
    bytes.extend_from_slice(&header.id.to_le_bytes());
    bytes.extend_from_slice(&header.first_sequence.to_le_bytes());
    bytes.extend_from_slice(&header.previous_last_sequence.to_le_bytes());
    bytes.extend_from_slice(header.previous_chain_digest.as_ref());
    debug_assert_eq!(bytes.len(), SEGMENT_HEADER_BODY_BYTES);
    bytes.extend_from_slice(&crc32(&bytes).to_le_bytes());
    debug_assert_eq!(bytes.len(), SEGMENT_HEADER_BYTES);
    bytes
}

fn decode_segment_header(file: &mut File, path: &Path) -> io::Result<(SegmentHeader, Vec<u8>)> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = vec![0u8; SEGMENT_HEADER_BYTES];
    file.read_exact(&mut bytes).map_err(|error| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "{} is not a complete segmented repair WAL header: {error}",
                path.display()
            ),
        )
    })?;
    if &bytes[..SEGMENT_FILE_MAGIC.len()] != SEGMENT_FILE_MAGIC {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("{} is not a v3 repair WAL segment", path.display()),
        ));
    }
    let expected = u32::from_le_bytes(
        bytes[SEGMENT_HEADER_BODY_BYTES..]
            .try_into()
            .expect("fixed segment header checksum"),
    );
    let actual = crc32(&bytes[..SEGMENT_HEADER_BODY_BYTES]);
    if actual != expected {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL segment header checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
        ));
    }
    let mut decoder = Decoder::new(&bytes[SEGMENT_FILE_MAGIC.len()..SEGMENT_HEADER_BODY_BYTES]);
    let header = SegmentHeader {
        id: decoder.u64()?,
        first_sequence: decoder.u64()?,
        previous_last_sequence: decoder.u64()?,
        previous_chain_digest: Hash::new_from_array(decoder.array()?),
    };
    debug_assert!(decoder.is_empty());
    Ok((header, bytes))
}

fn legacy_header(file: &mut File, path: &Path) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = vec![0u8; LEGACY_FILE_HEADER.len()];
    file.read_exact(&mut header).map_err(|error| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "{} is not a complete repair WAL header: {error}",
                path.display()
            ),
        )
    })?;
    if header != LEGACY_FILE_HEADER {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("{} is not a repair WAL; refusing to append", path.display()),
        ));
    }
    Ok(header)
}

fn validate_segment_header(
    header: &SegmentHeader,
    id: u64,
    expected_sequence: u64,
    previous: Option<&RepairWalSegment>,
) -> io::Result<()> {
    if header.id != id {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL header id {} does not match filename id {id}",
                header.id
            ),
        ));
    }
    if header.first_sequence != expected_sequence {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "repair WAL segment {id} starts at sequence {}, expected {expected_sequence}",
                header.first_sequence
            ),
        ));
    }
    let previous = previous.ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("repair WAL segment {id} has no preceding segment"),
        )
    })?;
    let expected_previous = previous.last_sequence.unwrap_or(NO_PREVIOUS_SEQUENCE);
    if header.previous_last_sequence != expected_previous
        || header.previous_chain_digest != previous.chain_digest
    {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("repair WAL segment {id} does not match the preceding segment chain"),
        ));
    }
    Ok(())
}

fn validate_segment_chain(segment_paths: &[(u64, PathBuf)]) -> io::Result<ValidatedChain> {
    let final_index = segment_paths.len().checked_sub(1).ok_or_else(|| {
        io::Error::new(ErrorKind::NotFound, "repair WAL has no retained segments")
    })?;
    let mut expected_sequence = 0u64;
    let mut previous: Option<RepairWalSegment> = None;
    let mut base = None;
    let mut retained_bytes = 0u64;
    let mut active = None;
    let mut descriptors = Vec::with_capacity(segment_paths.len());
    for (index, (id, path)) in segment_paths.iter().enumerate() {
        let is_active = index == final_index;
        let recovered =
            recover_segment(path, *id, is_active, expected_sequence, previous.as_ref())?;
        expected_sequence = recovered.next_sequence;
        retained_bytes = retained_bytes
            .checked_add(recovered.valid_len)
            .ok_or_else(|| io::Error::new(ErrorKind::StorageFull, "repair WAL size overflow"))?;
        if *id == 0 {
            base = Some(recovered.descriptor.clone());
        }
        descriptors.push(recovered.descriptor.clone());
        previous = Some(recovered.descriptor.clone());
        if is_active {
            active = Some(recovered);
        }
    }
    Ok(ValidatedChain {
        base: base.expect("validated chain begins with segment zero"),
        active: active.expect("nonempty segment list has an active segment"),
        retained_bytes,
        segments: descriptors,
    })
}

fn recover_segment(
    path: &Path,
    id: u64,
    is_active: bool,
    mut expected_sequence: u64,
    previous: Option<&RepairWalSegment>,
) -> io::Result<RecoveredSegment> {
    reject_symlink(path, "repair WAL segment")?;
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let (first_sequence, header_bytes) = if id == 0 {
        if expected_sequence != 0 || previous.is_some() {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "legacy repair WAL must be the first segment at sequence zero",
            ));
        }
        (0, legacy_header(&mut file, path)?)
    } else {
        let (header, bytes) = decode_segment_header(&mut file, path)?;
        validate_segment_header(&header, id, expected_sequence, previous)?;
        (header.first_sequence, bytes)
    };
    let mut valid_len = header_bytes.len() as u64;
    let mut chain_digest = initial_chain_digest(&header_bytes);
    while valid_len < file_len {
        let frame = read_frame(&mut file)?.ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "repair WAL ended before the advertised file length",
            )
        })?;
        let sequence = frame_sequence(&frame.body);
        if sequence != expected_sequence {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "repair WAL sequence discontinuity: expected {expected_sequence}, got {sequence}"
                ),
            ));
        }
        expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidData, "repair WAL sequence overflow")
        })?;
        chain_digest = extend_chain_digest(chain_digest, &frame.encoded);
        valid_len = file.stream_position()?;
    }
    let last_sequence = (expected_sequence > first_sequence).then_some(expected_sequence - 1);
    Ok(RecoveredSegment {
        descriptor: RepairWalSegment {
            id,
            path: path.to_path_buf(),
            first_sequence,
            last_sequence,
            file_bytes: valid_len,
            chain_digest,
            sealed: !is_active,
        },
        valid_len,
        next_sequence: expected_sequence,
    })
}

fn read_entries(path: &Path, durable_through: Option<u64>) -> io::Result<Vec<RepairWalEntry>> {
    let paths = discover_segment_paths(path)?;
    validate_segment_path_sequence(path, &paths)?;
    let validated = validate_segment_chain(&paths)?;
    validate_v3_control_exact(path, paths.len(), &validated)?;
    let mut entries = Vec::new();
    let mut expected_sequence = 0u64;
    let mut previous: Option<RepairWalSegment> = None;
    for (position, (id, path)) in paths.iter().enumerate() {
        if *id != position as u64 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("repair WAL segment id discontinuity: expected {position}, got {id}"),
            ));
        }
        let mut file = File::open(path)?;
        let (first_sequence, header_bytes) = if *id == 0 {
            (0, legacy_header(&mut file, path)?)
        } else {
            let (header, bytes) = decode_segment_header(&mut file, path)?;
            validate_segment_header(&header, *id, expected_sequence, previous.as_ref())?;
            (header.first_sequence, bytes)
        };
        let mut chain_digest = initial_chain_digest(&header_bytes);
        let mut last_sequence = None;
        loop {
            if durable_through.is_some_and(|through| expected_sequence > through) {
                return Ok(entries);
            }
            let Some(frame) = read_frame(&mut file)? else {
                break;
            };
            let entry = decode_body(&frame.body)?;
            if entry.sequence != expected_sequence {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "repair WAL sequence discontinuity: expected {expected_sequence}, got {}",
                        entry.sequence
                    ),
                ));
            }
            last_sequence = Some(entry.sequence);
            expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
                io::Error::new(ErrorKind::InvalidData, "repair WAL sequence overflow")
            })?;
            chain_digest = extend_chain_digest(chain_digest, &frame.encoded);
            entries.push(entry);
        }
        previous = Some(RepairWalSegment {
            id: *id,
            path: path.clone(),
            first_sequence,
            last_sequence,
            file_bytes: file.metadata()?.len(),
            chain_digest,
            sealed: position + 1 != paths.len(),
        });
    }
    if let Some(through) = durable_through
        && expected_sequence <= through
    {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            format!(
                "repair WAL durable boundary {through} is unavailable; next sequence is {expected_sequence}"
            ),
        ));
    }
    Ok(entries)
}

struct ReadFrame {
    body: Vec<u8>,
    encoded: Vec<u8>,
}

fn read_frame(file: &mut File) -> io::Result<Option<ReadFrame>> {
    let frame_start = file.stream_position()?;
    let mut prefix = [0u8; FRAME_PREFIX_BYTES];
    match file.read_exact(&mut prefix) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
            if file.stream_position()? == frame_start {
                return Ok(None);
            }
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "truncated repair WAL frame prefix",
            ));
        }
        Err(error) => return Err(error),
    }
    let body_len = u32::from_le_bytes(prefix) as usize;
    if !(MIN_BODY_BYTES..=MAX_FRAME_BODY_BYTES).contains(&body_len) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid repair WAL frame body length {body_len}"),
        ));
    }
    let mut body = vec![0u8; body_len];
    if let Err(error) = file.read_exact(&mut body) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("truncated repair WAL frame body: {error}"),
        ));
    }
    let mut checksum_bytes = [0u8; FRAME_CHECKSUM_BYTES];
    if let Err(error) = file.read_exact(&mut checksum_bytes) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("truncated repair WAL checksum: {error}"),
        ));
    }
    let expected = u32::from_le_bytes(checksum_bytes);
    let actual = crc32(&body);
    if actual != expected {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("repair WAL checksum mismatch: expected {expected:#010x}, got {actual:#010x}"),
        ));
    }
    let mut encoded = Vec::with_capacity(FRAME_PREFIX_BYTES + body.len() + FRAME_CHECKSUM_BYTES);
    encoded.extend_from_slice(&prefix);
    encoded.extend_from_slice(&body);
    encoded.extend_from_slice(&checksum_bytes);
    Ok(Some(ReadFrame { body, encoded }))
}

fn encode_frame(body_len: u32, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(FRAME_PREFIX_BYTES + body.len() + FRAME_CHECKSUM_BYTES);
    frame.extend_from_slice(&body_len.to_le_bytes());
    frame.extend_from_slice(body);
    frame.extend_from_slice(&crc32(body).to_le_bytes());
    frame
}

fn frame_sequence(body: &[u8]) -> u64 {
    u64::from_le_bytes(
        body[..MIN_BODY_BYTES]
            .try_into()
            .expect("minimum body checked"),
    )
}

fn initial_chain_digest(header: &[u8]) -> Hash {
    sha256_parts(&[b"shred-repair-wal-segment-chain-v1", header])
}

fn extend_chain_digest(previous: Hash, frame: &[u8]) -> Hash {
    sha256_parts(&[
        b"shred-repair-wal-segment-chain-v1",
        previous.as_ref(),
        frame,
    ])
}

fn sha256_parts(parts: &[&[u8]]) -> Hash {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    Hash::new_from_array(hasher.finalize().into())
}

fn encode_body(
    sequence: u64,
    provenance: &RepairProvenance,
    shred_payload: &[u8],
) -> io::Result<Vec<u8>> {
    let peer_addr = provenance.peer_addr.as_bytes();
    let peer_addr_len = u16::try_from(peer_addr.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "repair peer address is too long"))?;
    let payload_len = u32::try_from(shred_payload.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "repair shred payload is too long"))?;
    let (request_kind, request_slot, request_index) = encode_request(provenance.request);
    let mut body = Vec::with_capacity(256 + peer_addr.len() + shred_payload.len());
    body.extend_from_slice(&sequence.to_le_bytes());
    body.extend_from_slice(&provenance.received_at_unix_ms.to_le_bytes());
    body.extend_from_slice(&provenance.nonce.to_le_bytes());
    body.push(request_kind);
    body.extend_from_slice(&request_slot.to_le_bytes());
    body.extend_from_slice(&request_index.to_le_bytes());
    body.extend_from_slice(&peer_addr_len.to_le_bytes());
    body.extend_from_slice(peer_addr);
    body.extend_from_slice(provenance.peer_pubkey.as_ref());
    body.extend_from_slice(&provenance.shred_slot.to_le_bytes());
    body.extend_from_slice(&provenance.shred_index.to_le_bytes());
    body.extend_from_slice(&provenance.fec_set_index.to_le_bytes());
    body.extend_from_slice(&provenance.shred_version.to_le_bytes());
    body.extend_from_slice(provenance.expected_slot_leader.as_ref());
    body.extend_from_slice(provenance.fec_merkle_root.as_ref());
    body.extend_from_slice(&provenance.trust_anchor_fec_set_index.to_le_bytes());
    body.push(u8::from(provenance.learned_chained_merkle_root));
    match provenance.chained_merkle_root {
        Some(root) => {
            body.push(1);
            body.extend_from_slice(root.as_ref());
        }
        None => body.push(0),
    }
    body.extend_from_slice(provenance.leader_signature.as_ref());
    body.extend_from_slice(&payload_len.to_le_bytes());
    body.extend_from_slice(shred_payload);
    Ok(body)
}

fn decode_body(body: &[u8]) -> io::Result<RepairWalEntry> {
    let mut decoder = Decoder::new(body);
    let sequence = decoder.u64()?;
    let received_at_unix_ms = decoder.u64()?;
    let nonce = decoder.u32()?;
    let request_kind = decoder.u8()?;
    let request_slot = decoder.u64()?;
    let request_index = decoder.u64()?;
    let request = decode_request(request_kind, request_slot, request_index)?;
    let peer_addr_len = decoder.u16()? as usize;
    let peer_addr = String::from_utf8(decoder.bytes(peer_addr_len)?.to_vec()).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL peer address is not UTF-8",
        )
    })?;
    let peer_pubkey = Pubkey::new_from_array(decoder.array()?);
    let shred_slot = decoder.u64()?;
    let shred_index = decoder.u32()?;
    let fec_set_index = decoder.u32()?;
    let shred_version = decoder.u16()?;
    let expected_slot_leader = Pubkey::new_from_array(decoder.array()?);
    let fec_merkle_root = Hash::new_from_array(decoder.array()?);
    let trust_anchor_fec_set_index = decoder.u32()?;
    let learned_chained_merkle_root = match decoder.u8()? {
        0 => false,
        1 => true,
        value => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid repair WAL learned-chain flag {value}"),
            ));
        }
    };
    let chained_merkle_root = match decoder.u8()? {
        0 => None,
        1 => Some(Hash::new_from_array(decoder.array()?)),
        value => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid repair WAL chained-root flag {value}"),
            ));
        }
    };
    let leader_signature = Signature::from(decoder.array::<64>()?);
    let payload_len = decoder.u32()? as usize;
    let shred_payload = decoder.bytes(payload_len)?.to_vec();
    if !decoder.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL frame has trailing bytes",
        ));
    }
    Ok(RepairWalEntry {
        sequence,
        provenance: RepairProvenance {
            received_at_unix_ms,
            nonce,
            request,
            peer_addr,
            peer_pubkey,
            shred_slot,
            shred_index,
            fec_set_index,
            shred_version,
            expected_slot_leader,
            fec_merkle_root,
            trust_anchor_fec_set_index,
            learned_chained_merkle_root,
            chained_merkle_root,
            leader_signature,
        },
        shred_payload,
    })
}

fn encode_request(request: ShredRepairRequest) -> (u8, u64, u64) {
    match request {
        ShredRepairRequest::Shred { slot, shred_index } => (0, slot, shred_index),
        ShredRepairRequest::HighestShred { slot, shred_index } => (1, slot, shred_index),
        ShredRepairRequest::Orphan { slot } => (2, slot, 0),
    }
}

fn decode_request(kind: u8, slot: u64, index: u64) -> io::Result<ShredRepairRequest> {
    match kind {
        0 => Ok(ShredRepairRequest::Shred {
            slot,
            shred_index: index,
        }),
        1 => Ok(ShredRepairRequest::HighestShred {
            slot,
            shred_index: index,
        }),
        2 if index == 0 => Ok(ShredRepairRequest::Orphan { slot }),
        _ => Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid repair WAL request kind/index {kind}/{index}"),
        )),
    }
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn bytes(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let Some((head, tail)) = self.remaining.split_at_checked(len) else {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "truncated repair WAL record",
            ));
        };
        self.remaining = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        Ok(self
            .bytes(N)?
            .try_into()
            .expect("decoder requested a fixed checked length"))
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

fn elapsed(now: Instant, earlier: Instant) -> Duration {
    now.checked_duration_since(earlier)
        .unwrap_or(Duration::ZERO)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320u32 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn provenance() -> RepairProvenance {
        RepairProvenance {
            received_at_unix_ms: 1_723_456_789_012,
            nonce: 42,
            request: ShredRepairRequest::Shred {
                slot: 123,
                shred_index: 7,
            },
            peer_addr: "127.0.0.1:8000".into(),
            peer_pubkey: Pubkey::new_from_array([1; 32]),
            shred_slot: 123,
            shred_index: 7,
            fec_set_index: 0,
            shred_version: 50093,
            expected_slot_leader: Pubkey::new_from_array([2; 32]),
            fec_merkle_root: Hash::new_from_array([3; 32]),
            trust_anchor_fec_set_index: 0,
            learned_chained_merkle_root: false,
            chained_merkle_root: Some(Hash::new_from_array([4; 32])),
            leader_signature: Signature::from([5; 64]),
        }
    }

    #[test]
    fn round_trips_provenance_and_payload() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("accepted.repair.wal");
        let now = Instant::now();
        {
            let mut wal = RepairWal::open(
                RepairWalConfig {
                    path: path.clone(),
                    fsync: RepairWalFsyncPolicy::EveryRecord,
                    max_file_bytes: 1024 * 1024,
                    max_retained_bytes: 64 * 1024 * 1024,
                    filesystem_reserve_bytes: 1,
                },
                now,
            )
            .unwrap();
            let append = wal.append(&provenance(), &[9, 8, 7], now).unwrap();
            assert_eq!(append.sequence, 0);
            assert!(append.synced);
        }

        let entries = RepairWal::read_all(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[0].provenance, provenance());
        assert_eq!(entries[0].shred_payload, [9, 8, 7]);
    }

    #[test]
    fn refuses_non_repair_file_and_wrong_suffix() {
        let directory = tempdir().unwrap();
        let wrong_suffix = directory.path().join("raw.wal");
        assert!(
            RepairWal::open(
                RepairWalConfig {
                    path: wrong_suffix,
                    fsync: RepairWalFsyncPolicy::EveryRecord,
                    max_file_bytes: 1024 * 1024,
                    max_retained_bytes: 64 * 1024 * 1024,
                    filesystem_reserve_bytes: 1,
                },
                Instant::now(),
            )
            .is_err()
        );

        let foreign = directory.path().join("raw-disguised.repair.wal");
        std::fs::write(&foreign, b"not a repair wal").unwrap();
        assert!(
            RepairWal::open(
                RepairWalConfig {
                    path: foreign.clone(),
                    fsync: RepairWalFsyncPolicy::EveryRecord,
                    max_file_bytes: 1024 * 1024,
                    max_retained_bytes: 64 * 1024 * 1024,
                    filesystem_reserve_bytes: 1,
                },
                Instant::now(),
            )
            .is_err()
        );
        assert_eq!(std::fs::read(foreign).unwrap(), b"not a repair wal");
    }

    #[test]
    fn initializes_a_precreated_empty_legacy_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("empty.repair.wal");
        File::create(&path).unwrap();
        let wal = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: 1024 * 1024,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            Instant::now(),
        )
        .unwrap();
        assert_eq!(wal.next_sequence(), 0);
        assert_eq!(std::fs::read(path).unwrap(), LEGACY_FILE_HEADER);
    }

    #[test]
    fn truncated_tail_fails_closed_without_changing_the_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("accepted.repair.wal");
        let now = Instant::now();
        {
            let mut wal = RepairWal::open(
                RepairWalConfig {
                    path: path.clone(),
                    fsync: RepairWalFsyncPolicy::EveryRecord,
                    max_file_bytes: 1024 * 1024,
                    max_retained_bytes: 64 * 1024 * 1024,
                    filesystem_reserve_bytes: 1,
                },
                now,
            )
            .unwrap();
            wal.append(&provenance(), &[1, 2, 3], now).unwrap();
        }
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[10, 0, 0, 0, 1, 2])
            .unwrap();
        let bytes_before = std::fs::read(&path).unwrap();

        let error = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: 1024 * 1024,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(std::fs::read(path).unwrap(), bytes_before);
    }

    #[test]
    fn batch_policy_syncs_on_age_without_a_new_append() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("batched.repair.wal");
        let now = Instant::now();
        let mut wal = RepairWal::open(
            RepairWalConfig {
                path,
                fsync: RepairWalFsyncPolicy::Batch {
                    max_unsynced_records: NonZeroU64::new(10).unwrap(),
                    max_unsynced_age: Duration::from_millis(100),
                },
                max_file_bytes: 1024 * 1024,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap();
        assert!(!wal.append(&provenance(), &[1], now).unwrap().synced);
        assert!(!wal.sync_if_due(now + Duration::from_millis(99)).unwrap());
        assert!(wal.sync_if_due(now + Duration::from_millis(100)).unwrap());
        assert!(!wal.sync_if_due(now + Duration::from_millis(200)).unwrap());
    }

    #[test]
    fn full_legacy_file_rolls_without_overwriting_it() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("capped.repair.wal");
        let now = Instant::now();
        let mut wal = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        let payload = vec![7; MAX_FRAME_BODY_BYTES - overhead];
        let first = wal.append(&provenance(), &payload, now).unwrap();
        assert!(!first.rolled_segment);
        assert_eq!(wal.active_segment_id(), 0);
        let before = std::fs::read(&path).unwrap();

        let second = wal.append(&provenance(), &[1], now).unwrap();
        assert!(second.rolled_segment);
        assert_eq!(second.segment_id, 1);
        assert_eq!(wal.next_sequence(), 2);
        assert_eq!(wal.segment_count(), 2);
        assert_eq!(wal.active_segment_id(), 1);
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let rolled = segment_path(&path, 1).unwrap();
        assert!(rolled.exists());
        drop(wal);

        let entries = RepairWal::read_all(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[0].shred_payload, payload);
        assert_eq!(entries[1].sequence, 1);
        assert_eq!(entries[1].shred_payload, [1]);

        let reopened = RepairWal::open(
            RepairWalConfig {
                path,
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap();
        assert_eq!(reopened.next_sequence(), 2);
        assert_eq!(reopened.active_segment_id(), 1);
        assert_eq!(reopened.segment_count(), 2);
        assert_eq!(reopened.durable_through_sequence(), Some(1));
    }

    #[test]
    fn interrupted_staging_header_is_invisible_to_rotation_and_recovery() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("staged.repair.wal");
        let now = Instant::now();
        let mut wal = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();

        let interrupted = directory
            .path()
            .join(".staged.segment-00000000000000000001.repair.wal.creating-old");
        std::fs::write(&interrupted, &SEGMENT_FILE_MAGIC[..7]).unwrap();
        let append = wal.append(&provenance(), &[1], now).unwrap();
        assert!(append.rolled_segment);
        drop(wal);

        assert_eq!(
            std::fs::read(&interrupted).unwrap(),
            &SEGMENT_FILE_MAGIC[..7]
        );
        let entries = RepairWal::read_all(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].sequence, 1);
    }

    #[test]
    fn rotation_never_replaces_an_existing_destination() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("noclobber.repair.wal");
        let now = Instant::now();
        let mut wal = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();
        let legacy_before = std::fs::read(&path).unwrap();
        let destination = segment_path(&path, 1).unwrap();
        std::fs::write(&destination, b"pre-existing unconsumed bytes").unwrap();

        let error = wal.append(&provenance(), &[1], now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"pre-existing unconsumed bytes"
        );
        assert_eq!(std::fs::read(path).unwrap(), legacy_before);
        assert_eq!(wal.next_sequence(), 1);
        assert_eq!(wal.active_segment_id(), 0);
    }

    #[test]
    fn durable_boundary_excludes_later_complete_frames() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("bounded.repair.wal");
        let now = Instant::now();
        let mut wal = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: 1024 * 1024,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap();
        wal.append(&provenance(), &[1], now).unwrap();
        wal.append(&provenance(), &[2], now).unwrap();
        assert_eq!(wal.durable_through_sequence(), Some(1));

        let entries = RepairWal::read_through(&path, Some(0)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].shred_payload, [1]);
    }

    #[test]
    fn missing_or_corrupt_segment_chain_fails_closed() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("chained.repair.wal");
        let now = Instant::now();
        let mut wal = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();
        wal.append(&provenance(), &[1], now).unwrap();
        drop(wal);

        let second = segment_path(&path, 1).unwrap();
        let mut bytes = std::fs::read(&second).unwrap();
        bytes[40] ^= 0x80;
        std::fs::write(&second, &bytes).unwrap();
        let error = RepairWal::open(
            RepairWalConfig {
                path,
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("header checksum"));
    }

    #[test]
    fn truncated_active_segment_fails_closed_without_shrinking() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("tail.repair.wal");
        let now = Instant::now();
        let config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };
        let mut wal = RepairWal::open(config.clone(), now).unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();
        wal.append(&provenance(), &[1], now).unwrap();
        let active = segment_path(&path, 1).unwrap();
        drop(wal);

        OpenOptions::new()
            .append(true)
            .open(&active)
            .unwrap()
            .write_all(&[10, 0, 0])
            .unwrap();
        let bytes_before = std::fs::read(&active).unwrap();
        let error = RepairWal::open(config, now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("truncated repair WAL frame prefix")
        );
        assert_eq!(std::fs::read(active).unwrap(), bytes_before);
    }

    #[test]
    fn corrupt_length_prefix_cannot_be_mistaken_for_a_truncatable_tail() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("length-corrupt.repair.wal");
        let now = Instant::now();
        let config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: 1024 * 1024,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };
        let mut wal = RepairWal::open(config.clone(), now).unwrap();
        wal.append(&provenance(), &[1, 2, 3], now).unwrap();
        drop(wal);

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[LEGACY_FILE_HEADER.len()..LEGACY_FILE_HEADER.len() + FRAME_PREFIX_BYTES]
            .copy_from_slice(&(MAX_FRAME_BODY_BYTES as u32).to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let bytes_before = std::fs::read(&path).unwrap();

        let error = RepairWal::open(config, now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("truncated repair WAL frame body")
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes_before);

        let inspection = RepairWal::inspect(&path).unwrap();
        assert_eq!(inspection.retained_bytes, bytes_before.len() as u64);
        assert!(inspection.validation_error.is_some());
    }

    #[test]
    fn hard_retained_ceiling_stops_before_seal_or_rollover() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("hard-cap.repair.wal");
        let now = Instant::now();
        let config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
            max_retained_bytes: MIN_REPAIR_WAL_FILE_BYTES,
            filesystem_reserve_bytes: 1,
        };
        let mut wal = RepairWal::open(config.clone(), now).unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();
        let bytes_before = std::fs::read(&path).unwrap();

        let error = wal.append(&provenance(), &[1], now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::StorageFull);
        assert!(error.to_string().contains("hard retained-byte ceiling"));
        assert_eq!(std::fs::read(&path).unwrap(), bytes_before);
        assert!(!v3_seal_path(&path).exists());
        assert!(!segment_path(&path, 1).unwrap().exists());
        drop(wal);

        let reopened = RepairWal::open(config, now).unwrap();
        assert_eq!(reopened.next_sequence(), 1);
        assert_eq!(std::fs::read(path).unwrap(), bytes_before);
    }

    #[test]
    fn oversized_existing_generation_fails_open_but_inspection_reports_bytes() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("oversized.repair.wal");
        let now = Instant::now();
        let mut config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };
        let mut wal = RepairWal::open(config.clone(), now).unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();
        wal.append(&provenance(), &[1], now).unwrap();
        drop(wal);

        let inspection = RepairWal::inspect(&path).unwrap();
        assert!(inspection.retained_bytes > MIN_REPAIR_WAL_FILE_BYTES);
        assert!(inspection.validation_error.is_none());
        config.max_retained_bytes = MIN_REPAIR_WAL_FILE_BYTES;
        let error = RepairWal::open(config, now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::StorageFull);
        assert_eq!(
            RepairWal::inspect(&path).unwrap().retained_bytes,
            inspection.retained_bytes
        );
    }

    #[test]
    fn filesystem_reserve_stops_before_writing_a_frame() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("reserve.repair.wal");
        let now = Instant::now();
        let mut wal = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: 1024 * 1024,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .unwrap();
        wal.filesystem_reserve_bytes = u64::MAX;
        let bytes_before = std::fs::read(&path).unwrap();

        let error = wal.append(&provenance(), &[1], now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::StorageFull);
        assert!(error.to_string().contains("filesystem reserve"));
        assert_eq!(std::fs::read(path).unwrap(), bytes_before);
        assert_eq!(wal.next_sequence(), 0);
    }

    #[test]
    fn transitional_v3_head_is_finished_without_reusing_the_legacy_base() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sealed.repair.wal");
        let now = Instant::now();
        let config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: 1024 * 1024,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };
        let mut wal = RepairWal::open(config.clone(), now).unwrap();
        wal.append(&provenance(), &[1], now).unwrap();
        drop(wal);

        let segments = discover_segment_paths(&path).unwrap();
        let validated = validate_segment_chain(&segments).unwrap();
        publish_v3_head(&path, &v3_head_from_recovered(&validated.active)).unwrap();
        assert!(v3_head_path(&path).exists());
        assert!(!v3_seal_path(&path).exists());

        let mut reopened = RepairWal::open(config, now).unwrap();
        assert!(reopened.v3_sealed());
        assert_eq!(reopened.active_segment_id(), 0);
        assert!(std::fs::metadata(&path).unwrap().permissions().readonly());
        assert!(v3_seal_path(&path).exists());
        let append = reopened.append(&provenance(), &[2], now).unwrap();
        assert!(append.rolled_segment);
        assert_eq!(append.segment_id, 1);
        assert_eq!(RepairWal::read_all(&path).unwrap().len(), 2);
    }

    #[test]
    fn v3_seal_without_head_fails_closed() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("head-missing.repair.wal");
        let now = Instant::now();
        let config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };
        let mut wal = RepairWal::open(config.clone(), now).unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();
        wal.append(&provenance(), &[1], now).unwrap();
        drop(wal);

        std::fs::remove_file(v3_head_path(&path)).unwrap();
        let error = RepairWal::open(config, now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("without its required durable head")
        );
        assert!(
            RepairWal::inspect(&path)
                .unwrap()
                .validation_error
                .is_some()
        );
        assert!(RepairWal::read_all(&path).is_err());
    }

    #[test]
    fn deleting_the_highest_v3_segment_cannot_roll_back_or_reuse_its_sequence() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("deleted-highest.repair.wal");
        let now = Instant::now();
        let config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };
        let mut wal = RepairWal::open(config.clone(), now).unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();
        wal.append(&provenance(), &[1], now).unwrap();
        drop(wal);

        let highest = segment_path(&path, 1).unwrap();
        std::fs::remove_file(&highest).unwrap();
        let error = RepairWal::open(config, now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("behind its durable head"));
        assert!(!highest.exists());
        assert!(
            RepairWal::inspect(&path)
                .unwrap()
                .validation_error
                .is_some()
        );
        assert!(RepairWal::read_all(&path).is_err());
    }

    #[test]
    fn clean_frame_boundary_truncation_behind_v3_head_fails_closed() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("clean-truncation.repair.wal");
        let now = Instant::now();
        let config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };
        let mut wal = RepairWal::open(config.clone(), now).unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();
        wal.append(&provenance(), &[1], now).unwrap();
        let first_frame_boundary = wal.file_len();
        wal.append(&provenance(), &[2], now).unwrap();
        let active = segment_path(&path, 1).unwrap();
        drop(wal);

        OpenOptions::new()
            .write(true)
            .open(&active)
            .unwrap()
            .set_len(first_frame_boundary)
            .unwrap();
        let error = RepairWal::open(config, now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("truncated behind durable head"));
        assert_eq!(
            std::fs::metadata(&active).unwrap().len(),
            first_frame_boundary
        );
        assert!(
            RepairWal::inspect(&path)
                .unwrap()
                .validation_error
                .is_some()
        );
        assert!(RepairWal::read_all(&path).is_err());
    }

    #[test]
    fn writer_proves_and_checkpoints_a_synced_wal_ahead_crash_tail() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("wal-ahead.repair.wal");
        let now = Instant::now();
        let config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };
        let mut wal = RepairWal::open(config.clone(), now).unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        wal.append(
            &provenance(),
            &vec![7; MAX_FRAME_BODY_BYTES - overhead],
            now,
        )
        .unwrap();
        wal.append(&provenance(), &[1], now).unwrap();
        drop(wal);

        let head_before = read_v3_head(&path).unwrap().unwrap();
        let body = encode_body(2, &provenance(), &[2]).unwrap();
        let frame = encode_frame(body.len() as u32, &body);
        let active = segment_path(&path, 1).unwrap();
        let mut active_file = OpenOptions::new().append(true).open(&active).unwrap();
        active_file.write_all(&frame).unwrap();
        active_file.sync_data().unwrap();
        drop(active_file);

        // Read-only consumers never advertise bytes beyond the durable checkpoint. Only the
        // exclusive writer may prove this exact chain prefix and advance the checkpoint.
        assert_eq!(read_v3_head(&path).unwrap().unwrap(), head_before);
        assert!(
            RepairWal::inspect(&path)
                .unwrap()
                .validation_error
                .is_some()
        );
        assert!(RepairWal::read_all(&path).is_err());

        let reopened = RepairWal::open(config, now).unwrap();
        assert_eq!(reopened.next_sequence(), 3);
        assert_eq!(reopened.durable_through_sequence(), Some(2));
        let terminal = current_v3_head(&reopened);
        assert_eq!(read_v3_head(&path).unwrap().unwrap(), terminal);
        drop(reopened);
        assert_eq!(RepairWal::read_all(&path).unwrap().len(), 3);
    }

    #[test]
    fn orphan_v3_seal_never_creates_a_fresh_sequence_zero_base() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("orphan.repair.wal");
        create_v3_seal_from_values(&path, 1, &Hash::new_from_array([9; 32])).unwrap();
        let config = RepairWalConfig {
            path: path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: 1024 * 1024,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };

        let error = RepairWal::open(config, Instant::now()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("every segment is missing"));
        assert!(!path.exists());
        let inspection = RepairWal::inspect(&path).unwrap();
        assert!(inspection.v3_sealed);
        assert!(inspection.validation_error.is_some());
    }

    #[test]
    fn bare_relative_path_rolls_and_reopens() {
        const CHILD_ENV: &str = "SHRED_REPAIR_WAL_RELATIVE_PATH_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            let path = PathBuf::from("accepted.repair.wal");
            let now = Instant::now();
            let config = RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            };
            let mut wal = RepairWal::open(config.clone(), now).unwrap();
            let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
            wal.append(
                &provenance(),
                &vec![7; MAX_FRAME_BODY_BYTES - overhead],
                now,
            )
            .unwrap();
            wal.append(&provenance(), &[1], now).unwrap();
            drop(wal);
            let reopened = RepairWal::open(config, now).unwrap();
            assert_eq!(reopened.active_segment_id(), 1);
            assert_eq!(reopened.next_sequence(), 2);
            return;
        }

        let directory = tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("repair_wal::tests::bare_relative_path_rolls_and_reopens")
            .env(CHILD_ENV, "1")
            .current_dir(directory.path())
            .status()
            .unwrap();
        assert!(status.success());
        assert!(directory.path().join("accepted.repair.wal").exists());
        assert!(
            directory
                .path()
                .join("accepted.segment-00000000000000000001.repair.wal")
                .exists()
        );
    }

    #[test]
    fn writer_lock_prevents_a_second_segmented_writer() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("locked.repair.wal");
        let config = RepairWalConfig {
            path,
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: 1024 * 1024,
            max_retained_bytes: 64 * 1024 * 1024,
            filesystem_reserve_bytes: 1,
        };
        let _first = RepairWal::open(config.clone(), Instant::now()).unwrap();
        let error = RepairWal::open(config, Instant::now()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::WouldBlock);
    }

    #[test]
    fn cap_must_hold_header_and_one_maximum_frame() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("too-small.repair.wal");
        let error = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES - 1,
                max_retained_bytes: 64 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            Instant::now(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(
            !path.exists(),
            "invalid cap must fail before creating a file"
        );
    }
}
