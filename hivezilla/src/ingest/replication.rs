//! Primary/replica protocol state and deletion-safety rules.
//!
//! Transport and cryptographic verification are intentionally separate from the retention state
//! machine. A network response can never directly make local data eligible for deletion: the
//! receipt must first be authenticated and then committed to the replica's receipt journal.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};

use anyhow::{Context, Result as AnyResult, ensure};
use serde::{Deserialize, Serialize};

use super::{
    dedup::{ContentDigest, LogicalKey, ObservationId},
    spool::{DurableSpoolRecord, SpoolLocation},
};

pub const REPLICATION_PROTOCOL_VERSION: u16 = 1;
const RECEIPT_SIGNING_DOMAIN: &[u8] = b"BLOCKZILLA-INGEST-RECEIPT-v1";
const ED25519_SIGNATURE_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationOffer {
    pub protocol_version: u16,
    pub cluster_id: String,
    pub record: ObservationId,
    pub source_id: String,
    pub logical_key: LogicalKey,
    pub content_digest: ContentDigest,
    pub payload_len: u64,
    pub payload_format_version: u16,
    pub commitment: CommitmentEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitmentEvidence {
    Unknown,
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptDisposition {
    /// The complete raw event and observation metadata are durable in the primary WAL.
    DurablyStored,
    /// An exact event-to-archive mapping was already committed durably.
    AlreadyCommitted,
    /// A conflicting candidate was retained durably in the primary quarantine/repair lane.
    DurablyStoredConflict,
    /// Validation or policy rejected the record. This never permits replica deletion.
    Rejected,
}

impl ReceiptDisposition {
    pub fn authorizes_replica_gc(self) -> bool {
        matches!(
            self,
            Self::DurablyStored | Self::AlreadyCommitted | Self::DurablyStoredConflict
        )
    }
}

/// Receipt returned by the primary after its durability boundary.
///
/// The signature must cover every field using a domain-separated, deterministic encoding. mTLS
/// authenticates the connection; this signature is the durable proof retained for later GC and
/// key rotation audits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryReceipt {
    pub protocol_version: u16,
    pub cluster_id: String,
    pub primary_id: String,
    pub primary_term: u64,
    pub record: ObservationId,
    pub content_digest: ContentDigest,
    pub disposition: ReceiptDisposition,
    pub durable_lsn: u64,
    pub archive_commit_digest: Option<ContentDigest>,
    pub signing_key_id: String,
    pub signature: Vec<u8>,
}

impl PrimaryReceipt {
    /// Canonical domain-separated bytes covered by the receipt signature.
    ///
    /// This deliberately avoids serializing JSON/protobuf, whose encoding may change while the
    /// signed protocol remains stable.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, ReceiptValidationError> {
        validate_signable_receipt_fields(self)?;
        let mut output = Vec::with_capacity(256);
        output.extend_from_slice(RECEIPT_SIGNING_DOMAIN);
        push_bounded_string(&mut output, &self.cluster_id)?;
        push_bounded_string(&mut output, &self.primary_id)?;
        output.extend_from_slice(&self.primary_term.to_le_bytes());
        push_bounded_string(&mut output, &self.record.origin_node_id)?;
        output.extend_from_slice(&self.record.journal_id);
        output.extend_from_slice(&self.record.sequence.to_le_bytes());
        output.extend_from_slice(&self.content_digest.0);
        output.push(match self.disposition {
            ReceiptDisposition::DurablyStored => 1,
            ReceiptDisposition::AlreadyCommitted => 2,
            ReceiptDisposition::DurablyStoredConflict => 3,
            ReceiptDisposition::Rejected => 4,
        });
        output.extend_from_slice(&self.durable_lsn.to_le_bytes());
        match self.archive_commit_digest {
            Some(digest) => {
                output.push(1);
                output.extend_from_slice(&digest.0);
            }
            None => output.push(0),
        }
        push_bounded_string(&mut output, &self.signing_key_id)?;
        Ok(output)
    }
}

fn push_bounded_string(output: &mut Vec<u8>, value: &str) -> Result<(), ReceiptValidationError> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(ReceiptValidationError::InvalidField);
    }
    let len = u16::try_from(value.len()).map_err(|_| ReceiptValidationError::InvalidField)?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn validate_signable_receipt_fields(
    receipt: &PrimaryReceipt,
) -> Result<(), ReceiptValidationError> {
    if receipt.primary_term == 0
        || (receipt.disposition == ReceiptDisposition::AlreadyCommitted
            && receipt.archive_commit_digest.is_none())
    {
        return Err(ReceiptValidationError::InvalidField);
    }
    // Exercise the same string bounds without retaining an allocation.
    for value in [
        receipt.cluster_id.as_str(),
        receipt.primary_id.as_str(),
        receipt.record.origin_node_id.as_str(),
        receipt.signing_key_id.as_str(),
    ] {
        if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
            return Err(ReceiptValidationError::InvalidField);
        }
    }
    Ok(())
}

/// Expected identity binds a receipt to the exact local spool record being acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedReceipt<'a> {
    pub cluster_id: &'a str,
    pub primary_id: &'a str,
    pub record: &'a ObservationId,
    pub content_digest: ContentDigest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptValidationError {
    UnsupportedProtocol,
    WrongCluster,
    WrongPrimary,
    WrongRecord,
    WrongContent,
    EmptySigningKeyId,
    EmptySignature,
    InvalidSignatureLength,
    InvalidField,
    InvalidSignature,
}

/// Pluggable verifier backed by configured trusted public keys.
#[allow(dead_code)] // Wired when the concrete Ed25519 keyring/transport lands.
pub(crate) trait ReceiptSignatureVerifier {
    fn verify_signature(&self, key_id: &str, signing_bytes: &[u8], signature: &[u8]) -> bool;
}

/// An authenticated receipt bound to one expected local record.
///
/// It still does not authorize deletion until it is written and synced to the local receipt WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedReceipt(PrimaryReceipt);

impl VerifiedReceipt {
    pub fn receipt(&self) -> &PrimaryReceipt {
        &self.0
    }
}

#[allow(dead_code)] // Kept crate-private so external callers cannot supply an accept-all verifier.
pub(crate) fn verify_receipt<V: ReceiptSignatureVerifier>(
    receipt: PrimaryReceipt,
    expected: ExpectedReceipt<'_>,
    verifier: &V,
) -> std::result::Result<VerifiedReceipt, ReceiptValidationError> {
    if receipt.protocol_version != REPLICATION_PROTOCOL_VERSION {
        return Err(ReceiptValidationError::UnsupportedProtocol);
    }
    if receipt.cluster_id != expected.cluster_id {
        return Err(ReceiptValidationError::WrongCluster);
    }
    if receipt.primary_id != expected.primary_id {
        return Err(ReceiptValidationError::WrongPrimary);
    }
    if receipt.record != *expected.record {
        return Err(ReceiptValidationError::WrongRecord);
    }
    if receipt.content_digest != expected.content_digest {
        return Err(ReceiptValidationError::WrongContent);
    }
    if receipt.signing_key_id.trim().is_empty() {
        return Err(ReceiptValidationError::EmptySigningKeyId);
    }
    if receipt.signature.is_empty() {
        return Err(ReceiptValidationError::EmptySignature);
    }
    if receipt.signature.len() != ED25519_SIGNATURE_BYTES {
        return Err(ReceiptValidationError::InvalidSignatureLength);
    }
    let signing_bytes = receipt.signing_bytes()?;
    if !verifier.verify_signature(&receipt.signing_key_id, &signing_bytes, &receipt.signature) {
        return Err(ReceiptValidationError::InvalidSignature);
    }
    Ok(VerifiedReceipt(receipt))
}

/// Receipt after the replica has appended it to its local acknowledgment WAL and fsynced it.
///
/// The constructor is intentionally private. A future receipt-WAL implementation in this module
/// will be the production path that creates the token after a successful sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableLocalReceipt {
    receipt: VerifiedReceipt,
    local_location: SpoolLocation,
}

impl DurableLocalReceipt {
    pub fn receipt(&self) -> &PrimaryReceipt {
        self.receipt.receipt()
    }

    pub fn local_location(&self) -> SpoolLocation {
        self.local_location
    }

    /// Whether the exact local record may be garbage-collected.
    pub fn authorizes_gc(&self) -> bool {
        self.receipt().disposition.authorizes_replica_gc()
    }
}

const RECEIPT_WAL_MAGIC: &[u8; 8] = b"BZRACK01";
const RECEIPT_FRAME_MAGIC: &[u8; 4] = b"BZRA";
const RECEIPT_COMMIT_MAGIC: &[u8; 4] = b"CMIT";
const RECEIPT_FRAME_VERSION: u16 = 1;
const RECEIPT_WAL_HEADER_LEN: u64 = RECEIPT_WAL_MAGIC.len() as u64;
const RECEIPT_FRAME_FIXED_LEN: u64 = 4 + 2 + 4 + 4;
const RECEIPT_FRAME_TRAILER_LEN: u64 = 4 + 4;
const MAX_RECEIPT_FRAME_BYTES: usize = 1024 * 1024;

/// Append-only local acknowledgment journal.
///
/// `commit_verified` returns a GC-capable token only after the frame is flushed and `sync_data`
/// succeeds. Opening the WAL truncates an incomplete trailing frame left by a crash. Receipt
/// signatures must be verified again when rebuilding GC state from this journal after restart.
#[derive(Debug)]
pub struct ReceiptWal {
    path: PathBuf,
    writer: BufWriter<File>,
    poisoned: bool,
}

impl ReceiptWal {
    pub fn open(path: impl AsRef<Path>) -> AnyResult<Self> {
        let path = path.as_ref().to_path_buf();
        let parent = durable_parent(&path);
        create_dir_all_durable(&parent)?;
        let (mut file, _created) = open_receipt_file(&path, true)?;
        try_lock_exclusive(&file, &path)?;
        initialize_receipt_wal(&mut file, &path)?;
        // Always persist the directory entry after taking ownership. Another process may have
        // created the file and lost the lock before it could sync the parent directory.
        sync_directory(&parent)?;
        let file_len = file.metadata()?.len();
        let valid_len = recover_receipt_wal(&mut file, &path)?;
        if valid_len != file_len {
            file.set_len(valid_len)
                .with_context(|| format!("truncate receipt WAL {}", path.display()))?;
            file.sync_data()
                .with_context(|| format!("sync recovered receipt WAL {}", path.display()))?;
        }
        file.seek(std::io::SeekFrom::End(0))
            .with_context(|| format!("seek receipt WAL {}", path.display()))?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
            poisoned: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Persist an already-authenticated receipt and establish the replica deletion boundary.
    pub fn commit_verified(
        &mut self,
        receipt: VerifiedReceipt,
        local_record: &DurableSpoolRecord,
    ) -> AnyResult<DurableLocalReceipt> {
        ensure!(
            !self.poisoned,
            "receipt WAL writer is poisoned; reopen it to recover before appending"
        );
        ensure!(
            receipt.receipt().record == local_record.metadata().observation,
            "primary receipt observation does not match durable local spool record"
        );
        ensure!(
            receipt.receipt().content_digest == local_record.metadata().content_digest,
            "primary receipt content digest does not match durable local spool record"
        );
        let local_location = local_record.location();
        let stored = StoredReceiptFrame {
            receipt: receipt.receipt().clone(),
            local_location,
        };
        let payload = serde_json::to_vec(&stored).context("encode primary receipt")?;
        ensure!(
            payload.len() <= MAX_RECEIPT_FRAME_BYTES,
            "receipt frame exceeds {} bytes",
            MAX_RECEIPT_FRAME_BYTES
        );
        let len = u32::try_from(payload.len()).context("receipt frame length exceeds u32")?;
        let version_bytes = RECEIPT_FRAME_VERSION.to_le_bytes();
        let len_bytes = len.to_le_bytes();
        let mut header_crc = Crc32c::new();
        header_crc.update(RECEIPT_FRAME_MAGIC);
        header_crc.update(&version_bytes);
        header_crc.update(&len_bytes);
        let mut payload_crc = Crc32c::new();
        payload_crc.update(&payload);

        self.poisoned = true;
        self.writer
            .write_all(RECEIPT_FRAME_MAGIC)
            .with_context(|| format!("write receipt frame magic {}", self.path.display()))?;
        self.writer
            .write_all(&version_bytes)
            .with_context(|| format!("write receipt frame version {}", self.path.display()))?;
        self.writer
            .write_all(&len_bytes)
            .with_context(|| format!("write receipt frame length {}", self.path.display()))?;
        self.writer
            .write_all(&header_crc.finish().to_le_bytes())
            .with_context(|| format!("write receipt header checksum {}", self.path.display()))?;
        self.writer
            .write_all(&payload)
            .with_context(|| format!("write receipt WAL payload {}", self.path.display()))?;
        self.writer
            .write_all(&payload_crc.finish().to_le_bytes())
            .with_context(|| format!("write receipt payload checksum {}", self.path.display()))?;
        self.writer
            .write_all(RECEIPT_COMMIT_MAGIC)
            .with_context(|| format!("write receipt commit marker {}", self.path.display()))?;
        self.writer
            .flush()
            .with_context(|| format!("flush receipt WAL {}", self.path.display()))?;
        self.writer
            .get_ref()
            .sync_data()
            .with_context(|| format!("sync receipt WAL {}", self.path.display()))?;
        self.poisoned = false;
        Ok(DurableLocalReceipt {
            receipt,
            local_location,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredReceiptFrame {
    receipt: PrimaryReceipt,
    local_location: SpoolLocation,
}

fn recover_receipt_wal(file: &mut File, path: &Path) -> AnyResult<u64> {
    file.seek(std::io::SeekFrom::Start(0))
        .with_context(|| format!("seek receipt WAL {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut wal_magic = [0u8; 8];
    reader
        .read_exact(&mut wal_magic)
        .with_context(|| format!("read receipt WAL header {}", path.display()))?;
    ensure!(
        &wal_magic == RECEIPT_WAL_MAGIC,
        "invalid receipt WAL header in {}",
        path.display()
    );
    let mut valid_len = RECEIPT_WAL_HEADER_LEN;
    loop {
        let frame_offset = valid_len;
        let mut frame_magic = [0u8; 4];
        if !read_receipt_exact_or_tail(&mut reader, &mut frame_magic, path)? {
            break;
        }
        ensure!(
            &frame_magic == RECEIPT_FRAME_MAGIC,
            "corrupt receipt frame magic at {} in {}",
            frame_offset,
            path.display()
        );

        let mut version_bytes = [0u8; 2];
        if !read_receipt_exact_or_tail(&mut reader, &mut version_bytes, path)? {
            break;
        }
        let version = u16::from_le_bytes(version_bytes);

        let mut length_bytes = [0u8; 4];
        if !read_receipt_exact_or_tail(&mut reader, &mut length_bytes, path)? {
            break;
        }

        let mut expected_header_crc_bytes = [0u8; 4];
        if !read_receipt_exact_or_tail(&mut reader, &mut expected_header_crc_bytes, path)? {
            break;
        }
        let mut header_crc = Crc32c::new();
        header_crc.update(&frame_magic);
        header_crc.update(&version_bytes);
        header_crc.update(&length_bytes);
        ensure!(
            header_crc.finish() == u32::from_le_bytes(expected_header_crc_bytes),
            "receipt frame header checksum mismatch at {} in {}",
            frame_offset,
            path.display()
        );
        ensure!(
            version == RECEIPT_FRAME_VERSION,
            "unsupported receipt frame version {version} at {} in {}",
            frame_offset,
            path.display()
        );

        let payload_len = u32::from_le_bytes(length_bytes) as usize;
        ensure!(
            payload_len <= MAX_RECEIPT_FRAME_BYTES,
            "receipt frame length {} exceeds maximum at {} in {}",
            payload_len,
            frame_offset,
            path.display()
        );
        let mut payload = vec![0u8; payload_len];
        if !read_receipt_exact_or_tail(&mut reader, &mut payload, path)? {
            break;
        }

        let mut expected_payload_crc_bytes = [0u8; 4];
        if !read_receipt_exact_or_tail(&mut reader, &mut expected_payload_crc_bytes, path)? {
            break;
        }

        let mut commit_magic = [0u8; 4];
        if !read_receipt_exact_or_tail(&mut reader, &mut commit_magic, path)? {
            break;
        }
        let mut payload_crc = Crc32c::new();
        payload_crc.update(&payload);
        ensure!(
            payload_crc.finish() == u32::from_le_bytes(expected_payload_crc_bytes),
            "receipt frame payload checksum mismatch at {} in {}",
            frame_offset,
            path.display()
        );
        ensure!(
            &commit_magic == RECEIPT_COMMIT_MAGIC,
            "missing receipt frame commit marker at {} in {}",
            frame_offset,
            path.display()
        );
        serde_json::from_slice::<StoredReceiptFrame>(&payload).with_context(|| {
            format!(
                "decode committed receipt frame at {} in {}",
                frame_offset,
                path.display()
            )
        })?;
        valid_len = valid_len
            .checked_add(RECEIPT_FRAME_FIXED_LEN)
            .and_then(|len| len.checked_add(payload_len as u64))
            .and_then(|len| len.checked_add(RECEIPT_FRAME_TRAILER_LEN))
            .context("receipt WAL length overflow")?;
    }
    Ok(valid_len)
}

fn initialize_receipt_wal(file: &mut File, path: &Path) -> AnyResult<()> {
    let len = file.metadata()?.len();
    if len >= RECEIPT_WAL_HEADER_LEN {
        return Ok(());
    }
    let mut existing = vec![0u8; len as usize];
    file.seek(std::io::SeekFrom::Start(0))?;
    file.read_exact(&mut existing)?;
    ensure!(
        RECEIPT_WAL_MAGIC.starts_with(&existing),
        "refusing non-receipt file with invalid partial header: {}",
        path.display()
    );
    file.set_len(0)?;
    file.seek(std::io::SeekFrom::Start(0))?;
    file.write_all(RECEIPT_WAL_MAGIC)
        .with_context(|| format!("write receipt WAL header {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("sync receipt WAL header {}", path.display()))
}

fn read_receipt_exact_or_tail<R: Read>(
    reader: &mut R,
    output: &mut [u8],
    path: &Path,
) -> AnyResult<bool> {
    match reader.read_exact(output) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(err) => Err(err).with_context(|| format!("read receipt WAL {}", path.display())),
    }
}

fn durable_parent(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

fn open_receipt_file(path: &Path, create_if_missing: bool) -> AnyResult<(File, bool)> {
    let (file, created) = if create_if_missing {
        match open_receipt_descriptor(path, true) {
            Ok(file) => (file, true),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => (
                open_receipt_descriptor(path, false)
                    .with_context(|| format!("open existing receipt WAL {}", path.display()))?,
                false,
            ),
            Err(err) => {
                return Err(err).with_context(|| format!("create receipt WAL {}", path.display()));
            }
        }
    } else {
        (
            open_receipt_descriptor(path, false)
                .with_context(|| format!("open receipt WAL {}", path.display()))?,
            false,
        )
    };
    ensure!(
        file.metadata()?.file_type().is_file(),
        "receipt WAL path is not a regular file: {}",
        path.display()
    );
    Ok((file, created))
}

fn open_receipt_descriptor(path: &Path, create_new: bool) -> io::Result<File> {
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
fn try_lock_exclusive(file: &File, path: &Path) -> AnyResult<()> {
    // SAFETY: the descriptor is valid and remains owned by the receipt WAL for the lock lifetime.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
            .with_context(|| format!("lock receipt WAL {}", path.display()))
    }
}

#[cfg(not(unix))]
fn try_lock_exclusive(file: &File, path: &Path) -> AnyResult<()> {
    file.try_lock()
        .with_context(|| format!("lock receipt WAL {}", path.display()))
}

fn sync_directory(path: &Path) -> AnyResult<()> {
    let directory = File::open(path)
        .with_context(|| format!("open receipt WAL directory for sync {}", path.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("sync receipt WAL directory {}", path.display()))
}

fn create_dir_all_durable(path: &Path) -> AnyResult<()> {
    let mut missing = Vec::new();
    let mut cursor = path;
    while !cursor.exists() {
        missing.push(cursor.to_path_buf());
        cursor = cursor.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "receipt WAL directory has no existing ancestor: {}",
                path.display()
            )
        })?;
    }
    fs::create_dir_all(path)
        .with_context(|| format!("create receipt WAL directory {}", path.display()))?;
    for created in missing.iter().rev() {
        sync_directory(&durable_parent(created))?;
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

/// State of one replica record. Disconnects and process restarts return non-durable network states
/// to `LocalSpoolDurable`. `LocalReceiptDurable` makes only this record eligible for a later
/// sealed-segment audit; it does not directly authorize unlinking a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaRecordState {
    LocalSpoolDurable,
    Offered,
    Uploading,
    VerifiedPrimaryReceipt,
    LocalReceiptDurable,
    Deleted,
}

impl ReplicaRecordState {
    pub fn recovers_as(self) -> Self {
        match self {
            Self::Offered | Self::Uploading | Self::VerifiedPrimaryReceipt => {
                Self::LocalSpoolDurable
            }
            durable => durable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordGcEligibility {
    NotDurable,
    ReceiptMismatch,
    Rejected,
    /// The record is acknowledged. The containing segment may be unlinked only after every frame
    /// in the sealed segment has this status and the segment tombstone is itself durable.
    EligibleAfterSegmentAudit,
}

pub fn replica_record_gc_eligibility(
    state: ReplicaRecordState,
    target: &DurableSpoolRecord,
    durable_receipt: Option<&DurableLocalReceipt>,
) -> RecordGcEligibility {
    if state != ReplicaRecordState::LocalReceiptDurable {
        return RecordGcEligibility::NotDurable;
    }
    let Some(receipt) = durable_receipt else {
        return RecordGcEligibility::NotDurable;
    };
    if receipt.local_location() != target.location()
        || receipt.receipt().record != target.metadata().observation
        || receipt.receipt().content_digest != target.metadata().content_digest
    {
        return RecordGcEligibility::ReceiptMismatch;
    }
    if receipt.authorizes_gc() {
        RecordGcEligibility::EligibleAfterSegmentAudit
    } else {
        RecordGcEligibility::Rejected
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{
        IngressRecordMeta, LogicalKey, SpoolJournalIdentity, SpoolOptions, SpoolWriter,
        compute_content_digest,
    };

    struct AcceptTestSignature;

    impl ReceiptSignatureVerifier for AcceptTestSignature {
        fn verify_signature(
            &self,
            _key_id: &str,
            _signing_bytes: &[u8],
            _signature: &[u8],
        ) -> bool {
            true
        }
    }

    struct ExactTestSignature {
        signing_bytes: Vec<u8>,
        signature: Vec<u8>,
    }

    impl ReceiptSignatureVerifier for ExactTestSignature {
        fn verify_signature(&self, _key_id: &str, signing_bytes: &[u8], signature: &[u8]) -> bool {
            signing_bytes == self.signing_bytes && signature == self.signature
        }
    }

    fn observation() -> ObservationId {
        ObservationId {
            origin_node_id: "backup-a".to_string(),
            journal_id: [7; 16],
            sequence: 91,
        }
    }

    fn receipt(disposition: ReceiptDisposition) -> PrimaryReceipt {
        let logical_key = LogicalKey::Block {
            slot: 42,
            blockhash: [8; 32],
        };
        PrimaryReceipt {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            cluster_id: "solana-mainnet".to_string(),
            primary_id: "nas-primary".to_string(),
            primary_term: 1,
            record: observation(),
            content_digest: compute_content_digest(
                "solana-mainnet",
                &logical_key,
                1,
                b"durable-event",
            ),
            disposition,
            durable_lsn: 123,
            archive_commit_digest: (disposition == ReceiptDisposition::AlreadyCommitted)
                .then_some(ContentDigest([6; 32])),
            signing_key_id: "2026-q3".to_string(),
            signature: vec![1; ED25519_SIGNATURE_BYTES],
        }
    }

    #[test]
    fn unsigned_receipt_can_be_canonically_signed_and_verified() {
        let mut signed_receipt = receipt(ReceiptDisposition::DurablyStored);
        signed_receipt.signature.clear();
        let signing_bytes = signed_receipt.signing_bytes().unwrap();
        let signature = vec![0xa5; ED25519_SIGNATURE_BYTES];
        signed_receipt.signature.clone_from(&signature);
        let expected_record = signed_receipt.record.clone();
        let expected_digest = signed_receipt.content_digest;

        let verified = verify_receipt(
            signed_receipt,
            ExpectedReceipt {
                cluster_id: "solana-mainnet",
                primary_id: "nas-primary",
                record: &expected_record,
                content_digest: expected_digest,
            },
            &ExactTestSignature {
                signing_bytes,
                signature,
            },
        )
        .unwrap();

        assert_eq!(verified.receipt().record, expected_record);
    }

    fn verified(disposition: ReceiptDisposition) -> VerifiedReceipt {
        let record = observation();
        let content_digest = receipt(disposition).content_digest;
        verify_receipt(
            receipt(disposition),
            ExpectedReceipt {
                cluster_id: "solana-mainnet",
                primary_id: "nas-primary",
                record: &record,
                content_digest,
            },
            &AcceptTestSignature,
        )
        .unwrap()
    }

    fn receipt_wal_path(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-ingest-{label}-{}-{unique}.acks.wal",
            std::process::id()
        ))
    }

    fn durable_spool_record(label: &str) -> (PathBuf, DurableSpoolRecord) {
        let root = receipt_wal_path(label).with_extension("spool");
        let payload = b"durable-event";
        let metadata = IngressRecordMeta::from_payload(
            "solana-mainnet".to_string(),
            observation(),
            "replica-primary".to_string(),
            LogicalKey::Block {
                slot: 42,
                blockhash: [8; 32],
            },
            1,
            payload,
        );
        let mut spool = SpoolWriter::open(
            &root,
            SpoolJournalIdentity {
                cluster_id: "solana-mainnet".to_string(),
                origin_node_id: "backup-a".to_string(),
                source_id: "replica-primary".to_string(),
                journal_id: [7; 16],
            },
            SpoolOptions {
                segment_target_bytes: 1024 * 1024,
                max_record_bytes: 1024,
            },
        )
        .unwrap();
        let durable = spool.append_and_sync(metadata, payload).unwrap();
        drop(spool);
        (root, durable)
    }

    fn durable_spool_record_pair(label: &str) -> (PathBuf, DurableSpoolRecord, DurableSpoolRecord) {
        let root = receipt_wal_path(label).with_extension("spool");
        let payload = b"durable-event";
        let metadata = IngressRecordMeta::from_payload(
            "solana-mainnet".to_string(),
            observation(),
            "replica-primary".to_string(),
            LogicalKey::Block {
                slot: 42,
                blockhash: [8; 32],
            },
            1,
            payload,
        );
        let mut spool = SpoolWriter::open(
            &root,
            SpoolJournalIdentity {
                cluster_id: "solana-mainnet".to_string(),
                origin_node_id: "backup-a".to_string(),
                source_id: "replica-primary".to_string(),
                journal_id: [7; 16],
            },
            SpoolOptions {
                segment_target_bytes: 1024 * 1024,
                max_record_bytes: 1024,
            },
        )
        .unwrap();
        let first = spool.append_and_sync(metadata.clone(), payload).unwrap();
        let second = spool.append_and_sync(metadata, payload).unwrap();
        drop(spool);
        (root, first, second)
    }

    fn gc_decision(
        state: ReplicaRecordState,
        target: &DurableSpoolRecord,
        durable_receipt: Option<&DurableLocalReceipt>,
    ) -> RecordGcEligibility {
        replica_record_gc_eligibility(state, target, durable_receipt)
    }

    #[test]
    fn network_ack_never_directly_authorizes_deletion() {
        let _verified = verified(ReceiptDisposition::DurablyStored);
        let (spool_root, local_record) = durable_spool_record("network-only");
        assert_eq!(
            gc_decision(
                ReplicaRecordState::VerifiedPrimaryReceipt,
                &local_record,
                None,
            ),
            RecordGcEligibility::NotDurable
        );
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn durable_stored_and_exact_duplicate_receipts_allow_gc_after_local_sync() {
        for disposition in [
            ReceiptDisposition::DurablyStored,
            ReceiptDisposition::AlreadyCommitted,
            ReceiptDisposition::DurablyStoredConflict,
        ] {
            let path = receipt_wal_path("gc");
            let (spool_root, local_record) = durable_spool_record("gc-record");
            let mut wal = ReceiptWal::open(&path).unwrap();
            let durable = wal
                .commit_verified(verified(disposition), &local_record)
                .unwrap();
            assert_eq!(
                gc_decision(
                    ReplicaRecordState::LocalReceiptDurable,
                    &local_record,
                    Some(&durable),
                ),
                RecordGcEligibility::EligibleAfterSegmentAudit
            );
            drop(wal);
            std::fs::remove_file(path).unwrap();
            std::fs::remove_dir_all(spool_root).unwrap();
        }
    }

    #[test]
    fn rejected_receipt_never_allows_gc() {
        let path = receipt_wal_path("rejected");
        let (spool_root, local_record) = durable_spool_record("rejected-record");
        let mut wal = ReceiptWal::open(&path).unwrap();
        let durable = wal
            .commit_verified(verified(ReceiptDisposition::Rejected), &local_record)
            .unwrap();
        assert_eq!(
            gc_decision(
                ReplicaRecordState::LocalReceiptDurable,
                &local_record,
                Some(&durable),
            ),
            RecordGcEligibility::Rejected
        );
        drop(wal);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn receipt_must_match_exact_record_and_content() {
        let expected_record = observation();
        let mut wrong = receipt(ReceiptDisposition::AlreadyCommitted);
        let expected_digest = wrong.content_digest;
        wrong.content_digest = ContentDigest([10; 32]);
        assert_eq!(
            verify_receipt(
                wrong,
                ExpectedReceipt {
                    cluster_id: "solana-mainnet",
                    primary_id: "nas-primary",
                    record: &expected_record,
                    content_digest: expected_digest,
                },
                &AcceptTestSignature,
            ),
            Err(ReceiptValidationError::WrongContent)
        );
    }

    #[test]
    fn already_committed_receipt_requires_archive_commit_digest() {
        let expected_record = observation();
        let mut incomplete = receipt(ReceiptDisposition::AlreadyCommitted);
        let expected_digest = incomplete.content_digest;
        incomplete.archive_commit_digest = None;
        assert_eq!(
            verify_receipt(
                incomplete,
                ExpectedReceipt {
                    cluster_id: "solana-mainnet",
                    primary_id: "nas-primary",
                    record: &expected_record,
                    content_digest: expected_digest,
                },
                &AcceptTestSignature,
            ),
            Err(ReceiptValidationError::InvalidField)
        );
    }

    #[test]
    fn transient_states_recover_to_local_spool() {
        for state in [
            ReplicaRecordState::Offered,
            ReplicaRecordState::Uploading,
            ReplicaRecordState::VerifiedPrimaryReceipt,
        ] {
            assert_eq!(state.recovers_as(), ReplicaRecordState::LocalSpoolDurable);
        }
    }

    #[test]
    fn receipt_wal_truncates_partial_crash_tail_before_appending() {
        let path = receipt_wal_path("recovery");
        let (spool_root, local_record) = durable_spool_record("recovery-record");
        let first_len = {
            let mut wal = ReceiptWal::open(&path).unwrap();
            wal.commit_verified(verified(ReceiptDisposition::DurablyStored), &local_record)
                .unwrap();
            std::fs::metadata(&path).unwrap().len()
        };
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(RECEIPT_FRAME_MAGIC).unwrap();
        file.write_all(&RECEIPT_FRAME_VERSION.to_le_bytes()[..1])
            .unwrap();
        file.sync_data().unwrap();
        drop(file);

        let wal = ReceiptWal::open(&path).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), first_len);
        drop(wal);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn committed_receipt_corruption_fails_without_truncating_later_bytes() {
        let path = receipt_wal_path("corrupt");
        let (spool_root, local_record) = durable_spool_record("corrupt-record");
        {
            let mut wal = ReceiptWal::open(&path).unwrap();
            wal.commit_verified(verified(ReceiptDisposition::DurablyStored), &local_record)
                .unwrap();
            wal.commit_verified(verified(ReceiptDisposition::DurablyStored), &local_record)
                .unwrap();
        }
        let original_len = std::fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(std::io::SeekFrom::Start(RECEIPT_WAL_HEADER_LEN))
            .unwrap();
        file.write_all(b"X").unwrap();
        file.sync_data().unwrap();
        drop(file);

        assert!(ReceiptWal::open(&path).is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), original_len);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn unrelated_nonempty_file_is_never_truncated() {
        let path = receipt_wal_path("canary");
        std::fs::write(&path, b"unrelated-canary-data").unwrap();
        let original = std::fs::read(&path).unwrap();
        assert!(ReceiptWal::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn receipt_wal_rejects_a_second_writer() {
        let path = receipt_wal_path("lock");
        let first = ReceiptWal::open(&path).unwrap();
        let error = ReceiptWal::open(&path).unwrap_err();
        assert!(error.to_string().contains("lock receipt WAL"));
        drop(first);
        let reopened = ReceiptWal::open(&path).unwrap();
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn receipt_wal_refuses_to_follow_a_symlink() {
        use std::os::unix::fs::symlink;

        let path = receipt_wal_path("symlink");
        let canary = receipt_wal_path("symlink-canary");
        std::fs::write(&canary, b"do-not-touch").unwrap();
        symlink(&canary, &path).unwrap();
        assert!(ReceiptWal::open(&path).is_err());
        assert_eq!(std::fs::read(&canary).unwrap(), b"do-not-touch");
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(canary).unwrap();
    }

    #[test]
    fn receipt_for_one_spool_record_cannot_delete_another() {
        let path = receipt_wal_path("mismatch");
        let (spool_root, first_record, other_record) =
            durable_spool_record_pair("mismatch-records");
        let mut wal = ReceiptWal::open(&path).unwrap();
        let durable = wal
            .commit_verified(verified(ReceiptDisposition::DurablyStored), &first_record)
            .unwrap();
        // Equal record/digest metadata is insufficient: the receipt WAL token is also bound to
        // the exact local frame that may be collected.
        assert_eq!(
            replica_record_gc_eligibility(
                ReplicaRecordState::LocalReceiptDurable,
                &other_record,
                Some(&durable),
            ),
            RecordGcEligibility::ReceiptMismatch
        );
        drop(wal);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }
}
