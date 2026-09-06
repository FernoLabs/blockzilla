//! Primary/replica protocol state and deletion-safety rules.
//!
//! Transport and cryptographic verification are intentionally separate from the retention state
//! machine. A network response can never directly make local data eligible for deletion: the
//! receipt must first be authenticated and then committed to the replica's receipt journal.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
};

use anyhow::{Context, Result as AnyResult, ensure};
use serde::{Deserialize, Serialize};

use super::{
    dedup::{ContentDigest, IngressRecordMeta, LogicalKey, ObservationId},
    spool::{DurableSpoolRecord, SpoolLocation},
};

pub const REPLICATION_PROTOCOL_VERSION: u16 = 1;
const RECEIPT_SIGNING_DOMAIN: &[u8] = b"BLOCKZILLA-INGEST-RECEIPT-v1";
const CUMULATIVE_ACK_SIGNING_DOMAIN: &[u8] = b"BLOCKZILLA-INGEST-CUMULATIVE-ACK-v1";
const CUMULATIVE_CHAIN_SEED_DOMAIN: &[u8] = b"BLOCKZILLA-INGEST-CUMULATIVE-CHAIN-SEED-v1";
const CUMULATIVE_CHAIN_STEP_DOMAIN: &[u8] = b"BLOCKZILLA-INGEST-CUMULATIVE-CHAIN-STEP-v1";
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

/// Stable identity of one independently ordered replication journal.
///
/// Sequence numbers are contiguous only inside this identity. A newly created journal receives a
/// new `journal_id`, even when it continues the same origin/source pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplicationStreamId {
    pub cluster_id: String,
    pub origin_node_id: String,
    pub source_id: String,
    pub journal_id: [u8; 16],
}

impl ReplicationStreamId {
    pub fn from_offer(offer: &ReplicationOffer) -> Self {
        Self {
            cluster_id: offer.cluster_id.clone(),
            origin_node_id: offer.record.origin_node_id.clone(),
            source_id: offer.source_id.clone(),
            journal_id: offer.record.journal_id,
        }
    }
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

/// Deterministic digest before the first observation in a replication stream.
///
/// The seed is stream-specific, preventing an otherwise identical observation prefix from being
/// transplanted between clusters, origins, sources, or journals.
pub fn cumulative_chain_seed(
    stream: &ReplicationStreamId,
) -> Result<ContentDigest, CumulativeAckValidationError> {
    validate_stream_id(stream)?;
    let mut encoded = Vec::with_capacity(256);
    encoded.extend_from_slice(CUMULATIVE_CHAIN_SEED_DOMAIN);
    encoded.extend_from_slice(&REPLICATION_PROTOCOL_VERSION.to_le_bytes());
    push_stream_id(&mut encoded, stream)?;
    Ok(ContentDigest(sha256(&encoded)))
}

/// Extend a stream's rolling digest with one complete replication offer.
///
/// `ReplicationOffer::content_digest` binds the exact payload bytes. The remaining offer fields
/// bind their observation identity, semantic identity, payload format, source, and commitment.
pub fn cumulative_chain_next(
    stream: &ReplicationStreamId,
    previous: ContentDigest,
    offer: &ReplicationOffer,
) -> Result<ContentDigest, CumulativeAckValidationError> {
    if offer.protocol_version != REPLICATION_PROTOCOL_VERSION {
        return Err(CumulativeAckValidationError::UnsupportedProtocol);
    }
    if ReplicationStreamId::from_offer(offer) != *stream {
        return Err(CumulativeAckValidationError::WrongStream);
    }
    validate_stream_id(stream)?;
    validate_replication_offer(offer)?;

    let mut encoded = Vec::with_capacity(512);
    encoded.extend_from_slice(CUMULATIVE_CHAIN_STEP_DOMAIN);
    encoded.extend_from_slice(&previous.0);
    push_replication_offer(&mut encoded, offer)?;
    Ok(ContentDigest(sha256(&encoded)))
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    Sha256::digest(bytes).into()
}

fn validate_stream_id(stream: &ReplicationStreamId) -> Result<(), CumulativeAckValidationError> {
    validate_bounded_string(&stream.cluster_id)?;
    validate_bounded_string(&stream.origin_node_id)?;
    validate_bounded_string(&stream.source_id)?;
    Ok(())
}

fn validate_replication_offer(
    offer: &ReplicationOffer,
) -> Result<(), CumulativeAckValidationError> {
    validate_bounded_string(&offer.cluster_id)?;
    validate_bounded_string(&offer.record.origin_node_id)?;
    validate_bounded_string(&offer.source_id)?;
    if offer.payload_format_version == 0 {
        return Err(CumulativeAckValidationError::InvalidField);
    }
    Ok(())
}

fn validate_bounded_string(value: &str) -> Result<(), CumulativeAckValidationError> {
    let mut characters = value.chars();
    let valid_first = characters
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric());
    let valid_rest = characters
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'));
    if value.len() > 64 || !valid_first || !valid_rest {
        Err(CumulativeAckValidationError::InvalidField)
    } else {
        Ok(())
    }
}

fn push_stream_id(
    output: &mut Vec<u8>,
    stream: &ReplicationStreamId,
) -> Result<(), CumulativeAckValidationError> {
    push_cumulative_bounded_string(output, &stream.cluster_id)?;
    push_cumulative_bounded_string(output, &stream.origin_node_id)?;
    push_cumulative_bounded_string(output, &stream.source_id)?;
    output.extend_from_slice(&stream.journal_id);
    Ok(())
}

fn push_replication_offer(
    output: &mut Vec<u8>,
    offer: &ReplicationOffer,
) -> Result<(), CumulativeAckValidationError> {
    output.extend_from_slice(&offer.protocol_version.to_le_bytes());
    push_cumulative_bounded_string(output, &offer.cluster_id)?;
    push_cumulative_bounded_string(output, &offer.record.origin_node_id)?;
    output.extend_from_slice(&offer.record.journal_id);
    output.extend_from_slice(&offer.record.sequence.to_le_bytes());
    push_cumulative_bounded_string(output, &offer.source_id)?;
    push_logical_key(output, &offer.logical_key);
    output.extend_from_slice(&offer.content_digest.0);
    output.extend_from_slice(&offer.payload_len.to_le_bytes());
    output.extend_from_slice(&offer.payload_format_version.to_le_bytes());
    output.push(commitment_tag(offer.commitment));
    Ok(())
}

fn push_logical_key(output: &mut Vec<u8>, key: &LogicalKey) {
    match key {
        LogicalKey::Block { slot, blockhash } => {
            output.push(1);
            output.extend_from_slice(&slot.to_le_bytes());
            output.extend_from_slice(blockhash);
        }
        LogicalKey::Entry {
            slot,
            entry_index,
            entry_hash,
        } => {
            output.push(2);
            output.extend_from_slice(&slot.to_le_bytes());
            output.extend_from_slice(&entry_index.to_le_bytes());
            output.extend_from_slice(entry_hash);
        }
        LogicalKey::Shred {
            slot,
            kind,
            shred_index,
            fec_set_index,
        } => {
            output.push(3);
            output.extend_from_slice(&slot.to_le_bytes());
            output.push(match kind {
                super::dedup::ShredKind::Data => 1,
                super::dedup::ShredKind::Coding => 2,
            });
            output.extend_from_slice(&shred_index.to_le_bytes());
            match fec_set_index {
                Some(index) => {
                    output.push(1);
                    output.extend_from_slice(&index.to_le_bytes());
                }
                None => output.push(0),
            }
        }
    }
}

fn commitment_tag(commitment: CommitmentEvidence) -> u8 {
    match commitment {
        CommitmentEvidence::Unknown => 1,
        CommitmentEvidence::Processed => 2,
        CommitmentEvidence::Confirmed => 3,
        CommitmentEvidence::Finalized => 4,
    }
}

fn disposition_tag(disposition: ReceiptDisposition) -> u8 {
    match disposition {
        ReceiptDisposition::DurablyStored => 1,
        ReceiptDisposition::AlreadyCommitted => 2,
        ReceiptDisposition::DurablyStoredConflict => 3,
        ReceiptDisposition::Rejected => 4,
    }
}

/// Signed proof that one primary durably holds a complete contiguous stream prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CumulativePrimaryAck {
    pub protocol_version: u16,
    pub stream: ReplicationStreamId,
    pub primary_id: String,
    pub primary_term: u64,
    pub through_sequence: u64,
    pub through_content_digest: ContentDigest,
    pub rolling_chain_digest: ContentDigest,
    pub disposition: ReceiptDisposition,
    pub durable_lsn: u64,
    pub signing_key_id: String,
    pub signature: Vec<u8>,
}

impl CumulativePrimaryAck {
    /// Canonical, domain-separated bytes covered by the cumulative ACK signature.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, CumulativeAckValidationError> {
        validate_signable_cumulative_ack_fields(self)?;
        let mut output = Vec::with_capacity(384);
        output.extend_from_slice(CUMULATIVE_ACK_SIGNING_DOMAIN);
        output.extend_from_slice(&self.protocol_version.to_le_bytes());
        push_stream_id(&mut output, &self.stream)?;
        push_cumulative_bounded_string(&mut output, &self.primary_id)?;
        output.extend_from_slice(&self.primary_term.to_le_bytes());
        output.extend_from_slice(&self.through_sequence.to_le_bytes());
        output.extend_from_slice(&self.through_content_digest.0);
        output.extend_from_slice(&self.rolling_chain_digest.0);
        output.push(disposition_tag(self.disposition));
        output.extend_from_slice(&self.durable_lsn.to_le_bytes());
        push_cumulative_bounded_string(&mut output, &self.signing_key_id)?;
        Ok(output)
    }
}

fn push_cumulative_bounded_string(
    output: &mut Vec<u8>,
    value: &str,
) -> Result<(), CumulativeAckValidationError> {
    validate_bounded_string(value)?;
    let length =
        u16::try_from(value.len()).map_err(|_| CumulativeAckValidationError::InvalidField)?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn validate_signable_cumulative_ack_fields(
    ack: &CumulativePrimaryAck,
) -> Result<(), CumulativeAckValidationError> {
    validate_stream_id(&ack.stream)?;
    validate_bounded_string(&ack.primary_id)?;
    validate_bounded_string(&ack.signing_key_id)?;
    if ack.primary_term == 0 || ack.durable_lsn == 0 {
        return Err(CumulativeAckValidationError::InvalidField);
    }
    Ok(())
}

/// Local expectations bind a signed ACK to an exact durable stream prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedCumulativeAck<'a> {
    pub stream: &'a ReplicationStreamId,
    pub primary_id: &'a str,
    pub minimum_primary_term: u64,
    pub through_sequence: u64,
    pub through_content_digest: ContentDigest,
    pub rolling_chain_digest: ContentDigest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CumulativeAckValidationError {
    UnsupportedProtocol,
    WrongStream,
    WrongPrimary,
    PrimaryTermRollback,
    WrongSequence,
    WrongContent,
    WrongChain,
    NonDurableDisposition,
    EmptySigningKeyId,
    EmptySignature,
    InvalidSignatureLength,
    InvalidField,
    InvalidSignature,
}

/// Pluggable verifier backed by configured trusted primary public keys.
#[allow(dead_code)] // The concrete Ed25519 keyring is wired independently from protocol state.
pub(crate) trait CumulativeAckSignatureVerifier {
    fn verify_cumulative_ack_signature(
        &self,
        key_id: &str,
        signing_bytes: &[u8],
        signature: &[u8],
    ) -> bool;
}

/// An authenticated cumulative ACK bound to one exact local stream prefix.
///
/// This value is not durable authority. It must first cross [`CumulativeAckWal::commit_verified`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCumulativeAck(CumulativePrimaryAck);

impl VerifiedCumulativeAck {
    pub fn ack(&self) -> &CumulativePrimaryAck {
        &self.0
    }
}

#[allow(dead_code)] // Used by the concrete replication client when transport lands.
pub(crate) fn verify_cumulative_ack<V: CumulativeAckSignatureVerifier>(
    ack: CumulativePrimaryAck,
    expected: ExpectedCumulativeAck<'_>,
    verifier: &V,
) -> std::result::Result<VerifiedCumulativeAck, CumulativeAckValidationError> {
    if ack.protocol_version != REPLICATION_PROTOCOL_VERSION {
        return Err(CumulativeAckValidationError::UnsupportedProtocol);
    }
    if ack.stream != *expected.stream {
        return Err(CumulativeAckValidationError::WrongStream);
    }
    if ack.primary_id != expected.primary_id {
        return Err(CumulativeAckValidationError::WrongPrimary);
    }
    if ack.primary_term < expected.minimum_primary_term {
        return Err(CumulativeAckValidationError::PrimaryTermRollback);
    }
    if ack.through_sequence != expected.through_sequence {
        return Err(CumulativeAckValidationError::WrongSequence);
    }
    if ack.through_content_digest != expected.through_content_digest {
        return Err(CumulativeAckValidationError::WrongContent);
    }
    if ack.rolling_chain_digest != expected.rolling_chain_digest {
        return Err(CumulativeAckValidationError::WrongChain);
    }
    if !ack.disposition.authorizes_replica_gc() {
        return Err(CumulativeAckValidationError::NonDurableDisposition);
    }
    if ack.signing_key_id.trim().is_empty() {
        return Err(CumulativeAckValidationError::EmptySigningKeyId);
    }
    if ack.signature.is_empty() {
        return Err(CumulativeAckValidationError::EmptySignature);
    }
    if ack.signature.len() != ED25519_SIGNATURE_BYTES {
        return Err(CumulativeAckValidationError::InvalidSignatureLength);
    }
    let signing_bytes = ack.signing_bytes()?;
    if !verifier.verify_cumulative_ack_signature(
        &ack.signing_key_id,
        &signing_bytes,
        &ack.signature,
    ) {
        return Err(CumulativeAckValidationError::InvalidSignature);
    }
    Ok(VerifiedCumulativeAck(ack))
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

const CUMULATIVE_ACK_WAL_MAGIC: &[u8; 8] = b"BZCACK01";
const CUMULATIVE_ACK_FRAME_MAGIC: &[u8; 4] = b"BZCA";
const CUMULATIVE_ACK_COMMIT_MAGIC: &[u8; 4] = b"CMIT";
// Version 2 binds an ACK to the complete physical WAL identity as well as its byte location. A
// location alone can repeat after a storage-generation rotation and must never authorize cursor
// advancement in another physical journal.
const CUMULATIVE_ACK_FRAME_VERSION: u16 = 2;
const CUMULATIVE_ACK_WAL_HEADER_LEN: u64 = CUMULATIVE_ACK_WAL_MAGIC.len() as u64;
const CUMULATIVE_ACK_FRAME_FIXED_LEN: u64 = 4 + 2 + 4 + 4;
const CUMULATIVE_ACK_FRAME_TRAILER_LEN: u64 = 4 + 4;
const MAX_CUMULATIVE_ACK_FRAME_BYTES: usize = 1024 * 1024;
/// Compact after this many bytes have accumulated beyond the smallest snapshot of the currently
/// retained stream states. The snapshot itself may legitimately exceed this value; using growth
/// rather than absolute WAL length avoids rewriting a large baseline on every later ACK.
const CUMULATIVE_ACK_COMPACT_GROWTH_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalLocalSpoolBinding {
    metadata: IngressRecordMeta,
    location: SpoolLocation,
}

impl PhysicalLocalSpoolBinding {
    fn from_record(record: &DurableSpoolRecord) -> Self {
        Self {
            metadata: record.metadata().clone(),
            location: record.location(),
        }
    }
}

/// Exact logical prefix tail mapped to one verified physical local WAL frame.
///
/// Hot storage generations have distinct physical journal IDs, but replication deliberately keeps
/// one logical stream across rotations. This capability can only be constructed inside the crate
/// after the physical frame and its logical mapping have both been verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableReplicationWitness {
    stream: ReplicationStreamId,
    through_sequence: u64,
    through_content_digest: ContentDigest,
    local_binding: PhysicalLocalSpoolBinding,
}

impl DurableReplicationWitness {
    pub(crate) fn from_verified_mapping(
        local_record: &DurableSpoolRecord,
        stream: ReplicationStreamId,
        through_sequence: u64,
        through_content_digest: ContentDigest,
    ) -> AnyResult<Self> {
        let metadata = local_record.metadata();
        ensure!(
            metadata.cluster_id == stream.cluster_id
                && metadata.observation.origin_node_id == stream.origin_node_id
                && metadata.source_id == stream.source_id,
            "logical replication stream does not match its physical local WAL frame"
        );
        ensure!(
            metadata.content_digest == through_content_digest,
            "logical replication digest does not match its physical local WAL frame"
        );
        Ok(Self {
            stream,
            through_sequence,
            through_content_digest,
            local_binding: PhysicalLocalSpoolBinding::from_record(local_record),
        })
    }

    pub fn local_location(&self) -> SpoolLocation {
        self.local_binding.location
    }
}

/// A verified ACK after it has been appended to the local cumulative-ACK journal and fsynced.
///
/// This is durable protocol evidence only. It deliberately exposes no deletion/GC operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCumulativeAck {
    ack: VerifiedCumulativeAck,
    local_binding: PhysicalLocalSpoolBinding,
}

/// Unforgeable in-process authority to retire a local raw-storage generation.
///
/// The only constructor lives on [`CumulativeAckWal`]: callers cannot promote a network ACK (or a
/// deserialized retention checkpoint) into deletion authority.  The cloned frame retains both the
/// signed cumulative prefix and the exact physical WAL frame that was verified before the ACK WAL
/// crossed its fsync boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableGcAuthorization {
    stored: StoredCumulativeAckFrame,
}

impl DurableGcAuthorization {
    pub fn ack(&self) -> &CumulativePrimaryAck {
        &self.stored.ack
    }

    pub(crate) fn local_metadata(&self) -> &IngressRecordMeta {
        &self.stored.local_binding.metadata
    }

    pub(crate) fn local_location(&self) -> SpoolLocation {
        self.stored.local_binding.location
    }

    pub(crate) fn matches_replication_witness(&self, witness: &DurableReplicationWitness) -> bool {
        self.stored.ack.stream == witness.stream
            && self.stored.ack.through_sequence == witness.through_sequence
            && self.stored.ack.through_content_digest == witness.through_content_digest
            && self.stored.local_binding == witness.local_binding
    }
}

impl DurableCumulativeAck {
    pub fn ack(&self) -> &CumulativePrimaryAck {
        self.ack.ack()
    }

    pub fn local_location(&self) -> SpoolLocation {
        self.local_binding.location
    }

    pub(crate) fn matches_replication_witness(&self, witness: &DurableReplicationWitness) -> bool {
        self.ack.ack().stream == witness.stream
            && self.ack.ack().through_sequence == witness.through_sequence
            && self.ack.ack().through_content_digest == witness.through_content_digest
            && self.local_binding == witness.local_binding
    }
}

#[derive(Debug, Default)]
struct CumulativeAckWalState {
    // A fencing term belongs to the logical primary cluster, not one node name. Keying this by
    // primary_id would let an old primary bypass the fence merely by changing identities.
    highest_primary_terms: HashMap<String, u64>,
    latest_stream_acks: HashMap<ReplicationStreamId, StoredCumulativeAckFrame>,
}

impl CumulativeAckWalState {
    fn validate_transition(&self, stored: &StoredCumulativeAckFrame) -> AnyResult<()> {
        let ack = &stored.ack;
        ensure!(
            ack.protocol_version == REPLICATION_PROTOCOL_VERSION,
            "unsupported cumulative ACK protocol version {}",
            ack.protocol_version
        );
        validate_signable_cumulative_ack_fields(ack)
            .map_err(|_| anyhow::anyhow!("cumulative ACK contains an invalid signable field"))?;
        ensure!(
            ack.disposition.authorizes_replica_gc(),
            "cumulative ACK disposition does not prove durable storage"
        );
        ensure!(
            ack.signature.len() == ED25519_SIGNATURE_BYTES,
            "cumulative ACK signature length is {}, expected {}",
            ack.signature.len(),
            ED25519_SIGNATURE_BYTES
        );

        if let Some(highest_term) = self.highest_primary_terms.get(&ack.stream.cluster_id) {
            ensure!(
                ack.primary_term >= *highest_term,
                "cumulative ACK primary term rollback: received {}, highest durable {}",
                ack.primary_term,
                highest_term
            );
        }

        if let Some(previous) = self.latest_stream_acks.get(&ack.stream) {
            if ack.primary_id != previous.ack.primary_id {
                ensure!(
                    ack.primary_term > previous.ack.primary_term,
                    "cumulative ACK primary identity changed without a higher fencing term"
                );
            }
            ensure!(
                ack.through_sequence >= previous.ack.through_sequence,
                "cumulative ACK sequence rollback: received {}, highest durable {}",
                ack.through_sequence,
                previous.ack.through_sequence
            );
            if ack.through_sequence == previous.ack.through_sequence {
                ensure!(
                    ack.through_content_digest == previous.ack.through_content_digest
                        && ack.rolling_chain_digest == previous.ack.rolling_chain_digest,
                    "cumulative ACK reused one sequence for a different digest chain"
                );
                ensure!(
                    stored.local_binding == previous.local_binding,
                    "cumulative ACK reused one sequence for a different local spool frame"
                );
            }
            if ack.primary_term == previous.ack.primary_term {
                ensure!(
                    ack.durable_lsn >= previous.ack.durable_lsn,
                    "cumulative ACK durable LSN rollback within primary term {}",
                    ack.primary_term
                );
            }
        }
        Ok(())
    }

    fn record(&mut self, stored: StoredCumulativeAckFrame) {
        let ack = &stored.ack;
        self.highest_primary_terms
            .entry(ack.stream.cluster_id.clone())
            .and_modify(|term| *term = (*term).max(ack.primary_term))
            .or_insert(ack.primary_term);
        self.latest_stream_acks.insert(ack.stream.clone(), stored);
    }
}

/// Append-only journal for authenticated cumulative ACKs.
///
/// `commit_verified` returns a durable token only after the complete frame and commit marker have
/// been flushed and `sync_data` succeeds. Recovery truncates only an incomplete final frame;
/// committed corruption and primary-term/sequence rollback fail closed.
#[derive(Debug)]
pub struct CumulativeAckWal {
    path: PathBuf,
    /// Stable exclusion survives atomic WAL replacement during compaction.
    _lock_file: File,
    writer: BufWriter<File>,
    state: CumulativeAckWalState,
    compacted_baseline_bytes: u64,
    poisoned: bool,
}

impl CumulativeAckWal {
    pub fn open(path: impl AsRef<Path>) -> AnyResult<Self> {
        let path = path.as_ref().to_path_buf();
        let parent = durable_parent(&path);
        create_dir_all_durable(&parent)?;
        let lock_path = cumulative_ack_lock_path(&path);
        let lock_file = open_cumulative_ack_lock_file(&lock_path)?;
        try_lock_cumulative_ack_wal(&lock_file, &lock_path)?;
        remove_orphaned_cumulative_ack_compaction(&path)?;
        let mut file = open_cumulative_ack_file(&path)?;
        try_lock_cumulative_ack_wal(&file, &path)?;
        initialize_cumulative_ack_wal(&mut file, &path)?;
        sync_directory(&parent)?;
        let file_len = file.metadata()?.len();
        let recovered = recover_cumulative_ack_wal(&mut file, &path)?;
        if recovered.valid_len != file_len {
            file.set_len(recovered.valid_len)
                .with_context(|| format!("truncate cumulative ACK WAL {}", path.display()))?;
            file.sync_data()
                .with_context(|| format!("sync recovered cumulative ACK WAL {}", path.display()))?;
        }
        file.seek(std::io::SeekFrom::End(0))
            .with_context(|| format!("seek cumulative ACK WAL {}", path.display()))?;
        let compacted_baseline_bytes = cumulative_ack_compacted_length(&recovered.state)?;
        Ok(Self {
            path,
            _lock_file: lock_file,
            writer: BufWriter::new(file),
            state: recovered.state,
            compacted_baseline_bytes,
            poisoned: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn highest_primary_term(&self, cluster_id: &str) -> Option<u64> {
        self.state.highest_primary_terms.get(cluster_id).copied()
    }

    /// Latest cumulative ACK that previously crossed this WAL's fsync boundary for `stream`.
    /// Recovery revalidates framing, checksums, transition monotonicity, and the local spool
    /// binding stored with the ACK. Signature trust was established before the original append.
    pub fn latest_stream_ack(&self, stream: &ReplicationStreamId) -> Option<&CumulativePrimaryAck> {
        self.state
            .latest_stream_acks
            .get(stream)
            .map(|stored| &stored.ack)
    }

    /// Return deletion authority only from an ACK frame recovered from (or fsynced into) this
    /// locked WAL.  Keeping the wrapper's fields private prevents ordinary protocol values from
    /// being used as local-GC capabilities.
    pub fn durable_gc_authorization(
        &self,
        stream: &ReplicationStreamId,
    ) -> AnyResult<Option<DurableGcAuthorization>> {
        ensure!(
            !self.poisoned,
            "cumulative ACK WAL is poisoned; reopen it before requesting GC authority"
        );
        Ok(self
            .state
            .latest_stream_acks
            .get(stream)
            .cloned()
            .map(|stored| DurableGcAuthorization { stored }))
    }

    /// Persist an already-authenticated ACK and bind it to the exact local prefix-tail frame.
    pub fn commit_verified(
        &mut self,
        ack: VerifiedCumulativeAck,
        local_tail: &DurableSpoolRecord,
    ) -> AnyResult<DurableCumulativeAck> {
        let signed = ack.ack();
        let metadata = local_tail.metadata();
        ensure!(
            metadata.cluster_id == signed.stream.cluster_id
                && metadata.observation.origin_node_id == signed.stream.origin_node_id
                && metadata.observation.journal_id == signed.stream.journal_id
                && metadata.source_id == signed.stream.source_id,
            "cumulative ACK stream does not match durable local spool record"
        );
        ensure!(
            metadata.observation.sequence == signed.through_sequence,
            "cumulative ACK sequence does not match durable local spool record"
        );
        ensure!(
            metadata.content_digest == signed.through_content_digest,
            "cumulative ACK content digest does not match durable local spool record"
        );

        self.commit_bound(ack, PhysicalLocalSpoolBinding::from_record(local_tail))
    }

    /// Persist an authenticated ACK against a verified logical-to-physical generation mapping.
    pub fn commit_verified_replication(
        &mut self,
        ack: VerifiedCumulativeAck,
        witness: &DurableReplicationWitness,
    ) -> AnyResult<DurableCumulativeAck> {
        let signed = ack.ack();
        ensure!(
            signed.stream == witness.stream,
            "cumulative ACK stream does not match durable replication witness"
        );
        ensure!(
            signed.through_sequence == witness.through_sequence,
            "cumulative ACK sequence does not match durable replication witness"
        );
        ensure!(
            signed.through_content_digest == witness.through_content_digest,
            "cumulative ACK content digest does not match durable replication witness"
        );
        self.commit_bound(ack, witness.local_binding.clone())
    }

    /// Durably record a newly authenticated fencing term or signing-key transition for the exact
    /// prefix already bound in this WAL. This is used when `GetAck` returns the same content prefix
    /// under a newer primary term: the stronger fence must survive a crash even though the source
    /// cursor does not advance and no new local payload witness exists.
    pub fn commit_verified_existing_prefix(
        &mut self,
        ack: VerifiedCumulativeAck,
    ) -> AnyResult<DurableCumulativeAck> {
        let signed = ack.ack();
        let previous = self
            .state
            .latest_stream_acks
            .get(&signed.stream)
            .context("cannot update a cumulative ACK fence without a durable local prefix")?;
        ensure!(
            signed.through_sequence == previous.ack.through_sequence
                && signed.through_content_digest == previous.ack.through_content_digest
                && signed.rolling_chain_digest == previous.ack.rolling_chain_digest,
            "cumulative ACK fence update changed the durable content prefix"
        );
        self.commit_bound(ack, previous.local_binding.clone())
    }

    fn commit_bound(
        &mut self,
        ack: VerifiedCumulativeAck,
        local_binding: PhysicalLocalSpoolBinding,
    ) -> AnyResult<DurableCumulativeAck> {
        ensure!(
            !self.poisoned,
            "cumulative ACK WAL writer is poisoned; reopen it to recover before appending"
        );
        let signed = ack.ack();
        let stored = StoredCumulativeAckFrame {
            ack: signed.clone(),
            local_binding: local_binding.clone(),
        };
        self.state.validate_transition(&stored)?;
        self.compact_if_needed()?;
        let frame = encode_cumulative_ack_frame(&stored)?;

        self.poisoned = true;
        self.writer
            .write_all(&frame)
            .with_context(|| format!("write cumulative ACK frame {}", self.path.display()))?;
        self.writer
            .flush()
            .with_context(|| format!("flush cumulative ACK WAL {}", self.path.display()))?;
        self.writer
            .get_ref()
            .sync_data()
            .with_context(|| format!("sync cumulative ACK WAL {}", self.path.display()))?;
        self.state.record(stored);
        self.poisoned = false;
        Ok(DurableCumulativeAck { ack, local_binding })
    }

    fn compact_if_needed(&mut self) -> AnyResult<()> {
        let current_len = self
            .writer
            .get_ref()
            .metadata()
            .with_context(|| format!("inspect cumulative ACK WAL {}", self.path.display()))?
            .len();
        let compact_after = self
            .compacted_baseline_bytes
            .checked_add(CUMULATIVE_ACK_COMPACT_GROWTH_BYTES)
            .context("cumulative ACK compaction threshold overflow")?;
        if current_len < compact_after {
            return Ok(());
        }

        self.poisoned = true;
        self.writer
            .flush()
            .with_context(|| format!("flush cumulative ACK WAL {}", self.path.display()))?;
        let parent = durable_parent(&self.path);
        // A single deterministic temporary path bounds crash debris to one file. `open` removes a
        // validated orphan while holding the stable sidecar lock before it touches the live WAL.
        let temporary = cumulative_ack_compaction_path(&self.path)?;
        let compacted = (|| -> AnyResult<(BufWriter<File>, u64)> {
            let mut file = open_receipt_descriptor(&temporary, true).with_context(|| {
                format!(
                    "create cumulative ACK compacted WAL {}",
                    temporary.display()
                )
            })?;
            try_lock_cumulative_ack_wal(&file, &temporary)?;
            file.write_all(CUMULATIVE_ACK_WAL_MAGIC)?;

            let mut latest = self.state.latest_stream_acks.values().collect::<Vec<_>>();
            latest.sort_unstable_by(|left, right| {
                (
                    left.ack.stream.cluster_id.as_str(),
                    left.ack.primary_term,
                    left.ack.stream.origin_node_id.as_str(),
                    left.ack.stream.source_id.as_str(),
                    left.ack.stream.journal_id,
                )
                    .cmp(&(
                        right.ack.stream.cluster_id.as_str(),
                        right.ack.primary_term,
                        right.ack.stream.origin_node_id.as_str(),
                        right.ack.stream.source_id.as_str(),
                        right.ack.stream.journal_id,
                    ))
            });
            for stored in latest {
                file.write_all(&encode_cumulative_ack_frame(stored)?)?;
            }
            file.sync_data().with_context(|| {
                format!("sync cumulative ACK compacted WAL {}", temporary.display())
            })?;
            let compacted_len = file
                .metadata()
                .with_context(|| {
                    format!(
                        "inspect cumulative ACK compacted WAL {}",
                        temporary.display()
                    )
                })?
                .len();
            fs::rename(&temporary, &self.path).with_context(|| {
                format!(
                    "publish cumulative ACK compacted WAL {}",
                    self.path.display()
                )
            })?;
            sync_directory(&parent)?;
            file.seek(std::io::SeekFrom::End(0))?;
            Ok((BufWriter::new(file), compacted_len))
        })();
        match compacted {
            Ok((replacement, compacted_len)) => {
                let previous = std::mem::replace(&mut self.writer, replacement);
                drop(previous);
                self.compacted_baseline_bytes = compacted_len;
                self.poisoned = false;
                Ok(())
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                Err(error)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCumulativeAckFrame {
    ack: CumulativePrimaryAck,
    local_binding: PhysicalLocalSpoolBinding,
}

fn encode_cumulative_ack_frame(stored: &StoredCumulativeAckFrame) -> AnyResult<Vec<u8>> {
    let payload = serde_json::to_vec(stored).context("encode cumulative ACK")?;
    ensure!(
        payload.len() <= MAX_CUMULATIVE_ACK_FRAME_BYTES,
        "cumulative ACK frame exceeds {} bytes",
        MAX_CUMULATIVE_ACK_FRAME_BYTES
    );
    let length = u32::try_from(payload.len()).context("cumulative ACK length exceeds u32")?;
    let version_bytes = CUMULATIVE_ACK_FRAME_VERSION.to_le_bytes();
    let length_bytes = length.to_le_bytes();
    let mut header_crc = Crc32c::new();
    header_crc.update(CUMULATIVE_ACK_FRAME_MAGIC);
    header_crc.update(&version_bytes);
    header_crc.update(&length_bytes);
    let mut payload_crc = Crc32c::new();
    payload_crc.update(&payload);

    let capacity = usize::try_from(CUMULATIVE_ACK_FRAME_FIXED_LEN)
        .ok()
        .and_then(|fixed| fixed.checked_add(payload.len()))
        .and_then(|length| {
            usize::try_from(CUMULATIVE_ACK_FRAME_TRAILER_LEN)
                .ok()
                .and_then(|trailer| length.checked_add(trailer))
        })
        .context("cumulative ACK encoded frame length overflows usize")?;
    let mut frame = Vec::with_capacity(capacity);
    frame.extend_from_slice(CUMULATIVE_ACK_FRAME_MAGIC);
    frame.extend_from_slice(&version_bytes);
    frame.extend_from_slice(&length_bytes);
    frame.extend_from_slice(&header_crc.finish().to_le_bytes());
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(&payload_crc.finish().to_le_bytes());
    frame.extend_from_slice(CUMULATIVE_ACK_COMMIT_MAGIC);
    Ok(frame)
}

fn cumulative_ack_compacted_length(state: &CumulativeAckWalState) -> AnyResult<u64> {
    state
        .latest_stream_acks
        .values()
        .try_fold(CUMULATIVE_ACK_WAL_HEADER_LEN, |length, stored| {
            let frame_len = u64::try_from(encode_cumulative_ack_frame(stored)?.len())
                .context("cumulative ACK frame length exceeds u64")?;
            length
                .checked_add(frame_len)
                .context("cumulative ACK compacted WAL length overflow")
        })
}

#[derive(Debug)]
struct RecoveredCumulativeAckWal {
    valid_len: u64,
    state: CumulativeAckWalState,
}

fn recover_cumulative_ack_wal(
    file: &mut File,
    path: &Path,
) -> AnyResult<RecoveredCumulativeAckWal> {
    file.seek(std::io::SeekFrom::Start(0))
        .with_context(|| format!("seek cumulative ACK WAL {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut wal_magic = [0u8; 8];
    reader
        .read_exact(&mut wal_magic)
        .with_context(|| format!("read cumulative ACK WAL header {}", path.display()))?;
    ensure!(
        &wal_magic == CUMULATIVE_ACK_WAL_MAGIC,
        "invalid cumulative ACK WAL header in {}",
        path.display()
    );
    let mut valid_len = CUMULATIVE_ACK_WAL_HEADER_LEN;
    let mut state = CumulativeAckWalState::default();
    loop {
        let frame_offset = valid_len;
        let mut frame_magic = [0u8; 4];
        if !read_cumulative_ack_exact_or_tail(&mut reader, &mut frame_magic, path)? {
            break;
        }
        ensure!(
            &frame_magic == CUMULATIVE_ACK_FRAME_MAGIC,
            "corrupt cumulative ACK frame magic at {} in {}",
            frame_offset,
            path.display()
        );

        let mut version_bytes = [0u8; 2];
        if !read_cumulative_ack_exact_or_tail(&mut reader, &mut version_bytes, path)? {
            break;
        }
        let mut length_bytes = [0u8; 4];
        if !read_cumulative_ack_exact_or_tail(&mut reader, &mut length_bytes, path)? {
            break;
        }
        let mut expected_header_crc_bytes = [0u8; 4];
        if !read_cumulative_ack_exact_or_tail(&mut reader, &mut expected_header_crc_bytes, path)? {
            break;
        }
        let mut header_crc = Crc32c::new();
        header_crc.update(&frame_magic);
        header_crc.update(&version_bytes);
        header_crc.update(&length_bytes);
        ensure!(
            header_crc.finish() == u32::from_le_bytes(expected_header_crc_bytes),
            "cumulative ACK header checksum mismatch at {} in {}",
            frame_offset,
            path.display()
        );
        ensure!(
            u16::from_le_bytes(version_bytes) == CUMULATIVE_ACK_FRAME_VERSION,
            "unsupported cumulative ACK frame version at {} in {}",
            frame_offset,
            path.display()
        );

        let payload_len = u32::from_le_bytes(length_bytes) as usize;
        ensure!(
            payload_len <= MAX_CUMULATIVE_ACK_FRAME_BYTES,
            "cumulative ACK frame length {} exceeds maximum at {} in {}",
            payload_len,
            frame_offset,
            path.display()
        );
        let mut payload = vec![0u8; payload_len];
        if !read_cumulative_ack_exact_or_tail(&mut reader, &mut payload, path)? {
            break;
        }
        let mut expected_payload_crc_bytes = [0u8; 4];
        if !read_cumulative_ack_exact_or_tail(&mut reader, &mut expected_payload_crc_bytes, path)? {
            break;
        }
        let mut commit_magic = [0u8; 4];
        if !read_cumulative_ack_exact_or_tail(&mut reader, &mut commit_magic, path)? {
            break;
        }
        let mut payload_crc = Crc32c::new();
        payload_crc.update(&payload);
        ensure!(
            payload_crc.finish() == u32::from_le_bytes(expected_payload_crc_bytes),
            "cumulative ACK payload checksum mismatch at {} in {}",
            frame_offset,
            path.display()
        );
        ensure!(
            &commit_magic == CUMULATIVE_ACK_COMMIT_MAGIC,
            "missing cumulative ACK commit marker at {} in {}",
            frame_offset,
            path.display()
        );
        let stored: StoredCumulativeAckFrame =
            serde_json::from_slice(&payload).with_context(|| {
                format!(
                    "decode committed cumulative ACK frame at {} in {}",
                    frame_offset,
                    path.display()
                )
            })?;
        state.validate_transition(&stored).with_context(|| {
            format!(
                "validate committed cumulative ACK frame at {} in {}",
                frame_offset,
                path.display()
            )
        })?;
        state.record(stored);
        valid_len = valid_len
            .checked_add(CUMULATIVE_ACK_FRAME_FIXED_LEN)
            .and_then(|length| length.checked_add(payload_len as u64))
            .and_then(|length| length.checked_add(CUMULATIVE_ACK_FRAME_TRAILER_LEN))
            .context("cumulative ACK WAL length overflow")?;
    }
    Ok(RecoveredCumulativeAckWal { valid_len, state })
}

fn initialize_cumulative_ack_wal(file: &mut File, path: &Path) -> AnyResult<()> {
    let length = file.metadata()?.len();
    if length >= CUMULATIVE_ACK_WAL_HEADER_LEN {
        return Ok(());
    }
    let mut existing = vec![0u8; length as usize];
    file.seek(std::io::SeekFrom::Start(0))?;
    file.read_exact(&mut existing)?;
    ensure!(
        CUMULATIVE_ACK_WAL_MAGIC.starts_with(&existing),
        "refusing non-cumulative-ACK file with invalid partial header: {}",
        path.display()
    );
    file.set_len(0)?;
    file.seek(std::io::SeekFrom::Start(0))?;
    file.write_all(CUMULATIVE_ACK_WAL_MAGIC)
        .with_context(|| format!("write cumulative ACK WAL header {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("sync cumulative ACK WAL header {}", path.display()))
}

fn read_cumulative_ack_exact_or_tail<R: Read>(
    reader: &mut R,
    output: &mut [u8],
    path: &Path,
) -> AnyResult<bool> {
    match reader.read_exact(output) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("read cumulative ACK WAL {}", path.display()))
        }
    }
}

fn open_cumulative_ack_file(path: &Path) -> AnyResult<File> {
    let file = match open_receipt_descriptor(path, true) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            open_receipt_descriptor(path, false)
                .with_context(|| format!("open existing cumulative ACK WAL {}", path.display()))?
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("create cumulative ACK WAL {}", path.display()));
        }
    };
    ensure!(
        file.metadata()?.file_type().is_file(),
        "cumulative ACK WAL path is not a regular file: {}",
        path.display()
    );
    Ok(file)
}

fn cumulative_ack_lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}

fn cumulative_ack_compaction_path(path: &Path) -> AnyResult<PathBuf> {
    let parent = durable_parent(path);
    let file_name = path
        .file_name()
        .context("cumulative ACK WAL has no file name")?
        .to_string_lossy();
    Ok(parent.join(format!(".{file_name}.compact.tmp")))
}

fn remove_orphaned_cumulative_ack_compaction(path: &Path) -> AnyResult<()> {
    let temporary = cumulative_ack_compaction_path(path)?;
    let metadata = match fs::symlink_metadata(&temporary) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "inspect orphaned cumulative ACK compaction {}",
                    temporary.display()
                )
            });
        }
    };
    ensure!(
        metadata.file_type().is_file(),
        "refusing non-regular cumulative ACK compaction temporary: {}",
        temporary.display()
    );
    #[cfg(unix)]
    {
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            metadata.uid() == effective_uid || metadata.uid() == 0,
            "refusing cumulative ACK compaction temporary owned by uid {}: {}",
            metadata.uid(),
            temporary.display()
        );
        ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "refusing cumulative ACK compaction temporary with group/world permissions: {}",
            temporary.display()
        );
        ensure!(
            metadata.nlink() == 1,
            "refusing multiply-linked cumulative ACK compaction temporary: {}",
            temporary.display()
        );
    }
    fs::remove_file(&temporary).with_context(|| {
        format!(
            "remove orphaned cumulative ACK compaction {}",
            temporary.display()
        )
    })?;
    sync_directory(&durable_parent(path))
}

fn open_cumulative_ack_lock_file(path: &Path) -> AnyResult<File> {
    let file = match open_receipt_descriptor(path, true) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            open_receipt_descriptor(path, false)
                .with_context(|| format!("open cumulative ACK lock {}", path.display()))?
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("create cumulative ACK lock {}", path.display()));
        }
    };
    ensure!(
        file.metadata()?.file_type().is_file(),
        "cumulative ACK lock path is not a regular file: {}",
        path.display()
    );
    Ok(file)
}

#[cfg(unix)]
fn try_lock_cumulative_ack_wal(file: &File, path: &Path) -> AnyResult<()> {
    // SAFETY: the descriptor remains owned by the cumulative ACK WAL for the lock lifetime.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
            .with_context(|| format!("lock cumulative ACK WAL {}", path.display()))
    }
}

#[cfg(not(unix))]
fn try_lock_cumulative_ack_wal(file: &File, path: &Path) -> AnyResult<()> {
    file.try_lock()
        .with_context(|| format!("lock cumulative ACK WAL {}", path.display()))
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

    struct AcceptCumulativeSignature;

    impl CumulativeAckSignatureVerifier for AcceptCumulativeSignature {
        fn verify_cumulative_ack_signature(
            &self,
            _key_id: &str,
            _signing_bytes: &[u8],
            _signature: &[u8],
        ) -> bool {
            true
        }
    }

    struct ExactCumulativeSignature {
        key_id: String,
        signing_bytes: Vec<u8>,
        signature: Vec<u8>,
    }

    impl CumulativeAckSignatureVerifier for ExactCumulativeSignature {
        fn verify_cumulative_ack_signature(
            &self,
            key_id: &str,
            signing_bytes: &[u8],
            signature: &[u8],
        ) -> bool {
            key_id == self.key_id
                && signing_bytes == self.signing_bytes
                && signature == self.signature
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

    fn replication_stream() -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "solana-mainnet".to_string(),
            origin_node_id: "backup-a".to_string(),
            source_id: "replica-primary".to_string(),
            journal_id: [7; 16],
        }
    }

    fn replication_offer(sequence: u64) -> ReplicationOffer {
        let logical_key = LogicalKey::Block {
            slot: 42 + sequence,
            blockhash: [sequence as u8; 32],
        };
        let payload = format!("payload-{sequence}");
        ReplicationOffer {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            cluster_id: "solana-mainnet".to_string(),
            record: ObservationId {
                origin_node_id: "backup-a".to_string(),
                journal_id: [7; 16],
                sequence,
            },
            source_id: "replica-primary".to_string(),
            logical_key: logical_key.clone(),
            content_digest: compute_content_digest(
                "solana-mainnet",
                &logical_key,
                1,
                payload.as_bytes(),
            ),
            payload_len: payload.len() as u64,
            payload_format_version: 1,
            commitment: CommitmentEvidence::Confirmed,
        }
    }

    fn cumulative_ack(primary_term: u64) -> CumulativePrimaryAck {
        CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: replication_stream(),
            primary_id: "nas-primary".to_string(),
            primary_term,
            through_sequence: observation().sequence,
            through_content_digest: receipt(ReceiptDisposition::DurablyStored).content_digest,
            rolling_chain_digest: ContentDigest([4; 32]),
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: 123,
            signing_key_id: "2026-q3".to_string(),
            signature: vec![1; ED25519_SIGNATURE_BYTES],
        }
    }

    fn expected_cumulative_ack<'a>(
        ack: &'a CumulativePrimaryAck,
        minimum_primary_term: u64,
    ) -> ExpectedCumulativeAck<'a> {
        ExpectedCumulativeAck {
            stream: &ack.stream,
            primary_id: &ack.primary_id,
            minimum_primary_term,
            through_sequence: ack.through_sequence,
            through_content_digest: ack.through_content_digest,
            rolling_chain_digest: ack.rolling_chain_digest,
        }
    }

    fn verified_cumulative_ack(primary_term: u64) -> VerifiedCumulativeAck {
        let ack = cumulative_ack(primary_term);
        let expected = expected_cumulative_ack(&ack, 0);
        verify_cumulative_ack(ack.clone(), expected, &AcceptCumulativeSignature).unwrap()
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

    #[test]
    fn cumulative_chain_is_deterministic_and_bound_to_stream_and_offer() {
        let stream = replication_stream();
        let seed = cumulative_chain_seed(&stream).unwrap();
        assert_eq!(seed, cumulative_chain_seed(&stream).unwrap());
        assert_eq!(
            seed,
            ContentDigest([
                0xdc, 0x30, 0xef, 0xbc, 0x10, 0x6e, 0x94, 0x7b, 0x51, 0xe0, 0x89, 0xd6, 0xe3, 0xe5,
                0x04, 0x3d, 0x10, 0xcd, 0xab, 0x87, 0x2e, 0xf8, 0x1d, 0x4e, 0x48, 0xbd, 0xff, 0xa0,
                0xa5, 0x8b, 0x76, 0xfb,
            ])
        );

        let offer = replication_offer(0);
        let first = cumulative_chain_next(&stream, seed, &offer).unwrap();
        assert_eq!(first, cumulative_chain_next(&stream, seed, &offer).unwrap());
        assert_eq!(
            first,
            ContentDigest([
                0x23, 0x7c, 0x67, 0x4b, 0xc7, 0xc6, 0xdb, 0xcd, 0x44, 0x63, 0xe7, 0xa6, 0x02, 0x95,
                0x0d, 0x25, 0xe1, 0x2c, 0x7c, 0x06, 0xb5, 0x23, 0xa8, 0x88, 0x7e, 0x76, 0x70, 0xa8,
                0x47, 0xe1, 0x68, 0x9f,
            ])
        );

        let mut changed_stream = stream.clone();
        changed_stream.journal_id = [8; 16];
        assert_ne!(seed, cumulative_chain_seed(&changed_stream).unwrap());
        assert_eq!(
            cumulative_chain_next(&changed_stream, seed, &offer),
            Err(CumulativeAckValidationError::WrongStream)
        );

        let mut changed_offer = offer.clone();
        changed_offer.content_digest = ContentDigest([9; 32]);
        assert_ne!(
            first,
            cumulative_chain_next(&stream, seed, &changed_offer).unwrap()
        );
    }

    #[test]
    fn cumulative_ack_signing_is_canonical_and_detects_signed_field_tampering() {
        let mut signed = cumulative_ack(7);
        signed.signature.clear();
        let signing_bytes = signed.signing_bytes().unwrap();
        assert_eq!(signing_bytes, signed.signing_bytes().unwrap());
        assert_eq!(
            sha256(&signing_bytes),
            [
                0x5f, 0x30, 0xdb, 0xbf, 0x89, 0x25, 0xab, 0x74, 0x11, 0x75, 0x64, 0xc0, 0x88, 0x3c,
                0x67, 0xe0, 0xe5, 0x95, 0x29, 0x55, 0xda, 0x7d, 0xbb, 0x8d, 0x30, 0x7e, 0x9d, 0xe9,
                0x51, 0xdf, 0x42, 0x23,
            ]
        );
        let signature = vec![0xa5; ED25519_SIGNATURE_BYTES];
        signed.signature.clone_from(&signature);
        let expected_stream = signed.stream.clone();
        let expected_primary = signed.primary_id.clone();
        let expected_sequence = signed.through_sequence;
        let expected_content = signed.through_content_digest;
        let expected_chain = signed.rolling_chain_digest;
        let verifier = ExactCumulativeSignature {
            key_id: signed.signing_key_id.clone(),
            signing_bytes,
            signature,
        };
        let expected = || ExpectedCumulativeAck {
            stream: &expected_stream,
            primary_id: &expected_primary,
            minimum_primary_term: 7,
            through_sequence: expected_sequence,
            through_content_digest: expected_content,
            rolling_chain_digest: expected_chain,
        };

        verify_cumulative_ack(signed.clone(), expected(), &verifier).unwrap();

        let mut tampered = signed.clone();
        tampered.durable_lsn += 1;
        assert_eq!(
            verify_cumulative_ack(tampered, expected(), &verifier),
            Err(CumulativeAckValidationError::InvalidSignature)
        );
        let mut tampered = signed.clone();
        tampered.disposition = ReceiptDisposition::DurablyStoredConflict;
        assert_eq!(
            verify_cumulative_ack(tampered, expected(), &verifier),
            Err(CumulativeAckValidationError::InvalidSignature)
        );
        let mut tampered = signed.clone();
        tampered.primary_term += 1;
        assert_eq!(
            verify_cumulative_ack(tampered, expected(), &verifier),
            Err(CumulativeAckValidationError::InvalidSignature)
        );
        let mut tampered = signed;
        tampered.signing_key_id = "other-key".to_string();
        assert_eq!(
            verify_cumulative_ack(tampered, expected(), &verifier),
            Err(CumulativeAckValidationError::InvalidSignature)
        );
    }

    #[test]
    fn cumulative_ack_rejects_wrong_stream_sequence_content_chain_and_term() {
        let ack = cumulative_ack(7);
        let expected_stream = ack.stream.clone();
        let expected_primary = ack.primary_id.clone();
        let expected_sequence = ack.through_sequence;
        let expected_content = ack.through_content_digest;
        let expected_chain = ack.rolling_chain_digest;
        let expected = || ExpectedCumulativeAck {
            stream: &expected_stream,
            primary_id: &expected_primary,
            minimum_primary_term: 7,
            through_sequence: expected_sequence,
            through_content_digest: expected_content,
            rolling_chain_digest: expected_chain,
        };

        let mut wrong = ack.clone();
        wrong.stream.journal_id = [9; 16];
        assert_eq!(
            verify_cumulative_ack(wrong, expected(), &AcceptCumulativeSignature),
            Err(CumulativeAckValidationError::WrongStream)
        );
        let mut wrong = ack.clone();
        wrong.through_sequence += 1;
        assert_eq!(
            verify_cumulative_ack(wrong, expected(), &AcceptCumulativeSignature),
            Err(CumulativeAckValidationError::WrongSequence)
        );
        let mut wrong = ack.clone();
        wrong.through_content_digest = ContentDigest([10; 32]);
        assert_eq!(
            verify_cumulative_ack(wrong, expected(), &AcceptCumulativeSignature),
            Err(CumulativeAckValidationError::WrongContent)
        );
        let mut wrong = ack.clone();
        wrong.rolling_chain_digest = ContentDigest([11; 32]);
        assert_eq!(
            verify_cumulative_ack(wrong, expected(), &AcceptCumulativeSignature),
            Err(CumulativeAckValidationError::WrongChain)
        );
        let mut wrong = ack.clone();
        wrong.durable_lsn = 0;
        assert_eq!(
            verify_cumulative_ack(wrong, expected(), &AcceptCumulativeSignature),
            Err(CumulativeAckValidationError::InvalidField)
        );
        let mut wrong = ack.clone();
        wrong.disposition = ReceiptDisposition::Rejected;
        assert_eq!(
            verify_cumulative_ack(wrong, expected(), &AcceptCumulativeSignature),
            Err(CumulativeAckValidationError::NonDurableDisposition)
        );
        let mut wrong = ack;
        wrong.primary_term = 6;
        assert_eq!(
            verify_cumulative_ack(wrong, expected(), &AcceptCumulativeSignature),
            Err(CumulativeAckValidationError::PrimaryTermRollback)
        );
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

    fn cumulative_ack_wal_path(label: &str) -> PathBuf {
        receipt_wal_path(&format!("cumulative-{label}"))
    }

    fn durable_spool_record(label: &str) -> (PathBuf, DurableSpoolRecord) {
        durable_spool_record_with_journal(label, [7; 16])
    }

    fn durable_spool_record_with_journal(
        label: &str,
        journal_id: [u8; 16],
    ) -> (PathBuf, DurableSpoolRecord) {
        let root = receipt_wal_path(label).with_extension("spool");
        let payload = b"durable-event";
        let mut physical_observation = observation();
        physical_observation.journal_id = journal_id;
        let metadata = IngressRecordMeta::from_payload(
            "solana-mainnet".to_string(),
            physical_observation,
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
                journal_id,
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
    fn cumulative_ack_becomes_durable_only_through_its_synced_wal() {
        let path = cumulative_ack_wal_path("durable");
        let (spool_root, local_record) = durable_spool_record("cumulative-durable-record");
        let mut wal = CumulativeAckWal::open(&path).unwrap();
        let durable = wal
            .commit_verified(verified_cumulative_ack(5), &local_record)
            .unwrap();

        assert_eq!(durable.ack().primary_term, 5);
        assert_eq!(durable.local_location(), local_record.location());
        assert_eq!(wal.highest_primary_term("solana-mainnet"), Some(5));
        let stream = durable.ack().stream.clone();
        assert_eq!(wal.latest_stream_ack(&stream), Some(durable.ack()));
        assert!(std::fs::metadata(&path).unwrap().len() > CUMULATIVE_ACK_WAL_HEADER_LEN);

        drop(wal);
        let recovered = CumulativeAckWal::open(&path).unwrap();
        assert_eq!(recovered.latest_stream_ack(&stream), Some(durable.ack()));
        drop(recovered);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn logical_ack_is_bound_to_the_full_physical_generation_witness() {
        let path = cumulative_ack_wal_path("physical-generation-binding");
        let lock_path = cumulative_ack_lock_path(&path);
        let (first_root, first_record) =
            durable_spool_record_with_journal("physical-generation-a", [8; 16]);
        let (second_root, second_record) =
            durable_spool_record_with_journal("physical-generation-b", [9; 16]);
        assert_eq!(first_record.location(), second_record.location());
        assert_eq!(
            first_record.metadata().content_digest,
            second_record.metadata().content_digest
        );

        let stream = replication_stream();
        let first_witness = DurableReplicationWitness::from_verified_mapping(
            &first_record,
            stream.clone(),
            observation().sequence,
            first_record.metadata().content_digest,
        )
        .unwrap();
        let second_witness = DurableReplicationWitness::from_verified_mapping(
            &second_record,
            stream,
            observation().sequence,
            second_record.metadata().content_digest,
        )
        .unwrap();
        let mut wal = CumulativeAckWal::open(&path).unwrap();
        let durable = wal
            .commit_verified_replication(verified_cumulative_ack(5), &first_witness)
            .unwrap();
        assert!(durable.matches_replication_witness(&first_witness));
        assert!(!durable.matches_replication_witness(&second_witness));
        assert!(
            wal.commit_verified_replication(verified_cumulative_ack(5), &second_witness)
                .unwrap_err()
                .to_string()
                .contains("different local spool frame")
        );

        drop(wal);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(lock_path).unwrap();
        std::fs::remove_dir_all(first_root).unwrap();
        std::fs::remove_dir_all(second_root).unwrap();
    }

    #[test]
    fn cumulative_ack_wal_compacts_to_latest_stream_state_without_losing_its_fence() {
        let path = cumulative_ack_wal_path("compaction");
        let lock_path = cumulative_ack_lock_path(&path);
        let (spool_root, local_record) = durable_spool_record("cumulative-compaction-record");
        let expected = {
            let mut wal = CumulativeAckWal::open(&path).unwrap();
            let durable = wal
                .commit_verified(verified_cumulative_ack(5), &local_record)
                .unwrap();
            let expected = durable.ack().clone();
            wal.writer.flush().unwrap();
            let compact_at = wal
                .compacted_baseline_bytes
                .checked_add(CUMULATIVE_ACK_COMPACT_GROWTH_BYTES)
                .unwrap();
            wal.writer.get_mut().set_len(compact_at).unwrap();
            wal.compact_if_needed().unwrap();
            let compacted_len = std::fs::metadata(&path).unwrap().len();
            assert_eq!(wal.compacted_baseline_bytes, compacted_len);
            assert!(compacted_len < compact_at);

            // A snapshot can itself grow beyond 64 MiB. Only bytes appended beyond that
            // baseline may trigger another rewrite.
            let below_next_growth = compacted_len
                .checked_add(CUMULATIVE_ACK_COMPACT_GROWTH_BYTES - 1)
                .unwrap();
            wal.writer.get_mut().set_len(below_next_growth).unwrap();
            wal.compact_if_needed().unwrap();
            assert_eq!(std::fs::metadata(&path).unwrap().len(), below_next_growth);
            wal.writer
                .get_mut()
                .set_len(
                    compacted_len
                        .checked_add(CUMULATIVE_ACK_COMPACT_GROWTH_BYTES)
                        .unwrap(),
                )
                .unwrap();
            wal.compact_if_needed().unwrap();
            assert_eq!(std::fs::metadata(&path).unwrap().len(), compacted_len);
            expected
        };

        let recovered = CumulativeAckWal::open(&path).unwrap();
        assert_eq!(recovered.highest_primary_term("solana-mainnet"), Some(5));
        assert_eq!(
            recovered.latest_stream_ack(&expected.stream),
            Some(&expected)
        );
        drop(recovered);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(lock_path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn cumulative_ack_wal_removes_one_fixed_orphaned_compaction_on_open() {
        let path = cumulative_ack_wal_path("orphaned-compaction");
        let lock_path = cumulative_ack_lock_path(&path);
        let temporary = cumulative_ack_compaction_path(&path).unwrap();
        {
            let wal = CumulativeAckWal::open(&path).unwrap();
            drop(wal);
        }
        {
            let mut orphan = open_receipt_descriptor(&temporary, true).unwrap();
            orphan.write_all(b"incomplete compacted WAL").unwrap();
            orphan.sync_data().unwrap();
        }
        assert!(temporary.exists());

        let wal = CumulativeAckWal::open(&path).unwrap();
        assert!(!temporary.exists());

        drop(wal);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(lock_path).unwrap();
    }

    #[test]
    fn cumulative_ack_wal_rejects_primary_term_rollback_after_restart() {
        let path = cumulative_ack_wal_path("term-rollback");
        let (spool_root, local_record) = durable_spool_record("term-rollback-record");
        {
            let mut wal = CumulativeAckWal::open(&path).unwrap();
            wal.commit_verified(verified_cumulative_ack(5), &local_record)
                .unwrap();
        }
        let durable_len = std::fs::metadata(&path).unwrap().len();
        let mut wal = CumulativeAckWal::open(&path).unwrap();
        assert_eq!(wal.highest_primary_term("solana-mainnet"), Some(5));
        let error = wal
            .commit_verified(verified_cumulative_ack(4), &local_record)
            .unwrap_err();
        assert!(error.to_string().contains("primary term rollback"));
        assert!(!wal.is_poisoned());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), durable_len);

        drop(wal);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn cumulative_ack_wal_durably_fences_a_new_term_on_the_same_prefix() {
        let path = cumulative_ack_wal_path("same-prefix-term-fence");
        let (spool_root, local_record) = durable_spool_record("same-prefix-term-record");
        let stream = {
            let mut wal = CumulativeAckWal::open(&path).unwrap();
            let first = wal
                .commit_verified(verified_cumulative_ack(5), &local_record)
                .unwrap();
            let stream = first.ack().stream.clone();
            let fenced = wal
                .commit_verified_existing_prefix(verified_cumulative_ack(6))
                .unwrap();
            assert_eq!(fenced.ack().through_sequence, first.ack().through_sequence);
            assert_eq!(fenced.local_location(), first.local_location());
            assert_eq!(wal.highest_primary_term("solana-mainnet"), Some(6));
            stream
        };

        let recovered = CumulativeAckWal::open(&path).unwrap();
        assert_eq!(recovered.highest_primary_term("solana-mainnet"), Some(6));
        assert_eq!(
            recovered.latest_stream_ack(&stream).unwrap().primary_term,
            6
        );
        drop(recovered);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn cumulative_ack_wal_fences_terms_across_primary_identity_changes() {
        let path = cumulative_ack_wal_path("cross-primary-term-fence");
        let (spool_root, local_record) = durable_spool_record("cross-primary-term-record");
        {
            let mut standby = cumulative_ack(6);
            standby.primary_id = "nas-standby".to_owned();
            let expected = expected_cumulative_ack(&standby, 6);
            let verified =
                verify_cumulative_ack(standby.clone(), expected, &AcceptCumulativeSignature)
                    .unwrap();
            let mut wal = CumulativeAckWal::open(&path).unwrap();
            wal.commit_verified(verified, &local_record).unwrap();
            assert_eq!(wal.highest_primary_term("solana-mainnet"), Some(6));
        }

        let old_primary = cumulative_ack(5);
        let expected = expected_cumulative_ack(&old_primary, 5);
        let verified =
            verify_cumulative_ack(old_primary.clone(), expected, &AcceptCumulativeSignature)
                .unwrap();
        let mut wal = CumulativeAckWal::open(&path).unwrap();
        let error = wal.commit_verified(verified, &local_record).unwrap_err();
        assert!(error.to_string().contains("primary term rollback"));

        drop(wal);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn cumulative_ack_wal_rejects_same_sequence_with_a_different_chain() {
        let path = cumulative_ack_wal_path("chain-reuse");
        let (spool_root, local_record) = durable_spool_record("chain-reuse-record");
        let mut wal = CumulativeAckWal::open(&path).unwrap();
        wal.commit_verified(verified_cumulative_ack(5), &local_record)
            .unwrap();
        let durable_len = std::fs::metadata(&path).unwrap().len();

        let mut changed = cumulative_ack(5);
        changed.rolling_chain_digest = ContentDigest([12; 32]);
        let expected = expected_cumulative_ack(&changed, 5);
        let verified =
            verify_cumulative_ack(changed.clone(), expected, &AcceptCumulativeSignature).unwrap();
        let error = wal.commit_verified(verified, &local_record).unwrap_err();
        assert!(error.to_string().contains("different digest chain"));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), durable_len);

        drop(wal);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn cumulative_ack_wal_truncates_partial_crash_tail_and_retains_term_fence() {
        let path = cumulative_ack_wal_path("crash-tail");
        let (spool_root, local_record) = durable_spool_record("crash-tail-record");
        let durable_len = {
            let mut wal = CumulativeAckWal::open(&path).unwrap();
            wal.commit_verified(verified_cumulative_ack(8), &local_record)
                .unwrap();
            std::fs::metadata(&path).unwrap().len()
        };
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(CUMULATIVE_ACK_FRAME_MAGIC).unwrap();
        file.write_all(&CUMULATIVE_ACK_FRAME_VERSION.to_le_bytes()[..1])
            .unwrap();
        file.sync_data().unwrap();
        drop(file);

        let mut wal = CumulativeAckWal::open(&path).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), durable_len);
        assert_eq!(wal.highest_primary_term("solana-mainnet"), Some(8));
        assert!(
            wal.commit_verified(verified_cumulative_ack(7), &local_record)
                .unwrap_err()
                .to_string()
                .contains("primary term rollback")
        );

        drop(wal);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(spool_root).unwrap();
    }

    #[test]
    fn committed_cumulative_ack_corruption_fails_closed_without_truncation() {
        let path = cumulative_ack_wal_path("corrupt");
        let (spool_root, local_record) = durable_spool_record("cumulative-corrupt-record");
        {
            let mut wal = CumulativeAckWal::open(&path).unwrap();
            wal.commit_verified(verified_cumulative_ack(5), &local_record)
                .unwrap();
        }
        let original_len = std::fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(std::io::SeekFrom::Start(CUMULATIVE_ACK_WAL_HEADER_LEN))
            .unwrap();
        file.write_all(b"X").unwrap();
        file.sync_data().unwrap();
        drop(file);

        assert!(CumulativeAckWal::open(&path).is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), original_len);
        std::fs::remove_file(path).unwrap();
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
