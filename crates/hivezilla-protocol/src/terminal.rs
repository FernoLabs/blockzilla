use std::collections::BTreeSet;
use std::fmt::Write as _;

use sha2::{Digest, Sha256};

use crate::{
    CURSOR_V1_ENCODED_LEN, CursorV1, DeletionAuthorizingStoreId, DurabilityPolicyId,
    DurabilityTargetId, FRAME_V1_FIXED_ENCODED_LEN, FRAME_V1_MAGIC, FailureDomainId, FrameV1,
    MAX_OBJECT_VERSION_BYTES, ProtocolError, Result, StreamId, StreamManifestSha256,
    StreamManifestV1,
};

pub const TERMINAL_RAW_HEADER_V1_MAGIC: [u8; 8] = *b"HIVERAW1";
pub const TERMINAL_RAW_FOOTER_V1_MAGIC: [u8; 9] = *b"HIVEREND1";
pub const TERMINAL_RAW_FOOTER_V1_ENCODED_LEN: usize =
    TERMINAL_RAW_FOOTER_V1_MAGIC.len() + CURSOR_V1_ENCODED_LEN;

// The smallest stored manifest has a one-byte descriptor and five absent
// option tags. Custody-bearing manifests are larger, but this exact lower bound
// lets hostile receipt lengths fail before any object I/O.
const MIN_STREAM_MANIFEST_V1_ENCODED_LEN: usize =
    2 + crate::STREAM_HEADER_V1_ENCODED_LEN + 8 + 1 + 5;
pub const MIN_TERMINAL_RAW_OBJECT_V1_ENCODED_LEN: usize = TERMINAL_RAW_HEADER_V1_MAGIC.len()
    + MIN_STREAM_MANIFEST_V1_ENCODED_LEN
    + CURSOR_V1_ENCODED_LEN
    + FRAME_V1_FIXED_ENCODED_LEN
    + TERMINAL_RAW_FOOTER_V1_ENCODED_LEN;

pub const MAX_TERMINAL_OBJECT_KEY_BYTES: u64 = 4_096;
pub const MAX_TERMINAL_OBJECT_VERSION_BYTES: u64 = MAX_OBJECT_VERSION_BYTES;
pub const TERMINAL_RAW_OBJECT_KEY_V1_LEN: usize = 148;

pub const TERMINAL_COPY_RECEIPT_V1_FIXED_ENCODED_LEN: usize =
    5 * 16 + 2 * CURSOR_V1_ENCODED_LEN + 8 + 1 + 8 + 32 + 1;
pub const MAX_TERMINAL_COPY_RECEIPT_V1_ENCODED_LEN: usize =
    TERMINAL_COPY_RECEIPT_V1_FIXED_ENCODED_LEN
        + MAX_TERMINAL_OBJECT_KEY_BYTES as usize
        + 8
        + MAX_TERMINAL_OBJECT_VERSION_BYTES as usize;
pub const TERMINAL_RANGE_INDEX_V1_FIXED_ENCODED_LEN: usize =
    StreamId::LENGTH + 2 * CURSOR_V1_ENCODED_LEN + 8 + 32 + 4;
pub const MAX_TERMINAL_RANGE_INDEX_COPIES_V1: usize = crate::DURABILITY_POLICY_MAX_TARGETS;
pub const MAX_TERMINAL_RANGE_INDEX_V1_ENCODED_LEN: usize = TERMINAL_RANGE_INDEX_V1_FIXED_ENCODED_LEN
    + MAX_TERMINAL_RANGE_INDEX_COPIES_V1 * MAX_TERMINAL_COPY_RECEIPT_V1_ENCODED_LEN;
pub const TERMINAL_CURSOR_CHECKPOINT_V1_ENCODED_LEN: usize = DeletionAuthorizingStoreId::LENGTH
    + StreamId::LENGTH
    + StreamManifestSha256::LENGTH
    + DurabilityPolicyId::LENGTH
    + CURSOR_V1_ENCODED_LEN;

const TERMINAL_RAW_KEY_PREFIX: &str = "hive-raw/v1/";

/// Mandatory finite read-side resource limits for decoding an externally
/// supplied terminal raw object. These are parser budgets, not object identity
/// or producer objectization settings. A runtime must configure this byte limit
/// at least as high as the producer's object cap; that object cap must itself
/// accommodate one maximum admitted record plus framing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalRawObjectDecodeLimitsV1 {
    max_encoded_bytes: u64,
    max_records: u64,
}

impl TerminalRawObjectDecodeLimitsV1 {
    pub fn new(max_encoded_bytes: u64, max_records: u64) -> Result<Self> {
        let minimum_bytes = MIN_TERMINAL_RAW_OBJECT_V1_ENCODED_LEN as u64;
        if max_encoded_bytes < minimum_bytes {
            return Err(ProtocolError::InvalidTerminalRawObjectLimit {
                field: "max_encoded_bytes",
                actual: max_encoded_bytes,
                minimum: minimum_bytes,
            });
        }
        if max_records == 0 {
            return Err(ProtocolError::InvalidTerminalRawObjectLimit {
                field: "max_records",
                actual: max_records,
                minimum: 1,
            });
        }
        Ok(Self {
            max_encoded_bytes,
            max_records,
        })
    }

    #[must_use]
    pub const fn max_encoded_bytes(self) -> u64 {
        self.max_encoded_bytes
    }

    #[must_use]
    pub const fn max_records(self) -> u64 {
        self.max_records
    }

    fn validate_encoded_len(self, actual: usize) -> Result<u64> {
        let actual = u64::try_from(actual).map_err(|_| ProtocolError::IntegerOverflow {
            field: "terminal_raw_object_encoded_len",
        })?;
        if actual > self.max_encoded_bytes {
            return Err(ProtocolError::TerminalRawObjectTooLarge {
                actual,
                max: self.max_encoded_bytes,
            });
        }
        Ok(actual)
    }
}

/// Self-delimiting prefix of one permanent raw object. The manifest is stored
/// directly after the magic without a second length or digest wrapper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRawHeaderV1 {
    manifest: StreamManifestV1,
    start: CursorV1,
}

impl TerminalRawHeaderV1 {
    pub fn new(manifest: StreamManifestV1, start: CursorV1) -> Result<Self> {
        validate_initial_cursor_if_zero(start, &manifest)?;
        Ok(Self { manifest, start })
    }

    #[must_use]
    pub const fn manifest(&self) -> &StreamManifestV1 {
        &self.manifest
    }

    #[must_use]
    pub const fn start(&self) -> CursorV1 {
        self.start
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        TERMINAL_RAW_HEADER_V1_MAGIC.len() + self.manifest.encode().len() + CURSOR_V1_ENCODED_LEN
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.encoded_len());
        encoded.extend_from_slice(&TERMINAL_RAW_HEADER_V1_MAGIC);
        encoded.extend_from_slice(&self.manifest.encode());
        encoded.extend_from_slice(&self.start.fixed_encode());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let (header, consumed) = Self::decode_prefix(encoded)?;
        if consumed != encoded.len() {
            return Err(ProtocolError::TrailingBytes {
                context: "TerminalRawHeaderV1",
                count: encoded.len() - consumed,
            });
        }
        Ok(header)
    }

    fn decode_prefix(encoded: &[u8]) -> Result<(Self, usize)> {
        require_at_least(
            "TerminalRawHeaderV1.magic",
            encoded.len(),
            TERMINAL_RAW_HEADER_V1_MAGIC.len(),
        )?;
        require_magic(
            "TerminalRawHeaderV1",
            &encoded[..TERMINAL_RAW_HEADER_V1_MAGIC.len()],
            &TERMINAL_RAW_HEADER_V1_MAGIC,
        )?;
        let manifest_offset = TERMINAL_RAW_HEADER_V1_MAGIC.len();
        let (manifest, manifest_len) =
            StreamManifestV1::decode_prefix(&encoded[manifest_offset..])?;
        let cursor_offset =
            manifest_offset
                .checked_add(manifest_len)
                .ok_or(ProtocolError::IntegerOverflow {
                    field: "terminal_raw_header_manifest_len",
                })?;
        let cursor_end = cursor_offset.checked_add(CURSOR_V1_ENCODED_LEN).ok_or(
            ProtocolError::IntegerOverflow {
                field: "terminal_raw_header_cursor",
            },
        )?;
        require_at_least("TerminalRawHeaderV1.start", encoded.len(), cursor_end)?;
        let start = CursorV1::decode(&encoded[cursor_offset..cursor_end])?;
        Ok((Self::new(manifest, start)?, cursor_end))
    }
}

/// Fixed trailer of one permanent raw object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalRawFooterV1 {
    end: CursorV1,
}

impl TerminalRawFooterV1 {
    #[must_use]
    pub const fn new(end: CursorV1) -> Self {
        Self { end }
    }

    #[must_use]
    pub const fn end(&self) -> CursorV1 {
        self.end
    }

    #[must_use]
    pub fn encode(&self) -> [u8; TERMINAL_RAW_FOOTER_V1_ENCODED_LEN] {
        let mut encoded = [0; TERMINAL_RAW_FOOTER_V1_ENCODED_LEN];
        encoded[..TERMINAL_RAW_FOOTER_V1_MAGIC.len()]
            .copy_from_slice(&TERMINAL_RAW_FOOTER_V1_MAGIC);
        encoded[TERMINAL_RAW_FOOTER_V1_MAGIC.len()..].copy_from_slice(&self.end.fixed_encode());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_len(
            "TerminalRawFooterV1",
            encoded.len(),
            TERMINAL_RAW_FOOTER_V1_ENCODED_LEN,
        )?;
        require_magic(
            "TerminalRawFooterV1",
            &encoded[..TERMINAL_RAW_FOOTER_V1_MAGIC.len()],
            &TERMINAL_RAW_FOOTER_V1_MAGIC,
        )?;
        Ok(Self::new(CursorV1::decode(
            &encoded[TERMINAL_RAW_FOOTER_V1_MAGIC.len()..],
        )?))
    }
}

/// One complete, non-empty, chain-verified permanent raw range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRawObjectV1 {
    header: TerminalRawHeaderV1,
    frames: Vec<FrameV1>,
    footer: TerminalRawFooterV1,
}

impl TerminalRawObjectV1 {
    pub fn new(manifest: StreamManifestV1, start: CursorV1, frames: Vec<FrameV1>) -> Result<Self> {
        if frames.is_empty() {
            return Err(ProtocolError::InvalidTerminalRawObject {
                reason: "terminal raw ranges must contain at least one frame",
            });
        }
        let header = TerminalRawHeaderV1::new(manifest, start)?;
        let mut end = start;
        for frame in &frames {
            end = frame.validate_after(end)?;
        }
        Ok(Self {
            header,
            frames,
            footer: TerminalRawFooterV1::new(end),
        })
    }

    #[must_use]
    pub const fn header(&self) -> &TerminalRawHeaderV1 {
        &self.header
    }

    #[must_use]
    pub fn frames(&self) -> &[FrameV1] {
        &self.frames
    }

    #[must_use]
    pub const fn footer(&self) -> TerminalRawFooterV1 {
        self.footer
    }

    #[must_use]
    pub const fn manifest(&self) -> &StreamManifestV1 {
        self.header.manifest()
    }

    #[must_use]
    pub const fn start(&self) -> CursorV1 {
        self.header.start()
    }

    #[must_use]
    pub const fn end(&self) -> CursorV1 {
        self.footer.end()
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        self.header.encoded_len()
            + self.frames.iter().map(FrameV1::encoded_len).sum::<usize>()
            + TERMINAL_RAW_FOOTER_V1_ENCODED_LEN
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.encoded_len());
        encoded.extend_from_slice(&self.header.encode());
        for frame in &self.frames {
            encoded.extend_from_slice(&frame.encode());
        }
        encoded.extend_from_slice(&self.footer.encode());
        encoded
    }

    #[must_use]
    pub fn encoded_sha256(&self) -> [u8; 32] {
        Sha256::digest(self.encode()).into()
    }

    /// Deterministic immutable object key. Provider versions and receipts are
    /// deliberately absent from identity.
    #[must_use]
    pub fn object_key(&self) -> String {
        terminal_raw_object_key(
            self.manifest().stream().stream_id(),
            self.start(),
            self.end(),
            &self.encoded_sha256(),
        )
    }

    /// Decodes one complete object after enforcing caller-selected finite byte
    /// and record limits before payload-backed allocations can grow unchecked.
    pub fn decode(encoded: &[u8], limits: TerminalRawObjectDecodeLimitsV1) -> Result<Self> {
        limits.validate_encoded_len(encoded.len())?;
        let (header, mut offset) = TerminalRawHeaderV1::decode_prefix(encoded)?;
        let mut cursor = header.start();
        let mut frames = Vec::new();

        loop {
            let remaining = &encoded[offset..];
            if remaining.len() < TERMINAL_RAW_FOOTER_V1_MAGIC.len()
                && TERMINAL_RAW_FOOTER_V1_MAGIC.starts_with(remaining)
            {
                return Err(ProtocolError::Truncated {
                    context: "TerminalRawFooterV1.magic",
                    expected: TERMINAL_RAW_FOOTER_V1_MAGIC.len(),
                    actual: remaining.len(),
                });
            }
            require_at_least(
                "terminal raw frame/footer magic",
                remaining.len(),
                TERMINAL_RAW_FOOTER_V1_MAGIC.len().min(FRAME_V1_MAGIC.len()),
            )?;
            if remaining.starts_with(&TERMINAL_RAW_FOOTER_V1_MAGIC) {
                if frames.is_empty() {
                    return Err(ProtocolError::InvalidTerminalRawObject {
                        reason: "terminal raw ranges must contain at least one frame",
                    });
                }
                let footer = TerminalRawFooterV1::decode(remaining)?;
                if footer.end() != cursor {
                    return Err(ProtocolError::CursorMismatch {
                        context: "TerminalRawFooterV1.end",
                    });
                }
                return Ok(Self {
                    header,
                    frames,
                    footer,
                });
            }
            if !remaining.starts_with(&FRAME_V1_MAGIC) {
                return Err(ProtocolError::InvalidMagic {
                    context: "terminal raw frame/footer",
                });
            }
            let record_count =
                u64::try_from(frames.len()).map_err(|_| ProtocolError::IntegerOverflow {
                    field: "terminal_raw_object_record_count",
                })?;
            let next_record_count =
                record_count
                    .checked_add(1)
                    .ok_or(ProtocolError::IntegerOverflow {
                        field: "terminal_raw_object_record_count",
                    })?;
            if next_record_count > limits.max_records {
                return Err(ProtocolError::TerminalRawObjectRecordLimitExceeded {
                    actual: next_record_count,
                    max: limits.max_records,
                });
            }
            let (frame, end, consumed) = FrameV1::decode_prefix_after(remaining, cursor)?;
            frames.push(frame);
            cursor = end;
            offset = offset
                .checked_add(consumed)
                .ok_or(ProtocolError::IntegerOverflow {
                    field: "terminal_raw_object_offset",
                })?;
        }
    }
}

/// Exact V1 raw-object key derivation for a known object identity.
#[must_use]
pub fn terminal_raw_object_key(
    stream_id: StreamId,
    start: CursorV1,
    end: CursorV1,
    encoded_sha256: &[u8; 32],
) -> String {
    let mut key = String::with_capacity(TERMINAL_RAW_OBJECT_KEY_V1_LEN);
    key.push_str(TERMINAL_RAW_KEY_PREFIX);
    push_hex(&mut key, stream_id.as_bytes());
    key.push('/');
    write!(key, "{:016x}", start.next_sequence()).expect("writing to String cannot fail");
    key.push('-');
    write!(key, "{:016x}", end.next_sequence()).expect("writing to String cannot fail");
    key.push('-');
    push_hex(&mut key, encoded_sha256);
    key.push_str(".hraw");
    debug_assert_eq!(key.len(), TERMINAL_RAW_OBJECT_KEY_V1_LEN);
    key
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TerminalVerificationV1 {
    ProviderSha256 = 1,
    FullReadbackSha256 = 2,
}

impl TryFrom<u8> for TerminalVerificationV1 {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::ProviderSha256),
            2 => Ok(Self::FullReadbackSha256),
            value => Err(ProtocolError::UnknownTerminalVerification { value }),
        }
    }
}

/// Crash-durable evidence for one exact object copy on one immutable policy
/// target. Construction validates the locator shape, not provider I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalCopyReceiptV1 {
    terminal_store_id: DeletionAuthorizingStoreId,
    policy_id: DurabilityPolicyId,
    target_id: DurabilityTargetId,
    failure_domain_id: FailureDomainId,
    stream_id: StreamId,
    start: CursorV1,
    end: CursorV1,
    object_key: Vec<u8>,
    object_version: Option<Vec<u8>>,
    encoded_len: u64,
    encoded_sha256: [u8; 32],
    verification: TerminalVerificationV1,
}

impl TerminalCopyReceiptV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        terminal_store_id: DeletionAuthorizingStoreId,
        policy_id: DurabilityPolicyId,
        target_id: DurabilityTargetId,
        failure_domain_id: FailureDomainId,
        stream_id: StreamId,
        start: CursorV1,
        end: CursorV1,
        object_key: Vec<u8>,
        object_version: Option<Vec<u8>>,
        encoded_len: u64,
        encoded_sha256: [u8; 32],
        verification: TerminalVerificationV1,
    ) -> Result<Self> {
        validate_nonempty_range(start, end, "TerminalCopyReceiptV1")?;
        validate_terminal_locator(
            "terminal_object_key",
            &object_key,
            MAX_TERMINAL_OBJECT_KEY_BYTES,
        )?;
        if let Some(version) = object_version.as_deref() {
            validate_terminal_locator(
                "terminal_object_version",
                version,
                MAX_TERMINAL_OBJECT_VERSION_BYTES,
            )?;
        }
        if encoded_len < MIN_TERMINAL_RAW_OBJECT_V1_ENCODED_LEN as u64 {
            return Err(ProtocolError::InvalidTerminalCopyReceipt {
                reason: "encoded length is below the minimum terminal raw object length",
            });
        }
        let expected_key = terminal_raw_object_key(stream_id, start, end, &encoded_sha256);
        if object_key != expected_key.as_bytes() {
            return Err(ProtocolError::TerminalObjectKeyMismatch);
        }
        Ok(Self {
            terminal_store_id,
            policy_id,
            target_id,
            failure_domain_id,
            stream_id,
            start,
            end,
            object_key,
            object_version,
            encoded_len,
            encoded_sha256,
            verification,
        })
    }

    #[must_use]
    pub const fn terminal_store_id(&self) -> DeletionAuthorizingStoreId {
        self.terminal_store_id
    }

    #[must_use]
    pub const fn policy_id(&self) -> DurabilityPolicyId {
        self.policy_id
    }

    #[must_use]
    pub const fn target_id(&self) -> DurabilityTargetId {
        self.target_id
    }

    #[must_use]
    pub const fn failure_domain_id(&self) -> FailureDomainId {
        self.failure_domain_id
    }

    #[must_use]
    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    #[must_use]
    pub const fn start(&self) -> CursorV1 {
        self.start
    }

    #[must_use]
    pub const fn end(&self) -> CursorV1 {
        self.end
    }

    #[must_use]
    pub fn object_key(&self) -> &[u8] {
        &self.object_key
    }

    #[must_use]
    pub fn object_version(&self) -> Option<&[u8]> {
        self.object_version.as_deref()
    }

    #[must_use]
    pub const fn encoded_len(&self) -> u64 {
        self.encoded_len
    }

    #[must_use]
    pub const fn encoded_sha256(&self) -> &[u8; 32] {
        &self.encoded_sha256
    }

    #[must_use]
    pub const fn verification(&self) -> TerminalVerificationV1 {
        self.verification
    }

    #[must_use]
    pub fn canonical_encoded_len(&self) -> usize {
        TERMINAL_COPY_RECEIPT_V1_FIXED_ENCODED_LEN
            + self.object_key.len()
            + self
                .object_version
                .as_ref()
                .map_or(0, |value| 8 + value.len())
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.canonical_encoded_len());
        self.encode_into(&mut encoded);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let (receipt, consumed) = Self::decode_prefix(encoded)?;
        if consumed != encoded.len() {
            return Err(ProtocolError::TrailingBytes {
                context: "TerminalCopyReceiptV1",
                count: encoded.len() - consumed,
            });
        }
        Ok(receipt)
    }

    fn encode_into(&self, encoded: &mut Vec<u8>) {
        encoded.extend_from_slice(self.terminal_store_id.as_bytes());
        encoded.extend_from_slice(self.policy_id.as_bytes());
        encoded.extend_from_slice(self.target_id.as_bytes());
        encoded.extend_from_slice(self.failure_domain_id.as_bytes());
        encoded.extend_from_slice(self.stream_id.as_bytes());
        encoded.extend_from_slice(&self.start.fixed_encode());
        encoded.extend_from_slice(&self.end.fixed_encode());
        encode_bytes(encoded, &self.object_key);
        encode_optional_bytes(encoded, self.object_version.as_deref());
        encoded.extend_from_slice(&self.encoded_len.to_be_bytes());
        encoded.extend_from_slice(&self.encoded_sha256);
        encoded.push(self.verification as u8);
    }

    fn decode_prefix(encoded: &[u8]) -> Result<(Self, usize)> {
        let mut reader = Reader::new(encoded);
        let terminal_store_id = DeletionAuthorizingStoreId::try_from(
            reader.take(DeletionAuthorizingStoreId::LENGTH, "terminal_copy_store_id")?,
        )?;
        let policy_id = DurabilityPolicyId::try_from(
            reader.take(DurabilityPolicyId::LENGTH, "terminal_copy_policy_id")?,
        )?;
        let target_id = DurabilityTargetId::try_from(
            reader.take(DurabilityTargetId::LENGTH, "terminal_copy_target_id")?,
        )?;
        let failure_domain_id = FailureDomainId::try_from(
            reader.take(FailureDomainId::LENGTH, "terminal_copy_failure_domain_id")?,
        )?;
        let stream_id =
            StreamId::try_from(reader.take(StreamId::LENGTH, "terminal_copy_stream_id")?)?;
        let start = CursorV1::decode(reader.take(CURSOR_V1_ENCODED_LEN, "terminal_copy_start")?)?;
        let end = CursorV1::decode(reader.take(CURSOR_V1_ENCODED_LEN, "terminal_copy_end")?)?;
        let object_key = reader.locator("terminal_object_key", MAX_TERMINAL_OBJECT_KEY_BYTES)?;
        let object_version = match reader.u8("terminal_object_version")? {
            0 => None,
            1 => {
                Some(reader.locator("terminal_object_version", MAX_TERMINAL_OBJECT_VERSION_BYTES)?)
            }
            value => {
                return Err(ProtocolError::InvalidOptionTag {
                    field: "terminal_object_version",
                    value,
                });
            }
        };
        let encoded_len = reader.u64("terminal_copy_encoded_len")?;
        let encoded_sha256: [u8; 32] = reader
            .take(32, "terminal_copy_encoded_sha256")?
            .try_into()
            .expect("fixed slice");
        let verification = TerminalVerificationV1::try_from(reader.u8("terminal_verification")?)?;
        let consumed = reader.offset();
        let decoded = Self::new(
            terminal_store_id,
            policy_id,
            target_id,
            failure_domain_id,
            stream_id,
            start,
            end,
            object_key,
            object_version,
            encoded_len,
            encoded_sha256,
            verification,
        )?;
        if decoded.encode() != encoded[..consumed] {
            return Err(ProtocolError::NonCanonicalOrder {
                context: "TerminalCopyReceiptV1",
            });
        }
        Ok((decoded, consumed))
    }
}

/// Durable catalog record for one exact range and all committed policy-copy
/// receipts currently attached to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRangeIndexV1 {
    stream_id: StreamId,
    start: CursorV1,
    end: CursorV1,
    encoded_len: u64,
    encoded_sha256: [u8; 32],
    copies: Vec<TerminalCopyReceiptV1>,
}

impl TerminalRangeIndexV1 {
    pub fn new(
        stream_id: StreamId,
        start: CursorV1,
        end: CursorV1,
        encoded_len: u64,
        encoded_sha256: [u8; 32],
        mut copies: Vec<TerminalCopyReceiptV1>,
    ) -> Result<Self> {
        validate_nonempty_range(start, end, "TerminalRangeIndexV1")?;
        validate_copy_count(copies.len())?;
        copies.sort_by_key(TerminalCopyReceiptV1::target_id);
        if copies
            .windows(2)
            .any(|pair| pair[0].target_id() >= pair[1].target_id())
        {
            return Err(ProtocolError::InvalidTerminalRangeIndex {
                reason: "copy target IDs must be unique",
            });
        }
        let index = Self {
            stream_id,
            start,
            end,
            encoded_len,
            encoded_sha256,
            copies,
        };
        index.validate_copy_identity()?;
        Ok(index)
    }

    #[must_use]
    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    #[must_use]
    pub const fn start(&self) -> CursorV1 {
        self.start
    }

    #[must_use]
    pub const fn end(&self) -> CursorV1 {
        self.end
    }

    #[must_use]
    pub const fn encoded_len(&self) -> u64 {
        self.encoded_len
    }

    #[must_use]
    pub const fn encoded_sha256(&self) -> &[u8; 32] {
        &self.encoded_sha256
    }

    #[must_use]
    pub fn copies(&self) -> &[TerminalCopyReceiptV1] {
        &self.copies
    }

    #[must_use]
    pub fn canonical_encoded_len(&self) -> usize {
        TERMINAL_RANGE_INDEX_V1_FIXED_ENCODED_LEN
            + self
                .copies
                .iter()
                .map(TerminalCopyReceiptV1::canonical_encoded_len)
                .sum::<usize>()
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.canonical_encoded_len());
        encoded.extend_from_slice(self.stream_id.as_bytes());
        encoded.extend_from_slice(&self.start.fixed_encode());
        encoded.extend_from_slice(&self.end.fixed_encode());
        encoded.extend_from_slice(&self.encoded_len.to_be_bytes());
        encoded.extend_from_slice(&self.encoded_sha256);
        encoded.extend_from_slice(&(self.copies.len() as u32).to_be_bytes());
        for receipt in &self.copies {
            receipt.encode_into(&mut encoded);
        }
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() > MAX_TERMINAL_RANGE_INDEX_V1_ENCODED_LEN {
            return Err(ProtocolError::TerminalRangeIndexTooLarge {
                actual: encoded.len() as u64,
                max: MAX_TERMINAL_RANGE_INDEX_V1_ENCODED_LEN as u64,
            });
        }
        let mut reader = Reader::new(encoded);
        let stream_id =
            StreamId::try_from(reader.take(StreamId::LENGTH, "terminal_index_stream_id")?)?;
        let start = CursorV1::decode(reader.take(CURSOR_V1_ENCODED_LEN, "terminal_index_start")?)?;
        let end = CursorV1::decode(reader.take(CURSOR_V1_ENCODED_LEN, "terminal_index_end")?)?;
        let encoded_len = reader.u64("terminal_index_encoded_len")?;
        let encoded_sha256: [u8; 32] = reader
            .take(32, "terminal_index_encoded_sha256")?
            .try_into()
            .expect("fixed slice");
        let copy_count = reader.u32("terminal_index_copy_count")? as usize;
        validate_copy_count(copy_count)?;
        let mut copies = Vec::with_capacity(copy_count);
        for _ in 0..copy_count {
            let (copy, consumed) = TerminalCopyReceiptV1::decode_prefix(reader.remaining())?;
            reader.advance(consumed, "terminal_copy_receipt")?;
            copies.push(copy);
        }
        reader.finish("TerminalRangeIndexV1")?;
        if copies
            .windows(2)
            .any(|pair| pair[0].target_id() >= pair[1].target_id())
        {
            return Err(ProtocolError::NonCanonicalOrder {
                context: "TerminalRangeIndexV1.copies",
            });
        }
        let decoded = Self::new(stream_id, start, end, encoded_len, encoded_sha256, copies)?;
        if decoded.encode() != encoded {
            return Err(ProtocolError::NonCanonicalOrder {
                context: "TerminalRangeIndexV1",
            });
        }
        Ok(decoded)
    }

    /// Proves all canonical bindings and invokes `verify_copy` for every
    /// receipt before returning an externally-verified range token. The
    /// callback must independently stat/read the configured target
    /// and verify the receipt's exact key, optional version, length, digest,
    /// and declared verification method. Returning `Ok(())` without that I/O
    /// is not sufficient evidence for an ACK.
    ///
    /// # Trust boundary
    ///
    /// This dependency-light crate performs no storage I/O and cannot prove
    /// that the callback did so. An unconditional `Ok(())` callback produces
    /// only structurally consistent metadata and **must never** be used by a
    /// runtime to authorize an ACK. The runtime must bind this call to its
    /// provider attestation or full-readback implementation and durably commit
    /// the receipts/index before checkpoint construction.
    pub fn validate_for_protection_with<F>(
        &self,
        manifest: &StreamManifestV1,
        exact_object: &[u8],
        decode_limits: TerminalRawObjectDecodeLimitsV1,
        mut verify_copy: F,
    ) -> Result<ExternallyVerifiedTerminalRangeV1>
    where
        F: FnMut(&TerminalCopyReceiptV1) -> Result<()>,
    {
        let actual_len = decode_limits.validate_encoded_len(exact_object.len())?;
        if self.encoded_len != actual_len {
            return Err(ProtocolError::EncodedLengthMismatch {
                expected: self.encoded_len,
                actual: actual_len,
            });
        }
        let actual_sha256: [u8; 32] = Sha256::digest(exact_object).into();
        if self.encoded_sha256 != actual_sha256 {
            return Err(ProtocolError::EncodedSha256Mismatch);
        }

        let object = TerminalRawObjectV1::decode(exact_object, decode_limits)?;
        if object.manifest() != manifest {
            return Err(ProtocolError::InvalidTerminalRangeIndex {
                reason: "raw object embedded manifest differs from the configured manifest",
            });
        }
        if self.stream_id != manifest.stream().stream_id()
            || self.stream_id != object.manifest().stream().stream_id()
        {
            return Err(ProtocolError::StreamMismatch {
                context: "TerminalRangeIndexV1.object",
            });
        }
        if self.start != object.start() || self.end != object.end() {
            return Err(ProtocolError::CursorMismatch {
                context: "TerminalRangeIndexV1.object",
            });
        }
        let terminal_store_id = manifest.deletion_authorizing_store_id().ok_or(
            ProtocolError::InvalidTerminalRangeIndex {
                reason: "manifest has no deletion-authorizing terminal store",
            },
        )?;
        let policy =
            manifest
                .durability_policy()
                .ok_or(ProtocolError::InvalidTerminalRangeIndex {
                    reason: "manifest has no terminal durability policy",
                })?;
        let mut verified_domains = BTreeSet::new();
        for receipt in &self.copies {
            if receipt.terminal_store_id != terminal_store_id
                || receipt.policy_id != policy.policy_id()
            {
                return Err(ProtocolError::InvalidTerminalCopyReceipt {
                    reason: "store or policy ID differs from the immutable manifest",
                });
            }
            let target = policy
                .targets()
                .binary_search_by_key(&receipt.target_id, |target| target.target_id)
                .ok()
                .map(|index| policy.targets()[index])
                .ok_or(ProtocolError::InvalidTerminalCopyReceipt {
                    reason: "target ID is absent from the immutable policy",
                })?;
            if receipt.failure_domain_id != target.failure_domain_id {
                return Err(ProtocolError::InvalidTerminalCopyReceipt {
                    reason: "receipt failure domain differs from the policy target",
                });
            }
            verified_domains.insert(target.failure_domain_id);
        }
        let required = usize::from(policy.minimum_independent_copies());
        if verified_domains.len() < required {
            return Err(ProtocolError::TerminalDurabilityDeficit {
                actual: verified_domains.len() as u64,
                required: required as u64,
            });
        }
        for receipt in &self.copies {
            verify_copy(receipt)?;
        }
        Ok(ExternallyVerifiedTerminalRangeV1 {
            terminal_store_id,
            stream_id: self.stream_id,
            stream_manifest_sha256: manifest.stream().stream_manifest_sha256(),
            policy_id: policy.policy_id(),
            start: self.start,
            end: self.end,
            encoded_len: self.encoded_len,
            encoded_sha256: self.encoded_sha256,
        })
    }

    fn validate_copy_identity(&self) -> Result<()> {
        for copy in &self.copies {
            if copy.stream_id != self.stream_id
                || copy.start != self.start
                || copy.end != self.end
                || copy.encoded_len != self.encoded_len
                || copy.encoded_sha256 != self.encoded_sha256
            {
                return Err(ProtocolError::InvalidTerminalCopyReceipt {
                    reason: "copy object identity differs from its range index",
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalRangeIndexInsertDispositionV1 {
    Insert,
    ReuseExisting,
}

/// Enforces the non-overlap/idempotence rule before a catalog mutation.
pub fn validate_terminal_range_index_insert(
    existing: &[TerminalRangeIndexV1],
    candidate: &TerminalRangeIndexV1,
) -> Result<TerminalRangeIndexInsertDispositionV1> {
    for current in existing {
        if current.stream_id != candidate.stream_id {
            return Err(ProtocolError::StreamMismatch {
                context: "TerminalRangeIndexV1.catalog",
            });
        }
        if same_object_identity(current, candidate) {
            return Ok(TerminalRangeIndexInsertDispositionV1::ReuseExisting);
        }
        validate_disjoint_ranges(current.start, current.end, candidate.start, candidate.end)?;
    }
    Ok(TerminalRangeIndexInsertDispositionV1::Insert)
}

/// Exact range bindings returned after the trusted runtime reports completion
/// of physical-copy verification. This is evidence passed across the protocol
/// layer, not a capability and not independent proof that storage I/O occurred.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternallyVerifiedTerminalRangeV1 {
    terminal_store_id: DeletionAuthorizingStoreId,
    stream_id: StreamId,
    stream_manifest_sha256: StreamManifestSha256,
    policy_id: DurabilityPolicyId,
    start: CursorV1,
    end: CursorV1,
    encoded_len: u64,
    encoded_sha256: [u8; 32],
}

impl ExternallyVerifiedTerminalRangeV1 {
    #[must_use]
    pub const fn start(&self) -> CursorV1 {
        self.start
    }

    #[must_use]
    pub const fn end(&self) -> CursorV1 {
        self.end
    }
}

/// Derived, crash-durable terminal protected-prefix checkpoint.
///
/// This protocol value does not independently prove that physical target I/O
/// occurred and cannot authorize an ACK by itself. A trusted runtime may
/// persist/use it only after real provider attestation or full readback,
/// successful `validate_for_protection_with` calls, and the required durable
/// receipt/index transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalCursorCheckpointV1 {
    terminal_store_id: DeletionAuthorizingStoreId,
    stream_id: StreamId,
    stream_manifest_sha256: StreamManifestSha256,
    policy_id: DurabilityPolicyId,
    protected_through: CursorV1,
}

impl TerminalCursorCheckpointV1 {
    /// Recomputes the largest gap-free protected prefix. Later protected
    /// objects are retained but cannot jump a hole.
    pub fn from_verified_ranges(
        manifest: &StreamManifestV1,
        ranges: &[ExternallyVerifiedTerminalRangeV1],
    ) -> Result<Self> {
        let terminal_store_id = manifest.deletion_authorizing_store_id().ok_or(
            ProtocolError::InvalidTerminalCheckpoint {
                reason: "manifest has no deletion-authorizing terminal store",
            },
        )?;
        let policy_id = manifest
            .durability_policy()
            .map(|policy| policy.policy_id())
            .ok_or(ProtocolError::InvalidTerminalCheckpoint {
                reason: "manifest has no terminal durability policy",
            })?;
        let protected_through = largest_protected_cursor(manifest, ranges)?;
        Ok(Self {
            terminal_store_id,
            stream_id: manifest.stream().stream_id(),
            stream_manifest_sha256: manifest.stream().stream_manifest_sha256(),
            policy_id,
            protected_through,
        })
    }

    #[must_use]
    pub const fn terminal_store_id(&self) -> DeletionAuthorizingStoreId {
        self.terminal_store_id
    }

    #[must_use]
    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    #[must_use]
    pub const fn stream_manifest_sha256(&self) -> StreamManifestSha256 {
        self.stream_manifest_sha256
    }

    #[must_use]
    pub const fn policy_id(&self) -> DurabilityPolicyId {
        self.policy_id
    }

    #[must_use]
    pub const fn protected_through(&self) -> CursorV1 {
        self.protected_through
    }

    #[must_use]
    pub fn fixed_encode(&self) -> [u8; TERMINAL_CURSOR_CHECKPOINT_V1_ENCODED_LEN] {
        let mut encoded = [0; TERMINAL_CURSOR_CHECKPOINT_V1_ENCODED_LEN];
        let mut offset = 0;
        copy_field(&mut encoded, &mut offset, self.terminal_store_id.as_bytes());
        copy_field(&mut encoded, &mut offset, self.stream_id.as_bytes());
        copy_field(
            &mut encoded,
            &mut offset,
            self.stream_manifest_sha256.as_bytes(),
        );
        copy_field(&mut encoded, &mut offset, self.policy_id.as_bytes());
        copy_field(
            &mut encoded,
            &mut offset,
            &self.protected_through.fixed_encode(),
        );
        debug_assert_eq!(offset, TERMINAL_CURSOR_CHECKPOINT_V1_ENCODED_LEN);
        encoded
    }

    /// Decodes fixed bytes and validates immutable manifest bindings. The
    /// caller must additionally call `validate_against_verified_ranges`
    /// before treating this derived checkpoint as ACK-authorizing state.
    pub fn decode(encoded: &[u8], manifest: &StreamManifestV1) -> Result<Self> {
        require_exact_len(
            "TerminalCursorCheckpointV1",
            encoded.len(),
            TERMINAL_CURSOR_CHECKPOINT_V1_ENCODED_LEN,
        )?;
        let mut reader = Reader::new(encoded);
        let checkpoint = Self {
            terminal_store_id: DeletionAuthorizingStoreId::try_from(reader.take(
                DeletionAuthorizingStoreId::LENGTH,
                "terminal_checkpoint_store_id",
            )?)?,
            stream_id: StreamId::try_from(
                reader.take(StreamId::LENGTH, "terminal_checkpoint_stream_id")?,
            )?,
            stream_manifest_sha256: StreamManifestSha256::try_from(reader.take(
                StreamManifestSha256::LENGTH,
                "terminal_checkpoint_manifest_sha256",
            )?)?,
            policy_id: DurabilityPolicyId::try_from(
                reader.take(DurabilityPolicyId::LENGTH, "terminal_checkpoint_policy_id")?,
            )?,
            protected_through: CursorV1::decode(reader.take(
                CURSOR_V1_ENCODED_LEN,
                "terminal_checkpoint_protected_through",
            )?)?,
        };
        reader.finish("TerminalCursorCheckpointV1")?;
        checkpoint.validate_manifest_bindings(manifest)?;
        Ok(checkpoint)
    }

    pub fn validate_against_verified_ranges(
        &self,
        manifest: &StreamManifestV1,
        ranges: &[ExternallyVerifiedTerminalRangeV1],
    ) -> Result<()> {
        self.validate_manifest_bindings(manifest)?;
        let recomputed = largest_protected_cursor(manifest, ranges)?;
        if self.protected_through.next_sequence() > recomputed.next_sequence() {
            return Err(ProtocolError::InvalidTerminalCheckpoint {
                reason: "checkpoint is ahead of the recomputed protected prefix",
            });
        }
        if self.protected_through == manifest.stream().initial_cursor() {
            return Ok(());
        }
        if !ranges
            .iter()
            .any(|range| range.end == self.protected_through)
        {
            return Err(ProtocolError::InvalidTerminalCheckpoint {
                reason: "checkpoint is not the exact end of a protected object",
            });
        }
        Ok(())
    }

    fn validate_manifest_bindings(&self, manifest: &StreamManifestV1) -> Result<()> {
        let expected_store = manifest.deletion_authorizing_store_id();
        let expected_policy = manifest
            .durability_policy()
            .map(|policy| policy.policy_id());
        if expected_store != Some(self.terminal_store_id)
            || expected_policy != Some(self.policy_id)
            || self.stream_id != manifest.stream().stream_id()
            || self.stream_manifest_sha256 != manifest.stream().stream_manifest_sha256()
        {
            return Err(ProtocolError::InvalidTerminalCheckpoint {
                reason: "checkpoint identity differs from the immutable manifest",
            });
        }
        validate_initial_cursor_if_zero(self.protected_through, manifest)
    }
}

/// Checkpoints never move backward or change their immutable identity.
pub fn validate_terminal_checkpoint_transition(
    previous: &TerminalCursorCheckpointV1,
    next: &TerminalCursorCheckpointV1,
) -> Result<()> {
    if previous.terminal_store_id != next.terminal_store_id
        || previous.stream_id != next.stream_id
        || previous.stream_manifest_sha256 != next.stream_manifest_sha256
        || previous.policy_id != next.policy_id
    {
        return Err(ProtocolError::InvalidTerminalCheckpoint {
            reason: "checkpoint identity changed",
        });
    }
    if next.protected_through.next_sequence() < previous.protected_through.next_sequence() {
        return Err(ProtocolError::InvalidTerminalCheckpoint {
            reason: "protected cursor moved backward",
        });
    }
    if next.protected_through.next_sequence() == previous.protected_through.next_sequence()
        && next.protected_through != previous.protected_through
    {
        return Err(ProtocolError::CursorMismatch {
            context: "TerminalCursorCheckpointV1.transition",
        });
    }
    Ok(())
}

fn largest_protected_cursor(
    manifest: &StreamManifestV1,
    ranges: &[ExternallyVerifiedTerminalRangeV1],
) -> Result<CursorV1> {
    let store_id = manifest.deletion_authorizing_store_id().ok_or(
        ProtocolError::InvalidTerminalCheckpoint {
            reason: "manifest has no deletion-authorizing terminal store",
        },
    )?;
    let policy_id = manifest
        .durability_policy()
        .map(|policy| policy.policy_id())
        .ok_or(ProtocolError::InvalidTerminalCheckpoint {
            reason: "manifest has no terminal durability policy",
        })?;
    for range in ranges {
        if range.terminal_store_id != store_id
            || range.stream_id != manifest.stream().stream_id()
            || range.stream_manifest_sha256 != manifest.stream().stream_manifest_sha256()
            || range.policy_id != policy_id
        {
            return Err(ProtocolError::InvalidTerminalCheckpoint {
                reason: "protected range belongs to a different manifest binding",
            });
        }
    }

    let mut ordered = ranges.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|range| (range.start.next_sequence(), range.end.next_sequence()));
    for pair in ordered.windows(2) {
        let left = pair[0];
        let right = pair[1];
        if same_protected_identity(left, right) {
            continue;
        }
        validate_disjoint_ranges(left.start, left.end, right.start, right.end)?;
    }

    let mut cursor = manifest.stream().initial_cursor();
    let mut previous: Option<&ExternallyVerifiedTerminalRangeV1> = None;
    for range in ordered {
        if previous.is_some_and(|prior| same_protected_identity(prior, range)) {
            continue;
        }
        previous = Some(range);
        if range.start.next_sequence() > cursor.next_sequence() {
            break;
        }
        if range.start.next_sequence() < cursor.next_sequence() {
            return Err(ProtocolError::TerminalRangeConflict {
                reason: "protected ranges overlap",
            });
        }
        if range.start != cursor {
            return Err(ProtocolError::CursorMismatch {
                context: "ExternallyVerifiedTerminalRangeV1.start",
            });
        }
        cursor = range.end;
    }
    Ok(cursor)
}

fn same_object_identity(left: &TerminalRangeIndexV1, right: &TerminalRangeIndexV1) -> bool {
    left.start == right.start
        && left.end == right.end
        && left.encoded_len == right.encoded_len
        && left.encoded_sha256 == right.encoded_sha256
}

fn same_protected_identity(
    left: &ExternallyVerifiedTerminalRangeV1,
    right: &ExternallyVerifiedTerminalRangeV1,
) -> bool {
    left.start == right.start
        && left.end == right.end
        && left.encoded_len == right.encoded_len
        && left.encoded_sha256 == right.encoded_sha256
}

fn validate_disjoint_ranges(
    left_start: CursorV1,
    left_end: CursorV1,
    right_start: CursorV1,
    right_end: CursorV1,
) -> Result<()> {
    let overlaps = left_start.next_sequence() < right_end.next_sequence()
        && right_start.next_sequence() < left_end.next_sequence();
    if overlaps {
        return Err(ProtocolError::TerminalRangeConflict {
            reason: "ranges overlap with different object identity",
        });
    }
    if left_end.next_sequence() == right_start.next_sequence() && left_end != right_start {
        return Err(ProtocolError::CursorMismatch {
            context: "TerminalRangeIndexV1.adjacent_boundary",
        });
    }
    if right_end.next_sequence() == left_start.next_sequence() && right_end != left_start {
        return Err(ProtocolError::CursorMismatch {
            context: "TerminalRangeIndexV1.adjacent_boundary",
        });
    }
    Ok(())
}

fn validate_nonempty_range(start: CursorV1, end: CursorV1, context: &'static str) -> Result<()> {
    if end.next_sequence() <= start.next_sequence() {
        return Err(ProtocolError::InvalidCursorOrder { context });
    }
    Ok(())
}

fn validate_initial_cursor_if_zero(cursor: CursorV1, manifest: &StreamManifestV1) -> Result<()> {
    if cursor.next_sequence() == 0 && cursor != manifest.stream().initial_cursor() {
        return Err(ProtocolError::CursorMismatch {
            context: "terminal stream initial cursor",
        });
    }
    Ok(())
}

fn validate_terminal_locator(field: &'static str, value: &[u8], max: u64) -> Result<()> {
    let actual = value.len() as u64;
    if actual == 0 || actual > max {
        return Err(ProtocolError::InvalidTerminalLocatorLength {
            field,
            actual,
            min: 1,
            max,
        });
    }
    Ok(())
}

fn validate_copy_count(actual: usize) -> Result<()> {
    if actual == 0 || actual > MAX_TERMINAL_RANGE_INDEX_COPIES_V1 {
        return Err(ProtocolError::InvalidTerminalCopyCount {
            actual: actual as u64,
            min: 1,
            max: MAX_TERMINAL_RANGE_INDEX_COPIES_V1 as u64,
        });
    }
    Ok(())
}

fn encode_bytes(encoded: &mut Vec<u8>, value: &[u8]) {
    encoded.extend_from_slice(&(value.len() as u64).to_be_bytes());
    encoded.extend_from_slice(value);
}

fn encode_optional_bytes(encoded: &mut Vec<u8>, value: Option<&[u8]>) {
    if let Some(value) = value {
        encoded.push(1);
        encode_bytes(encoded, value);
    } else {
        encoded.push(0);
    }
}

fn push_hex(target: &mut String, bytes: &[u8]) {
    for byte in bytes {
        write!(target, "{byte:02x}").expect("writing to String cannot fail");
    }
}

fn require_magic(context: &'static str, actual: &[u8], expected: &[u8]) -> Result<()> {
    if actual != expected {
        return Err(ProtocolError::InvalidMagic { context });
    }
    Ok(())
}

fn require_at_least(context: &'static str, actual: usize, expected: usize) -> Result<()> {
    if actual < expected {
        return Err(ProtocolError::Truncated {
            context,
            expected,
            actual,
        });
    }
    Ok(())
}

fn require_exact_len(context: &'static str, actual: usize, expected: usize) -> Result<()> {
    require_at_least(context, actual, expected)?;
    if actual > expected {
        return Err(ProtocolError::TrailingBytes {
            context,
            count: actual - expected,
        });
    }
    Ok(())
}

fn copy_field<const N: usize>(target: &mut [u8], offset: &mut usize, field: &[u8; N]) {
    let end = *offset + N;
    target[*offset..end].copy_from_slice(field);
    *offset = end;
}

struct Reader<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn take(&mut self, length: usize, context: &'static str) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ProtocolError::IntegerOverflow { field: context })?;
        if end > self.encoded.len() {
            return Err(ProtocolError::Truncated {
                context,
                expected: end,
                actual: self.encoded.len(),
            });
        }
        let bytes = &self.encoded[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8> {
        Ok(self.take(1, field)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4, field)?.try_into().expect("fixed slice"),
        ))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8, field)?.try_into().expect("fixed slice"),
        ))
    }

    fn locator(&mut self, field: &'static str, max: u64) -> Result<Vec<u8>> {
        let length = self.u64(field)?;
        if length == 0 || length > max {
            return Err(ProtocolError::InvalidTerminalLocatorLength {
                field,
                actual: length,
                min: 1,
                max,
            });
        }
        let length =
            usize::try_from(length).map_err(|_| ProtocolError::IntegerOverflow { field })?;
        Ok(self.take(length, field)?.to_vec())
    }

    const fn offset(&self) -> usize {
        self.offset
    }

    fn remaining(&self) -> &'a [u8] {
        &self.encoded[self.offset..]
    }

    fn advance(&mut self, length: usize, context: &'static str) -> Result<()> {
        self.take(length, context).map(|_| ())
    }

    fn finish(self, context: &'static str) -> Result<()> {
        if self.offset != self.encoded.len() {
            return Err(ProtocolError::TrailingBytes {
                context,
                count: self.encoded.len() - self.offset,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterGenesisHash, DurabilityPolicyV1, DurabilityTargetDescriptorSha256,
        DurabilityTargetV1, OverflowNamespaceSha256, PrefixHash, TerminalCatalogDescriptorSha256,
    };

    const RAW_OBJECT_SHA256: [u8; 32] = [
        0xd6, 0xa0, 0xb8, 0x94, 0xb8, 0x4a, 0xf8, 0x7b, 0xaa, 0xbf, 0x64, 0x51, 0x3f, 0x08, 0x6b,
        0x94, 0xa0, 0x77, 0x96, 0x86, 0x8f, 0x9f, 0x73, 0x01, 0x0c, 0xd1, 0x32, 0xea, 0x26, 0xeb,
        0x3b, 0xaf,
    ];
    const RECEIPT_SHA256: [u8; 32] = [
        0x87, 0x70, 0x60, 0xd7, 0x6c, 0x87, 0x36, 0x28, 0x15, 0xfc, 0x97, 0x86, 0x54, 0x5a, 0x1b,
        0x64, 0x0c, 0xad, 0x03, 0xa1, 0x95, 0xc8, 0x2c, 0x86, 0x98, 0xd8, 0x68, 0x11, 0x15, 0x88,
        0x52, 0x1b,
    ];
    const RANGE_INDEX_SHA256: [u8; 32] = [
        0xdd, 0x06, 0x8a, 0x6f, 0x4d, 0x66, 0xa3, 0x49, 0xb9, 0x95, 0x32, 0x63, 0xd3, 0x74, 0xad,
        0x3d, 0xa4, 0x47, 0x05, 0x47, 0x13, 0x4d, 0x76, 0x57, 0xe0, 0x72, 0xe5, 0x8f, 0xb8, 0x21,
        0x0d, 0x04,
    ];

    fn target(id: u8, domain: u8) -> DurabilityTargetV1 {
        DurabilityTargetV1 {
            target_id: DurabilityTargetId::new([id; 16]),
            failure_domain_id: FailureDomainId::new([domain; 16]),
            target_descriptor_sha256: DurabilityTargetDescriptorSha256::new([id ^ domain; 32]),
        }
    }

    fn manifest_with_descriptor(producer_descriptor: Vec<u8>) -> StreamManifestV1 {
        let policy = DurabilityPolicyV1::new(
            DurabilityPolicyId::new([0x80; 16]),
            2,
            TerminalCatalogDescriptorSha256::new([0x90; 32]),
            vec![target(0xa1, 0xd1), target(0xa2, 0xd2), target(0xa3, 0xd1)],
        )
        .unwrap();
        StreamManifestV1::new(
            StreamId::new(core::array::from_fn(|index| index as u8)),
            ClusterGenesisHash::new(core::array::from_fn(|index| (index + 0x10) as u8)),
            2,
            1,
            producer_descriptor,
            None,
            Some(StreamId::new([0xf0; 16])),
            Some(OverflowNamespaceSha256::new([0x60; 32])),
            Some(DeletionAuthorizingStoreId::new([0x70; 16])),
            Some(policy),
        )
        .unwrap()
    }

    fn manifest() -> StreamManifestV1 {
        manifest_with_descriptor(b"capture-v1".to_vec())
    }

    fn object_from(start: CursorV1, payloads: &[&[u8]]) -> TerminalRawObjectV1 {
        let mut cursor = start;
        let frames = payloads
            .iter()
            .map(|payload| {
                let frame = FrameV1::new(cursor, payload.to_vec()).unwrap();
                cursor = frame.validate_after(cursor).unwrap();
                frame
            })
            .collect();
        TerminalRawObjectV1::new(manifest(), start, frames).unwrap()
    }

    fn object() -> TerminalRawObjectV1 {
        object_from(
            manifest().stream().initial_cursor(),
            &[b"abc", &[0, 1, 2, 255]],
        )
    }

    fn decode_limits() -> TerminalRawObjectDecodeLimitsV1 {
        TerminalRawObjectDecodeLimitsV1::new(1_048_576, 1_024).unwrap()
    }

    fn receipt(
        object: &TerminalRawObjectV1,
        target_id: u8,
        domain_id: u8,
        version: Option<Vec<u8>>,
    ) -> TerminalCopyReceiptV1 {
        let encoded = object.encode();
        receipt_for_encoded(object, &encoded, target_id, domain_id, version)
    }

    fn receipt_for_encoded(
        object: &TerminalRawObjectV1,
        encoded: &[u8],
        target_id: u8,
        domain_id: u8,
        version: Option<Vec<u8>>,
    ) -> TerminalCopyReceiptV1 {
        let digest: [u8; 32] = Sha256::digest(encoded).into();
        TerminalCopyReceiptV1::new(
            DeletionAuthorizingStoreId::new([0x70; 16]),
            DurabilityPolicyId::new([0x80; 16]),
            DurabilityTargetId::new([target_id; 16]),
            FailureDomainId::new([domain_id; 16]),
            object.manifest().stream().stream_id(),
            object.start(),
            object.end(),
            terminal_raw_object_key(
                object.manifest().stream().stream_id(),
                object.start(),
                object.end(),
                &digest,
            )
            .into_bytes(),
            version,
            encoded.len() as u64,
            digest,
            TerminalVerificationV1::FullReadbackSha256,
        )
        .unwrap()
    }

    fn index(object: &TerminalRawObjectV1, targets: &[(u8, u8)]) -> TerminalRangeIndexV1 {
        let encoded = object.encode();
        index_for_encoded(object, &encoded, targets)
    }

    fn index_for_encoded(
        object: &TerminalRawObjectV1,
        encoded: &[u8],
        targets: &[(u8, u8)],
    ) -> TerminalRangeIndexV1 {
        TerminalRangeIndexV1::new(
            object.manifest().stream().stream_id(),
            object.start(),
            object.end(),
            encoded.len() as u64,
            Sha256::digest(encoded).into(),
            targets
                .iter()
                .map(|&(target, domain)| receipt_for_encoded(object, encoded, target, domain, None))
                .collect(),
        )
        .unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn raw_object_is_self_describing_golden_and_round_trips() {
        let object = object();
        let encoded = object.encode();
        assert_eq!(Sha256::digest(&encoded).as_slice(), RAW_OBJECT_SHA256);
        assert_eq!(encoded.len(), 644);
        assert_eq!(
            TerminalRawObjectV1::decode(&encoded, decode_limits()),
            Ok(object.clone())
        );

        let manifest_bytes = object.manifest().encode();
        assert_eq!(&encoded[..8], b"HIVERAW1");
        assert_eq!(&encoded[8..8 + manifest_bytes.len()], manifest_bytes);
        assert_eq!(
            &encoded[8 + manifest_bytes.len()..8 + manifest_bytes.len() + 40],
            object.start().fixed_encode()
        );
        assert_eq!(
            object.object_key(),
            "hive-raw/v1/000102030405060708090a0b0c0d0e0f/0000000000000000-0000000000000002-\
             d6a0b894b84af87baabf64513f086b94a07796868f9f73010cd132ea26eb3baf.hraw"
                .replace(' ', "")
        );
    }

    #[test]
    fn manifest_prefix_is_exactly_self_delimiting() {
        let manifest = manifest();
        let bytes = manifest.encode();
        let mut container = bytes.clone();
        container.extend_from_slice(b"suffix");
        assert_eq!(
            StreamManifestV1::decode_prefix(&container),
            Ok((manifest, bytes.len()))
        );
        assert!(matches!(
            StreamManifestV1::decode(&container),
            Err(ProtocolError::TrailingBytes { count: 6, .. })
        ));
    }

    #[test]
    fn raw_object_rejects_empty_corrupt_truncated_and_trailing_data() {
        let manifest = manifest();
        let start = manifest.stream().initial_cursor();
        assert!(matches!(
            TerminalRawObjectV1::new(manifest.clone(), start, Vec::new()),
            Err(ProtocolError::InvalidTerminalRawObject { .. })
        ));

        let object = object();
        let encoded = object.encode();
        for length in [0, 7, encoded.len() - 1] {
            assert!(TerminalRawObjectV1::decode(&encoded[..length], decode_limits()).is_err());
        }

        let mut bad_manifest = encoded.clone();
        // Stored manifest digest starts after magic, version, stream/genesis,
        // format/version, and producer digest.
        bad_manifest[8 + 2 + 16 + 32 + 4 + 2 + 32] ^= 1;
        assert_eq!(
            TerminalRawObjectV1::decode(&bad_manifest, decode_limits()),
            Err(ProtocolError::ManifestHashMismatch)
        );

        let mut bad_frame = encoded.clone();
        let first_frame = object.header().encoded_len();
        bad_frame[first_frame + object.frames()[0].encoded_len() - 1] ^= 1;
        assert_eq!(
            TerminalRawObjectV1::decode(&bad_frame, decode_limits()),
            Err(ProtocolError::PrefixMismatch)
        );

        let mut bad_footer = encoded.clone();
        let last = bad_footer.len() - 1;
        bad_footer[last] ^= 1;
        assert!(matches!(
            TerminalRawObjectV1::decode(&bad_footer, decode_limits()),
            Err(ProtocolError::CursorMismatch { .. })
        ));

        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            TerminalRawObjectV1::decode(&trailing, decode_limits()),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let complete = object.encode();
        let footer_start = complete.len() - TERMINAL_RAW_FOOTER_V1_ENCODED_LEN;
        for prefix_len in 1..TERMINAL_RAW_FOOTER_V1_MAGIC.len() {
            assert!(matches!(
                TerminalRawObjectV1::decode(
                    &complete[..footer_start + prefix_len],
                    decode_limits(),
                ),
                Err(ProtocolError::Truncated { .. })
            ));
        }
    }

    #[test]
    fn raw_object_decode_enforces_finite_byte_and_record_limits() {
        assert!(matches!(
            TerminalRawObjectDecodeLimitsV1::new(
                MIN_TERMINAL_RAW_OBJECT_V1_ENCODED_LEN as u64 - 1,
                1,
            ),
            Err(ProtocolError::InvalidTerminalRawObjectLimit {
                field: "max_encoded_bytes",
                ..
            })
        ));
        assert!(matches!(
            TerminalRawObjectDecodeLimitsV1::new(MIN_TERMINAL_RAW_OBJECT_V1_ENCODED_LEN as u64, 0,),
            Err(ProtocolError::InvalidTerminalRawObjectLimit {
                field: "max_records",
                ..
            })
        ));

        let object = object();
        let encoded = object.encode();
        let too_small = TerminalRawObjectDecodeLimitsV1::new(encoded.len() as u64 - 1, 2).unwrap();
        assert_eq!(
            TerminalRawObjectV1::decode(&encoded, too_small),
            Err(ProtocolError::TerminalRawObjectTooLarge {
                actual: encoded.len() as u64,
                max: encoded.len() as u64 - 1,
            })
        );
        let malformed_oversized = vec![0; MIN_TERMINAL_RAW_OBJECT_V1_ENCODED_LEN + 1];
        let minimum_limit =
            TerminalRawObjectDecodeLimitsV1::new(MIN_TERMINAL_RAW_OBJECT_V1_ENCODED_LEN as u64, 1)
                .unwrap();
        assert_eq!(
            TerminalRawObjectV1::decode(&malformed_oversized, minimum_limit),
            Err(ProtocolError::TerminalRawObjectTooLarge {
                actual: malformed_oversized.len() as u64,
                max: MIN_TERMINAL_RAW_OBJECT_V1_ENCODED_LEN as u64,
            })
        );

        let one_record = TerminalRawObjectDecodeLimitsV1::new(encoded.len() as u64, 1).unwrap();
        assert_eq!(
            TerminalRawObjectV1::decode(&encoded, one_record),
            Err(ProtocolError::TerminalRawObjectRecordLimitExceeded { actual: 2, max: 1 })
        );
        let second_frame = object.header().encoded_len() + object.frames()[0].encoded_len();
        let truncated_second_frame = &encoded[..second_frame + FRAME_V1_MAGIC.len()];
        assert_eq!(
            TerminalRawObjectV1::decode(truncated_second_frame, one_record),
            Err(ProtocolError::TerminalRawObjectRecordLimitExceeded { actual: 2, max: 1 })
        );
    }

    #[test]
    fn copy_receipt_is_golden_and_strictly_decoded() {
        let object = object();
        let receipt = receipt(&object, 0xa1, 0xd1, Some(b"version-1".to_vec()));
        let encoded = receipt.encode();
        assert_eq!(Sha256::digest(&encoded).as_slice(), RECEIPT_SHA256);
        assert_eq!(encoded.len(), 375);
        assert_eq!(TerminalCopyReceiptV1::decode(&encoded), Ok(receipt));

        let version_tag = 5 * 16 + 2 * CURSOR_V1_ENCODED_LEN + 8 + TERMINAL_RAW_OBJECT_KEY_V1_LEN;
        let mut unknown_option = encoded.clone();
        unknown_option[version_tag] = 3;
        assert!(matches!(
            TerminalCopyReceiptV1::decode(&unknown_option),
            Err(ProtocolError::InvalidOptionTag { .. })
        ));

        let mut unknown_verification = encoded.clone();
        *unknown_verification.last_mut().unwrap() = 0;
        assert_eq!(
            TerminalCopyReceiptV1::decode(&unknown_verification),
            Err(ProtocolError::UnknownTerminalVerification { value: 0 })
        );

        let version_len = version_tag + 1;
        let mut empty_version = encoded.clone();
        empty_version[version_len..version_len + 8].copy_from_slice(&0_u64.to_be_bytes());
        assert!(matches!(
            TerminalCopyReceiptV1::decode(&empty_version),
            Err(ProtocolError::InvalidTerminalLocatorLength { actual: 0, .. })
        ));

        let mut oversized_version = encoded.clone();
        oversized_version[version_len..version_len + 8]
            .copy_from_slice(&(MAX_TERMINAL_OBJECT_VERSION_BYTES + 1).to_be_bytes());
        assert!(matches!(
            TerminalCopyReceiptV1::decode(&oversized_version),
            Err(ProtocolError::InvalidTerminalLocatorLength { .. })
        ));

        let key_len_offset = 5 * 16 + 2 * CURSOR_V1_ENCODED_LEN;
        let mut oversized_key = encoded.clone();
        oversized_key[key_len_offset..key_len_offset + 8]
            .copy_from_slice(&(MAX_TERMINAL_OBJECT_KEY_BYTES + 1).to_be_bytes());
        assert!(matches!(
            TerminalCopyReceiptV1::decode(&oversized_key),
            Err(ProtocolError::InvalidTerminalLocatorLength { .. })
        ));

        let mut wrong_key = encoded.clone();
        wrong_key[key_len_offset + 8] ^= 1;
        assert_eq!(
            TerminalCopyReceiptV1::decode(&wrong_key),
            Err(ProtocolError::TerminalObjectKeyMismatch)
        );

        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            TerminalCopyReceiptV1::decode(&trailing),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));
        assert!(TerminalCopyReceiptV1::decode(&trailing[..trailing.len() - 2]).is_err());
    }

    #[test]
    fn range_index_is_golden_sorted_and_strictly_decoded() {
        let object = object();
        let index = index(&object, &[(0xa2, 0xd2), (0xa1, 0xd1)]);
        assert_eq!(
            index.copies()[0].target_id(),
            DurabilityTargetId::new([0xa1; 16])
        );
        let encoded = index.encode();
        assert_eq!(Sha256::digest(&encoded).as_slice(), RANGE_INDEX_SHA256);
        assert_eq!(encoded.len(), 856);
        assert_eq!(TerminalRangeIndexV1::decode(&encoded), Ok(index.clone()));

        let receipt_len = index.copies()[0].canonical_encoded_len();
        assert_eq!(receipt_len, index.copies()[1].canonical_encoded_len());
        let mut noncanonical = encoded.clone();
        let first = TERMINAL_RANGE_INDEX_V1_FIXED_ENCODED_LEN;
        let second = first + receipt_len;
        let left = encoded[first..second].to_vec();
        let right = encoded[second..second + receipt_len].to_vec();
        noncanonical[first..second].copy_from_slice(&right);
        noncanonical[second..second + receipt_len].copy_from_slice(&left);
        assert!(matches!(
            TerminalRangeIndexV1::decode(&noncanonical),
            Err(ProtocolError::NonCanonicalOrder { .. })
        ));

        assert!(matches!(
            TerminalRangeIndexV1::new(
                index.stream_id(),
                index.start(),
                index.end(),
                index.encoded_len(),
                *index.encoded_sha256(),
                vec![index.copies()[0].clone(), index.copies()[0].clone()],
            ),
            Err(ProtocolError::InvalidTerminalRangeIndex { .. })
        ));

        let mut huge_count = encoded;
        let count_offset = TERMINAL_RANGE_INDEX_V1_FIXED_ENCODED_LEN - 4;
        huge_count[count_offset..count_offset + 4].copy_from_slice(&257_u32.to_be_bytes());
        assert!(matches!(
            TerminalRangeIndexV1::decode(&huge_count),
            Err(ProtocolError::InvalidTerminalCopyCount { .. })
        ));

        let mut zero_count = index.encode();
        zero_count[count_offset..count_offset + 4].copy_from_slice(&0_u32.to_be_bytes());
        assert!(matches!(
            TerminalRangeIndexV1::decode(&zero_count),
            Err(ProtocolError::InvalidTerminalCopyCount { actual: 0, .. })
        ));

        let mut trailing = index.encode();
        trailing.push(0);
        assert!(matches!(
            TerminalRangeIndexV1::decode(&trailing),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let oversized = vec![0; MAX_TERMINAL_RANGE_INDEX_V1_ENCODED_LEN + 1];
        assert!(matches!(
            TerminalRangeIndexV1::decode(&oversized),
            Err(ProtocolError::TerminalRangeIndexTooLarge { .. })
        ));
    }

    #[test]
    fn protection_requires_exact_policy_domains_object_and_external_checks() {
        let object = object();
        let encoded = object.encode();
        let manifest = manifest();

        let one_copy = index(&object, &[(0xa1, 0xd1)]);
        let mut checked = 0;
        assert_eq!(
            one_copy.validate_for_protection_with(&manifest, &encoded, decode_limits(), |_| {
                checked += 1;
                Ok(())
            }),
            Err(ProtocolError::TerminalDurabilityDeficit {
                actual: 1,
                required: 2,
            })
        );
        assert_eq!(checked, 0);

        let same_domain = index(&object, &[(0xa1, 0xd1), (0xa3, 0xd1)]);
        assert!(matches!(
            same_domain.validate_for_protection_with(&manifest, &encoded, decode_limits(), |_| {
                checked += 1;
                Ok(())
            },),
            Err(ProtocolError::TerminalDurabilityDeficit { .. })
        ));
        assert_eq!(checked, 0);

        let wrong_domain = index(&object, &[(0xa1, 0xd2), (0xa2, 0xd2)]);
        assert!(matches!(
            wrong_domain.validate_for_protection_with(&manifest, &encoded, decode_limits(), |_| {
                checked += 1;
                Ok(())
            },),
            Err(ProtocolError::InvalidTerminalCopyReceipt { .. })
        ));
        assert_eq!(checked, 0);

        let unknown_target = index(&object, &[(0xa1, 0xd1), (0xaf, 0xd2)]);
        assert!(matches!(
            unknown_target.validate_for_protection_with(
                &manifest,
                &encoded,
                decode_limits(),
                |_| {
                    checked += 1;
                    Ok(())
                },
            ),
            Err(ProtocolError::InvalidTerminalCopyReceipt { .. })
        ));
        assert_eq!(checked, 0);

        let valid = index(&object, &[(0xa1, 0xd1), (0xa2, 0xd2)]);
        let evidence = valid
            .validate_for_protection_with(&manifest, &encoded, decode_limits(), |copy| {
                checked += 1;
                assert_eq!(
                    copy.verification(),
                    TerminalVerificationV1::FullReadbackSha256
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(checked, 2);
        assert_eq!(evidence.start(), object.start());
        assert_eq!(evidence.end(), object.end());

        let mut denied = 0;
        assert!(
            valid
                .validate_for_protection_with(&manifest, &encoded, decode_limits(), |_| {
                    denied += 1;
                    Err(ProtocolError::InvalidTerminalCopyReceipt {
                        reason: "test external verification failure",
                    })
                },)
                .is_err()
        );
        assert_eq!(denied, 1);

        let mut preflight_callbacks = 0;
        assert!(matches!(
            valid.validate_for_protection_with(
                &manifest,
                &encoded[..encoded.len() - 1],
                decode_limits(),
                |_| {
                    preflight_callbacks += 1;
                    Ok(())
                },
            ),
            Err(ProtocolError::EncodedLengthMismatch { .. })
        ));
        assert_eq!(preflight_callbacks, 0);

        let undersized_limit =
            TerminalRawObjectDecodeLimitsV1::new(encoded.len() as u64 - 1, 2).unwrap();
        assert!(matches!(
            valid.validate_for_protection_with(&manifest, &encoded, undersized_limit, |_| {
                preflight_callbacks += 1;
                Ok(())
            },),
            Err(ProtocolError::TerminalRawObjectTooLarge { .. })
        ));
        assert_eq!(preflight_callbacks, 0);

        let mut corrupt = encoded.clone();
        corrupt[object.header().encoded_len() + 12] ^= 1;
        assert!(matches!(
            valid.validate_for_protection_with(&manifest, &corrupt, decode_limits(), |_| {
                preflight_callbacks += 1;
                Ok(())
            }),
            Err(ProtocolError::EncodedSha256Mismatch)
        ));
        assert_eq!(preflight_callbacks, 0);

        let corrupt_index = index_for_encoded(&object, &corrupt, &[(0xa1, 0xd1), (0xa2, 0xd2)]);
        assert!(
            corrupt_index
                .validate_for_protection_with(&manifest, &corrupt, decode_limits(), |_| {
                    preflight_callbacks += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(preflight_callbacks, 0);

        let other_manifest = manifest_with_descriptor(b"capture-v2".to_vec());
        assert!(matches!(
            valid.validate_for_protection_with(&other_manifest, &encoded, decode_limits(), |_| {
                preflight_callbacks += 1;
                Ok(())
            },),
            Err(ProtocolError::InvalidTerminalRangeIndex { .. })
        ));
        assert_eq!(preflight_callbacks, 0);
    }

    #[test]
    fn catalog_insert_is_idempotent_but_rejects_overlap_and_bad_joins() {
        let first_object = object_from(manifest().stream().initial_cursor(), &[b"a"]);
        let first = index(&first_object, &[(0xa1, 0xd1)]);
        assert_eq!(
            validate_terminal_range_index_insert(std::slice::from_ref(&first), &first),
            Ok(TerminalRangeIndexInsertDispositionV1::ReuseExisting)
        );

        let second_object = object_from(first_object.end(), &[b"b"]);
        let second = index(&second_object, &[(0xa1, 0xd1)]);
        assert_eq!(
            validate_terminal_range_index_insert(std::slice::from_ref(&first), &second),
            Ok(TerminalRangeIndexInsertDispositionV1::Insert)
        );

        let combined = object_from(first_object.start(), &[b"a", b"b"]);
        let overlapping = index(&combined, &[(0xa1, 0xd1)]);
        assert!(matches!(
            validate_terminal_range_index_insert(std::slice::from_ref(&first), &overlapping),
            Err(ProtocolError::TerminalRangeConflict { .. })
        ));

        let wrong_start = CursorV1::new(first.end().next_sequence(), PrefixHash::new([0xee; 32]));
        let wrong_join_object = object_from(wrong_start, &[b"b"]);
        let wrong_join = index(&wrong_join_object, &[(0xa1, 0xd1)]);
        assert!(matches!(
            validate_terminal_range_index_insert(&[first], &wrong_join),
            Err(ProtocolError::CursorMismatch { .. })
        ));
    }

    #[test]
    fn checkpoint_advances_only_over_gap_free_externally_verified_ranges() {
        let manifest = manifest();
        let golden_object = object();
        let golden_range = index(&golden_object, &[(0xa1, 0xd1), (0xa2, 0xd2)])
            .validate_for_protection_with(
                &manifest,
                &golden_object.encode(),
                decode_limits(),
                |_| Ok(()),
            )
            .unwrap();
        let golden =
            TerminalCursorCheckpointV1::from_verified_ranges(&manifest, &[golden_range]).unwrap();
        assert_eq!(
            hex(&golden.fixed_encode()),
            concat!(
                "70707070707070707070707070707070",
                "000102030405060708090a0b0c0d0e0f",
                "efa990d827e7c54c09bb9a971ba001ab198dae1120447ffcf0e75ba80d83debc",
                "80808080808080808080808080808080",
                "0000000000000002",
                "90e9fab1aa9b1abcef1ff502e9abb5898a8d15eaac3dc7209853a440eed7a27e"
            )
        );

        let first_object = object_from(manifest.stream().initial_cursor(), &[b"a"]);
        let second_object = object_from(first_object.end(), &[b"b"]);
        let first = index(&first_object, &[(0xa1, 0xd1), (0xa2, 0xd2)])
            .validate_for_protection_with(
                &manifest,
                &first_object.encode(),
                decode_limits(),
                |_| Ok(()),
            )
            .unwrap();
        let second = index(&second_object, &[(0xa1, 0xd1), (0xa2, 0xd2)])
            .validate_for_protection_with(
                &manifest,
                &second_object.encode(),
                decode_limits(),
                |_| Ok(()),
            )
            .unwrap();

        let gap = TerminalCursorCheckpointV1::from_verified_ranges(&manifest, &[second]).unwrap();
        assert_eq!(gap.protected_through(), manifest.stream().initial_cursor());

        let first_only =
            TerminalCursorCheckpointV1::from_verified_ranges(&manifest, &[first]).unwrap();
        let complete =
            TerminalCursorCheckpointV1::from_verified_ranges(&manifest, &[second, first]).unwrap();
        assert_eq!(complete.protected_through(), second_object.end());
        assert_eq!(
            complete.fixed_encode().len(),
            TERMINAL_CURSOR_CHECKPOINT_V1_ENCODED_LEN
        );
        assert_eq!(
            TerminalCursorCheckpointV1::decode(&complete.fixed_encode(), &manifest),
            Ok(complete)
        );
        assert!(
            first_only
                .validate_against_verified_ranges(&manifest, &[first, second])
                .is_ok()
        );
        assert!(validate_terminal_checkpoint_transition(&first_only, &complete).is_ok());
        assert!(matches!(
            validate_terminal_checkpoint_transition(&complete, &first_only),
            Err(ProtocolError::InvalidTerminalCheckpoint { .. })
        ));

        let mut wrong_binding = complete.fixed_encode();
        wrong_binding[0] ^= 1;
        assert!(matches!(
            TerminalCursorCheckpointV1::decode(&wrong_binding, &manifest),
            Err(ProtocolError::InvalidTerminalCheckpoint { .. })
        ));

        let mut ahead = complete.fixed_encode();
        let cursor_offset = TERMINAL_CURSOR_CHECKPOINT_V1_ENCODED_LEN - CURSOR_V1_ENCODED_LEN;
        ahead[cursor_offset..cursor_offset + 8].copy_from_slice(&3_u64.to_be_bytes());
        let ahead = TerminalCursorCheckpointV1::decode(&ahead, &manifest).unwrap();
        assert!(matches!(
            ahead.validate_against_verified_ranges(&manifest, &[first, second]),
            Err(ProtocolError::InvalidTerminalCheckpoint { .. })
        ));
    }

    #[test]
    fn fixed_decoders_reject_truncation_and_trailing_bytes() {
        let object = object();
        let header = object.header().encode();
        assert!(TerminalRawHeaderV1::decode(&header[..header.len() - 1]).is_err());
        let mut header_trailing = header;
        header_trailing.push(0);
        assert!(matches!(
            TerminalRawHeaderV1::decode(&header_trailing),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let footer = object.footer().encode();
        assert!(TerminalRawFooterV1::decode(&footer[..footer.len() - 1]).is_err());
        let mut footer_trailing = footer.to_vec();
        footer_trailing.push(0);
        assert!(matches!(
            TerminalRawFooterV1::decode(&footer_trailing),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));
    }
}
