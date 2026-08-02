use sha2::{Digest, Sha256};

use crate::{
    CURSOR_V1_ENCODED_LEN, CursorV1, DeletionAuthorizingStoreId, FRAME_V1_FIXED_ENCODED_LEN,
    FRAME_V1_MAGIC, FrameV1, MAX_RECORD_V1_ENCODED_LEN, ProtocolError, RECORD_V1_FIXED_ENCODED_LEN,
    Result, SessionId, StreamHeaderV1, StreamId, StreamManifestV1,
};

pub const MAX_CHUNK_RECORDS_V1: u32 = 1_048_576;
pub const MAX_PARALLEL_FETCHES_V1: u16 = 64;
pub const MIN_SYNC_RECORD_BYTES_V1: u64 = RECORD_V1_FIXED_ENCODED_LEN as u64;
pub const MAX_SYNC_RECORD_BYTES_V1: u64 = MAX_RECORD_V1_ENCODED_LEN;

pub const CHUNK_HEADER_V1_MAGIC: [u8; 8] = *b"HIVECHK1";
pub const CHUNK_FOOTER_V1_MAGIC: [u8; 8] = *b"HIVEEND1";
pub const CHUNK_HEADER_V1_ENCODED_LEN: usize =
    CHUNK_HEADER_V1_MAGIC.len() + StreamId::LENGTH + CURSOR_V1_ENCODED_LEN;
pub const CHUNK_FOOTER_V1_ENCODED_LEN: usize = CHUNK_FOOTER_V1_MAGIC.len() + CURSOR_V1_ENCODED_LEN;
pub const MIN_TRANSFER_CHUNK_V1_ENCODED_LEN: usize =
    CHUNK_HEADER_V1_ENCODED_LEN + FRAME_V1_FIXED_ENCODED_LEN + CHUNK_FOOTER_V1_ENCODED_LEN;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenV1 {
    stream_id: StreamId,
    terminal_store_id: DeletionAuthorizingStoreId,
    protected_cursor: Option<CursorV1>,
}

impl OpenV1 {
    #[must_use]
    pub const fn new(
        stream_id: StreamId,
        terminal_store_id: DeletionAuthorizingStoreId,
        protected_cursor: Option<CursorV1>,
    ) -> Self {
        Self {
            stream_id,
            terminal_store_id,
            protected_cursor,
        }
    }

    #[must_use]
    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    #[must_use]
    pub const fn terminal_store_id(&self) -> DeletionAuthorizingStoreId {
        self.terminal_store_id
    }

    #[must_use]
    pub const fn protected_cursor(&self) -> Option<CursorV1> {
        self.protected_cursor
    }

    /// Resolves `None` to this stream's exact `P(0)` and rejects use with a
    /// different stream header.
    pub fn effective_protected_cursor(&self, stream: StreamHeaderV1) -> Result<CursorV1> {
        if stream.stream_id() != self.stream_id {
            return Err(ProtocolError::StreamMismatch { context: "OpenV1" });
        }
        let protected_cursor = self
            .protected_cursor
            .unwrap_or_else(|| stream.initial_cursor());
        validate_initial_cursor_if_zero(protected_cursor, stream)?;
        Ok(protected_cursor)
    }

    /// Verifies the immutable source/store binding. A protected cursor above
    /// `P(0)` still requires an exact source-index lookup before open succeeds.
    pub fn validate_against_manifest(&self, manifest: &StreamManifestV1) -> Result<()> {
        if self.stream_id != manifest.stream().stream_id() {
            return Err(ProtocolError::StreamMismatch { context: "OpenV1" });
        }
        if manifest.deletion_authorizing_store_id() != Some(self.terminal_store_id) {
            return Err(ProtocolError::AckBindingMismatch {
                reason: "open terminal store is not the deletion-authorizing store",
            });
        }
        self.effective_protected_cursor(manifest.stream())?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResumeV1 {
    stream: StreamHeaderV1,
    session_id: SessionId,
    first_available: CursorV1,
    bulk_start: CursorV1,
    cutover: CursorV1,
    max_record_bytes: u64,
    max_chunk_records: u32,
    max_parallel_fetches: u16,
}

impl ResumeV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: StreamHeaderV1,
        session_id: SessionId,
        first_available: CursorV1,
        bulk_start: CursorV1,
        cutover: CursorV1,
        max_record_bytes: u64,
        max_chunk_records: u32,
        max_parallel_fetches: u16,
    ) -> Result<Self> {
        validate_limit(
            "max_record_bytes",
            max_record_bytes,
            MIN_SYNC_RECORD_BYTES_V1,
            MAX_SYNC_RECORD_BYTES_V1,
        )?;
        validate_limit(
            "max_chunk_records",
            u64::from(max_chunk_records),
            1,
            u64::from(MAX_CHUNK_RECORDS_V1),
        )?;
        validate_limit(
            "max_parallel_fetches",
            u64::from(max_parallel_fetches),
            1,
            u64::from(MAX_PARALLEL_FETCHES_V1),
        )?;
        validate_cursor_not_after(first_available, bulk_start, "ResumeV1.first_available")?;
        validate_cursor_not_after(bulk_start, cutover, "ResumeV1.bulk_start")?;
        validate_initial_cursor_if_zero(first_available, stream)?;
        validate_initial_cursor_if_zero(bulk_start, stream)?;
        validate_initial_cursor_if_zero(cutover, stream)?;
        Ok(Self {
            stream,
            session_id,
            first_available,
            bulk_start,
            cutover,
            max_record_bytes,
            max_chunk_records,
            max_parallel_fetches,
        })
    }

    #[must_use]
    pub const fn stream(&self) -> StreamHeaderV1 {
        self.stream
    }

    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    #[must_use]
    pub const fn first_available(&self) -> CursorV1 {
        self.first_available
    }

    #[must_use]
    pub const fn bulk_start(&self) -> CursorV1 {
        self.bulk_start
    }

    #[must_use]
    pub const fn cutover(&self) -> CursorV1 {
        self.cutover
    }

    #[must_use]
    pub const fn max_record_bytes(&self) -> u64 {
        self.max_record_bytes
    }

    #[must_use]
    pub const fn max_chunk_records(&self) -> u32 {
        self.max_chunk_records
    }

    #[must_use]
    pub const fn max_parallel_fetches(&self) -> u16 {
        self.max_parallel_fetches
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchRangeV1 {
    session_id: SessionId,
    cutover: CursorV1,
    first_sequence: u64,
    next_sequence: u64,
}

impl FetchRangeV1 {
    pub fn new(
        session_id: SessionId,
        cutover: CursorV1,
        first_sequence: u64,
        next_sequence: u64,
    ) -> Result<Self> {
        if first_sequence >= next_sequence {
            return Err(ProtocolError::InvalidFetchRange {
                reason: "range must be non-empty and increasing",
            });
        }
        if next_sequence > cutover.next_sequence() {
            return Err(ProtocolError::InvalidFetchRange {
                reason: "range ends after cutover",
            });
        }
        if next_sequence - first_sequence > u64::from(MAX_CHUNK_RECORDS_V1) {
            return Err(ProtocolError::InvalidFetchRange {
                reason: "range exceeds the global V1 record-count limit",
            });
        }
        Ok(Self {
            session_id,
            cutover,
            first_sequence,
            next_sequence,
        })
    }

    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    #[must_use]
    pub const fn cutover(&self) -> CursorV1 {
        self.cutover
    }

    #[must_use]
    pub const fn first_sequence(&self) -> u64 {
        self.first_sequence
    }

    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.next_sequence - self.first_sequence
    }

    pub fn validate_against_resume(&self, resume: &ResumeV1) -> Result<()> {
        if self.session_id != resume.session_id {
            return Err(ProtocolError::SessionMismatch);
        }
        if self.cutover != resume.cutover {
            return Err(ProtocolError::CursorMismatch {
                context: "FetchRangeV1.cutover",
            });
        }
        if self.first_sequence < resume.bulk_start.next_sequence() {
            return Err(ProtocolError::InvalidFetchRange {
                reason: "range starts before bulk_start",
            });
        }
        if self.record_count() > u64::from(resume.max_chunk_records) {
            return Err(ProtocolError::InvalidFetchRange {
                reason: "range exceeds the advertised record-count limit",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransferChunkCommitV1 {
    start: CursorV1,
    end: CursorV1,
    encoded_len: u64,
    encoded_sha256: [u8; 32],
}

impl TransferChunkCommitV1 {
    pub fn new(
        start: CursorV1,
        end: CursorV1,
        encoded_len: u64,
        encoded_sha256: [u8; 32],
    ) -> Result<Self> {
        if start.next_sequence() >= end.next_sequence() {
            return Err(ProtocolError::InvalidTransferChunk {
                reason: "chunk range must be non-empty and increasing",
            });
        }
        if encoded_len < MIN_TRANSFER_CHUNK_V1_ENCODED_LEN as u64 {
            return Err(ProtocolError::InvalidTransferChunk {
                reason: "encoded length is below one-frame minimum",
            });
        }
        Ok(Self {
            start,
            end,
            encoded_len,
            encoded_sha256,
        })
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum HiveSyncErrorCodeV1 {
    Unauthorized = 1,
    CursorMismatch = 2,
    PrefixRetired = 3,
    RecoveryIncomplete = 4,
    ChunkMismatch = 5,
    TemporarilyUnavailable = 6,
    LiveBackpressure = 7,
    Limit = 8,
    StaleSession = 9,
}

impl TryFrom<i32> for HiveSyncErrorCodeV1 {
    type Error = ProtocolError;

    fn try_from(value: i32) -> Result<Self> {
        match value {
            1 => Ok(Self::Unauthorized),
            2 => Ok(Self::CursorMismatch),
            3 => Ok(Self::PrefixRetired),
            4 => Ok(Self::RecoveryIncomplete),
            5 => Ok(Self::ChunkMismatch),
            6 => Ok(Self::TemporarilyUnavailable),
            7 => Ok(Self::LiveBackpressure),
            8 => Ok(Self::Limit),
            9 => Ok(Self::StaleSession),
            value => Err(ProtocolError::UnknownHiveSyncErrorCode { value }),
        }
    }
}

impl TryFrom<u16> for HiveSyncErrorCodeV1 {
    type Error = ProtocolError;

    fn try_from(value: u16) -> Result<Self> {
        Self::try_from(i32::from(value))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorV1 {
    code: HiveSyncErrorCodeV1,
}

impl ErrorV1 {
    #[must_use]
    pub const fn new(code: HiveSyncErrorCodeV1) -> Self {
        Self { code }
    }

    #[must_use]
    pub const fn code(&self) -> HiveSyncErrorCodeV1 {
        self.code
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkHeaderV1 {
    stream_id: StreamId,
    start: CursorV1,
}

impl ChunkHeaderV1 {
    #[must_use]
    pub const fn new(stream_id: StreamId, start: CursorV1) -> Self {
        Self { stream_id, start }
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
    pub fn encode(&self) -> [u8; CHUNK_HEADER_V1_ENCODED_LEN] {
        let mut encoded = [0_u8; CHUNK_HEADER_V1_ENCODED_LEN];
        encoded[..8].copy_from_slice(&CHUNK_HEADER_V1_MAGIC);
        encoded[8..24].copy_from_slice(self.stream_id.as_bytes());
        encoded[24..].copy_from_slice(&self.start.fixed_encode());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_len("ChunkHeaderV1", encoded.len(), CHUNK_HEADER_V1_ENCODED_LEN)?;
        require_magic("ChunkHeaderV1", &encoded[..8], &CHUNK_HEADER_V1_MAGIC)?;
        Ok(Self::new(
            StreamId::try_from(&encoded[8..24])?,
            CursorV1::decode(&encoded[24..])?,
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkFooterV1 {
    end: CursorV1,
}

impl ChunkFooterV1 {
    #[must_use]
    pub const fn new(end: CursorV1) -> Self {
        Self { end }
    }

    #[must_use]
    pub const fn end(&self) -> CursorV1 {
        self.end
    }

    #[must_use]
    pub fn encode(&self) -> [u8; CHUNK_FOOTER_V1_ENCODED_LEN] {
        let mut encoded = [0_u8; CHUNK_FOOTER_V1_ENCODED_LEN];
        encoded[..8].copy_from_slice(&CHUNK_FOOTER_V1_MAGIC);
        encoded[8..].copy_from_slice(&self.end.fixed_encode());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_len("ChunkFooterV1", encoded.len(), CHUNK_FOOTER_V1_ENCODED_LEN)?;
        require_magic("ChunkFooterV1", &encoded[..8], &CHUNK_FOOTER_V1_MAGIC)?;
        Ok(Self::new(CursorV1::decode(&encoded[8..])?))
    }

    pub fn validate_at(&self, expected: CursorV1) -> Result<()> {
        if self.end != expected {
            return Err(ProtocolError::CursorMismatch {
                context: "ChunkFooterV1.end",
            });
        }
        Ok(())
    }
}

/// One complete non-empty canonical body returned by `FetchRange`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferChunkV1 {
    header: ChunkHeaderV1,
    frames: Vec<FrameV1>,
    footer: ChunkFooterV1,
}

impl TransferChunkV1 {
    pub fn new(header: ChunkHeaderV1, frames: Vec<FrameV1>) -> Result<Self> {
        if frames.is_empty() {
            return Err(ProtocolError::InvalidTransferChunk {
                reason: "a fetch chunk must contain at least one frame",
            });
        }
        if frames.len() > MAX_CHUNK_RECORDS_V1 as usize {
            return Err(ProtocolError::InvalidTransferChunk {
                reason: "frame count exceeds the global V1 limit",
            });
        }
        let mut cursor = header.start;
        for frame in &frames {
            cursor = frame.validate_after(cursor)?;
        }
        Ok(Self {
            header,
            frames,
            footer: ChunkFooterV1::new(cursor),
        })
    }

    #[must_use]
    pub const fn header(&self) -> ChunkHeaderV1 {
        self.header
    }

    #[must_use]
    pub fn frames(&self) -> &[FrameV1] {
        &self.frames
    }

    #[must_use]
    pub const fn footer(&self) -> ChunkFooterV1 {
        self.footer
    }

    #[must_use]
    pub const fn start(&self) -> CursorV1 {
        self.header.start
    }

    #[must_use]
    pub const fn end(&self) -> CursorV1 {
        self.footer.end
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        CHUNK_HEADER_V1_ENCODED_LEN
            + self.frames.iter().map(FrameV1::encoded_len).sum::<usize>()
            + CHUNK_FOOTER_V1_ENCODED_LEN
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

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_at_least("ChunkHeaderV1", encoded.len(), CHUNK_HEADER_V1_ENCODED_LEN)?;
        let header = ChunkHeaderV1::decode(&encoded[..CHUNK_HEADER_V1_ENCODED_LEN])?;
        let mut cursor = header.start;
        let mut offset = CHUNK_HEADER_V1_ENCODED_LEN;
        let mut frames = Vec::new();

        loop {
            let remaining = &encoded[offset..];
            require_at_least(
                "transfer chunk frame/footer magic",
                remaining.len(),
                FRAME_V1_MAGIC.len(),
            )?;
            if remaining.len() < CHUNK_FOOTER_V1_MAGIC.len()
                && CHUNK_FOOTER_V1_MAGIC.starts_with(remaining)
            {
                return Err(ProtocolError::Truncated {
                    context: "ChunkFooterV1.magic",
                    expected: CHUNK_FOOTER_V1_MAGIC.len(),
                    actual: remaining.len(),
                });
            }
            if remaining.starts_with(&CHUNK_FOOTER_V1_MAGIC) {
                if frames.is_empty() {
                    return Err(ProtocolError::InvalidTransferChunk {
                        reason: "a fetch chunk must contain at least one frame",
                    });
                }
                let footer = ChunkFooterV1::decode(remaining)?;
                footer.validate_at(cursor)?;
                return Ok(Self {
                    header,
                    frames,
                    footer,
                });
            }
            if !remaining.starts_with(&FRAME_V1_MAGIC) {
                return Err(ProtocolError::InvalidMagic {
                    context: "transfer chunk frame/footer",
                });
            }
            if frames.len() == MAX_CHUNK_RECORDS_V1 as usize {
                return Err(ProtocolError::InvalidTransferChunk {
                    reason: "frame count exceeds the global V1 limit",
                });
            }
            let (frame, end, consumed) = FrameV1::decode_prefix_after(remaining, cursor)?;
            cursor = end;
            frames.push(frame);
            offset = offset
                .checked_add(consumed)
                .ok_or(ProtocolError::IntegerOverflow {
                    field: "transfer_chunk_offset",
                })?;
        }
    }

    pub fn commit(&self) -> Result<TransferChunkCommitV1> {
        let encoded = self.encode();
        TransferChunkCommitV1::new(
            self.start(),
            self.end(),
            u64::try_from(encoded.len()).map_err(|_| ProtocolError::IntegerOverflow {
                field: "transfer_chunk_encoded_len",
            })?,
            Sha256::digest(&encoded).into(),
        )
    }

    pub fn decode_committed(
        encoded: &[u8],
        expected_stream_id: StreamId,
        commit: TransferChunkCommitV1,
    ) -> Result<Self> {
        let actual_len =
            u64::try_from(encoded.len()).map_err(|_| ProtocolError::IntegerOverflow {
                field: "transfer_chunk_encoded_len",
            })?;
        if actual_len != commit.encoded_len {
            return Err(ProtocolError::EncodedLengthMismatch {
                expected: commit.encoded_len,
                actual: actual_len,
            });
        }
        if <[u8; 32]>::from(Sha256::digest(encoded)) != commit.encoded_sha256 {
            return Err(ProtocolError::EncodedSha256Mismatch);
        }
        let chunk = Self::decode(encoded)?;
        if chunk.header.stream_id != expected_stream_id {
            return Err(ProtocolError::StreamMismatch {
                context: "TransferChunkV1",
            });
        }
        if chunk.start() != commit.start {
            return Err(ProtocolError::CursorMismatch {
                context: "TransferChunkCommitV1.start",
            });
        }
        if chunk.end() != commit.end {
            return Err(ProtocolError::CursorMismatch {
                context: "TransferChunkCommitV1.end",
            });
        }
        Ok(chunk)
    }

    pub fn validate_for_fetch(&self, stream_id: StreamId, fetch: &FetchRangeV1) -> Result<()> {
        if self.header.stream_id != stream_id {
            return Err(ProtocolError::StreamMismatch {
                context: "TransferChunkV1",
            });
        }
        if self.start().next_sequence() != fetch.first_sequence
            || self.end().next_sequence() != fetch.next_sequence
        {
            return Err(ProtocolError::InvalidFetchRange {
                reason: "chunk boundaries differ from the requested numeric range",
            });
        }
        Ok(())
    }
}

fn validate_limit(field: &'static str, actual: u64, min: u64, max: u64) -> Result<()> {
    if actual < min || actual > max {
        return Err(ProtocolError::InvalidSyncLimit {
            field,
            actual,
            min,
            max,
        });
    }
    Ok(())
}

fn validate_cursor_not_after(
    earlier: CursorV1,
    later: CursorV1,
    context: &'static str,
) -> Result<()> {
    if earlier.next_sequence() > later.next_sequence() {
        return Err(ProtocolError::InvalidCursorOrder { context });
    }
    if earlier.next_sequence() == later.next_sequence() && earlier != later {
        return Err(ProtocolError::CursorMismatch { context });
    }
    Ok(())
}

fn validate_initial_cursor_if_zero(cursor: CursorV1, stream: StreamHeaderV1) -> Result<()> {
    if cursor.next_sequence() == 0 && cursor != stream.initial_cursor() {
        return Err(ProtocolError::CursorMismatch {
            context: "stream initial cursor",
        });
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterGenesisHash, PrefixHash, ProducerConfigSha256, StreamManifestSha256};

    const CHUNK_SHA256: [u8; 32] = [
        0xb9, 0x69, 0xd9, 0x44, 0x66, 0x58, 0x2d, 0x1b, 0x32, 0x86, 0x40, 0x75, 0x1e, 0x5a, 0xb9,
        0xcd, 0xbc, 0xb3, 0x50, 0x6b, 0x83, 0x88, 0xb3, 0x5c, 0x82, 0xf2, 0xa9, 0xf1, 0xd6, 0x29,
        0xd0, 0x23,
    ];

    fn fixture_stream() -> StreamHeaderV1 {
        StreamHeaderV1::new(
            StreamId::new(core::array::from_fn(|index| index as u8)),
            ClusterGenesisHash::new(core::array::from_fn(|index| (index + 0x10) as u8)),
            2,
            1,
            ProducerConfigSha256::new(core::array::from_fn(|index| (index + 0x30) as u8)),
            StreamManifestSha256::new(core::array::from_fn(|index| (index + 0x50) as u8)),
        )
        .unwrap()
    }

    fn fixture_chunk() -> TransferChunkV1 {
        let stream = fixture_stream();
        let start = stream.initial_cursor();
        let first = FrameV1::new(start, b"abc".to_vec()).unwrap();
        let second_start = first.validate_after(start).unwrap();
        let second = FrameV1::new(second_start, vec![0, 1, 2, 255]).unwrap();
        TransferChunkV1::new(
            ChunkHeaderV1::new(stream.stream_id(), start),
            vec![first, second],
        )
        .unwrap()
    }

    fn fixture_resume(max_chunk_records: u32) -> ResumeV1 {
        let stream = fixture_stream();
        let start = stream.initial_cursor();
        let end = fixture_chunk().end();
        ResumeV1::new(
            stream,
            SessionId::new([0x77; 16]),
            start,
            start,
            end,
            MAX_SYNC_RECORD_BYTES_V1,
            max_chunk_records,
            MAX_PARALLEL_FETCHES_V1,
        )
        .unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn chunk_header_footer_and_body_are_golden() {
        let chunk = fixture_chunk();
        assert_eq!(
            hex(&chunk.header().encode()),
            concat!(
                "4849564543484b31",
                "000102030405060708090a0b0c0d0e0f",
                "0000000000000000",
                "137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332"
            )
        );
        assert_eq!(
            hex(&chunk.footer().encode()),
            concat!(
                "48495645454e4431",
                "0000000000000002",
                "a28b0721a9a88a31f112dfaabfc69622ac7d302fe41a21cfd0de0a35061b359a"
            )
        );
        let encoded = chunk.encode();
        assert_eq!(encoded.len(), 207);
        assert_eq!(TransferChunkV1::decode(&encoded), Ok(chunk));
    }

    #[test]
    fn committed_chunk_checks_digest_length_stream_and_boundaries() {
        let chunk = fixture_chunk();
        let encoded = chunk.encode();
        let commit = chunk.commit().unwrap();
        assert_eq!(commit.encoded_sha256, CHUNK_SHA256);
        assert_eq!(
            TransferChunkV1::decode_committed(&encoded, fixture_stream().stream_id(), commit),
            Ok(chunk.clone())
        );

        let wrong_length = TransferChunkCommitV1::new(
            commit.start,
            commit.end,
            commit.encoded_len + 1,
            commit.encoded_sha256,
        )
        .unwrap();
        assert!(matches!(
            TransferChunkV1::decode_committed(&encoded, fixture_stream().stream_id(), wrong_length),
            Err(ProtocolError::EncodedLengthMismatch { .. })
        ));

        let mut wrong_sha = commit.encoded_sha256;
        wrong_sha[0] ^= 1;
        let wrong_digest =
            TransferChunkCommitV1::new(commit.start, commit.end, commit.encoded_len, wrong_sha)
                .unwrap();
        assert_eq!(
            TransferChunkV1::decode_committed(&encoded, fixture_stream().stream_id(), wrong_digest),
            Err(ProtocolError::EncodedSha256Mismatch)
        );

        let wrong_stream = StreamId::new([0xff; 16]);
        assert!(matches!(
            TransferChunkV1::decode_committed(&encoded, wrong_stream, commit),
            Err(ProtocolError::StreamMismatch { .. })
        ));

        let wrong_end = TransferChunkCommitV1::new(
            commit.start,
            CursorV1::new(commit.end.next_sequence() + 1, PrefixHash::new([9; 32])),
            commit.encoded_len,
            commit.encoded_sha256,
        )
        .unwrap();
        assert!(matches!(
            TransferChunkV1::decode_committed(&encoded, fixture_stream().stream_id(), wrong_end),
            Err(ProtocolError::CursorMismatch { .. })
        ));
    }

    #[test]
    fn chunk_rejects_empty_truncated_trailing_and_chain_corruption() {
        let chunk = fixture_chunk();
        assert!(matches!(
            TransferChunkV1::new(chunk.header(), Vec::new()),
            Err(ProtocolError::InvalidTransferChunk { .. })
        ));

        let encoded = chunk.encode();
        assert!(matches!(
            TransferChunkV1::decode(&encoded[..encoded.len() - 1]),
            Err(ProtocolError::Truncated { .. })
        ));

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            TransferChunkV1::decode(&trailing),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let partial_footer_end = encoded.len() - CHUNK_FOOTER_V1_ENCODED_LEN + 5;
        assert!(matches!(
            TransferChunkV1::decode(&encoded[..partial_footer_end]),
            Err(ProtocolError::Truncated {
                context: "ChunkFooterV1.magic",
                ..
            })
        ));

        let mut bad_prefix = encoded;
        let first_frame_last = CHUNK_HEADER_V1_ENCODED_LEN + chunk.frames()[0].encoded_len() - 1;
        bad_prefix[first_frame_last] ^= 1;
        assert_eq!(
            TransferChunkV1::decode(&bad_prefix),
            Err(ProtocolError::PrefixMismatch)
        );
    }

    #[test]
    fn frame_declared_limit_is_rejected_inside_chunk_before_allocation() {
        let chunk = fixture_chunk();
        let mut malicious = chunk.header().encode().to_vec();
        malicious.extend_from_slice(&FRAME_V1_MAGIC);
        malicious.extend_from_slice(&(crate::MAX_RECORD_PAYLOAD_BYTES + 1).to_be_bytes());
        malicious.extend_from_slice(&[0; 32]);
        assert!(matches!(
            TransferChunkV1::decode(&malicious),
            Err(ProtocolError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn resume_and_fetch_enforce_global_and_negotiated_limits() {
        let resume = fixture_resume(1);
        assert!(matches!(
            ResumeV1::new(
                resume.stream,
                resume.session_id,
                resume.first_available,
                resume.bulk_start,
                resume.cutover,
                0,
                1,
                1,
            ),
            Err(ProtocolError::InvalidSyncLimit { .. })
        ));
        assert!(matches!(
            ResumeV1::new(
                resume.stream,
                resume.session_id,
                resume.first_available,
                resume.bulk_start,
                resume.cutover,
                MAX_SYNC_RECORD_BYTES_V1,
                1,
                MAX_PARALLEL_FETCHES_V1 + 1,
            ),
            Err(ProtocolError::InvalidSyncLimit { .. })
        ));
        assert!(matches!(
            ResumeV1::new(
                resume.stream,
                resume.session_id,
                resume.first_available,
                resume.bulk_start,
                resume.cutover,
                MAX_SYNC_RECORD_BYTES_V1,
                MAX_CHUNK_RECORDS_V1 + 1,
                1,
            ),
            Err(ProtocolError::InvalidSyncLimit { .. })
        ));

        let fetch = FetchRangeV1::new(
            resume.session_id,
            resume.cutover,
            resume.bulk_start.next_sequence(),
            resume.cutover.next_sequence(),
        )
        .unwrap();
        assert!(matches!(
            fetch.validate_against_resume(&resume),
            Err(ProtocolError::InvalidFetchRange { .. })
        ));
        let wrong_session =
            FetchRangeV1::new(SessionId::new([0x88; 16]), resume.cutover, 0, 1).unwrap();
        assert_eq!(
            wrong_session.validate_against_resume(&resume),
            Err(ProtocolError::SessionMismatch)
        );
        assert!(matches!(
            FetchRangeV1::new(resume.session_id, resume.cutover, 1, 1),
            Err(ProtocolError::InvalidFetchRange { .. })
        ));
    }

    #[test]
    fn open_none_is_exactly_stream_p_zero_and_error_enum_is_closed() {
        let stream = fixture_stream();
        let open = OpenV1::new(
            stream.stream_id(),
            DeletionAuthorizingStoreId::new([3; 16]),
            None,
        );
        assert_eq!(
            open.effective_protected_cursor(stream),
            Ok(stream.initial_cursor())
        );
        let wrong_initial = OpenV1::new(
            stream.stream_id(),
            DeletionAuthorizingStoreId::new([3; 16]),
            Some(CursorV1::new(0, PrefixHash::new([9; 32]))),
        );
        assert!(matches!(
            wrong_initial.effective_protected_cursor(stream),
            Err(ProtocolError::CursorMismatch { .. })
        ));
        for value in 1..=9 {
            assert_eq!(HiveSyncErrorCodeV1::try_from(value).unwrap() as i32, value);
        }
        assert_eq!(
            HiveSyncErrorCodeV1::try_from(0),
            Err(ProtocolError::UnknownHiveSyncErrorCode { value: 0 })
        );
        assert_eq!(
            HiveSyncErrorCodeV1::try_from(10),
            Err(ProtocolError::UnknownHiveSyncErrorCode { value: 10 })
        );
        assert_eq!(
            HiveSyncErrorCodeV1::try_from(-1_i32),
            Err(ProtocolError::UnknownHiveSyncErrorCode { value: -1 })
        );
    }
}
