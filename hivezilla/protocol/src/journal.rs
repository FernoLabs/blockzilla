use crate::{
    CursorV1, PrefixHash, ProtocolError, Result, STREAM_HEADER_V1_ENCODED_LEN, StreamHeaderV1,
    validate_record_payload_len,
};

pub const SEGMENT_HEADER_V1_MAGIC: [u8; 8] = *b"HIVESEG1";
pub const FRAME_V1_MAGIC: [u8; 4] = *b"HFR1";
pub const SEGMENT_FOOTER_V1_MAGIC: [u8; 4] = *b"HEND";

pub const SEGMENT_HEADER_V1_ENCODED_LEN: usize =
    SEGMENT_HEADER_V1_MAGIC.len() + STREAM_HEADER_V1_ENCODED_LEN + 8 + PrefixHash::LENGTH;
pub const FRAME_V1_FIXED_ENCODED_LEN: usize = FRAME_V1_MAGIC.len() + 8 + PrefixHash::LENGTH;
pub const SEGMENT_FOOTER_V1_ENCODED_LEN: usize =
    SEGMENT_FOOTER_V1_MAGIC.len() + 8 + PrefixHash::LENGTH;
pub const MIN_SEALED_SEGMENT_V1_ENCODED_LEN: usize =
    SEGMENT_HEADER_V1_ENCODED_LEN + SEGMENT_FOOTER_V1_ENCODED_LEN;

/// The fixed prefix of one Hivezilla V1 journal segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentHeaderV1 {
    stream: StreamHeaderV1,
    start: CursorV1,
}

impl SegmentHeaderV1 {
    pub fn new(stream: StreamHeaderV1, start: CursorV1) -> Result<Self> {
        if start.next_sequence() == 0 && start != stream.initial_cursor() {
            return Err(ProtocolError::CursorMismatch {
                context: "SegmentHeaderV1.start",
            });
        }
        Ok(Self { stream, start })
    }

    #[must_use]
    pub const fn stream(&self) -> StreamHeaderV1 {
        self.stream
    }

    #[must_use]
    pub const fn start(&self) -> CursorV1 {
        self.start
    }

    #[must_use]
    pub const fn first_sequence(&self) -> u64 {
        self.start.next_sequence()
    }

    #[must_use]
    pub const fn previous_prefix_hash(&self) -> PrefixHash {
        self.start.prefix_hash()
    }

    #[must_use]
    pub fn encode(&self) -> [u8; SEGMENT_HEADER_V1_ENCODED_LEN] {
        let mut encoded = [0_u8; SEGMENT_HEADER_V1_ENCODED_LEN];
        encoded[..8].copy_from_slice(&SEGMENT_HEADER_V1_MAGIC);
        encoded[8..8 + STREAM_HEADER_V1_ENCODED_LEN].copy_from_slice(&self.stream.fixed_encode());
        let sequence_offset = 8 + STREAM_HEADER_V1_ENCODED_LEN;
        encoded[sequence_offset..sequence_offset + 8]
            .copy_from_slice(&self.start.next_sequence().to_be_bytes());
        encoded[sequence_offset + 8..].copy_from_slice(self.start.prefix_hash().as_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_len(
            "SegmentHeaderV1",
            encoded.len(),
            SEGMENT_HEADER_V1_ENCODED_LEN,
        )?;
        require_magic(
            "SegmentHeaderV1",
            &encoded[..SEGMENT_HEADER_V1_MAGIC.len()],
            &SEGMENT_HEADER_V1_MAGIC,
        )?;
        let stream_start = SEGMENT_HEADER_V1_MAGIC.len();
        let stream_end = stream_start + STREAM_HEADER_V1_ENCODED_LEN;
        let stream = StreamHeaderV1::decode(&encoded[stream_start..stream_end])?;
        let first_sequence = u64::from_be_bytes(
            encoded[stream_end..stream_end + 8]
                .try_into()
                .expect("fixed slice"),
        );
        let previous_prefix_hash = PrefixHash::try_from(&encoded[stream_end + 8..])?;
        Self::new(stream, CursorV1::new(first_sequence, previous_prefix_hash))
    }

    /// Checks this header against the stream and chain boundary selected by
    /// trusted surrounding metadata.
    pub fn validate_at(&self, stream: StreamHeaderV1, start: CursorV1) -> Result<()> {
        if self.stream != stream {
            return Err(ProtocolError::StreamMismatch {
                context: "SegmentHeaderV1",
            });
        }
        if self.start != start {
            return Err(ProtocolError::CursorMismatch {
                context: "SegmentHeaderV1.start",
            });
        }
        Ok(())
    }
}

/// One journal record frame. Its sequence is inferred from its predecessor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameV1 {
    payload: Vec<u8>,
    prefix_hash: PrefixHash,
}

impl FrameV1 {
    /// Constructs the only frame that can validly follow `previous`.
    pub fn new(previous: CursorV1, payload: Vec<u8>) -> Result<Self> {
        let end = previous.advance(&payload)?;
        Ok(Self {
            payload,
            prefix_hash: end.prefix_hash(),
        })
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    #[must_use]
    pub const fn prefix_hash(&self) -> PrefixHash {
        self.prefix_hash
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        FRAME_V1_FIXED_ENCODED_LEN + self.payload.len()
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.encoded_len());
        encoded.extend_from_slice(&FRAME_V1_MAGIC);
        encoded.extend_from_slice(&(self.payload.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        encoded.extend_from_slice(self.prefix_hash.as_bytes());
        encoded
    }

    /// Decodes exactly one frame and proves it against `previous`.
    pub fn decode_after(encoded: &[u8], previous: CursorV1) -> Result<Self> {
        let (frame, _, consumed) = Self::decode_prefix_after(encoded, previous)?;
        if consumed != encoded.len() {
            return Err(ProtocolError::TrailingBytes {
                context: "FrameV1",
                count: encoded.len() - consumed,
            });
        }
        Ok(frame)
    }

    /// Revalidates this frame against its exact predecessor and returns its end.
    pub fn validate_after(&self, previous: CursorV1) -> Result<CursorV1> {
        let expected = previous.advance(&self.payload)?;
        if self.prefix_hash != expected.prefix_hash() {
            return Err(ProtocolError::PrefixMismatch);
        }
        Ok(expected)
    }

    pub(crate) fn decode_prefix_after(
        encoded: &[u8],
        previous: CursorV1,
    ) -> Result<(Self, CursorV1, usize)> {
        require_at_least("FrameV1.magic", encoded.len(), FRAME_V1_MAGIC.len())?;
        require_magic("FrameV1", &encoded[..FRAME_V1_MAGIC.len()], &FRAME_V1_MAGIC)?;
        let length_end = FRAME_V1_MAGIC.len() + 8;
        require_at_least("FrameV1.payload_len", encoded.len(), length_end)?;
        let payload_len = u64::from_be_bytes(
            encoded[FRAME_V1_MAGIC.len()..length_end]
                .try_into()
                .expect("fixed slice"),
        );
        // This must happen before converting the length or allocating payload.
        validate_record_payload_len(payload_len)?;
        let payload_len = usize::try_from(payload_len).expect("V1 payload bound fits usize");
        let frame_len = FRAME_V1_FIXED_ENCODED_LEN
            .checked_add(payload_len)
            .expect("V1 payload bound plus fixed frame length fits usize");
        require_at_least("FrameV1", encoded.len(), frame_len)?;

        let payload_end = length_end + payload_len;
        let payload = encoded[length_end..payload_end].to_vec();
        let prefix_hash = PrefixHash::try_from(&encoded[payload_end..frame_len])?;
        let frame = Self {
            payload,
            prefix_hash,
        };
        let end = frame.validate_after(previous)?;
        Ok((frame, end, frame_len))
    }
}

/// The fixed trailer that seals one immutable journal segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentFooterV1 {
    end: CursorV1,
}

impl SegmentFooterV1 {
    #[must_use]
    pub const fn new(end: CursorV1) -> Self {
        Self { end }
    }

    #[must_use]
    pub const fn end(&self) -> CursorV1 {
        self.end
    }

    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.end.next_sequence()
    }

    #[must_use]
    pub const fn prefix_hash(&self) -> PrefixHash {
        self.end.prefix_hash()
    }

    #[must_use]
    pub fn encode(&self) -> [u8; SEGMENT_FOOTER_V1_ENCODED_LEN] {
        let mut encoded = [0_u8; SEGMENT_FOOTER_V1_ENCODED_LEN];
        encoded[..4].copy_from_slice(&SEGMENT_FOOTER_V1_MAGIC);
        encoded[4..12].copy_from_slice(&self.end.next_sequence().to_be_bytes());
        encoded[12..].copy_from_slice(self.end.prefix_hash().as_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_len(
            "SegmentFooterV1",
            encoded.len(),
            SEGMENT_FOOTER_V1_ENCODED_LEN,
        )?;
        require_magic(
            "SegmentFooterV1",
            &encoded[..SEGMENT_FOOTER_V1_MAGIC.len()],
            &SEGMENT_FOOTER_V1_MAGIC,
        )?;
        let next_sequence = u64::from_be_bytes(encoded[4..12].try_into().expect("fixed slice"));
        let prefix_hash = PrefixHash::try_from(&encoded[12..])?;
        Ok(Self::new(CursorV1::new(next_sequence, prefix_hash)))
    }

    pub fn decode_at(encoded: &[u8], expected: CursorV1) -> Result<Self> {
        let footer = Self::decode(encoded)?;
        footer.validate_at(expected)?;
        Ok(footer)
    }

    pub fn validate_at(&self, expected: CursorV1) -> Result<()> {
        if self.end != expected {
            return Err(ProtocolError::CursorMismatch {
                context: "SegmentFooterV1.end",
            });
        }
        Ok(())
    }
}

/// A fully parsed sealed segment. This is a canonical byte container, not a
/// filesystem journal or recovery engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedSegmentV1 {
    header: SegmentHeaderV1,
    frames: Vec<FrameV1>,
    footer: SegmentFooterV1,
}

impl SealedSegmentV1 {
    pub fn new(header: SegmentHeaderV1, frames: Vec<FrameV1>) -> Result<Self> {
        let mut cursor = header.start();
        for frame in &frames {
            cursor = frame.validate_after(cursor)?;
        }
        Ok(Self {
            header,
            frames,
            footer: SegmentFooterV1::new(cursor),
        })
    }

    #[must_use]
    pub const fn header(&self) -> SegmentHeaderV1 {
        self.header
    }

    #[must_use]
    pub fn frames(&self) -> &[FrameV1] {
        &self.frames
    }

    #[must_use]
    pub const fn footer(&self) -> SegmentFooterV1 {
        self.footer
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
        SEGMENT_HEADER_V1_ENCODED_LEN
            + self.frames.iter().map(FrameV1::encoded_len).sum::<usize>()
            + SEGMENT_FOOTER_V1_ENCODED_LEN
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

    /// Parses a complete sealed segment, validates every inferred sequence and
    /// prefix hash, requires one final matching footer, and rejects trailing
    /// bytes. Deployment code must apply its smaller segment-byte limit before
    /// calling this parser.
    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_at_least(
            "SegmentHeaderV1",
            encoded.len(),
            SEGMENT_HEADER_V1_ENCODED_LEN,
        )?;
        let header = SegmentHeaderV1::decode(&encoded[..SEGMENT_HEADER_V1_ENCODED_LEN])?;
        let mut cursor = header.start();
        let mut offset = SEGMENT_HEADER_V1_ENCODED_LEN;
        let mut frames = Vec::new();

        loop {
            let remaining = &encoded[offset..];
            require_at_least(
                "sealed segment frame/footer magic",
                remaining.len(),
                SEGMENT_FOOTER_V1_MAGIC.len(),
            )?;
            if remaining.starts_with(&SEGMENT_FOOTER_V1_MAGIC) {
                let footer = SegmentFooterV1::decode_at(remaining, cursor)?;
                return Ok(Self {
                    header,
                    frames,
                    footer,
                });
            }
            if !remaining.starts_with(&FRAME_V1_MAGIC) {
                return Err(ProtocolError::InvalidMagic {
                    context: "sealed segment frame/footer",
                });
            }
            let (frame, end, consumed) = FrameV1::decode_prefix_after(remaining, cursor)?;
            cursor = end;
            frames.push(frame);
            offset = offset
                .checked_add(consumed)
                .ok_or(ProtocolError::IntegerOverflow {
                    field: "sealed_segment_offset",
                })?;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterGenesisHash, MAX_RECORD_PAYLOAD_BYTES, ProducerConfigSha256, StreamId,
        StreamManifestSha256,
    };

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

    fn fixture_segment() -> SealedSegmentV1 {
        let stream = fixture_stream();
        let start = stream.initial_cursor();
        let first = FrameV1::new(start, b"abc".to_vec()).unwrap();
        let second_start = first.validate_after(start).unwrap();
        let second = FrameV1::new(second_start, vec![0, 1, 2, 255]).unwrap();
        SealedSegmentV1::new(
            SegmentHeaderV1::new(stream, start).unwrap(),
            vec![first, second],
        )
        .unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn segment_parts_have_exact_golden_encodings() {
        let segment = fixture_segment();
        assert_eq!(
            hex(&segment.header().encode()),
            concat!(
                "4849564553454731",
                "000102030405060708090a0b0c0d0e0f",
                "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f",
                "000000020001",
                "303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f",
                "505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f",
                "0000000000000000",
                "137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332"
            )
        );
        assert_eq!(
            hex(&segment.frames()[0].encode()),
            concat!(
                "484652310000000000000003616263",
                "6917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583"
            )
        );
        assert_eq!(
            hex(&segment.footer().encode()),
            concat!(
                "48454e440000000000000002",
                "a28b0721a9a88a31f112dfaabfc69622ac7d302fe41a21cfd0de0a35061b359a"
            )
        );
    }

    #[test]
    fn sealed_segment_is_golden_and_round_trips() {
        let segment = fixture_segment();
        let encoded = segment.encode();
        assert_eq!(encoded.len(), 305);
        assert_eq!(SealedSegmentV1::decode(&encoded), Ok(segment));
    }

    #[test]
    fn exact_part_decoders_reject_truncation_and_trailing_bytes() {
        let segment = fixture_segment();
        let mut header = segment.header().encode().to_vec();
        assert!(matches!(
            SegmentHeaderV1::decode(&header[..header.len() - 1]),
            Err(ProtocolError::Truncated { .. })
        ));
        header.push(0);
        assert!(matches!(
            SegmentHeaderV1::decode(&header),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let start = segment.start();
        let mut frame = segment.frames()[0].encode();
        assert!(matches!(
            FrameV1::decode_after(&frame[..frame.len() - 1], start),
            Err(ProtocolError::Truncated { .. })
        ));
        frame.push(0);
        assert!(matches!(
            FrameV1::decode_after(&frame, start),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let mut footer = segment.footer().encode().to_vec();
        assert!(matches!(
            SegmentFooterV1::decode(&footer[..footer.len() - 1]),
            Err(ProtocolError::Truncated { .. })
        ));
        footer.push(0);
        assert!(matches!(
            SegmentFooterV1::decode(&footer),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));
    }

    #[test]
    fn declared_frame_limit_is_rejected_before_payload_allocation() {
        let mut malicious = Vec::new();
        malicious.extend_from_slice(&FRAME_V1_MAGIC);
        malicious.extend_from_slice(&(MAX_RECORD_PAYLOAD_BYTES + 1).to_be_bytes());
        malicious.extend_from_slice(&[0_u8; PrefixHash::LENGTH]);
        assert_eq!(
            FrameV1::decode_after(&malicious, fixture_stream().initial_cursor()),
            Err(ProtocolError::PayloadTooLarge {
                actual: MAX_RECORD_PAYLOAD_BYTES + 1,
                max: MAX_RECORD_PAYLOAD_BYTES,
            })
        );
    }

    #[test]
    fn segment_rejects_magic_chain_footer_and_trailing_corruption() {
        let segment = fixture_segment();

        let mut bad_header_magic = segment.encode();
        bad_header_magic[0] ^= 1;
        assert!(matches!(
            SealedSegmentV1::decode(&bad_header_magic),
            Err(ProtocolError::InvalidMagic { .. })
        ));

        let mut bad_frame_prefix = segment.encode();
        let first_frame_last =
            SEGMENT_HEADER_V1_ENCODED_LEN + segment.frames()[0].encoded_len() - 1;
        bad_frame_prefix[first_frame_last] ^= 1;
        assert_eq!(
            SealedSegmentV1::decode(&bad_frame_prefix),
            Err(ProtocolError::PrefixMismatch)
        );

        let mut bad_footer = segment.encode();
        let footer_sequence = bad_footer.len() - SEGMENT_FOOTER_V1_ENCODED_LEN + 4;
        bad_footer[footer_sequence + 7] ^= 1;
        assert!(matches!(
            SealedSegmentV1::decode(&bad_footer),
            Err(ProtocolError::CursorMismatch { .. })
        ));

        let mut trailing = segment.encode();
        trailing.push(0);
        assert!(matches!(
            SealedSegmentV1::decode(&trailing),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));
    }

    #[test]
    fn initial_header_requires_the_stream_initial_prefix() {
        let stream = fixture_stream();
        assert!(matches!(
            SegmentHeaderV1::new(stream, CursorV1::new(0, PrefixHash::new([0; 32]))),
            Err(ProtocolError::CursorMismatch { .. })
        ));
    }
}
