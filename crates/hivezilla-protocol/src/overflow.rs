use std::fmt::Write as _;

use sha2::{Digest, Sha256};

use crate::{
    CURSOR_V1_ENCODED_LEN, CursorV1, MIN_SEALED_SEGMENT_V1_ENCODED_LEN, ProtocolError, Result,
    SealedSegmentV1, StreamId, StreamManifestSha256,
};

pub const MAX_OBJECT_VERSION_BYTES: u64 = 4_096;
pub const OVERFLOW_OBJECT_V1_FIXED_ENCODED_LEN: usize =
    StreamId::LENGTH + 2 * CURSOR_V1_ENCODED_LEN + 8 + 32 + 1;
pub const MAX_OVERFLOW_OBJECT_V1_ENCODED_LEN: usize =
    OVERFLOW_OBJECT_V1_FIXED_ENCODED_LEN + 8 + MAX_OBJECT_VERSION_BYTES as usize;
pub const OVERFLOW_OBJECT_KEY_V1_LEN: usize = 153;
pub const OVERFLOW_MANIFEST_KEY_V1_LEN: usize = 132;

const OVERFLOW_KEY_PREFIX: &str = "hive-overflow/v1/";

/// Canonical local metadata for one verified, immutable overflow object.
///
/// The optional provider version is a receipt only. It is deliberately absent
/// from object identity and deterministic key derivation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OverflowObjectV1 {
    stream_id: StreamId,
    start: CursorV1,
    end: CursorV1,
    encoded_len: u64,
    encoded_sha256: [u8; 32],
    object_version: Option<Vec<u8>>,
}

impl OverflowObjectV1 {
    pub fn new(
        stream_id: StreamId,
        start: CursorV1,
        end: CursorV1,
        encoded_len: u64,
        encoded_sha256: [u8; 32],
        object_version: Option<Vec<u8>>,
    ) -> Result<Self> {
        validate_range(start, end)?;
        validate_segment_len(encoded_len)?;
        validate_object_version(object_version.as_deref())?;
        Ok(Self {
            stream_id,
            start,
            end,
            encoded_len,
            encoded_sha256,
            object_version,
        })
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
    pub fn object_version(&self) -> Option<&[u8]> {
        self.object_version.as_deref()
    }

    #[must_use]
    pub fn metadata_encoded_len(&self) -> usize {
        OVERFLOW_OBJECT_V1_FIXED_ENCODED_LEN
            + self
                .object_version
                .as_ref()
                .map_or(0, |version| 8 + version.len())
    }

    /// Encodes fields in specification order. A present opaque version uses a
    /// `1 || len_be_u64 || bytes` option; absence is the one byte `0`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.metadata_encoded_len());
        encoded.extend_from_slice(self.stream_id.as_bytes());
        encoded.extend_from_slice(&self.start.fixed_encode());
        encoded.extend_from_slice(&self.end.fixed_encode());
        encoded.extend_from_slice(&self.encoded_len.to_be_bytes());
        encoded.extend_from_slice(&self.encoded_sha256);
        if let Some(version) = &self.object_version {
            encoded.push(1);
            encoded.extend_from_slice(&(version.len() as u64).to_be_bytes());
            encoded.extend_from_slice(version);
        } else {
            encoded.push(0);
        }
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_at_least(
            "OverflowObjectV1",
            encoded.len(),
            OVERFLOW_OBJECT_V1_FIXED_ENCODED_LEN,
        )?;
        let mut offset = 0;

        let stream_id = StreamId::try_from(take(
            encoded,
            &mut offset,
            StreamId::LENGTH,
            "overflow_stream_id",
        )?)?;
        let start = CursorV1::decode(take(
            encoded,
            &mut offset,
            CURSOR_V1_ENCODED_LEN,
            "overflow_start",
        )?)?;
        let end = CursorV1::decode(take(
            encoded,
            &mut offset,
            CURSOR_V1_ENCODED_LEN,
            "overflow_end",
        )?)?;
        let encoded_len = u64::from_be_bytes(
            take(encoded, &mut offset, 8, "overflow_encoded_len")?
                .try_into()
                .expect("fixed slice"),
        );
        let encoded_sha256: [u8; 32] = take(encoded, &mut offset, 32, "overflow_encoded_sha256")?
            .try_into()
            .expect("fixed slice");
        let option_tag = take(encoded, &mut offset, 1, "overflow_object_version")?[0];
        let object_version = match option_tag {
            0 => None,
            1 => {
                let length = u64::from_be_bytes(
                    take(encoded, &mut offset, 8, "overflow_object_version_len")?
                        .try_into()
                        .expect("fixed slice"),
                );
                validate_object_version_len(length)?;
                let length =
                    usize::try_from(length).map_err(|_| ProtocolError::IntegerOverflow {
                        field: "overflow_object_version_len",
                    })?;
                Some(take(encoded, &mut offset, length, "overflow_object_version")?.to_vec())
            }
            value => {
                return Err(ProtocolError::InvalidOptionTag {
                    field: "overflow_object_version",
                    value,
                });
            }
        };
        if offset != encoded.len() {
            return Err(ProtocolError::TrailingBytes {
                context: "OverflowObjectV1",
                count: encoded.len() - offset,
            });
        }
        Self::new(
            stream_id,
            start,
            end,
            encoded_len,
            encoded_sha256,
            object_version,
        )
    }

    /// Deterministic immutable key; provider version is intentionally ignored.
    #[must_use]
    pub fn object_key(&self) -> String {
        let mut key = String::with_capacity(OVERFLOW_OBJECT_KEY_V1_LEN);
        key.push_str(OVERFLOW_KEY_PREFIX);
        push_hex(&mut key, self.stream_id.as_bytes());
        key.push('/');
        write!(key, "{:016x}", self.start.next_sequence()).expect("writing to String cannot fail");
        key.push('-');
        write!(key, "{:016x}", self.end.next_sequence()).expect("writing to String cannot fail");
        key.push('-');
        push_hex(&mut key, &self.encoded_sha256);
        key.push_str(".hseg");
        debug_assert_eq!(key.len(), OVERFLOW_OBJECT_KEY_V1_LEN);
        key
    }

    /// Validates exact object bytes against all metadata and the complete
    /// journal prefix chain. It performs no storage I/O.
    pub fn validate_segment(&self, encoded: &[u8]) -> Result<SealedSegmentV1> {
        let actual_len =
            u64::try_from(encoded.len()).map_err(|_| ProtocolError::IntegerOverflow {
                field: "overflow_segment_encoded_len",
            })?;
        if actual_len != self.encoded_len {
            return Err(ProtocolError::EncodedLengthMismatch {
                expected: self.encoded_len,
                actual: actual_len,
            });
        }
        let actual_sha256: [u8; 32] = Sha256::digest(encoded).into();
        if actual_sha256 != self.encoded_sha256 {
            return Err(ProtocolError::EncodedSha256Mismatch);
        }

        let segment = SealedSegmentV1::decode(encoded)?;
        if segment.header().stream().stream_id() != self.stream_id {
            return Err(ProtocolError::StreamMismatch {
                context: "OverflowObjectV1.segment",
            });
        }
        if segment.start() != self.start {
            return Err(ProtocolError::CursorMismatch {
                context: "OverflowObjectV1.start",
            });
        }
        if segment.end() != self.end {
            return Err(ProtocolError::CursorMismatch {
                context: "OverflowObjectV1.end",
            });
        }
        Ok(segment)
    }
}

/// Deterministic immutable key for the stream's exact canonical manifest.
#[must_use]
pub fn overflow_manifest_key(stream_id: StreamId, manifest_sha256: StreamManifestSha256) -> String {
    let mut key = String::with_capacity(OVERFLOW_MANIFEST_KEY_V1_LEN);
    key.push_str(OVERFLOW_KEY_PREFIX);
    push_hex(&mut key, stream_id.as_bytes());
    key.push_str("/manifest-");
    push_hex(&mut key, manifest_sha256.as_bytes());
    key.push_str(".manifest");
    debug_assert_eq!(key.len(), OVERFLOW_MANIFEST_KEY_V1_LEN);
    key
}

fn validate_range(start: CursorV1, end: CursorV1) -> Result<()> {
    if end.next_sequence() < start.next_sequence() {
        return Err(ProtocolError::ReversedOverflowRange);
    }
    if end.next_sequence() == start.next_sequence() && end != start {
        return Err(ProtocolError::InvalidEmptyOverflowRange);
    }
    Ok(())
}

fn validate_segment_len(encoded_len: u64) -> Result<()> {
    let min = MIN_SEALED_SEGMENT_V1_ENCODED_LEN as u64;
    if encoded_len < min {
        return Err(ProtocolError::EncodedSegmentTooSmall {
            actual: encoded_len,
            min,
        });
    }
    Ok(())
}

fn validate_object_version(version: Option<&[u8]>) -> Result<()> {
    if let Some(version) = version {
        validate_object_version_len(version.len() as u64)?;
    }
    Ok(())
}

fn validate_object_version_len(length: u64) -> Result<()> {
    if length == 0 {
        return Err(ProtocolError::EmptyObjectVersion);
    }
    if length > MAX_OBJECT_VERSION_BYTES {
        return Err(ProtocolError::ObjectVersionTooLarge {
            actual: length,
            max: MAX_OBJECT_VERSION_BYTES,
        });
    }
    Ok(())
}

fn take<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    length: usize,
    context: &'static str,
) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(length)
        .ok_or(ProtocolError::IntegerOverflow { field: context })?;
    if end > encoded.len() {
        return Err(ProtocolError::Truncated {
            context,
            expected: end,
            actual: encoded.len(),
        });
    }
    let bytes = &encoded[*offset..end];
    *offset = end;
    Ok(bytes)
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

fn push_hex(target: &mut String, bytes: &[u8]) {
    for byte in bytes {
        write!(target, "{byte:02x}").expect("writing to String cannot fail");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterGenesisHash, FrameV1, PrefixHash, ProducerConfigSha256, SegmentHeaderV1,
        StreamHeaderV1,
    };

    const SEGMENT_SHA256: [u8; 32] = [
        0xe5, 0x8f, 0xe2, 0x4b, 0xf1, 0xa7, 0x79, 0x8c, 0x85, 0x6c, 0xde, 0xcd, 0x2d, 0x7f, 0x95,
        0x9f, 0x80, 0x86, 0x97, 0xd7, 0xdf, 0xcd, 0x7f, 0x06, 0xc8, 0x74, 0x1c, 0xcd, 0xdb, 0x0c,
        0x34, 0x98,
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

    fn fixture_metadata(version: Option<Vec<u8>>) -> OverflowObjectV1 {
        let segment = fixture_segment();
        OverflowObjectV1::new(
            segment.header().stream().stream_id(),
            segment.start(),
            segment.end(),
            segment.encoded_len() as u64,
            SEGMENT_SHA256,
            version,
        )
        .unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn overflow_metadata_is_golden_and_round_trips() {
        let metadata = fixture_metadata(Some(b"v1".to_vec()));
        let encoded = metadata.encode();
        assert_eq!(
            hex(&encoded),
            concat!(
                "000102030405060708090a0b0c0d0e0f",
                "0000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332",
                "0000000000000002a28b0721a9a88a31f112dfaabfc69622ac7d302fe41a21cfd0de0a35061b359a",
                "0000000000000131",
                "e58fe24bf1a7798c856cdecd2d7f959f808697d7dfcd7f06c8741ccddb0c3498",
                "01",
                "0000000000000002",
                "7631"
            )
        );
        assert_eq!(OverflowObjectV1::decode(&encoded), Ok(metadata));

        let without_version = fixture_metadata(None);
        assert_eq!(
            without_version.encode().len(),
            OVERFLOW_OBJECT_V1_FIXED_ENCODED_LEN
        );
        assert_eq!(
            OverflowObjectV1::decode(&without_version.encode()),
            Ok(without_version)
        );
    }

    #[test]
    fn overflow_keys_are_exact_lowercase_golden_values() {
        let metadata = fixture_metadata(Some(b"ignored-by-key".to_vec()));
        assert_eq!(
            metadata.object_key(),
            concat!(
                "hive-overflow/v1/000102030405060708090a0b0c0d0e0f/",
                "0000000000000000-0000000000000002-",
                "e58fe24bf1a7798c856cdecd2d7f959f808697d7dfcd7f06c8741ccddb0c3498.hseg"
            )
        );
        assert_eq!(metadata.object_key().len(), OVERFLOW_OBJECT_KEY_V1_LEN);
        assert_eq!(
            overflow_manifest_key(
                fixture_stream().stream_id(),
                fixture_stream().stream_manifest_sha256()
            ),
            concat!(
                "hive-overflow/v1/000102030405060708090a0b0c0d0e0f/manifest-",
                "505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f.manifest"
            )
        );
    }

    #[test]
    fn metadata_validates_the_complete_segment() {
        let segment = fixture_segment();
        let encoded = segment.encode();
        assert_eq!(<[u8; 32]>::from(Sha256::digest(&encoded)), SEGMENT_SHA256);
        let metadata = fixture_metadata(None);
        assert_eq!(metadata.validate_segment(&encoded), Ok(segment));

        let mut wrong_length = metadata.clone();
        wrong_length.encoded_len += 1;
        assert!(matches!(
            wrong_length.validate_segment(&encoded),
            Err(ProtocolError::EncodedLengthMismatch { .. })
        ));

        let mut wrong_digest = metadata.clone();
        wrong_digest.encoded_sha256[0] ^= 1;
        assert_eq!(
            wrong_digest.validate_segment(&encoded),
            Err(ProtocolError::EncodedSha256Mismatch)
        );

        let wrong_stream = OverflowObjectV1::new(
            StreamId::new([0xff; 16]),
            metadata.start(),
            metadata.end(),
            metadata.encoded_len(),
            *metadata.encoded_sha256(),
            None,
        )
        .unwrap();
        assert!(matches!(
            wrong_stream.validate_segment(&encoded),
            Err(ProtocolError::StreamMismatch { .. })
        ));
    }

    #[test]
    fn overflow_decoder_rejects_noncanonical_and_unbounded_versions() {
        let metadata = fixture_metadata(None);
        let mut trailing = metadata.encode();
        trailing.push(0);
        assert!(matches!(
            OverflowObjectV1::decode(&trailing),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let mut invalid_tag = metadata.encode();
        *invalid_tag.last_mut().unwrap() = 2;
        assert!(matches!(
            OverflowObjectV1::decode(&invalid_tag),
            Err(ProtocolError::InvalidOptionTag { .. })
        ));

        let option_offset = OVERFLOW_OBJECT_V1_FIXED_ENCODED_LEN - 1;
        let mut empty_version = metadata.encode();
        empty_version[option_offset] = 1;
        empty_version.extend_from_slice(&0_u64.to_be_bytes());
        assert_eq!(
            OverflowObjectV1::decode(&empty_version),
            Err(ProtocolError::EmptyObjectVersion)
        );

        let mut oversized = metadata.encode();
        oversized[option_offset] = 1;
        oversized.extend_from_slice(&(MAX_OBJECT_VERSION_BYTES + 1).to_be_bytes());
        assert_eq!(
            OverflowObjectV1::decode(&oversized),
            Err(ProtocolError::ObjectVersionTooLarge {
                actual: MAX_OBJECT_VERSION_BYTES + 1,
                max: MAX_OBJECT_VERSION_BYTES,
            })
        );
    }

    #[test]
    fn impossible_ranges_and_too_small_segments_are_rejected() {
        let start = CursorV1::new(2, PrefixHash::new([2; 32]));
        let before = CursorV1::new(1, PrefixHash::new([1; 32]));
        assert_eq!(
            OverflowObjectV1::new(
                StreamId::new([0; 16]),
                start,
                before,
                MIN_SEALED_SEGMENT_V1_ENCODED_LEN as u64,
                [0; 32],
                None,
            ),
            Err(ProtocolError::ReversedOverflowRange)
        );
        assert_eq!(
            OverflowObjectV1::new(
                StreamId::new([0; 16]),
                start,
                CursorV1::new(2, PrefixHash::new([3; 32])),
                MIN_SEALED_SEGMENT_V1_ENCODED_LEN as u64,
                [0; 32],
                None,
            ),
            Err(ProtocolError::InvalidEmptyOverflowRange)
        );
        assert!(matches!(
            OverflowObjectV1::new(
                StreamId::new([0; 16]),
                start,
                start,
                MIN_SEALED_SEGMENT_V1_ENCODED_LEN as u64 - 1,
                [0; 32],
                None,
            ),
            Err(ProtocolError::EncodedSegmentTooSmall { .. })
        ));
        assert_eq!(
            OverflowObjectV1::new(
                StreamId::new([0; 16]),
                start,
                start,
                MIN_SEALED_SEGMENT_V1_ENCODED_LEN as u64,
                [0; 32],
                Some(Vec::new()),
            ),
            Err(ProtocolError::EmptyObjectVersion)
        );
    }
}
