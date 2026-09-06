use sha2::{Digest, Sha256};

use crate::{
    ClusterGenesisHash, PrefixHash, ProducerConfigSha256, ProtocolError, Result, StreamId,
    StreamManifestSha256,
};

pub const STREAM_HEADER_V1_ENCODED_LEN: usize = 118;
pub const CURSOR_V1_ENCODED_LEN: usize = 40;
pub const RECORD_V1_FIXED_ENCODED_LEN: usize = 48;
pub const MAX_RECORD_PAYLOAD_BYTES: u64 = 134_217_728;
pub const MAX_RECORD_V1_ENCODED_LEN: u64 = MAX_RECORD_PAYLOAD_BYTES + 48;
pub const MIN_REGISTERED_PAYLOAD_FORMAT_V1: u32 = 1;
pub const MAX_REGISTERED_PAYLOAD_FORMAT_V1: u32 = 7;
pub const REGISTERED_PAYLOAD_FORMAT_VERSION_V1: u16 = 1;

const STREAM_PREFIX_DOMAIN: &[u8] = b"hive/v1/stream";
const RECORD_PREFIX_DOMAIN: &[u8] = b"hive/v1/record";

/// Immutable identity fields used to seed a Hivezilla V1 record prefix chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamHeaderV1 {
    stream_id: StreamId,
    cluster_genesis_hash: ClusterGenesisHash,
    payload_format: u32,
    payload_format_version: u16,
    producer_config_sha256: ProducerConfigSha256,
    stream_manifest_sha256: StreamManifestSha256,
}

impl StreamHeaderV1 {
    pub fn new(
        stream_id: StreamId,
        cluster_genesis_hash: ClusterGenesisHash,
        payload_format: u32,
        payload_format_version: u16,
        producer_config_sha256: ProducerConfigSha256,
        stream_manifest_sha256: StreamManifestSha256,
    ) -> Result<Self> {
        if payload_format == 0 {
            return Err(ProtocolError::InvalidPayloadFormat);
        }
        if payload_format > MAX_REGISTERED_PAYLOAD_FORMAT_V1 {
            return Err(ProtocolError::UnknownPayloadFormat {
                value: payload_format,
            });
        }
        if payload_format_version != REGISTERED_PAYLOAD_FORMAT_VERSION_V1 {
            return Err(ProtocolError::UnknownPayloadFormatVersion {
                payload_format,
                version: payload_format_version,
            });
        }
        Ok(Self {
            stream_id,
            cluster_genesis_hash,
            payload_format,
            payload_format_version,
            producer_config_sha256,
            stream_manifest_sha256,
        })
    }

    #[must_use]
    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    #[must_use]
    pub const fn cluster_genesis_hash(&self) -> ClusterGenesisHash {
        self.cluster_genesis_hash
    }

    #[must_use]
    pub const fn payload_format(&self) -> u32 {
        self.payload_format
    }

    #[must_use]
    pub const fn payload_format_version(&self) -> u16 {
        self.payload_format_version
    }

    #[must_use]
    pub const fn producer_config_sha256(&self) -> ProducerConfigSha256 {
        self.producer_config_sha256
    }

    #[must_use]
    pub const fn stream_manifest_sha256(&self) -> StreamManifestSha256 {
        self.stream_manifest_sha256
    }

    /// The normative fixed encoding used by `P(0)`.
    #[must_use]
    pub fn fixed_encode(&self) -> [u8; STREAM_HEADER_V1_ENCODED_LEN] {
        let mut encoded = [0_u8; STREAM_HEADER_V1_ENCODED_LEN];
        let mut offset = 0;

        copy_field(&mut encoded, &mut offset, self.stream_id.as_bytes());
        copy_field(
            &mut encoded,
            &mut offset,
            self.cluster_genesis_hash.as_bytes(),
        );
        copy_field(
            &mut encoded,
            &mut offset,
            &self.payload_format.to_be_bytes(),
        );
        copy_field(
            &mut encoded,
            &mut offset,
            &self.payload_format_version.to_be_bytes(),
        );
        copy_field(
            &mut encoded,
            &mut offset,
            self.producer_config_sha256.as_bytes(),
        );
        copy_field(
            &mut encoded,
            &mut offset,
            self.stream_manifest_sha256.as_bytes(),
        );
        debug_assert_eq!(offset, STREAM_HEADER_V1_ENCODED_LEN);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_len(
            "StreamHeaderV1",
            encoded.len(),
            STREAM_HEADER_V1_ENCODED_LEN,
        )?;

        let stream_id = StreamId::try_from(&encoded[0..16])?;
        let cluster_genesis_hash = ClusterGenesisHash::try_from(&encoded[16..48])?;
        let payload_format = u32::from_be_bytes(encoded[48..52].try_into().expect("fixed slice"));
        let payload_format_version =
            u16::from_be_bytes(encoded[52..54].try_into().expect("fixed slice"));
        let producer_config_sha256 = ProducerConfigSha256::try_from(&encoded[54..86])?;
        let stream_manifest_sha256 = StreamManifestSha256::try_from(&encoded[86..118])?;

        Self::new(
            stream_id,
            cluster_genesis_hash,
            payload_format,
            payload_format_version,
            producer_config_sha256,
            stream_manifest_sha256,
        )
    }

    #[must_use]
    pub fn initial_cursor(&self) -> CursorV1 {
        let mut hasher = Sha256::new();
        hasher.update(STREAM_PREFIX_DOMAIN);
        hasher.update(self.fixed_encode());
        CursorV1::new(0, PrefixHash::new(hasher.finalize().into()))
    }
}

/// A contiguous prefix `[0, next_sequence)` in one stream's record chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CursorV1 {
    next_sequence: u64,
    prefix_hash: PrefixHash,
}

impl CursorV1 {
    #[must_use]
    pub const fn new(next_sequence: u64, prefix_hash: PrefixHash) -> Self {
        Self {
            next_sequence,
            prefix_hash,
        }
    }

    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    #[must_use]
    pub const fn prefix_hash(&self) -> PrefixHash {
        self.prefix_hash
    }

    #[must_use]
    pub fn fixed_encode(&self) -> [u8; CURSOR_V1_ENCODED_LEN] {
        let mut encoded = [0_u8; CURSOR_V1_ENCODED_LEN];
        encoded[..8].copy_from_slice(&self.next_sequence.to_be_bytes());
        encoded[8..].copy_from_slice(self.prefix_hash.as_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_len("CursorV1", encoded.len(), CURSOR_V1_ENCODED_LEN)?;
        let next_sequence = u64::from_be_bytes(encoded[..8].try_into().expect("fixed slice"));
        let prefix_hash = PrefixHash::try_from(&encoded[8..])?;
        Ok(Self::new(next_sequence, prefix_hash))
    }

    /// Computes the cursor after appending `payload` as the next exact record.
    pub fn advance(&self, payload: &[u8]) -> Result<Self> {
        validate_record_payload_len(payload.len() as u64)?;
        let next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ProtocolError::SequenceOverflow)?;

        let mut hasher = Sha256::new();
        hasher.update(RECORD_PREFIX_DOMAIN);
        hasher.update(self.prefix_hash.as_bytes());
        hasher.update(self.next_sequence.to_be_bytes());
        hasher.update((payload.len() as u64).to_be_bytes());
        hasher.update(payload);

        Ok(Self::new(
            next_sequence,
            PrefixHash::new(hasher.finalize().into()),
        ))
    }
}

/// One exact, chain-verified Hivezilla V1 source record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordV1 {
    sequence: u64,
    payload: Vec<u8>,
    prefix_hash: PrefixHash,
}

impl RecordV1 {
    /// Constructs the only record that can validly follow `previous`.
    pub fn new(previous: CursorV1, payload: Vec<u8>) -> Result<Self> {
        let end = previous.advance(&payload)?;
        Ok(Self {
            sequence: previous.next_sequence,
            payload,
            prefix_hash: end.prefix_hash,
        })
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
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
    pub fn end_cursor(&self) -> CursorV1 {
        // Construction and decoding reject a sequence that cannot advance.
        CursorV1::new(self.sequence + 1, self.prefix_hash)
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        RECORD_V1_FIXED_ENCODED_LEN + self.payload.len()
    }

    /// Encodes `sequence || payload_len || payload || prefix_hash`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.encoded_len());
        encoded.extend_from_slice(&self.sequence.to_be_bytes());
        encoded.extend_from_slice(&(self.payload.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        encoded.extend_from_slice(self.prefix_hash.as_bytes());
        encoded
    }

    /// Decodes an exact encoding and proves its sequence and prefix against
    /// `previous`. There is intentionally no public unverified decoder.
    pub fn decode_after(encoded: &[u8], previous: CursorV1) -> Result<Self> {
        let record = decode_unverified(encoded)?;
        record.validate_after(previous)?;
        Ok(record)
    }

    /// Revalidates this record against the cursor immediately before it.
    pub fn validate_after(&self, previous: CursorV1) -> Result<CursorV1> {
        if self.sequence != previous.next_sequence {
            return Err(ProtocolError::SequenceMismatch {
                expected: previous.next_sequence,
                actual: self.sequence,
            });
        }
        let expected = previous.advance(&self.payload)?;
        if self.prefix_hash != expected.prefix_hash {
            return Err(ProtocolError::PrefixMismatch);
        }
        Ok(expected)
    }
}

/// Checks a declared record payload size without allocating it.
pub fn validate_record_payload_len(payload_len: u64) -> Result<()> {
    if payload_len > MAX_RECORD_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge {
            actual: payload_len,
            max: MAX_RECORD_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

fn decode_unverified(encoded: &[u8]) -> Result<RecordV1> {
    if encoded.len() < RECORD_V1_FIXED_ENCODED_LEN {
        return Err(ProtocolError::Truncated {
            context: "RecordV1",
            expected: RECORD_V1_FIXED_ENCODED_LEN,
            actual: encoded.len(),
        });
    }

    let sequence = u64::from_be_bytes(encoded[..8].try_into().expect("fixed slice"));
    let payload_len = u64::from_be_bytes(encoded[8..16].try_into().expect("fixed slice"));
    validate_record_payload_len(payload_len)?;
    let payload_len = usize::try_from(payload_len).expect("V1 payload bound fits usize");
    let expected_len = RECORD_V1_FIXED_ENCODED_LEN + payload_len;

    if encoded.len() < expected_len {
        return Err(ProtocolError::Truncated {
            context: "RecordV1",
            expected: expected_len,
            actual: encoded.len(),
        });
    }
    if encoded.len() > expected_len {
        return Err(ProtocolError::TrailingBytes {
            context: "RecordV1",
            count: encoded.len() - expected_len,
        });
    }

    let payload_end = 16 + payload_len;
    let payload = encoded[16..payload_end].to_vec();
    let prefix_hash = PrefixHash::try_from(&encoded[payload_end..])?;

    Ok(RecordV1 {
        sequence,
        payload,
        prefix_hash,
    })
}

fn require_exact_len(context: &'static str, actual: usize, expected: usize) -> Result<()> {
    if actual < expected {
        return Err(ProtocolError::Truncated {
            context,
            expected,
            actual,
        });
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_header() -> StreamHeaderV1 {
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

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn stream_header_fixed_encoding_is_golden_and_round_trips() {
        let header = fixture_header();
        let encoded = header.fixed_encode();
        assert_eq!(
            hex(&encoded),
            concat!(
                "000102030405060708090a0b0c0d0e0f",
                "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f",
                "00000002",
                "0001",
                "303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f",
                "505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f"
            )
        );
        assert_eq!(StreamHeaderV1::decode(&encoded), Ok(header));
    }

    #[test]
    fn empty_one_and_multi_record_prefixes_are_golden() {
        let p0 = fixture_header().initial_cursor();
        assert_eq!(
            hex(p0.prefix_hash().as_bytes()),
            "137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332"
        );
        assert_eq!(
            hex(&p0.fixed_encode()),
            concat!(
                "0000000000000000",
                "137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332"
            )
        );
        assert_eq!(CursorV1::decode(&p0.fixed_encode()), Ok(p0));

        let first = RecordV1::new(p0, b"abc".to_vec()).unwrap();
        assert_eq!(
            hex(first.prefix_hash().as_bytes()),
            "6917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583"
        );
        assert_eq!(
            hex(&first.encode()),
            concat!(
                "0000000000000000",
                "0000000000000003",
                "616263",
                "6917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583"
            )
        );

        let second = RecordV1::new(first.end_cursor(), vec![0, 1, 2, 255]).unwrap();
        assert_eq!(
            hex(second.prefix_hash().as_bytes()),
            "a28b0721a9a88a31f112dfaabfc69622ac7d302fe41a21cfd0de0a35061b359a"
        );
    }

    #[test]
    fn record_round_trips_only_with_its_predecessor() {
        let start = fixture_header().initial_cursor();
        let record = RecordV1::new(start, b"round trip".to_vec()).unwrap();
        assert_eq!(RecordV1::decode_after(&record.encode(), start), Ok(record));
    }

    #[test]
    fn fixed_types_reject_truncation_trailing_bytes_and_format_zero() {
        let encoded = fixture_header().fixed_encode();
        assert!(matches!(
            StreamHeaderV1::decode(&encoded[..encoded.len() - 1]),
            Err(ProtocolError::Truncated { .. })
        ));

        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert_eq!(
            StreamHeaderV1::decode(&trailing),
            Err(ProtocolError::TrailingBytes {
                context: "StreamHeaderV1",
                count: 1,
            })
        );

        let mut invalid_format = encoded;
        invalid_format[48..52].copy_from_slice(&0_u32.to_be_bytes());
        assert_eq!(
            StreamHeaderV1::decode(&invalid_format),
            Err(ProtocolError::InvalidPayloadFormat)
        );

        let mut unknown_format = encoded;
        unknown_format[48..52].copy_from_slice(&8_u32.to_be_bytes());
        assert_eq!(
            StreamHeaderV1::decode(&unknown_format),
            Err(ProtocolError::UnknownPayloadFormat { value: 8 })
        );

        let mut unknown_version = encoded;
        unknown_version[52..54].copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            StreamHeaderV1::decode(&unknown_version),
            Err(ProtocolError::UnknownPayloadFormatVersion {
                payload_format: 2,
                version: 2,
            })
        );

        let cursor = fixture_header().initial_cursor().fixed_encode();
        assert!(matches!(
            CursorV1::decode(&cursor[..39]),
            Err(ProtocolError::Truncated { .. })
        ));
        let mut cursor_with_trailing = cursor.to_vec();
        cursor_with_trailing.push(0);
        assert!(matches!(
            CursorV1::decode(&cursor_with_trailing),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));
    }

    #[test]
    fn declared_payload_limit_is_enforced_before_allocation() {
        assert_eq!(
            validate_record_payload_len(MAX_RECORD_PAYLOAD_BYTES),
            Ok(())
        );
        assert_eq!(
            validate_record_payload_len(MAX_RECORD_PAYLOAD_BYTES + 1),
            Err(ProtocolError::PayloadTooLarge {
                actual: MAX_RECORD_PAYLOAD_BYTES + 1,
                max: MAX_RECORD_PAYLOAD_BYTES,
            })
        );

        let start = fixture_header().initial_cursor();
        let mut malicious = Vec::new();
        malicious.extend_from_slice(&0_u64.to_be_bytes());
        malicious.extend_from_slice(&(MAX_RECORD_PAYLOAD_BYTES + 1).to_be_bytes());
        malicious.extend_from_slice(&[0_u8; 32]);
        assert!(matches!(
            RecordV1::decode_after(&malicious, start),
            Err(ProtocolError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn record_rejects_truncation_trailing_sequence_and_prefix_changes() {
        let start = fixture_header().initial_cursor();
        let record = RecordV1::new(start, b"payload".to_vec()).unwrap();
        let encoded = record.encode();

        assert!(matches!(
            RecordV1::decode_after(&encoded[..encoded.len() - 1], start),
            Err(ProtocolError::Truncated { .. })
        ));

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            RecordV1::decode_after(&trailing, start),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let wrong_cursor = CursorV1::new(1, start.prefix_hash());
        assert_eq!(
            RecordV1::decode_after(&encoded, wrong_cursor),
            Err(ProtocolError::SequenceMismatch {
                expected: 1,
                actual: 0,
            })
        );

        let mut changed_prefix = encoded;
        *changed_prefix.last_mut().unwrap() ^= 1;
        assert_eq!(
            RecordV1::decode_after(&changed_prefix, start),
            Err(ProtocolError::PrefixMismatch)
        );
    }

    #[test]
    fn cursor_at_maximum_sequence_fails_closed() {
        let cursor = CursorV1::new(u64::MAX, PrefixHash::new([0; 32]));
        assert_eq!(cursor.advance(b"x"), Err(ProtocolError::SequenceOverflow));
        assert_eq!(
            RecordV1::new(cursor, b"x".to_vec()),
            Err(ProtocolError::SequenceOverflow)
        );
    }
}
