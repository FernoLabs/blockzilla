//! Alpenglow `VersionedBlockMarker` bytes stored in a CAR `SlotMeta`.
//!
//! The wire layout is Agave's default Wincode spelling: fixed-width
//! little-endian integers and a one-byte `Option` tag. It is not the LEB128
//! spelling of an Archive V2 hot block. BLS signatures are copied, not checked.
//!
//! [`parse`] splits one marker. [`decode_footer_certificates`] splits the
//! certificate bytes that remain after a footer's user agent. Callers that only
//! store markers do not need either function.

use std::fmt;

use crate::short_vec::decode_shortu16_len;

const MARKER_VERSION: u16 = 1;
const PAYLOAD_VERSION: u8 = 1;
const ENVELOPE_LEN: usize = 5;
const HEADER_PAYLOAD_LEN: usize = 41;
const BLS_COMPRESSED_LEN: usize = 96;
const BLS_AFFINE_LEN: usize = 192;
const MAX_GENESIS_BITMAP_LEN: usize = 512;

const VARIANT_FOOTER: u8 = 0;
const VARIANT_HEADER: u8 = 1;
const VARIANT_UPDATE_PARENT: u8 = 2;
const VARIANT_GENESIS: u8 = 3;

/// One parsed Alpenglow block marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Marker {
    Header(HeaderV1),
    UpdateParent(UpdateParentV1),
    Footer(FooterV1),
    Genesis(GenesisCertificate),
}

/// Parent slot and parent consensus block id from a header marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderV1 {
    pub parent_slot: u64,
    pub parent_block_id: [u8; 32],
}

/// Parent slot and parent consensus block id from an update-parent marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateParentV1 {
    pub new_parent_slot: u64,
    pub new_parent_block_id: [u8; 32],
}

/// Footer fields that precede the three optional certificates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FooterV1 {
    pub bank_hash: [u8; 32],
    pub block_producer_time_nanos: u64,
    pub block_user_agent: Vec<u8>,
    /// Finalization, skip-reward, and notar-reward certificates, still encoded.
    pub certificates: Vec<u8>,
}

/// Genesis certificate. Its payload has no inner version byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenesisCertificate {
    pub slot: u64,
    pub block_id: [u8; 32],
    pub bls_signature: [u8; 192],
    pub bitmap: Vec<u8>,
}

/// The three certificates carried by a footer, in source order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FooterCertificates {
    pub finalization: Option<FinalizationCert>,
    pub skip_reward: Option<SkipRewardCert>,
    pub notar_reward: Option<NotarRewardCert>,
}

/// Block finalization certificate from a footer.
///
/// `notar_aggregate` is absent on a fast finalization of the parent block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizationCert {
    pub slot: u64,
    pub block_id: [u8; 32],
    pub final_aggregate: VotesAggregate,
    pub notar_aggregate: Option<VotesAggregate>,
}

/// Compressed BLS signature and the validator bitmap that signed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VotesAggregate {
    pub signature: [u8; BLS_COMPRESSED_LEN],
    pub bitmap: Vec<u8>,
}

/// Skip-reward certificate. The bitmap length is a Solana `ShortU16`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkipRewardCert {
    pub slot: u64,
    pub signature: [u8; BLS_COMPRESSED_LEN],
    pub bitmap: Vec<u8>,
}

/// Notarization-reward certificate. The bitmap length is a Solana `ShortU16`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotarRewardCert {
    pub slot: u64,
    pub block_id: [u8; 32],
    pub signature: [u8; BLS_COMPRESSED_LEN],
    pub bitmap: Vec<u8>,
}

/// A marker or footer-certificate slice that does not match the Agave layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerError {
    Truncated,
    TrailingBytes,
    UnsupportedVersion(u16),
    UnknownVariant(u8),
    UnsupportedPayloadVersion(u8),
    LengthMismatch { expected: usize, actual: usize },
    InvalidOption(u8),
    InvalidBitmapLength,
    BitmapTooLong(usize),
}

impl fmt::Display for MarkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarkerError::Truncated => write!(f, "truncated block marker"),
            MarkerError::TrailingBytes => write!(f, "block marker has trailing bytes"),
            MarkerError::UnsupportedVersion(version) => {
                write!(f, "unsupported block marker version {version}")
            }
            MarkerError::UnknownVariant(variant) => {
                write!(f, "unknown block marker variant {variant}")
            }
            MarkerError::UnsupportedPayloadVersion(version) => {
                write!(f, "unsupported block marker payload version {version}")
            }
            MarkerError::LengthMismatch { expected, actual } => {
                write!(f, "block marker length is {actual}, expected {expected}")
            }
            MarkerError::InvalidOption(tag) => {
                write!(f, "block marker option tag {tag} is not 0 or 1")
            }
            MarkerError::InvalidBitmapLength => write!(f, "block marker bitmap length is invalid"),
            MarkerError::BitmapTooLong(len) => {
                write!(f, "block marker bitmap length {len} exceeds 512")
            }
        }
    }
}

impl std::error::Error for MarkerError {}

/// Parent consensus block id from the last header or update-parent marker.
///
/// A footer, a genesis marker, or a marker that does not parse is skipped.
pub fn parent_consensus_block_id(markers: &[Vec<u8>]) -> Option<[u8; 32]> {
    markers
        .iter()
        .rev()
        .find_map(|marker| match parse(marker).ok()? {
            Marker::Header(header) => Some(header.parent_block_id),
            Marker::UpdateParent(update) => Some(update.new_parent_block_id),
            Marker::Footer(_) | Marker::Genesis(_) => None,
        })
}

/// Parse one complete `VersionedBlockMarker`.
pub fn parse(bytes: &[u8]) -> Result<Marker, MarkerError> {
    if bytes.len() < ENVELOPE_LEN {
        return Err(MarkerError::Truncated);
    }
    let version = u16::from_le_bytes([bytes[0], bytes[1]]);
    if version != MARKER_VERSION {
        return Err(MarkerError::UnsupportedVersion(version));
    }
    let variant = bytes[2];
    let payload_len = usize::from(u16::from_le_bytes([bytes[3], bytes[4]]));
    let expected = ENVELOPE_LEN + payload_len;
    if bytes.len() != expected {
        return Err(MarkerError::LengthMismatch {
            expected,
            actual: bytes.len(),
        });
    }
    let payload = &bytes[ENVELOPE_LEN..];
    match variant {
        VARIANT_FOOTER => Ok(Marker::Footer(decode_footer(payload)?)),
        VARIANT_HEADER => {
            let (parent_slot, parent_block_id) = decode_slot_and_block_id(payload)?;
            Ok(Marker::Header(HeaderV1 {
                parent_slot,
                parent_block_id,
            }))
        }
        VARIANT_UPDATE_PARENT => {
            let (new_parent_slot, new_parent_block_id) = decode_slot_and_block_id(payload)?;
            Ok(Marker::UpdateParent(UpdateParentV1 {
                new_parent_slot,
                new_parent_block_id,
            }))
        }
        VARIANT_GENESIS => Ok(Marker::Genesis(decode_genesis(payload)?)),
        other => Err(MarkerError::UnknownVariant(other)),
    }
}

/// Decode the three optional certificates that follow a footer's user agent.
///
/// The slice must be consumed exactly. An empty slice is truncated: a footer
/// always carries the three presence tags.
pub fn decode_footer_certificates(bytes: &[u8]) -> Result<FooterCertificates, MarkerError> {
    let mut cursor = Cursor::new(bytes);
    let finalization = read_optional(&mut cursor, read_finalization_cert)?;
    let skip_reward = read_optional(&mut cursor, read_skip_reward)?;
    let notar_reward = read_optional(&mut cursor, read_notar_reward)?;
    cursor.finish()?;
    Ok(FooterCertificates {
        finalization,
        skip_reward,
        notar_reward,
    })
}

fn decode_footer(payload: &[u8]) -> Result<FooterV1, MarkerError> {
    let mut cursor = Cursor::new(payload);
    let version = cursor.u8()?;
    if version != PAYLOAD_VERSION {
        return Err(MarkerError::UnsupportedPayloadVersion(version));
    }
    let bank_hash = cursor.array()?;
    let block_producer_time_nanos = cursor.u64()?;
    let user_agent_len = usize::from(cursor.u8()?);
    let block_user_agent = cursor.take(user_agent_len)?.to_vec();
    let certificates = cursor.rest().to_vec();
    Ok(FooterV1 {
        bank_hash,
        block_producer_time_nanos,
        block_user_agent,
        certificates,
    })
}

fn decode_slot_and_block_id(payload: &[u8]) -> Result<(u64, [u8; 32]), MarkerError> {
    if payload.len() != HEADER_PAYLOAD_LEN {
        return Err(MarkerError::LengthMismatch {
            expected: HEADER_PAYLOAD_LEN,
            actual: payload.len(),
        });
    }
    let mut cursor = Cursor::new(payload);
    let version = cursor.u8()?;
    if version != PAYLOAD_VERSION {
        return Err(MarkerError::UnsupportedPayloadVersion(version));
    }
    let slot = cursor.u64()?;
    let block_id = cursor.array()?;
    cursor.finish()?;
    Ok((slot, block_id))
}

fn decode_genesis(payload: &[u8]) -> Result<GenesisCertificate, MarkerError> {
    let mut cursor = Cursor::new(payload);
    let slot = cursor.u64()?;
    let block_id = cursor.array()?;
    let bls_signature = cursor.array()?;
    let bitmap_len =
        usize::try_from(cursor.u64()?).map_err(|_| MarkerError::InvalidBitmapLength)?;
    if bitmap_len > MAX_GENESIS_BITMAP_LEN {
        return Err(MarkerError::BitmapTooLong(bitmap_len));
    }
    if cursor.rest().len() != bitmap_len {
        return Err(MarkerError::LengthMismatch {
            expected: bitmap_len,
            actual: cursor.rest().len(),
        });
    }
    let bitmap = cursor.take(bitmap_len)?.to_vec();
    cursor.finish()?;
    Ok(GenesisCertificate {
        slot,
        block_id,
        bls_signature,
        bitmap,
    })
}

fn read_finalization_cert(cursor: &mut Cursor<'_>) -> Result<FinalizationCert, MarkerError> {
    let slot = cursor.u64()?;
    let block_id = cursor.array()?;
    let final_aggregate = read_votes_aggregate(cursor)?;
    let notar_aggregate = read_optional(cursor, read_votes_aggregate)?;
    Ok(FinalizationCert {
        slot,
        block_id,
        final_aggregate,
        notar_aggregate,
    })
}

fn read_votes_aggregate(cursor: &mut Cursor<'_>) -> Result<VotesAggregate, MarkerError> {
    let signature = cursor.array()?;
    let bitmap_len = usize::from(cursor.u16()?);
    let bitmap = cursor.take(bitmap_len)?.to_vec();
    Ok(VotesAggregate { signature, bitmap })
}

fn read_skip_reward(cursor: &mut Cursor<'_>) -> Result<SkipRewardCert, MarkerError> {
    let slot = cursor.u64()?;
    let signature = cursor.array()?;
    let bitmap = read_short_bitmap(cursor)?;
    Ok(SkipRewardCert {
        slot,
        signature,
        bitmap,
    })
}

fn read_notar_reward(cursor: &mut Cursor<'_>) -> Result<NotarRewardCert, MarkerError> {
    let slot = cursor.u64()?;
    let block_id = cursor.array()?;
    let signature = cursor.array()?;
    let bitmap = read_short_bitmap(cursor)?;
    Ok(NotarRewardCert {
        slot,
        block_id,
        signature,
        bitmap,
    })
}

fn read_short_bitmap(cursor: &mut Cursor<'_>) -> Result<Vec<u8>, MarkerError> {
    let (bitmap_len, len_bytes) =
        decode_shortu16_len(cursor.rest()).map_err(|_| MarkerError::InvalidBitmapLength)?;
    cursor.take(len_bytes)?;
    Ok(cursor.take(bitmap_len)?.to_vec())
}

fn read_optional<T>(
    cursor: &mut Cursor<'_>,
    read: fn(&mut Cursor<'_>) -> Result<T, MarkerError>,
) -> Result<Option<T>, MarkerError> {
    match cursor.u8()? {
        0 => Ok(None),
        1 => Ok(Some(read(cursor)?)),
        tag => Err(MarkerError::InvalidOption(tag)),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn rest(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }

    fn finish(self) -> Result<(), MarkerError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(MarkerError::TrailingBytes)
        }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MarkerError> {
        let end = self.pos.checked_add(len).ok_or(MarkerError::Truncated)?;
        if end > self.bytes.len() {
            return Err(MarkerError::Truncated);
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, MarkerError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, MarkerError> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn u64(&mut self) -> Result<u64, MarkerError> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(
            bytes.try_into().expect("u64 is 8 bytes"),
        ))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], MarkerError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(variant: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MARKER_VERSION.to_le_bytes());
        out.push(variant);
        out.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn slot_and_id_payload(slot: u64, block_id: [u8; 32]) -> Vec<u8> {
        let mut payload = vec![PAYLOAD_VERSION];
        payload.extend_from_slice(&slot.to_le_bytes());
        payload.extend_from_slice(&block_id);
        payload
    }

    fn votes(signature_byte: u8, bitmap: &[u8]) -> Vec<u8> {
        let mut out = vec![signature_byte; BLS_COMPRESSED_LEN];
        out.extend_from_slice(&(bitmap.len() as u16).to_le_bytes());
        out.extend_from_slice(bitmap);
        out
    }

    fn short_u16(len: usize) -> Vec<u8> {
        if len < 0x80 {
            vec![len as u8]
        } else {
            vec![(len as u8 & 0x7f) | 0x80, (len >> 7) as u8]
        }
    }

    fn reward_body(
        slot: u64,
        block_id: Option<[u8; 32]>,
        signature_byte: u8,
        bitmap: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&slot.to_le_bytes());
        if let Some(block_id) = block_id {
            out.extend_from_slice(&block_id);
        }
        out.extend(std::iter::repeat_n(signature_byte, BLS_COMPRESSED_LEN));
        out.extend_from_slice(&short_u16(bitmap.len()));
        out.extend_from_slice(bitmap);
        out
    }

    fn footer_payload(user_agent: &str, certificates: &[u8]) -> Vec<u8> {
        let mut payload = vec![PAYLOAD_VERSION];
        payload.extend_from_slice(&[0xab; 32]);
        payload.extend_from_slice(&1_792_773_113_823_407_080u64.to_le_bytes());
        payload.push(user_agent.len() as u8);
        payload.extend_from_slice(user_agent.as_bytes());
        payload.extend_from_slice(certificates);
        payload
    }

    #[test]
    fn header_and_update_parent_expose_the_parent_block_id() {
        let header_parent = [0x11; 32];
        let updated_parent = [0x22; 32];
        let header = marker(VARIANT_HEADER, &slot_and_id_payload(40, header_parent));
        let update = marker(
            VARIANT_UPDATE_PARENT,
            &slot_and_id_payload(39, updated_parent),
        );
        assert_eq!(header.len(), 46);
        assert_eq!(update.len(), 46);

        let Marker::Header(parsed_header) = parse(&header).unwrap() else {
            panic!("header");
        };
        assert_eq!(parsed_header.parent_slot, 40);
        assert_eq!(parsed_header.parent_block_id, header_parent);
        let Marker::UpdateParent(parsed_update) = parse(&update).unwrap() else {
            panic!("update parent");
        };
        assert_eq!(parsed_update.new_parent_slot, 39);
        assert_eq!(parsed_update.new_parent_block_id, updated_parent);

        let footer = marker(VARIANT_FOOTER, &footer_payload("agave/4.3.2", &[0, 0, 0]));
        let markers = vec![header, footer, vec![1, 2, 3], update];
        assert_eq!(parent_consensus_block_id(&markers), Some(updated_parent));
    }

    #[test]
    fn fast_footer_keeps_certificates_raw_until_asked() {
        let bitmap = [0xaa, 0xbb];
        let reward_bitmap = [0x10, 0x20, 0x30];
        let block_id = [0x44; 32];
        let mut certificates = vec![1];
        certificates.extend_from_slice(&460_512_003u64.to_le_bytes());
        certificates.extend_from_slice(&block_id);
        certificates.extend_from_slice(&votes(0xa3, &bitmap));
        certificates.push(0);
        certificates.push(0);
        certificates.push(1);
        certificates.extend_from_slice(&reward_body(
            460_511_996,
            Some([0x55; 32]),
            0xa0,
            &reward_bitmap,
        ));

        let raw = marker(
            VARIANT_FOOTER,
            &footer_payload("agave/4.3.2", &certificates),
        );
        let Marker::Footer(footer) = parse(&raw).unwrap() else {
            panic!("footer");
        };
        assert_eq!(footer.bank_hash, [0xab; 32]);
        assert_eq!(footer.block_producer_time_nanos, 1_792_773_113_823_407_080);
        assert_eq!(footer.block_user_agent, b"agave/4.3.2");
        assert_eq!(footer.certificates, certificates);

        let decoded = decode_footer_certificates(&footer.certificates).unwrap();
        let finalization = decoded.finalization.unwrap();
        assert_eq!(finalization.slot, 460_512_003);
        assert_eq!(finalization.block_id, block_id);
        assert_eq!(finalization.final_aggregate.signature, [0xa3; 96]);
        assert_eq!(finalization.final_aggregate.bitmap, bitmap);
        assert!(finalization.notar_aggregate.is_none());
        assert!(decoded.skip_reward.is_none());
        let notar_reward = decoded.notar_reward.unwrap();
        assert_eq!(notar_reward.slot, 460_511_996);
        assert_eq!(notar_reward.block_id, [0x55; 32]);
        assert_eq!(notar_reward.signature, [0xa0; 96]);
        assert_eq!(notar_reward.bitmap, reward_bitmap);
    }

    #[test]
    fn slow_footer_carries_both_aggregates_and_a_skip_reward() {
        let skip_bitmap = vec![0x11; 128];
        let mut certificates = vec![1];
        certificates.extend_from_slice(&7u64.to_le_bytes());
        certificates.extend_from_slice(&[0x66; 32]);
        certificates.extend_from_slice(&votes(0x01, &[0x01]));
        certificates.push(1);
        certificates.extend_from_slice(&votes(0x02, &[0x02, 0x03]));
        certificates.push(1);
        certificates.extend_from_slice(&reward_body(8, None, 0x03, &skip_bitmap));
        certificates.push(0);

        let decoded = decode_footer_certificates(&certificates).unwrap();
        let finalization = decoded.finalization.unwrap();
        assert_eq!(finalization.final_aggregate.bitmap, [0x01]);
        let notar = finalization.notar_aggregate.unwrap();
        assert_eq!(notar.signature, [0x02; 96]);
        assert_eq!(notar.bitmap, [0x02, 0x03]);
        let skip = decoded.skip_reward.unwrap();
        assert_eq!(skip.slot, 8);
        assert_eq!(skip.bitmap, skip_bitmap);
        assert!(decoded.notar_reward.is_none());
    }

    #[test]
    fn genesis_certificate_uses_the_affine_signature_and_u64_bitmap_length() {
        let bitmap = vec![0x7e; 4];
        let mut payload = Vec::new();
        payload.extend_from_slice(&459_652_999u64.to_le_bytes());
        payload.extend_from_slice(&[0x91; 32]);
        payload.extend(std::iter::repeat_n(0xbe, BLS_AFFINE_LEN));
        payload.extend_from_slice(&(bitmap.len() as u64).to_le_bytes());
        payload.extend_from_slice(&bitmap);
        let raw = marker(VARIANT_GENESIS, &payload);

        let Marker::Genesis(cert) = parse(&raw).unwrap() else {
            panic!("genesis");
        };
        assert_eq!(cert.slot, 459_652_999);
        assert_eq!(cert.block_id, [0x91; 32]);
        assert_eq!(cert.bls_signature, [0xbe; 192]);
        assert_eq!(cert.bitmap, bitmap);
        assert_eq!(parent_consensus_block_id(&[raw]), None);
    }

    #[test]
    fn truncated_unknown_and_trailing_markers_fail() {
        assert_eq!(parse(&[1, 0, 1]).unwrap_err(), MarkerError::Truncated);
        assert_eq!(
            parse(&marker(9, &slot_and_id_payload(1, [2; 32]))).unwrap_err(),
            MarkerError::UnknownVariant(9)
        );
        let mut header = marker(VARIANT_HEADER, &slot_and_id_payload(1, [2; 32]));
        header.push(0xff);
        assert!(matches!(
            parse(&header),
            Err(MarkerError::LengthMismatch { .. })
        ));

        let short_footer = marker(VARIANT_FOOTER, &[PAYLOAD_VERSION]);
        assert_eq!(parse(&short_footer).unwrap_err(), MarkerError::Truncated);

        assert_eq!(
            decode_footer_certificates(&[]).unwrap_err(),
            MarkerError::Truncated
        );
        assert_eq!(
            decode_footer_certificates(&[0, 0, 0, 0xff]).unwrap_err(),
            MarkerError::TrailingBytes
        );
    }
}
