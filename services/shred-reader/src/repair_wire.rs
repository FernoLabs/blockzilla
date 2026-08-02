//! Agave v4.1.2 repair-protocol wire encoding.
//!
//! The request enum order, fixed-width bincode configuration, signature
//! placement, and ping/pong types intentionally mirror Agave v4.1.2. Keep the
//! golden tests when changing Solana dependencies: even a valid serde refactor
//! can otherwise produce packets that validators silently reject.

use std::{error::Error, fmt};

use bincode::serialize;
use serde::{Deserialize, Serialize};
use solana_gossip::ping_pong::{Ping, Pong};
use solana_hash::Hash;
use solana_keypair::{Keypair, Signature, Signer, signable::Signable};
use solana_ledger::shred::Shred;
use solana_pubkey::Pubkey;

/// Repair nonces are appended to shred responses as a little-endian `u32`.
pub type RepairNonce = u32;

pub const REPAIR_NONCE_BYTES: usize = std::mem::size_of::<RepairNonce>();
pub const REPAIR_PING_TOKEN_BYTES: usize = 32;
pub const REPAIR_PING_PACKET_BYTES: usize = 4 + 32 + REPAIR_PING_TOKEN_BYTES + 64;

const ENUM_DISCRIMINANT_BYTES: usize = 4;
const SIGNATURE_BYTES: usize = 64;
const SIGNATURE_OFFSET: usize = ENUM_DISCRIMINANT_BYTES;
const SIGNATURE_END: usize = SIGNATURE_OFFSET + SIGNATURE_BYTES;

pub type RepairPing = Ping<REPAIR_PING_TOKEN_BYTES>;

/// The subset of Agave shred-repair requests needed by a recent-slot repairer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShredRepairRequest {
    /// Request one exact data-shred index.
    Shred { slot: u64, shred_index: u64 },
    /// Request any available data shred at or above `shred_index`.
    HighestShred { slot: u64, shred_index: u64 },
    /// Request up to Agave's bounded set of parent-slot shreds.
    Orphan { slot: u64 },
}

/// A raw repair response after removing Agave's trailing request nonce.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShredResponse<'a> {
    pub payload: &'a [u8],
    pub nonce: RepairNonce,
}

#[derive(Debug)]
pub enum RepairWireError {
    Codec(bincode::Error),
    ResponseTooShort {
        actual: usize,
    },
    NonceMismatch {
        expected: RepairNonce,
        actual: RepairNonce,
    },
    InvalidSigningLayout {
        actual: usize,
    },
    MalformedShredResponse,
    UnexpectedShredResponse {
        request: ShredRepairRequest,
        slot: u64,
        index: u32,
        is_data: bool,
    },
}

impl fmt::Display for RepairWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(formatter, "repair wire codec error: {error}"),
            Self::ResponseTooShort { actual } => write!(
                formatter,
                "repair shred response is {actual} bytes; expected more than the {REPAIR_NONCE_BYTES}-byte nonce"
            ),
            Self::NonceMismatch { expected, actual } => write!(
                formatter,
                "repair response nonce mismatch: expected {expected}, received {actual}"
            ),
            Self::InvalidSigningLayout { actual } => write!(
                formatter,
                "serialized repair request is {actual} bytes; signature field is incomplete"
            ),
            Self::MalformedShredResponse => {
                write!(
                    formatter,
                    "repair response does not contain a valid Agave shred"
                )
            }
            Self::UnexpectedShredResponse {
                request,
                slot,
                index,
                is_data,
            } => write!(
                formatter,
                "repair response data shred identity does not match {request:?}: slot={slot}, index={index}, is_data={is_data}"
            ),
        }
    }
}

impl Error for RepairWireError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error.as_ref()),
            Self::ResponseTooShort { .. }
            | Self::NonceMismatch { .. }
            | Self::InvalidSigningLayout { .. }
            | Self::MalformedShredResponse
            | Self::UnexpectedShredResponse { .. } => None,
        }
    }
}

impl From<bincode::Error> for RepairWireError {
    fn from(error: bincode::Error) -> Self {
        Self::Codec(error)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct RepairRequestHeader {
    // Agave signs the serialized packet with this field zeroed, then replaces
    // exactly these 64 bytes in place.
    signature: Signature,
    sender: Pubkey,
    recipient: Pubkey,
    timestamp: u64,
    nonce: RepairNonce,
}

/// Keep this enum in exact Agave v4.1.2 declaration order. Bincode encodes the
/// zero-based variant index as a four-byte little-endian integer.
#[derive(Debug, Deserialize, Serialize)]
enum RepairProtocol {
    LegacyWindowIndex,
    LegacyHighestWindowIndex,
    LegacyOrphan,
    LegacyWindowIndexWithNonce,
    LegacyHighestWindowIndexWithNonce,
    LegacyOrphanWithNonce,
    LegacyAncestorHashes,
    Pong(Pong),
    WindowIndex {
        header: RepairRequestHeader,
        slot: u64,
        shred_index: u64,
    },
    HighestWindowIndex {
        header: RepairRequestHeader,
        slot: u64,
        shred_index: u64,
    },
    Orphan {
        header: RepairRequestHeader,
        slot: u64,
    },
    AncestorHashes {
        header: RepairRequestHeader,
        slot: u64,
    },
    ParentAndFecSetCount {
        header: RepairRequestHeader,
        slot: u64,
        block_id: Hash,
    },
    FecSetRoot {
        header: RepairRequestHeader,
        slot: u64,
        block_id: Hash,
        fec_set_index: u32,
    },
    WindowIndexForBlockId {
        header: RepairRequestHeader,
        slot: u64,
        shred_index: u32,
        fec_set_merkle_root: Hash,
        block_id: Hash,
    },
}

#[derive(Debug, Deserialize, Serialize)]
enum RepairResponse {
    Ping(RepairPing),
}

/// Encode and sign one Agave v4.1.2 shred-repair request.
///
/// `timestamp_ms` is the Unix timestamp in milliseconds. Validators currently
/// reject signed requests outside their repair time window, so callers should
/// produce it immediately before sending.
pub fn encode_shred_repair_request(
    request: ShredRepairRequest,
    sender: &Keypair,
    recipient: Pubkey,
    timestamp_ms: u64,
    nonce: RepairNonce,
) -> Result<Vec<u8>, RepairWireError> {
    let header = RepairRequestHeader {
        signature: Signature::default(),
        sender: sender.pubkey(),
        recipient,
        timestamp: timestamp_ms,
        nonce,
    };
    let protocol = match request {
        ShredRepairRequest::Shred { slot, shred_index } => RepairProtocol::WindowIndex {
            header,
            slot,
            shred_index,
        },
        ShredRepairRequest::HighestShred { slot, shred_index } => {
            RepairProtocol::HighestWindowIndex {
                header,
                slot,
                shred_index,
            }
        }
        ShredRepairRequest::Orphan { slot } => RepairProtocol::Orphan { header, slot },
    };
    encode_signed_protocol(&protocol, sender)
}

/// Parse and verify an Agave repair ping challenge.
///
/// A non-ping packet, malformed ping, or ping with an invalid signature returns
/// `None`; the caller may then attempt normal shred-response processing. This
/// matches Agave's false-positive-safe receive path.
pub fn parse_verified_ping(packet: &[u8]) -> Option<RepairPing> {
    if packet.len() != REPAIR_PING_PACKET_BYTES {
        return None;
    }
    let RepairResponse::Ping(ping) = bincode::deserialize(packet).ok()?;
    ping.verify().then_some(ping)
}

/// Encode the signed pong expected by an Agave v4.1.2 repair peer.
pub fn encode_pong(ping: &RepairPing, sender: &Keypair) -> Result<Vec<u8>, RepairWireError> {
    Ok(serialize(&RepairProtocol::Pong(Pong::new(ping, sender)))?)
}

/// Split a raw repair shred packet into payload and trailing nonce.
///
/// Call [`parse_verified_ping`] first because ping challenges do not carry a
/// trailing nonce.
pub fn split_shred_response(packet: &[u8]) -> Result<ShredResponse<'_>, RepairWireError> {
    if packet.len() <= REPAIR_NONCE_BYTES {
        return Err(RepairWireError::ResponseTooShort {
            actual: packet.len(),
        });
    }
    let split = packet.len() - REPAIR_NONCE_BYTES;
    let nonce = RepairNonce::from_le_bytes(
        packet[split..]
            .try_into()
            .expect("nonce slice has a fixed checked length"),
    );
    Ok(ShredResponse {
        payload: &packet[..split],
        nonce,
    })
}

/// Split a shred response and reject packets that do not match the outstanding
/// request nonce.
pub fn split_shred_response_for_nonce(
    packet: &[u8],
    expected: RepairNonce,
) -> Result<&[u8], RepairWireError> {
    let response = split_shred_response(packet)?;
    if response.nonce != expected {
        return Err(RepairWireError::NonceMismatch {
            expected,
            actual: response.nonce,
        });
    }
    Ok(response.payload)
}

/// Decode a nonce-matched response and bind it to the request that produced it.
///
/// This prevents a delayed or spoofed packet with a valid outstanding nonce from satisfying the
/// wrong slot/index request. It intentionally does **not** verify the leader signature: the caller
/// must verify the returned shred against the slot leader before admitting it to repair state or
/// durable storage.
pub fn decode_matching_shred_response(
    packet: &[u8],
    expected_nonce: RepairNonce,
    request: ShredRepairRequest,
) -> Result<Shred, RepairWireError> {
    let payload = split_shred_response_for_nonce(packet, expected_nonce)?;
    let shred = Shred::new_from_serialized_shred(payload.to_vec())
        .map_err(|_| RepairWireError::MalformedShredResponse)?;
    let slot = shred.slot();
    let index = shred.index();
    let is_data = shred.is_data();
    let matches = is_data
        && match request {
            ShredRepairRequest::Shred {
                slot: requested_slot,
                shred_index,
            } => slot == requested_slot && u64::from(index) == shred_index,
            ShredRepairRequest::HighestShred {
                slot: requested_slot,
                shred_index,
            } => slot == requested_slot && u64::from(index) >= shred_index,
            ShredRepairRequest::Orphan {
                slot: requested_slot,
            } => slot <= requested_slot,
        };
    if !matches {
        return Err(RepairWireError::UnexpectedShredResponse {
            request,
            slot,
            index,
            is_data,
        });
    }
    Ok(shred)
}

fn encode_signed_protocol(
    protocol: &RepairProtocol,
    sender: &Keypair,
) -> Result<Vec<u8>, RepairWireError> {
    let mut payload = serialize(protocol)?;
    if payload.len() < SIGNATURE_END {
        return Err(RepairWireError::InvalidSigningLayout {
            actual: payload.len(),
        });
    }

    let mut signable = Vec::with_capacity(payload.len() - SIGNATURE_BYTES);
    signable.extend_from_slice(&payload[..SIGNATURE_OFFSET]);
    signable.extend_from_slice(&payload[SIGNATURE_END..]);
    let signature = sender.sign_message(&signable);
    payload[SIGNATURE_OFFSET..SIGNATURE_END].copy_from_slice(signature.as_ref());
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW_INDEX_GOLDEN: &str = concat!(
        "0800000073aba031224e9144a73f6723df9bf60a6941298410f4216a9ae04b13",
        "5df2088050c650db7f2d9355acff9e611c546e5a28e19018d59ad22e0ebc585b",
        "97293e05ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea69",
        "1446d22c09090909090909090909090909090909090909090909090909090909",
        "09090909149a074691010000443322111bf2e51900000000c801000000000000"
    );
    const PING_GOLDEN: &str = concat!(
        "0000000066be7e332c7a453332bd9d0a7f7db055f5c5ef1a06ada66d98b39fb6",
        "810c473a16161616161616161616161616161616161616161616161616161616",
        "16161616d4fa99310e05e49b06764b06fdbae529391370fe71a96157b602bcb9",
        "6d98184dc661741150b0e54abd900e94fdc3866e47fdd00d901b720db72114944",
        "0aad108"
    );
    const PONG_GOLDEN: &str = concat!(
        "07000000ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea69",
        "1446d22c7911e4bfbe9c18f8745c58f46ed96b377a477fc8987187427a309588",
        "701d27281a46853a89dd751d33b442e9e5e45f7aff97c47708b0d25371d0c33",
        "f6fa74490e01117f294c07edf4ea4c3cf44e35d36b01cb5252d82a1d75c6bb6",
        "dfd4b57f05"
    );

    #[test]
    fn window_index_matches_agave_v4_1_2_golden() {
        let sender = Keypair::new_from_array([7; 32]);
        let packet = encode_shred_repair_request(
            ShredRepairRequest::Shred {
                slot: 434_500_123,
                shred_index: 456,
            },
            &sender,
            Pubkey::new_from_array([9; 32]),
            1_723_456_789_012,
            0x1122_3344,
        )
        .unwrap();

        assert_eq!(encode_hex(&packet), WINDOW_INDEX_GOLDEN);
        assert_eq!(packet.len(), 160);

        let protocol: RepairProtocol = bincode::deserialize(&packet).unwrap();
        let RepairProtocol::WindowIndex { header, .. } = protocol else {
            panic!("golden request decoded as the wrong variant");
        };
        let signable = [&packet[..SIGNATURE_OFFSET], &packet[SIGNATURE_END..]].concat();
        assert!(header.signature.verify(header.sender.as_ref(), &signable));
    }

    #[test]
    fn active_request_discriminants_match_agave_v4_1_2() {
        let sender = Keypair::new_from_array([7; 32]);
        let recipient = Pubkey::new_from_array([9; 32]);
        let cases = [
            (
                ShredRepairRequest::Shred {
                    slot: 1,
                    shred_index: 2,
                },
                8u32,
            ),
            (
                ShredRepairRequest::HighestShred {
                    slot: 1,
                    shred_index: 2,
                },
                9u32,
            ),
            (ShredRepairRequest::Orphan { slot: 1 }, 10u32),
        ];

        for (request, expected_discriminant) in cases {
            let packet = encode_shred_repair_request(request, &sender, recipient, 3, 4).unwrap();
            assert_eq!(
                u32::from_le_bytes(packet[..4].try_into().unwrap()),
                expected_discriminant
            );
        }
    }

    #[test]
    fn verified_ping_and_pong_match_agave_v4_1_2_goldens() {
        let server = Keypair::new_from_array([11; 32]);
        let ping = RepairPing::new([22; REPAIR_PING_TOKEN_BYTES], &server);
        let ping_packet = serialize(&RepairResponse::Ping(ping)).unwrap();
        assert_eq!(ping_packet.len(), REPAIR_PING_PACKET_BYTES);
        assert_eq!(encode_hex(&ping_packet), PING_GOLDEN);

        let parsed_ping = parse_verified_ping(&ping_packet).expect("valid ping challenge");
        let client = Keypair::new_from_array([7; 32]);
        let pong_packet = encode_pong(&parsed_ping, &client).unwrap();
        assert_eq!(encode_hex(&pong_packet), PONG_GOLDEN);
        assert_eq!(u32::from_le_bytes(pong_packet[..4].try_into().unwrap()), 7);

        let RepairProtocol::Pong(pong) = bincode::deserialize(&pong_packet).unwrap() else {
            panic!("golden pong decoded as the wrong variant");
        };
        assert!(pong.verify());
    }

    #[test]
    fn malformed_or_forged_ping_is_not_accepted() {
        assert!(parse_verified_ping(&[0; REPAIR_PING_PACKET_BYTES - 1]).is_none());

        let server = Keypair::new_from_array([11; 32]);
        let ping = RepairPing::new([22; REPAIR_PING_TOKEN_BYTES], &server);
        let mut packet = serialize(&RepairResponse::Ping(ping)).unwrap();
        *packet.last_mut().unwrap() ^= 1;
        assert!(parse_verified_ping(&packet).is_none());
    }

    #[test]
    fn shred_response_nonce_is_split_and_checked() {
        let payload = [0xaa, 0xbb, 0xcc, 0xdd, 0xee];
        let nonce: RepairNonce = 0x1234_abcd;
        let mut packet = payload.to_vec();
        packet.extend_from_slice(&nonce.to_le_bytes());

        assert_eq!(
            split_shred_response(&packet).unwrap(),
            ShredResponse {
                payload: &payload,
                nonce,
            }
        );
        assert_eq!(
            split_shred_response_for_nonce(&packet, nonce).unwrap(),
            payload
        );
        assert!(matches!(
            split_shred_response_for_nonce(&packet, nonce + 1),
            Err(RepairWireError::NonceMismatch {
                expected,
                actual
            }) if expected == nonce + 1 && actual == nonce
        ));
        assert!(matches!(
            split_shred_response(&[0; REPAIR_NONCE_BYTES]),
            Err(RepairWireError::ResponseTooShort { actual }) if actual == REPAIR_NONCE_BYTES
        ));
    }

    #[test]
    fn shred_response_is_bound_to_the_outstanding_request() {
        let nonce: RepairNonce = 0x1234_abcd;
        let mut packet = test_data_shred(434_500_123, 31);
        packet.extend_from_slice(&nonce.to_le_bytes());

        let exact = ShredRepairRequest::Shred {
            slot: 434_500_123,
            shred_index: 31,
        };
        let shred = decode_matching_shred_response(&packet, nonce, exact).unwrap();
        assert_eq!((shred.slot(), shred.index()), (434_500_123, 31));

        let highest = ShredRepairRequest::HighestShred {
            slot: 434_500_123,
            shred_index: 30,
        };
        assert!(decode_matching_shred_response(&packet, nonce, highest).is_ok());

        let wrong_index = ShredRepairRequest::Shred {
            slot: 434_500_123,
            shred_index: 30,
        };
        assert!(matches!(
            decode_matching_shred_response(&packet, nonce, wrong_index),
            Err(RepairWireError::UnexpectedShredResponse { .. })
        ));
    }

    fn test_data_shred(slot: u64, index: u32) -> Vec<u8> {
        let mut payload = vec![0u8; 1_203];
        payload[64] = 0x90; // MerkleData { proof_size: 0, resigned: false }
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&index.to_le_bytes());
        payload[77..79].copy_from_slice(&1u16.to_le_bytes());
        payload[79..83].copy_from_slice(&(index / 32 * 32).to_le_bytes());
        payload[83..85].copy_from_slice(&1u16.to_le_bytes());
        payload[85] = 0b1100_0000;
        payload[86..88].copy_from_slice(&88u16.to_le_bytes());
        payload
    }

    fn encode_hex(bytes: &[u8]) -> String {
        use std::fmt::Write;

        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut output, "{byte:02x}").unwrap();
        }
        output
    }
}
