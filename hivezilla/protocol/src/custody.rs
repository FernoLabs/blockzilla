use sha2::{Digest, Sha256};

use crate::{
    AcceptedAckReceiptSha256, CURSOR_V1_ENCODED_LEN, CursorV1, DeletionAuthorizingStoreId,
    DurabilityPolicyId, ProtocolError, Result, STREAM_HEADER_V1_ENCODED_LEN, StreamHeaderV1,
    StreamId, StreamManifestSha256, StreamManifestV1,
};

pub const ACK_V1_ENCODED_LEN: usize = StreamId::LENGTH
    + DeletionAuthorizingStoreId::LENGTH
    + StreamManifestSha256::LENGTH
    + DurabilityPolicyId::LENGTH
    + CURSOR_V1_ENCODED_LEN;
pub const MAX_AUTHENTICATED_PEER_ID_BYTES: u64 = 1_024;
pub const ACCEPTED_ACK_RECEIPT_V1_FIXED_ENCODED_LEN: usize = 8
    + AcceptedAckReceiptSha256::LENGTH
    + STREAM_HEADER_V1_ENCODED_LEN
    + 8
    + ACK_V1_ENCODED_LEN
    + AcceptedAckReceiptSha256::LENGTH;
pub const MAX_ACCEPTED_ACK_RECEIPT_V1_ENCODED_LEN: usize =
    ACCEPTED_ACK_RECEIPT_V1_FIXED_ENCODED_LEN + MAX_AUTHENTICATED_PEER_ID_BYTES as usize;
pub const SOURCE_RETIREMENT_CHECKPOINT_V1_ENCODED_LEN: usize =
    STREAM_HEADER_V1_ENCODED_LEN + CURSOR_V1_ENCODED_LEN + AcceptedAckReceiptSha256::LENGTH;

const ACCEPTED_ACK_RECEIPT_DOMAIN: &[u8] = b"hive/v1/accepted-ack";

/// One cumulative terminal-custody acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AckV1 {
    stream_id: StreamId,
    terminal_store_id: DeletionAuthorizingStoreId,
    stream_manifest_sha256: StreamManifestSha256,
    policy_id: DurabilityPolicyId,
    protected_cursor: CursorV1,
}

impl AckV1 {
    #[must_use]
    pub const fn new(
        stream_id: StreamId,
        terminal_store_id: DeletionAuthorizingStoreId,
        stream_manifest_sha256: StreamManifestSha256,
        policy_id: DurabilityPolicyId,
        protected_cursor: CursorV1,
    ) -> Self {
        Self {
            stream_id,
            terminal_store_id,
            stream_manifest_sha256,
            policy_id,
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
    pub const fn stream_manifest_sha256(&self) -> StreamManifestSha256 {
        self.stream_manifest_sha256
    }

    #[must_use]
    pub const fn policy_id(&self) -> DurabilityPolicyId {
        self.policy_id
    }

    #[must_use]
    pub const fn protected_cursor(&self) -> CursorV1 {
        self.protected_cursor
    }

    #[must_use]
    pub fn fixed_encode(&self) -> [u8; ACK_V1_ENCODED_LEN] {
        let mut encoded = [0_u8; ACK_V1_ENCODED_LEN];
        let mut offset = 0;
        copy(&mut encoded, &mut offset, self.stream_id.as_bytes());
        copy(&mut encoded, &mut offset, self.terminal_store_id.as_bytes());
        copy(
            &mut encoded,
            &mut offset,
            self.stream_manifest_sha256.as_bytes(),
        );
        copy(&mut encoded, &mut offset, self.policy_id.as_bytes());
        copy(
            &mut encoded,
            &mut offset,
            &self.protected_cursor.fixed_encode(),
        );
        debug_assert_eq!(offset, ACK_V1_ENCODED_LEN);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_len("AckV1", encoded.len(), ACK_V1_ENCODED_LEN)?;
        let mut offset = 0;
        let stream_id = StreamId::try_from(take(
            encoded,
            &mut offset,
            StreamId::LENGTH,
            "ack_stream_id",
        )?)?;
        let terminal_store_id = DeletionAuthorizingStoreId::try_from(take(
            encoded,
            &mut offset,
            DeletionAuthorizingStoreId::LENGTH,
            "ack_terminal_store_id",
        )?)?;
        let stream_manifest_sha256 = StreamManifestSha256::try_from(take(
            encoded,
            &mut offset,
            StreamManifestSha256::LENGTH,
            "ack_stream_manifest_sha256",
        )?)?;
        let policy_id = DurabilityPolicyId::try_from(take(
            encoded,
            &mut offset,
            DurabilityPolicyId::LENGTH,
            "ack_policy_id",
        )?)?;
        let protected_cursor = CursorV1::decode(take(
            encoded,
            &mut offset,
            CURSOR_V1_ENCODED_LEN,
            "ack_protected_cursor",
        )?)?;
        Ok(Self::new(
            stream_id,
            terminal_store_id,
            stream_manifest_sha256,
            policy_id,
            protected_cursor,
        ))
    }

    /// Verifies all immutable ACK bindings that are carried by the canonical
    /// stream manifest. Tail/prefix validation remains a journal-index lookup.
    pub fn validate_against_manifest(&self, manifest: &StreamManifestV1) -> Result<()> {
        if self.stream_id != manifest.stream().stream_id()
            || self.stream_manifest_sha256 != manifest.stream().stream_manifest_sha256()
        {
            return Err(ProtocolError::AckBindingMismatch {
                reason: "stream identity or manifest digest differs",
            });
        }
        if manifest.deletion_authorizing_store_id() != Some(self.terminal_store_id) {
            return Err(ProtocolError::AckBindingMismatch {
                reason: "terminal store is not the deletion-authorizing store",
            });
        }
        if manifest
            .durability_policy()
            .map(|policy| policy.policy_id())
            != Some(self.policy_id)
        {
            return Err(ProtocolError::AckBindingMismatch {
                reason: "durability policy ID differs",
            });
        }
        Ok(())
    }

    /// Requires the caller's exact cursor lookup at this sequence to match the
    /// ACK. This is the final prefix-chain check before accepting the ACK.
    pub fn validate_protected_cursor(&self, expected: CursorV1) -> Result<()> {
        if self.protected_cursor != expected {
            return Err(ProtocolError::CursorMismatch {
                context: "AckV1.protected_cursor",
            });
        }
        Ok(())
    }
}

/// One digest-linked, crash-durable record of an authenticated accepted ACK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedAckReceiptV1 {
    receipt_generation: u64,
    previous_receipt_sha256: AcceptedAckReceiptSha256,
    stream: StreamHeaderV1,
    authenticated_peer_id: Vec<u8>,
    ack: AckV1,
    receipt_sha256: AcceptedAckReceiptSha256,
}

impl AcceptedAckReceiptV1 {
    /// Creates generation zero after the caller has validated `ack` against
    /// the manifest, authorized peer mapping, durable tail, and exact stream
    /// prefix via [`AckV1::validate_protected_cursor`].
    pub fn first(
        stream: StreamHeaderV1,
        authenticated_peer_id: Vec<u8>,
        ack: AckV1,
    ) -> Result<Self> {
        validate_peer_id_len(authenticated_peer_id.len() as u64)?;
        validate_ack_stream(ack, stream)?;
        validate_initial_cursor_if_zero(ack.protected_cursor, stream)?;
        Ok(Self::build(
            0,
            AcceptedAckReceiptSha256::new([0; 32]),
            stream,
            authenticated_peer_id,
            ack,
        ))
    }

    /// Returns the existing receipt for an exact duplicate ACK. Otherwise it
    /// creates the only valid next generation and requires a strict cursor
    /// advance with unchanged ACK identity fields. The caller must first
    /// validate the new cursor against the retained stream index; a digest link
    /// cannot prove membership of an arbitrary later cursor by itself.
    pub fn advance(previous: &Self, authenticated_peer_id: Vec<u8>, ack: AckV1) -> Result<Self> {
        validate_peer_id_len(authenticated_peer_id.len() as u64)?;
        if ack == previous.ack {
            return Ok(previous.clone());
        }
        validate_ack_stream(ack, previous.stream)?;
        validate_ack_identity(previous.ack, ack)?;
        validate_strict_cursor_advance(previous.ack.protected_cursor, ack.protected_cursor)?;
        let receipt_generation = previous
            .receipt_generation
            .checked_add(1)
            .ok_or(ProtocolError::ReceiptGenerationOverflow)?;
        Ok(Self::build(
            receipt_generation,
            previous.receipt_sha256,
            previous.stream,
            authenticated_peer_id,
            ack,
        ))
    }

    #[must_use]
    pub const fn receipt_generation(&self) -> u64 {
        self.receipt_generation
    }

    #[must_use]
    pub const fn previous_receipt_sha256(&self) -> AcceptedAckReceiptSha256 {
        self.previous_receipt_sha256
    }

    #[must_use]
    pub const fn stream(&self) -> StreamHeaderV1 {
        self.stream
    }

    #[must_use]
    pub fn authenticated_peer_id(&self) -> &[u8] {
        &self.authenticated_peer_id
    }

    #[must_use]
    pub const fn ack(&self) -> AckV1 {
        self.ack
    }

    #[must_use]
    pub const fn receipt_sha256(&self) -> AcceptedAckReceiptSha256 {
        self.receipt_sha256
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        ACCEPTED_ACK_RECEIPT_V1_FIXED_ENCODED_LEN + self.authenticated_peer_id.len()
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = self.canonical_body();
        encoded.extend_from_slice(self.receipt_sha256.as_bytes());
        encoded
    }

    /// Decodes and verifies one receipt plus its immediate predecessor link.
    /// Passing `None` is valid only for generation zero.
    pub fn decode(encoded: &[u8], previous: Option<&Self>) -> Result<Self> {
        require_at_least(
            "AcceptedAckReceiptV1",
            encoded.len(),
            ACCEPTED_ACK_RECEIPT_V1_FIXED_ENCODED_LEN,
        )?;
        let mut offset = 0;
        let receipt_generation = read_u64(encoded, &mut offset, "receipt_generation")?;
        let previous_receipt_sha256 = AcceptedAckReceiptSha256::try_from(take(
            encoded,
            &mut offset,
            AcceptedAckReceiptSha256::LENGTH,
            "previous_receipt_sha256",
        )?)?;
        let stream = StreamHeaderV1::decode(take(
            encoded,
            &mut offset,
            STREAM_HEADER_V1_ENCODED_LEN,
            "accepted_ack_stream",
        )?)?;
        let peer_len = read_u64(encoded, &mut offset, "authenticated_peer_id_len")?;
        validate_peer_id_len(peer_len)?;
        let peer_len = usize::try_from(peer_len).map_err(|_| ProtocolError::IntegerOverflow {
            field: "authenticated_peer_id_len",
        })?;
        let authenticated_peer_id =
            take(encoded, &mut offset, peer_len, "authenticated_peer_id")?.to_vec();
        let ack = AckV1::decode(take(
            encoded,
            &mut offset,
            ACK_V1_ENCODED_LEN,
            "accepted_ack",
        )?)?;
        let receipt_sha256 = AcceptedAckReceiptSha256::try_from(take(
            encoded,
            &mut offset,
            AcceptedAckReceiptSha256::LENGTH,
            "receipt_sha256",
        )?)?;
        if offset != encoded.len() {
            return Err(ProtocolError::TrailingBytes {
                context: "AcceptedAckReceiptV1",
                count: encoded.len() - offset,
            });
        }

        let receipt = Self {
            receipt_generation,
            previous_receipt_sha256,
            stream,
            authenticated_peer_id,
            ack,
            receipt_sha256,
        };
        receipt.validate_digest()?;
        receipt.validate_link(previous)?;
        Ok(receipt)
    }

    fn build(
        receipt_generation: u64,
        previous_receipt_sha256: AcceptedAckReceiptSha256,
        stream: StreamHeaderV1,
        authenticated_peer_id: Vec<u8>,
        ack: AckV1,
    ) -> Self {
        let mut receipt = Self {
            receipt_generation,
            previous_receipt_sha256,
            stream,
            authenticated_peer_id,
            ack,
            receipt_sha256: AcceptedAckReceiptSha256::new([0; 32]),
        };
        receipt.receipt_sha256 = accepted_ack_receipt_sha256(&receipt.canonical_body());
        receipt
    }

    fn canonical_body(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.encoded_len() - 32);
        encoded.extend_from_slice(&self.receipt_generation.to_be_bytes());
        encoded.extend_from_slice(self.previous_receipt_sha256.as_bytes());
        encoded.extend_from_slice(&self.stream.fixed_encode());
        encoded.extend_from_slice(&(self.authenticated_peer_id.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&self.authenticated_peer_id);
        encoded.extend_from_slice(&self.ack.fixed_encode());
        encoded
    }

    fn validate_digest(&self) -> Result<()> {
        if accepted_ack_receipt_sha256(&self.canonical_body()) != self.receipt_sha256 {
            return Err(ProtocolError::AcceptedAckReceiptHashMismatch);
        }
        Ok(())
    }

    fn validate_link(&self, previous: Option<&Self>) -> Result<()> {
        validate_ack_stream(self.ack, self.stream)?;
        validate_initial_cursor_if_zero(self.ack.protected_cursor, self.stream)?;
        match previous {
            None => {
                if self.receipt_generation != 0
                    || self.previous_receipt_sha256 != AcceptedAckReceiptSha256::new([0; 32])
                {
                    return Err(ProtocolError::InvalidAcceptedAckReceiptChain {
                        reason: "generation zero must have the all-zero predecessor digest",
                    });
                }
            }
            Some(previous) => {
                let expected_generation = previous
                    .receipt_generation
                    .checked_add(1)
                    .ok_or(ProtocolError::ReceiptGenerationOverflow)?;
                if self.receipt_generation != expected_generation {
                    return Err(ProtocolError::InvalidAcceptedAckReceiptChain {
                        reason: "generation does not increment its predecessor by one",
                    });
                }
                if self.previous_receipt_sha256 != previous.receipt_sha256 {
                    return Err(ProtocolError::InvalidAcceptedAckReceiptChain {
                        reason: "previous receipt digest differs",
                    });
                }
                if self.stream != previous.stream {
                    return Err(ProtocolError::StreamMismatch {
                        context: "AcceptedAckReceiptV1",
                    });
                }
                validate_ack_identity(previous.ack, self.ack)?;
                validate_strict_cursor_advance(
                    previous.ack.protected_cursor,
                    self.ack.protected_cursor,
                )?;
            }
        }
        Ok(())
    }
}

/// Computes the exact domain-separated digest of a canonical receipt body.
#[must_use]
pub fn accepted_ack_receipt_sha256(
    canonical_receipt_without_hash: &[u8],
) -> AcceptedAckReceiptSha256 {
    let mut hasher = Sha256::new();
    hasher.update(ACCEPTED_ACK_RECEIPT_DOMAIN);
    hasher.update(canonical_receipt_without_hash);
    AcceptedAckReceiptSha256::new(hasher.finalize().into())
}

/// The sole source-side deletion anchor. The storage layer must additionally
/// resolve `retired_through` in the retained stream index before using it for
/// GC; this dependency-light type validates every relationship it can prove
/// from the retained receipt and prior checkpoint alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceRetirementCheckpointV1 {
    stream: StreamHeaderV1,
    retired_through: CursorV1,
    accepted_ack_receipt_sha256: AcceptedAckReceiptSha256,
}

impl SourceRetirementCheckpointV1 {
    pub fn new(
        stream: StreamHeaderV1,
        retired_through: CursorV1,
        receipt: &AcceptedAckReceiptV1,
        previous: Option<&Self>,
    ) -> Result<Self> {
        let checkpoint = Self {
            stream,
            retired_through,
            accepted_ack_receipt_sha256: receipt.receipt_sha256,
        };
        checkpoint.validate_against(receipt, previous)?;
        Ok(checkpoint)
    }

    #[must_use]
    pub const fn stream(&self) -> StreamHeaderV1 {
        self.stream
    }

    #[must_use]
    pub const fn retired_through(&self) -> CursorV1 {
        self.retired_through
    }

    #[must_use]
    pub const fn accepted_ack_receipt_sha256(&self) -> AcceptedAckReceiptSha256 {
        self.accepted_ack_receipt_sha256
    }

    #[must_use]
    pub fn fixed_encode(&self) -> [u8; SOURCE_RETIREMENT_CHECKPOINT_V1_ENCODED_LEN] {
        let mut encoded = [0_u8; SOURCE_RETIREMENT_CHECKPOINT_V1_ENCODED_LEN];
        encoded[..STREAM_HEADER_V1_ENCODED_LEN].copy_from_slice(&self.stream.fixed_encode());
        let cursor_end = STREAM_HEADER_V1_ENCODED_LEN + CURSOR_V1_ENCODED_LEN;
        encoded[STREAM_HEADER_V1_ENCODED_LEN..cursor_end]
            .copy_from_slice(&self.retired_through.fixed_encode());
        encoded[cursor_end..].copy_from_slice(self.accepted_ack_receipt_sha256.as_bytes());
        encoded
    }

    pub fn decode(
        encoded: &[u8],
        receipt: &AcceptedAckReceiptV1,
        previous: Option<&Self>,
    ) -> Result<Self> {
        require_exact_len(
            "SourceRetirementCheckpointV1",
            encoded.len(),
            SOURCE_RETIREMENT_CHECKPOINT_V1_ENCODED_LEN,
        )?;
        let stream = StreamHeaderV1::decode(&encoded[..STREAM_HEADER_V1_ENCODED_LEN])?;
        let cursor_end = STREAM_HEADER_V1_ENCODED_LEN + CURSOR_V1_ENCODED_LEN;
        let retired_through = CursorV1::decode(&encoded[STREAM_HEADER_V1_ENCODED_LEN..cursor_end])?;
        let accepted_ack_receipt_sha256 =
            AcceptedAckReceiptSha256::try_from(&encoded[cursor_end..])?;
        let checkpoint = Self {
            stream,
            retired_through,
            accepted_ack_receipt_sha256,
        };
        checkpoint.validate_against(receipt, previous)?;
        Ok(checkpoint)
    }

    pub fn validate_against(
        &self,
        receipt: &AcceptedAckReceiptV1,
        previous: Option<&Self>,
    ) -> Result<()> {
        if self.stream != receipt.stream {
            return Err(ProtocolError::StreamMismatch {
                context: "SourceRetirementCheckpointV1.receipt",
            });
        }
        if self.accepted_ack_receipt_sha256 != receipt.receipt_sha256 {
            return Err(ProtocolError::AcceptedAckReceiptHashMismatch);
        }
        validate_initial_cursor_if_zero(self.retired_through, self.stream)?;
        validate_cursor_not_after(
            self.retired_through,
            receipt.ack.protected_cursor,
            "SourceRetirementCheckpointV1.receipt_upper_bound",
        )?;
        if let Some(previous) = previous {
            if self.stream != previous.stream {
                return Err(ProtocolError::StreamMismatch {
                    context: "SourceRetirementCheckpointV1.previous",
                });
            }
            validate_cursor_not_after(
                previous.retired_through,
                self.retired_through,
                "SourceRetirementCheckpointV1.previous_lower_bound",
            )?;
        }
        Ok(())
    }

    /// Exact hook for the retained stream index's prefix lookup. Call this
    /// before GC when the checkpoint is below the receipt's ACK cursor.
    pub fn validate_retired_prefix(&self, expected: CursorV1) -> Result<()> {
        if self.retired_through != expected {
            return Err(ProtocolError::CursorMismatch {
                context: "SourceRetirementCheckpointV1.retired_through",
            });
        }
        Ok(())
    }
}

fn validate_peer_id_len(actual: u64) -> Result<()> {
    if actual == 0 || actual > MAX_AUTHENTICATED_PEER_ID_BYTES {
        return Err(ProtocolError::InvalidAuthenticatedPeerIdLength {
            actual,
            min: 1,
            max: MAX_AUTHENTICATED_PEER_ID_BYTES,
        });
    }
    Ok(())
}

fn validate_ack_stream(ack: AckV1, stream: StreamHeaderV1) -> Result<()> {
    if ack.stream_id != stream.stream_id()
        || ack.stream_manifest_sha256 != stream.stream_manifest_sha256()
    {
        return Err(ProtocolError::AckBindingMismatch {
            reason: "nested ACK does not match its stream header",
        });
    }
    Ok(())
}

fn validate_ack_identity(previous: AckV1, next: AckV1) -> Result<()> {
    if previous.stream_id != next.stream_id
        || previous.terminal_store_id != next.terminal_store_id
        || previous.stream_manifest_sha256 != next.stream_manifest_sha256
        || previous.policy_id != next.policy_id
    {
        return Err(ProtocolError::AckBindingMismatch {
            reason: "ACK identity fields changed within one receipt chain",
        });
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

fn validate_strict_cursor_advance(previous: CursorV1, next: CursorV1) -> Result<()> {
    if next.next_sequence() <= previous.next_sequence() {
        return Err(ProtocolError::NonMonotonicAck);
    }
    Ok(())
}

fn validate_cursor_not_after(
    lower: CursorV1,
    upper: CursorV1,
    context: &'static str,
) -> Result<()> {
    if lower.next_sequence() > upper.next_sequence() {
        return Err(ProtocolError::InvalidCursorOrder { context });
    }
    if lower.next_sequence() == upper.next_sequence() && lower != upper {
        return Err(ProtocolError::CursorMismatch { context });
    }
    Ok(())
}

fn read_u64(encoded: &[u8], offset: &mut usize, context: &'static str) -> Result<u64> {
    Ok(u64::from_be_bytes(
        take(encoded, offset, 8, context)?
            .try_into()
            .expect("fixed slice"),
    ))
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

fn copy<const N: usize>(target: &mut [u8], offset: &mut usize, field: &[u8; N]) {
    let end = *offset + N;
    target[*offset..end].copy_from_slice(field);
    *offset = end;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterGenesisHash, PrefixHash, ProducerConfigSha256, RecordV1};

    const FIRST_RECEIPT_SHA256: [u8; 32] = [
        0x38, 0x25, 0x4f, 0x86, 0x8c, 0x3f, 0x07, 0xc2, 0x4d, 0x9d, 0xc3, 0x00, 0x79, 0xbe, 0x83,
        0xb3, 0xe2, 0xaa, 0x2f, 0xc4, 0xd9, 0x60, 0xda, 0x65, 0x16, 0x0f, 0xd3, 0x41, 0x0a, 0x09,
        0xa8, 0x08,
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

    fn cursors() -> (CursorV1, CursorV1, CursorV1) {
        let p0 = fixture_stream().initial_cursor();
        let first = RecordV1::new(p0, b"abc".to_vec()).unwrap();
        let p1 = first.end_cursor();
        let second = RecordV1::new(p1, vec![0, 1, 2, 255]).unwrap();
        (p0, p1, second.end_cursor())
    }

    fn ack(cursor: CursorV1) -> AckV1 {
        let stream = fixture_stream();
        AckV1::new(
            stream.stream_id(),
            DeletionAuthorizingStoreId::new([0x70; 16]),
            stream.stream_manifest_sha256(),
            DurabilityPolicyId::new([0x80; 16]),
            cursor,
        )
    }

    fn first_receipt() -> AcceptedAckReceiptV1 {
        AcceptedAckReceiptV1::first(fixture_stream(), b"peer-a".to_vec(), ack(cursors().1)).unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn ack_and_first_receipt_are_golden_and_round_trip() {
        let ack = ack(cursors().1);
        assert_eq!(
            hex(&ack.fixed_encode()),
            concat!(
                "000102030405060708090a0b0c0d0e0f",
                "70707070707070707070707070707070",
                "505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f",
                "80808080808080808080808080808080",
                "0000000000000001",
                "6917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583"
            )
        );
        assert_eq!(AckV1::decode(&ack.fixed_encode()), Ok(ack));

        let receipt = first_receipt();
        assert_eq!(receipt.receipt_sha256.into_bytes(), FIRST_RECEIPT_SHA256);
        assert_eq!(
            AcceptedAckReceiptV1::decode(&receipt.encode(), None),
            Ok(receipt)
        );
    }

    #[test]
    fn receipt_duplicate_is_idempotent_and_advance_is_digest_linked() {
        let first = first_receipt();
        assert_eq!(
            AcceptedAckReceiptV1::advance(&first, b"peer-b".to_vec(), first.ack()).unwrap(),
            first
        );

        let second =
            AcceptedAckReceiptV1::advance(&first, b"peer-b".to_vec(), ack(cursors().2)).unwrap();
        assert_eq!(second.receipt_generation(), 1);
        assert_eq!(second.previous_receipt_sha256(), first.receipt_sha256());
        assert_eq!(
            AcceptedAckReceiptV1::decode(&second.encode(), Some(&first)),
            Ok(second)
        );
    }

    #[test]
    fn receipt_rejects_peer_bounds_hash_trailing_and_wrong_predecessor() {
        assert!(matches!(
            AcceptedAckReceiptV1::first(fixture_stream(), Vec::new(), ack(cursors().1)),
            Err(ProtocolError::InvalidAuthenticatedPeerIdLength { .. })
        ));

        let invalid_empty_peer = AcceptedAckReceiptV1::build(
            0,
            AcceptedAckReceiptSha256::new([0; 32]),
            fixture_stream(),
            Vec::new(),
            ack(cursors().1),
        );
        assert!(matches!(
            AcceptedAckReceiptV1::decode(&invalid_empty_peer.encode(), None),
            Err(ProtocolError::InvalidAuthenticatedPeerIdLength { .. })
        ));

        let receipt = first_receipt();
        let mut wrong_hash = receipt.encode();
        *wrong_hash.last_mut().unwrap() ^= 1;
        assert_eq!(
            AcceptedAckReceiptV1::decode(&wrong_hash, None),
            Err(ProtocolError::AcceptedAckReceiptHashMismatch)
        );

        let mut trailing = receipt.encode();
        trailing.push(0);
        assert!(matches!(
            AcceptedAckReceiptV1::decode(&trailing, None),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let peer_len_offset = 8 + 32 + STREAM_HEADER_V1_ENCODED_LEN;
        let mut oversized = receipt.encode();
        oversized[peer_len_offset..peer_len_offset + 8]
            .copy_from_slice(&(MAX_AUTHENTICATED_PEER_ID_BYTES + 1).to_be_bytes());
        assert!(matches!(
            AcceptedAckReceiptV1::decode(&oversized, None),
            Err(ProtocolError::InvalidAuthenticatedPeerIdLength { .. })
        ));

        let second =
            AcceptedAckReceiptV1::advance(&receipt, b"peer-b".to_vec(), ack(cursors().2)).unwrap();
        assert!(matches!(
            AcceptedAckReceiptV1::decode(&second.encode(), None),
            Err(ProtocolError::InvalidAcceptedAckReceiptChain { .. })
        ));
    }

    #[test]
    fn lower_or_identity_changed_ack_cannot_advance_a_receipt() {
        let first = first_receipt();
        assert_eq!(
            AcceptedAckReceiptV1::advance(&first, b"peer".to_vec(), ack(cursors().0)),
            Err(ProtocolError::NonMonotonicAck)
        );
        assert_eq!(
            AcceptedAckReceiptV1::advance(
                &first,
                b"peer".to_vec(),
                ack(CursorV1::new(
                    cursors().1.next_sequence(),
                    PrefixHash::new([9; 32]),
                )),
            ),
            Err(ProtocolError::NonMonotonicAck)
        );

        let mut changed = ack(cursors().2);
        changed.policy_id = DurabilityPolicyId::new([9; 16]);
        assert!(matches!(
            AcceptedAckReceiptV1::advance(&first, b"peer".to_vec(), changed),
            Err(ProtocolError::AckBindingMismatch { .. })
        ));
    }

    #[test]
    fn retirement_checkpoint_is_exact_and_bounded_by_receipt_and_prior() {
        let receipt = first_receipt();
        let checkpoint =
            SourceRetirementCheckpointV1::new(fixture_stream(), cursors().1, &receipt, None)
                .unwrap();
        assert_eq!(
            hex(&checkpoint.fixed_encode()),
            concat!(
                "000102030405060708090a0b0c0d0e0f",
                "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f",
                "000000020001",
                "303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f",
                "505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f",
                "0000000000000001",
                "6917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583",
                "38254f868c3f07c24d9dc30079be83b3e2aa2fc4d960da65160fd3410a09a808"
            )
        );
        assert_eq!(
            SourceRetirementCheckpointV1::decode(&checkpoint.fixed_encode(), &receipt, None),
            Ok(checkpoint)
        );

        assert!(matches!(
            SourceRetirementCheckpointV1::new(fixture_stream(), cursors().2, &receipt, None,),
            Err(ProtocolError::InvalidCursorOrder { .. })
        ));

        let prior = checkpoint;
        assert!(matches!(
            SourceRetirementCheckpointV1::new(
                fixture_stream(),
                cursors().0,
                &receipt,
                Some(&prior),
            ),
            Err(ProtocolError::InvalidCursorOrder { .. })
        ));

        assert!(matches!(
            checkpoint.validate_retired_prefix(CursorV1::new(
                checkpoint.retired_through().next_sequence(),
                PrefixHash::new([9; 32]),
            )),
            Err(ProtocolError::CursorMismatch { .. })
        ));
    }

    #[test]
    fn fixed_ack_and_checkpoint_decoders_reject_trailing_bytes() {
        let ack = ack(cursors().1);
        let mut encoded_ack = ack.fixed_encode().to_vec();
        encoded_ack.push(0);
        assert!(matches!(
            AckV1::decode(&encoded_ack),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let receipt = first_receipt();
        let checkpoint =
            SourceRetirementCheckpointV1::new(fixture_stream(), cursors().1, &receipt, None)
                .unwrap();
        let mut encoded_checkpoint = checkpoint.fixed_encode().to_vec();
        encoded_checkpoint.push(0);
        assert!(matches!(
            SourceRetirementCheckpointV1::decode(&encoded_checkpoint, &receipt, None),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));
    }
}
