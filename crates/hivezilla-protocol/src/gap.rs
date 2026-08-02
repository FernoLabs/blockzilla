use crate::{
    CURSOR_V1_ENCODED_LEN, CursorV1, GapEventProducerDescriptorV1, GapEventReasonV1, ProtocolError,
    Result, StreamId,
};

pub const MAX_GAP_EVENT_POSITION_BYTES: u64 = 4_096;
pub const GAP_EVENT_PAYLOAD_V1_FIXED_ENCODED_LEN: usize =
    StreamId::LENGTH + CURSOR_V1_ENCODED_LEN + 2 + 8 + 8;
pub const MAX_GAP_EVENT_PAYLOAD_V1_ENCODED_LEN: usize =
    GAP_EVENT_PAYLOAD_V1_FIXED_ENCODED_LEN + 2 * MAX_GAP_EVENT_POSITION_BYTES as usize;

/// Exact operational evidence carried as a format-7 record payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GapEventPayloadV1 {
    target_stream_id: StreamId,
    observed_at: CursorV1,
    reason: GapEventReasonV1,
    expected_source_position: Vec<u8>,
    observed_source_position: Vec<u8>,
}

impl GapEventPayloadV1 {
    pub fn new(
        target_stream_id: StreamId,
        observed_at: CursorV1,
        reason: GapEventReasonV1,
        expected_source_position: Vec<u8>,
        observed_source_position: Vec<u8>,
    ) -> Result<Self> {
        validate_position_len(
            "expected_source_position",
            expected_source_position.len() as u64,
        )?;
        validate_position_len(
            "observed_source_position",
            observed_source_position.len() as u64,
        )?;
        Ok(Self {
            target_stream_id,
            observed_at,
            reason,
            expected_source_position,
            observed_source_position,
        })
    }

    #[must_use]
    pub const fn target_stream_id(&self) -> StreamId {
        self.target_stream_id
    }

    #[must_use]
    pub const fn observed_at(&self) -> CursorV1 {
        self.observed_at
    }

    #[must_use]
    pub const fn reason(&self) -> GapEventReasonV1 {
        self.reason
    }

    #[must_use]
    pub fn expected_source_position(&self) -> &[u8] {
        &self.expected_source_position
    }

    #[must_use]
    pub fn observed_source_position(&self) -> &[u8] {
        &self.observed_source_position
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        GAP_EVENT_PAYLOAD_V1_FIXED_ENCODED_LEN
            + self.expected_source_position.len()
            + self.observed_source_position.len()
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.encoded_len());
        encoded.extend_from_slice(self.target_stream_id.as_bytes());
        encoded.extend_from_slice(&self.observed_at.fixed_encode());
        encoded.extend_from_slice(&(self.reason as u16).to_be_bytes());
        encoded.extend_from_slice(&(self.expected_source_position.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&self.expected_source_position);
        encoded.extend_from_slice(&(self.observed_source_position.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&self.observed_source_position);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let target_stream_id =
            StreamId::try_from(reader.take(StreamId::LENGTH, "gap_event_target_stream_id")?)?;
        let observed_at =
            CursorV1::decode(reader.take(CURSOR_V1_ENCODED_LEN, "gap_event_observed_at")?)?;
        let reason = GapEventReasonV1::try_from(reader.u16("gap_event_reason")?)?;
        let expected_source_position = reader.position("expected_source_position")?;
        let observed_source_position = reader.position("observed_source_position")?;
        reader.finish("GapEventPayloadV1")?;
        Self::new(
            target_stream_id,
            observed_at,
            reason,
            expected_source_position,
            observed_source_position,
        )
    }

    /// Binds this otherwise opaque payload to the canonical producer
    /// descriptor of its format-7 stream.
    pub fn validate_against_descriptor(
        &self,
        descriptor: &GapEventProducerDescriptorV1,
    ) -> Result<()> {
        if self.target_stream_id != descriptor.target_stream_id() {
            return Err(ProtocolError::GapEventDescriptorMismatch {
                reason: "target stream ID differs",
            });
        }
        if descriptor
            .permitted_reasons()
            .binary_search(&self.reason)
            .is_err()
        {
            return Err(ProtocolError::GapEventDescriptorMismatch {
                reason: "event reason is not permitted",
            });
        }
        Ok(())
    }
}

fn validate_position_len(field: &'static str, actual: u64) -> Result<()> {
    if actual > MAX_GAP_EVENT_POSITION_BYTES {
        return Err(ProtocolError::GapEventPositionTooLarge {
            field,
            actual,
            max: MAX_GAP_EVENT_POSITION_BYTES,
        });
    }
    Ok(())
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

    fn u16(&mut self, field: &'static str) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(2, field)?.try_into().expect("fixed slice"),
        ))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8, field)?.try_into().expect("fixed slice"),
        ))
    }

    fn position(&mut self, field: &'static str) -> Result<Vec<u8>> {
        let length = self.u64(field)?;
        validate_position_len(field, length)?;
        let length =
            usize::try_from(length).map_err(|_| ProtocolError::IntegerOverflow { field })?;
        Ok(self.take(length, field)?.to_vec())
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
    use crate::{PrefixHash, ProducerConfigSha256};

    fn fixture() -> GapEventPayloadV1 {
        GapEventPayloadV1::new(
            StreamId::new([0x11; 16]),
            CursorV1::new(7, PrefixHash::new([0x22; 32])),
            GapEventReasonV1::UdpDropCounter,
            b"ab".to_vec(),
            Vec::new(),
        )
        .unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn gap_event_payload_is_golden_and_round_trips() {
        let event = fixture();
        let encoded = event.encode();
        assert_eq!(
            hex(&encoded),
            concat!(
                "11111111111111111111111111111111",
                "0000000000000007",
                "2222222222222222222222222222222222222222222222222222222222222222",
                "0002",
                "0000000000000002",
                "6162",
                "0000000000000000"
            )
        );
        assert_eq!(GapEventPayloadV1::decode(&encoded), Ok(event));
    }

    #[test]
    fn positions_are_bounded_before_allocation_and_may_be_empty() {
        let empty = GapEventPayloadV1::new(
            StreamId::new([1; 16]),
            CursorV1::new(0, PrefixHash::new([2; 32])),
            GapEventReasonV1::OperatorDeclared,
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(GapEventPayloadV1::decode(&empty.encode()), Ok(empty));

        let mut oversized = Vec::new();
        oversized.extend_from_slice(&[1; 16]);
        oversized.extend_from_slice(&CursorV1::new(0, PrefixHash::new([2; 32])).fixed_encode());
        oversized.extend_from_slice(&(GapEventReasonV1::OperatorDeclared as u16).to_be_bytes());
        oversized.extend_from_slice(&(MAX_GAP_EVENT_POSITION_BYTES + 1).to_be_bytes());
        oversized.extend_from_slice(&0_u64.to_be_bytes());
        assert!(matches!(
            GapEventPayloadV1::decode(&oversized),
            Err(ProtocolError::GapEventPositionTooLarge {
                field: "expected_source_position",
                ..
            })
        ));
    }

    #[test]
    fn unknown_reason_truncation_and_trailing_bytes_are_rejected() {
        let encoded = fixture().encode();
        assert!(matches!(
            GapEventPayloadV1::decode(&encoded[..encoded.len() - 1]),
            Err(ProtocolError::Truncated { .. })
        ));

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            GapEventPayloadV1::decode(&trailing),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));

        let mut unknown = encoded;
        let reason_offset = StreamId::LENGTH + CURSOR_V1_ENCODED_LEN;
        unknown[reason_offset..reason_offset + 2].copy_from_slice(&99_u16.to_be_bytes());
        assert_eq!(
            GapEventPayloadV1::decode(&unknown),
            Err(ProtocolError::UnknownGapEventReason { value: 99 })
        );
    }

    #[test]
    fn producer_descriptor_binds_target_and_permitted_reason() {
        let event = fixture();
        let descriptor = GapEventProducerDescriptorV1::new(
            event.target_stream_id(),
            ProducerConfigSha256::new([3; 32]),
            vec![GapEventReasonV1::UdpDropCounter],
            b"position-schema".to_vec(),
        )
        .unwrap();
        assert_eq!(event.validate_against_descriptor(&descriptor), Ok(()));

        let wrong_target = GapEventProducerDescriptorV1::new(
            StreamId::new([9; 16]),
            ProducerConfigSha256::new([3; 32]),
            vec![GapEventReasonV1::UdpDropCounter],
            b"position-schema".to_vec(),
        )
        .unwrap();
        assert!(matches!(
            event.validate_against_descriptor(&wrong_target),
            Err(ProtocolError::GapEventDescriptorMismatch { .. })
        ));
    }
}
