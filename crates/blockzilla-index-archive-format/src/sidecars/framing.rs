//! Canonical retained-sidecar frame boundaries.

use thiserror::Error;

pub const MAX_FRAME_BYTES: usize = 64 << 20;
pub const MAX_PREFIX_BYTES: usize = 5;

/// Add one canonical unsigned-LEB128 u32 length before a Wincode 0.5.5 payload.
pub fn encode_frame(payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(FrameError::TooLong(payload.len()));
    }
    let mut value = u32::try_from(payload.len()).map_err(|_| FrameError::TooLong(payload.len()))?;
    let mut frame = Vec::with_capacity(MAX_PREFIX_BYTES.min(payload.len()) + payload.len());
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        frame.push(byte);
        if value == 0 {
            break;
        }
    }
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Return the one payload from a complete catalog-addressed frame.
pub fn decode_frame(frame: &[u8]) -> Result<&[u8], FrameError> {
    let (prefix_len, payload_len) = decode_prefix(frame)?;
    if payload_len > MAX_FRAME_BYTES {
        return Err(FrameError::TooLong(payload_len));
    }
    let expected = prefix_len
        .checked_add(payload_len)
        .ok_or(FrameError::LengthOverflow)?;
    if frame.len() != expected {
        return Err(FrameError::LengthMismatch {
            declared: payload_len,
            frame: frame.len(),
            prefix: prefix_len,
        });
    }
    Ok(&frame[prefix_len..])
}

fn decode_prefix(frame: &[u8]) -> Result<(usize, usize), FrameError> {
    let mut value = 0_u32;
    for (index, shift) in [0_u32, 7, 14, 21, 28].into_iter().enumerate() {
        let byte = *frame.get(index).ok_or(FrameError::TruncatedPrefix)?;
        let payload = u32::from(byte & 0x7f);
        if shift == 28 && payload > 0x0f {
            return Err(FrameError::LengthOverflow);
        }
        value |= payload << shift;
        if byte & 0x80 == 0 {
            if index != 0 && payload == 0 {
                return Err(FrameError::NonCanonicalPrefix);
            }
            return Ok((index + 1, value as usize));
        }
    }
    Err(FrameError::PrefixTooLong)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FrameError {
    #[error("sidecar frame length prefix is truncated")]
    TruncatedPrefix,
    #[error("sidecar frame length prefix is not minimal")]
    NonCanonicalPrefix,
    #[error("sidecar frame length prefix is too long")]
    PrefixTooLong,
    #[error("sidecar frame length overflows u32")]
    LengthOverflow,
    #[error("sidecar frame payload has {0} bytes, above the decode guard")]
    TooLong(usize),
    #[error(
        "sidecar frame declares {declared} payload bytes but has {frame} total bytes with a {prefix}-byte prefix"
    )]
    LengthMismatch {
        declared: usize,
        frame: usize,
        prefix: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_frames_round_trip_boundaries() {
        for length in [0, 1, 127, 128, 16_384] {
            let payload = vec![7; length];
            let frame = encode_frame(&payload).unwrap();
            assert_eq!(decode_frame(&frame).unwrap(), payload);
        }
    }

    #[test]
    fn padded_prefix_and_trailing_bytes_are_rejected() {
        assert_eq!(
            decode_frame(&[0x80, 0]),
            Err(FrameError::NonCanonicalPrefix)
        );
        assert!(matches!(
            decode_frame(&[1, 7, 8]),
            Err(FrameError::LengthMismatch { .. })
        ));
    }
}
