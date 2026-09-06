//! ULEB128 with a canonical-encoding rule.
//!
//! Column and posting pages must be byte-reproducible: `rebuild-indexes
//! --verify` compares a rebuilt index against the published bytes, so two
//! encoders that disagree on padding would report a false violation. A
//! multi-byte encoding whose final group is zero is therefore rejected on
//! read, not silently accepted.

use thiserror::Error;

/// Largest number of bytes a canonical `u64` encoding can occupy.
pub const MAX_ULEB128_LEN: usize = 10;

pub fn write_uleb128(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let group = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(group);
            return;
        }
        out.push(group | 0x80);
    }
}

/// Read one canonical ULEB128 value, advancing `cursor`.
pub fn read_uleb128(input: &[u8], cursor: &mut usize) -> Result<u64, VarintError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    let start = *cursor;
    loop {
        let byte = *input.get(*cursor).ok_or(VarintError::Truncated)?;
        *cursor += 1;
        let group = u64::from(byte & 0x7f);
        if shift >= 64 || (shift == 63 && group > 1) {
            return Err(VarintError::Overflow);
        }
        value |= group << shift;
        if byte & 0x80 == 0 {
            // A trailing zero group is only canonical when it is the whole
            // encoding, i.e. the value zero written as a single byte.
            if byte == 0 && *cursor - start > 1 {
                return Err(VarintError::NonCanonical);
            }
            return Ok(value);
        }
        shift += 7;
        if *cursor - start >= MAX_ULEB128_LEN {
            return Err(VarintError::Overflow);
        }
    }
}

pub fn read_uleb128_u32(input: &[u8], cursor: &mut usize) -> Result<u32, VarintError> {
    let value = read_uleb128(input, cursor)?;
    u32::try_from(value).map_err(|_| VarintError::Overflow)
}

/// Zigzag-map a signed value so small magnitudes stay one byte either way.
pub fn write_zigzag(out: &mut Vec<u8>, value: i64) {
    write_uleb128(out, ((value << 1) ^ (value >> 63)) as u64);
}

pub fn read_zigzag(input: &[u8], cursor: &mut usize) -> Result<i64, VarintError> {
    let raw = read_uleb128(input, cursor)?;
    Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum VarintError {
    #[error("varint runs past the end of the page")]
    Truncated,
    #[error("varint does not fit in the target integer")]
    Overflow,
    #[error("varint uses a non-canonical padded encoding")]
    NonCanonical,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: u64) {
        let mut buffer = Vec::new();
        write_uleb128(&mut buffer, value);
        let mut cursor = 0;
        assert_eq!(read_uleb128(&buffer, &mut cursor), Ok(value));
        assert_eq!(cursor, buffer.len());
    }

    #[test]
    fn boundaries_round_trip() {
        for value in [
            0,
            1,
            126,
            127,
            128,
            16_383,
            16_384,
            u32::MAX as u64,
            u64::MAX,
        ] {
            round_trip(value);
        }
    }

    #[test]
    fn padded_encodings_are_rejected() {
        // 0x80 0x00 decodes to zero under a permissive reader but is not the
        // canonical single-byte form.
        let mut cursor = 0;
        assert_eq!(
            read_uleb128(&[0x80, 0x00], &mut cursor),
            Err(VarintError::NonCanonical)
        );
        let mut cursor = 0;
        assert_eq!(
            read_uleb128(&[0x81, 0x80, 0x00], &mut cursor),
            Err(VarintError::NonCanonical)
        );
    }

    #[test]
    fn truncated_and_overlong_are_rejected() {
        let mut cursor = 0;
        assert_eq!(
            read_uleb128(&[0x80], &mut cursor),
            Err(VarintError::Truncated)
        );
        let mut cursor = 0;
        assert_eq!(
            read_uleb128(&[0xff; MAX_ULEB128_LEN + 1], &mut cursor),
            Err(VarintError::Overflow)
        );
    }

    #[test]
    fn zigzag_keeps_small_magnitudes_short() {
        for value in [0i64, -1, 1, -63, 63, -64, 64, i64::MIN, i64::MAX] {
            let mut buffer = Vec::new();
            write_zigzag(&mut buffer, value);
            let mut cursor = 0;
            assert_eq!(read_zigzag(&buffer, &mut cursor), Ok(value));
            assert_eq!(cursor, buffer.len());
        }
        // A balance that did not move costs one byte, not ten.
        let mut buffer = Vec::new();
        write_zigzag(&mut buffer, 0);
        assert_eq!(buffer.len(), 1);
        let mut buffer = Vec::new();
        write_zigzag(&mut buffer, -5_000);
        assert!(buffer.len() <= 2);
    }

    #[test]
    fn u32_narrowing_rejects_wide_values() {
        let mut buffer = Vec::new();
        write_uleb128(&mut buffer, u64::from(u32::MAX) + 1);
        let mut cursor = 0;
        assert_eq!(
            read_uleb128_u32(&buffer, &mut cursor),
            Err(VarintError::Overflow)
        );
    }
}
