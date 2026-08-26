//! Solana's canonical compact `u16` sequence-length encoding.

use wincode::{
    ReadError, ReadResult, WriteResult,
    config::ConfigCore,
    error::write_length_encoding_overflow,
    io::{Reader, Writer},
    len::SeqLen,
};

/// A Wincode sequence-length marker for Solana's one-to-three-byte `u16`.
///
/// This type is intended for use with [`wincode::containers`] adapters. It
/// rejects truncated, overflowing, and non-canonical encodings while reading.
pub struct ShortU16;

#[derive(Debug)]
enum DecodeError<E> {
    Input(E),
    NonCanonical,
    Overflow,
}

/// Return a canonical Solana short-vector length and its encoded byte count.
///
/// The encoding uses seven payload bits per byte and accepts values through
/// `u16::MAX`. Aliases such as `[0x80, 0x00]`, a continued third byte, and
/// values above `u16::MAX` are rejected.
#[allow(clippy::result_unit_err)]
pub fn decode_shortu16_len(bytes: &[u8]) -> Result<(usize, usize), ()> {
    let mut bytes = bytes.iter().copied();
    decode_short_u16(|| bytes.next().ok_or(())).map_err(|_| ())
}

#[inline]
fn decode_short_u16<E>(
    mut take_byte: impl FnMut() -> Result<u8, E>,
) -> Result<(usize, usize), DecodeError<E>> {
    let first = take_byte().map_err(DecodeError::Input)?;
    if first < 0x80 {
        return Ok((usize::from(first), 1));
    }

    let second = take_byte().map_err(DecodeError::Input)?;
    if second == 0 {
        return Err(DecodeError::NonCanonical);
    }
    if second < 0x80 {
        let value = usize::from(first & 0x7f) | (usize::from(second) << 7);
        return Ok((value, 2));
    }

    let third = take_byte().map_err(DecodeError::Input)?;
    if third == 0 {
        return Err(DecodeError::NonCanonical);
    }
    if third > 3 {
        return Err(DecodeError::Overflow);
    }

    let value =
        usize::from(first & 0x7f) | (usize::from(second & 0x7f) << 7) | (usize::from(third) << 14);
    Ok((value, 3))
}

#[inline]
fn read_short_u16<'de>(mut reader: impl Reader<'de>) -> ReadResult<usize> {
    decode_short_u16(|| reader.take_byte())
        .map(|(value, _)| value)
        .map_err(|error| match error {
            DecodeError::Input(error) => error.into(),
            DecodeError::NonCanonical => {
                ReadError::InvalidValue("short u16: non-canonical encoding")
            }
            DecodeError::Overflow => ReadError::LengthEncodingOverflow("u16::MAX"),
        })
}

#[inline]
fn encode_short_u16(value: u16) -> ([u8; 3], usize) {
    let mut bytes = [0_u8; 3];
    let encoded_len = match value {
        0..=0x7f => {
            bytes[0] = value as u8;
            1
        }
        0x80..=0x3fff => {
            bytes[0] = ((value & 0x7f) as u8) | 0x80;
            bytes[1] = (value >> 7) as u8;
            2
        }
        _ => {
            bytes[0] = ((value & 0x7f) as u8) | 0x80;
            bytes[1] = (((value >> 7) & 0x7f) as u8) | 0x80;
            bytes[2] = (value >> 14) as u8;
            3
        }
    };
    (bytes, encoded_len)
}

unsafe impl<C: ConfigCore> SeqLen<C> for ShortU16 {
    #[inline]
    fn read<'de>(reader: impl Reader<'de>) -> ReadResult<usize> {
        read_short_u16(reader)
    }

    #[inline]
    fn write(mut writer: impl Writer, len: usize) -> WriteResult<()> {
        let len = u16::try_from(len).map_err(|_| write_length_encoding_overflow("u16::MAX"))?;
        let (bytes, encoded_len) = encode_short_u16(len);
        writer.write(&bytes[..encoded_len])?;
        Ok(())
    }

    #[inline]
    fn write_bytes_needed(len: usize) -> WriteResult<usize> {
        let len = u16::try_from(len).map_err(|_| write_length_encoding_overflow("u16::MAX"))?;
        Ok(1 + usize::from(len >= 0x80) + usize::from(len >= 0x4000))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wincode::{SchemaRead, SchemaWrite, containers};

    #[derive(Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
    struct ShortBytes {
        #[wincode(with = "containers::Vec<u8, ShortU16>")]
        bytes: Vec<u8>,
    }

    #[test]
    fn helper_decodes_canonical_boundaries_and_reports_consumed_bytes() {
        for (encoded, expected) in [
            (&[0x00][..], (0, 1)),
            (&[0x7f][..], (0x7f, 1)),
            (&[0x80, 0x01][..], (0x80, 2)),
            (&[0xff, 0x7f][..], (0x3fff, 2)),
            (&[0x80, 0x80, 0x01][..], (0x4000, 3)),
            (&[0xff, 0xff, 0x03][..], (0xffff, 3)),
        ] {
            assert_eq!(decode_shortu16_len(encoded), Ok(expected));
        }

        assert_eq!(decode_shortu16_len(&[0x80, 0x01, 0xaa]), Ok((0x80, 2)));
    }

    #[test]
    fn helper_rejects_truncated_alias_and_overflow_encodings() {
        for encoded in [
            &[][..],
            &[0x80][..],
            &[0x80, 0x80][..],
            &[0x80, 0x00][..],
            &[0xff, 0x00][..],
            &[0x80, 0x80, 0x00][..],
            &[0xff, 0xff, 0x04][..],
            &[0xff, 0xff, 0x80][..],
        ] {
            assert_eq!(decode_shortu16_len(encoded), Err(()), "{encoded:02x?}");
        }
    }

    #[test]
    fn wincode_length_marker_round_trips_boundaries() {
        for (len, prefix) in [
            (0, &[0x00][..]),
            (0x7f, &[0x7f][..]),
            (0x80, &[0x80, 0x01][..]),
            (0x3fff, &[0xff, 0x7f][..]),
            (0x4000, &[0x80, 0x80, 0x01][..]),
            (0xffff, &[0xff, 0xff, 0x03][..]),
        ] {
            let value = ShortBytes {
                bytes: vec![0x5a; len],
            };
            let encoded = wincode::serialize(&value).expect("encode short vector");
            assert!(encoded.starts_with(prefix));
            assert_eq!(
                wincode::deserialize::<ShortBytes>(&encoded).expect("decode short vector"),
                value
            );
        }
    }

    #[test]
    fn wincode_length_marker_rejects_non_canonical_prefixes() {
        for encoded in [
            &[0x80, 0x00][..],
            &[0x80, 0x80, 0x00][..],
            &[0xff, 0xff, 0x04][..],
        ] {
            assert!(wincode::deserialize::<ShortBytes>(encoded).is_err());
        }
    }
}
