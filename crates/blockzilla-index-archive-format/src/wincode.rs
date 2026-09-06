//! One Wincode grammar for every structured archive object.
//!
//! The format uses Wincode 0.6 with canonical unsigned LEB128 integers,
//! zigzag-encoded signed integers, one-byte enum tags, and a 64 MiB allocation
//! guard. Object headers select the schema before this decoder is used. A
//! reader must never try another type after a decode error.

use ::wincode::{
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
    config::Configuration,
    error::invalid_value,
    int_encoding::{ByteOrder, IntEncoding, LittleEndian},
    io::{Reader, Writer},
    len::BincodeLen,
};

/// Maximum allocation that one decoded sequence can request.
pub const PREALLOCATION_SIZE_LIMIT: usize = 64 << 20;

/// Canonical archive Wincode configuration.
pub type ArchiveWincodeConfig =
    Configuration<true, PREALLOCATION_SIZE_LIMIT, BincodeLen, LittleEndian, CanonicalLeb128, u8>;

/// Construct the one archive Wincode configuration.
#[inline]
pub const fn archive_wincode_config() -> ArchiveWincodeConfig {
    Configuration::default()
        .with_preallocation_size_limit::<PREALLOCATION_SIZE_LIMIT>()
        .with_int_encoding::<CanonicalLeb128>()
        .with_tag_encoding::<u8>()
}

/// Serialize one current-schema value.
#[inline]
pub fn encode<T>(value: &T) -> WriteResult<Vec<u8>>
where
    T: SchemaWrite<ArchiveWincodeConfig, Src = T> + ?Sized,
{
    ::wincode::config::serialize(value, archive_wincode_config())
}

/// Deserialize one value and reject trailing bytes.
#[inline]
pub fn decode_exact<'de, T>(bytes: &'de [u8]) -> ReadResult<T>
where
    T: SchemaRead<'de, ArchiveWincodeConfig, Dst = T>,
{
    ::wincode::config::deserialize_exact(bytes, archive_wincode_config())
}

/// Unsigned LEB128 and zigzag signed integers with one canonical byte string
/// for each value.
#[derive(Debug, Clone, Copy)]
pub struct CanonicalLeb128;

unsafe impl<B: ByteOrder> IntEncoding<B> for CanonicalLeb128 {
    const STATIC: bool = false;
    const ZERO_COPY: bool = false;

    fn encode_u16(value: u16, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned(u128::from(value), writer)
    }

    fn size_of_u16(value: u16) -> usize {
        unsigned_size(u128::from(value))
    }

    fn decode_u16<'de>(reader: impl Reader<'de>) -> ReadResult<u16> {
        decode_unsigned(reader, u16::BITS).map(|value| value as u16)
    }

    fn encode_u32(value: u32, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned(u128::from(value), writer)
    }

    fn size_of_u32(value: u32) -> usize {
        unsigned_size(u128::from(value))
    }

    fn decode_u32<'de>(reader: impl Reader<'de>) -> ReadResult<u32> {
        decode_unsigned(reader, u32::BITS).map(|value| value as u32)
    }

    fn encode_u64(value: u64, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned(u128::from(value), writer)
    }

    fn size_of_u64(value: u64) -> usize {
        unsigned_size(u128::from(value))
    }

    fn decode_u64<'de>(reader: impl Reader<'de>) -> ReadResult<u64> {
        decode_unsigned(reader, u64::BITS).map(|value| value as u64)
    }

    fn encode_u128(value: u128, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned(value, writer)
    }

    fn size_of_u128(value: u128) -> usize {
        unsigned_size(value)
    }

    fn decode_u128<'de>(reader: impl Reader<'de>) -> ReadResult<u128> {
        decode_unsigned(reader, u128::BITS)
    }

    fn encode_i16(value: i16, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned(u128::from(zigzag_i16(value)), writer)
    }

    fn size_of_i16(value: i16) -> usize {
        unsigned_size(u128::from(zigzag_i16(value)))
    }

    fn decode_i16<'de>(reader: impl Reader<'de>) -> ReadResult<i16> {
        decode_unsigned(reader, u16::BITS).map(|value| unzigzag_i16(value as u16))
    }

    fn encode_i32(value: i32, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned(u128::from(zigzag_i32(value)), writer)
    }

    fn size_of_i32(value: i32) -> usize {
        unsigned_size(u128::from(zigzag_i32(value)))
    }

    fn decode_i32<'de>(reader: impl Reader<'de>) -> ReadResult<i32> {
        decode_unsigned(reader, u32::BITS).map(|value| unzigzag_i32(value as u32))
    }

    fn encode_i64(value: i64, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned(u128::from(zigzag_i64(value)), writer)
    }

    fn size_of_i64(value: i64) -> usize {
        unsigned_size(u128::from(zigzag_i64(value)))
    }

    fn decode_i64<'de>(reader: impl Reader<'de>) -> ReadResult<i64> {
        decode_unsigned(reader, u64::BITS).map(|value| unzigzag_i64(value as u64))
    }

    fn encode_i128(value: i128, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned(zigzag_i128(value), writer)
    }

    fn size_of_i128(value: i128) -> usize {
        unsigned_size(zigzag_i128(value))
    }

    fn decode_i128<'de>(reader: impl Reader<'de>) -> ReadResult<i128> {
        decode_unsigned(reader, u128::BITS).map(unzigzag_i128)
    }
}

#[inline]
const fn unsigned_size(mut value: u128) -> usize {
    let mut size = 1;
    while value >= 0x80 {
        value >>= 7;
        size += 1;
    }
    size
}

fn encode_unsigned(mut value: u128, mut writer: impl Writer) -> WriteResult<()> {
    let mut bytes = [0_u8; 19];
    let mut len = 0;
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        bytes[len] = byte;
        len += 1;
        if value == 0 {
            break;
        }
    }
    writer.write(&bytes[..len])?;
    Ok(())
}

fn decode_unsigned<'de>(mut reader: impl Reader<'de>, bits: u32) -> ReadResult<u128> {
    let maximum = if bits == u128::BITS {
        u128::MAX
    } else {
        (1_u128 << bits) - 1
    };
    let max_bytes = bits.div_ceil(7) as usize;
    let mut value = 0_u128;

    for index in 0..max_bytes {
        let byte = reader.take_byte()?;
        let payload = u128::from(byte & 0x7f);
        let shift = (index * 7) as u32;
        if payload > (u128::MAX >> shift) {
            return Err(invalid_value("LEB128 integer overflow"));
        }
        value |= payload << shift;

        if byte & 0x80 == 0 {
            if value > maximum {
                return Err(invalid_value("LEB128 integer overflow"));
            }
            if index != 0 && payload == 0 {
                return Err(invalid_value("non-canonical LEB128 integer"));
            }
            return Ok(value);
        }
    }

    Err(invalid_value("LEB128 integer overflow"))
}

macro_rules! zigzag_pair {
    ($encode:ident, $decode:ident, $signed:ty, $unsigned:ty) => {
        #[inline]
        const fn $encode(value: $signed) -> $unsigned {
            let unsigned = value as $unsigned;
            unsigned.wrapping_shl(1) ^ ((value >> (<$signed>::BITS - 1)) as $unsigned)
        }

        #[inline]
        const fn $decode(value: $unsigned) -> $signed {
            ((value >> 1) as $signed) ^ (-((value & 1) as $signed))
        }
    };
}

zigzag_pair!(zigzag_i16, unzigzag_i16, i16, u16);
zigzag_pair!(zigzag_i32, unzigzag_i32, i32, u32);
zigzag_pair!(zigzag_i64, unzigzag_i64, i64, u64);
zigzag_pair!(zigzag_i128, unzigzag_i128, i128, u128);

#[cfg(test)]
mod tests {
    use super::*;
    use ::wincode::{SchemaRead, SchemaWrite, error::ReadError};

    #[derive(Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
    #[wincode(tag_encoding = "u8")]
    enum GoldenEnum {
        Empty,
        Value(u64),
    }

    #[derive(Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
    struct GoldenRow {
        id: u64,
        delta: i64,
        state: GoldenEnum,
        bytes: Vec<u8>,
    }

    #[test]
    fn golden_bytes_freeze_leb128_tags_and_lengths() {
        let row = GoldenRow {
            id: 300,
            delta: -2,
            state: GoldenEnum::Value(128),
            bytes: vec![0xaa, 0xbb],
        };
        let bytes = encode(&row).unwrap();
        assert_eq!(
            bytes,
            [0xac, 0x02, 0x03, 0x01, 0x80, 0x01, 0x02, 0xaa, 0xbb]
        );
        assert_eq!(decode_exact::<GoldenRow>(&bytes).unwrap(), row);
    }

    #[test]
    fn decoder_rejects_non_minimal_and_trailing_bytes() {
        assert!(decode_exact::<u64>(&[0x80, 0x00]).is_err());
        assert!(decode_exact::<u64>(&[0x01, 0x00]).is_err());
    }

    #[test]
    fn allocation_guard_fails_before_a_large_vector_allocation() {
        // A Vec<u8> length of 64 MiB + 1, with no body.
        let encoded_length = [0x81, 0x80, 0x80, 0x20];
        assert!(matches!(
            decode_exact::<Vec<u8>>(&encoded_length),
            Err(ReadError::PreallocationSizeLimit { .. })
        ));
    }
}
