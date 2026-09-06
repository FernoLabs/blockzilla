use anyhow::{Context, Result};
use std::io::{Read, Write};
use wincode::{
    ReadResult, WriteResult,
    error::invalid_value,
    int_encoding::{ByteOrder, IntEncoding},
    io::{Reader, Writer},
};

/// Canonical upper bound for one generic Wincode/LEB128 framed record.
pub const WINCODE_LEB128_MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

pub type WincodeLeb128Config = wincode::config::Configuration<
    true,
    { wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED },
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    crate::Leb128,
>;

/// Historical source traversal accepts non-minimal integer spellings so exact
/// source fields can be retained. Canonical validation must use
/// [`WincodeLeb128Config`]. Both configurations write identical minimal bytes.
pub type HistoricalSourceWincodeLeb128Config = wincode::config::Configuration<
    true,
    { wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED },
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    HistoricalSourceLeb128,
>;

/// Historical source traversal with an unchanged per-sequence allocation cap.
pub type BoundedHistoricalSourceWincodeLeb128Config<const LIMIT: usize> =
    wincode::config::Configuration<
        true,
        LIMIT,
        wincode::len::BincodeLen,
        wincode::int_encoding::LittleEndian,
        HistoricalSourceLeb128,
    >;

/// Archive V2 reader configuration with the same wire grammar and a finite
/// per-sequence allocation limit. The limit changes admission only; it does
/// not change encoded bytes.
pub type BoundedWincodeLeb128Config<const LIMIT: usize> = wincode::config::Configuration<
    true,
    LIMIT,
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    crate::Leb128,
>;

#[inline]
pub fn wincode_leb128_config() -> WincodeLeb128Config {
    wincode::config::Configuration::default()
        .disable_preallocation_size_limit()
        .with_int_encoding::<crate::Leb128>()
}

#[inline]
pub fn historical_source_wincode_leb128_config() -> HistoricalSourceWincodeLeb128Config {
    wincode::config::Configuration::default()
        .disable_preallocation_size_limit()
        .with_int_encoding::<HistoricalSourceLeb128>()
}

#[inline]
pub fn bounded_historical_source_wincode_leb128_config<const LIMIT: usize>()
-> BoundedHistoricalSourceWincodeLeb128Config<LIMIT> {
    wincode::config::Configuration::default()
        .with_preallocation_size_limit::<LIMIT>()
        .with_int_encoding::<HistoricalSourceLeb128>()
}

#[inline]
pub fn bounded_wincode_leb128_config<const LIMIT: usize>() -> BoundedWincodeLeb128Config<LIMIT> {
    wincode::config::Configuration::default()
        .with_preallocation_size_limit::<LIMIT>()
        .with_int_encoding::<crate::Leb128>()
}

#[inline]
pub fn write_u32_varint<W: Write>(w: &mut W, mut x: u32) -> Result<()> {
    while x >= 0x80 {
        w.write_all(&[((x as u8) | 0x80)]).context("write varint")?;
        x >>= 7;
    }
    w.write_all(&[x as u8]).context("write varint")?;
    Ok(())
}

#[inline]
pub fn read_u32_varint<R: Read>(r: &mut R) -> Result<Option<u32>> {
    let mut x = 0u32;
    let mut shift = 0u32;
    let mut byte_count = 0usize;

    loop {
        let mut b = [0u8; 1];
        if r.read(&mut b)? == 0 {
            anyhow::ensure!(byte_count == 0, "truncated varint");
            return Ok(None);
        }
        let byte = b[0];
        if byte_count == 4 {
            anyhow::ensure!(byte & 0xf0 == 0, "varint overflow");
        }
        x |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            anyhow::ensure!(byte_count == 0 || byte & 0x7f != 0, "non-minimal varint");
            return Ok(Some(x));
        }
        byte_count += 1;
        shift += 7;
        anyhow::ensure!(shift <= 28, "varint overflow");
    }
}

pub struct WincodeLeb128FramedWriter<W> {
    writer: W,
}

impl<W: Write> WincodeLeb128FramedWriter<W> {
    #[inline]
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    #[inline]
    pub fn write<T>(&mut self, record: &T) -> Result<()>
    where
        T: wincode::SchemaWrite<WincodeLeb128Config, Src = T> + ?Sized,
    {
        let bytes = wincode::config::serialize(record, wincode_leb128_config())?;
        self.write_bytes(&bytes)
    }

    #[inline]
    pub fn write_with_scratch<T>(&mut self, record: &T, scratch: &mut Vec<u8>) -> Result<usize>
    where
        T: wincode::SchemaWrite<WincodeLeb128Config, Src = T> + ?Sized,
    {
        encode_with_scratch(record, scratch)?;
        let len = scratch.len();
        self.write_bytes(scratch)?;
        Ok(len)
    }

    #[inline]
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let len = u32::try_from(bytes.len()).context("archive v2 frame exceeds u32::MAX")?;
        write_u32_varint(&mut self.writer, len)?;
        self.writer.write_all(bytes)?;
        Ok(())
    }

    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("flush wincode framed writer")
    }

    #[inline]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

#[inline]
pub fn encode_with_scratch<T>(record: &T, scratch: &mut Vec<u8>) -> Result<()>
where
    T: wincode::SchemaWrite<WincodeLeb128Config, Src = T> + ?Sized,
{
    scratch.clear();
    wincode::config::serialize_into(&mut *scratch, record, wincode_leb128_config())?;
    Ok(())
}

pub struct WincodeLeb128FramedReader<R> {
    reader: R,
    buf: Vec<u8>,
    max_frame_bytes: usize,
}

impl<R: Read> WincodeLeb128FramedReader<R> {
    #[inline]
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::with_capacity(2 << 20),
            max_frame_bytes: WINCODE_LEB128_MAX_FRAME_BYTES,
        }
    }

    /// Set a smaller caller-specific frame limit.
    #[inline]
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes.min(WINCODE_LEB128_MAX_FRAME_BYTES);
        self
    }

    #[inline]
    pub fn reserve(&mut self, n: usize) {
        self.buf.reserve(n);
    }

    #[inline]
    pub fn read<T>(&mut self) -> Result<Option<(usize, T)>>
    where
        for<'de> T: wincode::SchemaRead<
                'de,
                BoundedWincodeLeb128Config<WINCODE_LEB128_MAX_FRAME_BYTES>,
                Dst = T,
            >,
    {
        let Some(len) = read_u32_varint(&mut self.reader)? else {
            return Ok(None);
        };
        let len = len as usize;
        anyhow::ensure!(
            len <= self.max_frame_bytes,
            "wincode frame has {len} bytes, above the {} byte limit",
            self.max_frame_bytes
        );
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf)?;
        let record = wincode::config::deserialize_exact(
            &self.buf,
            bounded_wincode_leb128_config::<WINCODE_LEB128_MAX_FRAME_BYTES>(),
        )?;
        Ok(Some((len, record)))
    }

    #[inline]
    pub fn read_bytes(&mut self) -> Result<Option<(usize, Vec<u8>)>> {
        self.read_bytes_with_limit(self.max_frame_bytes, |bytes| Ok(bytes.to_vec()))
    }

    #[inline]
    pub fn read_bytes_with<T>(
        &mut self,
        f: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<Option<(usize, T)>> {
        self.read_bytes_with_limit(self.max_frame_bytes, f)
    }

    /// Read and decode a frame only when its declared length is within the
    /// caller's memory budget. The check happens before resizing the scratch
    /// buffer, so a corrupt length prefix cannot trigger a huge allocation.
    #[inline]
    pub fn read_bytes_with_limit<T>(
        &mut self,
        max_len: usize,
        f: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<Option<(usize, T)>> {
        let Some(len) = read_u32_varint(&mut self.reader)? else {
            return Ok(None);
        };
        let len = len as usize;
        let limit = max_len.min(self.max_frame_bytes);
        anyhow::ensure!(
            len <= limit,
            "wincode frame length {len} exceeds configured limit {limit}"
        );
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf)?;
        let value = f(&self.buf)?;
        Ok(Some((len, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_source_integers_accept_padding_but_write_canonical_bytes() {
        for (bytes, canonical, value) in [
            (&[0x80, 0][..], &[0][..], 0_u64),
            (&[0x81, 0][..], &[1][..], 1),
            (&[0x80, 0x81, 0][..], &[0x80, 1][..], 128),
        ] {
            assert!(
                wincode::config::deserialize_exact::<u64, _>(bytes, wincode_leb128_config())
                    .is_err()
            );
            assert_eq!(
                wincode::config::deserialize_exact::<u64, _>(
                    bytes,
                    historical_source_wincode_leb128_config()
                )
                .unwrap(),
                value,
            );
            assert_eq!(
                wincode::config::serialize(&value, historical_source_wincode_leb128_config())
                    .unwrap(),
                canonical
            );
        }
        assert_eq!(
            wincode::config::deserialize_exact::<i64, _>(
                &[0x81, 0x82, 0],
                historical_source_wincode_leb128_config()
            )
            .unwrap(),
            -129,
        );
    }

    #[test]
    fn both_leb128_modes_reject_overflow_and_truncation() {
        for bits in [16_u32, 32, 64, 128] {
            let max_bytes = bits.div_ceil(7) as usize;
            let mut overflow = vec![0xff; max_bytes];
            overflow[max_bytes - 1] = 1 << (bits % 7);
            for bytes in [
                &[0x80][..],
                overflow.as_slice(),
                vec![0x80; max_bytes].as_slice(),
            ] {
                assert!(decode_unsigned_leb128::<true>(bytes, bits).is_err());
                assert!(decode_unsigned_leb128::<false>(bytes, bits).is_err());
            }
        }
    }

    #[test]
    fn historical_source_sequence_lengths_keep_allocation_limits() {
        let config = wincode::config::Configuration::default()
            .with_preallocation_size_limit::<4>()
            .with_int_encoding::<HistoricalSourceLeb128>();
        assert_eq!(
            wincode::config::deserialize_exact::<Vec<u8>, _>(&[0x81, 0, 0x42], config).unwrap(),
            [0x42],
        );
        let error =
            wincode::config::deserialize_exact::<Vec<u8>, _>(&[0x85, 0], config).unwrap_err();
        assert!(matches!(
            error,
            wincode::error::ReadError::PreallocationSizeLimit { .. }
        ));
    }

    #[test]
    fn leb128_integer_decoders_reject_non_minimal_encodings() {
        macro_rules! check_types {
            ($($ty:ty),+ $(,)?) => {
                $(
                    for bytes in [
                        &[0x80, 0x00][..],
                        &[0x81, 0x00][..],
                        &[0xff, 0x00][..],
                        &[0x80, 0x81, 0x00][..],
                    ] {
                        let error = wincode::config::deserialize_exact::<$ty, _>(
                            bytes,
                            wincode_leb128_config(),
                        )
                        .unwrap_err();
                        assert!(error.to_string().contains("non-minimal LEB128 integer"));
                    }
                )+
            };
        }
        check_types!(u16, u32, u64, u128, i16, i32, i64, i128);
    }

    #[test]
    fn leb128_integer_decoders_preserve_canonical_boundaries() {
        macro_rules! check_types {
            ($($ty:ty),+ $(,)?) => {
                $(
                    for value in [<$ty>::MIN, 0, 1, 127, 128, 16_383, 16_384, <$ty>::MAX] {
                        let bytes = wincode::config::serialize(&value, wincode_leb128_config())
                            .unwrap();
                        let decoded = wincode::config::deserialize_exact::<$ty, _>(
                            &bytes,
                            bounded_wincode_leb128_config::<1024>(),
                        )
                        .unwrap();
                        assert_eq!(decoded, value);
                    }
                )+
            };
        }
        check_types!(u16, u32, u64, u128, i16, i32, i64, i128);
        assert_eq!(
            wincode::config::deserialize_exact::<u64, _>(&[0x80, 0x01], wincode_leb128_config(),)
                .unwrap(),
            128,
        );
        assert_eq!(
            wincode::config::deserialize_exact::<i64, _>(&[0x81, 0x02], wincode_leb128_config(),)
                .unwrap(),
            -129,
        );
    }

    #[test]
    fn bounded_leb128_decoder_rejects_non_minimal_sequence_lengths() {
        for bytes in [&[0x80, 0x00][..], &[0x81, 0x00, 0x42][..]] {
            let error = wincode::config::deserialize_exact::<Vec<u8>, _>(
                bytes,
                bounded_wincode_leb128_config::<1024>(),
            )
            .unwrap_err();
            assert!(error.to_string().contains("non-minimal LEB128 integer"));
        }
        assert_eq!(
            wincode::config::deserialize_exact::<Vec<u8>, _>(
                &[0x01, 0x42],
                bounded_wincode_leb128_config::<1024>(),
            )
            .unwrap(),
            [0x42],
        );
    }

    #[test]
    fn u32_varint_reader_rejects_non_minimal_overflow_and_truncation() {
        assert!(read_u32_varint(&mut [0x80, 0x00].as_slice()).is_err());
        assert!(read_u32_varint(&mut [0xff, 0xff, 0xff, 0xff, 0x10].as_slice()).is_err());
        assert!(read_u32_varint(&mut [0x80].as_slice()).is_err());
        assert_eq!(
            read_u32_varint(&mut [0xff, 0xff, 0xff, 0xff, 0x0f].as_slice()).unwrap(),
            Some(u32::MAX)
        );
    }

    #[test]
    fn typed_frame_reader_rejects_trailing_record_bytes() {
        let mut bytes = Vec::new();
        write_u32_varint(&mut bytes, 2).unwrap();
        bytes.extend_from_slice(&[1, 0]);

        assert!(
            WincodeLeb128FramedReader::new(bytes.as_slice())
                .read::<u32>()
                .is_err()
        );
    }

    #[test]
    fn limited_frame_rejects_declared_length_before_allocating_or_reading_payload() {
        let mut prefix = Vec::new();
        write_u32_varint(&mut prefix, 256).unwrap();
        let mut reader = WincodeLeb128FramedReader::new(prefix.as_slice());
        let error = reader.read_bytes_with_limit(32, |_| Ok(())).unwrap_err();
        assert!(error.to_string().contains("exceeds configured limit"));
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Leb128;

unsafe impl<B: ByteOrder> IntEncoding<B> for Leb128 {
    const STATIC: bool = false;
    const ZERO_COPY: bool = false;

    #[inline]
    fn encode_u16(val: u16, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(val as u128, writer)
    }

    #[inline]
    fn size_of_u16(val: u16) -> usize {
        unsigned_leb128_size(val as u128)
    }

    #[inline]
    fn decode_u16<'de>(reader: impl Reader<'de>) -> ReadResult<u16> {
        Ok(decode_unsigned_leb128::<true>(reader, u16::BITS)? as u16)
    }

    #[inline]
    fn encode_u32(val: u32, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(val as u128, writer)
    }

    #[inline]
    fn size_of_u32(val: u32) -> usize {
        unsigned_leb128_size(val as u128)
    }

    #[inline]
    fn decode_u32<'de>(reader: impl Reader<'de>) -> ReadResult<u32> {
        Ok(decode_unsigned_leb128::<true>(reader, u32::BITS)? as u32)
    }

    #[inline]
    fn encode_u64(val: u64, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(val as u128, writer)
    }

    #[inline]
    fn size_of_u64(val: u64) -> usize {
        unsigned_leb128_size(val as u128)
    }

    #[inline]
    fn decode_u64<'de>(reader: impl Reader<'de>) -> ReadResult<u64> {
        Ok(decode_unsigned_leb128::<true>(reader, u64::BITS)? as u64)
    }

    #[inline]
    fn encode_u128(val: u128, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(val, writer)
    }

    #[inline]
    fn size_of_u128(val: u128) -> usize {
        unsigned_leb128_size(val)
    }

    #[inline]
    fn decode_u128<'de>(reader: impl Reader<'de>) -> ReadResult<u128> {
        decode_unsigned_leb128::<true>(reader, u128::BITS)
    }

    #[inline]
    fn encode_i16(val: i16, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(zigzag_i16(val) as u128, writer)
    }

    #[inline]
    fn size_of_i16(val: i16) -> usize {
        unsigned_leb128_size(zigzag_i16(val) as u128)
    }

    #[inline]
    fn decode_i16<'de>(reader: impl Reader<'de>) -> ReadResult<i16> {
        Ok(unzigzag_i16(
            decode_unsigned_leb128::<true>(reader, u16::BITS)? as u16,
        ))
    }

    #[inline]
    fn encode_i32(val: i32, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(zigzag_i32(val) as u128, writer)
    }

    #[inline]
    fn size_of_i32(val: i32) -> usize {
        unsigned_leb128_size(zigzag_i32(val) as u128)
    }

    #[inline]
    fn decode_i32<'de>(reader: impl Reader<'de>) -> ReadResult<i32> {
        Ok(unzigzag_i32(
            decode_unsigned_leb128::<true>(reader, u32::BITS)? as u32,
        ))
    }

    #[inline]
    fn encode_i64(val: i64, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(zigzag_i64(val) as u128, writer)
    }

    #[inline]
    fn size_of_i64(val: i64) -> usize {
        unsigned_leb128_size(zigzag_i64(val) as u128)
    }

    #[inline]
    fn decode_i64<'de>(reader: impl Reader<'de>) -> ReadResult<i64> {
        Ok(unzigzag_i64(
            decode_unsigned_leb128::<true>(reader, u64::BITS)? as u64,
        ))
    }

    #[inline]
    fn encode_i128(val: i128, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(zigzag_i128(val), writer)
    }

    #[inline]
    fn size_of_i128(val: i128) -> usize {
        unsigned_leb128_size(zigzag_i128(val))
    }

    #[inline]
    fn decode_i128<'de>(reader: impl Reader<'de>) -> ReadResult<i128> {
        Ok(unzigzag_i128(decode_unsigned_leb128::<true>(
            reader,
            u128::BITS,
        )?))
    }
}

/// Read historical source integers without normalizing their spelling.
/// Overflow and truncation checks are identical to [`Leb128`]; only minimality
/// differs. Writers still delegate to the canonical integer encoder.
#[derive(Debug, Clone, Copy)]
pub struct HistoricalSourceLeb128;

macro_rules! historical_source_integer {
    ($ty:ty, $encode:ident, $size:ident, $decode:ident, $convert:expr) => {
        #[inline]
        fn $encode(value: $ty, writer: impl Writer) -> WriteResult<()> {
            <Leb128 as IntEncoding<B>>::$encode(value, writer)
        }

        #[inline]
        fn $size(value: $ty) -> usize {
            <Leb128 as IntEncoding<B>>::$size(value)
        }

        #[inline]
        fn $decode<'de>(reader: impl Reader<'de>) -> ReadResult<$ty> {
            let value = decode_unsigned_leb128::<false>(reader, <$ty>::BITS)?;
            Ok(($convert)(value))
        }
    };
}

// SAFETY: sizes and writes use the canonical implementation. Reads use its
// shared bounded loop and the same unsigned or zigzag value conversion.
unsafe impl<B: ByteOrder> IntEncoding<B> for HistoricalSourceLeb128 {
    const STATIC: bool = false;
    const ZERO_COPY: bool = false;

    historical_source_integer!(u16, encode_u16, size_of_u16, decode_u16, |v| v as u16);
    historical_source_integer!(u32, encode_u32, size_of_u32, decode_u32, |v| v as u32);
    historical_source_integer!(u64, encode_u64, size_of_u64, decode_u64, |v| v as u64);
    historical_source_integer!(u128, encode_u128, size_of_u128, decode_u128, |v| v);
    historical_source_integer!(i16, encode_i16, size_of_i16, decode_i16, |v| unzigzag_i16(
        v as u16
    ));
    historical_source_integer!(i32, encode_i32, size_of_i32, decode_i32, |v| unzigzag_i32(
        v as u32
    ));
    historical_source_integer!(i64, encode_i64, size_of_i64, decode_i64, |v| unzigzag_i64(
        v as u64
    ));
    historical_source_integer!(i128, encode_i128, size_of_i128, decode_i128, unzigzag_i128);
}

#[inline]
fn unsigned_leb128_size(mut value: u128) -> usize {
    let mut size = 1usize;
    while value >= 0x80 {
        value >>= 7;
        size += 1;
    }
    size
}

#[inline]
fn encode_unsigned_leb128(mut value: u128, mut writer: impl Writer) -> WriteResult<()> {
    let mut bytes = [0u8; 19];
    let mut len = 0usize;

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

#[inline]
fn decode_unsigned_leb128<'de, const REQUIRE_MINIMAL: bool>(
    mut reader: impl Reader<'de>,
    max_bits: u32,
) -> ReadResult<u128> {
    let max = if max_bits == u128::BITS {
        u128::MAX
    } else {
        (1u128 << max_bits) - 1
    };
    let max_bytes = max_bits.div_ceil(7) as usize;
    let mut value = 0u128;

    for index in 0..max_bytes {
        let byte = reader.take_byte()?;
        let payload = u128::from(byte & 0x7f);
        let shift = (index * 7) as u32;
        if payload > (u128::MAX >> shift) {
            return Err(invalid_value("LEB128 integer overflow"));
        }
        value |= payload << shift;

        if byte & 0x80 == 0 {
            if value > max {
                return Err(invalid_value("LEB128 integer overflow"));
            }
            if REQUIRE_MINIMAL && index != 0 && payload == 0 {
                return Err(invalid_value("non-minimal LEB128 integer"));
            }
            return Ok(value);
        }
    }

    Err(invalid_value("LEB128 integer overflow"))
}

macro_rules! zigzag_pair {
    ($encode:ident, $decode:ident, $signed:ty, $unsigned:ty) => {
        #[inline]
        fn $encode(value: $signed) -> $unsigned {
            let unsigned = value as $unsigned;
            unsigned.wrapping_shl(1) ^ ((value >> (<$signed>::BITS - 1)) as $unsigned)
        }

        #[inline]
        fn $decode(value: $unsigned) -> $signed {
            ((value >> 1) as $signed) ^ (-((value & 1) as $signed))
        }
    };
}

zigzag_pair!(zigzag_i16, unzigzag_i16, i16, u16);
zigzag_pair!(zigzag_i32, unzigzag_i32, i32, u32);
zigzag_pair!(zigzag_i64, unzigzag_i64, i64, u64);
zigzag_pair!(zigzag_i128, unzigzag_i128, i128, u128);
