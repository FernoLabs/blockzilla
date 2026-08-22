use anyhow::{Context, Result};
use std::io::{Read, Write};

/// Canonical upper bound for one generic Wincode/LEB128 framed record.
pub const WINCODE_LEB128_MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

pub type WincodeLeb128Config = wincode::config::Configuration<
    true,
    { wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED },
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    crate::Leb128,
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
