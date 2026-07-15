use anyhow::{Context, Result};
use std::io::{Read, Write};

pub type WincodeLeb128Config = wincode::config::Configuration<
    true,
    { wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED },
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
    let mut shift = 0;

    loop {
        let mut b = [0u8; 1];
        if r.read(&mut b)? == 0 {
            return Ok(None);
        }
        let byte = b[0];
        x |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(Some(x));
        }
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
}

impl<R: Read> WincodeLeb128FramedReader<R> {
    #[inline]
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::with_capacity(2 << 20),
        }
    }

    #[inline]
    pub fn reserve(&mut self, n: usize) {
        self.buf.reserve(n);
    }

    #[inline]
    pub fn read<T>(&mut self) -> Result<Option<(usize, T)>>
    where
        for<'de> T: wincode::SchemaRead<'de, WincodeLeb128Config, Dst = T>,
    {
        let Some(len) = read_u32_varint(&mut self.reader)? else {
            return Ok(None);
        };
        let len = len as usize;
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf)?;
        let record = wincode::config::deserialize(&self.buf, wincode_leb128_config())?;
        Ok(Some((len, record)))
    }

    #[inline]
    pub fn read_bytes(&mut self) -> Result<Option<(usize, Vec<u8>)>> {
        let Some(len) = read_u32_varint(&mut self.reader)? else {
            return Ok(None);
        };
        let len = len as usize;
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf)?;
        Ok(Some((len, self.buf.clone())))
    }

    #[inline]
    pub fn read_bytes_with<T>(
        &mut self,
        f: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<Option<(usize, T)>> {
        self.read_bytes_with_limit(usize::MAX, f)
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
        anyhow::ensure!(
            len <= max_len,
            "wincode frame length {len} exceeds configured limit {max_len}"
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
    fn limited_frame_rejects_declared_length_before_allocating_or_reading_payload() {
        let mut prefix = Vec::new();
        write_u32_varint(&mut prefix, 256).unwrap();
        let mut reader = WincodeLeb128FramedReader::new(prefix.as_slice());
        let error = reader.read_bytes_with_limit(32, |_| Ok(())).unwrap_err();
        assert!(error.to_string().contains("exceeds configured limit"));
    }
}
