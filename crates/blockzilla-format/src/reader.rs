use anyhow::{Context, Result};
use std::io::Read;

pub struct PostcardFramedReader<R> {
    r: R,
    buf: Vec<u8>,
}

impl<R: Read> PostcardFramedReader<R> {
    pub fn new(r: R) -> Self {
        Self {
            r,
            buf: Vec::with_capacity(2 * 1024 * 1024),
        }
    }

    pub fn reserve(&mut self, n: usize) {
        self.buf.reserve(n);
    }

    #[inline]
    pub fn read<'a, T>(&'a mut self) -> Result<Option<T>>
    where
        T: serde::Deserialize<'a>,
    {
        let mut lenb = [0u8; 4];

        match self.r.read_exact(&mut lenb) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e).context("read frame len").map_err(Into::into),
        }

        let len = u32::from_le_bytes(lenb) as usize;

        self.buf.resize(len, 0);

        match self.r.read_exact(&mut self.buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e).context("read frame payload").map_err(Into::into),
        }

        let v = postcard::from_bytes::<T>(&self.buf).context("postcard decode")?;
        Ok(Some(v))
    }
}
