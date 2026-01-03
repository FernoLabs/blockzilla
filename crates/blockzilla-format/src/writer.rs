use anyhow::{Context, Result};
use std::io::Write;

pub struct PostcardFramedWriter<W> {
    w: W,
}

impl<W: Write> PostcardFramedWriter<W> {
    pub fn new(w: W) -> Self {
        Self { w }
    }

    #[inline]
    pub fn write<T: serde::Serialize>(&mut self, v: &T) -> Result<()> {
        let len = postcard::experimental::serialized_size(v)? as u32;
        self.w.write_all(&len.to_le_bytes())?;
        postcard::to_io(v, &mut self.w)?;
        Ok(())
    }

    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        self.w.flush().context("flush")
    }

    #[inline]
    pub fn into_inner(self) -> W {
        self.w
    }
}

impl<W: std::io::Write> PostcardFramedWriter<W> {
    #[inline]
    pub fn write_bytes(&mut self, payload: &[u8]) -> anyhow::Result<()> {
        let len = payload.len() as u32;
        self.w.write_all(&len.to_le_bytes())?;
        self.w.write_all(payload)?;
        Ok(())
    }
}