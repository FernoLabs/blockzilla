use anyhow::{Context, Result};
use std::io::{Read, Write};

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
