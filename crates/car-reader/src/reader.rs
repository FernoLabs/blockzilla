use bytes::BytesMut;
use std::io::{self, BufRead, Read};

use crate::{
    car_block_group::CarBlockGroup,
    cid::cid_bytes_len,
    error::{CarReadError, CarReadResult},
};

const MAX_UVARINT_LEN_64: usize = 10;

pub struct CarBlockReader<R: Read> {
    reader: std::io::BufReader<R>,
    buf: BytesMut,
    entries: Vec<(usize, usize, usize)>, // (entry_start, entry_end, cid_len)
}

impl<R: Read> CarBlockReader<R> {
    pub fn with_capacity(inner: R, io_buf_bytes: usize) -> Self {
        Self {
            reader: std::io::BufReader::with_capacity(io_buf_bytes, inner),
            buf: BytesMut::with_capacity(8 << 20),
            entries: Vec::with_capacity(8192),
        }
    }

    pub fn skip_header(&mut self) -> CarReadResult<()> {
        let header_len = read_uvarint_bufread(&mut self.reader)? as usize;
        let mut tmp = vec![0u8; header_len];
        self.reader
            .read_exact(&mut tmp)
            .map_err(|e| CarReadError::Io(e.to_string()))?;
        Ok(())
    }

    /// Reads CAR sections until it finds a "block" node (kind == 2) in the entry payload.
    /// Fills `out` (reusing its internal allocations) and returns:
    /// - Ok(true)  => group produced
    /// - Ok(false) => clean EOF (no more groups)
    pub fn read_until_block_into(&mut self, out: &mut CarBlockGroup) -> CarReadResult<bool> {
        out.clear();
        self.buf.clear();
        self.entries.clear();

        loop {
            let section_size = match read_uvarint_bufread(&mut self.reader) {
                Ok(v) => v as usize,
                Err(CarReadError::UnexpectedEof(_)) => {
                    if self.entries.is_empty() {
                        return Ok(false);
                    }
                    return Err(CarReadError::UnexpectedEof("EOF mid group".to_string()));
                }
                Err(e) => return Err(e),
            };

            if section_size == 0 {
                continue;
            }

            let (entry_start, entry_end) =
                match read_n_into_tail(&mut self.reader, &mut self.buf, section_size) {
                    Ok(v) => v,
                    Err(e) => {
                        // rollback the resize (critical)
                        let start = self.buf.len().saturating_sub(section_size);
                        self.buf.truncate(start);

                        if e.kind() == io::ErrorKind::UnexpectedEof {
                            return Ok(false);
                        }
                        return Err(CarReadError::Io(e.to_string()));
                    }
                };

            let entry = &self.buf[entry_start..entry_end];
            let cid_len = cid_bytes_len(entry)?;
            self.entries.push((entry_start, entry_end, cid_len));

            let payload = &entry[cid_len..];
            if is_block_node(payload) {
                // Freeze bytes up to entry_end, leaving remainder in self.buf for the next group.
                let frozen = self.buf.split_to(entry_end).freeze();

                let n = self.entries.len();
                if out.payloads.capacity() < n {
                    out.payloads.reserve(n - out.payloads.capacity());
                }
                let cap = out.cid_map.capacity();
                if cap < n {
                    out.cid_map.reserve(n - cap);
                }

                // Build group outputs
                // The last entry in entries is the block entry we just read.
                let (block_s, block_e, block_cid_len) =
                    *self.entries.last().expect("entries non-empty");

                for (idx, (s, e, cid_len_i)) in self.entries.drain(..).enumerate() {
                    let cid_key = frozen.slice(s..s + cid_len_i);
                    let payload = frozen.slice(s + cid_len_i..e);
                    out.payloads.push(payload);
                    out.cid_map.insert(cid_key, idx);
                }

                out.block_payload = frozen.slice(block_s + block_cid_len..block_e);
                return Ok(true);
            }
        }
    }
}

#[inline]
fn read_n_into_tail<R: Read>(
    reader: &mut R,
    buf: &mut BytesMut,
    n: usize,
) -> io::Result<(usize, usize)> {
    let start = buf.len();
    buf.resize(start + n, 0);
    reader.read_exact(&mut buf[start..start + n])?;
    Ok((start, start + n))
}

/// Fast uvarint reader using BufRead's internal buffer (no per-byte syscalls).
pub fn read_uvarint_bufread<R: BufRead>(r: &mut R) -> CarReadResult<u64> {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    let mut i: usize = 0;

    loop {
        if i >= MAX_UVARINT_LEN_64 {
            return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
        }

        let buf = r.fill_buf().map_err(|e| CarReadError::Io(e.to_string()))?;

        if buf.is_empty() {
            return Err(CarReadError::UnexpectedEof(
                "EOF while reading uvarint".to_string(),
            ));
        }

        let mut consumed = 0usize;

        for &byte in buf {
            consumed += 1;
            i += 1;

            if byte < 0x80 {
                if i == MAX_UVARINT_LEN_64 && byte > 1 {
                    return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
                }
                x |= (byte as u64) << s;
                r.consume(consumed);
                return Ok(x);
            }

            x |= ((byte & 0x7f) as u64) << s;
            s += 7;

            if s > 63 {
                return Err(CarReadError::VarintOverflow("uvarint too long".to_string()));
            }

            if i >= MAX_UVARINT_LEN_64 {
                break;
            }
        }

        r.consume(consumed);
    }
}

/// Returns true if `payload` looks like a CBOR array whose 1st element (kind) is the small uint `2`.
#[inline]
fn is_block_node(payload: &[u8]) -> bool {
    if payload.len() < 2 {
        return false;
    }
    let b0 = payload[0];
    if (b0 & 0b1110_0000) != 0b1000_0000 {
        return false;
    }
    payload[1] == 0x02
}
