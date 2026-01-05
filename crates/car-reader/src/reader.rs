use crate::car_block_group::CarBlockGroup;
use crate::error::CarReadError;
use crate::error::CarReadResult;
use std::io;
use std::io::BufRead;
use std::io::Read;

const MAX_UVARINT_LEN_64: usize = 10;

pub struct CarBlockReader<R: Read> {
    reader: io::BufReader<R>,
    cid_buf: Vec<u8>,
}

impl<R: Read> CarBlockReader<R> {
    pub fn with_capacity(inner: R, io_buf_bytes: usize) -> Self {
        Self {
            reader: io::BufReader::with_capacity(io_buf_bytes, inner),
            cid_buf: Vec::with_capacity(64),
        }
    }

    pub fn skip_header(&mut self) -> CarReadResult<()> {
        let header_len = read_uvarint64(&mut self.reader)? as usize;
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

        loop {
            let entry_len = match read_uvarint64(&mut self.reader) {
                Ok(v) => v as usize,
                Err(CarReadError::UnexpectedEof(_)) => {
                    return if out.is_empty() {
                        Ok(false)
                    } else {
                        Err(CarReadError::UnexpectedEof("EOF mid group".to_string()))
                    };
                }
                Err(e) => return Err(e),
            };

            if entry_len == 0 {
                continue;
            }

            self.cid_buf.clear();
            read_cid_bytes(&mut self.reader, &mut self.cid_buf)?;

            let done = out.read_entry_payload_into(&mut self.reader, &self.cid_buf, entry_len)?;
            if done {
                return Ok(true);
            }
        }
    }
}

/// Reads a uvarint64 without recording bytes.
fn read_uvarint64<R: BufRead>(r: &mut R) -> CarReadResult<u64> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
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
                x |= (byte as u64) << shift;
                r.consume(consumed);
                return Ok(x);
            }

            x |= ((byte & 0x7f) as u64) << shift;
            shift += 7;

            if shift > 63 {
                return Err(CarReadError::VarintOverflow("uvarint too long".to_string()));
            }

            if i >= MAX_UVARINT_LEN_64 {
                break;
            }
        }

        r.consume(consumed);
    }
}

fn read_cid_bytes<R: BufRead>(r: &mut R, out: &mut Vec<u8>) -> CarReadResult<()> {
    // First varint: either version=1 (CIDv1) or multihash code (CIDv0).
    let first = read_uvarint64_append(r, out)?;

    if first == 1 {
        // CIDv1: version(1) + codec + multihash(code + size + digest)
        let _codec = read_uvarint64_append(r, out)?;

        let _mh_code = read_uvarint64_append(r, out)?;
        let mh_size = read_uvarint64_append(r, out)? as usize;

        read_exact_append(r, out, mh_size)?;
    } else {
        // CIDv0: multihash(code + size + digest), where `first` was mh_code
        let mh_size = read_uvarint64_append(r, out)? as usize;
        read_exact_append(r, out, mh_size)?;
    }

    Ok(())
}
#[inline]
fn read_exact_append<R: Read>(r: &mut R, out: &mut Vec<u8>, n: usize) -> CarReadResult<()> {
    let start = out.len();
    out.resize(start + n, 0);
    r.read_exact(&mut out[start..])
        .map_err(|e| CarReadError::Io(e.to_string()))?;
    Ok(())
}
fn read_uvarint64_append<R: BufRead>(r: &mut R, out: &mut Vec<u8>) -> CarReadResult<u64> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
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
            out.push(byte);

            if byte < 0x80 {
                if i == MAX_UVARINT_LEN_64 && byte > 1 {
                    return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
                }
                x |= (byte as u64) << shift;
                r.consume(consumed);
                return Ok(x);
            }

            x |= ((byte & 0x7f) as u64) << shift;
            shift += 7;

            if shift > 63 {
                return Err(CarReadError::VarintOverflow("uvarint too long".to_string()));
            }

            if i >= MAX_UVARINT_LEN_64 {
                break;
            }
        }

        r.consume(consumed);
    }
}
