use crate::car_block_group::CarBlockGroup;
use crate::error::CarReadError;
use crate::error::CarReadResult;
use std::io;
use std::io::BufRead;
use std::io::Read;

const MAX_UVARINT_LEN_64: usize = 10;

pub struct CarBlockReader<R: Read> {
    reader: io::BufReader<R>,
}

impl<R: Read> CarBlockReader<R> {
    pub fn with_capacity(inner: R, io_buf_bytes: usize) -> Self {
        Self {
            reader: io::BufReader::with_capacity(io_buf_bytes, inner),
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
                Err(CarReadError::Eof) => {
                    return Ok(false);
                }
                Err(e) => return Err(e),
            };

            if entry_len == 0 {
                return Err(CarReadError::InvalidEntryLen("entry len 0".to_string()));
            }

            if entry_len <= 36 {
                return Err(CarReadError::InvalidEntryLen(format!(
                    "entry smaller than cid ({entry_len})"
                )));
            }

            let mut cid_buf = [0; 36];
            self.reader.read_exact(&mut cid_buf)?;
            if cid_buf[0] != 0x01 || cid_buf[1] != 0x71 || cid_buf[2] != 0x12 || cid_buf[3] != 0x20
            {
                return Err(CarReadError::Cid(format!("Not known cid {cid_buf:02x?}")));
            }

            let done = out.read_entry_payload_into(&mut self.reader, &cid_buf, entry_len)?;
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
            if x != 0 {
                return Err(CarReadError::UnexpectedEof(
                    "EOF while reading uvarint".to_string(),
                ));
            }
            return Err(CarReadError::Eof);
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
                r.consume(consumed);
                return Err(CarReadError::VarintOverflow("uvarint too long".to_string()));
            }

            if i >= MAX_UVARINT_LEN_64 {
                r.consume(consumed);
                return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
            }
        }

        r.consume(consumed);
    }
}
