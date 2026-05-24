use crate::car_block_group::CarBlockGroup;
use crate::error::CarReadError;
use crate::error::CarReadResult;
use crate::node::peek_node_type;
use crate::reconstruct::{Cid36, NodeLocation};
use std::io;
use std::io::BufRead;
use std::io::Read;

const MAX_UVARINT_LEN_64: usize = 10;
const CAR_CID_LEN: usize = 36;
const NODE_KIND_PREFIX_BYTES: usize = 16;

pub struct CarBlockReader<R: Read> {
    pub reader: io::BufReader<R>,
    pub offset: u64,
    pub entry_index: u64,
}

pub struct CarEntryPayload<'a> {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub payload: &'a [u8],
    pub payload_len: usize,
    pub entry_len: usize,
    pub varint_len: usize,
    pub total_len: usize,
}

pub struct CarEntryMaybePayload<'a> {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub prefix: &'a [u8],
    pub payload: Option<&'a [u8]>,
    pub payload_len: usize,
    pub entry_len: usize,
    pub varint_len: usize,
    pub total_len: usize,
}

pub enum CarPayloadRead {
    Skip,
    Prefix(usize),
    Full,
}

impl<R: Read> CarBlockReader<R> {
    pub fn with_capacity(inner: R, io_buf_bytes: usize) -> Self {
        Self {
            reader: io::BufReader::with_capacity(io_buf_bytes, inner),
            offset: 0,
            entry_index: 0,
        }
    }

    pub fn read_header_bytes(&mut self) -> CarReadResult<Vec<u8>> {
        let (header_len, header_varint) = read_uvarint64_with_bytes(&mut self.reader)?;
        let header_len = header_len as usize;
        let mut out = header_varint;
        let start = out.len();
        out.resize(start + header_len, 0u8);
        self.reader
            .read_exact(&mut out[start..])
            .map_err(|e| CarReadError::Io(e.to_string()))?;
        self.offset += out.len() as u64;
        Ok(out)
    }

    pub fn skip_header(&mut self) -> CarReadResult<()> {
        let _ = self.read_header_bytes()?;
        Ok(())
    }

    /// Safe group: follows block->entry->tx links.
    ///
    /// Reads CAR sections until it finds a "block" node (kind == 2) in the entry payload.
    /// Fills `out` (reusing its internal allocations) and returns:
    /// - Ok(true)  => group produced
    /// - Ok(false) => clean EOF (no more groups)
    pub fn read_until_block_into(&mut self, out: &mut CarBlockGroup) -> CarReadResult<bool> {
        out.clear();

        if !out.reads_transaction_payloads() {
            return self.read_until_block_selecting_payloads_into(out);
        }

        loop {
            let entry_len = match read_uvarint64_with_len(&mut self.reader) {
                Ok((v, varint_len)) => {
                    self.offset += varint_len as u64;
                    v as usize
                }
                Err(CarReadError::Eof) => {
                    return Ok(false);
                }
                Err(e) => return Err(e),
            };

            let mut cid_buf = [0; 36];
            self.reader.read_exact(&mut cid_buf)?;
            self.offset += cid_buf.len() as u64;

            let payload_len = entry_len
                .checked_sub(cid_buf.len())
                .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;

            let cid = Cid36::from_car_bytes(cid_buf);
            let done = out.read_entry_payload_with_cid_into(
                Some(cid.car_bytes()),
                &mut self.reader,
                payload_len,
            )?;
            self.offset += payload_len as u64;
            self.entry_index += 1;
            if done {
                return Ok(true);
            }
        }
    }

    fn read_until_block_selecting_payloads_into(
        &mut self,
        out: &mut CarBlockGroup,
    ) -> CarReadResult<bool> {
        let mut scratch = Vec::new();
        let transaction_prefix_bytes = out.transaction_prefix_bytes();
        loop {
            let Some(entry) = self.read_entry_payload_select_with_scratch(
                &mut scratch,
                NODE_KIND_PREFIX_BYTES,
                |prefix| match peek_node_type(prefix) {
                    Ok(0) => transaction_prefix_bytes
                        .map(CarPayloadRead::Prefix)
                        .unwrap_or(CarPayloadRead::Skip),
                    Ok(_) | Err(_) => CarPayloadRead::Full,
                },
            )?
            else {
                return Ok(false);
            };

            let done = out.read_entry_maybe_payload_with_cid_into(
                Some(entry.cid.car_bytes()),
                entry.prefix,
                entry.payload,
                entry.payload_len,
            )?;
            if done {
                return Ok(true);
            }
        }
    }

    pub fn read_until_block_lossless(
        &mut self,
        out: &mut crate::reconstruct::LosslessCarBlock,
    ) -> CarReadResult<bool> {
        out.clear();

        loop {
            let entry_offset = self.offset;
            let current_entry_index = self.entry_index;
            let entry_len = match read_uvarint64_with_len(&mut self.reader) {
                Ok((v, varint_len)) => {
                    self.offset += varint_len as u64;
                    v as usize
                }
                Err(CarReadError::Eof) => return Ok(false),
                Err(err) => return Err(err),
            };

            let mut cid_buf = [0u8; 36];
            self.reader.read_exact(&mut cid_buf)?;
            self.offset += cid_buf.len() as u64;

            let payload_len = entry_len
                .checked_sub(cid_buf.len())
                .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;

            let done = out.read_entry_payload_into(
                &mut self.reader,
                payload_len,
                crate::reconstruct::NodeLocation {
                    entry_index: current_entry_index,
                    car_offset: entry_offset,
                },
                cid_buf,
            )?;
            self.offset += payload_len as u64;
            self.entry_index += 1;
            if done {
                return Ok(true);
            }
        }
    }

    /// Reads a single CAR entry payload into caller-owned scratch.
    ///
    /// This is the lowest-level reusable scanner API: it preserves CAR offset
    /// and entry index for index builders while keeping the payload allocation
    /// under caller control.
    pub fn read_entry_payload_with_scratch<'a>(
        &mut self,
        scratch: &'a mut Vec<u8>,
    ) -> CarReadResult<Option<CarEntryPayload<'a>>> {
        let entry_offset = self.offset;
        let current_entry_index = self.entry_index;

        let (entry_len, varint_len) = match read_uvarint64_with_len(&mut self.reader) {
            Ok((value, varint_len)) => {
                self.offset += varint_len as u64;
                (value as usize, varint_len)
            }
            Err(CarReadError::Eof) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut cid_buf = [0u8; 36];
        self.reader.read_exact(&mut cid_buf)?;
        self.offset += cid_buf.len() as u64;

        let payload_len = entry_len
            .checked_sub(CAR_CID_LEN)
            .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;
        let total_len = varint_len
            .checked_add(entry_len)
            .ok_or_else(|| CarReadError::InvalidData("entry length overflow".to_string()))?;

        scratch.clear();
        scratch.resize(payload_len, 0u8);
        self.reader.read_exact(scratch)?;
        self.offset += payload_len as u64;
        self.entry_index += 1;

        Ok(Some(CarEntryPayload {
            location: NodeLocation {
                entry_index: current_entry_index,
                car_offset: entry_offset,
            },
            cid: Cid36::from_car_bytes(cid_buf),
            payload: scratch,
            payload_len,
            entry_len,
            varint_len,
            total_len,
        }))
    }

    /// Reads only a payload prefix first, then either reads the rest or skips it.
    ///
    /// This keeps transaction-only scanners fast: they can peek the CBOR node
    /// kind from a tiny prefix and avoid copying large block/entry/reward
    /// payloads they do not need.
    pub fn read_entry_payload_if_prefix_with_scratch<'a, F>(
        &mut self,
        scratch: &'a mut Vec<u8>,
        prefix_len: usize,
        should_read_payload: F,
    ) -> CarReadResult<Option<CarEntryMaybePayload<'a>>>
    where
        F: FnOnce(&[u8]) -> bool,
    {
        let entry_offset = self.offset;
        let current_entry_index = self.entry_index;

        let (entry_len, varint_len) = match read_uvarint64_with_len(&mut self.reader) {
            Ok((value, varint_len)) => {
                self.offset += varint_len as u64;
                (value as usize, varint_len)
            }
            Err(CarReadError::Eof) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut cid_buf = [0u8; CAR_CID_LEN];
        self.reader.read_exact(&mut cid_buf)?;
        self.offset += cid_buf.len() as u64;

        let payload_len = entry_len
            .checked_sub(CAR_CID_LEN)
            .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;
        let total_len = varint_len
            .checked_add(entry_len)
            .ok_or_else(|| CarReadError::InvalidData("entry length overflow".to_string()))?;
        let prefix_len = prefix_len.min(payload_len);

        scratch.clear();
        scratch.resize(prefix_len, 0u8);
        self.reader.read_exact(scratch)?;
        self.offset += prefix_len as u64;

        let read_payload = should_read_payload(scratch);
        if read_payload {
            scratch.resize(payload_len, 0u8);
            self.reader.read_exact(&mut scratch[prefix_len..])?;
            self.offset += (payload_len - prefix_len) as u64;
        } else {
            self.skip_payload_bytes(payload_len - prefix_len)?;
        }
        self.entry_index += 1;

        Ok(Some(CarEntryMaybePayload {
            location: NodeLocation {
                entry_index: current_entry_index,
                car_offset: entry_offset,
            },
            cid: Cid36::from_car_bytes(cid_buf),
            prefix: &scratch[..prefix_len],
            payload: read_payload.then_some(&scratch[..]),
            payload_len,
            entry_len,
            varint_len,
            total_len,
        }))
    }

    /// Reads a small initial payload prefix, then lets the caller decide whether
    /// to skip, extend to a larger prefix, or materialize the full payload.
    pub fn read_entry_payload_select_with_scratch<'a, F>(
        &mut self,
        scratch: &'a mut Vec<u8>,
        initial_prefix_len: usize,
        select_payload: F,
    ) -> CarReadResult<Option<CarEntryMaybePayload<'a>>>
    where
        F: FnOnce(&[u8]) -> CarPayloadRead,
    {
        let entry_offset = self.offset;
        let current_entry_index = self.entry_index;

        let (entry_len, varint_len) = match read_uvarint64_with_len(&mut self.reader) {
            Ok((value, varint_len)) => {
                self.offset += varint_len as u64;
                (value as usize, varint_len)
            }
            Err(CarReadError::Eof) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut cid_buf = [0u8; CAR_CID_LEN];
        self.reader.read_exact(&mut cid_buf)?;
        self.offset += cid_buf.len() as u64;

        let payload_len = entry_len
            .checked_sub(CAR_CID_LEN)
            .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;
        let total_len = varint_len
            .checked_add(entry_len)
            .ok_or_else(|| CarReadError::InvalidData("entry length overflow".to_string()))?;
        let initial_prefix_len = initial_prefix_len.min(payload_len);

        scratch.clear();
        scratch.resize(initial_prefix_len, 0u8);
        self.reader.read_exact(scratch)?;
        self.offset += initial_prefix_len as u64;

        let target_len = match select_payload(scratch) {
            CarPayloadRead::Skip => initial_prefix_len,
            CarPayloadRead::Prefix(len) => len.min(payload_len),
            CarPayloadRead::Full => payload_len,
        };

        if target_len > initial_prefix_len {
            scratch.resize(target_len, 0u8);
            self.reader.read_exact(&mut scratch[initial_prefix_len..])?;
            self.offset += (target_len - initial_prefix_len) as u64;
        }

        self.skip_payload_bytes(payload_len - target_len)?;
        self.entry_index += 1;

        Ok(Some(CarEntryMaybePayload {
            location: NodeLocation {
                entry_index: current_entry_index,
                car_offset: entry_offset,
            },
            cid: Cid36::from_car_bytes(cid_buf),
            prefix: &scratch[..target_len],
            payload: (target_len == payload_len).then_some(&scratch[..]),
            payload_len,
            entry_len,
            varint_len,
            total_len,
        }))
    }

    fn skip_payload_bytes(&mut self, mut len: usize) -> CarReadResult<()> {
        while len > 0 {
            let buf = self
                .reader
                .fill_buf()
                .map_err(|err| CarReadError::Io(err.to_string()))?;
            if buf.is_empty() {
                return Err(CarReadError::UnexpectedEof(
                    "EOF while skipping CAR payload".to_string(),
                ));
            }
            let consumed = len.min(buf.len());
            self.reader.consume(consumed);
            self.offset += consumed as u64;
            len -= consumed;
        }
        Ok(())
    }

    /// Reads a single CAR entry as a fully decoded raw node.
    ///
    /// The caller can reuse `scratch` across calls to avoid reallocating the
    /// payload buffer while scanning a whole archive.
    pub fn read_lossless_node_with_scratch(
        &mut self,
        scratch: &mut Vec<u8>,
    ) -> CarReadResult<Option<crate::reconstruct::RawNode>> {
        let Some(entry) = self.read_entry_payload_with_scratch(scratch)? else {
            return Ok(None);
        };

        let raw = crate::reconstruct::decode_raw_node(entry.location, entry.cid, entry.payload)
            .map_err(|err| {
                CarReadError::InvalidData(format!(
                    "entry {} at offset {}: {}",
                    entry.location.entry_index, entry.location.car_offset, err
                ))
            })?;

        Ok(Some(raw))
    }

    pub fn read_lossless_node(&mut self) -> CarReadResult<Option<crate::reconstruct::RawNode>> {
        let mut scratch = Vec::new();
        self.read_lossless_node_with_scratch(&mut scratch)
    }
}

/// Returns the payload slice from a complete raw CAR entry frame.
///
/// The input must include the entry length varint, 36-byte CID, and payload.
/// This is useful for offset indexes that fetch a single CAR frame directly.
pub fn entry_payload_slice(entry: &[u8]) -> CarReadResult<&[u8]> {
    let mut cursor = std::io::Cursor::new(entry);
    let (entry_len, varint_len) = match read_uvarint64_with_len(&mut cursor) {
        Ok(value) => value,
        Err(CarReadError::Eof) => {
            return Err(CarReadError::UnexpectedEof("empty CAR entry".to_string()));
        }
        Err(err) => return Err(err),
    };
    let entry_len = usize::try_from(entry_len)
        .map_err(|_| CarReadError::InvalidData("entry length exceeds usize".to_string()))?;
    let total_len = varint_len
        .checked_add(entry_len)
        .ok_or_else(|| CarReadError::InvalidData("entry size overflow".to_string()))?;
    if total_len != entry.len() {
        return Err(CarReadError::InvalidData(format!(
            "entry length mismatch: header says {total_len} bytes, fetched {}",
            entry.len()
        )));
    }
    if entry_len < CAR_CID_LEN {
        return Err(CarReadError::InvalidData(format!(
            "invalid entry len {entry_len}"
        )));
    }
    Ok(&entry[varint_len + CAR_CID_LEN..total_len])
}

/// Reads a uvarint64 without recording bytes.
pub fn read_uvarint64<R: BufRead>(r: &mut R) -> CarReadResult<u64> {
    read_uvarint64_with_len(r).map(|(value, _)| value)
}

/// Reads a uvarint64 and returns `(value, encoded_bytes)`.
pub fn read_uvarint64_with_bytes<R: BufRead>(r: &mut R) -> CarReadResult<(u64, Vec<u8>)> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
    let mut i: usize = 0;
    let mut bytes = Vec::with_capacity(MAX_UVARINT_LEN_64);

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

        let byte = buf[0];
        bytes.push(byte);
        r.consume(1);
        i += 1;

        if byte < 0x80 {
            if i == MAX_UVARINT_LEN_64 && byte > 1 {
                return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
            }
            x |= (byte as u64) << shift;
            return Ok((x, bytes));
        }

        x |= ((byte & 0x7f) as u64) << shift;
        shift += 7;

        if shift > 63 {
            return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
        }
    }
}

/// Reads a uvarint64 and returns `(value, consumed_len)`.
pub fn read_uvarint64_with_len<R: BufRead>(r: &mut R) -> CarReadResult<(u64, usize)> {
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
                return Ok((x, i));
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

#[cfg(test)]
mod tests {
    use super::{CarBlockReader, entry_payload_slice};

    #[test]
    fn entry_payload_slice_extracts_payload() {
        let mut entry = Vec::with_capacity(1 + 36 + 2);
        entry.push(38);
        entry.extend_from_slice(&[0u8; 36]);
        entry.extend_from_slice(&[1u8, 2u8]);

        assert_eq!(entry_payload_slice(&entry).unwrap(), [1u8, 2u8]);
    }

    #[test]
    fn prefix_reader_can_skip_payload_tail() {
        let mut car = Vec::new();
        car.push(0); // Empty CAR header for this framing-level test.
        car.push(40); // CID + 4 payload bytes.
        car.extend_from_slice(&[0u8; 36]);
        car.extend_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd]);

        let mut reader = CarBlockReader::with_capacity(&car[..], 16);
        reader.skip_header().unwrap();
        let mut scratch = Vec::new();
        let entry = reader
            .read_entry_payload_if_prefix_with_scratch(&mut scratch, 2, |prefix| {
                assert_eq!(prefix, [0xaa, 0xbb]);
                false
            })
            .unwrap()
            .unwrap();

        assert_eq!(entry.prefix, [0xaa, 0xbb]);
        assert!(entry.payload.is_none());
        assert_eq!(entry.payload_len, 4);
        assert_eq!(entry.total_len, 41);
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 1);
    }
}
