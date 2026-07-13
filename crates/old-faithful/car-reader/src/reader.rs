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

/// Low-level streaming reader for Old Faithful CAR archives.
///
/// `CarBlockReader` keeps track of the current CAR byte offset and entry index.
/// Use it directly when building indexes or scanners. For normal block-by-block
/// processing, [`crate::CarStream`] is the smaller wrapper around this type.
pub struct CarBlockReader<R: Read> {
    pub reader: io::BufReader<R>,
    /// Current byte offset in the CAR stream, including the header if it has
    /// already been read.
    pub offset: u64,
    /// Number of CAR entries read after the header.
    pub entry_index: u64,
}

/// Borrowed payload for one fully loaded CAR entry.
pub struct CarEntryPayload<'a> {
    /// Entry index and CAR byte offset.
    pub location: NodeLocation,
    /// CAR CID bytes for this entry.
    pub cid: Cid36,
    /// Entry payload bytes without the CID prefix.
    pub payload: &'a [u8],
    /// Payload length in bytes.
    pub payload_len: usize,
    /// CAR entry length from the entry varint, including CID and payload.
    pub entry_len: usize,
    /// Number of bytes used by the entry length varint.
    pub varint_len: usize,
    /// Total on-wire entry length, including varint, CID, and payload.
    pub total_len: usize,
}

/// Borrowed payload for an entry where the caller may have skipped the body.
pub struct CarEntryMaybePayload<'a> {
    pub location: NodeLocation,
    pub cid: Cid36,
    /// Prefix bytes that were always read before the selection callback ran.
    pub prefix: &'a [u8],
    /// Full payload when selected, or `None` when skipped.
    pub payload: Option<&'a [u8]>,
    pub payload_len: usize,
    pub entry_len: usize,
    pub varint_len: usize,
    pub total_len: usize,
}

/// Payload loading decision for [`CarBlockReader::read_entry_payload_select_with_scratch`].
pub enum CarPayloadRead {
    /// Skip the rest of the payload after reading the requested prefix.
    Skip,
    /// Read only this many bytes from the payload.
    Prefix(usize),
    /// Read the full payload.
    Full,
}

impl<R: Read> CarBlockReader<R> {
    /// Create a CAR reader with a specific internal I/O buffer size.
    pub fn with_capacity(inner: R, io_buf_bytes: usize) -> Self {
        Self {
            reader: io::BufReader::with_capacity(io_buf_bytes, inner),
            offset: 0,
            entry_index: 0,
        }
    }

    /// Read and return the raw CAR header bytes, including its length varint.
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

    /// Read and discard the CAR header.
    ///
    /// Call this once before reading entries or blocks from a normal CAR file.
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
        append_exact_from_bufread(&mut self.reader, scratch, payload_len)?;
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
        append_exact_from_bufread(&mut self.reader, scratch, prefix_len)?;
        self.offset += prefix_len as u64;

        let read_payload = should_read_payload(scratch);
        if read_payload {
            append_exact_from_bufread(&mut self.reader, scratch, payload_len - prefix_len)?;
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
        append_exact_from_bufread(&mut self.reader, scratch, initial_prefix_len)?;
        self.offset += initial_prefix_len as u64;

        let target_len = match select_payload(scratch) {
            CarPayloadRead::Skip => initial_prefix_len,
            CarPayloadRead::Prefix(len) => len.min(payload_len),
            CarPayloadRead::Full => payload_len,
        };

        if target_len > initial_prefix_len {
            append_exact_from_bufread(&mut self.reader, scratch, target_len - initial_prefix_len)?;
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

/// Append exactly `additional` bytes without first zero-filling the
/// destination's spare capacity.
///
/// `Vec::extend_from_slice` safely writes into spare capacity, while the
/// `BufRead` interface lets us avoid exposing uninitialized bytes to an
/// arbitrary `Read` implementation.
fn append_exact_from_bufread<R: BufRead>(
    reader: &mut R,
    out: &mut Vec<u8>,
    additional: usize,
) -> CarReadResult<()> {
    let target_len = out
        .len()
        .checked_add(additional)
        .ok_or_else(|| CarReadError::InvalidData("payload length overflow".to_string()))?;
    out.reserve(additional);

    while out.len() < target_len {
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(CarReadError::Io(err.to_string())),
        };
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            )
            .into());
        }

        let consumed = (target_len - out.len()).min(available.len());
        out.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
    }

    Ok(())
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
    use std::io::{self, BufReader, Cursor, Read};

    use super::{CarBlockReader, CarPayloadRead, append_exact_from_bufread, entry_payload_slice};
    use crate::error::CarReadError;

    fn framing_car(payloads: &[&[u8]]) -> Vec<u8> {
        let mut car = vec![0]; // Empty CAR header for framing-level tests.
        for (index, payload) in payloads.iter().enumerate() {
            let entry_len = 36usize.checked_add(payload.len()).unwrap();
            assert!(
                entry_len < 128,
                "test helper only supports one-byte lengths"
            );
            car.push(entry_len as u8);
            car.extend_from_slice(&[index as u8; 36]);
            car.extend_from_slice(payload);
        }
        car
    }

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
        let car = framing_car(&[&[0xaa, 0xbb, 0xcc, 0xdd]]);

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

    #[test]
    fn prefix_reader_can_materialize_payload_tail() {
        let payload = [0xaau8, 0xbb, 0xcc, 0xdd, 0xee];
        let car = framing_car(&[&payload]);
        let mut reader = CarBlockReader::with_capacity(&car[..], 2);
        reader.skip_header().unwrap();
        let mut scratch = Vec::with_capacity(32);
        let capacity = scratch.capacity();

        let entry = reader
            .read_entry_payload_if_prefix_with_scratch(&mut scratch, 2, |prefix| {
                assert_eq!(prefix, [0xaa, 0xbb]);
                true
            })
            .unwrap()
            .unwrap();

        assert_eq!(entry.prefix, [0xaa, 0xbb]);
        assert_eq!(entry.payload, Some(payload.as_slice()));
        drop(entry);
        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 1);
    }

    #[test]
    fn exact_append_reuses_capacity_across_small_fill_buf_chunks() {
        let input = [1u8, 2, 3, 4, 5];
        let mut reader = BufReader::with_capacity(2, Cursor::new(input));
        let mut out = Vec::with_capacity(32);
        out.extend_from_slice(&[9, 8]);
        let capacity = out.capacity();

        append_exact_from_bufread(&mut reader, &mut out, input.len()).unwrap();

        assert_eq!(out, [9, 8, 1, 2, 3, 4, 5]);
        assert_eq!(out.capacity(), capacity);
    }

    #[test]
    fn exact_append_retries_interrupted_reads() {
        struct InterruptOnce<R> {
            inner: R,
            interrupted: bool,
        }

        impl<R: Read> Read for InterruptOnce<R> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(io::Error::from(io::ErrorKind::Interrupted));
                }
                self.inner.read(buf)
            }
        }

        let input = [4u8, 3, 2, 1];
        let source = InterruptOnce {
            inner: Cursor::new(input),
            interrupted: false,
        };
        let mut reader = BufReader::with_capacity(2, source);
        let mut out = Vec::new();

        append_exact_from_bufread(&mut reader, &mut out, input.len()).unwrap();

        assert_eq!(out, input);
    }

    #[test]
    fn full_payload_reader_reuses_scratch_capacity() {
        let payload = [0x10u8, 0x20, 0x30, 0x40, 0x50];
        let car = framing_car(&[&payload]);
        let mut reader = CarBlockReader::with_capacity(&car[..], 2);
        reader.skip_header().unwrap();
        let mut scratch = Vec::with_capacity(64);
        scratch.extend_from_slice(&[0xff; 16]);
        let capacity = scratch.capacity();

        let entry = reader
            .read_entry_payload_with_scratch(&mut scratch)
            .unwrap()
            .unwrap();
        assert_eq!(entry.payload, payload);
        assert_eq!(entry.payload_len, payload.len());
        assert_eq!(entry.total_len, 1 + 36 + payload.len());
        drop(entry);

        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 1);
    }

    #[test]
    fn selective_reader_extends_prefix_or_materializes_full_payload() {
        let first = [1u8, 2, 3, 4, 5];
        let second = [6u8, 7, 8, 9];
        let car = framing_car(&[&first, &second]);
        let mut reader = CarBlockReader::with_capacity(&car[..], 2);
        reader.skip_header().unwrap();
        let mut scratch = Vec::with_capacity(64);
        let capacity = scratch.capacity();

        let prefix = reader
            .read_entry_payload_select_with_scratch(&mut scratch, 1, |initial| {
                assert_eq!(initial, [1]);
                CarPayloadRead::Prefix(3)
            })
            .unwrap()
            .unwrap();
        assert_eq!(prefix.prefix, [1, 2, 3]);
        assert!(prefix.payload.is_none());
        drop(prefix);

        let full = reader
            .read_entry_payload_select_with_scratch(&mut scratch, 1, |initial| {
                assert_eq!(initial, [6]);
                CarPayloadRead::Full
            })
            .unwrap()
            .unwrap();
        assert_eq!(full.prefix, second);
        assert_eq!(full.payload, Some(second.as_slice()));
        drop(full);

        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 2);
    }

    #[test]
    fn truncated_full_payload_preserves_reader_error_and_offset_semantics() {
        let mut car = vec![0, 40]; // Header, then CID + declared four-byte payload.
        car.extend_from_slice(&[0u8; 36]);
        car.extend_from_slice(&[0xaa, 0xbb]);
        let mut reader = CarBlockReader::with_capacity(&car[..], 2);
        reader.skip_header().unwrap();
        let mut scratch = Vec::with_capacity(8);

        let error = match reader.read_entry_payload_with_scratch(&mut scratch) {
            Err(error) => error,
            Ok(_) => panic!("truncated payload unexpectedly succeeded"),
        };

        assert!(matches!(error, CarReadError::Io(_)));
        assert_eq!(reader.offset, 1 + 1 + 36);
        assert_eq!(reader.entry_index, 0);
    }
}
