use anyhow::{Result, anyhow};
use bytes::{Bytes, BytesMut};
use std::io::{self, Read};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader, ReadBuf};

use cid::Cid;

use crate::node::{BlockNode, Node, decode_node, peek_node_type};

/// A contiguous section of the CAR corresponding to a BlockNode.
/// - `block_bytes` is the *CBOR payload* of the BlockNode (not the whole group).
/// - `entries` is a sorted Vec of (raw_cid_bytes, payload_bytes),
///    both are zero-copy slices into the underlying frozen `Bytes`.
pub struct CarBlock {
    pub block_bytes: Bytes,
    pub entries: Vec<(Bytes, Bytes)>, // sorted by raw CID bytes
}

impl CarBlock {
    pub fn block(&self) -> Result<BlockNode<'_>> {
        let Node::Block(block) = decode_node(&self.block_bytes)? else {
            return Err(anyhow!("Invalid block"));
        };
        Ok(block)
    }

    /// Zero-copy decode by CID using binary search over raw CID bytes.
    pub fn decode<'b>(&'b self, key: &[u8]) -> Result<Node<'b>> {
        let idx = self
            .entries
            .binary_search_by(|(raw, _)| raw.as_ref().cmp(&key))
            .map_err(|_| anyhow!("CID not found in block index"))?;
        decode_node(&self.entries[idx].1)
    }

    /// Build a streaming reader over concatenated DataFrames by CID (binary search lookup).
    pub fn dataframe_reader<'b>(&'b self, cid: &'b Cid) -> DataFrameReader<'b> {
        DataFrameReader {
            blk: self,
            current_index: Some(cid.to_bytes()), // store search key as raw bytes
            offset: 0,
        }
    }

    pub fn async_dataframe_reader<'b>(&'b self, cid: &'b Cid) -> AsyncDataFrameReader<'b> {
        AsyncDataFrameReader {
            blk: self,
            current_index: Some(cid.to_bytes()),
            offset: 0,
        }
    }
}

/// Metadata we gather while reading entries (no CID parsing).
#[derive(Debug, Clone, Copy)]
struct EntryMeta {
    cid_start: usize,
    cid_end: usize,
    payload_start: usize,
    payload_end: usize,
}

/// Streams a CAR file and yields CarBlocks.
/// Buffering and allocations are tuned for ~1–2 MB average block sizes.
pub struct CarBlockReader<R: AsyncRead + Unpin + Send> {
    reader: BufReader<R>,
    buf: BytesMut,
    entry_offsets: Vec<EntryMeta>,
}

impl<R: AsyncRead + Unpin + Send> CarBlockReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            // Larger read buffer reduces syscalls on 1–2 MB blocks
            reader: BufReader::with_capacity(16 << 20, inner),
            // Working buffer sized to hold a typical large block without reallocations
            buf: BytesMut::with_capacity(32 << 20),
            entry_offsets: Vec::with_capacity(4096),
        }
    }

    /// Read and discard the CAR header (length-prefixed blob).
    pub async fn read_header(&mut self) -> Result<()> {
        let len = read_varint(&mut self.reader).await?;
        self.reader.consume(len);
        Ok(())
    }

    /// Reads entries until a BlockNode (kind == 2) is found.
    /// Returns a CarBlock with:
    ///   - block_bytes: the BlockNode CBOR payload
    ///   - entries: sorted Vec of (raw_cid, payload) for all *prior* entries
    pub async fn next_block(&mut self) -> Result<Option<CarBlock>> {
        self.entry_offsets.clear();

        // Keep memory footprint under control across epochs
        if self.buf.capacity() > (256 << 20) {
            self.buf = BytesMut::with_capacity(32 << 20);
        } else {
            self.buf.clear();
        }

        loop {
            let Some((entry_start, entry_end, payload_start)) = self.read_next_entry().await?
            else {
                if self.entry_offsets.is_empty() {
                    return Ok(None);
                }
                tracing::error!("unexpected EOF: block without BlockNode");
                return Ok(None);
            };

            // Some datasets store a multicodec varint *before* CBOR.
            // Try peek; if it fails, attempt to skip one varint and peek again.
            let mut cbor_start = payload_start;
            let payload = &self.buf[payload_start..entry_end];
            let is_cbor = matches!(payload.first(), Some(b) if (0x80..=0xBF).contains(b));

            let kind_is_block = if is_cbor {
                peek_node_type(payload).map(|k| k == 2).unwrap_or(false)
            } else {
                // Try skipping one varint (likely codec tag) before CBOR
                if let Ok((_, skip)) = read_varint_usize_sync(payload) {
                    cbor_start = payload_start + skip;
                    let payload2 = &self.buf[cbor_start..entry_end];
                    matches!(peek_node_type(payload2), Ok(2))
                } else {
                    false
                }
            };

            if kind_is_block {
                // Freeze the region up to and including this BlockNode entry
                let frozen = self.buf.split_to(entry_end).freeze();
                let block_bytes = frozen.slice(cbor_start..entry_end); // CBOR payload only

                // Convert gathered offsets into zero-copy (raw_cid, payload) pairs
                let mut entries: Vec<(Bytes, Bytes)> = Vec::with_capacity(self.entry_offsets.len());
                for m in self.entry_offsets.drain(..) {
                    let raw_cid = frozen.slice(m.cid_start..m.cid_end);
                    let payload = frozen.slice(m.payload_start..m.payload_end);
                    entries.push((raw_cid, payload));
                }

                // Sort once by raw CID bytes for binary-search lookups
                entries.sort_unstable_by(|(a, _), (b, _)| a.as_ref().cmp(b.as_ref()));

                return Ok(Some(CarBlock {
                    block_bytes,
                    entries,
                }));
            }

            // Not a BlockNode → remember this entry's slices for the eventual block
            self.entry_offsets.push(EntryMeta {
                cid_start: entry_start,
                cid_end: payload_start,
                payload_start,
                payload_end: entry_end,
            });
        }
    }

    /// Reads one CAR entry into `buf`; returns (entry_start, entry_end, payload_start).
    /// We do *not* parse the CID—only compute its byte length to find `payload_start`.
    async fn read_next_entry(&mut self) -> Result<Option<(usize, usize, usize)>> {
        // 1) length prefix (varint) for this entry (CID + payload)
        let len = match read_varint(&mut self.reader).await {
            Ok(l) => l,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        // 2) append entry bytes in place
        let start = self.buf.len();
        self.ensure_capacity(start + len);
        self.buf.resize(start + len, 0);
        self.reader
            .read_exact(&mut self.buf[start..start + len])
            .await?;

        // 3) compute CID length (no allocation, no full parsing)
        let entry_slice = &self.buf[start..start + len];
        let cid_len = read_cid_len(entry_slice).map_err(|e| anyhow!("CID parse/len error: {e}"))?;

        let payload_start = start + cid_len;
        Ok(Some((start, start + len, payload_start)))
    }

    fn ensure_capacity(&mut self, need: usize) {
        if self.buf.capacity() < need {
            let new_cap = need.next_power_of_two().max(self.buf.capacity() * 2);
            self.buf.reserve(new_cap - self.buf.capacity());
        }
    }
}

/// Compact unsigned varint from a BufReader
#[inline]
async fn read_varint<R: AsyncRead + Unpin>(reader: &mut BufReader<R>) -> io::Result<usize> {
    let mut value: usize = 0;
    let mut shift = 0;
    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF while reading varint",
            ));
        }
        let b = buf[0];
        reader.consume(1);

        value |= ((b & 0x7F) as usize) << shift;
        if b & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
}

/// Compact unsigned varint from a slice; returns (value, bytes_consumed)
#[inline]
fn read_varint_usize_sync(buf: &[u8]) -> Result<(usize, usize)> {
    let mut value: usize = 0;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        value |= ((b & 0x7F) as usize) << shift;
        if b & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(anyhow!("varint overflow"));
        }
    }
    Err(anyhow!("unexpected EOF in varint"))
}

/// Minimal, zero-alloc CID length parser (CIDv1).
/// CIDv1 layout: 0x01 (version) + varint(codec) + multihash(varint(code), varint(digest_len), digest)
#[inline]
fn read_cid_len(buf: &[u8]) -> Result<usize> {
    if buf.is_empty() {
        return Err(anyhow!("empty entry"));
    }

    // CIDv1 must start with 0x01
    if buf[0] != 0x01 {
        // Some data could be CIDv0; not expected in CARv1 blocks, but handle gracefully
        // Fall back to full parser if needed; here we error out to surface misuse
        return Err(anyhow!("expected CIDv1 (0x01), got 0x{:02x}", buf[0]));
    }

    let mut off = 1;

    // codec
    let (_, n_codec) = read_varint_usize_sync(&buf[off..])?;
    off += n_codec;

    // multihash: code
    let (_, n_mh_code) = read_varint_usize_sync(&buf[off..])?;
    off += n_mh_code;

    // multihash: digest length
    let (digest_len, n_mh_len) = read_varint_usize_sync(&buf[off..])?;
    off += n_mh_len;

    // digest
    if buf.len() < off + digest_len {
        return Err(anyhow!("CID multihash digest truncated"));
    }
    off += digest_len;

    Ok(off)
}

/// Synchronous reader over chained DataFrames inside a CarBlock
pub struct DataFrameReader<'b> {
    blk: &'b CarBlock,
    current_index: Option<Vec<u8>>, // raw CID search key
    offset: usize,
}

impl<'b> Read for DataFrameReader<'b> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let mut written = 0;

        while written < out.len() {
            let Some(ref key) = self.current_index else {
                break;
            };

            // binary search by raw cid bytes
            let idx = match self
                .blk
                .entries
                .binary_search_by(|(raw, _)| raw.as_ref().cmp(key.as_slice()))
            {
                Ok(i) => i,
                Err(_) => return Ok(written), // CID not found
            };

            let node = decode_node(&self.blk.entries[idx].1)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let df = match node {
                Node::DataFrame(df) => df,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected DataFrame node",
                    ));
                }
            };

            let chunk = &df.data[self.offset..];
            let to_copy = std::cmp::min(chunk.len(), out.len() - written);
            out[written..written + to_copy].copy_from_slice(&chunk[..to_copy]);
            written += to_copy;
            self.offset += to_copy;

            if self.offset >= df.data.len() {
                self.offset = 0;
                // Follow the chained CID if present
                if let Some(next_cbor_cid) = df.next {
                    if let Ok(next) = next_cbor_cid.to_cid() {
                        self.current_index = Some(next.to_bytes());
                    } else {
                        self.current_index = None;
                    }
                } else {
                    self.current_index = None;
                }
            }

            if to_copy == 0 {
                break;
            }
        }

        Ok(written)
    }
}

/// Async version of DataFrameReader
pub struct AsyncDataFrameReader<'b> {
    blk: &'b CarBlock,
    current_index: Option<Vec<u8>>, // raw CID search key
    offset: usize,
}

impl<'b> AsyncRead for AsyncDataFrameReader<'b> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let dst = buf.initialize_unfilled();
        let mut written = 0;

        while written < dst.len() {
            let Some(ref key) = self.current_index else {
                break;
            };

            let idx = match self
                .blk
                .entries
                .binary_search_by(|(raw, _)| raw.as_ref().cmp(key.as_slice()))
            {
                Ok(i) => i,
                Err(_) => break, // CID not found
            };

            let node = decode_node(&self.blk.entries[idx].1)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let df = match node {
                Node::DataFrame(df) => df,
                _ => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected DataFrame node",
                    )));
                }
            };

            let chunk = &df.data[self.offset..];
            let to_copy = std::cmp::min(chunk.len(), dst.len() - written);
            dst[written..written + to_copy].copy_from_slice(&chunk[..to_copy]);
            written += to_copy;
            self.offset += to_copy;

            if self.offset >= df.data.len() {
                self.offset = 0;
                if let Some(next_cbor_cid) = df.next {
                    if let Ok(next) = next_cbor_cid.to_cid() {
                        self.current_index = Some(next.to_bytes());
                    } else {
                        self.current_index = None;
                    }
                } else {
                    self.current_index = None;
                }
            }

            if to_copy == 0 {
                break;
            }
        }

        buf.advance(written);
        Poll::Ready(Ok(()))
    }
}
