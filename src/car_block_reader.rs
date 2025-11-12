use anyhow::{Result, anyhow};
use bytes::Bytes;
use cid::Cid;
use std::io::{self, Read};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

use crate::node::{BlockNode, Node, decode_node, peek_node_type};

pub struct CarBlock {
    pub block_bytes: Bytes,
    pub entries: Vec<Bytes>,
    pub entry_index: Vec<(Bytes, usize)>,
}

impl CarBlock {
    pub fn block(&self) -> Result<BlockNode<'_>> {
        let Node::Block(block) = decode_node(&self.block_bytes)? else {
            return Err(anyhow!("Invalid block"));
        };
        Ok(block)
    }

    pub fn decode(&self, key: &[u8]) -> Result<Node<'_>> {
        let idx = self
            .entry_index
            .binary_search_by(|(raw, _)| raw.as_ref().cmp(key))
            .map_err(|_| anyhow!("CID not found in block index"))?;
        let entry_idx = self.entry_index[idx].1;
        decode_node(&self.entries[entry_idx])
    }

    pub fn dataframe_reader(&self, cid: &Cid) -> DataFrameReader<'_> {
        DataFrameReader {
            blk: self,
            current_index: Some(cid.to_bytes()),
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct EntryMeta {
    cid_start: usize,
    cid_end: usize,
    payload_start: usize,
    payload_end: usize,
}

pub struct CarBlockReader<R: AsyncRead + Unpin + Send> {
    reader: BufReader<R>,
    buf: Vec<u8>,
    entry_offsets: Vec<EntryMeta>,
}

impl<R: AsyncRead + Unpin + Send> CarBlockReader<R> {
    pub fn new(inner: R) -> Self {
        Self::with_capacity(inner, 64 << 10)
    }

    pub fn with_capacity(inner: R, read_capacity: usize) -> Self {
        Self {
            reader: BufReader::with_capacity(read_capacity, inner),
            buf: Vec::with_capacity(512 << 10),
            entry_offsets: Vec::with_capacity(8192),
        }
    }

    pub async fn read_header(&mut self) -> Result<()> {
        let len = read_varint(&mut self.reader).await?;
        let mut remaining = len;
        let mut discard_buf = vec![0u8; (64 << 10).min(len)];
        while remaining > 0 {
            let to_read = remaining.min(discard_buf.len());
            self.reader.read_exact(&mut discard_buf[..to_read]).await?;
            remaining -= to_read;
        }
        Ok(())
    }

    pub async fn next_block(&mut self) -> Result<Option<CarBlock>> {
        loop {
            let Some((entry_start, entry_end, payload_start)) = self.read_next_entry().await?
            else {
                if self.entry_offsets.is_empty() {
                    return Ok(None);
                }
                tracing::error!("unexpected EOF: block without BlockNode");
                return Ok(None);
            };

            let mut cbor_start = payload_start;
            let payload = &self.buf[payload_start..entry_end];
            let is_cbor = matches!(payload.first(), Some(b) if (0x80..=0xBF).contains(b));

            let kind_is_block = if is_cbor {
                peek_node_type(payload).map(|k| k == 2).unwrap_or(false)
            } else if let Ok((_, skip)) = read_varint_usize_sync(payload) {
                cbor_start = payload_start + skip;
                let payload2 = &self.buf[cbor_start..entry_end];
                matches!(peek_node_type(payload2), Ok(2))
            } else {
                false
            };

            if kind_is_block {
                let frozen = Bytes::copy_from_slice(&self.buf);
                let block_bytes = frozen.slice(cbor_start..entry_end);

                let mut entries = Vec::with_capacity(self.entry_offsets.len());
                let mut entry_index = Vec::with_capacity(self.entry_offsets.len());
                for (idx, m) in self.entry_offsets.drain(..).enumerate() {
                    let cid = frozen.slice(m.cid_start..m.cid_end);
                    let payload = frozen.slice(m.payload_start..m.payload_end);
                    entries.push(payload);
                    entry_index.push((cid, idx));
                }

                entries.sort_unstable_by(|(a, _), (b, _)| a.as_ref().cmp(b.as_ref()));

                self.buf.clear();
                self.entry_offsets.clear();

                return Ok(Some(CarBlock {
                    block_bytes,
                    entries,
                    entry_index,
                }));
            }

            self.entry_offsets.push(EntryMeta {
                cid_start: entry_start,
                cid_end: payload_start,
                payload_start,
                payload_end: entry_end,
            });
        }
    }

    async fn read_next_entry(&mut self) -> Result<Option<(usize, usize, usize)>> {
        loop {
            let len = match read_varint(&mut self.reader).await {
                Ok(l) => l,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            if len == 0 {
                tracing::warn!("encountered zero-length CAR entry; skipping");
                continue;
            }

            let start = self.buf.len();
            self.buf.resize(start + len, 0);
            self.reader
                .read_exact(&mut self.buf[start..start + len])
                .await?;

            let entry_slice = &self.buf[start..start + len];
            let cid_len =
                read_cid_len(entry_slice).map_err(|e| anyhow!("CID parse/len error: {e}"))?;
            let payload_start = start + cid_len;

            return Ok(Some((start, start + len, payload_start)));
        }
    }
}

pub struct DataFrameReader<'b> {
    blk: &'b CarBlock,
    current_index: Option<Vec<u8>>,
    offset: usize,
}

impl<'b> Read for DataFrameReader<'b> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let mut written = 0;
        while written < out.len() {
            let Some(ref key) = self.current_index else {
                break;
            };

            let entry_idx = match self
                .blk
                .entry_index
                .binary_search_by(|(raw, _)| raw.as_ref().cmp(key.as_slice()))
            {
                Ok(i) => self.blk.entry_index[i].1,
                Err(_) => return Ok(written),
            };

            let node = decode_node(&self.blk.entries[entry_idx]).map_err(io::Error::other)?;

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

#[inline]
fn read_varint_usize_sync(buf: &[u8]) -> anyhow::Result<(usize, usize)> {
    let mut value: usize = 0;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        value |= ((b & 0x7F) as usize) << shift;
        if b & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(anyhow::anyhow!("varint overflow"));
        }
    }
    Err(anyhow::anyhow!("unexpected EOF in varint"))
}

#[inline]
fn read_cid_len(buf: &[u8]) -> anyhow::Result<usize> {
    if buf.is_empty() {
        return Err(anyhow::anyhow!("empty entry"));
    }

    if buf[0] != 0x01 {
        return Err(anyhow::anyhow!(
            "expected CIDv1 (0x01), got 0x{:02x}",
            buf[0]
        ));
    }

    let mut off = 1;

    let (_, n_codec) = read_varint_usize_sync(&buf[off..])?;
    off += n_codec;

    let (_, n_mh_code) = read_varint_usize_sync(&buf[off..])?;
    off += n_mh_code;

    let (digest_len, n_mh_len) = read_varint_usize_sync(&buf[off..])?;
    off += n_mh_len;

    if buf.len() < off + digest_len {
        return Err(anyhow::anyhow!("CID multihash digest truncated"));
    }
    off += digest_len;

    Ok(off)
}
