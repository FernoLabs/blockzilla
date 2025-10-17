use ahash::AHashMap;
use anyhow::{Result, anyhow};
use bytes::{Bytes, BytesMut};
use cid::Cid;
use std::io::{self, Read};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader, ReadBuf};

use crate::node::{BlockNode, Node, decode_node, peek_node_type};

/// A self-contained partial CAR file (one logical Solana block)
/// Owns all raw bytes and CIDs for zero-copy decoding within this block.
pub struct CarBlock {
    pub block_bytes: Bytes,
    pub index: AHashMap<Cid, Bytes>,
}

impl CarBlock {
    pub fn block(&self) -> Result<BlockNode<'_>> {
        let Node::Block(block) = decode_node(&self.block_bytes)? else {
            return Err(anyhow!("Invalid block"));
        };
        Ok(block)
    }

    /// Decode one node zero-copy; lifetime is tied to this CarBlock
    pub fn decode<'b>(&'b self, cid: &Cid) -> Result<Node<'b>> {
        let bytes = self
            .index
            .get(cid)
            .ok_or_else(|| anyhow!("index out of range"))?;
        decode_node(bytes)
    }

    /// Peek node kind without decoding
    pub fn peek_kind(&self, cid: &Cid) -> Result<u64> {
        let bytes = self
            .index
            .get(cid)
            .ok_or_else(|| anyhow!("index out of range"))?;
        peek_node_type(bytes)
    }

    /// Build a streaming reader over concatenated DataFrames starting at index
    pub fn dataframe_reader<'b>(&'b self, cid: &'b Cid) -> DataFrameReader<'b> {
        DataFrameReader {
            blk: self,
            current_index: Some(*cid),
            offset: 0,
        }
    }

    /// Async version of the above
    pub fn async_dataframe_reader<'b>(&'b self, cid: &'b Cid) -> AsyncDataFrameReader<'b> {
        AsyncDataFrameReader {
            blk: self,
            current_index: Some(*cid),
            offset: 0,
        }
    }
}

/// Reader that streams the CAR file and yields self-contained CarBlocks.
/// Each block can be processed independently and in parallel.
pub struct CarBlockReader<R: AsyncRead + Unpin + Send> {
    reader: BufReader<R>,
    buf: BytesMut,
    cur_index: AHashMap<Cid, Bytes>,
}

impl<R: AsyncRead + Unpin + Send> CarBlockReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            reader: BufReader::with_capacity(100 << 20, inner),
            buf: BytesMut::with_capacity(1 << 20),
            cur_index: AHashMap::with_capacity(256),
        }
    }

    pub async fn read_header(&mut self) -> Result<()> {
        let len = read_varint_usize(&mut self.reader).await?;
        self.ensure_capacity(len);
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf[..len]).await?;
        Ok(())
    }

    pub async fn next_block(&mut self) -> Result<Option<CarBlock>> {
        self.cur_index.clear();

        loop {
            let Some((cid, payload)) = self.read_next_entry().await? else {
                tracing::debug!("unexpected EOF: block without BlockNode");
                return Ok(None);
            };

            // Check if this is a BlockNode before adding to collections
            if peek_node_type(&payload)? == 2 {
                return Ok(Some(self.flush_block(payload)));
            }

            // Only store non-BlockNode entries
            self.cur_index.insert(cid.clone(), payload);
        }
    }

    fn flush_block(&mut self, block_bytes: Bytes) -> CarBlock {
        CarBlock {
            block_bytes,
            index: std::mem::take(&mut self.cur_index),
        }
    }

    async fn read_next_entry(&mut self) -> Result<Option<(Cid, Bytes)>> {
        let len = match read_varint_usize(&mut self.reader).await {
            Ok(l) => l,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        self.ensure_capacity(len);
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf[..len]).await?;
        let bytes = self.buf.split_to(len).freeze();

        let mut cursor = std::io::Cursor::new(&bytes);
        let cid = Cid::read_bytes(&mut cursor).map_err(|e| anyhow!("CID parse error: {e}"))?;
        let pos = cursor.position() as usize;
        Ok(Some((cid, bytes.slice(pos..))))
    }

    fn ensure_capacity(&mut self, len: usize) {
        if self.buf.capacity() < len {
            let new_cap = len.next_power_of_two().max(self.buf.capacity() * 2);
            self.buf.reserve(new_cap - self.buf.capacity());
        }
    }
}

/// Compact unsigned varint reader
#[inline]
async fn read_varint_usize<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<usize> {
    let mut value: usize = 0;
    let mut shift = 0;
    loop {
        let b = reader.read_u8().await?;
        value |= ((b & 0x7F) as usize) << shift;
        if b & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= usize::BITS as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
}
/// Synchronous reader over chained DataFrames inside a CarBlock
pub struct DataFrameReader<'b> {
    blk: &'b CarBlock,
    current_index: Option<Cid>,
    offset: usize,
}

impl<'b> Read for DataFrameReader<'b> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let mut written = 0;

        while written < out.len() {
            let Some(cid) = self.current_index else {
                break;
            };
            let node = self
                .blk
                .decode(&cid)
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
                self.current_index = df.next.map(|cid| cid.to_cid().unwrap());
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
    current_index: Option<Cid>,
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
            let Some(cid) = self.current_index else {
                break;
            };
            let node = self
                .blk
                .decode(&cid)
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
                self.current_index = df.next.map(|cid| cid.to_cid().unwrap());
            }

            if to_copy == 0 {
                break;
            }
        }

        buf.advance(written);
        Poll::Ready(Ok(()))
    }
}
