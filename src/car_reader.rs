use anyhow::{Result, anyhow};
use cid::Cid;
use tokio::io::{self, AsyncRead, AsyncReadExt, BufReader};

/// Borrowed view over the current CAR block.
/// `data` borrows from the reader's internal buffer.
pub struct AsyncCarBlock<'a> {
    pub cid: Cid,
    pub data: &'a [u8],
}

pub struct AsyncCarReader<R: AsyncRead + Unpin + Send> {
    reader: BufReader<R>,
    buf: Vec<u8>,  // reused buffer
}

impl<R: AsyncRead + Unpin + Send> AsyncCarReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            reader: BufReader::with_capacity(10 * 1024 * 1024, inner),
            buf: Vec::with_capacity(1 << 20), // preallocate 1 MB
        }
    }

    pub async fn open(path: &str) -> Result<AsyncCarReader<tokio::fs::File>> {
        let file = tokio::fs::File::open(path).await?;
        let mut reader = AsyncCarReader::new(file);
        reader.read_header().await?;
        Ok(reader)
    }

    /// Read and discard the CAR header.
    pub async fn read_header(&mut self) -> Result<()> {
        let len = read_varint_usize(&mut self.reader).await?;
        if len > self.buf.capacity() {
            self.buf.reserve(len - self.buf.capacity());
        }
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf).await?;
        Ok(())
    }

    /// Read next block, borrowing bytes from the internal buffer.
    pub async fn next_block(&mut self) -> Result<Option<AsyncCarBlock<'_>>> {
        let len = match read_varint_usize(&mut self.reader).await {
            Ok(l) => l,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if len > self.buf.capacity() {
            self.buf.reserve(len - self.buf.capacity());
        }
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf).await?;

        // Parse CID directly from the buffer
        let mut cursor = std::io::Cursor::new(&self.buf);
        let cid = Cid::read_bytes(&mut cursor).map_err(|e| anyhow!("CID parse error: {e}"))?;
        let pos = cursor.position() as usize;

        Ok(Some(AsyncCarBlock {
            cid,
            data: &self.buf[pos..len], // <-- borrow, not clone
        }))
    }
}

#[inline]
async fn read_varint_usize<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<usize> {
    let mut value: usize = 0;
    let mut shift = 0;
    let mut buf = [0u8; 1];

    loop {
        reader.read_exact(&mut buf).await?;
        let byte = buf[0];
        value |= ((byte & 0x7F) as usize) << shift;

        if (byte & 0x80) == 0 {
            return Ok(value);
        }

        shift += 7;
        if shift >= std::mem::size_of::<usize>() * 8 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "varint overflow"));
        }
    }
}
