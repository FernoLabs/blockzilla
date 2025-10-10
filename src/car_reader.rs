use anyhow::{Result, anyhow};
use cid::Cid;
use tokio::io::{self, AsyncRead, AsyncReadExt, BufReader};

pub struct AsyncCarBlock {
    pub cid: Cid,
    pub data: Vec<u8>,
}

pub struct AsyncCarReader<R: AsyncRead + Unpin + Send> {
    reader: BufReader<R>,
}

impl<R: AsyncRead + Unpin + Send> AsyncCarReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            reader: BufReader::with_capacity(10 * 1024 * 1024, inner),
        }
    }

    pub async fn open(path: &str) -> Result<AsyncCarReader<tokio::fs::File>> {
        let file = tokio::fs::File::open(path).await?;
        let mut reader = AsyncCarReader::new(file);
        reader.read_header().await?;
        Ok(reader)
    }

    pub async fn read_header(&mut self) -> Result<()> {
        let len = read_varint_usize(&mut self.reader).await?;
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).await?;
        //let _value: Value = minicbor::from_slice(&buf)?;
        Ok(())
    }

    pub async fn next_block(&mut self) -> Result<Option<AsyncCarBlock>> {
        let len = match read_varint_usize(&mut self.reader).await {
            Ok(l) => l,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).await?;

        // Parse directly from the slice
        let mut cursor = std::io::Cursor::new(&buf);
        let cid = Cid::read_bytes(&mut cursor).map_err(|e| anyhow!("CID parse error: {e}"))?;
        let pos = cursor.position() as usize;

        Ok(Some(AsyncCarBlock {
            cid,
            data: buf[pos..len].to_vec(),
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
}
