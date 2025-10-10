use ahash::AHashMap as HashMap;
use anyhow::{Result, anyhow};
use cid::Cid;
use tokio::io::AsyncRead;

use crate::{
    car_reader::{AsyncCarBlock, AsyncCarReader},
    node::{BlockNode, DataFrame, Node, decode_node},
};

#[derive(Debug)]
pub struct CarBlock {
    pub block: BlockNode,
    pub entries: HashMap<Cid, Node>,
}

impl CarBlock {
    #[inline(always)]
    pub fn merge_dataframe(&self, root: &DataFrame) -> Result<Vec<u8>> {
        use smallvec::SmallVec;

        let mut buffer =
            Vec::with_capacity(root.total.map(|t| t as usize).unwrap_or(root.data.len()));
        let mut stack: SmallVec<[&DataFrame; 16]> = SmallVec::new();
        stack.push(root);

        while let Some(frame) = stack.pop() {
            buffer.extend_from_slice(&frame.data);

            if let Some(next) = &frame.next {
                for cid in next.iter().rev() {
                    let Some(Node::DataFrame(next_frame)) = self.entries.get(cid) else {
                        return Err(anyhow!("Missing DataFrame CID"));
                    };
                    stack.push(next_frame);
                }
            }
        }

        Ok(buffer)
    }
}
pub struct SolanaBlockStream<R: AsyncRead + Unpin + Send> {
    reader: AsyncCarReader<R>,
    cache: HashMap<Cid, Node>,
    last_cap: usize,
}

impl<R: AsyncRead + Unpin + Send> SolanaBlockStream<R> {
    pub async fn new(reader: R) -> Result<Self> {
        let mut reader = AsyncCarReader::new(reader, 16 << 20);
        let _header = reader.read_header().await?;
        Ok(Self {
            reader,
            cache: HashMap::with_capacity(2048),
            last_cap: 2048,
        })
    }

    pub async fn next_solana_block(&mut self) -> Result<Option<CarBlock>> {
        while let Some(AsyncCarBlock { cid, data }) = self.reader.next_block().await? {
            let node = decode_node(data)?;

            match node {
                Node::Block(block_node) => {
                    // reuse map allocation
                    let entries = std::mem::take(&mut self.cache);
                    self.last_cap = entries.capacity();
                    return Ok(Some(CarBlock {
                        block: block_node,
                        entries,
                    }));
                }
                _ => {
                    self.cache.insert(cid, node);
                }
            }
        }
        Ok(None)
    }
}
