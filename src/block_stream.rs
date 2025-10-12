use ahash::AHashMap;
use anyhow::{Result, anyhow};
use cid::{Cid, CidGeneric};
use tokio::io::AsyncRead;

use crate::{
    car_reader::{AsyncCarBlock, AsyncCarReader},
    node::{BlockNode, DataFrame, EntryNode, Node, decode_node, peek_node_type},
};

#[derive(Debug, Clone)]
pub struct BlockBuffer {
    buf: Vec<u8>,
    pub index: AHashMap<Cid, (usize, usize)>,
}

impl BlockBuffer {
    fn insert(&mut self, cid: Cid, data: &[u8]) {
        let start = self.buf.len();
        self.buf.extend_from_slice(data);
        let end = self.buf.len();
        self.index.insert(cid, (start, end));
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.index.clear();
    }

    pub fn get(&self, cid: &Cid) -> Option<Node<'_>> {
        self.index
            .get(cid)
            .map(|&(s, e)| &self.buf[s..e])
            .map(|data| decode_node(data).unwrap())
    }
    pub fn get_cids(&self) -> std::collections::hash_map::Keys<'_, CidGeneric<64>, (usize, usize)> {
        self.index.keys()
    }
    pub fn get_block_entries(&self) -> impl Iterator<Item = EntryNode> + '_ {
        self.index
            .iter()
            .filter(|(_k, (s, e))| peek_node_type(&self.buf[*s..*e]).unwrap() == 1)
            .map(|(_id, (s, e))| {
                let entry: EntryNode = minicbor::decode(&self.buf[*s..*e]).unwrap();
                entry
            })
    }
}

#[derive(Debug, Clone)]
pub struct CarBlock<'a> {
    pub block: BlockNode,
    pub entries: &'a BlockBuffer,
}

impl<'a> CarBlock<'a> {
    #[inline(always)]
    pub fn merge_dataframe(&self, root: DataFrame) -> Result<Vec<u8>> {
        use smallvec::SmallVec;

        let mut buffer =
            Vec::with_capacity(root.total.map(|t| t as usize).unwrap_or(root.data.len()));
        let mut stack: SmallVec<[DataFrame; 16]> = SmallVec::new();
        stack.push(root);

        while let Some(frame) = stack.pop() {
            buffer.extend_from_slice(&frame.data);

            if let Some(next) = &frame.next {
                let Some(Node::DataFrame(next_df)) = self.entries.get(&next.0) else {
                    return Err(anyhow!("Missing DataFrame CID"));
                };
                stack.push(next_df);
            }
        }

        Ok(buffer)
    }
}
pub struct SolanaBlockStream<R: AsyncRead + Unpin + Send> {
    reader: AsyncCarReader<R>,
    block_data: BlockBuffer,
}

impl<R: AsyncRead + Unpin + Send> SolanaBlockStream<R> {
    pub async fn new(reader: R) -> Result<Self> {
        let mut reader = AsyncCarReader::new(reader);
        let _header = reader.read_header().await?;
        Ok(Self {
            reader,
            block_data: BlockBuffer {
                buf: Vec::with_capacity(10 * 1024 * 1024),
                index: AHashMap::with_capacity(3 * 1024),
            },
        })
    }

    pub async fn next_solana_block<'a>(&mut self) -> Result<Option<CarBlock<'_>>> {
        self.block_data.clear();

        while let Some(AsyncCarBlock { cid, data }) = self.reader.next_block().await? {
            match peek_node_type(data)? {
                2 => {
                    let mut d = minicbor::Decoder::new(data);
                    let block = d.decode()?;
                    return Ok(Some(CarBlock {
                        block,
                        entries: &self.block_data,
                    }));
                }
                _ => self.block_data.insert(cid, data),
            }
        }
        Ok(None)
    }
}
