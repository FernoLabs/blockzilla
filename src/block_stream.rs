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
    // Store Vec<u8> directly - no Arc needed if owned
    blocks: AHashMap<Cid, Vec<u8>>,
}

impl BlockBuffer {
    #[inline]
    fn insert(&mut self, cid: Cid, data: Vec<u8>) {
        self.blocks.insert(cid, data);
    }

    #[inline]
    fn clear(&mut self) {
        self.blocks.clear();
    }

    #[inline]
    pub fn get(&self, cid: &Cid) -> Option<Node<'_>> {
        self.blocks
            .get(cid)
            .and_then(|data| decode_node(data).ok())
    }

    #[inline]
    pub fn get_cids(&self) -> std::collections::hash_map::Keys<'_, CidGeneric<64>, Vec<u8>> {
        self.blocks.keys()
    }

    #[inline]
    pub fn get_block_entries(&self) -> impl Iterator<Item = EntryNode> + '_ {
        self.blocks
            .iter()
            .filter(|(_k, data)| peek_node_type(data).ok() == Some(1))
            .filter_map(|(_id, data)| {
                minicbor::decode::<EntryNode>(data.as_slice()).ok()
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
                blocks: AHashMap::with_capacity(3 * 1024),
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
                _ => {
                    // Move data into Arc instead of copying into monolithic buffer
                    self.block_data.insert(cid, data.to_vec());
                }
            }
        }
        Ok(None)
    }
}