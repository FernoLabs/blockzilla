use anyhow::{Result, anyhow};
use cid::Cid;
use futures::io::AsyncRead;
use fvm_ipld_car::{CarHeader, CarReader};
use std::collections::{HashMap, HashSet};

use crate::node::{BlockNode, DataFrame, Node, decode_node};

#[derive(Debug)]
pub struct CarBlock {
    pub block: BlockNode,
    pub entries: HashMap<Cid, Node>,
}
impl CarBlock {
    /// Follows and merges a linked list of DataFrames into one contiguous Vec<u8>.
    pub fn merge_dataframe(&self, root: &DataFrame) -> Result<Vec<u8>> {
        let mut visited = HashSet::new();
        let mut buffer = Vec::new();
        let mut stack = Vec::new();

        // start from root
        stack.push(root);

        while let Some(frame) = stack.pop() {
            // prevent infinite recursion
            if let Some(hash) = frame.hash
                && !visited.insert(hash)
            {
                return Err(anyhow!("Cycle detected at DataFrame hash={hash}"));
            }

            // merge data
            buffer.extend_from_slice(&frame.data);

            // follow next links
            if let Some(next_cids) = &frame.next {
                for cid in next_cids {
                    let Some(node) = self.entries.get(cid) else {
                        return Err(anyhow!("Missing DataFrame CID {}", cid));
                    };
                    let Node::DataFrame(next_frame) = node else {
                        return Err(anyhow!("Invalid node type for CID {}", cid));
                    };
                    stack.push(next_frame);
                }
            }
        }

        Ok(buffer)
    }
}

pub struct SolanaBlockStream<R: AsyncRead + Unpin + Send> {
    inner: CarReader<R>,
    cache: HashMap<Cid, Node>,
}

impl<R: AsyncRead + Unpin + Send> SolanaBlockStream<R> {
    pub async fn new(reader: R) -> Result<Self> {
        let inner = CarReader::new(reader).await?;
        Ok(Self {
            inner,
            cache: HashMap::new(),
        })
    }

    pub fn header(&self) -> &CarHeader {
        &self.inner.header
    }

    pub async fn next_solana_block(&mut self) -> Result<Option<CarBlock>> {
        while let Some(car_block) = self.inner.next_block().await? {
            let node = decode_node(&car_block.data)?;
            if let Node::Block(b) = node {
                return Ok(Some(CarBlock {
                    block: b,
                    entries: self.cache.drain().collect(), // clone / clear maybe faster
                }));
            } else {
                self.cache.insert(car_block.cid, node);
            }
        }

        Ok(None)
    }
}
