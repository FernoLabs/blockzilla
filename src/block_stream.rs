use anyhow::{Result, anyhow};
use cid::Cid;
use std::collections::{HashMap, HashSet};
use tokio::io::AsyncRead;

use crate::{
    node::{BlockNode, DataFrame, Node, decode_node},
    car_reader::{AsyncCarReader, AsyncCarBlock},
};

#[derive(Debug)]
pub struct CarBlock {
    pub block: BlockNode,
    pub entries: HashMap<Cid, Node>,
}

impl CarBlock {
    /// Optimized merge with pre-allocation based on total field
    pub fn merge_dataframe(&self, root: &DataFrame) -> Result<Vec<u8>> {
        // Pre-allocate based on total field if available
        let capacity = root.total.map(|t| t as usize).unwrap_or(root.data.len());
        let mut buffer = Vec::with_capacity(capacity);
        
        let mut visited = HashSet::new();
        let mut stack = Vec::new();
        stack.push(root);

        while let Some(frame) = stack.pop() {
            if let Some(hash) = frame.hash {
                if !visited.insert(hash) {
                    return Err(anyhow!("Cycle detected at DataFrame hash={hash}"));
                }
            }

            buffer.extend_from_slice(&frame.data);

            if let Some(next_cids) = &frame.next {
                for cid in next_cids.iter().rev() {
                    let node = self.entries.get(cid)
                        .ok_or_else(|| anyhow!("Missing DataFrame CID {}", cid))?;
                    
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
    reader: AsyncCarReader<R>,
    cache: HashMap<Cid, Node>,
}

impl<R: AsyncRead + Unpin + Send> SolanaBlockStream<R> {
    pub async fn new(reader: R) -> Result<Self> {
        let mut reader = AsyncCarReader::new(reader, 16 << 20);
        let _header = reader.read_header().await?;
        Ok(Self {
            reader,
            cache: HashMap::with_capacity(1024), // Pre-allocate
        })
    }

    pub async fn next_solana_block(&mut self) -> Result<Option<CarBlock>> {
        while let Some(AsyncCarBlock { cid, data }) = self.reader.next_block().await? {
            let node = decode_node(data)?;
            
            match node {
                Node::Block(block_node) => {
                    let entries = std::mem::replace(
                        &mut self.cache, 
                        HashMap::with_capacity(1024)
                    );
                    return Ok(Some(CarBlock { block: block_node, entries }));
                }
                _ => {
                    self.cache.insert(cid, node);
                }
            }
        }
        Ok(None)
    }
}