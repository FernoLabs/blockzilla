use anyhow::Result;
use cid::Cid;
use futures::io::AsyncRead;
use fvm_ipld_car::{CarHeader, CarReader};
use serde_cbor::Value;
use std::collections::HashMap;

use crate::decode::decode_node;
use crate::node::{Entry, Node, Transaction};

#[derive(Debug)]
pub struct SolanaBlock {
    pub slot: u64,
    pub entries: Vec<Entry>,
    pub transactions: Vec<Transaction>,
    pub meta: crate::node::SlotMeta,
}

pub struct SolanaBlockStream<R: AsyncRead + Unpin + Send> {
    inner: CarReader<R>,
    scratch: Vec<u8>,
    cache: HashMap<Cid, Node>,
}

impl<R: AsyncRead + Unpin + Send> SolanaBlockStream<R> {
    pub async fn new(reader: R) -> Result<Self> {
        let inner = CarReader::new(reader).await?;
        Ok(Self {
            inner,
            scratch: Vec::with_capacity(8192),
            cache: HashMap::new(),
        })
    }

    pub fn header(&self) -> &CarHeader {
        &self.inner.header
    }

    pub async fn next_solana_block(&mut self) -> Result<Option<SolanaBlock>> {
        while let Some(block) = self.inner.next_block().await? {
            self.scratch.clear();
            self.scratch.extend_from_slice(&block.data);
            let val: Value = serde_cbor::from_slice(&self.scratch)?;
            let node = decode_node(&val)?;
            let cid = block.cid;

            self.cache.insert(cid, node.clone());

            if let Node::Block(b) = node {
                let mut entries = Vec::new();
                for e_cid in &b.entries {
                    if let Some(Node::Entry(entry)) = self.cache.get(e_cid) {
                        entries.push(entry.clone());
                    }
                }

                let mut txs = Vec::new();
                for e in &entries {
                    for t_cid in &e.transactions {
                        if let Some(Node::Transaction(tx)) = self.cache.get(t_cid) {
                            txs.push(tx.clone());
                        }
                    }
                }

                return Ok(Some(SolanaBlock {
                    slot: b.slot,
                    entries,
                    transactions: txs,
                    meta: b.meta,
                }));
            }
        }

        Ok(None)
    }
}
