pub mod block_stream;
pub mod decode;
pub mod node;
pub mod rpc_block;

use anyhow::Result;
use cid::Cid;
use futures::io::AsyncRead;
use fvm_ipld_car::{CarHeader, CarReader};
use serde_cbor::Value;

use decode::decode_node;
use node::Node;

pub struct CarStream<R: AsyncRead + Unpin + Send> {
    pub inner: CarReader<R>,
}

impl<R: AsyncRead + Unpin + Send> CarStream<R> {
    pub async fn new(reader: R) -> Result<Self> {
        let inner = CarReader::new(reader).await?;
        Ok(Self { inner })
    }

    pub fn header(&self) -> &CarHeader {
        &self.inner.header
    }

    pub async fn next(&mut self) -> Result<Option<(Cid, Node)>> {
        if let Some(block) = self.inner.next_block().await? {
            let val: Value = serde_cbor::from_slice(&block.data)?;
            let node = decode_node(&val)?;
            Ok(Some((block.cid, node)))
        } else {
            Ok(None)
        }
    }
}
