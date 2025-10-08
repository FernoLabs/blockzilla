pub mod block_stream;
pub mod node;
pub mod rpc_block;

use anyhow::Result;
use cid::Cid;
use futures::io::AsyncRead;
use fvm_ipld_car::{CarHeader, CarReader};

pub use node::Node;

use crate::node::decode_node;

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
            let node = decode_node(&block.data)?;
            Ok(Some((block.cid, node)))
        } else {
            Ok(None)
        }
    }
}
