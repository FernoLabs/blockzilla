use serde::{Deserialize, Serialize};

use crate::CompactMetaV1;

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactBlockRecord<'a> {
    pub header: CompactBlockHeader,
    #[serde(borrow)]
    pub txs: Vec<CompactTxWithMeta<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactBlockHeader {
    pub slot: u64,
    pub parent_slot: u64,
    pub blockhash: u32,
    pub previous_blockhash: u32,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTxWithMeta<'a> {
    #[serde(borrow)]
    pub tx: crate::compact::CompactTransaction<'a>,
    pub metadata: Option<CompactMetaV1>,
}
