use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::CompactMetaV1;

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactBlockRecord<'a> {
    pub header: CompactBlockHeader,
    #[serde(borrow)]
    pub txs: Vec<CompactTxWithMeta<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactShredding {
    pub entry_end_idx: i64,
    pub shred_end_idx: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactPohEntry {
    pub num_hashes: u64,
    pub hash: [u8; 32],
    pub tx_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactDataFrame {
    pub hash: Option<u64>,
    pub index: Option<u64>,
    pub total: Option<u64>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactBlockHeader {
    pub slot: u64,
    pub parent_slot: u64,
    pub blockhash: u32,
    pub previous_blockhash: u32,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub shredding: Vec<CompactShredding>,
    pub poh_entries: Vec<CompactPohEntry>,
    pub rewards: Option<CompactDataFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTxWithMeta<'a> {
    #[serde(borrow)]
    pub tx: crate::compact::CompactTransaction<'a>,
    pub metadata: Option<CompactMetaV1>,
}
