use cid::Cid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFrame {
    pub kind: u64,
    pub hash: Option<u64>,
    pub index: Option<u64>,
    pub total: Option<u64>,
    pub data: Vec<u8>,
    pub next: Option<Vec<Cid>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub kind: u64,
    pub data: DataFrame,
    pub metadata: DataFrame,
    pub slot: u64,
    pub index: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub kind: u64,
    pub num_hashes: u64,
    pub hash: Vec<u8>,
    pub transactions: Vec<Cid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SlotMeta {
    pub parent_slot: Option<u64>,
    pub blocktime: Option<u64>,
    pub block_height: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shredding {
    pub entry_end_idx: u64,
    pub shred_end_idx: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub kind: u64,
    pub slot: u64,
    pub shredding: Vec<Shredding>,
    pub entries: Vec<Cid>,
    pub meta: SlotMeta,
    pub rewards: Option<Cid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subset {
    pub kind: u64,
    pub first: u64,
    pub last: u64,
    pub blocks: Vec<Cid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Epoch {
    pub kind: u64,
    pub epoch: u64,
    pub subsets: Vec<Cid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rewards {
    pub kind: u64,
    pub slot: u64,
    pub data: DataFrame,
}

#[derive(Debug, Clone)]
pub enum Node {
    Transaction(Transaction),
    Entry(Entry),
    Block(Block),
    Subset(Subset),
    Epoch(Epoch),
    Rewards(Rewards),
    DataFrame(DataFrame),
}
