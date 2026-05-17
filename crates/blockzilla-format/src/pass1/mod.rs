use of_car_reader::reconstruct::Cid36;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pass1BlockIndexHeader {
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pass1BlockIndexRecord {
    pub slot: u64,
    pub block_cid: Cid36,
    pub blockhash: [u8; 32],
    pub first_car_entry_index: u64,
    pub first_car_offset: u64,
    pub block_car_entry_index: u64,
    pub block_car_offset: u64,
    pub reconstruct_record_start: u64,
    pub reconstruct_record_len: u64,
    pub historical_record_start: u64,
    pub historical_record_len: u64,
    pub metadata_record_start: u64,
    pub metadata_record_len: u64,
    pub entry_count: u64,
    pub transaction_count: u64,
    pub dataframe_count: u64,
    pub has_rewards: bool,
}
