//! `runtime/block_rewards.wincode`: block-scoped reward records.
//!
//! The catalog `FactLocator` owns absence and provenance. A present record can
//! contain an empty reward vector. It uses the same `Reward` shape as the
//! transaction-scoped reward file.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::wincode::{self as wire, ArchiveWincodeConfig};

use super::rewards::{Reward, RewardError, validate_rewards};

pub const PATH: &str = "runtime/block_rewards.wincode";
pub const SCHEMA: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct BlockRewards {
    pub num_partitions: Option<u64>,
    pub rewards: Vec<Reward>,
}

impl BlockRewards {
    pub fn validate(&self) -> Result<(), BlockRewardError> {
        validate_rewards(&self.rewards)?;
        Ok(())
    }
}

pub fn append_record(chunk: &mut Vec<u8>, record: &BlockRewards) -> Result<(), BlockRewardError> {
    record.validate()?;
    wincode::config::serialize_into(chunk, record, wire::archive_wincode_config())?;
    Ok(())
}

pub fn encode_record(record: &BlockRewards) -> Result<Vec<u8>, BlockRewardError> {
    let mut bytes = Vec::new();
    append_record(&mut bytes, record)?;
    Ok(bytes)
}

pub fn decode_record(bytes: &[u8]) -> Result<BlockRewards, BlockRewardError> {
    let record: BlockRewards = wire::decode_exact(bytes)?;
    record.validate()?;
    Ok(record)
}

pub fn decode_chunk(
    bytes: &[u8],
    record_count: u32,
) -> Result<Vec<BlockRewards>, BlockRewardError> {
    let mut remaining = bytes;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record = <BlockRewards as SchemaRead<'_, ArchiveWincodeConfig>>::get(&mut remaining)?;
        record.validate()?;
        records.push(record);
    }
    if !remaining.is_empty() {
        return Err(BlockRewardError::TrailingBytes(remaining.len()));
    }
    Ok(records)
}

#[derive(Debug, Error)]
pub enum BlockRewardError {
    #[error("block-reward Wincode: {0}")]
    WincodeRead(#[from] wincode::ReadError),
    #[error("block-reward Wincode: {0}")]
    WincodeWrite(#[from] wincode::WriteError),
    #[error("block reward: {0}")]
    Reward(#[from] RewardError),
    #[error("block-reward chunk has {0} trailing bytes")]
    TrailingBytes(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_rewards_and_partition_presence_round_trip() {
        let records = [
            BlockRewards {
                num_partitions: None,
                rewards: Vec::new(),
            },
            BlockRewards {
                num_partitions: Some(0),
                rewards: vec![Reward {
                    pubkey_id: 1,
                    lamports: -10,
                    post_balance: 20,
                    reward_type: 2,
                    commission: None,
                }],
            },
        ];
        let mut bytes = Vec::new();
        for record in &records {
            append_record(&mut bytes, record).unwrap();
        }
        assert_eq!(decode_chunk(&bytes, 2).unwrap(), records);
    }
}
