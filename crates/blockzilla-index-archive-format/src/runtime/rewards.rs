//! `runtime/rewards.wincode`: dense transaction reward records.
//!
//! Whole-record absence is owned by `EffectState`. When Outcome proves the
//! source metadata envelope, a clear Rewards bit is the sole known-empty
//! encoding, so this dense stream rejects empty records. Pubkeys remain
//! dictionary IDs.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::wincode::{self as wire, ArchiveWincodeConfig};

pub const PATH: &str = "runtime/rewards.wincode";
pub const SCHEMA: u16 = 1;
pub const MAX_REWARDS_PER_ENTRY: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct Reward {
    pub pubkey_id: u32,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: i32,
    pub commission: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct RewardRecord {
    rewards: Vec<Reward>,
}

pub(crate) fn validate_rewards(rewards: &[Reward]) -> Result<(), RewardError> {
    if rewards.len() > MAX_REWARDS_PER_ENTRY {
        return Err(RewardError::TooManyRewards(rewards.len()));
    }
    if rewards.iter().any(|reward| reward.pubkey_id == 0) {
        return Err(RewardError::ReservedPubkeyId);
    }
    Ok(())
}

pub fn append_record(chunk: &mut Vec<u8>, rewards: &[Reward]) -> Result<(), RewardError> {
    if rewards.is_empty() {
        return Err(RewardError::EmptyDenseRecord);
    }
    validate_rewards(rewards)?;
    let record = RewardRecord {
        rewards: rewards.to_vec(),
    };
    wincode::config::serialize_into(chunk, &record, wire::archive_wincode_config())?;
    Ok(())
}

pub fn encode_record(rewards: &[Reward]) -> Result<Vec<u8>, RewardError> {
    let mut bytes = Vec::new();
    append_record(&mut bytes, rewards)?;
    Ok(bytes)
}

pub fn decode_chunk(bytes: &[u8], record_count: u32) -> Result<Vec<Vec<Reward>>, RewardError> {
    let mut remaining = bytes;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record = <RewardRecord as SchemaRead<'_, ArchiveWincodeConfig>>::get(&mut remaining)?;
        if record.rewards.is_empty() {
            return Err(RewardError::EmptyDenseRecord);
        }
        validate_rewards(&record.rewards)?;
        records.push(record.rewards);
    }
    if !remaining.is_empty() {
        return Err(RewardError::TrailingBytes(remaining.len()));
    }
    Ok(records)
}

#[derive(Debug, Error)]
pub enum RewardError {
    #[error("reward Wincode: {0}")]
    WincodeRead(#[from] wincode::ReadError),
    #[error("reward Wincode: {0}")]
    WincodeWrite(#[from] wincode::WriteError),
    #[error("known-empty transaction rewards belong in EffectState, not a dense record")]
    EmptyDenseRecord,
    #[error("pubkey ID zero is reserved")]
    ReservedPubkeyId,
    #[error("entry has {0} rewards, above the decode guard")]
    TooManyRewards(usize),
    #[error("reward chunk has {0} trailing bytes")]
    TrailingBytes(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reward(pubkey_id: u32) -> Reward {
        Reward {
            pubkey_id,
            lamports: -500,
            post_balance: 7_000,
            reward_type: 2,
            commission: Some(10),
        }
    }

    #[test]
    fn dense_records_preserve_signed_values() {
        let records = [vec![reward(1), reward(900_001)]];
        let mut chunk = Vec::new();
        for record in &records {
            append_record(&mut chunk, record).unwrap();
        }
        assert_eq!(decode_chunk(&chunk, 1).unwrap(), records);
    }

    #[test]
    fn empty_dense_record_is_non_canonical() {
        assert!(matches!(
            encode_record(&[]),
            Err(RewardError::EmptyDenseRecord)
        ));
        assert!(matches!(
            decode_chunk(&[0], 1),
            Err(RewardError::EmptyDenseRecord)
        ));
    }

    #[test]
    fn zero_pubkey_is_rejected() {
        assert!(matches!(
            encode_record(&[reward(0)]),
            Err(RewardError::ReservedPubkeyId)
        ));
    }
}
