use crate::carblock_to_compact::{CompactBlock, CompactReward, CompactRewardType};
use crate::cbor_utils::CborArrayView;
use minicbor::Decoder;
use minicbor::decode::Error as DecodeError;

use super::transaction::CompactVersionedTxRef;

#[derive(Debug, minicbor::Decode)]
#[cbor(array)]
pub struct CompactBlockRef<'a> {
    #[n(0)]
    pub slot: u64,
    #[n(1)]
    #[cbor(borrow = "'a + 'bytes")]
    pub txs: CborArrayView<'a, CompactVersionedTxRef<'a>>,
    #[n(2)]
    #[cbor(borrow = "'a + 'bytes")]
    pub rewards: CborArrayView<'a, CompactRewardRef>,
}

impl<'a> CompactBlockRef<'a> {
    pub fn to_owned(&self) -> Result<CompactBlock, DecodeError> {
        let mut txs = Vec::with_capacity(self.txs.len());
        for tx in self.txs.iter() {
            txs.push(tx?.to_owned()?);
        }

        let mut rewards = Vec::with_capacity(self.rewards.len());
        for reward in self.rewards.iter() {
            rewards.push(reward?.to_owned());
        }

        Ok(CompactBlock {
            slot: self.slot,
            txs,
            rewards,
        })
    }

    pub fn tx_iter(
        &self,
    ) -> impl Iterator<Item = Result<CompactVersionedTxRef<'a>, DecodeError>> + 'a {
        self.txs.iter()
    }

    pub fn reward_iter(&self) -> impl Iterator<Item = Result<CompactRewardRef, DecodeError>> + 'a {
        self.rewards.iter()
    }
}

#[derive(Debug, minicbor::Decode)]
#[cbor(array)]
pub struct CompactRewardRef {
    #[n(0)]
    pub account_id: u32,
    #[n(1)]
    pub lamports: i64,
    #[n(2)]
    pub post_balance: u64,
    #[n(3)]
    pub reward_type: CompactRewardType,
    #[n(4)]
    pub commission: Option<u8>,
}

impl CompactRewardRef {
    pub fn to_owned(&self) -> CompactReward {
        CompactReward {
            account_id: self.account_id,
            lamports: self.lamports,
            post_balance: self.post_balance,
            reward_type: self.reward_type,
            commission: self.commission,
        }
    }
}

pub fn decode_compact_block(bytes: &[u8]) -> Result<CompactBlockRef<'_>, DecodeError> {
    let mut d = Decoder::new(bytes);
    let block: CompactBlockRef = d.decode()?;
    Ok(block)
}

pub fn decode_owned_compact_block(bytes: &[u8]) -> Result<CompactBlock, DecodeError> {
    let view = decode_compact_block(bytes)?;
    view.to_owned()
}

impl<'b, C> minicbor::Decode<'b, C> for CompactRewardType {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let value = d.u32()? as u8;
        let reward_type = match value {
            0 => CompactRewardType::Fee,
            1 => CompactRewardType::Rent,
            2 => CompactRewardType::Staking,
            3 => CompactRewardType::Voting,
            4 => CompactRewardType::Revoked,
            255 => CompactRewardType::Unspecified,
            _ => return Err(DecodeError::message("invalid CompactRewardType")),
        };
        Ok(reward_type)
    }
}
