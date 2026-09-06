//! `runtime/token_balances.wincode`: dense token-balance records.
//!
//! Each token account identity is stored once. A post identity exists only
//! when it changed. Two-sided amounts store `pre` plus a compact post delta.
//! `EffectState` owns whole-record absence. When Outcome proves the source
//! metadata envelope, a clear TokenBalances bit is the sole known-empty
//! encoding, so this dense stream rejects empty records.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::wincode::{self as wire, ArchiveWincodeConfig};

pub const PATH: &str = "runtime/token_balances.wincode";
pub const SCHEMA: u16 = 1;
pub const MAX_ENTRIES: usize = 1 << 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenBalanceIdentity {
    pub mint: Option<u32>,
    pub owner: Option<u32>,
    pub program_id: Option<u32>,
    pub decimals: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenBalance {
    pub account_index: u32,
    pub mint: Option<u32>,
    pub owner: Option<u32>,
    pub program_id: Option<u32>,
    pub decimals: u8,
    pub post_identity: Option<TokenBalanceIdentity>,
    pub pre: Option<u64>,
    pub post: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct TokenBalanceRecord {
    balances: Vec<WireTokenBalance>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct WireTokenBalance {
    account_delta: u32,
    identity: WireIdentity,
    post_identity: Option<WireIdentity>,
    amounts: TokenAmounts,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct WireIdentity {
    mint: Option<u32>,
    owner: Option<u32>,
    program_id: Option<u32>,
    decimals: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
enum TokenAmounts {
    Both { pre: u64, delta: AmountDelta },
    PreOnly(u64),
    PostOnly(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
enum AmountDelta {
    Debit(u64),
    Credit(u64),
}

impl WireIdentity {
    fn validate(&self) -> Result<(), TokenBalanceError> {
        if [self.mint, self.owner, self.program_id]
            .into_iter()
            .flatten()
            .any(|id| id == 0)
        {
            return Err(TokenBalanceError::ReservedPubkeyId);
        }
        Ok(())
    }
}

impl From<&TokenBalanceIdentity> for WireIdentity {
    fn from(value: &TokenBalanceIdentity) -> Self {
        Self {
            mint: value.mint,
            owner: value.owner,
            program_id: value.program_id,
            decimals: value.decimals,
        }
    }
}

impl From<WireIdentity> for TokenBalanceIdentity {
    fn from(value: WireIdentity) -> Self {
        Self {
            mint: value.mint,
            owner: value.owner,
            program_id: value.program_id,
            decimals: value.decimals,
        }
    }
}

impl TokenBalanceRecord {
    fn from_balances(balances: &[TokenBalance]) -> Result<Self, TokenBalanceError> {
        if balances.is_empty() {
            return Err(TokenBalanceError::EmptyDenseRecord);
        }
        if balances.len() > MAX_ENTRIES {
            return Err(TokenBalanceError::TooManyEntries(balances.len()));
        }
        let mut previous = None;
        let mut records = Vec::with_capacity(balances.len());
        for balance in balances {
            let identity = WireIdentity {
                mint: balance.mint,
                owner: balance.owner,
                program_id: balance.program_id,
                decimals: balance.decimals,
            };
            identity.validate()?;
            let account_delta = match previous {
                None => balance.account_index,
                Some(previous) => balance
                    .account_index
                    .checked_sub(previous)
                    .filter(|delta| *delta != 0)
                    .ok_or(TokenBalanceError::IndicesNotAscending {
                        previous,
                        current: balance.account_index,
                    })?,
            };
            previous = Some(balance.account_index);
            let post_identity = balance.post_identity.as_ref().map(WireIdentity::from);
            if let Some(post_identity) = &post_identity {
                post_identity.validate()?;
                if balance.pre.is_none() || balance.post.is_none() {
                    return Err(TokenBalanceError::PostIdentityWithoutBothSides(
                        balance.account_index,
                    ));
                }
                if post_identity == &identity {
                    return Err(TokenBalanceError::RedundantPostIdentity(
                        balance.account_index,
                    ));
                }
            }
            let amounts = match (balance.pre, balance.post) {
                (Some(pre), Some(post)) => TokenAmounts::Both {
                    pre,
                    delta: if post >= pre {
                        AmountDelta::Credit(post - pre)
                    } else {
                        AmountDelta::Debit(pre - post)
                    },
                },
                (Some(pre), None) => TokenAmounts::PreOnly(pre),
                (None, Some(post)) => TokenAmounts::PostOnly(post),
                (None, None) => return Err(TokenBalanceError::NeitherSide(balance.account_index)),
            };
            records.push(WireTokenBalance {
                account_delta,
                identity,
                post_identity,
                amounts,
            });
        }
        Ok(Self { balances: records })
    }

    fn into_balances(self) -> Result<Vec<TokenBalance>, TokenBalanceError> {
        if self.balances.is_empty() {
            return Err(TokenBalanceError::EmptyDenseRecord);
        }
        if self.balances.len() > MAX_ENTRIES {
            return Err(TokenBalanceError::TooManyEntries(self.balances.len()));
        }
        let mut account_index = 0_u32;
        let mut records = Vec::with_capacity(self.balances.len());
        for (number, balance) in self.balances.into_iter().enumerate() {
            account_index = if number == 0 {
                balance.account_delta
            } else {
                if balance.account_delta == 0 {
                    return Err(TokenBalanceError::IndicesNotAscending {
                        previous: account_index,
                        current: account_index,
                    });
                }
                account_index
                    .checked_add(balance.account_delta)
                    .ok_or(TokenBalanceError::IndexOverflow)?
            };
            balance.identity.validate()?;
            if let Some(identity) = &balance.post_identity {
                identity.validate()?;
                if identity == &balance.identity {
                    return Err(TokenBalanceError::RedundantPostIdentity(account_index));
                }
            }
            let (pre, post) = match balance.amounts {
                TokenAmounts::Both { pre, delta } => {
                    let post = match delta {
                        AmountDelta::Debit(0) => {
                            return Err(TokenBalanceError::NonCanonicalZeroDebit);
                        }
                        AmountDelta::Debit(value) => pre.checked_sub(value),
                        AmountDelta::Credit(value) => pre.checked_add(value),
                    }
                    .ok_or(TokenBalanceError::AmountOutOfRange { account_index })?;
                    (Some(pre), Some(post))
                }
                TokenAmounts::PreOnly(pre) => (Some(pre), None),
                TokenAmounts::PostOnly(post) => (None, Some(post)),
            };
            let identity = TokenBalanceIdentity::from(balance.identity);
            records.push(TokenBalance {
                account_index,
                mint: identity.mint,
                owner: identity.owner,
                program_id: identity.program_id,
                decimals: identity.decimals,
                post_identity: balance.post_identity.map(TokenBalanceIdentity::from),
                pre,
                post,
            });
        }
        Ok(records)
    }
}

pub fn append_record(
    chunk: &mut Vec<u8>,
    balances: &[TokenBalance],
) -> Result<(), TokenBalanceError> {
    let record = TokenBalanceRecord::from_balances(balances)?;
    wincode::config::serialize_into(chunk, &record, wire::archive_wincode_config())?;
    Ok(())
}

pub fn encode_record(balances: &[TokenBalance]) -> Result<Vec<u8>, TokenBalanceError> {
    let mut bytes = Vec::new();
    append_record(&mut bytes, balances)?;
    Ok(bytes)
}

pub fn decode_chunk(
    bytes: &[u8],
    record_count: u32,
) -> Result<Vec<Vec<TokenBalance>>, TokenBalanceError> {
    let mut remaining = bytes;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record =
            <TokenBalanceRecord as SchemaRead<'_, ArchiveWincodeConfig>>::get(&mut remaining)?;
        records.push(record.into_balances()?);
    }
    if !remaining.is_empty() {
        return Err(TokenBalanceError::TrailingBytes(remaining.len()));
    }
    Ok(records)
}

#[derive(Debug, Error)]
pub enum TokenBalanceError {
    #[error("token-balance Wincode: {0}")]
    WincodeRead(#[from] wincode::ReadError),
    #[error("token-balance Wincode: {0}")]
    WincodeWrite(#[from] wincode::WriteError),
    #[error("known-empty token balances belong in EffectState, not a dense record")]
    EmptyDenseRecord,
    #[error("transaction has {0} token balances, above the decode guard")]
    TooManyEntries(usize),
    #[error("pubkey ID zero is reserved")]
    ReservedPubkeyId,
    #[error("post identity for account {0} requires pre and post amounts")]
    PostIdentityWithoutBothSides(u32),
    #[error("post identity for account {0} repeats the base identity")]
    RedundantPostIdentity(u32),
    #[error("account indexes must ascend: {previous} then {current}")]
    IndicesNotAscending { previous: u32, current: u32 },
    #[error("account index overflows u32")]
    IndexOverflow,
    #[error("token balance for account {0} has neither side")]
    NeitherSide(u32),
    #[error("token amount for account {account_index} leaves u64")]
    AmountOutOfRange { account_index: u32 },
    #[error("a zero amount delta must use Credit")]
    NonCanonicalZeroDebit,
    #[error("token-balance chunk has {0} trailing bytes")]
    TrailingBytes(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer() -> Vec<TokenBalance> {
        vec![TokenBalance {
            account_index: 2,
            mint: Some(1),
            owner: Some(2),
            program_id: Some(3),
            decimals: 6,
            post_identity: None,
            pre: Some(1_000_000),
            post: Some(999_995),
        }]
    }

    #[test]
    fn identity_once_and_amount_delta_round_trip() {
        let records = [transfer()];
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
            Err(TokenBalanceError::EmptyDenseRecord)
        ));
        // Zero is the Wincode length of an empty TokenBalanceRecord.
        assert!(matches!(
            decode_chunk(&[0], 1),
            Err(TokenBalanceError::EmptyDenseRecord)
        ));
    }

    #[test]
    fn one_sided_amounts_remain_exact() {
        let mut balances = transfer();
        balances[0].pre = None;
        assert_eq!(
            decode_chunk(&encode_record(&balances).unwrap(), 1).unwrap()[0],
            balances
        );
    }
}
