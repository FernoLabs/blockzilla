//! `runtime/balances.wincode`: dense transaction balance records.
//!
//! A record stores `pre` once and only changed post values as signed-direction
//! deltas. `EffectState` owns whole-record absence. A present record with zero
//! accounts remains distinct from absence.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::wincode::{self as wire, ArchiveWincodeConfig};

pub const PATH: &str = "runtime/balances.wincode";
pub const SCHEMA: u16 = 1;
pub const MAX_ACCOUNTS: usize = 1 << 16;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Balances {
    pub pre: Vec<u64>,
    pub post: Vec<u64>,
}

impl Balances {
    pub fn validate(&self) -> Result<(), BalanceError> {
        if self.pre.len() != self.post.len() {
            return Err(BalanceError::LengthMismatch {
                pre: self.pre.len(),
                post: self.post.len(),
            });
        }
        if self.pre.len() > MAX_ACCOUNTS {
            return Err(BalanceError::TooManyAccounts(self.pre.len()));
        }
        Ok(())
    }

    pub fn changes(&self) -> impl Iterator<Item = (usize, i128)> + '_ {
        self.pre
            .iter()
            .zip(&self.post)
            .enumerate()
            .filter(|(_, (pre, post))| pre != post)
            .map(|(index, (pre, post))| (index, i128::from(*post) - i128::from(*pre)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct BalanceRecord {
    pre: Vec<u64>,
    changes: Vec<BalanceChange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct BalanceChange {
    position_delta: u32,
    delta: LamportDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
enum LamportDelta {
    Debit(u64),
    Credit(u64),
}

impl BalanceRecord {
    fn from_balances(balances: &Balances) -> Result<Self, BalanceError> {
        balances.validate()?;
        let mut previous = None;
        let mut changes = Vec::new();
        for (position, delta) in balances.changes() {
            let position = u32::try_from(position)
                .map_err(|_| BalanceError::TooManyAccounts(balances.pre.len()))?;
            let position_delta = previous.map_or(position, |previous| position - previous);
            changes.push(BalanceChange {
                position_delta,
                delta: if delta < 0 {
                    LamportDelta::Debit(u64::try_from(-delta).expect("u64 balance difference"))
                } else {
                    LamportDelta::Credit(u64::try_from(delta).expect("u64 balance difference"))
                },
            });
            previous = Some(position);
        }
        Ok(Self {
            pre: balances.pre.clone(),
            changes,
        })
    }

    fn into_balances(self) -> Result<Balances, BalanceError> {
        if self.pre.len() > MAX_ACCOUNTS {
            return Err(BalanceError::TooManyAccounts(self.pre.len()));
        }
        if self.changes.len() > self.pre.len() {
            return Err(BalanceError::MoreChangesThanAccounts {
                changes: self.changes.len(),
                accounts: self.pre.len(),
            });
        }
        let mut post = self.pre.clone();
        let mut position = 0_u32;
        for (number, change) in self.changes.into_iter().enumerate() {
            position = if number == 0 {
                change.position_delta
            } else {
                if change.position_delta == 0 {
                    return Err(BalanceError::IndicesNotAscending(position));
                }
                position
                    .checked_add(change.position_delta)
                    .ok_or(BalanceError::IndexOverflow)?
            };
            let account_count = post.len();
            let balance =
                post.get_mut(position as usize)
                    .ok_or(BalanceError::IndexOutsideAccounts {
                        index: position,
                        accounts: account_count,
                    })?;
            let (next, delta) = match change.delta {
                LamportDelta::Debit(0) | LamportDelta::Credit(0) => {
                    return Err(BalanceError::ZeroDelta(position));
                }
                LamportDelta::Debit(value) => (balance.checked_sub(value), -i128::from(value)),
                LamportDelta::Credit(value) => (balance.checked_add(value), i128::from(value)),
            };
            *balance = next.ok_or(BalanceError::PostBalanceOutOfRange {
                index: position,
                delta,
            })?;
        }
        Ok(Balances {
            pre: self.pre,
            post,
        })
    }
}

pub fn append_record(chunk: &mut Vec<u8>, balances: &Balances) -> Result<(), BalanceError> {
    let record = BalanceRecord::from_balances(balances)?;
    wincode::config::serialize_into(chunk, &record, wire::archive_wincode_config())?;
    Ok(())
}

pub fn encode_record(balances: &Balances) -> Result<Vec<u8>, BalanceError> {
    let mut bytes = Vec::new();
    append_record(&mut bytes, balances)?;
    Ok(bytes)
}

pub fn decode_chunk(bytes: &[u8], record_count: u32) -> Result<Vec<Balances>, BalanceError> {
    let mut remaining = bytes;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record = <BalanceRecord as SchemaRead<'_, ArchiveWincodeConfig>>::get(&mut remaining)?;
        records.push(record.into_balances()?);
    }
    if !remaining.is_empty() {
        return Err(BalanceError::TrailingBytes(remaining.len()));
    }
    Ok(records)
}

#[derive(Debug, Error)]
pub enum BalanceError {
    #[error("balance Wincode: {0}")]
    WincodeRead(#[from] wincode::ReadError),
    #[error("balance Wincode: {0}")]
    WincodeWrite(#[from] wincode::WriteError),
    #[error("pre has {pre} balances but post has {post}")]
    LengthMismatch { pre: usize, post: usize },
    #[error("transaction has {0} balances, above the decode guard")]
    TooManyAccounts(usize),
    #[error("{changes} changes exceed {accounts} accounts")]
    MoreChangesThanAccounts { changes: usize, accounts: usize },
    #[error("balance indexes are not ascending after {0}")]
    IndicesNotAscending(u32),
    #[error("balance index overflows u32")]
    IndexOverflow,
    #[error("balance index {index} is outside {accounts} accounts")]
    IndexOutsideAccounts { index: u32, accounts: usize },
    #[error("balance delta at index {0} is zero")]
    ZeroDelta(u32),
    #[error("delta {delta} at index {index} moves the balance outside u64")]
    PostBalanceOutOfRange { index: u32, delta: i128 },
    #[error("balance chunk has {0} trailing bytes")]
    TrailingBytes(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_delta_records_round_trip() {
        let records = [
            Balances {
                pre: vec![1_000_000, 20, 30],
                post: vec![995_000, 20, 35],
            },
            Balances::default(),
        ];
        let mut chunk = Vec::new();
        for record in &records {
            append_record(&mut chunk, record).unwrap();
        }
        assert_eq!(decode_chunk(&chunk, 2).unwrap(), records);
    }

    #[test]
    fn unchanged_post_values_are_not_stored_twice() {
        let record = Balances {
            pre: vec![1_000_000_000; 32],
            post: vec![1_000_000_000; 32],
        };
        let encoded = encode_record(&record).unwrap();
        let two_vectors = 32 * 8 * 2;
        assert!(encoded.len() < two_vectors / 3);
    }
}
