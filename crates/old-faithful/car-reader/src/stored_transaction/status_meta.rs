use solana_short_vec::ShortU16;
use std::mem::MaybeUninit;
use wincode::ReadResult;
use wincode::config::{Config, ConfigCore};
use wincode::error::invalid_tag_encoding;
use wincode::io::Reader;
use wincode::{SchemaRead, containers, len::BincodeLen};

use crate::stored_transaction::StoredTransactionError;
use crate::{confirmed_block, convert_metadata};

#[derive(SchemaRead, Clone)]
pub struct TransactionReturnData {
    pub program_id: [u8; 32],
    pub data: Vec<u8>,
}

#[derive(SchemaRead, Clone)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    #[wincode(with = "containers::Vec<_, ShortU16>")]
    pub accounts: Vec<u8>,
    #[wincode(with = "containers::Vec<_, ShortU16>")]
    pub data: Vec<u8>,
}

#[derive(SchemaRead, Clone)]
pub struct InnerInstructions {
    pub index: u8,
    #[wincode(with = "containers::Vec<_, BincodeLen>")]
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(SchemaRead, Clone)]
pub struct StoredExtendedReward {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: Option<u8>,
    pub commission: Option<u8>,
}

#[derive(SchemaRead, Clone)]
pub struct StoredConfirmedBlockReward {
    pub pubkey: String,
    pub lamports: i64,
}

#[derive(Clone)]
pub struct LegacyLogString(pub String);

unsafe impl<'de, C: Config> SchemaRead<'de, C> for LegacyLogString {
    type Dst = Self;

    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let value = <String as SchemaRead<'de, C>>::get(reader.by_ref())?;

        // Early archive metadata sometimes leaves a raw ';' byte after
        // "consumed ... compute units" log strings. wincode 0.5 removed
        // non-consuming peeks, and this decoder is only used after the normal
        // metadata shape fails, so consume the expected legacy separator here.
        if value.ends_with(" compute units") {
            let separator = reader.take_byte()?;
            if separator != b';' {
                return Err(invalid_tag_encoding(separator as usize));
            }
        }

        dst.write(Self(value));
        Ok(())
    }
}

#[derive(SchemaRead, Clone)]
pub struct StoredTokenAmount {
    pub ui_amount: f64,
    pub decimals: u8,
    pub amount: String,
}

#[derive(SchemaRead, Clone)]
pub struct StoredTransactionTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: StoredTokenAmount,
    // can't have Option EOF here as it will interpret next StoredTransactionTokenBalance
    //pub owner: OptionEof<String>,
    //pub program_id: OptionEof<String>,
}

#[derive(SchemaRead)]
#[wincode(tag_encoding = "u32")]
pub enum TransactionResult {
    Ok,
    Err(StoredTransactionError),
}

#[derive(SchemaRead)]
pub struct StoredTransactionStatusMeta {
    pub status: TransactionResult,
    pub fee: u64,
    #[wincode(with = "containers::Vec<_, BincodeLen>")]
    pub pre_balances: Vec<u64>,
    #[wincode(with = "containers::Vec<_, BincodeLen>")]
    pub post_balances: Vec<u64>,
    #[wincode(with = "OptionEof<containers::Vec<_, BincodeLen>>")]
    pub inner_instructions: OptionEof<Vec<InnerInstructions>>,
    #[wincode(with = "OptionEof<containers::Vec<String, BincodeLen>>")]
    pub log_messages: OptionEof<Vec<String>>,
    #[wincode(with = "OptionEof<containers::Vec<_, BincodeLen>>")]
    pub pre_token_balances: OptionEof<Vec<StoredTransactionTokenBalance>>,
    #[wincode(with = "OptionEof<containers::Vec<_, BincodeLen>>")]
    pub post_token_balances: OptionEof<Vec<StoredTransactionTokenBalance>>,
    pub rewards: OptionEof<Vec<StoredExtendedReward>>,
    pub return_data: OptionEof<TransactionReturnData>,
    pub compute_units_consumed: OptionEof<u64>,
    pub cost_units: OptionEof<u64>,
}

impl From<StoredTransactionStatusMeta> for confirmed_block::TransactionStatusMeta {
    fn from(m: StoredTransactionStatusMeta) -> Self {
        convert_metadata::stored_meta_to_proto(m)
    }
}

#[derive(SchemaRead)]
pub struct StoredTransactionStatusMetaLegacyLogs {
    pub status: TransactionResult,
    pub fee: u64,
    #[wincode(with = "containers::Vec<_, BincodeLen>")]
    pub pre_balances: Vec<u64>,
    #[wincode(with = "containers::Vec<_, BincodeLen>")]
    pub post_balances: Vec<u64>,
    #[wincode(with = "OptionEof<containers::Vec<_, BincodeLen>>")]
    pub inner_instructions: OptionEof<Vec<InnerInstructions>>,
    #[wincode(with = "OptionEof<containers::Vec<LegacyLogString, BincodeLen>>")]
    pub log_messages: OptionEof<Vec<LegacyLogString>>,
    #[wincode(with = "OptionEof<containers::Vec<_, BincodeLen>>")]
    pub pre_token_balances: OptionEof<Vec<StoredTransactionTokenBalance>>,
    #[wincode(with = "OptionEof<containers::Vec<_, BincodeLen>>")]
    pub post_token_balances: OptionEof<Vec<StoredTransactionTokenBalance>>,
    pub rewards: OptionEof<Vec<StoredExtendedReward>>,
    pub return_data: OptionEof<TransactionReturnData>,
    pub compute_units_consumed: OptionEof<u64>,
    pub cost_units: OptionEof<u64>,
}

impl From<StoredTransactionStatusMetaLegacyLogs> for confirmed_block::TransactionStatusMeta {
    fn from(m: StoredTransactionStatusMetaLegacyLogs) -> Self {
        let v1 = StoredTransactionStatusMeta {
            status: m.status,
            fee: m.fee,
            pre_balances: m.pre_balances,
            post_balances: m.post_balances,
            inner_instructions: m.inner_instructions,
            log_messages: m
                .log_messages
                .map(|logs| logs.into_iter().map(|log| log.0).collect()),
            pre_token_balances: m.pre_token_balances,
            post_token_balances: m.post_token_balances,
            rewards: m.rewards,
            return_data: m.return_data,
            compute_units_consumed: m.compute_units_consumed,
            cost_units: m.cost_units,
        };
        convert_metadata::stored_meta_to_proto(v1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum OptionEof<T> {
    #[default]
    None,
    Some(T),
}

impl<T> OptionEof<T> {
    /// Returns `true` if the option is a `Some` value.
    pub const fn is_some(&self) -> bool {
        matches!(self, OptionEof::Some(_))
    }

    /// Returns `true` if the option is a `None` value.
    pub const fn is_none(&self) -> bool {
        matches!(self, OptionEof::None)
    }

    /// Converts from `&OptionEof<T>` to `OptionEof<&T>`.
    pub const fn as_ref(&self) -> OptionEof<&T> {
        match self {
            OptionEof::Some(x) => OptionEof::Some(x),
            OptionEof::None => OptionEof::None,
        }
    }

    /// Converts from `&mut OptionEof<T>` to `OptionEof<&mut T>`.
    pub fn as_mut(&mut self) -> OptionEof<&mut T> {
        match self {
            OptionEof::Some(x) => OptionEof::Some(x),
            OptionEof::None => OptionEof::None,
        }
    }

    /// Returns the contained `Some` value, consuming the `self` value.
    pub fn unwrap(self) -> T {
        match self {
            OptionEof::Some(x) => x,
            OptionEof::None => panic!("called `OptionEof::unwrap()` on a `None` value"),
        }
    }

    /// Returns the contained `Some` value or a provided default.
    pub fn unwrap_or(self, default: T) -> T {
        match self {
            OptionEof::Some(x) => x,
            OptionEof::None => default,
        }
    }

    pub fn unwrap_or_default(self) -> T
    where
        T: Default,
    {
        match self {
            OptionEof::Some(x) => x,
            OptionEof::None => T::default(),
        }
    }

    /// Returns the contained `Some` value or computes it from a closure.
    pub fn unwrap_or_else<F>(self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        match self {
            OptionEof::Some(x) => x,
            OptionEof::None => f(),
        }
    }

    /// Maps an `OptionEof<T>` to `OptionEof<U>` by applying a function to a contained value.
    pub fn map<U, F>(self, f: F) -> OptionEof<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            OptionEof::Some(x) => OptionEof::Some(f(x)),
            OptionEof::None => OptionEof::None,
        }
    }

    /// Converts from `OptionEof<T>` to `Option<T>`.
    pub fn into_option(self) -> Option<T> {
        match self {
            OptionEof::Some(x) => Some(x),
            OptionEof::None => None,
        }
    }

    /// Converts from `Option<T>` to `OptionEof<T>`.
    pub fn from_option(opt: Option<T>) -> Self {
        match opt {
            Some(x) => OptionEof::Some(x),
            None => OptionEof::None,
        }
    }
}

impl<T> From<Option<T>> for OptionEof<T> {
    fn from(opt: Option<T>) -> Self {
        Self::from_option(opt)
    }
}

impl<T> From<OptionEof<T>> for Option<T> {
    fn from(opt: OptionEof<T>) -> Self {
        opt.into_option()
    }
}
// SAFETY: this impl only initializes `dst` on success, and treats a missing trailing
// field as `None` to match the legacy metadata encoding used in archived data.
unsafe impl<'de, T, C> SchemaRead<'de, C> for OptionEof<T>
where
    C: ConfigCore,
    T: SchemaRead<'de, C>,
{
    type Dst = OptionEof<T::Dst>;

    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let disc = match <u8 as SchemaRead<'de, C>>::get(reader.by_ref()) {
            Ok(disc) => disc,
            Err(e) => {
                // Only accept true EOF as "absent field"
                if let wincode::ReadError::Io(read_err) = &e {
                    if let wincode::io::ReadError::ReadSizeLimit(_) = read_err {
                        dst.write(OptionEof::None);
                        return Ok(());
                    }
                    if let wincode::io::ReadError::Io(ioe) = read_err
                        && ioe.kind() == std::io::ErrorKind::UnexpectedEof
                    {
                        dst.write(OptionEof::None);
                        return Ok(());
                    }
                }
                return Err(e);
            }
        };
        match disc {
            0 => {
                dst.write(OptionEof::None);
                Ok(())
            }
            1 => {
                dst.write(OptionEof::Some(T::get(reader)?));
                Ok(())
            }
            other => Err(invalid_tag_encoding(other as usize)),
        }
    }
}
