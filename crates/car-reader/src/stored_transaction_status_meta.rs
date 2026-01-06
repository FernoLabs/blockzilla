use std::mem::MaybeUninit;
use wincode::ReadResult;
use wincode::error::invalid_tag_encoding;
use wincode::io::Reader;
use wincode::{SchemaRead, containers, len::BincodeLen, len::ShortU16Len};

use crate::stored_transaction_error::StoredTransactionError;
use crate::{confirmed_block, convert_metadata};

#[derive(SchemaRead, Clone)]
pub struct TransactionReturnData {
    pub program_id: [u8; 32],
    pub data: Vec<u8>,
}

#[derive(SchemaRead, Clone)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    #[wincode(with = "containers::Vec<_, ShortU16Len>")]
    pub accounts: Vec<u8>,
    #[wincode(with = "containers::Vec<_, ShortU16Len>")]
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
impl<'de, T> SchemaRead<'de> for OptionEof<T>
where
    T: SchemaRead<'de>,
{
    type Dst = OptionEof<T::Dst>;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let mut discriminant = MaybeUninit::<u8>::uninit();

        if let Err(e) = u8::read(reader, &mut discriminant) {
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
            println!("{:#?}", e);
            return Err(e);
        }

        let disc = unsafe { discriminant.assume_init() };
        match disc {
            0 => {
                dst.write(OptionEof::None);
                Ok(())
            }
            1 => {
                let mut value = MaybeUninit::<T::Dst>::uninit();
                T::read(reader, &mut value)?;
                let value = unsafe { value.assume_init() };
                dst.write(OptionEof::Some(value));
                Ok(())
            }
            other => Err(invalid_tag_encoding(other as usize)),
        }
    }
}
