use crate::carblock_to_compact::{CompactInstructionError, CompactTxError};
use minicbor::Decoder;
use minicbor::decode::Error as DecodeError;

#[derive(Debug, Clone)]
pub enum CompactTxErrorRef<'a> {
    InstructionError {
        index: u32,
        error: CompactInstructionErrorRef,
    },
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    AlreadyProcessed,
    BlockhashNotFound,
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    SanitizedTransactionError,
    ProgramError,
    Other {
        name: &'a str,
    },
}

impl<'a> CompactTxErrorRef<'a> {
    pub(super) fn decode(d: &mut Decoder<'a>) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len == 0 {
            return Err(DecodeError::message(
                "CompactTxError expects array of length at least 1",
            ));
        }
        let tag = d.u32()?;
        match tag {
            0 => {
                let index = d.u32()?;
                let error = CompactInstructionErrorRef::decode(d)?;
                Ok(CompactTxErrorRef::InstructionError { index, error })
            }
            1 => Ok(CompactTxErrorRef::AccountInUse),
            2 => Ok(CompactTxErrorRef::AccountLoadedTwice),
            3 => Ok(CompactTxErrorRef::AccountNotFound),
            4 => Ok(CompactTxErrorRef::ProgramAccountNotFound),
            5 => Ok(CompactTxErrorRef::InsufficientFundsForFee),
            6 => Ok(CompactTxErrorRef::InvalidAccountForFee),
            7 => Ok(CompactTxErrorRef::AlreadyProcessed),
            8 => Ok(CompactTxErrorRef::BlockhashNotFound),
            9 => Ok(CompactTxErrorRef::CallChainTooDeep),
            10 => Ok(CompactTxErrorRef::MissingSignatureForFee),
            11 => Ok(CompactTxErrorRef::InvalidAccountIndex),
            12 => Ok(CompactTxErrorRef::SignatureFailure),
            13 => Ok(CompactTxErrorRef::SanitizedTransactionError),
            14 => Ok(CompactTxErrorRef::ProgramError),
            15 => {
                let name = d.str()?;
                Ok(CompactTxErrorRef::Other { name })
            }
            _ => Err(DecodeError::message("unknown CompactTxError variant")),
        }
    }

    pub fn to_owned(&self) -> CompactTxError {
        match self {
            CompactTxErrorRef::InstructionError { index, error } => {
                CompactTxError::InstructionError {
                    index: *index,
                    error: error.to_owned(),
                }
            }
            CompactTxErrorRef::AccountInUse => CompactTxError::AccountInUse,
            CompactTxErrorRef::AccountLoadedTwice => CompactTxError::AccountLoadedTwice,
            CompactTxErrorRef::AccountNotFound => CompactTxError::AccountNotFound,
            CompactTxErrorRef::ProgramAccountNotFound => CompactTxError::ProgramAccountNotFound,
            CompactTxErrorRef::InsufficientFundsForFee => CompactTxError::InsufficientFundsForFee,
            CompactTxErrorRef::InvalidAccountForFee => CompactTxError::InvalidAccountForFee,
            CompactTxErrorRef::AlreadyProcessed => CompactTxError::AlreadyProcessed,
            CompactTxErrorRef::BlockhashNotFound => CompactTxError::BlockhashNotFound,
            CompactTxErrorRef::CallChainTooDeep => CompactTxError::CallChainTooDeep,
            CompactTxErrorRef::MissingSignatureForFee => CompactTxError::MissingSignatureForFee,
            CompactTxErrorRef::InvalidAccountIndex => CompactTxError::InvalidAccountIndex,
            CompactTxErrorRef::SignatureFailure => CompactTxError::SignatureFailure,
            CompactTxErrorRef::SanitizedTransactionError => {
                CompactTxError::SanitizedTransactionError
            }
            CompactTxErrorRef::ProgramError => CompactTxError::ProgramError,
            CompactTxErrorRef::Other { name } => CompactTxError::Other {
                name: name.to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompactInstructionErrorRef {
    Builtin(u32),
    Custom(u32),
}

impl CompactInstructionErrorRef {
    pub fn to_owned(&self) -> CompactInstructionError {
        match self {
            CompactInstructionErrorRef::Builtin(code) => CompactInstructionError::Builtin(*code),
            CompactInstructionErrorRef::Custom(code) => CompactInstructionError::Custom(*code),
        }
    }

    fn decode(d: &mut Decoder<'_>) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 2 {
            return Err(DecodeError::message(
                "CompactInstructionError expects array[2]",
            ));
        }
        let variant = d.u32()?;
        let code = d.u32()?;
        match variant {
            0 => Ok(CompactInstructionErrorRef::Builtin(code)),
            1 => Ok(CompactInstructionErrorRef::Custom(code)),
            _ => Err(DecodeError::message(
                "unknown CompactInstructionError variant",
            )),
        }
    }
}
