use serde::{Deserialize, Serialize};

use crate::StringTable;

pub const STR_ID: &str = "TransferHook1111111111111111111111111111111";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferHookLog {
    Error(TransferHookErrorLog),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferHookErrorLog {
    IncorrectAccount,
    MintHasNoMintAuthority,
    IncorrectMintAuthority,
    ProgramCalledOutsideOfTransfer,
}

impl TransferHookErrorLog {
    #[inline]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "Incorrect account provided" => Some(Self::IncorrectAccount),
            "Mint has no mint authority" => Some(Self::MintHasNoMintAuthority),
            "Incorrect mint authority has signed the instruction" => {
                Some(Self::IncorrectMintAuthority)
            }
            "Program called outside of a token transfer" => {
                Some(Self::ProgramCalledOutsideOfTransfer)
            }
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IncorrectAccount => "Incorrect account provided",
            Self::MintHasNoMintAuthority => "Mint has no mint authority",
            Self::IncorrectMintAuthority => "Incorrect mint authority has signed the instruction",
            Self::ProgramCalledOutsideOfTransfer => "Program called outside of a token transfer",
        }
    }
}

impl TransferHookLog {
    #[inline]
    pub fn parse(payload: &str, _st: &mut StringTable) -> Option<Self> {
        TransferHookErrorLog::parse(payload).map(Self::Error)
    }

    #[inline]
    pub fn as_str(&self, _st: &StringTable) -> String {
        match self {
            Self::Error(e) => e.as_str().to_string(),
        }
    }
}
