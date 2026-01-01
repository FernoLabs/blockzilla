use serde::{Deserialize, Serialize};

/// SPL Token program id (Tokenkeg...)
pub const STR_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenErrorLog {
    NotRentExempt,
    InsufficientFunds,
    InvalidMint,
    MintMismatch,
    OwnerMismatch,
    FixedSupply,
    AlreadyInUse,
    InvalidNumberOfProvidedSigners,
    InvalidNumberOfRequiredSigners,
    UninitializedState,
    NativeNotSupported,
    NonNativeHasBalance,
    InvalidInstruction,
    InvalidState,
    Overflow,
    AuthorityTypeNotSupported,
    MintCannotFreeze,
    AccountFrozen,
    MintDecimalsMismatch,
    NonNativeNotSupported,
}

impl TokenErrorLog {
    #[inline]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "Error: Lamport balance below rent-exempt threshold" => Some(Self::NotRentExempt),
            "Error: insufficient funds" => Some(Self::InsufficientFunds),
            "Error: Invalid Mint" => Some(Self::InvalidMint),
            "Error: Account not associated with this Mint" => Some(Self::MintMismatch),
            "Error: owner does not match" => Some(Self::OwnerMismatch),
            "Error: the total supply of this token is fixed" => Some(Self::FixedSupply),
            "Error: account or token already in use" => Some(Self::AlreadyInUse),
            "Error: Invalid number of provided signers" => {
                Some(Self::InvalidNumberOfProvidedSigners)
            }
            "Error: Invalid number of required signers" => {
                Some(Self::InvalidNumberOfRequiredSigners)
            }
            "Error: State is uninitialized" => Some(Self::UninitializedState),
            "Error: Instruction does not support native tokens" => Some(Self::NativeNotSupported),
            "Error: Non-native account can only be closed if its balance is zero" => {
                Some(Self::NonNativeHasBalance)
            }
            "Error: Invalid instruction" => Some(Self::InvalidInstruction),
            "Error: Invalid account state for operation" => Some(Self::InvalidState),
            "Error: Operation overflowed" => Some(Self::Overflow),
            "Error: Account does not support specified authority type" => {
                Some(Self::AuthorityTypeNotSupported)
            }
            "Error: This token mint cannot freeze accounts" => Some(Self::MintCannotFreeze),
            "Error: Account is frozen" => Some(Self::AccountFrozen),
            "Error: decimals different from the Mint decimals" => Some(Self::MintDecimalsMismatch),
            "Error: Instruction does not support non-native tokens" => {
                Some(Self::NonNativeNotSupported)
            }
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotRentExempt => "Error: Lamport balance below rent-exempt threshold",
            Self::InsufficientFunds => "Error: insufficient funds",
            Self::InvalidMint => "Error: Invalid Mint",
            Self::MintMismatch => "Error: Account not associated with this Mint",
            Self::OwnerMismatch => "Error: owner does not match",
            Self::FixedSupply => "Error: the total supply of this token is fixed",
            Self::AlreadyInUse => "Error: account or token already in use",
            Self::InvalidNumberOfProvidedSigners => "Error: Invalid number of provided signers",
            Self::InvalidNumberOfRequiredSigners => "Error: Invalid number of required signers",
            Self::UninitializedState => "Error: State is uninitialized",
            Self::NativeNotSupported => "Error: Instruction does not support native tokens",
            Self::NonNativeHasBalance => {
                "Error: Non-native account can only be closed if its balance is zero"
            }
            Self::InvalidInstruction => "Error: Invalid instruction",
            Self::InvalidState => "Error: Invalid account state for operation",
            Self::Overflow => "Error: Operation overflowed",
            Self::AuthorityTypeNotSupported => {
                "Error: Account does not support specified authority type"
            }
            Self::MintCannotFreeze => "Error: This token mint cannot freeze accounts",
            Self::AccountFrozen => "Error: Account is frozen",
            Self::MintDecimalsMismatch => "Error: decimals different from the Mint decimals",
            Self::NonNativeNotSupported => "Error: Instruction does not support non-native tokens",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenLog {
    Error(TokenErrorLog),

    InstructionInitializeMint,
    InstructionInitializeAccount,
    InstructionInitializeAccount2,
    InstructionInitializeAccount3,
    InstructionInitializeMultisig,
    InstructionInitializeMultisig2,
    InstructionTransfer,
    InstructionTransferChecked,
    InstructionApprove,
    InstructionRevoke,
    InstructionSetAuthority,
    InstructionMintTo,
    InstructionMintToChecked,
    InstructionBurn,
    InstructionBurnChecked,
    InstructionCloseAccount,
    InstructionFreezeAccount,
    InstructionThawAccount,
    InstructionSyncNative,
}

impl TokenLog {
    /// `text` is the payload after "Program log: "
    #[inline]
    pub fn parse(text: &str) -> Option<Self> {
        if let Some(e) = TokenErrorLog::parse(text) {
            return Some(Self::Error(e));
        }

        let name = text.strip_prefix("Instruction: ")?.trim();
        match name {
            "InitializeMint" => Some(Self::InstructionInitializeMint),
            "InitializeAccount" => Some(Self::InstructionInitializeAccount),
            "InitializeAccount2" => Some(Self::InstructionInitializeAccount2),
            "InitializeAccount3" => Some(Self::InstructionInitializeAccount3),
            "InitializeMultisig" => Some(Self::InstructionInitializeMultisig),
            "InitializeMultisig2" => Some(Self::InstructionInitializeMultisig2),
            "Transfer" => Some(Self::InstructionTransfer),
            "TransferChecked" => Some(Self::InstructionTransferChecked),
            "Approve" => Some(Self::InstructionApprove),
            "Revoke" => Some(Self::InstructionRevoke),
            "SetAuthority" => Some(Self::InstructionSetAuthority),
            "MintTo" => Some(Self::InstructionMintTo),
            "MintToChecked" => Some(Self::InstructionMintToChecked),
            "Burn" => Some(Self::InstructionBurn),
            "BurnChecked" => Some(Self::InstructionBurnChecked),
            "CloseAccount" => Some(Self::InstructionCloseAccount),
            "FreezeAccount" => Some(Self::InstructionFreezeAccount),
            "ThawAccount" => Some(Self::InstructionThawAccount),
            "SyncNative" => Some(Self::InstructionSyncNative),
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error(e) => e.as_str(),
            Self::InstructionInitializeMint => "Instruction: InitializeMint",
            Self::InstructionInitializeAccount => "Instruction: InitializeAccount",
            Self::InstructionInitializeAccount2 => "Instruction: InitializeAccount2",
            Self::InstructionInitializeAccount3 => "Instruction: InitializeAccount3",
            Self::InstructionInitializeMultisig => "Instruction: InitializeMultisig",
            Self::InstructionInitializeMultisig2 => "Instruction: InitializeMultisig2",
            Self::InstructionTransfer => "Instruction: Transfer",
            Self::InstructionTransferChecked => "Instruction: TransferChecked",
            Self::InstructionApprove => "Instruction: Approve",
            Self::InstructionRevoke => "Instruction: Revoke",
            Self::InstructionSetAuthority => "Instruction: SetAuthority",
            Self::InstructionMintTo => "Instruction: MintTo",
            Self::InstructionMintToChecked => "Instruction: MintToChecked",
            Self::InstructionBurn => "Instruction: Burn",
            Self::InstructionBurnChecked => "Instruction: BurnChecked",
            Self::InstructionCloseAccount => "Instruction: CloseAccount",
            Self::InstructionFreezeAccount => "Instruction: FreezeAccount",
            Self::InstructionThawAccount => "Instruction: ThawAccount",
            Self::InstructionSyncNative => "Instruction: SyncNative",
        }
    }
}
