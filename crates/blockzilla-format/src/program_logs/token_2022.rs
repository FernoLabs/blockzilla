use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::str::FromStr;

use crate::{Registry, StrId, StringTable};

/// SPL Token-2022 program id
pub const STR_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// Compact id for any pubkey that exists in the Registry.
/// This is not “program id” specific here, it is simply the registry index + 1.
pub type PubkeyId = u32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Token2022Log {
    /// entrypoint.rs:17 msg!(error.to_str::<TokenError>())
    Error(Token2022ErrorLog),

    /// extension/confidential_transfer/processor.rs:188
    AccountNeedsResizePlusBytesDebug { bytes: usize },

    /// extension/reallocate.rs:69
    AccountNeedsResizePlusBytesDebug2 { bytes: usize },

    /// extension/confidential_transfer_fee/processor.rs:280
    ErrorHarvestingFrom {
        account_key: PubkeyId,
        /// Keep as string for now (often comes from Display impls / nested errors)
        error: StrId,
    },

    /// extension/confidential_transfer_fee/processor.rs:366
    ErrorHarvestingFrom2 { account_key: PubkeyId, error: StrId },

    /// extension/transfer_fee/processor.rs:197
    ErrorHarvestingFrom3 { account_key: PubkeyId, error: StrId },

    /// extension/transfer_fee/processor.rs:266
    ErrorHarvestingFrom4 { account_key: PubkeyId, error: StrId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Token2022ErrorLog {
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
    ExtensionTypeMismatch,
    ExtensionBaseMismatch,
    ExtensionAlreadyInitialized,
    ConfidentialTransferAccountHasBalance,
    ConfidentialTransferAccountNotApproved,
    ConfidentialTransferDepositsAndTransfersDisabled,
    ConfidentialTransferElGamalPubkeyMismatch,
    ConfidentialTransferBalanceMismatch,
    MintHasSupply,
    NoAuthorityExists,
    TransferFeeExceedsMaximum,
    MintRequiredForTransfer,
    FeeMismatch,
    FeeParametersMismatch,
    ImmutableOwner,
    AccountHasWithheldTransferFees,
    NoMemo,
    NonTransferable,
    NonTransferableNeedsImmutableOwnership,
    MaximumPendingBalanceCreditCounterExceeded,
    MaximumDepositAmountExceeded,
    CpiGuardSettingsLocked,
    CpiGuardTransferBlocked,
    CpiGuardBurnBlocked,
    CpiGuardCloseAccountBlocked,
    CpiGuardApproveBlocked,
    CpiGuardSetAuthorityBlocked,
    CpiGuardOwnerChangeBlocked,
    ExtensionNotFound,
    NonConfidentialTransfersDisabled,
    ConfidentialTransferFeeAccountHasWithheldFee,
    InvalidExtensionCombination,
    InvalidLengthForAlloc,
    AccountDecryption,
    ProofGeneration,
    InvalidProofInstructionOffset,
    HarvestToMintDisabled,
    SplitProofContextStateAccountsNotSupported,
    NotEnoughProofContextStateAccounts,
    MalformedCiphertext,
    CiphertextArithmeticFailed,
    PedersenCommitmentMismatch,
    RangeProofLengthMismatch,
    IllegalBitLength,
    FeeCalculation,
    IllegalMintBurnConversion,
    InvalidScale,
    MintPaused,
    PendingBalanceNonZero,
}

impl Token2022ErrorLog {
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

            "Error: New extension type does not match already existing extensions" => {
                Some(Self::ExtensionTypeMismatch)
            }
            "Error: Extension does not match the base type provided" => {
                Some(Self::ExtensionBaseMismatch)
            }
            "Error: Extension already initialized on this account" => {
                Some(Self::ExtensionAlreadyInitialized)
            }
            "Error: An account can only be closed if its confidential balance is zero" => {
                Some(Self::ConfidentialTransferAccountHasBalance)
            }
            "Error: Account not approved for confidential transfers" => {
                Some(Self::ConfidentialTransferAccountNotApproved)
            }
            "Error: Account not accepting deposits or transfers" => {
                Some(Self::ConfidentialTransferDepositsAndTransfersDisabled)
            }
            "Error: ElGamal public key mismatch" => {
                Some(Self::ConfidentialTransferElGamalPubkeyMismatch)
            }
            "Error: Balance mismatch" => Some(Self::ConfidentialTransferBalanceMismatch),
            "Error: Mint has non-zero supply. Burn all tokens before closing the mint" => {
                Some(Self::MintHasSupply)
            }
            "Error: No authority exists to perform the desired operation" => {
                Some(Self::NoAuthorityExists)
            }
            "Error: Transfer fee exceeds maximum of 10,000 basis points" => {
                Some(Self::TransferFeeExceedsMaximum)
            }
            "Mint required for this account to transfer tokens, use `transfer_checked` or `transfer_checked_with_fee`" => {
                Some(Self::MintRequiredForTransfer)
            }
            "Calculated fee does not match expected fee" => Some(Self::FeeMismatch),
            "Fee parameters associated with zero-knowledge proofs do not match fee parameters in mint" => {
                Some(Self::FeeParametersMismatch)
            }
            "The owner authority cannot be changed" => Some(Self::ImmutableOwner),
            "Error: An account can only be closed if its withheld fee balance is zero, harvest fees to the mint and try again" => {
                Some(Self::AccountHasWithheldTransferFees)
            }
            "Error: No memo in previous instruction required for recipient to receive a transfer" => {
                Some(Self::NoMemo)
            }
            "Transfer is disabled for this mint" => Some(Self::NonTransferable),
            "Non-transferable tokens can't be minted to an account without immutable ownership" => {
                Some(Self::NonTransferableNeedsImmutableOwnership)
            }
            "The total number of `Deposit` and `Transfer` instructions to an account cannot exceed the associated `maximum_pending_balance_credit_counter`" => {
                Some(Self::MaximumPendingBalanceCreditCounterExceeded)
            }
            "Deposit amount exceeds maximum limit" => Some(Self::MaximumDepositAmountExceeded),
            "CPI Guard status cannot be changed in CPI" => Some(Self::CpiGuardSettingsLocked),
            "CPI Guard is enabled, and a program attempted to transfer user funds without using a delegate" => {
                Some(Self::CpiGuardTransferBlocked)
            }
            "CPI Guard is enabled, and a program attempted to burn user funds without using a delegate" => {
                Some(Self::CpiGuardBurnBlocked)
            }
            "CPI Guard is enabled, and a program attempted to close an account without returning lamports to owner" => {
                Some(Self::CpiGuardCloseAccountBlocked)
            }
            "CPI Guard is enabled, and a program attempted to approve a delegate" => {
                Some(Self::CpiGuardApproveBlocked)
            }
            "CPI Guard is enabled, and a program attempted to add or change an authority" => {
                Some(Self::CpiGuardSetAuthorityBlocked)
            }
            "Account ownership cannot be changed while CPI Guard is enabled" => {
                Some(Self::CpiGuardOwnerChangeBlocked)
            }
            "Extension not found in account data" => Some(Self::ExtensionNotFound),
            "Non-confidential transfers disabled" => Some(Self::NonConfidentialTransfersDisabled),
            "Account has non-zero confidential withheld fee" => {
                Some(Self::ConfidentialTransferFeeAccountHasWithheldFee)
            }
            "Mint or account is initialized to an invalid combination of extensions" => {
                Some(Self::InvalidExtensionCombination)
            }
            "Extension allocation with overwrite must use the same length" => {
                Some(Self::InvalidLengthForAlloc)
            }
            "Failed to decrypt a confidential transfer account" => Some(Self::AccountDecryption),
            "Failed to generate proof" => Some(Self::ProofGeneration),
            "An invalid proof instruction offset was provided" => {
                Some(Self::InvalidProofInstructionOffset)
            }
            "Harvest of withheld tokens to mint is disabled" => Some(Self::HarvestToMintDisabled),
            "Split proof context state accounts not supported for instruction" => {
                Some(Self::SplitProofContextStateAccountsNotSupported)
            }
            "Not enough proof context state accounts provided" => {
                Some(Self::NotEnoughProofContextStateAccounts)
            }
            "Ciphertext is malformed" => Some(Self::MalformedCiphertext),
            "Ciphertext arithmetic failed" => Some(Self::CiphertextArithmeticFailed),
            "Pedersen commitments did not match" => Some(Self::PedersenCommitmentMismatch),
            "Range proof lengths did not match" => Some(Self::RangeProofLengthMismatch),
            "Illegal transfer amount bit length" => Some(Self::IllegalBitLength),
            "Transfer fee calculation failed" => Some(Self::FeeCalculation),
            "Conversions from normal to confidential token balance and vice versa are illegal if the confidential-mint-burn extension is enabled" => {
                Some(Self::IllegalMintBurnConversion)
            }
            "Invalid scale for scaled ui amount" => Some(Self::InvalidScale),
            "Transferring, minting, and burning is paused on this mint" => Some(Self::MintPaused),
            "Key rotation attempted while pending balance is not zero" => {
                Some(Self::PendingBalanceNonZero)
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

            Self::ExtensionTypeMismatch => {
                "Error: New extension type does not match already existing extensions"
            }
            Self::ExtensionBaseMismatch => "Error: Extension does not match the base type provided",
            Self::ExtensionAlreadyInitialized => {
                "Error: Extension already initialized on this account"
            }
            Self::ConfidentialTransferAccountHasBalance => {
                "Error: An account can only be closed if its confidential balance is zero"
            }
            Self::ConfidentialTransferAccountNotApproved => {
                "Error: Account not approved for confidential transfers"
            }
            Self::ConfidentialTransferDepositsAndTransfersDisabled => {
                "Error: Account not accepting deposits or transfers"
            }
            Self::ConfidentialTransferElGamalPubkeyMismatch => "Error: ElGamal public key mismatch",
            Self::ConfidentialTransferBalanceMismatch => "Error: Balance mismatch",
            Self::MintHasSupply => {
                "Error: Mint has non-zero supply. Burn all tokens before closing the mint"
            }
            Self::NoAuthorityExists => {
                "Error: No authority exists to perform the desired operation"
            }
            Self::TransferFeeExceedsMaximum => {
                "Error: Transfer fee exceeds maximum of 10,000 basis points"
            }
            Self::MintRequiredForTransfer => {
                "Mint required for this account to transfer tokens, use `transfer_checked` or `transfer_checked_with_fee`"
            }
            Self::FeeMismatch => "Calculated fee does not match expected fee",
            Self::FeeParametersMismatch => {
                "Fee parameters associated with zero-knowledge proofs do not match fee parameters in mint"
            }
            Self::ImmutableOwner => "The owner authority cannot be changed",
            Self::AccountHasWithheldTransferFees => {
                "Error: An account can only be closed if its withheld fee balance is zero, harvest fees to the mint and try again"
            }
            Self::NoMemo => {
                "Error: No memo in previous instruction required for recipient to receive a transfer"
            }
            Self::NonTransferable => "Transfer is disabled for this mint",
            Self::NonTransferableNeedsImmutableOwnership => {
                "Non-transferable tokens can't be minted to an account without immutable ownership"
            }
            Self::MaximumPendingBalanceCreditCounterExceeded => {
                "The total number of `Deposit` and `Transfer` instructions to an account cannot exceed the associated `maximum_pending_balance_credit_counter`"
            }
            Self::MaximumDepositAmountExceeded => "Deposit amount exceeds maximum limit",
            Self::CpiGuardSettingsLocked => "CPI Guard status cannot be changed in CPI",
            Self::CpiGuardTransferBlocked => {
                "CPI Guard is enabled, and a program attempted to transfer user funds without using a delegate"
            }
            Self::CpiGuardBurnBlocked => {
                "CPI Guard is enabled, and a program attempted to burn user funds without using a delegate"
            }
            Self::CpiGuardCloseAccountBlocked => {
                "CPI Guard is enabled, and a program attempted to close an account without returning lamports to owner"
            }
            Self::CpiGuardApproveBlocked => {
                "CPI Guard is enabled, and a program attempted to approve a delegate"
            }
            Self::CpiGuardSetAuthorityBlocked => {
                "CPI Guard is enabled, and a program attempted to add or change an authority"
            }
            Self::CpiGuardOwnerChangeBlocked => {
                "Account ownership cannot be changed while CPI Guard is enabled"
            }
            Self::ExtensionNotFound => "Extension not found in account data",
            Self::NonConfidentialTransfersDisabled => "Non-confidential transfers disabled",
            Self::ConfidentialTransferFeeAccountHasWithheldFee => {
                "Account has non-zero confidential withheld fee"
            }
            Self::InvalidExtensionCombination => {
                "Mint or account is initialized to an invalid combination of extensions"
            }
            Self::InvalidLengthForAlloc => {
                "Extension allocation with overwrite must use the same length"
            }
            Self::AccountDecryption => "Failed to decrypt a confidential transfer account",
            Self::ProofGeneration => "Failed to generate proof",
            Self::InvalidProofInstructionOffset => {
                "An invalid proof instruction offset was provided"
            }
            Self::HarvestToMintDisabled => "Harvest of withheld tokens to mint is disabled",
            Self::SplitProofContextStateAccountsNotSupported => {
                "Split proof context state accounts not supported for instruction"
            }
            Self::NotEnoughProofContextStateAccounts => {
                "Not enough proof context state accounts provided"
            }
            Self::MalformedCiphertext => "Ciphertext is malformed",
            Self::CiphertextArithmeticFailed => "Ciphertext arithmetic failed",
            Self::PedersenCommitmentMismatch => "Pedersen commitments did not match",
            Self::RangeProofLengthMismatch => "Range proof lengths did not match",
            Self::IllegalBitLength => "Illegal transfer amount bit length",
            Self::FeeCalculation => "Transfer fee calculation failed",
            Self::IllegalMintBurnConversion => {
                "Conversions from normal to confidential token balance and vice versa are illegal if the confidential-mint-burn extension is enabled"
            }
            Self::InvalidScale => "Invalid scale for scaled ui amount",
            Self::MintPaused => "Transferring, minting, and burning is paused on this mint",
            Self::PendingBalanceNonZero => {
                "Key rotation attempted while pending balance is not zero"
            }
        }
    }
}

impl Token2022Log {
    #[inline]
    pub fn parse(payload: &str, registry: &Registry, st: &mut StringTable) -> Option<Self> {
        if let Some(e) = Token2022ErrorLog::parse(payload) {
            return Some(Self::Error(e));
        }

        // "account needs resize, +{:?} bytes"
        // In practice the {:?} for usize prints a plain integer.
        if let Some(x) = parse_one_braced(payload, "account needs resize, +", " bytes") {
            if let Ok(bytes) = x.parse::<usize>() {
                return Some(Self::AccountNeedsResizePlusBytesDebug { bytes });
            }
        }

        // NOTE: you had a second enum variant for another site, but the log string is the same.
        // If you later want to distinguish these, you need an additional discriminator in the log line.
        if let Some(x) = parse_one_braced(payload, "account needs resize, +", " bytes") {
            if let Ok(bytes) = x.parse::<usize>() {
                // If you want to prefer the other variant instead, swap which one you return here.
                // For now we keep Debug as the canonical one, and Debug2 remains for future use.
                let _ = bytes;
            }
        }

        // "Error harvesting from {}: {}"
        if let Some((a, b)) = parse_two_braced(payload, "Error harvesting from ", ": ") {
            let account_key = lookup_pubkey_id_or_none(registry, a)?;
            return Some(Self::ErrorHarvestingFrom {
                account_key,
                error: st.push(b),
            });
        }

        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable, registry: &Registry) -> String {
        match self {
            Self::Error(e) => e.as_str().to_string(),

            Self::AccountNeedsResizePlusBytesDebug { bytes } => {
                format!("account needs resize, +{:?} bytes", bytes)
            }
            Self::AccountNeedsResizePlusBytesDebug2 { bytes } => {
                format!("account needs resize, +{:?} bytes", bytes)
            }

            Self::ErrorHarvestingFrom { account_key, error }
            | Self::ErrorHarvestingFrom2 { account_key, error }
            | Self::ErrorHarvestingFrom3 { account_key, error }
            | Self::ErrorHarvestingFrom4 { account_key, error } => format!(
                "Error harvesting from {}: {}",
                pubkey_id_to_string(registry, *account_key),
                st.resolve(*error),
            ),
        }
    }
}

#[inline]
fn lookup_pubkey_id_or_none(registry: &Registry, pk_txt: &str) -> Option<PubkeyId> {
    let pk = Pubkey::from_str(pk_txt.trim()).ok()?;
    // Registry lookup returns a 0-based index; we store ids as 1-based (same convention as your log.rs).
    let ix0 = registry.lookup(&pk.to_bytes())?;
    Some(ix0 + 1)
}

#[inline]
fn pubkey_id_to_string(registry: &Registry, id: PubkeyId) -> String {
    // Id=0 reserved/invalid.
    if id == 0 {
        return "<invalid-pubkey-id-0>".to_string();
    }
    let ix = (id - 1) as usize;
    let bytes = match registry.keys.get(ix) {
        Some(b) => b,
        None => return format!("<pubkey-id-oob:{}>", id),
    };
    Pubkey::new_from_array(*bytes).to_string()
}

#[inline]
fn parse_one_braced<'a>(text: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let rest = text.strip_prefix(prefix)?;
    let inner = rest.strip_suffix(suffix)?;
    Some(inner.trim())
}

#[inline]
fn parse_two_braced<'a>(text: &'a str, prefix: &str, mid: &str) -> Option<(&'a str, &'a str)> {
    let rest = text.strip_prefix(prefix)?;
    let (a, b) = rest.split_once(mid)?;
    Some((a.trim(), b.trim()))
}
