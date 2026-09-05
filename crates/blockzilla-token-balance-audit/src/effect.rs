//! Archive-independent decoding of the first public-balance replay effects.
//!
//! This module deliberately supports only the non-fee core SPL Token
//! instructions whose public amount effect is fixed by their instruction
//! bytes and ordered account list.  A known instruction is not automatically
//! a supported replay effect.  Token-2022 extensions, native-token amount
//! synchronization, and batches return a stable, explicit reason instead of
//! being treated as no-ops.

use core::fmt;

use crate::{
    CommitStatus, DecodeStatus, InstructionCoordinate, TokenProgram, TopLevelInstruction,
    decode_token_instruction,
};

/// A registry-independent Solana public key.
pub type Pubkey = [u8; 32];

/// Dense caller-assigned index of one account tracked by the target reducer.
pub type TargetAccountIndex = u32;

/// One resolved instruction account in its exact instruction-account order.
///
/// The scanner owns the public-key-to-target-index map.  The reducer verifies
/// every supplied index against its configured account address before use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedInstructionAccount {
    pub pubkey: Pubkey,
    pub target_index: Option<TargetAccountIndex>,
}

impl ResolvedInstructionAccount {
    #[must_use]
    pub const fn other(pubkey: Pubkey) -> Self {
        Self {
            pubkey,
            target_index: None,
        }
    }

    #[must_use]
    pub const fn target(pubkey: Pubkey, target_index: TargetAccountIndex) -> Self {
        Self {
            pubkey,
            target_index: Some(target_index),
        }
    }
}

/// One token instruction after program and account resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedTokenInstruction<'a> {
    pub coordinate: InstructionCoordinate,
    pub program: TokenProgram,
    pub data: &'a [u8],
    pub accounts: &'a [ResolvedInstructionAccount],
    pub commit_status: CommitStatus,
}

/// A core effect whose public amount semantics are complete without account
/// data or an extension processor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CorePublicEffect {
    /// A proven core instruction that does not change the tracked public
    /// amount, account owner, or account lifecycle.
    NoPublicBalanceEffect,
    InitializeMint {
        mint: ResolvedInstructionAccount,
        decimals: u8,
    },
    InitializeAccount {
        account: ResolvedInstructionAccount,
        mint: Pubkey,
        owner: Pubkey,
    },
    Transfer {
        source: ResolvedInstructionAccount,
        destination: ResolvedInstructionAccount,
        amount: u64,
        /// Present for `TransferChecked`.
        checked_mint: Option<Pubkey>,
        decimals: Option<u8>,
    },
    /// Token-2022 `TransferCheckedWithFee`.
    ///
    /// A committed invocation proves that the Token-2022 processor accepted
    /// `expected_fee`.  The fee stays withheld in extension state and is not
    /// part of the destination account's public raw amount.
    TransferCheckedWithFee {
        source: ResolvedInstructionAccount,
        mint: Pubkey,
        destination: ResolvedInstructionAccount,
        amount: u64,
        decimals: u8,
        expected_fee: u64,
    },
    MintTo {
        mint: Pubkey,
        destination: ResolvedInstructionAccount,
        amount: u64,
        decimals: Option<u8>,
    },
    Burn {
        source: ResolvedInstructionAccount,
        mint: Pubkey,
        amount: u64,
        decimals: Option<u8>,
    },
    CloseAccount {
        account: ResolvedInstructionAccount,
    },
    SetAccountOwner {
        account: ResolvedInstructionAccount,
        new_owner: Pubkey,
    },
}

/// Why a potentially relevant instruction has no supported exact effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedEffectReason {
    DecodeNotKnown(DecodeStatus),
    MissingInstructionAccounts {
        instruction: TopLevelInstruction,
        minimum: usize,
        actual: usize,
    },
    InvalidAuthorityType(u8),
    MissingNewAccountOwner,
    Token2022InterfaceInstruction,
    Token2022ExtensionInstruction,
    Token2022ConfigurationInstruction(TopLevelInstruction),
    TransferFeeEffect,
    ConfidentialPublicBalanceEffect,
    PermissionedBurnEffect,
    NativeBalanceEffect(TopLevelInstruction),
    BatchEffect,
}

impl UnsupportedEffectReason {
    /// Stable report code.  This is separate from the human-readable text so
    /// a report consumer does not need to parse an error string.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::DecodeNotKnown(_) => "decode_not_known",
            Self::MissingInstructionAccounts { .. } => "missing_instruction_accounts",
            Self::InvalidAuthorityType(_) => "invalid_authority_type",
            Self::MissingNewAccountOwner => "missing_new_account_owner",
            Self::Token2022InterfaceInstruction => "token_2022_interface_instruction",
            Self::Token2022ExtensionInstruction => "token_2022_extension_instruction",
            Self::Token2022ConfigurationInstruction(_) => "token_2022_configuration_instruction",
            Self::TransferFeeEffect => "transfer_fee_effect",
            Self::ConfidentialPublicBalanceEffect => "confidential_public_balance_effect",
            Self::PermissionedBurnEffect => "permissioned_burn_effect",
            Self::NativeBalanceEffect(_) => "native_balance_effect",
            Self::BatchEffect => "batch_effect",
        }
    }
}

impl fmt::Display for UnsupportedEffectReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DecodeNotKnown(status) => {
                write!(formatter, "instruction decode is not known: {status:?}")
            }
            Self::MissingInstructionAccounts {
                instruction,
                minimum,
                actual,
            } => write!(
                formatter,
                "{} has {actual} accounts; at least {minimum} are required",
                instruction.name()
            ),
            Self::InvalidAuthorityType(value) => {
                write!(
                    formatter,
                    "SetAuthority has unsupported authority type {value}"
                )
            }
            Self::MissingNewAccountOwner => {
                formatter.write_str("SetAuthority(AccountOwner) has no new owner")
            }
            Self::Token2022InterfaceInstruction => {
                formatter.write_str("Token-2022 interface instruction is not modeled")
            }
            Self::Token2022ExtensionInstruction => {
                formatter.write_str("Token-2022 extension instruction is not modeled")
            }
            Self::Token2022ConfigurationInstruction(instruction) => write!(
                formatter,
                "Token-2022 configuration instruction {} is not modeled",
                instruction.name()
            ),
            Self::TransferFeeEffect => {
                formatter.write_str("Token-2022 transfer-fee effect is not modeled")
            }
            Self::ConfidentialPublicBalanceEffect => {
                formatter.write_str("Token-2022 confidential public-balance effect is not modeled")
            }
            Self::PermissionedBurnEffect => {
                formatter.write_str("Token-2022 permissioned burn is not modeled")
            }
            Self::NativeBalanceEffect(instruction) => write!(
                formatter,
                "native-token effect {} needs lamport state",
                instruction.name()
            ),
            Self::BatchEffect => formatter.write_str("Token-2022 Batch is not modeled"),
        }
    }
}

impl std::error::Error for UnsupportedEffectReason {}

/// Decode one core non-fee public-balance effect.
///
/// The caller should call this only for a committed instruction that can touch
/// its target mint or one of its target account addresses.  Rolled-back calls
/// require no effect decoding.
pub fn decode_core_public_effect(
    instruction: &ResolvedTokenInstruction<'_>,
) -> Result<CorePublicEffect, UnsupportedEffectReason> {
    let decoded = decode_token_instruction(instruction.program, instruction.data);
    if decoded.status != DecodeStatus::Known {
        return Err(UnsupportedEffectReason::DecodeNotKnown(decoded.status));
    }
    if decoded.interface.is_some() {
        // Token metadata and token-group interface calls can change mint-side
        // descriptive state.  They cannot change a public token-account raw
        // amount, lifecycle, or owner, which are the only reducer state here.
        return Ok(CorePublicEffect::NoPublicBalanceEffect);
    }
    if let Some(extension) = decoded.extension {
        return match (extension.family, extension.subtype) {
            (crate::ExtensionFamily::TransferFee, 4) => {
                // HarvestWithheldTokensToMint moves withheld amounts from
                // token-account extensions into the mint extension.  It does
                // not read or write a base token-account amount, lifecycle,
                // owner, or the mint's base supply.
                Ok(CorePublicEffect::NoPublicBalanceEffect)
            }
            (crate::ExtensionFamily::TransferFee, 1) => {
                let top_level = TopLevelInstruction::TransferFeeExtension;
                Ok(CorePublicEffect::TransferCheckedWithFee {
                    source: account(instruction, top_level, 0, 4)?,
                    mint: account(instruction, top_level, 1, 4)?.pubkey,
                    destination: account(instruction, top_level, 2, 4)?,
                    amount: decoded
                        .amount
                        .expect("known fee-transfer framing has amount"),
                    decimals: decoded
                        .decimals
                        .expect("known fee-transfer framing has decimals"),
                    expected_fee: read_u64(instruction.data, 11)
                        .expect("known fee-transfer framing has expected fee"),
                })
            }
            (crate::ExtensionFamily::TransferFee, _) => {
                Err(UnsupportedEffectReason::TransferFeeEffect)
            }
            (crate::ExtensionFamily::ConfidentialTransfer, 5 | 6) => {
                Err(UnsupportedEffectReason::ConfidentialPublicBalanceEffect)
            }
            (crate::ExtensionFamily::PermissionedBurn, 1 | 2) => {
                Err(UnsupportedEffectReason::PermissionedBurnEffect)
            }
            // These calls only change configuration, UI state, or hidden
            // balances.  Any token instruction that they invoke is present as
            // its own inner invocation.  They have no direct effect on the
            // public raw token-account amount, lifecycle, or owner modeled by
            // this reducer.
            _ => Ok(CorePublicEffect::NoPublicBalanceEffect),
        };
    }

    let top_level = decoded
        .top_level
        .ok_or(UnsupportedEffectReason::DecodeNotKnown(decoded.status))?;
    use TopLevelInstruction as I;

    match top_level {
        I::InitializeMint => Ok(CorePublicEffect::InitializeMint {
            mint: account(instruction, top_level, 0, 2)?,
            decimals: decoded.decimals.expect("known mint framing has decimals"),
        }),
        I::InitializeMint2 => Ok(CorePublicEffect::InitializeMint {
            mint: account(instruction, top_level, 0, 1)?,
            decimals: decoded.decimals.expect("known mint framing has decimals"),
        }),
        I::InitializeAccount => Ok(CorePublicEffect::InitializeAccount {
            account: account(instruction, top_level, 0, 4)?,
            mint: account(instruction, top_level, 1, 4)?.pubkey,
            owner: account(instruction, top_level, 2, 4)?.pubkey,
        }),
        I::InitializeAccount2 => Ok(CorePublicEffect::InitializeAccount {
            account: account(instruction, top_level, 0, 3)?,
            mint: account(instruction, top_level, 1, 3)?.pubkey,
            owner: read_pubkey(instruction.data, 1)
                .expect("known InitializeAccount2 framing has owner"),
        }),
        I::InitializeAccount3 => Ok(CorePublicEffect::InitializeAccount {
            account: account(instruction, top_level, 0, 2)?,
            mint: account(instruction, top_level, 1, 2)?.pubkey,
            owner: read_pubkey(instruction.data, 1)
                .expect("known InitializeAccount3 framing has owner"),
        }),
        I::Transfer => Ok(CorePublicEffect::Transfer {
            source: account(instruction, top_level, 0, 3)?,
            destination: account(instruction, top_level, 1, 3)?,
            amount: decoded.amount.expect("known transfer framing has amount"),
            checked_mint: None,
            decimals: None,
        }),
        I::TransferChecked => Ok(CorePublicEffect::Transfer {
            source: account(instruction, top_level, 0, 4)?,
            destination: account(instruction, top_level, 2, 4)?,
            amount: decoded
                .amount
                .expect("known checked-transfer framing has amount"),
            checked_mint: Some(account(instruction, top_level, 1, 4)?.pubkey),
            decimals: decoded.decimals,
        }),
        I::MintTo | I::MintToChecked => Ok(CorePublicEffect::MintTo {
            mint: account(instruction, top_level, 0, 3)?.pubkey,
            destination: account(instruction, top_level, 1, 3)?,
            amount: decoded.amount.expect("known mint-to framing has amount"),
            decimals: decoded.decimals,
        }),
        I::Burn | I::BurnChecked => Ok(CorePublicEffect::Burn {
            source: account(instruction, top_level, 0, 3)?,
            mint: account(instruction, top_level, 1, 3)?.pubkey,
            amount: decoded.amount.expect("known burn framing has amount"),
            decimals: decoded.decimals,
        }),
        I::CloseAccount => Ok(CorePublicEffect::CloseAccount {
            account: account(instruction, top_level, 0, 3)?,
        }),
        I::SetAuthority => decode_set_authority(instruction, top_level),
        I::SyncNative | I::UnwrapLamports => {
            Err(UnsupportedEffectReason::NativeBalanceEffect(top_level))
        }
        I::Batch => Err(UnsupportedEffectReason::BatchEffect),
        I::CreateNativeMint => Err(UnsupportedEffectReason::NativeBalanceEffect(top_level)),
        I::InitializeMintCloseAuthority
        | I::Reallocate
        | I::InitializeNonTransferableMint
        | I::InitializePermanentDelegate
        | I::WithdrawExcessLamports => Ok(CorePublicEffect::NoPublicBalanceEffect),
        I::TransferFeeExtension
        | I::ConfidentialTransferExtension
        | I::DefaultAccountStateExtension
        | I::MemoTransferExtension
        | I::InterestBearingMintExtension
        | I::CpiGuardExtension
        | I::TransferHookExtension
        | I::ConfidentialTransferFeeExtension
        | I::MetadataPointerExtension
        | I::GroupPointerExtension
        | I::GroupMemberPointerExtension
        | I::ConfidentialMintBurnExtension
        | I::ScaledUiAmountExtension
        | I::PausableExtension
        | I::PermissionedBurnExtension => {
            unreachable!("known extension prefixes return before core dispatch")
        }
        I::InitializeMultisig
        | I::Approve
        | I::Revoke
        | I::FreezeAccount
        | I::ThawAccount
        | I::ApproveChecked
        | I::InitializeMultisig2
        | I::GetAccountDataSize
        | I::InitializeImmutableOwner
        | I::AmountToUiAmount
        | I::UiAmountToAmount => Ok(CorePublicEffect::NoPublicBalanceEffect),
    }
}

fn decode_set_authority(
    instruction: &ResolvedTokenInstruction<'_>,
    top_level: TopLevelInstruction,
) -> Result<CorePublicEffect, UnsupportedEffectReason> {
    let authority_type = instruction.data[1];
    let maximum = match instruction.program {
        TokenProgram::Legacy => 3,
        TokenProgram::Token2022 => 17,
    };
    if authority_type > maximum {
        return Err(UnsupportedEffectReason::InvalidAuthorityType(
            authority_type,
        ));
    }
    if authority_type != 2 {
        return Ok(CorePublicEffect::NoPublicBalanceEffect);
    }

    let new_owner = match instruction.data.get(2).copied() {
        Some(1) => read_pubkey(instruction.data, 3)
            .ok_or(UnsupportedEffectReason::MissingNewAccountOwner)?,
        Some(0) => return Err(UnsupportedEffectReason::MissingNewAccountOwner),
        _ => return Err(UnsupportedEffectReason::MissingNewAccountOwner),
    };
    Ok(CorePublicEffect::SetAccountOwner {
        account: account(instruction, top_level, 0, 2)?,
        new_owner,
    })
}

fn account(
    instruction: &ResolvedTokenInstruction<'_>,
    top_level: TopLevelInstruction,
    index: usize,
    minimum: usize,
) -> Result<ResolvedInstructionAccount, UnsupportedEffectReason> {
    instruction.accounts.get(index).copied().ok_or(
        UnsupportedEffectReason::MissingInstructionAccounts {
            instruction: top_level,
            minimum,
            actual: instruction.accounts.len(),
        },
    )
}

fn read_pubkey(data: &[u8], offset: usize) -> Option<Pubkey> {
    data.get(offset..offset + 32)?.try_into().ok()
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROGRAM: TokenProgram = TokenProgram::Token2022;

    fn key(value: u8) -> Pubkey {
        [value; 32]
    }

    fn committed<'a>(
        data: &'a [u8],
        accounts: &'a [ResolvedInstructionAccount],
    ) -> ResolvedTokenInstruction<'a> {
        ResolvedTokenInstruction {
            coordinate: InstructionCoordinate::outer(0),
            program: PROGRAM,
            data,
            accounts,
            commit_status: CommitStatus::Committed,
        }
    }

    #[test]
    fn decodes_checked_transfer_roles_without_allocating() {
        let mut data = [0_u8; 10];
        data[0] = TopLevelInstruction::TransferChecked.tag();
        data[1..9].copy_from_slice(&42_u64.to_le_bytes());
        data[9] = 8;
        let accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(9)),
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::other(key(3)),
        ];

        assert_eq!(
            decode_core_public_effect(&committed(&data, &accounts)),
            Ok(CorePublicEffect::Transfer {
                source: accounts[0],
                destination: accounts[2],
                amount: 42,
                checked_mint: Some(key(9)),
                decimals: Some(8),
            })
        );
    }

    #[test]
    fn decodes_transfer_checked_with_fee_fields_and_roles() {
        let mut data = [0_u8; 19];
        data[0] = TopLevelInstruction::TransferFeeExtension.tag();
        data[1] = 1;
        data[2..10].copy_from_slice(&42_u64.to_le_bytes());
        data[10] = 8;
        data[11..19].copy_from_slice(&3_u64.to_le_bytes());
        let accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(9)),
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::other(key(3)),
        ];

        assert_eq!(
            decode_core_public_effect(&committed(&data, &accounts)),
            Ok(CorePublicEffect::TransferCheckedWithFee {
                source: accounts[0],
                mint: key(9),
                destination: accounts[2],
                amount: 42,
                decimals: 8,
                expected_fee: 3,
            })
        );
    }

    #[test]
    fn account_owner_is_decoded_from_instruction_data() {
        let mut data = [0_u8; 35];
        data[0] = TopLevelInstruction::SetAuthority.tag();
        data[1] = 2;
        data[2] = 1;
        data[3..35].copy_from_slice(&key(7));
        let accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(2)),
        ];

        assert_eq!(
            decode_core_public_effect(&committed(&data, &accounts)),
            Ok(CorePublicEffect::SetAccountOwner {
                account: accounts[0],
                new_owner: key(7),
            })
        );
    }

    #[test]
    fn fee_and_native_effects_fail_closed_with_stable_codes() {
        let accounts = [ResolvedInstructionAccount::target(key(1), 0)];
        for fee_data in [
            vec![TopLevelInstruction::TransferFeeExtension.tag(), 0],
            vec![TopLevelInstruction::TransferFeeExtension.tag(), 2],
            vec![TopLevelInstruction::TransferFeeExtension.tag(), 3, 0],
            vec![
                TopLevelInstruction::TransferFeeExtension.tag(),
                5,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
        ] {
            let fee = decode_core_public_effect(&committed(&fee_data, &accounts)).unwrap_err();
            assert_eq!(fee, UnsupportedEffectReason::TransferFeeEffect);
            assert_eq!(fee.code(), "transfer_fee_effect");
        }

        let native_data = [TopLevelInstruction::SyncNative.tag()];
        let native = decode_core_public_effect(&committed(&native_data, &accounts)).unwrap_err();
        assert_eq!(
            native,
            UnsupportedEffectReason::NativeBalanceEffect(TopLevelInstruction::SyncNative)
        );
        assert_eq!(native.code(), "native_balance_effect");
    }

    #[test]
    fn harvest_withheld_tokens_to_mint_is_a_public_raw_no_op() {
        let data = [TopLevelInstruction::TransferFeeExtension.tag(), 4];
        let accounts = [
            ResolvedInstructionAccount::other(key(8)),
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::target(key(2), 1),
        ];

        assert_eq!(
            decode_core_public_effect(&committed(&data, &accounts)),
            Ok(CorePublicEffect::NoPublicBalanceEffect)
        );
    }

    #[test]
    fn interface_and_hidden_only_extension_are_public_raw_no_ops() {
        let accounts = [ResolvedInstructionAccount::other(key(9))];
        let metadata_update = [0xdd, 0xe9, 0x31, 0x2d, 0xb5, 0xca, 0xdc, 0xc8];
        assert_eq!(
            decode_core_public_effect(&committed(&metadata_update, &accounts)),
            Ok(CorePublicEffect::NoPublicBalanceEffect)
        );

        let confidential_transfer = [TopLevelInstruction::ConfidentialTransferExtension.tag(), 7];
        assert_eq!(
            decode_core_public_effect(&committed(&confidential_transfer, &accounts)),
            Ok(CorePublicEffect::NoPublicBalanceEffect)
        );
    }

    #[test]
    fn missing_role_is_not_silently_accepted() {
        let mut data = [0_u8; 9];
        data[0] = TopLevelInstruction::Transfer.tag();
        data[1..].copy_from_slice(&1_u64.to_le_bytes());
        let accounts = [ResolvedInstructionAccount::target(key(1), 0)];

        assert_eq!(
            decode_core_public_effect(&committed(&data, &accounts)),
            Err(UnsupportedEffectReason::MissingInstructionAccounts {
                instruction: TopLevelInstruction::Transfer,
                minimum: 3,
                actual: 1,
            })
        );
    }
}
