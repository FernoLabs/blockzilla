//! Launch-era native System-program account primitives.
//!
//! This module follows Solana's launch-era native System processors for the
//! variants present in the historical replay. Durable nonce initialization,
//! authorization, advancement, and withdrawal are implemented against the
//! instruction-provided launch sysvar accounts. Stable switches from the
//! legacy v1.0 processor to v1.2.32's replacement on entry to epoch 40.

use std::collections::BTreeSet;

use blockzilla_format::ArchiveV2SystemInstructionData;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AccountMap, AccountSnapshot, CowAccountMap, RECENT_BLOCKHASHES_SYSVAR_ID, RENT_SYSVAR_ID,
};

pub const SYSTEM_PROGRAM_ID: [u8; 32] = [0; 32];
const SYSVAR_OWNER_ID: [u8; 32] = [
    6, 167, 213, 23, 24, 117, 247, 41, 199, 61, 147, 64, 143, 33, 97, 32, 6, 126, 216, 140, 118,
    224, 140, 40, 127, 193, 148, 96, 0, 0, 0, 0,
];
pub const MAX_ADDRESS_SEED_LEN: usize = 32;
pub const MAX_PERMITTED_DATA_LENGTH: u64 = 10 * 1024 * 1024;
pub const LAUNCH_NONCE_ACCOUNT_DATA_LEN: usize = 80;
/// Solana v1.2.32 replaces the legacy Stable System processor on entry to
/// epoch 40. Among its consensus-visible changes, positive transfers to the
/// same account become supported no-ops instead of RefCell borrow failures,
/// and `CreateAccount` rejects destinations that already hold lamports.
pub const STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH: u64 = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaunchAccountMeta {
    pub pubkey: [u8; 32],
    pub is_signer: bool,
    pub is_writable: bool,
}

/// Compatibility alias retained for callers of the original System-only POC.
pub type LaunchSystemAccountMeta = LaunchAccountMeta;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchSystemMutation {
    CreateAccount {
        from: [u8; 32],
        to: [u8; 32],
        lamports: u64,
        space: u64,
        owner: [u8; 32],
        seeded: bool,
    },
    Assign {
        account: [u8; 32],
        owner: [u8; 32],
        seeded: bool,
    },
    Transfer {
        from: [u8; 32],
        to: [u8; 32],
        lamports: u64,
    },
    Allocate {
        account: [u8; 32],
        space: u64,
        owner: [u8; 32],
        seeded: bool,
    },
    AuthorizeNonce {
        account: [u8; 32],
        old_authority: [u8; 32],
        new_authority: [u8; 32],
    },
    InitializeNonce {
        account: [u8; 32],
        authority: [u8; 32],
        blockhash: [u8; 32],
        lamports_per_signature: u64,
    },
    AdvanceNonce {
        account: [u8; 32],
        authority: [u8; 32],
        old_blockhash: [u8; 32],
        new_blockhash: [u8; 32],
        lamports_per_signature: u64,
    },
    WithdrawNonce {
        account: [u8; 32],
        destination: [u8; 32],
        signer: [u8; 32],
        lamports: u64,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LaunchSystemError {
    #[error("system instruction is missing account position {position}")]
    MissingAccount { position: usize },
    #[error("account {pubkey:?} must sign the system instruction")]
    MissingRequiredSignature { pubkey: [u8; 32] },
    #[error("system account {pubkey:?} is already in use")]
    AccountAlreadyInUse { pubkey: [u8; 32] },
    #[error("requested account data length {space} exceeds the launch limit")]
    InvalidAccountDataLength { space: u64 },
    #[error("cannot assign a System account to launch sysvar id {owner:?}")]
    InvalidProgramId { owner: [u8; 32] },
    #[error("derived-address seed is {length} bytes; launch maximum is 32")]
    MaxSeedLengthExceeded { length: usize },
    #[error("derived address mismatch: supplied {supplied:?}, derived {derived:?}")]
    AddressWithSeedMismatch {
        supplied: [u8; 32],
        derived: [u8; 32],
    },
    #[error("source account {pubkey:?} carries data")]
    SourceCarriesData { pubkey: [u8; 32] },
    #[error("System program cannot spend lamports from externally owned account {pubkey:?}")]
    ExternalAccountLamportSpend { pubkey: [u8; 32] },
    #[error("System program cannot change the owner of account {pubkey:?}")]
    ModifiedProgramId { pubkey: [u8; 32] },
    #[error("read-only account {pubkey:?} changed lamports")]
    ReadonlyLamportChange { pubkey: [u8; 32] },
    #[error("System program cannot resize account {pubkey:?}")]
    AccountDataSizeChanged { pubkey: [u8; 32] },
    #[error("read-only account {pubkey:?} changed data")]
    ReadonlyDataModified { pubkey: [u8; 32] },
    #[error("System program changed data in externally owned account {pubkey:?}")]
    ExternalAccountDataModified { pubkey: [u8; 32] },
    #[error("System program made an invalid executable change to account {pubkey:?}")]
    ExecutableModified { pubkey: [u8; 32] },
    #[error("System program changed rent_epoch on account {pubkey:?}")]
    RentEpochModified { pubkey: [u8; 32] },
    #[error("account {pubkey:?} does not contain {required} lamports")]
    ResultWithNegativeLamports { pubkey: [u8; 32], required: u64 },
    #[error("System instruction is unbalanced: pre={pre_lamports}, post={post_lamports}")]
    UnbalancedInstruction {
        pre_lamports: u128,
        post_lamports: u128,
    },
    #[error("aliased source/destination {pubkey:?} would fail the launch borrow rules")]
    AccountBorrowConflict { pubkey: [u8; 32] },
    #[error("system account {pubkey:?} contains invalid nonce data")]
    InvalidAccountData { pubkey: [u8; 32] },
    #[error("system account {pubkey:?} is not an initialized launch nonce account")]
    BadNonceAccountState { pubkey: [u8; 32] },
    #[error("system instruction account {position} is {found:?}, expected sysvar {expected:?}")]
    InvalidSysvar {
        position: usize,
        expected: [u8; 32],
        found: [u8; 32],
    },
    #[error("system instruction account {position} contains invalid sysvar data")]
    InvalidSysvarData { position: usize },
    #[error("launch RecentBlockhashes is empty")]
    NoRecentBlockhashes,
    #[error("nonce account {pubkey:?} blockhash {blockhash:?} has not expired")]
    NonceNotExpired {
        pubkey: [u8; 32],
        blockhash: [u8; 32],
    },
    #[error("nonce account {pubkey:?} is not rent exempt: balance={balance}, minimum={minimum}")]
    NonceInsufficientFunds {
        pubkey: [u8; 32],
        balance: u64,
        minimum: u64,
    },
    #[error("nonce account {pubkey:?} has {balance} lamports, but withdrawal requires {required}")]
    InsufficientFunds {
        pubkey: [u8; 32],
        balance: u64,
        required: u64,
    },
    #[error("serialized nonce state for {pubkey:?} needs {needed} bytes, account has {available}")]
    AccountDataTooSmall {
        pubkey: [u8; 32],
        needed: usize,
        available: usize,
    },
    #[error("could not serialize nonce state for system account {pubkey:?}")]
    GenericError { pubkey: [u8; 32] },
    #[error("launch durable nonce variant {variant} requires Bank sysvars")]
    NonceRequiresBankSysvars { variant: &'static str },
    #[error("system variant {variant} did not exist in the v1.0.7 launch profile")]
    PostLaunchVariant { variant: &'static str },
}

pub fn default_system_account() -> AccountSnapshot {
    AccountSnapshot {
        lamports: 0,
        owner: SYSTEM_PROGRAM_ID,
        executable: false,
        rent_epoch: 0,
        data: Vec::new().into(),
    }
}

/// Apply one launch System instruction atomically to the supplied transaction
/// overlay. Missing instruction accounts load as the historical default System
/// account. The caller commits the overlay only if the containing transaction
/// succeeds.
pub fn apply_launch_system_instruction(
    instruction: &ArchiveV2SystemInstructionData,
    account_metas: &[LaunchSystemAccountMeta],
    accounts: &mut AccountMap,
) -> Result<LaunchSystemMutation, LaunchSystemError> {
    apply_launch_system_instruction_for_epoch(instruction, account_metas, accounts, 0)
}

/// Apply the System processor selected by the historical Stable-cluster epoch.
///
/// This public API is instruction-atomic: on error the caller's `accounts` map
/// is unchanged. The replay hot path uses
/// [`apply_launch_system_instruction_for_epoch_in_place`] instead because the
/// transaction overlay is discarded on any instruction failure.
pub fn apply_launch_system_instruction_for_epoch(
    instruction: &ArchiveV2SystemInstructionData,
    account_metas: &[LaunchSystemAccountMeta],
    accounts: &mut AccountMap,
    epoch: u64,
) -> Result<LaunchSystemMutation, LaunchSystemError> {
    let mut working = CowAccountMap::detached(accounts.clone());
    let mutation = apply_launch_system_instruction_for_epoch_on_overlay(
        instruction,
        account_metas,
        &mut working,
        epoch,
    )?;
    *accounts = working.into_local();
    Ok(mutation)
}

/// Replay-only fast path. Mutates `accounts` in place.
///
/// On error the overlay may be partially mutated. Callers must discard it
/// (transaction-level rollback), matching the Vote in-place contract.
///
/// Prefer a [`CowAccountMap::layered`] overlay so readonly accounts stay in the
/// parent Bank store until a write forces a local clone.
pub fn apply_launch_system_instruction_for_epoch_in_place(
    instruction: &ArchiveV2SystemInstructionData,
    account_metas: &[LaunchSystemAccountMeta],
    accounts: &mut AccountMap,
    epoch: u64,
) -> Result<LaunchSystemMutation, LaunchSystemError> {
    let mut cow = CowAccountMap::detached(std::mem::take(accounts));
    let result = apply_launch_system_instruction_for_epoch_on_overlay(
        instruction,
        account_metas,
        &mut cow,
        epoch,
    );
    *accounts = cow.into_local();
    result
}

/// Replay hot path over a layered/local overlay.
pub fn apply_launch_system_instruction_for_epoch_on_overlay(
    instruction: &ArchiveV2SystemInstructionData,
    account_metas: &[LaunchSystemAccountMeta],
    accounts: &mut CowAccountMap,
    epoch: u64,
) -> Result<LaunchSystemMutation, LaunchSystemError> {
    // Writable accounts must be local before mutation. Readonly accounts are
    // resolved through the parent on demand.
    accounts.materialize_writable(
        account_metas
            .iter()
            .map(|meta| (meta.pubkey, meta.is_writable)),
        default_system_account,
    );
    // Absent keys (not in parent) still need a local default for creates.
    for meta in account_metas {
        if !accounts.contains_key(&meta.pubkey) {
            accounts.insert(meta.pubkey, default_system_account());
        }
    }
    let pre_accounts = launch_pre_accounts(account_metas, accounts);
    let mutation = apply_inner(
        instruction,
        account_metas,
        accounts,
        epoch >= STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
    )?;
    verify_launch_system_instruction(&pre_accounts, accounts)?;
    Ok(mutation)
}

pub fn create_address_with_seed(
    base: &[u8; 32],
    seed: &str,
    owner: &[u8; 32],
) -> Result<[u8; 32], LaunchSystemError> {
    if seed.len() > MAX_ADDRESS_SEED_LEN {
        return Err(LaunchSystemError::MaxSeedLengthExceeded { length: seed.len() });
    }
    let mut hasher = Sha256::new();
    hasher.update(base);
    hasher.update(seed.as_bytes());
    hasher.update(owner);
    Ok(hasher.finalize().into())
}

fn apply_inner(
    instruction: &ArchiveV2SystemInstructionData,
    account_metas: &[LaunchSystemAccountMeta],
    accounts: &mut CowAccountMap,
    new_system_processor: bool,
) -> Result<LaunchSystemMutation, LaunchSystemError> {
    let signers = account_metas
        .iter()
        .filter(|meta| meta.is_signer)
        .map(|meta| meta.pubkey)
        .collect::<BTreeSet<_>>();
    match instruction {
        ArchiveV2SystemInstructionData::CreateAccount {
            lamports,
            space,
            owner,
        } => {
            let from = required_meta(account_metas, 0)?;
            let to = required_meta(account_metas, 1)?;
            let address = Address::plain(to.pubkey);
            reject_prefunded_create_destination(accounts, to, new_system_processor)?;
            allocate_and_assign(accounts, to, &address, *space, owner, &signers)?;
            transfer(accounts, from, to, *lamports, new_system_processor)?;
            Ok(LaunchSystemMutation::CreateAccount {
                from: from.pubkey,
                to: to.pubkey,
                lamports: *lamports,
                space: *space,
                owner: *owner,
                seeded: false,
            })
        }
        ArchiveV2SystemInstructionData::CreateAccountWithSeed {
            base,
            seed,
            lamports,
            space,
            owner,
        } => {
            let from = required_meta(account_metas, 0)?;
            let to = required_meta(account_metas, 1)?;
            let address = Address::seeded(to.pubkey, *base, seed, *owner)?;
            reject_prefunded_create_destination(accounts, to, new_system_processor)?;
            allocate_and_assign(accounts, to, &address, *space, owner, &signers)?;
            transfer(accounts, from, to, *lamports, new_system_processor)?;
            Ok(LaunchSystemMutation::CreateAccount {
                from: from.pubkey,
                to: to.pubkey,
                lamports: *lamports,
                space: *space,
                owner: *owner,
                seeded: true,
            })
        }
        ArchiveV2SystemInstructionData::Assign { owner } => {
            let account = required_meta(account_metas, 0)?;
            assign(
                accounts,
                account,
                &Address::plain(account.pubkey),
                owner,
                &signers,
            )?;
            Ok(LaunchSystemMutation::Assign {
                account: account.pubkey,
                owner: *owner,
                seeded: false,
            })
        }
        ArchiveV2SystemInstructionData::Transfer { lamports } => {
            let from = required_meta(account_metas, 0)?;
            let to = required_meta(account_metas, 1)?;
            transfer(accounts, from, to, *lamports, new_system_processor)?;
            Ok(LaunchSystemMutation::Transfer {
                from: from.pubkey,
                to: to.pubkey,
                lamports: *lamports,
            })
        }
        ArchiveV2SystemInstructionData::Allocate { space } => {
            let account = required_meta(account_metas, 0)?;
            allocate(
                accounts,
                account,
                &Address::plain(account.pubkey),
                *space,
                &signers,
            )?;
            Ok(LaunchSystemMutation::Allocate {
                account: account.pubkey,
                space: *space,
                owner: SYSTEM_PROGRAM_ID,
                seeded: false,
            })
        }
        ArchiveV2SystemInstructionData::AllocateWithSeed {
            base,
            seed,
            space,
            owner,
        } => {
            let account = required_meta(account_metas, 0)?;
            let address = Address::seeded(account.pubkey, *base, seed, *owner)?;
            allocate_and_assign(accounts, account, &address, *space, owner, &signers)?;
            Ok(LaunchSystemMutation::Allocate {
                account: account.pubkey,
                space: *space,
                owner: *owner,
                seeded: true,
            })
        }
        ArchiveV2SystemInstructionData::AssignWithSeed { base, seed, owner } => {
            let account = required_meta(account_metas, 0)?;
            let address = Address::seeded(account.pubkey, *base, seed, *owner)?;
            assign(accounts, account, &address, owner, &signers)?;
            Ok(LaunchSystemMutation::Assign {
                account: account.pubkey,
                owner: *owner,
                seeded: true,
            })
        }
        ArchiveV2SystemInstructionData::AdvanceNonceAccount => {
            let nonce = required_meta(account_metas, 0)?;
            let recent = decode_recent_blockhashes(account_metas, accounts, 1)?;
            advance_nonce(accounts, nonce, &recent, &signers)
        }
        ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports } => {
            let nonce = required_meta(account_metas, 0)?;
            let destination = required_meta(account_metas, 1)?;
            let recent = decode_recent_blockhashes(account_metas, accounts, 2)?;
            let rent = decode_rent(account_metas, accounts, 3)?;
            withdraw_nonce(
                accounts,
                nonce,
                destination,
                *lamports,
                &recent,
                rent,
                &signers,
            )
        }
        ArchiveV2SystemInstructionData::InitializeNonceAccount { authority } => {
            let nonce = required_meta(account_metas, 0)?;
            let recent = decode_recent_blockhashes(account_metas, accounts, 1)?;
            let rent = decode_rent(account_metas, accounts, 2)?;
            initialize_nonce(accounts, nonce, authority, &recent, rent)
        }
        ArchiveV2SystemInstructionData::AuthorizeNonceAccount { authority } => {
            let account = required_meta(account_metas, 0)?;
            authorize_nonce(accounts, account, authority, &signers)
        }
        ArchiveV2SystemInstructionData::TransferWithSeed { .. } => {
            Err(LaunchSystemError::PostLaunchVariant {
                variant: "TransferWithSeed",
            })
        }
        ArchiveV2SystemInstructionData::UpgradeNonceAccount => {
            Err(LaunchSystemError::PostLaunchVariant {
                variant: "UpgradeNonceAccount",
            })
        }
        ArchiveV2SystemInstructionData::CreateAccountAllowPrefund { .. } => {
            Err(LaunchSystemError::PostLaunchVariant {
                variant: "CreateAccountAllowPrefund",
            })
        }
    }
}

/// Private launch-era wire mirrors. These intentionally do not use a modern
/// Solana SDK: v1.0.7 stores `Versions::Current(Box<State>)`, whose initialized
/// fixed-int bincode representation is exactly 80 bytes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
enum LaunchNonceVersions {
    Current(Box<LaunchNonceState>),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
enum LaunchNonceState {
    Uninitialized,
    Initialized(LaunchNonceData),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
struct LaunchNonceData {
    authority: [u8; 32],
    blockhash: [u8; 32],
    fee_calculator: LaunchNonceFeeCalculator,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
struct LaunchNonceFeeCalculator {
    lamports_per_signature: u64,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
struct LaunchRecentBlockhashEntry {
    blockhash: [u8; 32],
    fee_calculator: LaunchNonceFeeCalculator,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
struct LaunchRecentBlockhashes(Vec<LaunchRecentBlockhashEntry>);

#[derive(
    Debug, Clone, Copy, PartialEq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
struct LaunchRent {
    lamports_per_byte_year: u64,
    exemption_threshold: f64,
    burn_percent: u8,
}

impl LaunchRent {
    fn minimum_balance(self, data_len: usize) -> u64 {
        const ACCOUNT_STORAGE_OVERHEAD: u64 = 128;
        (ACCOUNT_STORAGE_OVERHEAD
            .wrapping_add(data_len as u64)
            .wrapping_mul(self.lamports_per_byte_year) as f64
            * self.exemption_threshold) as u64
    }
}

fn decode_recent_blockhashes(
    account_metas: &[LaunchSystemAccountMeta],
    accounts: &CowAccountMap,
    position: usize,
) -> Result<LaunchRecentBlockhashes, LaunchSystemError> {
    let meta = required_meta(account_metas, position)?;
    if meta.pubkey != RECENT_BLOCKHASHES_SYSVAR_ID {
        return Err(LaunchSystemError::InvalidSysvar {
            position,
            expected: RECENT_BLOCKHASHES_SYSVAR_ID,
            found: meta.pubkey,
        });
    }
    let account = accounts
        .get(&meta.pubkey)
        .expect("instruction accounts were materialized before dispatch");
    wincode::deserialize(&account.data)
        .map_err(|_| LaunchSystemError::InvalidSysvarData { position })
}

fn decode_rent(
    account_metas: &[LaunchSystemAccountMeta],
    accounts: &CowAccountMap,
    position: usize,
) -> Result<LaunchRent, LaunchSystemError> {
    let meta = required_meta(account_metas, position)?;
    if meta.pubkey != RENT_SYSVAR_ID {
        return Err(LaunchSystemError::InvalidSysvar {
            position,
            expected: RENT_SYSVAR_ID,
            found: meta.pubkey,
        });
    }
    let account = accounts
        .get(&meta.pubkey)
        .expect("instruction accounts were materialized before dispatch");
    wincode::deserialize(&account.data)
        .map_err(|_| LaunchSystemError::InvalidSysvarData { position })
}

fn initialize_nonce(
    accounts: &mut CowAccountMap,
    meta: &LaunchSystemAccountMeta,
    authority: &[u8; 32],
    recent_blockhashes: &LaunchRecentBlockhashes,
    rent: LaunchRent,
) -> Result<LaunchSystemMutation, LaunchSystemError> {
    let recent = recent_blockhashes
        .0
        .first()
        .ok_or(LaunchSystemError::NoRecentBlockhashes)?;
    let account = accounts
        .get(&meta.pubkey)
        .expect("instruction accounts were materialized before dispatch");
    let versions: LaunchNonceVersions =
        wincode::deserialize(&account.data).map_err(|_| LaunchSystemError::InvalidAccountData {
            pubkey: meta.pubkey,
        })?;
    let LaunchNonceVersions::Current(state) = versions;
    if *state != LaunchNonceState::Uninitialized {
        return Err(LaunchSystemError::BadNonceAccountState {
            pubkey: meta.pubkey,
        });
    }
    let minimum = rent.minimum_balance(account.data.len());
    if account.lamports < minimum {
        return Err(LaunchSystemError::NonceInsufficientFunds {
            pubkey: meta.pubkey,
            balance: account.lamports,
            minimum,
        });
    }
    let blockhash = recent.blockhash;
    let lamports_per_signature = recent.fee_calculator.lamports_per_signature;
    let initialized =
        LaunchNonceVersions::Current(Box::new(LaunchNonceState::Initialized(LaunchNonceData {
            authority: *authority,
            blockhash,
            fee_calculator: LaunchNonceFeeCalculator {
                lamports_per_signature,
            },
        })));
    let encoded =
        wincode::serialize(&initialized).map_err(|_| LaunchSystemError::GenericError {
            pubkey: meta.pubkey,
        })?;
    if encoded.len() > account.data.len() {
        return Err(LaunchSystemError::AccountDataTooSmall {
            pubkey: meta.pubkey,
            needed: encoded.len(),
            available: account.data.len(),
        });
    }
    accounts
        .get_mut(&meta.pubkey)
        .expect("nonce account was checked above")
        .data[..encoded.len()]
        .copy_from_slice(&encoded);
    Ok(LaunchSystemMutation::InitializeNonce {
        account: meta.pubkey,
        authority: *authority,
        blockhash,
        lamports_per_signature,
    })
}

fn advance_nonce(
    accounts: &mut CowAccountMap,
    meta: &LaunchSystemAccountMeta,
    recent_blockhashes: &LaunchRecentBlockhashes,
    signers: &BTreeSet<[u8; 32]>,
) -> Result<LaunchSystemMutation, LaunchSystemError> {
    // v1.0.7 checks the Bank-provided list before even decoding the nonce
    // account. Preserve that observable error ordering for malformed state.
    let recent = recent_blockhashes
        .0
        .first()
        .ok_or(LaunchSystemError::NoRecentBlockhashes)?;
    let account = accounts
        .get(&meta.pubkey)
        .expect("instruction accounts were materialized before dispatch");
    let versions: LaunchNonceVersions =
        wincode::deserialize(&account.data).map_err(|_| LaunchSystemError::InvalidAccountData {
            pubkey: meta.pubkey,
        })?;
    let LaunchNonceVersions::Current(state) = versions;
    let LaunchNonceState::Initialized(mut data) = *state else {
        return Err(LaunchSystemError::BadNonceAccountState {
            pubkey: meta.pubkey,
        });
    };
    let authority = data.authority;
    if !signers.contains(&authority) {
        return Err(LaunchSystemError::MissingRequiredSignature { pubkey: authority });
    }
    let old_blockhash = data.blockhash;
    if old_blockhash == recent.blockhash {
        return Err(LaunchSystemError::NonceNotExpired {
            pubkey: meta.pubkey,
            blockhash: old_blockhash,
        });
    }

    data.blockhash = recent.blockhash;
    data.fee_calculator = recent.fee_calculator.clone();
    let new_blockhash = data.blockhash;
    let lamports_per_signature = data.fee_calculator.lamports_per_signature;
    let encoded = wincode::serialize(&LaunchNonceVersions::Current(Box::new(
        LaunchNonceState::Initialized(data),
    )))
    .map_err(|_| LaunchSystemError::GenericError {
        pubkey: meta.pubkey,
    })?;
    if encoded.len() > account.data.len() {
        return Err(LaunchSystemError::AccountDataTooSmall {
            pubkey: meta.pubkey,
            needed: encoded.len(),
            available: account.data.len(),
        });
    }
    accounts
        .get_mut(&meta.pubkey)
        .expect("nonce account was checked above")
        .data[..encoded.len()]
        .copy_from_slice(&encoded);

    Ok(LaunchSystemMutation::AdvanceNonce {
        account: meta.pubkey,
        authority,
        old_blockhash,
        new_blockhash,
        lamports_per_signature,
    })
}

fn withdraw_nonce(
    accounts: &mut CowAccountMap,
    nonce_meta: &LaunchSystemAccountMeta,
    destination_meta: &LaunchSystemAccountMeta,
    lamports: u64,
    recent_blockhashes: &LaunchRecentBlockhashes,
    rent: LaunchRent,
    signers: &BTreeSet<[u8; 32]>,
) -> Result<LaunchSystemMutation, LaunchSystemError> {
    // Preserve v1.0.7 `nonce::Account::withdraw_nonce_account` ordering: both
    // sysvars have already been decoded by the dispatcher, then nonce state
    // and funding constraints are checked before the required signature.
    let account = accounts
        .get(&nonce_meta.pubkey)
        .expect("instruction accounts were materialized before dispatch");
    let balance = account.lamports;
    let versions: LaunchNonceVersions =
        wincode::deserialize(&account.data).map_err(|_| LaunchSystemError::InvalidAccountData {
            pubkey: nonce_meta.pubkey,
        })?;
    let LaunchNonceVersions::Current(state) = versions;
    let signer = match *state {
        LaunchNonceState::Uninitialized => {
            if lamports > balance {
                return Err(LaunchSystemError::InsufficientFunds {
                    pubkey: nonce_meta.pubkey,
                    balance,
                    required: lamports,
                });
            }
            nonce_meta.pubkey
        }
        LaunchNonceState::Initialized(data) => {
            if lamports == balance {
                let recent = recent_blockhashes
                    .0
                    .first()
                    .ok_or(LaunchSystemError::NoRecentBlockhashes)?;
                if data.blockhash == recent.blockhash {
                    return Err(LaunchSystemError::NonceNotExpired {
                        pubkey: nonce_meta.pubkey,
                        blockhash: data.blockhash,
                    });
                }
            } else {
                let minimum = rent.minimum_balance(account.data.len());
                let required = lamports.wrapping_add(minimum);
                if required > balance {
                    return Err(LaunchSystemError::InsufficientFunds {
                        pubkey: nonce_meta.pubkey,
                        balance,
                        required,
                    });
                }
            }
            data.authority
        }
    };

    if !signers.contains(&signer) {
        return Err(LaunchSystemError::MissingRequiredSignature { pubkey: signer });
    }

    // Unlike ordinary System::Transfer, v1.0.7 releases the source RefCell
    // borrow before borrowing the destination. Repeated account indices are
    // therefore legal and produce a lamport no-op.
    if nonce_meta.pubkey != destination_meta.pubkey {
        let destination_lamports = accounts
            .get(&destination_meta.pubkey)
            .expect("instruction accounts were materialized before dispatch")
            .lamports
            .wrapping_add(lamports);
        accounts
            .get_mut(&nonce_meta.pubkey)
            .expect("nonce account was checked above")
            .lamports = balance.wrapping_sub(lamports);
        accounts
            .get_mut(&destination_meta.pubkey)
            .expect("destination account was materialized before dispatch")
            .lamports = destination_lamports;
    }

    Ok(LaunchSystemMutation::WithdrawNonce {
        account: nonce_meta.pubkey,
        destination: destination_meta.pubkey,
        signer,
        lamports,
    })
}

fn authorize_nonce(
    accounts: &mut CowAccountMap,
    meta: &LaunchSystemAccountMeta,
    new_authority: &[u8; 32],
    signers: &BTreeSet<[u8; 32]>,
) -> Result<LaunchSystemMutation, LaunchSystemError> {
    let account = accounts
        .get(&meta.pubkey)
        .expect("instruction accounts were materialized before dispatch");
    let versions: LaunchNonceVersions =
        wincode::deserialize(&account.data).map_err(|_| LaunchSystemError::InvalidAccountData {
            pubkey: meta.pubkey,
        })?;
    let LaunchNonceVersions::Current(state) = versions;
    let LaunchNonceState::Initialized(mut data) = *state else {
        return Err(LaunchSystemError::BadNonceAccountState {
            pubkey: meta.pubkey,
        });
    };
    let old_authority = data.authority;
    if !signers.contains(&old_authority) {
        return Err(LaunchSystemError::MissingRequiredSignature {
            pubkey: old_authority,
        });
    }
    data.authority = *new_authority;
    let encoded = wincode::serialize(&LaunchNonceVersions::Current(Box::new(
        LaunchNonceState::Initialized(data),
    )))
    .map_err(|_| LaunchSystemError::GenericError {
        pubkey: meta.pubkey,
    })?;
    if encoded.len() > account.data.len() {
        return Err(LaunchSystemError::AccountDataTooSmall {
            pubkey: meta.pubkey,
            needed: encoded.len(),
            available: account.data.len(),
        });
    }
    accounts
        .get_mut(&meta.pubkey)
        .expect("nonce account was checked above")
        .data[..encoded.len()]
        .copy_from_slice(&encoded);
    Ok(LaunchSystemMutation::AuthorizeNonce {
        account: meta.pubkey,
        old_authority,
        new_authority: *new_authority,
    })
}

#[derive(Debug, Clone, Copy)]
struct Address {
    address: [u8; 32],
    base: Option<[u8; 32]>,
}

impl Address {
    fn plain(address: [u8; 32]) -> Self {
        Self {
            address,
            base: None,
        }
    }

    fn seeded(
        supplied: [u8; 32],
        base: [u8; 32],
        seed: &str,
        owner: [u8; 32],
    ) -> Result<Self, LaunchSystemError> {
        let derived = create_address_with_seed(&base, seed, &owner)?;
        if supplied != derived {
            return Err(LaunchSystemError::AddressWithSeedMismatch { supplied, derived });
        }
        Ok(Self {
            address: supplied,
            base: Some(base),
        })
    }

    fn required_signer(self) -> [u8; 32] {
        self.base.unwrap_or(self.address)
    }
}

fn required_meta(
    account_metas: &[LaunchSystemAccountMeta],
    position: usize,
) -> Result<&LaunchSystemAccountMeta, LaunchSystemError> {
    account_metas
        .get(position)
        .ok_or(LaunchSystemError::MissingAccount { position })
}

/// The subset of v1.0.7 `message_processor::PreAccount` needed to verify a
/// native System instruction after its entrypoint returns successfully.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LaunchPreAccount {
    pubkey: [u8; 32],
    is_writable: bool,
    lamports: u64,
    data_len: usize,
    data: Option<Vec<u8>>,
    owner: [u8; 32],
    executable: bool,
    rent_epoch: u64,
}

impl LaunchPreAccount {
    fn new(pubkey: [u8; 32], is_writable: bool, account: &AccountSnapshot) -> LaunchPreAccount {
        Self {
            pubkey,
            is_writable,
            lamports: account.lamports,
            data_len: account.data.len(),
            data: should_verify_data(&account.owner, is_writable).then(|| account.data.to_vec()),
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        }
    }

    fn verify(&self, post: &AccountSnapshot) -> Result<(), LaunchSystemError> {
        // Keep this ordering aligned with v1.0.7 `PreAccount::verify`: the
        // first violated invariant is the historical InstructionError.
        if self.owner != post.owner
            && (!self.is_writable || self.owner != SYSTEM_PROGRAM_ID || !is_zeroed(&post.data))
        {
            return Err(LaunchSystemError::ModifiedProgramId {
                pubkey: self.pubkey,
            });
        }

        if self.owner != SYSTEM_PROGRAM_ID && self.lamports > post.lamports {
            return Err(LaunchSystemError::ExternalAccountLamportSpend {
                pubkey: self.pubkey,
            });
        }

        if !self.is_writable && self.lamports != post.lamports {
            return Err(LaunchSystemError::ReadonlyLamportChange {
                pubkey: self.pubkey,
            });
        }

        if self.data_len != post.data.len() && self.owner != SYSTEM_PROGRAM_ID {
            return Err(LaunchSystemError::AccountDataSizeChanged {
                pubkey: self.pubkey,
            });
        }

        if should_verify_data(&self.owner, self.is_writable)
            && self.data.as_ref() != Some(&post.data)
        {
            return Err(if self.is_writable {
                LaunchSystemError::ExternalAccountDataModified {
                    pubkey: self.pubkey,
                }
            } else {
                LaunchSystemError::ReadonlyDataModified {
                    pubkey: self.pubkey,
                }
            });
        }

        if self.executable != post.executable
            && (!self.is_writable || self.executable || self.owner != SYSTEM_PROGRAM_ID)
        {
            return Err(LaunchSystemError::ExecutableModified {
                pubkey: self.pubkey,
            });
        }

        if self.rent_epoch != post.rent_epoch {
            return Err(LaunchSystemError::RentEpochModified {
                pubkey: self.pubkey,
            });
        }

        Ok(())
    }
}

fn should_verify_data(owner: &[u8; 32], is_writable: bool) -> bool {
    *owner != SYSTEM_PROGRAM_ID || !is_writable
}

fn is_zeroed(data: &[u8]) -> bool {
    data.iter().all(|byte| *byte == 0)
}

fn launch_pre_accounts(
    account_metas: &[LaunchSystemAccountMeta],
    accounts: &CowAccountMap,
) -> Vec<LaunchPreAccount> {
    account_metas
        .iter()
        .enumerate()
        .filter(|(index, meta)| {
            // v1.0.7 skips an account when the same RefCell occurs later in
            // the instruction, so only its final occurrence is verified.
            !account_metas[index + 1..]
                .iter()
                .any(|later| later.pubkey == meta.pubkey)
        })
        .map(|(_, meta)| {
            LaunchPreAccount::new(
                meta.pubkey,
                meta.is_writable,
                accounts
                    .get(&meta.pubkey)
                    .expect("instruction accounts were materialized before snapshot"),
            )
        })
        .collect()
}

fn verify_launch_system_instruction(
    pre_accounts: &[LaunchPreAccount],
    accounts: &CowAccountMap,
) -> Result<(), LaunchSystemError> {
    let mut pre_lamports = 0_u128;
    let mut post_lamports = 0_u128;
    for pre in pre_accounts {
        let post = accounts
            .get(&pre.pubkey)
            .expect("instruction accounts remain materialized through verification");
        pre.verify(post)?;
        pre_lamports += u128::from(pre.lamports);
        post_lamports += u128::from(post.lamports);
    }
    if pre_lamports != post_lamports {
        return Err(LaunchSystemError::UnbalancedInstruction {
            pre_lamports,
            post_lamports,
        });
    }
    Ok(())
}

fn reject_prefunded_create_destination(
    accounts: &CowAccountMap,
    meta: &LaunchSystemAccountMeta,
    new_system_processor: bool,
) -> Result<(), LaunchSystemError> {
    if new_system_processor
        && accounts
            .get(&meta.pubkey)
            .expect("instruction accounts were materialized before dispatch")
            .lamports
            > 0
    {
        return Err(LaunchSystemError::AccountAlreadyInUse {
            pubkey: meta.pubkey,
        });
    }
    Ok(())
}

fn allocate(
    accounts: &mut CowAccountMap,
    meta: &LaunchSystemAccountMeta,
    address: &Address,
    space: u64,
    signers: &BTreeSet<[u8; 32]>,
) -> Result<(), LaunchSystemError> {
    let required_signer = address.required_signer();
    if !signers.contains(&required_signer) {
        return Err(LaunchSystemError::MissingRequiredSignature {
            pubkey: required_signer,
        });
    }
    let account = accounts
        .get_mut(&meta.pubkey)
        .expect("instruction accounts were materialized before dispatch");
    if !account.data.is_empty() || account.owner != SYSTEM_PROGRAM_ID {
        return Err(LaunchSystemError::AccountAlreadyInUse {
            pubkey: meta.pubkey,
        });
    }
    if space > MAX_PERMITTED_DATA_LENGTH {
        return Err(LaunchSystemError::InvalidAccountDataLength { space });
    }
    account.data = vec![0; space as usize].into();
    Ok(())
}

fn assign(
    accounts: &mut CowAccountMap,
    meta: &LaunchSystemAccountMeta,
    address: &Address,
    owner: &[u8; 32],
    signers: &BTreeSet<[u8; 32]>,
) -> Result<(), LaunchSystemError> {
    let account = accounts
        .get(&meta.pubkey)
        .expect("instruction accounts were materialized before dispatch");
    if account.owner == *owner {
        return Ok(());
    }
    let required_signer = address.required_signer();
    if !signers.contains(&required_signer) {
        return Err(LaunchSystemError::MissingRequiredSignature {
            pubkey: required_signer,
        });
    }
    if is_launch_sysvar_owner_id(owner) {
        return Err(LaunchSystemError::InvalidProgramId { owner: *owner });
    }
    accounts
        .get_mut(&meta.pubkey)
        .expect("account was checked above")
        .owner = *owner;
    Ok(())
}

fn allocate_and_assign(
    accounts: &mut CowAccountMap,
    meta: &LaunchSystemAccountMeta,
    address: &Address,
    space: u64,
    owner: &[u8; 32],
    signers: &BTreeSet<[u8; 32]>,
) -> Result<(), LaunchSystemError> {
    allocate(accounts, meta, address, space, signers)?;
    assign(accounts, meta, address, owner, signers)
}

fn transfer(
    accounts: &mut CowAccountMap,
    from: &LaunchSystemAccountMeta,
    to: &LaunchSystemAccountMeta,
    lamports: u64,
    self_transfer_supported: bool,
) -> Result<(), LaunchSystemError> {
    if lamports == 0 {
        return Ok(());
    }
    if !from.is_signer {
        return Err(LaunchSystemError::MissingRequiredSignature {
            pubkey: from.pubkey,
        });
    }
    // The legacy dispatcher holds a mutable RefCell borrow of `to` while
    // `transfer()` obtains its first source-account borrow. The epoch-40
    // replacement borrows source and destination sequentially instead.
    if from.pubkey == to.pubkey && !self_transfer_supported {
        return Err(LaunchSystemError::AccountBorrowConflict {
            pubkey: from.pubkey,
        });
    }
    let source = accounts
        .get(&from.pubkey)
        .expect("instruction accounts were materialized before dispatch");
    if !source.data.is_empty() {
        return Err(LaunchSystemError::SourceCarriesData {
            pubkey: from.pubkey,
        });
    }
    if source.lamports < lamports {
        return Err(LaunchSystemError::ResultWithNegativeLamports {
            pubkey: from.pubkey,
            required: lamports,
        });
    }
    if from.pubkey == to.pubkey {
        return Ok(());
    }
    let destination_lamports = accounts
        .get(&to.pubkey)
        .expect("instruction accounts were materialized before dispatch")
        .lamports
        .wrapping_add(lamports);
    accounts
        .get_mut(&from.pubkey)
        .expect("source account was checked above")
        .lamports -= lamports;
    accounts
        .get_mut(&to.pubkey)
        .expect("destination account was checked above")
        .lamports = destination_lamports;
    Ok(())
}

fn is_launch_sysvar_owner_id(pubkey: &[u8; 32]) -> bool {
    *pubkey == SYSVAR_OWNER_ID
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: [u8; 32] = [
        204, 241, 115, 109, 41, 173, 110, 48, 24, 113, 210, 213, 163, 78, 1, 112, 146, 114, 235,
        220, 96, 185, 184, 85, 163, 27, 124, 48, 54, 250, 233, 54,
    ];
    const TARGET: [u8; 32] = [
        11, 212, 126, 90, 51, 90, 195, 254, 212, 46, 210, 147, 188, 141, 145, 180, 248, 241, 36,
        115, 78, 149, 57, 42, 47, 156, 168, 112, 153, 138, 68, 29,
    ];
    const STAKE_PROGRAM: [u8; 32] = [
        6, 161, 216, 23, 145, 55, 84, 42, 152, 52, 55, 189, 254, 42, 122, 178, 85, 127, 83, 92,
        138, 120, 114, 43, 104, 164, 157, 192, 0, 0, 0, 0,
    ];
    fn meta(pubkey: [u8; 32], is_signer: bool, is_writable: bool) -> LaunchSystemAccountMeta {
        LaunchSystemAccountMeta {
            pubkey,
            is_signer,
            is_writable,
        }
    }

    fn pubkey(value: &str) -> [u8; 32] {
        bs58::decode(value).into_vec().unwrap().try_into().unwrap()
    }

    fn initialized_nonce_account(
        authority: [u8; 32],
        blockhash: [u8; 32],
        lamports_per_signature: u64,
    ) -> AccountSnapshot {
        let versions = LaunchNonceVersions::Current(Box::new(LaunchNonceState::Initialized(
            LaunchNonceData {
                authority,
                blockhash,
                fee_calculator: LaunchNonceFeeCalculator {
                    lamports_per_signature,
                },
            },
        )));
        let data = wincode::serialize(&versions).unwrap();
        assert_eq!(data.len(), LAUNCH_NONCE_ACCOUNT_DATA_LEN);
        AccountSnapshot {
            lamports: 1_000_000,
            data: data.into(),
            ..default_system_account()
        }
    }

    fn uninitialized_nonce_account(lamports: u64) -> AccountSnapshot {
        let versions = LaunchNonceVersions::Current(Box::new(LaunchNonceState::Uninitialized));
        let encoded = wincode::serialize(&versions).unwrap();
        let mut data = vec![0; LAUNCH_NONCE_ACCOUNT_DATA_LEN];
        data[..encoded.len()].copy_from_slice(&encoded);
        AccountSnapshot {
            lamports,
            data: data.into(),
            ..default_system_account()
        }
    }

    fn recent_blockhashes_account(entries: Vec<LaunchRecentBlockhashEntry>) -> AccountSnapshot {
        AccountSnapshot {
            owner: SYSVAR_OWNER_ID,
            data: wincode::serialize(&LaunchRecentBlockhashes(entries))
                .unwrap()
                .into(),
            ..default_system_account()
        }
    }

    fn rent_account(rent: LaunchRent) -> AccountSnapshot {
        AccountSnapshot {
            owner: SYSVAR_OWNER_ID,
            data: wincode::serialize(&rent).unwrap().into(),
            ..default_system_account()
        }
    }

    #[test]
    fn launch_slot_105368_allocate_with_seed_matches_historical_address() {
        assert_eq!(
            create_address_with_seed(&BASE, "1", &STAKE_PROGRAM).unwrap(),
            TARGET
        );
        let mut accounts = AccountMap::from([(
            BASE,
            AccountSnapshot {
                lamports: 19_090_880,
                ..default_system_account()
            },
        )]);
        let mutation = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::AllocateWithSeed {
                base: BASE,
                seed: "1".to_owned(),
                space: 200,
                owner: STAKE_PROGRAM,
            },
            &[meta(TARGET, false, true), meta(BASE, true, true)],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(
            mutation,
            LaunchSystemMutation::Allocate {
                account: TARGET,
                space: 200,
                owner: STAKE_PROGRAM,
                seeded: true,
            }
        );
        assert_eq!(accounts[&TARGET].owner, STAKE_PROGRAM);
        assert_eq!(accounts[&TARGET].lamports, 0);
        assert_eq!(accounts[&TARGET].data, vec![0; 200]);
        assert_eq!(accounts[&BASE].lamports, 19_090_880);
    }

    #[test]
    fn missing_seed_base_signature_rolls_back_default_account_creation() {
        let mut accounts = AccountMap::new();
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::AllocateWithSeed {
                base: BASE,
                seed: "1".to_owned(),
                space: 200,
                owner: STAKE_PROGRAM,
            },
            &[meta(TARGET, false, true), meta(BASE, false, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::MissingRequiredSignature { pubkey: BASE }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn seeded_address_mismatch_fails_before_mutation() {
        let wrong_target = [42; 32];
        let mut accounts = AccountMap::new();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::AllocateWithSeed {
                base: BASE,
                seed: "1".to_owned(),
                space: 200,
                owner: STAKE_PROGRAM,
            },
            &[meta(wrong_target, false, true), meta(BASE, true, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LaunchSystemError::AddressWithSeedMismatch { supplied, derived }
                if supplied == wrong_target && derived == TARGET
        ));
        assert!(accounts.is_empty());
    }

    #[test]
    fn create_account_allocates_assigns_and_balances_lamports() {
        let from = [1; 32];
        let to = [2; 32];
        let owner = [3; 32];
        let mut accounts = AccountMap::from([(
            from,
            AccountSnapshot {
                lamports: 10,
                ..default_system_account()
            },
        )]);
        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::CreateAccount {
                lamports: 7,
                space: 4,
                owner,
            },
            &[meta(from, true, true), meta(to, true, true)],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts[&from].lamports, 3);
        assert_eq!(accounts[&to].lamports, 7);
        assert_eq!(accounts[&to].owner, owner);
        assert_eq!(accounts[&to].data, vec![0; 4]);
    }

    #[test]
    fn create_account_with_seed_uses_the_base_signer() {
        let from = [4; 32];
        let mut accounts = AccountMap::from([(
            from,
            AccountSnapshot {
                lamports: 11,
                ..default_system_account()
            },
        )]);
        let mutation = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::CreateAccountWithSeed {
                base: BASE,
                seed: "1".to_owned(),
                lamports: 7,
                space: 3,
                owner: STAKE_PROGRAM,
            },
            &[
                meta(from, true, true),
                meta(TARGET, false, true),
                meta(BASE, true, false),
            ],
            &mut accounts,
        )
        .unwrap();

        assert!(matches!(
            mutation,
            LaunchSystemMutation::CreateAccount {
                from: value_from,
                to: TARGET,
                lamports: 7,
                space: 3,
                owner: STAKE_PROGRAM,
                seeded: true,
            } if value_from == from
        ));
        assert_eq!(accounts[&from].lamports, 4);
        assert_eq!(accounts[&TARGET].lamports, 7);
        assert_eq!(accounts[&TARGET].owner, STAKE_PROGRAM);
        assert_eq!(accounts[&TARGET].data, vec![0; 3]);
    }

    #[test]
    fn stable_epoch_40_create_account_rejects_prefunded_destination() {
        let from = [5; 32];
        let to = [6; 32];
        let owner = [7; 32];
        let initial = AccountMap::from([
            (
                from,
                AccountSnapshot {
                    lamports: 100,
                    ..default_system_account()
                },
            ),
            (
                to,
                AccountSnapshot {
                    lamports: 1,
                    ..default_system_account()
                },
            ),
        ]);
        let instruction = ArchiveV2SystemInstructionData::CreateAccount {
            lamports: 50,
            space: 2,
            owner,
        };
        let signed_metas = [meta(from, true, true), meta(to, true, true)];

        let mut legacy = initial.clone();
        apply_launch_system_instruction_for_epoch(
            &instruction,
            &signed_metas,
            &mut legacy,
            STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH - 1,
        )
        .unwrap();
        assert_eq!(legacy[&from].lamports, 50);
        assert_eq!(legacy[&to].lamports, 51);
        assert_eq!(legacy[&to].owner, owner);
        assert_eq!(legacy[&to].data, vec![0; 2]);

        let mut current = initial.clone();
        assert_eq!(
            apply_launch_system_instruction_for_epoch(
                &instruction,
                &signed_metas,
                &mut current,
                STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
            )
            .unwrap_err(),
            LaunchSystemError::AccountAlreadyInUse { pubkey: to }
        );
        assert_eq!(current, initial);

        let mut missing_destination_signature = initial.clone();
        assert_eq!(
            apply_launch_system_instruction_for_epoch(
                &instruction,
                &[meta(from, false, true), meta(to, false, true)],
                &mut missing_destination_signature,
                STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
            )
            .unwrap_err(),
            LaunchSystemError::AccountAlreadyInUse { pubkey: to }
        );
        assert_eq!(missing_destination_signature, initial);
    }

    #[test]
    fn stable_epoch_40_seeded_create_checks_address_then_prefunding() {
        let from = [8; 32];
        let initial = AccountMap::from([
            (
                from,
                AccountSnapshot {
                    lamports: 100,
                    ..default_system_account()
                },
            ),
            (
                TARGET,
                AccountSnapshot {
                    lamports: 1,
                    ..default_system_account()
                },
            ),
        ]);
        let instruction = ArchiveV2SystemInstructionData::CreateAccountWithSeed {
            base: BASE,
            seed: "1".to_owned(),
            lamports: 50,
            space: 2,
            owner: STAKE_PROGRAM,
        };
        let signed_metas = [
            meta(from, true, true),
            meta(TARGET, false, true),
            meta(BASE, true, false),
        ];

        let mut legacy = initial.clone();
        apply_launch_system_instruction_for_epoch(
            &instruction,
            &signed_metas,
            &mut legacy,
            STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH - 1,
        )
        .unwrap();
        assert_eq!(legacy[&from].lamports, 50);
        assert_eq!(legacy[&TARGET].lamports, 51);
        assert_eq!(legacy[&TARGET].owner, STAKE_PROGRAM);
        assert_eq!(legacy[&TARGET].data, vec![0; 2]);

        for metas in [
            signed_metas.to_vec(),
            vec![
                meta(from, false, true),
                meta(TARGET, false, true),
                meta(BASE, false, false),
            ],
        ] {
            let mut current = initial.clone();
            assert_eq!(
                apply_launch_system_instruction_for_epoch(
                    &instruction,
                    &metas,
                    &mut current,
                    STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
                )
                .unwrap_err(),
                LaunchSystemError::AccountAlreadyInUse { pubkey: TARGET }
            );
            assert_eq!(current, initial);
        }

        let wrong_target = [42; 32];
        let mut mismatched = AccountMap::from([
            (from, initial[&from].clone()),
            (wrong_target, initial[&TARGET].clone()),
        ]);
        assert!(matches!(
            apply_launch_system_instruction_for_epoch(
                &instruction,
                &[
                    meta(from, true, true),
                    meta(wrong_target, false, true),
                    meta(BASE, false, false),
                ],
                &mut mismatched,
                STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
            ),
            Err(LaunchSystemError::AddressWithSeedMismatch { supplied, derived })
                if supplied == wrong_target && derived == TARGET
        ));
    }

    #[test]
    fn plain_and_seeded_assign_and_plain_allocate_are_supported() {
        let plain = [5; 32];
        let allocated = [6; 32];
        let plain_owner = [7; 32];
        let mut accounts = AccountMap::new();

        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Assign { owner: plain_owner },
            &[meta(plain, true, true)],
            &mut accounts,
        )
        .unwrap();
        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Allocate { space: 5 },
            &[meta(allocated, true, true)],
            &mut accounts,
        )
        .unwrap();
        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::AssignWithSeed {
                base: BASE,
                seed: "1".to_owned(),
                owner: STAKE_PROGRAM,
            },
            &[meta(TARGET, false, true), meta(BASE, true, false)],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(accounts[&plain].owner, plain_owner);
        assert_eq!(accounts[&allocated].owner, SYSTEM_PROGRAM_ID);
        assert_eq!(accounts[&allocated].data, vec![0; 5]);
        assert_eq!(accounts[&TARGET].owner, STAKE_PROGRAM);
        assert!(accounts[&TARGET].data.is_empty());
    }

    #[test]
    fn transfer_moves_lamports_and_zero_transfer_is_a_privilege_free_noop() {
        let from = [8; 32];
        let to = [9; 32];
        let mut accounts = AccountMap::from([(
            from,
            AccountSnapshot {
                lamports: 9,
                ..default_system_account()
            },
        )]);
        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Transfer { lamports: 6 },
            &[meta(from, true, true), meta(to, false, true)],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts[&from].lamports, 3);
        assert_eq!(accounts[&to].lamports, 6);

        let before = accounts.clone();
        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Transfer { lamports: 0 },
            &[meta(from, false, false), meta(to, false, false)],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts, before);
    }

    #[test]
    fn zero_lamport_aliased_transfer_succeeds_on_both_system_processors() {
        let account = [10; 32];
        let initial = AccountMap::from([(
            account,
            AccountSnapshot {
                data: vec![1].into(),
                ..default_system_account()
            },
        )]);
        let instruction = ArchiveV2SystemInstructionData::Transfer { lamports: 0 };
        let metas = [meta(account, false, false), meta(account, false, false)];

        for epoch in [
            STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH - 1,
            STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
        ] {
            let mut accounts = initial.clone();
            apply_launch_system_instruction_for_epoch(&instruction, &metas, &mut accounts, epoch)
                .unwrap();
            assert_eq!(accounts, initial);
        }
    }

    #[test]
    fn readonly_zero_effect_allocate_and_create_succeed() {
        let allocated = [10; 32];
        let mut accounts = AccountMap::from([(allocated, default_system_account())]);
        let before = accounts.clone();
        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Allocate { space: 0 },
            &[meta(allocated, true, false)],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts, before);

        let from = [11; 32];
        let to = [12; 32];
        let mut accounts = AccountMap::from([
            (
                from,
                AccountSnapshot {
                    lamports: 3,
                    owner: [13; 32],
                    data: vec![1].into(),
                    ..default_system_account()
                },
            ),
            (to, default_system_account()),
        ]);
        let before = accounts.clone();
        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::CreateAccount {
                lamports: 0,
                space: 0,
                owner: SYSTEM_PROGRAM_ID,
            },
            &[meta(from, false, false), meta(to, true, false)],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts, before);
    }

    #[test]
    fn readonly_allocate_keeps_native_error_order_then_post_verifies() {
        let account = [14; 32];
        let mut accounts = AccountMap::from([(
            account,
            AccountSnapshot {
                data: vec![1].into(),
                ..default_system_account()
            },
        )]);
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Allocate {
                space: MAX_PERMITTED_DATA_LENGTH + 1,
            },
            &[meta(account, true, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::AccountAlreadyInUse { pubkey: account }
        );
        assert_eq!(accounts, before);

        let mut accounts = AccountMap::from([(account, default_system_account())]);
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Allocate {
                space: MAX_PERMITTED_DATA_LENGTH + 1,
            },
            &[meta(account, true, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LaunchSystemError::InvalidAccountDataLength { .. }
        ));
        assert_eq!(accounts, before);

        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Allocate { space: 1 },
            &[meta(account, true, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::ReadonlyDataModified { pubkey: account }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn assign_rejects_only_the_launch_sysvar_owner_id() {
        let account = [15; 32];
        let mut accounts = AccountMap::from([(account, default_system_account())]);
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Assign {
                owner: SYSVAR_OWNER_ID,
            },
            &[meta(account, true, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::InvalidProgramId {
                owner: SYSVAR_OWNER_ID
            }
        );
        assert_eq!(accounts, before);

        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Assign {
                owner: crate::CLOCK_SYSVAR_ID,
            },
            &[meta(account, true, true)],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts[&account].owner, crate::CLOCK_SYSVAR_ID);

        let mut accounts = AccountMap::from([(
            account,
            AccountSnapshot {
                owner: SYSVAR_OWNER_ID,
                data: vec![7].into(),
                ..default_system_account()
            },
        )]);
        let before = accounts.clone();
        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Assign {
                owner: SYSVAR_OWNER_ID,
            },
            &[meta(account, false, false)],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts, before);
    }

    #[test]
    fn assign_owner_constraints_are_post_instruction_invariants() {
        let account = [16; 32];
        for snapshot in [
            AccountSnapshot {
                owner: [17; 32],
                ..default_system_account()
            },
            AccountSnapshot {
                data: vec![1].into(),
                ..default_system_account()
            },
        ] {
            let mut accounts = AccountMap::from([(account, snapshot)]);
            let before = accounts.clone();
            let error = apply_launch_system_instruction(
                &ArchiveV2SystemInstructionData::Assign { owner: [18; 32] },
                &[meta(account, true, true)],
                &mut accounts,
            )
            .unwrap_err();
            assert_eq!(
                error,
                LaunchSystemError::ModifiedProgramId { pubkey: account }
            );
            assert_eq!(accounts, before);
        }

        let mut accounts = AccountMap::from([(account, default_system_account())]);
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Assign { owner: [18; 32] },
            &[meta(account, true, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::ModifiedProgramId { pubkey: account }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn transfer_runs_native_checks_before_post_instruction_invariants() {
        let from = [19; 32];
        let to = [20; 32];
        let mut accounts = AccountMap::from([
            (
                from,
                AccountSnapshot {
                    lamports: 3,
                    owner: [21; 32],
                    ..default_system_account()
                },
            ),
            (to, default_system_account()),
        ]);
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Transfer { lamports: 4 },
            &[meta(from, true, false), meta(to, false, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LaunchSystemError::ResultWithNegativeLamports { .. }
        ));
        assert_eq!(accounts, before);

        accounts.get_mut(&from).unwrap().lamports = 5;
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Transfer { lamports: 4 },
            &[meta(from, true, true), meta(to, false, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::ExternalAccountLamportSpend { pubkey: from }
        );
        assert_eq!(accounts, before);

        accounts.get_mut(&from).unwrap().owner = SYSTEM_PROGRAM_ID;
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Transfer { lamports: 1 },
            &[meta(from, true, true), meta(to, false, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::ReadonlyLamportChange { pubkey: to }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn positive_aliased_transfer_checks_signature_before_borrow_conflict() {
        let account = [22; 32];
        let mut accounts = AccountMap::from([(
            account,
            AccountSnapshot {
                lamports: 1,
                ..default_system_account()
            },
        )]);
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Transfer { lamports: 1 },
            &[meta(account, false, true), meta(account, false, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::MissingRequiredSignature { pubkey: account }
        );
        assert_eq!(accounts, before);

        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Transfer { lamports: 1 },
            &[meta(account, true, true), meta(account, true, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::AccountBorrowConflict { pubkey: account }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn stable_epoch_40_system_processor_supports_positive_self_transfer() {
        let account = [23; 32];
        let mut accounts = AccountMap::from([(
            account,
            AccountSnapshot {
                lamports: 1_003_770_000,
                ..default_system_account()
            },
        )]);
        let before = accounts.clone();
        let metas = [meta(account, true, true), meta(account, true, true)];
        let instruction = ArchiveV2SystemInstructionData::Transfer {
            lamports: 1_000_000,
        };

        assert_eq!(
            apply_launch_system_instruction_for_epoch(
                &instruction,
                &[meta(account, false, true), meta(account, false, true)],
                &mut accounts,
                STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
            )
            .unwrap_err(),
            LaunchSystemError::MissingRequiredSignature { pubkey: account }
        );
        assert_eq!(accounts, before);

        assert_eq!(
            apply_launch_system_instruction_for_epoch(
                &instruction,
                &metas,
                &mut accounts,
                STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH - 1,
            )
            .unwrap_err(),
            LaunchSystemError::AccountBorrowConflict { pubkey: account }
        );
        assert_eq!(accounts, before);

        assert_eq!(
            apply_launch_system_instruction_for_epoch(
                &instruction,
                &metas,
                &mut accounts,
                STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
            )
            .unwrap(),
            LaunchSystemMutation::Transfer {
                from: account,
                to: account,
                lamports: 1_000_000,
            }
        );
        assert_eq!(accounts, before);

        accounts.get_mut(&account).unwrap().data.push(1);
        let with_data = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction_for_epoch(
                &instruction,
                &metas,
                &mut accounts,
                STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
            )
            .unwrap_err(),
            LaunchSystemError::SourceCarriesData { pubkey: account }
        );
        assert_eq!(accounts, with_data);

        accounts.get_mut(&account).unwrap().data.clear();
        accounts.get_mut(&account).unwrap().lamports = 999_999;
        let insufficient = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction_for_epoch(
                &instruction,
                &metas,
                &mut accounts,
                STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH,
            )
            .unwrap_err(),
            LaunchSystemError::ResultWithNegativeLamports {
                pubkey: account,
                required: 1_000_000,
            }
        );
        assert_eq!(accounts, insufficient);
    }

    #[test]
    fn release_overflow_is_rejected_by_the_historical_balance_verifier() {
        let from = [23; 32];
        let to = [24; 32];
        let mut accounts = AccountMap::from([
            (
                from,
                AccountSnapshot {
                    lamports: 1,
                    ..default_system_account()
                },
            ),
            (
                to,
                AccountSnapshot {
                    lamports: u64::MAX,
                    ..default_system_account()
                },
            ),
        ]);
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Transfer { lamports: 1 },
            &[meta(from, true, true), meta(to, false, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchSystemError::UnbalancedInstruction {
                pre_lamports: u128::from(u64::MAX) + 1,
                post_lamports: 0,
            }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn failed_transfer_is_atomic() {
        let from = [1; 32];
        let to = [2; 32];
        let mut accounts = AccountMap::from([(
            from,
            AccountSnapshot {
                lamports: 3,
                ..default_system_account()
            },
        )]);
        let before = accounts.clone();
        let error = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::Transfer { lamports: 4 },
            &[meta(from, true, true), meta(to, false, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LaunchSystemError::ResultWithNegativeLamports { .. }
        ));
        assert_eq!(accounts, before);
    }

    #[test]
    fn authorize_nonce_replaces_only_the_initialized_authority() {
        let nonce = [21; 32];
        let old_authority = [22; 32];
        let new_authority = [23; 32];
        let blockhash = [24; 32];
        let mut nonce_account = initialized_nonce_account(old_authority, blockhash, 5_000);
        assert_eq!(&nonce_account.data[..4], &0_u32.to_le_bytes());
        assert_eq!(&nonce_account.data[4..8], &1_u32.to_le_bytes());
        assert_eq!(&nonce_account.data[8..40], &old_authority);
        assert_eq!(&nonce_account.data[40..72], &blockhash);
        assert_eq!(&nonce_account.data[72..80], &5_000_u64.to_le_bytes());
        nonce_account.data.extend_from_slice(&[0xaa, 0xbb]);
        let mut accounts = AccountMap::from([
            (nonce, nonce_account),
            (old_authority, default_system_account()),
        ]);

        let mutation = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::AuthorizeNonceAccount {
                authority: new_authority,
            },
            &[meta(nonce, false, true), meta(old_authority, true, false)],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(
            mutation,
            LaunchSystemMutation::AuthorizeNonce {
                account: nonce,
                old_authority,
                new_authority,
            }
        );
        let versions: LaunchNonceVersions = wincode::deserialize(&accounts[&nonce].data).unwrap();
        assert_eq!(
            versions,
            LaunchNonceVersions::Current(Box::new(LaunchNonceState::Initialized(
                LaunchNonceData {
                    authority: new_authority,
                    blockhash,
                    fee_calculator: LaunchNonceFeeCalculator {
                        lamports_per_signature: 5_000,
                    },
                }
            )))
        );
        assert_eq!(&accounts[&nonce].data[80..], &[0xaa, 0xbb]);
    }

    #[test]
    fn initialize_nonce_uses_first_recent_blockhash_and_launch_wire() {
        let nonce = [40; 32];
        let authority = [41; 32];
        let blockhash = [42; 32];
        let rent = LaunchRent {
            lamports_per_byte_year: 10,
            exemption_threshold: 2.0,
            burn_percent: 100,
        };
        let minimum = rent.minimum_balance(LAUNCH_NONCE_ACCOUNT_DATA_LEN);
        let mut accounts = AccountMap::from([
            (nonce, uninitialized_nonce_account(minimum)),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(vec![LaunchRecentBlockhashEntry {
                    blockhash,
                    fee_calculator: LaunchNonceFeeCalculator {
                        lamports_per_signature: 5_000,
                    },
                }]),
            ),
            (RENT_SYSVAR_ID, rent_account(rent)),
        ]);

        let mutation = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::InitializeNonceAccount { authority },
            &[
                meta(nonce, false, true),
                meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                meta(RENT_SYSVAR_ID, false, false),
            ],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(
            mutation,
            LaunchSystemMutation::InitializeNonce {
                account: nonce,
                authority,
                blockhash,
                lamports_per_signature: 5_000,
            }
        );
        let versions: LaunchNonceVersions = wincode::deserialize(&accounts[&nonce].data).unwrap();
        assert_eq!(
            versions,
            LaunchNonceVersions::Current(Box::new(LaunchNonceState::Initialized(
                LaunchNonceData {
                    authority,
                    blockhash,
                    fee_calculator: LaunchNonceFeeCalculator {
                        lamports_per_signature: 5_000,
                    },
                }
            )))
        );
        assert_eq!(accounts[&nonce].data.len(), LAUNCH_NONCE_ACCOUNT_DATA_LEN);
    }

    #[test]
    fn initialize_nonce_preserves_sysvar_and_state_error_order() {
        let nonce = [43; 32];
        let authority = [44; 32];
        let rent = LaunchRent {
            lamports_per_byte_year: 10,
            exemption_threshold: 2.0,
            burn_percent: 100,
        };
        let instruction = ArchiveV2SystemInstructionData::InitializeNonceAccount { authority };
        let metas = [
            meta(nonce, false, true),
            meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
            meta(RENT_SYSVAR_ID, false, false),
        ];

        let mut accounts = AccountMap::from([
            (nonce, uninitialized_nonce_account(u64::MAX)),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(Vec::new()),
            ),
            (RENT_SYSVAR_ID, rent_account(rent)),
        ]);
        let before = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction(&instruction, &metas, &mut accounts).unwrap_err(),
            LaunchSystemError::NoRecentBlockhashes
        );
        assert_eq!(accounts, before);

        accounts.insert(
            RECENT_BLOCKHASHES_SYSVAR_ID,
            recent_blockhashes_account(vec![LaunchRecentBlockhashEntry {
                blockhash: [45; 32],
                fee_calculator: LaunchNonceFeeCalculator {
                    lamports_per_signature: 1,
                },
            }]),
        );
        accounts.insert(nonce, initialized_nonce_account(authority, [46; 32], 2));
        assert_eq!(
            apply_launch_system_instruction(&instruction, &metas, &mut accounts).unwrap_err(),
            LaunchSystemError::BadNonceAccountState { pubkey: nonce }
        );

        accounts.insert(nonce, uninitialized_nonce_account(0));
        let minimum = rent.minimum_balance(LAUNCH_NONCE_ACCOUNT_DATA_LEN);
        assert_eq!(
            apply_launch_system_instruction(&instruction, &metas, &mut accounts).unwrap_err(),
            LaunchSystemError::NonceInsufficientFunds {
                pubkey: nonce,
                balance: 0,
                minimum,
            }
        );
    }

    #[test]
    fn initialize_nonce_rejects_wrong_or_malformed_sysvars_atomically() {
        let nonce = [47; 32];
        let authority = [48; 32];
        let wrong = [49; 32];
        let instruction = ArchiveV2SystemInstructionData::InitializeNonceAccount { authority };
        let mut accounts = AccountMap::from([(nonce, uninitialized_nonce_account(u64::MAX))]);
        let before = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[
                    meta(nonce, false, true),
                    meta(wrong, false, false),
                    meta(RENT_SYSVAR_ID, false, false),
                ],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::InvalidSysvar {
                position: 1,
                expected: RECENT_BLOCKHASHES_SYSVAR_ID,
                found: wrong,
            }
        );
        assert_eq!(accounts, before);

        accounts.insert(
            RECENT_BLOCKHASHES_SYSVAR_ID,
            AccountSnapshot {
                owner: SYSVAR_OWNER_ID,
                data: vec![0xff].into(),
                ..default_system_account()
            },
        );
        let before = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[
                    meta(nonce, false, true),
                    meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                    meta(RENT_SYSVAR_ID, false, false),
                ],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::InvalidSysvarData { position: 1 }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn advance_nonce_accepts_launch_three_meta_shape_and_preserves_padding() {
        let nonce = [50; 32];
        let authority = [51; 32];
        let old_blockhash = [52; 32];
        let new_blockhash = [53; 32];
        let mut nonce_account = initialized_nonce_account(authority, old_blockhash, 1);
        nonce_account.data.extend_from_slice(&[0xaa, 0xbb]);
        let mut accounts = AccountMap::from([
            (nonce, nonce_account),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(vec![LaunchRecentBlockhashEntry {
                    blockhash: new_blockhash,
                    fee_calculator: LaunchNonceFeeCalculator {
                        lamports_per_signature: 5_000,
                    },
                }]),
            ),
            (authority, default_system_account()),
        ]);

        let mutation = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::AdvanceNonceAccount,
            &[
                meta(nonce, false, true),
                meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                meta(authority, true, false),
            ],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(
            mutation,
            LaunchSystemMutation::AdvanceNonce {
                account: nonce,
                authority,
                old_blockhash,
                new_blockhash,
                lamports_per_signature: 5_000,
            }
        );
        let versions: LaunchNonceVersions = wincode::deserialize(&accounts[&nonce].data).unwrap();
        assert_eq!(
            versions,
            LaunchNonceVersions::Current(Box::new(LaunchNonceState::Initialized(
                LaunchNonceData {
                    authority,
                    blockhash: new_blockhash,
                    fee_calculator: LaunchNonceFeeCalculator {
                        lamports_per_signature: 5_000,
                    },
                }
            )))
        );
        assert_eq!(
            &accounts[&nonce].data[LAUNCH_NONCE_ACCOUNT_DATA_LEN..],
            &[0xaa, 0xbb]
        );
    }

    #[test]
    fn advance_nonce_checks_authority_before_not_expired() {
        let nonce = [54; 32];
        let authority = [55; 32];
        let blockhash = [56; 32];
        let instruction = ArchiveV2SystemInstructionData::AdvanceNonceAccount;
        let mut accounts = AccountMap::from([
            (nonce, initialized_nonce_account(authority, blockhash, 1)),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(vec![LaunchRecentBlockhashEntry {
                    blockhash,
                    fee_calculator: LaunchNonceFeeCalculator {
                        lamports_per_signature: 2,
                    },
                }]),
            ),
            (authority, default_system_account()),
        ]);
        let before = accounts.clone();

        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[
                    meta(nonce, false, true),
                    meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                    meta(authority, false, false),
                ],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::MissingRequiredSignature { pubkey: authority }
        );
        assert_eq!(accounts, before);

        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[
                    meta(nonce, false, true),
                    meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                    meta(authority, true, false),
                ],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::NonceNotExpired {
                pubkey: nonce,
                blockhash,
            }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn advance_nonce_preserves_sysvar_and_state_error_order() {
        let nonce = [57; 32];
        let authority = [58; 32];
        let wrong = [59; 32];
        let new_blockhash = [60; 32];
        let instruction = ArchiveV2SystemInstructionData::AdvanceNonceAccount;
        let malformed_nonce = AccountSnapshot {
            data: vec![0xff; LAUNCH_NONCE_ACCOUNT_DATA_LEN].into(),
            ..default_system_account()
        };
        let mut accounts = AccountMap::from([(nonce, malformed_nonce.clone())]);
        let before = accounts.clone();

        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[meta(nonce, false, true), meta(wrong, false, false)],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::InvalidSysvar {
                position: 1,
                expected: RECENT_BLOCKHASHES_SYSVAR_ID,
                found: wrong,
            }
        );
        assert_eq!(accounts, before);

        accounts.insert(
            RECENT_BLOCKHASHES_SYSVAR_ID,
            AccountSnapshot {
                owner: SYSVAR_OWNER_ID,
                data: vec![0xff].into(),
                ..default_system_account()
            },
        );
        let before = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[
                    meta(nonce, false, true),
                    meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                ],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::InvalidSysvarData { position: 1 }
        );
        assert_eq!(accounts, before);

        accounts.insert(
            RECENT_BLOCKHASHES_SYSVAR_ID,
            recent_blockhashes_account(Vec::new()),
        );
        let before = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[
                    meta(nonce, false, true),
                    meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                ],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::NoRecentBlockhashes
        );
        assert_eq!(accounts, before);

        accounts.insert(
            RECENT_BLOCKHASHES_SYSVAR_ID,
            recent_blockhashes_account(vec![LaunchRecentBlockhashEntry {
                blockhash: new_blockhash,
                fee_calculator: LaunchNonceFeeCalculator {
                    lamports_per_signature: 1,
                },
            }]),
        );
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[
                    meta(nonce, false, true),
                    meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                ],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::InvalidAccountData { pubkey: nonce }
        );

        accounts.insert(nonce, uninitialized_nonce_account(u64::MAX));
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[
                    meta(nonce, false, true),
                    meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                    meta(authority, true, false),
                ],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::BadNonceAccountState { pubkey: nonce }
        );
    }

    #[test]
    fn advance_nonce_readonly_post_verifier_rolls_back() {
        let nonce = [61; 32];
        let authority = [62; 32];
        let old_blockhash = [63; 32];
        let mut accounts = AccountMap::from([
            (
                nonce,
                initialized_nonce_account(authority, old_blockhash, 1),
            ),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(vec![LaunchRecentBlockhashEntry {
                    blockhash: [64; 32],
                    fee_calculator: LaunchNonceFeeCalculator {
                        lamports_per_signature: 2,
                    },
                }]),
            ),
            (authority, default_system_account()),
        ]);
        let before = accounts.clone();

        assert_eq!(
            apply_launch_system_instruction(
                &ArchiveV2SystemInstructionData::AdvanceNonceAccount,
                &[
                    meta(nonce, false, false),
                    meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                    meta(authority, true, false),
                ],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::ReadonlyDataModified { pubkey: nonce }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn authorize_nonce_preserves_launch_error_order_and_atomicity() {
        let nonce = [25; 32];
        let old_authority = [26; 32];
        let new_authority = [27; 32];
        let instruction = ArchiveV2SystemInstructionData::AuthorizeNonceAccount {
            authority: new_authority,
        };

        let mut accounts =
            AccountMap::from([(nonce, initialized_nonce_account(old_authority, [28; 32], 0))]);
        let before = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[meta(nonce, false, true), meta(old_authority, false, false)],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::MissingRequiredSignature {
                pubkey: old_authority,
            }
        );
        assert_eq!(accounts, before);

        let uninitialized = LaunchNonceVersions::Current(Box::new(LaunchNonceState::Uninitialized));
        let mut data = vec![0; LAUNCH_NONCE_ACCOUNT_DATA_LEN];
        let encoded = wincode::serialize(&uninitialized).unwrap();
        data[..encoded.len()].copy_from_slice(&encoded);
        let mut accounts = AccountMap::from([(
            nonce,
            AccountSnapshot {
                data: data.into(),
                ..default_system_account()
            },
        )]);
        let before = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[meta(nonce, false, true)],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::BadNonceAccountState { pubkey: nonce }
        );
        assert_eq!(accounts, before);

        let mut accounts = AccountMap::from([(
            nonce,
            AccountSnapshot {
                data: vec![255; LAUNCH_NONCE_ACCOUNT_DATA_LEN].into(),
                ..default_system_account()
            },
        )]);
        let before = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[meta(nonce, false, true)],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::InvalidAccountData { pubkey: nonce }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn authorize_nonce_data_changes_are_checked_by_the_post_verifier() {
        let nonce = [29; 32];
        let authority = [30; 32];
        let instruction = ArchiveV2SystemInstructionData::AuthorizeNonceAccount {
            authority: [31; 32],
        };

        let account = initialized_nonce_account(authority, [32; 32], 42);
        let mut accounts = AccountMap::from([(nonce, account.clone())]);
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[meta(nonce, false, false), meta(authority, true, false)],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::ReadonlyDataModified { pubkey: nonce }
        );
        assert_eq!(accounts, AccountMap::from([(nonce, account.clone())]));

        let external = AccountSnapshot {
            owner: [33; 32],
            ..account
        };
        let mut accounts = AccountMap::from([(nonce, external.clone())]);
        assert_eq!(
            apply_launch_system_instruction(
                &instruction,
                &[meta(nonce, false, true), meta(authority, true, false)],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::ExternalAccountDataModified { pubkey: nonce }
        );
        assert_eq!(accounts, AccountMap::from([(nonce, external)]));
    }

    #[test]
    fn same_authority_nonce_noop_succeeds_for_readonly_external_account() {
        let nonce = [34; 32];
        let authority = [35; 32];
        let external = AccountSnapshot {
            owner: [36; 32],
            ..initialized_nonce_account(authority, [37; 32], 99)
        };
        let mut accounts = AccountMap::from([
            (nonce, external.clone()),
            (authority, default_system_account()),
        ]);

        let mutation = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::AuthorizeNonceAccount { authority },
            &[meta(nonce, false, false), meta(authority, true, false)],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(accounts[&nonce], external);
        assert_eq!(
            mutation,
            LaunchSystemMutation::AuthorizeNonce {
                account: nonce,
                old_authority: authority,
                new_authority: authority,
            }
        );
    }

    #[test]
    fn withdraw_uninitialized_nonce_uses_the_nonce_signer_and_moves_lamports() {
        let nonce = [38; 32];
        let destination = [39; 32];
        let nonce_account = uninitialized_nonce_account(1_000);
        let nonce_data = nonce_account.data.clone();
        let mut accounts = AccountMap::from([
            (nonce, nonce_account),
            (
                destination,
                AccountSnapshot {
                    lamports: 9,
                    ..default_system_account()
                },
            ),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(Vec::new()),
            ),
            (
                RENT_SYSVAR_ID,
                rent_account(LaunchRent {
                    lamports_per_byte_year: 1,
                    exemption_threshold: 2.0,
                    burn_percent: 0,
                }),
            ),
        ]);

        let mutation = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports: 600 },
            &[
                meta(nonce, true, true),
                meta(destination, false, true),
                meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                meta(RENT_SYSVAR_ID, false, false),
            ],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(accounts[&nonce].lamports, 400);
        assert_eq!(accounts[&nonce].data, nonce_data);
        assert_eq!(accounts[&destination].lamports, 609);
        assert_eq!(
            mutation,
            LaunchSystemMutation::WithdrawNonce {
                account: nonce,
                destination,
                signer: nonce,
                lamports: 600,
            }
        );
    }

    #[test]
    fn epoch_49_partial_nonce_withdrawal_matches_compact_evidence() {
        let nonce = pubkey("6ces8oAcd36FZgkyHPLfaEatYUXF2WZeQ7szodc159eB");
        let destination_and_authority = pubkey("ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ");
        let stored_blockhash = pubkey("5nZwm7kWTAkqR6agFVRJxJMnMineiAR3G1oh4eAELbz8");
        let mut nonce_account =
            initialized_nonce_account(destination_and_authority, stored_blockhash, 5_000);
        nonce_account.lamports = 9_111_091_110;
        let nonce_data = nonce_account.data.clone();
        assert_eq!(
            Sha256::digest(&nonce_data).as_slice(),
            [
                0x9b, 0x2e, 0x0c, 0x49, 0xf9, 0x1c, 0x57, 0x6d, 0x45, 0x04, 0x9a, 0x23, 0x24, 0x52,
                0x9a, 0x96, 0x20, 0x10, 0x3c, 0x4b, 0xb1, 0x2c, 0x4b, 0x96, 0x92, 0x5b, 0x84, 0xed,
                0x3e, 0x6b, 0xa0, 0x3f,
            ]
        );
        let mut accounts = AccountMap::from([
            (nonce, nonce_account),
            (
                destination_and_authority,
                AccountSnapshot {
                    lamports: 2_877_888_890,
                    ..default_system_account()
                },
            ),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(Vec::new()),
            ),
            (
                RENT_SYSVAR_ID,
                rent_account(LaunchRent {
                    lamports_per_byte_year: 3_480,
                    exemption_threshold: 2.0,
                    burn_percent: 50,
                }),
            ),
        ]);

        let mutation = apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::WithdrawNonceAccount {
                lamports: 1_200_000_000,
            },
            &[
                meta(nonce, true, true),
                meta(destination_and_authority, true, true),
                meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                meta(RENT_SYSVAR_ID, false, false),
            ],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(accounts[&nonce].lamports, 7_911_091_110);
        assert_eq!(accounts[&destination_and_authority].lamports, 4_077_888_890);
        assert_eq!(accounts[&nonce].data, nonce_data);
        assert_eq!(
            mutation,
            LaunchSystemMutation::WithdrawNonce {
                account: nonce,
                destination: destination_and_authority,
                signer: destination_and_authority,
                lamports: 1_200_000_000,
            }
        );
    }

    #[test]
    fn withdraw_initialized_nonce_preserves_state_and_rent_reserve() {
        let nonce = [40; 32];
        let destination = [41; 32];
        let authority = [42; 32];
        let mut nonce_account = initialized_nonce_account(authority, [43; 32], 5_000);
        nonce_account.lamports = 1_000;
        let nonce_data = nonce_account.data.clone();
        let mut accounts = AccountMap::from([
            (nonce, nonce_account),
            (destination, default_system_account()),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(Vec::new()),
            ),
            (
                RENT_SYSVAR_ID,
                rent_account(LaunchRent {
                    lamports_per_byte_year: 1,
                    exemption_threshold: 2.0,
                    burn_percent: 0,
                }),
            ),
            (authority, default_system_account()),
        ]);
        let metas = [
            meta(nonce, false, true),
            meta(destination, false, true),
            meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
            meta(RENT_SYSVAR_ID, false, false),
            meta(authority, true, false),
        ];

        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports: 500 },
            &metas,
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts[&nonce].lamports, 500);
        assert_eq!(accounts[&nonce].data, nonce_data);
        assert_eq!(accounts[&destination].lamports, 500);

        let before = accounts.clone();
        assert_eq!(
            apply_launch_system_instruction(
                &ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports: 85 },
                &metas,
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::InsufficientFunds {
                pubkey: nonce,
                balance: 500,
                required: 501,
            }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn full_initialized_nonce_withdrawal_requires_expiry_but_keeps_data() {
        let nonce = [44; 32];
        let destination = [45; 32];
        let authority = [46; 32];
        let durable_blockhash = [47; 32];
        let mut nonce_account = initialized_nonce_account(authority, durable_blockhash, 1);
        nonce_account.lamports = 1_000;
        let nonce_data = nonce_account.data.clone();
        let mut accounts = AccountMap::from([
            (nonce, nonce_account),
            (destination, default_system_account()),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(vec![LaunchRecentBlockhashEntry {
                    blockhash: durable_blockhash,
                    fee_calculator: LaunchNonceFeeCalculator {
                        lamports_per_signature: 2,
                    },
                }]),
            ),
            (
                RENT_SYSVAR_ID,
                rent_account(LaunchRent {
                    lamports_per_byte_year: 1,
                    exemption_threshold: 2.0,
                    burn_percent: 0,
                }),
            ),
            (authority, default_system_account()),
        ]);
        let metas = [
            meta(nonce, false, true),
            meta(destination, false, true),
            meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
            meta(RENT_SYSVAR_ID, false, false),
            meta(authority, true, false),
        ];
        let before = accounts.clone();

        assert_eq!(
            apply_launch_system_instruction(
                &ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports: 1_000 },
                &metas,
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::NonceNotExpired {
                pubkey: nonce,
                blockhash: durable_blockhash,
            }
        );
        assert_eq!(accounts, before);

        accounts.insert(
            RECENT_BLOCKHASHES_SYSVAR_ID,
            recent_blockhashes_account(vec![LaunchRecentBlockhashEntry {
                blockhash: [48; 32],
                fee_calculator: LaunchNonceFeeCalculator {
                    lamports_per_signature: 2,
                },
            }]),
        );
        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports: 1_000 },
            &metas,
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts[&nonce].lamports, 0);
        assert_eq!(accounts[&nonce].data, nonce_data);
        assert_eq!(accounts[&destination].lamports, 1_000);
    }

    #[test]
    fn withdraw_nonce_allows_repeated_source_and_destination_index() {
        let nonce = [49; 32];
        let nonce_account = uninitialized_nonce_account(1_000);
        let mut accounts = AccountMap::from([
            (nonce, nonce_account.clone()),
            (
                RECENT_BLOCKHASHES_SYSVAR_ID,
                recent_blockhashes_account(Vec::new()),
            ),
            (
                RENT_SYSVAR_ID,
                rent_account(LaunchRent {
                    lamports_per_byte_year: 1,
                    exemption_threshold: 2.0,
                    burn_percent: 0,
                }),
            ),
        ]);

        apply_launch_system_instruction(
            &ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports: 500 },
            &[
                meta(nonce, true, true),
                meta(nonce, false, true),
                meta(RECENT_BLOCKHASHES_SYSVAR_ID, false, false),
                meta(RENT_SYSVAR_ID, false, false),
            ],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts[&nonce], nonce_account);
    }

    #[test]
    fn missing_nonce_accounts_and_post_launch_variants_fail_explicitly() {
        let mut accounts = AccountMap::new();
        assert_eq!(
            apply_launch_system_instruction(
                &ArchiveV2SystemInstructionData::AdvanceNonceAccount,
                &[],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::MissingAccount { position: 0 }
        );
        assert_eq!(
            apply_launch_system_instruction(
                &ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports: 1 },
                &[],
                &mut accounts,
            )
            .unwrap_err(),
            LaunchSystemError::MissingAccount { position: 0 }
        );
        assert!(matches!(
            apply_launch_system_instruction(
                &ArchiveV2SystemInstructionData::UpgradeNonceAccount,
                &[],
                &mut accounts,
            ),
            Err(LaunchSystemError::PostLaunchVariant { .. })
        ));
    }
}
