//! Launch-era native Stake-program primitives needed by the early mainnet
//! replay prefix.
//!
//! The state and instruction layouts follow Solana v1.0.7, with corpus-bound
//! native-program transitions applied when later releases changed semantics.
//! All seven launch-era variants are implemented, together with v1.3.3's
//! Merge transition: Initialize, DelegateStake, Split, Authorize, Withdraw,
//! Deactivate, SetLockup, and Merge.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AccountMap, AccountSnapshot, CowAccountMap, LaunchAccountMeta, MemoryAccountStore,
    RENT_SYSVAR_ID, decode_launch_vote_credits, default_system_account,
};

pub const STAKE_PROGRAM_ID: [u8; 32] = [
    6, 161, 216, 23, 145, 55, 84, 42, 152, 52, 55, 189, 254, 42, 122, 178, 85, 127, 83, 92, 138,
    120, 114, 43, 104, 164, 157, 192, 0, 0, 0, 0,
];
pub const LAUNCH_STAKE_ACCOUNT_DATA_LEN: usize = 200;
// Solana v1.1.3 exempted Staker changes from lockup, then v1.1.6 removed the
// remaining Withdrawer lockup check and retained Clock only as a reserved,
// ignored meta. This is the exact first Compact transaction whose success
// depends on both behaviors; the preceding corpus is transition-equivalent
// under the v1.0.7 implementation.
pub(crate) const STAKE_AUTHORIZE_LOCKUP_REMOVAL_SLOT: u64 = 11_030_629;
pub const CLOCK_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 24, 199, 116, 201, 40, 86, 99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155,
    75, 109, 92, 115, 85, 91, 33, 0, 0, 0, 0,
];
pub const STAKE_HISTORY_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 25, 53, 132, 208, 254, 237, 155, 179, 67, 29, 19, 32, 107, 229, 68, 40, 27,
    87, 184, 86, 108, 197, 55, 95, 244, 0, 0, 0,
];
/// `StakeConfig11111111111111111111111111111111`.
pub const STAKE_CONFIG_ID: [u8; 32] = [
    6, 161, 216, 23, 165, 2, 5, 11, 104, 7, 145, 230, 206, 109, 184, 142, 30, 91, 113, 80, 246, 31,
    198, 121, 10, 78, 180, 209, 0, 0, 0, 0,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaunchClock {
    pub slot: u64,
    pub epoch: u64,
    pub unix_timestamp: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LaunchStakeHistoryEntry {
    pub effective: u64,
    pub activating: u64,
    pub deactivating: u64,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
struct LaunchStakeLockupArgs {
    unix_timestamp: Option<i64>,
    epoch: Option<u64>,
    custodian: Option<[u8; 32]>,
}

pub type LaunchStakeHistory = BTreeMap<u64, LaunchStakeHistoryEntry>;

#[derive(
    Debug, Clone, Copy, PartialEq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
struct LaunchStakeRent {
    lamports_per_byte_year: u64,
    exemption_threshold: f64,
    #[allow(dead_code)]
    burn_percent: u8,
}

impl LaunchStakeRent {
    fn minimum_balance(self, data_len: usize) -> u64 {
        const ACCOUNT_STORAGE_OVERHEAD: u64 = 128;
        ((ACCOUNT_STORAGE_OVERHEAD
            .wrapping_add(data_len as u64)
            .wrapping_mul(self.lamports_per_byte_year)) as f64
            * self.exemption_threshold) as u64
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
struct LaunchStakeClockWire {
    slot: u64,
    #[allow(dead_code)]
    segment: u64,
    epoch: u64,
    #[allow(dead_code)]
    leader_schedule_epoch: u64,
    unix_timestamp: i64,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
struct LaunchStakeHistoryEntryWire {
    effective: u64,
    activating: u64,
    deactivating: u64,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
struct LaunchStakeHistoryWire(Vec<(u64, LaunchStakeHistoryEntryWire)>);

#[derive(
    Debug, Clone, Copy, PartialEq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
struct LaunchStakeConfig {
    warmup_cooldown_rate: f64,
    #[allow(dead_code)]
    slash_penalty: u8,
}

#[derive(Debug, Clone, Copy)]
pub struct LaunchStakeContext<'a> {
    pub clock: LaunchClock,
    pub stake_history: &'a LaunchStakeHistory,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
pub enum LaunchStakeAuthorize {
    Staker,
    Withdrawer,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
pub struct LaunchStakeAuthorized {
    pub staker: [u8; 32],
    pub withdrawer: [u8; 32],
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
pub struct LaunchStakeLockup {
    pub unix_timestamp: i64,
    pub epoch: u64,
    pub custodian: [u8; 32],
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
pub struct LaunchStakeMeta {
    pub rent_exempt_reserve: u64,
    pub authorized: LaunchStakeAuthorized,
    pub lockup: LaunchStakeLockup,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub struct LaunchDelegation {
    pub voter_pubkey: [u8; 32],
    pub stake: u64,
    pub activation_epoch: u64,
    pub deactivation_epoch: u64,
    pub warmup_cooldown_rate: f64,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub struct LaunchStake {
    pub delegation: LaunchDelegation,
    pub credits_observed: u64,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub enum LaunchStakeState {
    Uninitialized,
    Initialized(LaunchStakeMeta),
    Stake(LaunchStakeMeta, LaunchStake),
    RewardsPool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchStakeMutation {
    Initialize {
        stake_account: [u8; 32],
        rent_exempt_reserve: u64,
        authorized: LaunchStakeAuthorized,
        lockup: LaunchStakeLockup,
    },
    Delegate {
        stake_account: [u8; 32],
        vote_account: [u8; 32],
        delegated_lamports: u64,
        activation_epoch: u64,
        credits_observed: u64,
    },
    Split {
        source: [u8; 32],
        destination: [u8; 32],
        lamports: u64,
    },
    Authorize {
        stake_account: [u8; 32],
        new_authority: [u8; 32],
        authority_type: LaunchStakeAuthorize,
    },
    Withdraw {
        stake_account: [u8; 32],
        destination: [u8; 32],
        lamports: u64,
        full_withdrawal: bool,
    },
    Deactivate {
        stake_account: [u8; 32],
        deactivation_epoch: u64,
    },
    SetLockup {
        stake_account: [u8; 32],
        lockup: LaunchStakeLockup,
    },
    Merge {
        destination: [u8; 32],
        source: [u8; 32],
        lamports: u64,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LaunchStakeError {
    #[error("stake instruction is missing account position {position}")]
    MissingAccount { position: usize },
    #[error("stake instruction data is malformed")]
    InvalidInstructionData,
    #[error("launch Stake variant {discriminant} is not implemented")]
    UnsupportedVariant { discriminant: u32 },
    #[error("stake instruction account {position} is {found:?}, expected sysvar {expected:?}")]
    InvalidSysvar {
        position: usize,
        expected: [u8; 32],
        found: [u8; 32],
    },
    #[error("stake account {pubkey:?} has invalid state data")]
    InvalidAccountData { pubkey: [u8; 32] },
    #[error("stake instruction sysvar account {position} ({pubkey:?}) has invalid data")]
    InvalidSysvarData { position: usize, pubkey: [u8; 32] },
    #[error("stake instruction account {position} is {found:?}, expected config {expected:?}")]
    InvalidConfigAccount {
        position: usize,
        expected: [u8; 32],
        found: [u8; 32],
    },
    #[error("stake config account {pubkey:?} has invalid data")]
    InvalidConfigData { pubkey: [u8; 32] },
    #[error("vote account {pubkey:?} has invalid launch-era state")]
    InvalidVoteAccount { pubkey: [u8; 32] },
    #[error("stake account {pubkey:?} is too small for serialized state")]
    AccountDataTooSmall { pubkey: [u8; 32] },
    #[error("stake authority {pubkey:?} did not sign")]
    MissingRequiredSignature { pubkey: [u8; 32] },
    #[error("stake account {pubkey:?} has insufficient funds")]
    InsufficientFunds { pubkey: [u8; 32] },
    #[error("split amount exceeds delegated stake in account {pubkey:?}")]
    InsufficientStake { pubkey: [u8; 32] },
    #[error("stake account {pubkey:?} cannot be redelegated while effective")]
    TooSoonToRedelegate { pubkey: [u8; 32] },
    #[error("stake account {pubkey:?} is already deactivated")]
    AlreadyDeactivated { pubkey: [u8; 32] },
    #[error("stake account {pubkey:?} cannot be merged while effective")]
    MergeActivatedStake { pubkey: [u8; 32] },
    #[error("stake accounts {destination:?} and {source_pubkey:?} have incompatible metadata")]
    MergeMismatch {
        destination: [u8; 32],
        source_pubkey: [u8; 32],
    },
    #[error("stake lockup remains in force for account {pubkey:?}")]
    LockupInForce { pubkey: [u8; 32] },
    #[error("Stake program modified the owner of account {pubkey:?}")]
    ModifiedProgramId { pubkey: [u8; 32] },
    #[error("Stake program spent lamports from externally owned account {pubkey:?}")]
    ExternalAccountLamportSpend { pubkey: [u8; 32] },
    #[error("read-only account {pubkey:?} changed lamports")]
    ReadonlyLamportChange { pubkey: [u8; 32] },
    #[error("Stake program resized account {pubkey:?}")]
    AccountDataSizeChanged { pubkey: [u8; 32] },
    #[error("read-only account {pubkey:?} changed data")]
    ReadonlyDataModified { pubkey: [u8; 32] },
    #[error("Stake program changed data in externally owned account {pubkey:?}")]
    ExternalAccountDataModified { pubkey: [u8; 32] },
    #[error("Stake program made an invalid executable change to account {pubkey:?}")]
    ExecutableModified { pubkey: [u8; 32] },
    #[error("Stake program changed rent_epoch on account {pubkey:?}")]
    RentEpochModified { pubkey: [u8; 32] },
    #[error("Stake instruction is unbalanced: pre={pre_lamports}, post={post_lamports}")]
    UnbalancedInstruction {
        pre_lamports: u128,
        post_lamports: u128,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodedStakeInstruction {
    Initialize {
        authorized: LaunchStakeAuthorized,
        lockup: LaunchStakeLockup,
    },
    Authorize {
        new_authority: [u8; 32],
        authority_type: LaunchStakeAuthorize,
    },
    Split {
        lamports: u64,
    },
    Withdraw {
        lamports: u64,
    },
    DelegateStake,
    Deactivate,
    SetLockup {
        lockup: LaunchStakeLockupArgs,
    },
    Merge,
}

#[derive(Debug, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
enum LaunchStakeInstructionV100 {
    Initialize(LaunchStakeAuthorized, LaunchStakeLockup),
    Authorize([u8; 32], LaunchStakeAuthorize),
    DelegateStake,
    Split(u64),
    Withdraw(u64),
    Deactivate,
    SetLockup(LaunchStakeLockupArgs),
    Merge,
}

pub fn decode_launch_stake_state(
    pubkey: [u8; 32],
    data: &[u8],
) -> Result<LaunchStakeState, LaunchStakeError> {
    wincode::deserialize(data).map_err(|_| LaunchStakeError::InvalidAccountData { pubkey })
}

/// Rebuild the `Stakes::clone_with_epoch()` aggregate written to StakeHistory
/// when a launch Bank first enters a new epoch.
///
/// v1.0.7's stake cache ignores short Stake-owned accounts and tracks only the
/// delegation carried by nonzero, 200-byte Stake accounts. Because replay
/// stops on every unsupported Stake mutation, scanning canonical state here is
/// equivalent to advancing the cache for the implemented prefix.
pub fn launch_stake_history_entry(
    epoch: u64,
    accounts: &MemoryAccountStore,
    history: &LaunchStakeHistory,
) -> Result<LaunchStakeHistoryEntry, LaunchStakeError> {
    let mut aggregate = LaunchStakeHistoryEntry::default();
    let mut decode_error = None;
    accounts.visit_sorted(&mut |pubkey, account| {
        if decode_error.is_some() {
            return;
        }
        if account.owner != STAKE_PROGRAM_ID
            || account.lamports == 0
            || account.data.len() < LAUNCH_STAKE_ACCOUNT_DATA_LEN
        {
            return;
        }
        let state = match decode_launch_stake_state(pubkey, &account.data) {
            Ok(state) => state,
            Err(error) => {
                decode_error = Some(error);
                return;
            }
        };
        let LaunchStakeState::Stake(_, stake) = state else {
            return;
        };
        let (effective, activating, deactivating) =
            delegation_activation_status(&stake.delegation, epoch, history);
        aggregate.effective = aggregate.effective.wrapping_add(effective);
        aggregate.activating = aggregate.activating.wrapping_add(activating);
        aggregate.deactivating = aggregate.deactivating.wrapping_add(deactivating);
    });
    decode_error.map_or(Ok(aggregate), Err)
}

/// Apply one v1.0.7 Stake instruction atomically to a transaction overlay.
///
/// Instruction-atomic for external callers. Replay uses
/// [`apply_launch_stake_instruction_in_place`] on a disposable overlay.
pub fn apply_launch_stake_instruction(
    data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    context: LaunchStakeContext<'_>,
) -> Result<LaunchStakeMutation, LaunchStakeError> {
    let mut working = CowAccountMap::detached(accounts.clone());
    let mutation =
        apply_launch_stake_instruction_on_overlay(data, account_metas, &mut working, context)?;
    *accounts = working.into_local();
    Ok(mutation)
}

/// Replay-only fast path. On error `accounts` may be partially mutated and
/// must be discarded with the transaction overlay.
pub fn apply_launch_stake_instruction_in_place(
    data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    context: LaunchStakeContext<'_>,
) -> Result<LaunchStakeMutation, LaunchStakeError> {
    let mut cow = CowAccountMap::detached(std::mem::take(accounts));
    let result = apply_launch_stake_instruction_on_overlay(data, account_metas, &mut cow, context);
    *accounts = cow.into_local();
    result
}

pub fn apply_launch_stake_instruction_on_overlay(
    data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut CowAccountMap,
    context: LaunchStakeContext<'_>,
) -> Result<LaunchStakeMutation, LaunchStakeError> {
    accounts.materialize_writable(
        account_metas
            .iter()
            .map(|meta| (meta.pubkey, meta.is_writable)),
        default_system_account,
    );
    // Missing accounts retain the launch/native default-account behavior, but
    // parent-backed readonly sysvars and verifier baselines stay borrowed.
    for meta in account_metas {
        if !accounts.contains_key(&meta.pubkey) {
            accounts.insert(meta.pubkey, default_system_account());
        }
    }
    let pre_accounts = launch_pre_accounts(account_metas, accounts);
    let mutation = apply_inner(data, account_metas, accounts, context)?;
    verify_launch_stake_instruction(&pre_accounts, accounts)?;
    Ok(mutation)
}

fn apply_inner(
    data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut CowAccountMap,
    context: LaunchStakeContext<'_>,
) -> Result<LaunchStakeMutation, LaunchStakeError> {
    // v1.0.7 obtains the first keyed account before deserializing data.
    required_meta(account_metas, 0)?;
    let instruction = decode_instruction(data)?;
    let signers = account_metas
        .iter()
        .filter(|meta| meta.is_signer)
        .map(|meta| meta.pubkey)
        .collect::<BTreeSet<_>>();
    match instruction {
        DecodedStakeInstruction::Initialize { authorized, lockup } => {
            let stake_account = required_meta(account_metas, 0)?;
            // `Rent::from_keyed_account()` is evaluated before Stake state is
            // decoded by `StakeAccount::initialize()`.
            let rent = read_rent(account_metas, accounts, 1)?;
            let rent_exempt_reserve =
                initialize(accounts, stake_account, authorized, lockup, rent)?;
            Ok(LaunchStakeMutation::Initialize {
                stake_account: stake_account.pubkey,
                rent_exempt_reserve,
                authorized,
                lockup,
            })
        }
        DecodedStakeInstruction::DelegateStake => {
            let stake_account = required_meta(account_metas, 0)?;
            let vote_account = required_meta(account_metas, 1)?;
            // Preserve v1.0.7 argument evaluation: all Bank/config inputs are
            // decoded before Stake state, authority, and Vote state checks.
            let clock = read_clock(account_metas, accounts, 2)?;
            let stake_history = read_stake_history(account_metas, accounts, 3)?;
            let config = read_stake_config(account_metas, accounts, 4)?;
            let (delegated_lamports, credits_observed) = delegate(
                accounts,
                stake_account,
                vote_account,
                &signers,
                clock,
                &stake_history,
                config,
            )?;
            Ok(LaunchStakeMutation::Delegate {
                stake_account: stake_account.pubkey,
                vote_account: vote_account.pubkey,
                delegated_lamports,
                activation_epoch: clock.epoch,
                credits_observed,
            })
        }
        DecodedStakeInstruction::Split { lamports } => {
            let source = required_meta(account_metas, 0)?;
            let destination = required_meta(account_metas, 1)?;
            split(accounts, source, destination, lamports, &signers)?;
            Ok(LaunchStakeMutation::Split {
                source: source.pubkey,
                destination: destination.pubkey,
                lamports,
            })
        }
        DecodedStakeInstruction::Authorize {
            new_authority,
            authority_type,
        } => {
            let stake_account = required_meta(account_metas, 0)?;
            let enforce_lockup = context.clock.slot < STAKE_AUTHORIZE_LOCKUP_REMOVAL_SLOT;
            if enforce_lockup {
                validate_sysvar(account_metas, 1, CLOCK_SYSVAR_ID)?;
            }
            authorize(
                accounts,
                stake_account,
                new_authority,
                authority_type,
                &signers,
                context.clock,
                enforce_lockup,
            )?;
            Ok(LaunchStakeMutation::Authorize {
                stake_account: stake_account.pubkey,
                new_authority,
                authority_type,
            })
        }
        DecodedStakeInstruction::Withdraw { lamports } => {
            let stake_account = required_meta(account_metas, 0)?;
            let destination = required_meta(account_metas, 1)?;
            validate_sysvar(account_metas, 2, CLOCK_SYSVAR_ID)?;
            validate_sysvar(account_metas, 3, STAKE_HISTORY_SYSVAR_ID)?;
            let full_withdrawal = withdraw(
                accounts,
                stake_account,
                destination,
                lamports,
                &signers,
                context,
            )?;
            Ok(LaunchStakeMutation::Withdraw {
                stake_account: stake_account.pubkey,
                destination: destination.pubkey,
                lamports,
                full_withdrawal,
            })
        }
        DecodedStakeInstruction::Deactivate => {
            let stake_account = required_meta(account_metas, 0)?;
            // v1.0.7 evaluates `Clock::from_keyed_account()` before decoding
            // Stake state or checking the staker signature.
            let clock = read_clock(account_metas, accounts, 1)?;
            deactivate(accounts, stake_account, &signers, clock.epoch)?;
            Ok(LaunchStakeMutation::Deactivate {
                stake_account: stake_account.pubkey,
                deactivation_epoch: clock.epoch,
            })
        }
        DecodedStakeInstruction::SetLockup { lockup } => {
            let stake_account = required_meta(account_metas, 0)?;
            // v1.0.7 SetLockup has no Clock input. It always requires the
            // *current* custodian, even after the existing lockup has expired;
            // a withdrawer signature is not an alternative authority.
            let lockup = set_lockup(accounts, stake_account, lockup, &signers)?;
            Ok(LaunchStakeMutation::SetLockup {
                stake_account: stake_account.pubkey,
                lockup,
            })
        }
        DecodedStakeInstruction::Merge => {
            let destination = required_meta(account_metas, 0)?;
            let source = required_meta(account_metas, 1)?;
            // v1.3.3 constructs both sysvars before entering StakeAccount::merge,
            // so malformed Bank inputs win over state and authority errors.
            let clock = read_clock(account_metas, accounts, 2)?;
            let stake_history = read_stake_history(account_metas, accounts, 3)?;
            let lamports = merge(
                accounts,
                destination,
                source,
                &signers,
                clock.epoch,
                &stake_history,
            )?;
            Ok(LaunchStakeMutation::Merge {
                destination: destination.pubkey,
                source: source.pubkey,
                lamports,
            })
        }
    }
}

fn decode_instruction(data: &[u8]) -> Result<DecodedStakeInstruction, LaunchStakeError> {
    if data.len() > 1_232 {
        return Err(LaunchStakeError::InvalidInstructionData);
    }
    let instruction = wincode::config::deserialize(
        data,
        wincode::config::Configuration::default().with_fixint_encoding(),
    )
    .map_err(|_| LaunchStakeError::InvalidInstructionData)?;
    Ok(match instruction {
        LaunchStakeInstructionV100::Authorize(new_authority, authority_type) => {
            DecodedStakeInstruction::Authorize {
                new_authority,
                authority_type,
            }
        }
        LaunchStakeInstructionV100::Split(lamports) => DecodedStakeInstruction::Split { lamports },
        LaunchStakeInstructionV100::Withdraw(lamports) => {
            DecodedStakeInstruction::Withdraw { lamports }
        }
        LaunchStakeInstructionV100::Initialize(authorized, lockup) => {
            DecodedStakeInstruction::Initialize { authorized, lockup }
        }
        LaunchStakeInstructionV100::DelegateStake => DecodedStakeInstruction::DelegateStake,
        LaunchStakeInstructionV100::Deactivate => DecodedStakeInstruction::Deactivate,
        LaunchStakeInstructionV100::SetLockup(lockup) => {
            DecodedStakeInstruction::SetLockup { lockup }
        }
        LaunchStakeInstructionV100::Merge => DecodedStakeInstruction::Merge,
    })
}

fn required_meta(
    account_metas: &[LaunchAccountMeta],
    position: usize,
) -> Result<&LaunchAccountMeta, LaunchStakeError> {
    account_metas
        .get(position)
        .ok_or(LaunchStakeError::MissingAccount { position })
}

fn validate_sysvar(
    account_metas: &[LaunchAccountMeta],
    position: usize,
    expected: [u8; 32],
) -> Result<(), LaunchStakeError> {
    let meta = required_meta(account_metas, position)?;
    if meta.pubkey != expected {
        return Err(LaunchStakeError::InvalidSysvar {
            position,
            expected,
            found: meta.pubkey,
        });
    }
    Ok(())
}

fn read_rent(
    account_metas: &[LaunchAccountMeta],
    accounts: &CowAccountMap,
    position: usize,
) -> Result<LaunchStakeRent, LaunchStakeError> {
    validate_sysvar(account_metas, position, RENT_SYSVAR_ID)?;
    let pubkey = account_metas[position].pubkey;
    let account = accounts
        .get(&pubkey)
        .expect("instruction accounts were materialized before Stake dispatch");
    wincode::deserialize(&account.data)
        .map_err(|_| LaunchStakeError::InvalidSysvarData { position, pubkey })
}

fn read_clock(
    account_metas: &[LaunchAccountMeta],
    accounts: &CowAccountMap,
    position: usize,
) -> Result<LaunchClock, LaunchStakeError> {
    validate_sysvar(account_metas, position, CLOCK_SYSVAR_ID)?;
    let pubkey = account_metas[position].pubkey;
    let account = accounts
        .get(&pubkey)
        .expect("instruction accounts were materialized before Stake dispatch");
    let clock: LaunchStakeClockWire = wincode::deserialize(&account.data)
        .map_err(|_| LaunchStakeError::InvalidSysvarData { position, pubkey })?;
    Ok(LaunchClock {
        slot: clock.slot,
        epoch: clock.epoch,
        unix_timestamp: clock.unix_timestamp,
    })
}

fn read_stake_history(
    account_metas: &[LaunchAccountMeta],
    accounts: &CowAccountMap,
    position: usize,
) -> Result<LaunchStakeHistory, LaunchStakeError> {
    validate_sysvar(account_metas, position, STAKE_HISTORY_SYSVAR_ID)?;
    let pubkey = account_metas[position].pubkey;
    let account = accounts
        .get(&pubkey)
        .expect("instruction accounts were materialized before Stake dispatch");
    let history: LaunchStakeHistoryWire = wincode::deserialize(&account.data)
        .map_err(|_| LaunchStakeError::InvalidSysvarData { position, pubkey })?;
    Ok(history
        .0
        .into_iter()
        .map(|(epoch, entry)| {
            (
                epoch,
                LaunchStakeHistoryEntry {
                    effective: entry.effective,
                    activating: entry.activating,
                    deactivating: entry.deactivating,
                },
            )
        })
        .collect())
}

fn read_stake_config(
    account_metas: &[LaunchAccountMeta],
    accounts: &CowAccountMap,
    position: usize,
) -> Result<LaunchStakeConfig, LaunchStakeError> {
    let meta = required_meta(account_metas, position)?;
    if meta.pubkey != STAKE_CONFIG_ID {
        return Err(LaunchStakeError::InvalidConfigAccount {
            position,
            expected: STAKE_CONFIG_ID,
            found: meta.pubkey,
        });
    }
    let account = accounts
        .get(&meta.pubkey)
        .expect("instruction accounts were materialized before Stake dispatch");
    let payload =
        launch_config_payload(&account.data).ok_or(LaunchStakeError::InvalidConfigData {
            pubkey: meta.pubkey,
        })?;
    wincode::deserialize(payload).map_err(|_| LaunchStakeError::InvalidConfigData {
        pubkey: meta.pubkey,
    })
}

/// Return the config payload following v1.0.7's short-vec encoded
/// `ConfigKeys`. Every key is a fixed 32-byte pubkey followed by a bincode
/// boolean.
fn launch_config_payload(data: &[u8]) -> Option<&[u8]> {
    let mut key_count = 0_usize;
    let mut prefix_len = None;
    for index in 0..3 {
        let byte = *data.get(index)?;
        key_count |= usize::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            prefix_len = Some(index + 1);
            break;
        }
    }
    let prefix_len = prefix_len?;
    if key_count > usize::from(u16::MAX) {
        return None;
    }
    let keys_len = key_count.checked_mul(33)?;
    let payload_offset = prefix_len.checked_add(keys_len)?;
    let keys = data.get(prefix_len..payload_offset)?;
    if keys.chunks_exact(33).any(|entry| entry[32] > 1) {
        return None;
    }
    data.get(payload_offset..)
}

fn decode_account(
    accounts: &CowAccountMap,
    pubkey: [u8; 32],
) -> Result<LaunchStakeState, LaunchStakeError> {
    let account = accounts
        .get(&pubkey)
        .expect("instruction accounts were materialized before Stake dispatch");
    decode_launch_stake_state(pubkey, &account.data)
}

fn write_account_state(
    accounts: &mut CowAccountMap,
    pubkey: [u8; 32],
    state: &LaunchStakeState,
) -> Result<(), LaunchStakeError> {
    let encoded =
        wincode::serialize(state).map_err(|_| LaunchStakeError::InvalidAccountData { pubkey })?;
    let account = accounts
        .get_mut(&pubkey)
        .expect("instruction accounts were materialized before Stake dispatch");
    if encoded.len() > account.data.len() {
        return Err(LaunchStakeError::AccountDataTooSmall { pubkey });
    }
    account.data[..encoded.len()].copy_from_slice(&encoded);
    Ok(())
}

fn initialize(
    accounts: &mut CowAccountMap,
    stake_meta: &LaunchAccountMeta,
    authorized: LaunchStakeAuthorized,
    lockup: LaunchStakeLockup,
    rent: LaunchStakeRent,
) -> Result<u64, LaunchStakeError> {
    if decode_account(accounts, stake_meta.pubkey)? != LaunchStakeState::Uninitialized {
        return Err(LaunchStakeError::InvalidAccountData {
            pubkey: stake_meta.pubkey,
        });
    }
    let account = accounts
        .get(&stake_meta.pubkey)
        .expect("instruction accounts were materialized before Stake dispatch");
    let rent_exempt_reserve = rent.minimum_balance(account.data.len());
    // v1.0.7 used a strict comparison here: an exactly rent-exempt account
    // was rejected and needed at least one additional lamport.
    if rent_exempt_reserve >= account.lamports {
        return Err(LaunchStakeError::InsufficientFunds {
            pubkey: stake_meta.pubkey,
        });
    }
    write_account_state(
        accounts,
        stake_meta.pubkey,
        &LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve,
            authorized,
            lockup,
        }),
    )?;
    Ok(rent_exempt_reserve)
}

fn delegate(
    accounts: &mut CowAccountMap,
    stake_meta: &LaunchAccountMeta,
    vote_meta: &LaunchAccountMeta,
    signers: &BTreeSet<[u8; 32]>,
    clock: LaunchClock,
    stake_history: &LaunchStakeHistory,
    config: LaunchStakeConfig,
) -> Result<(u64, u64), LaunchStakeError> {
    let state = decode_account(accounts, stake_meta.pubkey)?;
    let (updated, delegated_lamports, credits_observed) = match state {
        LaunchStakeState::Initialized(meta) => {
            check_authorized(&meta.authorized, LaunchStakeAuthorize::Staker, signers)?;
            let delegated_lamports = accounts[&stake_meta.pubkey]
                .lamports
                .saturating_sub(meta.rent_exempt_reserve);
            let vote_account = accounts
                .get(&vote_meta.pubkey)
                .expect("instruction accounts were materialized before Stake dispatch");
            let credits_observed = decode_launch_vote_credits(vote_meta.pubkey, &vote_account.data)
                .map_err(|_| LaunchStakeError::InvalidVoteAccount {
                    pubkey: vote_meta.pubkey,
                })?;
            let stake = LaunchStake {
                delegation: LaunchDelegation {
                    voter_pubkey: vote_meta.pubkey,
                    stake: delegated_lamports,
                    activation_epoch: clock.epoch,
                    deactivation_epoch: u64::MAX,
                    warmup_cooldown_rate: config.warmup_cooldown_rate,
                },
                credits_observed,
            };
            (
                LaunchStakeState::Stake(meta, stake),
                delegated_lamports,
                credits_observed,
            )
        }
        LaunchStakeState::Stake(meta, mut stake) => {
            check_authorized(&meta.authorized, LaunchStakeAuthorize::Staker, signers)?;
            let vote_account = accounts
                .get(&vote_meta.pubkey)
                .expect("instruction accounts were materialized before Stake dispatch");
            let credits_observed = decode_launch_vote_credits(vote_meta.pubkey, &vote_account.data)
                .map_err(|_| LaunchStakeError::InvalidVoteAccount {
                    pubkey: vote_meta.pubkey,
                })?;
            if delegation_stake(&stake.delegation, clock.epoch, stake_history) != 0 {
                return Err(LaunchStakeError::TooSoonToRedelegate {
                    pubkey: stake_meta.pubkey,
                });
            }
            stake.delegation.activation_epoch = clock.epoch;
            stake.delegation.deactivation_epoch = u64::MAX;
            stake.delegation.voter_pubkey = vote_meta.pubkey;
            stake.delegation.warmup_cooldown_rate = config.warmup_cooldown_rate;
            stake.credits_observed = credits_observed;
            let delegated_lamports = stake.delegation.stake;
            (
                LaunchStakeState::Stake(meta, stake),
                delegated_lamports,
                credits_observed,
            )
        }
        LaunchStakeState::Uninitialized | LaunchStakeState::RewardsPool => {
            return Err(LaunchStakeError::InvalidAccountData {
                pubkey: stake_meta.pubkey,
            });
        }
    };
    write_account_state(accounts, stake_meta.pubkey, &updated)?;
    Ok((delegated_lamports, credits_observed))
}

fn split(
    accounts: &mut CowAccountMap,
    source_meta: &LaunchAccountMeta,
    destination_meta: &LaunchAccountMeta,
    lamports: u64,
    signers: &BTreeSet<[u8; 32]>,
) -> Result<(), LaunchStakeError> {
    let destination_state = decode_account(accounts, destination_meta.pubkey)?;
    if destination_state != LaunchStakeState::Uninitialized {
        return Err(LaunchStakeError::InvalidAccountData {
            pubkey: destination_meta.pubkey,
        });
    }
    let source_lamports = accounts[&source_meta.pubkey].lamports;
    let destination_lamports = accounts[&destination_meta.pubkey].lamports;
    if lamports > source_lamports {
        return Err(LaunchStakeError::InsufficientFunds {
            pubkey: source_meta.pubkey,
        });
    }

    match decode_account(accounts, source_meta.pubkey)? {
        LaunchStakeState::Stake(meta, mut stake) => {
            check_authorized(&meta.authorized, LaunchStakeAuthorize::Staker, signers)?;
            if destination_lamports.wrapping_add(lamports) < meta.rent_exempt_reserve
                || (lamports.wrapping_add(meta.rent_exempt_reserve) > source_lamports
                    && lamports != source_lamports)
            {
                return Err(LaunchStakeError::InsufficientFunds {
                    pubkey: source_meta.pubkey,
                });
            }
            let delegated_lamports = lamports.wrapping_sub(
                meta.rent_exempt_reserve
                    .saturating_sub(destination_lamports),
            );
            if delegated_lamports > stake.delegation.stake {
                return Err(LaunchStakeError::InsufficientStake {
                    pubkey: source_meta.pubkey,
                });
            }
            stake.delegation.stake -= delegated_lamports;
            let mut split_stake = stake;
            split_stake.delegation.stake = delegated_lamports;
            write_account_state(
                accounts,
                source_meta.pubkey,
                &LaunchStakeState::Stake(meta, stake),
            )?;
            write_account_state(
                accounts,
                destination_meta.pubkey,
                &LaunchStakeState::Stake(meta, split_stake),
            )?;
        }
        LaunchStakeState::Initialized(meta) => {
            check_authorized(&meta.authorized, LaunchStakeAuthorize::Staker, signers)?;
            if lamports < meta.rent_exempt_reserve
                || (lamports.wrapping_add(meta.rent_exempt_reserve) > source_lamports
                    && lamports != source_lamports)
            {
                return Err(LaunchStakeError::InsufficientFunds {
                    pubkey: source_meta.pubkey,
                });
            }
            write_account_state(
                accounts,
                destination_meta.pubkey,
                &LaunchStakeState::Initialized(meta),
            )?;
        }
        LaunchStakeState::Uninitialized => {
            if !signers.contains(&source_meta.pubkey) {
                return Err(LaunchStakeError::MissingRequiredSignature {
                    pubkey: source_meta.pubkey,
                });
            }
        }
        LaunchStakeState::RewardsPool => {
            return Err(LaunchStakeError::InvalidAccountData {
                pubkey: source_meta.pubkey,
            });
        }
    }

    accounts
        .get_mut(&destination_meta.pubkey)
        .expect("destination was loaded")
        .lamports = destination_lamports.wrapping_add(lamports);
    accounts
        .get_mut(&source_meta.pubkey)
        .expect("source was loaded")
        .lamports -= lamports;
    Ok(())
}

fn merge(
    accounts: &mut CowAccountMap,
    destination_meta: &LaunchAccountMeta,
    source_meta: &LaunchAccountMeta,
    signers: &BTreeSet<[u8; 32]>,
    epoch: u64,
    stake_history: &LaunchStakeHistory,
) -> Result<u64, LaunchStakeError> {
    let destination_state = decode_account(accounts, destination_meta.pubkey)?;
    let destination_stake_meta = mergeable_stake_meta(
        destination_meta.pubkey,
        destination_state,
        epoch,
        stake_history,
    )?;

    // v1.3.3 authorizes the destination before decoding the source account.
    check_authorized(
        &destination_stake_meta.authorized,
        LaunchStakeAuthorize::Staker,
        signers,
    )?;

    let source_state = decode_account(accounts, source_meta.pubkey)?;
    let source_stake_meta =
        mergeable_stake_meta(source_meta.pubkey, source_state, epoch, stake_history)?;
    if destination_stake_meta != source_stake_meta {
        return Err(LaunchStakeError::MergeMismatch {
            destination: destination_meta.pubkey,
            source_pubkey: source_meta.pubkey,
        });
    }

    // Historical Merge transfers lamports only. It deliberately leaves the
    // destination delegation/data untouched and the drained source data in
    // place; the Bank removes the zero-lamport source when committing state.
    let source_lamports = accounts[&source_meta.pubkey].lamports;
    if destination_meta.pubkey == source_meta.pubkey {
        // Duplicate aliases share the same account cell upstream: subtracting
        // and then adding the captured balance is a net no-op.
        return Ok(source_lamports);
    }
    accounts
        .get_mut(&source_meta.pubkey)
        .expect("merge source was loaded")
        .lamports = 0;
    let destination_lamports = accounts[&destination_meta.pubkey].lamports;
    accounts
        .get_mut(&destination_meta.pubkey)
        .expect("merge destination was loaded")
        .lamports = destination_lamports.wrapping_add(source_lamports);
    Ok(source_lamports)
}

fn mergeable_stake_meta(
    pubkey: [u8; 32],
    state: LaunchStakeState,
    epoch: u64,
    stake_history: &LaunchStakeHistory,
) -> Result<LaunchStakeMeta, LaunchStakeError> {
    match state {
        LaunchStakeState::Stake(meta, stake) => {
            if delegation_stake(&stake.delegation, epoch, stake_history) != 0 {
                return Err(LaunchStakeError::MergeActivatedStake { pubkey });
            }
            Ok(meta)
        }
        LaunchStakeState::Initialized(meta) => Ok(meta),
        LaunchStakeState::Uninitialized | LaunchStakeState::RewardsPool => {
            Err(LaunchStakeError::InvalidAccountData { pubkey })
        }
    }
}

fn authorize(
    accounts: &mut CowAccountMap,
    stake_meta: &LaunchAccountMeta,
    new_authority: [u8; 32],
    authority_type: LaunchStakeAuthorize,
    signers: &BTreeSet<[u8; 32]>,
    clock: LaunchClock,
    enforce_lockup: bool,
) -> Result<(), LaunchStakeError> {
    let state = decode_account(accounts, stake_meta.pubkey)?;
    let updated = match state {
        LaunchStakeState::Stake(mut meta, stake) => {
            authorize_meta(
                stake_meta.pubkey,
                &mut meta,
                new_authority,
                authority_type,
                signers,
                clock,
                enforce_lockup,
            )?;
            LaunchStakeState::Stake(meta, stake)
        }
        LaunchStakeState::Initialized(mut meta) => {
            authorize_meta(
                stake_meta.pubkey,
                &mut meta,
                new_authority,
                authority_type,
                signers,
                clock,
                enforce_lockup,
            )?;
            LaunchStakeState::Initialized(meta)
        }
        _ => {
            return Err(LaunchStakeError::InvalidAccountData {
                pubkey: stake_meta.pubkey,
            });
        }
    };
    write_account_state(accounts, stake_meta.pubkey, &updated)
}

fn authorize_meta(
    stake_pubkey: [u8; 32],
    meta: &mut LaunchStakeMeta,
    new_authority: [u8; 32],
    authority_type: LaunchStakeAuthorize,
    signers: &BTreeSet<[u8; 32]>,
    clock: LaunchClock,
    enforce_lockup: bool,
) -> Result<(), LaunchStakeError> {
    if enforce_lockup && lockup_in_force(&meta.lockup, clock, signers) {
        return Err(LaunchStakeError::LockupInForce {
            pubkey: stake_pubkey,
        });
    }
    match authority_type {
        LaunchStakeAuthorize::Staker => {
            if !signers.contains(&meta.authorized.staker)
                && !signers.contains(&meta.authorized.withdrawer)
            {
                return Err(LaunchStakeError::MissingRequiredSignature {
                    pubkey: meta.authorized.staker,
                });
            }
            meta.authorized.staker = new_authority;
        }
        LaunchStakeAuthorize::Withdrawer => {
            if !signers.contains(&meta.authorized.withdrawer) {
                return Err(LaunchStakeError::MissingRequiredSignature {
                    pubkey: meta.authorized.withdrawer,
                });
            }
            meta.authorized.withdrawer = new_authority;
        }
    }
    Ok(())
}

fn deactivate(
    accounts: &mut CowAccountMap,
    stake_meta: &LaunchAccountMeta,
    signers: &BTreeSet<[u8; 32]>,
    epoch: u64,
) -> Result<(), LaunchStakeError> {
    // Preserve v1.0.7 `StakeAccount::deactivate`: decode and require Stake
    // state, check the staker signature, then reject a second deactivation.
    let LaunchStakeState::Stake(meta, mut stake) = decode_account(accounts, stake_meta.pubkey)?
    else {
        return Err(LaunchStakeError::InvalidAccountData {
            pubkey: stake_meta.pubkey,
        });
    };
    check_authorized(&meta.authorized, LaunchStakeAuthorize::Staker, signers)?;
    if stake.delegation.deactivation_epoch != u64::MAX {
        return Err(LaunchStakeError::AlreadyDeactivated {
            pubkey: stake_meta.pubkey,
        });
    }
    stake.delegation.deactivation_epoch = epoch;
    write_account_state(
        accounts,
        stake_meta.pubkey,
        &LaunchStakeState::Stake(meta, stake),
    )
}

fn set_lockup(
    accounts: &mut CowAccountMap,
    stake_meta: &LaunchAccountMeta,
    lockup: LaunchStakeLockupArgs,
    signers: &BTreeSet<[u8; 32]>,
) -> Result<LaunchStakeLockup, LaunchStakeError> {
    // Preserve v1.0.7 `StakeAccount::set_lockup`: state decoding precedes the
    // custodian check, and both Initialized and delegated Stake states carry
    // the same Meta update.
    let state = decode_account(accounts, stake_meta.pubkey)?;
    let updated = match state {
        LaunchStakeState::Initialized(mut meta) => {
            set_lockup_meta(&mut meta, lockup, signers)?;
            LaunchStakeState::Initialized(meta)
        }
        LaunchStakeState::Stake(mut meta, stake) => {
            set_lockup_meta(&mut meta, lockup, signers)?;
            LaunchStakeState::Stake(meta, stake)
        }
        LaunchStakeState::Uninitialized | LaunchStakeState::RewardsPool => {
            return Err(LaunchStakeError::InvalidAccountData {
                pubkey: stake_meta.pubkey,
            });
        }
    };
    let updated_lockup = match updated {
        LaunchStakeState::Initialized(meta) | LaunchStakeState::Stake(meta, _) => meta.lockup,
        LaunchStakeState::Uninitialized | LaunchStakeState::RewardsPool => unreachable!(),
    };
    write_account_state(accounts, stake_meta.pubkey, &updated)?;
    Ok(updated_lockup)
}

fn set_lockup_meta(
    meta: &mut LaunchStakeMeta,
    lockup: LaunchStakeLockupArgs,
    signers: &BTreeSet<[u8; 32]>,
) -> Result<(), LaunchStakeError> {
    // This is intentionally independent of Clock/Lockup::is_in_force. The
    // launch implementation required the old custodian on every SetLockup.
    if !signers.contains(&meta.lockup.custodian) {
        return Err(LaunchStakeError::MissingRequiredSignature {
            pubkey: meta.lockup.custodian,
        });
    }
    if let Some(unix_timestamp) = lockup.unix_timestamp {
        meta.lockup.unix_timestamp = unix_timestamp;
    }
    if let Some(epoch) = lockup.epoch {
        meta.lockup.epoch = epoch;
    }
    if let Some(custodian) = lockup.custodian {
        meta.lockup.custodian = custodian;
    }
    Ok(())
}

fn withdraw(
    accounts: &mut CowAccountMap,
    stake_meta: &LaunchAccountMeta,
    destination_meta: &LaunchAccountMeta,
    lamports: u64,
    signers: &BTreeSet<[u8; 32]>,
    context: LaunchStakeContext<'_>,
) -> Result<bool, LaunchStakeError> {
    let source_lamports = accounts[&stake_meta.pubkey].lamports;
    let (lockup, reserve, is_staked) = match decode_account(accounts, stake_meta.pubkey)? {
        LaunchStakeState::Stake(meta, stake) => {
            check_authorized(&meta.authorized, LaunchStakeAuthorize::Withdrawer, signers)?;
            let staked = if context.clock.epoch >= stake.delegation.deactivation_epoch {
                delegation_stake(
                    &stake.delegation,
                    context.clock.epoch,
                    context.stake_history,
                )
            } else {
                stake.delegation.stake
            };
            (
                meta.lockup,
                staked.wrapping_add(meta.rent_exempt_reserve),
                staked != 0,
            )
        }
        LaunchStakeState::Initialized(meta) => {
            check_authorized(&meta.authorized, LaunchStakeAuthorize::Withdrawer, signers)?;
            (meta.lockup, meta.rent_exempt_reserve, false)
        }
        LaunchStakeState::Uninitialized => {
            if !signers.contains(&stake_meta.pubkey) {
                return Err(LaunchStakeError::MissingRequiredSignature {
                    pubkey: stake_meta.pubkey,
                });
            }
            (LaunchStakeLockup::default(), 0, false)
        }
        LaunchStakeState::RewardsPool => {
            return Err(LaunchStakeError::InvalidAccountData {
                pubkey: stake_meta.pubkey,
            });
        }
    };
    if lockup_in_force(&lockup, context.clock, signers) {
        return Err(LaunchStakeError::LockupInForce {
            pubkey: stake_meta.pubkey,
        });
    }
    if is_staked && lamports.wrapping_add(reserve) > source_lamports {
        return Err(LaunchStakeError::InsufficientFunds {
            pubkey: stake_meta.pubkey,
        });
    }
    if lamports != source_lamports && lamports.wrapping_add(reserve) > source_lamports {
        return Err(LaunchStakeError::InsufficientFunds {
            pubkey: stake_meta.pubkey,
        });
    }
    accounts
        .get_mut(&stake_meta.pubkey)
        .expect("stake source was loaded")
        .lamports = source_lamports.wrapping_sub(lamports);
    let destination_lamports = accounts[&destination_meta.pubkey].lamports;
    accounts
        .get_mut(&destination_meta.pubkey)
        .expect("withdraw destination was loaded")
        .lamports = destination_lamports.wrapping_add(lamports);
    Ok(lamports == source_lamports)
}

fn check_authorized(
    authorized: &LaunchStakeAuthorized,
    authority_type: LaunchStakeAuthorize,
    signers: &BTreeSet<[u8; 32]>,
) -> Result<(), LaunchStakeError> {
    let required = match authority_type {
        LaunchStakeAuthorize::Staker => authorized.staker,
        LaunchStakeAuthorize::Withdrawer => authorized.withdrawer,
    };
    if signers.contains(&required) {
        Ok(())
    } else {
        Err(LaunchStakeError::MissingRequiredSignature { pubkey: required })
    }
}

fn lockup_in_force(
    lockup: &LaunchStakeLockup,
    clock: LaunchClock,
    signers: &BTreeSet<[u8; 32]>,
) -> bool {
    (lockup.unix_timestamp > clock.unix_timestamp || lockup.epoch > clock.epoch)
        && !signers.contains(&lockup.custodian)
}

fn delegation_stake(
    delegation: &LaunchDelegation,
    epoch: u64,
    history: &LaunchStakeHistory,
) -> u64 {
    delegation_activation_status(delegation, epoch, history).0
}

fn delegation_activation_status(
    delegation: &LaunchDelegation,
    epoch: u64,
    history: &LaunchStakeHistory,
) -> (u64, u64, u64) {
    let (effective, activating) = stake_and_activating(delegation, epoch, history);
    if epoch < delegation.deactivation_epoch {
        (effective, activating, 0)
    } else if epoch == delegation.deactivation_epoch {
        (effective, 0, effective.min(delegation.stake))
    } else if let Some(mut entry) = history.get(&delegation.deactivation_epoch).copied() {
        let mut effective_stake = effective;
        let mut next_epoch = delegation.deactivation_epoch;
        loop {
            if entry.deactivating == 0 {
                break;
            }
            let weight = effective_stake as f64 / entry.deactivating as f64;
            effective_stake = effective_stake.saturating_sub(
                ((weight * entry.effective as f64 * delegation.warmup_cooldown_rate) as u64).max(1),
            );
            if effective_stake == 0 {
                break;
            }
            next_epoch = next_epoch.wrapping_add(1);
            if next_epoch >= epoch {
                break;
            }
            let Some(next) = history.get(&next_epoch).copied() else {
                break;
            };
            entry = next;
        }
        (effective_stake, 0, effective_stake)
    } else {
        (0, 0, 0)
    }
}

fn stake_and_activating(
    delegation: &LaunchDelegation,
    epoch: u64,
    history: &LaunchStakeHistory,
) -> (u64, u64) {
    if delegation.activation_epoch == u64::MAX {
        (delegation.stake, 0)
    } else if epoch == delegation.activation_epoch {
        (0, delegation.stake)
    } else if epoch < delegation.activation_epoch {
        (0, 0)
    } else if let Some(mut entry) = history.get(&delegation.activation_epoch).copied() {
        let mut effective_stake = 0;
        let mut next_epoch = delegation.activation_epoch;
        loop {
            if entry.activating == 0 {
                break;
            }
            let weight = (delegation.stake - effective_stake) as f64 / entry.activating as f64;
            effective_stake = effective_stake.wrapping_add(
                ((weight * entry.effective as f64 * delegation.warmup_cooldown_rate) as u64).max(1),
            );
            if effective_stake >= delegation.stake {
                effective_stake = delegation.stake;
                break;
            }
            next_epoch = next_epoch.wrapping_add(1);
            if next_epoch >= epoch || next_epoch >= delegation.deactivation_epoch {
                break;
            }
            let Some(next) = history.get(&next_epoch).copied() else {
                break;
            };
            entry = next;
        }
        (effective_stake, delegation.stake - effective_stake)
    } else {
        (delegation.stake, 0)
    }
}

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
    fn new(pubkey: [u8; 32], is_writable: bool, account: &AccountSnapshot) -> Self {
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

    fn verify(&self, post: &AccountSnapshot) -> Result<(), LaunchStakeError> {
        if self.owner != post.owner
            && (!self.is_writable || self.owner != STAKE_PROGRAM_ID || !is_zeroed(&post.data))
        {
            return Err(LaunchStakeError::ModifiedProgramId {
                pubkey: self.pubkey,
            });
        }
        if self.owner != STAKE_PROGRAM_ID && self.lamports > post.lamports {
            return Err(LaunchStakeError::ExternalAccountLamportSpend {
                pubkey: self.pubkey,
            });
        }
        if !self.is_writable && self.lamports != post.lamports {
            return Err(LaunchStakeError::ReadonlyLamportChange {
                pubkey: self.pubkey,
            });
        }
        if self.data_len != post.data.len() {
            return Err(LaunchStakeError::AccountDataSizeChanged {
                pubkey: self.pubkey,
            });
        }
        if should_verify_data(&self.owner, self.is_writable)
            && self.data.as_ref() != Some(&post.data)
        {
            return Err(if self.is_writable {
                LaunchStakeError::ExternalAccountDataModified {
                    pubkey: self.pubkey,
                }
            } else {
                LaunchStakeError::ReadonlyDataModified {
                    pubkey: self.pubkey,
                }
            });
        }
        if self.executable != post.executable
            && (!self.is_writable || self.executable || self.owner != STAKE_PROGRAM_ID)
        {
            return Err(LaunchStakeError::ExecutableModified {
                pubkey: self.pubkey,
            });
        }
        if self.rent_epoch != post.rent_epoch {
            return Err(LaunchStakeError::RentEpochModified {
                pubkey: self.pubkey,
            });
        }
        Ok(())
    }
}

fn should_verify_data(owner: &[u8; 32], is_writable: bool) -> bool {
    *owner != STAKE_PROGRAM_ID || !is_writable
}

fn is_zeroed(data: &[u8]) -> bool {
    data.iter().all(|byte| *byte == 0)
}

fn launch_pre_accounts(
    account_metas: &[LaunchAccountMeta],
    accounts: &CowAccountMap,
) -> Vec<LaunchPreAccount> {
    account_metas
        .iter()
        .enumerate()
        .filter(|(index, meta)| {
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

fn verify_launch_stake_instruction(
    pre_accounts: &[LaunchPreAccount],
    accounts: &CowAccountMap,
) -> Result<(), LaunchStakeError> {
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
        return Err(LaunchStakeError::UnbalancedInstruction {
            pre_lamports,
            post_lamports,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryAccountStore;

    fn meta(pubkey: [u8; 32], is_signer: bool, is_writable: bool) -> LaunchAccountMeta {
        LaunchAccountMeta {
            pubkey,
            is_signer,
            is_writable,
        }
    }

    fn stake_account(lamports: u64, state: LaunchStakeState) -> AccountSnapshot {
        let mut account = AccountSnapshot {
            lamports,
            owner: STAKE_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: vec![0; 200].into(),
        };
        let encoded = wincode::serialize(&state).unwrap();
        account.data[..encoded.len()].copy_from_slice(&encoded);
        account
    }

    fn readonly_account(owner: [u8; 32], data: Vec<u8>) -> AccountSnapshot {
        AccountSnapshot {
            lamports: 1,
            owner,
            executable: false,
            rent_epoch: 0,
            data: data.into(),
        }
    }

    fn rent_account(rent: LaunchStakeRent) -> AccountSnapshot {
        readonly_account(crate::SYSVAR_OWNER_ID, wincode::serialize(&rent).unwrap())
    }

    fn clock_account(epoch: u64) -> AccountSnapshot {
        readonly_account(
            crate::SYSVAR_OWNER_ID,
            wincode::serialize(&LaunchStakeClockWire {
                slot: 646_291,
                segment: 632,
                epoch,
                leader_schedule_epoch: epoch + 1,
                unix_timestamp: 1_585_930_517,
            })
            .unwrap(),
        )
    }

    fn stake_history_account(entries: Vec<(u64, LaunchStakeHistoryEntryWire)>) -> AccountSnapshot {
        readonly_account(
            crate::SYSVAR_OWNER_ID,
            wincode::serialize(&LaunchStakeHistoryWire(entries)).unwrap(),
        )
    }

    fn stake_config_account(warmup_cooldown_rate: f64) -> AccountSnapshot {
        let mut data = vec![0]; // empty v1.0.7 short-vec `ConfigKeys`
        data.extend_from_slice(
            &wincode::serialize(&LaunchStakeConfig {
                warmup_cooldown_rate,
                slash_penalty: 12,
            })
            .unwrap(),
        );
        readonly_account(crate::CONFIG_PROGRAM_ID, data)
    }

    fn initialized_vote_account(vote_pubkey: [u8; 32], epoch: u64) -> AccountSnapshot {
        let node_pubkey = [0x51; 32];
        let mut instruction = 0_u32.to_le_bytes().to_vec();
        instruction.extend_from_slice(&node_pubkey);
        instruction.extend_from_slice(&[0x52; 32]);
        instruction.extend_from_slice(&[0x53; 32]);
        instruction.push(100);
        let rent = LaunchStakeRent {
            lamports_per_byte_year: 3_480,
            exemption_threshold: 2.0,
            burn_percent: 100,
        };
        let mut accounts = AccountMap::from([
            (
                vote_pubkey,
                AccountSnapshot {
                    lamports: rent.minimum_balance(3_731),
                    owner: crate::VOTE_PROGRAM_ID,
                    executable: false,
                    rent_epoch: 0,
                    data: vec![0; 3_731].into(),
                },
            ),
            (RENT_SYSVAR_ID, rent_account(rent)),
            (CLOCK_SYSVAR_ID, clock_account(epoch)),
            (node_pubkey, crate::default_system_account()),
        ]);
        crate::apply_launch_vote_instruction(
            &instruction,
            &[
                meta(vote_pubkey, true, true),
                meta(RENT_SYSVAR_ID, false, false),
                meta(CLOCK_SYSVAR_ID, false, false),
                meta(node_pubkey, true, false),
            ],
            &mut accounts,
            epoch,
        )
        .unwrap();
        accounts.remove(&vote_pubkey).unwrap()
    }

    fn context() -> LaunchStakeContext<'static> {
        static HISTORY: std::sync::LazyLock<LaunchStakeHistory> =
            std::sync::LazyLock::new(LaunchStakeHistory::new);
        LaunchStakeContext {
            clock: LaunchClock {
                slot: 105_368,
                epoch: 0,
                unix_timestamp: 1_584_411_087,
            },
            stake_history: &HISTORY,
        }
    }

    fn context_at(epoch: u64, unix_timestamp: i64) -> LaunchStakeContext<'static> {
        context_at_slot(105_368, epoch, unix_timestamp)
    }

    fn context_at_slot(slot: u64, epoch: u64, unix_timestamp: i64) -> LaunchStakeContext<'static> {
        LaunchStakeContext {
            clock: LaunchClock {
                slot,
                epoch,
                unix_timestamp,
            },
            ..context()
        }
    }

    fn set_lockup_data(
        unix_timestamp: Option<i64>,
        epoch: Option<u64>,
        custodian: Option<[u8; 32]>,
    ) -> Vec<u8> {
        wincode::serialize(&LaunchStakeInstructionV100::SetLockup(
            LaunchStakeLockupArgs {
                unix_timestamp,
                epoch,
                custodian,
            },
        ))
        .unwrap()
    }

    fn delegated_stake_state(staker: [u8; 32], deactivation_epoch: u64) -> LaunchStakeState {
        LaunchStakeState::Stake(
            LaunchStakeMeta {
                rent_exempt_reserve: 100,
                authorized: LaunchStakeAuthorized {
                    staker,
                    withdrawer: [0xd1; 32],
                },
                lockup: LaunchStakeLockup::default(),
            },
            LaunchStake {
                delegation: LaunchDelegation {
                    voter_pubkey: [0xd2; 32],
                    stake: 900,
                    activation_epoch: u64::MAX,
                    deactivation_epoch,
                    warmup_cooldown_rate: 0.25,
                },
                credits_observed: 7,
            },
        )
    }

    #[test]
    fn initialize_uses_launch_rent_and_requires_one_lamport_above_minimum() {
        let stake_pubkey = [1; 32];
        let authorized = LaunchStakeAuthorized {
            staker: [2; 32],
            withdrawer: [3; 32],
        };
        let lockup = LaunchStakeLockup {
            unix_timestamp: 123,
            epoch: 4,
            custodian: [5; 32],
        };
        let instruction =
            wincode::serialize(&LaunchStakeInstructionV100::Initialize(authorized, lockup))
                .unwrap();
        let rent = LaunchStakeRent {
            lamports_per_byte_year: 3_480,
            exemption_threshold: 2.0,
            burn_percent: 100,
        };
        let reserve = rent.minimum_balance(LAUNCH_STAKE_ACCOUNT_DATA_LEN);
        let mut accounts = AccountMap::from([
            (
                stake_pubkey,
                stake_account(reserve + 1, LaunchStakeState::Uninitialized),
            ),
            (RENT_SYSVAR_ID, rent_account(rent)),
        ]);
        let metas = [
            meta(stake_pubkey, false, true),
            meta(RENT_SYSVAR_ID, false, false),
        ];

        let mutation =
            apply_launch_stake_instruction(&instruction, &metas, &mut accounts, context()).unwrap();
        assert_eq!(
            mutation,
            LaunchStakeMutation::Initialize {
                stake_account: stake_pubkey,
                rent_exempt_reserve: reserve,
                authorized,
                lockup,
            }
        );
        assert_eq!(
            decode_launch_stake_state(stake_pubkey, &accounts[&stake_pubkey].data).unwrap(),
            LaunchStakeState::Initialized(LaunchStakeMeta {
                rent_exempt_reserve: reserve,
                authorized,
                lockup,
            })
        );

        let mut exactly_exempt = AccountMap::from([
            (
                stake_pubkey,
                stake_account(reserve, LaunchStakeState::Uninitialized),
            ),
            (RENT_SYSVAR_ID, rent_account(rent)),
        ]);
        let before = exactly_exempt.clone();
        assert_eq!(
            apply_launch_stake_instruction(&instruction, &metas, &mut exactly_exempt, context(),)
                .unwrap_err(),
            LaunchStakeError::InsufficientFunds {
                pubkey: stake_pubkey,
            }
        );
        assert_eq!(exactly_exempt, before);

        let mut readonly = AccountMap::from([
            (
                stake_pubkey,
                stake_account(reserve + 1, LaunchStakeState::Uninitialized),
            ),
            (RENT_SYSVAR_ID, rent_account(rent)),
        ]);
        let before = readonly.clone();
        let error = apply_launch_stake_instruction(
            &instruction,
            &[
                meta(stake_pubkey, false, false),
                meta(RENT_SYSVAR_ID, false, false),
            ],
            &mut readonly,
            context(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LaunchStakeError::ReadonlyDataModified { pubkey } if pubkey == stake_pubkey
        ));
        assert_eq!(readonly, before);
    }

    #[test]
    fn layered_initialize_keeps_readonly_rent_sysvar_in_parent() {
        let stake_pubkey = [0x41; 32];
        let authorized = LaunchStakeAuthorized {
            staker: [0x42; 32],
            withdrawer: [0x43; 32],
        };
        let lockup = LaunchStakeLockup::default();
        let instruction =
            wincode::serialize(&LaunchStakeInstructionV100::Initialize(authorized, lockup))
                .unwrap();
        let rent = LaunchStakeRent {
            lamports_per_byte_year: 3_480,
            exemption_threshold: 2.0,
            burn_percent: 100,
        };
        let reserve = rent.minimum_balance(LAUNCH_STAKE_ACCOUNT_DATA_LEN);
        let original_stake = stake_account(reserve + 1, LaunchStakeState::Uninitialized);
        let mut parent = MemoryAccountStore::new();
        parent.insert(stake_pubkey, original_stake.clone());
        parent.insert(RENT_SYSVAR_ID, rent_account(rent));
        let mut overlay = CowAccountMap::layered(&parent);

        apply_launch_stake_instruction_on_overlay(
            &instruction,
            &[
                meta(stake_pubkey, false, true),
                meta(RENT_SYSVAR_ID, false, false),
            ],
            &mut overlay,
            context(),
        )
        .unwrap();

        assert!(overlay.local_contains_key(&stake_pubkey));
        assert!(!overlay.local_contains_key(&RENT_SYSVAR_ID));
        let local = overlay.into_local();
        assert_eq!(local.len(), 1);
        assert!(local.contains_key(&stake_pubkey));
        assert_eq!(parent[&stake_pubkey], original_stake);
    }

    #[test]
    fn delegate_decodes_bank_inputs_and_creates_launch_stake_state() {
        let stake_pubkey = [6; 32];
        let vote_pubkey = [7; 32];
        let authority = [8; 32];
        let reserve = 2_282_880;
        let lamports = 1_000_000_000;
        let initialized = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: reserve,
            authorized: LaunchStakeAuthorized {
                staker: authority,
                withdrawer: [9; 32],
            },
            lockup: LaunchStakeLockup::default(),
        });
        let mut accounts = AccountMap::from([
            (stake_pubkey, stake_account(lamports, initialized)),
            (vote_pubkey, initialized_vote_account(vote_pubkey, 1)),
            (CLOCK_SYSVAR_ID, clock_account(1)),
            (STAKE_HISTORY_SYSVAR_ID, stake_history_account(Vec::new())),
            (STAKE_CONFIG_ID, stake_config_account(0.125)),
        ]);
        let instruction = wincode::serialize(&LaunchStakeInstructionV100::DelegateStake).unwrap();

        let mutation = apply_launch_stake_instruction(
            &instruction,
            &[
                meta(stake_pubkey, false, true),
                meta(vote_pubkey, false, false),
                meta(CLOCK_SYSVAR_ID, false, false),
                meta(STAKE_HISTORY_SYSVAR_ID, false, false),
                meta(STAKE_CONFIG_ID, false, false),
                meta(authority, true, false),
            ],
            &mut accounts,
            context(),
        )
        .unwrap();

        assert_eq!(
            mutation,
            LaunchStakeMutation::Delegate {
                stake_account: stake_pubkey,
                vote_account: vote_pubkey,
                delegated_lamports: lamports - reserve,
                activation_epoch: 1,
                credits_observed: 0,
            }
        );
        let LaunchStakeState::Stake(_, stake) =
            decode_launch_stake_state(stake_pubkey, &accounts[&stake_pubkey].data).unwrap()
        else {
            panic!("initialized account must become delegated Stake state");
        };
        assert_eq!(stake.delegation.voter_pubkey, vote_pubkey);
        assert_eq!(stake.delegation.stake, lamports - reserve);
        assert_eq!(stake.delegation.activation_epoch, 1);
        assert_eq!(stake.delegation.deactivation_epoch, u64::MAX);
        assert_eq!(stake.delegation.warmup_cooldown_rate, 0.125);
        assert_eq!(stake.credits_observed, 0);
    }

    #[test]
    fn redelegate_requires_zero_effective_stake_and_preserves_amount() {
        let stake_pubkey = [0x16; 32];
        let old_vote = [0x17; 32];
        let new_vote = [0x18; 32];
        let authority = [0x19; 32];
        let meta_state = LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: authority,
                withdrawer: authority,
            },
            lockup: LaunchStakeLockup::default(),
        };
        let inactive_stake = LaunchStake {
            delegation: LaunchDelegation {
                voter_pubkey: old_vote,
                stake: 900,
                activation_epoch: 0,
                deactivation_epoch: 0,
                warmup_cooldown_rate: 0.25,
            },
            credits_observed: 99,
        };
        let common_accounts = [
            (new_vote, initialized_vote_account(new_vote, 1)),
            (CLOCK_SYSVAR_ID, clock_account(1)),
            (STAKE_HISTORY_SYSVAR_ID, stake_history_account(Vec::new())),
            (STAKE_CONFIG_ID, stake_config_account(0.5)),
        ];
        let instruction = wincode::serialize(&LaunchStakeInstructionV100::DelegateStake).unwrap();
        let metas = [
            meta(stake_pubkey, false, true),
            meta(new_vote, false, false),
            meta(CLOCK_SYSVAR_ID, false, false),
            meta(STAKE_HISTORY_SYSVAR_ID, false, false),
            meta(STAKE_CONFIG_ID, false, false),
            meta(authority, true, false),
        ];
        let mut accounts = AccountMap::from(common_accounts.clone());
        accounts.insert(
            stake_pubkey,
            stake_account(1_000, LaunchStakeState::Stake(meta_state, inactive_stake)),
        );

        let mutation =
            apply_launch_stake_instruction(&instruction, &metas, &mut accounts, context()).unwrap();
        assert_eq!(
            mutation,
            LaunchStakeMutation::Delegate {
                stake_account: stake_pubkey,
                vote_account: new_vote,
                delegated_lamports: 900,
                activation_epoch: 1,
                credits_observed: 0,
            }
        );
        let LaunchStakeState::Stake(_, redelegated) =
            decode_launch_stake_state(stake_pubkey, &accounts[&stake_pubkey].data).unwrap()
        else {
            panic!("stake must remain delegated");
        };
        assert_eq!(redelegated.delegation.stake, 900);
        assert_eq!(redelegated.delegation.voter_pubkey, new_vote);
        assert_eq!(redelegated.delegation.activation_epoch, 1);
        assert_eq!(redelegated.delegation.deactivation_epoch, u64::MAX);
        assert_eq!(redelegated.delegation.warmup_cooldown_rate, 0.5);
        assert_eq!(redelegated.credits_observed, 0);

        let active_stake = LaunchStake {
            delegation: LaunchDelegation {
                activation_epoch: u64::MAX,
                deactivation_epoch: u64::MAX,
                ..inactive_stake.delegation
            },
            ..inactive_stake
        };
        let mut active_accounts = AccountMap::from(common_accounts);
        active_accounts.insert(
            stake_pubkey,
            stake_account(1_000, LaunchStakeState::Stake(meta_state, active_stake)),
        );
        let before = active_accounts.clone();
        assert_eq!(
            apply_launch_stake_instruction(&instruction, &metas, &mut active_accounts, context(),)
                .unwrap_err(),
            LaunchStakeError::TooSoonToRedelegate {
                pubkey: stake_pubkey,
            }
        );
        assert_eq!(active_accounts, before);
    }

    #[test]
    fn deactivate_uses_clock_epoch_with_the_exact_three_meta_shape() {
        let stake_pubkey = [0x60; 32];
        let authority = [0x61; 32];
        let instruction = wincode::serialize(&LaunchStakeInstructionV100::Deactivate).unwrap();
        assert_eq!(instruction, [5, 0, 0, 0]);
        let mut accounts = AccountMap::from([
            (
                stake_pubkey,
                stake_account(1_000, delegated_stake_state(authority, u64::MAX)),
            ),
            (CLOCK_SYSVAR_ID, clock_account(7)),
        ]);

        let mutation = apply_launch_stake_instruction(
            &instruction,
            &[
                meta(stake_pubkey, false, true),
                meta(CLOCK_SYSVAR_ID, false, false),
                meta(authority, true, false),
            ],
            &mut accounts,
            context(),
        )
        .unwrap();

        assert_eq!(
            mutation,
            LaunchStakeMutation::Deactivate {
                stake_account: stake_pubkey,
                deactivation_epoch: 7,
            }
        );
        let LaunchStakeState::Stake(_, stake) =
            decode_launch_stake_state(stake_pubkey, &accounts[&stake_pubkey].data).unwrap()
        else {
            panic!("deactivated account must remain delegated Stake state");
        };
        assert_eq!(stake.delegation.deactivation_epoch, 7);
        assert_eq!(stake.delegation.activation_epoch, u64::MAX);
        assert_eq!(stake.delegation.stake, 900);
        assert_eq!(stake.credits_observed, 7);
    }

    #[test]
    fn deactivate_wrong_authority_is_rejected_atomically() {
        let stake_pubkey = [0x62; 32];
        let authority = [0x63; 32];
        let mut accounts = AccountMap::from([
            (
                stake_pubkey,
                stake_account(1_000, delegated_stake_state(authority, u64::MAX)),
            ),
            (CLOCK_SYSVAR_ID, clock_account(8)),
        ]);
        let before = accounts.clone();

        assert_eq!(
            apply_launch_stake_instruction(
                &[5, 0, 0, 0],
                &[
                    meta(stake_pubkey, false, true),
                    meta(CLOCK_SYSVAR_ID, false, false),
                    meta([0x64; 32], true, false),
                ],
                &mut accounts,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::MissingRequiredSignature { pubkey: authority }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn deactivate_rejects_an_already_deactivated_stake_after_authority_check() {
        let stake_pubkey = [0x65; 32];
        let authority = [0x66; 32];
        let original = AccountMap::from([
            (
                stake_pubkey,
                stake_account(1_000, delegated_stake_state(authority, 3)),
            ),
            (CLOCK_SYSVAR_ID, clock_account(8)),
        ]);
        let mut wrong_authority = original.clone();
        assert_eq!(
            apply_launch_stake_instruction(
                &[5, 0, 0, 0],
                &[
                    meta(stake_pubkey, false, true),
                    meta(CLOCK_SYSVAR_ID, false, false),
                    meta([0x67; 32], true, false),
                ],
                &mut wrong_authority,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::MissingRequiredSignature { pubkey: authority }
        );
        assert_eq!(wrong_authority, original);

        let mut already_deactivated = original.clone();
        assert_eq!(
            apply_launch_stake_instruction(
                &[5, 0, 0, 0],
                &[
                    meta(stake_pubkey, false, true),
                    meta(CLOCK_SYSVAR_ID, false, false),
                    meta(authority, true, false),
                ],
                &mut already_deactivated,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::AlreadyDeactivated {
                pubkey: stake_pubkey,
            }
        );
        assert_eq!(already_deactivated, original);
    }

    #[test]
    fn readonly_deactivate_is_rolled_back_by_the_post_verifier() {
        let stake_pubkey = [0x68; 32];
        let authority = [0x69; 32];
        let mut accounts = AccountMap::from([
            (
                stake_pubkey,
                stake_account(1_000, delegated_stake_state(authority, u64::MAX)),
            ),
            (CLOCK_SYSVAR_ID, clock_account(9)),
        ]);
        let before = accounts.clone();

        assert_eq!(
            apply_launch_stake_instruction(
                &[5, 0, 0, 0],
                &[
                    meta(stake_pubkey, false, false),
                    meta(CLOCK_SYSVAR_ID, false, false),
                    meta(authority, true, false),
                ],
                &mut accounts,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::ReadonlyDataModified {
                pubkey: stake_pubkey,
            }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn deactivate_preserves_v100_clock_and_state_error_ordering() {
        let stake_pubkey = [0x6a; 32];
        let authority = [0x6b; 32];
        let wrong_sysvar = [0x6c; 32];
        let stake = stake_account(1_000, LaunchStakeState::RewardsPool);

        let mut missing_first = AccountMap::new();
        assert_eq!(
            apply_launch_stake_instruction(&[], &[], &mut missing_first, context(),).unwrap_err(),
            LaunchStakeError::MissingAccount { position: 0 }
        );

        let mut missing_clock = AccountMap::from([(stake_pubkey, stake.clone())]);
        assert_eq!(
            apply_launch_stake_instruction(
                &[5, 0, 0, 0],
                &[meta(stake_pubkey, false, true)],
                &mut missing_clock,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::MissingAccount { position: 1 }
        );

        let mut wrong_clock = AccountMap::from([
            (stake_pubkey, stake.clone()),
            (wrong_sysvar, clock_account(10)),
        ]);
        assert_eq!(
            apply_launch_stake_instruction(
                &[5, 0, 0, 0],
                &[
                    meta(stake_pubkey, false, true),
                    meta(wrong_sysvar, false, false),
                ],
                &mut wrong_clock,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::InvalidSysvar {
                position: 1,
                expected: CLOCK_SYSVAR_ID,
                found: wrong_sysvar,
            }
        );

        let mut malformed_clock = AccountMap::from([
            (stake_pubkey, stake.clone()),
            (
                CLOCK_SYSVAR_ID,
                readonly_account(crate::SYSVAR_OWNER_ID, Vec::new()),
            ),
        ]);
        assert_eq!(
            apply_launch_stake_instruction(
                &[5, 0, 0, 0],
                &[
                    meta(stake_pubkey, false, true),
                    meta(CLOCK_SYSVAR_ID, false, false),
                ],
                &mut malformed_clock,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::InvalidSysvarData {
                position: 1,
                pubkey: CLOCK_SYSVAR_ID,
            }
        );

        let mut invalid_state =
            AccountMap::from([(stake_pubkey, stake), (CLOCK_SYSVAR_ID, clock_account(10))]);
        assert_eq!(
            apply_launch_stake_instruction(
                &[5, 0, 0, 0],
                &[
                    meta(stake_pubkey, false, true),
                    meta(CLOCK_SYSVAR_ID, false, false),
                    meta(authority, false, false),
                ],
                &mut invalid_state,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::InvalidAccountData {
                pubkey: stake_pubkey,
            }
        );
    }

    #[test]
    fn delegate_validates_config_before_stake_state_and_authority() {
        let stake_pubkey = [0x26; 32];
        let vote_pubkey = [0x27; 32];
        let mut accounts = AccountMap::from([
            (
                stake_pubkey,
                stake_account(1_000, LaunchStakeState::RewardsPool),
            ),
            (vote_pubkey, initialized_vote_account(vote_pubkey, 1)),
            (CLOCK_SYSVAR_ID, clock_account(1)),
            (STAKE_HISTORY_SYSVAR_ID, stake_history_account(Vec::new())),
            (
                STAKE_CONFIG_ID,
                readonly_account(crate::CONFIG_PROGRAM_ID, Vec::new()),
            ),
        ]);
        let before = accounts.clone();
        let instruction = wincode::serialize(&LaunchStakeInstructionV100::DelegateStake).unwrap();
        assert_eq!(
            apply_launch_stake_instruction(
                &instruction,
                &[
                    meta(stake_pubkey, false, true),
                    meta(vote_pubkey, false, false),
                    meta(CLOCK_SYSVAR_ID, false, false),
                    meta(STAKE_HISTORY_SYSVAR_ID, false, false),
                    meta(STAKE_CONFIG_ID, false, false),
                ],
                &mut accounts,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::InvalidConfigData {
                pubkey: STAKE_CONFIG_ID,
            }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn stake_state_layout_fits_the_launch_200_byte_account() {
        let state = LaunchStakeState::Stake(
            LaunchStakeMeta {
                rent_exempt_reserve: 1,
                authorized: LaunchStakeAuthorized {
                    staker: [1; 32],
                    withdrawer: [2; 32],
                },
                lockup: LaunchStakeLockup::default(),
            },
            LaunchStake {
                delegation: LaunchDelegation {
                    voter_pubkey: [3; 32],
                    stake: 4,
                    activation_epoch: u64::MAX,
                    deactivation_epoch: u64::MAX,
                    warmup_cooldown_rate: 0.25,
                },
                credits_observed: 5,
            },
        );
        assert_eq!(wincode::serialized_size(&state).unwrap(), 196);
        let account = stake_account(10, state);
        assert_eq!(
            decode_launch_stake_state([9; 32], &account.data).unwrap(),
            state
        );
    }

    #[test]
    fn split_moves_lamports_and_delegated_stake() {
        let source = [10; 32];
        let destination = [11; 32];
        let authority = [12; 32];
        let meta_state = LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: authority,
                withdrawer: [13; 32],
            },
            lockup: LaunchStakeLockup::default(),
        };
        let stake = LaunchStake {
            delegation: LaunchDelegation {
                voter_pubkey: [14; 32],
                stake: 900,
                activation_epoch: u64::MAX,
                deactivation_epoch: u64::MAX,
                warmup_cooldown_rate: 0.25,
            },
            credits_observed: 7,
        };
        let mut accounts = AccountMap::from([
            (
                source,
                stake_account(1_000, LaunchStakeState::Stake(meta_state, stake)),
            ),
            (
                destination,
                stake_account(0, LaunchStakeState::Uninitialized),
            ),
        ]);
        let mutation = apply_launch_stake_instruction(
            &[3, 0, 0, 0, 144, 1, 0, 0, 0, 0, 0, 0],
            &[
                meta(source, false, true),
                meta(destination, false, true),
                meta(authority, true, false),
            ],
            &mut accounts,
            context(),
        )
        .unwrap();
        assert_eq!(
            mutation,
            LaunchStakeMutation::Split {
                source,
                destination,
                lamports: 400,
            }
        );
        assert_eq!(accounts[&source].lamports, 600);
        assert_eq!(accounts[&destination].lamports, 400);
        let LaunchStakeState::Stake(_, source_stake) =
            decode_launch_stake_state(source, &accounts[&source].data).unwrap()
        else {
            panic!("source must remain delegated stake");
        };
        let LaunchStakeState::Stake(_, destination_stake) =
            decode_launch_stake_state(destination, &accounts[&destination].data).unwrap()
        else {
            panic!("destination must become delegated stake");
        };
        assert_eq!(source_stake.delegation.stake, 600);
        assert_eq!(destination_stake.delegation.stake, 300);
    }

    #[test]
    fn v1_3_3_merge_wire_drains_the_observed_source_without_rewriting_data() {
        let destination = [0x71; 32];
        let source = [0x72; 32];
        let authority = [0x73; 32];
        let stake_meta = LaunchStakeMeta {
            rent_exempt_reserve: 2_282_880,
            authorized: LaunchStakeAuthorized {
                staker: authority,
                withdrawer: [0x74; 32],
            },
            lockup: LaunchStakeLockup::default(),
        };
        let inactive_stake = LaunchStake {
            delegation: LaunchDelegation {
                voter_pubkey: [0x75; 32],
                stake: 86_000_000_000_000,
                activation_epoch: u64::MAX,
                deactivation_epoch: 0,
                warmup_cooldown_rate: 0.25,
            },
            credits_observed: 91,
        };
        let instruction = wincode::serialize(&LaunchStakeInstructionV100::Merge).unwrap();
        assert_eq!(instruction, [7, 0, 0, 0]);

        // Exact balance movement observed at slot 28,621,186. The fee payer is
        // outside the Stake instruction account list and is reconciled by the
        // transaction layer.
        let mut accounts = AccountMap::from([
            (
                destination,
                stake_account(
                    87_001_000_000_000,
                    LaunchStakeState::Stake(stake_meta, inactive_stake),
                ),
            ),
            (
                source,
                stake_account(5_000_000_000_000, LaunchStakeState::Initialized(stake_meta)),
            ),
            (CLOCK_SYSVAR_ID, clock_account(66)),
            (STAKE_HISTORY_SYSVAR_ID, stake_history_account(Vec::new())),
            (authority, crate::default_system_account()),
        ]);
        let destination_data = accounts[&destination].data.clone();
        let source_data = accounts[&source].data.clone();

        let mutation = apply_launch_stake_instruction(
            &instruction,
            &[
                meta(destination, false, true),
                meta(source, false, true),
                meta(CLOCK_SYSVAR_ID, false, false),
                meta(STAKE_HISTORY_SYSVAR_ID, false, false),
                meta(authority, true, false),
            ],
            &mut accounts,
            context_at(66, 0),
        )
        .unwrap();

        assert_eq!(
            mutation,
            LaunchStakeMutation::Merge {
                destination,
                source,
                lamports: 5_000_000_000_000,
            }
        );
        assert_eq!(accounts[&destination].lamports, 92_001_000_000_000);
        assert_eq!(accounts[&source].lamports, 0);
        assert_eq!(accounts[&destination].data, destination_data);
        assert_eq!(accounts[&source].data, source_data);
    }

    #[test]
    fn merge_preserves_historical_state_authority_and_metadata_error_order() {
        let destination = [0x76; 32];
        let source = [0x77; 32];
        let authority = [0x78; 32];
        let stake_meta = LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: authority,
                withdrawer: [0x79; 32],
            },
            lockup: LaunchStakeLockup::default(),
        };
        let active_stake = LaunchStake {
            delegation: LaunchDelegation {
                voter_pubkey: [0x7a; 32],
                stake: 900,
                activation_epoch: u64::MAX,
                deactivation_epoch: u64::MAX,
                warmup_cooldown_rate: 0.25,
            },
            credits_observed: 1,
        };
        let instruction = [7, 0, 0, 0];
        let metas = [
            meta(destination, false, true),
            meta(source, false, true),
            meta(CLOCK_SYSVAR_ID, false, false),
            meta(STAKE_HISTORY_SYSVAR_ID, false, false),
            meta(authority, true, false),
        ];

        let mut active_destination = AccountMap::from([
            (
                destination,
                stake_account(1_000, LaunchStakeState::Stake(stake_meta, active_stake)),
            ),
            (
                source,
                stake_account(1_000, LaunchStakeState::Initialized(stake_meta)),
            ),
            (CLOCK_SYSVAR_ID, clock_account(66)),
            (STAKE_HISTORY_SYSVAR_ID, stake_history_account(Vec::new())),
        ]);
        let before = active_destination.clone();
        assert_eq!(
            apply_launch_stake_instruction(
                &instruction,
                &metas,
                &mut active_destination,
                context_at(66, 0),
            )
            .unwrap_err(),
            LaunchStakeError::MergeActivatedStake {
                pubkey: destination,
            }
        );
        assert_eq!(active_destination, before);

        // Destination authorization is checked before source state decoding.
        let mut missing_authority = AccountMap::from([
            (
                destination,
                stake_account(1_000, LaunchStakeState::Initialized(stake_meta)),
            ),
            (source, stake_account(1_000, LaunchStakeState::RewardsPool)),
            (CLOCK_SYSVAR_ID, clock_account(66)),
            (STAKE_HISTORY_SYSVAR_ID, stake_history_account(Vec::new())),
        ]);
        let before = missing_authority.clone();
        let mut unsigned_metas = metas;
        unsigned_metas[4].is_signer = false;
        assert_eq!(
            apply_launch_stake_instruction(
                &instruction,
                &unsigned_metas,
                &mut missing_authority,
                context_at(66, 0),
            )
            .unwrap_err(),
            LaunchStakeError::MissingRequiredSignature { pubkey: authority }
        );
        assert_eq!(missing_authority, before);

        let different_meta = LaunchStakeMeta {
            rent_exempt_reserve: 101,
            ..stake_meta
        };
        let mut mismatch = AccountMap::from([
            (
                destination,
                stake_account(1_000, LaunchStakeState::Initialized(stake_meta)),
            ),
            (
                source,
                stake_account(1_000, LaunchStakeState::Initialized(different_meta)),
            ),
            (CLOCK_SYSVAR_ID, clock_account(66)),
            (STAKE_HISTORY_SYSVAR_ID, stake_history_account(Vec::new())),
        ]);
        let before = mismatch.clone();
        assert_eq!(
            apply_launch_stake_instruction(&instruction, &metas, &mut mismatch, context_at(66, 0),)
                .unwrap_err(),
            LaunchStakeError::MergeMismatch {
                destination,
                source_pubkey: source,
            }
        );
        assert_eq!(mismatch, before);
    }

    #[test]
    fn merge_duplicate_account_alias_is_a_valid_balance_noop() {
        let stake_pubkey = [0x7b; 32];
        let authority = [0x7c; 32];
        let stake_meta = LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: authority,
                withdrawer: authority,
            },
            lockup: LaunchStakeLockup::default(),
        };
        let mut accounts = AccountMap::from([
            (
                stake_pubkey,
                stake_account(5_000, LaunchStakeState::Initialized(stake_meta)),
            ),
            (CLOCK_SYSVAR_ID, clock_account(66)),
            (STAKE_HISTORY_SYSVAR_ID, stake_history_account(Vec::new())),
            (authority, crate::default_system_account()),
        ]);
        let before = accounts.clone();

        let mutation = apply_launch_stake_instruction(
            &[7, 0, 0, 0],
            &[
                meta(stake_pubkey, false, true),
                meta(stake_pubkey, false, true),
                meta(CLOCK_SYSVAR_ID, false, false),
                meta(STAKE_HISTORY_SYSVAR_ID, false, false),
                meta(authority, true, false),
            ],
            &mut accounts,
            context_at(66, 0),
        )
        .unwrap();

        assert_eq!(
            mutation,
            LaunchStakeMutation::Merge {
                destination: stake_pubkey,
                source: stake_pubkey,
                lamports: 5_000,
            }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn authorize_staker_accepts_the_withdrawer_signature() {
        let stake_pubkey = [20; 32];
        let withdrawer = [21; 32];
        let new_authority = [22; 32];
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: [23; 32],
                withdrawer,
            },
            lockup: LaunchStakeLockup::default(),
        });
        let mut data = vec![1, 0, 0, 0];
        data.extend_from_slice(&new_authority);
        data.extend_from_slice(&0_u32.to_le_bytes());
        let mut accounts = AccountMap::from([(stake_pubkey, stake_account(1_000, state))]);
        apply_launch_stake_instruction(
            &data,
            &[
                meta(stake_pubkey, false, true),
                meta(CLOCK_SYSVAR_ID, false, false),
                meta(withdrawer, true, false),
            ],
            &mut accounts,
            context(),
        )
        .unwrap();
        let LaunchStakeState::Initialized(meta) =
            decode_launch_stake_state(stake_pubkey, &accounts[&stake_pubkey].data).unwrap()
        else {
            panic!("stake must remain initialized");
        };
        assert_eq!(meta.authorized.staker, new_authority);
    }

    #[test]
    fn stake_authorize_stops_enforcing_lockup_at_v1_1_6_corpus_boundary() {
        let stake_pubkey = [0x81; 32];
        let staker = [0x82; 32];
        let withdrawer = [0x83; 32];
        let new_authority = [0x84; 32];
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized { staker, withdrawer },
            lockup: LaunchStakeLockup {
                unix_timestamp: i64::MAX,
                epoch: u64::MAX,
                custodian: [0x85; 32],
            },
        });
        let mut staker_instruction = vec![1, 0, 0, 0];
        staker_instruction.extend_from_slice(&new_authority);
        staker_instruction.extend_from_slice(&0_u32.to_le_bytes());
        let mut accounts = AccountMap::from([(stake_pubkey, stake_account(1_000, state))]);
        let before = accounts.clone();

        assert_eq!(
            apply_launch_stake_instruction(
                &staker_instruction,
                &[
                    meta(stake_pubkey, false, true),
                    meta(CLOCK_SYSVAR_ID, false, false),
                    meta(staker, true, false),
                ],
                &mut accounts,
                context_at_slot(STAKE_AUTHORIZE_LOCKUP_REMOVAL_SLOT - 1, 25, 0),
            )
            .unwrap_err(),
            LaunchStakeError::LockupInForce {
                pubkey: stake_pubkey,
            }
        );
        assert_eq!(accounts, before);

        // v1.1.6 keeps Clock only as a reserved client-side meta. The native
        // processor no longer consumes it, so the two-account shape succeeds.
        apply_launch_stake_instruction(
            &staker_instruction,
            &[meta(stake_pubkey, false, true), meta(staker, true, false)],
            &mut accounts,
            context_at_slot(STAKE_AUTHORIZE_LOCKUP_REMOVAL_SLOT, 25, 0),
        )
        .unwrap();

        let mut withdrawer_instruction = vec![1, 0, 0, 0];
        withdrawer_instruction.extend_from_slice(&new_authority);
        withdrawer_instruction.extend_from_slice(&1_u32.to_le_bytes());
        apply_launch_stake_instruction(
            &withdrawer_instruction,
            &[
                meta(stake_pubkey, false, true),
                meta(withdrawer, true, false),
            ],
            &mut accounts,
            context_at_slot(STAKE_AUTHORIZE_LOCKUP_REMOVAL_SLOT, 25, 0),
        )
        .unwrap();

        let LaunchStakeState::Initialized(meta) =
            decode_launch_stake_state(stake_pubkey, &accounts[&stake_pubkey].data).unwrap()
        else {
            panic!("stake must remain initialized");
        };
        assert_eq!(meta.authorized.staker, new_authority);
        assert_eq!(meta.authorized.withdrawer, new_authority);
    }

    #[test]
    fn withdraw_moves_unstaked_lamports() {
        let stake_pubkey = [30; 32];
        let destination = [31; 32];
        let withdrawer = destination;
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: [32; 32],
                withdrawer,
            },
            lockup: LaunchStakeLockup::default(),
        });
        let mut accounts = AccountMap::from([(stake_pubkey, stake_account(1_100, state))]);
        apply_launch_stake_instruction(
            &[4, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0],
            &[
                meta(stake_pubkey, false, true),
                meta(destination, true, true),
                meta(CLOCK_SYSVAR_ID, false, false),
                meta(STAKE_HISTORY_SYSVAR_ID, false, false),
            ],
            &mut accounts,
            context(),
        )
        .unwrap();
        assert_eq!(accounts[&stake_pubkey].lamports, 100);
        assert_eq!(accounts[&destination].lamports, 1_000);
    }

    #[test]
    fn launch_decoder_accepts_trailing_bytes_and_validates_variant_payloads() {
        let mut split = vec![3, 0, 0, 0];
        split.extend_from_slice(&400_u64.to_le_bytes());
        split.extend_from_slice(&[0xaa, 0xbb]);
        assert_eq!(
            decode_instruction(&split).unwrap(),
            DecodedStakeInstruction::Split { lamports: 400 }
        );
        assert_eq!(
            decode_instruction(&[2, 0, 0, 0]).unwrap(),
            DecodedStakeInstruction::DelegateStake
        );
        let set_lockup = wincode::serialize(&LaunchStakeInstructionV100::SetLockup(
            LaunchStakeLockupArgs {
                unix_timestamp: None,
                epoch: None,
                custodian: None,
            },
        ))
        .unwrap();
        assert_eq!(set_lockup, [6, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            decode_instruction(&set_lockup).unwrap(),
            DecodedStakeInstruction::SetLockup {
                lockup: LaunchStakeLockupArgs {
                    unix_timestamp: None,
                    epoch: None,
                    custodian: None,
                },
            }
        );
        assert_eq!(
            decode_instruction(&[0, 0, 0, 0]).unwrap_err(),
            LaunchStakeError::InvalidInstructionData
        );
    }

    #[test]
    fn epoch_11_set_lockup_fixture_decodes_and_updates_only_epoch() {
        // Mainnet slot 4,831,848, transaction 0, instruction 0:
        // stake 2vjVjxpPVpNJqUayjWBfeJm4StMR7bmJkudDs7FsDxcF,
        // custodian Mc5XB47H3DKJHym5RLa9mPzWv5snERsF3KNv5AauXK8.
        let instruction = [
            6, 0, 0, 0, // SetLockup
            0, // unix_timestamp: None
            1, 177, 0, 0, 0, 0, 0, 0, 0, // epoch: Some(177)
            0, // custodian: None
        ];
        assert_eq!(
            decode_instruction(&instruction).unwrap(),
            DecodedStakeInstruction::SetLockup {
                lockup: LaunchStakeLockupArgs {
                    unix_timestamp: None,
                    epoch: Some(177),
                    custodian: None,
                },
            }
        );

        let stake_pubkey = [0x61; 32];
        let custodian = [0x62; 32];
        let authorized = LaunchStakeAuthorized {
            staker: [0x63; 32],
            withdrawer: [0x64; 32],
        };
        let original_lockup = LaunchStakeLockup {
            unix_timestamp: 1_700_000_000,
            epoch: 160,
            custodian,
        };
        let original_state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: 123,
            authorized,
            lockup: original_lockup,
        });
        let state_wire_len = wincode::serialize(&original_state).unwrap().len();
        let mut account = stake_account(1_000, original_state);
        account.data[state_wire_len..].fill(0xa5);
        let mut accounts = AccountMap::from([(stake_pubkey, account)]);

        let mutation = apply_launch_stake_instruction(
            &instruction,
            &[
                meta(stake_pubkey, false, true),
                meta(custodian, true, false),
            ],
            &mut accounts,
            context(),
        )
        .unwrap();
        let expected_lockup = LaunchStakeLockup {
            epoch: 177,
            ..original_lockup
        };
        assert_eq!(
            mutation,
            LaunchStakeMutation::SetLockup {
                stake_account: stake_pubkey,
                lockup: expected_lockup,
            }
        );
        assert_eq!(
            accounts[&stake_pubkey].data.len(),
            LAUNCH_STAKE_ACCOUNT_DATA_LEN
        );
        assert_eq!(
            decode_launch_stake_state(stake_pubkey, &accounts[&stake_pubkey].data).unwrap(),
            LaunchStakeState::Initialized(LaunchStakeMeta {
                rent_exempt_reserve: 123,
                authorized,
                lockup: expected_lockup,
            })
        );
        assert!(
            accounts[&stake_pubkey].data[state_wire_len..]
                .iter()
                .all(|byte| *byte == 0xa5)
        );
    }

    #[test]
    fn set_lockup_preserves_delegated_stake_and_none_fields() {
        let stake_pubkey = [0x65; 32];
        let custodian = [0x66; 32];
        let lockup = LaunchStakeLockup {
            unix_timestamp: 55,
            epoch: 66,
            custodian,
        };
        let stake = LaunchStake {
            delegation: LaunchDelegation {
                voter_pubkey: [0x67; 32],
                stake: 900,
                activation_epoch: 4,
                deactivation_epoch: u64::MAX,
                warmup_cooldown_rate: 0.25,
            },
            credits_observed: 77,
        };
        let state = LaunchStakeState::Stake(
            LaunchStakeMeta {
                rent_exempt_reserve: 100,
                authorized: LaunchStakeAuthorized {
                    staker: [0x68; 32],
                    withdrawer: [0x69; 32],
                },
                lockup,
            },
            stake,
        );
        let mut accounts = AccountMap::from([(stake_pubkey, stake_account(1_000, state))]);
        apply_launch_stake_instruction(
            &set_lockup_data(Some(88), None, None),
            &[
                meta(stake_pubkey, false, true),
                meta(custodian, true, false),
            ],
            &mut accounts,
            context(),
        )
        .unwrap();

        let LaunchStakeState::Stake(meta, updated_stake) =
            decode_launch_stake_state(stake_pubkey, &accounts[&stake_pubkey].data).unwrap()
        else {
            panic!("delegated Stake state must be preserved");
        };
        assert_eq!(updated_stake, stake);
        assert_eq!(
            meta.lockup,
            LaunchStakeLockup {
                unix_timestamp: 88,
                ..lockup
            }
        );
    }

    #[test]
    fn set_lockup_always_requires_current_custodian_not_withdrawer_or_new_custodian() {
        let stake_pubkey = [0x70; 32];
        let custodian = [0x71; 32];
        let withdrawer = [0x72; 32];
        let new_custodian = [0x73; 32];
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: [0x74; 32],
                withdrawer,
            },
            lockup: LaunchStakeLockup {
                unix_timestamp: 1,
                epoch: 1,
                custodian,
            },
        });
        let instruction = set_lockup_data(None, None, Some(new_custodian));

        for wrong_signer in [None, Some(withdrawer), Some(new_custodian)] {
            let mut accounts = AccountMap::from([(stake_pubkey, stake_account(1_000, state))]);
            let before = accounts.clone();
            let mut metas = vec![meta(stake_pubkey, false, true)];
            if let Some(wrong_signer) = wrong_signer {
                metas.push(meta(wrong_signer, true, false));
            }
            assert_eq!(
                apply_launch_stake_instruction(
                    &instruction,
                    &metas,
                    &mut accounts,
                    context_at(2, 2),
                )
                .unwrap_err(),
                LaunchStakeError::MissingRequiredSignature { pubkey: custodian }
            );
            assert_eq!(accounts, before);
        }

        let mut accounts = AccountMap::from([(stake_pubkey, stake_account(1_000, state))]);
        apply_launch_stake_instruction(
            &instruction,
            &[
                meta(stake_pubkey, false, true),
                meta(custodian, true, false),
            ],
            &mut accounts,
            context_at(2, 2),
        )
        .unwrap();
        let before = accounts.clone();
        assert_eq!(
            apply_launch_stake_instruction(
                &set_lockup_data(None, Some(9), None),
                &[
                    meta(stake_pubkey, false, true),
                    meta(custodian, true, false),
                ],
                &mut accounts,
                context_at(2, 2),
            )
            .unwrap_err(),
            LaunchStakeError::MissingRequiredSignature {
                pubkey: new_custodian,
            }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn set_lockup_does_not_consult_clock_or_lockup_in_force() {
        let stake_pubkey = [0x75; 32];
        let custodian = [0x76; 32];
        let withdrawer = [0x77; 32];
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: [0x78; 32],
                withdrawer,
            },
            lockup: LaunchStakeLockup {
                unix_timestamp: 10,
                epoch: 10,
                custodian,
            },
        });

        for clock in [context_at(0, 0), context_at(11, 11)] {
            let mut accounts = AccountMap::from([(stake_pubkey, stake_account(1_000, state))]);
            assert_eq!(
                apply_launch_stake_instruction(
                    &set_lockup_data(None, Some(20), None),
                    &[
                        meta(stake_pubkey, false, true),
                        meta(withdrawer, true, false),
                    ],
                    &mut accounts,
                    clock,
                )
                .unwrap_err(),
                LaunchStakeError::MissingRequiredSignature { pubkey: custodian }
            );

            apply_launch_stake_instruction(
                &set_lockup_data(None, Some(20), None),
                &[
                    meta(stake_pubkey, false, true),
                    meta(custodian, true, false),
                ],
                &mut accounts,
                clock,
            )
            .unwrap();
        }
    }

    #[test]
    fn set_lockup_verifier_and_state_errors_are_atomic_and_launch_ordered() {
        let stake_pubkey = [0x79; 32];
        let custodian = [0x7a; 32];
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: [0x7b; 32],
                withdrawer: [0x7c; 32],
            },
            lockup: LaunchStakeLockup {
                custodian,
                ..LaunchStakeLockup::default()
            },
        });
        let instruction = set_lockup_data(Some(123), None, None);

        let mut readonly = AccountMap::from([(stake_pubkey, stake_account(1_000, state))]);
        let before = readonly.clone();
        assert_eq!(
            apply_launch_stake_instruction(
                &instruction,
                &[
                    meta(stake_pubkey, false, false),
                    meta(custodian, true, false),
                ],
                &mut readonly,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::ReadonlyDataModified {
                pubkey: stake_pubkey,
            }
        );
        assert_eq!(readonly, before);

        let mut wrong_owner_account = stake_account(1_000, state);
        wrong_owner_account.owner = [0x7d; 32];
        let mut wrong_owner = AccountMap::from([(stake_pubkey, wrong_owner_account)]);
        let before = wrong_owner.clone();
        assert_eq!(
            apply_launch_stake_instruction(
                &instruction,
                &[
                    meta(stake_pubkey, false, true),
                    meta(custodian, true, false),
                ],
                &mut wrong_owner,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::ExternalAccountDataModified {
                pubkey: stake_pubkey,
            }
        );
        assert_eq!(wrong_owner, before);

        let mut malformed = AccountMap::from([(
            stake_pubkey,
            AccountSnapshot {
                lamports: 1_000,
                owner: STAKE_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
                data: vec![0xff; LAUNCH_STAKE_ACCOUNT_DATA_LEN].into(),
            },
        )]);
        let before = malformed.clone();
        assert_eq!(
            apply_launch_stake_instruction(
                &instruction,
                &[meta(stake_pubkey, false, true)],
                &mut malformed,
                context(),
            )
            .unwrap_err(),
            LaunchStakeError::InvalidAccountData {
                pubkey: stake_pubkey,
            }
        );
        assert_eq!(malformed, before);
    }

    #[test]
    fn self_withdraw_is_a_net_zero_native_alias_operation() {
        let stake_pubkey = [35; 32];
        let withdrawer = [36; 32];
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: [37; 32],
                withdrawer,
            },
            lockup: LaunchStakeLockup::default(),
        });
        let mut accounts = AccountMap::from([(stake_pubkey, stake_account(1_100, state))]);
        apply_launch_stake_instruction(
            &[4, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0],
            &[
                meta(stake_pubkey, false, true),
                meta(stake_pubkey, false, true),
                meta(CLOCK_SYSVAR_ID, false, false),
                meta(STAKE_HISTORY_SYSVAR_ID, false, false),
                meta(withdrawer, true, false),
            ],
            &mut accounts,
            context(),
        )
        .unwrap();
        assert_eq!(accounts[&stake_pubkey].lamports, 1_100);
        assert_eq!(
            decode_launch_stake_state(stake_pubkey, &accounts[&stake_pubkey].data).unwrap(),
            state
        );
    }

    #[test]
    fn readonly_split_is_rejected_by_the_post_verifier_and_is_atomic() {
        let source = [40; 32];
        let destination = [41; 32];
        let authority = [42; 32];
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: 100,
            authorized: LaunchStakeAuthorized {
                staker: authority,
                withdrawer: authority,
            },
            lockup: LaunchStakeLockup::default(),
        });
        let mut accounts = AccountMap::from([
            (source, stake_account(1_000, state)),
            (
                destination,
                stake_account(0, LaunchStakeState::Uninitialized),
            ),
        ]);
        let before = accounts.clone();
        let error = apply_launch_stake_instruction(
            &[3, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0],
            &[
                meta(source, false, true),
                meta(destination, false, false),
                meta(authority, true, false),
            ],
            &mut accounts,
            context(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LaunchStakeError::ReadonlyLamportChange { pubkey } if pubkey == destination
        ));
        assert_eq!(accounts, before);
    }
}
