//! Launch-era (`v1.0.7`) vote-state mutation used by the trusted-history POC.
//!
//! This is intentionally not the current Agave vote program.  Its serialized
//! types mirror the source that mainnet launched with, because even a logically
//! equivalent modern type can produce different account bytes.  Replay trusts
//! archived successful transactions, so cryptographic signature checks and the
//! vote hash/slot-history acceptance checks are outside this mutator.

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
    collections::{BTreeMap, VecDeque},
    mem::size_of,
};
use thiserror::Error;

use crate::{AccountMap, CowAccountMap, AccountSnapshot, CLOCK_SYSVAR_ID, LaunchAccountMeta, RENT_SYSVAR_ID};

const MAX_LOCKOUT_HISTORY: usize = 31;
const MAX_EPOCH_CREDITS_HISTORY: usize = 64;
const INITIAL_LOCKOUT: u64 = 2;
const PRIOR_VOTER_ITEMS: usize = 32;
const ACCOUNT_STORAGE_OVERHEAD: u64 = 128;
const PACKET_DATA_SIZE: u64 = 1_232;
// Solana PR #8947 added the validator-identity signature to Vote account
// initialization. Mainnet's first Compact transaction whose outcome depends
// on that v1.1 behavior is slot 641,616; earlier v1.0 initializations with the
// original three-account shape are canonical and must remain accepted.
pub(crate) const INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT: u64 = 641_616;

type Pubkey = [u8; 32];
type Hash = [u8; 32];

/// `Vote111111111111111111111111111111111111111`.
pub const VOTE_PROGRAM_ID: [u8; 32] = [
    7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179, 216, 53, 94, 115, 209,
    16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0,
];

/// Ephemeral decoded state for the replay-only, single-instruction Vote path.
///
/// Canonical account bytes remain the source of truth at every externally
/// observable boundary. The sequential allocation-minimal replay experiment
/// may defer serialization across adjacent direct Votes, but it materializes
/// before generic account reads, account deletion, checkpoints, and outcomes.
/// This cache is never checkpointed or hashed.
#[derive(Debug)]
struct CachedVoteState {
    state: VoteStateV100,
    /// Whether `state` is already represented by the canonical account's
    /// `VoteStateVersions::Current` prefix. A decoded V0_23_5 account is
    /// converted in memory, but its first successful Vote must still rewrite
    /// the discriminant and migrated layout even when every Vote field is a
    /// semantic no-op.
    canonical_state_is_current: bool,
    /// The epoch for which `get_and_update_authorized_voter`'s insertion and
    /// stale-entry purge have already been reflected in `state`.
    normalized_authorized_epoch: Option<u64>,
    /// Avoid another ordered-set lookup after this direct path has already
    /// reported a byte-changing commit for the account.
    changed_account_already_recorded: bool,
    /// Complete proof for the latest logical state whose bytes have not yet
    /// reached the canonical account. Keeping the prepared layout makes a
    /// later materialization infallible while the account shape is unchanged.
    pending_encoding: Option<PreparedVoteStateEncoding>,
    #[cfg(test)]
    direct_authorized_voter_normalizations: u64,
}

impl CachedVoteState {
    fn decoded(versioned: VoteStateVersionsV100, account_data: &[u8]) -> Self {
        let canonical_state_is_current = match &versioned {
            VoteStateVersionsV100::Current(state) => {
                current_vote_state_wire_is_canonical(account_data, state)
            }
            VoteStateVersionsV100::V0_23_5(_) => false,
        };
        Self {
            state: versioned.into_current(),
            canonical_state_is_current,
            normalized_authorized_epoch: None,
            changed_account_already_recorded: false,
            pending_encoding: None,
            #[cfg(test)]
            direct_authorized_voter_normalizations: 0,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct LaunchVoteStateCache {
    states: HashMap<Pubkey, CachedVoteState>,
    /// Accounts with a pending canonical prefix, appended only on the clean to
    /// dirty transition. This keeps slot/outcome barriers proportional to the
    /// touched Vote set instead of scanning the epoch-wide decoded registry.
    dirty_accounts: Vec<Pubkey>,
    /// Host-only experiment counters. They are deliberately absent from the
    /// portable checkpoint wire and restart when a checkpoint is restored.
    lazy_direct_commits: u64,
    materializations: u64,
    #[cfg(test)]
    fast_path_commits: u64,
}

impl LaunchVoteStateCache {
    pub(crate) fn from_accounts(
        accounts: &crate::MemoryAccountStore,
        vote_program: Pubkey,
    ) -> Self {
        let mut cache = Self::default();
        accounts.visit_sorted(&mut |pubkey, account| {
            if account.owner == vote_program {
                cache.seed(pubkey, &account.data);
            }
        });
        cache
    }

    pub(crate) fn seed(&mut self, pubkey: Pubkey, account_data: &[u8]) {
        if let Ok(state) = decode_vote_state(account_data) {
            self.states
                .insert(pubkey, CachedVoteState::decoded(state, account_data));
        }
    }

    pub(crate) fn invalidate(&mut self, pubkey: Pubkey) {
        if self.states.remove(&pubkey).is_some()
            && let Some(index) = self
                .dirty_accounts
                .iter()
                .position(|dirty| *dirty == pubkey)
        {
            self.dirty_accounts.swap_remove(index);
        }
    }

    /// Publish one pending logical Vote state into canonical account bytes.
    ///
    /// `pending_encoding` was prepared after the latest logical mutation and
    /// neither data length nor owner can change without a replay barrier that
    /// calls this method first. A missing/non-Vote account therefore denotes a
    /// caller invariant violation, never a recoverable replay condition.
    pub(crate) fn materialize_account(
        &mut self,
        pubkey: Pubkey,
        account: &mut AccountSnapshot,
    ) -> bool {
        let Some(cached) = self.states.get_mut(&pubkey) else {
            return false;
        };
        let Some(encoding) = cached.pending_encoding.take() else {
            return false;
        };
        assert_eq!(
            account.owner, VOTE_PROGRAM_ID,
            "dirty Vote cache entry must retain Vote ownership until its barrier"
        );
        encode_prepared_vote_state_v100(&cached.state, account.data.as_mut_slice(), encoding);
        cached.canonical_state_is_current = true;
        self.materializations = self.materializations.saturating_add(1);
        if let Some(index) = self
            .dirty_accounts
            .iter()
            .position(|dirty| *dirty == pubkey)
        {
            self.dirty_accounts.swap_remove(index);
        }
        true
    }

    pub(crate) fn materialize_referenced(
        &mut self,
        accounts: &mut crate::MemoryAccountStore,
        pubkeys: &[[u8; 32]],
    ) {
        for &pubkey in pubkeys {
            if !self
                .states
                .get(&pubkey)
                .is_some_and(|cached| cached.pending_encoding.is_some())
            {
                continue;
            }
            let account = accounts
                .get_mut(&pubkey)
                .expect("dirty Vote cache entry must have a canonical account until deletion");
            self.materialize_account(pubkey, account);
        }
    }

    pub(crate) fn materialize_all(&mut self, accounts: &mut crate::MemoryAccountStore) {
        let mut dirty_accounts = std::mem::take(&mut self.dirty_accounts);
        for pubkey in dirty_accounts.drain(..) {
            let Some(cached) = self.states.get_mut(&pubkey) else {
                continue;
            };
            let Some(encoding) = cached.pending_encoding.take() else {
                continue;
            };
            let account = accounts
                .get_mut(&pubkey)
                .expect("dirty Vote cache entry must have a canonical account until deletion");
            assert_eq!(
                account.owner, VOTE_PROGRAM_ID,
                "dirty Vote cache entry must retain Vote ownership until its barrier"
            );
            encode_prepared_vote_state_v100(&cached.state, account.data.as_mut_slice(), encoding);
            cached.canonical_state_is_current = true;
            self.materializations = self.materializations.saturating_add(1);
        }
        self.dirty_accounts = dirty_accounts;
    }

    pub(crate) fn lazy_direct_commits(&self) -> u64 {
        self.lazy_direct_commits
    }

    pub(crate) fn materializations(&self) -> u64 {
        self.materializations
    }

    pub(crate) fn has_pending_materializations(&self) -> bool {
        !self.dirty_accounts.is_empty()
    }

    /// Move one vote account's decoded state into a transaction-local cache.
    ///
    /// Independent vote transactions can then advance distinct cache entries on
    /// worker threads without placing a lock around the launch-era fast path.
    /// The caller must merge the returned cache before processing another
    /// transaction that references `pubkey`.
    pub(crate) fn take_account(&mut self, pubkey: Pubkey) -> Self {
        let mut account_cache = Self::default();
        if let Some(state) = self.states.remove(&pubkey) {
            if state.pending_encoding.is_some()
                && let Some(index) = self
                    .dirty_accounts
                    .iter()
                    .position(|dirty| *dirty == pubkey)
            {
                self.dirty_accounts.swap_remove(index);
                account_cache.dirty_accounts.push(pubkey);
            }
            account_cache.states.insert(pubkey, state);
        }
        account_cache
    }

    /// Return a transaction-local cache created by [`Self::take_account`].
    pub(crate) fn merge_account(&mut self, mut account_cache: Self) {
        debug_assert!(account_cache.states.len() <= 1);
        self.states.extend(account_cache.states.drain());
        self.dirty_accounts
            .append(&mut account_cache.dirty_accounts);
        self.lazy_direct_commits = self
            .lazy_direct_commits
            .saturating_add(account_cache.lazy_direct_commits);
        self.materializations = self
            .materializations
            .saturating_add(account_cache.materializations);
        #[cfg(test)]
        {
            self.fast_path_commits = self
                .fast_path_commits
                .saturating_add(account_cache.fast_path_commits);
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pubkey: &Pubkey) -> bool {
        self.states.contains_key(pubkey)
    }

    #[cfg(test)]
    pub(crate) fn fast_path_commits(&self) -> u64 {
        self.fast_path_commits
    }

    #[cfg(test)]
    fn normalized_authorized_epoch(&self, pubkey: &Pubkey) -> Option<u64> {
        self.states
            .get(pubkey)
            .and_then(|cached| cached.normalized_authorized_epoch)
    }

    #[cfg(test)]
    fn direct_authorized_voter_normalizations(&self, pubkey: &Pubkey) -> u64 {
        self.states
            .get(pubkey)
            .map_or(0, |cached| cached.direct_authorized_voter_normalizations)
    }
}

/// Cheap, mutation-free guard for the replay-only direct Vote path.
///
/// This deliberately mirrors every guard that can return `Fallback` before
/// decoded vote state is touched. A caller may use it to form an independent
/// worker batch while keeping uncommon Vote instructions on the generic path.
pub(crate) fn launch_vote_direct_shape_supported(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    vote_account: &AccountSnapshot,
) -> bool {
    let Some(vote_meta) = account_metas.first() else {
        return false;
    };
    vote_meta.is_writable
        && vote_account.owner == VOTE_PROGRAM_ID
        && !account_metas.iter().enumerate().any(|(index, meta)| {
            account_metas[index + 1..]
                .iter()
                .any(|later| later.pubkey == meta.pubkey)
        })
        && launch_vote_direct_wire_supported(instruction_data)
}

#[inline]
pub(crate) fn launch_vote_direct_wire_supported(instruction_data: &[u8]) -> bool {
    BorrowedVoteV100::parse(instruction_data).is_some()
}

/// Result of attempting the allocation-minimal, single-instruction Vote path.
///
/// `Fallback` is not an execution failure. It asks launch replay to use the
/// generic transaction overlay, which remains the semantic oracle for every
/// uncommon or ambiguous shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaunchFastVoteApply {
    Applied {
        account_changed: bool,
        record_changed_account: bool,
    },
    Fallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedVoteMutation {
    pub voted_slots: Vec<u64>,
    pub timestamp: Option<i64>,
    pub root_slot: Option<u64>,
    pub credits: u64,
}

/// State mutation performed by one launch-era Vote instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchVoteMutation {
    InitializeAccount {
        node_pubkey: Pubkey,
        authorized_voter: Pubkey,
        authorized_withdrawer: Pubkey,
        commission: u8,
        epoch: u64,
    },
    Authorize {
        old_authority: Pubkey,
        new_authority: Pubkey,
        authority_type: LaunchVoteAuthorize,
        effective_epoch: Option<u64>,
    },
    UpdateCommission {
        old_commission: u8,
        new_commission: u8,
    },
    Vote(TrustedVoteMutation),
    Withdraw {
        destination: Pubkey,
        lamports: u64,
    },
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
pub enum LaunchVoteAuthorize {
    Voter,
    Withdrawer,
}

#[derive(Debug, Error)]
pub enum LaunchVoteError {
    #[error("vote instruction is missing account position {position}")]
    MissingAccount { position: usize },
    #[error("vote instruction account {pubkey:?} is absent from the transaction overlay")]
    MissingAccountState { pubkey: Pubkey },
    #[error("decode launch-era vote instruction: {0}")]
    DecodeInstruction(#[source] wincode::error::Error),
    #[error("launch-era vote instruction {0} is not implemented by the epoch replay POC")]
    UnsupportedInstruction(&'static str),
    #[error("decode launch-era vote account: {0}")]
    DecodeAccount(#[source] wincode::error::Error),
    #[error("vote account is uninitialized")]
    UninitializedAccount,
    #[error("vote account is already initialized")]
    AccountAlreadyInitialized,
    #[error("vote instruction account {position} is {found:?}, expected sysvar {expected:?}")]
    InvalidSysvar {
        position: usize,
        expected: Pubkey,
        found: Pubkey,
    },
    #[error("vote instruction sysvar account {position} ({pubkey:?}) has invalid data")]
    InvalidSysvarData { position: usize, pubkey: Pubkey },
    #[error("vote account {pubkey:?} is not rent exempt: balance={balance}, required={required}")]
    InsufficientFunds {
        pubkey: Pubkey,
        balance: u64,
        required: u64,
    },
    #[error("vote authority {pubkey:?} did not sign")]
    MissingRequiredSignature { pubkey: Pubkey },
    #[error("authorized voter has already been changed for epoch {epoch}")]
    TooSoonToReauthorize { epoch: u64 },
    #[error("vote contains no slots")]
    EmptySlots,
    #[error("vote timestamp moves backwards")]
    TimestampTooOld,
    #[error("vote account has no authorized voter for epoch {0}")]
    MissingAuthorizedVoter(u64),
    #[error("serialize launch-era vote account: {0}")]
    EncodeAccount(#[source] wincode::error::Error),
    #[error("serialized vote state needs {needed} bytes but the account has {available}")]
    AccountDataTooSmall { needed: usize, available: usize },
    #[error("Vote program changed the owner of account {pubkey:?}")]
    ModifiedProgramId { pubkey: Pubkey },
    #[error("Vote program spent lamports from externally owned account {pubkey:?}")]
    ExternalAccountLamportSpend { pubkey: Pubkey },
    #[error("read-only account {pubkey:?} changed lamports")]
    ReadonlyLamportChange { pubkey: Pubkey },
    #[error("Vote program resized account {pubkey:?}")]
    AccountDataSizeChanged { pubkey: Pubkey },
    #[error("read-only account {pubkey:?} changed data")]
    ReadonlyDataModified { pubkey: Pubkey },
    #[error("Vote program changed data in externally owned account {pubkey:?}")]
    ExternalAccountDataModified { pubkey: Pubkey },
    #[error("Vote program made an invalid executable change to account {pubkey:?}")]
    ExecutableModified { pubkey: Pubkey },
    #[error("Vote program changed rent_epoch on account {pubkey:?}")]
    RentEpochModified { pubkey: Pubkey },
    #[error("Vote instruction is unbalanced: pre={pre_lamports}, post={post_lamports}")]
    UnbalancedInstruction {
        pre_lamports: u128,
        post_lamports: u128,
    },
}

/// Apply one v1.0.7 Vote instruction atomically to a transaction overlay.
///
/// Initialize, Authorize, and Withdraw preserve the native program's account,
/// sysvar, signer-meta, and error ordering. Vote and VoteSwitch remain the
/// replay POC's trusted-history fast path: signatures, SlotHashes/switch
/// proofs, and bank hashes are deliberately not verified, but their serialized
/// state transition is launch-era exact.
pub fn apply_launch_vote_instruction(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    trusted_vote_epoch: u64,
) -> Result<LaunchVoteMutation, LaunchVoteError> {
    let mut working = CowAccountMap::detached(accounts.clone());
    let mutation = apply_launch_vote_instruction_on_overlay(
        instruction_data,
        account_metas,
        &mut working,
        trusted_vote_epoch,
    )?;
    *accounts = working.into_local();
    Ok(mutation)
}

/// Replay-only fast path. The transaction overlay is disposable on error, so
/// it does not clone every Vote account a second time merely to provide the
/// standalone API's instruction-level atomicity.
pub(crate) fn apply_launch_vote_instruction_in_place(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    trusted_vote_epoch: u64,
) -> Result<LaunchVoteMutation, LaunchVoteError> {
    let mut cow = CowAccountMap::detached(std::mem::take(accounts));
    let result = apply_launch_vote_instruction_on_overlay(
        instruction_data, account_metas, &mut cow, trusted_vote_epoch,
    );
    *accounts = cow.into_local();
    result
}

pub(crate) fn apply_launch_vote_instruction_on_overlay(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut CowAccountMap,
    trusted_vote_epoch: u64,
) -> Result<LaunchVoteMutation, LaunchVoteError> {
    apply_launch_vote_instruction_in_place_impl(
        instruction_data,
        account_metas,
        accounts,
        trusted_vote_epoch,
        None,
    )
    .map(|(mutation, _)| mutation)
}

/// Cache-aware replay path for a Vote-program transaction that contains
/// exactly one instruction. The boolean result reports whether decoded Vote
/// state came from the cache; non-Vote variants always report `false`.
pub(crate) fn apply_launch_vote_instruction_in_place_cached(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    trusted_vote_epoch: u64,
    cache: &mut LaunchVoteStateCache,
) -> Result<(LaunchVoteMutation, bool), LaunchVoteError> {
    let mut cow = CowAccountMap::detached(std::mem::take(accounts));
    let result = apply_launch_vote_instruction_on_overlay_cached(
        instruction_data, account_metas, &mut cow, trusted_vote_epoch, cache,
    );
    *accounts = cow.into_local();
    result
}

pub(crate) fn apply_launch_vote_instruction_on_overlay_cached(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut CowAccountMap,
    trusted_vote_epoch: u64,
    cache: &mut LaunchVoteStateCache,
) -> Result<(LaunchVoteMutation, bool), LaunchVoteError> {
    let vote_pubkey = account_metas.first().map(|meta| meta.pubkey);
    let result = apply_launch_vote_instruction_in_place_impl(
        instruction_data,
        account_metas,
        accounts,
        trusted_vote_epoch,
        Some(cache),
    );
    if result.is_err()
        && let Some(vote_pubkey) = vote_pubkey
    {
        cache.invalidate(vote_pubkey);
    }
    result
}

/// Attempt the replay-only fast path for launch-era `Vote` and `VoteSwitch`.
///
/// The caller must still restrict this to a transaction containing exactly one
/// instruction and to a mode that retains no instruction effect or diff. This
/// function adds the account-local guards: the vote account must be writable,
/// owned by the Vote program, and occur only once in the instruction metas.
/// Any guard or wire-shape miss returns [`LaunchFastVoteApply::Fallback`]
/// without changing the account or advancing cached state.
pub(crate) fn try_apply_launch_vote_direct_cached(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    vote_account: &mut AccountSnapshot,
    trusted_vote_epoch: u64,
    cache: &mut LaunchVoteStateCache,
) -> Result<LaunchFastVoteApply, LaunchVoteError> {
    try_apply_launch_vote_direct_cached_impl(
        instruction_data,
        account_metas,
        vote_account,
        trusted_vote_epoch,
        cache,
        false,
    )
}

/// Sequential-only direct Vote path that advances decoded logical state while
/// deferring its canonical prefix write until an explicit replay barrier.
pub(crate) fn try_apply_launch_vote_direct_cached_lazy(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    vote_account: &mut AccountSnapshot,
    trusted_vote_epoch: u64,
    cache: &mut LaunchVoteStateCache,
) -> Result<LaunchFastVoteApply, LaunchVoteError> {
    try_apply_launch_vote_direct_cached_impl(
        instruction_data,
        account_metas,
        vote_account,
        trusted_vote_epoch,
        cache,
        true,
    )
}

fn try_apply_launch_vote_direct_cached_impl(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    vote_account: &mut AccountSnapshot,
    trusted_vote_epoch: u64,
    cache: &mut LaunchVoteStateCache,
    defer_materialization: bool,
) -> Result<LaunchFastVoteApply, LaunchVoteError> {
    let Some(vote_meta) = account_metas.first() else {
        return Ok(LaunchFastVoteApply::Fallback);
    };
    if !vote_meta.is_writable
        || vote_account.owner != VOTE_PROGRAM_ID
        || account_metas.iter().enumerate().any(|(index, meta)| {
            account_metas[index + 1..]
                .iter()
                .any(|later| later.pubkey == meta.pubkey)
        })
    {
        return Ok(LaunchFastVoteApply::Fallback);
    }
    let Some(vote) = BorrowedVoteV100::parse(instruction_data) else {
        return Ok(LaunchFastVoteApply::Fallback);
    };

    let applied = {
        let (states, dirty_accounts, materializations) = (
            &mut cache.states,
            &mut cache.dirty_accounts,
            &mut cache.materializations,
        );
        let cached = match states.entry(vote_meta.pubkey) {
            hashbrown::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            hashbrown::hash_map::Entry::Vacant(entry) => {
                let state = decode_vote_state(&vote_account.data)?;
                entry.insert(CachedVoteState::decoded(state, &vote_account.data))
            }
        };
        let mut dirty_was_tracked = cached.pending_encoding.is_some();

        // If this input's conservative post-state bound does not fit, publish
        // an older pending commit before touching logical state. A subsequent
        // direct-path fallback can then safely discard the cache and let the
        // generic oracle reproduce the current transaction from canonical
        // bytes without losing earlier committed Votes.
        if defer_materialization
            && cached.pending_encoding.is_some()
            && (!borrowed_vote_post_state_fits(cached, vote, vote_account.data.len())
                || borrowed_vote_may_shrink_encoded_state(cached, vote, trusted_vote_epoch))
        {
            let encoding = cached
                .pending_encoding
                .take()
                .expect("dirty Vote state carries a prepared encoding");
            encode_prepared_vote_state_v100(
                &cached.state,
                vote_account.data.as_mut_slice(),
                encoding,
            );
            cached.canonical_state_is_current = true;
            *materializations = materializations.saturating_add(1);
            let index = dirty_accounts
                .iter()
                .position(|dirty| *dirty == vote_meta.pubkey)
                .expect("pending Vote encoding is tracked in the dirty queue");
            dirty_accounts.swap_remove(index);
            dirty_was_tracked = false;
        }

        // Every fallible semantic check happens against immutable cached
        // state. Only after this succeeds may the cache advance. A failed
        // transaction therefore leaves decoded canonical state hot.
        let preflight =
            preflight_borrowed_vote(&cached.state, vote, trusted_vote_epoch, account_metas)?;
        if cached.normalized_authorized_epoch != Some(trusted_vote_epoch) {
            if preflight.authorized_voter_normalization_changes {
                cached.state.normalize_authorized_voter_for_epoch(
                    trusted_vote_epoch,
                    preflight.authorized_voter,
                );
            }
            cached.normalized_authorized_epoch = Some(trusted_vote_epoch);
            #[cfg(test)]
            {
                cached.direct_authorized_voter_normalizations = cached
                    .direct_authorized_voter_normalizations
                    .saturating_add(1);
            }
        }
        if preflight.vote_slots_change {
            for slot in vote.slots() {
                cached.state.process_slot(slot, trusted_vote_epoch);
            }
        }
        if preflight.timestamp_changes {
            cached
                .state
                .process_timestamp(
                    preflight.max_voted_slot,
                    vote.timestamp
                        .expect("timestamp change requires a proposed timestamp"),
                )
                .expect("immutable Vote preflight validated the timestamp");
        }

        let account_changed = !cached.canonical_state_is_current
            || preflight.authorized_voter_normalization_changes
            || preflight.vote_slots_change
            || preflight.timestamp_changes;
        if !account_changed {
            Some((false, false))
        } else if let Some(encoding) =
            prepare_vote_state_v100_encoding(&cached.state, vote_account.data.len())
        {
            let record_changed_account = !cached.changed_account_already_recorded;
            if defer_materialization {
                cached.pending_encoding = Some(encoding);
                if !dirty_was_tracked {
                    dirty_accounts.push(vote_meta.pubkey);
                }
            } else {
                // `encoding` proves every length conversion and output boundary
                // before the first canonical byte is touched. The exact-prefix
                // writer is therefore infallible for this state and deliberately
                // leaves the launch-era account-data tail untouched.
                encode_prepared_vote_state_v100(
                    &cached.state,
                    vote_account.data.as_mut_slice(),
                    encoding,
                );
                cached.canonical_state_is_current = true;
                cached.pending_encoding = None;
            }
            // Logical state and either its canonical bytes or prepared lazy
            // encoding are complete before this hint advances. Failures and
            // no-op Votes leave it false.
            cached.changed_account_already_recorded = true;
            Some((true, record_changed_account))
        } else {
            // Cached state may have advanced but canonical bytes have not.
            // Drop it and let the generic path reproduce the exact legacy
            // encoding error and ordering from canonical bytes.
            None
        }
    };

    let Some((account_changed, record_changed_account)) = applied else {
        cache.invalidate(vote_meta.pubkey);
        return Ok(LaunchFastVoteApply::Fallback);
    };
    if defer_materialization && account_changed {
        cache.lazy_direct_commits = cache.lazy_direct_commits.saturating_add(1);
    }
    #[cfg(test)]
    {
        cache.fast_path_commits = cache.fast_path_commits.saturating_add(1);
    }
    Ok(LaunchFastVoteApply::Applied {
        account_changed,
        record_changed_account,
    })
}

/// Conservative, allocation-free proof that every dynamic Vote-state field
/// still fits after this borrowed Vote. It intentionally assumes every input
/// slot can append both a lockout and an epoch-credit row, and that authorized
/// voter normalization can add one map entry. When the bound does not fit, the
/// lazy path materializes the prior state before mutation to retain exact
/// fallback atomicity.
fn borrowed_vote_post_state_fits(
    cached: &CachedVoteState,
    vote: BorrowedVoteV100<'_>,
    available: usize,
) -> bool {
    const FIXED_BYTES: usize = 1_655;
    const LOCKOUT_BYTES: usize = 12;
    const ROOT_SLOT_BYTES: usize = 8;
    const AUTHORIZED_VOTER_BYTES: usize = 40;
    const EPOCH_CREDITS_BYTES: usize = 24;

    // `process_slot` never retains more than the launch lockout window, and
    // credit history is pruned to its launch-era cap after every insertion.
    // Applying those exact caps keeps this proof conservative without forcing
    // a materialization for ordinary multi-slot Votes at steady state.
    let slots = vote.slots().len();
    let vote_rows = cached
        .state
        .votes
        .len()
        .saturating_add(slots)
        .min(MAX_LOCKOUT_HISTORY);
    let epoch_credit_rows = cached
        .state
        .epoch_credits
        .len()
        .saturating_add(slots)
        .min(MAX_EPOCH_CREDITS_HISTORY);
    FIXED_BYTES
        .checked_add(vote_rows.checked_mul(LOCKOUT_BYTES).unwrap_or(usize::MAX))
        .and_then(|bytes| bytes.checked_add(ROOT_SLOT_BYTES))
        .and_then(|bytes| {
            cached
                .state
                .authorized_voters
                .authorized_voters
                .len()
                .checked_add(1)
                .and_then(|rows| rows.checked_mul(AUTHORIZED_VOTER_BYTES))
                .and_then(|dynamic| bytes.checked_add(dynamic))
        })
        .and_then(|bytes| {
            epoch_credit_rows
                .checked_mul(EPOCH_CREDITS_BYTES)
                .and_then(|dynamic| bytes.checked_add(dynamic))
        })
        .is_some_and(|needed| needed <= available)
}

/// Whether this instruction can leave a shorter Current prefix than the
/// pending logical state. The launch serializer overwrites only that prefix,
/// so an eager replay preserves bytes from the immediately preceding, longer
/// state in the newly exposed tail. Materializing the pending state before a
/// possible shrink reproduces those exact historical allocation bytes.
fn borrowed_vote_may_shrink_encoded_state(
    cached: &CachedVoteState,
    vote: BorrowedVoteV100<'_>,
    trusted_vote_epoch: u64,
) -> bool {
    if cached.normalized_authorized_epoch != Some(trusted_vote_epoch)
        && cached
            .state
            .authorized_voters
            .authorized_voters
            .first_key_value()
            .is_some_and(|(epoch, _)| *epoch < trusted_vote_epoch)
    {
        // Epoch normalization can remove older 40-byte map rows. The replay
        // runner already materializes at epoch boundaries; retain this local
        // proof so the lazy primitive stays exact for standalone callers too.
        return true;
    }
    let mut slots = vote.slots();
    let slot_count = slots.len();
    if slot_count == 0 {
        return false;
    }
    if cached.state.votes.len() > MAX_LOCKOUT_HISTORY {
        // Historical valid state is bounded by this window. Stay fail-closed
        // for a malformed decoded account instead of spilling the simulator.
        return true;
    }
    if slot_count != 1 {
        let initial_rows = cached.state.votes.len();
        let mut simulated = SmallVec::<[LockoutV100; MAX_LOCKOUT_HISTORY]>::new();
        simulated.extend(cached.state.votes.iter().copied());
        for slot in slots {
            if simulated
                .last()
                .is_some_and(|old_vote| old_vote.slot >= slot)
            {
                continue;
            }
            while simulated
                .last()
                .is_some_and(|lockout| lockout.expiration_slot() < slot)
            {
                simulated.pop();
            }
            if simulated.len() == MAX_LOCKOUT_HISTORY {
                simulated.remove(0);
            }
            simulated.push(LockoutV100 {
                slot,
                confirmation_count: 1,
            });
            let stack_depth = simulated.len();
            for (index, lockout) in simulated.iter_mut().enumerate() {
                if stack_depth > index + lockout.confirmation_count as usize {
                    lockout.confirmation_count += 1;
                }
            }
        }
        return simulated.len() < initial_rows;
    }
    let slot = slots
        .next()
        .expect("one borrowed Vote slot was established above");
    // One expired back entry is replaced by the new lockout, preserving the
    // encoded row count. Two expired entries are the first case where the
    // post-instruction prefix is strictly shorter than the pending prefix.
    cached
        .state
        .votes
        .iter()
        .rev()
        .take_while(|lockout| lockout.expiration_slot() < slot)
        .take(2)
        .count()
        == 2
}

fn apply_launch_vote_instruction_in_place_impl(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut CowAccountMap,
    trusted_vote_epoch: u64,
    cache: Option<&mut LaunchVoteStateCache>,
) -> Result<(LaunchVoteMutation, bool), LaunchVoteError> {
    let pre_accounts = launch_pre_accounts(account_metas, accounts)?;
    let (mutation, cache_hit) = apply_launch_vote_inner(
        instruction_data,
        account_metas,
        accounts,
        trusted_vote_epoch,
        cache,
    )?;
    verify_launch_vote_instruction(&pre_accounts, accounts)?;
    Ok((mutation, cache_hit))
}

fn apply_launch_vote_inner(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut CowAccountMap,
    trusted_vote_epoch: u64,
    mut cache: Option<&mut LaunchVoteStateCache>,
) -> Result<(LaunchVoteMutation, bool), LaunchVoteError> {
    // v1.0.7 advances the keyed-account iterator to `me` before decoding the
    // instruction. Preserve that observable NotEnoughAccountKeys ordering.
    let vote_meta = required_meta(account_metas, 0)?;
    required_account(accounts, vote_meta.pubkey)?;
    let instruction = decode_instruction(instruction_data)?;
    match instruction {
        VoteInstructionV100::InitializeAccount(vote_init) => {
            if let Some(cache) = cache.as_deref_mut() {
                cache.invalidate(vote_meta.pubkey);
            }
            // `verify_rent_exemption()` consumes and validates Rent before the
            // Clock account is even requested.
            let rent = read_rent(account_metas, accounts, 1)?;
            let vote_account = required_account(accounts, vote_meta.pubkey)?;
            let required = rent.minimum_balance(vote_account.data.len());
            if vote_account.lamports < required {
                return Err(LaunchVoteError::InsufficientFunds {
                    pubkey: vote_meta.pubkey,
                    balance: vote_account.lamports,
                    required,
                });
            }
            let clock = read_clock(account_metas, accounts, 2)?;
            initialize_account(accounts, vote_meta.pubkey, vote_init, account_metas, clock)
                .map(|mutation| (mutation, false))
        }
        VoteInstructionV100::Authorize(new_authority, authority_type) => {
            if let Some(cache) = cache.as_deref_mut() {
                cache.invalidate(vote_meta.pubkey);
            }
            // The historical processor decodes Clock before `authorize()`
            // deserializes VoteState.
            let clock = read_clock(account_metas, accounts, 1)?;
            authorize(
                accounts,
                vote_meta.pubkey,
                new_authority,
                authority_type,
                account_metas,
                clock,
            )
            .map(|mutation| (mutation, false))
        }
        VoteInstructionV100::Vote(vote) | VoteInstructionV100::VoteSwitch(vote, _) => {
            let account = required_account_mut(accounts, vote_meta.pubkey)?;
            if let Some(cache) = cache {
                apply_decoded_trusted_vote_cached(
                    &mut account.data,
                    vote,
                    trusted_vote_epoch,
                    account_metas,
                    vote_meta.pubkey,
                    cache,
                )
                .map(|(mutation, cache_hit)| (LaunchVoteMutation::Vote(mutation), cache_hit))
            } else {
                apply_decoded_trusted_vote(
                    &mut account.data,
                    vote,
                    trusted_vote_epoch,
                    Some(account_metas),
                )
                .map(|mutation| (LaunchVoteMutation::Vote(mutation), false))
            }
        }
        VoteInstructionV100::Withdraw(lamports) => {
            if let Some(cache) = cache.as_deref_mut() {
                cache.invalidate(vote_meta.pubkey);
            }
            // The destination is fetched before `withdraw()` decodes state.
            let destination = required_meta(account_metas, 1)?.pubkey;
            required_account(accounts, destination)?;
            withdraw(
                accounts,
                vote_meta.pubkey,
                destination,
                lamports,
                account_metas,
            )
            .map(|mutation| (mutation, false))
        }
        VoteInstructionV100::UpdateCommission(commission) => {
            if let Some(cache) = cache.as_deref_mut() {
                cache.invalidate(vote_meta.pubkey);
            }
            update_commission(accounts, vote_meta.pubkey, commission, account_metas)
                .map(|mutation| (mutation, false))
        }
        VoteInstructionV100::UpdateNode(_) => {
            if let Some(cache) = cache {
                cache.invalidate(vote_meta.pubkey);
            }
            Err(LaunchVoteError::UnsupportedInstruction("UpdateNode"))
        }
    }
}

/// Allocation-free view of the fixed-int bincode wire for the launch-era
/// `Vote` and `VoteSwitch` instruction variants. Any malformed or different
/// instruction falls back to the generic bincode decoder so its exact error
/// remains authoritative. The switch proof is intentionally only shape-
/// checked because trusted replay does not verify the voted fork.
#[derive(Debug, Clone, Copy)]
struct BorrowedVoteV100<'a> {
    slot_bytes: &'a [u8],
    timestamp: Option<i64>,
}

impl<'a> BorrowedVoteV100<'a> {
    fn parse(data: &'a [u8]) -> Option<Self> {
        const VOTE_DISCRIMINANT: u32 = 2;
        const VOTE_SWITCH_DISCRIMINANT: u32 = 6;
        const ENUM_BYTES: usize = size_of::<u32>();
        const VEC_LENGTH_BYTES: usize = size_of::<u64>();
        const HASH_BYTES: usize = 32;

        let switch_proof_bytes = match read_u32(data, 0)? {
            VOTE_DISCRIMINANT => 0,
            VOTE_SWITCH_DISCRIMINANT => HASH_BYTES,
            _ => return None,
        };
        let slot_count = usize::try_from(read_u64(data, ENUM_BYTES)?).ok()?;
        let slots_start = ENUM_BYTES.checked_add(VEC_LENGTH_BYTES)?;
        let slots_bytes = slot_count.checked_mul(size_of::<u64>())?;
        let slots_end = slots_start.checked_add(slots_bytes)?;
        let timestamp_tag_offset = slots_end.checked_add(HASH_BYTES)?;
        let timestamp_tag = *data.get(timestamp_tag_offset)?;
        let (timestamp, consumed) = match timestamp_tag {
            0 => (None, timestamp_tag_offset.checked_add(1)?),
            1 => {
                let timestamp_offset = timestamp_tag_offset.checked_add(1)?;
                (
                    Some(i64::from_le_bytes(
                        data.get(timestamp_offset..timestamp_offset.checked_add(8)?)?
                            .try_into()
                            .ok()?,
                    )),
                    timestamp_offset.checked_add(8)?,
                )
            }
            _ => return None,
        };
        let decoded_end = consumed.checked_add(switch_proof_bytes)?;
        data.get(consumed..decoded_end)?;
        if decoded_end as u64 > PACKET_DATA_SIZE {
            return None;
        }
        Some(Self {
            slot_bytes: data.get(slots_start..slots_end)?,
            timestamp,
        })
    }

    fn slots(self) -> impl ExactSizeIterator<Item = u64> + Clone + 'a {
        self.slot_bytes.chunks_exact(size_of::<u64>()).map(|bytes| {
            u64::from_le_bytes(
                bytes
                    .try_into()
                    .expect("borrowed Vote slots are validated eight-byte chunks"),
            )
        })
    }
}

fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        data.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset.checked_add(8)?)?.try_into().ok()?,
    ))
}

/// Prove that the only map on the Current wire was already encoded in the
/// deterministic order produced by bincode/serde.
///
/// `BTreeMap` deserialization accepts out-of-order and duplicate keys, then
/// canonicalizes them in memory. All other Current fields have a one-to-one
/// fixed-int representation (or retain their sequence order), so matching the
/// wire map against the decoded sorted map is sufficient to know whether a
/// semantic no-op may safely skip the generic canonical rewrite.
fn current_vote_state_wire_is_canonical(data: &[u8], state: &VoteStateV100) -> bool {
    const CURRENT_VARIANT: u32 = 1;
    const FIXED_PREFIX_BYTES: usize = size_of::<u32>() + 32 + 32 + size_of::<u8>();
    const LENGTH_BYTES: usize = size_of::<u64>();
    const LOCKOUT_BYTES: usize = size_of::<u64>() + size_of::<u32>();
    const AUTHORIZED_VOTER_BYTES: usize = size_of::<u64>() + 32;

    let Some(wire_votes_len) =
        read_u64(data, FIXED_PREFIX_BYTES).and_then(|length| usize::try_from(length).ok())
    else {
        return false;
    };
    if read_u32(data, 0) != Some(CURRENT_VARIANT) || wire_votes_len != state.votes.len() {
        return false;
    }
    let Some(mut offset) = FIXED_PREFIX_BYTES
        .checked_add(LENGTH_BYTES)
        .and_then(|offset| {
            wire_votes_len
                .checked_mul(LOCKOUT_BYTES)
                .and_then(|votes_bytes| offset.checked_add(votes_bytes))
        })
    else {
        return false;
    };

    let Some(root_tag) = data.get(offset).copied() else {
        return false;
    };
    offset = match (root_tag, state.root_slot) {
        (0, None) => match offset.checked_add(1) {
            Some(offset) => offset,
            None => return false,
        },
        (1, Some(expected_root))
            if offset
                .checked_add(1)
                .and_then(|root_offset| read_u64(data, root_offset))
                == Some(expected_root) =>
        {
            match offset.checked_add(1 + size_of::<u64>()) {
                Some(offset) => offset,
                None => return false,
            }
        }
        _ => return false,
    };

    let Some(wire_authorized_voters_len) =
        read_u64(data, offset).and_then(|length| usize::try_from(length).ok())
    else {
        return false;
    };
    if wire_authorized_voters_len != state.authorized_voters.authorized_voters.len() {
        return false;
    }
    let Some(entries_start) = offset.checked_add(LENGTH_BYTES) else {
        return false;
    };
    for (index, (expected_epoch, expected_voter)) in
        state.authorized_voters.authorized_voters.iter().enumerate()
    {
        let Some(entry_start) = index
            .checked_mul(AUTHORIZED_VOTER_BYTES)
            .and_then(|entry_bytes| entries_start.checked_add(entry_bytes))
        else {
            return false;
        };
        let Some(voter_start) = entry_start.checked_add(size_of::<u64>()) else {
            return false;
        };
        let Some(entry_end) = entry_start.checked_add(AUTHORIZED_VOTER_BYTES) else {
            return false;
        };
        if read_u64(data, entry_start) != Some(*expected_epoch)
            || data.get(voter_start..entry_end) != Some(expected_voter.as_slice())
        {
            return false;
        }
    }
    true
}

#[derive(Debug, Clone, Copy)]
struct BorrowedVotePreflight {
    max_voted_slot: u64,
    authorized_voter: Pubkey,
    authorized_voter_normalization_changes: bool,
    vote_slots_change: bool,
    timestamp_changes: bool,
}

fn preflight_borrowed_vote(
    state: &VoteStateV100,
    vote: BorrowedVoteV100<'_>,
    epoch: u64,
    signer_metas: &[LaunchAccountMeta],
) -> Result<BorrowedVotePreflight, LaunchVoteError> {
    if state.authorized_voters.is_empty() {
        return Err(LaunchVoteError::UninitializedAccount);
    }
    let authorized_voter = state.authorized_voter_for_epoch(epoch)?;
    verify_authorized_signer(authorized_voter, signer_metas)?;
    let max_voted_slot = vote.slots().max().ok_or(LaunchVoteError::EmptySlots)?;
    let authorized_voter_normalization_changes = !state
        .authorized_voters
        .authorized_voters
        .contains_key(&epoch)
        || state
            .authorized_voters
            .authorized_voters
            .first_key_value()
            .is_some_and(|(authorized_epoch, _)| *authorized_epoch < epoch);
    let vote_slots_change = state
        .votes
        .back()
        .is_none_or(|last_vote| max_voted_slot > last_vote.slot);
    let mut timestamp_changes = false;
    if let Some(timestamp) = vote.timestamp {
        state.check_timestamp(max_voted_slot, timestamp)?;
        timestamp_changes = state.last_timestamp
            != (BlockTimestampV100 {
                slot: max_voted_slot,
                timestamp,
            });
    }
    Ok(BorrowedVotePreflight {
        max_voted_slot,
        authorized_voter,
        authorized_voter_normalization_changes,
        vote_slots_change,
        timestamp_changes,
    })
}

fn decode_instruction(data: &[u8]) -> Result<VoteInstructionV100, LaunchVoteError> {
    // Mirrors v1.0.7 `program_utils::limited_deserialize`: fixed integers,
    // trailing bytes accepted, and a packet-sized allocation budget.
    if data.len() > PACKET_DATA_SIZE as usize {
        let limit_error = wincode::error::ReadError::from(
            wincode::error::preallocation_size_limit(data.len(), PACKET_DATA_SIZE as usize),
        );
        return Err(LaunchVoteError::DecodeInstruction(
            wincode::error::Error::ReadError(limit_error),
        ));
    }
    wincode::config::deserialize(
        data,
        wincode::config::Configuration::default().with_fixint_encoding(),
    )
    .map_err(|source| LaunchVoteError::DecodeInstruction(wincode::error::Error::ReadError(source)))
}

fn initialize_account(
    accounts: &mut CowAccountMap,
    vote_pubkey: Pubkey,
    vote_init: VoteInitV100,
    account_metas: &[LaunchAccountMeta],
    clock: ClockV100,
) -> Result<LaunchVoteMutation, LaunchVoteError> {
    let account = required_account_mut(accounts, vote_pubkey)?;
    let versioned = decode_vote_state(&account.data)?;
    if !versioned.is_uninitialized() {
        return Err(LaunchVoteError::AccountAlreadyInitialized);
    }
    if clock.slot >= INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT {
        verify_authorized_signer(vote_init.node_pubkey, account_metas)?;
    }
    let state = VoteStateV100::new(&vote_init, clock.epoch);
    write_vote_state(&mut account.data, &state)?;
    Ok(LaunchVoteMutation::InitializeAccount {
        node_pubkey: vote_init.node_pubkey,
        authorized_voter: vote_init.authorized_voter,
        authorized_withdrawer: vote_init.authorized_withdrawer,
        commission: vote_init.commission,
        epoch: clock.epoch,
    })
}

fn authorize(
    accounts: &mut CowAccountMap,
    vote_pubkey: Pubkey,
    new_authority: Pubkey,
    authority_type: LaunchVoteAuthorize,
    account_metas: &[LaunchAccountMeta],
    clock: ClockV100,
) -> Result<LaunchVoteMutation, LaunchVoteError> {
    let account = required_account_mut(accounts, vote_pubkey)?;
    let mut state = decode_vote_state(&account.data)?.into_current();
    let (old_authority, effective_epoch) = match authority_type {
        LaunchVoteAuthorize::Voter => {
            let old_authority = state.get_and_update_authorized_voter(clock.epoch)?;
            verify_authorized_signer(old_authority, account_metas)?;
            let target_epoch = clock.leader_schedule_epoch.wrapping_add(1);
            state.set_new_authorized_voter(new_authority, target_epoch)?;
            (old_authority, Some(target_epoch))
        }
        LaunchVoteAuthorize::Withdrawer => {
            let old_authority = state.authorized_withdrawer;
            verify_authorized_signer(old_authority, account_metas)?;
            state.authorized_withdrawer = new_authority;
            (old_authority, None)
        }
    };
    write_vote_state(&mut account.data, &state)?;
    Ok(LaunchVoteMutation::Authorize {
        old_authority,
        new_authority,
        authority_type,
        effective_epoch,
    })
}

fn withdraw(
    accounts: &mut CowAccountMap,
    vote_pubkey: Pubkey,
    destination: Pubkey,
    lamports: u64,
    account_metas: &[LaunchAccountMeta],
) -> Result<LaunchVoteMutation, LaunchVoteError> {
    let state = decode_vote_state(&required_account(accounts, vote_pubkey)?.data)?.into_current();
    verify_authorized_signer(state.authorized_withdrawer, account_metas)?;
    let balance = required_account(accounts, vote_pubkey)?.lamports;
    if balance < lamports {
        return Err(LaunchVoteError::InsufficientFunds {
            pubkey: vote_pubkey,
            balance,
            required: lamports,
        });
    }

    if vote_pubkey == destination {
        // Sequential RefCell borrows in v1.0.7 make a self-withdraw legal and
        // net-neutral; reproduce that without holding two map borrows.
        let account = required_account_mut(accounts, vote_pubkey)?;
        account.lamports = account.lamports.wrapping_sub(lamports);
        account.lamports = account.lamports.wrapping_add(lamports);
    } else {
        required_account_mut(accounts, vote_pubkey)?.lamports = balance - lamports;
        let destination_account = required_account_mut(accounts, destination)?;
        destination_account.lamports = destination_account.lamports.wrapping_add(lamports);
    }
    Ok(LaunchVoteMutation::Withdraw {
        destination,
        lamports,
    })
}

fn update_commission(
    accounts: &mut CowAccountMap,
    vote_pubkey: Pubkey,
    new_commission: u8,
    account_metas: &[LaunchAccountMeta],
) -> Result<LaunchVoteMutation, LaunchVoteError> {
    // v1.2.32 deserializes VoteState before checking the withdraw authority.
    let account = required_account_mut(accounts, vote_pubkey)?;
    let mut state = decode_vote_state(&account.data)?.into_current();
    verify_authorized_signer(state.authorized_withdrawer, account_metas)?;
    let old_commission = state.commission;
    state.commission = new_commission;
    write_vote_state(&mut account.data, &state)?;
    Ok(LaunchVoteMutation::UpdateCommission {
        old_commission,
        new_commission,
    })
}

fn verify_authorized_signer(
    authority: Pubkey,
    account_metas: &[LaunchAccountMeta],
) -> Result<(), LaunchVoteError> {
    if account_metas
        .iter()
        .any(|meta| meta.is_signer && meta.pubkey == authority)
    {
        Ok(())
    } else {
        Err(LaunchVoteError::MissingRequiredSignature { pubkey: authority })
    }
}

/// Allocation-free encoder for the exact bincode 1.x fixed-int wire used by
/// `VoteStateVersions::Current` at launch. This intentionally remains separate
/// from [`write_vote_state`], whose serde/bincode implementation is the oracle
/// for generic replay and uncommon fallback paths.
#[cfg(test)]
fn encode_vote_state_v100_into(state: &VoteStateV100, output: &mut [u8]) -> Option<usize> {
    let encoding = prepare_vote_state_v100_encoding(state, output.len())?;
    encode_prepared_vote_state_v100(state, output, encoding);
    Some(encoding.encoded_len)
}

/// Complete precommit proof for a direct canonical Vote-state write.
///
/// Every conversion or boundary check that can depend on decoded account data
/// is captured here. Once this value exists, encoding contains no fallible
/// branch and can safely target the canonical account prefix without a second
/// full-state buffer.
#[derive(Debug, Clone, Copy)]
struct PreparedVoteStateEncoding {
    encoded_len: usize,
    votes_len: u64,
    authorized_voters_len: u64,
    prior_voters_idx: u64,
    epoch_credits_len: u64,
}

fn prepare_vote_state_v100_encoding(
    state: &VoteStateV100,
    available: usize,
) -> Option<PreparedVoteStateEncoding> {
    let encoded_len = vote_state_v100_encoded_len(state)?;
    if encoded_len > available {
        return None;
    }
    Some(PreparedVoteStateEncoding {
        encoded_len,
        votes_len: u64::try_from(state.votes.len()).ok()?,
        authorized_voters_len: u64::try_from(state.authorized_voters.authorized_voters.len())
            .ok()?,
        prior_voters_idx: u64::try_from(state.prior_voters.idx).ok()?,
        epoch_credits_len: u64::try_from(state.epoch_credits.len()).ok()?,
    })
}

fn encode_prepared_vote_state_v100(
    state: &VoteStateV100,
    output: &mut [u8],
    encoding: PreparedVoteStateEncoding,
) {
    const CURRENT_VARIANT: u32 = 1;

    // Check the caller's slice before the first write. All subsequent field
    // widths sum to `encoded_len`, as established by the prepared layout.
    let output = output
        .get_mut(..encoding.encoded_len)
        .expect("prepared Vote-state encoding fits its canonical account");
    let mut encoder = FixedVoteStateEncoder::new(output);
    encoder.write_u32(CURRENT_VARIANT);
    encoder.write_bytes(&state.node_pubkey);
    encoder.write_bytes(&state.authorized_withdrawer);
    encoder.write_u8(state.commission);

    encoder.write_u64(encoding.votes_len);
    for lockout in &state.votes {
        encoder.write_u64(lockout.slot);
        encoder.write_u32(lockout.confirmation_count);
    }

    match state.root_slot {
        None => encoder.write_u8(0),
        Some(root_slot) => {
            encoder.write_u8(1);
            encoder.write_u64(root_slot);
        }
    }

    encoder.write_u64(encoding.authorized_voters_len);
    for (epoch, voter) in &state.authorized_voters.authorized_voters {
        encoder.write_u64(*epoch);
        encoder.write_bytes(voter);
    }

    // `[I; 32]` and tuples carry no length prefix in serde/bincode. `usize`
    // is serialized through serde as a fixed-width u64, independent of host
    // pointer width.
    for (voter, start_epoch, end_epoch) in &state.prior_voters.buf {
        encoder.write_bytes(voter);
        encoder.write_u64(*start_epoch);
        encoder.write_u64(*end_epoch);
    }
    encoder.write_u64(encoding.prior_voters_idx);
    encoder.write_u8(u8::from(state.prior_voters.is_empty));

    encoder.write_u64(encoding.epoch_credits_len);
    for (epoch, credits, previous_credits) in &state.epoch_credits {
        encoder.write_u64(*epoch);
        encoder.write_u64(*credits);
        encoder.write_u64(*previous_credits);
    }
    encoder.write_u64(state.last_timestamp.slot);
    encoder.write_i64(state.last_timestamp.timestamp);
    assert_eq!(encoder.position(), encoding.encoded_len);
}

fn vote_state_v100_encoded_len(state: &VoteStateV100) -> Option<usize> {
    // Current discriminant, fixed scalar fields, empty dynamic lengths, the
    // complete 32-entry prior-voter ring, and the timestamp total 1,655 bytes.
    const FIXED_BYTES: usize = 1_655;
    const LOCKOUT_BYTES: usize = 12;
    const ROOT_SLOT_BYTES: usize = 8;
    const AUTHORIZED_VOTER_BYTES: usize = 40;
    const EPOCH_CREDITS_BYTES: usize = 24;

    FIXED_BYTES
        .checked_add(state.votes.len().checked_mul(LOCKOUT_BYTES)?)?
        .checked_add(state.root_slot.map_or(0, |_| ROOT_SLOT_BYTES))?
        .checked_add(
            state
                .authorized_voters
                .authorized_voters
                .len()
                .checked_mul(AUTHORIZED_VOTER_BYTES)?,
        )?
        .checked_add(state.epoch_credits.len().checked_mul(EPOCH_CREDITS_BYTES)?)
}

struct FixedVoteStateEncoder<'a> {
    output: &'a mut [u8],
    position: usize,
}

impl<'a> FixedVoteStateEncoder<'a> {
    #[inline]
    fn new(output: &'a mut [u8]) -> Self {
        Self {
            output,
            position: 0,
        }
    }

    #[inline]
    fn position(&self) -> usize {
        self.position
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.write_bytes(&[value])
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.write_bytes(&value.to_le_bytes())
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes())
    }

    #[inline]
    fn write_i64(&mut self, value: i64) {
        self.write_bytes(&value.to_le_bytes())
    }

    #[inline]
    fn write_bytes(&mut self, bytes: &[u8]) {
        let end = self
            .position
            .checked_add(bytes.len())
            .expect("prepared Vote-state encoding offset does not overflow");
        self.output
            .get_mut(self.position..end)
            .expect("prepared Vote-state encoding contains every field")
            .copy_from_slice(bytes);
        self.position = end;
    }
}

fn write_vote_state(account_data: &mut [u8], state: &VoteStateV100) -> Result<(), LaunchVoteError> {
    let versioned = VoteStateVersionsV100Ref::Current(state);
    let needed = wincode::serialized_size(&versioned)
        .map_err(|source| {
            LaunchVoteError::EncodeAccount(wincode::error::Error::WriteError(source))
        })?
        .try_into()
        .unwrap_or(usize::MAX);
    if needed > account_data.len() {
        return Err(LaunchVoteError::AccountDataTooSmall {
            needed,
            available: account_data.len(),
        });
    }
    // v1.0.7 `Account::serialize_data` writes only the encoded prefix.
    wincode::serialize_into(&mut account_data[..needed], &versioned).map_err(|source| {
        LaunchVoteError::EncodeAccount(wincode::error::Error::WriteError(source))
    })?;
    Ok(())
}

fn decode_vote_state(data: &[u8]) -> Result<VoteStateVersionsV100, LaunchVoteError> {
    wincode::deserialize(data)
        .map_err(|source| LaunchVoteError::DecodeAccount(wincode::error::Error::ReadError(source)))
}

fn required_meta(
    account_metas: &[LaunchAccountMeta],
    position: usize,
) -> Result<&LaunchAccountMeta, LaunchVoteError> {
    account_metas
        .get(position)
        .ok_or(LaunchVoteError::MissingAccount { position })
}

fn required_account<'a>(
    accounts: &'a CowAccountMap,
    pubkey: Pubkey,
) -> Result<&'a AccountSnapshot, LaunchVoteError> {
    accounts
        .get(&pubkey)
        .ok_or(LaunchVoteError::MissingAccountState { pubkey })
}

fn required_account_mut<'a>(
    accounts: &'a mut CowAccountMap,
    pubkey: Pubkey,
) -> Result<&'a mut AccountSnapshot, LaunchVoteError> {
    accounts
        .get_mut(&pubkey)
        .ok_or(LaunchVoteError::MissingAccountState { pubkey })
}

fn read_rent(
    account_metas: &[LaunchAccountMeta],
    accounts: &CowAccountMap,
    position: usize,
) -> Result<RentV100, LaunchVoteError> {
    let meta = required_meta(account_metas, position)?;
    if meta.pubkey != RENT_SYSVAR_ID {
        return Err(LaunchVoteError::InvalidSysvar {
            position,
            expected: RENT_SYSVAR_ID,
            found: meta.pubkey,
        });
    }
    wincode::deserialize(&required_account(accounts, meta.pubkey)?.data).map_err(|_| {
        LaunchVoteError::InvalidSysvarData {
            position,
            pubkey: meta.pubkey,
        }
    })
}

fn read_clock(
    account_metas: &[LaunchAccountMeta],
    accounts: &CowAccountMap,
    position: usize,
) -> Result<ClockV100, LaunchVoteError> {
    let meta = required_meta(account_metas, position)?;
    if meta.pubkey != CLOCK_SYSVAR_ID {
        return Err(LaunchVoteError::InvalidSysvar {
            position,
            expected: CLOCK_SYSVAR_ID,
            found: meta.pubkey,
        });
    }
    wincode::deserialize(&required_account(accounts, meta.pubkey)?.data).map_err(|_| {
        LaunchVoteError::InvalidSysvarData {
            position,
            pubkey: meta.pubkey,
        }
    })
}

/// Apply the state-changing part of a successful launch-era Vote instruction.
///
/// The caller is responsible for proving that `account_data` belongs to the
/// vote program and for selecting only transactions that the historical bank
/// committed.  This function deliberately does not verify signatures, slot
/// hashes, or the vote's bank hash.
pub fn apply_trusted_vote_instruction(
    account_data: &mut [u8],
    instruction_data: &[u8],
    epoch: u64,
) -> Result<TrustedVoteMutation, LaunchVoteError> {
    let instruction = decode_instruction(instruction_data)?;
    let vote = match instruction {
        VoteInstructionV100::Vote(vote) | VoteInstructionV100::VoteSwitch(vote, _) => vote,
        VoteInstructionV100::InitializeAccount(_) => {
            return Err(LaunchVoteError::UnsupportedInstruction("InitializeAccount"));
        }
        VoteInstructionV100::Authorize(_, _) => {
            return Err(LaunchVoteError::UnsupportedInstruction("Authorize"));
        }
        VoteInstructionV100::Withdraw(_) => {
            return Err(LaunchVoteError::UnsupportedInstruction("Withdraw"));
        }
        VoteInstructionV100::UpdateNode(_) => {
            return Err(LaunchVoteError::UnsupportedInstruction("UpdateNode"));
        }
        VoteInstructionV100::UpdateCommission(_) => {
            return Err(LaunchVoteError::UnsupportedInstruction("UpdateCommission"));
        }
    };
    apply_decoded_trusted_vote(account_data, vote, epoch, None)
}

fn apply_decoded_trusted_vote(
    account_data: &mut [u8],
    vote: VoteV100,
    epoch: u64,
    signer_metas: Option<&[LaunchAccountMeta]>,
) -> Result<TrustedVoteMutation, LaunchVoteError> {
    let versioned = decode_vote_state(account_data)?;
    let mut state = versioned.into_current();
    let mutation = apply_decoded_trusted_vote_state(&mut state, vote, epoch, signer_metas)?;
    write_vote_state(account_data, &state)?;
    Ok(mutation)
}

fn apply_decoded_trusted_vote_cached(
    account_data: &mut [u8],
    vote: VoteV100,
    epoch: u64,
    signer_metas: &[LaunchAccountMeta],
    vote_pubkey: Pubkey,
    cache: &mut LaunchVoteStateCache,
) -> Result<(TrustedVoteMutation, bool), LaunchVoteError> {
    let (cached, cache_hit) = match cache.states.entry(vote_pubkey) {
        hashbrown::hash_map::Entry::Occupied(entry) => (entry.into_mut(), true),
        hashbrown::hash_map::Entry::Vacant(entry) => {
            let state = decode_vote_state(account_data)?;
            (
                entry.insert(CachedVoteState::decoded(state, account_data)),
                false,
            )
        }
    };
    let mutation =
        apply_decoded_trusted_vote_state(&mut cached.state, vote, epoch, Some(signer_metas))?;
    write_vote_state(account_data, &cached.state)?;
    cached.canonical_state_is_current = true;
    cached.pending_encoding = None;
    cached.normalized_authorized_epoch = Some(epoch);
    Ok((mutation, cache_hit))
}

fn apply_decoded_trusted_vote_state(
    state: &mut VoteStateV100,
    vote: VoteV100,
    epoch: u64,
    signer_metas: Option<&[LaunchAccountMeta]>,
) -> Result<TrustedVoteMutation, LaunchVoteError> {
    if state.authorized_voters.is_empty() {
        return Err(LaunchVoteError::UninitializedAccount);
    }
    let authorized_voter = state.get_and_update_authorized_voter(epoch)?;
    if let Some(signer_metas) = signer_metas {
        verify_authorized_signer(authorized_voter, signer_metas)?;
    }
    if vote.slots.is_empty() {
        return Err(LaunchVoteError::EmptySlots);
    }

    for slot in vote.slots.iter().copied() {
        state.process_slot(slot, epoch);
    }
    if let Some(timestamp) = vote.timestamp {
        let slot = vote
            .slots
            .iter()
            .copied()
            .max()
            .ok_or(LaunchVoteError::EmptySlots)?;
        state.process_timestamp(slot, timestamp)?;
    }

    let mutation = TrustedVoteMutation {
        voted_slots: vote.slots,
        timestamp: vote.timestamp,
        root_slot: state.root_slot,
        credits: state.credits(),
    };
    Ok(mutation)
}

/// Decode the credits observed by launch-era Stake delegation.
pub fn decode_launch_vote_credits(_pubkey: Pubkey, data: &[u8]) -> Result<u64, LaunchVoteError> {
    Ok(decode_vote_state(data)?.into_current().credits())
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
enum VoteInstructionV100 {
    InitializeAccount(VoteInitV100),
    Authorize(Pubkey, LaunchVoteAuthorize),
    Vote(VoteV100),
    Withdraw(u64),
    UpdateNode(Pubkey),
    UpdateCommission(u8),
    VoteSwitch(VoteV100, Hash),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct VoteInitV100 {
    node_pubkey: Pubkey,
    authorized_voter: Pubkey,
    authorized_withdrawer: Pubkey,
    commission: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct VoteV100 {
    slots: Vec<u64>,
    hash: Hash,
    timestamp: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct ClockV100 {
    #[allow(dead_code)]
    slot: u64,
    #[allow(dead_code)]
    segment: u64,
    epoch: u64,
    leader_schedule_epoch: u64,
    #[allow(dead_code)]
    unix_timestamp: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct RentV100 {
    lamports_per_byte_year: u64,
    exemption_threshold: f64,
    #[allow(dead_code)]
    burn_percent: u8,
}

impl RentV100 {
    fn minimum_balance(self, data_len: usize) -> u64 {
        // This deliberately follows v1.0.7's release arithmetic and float-to-
        // integer conversion instead of a modern Rent helper.
        let bytes = data_len as u64;
        ((ACCOUNT_STORAGE_OVERHEAD
            .wrapping_add(bytes)
            .wrapping_mul(self.lamports_per_byte_year)) as f64
            * self.exemption_threshold) as u64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
// Current is deliberately inline: this short-lived decode value trades about
// 1.7 KiB of stack space for one heap allocation on every Vote instruction.
#[allow(clippy::large_enum_variant)]
enum VoteStateVersionsV100 {
    V0_23_5(Box<VoteState0235>),
    Current(VoteStateV100),
}

#[allow(dead_code)]
#[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
enum VoteStateVersionsV100Ref<'a> {
    V0_23_5(&'a VoteState0235),
    Current(&'a VoteStateV100),
}

impl VoteStateVersionsV100 {
    fn into_current(self) -> VoteStateV100 {
        match self {
            Self::Current(state) => state,
            Self::V0_23_5(state) => VoteStateV100::from_0235(*state),
        }
    }

    fn is_uninitialized(&self) -> bool {
        match self {
            Self::V0_23_5(state) => state.authorized_voter == [0; 32],
            Self::Current(state) => state.authorized_voters.is_empty(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct VoteStateV100 {
    node_pubkey: Pubkey,
    authorized_withdrawer: Pubkey,
    commission: u8,
    votes: VecDeque<LockoutV100>,
    root_slot: Option<u64>,
    authorized_voters: AuthorizedVotersV100,
    prior_voters: CircBufV100<(Pubkey, u64, u64)>,
    epoch_credits: Vec<(u64, u64, u64)>,
    last_timestamp: BlockTimestampV100,
}

impl VoteStateV100 {
    fn new(vote_init: &VoteInitV100, epoch: u64) -> Self {
        Self {
            node_pubkey: vote_init.node_pubkey,
            authorized_withdrawer: vote_init.authorized_withdrawer,
            commission: vote_init.commission,
            votes: VecDeque::new(),
            root_slot: None,
            authorized_voters: AuthorizedVotersV100 {
                authorized_voters: BTreeMap::from([(epoch, vote_init.authorized_voter)]),
            },
            prior_voters: CircBufV100::default(),
            epoch_credits: Vec::new(),
            last_timestamp: BlockTimestampV100::default(),
        }
    }

    fn from_0235(state: VoteState0235) -> Self {
        let mut authorized_voters = BTreeMap::new();
        authorized_voters.insert(state.authorized_voter_epoch, state.authorized_voter);
        Self {
            node_pubkey: state.node_pubkey,
            authorized_withdrawer: state.authorized_withdrawer,
            commission: state.commission,
            votes: state.votes,
            root_slot: state.root_slot,
            authorized_voters: AuthorizedVotersV100 { authorized_voters },
            prior_voters: CircBufV100::default(),
            epoch_credits: state.epoch_credits,
            last_timestamp: state.last_timestamp,
        }
    }

    fn get_and_update_authorized_voter(&mut self, epoch: u64) -> Result<Pubkey, LaunchVoteError> {
        let voter = self.authorized_voter_for_epoch(epoch)?;
        self.normalize_authorized_voter_for_epoch(epoch, voter);
        Ok(voter)
    }

    fn normalize_authorized_voter_for_epoch(&mut self, epoch: u64, voter: Pubkey) {
        self.authorized_voters
            .authorized_voters
            .entry(epoch)
            .or_insert(voter);
        self.authorized_voters
            .authorized_voters
            .retain(|authorized_epoch, _| *authorized_epoch >= epoch);
    }

    fn authorized_voter_for_epoch(&self, epoch: u64) -> Result<Pubkey, LaunchVoteError> {
        self.authorized_voters
            .authorized_voters
            .get(&epoch)
            .copied()
            .or_else(|| {
                self.authorized_voters
                    .authorized_voters
                    .range(..epoch)
                    .next_back()
                    .map(|(_, voter)| *voter)
            })
            .ok_or(LaunchVoteError::MissingAuthorizedVoter(epoch))
    }

    fn set_new_authorized_voter(
        &mut self,
        new_authority: Pubkey,
        target_epoch: u64,
    ) -> Result<(), LaunchVoteError> {
        if self
            .authorized_voters
            .authorized_voters
            .contains_key(&target_epoch)
        {
            return Err(LaunchVoteError::TooSoonToReauthorize {
                epoch: target_epoch,
            });
        }
        let (latest_epoch, latest_authority) = self
            .authorized_voters
            .authorized_voters
            .last_key_value()
            .map(|(epoch, authority)| (*epoch, *authority))
            .ok_or(LaunchVoteError::MissingAuthorizedVoter(target_epoch))?;
        if latest_authority != new_authority {
            let previous_switch = self
                .prior_voters
                .last()
                .map(|(_, _, end_epoch)| *end_epoch)
                .unwrap_or(0);
            // A valid launch Clock makes this strictly monotonic. Return a
            // stable error instead of reproducing the historical assertion.
            if target_epoch <= latest_epoch {
                return Err(LaunchVoteError::TooSoonToReauthorize {
                    epoch: target_epoch,
                });
            }
            self.prior_voters
                .append((latest_authority, previous_switch, target_epoch));
        }
        self.authorized_voters
            .authorized_voters
            .insert(target_epoch, new_authority);
        Ok(())
    }

    fn process_slot(&mut self, slot: u64, epoch: u64) {
        if self
            .votes
            .back()
            .is_some_and(|old_vote| old_vote.slot >= slot)
        {
            return;
        }

        while self
            .votes
            .back()
            .is_some_and(|vote| vote.expiration_slot() < slot)
        {
            self.votes.pop_back();
        }

        if self.votes.len() == MAX_LOCKOUT_HISTORY
            && let Some(root) = self.votes.pop_front()
        {
            self.root_slot = Some(root.slot);
            self.increment_credits(epoch);
        }
        self.votes.push_back(LockoutV100 {
            slot,
            confirmation_count: 1,
        });

        let stack_depth = self.votes.len();
        for (index, vote) in self.votes.iter_mut().enumerate() {
            if stack_depth > index + vote.confirmation_count as usize {
                vote.confirmation_count += 1;
            }
        }
    }

    fn process_timestamp(&mut self, slot: u64, timestamp: i64) -> Result<(), LaunchVoteError> {
        self.check_timestamp(slot, timestamp)?;
        self.last_timestamp = BlockTimestampV100 { slot, timestamp };
        Ok(())
    }

    fn check_timestamp(&self, slot: u64, timestamp: i64) -> Result<(), LaunchVoteError> {
        let proposed = BlockTimestampV100 { slot, timestamp };
        if (slot < self.last_timestamp.slot || timestamp < self.last_timestamp.timestamp)
            || ((slot == self.last_timestamp.slot || timestamp == self.last_timestamp.timestamp)
                && proposed != self.last_timestamp
                && self.last_timestamp.slot != 0)
        {
            return Err(LaunchVoteError::TimestampTooOld);
        }
        Ok(())
    }

    fn increment_credits(&mut self, epoch: u64) {
        if self.epoch_credits.is_empty() {
            self.epoch_credits.push((epoch, 0, 0));
        } else if self
            .epoch_credits
            .last()
            .is_some_and(|value| value.0 != epoch)
        {
            let (_, credits, previous_credits) = *self.epoch_credits.last().unwrap();
            if credits != previous_credits {
                self.epoch_credits.push((epoch, credits, credits));
            } else if let Some(last) = self.epoch_credits.last_mut() {
                last.0 = epoch;
            }
            if self.epoch_credits.len() > MAX_EPOCH_CREDITS_HISTORY {
                self.epoch_credits.remove(0);
            }
        }
        if let Some(last) = self.epoch_credits.last_mut() {
            last.1 += 1;
        }
    }

    fn credits(&self) -> u64 {
        self.epoch_credits.last().map_or(0, |value| value.1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct AuthorizedVotersV100 {
    authorized_voters: BTreeMap<u64, Pubkey>,
}

impl AuthorizedVotersV100 {
    fn is_empty(&self) -> bool {
        self.authorized_voters.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct VoteState0235 {
    node_pubkey: Pubkey,
    authorized_voter: Pubkey,
    authorized_voter_epoch: u64,
    prior_voters: CircBuf0235<(Pubkey, u64, u64, u64)>,
    authorized_withdrawer: Pubkey,
    commission: u8,
    votes: VecDeque<LockoutV100>,
    root_slot: Option<u64>,
    epoch_credits: Vec<(u64, u64, u64)>,
    last_timestamp: BlockTimestampV100,
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
struct LockoutV100 {
    slot: u64,
    confirmation_count: u32,
}

impl LockoutV100 {
    fn expiration_slot(self) -> u64 {
        self.slot
            .saturating_add(INITIAL_LOCKOUT.saturating_pow(self.confirmation_count))
    }
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
struct BlockTimestampV100 {
    slot: u64,
    timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct CircBufV100<I> {
    buf: [I; PRIOR_VOTER_ITEMS],
    idx: usize,
    is_empty: bool,
}

impl<I: Default + Copy> Default for CircBufV100<I> {
    fn default() -> Self {
        Self {
            buf: [I::default(); PRIOR_VOTER_ITEMS],
            idx: PRIOR_VOTER_ITEMS - 1,
            is_empty: true,
        }
    }
}

impl<I> CircBufV100<I> {
    fn append(&mut self, item: I) {
        self.idx = (self.idx + 1) % PRIOR_VOTER_ITEMS;
        self.buf[self.idx] = item;
        self.is_empty = false;
    }

    fn last(&self) -> Option<&I> {
        (!self.is_empty).then(|| &self.buf[self.idx])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct CircBuf0235<I> {
    buf: [I; PRIOR_VOTER_ITEMS],
    idx: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaunchPreAccount {
    pubkey: Pubkey,
    is_writable: bool,
    lamports: u64,
    data_len: usize,
    data: Option<Vec<u8>>,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
}

impl LaunchPreAccount {
    fn new(pubkey: Pubkey, is_writable: bool, account: &AccountSnapshot) -> Self {
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

    fn verify(&self, post: &AccountSnapshot) -> Result<(), LaunchVoteError> {
        // Keep this ordering aligned with v1.0.7 `PreAccount::verify`.
        if self.owner != post.owner
            && (!self.is_writable || self.owner != VOTE_PROGRAM_ID || !is_zeroed(&post.data))
        {
            return Err(LaunchVoteError::ModifiedProgramId {
                pubkey: self.pubkey,
            });
        }
        if self.owner != VOTE_PROGRAM_ID && self.lamports > post.lamports {
            return Err(LaunchVoteError::ExternalAccountLamportSpend {
                pubkey: self.pubkey,
            });
        }
        if !self.is_writable && self.lamports != post.lamports {
            return Err(LaunchVoteError::ReadonlyLamportChange {
                pubkey: self.pubkey,
            });
        }
        if self.data_len != post.data.len() {
            return Err(LaunchVoteError::AccountDataSizeChanged {
                pubkey: self.pubkey,
            });
        }
        if should_verify_data(&self.owner, self.is_writable)
            && self.data.as_ref() != Some(&post.data)
        {
            return Err(if self.is_writable {
                LaunchVoteError::ExternalAccountDataModified {
                    pubkey: self.pubkey,
                }
            } else {
                LaunchVoteError::ReadonlyDataModified {
                    pubkey: self.pubkey,
                }
            });
        }
        if self.executable != post.executable
            && (!self.is_writable || self.executable || self.owner != VOTE_PROGRAM_ID)
        {
            return Err(LaunchVoteError::ExecutableModified {
                pubkey: self.pubkey,
            });
        }
        if self.rent_epoch != post.rent_epoch {
            return Err(LaunchVoteError::RentEpochModified {
                pubkey: self.pubkey,
            });
        }
        Ok(())
    }
}

fn launch_pre_accounts(
    account_metas: &[LaunchAccountMeta],
    accounts: &CowAccountMap,
) -> Result<SmallVec<[LaunchPreAccount; 4]>, LaunchVoteError> {
    account_metas
        .iter()
        .enumerate()
        .filter(|(index, meta)| {
            // v1.0.7 verifies only the final occurrence of an aliased account.
            !account_metas[index + 1..]
                .iter()
                .any(|later| later.pubkey == meta.pubkey)
        })
        .map(|(_, meta)| {
            required_account(accounts, meta.pubkey)
                .map(|account| LaunchPreAccount::new(meta.pubkey, meta.is_writable, account))
        })
        .collect()
}

fn verify_launch_vote_instruction(
    pre_accounts: &[LaunchPreAccount],
    accounts: &CowAccountMap,
) -> Result<(), LaunchVoteError> {
    let mut pre_lamports = 0_u128;
    let mut post_lamports = 0_u128;
    for pre in pre_accounts {
        let post = required_account(accounts, pre.pubkey)?;
        pre.verify(post)?;
        pre_lamports += u128::from(pre.lamports);
        post_lamports += u128::from(post.lamports);
    }
    if pre_lamports != post_lamports {
        return Err(LaunchVoteError::UnbalancedInstruction {
            pre_lamports,
            post_lamports,
        });
    }
    Ok(())
}

fn should_verify_data(owner: &Pubkey, is_writable: bool) -> bool {
    *owner != VOTE_PROGRAM_ID || !is_writable
}

fn is_zeroed(data: &[u8]) -> bool {
    data.iter().all(|byte| *byte == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    #[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
    enum LegacyBoxedVoteStateVersionsV100 {
        V0_23_5(Box<VoteState0235>),
        Current(Box<VoteStateV100>),
    }

    const VOTE_ACCOUNT: Pubkey = [11; 32];
    const DESTINATION: Pubkey = [12; 32];
    const AUTHORIZED_VOTER: Pubkey = [7; 32];
    const AUTHORIZED_WITHDRAWER: Pubkey = [9; 32];

    fn meta(pubkey: Pubkey, is_signer: bool, is_writable: bool) -> LaunchAccountMeta {
        LaunchAccountMeta {
            pubkey,
            is_signer,
            is_writable,
        }
    }

    fn vote_account(data: Vec<u8>, lamports: u64) -> AccountSnapshot {
        AccountSnapshot {
            lamports,
            owner: VOTE_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: data.into(),
        }
    }

    fn sysvar_account(data: Vec<u8>) -> AccountSnapshot {
        AccountSnapshot {
            lamports: 1,
            owner: crate::SYSVAR_OWNER_ID,
            executable: false,
            rent_epoch: 0,
            data: data.into(),
        }
    }

    fn clock_account(epoch: u64, leader_schedule_epoch: u64) -> AccountSnapshot {
        clock_account_at(521_850, epoch, leader_schedule_epoch)
    }

    fn clock_account_at(slot: u64, epoch: u64, leader_schedule_epoch: u64) -> AccountSnapshot {
        sysvar_account(
            wincode::serialize(&ClockV100 {
                slot,
                segment: 510,
                epoch,
                leader_schedule_epoch,
                unix_timestamp: 1_585_432_740,
            })
            .unwrap(),
        )
    }

    fn rent_account() -> AccountSnapshot {
        sysvar_account(
            wincode::serialize(&RentV100 {
                lamports_per_byte_year: 3_480,
                exemption_threshold: 2.0,
                burn_percent: 100,
            })
            .unwrap(),
        )
    }

    fn fixed_vote_data(state: VoteStateVersionsV100) -> Vec<u8> {
        let encoded = wincode::serialize(&state).unwrap();
        let mut data = vec![0; 3_731];
        data[..encoded.len()].copy_from_slice(&encoded);
        data
    }

    fn initialized_state() -> VoteStateVersionsV100 {
        VoteStateVersionsV100::Current(VoteStateV100 {
            node_pubkey: [8; 32],
            authorized_withdrawer: AUTHORIZED_WITHDRAWER,
            commission: 100,
            votes: VecDeque::new(),
            root_slot: None,
            authorized_voters: AuthorizedVotersV100 {
                authorized_voters: BTreeMap::from([(0, AUTHORIZED_VOTER)]),
            },
            prior_voters: CircBufV100::default(),
            epoch_credits: Vec::new(),
            last_timestamp: BlockTimestampV100::default(),
        })
    }

    fn assert_fast_vote_state_encoding_matches_bincode(state: &VoteStateV100) -> usize {
        let oracle = wincode::serialize(&VoteStateVersionsV100Ref::Current(state)).unwrap();
        let mut scratch = vec![0xa5; oracle.len() + 37];
        let encoded_len = encode_vote_state_v100_into(state, &mut scratch).unwrap();
        assert_eq!(encoded_len, oracle.len());
        assert_eq!(&scratch[..encoded_len], oracle.as_slice());
        assert!(scratch[encoded_len..].iter().all(|byte| *byte == 0xa5));

        let mut exact = vec![0; oracle.len()];
        assert_eq!(
            encode_vote_state_v100_into(state, &mut exact),
            Some(oracle.len())
        );
        assert_eq!(exact, oracle);

        let mut undersized = vec![0x5a; oracle.len() - 1];
        let before = undersized.clone();
        assert_eq!(encode_vote_state_v100_into(state, &mut undersized), None);
        assert_eq!(undersized, before);
        encoded_len
    }

    fn steady_state_encoder_benchmark_state() -> VoteStateV100 {
        let mut state = initialized_state().into_current();
        state.votes = (0_u64..MAX_LOCKOUT_HISTORY as u64)
            .map(|index| LockoutV100 {
                slot: 10_000 + index,
                confirmation_count: (MAX_LOCKOUT_HISTORY as u32).saturating_sub(index as u32),
            })
            .collect();
        state.root_slot = Some(9_999);
        for index in 0_u64..37 {
            state
                .prior_voters
                .append(([index as u8; 32], index * 3, index * 3 + 2));
        }
        state.epoch_credits = (0_u64..32)
            .map(|epoch| (epoch, epoch * 1_000 + 999, epoch * 1_000))
            .collect();
        state.last_timestamp = BlockTimestampV100 {
            slot: 10_030,
            timestamp: 1_585_432_740,
        };
        state
    }

    #[test]
    #[ignore = "manual release-mode encoder microbenchmark"]
    fn steady_state_vote_encoder_microbenchmark() {
        let iterations = std::env::var("BLOCKZILLA_VOTE_ENCODER_BENCH_ITERATIONS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1_000_000);
        let rounds = std::env::var("BLOCKZILLA_VOTE_ENCODER_BENCH_ROUNDS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(9);
        assert!(iterations > 0 && rounds > 0);

        let mut state = steady_state_encoder_benchmark_state();
        let encoding = prepare_vote_state_v100_encoding(&state, 3_731).unwrap();
        let mut output = vec![0_u8; encoding.encoded_len];
        let mut samples = Vec::with_capacity(rounds);
        for round in 0..rounds {
            let started = std::time::Instant::now();
            for iteration in 0..iterations {
                state.last_timestamp.timestamp = (round as i64)
                    .wrapping_mul(iterations as i64)
                    .wrapping_add(iteration as i64);
                encode_prepared_vote_state_v100(&state, &mut output, encoding);
                std::hint::black_box(output.as_slice());
            }
            let elapsed = started.elapsed();
            let oracle = wincode::serialize(&VoteStateVersionsV100Ref::Current(&state)).unwrap();
            assert_eq!(output, oracle);
            samples.push(elapsed.as_nanos() / u128::from(iterations));
        }
        samples.sort_unstable();
        eprintln!(
            "steady_state_vote_encoder bytes={} iterations={} rounds={} min_ns_per_encode={} median_ns_per_encode={} max_ns_per_encode={}",
            encoding.encoded_len,
            iterations,
            rounds,
            samples[0],
            samples[samples.len() / 2],
            samples[samples.len() - 1],
        );
    }

    fn vote_instruction(slots: Vec<u64>, timestamp: Option<i64>) -> Vec<u8> {
        wincode::serialize(&VoteInstructionV100::Vote(VoteV100 {
            slots,
            hash: [3; 32],
            timestamp,
        }))
        .unwrap()
    }

    fn vote_switch_instruction(slots: Vec<u64>, timestamp: Option<i64>, proof: Hash) -> Vec<u8> {
        wincode::serialize(&VoteInstructionV100::VoteSwitch(
            VoteV100 {
                slots,
                hash: [3; 32],
                timestamp,
            },
            proof,
        ))
        .unwrap()
    }

    #[test]
    fn vote_switch_wire_generic_and_direct_paths_match_plain_vote() {
        let plain = vote_instruction(vec![26_752_196], Some(1_588_000_000));
        let switched = vote_switch_instruction(vec![26_752_196], Some(1_588_000_000), [0x99; 32]);
        assert_eq!(&switched[..4], &6_u32.to_le_bytes());
        assert_eq!(&switched[4..switched.len() - 32], &plain[4..]);
        assert_eq!(&switched[switched.len() - 32..], &[0x99; 32]);

        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let initial_vote = vote_account(fixed_vote_data(initialized_state()), 30_000_000);
        let initial = AccountMap::from([
            (VOTE_ACCOUNT, initial_vote.clone()),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        let mut plain_accounts = initial.clone();
        let mut switched_accounts = initial;
        let plain_mutation =
            apply_launch_vote_instruction(&plain, &metas, &mut plain_accounts, 61).unwrap();
        let switched_mutation =
            apply_launch_vote_instruction(&switched, &metas, &mut switched_accounts, 61).unwrap();
        assert_eq!(switched_mutation, plain_mutation);
        assert_eq!(switched_accounts, plain_accounts);

        let mut direct_account = initial_vote;
        let mut cache = LaunchVoteStateCache::default();
        assert!(matches!(
            try_apply_launch_vote_direct_cached(
                &switched,
                &metas,
                &mut direct_account,
                61,
                &mut cache,
            )
            .unwrap(),
            LaunchFastVoteApply::Applied { .. }
        ));
        assert_eq!(direct_account, plain_accounts[&VOTE_ACCOUNT]);

        let truncated = &switched[..switched.len() - 1];
        let mut untouched = direct_account.clone();
        let before = untouched.clone();
        assert_eq!(
            try_apply_launch_vote_direct_cached(
                truncated,
                &metas,
                &mut untouched,
                61,
                &mut LaunchVoteStateCache::default(),
            )
            .unwrap(),
            LaunchFastVoteApply::Fallback
        );
        assert_eq!(untouched, before);
        assert!(matches!(
            decode_instruction(truncated),
            Err(LaunchVoteError::DecodeInstruction(_))
        ));
    }

    #[test]
    fn unboxed_current_state_is_wire_identical_and_direct_write_preserves_tail() {
        let mut state = initialized_state().into_current();
        state.votes = VecDeque::from([
            LockoutV100 {
                slot: 431_998,
                confirmation_count: 2,
            },
            LockoutV100 {
                slot: 431_999,
                confirmation_count: 1,
            },
        ]);
        state.root_slot = Some(431_967);
        state
            .authorized_voters
            .authorized_voters
            .insert(1, [0x44; 32]);
        state.prior_voters.append((AUTHORIZED_VOTER, 0, 1));
        state.epoch_credits = vec![(0, 9_123, 0), (1, 9_124, 9_123)];
        state.last_timestamp = BlockTimestampV100 {
            slot: 431_999,
            timestamp: 1_585_432_740,
        };

        let legacy = wincode::serialize(&LegacyBoxedVoteStateVersionsV100::Current(Box::new(
            state.clone(),
        )))
        .unwrap();
        let unboxed = wincode::serialize(&VoteStateVersionsV100::Current(state.clone())).unwrap();
        assert_eq!(
            unboxed, legacy,
            "Box must be transparent on the bincode wire"
        );

        let mut account_data = vec![0xa5; 3_731];
        write_vote_state(&mut account_data, &state).unwrap();
        assert_eq!(&account_data[..legacy.len()], legacy.as_slice());
        assert!(
            account_data[legacy.len()..]
                .iter()
                .all(|byte| *byte == 0xa5)
        );

        let decoded = decode_vote_state(&legacy).unwrap().into_current();
        assert_eq!(
            wincode::serialize(&VoteStateVersionsV100::Current(decoded)).unwrap(),
            legacy
        );
    }

    #[test]
    fn fast_encoder_matches_uninitialized_and_initialized_current_wire() {
        let initialized = initialized_state().into_current();
        assert_eq!(
            assert_fast_vote_state_encoding_matches_bincode(&initialized),
            1_695
        );

        let mut uninitialized = initialized;
        uninitialized.node_pubkey = [0; 32];
        uninitialized.authorized_withdrawer = [0; 32];
        uninitialized.commission = 0;
        uninitialized.authorized_voters.authorized_voters.clear();
        assert!(uninitialized.authorized_voters.is_empty());
        assert_eq!(
            assert_fast_vote_state_encoding_matches_bincode(&uninitialized),
            1_655
        );
    }

    #[test]
    fn fast_encoder_matches_fully_populated_current_wire() {
        let mut state = initialized_state().into_current();
        state.node_pubkey = [0x31; 32];
        state.authorized_withdrawer = [0x32; 32];
        state.commission = 37;

        let mut votes = VecDeque::with_capacity(8);
        for slot in 10_u64..18 {
            votes.push_back(LockoutV100 {
                slot,
                confirmation_count: slot as u32,
            });
        }
        for _ in 0..5 {
            votes.pop_front();
        }
        for slot in 18_u64..23 {
            votes.push_back(LockoutV100 {
                slot,
                confirmation_count: slot as u32,
            });
        }
        state.votes = votes;
        state.root_slot = Some(u64::MAX - 1);
        state
            .authorized_voters
            .authorized_voters
            .insert(2, [0x42; 32]);
        state
            .authorized_voters
            .authorized_voters
            .insert(1, [0x41; 32]);
        for index in 0_u64..37 {
            state
                .prior_voters
                .append(([index as u8; 32], index * 3, index * 3 + 2));
        }
        state.epoch_credits = vec![
            (0, 11, 0),
            (1, 29, 11),
            (u64::MAX, u64::MAX - 1, u64::MAX - 2),
        ];
        state.last_timestamp = BlockTimestampV100 {
            slot: u64::MAX,
            timestamp: i64::MIN,
        };

        assert_fast_vote_state_encoding_matches_bincode(&state);
    }

    #[test]
    fn fast_encoder_matches_bincode_through_epoch_transitions() {
        let mut state = initialized_state().into_current();
        state
            .authorized_voters
            .authorized_voters
            .insert(1, [0x51; 32]);
        state
            .authorized_voters
            .authorized_voters
            .insert(2, [0x52; 32]);
        let mut next_slot = 1_u64;

        for epoch in 0_u64..=2 {
            let voter = state.authorized_voter_for_epoch(epoch).unwrap();
            state.normalize_authorized_voter_for_epoch(epoch, voter);
            state
                .prior_voters
                .append((voter, epoch, epoch.wrapping_add(1)));
            for _ in 0..32 {
                state.process_slot(next_slot, epoch);
                next_slot += 1;
            }
            state
                .process_timestamp(next_slot - 1, 1_000 + epoch as i64)
                .unwrap();
            assert_fast_vote_state_encoding_matches_bincode(&state);
        }
        assert_eq!(
            state.authorized_voters.authorized_voters,
            BTreeMap::from([(2, [0x52; 32])])
        );
        assert_eq!(state.epoch_credits.len(), 3);
    }

    #[test]
    fn direct_fast_encoder_matches_generic_v0235_migration() {
        let mut legacy_prior_voters = [([0; 32], 0, 0, 0); PRIOR_VOTER_ITEMS];
        legacy_prior_voters[3] = ([0x63; 32], 1, 2, 3);
        let legacy = VoteStateVersionsV100::V0_23_5(Box::new(VoteState0235 {
            node_pubkey: [0x61; 32],
            authorized_voter: AUTHORIZED_VOTER,
            authorized_voter_epoch: 0,
            prior_voters: CircBuf0235 {
                buf: legacy_prior_voters,
                idx: 3,
            },
            authorized_withdrawer: AUTHORIZED_WITHDRAWER,
            commission: 29,
            votes: VecDeque::from([LockoutV100 {
                slot: 7,
                confirmation_count: 2,
            }]),
            root_slot: Some(5),
            epoch_credits: vec![(0, 9, 3)],
            last_timestamp: BlockTimestampV100 {
                slot: 7,
                timestamp: 700,
            },
        }));
        let encoded_legacy = wincode::serialize(&legacy).unwrap();
        let mut initial_data = vec![0xa5; 3_731];
        initial_data[..encoded_legacy.len()].copy_from_slice(&encoded_legacy);
        let mut direct_account = vote_account(initial_data.clone(), 30_000_000);
        let mut generic_accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(initial_data.clone(), 30_000_000)),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let instruction = vote_instruction(vec![8], Some(800));

        apply_launch_vote_instruction_in_place(&instruction, &metas, &mut generic_accounts, 0)
            .unwrap();
        let mut cache = LaunchVoteStateCache::default();
        assert!(matches!(
            try_apply_launch_vote_direct_cached(
                &instruction,
                &metas,
                &mut direct_account,
                0,
                &mut cache,
            )
            .unwrap(),
            LaunchFastVoteApply::Applied {
                account_changed: true,
                ..
            }
        ));
        assert_eq!(direct_account.data, generic_accounts[&VOTE_ACCOUNT].data);

        let migrated = decode_vote_state(&direct_account.data).unwrap();
        let VoteStateVersionsV100::Current(migrated) = migrated else {
            panic!("successful legacy Vote must migrate to Current")
        };
        let current_prefix = wincode::serialize(&VoteStateVersionsV100Ref::Current(&migrated))
            .unwrap()
            .len();
        assert_eq!(
            &direct_account.data[current_prefix..],
            &initial_data[current_prefix..]
        );
    }

    #[test]
    fn direct_fast_encoder_capacity_failure_falls_back_without_commit() {
        let initial_data = wincode::serialize(&initialized_state()).unwrap();
        assert_eq!(initial_data.len(), 1_695);
        let mut direct_account = vote_account(initial_data.clone(), 30_000_000);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let instruction = vote_instruction(vec![1], None);
        let mut cache = LaunchVoteStateCache::default();

        assert_eq!(
            try_apply_launch_vote_direct_cached(
                &instruction,
                &metas,
                &mut direct_account,
                0,
                &mut cache,
            )
            .unwrap(),
            LaunchFastVoteApply::Fallback
        );
        assert_eq!(direct_account.data, initial_data);
        assert!(!cache.contains(&VOTE_ACCOUNT));

        let mut generic_accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(initial_data.clone(), 30_000_000)),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        let error =
            apply_launch_vote_instruction_in_place(&instruction, &metas, &mut generic_accounts, 0)
                .unwrap_err();
        assert!(matches!(
            error,
            LaunchVoteError::AccountDataTooSmall {
                needed: 1_707,
                available: 1_695,
            }
        ));
        assert_eq!(generic_accounts[&VOTE_ACCOUNT].data, initial_data);
    }

    #[test]
    fn lazy_direct_capacity_fallback_preserves_earlier_pending_commit() {
        let encoded = wincode::serialize(&initialized_state()).unwrap();
        assert_eq!(encoded.len(), 1_695);
        let mut initial_data = vec![0xa5; 1_707];
        initial_data[..encoded.len()].copy_from_slice(&encoded);
        let mut direct_account = vote_account(initial_data.clone(), 30_000_000);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let first = vote_instruction(vec![1], None);
        let second = vote_instruction(vec![2], None);
        let mut cache = LaunchVoteStateCache::default();

        assert!(matches!(
            try_apply_launch_vote_direct_cached_lazy(
                &first,
                &metas,
                &mut direct_account,
                0,
                &mut cache,
            )
            .unwrap(),
            LaunchFastVoteApply::Applied {
                account_changed: true,
                ..
            }
        ));
        // The first logical commit remains deferred.
        assert_eq!(direct_account.data, initial_data);
        assert_eq!(cache.lazy_direct_commits(), 1);
        assert_eq!(cache.materializations(), 0);

        assert_eq!(
            try_apply_launch_vote_direct_cached_lazy(
                &second,
                &metas,
                &mut direct_account,
                0,
                &mut cache,
            )
            .unwrap(),
            LaunchFastVoteApply::Fallback
        );

        let mut generic_accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(initial_data, 30_000_000)),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        apply_launch_vote_instruction_in_place(&first, &metas, &mut generic_accounts, 0).unwrap();
        assert_eq!(direct_account.data, generic_accounts[&VOTE_ACCOUNT].data);
        assert_eq!(cache.lazy_direct_commits(), 1);
        assert_eq!(cache.materializations(), 1);
        assert!(!cache.contains(&VOTE_ACCOUNT));
        assert!(matches!(
            apply_launch_vote_instruction_in_place(&second, &metas, &mut generic_accounts, 0,),
            Err(LaunchVoteError::AccountDataTooSmall {
                needed: 1_719,
                available: 1_707,
            })
        ));
    }

    #[test]
    fn lazy_direct_materializes_before_a_shorter_prefix_to_preserve_tail_bytes() {
        let encoded = wincode::serialize(&initialized_state()).unwrap();
        let mut initial_data = vec![0xa5; 3_731];
        initial_data[..encoded.len()].copy_from_slice(&encoded);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let instructions = [
            vote_instruction(vec![1], None),
            vote_instruction(vec![2], None),
            // Both preceding lockouts expire. The resulting Current prefix is
            // shorter, and eager replay retains bytes from the two-lockout
            // state in the newly exposed allocation tail.
            vote_instruction(vec![100], None),
        ];
        let mut eager_account = vote_account(initial_data.clone(), 30_000_000);
        let mut eager_cache = LaunchVoteStateCache::default();
        let mut lazy_account = vote_account(initial_data, 30_000_000);
        let mut lazy_cache = LaunchVoteStateCache::default();

        for instruction in &instructions {
            assert!(matches!(
                try_apply_launch_vote_direct_cached(
                    instruction,
                    &metas,
                    &mut eager_account,
                    0,
                    &mut eager_cache,
                )
                .unwrap(),
                LaunchFastVoteApply::Applied { .. }
            ));
            assert!(matches!(
                try_apply_launch_vote_direct_cached_lazy(
                    instruction,
                    &metas,
                    &mut lazy_account,
                    0,
                    &mut lazy_cache,
                )
                .unwrap(),
                LaunchFastVoteApply::Applied { .. }
            ));
        }

        // The previous pending state is published exactly once before the
        // shrinking Vote; the latest logical state remains deferred.
        assert_eq!(lazy_cache.materializations(), 1);
        assert!(lazy_cache.materialize_account(VOTE_ACCOUNT, &mut lazy_account));
        assert_eq!(lazy_cache.materializations(), 2);
        assert_eq!(lazy_account.data, eager_account.data);
    }

    #[test]
    fn lazy_direct_multi_slot_growth_stays_deferred_and_exact() {
        let encoded = wincode::serialize(&initialized_state()).unwrap();
        let mut initial_data = vec![0xa5; 3_731];
        initial_data[..encoded.len()].copy_from_slice(&encoded);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let instructions = [
            vote_instruction(vec![1, 2, 3], None),
            vote_instruction(vec![4, 5], None),
        ];
        let mut eager_account = vote_account(initial_data.clone(), 30_000_000);
        let mut eager_cache = LaunchVoteStateCache::default();
        let mut lazy_account = vote_account(initial_data, 30_000_000);
        let mut lazy_cache = LaunchVoteStateCache::default();

        for instruction in &instructions {
            assert!(matches!(
                try_apply_launch_vote_direct_cached(
                    instruction,
                    &metas,
                    &mut eager_account,
                    0,
                    &mut eager_cache,
                )
                .unwrap(),
                LaunchFastVoteApply::Applied { .. }
            ));
            assert!(matches!(
                try_apply_launch_vote_direct_cached_lazy(
                    instruction,
                    &metas,
                    &mut lazy_account,
                    0,
                    &mut lazy_cache,
                )
                .unwrap(),
                LaunchFastVoteApply::Applied { .. }
            ));
        }

        assert_eq!(lazy_cache.materializations(), 0);
        assert!(lazy_cache.materialize_account(VOTE_ACCOUNT, &mut lazy_account));
        assert_eq!(lazy_account.data, eager_account.data);
    }

    #[test]
    fn direct_vote_true_noop_preserves_exact_allocation() {
        let mut state = initialized_state().into_current();
        state.votes.push_back(LockoutV100 {
            slot: 20,
            confirmation_count: 1,
        });
        state.last_timestamp = BlockTimestampV100 {
            slot: 10,
            timestamp: 100,
        };
        // The exact Current prefix has no room to grow by another Lockout. A
        // duplicate/old Vote and an identical timestamp require no write at
        // all, so semantic tracking must report a true no-op without relying
        // on a byte comparison.
        let initial_data = wincode::serialize(&VoteStateVersionsV100::Current(state)).unwrap();
        let mut account = vote_account(initial_data.clone(), 30_000_000);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let mut cache = LaunchVoteStateCache::default();

        for instruction in [
            vote_instruction(vec![19], None),
            vote_instruction(vec![10], Some(100)),
        ] {
            assert_eq!(
                try_apply_launch_vote_direct_cached(
                    &instruction,
                    &metas,
                    &mut account,
                    0,
                    &mut cache,
                )
                .unwrap(),
                LaunchFastVoteApply::Applied {
                    account_changed: false,
                    record_changed_account: false,
                }
            );
            assert_eq!(account.data, initial_data);
        }
        assert!(cache.contains(&VOTE_ACCOUNT));
    }

    #[test]
    fn direct_vote_semantic_change_tracking_matches_generic_sequence() {
        let mut state = initialized_state().into_current();
        state.votes.push_back(LockoutV100 {
            slot: 20,
            confirmation_count: 1,
        });
        state.last_timestamp = BlockTimestampV100 {
            slot: 10,
            timestamp: 100,
        };
        state
            .authorized_voters
            .authorized_voters
            .insert(1, AUTHORIZED_VOTER);
        let initial_data = fixed_vote_data(VoteStateVersionsV100::Current(state));
        let mut direct_account = vote_account(initial_data.clone(), 30_000_000);
        let mut generic_accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(initial_data, 30_000_000)),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let mut cache = LaunchVoteStateCache::default();
        let mut already_recorded = false;

        for (epoch, instruction) in [
            (0, vote_instruction(vec![20], None)),
            (0, vote_instruction(vec![19], None)),
            // The Vote slot is old, but this advances the timestamp alone.
            (0, vote_instruction(vec![11], Some(101))),
            (0, vote_instruction(vec![11], Some(101))),
            // The first epoch-1 Vote only purges the epoch-0 authority.
            (1, vote_instruction(vec![20], None)),
            (1, vote_instruction(vec![20], None)),
            (1, vote_instruction(vec![21], None)),
            (1, vote_instruction(vec![20], None)),
        ] {
            let before = generic_accounts[&VOTE_ACCOUNT].data.clone();
            apply_launch_vote_instruction_in_place(
                &instruction,
                &metas,
                &mut generic_accounts,
                epoch,
            )
            .unwrap();
            let expected_changed = generic_accounts[&VOTE_ACCOUNT].data != before;
            let expected_record = expected_changed && !already_recorded;

            assert_eq!(
                try_apply_launch_vote_direct_cached(
                    &instruction,
                    &metas,
                    &mut direct_account,
                    epoch,
                    &mut cache,
                )
                .unwrap(),
                LaunchFastVoteApply::Applied {
                    account_changed: expected_changed,
                    record_changed_account: expected_record,
                }
            );
            already_recorded |= expected_changed;
            assert_eq!(direct_account.data, generic_accounts[&VOTE_ACCOUNT].data);
        }
    }

    #[test]
    fn direct_vote_rewrites_noncanonical_current_authorized_voter_maps() {
        let mut state = initialized_state().into_current();
        state.votes.push_back(LockoutV100 {
            slot: 20,
            confirmation_count: 1,
        });
        state
            .authorized_voters
            .authorized_voters
            .insert(1, [0x44; 32]);
        let canonical = wincode::serialize(&VoteStateVersionsV100::Current(state.clone())).unwrap();

        const FIXED_PREFIX_BYTES: usize = size_of::<u32>() + 32 + 32 + size_of::<u8>();
        const LENGTH_BYTES: usize = size_of::<u64>();
        const LOCKOUT_BYTES: usize = size_of::<u64>() + size_of::<u32>();
        const AUTHORIZED_VOTER_BYTES: usize = size_of::<u64>() + 32;
        let authorized_voters_len_offset =
            FIXED_PREFIX_BYTES + LENGTH_BYTES + LOCKOUT_BYTES * state.votes.len() + 1;
        let entries_start = authorized_voters_len_offset + LENGTH_BYTES;
        assert_eq!(read_u64(&canonical, authorized_voters_len_offset), Some(2));

        let mut out_of_order = canonical.clone();
        let first_entry: [u8; AUTHORIZED_VOTER_BYTES] = out_of_order
            [entries_start..entries_start + AUTHORIZED_VOTER_BYTES]
            .try_into()
            .unwrap();
        out_of_order.copy_within(
            entries_start + AUTHORIZED_VOTER_BYTES..entries_start + 2 * AUTHORIZED_VOTER_BYTES,
            entries_start,
        );
        out_of_order
            [entries_start + AUTHORIZED_VOTER_BYTES..entries_start + 2 * AUTHORIZED_VOTER_BYTES]
            .copy_from_slice(&first_entry);

        let mut duplicate = canonical.clone();
        duplicate.splice(
            entries_start + AUTHORIZED_VOTER_BYTES..entries_start + AUTHORIZED_VOTER_BYTES,
            first_entry,
        );
        duplicate[authorized_voters_len_offset..entries_start]
            .copy_from_slice(&3_u64.to_le_bytes());

        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let instruction = vote_instruction(vec![20], None);
        for noncanonical in [out_of_order, duplicate] {
            let decoded = decode_vote_state(&noncanonical).unwrap().into_current();
            assert_eq!(
                decoded.authorized_voters.authorized_voters,
                state.authorized_voters.authorized_voters
            );
            assert!(!current_vote_state_wire_is_canonical(
                &noncanonical,
                &decoded
            ));

            let mut initial_data = vec![0xa5; 3_731];
            initial_data[..noncanonical.len()].copy_from_slice(&noncanonical);
            let mut direct_account = vote_account(initial_data.clone(), 30_000_000);
            let mut generic_accounts = AccountMap::from([
                (VOTE_ACCOUNT, vote_account(initial_data, 30_000_000)),
                (AUTHORIZED_VOTER, crate::default_system_account()),
            ]);
            apply_launch_vote_instruction_in_place(&instruction, &metas, &mut generic_accounts, 0)
                .unwrap();

            let mut cache = LaunchVoteStateCache::default();
            cache.seed(VOTE_ACCOUNT, &direct_account.data);
            assert_eq!(
                try_apply_launch_vote_direct_cached(
                    &instruction,
                    &metas,
                    &mut direct_account,
                    0,
                    &mut cache,
                )
                .unwrap(),
                LaunchFastVoteApply::Applied {
                    account_changed: true,
                    record_changed_account: true,
                }
            );
            assert_eq!(direct_account.data, generic_accounts[&VOTE_ACCOUNT].data);
            assert_eq!(
                &direct_account.data[..canonical.len()],
                canonical.as_slice()
            );
        }
    }

    #[test]
    fn direct_vote_migrates_v0235_even_when_vote_fields_are_noops() {
        let legacy = VoteStateVersionsV100::V0_23_5(Box::new(VoteState0235 {
            node_pubkey: [0x71; 32],
            authorized_voter: AUTHORIZED_VOTER,
            authorized_voter_epoch: 0,
            prior_voters: CircBuf0235 {
                buf: [([0; 32], 0, 0, 0); PRIOR_VOTER_ITEMS],
                idx: 0,
            },
            authorized_withdrawer: AUTHORIZED_WITHDRAWER,
            commission: 17,
            votes: VecDeque::from([LockoutV100 {
                slot: 20,
                confirmation_count: 1,
            }]),
            root_slot: None,
            epoch_credits: Vec::new(),
            last_timestamp: BlockTimestampV100::default(),
        }));
        let encoded = wincode::serialize(&legacy).unwrap();
        let mut initial_data = vec![0xa5; 3_731];
        initial_data[..encoded.len()].copy_from_slice(&encoded);
        let mut direct_account = vote_account(initial_data.clone(), 30_000_000);
        let mut generic_accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(initial_data, 30_000_000)),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let instruction = vote_instruction(vec![20], None);

        apply_launch_vote_instruction_in_place(&instruction, &metas, &mut generic_accounts, 0)
            .unwrap();
        let mut cache = LaunchVoteStateCache::default();
        cache.seed(VOTE_ACCOUNT, &direct_account.data);
        assert_eq!(
            try_apply_launch_vote_direct_cached(
                &instruction,
                &metas,
                &mut direct_account,
                0,
                &mut cache,
            )
            .unwrap(),
            LaunchFastVoteApply::Applied {
                account_changed: true,
                record_changed_account: true,
            }
        );
        assert_eq!(direct_account.data, generic_accounts[&VOTE_ACCOUNT].data);
        assert!(matches!(
            decode_vote_state(&direct_account.data).unwrap(),
            VoteStateVersionsV100::Current(_)
        ));
    }

    #[test]
    fn fast_encoder_fills_the_maximum_launch_vote_account_exactly() {
        let mut state = initialized_state().into_current();
        state.votes = (0_u64..MAX_LOCKOUT_HISTORY as u64)
            .map(|slot| LockoutV100 {
                slot,
                confirmation_count: slot as u32,
            })
            .collect();
        state.root_slot = Some(0);
        state.authorized_voters.authorized_voters =
            (0_u64..4).map(|epoch| (epoch, [epoch as u8; 32])).collect();
        state.epoch_credits = (0_u64..MAX_EPOCH_CREDITS_HISTORY as u64)
            .map(|epoch| (epoch, epoch + 1, epoch))
            .collect();

        assert_eq!(
            assert_fast_vote_state_encoding_matches_bincode(&state),
            3_731
        );
    }

    #[test]
    fn fast_encoder_matches_bincode_for_deterministic_state_matrix() {
        for case in 0_u64..96 {
            let mut state = initialized_state().into_current();
            state.node_pubkey = [case as u8; 32];
            state.authorized_withdrawer = [case.wrapping_mul(3) as u8; 32];
            state.commission = case.wrapping_mul(7) as u8;
            state.votes = (0..case % (MAX_LOCKOUT_HISTORY as u64 + 1))
                .map(|index| LockoutV100 {
                    slot: case.wrapping_mul(1_000).wrapping_add(index),
                    confirmation_count: (case as u32).wrapping_mul(17).wrapping_add(index as u32),
                })
                .collect();
            state.root_slot = (case % 3 != 0).then_some(case.wrapping_mul(97));

            state.authorized_voters.authorized_voters.clear();
            let authority_count = (case % 5) as u8;
            for index in (0..authority_count).rev() {
                state.authorized_voters.authorized_voters.insert(
                    u64::from(index) * 2 + case % 2,
                    [index.wrapping_add(case as u8); 32],
                );
            }

            for index in 0..case % 40 {
                state.prior_voters.append((
                    [index.wrapping_add(case) as u8; 32],
                    index.wrapping_mul(5),
                    index.wrapping_mul(5).wrapping_add(3),
                ));
            }
            state.epoch_credits = (0..case % (MAX_EPOCH_CREDITS_HISTORY as u64 + 1))
                .map(|epoch| {
                    let credits = case.wrapping_mul(101).wrapping_add(epoch);
                    (epoch, credits, credits.wrapping_sub(case))
                })
                .collect();
            state.last_timestamp = BlockTimestampV100 {
                slot: case.wrapping_mul(10_003),
                timestamp: match case % 3 {
                    0 => i64::MIN.wrapping_add(case as i64),
                    1 => -(case as i64),
                    _ => i64::MAX.wrapping_sub(case as i64),
                },
            };

            assert_fast_vote_state_encoding_matches_bincode(&state);
        }
    }

    #[test]
    fn cached_vote_hit_is_byte_identical_to_the_generic_path() {
        let base = AccountMap::from([
            (
                VOTE_ACCOUNT,
                vote_account(fixed_vote_data(initialized_state()), 30_000_000),
            ),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let mut generic = base.clone();
        let mut cached = base;
        let mut cache = LaunchVoteStateCache::default();

        for (index, instruction) in [
            vote_instruction(vec![1], Some(10)),
            vote_instruction(vec![2], Some(11)),
        ]
        .iter()
        .enumerate()
        {
            let generic_mutation =
                apply_launch_vote_instruction_in_place(instruction, &metas, &mut generic, 0)
                    .unwrap();
            let (cached_mutation, cache_hit) = apply_launch_vote_instruction_in_place_cached(
                instruction,
                &metas,
                &mut cached,
                0,
                &mut cache,
            )
            .unwrap();

            assert_eq!(cache_hit, index != 0);
            assert_eq!(cached_mutation, generic_mutation);
            assert_eq!(cached, generic);
        }
        assert!(cache.contains(&VOTE_ACCOUNT));
        assert_eq!(cache.normalized_authorized_epoch(&VOTE_ACCOUNT), Some(0));
    }

    #[test]
    fn direct_vote_normalizes_authority_once_per_epoch_after_successful_preflight() {
        let mut state = initialized_state().into_current();
        // An exact epoch-1 entry must not make the first successful epoch-1
        // Vote skip the historical purge of epoch 0.
        state
            .authorized_voters
            .authorized_voters
            .insert(1, AUTHORIZED_VOTER);
        let initial_data = fixed_vote_data(VoteStateVersionsV100::Current(state));
        let mut direct_account = vote_account(initial_data.clone(), 30_000_000);
        let mut generic_accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(initial_data.clone(), 30_000_000)),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        let signed_metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let unsigned_metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, false, false),
        ];
        let mut cache = LaunchVoteStateCache::default();

        for instruction in [
            vote_instruction(vec![1], Some(10)),
            vote_instruction(vec![2], Some(11)),
        ] {
            apply_launch_vote_instruction_in_place(
                &instruction,
                &signed_metas,
                &mut generic_accounts,
                0,
            )
            .unwrap();
            assert!(matches!(
                try_apply_launch_vote_direct_cached(
                    &instruction,
                    &signed_metas,
                    &mut direct_account,
                    0,
                    &mut cache,
                )
                .unwrap(),
                LaunchFastVoteApply::Applied { .. }
            ));
            assert_eq!(direct_account.data, generic_accounts[&VOTE_ACCOUNT].data);
        }
        assert_eq!(cache.normalized_authorized_epoch(&VOTE_ACCOUNT), Some(0));
        assert_eq!(
            cache.direct_authorized_voter_normalizations(&VOTE_ACCOUNT),
            1
        );

        let before_failures = direct_account.data.clone();
        let empty_vote = vote_instruction(Vec::new(), None);
        let generic_error = apply_launch_vote_instruction_in_place(
            &empty_vote,
            &signed_metas,
            &mut generic_accounts,
            1,
        )
        .unwrap_err();
        let direct_error = try_apply_launch_vote_direct_cached(
            &empty_vote,
            &signed_metas,
            &mut direct_account,
            1,
            &mut cache,
        )
        .unwrap_err();
        assert!(matches!(generic_error, LaunchVoteError::EmptySlots));
        assert!(matches!(direct_error, LaunchVoteError::EmptySlots));

        let unsigned_vote = vote_instruction(vec![432_000], None);
        let generic_error = apply_launch_vote_instruction_in_place(
            &unsigned_vote,
            &unsigned_metas,
            &mut generic_accounts,
            1,
        )
        .unwrap_err();
        let direct_error = try_apply_launch_vote_direct_cached(
            &unsigned_vote,
            &unsigned_metas,
            &mut direct_account,
            1,
            &mut cache,
        )
        .unwrap_err();
        assert!(matches!(
            generic_error,
            LaunchVoteError::MissingRequiredSignature { .. }
        ));
        assert!(matches!(
            direct_error,
            LaunchVoteError::MissingRequiredSignature { .. }
        ));
        assert_eq!(direct_account.data, before_failures);
        assert_eq!(generic_accounts[&VOTE_ACCOUNT].data, before_failures);
        assert_eq!(cache.normalized_authorized_epoch(&VOTE_ACCOUNT), Some(0));
        assert_eq!(
            cache.direct_authorized_voter_normalizations(&VOTE_ACCOUNT),
            1
        );

        for instruction in [
            vote_instruction(vec![432_000], None),
            vote_instruction(vec![432_001], None),
        ] {
            apply_launch_vote_instruction_in_place(
                &instruction,
                &signed_metas,
                &mut generic_accounts,
                1,
            )
            .unwrap();
            assert!(matches!(
                try_apply_launch_vote_direct_cached(
                    &instruction,
                    &signed_metas,
                    &mut direct_account,
                    1,
                    &mut cache,
                )
                .unwrap(),
                LaunchFastVoteApply::Applied { .. }
            ));
            assert_eq!(direct_account.data, generic_accounts[&VOTE_ACCOUNT].data);
        }
        assert_eq!(cache.normalized_authorized_epoch(&VOTE_ACCOUNT), Some(1));
        assert_eq!(
            cache.direct_authorized_voter_normalizations(&VOTE_ACCOUNT),
            2
        );
        let final_state = decode_vote_state(&direct_account.data)
            .unwrap()
            .into_current();
        assert_eq!(
            final_state.authorized_voters.authorized_voters,
            BTreeMap::from([(1, AUTHORIZED_VOTER)])
        );
    }

    #[test]
    fn direct_vote_reports_only_the_first_byte_changing_account_commit() {
        let initial_data = fixed_vote_data(initialized_state());
        let mut account = vote_account(initial_data.clone(), 30_000_000);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let mut cache = LaunchVoteStateCache::default();

        let error = try_apply_launch_vote_direct_cached(
            &vote_instruction(Vec::new(), None),
            &metas,
            &mut account,
            0,
            &mut cache,
        )
        .unwrap_err();
        assert!(matches!(error, LaunchVoteError::EmptySlots));
        assert_eq!(account.data, initial_data);
        assert_eq!(
            try_apply_launch_vote_direct_cached(&[0xff], &metas, &mut account, 0, &mut cache,)
                .unwrap(),
            LaunchFastVoteApply::Fallback
        );

        let first_vote = vote_instruction(vec![1], None);
        assert_eq!(
            try_apply_launch_vote_direct_cached(&first_vote, &metas, &mut account, 0, &mut cache,)
                .unwrap(),
            LaunchFastVoteApply::Applied {
                account_changed: true,
                record_changed_account: true,
            }
        );
        assert_eq!(
            try_apply_launch_vote_direct_cached(&first_vote, &metas, &mut account, 0, &mut cache,)
                .unwrap(),
            LaunchFastVoteApply::Applied {
                account_changed: false,
                record_changed_account: false,
            }
        );
        assert_eq!(
            try_apply_launch_vote_direct_cached(
                &vote_instruction(vec![2], None),
                &metas,
                &mut account,
                0,
                &mut cache,
            )
            .unwrap(),
            LaunchFastVoteApply::Applied {
                account_changed: true,
                record_changed_account: false,
            }
        );
    }

    #[test]
    fn failed_cached_vote_invalidates_mutated_decoded_state() {
        let mut cached = AccountMap::from([
            (
                VOTE_ACCOUNT,
                vote_account(fixed_vote_data(initialized_state()), 30_000_000),
            ),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let mut cache = LaunchVoteStateCache::default();
        apply_launch_vote_instruction_in_place_cached(
            &vote_instruction(vec![1], Some(10)),
            &metas,
            &mut cached,
            0,
            &mut cache,
        )
        .unwrap();
        let before_failure = cached.clone();

        let error = apply_launch_vote_instruction_in_place_cached(
            &vote_instruction(vec![2], Some(9)),
            &metas,
            &mut cached,
            0,
            &mut cache,
        )
        .unwrap_err();
        assert!(matches!(error, LaunchVoteError::TimestampTooOld));
        assert_eq!(cached, before_failure);
        assert!(!cache.contains(&VOTE_ACCOUNT));

        let retry = vote_instruction(vec![2], Some(11));
        let mut generic = before_failure;
        apply_launch_vote_instruction_in_place(&retry, &metas, &mut generic, 0).unwrap();
        let (_, cache_hit) = apply_launch_vote_instruction_in_place_cached(
            &retry,
            &metas,
            &mut cached,
            0,
            &mut cache,
        )
        .unwrap();
        assert!(!cache_hit);
        assert_eq!(cached, generic);
    }

    #[test]
    fn authorize_and_withdraw_bypass_and_invalidate_vote_cache() {
        let new_withdrawer = [22; 32];
        let mut accounts = AccountMap::from([
            (
                VOTE_ACCOUNT,
                vote_account(fixed_vote_data(initialized_state()), 30_000_000),
            ),
            (AUTHORIZED_VOTER, crate::default_system_account()),
            (AUTHORIZED_WITHDRAWER, crate::default_system_account()),
            (new_withdrawer, crate::default_system_account()),
            (DESTINATION, crate::default_system_account()),
            (CLOCK_SYSVAR_ID, clock_account(0, 0)),
        ]);
        let vote_metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let mut cache = LaunchVoteStateCache::default();
        apply_launch_vote_instruction_in_place_cached(
            &vote_instruction(vec![1], None),
            &vote_metas,
            &mut accounts,
            0,
            &mut cache,
        )
        .unwrap();
        assert!(cache.contains(&VOTE_ACCOUNT));

        let authorize = wincode::serialize(&VoteInstructionV100::Authorize(
            new_withdrawer,
            LaunchVoteAuthorize::Withdrawer,
        ))
        .unwrap();
        let (_, cache_hit) = apply_launch_vote_instruction_in_place_cached(
            &authorize,
            &[
                meta(VOTE_ACCOUNT, false, true),
                meta(CLOCK_SYSVAR_ID, false, false),
                meta(AUTHORIZED_WITHDRAWER, true, false),
            ],
            &mut accounts,
            0,
            &mut cache,
        )
        .unwrap();
        assert!(!cache_hit);
        assert!(!cache.contains(&VOTE_ACCOUNT));

        let (_, cache_hit) = apply_launch_vote_instruction_in_place_cached(
            &vote_instruction(vec![2], None),
            &vote_metas,
            &mut accounts,
            0,
            &mut cache,
        )
        .unwrap();
        assert!(!cache_hit);
        assert!(cache.contains(&VOTE_ACCOUNT));

        let withdraw = wincode::serialize(&VoteInstructionV100::Withdraw(1)).unwrap();
        let (_, cache_hit) = apply_launch_vote_instruction_in_place_cached(
            &withdraw,
            &[
                meta(VOTE_ACCOUNT, false, true),
                meta(DESTINATION, false, true),
                meta(new_withdrawer, true, false),
            ],
            &mut accounts,
            0,
            &mut cache,
        )
        .unwrap();
        assert!(!cache_hit);
        assert!(!cache.contains(&VOTE_ACCOUNT));
    }

    #[test]
    fn update_commission_matches_v1_2_32_wire_authority_and_cache_semantics() {
        let instruction = wincode::serialize(&VoteInstructionV100::UpdateCommission(10)).unwrap();
        assert_eq!(instruction, [5, 0, 0, 0, 10]);
        let initial_data = fixed_vote_data(initialized_state());
        let mut accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(initial_data.clone(), 30_000_000)),
            (AUTHORIZED_WITHDRAWER, crate::default_system_account()),
        ]);
        let signed_metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_WITHDRAWER, true, false),
        ];
        let mut cache = LaunchVoteStateCache::default();
        cache.seed(VOTE_ACCOUNT, &initial_data);
        assert!(cache.contains(&VOTE_ACCOUNT));

        let (mutation, cache_hit) = apply_launch_vote_instruction_in_place_cached(
            &instruction,
            &signed_metas,
            &mut accounts,
            0,
            &mut cache,
        )
        .unwrap();
        assert!(!cache_hit);
        assert!(!cache.contains(&VOTE_ACCOUNT));
        assert_eq!(
            mutation,
            LaunchVoteMutation::UpdateCommission {
                old_commission: 100,
                new_commission: 10,
            }
        );
        assert_eq!(
            decode_vote_state(&accounts[&VOTE_ACCOUNT].data)
                .unwrap()
                .into_current()
                .commission,
            10
        );

        let max_commission =
            wincode::serialize(&VoteInstructionV100::UpdateCommission(u8::MAX)).unwrap();
        let mutation =
            apply_launch_vote_instruction(&max_commission, &signed_metas, &mut accounts, 0)
                .unwrap();
        assert_eq!(
            mutation,
            LaunchVoteMutation::UpdateCommission {
                old_commission: 10,
                new_commission: u8::MAX,
            }
        );
    }

    #[test]
    fn update_commission_preserves_decode_signer_verifier_order_and_atomicity() {
        let instruction = wincode::serialize(&VoteInstructionV100::UpdateCommission(10)).unwrap();
        let unsigned_metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(AUTHORIZED_WITHDRAWER, false, false),
        ];
        let mut malformed = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(vec![0xff; 3_731], 30_000_000)),
            (AUTHORIZED_WITHDRAWER, crate::default_system_account()),
        ]);
        assert!(matches!(
            apply_launch_vote_instruction(&instruction, &unsigned_metas, &mut malformed, 0),
            Err(LaunchVoteError::DecodeAccount(_))
        ));

        let initial_data = fixed_vote_data(initialized_state());
        let mut accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(initial_data.clone(), 30_000_000)),
            (AUTHORIZED_WITHDRAWER, crate::default_system_account()),
        ]);
        let mut cache = LaunchVoteStateCache::default();
        cache.seed(VOTE_ACCOUNT, &initial_data);
        let before = accounts.clone();
        assert!(matches!(
            apply_launch_vote_instruction_in_place_cached(
                &instruction,
                &unsigned_metas,
                &mut accounts,
                0,
                &mut cache,
            ),
            Err(LaunchVoteError::MissingRequiredSignature {
                pubkey: AUTHORIZED_WITHDRAWER
            })
        ));
        assert_eq!(accounts, before);
        assert!(!cache.contains(&VOTE_ACCOUNT));

        let readonly_metas = [
            meta(VOTE_ACCOUNT, false, false),
            meta(AUTHORIZED_WITHDRAWER, true, false),
        ];
        assert!(matches!(
            apply_launch_vote_instruction(&instruction, &readonly_metas, &mut accounts, 0),
            Err(LaunchVoteError::ReadonlyDataModified {
                pubkey: VOTE_ACCOUNT
            })
        ));
        assert_eq!(accounts, before);
    }

    #[test]
    fn trusted_vote_mutates_the_fixed_account_allocation() {
        let encoded = wincode::serialize(&initialized_state()).unwrap();
        let mut account = vec![0xa5; 3_731];
        account[..encoded.len()].copy_from_slice(&encoded);

        let mutation =
            apply_trusted_vote_instruction(&mut account, &vote_instruction(vec![1, 2], None), 0)
                .unwrap();

        assert_eq!(mutation.voted_slots, vec![1, 2]);
        let decoded: VoteStateVersionsV100 = wincode::deserialize(&account).unwrap();
        let state = decoded.into_current();
        assert_eq!(state.votes.len(), 2);
        assert_eq!(state.votes[0].confirmation_count, 2);
        assert_eq!(state.votes[1].confirmation_count, 1);
        let encoded_after = wincode::serialize(&VoteStateVersionsV100::Current(state)).unwrap();
        assert!(
            account[encoded_after.len()..]
                .iter()
                .all(|byte| *byte == 0xa5)
        );
    }

    #[test]
    fn epoch_transition_caches_then_purges_authorized_voter() {
        let encoded = wincode::serialize(&initialized_state()).unwrap();
        let mut account = vec![0; 3_731];
        account[..encoded.len()].copy_from_slice(&encoded);

        apply_trusted_vote_instruction(&mut account, &vote_instruction(vec![432_000], None), 1)
            .unwrap();

        let decoded: VoteStateVersionsV100 = wincode::deserialize(&account).unwrap();
        let state = decoded.into_current();
        assert_eq!(state.authorized_voters.authorized_voters.len(), 1);
        assert!(state.authorized_voters.authorized_voters.contains_key(&1));
    }

    #[test]
    fn non_vote_variants_stop_explicitly() {
        let encoded = wincode::serialize(&initialized_state()).unwrap();
        let mut account = vec![0; 3_731];
        account[..encoded.len()].copy_from_slice(&encoded);
        let instruction = wincode::serialize(&VoteInstructionV100::Withdraw(1)).unwrap();

        let error = apply_trusted_vote_instruction(&mut account, &instruction, 0).unwrap_err();
        assert!(matches!(
            error,
            LaunchVoteError::UnsupportedInstruction("Withdraw")
        ));
    }

    #[test]
    fn initializes_exact_launch_account_and_preserves_allocation_tail() {
        let vote_init = VoteInitV100 {
            node_pubkey: [0x51; 32],
            authorized_voter: [0x52; 32],
            authorized_withdrawer: [0x53; 32],
            commission: 100,
        };
        let instruction =
            wincode::serialize(&VoteInstructionV100::InitializeAccount(vote_init)).unwrap();
        assert_eq!(instruction.len(), 101);
        let mut accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(vec![0; 3_731], 26_858_640)),
            (RENT_SYSVAR_ID, rent_account()),
            (CLOCK_SYSVAR_ID, clock_account(1, 2)),
        ]);
        let metas = [
            meta(VOTE_ACCOUNT, true, true),
            meta(RENT_SYSVAR_ID, false, false),
            meta(CLOCK_SYSVAR_ID, false, false),
        ];

        let mutation = apply_launch_vote_instruction(&instruction, &metas, &mut accounts, 1)
            .expect("the epoch-1 launch account is exactly rent exempt");

        assert_eq!(
            mutation,
            LaunchVoteMutation::InitializeAccount {
                node_pubkey: vote_init.node_pubkey,
                authorized_voter: vote_init.authorized_voter,
                authorized_withdrawer: vote_init.authorized_withdrawer,
                commission: 100,
                epoch: 1,
            }
        );
        let decoded = decode_vote_state(&accounts[&VOTE_ACCOUNT].data)
            .unwrap()
            .into_current();
        assert_eq!(decoded.node_pubkey, vote_init.node_pubkey);
        assert_eq!(
            decoded.authorized_withdrawer,
            vote_init.authorized_withdrawer
        );
        assert_eq!(decoded.commission, 100);
        assert_eq!(
            decoded.authorized_voters.authorized_voters,
            BTreeMap::from([(1, vote_init.authorized_voter)])
        );
        let encoded = wincode::serialize(&VoteStateVersionsV100::Current(decoded)).unwrap();
        assert!(
            accounts[&VOTE_ACCOUNT].data[encoded.len()..]
                .iter()
                .all(|byte| *byte == 0)
        );
    }

    #[test]
    fn initialize_node_signer_activates_at_mainnet_v1_1_boundary() {
        let vote_init = VoteInitV100 {
            node_pubkey: [0x61; 32],
            authorized_voter: [0x62; 32],
            authorized_withdrawer: [0x63; 32],
            commission: 100,
        };
        let instruction =
            wincode::serialize(&VoteInstructionV100::InitializeAccount(vote_init)).unwrap();
        let unsigned_metas = [
            meta(VOTE_ACCOUNT, true, true),
            meta(RENT_SYSVAR_ID, false, false),
            meta(CLOCK_SYSVAR_ID, false, false),
        ];

        // v1.0.7's original three-account wire remains valid immediately
        // before the mainnet transition.
        let mut pre_activation = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(vec![0; 3_731], 26_858_640)),
            (RENT_SYSVAR_ID, rent_account()),
            (
                CLOCK_SYSVAR_ID,
                clock_account_at(INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT - 1, 1, 2),
            ),
        ]);
        apply_launch_vote_instruction(&instruction, &unsigned_metas, &mut pre_activation, 1)
            .unwrap();

        let mut accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(vec![0; 3_731], 26_858_640)),
            (RENT_SYSVAR_ID, rent_account()),
            (
                CLOCK_SYSVAR_ID,
                clock_account_at(INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT, 1, 2),
            ),
            (vote_init.node_pubkey, crate::default_system_account()),
        ]);
        let before = accounts.clone();
        assert!(matches!(
            apply_launch_vote_instruction(&instruction, &unsigned_metas, &mut accounts, 1),
            Err(LaunchVoteError::MissingRequiredSignature { pubkey })
                if pubkey == vote_init.node_pubkey
        ));
        assert_eq!(accounts, before);

        let signed_metas = [
            unsigned_metas[0],
            unsigned_metas[1],
            unsigned_metas[2],
            meta(vote_init.node_pubkey, true, false),
        ];
        apply_launch_vote_instruction(&instruction, &signed_metas, &mut accounts, 1).unwrap();
        let initialized = accounts.clone();

        // v1.1 decodes and rejects initialized state before checking the new
        // validator-identity signature.
        assert!(matches!(
            apply_launch_vote_instruction(&instruction, &unsigned_metas, &mut accounts, 1),
            Err(LaunchVoteError::AccountAlreadyInitialized)
        ));
        assert_eq!(accounts, initialized);
    }

    #[test]
    fn initialize_preserves_historical_rent_clock_and_state_error_order() {
        let instruction =
            wincode::serialize(&VoteInstructionV100::InitializeAccount(VoteInitV100 {
                node_pubkey: [1; 32],
                authorized_voter: [2; 32],
                authorized_withdrawer: [3; 32],
                commission: 4,
            }))
            .unwrap();
        let wrong_rent = [21; 32];
        let mut accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(vec![0; 3_731], 0)),
            (wrong_rent, rent_account()),
        ]);

        let error = apply_launch_vote_instruction(
            &instruction,
            &[
                meta(VOTE_ACCOUNT, true, true),
                meta(wrong_rent, false, false),
            ],
            &mut accounts,
            1,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LaunchVoteError::InvalidSysvar { position: 1, .. }
        ));

        accounts.insert(RENT_SYSVAR_ID, rent_account());
        let error = apply_launch_vote_instruction(
            &instruction,
            &[
                meta(VOTE_ACCOUNT, true, true),
                meta(RENT_SYSVAR_ID, false, false),
            ],
            &mut accounts,
            1,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LaunchVoteError::InsufficientFunds {
                required: 26_858_640,
                ..
            }
        ));

        accounts.get_mut(&VOTE_ACCOUNT).unwrap().lamports = 26_858_640;
        let error = apply_launch_vote_instruction(
            &instruction,
            &[
                meta(VOTE_ACCOUNT, true, true),
                meta(RENT_SYSVAR_ID, false, false),
            ],
            &mut accounts,
            1,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LaunchVoteError::MissingAccount { position: 2 }
        ));
    }

    #[test]
    fn readonly_initialize_is_rejected_without_leaking_mutation() {
        let instruction =
            wincode::serialize(&VoteInstructionV100::InitializeAccount(VoteInitV100 {
                node_pubkey: [1; 32],
                authorized_voter: [2; 32],
                authorized_withdrawer: [3; 32],
                commission: 4,
            }))
            .unwrap();
        let mut accounts = AccountMap::from([
            (VOTE_ACCOUNT, vote_account(vec![0; 3_731], 26_858_640)),
            (RENT_SYSVAR_ID, rent_account()),
            (CLOCK_SYSVAR_ID, clock_account(1, 2)),
        ]);
        let before = accounts.clone();

        let error = apply_launch_vote_instruction(
            &instruction,
            &[
                meta(VOTE_ACCOUNT, true, false),
                meta(RENT_SYSVAR_ID, false, false),
                meta(CLOCK_SYSVAR_ID, false, false),
            ],
            &mut accounts,
            1,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            LaunchVoteError::ReadonlyDataModified { .. }
        ));
        assert_eq!(accounts, before);
    }

    #[test]
    fn authorize_enforces_signer_then_target_epoch_and_updates_prior_voters() {
        let new_authority = [22; 32];
        let instruction = wincode::serialize(&VoteInstructionV100::Authorize(
            new_authority,
            LaunchVoteAuthorize::Voter,
        ))
        .unwrap();
        let mut accounts = AccountMap::from([
            (
                VOTE_ACCOUNT,
                vote_account(fixed_vote_data(initialized_state()), 30_000_000),
            ),
            (CLOCK_SYSVAR_ID, clock_account(1, 2)),
            (AUTHORIZED_VOTER, crate::default_system_account()),
        ]);
        let unsigned_metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(CLOCK_SYSVAR_ID, false, false),
            meta(AUTHORIZED_VOTER, false, false),
        ];
        let before = accounts.clone();
        assert!(matches!(
            apply_launch_vote_instruction(&instruction, &unsigned_metas, &mut accounts, 1),
            Err(LaunchVoteError::MissingRequiredSignature {
                pubkey: AUTHORIZED_VOTER
            })
        ));
        assert_eq!(accounts, before);

        let signed_metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(CLOCK_SYSVAR_ID, false, false),
            meta(AUTHORIZED_VOTER, true, false),
        ];
        let mutation =
            apply_launch_vote_instruction(&instruction, &signed_metas, &mut accounts, 1).unwrap();
        assert_eq!(
            mutation,
            LaunchVoteMutation::Authorize {
                old_authority: AUTHORIZED_VOTER,
                new_authority,
                authority_type: LaunchVoteAuthorize::Voter,
                effective_epoch: Some(3),
            }
        );
        let state = decode_vote_state(&accounts[&VOTE_ACCOUNT].data)
            .unwrap()
            .into_current();
        assert_eq!(
            state.authorized_voters.authorized_voters,
            BTreeMap::from([(1, AUTHORIZED_VOTER), (3, new_authority)])
        );
        assert_eq!(state.prior_voters.last(), Some(&(AUTHORIZED_VOTER, 0, 3)));

        assert!(matches!(
            apply_launch_vote_instruction(&instruction, &signed_metas, &mut accounts, 1),
            Err(LaunchVoteError::TooSoonToReauthorize { epoch: 3 })
        ));
    }

    #[test]
    fn withdraw_moves_lamports_and_rolls_back_readonly_destination() {
        let instruction = wincode::serialize(&VoteInstructionV100::Withdraw(400)).unwrap();
        let mut accounts = AccountMap::from([
            (
                VOTE_ACCOUNT,
                vote_account(fixed_vote_data(initialized_state()), 1_000),
            ),
            (
                DESTINATION,
                AccountSnapshot {
                    lamports: 5,
                    ..crate::default_system_account()
                },
            ),
            (AUTHORIZED_WITHDRAWER, crate::default_system_account()),
        ]);
        let metas = [
            meta(VOTE_ACCOUNT, false, true),
            meta(DESTINATION, false, true),
            meta(AUTHORIZED_WITHDRAWER, true, false),
        ];
        let mutation =
            apply_launch_vote_instruction(&instruction, &metas, &mut accounts, 0).unwrap();
        assert_eq!(
            mutation,
            LaunchVoteMutation::Withdraw {
                destination: DESTINATION,
                lamports: 400,
            }
        );
        assert_eq!(accounts[&VOTE_ACCOUNT].lamports, 600);
        assert_eq!(accounts[&DESTINATION].lamports, 405);

        let readonly_metas = [metas[0], meta(DESTINATION, false, false), metas[2]];
        let before = accounts.clone();
        assert!(matches!(
            apply_launch_vote_instruction(&instruction, &readonly_metas, &mut accounts, 0),
            Err(LaunchVoteError::ReadonlyLamportChange {
                pubkey: DESTINATION
            })
        ));
        assert_eq!(accounts, before);
    }

    #[test]
    fn launch_vote_credits_decodes_legacy_wire_state() {
        let mut state = initialized_state().into_current();
        state.epoch_credits = vec![(1, 42, 9)];
        let data = fixed_vote_data(VoteStateVersionsV100::Current(state));
        assert_eq!(decode_launch_vote_credits(VOTE_ACCOUNT, &data).unwrap(), 42);
    }
}
