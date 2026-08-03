//! Solana v1.0.7 launch-Bank sysvar account materialization.
//!
//! None of these accounts are serialized in `genesis.bin`. The historical
//! Bank creates them around ordinary-account and native-builtin loading. This
//! module reproduces the resulting account bytes without importing a modern
//! Solana SDK whose wire types may have changed.

use std::collections::{BTreeMap, VecDeque};

use serde::Serialize;
use thiserror::Error;

use crate::{
    AccountSnapshot, AccountStoreError, CompactGenesisProbe, LaunchStakeError, LaunchStakeHistory,
    MemoryAccountStore, launch_stake_history_entry,
};

/// `Sysvar1111111111111111111111111111111111111`.
pub const SYSVAR_OWNER_ID: [u8; 32] = [
    6, 167, 213, 23, 24, 117, 247, 41, 199, 61, 147, 64, 143, 33, 97, 32, 6, 126, 216, 140, 118,
    224, 140, 40, 127, 193, 148, 96, 0, 0, 0, 0,
];
/// `SysvarFees111111111111111111111111111111111`.
pub const FEES_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 24, 226, 90, 141, 131, 80, 60, 37, 26, 122, 240, 113, 38, 253, 114, 0, 223,
    111, 196, 237, 82, 106, 156, 144, 0, 0, 0, 0,
];
/// `SysvarStakeHistory1111111111111111111111111`.
pub const STAKE_HISTORY_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 25, 53, 132, 208, 254, 237, 155, 179, 67, 29, 19, 32, 107, 229, 68, 40, 27,
    87, 184, 86, 108, 197, 55, 95, 244, 0, 0, 0,
];
/// `SysvarC1ock11111111111111111111111111111111`.
pub const CLOCK_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 24, 199, 116, 201, 40, 86, 99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155,
    75, 109, 92, 115, 85, 91, 33, 0, 0, 0, 0,
];
/// `SysvarRent111111111111111111111111111111111`.
pub const RENT_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 25, 44, 92, 81, 33, 140, 201, 76, 61, 74, 241, 127, 88, 218, 238, 8, 155, 161,
    253, 68, 227, 219, 217, 138, 0, 0, 0, 0,
];
/// `SysvarEpochSchedu1e111111111111111111111111`.
pub const EPOCH_SCHEDULE_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 24, 220, 63, 238, 2, 211, 228, 127, 1, 0, 248, 176, 84, 247, 148, 46, 96, 89,
    30, 63, 80, 135, 25, 168, 5, 0, 0, 0,
];
/// `SysvarRecentB1ockHashes11111111111111111111`.
pub const RECENT_BLOCKHASHES_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 25, 44, 86, 142, 224, 138, 132, 95, 115, 210, 151, 136, 207, 3, 92, 49, 69,
    178, 26, 179, 68, 216, 6, 46, 169, 64, 0, 0,
];
/// `SysvarRewards111111111111111111111111111111`.
pub const REWARDS_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 25, 44, 97, 55, 206, 224, 146, 217, 182, 146, 62, 225, 204, 214, 25, 3, 250,
    130, 184, 161, 97, 145, 87, 141, 128, 0, 0, 0,
];
/// `SysvarS1otHashes111111111111111111111111111`.
pub const SLOT_HASHES_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 25, 47, 10, 175, 198, 242, 101, 227, 251, 119, 204, 122, 218, 130, 197, 41,
    208, 190, 59, 19, 110, 45, 0, 85, 32, 0, 0, 0,
];
/// `SysvarS1otHistory11111111111111111111111111`.
pub const SLOT_HISTORY_SYSVAR_ID: [u8; 32] = [
    6, 167, 213, 23, 25, 47, 10, 175, 200, 117, 226, 225, 132, 87, 124, 80, 105, 207, 200, 70, 73,
    227, 235, 146, 120, 47, 149, 141, 72, 0, 0, 0,
];

pub const STAKE_HISTORY_DATA_LEN: usize = 16_392;
pub const RECENT_BLOCKHASHES_DATA_LEN: usize = 6_008;
pub const SLOT_HASHES_DATA_LEN: usize = 20_488;
pub const SLOT_HISTORY_DATA_LEN: usize = 131_097;

const RECENT_BLOCKHASH_SYSVAR_MAX_ENTRIES: usize = 150;
const RECENT_BLOCKHASH_SYSVAR_HEADER_LEN: usize = 8;
const RECENT_BLOCKHASH_SYSVAR_ENTRY_LEN: usize = 32 + 8;
const BLOCKHASH_QUEUE_MAX_AGE: u64 = 300;
const SLOT_HISTORY_MAX_ENTRIES: u64 = 1_024 * 1_024;
pub(crate) const SLOT_HISTORY_WORDS: usize = (SLOT_HISTORY_MAX_ENTRIES / 64) as usize;
const SLOT_HISTORY_WORDS_OFFSET: usize = 1 + 8;
const SLOT_HISTORY_BIT_LEN_OFFSET: usize = SLOT_HISTORY_WORDS_OFFSET + SLOT_HISTORY_WORDS * 8;
const SLOT_HISTORY_NEXT_SLOT_OFFSET: usize = SLOT_HISTORY_BIT_LEN_OFFSET + 8;

#[derive(Debug, Error)]
pub enum LaunchSysvarError {
    #[error("launch genesis has no slots-per-segment value")]
    MissingSlotsPerSegment,
    #[error("launch genesis slots-per-segment must be nonzero")]
    InvalidSlotsPerSegment,
    #[error("launch epoch schedule has invalid zero slots-per-epoch")]
    InvalidEpochSchedule,
    #[error("serialize launch {kind} sysvar: {source}")]
    Encode {
        kind: &'static str,
        #[source]
        source: wincode::error::Error,
    },
    #[error("serialized launch {kind} sysvar needs {needed} bytes, allocation is {available}")]
    AllocationTooSmall {
        kind: &'static str,
        needed: usize,
        available: usize,
    },
    #[error("launch Bank lifecycle must begin at slot 0, found slot {slot}")]
    UnexpectedFirstSlot { slot: u64 },
    #[error("slot {slot} declares parent {found}, replay expected parent {expected}")]
    ParentSlotMismatch {
        slot: u64,
        expected: u64,
        found: u64,
    },
    #[error("slot {slot} previous PoH blockhash does not match the completed parent Bank")]
    PreviousBlockhashMismatch { slot: u64 },
    #[error("slot {slot} repeats a PoH blockhash already present in the launch queue")]
    DuplicatePohBlockhash { slot: u64 },
    #[error("launch Bank sysvar lifecycle expected slot {expected}, found {found}")]
    SlotLifecycleOrder { expected: u64, found: u64 },
    #[error("slot {slot} began before its parent slot completed")]
    ParentBankIncomplete { slot: u64 },
    #[error("launch inflation rewards are not implemented at epoch {epoch}")]
    UnsupportedInflationRewards { epoch: u64 },
    #[error("rebuild launch StakeHistory: {0}")]
    StakeHistory(#[from] LaunchStakeError),
    #[error("store launch Bank sysvar accounts: {0}")]
    AccountStore(#[from] AccountStoreError),
    #[error("invalid frozen-Bank checkpoint state: {reason}")]
    InvalidCheckpointState { reason: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchBankSysvarUpdate {
    pub written_accounts: LaunchBankSysvarWrites,
    pub epoch_transition: Option<(u64, u64)>,
    /// SlotHashes needs the parent's Bank hash. Compact carries PoH hashes,
    /// which are deliberately not substituted.
    pub slot_hashes_unavailable: bool,
}

impl LaunchBankSysvarUpdate {
    fn genesis_bank() -> Self {
        Self {
            written_accounts: LaunchBankSysvarWrites::empty(),
            epoch_transition: None,
            slot_hashes_unavailable: false,
        }
    }
}

/// Allocation-free set of Bank-owned accounts written at one lifecycle edge.
///
/// The launch Bank has only three possible fixed write sets: none for the
/// genesis Bank, Clock/Fees/RecentBlockhashes for an ordinary child, and those
/// three plus Rewards/StakeHistory at an epoch boundary. Completion writes
/// only SlotHistory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaunchBankSysvarWrites {
    pubkeys: [[u8; 32]; 5],
    len: u8,
}

impl LaunchBankSysvarWrites {
    const fn empty() -> Self {
        Self {
            pubkeys: [[0; 32]; 5],
            len: 0,
        }
    }

    const fn child(epoch_transition: bool) -> Self {
        if epoch_transition {
            Self {
                pubkeys: [
                    REWARDS_SYSVAR_ID,
                    STAKE_HISTORY_SYSVAR_ID,
                    CLOCK_SYSVAR_ID,
                    FEES_SYSVAR_ID,
                    RECENT_BLOCKHASHES_SYSVAR_ID,
                ],
                len: 5,
            }
        } else {
            Self {
                pubkeys: [
                    CLOCK_SYSVAR_ID,
                    FEES_SYSVAR_ID,
                    RECENT_BLOCKHASHES_SYSVAR_ID,
                    [0; 32],
                    [0; 32],
                ],
                len: 3,
            }
        }
    }

    const fn slot_history() -> Self {
        Self {
            pubkeys: [SLOT_HISTORY_SYSVAR_ID, [0; 32], [0; 32], [0; 32], [0; 32]],
            len: 1,
        }
    }

    pub const fn len(&self) -> usize {
        self.len as usize
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl IntoIterator for LaunchBankSysvarWrites {
    type Item = [u8; 32];
    type IntoIter = std::iter::Take<std::array::IntoIter<[u8; 32], 5>>;

    fn into_iter(self) -> Self::IntoIter {
        self.pubkeys.into_iter().take(self.len())
    }
}

/// Stateful launch-era Bank sysvar lifecycle over a single ordered fork.
///
/// This owns only inputs that are not ordinary accounts: the fee governor,
/// recent-PoH-hash queue, SlotHistory bitset, and Bank position. SlotHashes is
/// intentionally absent until replay can compute historical Bank hashes.
#[derive(Debug, Clone)]
pub struct LaunchBankSysvarState {
    pub(crate) genesis: CompactGenesisProbe,
    pub(crate) fee_governor: LaunchFeeGovernor,
    pub(crate) current_fee: u64,
    pub(crate) parent_signature_count: u64,
    pub(crate) hash_height: u64,
    pub(crate) recent_blockhashes: BTreeMap<[u8; 32], LaunchRecentBlockhash>,
    /// The same entries as `recent_blockhashes`, newest hash height first.
    ///
    /// v1.0.7 kept the map above as its canonical cache. This bounded index
    /// avoids rebuilding and sorting that map every time a child Bank writes
    /// RecentBlockhashes. It is reconstructed rather than encoded in replay
    /// checkpoints so their wire representation remains stable.
    pub(crate) recent_blockhash_order: VecDeque<([u8; 32], LaunchRecentBlockhash)>,
    pub(crate) last_poh_blockhash: [u8; 32],
    pub(crate) slot_history_words: Vec<u64>,
    pub(crate) slot_history_next_slot: u64,
    pub(crate) current_slot: u64,
    pub(crate) current_epoch: u64,
    pub(crate) began_first_slot: bool,
    pub(crate) current_slot_completed: bool,
    pub(crate) inflation_disabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaunchRecentBlockhash {
    pub(crate) hash_height: u64,
    pub(crate) fee: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LaunchFeeGovernor {
    pub(crate) target_lamports_per_signature: u64,
    pub(crate) target_signatures_per_slot: u64,
    pub(crate) lamports_per_signature: u64,
}

impl LaunchFeeGovernor {
    fn derive(&mut self, latest_signatures_per_slot: u64) -> u64 {
        let target = self.target_lamports_per_signature;
        if self.target_signatures_per_slot == 0 {
            self.lamports_per_signature = target;
            return target;
        }
        let minimum = 1_u64.max(target / 2);
        let maximum = target.saturating_mul(10);
        let desired = maximum.min(minimum.max(
            target.saturating_mul(latest_signatures_per_slot.min(u64::from(u32::MAX)))
                / self.target_signatures_per_slot,
        ));
        let gap = i128::from(desired) - i128::from(self.lamports_per_signature);
        if gap == 0 {
            self.lamports_per_signature = desired;
        } else {
            let adjustment = 1_u64.max(target / 20);
            self.lamports_per_signature = if gap.is_positive() {
                self.lamports_per_signature.wrapping_add(adjustment)
            } else {
                self.lamports_per_signature.wrapping_sub(adjustment)
            }
            .clamp(minimum, maximum);
        }
        self.lamports_per_signature
    }
}

impl LaunchBankSysvarState {
    pub fn from_genesis(genesis: &CompactGenesisProbe) -> Result<Self, LaunchSysvarError> {
        // Reuse genesis account construction as the complete validation gate.
        launch_genesis_sysvar_accounts(genesis)?;
        let mut recent_blockhashes = BTreeMap::new();
        recent_blockhashes.insert(
            genesis.genesis_hash,
            LaunchRecentBlockhash {
                hash_height: 0,
                fee: 0,
            },
        );
        let mut recent_blockhash_order =
            VecDeque::with_capacity(BLOCKHASH_QUEUE_MAX_AGE as usize + 1);
        recent_blockhash_order.push_back((
            genesis.genesis_hash,
            LaunchRecentBlockhash {
                hash_height: 0,
                fee: 0,
            },
        ));
        let inflation_disabled = inflation_disabled_from_genesis(genesis);
        let mut slot_history_words = vec![0; SLOT_HISTORY_WORDS];
        slot_history_words[0] = 1;
        Ok(Self {
            genesis: genesis.clone(),
            fee_governor: LaunchFeeGovernor {
                target_lamports_per_signature: genesis.fees.target_lamports_per_sig,
                target_signatures_per_slot: genesis.fees.target_sigs_per_slot,
                // This private field is serde-skipped in v1.0.7 genesis.
                lamports_per_signature: 0,
            },
            current_fee: 0,
            parent_signature_count: 0,
            hash_height: 0,
            recent_blockhashes,
            recent_blockhash_order,
            last_poh_blockhash: genesis.genesis_hash,
            slot_history_words,
            slot_history_next_slot: 1,
            current_slot: 0,
            current_epoch: 0,
            began_first_slot: false,
            current_slot_completed: false,
            inflation_disabled,
        })
    }

    pub fn begin_slot(
        &mut self,
        slot: u64,
        parent_slot: u64,
        previous_blockhash: [u8; 32],
        accounts: &mut MemoryAccountStore,
        stake_history: &mut LaunchStakeHistory,
    ) -> Result<LaunchBankSysvarUpdate, LaunchSysvarError> {
        if !self.began_first_slot {
            if slot != 0 {
                return Err(LaunchSysvarError::UnexpectedFirstSlot { slot });
            }
            if previous_blockhash != self.last_poh_blockhash {
                return Err(LaunchSysvarError::PreviousBlockhashMismatch { slot });
            }
            self.began_first_slot = true;
            return Ok(LaunchBankSysvarUpdate::genesis_bank());
        }
        if !self.current_slot_completed {
            return Err(LaunchSysvarError::ParentBankIncomplete { slot });
        }
        if parent_slot != self.current_slot {
            return Err(LaunchSysvarError::ParentSlotMismatch {
                slot,
                expected: self.current_slot,
                found: parent_slot,
            });
        }
        if previous_blockhash != self.last_poh_blockhash {
            return Err(LaunchSysvarError::PreviousBlockhashMismatch { slot });
        }

        let next_epoch = epoch_for_slot(&self.genesis, slot)?;
        let epoch_transition =
            (next_epoch != self.current_epoch).then_some((self.current_epoch, next_epoch));
        let next_stake_history = if let Some((previous_epoch, next_epoch)) = epoch_transition {
            if !self.inflation_disabled {
                return Err(LaunchSysvarError::UnsupportedInflationRewards { epoch: next_epoch });
            }
            let mut next_history = stake_history.clone();
            let entry = launch_stake_history_entry(previous_epoch, accounts, &next_history)?;
            next_history.insert(previous_epoch, entry);
            Some(next_history)
        } else {
            None
        };

        let slots_per_segment = self
            .genesis
            .slots_per_segment
            .ok_or(LaunchSysvarError::MissingSlotsPerSegment)?;
        let clock = clock_for_slot(&self.genesis, slot, slots_per_segment)?;
        let mut next_fee_governor = self.fee_governor;
        let next_fee = next_fee_governor.derive(self.parent_signature_count);
        let stake_history_data = next_stake_history
            .as_ref()
            .map(stake_history_data)
            .transpose()?;

        if let (Some(next_history), Some(data)) = (next_stake_history, stake_history_data) {
            // Child-Bank order is Rewards, StakeHistory, Clock, Fees, then
            // RecentBlockhashes. SlotHashes would precede Rewards, but cannot
            // be materialized until the parent Bank hash is available.
            write_zeroed_sysvar_account(accounts, REWARDS_SYSVAR_ID, 16);
            accounts.insert(STAKE_HISTORY_SYSVAR_ID, sysvar_account(data));
            *stake_history = next_history;
        }
        write_clock_sysvar_account(accounts, &clock);
        write_fees_sysvar_account(accounts, next_fee);
        write_recent_blockhashes_sysvar_account(accounts, &self.recent_blockhash_order);

        self.fee_governor = next_fee_governor;
        self.current_fee = next_fee;
        self.current_slot = slot;
        self.current_epoch = next_epoch;
        self.current_slot_completed = false;
        Ok(LaunchBankSysvarUpdate {
            written_accounts: LaunchBankSysvarWrites::child(epoch_transition.is_some()),
            epoch_transition,
            slot_hashes_unavailable: true,
        })
    }

    pub fn complete_slot(
        &mut self,
        slot: u64,
        blockhash: [u8; 32],
        executed_signature_count: u64,
        accounts: &mut MemoryAccountStore,
    ) -> Result<LaunchBankSysvarWrites, LaunchSysvarError> {
        if slot != self.current_slot {
            return Err(LaunchSysvarError::SlotLifecycleOrder {
                expected: self.current_slot,
                found: slot,
            });
        }
        if self.recent_blockhashes.contains_key(&blockhash) {
            return Err(LaunchSysvarError::DuplicatePohBlockhash { slot });
        }
        if let Some(slot_history) = accounts.get(&SLOT_HISTORY_SYSVAR_ID)
            && slot_history.data.len() != SLOT_HISTORY_DATA_LEN
        {
            return Err(AccountStoreError::DataLengthMismatch {
                pubkey: SLOT_HISTORY_SYSVAR_ID,
                expected: SLOT_HISTORY_DATA_LEN,
                found: slot_history.data.len(),
            }
            .into());
        }
        self.hash_height = self.hash_height.wrapping_add(1);
        let hash_height = self.hash_height;
        if self.recent_blockhashes.len() >= BLOCKHASH_QUEUE_MAX_AGE as usize {
            while self
                .recent_blockhash_order
                .back()
                .is_some_and(|(_, entry)| {
                    hash_height.wrapping_sub(entry.hash_height) > BLOCKHASH_QUEUE_MAX_AGE
                })
            {
                let (expired_hash, expired_entry) = self
                    .recent_blockhash_order
                    .pop_back()
                    .expect("back was present");
                let removed_entry = self.recent_blockhashes.remove(&expired_hash);
                debug_assert_eq!(removed_entry, Some(expired_entry));
            }
        }
        let next_entry = LaunchRecentBlockhash {
            hash_height,
            fee: self.current_fee,
        };
        let replaced = self.recent_blockhashes.insert(blockhash, next_entry);
        debug_assert!(
            replaced.is_none(),
            "duplicate was rejected before insertion"
        );
        self.recent_blockhash_order
            .push_front((blockhash, next_entry));
        self.last_poh_blockhash = blockhash;
        self.parent_signature_count = executed_signature_count;
        // `blockstore_processor` freezes every fully replayed Bank, including
        // the terminal Bank when no child is constructed. Freeze writes the
        // current slot into SlotHistory after the final PoH hash is registered.
        self.add_slot_to_history(slot, accounts)?;
        self.current_slot_completed = true;
        Ok(LaunchBankSysvarWrites::slot_history())
    }

    pub fn current_fee(&self) -> u64 {
        self.current_fee
    }

    #[allow(dead_code)] // Used by the private checkpoint POC before its path runner lands.
    pub(crate) fn validate_frozen_checkpoint(
        &self,
        accounts: &MemoryAccountStore,
        stake_history: &LaunchStakeHistory,
    ) -> Result<(), LaunchSysvarError> {
        let invalid = |reason| LaunchSysvarError::InvalidCheckpointState { reason };
        if !self.began_first_slot || !self.current_slot_completed {
            return Err(invalid("Bank is not fully completed"));
        }
        if self.hash_height == 0 {
            return Err(invalid("completed Bank has zero hash height"));
        }
        if self.current_epoch != epoch_for_slot(&self.genesis, self.current_slot)? {
            return Err(invalid("current epoch does not match the genesis schedule"));
        }
        if self.fee_governor.target_lamports_per_signature
            != self.genesis.fees.target_lamports_per_sig
            || self.fee_governor.target_signatures_per_slot
                != self.genesis.fees.target_sigs_per_slot
        {
            return Err(invalid("fee governor targets do not match genesis"));
        }
        if self.inflation_disabled != inflation_disabled_from_genesis(&self.genesis) {
            return Err(invalid(
                "inflation-disabled cache does not match genesis inflation",
            ));
        }
        if self.fee_governor.lamports_per_signature != self.current_fee {
            return Err(invalid("fee governor and current fee disagree"));
        }
        if self.current_slot != 0 {
            let target = self.fee_governor.target_lamports_per_signature;
            let minimum = 1_u64.max(target / 2);
            let maximum = target.saturating_mul(10);
            if target == 0 || self.current_fee < minimum || self.current_fee > maximum {
                return Err(invalid("current fee is outside the genesis governor range"));
            }
        }
        if self.recent_blockhashes.len() > BLOCKHASH_QUEUE_MAX_AGE as usize + 1 {
            return Err(invalid("recent-blockhash queue exceeds its launch bound"));
        }
        if self.recent_blockhashes.values().any(|entry| {
            entry.hash_height > self.hash_height
                || self.hash_height.wrapping_sub(entry.hash_height) > BLOCKHASH_QUEUE_MAX_AGE
        }) {
            return Err(invalid(
                "recent-blockhash queue contains an impossible hash height",
            ));
        }
        if self.recent_blockhash_order.len() != self.recent_blockhashes.len() {
            return Err(invalid(
                "recent-blockhash order length does not match its cache",
            ));
        }
        let mut previous_height = None;
        for (hash, entry) in &self.recent_blockhash_order {
            if self.recent_blockhashes.get(hash) != Some(entry) {
                return Err(invalid(
                    "recent-blockhash order entry does not match its cache",
                ));
            }
            if previous_height.is_some_and(|previous| previous <= entry.hash_height) {
                return Err(invalid(
                    "recent-blockhash order is not strictly newest-first",
                ));
            }
            previous_height = Some(entry.hash_height);
        }
        if self.slot_history_words.len() != SLOT_HISTORY_WORDS {
            return Err(invalid("SlotHistory cache has the wrong word count"));
        }
        if self.slot_history_next_slot != self.current_slot.wrapping_add(1) {
            return Err(invalid(
                "SlotHistory next slot does not follow the frozen Bank",
            ));
        }
        let current_bit = self.current_slot % SLOT_HISTORY_MAX_ENTRIES;
        if self.slot_history_words[(current_bit / 64) as usize] & (1_u64 << (current_bit % 64)) == 0
        {
            return Err(invalid("SlotHistory does not contain the frozen slot"));
        }
        if stake_history
            .keys()
            .any(|epoch| *epoch >= self.current_epoch)
        {
            return Err(invalid(
                "StakeHistory contains an entry at or after the current epoch",
            ));
        }
        let last_hash = self
            .recent_blockhashes
            .get(&self.last_poh_blockhash)
            .ok_or_else(|| invalid("recent-blockhash queue lacks the frozen PoH hash"))?;
        if last_hash.hash_height != self.hash_height || last_hash.fee != self.current_fee {
            return Err(invalid(
                "frozen PoH queue entry has inconsistent height or fee",
            ));
        }
        let expected_genesis_sysvars = launch_genesis_sysvar_accounts(&self.genesis)?;
        for id in [RENT_SYSVAR_ID, EPOCH_SCHEDULE_SYSVAR_ID] {
            if accounts.get(&id) != expected_genesis_sysvars.get(&id) {
                return Err(invalid("immutable genesis sysvar account was modified"));
            }
        }
        let slots_per_segment = self
            .genesis
            .slots_per_segment
            .ok_or(LaunchSysvarError::MissingSlotsPerSegment)?;
        let expected_clock = sysvar_account(padded_data(
            "Clock",
            &clock_for_slot(&self.genesis, self.current_slot, slots_per_segment)?,
            None,
        )?);
        if accounts.get(&CLOCK_SYSVAR_ID) != Some(&expected_clock) {
            return Err(invalid("Clock sysvar does not match the frozen Bank"));
        }
        let fees = accounts
            .get(&FEES_SYSVAR_ID)
            .ok_or_else(|| invalid("Fees sysvar account is absent"))?;
        if fees != &sysvar_account(self.current_fee.to_le_bytes().to_vec()) {
            return Err(invalid("Fees sysvar does not match the fee cache"));
        }
        let expected_recent = sysvar_account(padded_data(
            "RecentBlockhashes",
            &recent_blockhashes_wire(&self.recent_blockhash_order, Some(self.last_poh_blockhash)),
            Some(RECENT_BLOCKHASHES_DATA_LEN),
        )?);
        if accounts.get(&RECENT_BLOCKHASHES_SYSVAR_ID) != Some(&expected_recent) {
            return Err(invalid(
                "RecentBlockhashes sysvar does not match its decoded cache",
            ));
        }
        let history = accounts
            .get(&SLOT_HISTORY_SYSVAR_ID)
            .ok_or_else(|| invalid("SlotHistory sysvar account is absent"))?;
        if history
            != &sysvar_account(slot_history_data(
                &self.slot_history_words,
                self.slot_history_next_slot,
            ))
        {
            return Err(invalid(
                "SlotHistory account does not match its decoded cache",
            ));
        }
        let stake = accounts
            .get(&STAKE_HISTORY_SYSVAR_ID)
            .ok_or_else(|| invalid("StakeHistory sysvar account is absent"))?;
        if stake != &sysvar_account(stake_history_data(stake_history)?) {
            return Err(invalid(
                "StakeHistory account does not match its decoded cache",
            ));
        }
        Ok(())
    }

    fn add_slot_to_history(
        &mut self,
        slot: u64,
        accounts: &mut MemoryAccountStore,
    ) -> Result<(), LaunchSysvarError> {
        let mut account = accounts.get_mut(&SLOT_HISTORY_SYSVAR_ID);
        if let Some(existing) = account.as_ref()
            && existing.data.len() != SLOT_HISTORY_DATA_LEN
        {
            let found = existing.data.len();
            return Err(AccountStoreError::DataLengthMismatch {
                pubkey: SLOT_HISTORY_SYSVAR_ID,
                expected: SLOT_HISTORY_DATA_LEN,
                found,
            }
            .into());
        }

        for skipped in self.slot_history_next_slot..slot {
            set_slot_history_bit(&mut self.slot_history_words, skipped, false);
            if let Some(existing) = account.as_mut() {
                let index = ((skipped % SLOT_HISTORY_MAX_ENTRIES) / 64) as usize;
                let offset = SLOT_HISTORY_WORDS_OFFSET + index * 8;
                existing.data[offset..offset + 8]
                    .copy_from_slice(&self.slot_history_words[index].to_le_bytes());
            }
        }
        set_slot_history_bit(&mut self.slot_history_words, slot, true);
        if let Some(existing) = account.as_mut() {
            let index = ((slot % SLOT_HISTORY_MAX_ENTRIES) / 64) as usize;
            let offset = SLOT_HISTORY_WORDS_OFFSET + index * 8;
            existing.data[offset..offset + 8]
                .copy_from_slice(&self.slot_history_words[index].to_le_bytes());
        }
        self.slot_history_next_slot = slot.wrapping_add(1);
        if let Some(existing) = account {
            existing.data[SLOT_HISTORY_NEXT_SLOT_OFFSET..SLOT_HISTORY_NEXT_SLOT_OFFSET + 8]
                .copy_from_slice(&self.slot_history_next_slot.to_le_bytes());
        } else {
            accounts.insert(
                SLOT_HISTORY_SYSVAR_ID,
                sysvar_account(slot_history_data(
                    &self.slot_history_words,
                    self.slot_history_next_slot,
                )),
            );
        }
        Ok(())
    }
}

fn inflation_disabled_from_genesis(genesis: &CompactGenesisProbe) -> bool {
    genesis.inflation.initial == 0.0
        && genesis.inflation.terminal == 0.0
        && genesis.inflation.taper == 0.0
        && genesis.inflation.foundation == 0.0
        && genesis.inflation.foundation_term == 0.0
        && genesis.inflation_storage.unwrap_or(0.0) == 0.0
}

fn set_slot_history_bit(words: &mut [u64], slot: u64, value: bool) {
    let bit = slot % SLOT_HISTORY_MAX_ENTRIES;
    let word = &mut words[(bit / 64) as usize];
    let mask = 1_u64 << (bit % 64);
    if value {
        *word |= mask;
    } else {
        *word &= !mask;
    }
}

fn slot_history_data(words: &[u64], next_slot: u64) -> Vec<u8> {
    let mut data = vec![0; SLOT_HISTORY_DATA_LEN];
    data[0] = 1; // `Some(Box<[u64]>)`
    data[1..9].copy_from_slice(&(words.len() as u64).to_le_bytes());
    for (index, word) in words.iter().enumerate() {
        let offset = SLOT_HISTORY_WORDS_OFFSET + index * 8;
        data[offset..offset + 8].copy_from_slice(&word.to_le_bytes());
    }
    data[SLOT_HISTORY_BIT_LEN_OFFSET..SLOT_HISTORY_BIT_LEN_OFFSET + 8]
        .copy_from_slice(&SLOT_HISTORY_MAX_ENTRIES.to_le_bytes());
    data[SLOT_HISTORY_NEXT_SLOT_OFFSET..SLOT_HISTORY_NEXT_SLOT_OFFSET + 8]
        .copy_from_slice(&next_slot.to_le_bytes());
    data
}

fn stake_history_data(history: &LaunchStakeHistory) -> Result<Vec<u8>, LaunchSysvarError> {
    let entries = history
        .iter()
        .rev()
        .take(512)
        .map(|(epoch, entry)| {
            (
                *epoch,
                StakeHistoryEntryWire {
                    effective: entry.effective,
                    activating: entry.activating,
                    deactivating: entry.deactivating,
                },
            )
        })
        .collect();
    padded_data(
        "StakeHistory",
        &StakeHistoryWire(entries),
        Some(STAKE_HISTORY_DATA_LEN),
    )
}

fn epoch_for_slot(genesis: &CompactGenesisProbe, slot: u64) -> Result<u64, LaunchSysvarError> {
    let schedule = &genesis.epoch_schedule;
    if schedule.slots_per_epoch == 0 {
        return Err(LaunchSysvarError::InvalidEpochSchedule);
    }
    if slot < schedule.first_normal_slot {
        let minimum_slots_per_epoch = 32_u64;
        let epoch = (slot + minimum_slots_per_epoch + 1)
            .next_power_of_two()
            .trailing_zeros()
            - minimum_slots_per_epoch.trailing_zeros()
            - 1;
        Ok(u64::from(epoch))
    } else {
        Ok(schedule.first_normal_epoch
            + (slot - schedule.first_normal_slot) / schedule.slots_per_epoch)
    }
}

/// Build the six accounts created by `Bank::new()` around launch genesis.
///
/// Fees starts at zero because v1.0.7's private fee-governor current-rate field
/// is `serde(skip)` in `genesis.bin`. RecentBlockhashes therefore also stores a
/// zero-fee entry for the genesis hash.
pub fn launch_genesis_sysvar_accounts(
    genesis: &CompactGenesisProbe,
) -> Result<BTreeMap<[u8; 32], AccountSnapshot>, LaunchSysvarError> {
    let slots_per_segment = genesis
        .slots_per_segment
        .ok_or(LaunchSysvarError::MissingSlotsPerSegment)?;
    if slots_per_segment == 0 {
        return Err(LaunchSysvarError::InvalidSlotsPerSegment);
    }
    if genesis.epoch_schedule.slots_per_epoch == 0 {
        return Err(LaunchSysvarError::InvalidEpochSchedule);
    }

    let fee_calculator = FeeCalculatorWire {
        lamports_per_signature: 0,
    };
    let fees = padded_data("Fees", &FeesWire { fee_calculator }, None)?;
    let stake_history = padded_data(
        "StakeHistory",
        &StakeHistoryWire(Vec::new()),
        Some(STAKE_HISTORY_DATA_LEN),
    )?;
    let clock = padded_data(
        "Clock",
        &clock_for_slot(genesis, 0, slots_per_segment)?,
        None,
    )?;
    let rent = padded_data(
        "Rent",
        &RentWire {
            lamports_per_byte_year: genesis.rent.lamports_per_byte_year,
            exemption_threshold: genesis.rent.exemption_threshold,
            burn_percent: genesis.rent.burn_percent,
        },
        None,
    )?;
    let epoch_schedule = padded_data(
        "EpochSchedule",
        &EpochScheduleWire {
            slots_per_epoch: genesis.epoch_schedule.slots_per_epoch,
            leader_schedule_slot_offset: genesis.epoch_schedule.leader_schedule_slot_offset,
            warmup: genesis.epoch_schedule.warmup,
            first_normal_epoch: genesis.epoch_schedule.first_normal_epoch,
            first_normal_slot: genesis.epoch_schedule.first_normal_slot,
        },
        None,
    )?;
    let recent_blockhashes = padded_data(
        "RecentBlockhashes",
        &OwnedRecentBlockhashesWire(vec![RecentBlockhashEntryWire {
            blockhash: genesis.genesis_hash,
            fee_calculator,
        }]),
        Some(RECENT_BLOCKHASHES_DATA_LEN),
    )?;

    Ok(BTreeMap::from([
        (FEES_SYSVAR_ID, sysvar_account(fees)),
        (STAKE_HISTORY_SYSVAR_ID, sysvar_account(stake_history)),
        (CLOCK_SYSVAR_ID, sysvar_account(clock)),
        (RENT_SYSVAR_ID, sysvar_account(rent)),
        (EPOCH_SCHEDULE_SYSVAR_ID, sysvar_account(epoch_schedule)),
        (
            RECENT_BLOCKHASHES_SYSVAR_ID,
            sysvar_account(recent_blockhashes),
        ),
    ]))
}

fn sysvar_account(data: Vec<u8>) -> AccountSnapshot {
    AccountSnapshot {
        lamports: 1,
        owner: SYSVAR_OWNER_ID,
        executable: false,
        rent_epoch: 0,
        data: data.into(),
    }
}

fn write_fixed_sysvar_account(
    accounts: &mut MemoryAccountStore,
    pubkey: [u8; 32],
    data_len: usize,
    write_data: impl FnOnce(&mut [u8]),
) {
    let Some(account) = accounts.get_mut(&pubkey) else {
        let mut account = sysvar_account(vec![0; data_len]);
        write_data(&mut account.data);
        let replaced = accounts.insert(pubkey, account);
        debug_assert!(replaced.is_none(), "missing sysvar account was initialized");
        return;
    };
    account.lamports = 1;
    account.owner = SYSVAR_OWNER_ID;
    account.executable = false;
    account.rent_epoch = 0;
    account.data.resize(data_len, 0);
    write_data(&mut account.data);
}

fn write_zeroed_sysvar_account(
    accounts: &mut MemoryAccountStore,
    pubkey: [u8; 32],
    data_len: usize,
) {
    write_fixed_sysvar_account(accounts, pubkey, data_len, |data| data.fill(0));
}

fn write_clock_sysvar_account(accounts: &mut MemoryAccountStore, clock: &ClockWire) {
    write_fixed_sysvar_account(accounts, CLOCK_SYSVAR_ID, 40, |data| {
        data[0..8].copy_from_slice(&clock.slot.to_le_bytes());
        data[8..16].copy_from_slice(&clock.segment.to_le_bytes());
        data[16..24].copy_from_slice(&clock.epoch.to_le_bytes());
        data[24..32].copy_from_slice(&clock.leader_schedule_epoch.to_le_bytes());
        data[32..40].copy_from_slice(&clock.unix_timestamp.to_le_bytes());
    });
}

fn write_fees_sysvar_account(accounts: &mut MemoryAccountStore, lamports_per_signature: u64) {
    write_fixed_sysvar_account(accounts, FEES_SYSVAR_ID, 8, |data| {
        data.copy_from_slice(&lamports_per_signature.to_le_bytes());
    });
}

fn write_recent_blockhashes_sysvar_account(
    accounts: &mut MemoryAccountStore,
    entries: &VecDeque<([u8; 32], LaunchRecentBlockhash)>,
) {
    // Rewriting directly from the already ordered Bank cache is cheaper than
    // validating the complete prior 6-KiB wire and then shifting it by one
    // entry. At the 150-entry steady state every output byte is written exactly
    // once. The same path also retains the historical self-healing behavior for
    // missing, malformed, or externally altered sysvar accounts.
    write_fixed_sysvar_account(
        accounts,
        RECENT_BLOCKHASHES_SYSVAR_ID,
        RECENT_BLOCKHASHES_DATA_LEN,
        |data| rewrite_recent_blockhashes_data(data, entries),
    );
}

fn rewrite_recent_blockhashes_data(
    data: &mut [u8],
    entries: &VecDeque<([u8; 32], LaunchRecentBlockhash)>,
) {
    debug_assert_eq!(data.len(), RECENT_BLOCKHASHES_DATA_LEN);
    let entry_count = entries.len().min(RECENT_BLOCKHASH_SYSVAR_MAX_ENTRIES);
    data[..RECENT_BLOCKHASH_SYSVAR_HEADER_LEN].copy_from_slice(&(entry_count as u64).to_le_bytes());
    let mut offset = RECENT_BLOCKHASH_SYSVAR_HEADER_LEN;
    for (blockhash, entry) in entries.iter().take(entry_count) {
        write_recent_blockhash_entry(data, offset, blockhash, *entry);
        offset += RECENT_BLOCKHASH_SYSVAR_ENTRY_LEN;
    }
    data[offset..].fill(0);
}

fn write_recent_blockhash_entry(
    data: &mut [u8],
    offset: usize,
    blockhash: &[u8; 32],
    entry: LaunchRecentBlockhash,
) {
    data[offset..offset + 32].copy_from_slice(blockhash);
    data[offset + 32..offset + RECENT_BLOCKHASH_SYSVAR_ENTRY_LEN]
        .copy_from_slice(&entry.fee.to_le_bytes());
}

fn recent_blockhashes_wire(
    entries: &VecDeque<([u8; 32], LaunchRecentBlockhash)>,
    excluded_hash: Option<[u8; 32]>,
) -> OwnedRecentBlockhashesWire {
    OwnedRecentBlockhashesWire(
        entries
            .iter()
            .filter(|(hash, _)| excluded_hash != Some(*hash))
            .take(RECENT_BLOCKHASH_SYSVAR_MAX_ENTRIES)
            .map(|(blockhash, entry)| RecentBlockhashEntryWire {
                blockhash: *blockhash,
                fee_calculator: FeeCalculatorWire {
                    lamports_per_signature: entry.fee,
                },
            })
            .collect(),
    )
}

fn padded_data<T: Serialize + wincode::SchemaWrite<wincode::config::DefaultConfig, Src = T>>(
    kind: &'static str,
    value: &T,
    allocation: Option<usize>,
) -> Result<Vec<u8>, LaunchSysvarError> {
    let encoded_len =
        wincode::serialized_size(value).map_err(|source| LaunchSysvarError::Encode {
            kind,
            source: wincode::error::Error::WriteError(source),
        })?;
    let encoded_len =
        usize::try_from(encoded_len).map_err(|_| LaunchSysvarError::AllocationTooSmall {
            kind,
            needed: usize::MAX,
            available: allocation.unwrap_or(usize::MAX),
        })?;
    let data_len = allocation.unwrap_or(encoded_len);
    if encoded_len > data_len {
        return Err(LaunchSysvarError::AllocationTooSmall {
            kind,
            needed: encoded_len,
            available: data_len,
        });
    }
    let mut data = vec![0; data_len];
    wincode::serialize_into(&mut data[..encoded_len], value).map_err(|source| {
        LaunchSysvarError::Encode {
            kind,
            source: wincode::error::Error::WriteError(source),
        }
    })?;
    Ok(data)
}

fn clock_for_slot(
    genesis: &CompactGenesisProbe,
    slot: u64,
    slots_per_segment: u64,
) -> Result<ClockWire, LaunchSysvarError> {
    let schedule = &genesis.epoch_schedule;
    if schedule.slots_per_epoch == 0 {
        return Err(LaunchSysvarError::InvalidEpochSchedule);
    }
    let (epoch, leader_schedule_epoch) = if slot < schedule.first_normal_slot {
        // The replay setup currently rejects warmup schedules, but preserving
        // the historical formula here keeps this serializer independently exact.
        let minimum_slots_per_epoch = 32_u64;
        let epoch = (slot + minimum_slots_per_epoch + 1)
            .next_power_of_two()
            .trailing_zeros()
            - minimum_slots_per_epoch.trailing_zeros()
            - 1;
        (u64::from(epoch), u64::from(epoch) + 1)
    } else {
        let normal_slot = slot - schedule.first_normal_slot;
        (
            schedule.first_normal_epoch + normal_slot / schedule.slots_per_epoch,
            schedule.first_normal_epoch
                + (normal_slot + schedule.leader_schedule_slot_offset) / schedule.slots_per_epoch,
        )
    };
    let ns_per_tick = u128::from(genesis.poh_params.tick_duration_secs)
        .checked_mul(1_000_000_000)
        .and_then(|nanos| nanos.checked_add(u128::from(genesis.poh_params.tick_duration_nanos)))
        .ok_or(LaunchSysvarError::InvalidEpochSchedule)?;
    let elapsed_seconds = u128::from(slot)
        .checked_mul(ns_per_tick)
        .and_then(|nanos| nanos.checked_mul(u128::from(genesis.ticks_per_slot)))
        .map(|nanos| nanos / 1_000_000_000)
        .ok_or(LaunchSysvarError::InvalidEpochSchedule)?;

    Ok(ClockWire {
        slot,
        segment: slot.div_ceil(slots_per_segment),
        epoch,
        leader_schedule_epoch,
        unix_timestamp: genesis
            .creation_time_unix
            .wrapping_add(elapsed_seconds as i64),
    })
}

#[derive(Debug, Clone, Copy, Serialize, wincode::SchemaWrite)]
struct FeeCalculatorWire {
    lamports_per_signature: u64,
}

#[derive(Debug, Serialize, wincode::SchemaWrite)]
struct FeesWire {
    fee_calculator: FeeCalculatorWire,
}

#[derive(Debug, Serialize, wincode::SchemaWrite)]
struct StakeHistoryEntryWire {
    effective: u64,
    activating: u64,
    deactivating: u64,
}

#[derive(Debug, Serialize, wincode::SchemaWrite)]
struct StakeHistoryWire(Vec<(u64, StakeHistoryEntryWire)>);

#[derive(Debug, Serialize, wincode::SchemaWrite)]
struct ClockWire {
    slot: u64,
    segment: u64,
    epoch: u64,
    leader_schedule_epoch: u64,
    unix_timestamp: i64,
}

#[derive(Debug, Serialize, wincode::SchemaWrite)]
struct RentWire {
    lamports_per_byte_year: u64,
    exemption_threshold: f64,
    burn_percent: u8,
}

#[derive(Debug, Serialize, wincode::SchemaWrite)]
struct EpochScheduleWire {
    slots_per_epoch: u64,
    leader_schedule_slot_offset: u64,
    warmup: bool,
    first_normal_epoch: u64,
    first_normal_slot: u64,
}

#[derive(Debug, Serialize, wincode::SchemaWrite)]
struct RecentBlockhashEntryWire {
    blockhash: [u8; 32],
    fee_calculator: FeeCalculatorWire,
}

#[derive(Debug, Serialize, wincode::SchemaWrite)]
struct OwnedRecentBlockhashesWire(Vec<RecentBlockhashEntryWire>);

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use blockzilla_format::{
        WincodeArchiveV2GenesisEpochSchedule, WincodeArchiveV2GenesisFeeParams,
        WincodeArchiveV2GenesisInflationParams, WincodeArchiveV2GenesisPohParams,
        WincodeArchiveV2GenesisRentParams,
    };
    use sha2::Digest;

    use super::*;

    #[test]
    fn fee_governor_arithmetic_saturates_for_hostile_values() {
        let mut governor = LaunchFeeGovernor {
            target_lamports_per_signature: u64::MAX,
            target_signatures_per_slot: 1,
            lamports_per_signature: u64::MAX,
        };
        assert_eq!(governor.derive(u64::MAX), u64::MAX);
    }
    use crate::{
        CompactGenesisSource, LaunchDelegation, LaunchStake, LaunchStakeAuthorized,
        LaunchStakeLockup, LaunchStakeMeta, LaunchStakeState, STAKE_PROGRAM_ID,
    };

    fn mainnet_fixture() -> CompactGenesisProbe {
        CompactGenesisProbe {
            source: CompactGenesisSource::ExactGenesisBin,
            genesis_hash: [0x45; 32],
            genesis_bin_len: 132_347,
            creation_time_unix: 1_584_368_940,
            cluster_id: 1,
            ticks_per_slot: 64,
            slots_per_segment: Some(1_024),
            backwards_compat_with_v0_23: Some(0),
            poh_params: WincodeArchiveV2GenesisPohParams {
                tick_duration_secs: 0,
                tick_duration_nanos: 6_250_000,
                tick_count: None,
                hashes_per_tick: Some(12_500),
            },
            fees: WincodeArchiveV2GenesisFeeParams {
                target_lamports_per_sig: 10_000,
                target_sigs_per_slot: 20_000,
                min_lamports_per_sig: 5_000,
                max_lamports_per_sig: 100_000,
                burn_percent: 100,
            },
            rent: WincodeArchiveV2GenesisRentParams {
                lamports_per_byte_year: 3_480,
                exemption_threshold: 2.0,
                burn_percent: 100,
            },
            inflation: WincodeArchiveV2GenesisInflationParams {
                initial: 0.0,
                terminal: 0.0,
                taper: 0.0,
                foundation: 0.0,
                foundation_term: 0.0,
                padding: [0; 8],
            },
            inflation_storage: Some(0.0),
            epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
                slots_per_epoch: 432_000,
                leader_schedule_slot_offset: 432_000,
                warmup: false,
                first_normal_epoch: 0,
                first_normal_slot: 0,
            },
            accounts: Vec::new(),
            builtins: Vec::new(),
            reward_pools: Vec::new(),
        }
    }

    fn numbered_hash(value: u64) -> [u8; 32] {
        let mut hash = [0xa5; 32];
        hash[..8].copy_from_slice(&value.to_le_bytes());
        hash
    }

    fn legacy_recent_blockhash_data(
        state: &LaunchBankSysvarState,
        excluded_hash: Option<[u8; 32]>,
    ) -> Vec<u8> {
        let mut entries = state
            .recent_blockhashes
            .iter()
            .filter(|(hash, _)| excluded_hash != Some(**hash))
            .map(|(hash, entry)| (*hash, *entry))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(_, entry)| std::cmp::Reverse(entry.hash_height));
        let wire = OwnedRecentBlockhashesWire(
            entries
                .into_iter()
                .take(RECENT_BLOCKHASH_SYSVAR_MAX_ENTRIES)
                .map(|(blockhash, entry)| RecentBlockhashEntryWire {
                    blockhash,
                    fee_calculator: FeeCalculatorWire {
                        lamports_per_signature: entry.fee,
                    },
                })
                .collect(),
        );
        let mut data = wincode::serialize(&wire).unwrap();
        data.resize(RECENT_BLOCKHASHES_DATA_LEN, 0);
        data
    }

    fn ordered_recent_blockhash_data(
        state: &LaunchBankSysvarState,
        excluded_hash: Option<[u8; 32]>,
    ) -> Vec<u8> {
        full_recent_blockhash_data(&state.recent_blockhash_order, excluded_hash)
    }

    fn full_recent_blockhash_data(
        entries: &VecDeque<([u8; 32], LaunchRecentBlockhash)>,
        excluded_hash: Option<[u8; 32]>,
    ) -> Vec<u8> {
        padded_data(
            "RecentBlockhashes",
            &recent_blockhashes_wire(entries, excluded_hash),
            Some(RECENT_BLOCKHASHES_DATA_LEN),
        )
        .unwrap()
    }

    #[test]
    fn launch_genesis_materializes_six_exactly_shaped_sysvars() {
        let genesis = mainnet_fixture();
        let accounts = launch_genesis_sysvar_accounts(&genesis).unwrap();
        assert_eq!(accounts.len(), 6);
        for account in accounts.values() {
            assert_eq!(account.lamports, 1);
            assert_eq!(account.owner, SYSVAR_OWNER_ID);
            assert!(!account.executable);
            assert_eq!(account.rent_epoch, 0);
        }
        assert_eq!(accounts[&FEES_SYSVAR_ID].data, 0_u64.to_le_bytes());
        assert_eq!(accounts[&STAKE_HISTORY_SYSVAR_ID].data.len(), 16_392);
        assert_eq!(&accounts[&STAKE_HISTORY_SYSVAR_ID].data[..8], &[0; 8]);
        assert_eq!(accounts[&CLOCK_SYSVAR_ID].data.len(), 40);
        assert_eq!(
            &accounts[&CLOCK_SYSVAR_ID].data,
            &[
                0, 0, 0, 0, 0, 0, 0, 0, // slot
                0, 0, 0, 0, 0, 0, 0, 0, // segment
                0, 0, 0, 0, 0, 0, 0, 0, // epoch
                1, 0, 0, 0, 0, 0, 0, 0, // leader schedule epoch
                44, 141, 111, 94, 0, 0, 0, 0, // unix timestamp
            ]
        );
        assert_eq!(accounts[&RENT_SYSVAR_ID].data.len(), 17);
        assert_eq!(accounts[&EPOCH_SCHEDULE_SYSVAR_ID].data.len(), 33);
        assert_eq!(accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data.len(), 6_008);
        assert_eq!(
            &accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data[..8],
            &1_u64.to_le_bytes()
        );
        assert_eq!(
            &accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data[8..40],
            &genesis.genesis_hash
        );
        assert_eq!(
            &accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data[40..48],
            &0_u64.to_le_bytes()
        );
        assert!(
            accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data[48..]
                .iter()
                .all(|byte| *byte == 0)
        );
    }

    #[test]
    fn in_place_recent_blockhashes_match_full_reference_across_gaps_and_eviction() {
        let genesis = mainnet_fixture();
        let mut accounts = launch_genesis_sysvar_accounts(&genesis)
            .unwrap()
            .into_iter()
            .collect::<MemoryAccountStore>();
        let mut history = LaunchStakeHistory::new();
        let mut lifecycle = LaunchBankSysvarState::from_genesis(&genesis).unwrap();
        let mut slot = 0;
        let mut parent_slot = 0;
        let mut previous_hash = genesis.genesis_hash;

        for height in 1..=310_u64 {
            if height > 1 {
                slot += 1 + height % 3;
            }
            lifecycle
                .begin_slot(
                    slot,
                    parent_slot,
                    previous_hash,
                    &mut accounts,
                    &mut history,
                )
                .unwrap();
            assert_eq!(
                accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data,
                legacy_recent_blockhash_data(&lifecycle, None),
                "child-Bank RecentBlockhashes diverged at hash height {height}",
            );
            assert_eq!(
                ordered_recent_blockhash_data(&lifecycle, None),
                legacy_recent_blockhash_data(&lifecycle, None),
            );

            let blockhash = numbered_hash(height);
            lifecycle
                .complete_slot(slot, blockhash, height, &mut accounts)
                .unwrap();
            assert_eq!(
                accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data,
                legacy_recent_blockhash_data(&lifecycle, Some(blockhash)),
                "frozen-account exclusion diverged at hash height {height}",
            );
            assert_eq!(
                ordered_recent_blockhash_data(&lifecycle, Some(blockhash)),
                legacy_recent_blockhash_data(&lifecycle, Some(blockhash)),
            );
            parent_slot = slot;
            previous_hash = blockhash;
        }

        assert_eq!(lifecycle.recent_blockhashes.len(), 301);
        assert_eq!(lifecycle.recent_blockhash_order.len(), 301);
        assert_eq!(
            lifecycle
                .recent_blockhash_order
                .front()
                .unwrap()
                .1
                .hash_height,
            310
        );
        assert_eq!(
            lifecycle
                .recent_blockhash_order
                .back()
                .unwrap()
                .1
                .hash_height,
            10
        );
        lifecycle
            .validate_frozen_checkpoint(&accounts, &history)
            .unwrap();
    }

    #[test]
    fn in_place_recent_blockhashes_repair_missing_and_malformed_accounts() {
        let entries = (0..=3_u64)
            .rev()
            .map(|height| {
                (
                    numbered_hash(height),
                    LaunchRecentBlockhash {
                        hash_height: height,
                        fee: 5_000 + height,
                    },
                )
            })
            .collect::<VecDeque<_>>();
        let newest_hash = entries.front().unwrap().0;
        let expected = sysvar_account(full_recent_blockhash_data(&entries, None));
        let previous_data = full_recent_blockhash_data(&entries, Some(newest_hash));

        let mut rewritten = MemoryAccountStore::new();
        let mut previous_account = sysvar_account(previous_data.clone());
        previous_account.lamports = 99;
        previous_account.owner = [0x44; 32];
        previous_account.executable = true;
        previous_account.rent_epoch = 77;
        rewritten.insert(RECENT_BLOCKHASHES_SYSVAR_ID, previous_account);
        let previous_pointer = rewritten[&RECENT_BLOCKHASHES_SYSVAR_ID].data.as_ptr();
        write_recent_blockhashes_sysvar_account(&mut rewritten, &entries);
        assert_eq!(rewritten[&RECENT_BLOCKHASHES_SYSVAR_ID], expected);
        assert_eq!(
            rewritten[&RECENT_BLOCKHASHES_SYSVAR_ID].data.as_ptr(),
            previous_pointer
        );

        let mut missing = MemoryAccountStore::new();
        write_recent_blockhashes_sysvar_account(&mut missing, &entries);
        assert_eq!(missing[&RECENT_BLOCKHASHES_SYSVAR_ID], expected);

        for malformed_data in [
            vec![0xa5; 17],
            vec![0xff; RECENT_BLOCKHASHES_DATA_LEN],
            {
                let mut corrupted = previous_data.clone();
                corrupted[RECENT_BLOCKHASH_SYSVAR_HEADER_LEN + 7] ^= 1;
                corrupted
            },
            {
                let mut corrupted = previous_data;
                *corrupted.last_mut().unwrap() = 1;
                corrupted
            },
        ] {
            let mut malformed = MemoryAccountStore::new();
            malformed.insert(RECENT_BLOCKHASHES_SYSVAR_ID, sysvar_account(malformed_data));
            write_recent_blockhashes_sysvar_account(&mut malformed, &entries);
            assert_eq!(malformed[&RECENT_BLOCKHASHES_SYSVAR_ID], expected);
        }
    }

    #[test]
    fn duplicate_recent_blockhash_is_rejected_without_mutating_freeze_state() {
        let genesis = mainnet_fixture();
        let mut accounts = launch_genesis_sysvar_accounts(&genesis)
            .unwrap()
            .into_iter()
            .collect::<MemoryAccountStore>();
        let mut history = LaunchStakeHistory::new();
        let mut lifecycle = LaunchBankSysvarState::from_genesis(&genesis).unwrap();
        let hash_a = numbered_hash(1);
        let hash_b = numbered_hash(2);
        let recovery_hash = numbered_hash(3);

        lifecycle
            .begin_slot(0, 0, genesis.genesis_hash, &mut accounts, &mut history)
            .unwrap();
        lifecycle
            .complete_slot(0, hash_a, 0, &mut accounts)
            .unwrap();
        lifecycle
            .begin_slot(2, 0, hash_a, &mut accounts, &mut history)
            .unwrap();
        lifecycle
            .complete_slot(2, hash_b, 0, &mut accounts)
            .unwrap();
        lifecycle
            .begin_slot(5, 2, hash_b, &mut accounts, &mut history)
            .unwrap();
        let map_before = lifecycle.recent_blockhashes.clone();
        let order_before = lifecycle.recent_blockhash_order.clone();
        let height_before = lifecycle.hash_height;
        let history_before = lifecycle.slot_history_words.clone();
        let next_slot_before = lifecycle.slot_history_next_slot;
        let accounts_before = accounts.clone();

        assert!(matches!(
            lifecycle.complete_slot(5, hash_a, 0, &mut accounts),
            Err(LaunchSysvarError::DuplicatePohBlockhash { slot: 5 })
        ));
        assert_eq!(lifecycle.recent_blockhashes, map_before);
        assert_eq!(lifecycle.recent_blockhash_order, order_before);
        assert_eq!(lifecycle.hash_height, height_before);
        assert_eq!(lifecycle.slot_history_words, history_before);
        assert_eq!(lifecycle.slot_history_next_slot, next_slot_before);
        assert_eq!(accounts, accounts_before);

        lifecycle
            .complete_slot(5, recovery_hash, 0, &mut accounts)
            .unwrap();
        lifecycle
            .validate_frozen_checkpoint(&accounts, &history)
            .unwrap();
    }

    #[test]
    fn fixed_sysvar_write_updates_in_place_and_initializes_missing_account() {
        let existing_key = [0x91; 32];
        let missing_key = [0x92; 32];
        let mut accounts = MemoryAccountStore::new();
        accounts.insert(
            existing_key,
            AccountSnapshot {
                lamports: 99,
                owner: [0x33; 32],
                executable: true,
                rent_epoch: 77,
                data: vec![0xaa; 8].into(),
            },
        );
        let existing_data_pointer = accounts[&existing_key].data.as_ptr();

        write_fixed_sysvar_account(&mut accounts, existing_key, 8, |data| {
            data.copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        });
        assert_eq!(accounts[&existing_key].data.as_ptr(), existing_data_pointer);
        assert_eq!(
            accounts[&existing_key],
            sysvar_account(vec![1, 2, 3, 4, 5, 6, 7, 8])
        );

        write_fixed_sysvar_account(&mut accounts, missing_key, 4, |data| {
            data.copy_from_slice(&[9, 10, 11, 12]);
        });
        assert_eq!(accounts[&missing_key], sysvar_account(vec![9, 10, 11, 12]));
        assert_eq!(accounts.len(), 2);
    }

    #[test]
    fn fixed_bank_sysvar_buffers_are_reused_across_slots() {
        let genesis = mainnet_fixture();
        let mut accounts = launch_genesis_sysvar_accounts(&genesis)
            .unwrap()
            .into_iter()
            .collect::<MemoryAccountStore>();
        let mut history = LaunchStakeHistory::new();
        let mut lifecycle = LaunchBankSysvarState::from_genesis(&genesis).unwrap();
        let clock_data = accounts[&CLOCK_SYSVAR_ID].data.as_ptr();
        let fees_data = accounts[&FEES_SYSVAR_ID].data.as_ptr();
        let recent_data = accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data.as_ptr();

        lifecycle
            .begin_slot(0, 0, genesis.genesis_hash, &mut accounts, &mut history)
            .unwrap();
        lifecycle
            .complete_slot(0, numbered_hash(1), 0, &mut accounts)
            .unwrap();
        let slot_history_data = accounts[&SLOT_HISTORY_SYSVAR_ID].data.as_ptr();

        lifecycle
            .begin_slot(3, 0, numbered_hash(1), &mut accounts, &mut history)
            .unwrap();
        assert_eq!(accounts[&CLOCK_SYSVAR_ID].data.as_ptr(), clock_data);
        assert_eq!(accounts[&FEES_SYSVAR_ID].data.as_ptr(), fees_data);
        assert_eq!(
            accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data.as_ptr(),
            recent_data
        );
        lifecycle
            .complete_slot(3, numbered_hash(2), 0, &mut accounts)
            .unwrap();
        assert_eq!(
            accounts[&SLOT_HISTORY_SYSVAR_ID].data.as_ptr(),
            slot_history_data
        );
        lifecycle
            .validate_frozen_checkpoint(&accounts, &history)
            .unwrap();
    }

    #[test]
    fn slot_history_updates_existing_account_in_place_and_initializes_missing() {
        let genesis = mainnet_fixture();
        let mut existing_lifecycle = LaunchBankSysvarState::from_genesis(&genesis).unwrap();
        let mut existing_accounts = MemoryAccountStore::new();
        existing_accounts.insert(
            SLOT_HISTORY_SYSVAR_ID,
            AccountSnapshot {
                lamports: 99,
                owner: [0x33; 32],
                executable: true,
                rent_epoch: 77,
                data: slot_history_data(
                    &existing_lifecycle.slot_history_words,
                    existing_lifecycle.slot_history_next_slot,
                )
                .into(),
            },
        );
        let account_pointer =
            existing_accounts.get(&SLOT_HISTORY_SYSVAR_ID).unwrap() as *const AccountSnapshot;
        let data_pointer = existing_accounts[&SLOT_HISTORY_SYSVAR_ID].data.as_ptr();

        existing_lifecycle
            .add_slot_to_history(3, &mut existing_accounts)
            .unwrap();

        let existing = existing_accounts.get(&SLOT_HISTORY_SYSVAR_ID).unwrap();
        assert_eq!(existing as *const AccountSnapshot, account_pointer);
        assert_eq!(existing.data.as_ptr(), data_pointer);
        assert_eq!(existing.lamports, 99);
        assert_eq!(existing.owner, [0x33; 32]);
        assert!(existing.executable);
        assert_eq!(existing.rent_epoch, 77);
        assert_eq!(
            existing.data,
            slot_history_data(
                &existing_lifecycle.slot_history_words,
                existing_lifecycle.slot_history_next_slot,
            )
        );

        let mut missing_lifecycle = LaunchBankSysvarState::from_genesis(&genesis).unwrap();
        let mut missing_accounts = MemoryAccountStore::new();
        missing_lifecycle
            .add_slot_to_history(3, &mut missing_accounts)
            .unwrap();
        assert_eq!(
            missing_accounts[&SLOT_HISTORY_SYSVAR_ID],
            sysvar_account(slot_history_data(
                &missing_lifecycle.slot_history_words,
                missing_lifecycle.slot_history_next_slot,
            ))
        );
    }

    #[test]
    fn malformed_slot_history_direct_update_is_atomic() {
        let genesis = mainnet_fixture();
        let mut lifecycle = LaunchBankSysvarState::from_genesis(&genesis).unwrap();
        let words_before = lifecycle.slot_history_words.clone();
        let next_slot_before = lifecycle.slot_history_next_slot;
        let malformed = AccountSnapshot {
            lamports: 99,
            owner: [0x33; 32],
            executable: true,
            rent_epoch: 77,
            data: vec![0xa5; 17].into(),
        };
        let mut accounts = MemoryAccountStore::new();
        accounts.insert(SLOT_HISTORY_SYSVAR_ID, malformed.clone());

        assert!(matches!(
            lifecycle.add_slot_to_history(3, &mut accounts),
            Err(LaunchSysvarError::AccountStore(
                AccountStoreError::DataLengthMismatch {
                    pubkey: SLOT_HISTORY_SYSVAR_ID,
                    expected: SLOT_HISTORY_DATA_LEN,
                    found: 17,
                }
            ))
        ));
        assert_eq!(lifecycle.slot_history_words, words_before);
        assert_eq!(lifecycle.slot_history_next_slot, next_slot_before);
        assert_eq!(accounts[&SLOT_HISTORY_SYSVAR_ID], malformed);
    }

    #[test]
    fn malformed_slot_history_fails_before_freeze_state_changes() {
        let genesis = mainnet_fixture();
        let mut accounts = launch_genesis_sysvar_accounts(&genesis)
            .unwrap()
            .into_iter()
            .collect::<MemoryAccountStore>();
        let mut history = LaunchStakeHistory::new();
        let mut lifecycle = LaunchBankSysvarState::from_genesis(&genesis).unwrap();
        lifecycle
            .begin_slot(0, 0, genesis.genesis_hash, &mut accounts, &mut history)
            .unwrap();
        accounts.insert(SLOT_HISTORY_SYSVAR_ID, sysvar_account(vec![0]));
        let map_before = lifecycle.recent_blockhashes.clone();
        let order_before = lifecycle.recent_blockhash_order.clone();
        let words_before = lifecycle.slot_history_words.clone();
        let accounts_before = accounts.clone();

        assert!(matches!(
            lifecycle.complete_slot(0, numbered_hash(1), 0, &mut accounts),
            Err(LaunchSysvarError::AccountStore(
                AccountStoreError::DataLengthMismatch {
                    pubkey: SLOT_HISTORY_SYSVAR_ID,
                    expected: SLOT_HISTORY_DATA_LEN,
                    found: 1,
                }
            ))
        ));
        assert_eq!(lifecycle.hash_height, 0);
        assert_eq!(lifecycle.recent_blockhashes, map_before);
        assert_eq!(lifecycle.recent_blockhash_order, order_before);
        assert_eq!(lifecycle.slot_history_words, words_before);
        assert_eq!(accounts, accounts_before);
    }

    #[test]
    fn child_bank_lifecycle_updates_exact_non_bank_hash_sysvars() {
        let genesis = mainnet_fixture();
        let mut accounts = launch_genesis_sysvar_accounts(&genesis)
            .unwrap()
            .into_iter()
            .collect::<MemoryAccountStore>();
        let mut history = LaunchStakeHistory::new();
        let mut lifecycle = LaunchBankSysvarState::from_genesis(&genesis).unwrap();
        let slot_0_hash = [0x44; 32];
        let slot_1_hash = [0x46; 32];

        let genesis_update = lifecycle
            .begin_slot(0, 0, genesis.genesis_hash, &mut accounts, &mut history)
            .unwrap();
        assert!(genesis_update.written_accounts.is_empty());
        assert_eq!(
            lifecycle
                .complete_slot(0, slot_0_hash, 0, &mut accounts)
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([SLOT_HISTORY_SYSVAR_ID])
        );

        let slot_1_update = lifecycle
            .begin_slot(1, 0, slot_0_hash, &mut accounts, &mut history)
            .unwrap();
        assert_eq!(
            slot_1_update
                .written_accounts
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                CLOCK_SYSVAR_ID,
                FEES_SYSVAR_ID,
                RECENT_BLOCKHASHES_SYSVAR_ID,
            ])
        );
        assert!(slot_1_update.slot_hashes_unavailable);
        assert!(!accounts.contains_key(&SLOT_HASHES_SYSVAR_ID));
        assert_eq!(lifecycle.current_fee(), 5_000);
        assert_eq!(
            &accounts[&CLOCK_SYSVAR_ID].data,
            &[
                1, 0, 0, 0, 0, 0, 0, 0, // slot
                1, 0, 0, 0, 0, 0, 0, 0, // segment
                0, 0, 0, 0, 0, 0, 0, 0, // epoch
                1, 0, 0, 0, 0, 0, 0, 0, // leader schedule epoch
                44, 141, 111, 94, 0, 0, 0, 0, // nominal timestamp
            ]
        );
        assert_eq!(accounts[&FEES_SYSVAR_ID].data, 5_000_u64.to_le_bytes());
        let recent = &accounts[&RECENT_BLOCKHASHES_SYSVAR_ID].data;
        assert_eq!(&recent[..8], &2_u64.to_le_bytes());
        assert_eq!(&recent[8..40], &slot_0_hash);
        assert_eq!(&recent[40..48], &0_u64.to_le_bytes());
        assert_eq!(&recent[48..80], &genesis.genesis_hash);
        assert_eq!(&recent[80..88], &0_u64.to_le_bytes());
        assert_eq!(accounts[&SLOT_HISTORY_SYSVAR_ID].data.len(), 131_097);
        let slot_history_hash = sha2::Sha256::digest(&accounts[&SLOT_HISTORY_SYSVAR_ID].data)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            slot_history_hash,
            "d0c4d18d94130217ebb3d4fb00dd59aac747a3ed9b058a97565a29eb9484c140"
        );

        let stake_pubkey = [0x47; 32];
        let stake_state = LaunchStakeState::Stake(
            LaunchStakeMeta {
                rent_exempt_reserve: 1,
                authorized: LaunchStakeAuthorized {
                    staker: [0x48; 32],
                    withdrawer: [0x49; 32],
                },
                lockup: LaunchStakeLockup::default(),
            },
            LaunchStake {
                delegation: LaunchDelegation {
                    voter_pubkey: [0x4a; 32],
                    stake: 123,
                    activation_epoch: u64::MAX,
                    deactivation_epoch: u64::MAX,
                    warmup_cooldown_rate: 0.25,
                },
                credits_observed: 0,
            },
        );
        let encoded = wincode::serialize(&stake_state).unwrap();
        let mut stake_data = vec![0; crate::LAUNCH_STAKE_ACCOUNT_DATA_LEN];
        stake_data[..encoded.len()].copy_from_slice(&encoded);
        accounts.insert(
            stake_pubkey,
            AccountSnapshot {
                lamports: 1_000,
                owner: STAKE_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
                data: stake_data.into(),
            },
        );
        lifecycle
            .complete_slot(1, slot_1_hash, 0, &mut accounts)
            .unwrap();
        let frozen_slot_history = &accounts[&SLOT_HISTORY_SYSVAR_ID].data;
        assert_eq!(&frozen_slot_history[9..17], &3_u64.to_le_bytes());
        assert_eq!(
            &frozen_slot_history[SLOT_HISTORY_NEXT_SLOT_OFFSET..],
            &2_u64.to_le_bytes()
        );
        let epoch_1_update = lifecycle
            .begin_slot(432_000, 1, slot_1_hash, &mut accounts, &mut history)
            .unwrap();
        assert_eq!(epoch_1_update.epoch_transition, Some((0, 1)));
        assert_eq!(history[&0].effective, 123);
        assert_eq!(history[&0].activating, 0);
        assert_eq!(history[&0].deactivating, 0);
        assert_eq!(accounts[&REWARDS_SYSVAR_ID].data, vec![0; 16]);
        let stake_history = &accounts[&STAKE_HISTORY_SYSVAR_ID].data;
        assert_eq!(&stake_history[..8], &1_u64.to_le_bytes());
        assert_eq!(&stake_history[8..16], &0_u64.to_le_bytes());
        assert_eq!(&stake_history[16..24], &123_u64.to_le_bytes());
        assert_eq!(
            &accounts[&CLOCK_SYSVAR_ID].data,
            &[
                128, 151, 6, 0, 0, 0, 0, 0, // slot 432000
                166, 1, 0, 0, 0, 0, 0, 0, // segment 422
                1, 0, 0, 0, 0, 0, 0, 0, // epoch
                2, 0, 0, 0, 0, 0, 0, 0, // leader schedule epoch
                44, 48, 114, 94, 0, 0, 0, 0, // nominal timestamp
            ]
        );
    }
}
