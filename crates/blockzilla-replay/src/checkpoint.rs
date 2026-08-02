//! Deterministic frozen-Bank checkpoints for the launch replay POC.
//!
//! This codec deliberately does not serialize Rust structs, hash-table
//! buckets, pointers, `usize`, or enum layouts. Every integer is explicitly
//! little-endian, maps and account records are key-sorted, all lengths are
//! bounded before allocation, and a SHA-256 checksum covers the versioned
//! header plus complete payload.

// The codec remains intentionally crate-private until a path-based resume
// runner can own generation validation and durable publication.
#![cfg_attr(not(test), allow(dead_code))]

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, VecDeque},
};

use blockzilla_format::{
    WincodeArchiveV2GenesisEpochSchedule, WincodeArchiveV2GenesisFeeParams,
    WincodeArchiveV2GenesisInflationParams, WincodeArchiveV2GenesisPohParams,
    WincodeArchiveV2GenesisRentParams,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AccountSnapshot, BPF_LOADER_PROGRAM_ID, CONFIG_PROGRAM_ID, CompactGenesisAccount,
    CompactGenesisBuiltin, CompactGenesisProbe, CompactGenesisSource, LaunchBankSysvarState,
    LaunchDerivedTransactionFailure, LaunchReplay, LaunchReplayFailureLocation,
    LaunchReplayOutcome, LaunchStakeHistory, LaunchStakeHistoryEntry,
    LaunchTransactionFailureReason, MemoryAccountStore, STAKE_PROGRAM_ID, SYSTEM_PROGRAM_ID,
    VOTE_PROGRAM_ID,
    launch_replay::{
        FIRST_AUTHORITATIVE_OUTCOME_SLOT, prune_legacy_hydrated_balance_only_system_accounts,
    },
    launch_stake::STAKE_AUTHORIZE_LOCKUP_REMOVAL_SLOT,
    launch_sysvar::{
        CLOCK_SYSVAR_ID, EPOCH_SCHEDULE_SYSVAR_ID, FEES_SYSVAR_ID, LaunchFeeGovernor,
        LaunchRecentBlockhash, RECENT_BLOCKHASHES_SYSVAR_ID, RENT_SYSVAR_ID, SLOT_HISTORY_WORDS,
        STAKE_HISTORY_SYSVAR_ID,
    },
    launch_vote::{INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT, LaunchVoteStateCache},
};

const CHECKPOINT_MAGIC: [u8; 8] = *b"BZLRCP01";
const LEGACY_CHECKPOINT_VERSION: u16 = 1;
const CHECKPOINT_VERSION: u16 = 2;
const CHECKPOINT_FLAGS: u16 = 0;
const HEADER_LEN: usize = 8 + 2 + 2 + 8;
const CHECKSUM_LEN: usize = 32;
const CHECKSUM_DOMAIN: &[u8] = b"blockzilla/launch-frozen-checkpoint/v1\0";
// This is a state-transition compatibility revision, not a marketing label.
// Bump it whenever a runtime change can alter replayed account bytes, counters,
// Bank sysvar caches, or cursor semantics for the same Compact input.
const LEGACY_RUNTIME_PROFILE_V1: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-and-trusted-vote-poc/checkpoint-runtime-revision-2026-07-29.1";
const PREVIOUS_RUNTIME_PROFILE_V2: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-and-trusted-vote-plus-v1.1.14-stable-bpf-loader-and-program-execution-poc/checkpoint-runtime-revision-2026-07-29.3";
const PREVIOUS_RUNTIME_PROFILE_V3: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-and-trusted-vote-plus-v1.1.14-stable-bpf-loader-program-execution-and-nonce-withdraw-poc/checkpoint-runtime-revision-2026-07-29.4";
const PREVIOUS_RUNTIME_PROFILE_V4: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-plus-v1.3.3-epoch63-pda-and-cpi-poc/checkpoint-runtime-revision-2026-07-29.5";
const PREVIOUS_RUNTIME_PROFILE_V5: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-plus-trusted-compact-failed-outcome-skip-poc/checkpoint-runtime-revision-2026-07-29.6";
const PREVIOUS_RUNTIME_PROFILE_V6: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-trusted-compact-failed-outcome-skip-plus-vote-init-node-signature-poc/checkpoint-runtime-revision-2026-07-29.7";
const PREVIOUS_RUNTIME_PROFILE_V7: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-trusted-compact-failed-outcome-skip-vote-init-node-signature-plus-v1.1.6-stake-authorize-lockup-removal-poc/checkpoint-runtime-revision-2026-07-29.8";
const PREVIOUS_RUNTIME_PROFILE_V8: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-trusted-compact-outcomes-plus-writable-post-balance-projection-and-fee-only-transfer-recovery-poc/checkpoint-runtime-revision-2026-07-29.9";
const PREVIOUS_RUNTIME_PROFILE_V9: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-trusted-compact-outcomes-plus-writable-post-balance-projection-and-fee-only-transfer-recovery-plus-v1.2.32-stable-epoch40-system-poc/checkpoint-runtime-revision-2026-07-29.10";
const PREVIOUS_RUNTIME_PROFILE_V10: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-trusted-compact-outcomes-plus-writable-post-balance-projection-and-structural-fee-only-system-noop-recoveries-plus-v1.2.32-stable-epoch40-system-poc/checkpoint-runtime-revision-2026-07-29.11";
const PREVIOUS_RUNTIME_PROFILE_V11: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-trusted-compact-outcomes-plus-writable-post-balance-projection-and-structural-fee-only-system-noop-recoveries-plus-v1.2.32-stable-epoch40-system-and-vote-update-commission-poc/checkpoint-runtime-revision-2026-07-29.12";
const PREVIOUS_RUNTIME_PROFILE_V12: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-trusted-compact-outcomes-plus-writable-post-balance-projection-and-structural-system-recoveries-plus-v1.2.32-stable-epoch40-system-and-vote-update-commission-plus-historical-loader-balance-suffix-and-canonical-prebalance-system-transfer-poc/checkpoint-runtime-revision-2026-07-29.13";
const PREVIOUS_RUNTIME_PROFILE_V13: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-trusted-compact-outcomes-plus-writable-post-balance-projection-and-structural-system-recoveries-plus-v1.2.32-stable-epoch40-system-and-vote-update-commission-and-vote-switch-plus-historical-loader-balance-suffix-and-canonical-prebalance-system-transfer-poc/checkpoint-runtime-revision-2026-07-29.14";
const PREVIOUS_RUNTIME_PROFILE_V14: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-and-stake-merge-trusted-compact-outcomes-plus-writable-post-balance-projection-and-structural-system-recoveries-plus-v1.2.32-stable-epoch40-system-and-vote-update-commission-and-vote-switch-plus-historical-loader-balance-suffix-and-canonical-prebalance-system-transfer-poc/checkpoint-runtime-revision-2026-07-30.15";
const PREVIOUS_RUNTIME_PROFILE_V15: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-and-stake-merge-trusted-compact-outcomes-plus-transient-covered-prebalance-system-accounts-and-structural-system-recoveries-plus-v1.2.32-stable-epoch40-system-and-vote-update-commission-and-vote-switch-plus-historical-loader-balance-suffix-poc/checkpoint-runtime-revision-2026-07-30.16";
const RUNTIME_PROFILE: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-immutable-account-metadata-and-stake-merge-trusted-compact-outcomes-plus-transient-covered-prebalance-system-accounts-and-structural-system-recoveries-plus-v1.2.32-stable-epoch40-system-and-vote-update-commission-and-vote-switch-plus-historical-loader-balance-suffix-poc/checkpoint-runtime-revision-2026-07-30.17";
const BPF_LOADER_STABLE_ACTIVATION_EPOCH: u64 = 34;
// The audited mainnet-launch Compact corpus first reaches WithdrawNonceAccount
// at this slot. A .3 checkpoint frozen before it is transition-equivalent to
// .4 and can be upgraded without replaying the preceding epochs.
const FIRST_WITHDRAW_NONCE_SLOT: u64 = 21_365_522;
// The audited Compact corpus first executes the epoch-63 PDA/CPI syscall
// environment at this slot. A .4 checkpoint frozen before it has exactly the
// same Bank state as .5 and can be migrated without replaying earlier epochs.
const FIRST_PDA_OR_CPI_SLOT: u64 = 29_188_719;
// Stable activates Solana v1.2.32's replacement System processor on entry to
// epoch 40. With the launch mainnet schedule this is slot 17,280,000.
const FIRST_NEW_SYSTEM_PROCESSOR_SLOT: u64 = 17_280_000;
// Canonical RPC and Compact both mark this prefunded CreateAccount retry as
// successful even though the destination remains unchanged and only the fee
// payer loses the fee. A .10 checkpoint frozen before the exact row has the
// same state as .11 and can be migrated safely.
const FIRST_PREFUNDED_CREATE_STATUS_ANOMALY_SLOT: u64 = 18_916_586;
// First audited Compact row carrying VoteInstruction::UpdateCommission.
// Earlier .11 checkpoints are state-equivalent because no prior decoded
// instruction can select this newly implemented variant.
const FIRST_VOTE_UPDATE_COMMISSION_SLOT: u64 = 19_392_740;
// Launch-era status metadata first carries two runtime loader-chain balances
// after the static message-account prefix at this exact audited row. Earlier
// .12 checkpoints are state-equivalent and can migrate without replay.
const FIRST_RUNTIME_LOADER_BALANCE_SUFFIX_SLOT: u64 = 24_005_334;
// First audited successful VoteSwitch row after the Stable switch-vote
// boundary. A .13 checkpoint before it has not decoded discriminant 6 and is
// transition-equivalent to .14.
const FIRST_VOTE_SWITCH_SLOT: u64 = 26_752_197;
// First audited successful StakeInstruction::Merge row. A .14 checkpoint
// frozen before it has not decoded discriminant 7 and is transition-equivalent
// to .15.
const FIRST_STAKE_MERGE_SLOT: u64 = 28_621_186;
pub(crate) const MAX_CHECKPOINT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_DECODE_ALLOC_BYTES: u64 = 256 * 1024 * 1024;
const MAX_RUNTIME_ACCOUNTS: u64 = 1_000_000;
const MAX_PUBKEY_SET_ITEMS: u64 = 1_000_000;
const MAX_GENESIS_ACCOUNTS: u64 = 16_384;
const MAX_REWARD_POOLS: u64 = 4_096;
const MAX_GENESIS_BUILTINS: u64 = 64;
const MAX_ACCOUNT_DATA_BYTES: u64 = 10 * 1024 * 1024;
const MAX_STRING_BYTES: u64 = 16 * 1024;
const MAX_BUILTIN_NAME_BYTES: u64 = 256;
const MAX_RECENT_BLOCKHASHES: u64 = 301;
const MAX_STAKE_HISTORY_ENTRIES: u64 = 4_096;
const SMALL_INITIAL_CAPACITY: usize = 1_024;

/// Immutable replay and Compact-generation identity expected on resume.
///
/// The generation fields identify the Compact generation whose cursor was
/// checkpointed. An exhausted epoch therefore remains explicitly bound with
/// `next_row == generation_block_count`. This private POC deliberately cannot
/// attach a later generation; that requires a future path-based resume runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaunchCheckpointDescriptor {
    pub(crate) runtime_profile_sha256: [u8; 32],
    pub(crate) generation_digest: [u8; 32],
    pub(crate) registry_sha256: [u8; 32],
}

/// Exact restart position in one descriptor-bound Compact generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompactCheckpointCursor {
    /// Last completely frozen Bank. This is redundant with runtime state and
    /// is checked during capture and restore.
    pub(crate) last_slot: u64,
    /// Epoch-local Compact index row to decode next.
    pub(crate) next_row: u64,
    /// Total rows in the target generation's validated index.
    pub(crate) generation_block_count: u64,
    /// Exact slot expected at `next_row`. `None` is legal only when
    /// `next_row == generation_block_count`.
    pub(crate) next_slot: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordedCompactCheckpoint {
    pub(crate) generation_digest: [u8; 32],
    pub(crate) registry_sha256: [u8; 32],
    pub(crate) cursor: CompactCheckpointCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrozenCheckpointMetadata {
    pub(crate) descriptor: LaunchCheckpointDescriptor,
    pub(crate) cursor: CompactCheckpointCursor,
    pub(crate) account_state_sha256: [u8; 32],
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum LaunchCheckpointError {
    #[error("checkpoint capture requires an enabled, fully completed Bank")]
    BankNotFrozen,
    #[error("checkpoint capture found an unfinished rolled-back transaction")]
    PendingTransaction,
    #[error("checkpoint capture cannot retain in-memory instruction mutations")]
    RetainedInstructionMutations,
    #[error("checkpoint capture requires a successfully replayed Compact index row")]
    MissingCompactCursor,
    #[error("checkpoint cursor last slot is {found}, frozen Bank is {expected}")]
    CursorLastSlotMismatch { expected: u64, found: u64 },
    #[error("invalid checkpoint cursor: {0}")]
    InvalidCursor(&'static str),
    #[error("checkpoint exceeds the {MAX_CHECKPOINT_BYTES}-byte POC bound")]
    CheckpointTooLarge,
    #[error("checkpoint is truncated")]
    Truncated,
    #[error("checkpoint magic is invalid")]
    InvalidMagic,
    #[error("checkpoint version {found} is unsupported")]
    UnsupportedVersion { found: u16 },
    #[error("checkpoint flags {found:#x} are unsupported")]
    UnsupportedFlags { found: u16 },
    #[error("checkpoint payload length is invalid")]
    InvalidPayloadLength,
    #[error("checkpoint checksum does not match")]
    ChecksumMismatch,
    #[error("checkpoint descriptor does not match the requested replay input")]
    DescriptorMismatch,
    #[error("legacy checkpoint is not safe to migrate: {0}")]
    UnsafeLegacyMigration(&'static str),
    #[error("previous v2 checkpoint is not safe to migrate: {0}")]
    UnsafePreviousRuntimeMigration(&'static str),
    #[error("checkpoint cannot attach completed Compact generation: {0}")]
    InvalidCompletedGeneration(&'static str),
    #[error("checkpoint field {field} exceeds its decode bound")]
    DecodeBound { field: &'static str },
    #[error("checkpoint contains invalid {field}: {reason}")]
    InvalidField {
        field: &'static str,
        reason: &'static str,
    },
    #[error("checkpoint contains trailing payload bytes")]
    TrailingPayload,
    #[error("checkpoint runtime invariant failed: {0}")]
    RuntimeInvariant(String),
}

impl LaunchReplay {
    /// Encode one portable checkpoint from the exact Compact position sealed
    /// by the most recently completed streamed row. Callers cannot supply the
    /// runtime profile, generation identity, or cursor.
    pub(crate) fn encode_frozen_checkpoint(&self) -> Result<Vec<u8>, LaunchCheckpointError> {
        if !self.bank_lifecycle_enabled
            || !self.bank_sysvars.began_first_slot
            || !self.bank_sysvars.current_slot_completed
        {
            return Err(LaunchCheckpointError::BankNotFrozen);
        }
        if self.vote_state_cache.has_pending_materializations() {
            return Err(LaunchCheckpointError::RuntimeInvariant(
                "lazy Vote state must be materialized before checkpoint encoding".to_owned(),
            ));
        }
        let recorded = self
            .compact_checkpoint
            .ok_or(LaunchCheckpointError::MissingCompactCursor)?;
        let descriptor = LaunchCheckpointDescriptor {
            runtime_profile_sha256: runtime_profile_sha256(),
            generation_digest: recorded.generation_digest,
            registry_sha256: recorded.registry_sha256,
        };
        let cursor = recorded.cursor;
        validate_live_replay(self, cursor)?;

        let mut checkpoint = Encoder::checkpoint();
        encode_descriptor(&mut checkpoint, descriptor);
        encode_cursor(&mut checkpoint, cursor);
        encode_replay(&mut checkpoint, self)?;
        checkpoint.finish_checkpoint()
    }

    /// Restore a frozen replay session and install a one-shot guard for the
    /// exact next Compact row/slot. The next internal Compact event must carry
    /// the same reader-validated generation binding and index evidence before
    /// any Bank mutation occurs.
    pub(crate) fn restore_frozen_checkpoint(
        bytes: &[u8],
        retain_instruction_mutations: bool,
    ) -> Result<(Self, FrozenCheckpointMetadata), LaunchCheckpointError> {
        let (version, payload) = validated_payload(bytes)?;
        let mut decoder = Decoder::new(payload);
        let descriptor = decode_descriptor(&mut decoder)?;
        let migrating_previous_v2 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v2_sha256();
        let migrating_previous_v3 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v3_sha256();
        let migrating_previous_v4 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v4_sha256();
        let migrating_previous_v5 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v5_sha256();
        let migrating_previous_v6 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v6_sha256();
        let migrating_previous_v7 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v7_sha256();
        let migrating_previous_v8 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v8_sha256();
        let migrating_previous_v9 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v9_sha256();
        let migrating_previous_v10 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v10_sha256();
        let migrating_previous_v11 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v11_sha256();
        let migrating_previous_v12 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v12_sha256();
        let migrating_previous_v13 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v13_sha256();
        let migrating_previous_v14 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v14_sha256();
        let migrating_previous_v15 = version == CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == previous_runtime_profile_v15_sha256();
        let migrating_legacy_v1 = version == LEGACY_CHECKPOINT_VERSION
            && descriptor.runtime_profile_sha256 == legacy_runtime_profile_v1_sha256();
        let accepted_runtime_profile = match version {
            LEGACY_CHECKPOINT_VERSION => migrating_legacy_v1,
            CHECKPOINT_VERSION => {
                descriptor.runtime_profile_sha256 == runtime_profile_sha256()
                    || migrating_previous_v2
                    || migrating_previous_v3
                    || migrating_previous_v4
                    || migrating_previous_v5
                    || migrating_previous_v6
                    || migrating_previous_v7
                    || migrating_previous_v8
                    || migrating_previous_v9
                    || migrating_previous_v10
                    || migrating_previous_v11
                    || migrating_previous_v12
                    || migrating_previous_v13
                    || migrating_previous_v14
                    || migrating_previous_v15
            }
            _ => unreachable!("validated_payload rejects unsupported versions"),
        };
        if !accepted_runtime_profile {
            return Err(LaunchCheckpointError::DescriptorMismatch);
        }
        let cursor = decode_cursor(&mut decoder)?;
        validate_cursor(cursor)?;
        if migrating_previous_v2 && cursor.last_slot >= FIRST_WITHDRAW_NONCE_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first nonce withdrawal",
            ));
        }
        if migrating_previous_v3 && cursor.last_slot >= FIRST_PDA_OR_CPI_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first PDA/CPI syscall",
            ));
        }
        if migrating_previous_v4 && cursor.last_slot >= FIRST_AUTHORITATIVE_OUTCOME_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first Compact transaction with an authoritative outcome",
            ));
        }
        let migrating_any_previous_v2 = migrating_previous_v2
            || migrating_previous_v3
            || migrating_previous_v4
            || migrating_previous_v5;
        if migrating_any_previous_v2 && cursor.last_slot >= INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches Vote InitializeAccount node-signature activation",
            ));
        }
        if migrating_legacy_v1 && cursor.last_slot >= INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT {
            return Err(LaunchCheckpointError::UnsafeLegacyMigration(
                "checkpoint reaches Vote InitializeAccount node-signature activation",
            ));
        }
        if migrating_previous_v6 && cursor.last_slot >= STAKE_AUTHORIZE_LOCKUP_REMOVAL_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches Stake Authorize lockup-removal activation",
            ));
        }
        if (migrating_previous_v6 || migrating_previous_v7)
            && cursor.last_slot >= FIRST_AUTHORITATIVE_OUTCOME_SLOT
        {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first Compact writable post-balance projection",
            ));
        }
        if migrating_previous_v8 && cursor.last_slot >= FIRST_NEW_SYSTEM_PROCESSOR_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches Stable epoch-40 System processor activation",
            ));
        }
        if migrating_previous_v9 && cursor.last_slot >= FIRST_PREFUNDED_CREATE_STATUS_ANOMALY_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first prefunded CreateAccount status anomaly",
            ));
        }
        if migrating_previous_v10 && cursor.last_slot >= FIRST_VOTE_UPDATE_COMMISSION_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first Vote UpdateCommission instruction",
            ));
        }
        if migrating_previous_v11 && cursor.last_slot >= FIRST_RUNTIME_LOADER_BALANCE_SUFFIX_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first historical runtime loader balance suffix",
            ));
        }
        if migrating_previous_v12 && cursor.last_slot >= FIRST_VOTE_SWITCH_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first VoteSwitch instruction",
            ));
        }
        if migrating_previous_v13 && cursor.last_slot >= FIRST_STAKE_MERGE_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first Stake Merge instruction",
            ));
        }
        let migrating_pre_immutable_cpi_runtime = migrating_legacy_v1
            || migrating_previous_v2
            || migrating_previous_v3
            || migrating_previous_v4
            || migrating_previous_v5
            || migrating_previous_v6
            || migrating_previous_v7
            || migrating_previous_v8
            || migrating_previous_v9
            || migrating_previous_v10
            || migrating_previous_v11
            || migrating_previous_v12
            || migrating_previous_v13
            || migrating_previous_v14
            || migrating_previous_v15;
        if migrating_pre_immutable_cpi_runtime && cursor.last_slot >= FIRST_PDA_OR_CPI_SLOT {
            return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first CPI immutable-account-metadata boundary",
            ));
        }
        let (mut replay, mut account_state_sha256) =
            decode_replay(&mut decoder, retain_instruction_mutations, version)?;
        if !decoder.is_finished() {
            return Err(LaunchCheckpointError::TrailingPayload);
        }
        if migrating_previous_v14 {
            if cursor.last_slot < FIRST_AUTHORITATIVE_OUTCOME_SLOT {
                return Err(LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                    "checkpoint predates the first authoritative Compact balance metadata",
                ));
            }
            prune_legacy_hydrated_balance_only_system_accounts(&mut replay.outcome);
            account_state_sha256 = replay.outcome.account_state.canonical_hash();
        }
        if replay.outcome.last_slot != Some(cursor.last_slot)
            || replay.bank_sysvars.current_slot != cursor.last_slot
        {
            return Err(LaunchCheckpointError::CursorLastSlotMismatch {
                expected: replay.bank_sysvars.current_slot,
                found: cursor.last_slot,
            });
        }
        if version == LEGACY_CHECKPOINT_VERSION {
            validate_legacy_v1_migration(&replay, cursor)?;
        }
        replay.compact_checkpoint = Some(RecordedCompactCheckpoint {
            generation_digest: descriptor.generation_digest,
            registry_sha256: descriptor.registry_sha256,
            cursor,
        });
        replay.pending_resume_descriptor = Some(descriptor);
        replay.pending_resume_cursor = Some(cursor);
        Ok((
            replay,
            FrozenCheckpointMetadata {
                descriptor,
                cursor,
                account_state_sha256,
            },
        ))
    }

    /// Attach a restored, exhausted cursor to the exact completed Compact
    /// generation that produced it.
    ///
    /// Restore deliberately installs a one-shot same-generation row guard. A
    /// successor generation may not be consumed until a path-owning runner has
    /// reopened and validated this source generation, then called this method.
    pub(crate) fn attach_completed_checkpoint_generation(
        &mut self,
        context: &crate::CompactGenerationContext,
    ) -> Result<(), LaunchCheckpointError> {
        let descriptor = self.pending_resume_descriptor.ok_or(
            LaunchCheckpointError::InvalidCompletedGeneration("restored descriptor is absent"),
        )?;
        let cursor =
            self.pending_resume_cursor
                .ok_or(LaunchCheckpointError::InvalidCompletedGeneration(
                    "restored cursor is absent",
                ))?;
        if !context.complete {
            return Err(LaunchCheckpointError::InvalidCompletedGeneration(
                "source generation is not sealed",
            ));
        }
        if descriptor.generation_digest != context.binding.generation_digest
            || descriptor.registry_sha256 != context.binding.registry_sha256
        {
            return Err(LaunchCheckpointError::InvalidCompletedGeneration(
                "source generation binding differs from the checkpoint descriptor",
            ));
        }
        if cursor.generation_block_count != context.block_count {
            return Err(LaunchCheckpointError::InvalidCompletedGeneration(
                "source generation block count differs from the checkpoint cursor",
            ));
        }
        if cursor.next_row != cursor.generation_block_count || cursor.next_slot.is_some() {
            return Err(LaunchCheckpointError::InvalidCompletedGeneration(
                "checkpoint cursor has not exhausted its source generation",
            ));
        }
        if context.last_slot != Some(cursor.last_slot) {
            return Err(LaunchCheckpointError::InvalidCompletedGeneration(
                "source generation final index slot differs from the frozen Bank",
            ));
        }
        if context.epoch != self.outcome.epoch || context.slots_per_epoch != self.slots_per_epoch {
            return Err(LaunchCheckpointError::InvalidCompletedGeneration(
                "source generation epoch schedule differs from the frozen runtime",
            ));
        }
        let genesis_matches = match (context.epoch, context.genesis.as_ref()) {
            (0, Some(genesis)) => {
                genesis.source == CompactGenesisSource::ExactGenesisBin
                    && crate::launch_replay::same_exact_genesis(genesis, &self.bank_sysvars.genesis)
            }
            (0, None) => false,
            (_, None) => true,
            (_, Some(_)) => false,
        };
        if !genesis_matches {
            return Err(LaunchCheckpointError::InvalidCompletedGeneration(
                "source generation genesis identity differs from the frozen runtime",
            ));
        }
        if self.compact_checkpoint
            != Some(RecordedCompactCheckpoint {
                generation_digest: descriptor.generation_digest,
                registry_sha256: descriptor.registry_sha256,
                cursor,
            })
        {
            return Err(LaunchCheckpointError::InvalidCompletedGeneration(
                "restored internal generation record is inconsistent",
            ));
        }
        self.pending_resume_descriptor = None;
        self.pending_resume_cursor = None;
        Ok(())
    }
}

fn validate_legacy_v1_migration(
    replay: &LaunchReplay,
    cursor: CompactCheckpointCursor,
) -> Result<(), LaunchCheckpointError> {
    if cursor.next_row != cursor.generation_block_count || cursor.next_slot.is_some() {
        return Err(LaunchCheckpointError::UnsafeLegacyMigration(
            "source generation is not exhausted",
        ));
    }
    if replay.outcome.epoch >= BPF_LOADER_STABLE_ACTIVATION_EPOCH {
        return Err(LaunchCheckpointError::UnsafeLegacyMigration(
            "frozen Bank is at or after BPF-loader activation",
        ));
    }
    if replay
        .outcome
        .account_state
        .contains_key(&BPF_LOADER_PROGRAM_ID)
    {
        return Err(LaunchCheckpointError::UnsafeLegacyMigration(
            "frozen Bank already contains the BPF-loader builtin",
        ));
    }
    debug_assert_eq!(replay.outcome.bpf_loader_mutations, 0);
    debug_assert!(replay.bpf_program_cache.is_empty());
    Ok(())
}

fn validate_live_replay(
    replay: &LaunchReplay,
    cursor: CompactCheckpointCursor,
) -> Result<(), LaunchCheckpointError> {
    if !replay.bank_lifecycle_enabled
        || !replay.bank_sysvars.began_first_slot
        || !replay.bank_sysvars.current_slot_completed
    {
        return Err(LaunchCheckpointError::BankNotFrozen);
    }
    if replay.rolled_back_transaction.is_some() {
        return Err(LaunchCheckpointError::PendingTransaction);
    }
    if !replay.outcome.instruction_mutations.is_empty() {
        return Err(LaunchCheckpointError::RetainedInstructionMutations);
    }
    let last_slot = replay
        .outcome
        .last_slot
        .ok_or(LaunchCheckpointError::BankNotFrozen)?;
    if cursor.last_slot != last_slot || cursor.last_slot != replay.bank_sysvars.current_slot {
        return Err(LaunchCheckpointError::CursorLastSlotMismatch {
            expected: replay.bank_sysvars.current_slot,
            found: cursor.last_slot,
        });
    }
    validate_cursor(cursor)?;
    if replay.outcome.slots_processed != replay.bank_sysvars.hash_height {
        return Err(LaunchCheckpointError::RuntimeInvariant(
            "processed-slot count does not match Bank hash height".to_owned(),
        ));
    }
    if replay.outcome.epoch != replay.bank_sysvars.current_epoch {
        return Err(LaunchCheckpointError::RuntimeInvariant(
            "reported epoch does not match frozen Bank epoch".to_owned(),
        ));
    }
    validate_outcome_semantics(&replay.outcome, replay.slots_per_epoch)?;
    replay
        .bank_sysvars
        .validate_frozen_checkpoint(&replay.outcome.account_state, &replay.stake_history)
        .map_err(|error| LaunchCheckpointError::RuntimeInvariant(error.to_string()))?;
    Ok(())
}

fn validate_cursor(cursor: CompactCheckpointCursor) -> Result<(), LaunchCheckpointError> {
    if cursor.next_row > cursor.generation_block_count {
        return Err(LaunchCheckpointError::InvalidCursor(
            "next row exceeds the target generation block count",
        ));
    }
    match cursor.next_slot {
        Some(next_slot) => {
            if cursor.next_row >= cursor.generation_block_count {
                return Err(LaunchCheckpointError::InvalidCursor(
                    "next slot exists at or after the generation end",
                ));
            }
            if next_slot <= cursor.last_slot {
                return Err(LaunchCheckpointError::InvalidCursor(
                    "next slot does not follow the frozen slot",
                ));
            }
        }
        None if cursor.next_row != cursor.generation_block_count => {
            return Err(LaunchCheckpointError::InvalidCursor(
                "missing next slot before the generation end",
            ));
        }
        None => {}
    }
    Ok(())
}

fn encode_descriptor(encoder: &mut Encoder, value: LaunchCheckpointDescriptor) {
    encoder.bytes(&value.runtime_profile_sha256);
    encoder.bytes(&value.generation_digest);
    encoder.bytes(&value.registry_sha256);
}

fn decode_descriptor(
    decoder: &mut Decoder<'_>,
) -> Result<LaunchCheckpointDescriptor, LaunchCheckpointError> {
    Ok(LaunchCheckpointDescriptor {
        runtime_profile_sha256: decoder.array()?,
        generation_digest: decoder.array()?,
        registry_sha256: decoder.array()?,
    })
}

fn encode_cursor(encoder: &mut Encoder, value: CompactCheckpointCursor) {
    encoder.u64(value.last_slot);
    encoder.u64(value.next_row);
    encoder.u64(value.generation_block_count);
    encoder.option_u64(value.next_slot);
}

fn decode_cursor(
    decoder: &mut Decoder<'_>,
) -> Result<CompactCheckpointCursor, LaunchCheckpointError> {
    Ok(CompactCheckpointCursor {
        last_slot: decoder.u64()?,
        next_row: decoder.u64()?,
        generation_block_count: decoder.u64()?,
        next_slot: decoder.option_u64("cursor next slot")?,
    })
}

fn encode_replay(
    encoder: &mut Encoder,
    replay: &LaunchReplay,
) -> Result<(), LaunchCheckpointError> {
    encoder.bytes(&replay.vote_program);
    encoder.bytes(&replay.config_program);
    encoder.bytes(&replay.system_program);
    encoder.bytes(&replay.stake_program);
    encoder.i64(replay.genesis_creation_time);
    encoder.u128(replay.ns_per_slot);
    encoder.u64(replay.slots_per_epoch);
    encode_stake_history(encoder, &replay.stake_history)?;
    encode_bank_state(encoder, &replay.bank_sysvars)?;
    encoder.boolean(replay.bank_lifecycle_enabled);
    encode_outcome(encoder, &replay.outcome)?;
    Ok(())
}

fn decode_replay(
    decoder: &mut Decoder<'_>,
    retain_instruction_mutations: bool,
    checkpoint_version: u16,
) -> Result<(LaunchReplay, [u8; 32]), LaunchCheckpointError> {
    let vote_program = decoder.array()?;
    let config_program = decoder.array()?;
    let system_program = decoder.array()?;
    let stake_program = decoder.array()?;
    if vote_program != VOTE_PROGRAM_ID
        || config_program != CONFIG_PROGRAM_ID
        || system_program != SYSTEM_PROGRAM_ID
        || stake_program != STAKE_PROGRAM_ID
    {
        return Err(LaunchCheckpointError::InvalidField {
            field: "runtime program ids",
            reason: "program identity does not match the launch profile",
        });
    }
    let genesis_creation_time = decoder.i64()?;
    let ns_per_slot = decoder.u128()?;
    let slots_per_epoch = decoder.u64()?;
    let stake_history = decode_stake_history(decoder)?;
    let bank_sysvars = decode_bank_state(decoder)?;
    validate_checkpoint_genesis(decoder, &bank_sysvars.genesis)?;
    let bank_lifecycle_enabled = decoder.boolean("Bank lifecycle flag")?;
    let (outcome, account_state_sha256) = decode_outcome(decoder, checkpoint_version)?;

    if !bank_lifecycle_enabled {
        return Err(LaunchCheckpointError::BankNotFrozen);
    }
    if bank_sysvars.genesis.source != CompactGenesisSource::ExactGenesisBin {
        return Err(LaunchCheckpointError::InvalidField {
            field: "genesis source",
            reason: "launch replay requires exact genesis.bin",
        });
    }
    if genesis_creation_time != bank_sysvars.genesis.creation_time_unix
        || slots_per_epoch != bank_sysvars.genesis.epoch_schedule.slots_per_epoch
    {
        return Err(LaunchCheckpointError::InvalidField {
            field: "genesis-derived runtime configuration",
            reason: "replay and Bank configuration disagree",
        });
    }
    let ns_per_tick = u128::from(bank_sysvars.genesis.poh_params.tick_duration_secs)
        .checked_mul(1_000_000_000)
        .and_then(|value| {
            value.checked_add(u128::from(
                bank_sysvars.genesis.poh_params.tick_duration_nanos,
            ))
        })
        .ok_or(LaunchCheckpointError::InvalidField {
            field: "genesis timing",
            reason: "nanoseconds per tick overflow",
        })?;
    let expected_ns_per_slot = ns_per_tick
        .checked_mul(u128::from(bank_sysvars.genesis.ticks_per_slot))
        .ok_or(LaunchCheckpointError::InvalidField {
            field: "genesis timing",
            reason: "nanoseconds per slot overflow",
        })?;
    if ns_per_slot != expected_ns_per_slot {
        return Err(LaunchCheckpointError::InvalidField {
            field: "nanoseconds per slot",
            reason: "value does not match genesis timing",
        });
    }
    if outcome.slots_processed != bank_sysvars.hash_height
        || outcome.last_slot != Some(bank_sysvars.current_slot)
        || outcome.epoch != bank_sysvars.current_epoch
    {
        return Err(LaunchCheckpointError::RuntimeInvariant(
            "replay progress and frozen Bank position disagree".to_owned(),
        ));
    }
    validate_outcome_semantics(&outcome, slots_per_epoch)?;
    bank_sysvars
        .validate_frozen_checkpoint(&outcome.account_state, &stake_history)
        .map_err(|error| LaunchCheckpointError::RuntimeInvariant(error.to_string()))?;
    let vote_state_cache =
        LaunchVoteStateCache::from_accounts(&outcome.account_state, vote_program);

    Ok((
        LaunchReplay {
            vote_program,
            config_program,
            system_program,
            stake_program,
            genesis_creation_time,
            ns_per_slot,
            slots_per_epoch,
            stake_history,
            bank_sysvars,
            bank_lifecycle_enabled,
            retain_instruction_mutations,
            vote_state_cache,
            parallel_vote_executor: None,
            lazy_vote_materialization_enabled: false,
            bpf_program_cache: Default::default(),
            bpf_compiler: crate::ReplayCompiler::new(),
            rolled_back_transaction: None,
            compact_checkpoint: None,
            pending_resume_descriptor: None,
            pending_resume_cursor: None,
            outcome,
        },
        account_state_sha256,
    ))
}

fn validate_outcome_semantics(
    outcome: &LaunchReplayOutcome,
    slots_per_epoch: u64,
) -> Result<(), LaunchCheckpointError> {
    if outcome.first_slot != Some(0) || outcome.last_slot.is_none() {
        return Err(LaunchCheckpointError::RuntimeInvariant(
            "launch replay does not begin at slot zero".to_owned(),
        ));
    }
    if slots_per_epoch == 0
        || outcome
            .last_slot
            .is_none_or(|last_slot| last_slot / slots_per_epoch != outcome.epoch)
    {
        return Err(LaunchCheckpointError::RuntimeInvariant(
            "reported epoch does not contain the frozen slot".to_owned(),
        ));
    }
    if (outcome.failed_transactions == 0) != outcome.first_failed_transaction.is_none() {
        return Err(LaunchCheckpointError::RuntimeInvariant(
            "failed-transaction counter and first failure disagree".to_owned(),
        ));
    }
    if let Some(failure) = &outcome.first_failed_transaction
        && (failure.location.slot < outcome.first_slot.unwrap_or(0)
            || failure.location.slot > outcome.last_slot.unwrap_or(0))
    {
        return Err(LaunchCheckpointError::RuntimeInvariant(
            "first transaction failure lies outside replay progress".to_owned(),
        ));
    }
    let native_mutations = outcome
        .vote_mutations
        .checked_add(outcome.config_mutations)
        .and_then(|count| count.checked_add(outcome.system_mutations))
        .and_then(|count| count.checked_add(outcome.stake_mutations))
        .and_then(|count| count.checked_add(outcome.bpf_loader_mutations))
        .ok_or_else(|| {
            LaunchCheckpointError::RuntimeInvariant("native mutation counters overflow".to_owned())
        })?;
    if native_mutations != outcome.instructions_processed {
        return Err(LaunchCheckpointError::RuntimeInvariant(
            "native mutation counters do not match processed instructions".to_owned(),
        ));
    }
    if outcome.slot_hashes_unavailable != outcome.last_slot.is_some_and(|slot| slot > 0) {
        return Err(LaunchCheckpointError::RuntimeInvariant(
            "SlotHashes availability phase is inconsistent".to_owned(),
        ));
    }
    Ok(())
}

fn validate_checkpoint_genesis(
    decoder: &mut Decoder<'_>,
    genesis: &CompactGenesisProbe,
) -> Result<(), LaunchCheckpointError> {
    let invalid = |reason| LaunchCheckpointError::InvalidField {
        field: "embedded genesis",
        reason,
    };
    if genesis.source != CompactGenesisSource::ExactGenesisBin || genesis.genesis_bin_len == 0 {
        return Err(invalid(
            "checkpoint does not contain exact genesis.bin identity",
        ));
    }
    if genesis.epoch_schedule.warmup
        || genesis.epoch_schedule.slots_per_epoch == 0
        || genesis.slots_per_segment == Some(0)
        || genesis.slots_per_segment.is_none()
        || genesis.poh_params.tick_duration_nanos >= 1_000_000_000
    {
        return Err(invalid("launch schedule or timing is unsupported"));
    }
    if genesis.fees.target_lamports_per_sig == 0
        || genesis.fees.target_lamports_per_sig > u64::MAX / 10
        || genesis.fees.target_sigs_per_slot == 0
        || genesis.fees.min_lamports_per_sig > genesis.fees.target_lamports_per_sig
        || genesis.fees.max_lamports_per_sig < genesis.fees.target_lamports_per_sig
        || genesis.fees.min_lamports_per_sig > genesis.fees.max_lamports_per_sig
        || genesis.fees.burn_percent > 100
        || genesis.rent.burn_percent > 100
    {
        return Err(invalid("launch fee or rent parameters are invalid"));
    }
    if !genesis.rent.exemption_threshold.is_finite()
        || !genesis.inflation.initial.is_finite()
        || !genesis.inflation.terminal.is_finite()
        || !genesis.inflation.taper.is_finite()
        || !genesis.inflation.foundation.is_finite()
        || !genesis.inflation.foundation_term.is_finite()
        || genesis
            .inflation_storage
            .is_some_and(|value| !value.is_finite())
    {
        return Err(invalid("floating-point parameters are not finite"));
    }

    for (name, expected) in [
        ("solana_config_program", CONFIG_PROGRAM_ID),
        ("solana_vote_program", VOTE_PROGRAM_ID),
        ("solana_system_program", SYSTEM_PROGRAM_ID),
        ("solana_stake_program", STAKE_PROGRAM_ID),
    ] {
        let mut matches = genesis
            .builtins
            .iter()
            .filter(|builtin| builtin.key == name);
        if matches.next().map(|builtin| builtin.pubkey) != Some(expected)
            || matches.next().is_some()
        {
            return Err(invalid("required native builtin identity is invalid"));
        }
    }
    for (index, builtin) in genesis.builtins.iter().enumerate() {
        if genesis.builtins[..index]
            .iter()
            .any(|prior| prior.key == builtin.key || prior.pubkey == builtin.pubkey)
        {
            return Err(invalid("native builtin identity is duplicated"));
        }
    }

    let account_count = genesis
        .accounts
        .len()
        .checked_add(genesis.reward_pools.len())
        .and_then(|count| count.checked_add(genesis.builtins.len()))
        .ok_or(LaunchCheckpointError::DecodeBound {
            field: "embedded genesis validation",
        })?;
    decoder.reserve_allocation(
        "embedded genesis validation",
        u64::try_from(account_count)
            .map_err(|_| LaunchCheckpointError::DecodeBound {
                field: "embedded genesis validation",
            })?
            .checked_mul(64)
            .ok_or(LaunchCheckpointError::DecodeBound {
                field: "embedded genesis validation",
            })?,
    )?;
    let mut occupied = BTreeSet::new();
    for account in genesis.accounts.iter().chain(&genesis.reward_pools) {
        if !occupied.insert(account.pubkey) {
            return Err(invalid("genesis account address is duplicated"));
        }
    }
    for builtin in &genesis.builtins {
        occupied.insert(builtin.pubkey);
    }
    if [
        FEES_SYSVAR_ID,
        STAKE_HISTORY_SYSVAR_ID,
        CLOCK_SYSVAR_ID,
        RENT_SYSVAR_ID,
        EPOCH_SCHEDULE_SYSVAR_ID,
        RECENT_BLOCKHASHES_SYSVAR_ID,
    ]
    .iter()
    .any(|pubkey| occupied.contains(pubkey))
    {
        return Err(invalid("genesis collides with a Bank-created sysvar"));
    }
    Ok(())
}

fn encode_stake_history(
    encoder: &mut Encoder,
    history: &LaunchStakeHistory,
) -> Result<(), LaunchCheckpointError> {
    encoder.collection_len(history.len(), MAX_STAKE_HISTORY_ENTRIES)?;
    for (epoch, entry) in history {
        encoder.u64(*epoch);
        encoder.u64(entry.effective);
        encoder.u64(entry.activating);
        encoder.u64(entry.deactivating);
    }
    Ok(())
}

fn decode_stake_history(
    decoder: &mut Decoder<'_>,
) -> Result<LaunchStakeHistory, LaunchCheckpointError> {
    let count = decoder.collection_count("StakeHistory", MAX_STAKE_HISTORY_ENTRIES, 4 * 8, 96)?;
    let mut history = BTreeMap::new();
    let mut previous = None;
    for _ in 0..count {
        let epoch = decoder.u64()?;
        if previous.is_some_and(|value| epoch <= value) {
            return Err(LaunchCheckpointError::InvalidField {
                field: "StakeHistory",
                reason: "epochs are not strictly increasing",
            });
        }
        previous = Some(epoch);
        history.insert(
            epoch,
            LaunchStakeHistoryEntry {
                effective: decoder.u64()?,
                activating: decoder.u64()?,
                deactivating: decoder.u64()?,
            },
        );
    }
    Ok(history)
}

fn encode_bank_state(
    encoder: &mut Encoder,
    state: &LaunchBankSysvarState,
) -> Result<(), LaunchCheckpointError> {
    encode_genesis(encoder, &state.genesis)?;
    encoder.u64(state.fee_governor.target_lamports_per_signature);
    encoder.u64(state.fee_governor.target_signatures_per_slot);
    encoder.u64(state.fee_governor.lamports_per_signature);
    encoder.u64(state.current_fee);
    encoder.u64(state.parent_signature_count);
    encoder.u64(state.hash_height);
    encoder.collection_len(state.recent_blockhashes.len(), MAX_RECENT_BLOCKHASHES)?;
    for (hash, entry) in &state.recent_blockhashes {
        encoder.bytes(hash);
        encoder.u64(entry.hash_height);
        encoder.u64(entry.fee);
    }
    encoder.bytes(&state.last_poh_blockhash);
    encoder.collection_len(state.slot_history_words.len(), SLOT_HISTORY_WORDS as u64)?;
    for word in &state.slot_history_words {
        encoder.u64(*word);
    }
    encoder.u64(state.slot_history_next_slot);
    encoder.u64(state.current_slot);
    encoder.u64(state.current_epoch);
    encoder.boolean(state.began_first_slot);
    encoder.boolean(state.current_slot_completed);
    encoder.boolean(state.inflation_disabled);
    Ok(())
}

fn decode_bank_state(
    decoder: &mut Decoder<'_>,
) -> Result<LaunchBankSysvarState, LaunchCheckpointError> {
    let genesis = decode_genesis(decoder)?;
    let fee_governor = LaunchFeeGovernor {
        target_lamports_per_signature: decoder.u64()?,
        target_signatures_per_slot: decoder.u64()?,
        lamports_per_signature: decoder.u64()?,
    };
    let current_fee = decoder.u64()?;
    let parent_signature_count = decoder.u64()?;
    let hash_height = decoder.u64()?;
    let recent_count = decoder.collection_count(
        "recent blockhashes",
        MAX_RECENT_BLOCKHASHES,
        32 + 8 + 8,
        128,
    )?;
    let mut recent_blockhashes = BTreeMap::new();
    let mut previous = None;
    for _ in 0..recent_count {
        let hash: [u8; 32] = decoder.array()?;
        if previous.is_some_and(|value| hash <= value) {
            return Err(LaunchCheckpointError::InvalidField {
                field: "recent blockhashes",
                reason: "hashes are not strictly increasing",
            });
        }
        previous = Some(hash);
        recent_blockhashes.insert(
            hash,
            LaunchRecentBlockhash {
                hash_height: decoder.u64()?,
                fee: decoder.u64()?,
            },
        );
    }
    let mut recent_blockhash_order = recent_blockhashes
        .iter()
        .map(|(hash, entry)| (*hash, *entry))
        .collect::<Vec<_>>();
    recent_blockhash_order.sort_unstable_by_key(|(_, entry)| Reverse(entry.hash_height));
    if recent_blockhash_order
        .windows(2)
        .any(|entries| entries[0].1.hash_height == entries[1].1.hash_height)
    {
        return Err(LaunchCheckpointError::InvalidField {
            field: "recent blockhashes",
            reason: "hash heights are not unique",
        });
    }
    let recent_blockhash_order = VecDeque::from(recent_blockhash_order);
    let last_poh_blockhash = decoder.array()?;
    let word_count =
        decoder.collection_count("SlotHistory words", SLOT_HISTORY_WORDS as u64, 8, 8)?;
    if word_count != SLOT_HISTORY_WORDS {
        return Err(LaunchCheckpointError::InvalidField {
            field: "SlotHistory words",
            reason: "word count is not the launch allocation",
        });
    }
    let mut slot_history_words = Vec::with_capacity(word_count.min(SMALL_INITIAL_CAPACITY));
    for _ in 0..word_count {
        slot_history_words.push(decoder.u64()?);
    }
    Ok(LaunchBankSysvarState {
        genesis,
        fee_governor,
        current_fee,
        parent_signature_count,
        hash_height,
        recent_blockhashes,
        recent_blockhash_order,
        last_poh_blockhash,
        slot_history_words,
        slot_history_next_slot: decoder.u64()?,
        current_slot: decoder.u64()?,
        current_epoch: decoder.u64()?,
        began_first_slot: decoder.boolean("began-first-slot flag")?,
        current_slot_completed: decoder.boolean("completed-slot flag")?,
        inflation_disabled: decoder.boolean("inflation-disabled flag")?,
    })
}

fn encode_outcome(
    encoder: &mut Encoder,
    outcome: &LaunchReplayOutcome,
) -> Result<(), LaunchCheckpointError> {
    encoder.u64(outcome.epoch);
    encoder.option_u64(outcome.first_slot);
    encoder.option_u64(outcome.last_slot);
    encoder.u64(outcome.slots_processed);
    encoder.u64(outcome.transactions_processed);
    encoder.u64(outcome.failed_transactions);
    encode_first_failure(encoder, outcome.first_failed_transaction.as_ref())?;
    encoder.u64(outcome.instructions_processed);
    encoder.u64(outcome.rolled_back_instructions);
    encoder.u64(outcome.vote_mutations);
    encoder.u64(outcome.config_mutations);
    encoder.u64(outcome.system_mutations);
    encoder.u64(outcome.stake_mutations);
    encoder.u64(outcome.bank_sysvar_writes);
    encode_pubkey_set(encoder, &outcome.bank_sysvar_accounts_written)?;
    encoder.boolean(outcome.slot_hashes_unavailable);
    encode_pubkey_set(encoder, &outcome.changed_accounts)?;
    if u64::try_from(outcome.account_state.len())
        .map_err(|_| LaunchCheckpointError::CheckpointTooLarge)?
        > MAX_RUNTIME_ACCOUNTS
    {
        return Err(LaunchCheckpointError::CheckpointTooLarge);
    }
    let account_hash = outcome.account_state.canonical_hash();
    encoder.bytes(&account_hash);
    encode_accounts(encoder, &outcome.account_state)?;
    // Appended after every v1 outcome field so all legacy offsets remain
    // stable and migration only needs version-aware tail decoding.
    encoder.u64(outcome.bpf_loader_mutations);
    Ok(())
}

fn decode_outcome(
    decoder: &mut Decoder<'_>,
    checkpoint_version: u16,
) -> Result<(LaunchReplayOutcome, [u8; 32]), LaunchCheckpointError> {
    let epoch = decoder.u64()?;
    let first_slot = decoder.option_u64("first slot")?;
    let last_slot = decoder.option_u64("last slot")?;
    let slots_processed = decoder.u64()?;
    let transactions_processed = decoder.u64()?;
    let failed_transactions = decoder.u64()?;
    let first_failed_transaction = decode_first_failure(decoder)?;
    let instructions_processed = decoder.u64()?;
    let rolled_back_instructions = decoder.u64()?;
    let vote_mutations = decoder.u64()?;
    let config_mutations = decoder.u64()?;
    let system_mutations = decoder.u64()?;
    let stake_mutations = decoder.u64()?;
    let bank_sysvar_writes = decoder.u64()?;
    let bank_sysvar_accounts_written = decode_pubkey_set(decoder, "Bank sysvar account set")?;
    let slot_hashes_unavailable = decoder.boolean("SlotHashes availability flag")?;
    let changed_accounts = decode_pubkey_set(decoder, "changed account set")?;
    let expected_account_hash: [u8; 32] = decoder.array()?;
    let (account_state, actual_account_hash) = decode_accounts(decoder)?;
    if actual_account_hash != expected_account_hash {
        return Err(LaunchCheckpointError::InvalidField {
            field: "account state hash",
            reason: "decoded accounts do not match the committed hash",
        });
    }
    let bpf_loader_mutations = if checkpoint_version == LEGACY_CHECKPOINT_VERSION {
        0
    } else {
        decoder.u64()?
    };
    Ok((
        LaunchReplayOutcome {
            epoch,
            first_slot,
            last_slot,
            slots_processed,
            transactions_processed,
            failed_transactions,
            first_failed_transaction,
            instructions_processed,
            rolled_back_instructions,
            vote_mutations,
            config_mutations,
            system_mutations,
            stake_mutations,
            bpf_loader_mutations,
            parallel_vote_batches: 0,
            parallel_vote_transactions: 0,
            max_parallel_vote_batch: 0,
            lazy_vote_commits: 0,
            vote_state_materializations: 0,
            bank_sysvar_writes,
            bank_sysvar_accounts_written,
            slot_hashes_unavailable,
            changed_accounts,
            instruction_mutations: Vec::new(),
            account_state,
        },
        actual_account_hash,
    ))
}

fn encode_first_failure(
    encoder: &mut Encoder,
    failure: Option<&LaunchDerivedTransactionFailure>,
) -> Result<(), LaunchCheckpointError> {
    match failure {
        None => encoder.u8(0),
        Some(failure) => {
            encoder.u8(1);
            encoder.u64(failure.location.slot);
            encoder.option_u32(failure.location.transaction_index);
            encoder.option_u32(failure.location.instruction_index);
            encoder.u64(failure.rolled_back_instructions);
            encoder.string(&failure.reason.to_string())?;
        }
    }
    Ok(())
}

fn decode_first_failure(
    decoder: &mut Decoder<'_>,
) -> Result<Option<LaunchDerivedTransactionFailure>, LaunchCheckpointError> {
    match decoder.u8()? {
        0 => Ok(None),
        1 => Ok(Some(LaunchDerivedTransactionFailure {
            location: LaunchReplayFailureLocation {
                slot: decoder.u64()?,
                transaction_index: decoder.option_u32("failure transaction index")?,
                instruction_index: decoder.option_u32("failure instruction index")?,
            },
            rolled_back_instructions: decoder.u64()?,
            reason: LaunchTransactionFailureReason::CheckpointRestored(
                decoder.string("first transaction failure")?,
            ),
        })),
        _ => Err(LaunchCheckpointError::InvalidField {
            field: "first transaction failure",
            reason: "option tag is not zero or one",
        }),
    }
}

fn encode_pubkey_set(
    encoder: &mut Encoder,
    values: &BTreeSet<[u8; 32]>,
) -> Result<(), LaunchCheckpointError> {
    encoder.collection_len(values.len(), MAX_PUBKEY_SET_ITEMS)?;
    for value in values {
        encoder.bytes(value);
    }
    Ok(())
}

fn decode_pubkey_set(
    decoder: &mut Decoder<'_>,
    field: &'static str,
) -> Result<BTreeSet<[u8; 32]>, LaunchCheckpointError> {
    let count = decoder.collection_count(field, MAX_PUBKEY_SET_ITEMS, 32, 96)?;
    let mut values = BTreeSet::new();
    let mut previous = None;
    for _ in 0..count {
        let value: [u8; 32] = decoder.array()?;
        if previous.is_some_and(|prior| value <= prior) {
            return Err(LaunchCheckpointError::InvalidField {
                field,
                reason: "pubkeys are not strictly increasing",
            });
        }
        previous = Some(value);
        values.insert(value);
    }
    Ok(values)
}

fn encode_accounts(
    encoder: &mut Encoder,
    accounts: &MemoryAccountStore,
) -> Result<(), LaunchCheckpointError> {
    encoder.collection_len(accounts.len(), MAX_RUNTIME_ACCOUNTS)?;
    let mut oversized = false;
    accounts.visit_sorted(&mut |pubkey, account| {
        if account.data.len() as u64 > MAX_ACCOUNT_DATA_BYTES {
            oversized = true;
            return;
        }
        encoder.bytes(&pubkey);
        encode_account_snapshot(encoder, account);
    });
    if oversized {
        return Err(LaunchCheckpointError::DecodeBound {
            field: "account data",
        });
    }
    Ok(())
}

fn decode_accounts(
    decoder: &mut Decoder<'_>,
) -> Result<(MemoryAccountStore, [u8; 32]), LaunchCheckpointError> {
    // pubkey + lamports + owner + executable + rent_epoch + data length
    let count = decoder.collection_count("accounts", MAX_RUNTIME_ACCOUNTS, 89, 192)?;
    let mut accounts = MemoryAccountStore::with_capacity(count.min(SMALL_INITIAL_CAPACITY));
    // Records are required to be in canonical pubkey order, so hash them as
    // they are decoded. This avoids the additional infallible key-sorting
    // allocation used by `MemoryAccountStore::canonical_hash`.
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla/replay-account-state/v1\0");
    hasher.update(
        u64::try_from(count)
            .map_err(|_| LaunchCheckpointError::DecodeBound { field: "accounts" })?
            .to_le_bytes(),
    );
    let mut previous = None;
    for _ in 0..count {
        let pubkey: [u8; 32] = decoder.array()?;
        if previous.is_some_and(|value| pubkey <= value) {
            return Err(LaunchCheckpointError::InvalidField {
                field: "accounts",
                reason: "pubkeys are not strictly increasing",
            });
        }
        previous = Some(pubkey);
        let account = decode_account_snapshot(decoder)?;
        hasher.update(pubkey);
        hasher.update(account.lamports.to_le_bytes());
        hasher.update(account.owner);
        hasher.update([u8::from(account.executable)]);
        hasher.update(account.rent_epoch.to_le_bytes());
        hasher.update(
            u64::try_from(account.data.len())
                .map_err(|_| LaunchCheckpointError::DecodeBound {
                    field: "account data",
                })?
                .to_le_bytes(),
        );
        hasher.update(&account.data);
        accounts.insert(pubkey, account);
    }
    Ok((accounts, hasher.finalize().into()))
}

fn encode_account_snapshot(encoder: &mut Encoder, account: &AccountSnapshot) {
    encoder.u64(account.lamports);
    encoder.bytes(&account.owner);
    encoder.boolean(account.executable);
    encoder.u64(account.rent_epoch);
    encoder.raw_vec(&account.data);
}

fn decode_account_snapshot(
    decoder: &mut Decoder<'_>,
) -> Result<AccountSnapshot, LaunchCheckpointError> {
    Ok(AccountSnapshot {
        lamports: decoder.u64()?,
        owner: decoder.array()?,
        executable: decoder.boolean("account executable flag")?,
        rent_epoch: decoder.u64()?,
        data: decoder.vec("account data", MAX_ACCOUNT_DATA_BYTES)?.into(),
    })
}

fn encode_genesis(
    encoder: &mut Encoder,
    genesis: &CompactGenesisProbe,
) -> Result<(), LaunchCheckpointError> {
    encoder.u8(match genesis.source {
        CompactGenesisSource::ExactGenesisBin => 0,
        CompactGenesisSource::InlineLegacy => 1,
    });
    encoder.bytes(&genesis.genesis_hash);
    encoder.u64(genesis.genesis_bin_len);
    encoder.i64(genesis.creation_time_unix);
    encoder.u32(genesis.cluster_id);
    encoder.u64(genesis.ticks_per_slot);
    encoder.option_u64(genesis.slots_per_segment);
    encoder.option_u64(genesis.backwards_compat_with_v0_23);
    encoder.u64(genesis.poh_params.tick_duration_secs);
    encoder.u32(genesis.poh_params.tick_duration_nanos);
    encoder.option_u64(genesis.poh_params.tick_count);
    encoder.option_u64(genesis.poh_params.hashes_per_tick);
    encoder.u64(genesis.fees.target_lamports_per_sig);
    encoder.u64(genesis.fees.target_sigs_per_slot);
    encoder.u64(genesis.fees.min_lamports_per_sig);
    encoder.u64(genesis.fees.max_lamports_per_sig);
    encoder.u8(genesis.fees.burn_percent);
    encoder.u64(genesis.rent.lamports_per_byte_year);
    encoder.u64(genesis.rent.exemption_threshold.to_bits());
    encoder.u8(genesis.rent.burn_percent);
    encoder.u64(genesis.inflation.initial.to_bits());
    encoder.u64(genesis.inflation.terminal.to_bits());
    encoder.u64(genesis.inflation.taper.to_bits());
    encoder.u64(genesis.inflation.foundation.to_bits());
    encoder.u64(genesis.inflation.foundation_term.to_bits());
    encoder.bytes(&genesis.inflation.padding);
    encoder.option_f64(genesis.inflation_storage);
    encoder.u64(genesis.epoch_schedule.slots_per_epoch);
    encoder.u64(genesis.epoch_schedule.leader_schedule_slot_offset);
    encoder.boolean(genesis.epoch_schedule.warmup);
    encoder.u64(genesis.epoch_schedule.first_normal_epoch);
    encoder.u64(genesis.epoch_schedule.first_normal_slot);
    encode_genesis_accounts(encoder, &genesis.accounts, MAX_GENESIS_ACCOUNTS)?;
    encoder.collection_len(genesis.builtins.len(), MAX_GENESIS_BUILTINS)?;
    for builtin in &genesis.builtins {
        encoder.string_bounded(&builtin.key, MAX_BUILTIN_NAME_BYTES)?;
        encoder.bytes(&builtin.pubkey);
    }
    encode_genesis_accounts(encoder, &genesis.reward_pools, MAX_REWARD_POOLS)?;
    Ok(())
}

fn decode_genesis(decoder: &mut Decoder<'_>) -> Result<CompactGenesisProbe, LaunchCheckpointError> {
    let source = match decoder.u8()? {
        0 => CompactGenesisSource::ExactGenesisBin,
        1 => CompactGenesisSource::InlineLegacy,
        _ => {
            return Err(LaunchCheckpointError::InvalidField {
                field: "genesis source",
                reason: "unknown source tag",
            });
        }
    };
    let genesis_hash = decoder.array()?;
    let genesis_bin_len = decoder.u64()?;
    let creation_time_unix = decoder.i64()?;
    let cluster_id = decoder.u32()?;
    let ticks_per_slot = decoder.u64()?;
    let slots_per_segment = decoder.option_u64("slots per segment")?;
    let backwards_compat_with_v0_23 = decoder.option_u64("backwards compatibility slot")?;
    let poh_params = WincodeArchiveV2GenesisPohParams {
        tick_duration_secs: decoder.u64()?,
        tick_duration_nanos: decoder.u32()?,
        tick_count: decoder.option_u64("tick count")?,
        hashes_per_tick: decoder.option_u64("hashes per tick")?,
    };
    let fees = WincodeArchiveV2GenesisFeeParams {
        target_lamports_per_sig: decoder.u64()?,
        target_sigs_per_slot: decoder.u64()?,
        min_lamports_per_sig: decoder.u64()?,
        max_lamports_per_sig: decoder.u64()?,
        burn_percent: decoder.u8()?,
    };
    let rent = WincodeArchiveV2GenesisRentParams {
        lamports_per_byte_year: decoder.u64()?,
        exemption_threshold: f64::from_bits(decoder.u64()?),
        burn_percent: decoder.u8()?,
    };
    let inflation = WincodeArchiveV2GenesisInflationParams {
        initial: f64::from_bits(decoder.u64()?),
        terminal: f64::from_bits(decoder.u64()?),
        taper: f64::from_bits(decoder.u64()?),
        foundation: f64::from_bits(decoder.u64()?),
        foundation_term: f64::from_bits(decoder.u64()?),
        padding: decoder.array()?,
    };
    let inflation_storage = decoder.option_f64("inflation storage")?;
    let epoch_schedule = WincodeArchiveV2GenesisEpochSchedule {
        slots_per_epoch: decoder.u64()?,
        leader_schedule_slot_offset: decoder.u64()?,
        warmup: decoder.boolean("epoch warmup flag")?,
        first_normal_epoch: decoder.u64()?,
        first_normal_slot: decoder.u64()?,
    };
    let accounts = decode_genesis_accounts(decoder, "genesis accounts", MAX_GENESIS_ACCOUNTS)?;
    // Empty name length prefix plus pubkey.
    let builtin_count =
        decoder.collection_count("genesis builtins", MAX_GENESIS_BUILTINS, 8 + 32, 96)?;
    let mut builtins = Vec::with_capacity(builtin_count.min(SMALL_INITIAL_CAPACITY));
    for _ in 0..builtin_count {
        builtins.push(CompactGenesisBuiltin {
            key: decoder.string_bounded("genesis builtin name", MAX_BUILTIN_NAME_BYTES)?,
            pubkey: decoder.array()?,
        });
    }
    let reward_pools = decode_genesis_accounts(decoder, "genesis reward pools", MAX_REWARD_POOLS)?;
    Ok(CompactGenesisProbe {
        source,
        genesis_hash,
        genesis_bin_len,
        creation_time_unix,
        cluster_id,
        ticks_per_slot,
        slots_per_segment,
        backwards_compat_with_v0_23,
        poh_params,
        fees,
        rent,
        inflation,
        inflation_storage,
        epoch_schedule,
        accounts,
        builtins,
        reward_pools,
    })
}

fn encode_genesis_accounts(
    encoder: &mut Encoder,
    accounts: &[CompactGenesisAccount],
    maximum: u64,
) -> Result<(), LaunchCheckpointError> {
    encoder.collection_len(accounts.len(), maximum)?;
    for account in accounts {
        if account.data.len() as u64 > MAX_ACCOUNT_DATA_BYTES {
            return Err(LaunchCheckpointError::DecodeBound {
                field: "genesis account data",
            });
        }
        encoder.bytes(&account.pubkey);
        encoder.u64(account.lamports);
        encoder.bytes(&account.owner);
        encoder.boolean(account.executable);
        encoder.u64(account.rent_epoch);
        encoder.raw_vec(&account.data);
    }
    Ok(())
}

fn decode_genesis_accounts(
    decoder: &mut Decoder<'_>,
    field: &'static str,
    maximum: u64,
) -> Result<Vec<CompactGenesisAccount>, LaunchCheckpointError> {
    // pubkey + lamports + owner + executable + rent_epoch + data length
    let count = decoder.collection_count(field, maximum, 89, 256)?;
    let mut accounts = Vec::with_capacity(count.min(SMALL_INITIAL_CAPACITY));
    for _ in 0..count {
        accounts.push(CompactGenesisAccount {
            pubkey: decoder.array()?,
            lamports: decoder.u64()?,
            owner: decoder.array()?,
            executable: decoder.boolean("genesis account executable flag")?,
            rent_epoch: decoder.u64()?,
            data: decoder.vec("genesis account data", MAX_ACCOUNT_DATA_BYTES)?,
        });
    }
    Ok(accounts)
}

fn validated_payload(bytes: &[u8]) -> Result<(u16, &[u8]), LaunchCheckpointError> {
    if bytes.len() < HEADER_LEN + CHECKSUM_LEN {
        return Err(LaunchCheckpointError::Truncated);
    }
    if bytes[..8] != CHECKPOINT_MAGIC {
        return Err(LaunchCheckpointError::InvalidMagic);
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    if version != LEGACY_CHECKPOINT_VERSION && version != CHECKPOINT_VERSION {
        return Err(LaunchCheckpointError::UnsupportedVersion { found: version });
    }
    let flags = u16::from_le_bytes(bytes[10..12].try_into().unwrap());
    if flags != CHECKPOINT_FLAGS {
        return Err(LaunchCheckpointError::UnsupportedFlags { found: flags });
    }
    let payload_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let expected_len = payload_len
        .checked_add((HEADER_LEN + CHECKSUM_LEN) as u64)
        .ok_or(LaunchCheckpointError::InvalidPayloadLength)?;
    if expected_len > MAX_CHECKPOINT_BYTES || expected_len != bytes.len() as u64 {
        return Err(LaunchCheckpointError::InvalidPayloadLength);
    }
    let payload_end = HEADER_LEN
        .checked_add(
            usize::try_from(payload_len)
                .map_err(|_| LaunchCheckpointError::InvalidPayloadLength)?,
        )
        .ok_or(LaunchCheckpointError::InvalidPayloadLength)?;
    let expected_checksum: [u8; 32] = bytes[payload_end..]
        .try_into()
        .map_err(|_| LaunchCheckpointError::InvalidPayloadLength)?;
    if checkpoint_checksum(&bytes[..payload_end]) != expected_checksum {
        return Err(LaunchCheckpointError::ChecksumMismatch);
    }
    Ok((version, &bytes[HEADER_LEN..payload_end]))
}

fn checkpoint_checksum(header_and_payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CHECKSUM_DOMAIN);
    hasher.update(header_and_payload);
    hasher.finalize().into()
}

fn runtime_profile_sha256() -> [u8; 32] {
    Sha256::digest(RUNTIME_PROFILE).into()
}

fn legacy_runtime_profile_v1_sha256() -> [u8; 32] {
    Sha256::digest(LEGACY_RUNTIME_PROFILE_V1).into()
}

fn previous_runtime_profile_v2_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V2).into()
}

fn previous_runtime_profile_v3_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V3).into()
}

fn previous_runtime_profile_v4_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V4).into()
}

fn previous_runtime_profile_v5_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V5).into()
}

fn previous_runtime_profile_v6_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V6).into()
}

fn previous_runtime_profile_v7_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V7).into()
}

fn previous_runtime_profile_v8_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V8).into()
}

fn previous_runtime_profile_v9_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V9).into()
}

fn previous_runtime_profile_v10_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V10).into()
}

fn previous_runtime_profile_v11_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V11).into()
}

fn previous_runtime_profile_v12_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V12).into()
}

fn previous_runtime_profile_v13_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V13).into()
}

fn previous_runtime_profile_v14_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V14).into()
}

fn previous_runtime_profile_v15_sha256() -> [u8; 32] {
    Sha256::digest(PREVIOUS_RUNTIME_PROFILE_V15).into()
}

struct Encoder {
    bytes: Vec<u8>,
    maximum_len: usize,
    too_large: bool,
}

impl Encoder {
    fn checkpoint() -> Self {
        let mut bytes = Vec::with_capacity(HEADER_LEN);
        bytes.extend_from_slice(&CHECKPOINT_MAGIC);
        bytes.extend_from_slice(&CHECKPOINT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&CHECKPOINT_FLAGS.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        Self {
            bytes,
            maximum_len: MAX_CHECKPOINT_BYTES as usize - CHECKSUM_LEN,
            too_large: false,
        }
    }

    fn finish_checkpoint(mut self) -> Result<Vec<u8>, LaunchCheckpointError> {
        if self.too_large || self.bytes.len() < HEADER_LEN {
            return Err(LaunchCheckpointError::CheckpointTooLarge);
        }
        let payload_len = self.bytes.len() - HEADER_LEN;
        let payload_len =
            u64::try_from(payload_len).map_err(|_| LaunchCheckpointError::CheckpointTooLarge)?;
        self.bytes[12..20].copy_from_slice(&payload_len.to_le_bytes());
        let checksum = checkpoint_checksum(&self.bytes);
        self.bytes
            .try_reserve_exact(CHECKSUM_LEN)
            .map_err(|_| LaunchCheckpointError::CheckpointTooLarge)?;
        self.bytes.extend_from_slice(&checksum);
        Ok(self.bytes)
    }

    fn bytes(&mut self, value: &[u8]) {
        if self.too_large {
            return;
        }
        let Some(next_len) = self.bytes.len().checked_add(value.len()) else {
            self.too_large = true;
            return;
        };
        if next_len > self.maximum_len {
            self.too_large = true;
            return;
        }
        if next_len > self.bytes.capacity() {
            // Checkpoint records contain many adjacent scalar fields. Grow
            // geometrically instead of issuing one realloc request per field,
            // but cap speculative capacity at the checkpoint size limit.
            // This changes allocation growth only, never encoded bytes.
            let target_capacity = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(next_len)
                .min(self.maximum_len);
            let additional = target_capacity - self.bytes.len();
            if self.bytes.try_reserve_exact(additional).is_err() {
                self.too_large = true;
                return;
            }
        }
        self.bytes.extend_from_slice(value);
    }

    fn u8(&mut self, value: u8) {
        self.bytes(&[value]);
    }

    fn boolean(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn u32(&mut self, value: u32) {
        self.bytes(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes(&value.to_le_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes(&value.to_le_bytes());
    }

    fn collection_len(&mut self, value: usize, maximum: u64) -> Result<(), LaunchCheckpointError> {
        let value = u64::try_from(value).map_err(|_| LaunchCheckpointError::CheckpointTooLarge)?;
        if value > maximum {
            return Err(LaunchCheckpointError::CheckpointTooLarge);
        }
        self.u64(value);
        Ok(())
    }

    fn raw_vec(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.bytes(value);
    }

    fn string(&mut self, value: &str) -> Result<(), LaunchCheckpointError> {
        self.string_bounded(value, MAX_STRING_BYTES)
    }

    fn string_bounded(&mut self, value: &str, maximum: u64) -> Result<(), LaunchCheckpointError> {
        if value.len() as u64 > maximum {
            return Err(LaunchCheckpointError::CheckpointTooLarge);
        }
        self.raw_vec(value.as_bytes());
        Ok(())
    }

    fn option_u64(&mut self, value: Option<u64>) {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
        }
    }

    fn option_u32(&mut self, value: Option<u32>) {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1);
                self.u32(value);
            }
        }
    }

    fn option_f64(&mut self, value: Option<f64>) {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1);
                self.u64(value.to_bits());
            }
        }
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
    allocation_remaining: u64,
}

#[cfg(test)]
// Keep the codec tests beside the Encoder/Decoder boundary they exercise. The
// remaining item is only the Decoder method implementation.
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use crate::{
        CompactGenerationContext, CompactSlotProbe, LaunchInstructionMutation, LaunchReplayError,
        LaunchReplayOutcome,
    };
    use blockzilla_read_sdk::GenerationBinding;

    fn launch_genesis() -> CompactGenesisProbe {
        CompactGenesisProbe {
            source: CompactGenesisSource::ExactGenesisBin,
            genesis_hash: [1; 32],
            genesis_bin_len: 128,
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
                slots_per_epoch: 2,
                leader_schedule_slot_offset: 2,
                warmup: false,
                first_normal_epoch: 0,
                first_normal_slot: 0,
            },
            accounts: Vec::new(),
            builtins: vec![
                CompactGenesisBuiltin {
                    key: "solana_config_program".to_owned(),
                    pubkey: CONFIG_PROGRAM_ID,
                },
                CompactGenesisBuiltin {
                    key: "solana_vote_program".to_owned(),
                    pubkey: VOTE_PROGRAM_ID,
                },
                CompactGenesisBuiltin {
                    key: "solana_system_program".to_owned(),
                    pubkey: SYSTEM_PROGRAM_ID,
                },
                CompactGenesisBuiltin {
                    key: "solana_stake_program".to_owned(),
                    pubkey: STAKE_PROGRAM_ID,
                },
            ],
            reward_pools: Vec::new(),
        }
    }

    fn slot(
        block_id: u32,
        slot: u64,
        parent_slot: u64,
        blockhash: [u8; 32],
        previous_blockhash: [u8; 32],
    ) -> CompactSlotProbe {
        CompactSlotProbe {
            block_id,
            slot,
            parent_slot,
            block_time: None,
            block_height: None,
            blockhash_id: block_id,
            blockhash,
            previous_blockhash_id: block_id.saturating_sub(1),
            previous_blockhash,
            transaction_count: 0,
            transactions: Vec::new(),
        }
    }

    fn replay() -> LaunchReplay {
        let genesis = launch_genesis();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        replay.enable_bank_lifecycle();
        replay
    }

    fn context(block_count: u64, first_slot: u64, seed: u8) -> CompactGenerationContext {
        CompactGenerationContext {
            root: PathBuf::from("validated-compact-fixture"),
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 0,
            generation_id: format!("generation-{seed}"),
            slots_per_epoch: 2,
            block_count,
            complete: true,
            first_slot: Some(first_slot),
            last_slot: (block_count != 0)
                .then(|| first_slot.saturating_add(block_count.saturating_sub(1))),
            binding: GenerationBinding {
                generation_digest: [seed; 32],
                registry_sha256: [seed.wrapping_add(1); 32],
            },
            genesis: Some(launch_genesis()),
        }
    }

    fn process_direct(replay: &mut LaunchReplay, slot: &CompactSlotProbe) {
        replay
            .process_slot(slot, &mut |_: &LaunchInstructionMutation| {})
            .unwrap();
    }

    fn process_compact(
        replay: &mut LaunchReplay,
        context: &CompactGenerationContext,
        row_number: u64,
        next_slot: Option<u64>,
        slot: &CompactSlotProbe,
    ) {
        replay
            .process_compact_row(context, row_number, next_slot, slot, &mut |_| {})
            .unwrap();
    }

    fn reference_checkpoint(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len() + CHECKSUM_LEN);
        bytes.extend_from_slice(&CHECKPOINT_MAGIC);
        bytes.extend_from_slice(&CHECKPOINT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&CHECKPOINT_FLAGS.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(payload);
        let checksum = checkpoint_checksum(&bytes);
        bytes.extend_from_slice(&checksum);
        bytes
    }

    #[test]
    fn amortized_encoder_growth_preserves_checkpoint_wire_bytes() {
        let body = (0..16_384)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut encoder = Encoder::checkpoint();
        let mut payload = Vec::new();

        encoder.u8(0xa5);
        payload.push(0xa5);
        encoder.boolean(true);
        payload.push(1);
        encoder.u32(0x1020_3040);
        payload.extend_from_slice(&0x1020_3040_u32.to_le_bytes());
        encoder.u64(0x5060_7080_90a0_b0c0);
        payload.extend_from_slice(&0x5060_7080_90a0_b0c0_u64.to_le_bytes());
        encoder.i64(-17);
        payload.extend_from_slice(&(-17_i64).to_le_bytes());
        encoder.u128(u128::MAX - 9);
        payload.extend_from_slice(&(u128::MAX - 9).to_le_bytes());
        encoder.raw_vec(&body);
        payload.extend_from_slice(&(body.len() as u64).to_le_bytes());
        payload.extend_from_slice(&body);
        encoder.option_u64(Some(41));
        payload.push(1);
        payload.extend_from_slice(&41_u64.to_le_bytes());
        encoder.option_u32(None);
        payload.push(0);

        let actual = encoder.finish_checkpoint().unwrap();
        let expected = reference_checkpoint(&payload);
        assert_eq!(actual, expected);
        assert_eq!(validated_payload(&actual).unwrap().1, payload);
    }

    #[test]
    fn encoder_amortizes_repeated_tiny_appends() {
        let mut encoder = Encoder::checkpoint();
        let mut capacity_growths = 0usize;
        for value in 0..65_536_u32 {
            let prior_capacity = encoder.bytes.capacity();
            encoder.u8(value as u8);
            capacity_growths += usize::from(encoder.bytes.capacity() != prior_capacity);
        }

        assert!(
            capacity_growths <= 32,
            "tiny appends caused {capacity_growths} capacity growths"
        );
        assert!(encoder.bytes.capacity() <= encoder.maximum_len);
        let bytes = encoder.finish_checkpoint().unwrap();
        let (_, payload) = validated_payload(&bytes).unwrap();
        assert_eq!(payload.len(), 65_536);
        assert_eq!(payload[0], 0);
        assert_eq!(payload[65_535], 0xff);
    }

    fn reseal(bytes: &mut [u8]) {
        let payload_end = bytes.len() - CHECKSUM_LEN;
        let checksum = checkpoint_checksum(&bytes[..payload_end]);
        bytes[payload_end..].copy_from_slice(&checksum);
    }

    fn convert_v2_to_v1(mut bytes: Vec<u8>, runtime_profile_sha256: [u8; 32]) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        let payload_end = bytes.len() - CHECKSUM_LEN;
        let counter_start = payload_end - std::mem::size_of::<u64>();
        bytes.truncate(counter_start);
        bytes[8..10].copy_from_slice(&LEGACY_CHECKPOINT_VERSION.to_le_bytes());
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&runtime_profile_sha256);
        let payload_len = u64::try_from(bytes.len() - HEADER_LEN).unwrap();
        bytes[12..20].copy_from_slice(&payload_len.to_le_bytes());
        let checksum = checkpoint_checksum(&bytes);
        bytes.extend_from_slice(&checksum);
        bytes
    }

    fn convert_v2_to_legacy_v1(bytes: Vec<u8>) -> Vec<u8> {
        convert_v2_to_v1(bytes, legacy_runtime_profile_v1_sha256())
    }

    fn convert_v2_to_previous_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v2_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_pda_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v3_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_outcome_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v4_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_vote_signature_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v5_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_stake_authorize_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v6_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_balance_projection_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v7_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_system_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v8_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_prefunded_create_recovery_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v9_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_vote_commission_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v10_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_loader_balance_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v11_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_vote_switch_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v12_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_stake_merge_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v13_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_transient_balance_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v14_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn convert_v2_to_previous_immutable_cpi_runtime(mut bytes: Vec<u8>) -> Vec<u8> {
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        bytes[HEADER_LEN..HEADER_LEN + 32].copy_from_slice(&previous_runtime_profile_v15_sha256());
        reseal(&mut bytes);
        bytes
    }

    fn checkpoint_at_mainnet_epoch_terminal(terminal_slot: u64) -> Vec<u8> {
        const MAINNET_SLOTS_PER_EPOCH: u64 = 432_000;
        const BPF_ACTIVATION_SLOT: u64 =
            BPF_LOADER_STABLE_ACTIVATION_EPOCH * MAINNET_SLOTS_PER_EPOCH;

        assert_eq!(
            terminal_slot % MAINNET_SLOTS_PER_EPOCH,
            MAINNET_SLOTS_PER_EPOCH - 1
        );
        assert!(terminal_slot > BPF_ACTIVATION_SLOT);
        let mut genesis = launch_genesis();
        genesis.epoch_schedule.slots_per_epoch = MAINNET_SLOTS_PER_EPOCH;
        genesis.epoch_schedule.leader_schedule_slot_offset = MAINNET_SLOTS_PER_EPOCH;
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        replay.enable_bank_lifecycle();

        let mut genesis_generation = context(1, 0, 101);
        genesis_generation.slots_per_epoch = MAINNET_SLOTS_PER_EPOCH;
        genesis_generation.genesis = Some(genesis.clone());
        process_compact(
            &mut replay,
            &genesis_generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis.genesis_hash),
        );

        let mut activation_generation = context(1, BPF_ACTIVATION_SLOT, 102);
        activation_generation.epoch = BPF_LOADER_STABLE_ACTIVATION_EPOCH;
        activation_generation.slots_per_epoch = MAINNET_SLOTS_PER_EPOCH;
        activation_generation.genesis = None;
        process_compact(
            &mut replay,
            &activation_generation,
            0,
            None,
            &slot(0, BPF_ACTIVATION_SLOT, 0, [3; 32], [2; 32]),
        );

        let terminal_epoch = terminal_slot / MAINNET_SLOTS_PER_EPOCH;
        let mut terminal_generation = context(1, terminal_slot, 103);
        terminal_generation.epoch = terminal_epoch;
        terminal_generation.slots_per_epoch = MAINNET_SLOTS_PER_EPOCH;
        terminal_generation.genesis = None;
        process_compact(
            &mut replay,
            &terminal_generation,
            0,
            None,
            &slot(0, terminal_slot, BPF_ACTIVATION_SLOT, [4; 32], [3; 32]),
        );
        replay.encode_frozen_checkpoint().unwrap()
    }

    fn process_empty_through_epoch(replay: &mut LaunchReplay, final_epoch: u64) {
        let mut previous_blockhash = launch_genesis().genesis_hash;
        for epoch in 0..=final_epoch {
            let first_slot = epoch * 2;
            let mut generation = context(2, first_slot, epoch as u8 + 10);
            generation.epoch = epoch;
            if epoch != 0 {
                generation.genesis = None;
            }
            let first_hash = [(first_slot as u8).wrapping_add(2); 32];
            let second_hash = [(first_slot as u8).wrapping_add(3); 32];
            process_compact(
                replay,
                &generation,
                0,
                Some(first_slot + 1),
                &slot(
                    first_slot as u32,
                    first_slot,
                    first_slot.saturating_sub(1),
                    first_hash,
                    previous_blockhash,
                ),
            );
            process_compact(
                replay,
                &generation,
                1,
                None,
                &slot(
                    first_slot as u32 + 1,
                    first_slot + 1,
                    first_slot,
                    second_hash,
                    first_hash,
                ),
            );
            previous_blockhash = second_hash;
        }
    }

    fn assert_outcomes_match(left: &LaunchReplayOutcome, right: &LaunchReplayOutcome) {
        assert_eq!(left.epoch, right.epoch);
        assert_eq!(left.first_slot, right.first_slot);
        assert_eq!(left.last_slot, right.last_slot);
        assert_eq!(left.slots_processed, right.slots_processed);
        assert_eq!(left.transactions_processed, right.transactions_processed);
        assert_eq!(left.failed_transactions, right.failed_transactions);
        assert_eq!(left.instructions_processed, right.instructions_processed);
        assert_eq!(left.bpf_loader_mutations, right.bpf_loader_mutations);
        assert_eq!(left.bank_sysvar_writes, right.bank_sysvar_writes);
        assert_eq!(
            left.bank_sysvar_accounts_written,
            right.bank_sysvar_accounts_written
        );
        assert_eq!(left.changed_accounts, right.changed_accounts);
        assert_eq!(
            left.account_state.canonical_hash(),
            right.account_state.canonical_hash()
        );
    }

    #[test]
    fn split_restore_matches_uninterrupted_and_is_byte_deterministic() {
        let genesis_hash = launch_genesis().genesis_hash;
        // `block_id` deliberately differs from the physical row ordinal.
        let slot_0 = slot(77, 0, 0, [2; 32], genesis_hash);
        let slot_1 = slot(88, 1, 0, [3; 32], [2; 32]);
        let context = context(2, 0, 4);

        let mut uninterrupted = replay();
        process_compact(&mut uninterrupted, &context, 0, Some(1), &slot_0);
        process_compact(&mut uninterrupted, &context, 1, None, &slot_1);

        let mut split = replay();
        process_compact(&mut split, &context, 0, Some(1), &slot_0);
        let split_cursor = CompactCheckpointCursor {
            last_slot: 0,
            next_row: 1,
            generation_block_count: 2,
            next_slot: Some(1),
        };
        let bytes = split.encode_frozen_checkpoint().unwrap();
        let (mut restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap();
        assert_eq!(metadata.cursor, split_cursor);
        assert_eq!(metadata.descriptor.generation_digest, [4; 32]);
        assert_eq!(
            restored.bank_sysvars.recent_blockhash_order, split.bank_sysvars.recent_blockhash_order,
            "checkpoint decode must reconstruct the unencoded newest-first cache",
        );
        assert_eq!(
            metadata.account_state_sha256,
            split.outcome.account_state.canonical_hash()
        );
        restored
            .process_compact_row(&context, 1, None, &slot_1, &mut |_| {})
            .unwrap();

        let uninterrupted_bytes = uninterrupted.encode_frozen_checkpoint().unwrap();
        let restored_bytes = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(uninterrupted_bytes, restored_bytes);

        assert_outcomes_match(&uninterrupted.finish(), &restored.finish());
    }

    #[test]
    fn v2_appends_and_roundtrips_the_bpf_loader_mutation_counter() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        replay.outcome.instructions_processed = 7;
        replay.outcome.bpf_loader_mutations = 7;

        let bytes = replay.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        let payload_end = bytes.len() - CHECKSUM_LEN;
        assert_eq!(
            u64::from_le_bytes(bytes[payload_end - 8..payload_end].try_into().unwrap()),
            7
        );
        let (restored, metadata) = LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap();
        assert_eq!(restored.outcome.bpf_loader_mutations, 7);
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            runtime_profile_sha256()
        );
        assert!(restored.bpf_program_cache.is_empty());
    }

    #[test]
    fn previous_runtime_profile_hashes_match_published_checkpoints() {
        assert_eq!(
            previous_runtime_profile_v2_sha256(),
            [
                0xb9, 0x26, 0x33, 0xd0, 0x99, 0xdf, 0xa2, 0x52, 0x42, 0x6a, 0x16, 0x2f, 0x3c, 0xa7,
                0xbe, 0x33, 0x4e, 0xca, 0xd9, 0x7c, 0x00, 0x97, 0x49, 0xdd, 0x26, 0xb5, 0xb9, 0x22,
                0x7c, 0xb1, 0xa7, 0x3c,
            ],
            "the .3 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v3_sha256(),
            [
                0x69, 0x94, 0xdc, 0x36, 0x5b, 0x16, 0x1d, 0x29, 0xd0, 0x43, 0xfe, 0x94, 0xf6, 0xa5,
                0xb9, 0x62, 0x20, 0x5a, 0xc0, 0x4e, 0xc6, 0xf6, 0xc1, 0xa4, 0x03, 0xf6, 0x79, 0x87,
                0x37, 0xe2, 0x69, 0x77,
            ],
            "the .4 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v4_sha256(),
            [
                0x1e, 0x48, 0xb1, 0x02, 0xec, 0x74, 0xcc, 0x3c, 0x91, 0x61, 0xcb, 0xb9, 0xd1, 0x6f,
                0x60, 0x84, 0xc1, 0x6e, 0xae, 0xf5, 0xa7, 0xd9, 0x8e, 0x5b, 0x12, 0x78, 0x1a, 0x82,
                0xbf, 0x1f, 0x84, 0x0f,
            ],
            "the .5 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v5_sha256(),
            [
                0xf8, 0x23, 0xb3, 0x56, 0xf5, 0xba, 0xe7, 0x62, 0x6f, 0x89, 0xbe, 0x5c, 0x21, 0xb8,
                0x4a, 0xb1, 0x53, 0x89, 0xd8, 0xc2, 0xd2, 0xf1, 0xcf, 0x0b, 0xff, 0x78, 0x38, 0xfd,
                0x3e, 0xef, 0x71, 0x07,
            ],
            "the .6 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v6_sha256(),
            [
                0xee, 0x5d, 0xbe, 0xa4, 0x09, 0xa1, 0xea, 0x9b, 0xbb, 0xdc, 0xdf, 0xba, 0x22, 0xae,
                0xf8, 0xb5, 0x07, 0xc1, 0xb5, 0x6f, 0x3a, 0xe4, 0x0b, 0x27, 0x6c, 0xb9, 0x99, 0x9b,
                0x34, 0xdf, 0xde, 0x5b,
            ],
            "the .7 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v7_sha256(),
            [
                0x25, 0x00, 0x70, 0x50, 0x97, 0xb7, 0xdd, 0xa3, 0x66, 0xa2, 0x44, 0x8b, 0xf6, 0x16,
                0x73, 0xa6, 0xa8, 0x0f, 0x81, 0xc2, 0x19, 0xa2, 0x8c, 0x71, 0x6f, 0x28, 0x10, 0xe0,
                0x7a, 0x10, 0x0b, 0x59,
            ],
            "the .8 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v8_sha256(),
            [
                0xcf, 0x1f, 0x44, 0x67, 0xb8, 0x77, 0xff, 0x37, 0xe7, 0x52, 0xb7, 0xa8, 0x1b, 0x39,
                0x9c, 0x4a, 0xe5, 0x47, 0x12, 0x49, 0x6a, 0x11, 0x6c, 0x29, 0x76, 0xf9, 0x35, 0x69,
                0xfa, 0x02, 0xfd, 0x9a,
            ],
            "the .9 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v9_sha256(),
            [
                0x39, 0x37, 0xfe, 0x57, 0x08, 0x0f, 0x62, 0x02, 0xe1, 0xcc, 0x38, 0x45, 0x66, 0x3b,
                0x36, 0xab, 0x44, 0x52, 0x9a, 0xc3, 0x27, 0x5d, 0xad, 0x07, 0xcf, 0xb6, 0xda, 0xe7,
                0x42, 0x04, 0x80, 0x52,
            ],
            "the .10 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v10_sha256(),
            [
                0x28, 0x46, 0x5e, 0xe2, 0xd1, 0xd3, 0x4b, 0xd1, 0x99, 0x14, 0x28, 0xf0, 0x5e, 0x44,
                0x30, 0x2c, 0x46, 0x4b, 0x81, 0xba, 0x88, 0x57, 0x3a, 0x8f, 0x5c, 0x9e, 0xff, 0x0b,
                0x03, 0x7b, 0x69, 0xb3,
            ],
            "the .11 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v11_sha256(),
            [
                0xd0, 0xd9, 0xb3, 0xec, 0xcc, 0x9b, 0x1c, 0x7c, 0x4a, 0xd4, 0x3a, 0x7f, 0x8b, 0x3e,
                0xfa, 0x6c, 0xa3, 0xe0, 0xf7, 0x82, 0x5e, 0x45, 0x79, 0xeb, 0x2c, 0x3a, 0xd4, 0x62,
                0x54, 0xc8, 0xc9, 0x74,
            ],
            "the .12 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v12_sha256(),
            [
                0xa5, 0xd4, 0x1b, 0x28, 0xea, 0x80, 0xc9, 0x2d, 0x20, 0x75, 0x3b, 0x7c, 0xb1, 0x39,
                0x4e, 0x5f, 0x5c, 0xce, 0x33, 0xde, 0x61, 0x05, 0x1b, 0xcd, 0xa7, 0x24, 0x40, 0x2e,
                0x1d, 0x5d, 0xc4, 0xb3,
            ],
            "the .13 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v13_sha256(),
            [
                0xba, 0x9f, 0x00, 0x9b, 0x4b, 0xb2, 0xa6, 0x6e, 0xa3, 0xf0, 0x2c, 0xd6, 0xb5, 0xe7,
                0x1f, 0x97, 0x68, 0xb9, 0x43, 0x52, 0x32, 0x6e, 0x8a, 0xbc, 0xd7, 0x88, 0xe5, 0xb9,
                0x63, 0x73, 0xc8, 0xf8,
            ],
            "the .14 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v14_sha256(),
            [
                0x40, 0x75, 0xb9, 0x50, 0x7f, 0x20, 0xb3, 0x1e, 0xed, 0x58, 0xe3, 0xd6, 0xa1, 0xb3,
                0xee, 0x5c, 0x8e, 0xd5, 0x95, 0xae, 0xf4, 0x1d, 0x3c, 0x42, 0x22, 0xf7, 0x5a, 0xbe,
                0x8a, 0x83, 0x64, 0x33,
            ],
            "the .15 profile must remain byte-identical to published checkpoints",
        );
        assert_eq!(
            previous_runtime_profile_v15_sha256(),
            [
                0xd7, 0xfe, 0xce, 0x3c, 0x51, 0x9e, 0xf9, 0x67, 0xc2, 0x58, 0x6e, 0x88, 0x6d, 0x54,
                0x80, 0x7c, 0x7f, 0x9a, 0x13, 0x5b, 0xa6, 0x6f, 0x8d, 0xcb, 0x8b, 0x27, 0x4e, 0x77,
                0x17, 0xe1, 0x06, 0xeb,
            ],
            "the .16 profile must remain byte-identical to published checkpoints",
        );
    }

    #[test]
    fn verified_epoch_65_v14_checkpoint_migrates_before_first_cpi() {
        const EPOCH_65_LAST_SLOT: u64 = 28_511_999;
        assert!(EPOCH_65_LAST_SLOT < FIRST_PDA_OR_CPI_SLOT);
        let previous = convert_v2_to_previous_stake_merge_runtime(
            checkpoint_at_mainnet_epoch_terminal(EPOCH_65_LAST_SLOT),
        );

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();

        assert_eq!(metadata.cursor.last_slot, EPOCH_65_LAST_SLOT);
        assert_eq!(restored.outcome.epoch, 65);
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v13_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn epoch_77_v16_checkpoint_is_rejected_after_first_cpi() {
        const EPOCH_77_LAST_SLOT: u64 = 33_695_999;
        assert!(EPOCH_77_LAST_SLOT >= FIRST_PDA_OR_CPI_SLOT);
        let previous = convert_v2_to_previous_immutable_cpi_runtime(
            checkpoint_at_mainnet_epoch_terminal(EPOCH_77_LAST_SLOT),
        );

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first CPI immutable-account-metadata boundary"
            )
        );
    }

    #[test]
    fn exhausted_pre_activation_v1_checkpoint_migrates_and_reencodes_as_v2() {
        assert_eq!(
            legacy_runtime_profile_v1_sha256(),
            [
                0x9a, 0xba, 0x4f, 0x20, 0x74, 0x48, 0xdf, 0x43, 0xff, 0x36, 0x4b, 0xe5, 0x8a, 0xd2,
                0x33, 0x83, 0xf1, 0xc2, 0x25, 0x46, 0x15, 0x67, 0x7c, 0x4a, 0xca, 0xeb, 0xa2, 0x2a,
                0x79, 0xb6, 0x9e, 0x59,
            ],
            "the migration profile must remain byte-identical to published v1 checkpoints"
        );
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let legacy = convert_v2_to_legacy_v1(replay.encode_frozen_checkpoint().unwrap());

        let (restored, metadata) = LaunchReplay::restore_frozen_checkpoint(&legacy, false).unwrap();
        assert_eq!(restored.outcome.bpf_loader_mutations, 0);
        assert!(restored.bpf_program_cache.is_empty());
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            legacy_runtime_profile_v1_sha256()
        );

        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            u16::from_le_bytes(migrated[8..10].try_into().unwrap()),
            CHECKPOINT_VERSION
        );
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
        let payload_end = migrated.len() - CHECKSUM_LEN;
        assert_eq!(
            u64::from_le_bytes(migrated[payload_end - 8..payload_end].try_into().unwrap()),
            0
        );
    }

    #[test]
    fn pre_withdraw_v2_checkpoint_migrates_and_reencodes_with_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous = convert_v2_to_previous_runtime(replay.encode_frozen_checkpoint().unwrap());

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v2_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_v2_profile_at_first_withdraw_slot_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous =
            convert_v2_to_previous_runtime(replay.encode_frozen_checkpoint().unwrap());
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_WITHDRAW_NONCE_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first nonce withdrawal"
            )
        );
    }

    #[test]
    fn pre_pda_v2_checkpoint_migrates_and_reencodes_with_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous =
            convert_v2_to_previous_pda_runtime(replay.encode_frozen_checkpoint().unwrap());

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v3_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_pda_profile_at_first_syscall_slot_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous =
            convert_v2_to_previous_pda_runtime(replay.encode_frozen_checkpoint().unwrap());
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_PDA_OR_CPI_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first PDA/CPI syscall"
            )
        );
    }

    #[test]
    fn pre_outcome_v2_checkpoint_migrates_and_reencodes_with_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous =
            convert_v2_to_previous_outcome_runtime(replay.encode_frozen_checkpoint().unwrap());

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v4_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_outcome_profile_at_first_authoritative_slot_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous =
            convert_v2_to_previous_outcome_runtime(replay.encode_frozen_checkpoint().unwrap());
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_AUTHORITATIVE_OUTCOME_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first Compact transaction with an authoritative outcome"
            )
        );
    }

    #[test]
    fn pre_vote_signature_v2_checkpoint_migrates_and_reencodes_with_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous = convert_v2_to_previous_vote_signature_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v5_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_vote_signature_profile_at_first_affected_slot_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous = convert_v2_to_previous_vote_signature_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches Vote InitializeAccount node-signature activation"
            )
        );
    }

    #[test]
    fn every_older_profile_is_rejected_at_vote_signature_activation() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let current = replay.encode_frozen_checkpoint().unwrap();
        let cursor_start = HEADER_LEN + 3 * 32;

        for mut previous in [
            convert_v2_to_previous_runtime(current.clone()),
            convert_v2_to_previous_pda_runtime(current.clone()),
            convert_v2_to_previous_outcome_runtime(current.clone()),
        ] {
            previous[cursor_start..cursor_start + 8]
                .copy_from_slice(&INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT.to_le_bytes());
            reseal(&mut previous);
            assert_eq!(
                LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
                LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                    "checkpoint reaches Vote InitializeAccount node-signature activation"
                )
            );
        }

        let mut legacy = convert_v2_to_legacy_v1(current);
        legacy[cursor_start..cursor_start + 8]
            .copy_from_slice(&INITIALIZE_NODE_SIGNER_ACTIVATION_SLOT.to_le_bytes());
        reseal(&mut legacy);
        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&legacy, false).unwrap_err(),
            LaunchCheckpointError::UnsafeLegacyMigration(
                "checkpoint reaches Vote InitializeAccount node-signature activation"
            )
        );
    }

    #[test]
    fn pre_stake_authorize_v2_checkpoint_migrates_and_reencodes_with_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous = convert_v2_to_previous_stake_authorize_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v6_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_stake_authorize_profile_at_first_affected_slot_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous = convert_v2_to_previous_stake_authorize_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&STAKE_AUTHORIZE_LOCKUP_REMOVAL_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches Stake Authorize lockup-removal activation"
            )
        );
    }

    #[test]
    fn pre_balance_projection_v2_checkpoint_migrates_and_reencodes_with_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous = convert_v2_to_previous_balance_projection_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v7_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_balance_profile_at_first_projection_slot_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous = convert_v2_to_previous_balance_projection_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_AUTHORITATIVE_OUTCOME_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first Compact writable post-balance projection"
            )
        );
    }

    #[test]
    fn pre_epoch_40_system_checkpoint_migrates_and_reencodes_with_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous =
            convert_v2_to_previous_system_runtime(replay.encode_frozen_checkpoint().unwrap());

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v8_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_system_profile_at_epoch_40_activation_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous =
            convert_v2_to_previous_system_runtime(replay.encode_frozen_checkpoint().unwrap());
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_NEW_SYSTEM_PROCESSOR_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches Stable epoch-40 System processor activation"
            )
        );
    }

    #[test]
    fn pre_prefunded_create_anomaly_checkpoint_migrates_to_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous = convert_v2_to_previous_prefunded_create_recovery_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v9_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_profile_at_prefunded_create_anomaly_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous = convert_v2_to_previous_prefunded_create_recovery_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_PREFUNDED_CREATE_STATUS_ANOMALY_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first prefunded CreateAccount status anomaly"
            )
        );
    }

    #[test]
    fn pre_vote_commission_checkpoint_migrates_to_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous = convert_v2_to_previous_vote_commission_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v10_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_profile_at_vote_commission_instruction_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous = convert_v2_to_previous_vote_commission_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_VOTE_UPDATE_COMMISSION_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first Vote UpdateCommission instruction"
            )
        );
    }

    #[test]
    fn pre_loader_balance_suffix_checkpoint_migrates_to_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous = convert_v2_to_previous_loader_balance_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v11_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_profile_at_loader_balance_suffix_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous = convert_v2_to_previous_loader_balance_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_RUNTIME_LOADER_BALANCE_SUFFIX_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first historical runtime loader balance suffix"
            )
        );
    }

    #[test]
    fn pre_vote_switch_checkpoint_migrates_to_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous =
            convert_v2_to_previous_vote_switch_runtime(replay.encode_frozen_checkpoint().unwrap());

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v12_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_profile_at_vote_switch_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous =
            convert_v2_to_previous_vote_switch_runtime(replay.encode_frozen_checkpoint().unwrap());
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_VOTE_SWITCH_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first VoteSwitch instruction"
            )
        );
    }

    #[test]
    fn pre_stake_merge_checkpoint_migrates_to_current_profile() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous =
            convert_v2_to_previous_stake_merge_runtime(replay.encode_frozen_checkpoint().unwrap());

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v13_sha256()
        );
        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn previous_profile_at_stake_merge_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut previous =
            convert_v2_to_previous_stake_merge_runtime(replay.encode_frozen_checkpoint().unwrap());
        let cursor_start = HEADER_LEN + 3 * 32;
        previous[cursor_start..cursor_start + 8]
            .copy_from_slice(&FIRST_STAKE_MERGE_SLOT.to_le_bytes());
        reseal(&mut previous);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint reaches the first Stake Merge instruction"
            )
        );
    }

    #[test]
    fn v15_checkpoint_migration_prunes_only_reported_balance_hydration() {
        const HYDRATED_A: [u8; 32] = [70; 32];
        const HYDRATED_B: [u8; 32] = [71; 32];
        const STRUCTURAL: [u8; 32] = [72; 32];
        const UNTOUCHED_GENESIS_SHAPE: [u8; 32] = [73; 32];

        let mut genesis = launch_genesis();
        genesis.epoch_schedule.slots_per_epoch = FIRST_AUTHORITATIVE_OUTCOME_SLOT + 100;
        genesis.epoch_schedule.leader_schedule_slot_offset = genesis.epoch_schedule.slots_per_epoch;
        let mut generation = context(2, 0, 91);
        generation.slots_per_epoch = genesis.epoch_schedule.slots_per_epoch;
        generation.first_slot = Some(0);
        generation.last_slot = Some(FIRST_AUTHORITATIVE_OUTCOME_SLOT);
        generation.genesis = Some(genesis.clone());
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        replay.enable_bank_lifecycle();
        process_compact(
            &mut replay,
            &generation,
            0,
            Some(FIRST_AUTHORITATIVE_OUTCOME_SLOT),
            &slot(0, 0, 0, [2; 32], genesis.genesis_hash),
        );
        process_compact(
            &mut replay,
            &generation,
            1,
            None,
            &slot(1, FIRST_AUTHORITATIVE_OUTCOME_SLOT, 0, [3; 32], [2; 32]),
        );

        let balance_account = |lamports| AccountSnapshot {
            lamports,
            owner: SYSTEM_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: Vec::new().into(),
        };
        replay
            .outcome
            .account_state
            .insert(HYDRATED_A, balance_account(11));
        replay
            .outcome
            .account_state
            .insert(HYDRATED_B, balance_account(22));
        replay.outcome.account_state.insert(
            STRUCTURAL,
            AccountSnapshot {
                lamports: 33,
                owner: SYSTEM_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
                data: vec![0; 8].into(),
            },
        );
        replay
            .outcome
            .account_state
            .insert(UNTOUCHED_GENESIS_SHAPE, balance_account(44));
        replay
            .outcome
            .changed_accounts
            .extend([HYDRATED_A, HYDRATED_B, STRUCTURAL]);
        let before_accounts = replay.outcome.account_state.len();
        let before_changed = replay.outcome.changed_accounts.len();
        let previous = convert_v2_to_previous_transient_balance_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );

        let (restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap();

        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            previous_runtime_profile_v14_sha256()
        );
        assert_eq!(restored.outcome.account_state.len(), before_accounts - 2);
        assert_eq!(restored.outcome.changed_accounts.len(), before_changed - 2);
        assert!(!restored.outcome.account_state.contains_key(&HYDRATED_A));
        assert!(!restored.outcome.account_state.contains_key(&HYDRATED_B));
        assert!(restored.outcome.account_state.contains_key(&STRUCTURAL));
        assert!(
            restored
                .outcome
                .account_state
                .contains_key(&UNTOUCHED_GENESIS_SHAPE)
        );
        assert_eq!(
            metadata.account_state_sha256,
            restored.outcome.account_state.canonical_hash()
        );

        let migrated = restored.encode_frozen_checkpoint().unwrap();
        assert!(migrated.len() < previous.len());
        assert_eq!(
            &migrated[HEADER_LEN..HEADER_LEN + 32],
            runtime_profile_sha256().as_slice()
        );
    }

    #[test]
    fn pre_balance_metadata_v15_checkpoint_is_not_pruned_ambiguously() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 92);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let previous = convert_v2_to_previous_transient_balance_runtime(
            replay.encode_frozen_checkpoint().unwrap(),
        );

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&previous, false).unwrap_err(),
            LaunchCheckpointError::UnsafePreviousRuntimeMigration(
                "checkpoint predates the first authoritative Compact balance metadata"
            )
        );
    }

    #[test]
    fn v1_migration_rejects_current_profile_and_unexhausted_generation() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(2, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            Some(1),
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let v2 = replay.encode_frozen_checkpoint().unwrap();

        let wrong_profile = convert_v2_to_v1(v2.clone(), runtime_profile_sha256());
        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&wrong_profile, false).unwrap_err(),
            LaunchCheckpointError::DescriptorMismatch
        );

        let legacy = convert_v2_to_legacy_v1(v2);
        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&legacy, false).unwrap_err(),
            LaunchCheckpointError::UnsafeLegacyMigration("source generation is not exhausted")
        );
    }

    #[test]
    fn v1_migration_rejects_a_pre_activation_bpf_loader_account() {
        let genesis_hash = launch_genesis().genesis_hash;
        let generation = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &generation,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        replay
            .outcome
            .account_state
            .insert(BPF_LOADER_PROGRAM_ID, crate::default_system_account());
        let legacy = convert_v2_to_legacy_v1(replay.encode_frozen_checkpoint().unwrap());

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&legacy, false).unwrap_err(),
            LaunchCheckpointError::UnsafeLegacyMigration(
                "frozen Bank already contains the BPF-loader builtin"
            )
        );
    }

    #[test]
    fn v1_migration_rejects_an_exhausted_epoch_34_bank_without_loader_account() {
        let mut replay = replay();
        process_empty_through_epoch(&mut replay, BPF_LOADER_STABLE_ACTIVATION_EPOCH);
        assert_eq!(replay.outcome.epoch, BPF_LOADER_STABLE_ACTIVATION_EPOCH);
        assert!(
            replay
                .outcome
                .account_state
                .remove(&BPF_LOADER_PROGRAM_ID)
                .is_some(),
            "epoch entry should have activated the loader before this test isolates the epoch guard"
        );
        let legacy = convert_v2_to_legacy_v1(replay.encode_frozen_checkpoint().unwrap());

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&legacy, false).unwrap_err(),
            LaunchCheckpointError::UnsafeLegacyMigration(
                "frozen Bank is at or after BPF-loader activation"
            )
        );
    }

    #[test]
    fn checkpoint_rejects_recent_hash_heights_that_cannot_rebuild_total_order() {
        let mut state = LaunchBankSysvarState::from_genesis(&launch_genesis()).unwrap();
        state.recent_blockhashes.insert(
            [9; 32],
            LaunchRecentBlockhash {
                hash_height: 0,
                fee: 42,
            },
        );
        let mut encoder = Encoder::checkpoint();
        encode_bank_state(&mut encoder, &state).unwrap();
        let bytes = encoder.finish_checkpoint().unwrap();
        let (_, payload) = validated_payload(&bytes).unwrap();
        let mut decoder = Decoder::new(payload);

        assert!(matches!(
            decode_bank_state(&mut decoder),
            Err(LaunchCheckpointError::InvalidField {
                field: "recent blockhashes",
                reason: "hash heights are not unique",
            })
        ));
    }

    #[test]
    fn duplicate_hash_rejection_recovers_to_a_valid_roundtrip_checkpoint() {
        let genesis = launch_genesis();
        let mut accounts = crate::launch_genesis_sysvar_accounts(&genesis)
            .unwrap()
            .into_iter()
            .collect::<MemoryAccountStore>();
        let mut stake_history = LaunchStakeHistory::new();
        let mut state = LaunchBankSysvarState::from_genesis(&genesis).unwrap();
        let hash_a = [2; 32];
        let recovery_hash = [3; 32];

        state
            .begin_slot(
                0,
                0,
                genesis.genesis_hash,
                &mut accounts,
                &mut stake_history,
            )
            .unwrap();
        state.complete_slot(0, hash_a, 0, &mut accounts).unwrap();
        state
            .begin_slot(1, 0, hash_a, &mut accounts, &mut stake_history)
            .unwrap();
        assert!(matches!(
            state.complete_slot(1, hash_a, 0, &mut accounts),
            Err(crate::LaunchSysvarError::DuplicatePohBlockhash { slot: 1 })
        ));
        state
            .complete_slot(1, recovery_hash, 0, &mut accounts)
            .unwrap();
        state
            .validate_frozen_checkpoint(&accounts, &stake_history)
            .unwrap();

        let mut encoder = Encoder::checkpoint();
        encode_bank_state(&mut encoder, &state).unwrap();
        let bytes = encoder.finish_checkpoint().unwrap();
        let (_, payload) = validated_payload(&bytes).unwrap();
        let mut decoder = Decoder::new(payload);
        let restored = decode_bank_state(&mut decoder).unwrap();
        assert!(decoder.is_finished());
        assert_eq!(restored.recent_blockhashes, state.recent_blockhashes);
        assert_eq!(
            restored.recent_blockhash_order,
            state.recent_blockhash_order
        );
        restored
            .validate_frozen_checkpoint(&accounts, &stake_history)
            .unwrap();
    }

    #[test]
    fn exhausted_generation_resume_fails_closed_without_a_path_runner() {
        let genesis_hash = launch_genesis().genesis_hash;
        let slot_0 = slot(0, 0, 0, [2; 32], genesis_hash);
        let epoch_1_slot = slot(0, 2, 0, [4; 32], [2; 32]);
        let source = context(1, 0, 4);
        let mut next = context(1, 2, 7);
        next.epoch = 1;
        next.genesis = None;
        let mut replay = replay();
        process_compact(&mut replay, &source, 0, None, &slot_0);
        let bytes = replay.encode_frozen_checkpoint().unwrap();
        let (mut restored, _) = LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap();
        assert!(matches!(
            restored.process_compact_row(&next, 0, None, &epoch_1_slot, &mut |_| {}),
            Err(LaunchReplayError::ResumeGenerationMismatch { .. })
        ));
        assert_eq!(restored.outcome.last_slot, Some(0));
    }

    #[test]
    fn completed_epoch_boundary_resume_matches_uninterrupted_replay() {
        let genesis_hash = launch_genesis().genesis_hash;
        let source = context(2, 0, 4);
        let slot_0 = slot(0, 0, 0, [2; 32], genesis_hash);
        let slot_1 = slot(1, 1, 0, [3; 32], [2; 32]);
        let mut next = context(2, 2, 7);
        next.epoch = 1;
        next.genesis = None;
        let slot_2 = slot(2, 2, 1, [4; 32], [3; 32]);
        let slot_3 = slot(3, 3, 2, [5; 32], [4; 32]);

        let mut uninterrupted = replay();
        process_compact(&mut uninterrupted, &source, 0, Some(1), &slot_0);
        process_compact(&mut uninterrupted, &source, 1, None, &slot_1);
        process_compact(&mut uninterrupted, &next, 0, Some(3), &slot_2);
        process_compact(&mut uninterrupted, &next, 1, None, &slot_3);

        let mut split = replay();
        process_compact(&mut split, &source, 0, Some(1), &slot_0);
        process_compact(&mut split, &source, 1, None, &slot_1);
        let epoch_zero_bytes = split.encode_frozen_checkpoint().unwrap();
        let (mut restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&epoch_zero_bytes, false).unwrap();
        assert_eq!(metadata.cursor.next_row, source.block_count);
        assert_eq!(metadata.cursor.next_slot, None);
        restored
            .attach_completed_checkpoint_generation(&source)
            .unwrap();
        process_compact(&mut restored, &next, 0, Some(3), &slot_2);
        process_compact(&mut restored, &next, 1, None, &slot_3);

        let uninterrupted_bytes = uninterrupted.encode_frozen_checkpoint().unwrap();
        let restored_bytes = restored.encode_frozen_checkpoint().unwrap();
        assert_eq!(uninterrupted_bytes, restored_bytes);
        let (mut invalid_epoch, _) =
            LaunchReplay::restore_frozen_checkpoint(&uninterrupted_bytes, false).unwrap();
        invalid_epoch.outcome.epoch = 0;
        assert!(matches!(
            invalid_epoch.encode_frozen_checkpoint(),
            Err(LaunchCheckpointError::RuntimeInvariant(message))
                if message.contains("reported epoch")
        ));
        assert_outcomes_match(&uninterrupted.finish(), &restored.finish());
    }

    #[test]
    fn completed_generation_attachment_rejects_a_wrong_anchor_without_consuming_guard() {
        let genesis_hash = launch_genesis().genesis_hash;
        let source = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &source,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let bytes = replay.encode_frozen_checkpoint().unwrap();
        let (mut restored, _) = LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap();

        let mut wrong = source.clone();
        wrong.binding.generation_digest = [99; 32];
        assert!(matches!(
            restored.attach_completed_checkpoint_generation(&wrong),
            Err(LaunchCheckpointError::InvalidCompletedGeneration(message))
                if message.contains("binding differs")
        ));
        assert!(restored.pending_resume_cursor.is_some());
        restored
            .attach_completed_checkpoint_generation(&source)
            .unwrap();
        assert!(restored.pending_resume_cursor.is_none());
        assert!(restored.pending_resume_descriptor.is_none());
    }

    #[test]
    fn capture_rejects_unfrozen_phase_and_non_compact_execution() {
        let unfrozen = replay();
        assert_eq!(
            unfrozen.encode_frozen_checkpoint(),
            Err(LaunchCheckpointError::BankNotFrozen)
        );

        let genesis_hash = launch_genesis().genesis_hash;
        let mut frozen = replay();
        process_direct(&mut frozen, &slot(0, 0, 0, [2; 32], genesis_hash));
        assert_eq!(
            frozen.encode_frozen_checkpoint(),
            Err(LaunchCheckpointError::MissingCompactCursor)
        );
    }

    #[test]
    fn checksum_truncation_descriptor_and_cursor_corruption_fail_closed() {
        let genesis_hash = launch_genesis().genesis_hash;
        let slot_0 = slot(0, 0, 0, [2; 32], genesis_hash);
        let context = context(2, 0, 4);
        let mut replay = replay();
        process_compact(&mut replay, &context, 0, Some(1), &slot_0);
        let bytes = replay.encode_frozen_checkpoint().unwrap();

        let mut corrupt = bytes.clone();
        corrupt[HEADER_LEN + 5] ^= 1;
        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&corrupt, false).unwrap_err(),
            LaunchCheckpointError::ChecksumMismatch
        );
        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&bytes[..bytes.len() - 1], false).unwrap_err(),
            LaunchCheckpointError::InvalidPayloadLength
        );

        let mut wrong_profile = bytes.clone();
        wrong_profile[HEADER_LEN] ^= 1;
        reseal(&mut wrong_profile);
        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&wrong_profile, false).unwrap_err(),
            LaunchCheckpointError::DescriptorMismatch
        );

        let (mut restored, _) = LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap();
        assert!(matches!(
            restored.process_compact_row(&context, 0, Some(1), &slot_0, &mut |_| {}),
            Err(LaunchReplayError::ResumeRowMismatch {
                expected: 1,
                found: 0
            })
        ));
        assert!(matches!(
            restored.process_compact_row(&context, 1, None, &slot_0, &mut |_| {}),
            Err(LaunchReplayError::ResumeSlotMismatch {
                expected: 1,
                found: 0
            })
        ));
    }

    #[test]
    fn authenticated_runtime_corruption_is_rejected_by_invariants() {
        let genesis_hash = launch_genesis().genesis_hash;
        let context = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &context,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut bytes = replay.encode_frozen_checkpoint().unwrap();
        // Payload begins with 96 descriptor bytes and 25 cursor bytes for a
        // `None` next slot. The following byte is the Vote program id.
        let vote_program_offset = HEADER_LEN + 96 + 8 + 8 + 8 + 1;
        bytes[vote_program_offset] ^= 1;
        reseal(&mut bytes);
        assert!(matches!(
            LaunchReplay::restore_frozen_checkpoint(&bytes, false),
            Err(LaunchCheckpointError::InvalidField {
                field: "runtime program ids",
                ..
            })
        ));
    }

    #[test]
    fn tiny_authenticated_oversized_count_fails_before_allocation() {
        let genesis_hash = launch_genesis().genesis_hash;
        let context = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &context,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let bytes = replay.encode_frozen_checkpoint().unwrap();

        // descriptor + `None` cursor + four program ids + replay timing.
        let stake_count_offset = HEADER_LEN + 96 + 25 + 4 * 32 + 8 + 16 + 8;
        let mut tiny = bytes[..stake_count_offset + 8].to_vec();
        tiny[stake_count_offset..stake_count_offset + 8]
            .copy_from_slice(&(MAX_STAKE_HISTORY_ENTRIES + 1).to_le_bytes());
        let payload_len = u64::try_from(tiny.len() - HEADER_LEN).unwrap();
        tiny[12..20].copy_from_slice(&payload_len.to_le_bytes());
        let checksum = checkpoint_checksum(&tiny);
        tiny.extend_from_slice(&checksum);

        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&tiny, false).unwrap_err(),
            LaunchCheckpointError::DecodeBound {
                field: "StakeHistory"
            }
        );
    }

    #[test]
    fn authenticated_fee_governor_corruption_is_semantically_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let context = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &context,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut bytes = replay.encode_frozen_checkpoint().unwrap();
        let payload_end = bytes.len() - CHECKSUM_LEN;
        let mut decoder = Decoder::new(&bytes[HEADER_LEN..payload_end]);
        decode_descriptor(&mut decoder).unwrap();
        decode_cursor(&mut decoder).unwrap();
        decoder.take(4 * 32).unwrap();
        decoder.i64().unwrap();
        decoder.u128().unwrap();
        decoder.u64().unwrap();
        decode_stake_history(&mut decoder).unwrap();
        decode_genesis(&mut decoder).unwrap();
        let fee_target_offset = HEADER_LEN + decoder.position;
        bytes[fee_target_offset..fee_target_offset + 8].copy_from_slice(&11_000_u64.to_le_bytes());
        reseal(&mut bytes);

        assert!(matches!(
            LaunchReplay::restore_frozen_checkpoint(&bytes, false),
            Err(LaunchCheckpointError::RuntimeInvariant(message))
                if message.contains("fee governor targets")
        ));
    }

    #[test]
    fn authenticated_unsafe_genesis_fee_target_is_rejected() {
        let genesis_hash = launch_genesis().genesis_hash;
        let context = context(1, 0, 4);
        let mut replay = replay();
        process_compact(
            &mut replay,
            &context,
            0,
            None,
            &slot(0, 0, 0, [2; 32], genesis_hash),
        );
        let mut bytes = replay.encode_frozen_checkpoint().unwrap();
        let payload_end = bytes.len() - CHECKSUM_LEN;
        let mut decoder = Decoder::new(&bytes[HEADER_LEN..payload_end]);
        decode_descriptor(&mut decoder).unwrap();
        decode_cursor(&mut decoder).unwrap();
        decoder.take(4 * 32).unwrap();
        decoder.i64().unwrap();
        decoder.u128().unwrap();
        decoder.u64().unwrap();
        decode_stake_history(&mut decoder).unwrap();

        let genesis_start = decoder.position;
        let mut genesis_decoder = Decoder::new(&bytes[HEADER_LEN + genesis_start..payload_end]);
        genesis_decoder.u8().unwrap();
        genesis_decoder.array::<32>().unwrap();
        genesis_decoder.u64().unwrap();
        genesis_decoder.i64().unwrap();
        genesis_decoder.u32().unwrap();
        genesis_decoder.u64().unwrap();
        genesis_decoder.option_u64("slots per segment").unwrap();
        genesis_decoder
            .option_u64("backwards compatibility slot")
            .unwrap();
        genesis_decoder.u64().unwrap();
        genesis_decoder.u32().unwrap();
        genesis_decoder.option_u64("tick count").unwrap();
        genesis_decoder.option_u64("hashes per tick").unwrap();
        let genesis_fee_target_offset = HEADER_LEN + genesis_start + genesis_decoder.position;

        decode_genesis(&mut decoder).unwrap();
        let cached_fee_target_offset = HEADER_LEN + decoder.position;
        for offset in [genesis_fee_target_offset, cached_fee_target_offset] {
            bytes[offset..offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        }
        reseal(&mut bytes);

        assert!(matches!(
            LaunchReplay::restore_frozen_checkpoint(&bytes, false),
            Err(LaunchCheckpointError::InvalidField {
                field: "embedded genesis",
                reason: "launch fee or rent parameters are invalid"
            })
        ));
    }

    #[test]
    fn resumed_epoch_zero_row_rechecks_the_complete_compact_genesis() {
        let genesis_hash = launch_genesis().genesis_hash;
        let slot_0 = slot(0, 0, 0, [2; 32], genesis_hash);
        let slot_1 = slot(1, 1, 0, [3; 32], [2; 32]);
        let context = context(2, 0, 4);
        let mut replay = replay();
        process_compact(&mut replay, &context, 0, Some(1), &slot_0);
        let bytes = replay.encode_frozen_checkpoint().unwrap();
        let (mut restored, _) = LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap();

        let mut changed_context = context.clone();
        changed_context
            .genesis
            .as_mut()
            .unwrap()
            .fees
            .target_lamports_per_sig += 1;
        assert!(matches!(
            restored.process_compact_row(&changed_context, 1, None, &slot_1, &mut |_| {}),
            Err(LaunchReplayError::IncompatibleGeneration { .. })
        ));
        assert_eq!(restored.outcome.last_slot, Some(0));
    }
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            allocation_remaining: MAX_DECODE_ALLOC_BYTES,
        }
    }

    fn is_finished(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], LaunchCheckpointError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(LaunchCheckpointError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(LaunchCheckpointError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn reserve_allocation(
        &mut self,
        field: &'static str,
        bytes: u64,
    ) -> Result<(), LaunchCheckpointError> {
        self.allocation_remaining = self
            .allocation_remaining
            .checked_sub(bytes)
            .ok_or(LaunchCheckpointError::DecodeBound { field })?;
        Ok(())
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], LaunchCheckpointError> {
        self.take(N)?
            .try_into()
            .map_err(|_| LaunchCheckpointError::Truncated)
    }

    fn u8(&mut self) -> Result<u8, LaunchCheckpointError> {
        Ok(self.take(1)?[0])
    }

    fn boolean(&mut self, field: &'static str) -> Result<bool, LaunchCheckpointError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(LaunchCheckpointError::InvalidField {
                field,
                reason: "boolean is not zero or one",
            }),
        }
    }

    fn u32(&mut self) -> Result<u32, LaunchCheckpointError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, LaunchCheckpointError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, LaunchCheckpointError> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn u128(&mut self) -> Result<u128, LaunchCheckpointError> {
        Ok(u128::from_le_bytes(self.array()?))
    }

    fn bounded_count(
        &mut self,
        field: &'static str,
        maximum: u64,
    ) -> Result<usize, LaunchCheckpointError> {
        let count = self.u64()?;
        if count > maximum {
            return Err(LaunchCheckpointError::DecodeBound { field });
        }
        usize::try_from(count).map_err(|_| LaunchCheckpointError::DecodeBound { field })
    }

    fn collection_count(
        &mut self,
        field: &'static str,
        maximum: u64,
        minimum_wire_bytes: u64,
        allocation_bytes: u64,
    ) -> Result<usize, LaunchCheckpointError> {
        let count = self.bounded_count(field, maximum)?;
        let count_u64 =
            u64::try_from(count).map_err(|_| LaunchCheckpointError::DecodeBound { field })?;
        let minimum_wire_total = count_u64
            .checked_mul(minimum_wire_bytes)
            .ok_or(LaunchCheckpointError::DecodeBound { field })?;
        if minimum_wire_total > self.remaining() as u64 {
            return Err(LaunchCheckpointError::Truncated);
        }
        let allocation_total = count_u64
            .checked_mul(allocation_bytes)
            .ok_or(LaunchCheckpointError::DecodeBound { field })?;
        self.reserve_allocation(field, allocation_total)?;
        Ok(count)
    }

    fn vec(&mut self, field: &'static str, maximum: u64) -> Result<Vec<u8>, LaunchCheckpointError> {
        let length = self.bounded_count(field, maximum)?;
        if length > self.remaining() {
            return Err(LaunchCheckpointError::Truncated);
        }
        self.reserve_allocation(
            field,
            u64::try_from(length).map_err(|_| LaunchCheckpointError::DecodeBound { field })?,
        )?;
        Ok(self.take(length)?.to_vec())
    }

    fn string(&mut self, field: &'static str) -> Result<String, LaunchCheckpointError> {
        self.string_bounded(field, MAX_STRING_BYTES)
    }

    fn string_bounded(
        &mut self,
        field: &'static str,
        maximum: u64,
    ) -> Result<String, LaunchCheckpointError> {
        let bytes = self.vec(field, maximum)?;
        String::from_utf8(bytes).map_err(|_| LaunchCheckpointError::InvalidField {
            field,
            reason: "string is not UTF-8",
        })
    }

    fn option_u64(&mut self, field: &'static str) -> Result<Option<u64>, LaunchCheckpointError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(LaunchCheckpointError::InvalidField {
                field,
                reason: "option tag is not zero or one",
            }),
        }
    }

    fn option_u32(&mut self, field: &'static str) -> Result<Option<u32>, LaunchCheckpointError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u32()?)),
            _ => Err(LaunchCheckpointError::InvalidField {
                field,
                reason: "option tag is not zero or one",
            }),
        }
    }

    fn option_f64(&mut self, field: &'static str) -> Result<Option<f64>, LaunchCheckpointError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(f64::from_bits(self.u64()?))),
            _ => Err(LaunchCheckpointError::InvalidField {
                field,
                reason: "option tag is not zero or one",
            }),
        }
    }
}
