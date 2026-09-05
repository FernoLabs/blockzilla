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
use blockzilla_read_sdk::ArchiveV2WireProfile;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AccountSnapshot, CONFIG_PROGRAM_ID, CompactGenesisAccount, CompactGenesisBuiltin,
    CompactGenesisProbe, CompactGenesisSource, LaunchBankSysvarState,
    LaunchDerivedTransactionFailure, LaunchReplay, LaunchReplayFailureLocation,
    LaunchReplayOutcome, LaunchStakeHistory, LaunchStakeHistoryEntry,
    LaunchTransactionFailureReason, MemoryAccountStore, STAKE_PROGRAM_ID, SYSTEM_PROGRAM_ID,
    VOTE_PROGRAM_ID,
    launch_sysvar::{
        CLOCK_SYSVAR_ID, EPOCH_SCHEDULE_SYSVAR_ID, FEES_SYSVAR_ID, LaunchFeeGovernor,
        LaunchRecentBlockhash, RECENT_BLOCKHASHES_SYSVAR_ID, RENT_SYSVAR_ID, SLOT_HISTORY_WORDS,
        STAKE_HISTORY_SYSVAR_ID,
    },
    launch_vote::LaunchVoteStateCache,
};

const CHECKPOINT_MAGIC: [u8; 8] = *b"BZLRCP01";
const CHECKPOINT_VERSION: u16 = 3;
const CHECKPOINT_FLAGS: u16 = 0;
const HEADER_LEN: usize = 8 + 2 + 2 + 8;
const CHECKSUM_LEN: usize = 32;
const MAX_CLUSTER_ID_BYTES: u64 = 64;
const MAX_GENERATION_ID_BYTES: u64 = 256;
const CHECKSUM_DOMAIN: &[u8] = b"blockzilla/launch-frozen-checkpoint/v1\0";
const RUNTIME_PROFILE: &[u8] = b"launch-v1.0.7-bank-sysvars-native-config-system-stake-and-trusted-vote-v1.1.14-bpf-loader-nonce-withdraw-v1.3.3-epoch63-pda-cpi-immutable-account-metadata-and-stake-merge-trusted-compact-outcomes-plus-transient-covered-prebalance-system-accounts-and-structural-system-recoveries-plus-v1.2.32-stable-epoch40-system-and-vote-update-commission-and-vote-switch-plus-historical-loader-balance-suffix-poc/checkpoint-runtime-revision-2026-07-30.17";
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LaunchCheckpointDescriptor {
    pub(crate) runtime_profile_sha256: [u8; 32],
    pub(crate) source: CompactCheckpointSource,
    pub(crate) wire_profile: ArchiveV2WireProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactCheckpointSource {
    pub(crate) cluster_id: String,
    pub(crate) epoch: u64,
    pub(crate) generation_id: String,
    pub(crate) first_slot: Option<u64>,
    pub(crate) slots_per_epoch: u64,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordedCompactCheckpoint {
    pub(crate) source: CompactCheckpointSource,
    pub(crate) wire_profile: ArchiveV2WireProfile,
    pub(crate) cursor: CompactCheckpointCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    #[error("checkpoint is bound to a digest identity format (version {found})")]
    LegacyDigestBoundCheckpoint { found: u16 },
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
            .as_ref()
            .ok_or(LaunchCheckpointError::MissingCompactCursor)?;
        let wire_profile = recorded.wire_profile;
        let descriptor = LaunchCheckpointDescriptor {
            runtime_profile_sha256: runtime_profile_sha256(),
            source: recorded.source.clone(),
            wire_profile,
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
        if bytes.len() < HEADER_LEN {
            return Err(LaunchCheckpointError::Truncated);
        }
        let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        if version == 1 || version == 2 {
            return Err(LaunchCheckpointError::LegacyDigestBoundCheckpoint { found: version });
        }
        if version != CHECKPOINT_VERSION {
            return Err(LaunchCheckpointError::UnsupportedVersion { found: version });
        }
        let (_, payload) = validated_payload(bytes)?;
        let mut decoder = Decoder::new(payload);
        let descriptor = decode_descriptor(&mut decoder)?;
        if descriptor.runtime_profile_sha256 != runtime_profile_sha256() {
            return Err(LaunchCheckpointError::DescriptorMismatch);
        }
        let cursor = decode_cursor(&mut decoder)?;
        validate_cursor(cursor)?;
        let (mut replay, account_state_sha256) =
            decode_replay(&mut decoder, retain_instruction_mutations)?;
        if !decoder.is_finished() {
            return Err(LaunchCheckpointError::TrailingPayload);
        }
        if replay.outcome.last_slot != Some(cursor.last_slot)
            || replay.bank_sysvars.current_slot != cursor.last_slot
        {
            return Err(LaunchCheckpointError::CursorLastSlotMismatch {
                expected: replay.bank_sysvars.current_slot,
                found: cursor.last_slot,
            });
        }
        let metadata_descriptor = descriptor;
        replay.compact_checkpoint = Some(RecordedCompactCheckpoint {
            source: metadata_descriptor.source.clone(),
            wire_profile: metadata_descriptor.wire_profile,
            cursor,
        });
        replay.pending_resume_descriptor = Some(metadata_descriptor.clone());
        replay.pending_resume_cursor = Some(cursor);
        Ok((
            replay,
            FrozenCheckpointMetadata {
                descriptor: metadata_descriptor,
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
        let descriptor = self.pending_resume_descriptor.as_ref().ok_or(
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
        if !source_matches_context(&descriptor.source, context)
            || descriptor.wire_profile != context.binding.wire_profile
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
        if self.compact_checkpoint.as_ref()
            != Some(&RecordedCompactCheckpoint {
                source: descriptor.source.clone(),
                wire_profile: descriptor.wire_profile,
                cursor,
            })
        {
            return Err(LaunchCheckpointError::InvalidCompletedGeneration(
                "restored internal generation record is inconsistent",
            ));
        }
        self.compact_checkpoint = Some(RecordedCompactCheckpoint {
            source: descriptor.source.clone(),
            wire_profile: context.binding.wire_profile,
            cursor,
        });
        self.pending_resume_descriptor = None;
        self.pending_resume_cursor = None;
        Ok(())
    }
}

fn source_matches_context(
    source: &CompactCheckpointSource,
    context: &crate::CompactGenerationContext,
) -> bool {
    source.cluster_id == context.cluster_id
        && source.epoch == context.epoch
        && source.generation_id == context.generation_id
        && source.first_slot == context.first_slot
        && source.slots_per_epoch == context.slots_per_epoch
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
    encoder
        .string_bounded(&value.source.cluster_id, MAX_CLUSTER_ID_BYTES)
        .unwrap();
    encoder.u64(value.source.epoch);
    encoder
        .string_bounded(&value.source.generation_id, MAX_GENERATION_ID_BYTES)
        .unwrap();
    encoder.option_u64(value.source.first_slot);
    encoder.u64(value.source.slots_per_epoch);
    encoder.u8(match value.wire_profile {
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => 0,
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => 1,
    });
}

fn decode_descriptor(
    decoder: &mut Decoder<'_>,
) -> Result<LaunchCheckpointDescriptor, LaunchCheckpointError> {
    let runtime_profile_sha256 = decoder.array()?;
    let cluster_id = decoder.string_bounded("cluster_id", MAX_CLUSTER_ID_BYTES)?;
    let epoch = decoder.u64()?;
    let generation_id = decoder.string_bounded("generation_id", MAX_GENERATION_ID_BYTES)?;
    let first_slot = decoder.option_u64("first_slot")?;
    let slots_per_epoch = decoder.u64()?;
    let wire_profile = match decoder.u8()? {
        0 => ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        1 => ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        _ => {
            return Err(LaunchCheckpointError::InvalidField {
                field: "Archive V2 wire profile",
                reason: "unknown profile tag",
            });
        }
    };
    Ok(LaunchCheckpointDescriptor {
        runtime_profile_sha256,
        source: CompactCheckpointSource {
            cluster_id,
            epoch,
            generation_id,
            first_slot,
            slots_per_epoch,
        },
        wire_profile,
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
    let (outcome, account_state_sha256) = decode_outcome(decoder)?;

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
    // Appended after all fixed outcome fields in version 3.
    encoder.u64(outcome.bpf_loader_mutations);
    Ok(())
}

fn decode_outcome(
    decoder: &mut Decoder<'_>,
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
    let bpf_loader_mutations = decoder.u64()?;
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
    if version != CHECKPOINT_VERSION {
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
    use blockzilla_read_sdk::{ArchiveV2WireProfile, GenerationBinding};

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
                wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
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

    fn assert_legacy_checkpoint_rejected(bytes: &[u8], found: u16) {
        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(bytes, false).unwrap_err(),
            LaunchCheckpointError::LegacyDigestBoundCheckpoint { found }
        );
    }

    fn checkpoint_wire_profile_byte_offset(bytes: &[u8]) -> usize {
        let payload = validated_payload(bytes).unwrap().1;
        let mut offset = 32;
        let cluster_len: usize =
            u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap())
                .try_into()
                .unwrap();
        offset += 8 + cluster_len;
        offset += 8;
        let generation_len: usize =
            u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap())
                .try_into()
                .unwrap();
        offset += 8 + generation_len;
        offset += 1;
        if payload[offset - 1] != 0 {
            offset += 8;
        }
        offset += 8;
        HEADER_LEN + offset
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
        assert_eq!(metadata.descriptor.source.generation_id, "generation-4");
        assert_eq!(metadata.descriptor.source.first_slot, Some(0));
        assert_eq!(metadata.descriptor.source.slots_per_epoch, 2);
        assert_eq!(
            metadata.descriptor.wire_profile,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        );
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
    fn v3_appends_profile_and_roundtrips_the_bpf_loader_mutation_counter() {
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
        let wire_profile_offset = checkpoint_wire_profile_byte_offset(&bytes);
        assert_eq!(bytes[wire_profile_offset], 0);
        let payload_end = bytes.len() - CHECKSUM_LEN;
        assert_eq!(
            u64::from_le_bytes(bytes[payload_end - 8..payload_end].try_into().unwrap()),
            7
        );
        let (restored, metadata) = LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap();
        assert_eq!(restored.outcome.bpf_loader_mutations, 7);
        assert_eq!(
            metadata.descriptor.wire_profile,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        );
        assert_eq!(
            metadata.descriptor.runtime_profile_sha256,
            runtime_profile_sha256()
        );
        assert!(restored.bpf_program_cache.is_empty());
    }

    #[test]
    fn v3_wire_profile_tags_are_stable() {
        let genesis_hash = launch_genesis().genesis_hash;
        for (profile, tag, seed) in [
            (
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                0,
                40,
            ),
            (
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
                1,
                41,
            ),
        ] {
            let mut generation = context(1, 0, seed);
            generation.binding.wire_profile = profile;
            let mut replay = replay();
            process_compact(
                &mut replay,
                &generation,
                0,
                None,
                &slot(0, 0, 0, [2; 32], genesis_hash),
            );

            let bytes = replay.encode_frozen_checkpoint().unwrap();
            let wire_profile_offset = checkpoint_wire_profile_byte_offset(&bytes);
            assert_eq!(bytes[wire_profile_offset], tag);
            let (_, metadata) = LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap();
            assert_eq!(metadata.descriptor.wire_profile, profile);
        }
    }

    #[test]
    fn legacy_v1_checkpoint_is_rejected_without_migration() {
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
        let mut legacy = replay.encode_frozen_checkpoint().unwrap();
        legacy[8..10].copy_from_slice(&1_u16.to_le_bytes());
        reseal(&mut legacy);
        assert_legacy_checkpoint_rejected(&legacy, 1);
    }

    #[test]
    fn legacy_v2_checkpoint_is_rejected_without_migration() {
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
        let mut previous = replay.encode_frozen_checkpoint().unwrap();
        previous[8..10].copy_from_slice(&2_u16.to_le_bytes());
        reseal(&mut previous);
        assert_legacy_checkpoint_rejected(&previous, 2);
    }

    #[test]
    fn invalid_version_is_rejected_as_unsupported() {
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
        let mut bytes = replay.encode_frozen_checkpoint().unwrap();
        bytes[8..10].copy_from_slice(&999_u16.to_le_bytes());
        let payload_len = u64::try_from(bytes.len() - HEADER_LEN - CHECKSUM_LEN).unwrap();
        bytes[12..20].copy_from_slice(&payload_len.to_le_bytes());
        reseal(&mut bytes);
        assert_eq!(
            LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap_err(),
            LaunchCheckpointError::UnsupportedVersion { found: 999 }
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
        wrong.generation_id = "wrong-generation-id".to_owned();
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
    fn completed_generation_attachment_rejects_a_different_wire_profile_for_same_bytes() {
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
        let mut bytes = replay.encode_frozen_checkpoint().unwrap();
        let wire_profile_offset = checkpoint_wire_profile_byte_offset(&bytes);
        assert_eq!(bytes[wire_profile_offset], 0);
        bytes[wire_profile_offset] = 1;
        reseal(&mut bytes);

        let (mut restored, metadata) =
            LaunchReplay::restore_frozen_checkpoint(&bytes, false).unwrap();
        assert_eq!(
            metadata.descriptor.wire_profile,
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        );
        assert!(matches!(
            restored.attach_completed_checkpoint_generation(&source),
            Err(LaunchCheckpointError::InvalidCompletedGeneration(message))
                if message.contains("binding differs")
        ));
        assert!(restored.pending_resume_cursor.is_some());
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
        // Descriptor, cursor and replay header are variable-width.
        // The following byte is the Vote program id.
        let mut decoder = Decoder::new(&bytes[HEADER_LEN..bytes.len() - CHECKSUM_LEN]);
        decode_descriptor(&mut decoder).unwrap();
        decode_cursor(&mut decoder).unwrap();
        let vote_program_offset = HEADER_LEN + decoder.position;
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

        let mut decoder = Decoder::new(&bytes[HEADER_LEN..bytes.len() - CHECKSUM_LEN]);
        decode_descriptor(&mut decoder).unwrap();
        decode_cursor(&mut decoder).unwrap();
        decoder.take(4 * 32).unwrap();
        decoder.i64().unwrap();
        decoder.u128().unwrap();
        decoder.u64().unwrap();
        let stake_count_offset = HEADER_LEN + decoder.position;
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
