//! Replay-first Solana runtime experiments.
//!
//! This crate deliberately starts below a complete SVM.  It owns the pieces we
//! need to make program deployment and account mutation observable before we
//! attempt historical bank replay:
//!
//! - loader-account to canonical ELF extraction;
//! - verifier-gated SBPF loading and native JIT compilation where supported;
//! - a no-protocol-CU execution harness with a host-safety watchdog;
//! - genesis fingerprinting;
//! - launch-era native Config/System/Stake and trusted Vote state mutation;
//! - Bank-created NativeLoader accounts for genesis-declared builtins;
//! - the six sysvar accounts created by the launch genesis Bank;
//! - instruction-boundary, field-level account diffs.

pub mod account_store;
mod checkpoint;
mod checkpoint_file;
pub mod compact;
pub mod compiler;
pub mod conflict_schedule;
pub mod diff;
#[cfg(feature = "genesis")]
pub mod genesis;
pub mod launch_bpf_execute;
pub mod launch_bpf_loader;
pub mod launch_config;
pub mod launch_replay;
pub mod launch_stake;
pub mod launch_system;
pub mod launch_sysvar;
pub mod launch_vote;
#[cfg(target_arch = "aarch64")]
mod native_aarch64;
pub mod program;

pub use account_store::{
    AccountBatchCommit, AccountDataPatch, AccountPubkey, AccountStore, AccountStoreError,
    AccountWrite, AccountWriteBatch, MemoryAccountStore, canonical_account_state_hash,
};
pub use compact::{
    CompactAddressTableLookupProbe, CompactArchiveProbe, CompactArchivedTransactionOutcome,
    CompactGenerationContext, CompactGenesisAccount, CompactGenesisBuiltin, CompactGenesisProbe,
    CompactGenesisSource, CompactInstructionData, CompactInstructionProbe, CompactMessageVersion,
    CompactProbeConfig, CompactProbeError, CompactProbeTotals, CompactRecentBlockhashProbe,
    CompactSlotProbe, CompactTransactionBalanceOracle, CompactTransactionProbe, CompactVisitConfig,
    CompactVisitControl, CompactVisitEvent, CompactVisitSummary, instruction_data_bytes,
    probe_compact_generation, read_compact_generation_context, visit_compact_generation,
    visit_compact_generation_without_program_counts,
};
pub use compiler::{
    CompilationBackend, CompilationManifest, CompiledProgram, CompilerError, ExecutionEngine,
    ExecutionOutcome, ExecutionRequest, ReplayCompiler,
};
pub use diff::{
    AccountData, AccountDiff, AccountSnapshot, ByteRangeDiff, DataDiff, DiffBoundary,
    DiffDisposition, DiffPolicy, InlineByteRangeDiffs, InlineDiffBytes, InlineInstructionPath,
    InstructionDiff, ValueDiff, diff_account_sets,
};
#[cfg(feature = "genesis")]
pub use genesis::{
    EpochWindow, GenesisBuiltinSummary, GenesisFeeSummary, GenesisInflationSummary,
    GenesisRentSummary, GenesisSummary, GenesisSummaryError, MAINNET_BETA_GENESIS_HASH_BASE58,
    bytes_to_hex, pubkey_to_base58, read_genesis_archive_from_file, read_genesis_summary,
    summarize_genesis,
};
pub use launch_bpf_execute::{
    LaunchBpfExecutionError, LaunchBpfExecutionMutation, apply_launch_bpf_program_instruction,
    validate_launch_bpf_program_account,
};
pub use launch_bpf_loader::{
    BPF_LOADER_PROGRAM_ID, LaunchBpfLoaderApply, LaunchBpfLoaderContext, LaunchBpfLoaderError,
    LaunchBpfLoaderMutation, LaunchBpfLoaderProfile, LaunchBpfLoaderRent,
    apply_launch_bpf_loader_instruction,
};
pub use launch_config::{
    CONFIG_PROGRAM_ID, LaunchConfigError, LaunchConfigKey, LaunchConfigMutation,
    apply_launch_config_instruction,
};
pub use launch_replay::{
    LaunchCheckpointPublication, LaunchCheckpointResumeConfig, LaunchDerivedTransactionFailure,
    LaunchDiagnosticReplayChainOutcome, LaunchDiagnosticReplayOutcome, LaunchGenerationMetrics,
    LaunchInstructionEffect, LaunchInstructionMutation, LaunchReplay, LaunchReplayError,
    LaunchReplayFailure, LaunchReplayFailureLocation, LaunchReplayOutcome,
    LaunchRolledBackTransaction, LaunchStreamingReplayOutcome, LaunchTransactionFailureReason,
    resume_launch_chain_diagnostic_from_checkpoint,
    resume_launch_chain_diagnostic_from_checkpoint_with_generation_metrics,
    visit_launch_chain_diagnostic, visit_launch_chain_diagnostic_with_checkpoint,
    visit_launch_chain_diagnostic_with_generation_metrics, visit_launch_prefix,
    visit_launch_prefix_diagnostic, visit_trusted_launch_votes,
    visit_trusted_launch_votes_diagnostic,
};
pub use launch_stake::{
    CLOCK_SYSVAR_ID, LAUNCH_STAKE_ACCOUNT_DATA_LEN, LaunchClock, LaunchDelegation, LaunchStake,
    LaunchStakeAuthorize, LaunchStakeAuthorized, LaunchStakeContext, LaunchStakeError,
    LaunchStakeHistory, LaunchStakeHistoryEntry, LaunchStakeLockup, LaunchStakeMeta,
    LaunchStakeMutation, LaunchStakeState, STAKE_HISTORY_SYSVAR_ID, STAKE_PROGRAM_ID,
    apply_launch_stake_instruction, decode_launch_stake_state, launch_stake_history_entry,
};
pub use launch_system::{
    LAUNCH_NONCE_ACCOUNT_DATA_LEN, LaunchAccountMeta, LaunchSystemAccountMeta, LaunchSystemError,
    LaunchSystemMutation, MAX_ADDRESS_SEED_LEN, MAX_PERMITTED_DATA_LENGTH,
    STABLE_NEW_SYSTEM_PROGRAM_ACTIVATION_EPOCH, SYSTEM_PROGRAM_ID, apply_launch_system_instruction,
    apply_launch_system_instruction_for_epoch, create_address_with_seed, default_system_account,
};
pub use launch_sysvar::{
    EPOCH_SCHEDULE_SYSVAR_ID, FEES_SYSVAR_ID, LaunchBankSysvarState, LaunchBankSysvarUpdate,
    LaunchSysvarError, RECENT_BLOCKHASHES_DATA_LEN, RECENT_BLOCKHASHES_SYSVAR_ID, RENT_SYSVAR_ID,
    REWARDS_SYSVAR_ID, SLOT_HASHES_DATA_LEN, SLOT_HASHES_SYSVAR_ID, SLOT_HISTORY_DATA_LEN,
    SLOT_HISTORY_SYSVAR_ID, STAKE_HISTORY_DATA_LEN, SYSVAR_OWNER_ID,
    launch_genesis_sysvar_accounts,
};
pub use launch_vote::{
    LaunchVoteAuthorize, LaunchVoteError, LaunchVoteMutation, TrustedVoteMutation, VOTE_PROGRAM_ID,
    apply_launch_vote_instruction, apply_trusted_vote_instruction, decode_launch_vote_credits,
};
pub use program::{ExtractedProgram, LoaderAccountKind, ProgramExtractError, extract_program};
