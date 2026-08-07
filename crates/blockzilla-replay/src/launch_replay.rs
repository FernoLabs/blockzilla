//! Minimal launch-era state mutation over ordered Blockzilla compact input.
//!
//! This is deliberately narrower than Bank replay. It materializes the exact
//! serialized genesis accounts, genesis-declared NativeLoader program accounts,
//! and the six sysvars created by the genesis Bank. It applies the launch-era
//! native Config, System, and Stake primitives plus trusted Vote mutations in
//! transaction order. A transaction overlay commits only after every instruction
//! succeeds, so a later failure rolls earlier instruction effects back. The POC
//! still assumes that a fully supported transaction which succeeds under this
//! deliberately narrow model committed historically. It advances every launch
//! Bank sysvar that does not require a Bank hash and freezes SlotHistory after
//! each completed slot. It does not execute historical fee/rent economics;
//! when Compact metadata exists it instead projects writable post balances at
//! transaction boundaries. It does not materialize SlotHashes, validate vote
//! hashes, or compute Bank/account hashes. Those omissions are surfaced in the
//! result profile and must be closed before claiming full Bank parity.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use hashbrown::{HashMap, hash_map::Entry as HashEntry};
use rayon::prelude::*;
use smallvec::SmallVec;
use thiserror::Error;

use crate::compact::visit_compact_generation_without_program_counts;
use crate::diff::AccountDiffJournal;
use crate::launch_vote::{
    LaunchFastVoteApply, LaunchVoteStateCache, apply_launch_vote_instruction_on_overlay,
    apply_launch_vote_instruction_on_overlay_cached, launch_vote_direct_shape_supported,
    launch_vote_direct_wire_supported, try_apply_launch_vote_direct_cached,
    try_apply_launch_vote_direct_cached_lazy,
};
use crate::{
    AccountBatchCommit, AccountMap, AccountSnapshot, AccountStoreError, AccountWriteBatch,
    CowAccountMap,
    BPF_LOADER_PROGRAM_ID, CLOCK_SYSVAR_ID, CONFIG_PROGRAM_ID, CompactArchivedTransactionOutcome,
    CompactGenerationContext, CompactGenesisProbe, CompactGenesisSource, CompactInstructionData,
    CompactInstructionProbe, CompactMessageVersion, CompactProbeError, CompactSlotProbe,
    CompactTransactionProbe, CompactVisitConfig, CompactVisitControl, CompactVisitEvent,
    CompactVisitSummary, CompiledProgram, DiffBoundary, DiffDisposition, DiffPolicy,
    InlineInstructionPath, InstructionDiff, LaunchAccountMeta, LaunchBankSysvarState,
    LaunchBpfExecutionError, LaunchBpfExecutionMutation, LaunchBpfLoaderContext,
    LaunchBpfLoaderError, LaunchBpfLoaderMutation, LaunchBpfLoaderProfile, LaunchBpfLoaderRent,
    LaunchClock, LaunchConfigError, LaunchConfigMutation, LaunchStakeContext, LaunchStakeError,
    LaunchStakeHistory, LaunchStakeMutation, LaunchSystemError, LaunchSystemMutation,
    LaunchSysvarError, LaunchVoteError, LaunchVoteMutation, LoaderAccountKind, MemoryAccountStore,
    ReplayCompiler, SLOT_HISTORY_SYSVAR_ID, STAKE_PROGRAM_ID, SYSTEM_PROGRAM_ID, VOTE_PROGRAM_ID,
    apply_launch_bpf_loader_instruction_on_overlay,
    apply_launch_config_instruction_on_overlay, apply_launch_stake_instruction_on_overlay,
    apply_launch_system_instruction_for_epoch_on_overlay, checkpoint::CompactCheckpointCursor,
    checkpoint::LaunchCheckpointDescriptor, checkpoint::RecordedCompactCheckpoint,
    checkpoint_file::publish_frozen_checkpoint, checkpoint_file::read_trusted_frozen_checkpoint,
    default_system_account, instruction_data_bytes, launch_genesis_sysvar_accounts,
    read_compact_generation_context,
};

const CONFIG_BUILTIN_NAME: &str = "solana_config_program";
const VOTE_BUILTIN_NAME: &str = "solana_vote_program";
const SYSTEM_BUILTIN_NAME: &str = "solana_system_program";
const STAKE_BUILTIN_NAME: &str = "solana_stake_program";
const BPF_LOADER_BUILTIN_NAME: &str = "solana_bpf_loader_program";
const BPF_LOADER_STABLE_ACTIVATION_EPOCH: u64 = 34;
// v1.3.3 enables the PDA and cross-program invocation syscall environment on
// entry to epoch 63. Set this from the slot clock rather than replay outcome
// state so fresh and checkpoint-restored sessions select the same semantics.
const BPF_PDA_AND_CPI_SYSCALL_ACTIVATION_EPOCH: u64 = 63;
// Exact first launch-mainnet Compact row backed by decoded transaction-status
// metadata. From here onward a missing writable account must have a canonical
// pre-balance or replay stops instead of inventing a zero balance.
pub(crate) const FIRST_AUTHORITATIVE_OUTCOME_SLOT: u64 = 4_258_776;

#[inline]
fn bpf_pda_and_cpi_syscalls_supported(epoch: u64) -> bool {
    epoch >= BPF_PDA_AND_CPI_SYSCALL_ACTIVATION_EPOCH
}

/// `NativeLoader1111111111111111111111111111111`.
const NATIVE_LOADER_ID: [u8; 32] = [
    5, 135, 132, 191, 20, 139, 164, 40, 47, 176, 18, 87, 72, 136, 169, 241, 83, 160, 125, 173, 247,
    101, 192, 69, 92, 154, 151, 3, 128, 0, 0, 0,
];

#[derive(Debug)]
pub struct LaunchReplayOutcome {
    pub epoch: u64,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub slots_processed: u64,
    /// Transactions whose full overlay committed in this mutation-only model.
    pub transactions_processed: u64,
    /// Transactions rejected by an implemented launch instruction, plus
    /// authoritative Compact transactions whose archived outcome is failed.
    /// Their overlays were discarded (or never allocated) and replay continued
    /// with the next transaction.
    pub failed_transactions: u64,
    /// Bounded diagnostic evidence for the first derived transaction failure.
    pub first_failed_transaction: Option<LaunchDerivedTransactionFailure>,
    pub instructions_processed: u64,
    pub rolled_back_instructions: u64,
    pub vote_mutations: u64,
    pub config_mutations: u64,
    pub system_mutations: u64,
    pub stake_mutations: u64,
    pub bpf_loader_mutations: u64,
    /// Host-only evidence that the replay executor ran independent direct Vote
    /// transactions on worker threads. These counters are intentionally omitted
    /// from portable Bank checkpoints and reset when a checkpoint is restored.
    pub parallel_vote_batches: u64,
    pub parallel_vote_transactions: u64,
    pub max_parallel_vote_batch: usize,
    /// Sequential lazy-Vote experiment diagnostics. A logical commit advances
    /// decoded Vote state; a materialization writes that state to canonical
    /// account bytes. Both are host-only and reset after checkpoint restore.
    pub lazy_vote_commits: u64,
    pub vote_state_materializations: u64,
    /// Bank-boundary sysvar stores, counted even when a write preserves bytes.
    pub bank_sysvar_writes: u64,
    pub bank_sysvar_accounts_written: BTreeSet<[u8; 32]>,
    /// True after the first child Bank because SlotHashes requires a Bank hash,
    /// not the PoH blockhash carried by Compact.
    pub slot_hashes_unavailable: bool,
    pub changed_accounts: BTreeSet<[u8; 32]>,
    /// The owned-probe API retains every mutation. The streaming API delivers
    /// mutations to its callback and leaves this vector empty.
    pub instruction_mutations: Vec<LaunchInstructionMutation>,
    /// Exact serialized genesis accounts, Bank-created NativeLoader builtin
    /// accounts, non-Bank-hash sysvars, and mutations made by this POC.
    pub account_state: MemoryAccountStore,
}

#[derive(Debug, Clone)]
pub struct LaunchInstructionMutation {
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u32,
    pub effect: LaunchInstructionEffect,
    pub diff: InstructionDiff,
}

/// Controls how many instruction diffs a streaming replay constructs and
/// delivers to its mutation visitor.
///
/// The existing replay entry points use [`Self::All`] so analytical consumers
/// retain their original behavior. CLI/reporting callers that need only a
/// bounded sample can use [`Self::First`]; after that budget is exhausted the
/// runtime skips account before/after snapshots, data hashing, and range-diff
/// construction. A multi-instruction transaction may still retain internal
/// diffs past the sample budget so an eventual hard replay failure can report
/// the exact instructions that were rolled back, but those internal diffs are
/// not delivered to the visitor. [`Self::None`] is the allocation-minimal
/// execution mode: it also disables that diagnostic-only rollback evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchInstructionDiffCapture {
    All,
    First(usize),
    None,
}

#[derive(Debug, Clone, Copy)]
struct LaunchInstructionDiffCaptureState {
    remaining: Option<usize>,
    preserve_hard_failure_rollback: bool,
}

impl LaunchInstructionDiffCaptureState {
    fn new(capture: LaunchInstructionDiffCapture) -> Self {
        Self {
            remaining: match capture {
                LaunchInstructionDiffCapture::All => None,
                LaunchInstructionDiffCapture::First(limit) => Some(limit),
                LaunchInstructionDiffCapture::None => Some(0),
            },
            preserve_hard_failure_rollback: capture != LaunchInstructionDiffCapture::None,
        }
    }

    fn wants_visitor_diff(self) -> bool {
        self.remaining.is_none_or(|remaining| remaining != 0)
    }

    fn record_visitor_diff(&mut self) {
        if let Some(remaining) = &mut self.remaining {
            *remaining = remaining
                .checked_sub(1)
                .expect("a visitor diff is reserved only while budget remains");
        }
    }

    fn preserves_hard_failure_rollback(self) -> bool {
        self.preserve_hard_failure_rollback
    }

    fn is_allocation_minimal(self) -> bool {
        self.remaining == Some(0) && !self.preserve_hard_failure_rollback
    }
}

#[derive(Debug)]
struct PendingCapturedMutation {
    mutation: LaunchInstructionMutation,
    emit_to_visitor: bool,
}

#[derive(Debug, Clone, Copy)]
enum LaunchInstructionKind {
    Vote,
    Config,
    System,
    Stake,
    BpfLoader,
}

enum FastVoteTransactionResult {
    NotEligible,
    Applied {
        vote_account: [u8; 32],
        record_changed_account: bool,
    },
    Failed(LaunchReplayError),
}

// Rayon wakeup and per-batch allocation dominated the first antichain POC on
// real epoch-73 data.  Keep dispatches coarse and independent of the requested
// worker count so A/B worker-count comparisons execute identical windows.
const PARALLEL_VOTE_MAX_WINDOW_TRANSACTIONS: usize = 4_096;
const PARALLEL_VOTE_MIN_WINDOW_TRANSACTIONS: usize = 32;

pub(crate) struct ParallelVoteExecutor {
    workers: usize,
    pool: rayon::ThreadPool,
}

impl std::fmt::Debug for ParallelVoteExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ParallelVoteExecutor")
            .field("workers", &self.workers)
            .finish_non_exhaustive()
    }
}

impl ParallelVoteExecutor {
    fn new(workers: usize) -> Result<Option<Self>, LaunchReplayError> {
        if workers == 0 {
            return Err(LaunchReplayError::InvalidReplayWorkerCount);
        }
        if workers == 1 {
            return Ok(None);
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("blockzilla-replay-{index}"))
            .build()
            .map_err(|error| LaunchReplayError::ParallelExecutor(error.to_string()))?;
        Ok(Some(Self { workers, pool }))
    }

    fn execute(&self, jobs: &mut [ParallelVoteJob<'_>]) {
        self.pool.install(|| {
            jobs.par_iter_mut().for_each(ParallelVoteJob::execute);
        });
    }
}

struct ParallelVoteStep<'a> {
    instruction_data: &'a [u8],
    vote_metas: SmallVec<[LaunchAccountMeta; 8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParallelVoteJobResult {
    Pending,
    Applied,
    Fallback,
}

struct ParallelVoteJob<'a> {
    vote_account: [u8; 32],
    account: AccountSnapshot,
    cache: LaunchVoteStateCache,
    steps: Vec<ParallelVoteStep<'a>>,
    trusted_vote_epoch: u64,
    record_changed_account: bool,
    result: ParallelVoteJobResult,
}

impl ParallelVoteJob<'_> {
    fn execute(&mut self) {
        for step in &self.steps {
            match try_apply_launch_vote_direct_cached(
                step.instruction_data,
                &step.vote_metas,
                &mut self.account,
                self.trusted_vote_epoch,
                &mut self.cache,
            ) {
                Ok(LaunchFastVoteApply::Applied {
                    record_changed_account,
                    ..
                }) => self.record_changed_account |= record_changed_account,
                Ok(LaunchFastVoteApply::Fallback) | Err(_) => {
                    self.result = ParallelVoteJobResult::Fallback;
                    return;
                }
            }
        }
        self.result = ParallelVoteJobResult::Applied;
    }
}

struct ParallelVoteCandidate<'a> {
    transaction_offset: usize,
    vote_account: [u8; 32],
    transaction_metas: TransactionAccountMetaLayout<'a>,
}

struct ParallelVoteGroup<'a> {
    vote_account: [u8; 32],
    last_transaction_offset: usize,
    steps: Vec<ParallelVoteStep<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParallelVoteBatchResult {
    NotEligible,
    Applied(usize),
    Fallback(usize),
}

#[inline]
fn is_parallel_vote_wire_candidate(
    transaction: &CompactTransactionProbe,
    vote_program: [u8; 32],
) -> bool {
    if transaction.archived_outcome != CompactArchivedTransactionOutcome::Succeeded
        || transaction.version != CompactMessageVersion::Legacy
    {
        return false;
    }
    let [instruction] = transaction.instructions.as_slice() else {
        return false;
    };
    if instruction.program_id != vote_program
        || u16::try_from(instruction.instruction_index).is_err()
    {
        return false;
    }
    let CompactInstructionData::Raw(instruction_data) = &instruction.data else {
        return false;
    };
    launch_vote_direct_wire_supported(instruction_data)
}

#[derive(Debug, Clone, Copy)]
enum BankSysvarWritePhase {
    Child { epoch_transition: bool },
    SlotHistory,
}

#[derive(Debug, Default, Clone, Copy)]
struct PendingInstructionCounts {
    total: u64,
    vote: u64,
    config: u64,
    system: u64,
    stake: u64,
    bpf_loader: u64,
}

impl PendingInstructionCounts {
    fn record(&mut self, kind: LaunchInstructionKind) {
        self.total += 1;
        match kind {
            LaunchInstructionKind::Vote => self.vote += 1,
            LaunchInstructionKind::Config => self.config += 1,
            LaunchInstructionKind::System => self.system += 1,
            LaunchInstructionKind::Stake => self.stake += 1,
            LaunchInstructionKind::BpfLoader => self.bpf_loader += 1,
        }
    }
}

#[derive(Debug, Clone)]
pub enum LaunchInstructionEffect {
    Vote {
        vote_account: [u8; 32],
        mutation: LaunchVoteMutation,
    },
    Config(LaunchConfigMutation),
    System(LaunchSystemMutation),
    Stake(LaunchStakeMutation),
    BpfLoader(LaunchBpfLoaderMutation),
    BpfProgram(LaunchBpfExecutionMutation),
}

/// Result of bounded-memory compact replay.
///
/// `replay.instruction_mutations` is empty because each mutation was delivered
/// to the callback before the decoded slot was dropped.
#[derive(Debug)]
pub struct LaunchStreamingReplayOutcome {
    pub context: CompactGenerationContext,
    pub replay: LaunchReplayOutcome,
    pub compact_visit: CompactVisitSummary,
}

/// Result of diagnostic streaming replay.
///
/// Unlike [`visit_launch_prefix`], a replay-semantic failure discovered
/// while processing a slot is returned in `failure` alongside the committed
/// prefix. Compact open/decode failures and genesis initialization failures
/// remain `Err` because they do not identify an unsupported runtime
/// instruction. `compact_visit` includes the decoded slot containing the
/// failure, while `replay` contains only transactions committed before it.
/// The replay's slot range/count includes only fully processed slots; its
/// transaction, instruction, and mutation counters include the committed
/// transaction prefix, which can extend into the failing slot.
#[derive(Debug)]
pub struct LaunchDiagnosticReplayOutcome {
    pub context: CompactGenerationContext,
    pub replay: LaunchReplayOutcome,
    pub compact_visit: CompactVisitSummary,
    pub failure: Option<LaunchReplayFailure>,
}

/// Result of replaying several ordered Compact generations through one Bank.
///
/// The first generation initializes genesis exactly once. Every later
/// generation is identity-checked and its first decoded Bank must link to the
/// previously completed Bank through the normal parent-slot and PoH-hash
/// checks. This is the fast path for sharded epoch input; it never reinitializes
/// canonical accounts between generation directories.
#[derive(Debug)]
pub struct LaunchDiagnosticReplayChainOutcome {
    pub contexts: Vec<CompactGenerationContext>,
    /// Validated completed source generation when this invocation resumed from
    /// a frozen checkpoint. Its block rows are not included in `compact_visit`.
    pub checkpoint_source: Option<CompactGenerationContext>,
    /// Durable checkpoints published at fully exhausted generation boundaries.
    pub checkpoint_publications: Vec<LaunchCheckpointPublication>,
    pub replay: LaunchReplayOutcome,
    pub compact_visit: CompactVisitSummary,
    pub failure: Option<LaunchReplayFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchCheckpointPublication {
    pub path: PathBuf,
    pub epoch: u64,
    pub last_slot: u64,
    pub generation_digest: [u8; 32],
    pub account_state_sha256: [u8; 32],
    /// Standard SHA-256 over every byte of the published checkpoint file.
    pub checkpoint_file_sha256: [u8; 32],
}

/// Host-local timing for one fully consumed sealed Compact generation.
///
/// These measurements are optional observability only. They are never part of
/// replay state, the runtime profile, or a frozen checkpoint. `compact_visit`
/// includes the nested replay callbacks; `compact_decode_visit` is the
/// remaining archive open/read/decode/visitor time after subtracting them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchGenerationMetrics {
    pub epoch: u64,
    pub generation_id: String,
    pub generation_digest: [u8; 32],
    pub first_slot: u64,
    pub last_slot: u64,
    pub slots_visited: u64,
    pub transactions_visited: u64,
    pub instructions_visited: u64,
    /// Logical compressed bytes from visited `blocks.bin` frames. Archive
    /// index, registry, manifest, and genesis-sidecar traffic is excluded.
    pub compact_compressed_bytes: u64,
    /// Live canonical Bank-account table cardinality at the generation
    /// boundaries. This is not the Compact archive's per-generation pubkey
    /// decode registry.
    pub account_registry_start: usize,
    pub account_registry_end: usize,
    /// Cardinality of the cumulative structural changed-key set at each
    /// generation boundary. This is not a count of writes.
    pub changed_accounts_start: usize,
    pub changed_accounts_end: usize,
    pub committed_transactions: u64,
    pub failed_transactions: u64,
    pub committed_instructions: u64,
    pub rolled_back_instructions: u64,
    /// Generic transaction-overlay batch publications. Direct Vote mutation,
    /// Compact lamport reconciliation, and Bank-owned sysvar writes do not use
    /// this API and are intentionally excluded.
    pub account_batch_commits: u64,
    pub account_batch_inserted: u64,
    pub account_batch_updated: u64,
    pub account_batch_deleted: u64,
    pub account_batch_patched: u64,
    pub account_batch_commit: Duration,
    pub checkpoint_published: bool,
    pub generation_wall: Duration,
    pub compact_visit: Duration,
    pub compact_decode_visit: Duration,
    pub replay: Duration,
    pub checkpoint_encode: Duration,
    pub checkpoint_publish: Duration,
    pub checkpoint_state_hash: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaunchGenerationReplaySnapshot {
    account_registry: usize,
    changed_accounts: usize,
    committed_transactions: u64,
    failed_transactions: u64,
    committed_instructions: u64,
    rolled_back_instructions: u64,
}

impl LaunchGenerationReplaySnapshot {
    #[inline]
    fn capture(replay: &LaunchReplay) -> Self {
        Self {
            account_registry: replay.outcome.account_state.len(),
            changed_accounts: replay.outcome.changed_accounts.len(),
            committed_transactions: replay.outcome.transactions_processed,
            failed_transactions: replay.outcome.failed_transactions,
            committed_instructions: replay.outcome.instructions_processed,
            rolled_back_instructions: replay.outcome.rolled_back_instructions,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LaunchGenerationAccountBatchMetrics {
    commits: u64,
    inserted: u64,
    updated: u64,
    deleted: u64,
    patched: u64,
    duration: Duration,
}

impl LaunchGenerationAccountBatchMetrics {
    #[inline]
    fn record(&mut self, commit: AccountBatchCommit, duration: Duration) {
        self.commits = self.commits.saturating_add(1);
        self.inserted = self.inserted.saturating_add(commit.inserted as u64);
        self.updated = self.updated.saturating_add(commit.updated as u64);
        self.deleted = self.deleted.saturating_add(commit.deleted as u64);
        self.patched = self.patched.saturating_add(commit.patched as u64);
        self.duration = self.duration.saturating_add(duration);
    }
}

#[inline(always)]
fn record_generation_account_batch<M>(
    metrics: &mut LaunchGenerationAccountBatchMetrics,
    commit: AccountBatchCommit,
    duration: Duration,
) where
    M: LaunchGenerationMetricsSink,
{
    if M::ENABLED {
        metrics.record(commit, duration);
    }
}

trait LaunchGenerationMetricsSink {
    const ENABLED: bool;

    fn record(&mut self, metrics: LaunchGenerationMetrics);
}

struct DisabledLaunchGenerationMetrics;

impl LaunchGenerationMetricsSink for DisabledLaunchGenerationMetrics {
    const ENABLED: bool = false;

    #[inline(always)]
    fn record(&mut self, _metrics: LaunchGenerationMetrics) {}
}

struct LaunchGenerationMetricsVisitor<F>(F);

impl<F> LaunchGenerationMetricsSink for LaunchGenerationMetricsVisitor<F>
where
    F: FnMut(&LaunchGenerationMetrics),
{
    const ENABLED: bool = true;

    fn record(&mut self, metrics: LaunchGenerationMetrics) {
        (self.0)(&metrics);
    }
}

#[inline(always)]
fn measure_generation_phase<M, R>(action: impl FnOnce() -> R) -> (R, Duration)
where
    M: LaunchGenerationMetricsSink,
{
    if M::ENABLED {
        let started = Instant::now();
        let result = action();
        (result, started.elapsed())
    } else {
        (action(), Duration::ZERO)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LaunchCheckpointResumeConfig<'a> {
    pub checkpoint_path: &'a Path,
    pub expected_checkpoint_file_sha256: [u8; 32],
    pub completed_generation: &'a Path,
    pub checkpoint_out: Option<&'a Path>,
    /// Total worker threads for the opt-in direct-Vote replay experiment.
    /// `1` preserves the established sequential execution path.
    pub replay_workers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaunchReplayFailureLocation {
    pub slot: u64,
    pub transaction_index: Option<u32>,
    pub instruction_index: Option<u32>,
}

/// First replay-semantic failure encountered by diagnostic streaming replay.
#[derive(Debug)]
pub struct LaunchReplayFailure {
    pub location: LaunchReplayFailureLocation,
    pub error: LaunchReplayError,
    /// Instructions that succeeded inside the failing transaction before a
    /// later instruction failed. Their diffs are marked `RolledBack` and were
    /// never committed to canonical replay state.
    pub rolled_back_transaction: Option<LaunchRolledBackTransaction>,
}

impl LaunchReplayFailure {
    fn at_slot(
        slot: u64,
        error: LaunchReplayError,
        rolled_back_transaction: Option<LaunchRolledBackTransaction>,
    ) -> Self {
        let (transaction_index, instruction_index) = replay_error_position(&error);
        Self {
            location: LaunchReplayFailureLocation {
                slot,
                transaction_index,
                instruction_index,
            },
            error,
            rolled_back_transaction,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LaunchRolledBackTransaction {
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_mutations: Vec<LaunchInstructionMutation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchDerivedTransactionFailure {
    pub location: LaunchReplayFailureLocation,
    pub reason: LaunchTransactionFailureReason,
    pub rolled_back_instructions: u64,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LaunchTransactionFailureReason {
    #[error("launch Config instruction error: {0}")]
    Config(LaunchConfigError),
    #[error("launch System instruction error: {0}")]
    System(LaunchSystemError),
    #[error("launch Stake instruction error: {0}")]
    Stake(LaunchStakeError),
    #[error("launch Vote instruction error: {0}")]
    Vote(String),
    #[error("launch legacy BPF-loader instruction error: {0}")]
    BpfLoader(String),
    #[error("launch legacy BPF program instruction error: {0}")]
    BpfProgram(String),
    /// Stable diagnostic text recovered from a portable checkpoint. It is not
    /// consulted by execution; concrete error variants remain available in an
    /// uninterrupted process until the first checkpoint boundary.
    #[error("{0}")]
    CheckpointRestored(String),
}

#[derive(Debug, Error)]
pub enum LaunchReplayError {
    #[error("replay worker count must be at least one")]
    InvalidReplayWorkerCount,
    #[error("initialize parallel replay executor: {0}")]
    ParallelExecutor(String),
    #[error(
        "slot {slot} tx {transaction_index} violated the parallel direct-Vote eligibility invariant: {message}"
    )]
    ParallelVoteInvariant {
        slot: u64,
        transaction_index: u32,
        message: &'static str,
    },
    #[error("compact generation has no genesis state")]
    MissingGenesis,
    #[error("launch replay requires exact digest-bound genesis.bin, found {0:?}")]
    InexactGenesis(CompactGenesisSource),
    #[error("compact genesis does not declare {VOTE_BUILTIN_NAME}")]
    MissingVoteBuiltin,
    #[error("compact genesis does not declare {CONFIG_BUILTIN_NAME}")]
    MissingConfigBuiltin,
    #[error("compact genesis does not declare {SYSTEM_BUILTIN_NAME}")]
    MissingSystemBuiltin,
    #[error("compact genesis does not declare {STAKE_BUILTIN_NAME}")]
    MissingStakeBuiltin,
    #[error("compact genesis declares unexpected launch System program id {found:?}")]
    UnexpectedSystemBuiltin { found: [u8; 32] },
    #[error("compact genesis declares unexpected launch Stake program id {found:?}")]
    UnexpectedStakeBuiltin { found: [u8; 32] },
    #[error("compact genesis declares unexpected launch Config program id {found:?}")]
    UnexpectedConfigBuiltin { found: [u8; 32] },
    #[error("compact genesis declares unexpected launch Vote program id {found:?}")]
    UnexpectedVoteBuiltin { found: [u8; 32] },
    #[error("launch replay does not yet support a warmup epoch schedule")]
    UnsupportedWarmupEpochSchedule,
    #[error("launch genesis has invalid PoH timing parameters")]
    InvalidGenesisTiming,
    #[error("duplicate genesis account {pubkey:?}")]
    DuplicateGenesisAccount { pubkey: [u8; 32] },
    #[error("launch genesis runtime account {pubkey:?} collides with existing state")]
    DuplicateGenesisRuntimeAccount { pubkey: [u8; 32] },
    #[error("advance launch Bank sysvars: {0}")]
    BankSysvars(#[from] LaunchSysvarError),
    #[error("publish canonical replay account batch: {0}")]
    AccountStore(#[from] AccountStoreError),
    #[error(
        "slot {slot} retains {retained} transactions but the compact block declares {declared}; replay requires every ordered transaction"
    )]
    IncompleteTransactions {
        slot: u64,
        retained: usize,
        declared: u32,
    },
    #[error("slot {slot} tx {transaction_index} uses unsupported message version {version:?}")]
    UnsupportedMessageVersion {
        slot: u64,
        transaction_index: u32,
        version: CompactMessageVersion,
    },
    #[error(
        "slot {slot} tx {transaction_index} has invalid message header: required={required_signatures} readonly_signed={readonly_signed} readonly_unsigned={readonly_unsigned} keys={account_keys}"
    )]
    InvalidMessageHeader {
        slot: u64,
        transaction_index: u32,
        required_signatures: u8,
        readonly_signed: u8,
        readonly_unsigned: u8,
        account_keys: usize,
    },
    #[error("slot {slot} tx {transaction_index} has invalid Compact balance projection: {message}")]
    InvalidCompactBalanceProjection {
        slot: u64,
        transaction_index: u32,
        message: &'static str,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} calls unsupported program {program_id:?}"
    )]
    UnsupportedProgram {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        program_id: [u8; 32],
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} has no resolved vote account"
    )]
    MissingVoteAccount {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} account position {account_position} is unresolved"
    )]
    UnresolvedInstructionAccount {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        account_position: usize,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} references absent vote account {pubkey:?}"
    )]
    AbsentVoteAccount {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        pubkey: [u8; 32],
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} account {pubkey:?} is owned by {owner:?}, expected vote program {vote_program:?}"
    )]
    WrongVoteOwner {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        pubkey: [u8; 32],
        owner: [u8; 32],
        vote_program: [u8; 32],
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} has a compact semantic encoding unsupported by the launch Vote profile"
    )]
    UnsupportedVoteEncoding {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} has a compact encoding unsupported by the launch Config profile"
    )]
    UnsupportedConfigEncoding {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} has a compact encoding unsupported by the launch System profile"
    )]
    UnsupportedSystemEncoding {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} has a compact encoding unsupported by the launch Stake profile"
    )]
    UnsupportedStakeEncoding {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} cannot be represented in a u16 instruction path"
    )]
    InstructionPathIndexOverflow {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} failed launch Vote mutation: {source}"
    )]
    VoteMutation {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        #[source]
        source: LaunchVoteError,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} failed launch Config mutation: {source}"
    )]
    ConfigMutation {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        #[source]
        source: LaunchConfigError,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} failed launch System mutation: {source}"
    )]
    SystemMutation {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        #[source]
        source: LaunchSystemError,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} failed launch Stake mutation: {source}"
    )]
    StakeMutation {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        #[source]
        source: LaunchStakeError,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} failed launch legacy BPF-loader mutation: {source}"
    )]
    BpfLoaderMutation {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        #[source]
        source: LaunchBpfLoaderError,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} cannot load legacy BPF program {program_id:?}: {message}"
    )]
    BpfProgramLoad {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        program_id: [u8; 32],
        message: String,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} has a compact encoding unsupported by legacy BPF execution"
    )]
    UnsupportedBpfEncoding {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
    },
    #[error(
        "slot {slot} tx {transaction_index} instruction {instruction_index} failed legacy BPF program {program_id:?} execution: {source}"
    )]
    BpfProgramExecution {
        slot: u64,
        transaction_index: u32,
        instruction_index: u32,
        program_id: [u8; 32],
        archived_outcome: CompactArchivedTransactionOutcome,
        #[source]
        source: LaunchBpfExecutionError,
    },
    #[error("compact launch replay input failed: {0}")]
    CompactInput(#[from] CompactProbeError),
    #[error("compact visitor did not deliver its generation context")]
    MissingGenerationEvent,
    #[error("compact replay chain contains no generation directories")]
    EmptyGenerationChain,
    #[error("compact generation {generation_id} cannot continue replay: {message}")]
    IncompatibleGeneration {
        generation_id: String,
        message: String,
    },
    #[error("checkpoint cursor has no next Compact row after frozen slot {last_slot}")]
    ResumeCursorExhausted { last_slot: u64 },
    #[error("checkpoint expected Compact row {expected}, found {found}")]
    ResumeRowMismatch { expected: u64, found: u64 },
    #[error("checkpoint expected Compact slot {expected}, found {found}")]
    ResumeSlotMismatch { expected: u64, found: u64 },
    #[error("checkpoint generation binding does not match Compact generation {generation_id}")]
    ResumeGenerationMismatch { generation_id: String },
    #[error("checkpoint expected {expected} Compact rows, generation contains {found}")]
    ResumeBlockCountMismatch { expected: u64, found: u64 },
    #[error("checkpoint restore must resume through an exact Compact index row")]
    ResumeRequiresCompactRow,
    #[error("Compact row {row_number} is outside generation block count {block_count}")]
    InvalidCompactRow { row_number: u64, block_count: u64 },
    #[error("Compact row {row_number} has inconsistent bound-index next-slot evidence")]
    InvalidCompactNextSlot { row_number: u64 },
    #[error("Compact replay expected physical row {expected}, found {found}")]
    CompactRowOrderMismatch { expected: u64, found: u64 },
    #[error("Compact replay expected physical row slot {expected}, found {found}")]
    CompactSlotOrderMismatch { expected: u64, found: u64 },
    #[error("Compact generation changed before the prior physical row cursor was exhausted")]
    CompactGenerationChangedBeforeExhaustion,
    #[error("Compact generation received a row after its physical cursor was exhausted")]
    CompactCursorExhausted,
    #[error("frozen checkpoint operation failed: {0}")]
    Checkpoint(String),
    #[error("launch replay counter overflow")]
    CounterOverflow,
}

/// Replay directly from an Archive V2 generation while retaining only one
/// decoded compact slot at a time.
///
/// The callback receives committed mutations after a successful transaction.
/// It also receives earlier instruction mutations from a transaction rejected
/// by an implemented native instruction, marked `RolledBack`; those changes
/// never enter canonical replay state. Returning from the callback drops the
/// mutation unless the caller persists it. The returned replay outcome keeps
/// account state and counters but intentionally has an empty
/// `instruction_mutations` vector.
pub fn visit_launch_prefix(
    root: impl AsRef<Path>,
    config: CompactVisitConfig,
    mut mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchStreamingReplayOutcome, LaunchReplayError> {
    let mut replay = None::<LaunchReplay>;
    let mut generation_context = None::<CompactGenerationContext>;
    let mut replay_error = None::<LaunchReplayError>;
    let compact_visit = visit_compact_generation_without_program_counts(root, config, |event| {
        let result = match event {
            CompactVisitEvent::Generation(context) => {
                initialize_streaming_replay(&mut replay, &mut generation_context, context)
            }
            CompactVisitEvent::Slot {
                context,
                row_number,
                next_slot,
                slot,
            } => replay
                .as_mut()
                .ok_or(LaunchReplayError::MissingGenerationEvent)
                .and_then(|replay| {
                    replay.process_compact_row(
                        context,
                        row_number,
                        next_slot,
                        slot,
                        &mut mutation_visitor,
                    )
                }),
        };
        match result {
            Ok(()) => Ok(CompactVisitControl::Continue),
            Err(error) => {
                let message = error.to_string();
                replay_error = Some(error);
                Err(CompactProbeError::Visitor(message))
            }
        }
    });
    if let Some(error) = replay_error {
        return Err(error);
    }
    let compact_visit = compact_visit?;
    let replay = replay.ok_or(LaunchReplayError::MissingGenerationEvent)?;
    let context = generation_context.ok_or(LaunchReplayError::MissingGenerationEvent)?;
    Ok(LaunchStreamingReplayOutcome {
        context,
        replay: replay.finish(),
        compact_visit,
    })
}

/// Compatibility name for the original Vote-only POC API.
pub fn visit_trusted_launch_votes(
    root: impl AsRef<Path>,
    config: CompactVisitConfig,
    mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchStreamingReplayOutcome, LaunchReplayError> {
    visit_launch_prefix(root, config, mutation_visitor)
}

/// Replay directly from an Archive V2 generation and preserve the committed
/// prefix when the implemented launch-runtime subset encounters its first
/// unsupported or failing instruction.
///
/// A failure in a transaction discards that transaction's full overlay and
/// pending instruction diffs. Mutations from earlier committed transactions
/// have already reached `mutation_visitor` and remain in `replay.account_state`.
/// The returned replay does not retain mutation objects. On a complete replay,
/// `failure` is `None`. Compact input and genesis setup failures are returned
/// as [`LaunchReplayError`] and retain the fail-closed behavior of the primary
/// API.
pub fn visit_launch_prefix_diagnostic(
    root: impl AsRef<Path>,
    config: CompactVisitConfig,
    mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchDiagnosticReplayOutcome, LaunchReplayError> {
    visit_launch_prefix_diagnostic_with_diff_capture(
        root,
        config,
        LaunchInstructionDiffCapture::All,
        mutation_visitor,
    )
}

/// Diagnostic prefix replay with explicit bounded instruction-diff capture.
///
/// Runtime execution, account commits, failure classification, and counters do
/// not depend on `diff_capture`. Only construction and visitor delivery of
/// analytical mutation diffs is bounded.
pub fn visit_launch_prefix_diagnostic_with_diff_capture(
    root: impl AsRef<Path>,
    config: CompactVisitConfig,
    diff_capture: LaunchInstructionDiffCapture,
    mut mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchDiagnosticReplayOutcome, LaunchReplayError> {
    let mut replay = None::<LaunchReplay>;
    let mut generation_context = None::<CompactGenerationContext>;
    let mut setup_error = None::<LaunchReplayError>;
    let mut failure = None::<LaunchReplayFailure>;
    let mut diff_capture = LaunchInstructionDiffCaptureState::new(diff_capture);
    let mut disabled_account_batch_metrics = LaunchGenerationAccountBatchMetrics::default();
    let compact_visit =
        visit_compact_generation_without_program_counts(root, config, |event| match event {
            CompactVisitEvent::Generation(context) => {
                match initialize_streaming_replay(&mut replay, &mut generation_context, context) {
                    Ok(()) => Ok(CompactVisitControl::Continue),
                    Err(error) => {
                        let message = error.to_string();
                        setup_error = Some(error);
                        Err(CompactProbeError::Visitor(message))
                    }
                }
            }
            CompactVisitEvent::Slot {
                context,
                row_number,
                next_slot,
                slot,
            } => {
                let Some(replay) = replay.as_mut() else {
                    let error = LaunchReplayError::MissingGenerationEvent;
                    let message = error.to_string();
                    setup_error = Some(error);
                    return Err(CompactProbeError::Visitor(message));
                };
                match replay
                    .process_compact_row_with_diff_capture::<DisabledLaunchGenerationMetrics>(
                        context,
                        row_number,
                        next_slot,
                        slot,
                        &mut diff_capture,
                        &mut disabled_account_batch_metrics,
                        &mut mutation_visitor,
                    ) {
                    Ok(()) => Ok(CompactVisitControl::Continue),
                    Err(error) => {
                        let rolled_back_transaction = replay.take_rolled_back_transaction();
                        failure = Some(LaunchReplayFailure::at_slot(
                            slot.slot,
                            error,
                            rolled_back_transaction,
                        ));
                        Ok(CompactVisitControl::Stop)
                    }
                }
            }
        });
    if let Some(error) = setup_error {
        return Err(error);
    }
    let compact_visit = compact_visit?;
    let replay = replay.ok_or(LaunchReplayError::MissingGenerationEvent)?;
    let context = generation_context.ok_or(LaunchReplayError::MissingGenerationEvent)?;
    Ok(LaunchDiagnosticReplayOutcome {
        context,
        replay: replay.finish(),
        compact_visit,
        failure,
    })
}

/// Compatibility name for the original Vote-only POC diagnostic API.
pub fn visit_trusted_launch_votes_diagnostic(
    root: impl AsRef<Path>,
    config: CompactVisitConfig,
    mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchDiagnosticReplayOutcome, LaunchReplayError> {
    visit_launch_prefix_diagnostic(root, config, mutation_visitor)
}

/// Replay ordered Compact generation directories without resetting genesis.
///
/// `config.max_slots` is a global bound across the complete chain. Slot-range
/// bounds are applied to every generation. The runtime's Bank lifecycle is the
/// final continuity authority: a missing real Bank between two inputs fails on
/// the next generation's parent slot or previous PoH blockhash.
pub fn visit_launch_chain_diagnostic(
    roots: &[PathBuf],
    config: CompactVisitConfig,
    mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchDiagnosticReplayChainOutcome, LaunchReplayError> {
    visit_launch_chain_diagnostic_with_diff_capture(
        roots,
        config,
        LaunchInstructionDiffCapture::All,
        mutation_visitor,
    )
}

/// Ordered-generation diagnostic replay with explicit bounded diff capture.
///
/// The capture budget spans the complete generation chain rather than resetting
/// at an epoch boundary.
pub fn visit_launch_chain_diagnostic_with_diff_capture(
    roots: &[PathBuf],
    config: CompactVisitConfig,
    diff_capture: LaunchInstructionDiffCapture,
    mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchDiagnosticReplayChainOutcome, LaunchReplayError> {
    visit_launch_chain_diagnostic_core(
        roots,
        config,
        diff_capture,
        LaunchChainStart::Fresh,
        None,
        DisabledLaunchGenerationMetrics,
        mutation_visitor,
    )
}

/// Replay a fresh ordered chain and atomically refresh `checkpoint_out` after
/// every sealed generation that is consumed through its final bound row.
pub fn visit_launch_chain_diagnostic_with_checkpoint(
    roots: &[PathBuf],
    config: CompactVisitConfig,
    diff_capture: LaunchInstructionDiffCapture,
    checkpoint_out: impl AsRef<Path>,
    mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchDiagnosticReplayChainOutcome, LaunchReplayError> {
    visit_launch_chain_diagnostic_core(
        roots,
        config,
        diff_capture,
        LaunchChainStart::Fresh,
        Some(checkpoint_out.as_ref()),
        DisabledLaunchGenerationMetrics,
        mutation_visitor,
    )
}

/// Replay a fresh ordered chain with opt-in host timing at completed sealed
/// generation boundaries.
///
/// The callback runs immediately after optional checkpoint publication for the
/// boundary. Existing unprofiled entry points use a statically disabled sink,
/// so their hot path performs no clock reads or metrics callbacks.
pub fn visit_launch_chain_diagnostic_with_generation_metrics(
    roots: &[PathBuf],
    config: CompactVisitConfig,
    diff_capture: LaunchInstructionDiffCapture,
    checkpoint_out: Option<&Path>,
    generation_metrics_visitor: impl FnMut(&LaunchGenerationMetrics),
    mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchDiagnosticReplayChainOutcome, LaunchReplayError> {
    visit_launch_chain_diagnostic_core(
        roots,
        config,
        diff_capture,
        LaunchChainStart::Fresh,
        checkpoint_out,
        LaunchGenerationMetricsVisitor(generation_metrics_visitor),
        mutation_visitor,
    )
}

/// Resume from a trusted frozen checkpoint at a completed generation boundary.
///
/// `expected_checkpoint_file_sha256` must be supplied from trusted metadata; a
/// digest stored beside the checkpoint is not an authenticity boundary. The
/// `completed_generation` path is reopened and matched to the checkpoint's
/// exhausted physical cursor before any successor row can mutate the Bank.
pub fn resume_launch_chain_diagnostic_from_checkpoint(
    roots: &[PathBuf],
    config: CompactVisitConfig,
    diff_capture: LaunchInstructionDiffCapture,
    resume: LaunchCheckpointResumeConfig<'_>,
    mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchDiagnosticReplayChainOutcome, LaunchReplayError> {
    resume_launch_chain_diagnostic_from_checkpoint_core(
        roots,
        config,
        diff_capture,
        resume,
        DisabledLaunchGenerationMetrics,
        mutation_visitor,
    )
}

/// Resume a chain with opt-in host timing at each newly completed sealed
/// generation boundary.
pub fn resume_launch_chain_diagnostic_from_checkpoint_with_generation_metrics(
    roots: &[PathBuf],
    config: CompactVisitConfig,
    diff_capture: LaunchInstructionDiffCapture,
    resume: LaunchCheckpointResumeConfig<'_>,
    generation_metrics_visitor: impl FnMut(&LaunchGenerationMetrics),
    mutation_visitor: impl FnMut(&LaunchInstructionMutation),
) -> Result<LaunchDiagnosticReplayChainOutcome, LaunchReplayError> {
    resume_launch_chain_diagnostic_from_checkpoint_core(
        roots,
        config,
        diff_capture,
        resume,
        LaunchGenerationMetricsVisitor(generation_metrics_visitor),
        mutation_visitor,
    )
}

fn resume_launch_chain_diagnostic_from_checkpoint_core<M, F>(
    roots: &[PathBuf],
    config: CompactVisitConfig,
    diff_capture: LaunchInstructionDiffCapture,
    resume: LaunchCheckpointResumeConfig<'_>,
    generation_metrics: M,
    mutation_visitor: F,
) -> Result<LaunchDiagnosticReplayChainOutcome, LaunchReplayError>
where
    M: LaunchGenerationMetricsSink,
    F: FnMut(&LaunchInstructionMutation),
{
    if config.start_slot.is_some() {
        return Err(LaunchReplayError::Checkpoint(
            "checkpoint resume owns the exact next row; start_slot must be omitted".to_owned(),
        ));
    }
    let trusted = read_trusted_frozen_checkpoint(
        resume.checkpoint_path,
        resume.expected_checkpoint_file_sha256,
    )
    .map_err(|error| LaunchReplayError::Checkpoint(error.to_string()))?;
    debug_assert_eq!(trusted.file_sha256, resume.expected_checkpoint_file_sha256);
    let _restored_metadata = trusted.metadata;
    let source = read_compact_generation_context(resume.completed_generation)?;
    let mut replay = trusted.replay;
    replay.parallel_vote_executor = ParallelVoteExecutor::new(resume.replay_workers)?;
    replay.lazy_vote_materialization_enabled =
        resume.replay_workers == 1 && diff_capture == LaunchInstructionDiffCapture::None;
    replay
        .attach_completed_checkpoint_generation(&source)
        .map_err(|error| LaunchReplayError::Checkpoint(error.to_string()))?;
    let identity = LaunchChainIdentity {
        cluster_id: source.cluster_id.clone(),
        slots_per_epoch: replay.slots_per_epoch,
        genesis_hash: replay.bank_sysvars.genesis.genesis_hash,
    };
    visit_launch_chain_diagnostic_core(
        roots,
        config,
        diff_capture,
        LaunchChainStart::Restored {
            replay: Box::new(replay),
            source: Box::new(source),
            identity,
        },
        resume.checkpoint_out,
        generation_metrics,
        mutation_visitor,
    )
}

#[derive(Debug)]
enum LaunchChainStart {
    Fresh,
    Restored {
        replay: Box<LaunchReplay>,
        source: Box<CompactGenerationContext>,
        identity: LaunchChainIdentity,
    },
}

#[derive(Debug, Clone)]
struct LaunchChainIdentity {
    cluster_id: String,
    slots_per_epoch: u64,
    genesis_hash: [u8; 32],
}

fn visit_launch_chain_diagnostic_core<M, F>(
    roots: &[PathBuf],
    config: CompactVisitConfig,
    diff_capture: LaunchInstructionDiffCapture,
    start: LaunchChainStart,
    checkpoint_out: Option<&Path>,
    mut generation_metrics: M,
    mut mutation_visitor: F,
) -> Result<LaunchDiagnosticReplayChainOutcome, LaunchReplayError>
where
    M: LaunchGenerationMetricsSink,
    F: FnMut(&LaunchInstructionMutation),
{
    if roots.is_empty() {
        return Err(LaunchReplayError::EmptyGenerationChain);
    }

    let (mut replay, mut identity, mut previous_context, checkpoint_source) = match start {
        LaunchChainStart::Fresh => (None, None, None, None),
        LaunchChainStart::Restored {
            replay,
            source,
            identity,
        } => {
            let source = *source;
            (
                Some(*replay),
                Some(identity),
                Some(source.clone()),
                Some(source),
            )
        }
    };
    let mut contexts = Vec::<CompactGenerationContext>::with_capacity(roots.len());
    let mut seen_generation_digests = BTreeSet::new();
    if let Some(source) = &checkpoint_source {
        seen_generation_digests.insert(source.binding.generation_digest);
    }
    let mut checkpoint_publications = Vec::new();
    let mut compact_visit = empty_compact_visit_summary();
    let mut remaining_slots = config.max_slots;
    let mut setup_error = None::<LaunchReplayError>;
    let mut failure = None::<LaunchReplayFailure>;
    let mut diff_capture = LaunchInstructionDiffCaptureState::new(diff_capture);

    for root in roots {
        if remaining_slots == Some(0) || failure.is_some() {
            break;
        }
        let generation_started = M::ENABLED.then(Instant::now);
        let mut generation_replay_start = None::<LaunchGenerationReplaySnapshot>;
        let mut generation_account_batches = LaunchGenerationAccountBatchMetrics::default();
        let mut replay_time = Duration::ZERO;
        let (visit, compact_visit_time) = measure_generation_phase::<M, _>(|| {
            visit_compact_generation_without_program_counts(
                root,
                CompactVisitConfig {
                    start_slot: config.start_slot,
                    end_slot_exclusive: config.end_slot_exclusive,
                    max_slots: remaining_slots,
                },
                |event| {
                    let (result, elapsed) = measure_generation_phase::<M, _>(|| match event {
                        CompactVisitEvent::Generation(context) => {
                            let result = if replay.is_some() {
                                match previous_context.as_ref() {
                                    None => Err(LaunchReplayError::MissingGenerationEvent),
                                    Some(previous) => {
                                        if !seen_generation_digests
                                            .insert(context.binding.generation_digest)
                                        {
                                            Err(LaunchReplayError::IncompatibleGeneration {
                                                generation_id: context.generation_id.clone(),
                                                message: "generation digest is already present in the chain"
                                                    .to_owned(),
                                            })
                                        } else {
                                            let identity = identity.as_ref().expect(
                                                "an initialized replay has a chain identity",
                                            );
                                            validate_continuation_context(
                                                identity, previous, context,
                                            )
                                            .and_then(
                                                |()| {
                                                    validate_completed_generation_transition(
                                                        replay.as_ref(),
                                                        previous,
                                                        context,
                                                    )
                                                },
                                            )
                                        }
                                    }
                                }
                            } else {
                                let mut ignored_context = None;
                                initialize_streaming_replay(
                                    &mut replay,
                                    &mut ignored_context,
                                    context,
                                )
                                .map(|()| {
                                    let genesis = context
                                        .genesis
                                        .as_ref()
                                        .expect("successful launch initialization has genesis");
                                    identity = Some(LaunchChainIdentity {
                                        cluster_id: context.cluster_id.clone(),
                                        slots_per_epoch: context.slots_per_epoch,
                                        genesis_hash: genesis.genesis_hash,
                                    });
                                    seen_generation_digests
                                        .insert(context.binding.generation_digest);
                                })
                            };
                            match result {
                                Ok(()) => {
                                    if M::ENABLED {
                                        generation_replay_start = replay
                                            .as_ref()
                                            .map(LaunchGenerationReplaySnapshot::capture);
                                    }
                                    contexts.push(context.clone());
                                    Ok(CompactVisitControl::Continue)
                                }
                                Err(error) => {
                                    let message = error.to_string();
                                    setup_error = Some(error);
                                    Err(CompactProbeError::Visitor(message))
                                }
                            }
                        }
                        CompactVisitEvent::Slot {
                            context,
                            row_number,
                            next_slot,
                            slot,
                        } => {
                            let Some(replay) = replay.as_mut() else {
                                let error = LaunchReplayError::MissingGenerationEvent;
                                let message = error.to_string();
                                setup_error = Some(error);
                                return Err(CompactProbeError::Visitor(message));
                            };
                            match replay.process_compact_row_with_diff_capture::<M>(
                                context,
                                row_number,
                                next_slot,
                                slot,
                                &mut diff_capture,
                                &mut generation_account_batches,
                                &mut mutation_visitor,
                            ) {
                                Ok(()) => Ok(CompactVisitControl::Continue),
                                Err(error) => {
                                    let rolled_back_transaction =
                                        replay.take_rolled_back_transaction();
                                    failure = Some(LaunchReplayFailure::at_slot(
                                        slot.slot,
                                        error,
                                        rolled_back_transaction,
                                    ));
                                    Ok(CompactVisitControl::Stop)
                                }
                            }
                        }
                    });
                    replay_time = replay_time.saturating_add(elapsed);
                    result
                },
            )
        });
        if let Some(error) = setup_error.take() {
            return Err(error);
        }
        let visit = visit?;
        let generation_slots = visit.slots_visited;
        let generation_transactions = visit.transactions_visited;
        let generation_instructions = visit.instructions_visited;
        let generation_compressed_bytes = visit.compressed_bytes_visited;
        if let Some(remaining) = &mut remaining_slots {
            *remaining = remaining
                .checked_sub(visit.slots_visited as usize)
                .ok_or(LaunchReplayError::CounterOverflow)?;
        }
        merge_compact_visit_summary(&mut compact_visit, visit)?;
        let current_context = contexts
            .last()
            .expect("a successful Compact visit records its generation context")
            .clone();
        let exhausted_generation = failure.is_none()
            && replay
                .as_ref()
                .is_some_and(|replay| replay_exhausted_generation(replay, &current_context));
        let mut checkpoint_published = false;
        let mut checkpoint_encode_time = Duration::ZERO;
        let mut checkpoint_publish_time = Duration::ZERO;
        let mut checkpoint_state_hash_time = Duration::ZERO;
        if exhausted_generation && let Some(path) = checkpoint_out {
            let replay_ref = replay
                .as_mut()
                .expect("an exhausted generation has initialized replay state");
            // Keep checkpoint encoding itself immutable and fail-closed by
            // establishing the lazy Vote materialization barrier immediately
            // before both wire encoding and the reported account-state hash.
            replay_ref.materialize_all_vote_state();
            let (bytes, elapsed) =
                measure_generation_phase::<M, _>(|| replay_ref.encode_frozen_checkpoint());
            checkpoint_encode_time = elapsed;
            let bytes = bytes.map_err(|error| LaunchReplayError::Checkpoint(error.to_string()))?;
            let (checkpoint_file_sha256, elapsed) =
                measure_generation_phase::<M, _>(|| publish_frozen_checkpoint(path, &bytes));
            checkpoint_publish_time = elapsed;
            let checkpoint_file_sha256 = checkpoint_file_sha256
                .map_err(|error| LaunchReplayError::Checkpoint(error.to_string()))?;
            let (account_state_sha256, elapsed) = measure_generation_phase::<M, _>(|| {
                replay_ref.outcome.account_state.canonical_hash()
            });
            checkpoint_state_hash_time = elapsed;
            checkpoint_publications.push(LaunchCheckpointPublication {
                path: path.to_path_buf(),
                epoch: current_context.epoch,
                last_slot: current_context
                    .last_slot
                    .expect("an exhausted generation has a final index row"),
                generation_digest: current_context.binding.generation_digest,
                account_state_sha256,
                checkpoint_file_sha256,
            });
            checkpoint_published = true;
        }
        if exhausted_generation && M::ENABLED {
            let generation_wall = generation_started
                .map(|started| started.elapsed())
                .unwrap_or(Duration::ZERO);
            let replay_start = generation_replay_start
                .expect("enabled generation metrics captured the initialized replay boundary");
            let replay_end = LaunchGenerationReplaySnapshot::capture(
                replay
                    .as_ref()
                    .expect("an exhausted generation has initialized replay state"),
            );
            generation_metrics.record(LaunchGenerationMetrics {
                epoch: current_context.epoch,
                generation_id: current_context.generation_id.clone(),
                generation_digest: current_context.binding.generation_digest,
                first_slot: current_context
                    .first_slot
                    .expect("an exhausted generation has a first index row"),
                last_slot: current_context
                    .last_slot
                    .expect("an exhausted generation has a final index row"),
                slots_visited: generation_slots,
                transactions_visited: generation_transactions,
                instructions_visited: generation_instructions,
                compact_compressed_bytes: generation_compressed_bytes,
                account_registry_start: replay_start.account_registry,
                account_registry_end: replay_end.account_registry,
                changed_accounts_start: replay_start.changed_accounts,
                changed_accounts_end: replay_end.changed_accounts,
                committed_transactions: replay_end
                    .committed_transactions
                    .saturating_sub(replay_start.committed_transactions),
                failed_transactions: replay_end
                    .failed_transactions
                    .saturating_sub(replay_start.failed_transactions),
                committed_instructions: replay_end
                    .committed_instructions
                    .saturating_sub(replay_start.committed_instructions),
                rolled_back_instructions: replay_end
                    .rolled_back_instructions
                    .saturating_sub(replay_start.rolled_back_instructions),
                account_batch_commits: generation_account_batches.commits,
                account_batch_inserted: generation_account_batches.inserted,
                account_batch_updated: generation_account_batches.updated,
                account_batch_deleted: generation_account_batches.deleted,
                account_batch_patched: generation_account_batches.patched,
                account_batch_commit: generation_account_batches.duration,
                checkpoint_published,
                generation_wall,
                compact_visit: compact_visit_time,
                compact_decode_visit: compact_visit_time.saturating_sub(replay_time),
                replay: replay_time,
                checkpoint_encode: checkpoint_encode_time,
                checkpoint_publish: checkpoint_publish_time,
                checkpoint_state_hash: checkpoint_state_hash_time,
            });
        }
        previous_context = Some(current_context);
    }
    if remaining_slots == Some(0) && contexts.len() < roots.len() {
        compact_visit.stopped_early = true;
    }

    let replay = replay.ok_or(LaunchReplayError::MissingGenerationEvent)?;
    Ok(LaunchDiagnosticReplayChainOutcome {
        contexts,
        checkpoint_source,
        checkpoint_publications,
        replay: replay.finish(),
        compact_visit,
        failure,
    })
}

fn replay_exhausted_generation(replay: &LaunchReplay, context: &CompactGenerationContext) -> bool {
    context.complete
        && context.last_slot.is_some()
        && replay.compact_checkpoint.is_some_and(|recorded| {
            recorded.generation_digest == context.binding.generation_digest
                && recorded.registry_sha256 == context.binding.registry_sha256
                && recorded.cursor.generation_block_count == context.block_count
                && recorded.cursor.next_row == context.block_count
                && recorded.cursor.next_slot.is_none()
                && context.last_slot == Some(recorded.cursor.last_slot)
        })
}

fn validate_continuation_context(
    identity: &LaunchChainIdentity,
    previous: &CompactGenerationContext,
    next: &CompactGenerationContext,
) -> Result<(), LaunchReplayError> {
    let incompatible = |message: String| LaunchReplayError::IncompatibleGeneration {
        generation_id: next.generation_id.clone(),
        message,
    };
    if next.cluster_id != identity.cluster_id {
        return Err(incompatible(format!(
            "cluster is {}, expected {}",
            next.cluster_id, identity.cluster_id
        )));
    }
    if next.slots_per_epoch != identity.slots_per_epoch {
        return Err(incompatible(format!(
            "slots_per_epoch is {}, expected {}",
            next.slots_per_epoch, identity.slots_per_epoch
        )));
    }
    if next.epoch < previous.epoch || next.epoch > previous.epoch.saturating_add(1) {
        return Err(incompatible(format!(
            "epoch {} does not follow prior epoch {}",
            next.epoch, previous.epoch
        )));
    }
    match (next.epoch, next.genesis.as_ref()) {
        (0, Some(next_genesis)) => {
            if next_genesis.source != CompactGenesisSource::ExactGenesisBin {
                return Err(incompatible(format!(
                    "genesis source is {:?}, expected exact genesis.bin",
                    next_genesis.source
                )));
            }
            if next_genesis.genesis_hash != identity.genesis_hash {
                return Err(incompatible(
                    "genesis hash differs from the chain root".to_owned(),
                ));
            }
        }
        (0, None) => {
            return Err(incompatible(
                "epoch-0 continuation has no exact genesis".to_owned(),
            ));
        }
        (_, Some(_)) => {
            return Err(incompatible(
                "post-genesis generation unexpectedly embeds genesis".to_owned(),
            ));
        }
        (_, None) => {}
    }
    Ok(())
}

fn validate_completed_generation_transition(
    replay: Option<&LaunchReplay>,
    previous: &CompactGenerationContext,
    next: &CompactGenerationContext,
) -> Result<(), LaunchReplayError> {
    let incompatible = |message: &str| LaunchReplayError::IncompatibleGeneration {
        generation_id: next.generation_id.clone(),
        message: message.to_owned(),
    };
    if !previous.complete {
        return Err(incompatible("previous Compact generation is not sealed"));
    }
    let recorded = replay
        .and_then(|replay| replay.compact_checkpoint)
        .ok_or_else(|| incompatible("previous Compact generation has no frozen row cursor"))?;
    if recorded.generation_digest != previous.binding.generation_digest
        || recorded.registry_sha256 != previous.binding.registry_sha256
        || recorded.cursor.generation_block_count != previous.block_count
        || recorded.cursor.next_row != previous.block_count
        || recorded.cursor.next_slot.is_some()
        || previous.last_slot != Some(recorded.cursor.last_slot)
    {
        return Err(incompatible(
            "previous Compact generation was not replayed through its final bound row",
        ));
    }
    Ok(())
}

fn empty_compact_visit_summary() -> CompactVisitSummary {
    CompactVisitSummary {
        slots_visited: 0,
        transactions_visited: 0,
        instructions_visited: 0,
        compressed_bytes_visited: 0,
        stopped_early: false,
        program_instruction_counts: BTreeMap::new(),
    }
}

fn merge_compact_visit_summary(
    aggregate: &mut CompactVisitSummary,
    next: CompactVisitSummary,
) -> Result<(), LaunchReplayError> {
    aggregate.slots_visited = aggregate
        .slots_visited
        .checked_add(next.slots_visited)
        .ok_or(LaunchReplayError::CounterOverflow)?;
    aggregate.transactions_visited = aggregate
        .transactions_visited
        .checked_add(next.transactions_visited)
        .ok_or(LaunchReplayError::CounterOverflow)?;
    aggregate.instructions_visited = aggregate
        .instructions_visited
        .checked_add(next.instructions_visited)
        .ok_or(LaunchReplayError::CounterOverflow)?;
    aggregate.compressed_bytes_visited = aggregate
        .compressed_bytes_visited
        .checked_add(next.compressed_bytes_visited)
        .ok_or(LaunchReplayError::CounterOverflow)?;
    aggregate.stopped_early |= next.stopped_early;
    for (program_id, count) in next.program_instruction_counts {
        let total = aggregate
            .program_instruction_counts
            .entry(program_id)
            .or_default();
        *total = total
            .checked_add(count)
            .ok_or(LaunchReplayError::CounterOverflow)?;
    }
    Ok(())
}

fn initialize_streaming_replay(
    replay: &mut Option<LaunchReplay>,
    generation_context: &mut Option<CompactGenerationContext>,
    context: &CompactGenerationContext,
) -> Result<(), LaunchReplayError> {
    let mut initialized =
        LaunchReplay::from_genesis(context.epoch, context.genesis.as_ref(), false)?;
    initialized.enable_bank_lifecycle();
    *replay = Some(initialized);
    *generation_context = Some(context.clone());
    Ok(())
}

pub(crate) fn same_exact_genesis(left: &CompactGenesisProbe, right: &CompactGenesisProbe) -> bool {
    let left_poh = &left.poh_params;
    let right_poh = &right.poh_params;
    let left_fees = &left.fees;
    let right_fees = &right.fees;
    let left_rent = &left.rent;
    let right_rent = &right.rent;
    let left_inflation = &left.inflation;
    let right_inflation = &right.inflation;
    let left_schedule = &left.epoch_schedule;
    let right_schedule = &right.epoch_schedule;

    left.source == right.source
        && left.genesis_hash == right.genesis_hash
        && left.genesis_bin_len == right.genesis_bin_len
        && left.creation_time_unix == right.creation_time_unix
        && left.cluster_id == right.cluster_id
        && left.ticks_per_slot == right.ticks_per_slot
        && left.slots_per_segment == right.slots_per_segment
        && left.backwards_compat_with_v0_23 == right.backwards_compat_with_v0_23
        && left_poh.tick_duration_secs == right_poh.tick_duration_secs
        && left_poh.tick_duration_nanos == right_poh.tick_duration_nanos
        && left_poh.tick_count == right_poh.tick_count
        && left_poh.hashes_per_tick == right_poh.hashes_per_tick
        && left_fees.target_lamports_per_sig == right_fees.target_lamports_per_sig
        && left_fees.target_sigs_per_slot == right_fees.target_sigs_per_slot
        && left_fees.min_lamports_per_sig == right_fees.min_lamports_per_sig
        && left_fees.max_lamports_per_sig == right_fees.max_lamports_per_sig
        && left_fees.burn_percent == right_fees.burn_percent
        && left_rent.lamports_per_byte_year == right_rent.lamports_per_byte_year
        && left_rent.exemption_threshold.to_bits() == right_rent.exemption_threshold.to_bits()
        && left_rent.burn_percent == right_rent.burn_percent
        && left_inflation.initial.to_bits() == right_inflation.initial.to_bits()
        && left_inflation.terminal.to_bits() == right_inflation.terminal.to_bits()
        && left_inflation.taper.to_bits() == right_inflation.taper.to_bits()
        && left_inflation.foundation.to_bits() == right_inflation.foundation.to_bits()
        && left_inflation.foundation_term.to_bits() == right_inflation.foundation_term.to_bits()
        && left_inflation.padding == right_inflation.padding
        && option_f64_bits_equal(left.inflation_storage, right.inflation_storage)
        && left_schedule.slots_per_epoch == right_schedule.slots_per_epoch
        && left_schedule.leader_schedule_slot_offset == right_schedule.leader_schedule_slot_offset
        && left_schedule.warmup == right_schedule.warmup
        && left_schedule.first_normal_epoch == right_schedule.first_normal_epoch
        && left_schedule.first_normal_slot == right_schedule.first_normal_slot
        && left.accounts == right.accounts
        && left.builtins == right.builtins
        && left.reward_pools == right.reward_pools
}

fn option_f64_bits_equal(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.to_bits() == right.to_bits(),
        (None, None) => true,
        _ => false,
    }
}

#[derive(Debug)]
pub struct LaunchReplay {
    pub(crate) vote_program: [u8; 32],
    pub(crate) config_program: [u8; 32],
    pub(crate) system_program: [u8; 32],
    pub(crate) stake_program: [u8; 32],
    pub(crate) genesis_creation_time: i64,
    pub(crate) ns_per_slot: u128,
    pub(crate) slots_per_epoch: u64,
    pub(crate) stake_history: LaunchStakeHistory,
    pub(crate) bank_sysvars: LaunchBankSysvarState,
    pub(crate) bank_lifecycle_enabled: bool,
    pub(crate) retain_instruction_mutations: bool,
    pub(crate) vote_state_cache: LaunchVoteStateCache,
    pub(crate) parallel_vote_executor: Option<ParallelVoteExecutor>,
    /// Enabled only for Capture::None checkpoint resumes requested with one
    /// worker. Parallel replay intentionally retains eager account writes.
    pub(crate) lazy_vote_materialization_enabled: bool,
    /// Host-local derivative artifacts. Account bytes remain authoritative;
    /// this cache is intentionally absent from portable Bank checkpoints.
    pub(crate) bpf_program_cache: HashMap<[u8; 32], CompiledProgram>,
    pub(crate) bpf_compiler: ReplayCompiler,
    pub(crate) rolled_back_transaction: Option<LaunchRolledBackTransaction>,
    pub(crate) compact_checkpoint: Option<RecordedCompactCheckpoint>,
    pub(crate) pending_resume_descriptor: Option<LaunchCheckpointDescriptor>,
    pub(crate) pending_resume_cursor: Option<CompactCheckpointCursor>,
    pub(crate) outcome: LaunchReplayOutcome,
}

impl LaunchReplay {
    pub(crate) fn from_genesis(
        epoch: u64,
        genesis: Option<&CompactGenesisProbe>,
        retain_instruction_mutations: bool,
    ) -> Result<Self, LaunchReplayError> {
        let genesis = genesis.ok_or(LaunchReplayError::MissingGenesis)?;
        if genesis.source != CompactGenesisSource::ExactGenesisBin {
            return Err(LaunchReplayError::InexactGenesis(genesis.source));
        }
        let vote_program = genesis
            .builtins
            .iter()
            .find(|builtin| builtin.key == VOTE_BUILTIN_NAME)
            .map(|builtin| builtin.pubkey)
            .ok_or(LaunchReplayError::MissingVoteBuiltin)?;
        if vote_program != VOTE_PROGRAM_ID {
            return Err(LaunchReplayError::UnexpectedVoteBuiltin {
                found: vote_program,
            });
        }
        let config_program = genesis
            .builtins
            .iter()
            .find(|builtin| builtin.key == CONFIG_BUILTIN_NAME)
            .map(|builtin| builtin.pubkey)
            .ok_or(LaunchReplayError::MissingConfigBuiltin)?;
        if config_program != CONFIG_PROGRAM_ID {
            return Err(LaunchReplayError::UnexpectedConfigBuiltin {
                found: config_program,
            });
        }
        let system_program = genesis
            .builtins
            .iter()
            .find(|builtin| builtin.key == SYSTEM_BUILTIN_NAME)
            .map(|builtin| builtin.pubkey)
            .ok_or(LaunchReplayError::MissingSystemBuiltin)?;
        if system_program != SYSTEM_PROGRAM_ID {
            return Err(LaunchReplayError::UnexpectedSystemBuiltin {
                found: system_program,
            });
        }
        let stake_program = genesis
            .builtins
            .iter()
            .find(|builtin| builtin.key == STAKE_BUILTIN_NAME)
            .map(|builtin| builtin.pubkey)
            .ok_or(LaunchReplayError::MissingStakeBuiltin)?;
        if stake_program != STAKE_PROGRAM_ID {
            return Err(LaunchReplayError::UnexpectedStakeBuiltin {
                found: stake_program,
            });
        }
        if genesis.epoch_schedule.warmup {
            return Err(LaunchReplayError::UnsupportedWarmupEpochSchedule);
        }
        if genesis.epoch_schedule.slots_per_epoch == 0
            || genesis.poh_params.tick_duration_nanos >= 1_000_000_000
        {
            return Err(LaunchReplayError::InvalidGenesisTiming);
        }
        let ns_per_tick = u128::from(genesis.poh_params.tick_duration_secs)
            .checked_mul(1_000_000_000)
            .and_then(|nanos| nanos.checked_add(u128::from(genesis.poh_params.tick_duration_nanos)))
            .ok_or(LaunchReplayError::InvalidGenesisTiming)?;
        let ns_per_slot = ns_per_tick
            .checked_mul(u128::from(genesis.ticks_per_slot))
            .ok_or(LaunchReplayError::InvalidGenesisTiming)?;

        let runtime_accounts = genesis
            .accounts
            .len()
            .saturating_add(genesis.reward_pools.len())
            .saturating_add(genesis.builtins.len())
            .saturating_add(6);
        let mut account_state = MemoryAccountStore::with_capacity(runtime_accounts);
        for account in genesis.accounts.iter().chain(&genesis.reward_pools) {
            let snapshot = AccountSnapshot {
                lamports: account.lamports,
                owner: account.owner,
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: account.data.clone().into(),
            };
            if account_state.insert(account.pubkey, snapshot).is_some() {
                return Err(LaunchReplayError::DuplicateGenesisAccount {
                    pubkey: account.pubkey,
                });
            }
        }
        // v1.0.7 `Bank::process_genesis_config()` stores every declared native
        // processor after ordinary genesis accounts. Like the historical Bank,
        // this intentionally overwrites a colliding address instead of treating
        // it as another serialized-genesis duplicate.
        for builtin in &genesis.builtins {
            account_state.insert(
                builtin.pubkey,
                native_builtin_account(builtin.key.as_bytes()),
            );
        }
        for (pubkey, account) in launch_genesis_sysvar_accounts(genesis)? {
            if account_state.insert(pubkey, account).is_some() {
                return Err(LaunchReplayError::DuplicateGenesisRuntimeAccount { pubkey });
            }
        }

        let bank_sysvars = LaunchBankSysvarState::from_genesis(genesis)?;
        let vote_state_cache = LaunchVoteStateCache::from_accounts(&account_state, vote_program);
        Ok(Self {
            vote_program,
            config_program,
            system_program,
            stake_program,
            genesis_creation_time: genesis.creation_time_unix,
            ns_per_slot,
            slots_per_epoch: genesis.epoch_schedule.slots_per_epoch,
            stake_history: LaunchStakeHistory::new(),
            bank_sysvars,
            bank_lifecycle_enabled: false,
            retain_instruction_mutations,
            vote_state_cache,
            parallel_vote_executor: None,
            lazy_vote_materialization_enabled: false,
            bpf_program_cache: HashMap::new(),
            bpf_compiler: ReplayCompiler::new(),
            rolled_back_transaction: None,
            compact_checkpoint: None,
            pending_resume_descriptor: None,
            pending_resume_cursor: None,
            outcome: LaunchReplayOutcome {
                epoch,
                first_slot: None,
                last_slot: None,
                slots_processed: 0,
                transactions_processed: 0,
                failed_transactions: 0,
                first_failed_transaction: None,
                instructions_processed: 0,
                rolled_back_instructions: 0,
                vote_mutations: 0,
                config_mutations: 0,
                system_mutations: 0,
                stake_mutations: 0,
                bpf_loader_mutations: 0,
                parallel_vote_batches: 0,
                parallel_vote_transactions: 0,
                max_parallel_vote_batch: 0,
                lazy_vote_commits: 0,
                vote_state_materializations: 0,
                bank_sysvar_writes: 0,
                bank_sysvar_accounts_written: BTreeSet::new(),
                slot_hashes_unavailable: false,
                changed_accounts: BTreeSet::new(),
                instruction_mutations: Vec::new(),
                account_state,
            },
        })
    }

    fn clock_for_slot(&self, slot: u64) -> LaunchClock {
        let elapsed_seconds =
            (u128::from(slot).wrapping_mul(self.ns_per_slot) / 1_000_000_000) as i64;
        LaunchClock {
            slot,
            epoch: slot / self.slots_per_epoch,
            unix_timestamp: self.genesis_creation_time.wrapping_add(elapsed_seconds),
        }
    }

    fn bpf_loader_context(&self) -> LaunchBpfLoaderContext {
        LaunchBpfLoaderContext {
            profile: LaunchBpfLoaderProfile::V1_1_14,
            bank_rent: LaunchBpfLoaderRent {
                lamports_per_byte_year: self.bank_sysvars.genesis.rent.lamports_per_byte_year,
                exemption_threshold: self.bank_sysvars.genesis.rent.exemption_threshold,
            },
        }
    }

    fn bpf_loader_is_active(&self) -> bool {
        self.outcome
            .account_state
            .get(&BPF_LOADER_PROGRAM_ID)
            .is_some_and(|account| {
                account.owner == NATIVE_LOADER_ID
                    && account.executable
                    && account.data == BPF_LOADER_BUILTIN_NAME.as_bytes()
            })
    }

    fn activate_epoch_programs(&mut self, entered_epoch: Option<(u64, u64)>) {
        if entered_epoch.is_some_and(|(_, epoch)| epoch == BPF_LOADER_STABLE_ACTIVATION_EPOCH) {
            // v1.1.14 Stable `get_programs()` activates the legacy BPF loader
            // on entry to epoch 34 through the Bank's native-program path.
            self.outcome.account_state.insert(
                BPF_LOADER_PROGRAM_ID,
                native_builtin_account(BPF_LOADER_BUILTIN_NAME.as_bytes()),
            );
        }
    }

    pub(crate) fn enable_bank_lifecycle(&mut self) {
        self.bank_lifecycle_enabled = true;
    }

    #[cfg(test)]
    pub(crate) fn process_slot(
        &mut self,
        slot: &CompactSlotProbe,
        mutation_visitor: &mut impl FnMut(&LaunchInstructionMutation),
    ) -> Result<(), LaunchReplayError> {
        if self.pending_resume_cursor.is_some() {
            return Err(LaunchReplayError::ResumeRequiresCompactRow);
        }
        let mut diff_capture =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::All);
        self.process_slot_inner(slot, &mut diff_capture, mutation_visitor)
    }

    #[cfg(test)]
    fn process_slot_inner(
        &mut self,
        slot: &CompactSlotProbe,
        diff_capture: &mut LaunchInstructionDiffCaptureState,
        mutation_visitor: &mut impl FnMut(&LaunchInstructionMutation),
    ) -> Result<(), LaunchReplayError> {
        let slot_clock = self.clock_for_slot(slot.slot);
        let mut account_batch_metrics = LaunchGenerationAccountBatchMetrics::default();
        self.process_slot_inner_with_clock::<DisabledLaunchGenerationMetrics>(
            slot,
            slot_clock,
            diff_capture,
            &mut account_batch_metrics,
            mutation_visitor,
        )
    }

    pub(crate) fn process_compact_row(
        &mut self,
        context: &CompactGenerationContext,
        row_number: u64,
        next_slot: Option<u64>,
        slot: &CompactSlotProbe,
        mutation_visitor: &mut impl FnMut(&LaunchInstructionMutation),
    ) -> Result<(), LaunchReplayError> {
        let mut diff_capture =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::All);
        let mut account_batch_metrics = LaunchGenerationAccountBatchMetrics::default();
        self.process_compact_row_with_diff_capture::<DisabledLaunchGenerationMetrics>(
            context,
            row_number,
            next_slot,
            slot,
            &mut diff_capture,
            &mut account_batch_metrics,
            mutation_visitor,
        )
    }

    fn process_compact_row_with_diff_capture<M>(
        &mut self,
        context: &CompactGenerationContext,
        row_number: u64,
        next_slot: Option<u64>,
        slot: &CompactSlotProbe,
        diff_capture: &mut LaunchInstructionDiffCaptureState,
        account_batch_metrics: &mut LaunchGenerationAccountBatchMetrics,
        mutation_visitor: &mut impl FnMut(&LaunchInstructionMutation),
    ) -> Result<(), LaunchReplayError>
    where
        M: LaunchGenerationMetricsSink,
    {
        let next_row = row_number
            .checked_add(1)
            .ok_or(LaunchReplayError::CounterOverflow)?;
        if row_number >= context.block_count {
            return Err(LaunchReplayError::InvalidCompactRow {
                row_number,
                block_count: context.block_count,
            });
        }
        if (next_row < context.block_count) != next_slot.is_some()
            || next_slot.is_some_and(|next| next <= slot.slot)
            || (row_number == 0 && context.first_slot != Some(slot.slot))
        {
            return Err(LaunchReplayError::InvalidCompactNextSlot { row_number });
        }
        let slot_clock = self.clock_for_slot(slot.slot);
        if context.slots_per_epoch != self.slots_per_epoch || context.epoch != slot_clock.epoch {
            return Err(LaunchReplayError::IncompatibleGeneration {
                generation_id: context.generation_id.clone(),
                message: "slot or epoch schedule disagrees with the replay runtime".to_owned(),
            });
        }
        let genesis_matches = match (context.epoch, context.genesis.as_ref()) {
            (0, Some(genesis)) => {
                genesis.source == CompactGenesisSource::ExactGenesisBin
                    && genesis.genesis_hash == self.bank_sysvars.genesis.genesis_hash
                    && genesis.genesis_bin_len == self.bank_sysvars.genesis.genesis_bin_len
            }
            (0, None) => false,
            (_, None) => true,
            (_, Some(_)) => false,
        };
        if !genesis_matches {
            return Err(LaunchReplayError::IncompatibleGeneration {
                generation_id: context.generation_id.clone(),
                message: "embedded genesis does not match the replay runtime".to_owned(),
            });
        }
        if let Some(cursor) = self.pending_resume_cursor {
            let descriptor = self
                .pending_resume_descriptor
                .expect("a restored cursor always has a descriptor");
            if descriptor.generation_digest != context.binding.generation_digest
                || descriptor.registry_sha256 != context.binding.registry_sha256
            {
                return Err(LaunchReplayError::ResumeGenerationMismatch {
                    generation_id: context.generation_id.clone(),
                });
            }
            if cursor.generation_block_count != context.block_count {
                return Err(LaunchReplayError::ResumeBlockCountMismatch {
                    expected: cursor.generation_block_count,
                    found: context.block_count,
                });
            }
            let expected_slot =
                cursor
                    .next_slot
                    .ok_or(LaunchReplayError::ResumeCursorExhausted {
                        last_slot: cursor.last_slot,
                    })?;
            if row_number != cursor.next_row {
                return Err(LaunchReplayError::ResumeRowMismatch {
                    expected: cursor.next_row,
                    found: row_number,
                });
            }
            if slot.slot != expected_slot {
                return Err(LaunchReplayError::ResumeSlotMismatch {
                    expected: expected_slot,
                    found: slot.slot,
                });
            }
            if let Some(compact_genesis) = context.genesis.as_ref()
                && !same_exact_genesis(compact_genesis, &self.bank_sysvars.genesis)
            {
                return Err(LaunchReplayError::IncompatibleGeneration {
                    generation_id: context.generation_id.clone(),
                    message: "complete embedded genesis differs from restored runtime state"
                        .to_owned(),
                });
            }
        } else if let Some(previous) = self.compact_checkpoint {
            let same_digest = previous.generation_digest == context.binding.generation_digest;
            let same_registry = previous.registry_sha256 == context.binding.registry_sha256;
            if same_digest != same_registry {
                return Err(LaunchReplayError::IncompatibleGeneration {
                    generation_id: context.generation_id.clone(),
                    message: "generation digest and registry binding disagree with active replay"
                        .to_owned(),
                });
            }
            let same_generation = same_digest && same_registry;
            if same_generation {
                if row_number != previous.cursor.next_row {
                    return Err(LaunchReplayError::CompactRowOrderMismatch {
                        expected: previous.cursor.next_row,
                        found: row_number,
                    });
                }
                let expected_slot = previous
                    .cursor
                    .next_slot
                    .ok_or(LaunchReplayError::CompactCursorExhausted)?;
                if slot.slot != expected_slot {
                    return Err(LaunchReplayError::CompactSlotOrderMismatch {
                        expected: expected_slot,
                        found: slot.slot,
                    });
                }
            } else {
                if previous.cursor.next_row != previous.cursor.generation_block_count
                    || previous.cursor.next_slot.is_some()
                {
                    return Err(LaunchReplayError::CompactGenerationChangedBeforeExhaustion);
                }
                if row_number != 0 {
                    return Err(LaunchReplayError::CompactRowOrderMismatch {
                        expected: 0,
                        found: row_number,
                    });
                }
            }
        } else if row_number != 0 {
            return Err(LaunchReplayError::CompactRowOrderMismatch {
                expected: 0,
                found: row_number,
            });
        }
        let recorded = RecordedCompactCheckpoint {
            generation_digest: context.binding.generation_digest,
            registry_sha256: context.binding.registry_sha256,
            cursor: CompactCheckpointCursor {
                last_slot: slot.slot,
                next_row,
                generation_block_count: context.block_count,
                next_slot,
            },
        };
        self.process_slot_inner_with_clock::<M>(
            slot,
            slot_clock,
            diff_capture,
            account_batch_metrics,
            mutation_visitor,
        )?;
        if self.lazy_vote_materialization_enabled && next_slot.is_none() {
            // The final physical row is the only place this runner can publish
            // a frozen generation checkpoint. Canonicalize pending Vote data
            // before exposing that checkpoint/hash boundary while allowing
            // unrelated slot boundaries to retain epoch-long logical chains.
            self.materialize_all_vote_state();
        }
        self.compact_checkpoint = Some(recorded);
        self.pending_resume_descriptor = None;
        self.pending_resume_cursor = None;
        Ok(())
    }

    fn try_process_allocation_minimal_vote(
        &mut self,
        slot: u64,
        transaction: &CompactTransactionProbe,
        transaction_metas: &TransactionAccountMetaLayout<'_>,
        trusted_vote_epoch: u64,
    ) -> FastVoteTransactionResult {
        let [instruction] = transaction.instructions.as_slice() else {
            return FastVoteTransactionResult::NotEligible;
        };
        if instruction.program_id != self.vote_program
            || u16::try_from(instruction.instruction_index).is_err()
        {
            return FastVoteTransactionResult::NotEligible;
        }
        let CompactInstructionData::Raw(instruction_data) = &instruction.data else {
            return FastVoteTransactionResult::NotEligible;
        };
        let vote_metas = match instruction_account_metas(
            slot,
            transaction.tx_index,
            instruction,
            transaction_metas,
        ) {
            Ok(metas) => metas,
            Err(error) => return FastVoteTransactionResult::Failed(error),
        };
        let Some(vote_meta) = vote_metas.first() else {
            return FastVoteTransactionResult::NotEligible;
        };

        // The direct mutator writes canonical bytes only after all semantic
        // checks and scratch serialization succeed. It is also the single
        // authority for writable, owner, and duplicate-meta guards, so the
        // account can remain in its canonical hash-table entry without
        // repeating those scans in this caller.
        let Some(vote_account) = self.outcome.account_state.get_mut(&vote_meta.pubkey) else {
            return FastVoteTransactionResult::NotEligible;
        };
        let result =
            if self.lazy_vote_materialization_enabled && self.parallel_vote_executor.is_none() {
                try_apply_launch_vote_direct_cached_lazy(
                    instruction_data,
                    &vote_metas,
                    vote_account,
                    trusted_vote_epoch,
                    &mut self.vote_state_cache,
                )
            } else {
                try_apply_launch_vote_direct_cached(
                    instruction_data,
                    &vote_metas,
                    vote_account,
                    trusted_vote_epoch,
                    &mut self.vote_state_cache,
                )
            };

        match result {
            Ok(LaunchFastVoteApply::Applied {
                record_changed_account,
                ..
            }) => FastVoteTransactionResult::Applied {
                vote_account: vote_meta.pubkey,
                record_changed_account,
            },
            Ok(LaunchFastVoteApply::Fallback) => FastVoteTransactionResult::NotEligible,
            Err(source) => FastVoteTransactionResult::Failed(LaunchReplayError::VoteMutation {
                slot,
                transaction_index: transaction.tx_index,
                instruction_index: instruction.instruction_index,
                source,
            }),
        }
    }

    /// Compact post balances are the cheapest complete projection of the old
    /// Bank's fee/rent/collector effects. Program execution remains
    /// authoritative for data, owner, executable, and rent_epoch; this only
    /// repairs lamports on writable transaction accounts.
    fn reconcile_compact_post_balances(
        &mut self,
        slot: u64,
        transaction: &CompactTransactionProbe,
        transaction_metas: &TransactionAccountMetaLayout<'_>,
    ) -> Result<(), LaunchReplayError> {
        validate_compact_balance_projection(slot, transaction, transaction_metas)?;
        self.reconcile_compact_post_balances_prevalidated(transaction, transaction_metas);
        Ok(())
    }

    /// Apply a Compact projection whose vector shape and writable prefix were
    /// already validated. Parallel Vote windows use this infallible commit
    /// phase after every speculative job and counter precheck has succeeded.
    fn reconcile_compact_post_balances_prevalidated(
        &mut self,
        transaction: &CompactTransactionProbe,
        transaction_metas: &TransactionAccountMetaLayout<'_>,
    ) {
        let Some(oracle) = &transaction.balance_oracle else {
            return;
        };

        for (index, (&pubkey, &post_lamports)) in transaction_metas
            .account_keys
            .iter()
            .zip(&oracle.post_balances)
            .enumerate()
        {
            if !transaction_metas.is_writable(index) {
                continue;
            }
            let mut removed = false;
            let changed = match self.outcome.account_state.entry(pubkey) {
                HashEntry::Occupied(mut entry) => {
                    // A plain System account is entirely recoverable from the
                    // next Compact pre-balance that presents it as writable.
                    // Do not let fee/rent projection turn that transient
                    // balance into canonical replay state.
                    if is_balance_only_system_account(entry.get()) {
                        entry.remove();
                        self.outcome.changed_accounts.remove(&pubkey);
                        removed = true;
                        false
                    } else if entry.get().lamports == post_lamports {
                        false
                    } else if post_lamports == 0 {
                        entry.remove();
                        removed = true;
                        true
                    } else {
                        entry.get_mut().lamports = post_lamports;
                        true
                    }
                }
                // Compact alone carries no structural account state. A vacant
                // writable key with a positive post-balance therefore remains
                // transient instead of allocating a hash-table entry.
                HashEntry::Vacant(_) => false,
            };
            if removed {
                // A pending logical Vote belongs to the deleted account. Drop
                // it here so a later slot/outcome barrier cannot resurrect
                // canonical state after a zero-balance projection.
                self.vote_state_cache.invalidate(pubkey);
            }
            if changed {
                self.outcome.changed_accounts.insert(pubkey);
            }
        }
    }

    /// Make every cached Vote account referenced by a generic transaction
    /// canonical before any native/BPF processor can read it, then invalidate
    /// those derivative entries because the generic transaction may change
    /// their data, owner, or length.
    fn prepare_generic_transaction(&mut self, account_keys: &[[u8; 32]]) {
        self.vote_state_cache
            .materialize_referenced(&mut self.outcome.account_state, account_keys);
        for &pubkey in account_keys {
            self.vote_state_cache.invalidate(pubkey);
        }
    }

    fn materialize_all_vote_state(&mut self) {
        self.vote_state_cache
            .materialize_all(&mut self.outcome.account_state);
    }

    /// Execute one coarse window of successful direct-Vote transactions.
    /// Transactions are grouped by Vote account and retain canonical order
    /// within each group. Workers mutate COW account snapshots plus detached
    /// decoded caches, so shared fee payers and readonly sysvars do not force a
    /// new Rayon dispatch. Only after every group succeeds are account data and
    /// cache state published, followed by Compact balance projection in exact
    /// transaction order.
    ///
    /// A speculative miss leaves canonical account data untouched. Detached
    /// caches are rebuilt from those bytes and the caller executes the whole
    /// window through the established sequential path.
    fn try_process_parallel_vote_batch(
        &mut self,
        slot: &CompactSlotProbe,
        start_offset: usize,
        trusted_vote_epoch: u64,
        executed_signature_count: u64,
    ) -> Result<ParallelVoteBatchResult, LaunchReplayError> {
        if self.parallel_vote_executor.is_none() {
            return Ok(ParallelVoteBatchResult::NotEligible);
        }
        let Some(first_two) = slot
            .transactions
            .get(start_offset..start_offset.saturating_add(2))
        else {
            return Ok(ParallelVoteBatchResult::NotEligible);
        };
        if !first_two
            .iter()
            .all(|transaction| is_parallel_vote_wire_candidate(transaction, self.vote_program))
        {
            return Ok(ParallelVoteBatchResult::NotEligible);
        }

        let window_capacity = slot
            .transactions
            .len()
            .saturating_sub(start_offset)
            .min(PARALLEL_VOTE_MAX_WINDOW_TRANSACTIONS);
        let initial_capacity = window_capacity.min(256);
        let mut candidates = Vec::<ParallelVoteCandidate<'_>>::with_capacity(initial_capacity);
        let mut groups = Vec::<ParallelVoteGroup<'_>>::with_capacity(initial_capacity.min(64));
        let mut group_indexes =
            HashMap::<[u8; 32], usize>::with_capacity(initial_capacity.min(128));
        for (transaction_offset, transaction) in slot
            .transactions
            .iter()
            .enumerate()
            .skip(start_offset)
            .take(window_capacity)
        {
            if transaction.archived_outcome != CompactArchivedTransactionOutcome::Succeeded
                || transaction.version != CompactMessageVersion::Legacy
            {
                break;
            }
            let Ok(transaction_metas) = transaction_account_meta_layout(slot.slot, transaction)
            else {
                break;
            };
            if validate_compact_balance_projection(slot.slot, transaction, &transaction_metas)
                .is_err()
                || validate_absent_writable_prebalance_coverage(
                    slot.slot,
                    &self.outcome.account_state,
                    transaction,
                    &transaction_metas,
                )
                .is_err()
                || transaction_metas
                    .account_keys
                    .iter()
                    .enumerate()
                    .any(|(index, pubkey)| {
                        transaction_metas.account_keys[index + 1..].contains(pubkey)
                    })
            {
                break;
            }
            let [instruction] = transaction.instructions.as_slice() else {
                break;
            };
            if instruction.program_id != self.vote_program
                || u16::try_from(instruction.instruction_index).is_err()
            {
                break;
            }
            let CompactInstructionData::Raw(instruction_data) = &instruction.data else {
                break;
            };
            let Ok(vote_metas) = instruction_account_metas(
                slot.slot,
                transaction.tx_index,
                instruction,
                &transaction_metas,
            ) else {
                break;
            };
            let Some(vote_meta) = vote_metas.first() else {
                break;
            };
            let vote_account_pubkey = vote_meta.pubkey;
            let Some(vote_account) = self.outcome.account_state.get(&vote_account_pubkey) else {
                break;
            };
            if !launch_vote_direct_shape_supported(instruction_data, &vote_metas, vote_account) {
                break;
            }

            let group_index = match group_indexes.entry(vote_account_pubkey) {
                HashEntry::Occupied(entry) => *entry.get(),
                HashEntry::Vacant(entry) => {
                    let group_index = groups.len();
                    entry.insert(group_index);
                    groups.push(ParallelVoteGroup {
                        vote_account: vote_account_pubkey,
                        last_transaction_offset: transaction_offset,
                        steps: Vec::new(),
                    });
                    group_index
                }
            };
            let group = &mut groups[group_index];
            group.last_transaction_offset = transaction_offset;
            group.steps.push(ParallelVoteStep {
                instruction_data,
                vote_metas,
            });
            candidates.push(ParallelVoteCandidate {
                transaction_offset,
                vote_account: vote_account_pubkey,
                transaction_metas,
            });
        }

        // Require enough work and enough independent tail behind the longest
        // account chain to amortize one pool wakeup. A 1.5x structural ceiling
        // is intentionally conservative on the NAS's hybrid cores.
        let max_chain_len = groups
            .iter()
            .map(|group| group.steps.len())
            .max()
            .unwrap_or_default();
        if candidates.is_empty() {
            return Ok(ParallelVoteBatchResult::NotEligible);
        }
        if candidates.len() < PARALLEL_VOTE_MIN_WINDOW_TRANSACTIONS
            || groups.len() < 2
            || max_chain_len.saturating_mul(3) > candidates.len().saturating_mul(2)
        {
            return Ok(ParallelVoteBatchResult::Fallback(candidates.len()));
        }

        // Balance projection commutes with direct Vote data mutation except
        // when it removes state that a later Vote in this window must read.
        // Also preserve the authoritative missing-oracle error if an earlier
        // projection removed another writable account.
        let mut projected_absent_accounts = HashMap::<[u8; 32], usize>::new();
        for candidate in &candidates {
            debug_assert!(group_indexes.contains_key(&candidate.vote_account));
            let transaction = &slot.transactions[candidate.transaction_offset];
            let transaction_metas = &candidate.transaction_metas;
            if slot.slot >= FIRST_AUTHORITATIVE_OUTCOME_SLOT
                && transaction.balance_oracle.is_none()
                && transaction_metas
                    .account_keys
                    .iter()
                    .enumerate()
                    .any(|(index, pubkey)| {
                        transaction_metas.is_writable(index)
                            && projected_absent_accounts.contains_key(pubkey)
                    })
            {
                return Ok(ParallelVoteBatchResult::Fallback(candidates.len()));
            }
            let Some(oracle) = &transaction.balance_oracle else {
                continue;
            };
            for (index, (&pubkey, &post_lamports)) in transaction_metas
                .account_keys
                .iter()
                .zip(&oracle.post_balances)
                .enumerate()
            {
                if !transaction_metas.is_writable(index) {
                    continue;
                }
                if post_lamports == 0
                    && group_indexes.get(&pubkey).is_some_and(|group_index| {
                        groups[*group_index].last_transaction_offset > candidate.transaction_offset
                    })
                {
                    return Ok(ParallelVoteBatchResult::Fallback(candidates.len()));
                }
                // Reconciliation also drops plain System accounts regardless
                // of their projected positive balance; Compact can hydrate
                // them transaction-locally again only when an oracle exists.
                if post_lamports == 0
                    || projected_absent_accounts.contains_key(&pubkey)
                    || self
                        .outcome
                        .account_state
                        .get(&pubkey)
                        .is_none_or(is_balance_only_system_account)
                {
                    projected_absent_accounts
                        .entry(pubkey)
                        .or_insert(candidate.transaction_offset);
                }
            }
        }

        // The caller publishes this slot-local Bank counter after a successful
        // batch. Prove that publication cannot overflow before detaching cache
        // entries or dispatching speculative work.
        let _projected_signature_count = candidates.iter().try_fold(
            executed_signature_count,
            |signature_count, candidate| {
                signature_count
                    .checked_add(u64::from(
                        slot.transactions[candidate.transaction_offset]
                            .header
                            .num_required_signatures,
                    ))
                    .ok_or(LaunchReplayError::CounterOverflow)
            },
        )?;

        // Every fallible outcome-counter update is prepared before decoded
        // cache entries leave the canonical cache. An overflow therefore
        // returns the exact pre-window state without needing rollback.
        let transaction_count =
            u64::try_from(candidates.len()).map_err(|_| LaunchReplayError::CounterOverflow)?;
        let parallel_vote_batches = increment(self.outcome.parallel_vote_batches)?;
        let parallel_vote_transactions = self
            .outcome
            .parallel_vote_transactions
            .checked_add(transaction_count)
            .ok_or(LaunchReplayError::CounterOverflow)?;
        let instructions_processed = self
            .outcome
            .instructions_processed
            .checked_add(transaction_count)
            .ok_or(LaunchReplayError::CounterOverflow)?;
        let vote_mutations = self
            .outcome
            .vote_mutations
            .checked_add(transaction_count)
            .ok_or(LaunchReplayError::CounterOverflow)?;
        let transactions_processed = self
            .outcome
            .transactions_processed
            .checked_add(transaction_count)
            .ok_or(LaunchReplayError::CounterOverflow)?;

        let mut jobs = Vec::<ParallelVoteJob<'_>>::with_capacity(groups.len());
        for group in groups {
            let account = self.outcome.account_state[&group.vote_account].clone();
            jobs.push(ParallelVoteJob {
                vote_account: group.vote_account,
                account,
                cache: self.vote_state_cache.take_account(group.vote_account),
                steps: group.steps,
                trusted_vote_epoch,
                record_changed_account: false,
                result: ParallelVoteJobResult::Pending,
            });
        }
        self.parallel_vote_executor
            .as_ref()
            .expect("parallel Vote executor was checked above")
            .execute(&mut jobs);

        if jobs
            .iter()
            .any(|job| job.result != ParallelVoteJobResult::Applied)
        {
            // A direct-path encoding or semantic miss is not a replay failure.
            // Discard every COW snapshot and rebuild detached cache entries
            // from canonical bytes before the caller runs this window serially.
            for job in &jobs {
                self.vote_state_cache.invalidate(job.vote_account);
                let account = &self.outcome.account_state[&job.vote_account];
                self.vote_state_cache.seed(job.vote_account, &account.data);
            }
            return Ok(ParallelVoteBatchResult::Fallback(candidates.len()));
        }

        for job in jobs {
            debug_assert_eq!(job.result, ParallelVoteJobResult::Applied);
            let replaced = self
                .outcome
                .account_state
                .insert(job.vote_account, job.account);
            debug_assert!(replaced.is_some());
            self.vote_state_cache.merge_account(job.cache);
            if job.record_changed_account {
                self.outcome.changed_accounts.insert(job.vote_account);
            }
        }
        for candidate in &candidates {
            let transaction = &slot.transactions[candidate.transaction_offset];
            self.reconcile_compact_post_balances_prevalidated(
                transaction,
                &candidate.transaction_metas,
            );
        }

        self.outcome.parallel_vote_batches = parallel_vote_batches;
        self.outcome.parallel_vote_transactions = parallel_vote_transactions;
        self.outcome.max_parallel_vote_batch =
            self.outcome.max_parallel_vote_batch.max(candidates.len());
        self.outcome.instructions_processed = instructions_processed;
        self.outcome.vote_mutations = vote_mutations;
        self.outcome.transactions_processed = transactions_processed;
        Ok(ParallelVoteBatchResult::Applied(candidates.len()))
    }

    fn process_slot_inner_with_clock<M>(
        &mut self,
        slot: &CompactSlotProbe,
        slot_clock: LaunchClock,
        diff_capture: &mut LaunchInstructionDiffCaptureState,
        account_batch_metrics: &mut LaunchGenerationAccountBatchMetrics,
        mutation_visitor: &mut impl FnMut(&LaunchInstructionMutation),
    ) -> Result<(), LaunchReplayError>
    where
        M: LaunchGenerationMetricsSink,
    {
        if slot.transactions.len() != slot.transaction_count as usize {
            return Err(LaunchReplayError::IncompleteTransactions {
                slot: slot.slot,
                retained: slot.transactions.len(),
                declared: slot.transaction_count,
            });
        }
        debug_assert_eq!(slot_clock.slot, slot.slot);
        self.bpf_compiler
            .set_cross_program_supported(bpf_pda_and_cpi_syscalls_supported(slot_clock.epoch));
        if self.lazy_vote_materialization_enabled && slot_clock.epoch != self.outcome.epoch {
            // Epoch-boundary stake/reward processing may scan canonical Bank
            // accounts before the first transaction in the new epoch.
            self.materialize_all_vote_state();
        }
        if self.bank_lifecycle_enabled {
            let update = self.bank_sysvars.begin_slot(
                slot.slot,
                slot.parent_slot,
                slot.previous_blockhash,
                &mut self.outcome.account_state,
                &mut self.stake_history,
            )?;
            self.outcome.bank_sysvar_writes = self
                .outcome
                .bank_sysvar_writes
                .checked_add(update.written_accounts.len() as u64)
                .ok_or(LaunchReplayError::CounterOverflow)?;
            if !update.written_accounts.is_empty() {
                record_bank_sysvar_accounts(
                    &mut self.outcome.bank_sysvar_accounts_written,
                    update.written_accounts,
                    BankSysvarWritePhase::Child {
                        epoch_transition: update.epoch_transition.is_some(),
                    },
                );
            }
            self.outcome.slot_hashes_unavailable |= update.slot_hashes_unavailable;
            self.activate_epoch_programs(update.epoch_transition);
        }
        let mut executed_signature_count = 0_u64;
        let mut parallel_fallback_until = 0_usize;
        let mut transactions = slot.transactions.iter().enumerate();
        while let Some((transaction_offset, transaction)) = transactions.next() {
            if diff_capture.is_allocation_minimal() && transaction_offset >= parallel_fallback_until
            {
                match self.try_process_parallel_vote_batch(
                    slot,
                    transaction_offset,
                    slot_clock.epoch,
                    executed_signature_count,
                )? {
                    ParallelVoteBatchResult::Applied(parallel_transactions) => {
                        self.rolled_back_transaction = None;
                        for parallel_transaction in &slot.transactions
                            [transaction_offset..transaction_offset + parallel_transactions]
                        {
                            executed_signature_count = executed_signature_count
                                .checked_add(u64::from(
                                    parallel_transaction.header.num_required_signatures,
                                ))
                                .ok_or(LaunchReplayError::CounterOverflow)?;
                        }
                        for _ in 1..parallel_transactions {
                            let _ = transactions.next().expect(
                                "parallel Vote batch is bounded by the slot transaction slice",
                            );
                        }
                        continue;
                    }
                    ParallelVoteBatchResult::Fallback(parallel_transactions) => {
                        parallel_fallback_until = transaction_offset
                            .checked_add(parallel_transactions)
                            .ok_or(LaunchReplayError::CounterOverflow)?;
                    }
                    ParallelVoteBatchResult::NotEligible => {}
                }
            }
            self.rolled_back_transaction = None;
            if transaction.version != CompactMessageVersion::Legacy {
                return Err(LaunchReplayError::UnsupportedMessageVersion {
                    slot: slot.slot,
                    transaction_index: transaction.tx_index,
                    version: transaction.version,
                });
            }
            executed_signature_count = executed_signature_count
                .checked_add(u64::from(transaction.header.num_required_signatures))
                .ok_or(LaunchReplayError::CounterOverflow)?;
            let transaction_metas = transaction_account_meta_layout(slot.slot, transaction)?;
            validate_compact_balance_projection(slot.slot, transaction, &transaction_metas)?;
            validate_absent_writable_prebalance_coverage(
                slot.slot,
                &self.outcome.account_state,
                transaction,
                &transaction_metas,
            )?;

            // Compact is already the trusted, ordered ledger boundary for this
            // replay-first runtime. A recorded failed transaction cannot mutate
            // program state, so avoid overlays, native dispatch, and SBPF
            // execution. Its fee/rent-side lamports are still projected from
            // the archived post balances. Unknown launch-era rows still derive
            // their outcome from runtime execution below.
            if transaction.archived_outcome == CompactArchivedTransactionOutcome::Failed {
                // No program reads or writes occur here. Pending Vote data may
                // safely remain logical while lamports are projected; a zero
                // post-balance below invalidates it before deleting the account.
                self.reconcile_compact_post_balances(slot.slot, transaction, &transaction_metas)?;
                self.outcome.failed_transactions = increment(self.outcome.failed_transactions)?;
                continue;
            }

            if diff_capture.is_allocation_minimal() {
                match self.try_process_allocation_minimal_vote(
                    slot.slot,
                    transaction,
                    &transaction_metas,
                    slot_clock.epoch,
                ) {
                    FastVoteTransactionResult::NotEligible => {}
                    FastVoteTransactionResult::Applied {
                        vote_account,
                        record_changed_account,
                    } => {
                        if record_changed_account {
                            self.outcome.changed_accounts.insert(vote_account);
                        }
                        self.reconcile_compact_post_balances(
                            slot.slot,
                            transaction,
                            &transaction_metas,
                        )?;
                        self.outcome.instructions_processed =
                            increment(self.outcome.instructions_processed)?;
                        self.outcome.vote_mutations = increment(self.outcome.vote_mutations)?;
                        self.outcome.transactions_processed =
                            increment(self.outcome.transactions_processed)?;
                        continue;
                    }
                    FastVoteTransactionResult::Failed(error) => {
                        if transaction.archived_outcome
                            != CompactArchivedTransactionOutcome::Succeeded
                            && is_historical_transaction_failure(&error)
                        {
                            if self.outcome.first_failed_transaction.is_none() {
                                let reason = historical_transaction_failure(&error).expect(
                                    "classified direct Vote failure has a diagnostic reason",
                                );
                                let (_, instruction_index) = replay_error_position(&error);
                                self.outcome.first_failed_transaction =
                                    Some(LaunchDerivedTransactionFailure {
                                        location: LaunchReplayFailureLocation {
                                            slot: slot.slot,
                                            transaction_index: Some(transaction.tx_index),
                                            instruction_index,
                                        },
                                        reason,
                                        rolled_back_instructions: 0,
                                    });
                            }
                            self.outcome.failed_transactions =
                                increment(self.outcome.failed_transactions)?;
                            continue;
                        }
                        return Err(error);
                    }
                }
            }

            if self.lazy_vote_materialization_enabled {
                self.prepare_generic_transaction(transaction_metas.account_keys);
            }

            // Layered overlay: readonly accounts stay in the Bank store until a
            // write forces a local clone. If a later instruction fails, the
            // local overlay is discarded.
            let mut overlay = CowAccountMap::layered(&self.outcome.account_state);
            let mut absent_overlay_accounts = AbsentOverlayAccounts::new();
            seed_absent_covered_pre_balances(
                transaction,
                &transaction_metas,
                &mut overlay,
                &mut absent_overlay_accounts,
            );
            let mut pending = Vec::<PendingCapturedMutation>::new();
            let mut pending_counts = PendingInstructionCounts::default();
            let mut pending_bpf_programs = Vec::<([u8; 32], CompiledProgram)>::new();
            // With more than one instruction, a later unsupported instruction
            // can hard-stop diagnostic replay. Preserve internal diffs for that
            // rare failing transaction even after the visitor's sample budget
            // is exhausted. They are not emitted on an otherwise successful
            // transaction.
            let preserve_hard_failure_rollback = transaction.instructions.len() > 1
                && diff_capture.preserves_hard_failure_rollback();
            let instruction_result = (|| -> Result<(), LaunchReplayError> {
                for instruction in &transaction.instructions {
                    let emit_diff = diff_capture.wants_visitor_diff();
                    let capture_diff = emit_diff || preserve_hard_failure_rollback;
                    let instruction_path_index = u16::try_from(instruction.instruction_index)
                        .map_err(|_| LaunchReplayError::InstructionPathIndexOverflow {
                            slot: slot.slot,
                            transaction_index: transaction.tx_index,
                            instruction_index: instruction.instruction_index,
                        })?;
                    let instruction_metas = instruction_account_metas(
                        slot.slot,
                        transaction.tx_index,
                        instruction,
                        &transaction_metas,
                    )?;
                    let (kind, captured) = if instruction.program_id == self.vote_program {
                        let vote_metas = instruction_metas;
                        let vote_account = vote_metas.first().map(|meta| meta.pubkey).ok_or(
                            LaunchReplayError::MissingVoteAccount {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                            },
                        )?;
                        let use_vote_state_cache = transaction.instructions.len() == 1;
                        if !use_vote_state_cache {
                            self.vote_state_cache.invalidate(vote_account);
                        }
                        for meta in &vote_metas {
                            if !meta.is_writable || overlay.local_contains_key(&meta.pubkey) {
                                continue;
                            }
                            if let Some(account) = overlay.get(&meta.pubkey).cloned() {
                                overlay.insert(meta.pubkey, account);
                            } else {
                                overlay.insert(meta.pubkey, default_system_account());
                                absent_overlay_accounts.insert(meta.pubkey);
                            }
                        }
                        let bytes = match &instruction.data {
                            CompactInstructionData::Raw(bytes)
                            | CompactInstructionData::UnknownVote(bytes) => bytes.as_slice(),
                            _ => {
                                self.vote_state_cache.invalidate(vote_account);
                                return Err(LaunchReplayError::UnsupportedVoteEncoding {
                                    slot: slot.slot,
                                    transaction_index: transaction.tx_index,
                                    instruction_index: instruction.instruction_index,
                                });
                            }
                        };
                        let journal = capture_diff.then(|| {
                            begin_account_diff_journal(
                                &overlay,
                                &vote_metas,
                                &absent_overlay_accounts,
                            )
                        });
                        let mutation_result = if use_vote_state_cache {
                            apply_launch_vote_instruction_on_overlay_cached(
                                bytes,
                                &vote_metas,
                                &mut overlay,
                                slot_clock.epoch,
                                &mut self.vote_state_cache,
                            )
                            .map(|(mutation, _cache_hit)| mutation)
                        } else {
                            apply_launch_vote_instruction_on_overlay(
                                bytes,
                                &vote_metas,
                                &mut overlay,
                                slot_clock.epoch,
                            )
                        };
                        let mutation =
                            mutation_result.map_err(|source| LaunchReplayError::VoteMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                source,
                            })?;
                        let captured = if let Some(journal) = journal {
                            let diff = capture_instruction_diff(
                                slot.slot,
                                transaction.tx_index,
                                instruction,
                                instruction_path_index,
                                self.vote_program,
                                journal,
                                &overlay,
                                &vote_metas,
                                &mut absent_overlay_accounts,
                            );
                            Some(LaunchInstructionMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                effect: LaunchInstructionEffect::Vote {
                                    vote_account,
                                    mutation,
                                },
                                diff,
                            })
                        } else {
                            reconcile_absent_overlay_accounts(
                                &overlay,
                                &vote_metas,
                                &mut absent_overlay_accounts,
                            );
                            None
                        };
                        (LaunchInstructionKind::Vote, captured)
                    } else if instruction.program_id == self.config_program {
                        let config_metas = instruction_metas;
                        for meta in &config_metas {
                            if !meta.is_writable || overlay.local_contains_key(&meta.pubkey) {
                                continue;
                            }
                            if let Some(account) = overlay.get(&meta.pubkey).cloned() {
                                overlay.insert(meta.pubkey, account);
                            } else {
                                overlay.insert(meta.pubkey, default_system_account());
                                absent_overlay_accounts.insert(meta.pubkey);
                            }
                        }
                        let journal = capture_diff.then(|| {
                            begin_account_diff_journal(
                                &overlay,
                                &config_metas,
                                &absent_overlay_accounts,
                            )
                        });
                        let config_instruction = instruction_data_bytes(&instruction.data).ok_or(
                            LaunchReplayError::UnsupportedConfigEncoding {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                            },
                        )?;
                        let mutation = apply_launch_config_instruction_on_overlay(
                            config_instruction,
                            &config_metas,
                            &mut overlay,
                        )
                        .map_err(|source| {
                            LaunchReplayError::ConfigMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                source,
                            }
                        })?;
                        let captured = if let Some(journal) = journal {
                            let diff = capture_instruction_diff(
                                slot.slot,
                                transaction.tx_index,
                                instruction,
                                instruction_path_index,
                                self.config_program,
                                journal,
                                &overlay,
                                &config_metas,
                                &mut absent_overlay_accounts,
                            );
                            Some(LaunchInstructionMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                effect: LaunchInstructionEffect::Config(mutation),
                                diff,
                            })
                        } else {
                            reconcile_absent_overlay_accounts(
                                &overlay,
                                &config_metas,
                                &mut absent_overlay_accounts,
                            );
                            None
                        };
                        (LaunchInstructionKind::Config, captured)
                    } else if instruction.program_id == self.system_program {
                        let system_metas = instruction_metas;
                        for meta in &system_metas {
                            if !meta.is_writable || overlay.local_contains_key(&meta.pubkey) {
                                continue;
                            }
                            if let Some(account) = overlay.get(&meta.pubkey).cloned() {
                                overlay.insert(meta.pubkey, account);
                            } else {
                                overlay.insert(meta.pubkey, default_system_account());
                                absent_overlay_accounts.insert(meta.pubkey);
                            }
                        }
                        let journal = capture_diff.then(|| {
                            begin_account_diff_journal(
                                &overlay,
                                &system_metas,
                                &absent_overlay_accounts,
                            )
                        });
                        let system_instruction = match &instruction.data {
                            CompactInstructionData::System(system_instruction) => {
                                system_instruction
                            }
                            _ => {
                                return Err(LaunchReplayError::UnsupportedSystemEncoding {
                                    slot: slot.slot,
                                    transaction_index: transaction.tx_index,
                                    instruction_index: instruction.instruction_index,
                                });
                            }
                        };
                        let mutation = apply_launch_system_instruction_for_epoch_on_overlay(
                            system_instruction,
                            &system_metas,
                            &mut overlay,
                            slot_clock.epoch,
                        )
                        .map_err(|source| {
                            LaunchReplayError::SystemMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                source,
                            }
                        })?;
                        let captured = if let Some(journal) = journal {
                            let diff = capture_instruction_diff(
                                slot.slot,
                                transaction.tx_index,
                                instruction,
                                instruction_path_index,
                                self.system_program,
                                journal,
                                &overlay,
                                &system_metas,
                                &mut absent_overlay_accounts,
                            );
                            Some(LaunchInstructionMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                effect: LaunchInstructionEffect::System(mutation),
                                diff,
                            })
                        } else {
                            reconcile_absent_overlay_accounts(
                                &overlay,
                                &system_metas,
                                &mut absent_overlay_accounts,
                            );
                            None
                        };
                        (LaunchInstructionKind::System, captured)
                    } else if instruction.program_id == self.stake_program {
                        let stake_metas = instruction_metas;
                        for meta in &stake_metas {
                            if !meta.is_writable || overlay.local_contains_key(&meta.pubkey) {
                                continue;
                            }
                            if let Some(account) = overlay.get(&meta.pubkey).cloned() {
                                overlay.insert(meta.pubkey, account);
                            } else {
                                overlay.insert(meta.pubkey, default_system_account());
                                absent_overlay_accounts.insert(meta.pubkey);
                            }
                        }
                        let journal = capture_diff.then(|| {
                            begin_account_diff_journal(
                                &overlay,
                                &stake_metas,
                                &absent_overlay_accounts,
                            )
                        });
                        let stake_instruction = instruction_data_bytes(&instruction.data).ok_or(
                            LaunchReplayError::UnsupportedStakeEncoding {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                            },
                        )?;
                        let mutation = apply_launch_stake_instruction_on_overlay(
                            stake_instruction,
                            &stake_metas,
                            &mut overlay,
                            LaunchStakeContext {
                                clock: slot_clock,
                                stake_history: &self.stake_history,
                            },
                        )
                        .map_err(|source| {
                            LaunchReplayError::StakeMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                source,
                            }
                        })?;
                        let captured = if let Some(journal) = journal {
                            let diff = capture_instruction_diff(
                                slot.slot,
                                transaction.tx_index,
                                instruction,
                                instruction_path_index,
                                self.stake_program,
                                journal,
                                &overlay,
                                &stake_metas,
                                &mut absent_overlay_accounts,
                            );
                            Some(LaunchInstructionMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                effect: LaunchInstructionEffect::Stake(mutation),
                                diff,
                            })
                        } else {
                            reconcile_absent_overlay_accounts(
                                &overlay,
                                &stake_metas,
                                &mut absent_overlay_accounts,
                            );
                            None
                        };
                        (LaunchInstructionKind::Stake, captured)
                    } else if instruction.program_id == BPF_LOADER_PROGRAM_ID
                        && self.bpf_loader_is_active()
                    {
                        let loader_metas = instruction_metas;
                        for meta in &loader_metas {
                            if !meta.is_writable || overlay.local_contains_key(&meta.pubkey) {
                                continue;
                            }
                            if let Some(account) = overlay.get(&meta.pubkey).cloned() {
                                overlay.insert(meta.pubkey, account);
                            } else {
                                overlay.insert(meta.pubkey, default_system_account());
                                absent_overlay_accounts.insert(meta.pubkey);
                            }
                        }
                        let journal = capture_diff.then(|| {
                            begin_account_diff_journal(
                                &overlay,
                                &loader_metas,
                                &absent_overlay_accounts,
                            )
                        });
                        let loader_instruction = instruction_data_bytes(&instruction.data).ok_or(
                            LaunchReplayError::BpfLoaderMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                source: LaunchBpfLoaderError::InvalidInstructionData,
                            },
                        )?;
                        let applied = apply_launch_bpf_loader_instruction_on_overlay(
                            loader_instruction,
                            &loader_metas,
                            &mut overlay,
                            &self.bpf_compiler,
                            self.bpf_loader_context(),
                        )
                        .map_err(|source| {
                            LaunchReplayError::BpfLoaderMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                source,
                            }
                        })?;
                        if let Some(compiled_program) = applied.compiled_program {
                            pending_bpf_programs.push(compiled_program);
                        }
                        let captured = if let Some(journal) = journal {
                            let diff = capture_instruction_diff(
                                slot.slot,
                                transaction.tx_index,
                                instruction,
                                instruction_path_index,
                                BPF_LOADER_PROGRAM_ID,
                                journal,
                                &overlay,
                                &loader_metas,
                                &mut absent_overlay_accounts,
                            );
                            Some(LaunchInstructionMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                effect: LaunchInstructionEffect::BpfLoader(applied.mutation),
                                diff,
                            })
                        } else {
                            reconcile_absent_overlay_accounts(
                                &overlay,
                                &loader_metas,
                                &mut absent_overlay_accounts,
                            );
                            None
                        };
                        (LaunchInstructionKind::BpfLoader, captured)
                    } else if self.bpf_loader_is_active()
                        && overlay
                            .get(&instruction.program_id)
                            .or_else(|| self.outcome.account_state.get(&instruction.program_id))
                            .is_some_and(|account| {
                                account.executable && account.owner == BPF_LOADER_PROGRAM_ID
                            })
                    {
                        let bpf_metas = instruction_metas;
                        for meta in &bpf_metas {
                            if !meta.is_writable || overlay.local_contains_key(&meta.pubkey) {
                                continue;
                            }
                            if let Some(account) = overlay.get(&meta.pubkey).cloned() {
                                overlay.insert(meta.pubkey, account);
                            } else {
                                overlay.insert(meta.pubkey, default_system_account());
                                absent_overlay_accounts.insert(meta.pubkey);
                            }
                        }
                        if !self.bpf_program_cache.contains_key(&instruction.program_id)
                            && !pending_bpf_programs
                                .iter()
                                .any(|(program_id, _)| *program_id == instruction.program_id)
                        {
                            let program_account = overlay
                                .get(&instruction.program_id)
                                .or_else(|| self.outcome.account_state.get(&instruction.program_id))
                                .expect("legacy BPF dispatch predicate validated program state");
                            let compiled = self
                                .bpf_compiler
                                .compile_account(LoaderAccountKind::Legacy, &program_account.data)
                                .map_err(|error| LaunchReplayError::BpfProgramLoad {
                                    slot: slot.slot,
                                    transaction_index: transaction.tx_index,
                                    instruction_index: instruction.instruction_index,
                                    program_id: instruction.program_id,
                                    message: error.to_string(),
                                })?;
                            pending_bpf_programs.push((instruction.program_id, compiled));
                        }
                        let journal = capture_diff.then(|| {
                            begin_account_diff_journal(
                                &overlay,
                                &bpf_metas,
                                &absent_overlay_accounts,
                            )
                        });
                        let bpf_instruction = instruction_data_bytes(&instruction.data).ok_or(
                            LaunchReplayError::UnsupportedBpfEncoding {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                            },
                        )?;
                        let compiled_program = self
                            .bpf_program_cache
                            .get(&instruction.program_id)
                            .or_else(|| {
                                pending_bpf_programs.iter().rev().find_map(
                                    |(program_id, compiled)| {
                                        (*program_id == instruction.program_id).then_some(compiled)
                                    },
                                )
                            })
                            .expect("legacy BPF program was compiled or already cached");
                        let mutation =
                            crate::launch_bpf_execute::apply_launch_bpf_program_instruction_with_stack(
                            instruction.program_id,
                            bpf_instruction,
                            &bpf_metas,
                            &mut overlay,
                            &self.bpf_compiler,
                            compiled_program,
                            self.bpf_loader_context().bank_rent,
                            smallvec::SmallVec::from_slice(&[instruction.program_id]),
                        )
                        .map_err(|source| {
                            LaunchReplayError::BpfProgramExecution {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                program_id: instruction.program_id,
                                archived_outcome: transaction.archived_outcome,
                                source,
                            }
                        })?;
                        let captured = if let Some(journal) = journal {
                            let diff = capture_instruction_diff(
                                slot.slot,
                                transaction.tx_index,
                                instruction,
                                instruction_path_index,
                                instruction.program_id,
                                journal,
                                &overlay,
                                &bpf_metas,
                                &mut absent_overlay_accounts,
                            );
                            Some(LaunchInstructionMutation {
                                slot: slot.slot,
                                transaction_index: transaction.tx_index,
                                instruction_index: instruction.instruction_index,
                                effect: LaunchInstructionEffect::BpfProgram(mutation),
                                diff,
                            })
                        } else {
                            reconcile_absent_overlay_accounts(
                                &overlay,
                                &bpf_metas,
                                &mut absent_overlay_accounts,
                            );
                            None
                        };
                        // The portable checkpoint's existing counter is the
                        // total legacy-BPF instruction class: deployment and
                        // executable invocation. Compact provenance reports
                        // the deployment subset independently.
                        (LaunchInstructionKind::BpfLoader, captured)
                    } else {
                        return Err(LaunchReplayError::UnsupportedProgram {
                            slot: slot.slot,
                            transaction_index: transaction.tx_index,
                            instruction_index: instruction.instruction_index,
                            program_id: instruction.program_id,
                        });
                    };
                    pending_counts.record(kind);
                    if let Some(mutation) = captured {
                        if emit_diff {
                            diff_capture.record_visitor_diff();
                        }
                        pending.push(PendingCapturedMutation {
                            mutation,
                            emit_to_visitor: emit_diff,
                        });
                    }
                }
                Ok(())
            })();
            if let Err(error) = instruction_result {
                if !pending.is_empty() {
                    for captured in &mut pending {
                        captured.mutation.diff.disposition = DiffDisposition::RolledBack;
                    }
                }

                if transaction.archived_outcome != CompactArchivedTransactionOutcome::Succeeded
                    && is_historical_transaction_failure(&error)
                {
                    let rolled_back_instructions = pending_counts.total;
                    if self.outcome.first_failed_transaction.is_none() {
                        let reason = historical_transaction_failure(&error)
                            .expect("classified native failure has a diagnostic reason");
                        let (_, instruction_index) = replay_error_position(&error);
                        self.outcome.first_failed_transaction =
                            Some(LaunchDerivedTransactionFailure {
                                location: LaunchReplayFailureLocation {
                                    slot: slot.slot,
                                    transaction_index: Some(transaction.tx_index),
                                    instruction_index,
                                },
                                reason,
                                rolled_back_instructions,
                            });
                    }
                    self.outcome.failed_transactions = increment(self.outcome.failed_transactions)?;
                    self.outcome.rolled_back_instructions = self
                        .outcome
                        .rolled_back_instructions
                        .checked_add(pending_counts.total)
                        .ok_or(LaunchReplayError::CounterOverflow)?;
                    for captured in pending {
                        if captured.emit_to_visitor {
                            mutation_visitor(&captured.mutation);
                            if self.retain_instruction_mutations {
                                self.outcome.instruction_mutations.push(captured.mutation);
                            }
                        }
                    }
                    continue;
                }

                if is_archived_system_transfer_with_canonical_prebalance(
                    transaction,
                    &transaction_metas,
                    &error,
                ) || is_archived_fee_only_system_transfer(
                    transaction,
                    &transaction_metas,
                    &error,
                ) || is_archived_fee_only_prefunded_create(
                    transaction,
                    &transaction_metas,
                    &error,
                ) {
                    self.reconcile_compact_post_balances(
                        slot.slot,
                        transaction,
                        &transaction_metas,
                    )?;
                    self.outcome.instructions_processed =
                        increment(self.outcome.instructions_processed)?;
                    self.outcome.system_mutations = increment(self.outcome.system_mutations)?;
                    self.outcome.transactions_processed =
                        increment(self.outcome.transactions_processed)?;
                    continue;
                }

                self.rolled_back_transaction =
                    (!pending.is_empty()).then_some(LaunchRolledBackTransaction {
                        slot: slot.slot,
                        transaction_index: transaction.tx_index,
                        instruction_mutations: pending
                            .into_iter()
                            .map(|captured| captured.mutation)
                            .collect(),
                    });
                return Err(error);
            }

            let mut account_batch = AccountWriteBatch::new();
            for (pubkey, account) in overlay.into_local() {
                let before = self.outcome.account_state.get(&pubkey);
                let changed = before.map_or_else(
                    || account != default_system_account(),
                    |before| before != &account,
                );
                if changed {
                    if transaction.balance_oracle.is_some()
                        && is_balance_only_system_account(&account)
                    {
                        // This includes an absent account hydrated from the
                        // canonical pre-balance and a legacy balance-only
                        // account touched by the transaction. Neither carries
                        // replayable structure, so neither belongs in the Bank
                        // state or the structural changed-account report.
                        if before.is_some() {
                            account_batch.delete(pubkey)?;
                        }
                        self.outcome.changed_accounts.remove(&pubkey);
                    } else if account.lamports == 0 {
                        account_batch.delete(pubkey)?;
                        self.outcome.changed_accounts.insert(pubkey);
                    } else {
                        account_batch.put(pubkey, account)?;
                        self.outcome.changed_accounts.insert(pubkey);
                    }
                }
            }
            let (account_batch_commit, account_batch_commit_time) =
                measure_generation_phase::<M, _>(|| {
                    self.outcome.account_state.apply_batch(account_batch)
                });
            let account_batch_commit = account_batch_commit?;
            record_generation_account_batch::<M>(
                account_batch_metrics,
                account_batch_commit,
                account_batch_commit_time,
            );
            self.reconcile_compact_post_balances(slot.slot, transaction, &transaction_metas)?;
            for (program_id, compiled_program) in pending_bpf_programs {
                self.bpf_program_cache.insert(program_id, compiled_program);
            }
            self.outcome.instructions_processed = self
                .outcome
                .instructions_processed
                .checked_add(pending_counts.total)
                .ok_or(LaunchReplayError::CounterOverflow)?;
            self.outcome.vote_mutations = self
                .outcome
                .vote_mutations
                .checked_add(pending_counts.vote)
                .ok_or(LaunchReplayError::CounterOverflow)?;
            self.outcome.config_mutations = self
                .outcome
                .config_mutations
                .checked_add(pending_counts.config)
                .ok_or(LaunchReplayError::CounterOverflow)?;
            self.outcome.system_mutations = self
                .outcome
                .system_mutations
                .checked_add(pending_counts.system)
                .ok_or(LaunchReplayError::CounterOverflow)?;
            self.outcome.stake_mutations = self
                .outcome
                .stake_mutations
                .checked_add(pending_counts.stake)
                .ok_or(LaunchReplayError::CounterOverflow)?;
            self.outcome.bpf_loader_mutations = self
                .outcome
                .bpf_loader_mutations
                .checked_add(pending_counts.bpf_loader)
                .ok_or(LaunchReplayError::CounterOverflow)?;
            for mut captured in pending {
                captured.mutation.diff.disposition = DiffDisposition::Committed;
                if captured.emit_to_visitor {
                    mutation_visitor(&captured.mutation);
                    if self.retain_instruction_mutations {
                        self.outcome.instruction_mutations.push(captured.mutation);
                    }
                }
            }
            self.outcome.transactions_processed = increment(self.outcome.transactions_processed)?;
        }
        if self.outcome.first_slot.is_none() {
            self.outcome.first_slot = Some(slot.slot);
        }
        self.outcome.last_slot = Some(slot.slot);
        self.outcome.slots_processed = increment(self.outcome.slots_processed)?;
        if self.bank_lifecycle_enabled {
            let written_accounts = self.bank_sysvars.complete_slot(
                slot.slot,
                slot.blockhash,
                executed_signature_count,
                &mut self.outcome.account_state,
            )?;
            self.outcome.bank_sysvar_writes = self
                .outcome
                .bank_sysvar_writes
                .checked_add(written_accounts.len() as u64)
                .ok_or(LaunchReplayError::CounterOverflow)?;
            record_bank_sysvar_accounts(
                &mut self.outcome.bank_sysvar_accounts_written,
                written_accounts,
                BankSysvarWritePhase::SlotHistory,
            );
        }
        self.outcome.epoch = slot_clock.epoch;
        Ok(())
    }

    pub fn finish(mut self) -> LaunchReplayOutcome {
        // Also covers a diagnostic stop in the middle of a slot: committed
        // direct Votes before the failure remain part of the returned prefix.
        self.materialize_all_vote_state();
        self.outcome.lazy_vote_commits = self.vote_state_cache.lazy_direct_commits();
        self.outcome.vote_state_materializations = self.vote_state_cache.materializations();
        self.outcome
    }

    fn take_rolled_back_transaction(&mut self) -> Option<LaunchRolledBackTransaction> {
        self.rolled_back_transaction.take()
    }
}

#[derive(Clone, Copy)]
struct TransactionAccountMetaLayout<'a> {
    account_keys: &'a [[u8; 32]],
    required: usize,
    writable_signed: usize,
    writable_unsigned_end: usize,
}

/// Transaction-local membership for accounts synthesized from Compact
/// pre-balances. Message account lists are normally small, so keep the first
/// eight keys inline while retaining the sorted/deduplicated behavior of the
/// former `BTreeSet` for larger transactions.
#[derive(Debug, Default)]
struct AbsentOverlayAccounts {
    pubkeys: SmallVec<[[u8; 32]; 8]>,
}

impl AbsentOverlayAccounts {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&mut self, pubkey: [u8; 32]) -> bool {
        match self.pubkeys.binary_search(&pubkey) {
            Ok(_) => false,
            Err(index) => {
                self.pubkeys.insert(index, pubkey);
                true
            }
        }
    }

    #[inline]
    fn contains(&self, pubkey: &[u8; 32]) -> bool {
        self.pubkeys.binary_search(pubkey).is_ok()
    }

    fn remove(&mut self, pubkey: &[u8; 32]) -> bool {
        let Ok(index) = self.pubkeys.binary_search(pubkey) else {
            return false;
        };
        self.pubkeys.remove(index);
        true
    }

    #[cfg(test)]
    fn as_slice(&self) -> &[[u8; 32]] {
        &self.pubkeys
    }

    #[cfg(test)]
    fn spilled(&self) -> bool {
        self.pubkeys.spilled()
    }
}

impl TransactionAccountMetaLayout<'_> {
    #[inline]
    fn is_writable(&self, index: usize) -> bool {
        index < self.writable_signed
            || (index >= self.required && index < self.writable_unsigned_end)
    }

    fn get(&self, index: usize) -> Option<LaunchAccountMeta> {
        self.account_keys
            .get(index)
            .copied()
            .map(|pubkey| LaunchAccountMeta {
                pubkey,
                is_signer: index < self.required,
                is_writable: self.is_writable(index),
            })
    }

    /// Smallest leading balance slice that still contains every writable
    /// legacy message account. Launch-era status metadata can omit a trailing
    /// readonly-unsigned suffix.
    #[inline]
    fn writable_projection_prefix_len(&self) -> usize {
        if self.writable_unsigned_end > self.required {
            self.writable_unsigned_end
        } else {
            self.writable_signed
        }
    }
}

/// True when lamports are the account's only non-default field.
///
/// Such an account has no PDA/program/nonce/allocated state for replay to
/// preserve. Compact's canonical pre/post balance vectors can reconstruct it
/// transaction-locally whenever the account is covered by the balance oracle.
#[inline]
pub(crate) fn is_balance_only_system_account(account: &AccountSnapshot) -> bool {
    account.owner == SYSTEM_PROGRAM_ID
        && !account.executable
        && account.rent_epoch == 0
        && account.data.is_empty()
}

fn validate_compact_balance_projection(
    slot: u64,
    transaction: &CompactTransactionProbe,
    transaction_metas: &TransactionAccountMetaLayout<'_>,
) -> Result<(), LaunchReplayError> {
    let Some(oracle) = &transaction.balance_oracle else {
        return Ok(());
    };
    if oracle.pre_balances.len() != oracle.post_balances.len()
        || oracle.post_balances.len() < transaction_metas.writable_projection_prefix_len()
        || oracle.post_balances.len() > transaction_metas.account_keys.len()
    {
        return Err(LaunchReplayError::InvalidCompactBalanceProjection {
            slot,
            transaction_index: transaction.tx_index,
            message: "balance count does not cover exactly the writable message prefix",
        });
    }
    Ok(())
}

/// Seed absent message accounts covered by an already-validated Compact
/// pre-balance projection. This includes readonly accounts whose lamports may
/// affect BPF execution. The accounts remain transaction-local until an
/// instruction gives them structural state.
fn validate_absent_writable_prebalance_coverage(
    slot: u64,
    canonical: &MemoryAccountStore,
    transaction: &CompactTransactionProbe,
    transaction_metas: &TransactionAccountMetaLayout<'_>,
) -> Result<(), LaunchReplayError> {
    if slot >= FIRST_AUTHORITATIVE_OUTCOME_SLOT
        && transaction.balance_oracle.is_none()
        && transaction_metas
            .account_keys
            .iter()
            .enumerate()
            .any(|(index, pubkey)| {
                transaction_metas.is_writable(index) && !canonical.contains_key(pubkey)
            })
    {
        return Err(LaunchReplayError::InvalidCompactBalanceProjection {
            slot,
            transaction_index: transaction.tx_index,
            message: "absent writable account has no canonical Compact pre-balance",
        });
    }
    Ok(())
}

fn seed_absent_covered_pre_balances(
    transaction: &CompactTransactionProbe,
    transaction_metas: &TransactionAccountMetaLayout<'_>,
    overlay: &mut CowAccountMap,
    absent_overlay_accounts: &mut AbsentOverlayAccounts,
) {
    let Some(oracle) = &transaction.balance_oracle else {
        return;
    };
    debug_assert_eq!(oracle.pre_balances.len(), oracle.post_balances.len());
    debug_assert!(
        oracle.pre_balances.len() >= transaction_metas.writable_projection_prefix_len()
            && oracle.pre_balances.len() <= transaction_metas.account_keys.len()
    );

    for (&pubkey, &pre_lamports) in transaction_metas
        .account_keys
        .iter()
        .zip(&oracle.pre_balances)
    {
        if overlay.contains_key(&pubkey) {
            continue;
        }
        let mut account = default_system_account();
        account.lamports = pre_lamports;
        overlay.insert(pubkey, account);
        absent_overlay_accounts.insert(pubkey);
    }
}

/// Remove balance-only accounts that an older writable-post-balance runtime
/// dynamically hydrated. Untouched serialized-genesis accounts are excluded:
/// only keys already classified as replay changes are migration candidates.
pub(crate) fn prune_legacy_hydrated_balance_only_system_accounts(
    outcome: &mut LaunchReplayOutcome,
) -> usize {
    let candidates = outcome
        .changed_accounts
        .iter()
        .copied()
        .filter(|pubkey| {
            outcome
                .account_state
                .get(pubkey)
                .is_some_and(is_balance_only_system_account)
        })
        .collect::<Vec<_>>();
    for pubkey in &candidates {
        outcome.account_state.remove(pubkey);
        outcome.changed_accounts.remove(pubkey);
    }
    candidates.len()
}

fn transaction_account_meta_layout(
    slot: u64,
    transaction: &CompactTransactionProbe,
) -> Result<TransactionAccountMetaLayout<'_>, LaunchReplayError> {
    let required = transaction.header.num_required_signatures as usize;
    let readonly_signed = transaction.header.num_readonly_signed_accounts as usize;
    let readonly_unsigned = transaction.header.num_readonly_unsigned_accounts as usize;
    let account_keys = transaction.account_keys.len();
    if required > account_keys
        || readonly_signed > required
        || readonly_unsigned > account_keys.saturating_sub(required)
    {
        return Err(LaunchReplayError::InvalidMessageHeader {
            slot,
            transaction_index: transaction.tx_index,
            required_signatures: transaction.header.num_required_signatures,
            readonly_signed: transaction.header.num_readonly_signed_accounts,
            readonly_unsigned: transaction.header.num_readonly_unsigned_accounts,
            account_keys,
        });
    }
    let writable_signed = required - readonly_signed;
    let writable_unsigned_end = account_keys - readonly_unsigned;
    Ok(TransactionAccountMetaLayout {
        account_keys: &transaction.account_keys,
        required,
        writable_signed,
        writable_unsigned_end,
    })
}

/// One early-Bank anomaly is visible in canonical RPC and Compact as a
/// successful, fee-only System transfer: the next transaction confirms that
/// neither source nor destination received the transfer delta. Keep the
/// bypass structural and exact so it cannot hide a BPF/native data-state bug.
fn is_archived_fee_only_system_transfer(
    transaction: &CompactTransactionProbe,
    transaction_metas: &TransactionAccountMetaLayout<'_>,
    error: &LaunchReplayError,
) -> bool {
    if transaction.archived_outcome != CompactArchivedTransactionOutcome::Succeeded
        || transaction.instructions.len() != 1
        || transaction_metas.account_keys.len() < 2
        || !transaction_metas.is_writable(0)
        || !transaction_metas.is_writable(1)
        || transaction_metas.account_keys[0] == transaction_metas.account_keys[1]
    {
        return false;
    }
    let instruction = &transaction.instructions[0];
    let CompactInstructionData::System(
        blockzilla_format::ArchiveV2SystemInstructionData::Transfer { lamports },
    ) = &instruction.data
    else {
        return false;
    };
    if instruction.program_id != SYSTEM_PROGRAM_ID
        || instruction.account_indexes.as_slice() != [0, 1]
    {
        return false;
    }
    let LaunchReplayError::SystemMutation {
        source: LaunchSystemError::ResultWithNegativeLamports { pubkey, required },
        ..
    } = error
    else {
        return false;
    };
    let Some(oracle) = &transaction.balance_oracle else {
        return false;
    };
    if oracle.pre_balances.len() != transaction_metas.account_keys.len()
        || oracle.post_balances.len() != transaction_metas.account_keys.len()
    {
        return false;
    }

    *pubkey == transaction_metas.account_keys[0]
        && *required == *lamports
        && oracle.pre_balances[0] < *lamports
        && oracle.pre_balances[0].checked_sub(oracle.post_balances[0]) == Some(oracle.fee)
        && oracle.pre_balances[1] == oracle.post_balances[1]
}

/// The replay POC deliberately does not materialize every historical block
/// reward, so a fee-recipient account can have stale lamports when it later
/// funds a plain System transfer. Compact pre/post balances are authoritative
/// for lamports; recover only the exact successful transfer equation after the
/// native processor has already established every non-balance precondition.
fn is_archived_system_transfer_with_canonical_prebalance(
    transaction: &CompactTransactionProbe,
    transaction_metas: &TransactionAccountMetaLayout<'_>,
    error: &LaunchReplayError,
) -> bool {
    if transaction.archived_outcome != CompactArchivedTransactionOutcome::Succeeded
        || transaction.instructions.len() != 1
        || transaction_metas.account_keys.len() < 2
        || !transaction_metas.is_writable(0)
        || !transaction_metas.is_writable(1)
        || transaction_metas.account_keys[0] == transaction_metas.account_keys[1]
    {
        return false;
    }
    let [instruction] = transaction.instructions.as_slice() else {
        return false;
    };
    let CompactInstructionData::System(
        blockzilla_format::ArchiveV2SystemInstructionData::Transfer { lamports },
    ) = &instruction.data
    else {
        return false;
    };
    if instruction.program_id != SYSTEM_PROGRAM_ID
        || instruction.account_indexes.as_slice() != [0, 1]
    {
        return false;
    }
    let LaunchReplayError::SystemMutation {
        source: LaunchSystemError::ResultWithNegativeLamports { pubkey, required },
        ..
    } = error
    else {
        return false;
    };
    let Some(oracle) = &transaction.balance_oracle else {
        return false;
    };
    if oracle.pre_balances.len() < 2 || oracle.post_balances.len() < 2 {
        return false;
    }
    let Some(expected_source_debit) = lamports.checked_add(oracle.fee) else {
        return false;
    };

    *pubkey == transaction_metas.account_keys[0]
        && *required == *lamports
        && oracle.pre_balances[0] >= *lamports
        && oracle.pre_balances[0].checked_sub(oracle.post_balances[0])
            == Some(expected_source_debit)
        && oracle.post_balances[1].checked_sub(oracle.pre_balances[1]) == Some(*lamports)
}

/// Canonical status metadata contains one epoch-43 prefunded CreateAccount
/// retry marked successful even though the preceding retry created the
/// destination, later retries report AccountAlreadyInUse, and this row changes
/// only the fee payer. Recover only that structurally impossible no-op shape;
/// data/owner execution remains authoritative everywhere else.
fn is_archived_fee_only_prefunded_create(
    transaction: &CompactTransactionProbe,
    transaction_metas: &TransactionAccountMetaLayout<'_>,
    error: &LaunchReplayError,
) -> bool {
    if transaction.archived_outcome != CompactArchivedTransactionOutcome::Succeeded
        || transaction.signature_count != 2
        || transaction.header.num_required_signatures != 2
        || transaction.header.num_readonly_signed_accounts != 0
        || transaction.header.num_readonly_unsigned_accounts != 1
        || transaction.instructions.len() != 1
        || transaction_metas.account_keys.len() != 3
        || transaction_metas.account_keys[2] != SYSTEM_PROGRAM_ID
        || !transaction_metas.is_writable(0)
        || !transaction_metas.is_writable(1)
        || transaction_metas.account_keys[0] == transaction_metas.account_keys[1]
    {
        return false;
    }
    let instruction = &transaction.instructions[0];
    let CompactInstructionData::System(
        blockzilla_format::ArchiveV2SystemInstructionData::CreateAccount { lamports, .. },
    ) = &instruction.data
    else {
        return false;
    };
    if *lamports == 0
        || instruction.program_id != SYSTEM_PROGRAM_ID
        || instruction.program_id_index != 2
        || instruction.instruction_index != 0
        || instruction.account_indexes.as_slice() != [0, 1]
    {
        return false;
    }
    let LaunchReplayError::SystemMutation {
        transaction_index,
        instruction_index,
        source: LaunchSystemError::AccountAlreadyInUse { pubkey },
        ..
    } = error
    else {
        return false;
    };
    let Some(oracle) = &transaction.balance_oracle else {
        return false;
    };
    if oracle.pre_balances.len() != 3 || oracle.post_balances.len() != 3 {
        return false;
    }

    *transaction_index == transaction.tx_index
        && *instruction_index == instruction.instruction_index
        && *pubkey == transaction_metas.account_keys[1]
        && oracle.fee > 0
        && oracle.pre_balances[0].checked_sub(oracle.post_balances[0]) == Some(oracle.fee)
        && oracle.pre_balances[1] > 0
        && oracle.pre_balances[1] == oracle.post_balances[1]
        && oracle.pre_balances[2] == oracle.post_balances[2]
}

fn instruction_account_metas(
    slot: u64,
    transaction_index: u32,
    instruction: &CompactInstructionProbe,
    transaction_metas: &TransactionAccountMetaLayout<'_>,
) -> Result<SmallVec<[LaunchAccountMeta; 8]>, LaunchReplayError> {
    let mut metas = SmallVec::with_capacity(instruction.account_indexes.len());
    for (account_position, account_index) in instruction.account_indexes.iter().enumerate() {
        let meta = transaction_metas.get(*account_index as usize).ok_or(
            LaunchReplayError::UnresolvedInstructionAccount {
                slot,
                transaction_index,
                instruction_index: instruction.instruction_index,
                account_position,
            },
        )?;
        metas.push(meta);
    }
    Ok(metas)
}

fn begin_account_diff_journal(
    accounts: &CowAccountMap,
    metas: &[LaunchAccountMeta],
    absent_accounts: &AbsentOverlayAccounts,
) -> AccountDiffJournal {
    let mut journal = AccountDiffJournal::new();
    for meta in metas {
        if meta.is_writable {
            let before = (!absent_accounts.contains(&meta.pubkey)).then(|| {
                accounts
                    .get(&meta.pubkey)
                    .expect("instruction accounts were loaded into the overlay")
            });
            journal.record_first_write(meta.pubkey, before);
        }
    }
    journal
}

fn reconcile_absent_overlay_accounts(
    accounts: &CowAccountMap,
    metas: &[LaunchAccountMeta],
    absent_accounts: &mut AbsentOverlayAccounts,
) {
    for meta in metas {
        if absent_accounts.contains(&meta.pubkey)
            && accounts
                .get(&meta.pubkey)
                .is_some_and(|account| !is_balance_only_system_account(account))
        {
            absent_accounts.remove(&meta.pubkey);
        }
    }
}

fn capture_instruction_diff(
    slot: u64,
    transaction_index: u32,
    instruction: &CompactInstructionProbe,
    instruction_path_index: u16,
    program_id: [u8; 32],
    journal: AccountDiffJournal,
    accounts: &CowAccountMap,
    metas: &[LaunchAccountMeta],
    absent_accounts: &mut AbsentOverlayAccounts,
) -> InstructionDiff {
    reconcile_absent_overlay_accounts(accounts, metas, absent_accounts);
    journal.finish(
        DiffBoundary {
            slot,
            transaction_index,
            trace_index: instruction.instruction_index,
            stack_height: 1,
            instruction_path: InlineInstructionPath::from_slice(&[instruction_path_index]),
        },
        program_id,
        DiffDisposition::Speculative,
        DiffPolicy::default(),
        |pubkey| {
            (!absent_accounts.contains(pubkey)).then(|| {
                accounts
                    .get(pubkey)
                    .expect("journaled accounts remain loaded in the overlay")
            })
        },
    )
}

fn increment(value: u64) -> Result<u64, LaunchReplayError> {
    value
        .checked_add(1)
        .ok_or(LaunchReplayError::CounterOverflow)
}

fn record_bank_sysvar_accounts(
    recorded: &mut BTreeSet<[u8; 32]>,
    written_accounts: impl IntoIterator<Item = [u8; 32]>,
    phase: BankSysvarWritePhase,
) -> bool {
    let should_record = match phase {
        BankSysvarWritePhase::Child { epoch_transition } => {
            epoch_transition || !recorded.contains(&CLOCK_SYSVAR_ID)
        }
        BankSysvarWritePhase::SlotHistory => !recorded.contains(&SLOT_HISTORY_SYSVAR_ID),
    };
    if should_record {
        recorded.extend(written_accounts);
    }
    should_record
}

fn native_builtin_account(name: &[u8]) -> AccountSnapshot {
    AccountSnapshot {
        lamports: 1,
        owner: NATIVE_LOADER_ID,
        executable: true,
        rent_epoch: 0,
        data: name.to_vec().into(),
    }
}

fn historical_transaction_failure(
    error: &LaunchReplayError,
) -> Option<LaunchTransactionFailureReason> {
    if !is_historical_transaction_failure(error) {
        return None;
    }
    match error {
        LaunchReplayError::ConfigMutation { source, .. } => {
            Some(LaunchTransactionFailureReason::Config(source.clone()))
        }
        LaunchReplayError::SystemMutation { source, .. } => {
            Some(LaunchTransactionFailureReason::System(source.clone()))
        }
        LaunchReplayError::StakeMutation { source, .. } => {
            Some(LaunchTransactionFailureReason::Stake(source.clone()))
        }
        LaunchReplayError::VoteMutation { source, .. } => {
            Some(LaunchTransactionFailureReason::Vote(source.to_string()))
        }
        LaunchReplayError::BpfLoaderMutation { source, .. } => Some(
            LaunchTransactionFailureReason::BpfLoader(source.to_string()),
        ),
        LaunchReplayError::BpfProgramExecution { source, .. } => Some(
            LaunchTransactionFailureReason::BpfProgram(source.to_string()),
        ),
        _ => unreachable!("failure classification was checked above"),
    }
}

fn is_historical_transaction_failure(error: &LaunchReplayError) -> bool {
    match error {
        LaunchReplayError::ConfigMutation { .. } => true,
        LaunchReplayError::SystemMutation { source, .. } => !matches!(
            source,
            LaunchSystemError::NonceRequiresBankSysvars { .. }
                | LaunchSystemError::PostLaunchVariant { .. }
        ),
        LaunchReplayError::StakeMutation { source, .. } => {
            !matches!(source, LaunchStakeError::UnsupportedVariant { .. })
        }
        LaunchReplayError::VoteMutation { source, .. } => {
            !matches!(source, LaunchVoteError::UnsupportedInstruction(_))
        }
        LaunchReplayError::BpfLoaderMutation { source, .. } => {
            !matches!(source, LaunchBpfLoaderError::ExecutableInvocation { .. })
        }
        LaunchReplayError::BpfProgramExecution {
            archived_outcome,
            source,
            ..
        } => {
            *archived_outcome != CompactArchivedTransactionOutcome::Succeeded
                && source.is_historical_instruction_failure()
        }
        _ => false,
    }
}

fn replay_error_position(error: &LaunchReplayError) -> (Option<u32>, Option<u32>) {
    match error {
        LaunchReplayError::UnsupportedMessageVersion {
            transaction_index, ..
        }
        | LaunchReplayError::InvalidMessageHeader {
            transaction_index, ..
        } => (Some(*transaction_index), None),
        LaunchReplayError::UnsupportedProgram {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::MissingVoteAccount {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::UnresolvedInstructionAccount {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::AbsentVoteAccount {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::WrongVoteOwner {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::UnsupportedVoteEncoding {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::UnsupportedConfigEncoding {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::UnsupportedSystemEncoding {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::UnsupportedStakeEncoding {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::UnsupportedBpfEncoding {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::InstructionPathIndexOverflow {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::VoteMutation {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::ConfigMutation {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::SystemMutation {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::StakeMutation {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::BpfLoaderMutation {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::BpfProgramLoad {
            transaction_index,
            instruction_index,
            ..
        }
        | LaunchReplayError::BpfProgramExecution {
            transaction_index,
            instruction_index,
            ..
        } => (Some(*transaction_index), Some(*instruction_index)),
        _ => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, path::PathBuf};

    use blockzilla_format::{
        ArchiveV2SystemInstructionData, CompactMessageHeader, WincodeArchiveV2GenesisEpochSchedule,
        WincodeArchiveV2GenesisFeeParams, WincodeArchiveV2GenesisInflationParams,
        WincodeArchiveV2GenesisPohParams, WincodeArchiveV2GenesisRentParams,
    };
    use blockzilla_read_sdk::GenerationBinding;
    use serde::Serialize;

    use super::*;
    use crate::{
        CompactGenesisAccount, CompactGenesisBuiltin, CompactInstructionProbe,
        CompactRecentBlockhashProbe, CompactTransactionProbe, LaunchStakeAuthorized,
        LaunchStakeLockup, LaunchStakeMeta, LaunchStakeState, decode_launch_stake_state,
    };

    const VOTE_PROGRAM: [u8; 32] = VOTE_PROGRAM_ID;
    const VOTE_ACCOUNT: [u8; 32] = [7; 32];
    const SYSTEM_BASE: [u8; 32] = [
        204, 241, 115, 109, 41, 173, 110, 48, 24, 113, 210, 213, 163, 78, 1, 112, 146, 114, 235,
        220, 96, 185, 184, 85, 163, 27, 124, 48, 54, 250, 233, 54,
    ];
    const SYSTEM_TARGET: [u8; 32] = [
        11, 212, 126, 90, 51, 90, 195, 254, 212, 46, 210, 147, 188, 141, 145, 180, 248, 241, 36,
        115, 78, 149, 57, 42, 47, 156, 168, 112, 153, 138, 68, 29,
    ];
    const STAKE_PROGRAM: [u8; 32] = [
        6, 161, 216, 23, 145, 55, 84, 42, 152, 52, 55, 189, 254, 42, 122, 178, 85, 127, 83, 92,
        138, 120, 114, 43, 104, 164, 157, 192, 0, 0, 0, 0,
    ];
    const STAKE_SOURCE: [u8; 32] = [42; 32];
    const WRONG_STAKE_AUTHORITY: [u8; 32] = [43; 32];
    const CONFIG_ACCOUNT: [u8; 32] = [45; 32];
    /// `breakbUwq5541KXXmMEgaDBEwgWYiVe23P3u3n7qod3`, the first legacy
    /// loader target observed at epoch-34 slot 15,105,072.
    const OBSERVED_BPF_PROGRAM_ACCOUNT: [u8; 32] = [
        8, 237, 226, 119, 207, 115, 25, 232, 232, 21, 21, 5, 127, 156, 69, 65, 82, 132, 240, 233,
        17, 144, 22, 232, 187, 63, 148, 89, 104, 6, 116, 34,
    ];
    const OBSERVED_BPF_PAYER: [u8; 32] = [46; 32];
    const UNSUPPORTED_PROGRAM: [u8; 32] = [47; 32];
    const OBSERVED_BPF_SLOT: u64 = 15_105_072;
    /// Exact deployment length reconstructed from all 17 Compact Writes.
    const OBSERVED_BPF_ACCOUNT_DATA_LEN: usize = 15_464;
    const STAKE_RENT_RESERVE: u64 = 2_282_880;
    const SPLIT_LAMPORTS: u64 = 1_000_000_000_000_000;
    const STAKE_SOURCE_LAMPORTS: u64 = 4_999_999_980_909_120;

    #[test]
    fn instruction_diff_subsets_capture_only_writable_accounts() {
        const READONLY: [u8; 32] = [1; 32];
        const WRITABLE: [u8; 32] = [2; 32];
        const CREATED_WRITABLE: [u8; 32] = [3; 32];
        const CREATED_READONLY: [u8; 32] = [4; 32];

        let structural_account = || AccountSnapshot {
            owner: [0xaa; 32],
            data: vec![7].into(),
            ..default_system_account()
        };
        let mut accounts = AccountMap::from([
            (READONLY, structural_account()),
            (WRITABLE, structural_account()),
            (CREATED_WRITABLE, structural_account()),
            (CREATED_READONLY, structural_account()),
        ]);
        let metas = [
            LaunchAccountMeta {
                pubkey: READONLY,
                is_signer: false,
                is_writable: false,
            },
            LaunchAccountMeta {
                pubkey: WRITABLE,
                is_signer: false,
                is_writable: true,
            },
            LaunchAccountMeta {
                pubkey: CREATED_WRITABLE,
                is_signer: false,
                is_writable: true,
            },
            LaunchAccountMeta {
                pubkey: CREATED_READONLY,
                is_signer: false,
                is_writable: false,
            },
        ];
        let mut absent_accounts = AbsentOverlayAccounts::new();
        absent_accounts.insert(CREATED_WRITABLE);
        absent_accounts.insert(CREATED_READONLY);

        let cow = CowAccountMap::detached(accounts.clone());
        let journal = begin_account_diff_journal(&cow, &metas, &absent_accounts);
        accounts
            .get_mut(&WRITABLE)
            .expect("writable fixture exists")
            .data
            .set_from_slice(&[8]);
        accounts
            .get_mut(&READONLY)
            .expect("readonly fixture exists")
            .data
            .set_from_slice(&[9]);
        let cow = CowAccountMap::detached(accounts.clone());
        reconcile_absent_overlay_accounts(&cow, &metas, &mut absent_accounts);
        let diff = journal.finish(
            DiffBoundary {
                slot: 1,
                transaction_index: 2,
                trace_index: 3,
                stack_height: 1,
                instruction_path: InlineInstructionPath::from_slice(&[0]),
            },
            [9; 32],
            DiffDisposition::Speculative,
            DiffPolicy::default(),
            |pubkey| accounts.get(pubkey),
        );
        assert_eq!(
            diff.accounts
                .iter()
                .map(|account| account.pubkey)
                .collect::<Vec<_>>(),
            [WRITABLE, CREATED_WRITABLE]
        );
        assert!(!diff.accounts[0].created);
        assert!(diff.accounts[1].created);
        assert!(absent_accounts.as_slice().is_empty());
    }

    #[test]
    fn disabled_generation_measurement_executes_without_recording_time() {
        let (value, elapsed) =
            measure_generation_phase::<DisabledLaunchGenerationMetrics, _>(|| 42_u64);

        assert_eq!(value, 42);
        assert_eq!(elapsed, Duration::ZERO);

        let commit = AccountBatchCommit {
            inserted: 1,
            updated: 2,
            deleted: 3,
            patched: 4,
        };
        let mut batch_metrics = LaunchGenerationAccountBatchMetrics::default();
        record_generation_account_batch::<DisabledLaunchGenerationMetrics>(
            &mut batch_metrics,
            commit,
            Duration::from_millis(5),
        );
        assert_eq!(
            batch_metrics,
            LaunchGenerationAccountBatchMetrics::default()
        );

        record_generation_account_batch::<
            LaunchGenerationMetricsVisitor<fn(&LaunchGenerationMetrics)>,
        >(&mut batch_metrics, commit, Duration::from_millis(5));
        assert_eq!(batch_metrics.commits, 1);
        assert_eq!(batch_metrics.inserted, 1);
        assert_eq!(batch_metrics.updated, 2);
        assert_eq!(batch_metrics.deleted, 3);
        assert_eq!(batch_metrics.patched, 4);
        assert_eq!(batch_metrics.duration, Duration::from_millis(5));
    }

    #[test]
    fn generation_metrics_visitor_receives_one_complete_boundary() {
        let expected = LaunchGenerationMetrics {
            epoch: 7,
            generation_id: "epoch-7".to_owned(),
            generation_digest: [7; 32],
            first_slot: 70,
            last_slot: 79,
            slots_visited: 10,
            transactions_visited: 20,
            instructions_visited: 30,
            compact_compressed_bytes: 40,
            account_registry_start: 50,
            account_registry_end: 60,
            changed_accounts_start: 7,
            changed_accounts_end: 9,
            committed_transactions: 18,
            failed_transactions: 2,
            committed_instructions: 27,
            rolled_back_instructions: 3,
            account_batch_commits: 16,
            account_batch_inserted: 4,
            account_batch_updated: 8,
            account_batch_deleted: 2,
            account_batch_patched: 2,
            account_batch_commit: Duration::from_millis(1),
            checkpoint_published: true,
            generation_wall: Duration::from_millis(11),
            compact_visit: Duration::from_millis(7),
            compact_decode_visit: Duration::from_millis(2),
            replay: Duration::from_millis(5),
            checkpoint_encode: Duration::from_millis(1),
            checkpoint_publish: Duration::from_millis(2),
            checkpoint_state_hash: Duration::from_millis(1),
        };
        let mut recorded = Vec::new();
        let mut visitor = LaunchGenerationMetricsVisitor(|metrics: &LaunchGenerationMetrics| {
            recorded.push(metrics.clone());
        });

        visitor.record(expected.clone());
        drop(visitor);

        assert_eq!(recorded, [expected]);
    }

    #[allow(dead_code)]
    #[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
    enum FixtureVoteInstruction {
        Initialize(()),
        Authorize((), ()),
        Vote(FixtureVote),
        Withdraw(u64),
        UpdateNode([u8; 32]),
        UpdateCommission(u8),
    }

    #[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
    struct FixtureVote {
        slots: Vec<u64>,
        hash: [u8; 32],
        timestamp: Option<i64>,
    }

    #[allow(dead_code)]
    #[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
    enum FixtureVoteStateVersions {
        Legacy(()),
        Current(Box<FixtureVoteState>),
    }

    #[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
    struct FixtureVoteState {
        node_pubkey: [u8; 32],
        authorized_withdrawer: [u8; 32],
        commission: u8,
        votes: VecDeque<FixtureLockout>,
        root_slot: Option<u64>,
        authorized_voters: FixtureAuthorizedVoters,
        prior_voters: FixturePriorVoters,
        epoch_credits: Vec<(u64, u64, u64)>,
        last_timestamp: FixtureBlockTimestamp,
    }

    #[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
    struct FixtureLockout {
        slot: u64,
        confirmation_count: u32,
    }

    #[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
    struct FixtureAuthorizedVoters {
        authorized_voters: BTreeMap<u64, [u8; 32]>,
    }

    #[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
    struct FixturePriorVoters {
        buf: [([u8; 32], u64, u64); 32],
        idx: usize,
        is_empty: bool,
    }

    #[derive(Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
    struct FixtureBlockTimestamp {
        slot: u64,
        timestamp: i64,
    }

    fn exact_genesis() -> CompactGenesisProbe {
        CompactGenesisProbe {
            source: CompactGenesisSource::ExactGenesisBin,
            genesis_hash: [1; 32],
            genesis_bin_len: 1,
            creation_time_unix: 0,
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
                padding: 0.0_f64.to_le_bytes(),
            },
            inflation_storage: Some(0.0),
            epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
                slots_per_epoch: 432_000,
                leader_schedule_slot_offset: 432_000,
                warmup: false,
                first_normal_epoch: 0,
                first_normal_slot: 0,
            },
            accounts: vec![CompactGenesisAccount {
                pubkey: VOTE_ACCOUNT,
                lamports: 1,
                owner: VOTE_PROGRAM,
                executable: false,
                rent_epoch: 0,
                data: Vec::new(),
            }],
            builtins: vec![
                CompactGenesisBuiltin {
                    key: VOTE_BUILTIN_NAME.to_owned(),
                    pubkey: VOTE_PROGRAM,
                },
                CompactGenesisBuiltin {
                    key: CONFIG_BUILTIN_NAME.to_owned(),
                    pubkey: CONFIG_PROGRAM_ID,
                },
                CompactGenesisBuiltin {
                    key: SYSTEM_BUILTIN_NAME.to_owned(),
                    pubkey: SYSTEM_PROGRAM_ID,
                },
                CompactGenesisBuiltin {
                    key: STAKE_BUILTIN_NAME.to_owned(),
                    pubkey: STAKE_PROGRAM,
                },
            ],
            reward_pools: Vec::new(),
        }
    }

    fn exact_genesis_with_system_base() -> CompactGenesisProbe {
        let mut genesis = exact_genesis();
        genesis.accounts.push(CompactGenesisAccount {
            pubkey: SYSTEM_BASE,
            lamports: 19_090_880,
            owner: SYSTEM_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: Vec::new(),
        });
        genesis
    }

    fn exact_genesis_with_config_account() -> CompactGenesisProbe {
        let mut genesis = exact_genesis();
        genesis.accounts.push(CompactGenesisAccount {
            pubkey: CONFIG_ACCOUNT,
            lamports: 960_480,
            owner: CONFIG_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: vec![0; 10],
        });
        genesis
    }

    fn exact_genesis_with_initialized_stake_source() -> CompactGenesisProbe {
        let mut genesis = exact_genesis_with_system_base();
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: STAKE_RENT_RESERVE,
            authorized: LaunchStakeAuthorized {
                staker: SYSTEM_BASE,
                withdrawer: [44; 32],
            },
            lockup: LaunchStakeLockup::default(),
        });
        let encoded = wincode::serialize(&state).unwrap();
        let mut data = vec![0; 200];
        data[..encoded.len()].copy_from_slice(&encoded);
        genesis.accounts.push(CompactGenesisAccount {
            pubkey: STAKE_SOURCE,
            lamports: STAKE_SOURCE_LAMPORTS,
            owner: STAKE_PROGRAM,
            executable: false,
            rent_epoch: 0,
            data,
        });
        genesis
    }

    fn exact_genesis_with_observed_bpf_program(executable: bool) -> CompactGenesisProbe {
        let mut genesis = exact_genesis();
        genesis.accounts.push(CompactGenesisAccount {
            pubkey: OBSERVED_BPF_PROGRAM_ACCOUNT,
            lamports: 100_000_000,
            owner: BPF_LOADER_PROGRAM_ID,
            executable,
            rent_epoch: 0,
            data: vec![0; OBSERVED_BPF_ACCOUNT_DATA_LEN],
        });
        genesis
    }

    fn slot_with_instruction(instruction_index: u32, account_indexes: Vec<u8>) -> CompactSlotProbe {
        CompactSlotProbe {
            block_id: 0,
            slot: 1,
            parent_slot: 0,
            block_time: None,
            block_height: None,
            blockhash_id: 1,
            blockhash: [2; 32],
            previous_blockhash_id: 0,
            previous_blockhash: [1; 32],
            transaction_count: 1,
            transactions: vec![CompactTransactionProbe {
                tx_index: 0,
                row_flags: 0,
                archived_outcome: crate::CompactArchivedTransactionOutcome::Unknown,
                balance_oracle: None,
                signature_count: 1,
                version: CompactMessageVersion::Legacy,
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: smallvec::smallvec![VOTE_ACCOUNT, VOTE_PROGRAM],
                recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
                address_table_lookups: Vec::new(),
                instructions: smallvec::smallvec![CompactInstructionProbe {
                    instruction_index,
                    program_id_index: 1,
                    program_id: VOTE_PROGRAM,
                    account_indexes: account_indexes.into(),
                    data: CompactInstructionData::Raw(SmallVec::new()),
                }],
            }],
        }
    }

    fn initialized_vote_account_data() -> Vec<u8> {
        let state = FixtureVoteStateVersions::Current(Box::new(FixtureVoteState {
            node_pubkey: [8; 32],
            authorized_withdrawer: [9; 32],
            commission: 100,
            votes: VecDeque::new(),
            root_slot: None,
            authorized_voters: FixtureAuthorizedVoters {
                authorized_voters: BTreeMap::from([(0, VOTE_ACCOUNT)]),
            },
            prior_voters: FixturePriorVoters {
                buf: [([0; 32], 0, 0); 32],
                idx: 31,
                is_empty: true,
            },
            epoch_credits: Vec::new(),
            last_timestamp: FixtureBlockTimestamp {
                slot: 0,
                timestamp: 0,
            },
        }));
        let encoded = wincode::serialize(&state).unwrap();
        let mut account_data = vec![0xa5; 3_731];
        account_data[..encoded.len()].copy_from_slice(&encoded);
        account_data
    }

    fn vote_instruction_data(slots: Vec<u64>) -> Vec<u8> {
        vote_instruction_data_with_timestamp(slots, None)
    }

    fn vote_instruction_data_with_timestamp(slots: Vec<u64>, timestamp: Option<i64>) -> Vec<u8> {
        wincode::serialize(&FixtureVoteInstruction::Vote(FixtureVote {
            slots,
            hash: [3; 32],
            timestamp,
        }))
        .unwrap()
    }

    fn fixture_instruction(
        instruction_index: u32,
        program_id: [u8; 32],
        data: Vec<u8>,
    ) -> CompactInstructionProbe {
        CompactInstructionProbe {
            instruction_index,
            program_id_index: 1,
            program_id,
            account_indexes: smallvec::smallvec![0],
            data: CompactInstructionData::Raw(data.into()),
        }
    }

    fn fixture_transaction(
        tx_index: u32,
        instructions: Vec<CompactInstructionProbe>,
    ) -> CompactTransactionProbe {
        CompactTransactionProbe {
            tx_index,
            row_flags: 0,
            archived_outcome: crate::CompactArchivedTransactionOutcome::Unknown,
            balance_oracle: None,
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec::smallvec![VOTE_ACCOUNT, VOTE_PROGRAM],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: instructions.into(),
        }
    }

    fn config_transaction(data: Vec<u8>) -> CompactTransactionProbe {
        CompactTransactionProbe {
            tx_index: 0,
            row_flags: 0,
            archived_outcome: crate::CompactArchivedTransactionOutcome::Unknown,
            balance_oracle: None,
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec::smallvec![CONFIG_ACCOUNT, CONFIG_PROGRAM_ID],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![CompactInstructionProbe {
                instruction_index: 0,
                program_id_index: 1,
                program_id: CONFIG_PROGRAM_ID,
                account_indexes: smallvec::smallvec![0],
                data: CompactInstructionData::Raw(data.into()),
            }],
        }
    }

    fn system_then_unsigned_config_transaction() -> CompactTransactionProbe {
        CompactTransactionProbe {
            tx_index: 0,
            row_flags: 0,
            archived_outcome: crate::CompactArchivedTransactionOutcome::Unknown,
            balance_oracle: None,
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys: smallvec::smallvec![
                SYSTEM_BASE,
                SYSTEM_TARGET,
                CONFIG_ACCOUNT,
                SYSTEM_PROGRAM_ID,
                CONFIG_PROGRAM_ID,
            ],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![
                CompactInstructionProbe {
                    instruction_index: 0,
                    program_id_index: 3,
                    program_id: SYSTEM_PROGRAM_ID,
                    account_indexes: smallvec::smallvec![1, 0],
                    data: CompactInstructionData::System(
                        ArchiveV2SystemInstructionData::AllocateWithSeed {
                            base: SYSTEM_BASE,
                            seed: "1".to_owned(),
                            space: 200,
                            owner: STAKE_PROGRAM,
                        },
                    ),
                },
                CompactInstructionProbe {
                    instruction_index: 1,
                    program_id_index: 4,
                    program_id: CONFIG_PROGRAM_ID,
                    account_indexes: smallvec::smallvec![2],
                    data: CompactInstructionData::Raw(smallvec::smallvec![0, 1]),
                },
            ],
        }
    }

    fn allocate_with_seed_transaction(
        include_unsupported_program: bool,
    ) -> CompactTransactionProbe {
        let mut instructions = smallvec::smallvec![CompactInstructionProbe {
            instruction_index: 0,
            program_id_index: 2,
            program_id: SYSTEM_PROGRAM_ID,
            account_indexes: smallvec::smallvec![1, 0],
            data: CompactInstructionData::System(
                ArchiveV2SystemInstructionData::AllocateWithSeed {
                    base: SYSTEM_BASE,
                    seed: "1".to_owned(),
                    space: 200,
                    owner: STAKE_PROGRAM,
                },
            ),
        }];
        if include_unsupported_program {
            instructions.push(CompactInstructionProbe {
                instruction_index: 1,
                program_id_index: 3,
                program_id: [6; 32],
                account_indexes: smallvec::smallvec![1],
                data: CompactInstructionData::Raw(SmallVec::new()),
            });
        }
        CompactTransactionProbe {
            tx_index: 0,
            row_flags: 0,
            archived_outcome: crate::CompactArchivedTransactionOutcome::Unknown,
            balance_oracle: None,
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys: smallvec::smallvec![
                SYSTEM_BASE,
                SYSTEM_TARGET,
                SYSTEM_PROGRAM_ID,
                [6; 32]
            ],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions,
        }
    }

    fn epoch_11_set_lockup_transaction() -> CompactTransactionProbe {
        CompactTransactionProbe {
            tx_index: 0,
            row_flags: 0,
            archived_outcome: crate::CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: None,
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 1,
            },
            // The Compact fixture at slot 4,831,848 has this same three-key
            // shape: readonly signer/custodian, writable stake, Stake program.
            account_keys: smallvec::smallvec![SYSTEM_BASE, STAKE_SOURCE, STAKE_PROGRAM],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![CompactInstructionProbe {
                instruction_index: 0,
                program_id_index: 2,
                program_id: STAKE_PROGRAM,
                account_indexes: smallvec::smallvec![1, 0],
                data: CompactInstructionData::Raw(smallvec::smallvec![
                    6, 0, 0, 0, // SetLockup
                    0, // unix_timestamp: None
                    1, 177, 0, 0, 0, 0, 0, 0, 0, // epoch: Some(177)
                    0, // custodian: None
                ]),
            }],
        }
    }

    fn split_transaction(tx_index: u32, correct_authority: bool) -> CompactTransactionProbe {
        let (account_keys, header, authority_index) = if correct_authority {
            (
                vec![
                    SYSTEM_BASE,
                    SYSTEM_TARGET,
                    STAKE_SOURCE,
                    SYSTEM_PROGRAM_ID,
                    STAKE_PROGRAM,
                ],
                CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 2,
                },
                0,
            )
        } else {
            (
                vec![
                    SYSTEM_BASE,
                    WRONG_STAKE_AUTHORITY,
                    SYSTEM_TARGET,
                    STAKE_SOURCE,
                    SYSTEM_PROGRAM_ID,
                    STAKE_PROGRAM,
                ],
                CompactMessageHeader {
                    num_required_signatures: 2,
                    num_readonly_signed_accounts: 1,
                    num_readonly_unsigned_accounts: 2,
                },
                1,
            )
        };
        let target_index = if correct_authority { 1 } else { 2 };
        let source_index = if correct_authority { 2 } else { 3 };
        let system_index = if correct_authority { 3 } else { 4 };
        let stake_index = if correct_authority { 4 } else { 5 };
        let mut stake_data = 3_u32.to_le_bytes().to_vec();
        stake_data.extend_from_slice(&SPLIT_LAMPORTS.to_le_bytes());
        CompactTransactionProbe {
            tx_index,
            row_flags: 0,
            archived_outcome: crate::CompactArchivedTransactionOutcome::Unknown,
            balance_oracle: None,
            signature_count: header.num_required_signatures,
            version: CompactMessageVersion::Legacy,
            header,
            account_keys: account_keys.into(),
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![
                CompactInstructionProbe {
                    instruction_index: 0,
                    program_id_index: system_index,
                    program_id: SYSTEM_PROGRAM_ID,
                    account_indexes: smallvec::smallvec![target_index, 0],
                    data: CompactInstructionData::System(
                        ArchiveV2SystemInstructionData::AllocateWithSeed {
                            base: SYSTEM_BASE,
                            seed: "1".to_owned(),
                            space: 200,
                            owner: STAKE_PROGRAM,
                        },
                    ),
                },
                CompactInstructionProbe {
                    instruction_index: 1,
                    program_id_index: stake_index,
                    program_id: STAKE_PROGRAM,
                    account_indexes: smallvec::smallvec![
                        source_index,
                        target_index,
                        authority_index
                    ],
                    data: CompactInstructionData::Raw(stake_data.into()),
                },
            ],
        }
    }

    fn observed_bpf_write(offset: u32, payload: &[u8]) -> Vec<u8> {
        let mut data = Vec::with_capacity(16 + payload.len());
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&offset.to_le_bytes());
        data.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        data.extend_from_slice(payload);
        data
    }

    fn observed_bpf_loader_transaction(
        instruction_data: Vec<u8>,
        include_unsupported_program: bool,
    ) -> CompactTransactionProbe {
        let mut account_keys = smallvec::smallvec![
            OBSERVED_BPF_PAYER,
            OBSERVED_BPF_PROGRAM_ACCOUNT,
            BPF_LOADER_PROGRAM_ID,
        ];
        let mut instructions = smallvec::smallvec![CompactInstructionProbe {
            instruction_index: 0,
            program_id_index: 2,
            program_id: BPF_LOADER_PROGRAM_ID,
            account_indexes: smallvec::smallvec![1],
            data: CompactInstructionData::Raw(instruction_data.into()),
        }];
        if include_unsupported_program {
            account_keys.push(UNSUPPORTED_PROGRAM);
            instructions.push(CompactInstructionProbe {
                instruction_index: 1,
                program_id_index: 3,
                program_id: UNSUPPORTED_PROGRAM,
                account_indexes: smallvec::smallvec![1],
                data: CompactInstructionData::Raw(SmallVec::new()),
            });
        }
        let mut pre_balances = smallvec::smallvec![10_000, 100_000_000, 1];
        let mut post_balances = smallvec::smallvec![5_000, 100_000_000, 1];
        if include_unsupported_program {
            pre_balances.push(0);
            post_balances.push(0);
        }
        CompactTransactionProbe {
            tx_index: 0,
            row_flags: 1,
            archived_outcome: crate::CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                fee: 5_000,
                pre_balances,
                post_balances,
            }),
            signature_count: 2,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1 + u8::from(include_unsupported_program),
            },
            account_keys,
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions,
        }
    }

    fn observed_bpf_slot(transaction: CompactTransactionProbe) -> CompactSlotProbe {
        CompactSlotProbe {
            block_id: 0,
            slot: OBSERVED_BPF_SLOT,
            parent_slot: OBSERVED_BPF_SLOT - 1,
            block_time: None,
            block_height: None,
            blockhash_id: 1,
            blockhash: [2; 32],
            previous_blockhash_id: 0,
            previous_blockhash: [1; 32],
            transaction_count: 1,
            transactions: vec![transaction],
        }
    }

    fn slot_with_transactions(transactions: Vec<CompactTransactionProbe>) -> CompactSlotProbe {
        CompactSlotProbe {
            block_id: 0,
            slot: 105_368,
            parent_slot: 105_367,
            block_time: None,
            block_height: None,
            blockhash_id: 1,
            blockhash: [2; 32],
            previous_blockhash_id: 0,
            previous_blockhash: [1; 32],
            transaction_count: transactions.len() as u32,
            transactions,
        }
    }

    fn transient_system_transaction(
        tx_index: u32,
        archived_outcome: CompactArchivedTransactionOutcome,
        pre_balances: [u64; 3],
        post_balances: [u64; 3],
        instruction: ArchiveV2SystemInstructionData,
        account_indexes: SmallVec<[u8; 8]>,
    ) -> CompactTransactionProbe {
        CompactTransactionProbe {
            tx_index,
            row_flags: 1,
            archived_outcome,
            balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                fee: pre_balances[0]
                    .saturating_sub(post_balances[0])
                    .saturating_sub(match &instruction {
                        ArchiveV2SystemInstructionData::Transfer { lamports } => *lamports,
                        _ => 0,
                    }),
                pre_balances: pre_balances.into_iter().collect(),
                post_balances: post_balances.into_iter().collect(),
            }),
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec::smallvec![SYSTEM_BASE, SYSTEM_TARGET, SYSTEM_PROGRAM_ID],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![CompactInstructionProbe {
                instruction_index: 0,
                program_id_index: 2,
                program_id: SYSTEM_PROGRAM_ID,
                account_indexes,
                data: CompactInstructionData::System(instruction),
            }],
        }
    }

    #[test]
    fn epoch_63_activates_pda_and_cpi_syscalls() {
        assert!(!bpf_pda_and_cpi_syscalls_supported(62));
        assert!(bpf_pda_and_cpi_syscalls_supported(63));
        assert!(bpf_pda_and_cpi_syscalls_supported(64));
    }

    #[test]
    fn exact_genesis_is_required() {
        let mut genesis = exact_genesis();
        genesis.source = CompactGenesisSource::InlineLegacy;
        let error = LaunchReplay::from_genesis(0, Some(&genesis), true).unwrap_err();
        assert!(matches!(
            error,
            LaunchReplayError::InexactGenesis(CompactGenesisSource::InlineLegacy)
        ));
    }

    #[test]
    fn duplicate_genesis_accounts_fail_closed() {
        let mut genesis = exact_genesis();
        genesis.accounts.push(genesis.accounts[0].clone());
        let error = LaunchReplay::from_genesis(0, Some(&genesis), true).unwrap_err();
        assert!(matches!(
            error,
            LaunchReplayError::DuplicateGenesisAccount {
                pubkey: VOTE_ACCOUNT
            }
        ));
    }

    #[test]
    fn genesis_declared_builtins_and_bank_sysvars_are_materialized() {
        let genesis = exact_genesis();
        let replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();

        for (pubkey, name) in [
            (VOTE_PROGRAM, VOTE_BUILTIN_NAME),
            (CONFIG_PROGRAM_ID, CONFIG_BUILTIN_NAME),
            (SYSTEM_PROGRAM_ID, SYSTEM_BUILTIN_NAME),
            (STAKE_PROGRAM, STAKE_BUILTIN_NAME),
        ] {
            let account = &replay.outcome.account_state[&pubkey];
            assert_eq!(account.lamports, 1);
            assert_eq!(account.owner, NATIVE_LOADER_ID);
            assert!(account.executable);
            assert_eq!(account.rent_epoch, 0);
            assert_eq!(account.data, name.as_bytes());
        }
        for pubkey in [
            crate::FEES_SYSVAR_ID,
            crate::STAKE_HISTORY_SYSVAR_ID,
            crate::CLOCK_SYSVAR_ID,
            crate::RENT_SYSVAR_ID,
            crate::EPOCH_SCHEDULE_SYSVAR_ID,
            crate::RECENT_BLOCKHASHES_SYSVAR_ID,
        ] {
            let account = &replay.outcome.account_state[&pubkey];
            assert_eq!(account.lamports, 1);
            assert_eq!(account.owner, crate::SYSVAR_OWNER_ID);
            assert!(!account.executable);
            assert_eq!(account.rent_epoch, 0);
        }
        assert_eq!(replay.outcome.account_state.len(), 11);
    }

    #[test]
    fn bank_sysvar_account_recording_skips_repeated_fixed_sets_and_survives_restore() {
        let ordinary_child = [
            crate::CLOCK_SYSVAR_ID,
            crate::FEES_SYSVAR_ID,
            crate::RECENT_BLOCKHASHES_SYSVAR_ID,
        ];
        let mut recorded = BTreeSet::new();

        assert!(record_bank_sysvar_accounts(
            &mut recorded,
            [crate::SLOT_HISTORY_SYSVAR_ID],
            BankSysvarWritePhase::SlotHistory,
        ));
        assert!(!record_bank_sysvar_accounts(
            &mut recorded,
            [crate::SLOT_HISTORY_SYSVAR_ID],
            BankSysvarWritePhase::SlotHistory,
        ));
        assert!(record_bank_sysvar_accounts(
            &mut recorded,
            ordinary_child,
            BankSysvarWritePhase::Child {
                epoch_transition: false,
            },
        ));
        assert!(!record_bank_sysvar_accounts(
            &mut recorded,
            ordinary_child,
            BankSysvarWritePhase::Child {
                epoch_transition: false,
            },
        ));

        let mut restored = recorded.clone();
        assert!(!record_bank_sysvar_accounts(
            &mut restored,
            ordinary_child,
            BankSysvarWritePhase::Child {
                epoch_transition: false,
            },
        ));
        assert!(!record_bank_sysvar_accounts(
            &mut restored,
            [crate::SLOT_HISTORY_SYSVAR_ID],
            BankSysvarWritePhase::SlotHistory,
        ));
        assert!(record_bank_sysvar_accounts(
            &mut restored,
            [
                crate::REWARDS_SYSVAR_ID,
                crate::STAKE_HISTORY_SYSVAR_ID,
                crate::CLOCK_SYSVAR_ID,
                crate::FEES_SYSVAR_ID,
                crate::RECENT_BLOCKHASHES_SYSVAR_ID,
            ],
            BankSysvarWritePhase::Child {
                epoch_transition: true,
            },
        ));
        assert_eq!(
            restored,
            BTreeSet::from([
                crate::SLOT_HISTORY_SYSVAR_ID,
                crate::REWARDS_SYSVAR_ID,
                crate::STAKE_HISTORY_SYSVAR_ID,
                crate::CLOCK_SYSVAR_ID,
                crate::FEES_SYSVAR_ID,
                crate::RECENT_BLOCKHASHES_SYSVAR_ID,
            ])
        );
    }

    #[test]
    fn replay_bank_lifecycle_keeps_sysvar_writes_out_of_instruction_diffs() {
        let genesis = exact_genesis();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        replay.enable_bank_lifecycle();
        let empty_slot = |slot, parent_slot, blockhash, previous_blockhash| CompactSlotProbe {
            block_id: slot as u32,
            slot,
            parent_slot,
            block_time: None,
            block_height: None,
            blockhash_id: slot as u32,
            blockhash,
            previous_blockhash_id: parent_slot as u32,
            previous_blockhash,
            transaction_count: 0,
            transactions: Vec::new(),
        };

        replay
            .process_slot(
                &empty_slot(0, 0, [2; 32], genesis.genesis_hash),
                &mut |_| {},
            )
            .unwrap();
        replay
            .process_slot(&empty_slot(1, 0, [3; 32], [2; 32]), &mut |_| {})
            .unwrap();
        let outcome = replay.finish();

        assert_eq!(outcome.bank_sysvar_writes, 5);
        assert_eq!(
            outcome.bank_sysvar_accounts_written,
            BTreeSet::from([
                crate::SLOT_HISTORY_SYSVAR_ID,
                crate::CLOCK_SYSVAR_ID,
                crate::FEES_SYSVAR_ID,
                crate::RECENT_BLOCKHASHES_SYSVAR_ID,
            ])
        );
        assert!(outcome.slot_hashes_unavailable);
        assert!(
            !outcome
                .account_state
                .contains_key(&crate::SLOT_HASHES_SYSVAR_ID)
        );
        assert_eq!(outcome.account_state.len(), 12);
        assert!(outcome.changed_accounts.is_empty());
        assert!(outcome.instruction_mutations.is_empty());
    }

    #[test]
    fn config_instruction_mutates_state_and_emits_raw_data_diff() {
        let genesis = exact_genesis_with_config_account();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let data = vec![0, 0, 0, 0, 0, 0, 208, 63, 12];
        let mut emitted = Vec::new();

        replay
            .process_slot(
                &slot_with_transactions(vec![config_transaction(data.clone())]),
                &mut |mutation| emitted.push(mutation.clone()),
            )
            .unwrap();
        let outcome = replay.finish();

        assert_eq!(outcome.transactions_processed, 1);
        assert_eq!(outcome.config_mutations, 1);
        assert_eq!(outcome.changed_accounts, BTreeSet::from([CONFIG_ACCOUNT]));
        assert_eq!(&outcome.account_state[&CONFIG_ACCOUNT].data[..9], data);
        assert_eq!(outcome.account_state[&CONFIG_ACCOUNT].data[9], 0);
        assert_eq!(emitted.len(), 1);
        assert!(matches!(
            emitted[0].effect,
            LaunchInstructionEffect::Config(LaunchConfigMutation {
                config_account: CONFIG_ACCOUNT,
                data_len: 9,
                ..
            })
        ));
        assert_eq!(emitted[0].diff.disposition, DiffDisposition::Committed);
        assert_eq!(emitted[0].diff.accounts.len(), 1);
        assert!(emitted[0].diff.accounts[0].data.is_some());
    }

    #[test]
    fn authoritative_failed_transaction_is_skipped_without_allocating_an_overlay() {
        let genesis = exact_genesis_with_config_account();
        let original = genesis.accounts.last().unwrap().data.clone();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut transaction = config_transaction(vec![0, 0, 0, 0, 0, 0, 208, 63, 12]);
        transaction.archived_outcome = CompactArchivedTransactionOutcome::Failed;
        transaction.balance_oracle = Some(crate::CompactTransactionBalanceOracle {
            fee: 5_000,
            pre_balances: smallvec::smallvec![960_480, 1],
            post_balances: smallvec::smallvec![955_480, 1],
        });
        let mut emitted = Vec::new();

        replay
            .process_slot(
                &slot_with_transactions(vec![transaction]),
                &mut |mutation| emitted.push(mutation.clone()),
            )
            .unwrap();
        let outcome = replay.finish();

        assert_eq!(outcome.failed_transactions, 1);
        assert_eq!(outcome.transactions_processed, 0);
        assert_eq!(outcome.instructions_processed, 0);
        assert_eq!(outcome.config_mutations, 0);
        assert_eq!(outcome.account_state[&CONFIG_ACCOUNT].data, original);
        assert_eq!(outcome.account_state[&CONFIG_ACCOUNT].lamports, 955_480);
        assert_eq!(outcome.changed_accounts, BTreeSet::from([CONFIG_ACCOUNT]));
        assert!(outcome.first_failed_transaction.is_none());
        assert!(emitted.is_empty());
    }

    #[test]
    fn compact_post_balance_reconciliation_preserves_all_entry_cases() {
        const UPDATED: [u8; 32] = [80; 32];
        const REMOVED: [u8; 32] = [81; 32];
        const UNCHANGED: [u8; 32] = [82; 32];
        const ZERO_PRESENT: [u8; 32] = [83; 32];
        const INSERTED: [u8; 32] = [84; 32];
        const ABSENT_ZERO: [u8; 32] = [85; 32];
        const READONLY: [u8; 32] = [86; 32];

        let account = |lamports, marker| AccountSnapshot {
            lamports,
            owner: [marker; 32],
            executable: marker.is_multiple_of(2),
            rent_epoch: u64::from(marker),
            data: vec![marker; 5].into(),
        };
        let mut replay = LaunchReplay::from_genesis(0, Some(&exact_genesis()), false).unwrap();
        let updated_before = account(10, 1);
        let removed_before = account(50, 2);
        let unchanged_before = account(30, 3);
        let zero_before = account(0, 4);
        let readonly_before = account(70, 5);
        replay
            .outcome
            .account_state
            .insert(UPDATED, updated_before.clone());
        replay.outcome.account_state.insert(REMOVED, removed_before);
        replay
            .outcome
            .account_state
            .insert(UNCHANGED, unchanged_before.clone());
        replay
            .outcome
            .account_state
            .insert(ZERO_PRESENT, zero_before.clone());
        replay
            .outcome
            .account_state
            .insert(READONLY, readonly_before.clone());
        replay.outcome.changed_accounts.clear();
        let updated_data = replay.outcome.account_state[&UPDATED].data.as_ptr();

        let transaction = CompactTransactionProbe {
            tx_index: 9,
            row_flags: 1,
            archived_outcome: CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                fee: 5_000,
                pre_balances: smallvec::smallvec![10, 50, 30, 0, 0, 0, 70],
                post_balances: smallvec::smallvec![20, 0, 30, 0, 40, 0, 999],
            }),
            signature_count: 0,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 0,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec::smallvec![
                UPDATED,
                REMOVED,
                UNCHANGED,
                ZERO_PRESENT,
                INSERTED,
                ABSENT_ZERO,
                READONLY,
            ],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: SmallVec::new(),
        };
        let transaction_metas = transaction_account_meta_layout(123, &transaction).unwrap();

        replay
            .reconcile_compact_post_balances(123, &transaction, &transaction_metas)
            .unwrap();

        let updated = &replay.outcome.account_state[&UPDATED];
        assert_eq!(updated.lamports, 20);
        assert_eq!(updated.owner, updated_before.owner);
        assert_eq!(updated.executable, updated_before.executable);
        assert_eq!(updated.rent_epoch, updated_before.rent_epoch);
        assert_eq!(updated.data, updated_before.data);
        assert_eq!(updated.data.as_ptr(), updated_data);
        assert!(!replay.outcome.account_state.contains_key(&REMOVED));
        assert_eq!(replay.outcome.account_state[&UNCHANGED], unchanged_before);
        assert_eq!(replay.outcome.account_state[&ZERO_PRESENT], zero_before);
        assert!(!replay.outcome.account_state.contains_key(&ABSENT_ZERO));
        assert_eq!(replay.outcome.account_state[&READONLY], readonly_before);

        assert!(!replay.outcome.account_state.contains_key(&INSERTED));
        assert_eq!(
            replay.outcome.changed_accounts,
            BTreeSet::from([REMOVED, UPDATED])
        );
    }

    #[test]
    fn absent_system_transfer_uses_pre_balances_without_persisting_balance_accounts() {
        let transaction = transient_system_transaction(
            0,
            CompactArchivedTransactionOutcome::Succeeded,
            [100, 0, 1],
            [65, 30, 1],
            ArchiveV2SystemInstructionData::Transfer { lamports: 30 },
            smallvec::smallvec![0, 1],
        );
        let mut replay = LaunchReplay::from_genesis(0, Some(&exact_genesis()), false).unwrap();

        replay
            .process_slot(&slot_with_transactions(vec![transaction]), &mut |_| {})
            .unwrap();

        assert_eq!(replay.outcome.transactions_processed, 1);
        assert_eq!(replay.outcome.system_mutations, 1);
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_BASE));
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
        assert!(!replay.outcome.changed_accounts.contains(&SYSTEM_BASE));
        assert!(!replay.outcome.changed_accounts.contains(&SYSTEM_TARGET));
    }

    #[test]
    fn covered_readonly_account_is_seeded_transaction_locally() {
        const READONLY: [u8; 32] = [0xa7; 32];
        let mut transaction = transient_system_transaction(
            0,
            CompactArchivedTransactionOutcome::Succeeded,
            [100, 0, 77],
            [65, 30, 77],
            ArchiveV2SystemInstructionData::Transfer { lamports: 30 },
            smallvec::smallvec![0, 1],
        );
        transaction.account_keys[2] = READONLY;
        let transaction_metas = transaction_account_meta_layout(105_368, &transaction).unwrap();
        assert!(!transaction_metas.is_writable(2));
        let replay = LaunchReplay::from_genesis(0, Some(&exact_genesis()), false).unwrap();
        let mut overlay = CowAccountMap::layered(&replay.outcome.account_state);
        let mut absent = AbsentOverlayAccounts::new();

        seed_absent_covered_pre_balances(
            &transaction,
            &transaction_metas,
            &mut overlay,
            &mut absent,
        );

        assert_eq!(overlay[&READONLY].lamports, 77);
        assert!(is_balance_only_system_account(&overlay[&READONLY]));
        assert!(absent.contains(&READONLY));
        assert!(!absent.spilled());
    }

    #[test]
    fn absent_overlay_accounts_are_sorted_deduplicated_and_inline() {
        let mut absent = AbsentOverlayAccounts::new();

        assert!(absent.insert([3; 32]));
        assert!(absent.insert([1; 32]));
        assert!(absent.insert([2; 32]));
        assert!(!absent.insert([2; 32]));

        assert_eq!(absent.as_slice(), &[[1; 32], [2; 32], [3; 32]]);
        assert!(!absent.spilled());
        assert!(absent.contains(&[2; 32]));
        assert!(absent.remove(&[2; 32]));
        assert!(!absent.remove(&[2; 32]));
        assert_eq!(absent.as_slice(), &[[1; 32], [3; 32]]);
    }

    #[test]
    fn repeated_balance_only_appearances_reseed_from_each_canonical_pre_balance() {
        let first = transient_system_transaction(
            0,
            CompactArchivedTransactionOutcome::Succeeded,
            [100, 0, 1],
            [65, 30, 1],
            ArchiveV2SystemInstructionData::Transfer { lamports: 30 },
            smallvec::smallvec![0, 1],
        );
        let second = transient_system_transaction(
            1,
            CompactArchivedTransactionOutcome::Succeeded,
            [65, 30, 1],
            [40, 50, 1],
            ArchiveV2SystemInstructionData::Transfer { lamports: 20 },
            smallvec::smallvec![0, 1],
        );
        let mut replay = LaunchReplay::from_genesis(0, Some(&exact_genesis()), false).unwrap();

        replay
            .process_slot(&slot_with_transactions(vec![first, second]), &mut |_| {})
            .unwrap();

        assert_eq!(replay.outcome.transactions_processed, 2);
        assert_eq!(replay.outcome.system_mutations, 2);
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_BASE));
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
    }

    #[test]
    fn later_allocate_and_assign_promote_transient_balance_to_persistent_state() {
        const ASSIGNED_OWNER: [u8; 32] = [91; 32];
        let transfer = transient_system_transaction(
            0,
            CompactArchivedTransactionOutcome::Succeeded,
            [100, 0, 1],
            [65, 30, 1],
            ArchiveV2SystemInstructionData::Transfer { lamports: 30 },
            smallvec::smallvec![0, 1],
        );
        let mut allocate = transient_system_transaction(
            1,
            CompactArchivedTransactionOutcome::Unknown,
            [30, 0, 1],
            [25, 0, 1],
            ArchiveV2SystemInstructionData::Allocate { space: 8 },
            smallvec::smallvec![0],
        );
        allocate.account_keys.swap(0, 1);
        allocate.instructions[0].account_indexes = smallvec::smallvec![0];
        allocate.instructions.push(CompactInstructionProbe {
            instruction_index: 1,
            program_id_index: 2,
            program_id: SYSTEM_PROGRAM_ID,
            account_indexes: smallvec::smallvec![0],
            data: CompactInstructionData::System(ArchiveV2SystemInstructionData::Assign {
                owner: ASSIGNED_OWNER,
            }),
        });
        let mut replay = LaunchReplay::from_genesis(0, Some(&exact_genesis()), false).unwrap();

        replay
            .process_slot(
                &slot_with_transactions(vec![transfer, allocate]),
                &mut |_| {},
            )
            .unwrap();

        let account = &replay.outcome.account_state[&SYSTEM_TARGET];
        assert_eq!(account.lamports, 25);
        assert_eq!(account.owner, ASSIGNED_OWNER);
        assert_eq!(account.data, vec![0; 8]);
        assert!(replay.outcome.changed_accounts.contains(&SYSTEM_TARGET));
    }

    #[test]
    fn archived_failed_transaction_does_not_hydrate_balance_only_accounts() {
        let transaction = transient_system_transaction(
            0,
            CompactArchivedTransactionOutcome::Failed,
            [100, 0, 1],
            [95, 0, 1],
            ArchiveV2SystemInstructionData::Transfer { lamports: 30 },
            smallvec::smallvec![0, 1],
        );
        let mut replay = LaunchReplay::from_genesis(0, Some(&exact_genesis()), false).unwrap();

        replay
            .process_slot(&slot_with_transactions(vec![transaction]), &mut |_| {})
            .unwrap();

        assert_eq!(replay.outcome.failed_transactions, 1);
        assert_eq!(replay.outcome.transactions_processed, 0);
        assert_eq!(replay.outcome.instructions_processed, 0);
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_BASE));
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
        assert!(replay.outcome.changed_accounts.is_empty());
    }

    #[test]
    fn post_activation_absent_writable_without_pre_balance_fails_before_execution() {
        let mut transaction = transient_system_transaction(
            0,
            CompactArchivedTransactionOutcome::Unknown,
            [100, 0, 1],
            [65, 30, 1],
            ArchiveV2SystemInstructionData::Transfer { lamports: 30 },
            smallvec::smallvec![0, 1],
        );
        transaction.balance_oracle = None;
        let mut slot = slot_with_transactions(vec![transaction]);
        slot.slot = FIRST_AUTHORITATIVE_OUTCOME_SLOT;
        slot.parent_slot = slot.slot - 1;
        let mut replay = LaunchReplay::from_genesis(9, Some(&exact_genesis()), false).unwrap();

        assert!(matches!(
            replay.process_slot(&slot, &mut |_| {}).unwrap_err(),
            LaunchReplayError::InvalidCompactBalanceProjection {
                slot: FIRST_AUTHORITATIVE_OUTCOME_SLOT,
                transaction_index: 0,
                message: "absent writable account has no canonical Compact pre-balance",
            }
        ));
        assert_eq!(replay.outcome.transactions_processed, 0);
        assert_eq!(replay.outcome.instructions_processed, 0);
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_BASE));
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
    }

    #[test]
    fn compact_post_balances_may_omit_a_readonly_unsigned_suffix() {
        const PAYER: [u8; 32] = [87; 32];
        const VOTE_ACCOUNT: [u8; 32] = [88; 32];
        const SLOT_HASHES: [u8; 32] = [89; 32];
        const CLOCK: [u8; 32] = [90; 32];

        let mut replay = LaunchReplay::from_genesis(0, Some(&exact_genesis()), false).unwrap();
        replay.outcome.account_state.insert(
            PAYER,
            AccountSnapshot {
                lamports: 66_747_417_856_252,
                ..default_system_account()
            },
        );
        replay.outcome.account_state.insert(
            VOTE_ACCOUNT,
            AccountSnapshot {
                lamports: 8_539_925_000,
                owner: VOTE_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
                data: vec![0; 3_731].into(),
            },
        );
        let readonly_before = replay.outcome.account_state[&VOTE_PROGRAM_ID].clone();
        replay.outcome.changed_accounts.clear();

        let transaction = CompactTransactionProbe {
            tx_index: 71,
            row_flags: 1,
            archived_outcome: CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                fee: 5_000,
                pre_balances: smallvec::smallvec![66_747_417_856_252, 8_539_925_000, 1],
                post_balances: smallvec::smallvec![66_746_557_851_252, 9_399_925_000, 1],
            }),
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 3,
            },
            account_keys: smallvec::smallvec![
                PAYER,
                VOTE_ACCOUNT,
                SLOT_HASHES,
                CLOCK,
                VOTE_PROGRAM_ID,
            ],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: SmallVec::new(),
        };
        let transaction_metas = transaction_account_meta_layout(24_005_334, &transaction).unwrap();

        replay
            .reconcile_compact_post_balances(24_005_334, &transaction, &transaction_metas)
            .unwrap();

        assert!(!replay.outcome.account_state.contains_key(&PAYER));
        assert_eq!(
            replay.outcome.account_state[&VOTE_ACCOUNT].lamports,
            9_399_925_000
        );
        assert_eq!(
            replay.outcome.account_state[&VOTE_PROGRAM_ID],
            readonly_before
        );
        assert_eq!(
            replay.outcome.changed_accounts,
            BTreeSet::from([VOTE_ACCOUNT])
        );
    }

    #[test]
    fn archived_fee_only_system_transfer_projects_post_balances_without_a_diff() {
        let mut genesis = exact_genesis_with_system_base();
        genesis
            .accounts
            .iter_mut()
            .find(|account| account.pubkey == SYSTEM_BASE)
            .unwrap()
            .lamports = 1_554_742;
        genesis.accounts.push(CompactGenesisAccount {
            pubkey: SYSTEM_TARGET,
            lamports: 2_904_649_619,
            owner: SYSTEM_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: Vec::new(),
        });
        let transaction = |destination_post| CompactTransactionProbe {
            tx_index: 27,
            row_flags: 1,
            archived_outcome: CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                fee: 5_000,
                pre_balances: smallvec::smallvec![1_179_735, 2_889_069_619, 1],
                post_balances: smallvec::smallvec![1_174_735, destination_post, 1],
            }),
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec::smallvec![SYSTEM_BASE, SYSTEM_TARGET, SYSTEM_PROGRAM_ID],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![CompactInstructionProbe {
                instruction_index: 0,
                program_id_index: 2,
                program_id: SYSTEM_PROGRAM_ID,
                account_indexes: smallvec::smallvec![0, 1],
                data: CompactInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                    lamports: 88_555_280,
                },),
            }],
        };
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut emitted = Vec::new();

        replay
            .process_slot(
                &slot_with_transactions(vec![transaction(2_889_069_619)]),
                &mut |mutation| emitted.push(mutation.clone()),
            )
            .unwrap();

        assert_eq!(replay.outcome.transactions_processed, 1);
        assert_eq!(replay.outcome.instructions_processed, 1);
        assert_eq!(replay.outcome.system_mutations, 1);
        assert_eq!(replay.outcome.failed_transactions, 0);
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_BASE));
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
        assert!(emitted.is_empty());

        let mut rejected = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        assert!(matches!(
            rejected.process_slot(
                &slot_with_transactions(vec![transaction(2_977_624_899)]),
                &mut |_| {},
            ),
            Err(LaunchReplayError::SystemMutation {
                source: LaunchSystemError::ResultWithNegativeLamports { .. },
                ..
            })
        ));
    }

    #[test]
    fn archived_system_transfer_recovers_a_stale_reward_funded_source_balance() {
        let mut genesis = exact_genesis_with_system_base();
        genesis
            .accounts
            .iter_mut()
            .find(|account| account.pubkey == SYSTEM_BASE)
            .unwrap()
            .lamports = 100_000_000;
        let transaction = |destination_post| CompactTransactionProbe {
            tx_index: 52,
            row_flags: 1,
            archived_outcome: CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                fee: 5_000,
                pre_balances: smallvec::smallvec![14_265_540_403_028_132, 0, 1],
                post_balances: smallvec::smallvec![14_265_539_863_023_132, destination_post, 1],
            }),
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec::smallvec![SYSTEM_BASE, SYSTEM_TARGET, SYSTEM_PROGRAM_ID],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![CompactInstructionProbe {
                instruction_index: 0,
                program_id_index: 2,
                program_id: SYSTEM_PROGRAM_ID,
                account_indexes: smallvec::smallvec![0, 1],
                data: CompactInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                    lamports: 540_000_000,
                }),
            }],
        };
        let mut slot = slot_with_transactions(vec![transaction(540_000_000)]);
        slot.slot = 24_023_388;
        slot.parent_slot = slot.slot - 1;
        let mut replay = LaunchReplay::from_genesis(55, Some(&genesis), false).unwrap();
        let mut emitted = Vec::new();

        replay
            .process_slot(&slot, &mut |mutation| emitted.push(mutation.clone()))
            .unwrap();

        assert_eq!(replay.outcome.transactions_processed, 1);
        assert_eq!(replay.outcome.instructions_processed, 1);
        assert_eq!(replay.outcome.system_mutations, 1);
        assert_eq!(replay.outcome.failed_transactions, 0);
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_BASE));
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
        assert!(emitted.is_empty());

        let mut rejected = LaunchReplay::from_genesis(55, Some(&genesis), false).unwrap();
        let mut rejected_slot = slot_with_transactions(vec![transaction(539_999_999)]);
        rejected_slot.slot = 24_023_388;
        rejected_slot.parent_slot = rejected_slot.slot - 1;
        assert!(matches!(
            rejected.process_slot(&rejected_slot, &mut |_| {}),
            Err(LaunchReplayError::SystemMutation {
                source: LaunchSystemError::ResultWithNegativeLamports { .. },
                ..
            })
        ));
    }

    #[test]
    fn archived_fee_only_prefunded_create_projects_fee_without_state_diff() {
        let owner = [57; 32];
        let destination_before = AccountSnapshot {
            lamports: 28_926,
            owner,
            executable: false,
            rent_epoch: 0,
            data: vec![0; 125].into(),
        };
        let mut genesis = exact_genesis_with_system_base();
        genesis
            .accounts
            .iter_mut()
            .find(|account| account.pubkey == SYSTEM_BASE)
            .unwrap()
            .lamports = 8_694_383_170;
        genesis.accounts.push(CompactGenesisAccount {
            pubkey: SYSTEM_TARGET,
            lamports: destination_before.lamports,
            owner: destination_before.owner,
            executable: destination_before.executable,
            rent_epoch: destination_before.rent_epoch,
            data: destination_before.data.to_vec(),
        });
        let transaction = |destination_post| CompactTransactionProbe {
            tx_index: 150,
            row_flags: 1,
            archived_outcome: CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                fee: 10_000,
                pre_balances: smallvec::smallvec![8_694_383_170, 28_926, 1],
                post_balances: smallvec::smallvec![8_694_373_170, destination_post, 1],
            }),
            signature_count: 2,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec::smallvec![SYSTEM_BASE, SYSTEM_TARGET, SYSTEM_PROGRAM_ID],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![CompactInstructionProbe {
                instruction_index: 0,
                program_id_index: 2,
                program_id: SYSTEM_PROGRAM_ID,
                account_indexes: smallvec::smallvec![0, 1],
                data: CompactInstructionData::System(
                    ArchiveV2SystemInstructionData::CreateAccount {
                        lamports: 33_747,
                        space: 125,
                        owner,
                    },
                ),
            }],
        };
        let mut slot = slot_with_transactions(vec![transaction(28_926)]);
        slot.slot = 18_916_586;
        slot.parent_slot = slot.slot - 1;
        let mut replay = LaunchReplay::from_genesis(43, Some(&genesis), false).unwrap();
        let mut emitted = Vec::new();

        replay
            .process_slot(&slot, &mut |mutation| emitted.push(mutation.clone()))
            .unwrap();

        assert_eq!(replay.outcome.epoch, 43);
        assert_eq!(replay.outcome.transactions_processed, 1);
        assert_eq!(replay.outcome.instructions_processed, 1);
        assert_eq!(replay.outcome.system_mutations, 1);
        assert_eq!(replay.outcome.failed_transactions, 0);
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_BASE));
        assert_eq!(
            replay.outcome.account_state[&SYSTEM_TARGET],
            destination_before
        );
        assert!(emitted.is_empty());

        let mut rejected = LaunchReplay::from_genesis(43, Some(&genesis), false).unwrap();
        let mut near_miss_slot = slot_with_transactions(vec![transaction(62_673)]);
        near_miss_slot.slot = 18_916_586;
        near_miss_slot.parent_slot = near_miss_slot.slot - 1;
        assert!(matches!(
            rejected.process_slot(&near_miss_slot, &mut |_| {}),
            Err(LaunchReplayError::SystemMutation {
                source: LaunchSystemError::AccountAlreadyInUse { .. },
                ..
            })
        ));
    }

    #[test]
    fn epoch_40_new_system_processor_commits_positive_self_transfer() {
        let mut genesis = exact_genesis_with_system_base();
        genesis
            .accounts
            .iter_mut()
            .find(|account| account.pubkey == SYSTEM_BASE)
            .unwrap()
            .lamports = 1_003_770_000;
        let transaction = CompactTransactionProbe {
            tx_index: 6,
            row_flags: 1,
            archived_outcome: CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                fee: 5_000,
                pre_balances: smallvec::smallvec![1_003_770_000, 1],
                post_balances: smallvec::smallvec![1_003_765_000, 1],
            }),
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec::smallvec![SYSTEM_BASE, SYSTEM_PROGRAM_ID],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![CompactInstructionProbe {
                instruction_index: 0,
                program_id_index: 1,
                program_id: SYSTEM_PROGRAM_ID,
                account_indexes: smallvec::smallvec![0, 0],
                data: CompactInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                    lamports: 1_000_000,
                }),
            }],
        };
        let mut slot = slot_with_transactions(vec![transaction]);
        slot.slot = 17_305_974;
        slot.parent_slot = slot.slot - 1;
        let mut replay = LaunchReplay::from_genesis(40, Some(&genesis), false).unwrap();

        replay.process_slot(&slot, &mut |_| {}).unwrap();

        assert_eq!(replay.outcome.epoch, 40);
        assert_eq!(replay.outcome.transactions_processed, 1);
        assert_eq!(replay.outcome.instructions_processed, 1);
        assert_eq!(replay.outcome.system_mutations, 1);
        assert_eq!(replay.outcome.failed_transactions, 0);
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_BASE));
    }

    #[test]
    fn epoch_44_vote_update_commission_mutates_data_and_projects_fee() {
        const WITHDRAW_AUTHORITY: [u8; 32] = [9; 32];
        let mut genesis = exact_genesis();
        let initial_vote_data = initialized_vote_account_data();
        genesis.accounts[0].lamports = 26_858_640;
        genesis.accounts[0].data = initial_vote_data.clone();
        genesis.accounts.push(CompactGenesisAccount {
            pubkey: WITHDRAW_AUTHORITY,
            lamports: 416_523_696_520,
            owner: SYSTEM_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: Vec::new(),
        });
        let instruction_data =
            wincode::serialize(&FixtureVoteInstruction::UpdateCommission(10)).unwrap();
        assert_eq!(instruction_data, [5, 0, 0, 0, 10]);
        let transaction = CompactTransactionProbe {
            tx_index: 99,
            row_flags: 1,
            archived_outcome: CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                fee: 5_000,
                pre_balances: smallvec::smallvec![416_523_696_520, 26_858_640, 1],
                post_balances: smallvec::smallvec![416_523_691_520, 26_858_640, 1],
            }),
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec::smallvec![WITHDRAW_AUTHORITY, VOTE_ACCOUNT, VOTE_PROGRAM],
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: smallvec::smallvec![CompactInstructionProbe {
                instruction_index: 0,
                program_id_index: 2,
                program_id: VOTE_PROGRAM,
                account_indexes: smallvec::smallvec![1, 0],
                data: CompactInstructionData::Raw(instruction_data.into()),
            }],
        };
        let mut slot = slot_with_transactions(vec![transaction]);
        slot.slot = 19_392_740;
        slot.parent_slot = slot.slot - 1;
        let mut replay = LaunchReplay::from_genesis(44, Some(&genesis), false).unwrap();
        let mut emitted = Vec::new();

        replay
            .process_slot(&slot, &mut |mutation| emitted.push(mutation.clone()))
            .unwrap();

        assert_eq!(replay.outcome.epoch, 44);
        assert_eq!(replay.outcome.transactions_processed, 1);
        assert_eq!(replay.outcome.instructions_processed, 1);
        assert_eq!(replay.outcome.vote_mutations, 1);
        assert_eq!(replay.outcome.failed_transactions, 0);
        assert!(
            !replay
                .outcome
                .account_state
                .contains_key(&WITHDRAW_AUTHORITY)
        );
        assert_eq!(
            replay.outcome.account_state[&VOTE_ACCOUNT].lamports,
            26_858_640
        );
        assert_eq!(
            replay.outcome.account_state[&VOTE_ACCOUNT].data.len(),
            initial_vote_data.len()
        );
        assert_ne!(
            replay.outcome.account_state[&VOTE_ACCOUNT].data,
            initial_vote_data
        );
        assert!(matches!(
            emitted.as_slice(),
            [LaunchInstructionMutation {
                effect: LaunchInstructionEffect::Vote {
                    vote_account: VOTE_ACCOUNT,
                    mutation: crate::LaunchVoteMutation::UpdateCommission {
                        old_commission: 100,
                        new_commission: 10,
                    },
                },
                ..
            }]
        ));
    }

    #[test]
    fn authoritative_success_that_runtime_rejects_is_a_hard_replay_gap() {
        let mut genesis = exact_genesis_with_system_base();
        genesis.accounts.push(CompactGenesisAccount {
            pubkey: CONFIG_ACCOUNT,
            lamports: 960_480,
            owner: CONFIG_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: vec![0; 10],
        });
        let original_config = genesis.accounts.last().unwrap().data.clone();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut transaction = system_then_unsigned_config_transaction();
        transaction.archived_outcome = CompactArchivedTransactionOutcome::Succeeded;

        let error = replay
            .process_slot(&slot_with_transactions(vec![transaction]), &mut |_| {})
            .unwrap_err();

        assert!(matches!(error, LaunchReplayError::ConfigMutation { .. }));
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
        assert_eq!(
            replay.outcome.account_state[&CONFIG_ACCOUNT].data,
            original_config
        );
        assert_eq!(replay.outcome.failed_transactions, 0);
        assert_eq!(replay.outcome.transactions_processed, 0);
        assert_eq!(
            replay
                .take_rolled_back_transaction()
                .unwrap()
                .instruction_mutations
                .len(),
            1
        );
    }

    #[test]
    fn failed_config_rolls_back_preceding_system_mutation() {
        let mut genesis = exact_genesis_with_system_base();
        genesis.accounts.push(CompactGenesisAccount {
            pubkey: CONFIG_ACCOUNT,
            lamports: 960_480,
            owner: CONFIG_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: vec![0; 10],
        });
        let original_config = genesis.accounts.last().unwrap().data.clone();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut emitted = Vec::new();

        replay
            .process_slot(
                &slot_with_transactions(vec![system_then_unsigned_config_transaction()]),
                &mut |mutation| emitted.push(mutation.clone()),
            )
            .unwrap();
        let outcome = replay.finish();

        assert_eq!(outcome.failed_transactions, 1);
        assert_eq!(outcome.transactions_processed, 0);
        assert_eq!(outcome.instructions_processed, 0);
        assert_eq!(outcome.rolled_back_instructions, 1);
        assert_eq!(outcome.config_mutations, 0);
        assert_eq!(outcome.system_mutations, 0);
        assert!(!outcome.account_state.contains_key(&SYSTEM_TARGET));
        assert_eq!(outcome.account_state[&CONFIG_ACCOUNT].data, original_config);
        assert_eq!(emitted.len(), 1);
        assert!(matches!(
            emitted[0].effect,
            LaunchInstructionEffect::System(LaunchSystemMutation::Allocate { .. })
        ));
        assert_eq!(emitted[0].diff.disposition, DiffDisposition::RolledBack);
        assert!(matches!(
            outcome.first_failed_transaction,
            Some(LaunchDerivedTransactionFailure {
                reason: LaunchTransactionFailureReason::Config(
                    LaunchConfigError::MissingRequiredSignature {
                        pubkey: CONFIG_ACCOUNT,
                    },
                ),
                ..
            })
        ));
    }

    #[test]
    fn unresolved_instruction_accounts_fail_before_vote_decode() {
        let genesis = exact_genesis();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), true).unwrap();
        let error = replay
            .process_slot(&slot_with_instruction(0, vec![0, 2]), &mut |_| {})
            .unwrap_err();
        assert!(matches!(
            error,
            LaunchReplayError::UnresolvedInstructionAccount {
                account_position: 1,
                ..
            }
        ));
    }

    #[test]
    fn oversized_instruction_path_index_fails_instead_of_truncating() {
        let genesis = exact_genesis();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), true).unwrap();
        let instruction_index = u32::from(u16::MAX) + 1;
        let error = replay
            .process_slot(
                &slot_with_instruction(instruction_index, vec![0]),
                &mut |_| {},
            )
            .unwrap_err();
        assert!(matches!(
            error,
            LaunchReplayError::InstructionPathIndexOverflow {
                instruction_index: value,
                ..
            } if value == instruction_index
        ));
    }

    #[test]
    fn streaming_initialization_clones_generation_binding() {
        let context = CompactGenerationContext {
            root: PathBuf::from("fixture"),
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 0,
            generation_id: "generation".to_owned(),
            slots_per_epoch: 432_000,
            block_count: 1,
            complete: true,
            first_slot: Some(0),
            last_slot: Some(0),
            binding: GenerationBinding {
                generation_digest: [4; 32],
                registry_sha256: [5; 32],
            },
            genesis: Some(exact_genesis()),
        };
        let mut replay = None;
        let mut cloned_context = None;
        initialize_streaming_replay(&mut replay, &mut cloned_context, &context).unwrap();
        let cloned_context = cloned_context.unwrap();
        assert_eq!(cloned_context.generation_id, context.generation_id);
        assert_eq!(cloned_context.binding, context.binding);
        assert!(replay.is_some());
    }

    #[test]
    fn continuation_context_accepts_same_chain_and_rejects_identity_changes() {
        let first = CompactGenerationContext {
            root: PathBuf::from("first"),
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 0,
            generation_id: "generation-0".to_owned(),
            slots_per_epoch: 432_000,
            block_count: 1,
            complete: true,
            first_slot: Some(0),
            last_slot: Some(0),
            binding: GenerationBinding {
                generation_digest: [4; 32],
                registry_sha256: [5; 32],
            },
            genesis: Some(exact_genesis()),
        };
        let mut next = first.clone();
        next.root = PathBuf::from("next");
        next.generation_id = "generation-1".to_owned();
        next.binding.generation_digest = [6; 32];
        let identity = LaunchChainIdentity {
            cluster_id: first.cluster_id.clone(),
            slots_per_epoch: first.slots_per_epoch,
            genesis_hash: first.genesis.as_ref().unwrap().genesis_hash,
        };
        validate_continuation_context(&identity, &first, &next).unwrap();

        next.cluster_id = "wrong-cluster".to_owned();
        assert!(matches!(
            validate_continuation_context(&identity, &first, &next),
            Err(LaunchReplayError::IncompatibleGeneration { .. })
        ));

        let mut epoch_one = first.clone();
        epoch_one.epoch = 1;
        epoch_one.generation_id = "epoch-1".to_owned();
        epoch_one.binding.generation_digest = [7; 32];
        epoch_one.genesis = None;
        validate_continuation_context(&identity, &first, &epoch_one).unwrap();
    }

    #[test]
    fn generation_transition_requires_a_sealed_exhausted_cursor() {
        let first = CompactGenerationContext {
            root: PathBuf::from("first"),
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 0,
            generation_id: "generation-0".to_owned(),
            slots_per_epoch: 432_000,
            block_count: 1,
            complete: true,
            first_slot: Some(0),
            last_slot: Some(0),
            binding: GenerationBinding {
                generation_digest: [4; 32],
                registry_sha256: [5; 32],
            },
            genesis: Some(exact_genesis()),
        };
        let mut next = first.clone();
        next.generation_id = "generation-1".to_owned();
        next.binding.generation_digest = [6; 32];

        let mut replay = LaunchReplay::from_genesis(0, first.genesis.as_ref(), false).unwrap();
        replay.enable_bank_lifecycle();
        assert!(validate_completed_generation_transition(Some(&replay), &first, &next).is_err());

        let slot = CompactSlotProbe {
            block_id: 99,
            slot: 0,
            parent_slot: 0,
            block_time: None,
            block_height: None,
            blockhash_id: 1,
            blockhash: [2; 32],
            previous_blockhash_id: 0,
            previous_blockhash: first.genesis.as_ref().unwrap().genesis_hash,
            transaction_count: 0,
            transactions: Vec::new(),
        };
        replay
            .process_compact_row(&first, 0, None, &slot, &mut |_| {})
            .unwrap();
        validate_completed_generation_transition(Some(&replay), &first, &next).unwrap();

        let mut incomplete = first.clone();
        incomplete.complete = false;
        assert!(
            validate_completed_generation_transition(Some(&replay), &incomplete, &next).is_err()
        );
    }

    #[test]
    fn compact_rows_must_follow_the_recorded_physical_cursor() {
        let genesis = exact_genesis();
        let context = CompactGenerationContext {
            root: PathBuf::from("ordered"),
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 0,
            generation_id: "ordered-generation".to_owned(),
            slots_per_epoch: 432_000,
            block_count: 3,
            complete: true,
            first_slot: Some(0),
            last_slot: Some(2),
            binding: GenerationBinding {
                generation_digest: [8; 32],
                registry_sha256: [9; 32],
            },
            genesis: Some(genesis.clone()),
        };
        let slot_0 = CompactSlotProbe {
            block_id: 77,
            slot: 0,
            parent_slot: 0,
            block_time: None,
            block_height: None,
            blockhash_id: 1,
            blockhash: [2; 32],
            previous_blockhash_id: 0,
            previous_blockhash: genesis.genesis_hash,
            transaction_count: 0,
            transactions: Vec::new(),
        };
        let slot_1 = CompactSlotProbe {
            block_id: 88,
            slot: 1,
            parent_slot: 0,
            block_time: None,
            block_height: None,
            blockhash_id: 2,
            blockhash: [3; 32],
            previous_blockhash_id: 1,
            previous_blockhash: [2; 32],
            transaction_count: 0,
            transactions: Vec::new(),
        };
        let slot_2 = CompactSlotProbe {
            block_id: 99,
            slot: 2,
            parent_slot: 1,
            block_time: None,
            block_height: None,
            blockhash_id: 3,
            blockhash: [4; 32],
            previous_blockhash_id: 2,
            previous_blockhash: [3; 32],
            transaction_count: 0,
            transactions: Vec::new(),
        };
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        replay.enable_bank_lifecycle();
        replay
            .process_compact_row(&context, 0, Some(1), &slot_0, &mut |_| {})
            .unwrap();

        assert!(matches!(
            replay.process_compact_row(&context, 0, Some(1), &slot_0, &mut |_| {}),
            Err(LaunchReplayError::CompactRowOrderMismatch {
                expected: 1,
                found: 0
            })
        ));
        assert!(matches!(
            replay.process_compact_row(&context, 2, None, &slot_2, &mut |_| {}),
            Err(LaunchReplayError::CompactRowOrderMismatch {
                expected: 1,
                found: 2
            })
        ));
        assert_eq!(replay.outcome.last_slot, Some(0));

        replay
            .process_compact_row(&context, 1, Some(2), &slot_1, &mut |_| {})
            .unwrap();
        assert_eq!(replay.outcome.last_slot, Some(1));
    }

    #[test]
    fn compact_visit_summaries_merge_with_checked_program_counts() {
        let mut aggregate = empty_compact_visit_summary();
        merge_compact_visit_summary(
            &mut aggregate,
            CompactVisitSummary {
                slots_visited: 2,
                transactions_visited: 3,
                instructions_visited: 4,
                compressed_bytes_visited: 11,
                stopped_early: false,
                program_instruction_counts: BTreeMap::from([(VOTE_PROGRAM_ID, 4)]),
            },
        )
        .unwrap();
        merge_compact_visit_summary(
            &mut aggregate,
            CompactVisitSummary {
                slots_visited: 1,
                transactions_visited: 2,
                instructions_visited: 3,
                compressed_bytes_visited: 13,
                stopped_early: true,
                program_instruction_counts: BTreeMap::from([(VOTE_PROGRAM_ID, 3)]),
            },
        )
        .unwrap();
        assert_eq!(aggregate.slots_visited, 3);
        assert_eq!(aggregate.transactions_visited, 5);
        assert_eq!(aggregate.instructions_visited, 7);
        assert_eq!(aggregate.compressed_bytes_visited, 24);
        assert_eq!(aggregate.program_instruction_counts[&VOTE_PROGRAM_ID], 7);
        assert!(aggregate.stopped_early);
    }

    #[test]
    fn launch_clock_matches_mainnet_genesis_timing() {
        let mut genesis = exact_genesis();
        genesis.creation_time_unix = 1_584_368_940;
        let replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        assert_eq!(
            replay.clock_for_slot(105_368),
            LaunchClock {
                slot: 105_368,
                epoch: 0,
                unix_timestamp: 1_584_411_087,
            }
        );
        assert_eq!(replay.clock_for_slot(106_440).unix_timestamp, 1_584_411_516);
        assert_eq!(replay.clock_for_slot(108_931).unix_timestamp, 1_584_412_512);
    }

    #[test]
    fn diagnostic_failure_location_identifies_the_exact_instruction() {
        let error = LaunchReplayError::UnsupportedProgram {
            slot: 42,
            transaction_index: 3,
            instruction_index: 7,
            program_id: [6; 32],
        };
        let failure = LaunchReplayFailure::at_slot(42, error, None);

        assert_eq!(
            failure.location,
            LaunchReplayFailureLocation {
                slot: 42,
                transaction_index: Some(3),
                instruction_index: Some(7),
            }
        );
        assert!(matches!(
            failure.error,
            LaunchReplayError::UnsupportedProgram { .. }
        ));
    }

    #[test]
    fn zero_lamport_allocate_commits_its_diff_then_is_purged() {
        let genesis = exact_genesis_with_system_base();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut emitted = Vec::new();
        replay
            .process_slot(
                &slot_with_transactions(vec![allocate_with_seed_transaction(false)]),
                &mut |mutation| emitted.push(mutation.clone()),
            )
            .unwrap();
        let outcome = replay.finish();

        assert_eq!(outcome.slots_processed, 1);
        assert_eq!(outcome.transactions_processed, 1);
        assert_eq!(outcome.instructions_processed, 1);
        assert_eq!(outcome.vote_mutations, 0);
        assert_eq!(outcome.system_mutations, 1);
        assert_eq!(outcome.changed_accounts, BTreeSet::from([SYSTEM_TARGET]));
        assert!(!outcome.account_state.contains_key(&SYSTEM_TARGET));
        assert_eq!(emitted.len(), 1);
        assert!(matches!(
            emitted[0].effect,
            LaunchInstructionEffect::System(LaunchSystemMutation::Allocate {
                account: SYSTEM_TARGET,
                space: 200,
                owner: STAKE_PROGRAM,
                seeded: true,
            })
        ));
        assert_eq!(emitted[0].diff.disposition, DiffDisposition::Committed);
        assert!(emitted[0].diff.accounts[0].created);
    }

    #[test]
    fn bounded_diff_capture_preserves_execution_and_emits_only_the_requested_prefix() {
        let mut genesis = exact_genesis();
        genesis.accounts[0].data = initialized_vote_account_data();
        let slot = slot_with_transactions(vec![
            fixture_transaction(
                0,
                vec![fixture_instruction(
                    0,
                    VOTE_PROGRAM,
                    vote_instruction_data(vec![1]),
                )],
            ),
            fixture_transaction(
                1,
                vec![fixture_instruction(
                    0,
                    VOTE_PROGRAM,
                    vote_instruction_data(vec![2]),
                )],
            ),
        ]);

        let mut all = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut all_emitted = Vec::new();
        all.process_slot(&slot, &mut |mutation| {
            all_emitted.push(mutation.clone());
        })
        .unwrap();
        let all = all.finish();

        let mut one = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut one_capture =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::First(1));
        let mut one_emitted = Vec::new();
        one.process_slot_inner(&slot, &mut one_capture, &mut |mutation| {
            one_emitted.push(mutation.clone());
        })
        .unwrap();
        let one = one.finish();

        let mut zero = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut zero_capture =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::First(0));
        zero.process_slot_inner(&slot, &mut zero_capture, &mut |_| {
            panic!("a zero diff budget must not invoke the mutation visitor");
        })
        .unwrap();
        let zero = zero.finish();

        assert_eq!(all_emitted.len(), 2);
        assert_eq!(one_emitted.len(), 1);
        assert_eq!(one_emitted[0].slot, all_emitted[0].slot);
        assert_eq!(
            one_emitted[0].transaction_index,
            all_emitted[0].transaction_index
        );
        assert_eq!(one_emitted[0].diff, all_emitted[0].diff);
        assert_eq!(one.transactions_processed, all.transactions_processed);
        assert_eq!(one.instructions_processed, all.instructions_processed);
        assert_eq!(one.vote_mutations, all.vote_mutations);
        assert_eq!(one.account_state, all.account_state);
        assert_eq!(zero.transactions_processed, all.transactions_processed);
        assert_eq!(zero.instructions_processed, all.instructions_processed);
        assert_eq!(zero.vote_mutations, all.vote_mutations);
        assert_eq!(zero.changed_accounts, all.changed_accounts);
        assert_eq!(zero.account_state, all.account_state);
    }

    #[test]
    fn allocation_minimal_vote_path_matches_generic_repeated_sequence_across_epoch_boundary() {
        let mut genesis = exact_genesis();
        genesis.accounts[0].data = initialized_vote_account_data();
        let mut first = slot_with_transactions(
            (0_u32..24)
                .map(|tx_index| {
                    let mut data = if tx_index == 0 {
                        vote_instruction_data_with_timestamp(
                            vec![u64::from(tx_index) + 1],
                            Some(1_584_000_000),
                        )
                    } else {
                        vote_instruction_data(vec![u64::from(tx_index) + 1])
                    };
                    if tx_index == 1 {
                        data.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
                    }
                    fixture_transaction(tx_index, vec![fixture_instruction(0, VOTE_PROGRAM, data)])
                })
                .collect(),
        );
        first.slot = 431_999;
        first.parent_slot = 431_998;
        let mut second = slot_with_transactions(
            (0_u32..24)
                .map(|tx_index| {
                    fixture_transaction(
                        tx_index,
                        vec![fixture_instruction(
                            0,
                            VOTE_PROGRAM,
                            vote_instruction_data(vec![432_000 + u64::from(tx_index)]),
                        )],
                    )
                })
                .collect(),
        );
        second.slot = 432_000;
        second.parent_slot = 431_999;

        let mut generic = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut generic_capture =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::First(0));
        generic
            .process_slot_inner(&first, &mut generic_capture, &mut |_| {})
            .unwrap();
        generic
            .process_slot_inner(&second, &mut generic_capture, &mut |_| {})
            .unwrap();
        assert_eq!(generic.vote_state_cache.fast_path_commits(), 0);

        let mut fast = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        fast.process_slot_inner(&first, &mut no_diffs, &mut |_| {
            panic!("Capture::None must not allocate or emit a Vote effect")
        })
        .unwrap();
        fast.process_slot_inner(&second, &mut no_diffs, &mut |_| {
            panic!("Capture::None must not allocate or emit a Vote effect")
        })
        .unwrap();
        assert_eq!(fast.vote_state_cache.fast_path_commits(), 48);

        assert_eq!(fast.outcome.epoch, generic.outcome.epoch);
        assert_eq!(
            fast.outcome.transactions_processed,
            generic.outcome.transactions_processed
        );
        assert_eq!(
            fast.outcome.instructions_processed,
            generic.outcome.instructions_processed
        );
        assert_eq!(fast.outcome.vote_mutations, generic.outcome.vote_mutations);
        assert_eq!(
            fast.outcome.changed_accounts,
            generic.outcome.changed_accounts
        );
        assert_eq!(fast.outcome.account_state, generic.outcome.account_state);
        assert_eq!(
            fast.outcome.account_state.canonical_hash(),
            generic.outcome.account_state.canonical_hash()
        );
        // The initialized fixture fills the unused account tail with 0xa5.
        // Whole-account equality above proves the direct prefix serializer did
        // not zero or otherwise overwrite those historical tail bytes.
        assert!(
            fast.outcome.account_state[&VOTE_ACCOUNT]
                .data
                .ends_with(&[0xa5; 32])
        );
    }

    #[test]
    fn sequential_lazy_vote_coalesces_cross_slot_commits_into_one_materialization() {
        let mut genesis = exact_genesis();
        let initial_data = initialized_vote_account_data();
        genesis.accounts[0].data = initial_data.clone();
        let first = slot_with_transactions(
            (0_u32..24)
                .map(|tx_index| {
                    fixture_transaction(
                        tx_index,
                        vec![fixture_instruction(
                            0,
                            VOTE_PROGRAM,
                            vote_instruction_data(vec![u64::from(tx_index) + 1]),
                        )],
                    )
                })
                .collect(),
        );
        let mut second = slot_with_transactions(
            (0_u32..24)
                .map(|tx_index| {
                    fixture_transaction(
                        tx_index,
                        vec![fixture_instruction(
                            0,
                            VOTE_PROGRAM,
                            vote_instruction_data(vec![u64::from(tx_index) + 25]),
                        )],
                    )
                })
                .collect(),
        );
        second.slot = first.slot + 1;
        second.parent_slot = first.slot;

        let mut eager = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        eager
            .process_slot_inner(&first, &mut no_diffs, &mut |_| {})
            .unwrap();
        eager
            .process_slot_inner(&second, &mut no_diffs, &mut |_| {})
            .unwrap();
        let eager = eager.finish();

        let mut lazy = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        lazy.lazy_vote_materialization_enabled = true;
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        lazy.process_slot_inner(&first, &mut no_diffs, &mut |_| {})
            .unwrap();
        lazy.process_slot_inner(&second, &mut no_diffs, &mut |_| {})
            .unwrap();
        assert_eq!(lazy.outcome.account_state[&VOTE_ACCOUNT].data, initial_data);
        assert_eq!(lazy.vote_state_cache.materializations(), 0);
        let lazy = lazy.finish();

        assert_eq!(lazy.account_state, eager.account_state);
        assert_eq!(
            lazy.account_state.canonical_hash(),
            eager.account_state.canonical_hash()
        );
        assert_eq!(lazy.changed_accounts, eager.changed_accounts);
        assert_eq!(lazy.lazy_vote_commits, 48);
        assert_eq!(lazy.vote_state_materializations, 1);
    }

    #[test]
    fn sequential_lazy_vote_materializes_before_generic_read_barrier() {
        let mut genesis = exact_genesis();
        genesis.accounts[0].data = initialized_vote_account_data();
        let direct = fixture_transaction(
            0,
            vec![fixture_instruction(
                0,
                VOTE_PROGRAM,
                vote_instruction_data(vec![1]),
            )],
        );
        // Two instructions deliberately route through the generic overlay. Its
        // first Vote must observe tx0's logical state in canonical bytes.
        let generic = fixture_transaction(
            1,
            vec![
                fixture_instruction(0, VOTE_PROGRAM, vote_instruction_data(vec![2])),
                fixture_instruction(1, VOTE_PROGRAM, vote_instruction_data(vec![3])),
            ],
        );
        let slot = slot_with_transactions(vec![direct, generic]);

        let mut eager = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        eager
            .process_slot_inner(&slot, &mut no_diffs, &mut |_| {})
            .unwrap();
        let eager = eager.finish();

        let mut lazy = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        lazy.lazy_vote_materialization_enabled = true;
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        lazy.process_slot_inner(&slot, &mut no_diffs, &mut |_| {})
            .unwrap();
        let lazy = lazy.finish();

        assert_eq!(lazy.account_state, eager.account_state);
        assert_eq!(lazy.transactions_processed, 2);
        assert_eq!(lazy.vote_mutations, 3);
        assert_eq!(lazy.lazy_vote_commits, 1);
        assert_eq!(lazy.vote_state_materializations, 1);
    }

    #[test]
    fn sequential_lazy_vote_zero_balance_deletion_never_resurrects_account() {
        let mut genesis = exact_genesis();
        genesis.accounts[0].data = initialized_vote_account_data();
        let mut transaction = fixture_transaction(
            0,
            vec![fixture_instruction(
                0,
                VOTE_PROGRAM,
                vote_instruction_data(vec![1]),
            )],
        );
        transaction.archived_outcome = CompactArchivedTransactionOutcome::Succeeded;
        transaction.balance_oracle = Some(crate::CompactTransactionBalanceOracle {
            fee: 0,
            pre_balances: smallvec::smallvec![genesis.accounts[0].lamports, 1],
            post_balances: smallvec::smallvec![0, 1],
        });
        let slot = slot_with_transactions(vec![transaction]);

        let mut lazy = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        lazy.lazy_vote_materialization_enabled = true;
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        lazy.process_slot_inner(&slot, &mut no_diffs, &mut |_| {})
            .unwrap();
        let lazy = lazy.finish();

        assert!(!lazy.account_state.contains_key(&VOTE_ACCOUNT));
        assert!(lazy.changed_accounts.contains(&VOTE_ACCOUNT));
        assert_eq!(lazy.lazy_vote_commits, 1);
        assert_eq!(lazy.vote_state_materializations, 0);
    }

    #[test]
    fn sequential_lazy_vote_finish_materializes_a_mid_slot_committed_prefix() {
        let mut genesis = exact_genesis();
        let initial_data = initialized_vote_account_data();
        genesis.accounts[0].data = initial_data.clone();
        let transaction = fixture_transaction(
            0,
            vec![fixture_instruction(
                0,
                VOTE_PROGRAM,
                vote_instruction_data(vec![1]),
            )],
        );
        let transaction_metas = transaction_account_meta_layout(1, &transaction).unwrap();

        let mut lazy = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        lazy.lazy_vote_materialization_enabled = true;
        assert!(matches!(
            lazy.try_process_allocation_minimal_vote(1, &transaction, &transaction_metas, 0,),
            FastVoteTransactionResult::Applied { .. }
        ));
        assert_eq!(lazy.outcome.account_state[&VOTE_ACCOUNT].data, initial_data);

        let outcome = lazy.finish();
        let mut expected_data = initial_data;
        crate::apply_trusted_vote_instruction(
            &mut expected_data,
            &vote_instruction_data(vec![1]),
            0,
        )
        .unwrap();
        assert_eq!(outcome.account_state[&VOTE_ACCOUNT].data, expected_data);
        assert_eq!(outcome.lazy_vote_commits, 1);
        assert_eq!(outcome.vote_state_materializations, 1);
    }

    #[test]
    fn parallel_direct_vote_batch_matches_sequential_state_and_counters() {
        const PARALLEL_VOTE_ACCOUNTS: [[u8; 32]; 4] = [[20; 32], [21; 32], [22; 32], [23; 32]];
        let mut genesis = exact_genesis();
        let vote_data = initialized_vote_account_data();
        for pubkey in PARALLEL_VOTE_ACCOUNTS {
            genesis.accounts.push(CompactGenesisAccount {
                pubkey,
                lamports: 10_000_000,
                owner: VOTE_PROGRAM,
                executable: false,
                rent_epoch: 0,
                data: vote_data.clone(),
            });
        }
        let transactions = (0..32)
            .map(|index| {
                let vote_account = PARALLEL_VOTE_ACCOUNTS[index % PARALLEL_VOTE_ACCOUNTS.len()];
                CompactTransactionProbe {
                    tx_index: index as u32,
                    row_flags: 1,
                    archived_outcome: CompactArchivedTransactionOutcome::Succeeded,
                    balance_oracle: Some(crate::CompactTransactionBalanceOracle {
                        fee: 1,
                        pre_balances: smallvec::smallvec![1_000 - index as u64, 10_000_000],
                        // A deletion after this account's final Vote commutes
                        // with grouped execution and must remain admissible.
                        post_balances: smallvec::smallvec![
                            999 - index as u64,
                            if index == 31 { 0 } else { 10_000_000 }
                        ],
                    }),
                    signature_count: 1,
                    version: CompactMessageVersion::Legacy,
                    header: CompactMessageHeader {
                        num_required_signatures: 1,
                        // The shared authorized voter is writable, matching the
                        // fee-payer conflict that fragmented the first POC.
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    },
                    // Four account-local chains share one writable fee payer and
                    // readonly program. Compact balances remain canonical-order.
                    account_keys: smallvec::smallvec![VOTE_ACCOUNT, vote_account, VOTE_PROGRAM],
                    recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
                    address_table_lookups: Vec::new(),
                    instructions: smallvec::smallvec![CompactInstructionProbe {
                        instruction_index: 0,
                        program_id_index: 2,
                        program_id: VOTE_PROGRAM,
                        account_indexes: smallvec::smallvec![1, 0],
                        data: CompactInstructionData::Raw(
                            vote_instruction_data(vec![index as u64 + 1]).into(),
                        ),
                    }],
                }
            })
            .collect();
        let slot = slot_with_transactions(transactions);

        let mut sequential = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        sequential
            .process_slot_inner(&slot, &mut no_diffs, &mut |_| {})
            .unwrap();

        let mut parallel = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        parallel.parallel_vote_executor = ParallelVoteExecutor::new(4).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        parallel
            .process_slot_inner(&slot, &mut no_diffs, &mut |_| {})
            .unwrap();

        assert_eq!(parallel.outcome.parallel_vote_batches, 1);
        assert_eq!(parallel.outcome.parallel_vote_transactions, 32);
        assert_eq!(parallel.outcome.max_parallel_vote_batch, 32);
        assert_eq!(parallel.vote_state_cache.fast_path_commits(), 32);
        assert_eq!(parallel.outcome.epoch, sequential.outcome.epoch);
        assert_eq!(
            parallel.outcome.transactions_processed,
            sequential.outcome.transactions_processed
        );
        assert_eq!(
            parallel.outcome.instructions_processed,
            sequential.outcome.instructions_processed
        );
        assert_eq!(
            parallel.outcome.vote_mutations,
            sequential.outcome.vote_mutations
        );
        assert_eq!(
            parallel.outcome.changed_accounts,
            sequential.outcome.changed_accounts
        );
        assert_eq!(
            parallel.outcome.account_state,
            sequential.outcome.account_state
        );
        assert_eq!(
            parallel.outcome.account_state.canonical_hash(),
            sequential.outcome.account_state.canonical_hash()
        );
        assert!(
            !parallel
                .outcome
                .account_state
                .contains_key(&PARALLEL_VOTE_ACCOUNTS[3])
        );

        // A worker miss after other groups have speculated must publish
        // nothing. Corrupt one canonical Vote payload after planning remains
        // structurally eligible, then prove COW accounts and detached caches
        // are discarded and rebuilt from the untouched canonical bytes.
        let mut fallback_genesis = genesis.clone();
        fallback_genesis
            .accounts
            .iter_mut()
            .find(|account| account.pubkey == PARALLEL_VOTE_ACCOUNTS[0])
            .unwrap()
            .data
            .truncate(32);
        let mut fallback = LaunchReplay::from_genesis(0, Some(&fallback_genesis), false).unwrap();
        fallback.parallel_vote_executor = ParallelVoteExecutor::new(4).unwrap();
        let before = fallback.outcome.account_state.canonical_hash();
        assert_eq!(
            fallback
                .try_process_parallel_vote_batch(&slot, 0, 0, 0)
                .unwrap(),
            ParallelVoteBatchResult::Fallback(32)
        );
        assert_eq!(fallback.outcome.account_state.canonical_hash(), before);
        assert_eq!(fallback.outcome.parallel_vote_batches, 0);
        assert_eq!(fallback.outcome.parallel_vote_transactions, 0);
        assert_eq!(fallback.vote_state_cache.fast_path_commits(), 0);

        // Moving the same zero projection to the first transaction would
        // delete account zero before its later chain steps. The parallel
        // planner must route that window to sequential execution without
        // touching canonical state or rescanning every suffix.
        let mut deletion_barrier_slot = slot;
        deletion_barrier_slot.transactions[0]
            .balance_oracle
            .as_mut()
            .unwrap()
            .post_balances[1] = 0;
        let mut deletion_barrier = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        deletion_barrier.parallel_vote_executor = ParallelVoteExecutor::new(4).unwrap();
        let before = deletion_barrier.outcome.account_state.canonical_hash();
        assert_eq!(
            deletion_barrier
                .try_process_parallel_vote_batch(&deletion_barrier_slot, 0, 0, 0)
                .unwrap(),
            ParallelVoteBatchResult::Fallback(32)
        );
        assert_eq!(
            deletion_barrier.outcome.account_state.canonical_hash(),
            before
        );
        assert_eq!(deletion_barrier.vote_state_cache.fast_path_commits(), 0);
    }

    #[test]
    fn allocation_minimal_vote_malformed_wire_uses_generic_failure_path() {
        let mut genesis = exact_genesis();
        let initial_data = initialized_vote_account_data();
        genesis.accounts[0].data = initial_data.clone();
        let mut malformed = vote_instruction_data(vec![1]);
        malformed.truncate(17);
        let slot = slot_with_transactions(vec![fixture_transaction(
            0,
            vec![fixture_instruction(0, VOTE_PROGRAM, malformed)],
        )]);

        let mut generic = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut generic_capture =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::First(0));
        generic
            .process_slot_inner(&slot, &mut generic_capture, &mut |_| {})
            .unwrap();

        let mut fast = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        fast.process_slot_inner(&slot, &mut no_diffs, &mut |_| {})
            .unwrap();

        assert_eq!(fast.vote_state_cache.fast_path_commits(), 0);
        assert_eq!(fast.outcome.failed_transactions, 1);
        assert_eq!(
            fast.outcome.first_failed_transaction,
            generic.outcome.first_failed_transaction
        );
        assert_eq!(fast.outcome.account_state, generic.outcome.account_state);
        assert_eq!(fast.outcome.account_state[&VOTE_ACCOUNT].data, initial_data);
    }

    #[test]
    fn allocation_minimal_vote_readonly_alias_and_foreign_owner_guards_fall_back() {
        let mut genesis = exact_genesis();
        genesis.accounts[0].data = initialized_vote_account_data();

        let mut readonly = fixture_transaction(
            0,
            vec![fixture_instruction(
                0,
                VOTE_PROGRAM,
                vote_instruction_data(vec![1]),
            )],
        );
        readonly.header.num_readonly_signed_accounts = 1;
        let mut readonly_replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        readonly_replay
            .process_slot_inner(
                &slot_with_transactions(vec![readonly]),
                &mut no_diffs,
                &mut |_| {},
            )
            .unwrap();
        assert_eq!(readonly_replay.vote_state_cache.fast_path_commits(), 0);
        assert_eq!(readonly_replay.outcome.failed_transactions, 1);

        let mut aliased_instruction =
            fixture_instruction(0, VOTE_PROGRAM, vote_instruction_data(vec![1]));
        aliased_instruction.account_indexes = smallvec::smallvec![0, 0];
        let mut alias_replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        alias_replay
            .process_slot_inner(
                &slot_with_transactions(vec![fixture_transaction(0, vec![aliased_instruction])]),
                &mut no_diffs,
                &mut |_| {},
            )
            .unwrap();
        assert_eq!(alias_replay.vote_state_cache.fast_path_commits(), 0);
        assert_eq!(alias_replay.outcome.transactions_processed, 1);

        let mut foreign_genesis = genesis;
        foreign_genesis.accounts[0].owner = SYSTEM_PROGRAM_ID;
        let mut foreign_replay =
            LaunchReplay::from_genesis(0, Some(&foreign_genesis), false).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        foreign_replay
            .process_slot_inner(
                &slot_with_transactions(vec![fixture_transaction(
                    0,
                    vec![fixture_instruction(
                        0,
                        VOTE_PROGRAM,
                        vote_instruction_data(vec![1]),
                    )],
                )]),
                &mut no_diffs,
                &mut |_| {},
            )
            .unwrap();
        assert_eq!(foreign_replay.vote_state_cache.fast_path_commits(), 0);
        assert_eq!(foreign_replay.outcome.failed_transactions, 1);
    }

    #[test]
    fn allocation_minimal_vote_failure_does_not_advance_account_or_cached_state() {
        let mut genesis = exact_genesis();
        let initial_data = initialized_vote_account_data();
        genesis.accounts[0].data = initial_data.clone();
        let rejected = slot_with_transactions(vec![fixture_transaction(
            0,
            vec![fixture_instruction(
                0,
                VOTE_PROGRAM,
                vote_instruction_data(Vec::new()),
            )],
        )]);
        let accepted_data = vote_instruction_data(vec![2]);
        let mut accepted = slot_with_transactions(vec![fixture_transaction(
            0,
            vec![fixture_instruction(0, VOTE_PROGRAM, accepted_data.clone())],
        )]);
        accepted.slot = 2;
        accepted.parent_slot = 1;

        let mut fast = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut no_diffs =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        fast.process_slot_inner(&rejected, &mut no_diffs, &mut |_| {})
            .unwrap();
        assert_eq!(fast.outcome.failed_transactions, 1);
        assert_eq!(fast.outcome.account_state[&VOTE_ACCOUNT].data, initial_data);
        assert!(fast.vote_state_cache.contains(&VOTE_ACCOUNT));
        assert_eq!(fast.vote_state_cache.fast_path_commits(), 0);

        fast.process_slot_inner(&accepted, &mut no_diffs, &mut |_| {})
            .unwrap();
        assert_eq!(fast.vote_state_cache.fast_path_commits(), 1);
        let mut expected_data = initial_data;
        crate::apply_trusted_vote_instruction(&mut expected_data, &accepted_data, 0).unwrap();
        assert_eq!(
            fast.outcome.account_state[&VOTE_ACCOUNT].data,
            expected_data
        );
    }

    #[test]
    fn zero_diff_budget_keeps_hard_failure_rollback_evidence() {
        let genesis = exact_genesis_with_system_base();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let slot = slot_with_transactions(vec![allocate_with_seed_transaction(true)]);
        let mut capture =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::First(0));
        let error = replay
            .process_slot_inner(&slot, &mut capture, &mut |_| {
                panic!("diagnostic-only rollback evidence must not reach the visitor");
            })
            .unwrap_err();

        assert!(matches!(
            error,
            LaunchReplayError::UnsupportedProgram {
                program_id,
                ..
            } if program_id == [6; 32]
        ));
        let rolled_back = replay.take_rolled_back_transaction().unwrap();
        assert_eq!(rolled_back.instruction_mutations.len(), 1);
        assert_eq!(
            rolled_back.instruction_mutations[0].diff.disposition,
            DiffDisposition::RolledBack
        );
        assert!(rolled_back.instruction_mutations[0].diff.accounts[0].created);
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
    }

    #[test]
    fn no_diff_capture_skips_diagnostic_hard_failure_rollback_evidence() {
        let genesis = exact_genesis_with_system_base();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let slot = slot_with_transactions(vec![allocate_with_seed_transaction(true)]);
        let mut capture =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::None);
        let error = replay
            .process_slot_inner(&slot, &mut capture, &mut |_| {
                panic!("disabled diff capture must not invoke the mutation visitor");
            })
            .unwrap_err();

        assert!(matches!(
            error,
            LaunchReplayError::UnsupportedProgram {
                program_id,
                ..
            } if program_id == [6; 32]
        ));
        assert!(replay.take_rolled_back_transaction().is_none());
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
    }

    #[test]
    fn zero_diff_budget_keeps_derived_failure_rollback_count() {
        let genesis = exact_genesis_with_initialized_stake_source();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let slot = slot_with_transactions(vec![split_transaction(0, false)]);
        let mut capture =
            LaunchInstructionDiffCaptureState::new(LaunchInstructionDiffCapture::First(0));
        replay
            .process_slot_inner(&slot, &mut capture, &mut |_| {
                panic!("a zero diff budget must not emit a derived rollback diff");
            })
            .unwrap();

        assert_eq!(replay.outcome.failed_transactions, 1);
        assert_eq!(replay.outcome.rolled_back_instructions, 1);
        assert_eq!(
            replay
                .outcome
                .first_failed_transaction
                .as_ref()
                .map(|failure| failure.rolled_back_instructions),
            Some(1)
        );
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
    }

    #[test]
    fn failed_split_rolls_back_and_the_later_authorized_retry_commits() {
        let genesis = exact_genesis_with_initialized_stake_source();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), true).unwrap();
        let mut emitted = Vec::new();

        replay
            .process_slot(
                &slot_with_transactions(vec![split_transaction(0, false)]),
                &mut |mutation| emitted.push(mutation.clone()),
            )
            .unwrap();
        assert_eq!(replay.outcome.failed_transactions, 1);
        assert_eq!(replay.outcome.transactions_processed, 0);
        assert_eq!(replay.outcome.rolled_back_instructions, 1);
        assert_eq!(
            replay.outcome.first_failed_transaction,
            Some(LaunchDerivedTransactionFailure {
                location: LaunchReplayFailureLocation {
                    slot: 105_368,
                    transaction_index: Some(0),
                    instruction_index: Some(1),
                },
                reason: LaunchTransactionFailureReason::Stake(
                    LaunchStakeError::MissingRequiredSignature {
                        pubkey: SYSTEM_BASE,
                    },
                ),
                rolled_back_instructions: 1,
            })
        );
        assert!(!replay.outcome.account_state.contains_key(&SYSTEM_TARGET));
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].diff.disposition, DiffDisposition::RolledBack);
        assert!(matches!(
            emitted[0].effect,
            LaunchInstructionEffect::System(LaunchSystemMutation::Allocate { .. })
        ));

        let mut successful_slot = slot_with_transactions(vec![split_transaction(0, true)]);
        successful_slot.slot = 105_800;
        successful_slot.parent_slot = 105_799;
        replay
            .process_slot(&successful_slot, &mut |mutation| {
                emitted.push(mutation.clone());
            })
            .unwrap();
        let outcome = replay.finish();

        assert_eq!(outcome.failed_transactions, 1);
        assert_eq!(outcome.transactions_processed, 1);
        assert_eq!(outcome.instructions_processed, 2);
        assert_eq!(outcome.rolled_back_instructions, 1);
        assert_eq!(outcome.system_mutations, 1);
        assert_eq!(outcome.stake_mutations, 1);
        assert_eq!(
            outcome.account_state[&STAKE_SOURCE].lamports,
            STAKE_SOURCE_LAMPORTS - SPLIT_LAMPORTS
        );
        let target = &outcome.account_state[&SYSTEM_TARGET];
        assert_eq!(target.owner, STAKE_PROGRAM);
        assert_eq!(target.lamports, SPLIT_LAMPORTS);
        assert!(matches!(
            decode_launch_stake_state(SYSTEM_TARGET, &target.data).unwrap(),
            LaunchStakeState::Initialized(LaunchStakeMeta {
                rent_exempt_reserve: STAKE_RENT_RESERVE,
                authorized: LaunchStakeAuthorized {
                    staker: SYSTEM_BASE,
                    ..
                },
                ..
            })
        ));
        assert_eq!(emitted.len(), 3);
        assert_eq!(emitted[1].diff.disposition, DiffDisposition::Committed);
        assert_eq!(emitted[2].diff.disposition, DiffDisposition::Committed);
        assert!(matches!(
            emitted[2].effect,
            LaunchInstructionEffect::Stake(LaunchStakeMutation::Split {
                source: STAKE_SOURCE,
                destination: SYSTEM_TARGET,
                lamports: SPLIT_LAMPORTS,
            })
        ));
    }

    #[test]
    fn epoch_11_set_lockup_shape_replays_and_mutates_stake_state() {
        let mut genesis = exact_genesis_with_initialized_stake_source();
        let original_lockup = LaunchStakeLockup {
            unix_timestamp: 1_700_000_000,
            epoch: 160,
            custodian: SYSTEM_BASE,
        };
        let state = LaunchStakeState::Initialized(LaunchStakeMeta {
            rent_exempt_reserve: STAKE_RENT_RESERVE,
            authorized: LaunchStakeAuthorized {
                staker: SYSTEM_BASE,
                withdrawer: [44; 32],
            },
            lockup: original_lockup,
        });
        let encoded = wincode::serialize(&state).unwrap();
        let stake_account = genesis
            .accounts
            .iter_mut()
            .find(|account| account.pubkey == STAKE_SOURCE)
            .unwrap();
        stake_account.data.fill(0);
        stake_account.data[..encoded.len()].copy_from_slice(&encoded);

        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut emitted = Vec::new();
        replay
            .process_slot(
                &slot_with_transactions(vec![epoch_11_set_lockup_transaction()]),
                &mut |mutation| emitted.push(mutation.clone()),
            )
            .unwrap();
        let outcome = replay.finish();

        assert_eq!(outcome.transactions_processed, 1);
        assert_eq!(outcome.instructions_processed, 1);
        assert_eq!(outcome.stake_mutations, 1);
        assert_eq!(emitted.len(), 1);
        let expected_lockup = LaunchStakeLockup {
            epoch: 177,
            ..original_lockup
        };
        assert!(matches!(
            emitted[0].effect,
            LaunchInstructionEffect::Stake(LaunchStakeMutation::SetLockup {
                stake_account: STAKE_SOURCE,
                lockup,
            }) if lockup == expected_lockup
        ));
        assert_eq!(
            decode_launch_stake_state(STAKE_SOURCE, &outcome.account_state[&STAKE_SOURCE].data,)
                .unwrap(),
            LaunchStakeState::Initialized(LaunchStakeMeta {
                rent_exempt_reserve: STAKE_RENT_RESERVE,
                authorized: LaunchStakeAuthorized {
                    staker: SYSTEM_BASE,
                    withdrawer: [44; 32],
                },
                lockup: expected_lockup,
            })
        );
    }

    #[test]
    fn unsupported_program_rolls_back_preceding_system_allocation() {
        let genesis = exact_genesis_with_system_base();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        let mut emitted = Vec::new();
        let error = replay
            .process_slot(
                &slot_with_transactions(vec![allocate_with_seed_transaction(true)]),
                &mut |mutation| emitted.push(mutation.clone()),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            LaunchReplayError::UnsupportedProgram {
                slot: 105_368,
                transaction_index: 0,
                instruction_index: 1,
                program_id,
            } if program_id == [6; 32]
        ));
        let rolled_back = replay.take_rolled_back_transaction().unwrap();
        let outcome = replay.finish();
        assert_eq!(outcome.slots_processed, 0);
        assert_eq!(outcome.transactions_processed, 0);
        assert_eq!(outcome.instructions_processed, 0);
        assert_eq!(outcome.system_mutations, 0);
        assert!(!outcome.account_state.contains_key(&SYSTEM_TARGET));
        assert!(outcome.changed_accounts.is_empty());
        assert!(emitted.is_empty());
        assert_eq!(rolled_back.instruction_mutations.len(), 1);
        assert_eq!(
            rolled_back.instruction_mutations[0].diff.disposition,
            DiffDisposition::RolledBack
        );
        assert_eq!(rolled_back.instruction_mutations[0].diff.accounts.len(), 1);
        assert!(rolled_back.instruction_mutations[0].diff.accounts[0].created);
    }

    #[test]
    fn diagnostic_failure_preserves_only_the_committed_transaction_prefix() {
        let mut genesis = exact_genesis();
        let initial_data = initialized_vote_account_data();
        genesis.accounts[0].data = initial_data.clone();
        let committed_vote = vote_instruction_data(vec![1]);
        let rolled_back_vote = vote_instruction_data(vec![2]);
        let slot = CompactSlotProbe {
            block_id: 0,
            slot: 1,
            parent_slot: 0,
            block_time: None,
            block_height: None,
            blockhash_id: 1,
            blockhash: [2; 32],
            previous_blockhash_id: 0,
            previous_blockhash: [1; 32],
            transaction_count: 2,
            transactions: vec![
                fixture_transaction(
                    0,
                    vec![fixture_instruction(0, VOTE_PROGRAM, committed_vote.clone())],
                ),
                fixture_transaction(
                    1,
                    vec![
                        fixture_instruction(0, VOTE_PROGRAM, rolled_back_vote),
                        fixture_instruction(1, [6; 32], Vec::new()),
                    ],
                ),
            ],
        };

        let mut expected_data = initial_data;
        crate::apply_trusted_vote_instruction(&mut expected_data, &committed_vote, 0).unwrap();
        let mut emitted = Vec::new();
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        assert!(replay.vote_state_cache.contains(&VOTE_ACCOUNT));
        let error = replay
            .process_slot(&slot, &mut |mutation| emitted.push(mutation.clone()))
            .unwrap_err();
        assert!(!replay.vote_state_cache.contains(&VOTE_ACCOUNT));
        let rolled_back_transaction = replay.take_rolled_back_transaction();
        let failure = LaunchReplayFailure::at_slot(slot.slot, error, rolled_back_transaction);
        let outcome = replay.finish();

        assert_eq!(
            failure.location,
            LaunchReplayFailureLocation {
                slot: 1,
                transaction_index: Some(1),
                instruction_index: Some(1),
            }
        );
        assert_eq!(outcome.slots_processed, 0);
        assert_eq!(outcome.first_slot, None);
        assert_eq!(outcome.last_slot, None);
        assert_eq!(outcome.transactions_processed, 1);
        assert_eq!(outcome.instructions_processed, 1);
        assert_eq!(outcome.vote_mutations, 1);
        assert_eq!(outcome.system_mutations, 0);
        assert_eq!(emitted.len(), 1);
        assert!(matches!(
            &emitted[0].effect,
            LaunchInstructionEffect::Vote {
                mutation: LaunchVoteMutation::Vote(mutation),
                ..
            } if mutation.voted_slots == vec![1]
        ));
        assert!(outcome.instruction_mutations.is_empty());
        assert_eq!(outcome.changed_accounts, BTreeSet::from([VOTE_ACCOUNT]));
        assert_eq!(outcome.account_state[&VOTE_ACCOUNT].data, expected_data);
        let rolled_back = failure.rolled_back_transaction.unwrap();
        assert_eq!(rolled_back.transaction_index, 1);
        assert_eq!(rolled_back.instruction_mutations.len(), 1);
        assert_eq!(
            rolled_back.instruction_mutations[0].diff.disposition,
            DiffDisposition::RolledBack
        );
    }

    #[test]
    fn failed_single_vote_invalidates_cache_and_rolls_back_complete_transaction() {
        let mut genesis = exact_genesis();
        let initial_data = initialized_vote_account_data();
        genesis.accounts[0].data = initial_data.clone();
        let rejected_vote = vote_instruction_data(vec![1]);
        let mut readonly_transaction =
            fixture_transaction(0, vec![fixture_instruction(0, VOTE_PROGRAM, rejected_vote)]);
        readonly_transaction.header.num_readonly_signed_accounts = 1;
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        assert!(replay.vote_state_cache.contains(&VOTE_ACCOUNT));

        replay
            .process_slot(
                &slot_with_transactions(vec![readonly_transaction]),
                &mut |_| {},
            )
            .unwrap();
        assert_eq!(replay.outcome.failed_transactions, 1);
        assert_eq!(replay.outcome.transactions_processed, 0);
        assert_eq!(
            replay.outcome.account_state[&VOTE_ACCOUNT].data,
            initial_data
        );
        assert!(!replay.vote_state_cache.contains(&VOTE_ACCOUNT));

        let committed_vote = vote_instruction_data(vec![2]);
        let mut next_slot = slot_with_transactions(vec![fixture_transaction(
            0,
            vec![fixture_instruction(0, VOTE_PROGRAM, committed_vote.clone())],
        )]);
        next_slot.slot += 1;
        next_slot.parent_slot += 1;
        let mut expected_data = initial_data;
        crate::apply_trusted_vote_instruction(&mut expected_data, &committed_vote, 0).unwrap();

        replay.process_slot(&next_slot, &mut |_| {}).unwrap();
        assert_eq!(replay.outcome.transactions_processed, 1);
        assert_eq!(
            replay.outcome.account_state[&VOTE_ACCOUNT].data,
            expected_data
        );
        assert!(replay.vote_state_cache.contains(&VOTE_ACCOUNT));
    }

    #[test]
    fn implemented_vote_errors_are_derived_transaction_failures() {
        let authority = [91; 32];
        let implemented = LaunchReplayError::VoteMutation {
            slot: 633_492,
            transaction_index: 0,
            instruction_index: 0,
            source: LaunchVoteError::MissingRequiredSignature { pubkey: authority },
        };
        assert!(matches!(
            historical_transaction_failure(&implemented),
            Some(LaunchTransactionFailureReason::Vote(reason))
                if reason.contains("did not sign")
        ));

        let unsupported = LaunchReplayError::VoteMutation {
            slot: 633_492,
            transaction_index: 0,
            instruction_index: 0,
            source: LaunchVoteError::UnsupportedInstruction("UpdateNode"),
        };
        assert!(historical_transaction_failure(&unsupported).is_none());
    }

    #[test]
    fn wrong_owner_vote_account_reaches_native_failure_and_replay_continues() {
        let mut genesis = exact_genesis();
        genesis.accounts[0].owner = SYSTEM_PROGRAM_ID;
        genesis.accounts[0].data.clear();
        let transaction = fixture_transaction(
            0,
            vec![fixture_instruction(
                0,
                VOTE_PROGRAM,
                vote_instruction_data(vec![1]),
            )],
        );
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();

        replay
            .process_slot(&slot_with_transactions(vec![transaction]), &mut |_| {})
            .unwrap();
        let outcome = replay.finish();

        assert_eq!(outcome.slots_processed, 1);
        assert_eq!(outcome.transactions_processed, 0);
        assert_eq!(outcome.failed_transactions, 1);
        assert!(matches!(
            outcome.first_failed_transaction,
            Some(LaunchDerivedTransactionFailure {
                reason: LaunchTransactionFailureReason::Vote(reason),
                ..
            }) if reason.contains("decode launch-era vote account")
        ));
        assert_eq!(
            outcome.account_state[&VOTE_ACCOUNT].owner,
            SYSTEM_PROGRAM_ID
        );
    }

    #[test]
    fn entering_epoch_34_materializes_exact_legacy_bpf_loader_builtin() {
        let mut genesis = exact_genesis();
        genesis.epoch_schedule.slots_per_epoch = 1;
        genesis.epoch_schedule.leader_schedule_slot_offset = 1;
        let mut replay = LaunchReplay::from_genesis(0, Some(&genesis), false).unwrap();
        replay.enable_bank_lifecycle();
        let blockhash = |slot: u64| {
            let mut hash = [0_u8; 32];
            hash[..8].copy_from_slice(&slot.wrapping_add(2).to_le_bytes());
            hash
        };
        for slot in 0..34 {
            replay
                .process_slot(
                    &CompactSlotProbe {
                        block_id: slot as u32,
                        slot,
                        parent_slot: slot.saturating_sub(1),
                        block_time: None,
                        block_height: None,
                        blockhash_id: slot as u32,
                        blockhash: blockhash(slot),
                        previous_blockhash_id: slot.saturating_sub(1) as u32,
                        previous_blockhash: if slot == 0 {
                            genesis.genesis_hash
                        } else {
                            blockhash(slot - 1)
                        },
                        transaction_count: 0,
                        transactions: Vec::new(),
                    },
                    &mut |_| {},
                )
                .unwrap();
        }
        assert!(
            !replay
                .outcome
                .account_state
                .contains_key(&BPF_LOADER_PROGRAM_ID)
        );
        let account_count = replay.outcome.account_state.len();

        replay
            .process_slot(
                &CompactSlotProbe {
                    block_id: 34,
                    slot: 34,
                    parent_slot: 33,
                    block_time: None,
                    block_height: None,
                    blockhash_id: 34,
                    blockhash: blockhash(34),
                    previous_blockhash_id: 33,
                    previous_blockhash: blockhash(33),
                    transaction_count: 0,
                    transactions: Vec::new(),
                },
                &mut |_| {},
            )
            .unwrap();

        assert_eq!(replay.outcome.epoch, 34);
        assert_eq!(replay.outcome.account_state.len(), account_count + 1);
        assert_eq!(
            replay.outcome.account_state[&BPF_LOADER_PROGRAM_ID],
            AccountSnapshot {
                lamports: 1,
                owner: NATIVE_LOADER_ID,
                executable: true,
                rent_epoch: 0,
                data: BPF_LOADER_BUILTIN_NAME.as_bytes().to_vec().into(),
            }
        );
        assert!(replay.bpf_loader_is_active());
    }

    #[test]
    fn observed_epoch_34_compact_write_commits_and_is_counted() {
        let genesis = exact_genesis_with_observed_bpf_program(false);
        let payload: Vec<u8> = (0..932).map(|index| (index % 251) as u8).collect();
        let instruction_data = observed_bpf_write(932, &payload);
        assert_eq!(instruction_data.len(), 948);
        let slot = observed_bpf_slot(observed_bpf_loader_transaction(instruction_data, false));
        let mut replay = LaunchReplay::from_genesis(33, Some(&genesis), true).unwrap();
        replay.activate_epoch_programs(Some((33, 34)));
        let mut emitted = Vec::new();

        replay
            .process_slot(&slot, &mut |mutation| emitted.push(mutation.clone()))
            .unwrap();

        assert!(replay.bpf_program_cache.is_empty());
        let outcome = replay.finish();
        let program = &outcome.account_state[&OBSERVED_BPF_PROGRAM_ACCOUNT];
        assert!(program.data[..932].iter().all(|byte| *byte == 0));
        assert_eq!(&program.data[932..1_864], payload.as_slice());
        assert!(program.data[1_864..].iter().all(|byte| *byte == 0));
        assert_eq!(outcome.transactions_processed, 1);
        assert_eq!(outcome.instructions_processed, 1);
        assert_eq!(outcome.bpf_loader_mutations, 1);
        assert_eq!(outcome.failed_transactions, 0);
        assert_eq!(
            outcome.changed_accounts,
            BTreeSet::from([OBSERVED_BPF_PROGRAM_ACCOUNT])
        );
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].diff.disposition, DiffDisposition::Committed);
        assert!(matches!(
            emitted[0].effect,
            LaunchInstructionEffect::BpfLoader(LaunchBpfLoaderMutation::Write {
                program_account: OBSERVED_BPF_PROGRAM_ACCOUNT,
                offset: 932,
                bytes_written: 932,
            })
        ));
    }

    #[test]
    fn later_unsupported_instruction_rolls_back_bpf_write_without_publication() {
        let genesis = exact_genesis_with_observed_bpf_program(false);
        let initial_program_data = genesis
            .accounts
            .iter()
            .find(|account| account.pubkey == OBSERVED_BPF_PROGRAM_ACCOUNT)
            .unwrap()
            .data
            .clone();
        let instruction_data = observed_bpf_write(932, &[0x5a; 932]);
        let slot = observed_bpf_slot(observed_bpf_loader_transaction(instruction_data, true));
        let mut replay = LaunchReplay::from_genesis(33, Some(&genesis), true).unwrap();
        replay.activate_epoch_programs(Some((33, 34)));
        let mut emitted = Vec::new();

        let error = replay
            .process_slot(&slot, &mut |mutation| emitted.push(mutation.clone()))
            .unwrap_err();

        assert!(matches!(
            error,
            LaunchReplayError::UnsupportedProgram {
                slot: OBSERVED_BPF_SLOT,
                transaction_index: 0,
                instruction_index: 1,
                program_id: UNSUPPORTED_PROGRAM,
            }
        ));
        assert!(replay.bpf_program_cache.is_empty());
        let rolled_back = replay.take_rolled_back_transaction().unwrap();
        assert_eq!(rolled_back.instruction_mutations.len(), 1);
        assert_eq!(
            rolled_back.instruction_mutations[0].diff.disposition,
            DiffDisposition::RolledBack
        );
        assert!(matches!(
            rolled_back.instruction_mutations[0].effect,
            LaunchInstructionEffect::BpfLoader(LaunchBpfLoaderMutation::Write {
                program_account: OBSERVED_BPF_PROGRAM_ACCOUNT,
                offset: 932,
                bytes_written: 932,
            })
        ));
        let outcome = replay.finish();
        assert_eq!(outcome.transactions_processed, 0);
        assert_eq!(outcome.instructions_processed, 0);
        assert_eq!(outcome.bpf_loader_mutations, 0);
        assert_eq!(outcome.failed_transactions, 0);
        assert!(outcome.instruction_mutations.is_empty());
        assert!(outcome.changed_accounts.is_empty());
        assert!(emitted.is_empty());
        assert_eq!(
            outcome.account_state[&OBSERVED_BPF_PROGRAM_ACCOUNT].data,
            initial_program_data
        );
    }

    #[test]
    fn executable_loader_target_is_a_hard_invocation_stop_before_decode() {
        let genesis = exact_genesis_with_observed_bpf_program(true);
        let slot = observed_bpf_slot(observed_bpf_loader_transaction(vec![0xff], false));
        let mut replay = LaunchReplay::from_genesis(33, Some(&genesis), false).unwrap();
        replay.activate_epoch_programs(Some((33, 34)));

        let error = replay.process_slot(&slot, &mut |_| {}).unwrap_err();

        assert!(matches!(
            error,
            LaunchReplayError::BpfLoaderMutation {
                slot: OBSERVED_BPF_SLOT,
                transaction_index: 0,
                instruction_index: 0,
                source: LaunchBpfLoaderError::ExecutableInvocation {
                    pubkey: OBSERVED_BPF_PROGRAM_ACCOUNT,
                },
            }
        ));
        assert_eq!(replay.outcome.failed_transactions, 0);
        assert_eq!(replay.outcome.transactions_processed, 0);
        assert_eq!(replay.outcome.instructions_processed, 0);
        assert_eq!(replay.outcome.bpf_loader_mutations, 0);
        assert!(replay.bpf_program_cache.is_empty());
        assert!(replay.take_rolled_back_transaction().is_none());
    }
}
