use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CpiCoverage, ExecutionStatus, InstructionCoverage, InstructionDataCoverage,
    MAX_CANONICAL_SHORT_VEC_ITEMS, TransactionHeader, TransactionView,
};

use super::batch::{borrowed_classic_token_batch_prefix, inspect_classic_token_batch};
use super::decode::validate_classic_token_instruction_structure;

use super::{
    AccountLifecycleChange, BalanceDirection, ClassicTokenDecodeError, ClassicTokenInstruction,
    DecodedClassicTokenInstruction, HistoryCoverage, LifecycleCause, MAX_EXPANDED_TOKEN_LEAVES,
    MAX_TOKEN_ACCOUNT_UPDATES_PER_TRANSACTION, MAX_TOKEN_COVERAGE_ISSUES_PER_TRANSACTION,
    MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION, MAX_TOKEN_INSTRUCTION_ACCOUNTS,
    MAX_TOKEN_INSTRUCTION_DATA_BYTES, ObservedTokenInstruction, PubkeyBytes,
    RawUnknownTokenInstruction, TargetAccountSnapshot, TargetAccountUpdate, TargetMintEffect,
    TargetMintTrackerError, TargetMintTrackerSnapshot, TargetTransfer, TokenAccountLifecycle,
    TokenAccountRole, TokenAccountState, TokenCommitState, TokenCoverageIssue,
    TokenCoverageIssueKind, TokenInvocationEvidence, TokenTrackerBuffer, TokenTrackerWork,
    TrackedTokenEvent, TrackedTokenTransaction, TransferLeg, TransferLegRole,
    decode_classic_token_batch, decode_classic_token_instruction, is_classic_token_instruction,
};

// One leaf can change two lifecycles and add one amount effect.
const MAX_TOKEN_EFFECTS_PER_EVENT: usize = 3;

/// An ordered lifecycle tracker for one mint on the classic SPL Token program.
#[derive(Debug, Clone)]
pub struct TargetMintTracker {
    target_mint: PubkeyBytes,
    history: HistoryCoverage,
    /// This map contains only accounts that are or were related to the target.
    accounts: BTreeMap<PubkeyBytes, TargetAccountSnapshot>,
    /// A history gap advances this revision in constant time.
    certainty_revision: u64,
    last_transaction_work: TokenTrackerWork,
}

struct AccountOverlay<'a> {
    base: &'a BTreeMap<PubkeyBytes, TargetAccountSnapshot>,
    changes: BTreeMap<PubkeyBytes, TargetAccountSnapshot>,
}

impl<'a> AccountOverlay<'a> {
    fn new(base: &'a BTreeMap<PubkeyBytes, TargetAccountSnapshot>) -> Self {
        Self {
            base,
            changes: BTreeMap::new(),
        }
    }

    fn get(&self, account: &PubkeyBytes) -> Option<TargetAccountSnapshot> {
        self.changes
            .get(account)
            .or_else(|| self.base.get(account))
            .copied()
    }

    fn contains_key(&self, account: &PubkeyBytes) -> bool {
        self.changes.contains_key(account) || self.base.contains_key(account)
    }

    fn insert(&mut self, account: PubkeyBytes, state: TargetAccountSnapshot) {
        self.changes.insert(account, state);
    }

    fn change_count(&self) -> usize {
        self.changes.len()
    }

    fn into_changes(self) -> BTreeMap<PubkeyBytes, TargetAccountSnapshot> {
        self.changes
    }
}

fn preflight_transaction(
    transaction: TransactionView<'_>,
) -> Result<usize, TargetMintTrackerError> {
    if transaction.instructions.len() > MAX_CANONICAL_SHORT_VEC_ITEMS {
        return Err(TargetMintTrackerError::TransactionInstructionLimit {
            limit: MAX_CANONICAL_SHORT_VEC_ITEMS,
            actual: transaction.instructions.len(),
        });
    }

    // Check all direct source geometry before a decoder can clone bytes or
    // account keys from any instruction.
    for instruction in transaction.instructions {
        if instruction.program_id.is_none() {
            return Err(TargetMintTrackerError::ProgramIdentityNotRequested);
        }
        if instruction.accounts.len() > MAX_TOKEN_INSTRUCTION_ACCOUNTS {
            return Err(TargetMintTrackerError::InstructionGeometry {
                coordinate: instruction.coordinate,
                error: ClassicTokenDecodeError::InstructionAccountLimit {
                    limit: MAX_TOKEN_INSTRUCTION_ACCOUNTS,
                    actual: instruction.accounts.len(),
                },
            });
        }
        if instruction.data.len() > MAX_TOKEN_INSTRUCTION_DATA_BYTES {
            return Err(TargetMintTrackerError::InstructionGeometry {
                coordinate: instruction.coordinate,
                error: ClassicTokenDecodeError::InstructionDataLimit {
                    limit: MAX_TOKEN_INSTRUCTION_DATA_BYTES,
                    actual: instruction.data.len(),
                },
            });
        }
    }

    let mut expanded_token_leaves = 0usize;
    let mut token_input_bytes = 0usize;
    for instruction in transaction.instructions {
        if !is_classic_token_instruction(instruction) {
            continue;
        }

        let Some(account_bytes) = instruction.accounts.len().checked_mul(32) else {
            return Err(TargetMintTrackerError::TokenInputByteLimit {
                limit: MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION,
                actual: usize::MAX,
            });
        };
        let Some(instruction_input_bytes) = instruction.data.len().checked_add(account_bytes)
        else {
            return Err(TargetMintTrackerError::TokenInputByteLimit {
                limit: MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION,
                actual: usize::MAX,
            });
        };
        let Some(total_input_bytes) = token_input_bytes.checked_add(instruction_input_bytes) else {
            return Err(TargetMintTrackerError::TokenInputByteLimit {
                limit: MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION,
                actual: usize::MAX,
            });
        };
        token_input_bytes = total_input_bytes;
        if token_input_bytes > MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION {
            return Err(TargetMintTrackerError::TokenInputByteLimit {
                limit: MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION,
                actual: token_input_bytes,
            });
        }

        let instruction_leaves = if instruction.data_coverage == InstructionDataCoverage::Exact
            && instruction.data.first() == Some(&255)
        {
            let inspection = inspect_classic_token_batch(&instruction.accounts, &instruction.data)
                .map_err(|error| TargetMintTrackerError::InstructionGeometry {
                    coordinate: instruction.coordinate,
                    error,
                })?;
            let completion_is_proven =
                batch_completion_is_proven(transaction.header, instruction.coordinate);
            if completion_is_proven {
                if let Some(error) = inspection.terminal_error.as_ref() {
                    return Err(TargetMintTrackerError::CompletedBatchHasTerminalError {
                        coordinate: instruction.coordinate,
                        error: error.clone(),
                    });
                }
                for child in borrowed_classic_token_batch_prefix(
                    &instruction.accounts,
                    &instruction.data,
                    &inspection,
                ) {
                    match validate_classic_token_instruction_structure(child.accounts, child.data) {
                        Ok(()) | Err(ClassicTokenDecodeError::UnknownTag { .. }) => {}
                        Err(ClassicTokenDecodeError::AllocationFailed { requested }) => {
                            return Err(TargetMintTrackerError::AllocationFailed {
                                buffer: TokenTrackerBuffer::InstructionDecode,
                                requested,
                            });
                        }
                        Err(error) => {
                            return Err(
                                TargetMintTrackerError::CompletedBatchChildHasStructuralError {
                                    coordinate: instruction.coordinate,
                                    batch_index: child.batch_index,
                                    error,
                                },
                            );
                        }
                    }
                }
            }
            inspection.child_count + usize::from(inspection.terminal_error.is_some())
        } else {
            1
        };

        expanded_token_leaves = expanded_token_leaves.saturating_add(instruction_leaves);
        if expanded_token_leaves > MAX_EXPANDED_TOKEN_LEAVES {
            return Err(TargetMintTrackerError::ExpandedTokenLeafLimit {
                limit: MAX_EXPANDED_TOKEN_LEAVES,
                actual: expanded_token_leaves,
            });
        }
    }

    Ok(expanded_token_leaves)
}

fn batch_completion_is_proven(
    transaction_header: TransactionHeader,
    coordinate: crate::InstructionCoordinate,
) -> bool {
    match transaction_header.status {
        ExecutionStatus::Succeeded => true,
        ExecutionStatus::Failed => transaction_header
            .failed_outer_instruction_index
            .is_some_and(|failed_outer| failed_outer > coordinate.outer_index),
        ExecutionStatus::Unknown(_) => false,
    }
}

fn reserve_tracker_buffer<T>(
    values: &mut Vec<T>,
    requested: usize,
    buffer: TokenTrackerBuffer,
) -> Result<(), TargetMintTrackerError> {
    values
        .try_reserve_exact(requested)
        .map_err(|_| TargetMintTrackerError::AllocationFailed { buffer, requested })
}

fn copy_tracker_slice<T: Copy>(
    source: &[T],
    buffer: TokenTrackerBuffer,
) -> Result<Vec<T>, TargetMintTrackerError> {
    let mut copy = Vec::new();
    reserve_tracker_buffer(&mut copy, source.len(), buffer)?;
    copy.extend_from_slice(source);
    Ok(copy)
}

impl TargetMintTracker {
    /// Create a tracker with an explicit history trust state.
    pub fn new(target_mint: PubkeyBytes, history: HistoryCoverage) -> Self {
        Self {
            target_mint,
            history,
            accounts: BTreeMap::new(),
            certainty_revision: 1,
            last_transaction_work: TokenTrackerWork::default(),
        }
    }

    /// Create a tracker for a continuous scan from a complete start.
    pub fn from_complete_start(target_mint: PubkeyBytes) -> Self {
        Self::new(target_mint, HistoryCoverage::Complete)
    }

    /// Create a tracker for a sparse range with no trusted opening state.
    pub fn from_sparse_start(target_mint: PubkeyBytes) -> Self {
        Self::new(target_mint, HistoryCoverage::Partial)
    }

    /// Create a tracker from a trusted set of active target accounts.
    pub fn from_active_account_seed(
        target_mint: PubkeyBytes,
        accounts: impl IntoIterator<Item = PubkeyBytes>,
    ) -> Self {
        let mut tracker = Self::from_complete_start(target_mint);
        for account in accounts {
            tracker.accounts.insert(
                account,
                TargetAccountSnapshot {
                    lifecycle: TokenAccountLifecycle {
                        generation: 1,
                        state: TokenAccountState::ActiveTarget,
                    },
                    confirmed_revision: tracker.certainty_revision,
                },
            );
        }
        tracker
    }

    /// Restore all tracker state from an in-memory checkpoint.
    pub fn from_snapshot(snapshot: TargetMintTrackerSnapshot) -> Self {
        Self {
            target_mint: snapshot.target_mint,
            history: snapshot.history,
            accounts: snapshot.accounts,
            certainty_revision: snapshot.certainty_revision,
            last_transaction_work: TokenTrackerWork::default(),
        }
    }

    /// Capture all state that is needed to continue the scan.
    pub fn snapshot(&self) -> TargetMintTrackerSnapshot {
        TargetMintTrackerSnapshot {
            target_mint: self.target_mint,
            history: self.history,
            accounts: self.accounts.clone(),
            certainty_revision: self.certainty_revision,
        }
    }

    /// Replace all tracker state with an in-memory checkpoint.
    pub fn restore(&mut self, snapshot: TargetMintTrackerSnapshot) {
        *self = Self::from_snapshot(snapshot);
    }

    /// Return work retained for the last processed transaction.
    pub const fn last_transaction_work(&self) -> TokenTrackerWork {
        self.last_transaction_work
    }

    /// Return the number of retained account lifetimes.
    pub fn retained_account_count(&self) -> usize {
        self.accounts.len()
    }

    pub const fn target_mint(&self) -> PubkeyBytes {
        self.target_mint
    }

    pub const fn history_coverage(&self) -> HistoryCoverage {
        self.history
    }

    /// Return the retained account record, including stale observed state.
    pub fn account_state(&self, account: &PubkeyBytes) -> Option<TargetAccountSnapshot> {
        self.accounts.get(account).copied()
    }

    /// Return the retained lifecycle, including stale observed state.
    pub fn lifecycle(&self, account: &PubkeyBytes) -> Option<TokenAccountLifecycle> {
        self.account_state(account).map(|state| state.lifecycle)
    }

    pub fn is_active_target(&self, account: &PubkeyBytes) -> bool {
        self.accounts.get(account).is_some_and(|state| {
            state.confirmed_revision == self.certainty_revision
                && state.lifecycle.state == TokenAccountState::ActiveTarget
        })
    }

    /// Iterate over active target accounts in public-key byte order.
    pub fn active_target_accounts(&self) -> impl Iterator<Item = PubkeyBytes> + '_ {
        self.accounts.iter().filter_map(|(account, state)| {
            (state.confirmed_revision == self.certainty_revision
                && state.lifecycle.state == TokenAccountState::ActiveTarget)
                .then_some(*account)
        })
    }

    /// Decode and apply one transaction in source order.
    ///
    /// Only a successful transaction commits delta effects and account state.
    pub fn process_transaction(
        &mut self,
        transaction: TransactionView<'_>,
    ) -> Result<TrackedTokenTransaction, TargetMintTrackerError> {
        let expanded_token_leaves = preflight_transaction(transaction)?;
        let execution_status = transaction.header.status;
        let committed = matches!(execution_status, ExecutionStatus::Succeeded);
        let unknown_execution = matches!(execution_status, ExecutionStatus::Unknown(_));
        let state_can_change_history = committed || unknown_execution;
        let target_mint = self.target_mint;
        let mut working_history = self.history;
        let mut working_revision = self.certainty_revision;
        let mut accounts = AccountOverlay::new(&self.accounts);
        let mut events = Vec::new();
        reserve_tracker_buffer(
            &mut events,
            expanded_token_leaves,
            TokenTrackerBuffer::Events,
        )?;
        let mut coverage_issues = Vec::new();
        let coverage_issue_capacity = expanded_token_leaves
            .checked_mul(4)
            .and_then(|token_issues| token_issues.checked_add(transaction.instructions.len()))
            .and_then(|instruction_issues| instruction_issues.checked_add(4))
            .ok_or(TargetMintTrackerError::AllocationFailed {
                buffer: TokenTrackerBuffer::CoverageIssues,
                requested: usize::MAX,
            })?;
        debug_assert!(coverage_issue_capacity <= MAX_TOKEN_COVERAGE_ISSUES_PER_TRANSACTION);
        reserve_tracker_buffer(
            &mut coverage_issues,
            coverage_issue_capacity,
            TokenTrackerBuffer::CoverageIssues,
        )?;
        let immutable_owner_target_hints = if committed {
            immutable_owner_target_hints(
                transaction.instructions,
                target_mint,
                expanded_token_leaves,
            )?
        } else {
            BTreeSet::new()
        };

        match transaction.header.instruction_coverage {
            InstructionCoverage::Complete => {}
            InstructionCoverage::Unknown(reason) => {
                coverage_issues.push(TokenCoverageIssue {
                    coordinate: None,
                    kind: TokenCoverageIssueKind::IncompleteInstructions(reason),
                });
                if state_can_change_history {
                    mark_history_partial(&mut working_history, &mut working_revision)?;
                }
            }
        }
        match transaction.header.cpi_coverage {
            CpiCoverage::Complete => {}
            CpiCoverage::NotRecorded => {
                coverage_issues.push(TokenCoverageIssue {
                    coordinate: None,
                    kind: TokenCoverageIssueKind::CpiNotRecorded,
                });
                if state_can_change_history {
                    mark_history_partial(&mut working_history, &mut working_revision)?;
                }
            }
            CpiCoverage::Unknown(reason) => {
                coverage_issues.push(TokenCoverageIssue {
                    coordinate: None,
                    kind: TokenCoverageIssueKind::IncompleteCpi(reason),
                });
                if state_can_change_history {
                    mark_history_partial(&mut working_history, &mut working_revision)?;
                }
            }
        }
        if let ExecutionStatus::Unknown(reason) = execution_status {
            coverage_issues.push(TokenCoverageIssue {
                coordinate: None,
                kind: TokenCoverageIssueKind::UnknownExecution(reason),
            });
            mark_history_partial(&mut working_history, &mut working_revision)?;
        }

        for (position, source_instruction) in transaction.instructions.iter().enumerate() {
            let expected_order = u32::try_from(position).unwrap_or(u32::MAX);
            if source_instruction.coordinate.order != expected_order {
                coverage_issues.push(TokenCoverageIssue {
                    coordinate: Some(source_instruction.coordinate),
                    kind: TokenCoverageIssueKind::InvalidInstructionOrder {
                        expected: expected_order,
                        actual: source_instruction.coordinate.order,
                    },
                });
                if state_can_change_history {
                    mark_history_partial(&mut working_history, &mut working_revision)?;
                }
            }

            if !is_classic_token_instruction(source_instruction) {
                continue;
            }
            if source_instruction.data_coverage != InstructionDataCoverage::Exact {
                let touches_target =
                    touches_current_target(&accounts, target_mint, &source_instruction.accounts);
                if touches_target || state_can_change_history {
                    coverage_issues.push(TokenCoverageIssue {
                        coordinate: Some(source_instruction.coordinate),
                        kind: TokenCoverageIssueKind::InstructionDataUnavailable(
                            source_instruction.data_coverage,
                        ),
                    });
                    if touches_target {
                        events.push(raw_unknown_event(source_instruction, transaction.header)?);
                    }
                    if state_can_change_history {
                        mark_history_partial(&mut working_history, &mut working_revision)?;
                    }
                }
                continue;
            }

            if source_instruction.data.first() == Some(&255) {
                let batch = match decode_classic_token_batch(
                    &source_instruction.accounts,
                    &source_instruction.data,
                ) {
                    Ok(batch) => batch,
                    Err(ClassicTokenDecodeError::AllocationFailed { requested }) => {
                        return Err(TargetMintTrackerError::AllocationFailed {
                            buffer: TokenTrackerBuffer::BatchChildren,
                            requested,
                        });
                    }
                    Err(error) => {
                        let touches_target = touches_current_target(
                            &accounts,
                            target_mint,
                            &source_instruction.accounts,
                        );
                        if touches_target || state_can_change_history {
                            coverage_issues.push(TokenCoverageIssue {
                                coordinate: Some(source_instruction.coordinate),
                                kind: TokenCoverageIssueKind::Decode(error),
                            });
                            if touches_target {
                                events.push(raw_unknown_event(
                                    source_instruction,
                                    transaction.header,
                                )?);
                            }
                            if state_can_change_history {
                                mark_history_partial(&mut working_history, &mut working_revision)?;
                            }
                        }
                        continue;
                    }
                };

                let terminal_error = batch.terminal_error;
                for child in batch.children {
                    process_exact_leaf(
                        &mut accounts,
                        target_mint,
                        &mut working_history,
                        &mut working_revision,
                        source_instruction.coordinate,
                        Some(child.batch_index),
                        source_instruction
                            .program_id
                            .ok_or(TargetMintTrackerError::ProgramIdentityNotRequested)?,
                        &child.accounts,
                        &child.data,
                        immutable_owner_target_hints.contains(&(
                            source_instruction.coordinate.order,
                            Some(child.batch_index),
                        )),
                        transaction.header,
                        state_can_change_history,
                        &mut events,
                        &mut coverage_issues,
                    )?;
                }
                if let Some(error) = terminal_error {
                    let touches_target = touches_current_target(
                        &accounts,
                        target_mint,
                        &source_instruction.accounts,
                    );
                    if touches_target || state_can_change_history {
                        coverage_issues.push(TokenCoverageIssue {
                            coordinate: Some(source_instruction.coordinate),
                            kind: TokenCoverageIssueKind::Decode(error),
                        });
                        if touches_target {
                            events.push(raw_unknown_event(source_instruction, transaction.header)?);
                        }
                        if state_can_change_history {
                            mark_history_partial(&mut working_history, &mut working_revision)?;
                        }
                    }
                }
                continue;
            }

            process_exact_leaf(
                &mut accounts,
                target_mint,
                &mut working_history,
                &mut working_revision,
                source_instruction.coordinate,
                None,
                source_instruction
                    .program_id
                    .ok_or(TargetMintTrackerError::ProgramIdentityNotRequested)?,
                &source_instruction.accounts,
                &source_instruction.data,
                immutable_owner_target_hints.contains(&(source_instruction.coordinate.order, None)),
                transaction.header,
                state_can_change_history,
                &mut events,
                &mut coverage_issues,
            )?;
        }

        let overlay_account_count = accounts.change_count();
        let changes = accounts.into_changes();
        if changes.len() > MAX_TOKEN_ACCOUNT_UPDATES_PER_TRANSACTION {
            return Err(TargetMintTrackerError::AccountUpdateLimit {
                limit: MAX_TOKEN_ACCOUNT_UPDATES_PER_TRANSACTION,
                actual: changes.len(),
            });
        }
        let account_updates = if committed {
            let mut updates = Vec::new();
            reserve_tracker_buffer(
                &mut updates,
                changes.len(),
                TokenTrackerBuffer::AccountUpdates,
            )?;
            for (account, state) in &changes {
                updates.push(TargetAccountUpdate {
                    account: *account,
                    state: *state,
                });
            }
            updates
        } else {
            Vec::new()
        };
        self.last_transaction_work = TokenTrackerWork {
            overlay_accounts: overlay_account_count,
        };
        if committed {
            self.accounts.extend(changes);
            self.history = working_history;
            self.certainty_revision = working_revision;
        } else if unknown_execution {
            // Account changes do not commit when execution is unknown. The gap
            // invalidates old unchecked-transfer evidence in constant time.
            self.history = working_history;
            self.certainty_revision = working_revision;
        }

        Ok(TrackedTokenTransaction {
            block: transaction.block,
            tx_index: transaction.header.tx_index,
            execution_status,
            events,
            coverage_issues,
            account_updates,
            history_after: self.history,
            certainty_revision_after: self.certainty_revision,
        })
    }
}

fn immutable_owner_target_hints(
    instructions: &[crate::ResolvedInstruction],
    target_mint: PubkeyBytes,
    expanded_token_leaves: usize,
) -> Result<BTreeSet<(u32, Option<u32>)>, TargetMintTrackerError> {
    enum LookaheadLeaf {
        Decoded {
            order: u32,
            batch_index: Option<u32>,
            instruction: DecodedClassicTokenInstruction,
        },
        Barrier(Vec<PubkeyBytes>),
    }

    let mut leaves = Vec::new();
    reserve_tracker_buffer(
        &mut leaves,
        expanded_token_leaves,
        TokenTrackerBuffer::ImmutableOwnerLookahead,
    )?;
    for instruction in instructions {
        if !is_classic_token_instruction(instruction) {
            continue;
        }
        if instruction.data_coverage != InstructionDataCoverage::Exact {
            leaves.push(LookaheadLeaf::Barrier(copy_tracker_slice(
                &instruction.accounts,
                TokenTrackerBuffer::ImmutableOwnerLookahead,
            )?));
            continue;
        }
        if instruction.data.first() == Some(&255) {
            let batch = match decode_classic_token_batch(&instruction.accounts, &instruction.data) {
                Ok(batch) => batch,
                Err(ClassicTokenDecodeError::AllocationFailed { requested }) => {
                    return Err(TargetMintTrackerError::AllocationFailed {
                        buffer: TokenTrackerBuffer::BatchChildren,
                        requested,
                    });
                }
                Err(_) => {
                    leaves.push(LookaheadLeaf::Barrier(copy_tracker_slice(
                        &instruction.accounts,
                        TokenTrackerBuffer::ImmutableOwnerLookahead,
                    )?));
                    continue;
                }
            };
            let has_terminal_error = batch.terminal_error.is_some();
            for child in batch.children {
                match decode_classic_token_instruction(&child.accounts, &child.data) {
                    Ok(decoded) => leaves.push(LookaheadLeaf::Decoded {
                        order: instruction.coordinate.order,
                        batch_index: Some(child.batch_index),
                        instruction: decoded,
                    }),
                    Err(ClassicTokenDecodeError::AllocationFailed { requested }) => {
                        return Err(TargetMintTrackerError::AllocationFailed {
                            buffer: TokenTrackerBuffer::ImmutableOwnerLookahead,
                            requested,
                        });
                    }
                    Err(_) => leaves.push(LookaheadLeaf::Barrier(child.accounts)),
                }
            }
            if has_terminal_error {
                leaves.push(LookaheadLeaf::Barrier(copy_tracker_slice(
                    &instruction.accounts,
                    TokenTrackerBuffer::ImmutableOwnerLookahead,
                )?));
            }
        } else {
            match decode_classic_token_instruction(&instruction.accounts, &instruction.data) {
                Ok(decoded) => leaves.push(LookaheadLeaf::Decoded {
                    order: instruction.coordinate.order,
                    batch_index: None,
                    instruction: decoded,
                }),
                Err(ClassicTokenDecodeError::AllocationFailed { requested }) => {
                    return Err(TargetMintTrackerError::AllocationFailed {
                        buffer: TokenTrackerBuffer::ImmutableOwnerLookahead,
                        requested,
                    });
                }
                Err(_) => leaves.push(LookaheadLeaf::Barrier(copy_tracker_slice(
                    &instruction.accounts,
                    TokenTrackerBuffer::ImmutableOwnerLookahead,
                )?)),
            }
        }
    }

    let mut later_target_initializations = BTreeSet::new();
    let mut hints = BTreeSet::new();
    for leaf in leaves.into_iter().rev() {
        let LookaheadLeaf::Decoded {
            order,
            batch_index,
            instruction: decoded,
        } = leaf
        else {
            let LookaheadLeaf::Barrier(accounts) = leaf else {
                unreachable!("the look-ahead leaf variants are exhaustive");
            };
            for account in accounts {
                later_target_initializations.remove(&account);
            }
            continue;
        };
        match decoded.instruction {
            ClassicTokenInstruction::InitializeAccount
            | ClassicTokenInstruction::InitializeAccount2 { .. }
            | ClassicTokenInstruction::InitializeAccount3 { .. } => {
                if let (Some(account), Some(mint)) = (
                    decoded.account(TokenAccountRole::TokenAccount),
                    decoded.account(TokenAccountRole::Mint),
                ) {
                    if mint == target_mint {
                        later_target_initializations.insert(account);
                    } else {
                        later_target_initializations.remove(&account);
                    }
                }
            }
            ClassicTokenInstruction::CloseAccount => {
                if let Some(account) = decoded.account(TokenAccountRole::TokenAccount) {
                    later_target_initializations.remove(&account);
                }
            }
            ClassicTokenInstruction::InitializeImmutableOwner
                if decoded
                    .account(TokenAccountRole::TokenAccount)
                    .is_some_and(|account| later_target_initializations.contains(&account)) =>
            {
                hints.insert((order, batch_index));
            }
            _ => {}
        }
    }
    Ok(hints)
}

#[allow(clippy::too_many_arguments)]
fn process_exact_leaf(
    accounts: &mut AccountOverlay<'_>,
    target_mint: PubkeyBytes,
    history: &mut HistoryCoverage,
    certainty_revision: &mut u64,
    coordinate: crate::InstructionCoordinate,
    batch_index: Option<u32>,
    program_id: PubkeyBytes,
    instruction_accounts: &[PubkeyBytes],
    data: &[u8],
    target_hint: bool,
    transaction_header: TransactionHeader,
    state_can_change_history: bool,
    events: &mut Vec<TrackedTokenEvent>,
    coverage_issues: &mut Vec<TokenCoverageIssue>,
) -> Result<(), TargetMintTrackerError> {
    let raw = match decode_classic_token_instruction(instruction_accounts, data) {
        Ok(raw) => raw,
        Err(ClassicTokenDecodeError::AllocationFailed { requested }) => {
            return Err(TargetMintTrackerError::AllocationFailed {
                buffer: TokenTrackerBuffer::InstructionDecode,
                requested,
            });
        }
        Err(error) => {
            let touches_target =
                touches_current_target(accounts, target_mint, instruction_accounts);
            if touches_target || state_can_change_history {
                coverage_issues.push(TokenCoverageIssue {
                    coordinate: Some(coordinate),
                    kind: TokenCoverageIssueKind::Decode(error),
                });
                if touches_target {
                    events.push(raw_unknown_event_parts(
                        coordinate,
                        batch_index,
                        transaction_header,
                        program_id,
                        instruction_accounts,
                        InstructionDataCoverage::Exact,
                        data,
                    )?);
                }
                if state_can_change_history {
                    mark_history_partial(history, certainty_revision)?;
                }
            }
            return Ok(());
        }
    };

    let (commit, invocation) = event_evidence(transaction_header, coordinate, batch_index);
    let touches_target_before = target_hint || decoded_touches_target(accounts, target_mint, &raw);
    if commit != TokenCommitState::Committed {
        if touches_target_before {
            events.push(TrackedTokenEvent {
                coordinate,
                batch_index,
                commit,
                invocation,
                raw: ObservedTokenInstruction::Classic(raw),
                effects: Vec::new(),
            });
        }
        return Ok(());
    }
    let issue_count_before = coverage_issues.len();
    let mut effects = Vec::new();
    reserve_tracker_buffer(
        &mut effects,
        MAX_TOKEN_EFFECTS_PER_EVENT,
        TokenTrackerBuffer::EventEffects,
    )?;
    apply_instruction(
        accounts,
        target_mint,
        history,
        certainty_revision,
        coordinate,
        &raw,
        &mut effects,
        coverage_issues,
    )?;
    debug_assert!(effects.len() <= MAX_TOKEN_EFFECTS_PER_EVENT);
    if touches_target_before || !effects.is_empty() || coverage_issues.len() != issue_count_before {
        events.push(TrackedTokenEvent {
            coordinate,
            batch_index,
            commit,
            invocation,
            raw: ObservedTokenInstruction::Classic(raw),
            effects,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_instruction(
    accounts: &mut AccountOverlay<'_>,
    target_mint: PubkeyBytes,
    history: &mut HistoryCoverage,
    certainty_revision: &mut u64,
    coordinate: crate::InstructionCoordinate,
    raw: &DecodedClassicTokenInstruction,
    effects: &mut Vec<TargetMintEffect>,
    issues: &mut Vec<TokenCoverageIssue>,
) -> Result<(), TargetMintTrackerError> {
    match &raw.instruction {
        ClassicTokenInstruction::InitializeAccount
        | ClassicTokenInstruction::InitializeAccount2 { .. }
        | ClassicTokenInstruction::InitializeAccount3 { .. } => {
            if let (Some(account), Some(mint)) = (
                raw.account(TokenAccountRole::TokenAccount),
                raw.account(TokenAccountRole::Mint),
            ) {
                observe_account_mint(
                    accounts,
                    target_mint,
                    account,
                    mint,
                    LifecycleCause::InitializeAccount,
                    true,
                    history,
                    certainty_revision,
                    coordinate,
                    effects,
                    issues,
                )?;
            }
        }
        ClassicTokenInstruction::Transfer { amount } => {
            if let (Some(source), Some(destination)) = (
                raw.account(TokenAccountRole::Source),
                raw.account(TokenAccountRole::Destination),
            ) {
                apply_unchecked_transfer(
                    accounts,
                    target_mint,
                    source,
                    destination,
                    *amount,
                    history,
                    certainty_revision,
                    coordinate,
                    effects,
                    issues,
                )?;
            }
        }
        ClassicTokenInstruction::TransferChecked { amount, decimals } => {
            if let (Some(source), Some(mint), Some(destination)) = (
                raw.account(TokenAccountRole::Source),
                raw.account(TokenAccountRole::Mint),
                raw.account(TokenAccountRole::Destination),
            ) {
                observe_account_mint(
                    accounts,
                    target_mint,
                    source,
                    mint,
                    LifecycleCause::CheckedTransfer,
                    false,
                    history,
                    certainty_revision,
                    coordinate,
                    effects,
                    issues,
                )?;
                if destination != source {
                    observe_account_mint(
                        accounts,
                        target_mint,
                        destination,
                        mint,
                        LifecycleCause::CheckedTransfer,
                        false,
                        history,
                        certainty_revision,
                        coordinate,
                        effects,
                        issues,
                    )?;
                }
                refresh_confirmation(accounts, source, *certainty_revision);
                if destination != source {
                    refresh_confirmation(accounts, destination, *certainty_revision);
                }
                if mint == target_mint {
                    let source_generation = account_generation(accounts, source);
                    let destination_generation = account_generation(accounts, destination);
                    effects.push(TargetMintEffect::Transfer(target_transfer(
                        source,
                        source_generation,
                        destination,
                        destination_generation,
                        *amount,
                        Some(*decimals),
                        true,
                    )));
                }
            }
        }
        ClassicTokenInstruction::MintTo { amount }
        | ClassicTokenInstruction::MintToChecked { amount, .. } => {
            if let (Some(mint), Some(destination)) = (
                raw.account(TokenAccountRole::Mint),
                raw.account(TokenAccountRole::Destination),
            ) {
                observe_account_mint(
                    accounts,
                    target_mint,
                    destination,
                    mint,
                    LifecycleCause::ExplicitMintInstruction,
                    false,
                    history,
                    certainty_revision,
                    coordinate,
                    effects,
                    issues,
                )?;
                if mint == target_mint {
                    effects.push(TargetMintEffect::Mint {
                        account: destination,
                        generation: account_generation(accounts, destination),
                        amount: *amount,
                        decimals: raw.instruction.decimals(),
                    });
                }
            }
        }
        ClassicTokenInstruction::Burn { amount }
        | ClassicTokenInstruction::BurnChecked { amount, .. } => {
            if let (Some(account), Some(mint)) = (
                raw.account(TokenAccountRole::Source),
                raw.account(TokenAccountRole::Mint),
            ) {
                observe_account_mint(
                    accounts,
                    target_mint,
                    account,
                    mint,
                    LifecycleCause::ExplicitMintInstruction,
                    false,
                    history,
                    certainty_revision,
                    coordinate,
                    effects,
                    issues,
                )?;
                if mint == target_mint {
                    effects.push(TargetMintEffect::Burn {
                        account,
                        generation: account_generation(accounts, account),
                        amount: *amount,
                        decimals: raw.instruction.decimals(),
                    });
                }
            }
        }
        ClassicTokenInstruction::ApproveChecked { .. } => {
            apply_explicit_account_mint(
                accounts,
                target_mint,
                raw,
                TokenAccountRole::Source,
                history,
                certainty_revision,
                coordinate,
                effects,
                issues,
            )?;
        }
        ClassicTokenInstruction::FreezeAccount | ClassicTokenInstruction::ThawAccount => {
            apply_explicit_account_mint(
                accounts,
                target_mint,
                raw,
                TokenAccountRole::TokenAccount,
                history,
                certainty_revision,
                coordinate,
                effects,
                issues,
            )?;
        }
        ClassicTokenInstruction::CloseAccount => {
            if let Some(account) = raw.account(TokenAccountRole::TokenAccount) {
                close_account(
                    accounts,
                    target_mint,
                    account,
                    history,
                    certainty_revision,
                    coordinate,
                    effects,
                    issues,
                )?;
            }
        }
        ClassicTokenInstruction::SyncNative => {
            if let Some(account) = raw.account(TokenAccountRole::TokenAccount)
                && accounts
                    .get(&account)
                    .is_some_and(|state| state.lifecycle.state == TokenAccountState::ActiveTarget)
            {
                issues.push(TokenCoverageIssue {
                    coordinate: Some(coordinate),
                    kind: TokenCoverageIssueKind::SyncNativeOnTargetAccount { account },
                });
                mark_history_partial(history, certainty_revision)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_explicit_account_mint(
    accounts: &mut AccountOverlay<'_>,
    target_mint: PubkeyBytes,
    raw: &DecodedClassicTokenInstruction,
    account_role: TokenAccountRole,
    history: &mut HistoryCoverage,
    certainty_revision: &mut u64,
    coordinate: crate::InstructionCoordinate,
    effects: &mut Vec<TargetMintEffect>,
    issues: &mut Vec<TokenCoverageIssue>,
) -> Result<(), TargetMintTrackerError> {
    if let (Some(account), Some(mint)) = (
        raw.account(account_role),
        raw.account(TokenAccountRole::Mint),
    ) {
        observe_account_mint(
            accounts,
            target_mint,
            account,
            mint,
            LifecycleCause::ExplicitMintInstruction,
            false,
            history,
            certainty_revision,
            coordinate,
            effects,
            issues,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn observe_account_mint(
    accounts: &mut AccountOverlay<'_>,
    target_mint: PubkeyBytes,
    account: PubkeyBytes,
    observed_mint: PubkeyBytes,
    cause: LifecycleCause,
    starts_new_lifetime: bool,
    history: &mut HistoryCoverage,
    certainty_revision: &mut u64,
    coordinate: crate::InstructionCoordinate,
    effects: &mut Vec<TargetMintEffect>,
    issues: &mut Vec<TokenCoverageIssue>,
) -> Result<(), TargetMintTrackerError> {
    let before_record = accounts.get(&account);
    let before = before_record.map(|record| record.lifecycle);
    if observed_mint != target_mint && before_record.is_none() {
        // The target tracker does not retain unrelated token accounts.
        return Ok(());
    }

    if let Some(previous_record) = before_record {
        let previous = previous_record.lifecycle;
        let previous_is_exact = previous_record.confirmed_revision == *certainty_revision;
        if starts_new_lifetime && !matches!(previous.state, TokenAccountState::Closed { .. }) {
            push_insufficient_history(issues, coordinate, account, None);
            mark_history_partial(history, certainty_revision)?;
        }
        if previous_is_exact
            && let Some(known_mint) = previous.state.active_mint(target_mint)
            && known_mint != observed_mint
        {
            issues.push(TokenCoverageIssue {
                coordinate: Some(coordinate),
                kind: TokenCoverageIssueKind::ConflictingMintEvidence {
                    account,
                    known_mint,
                    observed_mint,
                },
            });
            mark_history_partial(history, certainty_revision)?;
        }
        let needs_history_issue = (matches!(previous.state, TokenAccountState::Closed { .. })
            && !starts_new_lifetime)
            || !previous_is_exact;
        if needs_history_issue {
            push_insufficient_history(issues, coordinate, account, None);
            mark_history_partial(history, certainty_revision)?;
        }
    }

    let generation = match before {
        None => 1,
        Some(previous)
            if starts_new_lifetime
                || matches!(previous.state, TokenAccountState::Closed { .. }) =>
        {
            previous
                .generation
                .checked_add(1)
                .ok_or(TargetMintTrackerError::LifecycleGenerationExhausted { account })?
        }
        Some(previous)
            if before_record
                .is_some_and(|record| record.confirmed_revision != *certainty_revision) =>
        {
            previous
                .generation
                .checked_add(1)
                .ok_or(TargetMintTrackerError::LifecycleGenerationExhausted { account })?
        }
        Some(previous) => previous.generation,
    };
    let state = if observed_mint == target_mint {
        TokenAccountState::ActiveTarget
    } else {
        TokenAccountState::ActiveOther {
            mint: observed_mint,
        }
    };
    let after = TokenAccountLifecycle { generation, state };
    accounts.insert(
        account,
        TargetAccountSnapshot {
            lifecycle: after,
            confirmed_revision: *certainty_revision,
        },
    );
    if before != Some(after) {
        effects.push(TargetMintEffect::Lifecycle(AccountLifecycleChange {
            account,
            before,
            after,
            cause,
        }));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_unchecked_transfer(
    accounts: &mut AccountOverlay<'_>,
    target_mint: PubkeyBytes,
    source: PubkeyBytes,
    destination: PubkeyBytes,
    amount: u64,
    history: &mut HistoryCoverage,
    certainty_revision: &mut u64,
    coordinate: crate::InstructionCoordinate,
    effects: &mut Vec<TargetMintEffect>,
    issues: &mut Vec<TokenCoverageIssue>,
) -> Result<(), TargetMintTrackerError> {
    let source_record = accounts.get(&source);
    let destination_record = accounts.get(&destination);
    let observed_source_mint =
        source_record.and_then(|record| record.lifecycle.state.active_mint(target_mint));
    let observed_destination_mint =
        destination_record.and_then(|record| record.lifecycle.state.active_mint(target_mint));
    let source_mint = source_record
        .filter(|record| record.confirmed_revision == *certainty_revision)
        .and_then(|record| record.lifecycle.state.active_mint(target_mint));
    let destination_mint = destination_record
        .filter(|record| record.confirmed_revision == *certainty_revision)
        .and_then(|record| record.lifecycle.state.active_mint(target_mint));
    let source_is_closed = source_record
        .is_some_and(|record| matches!(record.lifecycle.state, TokenAccountState::Closed { .. }));
    let destination_is_closed = destination_record
        .is_some_and(|record| matches!(record.lifecycle.state, TokenAccountState::Closed { .. }));

    if source_mint == Some(target_mint) || destination_mint == Some(target_mint) {
        observe_account_mint(
            accounts,
            target_mint,
            source,
            target_mint,
            LifecycleCause::UncheckedTransfer,
            false,
            history,
            certainty_revision,
            coordinate,
            effects,
            issues,
        )?;
        if destination != source {
            observe_account_mint(
                accounts,
                target_mint,
                destination,
                target_mint,
                LifecycleCause::UncheckedTransfer,
                false,
                history,
                certainty_revision,
                coordinate,
                effects,
                issues,
            )?;
        }
        refresh_confirmation(accounts, source, *certainty_revision);
        if destination != source {
            refresh_confirmation(accounts, destination, *certainty_revision);
        }
        let source_generation = account_generation(accounts, source);
        let destination_generation = account_generation(accounts, destination);
        effects.push(TargetMintEffect::Transfer(target_transfer(
            source,
            source_generation,
            destination,
            destination_generation,
            amount,
            None,
            false,
        )));
        return Ok(());
    }

    let known_other = source_mint.or(destination_mint);
    if let (Some(left), Some(right)) = (source_mint, destination_mint)
        && left != right
    {
        issues.push(TokenCoverageIssue {
            coordinate: Some(coordinate),
            kind: TokenCoverageIssueKind::ConflictingMintEvidence {
                account: destination,
                known_mint: right,
                observed_mint: left,
            },
        });
        mark_history_partial(history, certainty_revision)?;
        return Ok(());
    }
    if let Some(other_mint) = known_other {
        if accounts.contains_key(&source) {
            observe_account_mint(
                accounts,
                target_mint,
                source,
                other_mint,
                LifecycleCause::UncheckedTransfer,
                false,
                history,
                certainty_revision,
                coordinate,
                effects,
                issues,
            )?;
        }
        if destination != source && accounts.contains_key(&destination) {
            observe_account_mint(
                accounts,
                target_mint,
                destination,
                other_mint,
                LifecycleCause::UncheckedTransfer,
                false,
                history,
                certainty_revision,
                coordinate,
                effects,
                issues,
            )?;
        }
        return Ok(());
    }

    let has_unconfirmed_mint = observed_source_mint.is_some_and(|_| source_mint.is_none())
        || observed_destination_mint.is_some_and(|_| destination_mint.is_none());
    if source_is_closed
        || destination_is_closed
        || has_unconfirmed_mint
        || *history == HistoryCoverage::Partial
    {
        push_insufficient_history(issues, coordinate, source, Some(destination));
        mark_history_partial(history, certainty_revision)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn close_account(
    accounts: &mut AccountOverlay<'_>,
    target_mint: PubkeyBytes,
    account: PubkeyBytes,
    history: &mut HistoryCoverage,
    certainty_revision: &mut u64,
    coordinate: crate::InstructionCoordinate,
    effects: &mut Vec<TargetMintEffect>,
    issues: &mut Vec<TokenCoverageIssue>,
) -> Result<(), TargetMintTrackerError> {
    let Some(before_record) = accounts.get(&account) else {
        if *history == HistoryCoverage::Partial {
            push_insufficient_history(issues, coordinate, account, None);
        }
        return Ok(());
    };
    let before = before_record.lifecycle;
    if before_record.confirmed_revision != *certainty_revision {
        push_insufficient_history(issues, coordinate, account, None);
        mark_history_partial(history, certainty_revision)?;
        return Ok(());
    }
    if matches!(before.state, TokenAccountState::Closed { .. }) {
        return Ok(());
    }
    let after = TokenAccountLifecycle {
        generation: before.generation,
        state: TokenAccountState::Closed {
            last_mint: before.state.active_mint(target_mint),
        },
    };
    accounts.insert(
        account,
        TargetAccountSnapshot {
            lifecycle: after,
            confirmed_revision: *certainty_revision,
        },
    );
    effects.push(TargetMintEffect::Lifecycle(AccountLifecycleChange {
        account,
        before: Some(before),
        after,
        cause: LifecycleCause::CloseAccount,
    }));
    Ok(())
}

fn target_transfer(
    source: PubkeyBytes,
    source_generation: u64,
    destination: PubkeyBytes,
    destination_generation: u64,
    amount: u64,
    decimals: Option<u8>,
    checked: bool,
) -> TargetTransfer {
    TargetTransfer {
        source,
        destination,
        amount,
        decimals,
        checked,
        legs: [
            TransferLeg {
                role: TransferLegRole::Source,
                account: source,
                generation: source_generation,
                direction: BalanceDirection::Debit,
                amount,
            },
            TransferLeg {
                role: TransferLegRole::Destination,
                account: destination,
                generation: destination_generation,
                direction: BalanceDirection::Credit,
                amount,
            },
        ],
    }
}

fn account_generation(accounts: &AccountOverlay<'_>, account: PubkeyBytes) -> u64 {
    accounts
        .get(&account)
        .map_or(1, |state| state.lifecycle.generation)
}

fn refresh_confirmation(
    accounts: &mut AccountOverlay<'_>,
    account: PubkeyBytes,
    certainty_revision: u64,
) {
    if let Some(mut state) = accounts.get(&account) {
        state.confirmed_revision = certainty_revision;
        accounts.insert(account, state);
    }
}

fn mark_history_partial(
    history: &mut HistoryCoverage,
    certainty_revision: &mut u64,
) -> Result<(), TargetMintTrackerError> {
    let next_revision = certainty_revision
        .checked_add(1)
        .ok_or(TargetMintTrackerError::CertaintyRevisionExhausted)?;
    *history = HistoryCoverage::Partial;
    *certainty_revision = next_revision;
    Ok(())
}

fn push_insufficient_history(
    issues: &mut Vec<TokenCoverageIssue>,
    coordinate: crate::InstructionCoordinate,
    first_account: PubkeyBytes,
    second_account: Option<PubkeyBytes>,
) {
    issues.push(TokenCoverageIssue {
        coordinate: Some(coordinate),
        kind: TokenCoverageIssueKind::InsufficientHistory {
            first_account,
            second_account,
        },
    });
}

fn touches_current_target(
    accounts: &AccountOverlay<'_>,
    target_mint: PubkeyBytes,
    instruction_accounts: &[PubkeyBytes],
) -> bool {
    instruction_accounts.iter().any(|account| {
        *account == target_mint
            || accounts
                .get(account)
                .is_some_and(|state| state.lifecycle.state == TokenAccountState::ActiveTarget)
    })
}

fn decoded_touches_target(
    accounts: &AccountOverlay<'_>,
    target_mint: PubkeyBytes,
    instruction: &DecodedClassicTokenInstruction,
) -> bool {
    instruction.roles.iter().any(|binding| match binding.role {
        TokenAccountRole::Mint => binding.address == target_mint,
        TokenAccountRole::TokenAccount
        | TokenAccountRole::Source
        | TokenAccountRole::Destination
        | TokenAccountRole::AuthoritySubject => {
            binding.address == target_mint
                || accounts
                    .get(&binding.address)
                    .is_some_and(|state| state.lifecycle.state == TokenAccountState::ActiveTarget)
        }
        TokenAccountRole::MultisigAccount
        | TokenAccountRole::LamportDestination
        | TokenAccountRole::Owner
        | TokenAccountRole::Delegate
        | TokenAccountRole::Authority
        | TokenAccountRole::RentSysvar
        | TokenAccountRole::MultisigSigner
        | TokenAccountRole::Additional => false,
    })
}

fn raw_unknown_event(
    instruction: &crate::ResolvedInstruction,
    transaction_header: TransactionHeader,
) -> Result<TrackedTokenEvent, TargetMintTrackerError> {
    raw_unknown_event_parts(
        instruction.coordinate,
        None,
        transaction_header,
        instruction
            .program_id
            .ok_or(TargetMintTrackerError::ProgramIdentityNotRequested)?,
        &instruction.accounts,
        instruction.data_coverage,
        &instruction.data,
    )
}

#[allow(clippy::too_many_arguments)]
fn raw_unknown_event_parts(
    coordinate: crate::InstructionCoordinate,
    batch_index: Option<u32>,
    transaction_header: TransactionHeader,
    program_id: PubkeyBytes,
    accounts: &[PubkeyBytes],
    data_coverage: InstructionDataCoverage,
    data: &[u8],
) -> Result<TrackedTokenEvent, TargetMintTrackerError> {
    let (commit, invocation) = event_evidence(transaction_header, coordinate, batch_index);
    Ok(TrackedTokenEvent {
        coordinate,
        batch_index,
        commit,
        invocation,
        raw: ObservedTokenInstruction::Unknown(RawUnknownTokenInstruction {
            program_id,
            accounts: copy_tracker_slice(accounts, TokenTrackerBuffer::RawEvent)?,
            data_coverage,
            data: copy_tracker_slice(data, TokenTrackerBuffer::RawEvent)?,
        }),
        effects: Vec::new(),
    })
}

fn event_evidence(
    transaction_header: TransactionHeader,
    coordinate: crate::InstructionCoordinate,
    batch_index: Option<u32>,
) -> (TokenCommitState, TokenInvocationEvidence) {
    match transaction_header.status {
        ExecutionStatus::Succeeded => (
            TokenCommitState::Committed,
            TokenInvocationEvidence::Invoked,
        ),
        ExecutionStatus::Failed => match transaction_header.failed_outer_instruction_index {
            Some(failed_outer) if coordinate.outer_index < failed_outer => (
                TokenCommitState::RolledBack,
                TokenInvocationEvidence::Invoked,
            ),
            Some(failed_outer) if coordinate.outer_index > failed_outer => (
                TokenCommitState::NotCommitted,
                TokenInvocationEvidence::NotInvoked,
            ),
            Some(_) if batch_index.is_some() => (
                TokenCommitState::NotCommitted,
                TokenInvocationEvidence::Unknown,
            ),
            Some(_) => (
                TokenCommitState::RolledBack,
                TokenInvocationEvidence::Invoked,
            ),
            None if batch_index.is_some() => (
                TokenCommitState::NotCommitted,
                TokenInvocationEvidence::Unknown,
            ),
            None if coordinate.inner_index.is_some() => (
                TokenCommitState::RolledBack,
                TokenInvocationEvidence::Invoked,
            ),
            None => (
                TokenCommitState::NotCommitted,
                TokenInvocationEvidence::Unknown,
            ),
        },
        ExecutionStatus::Unknown(_)
            if batch_index.is_none() && coordinate.inner_index.is_some() =>
        {
            (TokenCommitState::Unknown, TokenInvocationEvidence::Invoked)
        }
        ExecutionStatus::Unknown(_) => {
            (TokenCommitState::Unknown, TokenInvocationEvidence::Unknown)
        }
    }
}
