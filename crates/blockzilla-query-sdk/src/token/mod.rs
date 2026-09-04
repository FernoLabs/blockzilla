//! Classic SPL Token instruction facts and target-mint tracking.
//!
//! This module uses resolved public keys. It does not read an archive format.
//! It does not use pre-token or post-token balances.

use std::collections::BTreeMap;

mod batch;
mod decode;
mod tracker;

pub use batch::decode_classic_token_batch;
pub use decode::decode_classic_token_instruction;
pub use tracker::TargetMintTracker;

use crate::{
    BlockHeader, CoverageReason, InstructionCoordinate, InstructionDataCoverage,
    ResolvedInstruction, TransactionView,
};

/// A public key in its source-neutral byte form.
pub type PubkeyBytes = [u8; 32];

/// The classic SPL Token program.
pub const CLASSIC_SPL_TOKEN_PROGRAM_ID: PubkeyBytes = [
    6, 221, 246, 225, 215, 101, 161, 147, 217, 203, 225, 70, 206, 235, 121, 172, 28, 180, 133, 237,
    95, 91, 55, 145, 58, 140, 245, 133, 126, 255, 0, 169,
];

/// Maximum instruction data size in the signed-message source geometry.
pub const MAX_TOKEN_INSTRUCTION_DATA_BYTES: usize = crate::MAX_CANONICAL_SHORT_VEC_ITEMS;

/// Maximum resolved accounts in the signed-message source geometry.
pub const MAX_TOKEN_INSTRUCTION_ACCOUNTS: usize = crate::MAX_CANONICAL_SHORT_VEC_ITEMS;

/// Maximum classic Token leaves after Batch expansion in one transaction.
///
/// This limit equals the canonical Solana short-vector item limit. It bounds
/// work and output allocation when many Batch instructions are present.
pub const MAX_EXPANDED_TOKEN_LEAVES: usize = crate::MAX_CANONICAL_SHORT_VEC_ITEMS;

/// Maximum committed account records from one tracked transaction.
///
/// One classic Token leaf can update at most a source and a destination.
pub const MAX_TOKEN_ACCOUNT_UPDATES_PER_TRANSACTION: usize = MAX_EXPANDED_TOKEN_LEAVES * 2;

/// Maximum classic Token input payload inspected in one transaction.
///
/// The payload is instruction data plus 32 bytes for each account reference.
/// This resource limit bounds event and look-ahead copies. It is larger than
/// a real Solana transaction but also protects direct `TransactionView` use.
pub const MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION: usize = 16 * 1024 * 1024;

/// Maximum token coverage issues from one direct transaction view.
///
/// This value includes one order issue for each direct instruction, four
/// issues for each expanded token leaf, and four transaction-level issues.
pub const MAX_TOKEN_COVERAGE_ISSUES_PER_TRANSACTION: usize =
    crate::MAX_CANONICAL_SHORT_VEC_ITEMS + 4 * MAX_EXPANDED_TOKEN_LEAVES + 4;

/// The authority type in a SetAuthority instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenAuthorityType {
    MintTokens,
    FreezeAccount,
    AccountOwner,
    CloseAccount,
}

/// The data in one classic SPL Token instruction.
///
/// The variants match the classic SPL Token 3.0 instruction tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassicTokenInstruction {
    InitializeMint {
        decimals: u8,
        mint_authority: PubkeyBytes,
        freeze_authority: Option<PubkeyBytes>,
    },
    InitializeAccount,
    InitializeMultisig {
        required_signers: u8,
    },
    Transfer {
        amount: u64,
    },
    Approve {
        amount: u64,
    },
    Revoke,
    SetAuthority {
        authority_type: TokenAuthorityType,
        new_authority: Option<PubkeyBytes>,
    },
    MintTo {
        amount: u64,
    },
    Burn {
        amount: u64,
    },
    CloseAccount,
    FreezeAccount,
    ThawAccount,
    TransferChecked {
        amount: u64,
        decimals: u8,
    },
    ApproveChecked {
        amount: u64,
        decimals: u8,
    },
    MintToChecked {
        amount: u64,
        decimals: u8,
    },
    BurnChecked {
        amount: u64,
        decimals: u8,
    },
    InitializeAccount2 {
        owner: PubkeyBytes,
    },
    SyncNative,
    InitializeAccount3 {
        owner: PubkeyBytes,
    },
    InitializeMultisig2 {
        required_signers: u8,
    },
    InitializeMint2 {
        decimals: u8,
        mint_authority: PubkeyBytes,
        freeze_authority: Option<PubkeyBytes>,
    },
    GetAccountDataSize,
    InitializeImmutableOwner,
    AmountToUiAmount {
        amount: u64,
    },
    UiAmountToAmount {
        ui_amount: String,
    },
    WithdrawExcessLamports,
    UnwrapLamports {
        /// None means all source lamports.
        amount: Option<u64>,
    },
    Batch,
}

impl ClassicTokenInstruction {
    /// Return the stable classic SPL Token tag.
    pub const fn tag(&self) -> u8 {
        match self {
            Self::InitializeMint { .. } => 0,
            Self::InitializeAccount => 1,
            Self::InitializeMultisig { .. } => 2,
            Self::Transfer { .. } => 3,
            Self::Approve { .. } => 4,
            Self::Revoke => 5,
            Self::SetAuthority { .. } => 6,
            Self::MintTo { .. } => 7,
            Self::Burn { .. } => 8,
            Self::CloseAccount => 9,
            Self::FreezeAccount => 10,
            Self::ThawAccount => 11,
            Self::TransferChecked { .. } => 12,
            Self::ApproveChecked { .. } => 13,
            Self::MintToChecked { .. } => 14,
            Self::BurnChecked { .. } => 15,
            Self::InitializeAccount2 { .. } => 16,
            Self::SyncNative => 17,
            Self::InitializeAccount3 { .. } => 18,
            Self::InitializeMultisig2 { .. } => 19,
            Self::InitializeMint2 { .. } => 20,
            Self::GetAccountDataSize => 21,
            Self::InitializeImmutableOwner => 22,
            Self::AmountToUiAmount { .. } => 23,
            Self::UiAmountToAmount { .. } => 24,
            Self::WithdrawExcessLamports => 38,
            Self::UnwrapLamports { .. } => 45,
            Self::Batch => 255,
        }
    }

    /// Return the optional lamport amount in an UnwrapLamports instruction.
    ///
    /// The outer option identifies the instruction. The inner option is the
    /// COption value from the wire.
    pub const fn unwrap_lamport_amount(&self) -> Option<Option<u64>> {
        match self {
            Self::UnwrapLamports { amount } => Some(*amount),
            _ => None,
        }
    }

    /// Return the raw token amount when the instruction contains one.
    pub const fn amount(&self) -> Option<u64> {
        match self {
            Self::Transfer { amount }
            | Self::Approve { amount }
            | Self::MintTo { amount }
            | Self::Burn { amount }
            | Self::TransferChecked { amount, .. }
            | Self::ApproveChecked { amount, .. }
            | Self::MintToChecked { amount, .. }
            | Self::BurnChecked { amount, .. }
            | Self::AmountToUiAmount { amount } => Some(*amount),
            _ => None,
        }
    }

    /// Return the decimal count when the instruction contains one.
    pub const fn decimals(&self) -> Option<u8> {
        match self {
            Self::InitializeMint { decimals, .. }
            | Self::TransferChecked { decimals, .. }
            | Self::ApproveChecked { decimals, .. }
            | Self::MintToChecked { decimals, .. }
            | Self::BurnChecked { decimals, .. }
            | Self::InitializeMint2 { decimals, .. } => Some(*decimals),
            _ => None,
        }
    }
}

/// One role of an account in a classic SPL Token instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenAccountRole {
    Mint,
    TokenAccount,
    MultisigAccount,
    Source,
    Destination,
    LamportDestination,
    Owner,
    Delegate,
    Authority,
    AuthoritySubject,
    RentSysvar,
    MultisigSigner,
    Additional,
}

/// One resolved instruction account and its role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenAccountRoleBinding {
    pub account_index: u32,
    pub address: PubkeyBytes,
    pub role: TokenAccountRole,
}

/// A fully decoded classic SPL Token instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedClassicTokenInstruction {
    pub instruction: ClassicTokenInstruction,
    pub roles: Vec<TokenAccountRoleBinding>,
    /// Bytes that the classic decoder accepts but does not use.
    pub trailing_data: Vec<u8>,
}

impl DecodedClassicTokenInstruction {
    /// Return the first account with the selected role.
    pub fn account(&self, role: TokenAccountRole) -> Option<PubkeyBytes> {
        self.roles
            .iter()
            .find(|binding| binding.role == role)
            .map(|binding| binding.address)
    }
}

/// Exact source facts for a possible target instruction that is not decoded.
///
/// Account membership is conservative evidence. This value does not prove
/// that the instruction has a semantic target-mint role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawUnknownTokenInstruction {
    pub program_id: PubkeyBytes,
    pub accounts: Vec<PubkeyBytes>,
    pub data_coverage: InstructionDataCoverage,
    pub data: Vec<u8>,
}

/// One structured classic instruction or one raw unknown instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedTokenInstruction {
    Classic(DecodedClassicTokenInstruction),
    Unknown(RawUnknownTokenInstruction),
}

impl ObservedTokenInstruction {
    /// Return the structured instruction when the classic decoder knows it.
    pub const fn classic(&self) -> Option<&DecodedClassicTokenInstruction> {
        match self {
            Self::Classic(instruction) => Some(instruction),
            Self::Unknown(_) => None,
        }
    }
}

/// A structural error in one classic SPL Token instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassicTokenDecodeError {
    AllocationFailed {
        requested: usize,
    },
    InstructionDataLimit {
        limit: usize,
        actual: usize,
    },
    InstructionAccountLimit {
        limit: usize,
        actual: usize,
    },
    EmptyData,
    UnknownTag {
        tag: u8,
    },
    TruncatedData {
        tag: u8,
        needed: usize,
        actual: usize,
    },
    InvalidOptionalPubkeyTag {
        tag: u8,
        value: u8,
    },
    InvalidAuthorityType {
        value: u8,
    },
    InvalidUiAmountUtf8,
    InsufficientAccounts {
        tag: u8,
        needed: usize,
        actual: usize,
    },
    TooManyAccounts,
    InvalidOptionalU64Tag {
        tag: u8,
        value: u8,
    },
    NotBatch,
    EmptyBatch,
    TruncatedBatchHeader {
        batch_index: u32,
    },
    EmptyBatchChildData {
        batch_index: u32,
    },
    BatchDataOverrun {
        batch_index: u32,
        declared: usize,
        available: usize,
    },
    BatchAccountOverrun {
        batch_index: u32,
        declared: usize,
        available: usize,
    },
    NestedBatch {
        batch_index: u32,
    },
}

/// One child geometry from a Batch instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicTokenBatchChild {
    pub batch_index: u32,
    pub accounts: Vec<PubkeyBytes>,
    pub data: Vec<u8>,
}

/// One decoded Batch prefix and an optional terminal geometry error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedClassicTokenBatch {
    pub children: Vec<ClassicTokenBatchChild>,
    /// A terminal error after the valid child prefix.
    pub terminal_error: Option<ClassicTokenDecodeError>,
    /// Number of parent accounts consumed by the valid child prefix.
    pub consumed_account_count: usize,
}

/// The trust state of the tracker history before the current position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryCoverage {
    Complete,
    Partial,
}

/// The current state of one target-related token-account lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenAccountState {
    ActiveTarget,
    ActiveOther { mint: PubkeyBytes },
    Closed { last_mint: Option<PubkeyBytes> },
}

impl TokenAccountState {
    pub const fn active_mint(self, target_mint: PubkeyBytes) -> Option<PubkeyBytes> {
        match self {
            Self::ActiveTarget => Some(target_mint),
            Self::ActiveOther { mint } => Some(mint),
            Self::Closed { .. } => None,
        }
    }
}

/// One target-related account lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenAccountLifecycle {
    /// The first observed lifetime has generation 1.
    pub generation: u64,
    pub state: TokenAccountState,
}

/// One retained account record in a tracker checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetAccountSnapshot {
    pub lifecycle: TokenAccountLifecycle,
    /// This account is exact when this value equals the tracker revision.
    pub confirmed_revision: u64,
}

/// One final committed account record from a processed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetAccountUpdate {
    pub account: PubkeyBytes,
    pub state: TargetAccountSnapshot,
}

/// A complete in-memory checkpoint for one target-mint tracker.
///
/// This value includes closed lifetimes and the certainty state that is needed
/// to continue unchecked-transfer discovery after a history gap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetMintTrackerSnapshot {
    target_mint: PubkeyBytes,
    history: HistoryCoverage,
    accounts: BTreeMap<PubkeyBytes, TargetAccountSnapshot>,
    certainty_revision: u64,
}

/// A structural error in restored target-mint tracker state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetMintSnapshotError {
    ZeroCertaintyRevision,
    DuplicateAccount {
        account: PubkeyBytes,
    },
    ZeroGeneration {
        account: PubkeyBytes,
    },
    ZeroConfirmedRevision {
        account: PubkeyBytes,
    },
    FutureConfirmedRevision {
        account: PubkeyBytes,
        confirmed_revision: u64,
        certainty_revision: u64,
    },
    TargetMintStoredAsOther {
        account: PubkeyBytes,
    },
    InexactAccountInCompleteHistory {
        account: PubkeyBytes,
    },
}

/// One tracker output buffer that could not reserve memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenTrackerBuffer {
    BatchChildren,
    InstructionDecode,
    ImmutableOwnerLookahead,
    Events,
    EventEffects,
    RawEvent,
    CoverageIssues,
    AccountUpdates,
}

/// A tracker input, allocation, or state error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetMintTrackerError {
    ProgramIdentityNotRequested,
    TransactionInstructionLimit {
        limit: usize,
        actual: usize,
    },
    InstructionGeometry {
        coordinate: InstructionCoordinate,
        error: ClassicTokenDecodeError,
    },
    ExpandedTokenLeafLimit {
        limit: usize,
        actual: usize,
    },
    AccountUpdateLimit {
        limit: usize,
        actual: usize,
    },
    TokenInputByteLimit {
        limit: usize,
        actual: usize,
    },
    CompletedBatchHasTerminalError {
        coordinate: InstructionCoordinate,
        error: ClassicTokenDecodeError,
    },
    CompletedBatchChildHasStructuralError {
        coordinate: InstructionCoordinate,
        batch_index: u32,
        error: ClassicTokenDecodeError,
    },
    AllocationFailed {
        buffer: TokenTrackerBuffer,
        requested: usize,
    },
    CertaintyRevisionExhausted,
    LifecycleGenerationExhausted {
        account: PubkeyBytes,
    },
}

impl std::fmt::Display for TargetMintTrackerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProgramIdentityNotRequested => {
                formatter.write_str("token tracking requires instruction program identities")
            }
            Self::TransactionInstructionLimit { limit, actual } => write!(
                formatter,
                "the transaction has {actual} instructions, above the limit {limit}"
            ),
            Self::InstructionGeometry { coordinate, error } => write!(
                formatter,
                "token input geometry is invalid at instruction {}: {error:?}",
                coordinate.order
            ),
            Self::ExpandedTokenLeafLimit { limit, actual } => write!(
                formatter,
                "the transaction expands to {actual} token leaves, above the limit {limit}"
            ),
            Self::AccountUpdateLimit { limit, actual } => write!(
                formatter,
                "the transaction updates {actual} token accounts, above the limit {limit}"
            ),
            Self::TokenInputByteLimit { limit, actual } => write!(
                formatter,
                "the transaction has {actual} token input bytes, above the limit {limit}"
            ),
            Self::CompletedBatchHasTerminalError { coordinate, error } => write!(
                formatter,
                "a completed Batch at instruction {} has a terminal error: {error:?}",
                coordinate.order
            ),
            Self::CompletedBatchChildHasStructuralError {
                coordinate,
                batch_index,
                error,
            } => write!(
                formatter,
                "completed Batch child {batch_index} at instruction {} has a structural error: {error:?}",
                coordinate.order
            ),
            Self::AllocationFailed { buffer, requested } => write!(
                formatter,
                "the {buffer:?} buffer cannot reserve {requested} items"
            ),
            Self::CertaintyRevisionExhausted => {
                formatter.write_str("the token certainty revision is exhausted")
            }
            Self::LifecycleGenerationExhausted { account } => {
                write!(
                    formatter,
                    "the token account lifecycle generation is exhausted for {account:?}"
                )
            }
        }
    }
}

impl std::error::Error for TargetMintTrackerError {}

impl TargetMintTrackerSnapshot {
    pub const fn target_mint(&self) -> PubkeyBytes {
        self.target_mint
    }

    pub const fn history_coverage(&self) -> HistoryCoverage {
        self.history
    }

    pub const fn certainty_revision(&self) -> u64 {
        self.certainty_revision
    }

    /// Return all retained lifetimes, including closed and reused accounts.
    pub fn accounts(&self) -> &BTreeMap<PubkeyBytes, TargetAccountSnapshot> {
        &self.accounts
    }

    /// Return active accounts that have exact mint evidence after the last gap.
    pub fn confirmed_active_accounts(&self) -> impl Iterator<Item = PubkeyBytes> + '_ {
        self.accounts.iter().filter_map(|(account, state)| {
            (state.confirmed_revision == self.certainty_revision
                && state.lifecycle.state == TokenAccountState::ActiveTarget)
                .then_some(*account)
        })
    }

    /// Reconstruct a checked tracker checkpoint from durable records.
    pub fn from_parts(
        target_mint: PubkeyBytes,
        history: HistoryCoverage,
        certainty_revision: u64,
        accounts: impl IntoIterator<Item = (PubkeyBytes, TargetAccountSnapshot)>,
    ) -> Result<Self, TargetMintSnapshotError> {
        if certainty_revision == 0 {
            return Err(TargetMintSnapshotError::ZeroCertaintyRevision);
        }
        let mut checked_accounts = BTreeMap::new();
        for (account, state) in accounts {
            if state.lifecycle.generation == 0 {
                return Err(TargetMintSnapshotError::ZeroGeneration { account });
            }
            if state.confirmed_revision == 0 {
                return Err(TargetMintSnapshotError::ZeroConfirmedRevision { account });
            }
            if state.confirmed_revision > certainty_revision {
                return Err(TargetMintSnapshotError::FutureConfirmedRevision {
                    account,
                    confirmed_revision: state.confirmed_revision,
                    certainty_revision,
                });
            }
            if matches!(
                state.lifecycle.state,
                TokenAccountState::ActiveOther { mint } if mint == target_mint
            ) {
                return Err(TargetMintSnapshotError::TargetMintStoredAsOther { account });
            }
            if history == HistoryCoverage::Complete
                && state.confirmed_revision != certainty_revision
            {
                return Err(TargetMintSnapshotError::InexactAccountInCompleteHistory { account });
            }
            if checked_accounts.insert(account, state).is_some() {
                return Err(TargetMintSnapshotError::DuplicateAccount { account });
            }
        }
        Ok(Self {
            target_mint,
            history,
            accounts: checked_accounts,
            certainty_revision,
        })
    }
}

/// Work that the tracker retained for the last transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenTrackerWork {
    /// Account records stored in the bounded transaction overlay.
    pub overlay_accounts: usize,
}

/// The reason for one lifecycle change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleCause {
    InitializeAccount,
    ExplicitMintInstruction,
    CheckedTransfer,
    UncheckedTransfer,
    CloseAccount,
}

/// One change to a target-related account lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountLifecycleChange {
    pub account: PubkeyBytes,
    pub before: Option<TokenAccountLifecycle>,
    pub after: TokenAccountLifecycle,
    pub cause: LifecycleCause,
}

/// The sign of one instruction-derived balance change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalanceDirection {
    Debit,
    Credit,
}

/// The source or destination role of one transfer leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferLegRole {
    Source,
    Destination,
}

/// One transfer leg. The amount stays unsigned, including `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferLeg {
    pub role: TransferLegRole,
    pub account: PubkeyBytes,
    /// The account lifetime that receives this leg.
    pub generation: u64,
    pub direction: BalanceDirection,
    pub amount: u64,
}

/// One exact transfer for the selected mint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetTransfer {
    pub source: PubkeyBytes,
    pub destination: PubkeyBytes,
    pub amount: u64,
    pub decimals: Option<u8>,
    pub checked: bool,
    pub legs: [TransferLeg; 2],
}

impl TargetTransfer {
    /// Return the net instruction change for one account.
    ///
    /// A self-transfer has two roles and a net change of zero.
    pub fn net_change_for(&self, account: &PubkeyBytes) -> i128 {
        self.legs.iter().fold(0i128, |total, leg| {
            if &leg.account != account {
                return total;
            }
            let amount = i128::from(leg.amount);
            match leg.direction {
                BalanceDirection::Debit => total - amount,
                BalanceDirection::Credit => total + amount,
            }
        })
    }
}

/// One target-mint effect from a decoded instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetMintEffect {
    Lifecycle(AccountLifecycleChange),
    Transfer(TargetTransfer),
    Mint {
        account: PubkeyBytes,
        generation: u64,
        amount: u64,
        decimals: Option<u8>,
    },
    Burn {
        account: PubkeyBytes,
        generation: u64,
        amount: u64,
        decimals: Option<u8>,
    },
}

/// The commit result for one token event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenCommitState {
    /// The transaction succeeded and the effects committed.
    Committed,
    /// The invocation was observed, but the failed transaction rolled it back.
    RolledBack,
    /// The transaction failed, but this model cannot prove invocation.
    NotCommitted,
    /// The source transaction execution state is unknown.
    Unknown,
}

/// Evidence that the runtime invoked one instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenInvocationEvidence {
    /// The source proves that the instruction was invoked.
    Invoked,
    /// The failure boundary proves that the instruction was not invoked.
    NotInvoked,
    /// The current source model cannot prove invocation.
    Unknown,
}

/// One target-related classic Token instruction in source order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedTokenEvent {
    pub coordinate: InstructionCoordinate,
    /// The zero-based Batch child index. This is None for a normal instruction.
    pub batch_index: Option<u32>,
    pub commit: TokenCommitState,
    pub invocation: TokenInvocationEvidence,
    pub raw: ObservedTokenInstruction,
    pub effects: Vec<TargetMintEffect>,
}

/// A token-specific coverage problem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenCoverageIssueKind {
    Decode(ClassicTokenDecodeError),
    InstructionDataUnavailable(InstructionDataCoverage),
    InsufficientHistory {
        first_account: PubkeyBytes,
        second_account: Option<PubkeyBytes>,
    },
    ConflictingMintEvidence {
        account: PubkeyBytes,
        known_mint: PubkeyBytes,
        observed_mint: PubkeyBytes,
    },
    /// SyncNative succeeded on an account that was known for the target mint.
    SyncNativeOnTargetAccount {
        account: PubkeyBytes,
    },
    InvalidInstructionOrder {
        expected: u32,
        actual: u32,
    },
    IncompleteInstructions(CoverageReason),
    IncompleteCpi(CoverageReason),
    CpiNotRecorded,
    UnknownExecution(CoverageReason),
}

/// One coverage problem at a source instruction or transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenCoverageIssue {
    pub coordinate: Option<InstructionCoordinate>,
    pub kind: TokenCoverageIssueKind,
}

/// The token result for one source transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedTokenTransaction {
    pub block: BlockHeader,
    pub tx_index: u32,
    pub execution_status: crate::ExecutionStatus,
    pub events: Vec<TrackedTokenEvent>,
    pub coverage_issues: Vec<TokenCoverageIssue>,
    /// Final account records to insert or replace with this checkpoint.
    ///
    /// The list includes certainty refreshes with no lifecycle change. It is
    /// empty when account state did not commit.
    pub account_updates: Vec<TargetAccountUpdate>,
    pub history_after: HistoryCoverage,
    pub certainty_revision_after: u64,
}

impl TrackedTokenTransaction {
    /// Iterate over exact target-mint transfers that committed.
    pub fn transfers(&self) -> impl Iterator<Item = (&TrackedTokenEvent, &TargetTransfer)> {
        self.events.iter().flat_map(|event| {
            let committed = event.commit == TokenCommitState::Committed;
            event.effects.iter().filter_map(move |effect| match effect {
                TargetMintEffect::Transfer(transfer) if committed => Some((event, transfer)),
                _ => None,
            })
        })
    }
}

/// Process one transaction with a target tracker.
pub fn track_transaction(
    tracker: &mut TargetMintTracker,
    transaction: TransactionView<'_>,
) -> Result<TrackedTokenTransaction, TargetMintTrackerError> {
    tracker.process_transaction(transaction)
}

fn is_classic_token_instruction(instruction: &ResolvedInstruction) -> bool {
    instruction.program_id == Some(CLASSIC_SPL_TOKEN_PROGRAM_ID)
}

#[cfg(test)]
mod tests;
