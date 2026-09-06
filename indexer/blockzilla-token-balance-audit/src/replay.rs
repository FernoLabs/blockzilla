//! Atomic target-only replay of public SPL Token account balances.
//!
//! The reducer is independent of Archive V2 and of any registry format.  Its
//! caller resolves exact instruction accounts, supplies commit decisions, and
//! assigns dense indices to all known addresses for the target mint.
//!
//! A transaction either applies completely or leaves account and mint state
//! unchanged.  The first relevant unknown or unsupported effect poisons the
//! reducer.  This prevents a later instruction from turning an incomplete
//! history into an apparently complete balance result.

use core::fmt;
use std::collections::HashSet;

use crate::{
    CommitStatus, InstructionCoordinate, TokenProgram, UnknownReason,
    effect::{
        CorePublicEffect, Pubkey, ResolvedInstructionAccount, ResolvedTokenInstruction,
        TargetAccountIndex, UnsupportedEffectReason, decode_core_public_effect,
    },
};

/// What is independently known about a Token-2022 transfer-fee extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferFeeKnowledge {
    /// History starts before mint initialization and no committed transfer-fee
    /// configuration was seen before `InitializeMint` / `InitializeMint2`.
    KnownAbsent,
    /// A prior mint history has not proved that the extension is absent.
    Unknown,
}

/// Fixed identity and initial knowledge for one replay target mint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetMintConfig {
    pub mint: Pubkey,
    pub program: TokenProgram,
    pub decimals: u8,
    pub native: bool,
    /// Set this only when replay starts after mint initialization.  A replay
    /// that starts before the mint creation sets this to false.  A committed
    /// core mint initialization then changes it to true atomically.
    pub initialized: bool,
    pub transfer_fee_knowledge: TransferFeeKnowledge,
}

/// Target account data that is relevant to public token-balance metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountLifecycle {
    /// The address does not currently hold an account for the target mint.
    Closed,
    /// The address currently holds a token account for the target mint.
    Open { owner: Pubkey, amount: u64 },
}

/// State of one caller-indexed target account address.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetAccountState {
    pub address: Pubkey,
    /// Increases on each committed initialization for the target mint.  It
    /// separates two lifecycles when an address is closed and later reused.
    pub generation: u64,
    pub lifecycle: AccountLifecycle,
}

/// One account whose transaction-final state differs from its state before
/// the last successful transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetAccountChange {
    pub index: TargetAccountIndex,
    pub previous: TargetAccountState,
}

impl TargetAccountState {
    #[must_use]
    pub const fn closed(address: Pubkey) -> Self {
        Self {
            address,
            generation: 0,
            lifecycle: AccountLifecycle::Closed,
        }
    }

    #[must_use]
    pub const fn is_open(self) -> bool {
        matches!(self.lifecycle, AccountLifecycle::Open { .. })
    }

    #[must_use]
    pub const fn amount(self) -> Option<u64> {
        match self.lifecycle {
            AccountLifecycle::Closed => None,
            AccountLifecycle::Open { amount, .. } => Some(amount),
        }
    }

    #[must_use]
    pub const fn owner(self) -> Option<Pubkey> {
        match self.lifecycle {
            AccountLifecycle::Closed => None,
            AccountLifecycle::Open { owner, .. } => Some(owner),
        }
    }
}

/// Invalid fixed reducer input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReducerConfigError {
    TooManyTargetAccounts,
    DuplicateTargetAccount(Pubkey),
    TargetMintIsAlsoTokenAccount,
}

impl fmt::Display for ReducerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyTargetAccounts => {
                formatter.write_str("target account count exceeds u32 indexing")
            }
            Self::DuplicateTargetAccount(_) => {
                formatter.write_str("target account address occurs more than once")
            }
            Self::TargetMintIsAlsoTokenAccount => {
                formatter.write_str("target mint also occurs as a target token account")
            }
        }
    }
}

impl std::error::Error for ReducerConfigError {}

/// Why exact target replay cannot continue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayErrorReason {
    ReducerAlreadyPoisoned,
    UnknownCommitStatus(UnknownReason),
    UnsupportedEffect(UnsupportedEffectReason),
    InvalidTargetAccountIndex(TargetAccountIndex),
    TargetAccountAddressMismatch(TargetAccountIndex),
    WrongTokenProgram {
        expected: TokenProgram,
        actual: TokenProgram,
    },
    NativeTargetUnsupported,
    TargetMintNotInitialized,
    TargetMintAlreadyInitialized,
    TargetMintCloseUnsupported,
    TransferFeeStateUnknown,
    DecimalsMismatch {
        expected: u8,
        actual: u8,
    },
    UntrackedTargetAccount,
    UntrackedTransferCounterparty,
    TargetAccountAlreadyOpen(TargetAccountIndex),
    TargetAccountNotOpen(TargetAccountIndex),
    TargetAccountMintMismatch(TargetAccountIndex),
    TransferFeeExceedsAmount {
        amount: u64,
        expected_fee: u64,
    },
    InsufficientFunds(TargetAccountIndex),
    AmountOverflow(TargetAccountIndex),
    GenerationOverflow(TargetAccountIndex),
    CloseAccountHasNonZeroAmount(TargetAccountIndex),
}

impl ReplayErrorReason {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ReducerAlreadyPoisoned => "reducer_already_poisoned",
            Self::UnknownCommitStatus(_) => "unknown_commit_status",
            Self::UnsupportedEffect(_) => "unsupported_effect",
            Self::InvalidTargetAccountIndex(_) => "invalid_target_account_index",
            Self::TargetAccountAddressMismatch(_) => "target_account_address_mismatch",
            Self::WrongTokenProgram { .. } => "wrong_token_program",
            Self::NativeTargetUnsupported => "native_target_unsupported",
            Self::TargetMintNotInitialized => "target_mint_not_initialized",
            Self::TargetMintAlreadyInitialized => "target_mint_already_initialized",
            Self::TargetMintCloseUnsupported => "target_mint_close_unsupported",
            Self::TransferFeeStateUnknown => "transfer_fee_state_unknown",
            Self::DecimalsMismatch { .. } => "decimals_mismatch",
            Self::UntrackedTargetAccount => "untracked_target_account",
            Self::UntrackedTransferCounterparty => "untracked_transfer_counterparty",
            Self::TargetAccountAlreadyOpen(_) => "target_account_already_open",
            Self::TargetAccountNotOpen(_) => "target_account_not_open",
            Self::TargetAccountMintMismatch(_) => "target_account_mint_mismatch",
            Self::TransferFeeExceedsAmount { .. } => "transfer_fee_exceeds_amount",
            Self::InsufficientFunds(_) => "insufficient_funds",
            Self::AmountOverflow(_) => "amount_overflow",
            Self::GenerationOverflow(_) => "generation_overflow",
            Self::CloseAccountHasNonZeroAmount(_) => "close_account_has_non_zero_amount",
        }
    }
}

impl fmt::Display for ReplayErrorReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReducerAlreadyPoisoned => formatter.write_str("reducer is already poisoned"),
            Self::UnknownCommitStatus(reason) => {
                write!(
                    formatter,
                    "instruction commit status is unknown: {reason:?}"
                )
            }
            Self::UnsupportedEffect(reason) => write!(formatter, "{reason}"),
            Self::InvalidTargetAccountIndex(index) => {
                write!(formatter, "target account index {index} is out of range")
            }
            Self::TargetAccountAddressMismatch(index) => write!(
                formatter,
                "target account index {index} does not match its configured address"
            ),
            Self::WrongTokenProgram { expected, actual } => write!(
                formatter,
                "target uses {expected:?}, but instruction uses {actual:?}"
            ),
            Self::NativeTargetUnsupported => {
                formatter.write_str("native token replay needs lamport state")
            }
            Self::TargetMintNotInitialized => formatter.write_str("target mint is not initialized"),
            Self::TargetMintAlreadyInitialized => {
                formatter.write_str("target mint is already initialized")
            }
            Self::TargetMintCloseUnsupported => {
                formatter.write_str("closing and reopening the target mint is not modeled")
            }
            Self::TransferFeeStateUnknown => formatter
                .write_str("Token-2022 transfer fee absence has not been independently proved"),
            Self::DecimalsMismatch { expected, actual } => write!(
                formatter,
                "instruction decimals are {actual}; target mint decimals are {expected}"
            ),
            Self::UntrackedTargetAccount => {
                formatter.write_str("instruction creates or credits an untracked target account")
            }
            Self::UntrackedTransferCounterparty => {
                formatter.write_str("target transfer has an untracked or non-target counterparty")
            }
            Self::TargetAccountAlreadyOpen(index) => {
                write!(formatter, "target account {index} is already open")
            }
            Self::TargetAccountNotOpen(index) => {
                write!(formatter, "target account {index} is not open")
            }
            Self::TargetAccountMintMismatch(index) => {
                write!(
                    formatter,
                    "target account {index} is used with another mint"
                )
            }
            Self::TransferFeeExceedsAmount {
                amount,
                expected_fee,
            } => write!(
                formatter,
                "expected transfer fee {expected_fee} exceeds transfer amount {amount}"
            ),
            Self::InsufficientFunds(index) => {
                write!(
                    formatter,
                    "target account {index} has insufficient replay funds"
                )
            }
            Self::AmountOverflow(index) => {
                write!(formatter, "target account {index} amount overflows u64")
            }
            Self::GenerationOverflow(index) => {
                write!(formatter, "target account {index} generation overflows u64")
            }
            Self::CloseAccountHasNonZeroAmount(index) => write!(
                formatter,
                "target account {index} is closed with a non-zero public amount"
            ),
        }
    }
}

impl std::error::Error for ReplayErrorReason {}

/// Transaction coordinate, affected targets, and the first fail-closed reason.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayError {
    pub coordinate: InstructionCoordinate,
    pub reason: ReplayErrorReason,
    pub target_accounts: Vec<TargetAccountIndex>,
    pub touches_target_mint: bool,
}

impl fmt::Display for ReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "token replay failed at outer {}",
            self.coordinate.outer_index
        )?;
        if let Some(inner) = self.coordinate.inner_index {
            write!(formatter, ", inner {inner}")?;
        }
        write!(formatter, ": {}", self.reason)
    }
}

impl std::error::Error for ReplayError {}

/// Allocation-free transaction summary.  Detailed account state remains in
/// the reducer and can be read by dense target index.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransactionReplay {
    pub instructions: usize,
    pub committed_instructions: usize,
    pub rolled_back_instructions: usize,
    pub irrelevant_instructions: usize,
    pub no_public_balance_effect_instructions: usize,
    pub state_changing_instructions: usize,
    pub account_mutations: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AppliedEffect {
    Irrelevant,
    NoPublicBalanceEffect,
    StateChanged { account_mutations: usize },
}

/// Exact state for one non-native target mint and its known account addresses.
#[derive(Debug)]
pub struct TargetBalanceReducer {
    config: TargetMintConfig,
    accounts: Vec<TargetAccountState>,
    undo: Vec<TargetAccountChange>,
    mint_undo: Option<(bool, TransferFeeKnowledge)>,
    poisoned: Option<ReplayErrorReason>,
}

impl TargetBalanceReducer {
    /// Start with all target addresses closed.
    pub fn new(
        config: TargetMintConfig,
        target_accounts: Vec<Pubkey>,
    ) -> Result<Self, ReducerConfigError> {
        let states = target_accounts
            .into_iter()
            .map(TargetAccountState::closed)
            .collect();
        Self::from_states(config, states)
    }

    /// Start from explicit account lifecycles, for a separately proved anchor.
    pub fn from_states(
        config: TargetMintConfig,
        accounts: Vec<TargetAccountState>,
    ) -> Result<Self, ReducerConfigError> {
        if accounts.len() > u32::MAX as usize {
            return Err(ReducerConfigError::TooManyTargetAccounts);
        }
        let mut unique = HashSet::with_capacity(accounts.len());
        for account in &accounts {
            if account.address == config.mint {
                return Err(ReducerConfigError::TargetMintIsAlsoTokenAccount);
            }
            if !unique.insert(account.address) {
                return Err(ReducerConfigError::DuplicateTargetAccount(account.address));
            }
        }
        Ok(Self {
            config,
            accounts,
            undo: Vec::new(),
            mint_undo: None,
            poisoned: None,
        })
    }

    #[must_use]
    pub const fn config(&self) -> TargetMintConfig {
        self.config
    }

    #[must_use]
    pub fn accounts(&self) -> &[TargetAccountState] {
        &self.accounts
    }

    #[must_use]
    pub fn account(&self, index: TargetAccountIndex) -> Option<&TargetAccountState> {
        usize::try_from(index)
            .ok()
            .and_then(|index| self.accounts.get(index))
    }

    /// Account changes from the last successful transaction. Each entry keeps
    /// the transaction-start state. The buffer is owned and reused by the
    /// reducer, and remains valid until the next apply attempt.
    #[must_use]
    pub fn last_account_changes(&self) -> &[TargetAccountChange] {
        &self.undo
    }

    #[must_use]
    pub const fn poison_reason(&self) -> Option<ReplayErrorReason> {
        self.poisoned
    }

    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    /// Checked sum of all open target account amounts.
    #[must_use]
    pub fn checked_total_public_amount(&self) -> Option<u64> {
        self.accounts.iter().try_fold(0_u64, |total, account| {
            total.checked_add(account.amount().unwrap_or(0))
        })
    }

    /// Apply one complete transaction atomically.
    ///
    /// `instructions` must contain all token invocations in execution order.
    /// The commit classifier is authoritative for rollback.  A rolled-back
    /// unsupported instruction is ignored because it changed no state.  An
    /// unknown status is accepted only when the instruction account list has
    /// no target mint or target account marker.
    pub fn apply_transaction(
        &mut self,
        instructions: &[ResolvedTokenInstruction<'_>],
    ) -> Result<TransactionReplay, ReplayError> {
        self.apply_transaction_iter(instructions.iter().copied())
    }

    /// Apply one complete transaction from a streaming instruction iterator.
    ///
    /// This has the same atomic contract as [`Self::apply_transaction`], but
    /// lets an archive scanner reuse its account and descriptor buffers without
    /// allocating a second vector of borrowed instruction views.
    pub fn apply_transaction_iter<'a>(
        &mut self,
        instructions: impl IntoIterator<Item = ResolvedTokenInstruction<'a>>,
    ) -> Result<TransactionReplay, ReplayError> {
        self.undo.clear();
        self.mint_undo = None;
        let mut summary = TransactionReplay::default();

        for instruction in instructions {
            summary.instructions += 1;
            if self.poisoned.is_some() {
                return Err(self.error(&instruction, ReplayErrorReason::ReducerAlreadyPoisoned));
            }

            match instruction.commit_status {
                CommitStatus::RolledBack(_) => {
                    summary.rolled_back_instructions += 1;
                    continue;
                }
                CommitStatus::Unknown(reason) => {
                    if self.potentially_relevant(&instruction) {
                        return Err(self
                            .abort(&instruction, ReplayErrorReason::UnknownCommitStatus(reason)));
                    }
                    summary.irrelevant_instructions += 1;
                    continue;
                }
                CommitStatus::Committed => {
                    summary.committed_instructions += 1;
                }
            }

            if !self.potentially_relevant(&instruction) {
                summary.irrelevant_instructions += 1;
                continue;
            }
            if let Err(reason) = self.validate_target_markers(&instruction) {
                return Err(self.abort(&instruction, reason));
            }
            let effect = match decode_core_public_effect(&instruction) {
                Ok(effect) => effect,
                Err(reason) => {
                    return Err(
                        self.abort(&instruction, ReplayErrorReason::UnsupportedEffect(reason))
                    );
                }
            };
            let applied = match self.apply_effect(instruction.program, effect) {
                Ok(applied) => applied,
                Err(reason) => return Err(self.abort(&instruction, reason)),
            };
            match applied {
                AppliedEffect::Irrelevant => summary.irrelevant_instructions += 1,
                AppliedEffect::NoPublicBalanceEffect => {
                    summary.no_public_balance_effect_instructions += 1;
                }
                AppliedEffect::StateChanged { account_mutations } => {
                    summary.state_changing_instructions += 1;
                    summary.account_mutations += account_mutations;
                }
            }
        }

        let accounts = &self.accounts;
        self.undo.retain(|change| {
            accounts[usize::try_from(change.index).expect("configured target indexes fit usize")]
                != change.previous
        });
        self.mint_undo = None;
        Ok(summary)
    }

    fn potentially_relevant(&self, instruction: &ResolvedTokenInstruction<'_>) -> bool {
        instruction
            .accounts
            .iter()
            .any(|account| account.pubkey == self.config.mint || account.target_index.is_some())
    }

    fn validate_target_markers(
        &self,
        instruction: &ResolvedTokenInstruction<'_>,
    ) -> Result<(), ReplayErrorReason> {
        for account in instruction.accounts {
            let Some(index) = account.target_index else {
                continue;
            };
            let index_usize = usize::try_from(index)
                .map_err(|_| ReplayErrorReason::InvalidTargetAccountIndex(index))?;
            let Some(configured) = self.accounts.get(index_usize) else {
                return Err(ReplayErrorReason::InvalidTargetAccountIndex(index));
            };
            if configured.address != account.pubkey {
                return Err(ReplayErrorReason::TargetAccountAddressMismatch(index));
            }
        }
        Ok(())
    }

    fn apply_effect(
        &mut self,
        program: TokenProgram,
        effect: CorePublicEffect,
    ) -> Result<AppliedEffect, ReplayErrorReason> {
        match effect {
            CorePublicEffect::NoPublicBalanceEffect => Ok(AppliedEffect::NoPublicBalanceEffect),
            CorePublicEffect::InitializeMint { mint, decimals } => {
                self.apply_initialize_mint(program, mint, decimals)
            }
            CorePublicEffect::InitializeAccount {
                account,
                mint,
                owner,
            } => self.apply_initialize_account(program, account, mint, owner),
            CorePublicEffect::Transfer {
                source,
                destination,
                amount,
                checked_mint,
                decimals,
            } => self.apply_transfer(program, source, destination, amount, checked_mint, decimals),
            CorePublicEffect::TransferCheckedWithFee {
                source,
                mint,
                destination,
                amount,
                decimals,
                expected_fee,
            } => self.apply_transfer_checked_with_fee(
                program,
                source,
                mint,
                destination,
                amount,
                decimals,
                expected_fee,
            ),
            CorePublicEffect::MintTo {
                mint,
                destination,
                amount,
                decimals,
            } => self.apply_mint_to(program, mint, destination, amount, decimals),
            CorePublicEffect::Burn {
                source,
                mint,
                amount,
                decimals,
            } => self.apply_burn(program, source, mint, amount, decimals),
            CorePublicEffect::CloseAccount { account } => {
                self.apply_close_account(program, account)
            }
            CorePublicEffect::SetAccountOwner { account, new_owner } => {
                self.apply_set_owner(program, account, new_owner)
            }
        }
    }

    fn apply_initialize_mint(
        &mut self,
        program: TokenProgram,
        mint: ResolvedInstructionAccount,
        decimals: u8,
    ) -> Result<AppliedEffect, ReplayErrorReason> {
        if mint.pubkey != self.config.mint {
            if let Some(index) = mint.target_index
                && self.open(index)?.is_some()
            {
                return Err(ReplayErrorReason::TargetAccountMintMismatch(index));
            }
            return Ok(AppliedEffect::Irrelevant);
        }
        self.require_target_program(program)?;
        if self.config.initialized {
            return Err(ReplayErrorReason::TargetMintAlreadyInitialized);
        }
        self.require_decimals(decimals)?;
        self.remember_mint();
        self.config.initialized = true;
        if self.config.transfer_fee_knowledge == TransferFeeKnowledge::Unknown {
            // A transfer-fee configuration must be initialized before the
            // base mint.  Such a relevant instruction would already have
            // failed closed, so reaching this point proves absence for the
            // supplied complete history.
            self.config.transfer_fee_knowledge = TransferFeeKnowledge::KnownAbsent;
        }
        Ok(AppliedEffect::StateChanged {
            account_mutations: 0,
        })
    }

    fn apply_initialize_account(
        &mut self,
        program: TokenProgram,
        account: ResolvedInstructionAccount,
        mint: Pubkey,
        owner: Pubkey,
    ) -> Result<AppliedEffect, ReplayErrorReason> {
        if mint != self.config.mint {
            if let Some(index) = account.target_index
                && self.open(index)?.is_some()
            {
                return Err(ReplayErrorReason::TargetAccountMintMismatch(index));
            }
            return Ok(AppliedEffect::Irrelevant);
        }
        self.require_target_program(program)?;
        self.require_non_native()?;
        self.require_initialized_mint()?;
        let index = account
            .target_index
            .ok_or(ReplayErrorReason::UntrackedTargetAccount)?;
        if self.open(index)?.is_some() {
            return Err(ReplayErrorReason::TargetAccountAlreadyOpen(index));
        }
        let index_usize = self.index(index)?;
        let generation = self.accounts[index_usize]
            .generation
            .checked_add(1)
            .ok_or(ReplayErrorReason::GenerationOverflow(index))?;
        self.remember_account(index_usize);
        self.accounts[index_usize].generation = generation;
        self.accounts[index_usize].lifecycle = AccountLifecycle::Open { owner, amount: 0 };
        Ok(AppliedEffect::StateChanged {
            account_mutations: 1,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_transfer(
        &mut self,
        program: TokenProgram,
        source: ResolvedInstructionAccount,
        destination: ResolvedInstructionAccount,
        amount: u64,
        checked_mint: Option<Pubkey>,
        decimals: Option<u8>,
    ) -> Result<AppliedEffect, ReplayErrorReason> {
        let source_open = self.open_operand(source)?;
        let destination_open = self.open_operand(destination)?;

        if let Some(mint) = checked_mint {
            if mint != self.config.mint {
                if source_open.is_some() || destination_open.is_some() {
                    let index = source
                        .target_index
                        .or(destination.target_index)
                        .expect("open target operand has a target index");
                    return Err(ReplayErrorReason::TargetAccountMintMismatch(index));
                }
                return Ok(AppliedEffect::Irrelevant);
            }
        } else if source_open.is_none() && destination_open.is_none() {
            return Ok(AppliedEffect::Irrelevant);
        }

        self.require_target_program(program)?;
        self.require_non_native()?;
        self.require_initialized_mint()?;
        self.require_fee_free_transfer(program)?;
        if let Some(decimals) = decimals {
            self.require_decimals(decimals)?;
        }

        let source_index = source
            .target_index
            .ok_or(ReplayErrorReason::UntrackedTransferCounterparty)?;
        let destination_index = destination
            .target_index
            .ok_or(ReplayErrorReason::UntrackedTransferCounterparty)?;
        let (_, source_amount) =
            source_open.ok_or(ReplayErrorReason::TargetAccountNotOpen(source_index))?;
        let (_, destination_amount) =
            destination_open.ok_or(ReplayErrorReason::TargetAccountNotOpen(destination_index))?;

        if source_index == destination_index {
            return Ok(AppliedEffect::NoPublicBalanceEffect);
        }
        if source_amount < amount {
            return Err(ReplayErrorReason::InsufficientFunds(source_index));
        }
        let new_source = source_amount - amount;
        let new_destination = destination_amount
            .checked_add(amount)
            .ok_or(ReplayErrorReason::AmountOverflow(destination_index))?;
        self.set_amount(source_index, new_source)?;
        self.set_amount(destination_index, new_destination)?;
        Ok(AppliedEffect::StateChanged {
            account_mutations: 2,
        })
    }

    fn apply_mint_to(
        &mut self,
        program: TokenProgram,
        mint: Pubkey,
        destination: ResolvedInstructionAccount,
        amount: u64,
        decimals: Option<u8>,
    ) -> Result<AppliedEffect, ReplayErrorReason> {
        if mint != self.config.mint {
            if let Some(index) = destination.target_index
                && self.open(index)?.is_some()
            {
                return Err(ReplayErrorReason::TargetAccountMintMismatch(index));
            }
            return Ok(AppliedEffect::Irrelevant);
        }
        self.require_target_program(program)?;
        self.require_non_native()?;
        self.require_initialized_mint()?;
        if let Some(decimals) = decimals {
            self.require_decimals(decimals)?;
        }
        let index = destination
            .target_index
            .ok_or(ReplayErrorReason::UntrackedTargetAccount)?;
        let (_, old_amount) = self
            .open(index)?
            .ok_or(ReplayErrorReason::TargetAccountNotOpen(index))?;
        let new_amount = old_amount
            .checked_add(amount)
            .ok_or(ReplayErrorReason::AmountOverflow(index))?;
        self.set_amount(index, new_amount)?;
        Ok(AppliedEffect::StateChanged {
            account_mutations: 1,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_transfer_checked_with_fee(
        &mut self,
        program: TokenProgram,
        source: ResolvedInstructionAccount,
        mint: Pubkey,
        destination: ResolvedInstructionAccount,
        amount: u64,
        decimals: u8,
        expected_fee: u64,
    ) -> Result<AppliedEffect, ReplayErrorReason> {
        let source_open = self.open_operand(source)?;
        let destination_open = self.open_operand(destination)?;

        if mint != self.config.mint {
            if source_open.is_some() || destination_open.is_some() {
                let index = source
                    .target_index
                    .or(destination.target_index)
                    .expect("open target operand has a target index");
                return Err(ReplayErrorReason::TargetAccountMintMismatch(index));
            }
            return Ok(AppliedEffect::Irrelevant);
        }

        self.require_target_program(program)?;
        self.require_non_native()?;
        self.require_initialized_mint()?;
        self.require_decimals(decimals)?;

        let credited_amount = amount.checked_sub(expected_fee).ok_or(
            ReplayErrorReason::TransferFeeExceedsAmount {
                amount,
                expected_fee,
            },
        )?;
        let source_index = source
            .target_index
            .ok_or(ReplayErrorReason::UntrackedTransferCounterparty)?;
        let destination_index = destination
            .target_index
            .ok_or(ReplayErrorReason::UntrackedTransferCounterparty)?;
        let (_, source_amount) =
            source_open.ok_or(ReplayErrorReason::TargetAccountNotOpen(source_index))?;
        let (_, destination_amount) =
            destination_open.ok_or(ReplayErrorReason::TargetAccountNotOpen(destination_index))?;

        // The Token-2022 processor does not change public amount state when
        // both writable roles name the same account.
        if source_index == destination_index {
            return Ok(AppliedEffect::NoPublicBalanceEffect);
        }
        if source_amount < amount {
            return Err(ReplayErrorReason::InsufficientFunds(source_index));
        }
        let new_source = source_amount - amount;
        let new_destination = destination_amount
            .checked_add(credited_amount)
            .ok_or(ReplayErrorReason::AmountOverflow(destination_index))?;
        self.set_amount(source_index, new_source)?;
        self.set_amount(destination_index, new_destination)?;
        Ok(AppliedEffect::StateChanged {
            account_mutations: 2,
        })
    }

    fn apply_burn(
        &mut self,
        program: TokenProgram,
        source: ResolvedInstructionAccount,
        mint: Pubkey,
        amount: u64,
        decimals: Option<u8>,
    ) -> Result<AppliedEffect, ReplayErrorReason> {
        if mint != self.config.mint {
            if let Some(index) = source.target_index
                && self.open(index)?.is_some()
            {
                return Err(ReplayErrorReason::TargetAccountMintMismatch(index));
            }
            return Ok(AppliedEffect::Irrelevant);
        }
        self.require_target_program(program)?;
        self.require_non_native()?;
        self.require_initialized_mint()?;
        if let Some(decimals) = decimals {
            self.require_decimals(decimals)?;
        }
        let index = source
            .target_index
            .ok_or(ReplayErrorReason::UntrackedTargetAccount)?;
        let (_, old_amount) = self
            .open(index)?
            .ok_or(ReplayErrorReason::TargetAccountNotOpen(index))?;
        let new_amount = old_amount
            .checked_sub(amount)
            .ok_or(ReplayErrorReason::InsufficientFunds(index))?;
        self.set_amount(index, new_amount)?;
        Ok(AppliedEffect::StateChanged {
            account_mutations: 1,
        })
    }

    fn apply_close_account(
        &mut self,
        program: TokenProgram,
        account: ResolvedInstructionAccount,
    ) -> Result<AppliedEffect, ReplayErrorReason> {
        if account.pubkey == self.config.mint {
            return Err(ReplayErrorReason::TargetMintCloseUnsupported);
        }
        let Some(index) = account.target_index else {
            return Ok(AppliedEffect::Irrelevant);
        };
        let Some((_, amount)) = self.open(index)? else {
            // The address can hold a non-target token account between target
            // lifecycles.  Closing that account does not change target state.
            return Ok(AppliedEffect::Irrelevant);
        };
        self.require_target_program(program)?;
        self.require_non_native()?;
        if amount != 0 {
            return Err(ReplayErrorReason::CloseAccountHasNonZeroAmount(index));
        }
        let index_usize = self.index(index)?;
        self.remember_account(index_usize);
        self.accounts[index_usize].lifecycle = AccountLifecycle::Closed;
        Ok(AppliedEffect::StateChanged {
            account_mutations: 1,
        })
    }

    fn apply_set_owner(
        &mut self,
        program: TokenProgram,
        account: ResolvedInstructionAccount,
        new_owner: Pubkey,
    ) -> Result<AppliedEffect, ReplayErrorReason> {
        let Some(index) = account.target_index else {
            return Ok(AppliedEffect::Irrelevant);
        };
        let Some((_, amount)) = self.open(index)? else {
            return Ok(AppliedEffect::Irrelevant);
        };
        self.require_target_program(program)?;
        let index_usize = self.index(index)?;
        self.remember_account(index_usize);
        self.accounts[index_usize].lifecycle = AccountLifecycle::Open {
            owner: new_owner,
            amount,
        };
        Ok(AppliedEffect::StateChanged {
            account_mutations: 1,
        })
    }

    fn require_target_program(&self, actual: TokenProgram) -> Result<(), ReplayErrorReason> {
        if actual != self.config.program {
            return Err(ReplayErrorReason::WrongTokenProgram {
                expected: self.config.program,
                actual,
            });
        }
        Ok(())
    }

    fn require_non_native(&self) -> Result<(), ReplayErrorReason> {
        if self.config.native {
            return Err(ReplayErrorReason::NativeTargetUnsupported);
        }
        Ok(())
    }

    fn require_initialized_mint(&self) -> Result<(), ReplayErrorReason> {
        if !self.config.initialized {
            return Err(ReplayErrorReason::TargetMintNotInitialized);
        }
        Ok(())
    }

    fn require_fee_free_transfer(&self, program: TokenProgram) -> Result<(), ReplayErrorReason> {
        if program == TokenProgram::Token2022
            && self.config.transfer_fee_knowledge != TransferFeeKnowledge::KnownAbsent
        {
            return Err(ReplayErrorReason::TransferFeeStateUnknown);
        }
        Ok(())
    }

    fn require_decimals(&self, actual: u8) -> Result<(), ReplayErrorReason> {
        if actual != self.config.decimals {
            return Err(ReplayErrorReason::DecimalsMismatch {
                expected: self.config.decimals,
                actual,
            });
        }
        Ok(())
    }

    fn index(&self, index: TargetAccountIndex) -> Result<usize, ReplayErrorReason> {
        let index_usize = usize::try_from(index)
            .map_err(|_| ReplayErrorReason::InvalidTargetAccountIndex(index))?;
        if index_usize >= self.accounts.len() {
            return Err(ReplayErrorReason::InvalidTargetAccountIndex(index));
        }
        Ok(index_usize)
    }

    fn open(&self, index: TargetAccountIndex) -> Result<Option<(Pubkey, u64)>, ReplayErrorReason> {
        let index_usize = self.index(index)?;
        Ok(match self.accounts[index_usize].lifecycle {
            AccountLifecycle::Closed => None,
            AccountLifecycle::Open { owner, amount } => Some((owner, amount)),
        })
    }

    fn open_operand(
        &self,
        account: ResolvedInstructionAccount,
    ) -> Result<Option<(Pubkey, u64)>, ReplayErrorReason> {
        match account.target_index {
            Some(index) => self.open(index),
            None => Ok(None),
        }
    }

    fn set_amount(
        &mut self,
        index: TargetAccountIndex,
        new_amount: u64,
    ) -> Result<(), ReplayErrorReason> {
        let index_usize = self.index(index)?;
        let owner = match self.accounts[index_usize].lifecycle {
            AccountLifecycle::Closed => {
                return Err(ReplayErrorReason::TargetAccountNotOpen(index));
            }
            AccountLifecycle::Open { owner, .. } => owner,
        };
        self.remember_account(index_usize);
        self.accounts[index_usize].lifecycle = AccountLifecycle::Open {
            owner,
            amount: new_amount,
        };
        Ok(())
    }

    fn remember_account(&mut self, index: usize) {
        let target_index = u32::try_from(index).expect("configured target indexes fit u32");
        if self.undo.iter().any(|change| change.index == target_index) {
            return;
        }
        self.undo.push(TargetAccountChange {
            index: target_index,
            previous: self.accounts[index],
        });
    }

    fn remember_mint(&mut self) {
        if self.mint_undo.is_none() {
            self.mint_undo = Some((self.config.initialized, self.config.transfer_fee_knowledge));
        }
    }

    fn rollback(&mut self) {
        while let Some(change) = self.undo.pop() {
            self.accounts
                [usize::try_from(change.index).expect("configured target indexes fit usize")] =
                change.previous;
        }
        if let Some((initialized, transfer_fee_knowledge)) = self.mint_undo.take() {
            self.config.initialized = initialized;
            self.config.transfer_fee_knowledge = transfer_fee_knowledge;
        }
    }

    fn abort(
        &mut self,
        instruction: &ResolvedTokenInstruction<'_>,
        reason: ReplayErrorReason,
    ) -> ReplayError {
        self.rollback();
        self.poisoned = Some(reason);
        self.error(instruction, reason)
    }

    fn error(
        &self,
        instruction: &ResolvedTokenInstruction<'_>,
        reason: ReplayErrorReason,
    ) -> ReplayError {
        let mut target_accounts = Vec::new();
        let mut touches_target_mint = false;
        for account in instruction.accounts {
            touches_target_mint |= account.pubkey == self.config.mint;
            if let Some(index) = account.target_index
                && !target_accounts.contains(&index)
            {
                target_accounts.push(index);
            }
        }
        ReplayError {
            coordinate: instruction.coordinate,
            reason,
            target_accounts,
            touches_target_mint,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{RollbackReason, TopLevelInstruction};

    use super::*;

    fn key(value: u8) -> Pubkey {
        [value; 32]
    }

    fn config(initialized: bool) -> TargetMintConfig {
        TargetMintConfig {
            mint: key(9),
            program: TokenProgram::Token2022,
            decimals: 8,
            native: false,
            initialized,
            transfer_fee_knowledge: if initialized {
                TransferFeeKnowledge::KnownAbsent
            } else {
                TransferFeeKnowledge::Unknown
            },
        }
    }

    fn amount_data(tag: TopLevelInstruction, amount: u64) -> [u8; 9] {
        let mut data = [0_u8; 9];
        data[0] = tag.tag();
        data[1..].copy_from_slice(&amount.to_le_bytes());
        data
    }

    fn checked_data(tag: TopLevelInstruction, amount: u64) -> [u8; 10] {
        let mut data = [0_u8; 10];
        data[0] = tag.tag();
        data[1..9].copy_from_slice(&amount.to_le_bytes());
        data[9] = 8;
        data
    }

    fn transfer_checked_with_fee_data(amount: u64, expected_fee: u64) -> [u8; 19] {
        let mut data = [0_u8; 19];
        data[0] = TopLevelInstruction::TransferFeeExtension.tag();
        data[1] = 1;
        data[2..10].copy_from_slice(&amount.to_le_bytes());
        data[10] = 8;
        data[11..19].copy_from_slice(&expected_fee.to_le_bytes());
        data
    }

    fn instruction<'a>(
        outer: u32,
        data: &'a [u8],
        accounts: &'a [ResolvedInstructionAccount],
    ) -> ResolvedTokenInstruction<'a> {
        ResolvedTokenInstruction {
            coordinate: InstructionCoordinate::outer(outer),
            program: TokenProgram::Token2022,
            data,
            accounts,
            commit_status: CommitStatus::Committed,
        }
    }

    #[test]
    fn initialize_mint_proves_fee_absence_then_replays_core_lifecycle() {
        let mut reducer = TargetBalanceReducer::new(config(false), vec![key(1), key(2)]).unwrap();

        let mut mint_data = [0_u8; 35];
        mint_data[0] = TopLevelInstruction::InitializeMint2.tag();
        mint_data[1] = 8;
        mint_data[2..34].copy_from_slice(&key(7));
        mint_data[34] = 0;
        let mint_accounts = [ResolvedInstructionAccount::other(key(9))];
        reducer
            .apply_transaction(&[instruction(0, &mint_data, &mint_accounts)])
            .unwrap();
        assert!(reducer.config().initialized);
        assert_eq!(
            reducer.config().transfer_fee_knowledge,
            TransferFeeKnowledge::KnownAbsent
        );

        let mut init_data = [0_u8; 33];
        init_data[0] = TopLevelInstruction::InitializeAccount3.tag();
        init_data[1..].copy_from_slice(&key(4));
        let init_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(9)),
        ];
        reducer
            .apply_transaction(&[instruction(1, &init_data, &init_accounts)])
            .unwrap();

        let mint_to = amount_data(TopLevelInstruction::MintTo, 100);
        let mint_to_accounts = [
            ResolvedInstructionAccount::other(key(9)),
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(7)),
        ];
        reducer
            .apply_transaction(&[instruction(2, &mint_to, &mint_to_accounts)])
            .unwrap();
        assert_eq!(reducer.account(0).unwrap().amount(), Some(100));

        let burn = amount_data(TopLevelInstruction::Burn, 100);
        let burn_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(9)),
            ResolvedInstructionAccount::other(key(4)),
        ];
        reducer
            .apply_transaction(&[instruction(3, &burn, &burn_accounts)])
            .unwrap();

        let close_data = [TopLevelInstruction::CloseAccount.tag()];
        let close_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(8)),
            ResolvedInstructionAccount::other(key(4)),
        ];
        reducer
            .apply_transaction(&[instruction(4, &close_data, &close_accounts)])
            .unwrap();
        assert_eq!(
            reducer.account(0).unwrap().lifecycle,
            AccountLifecycle::Closed
        );
        assert_eq!(reducer.account(0).unwrap().generation, 1);
        assert_eq!(
            reducer.last_account_changes(),
            &[TargetAccountChange {
                index: 0,
                previous: TargetAccountState {
                    address: key(1),
                    generation: 1,
                    lifecycle: AccountLifecycle::Open {
                        owner: key(4),
                        amount: 0,
                    },
                },
            }]
        );

        reducer
            .apply_transaction(&[instruction(5, &init_data, &init_accounts)])
            .unwrap();
        assert_eq!(reducer.account(0).unwrap().generation, 2);
        assert_eq!(reducer.account(0).unwrap().amount(), Some(0));
        assert_eq!(
            reducer.last_account_changes(),
            &[TargetAccountChange {
                index: 0,
                previous: TargetAccountState {
                    address: key(1),
                    generation: 1,
                    lifecycle: AccountLifecycle::Closed,
                },
            }]
        );
    }

    #[test]
    fn self_transfer_can_exceed_balance_and_has_zero_net_effect() {
        let state = TargetAccountState {
            address: key(1),
            generation: 1,
            lifecycle: AccountLifecycle::Open {
                owner: key(4),
                amount: 50,
            },
        };
        let mut reducer = TargetBalanceReducer::from_states(config(true), vec![state]).unwrap();
        let data = amount_data(TopLevelInstruction::Transfer, 60);
        let accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(4)),
        ];
        let result = reducer
            .apply_transaction(&[instruction(0, &data, &accounts)])
            .unwrap();
        assert_eq!(result.no_public_balance_effect_instructions, 1);
        assert_eq!(reducer.account(0).unwrap().amount(), Some(50));
        assert!(reducer.last_account_changes().is_empty());
    }

    #[test]
    fn zero_value_transfer_does_not_report_a_final_account_mutation() {
        let states = vec![
            TargetAccountState {
                address: key(1),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(4),
                    amount: 50,
                },
            },
            TargetAccountState {
                address: key(2),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(5),
                    amount: 10,
                },
            },
        ];
        let mut reducer = TargetBalanceReducer::from_states(config(true), states).unwrap();
        let data = amount_data(TopLevelInstruction::Transfer, 0);
        let accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::other(key(4)),
        ];

        reducer
            .apply_transaction(&[instruction(0, &data, &accounts)])
            .unwrap();

        assert_eq!(reducer.account(0).unwrap().amount(), Some(50));
        assert_eq!(reducer.account(1).unwrap().amount(), Some(10));
        assert!(reducer.last_account_changes().is_empty());
    }

    #[test]
    fn net_zero_transfers_do_not_report_final_account_mutations() {
        let states = vec![
            TargetAccountState {
                address: key(1),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(4),
                    amount: 50,
                },
            },
            TargetAccountState {
                address: key(2),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(5),
                    amount: 10,
                },
            },
        ];
        let mut reducer = TargetBalanceReducer::from_states(config(true), states).unwrap();
        let data = amount_data(TopLevelInstruction::Transfer, 10);
        let forward_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::other(key(4)),
        ];
        let reverse_accounts = [
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(5)),
        ];
        let instructions = [
            instruction(0, &data, &forward_accounts),
            instruction(1, &data, &reverse_accounts),
        ];

        reducer.apply_transaction(&instructions).unwrap();

        assert_eq!(reducer.account(0).unwrap().amount(), Some(50));
        assert_eq!(reducer.account(1).unwrap().amount(), Some(10));
        assert!(reducer.last_account_changes().is_empty());
    }

    #[test]
    fn transfer_checked_with_fee_debits_gross_and_credits_net() {
        let states = vec![
            TargetAccountState {
                address: key(1),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(4),
                    amount: 100,
                },
            },
            TargetAccountState {
                address: key(2),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(5),
                    amount: 10,
                },
            },
        ];
        let mut target = config(true);
        target.transfer_fee_knowledge = TransferFeeKnowledge::Unknown;
        let mut reducer = TargetBalanceReducer::from_states(target, states).unwrap();
        let data = transfer_checked_with_fee_data(40, 7);
        let accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(9)),
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::other(key(4)),
        ];

        let result = reducer
            .apply_transaction(&[instruction(0, &data, &accounts)])
            .unwrap();
        assert_eq!(
            reducer.last_account_changes(),
            &[
                TargetAccountChange {
                    index: 0,
                    previous: TargetAccountState {
                        address: key(1),
                        generation: 1,
                        lifecycle: AccountLifecycle::Open {
                            owner: key(4),
                            amount: 100,
                        },
                    },
                },
                TargetAccountChange {
                    index: 1,
                    previous: TargetAccountState {
                        address: key(2),
                        generation: 1,
                        lifecycle: AccountLifecycle::Open {
                            owner: key(5),
                            amount: 10,
                        },
                    },
                },
            ]
        );

        assert_eq!(result.state_changing_instructions, 1);
        assert_eq!(result.account_mutations, 2);
        assert_eq!(reducer.account(0).unwrap().amount(), Some(60));
        assert_eq!(reducer.account(1).unwrap().amount(), Some(43));
    }

    #[test]
    fn transfer_checked_with_fee_self_transfer_has_no_public_amount_effect() {
        let state = TargetAccountState {
            address: key(1),
            generation: 1,
            lifecycle: AccountLifecycle::Open {
                owner: key(4),
                amount: 50,
            },
        };
        let mut reducer = TargetBalanceReducer::from_states(config(true), vec![state]).unwrap();
        let data = transfer_checked_with_fee_data(60, 3);
        let accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(9)),
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(4)),
        ];

        let result = reducer
            .apply_transaction(&[instruction(0, &data, &accounts)])
            .unwrap();

        assert_eq!(result.no_public_balance_effect_instructions, 1);
        assert_eq!(reducer.account(0).unwrap().amount(), Some(50));
    }

    #[test]
    fn fee_over_amount_rejects_and_rolls_back_the_whole_transaction() {
        let states = vec![
            TargetAccountState {
                address: key(1),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(4),
                    amount: 100,
                },
            },
            TargetAccountState {
                address: key(2),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(5),
                    amount: 10,
                },
            },
        ];
        let mut reducer = TargetBalanceReducer::from_states(config(true), states).unwrap();
        let valid = transfer_checked_with_fee_data(20, 3);
        let invalid = transfer_checked_with_fee_data(1, 2);
        let accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(9)),
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::other(key(4)),
        ];
        let instructions = [
            instruction(0, &valid, &accounts),
            instruction(1, &invalid, &accounts),
        ];

        let error = reducer.apply_transaction(&instructions).unwrap_err();

        assert_eq!(
            error.reason,
            ReplayErrorReason::TransferFeeExceedsAmount {
                amount: 1,
                expected_fee: 2,
            }
        );
        assert_eq!(error.coordinate, InstructionCoordinate::outer(1));
        assert_eq!(reducer.account(0).unwrap().amount(), Some(100));
        assert_eq!(reducer.account(1).unwrap().amount(), Some(10));
        assert!(reducer.is_poisoned());
    }

    #[test]
    fn unknown_fee_state_blocks_token_2022_transfer() {
        let state = TargetAccountState {
            address: key(1),
            generation: 1,
            lifecycle: AccountLifecycle::Open {
                owner: key(4),
                amount: 50,
            },
        };
        let mut unknown_fee = config(true);
        unknown_fee.transfer_fee_knowledge = TransferFeeKnowledge::Unknown;
        let mut reducer = TargetBalanceReducer::from_states(unknown_fee, vec![state]).unwrap();
        let data = amount_data(TopLevelInstruction::Transfer, 1);
        let accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(4)),
        ];

        let error = reducer
            .apply_transaction(&[instruction(0, &data, &accounts)])
            .unwrap_err();
        assert_eq!(error.reason, ReplayErrorReason::TransferFeeStateUnknown);
        assert_eq!(reducer.account(0).unwrap().amount(), Some(50));
    }

    #[test]
    fn nonzero_close_and_credit_overflow_fail_without_partial_state() {
        let states = vec![
            TargetAccountState {
                address: key(1),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(4),
                    amount: 1,
                },
            },
            TargetAccountState {
                address: key(2),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(5),
                    amount: u64::MAX,
                },
            },
        ];
        let mut reducer = TargetBalanceReducer::from_states(config(true), states).unwrap();
        let close = [TopLevelInstruction::CloseAccount.tag()];
        let close_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(8)),
            ResolvedInstructionAccount::other(key(4)),
        ];
        let close_error = reducer
            .apply_transaction(&[instruction(0, &close, &close_accounts)])
            .unwrap_err();
        assert_eq!(
            close_error.reason,
            ReplayErrorReason::CloseAccountHasNonZeroAmount(0)
        );
        assert_eq!(reducer.account(0).unwrap().amount(), Some(1));

        // A poisoned reducer cannot be resumed.  Use the same proved anchor to
        // test checked arithmetic independently.
        let states = reducer.accounts().to_vec();
        let mut overflow_reducer = TargetBalanceReducer::from_states(config(true), states).unwrap();
        let transfer = amount_data(TopLevelInstruction::Transfer, 1);
        let transfer_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::other(key(4)),
        ];
        let overflow = overflow_reducer
            .apply_transaction(&[instruction(1, &transfer, &transfer_accounts)])
            .unwrap_err();
        assert_eq!(overflow.reason, ReplayErrorReason::AmountOverflow(1));
        assert_eq!(overflow_reducer.account(0).unwrap().amount(), Some(1));
        assert_eq!(
            overflow_reducer.account(1).unwrap().amount(),
            Some(u64::MAX)
        );
    }

    #[test]
    fn transfer_and_set_owner_update_exact_fields() {
        let states = vec![
            TargetAccountState {
                address: key(1),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(4),
                    amount: 75,
                },
            },
            TargetAccountState {
                address: key(2),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(5),
                    amount: 10,
                },
            },
        ];
        let mut reducer = TargetBalanceReducer::from_states(config(true), states).unwrap();
        let transfer = checked_data(TopLevelInstruction::TransferChecked, 25);
        let transfer_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(9)),
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::other(key(4)),
        ];
        reducer
            .apply_transaction(&[instruction(0, &transfer, &transfer_accounts)])
            .unwrap();
        assert_eq!(reducer.account(0).unwrap().amount(), Some(50));
        assert_eq!(reducer.account(1).unwrap().amount(), Some(35));

        let mut set_owner = [0_u8; 35];
        set_owner[0] = TopLevelInstruction::SetAuthority.tag();
        set_owner[1] = 2;
        set_owner[2] = 1;
        set_owner[3..].copy_from_slice(&key(6));
        let owner_accounts = [
            ResolvedInstructionAccount::target(key(2), 1),
            ResolvedInstructionAccount::other(key(5)),
        ];
        reducer
            .apply_transaction(&[instruction(1, &set_owner, &owner_accounts)])
            .unwrap();
        assert_eq!(reducer.account(1).unwrap().owner(), Some(key(6)));
    }

    #[test]
    fn later_unsupported_effect_rolls_back_the_whole_transaction_and_poisons() {
        let state = TargetAccountState {
            address: key(1),
            generation: 1,
            lifecycle: AccountLifecycle::Open {
                owner: key(4),
                amount: 10,
            },
        };
        let mut reducer = TargetBalanceReducer::from_states(config(true), vec![state]).unwrap();
        let mint_to = amount_data(TopLevelInstruction::MintTo, 5);
        let mint_accounts = [
            ResolvedInstructionAccount::other(key(9)),
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(7)),
        ];
        let fee = [TopLevelInstruction::TransferFeeExtension.tag(), 0];
        let fee_accounts = [ResolvedInstructionAccount::other(key(9))];
        let instructions = [
            instruction(0, &mint_to, &mint_accounts),
            instruction(1, &fee, &fee_accounts),
        ];

        let error = reducer.apply_transaction(&instructions).unwrap_err();
        assert_eq!(
            error.reason,
            ReplayErrorReason::UnsupportedEffect(UnsupportedEffectReason::TransferFeeEffect)
        );
        assert_eq!(reducer.account(0).unwrap().amount(), Some(10));
        assert!(reducer.is_poisoned());

        let second = reducer.apply_transaction(&instructions[..1]).unwrap_err();
        assert_eq!(second.reason, ReplayErrorReason::ReducerAlreadyPoisoned);
        assert_eq!(reducer.account(0).unwrap().amount(), Some(10));
    }

    #[test]
    fn rolled_back_unsupported_effect_does_not_taint_state() {
        let mut reducer = TargetBalanceReducer::new(config(true), vec![key(1)]).unwrap();
        let fee = [TopLevelInstruction::TransferFeeExtension.tag(), 0];
        let accounts = [ResolvedInstructionAccount::other(key(9))];
        let mut rolled_back = instruction(0, &fee, &accounts);
        rolled_back.commit_status = CommitStatus::RolledBack(RollbackReason::AncestorFailed);

        let result = reducer.apply_transaction(&[rolled_back]).unwrap();
        assert_eq!(result.rolled_back_instructions, 1);
        assert!(!reducer.is_poisoned());
    }

    #[test]
    fn unknown_relevant_commit_status_fails_closed_but_irrelevant_does_not() {
        let mut reducer = TargetBalanceReducer::new(config(true), vec![key(1)]).unwrap();
        let transfer = amount_data(TopLevelInstruction::Transfer, 1);
        let other_accounts = [
            ResolvedInstructionAccount::other(key(2)),
            ResolvedInstructionAccount::other(key(3)),
            ResolvedInstructionAccount::other(key(4)),
        ];
        let mut irrelevant = instruction(0, &transfer, &other_accounts);
        irrelevant.commit_status = CommitStatus::Unknown(UnknownReason::MissingInvocationLog);
        reducer.apply_transaction(&[irrelevant]).unwrap();

        let target_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(3)),
            ResolvedInstructionAccount::other(key(4)),
        ];
        let mut relevant = instruction(1, &transfer, &target_accounts);
        relevant.commit_status = CommitStatus::Unknown(UnknownReason::MissingInvocationLog);
        let error = reducer.apply_transaction(&[relevant]).unwrap_err();
        assert_eq!(
            error.reason,
            ReplayErrorReason::UnknownCommitStatus(UnknownReason::MissingInvocationLog)
        );
        assert_eq!(error.target_accounts, vec![0]);
    }

    #[test]
    fn address_can_close_reopen_and_hold_another_mint_between_generations() {
        let mut reducer = TargetBalanceReducer::new(config(true), vec![key(1)]).unwrap();
        let mut other_init = [0_u8; 33];
        other_init[0] = TopLevelInstruction::InitializeAccount3.tag();
        other_init[1..].copy_from_slice(&key(4));
        let other_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(8)),
        ];
        let result = reducer
            .apply_transaction(&[instruction(0, &other_init, &other_accounts)])
            .unwrap();
        assert_eq!(result.irrelevant_instructions, 1);
        assert_eq!(
            reducer.account(0).unwrap().lifecycle,
            AccountLifecycle::Closed
        );

        let mut target_init = other_init;
        target_init[1..].copy_from_slice(&key(5));
        let target_accounts = [
            ResolvedInstructionAccount::target(key(1), 0),
            ResolvedInstructionAccount::other(key(9)),
        ];
        reducer
            .apply_transaction(&[instruction(1, &target_init, &target_accounts)])
            .unwrap();
        assert_eq!(reducer.account(0).unwrap().generation, 1);
        assert_eq!(reducer.account(0).unwrap().owner(), Some(key(5)));
    }
}
