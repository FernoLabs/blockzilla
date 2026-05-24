use anyhow::{Context, Result};
use of_car_reader::metadata_decoder::{
    InnerInstructionVisit, ReturnDataVisit, TokenBalanceVisit, TransactionStatusMetaVisitor,
    visit_protobuf_transaction_status_meta,
};
use of_car_reader::stored_transaction::{
    InstructionError as StoredInstructionError, StoredTransactionError,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::str::FromStr;
use wincode::{SchemaRead, SchemaWrite};

use crate::{CompactLogStream, CompactPubkey, KeyIndex};

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactMetaV1 {
    pub err: Option<CompactTransactionError>,

    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,

    pub inner_instructions: Option<Vec<CompactInnerInstructions>>,
    pub logs: Option<CompactLogStream>,

    pub pre_token_balances: Vec<CompactTokenBalance>,
    pub post_token_balances: Vec<CompactTokenBalance>,

    pub rewards: Vec<CompactReward>,

    pub loaded_writable_addresses: Vec<CompactPubkey>,
    pub loaded_readonly_addresses: Vec<CompactPubkey>,

    pub return_data: Option<CompactReturnData>,

    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum CompactTransactionError {
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    AlreadyProcessed,
    BlockhashNotFound,
    InstructionError(u8, CompactInstructionError),
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    InvalidProgramForExecution,
    SanitizeFailure,
    ClusterMaintenance,
    AccountBorrowOutstanding,
    WouldExceedMaxBlockCostLimit,
    UnsupportedVersion,
    InvalidWritableAccount,
    WouldExceedMaxAccountCostLimit,
    WouldExceedAccountDataBlockLimit,
    TooManyAccountLocks,
    AddressLookupTableNotFound,
    InvalidAddressLookupTableOwner,
    InvalidAddressLookupTableData,
    InvalidAddressLookupTableIndex,
    InvalidRentPayingAccount,
    WouldExceedMaxVoteCostLimit,
    WouldExceedAccountDataTotalLimit,
    DuplicateInstruction(u8),
    InsufficientFundsForRent { account_index: u8 },
    MaxLoadedAccountsDataSizeExceeded,
    InvalidLoadedAccountsDataSizeLimit,
    ResanitizationNeeded,
    ProgramExecutionTemporarilyRestricted { account_index: u8 },
    UnbalancedTransaction,
    ProgramCacheHitMaxLimit,
    CommitCancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum CompactInstructionError {
    GenericError,
    InvalidArgument,
    InvalidInstructionData,
    InvalidAccountData,
    AccountDataTooSmall,
    InsufficientFunds,
    IncorrectProgramId,
    MissingRequiredSignature,
    AccountAlreadyInitialized,
    UninitializedAccount,
    UnbalancedInstruction,
    ModifiedProgramId,
    ExternalAccountLamportSpend,
    ExternalAccountDataModified,
    ReadonlyLamportChange,
    ReadonlyDataModified,
    DuplicateAccountIndex,
    ExecutableModified,
    RentEpochModified,
    NotEnoughAccountKeys,
    AccountDataSizeChanged,
    AccountNotExecutable,
    AccountBorrowFailed,
    AccountBorrowOutstanding,
    DuplicateAccountOutOfSync,
    Custom(u32),
    InvalidError,
    ExecutableDataModified,
    ExecutableLamportChange,
    ExecutableAccountNotRentExempt,
    UnsupportedProgramId,
    CallDepth,
    MissingAccount,
    ReentrancyNotAllowed,
    MaxSeedLengthExceeded,
    InvalidSeeds,
    InvalidRealloc,
    ComputationalBudgetExceeded,
    PrivilegeEscalation,
    ProgramEnvironmentSetupFailure,
    ProgramFailedToComplete,
    ProgramFailedToCompile,
    Immutable,
    IncorrectAuthority,
    BorshIoError(String),
    AccountNotRentExempt,
    InvalidAccountOwner,
    ArithmeticOverflow,
    UnsupportedSysvar,
    IllegalOwner,
    MaxAccountsDataAllocationsExceeded,
    MaxAccountsExceeded,
    MaxInstructionTraceLengthExceeded,
    BuiltinProgramsMustConsumeComputeUnits,
}

impl CompactTransactionError {
    pub fn from_stored_wincode_bytes(bytes: &[u8]) -> Result<Self> {
        let stored = decode_stored_transaction_error_bytes(bytes)?;
        Ok(Self::from(stored))
    }
}

impl From<StoredTransactionError> for CompactTransactionError {
    fn from(err: StoredTransactionError) -> Self {
        match err {
            StoredTransactionError::AccountInUse => Self::AccountInUse,
            StoredTransactionError::AccountLoadedTwice => Self::AccountLoadedTwice,
            StoredTransactionError::AccountNotFound => Self::AccountNotFound,
            StoredTransactionError::ProgramAccountNotFound => Self::ProgramAccountNotFound,
            StoredTransactionError::InsufficientFundsForFee => Self::InsufficientFundsForFee,
            StoredTransactionError::InvalidAccountForFee => Self::InvalidAccountForFee,
            StoredTransactionError::AlreadyProcessed => Self::AlreadyProcessed,
            StoredTransactionError::BlockhashNotFound => Self::BlockhashNotFound,
            StoredTransactionError::InstructionError(index, err) => {
                Self::InstructionError(index, CompactInstructionError::from(err))
            }
            StoredTransactionError::CallChainTooDeep => Self::CallChainTooDeep,
            StoredTransactionError::MissingSignatureForFee => Self::MissingSignatureForFee,
            StoredTransactionError::InvalidAccountIndex => Self::InvalidAccountIndex,
            StoredTransactionError::SignatureFailure => Self::SignatureFailure,
            StoredTransactionError::InvalidProgramForExecution => Self::InvalidProgramForExecution,
            StoredTransactionError::SanitizeFailure => Self::SanitizeFailure,
            StoredTransactionError::ClusterMaintenance => Self::ClusterMaintenance,
            StoredTransactionError::AccountBorrowOutstanding => Self::AccountBorrowOutstanding,
            StoredTransactionError::WouldExceedMaxBlockCostLimit => {
                Self::WouldExceedMaxBlockCostLimit
            }
            StoredTransactionError::UnsupportedVersion => Self::UnsupportedVersion,
            StoredTransactionError::InvalidWritableAccount => Self::InvalidWritableAccount,
            StoredTransactionError::WouldExceedMaxAccountCostLimit => {
                Self::WouldExceedMaxAccountCostLimit
            }
            StoredTransactionError::WouldExceedAccountDataBlockLimit => {
                Self::WouldExceedAccountDataBlockLimit
            }
            StoredTransactionError::TooManyAccountLocks => Self::TooManyAccountLocks,
            StoredTransactionError::AddressLookupTableNotFound => Self::AddressLookupTableNotFound,
            StoredTransactionError::InvalidAddressLookupTableOwner => {
                Self::InvalidAddressLookupTableOwner
            }
            StoredTransactionError::InvalidAddressLookupTableData => {
                Self::InvalidAddressLookupTableData
            }
            StoredTransactionError::InvalidAddressLookupTableIndex => {
                Self::InvalidAddressLookupTableIndex
            }
            StoredTransactionError::InvalidRentPayingAccount => Self::InvalidRentPayingAccount,
            StoredTransactionError::WouldExceedMaxVoteCostLimit => {
                Self::WouldExceedMaxVoteCostLimit
            }
            StoredTransactionError::WouldExceedAccountDataTotalLimit => {
                Self::WouldExceedAccountDataTotalLimit
            }
            StoredTransactionError::DuplicateInstruction(index) => {
                Self::DuplicateInstruction(index)
            }
            StoredTransactionError::InsufficientFundsForRent { account_index } => {
                Self::InsufficientFundsForRent { account_index }
            }
            StoredTransactionError::MaxLoadedAccountsDataSizeExceeded => {
                Self::MaxLoadedAccountsDataSizeExceeded
            }
            StoredTransactionError::InvalidLoadedAccountsDataSizeLimit => {
                Self::InvalidLoadedAccountsDataSizeLimit
            }
            StoredTransactionError::ResanitizationNeeded => Self::ResanitizationNeeded,
            StoredTransactionError::ProgramExecutionTemporarilyRestricted { account_index } => {
                Self::ProgramExecutionTemporarilyRestricted { account_index }
            }
            StoredTransactionError::UnbalancedTransaction => Self::UnbalancedTransaction,
            StoredTransactionError::ProgramCacheHitMaxLimit => Self::ProgramCacheHitMaxLimit,
            StoredTransactionError::CommitCancelled => Self::CommitCancelled,
        }
    }
}

impl From<StoredInstructionError> for CompactInstructionError {
    fn from(err: StoredInstructionError) -> Self {
        match err {
            StoredInstructionError::GenericError => Self::GenericError,
            StoredInstructionError::InvalidArgument => Self::InvalidArgument,
            StoredInstructionError::InvalidInstructionData => Self::InvalidInstructionData,
            StoredInstructionError::InvalidAccountData => Self::InvalidAccountData,
            StoredInstructionError::AccountDataTooSmall => Self::AccountDataTooSmall,
            StoredInstructionError::InsufficientFunds => Self::InsufficientFunds,
            StoredInstructionError::IncorrectProgramId => Self::IncorrectProgramId,
            StoredInstructionError::MissingRequiredSignature => Self::MissingRequiredSignature,
            StoredInstructionError::AccountAlreadyInitialized => Self::AccountAlreadyInitialized,
            StoredInstructionError::UninitializedAccount => Self::UninitializedAccount,
            StoredInstructionError::UnbalancedInstruction => Self::UnbalancedInstruction,
            StoredInstructionError::ModifiedProgramId => Self::ModifiedProgramId,
            StoredInstructionError::ExternalAccountLamportSpend => {
                Self::ExternalAccountLamportSpend
            }
            StoredInstructionError::ExternalAccountDataModified => {
                Self::ExternalAccountDataModified
            }
            StoredInstructionError::ReadonlyLamportChange => Self::ReadonlyLamportChange,
            StoredInstructionError::ReadonlyDataModified => Self::ReadonlyDataModified,
            StoredInstructionError::DuplicateAccountIndex => Self::DuplicateAccountIndex,
            StoredInstructionError::ExecutableModified => Self::ExecutableModified,
            StoredInstructionError::RentEpochModified => Self::RentEpochModified,
            StoredInstructionError::NotEnoughAccountKeys => Self::NotEnoughAccountKeys,
            StoredInstructionError::AccountDataSizeChanged => Self::AccountDataSizeChanged,
            StoredInstructionError::AccountNotExecutable => Self::AccountNotExecutable,
            StoredInstructionError::AccountBorrowFailed => Self::AccountBorrowFailed,
            StoredInstructionError::AccountBorrowOutstanding => Self::AccountBorrowOutstanding,
            StoredInstructionError::DuplicateAccountOutOfSync => Self::DuplicateAccountOutOfSync,
            StoredInstructionError::Custom(code) => Self::Custom(code),
            StoredInstructionError::InvalidError => Self::InvalidError,
            StoredInstructionError::ExecutableDataModified => Self::ExecutableDataModified,
            StoredInstructionError::ExecutableLamportChange => Self::ExecutableLamportChange,
            StoredInstructionError::ExecutableAccountNotRentExempt => {
                Self::ExecutableAccountNotRentExempt
            }
            StoredInstructionError::UnsupportedProgramId => Self::UnsupportedProgramId,
            StoredInstructionError::CallDepth => Self::CallDepth,
            StoredInstructionError::MissingAccount => Self::MissingAccount,
            StoredInstructionError::ReentrancyNotAllowed => Self::ReentrancyNotAllowed,
            StoredInstructionError::MaxSeedLengthExceeded => Self::MaxSeedLengthExceeded,
            StoredInstructionError::InvalidSeeds => Self::InvalidSeeds,
            StoredInstructionError::InvalidRealloc => Self::InvalidRealloc,
            StoredInstructionError::ComputationalBudgetExceeded => {
                Self::ComputationalBudgetExceeded
            }
            StoredInstructionError::PrivilegeEscalation => Self::PrivilegeEscalation,
            StoredInstructionError::ProgramEnvironmentSetupFailure => {
                Self::ProgramEnvironmentSetupFailure
            }
            StoredInstructionError::ProgramFailedToComplete => Self::ProgramFailedToComplete,
            StoredInstructionError::ProgramFailedToCompile => Self::ProgramFailedToCompile,
            StoredInstructionError::Immutable => Self::Immutable,
            StoredInstructionError::IncorrectAuthority => Self::IncorrectAuthority,
            StoredInstructionError::BorshIoError(message) => Self::BorshIoError(message),
            StoredInstructionError::AccountNotRentExempt => Self::AccountNotRentExempt,
            StoredInstructionError::InvalidAccountOwner => Self::InvalidAccountOwner,
            StoredInstructionError::ArithmeticOverflow => Self::ArithmeticOverflow,
            StoredInstructionError::UnsupportedSysvar => Self::UnsupportedSysvar,
            StoredInstructionError::IllegalOwner => Self::IllegalOwner,
            StoredInstructionError::MaxAccountsDataAllocationsExceeded => {
                Self::MaxAccountsDataAllocationsExceeded
            }
            StoredInstructionError::MaxAccountsExceeded => Self::MaxAccountsExceeded,
            StoredInstructionError::MaxInstructionTraceLengthExceeded => {
                Self::MaxInstructionTraceLengthExceeded
            }
            StoredInstructionError::BuiltinProgramsMustConsumeComputeUnits => {
                Self::BuiltinProgramsMustConsumeComputeUnits
            }
        }
    }
}

fn decode_stored_transaction_error_bytes(bytes: &[u8]) -> Result<StoredTransactionError> {
    match wincode::deserialize::<StoredTransactionError>(bytes) {
        Ok(err) => Ok(err),
        Err(err) => decode_unit_borsh_io_instruction_error(bytes)
            .map_err(|_| anyhow::anyhow!("decode transaction error: {err}")),
    }
}

fn decode_unit_borsh_io_instruction_error(
    bytes: &[u8],
) -> std::result::Result<StoredTransactionError, ()> {
    const TRANSACTION_ERROR_INSTRUCTION_ERROR: u32 = 8;
    const INSTRUCTION_ERROR_BORSH_IO_ERROR: u32 = 44;

    if bytes.len() != 9 {
        return Err(());
    }

    let transaction_error_tag = u32::from_le_bytes(bytes[0..4].try_into().expect("checked length"));
    let instruction_error_tag = u32::from_le_bytes(bytes[5..9].try_into().expect("checked length"));
    if transaction_error_tag != TRANSACTION_ERROR_INSTRUCTION_ERROR
        || instruction_error_tag != INSTRUCTION_ERROR_BORSH_IO_ERROR
    {
        return Err(());
    }

    Ok(StoredTransactionError::InstructionError(
        bytes[4],
        StoredInstructionError::BorshIoError(String::new()),
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactInnerInstructions {
    pub index: u32,
    pub instructions: Vec<CompactInnerInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactInnerInstruction {
    pub program_id_index: u32, // message index
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactReturnData {
    pub program_id: CompactPubkey,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactTokenBalance {
    pub account_index: u32,

    pub mint: Option<CompactPubkey>,
    pub owner: Option<CompactPubkey>,
    pub program_id: Option<CompactPubkey>,

    pub amount: u64,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactReward {
    pub pubkey: CompactPubkey,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: i32,
    pub commission: Option<u8>,
}

pub fn compact_meta_from_proto(
    meta: &of_car_reader::confirmed_block::TransactionStatusMeta,
    index: &KeyIndex,
) -> Result<CompactMetaV1> {
    let err = meta
        .err
        .as_ref()
        .map(|e| CompactTransactionError::from_stored_wincode_bytes(&e.err))
        .transpose()?;

    let loaded_writable_addresses = meta
        .loaded_writable_addresses
        .iter()
        .map(|a| index.compact(a.as_slice().try_into().unwrap()))
        .collect();
    let loaded_readonly_addresses = meta
        .loaded_readonly_addresses
        .iter()
        .map(|a| index.compact(a.as_slice().try_into().unwrap()))
        .collect();

    let inner_instructions = if meta.inner_instructions_none {
        None
    } else {
        Some(
            meta.inner_instructions
                .iter()
                .map(|ii| CompactInnerInstructions {
                    index: ii.index,
                    instructions: ii
                        .instructions
                        .iter()
                        .map(|ix| CompactInnerInstruction {
                            program_id_index: ix.program_id_index,
                            accounts: ix.accounts.to_vec(),
                            data: ix.data.to_vec(),
                            stack_height: ix.stack_height,
                        })
                        .collect(),
                })
                .collect(),
        )
    };

    let logs = if meta.log_messages_none {
        None
    } else {
        Some(crate::log::parse_logs(&meta.log_messages, index))
    };

    let pre_token_balances = meta
        .pre_token_balances
        .iter()
        .map(|tb| compact_token_balance(tb, index))
        .collect::<Result<Vec<_>>>()?;

    let post_token_balances = meta
        .post_token_balances
        .iter()
        .map(|tb| compact_token_balance(tb, index))
        .collect::<Result<Vec<_>>>()?;

    let rewards = meta
        .rewards
        .iter()
        .map(|rw| compact_reward(rw, index))
        .collect::<Result<Vec<_>>>()?;

    let return_data = if meta.return_data_none {
        None
    } else {
        meta.return_data
            .as_ref()
            .map(|rd| -> Result<CompactReturnData> {
                Ok(CompactReturnData {
                    program_id: index.compact(rd.program_id.as_slice().try_into().unwrap()),
                    data: rd.data.clone(),
                })
            })
            .transpose()?
    };

    Ok(CompactMetaV1 {
        err,

        fee: meta.fee,
        pre_balances: meta.pre_balances.to_vec(),
        post_balances: meta.post_balances.to_vec(),

        inner_instructions,
        logs,

        pre_token_balances,
        post_token_balances,

        rewards,

        loaded_writable_addresses,
        loaded_readonly_addresses,

        return_data,

        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

pub fn compact_meta_from_protobuf_visit(bytes: &[u8], index: &KeyIndex) -> Result<CompactMetaV1> {
    let mut visitor = CompactMetaVisitor::new(index);
    visit_protobuf_transaction_status_meta(bytes, &mut visitor)
        .map_err(|err| anyhow::anyhow!("protobuf visit: {err}"))?;
    visitor.finish()
}

struct CompactMetaVisitor<'a> {
    index: &'a KeyIndex,
    err: Option<CompactTransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Vec<CompactInnerInstructions>,
    inner_instructions_none: bool,
    log_messages: Vec<String>,
    log_messages_none: bool,
    pre_token_balances: Vec<CompactTokenBalance>,
    post_token_balances: Vec<CompactTokenBalance>,
    rewards: Vec<CompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Option<CompactReturnData>,
    return_data_none: bool,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
    error: Option<anyhow::Error>,
}

impl<'a> CompactMetaVisitor<'a> {
    fn new(index: &'a KeyIndex) -> Self {
        Self {
            index,
            err: None,
            fee: 0,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: Vec::new(),
            inner_instructions_none: false,
            log_messages: Vec::new(),
            log_messages_none: false,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            return_data_none: false,
            compute_units_consumed: None,
            cost_units: None,
            error: None,
        }
    }

    fn record_error(&mut self, err: anyhow::Error) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }

    fn finish(self) -> Result<CompactMetaV1> {
        if let Some(err) = self.error {
            return Err(err);
        }

        let inner_instructions = if self.inner_instructions_none {
            None
        } else {
            Some(self.inner_instructions)
        };
        let logs = if self.log_messages_none {
            None
        } else {
            Some(crate::log::parse_logs(&self.log_messages, self.index))
        };
        let return_data = if self.return_data_none {
            None
        } else {
            self.return_data
        };

        Ok(CompactMetaV1 {
            err: self.err,
            fee: self.fee,
            pre_balances: self.pre_balances,
            post_balances: self.post_balances,
            inner_instructions,
            logs,
            pre_token_balances: self.pre_token_balances,
            post_token_balances: self.post_token_balances,
            rewards: self.rewards,
            loaded_writable_addresses: self.loaded_writable_addresses,
            loaded_readonly_addresses: self.loaded_readonly_addresses,
            return_data,
            compute_units_consumed: self.compute_units_consumed,
            cost_units: self.cost_units,
        })
    }
}

impl<'a, 'b> TransactionStatusMetaVisitor<'b> for CompactMetaVisitor<'a> {
    #[inline]
    fn wants_status_error(&self) -> bool {
        true
    }

    #[inline]
    fn wants_pre_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_inner_instructions(&self) -> bool {
        true
    }

    #[inline]
    fn wants_log_messages(&self) -> bool {
        true
    }

    #[inline]
    fn wants_pre_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_rewards(&self) -> bool {
        true
    }

    #[inline]
    fn wants_loaded_addresses(&self) -> bool {
        true
    }

    #[inline]
    fn wants_return_data(&self) -> bool {
        true
    }

    #[inline]
    fn status_error(&mut self, err: &'b [u8]) {
        match CompactTransactionError::from_stored_wincode_bytes(err) {
            Ok(err) => self.err = Some(err),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn fee(&mut self, fee: u64) {
        self.fee = fee;
    }

    #[inline]
    fn pre_balance(&mut self, _index: usize, lamports: u64) {
        self.pre_balances.push(lamports);
    }

    #[inline]
    fn post_balance(&mut self, _index: usize, lamports: u64) {
        self.post_balances.push(lamports);
    }

    #[inline]
    fn inner_instruction(&mut self, instruction: InnerInstructionVisit<'b>) {
        if self
            .inner_instructions
            .last()
            .is_none_or(|group| group.index != instruction.outer_instruction_index)
        {
            self.inner_instructions.push(CompactInnerInstructions {
                index: instruction.outer_instruction_index,
                instructions: Vec::new(),
            });
        }

        let Some(group) = self.inner_instructions.last_mut() else {
            return;
        };
        group.instructions.push(CompactInnerInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts.to_vec(),
            data: instruction.data.to_vec(),
            stack_height: instruction.stack_height,
        });
    }

    #[inline]
    fn inner_instructions_none(&mut self, none: bool) {
        self.inner_instructions_none = none;
    }

    #[inline]
    fn log_message(&mut self, message: &'b str) {
        self.log_messages.push(message.to_owned());
    }

    #[inline]
    fn log_messages_none(&mut self, none: bool) {
        self.log_messages_none = none;
    }

    #[inline]
    fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'b>) {
        match compact_token_balance_visit(balance, self.index) {
            Ok(balance) => self.pre_token_balances.push(balance),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn post_token_balance(&mut self, balance: TokenBalanceVisit<'b>) {
        match compact_token_balance_visit(balance, self.index) {
            Ok(balance) => self.post_token_balances.push(balance),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn reward_raw(&mut self, bytes: &'b [u8]) {
        match of_car_reader::confirmed_block::Reward::decode(bytes)
            .map_err(anyhow::Error::from)
            .and_then(|reward| compact_reward(&reward, self.index))
        {
            Ok(reward) => self.rewards.push(reward),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn loaded_writable_address(&mut self, address: &'b [u8]) {
        match address.try_into() {
            Ok(address) => self
                .loaded_writable_addresses
                .push(self.index.compact(address)),
            Err(_) => self.record_error(anyhow::anyhow!(
                "invalid writable loaded address len {}",
                address.len()
            )),
        }
    }

    #[inline]
    fn loaded_readonly_address(&mut self, address: &'b [u8]) {
        match address.try_into() {
            Ok(address) => self
                .loaded_readonly_addresses
                .push(self.index.compact(address)),
            Err(_) => self.record_error(anyhow::anyhow!(
                "invalid readonly loaded address len {}",
                address.len()
            )),
        }
    }

    #[inline]
    fn return_data(&mut self, return_data: ReturnDataVisit<'b>) {
        match return_data.program_id.try_into() {
            Ok(program_id) => {
                self.return_data = Some(CompactReturnData {
                    program_id: self.index.compact(program_id),
                    data: return_data.data.to_vec(),
                });
            }
            Err(_) => self.record_error(anyhow::anyhow!(
                "invalid return data program id len {}",
                return_data.program_id.len()
            )),
        }
    }

    #[inline]
    fn return_data_none(&mut self, none: bool) {
        self.return_data_none = none;
    }

    #[inline]
    fn compute_units_consumed(&mut self, units: u64) {
        self.compute_units_consumed = Some(units);
    }

    #[inline]
    fn cost_units(&mut self, units: u64) {
        self.cost_units = Some(units);
    }
}

#[inline]
fn compact_pubkey_optional(index: &KeyIndex, s: &str) -> Option<CompactPubkey> {
    if s.is_empty() {
        return None;
    }
    index.compact_str(s)
}

fn compact_token_balance(
    tb: &of_car_reader::confirmed_block::TokenBalance,
    index: &KeyIndex,
) -> Result<CompactTokenBalance> {
    let mint = compact_pubkey_optional(index, &tb.mint);
    let owner = compact_pubkey_optional(index, &tb.owner);
    let program_id = compact_pubkey_optional(index, &tb.program_id);

    let (amount, decimals) = match &tb.ui_token_amount {
        None => (0u64, 0u8),
        Some(uta) => {
            let amount = uta
                .amount
                .parse::<u64>()
                .context("parse token amount u64")?;
            (amount, uta.decimals as u8)
        }
    };

    Ok(CompactTokenBalance {
        account_index: tb.account_index,
        mint,
        owner,
        program_id,
        amount,
        decimals,
    })
}

fn compact_token_balance_visit(
    tb: TokenBalanceVisit<'_>,
    index: &KeyIndex,
) -> Result<CompactTokenBalance> {
    let mint = compact_pubkey_optional(index, tb.mint);
    let owner = compact_pubkey_optional(index, tb.owner);
    let program_id = compact_pubkey_optional(index, tb.program_id);

    let (amount, decimals) = match tb.ui_token_amount {
        None => (0u64, 0u8),
        Some(uta) => {
            let amount = uta
                .amount
                .parse::<u64>()
                .context("parse token amount u64")?;
            (amount, uta.decimals as u8)
        }
    };

    Ok(CompactTokenBalance {
        account_index: tb.account_index,
        mint,
        owner,
        program_id,
        amount,
        decimals,
    })
}

fn compact_reward(
    rw: &of_car_reader::confirmed_block::Reward,
    index: &KeyIndex,
) -> Result<CompactReward> {
    let pk = Pubkey::from_str(&rw.pubkey)
        .context("reward pubkey parse")?
        .to_bytes();
    let commission = rw.commission.parse::<u8>().ok();

    Ok(CompactReward {
        pubkey: index.compact(&pk),
        lamports: rw.lamports,
        post_balance: rw.post_balance,
        reward_type: rw.reward_type,
        commission,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_transaction_error_decodes_stored_wincode_bytes() {
        let bytes = wincode::serialize(&StoredTransactionError::InstructionError(
            0,
            StoredInstructionError::Custom(0),
        ))
        .expect("serialize stored transaction error");

        let compact = CompactTransactionError::from_stored_wincode_bytes(&bytes)
            .expect("decode stored transaction error");

        assert!(matches!(
            compact,
            CompactTransactionError::InstructionError(0, CompactInstructionError::Custom(0))
        ));
    }

    #[test]
    fn compact_transaction_error_decodes_legacy_unit_borsh_io_error() {
        let bytes = [
            8, 0, 0, 0, // StoredTransactionError::InstructionError
            7, // instruction index
            44, 0, 0, 0, // StoredInstructionError::BorshIoError as old unit variant
        ];

        let compact = CompactTransactionError::from_stored_wincode_bytes(&bytes)
            .expect("decode legacy stored transaction error");

        assert!(matches!(
            compact,
            CompactTransactionError::InstructionError(
                7,
                CompactInstructionError::BorshIoError(ref message)
            ) if message.is_empty()
        ));
    }
}
