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

use crate::{CompactLogReuse, CompactLogStream};
use blockzilla_primitives::{CompactPubkey};
use blockzilla_registry::{KeyIndex};

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
    match wincode::deserialize_exact::<StoredTransactionError>(bytes) {
        Ok(err) => Ok(err),
        Err(err) => decode_unit_borsh_io_instruction_error(bytes).map_err(|_| {
            anyhow::anyhow!(
                "decode transaction error from {} exact bytes: {err}",
                bytes.len()
            )
        }),
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

/// Reusable allocation storage for protobuf metadata compaction.
///
/// Keep one value per worker. After the compact metadata has been encoded or
/// otherwise consumed, pass it to [`Self::recycle`]. A later decode can then
/// reuse its top-level vectors, inner-instruction vectors, return-data buffer,
/// and compact-log storage without synchronization.
#[derive(Debug)]
pub struct CompactMetaReuse {
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Vec<CompactInnerInstructions>,
    inner_instruction_lists: Vec<Vec<CompactInnerInstruction>>,
    inner_instruction_accounts: Vec<Vec<u8>>,
    inner_instruction_data: Vec<Vec<u8>>,
    log_message_ranges: Vec<(usize, usize)>,
    pre_token_balances: Vec<CompactTokenBalance>,
    post_token_balances: Vec<CompactTokenBalance>,
    rewards: Vec<CompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Vec<u8>,
    logs: CompactLogReuse,
    max_retained_buffer_bytes: usize,
}

/// Default limit for one retained metadata buffer.
///
/// The limit applies to each top-level vector. A nested vector pool is also
/// discarded if its total retained capacity exceeds this limit.
pub const DEFAULT_COMPACT_META_MAX_RETAINED_BUFFER_BYTES: usize = 1024 * 1024;

impl Default for CompactMetaReuse {
    fn default() -> Self {
        Self::with_max_retained_buffer_bytes(DEFAULT_COMPACT_META_MAX_RETAINED_BUFFER_BYTES)
    }
}

impl CompactMetaReuse {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_retained_buffer_bytes(max_retained_buffer_bytes: usize) -> Self {
        Self {
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: Vec::new(),
            inner_instruction_lists: Vec::new(),
            inner_instruction_accounts: Vec::new(),
            inner_instruction_data: Vec::new(),
            log_message_ranges: Vec::new(),
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: Vec::new(),
            logs: CompactLogReuse::with_max_retained_buffer_bytes(max_retained_buffer_bytes),
            max_retained_buffer_bytes,
        }
    }

    /// Returns already-consumed compact metadata to this reuse slot.
    pub fn recycle(&mut self, mut meta: CompactMetaV1) {
        retain_meta_vec(
            &mut self.pre_balances,
            meta.pre_balances,
            self.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.post_balances,
            meta.post_balances,
            self.max_retained_buffer_bytes,
        );
        if let Some(inner_instructions) = meta.inner_instructions.take() {
            self.recycle_inner_instructions(inner_instructions);
        }
        if let Some(logs) = meta.logs.take() {
            self.logs.recycle(logs);
        }
        retain_meta_vec(
            &mut self.pre_token_balances,
            meta.pre_token_balances,
            self.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.post_token_balances,
            meta.post_token_balances,
            self.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.rewards,
            meta.rewards,
            self.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.loaded_writable_addresses,
            meta.loaded_writable_addresses,
            self.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.loaded_readonly_addresses,
            meta.loaded_readonly_addresses,
            self.max_retained_buffer_bytes,
        );
        if let Some(return_data) = meta.return_data.take() {
            retain_meta_vec(
                &mut self.return_data,
                return_data.data,
                self.max_retained_buffer_bytes,
            );
        }
    }

    /// Total capacity, in bytes, currently retained by this reuse slot.
    pub fn retained_capacity_bytes(&self) -> usize {
        let inner_list_bytes = meta_vec_pool_bytes(&self.inner_instruction_lists);
        let inner_account_bytes = meta_vec_pool_bytes(&self.inner_instruction_accounts);
        let inner_data_bytes = meta_vec_pool_bytes(&self.inner_instruction_data);

        meta_vec_bytes(&self.pre_balances)
            .saturating_add(meta_vec_bytes(&self.post_balances))
            .saturating_add(meta_vec_bytes(&self.inner_instructions))
            .saturating_add(inner_list_bytes)
            .saturating_add(inner_account_bytes)
            .saturating_add(inner_data_bytes)
            .saturating_add(meta_vec_bytes(&self.log_message_ranges))
            .saturating_add(meta_vec_bytes(&self.pre_token_balances))
            .saturating_add(meta_vec_bytes(&self.post_token_balances))
            .saturating_add(meta_vec_bytes(&self.rewards))
            .saturating_add(meta_vec_bytes(&self.loaded_writable_addresses))
            .saturating_add(meta_vec_bytes(&self.loaded_readonly_addresses))
            .saturating_add(meta_vec_bytes(&self.return_data))
            .saturating_add(self.logs.retained_capacity_bytes())
    }

    fn recycle_inner_instructions(
        &mut self,
        mut inner_instructions: Vec<CompactInnerInstructions>,
    ) {
        for group in inner_instructions.drain(..) {
            let mut instructions = group.instructions;
            for instruction in instructions.drain(..) {
                retain_meta_vec_in_pool(
                    &mut self.inner_instruction_accounts,
                    instruction.accounts,
                    self.max_retained_buffer_bytes,
                );
                retain_meta_vec_in_pool(
                    &mut self.inner_instruction_data,
                    instruction.data,
                    self.max_retained_buffer_bytes,
                );
            }
            retain_meta_vec_in_pool(
                &mut self.inner_instruction_lists,
                instructions,
                self.max_retained_buffer_bytes,
            );
        }
        retain_meta_vec(
            &mut self.inner_instructions,
            inner_instructions,
            self.max_retained_buffer_bytes,
        );
        trim_meta_vec_pool(
            &mut self.inner_instruction_lists,
            self.max_retained_buffer_bytes,
        );
        trim_meta_vec_pool(
            &mut self.inner_instruction_accounts,
            self.max_retained_buffer_bytes,
        );
        trim_meta_vec_pool(
            &mut self.inner_instruction_data,
            self.max_retained_buffer_bytes,
        );
    }
}

#[inline]
fn meta_vec_bytes<T>(values: &Vec<T>) -> usize {
    values.capacity().saturating_mul(std::mem::size_of::<T>())
}

#[inline]
fn retain_meta_vec<T>(slot: &mut Vec<T>, mut values: Vec<T>, max_bytes: usize) {
    values.clear();
    if meta_vec_bytes(&values) > max_bytes {
        return;
    }
    if values.capacity() > slot.capacity() {
        *slot = values;
    }
}

#[inline]
fn retain_meta_vec_in_pool<T>(pool: &mut Vec<Vec<T>>, mut values: Vec<T>, max_bytes: usize) {
    values.clear();
    if meta_vec_bytes(&values) <= max_bytes {
        pool.push(values);
    }
}

#[inline]
fn trim_meta_vec_pool<T>(pool: &mut Vec<Vec<T>>, max_bytes: usize) {
    if meta_vec_pool_bytes(pool) > max_bytes {
        *pool = Vec::new();
    }
}

#[inline]
fn meta_vec_pool_bytes<T>(pool: &Vec<Vec<T>>) -> usize {
    pool.iter().fold(meta_vec_bytes(pool), |total, values| {
        total.saturating_add(meta_vec_bytes(values))
    })
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
    let mut reuse = CompactMetaReuse::new();
    compact_meta_from_protobuf_visit_reusing(bytes, index, &mut reuse)
}

/// Compacts protobuf metadata with caller-owned reusable allocation storage.
///
/// The returned metadata owns its vectors. Call [`CompactMetaReuse::recycle`]
/// after the metadata has been encoded or otherwise consumed.
pub fn compact_meta_from_protobuf_visit_reusing(
    bytes: &[u8],
    index: &KeyIndex,
    reuse: &mut CompactMetaReuse,
) -> Result<CompactMetaV1> {
    let mut visitor = CompactMetaVisitor::new(bytes, index, reuse);
    visit_protobuf_transaction_status_meta(bytes, &mut visitor)
        .map_err(|err| anyhow::anyhow!("protobuf visit: {err}"))?;
    visitor.finish()
}

const BALANCES_RESERVE: usize = 32;
const INNER_INSTRUCTION_GROUPS_RESERVE: usize = 4;
const INNER_INSTRUCTIONS_PER_GROUP_RESERVE: usize = 4;
const LOG_MESSAGES_RESERVE: usize = 16;
const TOKEN_BALANCES_RESERVE: usize = 8;
const REWARDS_RESERVE: usize = 1;
const LOADED_ADDRESSES_RESERVE: usize = 8;

#[inline]
fn reserve_on_first<T>(values: &mut Vec<T>, additional: usize) {
    if values.capacity() == 0 {
        values.reserve(additional);
    }
}

struct CompactMetaVisitor<'index, 'metadata, 'reuse> {
    metadata: &'metadata [u8],
    index: &'index KeyIndex,
    reuse: &'reuse mut CompactMetaReuse,
    err: Option<CompactTransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Vec<CompactInnerInstructions>,
    inner_instructions_none: bool,
    log_message_ranges: Vec<(usize, usize)>,
    log_messages_none: bool,
    pre_token_balances: Vec<CompactTokenBalance>,
    post_token_balances: Vec<CompactTokenBalance>,
    rewards: Vec<CompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data_program_id: Option<CompactPubkey>,
    return_data: Vec<u8>,
    return_data_none: bool,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
    error: Option<anyhow::Error>,
}

impl<'index, 'metadata, 'reuse> CompactMetaVisitor<'index, 'metadata, 'reuse> {
    fn new(
        metadata: &'metadata [u8],
        index: &'index KeyIndex,
        reuse: &'reuse mut CompactMetaReuse,
    ) -> Self {
        let pre_balances = std::mem::take(&mut reuse.pre_balances);
        let post_balances = std::mem::take(&mut reuse.post_balances);
        let inner_instructions = std::mem::take(&mut reuse.inner_instructions);
        let log_message_ranges = std::mem::take(&mut reuse.log_message_ranges);
        let pre_token_balances = std::mem::take(&mut reuse.pre_token_balances);
        let post_token_balances = std::mem::take(&mut reuse.post_token_balances);
        let rewards = std::mem::take(&mut reuse.rewards);
        let loaded_writable_addresses = std::mem::take(&mut reuse.loaded_writable_addresses);
        let loaded_readonly_addresses = std::mem::take(&mut reuse.loaded_readonly_addresses);
        let return_data = std::mem::take(&mut reuse.return_data);

        Self {
            metadata,
            index,
            reuse,
            err: None,
            fee: 0,
            pre_balances,
            post_balances,
            inner_instructions,
            inner_instructions_none: false,
            log_message_ranges,
            log_messages_none: false,
            pre_token_balances,
            post_token_balances,
            rewards,
            loaded_writable_addresses,
            loaded_readonly_addresses,
            return_data_program_id: None,
            return_data,
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

    fn finish(&mut self) -> Result<CompactMetaV1> {
        if let Some(err) = self.error.take() {
            return Err(err);
        }

        let inner_instructions = if self.inner_instructions_none {
            None
        } else {
            Some(std::mem::take(&mut self.inner_instructions))
        };
        let logs = if self.log_messages_none {
            None
        } else {
            let metadata = self.metadata;
            let lines = self.log_message_ranges.iter().map(|&(start, end)| {
                std::str::from_utf8(&metadata[start..end])
                    .expect("protobuf visitor supplied valid utf-8 log text")
            });
            Some(crate::log::parse_log_iter_with_compactor_reusing(
                lines,
                self.log_message_ranges.len(),
                self.index,
                None,
                &mut self.reuse.logs,
            ))
        };
        let return_data = if self.return_data_none {
            None
        } else {
            self.return_data_program_id
                .take()
                .map(|program_id| CompactReturnData {
                    program_id,
                    data: std::mem::take(&mut self.return_data),
                })
        };

        Ok(CompactMetaV1 {
            err: self.err.take(),
            fee: self.fee,
            pre_balances: std::mem::take(&mut self.pre_balances),
            post_balances: std::mem::take(&mut self.post_balances),
            inner_instructions,
            logs,
            pre_token_balances: std::mem::take(&mut self.pre_token_balances),
            post_token_balances: std::mem::take(&mut self.post_token_balances),
            rewards: std::mem::take(&mut self.rewards),
            loaded_writable_addresses: std::mem::take(&mut self.loaded_writable_addresses),
            loaded_readonly_addresses: std::mem::take(&mut self.loaded_readonly_addresses),
            return_data,
            compute_units_consumed: self.compute_units_consumed,
            cost_units: self.cost_units,
        })
    }
}

impl Drop for CompactMetaVisitor<'_, '_, '_> {
    fn drop(&mut self) {
        self.reuse
            .recycle_inner_instructions(std::mem::take(&mut self.inner_instructions));
        retain_meta_vec(
            &mut self.reuse.pre_balances,
            std::mem::take(&mut self.pre_balances),
            self.reuse.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.reuse.post_balances,
            std::mem::take(&mut self.post_balances),
            self.reuse.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.reuse.log_message_ranges,
            std::mem::take(&mut self.log_message_ranges),
            self.reuse.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.reuse.pre_token_balances,
            std::mem::take(&mut self.pre_token_balances),
            self.reuse.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.reuse.post_token_balances,
            std::mem::take(&mut self.post_token_balances),
            self.reuse.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.reuse.rewards,
            std::mem::take(&mut self.rewards),
            self.reuse.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.reuse.loaded_writable_addresses,
            std::mem::take(&mut self.loaded_writable_addresses),
            self.reuse.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.reuse.loaded_readonly_addresses,
            std::mem::take(&mut self.loaded_readonly_addresses),
            self.reuse.max_retained_buffer_bytes,
        );
        retain_meta_vec(
            &mut self.reuse.return_data,
            std::mem::take(&mut self.return_data),
            self.reuse.max_retained_buffer_bytes,
        );
    }
}

impl<'index, 'metadata, 'reuse> TransactionStatusMetaVisitor<'metadata>
    for CompactMetaVisitor<'index, 'metadata, 'reuse>
{
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
    fn status_error(&mut self, err: &'metadata [u8]) {
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
        reserve_on_first(&mut self.pre_balances, BALANCES_RESERVE);
        self.pre_balances.push(lamports);
    }

    #[inline]
    fn post_balance(&mut self, _index: usize, lamports: u64) {
        reserve_on_first(&mut self.post_balances, BALANCES_RESERVE);
        self.post_balances.push(lamports);
    }

    #[inline]
    fn inner_instruction(&mut self, instruction: InnerInstructionVisit<'metadata>) {
        if self
            .inner_instructions
            .last()
            .is_none_or(|group| group.index != instruction.outer_instruction_index)
        {
            reserve_on_first(
                &mut self.inner_instructions,
                INNER_INSTRUCTION_GROUPS_RESERVE,
            );
            let mut instructions = self
                .reuse
                .inner_instruction_lists
                .pop()
                .unwrap_or_else(|| Vec::with_capacity(INNER_INSTRUCTIONS_PER_GROUP_RESERVE));
            instructions.clear();
            self.inner_instructions.push(CompactInnerInstructions {
                index: instruction.outer_instruction_index,
                instructions,
            });
        }

        let Some(group) = self.inner_instructions.last_mut() else {
            return;
        };
        let mut accounts = self
            .reuse
            .inner_instruction_accounts
            .pop()
            .unwrap_or_default();
        accounts.clear();
        accounts.extend_from_slice(instruction.accounts);
        let mut data = self.reuse.inner_instruction_data.pop().unwrap_or_default();
        data.clear();
        data.extend_from_slice(instruction.data);
        group.instructions.push(CompactInnerInstruction {
            program_id_index: instruction.program_id_index,
            accounts,
            data,
            stack_height: instruction.stack_height,
        });
    }

    #[inline]
    fn inner_instructions_none(&mut self, none: bool) {
        self.inner_instructions_none = none;
    }

    #[inline]
    fn log_message(&mut self, message: &'metadata str) {
        reserve_on_first(&mut self.log_message_ranges, LOG_MESSAGES_RESERVE);
        if message.is_empty() {
            self.log_message_ranges.push((0, 0));
            return;
        }

        let metadata_start = self.metadata.as_ptr() as usize;
        let message_start = message.as_ptr() as usize;
        let Some(start) = message_start.checked_sub(metadata_start) else {
            self.record_error(anyhow::anyhow!(
                "protobuf log text is outside the metadata input"
            ));
            return;
        };
        let Some(end) = start.checked_add(message.len()) else {
            self.record_error(anyhow::anyhow!("protobuf log text range overflow"));
            return;
        };
        if end > self.metadata.len() {
            self.record_error(anyhow::anyhow!(
                "protobuf log text is outside the metadata input"
            ));
            return;
        }
        self.log_message_ranges.push((start, end));
    }

    #[inline]
    fn log_messages_none(&mut self, none: bool) {
        self.log_messages_none = none;
    }

    #[inline]
    fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'metadata>) {
        reserve_on_first(&mut self.pre_token_balances, TOKEN_BALANCES_RESERVE);
        match compact_token_balance_visit(balance, self.index) {
            Ok(balance) => self.pre_token_balances.push(balance),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn post_token_balance(&mut self, balance: TokenBalanceVisit<'metadata>) {
        reserve_on_first(&mut self.post_token_balances, TOKEN_BALANCES_RESERVE);
        match compact_token_balance_visit(balance, self.index) {
            Ok(balance) => self.post_token_balances.push(balance),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn reward_raw(&mut self, bytes: &'metadata [u8]) {
        reserve_on_first(&mut self.rewards, REWARDS_RESERVE);
        match of_car_reader::confirmed_block::Reward::decode(bytes)
            .map_err(anyhow::Error::from)
            .and_then(|reward| compact_reward(&reward, self.index))
        {
            Ok(reward) => self.rewards.push(reward),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn loaded_writable_address(&mut self, address: &'metadata [u8]) {
        reserve_on_first(
            &mut self.loaded_writable_addresses,
            LOADED_ADDRESSES_RESERVE,
        );
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
    fn loaded_readonly_address(&mut self, address: &'metadata [u8]) {
        reserve_on_first(
            &mut self.loaded_readonly_addresses,
            LOADED_ADDRESSES_RESERVE,
        );
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
    fn return_data(&mut self, return_data: ReturnDataVisit<'metadata>) {
        match return_data.program_id.try_into() {
            Ok(program_id) => {
                self.return_data_program_id = Some(self.index.compact(program_id));
                self.return_data.clear();
                self.return_data.extend_from_slice(return_data.data);
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
    use of_car_reader::confirmed_block::{
        InnerInstruction, InnerInstructions, ReturnData, Reward, TokenBalance,
        TransactionStatusMeta, UiTokenAmount,
    };

    fn assert_protobuf_visitor_matches_owned(meta: &TransactionStatusMeta, index: &KeyIndex) {
        let expected = compact_meta_from_proto(meta, index).expect("compact owned metadata");
        let protobuf = meta.encode_to_vec();
        let actual = compact_meta_from_protobuf_visit(&protobuf, index)
            .expect("compact borrowed protobuf metadata");

        assert_eq!(
            wincode::serialize(&actual).expect("serialize borrowed compact metadata"),
            wincode::serialize(&expected).expect("serialize owned compact metadata")
        );
    }

    fn representative_log_metadata() -> (TransactionStatusMeta, KeyIndex) {
        const SYSTEM: &str = "11111111111111111111111111111111";
        const COMPUTE_BUDGET: &str = "ComputeBudget111111111111111111111111111111";
        const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

        let system = Pubkey::from_str(SYSTEM).unwrap().to_bytes();
        let compute_budget = Pubkey::from_str(COMPUTE_BUDGET).unwrap().to_bytes();
        let token = Pubkey::from_str(TOKEN).unwrap().to_bytes();
        let index = KeyIndex::build(vec![system, compute_budget, token]);

        let log_messages = vec![
            format!("Program {COMPUTE_BUDGET} invoke [1]"),
            "Program log: Instruction: RequestUnits".to_owned(),
            format!("Program {COMPUTE_BUDGET} consumed 150 of 200000 compute units"),
            format!("Program {COMPUTE_BUDGET} success"),
            format!("Program {SYSTEM} invoke [1]"),
            "Transfer: insufficient lamports 3, need 5".to_owned(),
            "Program data: AQID BAU=".to_owned(),
            format!("Program return: {SYSTEM} AQID"),
            "Program log: an unstructured payload".to_owned(),
            "plain runtime text".to_owned(),
            format!("Program {SYSTEM} success"),
        ];

        let meta = TransactionStatusMeta {
            fee: 5_000,
            pre_balances: (0..40).map(|value| value * 10).collect(),
            post_balances: (0..40).map(|value| value * 10 + 1).collect(),
            inner_instructions: vec![InnerInstructions {
                index: 2,
                instructions: vec![
                    InnerInstruction {
                        program_id_index: 3,
                        accounts: vec![0, 1, 2],
                        data: vec![9, 8, 7],
                        stack_height: Some(2),
                    },
                    InnerInstruction {
                        program_id_index: 4,
                        accounts: vec![3, 4],
                        data: vec![6, 5],
                        stack_height: None,
                    },
                ],
            }],
            inner_instructions_none: false,
            log_messages,
            log_messages_none: false,
            pre_token_balances: vec![TokenBalance {
                account_index: 1,
                mint: TOKEN.to_owned(),
                ui_token_amount: Some(UiTokenAmount {
                    ui_amount: 42.0,
                    decimals: 6,
                    amount: "42000000".to_owned(),
                    ui_amount_string: "42".to_owned(),
                }),
                owner: SYSTEM.to_owned(),
                program_id: TOKEN.to_owned(),
            }],
            post_token_balances: vec![TokenBalance {
                account_index: 1,
                mint: TOKEN.to_owned(),
                ui_token_amount: Some(UiTokenAmount {
                    ui_amount: 43.0,
                    decimals: 6,
                    amount: "43000000".to_owned(),
                    ui_amount_string: "43".to_owned(),
                }),
                owner: SYSTEM.to_owned(),
                program_id: TOKEN.to_owned(),
            }],
            rewards: vec![Reward {
                pubkey: SYSTEM.to_owned(),
                lamports: 50,
                post_balance: 1_000,
                reward_type: 1,
                commission: "7".to_owned(),
            }],
            loaded_writable_addresses: vec![system.to_vec(), token.to_vec()],
            loaded_readonly_addresses: vec![compute_budget.to_vec()],
            return_data: Some(ReturnData {
                program_id: system.to_vec(),
                data: vec![1, 2, 3, 4],
            }),
            return_data_none: false,
            compute_units_consumed: Some(123_456),
            cost_units: Some(123_999),
            ..TransactionStatusMeta::default()
        };
        (meta, index)
    }

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
    fn compact_transaction_error_rejects_trailing_stored_wincode_bytes() {
        let mut bytes = wincode::serialize(&StoredTransactionError::InstructionError(
            0,
            StoredInstructionError::Custom(0),
        ))
        .expect("serialize stored transaction error");
        bytes.push(0xaa);

        assert!(CompactTransactionError::from_stored_wincode_bytes(&bytes).is_err());
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

    #[test]
    fn borrowed_log_visitor_is_byte_identical_to_owned_metadata_compaction() {
        let (meta, index) = representative_log_metadata();
        assert_protobuf_visitor_matches_owned(&meta, &index);
    }

    #[test]
    fn borrowed_log_visitor_preserves_none_and_empty_log_semantics() {
        let (mut meta, index) = representative_log_metadata();
        meta.log_messages_none = true;
        assert_protobuf_visitor_matches_owned(&meta, &index);

        meta.log_messages.clear();
        meta.log_messages_none = false;
        assert_protobuf_visitor_matches_owned(&meta, &index);
    }

    #[test]
    fn metadata_reuse_preserves_output_and_reuses_nested_allocations() {
        let (meta, index) = representative_log_metadata();
        let protobuf = meta.encode_to_vec();
        let expected =
            compact_meta_from_protobuf_visit(&protobuf, &index).expect("compact expected metadata");
        let expected_bytes = wincode::serialize(&expected).expect("serialize expected metadata");
        let mut reuse = CompactMetaReuse::new();

        let first = compact_meta_from_protobuf_visit_reusing(&protobuf, &index, &mut reuse)
            .expect("first reusable metadata decode");
        assert_eq!(
            wincode::serialize(&first).expect("serialize first reusable metadata"),
            expected_bytes
        );
        let pre_balances_ptr = first.pre_balances.as_ptr();
        let inner_instructions = first
            .inner_instructions
            .as_ref()
            .expect("representative inner instructions");
        let inner_groups_ptr = inner_instructions.as_ptr();
        let inner_list_ptr = inner_instructions[0].instructions.as_ptr();
        let mut account_ptrs = inner_instructions[0]
            .instructions
            .iter()
            .map(|instruction| instruction.accounts.as_ptr() as usize)
            .collect::<Vec<_>>();
        account_ptrs.sort_unstable();
        let mut data_ptrs = inner_instructions[0]
            .instructions
            .iter()
            .map(|instruction| instruction.data.as_ptr() as usize)
            .collect::<Vec<_>>();
        data_ptrs.sort_unstable();
        let log_events_ptr = first
            .logs
            .as_ref()
            .expect("representative logs")
            .events
            .as_ptr();
        let return_data_ptr = first
            .return_data
            .as_ref()
            .expect("representative return data")
            .data
            .as_ptr();
        let log_range_ptr = reuse.log_message_ranges.as_ptr();
        reuse.recycle(first);
        assert!(reuse.retained_capacity_bytes() > 0);

        let second = compact_meta_from_protobuf_visit_reusing(&protobuf, &index, &mut reuse)
            .expect("second reusable metadata decode");
        assert_eq!(
            wincode::serialize(&second).expect("serialize second reusable metadata"),
            expected_bytes
        );
        assert_eq!(second.pre_balances.as_ptr(), pre_balances_ptr);
        let second_inner = second
            .inner_instructions
            .as_ref()
            .expect("second representative inner instructions");
        assert_eq!(second_inner.as_ptr(), inner_groups_ptr);
        assert_eq!(second_inner[0].instructions.as_ptr(), inner_list_ptr);
        let mut second_account_ptrs = second_inner[0]
            .instructions
            .iter()
            .map(|instruction| instruction.accounts.as_ptr() as usize)
            .collect::<Vec<_>>();
        second_account_ptrs.sort_unstable();
        assert_eq!(second_account_ptrs, account_ptrs);
        let mut second_data_ptrs = second_inner[0]
            .instructions
            .iter()
            .map(|instruction| instruction.data.as_ptr() as usize)
            .collect::<Vec<_>>();
        second_data_ptrs.sort_unstable();
        assert_eq!(second_data_ptrs, data_ptrs);
        assert_eq!(
            second.logs.as_ref().expect("second logs").events.as_ptr(),
            log_events_ptr
        );
        assert_eq!(
            second
                .return_data
                .as_ref()
                .expect("second return data")
                .data
                .as_ptr(),
            return_data_ptr
        );
        assert_eq!(reuse.log_message_ranges.as_ptr(), log_range_ptr);
    }

    #[test]
    fn metadata_reuse_discards_oversized_buffers() {
        let (meta, index) = representative_log_metadata();
        let protobuf = meta.encode_to_vec();
        let mut reuse = CompactMetaReuse::with_max_retained_buffer_bytes(32);
        let compact = compact_meta_from_protobuf_visit_reusing(&protobuf, &index, &mut reuse)
            .expect("compact metadata");

        reuse.recycle(compact);

        assert_eq!(reuse.pre_balances.capacity(), 0);
        assert_eq!(reuse.post_balances.capacity(), 0);
        assert!(
            reuse
                .inner_instruction_lists
                .iter()
                .all(|values| meta_vec_bytes(values) <= 32)
        );
        assert!(meta_vec_pool_bytes(&reuse.inner_instruction_lists) <= 32);
        assert!(
            reuse
                .inner_instruction_accounts
                .iter()
                .all(|values| meta_vec_bytes(values) <= 32)
        );
        assert!(meta_vec_pool_bytes(&reuse.inner_instruction_accounts) <= 32);
        assert!(
            reuse
                .inner_instruction_data
                .iter()
                .all(|values| meta_vec_bytes(values) <= 32)
        );
        assert!(meta_vec_pool_bytes(&reuse.inner_instruction_data) <= 32);
    }
}
