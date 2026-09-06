//! `runtime/outcomes.wincode`: transaction outcome and return data.
//!
//! The transaction row's `Outcome` effect bit is the sole owner of whole-record
//! absence. A present record owns the execution result, fee, unit counts, and
//! optional return value. There is no separate return-data object.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    ledger::transactions::PubkeyId,
    wincode::{self as wire, ArchiveWincodeConfig},
};

pub const PATH: &str = "runtime/outcomes.wincode";
pub const SCHEMA: u16 = 1;
pub const MAX_ERROR_TEXT_LEN: usize = 64 << 10;
pub const MAX_RETURN_DATA_LEN: usize = 1 << 20;

/// One present transaction outcome. `error == None` is success.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct TransactionOutcome {
    pub error: Option<TransactionFailure>,
    pub fee: u64,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
    pub return_data: Option<ReturnData>,
}

/// Compatibility name for readers that used the old effect type name.
pub type Outcome = TransactionOutcome;

/// A returned byte string. `Some` with empty `data` is distinct from `None`.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ReturnData {
    pub program_id: PubkeyId,
    pub data: Vec<u8>,
}

/// Stable archive-owned transaction failure schema.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum TransactionFailure {
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    AlreadyProcessed,
    BlockhashNotFound,
    InstructionError(u8, InstructionFailure),
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

/// Stable archive-owned instruction failure schema.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum InstructionFailure {
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

impl TransactionOutcome {
    pub fn validate(&self) -> Result<(), OutcomeError> {
        if let Some(ReturnData { program_id, .. }) = &self.return_data
            && program_id.0 == 0
        {
            return Err(OutcomeError::ReservedPubkeyId);
        }
        if let Some(ReturnData { data, .. }) = &self.return_data
            && data.len() > MAX_RETURN_DATA_LEN
        {
            return Err(OutcomeError::ReturnDataTooLong(data.len()));
        }
        if let Some(TransactionFailure::InstructionError(_, InstructionFailure::BorshIoError(text))) =
            &self.error
            && text.len() > MAX_ERROR_TEXT_LEN
        {
            return Err(OutcomeError::ErrorTextTooLong(text.len()));
        }
        Ok(())
    }
}

/// Append one dense outcome record to an uncompressed effect chunk.
pub fn append_record(chunk: &mut Vec<u8>, record: &TransactionOutcome) -> Result<(), OutcomeError> {
    record.validate()?;
    wincode::config::serialize_into(chunk, record, wire::archive_wincode_config())?;
    Ok(())
}

pub fn encode_record(record: &TransactionOutcome) -> Result<Vec<u8>, OutcomeError> {
    let mut bytes = Vec::new();
    append_record(&mut bytes, record)?;
    Ok(bytes)
}

/// Decode a chunk with the exact dense record count from `EffectState` rank.
pub fn decode_chunk(
    bytes: &[u8],
    record_count: u32,
) -> Result<Vec<TransactionOutcome>, OutcomeError> {
    let mut remaining = bytes;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record =
            <TransactionOutcome as SchemaRead<'_, ArchiveWincodeConfig>>::get(&mut remaining)?;
        record.validate()?;
        records.push(record);
    }
    if !remaining.is_empty() {
        return Err(OutcomeError::TrailingBytes(remaining.len()));
    }
    Ok(records)
}

#[derive(Debug, Error)]
pub enum OutcomeError {
    #[error("outcome Wincode: {0}")]
    WincodeRead(#[from] wincode::ReadError),
    #[error("outcome Wincode: {0}")]
    WincodeWrite(#[from] wincode::WriteError),
    #[error("pubkey ID zero is reserved")]
    ReservedPubkeyId,
    #[error("return data has {0} bytes, above the decode guard")]
    ReturnDataTooLong(usize),
    #[error("instruction error text has {0} bytes, above the decode guard")]
    ErrorTextTooLong(usize),
    #[error("outcome chunk has {0} trailing bytes")]
    TrailingBytes(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome() -> TransactionOutcome {
        TransactionOutcome {
            error: Some(TransactionFailure::InstructionError(
                2,
                InstructionFailure::Custom(300),
            )),
            fee: 5_000,
            compute_units_consumed: Some(200_000),
            cost_units: None,
            return_data: Some(ReturnData {
                program_id: PubkeyId(9),
                data: vec![0xaa, 0xbb],
            }),
        }
    }

    #[test]
    fn outcome_and_return_data_have_one_round_trip() {
        let record = outcome();
        let bytes = encode_record(&record).unwrap();
        assert_eq!(decode_chunk(&bytes, 1).unwrap(), [record]);
    }

    #[test]
    fn absent_return_and_present_empty_return_are_distinct() {
        let mut absent = outcome();
        absent.return_data = None;
        let mut empty = outcome();
        empty.return_data.as_mut().unwrap().data.clear();
        assert_ne!(
            encode_record(&absent).unwrap(),
            encode_record(&empty).unwrap()
        );
    }

    #[test]
    fn golden_bytes_freeze_merged_layout() {
        assert_eq!(
            encode_record(&outcome()).unwrap(),
            [
                1, 8, 2, 25, 0xac, 2, 0x88, 0x27, 1, 0xc0, 0x9a, 0x0c, 0, 1, 9, 2, 0xaa, 0xbb
            ]
        );
    }
}
