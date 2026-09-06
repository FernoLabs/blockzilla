//! Mechanical mapping from Compact V2 errors to the archive-owned schema.

use blockzilla_archive_v3::runtime::outcomes::{InstructionFailure, TransactionFailure};
use blockzilla_compact::{CompactInstructionError, CompactTransactionError};

pub(crate) fn transaction(value: &CompactTransactionError) -> TransactionFailure {
    use CompactTransactionError as S;
    use TransactionFailure as T;
    match value {
        S::AccountInUse => T::AccountInUse,
        S::AccountLoadedTwice => T::AccountLoadedTwice,
        S::AccountNotFound => T::AccountNotFound,
        S::ProgramAccountNotFound => T::ProgramAccountNotFound,
        S::InsufficientFundsForFee => T::InsufficientFundsForFee,
        S::InvalidAccountForFee => T::InvalidAccountForFee,
        S::AlreadyProcessed => T::AlreadyProcessed,
        S::BlockhashNotFound => T::BlockhashNotFound,
        S::InstructionError(index, error) => T::InstructionError(*index, instruction(error)),
        S::CallChainTooDeep => T::CallChainTooDeep,
        S::MissingSignatureForFee => T::MissingSignatureForFee,
        S::InvalidAccountIndex => T::InvalidAccountIndex,
        S::SignatureFailure => T::SignatureFailure,
        S::InvalidProgramForExecution => T::InvalidProgramForExecution,
        S::SanitizeFailure => T::SanitizeFailure,
        S::ClusterMaintenance => T::ClusterMaintenance,
        S::AccountBorrowOutstanding => T::AccountBorrowOutstanding,
        S::WouldExceedMaxBlockCostLimit => T::WouldExceedMaxBlockCostLimit,
        S::UnsupportedVersion => T::UnsupportedVersion,
        S::InvalidWritableAccount => T::InvalidWritableAccount,
        S::WouldExceedMaxAccountCostLimit => T::WouldExceedMaxAccountCostLimit,
        S::WouldExceedAccountDataBlockLimit => T::WouldExceedAccountDataBlockLimit,
        S::TooManyAccountLocks => T::TooManyAccountLocks,
        S::AddressLookupTableNotFound => T::AddressLookupTableNotFound,
        S::InvalidAddressLookupTableOwner => T::InvalidAddressLookupTableOwner,
        S::InvalidAddressLookupTableData => T::InvalidAddressLookupTableData,
        S::InvalidAddressLookupTableIndex => T::InvalidAddressLookupTableIndex,
        S::InvalidRentPayingAccount => T::InvalidRentPayingAccount,
        S::WouldExceedMaxVoteCostLimit => T::WouldExceedMaxVoteCostLimit,
        S::WouldExceedAccountDataTotalLimit => T::WouldExceedAccountDataTotalLimit,
        S::DuplicateInstruction(index) => T::DuplicateInstruction(*index),
        S::InsufficientFundsForRent { account_index } => T::InsufficientFundsForRent {
            account_index: *account_index,
        },
        S::MaxLoadedAccountsDataSizeExceeded => T::MaxLoadedAccountsDataSizeExceeded,
        S::InvalidLoadedAccountsDataSizeLimit => T::InvalidLoadedAccountsDataSizeLimit,
        S::ResanitizationNeeded => T::ResanitizationNeeded,
        S::ProgramExecutionTemporarilyRestricted { account_index } => {
            T::ProgramExecutionTemporarilyRestricted {
                account_index: *account_index,
            }
        }
        S::UnbalancedTransaction => T::UnbalancedTransaction,
        S::ProgramCacheHitMaxLimit => T::ProgramCacheHitMaxLimit,
        S::CommitCancelled => T::CommitCancelled,
    }
}

fn instruction(value: &CompactInstructionError) -> InstructionFailure {
    use CompactInstructionError as S;
    use InstructionFailure as T;
    match value {
        S::GenericError => T::GenericError,
        S::InvalidArgument => T::InvalidArgument,
        S::InvalidInstructionData => T::InvalidInstructionData,
        S::InvalidAccountData => T::InvalidAccountData,
        S::AccountDataTooSmall => T::AccountDataTooSmall,
        S::InsufficientFunds => T::InsufficientFunds,
        S::IncorrectProgramId => T::IncorrectProgramId,
        S::MissingRequiredSignature => T::MissingRequiredSignature,
        S::AccountAlreadyInitialized => T::AccountAlreadyInitialized,
        S::UninitializedAccount => T::UninitializedAccount,
        S::UnbalancedInstruction => T::UnbalancedInstruction,
        S::ModifiedProgramId => T::ModifiedProgramId,
        S::ExternalAccountLamportSpend => T::ExternalAccountLamportSpend,
        S::ExternalAccountDataModified => T::ExternalAccountDataModified,
        S::ReadonlyLamportChange => T::ReadonlyLamportChange,
        S::ReadonlyDataModified => T::ReadonlyDataModified,
        S::DuplicateAccountIndex => T::DuplicateAccountIndex,
        S::ExecutableModified => T::ExecutableModified,
        S::RentEpochModified => T::RentEpochModified,
        S::NotEnoughAccountKeys => T::NotEnoughAccountKeys,
        S::AccountDataSizeChanged => T::AccountDataSizeChanged,
        S::AccountNotExecutable => T::AccountNotExecutable,
        S::AccountBorrowFailed => T::AccountBorrowFailed,
        S::AccountBorrowOutstanding => T::AccountBorrowOutstanding,
        S::DuplicateAccountOutOfSync => T::DuplicateAccountOutOfSync,
        S::Custom(code) => T::Custom(*code),
        S::InvalidError => T::InvalidError,
        S::ExecutableDataModified => T::ExecutableDataModified,
        S::ExecutableLamportChange => T::ExecutableLamportChange,
        S::ExecutableAccountNotRentExempt => T::ExecutableAccountNotRentExempt,
        S::UnsupportedProgramId => T::UnsupportedProgramId,
        S::CallDepth => T::CallDepth,
        S::MissingAccount => T::MissingAccount,
        S::ReentrancyNotAllowed => T::ReentrancyNotAllowed,
        S::MaxSeedLengthExceeded => T::MaxSeedLengthExceeded,
        S::InvalidSeeds => T::InvalidSeeds,
        S::InvalidRealloc => T::InvalidRealloc,
        S::ComputationalBudgetExceeded => T::ComputationalBudgetExceeded,
        S::PrivilegeEscalation => T::PrivilegeEscalation,
        S::ProgramEnvironmentSetupFailure => T::ProgramEnvironmentSetupFailure,
        S::ProgramFailedToComplete => T::ProgramFailedToComplete,
        S::ProgramFailedToCompile => T::ProgramFailedToCompile,
        S::Immutable => T::Immutable,
        S::IncorrectAuthority => T::IncorrectAuthority,
        S::BorshIoError(text) => T::BorshIoError(text.clone()),
        S::AccountNotRentExempt => T::AccountNotRentExempt,
        S::InvalidAccountOwner => T::InvalidAccountOwner,
        S::ArithmeticOverflow => T::ArithmeticOverflow,
        S::UnsupportedSysvar => T::UnsupportedSysvar,
        S::IllegalOwner => T::IllegalOwner,
        S::MaxAccountsDataAllocationsExceeded => T::MaxAccountsDataAllocationsExceeded,
        S::MaxAccountsExceeded => T::MaxAccountsExceeded,
        S::MaxInstructionTraceLengthExceeded => T::MaxInstructionTraceLengthExceeded,
        S::BuiltinProgramsMustConsumeComputeUnits => T::BuiltinProgramsMustConsumeComputeUnits,
    }
}
