//! JSON `meta` object written by [`stream_metadata_value`].
//!
//! This is the native entry point for the same renderer the get-block worker
//! calls. Public keys are resolved by the caller. Logs are rendered as text.

use std::future::Future;
use std::io::{self, Write};

use blockzilla_compact::{
    CompactInnerInstructions, CompactInstructionError as InstructionError, CompactLogStream,
    CompactMetaV1, CompactReturnData, CompactReward, CompactTransactionError as StoredTransactionError,
    LogEvent,
};
use blockzilla_primitives::CompactPubkey;
use blockzilla_program_logs::{program_logs, program_logs::ProgramLog};
use blockzilla_registry::KeyStore;
use serde::Serialize;

const LOG_MESSAGES_BYTES_LIMIT: usize = 10 * 1000;

pub trait MetadataJsonWrite: Write {
    fn raw(&mut self, bytes: &[u8]);

    fn string(&mut self, value: &str) -> Result<(), String> {
        serde_json::to_writer(self, value).map_err(|err| err.to_string())
    }

    fn json<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), String> {
        serde_json::to_writer(self, value).map_err(|err| err.to_string())
    }

    fn u64(&mut self, value: u64) {
        let mut buffer = itoa::Buffer::new();
        self.raw(buffer.format(value).as_bytes());
    }

    fn i64(&mut self, value: i64) {
        let mut buffer = itoa::Buffer::new();
        self.raw(buffer.format(value).as_bytes());
    }

    fn option_u64(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            self.u64(value);
        } else {
            self.raw(b"null");
        }
    }

    fn option_u8(&mut self, value: Option<u8>) {
        if let Some(value) = value {
            self.u64(u64::from(value));
        } else {
            self.raw(b"null");
        }
    }

    fn u8_array(&mut self, values: &[u8]) {
        self.raw(b"[");
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                self.raw(b",");
            }
            self.u64(u64::from(*value));
        }
        self.raw(b"]");
    }

    fn u64_array(&mut self, values: &[u64]) {
        self.raw(b"[");
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                self.raw(b",");
            }
            self.u64(*value);
        }
        self.raw(b"]");
    }

    fn base58_32(&mut self, bytes: &[u8; 32]) {
        let mut buf = [0u8; five8::BASE58_ENCODED_32_MAX_LEN];
        let len = five8::encode_32(bytes, &mut buf) as usize;
        self.raw(b"\"");
        self.raw(&buf[..len]);
        self.raw(b"\"");
    }
}

pub trait MetadataPubkeyResolver {
    fn resolve_pubkey_bytes(
        &mut self,
        key: CompactPubkey,
    ) -> impl Future<Output = Result<[u8; 32], String>>;
}

pub struct ByteWriter<'a>(pub &'a mut Vec<u8>);

impl Write for ByteWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl MetadataJsonWrite for ByteWriter<'_> {
    fn raw(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

pub async fn stream_metadata_value<W, R>(
    w: &mut W,
    resolver: &mut R,
    meta: Option<&CompactMetaV1>,
    include_rewards: bool,
) -> Result<(), String>
where
    W: MetadataJsonWrite,
    R: MetadataPubkeyResolver,
{
    let Some(meta) = meta else {
        w.raw(b"null");
        return Ok(());
    };

    let decoded_err = meta.err.as_ref();

    w.raw(b"{\"err\":");
    stream_optional_transaction_error(w, decoded_err)?;
    w.raw(b",\"status\":");
    stream_transaction_status(w, decoded_err)?;
    w.raw(b",\"fee\":");
    w.u64(meta.fee);
    w.raw(b",\"preBalances\":");
    w.u64_array(&meta.pre_balances);
    w.raw(b",\"postBalances\":");
    w.u64_array(&meta.post_balances);
    w.raw(b",\"preTokenBalances\":");
    stream_token_balances_value(w, resolver, &meta.pre_token_balances).await?;
    w.raw(b",\"postTokenBalances\":");
    stream_token_balances_value(w, resolver, &meta.post_token_balances).await?;
    w.raw(b",\"logMessages\":");
    stream_log_messages_value(w, resolver, meta.logs.as_ref()).await?;
    w.raw(b",\"innerInstructions\":");
    stream_inner_instructions_value(w, meta.inner_instructions.as_deref());
    w.raw(b",\"loadedAddresses\":{\"writable\":");
    stream_compact_pubkey_array_value(w, resolver, &meta.loaded_writable_addresses).await?;
    w.raw(b",\"readonly\":");
    stream_compact_pubkey_array_value(w, resolver, &meta.loaded_readonly_addresses).await?;
    w.raw(b"}");
    if let Some(return_data) = &meta.return_data {
        w.raw(b",\"returnData\":");
        stream_return_data_value(w, resolver, return_data).await?;
    }
    w.raw(b",\"rewards\":");
    if include_rewards {
        stream_compact_rewards_value(w, resolver, &meta.rewards).await?;
    } else {
        w.raw(b"null");
    }
    if let Some(value) = meta.compute_units_consumed {
        w.raw(b",\"computeUnitsConsumed\":");
        w.u64(value);
    }
    if let Some(value) = meta.cost_units {
        w.raw(b",\"costUnits\":");
        w.u64(value);
    }
    w.raw(b"}");
    Ok(())
}

fn stream_optional_transaction_error<W: MetadataJsonWrite>(
    w: &mut W,
    err: Option<&StoredTransactionError>,
) -> Result<(), String> {
    if let Some(err) = err {
        stream_stored_transaction_error(w, err)
    } else {
        w.raw(b"null");
        Ok(())
    }
}

fn stream_transaction_status<W: MetadataJsonWrite>(
    w: &mut W,
    err: Option<&StoredTransactionError>,
) -> Result<(), String> {
    if let Some(err) = err {
        w.raw(b"{\"Err\":");
        stream_stored_transaction_error(w, err)?;
        w.raw(b"}");
    } else {
        w.raw(br#"{"Ok":null}"#);
    }
    Ok(())
}

fn stream_stored_transaction_error<W: MetadataJsonWrite>(
    w: &mut W,
    err: &StoredTransactionError,
) -> Result<(), String> {
    match err {
        StoredTransactionError::AccountInUse => w.string("AccountInUse"),
        StoredTransactionError::AccountLoadedTwice => w.string("AccountLoadedTwice"),
        StoredTransactionError::AccountNotFound => w.string("AccountNotFound"),
        StoredTransactionError::ProgramAccountNotFound => w.string("ProgramAccountNotFound"),
        StoredTransactionError::InsufficientFundsForFee => w.string("InsufficientFundsForFee"),
        StoredTransactionError::InvalidAccountForFee => w.string("InvalidAccountForFee"),
        StoredTransactionError::AlreadyProcessed => w.string("AlreadyProcessed"),
        StoredTransactionError::BlockhashNotFound => w.string("BlockhashNotFound"),
        StoredTransactionError::InstructionError(index, err) => {
            w.raw(b"{\"InstructionError\":[");
            w.u64(u64::from(*index));
            w.raw(b",");
            stream_instruction_error(w, err)?;
            w.raw(b"]}");
            Ok(())
        }
        StoredTransactionError::CallChainTooDeep => w.string("CallChainTooDeep"),
        StoredTransactionError::MissingSignatureForFee => w.string("MissingSignatureForFee"),
        StoredTransactionError::InvalidAccountIndex => w.string("InvalidAccountIndex"),
        StoredTransactionError::SignatureFailure => w.string("SignatureFailure"),
        StoredTransactionError::InvalidProgramForExecution => w.string("InvalidProgramForExecution"),
        StoredTransactionError::SanitizeFailure => w.string("SanitizeFailure"),
        StoredTransactionError::ClusterMaintenance => w.string("ClusterMaintenance"),
        StoredTransactionError::AccountBorrowOutstanding => w.string("AccountBorrowOutstanding"),
        StoredTransactionError::WouldExceedMaxBlockCostLimit => {
            w.string("WouldExceedMaxBlockCostLimit")
        }
        StoredTransactionError::UnsupportedVersion => w.string("UnsupportedVersion"),
        StoredTransactionError::InvalidWritableAccount => w.string("InvalidWritableAccount"),
        StoredTransactionError::WouldExceedMaxAccountCostLimit => {
            w.string("WouldExceedMaxAccountCostLimit")
        }
        StoredTransactionError::WouldExceedAccountDataBlockLimit => {
            w.string("WouldExceedAccountDataBlockLimit")
        }
        StoredTransactionError::TooManyAccountLocks => w.string("TooManyAccountLocks"),
        StoredTransactionError::AddressLookupTableNotFound => w.string("AddressLookupTableNotFound"),
        StoredTransactionError::InvalidAddressLookupTableOwner => {
            w.string("InvalidAddressLookupTableOwner")
        }
        StoredTransactionError::InvalidAddressLookupTableData => {
            w.string("InvalidAddressLookupTableData")
        }
        StoredTransactionError::InvalidAddressLookupTableIndex => {
            w.string("InvalidAddressLookupTableIndex")
        }
        StoredTransactionError::InvalidRentPayingAccount => w.string("InvalidRentPayingAccount"),
        StoredTransactionError::WouldExceedMaxVoteCostLimit => w.string("WouldExceedMaxVoteCostLimit"),
        StoredTransactionError::WouldExceedAccountDataTotalLimit => {
            w.string("WouldExceedAccountDataTotalLimit")
        }
        StoredTransactionError::DuplicateInstruction(index) => {
            w.raw(b"{\"DuplicateInstruction\":");
            w.u64(u64::from(*index));
            w.raw(b"}");
            Ok(())
        }
        StoredTransactionError::InsufficientFundsForRent { account_index } => {
            w.raw(b"{\"InsufficientFundsForRent\":{\"account_index\":");
            w.u64(u64::from(*account_index));
            w.raw(b"}}");
            Ok(())
        }
        StoredTransactionError::MaxLoadedAccountsDataSizeExceeded => {
            w.string("MaxLoadedAccountsDataSizeExceeded")
        }
        StoredTransactionError::InvalidLoadedAccountsDataSizeLimit => {
            w.string("InvalidLoadedAccountsDataSizeLimit")
        }
        StoredTransactionError::ResanitizationNeeded => w.string("ResanitizationNeeded"),
        StoredTransactionError::ProgramExecutionTemporarilyRestricted { account_index } => {
            w.raw(b"{\"ProgramExecutionTemporarilyRestricted\":{\"account_index\":");
            w.u64(u64::from(*account_index));
            w.raw(b"}}");
            Ok(())
        }
        StoredTransactionError::UnbalancedTransaction => w.string("UnbalancedTransaction"),
        StoredTransactionError::ProgramCacheHitMaxLimit => w.string("ProgramCacheHitMaxLimit"),
        StoredTransactionError::CommitCancelled => w.string("CommitCancelled"),
    }
}

fn stream_instruction_error<W: MetadataJsonWrite>(
    w: &mut W,
    err: &InstructionError,
) -> Result<(), String> {
    match err {
        InstructionError::GenericError => w.string("GenericError"),
        InstructionError::InvalidArgument => w.string("InvalidArgument"),
        InstructionError::InvalidInstructionData => w.string("InvalidInstructionData"),
        InstructionError::InvalidAccountData => w.string("InvalidAccountData"),
        InstructionError::AccountDataTooSmall => w.string("AccountDataTooSmall"),
        InstructionError::InsufficientFunds => w.string("InsufficientFunds"),
        InstructionError::IncorrectProgramId => w.string("IncorrectProgramId"),
        InstructionError::MissingRequiredSignature => w.string("MissingRequiredSignature"),
        InstructionError::AccountAlreadyInitialized => w.string("AccountAlreadyInitialized"),
        InstructionError::UninitializedAccount => w.string("UninitializedAccount"),
        InstructionError::UnbalancedInstruction => w.string("UnbalancedInstruction"),
        InstructionError::ModifiedProgramId => w.string("ModifiedProgramId"),
        InstructionError::ExternalAccountLamportSpend => w.string("ExternalAccountLamportSpend"),
        InstructionError::ExternalAccountDataModified => w.string("ExternalAccountDataModified"),
        InstructionError::ReadonlyLamportChange => w.string("ReadonlyLamportChange"),
        InstructionError::ReadonlyDataModified => w.string("ReadonlyDataModified"),
        InstructionError::DuplicateAccountIndex => w.string("DuplicateAccountIndex"),
        InstructionError::ExecutableModified => w.string("ExecutableModified"),
        InstructionError::RentEpochModified => w.string("RentEpochModified"),
        InstructionError::NotEnoughAccountKeys => w.string("NotEnoughAccountKeys"),
        InstructionError::AccountDataSizeChanged => w.string("AccountDataSizeChanged"),
        InstructionError::AccountNotExecutable => w.string("AccountNotExecutable"),
        InstructionError::AccountBorrowFailed => w.string("AccountBorrowFailed"),
        InstructionError::AccountBorrowOutstanding => w.string("AccountBorrowOutstanding"),
        InstructionError::DuplicateAccountOutOfSync => w.string("DuplicateAccountOutOfSync"),
        InstructionError::Custom(code) => {
            w.raw(b"{\"Custom\":");
            w.u64(u64::from(*code));
            w.raw(b"}");
            Ok(())
        }
        InstructionError::InvalidError => w.string("InvalidError"),
        InstructionError::ExecutableDataModified => w.string("ExecutableDataModified"),
        InstructionError::ExecutableLamportChange => w.string("ExecutableLamportChange"),
        InstructionError::ExecutableAccountNotRentExempt => {
            w.string("ExecutableAccountNotRentExempt")
        }
        InstructionError::UnsupportedProgramId => w.string("UnsupportedProgramId"),
        InstructionError::CallDepth => w.string("CallDepth"),
        InstructionError::MissingAccount => w.string("MissingAccount"),
        InstructionError::ReentrancyNotAllowed => w.string("ReentrancyNotAllowed"),
        InstructionError::MaxSeedLengthExceeded => w.string("MaxSeedLengthExceeded"),
        InstructionError::InvalidSeeds => w.string("InvalidSeeds"),
        InstructionError::InvalidRealloc => w.string("InvalidRealloc"),
        InstructionError::ComputationalBudgetExceeded => w.string("ComputationalBudgetExceeded"),
        InstructionError::PrivilegeEscalation => w.string("PrivilegeEscalation"),
        InstructionError::ProgramEnvironmentSetupFailure => {
            w.string("ProgramEnvironmentSetupFailure")
        }
        InstructionError::ProgramFailedToComplete => w.string("ProgramFailedToComplete"),
        InstructionError::ProgramFailedToCompile => w.string("ProgramFailedToCompile"),
        InstructionError::Immutable => w.string("Immutable"),
        InstructionError::IncorrectAuthority => w.string("IncorrectAuthority"),
        InstructionError::BorshIoError(_) => w.string("BorshIoError"),
        InstructionError::AccountNotRentExempt => w.string("AccountNotRentExempt"),
        InstructionError::InvalidAccountOwner => w.string("InvalidAccountOwner"),
        InstructionError::ArithmeticOverflow => w.string("ArithmeticOverflow"),
        InstructionError::UnsupportedSysvar => w.string("UnsupportedSysvar"),
        InstructionError::IllegalOwner => w.string("IllegalOwner"),
        InstructionError::MaxAccountsDataAllocationsExceeded => {
            w.string("MaxAccountsDataAllocationsExceeded")
        }
        InstructionError::MaxAccountsExceeded => w.string("MaxAccountsExceeded"),
        InstructionError::MaxInstructionTraceLengthExceeded => {
            w.string("MaxInstructionTraceLengthExceeded")
        }
        InstructionError::BuiltinProgramsMustConsumeComputeUnits => {
            w.string("BuiltinProgramsMustConsumeComputeUnits")
        }
    }
}

async fn stream_token_balances_value<W, R>(
    w: &mut W,
    resolver: &mut R,
    balances: &[blockzilla_compact::CompactTokenBalance],
) -> Result<(), String>
where
    W: MetadataJsonWrite,
    R: MetadataPubkeyResolver,
{
    w.raw(b"[");
    for (index, balance) in balances.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        w.raw(b"{\"accountIndex\":");
        w.u64(u64::from(balance.account_index));
        if let Some(mint) = balance.mint {
            w.raw(b",\"mint\":");
            stream_resolved_pubkey_value(w, resolver, mint).await?;
        }
        if let Some(owner) = balance.owner {
            w.raw(b",\"owner\":");
            stream_resolved_pubkey_value(w, resolver, owner).await?;
        }
        if let Some(program_id) = balance.program_id {
            w.raw(b",\"programId\":");
            stream_resolved_pubkey_value(w, resolver, program_id).await?;
        }
        w.raw(b",\"uiTokenAmount\":");
        stream_ui_token_amount_value(w, balance.amount, balance.decimals)?;
        w.raw(b"}");
    }
    w.raw(b"]");
    Ok(())
}

fn stream_ui_token_amount_value<W: MetadataJsonWrite>(
    w: &mut W,
    amount: u64,
    decimals: u8,
) -> Result<(), String> {
    w.raw(b"{\"amount\":");
    w.string(&amount.to_string())?;
    w.raw(b",\"decimals\":");
    w.u64(u64::from(decimals));
    w.raw(b",\"uiAmount\":");
    if amount == 0 {
        w.raw(b"null");
    } else {
        w.json(&ui_amount_number(amount, decimals))?;
    }
    w.raw(b",\"uiAmountString\":");
    w.string(&ui_amount_string(amount, decimals))?;
    w.raw(b"}");
    Ok(())
}

fn ui_amount_number(amount: u64, decimals: u8) -> f64 {
    (amount as f64) / 10f64.powi(i32::from(decimals))
}

fn ui_amount_string(amount: u64, decimals: u8) -> String {
    if decimals == 0 {
        return amount.to_string();
    }
    let decimals = decimals as usize;
    let mut digits = amount.to_string();
    if digits.len() <= decimals {
        let zeros = decimals + 1 - digits.len();
        let mut padded = String::with_capacity(decimals + 1);
        padded.extend(std::iter::repeat_n('0', zeros));
        padded.push_str(&digits);
        digits = padded;
    }
    let split = digits.len() - decimals;
    let (whole, fraction) = digits.split_at(split);
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    }
}

async fn stream_log_messages_value<W, R>(
    w: &mut W,
    resolver: &mut R,
    logs: Option<&CompactLogStream>,
) -> Result<(), String>
where
    W: MetadataJsonWrite,
    R: MetadataPubkeyResolver,
{
    let Some(logs) = logs else {
        w.raw(b"null");
        return Ok(());
    };
    if let Some(rendered) = render_logs_with_resolver(resolver, logs).await {
        w.json(&rendered)?;
    } else {
        w.raw(b"[]");
    }
    Ok(())
}

async fn render_logs_with_resolver<R: MetadataPubkeyResolver>(
    resolver: &mut R,
    logs: &CompactLogStream,
) -> Option<Vec<String>> {
    let mut out = Vec::with_capacity(logs.events.len());
    let st = &logs.strings;
    let dt = &logs.data;
    let empty_store = KeyStore { keys: Vec::new() };

    for event in &logs.events {
        match event {
            LogEvent::Invoke { program, depth } => out.push(format!(
                "Program {} invoke [{}]",
                resolver.resolve_pubkey_bytes(*program).await.ok().map(|bytes| base58_string(&bytes))?,
                depth
            )),
            LogEvent::BpfInvoke { program } => out.push(format!(
                "Call BPF program {}",
                pubkey_text(resolver, *program).await?
            )),
            LogEvent::Consumed { program, used, limit } => out.push(format!(
                "Program {} consumed {} of {} compute units",
                pubkey_text(resolver, *program).await?,
                used,
                limit
            )),
            LogEvent::BpfConsumed { used, limit } => {
                out.push(format!("BPF program consumed {used} of {limit} units"))
            }
            LogEvent::Success { program } => out.push(format!(
                "Program {} success",
                pubkey_text(resolver, *program).await?
            )),
            LogEvent::BpfSuccess { program } => out.push(format!(
                "BPF program {} success",
                pubkey_text(resolver, *program).await?
            )),
            LogEvent::Failure { program, reason } => out.push(format!(
                "Program {} failed: {}",
                pubkey_text(resolver, *program).await?,
                st.resolve(*reason)
            )),
            LogEvent::BpfFailure { program, reason } => out.push(format!(
                "BPF program {} failed {}",
                pubkey_text(resolver, *program).await?,
                st.resolve(*reason)
            )),
            LogEvent::FailureCustomProgramError { program, code } => out.push(format!(
                "Program {} failed: custom program error: 0x{:x}",
                pubkey_text(resolver, *program).await?,
                code
            )),
            LogEvent::BpfFailureCustomProgramError { program, code } => out.push(format!(
                "BPF program {} failed custom program error: 0x{:x}",
                pubkey_text(resolver, *program).await?,
                code
            )),
            LogEvent::FailureInvalidAccountData { program } => out.push(format!(
                "Program {} failed: invalid account data for instruction",
                pubkey_text(resolver, *program).await?
            )),
            LogEvent::BpfFailureInvalidAccountData { program } => out.push(format!(
                "BPF program {} failed invalid account data for instruction",
                pubkey_text(resolver, *program).await?
            )),
            LogEvent::FailureInvalidProgramArgument { program } => out.push(format!(
                "Program {} failed: invalid program argument",
                pubkey_text(resolver, *program).await?
            )),
            LogEvent::BpfFailureInvalidProgramArgument { program } => out.push(format!(
                "BPF program {} failed invalid program argument",
                pubkey_text(resolver, *program).await?
            )),
            LogEvent::FailedToComplete { reason } => {
                out.push(format!("Program failed to complete: {}", st.resolve(*reason)))
            }
            LogEvent::LogTruncated => out.push("Log truncated".to_string()),
            LogEvent::StakeMergingAccounts => out.push("Merging stake accounts".to_string()),
            LogEvent::LoaderUpgradedProgram { program } => out.push(format!(
                "Upgraded program {}",
                pubkey_text(resolver, *program).await?
            )),
            LogEvent::LoaderFinalizedAccount { account } => out.push(format!(
                "Finalized account {}",
                pubkey_text(resolver, *account).await?
            )),
            LogEvent::ProgramLog(log) => {
                let payload = render_program_log_without_pubkey_store(log, &empty_store, st)?;
                if payload.is_empty() {
                    out.push("Program log:".to_string());
                } else {
                    out.push(format!("Program log: {payload}"));
                }
            }
            LogEvent::ProgramLogError { msg } => {
                out.push(format!("Program log: Error: {}", st.resolve(*msg)))
            }
            LogEvent::ProgramIdLog { program, log } => {
                let program = pubkey_text(resolver, *program).await?;
                let payload = render_program_log_without_pubkey_store(log, &empty_store, st)?;
                if payload.is_empty() {
                    out.push(format!("Program {program} log:"));
                } else {
                    out.push(format!("Program {program} log: {payload}"));
                }
            }
            LogEvent::ProgramPlainLog(log) => {
                out.push(render_program_log_without_pubkey_store(log, &empty_store, st)?)
            }
            LogEvent::ProgramAccountNotWritable => {
                out.push("Program account not writeable".to_string())
            }
            LogEvent::ProgramNotUpgradeable => out.push("Program not upgradeable".to_string()),
            LogEvent::ProgramAndProgramDataAccountMismatch => {
                out.push("Program and ProgramData account mismatch".to_string())
            }
            LogEvent::ProgramWasExtendedInThisBlockAlready => {
                out.push("Program was extended in this block already".to_string())
            }
            LogEvent::CustomProgramError { code } => {
                out.push(format!("custom program error: 0x{:x}", code))
            }
            LogEvent::Return { program, data } => out.push(format!(
                "Program return: {} {}",
                pubkey_text(resolver, *program).await?,
                dt.render(*data),
            )),
            LogEvent::Data { data } => out.push(format!("Program data: {}", dt.render(*data))),
            LogEvent::Consumption { units } => {
                out.push(format!("Program consumption: {} units remaining", units))
            }
            LogEvent::CbRequestUnits { units } => out.push(format!(
                "Program ComputeBudget111111111111111111111111111111 request units {}",
                units
            )),
            LogEvent::ProgramNotDeployed { program } => {
                if let Some(program) = program {
                    out.push(format!(
                        "Program {} is not deployed",
                        pubkey_text(resolver, *program).await?
                    ));
                } else {
                    out.push("Program is not deployed".to_string());
                }
            }
            LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    out.push(format!(
                        "Program {} is not cached",
                        pubkey_text(resolver, *program).await?
                    ));
                } else {
                    out.push("Program is not cached".to_string());
                }
            }
            LogEvent::UnknownProgram { program } => {
                out.push(format!("Unknown program {}", st.resolve(*program)))
            }
            LogEvent::UnknownAccount { account } => out.push(format!(
                "Instruction references an unknown account {}",
                st.resolve(*account)
            )),
            LogEvent::VerifyEd25519 => out.push("VerifyEd25519".to_string()),
            LogEvent::VerifySecp256k1 => out.push("VerifySecp256k1".to_string()),
            LogEvent::RuntimeWritablePrivilegeEscalated { account } => out.push(format!(
                "{}'s writable privilege escalated",
                pubkey_text(resolver, *account).await?
            )),
            LogEvent::RuntimeSignerPrivilegeEscalated { account } => out.push(format!(
                "{}'s signer privilege escalated",
                pubkey_text(resolver, *account).await?
            )),
            LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => out.push(format!(
                "failed to verify account {} instruction spent from the balance of an account it does not own",
                pubkey_text(resolver, *account).await?
            )),
            LogEvent::CloseContextState => out.push("CloseContextState".to_string()),
            LogEvent::Plain { text } | LogEvent::Unparsed { text } => {
                out.push(st.resolve(*text).to_string())
            }
            LogEvent::ProgramIdMismatch => out.push("Program id mismatch".to_string()),
            LogEvent::System(system_log) => {
                out.push(render_system_log_without_pubkey_store(system_log, st)?)
            }
        }
    }

    Some(apply_log_messages_byte_limit(out))
}

async fn pubkey_text<R: MetadataPubkeyResolver>(
    resolver: &mut R,
    key: CompactPubkey,
) -> Option<String> {
    resolver
        .resolve_pubkey_bytes(key)
        .await
        .ok()
        .map(|bytes| base58_string(&bytes))
}

fn base58_string(bytes: &[u8; 32]) -> String {
    let mut buf = [0u8; five8::BASE58_ENCODED_32_MAX_LEN];
    let len = five8::encode_32(bytes, &mut buf) as usize;
    String::from_utf8(buf[..len].to_vec()).expect("base58 alphabet is UTF-8")
}

fn apply_log_messages_byte_limit(messages: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(messages.len());
    let mut bytes_written = 0usize;
    let mut limit_warning = false;

    for message in messages {
        let next_bytes_written = bytes_written.saturating_add(message.len());
        if next_bytes_written >= LOG_MESSAGES_BYTES_LIMIT {
            if !limit_warning {
                limit_warning = true;
                out.push("Log truncated".to_string());
            }
        } else {
            bytes_written = next_bytes_written;
            out.push(message);
        }
    }

    out
}

fn render_program_log_without_pubkey_store(
    log: &ProgramLog,
    empty_store: &KeyStore,
    strings: &blockzilla_primitives::StringTable,
) -> Option<String> {
    if let ProgramLog::Memo(
        blockzilla_program_logs::program_logs::memo::MemoLog::MemoLenAndDebug { len, memo },
    ) = log
    {
        let memo = strings.resolve(*memo);
        let memo = if memo.starts_with('"') && memo.ends_with('"') {
            memo.to_string()
        } else {
            format!("{memo:?}")
        };
        return Some(format!("Memo (len {}): {}", strings.resolve(*len), memo));
    }
    Some(program_logs::render_program_log(log, empty_store, strings))
}

fn render_system_log_without_pubkey_store(
    log: &blockzilla_program_logs::program_logs::system_program::SystemProgramLog,
    strings: &blockzilla_primitives::StringTable,
) -> Option<String> {
    use blockzilla_program_logs::{
        program_logs::system_program::SystemInstructionLog, program_logs::system_program::SystemProgramLog,
    };

    Some(match log {
        SystemProgramLog::Instruction(instruction) => match instruction {
            SystemInstructionLog::RevokePendingActivation => {
                "Instruction: RevokePendingActivation".to_string()
            }
        },
        SystemProgramLog::AllocateRequestedTooLarge { requested, max_allowed } => format!(
            "Allocate: requested {}, max allowed {}",
            requested, max_allowed
        ),
        SystemProgramLog::TransferFromMustNotCarryData => {
            "Transfer: `from` must not carry data".to_string()
        }
        SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions { limit } => format!(
            "SystemProgram::CreateAccount data size limited to {} in inner instructions",
            limit
        ),
        SystemProgramLog::TransferInsufficient { have, need } => {
            format!("Transfer: insufficient lamports {}, need {}", have, need)
        }
        SystemProgramLog::AdvanceNonceRecentBlockhashesEmpty => {
            "Advance nonce account: recent blockhash list is empty".to_string()
        }
        SystemProgramLog::InitializeNonceRecentBlockhashesEmpty => {
            "Initialize nonce account: recent blockhash list is empty".to_string()
        }
        SystemProgramLog::AuthorizeNonceAccount { msg } => {
            format!("Authorize nonce account: {}", strings.resolve(*msg))
        }
        SystemProgramLog::NonceInsufficientLamports { action, have, need } => format!(
            "{} nonce account: insufficient lamports {}, need {}",
            nonce_action_name(*action),
            have,
            need
        ),
        SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot { action } => format!(
            "{} nonce account: nonce can only advance once per slot",
            nonce_action_name(*action)
        ),
        _ => return None,
    })
}

fn nonce_action_name(
    action: blockzilla_program_logs::program_logs::system_program::NonceAction,
) -> &'static str {
    use blockzilla_program_logs::program_logs::system_program::NonceAction;

    match action {
        NonceAction::Advance => "Advance",
        NonceAction::Withdraw => "Withdraw",
        NonceAction::Initialize => "Initialize",
        NonceAction::Authorize => "Authorize",
    }
}

fn stream_inner_instructions_value<W: MetadataJsonWrite>(
    w: &mut W,
    inner_instructions: Option<&[CompactInnerInstructions]>,
) {
    let Some(inner_instructions) = inner_instructions else {
        w.raw(b"null");
        return;
    };
    w.raw(b"[");
    for (index, group) in inner_instructions.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        w.raw(b"{\"index\":");
        w.u64(u64::from(group.index));
        w.raw(b",\"instructions\":[");
        for (instruction_index, instruction) in group.instructions.iter().enumerate() {
            if instruction_index > 0 {
                w.raw(b",");
            }
            w.raw(b"{\"programIdIndex\":");
            w.u64(u64::from(instruction.program_id_index));
            w.raw(b",\"accounts\":");
            w.u8_array(&instruction.accounts);
            w.raw(b",\"data\":");
            let data = bs58::encode(&instruction.data).into_string();
            w.raw(b"\"");
            w.raw(data.as_bytes());
            w.raw(b"\"");
            w.raw(b",\"stackHeight\":");
            w.option_u64(instruction.stack_height.map(u64::from));
            w.raw(b"}");
        }
        w.raw(b"]}");
    }
    w.raw(b"]");
}

async fn stream_compact_pubkey_array_value<W, R>(
    w: &mut W,
    resolver: &mut R,
    keys: &[CompactPubkey],
) -> Result<(), String>
where
    W: MetadataJsonWrite,
    R: MetadataPubkeyResolver,
{
    w.raw(b"[");
    for (index, key) in keys.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        stream_resolved_pubkey_value(w, resolver, *key).await?;
    }
    w.raw(b"]");
    Ok(())
}

async fn stream_resolved_pubkey_value<W, R>(
    w: &mut W,
    resolver: &mut R,
    key: CompactPubkey,
) -> Result<(), String>
where
    W: MetadataJsonWrite,
    R: MetadataPubkeyResolver,
{
    let bytes = resolver.resolve_pubkey_bytes(key).await?;
    w.base58_32(&bytes);
    Ok(())
}

async fn stream_return_data_value<W, R>(
    w: &mut W,
    resolver: &mut R,
    return_data: &CompactReturnData,
) -> Result<(), String>
where
    W: MetadataJsonWrite,
    R: MetadataPubkeyResolver,
{
    use base64::Engine as _;
    w.raw(b"{\"programId\":");
    stream_resolved_pubkey_value(w, resolver, return_data.program_id).await?;
    w.raw(b",\"data\":[");
    w.string(&base64::engine::general_purpose::STANDARD.encode(&return_data.data))?;
    w.raw(br#","base64"]}"#);
    Ok(())
}

async fn stream_compact_rewards_value<W, R>(
    w: &mut W,
    resolver: &mut R,
    rewards: &[CompactReward],
) -> Result<(), String>
where
    W: MetadataJsonWrite,
    R: MetadataPubkeyResolver,
{
    if rewards.is_empty() {
        w.raw(b"[]");
        return Ok(());
    }
    w.raw(b"[");
    for (index, reward) in rewards.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        w.raw(b"{\"pubkey\":");
        stream_resolved_pubkey_value(w, resolver, reward.pubkey).await?;
        w.raw(b",\"lamports\":");
        w.i64(reward.lamports);
        w.raw(b",\"postBalance\":");
        w.u64(reward.post_balance);
        w.raw(b",\"rewardType\":");
        match reward.reward_type {
            1 => w.raw(br#""Fee""#),
            2 => w.raw(br#""Rent""#),
            3 => w.raw(br#""Staking""#),
            4 => w.raw(br#""Voting""#),
            _ => w.raw(b"null"),
        }
        w.raw(b",\"commission\":");
        w.option_u8(reward.commission);
        w.raw(b"}");
    }
    w.raw(b"]");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_compact::{CompactLogStream, CompactTransactionError, DataTable};
    use blockzilla_primitives::StringTable;

    struct RawResolver;

    impl MetadataPubkeyResolver for RawResolver {
        async fn resolve_pubkey_bytes(
            &mut self,
            key: CompactPubkey,
        ) -> Result<[u8; 32], String> {
            match key {
                CompactPubkey::Raw(bytes) => Ok(bytes),
                CompactPubkey::Id(id) => Err(format!("registry id {id} is not loaded")),
            }
        }
    }

    #[test]
    fn rendered_metadata_uses_balances_fee_and_log_text() {
        let metadata = CompactMetaV1 {
            err: Some(CompactTransactionError::AlreadyProcessed),
            fee: 5_000,
            pre_balances: vec![3, 4],
            post_balances: vec![1],
            inner_instructions: None,
            logs: Some(CompactLogStream {
                events: vec![LogEvent::Invoke {
                    program: CompactPubkey::Raw([1; 32]),
                    depth: 1,
                }],
                strings: StringTable::default(),
                data: DataTable::default(),
            }),
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: vec![CompactPubkey::Raw([2; 32])],
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: Some(12),
            cost_units: None,
        };
        let mut bytes = Vec::new();
        let mut writer = ByteWriter(&mut bytes);
        let mut resolver = RawResolver;
        let mut future = Box::pin(stream_metadata_value(
            &mut writer,
            &mut resolver,
            Some(&metadata),
            true,
        ));
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(&waker);
        let result = match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(result) => result,
            std::task::Poll::Pending => panic!("renderer waited"),
        };
        result.expect("render");
        drop(future);
        drop(writer);
        let json = String::from_utf8(bytes).expect("utf8");
        assert!(json.contains(r#""fee":5000"#));
        assert!(json.contains(r#""err":"AlreadyProcessed""#));
        assert!(json.contains(r#""preBalances":[3,4]"#));
        assert!(json.contains("invoke [1]"));
        assert!(json.contains(r#""computeUnitsConsumed":12"#));
        assert!(!json.contains("\"Id\""));
    }
}
