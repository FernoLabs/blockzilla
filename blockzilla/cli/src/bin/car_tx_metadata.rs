//! Write one `car_tx_metadata` row per Old Faithful CAR transaction.
//!
//! One thread reads the local CAR in file order and decodes transaction
//! metadata with `transaction_metadata`. The JSON uses the same get-transaction
//! field order as Compact V2 `stream_metadata_value`. The transaction message
//! is not decoded. The table is created beforehand. This binary does not run
//! `CREATE TABLE`.
//!
//! Expected columns, in the database selected by `--database`:
//!
//! ```sql
//! CREATE TABLE car_tx_metadata (
//!     slot UInt64,
//!     tx_index UInt32,
//!     metadata String,
//!     indexed_at DateTime('UTC') DEFAULT now()
//! ) ENGINE = ReplacingMergeTree(indexed_at)
//! ORDER BY (slot, tx_index)
//! ```

use std::fs::File;
use std::io::{BufReader, Read};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use blockzilla_get_block::metadata_json::{ByteWriter, MetadataJsonWrite};
use clap::Parser;
use of_car_reader::confirmed_block::{
    InnerInstruction, InnerInstructions, ReturnData, Reward, TokenBalance, TransactionStatusMeta,
    UiTokenAmount,
};
use of_car_reader::stored_transaction::{InstructionError, StoredTransactionError};
use of_car_reader::{CarBlockGroup, CarBlockReader};

const TABLE: &str = "car_tx_metadata";
const CAR_BUF: usize = 128 << 20;
const LOG_MESSAGES_BYTES_LIMIT: usize = 10 * 1000;
const RETRY_INITIAL: Duration = Duration::from_millis(500);
const RETRY_MAX: Duration = Duration::from_secs(15);
const RETRY_HORIZON: Duration = Duration::from_secs(600);
const DEFAULT_MAX_IN_FLIGHT: u64 = 128;

#[derive(Parser, Debug)]
#[command(
    name = "car-tx-metadata",
    about = "Fill ClickHouse car_tx_metadata from a local Old Faithful CAR"
)]
struct Args {
    /// Local CAR file, `.car` or `.car.zst`.
    #[arg(long)]
    input: PathBuf,
    /// Read at most this many blocks. Omit to scan the file.
    #[arg(long)]
    max_blocks: Option<NonZeroU32>,
    /// ClickHouse HTTP URL. User and password may be included.
    #[arg(long)]
    clickhouse_dsn: String,
    /// Database that receives `car_tx_metadata`.
    #[arg(long)]
    database: String,
    /// Rows per ClickHouse insert. One full batch is one HTTP request.
    #[arg(long, default_value_t = 1000)]
    batch_rows: usize,
    /// ClickHouse HTTP requests allowed at the same time.
    ///
    /// The reader waits when this many batches are still in flight. The cap
    /// counts requests, so it stays independent of `--batch-rows`.
    #[arg(long, default_value_t = DEFAULT_MAX_IN_FLIGHT, value_parser = clap::value_parser!(u64).range(1..))]
    max_in_flight: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut sink = MetadataSink::connect(
        &args.clickhouse_dsn,
        &args.database,
        args.batch_rows,
        args.max_in_flight,
    )?;
    let workers = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(2)
        .max(2);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    let started = Instant::now();
    let scan_result = runtime.block_on(async {
        tokio::task::block_in_place(|| scan(&args, &mut sink))
    });
    let finish_result = runtime.block_on(sink.finish());
    match (scan_result, finish_result) {
        (_, Err(error)) => Err(error),
        (Err(error), Ok(_)) => Err(error),
        (Ok(summary), Ok(written)) => {
            let elapsed = started.elapsed().as_secs_f64().max(1e-9);
            eprintln!(
                "database={} table={TABLE} blocks={} rows={written} transactions={} with_metadata={} without_metadata={} first_slot={} last_slot={} elapsed={elapsed:.1}s",
                args.database,
                summary.blocks,
                summary.transactions,
                summary.with_metadata,
                summary.transactions.saturating_sub(summary.with_metadata),
                summary.first_slot,
                summary.last_slot,
            );
            Ok(())
        }
    }
}

struct ScanSummary {
    blocks: u64,
    transactions: u64,
    with_metadata: u64,
    first_slot: u64,
    last_slot: u64,
}

fn scan(args: &Args, sink: &mut MetadataSink) -> Result<ScanSummary> {
    eprintln!(
        "source={} batch_rows={} max_in_flight={}",
        args.input.display(),
        args.batch_rows,
        args.max_in_flight
    );
    let file = File::open(&args.input)
        .with_context(|| format!("Cannot open {}", args.input.display()))?;
    let file = BufReader::with_capacity(CAR_BUF, file);
    if is_zstd(&args.input) {
        let decoder = zstd::Decoder::with_buffer(file)
            .with_context(|| format!("Cannot decompress {}", args.input.display()))?;
        scan_reader(decoder, args.max_blocks, sink)
    } else {
        scan_reader(file, args.max_blocks, sink)
    }
}

fn scan_reader<R: Read>(
    reader: R,
    max_blocks: Option<NonZeroU32>,
    sink: &mut MetadataSink,
) -> Result<ScanSummary> {
    let limit = max_blocks.map(|value| u64::from(value.get()));
    let mut car = CarBlockReader::with_capacity(reader, CAR_BUF);
    car.skip_header().context("Cannot skip the CAR header")?;
    let mut group = CarBlockGroup::without_rewards();
    let mut blocks = 0u64;
    let mut transactions = 0u64;
    let mut with_metadata = 0u64;
    let mut first_slot = None;
    let mut last_slot = 0u64;
    let mut mark = Instant::now();
    let mut mark_blocks = 0u64;
    let mut mark_transactions = 0u64;

    loop {
        if limit.is_some_and(|limit| blocks >= limit) {
            break;
        }
        let produced = car
            .read_until_block_into(&mut group)
            .context("Cannot read the next CAR block")?;
        if !produced {
            break;
        }
        let slot = group
            .slot
            .context("CAR block has no slot")?;
        if first_slot.is_none() {
            first_slot = Some(slot);
        }
        last_slot = slot;
        blocks += 1;

        let mut metadata = group.transaction_metadata();
        let mut position = 0u32;
        while let Some(tx) = metadata
            .next_metadata()
            .with_context(|| format!("slot {slot} metadata decode failed"))?
        {
            if tx.slot != slot {
                bail!(
                    "transaction {position} slot {} differs from block slot {slot}",
                    tx.slot
                );
            }
            let tx_index = canonical_tx_index(position, tx.index)
                .with_context(|| format!("slot {slot} transaction {position}"))?;
            let rendered = render_metadata(tx.metadata)
                .with_context(|| format!("slot {slot} transaction {tx_index}"))?;
            if rendered != "null" {
                with_metadata += 1;
            }
            transactions += 1;
            sink.queue(TxMetadataRow {
                slot,
                tx_index,
                metadata: rendered,
            })?;
            position = position
                .checked_add(1)
                .context("transaction position exceeds u32")?;
        }

        if blocks % 10_000 == 0 {
            let window = mark.elapsed().as_secs_f64().max(1e-9);
            let blocks_per_sec = (blocks - mark_blocks) as f64 / window;
            let tx_per_sec = (transactions - mark_transactions) as f64 / window;
            eprintln!(
                "blocks={blocks} slot={last_slot} transactions={transactions} blocks_per_sec={blocks_per_sec:.1} tx_per_sec={tx_per_sec:.0}"
            );
            mark = Instant::now();
            mark_blocks = blocks;
            mark_transactions = transactions;
        }
    }

    Ok(ScanSummary {
        blocks,
        transactions,
        with_metadata,
        first_slot: first_slot.unwrap_or(0),
        last_slot,
    })
}

fn canonical_tx_index(position: u32, declared: Option<u64>) -> Result<u32> {
    if let Some(declared) = declared {
        let declared = u32::try_from(declared).context("transaction index exceeds u32")?;
        if declared != position {
            bail!("transaction frame index {declared} differs from position {position}");
        }
    }
    Ok(position)
}

fn render_metadata(meta: Option<&TransactionStatusMeta>) -> Result<String> {
    let Some(meta) = meta else {
        return Ok("null".to_string());
    };
    let mut bytes = Vec::new();
    let mut writer = ByteWriter(&mut bytes);
    write_metadata(&mut writer, meta).map_err(|error| anyhow::anyhow!(error))?;
    drop(writer);
    String::from_utf8(bytes).context("metadata JSON is not UTF-8")
}

fn write_metadata<W: MetadataJsonWrite>(
    w: &mut W,
    meta: &TransactionStatusMeta,
) -> std::result::Result<(), String> {
    let err = decode_error(meta.err.as_ref()).map_err(|error| error.to_string())?;
    w.raw(b"{\"err\":");
    write_optional_error(w, err.as_ref())?;
    w.raw(b",\"status\":");
    write_status(w, err.as_ref())?;
    w.raw(b",\"fee\":");
    w.u64(meta.fee);
    w.raw(b",\"preBalances\":");
    w.u64_array(&meta.pre_balances);
    w.raw(b",\"postBalances\":");
    w.u64_array(&meta.post_balances);
    w.raw(b",\"preTokenBalances\":");
    write_token_balances(w, &meta.pre_token_balances)?;
    w.raw(b",\"postTokenBalances\":");
    write_token_balances(w, &meta.post_token_balances)?;
    w.raw(b",\"logMessages\":");
    write_log_messages(w, meta)?;
    w.raw(b",\"innerInstructions\":");
    write_inner_instructions(w, meta);
    w.raw(b",\"loadedAddresses\":{\"writable\":");
    write_pubkey_array(w, &meta.loaded_writable_addresses)?;
    w.raw(b",\"readonly\":");
    write_pubkey_array(w, &meta.loaded_readonly_addresses)?;
    w.raw(b"}");
    if !meta.return_data_none
        && let Some(return_data) = &meta.return_data
    {
        w.raw(b",\"returnData\":");
        write_return_data(w, return_data)?;
    }
    w.raw(b",\"rewards\":");
    write_rewards(w, &meta.rewards)?;
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

fn decode_error(
    err: Option<&of_car_reader::confirmed_block::TransactionError>,
) -> Result<Option<StoredTransactionError>> {
    let Some(err) = err else {
        return Ok(None);
    };
    if err.err.is_empty() {
        return Ok(None);
    }
    match wincode::deserialize_exact::<StoredTransactionError>(&err.err) {
        Ok(decoded) => Ok(Some(decoded)),
        Err(decode_err) => decode_legacy_unit_borsh_io_error(&err.err)
            .map(Some)
            .ok_or(decode_err)
            .context(format!(
                "transaction error payload is not exact stored-error bytes (len={})",
                err.err.len()
            )),
    }
}

/// Old archives encode `BorshIoError` as a unit variant: nine bytes, no string.
/// The JSON is the same string `"BorshIoError"` as the current stored form.
fn decode_legacy_unit_borsh_io_error(bytes: &[u8]) -> Option<StoredTransactionError> {
    const TRANSACTION_ERROR_INSTRUCTION_ERROR: u32 = 8;
    const INSTRUCTION_ERROR_BORSH_IO_ERROR: u32 = 44;

    if bytes.len() != 9 {
        return None;
    }
    let transaction_error_tag = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
    let instruction_error_tag = u32::from_le_bytes(bytes[5..9].try_into().ok()?);
    if transaction_error_tag != TRANSACTION_ERROR_INSTRUCTION_ERROR
        || instruction_error_tag != INSTRUCTION_ERROR_BORSH_IO_ERROR
    {
        return None;
    }
    Some(StoredTransactionError::InstructionError(
        bytes[4],
        InstructionError::BorshIoError(String::new()),
    ))
}

fn write_optional_error<W: MetadataJsonWrite>(
    w: &mut W,
    err: Option<&StoredTransactionError>,
) -> std::result::Result<(), String> {
    if let Some(err) = err {
        write_stored_error(w, err)
    } else {
        w.raw(b"null");
        Ok(())
    }
}

fn write_status<W: MetadataJsonWrite>(
    w: &mut W,
    err: Option<&StoredTransactionError>,
) -> std::result::Result<(), String> {
    if let Some(err) = err {
        w.raw(b"{\"Err\":");
        write_stored_error(w, err)?;
        w.raw(b"}");
    } else {
        w.raw(br#"{"Ok":null}"#);
    }
    Ok(())
}

fn write_stored_error<W: MetadataJsonWrite>(
    w: &mut W,
    err: &StoredTransactionError,
) -> std::result::Result<(), String> {
    match err {
        StoredTransactionError::AccountInUse => w.string("AccountInUse")?,
        StoredTransactionError::AccountLoadedTwice => w.string("AccountLoadedTwice")?,
        StoredTransactionError::AccountNotFound => w.string("AccountNotFound")?,
        StoredTransactionError::ProgramAccountNotFound => w.string("ProgramAccountNotFound")?,
        StoredTransactionError::InsufficientFundsForFee => w.string("InsufficientFundsForFee")?,
        StoredTransactionError::InvalidAccountForFee => w.string("InvalidAccountForFee")?,
        StoredTransactionError::AlreadyProcessed => w.string("AlreadyProcessed")?,
        StoredTransactionError::BlockhashNotFound => w.string("BlockhashNotFound")?,
        StoredTransactionError::InstructionError(index, err) => {
            w.raw(b"{\"InstructionError\":[");
            w.u64(u64::from(*index));
            w.raw(b",");
            write_instruction_error(w, err)?;
            w.raw(b"]}");
        }
        StoredTransactionError::CallChainTooDeep => w.string("CallChainTooDeep")?,
        StoredTransactionError::MissingSignatureForFee => w.string("MissingSignatureForFee")?,
        StoredTransactionError::InvalidAccountIndex => w.string("InvalidAccountIndex")?,
        StoredTransactionError::SignatureFailure => w.string("SignatureFailure")?,
        StoredTransactionError::InvalidProgramForExecution => {
            w.string("InvalidProgramForExecution")?
        }
        StoredTransactionError::SanitizeFailure => w.string("SanitizeFailure")?,
        StoredTransactionError::ClusterMaintenance => w.string("ClusterMaintenance")?,
        StoredTransactionError::AccountBorrowOutstanding => w.string("AccountBorrowOutstanding")?,
        StoredTransactionError::WouldExceedMaxBlockCostLimit => {
            w.string("WouldExceedMaxBlockCostLimit")?
        }
        StoredTransactionError::UnsupportedVersion => w.string("UnsupportedVersion")?,
        StoredTransactionError::InvalidWritableAccount => w.string("InvalidWritableAccount")?,
        StoredTransactionError::WouldExceedMaxAccountCostLimit => {
            w.string("WouldExceedMaxAccountCostLimit")?
        }
        StoredTransactionError::WouldExceedAccountDataBlockLimit => {
            w.string("WouldExceedAccountDataBlockLimit")?
        }
        StoredTransactionError::TooManyAccountLocks => w.string("TooManyAccountLocks")?,
        StoredTransactionError::AddressLookupTableNotFound => {
            w.string("AddressLookupTableNotFound")?
        }
        StoredTransactionError::InvalidAddressLookupTableOwner => {
            w.string("InvalidAddressLookupTableOwner")?
        }
        StoredTransactionError::InvalidAddressLookupTableData => {
            w.string("InvalidAddressLookupTableData")?
        }
        StoredTransactionError::InvalidAddressLookupTableIndex => {
            w.string("InvalidAddressLookupTableIndex")?
        }
        StoredTransactionError::InvalidRentPayingAccount => w.string("InvalidRentPayingAccount")?,
        StoredTransactionError::WouldExceedMaxVoteCostLimit => {
            w.string("WouldExceedMaxVoteCostLimit")?
        }
        StoredTransactionError::WouldExceedAccountDataTotalLimit => {
            w.string("WouldExceedAccountDataTotalLimit")?
        }
        StoredTransactionError::DuplicateInstruction(index) => {
            w.raw(b"{\"DuplicateInstruction\":");
            w.u64(u64::from(*index));
            w.raw(b"}");
        }
        StoredTransactionError::InsufficientFundsForRent { account_index } => {
            w.raw(b"{\"InsufficientFundsForRent\":{\"account_index\":");
            w.u64(u64::from(*account_index));
            w.raw(b"}}");
        }
        StoredTransactionError::MaxLoadedAccountsDataSizeExceeded => {
            w.string("MaxLoadedAccountsDataSizeExceeded")?
        }
        StoredTransactionError::InvalidLoadedAccountsDataSizeLimit => {
            w.string("InvalidLoadedAccountsDataSizeLimit")?
        }
        StoredTransactionError::ResanitizationNeeded => w.string("ResanitizationNeeded")?,
        StoredTransactionError::ProgramExecutionTemporarilyRestricted { account_index } => {
            w.raw(b"{\"ProgramExecutionTemporarilyRestricted\":{\"account_index\":");
            w.u64(u64::from(*account_index));
            w.raw(b"}}");
        }
        StoredTransactionError::UnbalancedTransaction => w.string("UnbalancedTransaction")?,
        StoredTransactionError::ProgramCacheHitMaxLimit => w.string("ProgramCacheHitMaxLimit")?,
        StoredTransactionError::CommitCancelled => w.string("CommitCancelled")?,
    }
    Ok(())
}

fn write_instruction_error<W: MetadataJsonWrite>(
    w: &mut W,
    err: &InstructionError,
) -> std::result::Result<(), String> {
    match err {
        InstructionError::GenericError => w.string("GenericError")?,
        InstructionError::InvalidArgument => w.string("InvalidArgument")?,
        InstructionError::InvalidInstructionData => w.string("InvalidInstructionData")?,
        InstructionError::InvalidAccountData => w.string("InvalidAccountData")?,
        InstructionError::AccountDataTooSmall => w.string("AccountDataTooSmall")?,
        InstructionError::InsufficientFunds => w.string("InsufficientFunds")?,
        InstructionError::IncorrectProgramId => w.string("IncorrectProgramId")?,
        InstructionError::MissingRequiredSignature => w.string("MissingRequiredSignature")?,
        InstructionError::AccountAlreadyInitialized => w.string("AccountAlreadyInitialized")?,
        InstructionError::UninitializedAccount => w.string("UninitializedAccount")?,
        InstructionError::UnbalancedInstruction => w.string("UnbalancedInstruction")?,
        InstructionError::ModifiedProgramId => w.string("ModifiedProgramId")?,
        InstructionError::ExternalAccountLamportSpend => w.string("ExternalAccountLamportSpend")?,
        InstructionError::ExternalAccountDataModified => w.string("ExternalAccountDataModified")?,
        InstructionError::ReadonlyLamportChange => w.string("ReadonlyLamportChange")?,
        InstructionError::ReadonlyDataModified => w.string("ReadonlyDataModified")?,
        InstructionError::DuplicateAccountIndex => w.string("DuplicateAccountIndex")?,
        InstructionError::ExecutableModified => w.string("ExecutableModified")?,
        InstructionError::RentEpochModified => w.string("RentEpochModified")?,
        InstructionError::NotEnoughAccountKeys => w.string("NotEnoughAccountKeys")?,
        InstructionError::AccountDataSizeChanged => w.string("AccountDataSizeChanged")?,
        InstructionError::AccountNotExecutable => w.string("AccountNotExecutable")?,
        InstructionError::AccountBorrowFailed => w.string("AccountBorrowFailed")?,
        InstructionError::AccountBorrowOutstanding => w.string("AccountBorrowOutstanding")?,
        InstructionError::DuplicateAccountOutOfSync => w.string("DuplicateAccountOutOfSync")?,
        InstructionError::Custom(code) => {
            w.raw(b"{\"Custom\":");
            w.u64(u64::from(*code));
            w.raw(b"}");
        }
        InstructionError::InvalidError => w.string("InvalidError")?,
        InstructionError::ExecutableDataModified => w.string("ExecutableDataModified")?,
        InstructionError::ExecutableLamportChange => w.string("ExecutableLamportChange")?,
        InstructionError::ExecutableAccountNotRentExempt => {
            w.string("ExecutableAccountNotRentExempt")?
        }
        InstructionError::UnsupportedProgramId => w.string("UnsupportedProgramId")?,
        InstructionError::CallDepth => w.string("CallDepth")?,
        InstructionError::MissingAccount => w.string("MissingAccount")?,
        InstructionError::ReentrancyNotAllowed => w.string("ReentrancyNotAllowed")?,
        InstructionError::MaxSeedLengthExceeded => w.string("MaxSeedLengthExceeded")?,
        InstructionError::InvalidSeeds => w.string("InvalidSeeds")?,
        InstructionError::InvalidRealloc => w.string("InvalidRealloc")?,
        InstructionError::ComputationalBudgetExceeded => w.string("ComputationalBudgetExceeded")?,
        InstructionError::PrivilegeEscalation => w.string("PrivilegeEscalation")?,
        InstructionError::ProgramEnvironmentSetupFailure => {
            w.string("ProgramEnvironmentSetupFailure")?
        }
        InstructionError::ProgramFailedToComplete => w.string("ProgramFailedToComplete")?,
        InstructionError::ProgramFailedToCompile => w.string("ProgramFailedToCompile")?,
        InstructionError::Immutable => w.string("Immutable")?,
        InstructionError::IncorrectAuthority => w.string("IncorrectAuthority")?,
        InstructionError::BorshIoError(_) => w.string("BorshIoError")?,
        InstructionError::AccountNotRentExempt => w.string("AccountNotRentExempt")?,
        InstructionError::InvalidAccountOwner => w.string("InvalidAccountOwner")?,
        InstructionError::ArithmeticOverflow => w.string("ArithmeticOverflow")?,
        InstructionError::UnsupportedSysvar => w.string("UnsupportedSysvar")?,
        InstructionError::IllegalOwner => w.string("IllegalOwner")?,
        InstructionError::MaxAccountsDataAllocationsExceeded => {
            w.string("MaxAccountsDataAllocationsExceeded")?
        }
        InstructionError::MaxAccountsExceeded => w.string("MaxAccountsExceeded")?,
        InstructionError::MaxInstructionTraceLengthExceeded => {
            w.string("MaxInstructionTraceLengthExceeded")?
        }
        InstructionError::BuiltinProgramsMustConsumeComputeUnits => {
            w.string("BuiltinProgramsMustConsumeComputeUnits")?
        }
    }
    Ok(())
}

fn write_token_balances<W: MetadataJsonWrite>(
    w: &mut W,
    balances: &[TokenBalance],
) -> std::result::Result<(), String> {
    w.raw(b"[");
    for (index, balance) in balances.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        write_token_balance(w, balance)?;
    }
    w.raw(b"]");
    Ok(())
}

fn write_token_balance<W: MetadataJsonWrite>(
    w: &mut W,
    balance: &TokenBalance,
) -> std::result::Result<(), String> {
    w.raw(b"{\"accountIndex\":");
    w.u64(u64::from(balance.account_index));
    write_optional_string(w, "mint", &balance.mint)?;
    write_optional_string(w, "owner", &balance.owner)?;
    write_optional_string(w, "programId", &balance.program_id)?;
    w.raw(b",\"uiTokenAmount\":");
    if let Some(amount) = &balance.ui_token_amount {
        write_ui_token_amount(w, amount)?;
    } else {
        w.raw(b"null");
    }
    w.raw(b"}");
    Ok(())
}

fn write_optional_string<W: MetadataJsonWrite>(
    w: &mut W,
    name: &str,
    value: &str,
) -> std::result::Result<(), String> {
    if value.is_empty() {
        return Ok(());
    }
    w.raw(b",\"");
    w.raw(name.as_bytes());
    w.raw(b"\":");
    w.string(value)?;
    Ok(())
}

fn write_ui_token_amount<W: MetadataJsonWrite>(
    w: &mut W,
    amount: &UiTokenAmount,
) -> std::result::Result<(), String> {
    w.raw(b"{\"amount\":");
    w.string(&amount.amount)?;
    w.raw(b",\"decimals\":");
    w.u64(u64::from(amount.decimals));
    w.raw(b",\"uiAmount\":");
    if amount.amount == "0" {
        w.raw(b"null");
    } else {
        w.json(&amount.ui_amount)?;
    }
    w.raw(b",\"uiAmountString\":");
    w.string(&amount.ui_amount_string)?;
    w.raw(b"}");
    Ok(())
}

fn write_log_messages<W: MetadataJsonWrite>(
    w: &mut W,
    meta: &TransactionStatusMeta,
) -> std::result::Result<(), String> {
    if meta.log_messages_none {
        w.raw(b"null");
        return Ok(());
    }
    w.json(&limit_log_messages(&meta.log_messages))?;
    Ok(())
}

fn limit_log_messages(messages: &[String]) -> Vec<String> {
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
            out.push(message.clone());
        }
    }
    out
}

fn write_inner_instructions<W: MetadataJsonWrite>(w: &mut W, meta: &TransactionStatusMeta) {
    if meta.inner_instructions_none {
        w.raw(b"null");
        return;
    }
    w.raw(b"[");
    for (index, group) in meta.inner_instructions.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        write_inner_group(w, group);
    }
    w.raw(b"]");
}

fn write_inner_group<W: MetadataJsonWrite>(w: &mut W, group: &InnerInstructions) {
    w.raw(b"{\"index\":");
    w.u64(u64::from(group.index));
    w.raw(b",\"instructions\":[");
    for (index, instruction) in group.instructions.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        write_inner_instruction(w, instruction);
    }
    w.raw(b"]}");
}

fn write_inner_instruction<W: MetadataJsonWrite>(w: &mut W, instruction: &InnerInstruction) {
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

fn write_pubkey_array<W: MetadataJsonWrite>(
    w: &mut W,
    keys: &[Vec<u8>],
) -> std::result::Result<(), String> {
    w.raw(b"[");
    for (index, key) in keys.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        write_pubkey(w, key)?;
    }
    w.raw(b"]");
    Ok(())
}

fn write_pubkey<W: MetadataJsonWrite>(w: &mut W, bytes: &[u8]) -> std::result::Result<(), String> {
    let bytes: &[u8; 32] = bytes
        .try_into()
        .map_err(|_| "public key is not 32 bytes".to_string())?;
    w.string(&bs58::encode(bytes).into_string())
}

fn write_return_data<W: MetadataJsonWrite>(
    w: &mut W,
    return_data: &ReturnData,
) -> std::result::Result<(), String> {
    use base64::Engine as _;
    w.raw(b"{\"programId\":");
    write_pubkey(w, &return_data.program_id)?;
    w.raw(b",\"data\":[");
    w.string(&base64::engine::general_purpose::STANDARD.encode(&return_data.data))?;
    w.raw(br#","base64"]}"#);
    Ok(())
}

fn write_rewards<W: MetadataJsonWrite>(
    w: &mut W,
    rewards: &[Reward],
) -> std::result::Result<(), String> {
    w.raw(b"[");
    for (index, reward) in rewards.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        w.raw(b"{\"pubkey\":");
        w.string(&reward.pubkey)?;
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
        write_commission(w, &reward.commission)?;
        w.raw(b"}");
    }
    w.raw(b"]");
    Ok(())
}

fn write_commission<W: MetadataJsonWrite>(
    w: &mut W,
    commission: &str,
) -> std::result::Result<(), String> {
    if commission.is_empty() {
        w.raw(b"null");
        return Ok(());
    }
    match commission.parse::<u8>() {
        Ok(value) => w.u64(u64::from(value)),
        Err(_) => w.string(commission)?,
    }
    Ok(())
}

fn is_zstd(path: &std::path::Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("zst")
}

#[derive(Debug, clickhouse::Row, serde::Serialize)]
struct TxMetadataRow {
    slot: u64,
    tx_index: u32,
    metadata: String,
}

struct MetadataSink {
    client: clickhouse::Client,
    pending: Vec<TxMetadataRow>,
    batch_rows: usize,
    max_in_flight: u64,
    writes: Arc<WriteState>,
}

struct WriteState {
    in_flight: AtomicU64,
    written: AtomicU64,
    failed: Mutex<Option<String>>,
}

struct InFlightGuard(Arc<WriteState>);

impl InFlightGuard {
    fn begin(state: &Arc<WriteState>) -> Self {
        state.in_flight.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(state))
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ClickHouseEndpoint {
    url: String,
    user: Option<String>,
    password: Option<String>,
}

impl MetadataSink {
    fn connect(dsn: &str, database: &str, batch_rows: usize, max_in_flight: u64) -> Result<Self> {
        check_identifier("database", database)?;
        check_identifier("table", TABLE)?;
        let endpoint = split_clickhouse_dsn(dsn)?;
        let mut client = clickhouse::Client::default()
            .with_url(endpoint.url)
            .with_database(database)
            .with_setting("async_insert", "1")
            .with_setting("wait_for_async_insert", "1");
        if let Some(user) = endpoint.user {
            client = client.with_user(user);
        }
        if let Some(password) = endpoint.password {
            client = client.with_password(password);
        }
        Ok(Self {
            client,
            pending: Vec::with_capacity(batch_rows.max(1)),
            batch_rows: batch_rows.max(1),
            max_in_flight: max_in_flight.max(1),
            writes: Arc::new(WriteState {
                in_flight: AtomicU64::new(0),
                written: AtomicU64::new(0),
                failed: Mutex::new(None),
            }),
        })
    }

    fn queue(&mut self, row: TxMetadataRow) -> Result<()> {
        self.raise_background_error()?;
        self.pending.push(row);
        if self.pending.len() >= self.batch_rows {
            self.spawn_batch()?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<u64> {
        if !self.pending.is_empty() {
            self.spawn_batch()?;
        }
        loop {
            if self.writes.in_flight.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.raise_background_error()?;
        Ok(self.writes.written.load(Ordering::SeqCst))
    }

    fn spawn_batch(&mut self) -> Result<()> {
        self.raise_background_error()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        while self.writes.in_flight.load(Ordering::SeqCst) >= self.max_in_flight {
            self.raise_background_error()?;
            std::thread::sleep(Duration::from_millis(1));
        }
        let rows = std::mem::take(&mut self.pending);
        self.pending.reserve(self.batch_rows);
        let row_count = rows.len() as u64;
        let client = self.client.clone();
        let writes = Arc::clone(&self.writes);
        let guard = InFlightGuard::begin(&writes);
        tokio::spawn(async move {
            let _guard = guard;
            match retry_insert(client, rows).await {
                Ok(()) => {
                    writes.written.fetch_add(row_count, Ordering::SeqCst);
                }
                Err(error) => {
                    eprintln!("{error}");
                    let mut failed = writes.failed.lock().expect("metadata insert error lock");
                    if failed.is_none() {
                        *failed = Some(error);
                    }
                }
            }
        });
        Ok(())
    }

    fn raise_background_error(&self) -> Result<()> {
        let failed = self.writes.failed.lock().expect("metadata insert error lock");
        match failed.as_ref() {
            Some(message) => bail!("{message}"),
            None => Ok(()),
        }
    }
}

async fn retry_insert(client: clickhouse::Client, rows: Vec<TxMetadataRow>) -> Result<(), String> {
    let started = Instant::now();
    let mut delay = RETRY_INITIAL;
    let mut attempt = 1u32;
    loop {
        match insert_rows(&client, &rows).await {
            Ok(()) => {
                if attempt > 1 {
                    eprintln!("ClickHouse insert succeeded on attempt {attempt}");
                }
                return Ok(());
            }
            Err(error) => {
                if started.elapsed() >= RETRY_HORIZON {
                    return Err(format!(
                        "ClickHouse insert still failing after {attempt} attempts: {error}"
                    ));
                }
                eprintln!(
                    "ClickHouse insert failed (attempt {attempt}); retrying in {:.1}s: {error}",
                    delay.as_secs_f64()
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(RETRY_MAX);
                attempt += 1;
            }
        }
    }
}

async fn insert_rows(
    client: &clickhouse::Client,
    rows: &[TxMetadataRow],
) -> Result<(), clickhouse::error::Error> {
    let mut insert = client.insert::<TxMetadataRow>(TABLE).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

fn split_clickhouse_dsn(dsn: &str) -> Result<ClickHouseEndpoint> {
    let dsn = dsn.trim();
    if dsn.is_empty() {
        bail!("ClickHouse DSN is empty");
    }
    let Some(rest) = dsn
        .strip_prefix("http://")
        .or_else(|| dsn.strip_prefix("https://"))
    else {
        bail!("ClickHouse DSN must start with http:// or https://");
    };
    let scheme_len = dsn.len() - rest.len();
    let (userinfo, host) = match rest.rsplit_once('@') {
        Some((userinfo, host)) if !host.is_empty() => (Some(userinfo), host),
        _ => (None, rest),
    };
    let host = host.split(['/', '?']).next().unwrap_or(host);
    if host.is_empty() {
        bail!("ClickHouse DSN has no host");
    }
    let (user, password) = match userinfo {
        Some(userinfo) => {
            let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
            if user.is_empty() {
                bail!("ClickHouse DSN user is empty");
            }
            (Some(user.to_owned()), Some(password.to_owned()))
        }
        None => (None, None),
    };
    Ok(ClickHouseEndpoint {
        url: format!("{}{host}", &dsn[..scheme_len]),
        user,
        password,
    })
}

fn check_identifier(kind: &str, name: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        bail!("{kind} name is empty");
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        bail!("{kind} name '{name}' must start with a letter or underscore");
    }
    if !chars.all(|character| character.is_ascii_alphanumeric() || character == '_') {
        bail!("{kind} name '{name}' may contain only letters, digits, and underscores");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_bytes(err: &StoredTransactionError) -> Vec<u8> {
        wincode::serialize(err).expect("stored error")
    }

    #[test]
    fn rendered_metadata_matches_get_transaction_fields() {
        let err = StoredTransactionError::InstructionError(0, InstructionError::Custom(0));
        let meta = TransactionStatusMeta {
            err: Some(of_car_reader::confirmed_block::TransactionError {
                err: error_bytes(&err),
            }),
            fee: 5_000,
            pre_balances: vec![3, 4],
            post_balances: vec![1],
            log_messages: vec!["Program Vote111 invoke [1]".to_string()],
            inner_instructions: vec![InnerInstructions {
                index: 0,
                instructions: vec![InnerInstruction {
                    program_id_index: 3,
                    accounts: vec![0, 1],
                    data: vec![1],
                    stack_height: Some(2),
                }],
            }],
            pre_token_balances: vec![TokenBalance {
                account_index: 1,
                mint: "Mint".to_string(),
                owner: String::new(),
                program_id: String::new(),
                ui_token_amount: Some(UiTokenAmount {
                    ui_amount: 0.0,
                    decimals: 6,
                    amount: "0".to_string(),
                    ui_amount_string: "0".to_string(),
                }),
            }],
            loaded_writable_addresses: vec![vec![9; 32]],
            return_data: Some(ReturnData {
                program_id: vec![4; 32],
                data: vec![9],
            }),
            rewards: vec![Reward {
                pubkey: "RewardPubkey".to_string(),
                lamports: 1,
                post_balance: 2,
                reward_type: 1,
                commission: String::new(),
            }],
            compute_units_consumed: Some(2_100),
            cost_units: Some(3_428),
            ..TransactionStatusMeta::default()
        };
        let json = render_metadata(Some(&meta)).unwrap();
        assert!(json.contains(r#""err":{"InstructionError":[0,{"Custom":0}]}"#));
        assert!(json.contains(r#""status":{"Err":{"InstructionError":[0,{"Custom":0}]}}"#));
        assert!(json.contains(r#""fee":5000"#));
        assert!(json.contains(r#""preBalances":[3,4]"#));
        assert!(json.contains(r#""postBalances":[1]"#));
        assert!(json.contains("Program Vote111 invoke [1]"));
        assert!(json.contains(r#""uiAmount":null"#));
        assert!(json.contains(r#""accounts":[0,1]"#));
        assert!(json.contains(r#""data":"2""#));
        assert!(json.contains(&bs58::encode([9u8; 32]).into_string()));
        assert!(json.contains(r#""data":["CQ==","base64"]"#));
        assert!(json.contains(r#""rewardType":"Fee""#));
        assert!(json.contains(r#""commission":null"#));
        assert!(json.contains(r#""computeUnitsConsumed":2100"#));
        assert!(json.contains(r#""costUnits":3428"#));
    }

    #[test]
    fn legacy_unit_borsh_io_error_matches_get_transaction_text() {
        let meta = TransactionStatusMeta {
            err: Some(of_car_reader::confirmed_block::TransactionError {
                err: vec![8, 0, 0, 0, 2, 44, 0, 0, 0],
            }),
            ..TransactionStatusMeta::default()
        };
        let json = render_metadata(Some(&meta)).unwrap();
        assert!(json.contains(r#""err":{"InstructionError":[2,"BorshIoError"]}"#));
        assert!(json.contains(r#""status":{"Err":{"InstructionError":[2,"BorshIoError"]}}"#));
    }

    #[test]
    fn malformed_stored_error_still_fails() {
        let meta = TransactionStatusMeta {
            err: Some(of_car_reader::confirmed_block::TransactionError {
                err: vec![8, 0, 0, 0, 2, 44, 0, 0],
            }),
            ..TransactionStatusMeta::default()
        };
        assert!(render_metadata(Some(&meta)).is_err());
    }

    #[test]
    fn missing_metadata_is_null() {
        assert_eq!(render_metadata(None).unwrap(), "null");
    }

    #[test]
    fn log_limit_replaces_the_overflowing_line() {
        let meta = TransactionStatusMeta {
            log_messages: vec!["x".repeat(LOG_MESSAGES_BYTES_LIMIT)],
            ..TransactionStatusMeta::default()
        };
        let json = render_metadata(Some(&meta)).unwrap();
        assert!(json.contains(r#""logMessages":["Log truncated"]"#));
        assert!(!json.contains(&"x".repeat(20)));
    }

    #[test]
    fn declared_index_must_match_position() {
        assert_eq!(canonical_tx_index(4, Some(4)).unwrap(), 4);
        assert_eq!(canonical_tx_index(4, None).unwrap(), 4);
        assert!(canonical_tx_index(4, Some(5)).is_err());
    }

    #[test]
    fn table_name_rejects_a_hyphen() {
        assert_eq!(TABLE, "car_tx_metadata");
        assert!(check_identifier("table", TABLE).is_ok());
        assert!(check_identifier("table", "car-tx").is_err());
    }

    #[test]
    fn dsn_keeps_the_password_out_of_the_url() {
        let endpoint = split_clickhouse_dsn("http://default:secret@127.0.0.1:8123").unwrap();
        assert_eq!(endpoint.url, "http://127.0.0.1:8123");
        assert_eq!(endpoint.user.as_deref(), Some("default"));
        assert_eq!(endpoint.password.as_deref(), Some("secret"));
    }
}
