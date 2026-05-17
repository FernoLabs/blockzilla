use crate::get_block::{GetBlockConfig, GetBlockEncoding, TransactionDetails};
use of_car_reader::confirmed_block::{self as proto, RewardType, Rewards};
use of_car_reader::metadata_decoder::{ZstdReusableDecoder, decode_rewards_from_frame};
use of_car_reader::node::OwnedDataFrame;
use of_car_reader::stored_transaction::{InstructionError, StoredTransactionError};
use of_car_reader::versioned_transaction::{
    CompiledInstruction, MessageAddressTableLookup, MessageHeader, VersionedMessage,
    VersionedTransaction,
};
use of_car_reader::{CarBlockReader, car_block_group::CarBlockGroup};
use std::fmt::Display;
use std::io::{Cursor, Write as _};

pub fn car_bytes_to_json_config(
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    config: GetBlockConfig,
) -> Result<Vec<u8>, String> {
    if config.encoding != GetBlockEncoding::Json {
        return Err("requested getBlock encoding mode is not served locally".to_string());
    }

    let decode = match config.transaction_details {
        TransactionDetails::Full | TransactionDetails::Accounts => car_bytes_to_json_bytes,
        TransactionDetails::Signatures | TransactionDetails::None => car_bytes_to_json_light_bytes,
    };
    let bytes = decode(bytes, previous_blockhash, config.rewards)?;
    let mut block: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|err| format!("failed to parse generated block JSON: {err}"))?;

    match config.transaction_details {
        TransactionDetails::Full => {}
        TransactionDetails::Signatures => keep_only_signatures(&mut block),
        TransactionDetails::None => drop_transaction_details(&mut block),
        TransactionDetails::Accounts => keep_only_accounts(&mut block),
    }

    serde_json::to_vec(&block).map_err(|err| format!("failed to encode block JSON: {err}"))
}

pub fn car_bytes_to_json_bytes(
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    include_rewards: bool,
) -> Result<Vec<u8>, String> {
    car_bytes_to_json_bytes_inner(bytes, previous_blockhash, include_rewards, true)
}

pub fn car_bytes_to_json_light_bytes(
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    include_rewards: bool,
) -> Result<Vec<u8>, String> {
    car_bytes_to_json_bytes_inner(bytes, previous_blockhash, include_rewards, false)
}

fn car_bytes_to_json_bytes_inner(
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    include_rewards: bool,
    include_transaction_meta: bool,
) -> Result<Vec<u8>, String> {
    let input_len = bytes.len();
    let mut block = read_car_block(bytes, include_rewards)?;
    let rewards = if include_rewards {
        decode_block_rewards_proto(block.rewards.as_ref())?
    } else {
        None
    };

    let mut w = JsonWriter::with_capacity(json_capacity_hint(input_len, include_transaction_meta));
    w.raw(b"{\"blockhash\":");
    w.bs58(block.blockhash)?;
    w.raw(b",\"previousBlockhash\":");
    w.previous_blockhash(previous_blockhash)?;
    w.raw(b",\"parentSlot\":");
    w.display(block.parent_slot.unwrap_or_default())?;
    w.raw(b",\"blockTime\":");
    write_option_i64(&mut w, rpc_block_time(block.block_time))?;
    w.raw(b",\"blockHeight\":");
    write_option_u64(&mut w, block.block_height)?;
    if include_rewards {
        w.raw(b",\"rewards\":");
        write_rewards(&mut w, rewards.as_ref())?;
    }
    w.raw(b",\"transactions\":[");

    if include_transaction_meta {
        let mut tx_iter = block.transactions();
        let mut first = true;
        while let Some((tx, meta)) = tx_iter
            .next_tx()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?
        {
            if !first {
                w.raw(b",");
            }
            first = false;
            write_block_transaction(&mut w, tx, meta)?;
        }
    } else {
        let mut tx_iter = block.transactions_no_meta();
        let mut first = true;
        while let Some(tx) = tx_iter
            .next_tx()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?
        {
            if !first {
                w.raw(b",");
            }
            first = false;
            write_block_transaction_light(&mut w, tx)?;
        }
    }

    w.raw(b"]}");
    Ok(w.into_inner())
}

fn json_capacity_hint(input_len: usize, include_transaction_meta: bool) -> usize {
    let multiplier = if include_transaction_meta { 3 } else { 2 };
    input_len
        .saturating_mul(multiplier)
        .clamp(4 * 1024, 32 * 1024 * 1024)
}

fn read_car_block(bytes: Vec<u8>, include_rewards: bool) -> Result<CarBlockGroup, String> {
    let len = bytes.len();
    let cursor = Cursor::new(bytes);
    let mut reader = CarBlockReader::with_capacity(cursor, len);
    let mut block = if include_rewards {
        CarBlockGroup::new()
    } else {
        CarBlockGroup::without_rewards()
    };

    let res = reader
        .read_until_block_into(&mut block)
        .map_err(|err| format!("Failed to read CAR block: {err}"))?;

    if !res {
        return Err("CAR reader returned false".to_string());
    }

    Ok(block)
}

fn decode_block_rewards_proto(rewards: Option<&OwnedDataFrame>) -> Result<Option<Rewards>, String> {
    let Some(rewards) = rewards else {
        return Ok(None);
    };

    let mut decoded = Rewards::default();
    let mut zstd = ZstdReusableDecoder::new();
    decode_rewards_from_frame(&rewards.data, &mut decoded, &mut zstd)
        .map_err(|err| format!("Failed to decode rewards: {err}"))?;
    Ok(Some(decoded))
}

fn rpc_block_time(block_time: Option<i64>) -> Option<i64> {
    block_time.filter(|value| *value != 0)
}

struct JsonWriter {
    out: Vec<u8>,
}

impl JsonWriter {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            out: Vec::with_capacity(capacity),
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.out
    }

    fn raw(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }

    fn string(&mut self, value: &str) -> Result<(), String> {
        serde_json::to_writer(&mut self.out, value)
            .map_err(|err| format!("failed to encode JSON string: {err}"))
    }

    fn bs58(&mut self, bytes: impl AsRef<[u8]>) -> Result<(), String> {
        self.out.push(b'"');
        bs58::encode(bytes)
            .onto(&mut self.out)
            .map_err(|err| format!("failed to encode base58: {err}"))?;
        self.out.push(b'"');
        Ok(())
    }

    fn previous_blockhash(&mut self, previous_blockhash: Option<[u8; 32]>) -> Result<(), String> {
        if let Some(previous_blockhash) = previous_blockhash {
            self.bs58(previous_blockhash)
        } else {
            self.string("todo")
        }
    }

    fn display(&mut self, value: impl Display) -> Result<(), String> {
        write!(&mut self.out, "{value}")
            .map_err(|err| format!("failed to write JSON number: {err}"))
    }

    fn f64(&mut self, value: f64) -> Result<(), String> {
        serde_json::to_writer(&mut self.out, &value)
            .map_err(|err| format!("failed to encode JSON float: {err}"))
    }
}

fn write_option_i64(w: &mut JsonWriter, value: Option<i64>) -> Result<(), String> {
    if let Some(value) = value {
        w.display(value)
    } else {
        w.raw(b"null");
        Ok(())
    }
}

fn write_option_u64(w: &mut JsonWriter, value: Option<u64>) -> Result<(), String> {
    if let Some(value) = value {
        w.display(value)
    } else {
        w.raw(b"null");
        Ok(())
    }
}

fn write_option_u32(w: &mut JsonWriter, value: Option<u32>) -> Result<(), String> {
    if let Some(value) = value {
        w.display(value)
    } else {
        w.raw(b"null");
        Ok(())
    }
}

fn write_rewards(w: &mut JsonWriter, rewards: Option<&Rewards>) -> Result<(), String> {
    let Some(rewards) = rewards else {
        w.raw(b"null");
        return Ok(());
    };

    w.raw(b"[");
    for (index, reward) in rewards.rewards.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.raw(b"{\"pubkey\":");
        w.string(&reward.pubkey)?;
        w.raw(b",\"lamports\":");
        w.display(reward.lamports)?;
        w.raw(b",\"postBalance\":");
        w.display(reward.post_balance)?;
        w.raw(b",\"rewardType\":");
        if let Some(reward_type) = reward_type_to_rpc(reward.reward_type) {
            w.string(reward_type)?;
        } else {
            w.raw(b"null");
        }
        w.raw(b",\"commission\":");
        if let Ok(commission) = reward.commission.parse::<u8>() {
            w.display(commission)?;
        } else {
            w.raw(b"null");
        }
        w.raw(b"}");
    }
    w.raw(b"]");
    Ok(())
}

fn reward_type_to_rpc(reward_type: i32) -> Option<&'static str> {
    match RewardType::try_from(reward_type).ok()? {
        RewardType::Fee => Some("Fee"),
        RewardType::Rent => Some("Rent"),
        RewardType::Staking => Some("Staking"),
        RewardType::Voting => Some("Voting"),
        RewardType::Unspecified => None,
    }
}

fn write_block_transaction(
    w: &mut JsonWriter,
    tx: &VersionedTransaction<'_>,
    meta: Option<&proto::TransactionStatusMeta>,
) -> Result<(), String> {
    w.raw(b"{\"transaction\":");
    write_encoded_transaction(w, tx)?;
    w.raw(b",\"meta\":");
    if let Some(meta) = meta {
        write_transaction_meta(w, meta)?;
    } else {
        w.raw(b"null");
    }
    w.raw(b"}");
    Ok(())
}

fn write_block_transaction_light(
    w: &mut JsonWriter,
    tx: &VersionedTransaction<'_>,
) -> Result<(), String> {
    w.raw(b"{\"transaction\":");
    write_encoded_transaction(w, tx)?;
    w.raw(b"}");
    Ok(())
}

fn write_encoded_transaction(
    w: &mut JsonWriter,
    tx: &VersionedTransaction<'_>,
) -> Result<(), String> {
    w.raw(b"{\"signatures\":[");
    for (index, signature) in tx.signatures.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.bs58(*signature)?;
    }
    w.raw(b"],\"message\":");
    write_message(w, &tx.message)?;
    w.raw(b"}");
    Ok(())
}

fn write_message(w: &mut JsonWriter, message: &VersionedMessage<'_>) -> Result<(), String> {
    match message {
        VersionedMessage::Legacy(message) => write_message_fields(
            w,
            message.header,
            &message.account_keys,
            message.recent_blockhash,
            &message.instructions,
            None,
        ),
        VersionedMessage::V0(message) => write_message_fields(
            w,
            message.header,
            &message.account_keys,
            message.recent_blockhash,
            &message.instructions,
            Some(&message.address_table_lookups),
        ),
    }
}

fn write_message_fields(
    w: &mut JsonWriter,
    header: MessageHeader,
    account_keys: &[&[u8; 32]],
    recent_blockhash: &[u8; 32],
    instructions: &[CompiledInstruction],
    address_table_lookups: Option<&[MessageAddressTableLookup<'_>]>,
) -> Result<(), String> {
    w.raw(b"{\"accountKeys\":[");
    for (index, account_key) in account_keys.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.bs58(*account_key)?;
    }
    w.raw(b"],\"instructions\":[");
    for (index, instruction) in instructions.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        write_compiled_instruction(w, instruction, Some(1))?;
    }
    w.raw(b"],\"recentBlockhash\":");
    w.bs58(recent_blockhash)?;
    w.raw(b",\"header\":");
    write_message_header(w, header)?;

    if let Some(address_table_lookups) = address_table_lookups {
        w.raw(b",\"addressTableLookups\":[");
        for (index, lookup) in address_table_lookups.iter().enumerate() {
            if index != 0 {
                w.raw(b",");
            }
            w.raw(b"{\"accountKey\":");
            w.bs58(lookup.account_key)?;
            w.raw(b",\"writableIndexes\":");
            write_u8_array(w, &lookup.writable_indexes)?;
            w.raw(b",\"readonlyIndexes\":");
            write_u8_array(w, &lookup.readonly_indexes)?;
            w.raw(b"}");
        }
        w.raw(b"]");
    }

    w.raw(b"}");
    Ok(())
}

fn write_message_header(w: &mut JsonWriter, header: MessageHeader) -> Result<(), String> {
    w.raw(b"{\"numRequiredSignatures\":");
    w.display(header.num_required_signatures)?;
    w.raw(b",\"numReadonlySignedAccounts\":");
    w.display(header.num_readonly_signed_accounts)?;
    w.raw(b",\"numReadonlyUnsignedAccounts\":");
    w.display(header.num_readonly_unsigned_accounts)?;
    w.raw(b"}");
    Ok(())
}

fn write_compiled_instruction(
    w: &mut JsonWriter,
    instruction: &CompiledInstruction,
    stack_height: Option<u32>,
) -> Result<(), String> {
    w.raw(b"{\"programIdIndex\":");
    w.display(instruction.program_id_index)?;
    w.raw(b",\"accounts\":");
    write_u8_array(w, &instruction.accounts)?;
    w.raw(b",\"data\":");
    w.bs58(&instruction.data)?;
    w.raw(b",\"stackHeight\":");
    write_option_u32(w, stack_height)?;
    w.raw(b"}");
    Ok(())
}

fn write_transaction_meta(
    w: &mut JsonWriter,
    meta: &proto::TransactionStatusMeta,
) -> Result<(), String> {
    w.raw(b"{\"err\":");
    write_transaction_error(w, meta.err.as_ref())?;
    w.raw(b",\"fee\":");
    w.display(meta.fee)?;
    w.raw(b",\"preBalances\":");
    write_u64_array(w, &meta.pre_balances)?;
    w.raw(b",\"postBalances\":");
    write_u64_array(w, &meta.post_balances)?;
    w.raw(b",\"preTokenBalances\":");
    write_token_balances(w, &meta.pre_token_balances)?;
    w.raw(b",\"postTokenBalances\":");
    write_token_balances(w, &meta.post_token_balances)?;
    w.raw(b",\"logMessages\":");
    write_log_messages(w, meta)?;
    w.raw(b",\"computeUnitsConsumed\":");
    write_option_u64(w, meta.compute_units_consumed)?;
    w.raw(b",\"innerInstructions\":");
    write_inner_instructions(w, meta)?;
    w.raw(b",\"loadedAddresses\":");
    write_loaded_addresses(w, meta)?;
    w.raw(b"}");
    Ok(())
}

fn write_token_balances(
    w: &mut JsonWriter,
    balances: &[proto::TokenBalance],
) -> Result<(), String> {
    if balances.is_empty() {
        w.raw(b"null");
        return Ok(());
    }

    w.raw(b"[");
    for (index, balance) in balances.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.raw(b"{\"accountIndex\":");
        w.display(balance.account_index)?;
        w.raw(b",\"mint\":");
        w.string(&balance.mint)?;
        w.raw(b",\"owner\":");
        if balance.owner.is_empty() {
            w.raw(b"null");
        } else {
            w.string(&balance.owner)?;
        }
        w.raw(b",\"programId\":");
        if balance.program_id.is_empty() {
            w.raw(b"null");
        } else {
            w.string(&balance.program_id)?;
        }
        w.raw(b",\"uiTokenAmount\":");
        if let Some(amount) = balance.ui_token_amount.as_ref() {
            w.raw(b"{\"amount\":");
            w.string(&amount.amount)?;
            w.raw(b",\"decimals\":");
            w.display(amount.decimals)?;
            w.raw(b",\"uiAmount\":");
            w.f64(amount.ui_amount)?;
            w.raw(b",\"uiAmountString\":");
            w.string(&amount.ui_amount_string)?;
            w.raw(b"}");
        } else {
            w.raw(b"null");
        }
        w.raw(b"}");
    }
    w.raw(b"]");
    Ok(())
}

fn write_log_messages(
    w: &mut JsonWriter,
    meta: &proto::TransactionStatusMeta,
) -> Result<(), String> {
    if meta.log_messages_none {
        w.raw(b"null");
        return Ok(());
    }

    w.raw(b"[");
    for (index, log) in meta.log_messages.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.string(log)?;
    }
    w.raw(b"]");
    Ok(())
}

fn write_inner_instructions(
    w: &mut JsonWriter,
    meta: &proto::TransactionStatusMeta,
) -> Result<(), String> {
    if meta.inner_instructions_none {
        w.raw(b"null");
        return Ok(());
    }

    w.raw(b"[");
    for (index, inner) in meta.inner_instructions.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.raw(b"{\"index\":");
        w.display(inner.index)?;
        w.raw(b",\"instructions\":[");
        for (instruction_index, instruction) in inner.instructions.iter().enumerate() {
            if instruction_index != 0 {
                w.raw(b",");
            }
            write_inner_instruction(w, instruction)?;
        }
        w.raw(b"]}");
    }
    w.raw(b"]");
    Ok(())
}

fn write_inner_instruction(
    w: &mut JsonWriter,
    instruction: &proto::InnerInstruction,
) -> Result<(), String> {
    w.raw(b"{\"programIdIndex\":");
    w.display(instruction.program_id_index)?;
    w.raw(b",\"accounts\":");
    write_u8_array(w, &instruction.accounts)?;
    w.raw(b",\"data\":");
    w.bs58(&instruction.data)?;
    w.raw(b",\"stackHeight\":");
    write_option_u32(w, instruction.stack_height)?;
    w.raw(b"}");
    Ok(())
}

fn write_loaded_addresses(
    w: &mut JsonWriter,
    meta: &proto::TransactionStatusMeta,
) -> Result<(), String> {
    if meta.loaded_readonly_addresses.is_empty() && meta.loaded_writable_addresses.is_empty() {
        w.raw(b"null");
        return Ok(());
    }

    w.raw(b"{\"writable\":[");
    for (index, address) in meta.loaded_writable_addresses.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.bs58(address)?;
    }
    w.raw(b"],\"readonly\":[");
    for (index, address) in meta.loaded_readonly_addresses.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.bs58(address)?;
    }
    w.raw(b"]}");
    Ok(())
}

fn write_transaction_error(
    w: &mut JsonWriter,
    err: Option<&proto::TransactionError>,
) -> Result<(), String> {
    let Some(err) = err else {
        w.raw(b"null");
        return Ok(());
    };

    let decoded = decode_transaction_error_bytes(&err.err)
        .map_err(|err| format!("Failed to decode transaction error: {err}"))?;
    write_stored_transaction_error(w, decoded)
}

fn decode_transaction_error_bytes(bytes: &[u8]) -> Result<StoredTransactionError, String> {
    match wincode::deserialize::<StoredTransactionError>(bytes) {
        Ok(err) => Ok(err),
        Err(err) => decode_unit_borsh_io_instruction_error(bytes).map_err(|_| err.to_string()),
    }
}

fn decode_unit_borsh_io_instruction_error(bytes: &[u8]) -> Result<StoredTransactionError, ()> {
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
        InstructionError::BorshIoError(String::new()),
    ))
}

fn write_stored_transaction_error(
    w: &mut JsonWriter,
    err: StoredTransactionError,
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
            w.display(index)?;
            w.raw(b",");
            write_instruction_error(w, err)?;
            w.raw(b"]}");
            Ok(())
        }
        StoredTransactionError::CallChainTooDeep => w.string("CallChainTooDeep"),
        StoredTransactionError::MissingSignatureForFee => w.string("MissingSignatureForFee"),
        StoredTransactionError::InvalidAccountIndex => w.string("InvalidAccountIndex"),
        StoredTransactionError::SignatureFailure => w.string("SignatureFailure"),
        StoredTransactionError::InvalidProgramForExecution => {
            w.string("InvalidProgramForExecution")
        }
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
        StoredTransactionError::AddressLookupTableNotFound => {
            w.string("AddressLookupTableNotFound")
        }
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
        StoredTransactionError::WouldExceedMaxVoteCostLimit => {
            w.string("WouldExceedMaxVoteCostLimit")
        }
        StoredTransactionError::WouldExceedAccountDataTotalLimit => {
            w.string("WouldExceedAccountDataTotalLimit")
        }
        StoredTransactionError::DuplicateInstruction(index) => {
            w.raw(b"{\"DuplicateInstruction\":");
            w.display(index)?;
            w.raw(b"}");
            Ok(())
        }
        StoredTransactionError::InsufficientFundsForRent { account_index } => {
            w.raw(b"{\"InsufficientFundsForRent\":{\"account_index\":");
            w.display(account_index)?;
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
            w.display(account_index)?;
            w.raw(b"}}");
            Ok(())
        }
        StoredTransactionError::UnbalancedTransaction => w.string("UnbalancedTransaction"),
        StoredTransactionError::ProgramCacheHitMaxLimit => w.string("ProgramCacheHitMaxLimit"),
        StoredTransactionError::CommitCancelled => w.string("CommitCancelled"),
    }
}

fn write_instruction_error(w: &mut JsonWriter, err: InstructionError) -> Result<(), String> {
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
            w.display(code)?;
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
        InstructionError::BorshIoError(message) => {
            if message.is_empty() {
                return w.string("BorshIoError");
            }
            w.raw(b"{\"BorshIoError\":");
            w.string(&message)?;
            w.raw(b"}");
            Ok(())
        }
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

fn write_u8_array(w: &mut JsonWriter, values: &[u8]) -> Result<(), String> {
    w.raw(b"[");
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.display(*value)?;
    }
    w.raw(b"]");
    Ok(())
}

fn write_u64_array(w: &mut JsonWriter, values: &[u64]) -> Result<(), String> {
    w.raw(b"[");
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.display(*value)?;
    }
    w.raw(b"]");
    Ok(())
}

fn keep_only_signatures(block: &mut serde_json::Value) {
    let signatures = block
        .get("transactions")
        .and_then(serde_json::Value::as_array)
        .map(|transactions| {
            transactions
                .iter()
                .filter_map(|tx| {
                    tx.get("transaction")?
                        .get("signatures")?
                        .as_array()?
                        .first()
                        .cloned()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Some(map) = block.as_object_mut() {
        map.remove("transactions");
        map.insert(
            "signatures".to_string(),
            serde_json::Value::Array(signatures),
        );
    }
}

fn drop_transaction_details(block: &mut serde_json::Value) {
    if let Some(map) = block.as_object_mut() {
        map.remove("transactions");
        map.remove("signatures");
    }
}

fn keep_only_accounts(block: &mut serde_json::Value) {
    let Some(transactions) = block
        .get_mut("transactions")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return;
    };

    for tx in transactions {
        let account_keys = accounts_only_keys(tx);
        let signatures = tx
            .get("transaction")
            .and_then(|transaction| transaction.get("signatures"))
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
        let version = tx
            .get("transaction")
            .and_then(|transaction| transaction.get("message"))
            .and_then(|message| message.get("addressTableLookups"))
            .map(|_| serde_json::Value::from(0))
            .unwrap_or_else(|| serde_json::Value::String("legacy".to_string()));

        if let Some(map) = tx.as_object_mut() {
            map.insert(
                "transaction".to_string(),
                serde_json::json!({
                    "accountKeys": account_keys,
                    "signatures": signatures,
                }),
            );
            if let Some(meta) = map.get_mut("meta") {
                prune_accounts_meta(meta);
            }
            map.insert("version".to_string(), version);
        }
    }
}

fn accounts_only_keys(tx: &serde_json::Value) -> Vec<serde_json::Value> {
    let Some(message) = tx
        .get("transaction")
        .and_then(|transaction| transaction.get("message"))
    else {
        return Vec::new();
    };
    let Some(static_keys) = message
        .get("accountKeys")
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };
    let header = message.get("header");
    let required_signatures = header
        .and_then(|h| h.get("numRequiredSignatures"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as usize;
    let readonly_signed = header
        .and_then(|h| h.get("numReadonlySignedAccounts"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as usize;
    let readonly_unsigned = header
        .and_then(|h| h.get("numReadonlyUnsignedAccounts"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as usize;
    let signed_writable = required_signatures.saturating_sub(readonly_signed);
    let unsigned_writable_end = static_keys.len().saturating_sub(readonly_unsigned);

    let mut out = Vec::with_capacity(static_keys.len());
    for (index, key) in static_keys.iter().enumerate() {
        let signer = index < required_signatures;
        let writable = if signer {
            index < signed_writable
        } else {
            index < unsigned_writable_end
        };
        out.push(serde_json::json!({
            "pubkey": key,
            "signer": signer,
            "source": "transaction",
            "writable": writable,
        }));
    }

    let loaded = tx
        .get("meta")
        .and_then(|meta| meta.get("loadedAddresses"))
        .and_then(serde_json::Value::as_object);
    if let Some(loaded) = loaded {
        append_loaded_account_keys(&mut out, loaded.get("writable"), true);
        append_loaded_account_keys(&mut out, loaded.get("readonly"), false);
    }

    out
}

fn append_loaded_account_keys(
    out: &mut Vec<serde_json::Value>,
    keys: Option<&serde_json::Value>,
    writable: bool,
) {
    let Some(keys) = keys.and_then(serde_json::Value::as_array) else {
        return;
    };
    for key in keys {
        out.push(serde_json::json!({
            "pubkey": key,
            "signer": false,
            "source": "lookupTable",
            "writable": writable,
        }));
    }
}

fn prune_accounts_meta(meta: &mut serde_json::Value) {
    let Some(map) = meta.as_object_mut() else {
        return;
    };
    let err = map.get("err").cloned().unwrap_or(serde_json::Value::Null);
    let status = if err.is_null() {
        serde_json::json!({ "Ok": null })
    } else {
        serde_json::json!({ "Err": err.clone() })
    };

    let keep = [
        "err",
        "fee",
        "postBalances",
        "postTokenBalances",
        "preBalances",
        "preTokenBalances",
    ];
    map.retain(|key, _| keep.contains(&key.as_str()));
    map.insert("status".to_string(), status);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_unit_borsh_io_error_instruction_error() {
        let decoded = decode_transaction_error_bytes(&[
            8, 0, 0, 0, // TransactionError::InstructionError
            2, // instruction index
            44, 0, 0, 0, // InstructionError::BorshIoError as a unit variant
        ])
        .expect("decode");

        match decoded {
            StoredTransactionError::InstructionError(
                2,
                InstructionError::BorshIoError(message),
            ) => assert!(message.is_empty()),
            _ => panic!("unexpected decode"),
        }
    }
}
