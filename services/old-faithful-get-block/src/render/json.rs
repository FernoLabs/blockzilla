use super::base58;
use crate::rpc::get_block::{GetBlockConfig, GetBlockEncoding, TransactionDetails};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use of_car_reader::confirmed_block::{self as proto, RewardType, Rewards};
use of_car_reader::confirmed_block_borrowed::solana::storage::ConfirmedBlock as borrowed_proto;
use of_car_reader::metadata_decoder::{
    BorrowedTransactionStatusMetaView, ZstdReusableDecoder, decode_rewards_from_frame,
    slot_uses_protobuf_metadata,
};
use of_car_reader::node::OwnedDataFrame;
use of_car_reader::stored_transaction::{InstructionError, StoredTransactionError};
use of_car_reader::versioned_transaction::{
    CompiledInstruction, MessageAddressTableLookup, MessageHeader, VersionedMessage,
    VersionedTransaction,
};
use of_car_reader::{CarBlockReader, car_block_group::CarBlockGroup};
use std::io::Cursor;

const IN_MEMORY_CAR_READER_BUFFER_BYTES: usize = 64 * 1024;

pub fn car_bytes_to_json_config(
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    config: GetBlockConfig,
) -> Result<Vec<u8>, String> {
    if config.encoding != GetBlockEncoding::Json {
        return Err("requested getBlock encoding mode is not served locally".to_string());
    }

    let mode = match config.transaction_details {
        TransactionDetails::Full => TransactionRenderMode::Full,
        TransactionDetails::Accounts => TransactionRenderMode::Accounts,
        TransactionDetails::Signatures => TransactionRenderMode::Signatures,
        TransactionDetails::None => TransactionRenderMode::None,
    };
    car_bytes_to_json_bytes_inner(bytes, previous_blockhash, config.rewards, mode)
}

pub fn car_bytes_to_json_bytes(
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    include_rewards: bool,
) -> Result<Vec<u8>, String> {
    car_bytes_to_json_bytes_inner(
        bytes,
        previous_blockhash,
        include_rewards,
        TransactionRenderMode::Full,
    )
}

pub fn car_bytes_to_json_light_bytes(
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    include_rewards: bool,
) -> Result<Vec<u8>, String> {
    car_bytes_to_json_bytes_inner(
        bytes,
        previous_blockhash,
        include_rewards,
        TransactionRenderMode::FullLight,
    )
}

pub fn car_bytes_to_block_time(bytes: Vec<u8>) -> Result<Option<i64>, String> {
    let block = read_car_block(bytes, false, TransactionReadMode::None)?;
    Ok(rpc_block_time(block.block_time))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionRenderMode {
    Full,
    FullLight,
    Accounts,
    Signatures,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionReadMode {
    Full,
    SignaturePrefix,
    None,
}

fn car_bytes_to_json_bytes_inner(
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    include_rewards: bool,
    mode: TransactionRenderMode,
) -> Result<Vec<u8>, String> {
    let input_len = bytes.len();
    let transaction_read_mode = match mode {
        TransactionRenderMode::Full
        | TransactionRenderMode::FullLight
        | TransactionRenderMode::Accounts => TransactionReadMode::Full,
        TransactionRenderMode::Signatures => TransactionReadMode::SignaturePrefix,
        TransactionRenderMode::None => TransactionReadMode::None,
    };
    let mut block = read_car_block(bytes, include_rewards, transaction_read_mode)?;
    let tx_count = block.get_len().0;
    let rewards = if include_rewards {
        let rewards = decode_block_rewards_proto(block.rewards.as_ref())?;
        rewards
    } else {
        None
    };

    let mut w = JsonWriter::with_capacity(json_capacity_hint(input_len, mode, tx_count));
    write_block_header(&mut w, &block, previous_blockhash)?;
    if include_rewards {
        w.raw(b",\"rewards\":");
        write_rewards(&mut w, rewards.as_ref())?;
    }

    match mode {
        TransactionRenderMode::Full => {
            if can_use_borrowed_metadata_fast_path(&block) {
                write_full_transactions_borrowed(&mut w, &mut block)?
            } else {
                write_full_transactions_owned(&mut w, &mut block)?
            }
        }
        TransactionRenderMode::FullLight => {
            w.raw(b",\"transactions\":[");
            let mut tx_iter = block.transactions_no_meta();
            let mut first = true;
            loop {
                let next = tx_iter
                    .next_tx()
                    .map_err(|err| format!("Failed to parse transaction: {err}"))?;
                let Some(tx) = next else {
                    break;
                };
                if !first {
                    w.raw(b",");
                }
                first = false;
                write_block_transaction_light(&mut w, tx)?;
            }
            w.raw(b"]");
        }
        TransactionRenderMode::Accounts => {
            if can_use_borrowed_metadata_fast_path(&block) {
                write_accounts_transactions_borrowed(&mut w, &mut block)?
            } else {
                write_accounts_transactions(&mut w, &mut block)?
            }
        }
        TransactionRenderMode::Signatures => write_signatures(&mut w, &mut block)?,
        TransactionRenderMode::None => {}
    }

    w.raw(b"}");
    Ok(w.into_inner())
}

fn json_capacity_hint(input_len: usize, mode: TransactionRenderMode, tx_count: usize) -> usize {
    match mode {
        TransactionRenderMode::Full => input_len.saturating_mul(3),
        TransactionRenderMode::Accounts => input_len.saturating_mul(3),
        TransactionRenderMode::FullLight => input_len.saturating_mul(2),
        TransactionRenderMode::Signatures => tx_count.saturating_mul(96).saturating_add(256),
        TransactionRenderMode::None => 256,
    }
    .clamp(4 * 1024, 32 * 1024 * 1024)
}

fn can_use_borrowed_metadata_fast_path(block: &CarBlockGroup) -> bool {
    block.slot.is_some_and(slot_uses_protobuf_metadata) && !force_owned_metadata_path()
}

#[cfg(not(target_arch = "wasm32"))]
fn force_owned_metadata_path() -> bool {
    std::env::var_os("OF_GET_BLOCK_FORCE_OWNED_METADATA").is_some()
}

#[cfg(target_arch = "wasm32")]
fn force_owned_metadata_path() -> bool {
    false
}

fn write_block_header(
    w: &mut JsonWriter,
    block: &CarBlockGroup,
    previous_blockhash: Option<[u8; 32]>,
) -> Result<(), String> {
    w.raw(b"{\"blockhash\":");
    w.bs58(block.blockhash)?;
    w.raw(b",\"previousBlockhash\":");
    w.previous_blockhash(previous_blockhash)?;
    w.raw(b",\"parentSlot\":");
    w.display(block.parent_slot.unwrap_or_default())?;
    w.raw(b",\"blockTime\":");
    write_option_i64(w, rpc_block_time(block.block_time))?;
    w.raw(b",\"blockHeight\":");
    write_option_u64(w, block.block_height)?;
    Ok(())
}

fn read_car_block(
    bytes: Vec<u8>,
    include_rewards: bool,
    transaction_read_mode: TransactionReadMode,
) -> Result<CarBlockGroup, String> {
    let len = bytes.len();
    let cursor = Cursor::new(bytes);
    let reader_capacity = match transaction_read_mode {
        TransactionReadMode::Full => len,
        TransactionReadMode::SignaturePrefix | TransactionReadMode::None => {
            len.min(IN_MEMORY_CAR_READER_BUFFER_BYTES)
        }
    };
    let mut reader = CarBlockReader::with_capacity(cursor, reader_capacity);
    let mut block = match (include_rewards, transaction_read_mode) {
        (true, TransactionReadMode::Full) => CarBlockGroup::new(),
        (false, TransactionReadMode::Full) => CarBlockGroup::without_rewards(),
        (true, TransactionReadMode::SignaturePrefix) => {
            CarBlockGroup::with_transaction_signature_prefixes()
        }
        (false, TransactionReadMode::SignaturePrefix) => {
            CarBlockGroup::without_rewards_and_transaction_signature_prefixes()
        }
        (true, TransactionReadMode::None) => CarBlockGroup::without_transaction_payloads(),
        (false, TransactionReadMode::None) => {
            CarBlockGroup::without_rewards_and_transaction_payloads()
        }
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
        base58::encode_into_vec(bytes.as_ref(), &mut self.out)?;
        self.out.push(b'"');
        Ok(())
    }

    fn previous_blockhash(&mut self, previous_blockhash: Option<[u8; 32]>) -> Result<(), String> {
        if let Some(previous_blockhash) = previous_blockhash {
            self.bs58(previous_blockhash)
        } else {
            self.raw(b"null");
            Ok(())
        }
    }

    fn display(&mut self, value: impl JsonNumber) -> Result<(), String> {
        value.write_json(&mut self.out);
        Ok(())
    }

    fn f64(&mut self, value: f64) -> Result<(), String> {
        if value.is_finite() {
            let mut buffer = ryu::Buffer::new();
            self.raw(buffer.format(value).as_bytes());
            Ok(())
        } else {
            serde_json::to_writer(&mut self.out, &value)
                .map_err(|err| format!("failed to encode JSON float: {err}"))
        }
    }

    fn bool(&mut self, value: bool) {
        self.raw(if value { b"true" } else { b"false" });
    }
}

trait JsonNumber {
    fn write_json(self, out: &mut Vec<u8>);
}

macro_rules! impl_json_number {
    (unsigned: $($ty:ty),* $(,)?) => {
        $(
            impl JsonNumber for $ty {
                fn write_json(self, out: &mut Vec<u8>) {
                    write_u128(self as u128, out);
                }
            }
        )*
    };
    (signed: $($ty:ty),* $(,)?) => {
        $(
            impl JsonNumber for $ty {
                fn write_json(self, out: &mut Vec<u8>) {
                    write_i128(self as i128, out);
                }
            }
        )*
    };
}

impl_json_number!(unsigned: u8, u16, u32, u64, usize);
impl_json_number!(signed: i8, i16, i32, i64, isize);

fn write_u128(mut value: u128, out: &mut Vec<u8>) {
    if value == 0 {
        out.push(b'0');
        return;
    }

    let mut buf = [0u8; 39];
    let mut pos = buf.len();
    while value != 0 {
        pos -= 1;
        buf[pos] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    out.extend_from_slice(&buf[pos..]);
}

fn write_i128(value: i128, out: &mut Vec<u8>) {
    if value < 0 {
        out.push(b'-');
    }
    write_u128(value.unsigned_abs(), out);
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

    write_reward_array(w, &rewards.rewards)
}

fn write_reward_array(w: &mut JsonWriter, rewards: &[proto::Reward]) -> Result<(), String> {
    w.raw(b"[");
    for (index, reward) in rewards.iter().enumerate() {
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

fn write_reward_array_borrowed(
    w: &mut JsonWriter,
    rewards: &[borrowed_proto::Reward<'_>],
) -> Result<(), String> {
    w.raw(b"[");
    for (index, reward) in rewards.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.raw(b"{\"pubkey\":");
        w.string(reward.pubkey.as_ref())?;
        w.raw(b",\"lamports\":");
        w.display(reward.lamports)?;
        w.raw(b",\"postBalance\":");
        w.display(reward.post_balance)?;
        w.raw(b",\"rewardType\":");
        if let Some(reward_type) = reward_type_to_rpc_borrowed(reward.reward_type) {
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

fn reward_type_to_rpc_borrowed(reward_type: borrowed_proto::RewardType) -> Option<&'static str> {
    match reward_type {
        borrowed_proto::RewardType::Fee => Some("Fee"),
        borrowed_proto::RewardType::Rent => Some("Rent"),
        borrowed_proto::RewardType::Staking => Some("Staking"),
        borrowed_proto::RewardType::Voting => Some("Voting"),
        borrowed_proto::RewardType::Unspecified => None,
    }
}

fn write_full_transactions_owned(
    w: &mut JsonWriter,
    block: &mut CarBlockGroup,
) -> Result<(), String> {
    w.raw(b",\"transactions\":[");
    let mut tx_iter = block.transactions();
    let mut first = true;
    loop {
        let next = tx_iter
            .next_tx()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?;
        let Some((tx, meta)) = next else {
            break;
        };
        if !first {
            w.raw(b",");
        }
        first = false;
        write_block_transaction(w, tx, meta)?;
    }
    w.raw(b"]");
    Ok(())
}

fn write_full_transactions_borrowed(
    w: &mut JsonWriter,
    block: &mut CarBlockGroup,
) -> Result<(), String> {
    w.raw(b",\"transactions\":[");
    let mut tx_iter = block.transactions_borrowed_metadata();
    let mut first = true;
    loop {
        let next = tx_iter
            .next_tx()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?;
        let Some(tx) = next else {
            break;
        };
        if !first {
            w.raw(b",");
        }
        first = false;
        write_block_transaction_borrowed(w, tx.transaction, tx.metadata.as_ref())?;
    }
    w.raw(b"]");
    Ok(())
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
    w.raw(b",\"version\":");
    write_transaction_version(w, tx)?;
    w.raw(b"}");
    Ok(())
}

fn write_block_transaction_borrowed(
    w: &mut JsonWriter,
    tx: &VersionedTransaction<'_>,
    meta: Option<&BorrowedTransactionStatusMetaView<'_>>,
) -> Result<(), String> {
    w.raw(b"{\"transaction\":");
    write_encoded_transaction(w, tx)?;
    w.raw(b",\"meta\":");
    if let Some(meta) = meta {
        write_transaction_meta_borrowed(w, meta)?;
    } else {
        w.raw(b"null");
    }
    w.raw(b",\"version\":");
    write_transaction_version(w, tx)?;
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

fn write_accounts_transactions(
    w: &mut JsonWriter,
    block: &mut CarBlockGroup,
) -> Result<(), String> {
    w.raw(b",\"transactions\":[");
    let mut tx_iter = block.transactions();
    let mut first = true;
    loop {
        let next = tx_iter
            .next_tx()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?;
        let Some((tx, meta)) = next else {
            break;
        };
        if !first {
            w.raw(b",");
        }
        first = false;
        write_accounts_transaction(w, tx, meta)?;
    }
    w.raw(b"]");
    Ok(())
}

fn write_accounts_transactions_borrowed(
    w: &mut JsonWriter,
    block: &mut CarBlockGroup,
) -> Result<(), String> {
    w.raw(b",\"transactions\":[");
    let mut tx_iter = block.transactions_borrowed_metadata();
    let mut first = true;
    loop {
        let next = tx_iter
            .next_tx()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?;
        let Some(tx) = next else {
            break;
        };
        if !first {
            w.raw(b",");
        }
        first = false;
        write_accounts_transaction_borrowed(w, tx.transaction, tx.metadata.as_ref())?;
    }
    w.raw(b"]");
    Ok(())
}

fn write_accounts_transaction(
    w: &mut JsonWriter,
    tx: &VersionedTransaction<'_>,
    meta: Option<&proto::TransactionStatusMeta>,
) -> Result<(), String> {
    w.raw(b"{\"transaction\":{\"accountKeys\":");
    write_account_key_objects(w, tx, meta)?;
    w.raw(b",\"signatures\":");
    write_signature_array(w, tx)?;
    w.raw(b"},\"meta\":");
    if let Some(meta) = meta {
        write_accounts_transaction_meta(w, meta)?;
    } else {
        w.raw(b"null");
    }
    w.raw(b",\"version\":");
    write_transaction_version(w, tx)?;
    w.raw(b"}");
    Ok(())
}

fn write_accounts_transaction_borrowed(
    w: &mut JsonWriter,
    tx: &VersionedTransaction<'_>,
    meta: Option<&BorrowedTransactionStatusMetaView<'_>>,
) -> Result<(), String> {
    w.raw(b"{\"transaction\":{\"accountKeys\":");
    write_account_key_objects_borrowed(w, tx, meta)?;
    w.raw(b",\"signatures\":");
    write_signature_array(w, tx)?;
    w.raw(b"},\"meta\":");
    if let Some(meta) = meta {
        write_accounts_transaction_meta_borrowed(w, meta)?;
    } else {
        w.raw(b"null");
    }
    w.raw(b",\"version\":");
    write_transaction_version(w, tx)?;
    w.raw(b"}");
    Ok(())
}

fn write_signatures(w: &mut JsonWriter, block: &mut CarBlockGroup) -> Result<(), String> {
    w.raw(b",\"signatures\":[");
    let mut tx_iter = block.first_signatures();
    let mut first = true;
    loop {
        let next = tx_iter
            .next_signature()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?;
        let Some(signature) = next else {
            break;
        };
        if !first {
            w.raw(b",");
        }
        first = false;
        w.bs58(signature)?;
    }
    w.raw(b"]");
    Ok(())
}

fn write_signature_array(w: &mut JsonWriter, tx: &VersionedTransaction<'_>) -> Result<(), String> {
    w.raw(b"[");
    for (index, signature) in tx.signatures.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.bs58(signature)?;
    }
    w.raw(b"]");
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

fn write_transaction_version(
    w: &mut JsonWriter,
    tx: &VersionedTransaction<'_>,
) -> Result<(), String> {
    match tx.message {
        VersionedMessage::Legacy(_) => {
            w.raw(b"\"legacy\"");
            Ok(())
        }
        VersionedMessage::V0(_) => {
            w.raw(b"0");
            Ok(())
        }
    }
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

fn write_account_key_objects(
    w: &mut JsonWriter,
    tx: &VersionedTransaction<'_>,
    meta: Option<&proto::TransactionStatusMeta>,
) -> Result<(), String> {
    let (header, account_keys) = match &tx.message {
        VersionedMessage::Legacy(message) => (message.header, message.account_keys.as_slice()),
        VersionedMessage::V0(message) => (message.header, message.account_keys.as_slice()),
    };

    let required_signatures = header.num_required_signatures as usize;
    let readonly_signed = header.num_readonly_signed_accounts as usize;
    let readonly_unsigned = header.num_readonly_unsigned_accounts as usize;
    let signed_writable = required_signatures.saturating_sub(readonly_signed);
    let unsigned_writable_end = account_keys.len().saturating_sub(readonly_unsigned);

    w.raw(b"[");
    let mut first = true;
    for (index, key) in account_keys.iter().enumerate() {
        let signer = index < required_signatures;
        let header_writable = if signer {
            index < signed_writable
        } else {
            index < unsigned_writable_end
        };
        write_account_key_object(
            w,
            &mut first,
            *key,
            signer,
            header_writable,
            AccountKeySource::Transaction,
        )?;
    }

    if let Some(meta) = meta {
        for address in &meta.loaded_writable_addresses {
            write_account_key_object(
                w,
                &mut first,
                address,
                false,
                true,
                AccountKeySource::LookupTable,
            )?;
        }
        for address in &meta.loaded_readonly_addresses {
            write_account_key_object(
                w,
                &mut first,
                address,
                false,
                false,
                AccountKeySource::LookupTable,
            )?;
        }
    }

    w.raw(b"]");
    Ok(())
}

fn write_account_key_objects_borrowed(
    w: &mut JsonWriter,
    tx: &VersionedTransaction<'_>,
    meta: Option<&BorrowedTransactionStatusMetaView<'_>>,
) -> Result<(), String> {
    let (header, account_keys) = match &tx.message {
        VersionedMessage::Legacy(message) => (message.header, message.account_keys.as_slice()),
        VersionedMessage::V0(message) => (message.header, message.account_keys.as_slice()),
    };

    let required_signatures = header.num_required_signatures as usize;
    let readonly_signed = header.num_readonly_signed_accounts as usize;
    let readonly_unsigned = header.num_readonly_unsigned_accounts as usize;
    let signed_writable = required_signatures.saturating_sub(readonly_signed);
    let unsigned_writable_end = account_keys.len().saturating_sub(readonly_unsigned);

    w.raw(b"[");
    let mut first = true;
    for (index, key) in account_keys.iter().enumerate() {
        let signer = index < required_signatures;
        let header_writable = if signer {
            index < signed_writable
        } else {
            index < unsigned_writable_end
        };
        write_account_key_object(
            w,
            &mut first,
            *key,
            signer,
            header_writable,
            AccountKeySource::Transaction,
        )?;
    }

    if let Some(meta) = meta {
        for address in &meta.loaded_writable_addresses {
            write_account_key_object(
                w,
                &mut first,
                address.as_ref(),
                false,
                true,
                AccountKeySource::LookupTable,
            )?;
        }
        for address in &meta.loaded_readonly_addresses {
            write_account_key_object(
                w,
                &mut first,
                address.as_ref(),
                false,
                false,
                AccountKeySource::LookupTable,
            )?;
        }
    }

    w.raw(b"]");
    Ok(())
}

fn write_account_key_object(
    w: &mut JsonWriter,
    first: &mut bool,
    pubkey: impl AsRef<[u8]>,
    signer: bool,
    writable: bool,
    source: AccountKeySource,
) -> Result<(), String> {
    if !*first {
        w.raw(b",");
    }
    *first = false;

    w.raw(b"{\"pubkey\":");
    w.bs58(pubkey)?;
    w.raw(b",\"signer\":");
    w.bool(signer);
    w.raw(b",\"source\":");
    w.raw(source.json_literal());
    w.raw(b",\"writable\":");
    w.bool(writable);
    w.raw(b"}");
    Ok(())
}

#[derive(Clone, Copy)]
enum AccountKeySource {
    Transaction,
    LookupTable,
}

impl AccountKeySource {
    fn json_literal(self) -> &'static [u8] {
        match self {
            Self::Transaction => b"\"transaction\"",
            Self::LookupTable => b"\"lookupTable\"",
        }
    }
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
    w.raw(b",\"status\":");
    write_transaction_status(w, meta.err.as_ref())?;
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
    if let Some(compute_units_consumed) = meta.compute_units_consumed {
        w.raw(b",\"computeUnitsConsumed\":");
        w.display(compute_units_consumed)?;
    }
    if let Some(cost_units) = meta.cost_units {
        w.raw(b",\"costUnits\":");
        w.display(cost_units)?;
    }
    w.raw(b",\"innerInstructions\":");
    write_inner_instructions(w, meta)?;
    w.raw(b",\"loadedAddresses\":");
    write_loaded_addresses(w, meta)?;
    w.raw(b",\"rewards\":");
    write_transaction_rewards(w, &meta.rewards)?;
    if !meta.return_data_none
        && let Some(return_data) = meta.return_data.as_ref()
    {
        w.raw(b",\"returnData\":");
        write_return_data(w, return_data)?;
    }
    w.raw(b"}");
    Ok(())
}

fn write_accounts_transaction_meta(
    w: &mut JsonWriter,
    meta: &proto::TransactionStatusMeta,
) -> Result<(), String> {
    w.raw(b"{\"err\":");
    write_transaction_error(w, meta.err.as_ref())?;
    w.raw(b",\"status\":");
    write_transaction_status(w, meta.err.as_ref())?;
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
    w.raw(b"}");
    Ok(())
}

fn write_transaction_meta_borrowed(
    w: &mut JsonWriter,
    meta: &BorrowedTransactionStatusMetaView<'_>,
) -> Result<(), String> {
    w.raw(b"{\"err\":");
    write_transaction_error_borrowed(w, meta.err.as_ref())?;
    w.raw(b",\"status\":");
    write_transaction_status_borrowed(w, meta.err.as_ref())?;
    w.raw(b",\"fee\":");
    w.display(meta.fee)?;
    w.raw(b",\"preBalances\":");
    write_u64_array(w, &meta.pre_balances)?;
    w.raw(b",\"postBalances\":");
    write_u64_array(w, &meta.post_balances)?;
    w.raw(b",\"preTokenBalances\":");
    write_token_balances_borrowed(w, &meta.pre_token_balances)?;
    w.raw(b",\"postTokenBalances\":");
    write_token_balances_borrowed(w, &meta.post_token_balances)?;
    w.raw(b",\"logMessages\":");
    write_log_messages_borrowed(w, meta)?;
    if let Some(compute_units_consumed) = meta.compute_units_consumed {
        w.raw(b",\"computeUnitsConsumed\":");
        w.display(compute_units_consumed)?;
    }
    if let Some(cost_units) = meta.cost_units {
        w.raw(b",\"costUnits\":");
        w.display(cost_units)?;
    }
    w.raw(b",\"innerInstructions\":");
    write_inner_instructions_borrowed(w, meta)?;
    w.raw(b",\"loadedAddresses\":");
    write_loaded_addresses_borrowed(w, meta)?;
    w.raw(b",\"rewards\":");
    write_transaction_rewards_borrowed(w, &meta.rewards)?;
    if !meta.return_data_none
        && let Some(return_data) = meta.return_data.as_ref()
    {
        w.raw(b",\"returnData\":");
        write_return_data_borrowed(w, return_data)?;
    }
    w.raw(b"}");
    Ok(())
}

fn write_accounts_transaction_meta_borrowed(
    w: &mut JsonWriter,
    meta: &BorrowedTransactionStatusMetaView<'_>,
) -> Result<(), String> {
    w.raw(b"{\"err\":");
    write_transaction_error_borrowed(w, meta.err.as_ref())?;
    w.raw(b",\"status\":");
    write_transaction_status_borrowed(w, meta.err.as_ref())?;
    w.raw(b",\"fee\":");
    w.display(meta.fee)?;
    w.raw(b",\"preBalances\":");
    write_u64_array(w, &meta.pre_balances)?;
    w.raw(b",\"postBalances\":");
    write_u64_array(w, &meta.post_balances)?;
    w.raw(b",\"preTokenBalances\":");
    write_token_balances_borrowed(w, &meta.pre_token_balances)?;
    w.raw(b",\"postTokenBalances\":");
    write_token_balances_borrowed(w, &meta.post_token_balances)?;
    w.raw(b"}");
    Ok(())
}

fn write_token_balances(
    w: &mut JsonWriter,
    balances: &[proto::TokenBalance],
) -> Result<(), String> {
    if balances.is_empty() {
        w.raw(b"[]");
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
            write_ui_amount(w, amount)?;
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

fn write_token_balances_borrowed(
    w: &mut JsonWriter,
    balances: &[borrowed_proto::TokenBalance<'_>],
) -> Result<(), String> {
    if balances.is_empty() {
        w.raw(b"[]");
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
        w.string(balance.mint.as_ref())?;
        w.raw(b",\"owner\":");
        if balance.owner.is_empty() {
            w.raw(b"null");
        } else {
            w.string(balance.owner.as_ref())?;
        }
        w.raw(b",\"programId\":");
        if balance.program_id.is_empty() {
            w.raw(b"null");
        } else {
            w.string(balance.program_id.as_ref())?;
        }
        w.raw(b",\"uiTokenAmount\":");
        if let Some(amount) = balance.ui_token_amount.as_ref() {
            w.raw(b"{\"amount\":");
            w.string(amount.amount.as_ref())?;
            w.raw(b",\"decimals\":");
            w.display(amount.decimals)?;
            w.raw(b",\"uiAmount\":");
            write_ui_amount_borrowed(w, amount)?;
            w.raw(b",\"uiAmountString\":");
            w.string(amount.ui_amount_string.as_ref())?;
            w.raw(b"}");
        } else {
            w.raw(b"null");
        }
        w.raw(b"}");
    }
    w.raw(b"]");
    Ok(())
}

fn write_transaction_rewards(w: &mut JsonWriter, rewards: &[proto::Reward]) -> Result<(), String> {
    if rewards.is_empty() {
        w.raw(b"null");
        return Ok(());
    }

    write_reward_array(w, rewards)
}

fn write_transaction_rewards_borrowed(
    w: &mut JsonWriter,
    rewards: &[borrowed_proto::Reward<'_>],
) -> Result<(), String> {
    if rewards.is_empty() {
        w.raw(b"null");
        return Ok(());
    }

    write_reward_array_borrowed(w, rewards)
}

fn write_ui_amount(w: &mut JsonWriter, amount: &proto::UiTokenAmount) -> Result<(), String> {
    if amount.amount == "0" {
        w.raw(b"null");
        return Ok(());
    }

    let value = amount
        .ui_amount_string
        .parse::<f64>()
        .unwrap_or(amount.ui_amount);
    w.f64(value)
}

fn write_ui_amount_borrowed(
    w: &mut JsonWriter,
    amount: &borrowed_proto::UiTokenAmount<'_>,
) -> Result<(), String> {
    if amount.amount == "0" {
        w.raw(b"null");
        return Ok(());
    }

    let value = amount
        .ui_amount_string
        .parse::<f64>()
        .unwrap_or(amount.ui_amount);
    w.f64(value)
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

fn write_log_messages_borrowed(
    w: &mut JsonWriter,
    meta: &BorrowedTransactionStatusMetaView<'_>,
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
        w.string(log.as_ref())?;
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

fn write_inner_instructions_borrowed(
    w: &mut JsonWriter,
    meta: &BorrowedTransactionStatusMetaView<'_>,
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
            write_inner_instruction_borrowed(w, instruction)?;
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

fn write_inner_instruction_borrowed(
    w: &mut JsonWriter,
    instruction: &borrowed_proto::InnerInstruction<'_>,
) -> Result<(), String> {
    w.raw(b"{\"programIdIndex\":");
    w.display(instruction.program_id_index)?;
    w.raw(b",\"accounts\":");
    write_u8_array(w, instruction.accounts.as_ref())?;
    w.raw(b",\"data\":");
    w.bs58(instruction.data.as_ref())?;
    w.raw(b",\"stackHeight\":");
    write_option_u32(
        w,
        (instruction.stack_height != 0).then_some(instruction.stack_height),
    )?;
    w.raw(b"}");
    Ok(())
}

fn write_loaded_addresses(
    w: &mut JsonWriter,
    meta: &proto::TransactionStatusMeta,
) -> Result<(), String> {
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

fn write_loaded_addresses_borrowed(
    w: &mut JsonWriter,
    meta: &BorrowedTransactionStatusMetaView<'_>,
) -> Result<(), String> {
    w.raw(b"{\"writable\":[");
    for (index, address) in meta.loaded_writable_addresses.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.bs58(address.as_ref())?;
    }
    w.raw(b"],\"readonly\":[");
    for (index, address) in meta.loaded_readonly_addresses.iter().enumerate() {
        if index != 0 {
            w.raw(b",");
        }
        w.bs58(address.as_ref())?;
    }
    w.raw(b"]}");
    Ok(())
}

fn write_return_data(w: &mut JsonWriter, return_data: &proto::ReturnData) -> Result<(), String> {
    w.raw(b"{\"programId\":");
    w.bs58(&return_data.program_id)?;
    w.raw(b",\"data\":[");
    w.string(&BASE64.encode(&return_data.data))?;
    w.raw(b",\"base64\"]}");
    Ok(())
}

fn write_return_data_borrowed(
    w: &mut JsonWriter,
    return_data: &borrowed_proto::ReturnData<'_>,
) -> Result<(), String> {
    w.raw(b"{\"programId\":");
    w.bs58(return_data.program_id.as_ref())?;
    w.raw(b",\"data\":[");
    w.string(&BASE64.encode(return_data.data.as_ref()))?;
    w.raw(b",\"base64\"]}");
    Ok(())
}

fn write_transaction_status(
    w: &mut JsonWriter,
    err: Option<&proto::TransactionError>,
) -> Result<(), String> {
    if let Some(err) = err {
        w.raw(b"{\"Err\":");
        write_transaction_error(w, Some(err))?;
        w.raw(b"}");
    } else {
        w.raw(b"{\"Ok\":null}");
    }
    Ok(())
}

fn write_transaction_status_borrowed(
    w: &mut JsonWriter,
    err: Option<&borrowed_proto::TransactionError<'_>>,
) -> Result<(), String> {
    if let Some(err) = err {
        w.raw(b"{\"Err\":");
        write_transaction_error_borrowed(w, Some(err))?;
        w.raw(b"}");
    } else {
        w.raw(b"{\"Ok\":null}");
    }
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

    write_transaction_error_bytes(w, &err.err)
}

fn write_transaction_error_borrowed(
    w: &mut JsonWriter,
    err: Option<&borrowed_proto::TransactionError<'_>>,
) -> Result<(), String> {
    let Some(err) = err else {
        w.raw(b"null");
        return Ok(());
    };

    write_transaction_error_bytes(w, err.err.as_ref())
}

fn write_transaction_error_bytes(w: &mut JsonWriter, bytes: &[u8]) -> Result<(), String> {
    let decoded = decode_transaction_error_bytes(bytes)
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

    #[test]
    fn writes_rpc_meta_shape_for_empty_optional_lists() {
        let meta = proto::TransactionStatusMeta {
            err: None,
            fee: 5000,
            pre_balances: vec![10],
            post_balances: vec![5],
            inner_instructions: Vec::new(),
            inner_instructions_none: true,
            log_messages: Vec::new(),
            log_messages_none: true,
            pre_token_balances: vec![proto::TokenBalance {
                account_index: 1,
                mint: "mint".to_string(),
                ui_token_amount: Some(proto::UiTokenAmount {
                    ui_amount: 0.0,
                    decimals: 6,
                    amount: "0".to_string(),
                    ui_amount_string: "0".to_string(),
                }),
                owner: "owner".to_string(),
                program_id: "program".to_string(),
            }],
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            return_data_none: true,
            compute_units_consumed: Some(12),
            cost_units: Some(34),
        };

        let mut writer = JsonWriter::with_capacity(512);
        write_transaction_meta(&mut writer, &meta).expect("write meta");
        let value: serde_json::Value =
            serde_json::from_slice(&writer.into_inner()).expect("valid JSON");

        assert_eq!(value["status"], serde_json::json!({ "Ok": null }));
        assert_eq!(
            value["preTokenBalances"][0]["uiTokenAmount"]["uiAmount"],
            serde_json::json!(null)
        );
        assert_eq!(value["postTokenBalances"], serde_json::json!([]));
        assert_eq!(
            value["loadedAddresses"],
            serde_json::json!({ "writable": [], "readonly": [] })
        );
        assert_eq!(value["rewards"], serde_json::json!(null));
        assert_eq!(value["costUnits"], serde_json::json!(34));
        assert!(value.get("returnData").is_none());
    }

    #[test]
    fn writes_return_data_when_present() {
        let meta = proto::TransactionStatusMeta {
            err: None,
            fee: 0,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: Vec::new(),
            inner_instructions_none: true,
            log_messages: Vec::new(),
            log_messages_none: true,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: Some(proto::ReturnData {
                program_id: vec![1; 32],
                data: b"hello".to_vec(),
            }),
            return_data_none: false,
            compute_units_consumed: None,
            cost_units: None,
        };

        let mut writer = JsonWriter::with_capacity(512);
        write_transaction_meta(&mut writer, &meta).expect("write meta");
        let value: serde_json::Value =
            serde_json::from_slice(&writer.into_inner()).expect("valid JSON");

        assert_eq!(
            value["returnData"]["data"],
            serde_json::json!(["aGVsbG8=", "base64"])
        );
        assert!(value["returnData"]["programId"].is_string());
        assert!(value.get("computeUnitsConsumed").is_none());
        assert!(value.get("costUnits").is_none());
    }

    #[test]
    fn base58_paths_match_known_vectors_and_roundtrip_large_input() {
        for (bytes, expected) in [
            (&b""[..], ""),
            (&b"\0"[..], "1"),
            (&b"\0\0\0\0"[..], "1111"),
            (&b"hello world"[..], "StV1DL6CwTryKyV"),
        ] {
            let mut writer =
                JsonWriter::with_capacity(base58_turbo::BITCOIN.encoded_len(bytes.len()) + 2);
            writer.bs58(bytes).expect("write base58");
            assert_eq!(
                String::from_utf8(writer.into_inner()).expect("utf8"),
                format!("\"{expected}\"")
            );
        }

        for len in [31usize, 32, 33, 64, 65, 256, 1024] {
            let bytes = (0..len)
                .map(|index| ((index * 37 + 11) & 0xff) as u8)
                .collect::<Vec<_>>();
            let mut writer = JsonWriter::with_capacity(base58_turbo::BITCOIN.encoded_len(len) + 2);
            writer.bs58(&bytes).expect("write base58");

            let encoded = String::from_utf8(writer.into_inner()).expect("utf8");
            let encoded = encoded.trim_matches('"');
            let mut decoded = vec![0; base58_turbo::BITCOIN.decoded_len(encoded.len())];
            let decoded_len = base58_turbo::BITCOIN
                .decode_into(encoded, &mut decoded)
                .expect("decode base58");
            decoded.truncate(decoded_len);
            assert_eq!(decoded, bytes, "base58 roundtrip mismatch for {len} bytes");
        }

        let mut large = vec![0; 1025];
        large[1024] = 1;
        let mut writer =
            JsonWriter::with_capacity(base58_turbo::BITCOIN.encoded_len(large.len()) + 2);
        writer.bs58(&large).expect("write large base58");
        assert_eq!(
            String::from_utf8(writer.into_inner()).expect("utf8"),
            format!("\"{}2\"", "1".repeat(1024))
        );
    }

    #[test]
    fn writes_ui_amount_from_decimal_string() {
        let amount = proto::UiTokenAmount {
            ui_amount: 71106423.22310276,
            decimals: 9,
            amount: "71106423223102772".to_string(),
            ui_amount_string: "71106423.223102772".to_string(),
        };

        let mut writer = JsonWriter::with_capacity(64);
        write_ui_amount(&mut writer, &amount).expect("write ui amount");
        let value: serde_json::Value =
            serde_json::from_slice(&writer.into_inner()).expect("valid JSON");

        assert_eq!(value, serde_json::json!(71106423.22310278));
    }

    #[test]
    fn writes_integer_edges() {
        let mut writer = JsonWriter::with_capacity(128);
        writer.display(0u64).unwrap();
        writer.raw(b",");
        writer.display(u64::MAX).unwrap();
        writer.raw(b",");
        writer.display(i64::MIN).unwrap();
        writer.raw(b",");
        writer.display(i64::MAX).unwrap();

        assert_eq!(
            String::from_utf8(writer.into_inner()).unwrap(),
            "0,18446744073709551615,-9223372036854775808,9223372036854775807"
        );
    }
}
