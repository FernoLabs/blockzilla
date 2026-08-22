//! Selective, allocation-light projection of Archive V2 transaction metadata.
//!
//! The projector validates every field it crosses, borrows inner-instruction
//! account-index slices, and skips raw instruction data, logs, token balances,
//! and rewards without materializing them. Legacy callers can stop after inner
//! instructions. V0 callers continue through the loaded-address vectors.

use blockzilla_format::{
    CompactInnerInstruction, CompactPubkey, DataArray, WincodeLeb128Config,
    canonicalize_archive_v2_metadata_owned,
    program_logs::{
        system_program::{PubkeyOrString, SystemAddress, SystemProgramLog},
        token_2022::Token2022Log,
    },
};
use wincode::{ReadResult, SchemaRead, error::invalid_tag_encoding, io::Reader};

use crate::MAX_MESSAGE_ACCOUNTS;

type Cfg = WincodeLeb128Config;
const MAX_LOG_TABLE_ITEMS: usize = 1 << 20;

#[derive(Default)]
struct LogReferences {
    maximum_string_id: Option<u32>,
    maximum_data_id: Option<u32>,
}

impl LogReferences {
    fn string(&mut self, id: u32) {
        self.maximum_string_id = Some(self.maximum_string_id.unwrap_or_default().max(id));
    }

    fn data(&mut self, id: u32) {
        self.maximum_data_id = Some(self.maximum_data_id.unwrap_or_default().max(id));
    }

    fn validate(&self, string_count: usize, data_count: usize) -> ReadResult<()> {
        if self
            .maximum_string_id
            .is_some_and(|id| id as usize >= string_count)
        {
            return Err(wincode::error::invalid_value(
                "log string reference is outside the string table",
            ));
        }
        if self
            .maximum_data_id
            .is_some_and(|id| id as usize >= data_count)
        {
            return Err(wincode::error::invalid_value(
                "log data reference is outside the data table",
            ));
        }
        Ok(())
    }
}

#[inline]
fn get<'de, T: SchemaRead<'de, Cfg>>(cursor: &mut &'de [u8]) -> ReadResult<T::Dst> {
    T::get(&mut *cursor)
}

#[inline]
fn validate_pubkey(
    value: CompactPubkey,
    registry_entries: Option<u32>,
) -> ReadResult<CompactPubkey> {
    if let (CompactPubkey::Id(id), Some(entries)) = (value, registry_entries)
        && (id == 0 || id > entries)
    {
        return Err(wincode::error::invalid_value(
            "pubkey registry ID exceeds the admitted registry",
        ));
    }
    Ok(value)
}

#[inline]
fn get_pubkey(cursor: &mut &[u8], registry_entries: Option<u32>) -> ReadResult<CompactPubkey> {
    validate_pubkey(get::<CompactPubkey>(cursor)?, registry_entries)
}

#[inline]
fn get_optional_pubkey(
    cursor: &mut &[u8],
    registry_entries: Option<u32>,
) -> ReadResult<Option<CompactPubkey>> {
    let value = get::<Option<CompactPubkey>>(cursor)?;
    value
        .map(|pubkey| validate_pubkey(pubkey, registry_entries))
        .transpose()
}

#[inline]
fn read_len(cursor: &mut &[u8]) -> ReadResult<usize> {
    let len = get::<u64>(cursor)?;
    usize::try_from(len).map_err(|_| wincode::error::pointer_sized_decode_error())
}

#[inline]
fn read_bounded_len(cursor: &mut &[u8], maximum: usize, error: &'static str) -> ReadResult<usize> {
    let len = read_len(cursor)?;
    if len > maximum {
        return Err(wincode::error::invalid_value(error));
    }
    Ok(len)
}

#[inline]
fn read_len_bounded_by_remaining(cursor: &mut &[u8], error: &'static str) -> ReadResult<usize> {
    read_bounded_len(cursor, cursor.len(), error)
}

#[inline]
fn skip_bytes(cursor: &mut &[u8]) -> ReadResult<()> {
    let len = read_len_bounded_by_remaining(cursor, "byte string length exceeds remaining input")?;
    cursor.take_borrowed(len)?;
    Ok(())
}

#[inline]
fn skip_string(cursor: &mut &[u8]) -> ReadResult<()> {
    let len = read_len_bounded_by_remaining(cursor, "string length exceeds remaining input")?;
    let bytes = cursor.take_borrowed(len)?;
    std::str::from_utf8(bytes)
        .map_err(|_| wincode::error::invalid_value("string is not valid UTF-8"))?;
    Ok(())
}

/// One decoded inner instruction: `program_id_index` plus a borrowed slice
/// of account indices. `data` and `stack_height` have already been skipped
/// / discarded.
pub struct BorrowedArchiveV2InnerInstruction<'de> {
    pub program_id_index: u32,
    pub accounts: &'de [u8],
}

pub struct ProjectedArchiveV2MetadataPrefix {
    pub has_error: bool,
    pub pre_balance_count: usize,
    pub post_balance_count: usize,
    pub inner_instructions_present: bool,
    /// Known only when the projection traversed the metadata tail.
    pub logs_present: Option<bool>,
    /// Known only when the projection traversed the metadata tail.
    pub token_balances_present: Option<bool>,
    /// Known only after complete-record validation.
    pub return_data_present: Option<bool>,
    pub loaded_addresses: Option<(Vec<CompactPubkey>, Vec<CompactPubkey>)>,
}

/// Decode just the archived transaction outcome. This is sufficient when the
/// row flags prove there are neither inner instructions nor loaded addresses.
pub fn project_archive_v2_metadata_error(cursor: &mut &[u8]) -> ReadResult<bool> {
    project_archive_v2_metadata_error_bounded(cursor, None, None)
}

fn project_archive_v2_metadata_error_bounded(
    cursor: &mut &[u8],
    top_level_instruction_count: Option<usize>,
    total_message_accounts: Option<usize>,
) -> ReadResult<bool> {
    match get::<u8>(cursor)? {
        0 => Ok(false),
        1 => {
            skip_transaction_error(cursor, top_level_instruction_count, total_message_accounts)?;
            Ok(true)
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

/// Decode only the transaction-outcome prefix from one metadata slice.
pub fn project_archive_v2_metadata_outcome(bytes: &[u8]) -> ReadResult<bool> {
    let mut cursor = bytes;
    project_archive_v2_metadata_error(&mut cursor)
}

fn skip_transaction_error(
    cursor: &mut &[u8],
    top_level_instruction_count: Option<usize>,
    total_message_accounts: Option<usize>,
) -> ReadResult<()> {
    let tag = get::<u8>(cursor)?;
    match tag {
        8 => {
            let instruction_index = usize::from(get::<u8>(cursor)?);
            if top_level_instruction_count.is_some_and(|count| instruction_index >= count) {
                return Err(wincode::error::invalid_value(
                    "instruction-error index is outside top-level instructions",
                ));
            }
            skip_instruction_error(cursor)?;
        }
        30 => {
            let instruction_index = usize::from(get::<u8>(cursor)?);
            if top_level_instruction_count.is_some_and(|count| instruction_index >= count) {
                return Err(wincode::error::invalid_value(
                    "duplicate-instruction index is outside top-level instructions",
                ));
            }
        }
        31 | 35 => {
            let account_index = usize::from(get::<u8>(cursor)?);
            if total_message_accounts.is_some_and(|count| account_index >= count) {
                return Err(wincode::error::invalid_value(
                    "transaction-error account index is outside resolved message accounts",
                ));
            }
        }
        0..=38 => {}
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_instruction_error(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u8>(cursor)?;
    match tag {
        25 => {
            get::<u32>(cursor)?;
        }
        44 => skip_string(cursor)?,
        0..=53 => {}
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn read_inner_instruction<'de>(
    cursor: &mut &'de [u8],
) -> ReadResult<BorrowedArchiveV2InnerInstruction<'de>> {
    let program_id_index = get::<u32>(cursor)?;
    let accounts_len = read_len_bounded_by_remaining(
        cursor,
        "inner-instruction account-index count exceeds remaining input",
    )?;
    let accounts = cursor.take_borrowed(accounts_len)?;
    skip_bytes(cursor)?; // data: Vec<u8> — never read, never allocated.
    get::<Option<u32>>(cursor)?; // stack_height — discarded.
    Ok(BorrowedArchiveV2InnerInstruction {
        program_id_index,
        accounts,
    })
}

#[derive(Clone, Copy)]
pub struct ArchiveV2MetadataProjectionLimits {
    pub total_message_accounts: usize,
    pub top_level_instruction_count: usize,
}

fn skip_balances(cursor: &mut &[u8], maximum: usize) -> ReadResult<usize> {
    let count = read_bounded_len(
        cursor,
        maximum,
        "balance count exceeds total message account count",
    )?;
    for _ in 0..count {
        get::<u64>(cursor)?;
    }
    Ok(count)
}

fn validate_pubkey_or_string(
    value: PubkeyOrString,
    registry_entries: Option<u32>,
    references: &mut LogReferences,
) -> ReadResult<()> {
    match value {
        PubkeyOrString::Pubkey(pubkey) => {
            validate_pubkey(pubkey, registry_entries)?;
        }
        PubkeyOrString::Text(id) => references.string(id),
    }
    Ok(())
}

fn validate_system_address(
    value: SystemAddress,
    registry_entries: Option<u32>,
    references: &mut LogReferences,
) -> ReadResult<()> {
    match value {
        SystemAddress::Pubkey(value) => {
            validate_pubkey_or_string(value, registry_entries, references)
        }
        SystemAddress::Debug { address, base } => {
            validate_pubkey_or_string(address, registry_entries, references)?;
            if let Some(base) = base {
                validate_pubkey_or_string(base, registry_entries, references)?;
            }
            Ok(())
        }
    }
}

fn validate_system_program_log(
    value: SystemProgramLog,
    registry_entries: Option<u32>,
    references: &mut LogReferences,
) -> ReadResult<()> {
    use SystemProgramLog as Log;
    match value {
        Log::CreateAddressMismatch {
            provided_addr,
            derived_addr,
        }
        | Log::TransferFromAddressMismatch {
            provided_addr,
            derived_addr,
        } => {
            validate_pubkey(provided_addr, registry_entries)?;
            validate_pubkey_or_string(derived_addr, registry_entries, references)
        }
        Log::CreateAccountAlreadyInUse { addr }
        | Log::AllocateAlreadyInUse { addr }
        | Log::AllocateToMustSign { addr }
        | Log::AllocateAccountAlreadyInUse { addr }
        | Log::AssignAccountMustSign { addr }
        | Log::CreateAccountAccountAlreadyInUse { addr } => {
            validate_system_address(addr, registry_entries, references)
        }
        Log::TransferFromMustSign { from } => {
            validate_pubkey(from, registry_entries)?;
            Ok(())
        }
        Log::NonceAccountMustBeWriteable { account, .. }
        | Log::NonceAccountMustBeSigner { account, .. }
        | Log::NonceAccountMustSign { account, .. }
        | Log::NonceAccountStateInvalid { account, .. } => {
            validate_pubkey_or_string(account, registry_entries, references)
        }
        Log::Instruction(_)
        | Log::AllocateRequestedTooLarge { .. }
        | Log::CreateAccountDataSizeLimitedInInnerInstructions { .. }
        | Log::TransferFromMustNotCarryData
        | Log::TransferInsufficient { .. }
        | Log::AdvanceNonceRecentBlockhashesEmpty
        | Log::InitializeNonceRecentBlockhashesEmpty
        | Log::NonceInsufficientLamports { .. }
        | Log::NonceCanOnlyAdvanceOncePerSlot { .. } => Ok(()),
        Log::AuthorizeNonceAccount { msg } => {
            references.string(msg);
            Ok(())
        }
    }
}

fn validate_token_2022_log(
    value: Token2022Log,
    registry_entries: Option<u32>,
    references: &mut LogReferences,
) -> ReadResult<()> {
    match value {
        Token2022Log::ErrorHarvestingFrom { account_key, error }
        | Token2022Log::ErrorHarvestingFrom2 { account_key, error }
        | Token2022Log::ErrorHarvestingFrom3 { account_key, error }
        | Token2022Log::ErrorHarvestingFrom4 { account_key, error } => {
            validate_pubkey(account_key, registry_entries)?;
            references.string(error);
            Ok(())
        }
        _ => Ok(()),
    }
}

fn skip_program_log(
    cursor: &mut &[u8],
    registry_entries: Option<u32>,
    references: &mut LogReferences,
) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => {}
        1 => {
            get::<blockzilla_format::program_logs::token::TokenLog>(cursor)?;
        }
        2 => {
            validate_token_2022_log(get::<Token2022Log>(cursor)?, registry_entries, references)?;
        }
        3 => {
            get::<blockzilla_format::program_logs::associated_token_account::TokenLog>(cursor)?;
        }
        4 => {
            use blockzilla_format::program_logs::address_lookup_table::AddressLookupTableLog as Log;
            match get::<Log>(cursor)? {
                Log::NotARecentSlot {
                    untrusted_recent_slot,
                } => references.string(untrusted_recent_slot),
                Log::TableAddressMustMatchDerivedAddress { derived_table_key } => {
                    references.string(derived_table_key);
                }
                Log::ExtendedLookupTableLengthWouldExceedMaxCapacity {
                    new_table_addresses_len,
                    lookup_table_max_addresses,
                } => {
                    references.string(new_table_addresses_len);
                    references.string(lookup_table_max_addresses);
                }
                Log::TableCannotBeClosedUntilFullyDeactivatedInBlocks { remaining_blocks } => {
                    references.string(remaining_blocks);
                }
                Log::Error(_) | Log::Instruction(_) => {}
            }
        }
        5 => {
            use blockzilla_format::program_logs::loader_v3::LoaderV3Log as Log;
            match get::<Log>(cursor)? {
                Log::WriteOverflow {
                    buffer_data_len,
                    end_offset,
                } => {
                    references.string(buffer_data_len);
                    references.string(end_offset);
                }
                Log::ExtendedProgramDataLengthExceedsMax {
                    new_len,
                    max_permitted_data_length,
                } => {
                    references.string(new_len);
                    references.string(max_permitted_data_length);
                }
                Log::DeployedProgram { program_key }
                | Log::DeployedProgramPlain { program_key }
                | Log::UpgradedProgram { program_key }
                | Log::UpgradedProgramPlain { program_key } => references.string(program_key),
                Log::NewAuthorityDebug { new_authority }
                | Log::NewAuthorityDebugPlain { new_authority }
                | Log::NewAuthorityDebug2 { new_authority } => references.string(new_authority),
                Log::ClosedUninitialized { key }
                | Log::ClosedBuffer { key }
                | Log::ClosedProgram { key } => references.string(key),
                Log::ExtendedProgramDataAccountBy { additional_bytes } => {
                    references.string(additional_bytes);
                }
                Log::Static(_) => {}
            }
        }
        6 => {
            use blockzilla_format::program_logs::loader_v4::LoaderV4Log as Log;
            if let Log::InsufficientLamportsRequired { required_lamports } = get::<Log>(cursor)? {
                references.string(required_lamports);
            }
        }
        7 => {
            use blockzilla_format::program_logs::memo::MemoLog as Log;
            match get::<Log>(cursor)? {
                Log::SignedByDebug { address } => references.string(address),
                Log::InvalidUtf8FromByte { valid_up_to } => references.string(valid_up_to),
                Log::MemoLenAndDebug { len, memo } => {
                    references.string(len);
                    references.string(memo);
                }
            }
        }
        8 => {
            use blockzilla_format::program_logs::record::RecordLog as Log;
            let Log::ReallocatingPlusBytesDebug { bytes } = get::<Log>(cursor)?;
            references.string(bytes);
        }
        9 => {
            get::<blockzilla_format::program_logs::transfer_hook::TransferHookLog>(cursor)?;
        }
        10 => {
            use blockzilla_format::program_logs::account_compression::AccountCompressionLog as Log;
            match get::<Log>(cursor)? {
                Log::CanopyLengthMismatch {
                    canopy_bytes_len,
                    node_size,
                } => {
                    references.string(canopy_bytes_len);
                    references.string(node_size);
                }
                Log::FailedToLoadTypeSizeMismatch {
                    type_name,
                    data_len,
                    expected_size,
                } => {
                    references.string(type_name);
                    references.string(data_len);
                    references.string(expected_size);
                }
            }
        }
        11 => {
            use blockzilla_format::program_logs::stake::StakeProgramLog as Log;
            if let Log::Error { msg } = get::<Log>(cursor)? {
                references.string(msg);
            }
        }
        12 => {
            use blockzilla_format::program_logs::zk_elgamal_proof::ZkElgamalProofLog as Log;
            if let Log::ProofVerificationFailed { err } = get::<Log>(cursor)? {
                references.string(err);
            }
        }
        13 | 16 => {
            references.string(get::<u32>(cursor)?);
        }
        14 => {
            references.string(get::<u32>(cursor)?);
            get::<u32>(cursor)?;
            references.string(get::<u32>(cursor)?);
        }
        15 => {
            references.string(get::<u32>(cursor)?);
            get::<u32>(cursor)?;
            references.string(get::<u32>(cursor)?);
            get::<u32>(cursor)?;
            references.string(get::<u32>(cursor)?);
        }
        17 => skip_known_program_log(cursor)?,
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_known_program_log(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => skip_drift_log(cursor)?,
        1 => skip_okx_router_log(cursor)?,
        2 => skip_phoenix_perps_log(cursor)?,
        3 => skip_phoenix_v1_log(cursor)?,
        4 => {
            get::<blockzilla_format::program_logs::known_programs::raydium_amm::RaydiumAmmLog>(
                cursor,
            )?;
        }
        5 => {
            get::<
                blockzilla_format::program_logs::known_programs::static_programs::StaticProgramLog,
            >(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_drift_log(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => skip_bytes(cursor)?,
        1 => {
            get::<u64>(cursor)?;
        }
        2 | 5 | 6 | 12 | 16 => {
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
        }
        3 | 4 | 7..=11 | 13..=15 => {}
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_okx_router_log(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => {
            skip_string(cursor)?;
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            get::<blockzilla_format::program_logs::known_programs::okx_router::AmountInSpelling>(
                cursor,
            )?;
        }
        1 => skip_string(cursor)?,
        2..=4 => {
            get::<u64>(cursor)?;
        }
        5 | 11 => {}
        6 | 7 => {
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
        }
        8 => {
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            skip_string(cursor)?;
        }
        9 => {
            get::<u8>(cursor)?;
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
        }
        10 => {
            get::<blockzilla_format::program_logs::known_programs::okx_router::OkxRouteLabel>(
                cursor,
            )?;
        }
        12 => {
            get::<blockzilla_format::program_logs::known_programs::okx_router::OkxMarker>(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_phoenix_perps_log(cursor: &mut &[u8]) -> ReadResult<()> {
    match get::<u32>(cursor)? {
        0 => skip_bytes(cursor),
        1 => {
            get::<blockzilla_format::program_logs::known_programs::phoenix_perps::PhoenixPerpsStaticLog>(cursor)?;
            Ok(())
        }
        2 => {
            get::<u64>(cursor)?;
            Ok(())
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_phoenix_v1_log(cursor: &mut &[u8]) -> ReadResult<()> {
    match get::<u32>(cursor)? {
        0 => {
            get::<
                blockzilla_format::program_logs::known_programs::phoenix_v1::PhoenixInstructionLog,
            >(cursor)?;
        }
        1 => {
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
        }
        2 => {
            skip_string(cursor)?;
            get::<u64>(cursor)?;
        }
        3 => {
            get::<blockzilla_format::program_logs::known_programs::phoenix_v1::PhoenixStaticLog>(
                cursor,
            )?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_log_event(
    cursor: &mut &[u8],
    registry_entries: Option<u32>,
    references: &mut LogReferences,
) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => {
            validate_system_program_log(
                get::<SystemProgramLog>(cursor)?,
                registry_entries,
                references,
            )?;
        }
        1 | 2 | 9..=13 | 38 | 39 | 43 => {}
        3 | 4 | 15 | 18 | 19 | 24..=27 | 40..=42 => {
            get_pubkey(cursor, registry_entries)?;
        }
        5 | 8 => skip_program_log(cursor, registry_entries, references)?,
        6 | 28 | 36 | 37 | 44 | 45 => {
            references.string(get::<u32>(cursor)?);
        }
        29 | 32 | 33 => {
            get::<u32>(cursor)?;
        }
        31 => references.data(get::<u32>(cursor)?),
        7 => {
            get_pubkey(cursor, registry_entries)?;
            skip_program_log(cursor, registry_entries, references)?;
        }
        14 => {
            get_pubkey(cursor, registry_entries)?;
            get::<u8>(cursor)?;
        }
        16 => {
            get_pubkey(cursor, registry_entries)?;
            get::<u32>(cursor)?;
            get::<u32>(cursor)?;
        }
        17 => {
            get::<u32>(cursor)?;
            get::<u32>(cursor)?;
        }
        20 | 21 => {
            get_pubkey(cursor, registry_entries)?;
            references.string(get::<u32>(cursor)?);
        }
        22 | 23 => {
            get_pubkey(cursor, registry_entries)?;
            get::<u32>(cursor)?;
        }
        30 => {
            get_pubkey(cursor, registry_entries)?;
            references.data(get::<u32>(cursor)?);
        }
        34 | 35 => {
            get_optional_pubkey(cursor, registry_entries)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

/// Stream past a `CompactLogStream` without materializing any outer or
/// nested vectors/strings. Every allocation-bearing known-program payload is
/// skipped from its bounded wire representation as well.
fn skip_logs(cursor: &mut &[u8], registry_entries: Option<u32>) -> ReadResult<bool> {
    match get::<u8>(cursor)? {
        0 => Ok(false),
        1 => {
            let mut references = LogReferences::default();
            let event_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log event count exceeds the canonical limit",
            )?;
            for _ in 0..event_count {
                skip_log_event(cursor, registry_entries, &mut references)?;
            }

            let string_length_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log string-length count exceeds the canonical limit",
            )?;
            let string_lengths = *cursor;
            let mut total_string_bytes = 0usize;
            for _ in 0..string_length_count {
                let length = usize::try_from(get::<u32>(cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                total_string_bytes = total_string_bytes.checked_add(length).ok_or_else(|| {
                    wincode::error::invalid_value("log string-table length overflow")
                })?;
            }
            let stored_string_bytes = read_bounded_len(
                cursor,
                cursor.len(),
                "log string-table bytes exceed remaining input",
            )?;
            if stored_string_bytes != total_string_bytes {
                return Err(wincode::error::invalid_value(
                    "log string-table lengths do not cover its byte lane exactly",
                ));
            }
            let string_bytes = cursor.take_borrowed(stored_string_bytes)?;
            let mut string_lengths = string_lengths;
            let mut string_offset = 0usize;
            for _ in 0..string_length_count {
                let length = usize::try_from(get::<u32>(&mut string_lengths)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                let end = string_offset + length;
                std::str::from_utf8(&string_bytes[string_offset..end]).map_err(|_| {
                    wincode::error::invalid_value("log string-table entry is not valid UTF-8")
                })?;
                string_offset = end;
            }

            let data_array_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log data-array count exceeds the canonical limit",
            )?;
            let mut total_chunks = 0usize;
            for _ in 0..data_array_count {
                let array = get::<DataArray>(cursor)?;
                let chunk_count = usize::try_from(array.chunk_count)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                total_chunks = total_chunks.checked_add(chunk_count).ok_or_else(|| {
                    wincode::error::invalid_value("log data-table chunk-count overflow")
                })?;
            }
            let chunk_length_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log chunk-length count exceeds the canonical limit",
            )?;
            if chunk_length_count != total_chunks {
                return Err(wincode::error::invalid_value(
                    "log data arrays do not cover the chunk-length lane exactly",
                ));
            }
            let mut total_data_bytes = 0usize;
            for _ in 0..chunk_length_count {
                let length = usize::try_from(get::<u32>(cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                total_data_bytes = total_data_bytes.checked_add(length).ok_or_else(|| {
                    wincode::error::invalid_value("log data-table byte length overflow")
                })?;
            }
            let stored_data_bytes = read_bounded_len(
                cursor,
                cursor.len(),
                "log data-table bytes exceed remaining input",
            )?;
            if stored_data_bytes != total_data_bytes {
                return Err(wincode::error::invalid_value(
                    "log chunk lengths do not cover the data byte lane exactly",
                ));
            }
            cursor.take_borrowed(stored_data_bytes)?;
            references.validate(string_length_count, data_array_count)?;
            Ok(true)
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_token_balances(
    cursor: &mut &[u8],
    maximum: usize,
    registry_entries: Option<u32>,
) -> ReadResult<usize> {
    let count = read_bounded_len(
        cursor,
        maximum,
        "token-balance count exceeds total message account count",
    )?;
    for _ in 0..count {
        let account_index = usize::try_from(get::<u32>(cursor)?)
            .map_err(|_| wincode::error::pointer_sized_decode_error())?;
        if account_index >= maximum {
            return Err(wincode::error::invalid_value(
                "token-balance account index is outside resolved message accounts",
            ));
        }
        get_optional_pubkey(cursor, registry_entries)?;
        get_optional_pubkey(cursor, registry_entries)?;
        get_optional_pubkey(cursor, registry_entries)?;
        get::<u64>(cursor)?;
        get::<u8>(cursor)?;
    }
    Ok(count)
}

fn skip_rewards(cursor: &mut &[u8], registry_entries: Option<u32>) -> ReadResult<()> {
    let count = read_len_bounded_by_remaining(cursor, "reward count exceeds remaining input")?;
    for _ in 0..count {
        get_pubkey(cursor, registry_entries)?;
        get::<i64>(cursor)?;
        get::<u64>(cursor)?;
        get::<i32>(cursor)?;
        get::<Option<u8>>(cursor)?;
    }
    Ok(())
}

fn read_loaded_addresses(
    cursor: &mut &[u8],
    maximum: usize,
    registry_entries: Option<u32>,
) -> ReadResult<Vec<CompactPubkey>> {
    let count = read_bounded_len(
        cursor,
        maximum,
        "loaded address count exceeds total message account count",
    )?;
    let mut addresses = Vec::with_capacity(count);
    for _ in 0..count {
        addresses.push(get_pubkey(cursor, registry_entries)?);
    }
    Ok(addresses)
}

/// Decode `CompactMetaV1`'s `err`/`fee`/`pre_balances`/`post_balances`
/// prefix (discarded — this indexer doesn't use them) followed by
/// `inner_instructions`, calling `on_inner_instruction` for each one as
/// it's decoded (no intermediate `Vec` is materialized, and no inner
/// instruction's `data` is ever allocated).
///
/// If `need_loaded_addresses` is false (legacy messages, which have no
/// address-table lookups to resolve), returns `None` and stops immediately
/// after `inner_instructions` — the entire metadata tail (logs, token
/// balances, rewards, return data, compute units) is never touched. If
/// true (V0 messages), streams past `logs`, `pre_token_balances`,
/// `post_token_balances`, and `rewards` without allocating their outer
/// vectors or byte tables, to reach and return
/// `Some((loaded_writable_addresses, loaded_readonly_addresses))`.
pub fn project_archive_v2_metadata_prefix<'de>(
    cursor: &mut &'de [u8],
    need_loaded_addresses: bool,
    limits: ArchiveV2MetadataProjectionLimits,
    on_inner_instruction: impl FnMut(BorrowedArchiveV2InnerInstruction<'de>),
) -> ReadResult<ProjectedArchiveV2MetadataPrefix> {
    project_archive_v2_metadata_prefix_impl(
        cursor,
        need_loaded_addresses,
        limits,
        None,
        on_inner_instruction,
    )
}

fn project_archive_v2_metadata_prefix_impl<'de>(
    cursor: &mut &'de [u8],
    need_loaded_addresses: bool,
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: Option<u32>,
    mut on_inner_instruction: impl FnMut(BorrowedArchiveV2InnerInstruction<'de>),
) -> ReadResult<ProjectedArchiveV2MetadataPrefix> {
    if limits.total_message_accounts > MAX_MESSAGE_ACCOUNTS {
        return Err(wincode::error::invalid_value(
            "total message account count exceeds message account cap",
        ));
    }
    let has_error = project_archive_v2_metadata_error_bounded(
        cursor,
        Some(limits.top_level_instruction_count),
        Some(limits.total_message_accounts),
    )?;
    get::<u64>(cursor)?; // fee
    let pre_balance_count = skip_balances(cursor, limits.total_message_accounts)?;
    let post_balance_count = skip_balances(cursor, limits.total_message_accounts)?;

    let inner_instructions_present = match get::<u8>(cursor)? {
        0 => false,
        1 => {
            let group_count = read_bounded_len(
                cursor,
                limits.top_level_instruction_count.min(cursor.len()),
                "inner-instruction group count exceeds top-level instruction count",
            )?;
            for _ in 0..group_count {
                let group_index = usize::try_from(get::<u32>(cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                if group_index >= limits.top_level_instruction_count {
                    return Err(wincode::error::invalid_value(
                        "inner-instruction group index is outside top-level instructions",
                    ));
                }
                let inner_count = read_len_bounded_by_remaining(
                    cursor,
                    "inner-instruction count exceeds remaining input",
                )?;
                for _ in 0..inner_count {
                    let instruction = read_inner_instruction(cursor)?;
                    let program_index = usize::try_from(instruction.program_id_index)
                        .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                    if program_index >= limits.total_message_accounts {
                        return Err(wincode::error::invalid_value(
                            "inner-instruction program index is outside message accounts",
                        ));
                    }
                    if instruction
                        .accounts
                        .iter()
                        .any(|index| usize::from(*index) >= limits.total_message_accounts)
                    {
                        return Err(wincode::error::invalid_value(
                            "inner-instruction account index is outside message accounts",
                        ));
                    }
                    on_inner_instruction(instruction);
                }
            }
            true
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    };

    if !need_loaded_addresses {
        return Ok(ProjectedArchiveV2MetadataPrefix {
            has_error,
            pre_balance_count,
            post_balance_count,
            inner_instructions_present,
            logs_present: None,
            token_balances_present: None,
            return_data_present: None,
            loaded_addresses: None,
        });
    }

    let logs_present = skip_logs(cursor, registry_entries)?;
    let pre_token_balance_count =
        skip_token_balances(cursor, limits.total_message_accounts, registry_entries)?;
    let post_token_balance_count =
        skip_token_balances(cursor, limits.total_message_accounts, registry_entries)?;
    skip_rewards(cursor, registry_entries)?;
    let loaded_writable_addresses =
        read_loaded_addresses(cursor, limits.total_message_accounts, registry_entries)?;
    let loaded_readonly_addresses =
        read_loaded_addresses(cursor, limits.total_message_accounts, registry_entries)?;
    if loaded_writable_addresses.len() + loaded_readonly_addresses.len()
        > limits.total_message_accounts
    {
        return Err(wincode::error::invalid_value(
            "loaded address count exceeds total message account count",
        ));
    }
    Ok(ProjectedArchiveV2MetadataPrefix {
        has_error,
        pre_balance_count,
        post_balance_count,
        inner_instructions_present,
        logs_present: Some(logs_present),
        token_balances_present: Some(pre_token_balance_count != 0 || post_token_balance_count != 0),
        return_data_present: None,
        loaded_addresses: Some((loaded_writable_addresses, loaded_readonly_addresses)),
    })
}

/// Validate one complete typed `CompactMetaV1` record with bounded borrowed
/// reads. This rejects trailing bytes and does not allocate logs, instruction
/// data, return data, token balances, or rewards.
pub fn validate_archive_v2_metadata_exact(
    bytes: &[u8],
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
) -> ReadResult<ProjectedArchiveV2MetadataPrefix> {
    if bytes.first() != Some(&0) {
        let (normalized, _schema) =
            canonicalize_archive_v2_metadata_owned(bytes).map_err(|_| {
                wincode::error::invalid_value(
                    "transaction metadata is neither one unambiguous current nor legacy record",
                )
            })?;
        return validate_current_archive_v2_metadata_exact(&normalized, limits, registry_entries);
    }
    validate_current_archive_v2_metadata_exact(bytes, limits, registry_entries)
}

fn validate_current_archive_v2_metadata_exact(
    bytes: &[u8],
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
) -> ReadResult<ProjectedArchiveV2MetadataPrefix> {
    let mut cursor = bytes;
    let mut projected = project_archive_v2_metadata_prefix_impl(
        &mut cursor,
        true,
        limits,
        Some(registry_entries),
        |_| {},
    )?;
    projected.return_data_present = Some(match get::<u8>(&mut cursor)? {
        0 => false,
        1 => {
            get_pubkey(&mut cursor, Some(registry_entries))?;
            skip_bytes(&mut cursor)?;
            true
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    });
    get::<Option<u64>>(&mut cursor)?;
    get::<Option<u64>>(&mut cursor)?;
    if !cursor.is_empty() {
        return Err(wincode::error::invalid_value(
            "transaction metadata has trailing bytes",
        ));
    }
    Ok(projected)
}

/// `CompactInnerInstruction` is unused directly (its fields are read
/// piecemeal by `read_inner_instruction`) but referenced here so `cargo`
/// flags it if the upstream type's shape ever changes in a way `rustc`
/// can statically catch.
#[allow(dead_code)]
fn _assert_inner_instruction_shape(value: CompactInnerInstruction) {
    let CompactInnerInstruction {
        program_id_index: _,
        accounts: _,
        data: _,
        stack_height: _,
    } = value;
}

#[cfg(test)]
mod tests {
    use blockzilla_format::{
        CompactInnerInstruction, CompactInnerInstructions, CompactLogStream, CompactMetaV1,
        CompactPubkey, CompactTokenBalance, DataTable, LogEvent, StringTable,
        wincode_leb128_config,
    };

    use super::*;

    fn metadata(inner: Option<Vec<CompactInnerInstructions>>) -> CompactMetaV1 {
        CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: vec![100],
            post_balances: vec![90],
            inner_instructions: inner,
            logs: None,
            pre_token_balances: vec![],
            post_token_balances: vec![],
            rewards: vec![],
            loaded_writable_addresses: vec![CompactPubkey::Id(50)],
            loaded_readonly_addresses: vec![CompactPubkey::Id(60)],
            return_data: None,
            compute_units_consumed: Some(100),
            cost_units: None,
        }
    }

    #[test]
    fn selective_projection_borrows_inner_accounts_and_reads_loaded_addresses() {
        let value = metadata(Some(vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 2,
                accounts: vec![0, 1],
                data: vec![7; 32],
                stack_height: Some(2),
            }],
        }]));
        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let mut cursor = bytes.as_slice();
        let mut inner = Vec::new();
        let projected = project_archive_v2_metadata_prefix(
            &mut cursor,
            true,
            ArchiveV2MetadataProjectionLimits {
                total_message_accounts: 3,
                top_level_instruction_count: 1,
            },
            |instruction| {
                inner.push((instruction.program_id_index, instruction.accounts.to_vec()));
            },
        )
        .unwrap();
        assert!(!projected.has_error);
        assert!(projected.inner_instructions_present);
        assert_eq!(inner, vec![(2, vec![0, 1])]);
        assert_eq!(
            projected.loaded_addresses,
            Some((vec![CompactPubkey::Id(50)], vec![CompactPubkey::Id(60)]))
        );
    }

    #[test]
    fn selective_projection_rejects_out_of_range_inner_group() {
        let value = metadata(Some(vec![CompactInnerInstructions {
            index: 1,
            instructions: vec![],
        }]));
        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        assert!(
            project_archive_v2_metadata_prefix(
                &mut bytes.as_slice(),
                false,
                ArchiveV2MetadataProjectionLimits {
                    total_message_accounts: 1,
                    top_level_instruction_count: 1,
                },
                |_| {},
            )
            .is_err()
        );
    }

    #[test]
    fn exact_metadata_validation_rejects_trailing_bytes() {
        let mut bytes =
            wincode::config::serialize(&metadata(None), wincode_leb128_config()).unwrap();
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };
        validate_archive_v2_metadata_exact(&bytes, limits, 100).unwrap();
        bytes.push(0);
        assert!(validate_archive_v2_metadata_exact(&bytes, limits, 100).is_err());
    }

    #[test]
    fn exact_metadata_validation_accepts_unambiguous_legacy_error_schema() {
        let current = wincode::config::serialize(&metadata(None), wincode_leb128_config()).unwrap();
        assert_eq!(current.first(), Some(&0));
        // Legacy err=Some(Vec<u8>) carrying StoredTransactionError::AccountInUse.
        let mut legacy = vec![1, 4, 0, 0, 0, 0];
        legacy.extend_from_slice(&current[1..]);
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };

        let projected = validate_archive_v2_metadata_exact(&legacy, limits, 100).unwrap();
        assert!(projected.has_error);
    }

    #[test]
    fn exact_metadata_validation_rejects_invalid_references() {
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 1,
        };

        let bytes = wincode::config::serialize(&metadata(None), wincode_leb128_config()).unwrap();
        assert!(validate_archive_v2_metadata_exact(&bytes, limits, 55).is_err());

        let inner = metadata(Some(vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 2,
                accounts: vec![3],
                data: vec![],
                stack_height: Some(2),
            }],
        }]));
        let bytes = wincode::config::serialize(&inner, wincode_leb128_config()).unwrap();
        assert!(validate_archive_v2_metadata_exact(&bytes, limits, 100).is_err());

        let mut token = metadata(None);
        token.pre_token_balances.push(CompactTokenBalance {
            account_index: 3,
            mint: None,
            owner: None,
            program_id: None,
            amount: 1,
            decimals: 0,
        });
        let bytes = wincode::config::serialize(&token, wincode_leb128_config()).unwrap();
        assert!(validate_archive_v2_metadata_exact(&bytes, limits, 100).is_err());
    }

    #[test]
    fn exact_metadata_validation_rejects_invalid_log_tables() {
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };
        let mut value = metadata(None);
        value.logs = Some(CompactLogStream {
            events: vec![LogEvent::Plain { text: 1 }],
            strings: StringTable::default(),
            data: DataTable::default(),
        });
        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        assert!(validate_archive_v2_metadata_exact(&bytes, limits, 100).is_err());

        value.logs = Some(CompactLogStream {
            events: vec![],
            strings: StringTable {
                lengths: vec![5],
                bytes: vec![],
            },
            data: DataTable::default(),
        });
        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        assert!(validate_archive_v2_metadata_exact(&bytes, limits, 100).is_err());
    }
}
