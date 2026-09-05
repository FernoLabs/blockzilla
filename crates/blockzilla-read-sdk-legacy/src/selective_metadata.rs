//! Selective, allocation-light projection of Archive V2 transaction metadata.
//!
//! The projector validates every field it crosses and borrows selected
//! inner-instruction fields. The prefix API skips raw instruction data and
//! token balances. The exact token API borrows raw inner data and streams token
//! balances. Both APIs skip logs and rewards without materializing them.

use blockzilla_format::{
    ArchiveV2WireMetadataErrorIndex, ArchiveV2WireMetadataErrorSchema, CompactInnerInstruction,
    CompactPubkey, CompactTokenBalance, DataArray, WincodeLeb128Config,
    canonicalize_archive_v2_metadata_owned,
    program_logs::{
        system_program::{PubkeyOrString, SystemAddress, SystemProgramLog},
        token_2022::Token2022Log,
    },
    validate_archive_v2_metadata_error_prefix_for_selected_schema,
};
use wincode::{ReadResult, SchemaRead, error::invalid_tag_encoding, io::Reader};

use crate::MAX_MESSAGE_ACCOUNTS;

type Cfg = WincodeLeb128Config;
const MAX_LOG_TABLE_ITEMS: usize = 1 << 20;

#[derive(Default)]
struct LogReferences {
    maximum_string_id: Option<u32>,
    maximum_data_id: Option<u32>,
    last_string_id: Option<u32>,
    last_data_id: Option<u32>,
    string_ids_decreased: bool,
    data_ids_decreased: bool,
}

impl LogReferences {
    fn string(&mut self, id: u32) {
        self.string_ids_decreased |= self.last_string_id.is_some_and(|last| id < last);
        self.last_string_id = Some(id);
        self.maximum_string_id = Some(self.maximum_string_id.unwrap_or_default().max(id));
    }

    fn data(&mut self, id: u32) {
        self.data_ids_decreased |= self.last_data_id.is_some_and(|last| id < last);
        self.last_data_id = Some(id);
        self.maximum_data_id = Some(self.maximum_data_id.unwrap_or_default().max(id));
    }

    fn ids_are_monotone(&self) -> bool {
        !self.string_ids_decreased && !self.data_ids_decreased
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
fn read_bytes<'de>(cursor: &mut &'de [u8]) -> ReadResult<&'de [u8]> {
    let len = read_len_bounded_by_remaining(cursor, "byte string length exceeds remaining input")?;
    Ok(cursor.take_borrowed(len)?)
}

#[inline]
fn skip_bytes(cursor: &mut &[u8]) -> ReadResult<()> {
    read_bytes(cursor)?;
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

/// One inner instruction for token discovery. Account indices and instruction
/// data borrow the metadata input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedArchiveV2InnerTokenInstruction<'de> {
    pub program_id_index: u32,
    pub accounts: &'de [u8],
    pub data: &'de [u8],
    pub stack_height: Option<u32>,
}

/// Identifies the token-balance vector that produced a streamed row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenBalanceSide {
    Pre,
    Post,
}

/// Identifies one of the two loaded-address vectors in transaction metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveV2LoadedAddressSide {
    Writable,
    Readonly,
}

/// One allocation-free token-balance row from the metadata input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedArchiveV2TokenBalance {
    pub account_index: u32,
    pub mint: Option<CompactPubkey>,
    pub owner: Option<CompactPubkey>,
    pub program_id: Option<CompactPubkey>,
    pub amount: u64,
    pub decimals: u8,
}

/// Result of one complete current-schema token metadata projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedArchiveV2TokenMetadata {
    pub has_error: bool,
    pub pre_balance_count: usize,
    pub post_balance_count: usize,
    pub inner_instructions_present: bool,
    pub pre_token_balance_count: usize,
    pub post_token_balance_count: usize,
    pub loaded_addresses: (Vec<CompactPubkey>, Vec<CompactPubkey>),
}

/// Allocation-free summary from one complete current-schema token metadata
/// projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectedArchiveV2TokenMetadataSummary {
    pub has_error: bool,
    pub pre_balance_count: usize,
    pub post_balance_count: usize,
    pub inner_instructions_present: bool,
    pub inner_instruction_count: usize,
    pub logs_present: bool,
    pub pre_token_balance_count: usize,
    pub post_token_balance_count: usize,
    pub loaded_writable_count: usize,
    pub loaded_readonly_count: usize,
    pub return_data_present: bool,
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

fn read_inner_token_instruction<'de>(
    cursor: &mut &'de [u8],
) -> ReadResult<BorrowedArchiveV2InnerTokenInstruction<'de>> {
    let program_id_index = get::<u32>(cursor)?;
    let accounts_len = read_len_bounded_by_remaining(
        cursor,
        "inner-instruction account-index count exceeds remaining input",
    )?;
    let accounts = cursor.take_borrowed(accounts_len)?;
    let data = read_bytes(cursor)?;
    let stack_height = get::<Option<u32>>(cursor)?;
    Ok(BorrowedArchiveV2InnerTokenInstruction {
        program_id_index,
        accounts,
        data,
        stack_height,
    })
}

fn read_inner_instruction<'de>(
    cursor: &mut &'de [u8],
) -> ReadResult<BorrowedArchiveV2InnerInstruction<'de>> {
    let instruction = read_inner_token_instruction(cursor)?;
    Ok(BorrowedArchiveV2InnerInstruction {
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts,
    })
}

#[derive(Clone, Copy)]
pub struct ArchiveV2MetadataProjectionLimits {
    pub total_message_accounts: usize,
    pub top_level_instruction_count: usize,
}

/// How much of the log region a projection validates while streaming past it.
///
/// Reaching the loaded-address lane always requires walking the log region, because the loaded
/// addresses are encoded after it and every count lane is LEB128, so no lane can be skipped by
/// pointer arithmetic. What is optional is how much of the log *content* is checked on the way.
///
/// Both modes perform identical cursor movement and identical bounds checking, so a projection
/// resolves the same loaded addresses and inner instructions either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogPayloadValidation {
    /// Additionally validate log payload content: every string-table entry must be valid UTF-8.
    ///
    /// Use this when the projection is auditing archive integrity.
    Full,
    /// Validate only what keeps the cursor honest: lane lengths must cover their byte lanes
    /// exactly, references must stay inside their tables, and every read stays in bounds. Log
    /// string bytes are stepped over without being decoded.
    ///
    /// Use this when the consumer never interprets log content — for example an extractor that
    /// copies metadata bytes verbatim and only needs the loaded-address lane.
    StructureOnly,
}

/// The exact borrowed wire for one structured nested log value.
///
/// `wire` includes the nested enum tag. `tag` is available separately so a
/// caller can classify the value without decoding an owned log type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedArchiveV2StructuredLog<'de> {
    pub tag: u32,
    pub wire: &'de [u8],
}

/// One borrowed `ProgramLog` value.
///
/// Structured program-specific payloads stay in their exact wire form. String
/// references are explicit IDs that can be resolved through
/// [`BorrowedArchiveV2LogTables::string`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BorrowedArchiveV2ProgramLog<'de> {
    Empty,
    Token(BorrowedArchiveV2StructuredLog<'de>),
    Token2022(BorrowedArchiveV2StructuredLog<'de>),
    Ata(BorrowedArchiveV2StructuredLog<'de>),
    AddressLookupTable(BorrowedArchiveV2StructuredLog<'de>),
    LoaderV3(BorrowedArchiveV2StructuredLog<'de>),
    LoaderV4(BorrowedArchiveV2StructuredLog<'de>),
    Memo(BorrowedArchiveV2StructuredLog<'de>),
    Record(BorrowedArchiveV2StructuredLog<'de>),
    TransferHook(BorrowedArchiveV2StructuredLog<'de>),
    AccountCompression(BorrowedArchiveV2StructuredLog<'de>),
    Stake(BorrowedArchiveV2StructuredLog<'de>),
    ZkElgamalProof(BorrowedArchiveV2StructuredLog<'de>),
    AnchorInstruction {
        name: u32,
    },
    AnchorErrorOccurred {
        code: u32,
        number: u32,
        message: u32,
    },
    AnchorErrorThrown {
        file: u32,
        line: u32,
        code: u32,
        number: u32,
        message: u32,
    },
    Unknown {
        text: u32,
    },
    Known(BorrowedArchiveV2StructuredLog<'de>),
}

/// The decoded outer shape of one compact log event.
///
/// Pubkeys, string IDs, data IDs, call depth, error codes, and compute-unit
/// values are available directly. The table IDs can be resolved without an
/// allocation through [`BorrowedArchiveV2LogTables`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BorrowedArchiveV2LogEventKind<'de> {
    System {
        log: BorrowedArchiveV2StructuredLog<'de>,
    },
    LogTruncated,
    StakeMergingAccounts,
    LoaderUpgradedProgram {
        program: CompactPubkey,
    },
    LoaderFinalizedAccount {
        account: CompactPubkey,
    },
    ProgramLog {
        log: BorrowedArchiveV2ProgramLog<'de>,
    },
    ProgramLogError {
        message: u32,
    },
    ProgramIdLog {
        program: CompactPubkey,
        log: BorrowedArchiveV2ProgramLog<'de>,
    },
    ProgramPlainLog {
        log: BorrowedArchiveV2ProgramLog<'de>,
    },
    ProgramAccountNotWritable,
    ProgramIdMismatch,
    ProgramNotUpgradeable,
    ProgramAndProgramDataAccountMismatch,
    ProgramWasExtendedInThisBlockAlready,
    Invoke {
        program: CompactPubkey,
        depth: u8,
    },
    BpfInvoke {
        program: CompactPubkey,
    },
    Consumed {
        program: CompactPubkey,
        used: u32,
        limit: u32,
    },
    BpfConsumed {
        used: u32,
        limit: u32,
    },
    Success {
        program: CompactPubkey,
    },
    BpfSuccess {
        program: CompactPubkey,
    },
    Failure {
        program: CompactPubkey,
        reason: u32,
    },
    BpfFailure {
        program: CompactPubkey,
        reason: u32,
    },
    FailureCustomProgramError {
        program: CompactPubkey,
        code: u32,
    },
    BpfFailureCustomProgramError {
        program: CompactPubkey,
        code: u32,
    },
    FailureInvalidAccountData {
        program: CompactPubkey,
    },
    BpfFailureInvalidAccountData {
        program: CompactPubkey,
    },
    FailureInvalidProgramArgument {
        program: CompactPubkey,
    },
    BpfFailureInvalidProgramArgument {
        program: CompactPubkey,
    },
    FailedToComplete {
        reason: u32,
    },
    CustomProgramError {
        code: u32,
    },
    Return {
        program: CompactPubkey,
        data: u32,
    },
    Data {
        data: u32,
    },
    Consumption {
        units: u32,
    },
    CbRequestUnits {
        units: u32,
    },
    ProgramNotDeployed {
        program: Option<CompactPubkey>,
    },
    ProgramNotCached {
        program: Option<CompactPubkey>,
    },
    UnknownProgram {
        program: u32,
    },
    UnknownAccount {
        account: u32,
    },
    VerifyEd25519,
    VerifySecp256k1,
    RuntimeWritablePrivilegeEscalated {
        account: CompactPubkey,
    },
    RuntimeSignerPrivilegeEscalated {
        account: CompactPubkey,
    },
    RuntimeAccountOwnerBalanceVerificationFailed {
        account: CompactPubkey,
    },
    CloseContextState,
    Plain {
        text: u32,
    },
    Unparsed {
        text: u32,
    },
}

/// One ordered compact log event borrowed from the metadata input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedArchiveV2LogEvent<'de> {
    pub ordinal: usize,
    pub tag: u32,
    pub wire: &'de [u8],
    pub kind: BorrowedArchiveV2LogEventKind<'de>,
}

/// A forward-only borrowed resolver for compact log string and data tables.
///
/// References must be resolved in nondecreasing ID order. Repeated IDs use a
/// cached borrowed result. Each encoded table lane is scanned at most once.
#[derive(Debug)]
pub struct BorrowedArchiveV2LogTables<'de> {
    string_lengths: &'de [u8],
    string_bytes: &'de [u8],
    string_count: usize,
    next_string_id: usize,
    last_string: Option<(u32, &'de str)>,
    data_arrays: &'de [u8],
    data_count: usize,
    data_chunk_lengths: &'de [u8],
    data_bytes: &'de [u8],
    next_data_id: usize,
    last_data: Option<(u32, BorrowedArchiveV2LogDataChunks<'de>)>,
}

impl<'de> BorrowedArchiveV2LogTables<'de> {
    #[inline]
    pub const fn string_count(&self) -> usize {
        self.string_count
    }

    #[inline]
    pub const fn data_count(&self) -> usize {
        self.data_count
    }

    /// Resolve one nondecreasing string table ID without allocating.
    pub fn string(&mut self, id: u32) -> ReadResult<&'de str> {
        let requested =
            usize::try_from(id).map_err(|_| wincode::error::pointer_sized_decode_error())?;
        if requested >= self.string_count {
            return Err(wincode::error::invalid_value(
                "log string reference is outside the string table",
            ));
        }
        if let Some((last_id, value)) = self.last_string {
            if id < last_id {
                return Err(wincode::error::invalid_value(
                    "log string references are not in nondecreasing order",
                ));
            }
            if id == last_id {
                return Ok(value);
            }
        }

        while self.next_string_id <= requested {
            let current_id = self.next_string_id;
            let length = usize::try_from(get::<u32>(&mut self.string_lengths)?)
                .map_err(|_| wincode::error::pointer_sized_decode_error())?;
            let value = self.string_bytes.get(..length).ok_or_else(|| {
                wincode::error::invalid_value("log string-table entry exceeds its byte lane")
            })?;
            self.string_bytes = self.string_bytes.get(length..).ok_or_else(|| {
                wincode::error::invalid_value("log string-table entry exceeds its byte lane")
            })?;
            self.next_string_id += 1;
            if current_id == requested {
                let value = std::str::from_utf8(value).map_err(|_| {
                    wincode::error::invalid_value("log string-table entry is not valid UTF-8")
                })?;
                self.last_string = Some((id, value));
                return Ok(value);
            }
        }
        Err(wincode::error::invalid_value(
            "log string resolver lost table position",
        ))
    }

    /// Resolve one nondecreasing data table ID as a borrowed chunk iterator.
    pub fn data_chunks(&mut self, id: u32) -> ReadResult<BorrowedArchiveV2LogDataChunks<'de>> {
        let requested =
            usize::try_from(id).map_err(|_| wincode::error::pointer_sized_decode_error())?;
        if requested >= self.data_count {
            return Err(wincode::error::invalid_value(
                "log data reference is outside the data table",
            ));
        }
        if let Some((last_id, ref value)) = self.last_data {
            if id < last_id {
                return Err(wincode::error::invalid_value(
                    "log data references are not in nondecreasing order",
                ));
            }
            if id == last_id {
                return Ok(value.clone());
            }
        }

        while self.next_data_id <= requested {
            let current_id = self.next_data_id;
            let array = get::<DataArray>(&mut self.data_arrays)?;
            let count = usize::try_from(array.chunk_count)
                .map_err(|_| wincode::error::pointer_sized_decode_error())?;
            let lengths_start = self.data_chunk_lengths;
            let mut total_bytes = 0usize;
            for _ in 0..count {
                let length = usize::try_from(get::<u32>(&mut self.data_chunk_lengths)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                total_bytes = total_bytes.checked_add(length).ok_or_else(|| {
                    wincode::error::invalid_value("log data-table byte length overflow")
                })?;
            }
            let lengths = consumed_wire(lengths_start, self.data_chunk_lengths);
            let bytes = self.data_bytes.get(..total_bytes).ok_or_else(|| {
                wincode::error::invalid_value("log data chunk exceeds its byte lane")
            })?;
            self.data_bytes = self.data_bytes.get(total_bytes..).ok_or_else(|| {
                wincode::error::invalid_value("log data chunk exceeds its byte lane")
            })?;
            self.next_data_id += 1;
            if current_id == requested {
                let value = BorrowedArchiveV2LogDataChunks {
                    lengths,
                    bytes,
                    remaining: count,
                };
                self.last_data = Some((id, value.clone()));
                return Ok(value);
            }
        }
        Err(wincode::error::invalid_value(
            "log data resolver lost table position",
        ))
    }
}

/// A borrowed iterator over the chunks in one compact log data entry.
#[derive(Debug, Clone)]
pub struct BorrowedArchiveV2LogDataChunks<'de> {
    lengths: &'de [u8],
    bytes: &'de [u8],
    remaining: usize,
}

impl<'de> Iterator for BorrowedArchiveV2LogDataChunks<'de> {
    type Item = &'de [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        // Instances can only be made from a fully validated table layout.
        let length = usize::try_from(get::<u32>(&mut self.lengths).ok()?).ok()?;
        let chunk = self.bytes.get(..length)?;
        self.bytes = self.bytes.get(length..)?;
        self.remaining -= 1;
        Some(chunk)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for BorrowedArchiveV2LogDataChunks<'_> {}

/// Allocation-free facts from one exact compact log visit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectedArchiveV2CompactLogsSummary {
    /// Facts from the complete metadata validation that preceded the log visit.
    pub metadata: ProjectedArchiveV2TokenMetadataSummary,
    pub has_error: bool,
    pub logs_present: bool,
    pub event_count: usize,
    pub string_count: usize,
    pub data_count: usize,
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

#[inline]
fn consumed_wire<'de>(start: &'de [u8], remaining: &[u8]) -> &'de [u8] {
    &start[..start.len() - remaining.len()]
}

fn read_borrowed_program_log<'de>(
    cursor: &mut &'de [u8],
    registry_entries: Option<u32>,
) -> ReadResult<BorrowedArchiveV2ProgramLog<'de>> {
    let start = *cursor;
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => Ok(BorrowedArchiveV2ProgramLog::Empty),
        13 => Ok(BorrowedArchiveV2ProgramLog::AnchorInstruction {
            name: get::<u32>(cursor)?,
        }),
        14 => Ok(BorrowedArchiveV2ProgramLog::AnchorErrorOccurred {
            code: get::<u32>(cursor)?,
            number: get::<u32>(cursor)?,
            message: get::<u32>(cursor)?,
        }),
        15 => Ok(BorrowedArchiveV2ProgramLog::AnchorErrorThrown {
            file: get::<u32>(cursor)?,
            line: get::<u32>(cursor)?,
            code: get::<u32>(cursor)?,
            number: get::<u32>(cursor)?,
            message: get::<u32>(cursor)?,
        }),
        16 => Ok(BorrowedArchiveV2ProgramLog::Unknown {
            text: get::<u32>(cursor)?,
        }),
        1..=12 | 17 => {
            let mut end = start;
            let mut references = LogReferences::default();
            skip_program_log(&mut end, registry_entries, &mut references)?;
            *cursor = end;
            let structured = BorrowedArchiveV2StructuredLog {
                tag,
                wire: consumed_wire(start, end),
            };
            Ok(match tag {
                1 => BorrowedArchiveV2ProgramLog::Token(structured),
                2 => BorrowedArchiveV2ProgramLog::Token2022(structured),
                3 => BorrowedArchiveV2ProgramLog::Ata(structured),
                4 => BorrowedArchiveV2ProgramLog::AddressLookupTable(structured),
                5 => BorrowedArchiveV2ProgramLog::LoaderV3(structured),
                6 => BorrowedArchiveV2ProgramLog::LoaderV4(structured),
                7 => BorrowedArchiveV2ProgramLog::Memo(structured),
                8 => BorrowedArchiveV2ProgramLog::Record(structured),
                9 => BorrowedArchiveV2ProgramLog::TransferHook(structured),
                10 => BorrowedArchiveV2ProgramLog::AccountCompression(structured),
                11 => BorrowedArchiveV2ProgramLog::Stake(structured),
                12 => BorrowedArchiveV2ProgramLog::ZkElgamalProof(structured),
                17 => BorrowedArchiveV2ProgramLog::Known(structured),
                _ => unreachable!(),
            })
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn read_borrowed_log_event<'de>(
    cursor: &mut &'de [u8],
    ordinal: usize,
    registry_entries: Option<u32>,
) -> ReadResult<BorrowedArchiveV2LogEvent<'de>> {
    let start = *cursor;
    let tag = get::<u32>(cursor)?;
    let kind = match tag {
        0 => {
            let nested_start = *cursor;
            let mut tag_cursor = nested_start;
            let nested_tag = get::<u32>(&mut tag_cursor)?;
            let mut references = LogReferences::default();
            validate_system_program_log(
                get::<SystemProgramLog>(cursor)?,
                registry_entries,
                &mut references,
            )?;
            BorrowedArchiveV2LogEventKind::System {
                log: BorrowedArchiveV2StructuredLog {
                    tag: nested_tag,
                    wire: consumed_wire(nested_start, cursor),
                },
            }
        }
        1 => BorrowedArchiveV2LogEventKind::LogTruncated,
        2 => BorrowedArchiveV2LogEventKind::StakeMergingAccounts,
        3 => BorrowedArchiveV2LogEventKind::LoaderUpgradedProgram {
            program: get_pubkey(cursor, registry_entries)?,
        },
        4 => BorrowedArchiveV2LogEventKind::LoaderFinalizedAccount {
            account: get_pubkey(cursor, registry_entries)?,
        },
        5 => BorrowedArchiveV2LogEventKind::ProgramLog {
            log: read_borrowed_program_log(cursor, registry_entries)?,
        },
        6 => BorrowedArchiveV2LogEventKind::ProgramLogError {
            message: get::<u32>(cursor)?,
        },
        7 => BorrowedArchiveV2LogEventKind::ProgramIdLog {
            program: get_pubkey(cursor, registry_entries)?,
            log: read_borrowed_program_log(cursor, registry_entries)?,
        },
        8 => BorrowedArchiveV2LogEventKind::ProgramPlainLog {
            log: read_borrowed_program_log(cursor, registry_entries)?,
        },
        9 => BorrowedArchiveV2LogEventKind::ProgramAccountNotWritable,
        10 => BorrowedArchiveV2LogEventKind::ProgramIdMismatch,
        11 => BorrowedArchiveV2LogEventKind::ProgramNotUpgradeable,
        12 => BorrowedArchiveV2LogEventKind::ProgramAndProgramDataAccountMismatch,
        13 => BorrowedArchiveV2LogEventKind::ProgramWasExtendedInThisBlockAlready,
        14 => BorrowedArchiveV2LogEventKind::Invoke {
            program: get_pubkey(cursor, registry_entries)?,
            depth: get::<u8>(cursor)?,
        },
        15 => BorrowedArchiveV2LogEventKind::BpfInvoke {
            program: get_pubkey(cursor, registry_entries)?,
        },
        16 => BorrowedArchiveV2LogEventKind::Consumed {
            program: get_pubkey(cursor, registry_entries)?,
            used: get::<u32>(cursor)?,
            limit: get::<u32>(cursor)?,
        },
        17 => BorrowedArchiveV2LogEventKind::BpfConsumed {
            used: get::<u32>(cursor)?,
            limit: get::<u32>(cursor)?,
        },
        18 => BorrowedArchiveV2LogEventKind::Success {
            program: get_pubkey(cursor, registry_entries)?,
        },
        19 => BorrowedArchiveV2LogEventKind::BpfSuccess {
            program: get_pubkey(cursor, registry_entries)?,
        },
        20 => BorrowedArchiveV2LogEventKind::Failure {
            program: get_pubkey(cursor, registry_entries)?,
            reason: get::<u32>(cursor)?,
        },
        21 => BorrowedArchiveV2LogEventKind::BpfFailure {
            program: get_pubkey(cursor, registry_entries)?,
            reason: get::<u32>(cursor)?,
        },
        22 => BorrowedArchiveV2LogEventKind::FailureCustomProgramError {
            program: get_pubkey(cursor, registry_entries)?,
            code: get::<u32>(cursor)?,
        },
        23 => BorrowedArchiveV2LogEventKind::BpfFailureCustomProgramError {
            program: get_pubkey(cursor, registry_entries)?,
            code: get::<u32>(cursor)?,
        },
        24 => BorrowedArchiveV2LogEventKind::FailureInvalidAccountData {
            program: get_pubkey(cursor, registry_entries)?,
        },
        25 => BorrowedArchiveV2LogEventKind::BpfFailureInvalidAccountData {
            program: get_pubkey(cursor, registry_entries)?,
        },
        26 => BorrowedArchiveV2LogEventKind::FailureInvalidProgramArgument {
            program: get_pubkey(cursor, registry_entries)?,
        },
        27 => BorrowedArchiveV2LogEventKind::BpfFailureInvalidProgramArgument {
            program: get_pubkey(cursor, registry_entries)?,
        },
        28 => BorrowedArchiveV2LogEventKind::FailedToComplete {
            reason: get::<u32>(cursor)?,
        },
        29 => BorrowedArchiveV2LogEventKind::CustomProgramError {
            code: get::<u32>(cursor)?,
        },
        30 => BorrowedArchiveV2LogEventKind::Return {
            program: get_pubkey(cursor, registry_entries)?,
            data: get::<u32>(cursor)?,
        },
        31 => BorrowedArchiveV2LogEventKind::Data {
            data: get::<u32>(cursor)?,
        },
        32 => BorrowedArchiveV2LogEventKind::Consumption {
            units: get::<u32>(cursor)?,
        },
        33 => BorrowedArchiveV2LogEventKind::CbRequestUnits {
            units: get::<u32>(cursor)?,
        },
        34 => BorrowedArchiveV2LogEventKind::ProgramNotDeployed {
            program: get_optional_pubkey(cursor, registry_entries)?,
        },
        35 => BorrowedArchiveV2LogEventKind::ProgramNotCached {
            program: get_optional_pubkey(cursor, registry_entries)?,
        },
        36 => BorrowedArchiveV2LogEventKind::UnknownProgram {
            program: get::<u32>(cursor)?,
        },
        37 => BorrowedArchiveV2LogEventKind::UnknownAccount {
            account: get::<u32>(cursor)?,
        },
        38 => BorrowedArchiveV2LogEventKind::VerifyEd25519,
        39 => BorrowedArchiveV2LogEventKind::VerifySecp256k1,
        40 => BorrowedArchiveV2LogEventKind::RuntimeWritablePrivilegeEscalated {
            account: get_pubkey(cursor, registry_entries)?,
        },
        41 => BorrowedArchiveV2LogEventKind::RuntimeSignerPrivilegeEscalated {
            account: get_pubkey(cursor, registry_entries)?,
        },
        42 => BorrowedArchiveV2LogEventKind::RuntimeAccountOwnerBalanceVerificationFailed {
            account: get_pubkey(cursor, registry_entries)?,
        },
        43 => BorrowedArchiveV2LogEventKind::CloseContextState,
        44 => BorrowedArchiveV2LogEventKind::Plain {
            text: get::<u32>(cursor)?,
        },
        45 => BorrowedArchiveV2LogEventKind::Unparsed {
            text: get::<u32>(cursor)?,
        },
        other => return Err(invalid_tag_encoding(other as usize)),
    };
    Ok(BorrowedArchiveV2LogEvent {
        ordinal,
        tag,
        wire: consumed_wire(start, cursor),
        kind,
    })
}

/// Stream past a `CompactLogStream` without materializing any outer or
/// nested vectors/strings. Every allocation-bearing known-program payload is
/// skipped from its bounded wire representation as well.
///
/// `validation` selects whether log string bytes are additionally decoded as UTF-8. It does not
/// affect cursor movement: the string byte lane is consumed in one bounded read either way, and
/// the lane-covers-its-lengths check runs in both modes.
struct BorrowedArchiveV2LogLayout<'de> {
    events: &'de [u8],
    event_count: usize,
    references_are_monotone: bool,
    tables: BorrowedArchiveV2LogTables<'de>,
}

fn read_log_layout<'de>(
    cursor: &mut &'de [u8],
    registry_entries: Option<u32>,
    validation: LogPayloadValidation,
) -> ReadResult<Option<BorrowedArchiveV2LogLayout<'de>>> {
    match get::<u8>(cursor)? {
        0 => Ok(None),
        1 => {
            let mut references = LogReferences::default();
            let event_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log event count exceeds the canonical limit",
            )?;
            let event_wire_start = *cursor;
            for _ in 0..event_count {
                skip_log_event(cursor, registry_entries, &mut references)?;
            }
            let events = &event_wire_start[..event_wire_start.len() - cursor.len()];

            let string_length_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log string-length count exceeds the canonical limit",
            )?;
            let string_lengths_start = *cursor;
            let mut total_string_bytes = 0usize;
            for _ in 0..string_length_count {
                let length = usize::try_from(get::<u32>(cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                total_string_bytes = total_string_bytes.checked_add(length).ok_or_else(|| {
                    wincode::error::invalid_value("log string-table length overflow")
                })?;
            }
            let string_lengths = &string_lengths_start[..string_lengths_start.len() - cursor.len()];
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
            // The byte lane is already consumed above and `stored_string_bytes` was checked to
            // equal the summed lengths, so the cursor is correct without inspecting the bytes.
            // Decoding them is a pure content check that only `Full` pays for.
            if validation == LogPayloadValidation::Full {
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
            }

            let data_array_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log data-array count exceeds the canonical limit",
            )?;
            let data_arrays_start = *cursor;
            let mut total_chunks = 0usize;
            for _ in 0..data_array_count {
                let array = get::<DataArray>(cursor)?;
                let chunk_count = usize::try_from(array.chunk_count)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                total_chunks = total_chunks.checked_add(chunk_count).ok_or_else(|| {
                    wincode::error::invalid_value("log data-table chunk-count overflow")
                })?;
            }
            let data_arrays = &data_arrays_start[..data_arrays_start.len() - cursor.len()];
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
            let data_chunk_lengths_start = *cursor;
            let mut total_data_bytes = 0usize;
            for _ in 0..chunk_length_count {
                let length = usize::try_from(get::<u32>(cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                total_data_bytes = total_data_bytes.checked_add(length).ok_or_else(|| {
                    wincode::error::invalid_value("log data-table byte length overflow")
                })?;
            }
            let data_chunk_lengths =
                &data_chunk_lengths_start[..data_chunk_lengths_start.len() - cursor.len()];
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
            let data_bytes = cursor.take_borrowed(stored_data_bytes)?;
            references.validate(string_length_count, data_array_count)?;
            Ok(Some(BorrowedArchiveV2LogLayout {
                events,
                event_count,
                references_are_monotone: references.ids_are_monotone(),
                tables: BorrowedArchiveV2LogTables {
                    string_lengths,
                    string_bytes,
                    string_count: string_length_count,
                    next_string_id: 0,
                    last_string: None,
                    data_arrays,
                    data_count: data_array_count,
                    data_chunk_lengths,
                    data_bytes,
                    next_data_id: 0,
                    last_data: None,
                },
            }))
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_logs(
    cursor: &mut &[u8],
    registry_entries: Option<u32>,
    validation: LogPayloadValidation,
) -> ReadResult<bool> {
    Ok(read_log_layout(cursor, registry_entries, validation)?.is_some())
}

fn visit_token_balances(
    cursor: &mut &[u8],
    maximum: usize,
    registry_entries: Option<u32>,
    mut on_balance: impl FnMut(BorrowedArchiveV2TokenBalance),
) -> ReadResult<usize> {
    let count = read_bounded_len(
        cursor,
        maximum,
        "token-balance count exceeds total message account count",
    )?;
    for _ in 0..count {
        let account_index = get::<u32>(cursor)?;
        let account_index_usize = usize::try_from(account_index)
            .map_err(|_| wincode::error::pointer_sized_decode_error())?;
        if account_index_usize >= maximum {
            return Err(wincode::error::invalid_value(
                "token-balance account index is outside resolved message accounts",
            ));
        }
        let balance = BorrowedArchiveV2TokenBalance {
            account_index,
            mint: get_optional_pubkey(cursor, registry_entries)?,
            owner: get_optional_pubkey(cursor, registry_entries)?,
            program_id: get_optional_pubkey(cursor, registry_entries)?,
            amount: get::<u64>(cursor)?,
            decimals: get::<u8>(cursor)?,
        };
        on_balance(balance);
    }
    Ok(count)
}

fn skip_token_balances(
    cursor: &mut &[u8],
    maximum: usize,
    registry_entries: Option<u32>,
) -> ReadResult<usize> {
    visit_token_balances(cursor, maximum, registry_entries, |_| {})
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
    let mut addresses = Vec::new();
    visit_loaded_addresses(cursor, maximum, registry_entries, |_, address| {
        addresses.push(address);
    })?;
    Ok(addresses)
}

fn visit_loaded_addresses(
    cursor: &mut &[u8],
    maximum: usize,
    registry_entries: Option<u32>,
    mut on_address: impl FnMut(usize, CompactPubkey),
) -> ReadResult<usize> {
    let count = read_bounded_len(
        cursor,
        maximum,
        "loaded address count exceeds total message account count",
    )?;
    for ordinal in 0..count {
        on_address(ordinal, get_pubkey(cursor, registry_entries)?);
    }
    Ok(count)
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

    let logs_present = skip_logs(cursor, registry_entries, LogPayloadValidation::Full)?;
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

/// Project one complete current-schema `CompactMetaV1` record for token
/// discovery. Inner instruction account indices and data borrow `bytes`.
/// Pre- and post-token-balance rows are validated and sent to `on_balance`
/// without an intermediate vector. Logs, rewards, and return data are fully
/// validated and skipped without allocation. Only the returned loaded-address
/// vectors allocate.
///
/// This function does not normalize the legacy metadata error schema. Callback
/// calls can occur before a later field proves malformed, so a caller must not
/// publish callback side effects unless this function returns `Ok`.
pub fn project_archive_v2_token_metadata_exact<'de>(
    bytes: &'de [u8],
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    mut on_inner: impl FnMut(BorrowedArchiveV2InnerTokenInstruction<'de>),
    on_balance: impl FnMut(TokenBalanceSide, BorrowedArchiveV2TokenBalance),
) -> ReadResult<ProjectedArchiveV2TokenMetadata> {
    project_archive_v2_token_metadata_exact_ordered(
        bytes,
        limits,
        registry_entries,
        |_, instruction| on_inner(instruction),
        on_balance,
    )
}

/// The execution-order form of [`project_archive_v2_token_metadata_exact`].
/// The first inner callback argument is the top-level instruction index whose
/// CPI execution produced the borrowed inner instruction.
pub fn project_archive_v2_token_metadata_exact_ordered<'de>(
    bytes: &'de [u8],
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    on_inner: impl FnMut(u32, BorrowedArchiveV2InnerTokenInstruction<'de>),
    on_balance: impl FnMut(TokenBalanceSide, BorrowedArchiveV2TokenBalance),
) -> ReadResult<ProjectedArchiveV2TokenMetadata> {
    let mut loaded_writable_addresses = Vec::new();
    let mut loaded_readonly_addresses = Vec::new();
    let summary = visit_archive_v2_token_metadata_exact_ordered(
        bytes,
        limits,
        registry_entries,
        LogPayloadValidation::Full,
        on_inner,
        on_balance,
        |side, ordinal, address| match side {
            ArchiveV2LoadedAddressSide::Writable => {
                debug_assert_eq!(ordinal, loaded_writable_addresses.len());
                loaded_writable_addresses.push(address);
            }
            ArchiveV2LoadedAddressSide::Readonly => {
                debug_assert_eq!(ordinal, loaded_readonly_addresses.len());
                loaded_readonly_addresses.push(address);
            }
        },
    )?;
    Ok(ProjectedArchiveV2TokenMetadata {
        has_error: summary.has_error,
        pre_balance_count: summary.pre_balance_count,
        post_balance_count: summary.post_balance_count,
        inner_instructions_present: summary.inner_instructions_present,
        pre_token_balance_count: summary.pre_token_balance_count,
        post_token_balance_count: summary.post_token_balance_count,
        loaded_addresses: (loaded_writable_addresses, loaded_readonly_addresses),
    })
}

/// Project and validate one complete current-schema `CompactMetaV1` record.
/// Inner instructions, token balances, and loaded addresses are sent to the
/// callbacks without an intermediate vector. A loaded-address ordinal is local
/// to the writable or readonly side.
///
/// Callback calls can occur before a later field proves malformed. A caller
/// must not publish callback side effects unless this function returns `Ok`.
pub fn visit_archive_v2_token_metadata_exact_ordered<'de>(
    bytes: &'de [u8],
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    log_payload_validation: LogPayloadValidation,
    mut on_inner: impl FnMut(u32, BorrowedArchiveV2InnerTokenInstruction<'de>),
    mut on_balance: impl FnMut(TokenBalanceSide, BorrowedArchiveV2TokenBalance),
    mut on_loaded_address: impl FnMut(ArchiveV2LoadedAddressSide, usize, CompactPubkey),
) -> ReadResult<ProjectedArchiveV2TokenMetadataSummary> {
    visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
        bytes,
        ArchiveV2WireMetadataErrorSchema::Current,
        limits,
        registry_entries,
        log_payload_validation,
        &mut on_inner,
        &mut on_balance,
        &mut on_loaded_address,
    )
}

/// Project and validate one complete `CompactMetaV1` record under an
/// explicitly selected current or legacy transaction-error schema.
///
/// The option and error prefix is validated without allocation. The common
/// metadata tail is then read by the same borrowed exact visitor as current
/// metadata. Schema authority belongs to generation admission or to a prior
/// complete value-level ambiguity decision; this function never probes the
/// alternate error schema.
///
/// Callback calls can occur before a later field proves malformed. A caller
/// must not publish callback side effects unless this function returns `Ok`.
#[allow(clippy::too_many_arguments)]
pub fn visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema<'de>(
    bytes: &'de [u8],
    error_schema: ArchiveV2WireMetadataErrorSchema,
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    log_payload_validation: LogPayloadValidation,
    on_inner: impl FnMut(u32, BorrowedArchiveV2InnerTokenInstruction<'de>),
    on_balance: impl FnMut(TokenBalanceSide, BorrowedArchiveV2TokenBalance),
    on_loaded_address: impl FnMut(ArchiveV2LoadedAddressSide, usize, CompactPubkey),
) -> ReadResult<ProjectedArchiveV2TokenMetadataSummary> {
    if limits.total_message_accounts > MAX_MESSAGE_ACCOUNTS {
        return Err(wincode::error::invalid_value(
            "total message account count exceeds message account cap",
        ));
    }

    let selected = validate_archive_v2_metadata_error_prefix_for_selected_schema(
        bytes,
        error_schema,
        bytes.len(),
    )
    .map_err(|_| {
        wincode::error::invalid_value(
            "transaction metadata has an invalid selected-schema error prefix",
        )
    })?;
    match selected.error_index {
        Some(ArchiveV2WireMetadataErrorIndex::TopLevelInstruction(index))
            if usize::from(index) >= limits.top_level_instruction_count =>
        {
            return Err(wincode::error::invalid_value(
                "transaction-error instruction index is outside top-level instructions",
            ));
        }
        Some(ArchiveV2WireMetadataErrorIndex::MessageAccount(index))
            if usize::from(index) >= limits.total_message_accounts =>
        {
            return Err(wincode::error::invalid_value(
                "transaction-error account index is outside resolved message accounts",
            ));
        }
        _ => {}
    }
    visit_archive_v2_token_metadata_tail_exact_ordered(
        selected.bytes,
        selected.has_error,
        limits,
        registry_entries,
        log_payload_validation,
        on_inner,
        on_balance,
        on_loaded_address,
    )
}

/// Validate one complete current-schema metadata record, then visit its compact
/// log events in wire order without materializing a vector or string.
///
/// No callback runs until the complete metadata record, all log table bounds,
/// and the nondecreasing table-reference order have been validated. The table
/// resolver scans each string/data lane once and accepts repeated IDs.
pub fn visit_archive_v2_compact_logs_exact<'de>(
    bytes: &'de [u8],
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    on_event: impl FnMut(
        BorrowedArchiveV2LogEvent<'de>,
        &mut BorrowedArchiveV2LogTables<'de>,
    ) -> ReadResult<()>,
) -> ReadResult<ProjectedArchiveV2CompactLogsSummary> {
    visit_archive_v2_compact_logs_exact_with_selected_error_schema(
        bytes,
        ArchiveV2WireMetadataErrorSchema::Current,
        limits,
        registry_entries,
        on_event,
    )
}

/// The selected current/legacy error-schema form of
/// [`visit_archive_v2_compact_logs_exact`].
pub fn visit_archive_v2_compact_logs_exact_with_selected_error_schema<'de>(
    bytes: &'de [u8],
    error_schema: ArchiveV2WireMetadataErrorSchema,
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    mut on_event: impl FnMut(
        BorrowedArchiveV2LogEvent<'de>,
        &mut BorrowedArchiveV2LogTables<'de>,
    ) -> ReadResult<()>,
) -> ReadResult<ProjectedArchiveV2CompactLogsSummary> {
    let validated = visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
        bytes,
        error_schema,
        limits,
        registry_entries,
        LogPayloadValidation::Full,
        |_, _| {},
        |_, _| {},
        |_, _, _| {},
    )?;

    let selected = validate_archive_v2_metadata_error_prefix_for_selected_schema(
        bytes,
        error_schema,
        bytes.len(),
    )
    .map_err(|_| {
        wincode::error::invalid_value(
            "transaction metadata has an invalid selected-schema error prefix",
        )
    })?;
    let Some(layout) = locate_archive_v2_log_layout(
        selected.bytes,
        limits,
        registry_entries,
        LogPayloadValidation::Full,
    )?
    else {
        return Ok(ProjectedArchiveV2CompactLogsSummary {
            metadata: validated,
            has_error: validated.has_error,
            logs_present: false,
            event_count: 0,
            string_count: 0,
            data_count: 0,
        });
    };

    if !layout.references_are_monotone {
        return Err(wincode::error::invalid_value(
            "log table references are not in nondecreasing event order",
        ));
    }

    let event_count = layout.event_count;
    let string_count = layout.tables.string_count();
    let data_count = layout.tables.data_count();
    let mut event_cursor = layout.events;
    let mut tables = layout.tables;
    for ordinal in 0..event_count {
        let event = read_borrowed_log_event(&mut event_cursor, ordinal, Some(registry_entries))?;
        on_event(event, &mut tables)?;
    }
    if !event_cursor.is_empty() {
        return Err(wincode::error::invalid_value(
            "log event lane has trailing bytes",
        ));
    }

    Ok(ProjectedArchiveV2CompactLogsSummary {
        metadata: validated,
        has_error: validated.has_error,
        logs_present: true,
        event_count,
        string_count,
        data_count,
    })
}

fn locate_archive_v2_log_layout<'de>(
    mut cursor: &'de [u8],
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    validation: LogPayloadValidation,
) -> ReadResult<Option<BorrowedArchiveV2LogLayout<'de>>> {
    get::<u64>(&mut cursor)?;
    skip_balances(&mut cursor, limits.total_message_accounts)?;
    skip_balances(&mut cursor, limits.total_message_accounts)?;

    match get::<u8>(&mut cursor)? {
        0 => {}
        1 => {
            let maximum_group_count = limits.top_level_instruction_count.min(cursor.len());
            let group_count = read_bounded_len(
                &mut cursor,
                maximum_group_count,
                "inner-instruction group count exceeds top-level instruction count",
            )?;
            let mut total_inner_count = 0usize;
            for _ in 0..group_count {
                let group_index = usize::try_from(get::<u32>(&mut cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                if group_index >= limits.top_level_instruction_count {
                    return Err(wincode::error::invalid_value(
                        "inner-instruction group index is outside top-level instructions",
                    ));
                }
                let inner_count = read_len_bounded_by_remaining(
                    &mut cursor,
                    "inner-instruction count exceeds remaining input",
                )?;
                total_inner_count =
                    total_inner_count.checked_add(inner_count).ok_or_else(|| {
                        wincode::error::invalid_value("inner-instruction count overflow")
                    })?;
                for _ in 0..inner_count {
                    let instruction = read_inner_token_instruction(&mut cursor)?;
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
                }
            }
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }

    read_log_layout(&mut cursor, Some(registry_entries), validation)
}

#[allow(clippy::too_many_arguments)]
fn visit_archive_v2_token_metadata_tail_exact_ordered<'de>(
    mut cursor: &'de [u8],
    has_error: bool,
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    log_payload_validation: LogPayloadValidation,
    mut on_inner: impl FnMut(u32, BorrowedArchiveV2InnerTokenInstruction<'de>),
    mut on_balance: impl FnMut(TokenBalanceSide, BorrowedArchiveV2TokenBalance),
    mut on_loaded_address: impl FnMut(ArchiveV2LoadedAddressSide, usize, CompactPubkey),
) -> ReadResult<ProjectedArchiveV2TokenMetadataSummary> {
    get::<u64>(&mut cursor)?; // fee
    let pre_balance_count = skip_balances(&mut cursor, limits.total_message_accounts)?;
    let post_balance_count = skip_balances(&mut cursor, limits.total_message_accounts)?;

    let mut inner_instruction_count = 0usize;
    let inner_instructions_present = match get::<u8>(&mut cursor)? {
        0 => false,
        1 => {
            let maximum_group_count = limits.top_level_instruction_count.min(cursor.len());
            let group_count = read_bounded_len(
                &mut cursor,
                maximum_group_count,
                "inner-instruction group count exceeds top-level instruction count",
            )?;
            for _ in 0..group_count {
                let group_index = usize::try_from(get::<u32>(&mut cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                if group_index >= limits.top_level_instruction_count {
                    return Err(wincode::error::invalid_value(
                        "inner-instruction group index is outside top-level instructions",
                    ));
                }
                let inner_count = read_len_bounded_by_remaining(
                    &mut cursor,
                    "inner-instruction count exceeds remaining input",
                )?;
                inner_instruction_count = inner_instruction_count
                    .checked_add(inner_count)
                    .ok_or_else(|| {
                        wincode::error::invalid_value("inner-instruction count overflow")
                    })?;
                for _ in 0..inner_count {
                    let instruction = read_inner_token_instruction(&mut cursor)?;
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
                    on_inner(
                        u32::try_from(group_index)
                            .map_err(|_| wincode::error::pointer_sized_decode_error())?,
                        instruction,
                    );
                }
            }
            true
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    };

    let logs_present = skip_logs(&mut cursor, Some(registry_entries), log_payload_validation)?;
    let pre_token_balance_count = visit_token_balances(
        &mut cursor,
        limits.total_message_accounts,
        Some(registry_entries),
        |balance| on_balance(TokenBalanceSide::Pre, balance),
    )?;
    let post_token_balance_count = visit_token_balances(
        &mut cursor,
        limits.total_message_accounts,
        Some(registry_entries),
        |balance| on_balance(TokenBalanceSide::Post, balance),
    )?;
    skip_rewards(&mut cursor, Some(registry_entries))?;

    let loaded_writable_count = visit_loaded_addresses(
        &mut cursor,
        limits.total_message_accounts,
        Some(registry_entries),
        |ordinal, address| {
            on_loaded_address(ArchiveV2LoadedAddressSide::Writable, ordinal, address);
        },
    )?;
    let loaded_readonly_count = visit_loaded_addresses(
        &mut cursor,
        limits.total_message_accounts,
        Some(registry_entries),
        |ordinal, address| {
            on_loaded_address(ArchiveV2LoadedAddressSide::Readonly, ordinal, address);
        },
    )?;
    if loaded_writable_count + loaded_readonly_count > limits.total_message_accounts {
        return Err(wincode::error::invalid_value(
            "loaded address count exceeds total message account count",
        ));
    }

    let return_data_present = match get::<u8>(&mut cursor)? {
        0 => false,
        1 => {
            get_pubkey(&mut cursor, Some(registry_entries))?;
            skip_bytes(&mut cursor)?;
            true
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    };
    get::<Option<u64>>(&mut cursor)?;
    get::<Option<u64>>(&mut cursor)?;
    if !cursor.is_empty() {
        return Err(wincode::error::invalid_value(
            "transaction metadata has trailing bytes",
        ));
    }

    Ok(ProjectedArchiveV2TokenMetadataSummary {
        has_error,
        pre_balance_count,
        post_balance_count,
        inner_instructions_present,
        inner_instruction_count,
        logs_present,
        pre_token_balance_count,
        post_token_balance_count,
        loaded_writable_count,
        loaded_readonly_count,
        return_data_present,
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
        return validate_archive_v2_current_metadata_exact(&normalized, limits, registry_entries);
    }
    validate_archive_v2_current_metadata_exact(bytes, limits, registry_entries)
}

/// Validate one complete current typed-error metadata record. This function
/// never probes or accepts the historical raw-error grammar.
pub fn validate_archive_v2_current_metadata_exact(
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

/// `CompactTokenBalance` is read field by field by `visit_token_balances`.
/// Keep this destructure so an upstream field addition requires review here.
#[allow(dead_code)]
fn _assert_token_balance_shape(value: CompactTokenBalance) {
    let CompactTokenBalance {
        account_index: _,
        mint: _,
        owner: _,
        program_id: _,
        amount: _,
        decimals: _,
    } = value;
}

#[cfg(test)]
mod tests {
    use blockzilla_format::{
        CompactInnerInstruction, CompactInnerInstructions, CompactLogStream, CompactMetaV1,
        CompactPubkey, CompactReturnData, CompactReward, CompactTokenBalance,
        CompactTransactionError, DataArray, DataTable, LogEvent, StringTable,
        program_logs::ProgramLog, wincode_leb128_config,
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
    fn exact_token_projection_borrows_inner_data_and_streams_both_balance_sides() {
        let mut value = metadata(Some(vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 2,
                accounts: vec![0, 3],
                data: vec![7; 32],
                stack_height: Some(2),
            }],
        }]));
        value.logs = Some(CompactLogStream {
            events: vec![],
            strings: StringTable::default(),
            data: DataTable::default(),
        });
        value.pre_token_balances.push(CompactTokenBalance {
            account_index: 1,
            mint: Some(CompactPubkey::Id(10)),
            owner: Some(CompactPubkey::Raw([11; 32])),
            program_id: Some(CompactPubkey::Id(12)),
            amount: 1_000,
            decimals: 6,
        });
        value.post_token_balances.push(CompactTokenBalance {
            account_index: 3,
            mint: Some(CompactPubkey::Id(10)),
            owner: None,
            program_id: Some(CompactPubkey::Id(12)),
            amount: 2_000,
            decimals: 6,
        });
        value.rewards.push(CompactReward {
            pubkey: CompactPubkey::Id(70),
            lamports: 1,
            post_balance: 2,
            reward_type: 0,
            commission: None,
        });
        value.return_data = Some(CompactReturnData {
            program_id: CompactPubkey::Id(80),
            data: vec![9; 64],
        });

        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let mut inner = Vec::new();
        let mut balances = Vec::new();
        let projected = project_archive_v2_token_metadata_exact(
            &bytes,
            ArchiveV2MetadataProjectionLimits {
                total_message_accounts: 4,
                top_level_instruction_count: 1,
            },
            100,
            |instruction| inner.push(instruction),
            |side, balance| balances.push((side, balance)),
        )
        .unwrap();

        assert!(!projected.has_error);
        assert!(projected.inner_instructions_present);
        assert_eq!(projected.pre_token_balance_count, 1);
        assert_eq!(projected.post_token_balance_count, 1);
        assert_eq!(
            projected.loaded_addresses,
            (vec![CompactPubkey::Id(50)], vec![CompactPubkey::Id(60)])
        );
        assert_eq!(inner.len(), 1);
        assert_eq!(inner[0].program_id_index, 2);
        assert_eq!(inner[0].accounts, [0, 3]);
        assert_eq!(inner[0].data, [7; 32]);
        assert_eq!(inner[0].stack_height, Some(2));
        assert_eq!(balances.len(), 2);
        assert_eq!(balances[0].0, TokenBalanceSide::Pre);
        assert_eq!(balances[0].1.account_index, 1);
        assert_eq!(balances[0].1.mint, Some(CompactPubkey::Id(10)));
        assert_eq!(balances[0].1.owner, Some(CompactPubkey::Raw([11; 32])));
        assert_eq!(balances[0].1.amount, 1_000);
        assert_eq!(balances[1].0, TokenBalanceSide::Post);
        assert_eq!(balances[1].1.account_index, 3);
        assert_eq!(balances[1].1.amount, 2_000);

        let input_start = bytes.as_ptr() as usize;
        let input_end = input_start + bytes.len();
        for borrowed in [inner[0].accounts, inner[0].data] {
            let borrowed_start = borrowed.as_ptr() as usize;
            assert!(borrowed_start >= input_start);
            assert!(borrowed_start + borrowed.len() <= input_end);
        }
    }

    #[test]
    fn exact_streaming_projection_visits_loaded_addresses_and_reports_tail_facts() {
        let mut value = metadata(None);
        value.loaded_writable_addresses = vec![CompactPubkey::Id(50), CompactPubkey::Raw([51; 32])];
        value.loaded_readonly_addresses = vec![CompactPubkey::Id(60)];
        value.return_data = Some(CompactReturnData {
            program_id: CompactPubkey::Id(80),
            data: vec![9; 4],
        });
        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let mut loaded = Vec::new();

        let summary = visit_archive_v2_token_metadata_exact_ordered(
            &bytes,
            ArchiveV2MetadataProjectionLimits {
                total_message_accounts: 4,
                top_level_instruction_count: 0,
            },
            100,
            LogPayloadValidation::Full,
            |_, _| {},
            |_, _| {},
            |side, ordinal, address| loaded.push((side, ordinal, address)),
        )
        .unwrap();

        assert_eq!(summary.loaded_writable_count, 2);
        assert_eq!(summary.loaded_readonly_count, 1);
        assert!(!summary.logs_present);
        assert!(summary.return_data_present);
        assert_eq!(
            loaded,
            vec![
                (
                    ArchiveV2LoadedAddressSide::Writable,
                    0,
                    CompactPubkey::Id(50),
                ),
                (
                    ArchiveV2LoadedAddressSide::Writable,
                    1,
                    CompactPubkey::Raw([51; 32]),
                ),
                (
                    ArchiveV2LoadedAddressSide::Readonly,
                    0,
                    CompactPubkey::Id(60),
                ),
            ]
        );
    }

    #[test]
    fn exact_streaming_projection_does_not_allocate_loaded_vectors() {
        let bytes = wincode::config::serialize(&metadata(None), wincode_leb128_config()).unwrap();
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };

        let ((summary, visited), allocations) =
            crate::test_allocations::count_current_thread_allocations(|| {
                let mut visited = 0usize;
                let summary = visit_archive_v2_token_metadata_exact_ordered(
                    &bytes,
                    limits,
                    100,
                    LogPayloadValidation::Full,
                    |_, _| {},
                    |_, _| {},
                    |_, _, _| visited += 1,
                )
                .unwrap();
                (summary, visited)
            });

        assert_eq!(visited, 2);
        assert_eq!(summary.loaded_writable_count, 1);
        assert_eq!(summary.loaded_readonly_count, 1);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn exact_streaming_projection_validates_loaded_refs_and_the_full_tail() {
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };
        let mut invalid_reference = metadata(None);
        invalid_reference.loaded_writable_addresses[0] = CompactPubkey::Id(101);
        let bytes =
            wincode::config::serialize(&invalid_reference, wincode_leb128_config()).unwrap();
        assert!(
            visit_archive_v2_token_metadata_exact_ordered(
                &bytes,
                limits,
                100,
                LogPayloadValidation::Full,
                |_, _| {},
                |_, _| {},
                |_, _, _| {},
            )
            .is_err()
        );

        let mut bytes =
            wincode::config::serialize(&metadata(None), wincode_leb128_config()).unwrap();
        bytes.push(0);
        assert!(
            visit_archive_v2_token_metadata_exact_ordered(
                &bytes,
                limits,
                100,
                LogPayloadValidation::Full,
                |_, _| {},
                |_, _| {},
                |_, _, _| {},
            )
            .is_err()
        );
    }

    #[test]
    fn exact_token_projection_reports_current_errors() {
        let mut value = metadata(None);
        value.err = Some(CompactTransactionError::AccountInUse);
        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let projected = project_archive_v2_token_metadata_exact(
            &bytes,
            ArchiveV2MetadataProjectionLimits {
                total_message_accounts: 3,
                top_level_instruction_count: 0,
            },
            100,
            |_| {},
            |_, _| {},
        )
        .unwrap();
        assert!(projected.has_error);
    }

    #[test]
    fn selected_error_schema_streams_current_and_legacy_without_allocations() {
        let inner = || {
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 2,
                    accounts: vec![0, 1],
                    data: vec![7, 8, 9],
                    stack_height: Some(2),
                }],
            }])
        };
        let mut current_value = metadata(inner());
        current_value.err = Some(CompactTransactionError::InstructionError(
            0,
            blockzilla_format::CompactInstructionError::GenericError,
        ));
        let current = wincode::config::serialize(&current_value, wincode_leb128_config()).unwrap();

        let successful =
            wincode::config::serialize(&metadata(inner()), wincode_leb128_config()).unwrap();
        // StoredTransactionError::InstructionError(0, GenericError): both enum
        // tags are fixed-width little-endian u32 in the historical payload.
        let stored: Vec<u8> = vec![8, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut legacy =
            wincode::config::serialize(&Some(stored), wincode_leb128_config()).unwrap();
        legacy.extend_from_slice(&successful[1..]);

        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 1,
        };
        for (bytes, schema) in [
            (
                current.as_slice(),
                ArchiveV2WireMetadataErrorSchema::Current,
            ),
            (legacy.as_slice(), ArchiveV2WireMetadataErrorSchema::Legacy),
        ] {
            validate_archive_v2_metadata_error_prefix_for_selected_schema(
                bytes,
                schema,
                bytes.len(),
            )
            .unwrap_or_else(|error| panic!("{schema:?} prefix failed: {error}"));
            let ((summary, inner_count, loaded_count), allocations) =
                crate::test_allocations::count_current_thread_allocations(|| {
                    let mut inner_count = 0usize;
                    let mut loaded_count = 0usize;
                    let summary =
                        visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
                            bytes,
                            schema,
                            limits,
                            100,
                            LogPayloadValidation::Full,
                            |outer_index, instruction| {
                                assert_eq!(outer_index, 0);
                                assert_eq!(instruction.program_id_index, 2);
                                assert_eq!(instruction.accounts, [0, 1]);
                                assert_eq!(instruction.data, [7, 8, 9]);
                                assert_eq!(instruction.stack_height, Some(2));
                                inner_count += 1;
                            },
                            |_, _| {},
                            |_, _, _| loaded_count += 1,
                        )
                        .unwrap();
                    (summary, inner_count, loaded_count)
                });
            assert!(summary.has_error);
            assert_eq!(summary.inner_instruction_count, 1);
            assert_eq!(inner_count, 1);
            assert_eq!(loaded_count, 2);
            assert_eq!(allocations, 0);
        }
    }

    #[test]
    fn selected_error_schema_exact_visitor_rejects_trailing_bytes() {
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };
        let mut current_value = metadata(None);
        current_value.err = Some(CompactTransactionError::AccountInUse);
        let mut current =
            wincode::config::serialize(&current_value, wincode_leb128_config()).unwrap();
        current.push(0);

        let successful =
            wincode::config::serialize(&metadata(None), wincode_leb128_config()).unwrap();
        // StoredTransactionError::AccountInUse.
        let stored: Vec<u8> = vec![0, 0, 0, 0];
        let mut legacy =
            wincode::config::serialize(&Some(stored), wincode_leb128_config()).unwrap();
        legacy.extend_from_slice(&successful[1..]);
        legacy.push(0);

        for (bytes, schema) in [
            (
                current.as_slice(),
                ArchiveV2WireMetadataErrorSchema::Current,
            ),
            (legacy.as_slice(), ArchiveV2WireMetadataErrorSchema::Legacy),
        ] {
            assert!(
                visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
                    bytes,
                    schema,
                    limits,
                    100,
                    LogPayloadValidation::Full,
                    |_, _| {},
                    |_, _| {},
                    |_, _, _| {},
                )
                .is_err()
            );
        }
    }

    #[test]
    fn exact_token_projection_reports_the_outer_instruction_group() {
        let value = metadata(Some(vec![CompactInnerInstructions {
            index: 1,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 2,
                accounts: vec![0],
                data: vec![3],
                stack_height: Some(2),
            }],
        }]));
        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let mut inner = Vec::new();
        project_archive_v2_token_metadata_exact_ordered(
            &bytes,
            ArchiveV2MetadataProjectionLimits {
                total_message_accounts: 3,
                top_level_instruction_count: 2,
            },
            100,
            |outer_index, instruction| inner.push((outer_index, instruction)),
            |_, _| {},
        )
        .unwrap();

        assert_eq!(inner.len(), 1);
        assert_eq!(inner[0].0, 1);
        assert_eq!(inner[0].1.program_id_index, 2);
    }

    #[test]
    fn exact_token_projection_rejects_bad_registry_references_and_trailing_bytes() {
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };
        let mut invalid_reference = metadata(None);
        invalid_reference
            .pre_token_balances
            .push(CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(101)),
                owner: None,
                program_id: None,
                amount: 1,
                decimals: 0,
            });
        let bytes =
            wincode::config::serialize(&invalid_reference, wincode_leb128_config()).unwrap();
        assert!(
            project_archive_v2_token_metadata_exact(&bytes, limits, 100, |_| {}, |_, _| {},)
                .is_err()
        );

        let mut bytes =
            wincode::config::serialize(&metadata(None), wincode_leb128_config()).unwrap();
        bytes.push(0);
        assert!(
            project_archive_v2_token_metadata_exact(&bytes, limits, 100, |_| {}, |_, _| {},)
                .is_err()
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

    #[test]
    fn compact_log_visitor_exposes_ordered_event_fields_and_borrowed_tables() {
        let program = CompactPubkey::Id(1);
        let mut strings = StringTable::default();
        for value in [
            "anchor_ix",
            "E_ONE",
            "occurred",
            "source.rs",
            "E_TWO",
            "thrown",
            "unknown",
            "failed",
            "incomplete",
            "plain",
            "unparsed",
        ] {
            strings.push(value);
        }
        let mut value = metadata(None);
        value.logs = Some(CompactLogStream {
            events: vec![
                LogEvent::LogTruncated,
                LogEvent::Invoke { program, depth: 3 },
                LogEvent::BpfInvoke { program },
                LogEvent::ProgramLog(ProgramLog::AnchorInstruction { name: 0 }),
                LogEvent::ProgramIdLog {
                    program,
                    log: ProgramLog::AnchorErrorOccurred {
                        code: 1,
                        number: 6_000,
                        msg: 2,
                    },
                },
                LogEvent::ProgramPlainLog(ProgramLog::AnchorErrorThrown {
                    file: 3,
                    line: 7,
                    code: 4,
                    number: 6_001,
                    msg: 5,
                }),
                LogEvent::ProgramLog(ProgramLog::Unknown(6)),
                LogEvent::Consumed {
                    program,
                    used: 5,
                    limit: 10,
                },
                LogEvent::BpfConsumed { used: 6, limit: 11 },
                LogEvent::Success { program },
                LogEvent::BpfSuccess { program },
                LogEvent::Failure { program, reason: 7 },
                LogEvent::BpfFailure { program, reason: 7 },
                LogEvent::FailureCustomProgramError { program, code: 42 },
                LogEvent::BpfFailureCustomProgramError { program, code: 43 },
                LogEvent::FailureInvalidAccountData { program },
                LogEvent::BpfFailureInvalidAccountData { program },
                LogEvent::FailureInvalidProgramArgument { program },
                LogEvent::BpfFailureInvalidProgramArgument { program },
                LogEvent::FailedToComplete { reason: 8 },
                LogEvent::CustomProgramError { code: 44 },
                LogEvent::Return { program, data: 0 },
                LogEvent::Data { data: 1 },
                LogEvent::Consumption { units: 12 },
                LogEvent::CbRequestUnits { units: 13 },
                LogEvent::Plain { text: 9 },
                LogEvent::Unparsed { text: 10 },
            ],
            strings,
            data: DataTable {
                arrays: vec![DataArray { chunk_count: 2 }, DataArray { chunk_count: 1 }],
                chunk_lengths: vec![2, 1, 3],
                bytes: vec![1, 2, 3, 4, 5, 6],
            },
        });
        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let mut visited = 0usize;

        let summary = visit_archive_v2_compact_logs_exact(
            &bytes,
            ArchiveV2MetadataProjectionLimits {
                total_message_accounts: 3,
                top_level_instruction_count: 0,
            },
            100,
            |event, tables| {
                assert_eq!(event.ordinal, visited);
                assert!(!event.wire.is_empty());
                match (event.ordinal, event.kind) {
                    (0, BorrowedArchiveV2LogEventKind::LogTruncated) => {}
                    (1, BorrowedArchiveV2LogEventKind::Invoke { program: p, depth }) => {
                        assert_eq!((p, depth), (program, 3));
                    }
                    (2, BorrowedArchiveV2LogEventKind::BpfInvoke { program: p }) => {
                        assert_eq!(p, program);
                    }
                    (
                        3,
                        BorrowedArchiveV2LogEventKind::ProgramLog {
                            log: BorrowedArchiveV2ProgramLog::AnchorInstruction { name },
                        },
                    ) => assert_eq!(tables.string(name)?, "anchor_ix"),
                    (
                        4,
                        BorrowedArchiveV2LogEventKind::ProgramIdLog {
                            program: p,
                            log:
                                BorrowedArchiveV2ProgramLog::AnchorErrorOccurred {
                                    code,
                                    number,
                                    message,
                                },
                        },
                    ) => {
                        assert_eq!(p, program);
                        assert_eq!(tables.string(code)?, "E_ONE");
                        assert_eq!(number, 6_000);
                        assert_eq!(tables.string(message)?, "occurred");
                    }
                    (
                        5,
                        BorrowedArchiveV2LogEventKind::ProgramPlainLog {
                            log:
                                BorrowedArchiveV2ProgramLog::AnchorErrorThrown {
                                    file,
                                    line,
                                    code,
                                    number,
                                    message,
                                },
                        },
                    ) => {
                        assert_eq!(tables.string(file)?, "source.rs");
                        assert_eq!(line, 7);
                        assert_eq!(tables.string(code)?, "E_TWO");
                        assert_eq!(number, 6_001);
                        assert_eq!(tables.string(message)?, "thrown");
                    }
                    (
                        6,
                        BorrowedArchiveV2LogEventKind::ProgramLog {
                            log: BorrowedArchiveV2ProgramLog::Unknown { text },
                        },
                    ) => assert_eq!(tables.string(text)?, "unknown"),
                    (
                        7,
                        BorrowedArchiveV2LogEventKind::Consumed {
                            program: p,
                            used,
                            limit,
                        },
                    ) => assert_eq!((p, used, limit), (program, 5, 10)),
                    (8, BorrowedArchiveV2LogEventKind::BpfConsumed { used, limit }) => {
                        assert_eq!((used, limit), (6, 11));
                    }
                    (9, BorrowedArchiveV2LogEventKind::Success { program: p })
                    | (10, BorrowedArchiveV2LogEventKind::BpfSuccess { program: p }) => {
                        assert_eq!(p, program);
                    }
                    (11, BorrowedArchiveV2LogEventKind::Failure { program: p, reason })
                    | (12, BorrowedArchiveV2LogEventKind::BpfFailure { program: p, reason }) => {
                        assert_eq!(p, program);
                        assert_eq!(tables.string(reason)?, "failed");
                    }
                    (
                        13,
                        BorrowedArchiveV2LogEventKind::FailureCustomProgramError {
                            program: p,
                            code,
                        },
                    ) => assert_eq!((p, code), (program, 42)),
                    (
                        14,
                        BorrowedArchiveV2LogEventKind::BpfFailureCustomProgramError {
                            program: p,
                            code,
                        },
                    ) => assert_eq!((p, code), (program, 43)),
                    (
                        15,
                        BorrowedArchiveV2LogEventKind::FailureInvalidAccountData { program: p },
                    )
                    | (
                        16,
                        BorrowedArchiveV2LogEventKind::BpfFailureInvalidAccountData { program: p },
                    )
                    | (
                        17,
                        BorrowedArchiveV2LogEventKind::FailureInvalidProgramArgument { program: p },
                    )
                    | (
                        18,
                        BorrowedArchiveV2LogEventKind::BpfFailureInvalidProgramArgument {
                            program: p,
                        },
                    ) => assert_eq!(p, program),
                    (19, BorrowedArchiveV2LogEventKind::FailedToComplete { reason }) => {
                        assert_eq!(tables.string(reason)?, "incomplete");
                    }
                    (20, BorrowedArchiveV2LogEventKind::CustomProgramError { code }) => {
                        assert_eq!(code, 44);
                    }
                    (21, BorrowedArchiveV2LogEventKind::Return { program: p, data }) => {
                        assert_eq!(p, program);
                        assert_eq!(
                            tables.data_chunks(data)?.collect::<Vec<_>>(),
                            vec![&[1, 2][..], &[3][..]]
                        );
                    }
                    (22, BorrowedArchiveV2LogEventKind::Data { data }) => {
                        assert_eq!(
                            tables.data_chunks(data)?.collect::<Vec<_>>(),
                            vec![&[4, 5, 6][..]]
                        );
                    }
                    (23, BorrowedArchiveV2LogEventKind::Consumption { units }) => {
                        assert_eq!(units, 12);
                    }
                    (24, BorrowedArchiveV2LogEventKind::CbRequestUnits { units }) => {
                        assert_eq!(units, 13);
                    }
                    (25, BorrowedArchiveV2LogEventKind::Plain { text }) => {
                        assert_eq!(tables.string(text)?, "plain");
                    }
                    (26, BorrowedArchiveV2LogEventKind::Unparsed { text }) => {
                        assert_eq!(tables.string(text)?, "unparsed");
                    }
                    other => panic!("unexpected compact log event: {other:?}"),
                }
                visited += 1;
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(visited, 27);
        assert_eq!(summary.event_count, 27);
        assert_eq!(summary.string_count, 11);
        assert_eq!(summary.data_count, 2);
        assert!(summary.logs_present);
    }

    #[test]
    fn compact_log_visitor_is_allocation_free_and_caches_repeated_ids() {
        let mut strings = StringTable::default();
        strings.push("repeat");
        let mut value = metadata(None);
        value.logs = Some(CompactLogStream {
            events: vec![
                LogEvent::Plain { text: 0 },
                LogEvent::Unparsed { text: 0 },
                LogEvent::Data { data: 0 },
                LogEvent::Data { data: 0 },
            ],
            strings,
            data: DataTable {
                arrays: vec![DataArray { chunk_count: 1 }],
                chunk_lengths: vec![3],
                bytes: vec![7, 8, 9],
            },
        });
        let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };

        let ((summary, text_bytes, data_bytes), allocations) =
            crate::test_allocations::count_current_thread_allocations(|| {
                let mut text_bytes = 0usize;
                let mut data_bytes = 0usize;
                let summary =
                    visit_archive_v2_compact_logs_exact(&bytes, limits, 100, |event, tables| {
                        match event.kind {
                            BorrowedArchiveV2LogEventKind::Plain { text }
                            | BorrowedArchiveV2LogEventKind::Unparsed { text } => {
                                text_bytes += tables.string(text)?.len();
                            }
                            BorrowedArchiveV2LogEventKind::Data { data } => {
                                for chunk in tables.data_chunks(data)? {
                                    data_bytes += chunk.len();
                                }
                            }
                            _ => unreachable!(),
                        }
                        Ok(())
                    })
                    .unwrap();
                (summary, text_bytes, data_bytes)
            });

        assert_eq!(summary.event_count, 4);
        assert_eq!(text_bytes, 12);
        assert_eq!(data_bytes, 6);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn compact_log_visitor_rejects_invalid_ids_before_callbacks() {
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };
        let cases = [
            CompactLogStream {
                events: vec![LogEvent::Plain { text: 1 }],
                strings: StringTable::default(),
                data: DataTable::default(),
            },
            CompactLogStream {
                events: vec![LogEvent::Data { data: 1 }],
                strings: StringTable::default(),
                data: DataTable::default(),
            },
        ];

        for logs in cases {
            let mut value = metadata(None);
            value.logs = Some(logs);
            let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
            let mut callbacks = 0usize;
            assert!(
                visit_archive_v2_compact_logs_exact(&bytes, limits, 100, |_, _| {
                    callbacks += 1;
                    Ok(())
                })
                .is_err()
            );
            assert_eq!(callbacks, 0);
        }
    }

    #[test]
    fn compact_log_visitor_rejects_nonmonotone_ids_before_callbacks() {
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };
        let mut strings = StringTable::default();
        strings.push("zero");
        strings.push("one");
        let cases = [
            CompactLogStream {
                events: vec![LogEvent::Plain { text: 1 }, LogEvent::Plain { text: 0 }],
                strings,
                data: DataTable::default(),
            },
            CompactLogStream {
                events: vec![LogEvent::Data { data: 1 }, LogEvent::Data { data: 0 }],
                strings: StringTable::default(),
                data: DataTable {
                    arrays: vec![DataArray { chunk_count: 0 }, DataArray { chunk_count: 0 }],
                    chunk_lengths: vec![],
                    bytes: vec![],
                },
            },
        ];

        for logs in cases {
            let mut value = metadata(None);
            value.logs = Some(logs);
            let bytes = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
            let mut callbacks = 0usize;
            assert!(
                visit_archive_v2_compact_logs_exact(&bytes, limits, 100, |_, _| {
                    callbacks += 1;
                    Ok(())
                })
                .is_err()
            );
            assert_eq!(callbacks, 0);
        }
    }

    #[test]
    fn compact_log_visitor_supports_current_and_legacy_error_schemas() {
        let mut successful = metadata(None);
        let mut strings = StringTable::default();
        strings.push("schema");
        successful.logs = Some(CompactLogStream {
            events: vec![LogEvent::Plain { text: 0 }],
            strings,
            data: DataTable::default(),
        });
        let successful_bytes =
            wincode::config::serialize(&successful, wincode_leb128_config()).unwrap();

        let mut current_value = successful;
        current_value.err = Some(CompactTransactionError::AccountInUse);
        let current = wincode::config::serialize(&current_value, wincode_leb128_config()).unwrap();

        let stored: Vec<u8> = vec![0, 0, 0, 0];
        let mut legacy =
            wincode::config::serialize(&Some(stored), wincode_leb128_config()).unwrap();
        legacy.extend_from_slice(&successful_bytes[1..]);

        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 3,
            top_level_instruction_count: 0,
        };
        for (bytes, schema) in [
            (
                current.as_slice(),
                ArchiveV2WireMetadataErrorSchema::Current,
            ),
            (legacy.as_slice(), ArchiveV2WireMetadataErrorSchema::Legacy),
        ] {
            let mut callbacks = 0usize;
            let summary = visit_archive_v2_compact_logs_exact_with_selected_error_schema(
                bytes,
                schema,
                limits,
                100,
                |event, tables| {
                    let BorrowedArchiveV2LogEventKind::Plain { text } = event.kind else {
                        unreachable!();
                    };
                    assert_eq!(tables.string(text)?, "schema");
                    callbacks += 1;
                    Ok(())
                },
            )
            .unwrap();
            assert!(summary.has_error);
            assert_eq!(callbacks, 1);
        }
    }
}
