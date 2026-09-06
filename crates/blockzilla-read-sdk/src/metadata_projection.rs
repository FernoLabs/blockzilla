//! Bounded semantic projection of Compact V2 transaction metadata.
//!
//! The projector keeps only execution status, CPI instructions, and loaded
//! addresses. All other metadata lanes are parsed and validated without
//! retaining their vectors, strings, or byte buffers. A projector is bound to
//! one generation schema. It never probes another schema for one record.

use blockzilla_format::{
    CompactInnerInstruction, CompactMetaV1, CompactPubkey, CompactReturnData, CompactReward,
    CompactTokenBalance, DataArray, WincodeLeb128Config,
    program_logs::{
        system_program::{PubkeyOrString, SystemAddress, SystemProgramLog},
        token_2022::Token2022Log,
    },
};
use thiserror::Error;
use wincode::{ReadResult, SchemaRead, error::invalid_tag_encoding, io::Reader};

use crate::{CompactV2MetadataSchema, message_projection::ProjectedCompactV2Message};

type Cfg = WincodeLeb128Config;

/// Compact message account indexes are one byte wide.
pub const MAX_COMPACT_V2_METADATA_ACCOUNTS: usize = u8::MAX as usize + 1;
/// A transaction-error instruction index is one byte wide.
pub const MAX_COMPACT_V2_TOP_LEVEL_INSTRUCTIONS: usize = u8::MAX as usize + 1;
/// Independent safety limit for all CPI instructions in one metadata record.
pub const MAX_COMPACT_V2_CPI_INSTRUCTIONS: usize = 1 << 16;

const MAX_COMPACT_V2_CPI_ACCOUNTS_PER_INSTRUCTION: usize = 1 << 16;
const MAX_ERROR_BYTES: usize = 1 << 16;
const MAX_INNER_DATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_LOG_TABLE_ITEMS: usize = 1 << 20;

#[derive(Debug, Error)]
pub enum CompactV2MetadataProjectionError {
    #[error("Compact V2 metadata wire decode failed: {0}")]
    Decode(#[from] wincode::error::ReadError),

    #[error("Compact V2 metadata has {0} trailing bytes")]
    TrailingBytes(usize),

    #[error("Compact V2 split metadata {plane} plane has {remaining} trailing bytes")]
    SplitPlaneTrailingBytes {
        plane: &'static str,
        remaining: usize,
    },
}

pub type CompactV2MetadataProjectionResult<T> =
    std::result::Result<T, CompactV2MetadataProjectionError>;

/// Message facts that bound references stored in one metadata record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactV2MetadataProjectionLimits {
    pub total_message_accounts: usize,
    pub top_level_instruction_count: usize,
    /// Exact writable lookup count from the projected message.
    pub expected_loaded_writable: usize,
    /// Exact readonly lookup count from the projected message.
    pub expected_loaded_readonly: usize,
}

impl CompactV2MetadataProjectionLimits {
    /// Bind metadata limits to an already validated message projection.
    /// Legacy and V1 messages supply zero for both loaded-address counts.
    pub fn for_message(message: &ProjectedCompactV2Message<'_>) -> Self {
        message.count_limits()
    }
}

/// The archived execution result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactV2ExecutionStatus {
    Succeeded,
    Failed {
        /// Present only for `TransactionError::InstructionError`.
        failed_outer_instruction_index: Option<u8>,
    },
}

impl CompactV2ExecutionStatus {
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Succeeded)
    }

    pub const fn failed_outer_instruction_index(self) -> Option<u8> {
        match self {
            Self::Failed {
                failed_outer_instruction_index,
            } => failed_outer_instruction_index,
            Self::Succeeded => None,
        }
    }
}

/// One CPI instruction. Account and instruction-data bytes borrow the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectedCompactV2InnerInstruction<'de> {
    pub program_id_index: u32,
    pub accounts: &'de [u8],
    pub data: &'de [u8],
    pub stack_height: Option<u32>,
}

/// The CPI instructions executed by one outer instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedCompactV2InnerInstructionGroup<'de> {
    pub outer_instruction_index: u32,
    pub instructions: Vec<ProjectedCompactV2InnerInstruction<'de>>,
}

/// The selected metadata graph needed by instruction-based readers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedCompactV2Metadata<'de> {
    pub execution_status: CompactV2ExecutionStatus,
    /// This preserves the wire distinction between `None` and `Some([])`.
    pub inner_instructions: Option<Vec<ProjectedCompactV2InnerInstructionGroup<'de>>>,
    pub loaded_writable_addresses: Vec<CompactPubkey>,
    pub loaded_readonly_addresses: Vec<CompactPubkey>,
}

/// Recorded token-balance rows selected from one transaction metadata record.
#[derive(Debug, Default, Clone)]
pub struct ProjectedCompactV2TokenBalances {
    pub pre: Vec<CompactTokenBalance>,
    pub post: Vec<CompactTokenBalance>,
}

/// A generation-bound Compact V2 metadata projector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactV2MetadataProjector {
    schema: CompactV2MetadataSchema,
    registry_entries: u32,
}

/// Counts from validated metadata, without a retained CPI or loaded-key graph.
#[derive(Debug, Clone, Copy)]
pub struct MetadataCounts {
    pub execution_status: CompactV2ExecutionStatus,
    pub inner: Option<(u64, u64)>, // groups, instructions; None means not recorded
}

impl CompactV2MetadataProjector {
    pub const fn new(schema: CompactV2MetadataSchema, registry_entries: u32) -> Self {
        Self {
            schema,
            registry_entries,
        }
    }

    pub const fn schema(self) -> CompactV2MetadataSchema {
        self.schema
    }

    pub const fn registry_entries(self) -> u32 {
        self.registry_entries
    }

    /// Project and validate one complete metadata record.
    pub fn project<'de>(
        self,
        bytes: &'de [u8],
        limits: CompactV2MetadataProjectionLimits,
    ) -> CompactV2MetadataProjectionResult<ProjectedCompactV2Metadata<'de>> {
        self.project_impl(bytes, limits, None)
    }

    pub fn count(
        &self,
        bytes: &[u8],
        limits: CompactV2MetadataProjectionLimits,
    ) -> CompactV2MetadataProjectionResult<MetadataCounts> {
        let mut inner = None;
        let metadata = self.project_impl(bytes, limits, Some(&mut inner))?;
        Ok(MetadataCounts {
            execution_status: metadata.execution_status,
            inner,
        })
    }

    fn project_impl<'de>(
        self,
        bytes: &'de [u8],
        limits: CompactV2MetadataProjectionLimits,
        counts: Option<&mut Option<(u64, u64)>>,
    ) -> CompactV2MetadataProjectionResult<ProjectedCompactV2Metadata<'de>> {
        let count_only = counts.is_some();
        validate_limits(limits)?;
        let mut cursor = bytes;
        let execution_status = match self.schema {
            CompactV2MetadataSchema::CurrentTypedError => {
                read_current_execution_status(&mut cursor, limits)?
            }
            CompactV2MetadataSchema::LegacyRawError => {
                read_legacy_execution_status(&mut cursor, limits)?
            }
        };

        get::<u64>(&mut cursor)?; // fee
        let pre_balance_count = skip_balances(&mut cursor, limits.total_message_accounts)?;
        let post_balance_count = skip_balances(&mut cursor, limits.total_message_accounts)?;
        if pre_balance_count != limits.total_message_accounts
            || post_balance_count != limits.total_message_accounts
        {
            return Err(wincode::error::invalid_value(
                "pre- and post-balance counts do not equal resolved message accounts",
            )
            .into());
        }

        let inner_instructions = if let Some(counts) = counts {
            *counts = skip_inner_instruction_groups(&mut cursor, limits)?;
            None
        } else {
            read_inner_instruction_groups(&mut cursor, limits)?
        };
        skip_logs(&mut cursor, self.registry_entries)?;
        skip_token_balances(
            &mut cursor,
            limits.total_message_accounts,
            self.registry_entries,
        )?;
        skip_token_balances(
            &mut cursor,
            limits.total_message_accounts,
            self.registry_entries,
        )?;
        skip_rewards(&mut cursor, self.registry_entries)?;

        let loaded_writable_addresses = if count_only {
            skip_loaded_addresses(
                &mut cursor,
                limits.expected_loaded_writable,
                self.registry_entries,
            )?;
            Vec::new()
        } else {
            read_loaded_addresses(
                &mut cursor,
                limits.expected_loaded_writable,
                self.registry_entries,
            )?
        };
        let loaded_readonly_addresses = if count_only {
            skip_loaded_addresses(
                &mut cursor,
                limits.expected_loaded_readonly,
                self.registry_entries,
            )?;
            Vec::new()
        } else {
            read_loaded_addresses(
                &mut cursor,
                limits.expected_loaded_readonly,
                self.registry_entries,
            )?
        };

        skip_return_data(&mut cursor, self.registry_entries)?;
        get::<Option<u64>>(&mut cursor)?; // compute_units_consumed
        get::<Option<u64>>(&mut cursor)?; // cost_units

        if !cursor.is_empty() {
            return Err(CompactV2MetadataProjectionError::TrailingBytes(
                cursor.len(),
            ));
        }

        Ok(ProjectedCompactV2Metadata {
            execution_status,
            inner_instructions,
            loaded_writable_addresses,
            loaded_readonly_addresses,
        })
    }

    /// Project only recorded pre- and post-token balances from one complete
    /// Compact V2 metadata record.
    ///
    /// Other fields are parsed and validated without retaining their vectors.
    pub fn project_token_balances(
        self,
        bytes: &[u8],
        limits: CompactV2MetadataProjectionLimits,
    ) -> CompactV2MetadataProjectionResult<ProjectedCompactV2TokenBalances> {
        let mut output = ProjectedCompactV2TokenBalances::default();
        self.project_token_balances_reusing(bytes, limits, &mut output)?;
        Ok(output)
    }

    /// Retain bounded worker-owned storage, not a new pair of lists per transaction.
    /// Read `output` only after this method returns successfully.
    pub fn project_token_balances_reusing(
        self,
        bytes: &[u8],
        limits: CompactV2MetadataProjectionLimits,
        output: &mut ProjectedCompactV2TokenBalances,
    ) -> CompactV2MetadataProjectionResult<()> {
        validate_limits(limits)?;
        let mut cursor = bytes;
        match self.schema {
            CompactV2MetadataSchema::CurrentTypedError => {
                read_current_execution_status(&mut cursor, limits)?;
            }
            CompactV2MetadataSchema::LegacyRawError => {
                read_legacy_execution_status(&mut cursor, limits)?;
            }
        }

        get::<u64>(&mut cursor)?; // fee
        let pre_balance_count = skip_balances(&mut cursor, limits.total_message_accounts)?;
        let post_balance_count = skip_balances(&mut cursor, limits.total_message_accounts)?;
        if pre_balance_count != limits.total_message_accounts
            || post_balance_count != limits.total_message_accounts
        {
            return Err(wincode::error::invalid_value(
                "pre- and post-balance counts do not equal resolved message accounts",
            )
            .into());
        }

        skip_inner_instruction_groups(&mut cursor, limits)?;
        skip_logs(&mut cursor, self.registry_entries)?;
        read_token_balances_into(
            &mut cursor,
            limits.total_message_accounts,
            self.registry_entries,
            &mut output.pre,
        )?;
        read_token_balances_into(
            &mut cursor,
            limits.total_message_accounts,
            self.registry_entries,
            &mut output.post,
        )?;
        skip_rewards(&mut cursor, self.registry_entries)?;
        skip_loaded_addresses(
            &mut cursor,
            limits.expected_loaded_writable,
            self.registry_entries,
        )?;
        skip_loaded_addresses(
            &mut cursor,
            limits.expected_loaded_readonly,
            self.registry_entries,
        )?;
        skip_return_data(&mut cursor, self.registry_entries)?;
        get::<Option<u64>>(&mut cursor)?; // compute_units_consumed
        get::<Option<u64>>(&mut cursor)?; // cost_units

        if !cursor.is_empty() {
            return Err(CompactV2MetadataProjectionError::TrailingBytes(
                cursor.len(),
            ));
        }
        Ok(())
    }

    /// Project the exact token-balance plane retained by Indexer V3.
    pub fn project_split_token_balances(
        self,
        token_balances: &[u8],
        limits: CompactV2MetadataProjectionLimits,
    ) -> CompactV2MetadataProjectionResult<ProjectedCompactV2TokenBalances> {
        let mut output = ProjectedCompactV2TokenBalances::default();
        self.project_split_token_balances_reusing(token_balances, limits, &mut output)?;
        Ok(output)
    }

    pub fn project_split_token_balances_reusing(
        self,
        token_balances: &[u8],
        limits: CompactV2MetadataProjectionLimits,
        output: &mut ProjectedCompactV2TokenBalances,
    ) -> CompactV2MetadataProjectionResult<()> {
        validate_limits(limits)?;
        let mut cursor = token_balances;
        read_token_balances_into(
            &mut cursor,
            limits.total_message_accounts,
            self.registry_entries,
            &mut output.pre,
        )?;
        read_token_balances_into(
            &mut cursor,
            limits.total_message_accounts,
            self.registry_entries,
            &mut output.post,
        )?;
        require_split_plane_consumed("token-balances", cursor)?;
        Ok(())
    }

    /// Project the three semantic metadata planes retained by Indexer V3.
    ///
    /// `outcome` is the exact concatenation of the original error, fee,
    /// return-data, compute-unit, and cost-unit fields. `loaded_addresses` is
    /// the exact concatenation of the writable and readonly loaded-address
    /// vectors. `inner_instructions` is the exact encoded optional CPI field.
    /// Each plane must be complete and have no trailing bytes.
    ///
    /// This helper validates only these retained planes. It makes no claim
    /// about the semantic content of the other Indexer V3 metadata planes.
    pub fn project_split_planes<'de>(
        self,
        outcome: &'de [u8],
        loaded_addresses: &'de [u8],
        inner_instructions: &'de [u8],
        limits: CompactV2MetadataProjectionLimits,
    ) -> CompactV2MetadataProjectionResult<ProjectedCompactV2Metadata<'de>> {
        validate_limits(limits)?;

        let mut outcome_cursor = outcome;
        let execution_status = match self.schema {
            CompactV2MetadataSchema::CurrentTypedError => {
                read_current_execution_status(&mut outcome_cursor, limits)?
            }
            CompactV2MetadataSchema::LegacyRawError => {
                read_legacy_execution_status(&mut outcome_cursor, limits)?
            }
        };
        get::<u64>(&mut outcome_cursor)?; // fee
        skip_return_data(&mut outcome_cursor, self.registry_entries)?;
        get::<Option<u64>>(&mut outcome_cursor)?; // compute_units_consumed
        get::<Option<u64>>(&mut outcome_cursor)?; // cost_units
        require_split_plane_consumed("outcome", outcome_cursor)?;

        let mut loaded_cursor = loaded_addresses;
        let loaded_writable_addresses = read_loaded_addresses(
            &mut loaded_cursor,
            limits.expected_loaded_writable,
            self.registry_entries,
        )?;
        let loaded_readonly_addresses = read_loaded_addresses(
            &mut loaded_cursor,
            limits.expected_loaded_readonly,
            self.registry_entries,
        )?;
        require_split_plane_consumed("loaded-addresses", loaded_cursor)?;

        let mut inner_cursor = inner_instructions;
        let inner_instructions = read_inner_instruction_groups(&mut inner_cursor, limits)?;
        require_split_plane_consumed("inner-instructions", inner_cursor)?;

        Ok(ProjectedCompactV2Metadata {
            execution_status,
            inner_instructions,
            loaded_writable_addresses,
            loaded_readonly_addresses,
        })
    }

    pub fn count_split_planes(
        self,
        outcome: &[u8],
        loaded: &[u8],
        inner: &[u8],
        limits: CompactV2MetadataProjectionLimits,
    ) -> CompactV2MetadataProjectionResult<MetadataCounts> {
        validate_limits(limits)?;
        let mut cursor = outcome;
        let execution_status = match self.schema {
            CompactV2MetadataSchema::CurrentTypedError => {
                read_current_execution_status(&mut cursor, limits)?
            }
            CompactV2MetadataSchema::LegacyRawError => {
                read_legacy_execution_status(&mut cursor, limits)?
            }
        };
        get::<u64>(&mut cursor)?;
        skip_return_data(&mut cursor, self.registry_entries)?;
        get::<Option<u64>>(&mut cursor)?;
        get::<Option<u64>>(&mut cursor)?;
        require_split_plane_consumed("outcome", cursor)?;
        let mut cursor = loaded;
        skip_loaded_addresses(
            &mut cursor,
            limits.expected_loaded_writable,
            self.registry_entries,
        )?;
        skip_loaded_addresses(
            &mut cursor,
            limits.expected_loaded_readonly,
            self.registry_entries,
        )?;
        require_split_plane_consumed("loaded-addresses", cursor)?;
        let mut cursor = inner;
        let inner = skip_inner_instruction_groups(&mut cursor, limits)?;
        require_split_plane_consumed("inner-instructions", cursor)?;
        Ok(MetadataCounts {
            execution_status,
            inner,
        })
    }
}

fn require_split_plane_consumed(
    plane: &'static str,
    remaining: &[u8],
) -> CompactV2MetadataProjectionResult<()> {
    if !remaining.is_empty() {
        return Err(CompactV2MetadataProjectionError::SplitPlaneTrailingBytes {
            plane,
            remaining: remaining.len(),
        });
    }
    Ok(())
}

#[inline]
fn get<'de, T: SchemaRead<'de, Cfg>>(cursor: &mut &'de [u8]) -> ReadResult<T::Dst> {
    T::get(&mut *cursor)
}

fn validate_limits(limits: CompactV2MetadataProjectionLimits) -> ReadResult<()> {
    if limits.total_message_accounts > MAX_COMPACT_V2_METADATA_ACCOUNTS {
        return Err(wincode::error::invalid_value(
            "total message account count exceeds the Compact V2 account cap",
        ));
    }
    if limits.top_level_instruction_count > MAX_COMPACT_V2_TOP_LEVEL_INSTRUCTIONS {
        return Err(wincode::error::invalid_value(
            "top-level instruction count exceeds the Compact V2 instruction cap",
        ));
    }
    if limits
        .expected_loaded_writable
        .checked_add(limits.expected_loaded_readonly)
        .is_none_or(|count| count > limits.total_message_accounts)
    {
        return Err(wincode::error::invalid_value(
            "expected loaded address count exceeds resolved message accounts",
        ));
    }
    Ok(())
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
fn read_bytes_bounded<'de>(
    cursor: &mut &'de [u8],
    maximum: usize,
    error: &'static str,
) -> ReadResult<&'de [u8]> {
    let len = read_bounded_len(cursor, maximum.min(cursor.len()), error)?;
    Ok(cursor.take_borrowed(len)?)
}

#[inline]
fn read_archive_string(cursor: &mut &[u8], maximum: usize) -> ReadResult<()> {
    let bytes = read_bytes_bounded(cursor, maximum, "string length exceeds its safety limit")?;
    std::str::from_utf8(bytes)
        .map_err(|_| wincode::error::invalid_value("string is not valid UTF-8"))?;
    Ok(())
}

#[inline]
fn validate_pubkey(value: CompactPubkey, registry_entries: u32) -> ReadResult<CompactPubkey> {
    if let CompactPubkey::Id(id) = value
        && (id == 0 || id > registry_entries)
    {
        return Err(wincode::error::invalid_value(
            "pubkey registry ID is outside the admitted registry",
        ));
    }
    Ok(value)
}

#[inline]
fn get_pubkey(cursor: &mut &[u8], registry_entries: u32) -> ReadResult<CompactPubkey> {
    validate_pubkey(get::<CompactPubkey>(cursor)?, registry_entries)
}

#[inline]
fn get_optional_pubkey(
    cursor: &mut &[u8],
    registry_entries: u32,
) -> ReadResult<Option<CompactPubkey>> {
    get::<Option<CompactPubkey>>(cursor)?
        .map(|value| validate_pubkey(value, registry_entries))
        .transpose()
}

fn read_current_execution_status(
    cursor: &mut &[u8],
    limits: CompactV2MetadataProjectionLimits,
) -> ReadResult<CompactV2ExecutionStatus> {
    match get::<u8>(cursor)? {
        0 => Ok(CompactV2ExecutionStatus::Succeeded),
        1 => {
            let failed_outer_instruction_index = read_current_transaction_error(cursor, limits)?;
            Ok(CompactV2ExecutionStatus::Failed {
                failed_outer_instruction_index,
            })
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn read_current_transaction_error(
    cursor: &mut &[u8],
    limits: CompactV2MetadataProjectionLimits,
) -> ReadResult<Option<u8>> {
    match get::<u8>(cursor)? {
        8 => {
            let index = get::<u8>(cursor)?;
            validate_instruction_index(index, limits.top_level_instruction_count)?;
            skip_current_instruction_error(cursor)?;
            Ok(Some(index))
        }
        30 => {
            let index = get::<u8>(cursor)?;
            validate_instruction_index(index, limits.top_level_instruction_count)?;
            Ok(None)
        }
        31 | 35 => {
            let index = get::<u8>(cursor)?;
            validate_account_index(index, limits.total_message_accounts)?;
            Ok(None)
        }
        0..=38 => Ok(None),
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_current_instruction_error(cursor: &mut &[u8]) -> ReadResult<()> {
    match get::<u8>(cursor)? {
        25 => {
            get::<u32>(cursor)?;
            Ok(())
        }
        44 => read_archive_string(cursor, MAX_ERROR_BYTES),
        0..=53 => Ok(()),
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn read_legacy_execution_status(
    cursor: &mut &[u8],
    limits: CompactV2MetadataProjectionLimits,
) -> ReadResult<CompactV2ExecutionStatus> {
    match get::<u8>(cursor)? {
        0 => Ok(CompactV2ExecutionStatus::Succeeded),
        1 => {
            let raw = read_bytes_bounded(
                cursor,
                MAX_ERROR_BYTES,
                "legacy transaction-error length exceeds its safety limit",
            )?;
            let failed_outer_instruction_index = parse_legacy_transaction_error(raw, limits)?;
            Ok(CompactV2ExecutionStatus::Failed {
                failed_outer_instruction_index,
            })
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn parse_legacy_transaction_error(
    bytes: &[u8],
    limits: CompactV2MetadataProjectionLimits,
) -> ReadResult<Option<u8>> {
    let mut cursor = bytes;
    let tag = read_fixed_u32(&mut cursor)?;
    let failed = match tag {
        8 => {
            let index = read_fixed_u8(&mut cursor)?;
            validate_instruction_index(index, limits.top_level_instruction_count)?;
            skip_legacy_instruction_error(&mut cursor)?;
            Some(index)
        }
        30 => {
            let index = read_fixed_u8(&mut cursor)?;
            validate_instruction_index(index, limits.top_level_instruction_count)?;
            None
        }
        31 | 35 => {
            let index = read_fixed_u8(&mut cursor)?;
            validate_account_index(index, limits.total_message_accounts)?;
            None
        }
        0..=38 => None,
        other => return Err(invalid_tag_encoding(other as usize)),
    };
    if !cursor.is_empty() {
        return Err(wincode::error::invalid_value(
            "legacy transaction error has trailing bytes",
        ));
    }
    Ok(failed)
}

fn skip_legacy_instruction_error(cursor: &mut &[u8]) -> ReadResult<()> {
    match read_fixed_u32(cursor)? {
        25 => {
            read_fixed_u32(cursor)?;
            Ok(())
        }
        44 if cursor.is_empty() => Ok(()), // Historical unit BorshIoError.
        44 => {
            let len = usize::try_from(read_fixed_u64(cursor)?)
                .map_err(|_| wincode::error::pointer_sized_decode_error())?;
            if len > MAX_ERROR_BYTES || len > cursor.len() {
                return Err(wincode::error::invalid_value(
                    "legacy Borsh error string exceeds its safety limit",
                ));
            }
            let bytes = cursor.take_borrowed(len)?;
            std::str::from_utf8(bytes).map_err(|_| {
                wincode::error::invalid_value("legacy Borsh error string is not valid UTF-8")
            })?;
            Ok(())
        }
        0..=53 => Ok(()),
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn read_fixed_u8(cursor: &mut &[u8]) -> ReadResult<u8> {
    Ok(cursor.take_array::<1>()?[0])
}

fn read_fixed_u32(cursor: &mut &[u8]) -> ReadResult<u32> {
    Ok(u32::from_le_bytes(cursor.take_array::<4>()?))
}

fn read_fixed_u64(cursor: &mut &[u8]) -> ReadResult<u64> {
    Ok(u64::from_le_bytes(cursor.take_array::<8>()?))
}

fn validate_instruction_index(index: u8, count: usize) -> ReadResult<()> {
    if usize::from(index) >= count {
        return Err(wincode::error::invalid_value(
            "transaction-error instruction index is outside top-level instructions",
        ));
    }
    Ok(())
}

fn validate_account_index(index: u8, count: usize) -> ReadResult<()> {
    if usize::from(index) >= count {
        return Err(wincode::error::invalid_value(
            "transaction-error account index is outside resolved message accounts",
        ));
    }
    Ok(())
}

fn skip_balances(cursor: &mut &[u8], maximum: usize) -> ReadResult<usize> {
    let count = read_bounded_len(
        cursor,
        maximum.min(cursor.len()),
        "balance count exceeds total message account count",
    )?;
    for _ in 0..count {
        get::<u64>(cursor)?;
    }
    Ok(count)
}

fn read_inner_instruction_groups<'de>(
    cursor: &mut &'de [u8],
    limits: CompactV2MetadataProjectionLimits,
) -> ReadResult<Option<Vec<ProjectedCompactV2InnerInstructionGroup<'de>>>> {
    match get::<u8>(cursor)? {
        0 => Ok(None),
        1 => {
            // Each group needs at least a one-byte index and one-byte vector length.
            let group_maximum = limits.top_level_instruction_count.min(cursor.len() / 2);
            let group_count = read_bounded_len(
                cursor,
                group_maximum,
                "CPI group count exceeds its semantic or input bound",
            )?;
            let mut groups = Vec::with_capacity(group_count);
            let mut previous_index = None;
            let mut total_instructions = 0usize;

            for _ in 0..group_count {
                let outer_instruction_index = get::<u32>(cursor)?;
                let outer_index = usize::try_from(outer_instruction_index)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                if outer_index >= limits.top_level_instruction_count {
                    return Err(wincode::error::invalid_value(
                        "CPI group index is outside top-level instructions",
                    ));
                }
                if previous_index.is_some_and(|previous| outer_instruction_index <= previous) {
                    return Err(wincode::error::invalid_value(
                        "CPI group indexes are not strictly increasing and unique",
                    ));
                }
                previous_index = Some(outer_instruction_index);

                // Each instruction needs at least four one-byte fields.
                let remaining_instruction_cap = MAX_COMPACT_V2_CPI_INSTRUCTIONS
                    .saturating_sub(total_instructions)
                    .min(cursor.len() / 4);
                let instruction_count = read_bounded_len(
                    cursor,
                    remaining_instruction_cap,
                    "CPI instruction count exceeds its safety or input bound",
                )?;
                total_instructions += instruction_count;
                let mut instructions = Vec::with_capacity(instruction_count);
                for _ in 0..instruction_count {
                    instructions.push(read_inner_instruction(
                        cursor,
                        limits.total_message_accounts,
                    )?);
                }
                groups.push(ProjectedCompactV2InnerInstructionGroup {
                    outer_instruction_index,
                    instructions,
                });
            }
            Ok(Some(groups))
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_inner_instruction_groups(
    cursor: &mut &[u8],
    limits: CompactV2MetadataProjectionLimits,
) -> ReadResult<Option<(u64, u64)>> {
    match get::<u8>(cursor)? {
        0 => Ok(None),
        1 => {
            let group_maximum = limits.top_level_instruction_count.min(cursor.len() / 2);
            let group_count = read_bounded_len(
                cursor,
                group_maximum,
                "CPI group count exceeds its semantic or input bound",
            )?;
            let mut previous_index = None;
            let mut total_instructions = 0usize;
            for _ in 0..group_count {
                let outer_instruction_index = get::<u32>(cursor)?;
                let outer_index = usize::try_from(outer_instruction_index)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                if outer_index >= limits.top_level_instruction_count {
                    return Err(wincode::error::invalid_value(
                        "CPI group index is outside top-level instructions",
                    ));
                }
                if previous_index.is_some_and(|previous| outer_instruction_index <= previous) {
                    return Err(wincode::error::invalid_value(
                        "CPI group indexes are not strictly increasing and unique",
                    ));
                }
                previous_index = Some(outer_instruction_index);
                let remaining_instruction_cap = MAX_COMPACT_V2_CPI_INSTRUCTIONS
                    .saturating_sub(total_instructions)
                    .min(cursor.len() / 4);
                let instruction_count = read_bounded_len(
                    cursor,
                    remaining_instruction_cap,
                    "CPI instruction count exceeds its safety or input bound",
                )?;
                total_instructions += instruction_count;
                for _ in 0..instruction_count {
                    read_inner_instruction(cursor, limits.total_message_accounts)?;
                }
            }
            Ok(Some((group_count as u64, total_instructions as u64)))
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn read_inner_instruction<'de>(
    cursor: &mut &'de [u8],
    total_message_accounts: usize,
) -> ReadResult<ProjectedCompactV2InnerInstruction<'de>> {
    let program_id_index = get::<u32>(cursor)?;
    let program_index = usize::try_from(program_id_index)
        .map_err(|_| wincode::error::pointer_sized_decode_error())?;
    if program_index >= total_message_accounts {
        return Err(wincode::error::invalid_value(
            "CPI program index is outside resolved message accounts",
        ));
    }

    // Repeated account indexes are legal, so the vector length is not bounded
    // by the number of distinct resolved message accounts. The borrowed slice
    // is still bounded by an independent safety cap and the remaining input.
    let accounts = read_bytes_bounded(
        cursor,
        MAX_COMPACT_V2_CPI_ACCOUNTS_PER_INSTRUCTION,
        "CPI account-index count exceeds its safety or input bound",
    )?;
    if accounts
        .iter()
        .any(|index| usize::from(*index) >= total_message_accounts)
    {
        return Err(wincode::error::invalid_value(
            "CPI account index is outside resolved message accounts",
        ));
    }
    let data = read_bytes_bounded(
        cursor,
        MAX_INNER_DATA_BYTES,
        "CPI data length exceeds its safety limit",
    )?;
    let stack_height = get::<Option<u32>>(cursor)?;
    if stack_height == Some(0) {
        return Err(wincode::error::invalid_value(
            "CPI stack height cannot be zero",
        ));
    }
    Ok(ProjectedCompactV2InnerInstruction {
        program_id_index,
        accounts,
        data,
        stack_height,
    })
}

fn skip_token_balances(
    cursor: &mut &[u8],
    maximum: usize,
    registry_entries: u32,
) -> ReadResult<()> {
    // Six fields have a minimum one-byte representation.
    let count = read_bounded_len(
        cursor,
        maximum.min(cursor.len() / 6),
        "token-balance count exceeds its semantic or input bound",
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
    Ok(())
}

fn read_token_balances_into(
    cursor: &mut &[u8],
    maximum: usize,
    registry_entries: u32,
    balances: &mut Vec<CompactTokenBalance>,
) -> ReadResult<()> {
    let count = read_bounded_len(
        cursor,
        maximum.min(cursor.len() / 6),
        "token-balance count exceeds its semantic or input bound",
    )?;
    balances.clear();
    balances.reserve(count);
    for _ in 0..count {
        let account_index = get::<u32>(cursor)?;
        let account_position = usize::try_from(account_index)
            .map_err(|_| wincode::error::pointer_sized_decode_error())?;
        if account_position >= maximum {
            return Err(wincode::error::invalid_value(
                "token-balance account index is outside resolved message accounts",
            ));
        }
        balances.push(CompactTokenBalance {
            account_index,
            mint: get_optional_pubkey(cursor, registry_entries)?,
            owner: get_optional_pubkey(cursor, registry_entries)?,
            program_id: get_optional_pubkey(cursor, registry_entries)?,
            amount: get::<u64>(cursor)?,
            decimals: get::<u8>(cursor)?,
        });
    }
    Ok(())
}

fn skip_rewards(cursor: &mut &[u8], registry_entries: u32) -> ReadResult<()> {
    // Five fields have a minimum one-byte representation.
    let count = read_bounded_len(
        cursor,
        MAX_LOG_TABLE_ITEMS.min(cursor.len() / 5),
        "reward count exceeds its safety or input bound",
    )?;
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
    expected: usize,
    registry_entries: u32,
) -> ReadResult<Vec<CompactPubkey>> {
    let count = read_bounded_len(
        cursor,
        expected.min(cursor.len()),
        "loaded address count exceeds the message lookup count or input bound",
    )?;
    if count != expected {
        return Err(wincode::error::invalid_value(
            "loaded address count does not equal the projected message lookup count",
        ));
    }
    let mut addresses = Vec::with_capacity(count);
    for _ in 0..count {
        addresses.push(get_pubkey(cursor, registry_entries)?);
    }
    Ok(addresses)
}

fn skip_loaded_addresses(
    cursor: &mut &[u8],
    expected: usize,
    registry_entries: u32,
) -> ReadResult<()> {
    let count = read_bounded_len(
        cursor,
        expected.min(cursor.len()),
        "loaded address count exceeds the message lookup count or input bound",
    )?;
    if count != expected {
        return Err(wincode::error::invalid_value(
            "loaded address count does not equal the projected message lookup count",
        ));
    }
    for _ in 0..count {
        get_pubkey(cursor, registry_entries)?;
    }
    Ok(())
}

fn skip_return_data(cursor: &mut &[u8], registry_entries: u32) -> ReadResult<()> {
    match get::<u8>(cursor)? {
        0 => Ok(()),
        1 => {
            get_pubkey(cursor, registry_entries)?;
            read_bytes_bounded(
                cursor,
                MAX_INNER_DATA_BYTES,
                "return-data length exceeds its safety limit",
            )?;
            Ok(())
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

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

fn validate_pubkey_or_string(
    value: PubkeyOrString,
    registry_entries: u32,
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
    registry_entries: u32,
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
    registry_entries: u32,
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
        Log::AuthorizeNonceAccount { msg } => {
            references.string(msg);
            Ok(())
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
    }
}

fn validate_token_2022_log(
    value: Token2022Log,
    registry_entries: u32,
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

/// Skip one `ProgramLog` without constructing its allocation-bearing known
/// program variants. Numeric tables are validated after the event stream.
fn skip_program_log(
    cursor: &mut &[u8],
    registry_entries: u32,
    references: &mut LogReferences,
) -> ReadResult<()> {
    match get::<u32>(cursor)? {
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
        13 | 16 => references.string(get::<u32>(cursor)?),
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
    match get::<u32>(cursor)? {
        0 => skip_drift_log(cursor),
        1 => skip_okx_router_log(cursor),
        2 => skip_phoenix_perps_log(cursor),
        3 => skip_phoenix_v1_log(cursor),
        4 => {
            get::<blockzilla_format::program_logs::known_programs::raydium_amm::RaydiumAmmLog>(
                cursor,
            )?;
            Ok(())
        }
        5 => {
            get::<
                blockzilla_format::program_logs::known_programs::static_programs::StaticProgramLog,
            >(cursor)?;
            Ok(())
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_drift_log(cursor: &mut &[u8]) -> ReadResult<()> {
    match get::<u32>(cursor)? {
        0 => {
            read_bytes_bounded(
                cursor,
                MAX_INNER_DATA_BYTES,
                "known Drift log bytes exceed their safety limit",
            )?;
        }
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
    match get::<u32>(cursor)? {
        0 => {
            read_archive_string(cursor, MAX_INNER_DATA_BYTES)?;
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            get::<blockzilla_format::program_logs::known_programs::okx_router::AmountInSpelling>(
                cursor,
            )?;
        }
        1 => read_archive_string(cursor, MAX_INNER_DATA_BYTES)?,
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
            read_archive_string(cursor, MAX_INNER_DATA_BYTES)?;
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
        0 => {
            read_bytes_bounded(
                cursor,
                MAX_INNER_DATA_BYTES,
                "known Phoenix log bytes exceed their safety limit",
            )?;
            Ok(())
        }
        1 => {
            get::<
                blockzilla_format::program_logs::known_programs::phoenix_perps::PhoenixPerpsStaticLog,
            >(cursor)?;
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
            read_archive_string(cursor, MAX_INNER_DATA_BYTES)?;
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
    registry_entries: u32,
    references: &mut LogReferences,
) -> ReadResult<()> {
    match get::<u32>(cursor)? {
        0 => validate_system_program_log(
            get::<SystemProgramLog>(cursor)?,
            registry_entries,
            references,
        )?,
        1 | 2 | 9..=13 | 38 | 39 | 43 => {}
        3 | 4 | 15 | 18 | 19 | 24..=27 | 40..=42 => {
            get_pubkey(cursor, registry_entries)?;
        }
        5 | 8 => skip_program_log(cursor, registry_entries, references)?,
        6 | 28 | 36 | 37 | 44 | 45 => references.string(get::<u32>(cursor)?),
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

fn skip_logs(cursor: &mut &[u8], registry_entries: u32) -> ReadResult<()> {
    match get::<u8>(cursor)? {
        0 => Ok(()),
        1 => {
            let mut references = LogReferences::default();
            let event_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log event count exceeds its safety or input bound",
            )?;
            for _ in 0..event_count {
                skip_log_event(cursor, registry_entries, &mut references)?;
            }

            let string_length_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log string-length count exceeds its safety or input bound",
            )?;
            let encoded_string_lengths = *cursor;
            let mut total_string_bytes = 0usize;
            for _ in 0..string_length_count {
                let length = usize::try_from(get::<u32>(cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                total_string_bytes = total_string_bytes.checked_add(length).ok_or_else(|| {
                    wincode::error::invalid_value("log string-table length overflow")
                })?;
            }
            let string_bytes = read_bytes_bounded(
                cursor,
                total_string_bytes,
                "log string-table byte lane exceeds its declared lengths",
            )?;
            if string_bytes.len() != total_string_bytes {
                return Err(wincode::error::invalid_value(
                    "log string-table lengths do not cover its byte lane exactly",
                ));
            }
            let mut encoded_string_lengths = encoded_string_lengths;
            let mut offset = 0usize;
            for _ in 0..string_length_count {
                let length = usize::try_from(get::<u32>(&mut encoded_string_lengths)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                let end = offset.checked_add(length).ok_or_else(|| {
                    wincode::error::invalid_value("log string-table offset overflow")
                })?;
                std::str::from_utf8(&string_bytes[offset..end]).map_err(|_| {
                    wincode::error::invalid_value("log string-table entry is not valid UTF-8")
                })?;
                offset = end;
            }

            let data_array_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log data-array count exceeds its safety or input bound",
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
            if total_chunks > MAX_LOG_TABLE_ITEMS {
                return Err(wincode::error::invalid_value(
                    "log data-table chunk count exceeds its safety limit",
                ));
            }
            let chunk_length_count = read_bounded_len(
                cursor,
                MAX_LOG_TABLE_ITEMS.min(cursor.len()),
                "log chunk-length count exceeds its safety or input bound",
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
            let data_bytes = read_bytes_bounded(
                cursor,
                total_data_bytes,
                "log data byte lane exceeds its declared chunk lengths",
            )?;
            if data_bytes.len() != total_data_bytes {
                return Err(wincode::error::invalid_value(
                    "log chunk lengths do not cover the data byte lane exactly",
                ));
            }
            references.validate(string_length_count, data_array_count)
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

// These destructures make an upstream metadata-lane shape change require a
// review of this field-by-field projector.
#[allow(dead_code)]
fn assert_metadata_shapes(
    metadata: CompactMetaV1,
    inner: CompactInnerInstruction,
    token: CompactTokenBalance,
    reward: CompactReward,
    return_data: CompactReturnData,
) {
    let CompactMetaV1 {
        err: _,
        fee: _,
        pre_balances: _,
        post_balances: _,
        inner_instructions: _,
        logs: _,
        pre_token_balances: _,
        post_token_balances: _,
        rewards: _,
        loaded_writable_addresses: _,
        loaded_readonly_addresses: _,
        return_data: _,
        compute_units_consumed: _,
        cost_units: _,
    } = metadata;
    let CompactInnerInstruction {
        program_id_index: _,
        accounts: _,
        data: _,
        stack_height: _,
    } = inner;
    let CompactTokenBalance {
        account_index: _,
        mint: _,
        owner: _,
        program_id: _,
        amount: _,
        decimals: _,
    } = token;
    let CompactReward {
        pubkey: _,
        lamports: _,
        post_balance: _,
        reward_type: _,
        commission: _,
    } = reward;
    let CompactReturnData {
        program_id: _,
        data: _,
    } = return_data;
}

#[cfg(test)]
mod tests {
    use blockzilla_format::{
        CompactInnerInstructions, CompactLogStream, CompactTransactionError, DataTable, LogEvent,
        StringTable, wincode_leb128_config,
    };
    use wincode::SchemaWrite;

    use super::*;

    const LIMITS: CompactV2MetadataProjectionLimits = CompactV2MetadataProjectionLimits {
        total_message_accounts: 4,
        top_level_instruction_count: 2,
        expected_loaded_writable: 1,
        expected_loaded_readonly: 1,
    };

    fn full_metadata(
        err: Option<CompactTransactionError>,
        inner_instructions: Option<Vec<CompactInnerInstructions>>,
    ) -> CompactMetaV1 {
        CompactMetaV1 {
            err,
            fee: 5_000,
            pre_balances: vec![100, 200, 300, 400],
            post_balances: vec![90, 210, 300, 400],
            inner_instructions,
            logs: Some(CompactLogStream {
                events: vec![LogEvent::Plain { text: 0 }, LogEvent::Data { data: 0 }],
                strings: StringTable {
                    lengths: vec![2],
                    bytes: b"ok".to_vec(),
                },
                data: DataTable {
                    arrays: vec![DataArray { chunk_count: 1 }],
                    chunk_lengths: vec![2],
                    bytes: vec![7, 8],
                },
            }),
            pre_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(2)),
                owner: Some(CompactPubkey::Raw([3; 32])),
                program_id: Some(CompactPubkey::Id(4)),
                amount: 10,
                decimals: 6,
            }],
            post_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(2)),
                owner: Some(CompactPubkey::Raw([3; 32])),
                program_id: Some(CompactPubkey::Id(4)),
                amount: 9,
                decimals: 6,
            }],
            rewards: vec![CompactReward {
                pubkey: CompactPubkey::Id(5),
                lamports: -1,
                post_balance: 99,
                reward_type: 1,
                commission: Some(2),
            }],
            loaded_writable_addresses: vec![CompactPubkey::Id(6)],
            loaded_readonly_addresses: vec![CompactPubkey::Raw([7; 32])],
            return_data: Some(CompactReturnData {
                program_id: CompactPubkey::Id(8),
                data: vec![1, 2, 3],
            }),
            compute_units_consumed: Some(1_000),
            cost_units: Some(2_000),
        }
    }

    fn current_bytes(value: &CompactMetaV1) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn current_split_bytes(value: &CompactMetaV1) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut outcome = wincode::config::serialize(&value.err, wincode_leb128_config()).unwrap();
        outcome.extend(wincode::config::serialize(&value.fee, wincode_leb128_config()).unwrap());
        outcome.extend(
            wincode::config::serialize(&value.return_data, wincode_leb128_config()).unwrap(),
        );
        outcome.extend(
            wincode::config::serialize(&value.compute_units_consumed, wincode_leb128_config())
                .unwrap(),
        );
        outcome.extend(
            wincode::config::serialize(&value.cost_units, wincode_leb128_config()).unwrap(),
        );

        let mut loaded =
            wincode::config::serialize(&value.loaded_writable_addresses, wincode_leb128_config())
                .unwrap();
        loaded.extend(
            wincode::config::serialize(&value.loaded_readonly_addresses, wincode_leb128_config())
                .unwrap(),
        );
        let inner =
            wincode::config::serialize(&value.inner_instructions, wincode_leb128_config()).unwrap();
        (outcome, loaded, inner)
    }

    fn token_split_bytes(value: &CompactMetaV1) -> Vec<u8> {
        let mut token =
            wincode::config::serialize(&value.pre_token_balances, wincode_leb128_config()).unwrap();
        token.extend(
            wincode::config::serialize(&value.post_token_balances, wincode_leb128_config())
                .unwrap(),
        );
        token
    }

    fn projector(schema: CompactV2MetadataSchema) -> CompactV2MetadataProjector {
        CompactV2MetadataProjector::new(schema, 100)
    }

    fn cpi_groups() -> Vec<CompactInnerInstructions> {
        vec![
            CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 3,
                    accounts: vec![0, 2],
                    data: vec![9, 8, 7],
                    stack_height: Some(3),
                }],
            },
            CompactInnerInstructions {
                index: 1,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 1,
                    accounts: vec![3],
                    data: vec![],
                    stack_height: None,
                }],
            },
        ]
    }

    #[test]
    fn current_projection_preserves_status_cpi_and_loaded_addresses() {
        let value = full_metadata(
            Some(CompactTransactionError::InstructionError(
                1,
                blockzilla_format::CompactInstructionError::Custom(42),
            )),
            Some(cpi_groups()),
        );
        let bytes = current_bytes(&value);
        let projected = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project(&bytes, LIMITS)
            .unwrap();

        assert_eq!(
            projected.execution_status,
            CompactV2ExecutionStatus::Failed {
                failed_outer_instruction_index: Some(1),
            }
        );
        let groups = projected.inner_instructions.unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].outer_instruction_index, 0);
        assert_eq!(groups[0].instructions[0].program_id_index, 3);
        assert_eq!(groups[0].instructions[0].accounts, [0, 2]);
        assert_eq!(groups[0].instructions[0].data, [9, 8, 7]);
        assert_eq!(groups[0].instructions[0].stack_height, Some(3));
        assert_eq!(groups[1].instructions[0].stack_height, None);
        assert_eq!(projected.loaded_writable_addresses, [CompactPubkey::Id(6)]);
        assert_eq!(
            projected.loaded_readonly_addresses,
            [CompactPubkey::Raw([7; 32])]
        );
    }

    #[test]
    fn token_balance_projection_matches_complete_and_split_metadata() {
        let value = full_metadata(None, Some(cpi_groups()));
        let complete = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project_token_balances(&current_bytes(&value), LIMITS)
            .unwrap();
        let split = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project_split_token_balances(&token_split_bytes(&value), LIMITS)
            .unwrap();

        for projected in [complete, split] {
            assert_eq!(projected.pre.len(), 1);
            assert_eq!(projected.post.len(), 1);
            assert_eq!(projected.pre[0].account_index, 0);
            assert!(matches!(projected.pre[0].mint, Some(CompactPubkey::Id(2))));
            assert_eq!(projected.pre[0].amount, 10);
            assert_eq!(projected.post[0].amount, 9);
            assert_eq!(projected.post[0].decimals, 6);
        }
    }

    #[test]
    fn token_balance_storage_is_reused_and_empty_rows_do_not_keep_old_values() {
        let mut value = full_metadata(None, Some(cpi_groups()));
        let projector = projector(CompactV2MetadataSchema::CurrentTypedError);
        let mut output = ProjectedCompactV2TokenBalances::default();
        projector
            .project_token_balances_reusing(&current_bytes(&value), LIMITS, &mut output)
            .unwrap();
        let capacities = (output.pre.capacity(), output.post.capacity());
        value.pre_token_balances.clear();
        value.post_token_balances[0].amount = 123;
        projector
            .project_split_token_balances_reusing(&token_split_bytes(&value), LIMITS, &mut output)
            .unwrap();
        assert!(output.pre.is_empty());
        assert_eq!(output.post.len(), 1);
        assert_eq!(output.post[0].amount, 123);
        assert_eq!(capacities, (output.pre.capacity(), output.post.capacity()));
        projector
            .project_token_balances_reusing(&current_bytes(&value), LIMITS, &mut output)
            .unwrap();
        assert!(output.pre.is_empty());
        assert_eq!(output.post[0].amount, 123);
        assert_eq!(capacities, (output.pre.capacity(), output.post.capacity()));
    }

    #[test]
    fn split_projection_matches_the_retained_semantic_fields() {
        let value = full_metadata(
            Some(CompactTransactionError::InstructionError(
                1,
                blockzilla_format::CompactInstructionError::Custom(42),
            )),
            Some(cpi_groups()),
        );
        let full_bytes = current_bytes(&value);
        let full = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project(&full_bytes, LIMITS)
            .unwrap();
        let (outcome, loaded, inner) = current_split_bytes(&value);
        let split = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project_split_planes(&outcome, &loaded, &inner, LIMITS)
            .unwrap();

        assert_eq!(split, full);
    }

    #[test]
    fn repeated_cpi_account_indexes_can_exceed_distinct_account_count() {
        let groups = vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 1,
                accounts: vec![0, 1, 0, 1, 0],
                data: vec![],
                stack_height: Some(2),
            }],
        }];
        let value = full_metadata(None, Some(groups));

        let full_bytes = current_bytes(&value);
        let full = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project(&full_bytes, LIMITS)
            .unwrap();
        let (outcome, loaded, inner) = current_split_bytes(&value);
        let split = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project_split_planes(&outcome, &loaded, &inner, LIMITS)
            .unwrap();

        let expected = [0, 1, 0, 1, 0];
        assert_eq!(
            full.inner_instructions.unwrap()[0].instructions[0].accounts,
            expected
        );
        assert_eq!(
            split.inner_instructions.unwrap()[0].instructions[0].accounts,
            expected
        );
    }

    #[test]
    fn repeated_cpi_accounts_still_require_each_index_to_resolve() {
        let groups = vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 1,
                accounts: vec![0, 4, 0, 4, 0],
                data: vec![],
                stack_height: Some(2),
            }],
        }];
        let value = full_metadata(None, Some(groups));

        let full_bytes = current_bytes(&value);
        assert!(
            projector(CompactV2MetadataSchema::CurrentTypedError)
                .project(&full_bytes, LIMITS)
                .is_err()
        );
        let (outcome, loaded, inner) = current_split_bytes(&value);
        assert!(
            projector(CompactV2MetadataSchema::CurrentTypedError)
                .project_split_planes(&outcome, &loaded, &inner, LIMITS)
                .is_err()
        );
    }

    #[test]
    fn split_projection_preserves_cpi_option_and_rejects_plane_trailing_bytes() {
        for expected_some in [false, true] {
            let value = full_metadata(None, expected_some.then(Vec::new));
            let (outcome, loaded, inner) = current_split_bytes(&value);
            let split = projector(CompactV2MetadataSchema::CurrentTypedError)
                .project_split_planes(&outcome, &loaded, &inner, LIMITS)
                .unwrap();
            assert_eq!(split.inner_instructions.is_some(), expected_some);
            assert!(
                split
                    .inner_instructions
                    .is_none_or(|groups| groups.is_empty())
            );
        }

        let value = full_metadata(None, None);
        let (mut outcome, loaded, inner) = current_split_bytes(&value);
        outcome.push(0xff);
        assert!(matches!(
            projector(CompactV2MetadataSchema::CurrentTypedError)
                .project_split_planes(&outcome, &loaded, &inner, LIMITS)
                .unwrap_err(),
            CompactV2MetadataProjectionError::SplitPlaneTrailingBytes {
                plane: "outcome",
                remaining: 1,
            }
        ));
    }

    #[test]
    fn cpi_option_presence_is_exact() {
        let none = current_bytes(&full_metadata(None, None));
        let some_empty = current_bytes(&full_metadata(None, Some(vec![])));

        let none = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project(&none, LIMITS)
            .unwrap();
        let some_empty = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project(&some_empty, LIMITS)
            .unwrap();

        assert_eq!(none.inner_instructions, None);
        assert_eq!(some_empty.inner_instructions, Some(vec![]));
    }

    #[derive(SchemaWrite)]
    struct LegacyRawMetadata {
        err: Option<Vec<u8>>,
        fee: u64,
        pre_balances: Vec<u64>,
        post_balances: Vec<u64>,
        inner_instructions: Option<Vec<CompactInnerInstructions>>,
        logs: Option<CompactLogStream>,
        pre_token_balances: Vec<CompactTokenBalance>,
        post_token_balances: Vec<CompactTokenBalance>,
        rewards: Vec<CompactReward>,
        loaded_writable_addresses: Vec<CompactPubkey>,
        loaded_readonly_addresses: Vec<CompactPubkey>,
        return_data: Option<CompactReturnData>,
        compute_units_consumed: Option<u64>,
        cost_units: Option<u64>,
    }

    fn legacy_bytes(raw_error: Vec<u8>) -> Vec<u8> {
        let current = full_metadata(None, Some(cpi_groups()));
        let value = LegacyRawMetadata {
            err: Some(raw_error),
            fee: current.fee,
            pre_balances: current.pre_balances,
            post_balances: current.post_balances,
            inner_instructions: current.inner_instructions,
            logs: current.logs,
            pre_token_balances: current.pre_token_balances,
            post_token_balances: current.post_token_balances,
            rewards: current.rewards,
            loaded_writable_addresses: current.loaded_writable_addresses,
            loaded_readonly_addresses: current.loaded_readonly_addresses,
            return_data: current.return_data,
            compute_units_consumed: current.compute_units_consumed,
            cost_units: current.cost_units,
        };
        wincode::config::serialize(&value, wincode_leb128_config()).unwrap()
    }

    fn legacy_instruction_error(index: u8, instruction_tag: u32, payload: &[u8]) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&8u32.to_le_bytes());
        raw.push(index);
        raw.extend_from_slice(&instruction_tag.to_le_bytes());
        raw.extend_from_slice(payload);
        raw
    }

    #[test]
    fn legacy_projection_reads_raw_instruction_error() {
        let bytes = legacy_bytes(legacy_instruction_error(1, 25, &42u32.to_le_bytes()));
        let projected = projector(CompactV2MetadataSchema::LegacyRawError)
            .project(&bytes, LIMITS)
            .unwrap();
        assert_eq!(
            projected.execution_status,
            CompactV2ExecutionStatus::Failed {
                failed_outer_instruction_index: Some(1),
            }
        );
        assert_eq!(projected.inner_instructions.unwrap().len(), 2);
    }

    #[test]
    fn legacy_projection_accepts_historical_unit_borsh_error() {
        let bytes = legacy_bytes(legacy_instruction_error(0, 44, &[]));
        let projected = projector(CompactV2MetadataSchema::LegacyRawError)
            .project(&bytes, LIMITS)
            .unwrap();
        assert_eq!(
            projected.execution_status.failed_outer_instruction_index(),
            Some(0)
        );
    }

    #[test]
    fn rejects_duplicate_or_decreasing_cpi_group_indexes() {
        for indexes in [[0, 0], [1, 0]] {
            let groups = indexes
                .into_iter()
                .map(|index| CompactInnerInstructions {
                    index,
                    instructions: vec![],
                })
                .collect();
            let bytes = current_bytes(&full_metadata(None, Some(groups)));
            assert!(
                projector(CompactV2MetadataSchema::CurrentTypedError)
                    .project(&bytes, LIMITS)
                    .is_err()
            );
        }
    }

    #[test]
    fn rejects_a_huge_declared_cpi_group_count_before_reserving() {
        // err, fee, pre balances, post balances, Some(CPI), then 65,536.
        let bytes = [0, 0, 0, 0, 1, 0x80, 0x80, 0x04];
        let error = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project(&bytes, LIMITS)
            .unwrap_err();
        assert!(matches!(error, CompactV2MetadataProjectionError::Decode(_)));
    }

    #[test]
    fn rejects_invalid_stack_option_and_trailing_bytes() {
        // One group and one CPI. The final byte is an invalid Option tag.
        let invalid_stack = [0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 2];
        assert!(
            projector(CompactV2MetadataSchema::CurrentTypedError)
                .project(
                    &invalid_stack,
                    CompactV2MetadataProjectionLimits {
                        total_message_accounts: 1,
                        top_level_instruction_count: 1,
                        expected_loaded_writable: 0,
                        expected_loaded_readonly: 0,
                    },
                )
                .is_err()
        );

        let mut trailing = current_bytes(&full_metadata(None, None));
        trailing.push(0xff);
        assert!(matches!(
            projector(CompactV2MetadataSchema::CurrentTypedError)
                .project(&trailing, LIMITS)
                .unwrap_err(),
            CompactV2MetadataProjectionError::TrailingBytes(1)
        ));
    }

    #[test]
    fn validates_discarded_log_and_registry_lanes() {
        let mut invalid_log = full_metadata(None, None);
        invalid_log.logs = Some(CompactLogStream {
            events: vec![LogEvent::Plain { text: 0 }],
            strings: StringTable {
                lengths: vec![1],
                bytes: vec![0xff],
            },
            data: DataTable::default(),
        });
        let bytes = current_bytes(&invalid_log);
        assert!(
            projector(CompactV2MetadataSchema::CurrentTypedError)
                .project(&bytes, LIMITS)
                .is_err()
        );

        let mut invalid_reward = full_metadata(None, None);
        invalid_reward.rewards[0].pubkey = CompactPubkey::Id(101);
        let bytes = current_bytes(&invalid_reward);
        assert!(
            projector(CompactV2MetadataSchema::CurrentTypedError)
                .project(&bytes, LIMITS)
                .is_err()
        );
    }

    #[test]
    fn rejects_loaded_address_counts_that_differ_from_the_message() {
        let bytes = current_bytes(&full_metadata(None, None));
        for limits in [
            CompactV2MetadataProjectionLimits {
                expected_loaded_writable: 0,
                expected_loaded_readonly: 2,
                ..LIMITS
            },
            CompactV2MetadataProjectionLimits {
                expected_loaded_writable: 2,
                expected_loaded_readonly: 0,
                ..LIMITS
            },
            CompactV2MetadataProjectionLimits {
                expected_loaded_writable: 2,
                expected_loaded_readonly: 2,
                ..LIMITS
            },
        ] {
            assert!(
                projector(CompactV2MetadataSchema::CurrentTypedError)
                    .project(&bytes, limits)
                    .is_err()
            );
        }

        let mut without_loaded = full_metadata(None, None);
        without_loaded.loaded_writable_addresses.clear();
        without_loaded.loaded_readonly_addresses.clear();
        let bytes = current_bytes(&without_loaded);
        let no_lookup_limits = CompactV2MetadataProjectionLimits {
            expected_loaded_writable: 0,
            expected_loaded_readonly: 0,
            ..LIMITS
        };
        let projected = projector(CompactV2MetadataSchema::CurrentTypedError)
            .project(&bytes, no_lookup_limits)
            .unwrap();
        assert!(projected.loaded_writable_addresses.is_empty());
        assert!(projected.loaded_readonly_addresses.is_empty());
    }

    #[test]
    fn rejects_equal_but_short_lamport_balance_lanes() {
        let mut value = full_metadata(None, None);
        value.pre_balances.pop();
        value.post_balances.pop();
        let bytes = current_bytes(&value);
        assert!(
            projector(CompactV2MetadataSchema::CurrentTypedError)
                .project(&bytes, LIMITS)
                .is_err()
        );
    }

    #[test]
    fn rejects_zero_cpi_stack_height() {
        let groups = vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: vec![],
                stack_height: Some(0),
            }],
        }];
        let bytes = current_bytes(&full_metadata(None, Some(groups)));
        assert!(
            projector(CompactV2MetadataSchema::CurrentTypedError)
                .project(&bytes, LIMITS)
                .is_err()
        );
    }

    #[test]
    fn selected_schema_is_not_probed_or_retried() {
        let current = current_bytes(&full_metadata(
            Some(CompactTransactionError::InstructionError(
                0,
                blockzilla_format::CompactInstructionError::GenericError,
            )),
            None,
        ));
        assert!(
            projector(CompactV2MetadataSchema::LegacyRawError)
                .project(&current, LIMITS)
                .is_err()
        );
    }
}
