//! Portable phase-A token-balance audit formats.
//!
//! Phase A is source bound. Public-key references either name a one-based row
//! in the admitted epoch registry or a zero-based row in the phase-A inline
//! public-key table. Consolidation resolves both forms before it creates the
//! final global registry.
//!
//! All binary records in this module use explicit little-endian encoding.
//! Rust layout, `usize`, and enum representation are never part of the wire
//! contract. Reserved bytes must be zero so a future schema cannot be confused
//! with this one.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

pub const PHASE_A_SCHEMA_VERSION_V1: u16 = 1;
pub const PHASE_A_MANIFEST_FILE: &str = "manifest.json";
pub const PHASE_A_TRANSACTIONS_FILE: &str = "transactions.bin";
pub const PHASE_A_TOKEN_INSTRUCTIONS_FILE: &str = "token-instructions.bin";
pub const PHASE_A_INSTRUCTION_ACCOUNTS_FILE: &str = "instruction-accounts.bin";
pub const PHASE_A_INSTRUCTION_DATA_FILE: &str = "instruction-data.bin";
pub const PHASE_A_INLINE_PUBKEYS_FILE: &str = "inline-pubkeys.bin";
pub const PHASE_A_TOKEN_BALANCE_ORACLE_FILE: &str = "token-balance-oracle.bin";
pub const PHASE_A_COVERAGE_FILE: &str = "coverage.bin";

pub const PHASE_A_FILE_MAGIC_V1: [u8; 8] = *b"BZTBPA01";
pub const PHASE_A_FILE_HEADER_V1_ENCODED_LEN: usize = 64;
pub const PHASE_A_PUBKEY_REF_V1_ENCODED_LEN: usize = 8;
pub const SOURCE_TRANSACTION_COORDINATE_V1_ENCODED_LEN: usize = 40;
pub const PHASE_A_TRANSACTION_RECORD_V1_ENCODED_LEN: usize = 96;
pub const PHASE_A_TOKEN_INSTRUCTION_RECORD_V1_ENCODED_LEN: usize = 64;
pub const PHASE_A_INSTRUCTION_ACCOUNT_RECORD_V1_ENCODED_LEN: usize = 12;
pub const PHASE_A_INLINE_PUBKEY_RECORD_V1_ENCODED_LEN: usize = 32;
pub const PHASE_A_TOKEN_BALANCE_VALUE_V1_ENCODED_LEN: usize = 40;
pub const PHASE_A_TOKEN_BALANCE_ORACLE_RECORD_V1_ENCODED_LEN: usize = 112;
pub const COVERAGE_COUNTER_V1_COUNT: usize = 36;
pub const PHASE_A_COVERAGE_COUNTERS_V1_ENCODED_LEN: usize = COVERAGE_COUNTER_V1_COUNT * 8;

pub const NONE_U32: u32 = u32::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    InvalidLength {
        record: &'static str,
        expected: usize,
        actual: usize,
    },
    InvalidTag {
        field: &'static str,
        value: u64,
    },
    InvalidValue(&'static str),
    NonZeroReserved(&'static str),
    ArithmeticOverflow(&'static str),
}

impl fmt::Display for FormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength {
                record,
                expected,
                actual,
            } => write!(
                formatter,
                "{record} has {actual} bytes; expected exactly {expected}"
            ),
            Self::InvalidTag { field, value } => {
                write!(formatter, "{field} has unknown tag {value}")
            }
            Self::InvalidValue(message) => formatter.write_str(message),
            Self::NonZeroReserved(record) => {
                write!(formatter, "{record} has non-zero reserved bytes")
            }
            Self::ArithmeticOverflow(field) => write!(formatter, "{field} overflow"),
        }
    }
}

impl Error for FormatError {}

pub type FormatResult<T> = Result<T, FormatError>;

macro_rules! for_each_coverage_counter {
    ($macro:ident) => {
        $macro!(epochs_scanned);
        $macro!(blocks_scanned);
        $macro!(owned_block_fallbacks);
        $macro!(transactions_scanned);
        $macro!(transactions_selected);
        $macro!(outer_token_instructions);
        $macro!(inner_token_instructions);
        $macro!(classic_token_instructions);
        $macro!(token_2022_instructions);
        $macro!(committed_token_instructions);
        $macro!(rolled_back_token_instructions);
        $macro!(not_executed_token_instructions);
        $macro!(unknown_execution_token_instructions);
        $macro!(decoded_token_instructions);
        $macro!(unknown_top_level_tags);
        $macro!(unknown_extension_subtags);
        $macro!(malformed_token_instructions);
        $macro!(empty_instruction_data);
        $macro!(batch_instructions);
        $macro!(batch_subinstructions);
        $macro!(instruction_account_references);
        $macro!(instruction_data_bytes);
        $macro!(transactions_with_token_balances);
        $macro!(pre_token_balance_rows);
        $macro!(post_token_balance_rows);
        $macro!(paired_oracle_rows);
        $macro!(inline_pubkeys);
        $macro!(missing_metadata);
        $macro!(raw_transaction_fallbacks);
        $macro!(raw_metadata_fallbacks);
        $macro!(message_decode_failures);
        $macro!(metadata_decode_failures);
        $macro!(unresolved_pubkey_references);
        $macro!(source_gap_count);
        $macro!(unknown_balance_effects);
        $macro!(callback_validation_discards);
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum PhaseARecordKindV1 {
    Transactions = 1,
    TokenInstructions = 2,
    InstructionAccounts = 3,
    InstructionData = 4,
    InlinePubkeys = 5,
    TokenBalanceOracle = 6,
    Coverage = 7,
}

impl PhaseARecordKindV1 {
    fn from_u8(value: u8) -> FormatResult<Self> {
        match value {
            1 => Ok(Self::Transactions),
            2 => Ok(Self::TokenInstructions),
            3 => Ok(Self::InstructionAccounts),
            4 => Ok(Self::InstructionData),
            5 => Ok(Self::InlinePubkeys),
            6 => Ok(Self::TokenBalanceOracle),
            7 => Ok(Self::Coverage),
            _ => Err(FormatError::InvalidTag {
                field: "phase-A record kind",
                value: u64::from(value),
            }),
        }
    }

    pub const fn record_bytes(self) -> u32 {
        match self {
            Self::Transactions => PHASE_A_TRANSACTION_RECORD_V1_ENCODED_LEN as u32,
            Self::TokenInstructions => PHASE_A_TOKEN_INSTRUCTION_RECORD_V1_ENCODED_LEN as u32,
            Self::InstructionAccounts => PHASE_A_INSTRUCTION_ACCOUNT_RECORD_V1_ENCODED_LEN as u32,
            Self::InstructionData => 0,
            Self::InlinePubkeys => PHASE_A_INLINE_PUBKEY_RECORD_V1_ENCODED_LEN as u32,
            Self::TokenBalanceOracle => PHASE_A_TOKEN_BALANCE_ORACLE_RECORD_V1_ENCODED_LEN as u32,
            Self::Coverage => PHASE_A_COVERAGE_COUNTERS_V1_ENCODED_LEN as u32,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseAFileHeaderV1 {
    pub record_kind: PhaseARecordKindV1,
    pub epoch: u64,
    pub source_generation_digest: [u8; 32],
}

impl PhaseAFileHeaderV1 {
    pub fn encode(self) -> [u8; PHASE_A_FILE_HEADER_V1_ENCODED_LEN] {
        let mut output = [0u8; PHASE_A_FILE_HEADER_V1_ENCODED_LEN];
        output[0..8].copy_from_slice(&PHASE_A_FILE_MAGIC_V1);
        output[8..10].copy_from_slice(&PHASE_A_SCHEMA_VERSION_V1.to_le_bytes());
        output[10] = self.record_kind as u8;
        // byte 11 is flags and is zero in V1.
        output[12..16].copy_from_slice(&self.record_kind.record_bytes().to_le_bytes());
        output[16..24].copy_from_slice(&self.epoch.to_le_bytes());
        output[24..56].copy_from_slice(&self.source_generation_digest);
        // bytes 56..64 are reserved.
        output
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "phase-A file header V1",
            bytes,
            PHASE_A_FILE_HEADER_V1_ENCODED_LEN,
        )?;
        if bytes[0..8] != PHASE_A_FILE_MAGIC_V1 {
            return Err(FormatError::InvalidValue(
                "phase-A file header has the wrong magic",
            ));
        }
        let schema_version = read_u16(bytes, 8);
        if schema_version != PHASE_A_SCHEMA_VERSION_V1 {
            return Err(FormatError::InvalidTag {
                field: "phase-A schema version",
                value: u64::from(schema_version),
            });
        }
        let record_kind = PhaseARecordKindV1::from_u8(bytes[10])?;
        if bytes[11] != 0 || bytes[56..64].iter().any(|byte| *byte != 0) {
            return Err(FormatError::NonZeroReserved("phase-A file header V1"));
        }
        if read_u32(bytes, 12) != record_kind.record_bytes() {
            return Err(FormatError::InvalidValue(
                "phase-A file header record size does not match its kind",
            ));
        }
        let mut source_generation_digest = [0u8; 32];
        source_generation_digest.copy_from_slice(&bytes[24..56]);
        Ok(Self {
            record_kind,
            epoch: read_u64(bytes, 16),
            source_generation_digest,
        })
    }
}

/// A fixed-width reference used before the global public-key registry exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum PhaseAPubkeyRefV1 {
    Missing,
    /// One-based row in the admitted source epoch `registry.bin`.
    SourceRegistryId(u32),
    /// Zero-based row in this epoch shard's `inline-pubkeys.bin`.
    InlinePubkeyOrdinal(u32),
}

impl PhaseAPubkeyRefV1 {
    pub const fn is_missing(self) -> bool {
        matches!(self, Self::Missing)
    }

    pub fn validate(self) -> FormatResult<()> {
        if matches!(self, Self::SourceRegistryId(0)) {
            return Err(FormatError::InvalidValue(
                "source registry public-key ID zero is reserved",
            ));
        }
        Ok(())
    }

    pub fn encode(self) -> [u8; PHASE_A_PUBKEY_REF_V1_ENCODED_LEN] {
        let mut output = [0u8; PHASE_A_PUBKEY_REF_V1_ENCODED_LEN];
        let (tag, value) = match self {
            Self::Missing => (0, 0),
            Self::SourceRegistryId(id) => (1, id),
            Self::InlinePubkeyOrdinal(ordinal) => (2, ordinal),
        };
        output[0] = tag;
        output[4..8].copy_from_slice(&value.to_le_bytes());
        output
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "phase-A public-key reference V1",
            bytes,
            PHASE_A_PUBKEY_REF_V1_ENCODED_LEN,
        )?;
        if bytes[1..4].iter().any(|byte| *byte != 0) {
            return Err(FormatError::NonZeroReserved(
                "phase-A public-key reference V1",
            ));
        }
        let value = read_u32(bytes, 4);
        let reference = match bytes[0] {
            0 if value == 0 => Self::Missing,
            0 => {
                return Err(FormatError::InvalidValue(
                    "missing public-key reference has a non-zero value",
                ));
            }
            1 => Self::SourceRegistryId(value),
            2 => Self::InlinePubkeyOrdinal(value),
            tag => {
                return Err(FormatError::InvalidTag {
                    field: "phase-A public-key reference kind",
                    value: u64::from(tag),
                });
            }
        };
        reference.validate()?;
        Ok(reference)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceTransactionCoordinateV1 {
    pub epoch: u64,
    pub slot: u64,
    pub source_block_id: u32,
    pub tx_index: u32,
    pub source_first_signature_ordinal: u64,
    pub signature_count: u16,
}

impl SourceTransactionCoordinateV1 {
    pub fn validate(self) -> FormatResult<()> {
        if self.signature_count == 0 {
            return Err(FormatError::InvalidValue(
                "source transaction signature count must be positive",
            ));
        }
        self.source_first_signature_ordinal
            .checked_add(u64::from(self.signature_count))
            .ok_or(FormatError::ArithmeticOverflow(
                "source transaction signature range",
            ))?;
        Ok(())
    }

    pub fn encode(self) -> [u8; SOURCE_TRANSACTION_COORDINATE_V1_ENCODED_LEN] {
        let mut output = [0u8; SOURCE_TRANSACTION_COORDINATE_V1_ENCODED_LEN];
        output[0..8].copy_from_slice(&self.epoch.to_le_bytes());
        output[8..16].copy_from_slice(&self.slot.to_le_bytes());
        output[16..20].copy_from_slice(&self.source_block_id.to_le_bytes());
        output[20..24].copy_from_slice(&self.tx_index.to_le_bytes());
        output[24..32].copy_from_slice(&self.source_first_signature_ordinal.to_le_bytes());
        output[32..34].copy_from_slice(&self.signature_count.to_le_bytes());
        output
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "source transaction coordinate V1",
            bytes,
            SOURCE_TRANSACTION_COORDINATE_V1_ENCODED_LEN,
        )?;
        if bytes[34..40].iter().any(|byte| *byte != 0) {
            return Err(FormatError::NonZeroReserved(
                "source transaction coordinate V1",
            ));
        }
        let coordinate = Self {
            epoch: read_u64(bytes, 0),
            slot: read_u64(bytes, 8),
            source_block_id: read_u32(bytes, 16),
            tx_index: read_u32(bytes, 20),
            source_first_signature_ordinal: read_u64(bytes, 24),
            signature_count: read_u16(bytes, 32),
        };
        coordinate.validate()?;
        Ok(coordinate)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum PhaseATransactionStatusV1 {
    Success = 1,
    InstructionError = 2,
    OtherError = 3,
    Unknown = 4,
}

impl PhaseATransactionStatusV1 {
    fn from_u8(value: u8) -> FormatResult<Self> {
        match value {
            1 => Ok(Self::Success),
            2 => Ok(Self::InstructionError),
            3 => Ok(Self::OtherError),
            4 => Ok(Self::Unknown),
            _ => Err(FormatError::InvalidTag {
                field: "phase-A transaction status",
                value: u64::from(value),
            }),
        }
    }
}

pub const TX_PHASE_FLAG_MESSAGE_VALIDATED: u32 = 1 << 0;
pub const TX_PHASE_FLAG_HAS_METADATA: u32 = 1 << 1;
pub const TX_PHASE_FLAG_METADATA_VALIDATED: u32 = 1 << 2;
pub const TX_PHASE_FLAG_HAS_TOKEN_BALANCES: u32 = 1 << 3;
pub const TX_PHASE_FLAG_RAW_TRANSACTION_FALLBACK: u32 = 1 << 4;
pub const TX_PHASE_FLAG_RAW_METADATA_FALLBACK: u32 = 1 << 5;
pub const TX_PHASE_FLAG_HAS_LOADED_ADDRESSES: u32 = 1 << 6;
pub const TX_PHASE_FLAG_HAS_INNER_INSTRUCTIONS: u32 = 1 << 7;
pub const TX_PHASE_FLAG_CALLBACKS_VALIDATED: u32 = 1 << 8;
const TX_PHASE_FLAGS_KNOWN_MASK: u32 = (1 << 9) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseATransactionRecordV1 {
    pub coordinate: SourceTransactionCoordinateV1,
    /// Zero-based row in `transactions.bin`.
    pub transaction_ordinal: u64,
    /// Exact source Archive V2 transaction flags.
    pub source_flags: u32,
    pub phase_flags: u32,
    pub status: PhaseATransactionStatusV1,
    /// Present only for [`PhaseATransactionStatusV1::InstructionError`].
    pub failed_instruction_index: Option<u32>,
    pub first_instruction_ordinal: u64,
    pub instruction_count: u32,
    pub first_oracle_row_ordinal: u64,
    pub oracle_row_count: u32,
}

impl PhaseATransactionRecordV1 {
    pub fn validate(self) -> FormatResult<()> {
        self.coordinate.validate()?;
        if self.phase_flags & !TX_PHASE_FLAGS_KNOWN_MASK != 0 {
            return Err(FormatError::InvalidValue(
                "phase-A transaction has unknown phase flags",
            ));
        }
        match (self.status, self.failed_instruction_index) {
            (PhaseATransactionStatusV1::InstructionError, Some(_)) => {}
            (PhaseATransactionStatusV1::InstructionError, None) => {
                return Err(FormatError::InvalidValue(
                    "instruction-error transaction has no failed instruction index",
                ));
            }
            (_, Some(_)) => {
                return Err(FormatError::InvalidValue(
                    "non-instruction-error transaction has a failed instruction index",
                ));
            }
            (_, None) => {}
        }
        checked_range(
            self.first_instruction_ordinal,
            u64::from(self.instruction_count),
            "transaction instruction range",
        )?;
        checked_range(
            self.first_oracle_row_ordinal,
            u64::from(self.oracle_row_count),
            "transaction oracle-row range",
        )?;
        let has_metadata = self.phase_flags & TX_PHASE_FLAG_HAS_METADATA != 0;
        let metadata_validated = self.phase_flags & TX_PHASE_FLAG_METADATA_VALIDATED != 0;
        let has_token_balances = self.phase_flags & TX_PHASE_FLAG_HAS_TOKEN_BALANCES != 0;
        if (metadata_validated || has_token_balances || self.oracle_row_count != 0) && !has_metadata
        {
            return Err(FormatError::InvalidValue(
                "transaction metadata-dependent fields require metadata",
            ));
        }
        if self.oracle_row_count != 0 && !has_token_balances {
            return Err(FormatError::InvalidValue(
                "transaction oracle rows require the token-balance flag",
            ));
        }
        Ok(())
    }

    pub fn encode(self) -> [u8; PHASE_A_TRANSACTION_RECORD_V1_ENCODED_LEN] {
        let mut output = [0u8; PHASE_A_TRANSACTION_RECORD_V1_ENCODED_LEN];
        output[0..40].copy_from_slice(&self.coordinate.encode());
        output[40..48].copy_from_slice(&self.transaction_ordinal.to_le_bytes());
        output[48..52].copy_from_slice(&self.source_flags.to_le_bytes());
        output[52..56].copy_from_slice(&self.phase_flags.to_le_bytes());
        output[56] = self.status as u8;
        output[60..64].copy_from_slice(
            &self
                .failed_instruction_index
                .unwrap_or(NONE_U32)
                .to_le_bytes(),
        );
        output[64..72].copy_from_slice(&self.first_instruction_ordinal.to_le_bytes());
        output[72..76].copy_from_slice(&self.instruction_count.to_le_bytes());
        output[80..88].copy_from_slice(&self.first_oracle_row_ordinal.to_le_bytes());
        output[88..92].copy_from_slice(&self.oracle_row_count.to_le_bytes());
        output
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "phase-A transaction record V1",
            bytes,
            PHASE_A_TRANSACTION_RECORD_V1_ENCODED_LEN,
        )?;
        if bytes[57..60].iter().any(|byte| *byte != 0)
            || bytes[76..80].iter().any(|byte| *byte != 0)
            || bytes[92..96].iter().any(|byte| *byte != 0)
        {
            return Err(FormatError::NonZeroReserved(
                "phase-A transaction record V1",
            ));
        }
        let failed_instruction_index = match read_u32(bytes, 60) {
            NONE_U32 => None,
            value => Some(value),
        };
        let record = Self {
            coordinate: SourceTransactionCoordinateV1::decode(&bytes[0..40])?,
            transaction_ordinal: read_u64(bytes, 40),
            source_flags: read_u32(bytes, 48),
            phase_flags: read_u32(bytes, 52),
            status: PhaseATransactionStatusV1::from_u8(bytes[56])?,
            failed_instruction_index,
            first_instruction_ordinal: read_u64(bytes, 64),
            instruction_count: read_u32(bytes, 72),
            first_oracle_row_ordinal: read_u64(bytes, 80),
            oracle_row_count: read_u32(bytes, 88),
        };
        record.validate()?;
        Ok(record)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum TokenProgramKindV1 {
    SplToken = 1,
    SplToken2022 = 2,
}

impl TokenProgramKindV1 {
    fn from_u8(value: u8) -> FormatResult<Self> {
        match value {
            1 => Ok(Self::SplToken),
            2 => Ok(Self::SplToken2022),
            _ => Err(FormatError::InvalidTag {
                field: "token program kind",
                value: u64::from(value),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum InstructionDispositionV1 {
    ExecutedCommitted = 1,
    ExecutedRolledBack = 2,
    NotExecuted = 3,
    Unknown = 4,
}

impl InstructionDispositionV1 {
    fn from_u8(value: u8) -> FormatResult<Self> {
        match value {
            1 => Ok(Self::ExecutedCommitted),
            2 => Ok(Self::ExecutedRolledBack),
            3 => Ok(Self::NotExecuted),
            4 => Ok(Self::Unknown),
            _ => Err(FormatError::InvalidTag {
                field: "token instruction disposition",
                value: u64::from(value),
            }),
        }
    }
}

pub const TOKEN_IX_FLAG_INNER: u16 = 1 << 0;
pub const TOKEN_IX_FLAG_RECOGNIZED: u16 = 1 << 1;
pub const TOKEN_IX_FLAG_UNKNOWN_TOP_LEVEL: u16 = 1 << 2;
pub const TOKEN_IX_FLAG_UNKNOWN_EXTENSION: u16 = 1 << 3;
pub const TOKEN_IX_FLAG_MALFORMED: u16 = 1 << 4;
pub const TOKEN_IX_FLAG_BATCH: u16 = 1 << 5;
pub const TOKEN_IX_FLAG_BATCH_CHILDREN_VALIDATED: u16 = 1 << 6;
const TOKEN_IX_FLAGS_KNOWN_MASK: u16 = (1 << 7) - 1;
const TOKEN_IX_CLASSIFICATION_MASK: u16 = TOKEN_IX_FLAG_RECOGNIZED
    | TOKEN_IX_FLAG_UNKNOWN_TOP_LEVEL
    | TOKEN_IX_FLAG_UNKNOWN_EXTENSION
    | TOKEN_IX_FLAG_MALFORMED;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseATokenInstructionRecordV1 {
    /// Zero-based row in `token-instructions.bin`.
    pub instruction_ordinal: u64,
    pub transaction_ordinal: u64,
    pub first_account_ordinal: u64,
    pub data_offset: u64,
    pub outer_instruction_index: u32,
    pub inner_instruction_index: Option<u32>,
    pub stack_height: Option<u32>,
    pub program_id_index: u32,
    pub account_count: u32,
    pub data_length: u32,
    pub flags: u16,
    pub disposition: InstructionDispositionV1,
    pub token_program: TokenProgramKindV1,
    /// Cached first byte. It is zero when `data_length` is zero.
    pub discriminator: u8,
}

impl PhaseATokenInstructionRecordV1 {
    pub fn validate(self) -> FormatResult<()> {
        if self.flags & !TOKEN_IX_FLAGS_KNOWN_MASK != 0 {
            return Err(FormatError::InvalidValue(
                "token instruction has unknown flags",
            ));
        }
        if self.program_id_index >= 256 {
            return Err(FormatError::InvalidValue(
                "token instruction program index exceeds the message-account limit",
            ));
        }
        if self.account_count > 256 {
            return Err(FormatError::InvalidValue(
                "token instruction account count exceeds the message-account limit",
            ));
        }
        let is_inner = self.flags & TOKEN_IX_FLAG_INNER != 0;
        if is_inner != self.inner_instruction_index.is_some() {
            return Err(FormatError::InvalidValue(
                "token instruction inner flag and inner index disagree",
            ));
        }
        if self.data_length == 0 && self.discriminator != 0 {
            return Err(FormatError::InvalidValue(
                "empty token instruction has a non-zero cached discriminator",
            ));
        }
        if (self.flags & TOKEN_IX_CLASSIFICATION_MASK).count_ones() != 1 {
            return Err(FormatError::InvalidValue(
                "token instruction must have exactly one decode classification",
            ));
        }
        if self.flags & TOKEN_IX_FLAG_BATCH_CHILDREN_VALIDATED != 0
            && self.flags & TOKEN_IX_FLAG_BATCH == 0
        {
            return Err(FormatError::InvalidValue(
                "validated batch children require the batch flag",
            ));
        }
        checked_range(
            self.first_account_ordinal,
            u64::from(self.account_count),
            "token instruction account range",
        )?;
        checked_range(
            self.data_offset,
            u64::from(self.data_length),
            "token instruction data range",
        )?;
        Ok(())
    }

    pub fn encode(self) -> [u8; PHASE_A_TOKEN_INSTRUCTION_RECORD_V1_ENCODED_LEN] {
        let mut output = [0u8; PHASE_A_TOKEN_INSTRUCTION_RECORD_V1_ENCODED_LEN];
        output[0..8].copy_from_slice(&self.instruction_ordinal.to_le_bytes());
        output[8..16].copy_from_slice(&self.transaction_ordinal.to_le_bytes());
        output[16..24].copy_from_slice(&self.first_account_ordinal.to_le_bytes());
        output[24..32].copy_from_slice(&self.data_offset.to_le_bytes());
        output[32..36].copy_from_slice(&self.outer_instruction_index.to_le_bytes());
        output[36..40].copy_from_slice(
            &self
                .inner_instruction_index
                .unwrap_or(NONE_U32)
                .to_le_bytes(),
        );
        output[40..44].copy_from_slice(&self.stack_height.unwrap_or(NONE_U32).to_le_bytes());
        output[44..48].copy_from_slice(&self.program_id_index.to_le_bytes());
        output[48..52].copy_from_slice(&self.account_count.to_le_bytes());
        output[52..56].copy_from_slice(&self.data_length.to_le_bytes());
        output[56..58].copy_from_slice(&self.flags.to_le_bytes());
        output[58] = self.disposition as u8;
        output[59] = self.token_program as u8;
        output[60] = self.discriminator;
        output
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "phase-A token instruction record V1",
            bytes,
            PHASE_A_TOKEN_INSTRUCTION_RECORD_V1_ENCODED_LEN,
        )?;
        if bytes[61..64].iter().any(|byte| *byte != 0) {
            return Err(FormatError::NonZeroReserved(
                "phase-A token instruction record V1",
            ));
        }
        let record = Self {
            instruction_ordinal: read_u64(bytes, 0),
            transaction_ordinal: read_u64(bytes, 8),
            first_account_ordinal: read_u64(bytes, 16),
            data_offset: read_u64(bytes, 24),
            outer_instruction_index: read_u32(bytes, 32),
            inner_instruction_index: optional_u32(read_u32(bytes, 36)),
            stack_height: optional_u32(read_u32(bytes, 40)),
            program_id_index: read_u32(bytes, 44),
            account_count: read_u32(bytes, 48),
            data_length: read_u32(bytes, 52),
            flags: read_u16(bytes, 56),
            disposition: InstructionDispositionV1::from_u8(bytes[58])?,
            token_program: TokenProgramKindV1::from_u8(bytes[59])?,
            discriminator: bytes[60],
        };
        record.validate()?;
        Ok(record)
    }
}

/// One ordered account in a token instruction's exact account-index list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseAInstructionAccountRecordV1 {
    pub pubkey: PhaseAPubkeyRefV1,
    pub instruction_account_index: u16,
    pub message_account_index: u16,
}

impl PhaseAInstructionAccountRecordV1 {
    pub fn validate(self) -> FormatResult<()> {
        self.pubkey.validate()?;
        if self.pubkey.is_missing() {
            return Err(FormatError::InvalidValue(
                "instruction account public key cannot be missing",
            ));
        }
        if self.instruction_account_index >= 256 || self.message_account_index >= 256 {
            return Err(FormatError::InvalidValue(
                "instruction account index exceeds the message-account limit",
            ));
        }
        Ok(())
    }

    pub fn encode(self) -> [u8; PHASE_A_INSTRUCTION_ACCOUNT_RECORD_V1_ENCODED_LEN] {
        let mut output = [0u8; PHASE_A_INSTRUCTION_ACCOUNT_RECORD_V1_ENCODED_LEN];
        output[0..8].copy_from_slice(&self.pubkey.encode());
        output[8..10].copy_from_slice(&self.instruction_account_index.to_le_bytes());
        output[10..12].copy_from_slice(&self.message_account_index.to_le_bytes());
        output
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "phase-A instruction account record V1",
            bytes,
            PHASE_A_INSTRUCTION_ACCOUNT_RECORD_V1_ENCODED_LEN,
        )?;
        let record = Self {
            pubkey: PhaseAPubkeyRefV1::decode(&bytes[0..8])?,
            instruction_account_index: read_u16(bytes, 8),
            message_account_index: read_u16(bytes, 10),
        };
        record.validate()?;
        Ok(record)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseAInlinePubkeyRecordV1(pub [u8; 32]);

impl PhaseAInlinePubkeyRecordV1 {
    pub const fn encode(self) -> [u8; PHASE_A_INLINE_PUBKEY_RECORD_V1_ENCODED_LEN] {
        self.0
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "phase-A inline public-key record V1",
            bytes,
            PHASE_A_INLINE_PUBKEY_RECORD_V1_ENCODED_LEN,
        )?;
        let mut key = [0u8; 32];
        key.copy_from_slice(bytes);
        Ok(Self(key))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseATokenBalanceValueV1 {
    pub mint: PhaseAPubkeyRefV1,
    pub owner: PhaseAPubkeyRefV1,
    pub program_id: PhaseAPubkeyRefV1,
    pub amount: u64,
    pub decimals: u8,
}

impl PhaseATokenBalanceValueV1 {
    pub const fn absent() -> Self {
        Self {
            mint: PhaseAPubkeyRefV1::Missing,
            owner: PhaseAPubkeyRefV1::Missing,
            program_id: PhaseAPubkeyRefV1::Missing,
            amount: 0,
            decimals: 0,
        }
    }

    pub fn validate_present(self) -> FormatResult<()> {
        self.mint.validate()?;
        self.owner.validate()?;
        self.program_id.validate()?;
        if self.mint.is_missing() {
            return Err(FormatError::InvalidValue(
                "present token balance has no mint",
            ));
        }
        Ok(())
    }

    pub fn validate_absent(self) -> FormatResult<()> {
        if self != Self::absent() {
            return Err(FormatError::InvalidValue(
                "absent token-balance side contains data",
            ));
        }
        Ok(())
    }

    pub fn encode(self) -> [u8; PHASE_A_TOKEN_BALANCE_VALUE_V1_ENCODED_LEN] {
        let mut output = [0u8; PHASE_A_TOKEN_BALANCE_VALUE_V1_ENCODED_LEN];
        output[0..8].copy_from_slice(&self.mint.encode());
        output[8..16].copy_from_slice(&self.owner.encode());
        output[16..24].copy_from_slice(&self.program_id.encode());
        output[24..32].copy_from_slice(&self.amount.to_le_bytes());
        output[32] = self.decimals;
        output
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "phase-A token balance value V1",
            bytes,
            PHASE_A_TOKEN_BALANCE_VALUE_V1_ENCODED_LEN,
        )?;
        if bytes[33..40].iter().any(|byte| *byte != 0) {
            return Err(FormatError::NonZeroReserved(
                "phase-A token balance value V1",
            ));
        }
        Ok(Self {
            mint: PhaseAPubkeyRefV1::decode(&bytes[0..8])?,
            owner: PhaseAPubkeyRefV1::decode(&bytes[8..16])?,
            program_id: PhaseAPubkeyRefV1::decode(&bytes[16..24])?,
            amount: read_u64(bytes, 24),
            decimals: bytes[32],
        })
    }
}

pub const TOKEN_BALANCE_ORACLE_FLAG_PRE_PRESENT: u16 = 1 << 0;
pub const TOKEN_BALANCE_ORACLE_FLAG_POST_PRESENT: u16 = 1 << 1;
const TOKEN_BALANCE_ORACLE_FLAGS_KNOWN_MASK: u16 =
    TOKEN_BALANCE_ORACLE_FLAG_PRE_PRESENT | TOKEN_BALANCE_ORACLE_FLAG_POST_PRESENT;

/// The union of one transaction's pre/post token-balance rows, paired by
/// message account index. Pre and post identity fields remain separate so
/// creation, close, and address reuse are not normalized away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseATokenBalanceOracleRecordV1 {
    /// Zero-based row in `token-balance-oracle.bin`.
    pub oracle_row_ordinal: u64,
    pub transaction_ordinal: u64,
    pub account: PhaseAPubkeyRefV1,
    pub message_account_index: u16,
    pub flags: u16,
    pub pre: PhaseATokenBalanceValueV1,
    pub post: PhaseATokenBalanceValueV1,
}

impl PhaseATokenBalanceOracleRecordV1 {
    pub fn validate(self) -> FormatResult<()> {
        self.account.validate()?;
        if self.account.is_missing() {
            return Err(FormatError::InvalidValue(
                "token-balance oracle account cannot be missing",
            ));
        }
        if self.message_account_index >= 256 {
            return Err(FormatError::InvalidValue(
                "token-balance oracle account index exceeds the message-account limit",
            ));
        }
        if self.flags & !TOKEN_BALANCE_ORACLE_FLAGS_KNOWN_MASK != 0 {
            return Err(FormatError::InvalidValue(
                "token-balance oracle row has unknown flags",
            ));
        }
        let pre_present = self.flags & TOKEN_BALANCE_ORACLE_FLAG_PRE_PRESENT != 0;
        let post_present = self.flags & TOKEN_BALANCE_ORACLE_FLAG_POST_PRESENT != 0;
        if !pre_present && !post_present {
            return Err(FormatError::InvalidValue(
                "token-balance oracle row has neither a pre nor post value",
            ));
        }
        if pre_present {
            self.pre.validate_present()?;
        } else {
            self.pre.validate_absent()?;
        }
        if post_present {
            self.post.validate_present()?;
        } else {
            self.post.validate_absent()?;
        }
        Ok(())
    }

    pub fn encode(self) -> [u8; PHASE_A_TOKEN_BALANCE_ORACLE_RECORD_V1_ENCODED_LEN] {
        let mut output = [0u8; PHASE_A_TOKEN_BALANCE_ORACLE_RECORD_V1_ENCODED_LEN];
        output[0..8].copy_from_slice(&self.oracle_row_ordinal.to_le_bytes());
        output[8..16].copy_from_slice(&self.transaction_ordinal.to_le_bytes());
        output[16..24].copy_from_slice(&self.account.encode());
        output[24..26].copy_from_slice(&self.message_account_index.to_le_bytes());
        output[26..28].copy_from_slice(&self.flags.to_le_bytes());
        output[32..72].copy_from_slice(&self.pre.encode());
        output[72..112].copy_from_slice(&self.post.encode());
        output
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "phase-A token-balance oracle record V1",
            bytes,
            PHASE_A_TOKEN_BALANCE_ORACLE_RECORD_V1_ENCODED_LEN,
        )?;
        if bytes[28..32].iter().any(|byte| *byte != 0) {
            return Err(FormatError::NonZeroReserved(
                "phase-A token-balance oracle record V1",
            ));
        }
        let record = Self {
            oracle_row_ordinal: read_u64(bytes, 0),
            transaction_ordinal: read_u64(bytes, 8),
            account: PhaseAPubkeyRefV1::decode(&bytes[16..24])?,
            message_account_index: read_u16(bytes, 24),
            flags: read_u16(bytes, 26),
            pre: PhaseATokenBalanceValueV1::decode(&bytes[32..72])?,
            post: PhaseATokenBalanceValueV1::decode(&bytes[72..112])?,
        };
        record.validate()?;
        Ok(record)
    }
}

/// Coverage is part of the proof result, not diagnostic text. A strict proof
/// is valid only when every blocker is zero.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseACoverageCountersV1 {
    pub epochs_scanned: u64,
    pub blocks_scanned: u64,
    pub owned_block_fallbacks: u64,
    pub transactions_scanned: u64,
    pub transactions_selected: u64,
    pub outer_token_instructions: u64,
    pub inner_token_instructions: u64,
    pub classic_token_instructions: u64,
    pub token_2022_instructions: u64,
    pub committed_token_instructions: u64,
    pub rolled_back_token_instructions: u64,
    pub not_executed_token_instructions: u64,
    pub unknown_execution_token_instructions: u64,
    pub decoded_token_instructions: u64,
    pub unknown_top_level_tags: u64,
    pub unknown_extension_subtags: u64,
    pub malformed_token_instructions: u64,
    pub empty_instruction_data: u64,
    pub batch_instructions: u64,
    pub batch_subinstructions: u64,
    pub instruction_account_references: u64,
    pub instruction_data_bytes: u64,
    pub transactions_with_token_balances: u64,
    pub pre_token_balance_rows: u64,
    pub post_token_balance_rows: u64,
    pub paired_oracle_rows: u64,
    pub inline_pubkeys: u64,
    pub missing_metadata: u64,
    pub raw_transaction_fallbacks: u64,
    pub raw_metadata_fallbacks: u64,
    pub message_decode_failures: u64,
    pub metadata_decode_failures: u64,
    pub unresolved_pubkey_references: u64,
    pub source_gap_count: u64,
    pub unknown_balance_effects: u64,
    pub callback_validation_discards: u64,
}

impl PhaseACoverageCountersV1 {
    pub fn token_instruction_count(self) -> FormatResult<u64> {
        self.outer_token_instructions
            .checked_add(self.inner_token_instructions)
            .ok_or(FormatError::ArithmeticOverflow(
                "total token instruction count",
            ))
    }

    pub fn strict_blocker_count(self) -> FormatResult<u64> {
        let blockers = [
            self.unknown_execution_token_instructions,
            self.unknown_top_level_tags,
            self.unknown_extension_subtags,
            self.malformed_token_instructions,
            self.missing_metadata,
            self.raw_transaction_fallbacks,
            self.raw_metadata_fallbacks,
            self.message_decode_failures,
            self.metadata_decode_failures,
            self.unresolved_pubkey_references,
            self.source_gap_count,
            self.unknown_balance_effects,
            self.callback_validation_discards,
        ];
        blockers.into_iter().try_fold(0u64, |total, value| {
            total
                .checked_add(value)
                .ok_or(FormatError::ArithmeticOverflow(
                    "strict coverage blocker count",
                ))
        })
    }

    pub fn is_strictly_complete(self) -> bool {
        self.validate().is_ok() && self.strict_blocker_count() == Ok(0)
    }

    pub fn validate(self) -> FormatResult<()> {
        if self.transactions_selected > self.transactions_scanned {
            return Err(FormatError::InvalidValue(
                "selected transaction count exceeds scanned transaction count",
            ));
        }
        if self.transactions_with_token_balances > self.transactions_selected {
            return Err(FormatError::InvalidValue(
                "token-balance transaction count exceeds selected transaction count",
            ));
        }
        let token_instructions = self.token_instruction_count()?;
        let program_total = self
            .classic_token_instructions
            .checked_add(self.token_2022_instructions)
            .ok_or(FormatError::ArithmeticOverflow(
                "token instruction program count",
            ))?;
        if program_total != token_instructions {
            return Err(FormatError::InvalidValue(
                "token instruction program counts do not cover every instruction",
            ));
        }
        let disposition_total = [
            self.committed_token_instructions,
            self.rolled_back_token_instructions,
            self.not_executed_token_instructions,
            self.unknown_execution_token_instructions,
        ]
        .into_iter()
        .try_fold(0u64, |total, value| {
            total
                .checked_add(value)
                .ok_or(FormatError::ArithmeticOverflow(
                    "token instruction disposition count",
                ))
        })?;
        if disposition_total != token_instructions {
            return Err(FormatError::InvalidValue(
                "token instruction dispositions do not cover every instruction",
            ));
        }
        let decode_total = [
            self.decoded_token_instructions,
            self.unknown_top_level_tags,
            self.unknown_extension_subtags,
            self.malformed_token_instructions,
        ]
        .into_iter()
        .try_fold(0u64, |total, value| {
            total
                .checked_add(value)
                .ok_or(FormatError::ArithmeticOverflow(
                    "token instruction decode-class count",
                ))
        })?;
        if decode_total != token_instructions {
            return Err(FormatError::InvalidValue(
                "token instruction decode classes do not cover every instruction",
            ));
        }
        if self.batch_instructions > self.decoded_token_instructions {
            return Err(FormatError::InvalidValue(
                "batch instruction count exceeds decoded instruction count",
            ));
        }
        let oracle_source_rows = self
            .pre_token_balance_rows
            .checked_add(self.post_token_balance_rows)
            .ok_or(FormatError::ArithmeticOverflow(
                "metadata token-balance row count",
            ))?;
        if self.paired_oracle_rows > oracle_source_rows
            || self.paired_oracle_rows
                < self
                    .pre_token_balance_rows
                    .max(self.post_token_balance_rows)
        {
            return Err(FormatError::InvalidValue(
                "paired oracle count is not the union of pre/post rows",
            ));
        }
        Ok(())
    }

    pub fn checked_add_assign(&mut self, other: Self) -> FormatResult<()> {
        macro_rules! add_counter {
            ($field:ident) => {
                self.$field = self
                    .$field
                    .checked_add(other.$field)
                    .ok_or(FormatError::ArithmeticOverflow(stringify!($field)))?;
            };
        }
        for_each_coverage_counter!(add_counter);
        Ok(())
    }

    pub fn encode(self) -> [u8; PHASE_A_COVERAGE_COUNTERS_V1_ENCODED_LEN] {
        let mut output = [0u8; PHASE_A_COVERAGE_COUNTERS_V1_ENCODED_LEN];
        for (index, value) in self.as_array().into_iter().enumerate() {
            let offset = index * 8;
            output[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }
        output
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<Self> {
        require_length(
            "phase-A coverage counters V1",
            bytes,
            PHASE_A_COVERAGE_COUNTERS_V1_ENCODED_LEN,
        )?;
        let mut values = [0u64; COVERAGE_COUNTER_V1_COUNT];
        for (index, value) in values.iter_mut().enumerate() {
            *value = read_u64(bytes, index * 8);
        }
        let counters = Self::from_array(values);
        counters.validate()?;
        Ok(counters)
    }

    fn as_array(self) -> [u64; COVERAGE_COUNTER_V1_COUNT] {
        [
            self.epochs_scanned,
            self.blocks_scanned,
            self.owned_block_fallbacks,
            self.transactions_scanned,
            self.transactions_selected,
            self.outer_token_instructions,
            self.inner_token_instructions,
            self.classic_token_instructions,
            self.token_2022_instructions,
            self.committed_token_instructions,
            self.rolled_back_token_instructions,
            self.not_executed_token_instructions,
            self.unknown_execution_token_instructions,
            self.decoded_token_instructions,
            self.unknown_top_level_tags,
            self.unknown_extension_subtags,
            self.malformed_token_instructions,
            self.empty_instruction_data,
            self.batch_instructions,
            self.batch_subinstructions,
            self.instruction_account_references,
            self.instruction_data_bytes,
            self.transactions_with_token_balances,
            self.pre_token_balance_rows,
            self.post_token_balance_rows,
            self.paired_oracle_rows,
            self.inline_pubkeys,
            self.missing_metadata,
            self.raw_transaction_fallbacks,
            self.raw_metadata_fallbacks,
            self.message_decode_failures,
            self.metadata_decode_failures,
            self.unresolved_pubkey_references,
            self.source_gap_count,
            self.unknown_balance_effects,
            self.callback_validation_discards,
        ]
    }

    fn from_array(values: [u64; COVERAGE_COUNTER_V1_COUNT]) -> Self {
        let mut values = values.into_iter();
        Self {
            epochs_scanned: values.next().expect("fixed counter count"),
            blocks_scanned: values.next().expect("fixed counter count"),
            owned_block_fallbacks: values.next().expect("fixed counter count"),
            transactions_scanned: values.next().expect("fixed counter count"),
            transactions_selected: values.next().expect("fixed counter count"),
            outer_token_instructions: values.next().expect("fixed counter count"),
            inner_token_instructions: values.next().expect("fixed counter count"),
            classic_token_instructions: values.next().expect("fixed counter count"),
            token_2022_instructions: values.next().expect("fixed counter count"),
            committed_token_instructions: values.next().expect("fixed counter count"),
            rolled_back_token_instructions: values.next().expect("fixed counter count"),
            not_executed_token_instructions: values.next().expect("fixed counter count"),
            unknown_execution_token_instructions: values.next().expect("fixed counter count"),
            decoded_token_instructions: values.next().expect("fixed counter count"),
            unknown_top_level_tags: values.next().expect("fixed counter count"),
            unknown_extension_subtags: values.next().expect("fixed counter count"),
            malformed_token_instructions: values.next().expect("fixed counter count"),
            empty_instruction_data: values.next().expect("fixed counter count"),
            batch_instructions: values.next().expect("fixed counter count"),
            batch_subinstructions: values.next().expect("fixed counter count"),
            instruction_account_references: values.next().expect("fixed counter count"),
            instruction_data_bytes: values.next().expect("fixed counter count"),
            transactions_with_token_balances: values.next().expect("fixed counter count"),
            pre_token_balance_rows: values.next().expect("fixed counter count"),
            post_token_balance_rows: values.next().expect("fixed counter count"),
            paired_oracle_rows: values.next().expect("fixed counter count"),
            inline_pubkeys: values.next().expect("fixed counter count"),
            missing_metadata: values.next().expect("fixed counter count"),
            raw_transaction_fallbacks: values.next().expect("fixed counter count"),
            raw_metadata_fallbacks: values.next().expect("fixed counter count"),
            message_decode_failures: values.next().expect("fixed counter count"),
            metadata_decode_failures: values.next().expect("fixed counter count"),
            unresolved_pubkey_references: values.next().expect("fixed counter count"),
            source_gap_count: values.next().expect("fixed counter count"),
            unknown_balance_effects: values.next().expect("fixed counter count"),
            callback_validation_discards: values.next().expect("fixed counter count"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseASourceWireProfileV1 {
    PostUnknownInstructionFallbacksV1,
    PreUnknownInstructionFallbacksV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PhaseASourceAuthorityV1 {
    PublishedManifest { manifest_sha256: String },
    TrustedLocal { identity_sha256: String },
}

impl PhaseASourceAuthorityV1 {
    fn validate(&self) -> FormatResult<()> {
        match self {
            Self::PublishedManifest { manifest_sha256 } => {
                validate_sha256(manifest_sha256, "source manifest SHA-256")
            }
            Self::TrustedLocal { identity_sha256 } => {
                validate_sha256(identity_sha256, "trusted-local identity SHA-256")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseASourceBindingV1 {
    pub cluster_id: String,
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub generation_digest_sha256: String,
    pub wire_profile: PhaseASourceWireProfileV1,
    pub authority: PhaseASourceAuthorityV1,
    pub registry_entries: u32,
    pub source_signature_count: u64,
}

impl PhaseASourceBindingV1 {
    pub fn validate(&self) -> FormatResult<()> {
        if self.cluster_id.trim().is_empty() {
            return Err(FormatError::InvalidValue("source cluster ID is empty"));
        }
        if self.slots_per_epoch == 0 {
            return Err(FormatError::InvalidValue(
                "source slots-per-epoch must be positive",
            ));
        }
        if self.registry_entries == 0 {
            return Err(FormatError::InvalidValue(
                "source registry entry count must be positive",
            ));
        }
        validate_sha256(
            &self.generation_digest_sha256,
            "source generation digest SHA-256",
        )?;
        self.authority.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseAFileManifestV1 {
    pub path: String,
    pub record_kind: PhaseARecordKindV1,
    /// Zero only for the raw instruction-data byte lane.
    pub record_bytes: u32,
    /// Zero for the raw instruction-data byte lane.
    pub record_count: u64,
    /// Bytes after the fixed file header.
    pub payload_bytes: u64,
    /// Header plus payload.
    pub file_bytes: u64,
    pub sha256: String,
}

impl PhaseAFileManifestV1 {
    pub fn validate(&self) -> FormatResult<()> {
        validate_relative_path(&self.path)?;
        validate_sha256(&self.sha256, "phase-A artifact SHA-256")?;
        if self.record_bytes != self.record_kind.record_bytes() {
            return Err(FormatError::InvalidValue(
                "artifact record size does not match its record kind",
            ));
        }
        if self.record_kind == PhaseARecordKindV1::InstructionData {
            if self.record_count != 0 {
                return Err(FormatError::InvalidValue(
                    "raw instruction-data lane must have zero record count",
                ));
            }
        } else {
            let expected_payload = self
                .record_count
                .checked_mul(u64::from(self.record_bytes))
                .ok_or(FormatError::ArithmeticOverflow(
                    "artifact fixed-record payload size",
                ))?;
            if self.payload_bytes != expected_payload {
                return Err(FormatError::InvalidValue(
                    "artifact payload size does not match its fixed records",
                ));
            }
        }
        let expected_file_bytes = (PHASE_A_FILE_HEADER_V1_ENCODED_LEN as u64)
            .checked_add(self.payload_bytes)
            .ok_or(FormatError::ArithmeticOverflow("artifact file size"))?;
        if self.file_bytes != expected_file_bytes {
            return Err(FormatError::InvalidValue(
                "artifact file size does not equal header plus payload",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseAArtifactsV1 {
    pub transactions: PhaseAFileManifestV1,
    pub token_instructions: PhaseAFileManifestV1,
    pub instruction_accounts: PhaseAFileManifestV1,
    pub instruction_data: PhaseAFileManifestV1,
    pub inline_pubkeys: PhaseAFileManifestV1,
    pub token_balance_oracle: PhaseAFileManifestV1,
    pub coverage: PhaseAFileManifestV1,
}

impl PhaseAArtifactsV1 {
    pub fn validate(&self, counters: &PhaseACoverageCountersV1) -> FormatResult<()> {
        let expected = [
            (
                &self.transactions,
                PHASE_A_TRANSACTIONS_FILE,
                PhaseARecordKindV1::Transactions,
            ),
            (
                &self.token_instructions,
                PHASE_A_TOKEN_INSTRUCTIONS_FILE,
                PhaseARecordKindV1::TokenInstructions,
            ),
            (
                &self.instruction_accounts,
                PHASE_A_INSTRUCTION_ACCOUNTS_FILE,
                PhaseARecordKindV1::InstructionAccounts,
            ),
            (
                &self.instruction_data,
                PHASE_A_INSTRUCTION_DATA_FILE,
                PhaseARecordKindV1::InstructionData,
            ),
            (
                &self.inline_pubkeys,
                PHASE_A_INLINE_PUBKEYS_FILE,
                PhaseARecordKindV1::InlinePubkeys,
            ),
            (
                &self.token_balance_oracle,
                PHASE_A_TOKEN_BALANCE_ORACLE_FILE,
                PhaseARecordKindV1::TokenBalanceOracle,
            ),
            (
                &self.coverage,
                PHASE_A_COVERAGE_FILE,
                PhaseARecordKindV1::Coverage,
            ),
        ];
        for (file, path, kind) in expected {
            file.validate()?;
            if file.path != path || file.record_kind != kind {
                return Err(FormatError::InvalidValue(
                    "phase-A artifact has the wrong canonical path or record kind",
                ));
            }
        }
        let paths = [
            &self.transactions.path,
            &self.token_instructions.path,
            &self.instruction_accounts.path,
            &self.instruction_data.path,
            &self.inline_pubkeys.path,
            &self.token_balance_oracle.path,
            &self.coverage.path,
        ];
        for (index, path) in paths.iter().enumerate() {
            if paths[..index].contains(path) {
                return Err(FormatError::InvalidValue(
                    "phase-A artifact paths are not unique",
                ));
            }
        }
        if self.transactions.record_count != counters.transactions_selected {
            return Err(FormatError::InvalidValue(
                "transaction artifact count differs from coverage",
            ));
        }
        if self.token_instructions.record_count != counters.token_instruction_count()? {
            return Err(FormatError::InvalidValue(
                "token-instruction artifact count differs from coverage",
            ));
        }
        if self.instruction_accounts.record_count != counters.instruction_account_references {
            return Err(FormatError::InvalidValue(
                "instruction-account artifact count differs from coverage",
            ));
        }
        if self.instruction_data.payload_bytes != counters.instruction_data_bytes {
            return Err(FormatError::InvalidValue(
                "instruction-data artifact size differs from coverage",
            ));
        }
        if self.inline_pubkeys.record_count != counters.inline_pubkeys {
            return Err(FormatError::InvalidValue(
                "inline-public-key artifact count differs from coverage",
            ));
        }
        if self.token_balance_oracle.record_count != counters.paired_oracle_rows {
            return Err(FormatError::InvalidValue(
                "token-balance oracle artifact count differs from coverage",
            ));
        }
        if self.coverage.record_count != 1 {
            return Err(FormatError::InvalidValue(
                "coverage artifact must contain exactly one record",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseAArtifactKindV1 {
    EpochShard,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseAEpochManifestV1 {
    pub schema_version: u16,
    pub artifact_kind: PhaseAArtifactKindV1,
    /// A consumer must reject the manifest until this is true.
    pub complete: bool,
    pub source: PhaseASourceBindingV1,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub artifacts: PhaseAArtifactsV1,
    pub coverage: PhaseACoverageCountersV1,
}

impl PhaseAEpochManifestV1 {
    pub fn validate(&self) -> FormatResult<()> {
        if self.schema_version != PHASE_A_SCHEMA_VERSION_V1 {
            return Err(FormatError::InvalidTag {
                field: "phase-A manifest schema version",
                value: u64::from(self.schema_version),
            });
        }
        if !self.complete {
            return Err(FormatError::InvalidValue(
                "phase-A manifest is not complete",
            ));
        }
        self.source.validate()?;
        self.coverage.validate()?;
        if self.coverage.epochs_scanned != 1 {
            return Err(FormatError::InvalidValue(
                "phase-A epoch manifest must cover exactly one epoch",
            ));
        }
        match (
            self.coverage.blocks_scanned,
            self.first_slot,
            self.last_slot,
        ) {
            (0, None, None) => {}
            (0, _, _) => {
                return Err(FormatError::InvalidValue(
                    "empty epoch shard has a slot range",
                ));
            }
            (_, Some(first), Some(last)) if first <= last => {}
            _ => {
                return Err(FormatError::InvalidValue(
                    "non-empty epoch shard has an invalid slot range",
                ));
            }
        }
        self.artifacts.validate(&self.coverage)
    }
}

fn require_length(record: &'static str, bytes: &[u8], expected: usize) -> FormatResult<()> {
    if bytes.len() != expected {
        return Err(FormatError::InvalidLength {
            record,
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn optional_u32(value: u32) -> Option<u32> {
    (value != NONE_U32).then_some(value)
}

fn checked_range(start: u64, length: u64, field: &'static str) -> FormatResult<()> {
    start
        .checked_add(length)
        .ok_or(FormatError::ArithmeticOverflow(field))?;
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("fixed slice"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed slice"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed slice"))
}

fn validate_sha256(value: &str, field: &'static str) -> FormatResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(FormatError::InvalidValue(field));
    }
    Ok(())
}

fn validate_relative_path(value: &str) -> FormatResult<()> {
    if value.is_empty()
        || value.starts_with('/')
        || value.contains('\\')
        || value
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(FormatError::InvalidValue(
            "artifact path is not a canonical relative path",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn coordinate() -> SourceTransactionCoordinateV1 {
        SourceTransactionCoordinateV1 {
            epoch: 900,
            slot: 388_800_123,
            source_block_id: 42,
            tx_index: 17,
            source_first_signature_ordinal: 9_000_000_000,
            signature_count: 2,
        }
    }

    fn coverage() -> PhaseACoverageCountersV1 {
        PhaseACoverageCountersV1 {
            epochs_scanned: 1,
            blocks_scanned: 1,
            transactions_scanned: 3,
            transactions_selected: 1,
            outer_token_instructions: 1,
            classic_token_instructions: 1,
            committed_token_instructions: 1,
            decoded_token_instructions: 1,
            instruction_account_references: 3,
            instruction_data_bytes: 9,
            transactions_with_token_balances: 1,
            pre_token_balance_rows: 1,
            post_token_balance_rows: 1,
            paired_oracle_rows: 1,
            inline_pubkeys: 1,
            ..Default::default()
        }
    }

    fn file(
        path: &str,
        record_kind: PhaseARecordKindV1,
        record_count: u64,
        payload_bytes: u64,
    ) -> PhaseAFileManifestV1 {
        PhaseAFileManifestV1 {
            path: path.to_owned(),
            record_kind,
            record_bytes: record_kind.record_bytes(),
            record_count,
            payload_bytes,
            file_bytes: PHASE_A_FILE_HEADER_V1_ENCODED_LEN as u64 + payload_bytes,
            sha256: HASH.to_owned(),
        }
    }

    fn artifacts(counters: PhaseACoverageCountersV1) -> PhaseAArtifactsV1 {
        PhaseAArtifactsV1 {
            transactions: file(
                PHASE_A_TRANSACTIONS_FILE,
                PhaseARecordKindV1::Transactions,
                counters.transactions_selected,
                counters.transactions_selected * PHASE_A_TRANSACTION_RECORD_V1_ENCODED_LEN as u64,
            ),
            token_instructions: file(
                PHASE_A_TOKEN_INSTRUCTIONS_FILE,
                PhaseARecordKindV1::TokenInstructions,
                counters.token_instruction_count().unwrap(),
                counters.token_instruction_count().unwrap()
                    * PHASE_A_TOKEN_INSTRUCTION_RECORD_V1_ENCODED_LEN as u64,
            ),
            instruction_accounts: file(
                PHASE_A_INSTRUCTION_ACCOUNTS_FILE,
                PhaseARecordKindV1::InstructionAccounts,
                counters.instruction_account_references,
                counters.instruction_account_references
                    * PHASE_A_INSTRUCTION_ACCOUNT_RECORD_V1_ENCODED_LEN as u64,
            ),
            instruction_data: file(
                PHASE_A_INSTRUCTION_DATA_FILE,
                PhaseARecordKindV1::InstructionData,
                0,
                counters.instruction_data_bytes,
            ),
            inline_pubkeys: file(
                PHASE_A_INLINE_PUBKEYS_FILE,
                PhaseARecordKindV1::InlinePubkeys,
                counters.inline_pubkeys,
                counters.inline_pubkeys * PHASE_A_INLINE_PUBKEY_RECORD_V1_ENCODED_LEN as u64,
            ),
            token_balance_oracle: file(
                PHASE_A_TOKEN_BALANCE_ORACLE_FILE,
                PhaseARecordKindV1::TokenBalanceOracle,
                counters.paired_oracle_rows,
                counters.paired_oracle_rows
                    * PHASE_A_TOKEN_BALANCE_ORACLE_RECORD_V1_ENCODED_LEN as u64,
            ),
            coverage: file(
                PHASE_A_COVERAGE_FILE,
                PhaseARecordKindV1::Coverage,
                1,
                PHASE_A_COVERAGE_COUNTERS_V1_ENCODED_LEN as u64,
            ),
        }
    }

    #[test]
    fn file_header_round_trip_and_rejects_reserved_bytes() {
        let header = PhaseAFileHeaderV1 {
            record_kind: PhaseARecordKindV1::TokenInstructions,
            epoch: 900,
            source_generation_digest: [7; 32],
        };
        let encoded = header.encode();
        assert_eq!(PhaseAFileHeaderV1::decode(&encoded), Ok(header));

        let mut invalid = encoded;
        invalid[63] = 1;
        assert!(matches!(
            PhaseAFileHeaderV1::decode(&invalid),
            Err(FormatError::NonZeroReserved(_))
        ));
    }

    #[test]
    fn pubkey_reference_round_trips_all_wire_forms() {
        for reference in [
            PhaseAPubkeyRefV1::Missing,
            PhaseAPubkeyRefV1::SourceRegistryId(55),
            PhaseAPubkeyRefV1::InlinePubkeyOrdinal(0),
            PhaseAPubkeyRefV1::InlinePubkeyOrdinal(u32::MAX),
        ] {
            assert_eq!(
                PhaseAPubkeyRefV1::decode(&reference.encode()),
                Ok(reference)
            );
        }
        assert!(PhaseAPubkeyRefV1::SourceRegistryId(0).validate().is_err());
    }

    #[test]
    fn coordinate_round_trip_is_exact_and_little_endian() {
        let coordinate = coordinate();
        let encoded = coordinate.encode();
        assert_eq!(&encoded[0..8], &900u64.to_le_bytes());
        assert_eq!(
            SourceTransactionCoordinateV1::decode(&encoded),
            Ok(coordinate)
        );
    }

    #[test]
    fn transaction_record_round_trip_and_status_invariants() {
        let record = PhaseATransactionRecordV1 {
            coordinate: coordinate(),
            transaction_ordinal: 12,
            source_flags: 0xa5a5,
            phase_flags: TX_PHASE_FLAG_MESSAGE_VALIDATED
                | TX_PHASE_FLAG_HAS_METADATA
                | TX_PHASE_FLAG_METADATA_VALIDATED
                | TX_PHASE_FLAG_HAS_TOKEN_BALANCES
                | TX_PHASE_FLAG_CALLBACKS_VALIDATED,
            status: PhaseATransactionStatusV1::Success,
            failed_instruction_index: None,
            first_instruction_ordinal: 20,
            instruction_count: 2,
            first_oracle_row_ordinal: 9,
            oracle_row_count: 1,
        };
        assert_eq!(
            PhaseATransactionRecordV1::decode(&record.encode()),
            Ok(record)
        );

        let mut invalid = record;
        invalid.status = PhaseATransactionStatusV1::InstructionError;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn instruction_record_round_trip_preserves_batch_discriminator_255() {
        let record = PhaseATokenInstructionRecordV1 {
            instruction_ordinal: 4,
            transaction_ordinal: 12,
            first_account_ordinal: 99,
            data_offset: 1234,
            outer_instruction_index: 7,
            inner_instruction_index: Some(2),
            stack_height: Some(3),
            program_id_index: 5,
            account_count: 4,
            data_length: 18,
            flags: TOKEN_IX_FLAG_INNER
                | TOKEN_IX_FLAG_RECOGNIZED
                | TOKEN_IX_FLAG_BATCH
                | TOKEN_IX_FLAG_BATCH_CHILDREN_VALIDATED,
            disposition: InstructionDispositionV1::ExecutedCommitted,
            token_program: TokenProgramKindV1::SplToken2022,
            discriminator: 255,
        };
        assert_eq!(
            PhaseATokenInstructionRecordV1::decode(&record.encode()),
            Ok(record)
        );

        let mut invalid = record;
        invalid.inner_instruction_index = None;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn instruction_account_and_inline_key_round_trip() {
        let account = PhaseAInstructionAccountRecordV1 {
            pubkey: PhaseAPubkeyRefV1::InlinePubkeyOrdinal(7),
            instruction_account_index: 2,
            message_account_index: 250,
        };
        assert_eq!(
            PhaseAInstructionAccountRecordV1::decode(&account.encode()),
            Ok(account)
        );

        let key = PhaseAInlinePubkeyRecordV1([42; 32]);
        assert_eq!(PhaseAInlinePubkeyRecordV1::decode(&key.encode()), Ok(key));
    }

    #[test]
    fn oracle_round_trip_preserves_missing_pre_side() {
        let post = PhaseATokenBalanceValueV1 {
            mint: PhaseAPubkeyRefV1::SourceRegistryId(4),
            owner: PhaseAPubkeyRefV1::Missing,
            program_id: PhaseAPubkeyRefV1::SourceRegistryId(2),
            amount: 99,
            decimals: 6,
        };
        let record = PhaseATokenBalanceOracleRecordV1 {
            oracle_row_ordinal: 3,
            transaction_ordinal: 12,
            account: PhaseAPubkeyRefV1::InlinePubkeyOrdinal(0),
            message_account_index: 7,
            flags: TOKEN_BALANCE_ORACLE_FLAG_POST_PRESENT,
            pre: PhaseATokenBalanceValueV1::absent(),
            post,
        };
        assert_eq!(
            PhaseATokenBalanceOracleRecordV1::decode(&record.encode()),
            Ok(record)
        );

        let mut invalid = record;
        invalid.pre.amount = 1;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn coverage_round_trip_checks_partition_invariants() {
        let counters = coverage();
        assert_eq!(
            PhaseACoverageCountersV1::decode(&counters.encode()),
            Ok(counters)
        );
        assert!(counters.is_strictly_complete());

        let mut invalid = counters;
        invalid.token_2022_instructions = 1;
        assert!(invalid.validate().is_err());

        let mut blocked = counters;
        blocked.unknown_balance_effects = 1;
        assert!(!blocked.is_strictly_complete());
    }

    #[test]
    fn coverage_checked_merge_detects_overflow() {
        let mut left = PhaseACoverageCountersV1 {
            blocks_scanned: u64::MAX,
            ..Default::default()
        };
        let right = PhaseACoverageCountersV1 {
            blocks_scanned: 1,
            ..Default::default()
        };
        assert!(matches!(
            left.checked_add_assign(right),
            Err(FormatError::ArithmeticOverflow("blocks_scanned"))
        ));
    }

    #[test]
    fn manifest_json_round_trip_and_cross_file_invariants() {
        let counters = coverage();
        let manifest = PhaseAEpochManifestV1 {
            schema_version: PHASE_A_SCHEMA_VERSION_V1,
            artifact_kind: PhaseAArtifactKindV1::EpochShard,
            complete: true,
            source: PhaseASourceBindingV1 {
                cluster_id: "mainnet-beta".to_owned(),
                epoch: 900,
                slots_per_epoch: 432_000,
                generation_digest_sha256: HASH.to_owned(),
                wire_profile: PhaseASourceWireProfileV1::PostUnknownInstructionFallbacksV1,
                authority: PhaseASourceAuthorityV1::PublishedManifest {
                    manifest_sha256: HASH.to_owned(),
                },
                registry_entries: 10,
                source_signature_count: 100,
            },
            first_slot: Some(388_800_000),
            last_slot: Some(388_800_000),
            artifacts: artifacts(counters),
            coverage: counters,
        };
        manifest.validate().unwrap();
        let json = serde_json::to_vec_pretty(&manifest).unwrap();
        let decoded: PhaseAEpochManifestV1 = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded, manifest);
        decoded.validate().unwrap();

        let mut wrong_count = decoded;
        wrong_count.artifacts.transactions.record_count += 1;
        wrong_count.artifacts.transactions.payload_bytes +=
            PHASE_A_TRANSACTION_RECORD_V1_ENCODED_LEN as u64;
        wrong_count.artifacts.transactions.file_bytes +=
            PHASE_A_TRANSACTION_RECORD_V1_ENCODED_LEN as u64;
        assert!(wrong_count.validate().is_err());
    }

    #[test]
    fn decoders_reject_trailing_bytes_and_unknown_tags() {
        let mut coordinate = coordinate().encode().to_vec();
        coordinate.push(0);
        assert!(matches!(
            SourceTransactionCoordinateV1::decode(&coordinate),
            Err(FormatError::InvalidLength { .. })
        ));

        let mut reference = PhaseAPubkeyRefV1::Missing.encode();
        reference[0] = 99;
        assert!(matches!(
            PhaseAPubkeyRefV1::decode(&reference),
            Err(FormatError::InvalidTag { .. })
        ));
    }
}
