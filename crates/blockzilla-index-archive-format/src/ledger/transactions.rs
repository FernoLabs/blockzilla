//! `ledger/transactions.wincode`: complete replay input and effect indexes.
//!
//! A block is one Wincode [`TransactionBlock`]. Canonical transaction rows are
//! stored once in `transaction_rows`. Effect payloads stay in six separate
//! runtime objects. One byte per transaction says which dense effect streams
//! contain a record. Small chunk directories give direct range reads without
//! six absolute offset-and-length pairs on every transaction.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::wincode::{self as wire, ArchiveWincodeConfig};

pub const PATH: &str = "ledger/transactions.wincode";
pub const SCHEMA: u16 = 1;

pub const EFFECT_KIND_COUNT: usize = 6;
pub const ROW_RESTART_INTERVAL: u32 = 32;
pub const EFFECT_CHUNK_TRANSACTIONS: u32 = 256;
pub const MAX_TRANSACTIONS_PER_BLOCK: u32 = 1 << 20;
pub const MAX_ACCOUNTS_PER_TRANSACTION: usize = 1 << 16;
pub const MAX_INSTRUCTIONS_PER_TRANSACTION: usize = 1 << 16;
pub const MAX_ACCOUNTS_PER_INSTRUCTION: usize = 1 << 16;
pub const MAX_INSTRUCTION_DATA_LEN: usize = 16 << 20;

const CHUNK_RAW_FLAG: u32 = 1 << 31;
const CHUNK_LENGTH_MASK: u32 = CHUNK_RAW_FLAG - 1;

/// Stable order of the six transaction-scoped effect files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EffectKind {
    InnerInstructions = 0,
    Outcome = 1,
    Balances = 2,
    TokenBalances = 3,
    Logs = 4,
    Rewards = 5,
}

impl EffectKind {
    pub const ALL: [Self; EFFECT_KIND_COUNT] = [
        Self::InnerInstructions,
        Self::Outcome,
        Self::Balances,
        Self::TokenBalances,
        Self::Logs,
        Self::Rewards,
    ];

    pub const fn index(self) -> usize {
        self as usize
    }

    const fn presence_bit(self) -> u8 {
        match self {
            Self::InnerInstructions => 0,
            Self::Outcome => 1 << 3,
            Self::Balances => 1 << 4,
            Self::TokenBalances => 1 << 5,
            Self::Logs => 1 << 6,
            Self::Rewards => 1 << 7,
        }
    }
}

/// Exact CPI capture state encoded in bits 0 through 2 of [`EffectState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CpiState {
    Unavailable = 0,
    NotRecorded = 1,
    SourceEmpty = 2,
    SourcePresent = 3,
    BackfillEmpty = 4,
    BackfillPresent = 5,
}

impl CpiState {
    fn from_code(code: u8) -> Result<Self, TransactionError> {
        Ok(match code {
            0 => Self::Unavailable,
            1 => Self::NotRecorded,
            2 => Self::SourceEmpty,
            3 => Self::SourcePresent,
            4 => Self::BackfillEmpty,
            5 => Self::BackfillPresent,
            other => return Err(TransactionError::UnknownCpiState(other)),
        })
    }

    pub const fn has_record(self) -> bool {
        matches!(self, Self::SourcePresent | Self::BackfillPresent)
    }
}

/// One transaction's complete effect index.
///
/// Bits 0..=2 are [`CpiState`]. Bits 3..=7 state whether the outcome, balances,
/// token balances, logs, and reward streams contain a dense record. The
/// Outcome bit also proves that the source runtime-metadata envelope existed.
/// Therefore, with Outcome present, a clear TokenBalances or Rewards bit is
/// the one canonical encoding of a known-empty vector. A clear Logs bit stays
/// unavailable because the source records log availability independently.
/// `TransactionBlock::effect_states` is the fixed one-byte prefix of each
/// logical transaction row. It is held in a parallel plane for rank queries;
/// it is not copied into `transaction_rows`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[repr(transparent)]
pub struct EffectState(u8);

impl EffectState {
    const CPI_MASK: u8 = 0b111;

    pub fn new(cpi: CpiState) -> Self {
        Self(cpi as u8)
    }

    pub fn from_byte(byte: u8) -> Result<Self, TransactionError> {
        let state = Self(byte);
        state.validate()?;
        Ok(state)
    }

    pub const fn as_byte(self) -> u8 {
        self.0
    }

    pub fn cpi(self) -> Result<CpiState, TransactionError> {
        CpiState::from_code(self.0 & Self::CPI_MASK)
    }

    /// Validate cross-bit rules for one logical transaction row.
    pub fn validate(self) -> Result<(), TransactionError> {
        self.cpi()?;
        if self.0 & EffectKind::Outcome.presence_bit() != 0
            && self.0 & EffectKind::Balances.presence_bit() == 0
        {
            return Err(TransactionError::MetadataEnvelopeWithoutBalances);
        }
        Ok(())
    }

    pub fn set_present(&mut self, kind: EffectKind, present: bool) {
        let bit = kind.presence_bit();
        assert!(bit != 0, "CPI presence is part of CpiState");
        if present {
            self.0 |= bit;
        } else {
            self.0 &= !bit;
        }
    }

    pub fn has_record(self, kind: EffectKind) -> Result<bool, TransactionError> {
        if kind == EffectKind::InnerInstructions {
            return Ok(self.cpi()?.has_record());
        }
        Ok(self.0 & kind.presence_bit() != 0)
    }

    /// Whether a missing dense record is the canonical known-empty value.
    ///
    /// CPI has explicit empty states. Token balances and transaction rewards
    /// are always vectors in a present source metadata envelope, so Outcome
    /// plus a clear vector bit proves empty. Logs do not use this rule.
    pub fn omitted_record_is_known_empty(self, kind: EffectKind) -> Result<bool, TransactionError> {
        self.validate()?;
        match kind {
            EffectKind::InnerInstructions => Ok(matches!(
                self.cpi()?,
                CpiState::SourceEmpty | CpiState::BackfillEmpty
            )),
            EffectKind::TokenBalances | EffectKind::Rewards => {
                Ok(self.has_record(EffectKind::Outcome)? && !self.has_record(kind)?)
            }
            EffectKind::Outcome | EffectKind::Balances | EffectKind::Logs => Ok(false),
        }
    }
}

/// Stored bytes for one independently addressable effect chunk.
///
/// Zero means no frame. Bit 31 means raw Wincode bytes. Otherwise the value is
/// the byte length of a zstd frame with content size and checksum enabled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[repr(transparent)]
pub struct ChunkFrame(u32);

impl ChunkFrame {
    pub const EMPTY: Self = Self(0);

    pub fn raw(stored_len: u32) -> Result<Self, TransactionError> {
        if stored_len == 0 || stored_len > CHUNK_LENGTH_MASK {
            return Err(TransactionError::InvalidChunkLength(stored_len));
        }
        Ok(Self(stored_len | CHUNK_RAW_FLAG))
    }

    pub fn zstd(stored_len: u32) -> Result<Self, TransactionError> {
        if stored_len == 0 || stored_len > CHUNK_LENGTH_MASK {
            return Err(TransactionError::InvalidChunkLength(stored_len));
        }
        Ok(Self(stored_len))
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn is_raw(self) -> bool {
        self.0 & CHUNK_RAW_FLAG != 0
    }

    pub const fn stored_len(self) -> u32 {
        self.0 & CHUNK_LENGTH_MASK
    }
}

/// Range index for one effect file over this block.
#[derive(Debug, Clone, Default, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct EffectFileIndex {
    /// Absolute file offset of the first non-empty chunk, or zero when every
    /// chunk is empty. Chunks are physically contiguous in chunk order.
    pub first_chunk_offset: u64,
    pub chunks: Vec<ChunkFrame>,
}

impl EffectFileIndex {
    pub fn chunk_offset(&self, index: usize) -> Option<u64> {
        let frame = *self.chunks.get(index)?;
        if frame.is_empty() {
            return None;
        }
        let before = self.chunks.get(..index)?;
        before
            .iter()
            .try_fold(self.first_chunk_offset, |offset, frame| {
                offset.checked_add(u64::from(frame.stored_len()))
            })
    }
}

/// Checkpoint for bounded point access to concatenated transaction rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct RowRestart {
    /// Byte offset in [`TransactionBlock::transaction_rows`].
    pub row_byte_offset: u32,
    /// Signature count before this restart, relative to the block catalog's
    /// `first_signature` value.
    pub signature_delta: u32,
}

/// One block's transaction rows and every transaction-scoped effect index.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct TransactionBlock {
    pub effect_states: Vec<EffectState>,
    pub row_restarts: Vec<RowRestart>,
    pub effect_files: [EffectFileIndex; EFFECT_KIND_COUNT],
    /// Concatenated canonical Wincode [`Transaction`] values. The catalog owns
    /// the transaction count; this arena does not repeat it.
    pub transaction_rows: Vec<u8>,
}

/// The small, offset-bearing prefix of one [`TransactionBlock`].
///
/// `transaction_rows` is the final Wincode field in `TransactionBlock`. This
/// split lets a converter encode and compress that large arena on a worker,
/// after which ordered commit only has to encode this prefix and the arena's
/// canonical vector length. Concatenating the decoded prefix and arena bytes
/// produces the exact same Wincode byte string as [`encode_block`].
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct TransactionBlockHeader {
    pub effect_states: Vec<EffectState>,
    pub row_restarts: Vec<RowRestart>,
    pub effect_files: [EffectFileIndex; EFFECT_KIND_COUNT],
}

impl TransactionBlock {
    pub fn validate(&self, transaction_count: u32) -> Result<(), TransactionError> {
        self.header().validate(transaction_count)?;
        validate_transaction_rows(
            &self.transaction_rows,
            transaction_count,
            &self.row_restarts,
        )
    }

    pub fn header(&self) -> TransactionBlockHeader {
        TransactionBlockHeader {
            effect_states: self.effect_states.clone(),
            row_restarts: self.row_restarts.clone(),
            effect_files: self.effect_files.clone(),
        }
    }

    pub fn into_parts(self) -> (TransactionBlockHeader, Vec<u8>) {
        (
            TransactionBlockHeader {
                effect_states: self.effect_states,
                row_restarts: self.row_restarts,
                effect_files: self.effect_files,
            },
            self.transaction_rows,
        )
    }

    pub fn from_parts(header: TransactionBlockHeader, transaction_rows: Vec<u8>) -> Self {
        Self {
            effect_states: header.effect_states,
            row_restarts: header.row_restarts,
            effect_files: header.effect_files,
            transaction_rows,
        }
    }
}

impl TransactionBlockHeader {
    /// Validate every block-local invariant that does not inspect row bytes.
    pub fn validate(&self, transaction_count: u32) -> Result<(), TransactionError> {
        if transaction_count > MAX_TRANSACTIONS_PER_BLOCK {
            return Err(TransactionError::TooManyTransactions(transaction_count));
        }
        if self.effect_states.len() != transaction_count as usize {
            return Err(TransactionError::EffectStateCount {
                actual: self.effect_states.len(),
                expected: transaction_count,
            });
        }
        for state in &self.effect_states {
            EffectState::from_byte(state.as_byte())?;
        }

        let expected_restarts = restart_count(transaction_count, ROW_RESTART_INTERVAL);
        if self.row_restarts.len() != expected_restarts {
            return Err(TransactionError::RestartCount {
                actual: self.row_restarts.len(),
                expected: expected_restarts,
            });
        }
        if let Some(first) = self.row_restarts.first()
            && (first.row_byte_offset != 0 || first.signature_delta != 0)
        {
            return Err(TransactionError::FirstRestartNotZero);
        }

        let expected_chunks = restart_count(transaction_count, EFFECT_CHUNK_TRANSACTIONS);
        for (kind, index) in EffectKind::ALL.into_iter().zip(&self.effect_files) {
            if index.chunks.len() != expected_chunks {
                return Err(TransactionError::ChunkCount {
                    kind,
                    actual: index.chunks.len(),
                    expected: expected_chunks,
                });
            }
            let has_bytes = index.chunks.iter().any(|frame| !frame.is_empty());
            if has_bytes != (index.first_chunk_offset != 0) {
                return Err(TransactionError::ChunkBaseDisagrees { kind });
            }
            index.chunks.iter().try_fold(0_u64, |total, frame| {
                total
                    .checked_add(u64::from(frame.stored_len()))
                    .ok_or(TransactionError::ChunkLengthOverflow)
            })?;
            for (chunk_index, frame) in index.chunks.iter().enumerate() {
                let start = chunk_index * EFFECT_CHUNK_TRANSACTIONS as usize;
                let end = self
                    .effect_states
                    .len()
                    .min(start + EFFECT_CHUNK_TRANSACTIONS as usize);
                let has_records =
                    self.effect_states[start..end]
                        .iter()
                        .try_fold(false, |present, state| {
                            state
                                .has_record(kind)
                                .map(|has_record| present || has_record)
                        })?;
                if has_records == frame.is_empty() {
                    return Err(TransactionError::ChunkPresenceDisagrees {
                        kind,
                        chunk: chunk_index,
                    });
                }
            }
        }
        Ok(())
    }

    pub fn effect_rank(
        &self,
        transaction_index: u32,
        kind: EffectKind,
    ) -> Result<Option<u32>, TransactionError> {
        effect_rank(&self.effect_states, transaction_index, kind)
    }

    /// Total dense records physically present in one effect object.
    pub fn effect_record_count(&self, kind: EffectKind) -> Result<u64, TransactionError> {
        effect_record_count(&self.effect_states, kind)
    }
}

impl TransactionBlock {
    pub fn effect_rank(
        &self,
        transaction_index: u32,
        kind: EffectKind,
    ) -> Result<Option<u32>, TransactionError> {
        effect_rank(&self.effect_states, transaction_index, kind)
    }

    /// Total dense records physically present in one effect object.
    pub fn effect_record_count(&self, kind: EffectKind) -> Result<u64, TransactionError> {
        effect_record_count(&self.effect_states, kind)
    }
}

fn effect_rank(
    effect_states: &[EffectState],
    transaction_index: u32,
    kind: EffectKind,
) -> Result<Option<u32>, TransactionError> {
    let state = *effect_states
        .get(transaction_index as usize)
        .ok_or(TransactionError::TransactionOutsideBlock(transaction_index))?;
    if !state.has_record(kind)? {
        return Ok(None);
    }
    let chunk_start = transaction_index / EFFECT_CHUNK_TRANSACTIONS * EFFECT_CHUNK_TRANSACTIONS;
    let mut rank = 0_u32;
    for state in &effect_states[chunk_start as usize..transaction_index as usize] {
        rank += u32::from(state.has_record(kind)?);
    }
    Ok(Some(rank))
}

fn effect_record_count(
    effect_states: &[EffectState],
    kind: EffectKind,
) -> Result<u64, TransactionError> {
    effect_states.iter().try_fold(0_u64, |count, state| {
        state
            .has_record(kind)
            .map(|present| count + u64::from(present))
    })
}

const fn restart_count(count: u32, interval: u32) -> usize {
    count.div_ceil(interval) as usize
}

/// Three signed message-header counts. They are also the sole owner of the
/// transaction signature count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed: u8,
    pub num_readonly_unsigned: u8,
}

/// Namespace for one canonical recent-blockhash reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum HashOwner {
    NonPoh,
    /// Final entry hash of catalog block `ordinal`.
    PohBlockFinal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct HashRef {
    pub owner: HashOwner,
    pub ordinal: u64,
}

/// One 1-based reference into `dictionary/pubkeys.pages`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SchemaRead, SchemaWrite)]
#[repr(transparent)]
pub struct PubkeyId(pub u32);

impl PubkeyId {
    pub fn new(value: u32) -> Result<Self, TransactionError> {
        if value == 0 {
            return Err(TransactionError::ReservedPubkeyId);
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct Transaction {
    pub header: MessageHeader,
    pub recent_blockhash: HashRef,
    pub message: Message,
}

/// Message version and all version-specific replay input.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum Message {
    Legacy {
        static_accounts: Vec<PubkeyId>,
        instructions: Vec<Instruction>,
    },
    V0 {
        static_accounts: Vec<PubkeyId>,
        loaded_addresses: LoadedAddresses,
        lookups: Vec<AddressTableLookup>,
        instructions: Vec<Instruction>,
    },
}

impl Message {
    pub fn static_accounts(&self) -> &[PubkeyId] {
        match self {
            Self::Legacy {
                static_accounts, ..
            }
            | Self::V0 {
                static_accounts, ..
            } => static_accounts,
        }
    }

    pub fn instructions(&self) -> &[Instruction] {
        match self {
            Self::Legacy { instructions, .. } | Self::V0 { instructions, .. } => instructions,
        }
    }
}

/// Exact V0 loaded-address coverage. Empty vectors remain complete empty
/// results; [`Self::Unavailable`] does not invent that result.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum LoadedAddresses {
    Source {
        writable: Vec<PubkeyId>,
        readonly: Vec<PubkeyId>,
    },
    Backfilled {
        writable: Vec<PubkeyId>,
        readonly: Vec<PubkeyId>,
    },
    Unavailable,
}

/// Signed V0 address-table lookup descriptor.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct AddressTableLookup {
    pub table_id: PubkeyId,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

/// Top-level instruction with its only copy of the raw data bytes.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct Instruction {
    pub program_position: u32,
    pub account_positions: Vec<u32>,
    pub data: Vec<u8>,
}

impl Transaction {
    pub fn validate(&self) -> Result<(), TransactionError> {
        let static_accounts = self.message.static_accounts();
        if self.header.num_required_signatures == 0 {
            return Err(TransactionError::NoRequiredSignatures);
        }
        if self.header.num_readonly_signed > self.header.num_required_signatures {
            return Err(TransactionError::ReadonlySignedExceedsSigners);
        }
        if usize::from(self.header.num_required_signatures) > static_accounts.len() {
            return Err(TransactionError::SignersExceedStaticAccounts);
        }
        let unsigned_accounts =
            static_accounts.len() - usize::from(self.header.num_required_signatures);
        if usize::from(self.header.num_readonly_unsigned) > unsigned_accounts {
            return Err(TransactionError::ReadonlyUnsignedExceedsUnsignedAccounts);
        }
        if static_accounts.len() > MAX_ACCOUNTS_PER_TRANSACTION {
            return Err(TransactionError::TooManyAccounts(static_accounts.len()));
        }
        validate_pubkeys(static_accounts)?;

        let mut resolved_count = static_accounts.len();
        if let Message::V0 {
            loaded_addresses,
            lookups,
            ..
        } = &self.message
        {
            let mut expected_writable = 0_usize;
            let mut expected_readonly = 0_usize;
            for lookup in lookups {
                if lookup.table_id.0 == 0 {
                    return Err(TransactionError::ReservedPubkeyId);
                }
                expected_writable = expected_writable
                    .checked_add(lookup.writable_indexes.len())
                    .ok_or(TransactionError::TooManyAccounts(usize::MAX))?;
                expected_readonly = expected_readonly
                    .checked_add(lookup.readonly_indexes.len())
                    .ok_or(TransactionError::TooManyAccounts(usize::MAX))?;
            }
            match loaded_addresses {
                LoadedAddresses::Source { writable, readonly }
                | LoadedAddresses::Backfilled { writable, readonly } => {
                    validate_pubkeys(writable)?;
                    validate_pubkeys(readonly)?;
                    if writable.len() != expected_writable || readonly.len() != expected_readonly {
                        return Err(TransactionError::LoadedAddressCountDisagrees {
                            expected_writable,
                            actual_writable: writable.len(),
                            expected_readonly,
                            actual_readonly: readonly.len(),
                        });
                    }
                }
                LoadedAddresses::Unavailable => {}
            }
            // Lookup descriptors still define the resolved message width when
            // the actual loaded pubkeys are unavailable. This keeps every
            // instruction position check exact without inventing pubkeys.
            resolved_count = resolved_count
                .checked_add(expected_writable)
                .and_then(|count| count.checked_add(expected_readonly))
                .ok_or(TransactionError::TooManyAccounts(usize::MAX))?;
        }
        if resolved_count > MAX_ACCOUNTS_PER_TRANSACTION {
            return Err(TransactionError::TooManyAccounts(resolved_count));
        }

        let instructions = self.message.instructions();
        if instructions.len() > MAX_INSTRUCTIONS_PER_TRANSACTION {
            return Err(TransactionError::TooManyInstructions(instructions.len()));
        }
        for instruction in instructions {
            if instruction.account_positions.len() > MAX_ACCOUNTS_PER_INSTRUCTION {
                return Err(TransactionError::TooManyInstructionAccounts(
                    instruction.account_positions.len(),
                ));
            }
            if instruction.data.len() > MAX_INSTRUCTION_DATA_LEN {
                return Err(TransactionError::InstructionDataTooLong(
                    instruction.data.len(),
                ));
            }
            if instruction.program_position as usize >= resolved_count
                || instruction
                    .account_positions
                    .iter()
                    .any(|position| *position as usize >= resolved_count)
            {
                return Err(TransactionError::InstructionAccountOutsideMessage);
            }
        }
        Ok(())
    }
}

fn validate_pubkeys(pubkeys: &[PubkeyId]) -> Result<(), TransactionError> {
    if pubkeys.iter().any(|id| id.0 == 0) {
        return Err(TransactionError::ReservedPubkeyId);
    }
    Ok(())
}

pub fn encode_block(block: &TransactionBlock) -> wincode::WriteResult<Vec<u8>> {
    wire::encode(block)
}

/// Encode the exact Wincode prefix before `TransactionBlock::transaction_rows`.
///
/// Wincode encodes a structure as its fields in declaration order, and its
/// canonical `Vec` length is an integer-encoded `u64`. Therefore the returned
/// bytes followed by exactly `transaction_rows_len` row bytes are byte-for-byte
/// identical to [`encode_block`] for the corresponding complete block.
pub fn encode_block_prefix(
    header: &TransactionBlockHeader,
    transaction_rows_len: usize,
) -> Result<Vec<u8>, TransactionError> {
    let rows_len = u32::try_from(transaction_rows_len)
        .map_err(|_| TransactionError::TransactionRowsTooLong(transaction_rows_len))?;
    let mut bytes = wire::encode(header)?;
    wincode::config::serialize_into(
        &mut bytes,
        &u64::from(rows_len),
        crate::wincode::archive_wincode_config(),
    )?;
    Ok(bytes)
}

/// Assemble the current logical Wincode block bytes from independently encoded
/// header and row-arena parts.
pub fn encode_block_from_parts(
    header: &TransactionBlockHeader,
    transaction_rows: &[u8],
) -> Result<Vec<u8>, TransactionError> {
    let mut bytes = encode_block_prefix(header, transaction_rows.len())?;
    bytes.extend_from_slice(transaction_rows);
    Ok(bytes)
}

pub fn decode_block(
    bytes: &[u8],
    transaction_count: u32,
) -> Result<TransactionBlock, TransactionError> {
    let block: TransactionBlock = wire::decode_exact(bytes)?;
    block.validate(transaction_count)?;
    Ok(block)
}

pub fn append_transaction(
    rows: &mut Vec<u8>,
    transaction: &Transaction,
) -> Result<(), TransactionError> {
    transaction.validate()?;
    wincode::config::serialize_into(rows, transaction, crate::wincode::archive_wincode_config())?;
    Ok(())
}

pub fn decode_transactions(
    bytes: &[u8],
    transaction_count: u32,
) -> Result<Vec<Transaction>, TransactionError> {
    let mut remaining = bytes;
    let mut transactions = Vec::with_capacity(transaction_count as usize);
    for _ in 0..transaction_count {
        let transaction =
            <Transaction as SchemaRead<'_, ArchiveWincodeConfig>>::get(&mut remaining)?;
        transaction.validate()?;
        transactions.push(transaction);
    }
    if !remaining.is_empty() {
        return Err(TransactionError::TrailingTransactionBytes(remaining.len()));
    }
    Ok(transactions)
}

fn validate_transaction_rows(
    bytes: &[u8],
    transaction_count: u32,
    row_restarts: &[RowRestart],
) -> Result<(), TransactionError> {
    let mut remaining = bytes;
    let mut signature_delta = 0_u32;
    for transaction_index in 0..transaction_count {
        if transaction_index % ROW_RESTART_INTERVAL == 0 {
            let restart = row_restarts
                .get((transaction_index / ROW_RESTART_INTERVAL) as usize)
                .ok_or(TransactionError::RestartCount {
                    actual: row_restarts.len(),
                    expected: restart_count(transaction_count, ROW_RESTART_INTERVAL),
                })?;
            let actual_offset = u32::try_from(bytes.len() - remaining.len())
                .map_err(|_| TransactionError::TransactionRowsTooLong(bytes.len()))?;
            if restart.row_byte_offset != actual_offset
                || restart.signature_delta != signature_delta
            {
                return Err(TransactionError::RestartDisagrees {
                    transaction_index,
                    expected_offset: actual_offset,
                    actual_offset: restart.row_byte_offset,
                    expected_signature_delta: signature_delta,
                    actual_signature_delta: restart.signature_delta,
                });
            }
        }
        let transaction =
            <Transaction as SchemaRead<'_, ArchiveWincodeConfig>>::get(&mut remaining)?;
        transaction.validate()?;
        signature_delta = signature_delta
            .checked_add(u32::from(transaction.header.num_required_signatures))
            .ok_or(TransactionError::SignatureCountOverflow)?;
    }
    if !remaining.is_empty() {
        return Err(TransactionError::TrailingTransactionBytes(remaining.len()));
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("transaction Wincode: {0}")]
    WincodeRead(#[from] wincode::ReadError),
    #[error("transaction Wincode: {0}")]
    WincodeWrite(#[from] wincode::WriteError),
    #[error("unknown CPI state {0}")]
    UnknownCpiState(u8),
    #[error("the Outcome metadata envelope requires a Balances record")]
    MetadataEnvelopeWithoutBalances,
    #[error("chunk length {0} is zero or uses the codec bit")]
    InvalidChunkLength(u32),
    #[error("block has {0} transactions, above the decode guard")]
    TooManyTransactions(u32),
    #[error("effect-state count {actual} does not match transaction count {expected}")]
    EffectStateCount { actual: usize, expected: u32 },
    #[error("row restart count {actual} does not match expected count {expected}")]
    RestartCount { actual: usize, expected: usize },
    #[error("the first row restart is not zero")]
    FirstRestartNotZero,
    #[error("row restart {0} is outside the transaction arena")]
    RestartOutsideRows(u32),
    #[error("row restart offsets are not strictly ascending")]
    RestartsNotAscending,
    #[error(
        "restart for transaction {transaction_index} is ({actual_offset}, {actual_signature_delta}), expected ({expected_offset}, {expected_signature_delta})"
    )]
    RestartDisagrees {
        transaction_index: u32,
        expected_offset: u32,
        actual_offset: u32,
        expected_signature_delta: u32,
        actual_signature_delta: u32,
    },
    #[error("transaction row arena has {0} bytes and cannot use u32 offsets")]
    TransactionRowsTooLong(usize),
    #[error("block signature count overflows u32")]
    SignatureCountOverflow,
    #[error("{kind:?} chunk count {actual} does not match expected count {expected}")]
    ChunkCount {
        kind: EffectKind,
        actual: usize,
        expected: usize,
    },
    #[error("{kind:?} chunk base disagrees with its non-empty chunks")]
    ChunkBaseDisagrees { kind: EffectKind },
    #[error("{kind:?} chunk {chunk} presence disagrees with EffectState popcount")]
    ChunkPresenceDisagrees { kind: EffectKind, chunk: usize },
    #[error("effect chunk lengths overflow u64")]
    ChunkLengthOverflow,
    #[error("transaction index {0} is outside the block")]
    TransactionOutsideBlock(u32),
    #[error("pubkey ID zero is reserved")]
    ReservedPubkeyId,
    #[error("a transaction must require at least one signature")]
    NoRequiredSignatures,
    #[error("readonly signed count exceeds required signatures")]
    ReadonlySignedExceedsSigners,
    #[error("required signatures exceed static accounts")]
    SignersExceedStaticAccounts,
    #[error("readonly unsigned count exceeds unsigned static accounts")]
    ReadonlyUnsignedExceedsUnsignedAccounts,
    #[error(
        "loaded-address counts are writable {actual_writable}/{expected_writable} and readonly {actual_readonly}/{expected_readonly}"
    )]
    LoadedAddressCountDisagrees {
        expected_writable: usize,
        actual_writable: usize,
        expected_readonly: usize,
        actual_readonly: usize,
    },
    #[error("transaction has {0} accounts, above the decode guard")]
    TooManyAccounts(usize),
    #[error("transaction has {0} instructions, above the decode guard")]
    TooManyInstructions(usize),
    #[error("instruction has {0} accounts, above the decode guard")]
    TooManyInstructionAccounts(usize),
    #[error("instruction data has {0} bytes, above the decode guard")]
    InstructionDataTooLong(usize),
    #[error("instruction account position is outside the resolved message accounts")]
    InstructionAccountOutsideMessage,
    #[error("transaction arena has {0} trailing bytes")]
    TrailingTransactionBytes(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction() -> Transaction {
        Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 1,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::PohBlockFinal,
                ordinal: 300,
            },
            message: Message::V0 {
                static_accounts: vec![PubkeyId(1), PubkeyId(130)],
                loaded_addresses: LoadedAddresses::Source {
                    writable: vec![PubkeyId(2)],
                    readonly: Vec::new(),
                },
                lookups: vec![AddressTableLookup {
                    table_id: PubkeyId(9),
                    writable_indexes: vec![1],
                    readonly_indexes: Vec::new(),
                }],
                instructions: vec![Instruction {
                    program_position: 1,
                    account_positions: vec![0, 2],
                    data: vec![0xaa],
                }],
            },
        }
    }

    #[test]
    fn transaction_golden_bytes_freeze_field_order_and_tags() {
        let bytes = wire::encode(&transaction()).unwrap();
        assert_eq!(
            bytes,
            [
                1, 0, 1, 1, 0xac, 2, 1, 2, 1, 0x82, 1, 0, 1, 2, 0, 1, 9, 1, 1, 0, 1, 1, 2, 0, 2, 1,
                0xaa,
            ]
        );
        let decoded: Transaction = wire::decode_exact(&bytes).unwrap();
        assert_eq!(decoded, transaction());
        decoded.validate().unwrap();
    }

    #[test]
    fn one_byte_effect_state_preserves_absent_empty_and_backfill() {
        let mut state = EffectState::new(CpiState::BackfillEmpty);
        state.set_present(EffectKind::Logs, true);
        assert_eq!(state.as_byte(), 0b0100_0100);
        assert!(!state.has_record(EffectKind::InnerInstructions).unwrap());
        assert!(state.has_record(EffectKind::Logs).unwrap());
        assert!(
            state
                .omitted_record_is_known_empty(EffectKind::InnerInstructions)
                .unwrap()
        );
        assert!(
            !state
                .omitted_record_is_known_empty(EffectKind::Logs)
                .unwrap()
        );
        assert_eq!(wire::encode(&state).unwrap(), [0b0100_0100]);
    }

    #[test]
    fn metadata_envelope_owns_known_empty_token_and_reward_vectors() {
        let mut state = EffectState::new(CpiState::NotRecorded);
        state.set_present(EffectKind::Outcome, true);
        state.set_present(EffectKind::Balances, true);
        assert!(
            state
                .omitted_record_is_known_empty(EffectKind::TokenBalances)
                .unwrap()
        );
        assert!(
            state
                .omitted_record_is_known_empty(EffectKind::Rewards)
                .unwrap()
        );
        assert!(
            !state
                .omitted_record_is_known_empty(EffectKind::Logs)
                .unwrap()
        );

        state.set_present(EffectKind::TokenBalances, true);
        assert!(
            !state
                .omitted_record_is_known_empty(EffectKind::TokenBalances)
                .unwrap()
        );

        let mut invalid = EffectState::new(CpiState::Unavailable);
        invalid.set_present(EffectKind::Outcome, true);
        assert!(matches!(
            EffectState::from_byte(invalid.as_byte()),
            Err(TransactionError::MetadataEnvelopeWithoutBalances)
        ));
    }

    #[test]
    fn dense_effect_rank_restarts_at_each_chunk() {
        let mut states = vec![EffectState::new(CpiState::Unavailable); 257];
        states[0].set_present(EffectKind::Logs, true);
        states[7].set_present(EffectKind::Logs, true);
        states[256].set_present(EffectKind::Logs, true);
        let block = TransactionBlock {
            effect_states: states,
            row_restarts: (0..restart_count(257, ROW_RESTART_INTERVAL))
                .map(|index| RowRestart {
                    row_byte_offset: index as u32,
                    signature_delta: index as u32,
                })
                .collect(),
            effect_files: std::array::from_fn(|_| EffectFileIndex {
                first_chunk_offset: 64,
                chunks: vec![ChunkFrame::raw(1).unwrap(); 2],
            }),
            transaction_rows: vec![0; 32],
        };
        assert_eq!(block.effect_rank(7, EffectKind::Logs).unwrap(), Some(1));
        assert_eq!(block.effect_rank(256, EffectKind::Logs).unwrap(), Some(0));
    }

    #[test]
    fn concatenated_transaction_rows_round_trip_exactly() {
        let mut rows = Vec::new();
        append_transaction(&mut rows, &transaction()).unwrap();
        append_transaction(&mut rows, &transaction()).unwrap();
        assert_eq!(decode_transactions(&rows, 2).unwrap().len(), 2);
        assert!(decode_transactions(&rows, 1).is_err());
    }

    #[test]
    fn split_header_and_arena_preserve_exact_block_bytes() {
        let mut rows = Vec::new();
        append_transaction(&mut rows, &transaction()).unwrap();
        append_transaction(&mut rows, &transaction()).unwrap();
        let header = TransactionBlockHeader {
            effect_states: vec![EffectState::new(CpiState::Unavailable); 2],
            row_restarts: vec![RowRestart {
                row_byte_offset: 0,
                signature_delta: 0,
            }],
            effect_files: std::array::from_fn(|_| EffectFileIndex {
                first_chunk_offset: 0,
                chunks: vec![ChunkFrame::EMPTY],
            }),
        };
        let block = TransactionBlock::from_parts(header.clone(), rows.clone());
        block.validate(2).unwrap();

        let original = encode_block(&block).unwrap();
        let prefix = encode_block_prefix(&header, rows.len()).unwrap();
        assert_eq!(&original[..prefix.len()], prefix);
        assert_eq!(&original[prefix.len()..], rows);
        assert_eq!(encode_block_from_parts(&header, &rows).unwrap(), original);
        assert_eq!(decode_block(&original, 2).unwrap(), block);
    }

    #[test]
    fn readonly_unsigned_count_cannot_underflow_role_math() {
        let mut transaction = transaction();
        transaction.header.num_readonly_unsigned = 2;
        assert!(matches!(
            transaction.validate(),
            Err(TransactionError::ReadonlyUnsignedExceedsUnsignedAccounts)
        ));
    }
}
