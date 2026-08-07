//! Ordered, signature-free Archive V2 input for replay experiments.
//!
//! This path decodes compact messages but never fetches signature bytes. For
//! replay, it projects only the small transaction-metadata prefix needed for
//! authoritative outcomes and Bank-owned lamport reconciliation; logs, token
//! balances, rewards, return data, and compute units remain undecoded. Every
//! transaction in the selected slots is counted; `max_transactions` only
//! bounds the owned samples retained.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use crate::genesis::parse_genesis_bin;
use blockzilla_format::{
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2ComputeBudgetInstructionData,
    ArchiveV2HotBlockHeader, ArchiveV2HotInstruction, ArchiveV2HotInstructionData,
    ArchiveV2HotMessagePayload, ArchiveV2HotTxRow, ArchiveV2SystemInstructionData,
    ArchiveV2VoteHashRef, ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync, CompactMessageHeader,
    CompactPubkey, CompactTransactionError, KeyStore, OwnedCompactAddressTableLookup,
    OwnedCompactRecentBlockhash, WincodeArchiveV2Genesis, WincodeArchiveV2GenesisEpochSchedule,
    WincodeArchiveV2GenesisFeeParams, WincodeArchiveV2GenesisInflationParams,
    WincodeArchiveV2GenesisPohParams, WincodeArchiveV2GenesisRentParams, wincode_leb128_config,
};
use blockzilla_read_sdk::{
    ArchiveReader, BorrowedDecodedBlock, DecodedBlock, GenerationBinding, HashVerification,
    LocalRangeSource, OpenOptions,
    manifest::{BLOCK_INDEX_FILE, BLOCKS_FILE, GenerationManifest},
};
use smallvec::SmallVec;
use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

const MAY_24_2026_MAINNET_EPOCH_0_BLOCKS_SHA256: &str =
    "1550941e1eeff2c361427ba1d545bd0f11e33b7cb7fa9d9fe96b8f45c3c8547f";
const MAY_24_2026_MAINNET_EPOCH_0_INDEX_SHA256: &str =
    "4bc518d10c71c3340f3ef24ddc1d29c3bf54b8dc9250b7ca322b364aa7b3f7de";
const MAY_24_2026_MAINNET_EPOCH_1_BLOCKS_SHA256: &str =
    "3f02379494439c87c70cfd9ab1a6bbdd30c296b29dbd2b13cf6c609f7cda925d";
const MAY_24_2026_MAINNET_EPOCH_1_INDEX_SHA256: &str =
    "5c663da2dd58f3bc6acfce90dd42ba63224f20f224bb78de7145d495c571db58";

/// Manifest object binding that opts a Compact generation into the exact
/// pre-`UnknownSystem`/`UnknownVote` Archive V2 hot-message enum ordering.
///
/// The object itself is a repository asset. Selection requires this exact
/// filename, size, and digest; merely using a similarly named object never
/// changes the decoder.
pub const MAY_24_2026_MESSAGE_SCHEMA_MARKER_FILE: &str =
    "archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker";
pub const MAY_24_2026_MESSAGE_SCHEMA_MARKER_SIZE: u64 = 87;
pub const MAY_24_2026_MESSAGE_SCHEMA_MARKER_SHA256: &str =
    "2a3aa5808085bc7b869c7536508227f19e6b9d9e3f5fb34b65ebda9936bf0206";
#[cfg(test)]
const MAY_24_2026_MESSAGE_SCHEMA_MARKER_BYTES: &[u8] =
    include_bytes!("../assets/archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactProbeConfig {
    pub start_slot: Option<u64>,
    pub end_slot_exclusive: Option<u64>,
    pub max_slots: usize,
    pub max_transactions: usize,
}

impl Default for CompactProbeConfig {
    fn default() -> Self {
        Self {
            start_slot: None,
            end_slot_exclusive: None,
            max_slots: 10,
            max_transactions: 1_000,
        }
    }
}

#[derive(Debug)]
pub struct CompactArchiveProbe {
    pub root: PathBuf,
    pub cluster_id: String,
    pub epoch: u64,
    pub generation_id: String,
    pub slots_per_epoch: u64,
    pub binding: GenerationBinding,
    pub genesis: Option<CompactGenesisProbe>,
    pub slots: Vec<CompactSlotProbe>,
    pub program_instruction_counts: BTreeMap<[u8; 32], u64>,
    pub totals: CompactProbeTotals,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactProbeTotals {
    pub slots_scanned: u64,
    pub transactions_scanned: u64,
    pub transactions_retained: u64,
    pub instructions_scanned: u64,
}

/// Immutable generation identity and the one owned genesis payload delivered
/// before streamed slots. The visitor borrows this value; it is never cloned
/// per block.
#[derive(Debug, Clone)]
pub struct CompactGenerationContext {
    pub root: PathBuf,
    pub cluster_id: String,
    pub epoch: u64,
    pub generation_id: String,
    pub slots_per_epoch: u64,
    /// Exact number of present block rows bound by this generation's hot
    /// index. Skipped ledger slots do not have rows.
    pub block_count: u64,
    /// Sealed manifest state validated by the Compact reader.
    pub complete: bool,
    /// Slot in bound hot-index row zero, or `None` for an empty generation.
    pub first_slot: Option<u64>,
    /// Slot in the final bound hot-index row, or `None` for an empty generation.
    pub last_slot: Option<u64>,
    pub binding: GenerationBinding,
    pub genesis: Option<CompactGenesisProbe>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactVisitConfig {
    pub start_slot: Option<u64>,
    pub end_slot_exclusive: Option<u64>,
    /// `None` streams every selected block. This bound counts blocks present
    /// in the compact index, not empty ledger slots.
    pub max_slots: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgramCountMode {
    Collect,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactVisitControl {
    Continue,
    Stop,
}

/// The first event owns no block data and lets a replay engine seed its bank
/// from `context.genesis`. Each subsequent slot borrow is valid only for that
/// callback and is dropped before the next block is decoded.
#[derive(Debug)]
pub enum CompactVisitEvent<'a> {
    Generation(&'a CompactGenerationContext),
    Slot {
        context: &'a CompactGenerationContext,
        /// Zero-based row ordinal in the generation's bound hot index.
        row_number: u64,
        /// Slot recorded by the immediately following bound hot-index row.
        /// This is independent of visitor range/limit truncation and is
        /// `None` only for the generation's final row.
        next_slot: Option<u64>,
        slot: &'a CompactSlotProbe,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactVisitSummary {
    pub slots_visited: u64,
    pub transactions_visited: u64,
    pub instructions_visited: u64,
    /// Sum of `compressed_len` for the selected `blocks.bin` index rows that
    /// reached the visitor. Control files, registries, genesis sidecars, and
    /// index bytes are deliberately excluded.
    pub compressed_bytes_visited: u64,
    pub stopped_early: bool,
    pub program_instruction_counts: BTreeMap<[u8; 32], u64>,
}

#[derive(Debug)]
pub struct CompactSlotProbe {
    pub block_id: u32,
    pub slot: u64,
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub blockhash_id: u32,
    pub blockhash: [u8; 32],
    pub previous_blockhash_id: u32,
    pub previous_blockhash: [u8; 32],
    pub transaction_count: u32,
    /// A global-prefix sample; this may be shorter than `transaction_count`.
    pub transactions: Vec<CompactTransactionProbe>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactMessageVersion {
    Legacy,
    V0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactMessageSchema {
    Current,
    /// Archive V2 hot blocks produced on 2026-05-24, before
    /// `UnknownSystem` and `UnknownVote` were inserted into the instruction
    /// enum. The insertion shifted every existing non-raw wincode enum tag by
    /// two without changing the hot-block version.
    May24_2026PreUnknownInstructionFallbacks,
}

const MAY24_INLINE_ACCOUNT_KEYS: usize = 8;
const MAY24_INLINE_INSTRUCTIONS: usize = 2;
const MAY24_INLINE_INSTRUCTION_ACCOUNTS: usize = 8;
const COMPACT_INLINE_ACCOUNT_KEYS: usize = 8;
const COMPACT_INLINE_INSTRUCTIONS: usize = 1;
const COMPACT_INLINE_INSTRUCTION_ACCOUNTS: usize = 8;
const COMPACT_INLINE_RAW_INSTRUCTION_BYTES: usize = 64;

#[derive(Debug, SchemaRead, SchemaWrite)]
enum May24ArchiveV2HotMessagePayload {
    Legacy(May24ArchiveV2HotLegacyMessage),
    V0(May24ArchiveV2HotV0Message),
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct May24ArchiveV2HotLegacyMessage {
    header: CompactMessageHeader,
    account_keys: SmallVec<[CompactPubkey; MAY24_INLINE_ACCOUNT_KEYS]>,
    recent_blockhash: OwnedCompactRecentBlockhash,
    instructions: SmallVec<[May24ArchiveV2HotInstruction; MAY24_INLINE_INSTRUCTIONS]>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct May24ArchiveV2HotV0Message {
    header: CompactMessageHeader,
    account_keys: SmallVec<[CompactPubkey; MAY24_INLINE_ACCOUNT_KEYS]>,
    recent_blockhash: OwnedCompactRecentBlockhash,
    instructions: SmallVec<[May24ArchiveV2HotInstruction; MAY24_INLINE_INSTRUCTIONS]>,
    address_table_lookups: Vec<OwnedCompactAddressTableLookup>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct May24ArchiveV2HotInstruction {
    program_id_index: u8,
    accounts: SmallVec<[u8; MAY24_INLINE_INSTRUCTION_ACCOUNTS]>,
    data: May24ArchiveV2HotInstructionData,
}

/// Exact pre-2026-06-25 ordering from the writer that produced the two known
/// mainnet epoch objects. Do not insert variants into this compatibility enum.
#[derive(Debug, SchemaRead, SchemaWrite)]
enum May24ArchiveV2HotInstructionData {
    Raw(SmallVec<[u8; COMPACT_INLINE_RAW_INSTRUCTION_BYTES]>),
    ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
    System(ArchiveV2SystemInstructionData),
    VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate),
    VoteCompactUpdateVoteStateSwitch {
        update: ArchiveV2VoteStateUpdate,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
    VoteTowerSync(ArchiveV2VoteTowerSync),
    VoteTowerSyncSwitch {
        tower: ArchiveV2VoteTowerSync,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
}

#[derive(Debug)]
pub struct CompactTransactionProbe {
    pub tx_index: u32,
    pub row_flags: u32,
    pub archived_outcome: CompactArchivedTransactionOutcome,
    /// Prefix-only metadata projection. This deliberately excludes logs,
    /// token balances, rewards, inner instructions, return data, and CU data.
    pub balance_oracle: Option<CompactTransactionBalanceOracle>,
    /// Count from the hot row. Signature bytes are deliberately not read.
    pub signature_count: u8,
    pub version: CompactMessageVersion,
    pub header: CompactMessageHeader,
    pub account_keys: SmallVec<[[u8; 32]; COMPACT_INLINE_ACCOUNT_KEYS]>,
    pub recent_blockhash: CompactRecentBlockhashProbe,
    pub address_table_lookups: Vec<CompactAddressTableLookupProbe>,
    pub instructions: SmallVec<[CompactInstructionProbe; COMPACT_INLINE_INSTRUCTIONS]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactTransactionBalanceOracle {
    pub fee: u64,
    pub pre_balances: SmallVec<[u64; 8]>,
    pub post_balances: SmallVec<[u64; 8]>,
}

#[derive(Debug, SchemaRead)]
struct CompactTransactionMetadataPrefix {
    err: Option<CompactTransactionError>,
    fee: u64,
    pre_balances: SmallVec<[u64; 8]>,
    post_balances: SmallVec<[u64; 8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactArchivedTransactionOutcome {
    /// The source archive did not carry decoded transaction metadata.
    Unknown,
    /// Decoded archive metadata classified the transaction as successful.
    /// Replay has not independently executed or verified this result.
    Succeeded,
    /// Decoded archive metadata classified the transaction as failed. Replay
    /// has not independently executed or verified this result.
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactRecentBlockhashProbe {
    Registry { id: i32, hash: [u8; 32] },
    Nonce([u8; 32]),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactAddressTableLookupProbe {
    pub account_key: [u8; 32],
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug)]
pub struct CompactInstructionProbe {
    pub instruction_index: u32,
    pub program_id_index: u8,
    pub program_id: [u8; 32],
    pub account_indexes: SmallVec<[u8; COMPACT_INLINE_INSTRUCTION_ACCOUNTS]>,
    pub data: CompactInstructionData,
}

/// Replay-owned instruction payload. Byte-oriented variants stay inline for
/// launch-era instructions up to 64 bytes while decoded semantic variants keep
/// the exact Archive V2 representation.
#[derive(Debug)]
pub enum CompactInstructionData {
    Raw(SmallVec<[u8; COMPACT_INLINE_RAW_INSTRUCTION_BYTES]>),
    UnknownSystem(SmallVec<[u8; COMPACT_INLINE_RAW_INSTRUCTION_BYTES]>),
    UnknownVote(SmallVec<[u8; COMPACT_INLINE_RAW_INSTRUCTION_BYTES]>),
    ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
    System(ArchiveV2SystemInstructionData),
    VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate),
    VoteCompactUpdateVoteStateSwitch {
        update: ArchiveV2VoteStateUpdate,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
    VoteTowerSync(ArchiveV2VoteTowerSync),
    VoteTowerSyncSwitch {
        tower: ArchiveV2VoteTowerSync,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactGenesisSource {
    ExactGenesisBin,
    InlineLegacy,
}

#[derive(Debug, Clone)]
pub struct CompactGenesisProbe {
    pub source: CompactGenesisSource,
    pub genesis_hash: [u8; 32],
    pub genesis_bin_len: u64,
    pub creation_time_unix: i64,
    pub cluster_id: u32,
    pub ticks_per_slot: u64,
    /// `None` means the legacy inline Archive V2 record omitted this field.
    pub slots_per_segment: Option<u64>,
    /// Also absent from the legacy inline record.
    pub backwards_compat_with_v0_23: Option<u64>,
    pub poh_params: WincodeArchiveV2GenesisPohParams,
    pub fees: WincodeArchiveV2GenesisFeeParams,
    pub rent: WincodeArchiveV2GenesisRentParams,
    pub inflation: WincodeArchiveV2GenesisInflationParams,
    /// Present only when recovered from exact `genesis.bin`.
    pub inflation_storage: Option<f64>,
    pub epoch_schedule: WincodeArchiveV2GenesisEpochSchedule,
    pub accounts: Vec<CompactGenesisAccount>,
    pub builtins: Vec<CompactGenesisBuiltin>,
    pub reward_pools: Vec<CompactGenesisAccount>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactGenesisAccount {
    pub pubkey: [u8; 32],
    pub lamports: u64,
    pub owner: [u8; 32],
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactGenesisBuiltin {
    pub key: String,
    pub pubkey: [u8; 32],
}

#[derive(Debug, Error)]
pub enum CompactProbeError {
    #[error("invalid compact probe range: start slot {start} is after end slot {end}")]
    InvalidRange { start: u64, end: u64 },
    #[error("open Archive V2 generation at {root}: {message}")]
    Open { root: PathBuf, message: String },
    #[error("read {path}: {message}")]
    Sidecar { path: PathBuf, message: String },
    #[error("invalid compact genesis: {0}")]
    Genesis(String),
    #[error("slot {slot} tx {tx_index} is a raw transaction fallback and cannot be replayed")]
    RawTransactionFallback { slot: u64, tx_index: u32 },
    #[error("slot {slot} tx {tx_index}: {message}")]
    Transaction {
        slot: u64,
        tx_index: u32,
        message: String,
    },
    #[error("read Archive V2 block row {row}: {message}")]
    Block { row: usize, message: String },
    #[error("counter overflow while scanning compact generation")]
    CounterOverflow,
    #[error("compact replay visitor failed: {0}")]
    Visitor(String),
}

/// Return the original instruction bytes for payload variants that Archive V2
/// intentionally leaves undecoded. Decoded system/vote/compute variants are
/// semantic records and therefore return `None` instead of inventing bytes.
pub fn instruction_data_bytes(data: &CompactInstructionData) -> Option<&[u8]> {
    match data {
        CompactInstructionData::Raw(bytes)
        | CompactInstructionData::UnknownSystem(bytes)
        | CompactInstructionData::UnknownVote(bytes) => Some(bytes),
        CompactInstructionData::ComputeBudget(_)
        | CompactInstructionData::System(_)
        | CompactInstructionData::VoteCompactUpdateVoteState(_)
        | CompactInstructionData::VoteCompactUpdateVoteStateSwitch { .. }
        | CompactInstructionData::VoteTowerSync(_)
        | CompactInstructionData::VoteTowerSyncSwitch { .. } => None,
    }
}

/// Open and validate one Compact generation without decoding a block payload.
///
/// This is the admission path used to bind a frozen replay checkpoint to the
/// exact completed generation whose final index row it consumed. Control files
/// retain the same validation policy as normal replay.
pub fn read_compact_generation_context(
    root: impl AsRef<Path>,
) -> Result<CompactGenerationContext, CompactProbeError> {
    OpenCompactGeneration::open(root.as_ref()).map(|opened| opened.context)
}

/// Read a bounded owned probe, preserving index/file order.
pub fn probe_compact_generation(
    root: impl AsRef<Path>,
    config: CompactProbeConfig,
) -> Result<CompactArchiveProbe, CompactProbeError> {
    validate_config(config)?;
    let opened = OpenCompactGeneration::open(root.as_ref())?;
    let mut slots = Vec::with_capacity(config.max_slots.min(opened.archive.index().rows.len()));
    let mut counts = BTreeMap::new();
    let mut totals = CompactProbeTotals::default();

    for (row_number, row) in opened.archive.index().rows.iter().copied().enumerate() {
        if config.start_slot.is_some_and(|start| row.slot < start) {
            continue;
        }
        if config.end_slot_exclusive.is_some_and(|end| row.slot >= end) {
            break;
        }
        if slots.len() == config.max_slots {
            break;
        }
        let remaining = config
            .max_transactions
            .saturating_sub(totals.transactions_retained as usize);
        let decoded = opened.decode_slot(row_number, remaining)?;
        totals.transactions_scanned =
            checked_add(totals.transactions_scanned, decoded.transactions_scanned)?;
        totals.transactions_retained = checked_add(
            totals.transactions_retained,
            decoded.slot.transactions.len() as u64,
        )?;
        totals.instructions_scanned =
            checked_add(totals.instructions_scanned, decoded.instructions_scanned)?;
        merge_program_counts(&mut counts, decoded.program_instruction_counts)?;
        slots.push(decoded.slot);
        totals.slots_scanned = checked_inc(totals.slots_scanned)?;
    }

    let CompactGenerationContext {
        root,
        cluster_id,
        epoch,
        generation_id,
        slots_per_epoch,
        block_count: _,
        complete: _,
        first_slot: _,
        last_slot: _,
        binding,
        genesis,
    } = opened.context;
    Ok(CompactArchiveProbe {
        root,
        cluster_id,
        epoch,
        generation_id,
        slots_per_epoch,
        binding,
        genesis,
        slots,
        program_instruction_counts: counts,
        totals,
    })
}

/// Stream one generation without retaining previously visited blocks.
///
/// The generation event is delivered exactly once before any slot. Every slot
/// event contains all transactions and instructions for that indexed block.
/// Returning [`CompactVisitControl::Stop`] ends cleanly after the current
/// callback. Callback failures can be represented with
/// [`CompactProbeError::Visitor`].
pub fn visit_compact_generation<F>(
    root: impl AsRef<Path>,
    config: CompactVisitConfig,
    visitor: F,
) -> Result<CompactVisitSummary, CompactProbeError>
where
    F: for<'a> FnMut(CompactVisitEvent<'a>) -> Result<CompactVisitControl, CompactProbeError>,
{
    visit_compact_generation_inner(root, config, ProgramCountMode::Collect, visitor)
}

/// Replay-only streaming path which omits the diagnostic program histogram.
///
/// Slot, transaction, and instruction totals are still exact. The public
/// [`visit_compact_generation`] API and owned probe continue to collect their
/// documented per-program counts.
pub fn visit_compact_generation_without_program_counts<F>(
    root: impl AsRef<Path>,
    config: CompactVisitConfig,
    visitor: F,
) -> Result<CompactVisitSummary, CompactProbeError>
where
    F: for<'a> FnMut(CompactVisitEvent<'a>) -> Result<CompactVisitControl, CompactProbeError>,
{
    visit_compact_generation_inner(root, config, ProgramCountMode::Skip, visitor)
}

fn visit_compact_generation_inner<F>(
    root: impl AsRef<Path>,
    config: CompactVisitConfig,
    program_count_mode: ProgramCountMode,
    mut visitor: F,
) -> Result<CompactVisitSummary, CompactProbeError>
where
    F: for<'a> FnMut(CompactVisitEvent<'a>) -> Result<CompactVisitControl, CompactProbeError>,
{
    validate_range(config.start_slot, config.end_slot_exclusive)?;
    let opened = OpenCompactGeneration::open(root.as_ref())?;
    let rows = &opened.archive.index().rows;
    let selected_start = config
        .start_slot
        .map_or(0, |start| rows.partition_point(|row| row.slot < start));
    let selected_end = config
        .end_slot_exclusive
        .map_or(rows.len(), |end| rows.partition_point(|row| row.slot < end));
    let selected_count = selected_end.saturating_sub(selected_start);
    let (visit_count, truncated_by_limit) = bounded_visit_count(selected_count, config.max_slots);
    let selected_row_end = selected_start + visit_count;
    let mut blocks = opened
        .archive
        .borrowed_blocks_without_rewards_range(selected_start..selected_row_end)
        .map_err(|error| CompactProbeError::Block {
            row: selected_start,
            message: error.to_string(),
        })?;
    let mut summary = begin_compact_visit(&opened.context, &mut visitor)?;
    if summary.stopped_early {
        summary.stopped_early |= truncated_by_limit;
        return Ok(summary);
    }

    let mut transactions = Vec::new();
    let mut relative_row = 0usize;
    while let Some(decoded) = blocks.next_block() {
        let row_number = selected_start + relative_row;
        let row_number_u64 =
            u64::try_from(row_number).map_err(|_| CompactProbeError::CounterOverflow)?;
        let next_slot = row_number
            .checked_add(1)
            .and_then(|next_row| rows.get(next_row))
            .map(|row| row.slot);
        let decoded = decoded.map_err(|error| CompactProbeError::Block {
            row: row_number,
            message: error.to_string(),
        })?;
        let mut decoded = opened.decode_slot_from_borrowed_block_with_transactions(
            row_number,
            usize::MAX,
            &decoded,
            transactions,
            program_count_mode,
        )?;
        let control = visit_decoded_compact_slot(
            &opened.context,
            &mut summary,
            row_number_u64,
            next_slot,
            u64::from(rows[row_number].compressed_len),
            &decoded,
            program_count_mode,
            &mut visitor,
        )?;
        // Reclaim and empty the outer transaction allocation before the loop
        // asks the coalesced block iterator for its next decoded frame.
        transactions = std::mem::take(&mut decoded.slot.transactions);
        transactions.clear();
        if control == CompactVisitControl::Stop {
            summary.stopped_early = true;
            break;
        }
        relative_row += 1;
    }
    summary.stopped_early |= truncated_by_limit;
    Ok(summary)
}

fn bounded_visit_count(selected_count: usize, max_slots: Option<usize>) -> (usize, bool) {
    let visit_count = selected_count.min(max_slots.unwrap_or(usize::MAX));
    (visit_count, visit_count < selected_count)
}

#[cfg(test)]
fn drive_compact_visit<I, F>(
    context: &CompactGenerationContext,
    decoded_slots: I,
    visitor: &mut F,
) -> Result<CompactVisitSummary, CompactProbeError>
where
    I: IntoIterator<Item = Result<DecodedCompactRow, CompactProbeError>>,
    F: for<'a> FnMut(CompactVisitEvent<'a>) -> Result<CompactVisitControl, CompactProbeError>,
{
    drive_compact_visit_with_program_counts(
        context,
        decoded_slots,
        ProgramCountMode::Collect,
        visitor,
    )
}

#[cfg(test)]
fn drive_compact_visit_with_program_counts<I, F>(
    context: &CompactGenerationContext,
    decoded_slots: I,
    program_count_mode: ProgramCountMode,
    visitor: &mut F,
) -> Result<CompactVisitSummary, CompactProbeError>
where
    I: IntoIterator<Item = Result<DecodedCompactRow, CompactProbeError>>,
    F: for<'a> FnMut(CompactVisitEvent<'a>) -> Result<CompactVisitControl, CompactProbeError>,
{
    let mut summary = begin_compact_visit(context, visitor)?;
    if summary.stopped_early {
        return Ok(summary);
    }
    for decoded in decoded_slots {
        let (row_number, next_slot, compressed_len, decoded) = decoded?;
        if visit_decoded_compact_slot(
            context,
            &mut summary,
            row_number,
            next_slot,
            compressed_len,
            &decoded,
            program_count_mode,
            visitor,
        )? == CompactVisitControl::Stop
        {
            summary.stopped_early = true;
            break;
        }
    }
    Ok(summary)
}

fn begin_compact_visit<F>(
    context: &CompactGenerationContext,
    visitor: &mut F,
) -> Result<CompactVisitSummary, CompactProbeError>
where
    F: for<'a> FnMut(CompactVisitEvent<'a>) -> Result<CompactVisitControl, CompactProbeError>,
{
    let mut summary = CompactVisitSummary {
        slots_visited: 0,
        transactions_visited: 0,
        instructions_visited: 0,
        compressed_bytes_visited: 0,
        stopped_early: false,
        program_instruction_counts: BTreeMap::new(),
    };
    if visitor(CompactVisitEvent::Generation(context))? == CompactVisitControl::Stop {
        summary.stopped_early = true;
    }
    Ok(summary)
}

fn visit_decoded_compact_slot<F>(
    context: &CompactGenerationContext,
    summary: &mut CompactVisitSummary,
    row_number: u64,
    next_slot: Option<u64>,
    compressed_len: u64,
    decoded: &DecodedCompactSlot,
    program_count_mode: ProgramCountMode,
    visitor: &mut F,
) -> Result<CompactVisitControl, CompactProbeError>
where
    F: for<'a> FnMut(CompactVisitEvent<'a>) -> Result<CompactVisitControl, CompactProbeError>,
{
    summary.slots_visited = checked_inc(summary.slots_visited)?;
    summary.transactions_visited =
        checked_add(summary.transactions_visited, decoded.transactions_scanned)?;
    summary.instructions_visited =
        checked_add(summary.instructions_visited, decoded.instructions_scanned)?;
    summary.compressed_bytes_visited =
        checked_add(summary.compressed_bytes_visited, compressed_len)?;
    if program_count_mode == ProgramCountMode::Collect {
        merge_program_counts(
            &mut summary.program_instruction_counts,
            decoded.program_instruction_counts.iter().copied(),
        )?;
    }
    visitor(CompactVisitEvent::Slot {
        context,
        row_number,
        next_slot,
        slot: &decoded.slot,
    })
}

struct OpenCompactGeneration {
    archive: ArchiveReader<LocalRangeSource>,
    keys: KeyStore,
    blockhashes: BlockhashStore,
    message_schema: CompactMessageSchema,
    context: CompactGenerationContext,
}

struct DecodedCompactSlot {
    slot: CompactSlotProbe,
    transactions_scanned: u64,
    instructions_scanned: u64,
    program_instruction_counts: SmallVec<[([u8; 32], u64); 4]>,
}

#[cfg(test)]
type DecodedCompactRow = (u64, Option<u64>, u64, DecodedCompactSlot);

#[derive(Debug, Clone, Copy)]
struct HistoricalMessageSchemaIdentity {
    epoch: u64,
    blocks_sha256: &'static str,
    index_sha256: &'static str,
}

const MAY_24_2026_MAINNET_IDENTITIES: [HistoricalMessageSchemaIdentity; 2] = [
    HistoricalMessageSchemaIdentity {
        epoch: 0,
        blocks_sha256: MAY_24_2026_MAINNET_EPOCH_0_BLOCKS_SHA256,
        index_sha256: MAY_24_2026_MAINNET_EPOCH_0_INDEX_SHA256,
    },
    HistoricalMessageSchemaIdentity {
        epoch: 1,
        blocks_sha256: MAY_24_2026_MAINNET_EPOCH_1_BLOCKS_SHA256,
        index_sha256: MAY_24_2026_MAINNET_EPOCH_1_INDEX_SHA256,
    },
];

fn message_schema_for_manifest(
    manifest: &GenerationManifest,
) -> Result<CompactMessageSchema, String> {
    let blocks = manifest
        .required_file(BLOCKS_FILE)
        .map_err(|error| error.to_string())?;
    let index = manifest
        .required_file(BLOCK_INDEX_FILE)
        .map_err(|error| error.to_string())?;

    if let Some(marker) = manifest.file(MAY_24_2026_MESSAGE_SCHEMA_MARKER_FILE) {
        if marker.size != MAY_24_2026_MESSAGE_SCHEMA_MARKER_SIZE
            || marker.sha256 != MAY_24_2026_MESSAGE_SCHEMA_MARKER_SHA256
        {
            return Err(format!(
                "malformed May 24 2026 historical message-schema marker binding: {} must have size {} and sha256 {}, found size {} and sha256 {}",
                MAY_24_2026_MESSAGE_SCHEMA_MARKER_FILE,
                MAY_24_2026_MESSAGE_SCHEMA_MARKER_SIZE,
                MAY_24_2026_MESSAGE_SCHEMA_MARKER_SHA256,
                marker.size,
                marker.sha256,
            ));
        }
        return Ok(CompactMessageSchema::May24_2026PreUnknownInstructionFallbacks);
    }

    for identity in MAY_24_2026_MAINNET_IDENTITIES {
        let blocks_match = blocks.sha256 == identity.blocks_sha256;
        let index_match = index.sha256 == identity.index_sha256;
        let provenance_matches = manifest.cluster_id == "mainnet-beta"
            && manifest.epoch == identity.epoch
            && manifest.slots_per_epoch == 432_000;
        if blocks_match && index_match && provenance_matches {
            return Ok(CompactMessageSchema::May24_2026PreUnknownInstructionFallbacks);
        }
        if blocks_match || index_match {
            return Err(format!(
                "known May 24 2026 historical message bytes have mismatched provenance: cluster={} epoch={} slots_per_epoch={} blocks_sha256={} index_sha256={}",
                manifest.cluster_id,
                manifest.epoch,
                manifest.slots_per_epoch,
                blocks.sha256,
                index.sha256,
            ));
        }
    }

    Ok(CompactMessageSchema::Current)
}

impl OpenCompactGeneration {
    fn open(root: &Path) -> Result<Self, CompactProbeError> {
        let root = root.to_path_buf();
        let options = OpenOptions {
            hash_verification: HashVerification::ControlFiles,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(LocalRangeSource::new(&root), options)
            .map_err(|error| CompactProbeError::Open {
                root: root.clone(),
                message: error.to_string(),
            })?;
        // Replay intentionally uses the manifest-bound schema identity without
        // re-hashing blocks.bin on every open. The Compact reader validates the
        // control plane and every indexed frame is still bounds/decompression
        // checked while streaming; full payload authentication belongs at the
        // archive admission boundary, not in this trusted replay hot path.
        let message_schema =
            message_schema_for_manifest(archive.manifest()).map_err(|message| {
                CompactProbeError::Open {
                    root: root.clone(),
                    message,
                }
            })?;
        let registry_path = root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
        let keys = KeyStore::load(&registry_path).map_err(|error| CompactProbeError::Sidecar {
            path: registry_path,
            message: error.to_string(),
        })?;
        let blockhashes = BlockhashStore::load(&root)?;
        let genesis = own_archive_genesis(archive.genesis(), archive.genesis_bin(), &keys)?;
        if let Some(genesis) = &genesis
            && blockhashes.resolve_current(0) != Some(genesis.genesis_hash)
        {
            return Err(CompactProbeError::Genesis(
                "blockhash registry does not begin with the embedded genesis hash".into(),
            ));
        }
        let manifest = archive.manifest();
        let block_count = u64::try_from(archive.index().rows.len())
            .map_err(|_| CompactProbeError::CounterOverflow)?;
        let first_slot = archive.index().rows.first().map(|row| row.slot);
        let last_slot = archive.index().rows.last().map(|row| row.slot);
        let context = CompactGenerationContext {
            root,
            cluster_id: manifest.cluster_id.clone(),
            epoch: manifest.epoch,
            generation_id: manifest.generation_id.clone(),
            slots_per_epoch: manifest.slots_per_epoch,
            block_count,
            complete: manifest.complete,
            first_slot,
            last_slot,
            binding: archive.binding(),
            genesis,
        };
        Ok(Self {
            archive,
            keys,
            blockhashes,
            message_schema,
            context,
        })
    }

    fn decode_slot(
        &self,
        row_number: usize,
        retain_transactions: usize,
    ) -> Result<DecodedCompactSlot, CompactProbeError> {
        let decoded =
            self.archive
                .read_block(row_number)
                .map_err(|error| CompactProbeError::Block {
                    row: row_number,
                    message: error.to_string(),
                })?;
        self.decode_slot_from_block(row_number, retain_transactions, decoded)
    }

    fn decode_slot_from_block(
        &self,
        row_number: usize,
        retain_transactions: usize,
        decoded: DecodedBlock,
    ) -> Result<DecodedCompactSlot, CompactProbeError> {
        self.decode_slot_from_block_with_transactions(
            row_number,
            retain_transactions,
            decoded,
            Vec::new(),
        )
    }

    fn decode_slot_from_block_with_transactions(
        &self,
        row_number: usize,
        retain_transactions: usize,
        decoded: DecodedBlock,
        transactions: Vec<CompactTransactionProbe>,
    ) -> Result<DecodedCompactSlot, CompactProbeError> {
        self.decode_slot_from_hot_parts(
            row_number,
            retain_transactions,
            decoded.index_row.block_id,
            &decoded.block.header,
            decoded.block.tx_count,
            decoded.block.tx_rows.len(),
            decoded.block.tx_rows.iter().copied(),
            &decoded.block.message_bytes,
            &decoded.block.metadata_bytes,
            transactions,
            ProgramCountMode::Collect,
        )
    }

    fn decode_slot_from_borrowed_block_with_transactions(
        &self,
        row_number: usize,
        retain_transactions: usize,
        decoded: &BorrowedDecodedBlock<'_>,
        transactions: Vec<CompactTransactionProbe>,
        program_count_mode: ProgramCountMode,
    ) -> Result<DecodedCompactSlot, CompactProbeError> {
        self.decode_slot_from_hot_parts(
            row_number,
            retain_transactions,
            decoded.index_row.block_id,
            decoded.header(),
            decoded.tx_count(),
            decoded.tx_rows_len(),
            decoded.tx_rows(),
            decoded.message_bytes(),
            decoded.metadata_bytes(),
            transactions,
            program_count_mode,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_slot_from_hot_parts(
        &self,
        row_number: usize,
        retain_transactions: usize,
        block_id: u32,
        header: &ArchiveV2HotBlockHeader,
        tx_count: u32,
        tx_rows_len: usize,
        tx_rows: impl ExactSizeIterator<Item = ArchiveV2HotTxRow>,
        message_bytes: &[u8],
        metadata_bytes: &[u8],
        mut transactions: Vec<CompactTransactionProbe>,
        program_count_mode: ProgramCountMode,
    ) -> Result<DecodedCompactSlot, CompactProbeError> {
        let current_id = header.blockhash_id;
        let previous_id = header.previous_blockhash_id;
        let blockhash = self
            .blockhashes
            .resolve_current(current_id)
            .ok_or_else(|| CompactProbeError::Block {
                row: row_number,
                message: format!("missing blockhash id {current_id}"),
            })?;
        let previous_blockhash = self
            .blockhashes
            .resolve_previous(current_id, previous_id)
            .ok_or_else(|| CompactProbeError::Block {
                row: row_number,
                message: format!("missing previous blockhash id {previous_id}"),
            })?;
        prepare_transaction_buffer(&mut transactions, retain_transactions.min(tx_rows_len));
        let mut instructions_scanned = 0u64;
        let mut program_instruction_counts = SmallVec::<[([u8; 32], u64); 4]>::new();
        for tx_row in tx_rows {
            let transaction = own_transaction(
                header.slot,
                tx_row,
                message_bytes,
                metadata_bytes,
                &self.keys,
                &self.blockhashes,
                self.message_schema,
            )?;
            instructions_scanned =
                checked_add(instructions_scanned, transaction.instructions.len() as u64)?;
            if program_count_mode == ProgramCountMode::Collect {
                for instruction in &transaction.instructions {
                    if let Some((_, count)) = program_instruction_counts
                        .iter_mut()
                        .find(|(program_id, _)| *program_id == instruction.program_id)
                    {
                        *count = checked_inc(*count)?;
                    } else {
                        program_instruction_counts.push((instruction.program_id, 1));
                    }
                }
            }
            if transactions.len() < retain_transactions {
                transactions.push(transaction);
            }
        }
        let transactions_scanned =
            u64::try_from(tx_rows_len).map_err(|_| CompactProbeError::CounterOverflow)?;
        Ok(DecodedCompactSlot {
            slot: CompactSlotProbe {
                block_id,
                slot: header.slot,
                parent_slot: header.parent_slot,
                block_time: header.block_time,
                block_height: header.block_height,
                blockhash_id: current_id,
                blockhash,
                previous_blockhash_id: previous_id,
                previous_blockhash,
                transaction_count: tx_count,
                transactions,
            },
            transactions_scanned,
            instructions_scanned,
            program_instruction_counts,
        })
    }
}

fn prepare_transaction_buffer(
    transactions: &mut Vec<CompactTransactionProbe>,
    retain_transactions: usize,
) {
    transactions.clear();
    if transactions.capacity() < retain_transactions {
        transactions.reserve(retain_transactions);
    }
}

fn checked_inc(value: u64) -> Result<u64, CompactProbeError> {
    checked_add(value, 1)
}

fn checked_add(left: u64, right: u64) -> Result<u64, CompactProbeError> {
    left.checked_add(right)
        .ok_or(CompactProbeError::CounterOverflow)
}

fn merge_program_counts(
    target: &mut BTreeMap<[u8; 32], u64>,
    source: impl IntoIterator<Item = ([u8; 32], u64)>,
) -> Result<(), CompactProbeError> {
    for (program_id, source_count) in source {
        let target_count = target.entry(program_id).or_insert(0);
        *target_count = checked_add(*target_count, source_count)?;
    }
    Ok(())
}

fn validate_config(config: CompactProbeConfig) -> Result<(), CompactProbeError> {
    validate_range(config.start_slot, config.end_slot_exclusive)
}

fn validate_range(
    start_slot: Option<u64>,
    end_slot_exclusive: Option<u64>,
) -> Result<(), CompactProbeError> {
    if let (Some(start), Some(end)) = (start_slot, end_slot_exclusive)
        && start > end
    {
        return Err(CompactProbeError::InvalidRange { start, end });
    }
    Ok(())
}

fn decode_balance_oracle(
    slot: u64,
    row: ArchiveV2HotTxRow,
    metadata_region: &[u8],
) -> Result<Option<CompactTransactionBalanceOracle>, CompactProbeError> {
    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0
        || row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0
    {
        return Ok(None);
    }

    let start = row.metadata_offset as usize;
    let end = start
        .checked_add(row.metadata_len as usize)
        .ok_or_else(|| tx_error(slot, row.tx_index, "metadata range overflow"))?;
    let bytes = metadata_region
        .get(start..end)
        .filter(|bytes| !bytes.is_empty())
        .ok_or_else(|| {
            tx_error(
                slot,
                row.tx_index,
                "metadata range is outside block payload",
            )
        })?;
    // Prefix decoding is intentional: wincode stops after post_balances, so
    // the large log/token/inner-instruction tail is neither visited nor owned.
    let prefix: CompactTransactionMetadataPrefix =
        wincode::config::deserialize(bytes, wincode_leb128_config()).map_err(|error| {
            tx_error(
                slot,
                row.tx_index,
                format!("decode metadata balance prefix: {error}"),
            )
        })?;
    let flag_has_error = row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;
    if prefix.err.is_some() != flag_has_error {
        return Err(tx_error(
            slot,
            row.tx_index,
            "metadata error disagrees with hot-row flags",
        ));
    }
    // Load-stage failures such as AccountNotFound legitimately carry status
    // metadata and a fee field but no account-balance snapshots at all.
    if prefix.pre_balances.is_empty() && prefix.post_balances.is_empty() {
        return Ok(None);
    }
    Ok(Some(CompactTransactionBalanceOracle {
        fee: prefix.fee,
        pre_balances: prefix.pre_balances,
        post_balances: prefix.post_balances,
    }))
}

fn own_transaction(
    slot: u64,
    row: ArchiveV2HotTxRow,
    message_region: &[u8],
    metadata_region: &[u8],
    keys: &KeyStore,
    blockhashes: &BlockhashStore,
    message_schema: CompactMessageSchema,
) -> Result<CompactTransactionProbe, CompactProbeError> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Err(CompactProbeError::RawTransactionFallback {
            slot,
            tx_index: row.tx_index,
        });
    }
    let start = row.message_offset as usize;
    let end = start
        .checked_add(row.message_len as usize)
        .ok_or_else(|| tx_error(slot, row.tx_index, "message range overflow"))?;
    let bytes = message_region
        .get(start..end)
        .ok_or_else(|| tx_error(slot, row.tx_index, "message range is outside block payload"))?;
    let balance_oracle = decode_balance_oracle(slot, row, metadata_region)?;
    match message_schema {
        CompactMessageSchema::Current => {
            let payload: ArchiveV2HotMessagePayload =
                wincode::config::deserialize_exact(bytes, wincode_leb128_config()).map_err(
                    |error| tx_error(slot, row.tx_index, format!("decode message: {error}")),
                )?;
            match payload {
                ArchiveV2HotMessagePayload::Legacy(message) => own_decoded_transaction(
                    slot,
                    row,
                    CompactMessageVersion::Legacy,
                    message.header,
                    message.account_keys,
                    message.recent_blockhash,
                    message.instructions.into_iter().map(Into::into),
                    Vec::new(),
                    balance_oracle,
                    keys,
                    blockhashes,
                ),
                ArchiveV2HotMessagePayload::V0(message) => own_decoded_transaction(
                    slot,
                    row,
                    CompactMessageVersion::V0,
                    message.header,
                    message.account_keys,
                    message.recent_blockhash,
                    message.instructions.into_iter().map(Into::into),
                    message.address_table_lookups,
                    balance_oracle,
                    keys,
                    blockhashes,
                ),
            }
        }
        CompactMessageSchema::May24_2026PreUnknownInstructionFallbacks => {
            let payload: May24ArchiveV2HotMessagePayload =
                wincode::config::deserialize_exact(bytes, wincode_leb128_config()).map_err(
                    |error| tx_error(slot, row.tx_index, format!("decode message: {error}")),
                )?;
            match payload {
                May24ArchiveV2HotMessagePayload::Legacy(message) => own_decoded_transaction(
                    slot,
                    row,
                    CompactMessageVersion::Legacy,
                    message.header,
                    message.account_keys,
                    message.recent_blockhash,
                    message.instructions.into_iter().map(Into::into),
                    Vec::new(),
                    balance_oracle,
                    keys,
                    blockhashes,
                ),
                May24ArchiveV2HotMessagePayload::V0(message) => own_decoded_transaction(
                    slot,
                    row,
                    CompactMessageVersion::V0,
                    message.header,
                    message.account_keys,
                    message.recent_blockhash,
                    message.instructions.into_iter().map(Into::into),
                    message.address_table_lookups,
                    balance_oracle,
                    keys,
                    blockhashes,
                ),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn own_decoded_transaction(
    slot: u64,
    row: ArchiveV2HotTxRow,
    version: CompactMessageVersion,
    header: CompactMessageHeader,
    compact_keys: impl IntoIterator<Item = CompactPubkey>,
    recent: OwnedCompactRecentBlockhash,
    instructions: impl IntoIterator<Item = DecodedCompactInstruction>,
    lookups: Vec<OwnedCompactAddressTableLookup>,
    mut balance_oracle: Option<CompactTransactionBalanceOracle>,
    keys: &KeyStore,
    blockhashes: &BlockhashStore,
) -> Result<CompactTransactionProbe, CompactProbeError> {
    if matches!(version, CompactMessageVersion::V0)
        != (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0)
    {
        return Err(tx_error(
            slot,
            row.tx_index,
            "message version disagrees with flags",
        ));
    }
    let account_keys = own_account_keys(slot, row.tx_index, compact_keys, keys)?;
    validate_message_header(slot, row.tx_index, header, account_keys.len())?;
    if row.signature_count != header.num_required_signatures {
        return Err(tx_error(
            slot,
            row.tx_index,
            format!(
                "row has {} signatures but message requires {}",
                row.signature_count, header.num_required_signatures,
            ),
        ));
    }
    let lookups = own_lookups(slot, row.tx_index, lookups, keys)?;
    let loaded_key_count = lookups
        .iter()
        .try_fold(0usize, |total, lookup| {
            total
                .checked_add(lookup.writable_indexes.len())?
                .checked_add(lookup.readonly_indexes.len())
        })
        .ok_or_else(|| tx_error(slot, row.tx_index, "loaded-address count overflow"))?;
    let total_key_count = account_keys
        .len()
        .checked_add(loaded_key_count)
        .ok_or_else(|| tx_error(slot, row.tx_index, "message account count overflow"))?;
    if total_key_count > 256 {
        return Err(tx_error(
            slot,
            row.tx_index,
            "message has more than 256 accounts",
        ));
    }
    let required = header.num_required_signatures as usize;
    let writable_signed = required - header.num_readonly_signed_accounts as usize;
    let writable_unsigned_end = account_keys.len() - header.num_readonly_unsigned_accounts as usize;
    let minimum_balance_count = if matches!(version, CompactMessageVersion::V0) {
        // Loaded writable addresses are appended after the static readonly
        // suffix, so a v0 message cannot use the legacy suffix omission rule.
        total_key_count
    } else if writable_unsigned_end > required {
        writable_unsigned_end
    } else {
        writable_signed
    };
    normalize_balance_oracle_for_message(
        slot,
        row.tx_index,
        total_key_count,
        minimum_balance_count,
        &mut balance_oracle,
    )?;
    let recent_blockhash = own_recent_blockhash(slot, row.tx_index, recent, blockhashes)?;
    let instructions: SmallVec<[CompactInstructionProbe; COMPACT_INLINE_INSTRUCTIONS]> =
        instructions
            .into_iter()
            .enumerate()
            .map(|(index, instruction)| {
                own_instruction(
                    slot,
                    row.tx_index,
                    index,
                    instruction,
                    &account_keys,
                    total_key_count,
                    version,
                )
            })
            .collect::<Result<_, _>>()?;
    Ok(CompactTransactionProbe {
        tx_index: row.tx_index,
        row_flags: row.flags,
        archived_outcome: archived_transaction_outcome(row.flags),
        balance_oracle,
        signature_count: row.signature_count,
        version,
        header,
        account_keys,
        recent_blockhash,
        address_table_lookups: lookups,
        instructions,
    })
}

/// Launch-era Banks appended executable loader-chain accounts to the balance
/// vectors stored in transaction status metadata. Those accounts are absent
/// from the transaction message and cannot be referenced by an instruction.
/// Keep the message-account prefix used by replay, but accept the historical
/// suffix only when every discarded balance is unchanged.
fn normalize_balance_oracle_for_message(
    slot: u64,
    tx_index: u32,
    message_account_count: usize,
    minimum_balance_count: usize,
    balance_oracle: &mut Option<CompactTransactionBalanceOracle>,
) -> Result<(), CompactProbeError> {
    let Some(oracle) = balance_oracle else {
        return Ok(());
    };
    if oracle.pre_balances.len() != oracle.post_balances.len() {
        return Err(tx_error(
            slot,
            tx_index,
            format!(
                "metadata pre/post balance counts disagree: pre={} post={}",
                oracle.pre_balances.len(),
                oracle.post_balances.len(),
            ),
        ));
    }
    if oracle.pre_balances.len() < minimum_balance_count {
        return Err(tx_error(
            slot,
            tx_index,
            format!(
                "metadata balance count is shorter than the writable message prefix: balances={} required={minimum_balance_count} accounts={message_account_count}",
                oracle.pre_balances.len(),
            ),
        ));
    }
    let suffix_start = message_account_count.min(oracle.pre_balances.len());
    if oracle.pre_balances[suffix_start..]
        .iter()
        .zip(&oracle.post_balances[suffix_start..])
        .any(|(pre, post)| pre != post)
    {
        return Err(tx_error(
            slot,
            tx_index,
            "metadata has a changed balance outside the message-account prefix",
        ));
    }
    if oracle.pre_balances.len() > message_account_count {
        oracle.pre_balances.truncate(message_account_count);
        oracle.post_balances.truncate(message_account_count);
    }
    Ok(())
}

struct DecodedCompactInstruction {
    program_id_index: u8,
    accounts: SmallVec<[u8; COMPACT_INLINE_INSTRUCTION_ACCOUNTS]>,
    data: CompactInstructionData,
}

impl From<ArchiveV2HotInstruction> for DecodedCompactInstruction {
    fn from(value: ArchiveV2HotInstruction) -> Self {
        Self {
            program_id_index: value.program_id_index,
            accounts: value.accounts.into(),
            data: value.data.into(),
        }
    }
}

impl From<May24ArchiveV2HotInstruction> for DecodedCompactInstruction {
    fn from(value: May24ArchiveV2HotInstruction) -> Self {
        Self {
            program_id_index: value.program_id_index,
            accounts: value.accounts,
            data: value.data.into(),
        }
    }
}

fn own_account_keys(
    slot: u64,
    tx_index: u32,
    compact_keys: impl IntoIterator<Item = CompactPubkey>,
    keys: &KeyStore,
) -> Result<SmallVec<[[u8; 32]; COMPACT_INLINE_ACCOUNT_KEYS]>, CompactProbeError> {
    compact_keys
        .into_iter()
        .enumerate()
        .map(|(index, key)| {
            resolve_pubkey(key, keys).ok_or_else(|| {
                tx_error(
                    slot,
                    tx_index,
                    format!("static key {index} has an invalid registry id"),
                )
            })
        })
        .collect()
}

impl From<ArchiveV2HotInstructionData> for CompactInstructionData {
    fn from(value: ArchiveV2HotInstructionData) -> Self {
        match value {
            ArchiveV2HotInstructionData::Raw(bytes) => Self::Raw(bytes.into()),
            ArchiveV2HotInstructionData::UnknownSystem(bytes) => Self::UnknownSystem(bytes.into()),
            ArchiveV2HotInstructionData::UnknownVote(bytes) => Self::UnknownVote(bytes.into()),
            ArchiveV2HotInstructionData::ComputeBudget(data) => Self::ComputeBudget(data),
            ArchiveV2HotInstructionData::System(data) => Self::System(data),
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update) => {
                Self::VoteCompactUpdateVoteState(update)
            }
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            } => Self::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            },
            ArchiveV2HotInstructionData::VoteTowerSync(tower) => Self::VoteTowerSync(tower),
            ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            } => Self::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            },
        }
    }
}

impl From<May24ArchiveV2HotInstructionData> for CompactInstructionData {
    fn from(value: May24ArchiveV2HotInstructionData) -> Self {
        match value {
            May24ArchiveV2HotInstructionData::Raw(bytes) => Self::Raw(bytes),
            May24ArchiveV2HotInstructionData::ComputeBudget(data) => Self::ComputeBudget(data),
            May24ArchiveV2HotInstructionData::System(data) => Self::System(data),
            May24ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update) => {
                Self::VoteCompactUpdateVoteState(update)
            }
            May24ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            } => Self::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            },
            May24ArchiveV2HotInstructionData::VoteTowerSync(tower) => Self::VoteTowerSync(tower),
            May24ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            } => Self::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            },
        }
    }
}

fn archived_transaction_outcome(flags: u32) -> CompactArchivedTransactionOutcome {
    if flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0
        || flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0
    {
        CompactArchivedTransactionOutcome::Unknown
    } else if flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0 {
        CompactArchivedTransactionOutcome::Failed
    } else {
        CompactArchivedTransactionOutcome::Succeeded
    }
}

fn own_instruction(
    slot: u64,
    tx_index: u32,
    instruction_index: usize,
    instruction: DecodedCompactInstruction,
    static_keys: &[[u8; 32]],
    total_key_count: usize,
    version: CompactMessageVersion,
) -> Result<CompactInstructionProbe, CompactProbeError> {
    let program_id = static_keys
        .get(instruction.program_id_index as usize)
        .copied()
        .ok_or_else(|| {
            tx_error(
                slot,
                tx_index,
                format!(
                    "instruction {instruction_index} program id index {} is not static",
                    instruction.program_id_index,
                ),
            )
        })?;
    for account_index in &instruction.accounts {
        let index = *account_index as usize;
        if index >= total_key_count {
            return Err(tx_error(
                slot,
                tx_index,
                format!(
                    "instruction {instruction_index} account index {index} exceeds {total_key_count} keys",
                ),
            ));
        }
        if index >= static_keys.len() && matches!(version, CompactMessageVersion::Legacy) {
            return Err(tx_error(
                slot,
                tx_index,
                format!("legacy instruction {instruction_index} references a loaded key",),
            ));
        }
    }
    Ok(CompactInstructionProbe {
        instruction_index: u32::try_from(instruction_index)
            .map_err(|_| tx_error(slot, tx_index, "instruction index exceeds u32"))?,
        program_id_index: instruction.program_id_index,
        program_id,
        account_indexes: instruction.accounts,
        data: instruction.data,
    })
}

fn own_lookups(
    slot: u64,
    tx_index: u32,
    lookups: Vec<OwnedCompactAddressTableLookup>,
    keys: &KeyStore,
) -> Result<Vec<CompactAddressTableLookupProbe>, CompactProbeError> {
    lookups
        .into_iter()
        .enumerate()
        .map(|(index, lookup)| {
            Ok(CompactAddressTableLookupProbe {
                account_key: resolve_pubkey(lookup.account_key, keys).ok_or_else(|| {
                    tx_error(
                        slot,
                        tx_index,
                        format!("address-table lookup {index} has an invalid key id"),
                    )
                })?,
                writable_indexes: lookup.writable_indexes,
                readonly_indexes: lookup.readonly_indexes,
            })
        })
        .collect()
}

fn own_recent_blockhash(
    slot: u64,
    tx_index: u32,
    value: OwnedCompactRecentBlockhash,
    store: &BlockhashStore,
) -> Result<CompactRecentBlockhashProbe, CompactProbeError> {
    match value {
        OwnedCompactRecentBlockhash::Id(id) => store
            .resolve(id)
            .map(|hash| CompactRecentBlockhashProbe::Registry { id, hash })
            .ok_or_else(|| {
                tx_error(
                    slot,
                    tx_index,
                    format!("recent blockhash id {id} is not resolvable",),
                )
            }),
        OwnedCompactRecentBlockhash::Nonce(hash) => Ok(CompactRecentBlockhashProbe::Nonce(hash)),
    }
}

fn validate_message_header(
    slot: u64,
    tx_index: u32,
    header: CompactMessageHeader,
    key_count: usize,
) -> Result<(), CompactProbeError> {
    let required = header.num_required_signatures as usize;
    if required > key_count
        || header.num_readonly_signed_accounts as usize > required
        || header.num_readonly_unsigned_accounts as usize > key_count.saturating_sub(required)
    {
        return Err(tx_error(slot, tx_index, "invalid compact message header"));
    }
    Ok(())
}

fn tx_error(slot: u64, tx_index: u32, message: impl Into<String>) -> CompactProbeError {
    CompactProbeError::Transaction {
        slot,
        tx_index,
        message: message.into(),
    }
}

fn resolve_pubkey(value: CompactPubkey, keys: &KeyStore) -> Option<[u8; 32]> {
    value.resolve(keys)
}

fn own_archive_genesis(
    inline: Option<&WincodeArchiveV2Genesis>,
    genesis_bin: Option<&[u8]>,
    keys: &KeyStore,
) -> Result<Option<CompactGenesisProbe>, CompactProbeError> {
    #[cfg(feature = "genesis")]
    if let Some(bytes) = genesis_bin {
        let inline = inline.ok_or_else(|| {
            CompactProbeError::Genesis("genesis.bin exists without inline genesis identity".into())
        })?;
        let parsed = parse_genesis_bin(bytes)
            .map_err(|error| CompactProbeError::Genesis(format!("parse genesis.bin: {error}")))?;
        return Ok(Some(own_exact_genesis(inline, parsed)));
    }
    let _ = genesis_bin;
    inline
        .map(|value| own_inline_genesis(value, keys))
        .transpose()
}

#[cfg(feature = "genesis")]
fn own_exact_genesis(
    inline: &WincodeArchiveV2Genesis,
    genesis: crate::genesis::GenesisConfig,
) -> CompactGenesisProbe {
    use blockzilla_format::{
        WincodeArchiveV2GenesisEpochSchedule as EpochSchedule,
        WincodeArchiveV2GenesisFeeParams as Fees,
        WincodeArchiveV2GenesisInflationParams as Inflation,
        WincodeArchiveV2GenesisPohParams as Poh, WincodeArchiveV2GenesisRentParams as Rent,
    };
    let accounts = genesis
        .accounts
        .into_iter()
        .map(|entry| CompactGenesisAccount {
            pubkey: entry.pubkey,
            lamports: entry.account.lamports,
            owner: entry.account.owner,
            executable: entry.account.executable,
            rent_epoch: entry.account.rent_epoch,
            data: entry.account.data,
        })
        .collect();
    let reward_pools = genesis
        .reward_pools
        .into_iter()
        .map(|entry| CompactGenesisAccount {
            pubkey: entry.pubkey,
            lamports: entry.account.lamports,
            owner: entry.account.owner,
            executable: entry.account.executable,
            rent_epoch: entry.account.rent_epoch,
            data: entry.account.data,
        })
        .collect();
    let builtins = genesis
        .builtins
        .into_iter()
        .map(|builtin| CompactGenesisBuiltin {
            key: builtin.key,
            pubkey: builtin.pubkey,
        })
        .collect();
    CompactGenesisProbe {
        source: CompactGenesisSource::ExactGenesisBin,
        genesis_hash: inline.genesis_hash,
        genesis_bin_len: inline.genesis_bin_len,
        creation_time_unix: genesis.creation_time_unix,
        cluster_id: genesis.cluster_id,
        ticks_per_slot: genesis.ticks_per_slot,
        slots_per_segment: Some(genesis.slots_per_segment),
        backwards_compat_with_v0_23: Some(genesis.backwards_compat_with_v0_23),
        poh_params: Poh {
            tick_duration_secs: genesis.poh_params.tick_duration_secs,
            tick_duration_nanos: genesis.poh_params.tick_duration_nanos,
            tick_count: genesis.poh_params.tick_count,
            hashes_per_tick: genesis.poh_params.hashes_per_tick,
        },
        fees: Fees {
            target_lamports_per_sig: genesis.fees.target_lamports_per_sig,
            target_sigs_per_slot: genesis.fees.target_sigs_per_slot,
            min_lamports_per_sig: genesis.fees.min_lamports_per_sig,
            max_lamports_per_sig: genesis.fees.max_lamports_per_sig,
            burn_percent: genesis.fees.burn_percent,
        },
        rent: Rent {
            lamports_per_byte_year: genesis.rent.lamports_per_byte_year,
            exemption_threshold: genesis.rent.exemption_threshold,
            burn_percent: genesis.rent.burn_percent,
        },
        inflation: Inflation {
            initial: genesis.inflation.initial,
            terminal: genesis.inflation.terminal,
            taper: genesis.inflation.taper,
            foundation: genesis.inflation.foundation,
            foundation_term: genesis.inflation.foundation_term,
            padding: genesis.inflation.storage.to_le_bytes(),
        },
        inflation_storage: Some(genesis.inflation.storage),
        epoch_schedule: EpochSchedule {
            slots_per_epoch: genesis.epoch_schedule.slots_per_epoch,
            leader_schedule_slot_offset: genesis.epoch_schedule.leader_schedule_slot_offset,
            warmup: genesis.epoch_schedule.warmup,
            first_normal_epoch: genesis.epoch_schedule.first_normal_epoch,
            first_normal_slot: genesis.epoch_schedule.first_normal_slot,
        },
        accounts,
        builtins,
        reward_pools,
    }
}

fn own_inline_genesis(
    genesis: &WincodeArchiveV2Genesis,
    keys: &KeyStore,
) -> Result<CompactGenesisProbe, CompactProbeError> {
    let accounts = genesis
        .accounts
        .iter()
        .enumerate()
        .map(|(index, account)| own_inline_genesis_account(account, keys, "account", index))
        .collect::<Result<Vec<_>, _>>()?;
    let reward_pools = genesis
        .reward_pools
        .iter()
        .enumerate()
        .map(|(index, account)| own_inline_genesis_account(account, keys, "reward pool", index))
        .collect::<Result<Vec<_>, _>>()?;
    let builtins = genesis
        .builtins
        .iter()
        .enumerate()
        .map(|(index, builtin)| {
            Ok(CompactGenesisBuiltin {
                key: builtin.key.clone(),
                pubkey: resolve_pubkey(builtin.pubkey, keys).ok_or_else(|| {
                    CompactProbeError::Genesis(format!("builtin {index} has an invalid pubkey id"))
                })?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CompactGenesisProbe {
        source: CompactGenesisSource::InlineLegacy,
        genesis_hash: genesis.genesis_hash,
        genesis_bin_len: genesis.genesis_bin_len,
        creation_time_unix: genesis.creation_time_unix,
        cluster_id: genesis.cluster_id,
        ticks_per_slot: genesis.ticks_per_slot,
        slots_per_segment: None,
        backwards_compat_with_v0_23: None,
        poh_params: genesis.poh_params.clone(),
        fees: genesis.fees.clone(),
        rent: genesis.rent.clone(),
        inflation: genesis.inflation.clone(),
        inflation_storage: Some(f64::from_le_bytes(genesis.inflation.padding)),
        epoch_schedule: genesis.epoch_schedule.clone(),
        accounts,
        builtins,
        reward_pools,
    })
}

fn own_inline_genesis_account(
    account: &blockzilla_format::WincodeArchiveV2GenesisAccount,
    keys: &KeyStore,
    kind: &str,
    index: usize,
) -> Result<CompactGenesisAccount, CompactProbeError> {
    Ok(CompactGenesisAccount {
        pubkey: resolve_pubkey(account.pubkey, keys).ok_or_else(|| {
            CompactProbeError::Genesis(format!("{kind} {index} has an invalid pubkey id"))
        })?,
        lamports: account.lamports,
        owner: resolve_pubkey(account.owner, keys).ok_or_else(|| {
            CompactProbeError::Genesis(format!("{kind} {index} has an invalid owner id"))
        })?,
        executable: account.executable,
        rent_epoch: account.rent_epoch,
        data: account.data.clone(),
    })
}

#[derive(Debug)]
struct BlockhashStore {
    current_bytes: Vec<u8>,
    previous_tail_bytes: Vec<u8>,
    previous_tail_stride: usize,
}

impl BlockhashStore {
    const HASH_LEN: usize = 32;
    const PREVIOUS_TAIL_ROW_LEN: usize = 40;

    fn load(root: &Path) -> Result<Self, CompactProbeError> {
        let current_path = root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
        let current_bytes =
            fs::read(&current_path).map_err(|error| CompactProbeError::Sidecar {
                path: current_path.clone(),
                message: error.to_string(),
            })?;
        if !current_bytes.len().is_multiple_of(32) {
            return Err(CompactProbeError::Sidecar {
                path: current_path,
                message: format!("length {} is not a multiple of 32", current_bytes.len()),
            });
        }
        let previous_path = root.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
        let previous_bytes = match fs::read(&previous_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(CompactProbeError::Sidecar {
                    path: previous_path,
                    message: error.to_string(),
                });
            }
        };
        let previous_tail_stride = if previous_bytes.is_empty()
            || previous_bytes
                .len()
                .is_multiple_of(Self::PREVIOUS_TAIL_ROW_LEN)
        {
            Self::PREVIOUS_TAIL_ROW_LEN
        } else if previous_bytes.len().is_multiple_of(Self::HASH_LEN) {
            Self::HASH_LEN
        } else {
            return Err(CompactProbeError::Sidecar {
                path: previous_path,
                message: format!(
                    "length {} is neither 40-byte rows nor legacy 32-byte rows",
                    previous_bytes.len(),
                ),
            });
        };
        Ok(Self {
            current_bytes,
            previous_tail_bytes: previous_bytes,
            previous_tail_stride,
        })
    }

    fn resolve(&self, id: i32) -> Option<[u8; 32]> {
        if id >= 0 {
            self.resolve_current(id as u32)
        } else {
            let distance = id.checked_neg()? as usize;
            let index = self.previous_tail_len().checked_sub(distance)?;
            Self::hash_at(&self.previous_tail_bytes, self.previous_tail_stride, index)
        }
    }

    fn resolve_current(&self, id: u32) -> Option<[u8; 32]> {
        Self::hash_at(&self.current_bytes, Self::HASH_LEN, id as usize)
    }

    fn resolve_previous(&self, current_id: u32, previous_id: u32) -> Option<[u8; 32]> {
        if current_id == 0 && self.previous_tail_len() != 0 {
            Self::hash_at(
                &self.previous_tail_bytes,
                self.previous_tail_stride,
                self.previous_tail_len() - 1,
            )
        } else {
            self.resolve_current(previous_id)
        }
    }

    fn previous_tail_len(&self) -> usize {
        self.previous_tail_bytes.len() / self.previous_tail_stride
    }

    fn hash_at(bytes: &[u8], stride: usize, index: usize) -> Option<[u8; 32]> {
        let start = index.checked_mul(stride)?;
        let end = start.checked_add(Self::HASH_LEN)?;
        bytes.get(start..end)?.try_into().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("invalid lowercase hex fixture"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }

    fn manifest_with_message_objects(
        cluster_id: &str,
        epoch: u64,
        blocks_sha256: &str,
        index_sha256: &str,
    ) -> GenerationManifest {
        GenerationManifest {
            schema_version: 1,
            cluster_id: cluster_id.to_owned(),
            epoch,
            generation_id: "test".to_owned(),
            generation_digest: "00".repeat(32),
            slots_per_epoch: 432_000,
            complete: true,
            files: vec![
                blockzilla_read_sdk::manifest::GenerationFile {
                    name: BLOCKS_FILE.to_owned(),
                    size: 0,
                    sha256: blocks_sha256.to_owned(),
                },
                blockzilla_read_sdk::manifest::GenerationFile {
                    name: BLOCK_INDEX_FILE.to_owned(),
                    size: 0,
                    sha256: index_sha256.to_owned(),
                },
            ],
        }
    }

    fn add_message_schema_marker(manifest: &mut GenerationManifest, size: u64, sha256: &str) {
        manifest
            .files
            .push(blockzilla_read_sdk::manifest::GenerationFile {
                name: MAY_24_2026_MESSAGE_SCHEMA_MARKER_FILE.to_owned(),
                size,
                sha256: sha256.to_owned(),
            });
    }

    #[test]
    fn reversed_slot_range_is_rejected() {
        let error = validate_config(CompactProbeConfig {
            start_slot: Some(9),
            end_slot_exclusive: Some(8),
            ..CompactProbeConfig::default()
        })
        .unwrap_err();
        assert!(matches!(
            error,
            CompactProbeError::InvalidRange { start: 9, end: 8 }
        ));
        validate_config(CompactProbeConfig {
            start_slot: Some(9),
            end_slot_exclusive: Some(9),
            ..CompactProbeConfig::default()
        })
        .unwrap();
    }

    #[test]
    fn undecoded_instruction_payloads_expose_original_bytes() {
        let raw = CompactInstructionData::Raw(smallvec::smallvec![1, 2, 3]);
        let system = CompactInstructionData::UnknownSystem(smallvec::smallvec![4, 5]);
        let vote = CompactInstructionData::UnknownVote(smallvec::smallvec![6, 7]);
        let decoded = CompactInstructionData::ComputeBudget(
            blockzilla_format::ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(10),
        );
        assert_eq!(instruction_data_bytes(&raw), Some([1, 2, 3].as_slice()));
        assert_eq!(instruction_data_bytes(&system), Some([4, 5].as_slice()));
        assert_eq!(instruction_data_bytes(&vote), Some([6, 7].as_slice()));
        assert_eq!(instruction_data_bytes(&decoded), None);
    }

    #[test]
    fn pinned_may24_schema_decodes_slot_105368_allocate_with_seed() {
        // Exact epoch-0 slot 105368 tx 2 message bytes from the pinned hot
        // object. Its first instruction is old outer tag 2 (`System`), which
        // the current enum would misread as `UnknownVote(Vec<u8>)`.
        let bytes = decode_hex(
            "0002010206121813150e0d00c0e60c02040202000209ccf1736d29ad6e301871d2d5a34e01709272ebdc60b9b855a31b7c3036fae9360131c80106a1d8179137542a983437bdfe2a7ab2557f535c8a78722b68a49dc0000000000503030201000c030000000080c6a47e8d0300",
        );

        let current: Result<ArchiveV2HotMessagePayload, _> =
            wincode::config::deserialize_exact(&bytes, wincode_leb128_config());
        let current_error = current
            .expect_err("the shifted current enum must not decode historical bytes")
            .to_string();
        assert!(current_error.contains("164162258") || current_error.contains("read"));

        let payload: May24ArchiveV2HotMessagePayload =
            wincode::config::deserialize_exact(&bytes, wincode_leb128_config()).unwrap();
        let encoded = wincode::config::serialize(&payload, wincode_leb128_config()).unwrap();
        assert_eq!(encoded, bytes, "SmallVec wire schema must remain exact");

        let May24ArchiveV2HotMessagePayload::Legacy(message) = payload else {
            panic!("expected legacy transaction message");
        };
        assert!(!message.account_keys.spilled());
        assert!(!message.instructions.spilled());
        assert_eq!(message.instructions.len(), 2);
        assert!(
            message
                .instructions
                .iter()
                .all(|instruction| !instruction.accounts.spilled())
        );
        match &message.instructions[0].data {
            May24ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AllocateWithSeed {
                    base,
                    seed,
                    space,
                    owner,
                },
            ) => {
                let expected_base: [u8; 32] =
                    decode_hex("ccf1736d29ad6e301871d2d5a34e01709272ebdc60b9b855a31b7c3036fae936")
                        .try_into()
                        .unwrap();
                let expected_owner: [u8; 32] =
                    decode_hex("06a1d8179137542a983437bdfe2a7ab2557f535c8a78722b68a49dc000000000")
                        .try_into()
                        .unwrap();
                assert_eq!(*base, expected_base);
                assert_eq!(seed, "1");
                assert_eq!(*space, 200);
                assert_eq!(*owner, expected_owner);
            }
            other => panic!("unexpected first instruction: {other:?}"),
        }
        assert!(matches!(
            &message.instructions[1].data,
            May24ArchiveV2HotInstructionData::Raw(data)
                if data.as_slice() == decode_hex("030000000080c6a47e8d0300").as_slice()
        ));
        let May24ArchiveV2HotInstructionData::Raw(raw) = &message.instructions[1].data else {
            unreachable!("fixture raw instruction was checked above");
        };
        assert!(!raw.spilled());

        let mut trailing = bytes;
        trailing.push(0);
        let trailing_result: Result<May24ArchiveV2HotMessagePayload, _> =
            wincode::config::deserialize_exact(&trailing, wincode_leb128_config());
        assert!(trailing_result.is_err());
    }

    #[test]
    fn historical_message_schema_is_selected_only_for_exact_pinned_identity() {
        let epoch_zero = manifest_with_message_objects(
            "mainnet-beta",
            0,
            MAY_24_2026_MAINNET_EPOCH_0_BLOCKS_SHA256,
            MAY_24_2026_MAINNET_EPOCH_0_INDEX_SHA256,
        );
        assert_eq!(
            message_schema_for_manifest(&epoch_zero).unwrap(),
            CompactMessageSchema::May24_2026PreUnknownInstructionFallbacks
        );

        let epoch_one = manifest_with_message_objects(
            "mainnet-beta",
            1,
            MAY_24_2026_MAINNET_EPOCH_1_BLOCKS_SHA256,
            MAY_24_2026_MAINNET_EPOCH_1_INDEX_SHA256,
        );
        assert_eq!(
            message_schema_for_manifest(&epoch_one).unwrap(),
            CompactMessageSchema::May24_2026PreUnknownInstructionFallbacks
        );

        let mislabeled = manifest_with_message_objects(
            "mainnet-beta",
            1,
            MAY_24_2026_MAINNET_EPOCH_0_BLOCKS_SHA256,
            MAY_24_2026_MAINNET_EPOCH_0_INDEX_SHA256,
        );
        assert!(message_schema_for_manifest(&mislabeled).is_err());

        let partial = manifest_with_message_objects(
            "mainnet-beta",
            0,
            MAY_24_2026_MAINNET_EPOCH_0_BLOCKS_SHA256,
            &"11".repeat(32),
        );
        assert!(message_schema_for_manifest(&partial).is_err());

        let current =
            manifest_with_message_objects("mainnet-beta", 2, &"22".repeat(32), &"33".repeat(32));
        assert_eq!(
            message_schema_for_manifest(&current).unwrap(),
            CompactMessageSchema::Current
        );

        let mut similarly_named = current;
        similarly_named
            .files
            .push(blockzilla_read_sdk::manifest::GenerationFile {
                name: format!("{MAY_24_2026_MESSAGE_SCHEMA_MARKER_FILE}.copy"),
                size: MAY_24_2026_MESSAGE_SCHEMA_MARKER_SIZE,
                sha256: MAY_24_2026_MESSAGE_SCHEMA_MARKER_SHA256.to_owned(),
            });
        assert_eq!(
            message_schema_for_manifest(&similarly_named).unwrap(),
            CompactMessageSchema::Current
        );
    }

    #[test]
    fn historical_message_schema_marker_asset_matches_its_manifest_binding() {
        use sha2::{Digest as _, Sha256};

        assert_eq!(
            MAY_24_2026_MESSAGE_SCHEMA_MARKER_BYTES.len() as u64,
            MAY_24_2026_MESSAGE_SCHEMA_MARKER_SIZE
        );
        let digest: [u8; 32] = Sha256::digest(MAY_24_2026_MESSAGE_SCHEMA_MARKER_BYTES).into();
        assert_eq!(
            digest.as_slice(),
            decode_hex(MAY_24_2026_MESSAGE_SCHEMA_MARKER_SHA256)
        );
    }

    #[test]
    fn exact_message_schema_marker_selects_historical_decoder() {
        let mut manifest =
            manifest_with_message_objects("mainnet-beta", 2, &"22".repeat(32), &"33".repeat(32));
        add_message_schema_marker(
            &mut manifest,
            MAY_24_2026_MESSAGE_SCHEMA_MARKER_SIZE,
            MAY_24_2026_MESSAGE_SCHEMA_MARKER_SHA256,
        );

        assert_eq!(
            message_schema_for_manifest(&manifest).unwrap(),
            CompactMessageSchema::May24_2026PreUnknownInstructionFallbacks
        );
    }

    #[test]
    fn malformed_message_schema_marker_binding_is_rejected() {
        let mut wrong_size =
            manifest_with_message_objects("mainnet-beta", 2, &"22".repeat(32), &"33".repeat(32));
        add_message_schema_marker(
            &mut wrong_size,
            MAY_24_2026_MESSAGE_SCHEMA_MARKER_SIZE + 1,
            MAY_24_2026_MESSAGE_SCHEMA_MARKER_SHA256,
        );
        assert!(
            message_schema_for_manifest(&wrong_size)
                .unwrap_err()
                .contains("malformed May 24 2026 historical message-schema marker binding")
        );

        let mut wrong_digest =
            manifest_with_message_objects("mainnet-beta", 2, &"22".repeat(32), &"33".repeat(32));
        add_message_schema_marker(
            &mut wrong_digest,
            MAY_24_2026_MESSAGE_SCHEMA_MARKER_SIZE,
            &"44".repeat(32),
        );
        assert!(
            message_schema_for_manifest(&wrong_digest)
                .unwrap_err()
                .contains("malformed May 24 2026 historical message-schema marker binding")
        );

        let mut malformed_pinned = manifest_with_message_objects(
            "mainnet-beta",
            0,
            MAY_24_2026_MAINNET_EPOCH_0_BLOCKS_SHA256,
            MAY_24_2026_MAINNET_EPOCH_0_INDEX_SHA256,
        );
        add_message_schema_marker(
            &mut malformed_pinned,
            MAY_24_2026_MESSAGE_SCHEMA_MARKER_SIZE + 1,
            MAY_24_2026_MESSAGE_SCHEMA_MARKER_SHA256,
        );
        assert!(message_schema_for_manifest(&malformed_pinned).is_err());
    }

    #[test]
    fn archived_outcome_requires_decoded_metadata() {
        assert_eq!(
            archived_transaction_outcome(0),
            CompactArchivedTransactionOutcome::Unknown
        );
        assert_eq!(
            archived_transaction_outcome(
                ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
            ),
            CompactArchivedTransactionOutcome::Unknown
        );
        assert_eq!(
            archived_transaction_outcome(ARCHIVE_V2_TX_FLAG_HAS_METADATA),
            CompactArchivedTransactionOutcome::Succeeded
        );
        assert_eq!(
            archived_transaction_outcome(
                ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_ERROR,
            ),
            CompactArchivedTransactionOutcome::Failed
        );
    }

    #[test]
    fn balance_oracle_decodes_only_the_compact_metadata_prefix() {
        let metadata = blockzilla_format::CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: vec![90, 10, 1],
            post_balances: vec![85, 10, 1],
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: Some(123),
            cost_units: Some(456),
        };
        let bytes = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let row = ArchiveV2HotTxRow {
            tx_index: 9,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA,
            message_offset: 0,
            message_len: 0,
            metadata_offset: 0,
            metadata_len: bytes.len().try_into().unwrap(),
            signature_count: 1,
            reserved: [0; 3],
        };

        let oracle = decode_balance_oracle(42, row, &bytes).unwrap().unwrap();
        assert_eq!(oracle.fee, 5_000);
        assert_eq!(oracle.pre_balances.as_slice(), &[90, 10, 1]);
        assert_eq!(oracle.post_balances.as_slice(), &[85, 10, 1]);
        assert!(!oracle.pre_balances.spilled());
        assert!(!oracle.post_balances.spilled());

        let empty_metadata = blockzilla_format::CompactMetaV1 {
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            ..metadata
        };
        let empty_bytes =
            wincode::config::serialize(&empty_metadata, wincode_leb128_config()).unwrap();
        let empty_row = ArchiveV2HotTxRow {
            metadata_len: empty_bytes.len().try_into().unwrap(),
            ..row
        };
        assert!(
            decode_balance_oracle(42, empty_row, &empty_bytes)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn historical_loader_balance_suffix_is_validated_then_dropped() {
        let mut oracle = Some(CompactTransactionBalanceOracle {
            fee: 5_000,
            pre_balances: smallvec::smallvec![393_713_661_360, 26_858_640, 1, 1, 1],
            post_balances: smallvec::smallvec![393_713_656_360, 26_858_640, 1, 1, 1],
        });

        normalize_balance_oracle_for_message(24_005_334, 70, 3, 2, &mut oracle).unwrap();

        let oracle = oracle.unwrap();
        assert_eq!(
            oracle.pre_balances.as_slice(),
            &[393_713_661_360, 26_858_640, 1]
        );
        assert_eq!(
            oracle.post_balances.as_slice(),
            &[393_713_656_360, 26_858_640, 1]
        );
    }

    #[test]
    fn historical_readonly_message_suffix_may_be_absent_from_balances() {
        let mut oracle = Some(CompactTransactionBalanceOracle {
            fee: 5_000,
            pre_balances: smallvec::smallvec![66_747_417_856_252, 8_539_925_000, 1],
            post_balances: smallvec::smallvec![66_746_557_851_252, 9_399_925_000, 1],
        });

        normalize_balance_oracle_for_message(24_005_334, 71, 5, 2, &mut oracle).unwrap();

        let oracle = oracle.unwrap();
        assert_eq!(oracle.pre_balances.len(), 3);
        assert_eq!(oracle.post_balances.len(), 3);
        assert_eq!(
            oracle.pre_balances[0].checked_sub(oracle.post_balances[0]),
            Some(860_005_000)
        );
        assert_eq!(
            oracle.post_balances[1].checked_sub(oracle.pre_balances[1]),
            Some(860_000_000)
        );
    }

    #[test]
    fn malformed_or_changed_loader_balance_suffix_is_rejected() {
        let oracle = |pre, post| {
            Some(CompactTransactionBalanceOracle {
                fee: 5_000,
                pre_balances: SmallVec::from_vec(pre),
                post_balances: SmallVec::from_vec(post),
            })
        };

        let mut unequal = oracle(vec![10, 1, 1], vec![5, 1]);
        assert!(
            normalize_balance_oracle_for_message(1, 2, 2, 2, &mut unequal)
                .unwrap_err()
                .to_string()
                .contains("pre/post balance counts disagree")
        );

        let mut short = oracle(vec![10], vec![5]);
        assert!(
            normalize_balance_oracle_for_message(1, 2, 2, 2, &mut short)
                .unwrap_err()
                .to_string()
                .contains("shorter than the writable message prefix")
        );

        let mut changed_suffix = oracle(vec![10, 1, 7], vec![5, 1, 8]);
        assert!(
            normalize_balance_oracle_for_message(1, 2, 2, 2, &mut changed_suffix)
                .unwrap_err()
                .to_string()
                .contains("changed balance outside the message-account prefix")
        );
    }

    #[test]
    fn raw_transaction_fallback_fails_before_message_decode() {
        let row = ArchiveV2HotTxRow {
            tx_index: 7,
            flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
            message_offset: 0,
            message_len: 0,
            metadata_offset: 0,
            metadata_len: 0,
            signature_count: 0,
            reserved: [0; 3],
        };
        let keys = KeyStore { keys: Vec::new() };
        let blockhashes = BlockhashStore {
            current_bytes: Vec::new(),
            previous_tail_bytes: Vec::new(),
            previous_tail_stride: BlockhashStore::PREVIOUS_TAIL_ROW_LEN,
        };
        let error = own_transaction(
            42,
            row,
            &[],
            &[],
            &keys,
            &blockhashes,
            CompactMessageSchema::Current,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CompactProbeError::RawTransactionFallback {
                slot: 42,
                tx_index: 7
            }
        ));
    }

    #[test]
    fn blockhash_store_resolves_raw_current_and_40_byte_tail_rows() {
        let current_zero = [0x10; 32];
        let current_one = [0x11; 32];
        let previous_zero = [0x20; 32];
        let previous_one = [0x21; 32];

        let mut current_bytes = Vec::new();
        current_bytes.extend_from_slice(&current_zero);
        current_bytes.extend_from_slice(&current_one);
        let mut previous_tail_bytes = Vec::new();
        previous_tail_bytes.extend_from_slice(&previous_zero);
        previous_tail_bytes.extend_from_slice(&[0xa0; 8]);
        previous_tail_bytes.extend_from_slice(&previous_one);
        previous_tail_bytes.extend_from_slice(&[0xa1; 8]);
        let store = BlockhashStore {
            current_bytes,
            previous_tail_bytes,
            previous_tail_stride: BlockhashStore::PREVIOUS_TAIL_ROW_LEN,
        };

        assert_eq!(store.resolve(0), Some(current_zero));
        assert_eq!(store.resolve(1), Some(current_one));
        assert_eq!(store.resolve(2), None);
        assert_eq!(store.resolve(-1), Some(previous_one));
        assert_eq!(store.resolve(-2), Some(previous_zero));
        assert_eq!(store.resolve(-3), None);
        assert_eq!(store.resolve(i32::MIN), None);
        assert_eq!(store.resolve_previous(0, 99), Some(previous_one));
        assert_eq!(store.resolve_previous(1, 0), Some(current_zero));
        assert_eq!(store.resolve_previous(1, 1), Some(current_one));
    }

    #[test]
    fn blockhash_store_resolves_legacy_32_byte_tail_rows() {
        let previous_zero = [0x30; 32];
        let previous_one = [0x31; 32];
        let mut previous_tail_bytes = Vec::new();
        previous_tail_bytes.extend_from_slice(&previous_zero);
        previous_tail_bytes.extend_from_slice(&previous_one);
        let store = BlockhashStore {
            current_bytes: Vec::new(),
            previous_tail_bytes,
            previous_tail_stride: BlockhashStore::HASH_LEN,
        };

        assert_eq!(store.resolve(-1), Some(previous_one));
        assert_eq!(store.resolve(-2), Some(previous_zero));
        assert_eq!(store.resolve(-3), None);
        assert_eq!(store.resolve_current(0), None);
        assert_eq!(store.resolve_previous(0, 0), Some(previous_one));
    }

    #[test]
    fn transaction_buffer_reuses_capacity_and_reserves_only_when_needed() {
        let mut transactions = Vec::with_capacity(4);
        transactions.push(CompactTransactionProbe {
            tx_index: 7,
            row_flags: 0,
            archived_outcome: CompactArchivedTransactionOutcome::Unknown,
            balance_oracle: None,
            signature_count: 1,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: SmallVec::new(),
            recent_blockhash: CompactRecentBlockhashProbe::Nonce([3; 32]),
            address_table_lookups: Vec::new(),
            instructions: SmallVec::new(),
        });
        let pointer = transactions.as_ptr();
        let capacity = transactions.capacity();

        prepare_transaction_buffer(&mut transactions, capacity);
        assert!(transactions.is_empty());
        assert_eq!(transactions.capacity(), capacity);
        assert_eq!(transactions.as_ptr(), pointer);

        prepare_transaction_buffer(&mut transactions, capacity + 1);
        assert!(transactions.capacity() > capacity);
    }

    #[test]
    fn max_slot_bound_reports_unvisited_selected_rows() {
        assert_eq!(bounded_visit_count(10, None), (10, false));
        assert_eq!(bounded_visit_count(10, Some(10)), (10, false));
        assert_eq!(bounded_visit_count(10, Some(3)), (3, true));
        assert_eq!(bounded_visit_count(0, Some(0)), (0, false));
    }

    #[test]
    fn bounded_streaming_prefix_counts_only_visited_compressed_frames() {
        let context = CompactGenerationContext {
            root: PathBuf::from("synthetic"),
            cluster_id: "test".into(),
            epoch: 0,
            generation_id: "generation".into(),
            slots_per_epoch: 32,
            block_count: 3,
            complete: true,
            first_slot: Some(10),
            last_slot: Some(12),
            binding: GenerationBinding {
                generation_digest: [1; 32],
                registry_sha256: [2; 32],
            },
            genesis: None,
        };
        let row = |row_number: u64, slot: u64, next_slot: Option<u64>, compressed_len| {
            Ok((
                row_number,
                next_slot,
                compressed_len,
                DecodedCompactSlot {
                    slot: CompactSlotProbe {
                        block_id: slot as u32,
                        slot,
                        parent_slot: slot - 1,
                        block_time: None,
                        block_height: None,
                        blockhash_id: slot as u32,
                        blockhash: [slot as u8; 32],
                        previous_blockhash_id: slot as u32 - 1,
                        previous_blockhash: [slot as u8 - 1; 32],
                        transaction_count: 0,
                        transactions: Vec::new(),
                    },
                    transactions_scanned: 0,
                    instructions_scanned: 0,
                    program_instruction_counts: SmallVec::new(),
                },
            ))
        };
        let decoded_rows = [
            row(0, 10, Some(11), 101),
            row(1, 11, Some(12), 202),
            row(2, 12, None, 303),
        ];
        let (visit_count, truncated) = bounded_visit_count(decoded_rows.len(), Some(2));
        let summary = drive_compact_visit(
            &context,
            decoded_rows.into_iter().take(visit_count),
            &mut |_| Ok(CompactVisitControl::Continue),
        )
        .unwrap();

        assert!(truncated);
        assert_eq!(summary.slots_visited, 2);
        assert_eq!(summary.compressed_bytes_visited, 303);
    }

    #[test]
    fn streaming_driver_preserves_order_and_stops_after_requested_slot_prefix() {
        let context = CompactGenerationContext {
            root: PathBuf::from("synthetic"),
            cluster_id: "test".into(),
            epoch: 0,
            generation_id: "generation".into(),
            slots_per_epoch: 32,
            block_count: 3,
            complete: true,
            first_slot: Some(10),
            last_slot: Some(12),
            binding: GenerationBinding {
                generation_digest: [1; 32],
                registry_sha256: [2; 32],
            },
            genesis: None,
        };
        let slots = [10, 11, 12];
        let compressed_lengths = [101_u64, 202, 303];
        let decoded_slots: Vec<Result<DecodedCompactRow, CompactProbeError>> = slots
            .into_iter()
            .enumerate()
            .map(|(row_number, slot)| {
                Ok((
                    u64::try_from(row_number).unwrap(),
                    slots.get(row_number + 1).copied(),
                    compressed_lengths[row_number],
                    DecodedCompactSlot {
                        slot: CompactSlotProbe {
                            block_id: slot as u32,
                            slot,
                            parent_slot: slot - 1,
                            block_time: None,
                            block_height: None,
                            blockhash_id: slot as u32,
                            blockhash: [slot as u8; 32],
                            previous_blockhash_id: slot as u32 - 1,
                            previous_blockhash: [slot as u8 - 1; 32],
                            transaction_count: 0,
                            transactions: Vec::new(),
                        },
                        transactions_scanned: 0,
                        instructions_scanned: 1,
                        program_instruction_counts: SmallVec::from_slice(&[([slot as u8; 32], 1)]),
                    },
                ))
            })
            .collect();
        let mut generation_events = 0;
        let mut visited_rows = Vec::new();
        let mut visited_slots = Vec::new();
        let summary = drive_compact_visit(&context, decoded_slots, &mut |event| match event {
            CompactVisitEvent::Generation(context) => {
                generation_events += 1;
                assert_eq!(context.epoch, 0);
                Ok(CompactVisitControl::Continue)
            }
            CompactVisitEvent::Slot {
                context,
                row_number,
                next_slot,
                slot,
            } => {
                assert_eq!(context.generation_id, "generation");
                assert_eq!(next_slot, slots.get(row_number as usize + 1).copied());
                visited_rows.push(row_number);
                visited_slots.push(slot.slot);
                Ok(if slot.slot == 11 {
                    CompactVisitControl::Stop
                } else {
                    CompactVisitControl::Continue
                })
            }
        })
        .unwrap();

        assert_eq!(generation_events, 1);
        assert_eq!(visited_rows, [0, 1]);
        assert_eq!(visited_slots, [10, 11]);
        assert_eq!(summary.slots_visited, 2);
        assert_eq!(summary.instructions_visited, 2);
        assert_eq!(summary.compressed_bytes_visited, 303);
        assert_eq!(summary.program_instruction_counts.len(), 2);
        assert_eq!(summary.program_instruction_counts[&[10; 32]], 1);
        assert_eq!(summary.program_instruction_counts[&[11; 32]], 1);
        assert!(summary.stopped_early);
    }

    #[test]
    fn replay_count_mode_keeps_totals_but_omits_program_histogram() {
        let context = CompactGenerationContext {
            root: PathBuf::from("synthetic"),
            cluster_id: "test".into(),
            epoch: 0,
            generation_id: "generation".into(),
            slots_per_epoch: 32,
            block_count: 1,
            complete: true,
            first_slot: Some(10),
            last_slot: Some(10),
            binding: GenerationBinding {
                generation_digest: [1; 32],
                registry_sha256: [2; 32],
            },
            genesis: None,
        };
        let decoded_slots = [Ok((
            0,
            None,
            4096,
            DecodedCompactSlot {
                slot: CompactSlotProbe {
                    block_id: 10,
                    slot: 10,
                    parent_slot: 9,
                    block_time: None,
                    block_height: None,
                    blockhash_id: 10,
                    blockhash: [10; 32],
                    previous_blockhash_id: 9,
                    previous_blockhash: [9; 32],
                    transaction_count: 2,
                    transactions: Vec::new(),
                },
                transactions_scanned: 2,
                instructions_scanned: 3,
                program_instruction_counts: SmallVec::from_slice(&[([7; 32], 3)]),
            },
        ))];
        let mut generation_events = 0;
        let mut slot_events = 0;
        let summary = drive_compact_visit_with_program_counts(
            &context,
            decoded_slots,
            ProgramCountMode::Skip,
            &mut |event| {
                match event {
                    CompactVisitEvent::Generation(_) => generation_events += 1,
                    CompactVisitEvent::Slot { .. } => slot_events += 1,
                }
                Ok(CompactVisitControl::Continue)
            },
        )
        .unwrap();

        assert_eq!(generation_events, 1);
        assert_eq!(slot_events, 1);
        assert_eq!(summary.slots_visited, 1);
        assert_eq!(summary.transactions_visited, 2);
        assert_eq!(summary.instructions_visited, 3);
        assert_eq!(summary.compressed_bytes_visited, 4096);
        assert!(summary.program_instruction_counts.is_empty());
    }
}
