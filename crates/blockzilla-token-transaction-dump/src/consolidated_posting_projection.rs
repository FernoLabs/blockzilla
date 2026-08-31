//! Allocation-free projection of one consolidated transaction for posting indexes.
//!
//! The transaction record borrows one consolidated frame. This module resolves
//! its complete static-plus-loaded message account list and the distinct set of
//! outer and metadata-recorded inner instruction programs. Per-transaction
//! work uses caller-owned fixed scratch.

use anyhow::{Context, Result, ensure};
use blockzilla_format::{
    ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX, ARCHIVE_V2_TX_FLAG_HAS_ERROR,
    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
    ARCHIVE_V2_TX_FLAG_HAS_LOGS, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA, ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2WireMetadataErrorSchema, CompactPubkey,
};
use blockzilla_read_sdk::{
    ArchiveV2LoadedAddressSide, ArchiveV2MessageProjector, ArchiveV2MetadataProjectionLimits,
    ArchiveV2WireProfile, LogPayloadValidation, MAX_MESSAGE_ACCOUNTS,
    ProjectedArchiveV2MessageAccountSummary, ProjectedArchiveV2TokenMetadataSummary,
    visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema,
};

use crate::{
    consolidated_reader::{
        BorrowedTransactionRecord, ExactMetadataSchemaSelection, select_exact_metadata_schema,
    },
    format::DumpWireProfile,
};

const INDEX_SET_WORDS: usize = MAX_MESSAGE_ACCOUNTS.div_ceil(u64::BITS as usize);
const KNOWN_TRANSACTION_FLAGS: u32 = ARCHIVE_V2_TX_FLAG_HAS_METADATA
    | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
    | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
    | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK
    | ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA
    | ARCHIVE_V2_TX_FLAG_HAS_LOGS
    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
    | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES
    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
    | ARCHIVE_V2_TX_FLAG_HAS_ERROR
    | ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX;
const OPAQUE_TRANSACTION_FLAGS: u32 =
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK;

/// The program occurs in a top-level transaction instruction.
pub const PROGRAM_INSTRUCTION_SCOPE_DIRECT: u8 = 1;
/// The program occurs in a metadata-recorded inner instruction (CPI).
pub const PROGRAM_INSTRUCTION_SCOPE_INNER: u8 = 2;
const PROGRAM_INSTRUCTION_SCOPE_MASK: u8 =
    PROGRAM_INSTRUCTION_SCOPE_DIRECT | PROGRAM_INSTRUCTION_SCOPE_INNER;

/// One distinct program invoked by a consolidated transaction.
///
/// A program can occur in both top-level and inner instructions. In that case,
/// both bits are set in `instruction_scope_mask`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsolidatedProgramPosting {
    pub registry_id: u32,
    pub instruction_scope_mask: u8,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MessageIndexSet {
    words: [u64; INDEX_SET_WORDS],
}

impl MessageIndexSet {
    fn clear(&mut self) {
        self.words.fill(0);
    }

    fn insert(&mut self, index: usize) -> bool {
        if index >= MAX_MESSAGE_ACCOUNTS {
            return false;
        }
        self.words[index / u64::BITS as usize] |= 1u64 << (index % u64::BITS as usize);
        true
    }

    fn contains(self, index: usize) -> bool {
        index < MAX_MESSAGE_ACCOUNTS
            && self.words[index / u64::BITS as usize] & (1u64 << (index % u64::BITS as usize)) != 0
    }

    fn count(self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    fn indices(self, limit: usize) -> impl Iterator<Item = usize> {
        (0..limit.min(MAX_MESSAGE_ACCOUNTS)).filter(move |index| self.contains(*index))
    }

    fn union(self, other: Self) -> Self {
        Self {
            words: std::array::from_fn(|index| self.words[index] | other.words[index]),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MetadataPostingStage {
    inner_program_indices: MessageIndexSet,
    loaded_indices: MessageIndexSet,
    loaded_ids: [u32; MAX_MESSAGE_ACCOUNTS],
    summary: Option<ProjectedArchiveV2TokenMetadataSummary>,
}

impl MetadataPostingStage {
    const fn new() -> Self {
        Self {
            inner_program_indices: MessageIndexSet {
                words: [0; INDEX_SET_WORDS],
            },
            loaded_indices: MessageIndexSet {
                words: [0; INDEX_SET_WORDS],
            },
            loaded_ids: [0; MAX_MESSAGE_ACCOUNTS],
            summary: None,
        }
    }

    fn begin(&mut self) {
        self.inner_program_indices.clear();
        self.loaded_indices.clear();
        self.summary = None;
    }
}

impl PartialEq for MetadataPostingStage {
    fn eq(&self, other: &Self) -> bool {
        self.inner_program_indices == other.inner_program_indices
            && self.loaded_indices == other.loaded_indices
            && self.summary == other.summary
            && (0..MAX_MESSAGE_ACCOUNTS).all(|index| {
                !self.loaded_indices.contains(index)
                    || self.loaded_ids[index] == other.loaded_ids[index]
            })
    }
}

impl Eq for MetadataPostingStage {}

/// Caller-owned storage reused for every consolidated transaction.
///
/// Construction and [`project_consolidated_transaction_postings`] perform no
/// allocation. The scratch remains bound to one admitted registry size.
#[derive(Debug)]
pub struct ConsolidatedPostingProjectionScratch {
    registry_entries: u32,
    static_ids: [u32; MAX_MESSAGE_ACCOUNTS],
    resolved_ids: [u32; MAX_MESSAGE_ACCOUNTS],
    outer_program_indices: MessageIndexSet,
    current_metadata: MetadataPostingStage,
    legacy_metadata: MetadataPostingStage,
    program_postings: [ConsolidatedProgramPosting; MAX_MESSAGE_ACCOUNTS],
    program_count: usize,
}

impl ConsolidatedPostingProjectionScratch {
    /// Create scratch bound to one admitted dump registry.
    pub fn new(registry_entries: u32) -> Result<Self> {
        ensure!(registry_entries != 0, "registry entry count is zero");
        Ok(Self {
            registry_entries,
            static_ids: [0; MAX_MESSAGE_ACCOUNTS],
            resolved_ids: [0; MAX_MESSAGE_ACCOUNTS],
            outer_program_indices: MessageIndexSet::default(),
            current_metadata: MetadataPostingStage::new(),
            legacy_metadata: MetadataPostingStage::new(),
            program_postings: [ConsolidatedProgramPosting::default(); MAX_MESSAGE_ACCOUNTS],
            program_count: 0,
        })
    }

    pub const fn registry_entries(&self) -> u32 {
        self.registry_entries
    }

    fn begin_transaction(&mut self) {
        self.outer_program_indices.clear();
        self.current_metadata.begin();
        self.legacy_metadata.begin();
        self.program_count = 0;
    }
}

/// One exact posting projection that borrows the caller's reusable scratch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsolidatedPostingProjection<'a> {
    /// Complete message account order: static, loaded-writable, loaded-readonly.
    pub resolved_account_registry_ids: &'a [u32],
    /// Distinct programs in ascending registry order, with their instruction scopes.
    pub program_postings: &'a [ConsolidatedProgramPosting],
    pub metadata_schema: ExactMetadataSchemaSelection,
}

/// Resolve the message accounts and distinct instruction programs of one
/// consolidated transaction without per-record allocation.
///
/// Failed transactions are included. Inner programs mean metadata-recorded
/// invocations, independent of transaction success. Callback output is staged
/// until both the message and the selected metadata schema pass exact
/// validation.
pub fn project_consolidated_transaction_postings<'scratch>(
    record: &BorrowedTransactionRecord<'_>,
    registry_entries: u32,
    scratch: &'scratch mut ConsolidatedPostingProjectionScratch,
) -> Result<ConsolidatedPostingProjection<'scratch>> {
    ensure!(
        scratch.registry_entries == registry_entries,
        "posting scratch is bound to a different registry size"
    );
    validate_record_flags(record)?;
    scratch.begin_transaction();

    let mut static_count = 0usize;
    let mut instruction_count = 0usize;
    let mut invalid_static_reference = false;
    let mut invalid_outer_index = false;
    let static_ids = &mut scratch.static_ids;
    let outer_program_indices = &mut scratch.outer_program_indices;
    let message = projector(record.source_wire_profile)
        .visit_static_accounts_and_instructions_exact(
            record.message_bytes,
            registry_entries,
            |ordinal, reference| {
                if ordinal != static_count || ordinal >= static_ids.len() {
                    invalid_static_reference = true;
                    return;
                }
                static_count += 1;
                match reference {
                    CompactPubkey::Id(id) if id != 0 && id <= registry_entries => {
                        static_ids[ordinal] = id;
                    }
                    CompactPubkey::Id(_) | CompactPubkey::Raw(_) => {
                        invalid_static_reference = true;
                    }
                }
            },
            |instruction| {
                instruction_count = instruction_count.saturating_add(1);
                invalid_outer_index |=
                    !outer_program_indices.insert(usize::from(instruction.program_id_index));
            },
        )
        .context("decode exact consolidated transaction message")?;
    ensure!(
        !invalid_static_reference
            && !invalid_outer_index
            && static_count == message.static_account_count
            && instruction_count == message.instruction_count,
        "message callbacks differ from its validated summary"
    );
    validate_message_summary(&message, record.flags, record.signature_count)?;

    let total_accounts = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("resolved message-account count overflow")?;
    ensure!(
        total_accounts <= MAX_MESSAGE_ACCOUNTS,
        "resolved message-account count exceeds its format cap"
    );

    let metadata_schema = {
        let static_ids = &scratch.static_ids;
        let current = &mut scratch.current_metadata;
        let legacy = &mut scratch.legacy_metadata;
        select_exact_metadata_schema(record.metadata_bytes, current, legacy, |stage, schema| {
            project_metadata_stage(
                stage,
                record.metadata_bytes,
                schema,
                &message,
                static_ids,
                registry_entries,
                record.flags,
            )
        })
        .context("select exact consolidated transaction metadata schema")?
    };

    let (loaded_indices, inner_program_indices) = match metadata_schema {
        ExactMetadataSchemaSelection::NoMetadata => {
            validate_absent_metadata(&message, record.flags)?;
            (MessageIndexSet::default(), MessageIndexSet::default())
        }
        ExactMetadataSchemaSelection::LegacyOnly => (
            scratch.legacy_metadata.loaded_indices,
            scratch.legacy_metadata.inner_program_indices,
        ),
        ExactMetadataSchemaSelection::NoError
        | ExactMetadataSchemaSelection::CurrentOnly
        | ExactMetadataSchemaSelection::BothIdentical => (
            scratch.current_metadata.loaded_indices,
            scratch.current_metadata.inner_program_indices,
        ),
    };

    scratch.resolved_ids[..message.static_account_count]
        .copy_from_slice(&scratch.static_ids[..message.static_account_count]);
    let selected_loaded_ids = match metadata_schema {
        ExactMetadataSchemaSelection::LegacyOnly => &scratch.legacy_metadata.loaded_ids,
        ExactMetadataSchemaSelection::NoMetadata => &scratch.current_metadata.loaded_ids,
        ExactMetadataSchemaSelection::NoError
        | ExactMetadataSchemaSelection::CurrentOnly
        | ExactMetadataSchemaSelection::BothIdentical => &scratch.current_metadata.loaded_ids,
    };
    for (offset, (resolved, selected)) in scratch.resolved_ids
        [message.static_account_count..total_accounts]
        .iter_mut()
        .zip(&selected_loaded_ids[message.static_account_count..total_accounts])
        .enumerate()
    {
        let index = message.static_account_count + offset;
        ensure!(
            loaded_indices.contains(index),
            "metadata did not resolve every loaded message account"
        );
        *resolved = *selected;
    }

    let program_account_indices = scratch.outer_program_indices.union(inner_program_indices);
    for index in program_account_indices.indices(total_accounts) {
        let destination = scratch
            .program_postings
            .get_mut(scratch.program_count)
            .context("distinct transaction program position count exceeds message-account cap")?;
        let mut instruction_scope_mask = 0;
        if scratch.outer_program_indices.contains(index) {
            instruction_scope_mask |= PROGRAM_INSTRUCTION_SCOPE_DIRECT;
        }
        if inner_program_indices.contains(index) {
            instruction_scope_mask |= PROGRAM_INSTRUCTION_SCOPE_INNER;
        }
        ensure!(
            instruction_scope_mask != 0
                && instruction_scope_mask & !PROGRAM_INSTRUCTION_SCOPE_MASK == 0,
            "projected program has an invalid instruction scope"
        );
        *destination = ConsolidatedProgramPosting {
            registry_id: scratch.resolved_ids[index],
            instruction_scope_mask,
        };
        scratch.program_count += 1;
    }
    scratch.program_postings[..scratch.program_count].sort_unstable();
    let candidate_count = scratch.program_count;
    let mut read = 0usize;
    let mut write = 0usize;
    while read < candidate_count {
        let candidate = scratch.program_postings[read];
        ensure!(
            candidate.registry_id != 0 && candidate.registry_id <= registry_entries,
            "program registry ID is outside the admitted registry"
        );
        if write == 0 || scratch.program_postings[write - 1].registry_id != candidate.registry_id {
            scratch.program_postings[write] = candidate;
            write += 1;
        } else {
            scratch.program_postings[write - 1].instruction_scope_mask |=
                candidate.instruction_scope_mask;
        }
        read += 1;
    }
    scratch.program_count = write;
    ensure!(
        scratch.program_postings[..scratch.program_count]
            .windows(2)
            .all(|pair| pair[0].registry_id < pair[1].registry_id),
        "projected transaction programs are not unique"
    );

    Ok(ConsolidatedPostingProjection {
        resolved_account_registry_ids: &scratch.resolved_ids[..total_accounts],
        program_postings: &scratch.program_postings[..scratch.program_count],
        metadata_schema,
    })
}

fn projector(profile: DumpWireProfile) -> ArchiveV2MessageProjector {
    ArchiveV2MessageProjector::new(match profile {
        DumpWireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        }
        DumpWireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        }
    })
}

fn validate_record_flags(record: &BorrowedTransactionRecord<'_>) -> Result<()> {
    ensure!(record.signature_count != 0, "transaction has no signatures");
    ensure!(
        !record.message_bytes.is_empty(),
        "transaction message is empty"
    );
    ensure!(
        record.flags & !KNOWN_TRANSACTION_FLAGS == 0,
        "transaction has unknown flags"
    );
    ensure!(
        record.flags & OPAQUE_TRANSACTION_FLAGS == 0,
        "opaque transaction payloads cannot be projected"
    );
    ensure!(
        (record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0) == !record.metadata_bytes.is_empty(),
        "metadata presence flag differs from metadata bytes"
    );
    ensure!(
        (record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
            == (record.metadata_bytes.first() == Some(&1)),
        "transaction error flag differs from metadata bytes"
    );
    Ok(())
}

fn validate_message_summary(
    message: &ProjectedArchiveV2MessageAccountSummary,
    flags: u32,
    signature_count: u8,
) -> Result<()> {
    ensure!(
        message.num_required_signatures == signature_count,
        "message signature count differs from transaction"
    );
    ensure!(
        message.is_v0 == (flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
        "message version differs from transaction flags"
    );
    ensure!(
        message.has_compact_vote_instruction
            == (flags & ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX != 0),
        "compact-vote presence differs from transaction flags"
    );
    Ok(())
}

fn project_metadata_stage(
    stage: &mut MetadataPostingStage,
    bytes: &[u8],
    error_schema: ArchiveV2WireMetadataErrorSchema,
    message: &ProjectedArchiveV2MessageAccountSummary,
    static_ids: &[u32; MAX_MESSAGE_ACCOUNTS],
    registry_entries: u32,
    flags: u32,
) -> Result<()> {
    stage.begin();
    let total_accounts = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("resolved message-account count overflow")?;
    ensure!(
        total_accounts <= MAX_MESSAGE_ACCOUNTS,
        "resolved message-account count exceeds its format cap"
    );

    let mut invalid_inner_index = false;
    let mut invalid_loaded_reference = false;
    let inner_program_indices = &mut stage.inner_program_indices;
    let loaded_indices = &mut stage.loaded_indices;
    let loaded_ids = &mut stage.loaded_ids;
    let summary = visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
        bytes,
        error_schema,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts: total_accounts,
            top_level_instruction_count: message.instruction_count,
        },
        registry_entries,
        LogPayloadValidation::StructureOnly,
        |_, instruction| {
            let Ok(index) = usize::try_from(instruction.program_id_index) else {
                invalid_inner_index = true;
                return;
            };
            invalid_inner_index |= !inner_program_indices.insert(index);
        },
        |_, _| {},
        |side, ordinal, reference| {
            let absolute = match side {
                ArchiveV2LoadedAddressSide::Writable => {
                    message.static_account_count.checked_add(ordinal)
                }
                ArchiveV2LoadedAddressSide::Readonly => message
                    .static_account_count
                    .checked_add(message.expected_loaded_writable)
                    .and_then(|start| start.checked_add(ordinal)),
            };
            let Some(absolute) = absolute.filter(|index| *index < total_accounts) else {
                invalid_loaded_reference = true;
                return;
            };
            match reference {
                CompactPubkey::Id(id) if id != 0 && id <= registry_entries => {
                    if loaded_indices.contains(absolute) && loaded_ids[absolute] != id {
                        invalid_loaded_reference = true;
                        return;
                    }
                    loaded_ids[absolute] = id;
                    invalid_loaded_reference |= !loaded_indices.insert(absolute);
                }
                CompactPubkey::Id(_) | CompactPubkey::Raw(_) => {
                    invalid_loaded_reference = true;
                }
            }
        },
    )?;
    ensure!(
        !invalid_inner_index && !invalid_loaded_reference,
        "metadata contains an unresolved message reference"
    );
    validate_metadata_summary(&summary, message, flags)?;
    let expected_loaded = message
        .expected_loaded_writable
        .checked_add(message.expected_loaded_readonly)
        .context("loaded account count overflow")?;
    ensure!(
        loaded_indices.count() == expected_loaded
            && (message.static_account_count..total_accounts)
                .all(|index| loaded_indices.contains(index) && loaded_ids[index] != 0),
        "metadata did not resolve the exact loaded message-account range"
    );
    ensure!(
        (0..message.static_account_count).all(|index| static_ids[index] != 0),
        "message has an unresolved static account"
    );
    stage.summary = Some(summary);
    Ok(())
}

fn validate_metadata_summary(
    metadata: &ProjectedArchiveV2TokenMetadataSummary,
    message: &ProjectedArchiveV2MessageAccountSummary,
    flags: u32,
) -> Result<()> {
    let has_token_balances =
        metadata.pre_token_balance_count != 0 || metadata.post_token_balance_count != 0;
    let has_loaded = metadata.loaded_writable_count != 0 || metadata.loaded_readonly_count != 0;
    ensure!(
        metadata.has_error == (flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
            && metadata.inner_instructions_present
                == (flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0)
            && metadata.logs_present == (flags & ARCHIVE_V2_TX_FLAG_HAS_LOGS != 0)
            && has_token_balances == (flags & ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES != 0)
            && metadata.return_data_present == (flags & ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA != 0),
        "metadata facts differ from transaction flags"
    );
    ensure!(
        metadata.pre_balance_count == metadata.post_balance_count
            && (metadata.pre_balance_count == 0
                || metadata.pre_balance_count >= message.minimum_balance_accounts),
        "metadata balances cannot cover the writable message-account prefix"
    );
    ensure!(
        metadata.loaded_writable_count == message.expected_loaded_writable
            && metadata.loaded_readonly_count == message.expected_loaded_readonly
            && has_loaded == (flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0),
        "loaded addresses differ from message lookups or transaction flags"
    );
    Ok(())
}

fn validate_absent_metadata(
    message: &ProjectedArchiveV2MessageAccountSummary,
    flags: u32,
) -> Result<()> {
    const METADATA_DERIVED_FLAGS: u32 = ARCHIVE_V2_TX_FLAG_HAS_ERROR
        | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
        | ARCHIVE_V2_TX_FLAG_HAS_LOGS
        | ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA
        | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
    ensure!(
        flags & METADATA_DERIVED_FLAGS == 0,
        "transaction declares metadata facts without metadata"
    );
    ensure!(
        message.expected_loaded_writable == 0 && message.expected_loaded_readonly == 0,
        "message needs loaded addresses but metadata is absent"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cell::Cell,
    };

    use blockzilla_format::{
        ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ArchiveV2HotInstruction, ArchiveV2HotInstructionData,
        ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, ArchiveV2HotV0Message,
        CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1,
        CompactPubkey, CompactTransactionError, OwnedCompactAddressTableLookup,
        OwnedCompactRecentBlockhash, wincode_leb128_config,
    };

    use super::*;
    use crate::format::TokenTransactionBlockContext;

    struct CountingAllocator;

    thread_local! {
        static TRACKING: Cell<bool> = const { Cell::new(false) };
        static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    }

    fn record_allocation() {
        TRACKING.with(|tracking| {
            if tracking.get() {
                ALLOCATIONS.with(|count| count.set(count.get().saturating_add(1)));
            }
        });
    }

    // SAFETY: All operations preserve the system allocator's pointer and
    // layout contract. The thread-local counter is diagnostic only.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record_allocation();
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record_allocation();
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record_allocation();
            unsafe { System.realloc(pointer, layout, new_size) }
        }
    }

    #[global_allocator]
    static TEST_ALLOCATOR: CountingAllocator = CountingAllocator;

    fn count_allocations<T>(operation: impl FnOnce() -> T) -> (T, usize) {
        TRACKING.with(|tracking| assert!(!tracking.replace(true)));
        ALLOCATIONS.with(|count| count.set(0));
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                TRACKING.with(|tracking| tracking.set(false));
            }
        }
        let reset = Reset;
        let value = operation();
        let allocations = ALLOCATIONS.with(Cell::get);
        drop(reset);
        (value, allocations)
    }

    fn instruction(program_id_index: u8) -> ArchiveV2HotInstruction {
        ArchiveV2HotInstruction {
            program_id_index,
            accounts: vec![0],
            data: ArchiveV2HotInstructionData::Raw(vec![1]),
        }
    }

    fn legacy_message(
        keys: Vec<CompactPubkey>,
        instructions: Vec<ArchiveV2HotInstruction>,
    ) -> Vec<u8> {
        wincode::config::serialize(
            &ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: keys,
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions,
            }),
            wincode_leb128_config(),
        )
        .unwrap()
    }

    fn v0_message(
        keys: Vec<CompactPubkey>,
        instructions: Vec<ArchiveV2HotInstruction>,
        writable: usize,
        readonly: usize,
    ) -> Vec<u8> {
        wincode::config::serialize(
            &ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: keys,
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions,
                address_table_lookups: vec![OwnedCompactAddressTableLookup {
                    account_key: CompactPubkey::Id(9),
                    writable_indexes: (0..u8::try_from(writable).unwrap()).collect(),
                    readonly_indexes: (0..u8::try_from(readonly).unwrap()).collect(),
                }],
            }),
            wincode_leb128_config(),
        )
        .unwrap()
    }

    fn metadata(
        error: Option<CompactTransactionError>,
        total_accounts: usize,
        inner: Vec<CompactInnerInstruction>,
        writable: Vec<CompactPubkey>,
        readonly: Vec<CompactPubkey>,
    ) -> CompactMetaV1 {
        CompactMetaV1 {
            err: error,
            fee: 5_000,
            pre_balances: vec![0; total_accounts],
            post_balances: vec![0; total_accounts],
            inner_instructions: (!inner.is_empty()).then_some(vec![CompactInnerInstructions {
                index: 0,
                instructions: inner,
            }]),
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: writable,
            loaded_readonly_addresses: readonly,
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn serialize_current(value: &CompactMetaV1) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn serialize_legacy_error(successful_tail: &CompactMetaV1) -> Vec<u8> {
        let successful = serialize_current(successful_tail);
        let stored_account_in_use = vec![0, 0, 0, 0];
        let mut legacy =
            wincode::config::serialize(&Some(stored_account_in_use), wincode_leb128_config())
                .unwrap();
        legacy.extend_from_slice(&successful[1..]);
        legacy
    }

    fn record<'a>(
        message_bytes: &'a [u8],
        metadata_bytes: &'a [u8],
        flags: u32,
        profile: DumpWireProfile,
    ) -> BorrowedTransactionRecord<'a> {
        BorrowedTransactionRecord {
            source_epoch: 801,
            source_generation_digest: [7; 32],
            source_wire_profile: profile,
            source_block_id: 1,
            block: TokenTransactionBlockContext {
                slot: 346_066_298,
                parent_slot: 346_066_297,
                blockhash_id: 2,
                previous_blockhash_id: 1,
                block_time: Some(1),
                block_height: Some(2),
                transaction_count: 1,
            },
            tx_index: 0,
            flags,
            source_first_signature_ordinal: 0,
            signature_count: 1,
            dump_signature_ordinal: Some(0),
            message_bytes,
            metadata_bytes,
        }
    }

    fn inner(program_id_index: u32) -> CompactInnerInstruction {
        CompactInnerInstruction {
            program_id_index,
            accounts: vec![0],
            data: vec![1],
            stack_height: Some(2),
        }
    }

    #[test]
    fn current_and_legacy_error_schemas_project_the_same_semantics() {
        let message = legacy_message(
            vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
            ],
            vec![instruction(1)],
        );
        let current_value = metadata(
            Some(CompactTransactionError::AccountInUse),
            3,
            vec![inner(2)],
            Vec::new(),
            Vec::new(),
        );
        let current = serialize_current(&current_value);
        let successful = metadata(None, 3, vec![inner(2)], Vec::new(), Vec::new());
        let legacy = serialize_legacy_error(&successful);
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_HAS_ERROR
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
        let mut scratch = ConsolidatedPostingProjectionScratch::new(10).unwrap();

        let current_projection = project_consolidated_transaction_postings(
            &record(
                &message,
                &current,
                flags,
                DumpWireProfile::PostUnknownInstructionFallbacksV1,
            ),
            10,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(
            current_projection.metadata_schema,
            ExactMetadataSchemaSelection::CurrentOnly
        );
        assert_eq!(current_projection.resolved_account_registry_ids, [1, 2, 3]);
        assert_eq!(
            current_projection.program_postings,
            [
                ConsolidatedProgramPosting {
                    registry_id: 2,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_DIRECT,
                },
                ConsolidatedProgramPosting {
                    registry_id: 3,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_INNER,
                },
            ]
        );

        let legacy_projection = project_consolidated_transaction_postings(
            &record(
                &message,
                &legacy,
                flags,
                DumpWireProfile::PostUnknownInstructionFallbacksV1,
            ),
            10,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(
            legacy_projection.metadata_schema,
            ExactMetadataSchemaSelection::LegacyOnly
        );
        assert_eq!(legacy_projection.resolved_account_registry_ids, [1, 2, 3]);
        assert_eq!(
            legacy_projection.program_postings,
            [
                ConsolidatedProgramPosting {
                    registry_id: 2,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_DIRECT,
                },
                ConsolidatedProgramPosting {
                    registry_id: 3,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_INNER,
                },
            ]
        );
    }

    #[test]
    fn loaded_inner_program_is_resolved_in_message_order() {
        let message = v0_message(
            vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            vec![instruction(1)],
            1,
            1,
        );
        let metadata = serialize_current(&metadata(
            None,
            4,
            vec![inner(2), inner(3)],
            vec![CompactPubkey::Id(3)],
            vec![CompactPubkey::Id(4)],
        ));
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
        let mut scratch = ConsolidatedPostingProjectionScratch::new(10).unwrap();
        let projection = project_consolidated_transaction_postings(
            &record(
                &message,
                &metadata,
                flags,
                DumpWireProfile::PostUnknownInstructionFallbacksV1,
            ),
            10,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(projection.resolved_account_registry_ids, [1, 2, 3, 4]);
        assert_eq!(
            projection.program_postings,
            [
                ConsolidatedProgramPosting {
                    registry_id: 2,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_DIRECT,
                },
                ConsolidatedProgramPosting {
                    registry_id: 3,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_INNER,
                },
                ConsolidatedProgramPosting {
                    registry_id: 4,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_INNER,
                },
            ]
        );
    }

    #[test]
    fn duplicate_program_occurrences_and_account_positions_are_deduplicated() {
        let message = v0_message(
            vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            vec![instruction(1), instruction(1)],
            1,
            0,
        );
        let metadata = serialize_current(&metadata(
            None,
            3,
            (0..300).map(|_| inner(2)).collect(),
            vec![CompactPubkey::Id(2)],
            Vec::new(),
        ));
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
        let mut scratch = ConsolidatedPostingProjectionScratch::new(10).unwrap();
        let projection = project_consolidated_transaction_postings(
            &record(
                &message,
                &metadata,
                flags,
                DumpWireProfile::PostUnknownInstructionFallbacksV1,
            ),
            10,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(projection.resolved_account_registry_ids, [1, 2, 2]);
        assert_eq!(
            projection.program_postings,
            [ConsolidatedProgramPosting {
                registry_id: 2,
                instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_DIRECT
                    | PROGRAM_INSTRUCTION_SCOPE_INNER,
            }]
        );
    }

    #[test]
    fn absent_metadata_accepts_a_legacy_message_without_loaded_accounts() {
        let message = legacy_message(
            vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            vec![instruction(1)],
        );
        let mut scratch = ConsolidatedPostingProjectionScratch::new(10).unwrap();
        let projection = project_consolidated_transaction_postings(
            &record(
                &message,
                &[],
                0,
                DumpWireProfile::PostUnknownInstructionFallbacksV1,
            ),
            10,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(
            projection.metadata_schema,
            ExactMetadataSchemaSelection::NoMetadata
        );
        assert_eq!(projection.resolved_account_registry_ids, [1, 2]);
        assert_eq!(
            projection.program_postings,
            [ConsolidatedProgramPosting {
                registry_id: 2,
                instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_DIRECT,
            }]
        );
    }

    #[test]
    fn raw_and_out_of_range_references_fail_closed() {
        let raw_message = legacy_message(
            vec![CompactPubkey::Id(1), CompactPubkey::Raw([2; 32])],
            vec![instruction(1)],
        );
        let mut scratch = ConsolidatedPostingProjectionScratch::new(10).unwrap();
        assert!(
            project_consolidated_transaction_postings(
                &record(
                    &raw_message,
                    &[],
                    0,
                    DumpWireProfile::PostUnknownInstructionFallbacksV1,
                ),
                10,
                &mut scratch,
            )
            .is_err()
        );

        let message = v0_message(
            vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            vec![instruction(1)],
            1,
            0,
        );
        let metadata = serialize_current(&metadata(
            None,
            3,
            Vec::new(),
            vec![CompactPubkey::Raw([3; 32])],
            Vec::new(),
        ));
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
        assert!(
            project_consolidated_transaction_postings(
                &record(
                    &message,
                    &metadata,
                    flags,
                    DumpWireProfile::PostUnknownInstructionFallbacksV1,
                ),
                10,
                &mut scratch,
            )
            .is_err()
        );
    }

    #[test]
    fn valid_projection_allocates_nothing_per_record() {
        let message = v0_message(
            vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            vec![instruction(1), instruction(1)],
            1,
            0,
        );
        let metadata = serialize_current(&metadata(
            None,
            3,
            (0..300).map(|_| inner(2)).collect(),
            vec![CompactPubkey::Id(3)],
            Vec::new(),
        ));
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
        let record = record(
            &message,
            &metadata,
            flags,
            DumpWireProfile::PostUnknownInstructionFallbacksV1,
        );
        let _ = count_allocations(|| ());
        let (scratch, scratch_allocations) =
            count_allocations(|| ConsolidatedPostingProjectionScratch::new(10).unwrap());
        assert_eq!(scratch_allocations, 0);
        let mut scratch = scratch;
        let ((account_count, program_count, checksum), allocations) = count_allocations(|| {
            let projection =
                project_consolidated_transaction_postings(&record, 10, &mut scratch).unwrap();
            (
                projection.resolved_account_registry_ids.len(),
                projection.program_postings.len(),
                projection
                    .resolved_account_registry_ids
                    .iter()
                    .copied()
                    .chain(
                        projection
                            .program_postings
                            .iter()
                            .map(|posting| posting.registry_id),
                    )
                    .sum::<u32>(),
            )
        });
        assert_eq!((account_count, program_count, checksum), (3, 2, 11));
        assert_eq!(allocations, 0);
    }
}
