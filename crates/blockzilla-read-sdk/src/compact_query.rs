//! Sequential source-neutral instruction projection for Compact V2 archives.
//!
//! This is the reference adapter. It uses the admitted `ArchiveReader`, keeps
//! bounded registry chunks across the scan, reads each block signature window
//! once, and publishes only through `OrderedBlockPublisher`.

use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
};

use blockzilla_format::{
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, ArchiveV2HotTxRow,
    CompactPubkey, OwnedCompactRecentBlockhash,
};
use blockzilla_query_sdk::{
    ArchiveFormat, ArchiveInstructionSource, BlockHeader, BlockSink, CanonicalBlock,
    CanonicalTransaction, CoverageReason, CpiCoverage, Error as QueryError, ExecutionStatus,
    InstructionCoordinate, InstructionCoverage, InstructionDataCoverage,
    InstructionDataRequirement, OrderedBlockPublisher, ResolvedInstruction, ScanIoReceipt,
    ScanReceipt, ScanRequest, SourceIdentity, SourceVerification, TransactionHeader,
};
use thiserror::Error;

use crate::{
    ArchiveReader, ArchiveReaderSourceKind, BlockhashResolver, BlockhashResolverError,
    CompactV2ExecutionStatus, CompactV2MessageProjectionError, CompactV2MessageProjector,
    CompactV2MetadataProjectionError, CompactV2MetadataProjectionLimits,
    CompactV2MetadataProjector, MAX_BLOCKHASH_REGISTRY_BYTES,
    MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS, MAX_VOTE_HASH_REGISTRY_BYTES, PreviousBlockhashTail,
    PreviousBlockhashTailSchema, ProjectedCompactV2Message, ProjectedCompactV2MessageVersion,
    RangeSource, SignedInstructionCandidates, SignedMessageCandidates, SignedMessageError,
    SignedMessageVersion, VoteHashRegistry, VoteHashResolver, parse_previous_blockhash_tail,
};

const REGISTRY_KEY_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 64;
const REGISTRY_KEYS_PER_CHUNK: usize = 2_048;
const REGISTRY_CACHE_CHUNKS: usize = 8;
const PREVIOUS_BLOCKHASH_RECORDS: usize = 300;
const MAX_SIGNATURE_BYTES_PER_BLOCK: usize = 256 * 1024 * 1024;

/// Maximum retained public-key payload bytes in the registry chunk cache.
///
/// This value does not include `HashMap`, `Vec`, or allocator overhead.
pub const COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES: usize =
    REGISTRY_KEYS_PER_CHUNK * REGISTRY_KEY_BYTES * REGISTRY_CACHE_CHUNKS;

#[derive(Debug, Error)]
pub enum CompactV2InstructionSourceError {
    #[error("Compact V2 reader error: {0}")]
    Reader(#[from] crate::Error),

    #[error("Compact V2 range source error: {0}")]
    RangeSource(#[from] crate::SourceError),

    #[error("Compact V2 message projection error: {0}")]
    Message(#[from] CompactV2MessageProjectionError),

    #[error("Compact V2 metadata projection error: {0}")]
    Metadata(#[from] CompactV2MetadataProjectionError),

    #[error("Compact V2 signed-message error: {0}")]
    SignedMessage(#[from] SignedMessageError),

    #[error("Compact V2 blockhash error: {0}")]
    Blockhash(#[from] BlockhashResolverError),

    #[error("Compact V2 sidecar {object} is required for {purpose}")]
    MissingSidecar {
        object: &'static str,
        purpose: &'static str,
    },

    #[error("invalid Compact V2 instruction source: {0}")]
    Invalid(String),
}

pub type CompactV2InstructionSourceResult<T> =
    std::result::Result<T, CompactV2InstructionSourceError>;

/// A sequential `ArchiveInstructionSource` over one admitted Compact V2 reader.
///
/// `first_slot` is explicit. The Compact V2 generation manifest records
/// `epoch` and `slots_per_epoch`, but it does not record a warm-up-aware first
/// slot. The adapter never derives this value with `epoch * slots_per_epoch`.
/// When `signatures.bin` exists, the adapter reads one signature window for
/// each non-empty block so it can publish `primary_signature`. This read also
/// supplies the proof used only when selected instruction data is ambiguous.
#[derive(Debug)]
pub struct CompactV2InstructionSource<S> {
    reader: ArchiveReader<S>,
    identity: SourceIdentity,
    context: ExactContext,
}

impl<S: RangeSource> CompactV2InstructionSource<S> {
    pub fn new(
        reader: ArchiveReader<S>,
        first_slot: u64,
    ) -> CompactV2InstructionSourceResult<Self> {
        let manifest = reader.manifest();
        let block_count = u32::try_from(reader.index().rows.len()).map_err(|_| {
            CompactV2InstructionSourceError::Invalid(
                "block row count exceeds the source-neutral u32 limit".into(),
            )
        })?;
        let last_slot = first_slot
            .checked_add(manifest.slots_per_epoch.saturating_sub(1))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "explicit epoch slot range overflows u64".into(),
                )
            })?;
        if manifest.slots_per_epoch == 0 {
            return Err(CompactV2InstructionSourceError::Invalid(
                "slots_per_epoch is zero".into(),
            ));
        }
        if let Some(row) = reader
            .index()
            .rows
            .iter()
            .find(|row| row.slot < first_slot || row.slot > last_slot)
        {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "block slot {} is outside explicit epoch slots {first_slot}..={last_slot}",
                row.slot
            )));
        }

        let (verification, binding) = match reader.source_kind() {
            ArchiveReaderSourceKind::PublishedManifest => (
                SourceVerification::PublishedManifest,
                Some(manifest.generation_digest.clone()),
            ),
            ArchiveReaderSourceKind::OperatorTrusted => (SourceVerification::OperatorTrusted, None),
        };
        let identity = SourceIdentity {
            format: ArchiveFormat::CompactV2,
            label: manifest.generation_id.clone(),
            cluster_id: Some(manifest.cluster_id.clone()),
            epoch: manifest.epoch,
            first_slot,
            slots_per_epoch: manifest.slots_per_epoch,
            block_count,
            verification,
            binding,
        };

        Ok(Self {
            reader,
            identity,
            context: ExactContext::default(),
        })
    }

    pub const fn reader(&self) -> &ArchiveReader<S> {
        &self.reader
    }

    pub fn into_reader(self) -> ArchiveReader<S> {
        self.reader
    }

    fn scan_inner(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt> {
        let identity = self.identity.clone();
        let mut publisher = OrderedBlockPublisher::new(&identity, request, sink)?;
        let start = request
            .range
            .map_or(0usize, |range| range.first_block as usize);
        let end = request
            .range
            .map_or(self.identity.block_count as usize, |range| {
                usize::try_from(
                    range
                        .first_block
                        .checked_add(range.block_count.get())
                        .expect("OrderedBlockPublisher validated the requested u32 range"),
                )
                .expect("u32 fits the supported address space")
            });
        let reader = &self.reader;
        let context = &mut self.context;
        let context_io_before = context.io;
        let mut blocks = reader
            .borrowed_blocks_without_rewards_range(Range { start, end })
            .map_err(source_error)?;

        while let Some(block) = blocks.next_block() {
            let block = block.map_err(source_error)?;
            let source_row = block.index_row;
            let signatures = context
                .read_block_signatures(reader, &source_row)
                .map_err(source_error)?;
            let mut signature_cursor = 0usize;
            let mut transactions = Vec::with_capacity(block.tx_rows_len());

            for row in block.tx_rows() {
                let transaction_signatures = match signatures.as_deref() {
                    Some(signatures) => {
                        let end = signature_cursor
                            .checked_add(usize::from(row.signature_count))
                            .filter(|end| *end <= signatures.len())
                            .ok_or_else(|| {
                                source_error(CompactV2InstructionSourceError::Invalid(format!(
                                    "slot {} transaction {} signature range exceeds its block window",
                                    source_row.slot, row.tx_index
                                )))
                            })?;
                        let selected = &signatures[signature_cursor..end];
                        signature_cursor = end;
                        Some(selected)
                    }
                    None => None,
                };
                let transaction = Self::project_transaction(
                    reader,
                    context,
                    request,
                    source_row.slot,
                    row,
                    block.message_bytes(),
                    block.metadata_bytes(),
                    transaction_signatures,
                )
                .map_err(source_error)?;
                transactions.push(transaction);
            }
            if let Some(signatures) = &signatures
                && signature_cursor != signatures.len()
            {
                return Err(source_error(CompactV2InstructionSourceError::Invalid(
                    format!(
                        "slot {} rows consume {signature_cursor} of {} block signatures",
                        source_row.slot,
                        signatures.len()
                    ),
                )));
            }

            let canonical = CanonicalBlock {
                header: BlockHeader {
                    epoch: identity.epoch,
                    block_ordinal: source_row.block_id,
                    slot: source_row.slot,
                },
                transactions,
            };
            publisher.publish(&canonical)?;
        }

        let block_io = blocks.io_stats();
        let context_io = context
            .io
            .difference(context_io_before)
            .map_err(source_error)?;
        publisher.set_io_receipt(ScanIoReceipt {
            source_read_calls: Some(
                block_io
                    .source_read_calls
                    .checked_add(context_io.calls)
                    .ok_or_else(|| {
                        source_error(CompactV2InstructionSourceError::Invalid(
                            "scan source-read count overflow".into(),
                        ))
                    })?,
            ),
            source_read_bytes: Some(
                block_io
                    .source_read_bytes
                    .checked_add(context_io.bytes)
                    .ok_or_else(|| {
                        source_error(CompactV2InstructionSourceError::Invalid(
                            "scan source-read byte count overflow".into(),
                        ))
                    })?,
            ),
            decoded_bytes: Some(block_io.decoded_bytes),
            cache_read_calls: None,
            cache_read_bytes: None,
        });
        publisher.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn project_transaction(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        request: &ScanRequest,
        slot: u64,
        row: ArchiveV2HotTxRow,
        message_lane: &[u8],
        metadata_lane: &[u8],
        signatures: Option<&[[u8; 64]]>,
    ) -> CompactV2InstructionSourceResult<CanonicalTransaction> {
        let primary_signature = signatures
            .and_then(|signatures| signatures.first())
            .copied();
        if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0
            && row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0
        {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "slot {slot} transaction {} has METADATA_RAW_FALLBACK without HAS_METADATA",
                row.tx_index
            )));
        }
        if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            return Ok(CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: row.tx_index,
                    status: ExecutionStatus::Unknown(CoverageReason::RawTransaction),
                    failed_outer_instruction_index: None,
                    instruction_coverage: InstructionCoverage::Unknown(
                        CoverageReason::RawTransaction,
                    ),
                    cpi_coverage: CpiCoverage::Unknown(CoverageReason::RawTransaction),
                },
                primary_signature,
                required_signers: Vec::new(),
                instructions: Vec::new(),
            });
        }

        let message_bytes = lane_region(message_lane, row.message_offset, row.message_len)?;
        let projector =
            CompactV2MessageProjector::new(reader.message_schema(), reader.registry_entries());
        let (message, static_keys) = Self::project_requested_message(
            reader,
            context,
            projector,
            message_bytes,
            &request.instruction_data,
            !request.require_complete_instruction_data,
        )?;
        let message_is_v0 = matches!(
            message.version(),
            ProjectedCompactV2MessageVersion::V0 { .. }
        );
        if message_is_v0 != (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0) {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "slot {slot} transaction {} message version differs from its row flags",
                row.tx_index
            )));
        }
        if row.signature_count != message.header().num_required_signatures {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "slot {slot} transaction {} has {} signature rows but requires {}",
                row.tx_index,
                row.signature_count,
                message.header().num_required_signatures
            )));
        }
        if let Some(signatures) = signatures
            && signatures.len() != usize::from(row.signature_count)
        {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "slot {slot} transaction {} signature window has the wrong length",
                row.tx_index
            )));
        }

        let metadata = if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            reject_set_flag(row, ARCHIVE_V2_TX_FLAG_HAS_ERROR, "HAS_ERROR")?;
            reject_set_flag(row, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, "HAS_INNER_IX")?;
            reject_set_flag(
                row,
                ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                "HAS_LOADED_ADDRESSES",
            )?;
            ProjectedMetadata::Absent
        } else if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
            ProjectedMetadata::Raw
        } else {
            let bytes = lane_region(metadata_lane, row.metadata_offset, row.metadata_len)?;
            let limits = CompactV2MetadataProjectionLimits::for_message(&message);
            let metadata = CompactV2MetadataProjector::new(
                reader.metadata_schema(),
                reader.registry_entries(),
            )
            .project(bytes, limits)?;
            require_flag_state(
                row,
                ARCHIVE_V2_TX_FLAG_HAS_ERROR,
                "HAS_ERROR",
                !metadata.execution_status.is_success(),
            )?;
            require_flag_state(
                row,
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                "HAS_INNER_IX",
                metadata.inner_instructions.is_some(),
            )?;
            require_flag_state(
                row,
                ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                "HAS_LOADED_ADDRESSES",
                !metadata.loaded_writable_addresses.is_empty()
                    || !metadata.loaded_readonly_addresses.is_empty(),
            )?;
            ProjectedMetadata::Exact(metadata)
        };

        let (status, failed_outer_instruction_index, cpi_coverage) = match &metadata {
            ProjectedMetadata::Absent => (
                ExecutionStatus::Unknown(CoverageReason::MetadataAbsent),
                None,
                CpiCoverage::Unknown(CoverageReason::MetadataAbsent),
            ),
            ProjectedMetadata::Raw => (
                ExecutionStatus::Unknown(CoverageReason::RawMetadata),
                None,
                CpiCoverage::Unknown(CoverageReason::RawMetadata),
            ),
            ProjectedMetadata::Exact(metadata) => {
                let (status, failed) = match metadata.execution_status {
                    CompactV2ExecutionStatus::Succeeded => (ExecutionStatus::Succeeded, None),
                    CompactV2ExecutionStatus::Failed {
                        failed_outer_instruction_index,
                    } => (
                        ExecutionStatus::Failed,
                        failed_outer_instruction_index.map(u32::from),
                    ),
                };
                let cpi = if metadata.inner_instructions.is_some() {
                    CpiCoverage::Complete
                } else {
                    CpiCoverage::NotRecorded
                };
                (status, failed, cpi)
            }
        };

        let loaded_keys = match &metadata {
            ProjectedMetadata::Exact(metadata) => {
                let mut keys = Vec::with_capacity(
                    metadata.loaded_writable_addresses.len()
                        + metadata.loaded_readonly_addresses.len(),
                );
                for reference in metadata
                    .loaded_writable_addresses
                    .iter()
                    .chain(&metadata.loaded_readonly_addresses)
                {
                    keys.push(context.resolve_pubkey(reader, *reference)?);
                }
                Some(keys)
            }
            ProjectedMetadata::Absent | ProjectedMetadata::Raw
                if message.expected_loaded_addresses() == 0 =>
            {
                Some(Vec::new())
            }
            ProjectedMetadata::Absent | ProjectedMetadata::Raw => None,
        };

        let (instruction_coverage, instructions) = if let Some(loaded_keys) = loaded_keys {
            let mut account_keys = static_keys;
            account_keys.extend(loaded_keys);
            let instructions = Self::project_instructions(
                reader,
                context,
                request,
                message_bytes,
                &message,
                &metadata,
                &account_keys,
                signatures,
            )?;
            (InstructionCoverage::Complete, instructions)
        } else {
            let reason = match metadata {
                ProjectedMetadata::Absent => CoverageReason::MetadataAbsent,
                ProjectedMetadata::Raw => CoverageReason::RawMetadata,
                ProjectedMetadata::Exact(_) => unreachable!("exact metadata supplied loaded keys"),
            };
            (InstructionCoverage::Unknown(reason), Vec::new())
        };

        let required = usize::from(message.header().num_required_signatures);
        let required_signers = static_keys_prefix(&message, required)?
            .iter()
            .map(|reference| context.resolve_pubkey(reader, *reference))
            .collect::<CompactV2InstructionSourceResult<Vec<_>>>()?;

        Ok(CanonicalTransaction {
            header: TransactionHeader {
                tx_index: row.tx_index,
                status,
                failed_outer_instruction_index,
                instruction_coverage,
                cpi_coverage,
            },
            primary_signature,
            required_signers,
            instructions,
        })
    }

    fn project_requested_message<'a>(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        requirement: &InstructionDataRequirement,
        relaxed: bool,
    ) -> CompactV2InstructionSourceResult<(ProjectedCompactV2Message<'a>, Vec<[u8; 32]>)> {
        match requirement {
            InstructionDataRequirement::All => {
                let message =
                    Self::project_all_with_vote_retry(reader, context, projector, bytes, relaxed)?;
                let static_keys = Self::resolve_static_keys(reader, context, &message)?;
                Ok((message, static_keys))
            }
            InstructionDataRequirement::None => {
                let message =
                    projector.project_with_instruction_data_for_programs(bytes, &[], None)?;
                let static_keys = Self::resolve_static_keys(reader, context, &message)?;
                Ok((message, static_keys))
            }
            InstructionDataRequirement::Programs(programs) => {
                let unselected =
                    projector.project_with_instruction_data_for_programs(bytes, &[], None)?;
                let static_keys = Self::resolve_static_keys(reader, context, &unselected)?;
                let mut selected_references = Vec::new();
                for instruction in unselected.instructions() {
                    let index = usize::from(instruction.program_id_index());
                    let program = *static_keys.get(index).ok_or_else(|| {
                        CompactV2InstructionSourceError::Invalid(
                            "projected program index is outside static keys".into(),
                        )
                    })?;
                    if programs.contains(&program) {
                        let reference = unselected.static_account_keys()[index];
                        if !selected_references.contains(&reference) {
                            selected_references.push(reference);
                        }
                    }
                }
                let message = Self::project_selected_with_vote_retry(
                    reader,
                    context,
                    projector,
                    bytes,
                    &selected_references,
                    relaxed,
                )?;
                Ok((message, static_keys))
            }
        }
    }

    fn resolve_static_keys(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        message: &ProjectedCompactV2Message<'_>,
    ) -> CompactV2InstructionSourceResult<Vec<[u8; 32]>> {
        message
            .static_account_keys()
            .iter()
            .map(|reference| context.resolve_pubkey(reader, *reference))
            .collect()
    }

    fn project_selected_with_vote_retry<'a>(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        programs: &[CompactPubkey],
        relaxed: bool,
    ) -> CompactV2InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        let first = projector.project_with_instruction_data_for_programs(bytes, programs, None);
        match first {
            Err(error) if needs_vote_hashes(&error) => {
                context.load_vote_hashes(reader)?;
                let retried = projector.project_with_instruction_data_for_programs(
                    bytes,
                    programs,
                    context.vote_hashes(),
                );
                match retried {
                    Err(error) if relaxed && is_relaxable_projection_error(&error) => Ok(projector
                        .project_with_instruction_data_for_programs_relaxed(
                            bytes,
                            programs,
                            context.vote_hashes(),
                        )?),
                    result => Ok(result?),
                }
            }
            Err(error) if relaxed && is_relaxable_projection_error(&error) => Ok(projector
                .project_with_instruction_data_for_programs_relaxed(
                    bytes,
                    programs,
                    context.vote_hashes(),
                )?),
            result => Ok(result?),
        }
    }

    fn project_all_with_vote_retry<'a>(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        relaxed: bool,
    ) -> CompactV2InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        match projector.project(bytes, context.vote_hashes()) {
            Err(error) if needs_vote_hashes(&error) => {
                context.load_vote_hashes(reader)?;
                let retried = projector.project(bytes, context.vote_hashes());
                match retried {
                    Err(error) if relaxed && is_relaxable_projection_error(&error) => {
                        Ok(projector.project_relaxed(bytes, context.vote_hashes())?)
                    }
                    result => Ok(result?),
                }
            }
            Err(error) if relaxed && is_relaxable_projection_error(&error) => {
                Ok(projector.project_relaxed(bytes, context.vote_hashes())?)
            }
            result => Ok(result?),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn project_instructions(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        request: &ScanRequest,
        message_bytes: &[u8],
        message: &ProjectedCompactV2Message<'_>,
        metadata: &ProjectedMetadata<'_>,
        account_keys: &[[u8; 32]],
        signatures: Option<&[[u8; 64]]>,
    ) -> CompactV2InstructionSourceResult<Vec<ResolvedInstruction>> {
        let has_selected_ambiguity = message.instructions().iter().any(|instruction| {
            instruction
                .data_candidates()
                .is_some_and(|candidates| candidates.len() > 1)
        });
        let selected_outer_data = if has_selected_ambiguity {
            match signatures {
                None if !request.require_complete_instruction_data => Some(
                    SelectedOuterData::Unknown(CoverageReason::InstructionDataUnavailable),
                ),
                None => {
                    return Err(CompactV2InstructionSourceError::MissingSidecar {
                        object: crate::manifest::SIGNATURES_FILE,
                        purpose: "selected ambiguous instruction signature sidecar proof",
                    });
                }
                Some(signatures) => {
                    match Self::select_exact_outer_data(reader, context, message_bytes, signatures)
                    {
                        Ok(data) => Some(SelectedOuterData::Exact(data)),
                        Err(error)
                            if !request.require_complete_instruction_data
                                && is_missing_instruction_proof(&error) =>
                        {
                            Some(SelectedOuterData::Unknown(
                                CoverageReason::InstructionDataUnavailable,
                            ))
                        }
                        Err(error)
                            if !request.require_complete_instruction_data
                                && is_unresolved_instruction_ambiguity(&error) =>
                        {
                            Some(SelectedOuterData::Unknown(
                                CoverageReason::AmbiguousInstructionData,
                            ))
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        } else {
            None
        };

        let inner_groups = match metadata {
            ProjectedMetadata::Exact(metadata) => metadata.inner_instructions.as_deref(),
            ProjectedMetadata::Absent | ProjectedMetadata::Raw => None,
        };
        let mut next_group = inner_groups.into_iter().flatten().peekable();
        let mut output = Vec::new();

        for (outer_index, instruction) in message.instructions().iter().enumerate() {
            let program_id = resolve_index(account_keys, instruction.program_id_index())?;
            let accounts = resolve_indexes(account_keys, instruction.accounts())?;
            let (data_coverage, data) = match instruction.data_candidates() {
                None => (InstructionDataCoverage::NotRequested, Vec::new()),
                Some([]) => (
                    InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable),
                    Vec::new(),
                ),
                Some(candidates) if candidates.len() == 1 => {
                    (InstructionDataCoverage::Exact, candidates[0].bytes.clone())
                }
                Some(_) => {
                    let selected = selected_outer_data.as_ref().ok_or_else(|| {
                        CompactV2InstructionSourceError::Invalid(
                            "ambiguous selected data was not signature-selected".into(),
                        )
                    })?;
                    match selected {
                        SelectedOuterData::Exact(selected) => {
                            let data = selected.get(outer_index).ok_or_else(|| {
                                CompactV2InstructionSourceError::Invalid(
                                    "selected signed message has the wrong instruction count"
                                        .into(),
                                )
                            })?;
                            (InstructionDataCoverage::Exact, data.clone())
                        }
                        SelectedOuterData::Unknown(reason) => {
                            (InstructionDataCoverage::Unknown(*reason), Vec::new())
                        }
                    }
                }
            };
            push_instruction(
                &mut output,
                outer_index,
                None,
                None,
                program_id,
                accounts,
                data_coverage,
                data,
            )?;

            if next_group
                .peek()
                .is_some_and(|group| group.outer_instruction_index as usize == outer_index)
            {
                let group = next_group.next().expect("peek proved a CPI group");
                for (inner_index, inner) in group.instructions.iter().enumerate() {
                    let program_id = resolve_index_u32(account_keys, inner.program_id_index)?;
                    let accounts = resolve_indexes(account_keys, inner.accounts)?;
                    let selected =
                        instruction_data_required(&request.instruction_data, &program_id);
                    let (data_coverage, data) = if selected {
                        (InstructionDataCoverage::Exact, inner.data.to_vec())
                    } else {
                        (InstructionDataCoverage::NotRequested, Vec::new())
                    };
                    push_instruction(
                        &mut output,
                        outer_index,
                        Some(inner_index),
                        inner.stack_height,
                        program_id,
                        accounts,
                        data_coverage,
                        data,
                    )?;
                }
            }
        }
        if next_group.next().is_some() {
            return Err(CompactV2InstructionSourceError::Invalid(
                "metadata CPI group has no matching outer instruction".into(),
            ));
        }
        Ok(output)
    }

    fn select_exact_outer_data(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        message_bytes: &[u8],
        signatures: &[[u8; 64]],
    ) -> CompactV2InstructionSourceResult<Vec<Vec<u8>>> {
        let projector =
            CompactV2MessageProjector::new(reader.message_schema(), reader.registry_entries());
        let message =
            Self::project_all_with_vote_retry(reader, context, projector, message_bytes, false)?;
        let static_keys = Self::resolve_static_keys(reader, context, &message)?;
        let recent_blockhash = match message.recent_blockhash() {
            OwnedCompactRecentBlockhash::Nonce(hash) => *hash,
            OwnedCompactRecentBlockhash::Id(id)
                if *id < 0
                    && reader
                        .manifest()
                        .file(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
                        .is_none() =>
            {
                return Err(CompactV2InstructionSourceError::MissingSidecar {
                    object: ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
                    purpose: "ambiguous signed-message previous blockhash",
                });
            }
            OwnedCompactRecentBlockhash::Id(id) => {
                context.load_blockhashes(reader)?.resolve(*id)?
            }
        };
        let resolved_lookups = match message.version() {
            ProjectedCompactV2MessageVersion::V0 {
                address_table_lookups,
            } => address_table_lookups
                .iter()
                .map(|lookup| {
                    context
                        .resolve_pubkey(reader, lookup.account_key())
                        .map(|account_key| crate::ResolvedAddressTableLookup {
                            account_key,
                            writable_indexes: lookup.writable_indexes(),
                            readonly_indexes: lookup.readonly_indexes(),
                        })
                })
                .collect::<CompactV2InstructionSourceResult<Vec<_>>>()?,
            ProjectedCompactV2MessageVersion::Legacy
            | ProjectedCompactV2MessageVersion::V1 { .. } => Vec::new(),
        };
        let version = match message.version() {
            ProjectedCompactV2MessageVersion::Legacy => SignedMessageVersion::Legacy,
            ProjectedCompactV2MessageVersion::V0 { .. } => SignedMessageVersion::V0 {
                address_table_lookups: &resolved_lookups,
            },
            ProjectedCompactV2MessageVersion::V1 { config } => {
                SignedMessageVersion::V1 { config: *config }
            }
        };
        let candidates = message
            .instructions()
            .iter()
            .enumerate()
            .map(|(index, instruction)| {
                let data_candidates = instruction.data_candidates().ok_or_else(|| {
                    CompactV2InstructionSourceError::Invalid(format!(
                        "full signature projection omitted instruction {index} data"
                    ))
                })?;
                Ok(SignedInstructionCandidates {
                    program_id_index: instruction.program_id_index(),
                    accounts: instruction.accounts(),
                    data_candidates,
                })
            })
            .collect::<CompactV2InstructionSourceResult<Vec<_>>>()?;
        let selected = crate::select_signed_message_candidate_ed25519(
            &SignedMessageCandidates {
                version,
                header: message.header(),
                static_account_keys: &static_keys,
                recent_blockhash,
                instructions: &candidates,
            },
            MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
            signatures,
        )?;
        Ok(selected.instruction_data)
    }
}

impl<S: RangeSource> ArchiveInstructionSource for CompactV2InstructionSource<S> {
    fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt> {
        self.scan_inner(request, sink)
    }
}

fn source_error(error: impl std::error::Error + Send + Sync + 'static) -> QueryError {
    QueryError::source(ArchiveFormat::CompactV2, error)
}

fn lane_region(bytes: &[u8], offset: u32, length: u32) -> CompactV2InstructionSourceResult<&[u8]> {
    let start = usize::try_from(offset).map_err(|_| {
        CompactV2InstructionSourceError::Invalid("lane offset exceeds address space".into())
    })?;
    let end = start
        .checked_add(usize::try_from(length).map_err(|_| {
            CompactV2InstructionSourceError::Invalid("lane length exceeds address space".into())
        })?)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("lane range is outside its block lane".into())
        })?;
    Ok(&bytes[start..end])
}

fn reject_set_flag(
    row: ArchiveV2HotTxRow,
    flag: u32,
    name: &str,
) -> CompactV2InstructionSourceResult<()> {
    require_flag_state(row, flag, name, false)
}

fn require_flag_state(
    row: ArchiveV2HotTxRow,
    flag: u32,
    name: &str,
    expected: bool,
) -> CompactV2InstructionSourceResult<()> {
    let actual = row.flags & flag != 0;
    if actual != expected {
        return Err(CompactV2InstructionSourceError::Invalid(format!(
            "transaction {} {name} flag is {actual}, expected {expected}",
            row.tx_index
        )));
    }
    Ok(())
}

fn needs_vote_hashes(error: &CompactV2MessageProjectionError) -> bool {
    matches!(
        error,
        CompactV2MessageProjectionError::ExactInstructionData(
            SignedMessageError::MissingVoteHashResolver { .. }
        )
    )
}

fn is_relaxable_projection_error(error: &CompactV2MessageProjectionError) -> bool {
    matches!(
        error,
        CompactV2MessageProjectionError::CandidateCombinationLimit
            | CompactV2MessageProjectionError::ExactInstructionData(
                SignedMessageError::MissingVoteHashResolver { .. }
            )
    )
}

fn is_missing_instruction_proof(error: &CompactV2InstructionSourceError) -> bool {
    matches!(
        error,
        CompactV2InstructionSourceError::MissingSidecar { .. }
            | CompactV2InstructionSourceError::Message(
                CompactV2MessageProjectionError::ExactInstructionData(
                    SignedMessageError::MissingVoteHashResolver { .. }
                )
            )
    )
}

fn is_unresolved_instruction_ambiguity(error: &CompactV2InstructionSourceError) -> bool {
    matches!(
        error,
        CompactV2InstructionSourceError::Message(
            CompactV2MessageProjectionError::CandidateCombinationLimit
                | CompactV2MessageProjectionError::ExactInstructionData(
                    SignedMessageError::AmbiguousInstructionEncoding { .. }
                )
        ) | CompactV2InstructionSourceError::SignedMessage(
            SignedMessageError::AmbiguousInstructionEncoding { .. }
                | SignedMessageError::CandidateCombinationLimitExceeded { .. }
                | SignedMessageError::MultipleVerifiedMessageCandidates
        )
    )
}

fn static_keys_prefix<'a>(
    message: &'a ProjectedCompactV2Message<'_>,
    required: usize,
) -> CompactV2InstructionSourceResult<&'a [CompactPubkey]> {
    message
        .static_account_keys()
        .get(..required)
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(
                "required signer prefix exceeds projected static keys".into(),
            )
        })
}

fn resolve_index(
    account_keys: &[[u8; 32]],
    index: u8,
) -> CompactV2InstructionSourceResult<[u8; 32]> {
    account_keys
        .get(usize::from(index))
        .copied()
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(format!(
                "message account index {index} is outside resolved keys"
            ))
        })
}

fn resolve_index_u32(
    account_keys: &[[u8; 32]],
    index: u32,
) -> CompactV2InstructionSourceResult<[u8; 32]> {
    let index = usize::try_from(index).map_err(|_| {
        CompactV2InstructionSourceError::Invalid("CPI account index exceeds address space".into())
    })?;
    account_keys.get(index).copied().ok_or_else(|| {
        CompactV2InstructionSourceError::Invalid(format!(
            "CPI account index {index} is outside resolved keys"
        ))
    })
}

fn resolve_indexes(
    account_keys: &[[u8; 32]],
    indexes: &[u8],
) -> CompactV2InstructionSourceResult<Vec<[u8; 32]>> {
    indexes
        .iter()
        .map(|index| resolve_index(account_keys, *index))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn push_instruction(
    output: &mut Vec<ResolvedInstruction>,
    outer_index: usize,
    inner_index: Option<usize>,
    stack_height: Option<u32>,
    program_id: [u8; 32],
    accounts: Vec<[u8; 32]>,
    data_coverage: InstructionDataCoverage,
    data: Vec<u8>,
) -> CompactV2InstructionSourceResult<()> {
    let order = u32::try_from(output.len()).map_err(|_| {
        CompactV2InstructionSourceError::Invalid("instruction order exceeds u32".into())
    })?;
    let outer_index = u32::try_from(outer_index).map_err(|_| {
        CompactV2InstructionSourceError::Invalid("outer instruction index exceeds u32".into())
    })?;
    let inner_index = inner_index
        .map(|index| {
            u32::try_from(index).map_err(|_| {
                CompactV2InstructionSourceError::Invalid(
                    "inner instruction index exceeds u32".into(),
                )
            })
        })
        .transpose()?;
    output.push(ResolvedInstruction {
        coordinate: InstructionCoordinate {
            order,
            outer_index,
            inner_index,
            stack_height,
        },
        program_id,
        accounts,
        data_coverage,
        data,
    });
    Ok(())
}

fn instruction_data_required(
    requirement: &InstructionDataRequirement,
    program_id: &[u8; 32],
) -> bool {
    match requirement {
        InstructionDataRequirement::All => true,
        InstructionDataRequirement::Programs(programs) => programs.contains(program_id),
        InstructionDataRequirement::None => false,
    }
}

enum ProjectedMetadata<'a> {
    Absent,
    Raw,
    Exact(crate::ProjectedCompactV2Metadata<'a>),
}

enum SelectedOuterData {
    Exact(Vec<Vec<u8>>),
    Unknown(CoverageReason),
}

#[derive(Debug, Default, Clone, Copy)]
struct ContextIo {
    calls: u64,
    bytes: u64,
}

impl ContextIo {
    fn record(&mut self, bytes: usize) -> CompactV2InstructionSourceResult<()> {
        self.calls = self.calls.checked_add(1).ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("context read count overflow".into())
        })?;
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes).map_err(|_| {
                CompactV2InstructionSourceError::Invalid(
                    "context read byte count exceeds u64".into(),
                )
            })?)
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("context read bytes overflow".into())
            })?;
        Ok(())
    }

    fn difference(self, before: Self) -> CompactV2InstructionSourceResult<Self> {
        Ok(Self {
            calls: self.calls.checked_sub(before.calls).ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "context read count moved backwards".into(),
                )
            })?,
            bytes: self.bytes.checked_sub(before.bytes).ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "context read bytes moved backwards".into(),
                )
            })?,
        })
    }
}

#[derive(Debug, Default)]
struct ExactContext {
    registry_chunks: HashMap<u32, Vec<[u8; 32]>>,
    registry_lru: VecDeque<u32>,
    vote_hashes_loaded: bool,
    vote_hashes: Option<VoteHashRegistry>,
    blockhashes: Option<BlockhashResolver>,
    io: ContextIo,
}

impl ExactContext {
    fn resolve_pubkey<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
        reference: CompactPubkey,
    ) -> CompactV2InstructionSourceResult<[u8; 32]> {
        let CompactPubkey::Id(id) = reference else {
            let CompactPubkey::Raw(pubkey) = reference else {
                unreachable!("CompactPubkey has only raw and ID forms")
            };
            return Ok(pubkey);
        };
        if id == 0 || id > reader.registry_entries() {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "registry ID {id} is outside 1..={}",
                reader.registry_entries()
            )));
        }
        let zero_based = usize::try_from(id - 1).map_err(|_| {
            CompactV2InstructionSourceError::Invalid("registry ID exceeds address space".into())
        })?;
        let chunk_id = u32::try_from(zero_based / REGISTRY_KEYS_PER_CHUNK).map_err(|_| {
            CompactV2InstructionSourceError::Invalid("registry chunk ID exceeds u32".into())
        })?;
        self.ensure_registry_chunk(reader, chunk_id)?;
        self.touch_registry_chunk(chunk_id);
        let index = zero_based % REGISTRY_KEYS_PER_CHUNK;
        self.registry_chunks
            .get(&chunk_id)
            .and_then(|chunk| chunk.get(index))
            .copied()
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(format!(
                    "registry ID {id} is outside its loaded chunk"
                ))
            })
    }

    fn ensure_registry_chunk<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
        chunk_id: u32,
    ) -> CompactV2InstructionSourceResult<()> {
        if self.registry_chunks.contains_key(&chunk_id) {
            return Ok(());
        }
        let first_key = usize::try_from(chunk_id)
            .ok()
            .and_then(|chunk| chunk.checked_mul(REGISTRY_KEYS_PER_CHUNK))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("registry chunk offset overflow".into())
            })?;
        let entries = usize::try_from(reader.registry_entries()).map_err(|_| {
            CompactV2InstructionSourceError::Invalid(
                "registry entry count exceeds address space".into(),
            )
        })?;
        let key_count = entries
            .saturating_sub(first_key)
            .min(REGISTRY_KEYS_PER_CHUNK);
        if key_count == 0 {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "registry chunk {chunk_id} is outside the registry"
            )));
        }
        let offset = u64::try_from(first_key)
            .ok()
            .and_then(|key| key.checked_mul(REGISTRY_KEY_BYTES as u64))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("registry byte offset overflow".into())
            })?;
        let length = key_count.checked_mul(REGISTRY_KEY_BYTES).ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("registry chunk length overflow".into())
        })?;
        let bytes = reader
            .source()
            .read_range(crate::manifest::REGISTRY_FILE, offset, length)?;
        self.io.record(bytes.len())?;
        let keys = bytes
            .chunks_exact(REGISTRY_KEY_BYTES)
            .map(|bytes| {
                let mut key = [0u8; 32];
                key.copy_from_slice(bytes);
                key
            })
            .collect::<Vec<_>>();
        if keys.len() != key_count {
            return Err(CompactV2InstructionSourceError::Invalid(
                "registry chunk has a partial public key".into(),
            ));
        }
        if self.registry_chunks.len() == REGISTRY_CACHE_CHUNKS
            && let Some(evicted) = self.registry_lru.pop_front()
        {
            self.registry_chunks.remove(&evicted);
        }
        self.registry_chunks.insert(chunk_id, keys);
        self.registry_lru.push_back(chunk_id);
        Ok(())
    }

    fn touch_registry_chunk(&mut self, chunk_id: u32) {
        if let Some(position) = self.registry_lru.iter().position(|id| *id == chunk_id) {
            self.registry_lru.remove(position);
            self.registry_lru.push_back(chunk_id);
        }
    }

    fn read_block_signatures<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
        row: &blockzilla_format::ArchiveV2HotBlockIndexRow,
    ) -> CompactV2InstructionSourceResult<Option<Vec<[u8; 64]>>> {
        if !reader.signatures_available() {
            return Ok(None);
        }
        let length = usize::try_from(row.signature_count)
            .ok()
            .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "block signature byte length overflow".into(),
                )
            })?;
        let row_bound = usize::try_from(row.tx_count)
            .ok()
            .and_then(|count| count.checked_mul(usize::from(u8::MAX)))
            .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
            .unwrap_or(usize::MAX)
            .min(MAX_SIGNATURE_BYTES_PER_BLOCK);
        if length > row_bound {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "block {} signature window is {length} bytes, above {row_bound}",
                row.block_id
            )));
        }
        if length == 0 {
            return Ok(Some(Vec::new()));
        }
        let offset = row
            .first_signature_ordinal
            .checked_mul(SIGNATURE_BYTES as u64)
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "block signature byte offset overflow".into(),
                )
            })?;
        let bytes = reader
            .source()
            .read_range(crate::manifest::SIGNATURES_FILE, offset, length)?;
        self.io.record(bytes.len())?;
        Ok(Some(
            bytes
                .chunks_exact(SIGNATURE_BYTES)
                .map(|bytes| {
                    let mut signature = [0u8; 64];
                    signature.copy_from_slice(bytes);
                    signature
                })
                .collect(),
        ))
    }

    fn vote_hashes(&self) -> Option<&dyn VoteHashResolver> {
        self.vote_hashes
            .as_ref()
            .map(|registry| registry as &dyn VoteHashResolver)
    }

    fn load_vote_hashes<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
    ) -> CompactV2InstructionSourceResult<()> {
        if self.vote_hashes_loaded {
            return Ok(());
        }
        let Some(binding) = reader.manifest().file(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE) else {
            self.vote_hashes_loaded = true;
            return Ok(());
        };
        let maximum = reader
            .index()
            .rows
            .len()
            .checked_mul(crate::VOTE_HASH_RECORD_LEN)
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("vote-hash registry bound overflow".into())
            })?
            .min(MAX_VOTE_HASH_REGISTRY_BYTES);
        let size = usize::try_from(binding.size).map_err(|_| {
            CompactV2InstructionSourceError::Invalid(
                "vote-hash registry size exceeds address space".into(),
            )
        })?;
        if size > maximum {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "vote-hash registry is {size} bytes, above the {maximum}-byte block bound"
            )));
        }
        let bytes = reader
            .source()
            .read_range(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, 0, size)?;
        self.io.record(bytes.len())?;
        self.vote_hashes = Some(VoteHashRegistry::from_bytes(&bytes)?);
        self.vote_hashes_loaded = true;
        Ok(())
    }

    fn load_blockhashes<'a, S: RangeSource>(
        &'a mut self,
        reader: &ArchiveReader<S>,
    ) -> CompactV2InstructionSourceResult<&'a BlockhashResolver> {
        if self.blockhashes.is_none() {
            let current = reader
                .manifest()
                .file(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)
                .ok_or(CompactV2InstructionSourceError::MissingSidecar {
                    object: ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                    purpose: "ambiguous signed-message recent blockhash",
                })?;
            let maximum = reader
                .index()
                .rows
                .len()
                .checked_add(1)
                .and_then(|count| count.checked_mul(crate::BLOCKHASH_RECORD_LEN))
                .ok_or_else(|| {
                    CompactV2InstructionSourceError::Invalid(
                        "blockhash registry bound overflow".into(),
                    )
                })?
                .min(MAX_BLOCKHASH_REGISTRY_BYTES);
            let current_size = usize::try_from(current.size).map_err(|_| {
                CompactV2InstructionSourceError::Invalid(
                    "blockhash registry size exceeds address space".into(),
                )
            })?;
            if current_size > maximum {
                return Err(CompactV2InstructionSourceError::Invalid(format!(
                    "blockhash registry is {current_size} bytes, above the {maximum}-byte block bound"
                )));
            }
            let current =
                reader
                    .source()
                    .read_range(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, 0, current_size)?;
            self.io.record(current.len())?;

            let previous = if reader.manifest().epoch == 0 {
                PreviousBlockhashTail {
                    schema: PreviousBlockhashTailSchema::CurrentHashAndSlot,
                    entries: Vec::new(),
                }
            } else {
                let binding = reader
                    .manifest()
                    .file(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
                    .ok_or(CompactV2InstructionSourceError::MissingSidecar {
                        object: ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
                        purpose: "ambiguous signed-message previous blockhash",
                    })?;
                let expected = PREVIOUS_BLOCKHASH_RECORDS
                    .checked_mul(crate::PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN)
                    .ok_or_else(|| {
                        CompactV2InstructionSourceError::Invalid(
                            "previous blockhash tail bound overflow".into(),
                        )
                    })?;
                let size = usize::try_from(binding.size).map_err(|_| {
                    CompactV2InstructionSourceError::Invalid(
                        "previous blockhash tail size exceeds address space".into(),
                    )
                })?;
                if size != expected {
                    return Err(CompactV2InstructionSourceError::Invalid(format!(
                        "previous blockhash tail is {size} bytes, expected {expected} current-schema bytes"
                    )));
                }
                let bytes =
                    reader
                        .source()
                        .read_range(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, 0, size)?;
                self.io.record(bytes.len())?;
                parse_previous_blockhash_tail(
                    &bytes,
                    PreviousBlockhashTailSchema::CurrentHashAndSlot,
                )?
            };
            self.blockhashes = Some(BlockhashResolver::from_bytes(&current, previous)?);
        }
        self.blockhashes.as_ref().ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(
                "blockhash resolver was not initialized".into(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, num::NonZeroU32, path::Path};

    use blockzilla_format::{
        ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader,
        ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
        ArchiveV2HotMetaRecord, ArchiveV2HotV0Message, ArchiveV2VoteHashRef,
        ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync, CompactInnerInstruction,
        CompactInnerInstructions, CompactInstructionError, CompactMessageHeader, CompactMetaV1,
        CompactTransactionError, OwnedCompactAddressTableLookup, WINCODE_ARCHIVE_V2_FLAG_LEB128,
        WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header,
        wincode_leb128_config, write_archive_v2_hot_block_index,
    };
    use blockzilla_query_sdk::{
        ArchiveInstructionSourceExt, BlockView, CpiCoverage, ExecutionStatus, InstructionCoverage,
        InstructionDataCoverage, ScanRange,
    };
    use ed25519_dalek::{Signer, SigningKey};
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        HashVerification, LocalRangeSource, OpenOptions, SignedInstruction, SignedMessage,
        compact_query::ExactContext,
        manifest::{
            BLOCK_INDEX_FILE, BLOCKS_FILE, GENERATION_MANIFEST_FILE, GenerationFile,
            GenerationManifest, META_FILE, REGISTRY_FILE, SIGNATURES_FILE,
            TrustedGenerationIdentity, compute_generation_digest,
        },
        reconstruct_instruction_data_candidates, serialize_signed_message,
    };

    const EPOCH: u64 = 1;
    const FIRST_SLOT: u64 = 100;
    const SLOTS_PER_EPOCH: u64 = 100;
    const TOKEN_PROGRAM: [u8; 32] = [2; 32];
    const VOTE_PROGRAM: [u8; 32] = [3; 32];
    const CPI_PROGRAM: [u8; 32] = [4; 32];
    const LOOKUP_TABLE: [u8; 32] = [5; 32];
    const LOADED_ACCOUNT: [u8; 32] = [6; 32];

    struct Fixture {
        directory: TempDir,
        signer: [u8; 32],
        signatures: Vec<[u8; 64]>,
        decoded_bytes: u64,
        compressed_bytes: u64,
    }

    impl Fixture {
        fn main() -> Self {
            let signing_key = SigningKey::from_bytes(&[41; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let registry = vec![
                signer,
                TOKEN_PROGRAM,
                VOTE_PROGRAM,
                CPI_PROGRAM,
                LOOKUP_TABLE,
                LOADED_ACCOUNT,
            ];

            let token_and_unrelated_vote = legacy_message(vec![
                raw_instruction(1, &[0], &[3, 11]),
                ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: Vec::new(),
                    data: vote_tower_data(true),
                },
            ]);
            let token_and_unrelated_vote_meta = metadata(3, None, Some(Vec::new()), vec![], vec![]);

            let v0 = blockzilla_format::ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header: header(),
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce([12; 32]),
                instructions: vec![raw_instruction(1, &[2], &[9, 8])],
                address_table_lookups: vec![OwnedCompactAddressTableLookup {
                    account_key: CompactPubkey::Id(5),
                    writable_indexes: vec![0],
                    readonly_indexes: vec![1],
                }],
            });
            let v0_meta = metadata(
                4,
                None,
                Some(vec![CompactInnerInstructions {
                    index: 0,
                    instructions: vec![CompactInnerInstruction {
                        program_id_index: 3,
                        accounts: vec![2],
                        data: vec![7, 7],
                        stack_height: Some(2),
                    }],
                }]),
                vec![CompactPubkey::Id(6)],
                vec![CompactPubkey::Id(4)],
            );

            let failed = legacy_message(vec![
                raw_instruction(1, &[0], &[1]),
                raw_instruction(1, &[0], &[2]),
            ]);
            let failed_meta = metadata(
                2,
                Some(CompactTransactionError::InstructionError(
                    1,
                    CompactInstructionError::Custom(42),
                )),
                Some(vec![CompactInnerInstructions {
                    index: 1,
                    instructions: vec![CompactInnerInstruction {
                        program_id_index: 1,
                        accounts: vec![0],
                        data: vec![4],
                        stack_height: Some(2),
                    }],
                }]),
                vec![],
                vec![],
            );

            let missing_meta = legacy_message(vec![raw_instruction(1, &[0], &[5])]);
            let raw_meta = legacy_message(vec![raw_instruction(1, &[0], &[6])]);
            let decoded_without_cpi = legacy_message(vec![raw_instruction(1, &[0], &[7])]);

            let signatures = vec![
                [21; 64], [22; 64], [23; 64], [24; 64], [25; 64], [26; 64], [27; 64],
            ];
            let transactions = vec![
                TxFixture::exact(
                    token_and_unrelated_vote,
                    token_and_unrelated_vote_meta,
                    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                ),
                TxFixture::exact(
                    v0,
                    v0_meta,
                    ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                        | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                ),
                TxFixture::exact(
                    failed,
                    failed_meta,
                    ARCHIVE_V2_TX_FLAG_HAS_ERROR | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                ),
                TxFixture::without_metadata(missing_meta),
                TxFixture::raw_metadata(raw_meta),
                TxFixture::raw_transaction(vec![0xff]),
                TxFixture::exact(
                    decoded_without_cpi,
                    metadata(2, None, None, vec![], vec![]),
                    0,
                ),
            ];
            Self::build(registry, vec![Vec::new(), transactions], Some(signatures))
        }

        fn ambiguous(with_signatures: bool) -> (Self, Vec<u8>) {
            let signing_key = SigningKey::from_bytes(&[44; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let data = vote_tower_data(false);
            let candidates = reconstruct_instruction_data_candidates(&data, None).unwrap();
            assert_eq!(candidates.len(), 2);
            let selected_data = candidates[1].bytes.clone();
            let signed_message = serialize_signed_message(&SignedMessage {
                version: SignedMessageVersion::Legacy,
                header: header(),
                static_account_keys: &[signer, VOTE_PROGRAM],
                recent_blockhash: [13; 32],
                instructions: &[SignedInstruction {
                    program_id_index: 1,
                    accounts: &[],
                    data: &selected_data,
                }],
            })
            .unwrap();
            let signature = signing_key.sign(&signed_message).to_bytes();
            let message = legacy_message(vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: Vec::new(),
                data,
            }]);
            let transaction = TxFixture::exact(
                message,
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            );
            let signatures = with_signatures.then_some(vec![signature]);
            (
                Self::build(
                    vec![signer, VOTE_PROGRAM],
                    vec![vec![transaction]],
                    signatures,
                ),
                selected_data,
            )
        }

        fn ambiguous_with_trusted_vote_sidecar() -> (Self, Vec<u8>) {
            let signing_key = SigningKey::from_bytes(&[46; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let vote_hash_bytes = vote_hash_registry_bytes();
            let vote_hashes = VoteHashRegistry::from_bytes(&vote_hash_bytes).unwrap();
            let data = vote_tower_data(true);
            let candidates = reconstruct_instruction_data_candidates(
                &data,
                Some(&vote_hashes as &dyn VoteHashResolver),
            )
            .unwrap();
            assert_eq!(candidates.len(), 2);
            let selected_data = candidates[1].bytes.clone();
            let signed_message = serialize_signed_message(&SignedMessage {
                version: SignedMessageVersion::Legacy,
                header: header(),
                static_account_keys: &[signer, VOTE_PROGRAM],
                recent_blockhash: [13; 32],
                instructions: &[SignedInstruction {
                    program_id_index: 1,
                    accounts: &[],
                    data: &selected_data,
                }],
            })
            .unwrap();
            let signature = signing_key.sign(&signed_message).to_bytes();
            let transaction = TxFixture::exact(
                legacy_message(vec![ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: Vec::new(),
                    data,
                }]),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            );
            let fixture = Self::build(
                vec![signer, VOTE_PROGRAM],
                vec![vec![transaction]],
                Some(vec![signature]),
            );
            fs::write(
                fixture
                    .directory
                    .path()
                    .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
                vote_hash_bytes,
            )
            .unwrap();
            fs::write(
                fixture
                    .directory
                    .path()
                    .join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
                [51; 32],
            )
            .unwrap();
            fs::write(
                fixture
                    .directory
                    .path()
                    .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE),
                vec![0; PREVIOUS_BLOCKHASH_RECORDS * crate::PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN],
            )
            .unwrap();
            (fixture, selected_data)
        }

        fn invalid_raw_metadata_flag() -> Self {
            let signing_key = SigningKey::from_bytes(&[47; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let transaction = TxFixture {
                flags: ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                message: encode(&legacy_message(vec![raw_instruction(1, &[0], &[1])])),
                metadata: None,
            };
            Self::build(vec![signer, TOKEN_PROGRAM], vec![vec![transaction]], None)
        }

        fn empty_with_large_registry() -> Self {
            let signing_key = SigningKey::from_bytes(&[49; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let count = REGISTRY_KEYS_PER_CHUNK * (REGISTRY_CACHE_CHUNKS + 1);
            let registry = (0..count)
                .map(|index| {
                    let mut key = [0u8; 32];
                    key[..8].copy_from_slice(&(index as u64).to_le_bytes());
                    key
                })
                .collect();
            Self::build(registry, Vec::new(), None).with_signer(signer)
        }

        fn with_signer(mut self, signer: [u8; 32]) -> Self {
            self.signer = signer;
            self
        }

        fn build(
            registry: Vec<[u8; 32]>,
            blocks: Vec<Vec<TxFixture>>,
            signatures: Option<Vec<[u8; 64]>>,
        ) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            fs::write(
                root.join(REGISTRY_FILE),
                registry.iter().flatten().copied().collect::<Vec<_>>(),
            )
            .unwrap();

            let mut encoded_blocks = Vec::new();
            let mut index_rows = Vec::new();
            let mut compressed_offset = 0u64;
            let mut transaction_ordinal = 0u64;
            let mut signature_ordinal = 0u64;
            let mut decoded_bytes = 0u64;
            for (block_id, transactions) in blocks.into_iter().enumerate() {
                let mut rows = Vec::new();
                let mut messages = Vec::new();
                let mut metadata = Vec::new();
                for (tx_index, transaction) in transactions.into_iter().enumerate() {
                    let message_offset = messages.len() as u32;
                    let metadata_offset = metadata.len() as u32;
                    messages.extend_from_slice(&transaction.message);
                    if let Some(bytes) = &transaction.metadata {
                        metadata.extend_from_slice(bytes);
                    }
                    rows.push(ArchiveV2HotTxRow {
                        tx_index: tx_index as u32,
                        flags: transaction.flags,
                        message_offset,
                        message_len: transaction.message.len() as u32,
                        metadata_offset,
                        metadata_len: transaction
                            .metadata
                            .as_ref()
                            .map_or(0, |bytes| bytes.len() as u32),
                        signature_count: 1,
                        reserved: [0; 3],
                    });
                }
                let slot = FIRST_SLOT + 1 + block_id as u64;
                let blob = ArchiveV2HotBlockBlob {
                    header: ArchiveV2HotBlockHeader {
                        slot,
                        parent_slot: slot - 1,
                        blockhash_id: block_id as u32 + 1,
                        previous_blockhash_id: block_id as u32,
                        block_time: None,
                        block_height: None,
                        rewards: None,
                    },
                    tx_count: rows.len() as u32,
                    tx_rows: rows,
                    message_bytes: messages,
                    metadata_bytes: metadata,
                };
                let uncompressed =
                    wincode::config::serialize(&blob, wincode_leb128_config()).unwrap();
                let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
                let signature_count = blob.tx_count;
                index_rows.push(blockzilla_format::ArchiveV2HotBlockIndexRow {
                    block_id: block_id as u32,
                    slot,
                    compressed_offset,
                    compressed_len: compressed.len() as u32,
                    uncompressed_len: uncompressed.len() as u32,
                    tx_count: blob.tx_count,
                    first_tx_ordinal: transaction_ordinal,
                    first_signature_ordinal: signature_ordinal,
                    signature_count,
                });
                compressed_offset += compressed.len() as u64;
                transaction_ordinal += u64::from(blob.tx_count);
                signature_ordinal += u64::from(signature_count);
                decoded_bytes += uncompressed.len() as u64;
                encoded_blocks.extend_from_slice(&compressed);
            }
            fs::write(root.join(BLOCKS_FILE), &encoded_blocks).unwrap();
            write_archive_v2_hot_block_index(
                &root.join(BLOCK_INDEX_FILE),
                encoded_blocks.len() as u64,
                1,
                0,
                &index_rows,
            )
            .unwrap();

            let transaction_count = index_rows.iter().map(|row| u64::from(row.tx_count)).sum();
            write_metadata_file(root, index_rows.len() as u64, transaction_count);
            if let Some(signatures) = &signatures {
                fs::write(
                    root.join(SIGNATURES_FILE),
                    signatures.iter().flatten().copied().collect::<Vec<_>>(),
                )
                .unwrap();
            }
            Self {
                directory,
                signer: registry.first().copied().unwrap_or([0; 32]),
                signatures: signatures.unwrap_or_default(),
                decoded_bytes,
                compressed_bytes: encoded_blocks.len() as u64,
            }
        }

        fn trusted_reader(&self) -> ArchiveReader<LocalRangeSource> {
            ArchiveReader::open_trusted(
                LocalRangeSource::new(self.directory.path()),
                TrustedGenerationIdentity {
                    cluster_id: "testnet".into(),
                    epoch: EPOCH,
                    generation_id: "compact-query-fixture".into(),
                    slots_per_epoch: SLOTS_PER_EPOCH,
                },
                OpenOptions {
                    hash_verification: HashVerification::SizesOnly,
                    ..OpenOptions::default()
                },
            )
            .unwrap()
        }

        fn published_reader(&self) -> ArchiveReader<LocalRangeSource> {
            write_manifest(self.directory.path());
            ArchiveReader::open(LocalRangeSource::new(self.directory.path())).unwrap()
        }
    }

    struct TxFixture {
        flags: u32,
        message: Vec<u8>,
        metadata: Option<Vec<u8>>,
    }

    impl TxFixture {
        fn exact(
            message: blockzilla_format::ArchiveV2HotMessagePayload,
            metadata: CompactMetaV1,
            flags: u32,
        ) -> Self {
            Self {
                flags: flags | ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                message: encode(&message),
                metadata: Some(encode(&metadata)),
            }
        }

        fn without_metadata(message: blockzilla_format::ArchiveV2HotMessagePayload) -> Self {
            Self {
                flags: 0,
                message: encode(&message),
                metadata: None,
            }
        }

        fn raw_metadata(message: blockzilla_format::ArchiveV2HotMessagePayload) -> Self {
            Self {
                flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                message: encode(&message),
                metadata: Some(vec![0xde, 0xad]),
            }
        }

        fn raw_transaction(message: Vec<u8>) -> Self {
            Self {
                flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                message,
                metadata: None,
            }
        }
    }

    fn header() -> CompactMessageHeader {
        CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        }
    }

    fn legacy_message(
        instructions: Vec<ArchiveV2HotInstruction>,
    ) -> blockzilla_format::ArchiveV2HotMessagePayload {
        let account_keys = if instructions
            .iter()
            .any(|instruction| instruction.program_id_index == 2)
        {
            vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
            ]
        } else {
            vec![CompactPubkey::Id(1), CompactPubkey::Id(2)]
        };
        blockzilla_format::ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: header(),
            account_keys,
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([13; 32]),
            instructions,
        })
    }

    fn raw_instruction(
        program_id_index: u8,
        accounts: &[u8],
        data: &[u8],
    ) -> ArchiveV2HotInstruction {
        ArchiveV2HotInstruction {
            program_id_index,
            accounts: accounts.to_vec(),
            data: ArchiveV2HotInstructionData::Raw(data.to_vec()),
        }
    }

    fn vote_tower_data(block_references: bool) -> ArchiveV2HotInstructionData {
        let hash = if block_references {
            ArchiveV2VoteHashRef::Block(0)
        } else {
            ArchiveV2VoteHashRef::Raw([31; 32])
        };
        ArchiveV2HotInstructionData::VoteTowerSync(ArchiveV2VoteTowerSync {
            update: ArchiveV2VoteStateUpdate {
                root: None,
                lockout_offsets: Vec::new(),
                hash,
                timestamp: None,
            },
            block_id_hash: hash,
        })
    }

    fn vote_hash_registry_bytes() -> Vec<u8> {
        let mut bytes = Vec::with_capacity(crate::VOTE_HASH_RECORD_LEN);
        bytes.push(0b11);
        bytes.extend_from_slice(&[32; 32]);
        bytes.extend_from_slice(&[33; 32]);
        bytes
    }

    fn metadata(
        account_count: usize,
        error: Option<CompactTransactionError>,
        inner_instructions: Option<Vec<CompactInnerInstructions>>,
        loaded_writable_addresses: Vec<CompactPubkey>,
        loaded_readonly_addresses: Vec<CompactPubkey>,
    ) -> CompactMetaV1 {
        CompactMetaV1 {
            err: error,
            fee: 5_000,
            pre_balances: vec![0; account_count],
            post_balances: vec![0; account_count],
            inner_instructions,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses,
            loaded_readonly_addresses,
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn encode<T: wincode::SchemaWrite<blockzilla_format::WincodeLeb128Config, Src = T>>(
        value: &T,
    ) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn write_metadata_file(root: &Path, blocks: u64, transactions: u64) {
        let records = [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks,
                transactions,
                ..WincodeArchiveV2Footer::default()
            }),
        ];
        let mut bytes = Vec::new();
        for record in records {
            let encoded = encode(&record);
            write_u32_varint(&mut bytes, encoded.len() as u32);
            bytes.extend_from_slice(&encoded);
        }
        fs::write(root.join(META_FILE), bytes).unwrap();
    }

    fn write_manifest(root: &Path) {
        let mut files = Vec::new();
        for name in [
            BLOCKS_FILE,
            BLOCK_INDEX_FILE,
            META_FILE,
            REGISTRY_FILE,
            SIGNATURES_FILE,
        ] {
            let Ok(bytes) = fs::read(root.join(name)) else {
                continue;
            };
            files.push(GenerationFile {
                name: name.into(),
                size: bytes.len() as u64,
                sha256: hex_lower(&Sha256::digest(&bytes)),
            });
        }
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "published-compact-query-fixture".into(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: SLOTS_PER_EPOCH,
            complete: true,
            files,
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        fs::write(
            root.join(GENERATION_MANIFEST_FILE),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
    }

    fn hex_lower(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
        output
    }

    fn write_u32_varint(output: &mut Vec<u8>, mut value: u32) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    #[test]
    fn publishes_exact_order_loaded_keys_cpi_failure_coverage_and_io() {
        let fixture = Fixture::main();
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        assert_eq!(
            source.identity().verification,
            SourceVerification::OperatorTrusted
        );
        assert_eq!(source.identity().binding, None);

        let request = ScanRequest::all()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .with_instruction_data_for([TOKEN_PROGRAM]);
        let mut blocks = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                blocks.push((block.header, block.transactions.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(blocks.len(), 2);
        assert!(blocks[0].1.is_empty());
        let transactions = &blocks[1].1;
        assert_eq!(transactions.len(), 7);
        assert_eq!(
            transactions
                .iter()
                .map(|transaction| transaction.header.tx_index)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3, 4, 5, 6]
        );
        assert_eq!(
            transactions[0].primary_signature,
            Some(fixture.signatures[0])
        );
        assert_eq!(transactions[0].required_signers, [fixture.signer]);
        assert_eq!(transactions[0].header.cpi_coverage, CpiCoverage::Complete);
        assert_eq!(transactions[0].instructions[0].data, [3, 11]);
        assert_eq!(
            transactions[0].instructions[0].data_coverage,
            InstructionDataCoverage::Exact
        );
        assert_eq!(
            transactions[0].instructions[1].data_coverage,
            InstructionDataCoverage::NotRequested
        );
        assert!(transactions[0].instructions[1].data.is_empty());

        let v0 = &transactions[1];
        assert_eq!(v0.instructions.len(), 2);
        assert_eq!(v0.instructions[0].accounts, [LOADED_ACCOUNT]);
        assert_eq!(v0.instructions[1].program_id, CPI_PROGRAM);
        assert_eq!(v0.instructions[1].accounts, [LOADED_ACCOUNT]);
        assert_eq!(v0.instructions[1].coordinate.order, 1);
        assert_eq!(v0.instructions[1].coordinate.outer_index, 0);
        assert_eq!(v0.instructions[1].coordinate.inner_index, Some(0));
        assert_eq!(v0.instructions[1].coordinate.stack_height, Some(2));
        assert_eq!(
            v0.instructions[1].data_coverage,
            InstructionDataCoverage::NotRequested
        );

        assert_eq!(transactions[2].header.status, ExecutionStatus::Failed);
        assert_eq!(
            transactions[2].header.failed_outer_instruction_index,
            Some(1)
        );
        assert_eq!(transactions[2].instructions[2].coordinate.outer_index, 1);
        assert_eq!(
            transactions[2].instructions[2].coordinate.inner_index,
            Some(0)
        );

        assert_eq!(
            transactions[3].header.status,
            ExecutionStatus::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            transactions[3].header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            transactions[3].header.instruction_coverage,
            InstructionCoverage::Complete
        );
        assert_eq!(
            transactions[4].header.status,
            ExecutionStatus::Unknown(CoverageReason::RawMetadata)
        );
        assert_eq!(
            transactions[4].header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::RawMetadata)
        );
        assert_eq!(
            transactions[5].header.instruction_coverage,
            InstructionCoverage::Unknown(CoverageReason::RawTransaction)
        );
        assert!(transactions[5].instructions.is_empty());
        assert_eq!(
            transactions[6].header.cpi_coverage,
            CpiCoverage::NotRecorded
        );

        assert_eq!(receipt.blocks, 2);
        assert_eq!(receipt.transactions, 7);
        assert_eq!(receipt.instructions, 10);
        assert_eq!(receipt.instructions_not_requested, 2);
        assert_eq!(receipt.instructions_with_unknown_data, 0);
        assert_eq!(receipt.transactions_with_incomplete_instructions, 1);
        assert_eq!(receipt.transactions_with_incomplete_cpi, 4);
        assert_eq!(receipt.transactions_with_unknown_execution, 3);
        assert_eq!(receipt.io.source_read_calls, Some(3));
        assert_eq!(
            receipt.io.source_read_bytes,
            Some(fixture.compressed_bytes + 6 * 32 + 7 * 64)
        );
        assert_eq!(receipt.io.decoded_bytes, Some(fixture.decoded_bytes));
    }

    #[test]
    fn selected_ambiguity_requires_and_uses_exact_signature_proof() {
        let (missing, _) = Fixture::ambiguous(false);
        let mut source =
            CompactV2InstructionSource::new(missing.trusted_reader(), FIRST_SLOT).unwrap();
        let error = source
            .for_each_block(&ScanRequest::all(), |_| Ok(()))
            .unwrap_err();
        assert!(error.to_string().contains("source error"));
        let source_text = format!("{error:?}");
        assert!(source_text.contains("signature sidecar proof"));

        let (missing, _) = Fixture::ambiguous(false);
        let mut source =
            CompactV2InstructionSource::new(missing.trusted_reader(), FIRST_SLOT).unwrap();
        let mut relaxed = None;
        let receipt = source
            .for_each_block(
                &ScanRequest::all().allow_incomplete_instruction_data(),
                |block| {
                    let instruction = &block.transactions[0].instructions[0];
                    relaxed = Some((instruction.data_coverage, instruction.data.clone()));
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            relaxed,
            Some((
                InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable),
                Vec::new()
            ))
        );
        assert_eq!(receipt.instructions_with_unknown_data, 1);

        let (proved, selected_data) = Fixture::ambiguous(true);
        let expected_signature = proved.signatures[0];
        let mut source =
            CompactV2InstructionSource::new(proved.trusted_reader(), FIRST_SLOT).unwrap();
        let mut observed = None;
        source
            .for_each_block(&ScanRequest::all(), |block| {
                observed = Some((
                    block.transactions[0].instructions[0].data.clone(),
                    block.transactions[0].primary_signature,
                ));
                Ok(())
            })
            .unwrap();
        assert_eq!(observed, Some((selected_data, Some(expected_signature))));
    }

    #[test]
    fn relaxed_candidate_limit_reports_ambiguity_and_keeps_scanning() {
        let signer = [71; 32];
        let instructions = (0..14)
            .map(|_| ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: Vec::new(),
                data: vote_tower_data(true),
            })
            .collect();
        let transaction = TxFixture::exact(
            legacy_message(instructions),
            metadata(2, None, Some(Vec::new()), vec![], vec![]),
            ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let fixture = Fixture::build(
            vec![signer, VOTE_PROGRAM],
            vec![vec![transaction]],
            Some(vec![[0; 64]]),
        );
        fs::write(
            fixture
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            vote_hash_registry_bytes(),
        )
        .unwrap();

        let mut strict =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        assert!(
            strict
                .for_each_block(&ScanRequest::all(), |_| Ok(()))
                .is_err()
        );

        let mut relaxed =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let mut coverages = Vec::new();
        let receipt = relaxed
            .for_each_block(
                &ScanRequest::all().allow_incomplete_instruction_data(),
                |block| {
                    coverages.extend(
                        block.transactions[0]
                            .instructions
                            .iter()
                            .map(|instruction| instruction.data_coverage),
                    );
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(coverages.len(), 14);
        assert!(coverages.iter().all(|coverage| {
            *coverage == InstructionDataCoverage::Unknown(CoverageReason::AmbiguousInstructionData)
        }));
        assert_eq!(receipt.instructions_with_unknown_data, 14);
    }

    #[test]
    fn relaxed_mode_rejects_present_vote_registry_without_referenced_hash() {
        let transaction = TxFixture::exact(
            legacy_message(vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: Vec::new(),
                data: vote_tower_data(true),
            }]),
            metadata(2, None, Some(Vec::new()), vec![], vec![]),
            ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let fixture = Fixture::build(vec![[72; 32], VOTE_PROGRAM], vec![vec![transaction]], None);
        fs::write(
            fixture
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            [],
        )
        .unwrap();

        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        assert!(
            source
                .for_each_block(
                    &ScanRequest::all().allow_incomplete_instruction_data(),
                    |_| Ok(()),
                )
                .is_err(),
            "a present registry with no referenced row is a hard source error"
        );
    }

    #[test]
    fn trusted_reader_admits_optional_sidecars_for_exact_vote_proof() {
        let (fixture, selected_data) = Fixture::ambiguous_with_trusted_vote_sidecar();
        let reader = fixture.trusted_reader();
        for name in [
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
            ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
        ] {
            assert!(reader.manifest().file(name).is_some(), "missing {name}");
        }
        let mut source = CompactV2InstructionSource::new(reader, FIRST_SLOT).unwrap();
        let mut observed = None;
        let receipt = source
            .for_each_block(&ScanRequest::all(), |block| {
                observed = Some(block.transactions[0].instructions[0].data.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(observed, Some(selected_data));
        assert_eq!(receipt.io.source_read_calls, Some(4));
        assert_eq!(
            receipt.io.source_read_bytes.map(|bytes| bytes > 0),
            Some(true)
        );
    }

    #[test]
    fn rejects_raw_metadata_fallback_without_metadata_presence() {
        let fixture = Fixture::invalid_raw_metadata_flag();
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let error = source
            .for_each_block(
                &ScanRequest::all()
                    .allow_incomplete_instructions()
                    .allow_incomplete_cpi()
                    .allow_unknown_execution(),
                |_| Ok(()),
            )
            .unwrap_err();
        assert!(format!("{error:?}").contains("METADATA_RAW_FALLBACK without HAS_METADATA"));
    }

    #[test]
    fn bounded_range_and_sink_stop_keep_publication_order() {
        let fixture = Fixture::main();
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let request = ScanRequest::bounded(ScanRange {
            first_block: 1,
            block_count: NonZeroU32::new(1).unwrap(),
        })
        .allow_incomplete_instructions()
        .allow_incomplete_cpi()
        .allow_unknown_execution()
        .with_instruction_data_for([TOKEN_PROGRAM]);
        let mut ordinals = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                ordinals.push(block.header.block_ordinal);
                Ok(())
            })
            .unwrap();
        assert_eq!(ordinals, [1]);
        assert_eq!(receipt.blocks, 1);

        struct StopSink {
            visits: usize,
        }
        impl BlockSink for StopSink {
            fn visit_block(&mut self, _block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
                self.visits += 1;
                Err(QueryError::sink(std::io::Error::other("stop")))
            }
        }
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let mut sink = StopSink { visits: 0 };
        assert!(source.scan_ordered(&request, &mut sink).is_err());
        assert_eq!(sink.visits, 1);
    }

    #[test]
    fn source_identity_preserves_published_and_operator_trust() {
        let fixture = Fixture::main();
        let trusted =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        assert_eq!(
            trusted.identity().verification,
            SourceVerification::OperatorTrusted
        );
        assert_eq!(trusted.identity().binding, None);

        let published =
            CompactV2InstructionSource::new(fixture.published_reader(), FIRST_SLOT).unwrap();
        assert_eq!(
            published.identity().verification,
            SourceVerification::PublishedManifest
        );
        assert!(published.identity().binding.is_some());
        assert_eq!(published.identity().first_slot, FIRST_SLOT);
    }

    #[test]
    fn registry_chunk_cache_caps_retained_key_payload_and_evicts() {
        assert_eq!(COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES, 512 * 1024);
        let fixture = Fixture::empty_with_large_registry();
        let reader = fixture.trusted_reader();
        let mut context = ExactContext::default();
        for chunk in 0..=REGISTRY_CACHE_CHUNKS {
            let id = u32::try_from(chunk * REGISTRY_KEYS_PER_CHUNK + 1).unwrap();
            context
                .resolve_pubkey(&reader, CompactPubkey::Id(id))
                .unwrap();
        }
        assert_eq!(context.registry_chunks.len(), REGISTRY_CACHE_CHUNKS);
        assert!(!context.registry_chunks.contains_key(&0));
        let retained = context
            .registry_chunks
            .values()
            .map(|chunk| chunk.len() * REGISTRY_KEY_BYTES)
            .sum::<usize>();
        assert!(retained <= COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES);
    }
}
