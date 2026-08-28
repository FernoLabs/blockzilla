//! Source-neutral instruction projection for one Indexer V3 candidate.
//!
//! The adapter uses the frozen standalone V3 reader and its contiguous
//! semantic scanner. The V3 ledger files bind to each other through their
//! internal headers. The retained Compact V2 sidecars do not have a digest
//! binding to that candidate, so this adapter never claims publication
//! verification.

use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    sync::{Arc, Mutex},
};

use blockzilla_format::{
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_ERROR,
    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, CompactPubkey, OwnedCompactRecentBlockhash,
};
use blockzilla_query_sdk::{
    ArchiveFormat, ArchiveInstructionSource, BlockHeader, BlockSink, CanonicalBlock,
    CanonicalTransaction, CoverageReason, CpiCoverage, Error as QueryError, ExecutionStatus,
    InstructionCoordinate, InstructionCoverage, InstructionDataCoverage,
    InstructionDataRequirement, OrderedBlockPublisher, ResolvedInstruction, ScanIoReceipt,
    ScanReceipt, ScanRequest, SourceIdentity, SourceVerification, TransactionHeader,
};
use blockzilla_read_sdk::{
    BLOCKHASH_RECORD_LEN, BlockhashResolver, BlockhashResolverError, CompactV2ExecutionStatus,
    CompactV2MessageProjectionError, CompactV2MessageProjector, CompactV2MetadataProjectionError,
    CompactV2MetadataProjectionLimits, CompactV2MetadataProjector, MAX_BLOCKHASH_REGISTRY_BYTES,
    MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS, MAX_VOTE_HASH_REGISTRY_BYTES,
    PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN, PinnedLocalRangeSource, PreviousBlockhashTail,
    PreviousBlockhashTailSchema, ProjectedCompactV2Message, ProjectedCompactV2MessageVersion,
    RangeSource, ResolvedAddressTableLookup, SignedInstructionCandidates, SignedMessageCandidates,
    SignedMessageError, SignedMessageVersion, VOTE_HASH_RECORD_LEN, VoteHashRegistry,
    VoteHashResolver, parse_previous_blockhash_tail, select_signed_message_candidate_ed25519,
};
use thiserror::Error;

use crate::indexer_v3_wire::{
    BlockRow, INDEX_FILE, Object, Reader, SemanticTransaction, StandaloneFormat,
};

const REGISTRY_KEY_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 64;
const REGISTRY_KEYS_PER_CHUNK: usize = 2_048;
const REGISTRY_CACHE_CHUNKS: usize = 8;
const PREVIOUS_BLOCKHASH_RECORDS: usize = 300;
const MAX_SIGNATURE_BYTES_PER_BLOCK: usize = 256 * 1024 * 1024;

/// Maximum retained public-key payload bytes in the V3 registry chunk cache.
///
/// This value excludes map, vector, allocator, and LRU overhead.
pub const INDEXER_V3_QUERY_REGISTRY_RETAINED_KEY_BYTES: usize =
    REGISTRY_KEYS_PER_CHUNK * REGISTRY_KEY_BYTES * REGISTRY_CACHE_CHUNKS;

/// Return the required internally bound V3 ledger object names.
///
/// The order is stable: the block index is first, followed by the 11 plane
/// files in their wire object-ID order.
pub fn indexer_v3_required_ledger_objects() -> impl Iterator<Item = &'static str> + Clone {
    std::iter::once(INDEX_FILE).chain(Object::ALL.into_iter().map(Object::file_name))
}

/// Retained sidecars required for source-neutral V3 projection.
pub const INDEXER_V3_REQUIRED_RETAINED_SIDECARS: [&str; 1] = [ARCHIVE_V2_PUBKEY_REGISTRY_FILE];

/// Retained sidecars that are optional until exact message proof needs them.
pub const INDEXER_V3_OPTIONAL_RETAINED_SIDECARS: [&str; 4] = [
    ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
];

/// Scope declared by the frozen Indexer V3 header.
///
/// `FullSelection` means that the candidate writer did not use its benchmark
/// prefix option. It is not a publication or complete-epoch assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexerV3SourceScope {
    SelectedPrefix,
    FullSelection,
}

#[derive(Debug, Error)]
pub enum IndexerV3InstructionSourceError {
    #[error("Indexer V3 reader error: {0:#}")]
    Reader(#[source] anyhow::Error),

    #[error("Indexer V3 range source error: {0}")]
    RangeSource(#[from] blockzilla_read_sdk::SourceError),

    #[error("Indexer V3 message projection error: {0}")]
    Message(#[from] CompactV2MessageProjectionError),

    #[error("Indexer V3 metadata projection error: {0}")]
    Metadata(#[from] CompactV2MetadataProjectionError),

    #[error("Indexer V3 signed-message error: {0}")]
    SignedMessage(#[from] SignedMessageError),

    #[error("Indexer V3 blockhash error: {0}")]
    Blockhash(#[from] BlockhashResolverError),

    #[error("Indexer V3 sidecar {object} is required for {purpose}")]
    MissingSidecar {
        object: &'static str,
        purpose: &'static str,
    },

    #[error("cannot reserve memory for {context}: {source}")]
    Allocation {
        context: &'static str,
        #[source]
        source: std::collections::TryReserveError,
    },

    #[error("invalid Indexer V3 instruction source: {0}")]
    Invalid(String),
}

pub type IndexerV3InstructionSourceResult<T> =
    std::result::Result<T, IndexerV3InstructionSourceError>;

/// A sequential instruction source over one internally bound Indexer V3 candidate.
///
/// `registry.bin` is required. `signatures.bin` is optional, but selected
/// ambiguous instruction data cannot be published without its signature proof.
/// Blockhash, previous-blockhash, and vote-hash sidecars are loaded only when
/// exact selected message reconstruction needs them. Retained sidecars have no
/// digest binding to the V3 ledger candidate.
pub struct IndexerV3InstructionSource {
    reader: Reader,
    identity: SourceIdentity,
    scope: IndexerV3SourceScope,
    meter: Arc<CountingRangeSource>,
    context: ExactContext,
}

impl std::fmt::Debug for IndexerV3InstructionSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexerV3InstructionSource")
            .field("identity", &self.identity)
            .field("scope", &self.scope)
            .field("registry_entries", &self.context.registry_entries)
            .field(
                "signatures_available",
                &self.context.sidecars.signatures_size.is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl IndexerV3InstructionSource {
    /// Open a local candidate through one anchored, immutable file source.
    ///
    /// `candidate_binding` must be a stable operator identity for this exact
    /// candidate, such as a separately computed full-folder digest. The
    /// adapter records it for durable resume checks, but does not verify it.
    pub fn open_local(
        root: impl AsRef<Path>,
        first_slot: u64,
        candidate_binding: impl Into<String>,
    ) -> IndexerV3InstructionSourceResult<Self> {
        let root = root.as_ref();
        let allowed = local_source_objects();
        let source = PinnedLocalRangeSource::new_anchored(root, &allowed)?;
        Self::open_with_source(
            Arc::new(source),
            root.display().to_string(),
            first_slot,
            candidate_binding,
        )
    }

    /// Open a strict shared range source, including HTTPS or a cached HTTPS source.
    ///
    /// The same source instance serves the V3 ledger and all retained sidecars.
    /// `candidate_binding` must identify this exact candidate across restarts.
    /// It is recorded but not verified by this constructor. Constructor,
    /// header, and size work is outside later scan receipts.
    pub fn open_with_source(
        source: Arc<dyn RangeSource>,
        source_label: impl Into<String>,
        first_slot: u64,
        candidate_binding: impl Into<String>,
    ) -> IndexerV3InstructionSourceResult<Self> {
        let label = source_label.into();
        if label.is_empty() {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "source label is empty".into(),
            ));
        }
        let candidate_binding = candidate_binding.into();
        if candidate_binding.is_empty() {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "stable candidate binding is empty".into(),
            ));
        }
        let meter = Arc::new(CountingRangeSource::new(source));
        let shared_source: Arc<dyn RangeSource> = meter.clone();
        let reader = Reader::open_with_source(shared_source.clone(), label.clone())
            .map_err(IndexerV3InstructionSourceError::Reader)?;
        if reader.header.format != StandaloneFormat::V3 {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "standalone candidate is not format V3".into(),
            ));
        }

        let block_count = u32::try_from(reader.header.selected_blocks).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 block count exceeds the source-neutral u32 limit".into(),
            )
        })?;
        if u64::from(block_count) > reader.header.slots_per_epoch {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 has {block_count} block rows, above {} slots per epoch",
                reader.header.slots_per_epoch
            )));
        }
        let last_slot = first_slot
            .checked_add(reader.header.slots_per_epoch - 1)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "explicit V3 epoch slot range overflows u64".into(),
                )
            })?;
        validate_rows(&reader, first_slot, last_slot, block_count)?;

        let scope = if reader.header.prefix {
            IndexerV3SourceScope::SelectedPrefix
        } else {
            IndexerV3SourceScope::FullSelection
        };
        let sidecars = SidecarGeometry::inspect(shared_source.as_ref(), &reader, scope)?;
        let registry_entries = registry_entries(sidecars.registry_size)?;
        let message_schema = reader.message_schema();
        let identity = SourceIdentity {
            format: ArchiveFormat::IndexerV3,
            label,
            cluster_id: None,
            epoch: reader.header.epoch,
            first_slot,
            slots_per_epoch: reader.header.slots_per_epoch,
            block_count,
            verification: SourceVerification::InternalBindingOnly,
            // This is an explicit operator-supplied candidate identity. It is
            // durable resume state, not proof that the V3 headers bind the
            // retained registry or any other candidate content.
            binding: Some(candidate_binding),
        };

        Ok(Self {
            reader,
            identity,
            scope,
            meter,
            context: ExactContext::new(shared_source, registry_entries, sidecars, message_schema),
        })
    }

    pub const fn scope(&self) -> IndexerV3SourceScope {
        self.scope
    }

    pub const fn registry_entries(&self) -> u32 {
        self.context.registry_entries
    }

    pub const fn signatures_available(&self) -> bool {
        self.context.sidecars.signatures_size.is_some()
    }

    /// Return the validated slot for one dense block ordinal.
    pub fn block_slot(&self, ordinal: u32) -> Option<u64> {
        self.reader.block(ordinal as usize).map(|row| row.slot)
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
            .map_or(identity.block_count as usize, |range| {
                usize::try_from(
                    range
                        .first_block
                        .checked_add(range.block_count.get())
                        .expect("publisher validated the requested u32 range"),
                )
                .expect("u32 fits the supported address space")
            });

        let reader = &self.reader;
        let context = &mut self.context;
        let source_io_before = self.meter.stats().map_err(source_error)?;
        let message_schema = reader.message_schema();
        let metadata_schema = reader.metadata_schema();
        let mut decoded_bytes = 0u64;
        let mut scan = reader
            .begin_contiguous_semantic_scan(start..end)
            .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;

        for ordinal in start..end {
            let row = reader.block(ordinal).ok_or_else(|| {
                source_error(IndexerV3InstructionSourceError::Invalid(format!(
                    "V3 block ordinal {ordinal} is missing after open validation"
                )))
            })?;
            let block_signatures = context.read_block_signatures(row).map_err(source_error)?;
            let transaction_capacity = usize::try_from(row.tx_count).map_err(|_| {
                source_error(IndexerV3InstructionSourceError::Invalid(
                    "V3 transaction count exceeds address space".into(),
                ))
            })?;
            let mut transactions = Vec::new();
            reserve_exact(
                &mut transactions,
                transaction_capacity,
                "canonical V3 block transactions",
            )
            .map_err(source_error)?;
            let mut signature_cursor = 0u64;

            let stats = scan
                .visit_semantic_transactions(ordinal, None, |transaction| {
                    let signatures = transaction_signatures(
                        row,
                        &transaction,
                        block_signatures.as_deref(),
                        &mut signature_cursor,
                    )?;
                    let projected = Self::project_transaction(
                        context,
                        request,
                        message_schema,
                        metadata_schema,
                        transaction,
                        signatures,
                    )?;
                    transactions.push(projected);
                    Ok(())
                })
                .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;
            if signature_cursor != u64::from(row.signature_count) {
                return Err(source_error(IndexerV3InstructionSourceError::Invalid(
                    format!(
                        "V3 block {} transactions consume {signature_cursor} of {} signatures",
                        row.block_id, row.signature_count
                    ),
                )));
            }
            decoded_bytes = decoded_bytes
                .checked_add(stats.total_decoded_bytes())
                .ok_or_else(|| {
                    source_error(IndexerV3InstructionSourceError::Invalid(
                        "V3 decoded-byte count overflow".into(),
                    ))
                })?;

            publisher.publish(&CanonicalBlock {
                header: BlockHeader {
                    epoch: identity.epoch,
                    block_ordinal: row.block_id,
                    slot: row.slot,
                },
                transactions,
            })?;
        }
        scan.finish()
            .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;

        let source_io = self
            .meter
            .stats()
            .map_err(source_error)?
            .difference(source_io_before)
            .map_err(source_error)?;
        publisher.set_io_receipt(ScanIoReceipt {
            source_read_calls: Some(source_io.calls),
            source_read_bytes: Some(source_io.bytes),
            decoded_bytes: Some(decoded_bytes),
            cache_read_calls: None,
            cache_read_bytes: None,
        });
        publisher.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn project_transaction(
        context: &mut ExactContext,
        request: &ScanRequest,
        message_schema: blockzilla_read_sdk::CompactV2MessageSchema,
        metadata_schema: blockzilla_read_sdk::CompactV2MetadataSchema,
        transaction: SemanticTransaction<'_>,
        signatures: Option<&[[u8; 64]]>,
    ) -> anyhow::Result<CanonicalTransaction> {
        Self::project_transaction_inner(
            context,
            request,
            message_schema,
            metadata_schema,
            transaction,
            signatures,
        )
        .map_err(anyhow::Error::new)
    }

    #[allow(clippy::too_many_arguments)]
    fn project_transaction_inner(
        context: &mut ExactContext,
        request: &ScanRequest,
        message_schema: blockzilla_read_sdk::CompactV2MessageSchema,
        metadata_schema: blockzilla_read_sdk::CompactV2MetadataSchema,
        transaction: SemanticTransaction<'_>,
        signatures: Option<&[[u8; 64]]>,
    ) -> IndexerV3InstructionSourceResult<CanonicalTransaction> {
        let flags = u32::from(transaction.source_flags);
        let primary_signature = signatures.and_then(|values| values.first()).copied();
        if flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0
            && flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0
        {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "slot {} transaction {} has METADATA_RAW_FALLBACK without HAS_METADATA",
                transaction.slot, transaction.tx_index
            )));
        }
        if flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            return Ok(CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: transaction.tx_index,
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

        let projector = CompactV2MessageProjector::new(message_schema, context.registry_entries);
        let (message, static_keys) = Self::project_requested_message(
            context,
            projector,
            transaction.message,
            &request.instruction_data,
            !request.require_complete_instruction_data,
        )?;
        let is_v0 = matches!(
            message.version(),
            ProjectedCompactV2MessageVersion::V0 { .. }
        );
        require_flag(
            &transaction,
            ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
            "MESSAGE_V0",
            is_v0,
        )?;
        let signature_count = usize::try_from(
            transaction
                .signature_ordinals
                .end
                .checked_sub(transaction.signature_ordinals.start)
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 transaction signature range decreases".into(),
                    )
                })?,
        )
        .map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 transaction signature count exceeds address space".into(),
            )
        })?;
        if signature_count != usize::from(message.header().num_required_signatures) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "slot {} transaction {} has {signature_count} signature rows but requires {}",
                transaction.slot,
                transaction.tx_index,
                message.header().num_required_signatures
            )));
        }
        if let Some(signatures) = signatures
            && signatures.len() != signature_count
        {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "slot {} transaction {} signature window has the wrong length",
                transaction.slot, transaction.tx_index
            )));
        }

        let metadata = if flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            reject_flag(&transaction, ARCHIVE_V2_TX_FLAG_HAS_ERROR, "HAS_ERROR")?;
            reject_flag(
                &transaction,
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                "HAS_INNER_IX",
            )?;
            reject_flag(
                &transaction,
                ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                "HAS_LOADED_ADDRESSES",
            )?;
            if transaction.loaded_addresses.is_some()
                || transaction.inner_instructions.is_some()
                || transaction.outcome.is_some()
                || transaction.raw_metadata.is_some()
            {
                return Err(IndexerV3InstructionSourceError::Invalid(
                    "metadata-absent V3 transaction exposes metadata planes".into(),
                ));
            }
            ProjectedMetadata::Absent
        } else if flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
            if transaction.raw_metadata.is_none()
                || transaction.loaded_addresses.is_some()
                || transaction.inner_instructions.is_some()
                || transaction.outcome.is_some()
            {
                return Err(IndexerV3InstructionSourceError::Invalid(
                    "raw-metadata V3 transaction has inconsistent semantic planes".into(),
                ));
            }
            ProjectedMetadata::Raw
        } else {
            let loaded = required_plane(transaction.loaded_addresses, "loaded-addresses")?;
            let inner = required_plane(transaction.inner_instructions, "inner-instructions")?;
            let outcome = required_plane(transaction.outcome, "outcome")?;
            if transaction.raw_metadata.is_some() {
                return Err(IndexerV3InstructionSourceError::Invalid(
                    "decoded V3 metadata also contains raw fallback bytes".into(),
                ));
            }
            let limits = CompactV2MetadataProjectionLimits::for_message(&message);
            let metadata =
                CompactV2MetadataProjector::new(metadata_schema, context.registry_entries)
                    .project_split_planes(outcome, loaded, inner, limits)?;
            require_flag(
                &transaction,
                ARCHIVE_V2_TX_FLAG_HAS_ERROR,
                "HAS_ERROR",
                !metadata.execution_status.is_success(),
            )?;
            require_flag(
                &transaction,
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                "HAS_INNER_IX",
                metadata.inner_instructions.is_some(),
            )?;
            require_flag(
                &transaction,
                ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                "HAS_LOADED_ADDRESSES",
                !metadata.loaded_writable_addresses.is_empty()
                    || !metadata.loaded_readonly_addresses.is_empty(),
            )?;
            let expected_cpi_state = match &metadata.inner_instructions {
                None => 1,
                Some(groups) if groups.is_empty() => 2,
                Some(_) => 3,
            };
            if transaction.effect_state & 0b111 != expected_cpi_state {
                return Err(IndexerV3InstructionSourceError::Invalid(format!(
                    "slot {} transaction {} CPI state differs from its inner plane",
                    transaction.slot, transaction.tx_index
                )));
            }
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
                let total = metadata
                    .loaded_writable_addresses
                    .len()
                    .checked_add(metadata.loaded_readonly_addresses.len())
                    .ok_or_else(|| {
                        IndexerV3InstructionSourceError::Invalid(
                            "loaded-address count overflow".into(),
                        )
                    })?;
                let mut keys = Vec::new();
                reserve_exact(&mut keys, total, "resolved V3 loaded addresses")?;
                for reference in metadata
                    .loaded_writable_addresses
                    .iter()
                    .chain(&metadata.loaded_readonly_addresses)
                {
                    keys.push(context.resolve_pubkey(*reference)?);
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
            let total = static_keys
                .len()
                .checked_add(loaded_keys.len())
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "combined V3 message account count overflow".into(),
                    )
                })?;
            let mut account_keys = Vec::new();
            reserve_exact(&mut account_keys, total, "combined V3 message account keys")?;
            account_keys.extend_from_slice(&static_keys);
            account_keys.extend(loaded_keys);
            let instructions = Self::project_instructions(
                context,
                request,
                transaction.message,
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
        let signer_references = message
            .static_account_keys()
            .get(..required)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "required signer prefix exceeds projected static keys".into(),
                )
            })?;
        let mut required_signers = Vec::new();
        reserve_exact(
            &mut required_signers,
            signer_references.len(),
            "resolved V3 required signers",
        )?;
        for reference in signer_references {
            required_signers.push(context.resolve_pubkey(*reference)?);
        }

        Ok(CanonicalTransaction {
            header: TransactionHeader {
                tx_index: transaction.tx_index,
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
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        requirement: &InstructionDataRequirement,
        relaxed: bool,
    ) -> IndexerV3InstructionSourceResult<(ProjectedCompactV2Message<'a>, Vec<[u8; 32]>)> {
        match requirement {
            InstructionDataRequirement::All => {
                let message =
                    Self::project_all_with_vote_retry(context, projector, bytes, relaxed)?;
                let static_keys = Self::resolve_static_keys(context, &message)?;
                Ok((message, static_keys))
            }
            InstructionDataRequirement::None => {
                let message =
                    projector.project_with_instruction_data_for_programs(bytes, &[], None)?;
                let static_keys = Self::resolve_static_keys(context, &message)?;
                Ok((message, static_keys))
            }
            InstructionDataRequirement::Programs(programs) => {
                let unselected =
                    projector.project_with_instruction_data_for_programs(bytes, &[], None)?;
                let static_keys = Self::resolve_static_keys(context, &unselected)?;
                let mut selected_references = Vec::new();
                reserve_exact(
                    &mut selected_references,
                    unselected.instructions().len(),
                    "selected V3 program references",
                )?;
                for instruction in unselected.instructions() {
                    let index = usize::from(instruction.program_id_index());
                    let program = *static_keys.get(index).ok_or_else(|| {
                        IndexerV3InstructionSourceError::Invalid(
                            "projected V3 program index is outside static keys".into(),
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
        context: &mut ExactContext,
        message: &ProjectedCompactV2Message<'_>,
    ) -> IndexerV3InstructionSourceResult<Vec<[u8; 32]>> {
        let mut keys = Vec::new();
        reserve_exact(
            &mut keys,
            message.static_account_keys().len(),
            "resolved V3 static account keys",
        )?;
        for reference in message.static_account_keys() {
            keys.push(context.resolve_pubkey(*reference)?);
        }
        Ok(keys)
    }

    fn project_selected_with_vote_retry<'a>(
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        programs: &[CompactPubkey],
        relaxed: bool,
    ) -> IndexerV3InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        let strict = projector.project_with_instruction_data_for_programs(bytes, programs, None);
        match strict {
            Err(error) if needs_vote_hashes(&error) => {
                context.load_vote_hashes()?;
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
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        relaxed: bool,
    ) -> IndexerV3InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        let strict = projector.project(bytes, context.vote_hashes());
        match strict {
            Err(error) if needs_vote_hashes(&error) => {
                context.load_vote_hashes()?;
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
        context: &mut ExactContext,
        request: &ScanRequest,
        message_bytes: &[u8],
        message: &ProjectedCompactV2Message<'_>,
        metadata: &ProjectedMetadata<'_>,
        account_keys: &[[u8; 32]],
        signatures: Option<&[[u8; 64]]>,
    ) -> IndexerV3InstructionSourceResult<Vec<ResolvedInstruction>> {
        let has_selected_ambiguity = message.instructions().iter().any(|instruction| {
            instruction
                .data_candidates()
                .is_some_and(|candidates| candidates.len() > 1)
        });
        let mut selected_outer_data = if has_selected_ambiguity {
            match signatures {
                None if !request.require_complete_instruction_data => Some(
                    SelectedOuterData::Unknown(CoverageReason::InstructionDataUnavailable),
                ),
                None => {
                    return Err(IndexerV3InstructionSourceError::MissingSidecar {
                        object: ARCHIVE_V2_SIGNATURES_FILE,
                        purpose: "ambiguous selected instruction-data proof",
                    });
                }
                Some(signatures) => {
                    match Self::select_exact_outer_data(context, message_bytes, signatures) {
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
        let inner_count = inner_groups
            .into_iter()
            .flatten()
            .try_fold(0usize, |total, group| {
                total.checked_add(group.instructions.len()).ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 canonical instruction count overflow".into(),
                    )
                })
            })?;
        let output_count = message
            .instructions()
            .len()
            .checked_add(inner_count)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 canonical instruction count overflow".into(),
                )
            })?;
        let mut output = Vec::new();
        reserve_exact(&mut output, output_count, "canonical V3 instructions")?;
        let mut next_group = inner_groups.into_iter().flatten().peekable();

        for (outer_index, instruction) in message.instructions().iter().enumerate() {
            let program_id = resolve_index(account_keys, instruction.program_id_index())?;
            let accounts = resolve_indexes(account_keys, instruction.accounts())?;
            let (data_coverage, data) = match instruction.data_candidates() {
                None => (InstructionDataCoverage::NotRequested, Vec::new()),
                Some([]) => (
                    InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable),
                    Vec::new(),
                ),
                Some(candidates) if candidates.len() == 1 => (
                    InstructionDataCoverage::Exact,
                    copy_bytes(&candidates[0].bytes, "exact V3 outer instruction data")?,
                ),
                Some(_) => {
                    let selected = selected_outer_data.as_mut().ok_or_else(|| {
                        IndexerV3InstructionSourceError::Invalid(
                            "ambiguous selected V3 data was not signature-selected".into(),
                        )
                    })?;
                    match selected {
                        SelectedOuterData::Exact(selected) => {
                            let data = selected.get_mut(outer_index).ok_or_else(|| {
                                IndexerV3InstructionSourceError::Invalid(
                                    "selected signed V3 message has the wrong instruction count"
                                        .into(),
                                )
                            })?;
                            (InstructionDataCoverage::Exact, std::mem::take(data))
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
                let group = next_group.next().expect("peek proved a V3 CPI group");
                for (inner_index, inner) in group.instructions.iter().enumerate() {
                    let program_id = resolve_index_u32(account_keys, inner.program_id_index)?;
                    let accounts = resolve_indexes(account_keys, inner.accounts)?;
                    let selected =
                        instruction_data_required(&request.instruction_data, &program_id);
                    let (data_coverage, data) = if selected {
                        let mut data = Vec::new();
                        reserve_exact(&mut data, inner.data.len(), "selected V3 CPI data")?;
                        data.extend_from_slice(inner.data);
                        (InstructionDataCoverage::Exact, data)
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
            return Err(IndexerV3InstructionSourceError::Invalid(
                "V3 metadata CPI group has no matching outer instruction".into(),
            ));
        }
        Ok(output)
    }

    fn select_exact_outer_data(
        context: &mut ExactContext,
        message_bytes: &[u8],
        signatures: &[[u8; 64]],
    ) -> IndexerV3InstructionSourceResult<Vec<Vec<u8>>> {
        let projector =
            CompactV2MessageProjector::new(context.message_schema, context.registry_entries);
        let message = Self::project_all_with_vote_retry(context, projector, message_bytes, false)?;
        let static_keys = Self::resolve_static_keys(context, &message)?;
        let recent_blockhash = match message.recent_blockhash() {
            OwnedCompactRecentBlockhash::Nonce(hash) => *hash,
            OwnedCompactRecentBlockhash::Id(id)
                if *id < 0 && context.sidecars.previous_blockhash_size.is_none() =>
            {
                return Err(IndexerV3InstructionSourceError::MissingSidecar {
                    object: ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
                    purpose: "ambiguous signed-message previous blockhash",
                });
            }
            OwnedCompactRecentBlockhash::Id(id) => context.load_blockhashes()?.resolve(*id)?,
        };
        let resolved_lookups = match message.version() {
            ProjectedCompactV2MessageVersion::V0 {
                address_table_lookups,
            } => {
                let mut lookups = Vec::new();
                reserve_exact(
                    &mut lookups,
                    address_table_lookups.len(),
                    "resolved V3 address-table lookups",
                )?;
                for lookup in address_table_lookups {
                    lookups.push(ResolvedAddressTableLookup {
                        account_key: context.resolve_pubkey(lookup.account_key())?,
                        writable_indexes: lookup.writable_indexes(),
                        readonly_indexes: lookup.readonly_indexes(),
                    });
                }
                lookups
            }
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
        let mut candidates = Vec::new();
        reserve_exact(
            &mut candidates,
            message.instructions().len(),
            "signed V3 instruction candidates",
        )?;
        for (index, instruction) in message.instructions().iter().enumerate() {
            let data_candidates = instruction.data_candidates().ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(format!(
                    "full V3 signature projection omitted instruction {index} data"
                ))
            })?;
            candidates.push(SignedInstructionCandidates {
                program_id_index: instruction.program_id_index(),
                accounts: instruction.accounts(),
                data_candidates,
            });
        }
        let selected = select_signed_message_candidate_ed25519(
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

impl ArchiveInstructionSource for IndexerV3InstructionSource {
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

fn validate_rows(
    reader: &Reader,
    first_slot: u64,
    last_slot: u64,
    block_count: u32,
) -> IndexerV3InstructionSourceResult<()> {
    let mut previous_slot = None;
    for ordinal in 0..block_count {
        let row = reader.block(ordinal as usize).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(format!(
                "V3 row {ordinal} is missing from the validated index"
            ))
        })?;
        if row.block_id != ordinal {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 row {ordinal} has block ID {}",
                row.block_id
            )));
        }
        if row.slot < first_slot || row.slot > last_slot {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 block {} slot {} is outside explicit epoch slots {first_slot}..={last_slot}",
                row.block_id, row.slot
            )));
        }
        if previous_slot.is_some_and(|prior| row.slot <= prior) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 block {} slot {} is not after the previous slot",
                row.block_id, row.slot
            )));
        }
        previous_slot = Some(row.slot);
    }
    if reader.block(block_count as usize).is_some() {
        return Err(IndexerV3InstructionSourceError::Invalid(
            "V3 reader exposes rows beyond its header block count".into(),
        ));
    }
    Ok(())
}

fn local_source_objects() -> Vec<&'static str> {
    let mut objects = Vec::with_capacity(18);
    objects.extend(indexer_v3_required_ledger_objects());
    objects.extend(INDEXER_V3_REQUIRED_RETAINED_SIDECARS);
    objects.extend(INDEXER_V3_OPTIONAL_RETAINED_SIDECARS);
    objects
}

fn registry_entries(size: u64) -> IndexerV3InstructionSourceResult<u32> {
    if !size.is_multiple_of(REGISTRY_KEY_BYTES as u64) {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "{} has a partial public-key record",
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE
        )));
    }
    u32::try_from(size / REGISTRY_KEY_BYTES as u64).map_err(|_| {
        IndexerV3InstructionSourceError::Invalid(format!(
            "{} entry count exceeds u32",
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE
        ))
    })
}

#[derive(Debug, Clone, Copy)]
struct SidecarGeometry {
    registry_size: u64,
    signatures_size: Option<u64>,
    blockhash_size: Option<u64>,
    previous_blockhash_size: Option<u64>,
    vote_hash_size: Option<u64>,
}

impl SidecarGeometry {
    fn inspect(
        source: &dyn RangeSource,
        reader: &Reader,
        scope: IndexerV3SourceScope,
    ) -> IndexerV3InstructionSourceResult<Self> {
        let registry_size = source.size(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)?.ok_or(
            IndexerV3InstructionSourceError::MissingSidecar {
                object: ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
                purpose: "CompactPubkey resolution",
            },
        )?;
        registry_entries(registry_size)?;

        let total_signatures = reader
            .header
            .selected_blocks
            .checked_sub(1)
            .and_then(|ordinal| reader.block(ordinal as usize))
            .map_or(Ok(0u64), |row| {
                row.first_signature_ordinal
                    .checked_add(u64::from(row.signature_count))
                    .ok_or_else(|| {
                        IndexerV3InstructionSourceError::Invalid(
                            "V3 total signature count overflows u64".into(),
                        )
                    })
            })?;
        let expected_signature_bytes = total_signatures
            .checked_mul(SIGNATURE_BYTES as u64)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 total signature byte length overflows u64".into(),
                )
            })?;
        let signatures_size = source.size(ARCHIVE_V2_SIGNATURES_FILE)?;
        if signatures_size.is_some_and(|size| {
            !size.is_multiple_of(SIGNATURE_BYTES as u64)
                || match scope {
                    IndexerV3SourceScope::SelectedPrefix => size < expected_signature_bytes,
                    IndexerV3SourceScope::FullSelection => size != expected_signature_bytes,
                }
        }) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "{} length {:?} does not match the {:?} V3 signature geometry ending at {expected_signature_bytes}",
                ARCHIVE_V2_SIGNATURES_FILE, signatures_size, scope
            )));
        }

        let maximum_blockhash_bytes = reader
            .header
            .slots_per_epoch
            .checked_add(1)
            .and_then(|count| count.checked_mul(BLOCKHASH_RECORD_LEN as u64))
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 blockhash sidecar bound overflows u64".into(),
                )
            })?;
        let blockhash_size = source.size(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)?;
        if blockhash_size.is_some_and(|size| {
            size > maximum_blockhash_bytes
                || size > MAX_BLOCKHASH_REGISTRY_BYTES as u64
                || !size.is_multiple_of(BLOCKHASH_RECORD_LEN as u64)
        }) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "{} has invalid V3 geometry",
                ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE
            )));
        }

        let previous_blockhash_size = source.size(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)?;
        let expected_previous =
            (PREVIOUS_BLOCKHASH_RECORDS * PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN) as u64;
        if previous_blockhash_size.is_some_and(|size| size != expected_previous) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "{} is {previous_blockhash_size:?}, expected {expected_previous} bytes",
                ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE
            )));
        }

        let maximum_vote_bytes = reader
            .header
            .slots_per_epoch
            .checked_mul(VOTE_HASH_RECORD_LEN as u64)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 vote-hash sidecar bound overflows u64".into(),
                )
            })?;
        let vote_hash_size = source.size(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE)?;
        if vote_hash_size.is_some_and(|size| {
            size > maximum_vote_bytes
                || size > MAX_VOTE_HASH_REGISTRY_BYTES as u64
                || !size.is_multiple_of(VOTE_HASH_RECORD_LEN as u64)
        }) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "{} has invalid V3 geometry",
                ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE
            )));
        }

        Ok(Self {
            registry_size,
            signatures_size,
            blockhash_size,
            previous_blockhash_size,
            vote_hash_size,
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct CountingRangeSourceStats {
    calls: u64,
    bytes: u64,
}

impl CountingRangeSourceStats {
    fn difference(self, before: Self) -> IndexerV3InstructionSourceResult<Self> {
        Ok(Self {
            calls: self.calls.checked_sub(before.calls).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 source read count moved backwards".into(),
                )
            })?,
            bytes: self.bytes.checked_sub(before.bytes).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 source read bytes moved backwards".into(),
                )
            })?,
        })
    }
}

struct CountingRangeSource {
    inner: Arc<dyn RangeSource>,
    stats: Mutex<CountingRangeSourceStats>,
}

impl CountingRangeSource {
    fn new(inner: Arc<dyn RangeSource>) -> Self {
        Self {
            inner,
            stats: Mutex::new(CountingRangeSourceStats::default()),
        }
    }

    fn stats(&self) -> IndexerV3InstructionSourceResult<CountingRangeSourceStats> {
        self.stats.lock().map(|stats| *stats).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid("V3 source read counter is poisoned".into())
        })
    }

    fn record(&self, bytes: usize) -> blockzilla_read_sdk::SourceResult<()> {
        let bytes = u64::try_from(bytes).map_err(|_| {
            blockzilla_read_sdk::SourceError::Protocol("V3 returned-byte count exceeds u64".into())
        })?;
        let mut stats = self.stats.lock().map_err(|_| {
            blockzilla_read_sdk::SourceError::Protocol("V3 source read counter is poisoned".into())
        })?;
        stats.calls = stats.calls.checked_add(1).ok_or_else(|| {
            blockzilla_read_sdk::SourceError::Protocol("V3 source read-call count overflow".into())
        })?;
        stats.bytes = stats.bytes.checked_add(bytes).ok_or_else(|| {
            blockzilla_read_sdk::SourceError::Protocol("V3 source read-byte count overflow".into())
        })?;
        Ok(())
    }
}

impl RangeSource for CountingRangeSource {
    fn size(&self, object: &str) -> blockzilla_read_sdk::SourceResult<Option<u64>> {
        self.inner.size(object)
    }

    fn read_range(
        &self,
        object: &str,
        offset: u64,
        length: usize,
    ) -> blockzilla_read_sdk::SourceResult<Vec<u8>> {
        let bytes = self.inner.read_range(object, offset, length)?;
        self.record(bytes.len())?;
        Ok(bytes)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> blockzilla_read_sdk::SourceResult<()> {
        self.inner
            .read_range_into(object, offset, length, destination)?;
        self.record(destination.len())
    }
}

struct ExactContext {
    source: Arc<dyn RangeSource>,
    registry_entries: u32,
    sidecars: SidecarGeometry,
    registry_chunks: HashMap<u32, Vec<[u8; 32]>>,
    registry_lru: VecDeque<u32>,
    vote_hashes_loaded: bool,
    vote_hashes: Option<VoteHashRegistry>,
    blockhashes: Option<BlockhashResolver>,
    message_schema: blockzilla_read_sdk::CompactV2MessageSchema,
}

impl std::fmt::Debug for ExactContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExactContext")
            .field("registry_entries", &self.registry_entries)
            .field("sidecars", &self.sidecars)
            .field("registry_chunks", &self.registry_chunks.len())
            .field("vote_hashes_loaded", &self.vote_hashes_loaded)
            .field("blockhashes_loaded", &self.blockhashes.is_some())
            .finish_non_exhaustive()
    }
}

impl ExactContext {
    fn new(
        source: Arc<dyn RangeSource>,
        registry_entries: u32,
        sidecars: SidecarGeometry,
        message_schema: blockzilla_read_sdk::CompactV2MessageSchema,
    ) -> Self {
        Self {
            source,
            registry_entries,
            sidecars,
            registry_chunks: HashMap::new(),
            registry_lru: VecDeque::new(),
            vote_hashes_loaded: false,
            vote_hashes: None,
            blockhashes: None,
            message_schema,
        }
    }

    fn resolve_pubkey(
        &mut self,
        reference: CompactPubkey,
    ) -> IndexerV3InstructionSourceResult<[u8; 32]> {
        let CompactPubkey::Id(id) = reference else {
            let CompactPubkey::Raw(pubkey) = reference else {
                unreachable!("CompactPubkey has only raw and ID forms")
            };
            return Ok(pubkey);
        };
        if id == 0 || id > self.registry_entries {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 registry ID {id} is outside 1..={}",
                self.registry_entries
            )));
        }
        let zero_based = usize::try_from(id - 1).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid("V3 registry ID exceeds address space".into())
        })?;
        let chunk_id = u32::try_from(zero_based / REGISTRY_KEYS_PER_CHUNK).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid("V3 registry chunk ID exceeds u32".into())
        })?;
        self.ensure_registry_chunk(chunk_id)?;
        self.touch_registry_chunk(chunk_id);
        let index = zero_based % REGISTRY_KEYS_PER_CHUNK;
        self.registry_chunks
            .get(&chunk_id)
            .and_then(|chunk| chunk.get(index))
            .copied()
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(format!(
                    "V3 registry ID {id} is outside its loaded chunk"
                ))
            })
    }

    fn ensure_registry_chunk(&mut self, chunk_id: u32) -> IndexerV3InstructionSourceResult<()> {
        if self.registry_chunks.contains_key(&chunk_id) {
            return Ok(());
        }
        let first_key = usize::try_from(chunk_id)
            .ok()
            .and_then(|chunk| chunk.checked_mul(REGISTRY_KEYS_PER_CHUNK))
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid("V3 registry chunk offset overflow".into())
            })?;
        let entries = usize::try_from(self.registry_entries).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 registry entry count exceeds address space".into(),
            )
        })?;
        let key_count = entries
            .saturating_sub(first_key)
            .min(REGISTRY_KEYS_PER_CHUNK);
        if key_count == 0 {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 registry chunk {chunk_id} is outside the registry"
            )));
        }
        let offset = u64::try_from(first_key)
            .ok()
            .and_then(|key| key.checked_mul(REGISTRY_KEY_BYTES as u64))
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid("V3 registry byte offset overflow".into())
            })?;
        let length = key_count.checked_mul(REGISTRY_KEY_BYTES).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid("V3 registry chunk length overflow".into())
        })?;
        let bytes = read_exact(
            self.source.as_ref(),
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            offset,
            length,
        )?;
        let mut keys = Vec::new();
        reserve_exact(&mut keys, key_count, "V3 registry chunk keys")?;
        for bytes in bytes.chunks_exact(REGISTRY_KEY_BYTES) {
            let mut key = [0u8; 32];
            key.copy_from_slice(bytes);
            keys.push(key);
        }
        if keys.len() != key_count {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "V3 registry chunk has a partial public key".into(),
            ));
        }
        if self.registry_chunks.len() == REGISTRY_CACHE_CHUNKS
            && let Some(evicted) = self.registry_lru.pop_front()
        {
            self.registry_chunks.remove(&evicted);
        }
        self.registry_chunks.try_reserve(1).map_err(|source| {
            IndexerV3InstructionSourceError::Allocation {
                context: "V3 registry chunk map",
                source,
            }
        })?;
        self.registry_lru.try_reserve(1).map_err(|source| {
            IndexerV3InstructionSourceError::Allocation {
                context: "V3 registry chunk LRU",
                source,
            }
        })?;
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

    fn read_block_signatures(
        &mut self,
        row: &BlockRow,
    ) -> IndexerV3InstructionSourceResult<Option<Vec<[u8; 64]>>> {
        if self.sidecars.signatures_size.is_none() {
            return Ok(None);
        }
        let count = usize::try_from(row.signature_count).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 block signature count exceeds address space".into(),
            )
        })?;
        let length = count.checked_mul(SIGNATURE_BYTES).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 block signature byte length overflow".into(),
            )
        })?;
        let row_bound = usize::try_from(row.tx_count)
            .ok()
            .and_then(|count| count.checked_mul(usize::from(u8::MAX)))
            .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
            .unwrap_or(usize::MAX)
            .min(MAX_SIGNATURE_BYTES_PER_BLOCK);
        if length > row_bound {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 block {} signature window is {length} bytes, above {row_bound}",
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
                IndexerV3InstructionSourceError::Invalid(
                    "V3 block signature byte offset overflow".into(),
                )
            })?;
        let bytes = read_exact(
            self.source.as_ref(),
            ARCHIVE_V2_SIGNATURES_FILE,
            offset,
            length,
        )?;
        let mut signatures = Vec::new();
        reserve_exact(&mut signatures, count, "V3 block signatures")?;
        for bytes in bytes.chunks_exact(SIGNATURE_BYTES) {
            let mut signature = [0u8; 64];
            signature.copy_from_slice(bytes);
            signatures.push(signature);
        }
        Ok(Some(signatures))
    }

    fn vote_hashes(&self) -> Option<&dyn VoteHashResolver> {
        self.vote_hashes
            .as_ref()
            .map(|registry| registry as &dyn VoteHashResolver)
    }

    fn load_vote_hashes(&mut self) -> IndexerV3InstructionSourceResult<()> {
        if self.vote_hashes_loaded {
            return Ok(());
        }
        let Some(size) = self.sidecars.vote_hash_size else {
            self.vote_hashes_loaded = true;
            return Ok(());
        };
        if size > MAX_VOTE_HASH_REGISTRY_BYTES as u64 {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 vote-hash sidecar is {size} bytes, above the {MAX_VOTE_HASH_REGISTRY_BYTES}-byte practical limit"
            )));
        }
        let size = usize::try_from(size).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 vote-hash sidecar exceeds address space".into(),
            )
        })?;
        let bytes = read_exact(
            self.source.as_ref(),
            ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
            0,
            size,
        )?;
        self.vote_hashes = Some(VoteHashRegistry::from_bytes(&bytes)?);
        self.vote_hashes_loaded = true;
        Ok(())
    }

    fn load_blockhashes(&mut self) -> IndexerV3InstructionSourceResult<&BlockhashResolver> {
        if self.blockhashes.is_none() {
            let current_size = self.sidecars.blockhash_size.ok_or(
                IndexerV3InstructionSourceError::MissingSidecar {
                    object: ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                    purpose: "ambiguous signed-message recent blockhash",
                },
            )?;
            if current_size > MAX_BLOCKHASH_REGISTRY_BYTES as u64 {
                return Err(IndexerV3InstructionSourceError::Invalid(format!(
                    "V3 blockhash sidecar is {current_size} bytes, above the {MAX_BLOCKHASH_REGISTRY_BYTES}-byte practical limit"
                )));
            }
            let current_size = usize::try_from(current_size).map_err(|_| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 blockhash sidecar exceeds address space".into(),
                )
            })?;
            let current = read_exact(
                self.source.as_ref(),
                ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                0,
                current_size,
            )?;

            let previous = match self.sidecars.previous_blockhash_size {
                None => PreviousBlockhashTail {
                    schema: PreviousBlockhashTailSchema::CurrentHashAndSlot,
                    entries: Vec::new(),
                },
                Some(size) => {
                    let size = usize::try_from(size).map_err(|_| {
                        IndexerV3InstructionSourceError::Invalid(
                            "V3 previous-blockhash sidecar exceeds address space".into(),
                        )
                    })?;
                    let bytes = read_exact(
                        self.source.as_ref(),
                        ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
                        0,
                        size,
                    )?;
                    parse_previous_blockhash_tail(
                        &bytes,
                        PreviousBlockhashTailSchema::CurrentHashAndSlot,
                    )?
                }
            };
            self.blockhashes = Some(BlockhashResolver::from_bytes(&current, previous)?);
        }
        self.blockhashes.as_ref().ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 blockhash resolver was not initialized".into(),
            )
        })
    }
}

fn transaction_signatures<'a>(
    row: &BlockRow,
    transaction: &SemanticTransaction<'_>,
    block_signatures: Option<&'a [[u8; 64]]>,
    signature_cursor: &mut u64,
) -> anyhow::Result<Option<&'a [[u8; 64]]>> {
    let count = transaction
        .signature_ordinals
        .end
        .checked_sub(transaction.signature_ordinals.start)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature ordinal range decreases"))?;
    let expected_start = row
        .first_signature_ordinal
        .checked_add(*signature_cursor)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature ordinal start overflows"))?;
    let expected_end = expected_start
        .checked_add(count)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature ordinal end overflows"))?;
    if transaction.signature_ordinals != (expected_start..expected_end) {
        anyhow::bail!(
            "V3 transaction {} signature ordinals are not contiguous in block {}",
            transaction.tx_index,
            row.block_id
        );
    }
    let expected_byte_start = expected_start
        .checked_mul(SIGNATURE_BYTES as u64)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature byte start overflows"))?;
    let expected_byte_end = expected_end
        .checked_mul(SIGNATURE_BYTES as u64)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature byte end overflows"))?;
    if transaction.signature_bytes != (expected_byte_start..expected_byte_end) {
        anyhow::bail!(
            "V3 transaction {} signature byte range differs from its ordinals",
            transaction.tx_index
        );
    }
    let start = usize::try_from(*signature_cursor)
        .map_err(|_| anyhow::anyhow!("V3 signature cursor exceeds address space"))?;
    *signature_cursor = signature_cursor
        .checked_add(count)
        .ok_or_else(|| anyhow::anyhow!("V3 block signature cursor overflows"))?;
    let end = usize::try_from(*signature_cursor)
        .map_err(|_| anyhow::anyhow!("V3 signature end exceeds address space"))?;
    block_signatures
        .map(|signatures| {
            signatures.get(start..end).ok_or_else(|| {
                anyhow::anyhow!(
                    "V3 transaction {} signature range exceeds its block window",
                    transaction.tx_index
                )
            })
        })
        .transpose()
}

fn read_exact(
    source: &dyn RangeSource,
    object: &'static str,
    offset: u64,
    length: usize,
) -> IndexerV3InstructionSourceResult<Vec<u8>> {
    if length == 0 {
        return Ok(Vec::new());
    }
    let mut bytes = Vec::new();
    reserve_exact(&mut bytes, length, "V3 exact source read")?;
    source.read_range_into(object, offset, length, &mut bytes)?;
    if bytes.len() != length {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "short {object} read: got {}, expected {length}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn source_error(error: impl std::error::Error + Send + Sync + 'static) -> QueryError {
    QueryError::source(ArchiveFormat::IndexerV3, error)
}

fn reserve_exact<T>(
    values: &mut Vec<T>,
    additional: usize,
    context: &'static str,
) -> IndexerV3InstructionSourceResult<()> {
    values
        .try_reserve_exact(additional)
        .map_err(|source| IndexerV3InstructionSourceError::Allocation { context, source })
}

fn copy_bytes(bytes: &[u8], context: &'static str) -> IndexerV3InstructionSourceResult<Vec<u8>> {
    let mut output = Vec::new();
    reserve_exact(&mut output, bytes.len(), context)?;
    output.extend_from_slice(bytes);
    Ok(output)
}

fn required_plane<'a>(
    plane: Option<&'a [u8]>,
    name: &'static str,
) -> IndexerV3InstructionSourceResult<&'a [u8]> {
    plane.ok_or_else(|| {
        IndexerV3InstructionSourceError::Invalid(format!("decoded V3 metadata has no {name} plane"))
    })
}

fn require_flag(
    transaction: &SemanticTransaction<'_>,
    flag: u32,
    name: &str,
    expected: bool,
) -> IndexerV3InstructionSourceResult<()> {
    let actual = u32::from(transaction.source_flags) & flag != 0;
    if actual != expected {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "slot {} transaction {} {name} flag is {actual}, expected {expected}",
            transaction.slot, transaction.tx_index
        )));
    }
    Ok(())
}

fn reject_flag(
    transaction: &SemanticTransaction<'_>,
    flag: u32,
    name: &str,
) -> IndexerV3InstructionSourceResult<()> {
    require_flag(transaction, flag, name, false)
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

fn is_missing_instruction_proof(error: &IndexerV3InstructionSourceError) -> bool {
    matches!(
        error,
        IndexerV3InstructionSourceError::MissingSidecar { .. }
            | IndexerV3InstructionSourceError::Message(
                CompactV2MessageProjectionError::ExactInstructionData(
                    SignedMessageError::MissingVoteHashResolver { .. }
                )
            )
    )
}

fn is_unresolved_instruction_ambiguity(error: &IndexerV3InstructionSourceError) -> bool {
    matches!(
        error,
        IndexerV3InstructionSourceError::Message(
            CompactV2MessageProjectionError::CandidateCombinationLimit
                | CompactV2MessageProjectionError::ExactInstructionData(
                    SignedMessageError::AmbiguousInstructionEncoding { .. }
                )
        ) | IndexerV3InstructionSourceError::SignedMessage(
            SignedMessageError::AmbiguousInstructionEncoding { .. }
                | SignedMessageError::CandidateCombinationLimitExceeded { .. }
                | SignedMessageError::MultipleVerifiedMessageCandidates
        )
    )
}

fn resolve_index(
    account_keys: &[[u8; 32]],
    index: u8,
) -> IndexerV3InstructionSourceResult<[u8; 32]> {
    account_keys
        .get(usize::from(index))
        .copied()
        .ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(format!(
                "V3 message account index {index} is outside resolved keys"
            ))
        })
}

fn resolve_index_u32(
    account_keys: &[[u8; 32]],
    index: u32,
) -> IndexerV3InstructionSourceResult<[u8; 32]> {
    let index = usize::try_from(index).map_err(|_| {
        IndexerV3InstructionSourceError::Invalid(
            "V3 CPI account index exceeds address space".into(),
        )
    })?;
    account_keys.get(index).copied().ok_or_else(|| {
        IndexerV3InstructionSourceError::Invalid(format!(
            "V3 CPI account index {index} is outside resolved keys"
        ))
    })
}

fn resolve_indexes(
    account_keys: &[[u8; 32]],
    indexes: &[u8],
) -> IndexerV3InstructionSourceResult<Vec<[u8; 32]>> {
    let mut resolved = Vec::new();
    reserve_exact(
        &mut resolved,
        indexes.len(),
        "resolved V3 instruction accounts",
    )?;
    for index in indexes {
        resolved.push(resolve_index(account_keys, *index)?);
    }
    Ok(resolved)
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
) -> IndexerV3InstructionSourceResult<()> {
    let order = u32::try_from(output.len()).map_err(|_| {
        IndexerV3InstructionSourceError::Invalid("V3 instruction order exceeds u32".into())
    })?;
    let outer_index = u32::try_from(outer_index).map_err(|_| {
        IndexerV3InstructionSourceError::Invalid("V3 outer instruction index exceeds u32".into())
    })?;
    let inner_index = inner_index
        .map(|index| {
            u32::try_from(index).map_err(|_| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 inner instruction index exceeds u32".into(),
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
    Exact(blockzilla_read_sdk::ProjectedCompactV2Metadata<'a>),
}

enum SelectedOuterData {
    Exact(Vec<Vec<u8>>),
    Unknown(CoverageReason),
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU32,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use blockzilla_format::{
        ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
        ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotBlockIndexRow, ArchiveV2HotInstruction,
        ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
        ArchiveV2HotV0Message, ArchiveV2VoteHashRef, ArchiveV2VoteStateUpdate,
        ArchiveV2VoteTowerSync, CompactInnerInstruction, CompactInnerInstructions,
        CompactInstructionError, CompactLogStream, CompactMessageHeader, CompactMetaV1,
        CompactReward, CompactTokenBalance, CompactTransactionError,
        OwnedCompactAddressTableLookup, WincodeLeb128Config, wincode_leb128_config,
    };
    use blockzilla_query_sdk::{
        ArchiveInstructionSourceExt, CpiCoverage, Error as QueryError, ExecutionStatus,
        InstructionCoverage, InstructionDataCoverage, ScanRange,
    };
    use blockzilla_read_sdk::{
        LocalRangeSource, SignedInstruction, SignedMessage,
        reconstruct_instruction_data_candidates, serialize_signed_message,
    };
    use ed25519_dalek::{Signer, SigningKey};
    use tempfile::TempDir;
    use wincode::SchemaWrite;

    use super::*;
    use crate::indexer_v3_wire::{
        Binding, CompressionPlan, DecodedMetadataParts, SourceBlockCore, WorkerScratch, Writers,
        encode_block_v3,
    };

    const FIRST_SLOT: u64 = 700;
    const SIGNER: [u8; 32] = [1; 32];
    const PROGRAM: [u8; 32] = [2; 32];
    const LOOKUP_TABLE: [u8; 32] = [3; 32];
    const LOADED_WRITABLE: [u8; 32] = [4; 32];
    const LOADED_READONLY: [u8; 32] = [5; 32];
    const VOTE_PROGRAM: [u8; 32] = [6; 32];

    struct Fixture {
        directory: TempDir,
    }

    enum FixtureMetadata {
        Absent,
        Raw(Vec<u8>),
        Exact(Box<CompactMetaV1>),
    }

    struct FixtureTransaction {
        flags: u32,
        message: Vec<u8>,
        metadata: FixtureMetadata,
    }

    impl FixtureTransaction {
        fn exact(message: ArchiveV2HotMessagePayload, metadata: CompactMetaV1, flags: u32) -> Self {
            Self {
                flags: flags | ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                message: encode(&message),
                metadata: FixtureMetadata::Exact(Box::new(metadata)),
            }
        }

        fn absent(message: ArchiveV2HotMessagePayload) -> Self {
            Self {
                flags: 0,
                message: encode(&message),
                metadata: FixtureMetadata::Absent,
            }
        }

        fn raw_metadata(message: ArchiveV2HotMessagePayload) -> Self {
            Self {
                flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                message: encode(&message),
                metadata: FixtureMetadata::Raw(vec![0xde, 0xad]),
            }
        }

        fn raw_transaction(bytes: Vec<u8>) -> Self {
            Self {
                flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                message: bytes,
                metadata: FixtureMetadata::Absent,
            }
        }
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let plan = CompressionPlan::default_level_three();
            let binding = Binding {
                epoch: 7,
                slots_per_epoch: 100,
                selected_blocks: 2,
                selected_transactions: 1,
                message_schema: blockzilla_read_sdk::CompactV2MessageSchema::Current,
                metadata_schema: blockzilla_read_sdk::CompactV2MetadataSchema::CurrentTypedError,
                prefix: false,
            };
            let mut writers = Writers::create_v3(directory.path(), binding, plan).unwrap();
            let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
            let mut scratch = WorkerScratch::default();

            scratch.begin_block_v3();
            scratch.record_block_rewards(&[0]).unwrap();
            scratch.finish_block(0).unwrap();
            let block = encode_block_v3(&mut scratch, &mut compressor, plan).unwrap();
            writers
                .append(
                    source_row(0, FIRST_SLOT, 0, 0, 0, 0),
                    source_core(FIRST_SLOT - 1),
                    block,
                )
                .unwrap();

            let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: ArchiveV2HotInstructionData::Raw(vec![4, 5]),
                }],
            });
            let inner = Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: vec![6, 7],
                    stack_height: Some(2),
                }],
            }]);
            let metadata = CompactMetaV1 {
                err: None,
                fee: 5_000,
                pre_balances: vec![1, 2],
                post_balances: vec![1, 2],
                inner_instructions: inner,
                logs: None,
                pre_token_balances: Vec::new(),
                post_token_balances: Vec::new(),
                rewards: Vec::new(),
                loaded_writable_addresses: Vec::new(),
                loaded_readonly_addresses: Vec::new(),
                return_data: None,
                compute_units_consumed: Some(10),
                cost_units: None,
            };
            let parts = split_parts(&metadata);

            scratch.begin_block_v3();
            scratch
                .begin_transaction(
                    ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                    1,
                    &encode(&message),
                )
                .unwrap();
            scratch.record_decoded_metadata(parts.borrow()).unwrap();
            scratch.record_block_rewards(&[0]).unwrap();
            scratch.finish_block(1).unwrap();
            let block = encode_block_v3(&mut scratch, &mut compressor, plan).unwrap();
            writers
                .append(
                    source_row(1, FIRST_SLOT + 2, 1, 0, 1, 0),
                    source_core(FIRST_SLOT),
                    block,
                )
                .unwrap();
            writers.finish(2, 1).unwrap();

            let mut registry = Vec::new();
            registry.extend_from_slice(&SIGNER);
            registry.extend_from_slice(&PROGRAM);
            std::fs::write(
                directory.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
                registry,
            )
            .unwrap();
            std::fs::write(directory.path().join(ARCHIVE_V2_SIGNATURES_FILE), [9u8; 64]).unwrap();

            Self { directory }
        }

        fn build(
            registry: &[[u8; 32]],
            blocks: Vec<Vec<FixtureTransaction>>,
            signatures: Option<&[[u8; 64]]>,
            prefix: bool,
            slots_per_epoch: u64,
        ) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let plan = CompressionPlan::default_level_three();
            let selected_blocks = blocks.len() as u64;
            let selected_transactions = blocks.iter().map(Vec::len).sum::<usize>() as u64;
            let binding = Binding {
                epoch: 7,
                slots_per_epoch,
                selected_blocks,
                selected_transactions,
                message_schema: blockzilla_read_sdk::CompactV2MessageSchema::Current,
                metadata_schema: blockzilla_read_sdk::CompactV2MetadataSchema::CurrentTypedError,
                prefix,
            };
            let mut writers = Writers::create_v3(directory.path(), binding, plan).unwrap();
            let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
            let mut scratch = WorkerScratch::default();
            let mut first_transaction = 0u64;
            let mut first_signature = 0u64;

            for (block_id, transactions) in blocks.into_iter().enumerate() {
                scratch.begin_block_v3();
                for transaction in &transactions {
                    scratch
                        .begin_transaction(transaction.flags, 1, &transaction.message)
                        .unwrap();
                    match &transaction.metadata {
                        FixtureMetadata::Absent => scratch.record_missing_metadata().unwrap(),
                        FixtureMetadata::Raw(bytes) => scratch.record_raw_metadata(bytes).unwrap(),
                        FixtureMetadata::Exact(metadata) => {
                            let parts = split_parts(metadata);
                            scratch.record_decoded_metadata(parts.borrow()).unwrap();
                        }
                    }
                }
                scratch.record_block_rewards(&[0]).unwrap();
                scratch.finish_block(transactions.len() as u32).unwrap();
                let block = encode_block_v3(&mut scratch, &mut compressor, plan).unwrap();
                let transaction_count = transactions.len() as u32;
                let slot = FIRST_SLOT + block_id as u64 * 2;
                writers
                    .append(
                        source_row(
                            block_id as u32,
                            slot,
                            transaction_count,
                            first_transaction,
                            transaction_count,
                            first_signature,
                        ),
                        source_core(slot.saturating_sub(1)),
                        block,
                    )
                    .unwrap();
                first_transaction += u64::from(transaction_count);
                first_signature += u64::from(transaction_count);
            }
            writers
                .finish(selected_blocks, selected_transactions)
                .unwrap();

            std::fs::write(
                directory.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
                registry.iter().flatten().copied().collect::<Vec<_>>(),
            )
            .unwrap();
            if let Some(signatures) = signatures {
                std::fs::write(
                    directory.path().join(ARCHIVE_V2_SIGNATURES_FILE),
                    signatures.iter().flatten().copied().collect::<Vec<_>>(),
                )
                .unwrap();
            }
            Self { directory }
        }

        fn open(&self, binding: &str) -> IndexerV3InstructionSource {
            IndexerV3InstructionSource::open_local(self.directory.path(), FIRST_SLOT, binding)
                .unwrap()
        }
    }

    fn source_row(
        block_id: u32,
        slot: u64,
        tx_count: u32,
        first_tx_ordinal: u64,
        signature_count: u32,
        first_signature_ordinal: u64,
    ) -> ArchiveV2HotBlockIndexRow {
        ArchiveV2HotBlockIndexRow {
            block_id,
            slot,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count,
            first_tx_ordinal,
            first_signature_ordinal,
            signature_count,
        }
    }

    fn source_core(parent_slot: u64) -> SourceBlockCore {
        SourceBlockCore {
            parent_slot,
            blockhash_id: 1,
            previous_blockhash_id: 0,
            block_time: None,
            block_height: None,
        }
    }

    fn encode<T: SchemaWrite<WincodeLeb128Config, Src = T>>(value: &T) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn message_header(unsigned_accounts: u8) -> CompactMessageHeader {
        CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: unsigned_accounts,
        }
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

    fn legacy_message(
        account_keys: Vec<CompactPubkey>,
        instructions: Vec<ArchiveV2HotInstruction>,
    ) -> ArchiveV2HotMessagePayload {
        ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: message_header((account_keys.len() - 1) as u8),
            account_keys,
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([13; 32]),
            instructions,
        })
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
            compute_units_consumed: Some(10),
            cost_units: None,
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

    struct SplitParts {
        outcome_head: Vec<u8>,
        pre_balances: Vec<u8>,
        post_balances: Vec<u8>,
        inner_instructions: Vec<u8>,
        logs: Vec<u8>,
        pre_token_balances: Vec<u8>,
        post_token_balances: Vec<u8>,
        transaction_rewards: Vec<u8>,
        loaded_writable: Vec<u8>,
        loaded_readonly: Vec<u8>,
        outcome_tail: Vec<u8>,
        effect_state: u8,
    }

    impl SplitParts {
        fn borrow(&self) -> DecodedMetadataParts<'_> {
            DecodedMetadataParts {
                outcome_head: &self.outcome_head,
                pre_balances: &self.pre_balances,
                post_balances: &self.post_balances,
                inner_instructions: &self.inner_instructions,
                logs: &self.logs,
                pre_token_balances: &self.pre_token_balances,
                post_token_balances: &self.post_token_balances,
                transaction_rewards: &self.transaction_rewards,
                loaded_writable: &self.loaded_writable,
                loaded_readonly: &self.loaded_readonly,
                outcome_tail: &self.outcome_tail,
                effect_state: self.effect_state,
            }
        }
    }

    fn split_parts(metadata: &CompactMetaV1) -> SplitParts {
        let mut outcome_head = encode(&metadata.err);
        outcome_head.extend(encode(&metadata.fee));
        let mut outcome_tail = encode(&metadata.return_data);
        outcome_tail.extend(encode(&metadata.compute_units_consumed));
        outcome_tail.extend(encode(&metadata.cost_units));
        let cpi_state = match &metadata.inner_instructions {
            None => 1,
            Some(groups) if groups.is_empty() => 2,
            Some(_) => 3,
        };
        SplitParts {
            outcome_head,
            pre_balances: encode(&metadata.pre_balances),
            post_balances: encode(&metadata.post_balances),
            inner_instructions: encode(&metadata.inner_instructions),
            logs: encode::<Option<CompactLogStream>>(&metadata.logs),
            pre_token_balances: encode::<Vec<CompactTokenBalance>>(&metadata.pre_token_balances),
            post_token_balances: encode::<Vec<CompactTokenBalance>>(&metadata.post_token_balances),
            transaction_rewards: encode::<Vec<CompactReward>>(&metadata.rewards),
            loaded_writable: encode(&metadata.loaded_writable_addresses),
            loaded_readonly: encode(&metadata.loaded_readonly_addresses),
            outcome_tail,
            effect_state: cpi_state | (1 << 3) | (1 << 4),
        }
    }

    #[test]
    fn local_v3_fixture_projects_empty_block_outer_cpi_and_exact_io() {
        let fixture = Fixture::new();
        let mut source = fixture.open("fixture-binding-a");
        assert_eq!(source.scope(), IndexerV3SourceScope::FullSelection);
        assert_eq!(
            source.identity().verification,
            SourceVerification::InternalBindingOnly
        );
        assert_eq!(
            source.identity().binding.as_deref(),
            Some("fixture-binding-a")
        );
        assert!(source.signatures_available());

        let request = ScanRequest::all().allow_unverified_source();
        let mut blocks = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                blocks.push((block.header, block.transactions.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].0.block_ordinal, 0);
        assert_eq!(blocks[0].0.slot, FIRST_SLOT);
        assert!(blocks[0].1.is_empty());
        let transaction = &blocks[1].1[0];
        assert_eq!(transaction.primary_signature, Some([9; 64]));
        assert_eq!(transaction.required_signers, [SIGNER]);
        assert_eq!(transaction.instructions.len(), 2);
        assert_eq!(transaction.instructions[0].program_id, PROGRAM);
        assert_eq!(transaction.instructions[0].data, [4, 5]);
        assert_eq!(
            transaction.instructions[0].data_coverage,
            InstructionDataCoverage::Exact
        );
        assert_eq!(transaction.instructions[1].coordinate.outer_index, 0);
        assert_eq!(transaction.instructions[1].coordinate.inner_index, Some(0));
        assert_eq!(transaction.instructions[1].coordinate.stack_height, Some(2));
        assert_eq!(transaction.instructions[1].data, [6, 7]);
        assert_eq!(receipt.blocks, 2);
        assert_eq!(receipt.transactions, 1);
        assert_eq!(receipt.instructions, 2);
        assert!(receipt.io.source_read_calls.is_some_and(|calls| calls > 0));
        assert!(receipt.io.source_read_bytes.is_some_and(|bytes| bytes > 0));
        assert!(receipt.io.decoded_bytes.is_some_and(|bytes| bytes > 0));
    }

    #[test]
    fn bounded_empty_row_reads_only_its_directory_and_binding_changes_identity() {
        let fixture = Fixture::new();
        let mut first = fixture.open("fixture-binding-a");
        let second = fixture.open("fixture-binding-b");
        assert_ne!(first.identity(), second.identity());
        assert!(
            IndexerV3InstructionSource::open_local(fixture.directory.path(), FIRST_SLOT, "")
                .is_err()
        );

        let request = ScanRequest::bounded(ScanRange {
            first_block: 0,
            block_count: NonZeroU32::new(1).unwrap(),
        })
        .allow_unverified_source();
        let receipt = first.for_each_block(&request, |_| Ok(())).unwrap();
        assert_eq!(receipt.blocks, 1);
        assert_eq!(receipt.transactions, 0);
        assert_eq!(receipt.io.source_read_calls, Some(1));
        assert!(receipt.io.source_read_bytes.is_some_and(|bytes| bytes > 0));
        assert!(receipt.io.decoded_bytes.is_some_and(|bytes| bytes > 0));
    }

    #[test]
    fn direct_v3_states_cover_raw_absent_raw_metadata_cpi_and_failure() {
        let base_message = || {
            legacy_message(
                vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                vec![raw_instruction(1, &[0], &[7])],
            )
        };
        let transactions = vec![
            FixtureTransaction::raw_transaction(vec![0xff]),
            FixtureTransaction::absent(base_message()),
            FixtureTransaction::raw_metadata(base_message()),
            FixtureTransaction::exact(base_message(), metadata(2, None, None, vec![], vec![]), 0),
            FixtureTransaction::exact(
                base_message(),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            ),
            FixtureTransaction::exact(
                base_message(),
                metadata(
                    2,
                    Some(CompactTransactionError::InstructionError(
                        0,
                        CompactInstructionError::Custom(42),
                    )),
                    Some(Vec::new()),
                    vec![],
                    vec![],
                ),
                ARCHIVE_V2_TX_FLAG_HAS_ERROR | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            ),
        ];
        let fixture = Fixture::build(&[SIGNER, PROGRAM], vec![transactions], None, false, 100);
        let mut source = fixture.open("direct-state-fixture");
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution();
        let mut observed = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                observed.extend(block.transactions.to_vec());
                Ok(())
            })
            .unwrap();

        assert_eq!(observed.len(), 6);
        assert_eq!(
            observed[0].header.instruction_coverage,
            InstructionCoverage::Unknown(CoverageReason::RawTransaction)
        );
        assert_eq!(
            observed[1].header.status,
            ExecutionStatus::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            observed[1].header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            observed[2].header.status,
            ExecutionStatus::Unknown(CoverageReason::RawMetadata)
        );
        assert_eq!(observed[3].header.cpi_coverage, CpiCoverage::NotRecorded);
        assert_eq!(observed[4].header.cpi_coverage, CpiCoverage::Complete);
        assert_eq!(observed[5].header.status, ExecutionStatus::Failed);
        assert_eq!(observed[5].header.failed_outer_instruction_index, Some(0));
        assert_eq!(receipt.transactions, 6);
        assert_eq!(receipt.transactions_with_unknown_execution, 3);
        assert_eq!(receipt.transactions_with_incomplete_instructions, 1);
        assert!(receipt.io.source_read_calls.is_some_and(|calls| calls > 0));
    }

    #[test]
    fn direct_v3_v0_resolves_loaded_writable_and_readonly_accounts() {
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: message_header(1),
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([14; 32]),
            instructions: vec![raw_instruction(1, &[2, 3], &[9, 8])],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(3),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let transaction = FixtureTransaction::exact(
            message,
            metadata(
                4,
                None,
                Some(Vec::new()),
                vec![CompactPubkey::Id(4)],
                vec![CompactPubkey::Id(5)],
            ),
            ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let fixture = Fixture::build(
            &[
                SIGNER,
                PROGRAM,
                LOOKUP_TABLE,
                LOADED_WRITABLE,
                LOADED_READONLY,
            ],
            vec![vec![transaction]],
            None,
            false,
            100,
        );
        let mut source = fixture.open("v0-loaded-fixture");
        let request = ScanRequest::all()
            .allow_unverified_source()
            .with_instruction_data_for([PROGRAM]);
        let mut observed = None;
        source
            .for_each_block(&request, |block| {
                observed = block.transactions.first().cloned();
                Ok(())
            })
            .unwrap();
        let transaction = observed.unwrap();
        assert_eq!(transaction.required_signers, [SIGNER]);
        assert_eq!(transaction.instructions.len(), 1);
        assert_eq!(transaction.instructions[0].program_id, PROGRAM);
        assert_eq!(
            transaction.instructions[0].accounts,
            [LOADED_WRITABLE, LOADED_READONLY]
        );
        assert_eq!(transaction.instructions[0].data, [9, 8]);
        assert_eq!(
            transaction.instructions[0].data_coverage,
            InstructionDataCoverage::Exact
        );
    }

    #[test]
    fn direct_v3_signature_ambiguity_is_exact_or_explicitly_unavailable() {
        let signing_key = SigningKey::from_bytes(&[44; 32]);
        let signer = signing_key.verifying_key().to_bytes();
        let compact_data = vote_tower_data(false);
        let candidates = reconstruct_instruction_data_candidates(&compact_data, None).unwrap();
        assert_eq!(candidates.len(), 2);
        let selected_data = candidates[1].bytes.clone();
        let signed = serialize_signed_message(&SignedMessage {
            version: SignedMessageVersion::Legacy,
            header: message_header(1),
            static_account_keys: &[signer, VOTE_PROGRAM],
            recent_blockhash: [13; 32],
            instructions: &[SignedInstruction {
                program_id_index: 1,
                accounts: &[],
                data: &selected_data,
            }],
        })
        .unwrap();
        let signature = signing_key.sign(&signed).to_bytes();
        let transaction = || {
            FixtureTransaction::exact(
                legacy_message(
                    vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                    vec![ArchiveV2HotInstruction {
                        program_id_index: 1,
                        accounts: Vec::new(),
                        data: vote_tower_data(false),
                    }],
                ),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )
        };

        let proved = Fixture::build(
            &[signer, VOTE_PROGRAM],
            vec![vec![transaction()]],
            Some(&[signature]),
            false,
            100,
        );
        let mut source = proved.open("proved-ambiguity");
        let mut exact = None;
        source
            .for_each_transaction(
                &ScanRequest::all().allow_unverified_source(),
                |transaction| {
                    exact = Some(transaction.instructions[0].data.clone());
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(exact, Some(selected_data));

        let missing = Fixture::build(
            &[signer, VOTE_PROGRAM],
            vec![vec![transaction()]],
            None,
            false,
            100,
        );
        let mut strict = missing.open("missing-ambiguity-strict");
        assert!(
            strict
                .for_each_block(&ScanRequest::all().allow_unverified_source(), |_| Ok(()))
                .is_err()
        );
        let mut relaxed = missing.open("missing-ambiguity-relaxed");
        let mut coverage = None;
        let receipt = relaxed
            .for_each_transaction(
                &ScanRequest::all()
                    .allow_unverified_source()
                    .allow_incomplete_instruction_data(),
                |transaction| {
                    coverage = Some((
                        transaction.instructions[0].data_coverage,
                        transaction.instructions[0].data.clone(),
                    ));
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            coverage,
            Some((
                InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable),
                Vec::new()
            ))
        );
        assert_eq!(receipt.instructions_with_unknown_data, 1);

        let inconsistent = Fixture::build(
            &[signer, VOTE_PROGRAM],
            vec![vec![transaction()]],
            Some(&[[0; 64]]),
            false,
            100,
        );
        let mut inconsistent = inconsistent.open("bad-signature-ambiguity");
        assert!(
            inconsistent
                .for_each_block(
                    &ScanRequest::all()
                        .allow_unverified_source()
                        .allow_incomplete_instruction_data(),
                    |_| Ok(()),
                )
                .is_err()
        );
    }

    #[test]
    fn relaxed_v3_preserves_exact_data_across_missing_vote_and_blockhash_proof() {
        let vote_transaction = || {
            FixtureTransaction::exact(
                legacy_message(
                    vec![
                        CompactPubkey::Id(1),
                        CompactPubkey::Id(2),
                        CompactPubkey::Id(3),
                    ],
                    vec![
                        raw_instruction(1, &[0], &[3, 11]),
                        ArchiveV2HotInstruction {
                            program_id_index: 2,
                            accounts: Vec::new(),
                            data: vote_tower_data(true),
                        },
                    ],
                ),
                metadata(3, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )
        };
        let missing_vote = Fixture::build(
            &[SIGNER, PROGRAM, VOTE_PROGRAM],
            vec![vec![vote_transaction()]],
            None,
            false,
            100,
        );
        let mut source = missing_vote.open("missing-vote-proof");
        let mut coverage = Vec::new();
        let receipt =
            source
                .for_each_transaction(
                    &ScanRequest::all()
                        .allow_unverified_source()
                        .allow_incomplete_instruction_data(),
                    |transaction| {
                        coverage.extend(transaction.instructions.iter().map(|instruction| {
                            (instruction.data_coverage, instruction.data.clone())
                        }));
                        Ok(())
                    },
                )
                .unwrap();
        assert_eq!(coverage[0], (InstructionDataCoverage::Exact, vec![3, 11]));
        assert_eq!(
            coverage[1],
            (
                InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable),
                Vec::new()
            )
        );
        assert_eq!(receipt.instructions_with_unknown_data, 1);

        let malformed_vote = Fixture::build(
            &[SIGNER, PROGRAM, VOTE_PROGRAM],
            vec![vec![vote_transaction()]],
            None,
            false,
            100,
        );
        let mut invalid_vote_row = vec![0u8; VOTE_HASH_RECORD_LEN];
        invalid_vote_row[0] = 4;
        std::fs::write(
            malformed_vote
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            invalid_vote_row,
        )
        .unwrap();
        let mut malformed_source = malformed_vote.open("malformed-vote-proof");
        assert!(
            malformed_source
                .for_each_block(
                    &ScanRequest::all()
                        .allow_unverified_source()
                        .allow_incomplete_instruction_data(),
                    |_| Ok(()),
                )
                .is_err()
        );

        let incomplete_vote = Fixture::build(
            &[SIGNER, PROGRAM, VOTE_PROGRAM],
            vec![vec![vote_transaction()]],
            None,
            false,
            100,
        );
        std::fs::write(
            incomplete_vote
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            [],
        )
        .unwrap();
        let mut incomplete_source = incomplete_vote.open("incomplete-vote-proof");
        assert!(
            incomplete_source
                .for_each_block(
                    &ScanRequest::all()
                        .allow_unverified_source()
                        .allow_incomplete_instruction_data(),
                    |_| Ok(()),
                )
                .is_err(),
            "a present registry with no referenced row is a hard source error"
        );

        let blockhash_message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: message_header(2),
            account_keys: vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![
                raw_instruction(1, &[0], &[5]),
                ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: Vec::new(),
                    data: vote_tower_data(false),
                },
            ],
        });
        let missing_blockhash = Fixture::build(
            &[SIGNER, PROGRAM, VOTE_PROGRAM],
            vec![vec![FixtureTransaction::exact(
                blockhash_message,
                metadata(3, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )]],
            Some(&[[0; 64]]),
            false,
            100,
        );
        let mut source = missing_blockhash.open("missing-blockhash-proof");
        let mut coverage = Vec::new();
        source
            .for_each_transaction(
                &ScanRequest::all()
                    .allow_unverified_source()
                    .allow_incomplete_instruction_data(),
                |transaction| {
                    coverage.extend(
                        transaction.instructions.iter().map(|instruction| {
                            (instruction.data_coverage, instruction.data.clone())
                        }),
                    );
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(coverage[0], (InstructionDataCoverage::Exact, vec![5]));
        assert_eq!(
            coverage[1].0,
            InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable)
        );
        assert!(coverage[1].1.is_empty());
    }

    #[test]
    fn direct_v3_prefix_and_full_signature_geometry_are_distinct() {
        let ledger_objects = indexer_v3_required_ledger_objects().collect::<Vec<_>>();
        assert_eq!(ledger_objects.len(), 12);
        assert_eq!(ledger_objects[0], INDEX_FILE);
        assert_eq!(INDEXER_V3_REQUIRED_RETAINED_SIDECARS, ["registry.bin"]);
        assert_eq!(INDEXER_V3_OPTIONAL_RETAINED_SIDECARS.len(), 4);

        let transaction = || {
            FixtureTransaction::exact(
                legacy_message(
                    vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                    vec![raw_instruction(1, &[0], &[1])],
                ),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )
        };
        let signatures = [[8; 64], [9; 64]];
        let prefix = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![transaction()]],
            Some(&signatures),
            true,
            100,
        );
        let prefix_source = prefix.open("prefix-extra-signatures");
        assert_eq!(prefix_source.scope(), IndexerV3SourceScope::SelectedPrefix);

        let full_extra = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![transaction()]],
            Some(&signatures),
            false,
            100,
        );
        assert!(
            IndexerV3InstructionSource::open_local(
                full_extra.directory.path(),
                FIRST_SLOT,
                "full-extra-signatures",
            )
            .is_err()
        );

        let full_exact = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![transaction()]],
            Some(&signatures[..1]),
            false,
            100,
        );
        let full_source = full_exact.open("full-exact-signatures");
        assert_eq!(full_source.scope(), IndexerV3SourceScope::FullSelection);
    }

    #[test]
    fn v3_sidecar_practical_caps_reject_before_body_reads() {
        struct SizeOverrideSource {
            inner: LocalRangeSource,
            object: &'static str,
            size: u64,
            body_reads: Arc<AtomicUsize>,
        }

        impl RangeSource for SizeOverrideSource {
            fn size(&self, object: &str) -> blockzilla_read_sdk::SourceResult<Option<u64>> {
                if object == self.object {
                    Ok(Some(self.size))
                } else {
                    self.inner.size(object)
                }
            }

            fn read_range(
                &self,
                object: &str,
                offset: u64,
                length: usize,
            ) -> blockzilla_read_sdk::SourceResult<Vec<u8>> {
                if object == self.object {
                    self.body_reads.fetch_add(1, Ordering::Relaxed);
                }
                self.inner.read_range(object, offset, length)
            }
        }

        let fixture = Fixture::build(&[SIGNER, PROGRAM], vec![Vec::new()], None, false, 3_000_000);
        for (object, cap, record_len) in [
            (
                ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                MAX_BLOCKHASH_REGISTRY_BYTES,
                BLOCKHASH_RECORD_LEN,
            ),
            (
                ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
                MAX_VOTE_HASH_REGISTRY_BYTES,
                VOTE_HASH_RECORD_LEN,
            ),
        ] {
            let oversized = (cap / record_len + 1) * record_len;
            let body_reads = Arc::new(AtomicUsize::new(0));
            let source = SizeOverrideSource {
                inner: LocalRangeSource::new(fixture.directory.path()),
                object,
                size: oversized as u64,
                body_reads: body_reads.clone(),
            };
            let error = IndexerV3InstructionSource::open_with_source(
                Arc::new(source),
                format!("oversized-{object}"),
                FIRST_SLOT,
                format!("oversized-{object}-binding"),
            )
            .expect_err("oversized sidecar must fail at open");
            assert!(error.to_string().contains("invalid V3 geometry"), "{error}");
            assert_eq!(body_reads.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn direct_v3_rejects_invalid_registry_id_and_stops_on_sink_error() {
        let invalid = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![FixtureTransaction::exact(
                legacy_message(
                    vec![CompactPubkey::Id(1), CompactPubkey::Id(3)],
                    vec![raw_instruction(1, &[0], &[1])],
                ),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )]],
            None,
            false,
            100,
        );
        let mut invalid_source = invalid.open("invalid-registry-id");
        assert!(
            invalid_source
                .for_each_block(&ScanRequest::all().allow_unverified_source(), |_| Ok(()))
                .is_err()
        );

        let transaction = || {
            FixtureTransaction::exact(
                legacy_message(
                    vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                    vec![raw_instruction(1, &[0], &[1])],
                ),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )
        };
        let fixture = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![transaction()], vec![transaction()]],
            None,
            false,
            100,
        );
        let mut source = fixture.open("sink-stop");
        let mut visits = 0usize;
        let error = source
            .for_each_block(&ScanRequest::all().allow_unverified_source(), |_| {
                visits += 1;
                Err(QueryError::InvalidTransaction("test sink stop".into()))
            })
            .unwrap_err();
        assert_eq!(visits, 1);
        assert!(error.to_string().contains("test sink stop"));
    }
}
