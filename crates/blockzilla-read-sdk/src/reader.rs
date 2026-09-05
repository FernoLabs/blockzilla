use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::Read,
    ops::Range,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread,
    time::{Duration, Instant},
};

use blockzilla_format::{
    ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES, ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY,
    ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ARCHIVE_V2_HOT_INDEX_HEADER_LEN,
    ARCHIVE_V2_HOT_INDEX_MAGIC, ARCHIVE_V2_HOT_INDEX_ROW_LEN, ARCHIVE_V2_HOT_INDEX_VERSION,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader,
    ArchiveV2HotBlockIndex, ArchiveV2HotBlockIndexRow, ArchiveV2HotMessagePayload,
    ArchiveV2HotMetaRecord, ArchiveV2HotTxRow, ArchiveV2HotTxRowIter,
    BorrowedArchiveV2HotBlockBlob, BorrowedArchiveV2HotBlockBlobWithoutRewards, CompactMetaV1,
    CompactPubkey, WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS,
    WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY, WINCODE_ARCHIVE_V2_FLAG_LEB128,
    WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
    WincodeArchiveV2Footer, WincodeArchiveV2Genesis, bounded_wincode_leb128_config,
    canonicalize_archive_v2_metadata_owned, deserialize_archive_v2_hot_block_blob,
    deserialize_archive_v2_hot_block_blob_borrowed_current,
    deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards,
};
use rayon::prelude::*;
use sha2::{Digest, Sha256};

use crate::{
    ArchiveV2MessageProjector, ArchiveV2MetadataProfileAdmission,
    ArchiveV2MetadataProjectionLimits, ArchiveV2MetadataWireProfile, ArchiveV2WireProfile,
    CURRENT_TYPED_ERRORS_MARKER_FILE, Error, POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
    PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE, ProjectedArchiveV2MetadataPrefix, Result,
    manifest::{
        BLOCK_INDEX_FILE, BLOCKS_FILE, GENERATION_MANIFEST_FILE, GENESIS_BIN_FILE,
        GenerationManifest, META_FILE, REGISTRY_FILE, REGISTRY_INDEX_FILE,
        REQUIRED_GENERATION_FILES, SIGNATURES_FILE, decode_sha256, hex_lower,
    },
    source::{RangeSource, RangeSourceReader},
    validate_archive_v2_current_metadata_exact, validate_archive_v2_metadata_exact,
};

const DEFAULT_IO_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_BLOCK_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_MAX_COMPRESSED_FRAME_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_PREFETCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_GATEWAY_RANGE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_META_FRAME_BYTES: usize = 64 * 1024 * 1024;
const MAX_IO_CHUNK_SIZE: usize = 64 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_GENESIS_BIN_BYTES: usize = 10_000_000;
const KNOWN_HOT_TX_FLAGS: u32 = (1 << 11) - 1;
const ARCHIVE_V2_HOT_META_GENESIS_TAG: u8 = 1;
const HISTORICAL_EPOCH0_HOT_META_GENESIS_TAG: u8 = 4;

pub const MAX_ORDERED_PARALLEL_DECODE_WORKERS: usize = 64;
pub const MAX_ORDERED_PARALLEL_COMPRESSED_BUFFERS: usize = 16;
pub const MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH: usize = 65_536;
pub const MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES: usize = 1024 * 1024 * 1024;
pub const MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES: usize = 1024 * 1024 * 1024;

static NEXT_READER_ID: AtomicU64 = AtomicU64::new(1);

/// Bounded resources for ordered block I/O with parallel borrowed decoding.
///
/// The reader performs one increasing range read at a time. It overlaps those
/// reads with a fixed local decode pool, but it publishes callback results in
/// exact block-index order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedParallelBlockConfig {
    /// Target compressed bytes in one frame-aligned read. The effective target
    /// is capped by the reader's admitted `prefetch_bytes` option. One frame is
    /// always admitted even when it is larger than this target.
    pub compressed_batch_target_bytes: usize,
    /// Maximum declared uncompressed bytes in one parallel batch. One admitted
    /// block is always allowed when it alone is larger than this budget. The
    /// public hard maximum is 1 GiB.
    pub uncompressed_batch_budget_bytes: usize,
    /// Maximum blocks in one parallel batch. This also bounds the number of
    /// caller-owned projection results retained before ordered consumption.
    pub max_blocks_per_batch: usize,
    /// Exact number of compressed `Vec` tokens shared by the producer and the
    /// coordinator. Three permits one fill, one decode, and one queued batch.
    /// The public hard maximum is 16.
    pub compressed_buffer_count: usize,
    /// Number of threads in the private decode pool. The public hard maximum
    /// is 64.
    pub decode_workers: usize,
    /// Maximum decompression-buffer capacity retained by each worker between
    /// blocks. A larger buffer is dropped after its block callback completes.
    /// The configured per-worker total has a public hard maximum of 1 GiB.
    pub retained_decompressed_bytes_per_worker: usize,
    /// Decode and validate current-schema rewards without retaining them.
    /// Historical schemas keep the existing owned compatibility fallback.
    pub discard_rewards: bool,
}

impl Default for OrderedParallelBlockConfig {
    fn default() -> Self {
        Self {
            compressed_batch_target_bytes: DEFAULT_PREFETCH_BYTES,
            uncompressed_batch_budget_bytes: DEFAULT_MAX_BLOCK_BYTES,
            max_blocks_per_batch: 8_192,
            compressed_buffer_count: 3,
            decode_workers: 4,
            retained_decompressed_bytes_per_worker: 32 * 1024 * 1024,
            discard_rewards: false,
        }
    }
}

/// Coarse measurements from one ordered parallel block pass.
///
/// Durations are accumulated once per batch or range read. The reader does not
/// add timing or synchronization to per-message loops.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OrderedParallelBlockStats {
    pub block_count: u64,
    /// Blocks decoded through the current-schema borrowed byte view.
    pub borrowed_storage_blocks: u64,
    /// Blocks that required the documented owned legacy-schema decoder.
    pub owned_schema_fallback_blocks: u64,
    pub batch_count: u64,
    pub read_call_count: u64,
    pub compressed_bytes: u64,
    pub producer_read_wall_time: Duration,
    pub coordinator_decode_project_wall_time: Duration,
    pub producer_wait_for_free_buffer_time: Duration,
    pub coordinator_wait_for_ready_batch_time: Duration,
    pub max_compressed_batch_bytes: usize,
    pub max_declared_uncompressed_batch_bytes: u64,
}

/// Measurements for one ordered parallel pass with an in-memory batch barrier.
///
/// Every source block is read and decompressed once. The reader first lends the
/// retained decompressed bytes to stage A, merges all stage-A outputs for the
/// batch in row order, and then lends the same bytes to stage B. The explicit
/// counters make that one-read, one-decompression contract observable.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BatchBarrierBlockStats {
    pub block_count: u64,
    /// Blocks decoded through the current-schema borrowed byte view.
    pub borrowed_storage_blocks: u64,
    /// Blocks that required the documented owned legacy-schema decoder.
    pub owned_schema_fallback_blocks: u64,
    pub batch_count: u64,
    /// Calls to the range source for `blocks.bin`. This is one call per batch.
    pub read_call_count: u64,
    /// Compressed source bytes read. Each requested byte is counted once.
    pub compressed_bytes: u64,
    /// Zstd frame decompressions. This equals `block_count` on success.
    pub decompression_count: u64,
    /// Decompressed frame bytes produced. Each frame is counted once.
    pub decompressed_bytes: u64,
    /// Successful stage-A borrowed block visits.
    pub stage_a_block_count: u64,
    /// Successful stage-B borrowed block visits.
    pub stage_b_block_count: u64,
    pub producer_read_wall_time: Duration,
    pub coordinator_stage_a_wall_time: Duration,
    pub coordinator_merge_wall_time: Duration,
    pub coordinator_stage_b_wall_time: Duration,
    pub producer_wait_for_free_buffer_time: Duration,
    pub coordinator_wait_for_ready_batch_time: Duration,
    pub max_compressed_batch_bytes: usize,
    pub max_declared_uncompressed_batch_bytes: u64,
    /// Maximum sum of live decompressed byte lengths for one batch.
    pub max_live_decompressed_batch_bytes: usize,
    /// Maximum retained decompressed capacity after a batch is recycled.
    pub max_retained_decompressed_capacity_bytes: usize,
    /// Frames whose assigned retained `Vec` already had enough capacity.
    pub decompressed_buffer_reuse_count: u64,
    /// Frames that needed a new or larger decompressed allocation.
    pub decompressed_buffer_growth_count: u64,
    /// Block state buffers whose active entries fit in retained capacity.
    pub transaction_state_buffer_reuse_count: u64,
    /// Block state buffers that increased their retained capacity.
    pub transaction_state_buffer_growth_count: u64,
    /// Maximum active transaction-state bytes in one batch.
    pub max_live_transaction_state_bytes: usize,
    /// Maximum aggregate retained transaction-state capacity in bytes.
    pub max_retained_transaction_state_capacity_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashVerification {
    /// Check object presence and exact lengths, then hash every manifest file.
    AllFiles,
    /// Hash the downloaded/cacheable control plane (`registry.bin`, the block
    /// index, publication metadata, genesis, and both selected wire-profile
    /// markers), while size-checking remote blocks and signatures. This is the
    /// intended HTTP streaming policy. The gateway must serve an immutable
    /// generation over authenticated TLS.
    ControlFiles,
    /// Check object presence and exact lengths only. This is useful when the
    /// transport already verified downloaded immutable files; block decoding
    /// and all structural checks remain enabled.
    SizesOnly,
}

#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub hash_verification: HashVerification,
    pub io_chunk_size: usize,
    pub max_block_bytes: usize,
    pub max_compressed_frame_bytes: usize,
    pub max_meta_frame_bytes: usize,
    /// Maximum contiguous blocks range fetched by sequential iterators. The
    /// gateway contract caps a single range at 64 MiB.
    pub prefetch_bytes: usize,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            hash_verification: HashVerification::AllFiles,
            io_chunk_size: DEFAULT_IO_CHUNK_SIZE,
            max_block_bytes: DEFAULT_MAX_BLOCK_BYTES,
            max_compressed_frame_bytes: DEFAULT_MAX_COMPRESSED_FRAME_BYTES,
            max_meta_frame_bytes: DEFAULT_MAX_META_FRAME_BYTES,
            prefetch_bytes: DEFAULT_PREFETCH_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GenerationBinding {
    pub generation_digest: [u8; 32],
    pub registry_sha256: [u8; 32],
    /// The message grammar is part of reader identity. This prevents a
    /// generation-bound filter or projection artifact from being reused
    /// under a different trusted-local profile assertion.
    pub wire_profile: ArchiveV2WireProfile,
}

/// Generation identity plus the metadata grammar selected at admission.
///
/// This additive binding keeps [`GenerationBinding`] source compatible while
/// allowing metadata-sensitive artifacts to bind their decoder authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProfiledGenerationBinding {
    pub generation: GenerationBinding,
    pub metadata_wire_profile: ArchiveV2MetadataWireProfile,
}

#[derive(Debug, Clone)]
pub struct CompiledPubkeyFilter {
    binding: GenerationBinding,
    registry_ids: HashSet<u32>,
    raw_pubkeys: HashSet<[u8; 32]>,
    resolved_ids: HashMap<[u8; 32], u32>,
    reader_id: u64,
}

impl CompiledPubkeyFilter {
    pub fn binding(&self) -> GenerationBinding {
        self.binding
    }

    pub fn reader_id(&self) -> u64 {
        self.reader_id
    }

    pub fn pubkey_count(&self) -> usize {
        self.raw_pubkeys.len()
    }

    pub fn registry_id_count(&self) -> usize {
        self.registry_ids.len()
    }

    /// Return the resolved one-based registry ID for a pubkey that was part
    /// of this compiled filter. Call this once when a hot loop must compare
    /// many compact references with the same pubkey.
    pub fn registry_id_for(&self, pubkey: &[u8; 32]) -> Option<u32> {
        self.resolved_ids.get(pubkey).copied()
    }

    /// Test whether one compact reference resolves to this exact pubkey.
    pub fn matches_reference(&self, reference: CompactPubkey, pubkey: &[u8; 32]) -> bool {
        match reference {
            CompactPubkey::Raw(raw) => &raw == pubkey,
            CompactPubkey::Id(id) => self.resolved_ids.get(pubkey) == Some(&id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndeterminateReason {
    RawTransactionFallback,
    InvalidRegistryReference,
    V0LoadedAddressesUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMatch {
    Match {
        static_account: bool,
        loaded_address: bool,
    },
    NoMatch,
    Indeterminate(IndeterminateReason),
}

#[derive(Debug)]
pub enum MetadataState {
    NotRead,
    Absent,
    RawFallback,
    Decoded(Box<CompactMetaV1>),
}

impl MetadataState {
    pub fn decoded(&self) -> Option<&CompactMetaV1> {
        match self {
            Self::Decoded(metadata) => Some(metadata),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignatureReference {
    pub generation_digest: [u8; 32],
    pub reader_id: u64,
    pub first_ordinal: u64,
    pub count: u8,
}

#[derive(Debug)]
pub struct ScannedTransaction {
    pub slot: u64,
    pub tx_index: u32,
    pub row: ArchiveV2HotTxRow,
    pub outcome: TransactionMatch,
    pub message: Option<ArchiveV2HotMessagePayload>,
    pub metadata: MetadataState,
    pub signatures: SignatureReference,
}

#[derive(Debug)]
pub struct ScannedBlock {
    pub block_id: u32,
    pub slot: u64,
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    /// Transactions in canonical source order (`tx_index` order).
    ///
    /// Signature references remain bound to the transaction row's storage
    /// position even when that position differs from `tx_index`.
    pub transactions: Vec<ScannedTransaction>,
}

#[derive(Debug)]
pub struct DecodedBlock {
    pub index_row: ArchiveV2HotBlockIndexRow,
    pub block: ArchiveV2HotBlockBlob,
}

impl DecodedBlock {
    /// Describe both transaction orders without changing the stored rows.
    ///
    /// Archive byte ranges and signature ordinals follow storage order. The
    /// source's canonical transaction order follows `tx_index` and can differ.
    /// This validates the public, mutable owned block again before exposing a
    /// trusted mapping. Borrowed blocks cannot be constructed or changed by a
    /// caller, so their matching method does not need a second validation.
    pub fn transaction_row_order(&self) -> Result<TransactionRowOrder> {
        validate_decoded_block(&self.index_row, &self.block)?;
        Ok(TransactionRowOrder::from_validated_rows(
            self.block.tx_rows.iter().copied(),
        ))
    }
}

/// One validated transaction row located in both block-local orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocatedTransactionRow {
    /// Position of this row in the hot block's row, message and metadata
    /// regions. Transaction signatures also follow this order.
    pub storage_position: u32,
    /// Signature offset from the block index row's
    /// `first_signature_ordinal`, calculated in storage order.
    pub first_signature_offset: u32,
    pub row: ArchiveV2HotTxRow,
}

/// Mapping from canonical transaction index to its storage-bound row.
///
/// A valid hot block contains every `tx_index` in `0..tx_count` exactly once.
/// The mapping keeps storage position and signature offset attached while it
/// exposes rows in canonical `tx_index` order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionRowOrder {
    canonical_rows: Vec<LocatedTransactionRow>,
    storage_order_is_canonical: bool,
}

impl TransactionRowOrder {
    fn from_validated_rows(rows: impl ExactSizeIterator<Item = ArchiveV2HotTxRow>) -> Self {
        let mut signature_offset = 0u32;
        let mut canonical_rows = Vec::with_capacity(rows.len());
        let mut storage_order_is_canonical = true;
        for (storage_position, row) in rows.enumerate() {
            let storage_position =
                u32::try_from(storage_position).expect("validated transaction row count fits u32");
            storage_order_is_canonical &= row.tx_index == storage_position;
            canonical_rows.push(LocatedTransactionRow {
                storage_position,
                first_signature_offset: signature_offset,
                row,
            });
            signature_offset = signature_offset
                .checked_add(u32::from(row.signature_count))
                .expect("validated block signature count fits u32");
        }
        canonical_rows.sort_unstable_by_key(|location| location.row.tx_index);
        Self {
            canonical_rows,
            storage_order_is_canonical,
        }
    }

    pub fn len(&self) -> usize {
        self.canonical_rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.canonical_rows.is_empty()
    }

    pub fn storage_order_is_canonical(&self) -> bool {
        self.storage_order_is_canonical
    }

    /// Rows in canonical transaction order. Each item retains its storage
    /// position and its storage-bound signature offset.
    pub fn canonical_rows(&self) -> &[LocatedTransactionRow] {
        &self.canonical_rows
    }

    pub fn row_for_tx_index(&self, tx_index: u32) -> Option<&LocatedTransactionRow> {
        self.canonical_rows.get(tx_index as usize)
    }
}

/// One decoded block lent by [`BorrowedBlockStream`].
///
/// Current-schema blocks borrow transaction rows, messages and metadata from the stream's reused
/// decompression buffer. Historical schemas use the existing owned decoder as a compatibility
/// fallback. Holding this value prevents calling `next_block` again until it is dropped.
#[derive(Debug)]
pub struct BorrowedDecodedBlock<'a> {
    pub index_row: ArchiveV2HotBlockIndexRow,
    block: BorrowedDecodedBlockPayload<'a>,
    uncompressed_bytes: &'a [u8],
}

#[derive(Debug)]
enum BorrowedDecodedBlockPayload<'a> {
    Current(BorrowedArchiveV2HotBlockBlob<'a>),
    CurrentWithoutRewards(BorrowedArchiveV2HotBlockBlobWithoutRewards<'a>),
    OwnedFallback(ArchiveV2HotBlockBlob),
}

impl BorrowedDecodedBlock<'_> {
    /// Exact decompressed frame bytes that back this borrowed block.
    ///
    /// This lets a length-preserving migration copy the current-schema outer
    /// encoding verbatim and replace only a validated borrowed byte range.
    #[inline]
    pub fn uncompressed_bytes(&self) -> &[u8] {
        self.uncompressed_bytes
    }

    #[inline]
    pub fn header(&self) -> &ArchiveV2HotBlockHeader {
        match &self.block {
            BorrowedDecodedBlockPayload::Current(block) => &block.header,
            BorrowedDecodedBlockPayload::CurrentWithoutRewards(block) => &block.header,
            BorrowedDecodedBlockPayload::OwnedFallback(block) => &block.header,
        }
    }

    #[inline]
    pub fn tx_count(&self) -> u32 {
        match &self.block {
            BorrowedDecodedBlockPayload::Current(block) => block.tx_count,
            BorrowedDecodedBlockPayload::CurrentWithoutRewards(block) => block.tx_count,
            BorrowedDecodedBlockPayload::OwnedFallback(block) => block.tx_count,
        }
    }

    #[inline]
    pub fn tx_rows_len(&self) -> usize {
        match &self.block {
            BorrowedDecodedBlockPayload::Current(block) => block.tx_rows_len(),
            BorrowedDecodedBlockPayload::CurrentWithoutRewards(block) => block.tx_rows_len(),
            BorrowedDecodedBlockPayload::OwnedFallback(block) => block.tx_rows.len(),
        }
    }

    #[inline]
    pub fn tx_rows(&self) -> BorrowedDecodedTxRowIter<'_> {
        match &self.block {
            BorrowedDecodedBlockPayload::Current(block) => {
                BorrowedDecodedTxRowIter::Current(block.tx_rows())
            }
            BorrowedDecodedBlockPayload::CurrentWithoutRewards(block) => {
                BorrowedDecodedTxRowIter::Current(block.tx_rows())
            }
            BorrowedDecodedBlockPayload::OwnedFallback(block) => {
                BorrowedDecodedTxRowIter::OwnedFallback(block.tx_rows.iter().copied())
            }
        }
    }

    #[inline]
    pub fn message_bytes(&self) -> &[u8] {
        match &self.block {
            BorrowedDecodedBlockPayload::Current(block) => block.message_bytes,
            BorrowedDecodedBlockPayload::CurrentWithoutRewards(block) => block.message_bytes,
            BorrowedDecodedBlockPayload::OwnedFallback(block) => &block.message_bytes,
        }
    }

    #[inline]
    pub fn metadata_bytes(&self) -> &[u8] {
        match &self.block {
            BorrowedDecodedBlockPayload::Current(block) => block.metadata_bytes,
            BorrowedDecodedBlockPayload::CurrentWithoutRewards(block) => block.metadata_bytes,
            BorrowedDecodedBlockPayload::OwnedFallback(block) => &block.metadata_bytes,
        }
    }

    /// Whether this block required the allocation-preserving historical-schema decoder.
    #[inline]
    pub fn uses_owned_fallback(&self) -> bool {
        matches!(&self.block, BorrowedDecodedBlockPayload::OwnedFallback(_))
    }

    /// Describe both transaction orders without changing the stored rows.
    ///
    /// Archive byte ranges and signature ordinals follow storage order. The
    /// source's canonical transaction order follows `tx_index` and can differ.
    pub fn transaction_row_order(&self) -> TransactionRowOrder {
        TransactionRowOrder::from_validated_rows(self.tx_rows())
    }

    /// Stream transaction rows in their exact source storage order.
    ///
    /// This path does not allocate or sort. Each item keeps the source row's
    /// storage position and cumulative signature offset. The block decoder has
    /// already validated every row, the exact `tx_index` permutation, all byte
    /// regions, and the total signature count before this iterator is exposed.
    #[inline]
    pub fn storage_transaction_rows(&self) -> LocatedStorageTransactionRowIter<'_> {
        LocatedStorageTransactionRowIter {
            rows: self.tx_rows(),
            storage_position: 0,
            first_signature_offset: 0,
        }
    }
}

/// Zero-allocation iterator over located transaction rows in source storage order.
#[derive(Debug, Clone)]
pub struct LocatedStorageTransactionRowIter<'a> {
    rows: BorrowedDecodedTxRowIter<'a>,
    storage_position: u32,
    first_signature_offset: u32,
}

impl Iterator for LocatedStorageTransactionRowIter<'_> {
    type Item = LocatedTransactionRow;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let row = self.rows.next()?;
        let located = LocatedTransactionRow {
            storage_position: self.storage_position,
            first_signature_offset: self.first_signature_offset,
            row,
        };
        self.storage_position = self
            .storage_position
            .checked_add(1)
            .expect("validated transaction row count fits u32");
        self.first_signature_offset = self
            .first_signature_offset
            .checked_add(u32::from(row.signature_count))
            .expect("validated block signature count fits u32");
        Some(located)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl ExactSizeIterator for LocatedStorageTransactionRowIter<'_> {}

impl std::iter::FusedIterator for LocatedStorageTransactionRowIter<'_> {}

/// Exact transaction-row iterator for either a current borrowed block or a historical fallback.
#[derive(Debug, Clone)]
pub enum BorrowedDecodedTxRowIter<'a> {
    Current(ArchiveV2HotTxRowIter<'a>),
    OwnedFallback(std::iter::Copied<std::slice::Iter<'a, ArchiveV2HotTxRow>>),
}

impl Iterator for BorrowedDecodedTxRowIter<'_> {
    type Item = ArchiveV2HotTxRow;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Current(rows) => rows.next(),
            Self::OwnedFallback(rows) => rows.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Current(rows) => rows.size_hint(),
            Self::OwnedFallback(rows) => rows.size_hint(),
        }
    }
}

impl DoubleEndedIterator for BorrowedDecodedTxRowIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Current(rows) => rows.next_back(),
            Self::OwnedFallback(rows) => rows.next_back(),
        }
    }
}

impl ExactSizeIterator for BorrowedDecodedTxRowIter<'_> {}

impl std::iter::FusedIterator for BorrowedDecodedTxRowIter<'_> {}

/// Structural publication audit shared by the gateway publisher and readers.
///
/// The helper validates the candidate manifest, required object sizes/hashes,
/// registry layout, hot index, optional signature length, and the metadata
/// footer. It does not require the manifest JSON itself to have been published.
/// Metadata-marker admission here verifies authority bindings and selects a
/// decoder. A producer must run the complete exact metadata audit before it
/// publishes that marker.
#[derive(Debug)]
pub struct ValidatedGeneration {
    pub index: ArchiveV2HotBlockIndex,
    pub genesis: Option<WincodeArchiveV2Genesis>,
    pub genesis_bin: Option<Vec<u8>>,
    pub metadata_footer: WincodeArchiveV2Footer,
    pub binding: GenerationBinding,
    pub registry_entries: u32,
    pub total_signatures: u64,
    pub signatures_available: bool,
    pub wire_profile: ArchiveV2WireProfile,
}

#[derive(Debug)]
pub struct ArchiveReader<S> {
    source: S,
    manifest: GenerationManifest,
    index: ArchiveV2HotBlockIndex,
    genesis: Option<WincodeArchiveV2Genesis>,
    genesis_bin: Option<Vec<u8>>,
    metadata_footer: WincodeArchiveV2Footer,
    binding: GenerationBinding,
    registry_entries: u32,
    total_signatures: u64,
    signatures_available: bool,
    wire_profile: ArchiveV2WireProfile,
    metadata_wire_profile: ArchiveV2MetadataWireProfile,
    options: OpenOptions,
    reader_id: u64,
}

impl<S: RangeSource> ArchiveReader<S> {
    /// Open a published generation with strict current metadata admission.
    /// An unmarked historical generation needs an explicit additive
    /// compatibility API.
    pub fn open(source: S) -> Result<Self> {
        Self::open_with_options(source, OpenOptions::default())
    }

    pub fn open_with_options(source: S, options: OpenOptions) -> Result<Self> {
        Self::open_with_options_and_metadata_admission(
            source,
            options,
            ArchiveV2MetadataProfileAdmission::RequireCurrentTypedErrors,
        )
    }

    /// Open an archive with an explicit metadata authority policy and default
    /// reader limits. Use this only for an intentional historical cutover.
    pub fn open_with_metadata_admission(
        source: S,
        metadata_admission: ArchiveV2MetadataProfileAdmission,
    ) -> Result<Self> {
        Self::open_with_options_and_metadata_admission(
            source,
            OpenOptions::default(),
            metadata_admission,
        )
    }

    /// Open an archive with explicit reader limits and metadata authority.
    pub fn open_with_options_and_metadata_admission(
        source: S,
        options: OpenOptions,
        metadata_admission: ArchiveV2MetadataProfileAdmission,
    ) -> Result<Self> {
        let manifest_bytes =
            source.read_all_bounded(GENERATION_MANIFEST_FILE, MAX_MANIFEST_BYTES)?;
        let manifest = GenerationManifest::parse(&manifest_bytes)?;
        Self::open_candidate_with_metadata_admission(source, manifest, options, metadata_admission)
    }

    /// Structurally validate an unpublished current-profile candidate.
    ///
    /// This authenticates the fixed marker binding and selects the strict
    /// current-only decoder. A producer must also call the complete semantic
    /// metadata audit before it publishes the marker or manifest.
    pub fn open_candidate(
        source: S,
        manifest: GenerationManifest,
        options: OpenOptions,
    ) -> Result<Self> {
        Self::open_candidate_with_metadata_admission(
            source,
            manifest,
            options,
            ArchiveV2MetadataProfileAdmission::RequireCurrentTypedErrors,
        )
    }

    /// Structurally validate a candidate with an explicit metadata authority
    /// policy. This is the compatibility path for old unmarked generations.
    pub fn open_candidate_with_metadata_admission(
        source: S,
        manifest: GenerationManifest,
        options: OpenOptions,
        metadata_admission: ArchiveV2MetadataProfileAdmission,
    ) -> Result<Self> {
        let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);
        let wire_profile = ArchiveV2WireProfile::for_published_manifest(&manifest)?;
        let metadata_wire_profile =
            ArchiveV2MetadataWireProfile::for_manifest(&manifest, metadata_admission)?;
        let validated = validate_generation_structure_with_profiles(
            &source,
            &manifest,
            &options,
            wire_profile,
            metadata_wire_profile,
        )?;

        Ok(Self {
            source,
            manifest,
            index: validated.index,
            genesis: validated.genesis,
            genesis_bin: validated.genesis_bin,
            metadata_footer: validated.metadata_footer,
            binding: validated.binding,
            registry_entries: validated.registry_entries,
            total_signatures: validated.total_signatures,
            signatures_available: validated.signatures_available,
            wire_profile,
            metadata_wire_profile,
            options,
            reader_id,
        })
    }

    /// Open a generation without a published `archive-v2-generation.json` and
    /// without hashing any file content, for a source the caller already
    /// trusts (e.g. a local NAS directory).
    ///
    /// This skips the manifest's cross-service integrity contract entirely:
    /// `identity` is taken as given, not verified against the archive's own
    /// content. Every declared file gets a synthetic name/size/profile hash,
    /// not a content hash. Structural validation (index/metadata bounds,
    /// registry shape, footer totals) still
    /// runs in full via [`validate_generation_structure`] — only the
    /// published-manifest and content-hash steps are skipped. Requires
    /// `options.hash_verification == HashVerification::SizesOnly`, since a
    /// synthetic hash would otherwise fail any real comparison.
    pub fn open_trusted(
        source: S,
        identity: crate::manifest::TrustedGenerationIdentity,
        options: OpenOptions,
    ) -> Result<Self> {
        Self::open_trusted_with_metadata_profile(
            source,
            identity,
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
            options,
        )
    }

    /// Open trusted local data with an explicit metadata decoder authority.
    pub fn open_trusted_with_metadata_profile(
        source: S,
        identity: crate::manifest::TrustedGenerationIdentity,
        metadata_wire_profile: ArchiveV2MetadataWireProfile,
        options: OpenOptions,
    ) -> Result<Self> {
        Self::open_trusted_with_additional_files_and_metadata_profile(
            source,
            identity,
            &[],
            &[],
            metadata_wire_profile,
            options,
        )
    }

    /// Open a trusted-local generation and add size bindings for sidecars used
    /// by a higher-level reader.
    ///
    /// The four core generation objects are always required. Signatures and
    /// epoch-0 genesis are admitted when present, as in [`Self::open_trusted`].
    /// Names in `required_additional_files` must exist. Names in
    /// `optional_additional_files` are admitted only when present. Every
    /// admitted name and size, plus both asserted wire profiles, contributes
    /// to the synthetic generation digest. File contents are not authenticated.
    /// This method requires `HashVerification::SizesOnly` and does not change
    /// the published-manifest admission path.
    pub fn open_trusted_with_additional_files(
        source: S,
        identity: crate::manifest::TrustedGenerationIdentity,
        required_additional_files: &[&str],
        optional_additional_files: &[&str],
        options: OpenOptions,
    ) -> Result<Self> {
        Self::open_trusted_with_additional_files_and_metadata_profile(
            source,
            identity,
            required_additional_files,
            optional_additional_files,
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
            options,
        )
    }

    /// Open trusted local data with extra files and an explicit metadata
    /// decoder authority. The profile is bound into the synthetic identity;
    /// it is never represented by a synthetic fixed-marker hash.
    pub fn open_trusted_with_additional_files_and_metadata_profile(
        source: S,
        identity: crate::manifest::TrustedGenerationIdentity,
        required_additional_files: &[&str],
        optional_additional_files: &[&str],
        metadata_wire_profile: ArchiveV2MetadataWireProfile,
        options: OpenOptions,
    ) -> Result<Self> {
        let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);
        if options.hash_verification != HashVerification::SizesOnly {
            return Err(Error::InvalidManifest(
                "trusted-local open requires HashVerification::SizesOnly".into(),
            ));
        }

        let mut required = REQUIRED_GENERATION_FILES
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut optional = BTreeSet::from([SIGNATURES_FILE]);
        if identity.epoch == 0 {
            optional.insert(GENESIS_BIN_FILE);
        }
        for &name in required_additional_files {
            crate::manifest::validate_object_name(name)
                .map_err(|message| Error::InvalidManifest(message.to_owned()))?;
            required.insert(name);
        }
        for &name in optional_additional_files {
            crate::manifest::validate_object_name(name)
                .map_err(|message| Error::InvalidManifest(message.to_owned()))?;
            optional.insert(name);
        }
        if required.iter().chain(&optional).any(|name| {
            crate::metadata_wire_profile::is_metadata_schema_marker_name(name)
                || *name == PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE
                || *name == POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE
        }) {
            return Err(Error::InvalidManifest(format!(
                "trusted-local profile authority is explicit; profile marker files such as {CURRENT_TYPED_ERRORS_MARKER_FILE} cannot be admitted through a synthetic file binding"
            )));
        }
        for name in &required {
            optional.remove(name);
        }

        let mut files = Vec::with_capacity(required.len() + optional.len());
        for name in required {
            let size = source
                .size(name)?
                .ok_or_else(|| Error::MissingFile(name.to_owned()))?;
            files.push((name.to_owned(), size));
        }
        for name in optional {
            if let Some(size) = source.size(name)? {
                files.push((name.to_owned(), size));
            }
        }

        let wire_profile = identity.wire_profile;
        let manifest =
            crate::manifest::synthesize_trusted_manifest(identity, metadata_wire_profile, files)?;
        let validated = validate_generation_structure_with_profiles(
            &source,
            &manifest,
            &options,
            wire_profile,
            metadata_wire_profile,
        )?;

        Ok(Self {
            source,
            manifest,
            index: validated.index,
            genesis: validated.genesis,
            genesis_bin: validated.genesis_bin,
            metadata_footer: validated.metadata_footer,
            binding: validated.binding,
            registry_entries: validated.registry_entries,
            total_signatures: validated.total_signatures,
            signatures_available: validated.signatures_available,
            wire_profile,
            metadata_wire_profile,
            options,
            reader_id,
        })
    }

    pub fn source(&self) -> &S {
        &self.source
    }

    pub fn manifest(&self) -> &GenerationManifest {
        &self.manifest
    }

    pub fn reader_id(&self) -> u64 {
        self.reader_id
    }

    pub fn index(&self) -> &ArchiveV2HotBlockIndex {
        &self.index
    }

    pub fn metadata_footer(&self) -> &WincodeArchiveV2Footer {
        &self.metadata_footer
    }

    /// Return the epoch-0 genesis payload embedded in compact metadata.
    ///
    /// Completed generations for epochs after zero do not carry this record.
    pub fn genesis(&self) -> Option<&WincodeArchiveV2Genesis> {
        self.genesis.as_ref()
    }

    /// Return exact, digest-bound `genesis.bin` bytes when the epoch-0
    /// generation publishes the backwards-compatible runtime sidecar.
    pub fn genesis_bin(&self) -> Option<&[u8]> {
        self.genesis_bin.as_deref()
    }

    pub fn binding(&self) -> GenerationBinding {
        self.binding
    }

    /// Return generation identity with the admitted metadata decoder profile.
    pub fn profiled_binding(&self) -> ProfiledGenerationBinding {
        ProfiledGenerationBinding {
            generation: self.binding,
            metadata_wire_profile: self.metadata_wire_profile,
        }
    }

    pub fn registry_entries(&self) -> u32 {
        self.registry_entries
    }

    pub fn total_signatures(&self) -> u64 {
        self.total_signatures
    }

    pub fn signatures_available(&self) -> bool {
        self.signatures_available
    }

    /// The immutable hot-message grammar selected when this generation was
    /// admitted. Published readers derive it from manifest bindings;
    /// trusted-local readers require an explicit caller assertion.
    pub fn wire_profile(&self) -> ArchiveV2WireProfile {
        self.wire_profile
    }

    /// The immutable metadata error grammar selected for this generation.
    pub fn metadata_wire_profile(&self) -> ArchiveV2MetadataWireProfile {
        self.metadata_wire_profile
    }

    /// Validate metadata with the grammar fixed at generation admission.
    /// Current marked generations never run the historical decoder.
    pub fn validate_metadata_exact(
        &self,
        bytes: &[u8],
        limits: ArchiveV2MetadataProjectionLimits,
    ) -> wincode::ReadResult<ProjectedArchiveV2MetadataPrefix> {
        match self.metadata_wire_profile {
            ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1 => {
                validate_archive_v2_current_metadata_exact(bytes, limits, self.registry_entries)
            }
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility => {
                validate_archive_v2_metadata_exact(bytes, limits, self.registry_entries)
            }
        }
    }

    /// Return a cheap copyable projector bound to this generation's selected
    /// wire profile.
    pub fn message_projector(&self) -> ArchiveV2MessageProjector {
        ArchiveV2MessageProjector::new(self.wire_profile)
    }

    /// Compile an include-any pubkey filter by scanning `registry.bin` once.
    /// Memory is O(number of requested pubkeys), not O(registry size). Queried
    /// bytes are retained as well as resolved IDs so inline raw pubkeys match.
    pub fn compile_pubkey_filter(
        &self,
        pubkeys: impl IntoIterator<Item = [u8; 32]>,
    ) -> Result<CompiledPubkeyFilter> {
        let raw_pubkeys: HashSet<[u8; 32]> = pubkeys.into_iter().collect();
        let mut registry_ids = HashSet::with_capacity(raw_pubkeys.len());
        let mut resolved_pubkeys = HashSet::with_capacity(raw_pubkeys.len());
        let mut resolved_ids = HashMap::with_capacity(raw_pubkeys.len());
        if !raw_pubkeys.is_empty() && self.registry_entries != 0 {
            let mut offset = 0u64;
            let mut bytes = Vec::new();
            let registry_size = self.manifest.required_file(REGISTRY_FILE)?.size;
            let chunk_size = (self.options.io_chunk_size / 32).max(1) * 32;
            while offset < registry_size {
                let length = usize::try_from((registry_size - offset).min(chunk_size as u64))
                    .expect("registry chunk is bounded by usize");
                self.source
                    .read_range_into(REGISTRY_FILE, offset, length, &mut bytes)?;
                if bytes.len() % 32 != 0 {
                    return Err(Error::InvalidRegistry(
                        "range source split registry on a partial pubkey".into(),
                    ));
                }
                for (position, key_bytes) in bytes.chunks_exact(32).enumerate() {
                    let mut key = [0u8; 32];
                    key.copy_from_slice(key_bytes);
                    if raw_pubkeys.contains(&key) {
                        if !resolved_pubkeys.insert(key) {
                            return Err(Error::InvalidRegistry(
                                "a requested pubkey occurs more than once in registry.bin".into(),
                            ));
                        }
                        let zero_based = offset / 32 + position as u64;
                        let id = u32::try_from(zero_based + 1)
                            .map_err(|_| Error::InvalidRegistry("registry id overflow".into()))?;
                        registry_ids.insert(id);
                        resolved_ids.insert(key, id);
                    }
                }
                offset += length as u64;
            }
        }
        Ok(CompiledPubkeyFilter {
            binding: self.binding,
            registry_ids,
            raw_pubkeys,
            resolved_ids,
            reader_id: self.reader_id,
        })
    }

    pub fn blocks(&self) -> BlockIterator<'_, S> {
        BlockIterator {
            archive: self,
            next: 0,
            end: self.index.rows.len(),
            batch_first: 0,
            batch_end: 0,
            batch_offset: 0,
            batch: Vec::new(),
            decompressor: None,
            decompressed: Vec::new(),
        }
    }

    /// Iterate a bounded index-row range in archive order while coalescing
    /// adjacent compressed frames into the configured prefetch buffer.
    ///
    /// Unlike calling [`Iterator::skip`] on [`Self::blocks`], rows before
    /// `range.start` are neither fetched nor decoded.
    pub fn blocks_range(&self, range: Range<usize>) -> Result<BlockIterator<'_, S>> {
        let row_count = self.index.rows.len();
        if range.start > range.end || range.end > row_count {
            return Err(Error::InvalidIndex(format!(
                "block row range {}..{} is outside 0..{row_count}",
                range.start, range.end,
            )));
        }
        Ok(BlockIterator {
            archive: self,
            next: range.start,
            end: range.end,
            batch_first: range.start,
            batch_end: range.start,
            batch_offset: 0,
            batch: Vec::new(),
            decompressor: None,
            decompressed: Vec::new(),
        })
    }

    /// Lend every block in archive order while reusing the prefetch and decompression buffers.
    ///
    /// Unlike [`Self::blocks`], this stream deliberately does not implement [`Iterator`]: the
    /// returned block borrows from the stream and must be consumed before `next_block` is called
    /// again.
    pub fn borrowed_blocks(&self) -> BorrowedBlockStream<'_, S> {
        BorrowedBlockStream {
            archive: self,
            discard_rewards: false,
            next: 0,
            end: self.index.rows.len(),
            batch_first: 0,
            batch_end: 0,
            batch_offset: 0,
            batch: Vec::new(),
            decompressor: None,
            decompressed: Vec::new(),
        }
    }

    /// Lend a bounded index-row range while coalescing adjacent compressed-frame reads.
    pub fn borrowed_blocks_range(&self, range: Range<usize>) -> Result<BorrowedBlockStream<'_, S>> {
        let row_count = self.index.rows.len();
        if range.start > range.end || range.end > row_count {
            return Err(Error::InvalidIndex(format!(
                "block row range {}..{} is outside 0..{row_count}",
                range.start, range.end,
            )));
        }
        Ok(BorrowedBlockStream {
            archive: self,
            discard_rewards: false,
            next: range.start,
            end: range.end,
            batch_first: range.start,
            batch_end: range.start,
            batch_offset: 0,
            batch: Vec::new(),
            decompressor: None,
            decompressed: Vec::new(),
        })
    }

    /// Lend a bounded row range while decoding and validating, but not retaining, current-schema
    /// block rewards.
    ///
    /// This is an execution-projection API for replay paths that intentionally do not apply
    /// rewards. Existing borrowed block methods retain their full-reward behavior. Historical hot
    /// schemas continue through the owned compatibility decoder.
    pub fn borrowed_blocks_without_rewards_range(
        &self,
        range: Range<usize>,
    ) -> Result<BorrowedBlockStream<'_, S>> {
        let row_count = self.index.rows.len();
        if range.start > range.end || range.end > row_count {
            return Err(Error::InvalidIndex(format!(
                "block row range {}..{} is outside 0..{row_count}",
                range.start, range.end,
            )));
        }
        Ok(BorrowedBlockStream {
            archive: self,
            discard_rewards: true,
            next: range.start,
            end: range.end,
            batch_first: range.start,
            batch_end: range.start,
            batch_offset: 0,
            batch: Vec::new(),
            decompressor: None,
            decompressed: Vec::new(),
        })
    }

    /// Read blocks in one monotonic I/O stream, project them in a private
    /// parallel decode pool, and publish the projection results in exact index
    /// order.
    ///
    /// `make_worker_state` creates one caller state for each decode worker.
    /// `project` runs in parallel and can use only its worker state plus the
    /// block that borrows from that worker's reusable decompression buffer.
    /// The block cannot escape the callback. `Output` should be a small owned
    /// summary because all outputs for one batch remain live until that batch
    /// is ordered. `consume_ordered` runs on the coordinator thread, once per
    /// successful projection and in increasing block-row order.
    ///
    /// A projection error is selected by row order, not worker completion
    /// order. The method stops before it publishes a later result. The current
    /// lending APIs remain independent and unchanged.
    pub fn process_borrowed_blocks_parallel_ordered<
        WorkerState,
        Output,
        MakeWorkerState,
        Project,
        Consume,
    >(
        &self,
        range: Range<usize>,
        config: OrderedParallelBlockConfig,
        mut make_worker_state: MakeWorkerState,
        project: Project,
        mut consume_ordered: Consume,
    ) -> Result<OrderedParallelBlockStats>
    where
        WorkerState: Send,
        Output: Send,
        MakeWorkerState: FnMut(usize) -> WorkerState,
        Project: for<'block> Fn(&mut WorkerState, usize, BorrowedDecodedBlock<'block>) -> Result<Output>
            + Send
            + Sync,
        Consume: FnMut(usize, Output) -> Result<()>,
    {
        let row_count = self.index.rows.len();
        if range.start > range.end || range.end > row_count {
            return Err(Error::InvalidIndex(format!(
                "block row range {}..{} is outside 0..{row_count}",
                range.start, range.end,
            )));
        }
        validate_ordered_parallel_config(config)?;
        if range.is_empty() {
            return Ok(OrderedParallelBlockStats::default());
        }

        let compressed_target = config
            .compressed_batch_target_bytes
            .min(self.options.prefetch_bytes);
        let plans = ordered_parallel_batch_plans(
            &self.index.rows,
            range,
            compressed_target,
            config.uncompressed_batch_budget_bytes,
            config.max_blocks_per_batch,
        )?;
        let decode_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.decode_workers)
            .thread_name(|index| format!("blockzilla-block-decode-{index}"))
            .build()
            .map_err(|error| {
                Error::InvalidManifest(format!(
                    "cannot create ordered parallel block decode pool: {error}"
                ))
            })?;
        let workers: Vec<_> = (0..config.decode_workers)
            .map(|worker| {
                Mutex::new(Some(OrderedParallelWorker {
                    decompressor: None,
                    decompressed: Vec::new(),
                    caller: make_worker_state(worker),
                }))
            })
            .collect();

        let (free_sender, free_receiver) = sync_channel(config.compressed_buffer_count);
        for _ in 0..config.compressed_buffer_count {
            free_sender
                .send(Vec::new())
                .expect("the new recycled-buffer channel has a receiver");
        }
        let (ready_sender, ready_receiver) = sync_channel(config.compressed_buffer_count);

        thread::scope(|scope| {
            let producer = scope.spawn(|| {
                produce_ordered_compressed_batches(
                    self,
                    &plans,
                    compressed_target,
                    free_receiver,
                    ready_sender,
                )
            });
            let mut coordinator = OrderedParallelCoordinator::default();
            let mut projected = Vec::new();

            'batches: for expected in &plans {
                let wait_started = Instant::now();
                let ready = match ready_receiver.recv() {
                    Ok(ready) => ready,
                    Err(_) => {
                        coordinator.producer_disconnected = true;
                        break;
                    }
                };
                coordinator.stats.coordinator_wait_for_ready_batch_time = coordinator
                    .stats
                    .coordinator_wait_for_ready_batch_time
                    .saturating_add(wait_started.elapsed());
                if ready.plan != *expected {
                    coordinator.error = Some(Error::InvalidIndex(format!(
                        "ordered block producer returned rows {}..{}, expected {}..{}",
                        ready.plan.row_start,
                        ready.plan.row_end,
                        expected.row_start,
                        expected.row_end,
                    )));
                    break;
                }

                let decode_started = Instant::now();
                decode_pool.install(|| {
                    self.index.rows[ready.plan.row_start..ready.plan.row_end]
                        .par_iter()
                        .enumerate()
                        .map(|(batch_row, row)| {
                            let row_number = ready.plan.row_start + batch_row;
                            let relative_offset = row
                                .compressed_offset
                                .checked_sub(ready.plan.compressed_offset)
                                .ok_or_else(|| {
                                    Error::InvalidIndex(
                                        "parallel block frame offset underflow".into(),
                                    )
                                })?;
                            let relative_offset = usize::try_from(relative_offset)
                                .map_err(|_| Error::Overflow("parallel block frame offset"))?;
                            let frame_end = relative_offset
                                .checked_add(row.compressed_len as usize)
                                .ok_or(Error::Overflow("parallel block frame range"))?;
                            let compressed =
                                ready.bytes.get(relative_offset..frame_end).ok_or_else(|| {
                                    Error::InvalidIndex(
                                        "parallel block frame is outside its read batch".into(),
                                    )
                                })?;
                            let worker_number = rayon::current_thread_index().ok_or_else(|| {
                                Error::InvalidIndex(
                                    "parallel block task ran outside its decode pool".into(),
                                )
                            })?;
                            let mut worker_guard = workers[worker_number].lock().map_err(|_| {
                                Error::InvalidIndex(
                                    "parallel block worker state is poisoned".into(),
                                )
                            })?;
                            let mut worker = worker_guard.take().ok_or_else(|| {
                                Error::InvalidIndex(
                                    "parallel block worker was re-entered by nested work".into(),
                                )
                            })?;
                            drop(worker_guard);
                            let result = worker.decode_and_project(
                                self,
                                *row,
                                compressed,
                                row_number,
                                config.discard_rewards,
                                config.retained_decompressed_bytes_per_worker,
                                &project,
                            );
                            let mut worker_guard = workers[worker_number].lock().map_err(|_| {
                                Error::InvalidIndex(
                                    "parallel block worker state is poisoned".into(),
                                )
                            })?;
                            *worker_guard = Some(worker);
                            result
                        })
                        .collect_into_vec(&mut projected)
                });
                coordinator.stats.coordinator_decode_project_wall_time = coordinator
                    .stats
                    .coordinator_decode_project_wall_time
                    .saturating_add(decode_started.elapsed());

                // No decoded value borrows from the compressed batch. Clear
                // and return the normal bounded allocation before ordered
                // result handling. An oversized one-frame batch used a
                // temporary Vec and kept this normal allocation aside.
                let _ = free_sender.send(recycle_ordered_compressed_buffer(ready));

                coordinator.stats.batch_count = match coordinator.stats.batch_count.checked_add(1) {
                    Some(value) => value,
                    None => {
                        coordinator.error = Some(Error::Overflow("parallel batch count"));
                        break;
                    }
                };
                let batch_blocks = match u64::try_from(expected.row_end - expected.row_start) {
                    Ok(value) => value,
                    Err(_) => {
                        coordinator.error = Some(Error::Overflow("parallel block count"));
                        break;
                    }
                };
                coordinator.stats.block_count =
                    match coordinator.stats.block_count.checked_add(batch_blocks) {
                        Some(value) => value,
                        None => {
                            coordinator.error = Some(Error::Overflow("parallel block count"));
                            break;
                        }
                    };

                for (batch_row, result) in projected.drain(..).enumerate() {
                    let row_number = expected.row_start + batch_row;
                    let projected = match result {
                        Ok(output) => output,
                        Err(error) => {
                            coordinator.error = Some(error);
                            break 'batches;
                        }
                    };
                    let mode_count = if projected.used_owned_schema_fallback {
                        &mut coordinator.stats.owned_schema_fallback_blocks
                    } else {
                        &mut coordinator.stats.borrowed_storage_blocks
                    };
                    *mode_count = match mode_count.checked_add(1) {
                        Some(value) => value,
                        None => {
                            coordinator.error =
                                Some(Error::Overflow("parallel decoded block mode count"));
                            break 'batches;
                        }
                    };
                    if let Err(error) = consume_ordered(row_number, projected.output) {
                        coordinator.error = Some(error);
                        break 'batches;
                    }
                }
            }

            // Closing both directions wakes a producer that is blocked either
            // on publishing a ready batch or on waiting for a recycled Vec.
            drop(ready_receiver);
            drop(free_sender);
            let producer_result = producer
                .join()
                .map_err(|_| Error::InvalidIndex("ordered block producer thread panicked".into()));

            if let Some(error) = coordinator.error {
                return Err(error);
            }
            let producer_stats = match producer_result {
                Ok(result) => result?,
                Err(error) => return Err(error),
            };
            if coordinator.producer_disconnected {
                return Err(Error::InvalidIndex(
                    "ordered block producer stopped before the requested range was complete".into(),
                ));
            }
            coordinator.stats.read_call_count = producer_stats.read_call_count;
            coordinator.stats.compressed_bytes = producer_stats.compressed_bytes;
            coordinator.stats.producer_read_wall_time = producer_stats.read_wall_time;
            coordinator.stats.producer_wait_for_free_buffer_time =
                producer_stats.wait_for_free_buffer_time;
            coordinator.stats.max_compressed_batch_bytes =
                producer_stats.max_compressed_batch_bytes;
            coordinator.stats.max_declared_uncompressed_batch_bytes =
                producer_stats.max_declared_uncompressed_batch_bytes;
            Ok(coordinator.stats)
        })
    }

    /// Process each compressed batch through two parallel borrowed stages with
    /// one ordered barrier between them.
    ///
    /// This compatibility entry point keeps the original callback signatures.
    /// Use
    /// [`Self::process_borrowed_blocks_parallel_batch_barrier_with_transaction_state`]
    /// when both stages need one reusable state item per storage transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn process_borrowed_blocks_parallel_batch_barrier<
        WorkerState,
        CoordinatorState,
        StageAOutput,
        StageBOutput,
        MakeWorkerState,
        StageA,
        MergeStageA,
        FinishStageA,
        StageB,
        ConsumeStageB,
    >(
        &self,
        range: Range<usize>,
        config: OrderedParallelBlockConfig,
        coordinator_state: &mut CoordinatorState,
        make_worker_state: MakeWorkerState,
        stage_a: StageA,
        merge_stage_a_ordered: MergeStageA,
        finish_stage_a_batch: FinishStageA,
        stage_b: StageB,
        consume_stage_b_ordered: ConsumeStageB,
    ) -> Result<BatchBarrierBlockStats>
    where
        WorkerState: Send,
        CoordinatorState: Sync,
        StageAOutput: Send,
        StageBOutput: Send,
        MakeWorkerState: FnMut(usize) -> WorkerState,
        StageA: for<'block> Fn(
                &mut WorkerState,
                usize,
                BorrowedDecodedBlock<'block>,
            ) -> Result<StageAOutput>
            + Send
            + Sync,
        MergeStageA: FnMut(&mut CoordinatorState, usize, StageAOutput) -> Result<()>,
        FinishStageA: FnMut(&mut CoordinatorState, Range<usize>) -> Result<()>,
        StageB: for<'block> Fn(
                &mut WorkerState,
                &CoordinatorState,
                usize,
                BorrowedDecodedBlock<'block>,
            ) -> Result<StageBOutput>
            + Send
            + Sync,
        ConsumeStageB: FnMut(&mut CoordinatorState, usize, StageBOutput) -> Result<()>,
    {
        self.process_borrowed_blocks_parallel_batch_barrier_with_transaction_state(
            range,
            config,
            0,
            coordinator_state,
            make_worker_state,
            |worker, _, row_number, block, _: &mut [()]| stage_a(worker, row_number, block),
            merge_stage_a_ordered,
            finish_stage_a_batch,
            |worker, state, row_number, block, _: &[()]| stage_b(worker, state, row_number, block),
            consume_stage_b_ordered,
        )
    }

    /// Process each compressed batch through two parallel borrowed stages with
    /// one ordered barrier and reusable per-transaction state between them.
    ///
    /// Each block frame is read and decompressed exactly once. Stage A runs in
    /// parallel against batch-owned decompressed buffers. After every stage-A
    /// projection in the batch succeeds, `merge_stage_a_ordered` receives the
    /// owned outputs in increasing block-row order. Stage A receives an
    /// immutable view of the coordinator state before that merge. After
    /// `finish_stage_a_batch`, stage B receives its post-merge view.
    ///
    /// Each stage-A block also receives a mutable transaction-state slice in
    /// exact storage transaction-row order. Stage B receives the same slice as
    /// immutable state. Its length equals the block's validated `tx_count`.
    /// Active entries are reset to `TransactionState::default()` before stage
    /// A. The per-block state buffers and their checked block-prefix table keep
    /// capacity across batches. `transaction_state_budget_bytes` bounds total
    /// active state bytes before either stage runs.
    ///
    /// The reader reparses the borrowed block view for stage B, but it does not
    /// read or decompress the frame again. Both callbacks can use
    /// [`BorrowedDecodedBlock::uncompressed_bytes`] to observe the same byte
    /// address. Stage-A callbacks should not make external side effects: if any
    /// stage-A row fails, no stage-A output is merged and stage B does not run
    /// for that batch. Stage-B callbacks can make completion-order side effects
    /// when the caller accepts partial output on error; ordered stage-B
    /// consumers do not run unless every stage-B row in the batch succeeds.
    #[allow(clippy::too_many_arguments)]
    pub fn process_borrowed_blocks_parallel_batch_barrier_with_transaction_state<
        WorkerState,
        CoordinatorState,
        TransactionState,
        StageAOutput,
        StageBOutput,
        MakeWorkerState,
        StageA,
        MergeStageA,
        FinishStageA,
        StageB,
        ConsumeStageB,
    >(
        &self,
        range: Range<usize>,
        config: OrderedParallelBlockConfig,
        transaction_state_budget_bytes: usize,
        coordinator_state: &mut CoordinatorState,
        mut make_worker_state: MakeWorkerState,
        stage_a: StageA,
        mut merge_stage_a_ordered: MergeStageA,
        mut finish_stage_a_batch: FinishStageA,
        stage_b: StageB,
        mut consume_stage_b_ordered: ConsumeStageB,
    ) -> Result<BatchBarrierBlockStats>
    where
        WorkerState: Send,
        CoordinatorState: Sync,
        TransactionState: Copy + Default + Send + Sync,
        StageAOutput: Send,
        StageBOutput: Send,
        MakeWorkerState: FnMut(usize) -> WorkerState,
        StageA: for<'block> Fn(
                &mut WorkerState,
                &CoordinatorState,
                usize,
                BorrowedDecodedBlock<'block>,
                &mut [TransactionState],
            ) -> Result<StageAOutput>
            + Send
            + Sync,
        MergeStageA: FnMut(&mut CoordinatorState, usize, StageAOutput) -> Result<()>,
        FinishStageA: FnMut(&mut CoordinatorState, Range<usize>) -> Result<()>,
        StageB: for<'block> Fn(
                &mut WorkerState,
                &CoordinatorState,
                usize,
                BorrowedDecodedBlock<'block>,
                &[TransactionState],
            ) -> Result<StageBOutput>
            + Send
            + Sync,
        ConsumeStageB: FnMut(&mut CoordinatorState, usize, StageBOutput) -> Result<()>,
    {
        let row_count = self.index.rows.len();
        if range.start > range.end || range.end > row_count {
            return Err(Error::InvalidIndex(format!(
                "block row range {}..{} is outside 0..{row_count}",
                range.start, range.end,
            )));
        }
        validate_ordered_parallel_config(config)?;
        if range.is_empty() {
            return Ok(BatchBarrierBlockStats::default());
        }

        let compressed_target = config
            .compressed_batch_target_bytes
            .min(self.options.prefetch_bytes);
        let plans = ordered_parallel_batch_plans(
            &self.index.rows,
            range,
            compressed_target,
            config.uncompressed_batch_budget_bytes,
            config.max_blocks_per_batch,
        )?;
        let decode_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.decode_workers)
            .thread_name(|index| format!("blockzilla-block-batch-barrier-{index}"))
            .build()
            .map_err(|error| {
                Error::InvalidManifest(format!(
                    "cannot create batch-barrier block decode pool: {error}"
                ))
            })?;
        let workers: Vec<_> = (0..config.decode_workers)
            .map(|worker| {
                Mutex::new(Some(BatchBarrierWorker {
                    decompressor: None,
                    caller: make_worker_state(worker),
                }))
            })
            .collect();

        let (free_sender, free_receiver) = sync_channel(config.compressed_buffer_count);
        for _ in 0..config.compressed_buffer_count {
            free_sender
                .send(Vec::new())
                .expect("the new recycled-buffer channel has a receiver");
        }
        let (ready_sender, ready_receiver) = sync_channel(config.compressed_buffer_count);

        thread::scope(|scope| {
            let producer = scope.spawn(|| {
                produce_ordered_compressed_batches(
                    self,
                    &plans,
                    compressed_target,
                    free_receiver,
                    ready_sender,
                )
            });
            let mut stats = BatchBarrierBlockStats::default();
            let mut first_error = None;
            let mut producer_disconnected = false;
            let mut decompressed = Vec::<BatchBarrierDecodedBuffer>::new();
            let mut stage_a_results = Vec::new();
            let mut stage_b_results = Vec::new();
            let mut stage_a_owned_fallbacks = Vec::new();
            let mut transaction_state_offsets = Vec::new();
            let mut transaction_state_buffers =
                Vec::<BatchBarrierTransactionStateBuffer<TransactionState>>::new();

            'batches: for expected in &plans {
                let wait_started = Instant::now();
                let ready = match ready_receiver.recv() {
                    Ok(ready) => ready,
                    Err(_) => {
                        producer_disconnected = true;
                        break;
                    }
                };
                stats.coordinator_wait_for_ready_batch_time = stats
                    .coordinator_wait_for_ready_batch_time
                    .saturating_add(wait_started.elapsed());
                if ready.plan != *expected {
                    let observed = ready.plan;
                    let _ = free_sender.send(recycle_ordered_compressed_buffer(ready));
                    first_error = Some(Error::InvalidIndex(format!(
                        "batch-barrier producer returned rows {}..{}, expected {}..{}",
                        observed.row_start, observed.row_end, expected.row_start, expected.row_end,
                    )));
                    break;
                }

                let batch_block_count = expected.row_end - expected.row_start;
                let (transaction_state_count, transaction_state_bytes) =
                    match prepare_batch_transaction_state_offsets(
                        &self.index.rows[expected.row_start..expected.row_end],
                        &mut transaction_state_offsets,
                        std::mem::size_of::<TransactionState>(),
                        transaction_state_budget_bytes,
                    ) {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            let _ = free_sender.send(recycle_ordered_compressed_buffer(ready));
                            first_error = Some(error);
                            break;
                        }
                    };
                if transaction_state_offsets.last().copied() != Some(transaction_state_count) {
                    let _ = free_sender.send(recycle_ordered_compressed_buffer(ready));
                    first_error = Some(Error::InvalidIndex(
                        "batch-barrier transaction-state prefix has the wrong total".into(),
                    ));
                    break;
                }
                stats.max_live_transaction_state_bytes = stats
                    .max_live_transaction_state_bytes
                    .max(transaction_state_bytes);
                if transaction_state_buffers.len() < batch_block_count {
                    transaction_state_buffers.resize_with(batch_block_count, Default::default);
                }
                for (batch_row, buffer) in transaction_state_buffers[..batch_block_count]
                    .iter_mut()
                    .enumerate()
                {
                    let transaction_count = transaction_state_offsets[batch_row + 1]
                        - transaction_state_offsets[batch_row];
                    let reused = match buffer.prepare(transaction_count) {
                        Ok(reused) => reused,
                        Err(error) => {
                            first_error = Some(error);
                            break;
                        }
                    };
                    if transaction_count == 0 || std::mem::size_of::<TransactionState>() == 0 {
                        continue;
                    }
                    let counter = if reused {
                        &mut stats.transaction_state_buffer_reuse_count
                    } else {
                        &mut stats.transaction_state_buffer_growth_count
                    };
                    *counter = match counter.checked_add(1) {
                        Some(value) => value,
                        None => {
                            first_error = Some(Error::Overflow(
                                "batch-barrier transaction-state buffer count",
                            ));
                            break;
                        }
                    };
                }
                if first_error.is_some() {
                    let _ = free_sender.send(recycle_ordered_compressed_buffer(ready));
                    break;
                }
                if decompressed.len() < batch_block_count {
                    decompressed.resize_with(batch_block_count, Default::default);
                }
                let retention_limit = config.uncompressed_batch_budget_bytes;
                for (row, buffer) in self.index.rows[expected.row_start..expected.row_end]
                    .iter()
                    .zip(&mut decompressed[..batch_block_count])
                {
                    if buffer.prepare(row.uncompressed_len as usize, retention_limit) {
                        stats.decompressed_buffer_reuse_count =
                            match stats.decompressed_buffer_reuse_count.checked_add(1) {
                                Some(value) => value,
                                None => {
                                    first_error =
                                        Some(Error::Overflow("decompressed buffer reuse count"));
                                    break;
                                }
                            };
                    } else {
                        stats.decompressed_buffer_growth_count =
                            match stats.decompressed_buffer_growth_count.checked_add(1) {
                                Some(value) => value,
                                None => {
                                    first_error =
                                        Some(Error::Overflow("decompressed buffer growth count"));
                                    break;
                                }
                            };
                    }
                }
                if first_error.is_some() {
                    let _ = free_sender.send(recycle_ordered_compressed_buffer(ready));
                    recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                    break;
                }

                let stage_a_started = Instant::now();
                let pre_merge_coordinator: &CoordinatorState = &*coordinator_state;
                decode_pool.install(|| {
                    self.index.rows[expected.row_start..expected.row_end]
                        .par_iter()
                        .zip(decompressed[..batch_block_count].par_iter_mut())
                        .zip(transaction_state_buffers[..batch_block_count].par_iter_mut())
                        .enumerate()
                        .map(|(batch_row, ((row, buffer), transaction_state_buffer))| {
                            let row_number = expected.row_start + batch_row;
                            let compressed = ordered_batch_frame(&ready, *row)?;
                            let worker_number = rayon::current_thread_index().ok_or_else(|| {
                                Error::InvalidIndex(
                                    "batch-barrier task ran outside its decode pool".into(),
                                )
                            })?;
                            let mut worker_guard = workers[worker_number].lock().map_err(|_| {
                                Error::InvalidIndex("batch-barrier worker state is poisoned".into())
                            })?;
                            let mut worker = worker_guard.take().ok_or_else(|| {
                                Error::InvalidIndex(
                                    "batch-barrier worker was re-entered by nested work".into(),
                                )
                            })?;
                            drop(worker_guard);
                            let result = worker.decompress_and_project(
                                self,
                                *row,
                                compressed,
                                &mut buffer.bytes,
                                row_number,
                                config.discard_rewards,
                                pre_merge_coordinator,
                                &mut transaction_state_buffer.states,
                                &stage_a,
                            );
                            let mut worker_guard = workers[worker_number].lock().map_err(|_| {
                                Error::InvalidIndex("batch-barrier worker state is poisoned".into())
                            })?;
                            *worker_guard = Some(worker);
                            result
                        })
                        .collect_into_vec(&mut stage_a_results)
                });
                stats.coordinator_stage_a_wall_time = stats
                    .coordinator_stage_a_wall_time
                    .saturating_add(stage_a_started.elapsed());

                // Stage A has finished using the compressed bytes. Returning
                // this buffer lets the producer overlap the next range read
                // while the coordinator merges and stage B uses decompressed
                // bytes only.
                let _ = free_sender.send(recycle_ordered_compressed_buffer(ready));

                if stage_a_results.iter().any(Result::is_err) {
                    first_error = std::mem::take(&mut stage_a_results)
                        .into_iter()
                        .find_map(Result::err);
                    recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                    break;
                }

                let live_bytes = match live_batch_barrier_bytes(&decompressed[..batch_block_count])
                {
                    Ok(value) => value,
                    Err(error) => {
                        first_error = Some(error);
                        recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                        break;
                    }
                };
                let declared_live_bytes =
                    match usize::try_from(expected.declared_uncompressed_bytes) {
                        Ok(value) => value,
                        Err(_) => {
                            first_error = Some(Error::Overflow(
                                "batch-barrier declared uncompressed byte count",
                            ));
                            recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                            break;
                        }
                    };
                if live_bytes != declared_live_bytes {
                    first_error = Some(Error::InvalidIndex(format!(
                        "batch-barrier retained {live_bytes} decompressed bytes, expected {declared_live_bytes}",
                    )));
                    recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                    break;
                }
                stats.max_live_decompressed_batch_bytes =
                    stats.max_live_decompressed_batch_bytes.max(live_bytes);

                let merge_started = Instant::now();
                stage_a_owned_fallbacks.clear();
                for (batch_row, result) in stage_a_results.drain(..).enumerate() {
                    let row_number = expected.row_start + batch_row;
                    let projected = result.expect("stage-A errors were handled above");
                    stage_a_owned_fallbacks.push(projected.used_owned_schema_fallback);
                    if let Err(error) =
                        merge_stage_a_ordered(coordinator_state, row_number, projected.output)
                    {
                        first_error = Some(error);
                        break;
                    }
                }
                if first_error.is_none()
                    && let Err(error) = finish_stage_a_batch(
                        coordinator_state,
                        expected.row_start..expected.row_end,
                    )
                {
                    first_error = Some(error);
                }
                stats.coordinator_merge_wall_time = stats
                    .coordinator_merge_wall_time
                    .saturating_add(merge_started.elapsed());
                if first_error.is_some() {
                    recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                    break;
                }

                let stage_b_started = Instant::now();
                let shared_coordinator: &CoordinatorState = &*coordinator_state;
                decode_pool.install(|| {
                    self.index.rows[expected.row_start..expected.row_end]
                        .par_iter()
                        .zip(decompressed[..batch_block_count].par_iter())
                        .zip(transaction_state_buffers[..batch_block_count].par_iter())
                        .enumerate()
                        .map(|(batch_row, ((row, buffer), transaction_state_buffer))| {
                            let row_number = expected.row_start + batch_row;
                            let worker_number = rayon::current_thread_index().ok_or_else(|| {
                                Error::InvalidIndex(
                                    "batch-barrier task ran outside its decode pool".into(),
                                )
                            })?;
                            let mut worker_guard = workers[worker_number].lock().map_err(|_| {
                                Error::InvalidIndex("batch-barrier worker state is poisoned".into())
                            })?;
                            let mut worker = worker_guard.take().ok_or_else(|| {
                                Error::InvalidIndex(
                                    "batch-barrier worker was re-entered by nested work".into(),
                                )
                            })?;
                            drop(worker_guard);
                            let result = worker.project_decompressed(
                                self,
                                *row,
                                &buffer.bytes,
                                row_number,
                                config.discard_rewards,
                                shared_coordinator,
                                &transaction_state_buffer.states,
                                &stage_b,
                            );
                            let mut worker_guard = workers[worker_number].lock().map_err(|_| {
                                Error::InvalidIndex("batch-barrier worker state is poisoned".into())
                            })?;
                            *worker_guard = Some(worker);
                            result
                        })
                        .collect_into_vec(&mut stage_b_results)
                });
                stats.coordinator_stage_b_wall_time = stats
                    .coordinator_stage_b_wall_time
                    .saturating_add(stage_b_started.elapsed());

                if stage_b_results.iter().any(Result::is_err) {
                    first_error = std::mem::take(&mut stage_b_results)
                        .into_iter()
                        .find_map(Result::err);
                    recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                    break;
                }

                let mut borrowed_blocks = 0u64;
                let mut owned_fallback_blocks = 0u64;
                for (batch_row, result) in stage_b_results.drain(..).enumerate() {
                    let row_number = expected.row_start + batch_row;
                    let projected = result.expect("stage-B errors were handled above");
                    if stage_a_owned_fallbacks.get(batch_row).copied()
                        != Some(projected.used_owned_schema_fallback)
                    {
                        first_error = Some(Error::InvalidIndex(format!(
                            "batch-barrier block row {row_number} changed decode mode between stages",
                        )));
                        break;
                    }
                    if projected.used_owned_schema_fallback {
                        owned_fallback_blocks = match owned_fallback_blocks.checked_add(1) {
                            Some(value) => value,
                            None => {
                                first_error = Some(Error::Overflow(
                                    "batch-barrier owned fallback block count",
                                ));
                                break;
                            }
                        };
                    } else {
                        borrowed_blocks = match borrowed_blocks.checked_add(1) {
                            Some(value) => value,
                            None => {
                                first_error =
                                    Some(Error::Overflow("batch-barrier borrowed block count"));
                                break;
                            }
                        };
                    }
                    if let Err(error) =
                        consume_stage_b_ordered(coordinator_state, row_number, projected.output)
                    {
                        first_error = Some(error);
                        break;
                    }
                }
                if first_error.is_some() {
                    recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                    break 'batches;
                }

                let batch_blocks = match u64::try_from(batch_block_count) {
                    Ok(value) => value,
                    Err(_) => {
                        first_error = Some(Error::Overflow("batch-barrier block count"));
                        recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                        break;
                    }
                };
                let add_counter = |current: u64, increment: u64, label| {
                    current.checked_add(increment).ok_or(Error::Overflow(label))
                };
                stats.batch_count =
                    match add_counter(stats.batch_count, 1, "batch-barrier batch count") {
                        Ok(value) => value,
                        Err(error) => {
                            first_error = Some(error);
                            recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                            break;
                        }
                    };
                for (field, increment, label) in [
                    (
                        &mut stats.block_count,
                        batch_blocks,
                        "batch-barrier block count",
                    ),
                    (
                        &mut stats.decompression_count,
                        batch_blocks,
                        "batch-barrier decompression count",
                    ),
                    (
                        &mut stats.stage_a_block_count,
                        batch_blocks,
                        "batch-barrier stage-A block count",
                    ),
                    (
                        &mut stats.stage_b_block_count,
                        batch_blocks,
                        "batch-barrier stage-B block count",
                    ),
                    (
                        &mut stats.borrowed_storage_blocks,
                        borrowed_blocks,
                        "batch-barrier borrowed block count",
                    ),
                    (
                        &mut stats.owned_schema_fallback_blocks,
                        owned_fallback_blocks,
                        "batch-barrier owned fallback block count",
                    ),
                ] {
                    match add_counter(*field, increment, label) {
                        Ok(value) => *field = value,
                        Err(error) => {
                            first_error = Some(error);
                            break;
                        }
                    }
                }
                if first_error.is_some() {
                    recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                    break;
                }
                stats.decompressed_bytes = match stats
                    .decompressed_bytes
                    .checked_add(expected.declared_uncompressed_bytes)
                {
                    Some(value) => value,
                    None => {
                        first_error =
                            Some(Error::Overflow("batch-barrier decompressed byte count"));
                        recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                        break;
                    }
                };

                let retained_transaction_state_bytes =
                    match recycle_batch_barrier_transaction_state_buffers(
                        &mut transaction_state_buffers,
                        transaction_state_budget_bytes,
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            first_error = Some(error);
                            recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                            break;
                        }
                    };
                stats.max_retained_transaction_state_capacity_bytes = stats
                    .max_retained_transaction_state_capacity_bytes
                    .max(retained_transaction_state_bytes);

                let retained = recycle_batch_barrier_buffers(&mut decompressed, retention_limit);
                stats.max_retained_decompressed_capacity_bytes =
                    stats.max_retained_decompressed_capacity_bytes.max(retained);
            }

            // Closing both directions wakes a producer that is blocked either
            // on publishing a ready batch or on waiting for a recycled Vec.
            drop(ready_receiver);
            drop(free_sender);
            let producer_result = producer.join().map_err(|_| {
                Error::InvalidIndex("batch-barrier block producer thread panicked".into())
            });

            if let Some(error) = first_error {
                return Err(error);
            }
            let producer_stats = match producer_result {
                Ok(result) => result?,
                Err(error) => return Err(error),
            };
            if producer_disconnected {
                return Err(Error::InvalidIndex(
                    "batch-barrier block producer stopped before the requested range was complete"
                        .into(),
                ));
            }
            stats.read_call_count = producer_stats.read_call_count;
            stats.compressed_bytes = producer_stats.compressed_bytes;
            stats.producer_read_wall_time = producer_stats.read_wall_time;
            stats.producer_wait_for_free_buffer_time = producer_stats.wait_for_free_buffer_time;
            stats.max_compressed_batch_bytes = producer_stats.max_compressed_batch_bytes;
            stats.max_declared_uncompressed_batch_bytes =
                producer_stats.max_declared_uncompressed_batch_bytes;
            if stats.read_call_count != stats.batch_count {
                return Err(Error::InvalidIndex(format!(
                    "batch-barrier made {} block range reads for {} batches",
                    stats.read_call_count, stats.batch_count,
                )));
            }
            if stats.decompression_count != stats.block_count
                || stats.stage_a_block_count != stats.block_count
                || stats.stage_b_block_count != stats.block_count
            {
                return Err(Error::InvalidIndex(
                    "batch-barrier block, decompression, and stage counts differ".into(),
                ));
            }
            Ok(stats)
        })
    }

    pub fn scan<'a>(&'a self, filter: &'a CompiledPubkeyFilter) -> Result<ScanIterator<'a, S>> {
        self.ensure_filter_binding(filter)?;
        Ok(ScanIterator {
            archive: self,
            filter,
            blocks: self.blocks(),
        })
    }

    pub fn read_block(&self, row_number: usize) -> Result<DecodedBlock> {
        let row = *self.index.rows.get(row_number).ok_or_else(|| {
            Error::InvalidIndex(format!("block row {row_number} is out of bounds"))
        })?;
        let compressed = self.source.read_range(
            BLOCKS_FILE,
            row.compressed_offset,
            row.compressed_len as usize,
        )?;
        self.decode_compressed_block(row, &compressed)
    }

    fn decode_compressed_block(
        &self,
        row: ArchiveV2HotBlockIndexRow,
        compressed: &[u8],
    ) -> Result<DecodedBlock> {
        validate_exact_zstd_frame(&row, compressed)?;
        let expected_length = row.uncompressed_len as usize;
        let bytes = zstd::bulk::decompress(compressed, expected_length).map_err(|error| {
            Error::DecodeBlock {
                slot: row.slot,
                message: format!("zstd frame: {error}"),
            }
        })?;
        self.decode_uncompressed_block(row, &bytes)
    }

    fn decode_compressed_block_reusing(
        &self,
        row: ArchiveV2HotBlockIndexRow,
        compressed: &[u8],
        decompressor: &mut zstd::bulk::Decompressor<'static>,
        decompressed: &mut Vec<u8>,
    ) -> Result<DecodedBlock> {
        validate_exact_zstd_frame(&row, compressed)?;
        let expected_length = row.uncompressed_len as usize;
        decompressed.clear();
        if decompressed.capacity() < expected_length {
            // `reserve_exact` is relative to the current length, which is zero
            // after `clear`, not relative to the retained capacity.
            decompressed.reserve_exact(expected_length);
        }
        let written = decompressor
            .decompress_to_buffer(compressed, decompressed)
            .map_err(|error| Error::DecodeBlock {
                slot: row.slot,
                message: format!("zstd frame: {error}"),
            })?;
        if written != decompressed.len() {
            return Err(Error::DecodeBlock {
                slot: row.slot,
                message: format!(
                    "zstd reported {written} output bytes but exposed {}",
                    decompressed.len()
                ),
            });
        }
        self.decode_uncompressed_block(row, decompressed)
    }

    fn decode_compressed_block_borrowed_reusing<'a>(
        &self,
        row: ArchiveV2HotBlockIndexRow,
        compressed: &[u8],
        decompressor: &mut zstd::bulk::Decompressor<'static>,
        decompressed: &'a mut Vec<u8>,
        discard_rewards: bool,
    ) -> Result<BorrowedDecodedBlock<'a>> {
        validate_exact_zstd_frame(&row, compressed)?;
        let expected_length = row.uncompressed_len as usize;
        decompressed.clear();
        if decompressed.capacity() < expected_length {
            decompressed.reserve_exact(expected_length);
        }
        let written = decompressor
            .decompress_to_buffer(compressed, &mut *decompressed)
            .map_err(|error| Error::DecodeBlock {
                slot: row.slot,
                message: format!("zstd frame: {error}"),
            })?;
        if written != decompressed.len() {
            return Err(Error::DecodeBlock {
                slot: row.slot,
                message: format!(
                    "zstd reported {written} output bytes but exposed {}",
                    decompressed.len()
                ),
            });
        }
        self.decode_uncompressed_block_borrowed(row, decompressed, discard_rewards)
    }

    fn decode_uncompressed_block(
        &self,
        row: ArchiveV2HotBlockIndexRow,
        bytes: &[u8],
    ) -> Result<DecodedBlock> {
        let expected_length = row.uncompressed_len as usize;
        if bytes.len() != expected_length {
            return Err(Error::InvalidBlock {
                slot: row.slot,
                message: format!(
                    "zstd output is {} bytes, expected {}",
                    bytes.len(),
                    expected_length
                ),
            });
        }
        let block =
            deserialize_archive_v2_hot_block_blob(bytes).map_err(|error| Error::DecodeBlock {
                slot: row.slot,
                message: error.to_string(),
            })?;
        validate_decoded_block(&row, &block)?;
        Ok(DecodedBlock {
            index_row: row,
            block,
        })
    }

    fn decode_uncompressed_block_borrowed<'a>(
        &self,
        row: ArchiveV2HotBlockIndexRow,
        bytes: &'a [u8],
        discard_rewards: bool,
    ) -> Result<BorrowedDecodedBlock<'a>> {
        let expected_length = row.uncompressed_len as usize;
        if bytes.len() != expected_length {
            return Err(Error::InvalidBlock {
                slot: row.slot,
                message: format!(
                    "zstd output is {} bytes, expected {}",
                    bytes.len(),
                    expected_length
                ),
            });
        }
        let current = if discard_rewards {
            deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(bytes)
                .map(BorrowedDecodedBlockPayload::CurrentWithoutRewards)
        } else {
            deserialize_archive_v2_hot_block_blob_borrowed_current(bytes)
                .map(BorrowedDecodedBlockPayload::Current)
        };
        let block = match current {
            Ok(block) => block,
            Err(_) => BorrowedDecodedBlockPayload::OwnedFallback(
                deserialize_archive_v2_hot_block_blob(bytes).map_err(|error| {
                    Error::DecodeBlock {
                        slot: row.slot,
                        message: error.to_string(),
                    }
                })?,
            ),
        };
        validate_borrowed_decoded_block(&row, &block)?;
        Ok(BorrowedDecodedBlock {
            index_row: row,
            block,
            uncompressed_bytes: bytes,
        })
    }

    pub fn scan_decoded_block(
        &self,
        filter: &CompiledPubkeyFilter,
        decoded: DecodedBlock,
    ) -> Result<ScannedBlock> {
        self.ensure_filter_binding(filter)?;
        let DecodedBlock { index_row, block } = decoded;
        let mut first_signature_ordinal = index_row.first_signature_ordinal;
        let mut transactions = Vec::with_capacity(block.tx_rows.len());
        let scan_context = TransactionScanContext {
            filter,
            registry_entries: self.registry_entries,
            message_projector: self.message_projector(),
            metadata_wire_profile: self.metadata_wire_profile,
        };
        for row in block.tx_rows.iter().copied() {
            let signatures = SignatureReference {
                generation_digest: self.binding.generation_digest,
                reader_id: self.reader_id,
                first_ordinal: first_signature_ordinal,
                count: row.signature_count,
            };
            first_signature_ordinal = first_signature_ordinal
                .checked_add(u64::from(row.signature_count))
                .ok_or(Error::Overflow("transaction signature ordinal"))?;
            transactions.push(scan_transaction(
                index_row.slot,
                row,
                &block,
                signatures,
                &scan_context,
            )?);
        }
        transactions.sort_unstable_by_key(|transaction| transaction.tx_index);
        Ok(ScannedBlock {
            block_id: index_row.block_id,
            slot: block.header.slot,
            parent_slot: block.header.parent_slot,
            block_time: block.header.block_time,
            block_height: block.header.block_height,
            transactions,
        })
    }

    pub fn read_signature_ordinal(&self, ordinal: u64) -> Result<[u8; 64]> {
        if !self.signatures_available {
            return Err(Error::SignaturesUnavailable);
        }
        if ordinal >= self.total_signatures {
            return Err(Error::InvalidIndex(format!(
                "signature ordinal {ordinal} is outside {} signatures",
                self.total_signatures
            )));
        }
        let offset = ordinal
            .checked_mul(64)
            .ok_or(Error::Overflow("signature byte offset"))?;
        let bytes = self.source.read_range(SIGNATURES_FILE, offset, 64)?;
        let mut signature = [0u8; 64];
        signature.copy_from_slice(&bytes);
        Ok(signature)
    }

    pub fn read_transaction_signatures(
        &self,
        reference: SignatureReference,
    ) -> Result<Vec<[u8; 64]>> {
        if reference.generation_digest != self.binding.generation_digest {
            return Err(Error::FilterBindingMismatch);
        }
        if reference.reader_id != self.reader_id {
            return Err(Error::FilterBindingMismatch);
        }
        if !self.signatures_available {
            return Err(Error::SignaturesUnavailable);
        }
        let end = reference
            .first_ordinal
            .checked_add(u64::from(reference.count))
            .ok_or(Error::Overflow("transaction signature range"))?;
        if end > self.total_signatures {
            return Err(Error::InvalidIndex(format!(
                "signature range {}..{} is outside {} signatures",
                reference.first_ordinal, end, self.total_signatures
            )));
        }
        let offset = reference
            .first_ordinal
            .checked_mul(64)
            .ok_or(Error::Overflow("signature byte offset"))?;
        let length = usize::from(reference.count) * 64;
        let bytes = self.source.read_range(SIGNATURES_FILE, offset, length)?;
        Ok(bytes
            .chunks_exact(64)
            .map(|bytes| {
                let mut signature = [0u8; 64];
                signature.copy_from_slice(bytes);
                signature
            })
            .collect())
    }

    fn ensure_filter_binding(&self, filter: &CompiledPubkeyFilter) -> Result<()> {
        if filter.reader_id != self.reader_id {
            return Err(Error::FilterBindingMismatch);
        }
        if filter.binding != self.binding {
            return Err(Error::FilterBindingMismatch);
        }
        Ok(())
    }
}

fn validate_exact_zstd_frame(row: &ArchiveV2HotBlockIndexRow, compressed: &[u8]) -> Result<()> {
    if compressed.len() != row.compressed_len as usize {
        return Err(Error::InvalidBlock {
            slot: row.slot,
            message: format!(
                "compressed frame is {} bytes, expected {}",
                compressed.len(),
                row.compressed_len
            ),
        });
    }
    let first_frame_len =
        zstd::zstd_safe::find_frame_compressed_size(compressed).map_err(|error_code| {
            Error::DecodeBlock {
                slot: row.slot,
                message: format!(
                    "invalid zstd frame: {}",
                    zstd::zstd_safe::get_error_name(error_code)
                ),
            }
        })?;
    if first_frame_len != compressed.len() {
        return Err(Error::InvalidBlock {
            slot: row.slot,
            message: format!(
                "first zstd frame is {first_frame_len} bytes, but the index row contains {} bytes",
                compressed.len()
            ),
        });
    }
    Ok(())
}

fn validate_ordered_parallel_config(config: OrderedParallelBlockConfig) -> Result<()> {
    if config.compressed_batch_target_bytes == 0
        || config.uncompressed_batch_budget_bytes == 0
        || config.max_blocks_per_batch == 0
        || config.compressed_buffer_count == 0
        || config.decode_workers == 0
    {
        return Err(Error::InvalidManifest(
            "ordered parallel block limits and worker counts must be non-zero".into(),
        ));
    }
    if config.decode_workers > MAX_ORDERED_PARALLEL_DECODE_WORKERS {
        return Err(Error::InvalidManifest(format!(
            "ordered parallel decode_workers {} exceeds the {MAX_ORDERED_PARALLEL_DECODE_WORKERS} worker limit",
            config.decode_workers,
        )));
    }
    if config.compressed_buffer_count > MAX_ORDERED_PARALLEL_COMPRESSED_BUFFERS {
        return Err(Error::InvalidManifest(format!(
            "ordered parallel compressed_buffer_count {} exceeds the {MAX_ORDERED_PARALLEL_COMPRESSED_BUFFERS} buffer limit",
            config.compressed_buffer_count,
        )));
    }
    if config.max_blocks_per_batch > MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH {
        return Err(Error::InvalidManifest(format!(
            "ordered parallel max_blocks_per_batch {} exceeds the {MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH} block limit",
            config.max_blocks_per_batch,
        )));
    }
    if config.uncompressed_batch_budget_bytes > MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES {
        return Err(Error::InvalidManifest(format!(
            "ordered parallel uncompressed batch budget {} exceeds the {MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES} byte limit",
            config.uncompressed_batch_budget_bytes,
        )));
    }
    let retained_total = config
        .retained_decompressed_bytes_per_worker
        .checked_mul(config.decode_workers)
        .ok_or(Error::Overflow(
            "ordered parallel retained decompression capacity",
        ))?;
    if retained_total > MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES {
        return Err(Error::InvalidManifest(format!(
            "ordered parallel retained decompression capacity {retained_total} exceeds the {MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES} byte limit",
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrderedParallelBatchPlan {
    row_start: usize,
    row_end: usize,
    compressed_offset: u64,
    compressed_len: usize,
    declared_uncompressed_bytes: u64,
}

fn ordered_parallel_batch_plans(
    rows: &[ArchiveV2HotBlockIndexRow],
    range: Range<usize>,
    compressed_target: usize,
    uncompressed_budget: usize,
    max_blocks_per_batch: usize,
) -> Result<Vec<OrderedParallelBatchPlan>> {
    let mut plans = Vec::new();
    let mut start = range.start;
    let uncompressed_budget = u64::try_from(uncompressed_budget)
        .map_err(|_| Error::Overflow("parallel uncompressed batch budget"))?;
    while start < range.end {
        let first = rows[start];
        let mut end = start;
        let mut compressed_len = 0usize;
        let mut declared_uncompressed_bytes = 0u64;
        while end < range.end {
            let row = rows[end];
            let expected_offset = first
                .compressed_offset
                .checked_add(
                    u64::try_from(compressed_len)
                        .map_err(|_| Error::Overflow("parallel compressed batch offset"))?,
                )
                .ok_or(Error::Overflow("parallel compressed batch offset"))?;
            if row.compressed_offset != expected_offset {
                return Err(Error::InvalidIndex(format!(
                    "slot {} starts at {}, expected monotonic batch offset {expected_offset}",
                    row.slot, row.compressed_offset,
                )));
            }
            let next_compressed = compressed_len
                .checked_add(row.compressed_len as usize)
                .ok_or(Error::Overflow("parallel compressed batch length"))?;
            let next_uncompressed = declared_uncompressed_bytes
                .checked_add(u64::from(row.uncompressed_len))
                .ok_or(Error::Overflow("parallel uncompressed batch length"))?;
            if end > start
                && (end - start >= max_blocks_per_batch
                    || next_compressed > compressed_target
                    || next_uncompressed > uncompressed_budget)
            {
                break;
            }
            compressed_len = next_compressed;
            declared_uncompressed_bytes = next_uncompressed;
            end += 1;
        }
        plans.push(OrderedParallelBatchPlan {
            row_start: start,
            row_end: end,
            compressed_offset: first.compressed_offset,
            compressed_len,
            declared_uncompressed_bytes,
        });
        start = end;
    }
    Ok(plans)
}

struct OrderedReadyBatch {
    plan: OrderedParallelBatchPlan,
    bytes: Vec<u8>,
    retained_buffer: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
struct OrderedProducerStats {
    read_call_count: u64,
    compressed_bytes: u64,
    read_wall_time: Duration,
    wait_for_free_buffer_time: Duration,
    max_compressed_batch_bytes: usize,
    max_declared_uncompressed_batch_bytes: u64,
}

fn produce_ordered_compressed_batches<S: RangeSource>(
    archive: &ArchiveReader<S>,
    plans: &[OrderedParallelBatchPlan],
    retained_buffer_bytes: usize,
    free_receiver: Receiver<Vec<u8>>,
    ready_sender: SyncSender<OrderedReadyBatch>,
) -> Result<OrderedProducerStats> {
    let mut stats = OrderedProducerStats::default();
    for plan in plans {
        let wait_started = Instant::now();
        let Ok(recycled) = free_receiver.recv() else {
            // The coordinator stopped after an earlier ordered error.
            return Ok(stats);
        };
        stats.wait_for_free_buffer_time = stats
            .wait_for_free_buffer_time
            .saturating_add(wait_started.elapsed());

        let (mut bytes, retained_buffer) =
            ordered_compressed_read_buffer(recycled, plan.compressed_len, retained_buffer_bytes);
        let read_started = Instant::now();
        let read_result = archive.source.read_range_into(
            BLOCKS_FILE,
            plan.compressed_offset,
            plan.compressed_len,
            &mut bytes,
        );
        stats.read_wall_time = stats.read_wall_time.saturating_add(read_started.elapsed());
        read_result?;
        if bytes.len() != plan.compressed_len {
            return Err(Error::InvalidIndex(format!(
                "ordered block range returned {} bytes, expected {}",
                bytes.len(),
                plan.compressed_len,
            )));
        }
        stats.read_call_count = stats
            .read_call_count
            .checked_add(1)
            .ok_or(Error::Overflow("parallel range-read count"))?;
        stats.compressed_bytes = stats
            .compressed_bytes
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| Error::Overflow("parallel compressed byte count"))?,
            )
            .ok_or(Error::Overflow("parallel compressed byte count"))?;
        stats.max_compressed_batch_bytes = stats.max_compressed_batch_bytes.max(bytes.len());
        stats.max_declared_uncompressed_batch_bytes = stats
            .max_declared_uncompressed_batch_bytes
            .max(plan.declared_uncompressed_bytes);

        if ready_sender
            .send(OrderedReadyBatch {
                plan: *plan,
                bytes,
                retained_buffer,
            })
            .is_err()
        {
            // The coordinator selected an earlier row-order error.
            return Ok(stats);
        }
    }
    Ok(stats)
}

fn ordered_compressed_read_buffer(
    mut recycled: Vec<u8>,
    requested_bytes: usize,
    retained_buffer_bytes: usize,
) -> (Vec<u8>, Option<Vec<u8>>) {
    recycled.clear();
    if requested_bytes > retained_buffer_bytes {
        (Vec::new(), Some(recycled))
    } else {
        (recycled, None)
    }
}

fn recycle_ordered_compressed_buffer(mut ready: OrderedReadyBatch) -> Vec<u8> {
    if let Some(mut retained) = ready.retained_buffer.take() {
        retained.clear();
        retained
    } else {
        ready.bytes.clear();
        ready.bytes
    }
}

fn ordered_batch_frame(ready: &OrderedReadyBatch, row: ArchiveV2HotBlockIndexRow) -> Result<&[u8]> {
    let relative_offset = row
        .compressed_offset
        .checked_sub(ready.plan.compressed_offset)
        .ok_or_else(|| Error::InvalidIndex("parallel block frame offset underflow".into()))?;
    let relative_offset = usize::try_from(relative_offset)
        .map_err(|_| Error::Overflow("parallel block frame offset"))?;
    let frame_end = relative_offset
        .checked_add(row.compressed_len as usize)
        .ok_or(Error::Overflow("parallel block frame range"))?;
    ready
        .bytes
        .get(relative_offset..frame_end)
        .ok_or_else(|| Error::InvalidIndex("parallel block frame is outside its read batch".into()))
}

/// One retained row slot in the decompressed batch pool.
///
/// A frame larger than the total retention bound gets a temporary `Vec`; the
/// normal allocation for this row slot stays in `retained_buffer` and returns
/// after stage B. Normal frames keep their allocation across batches.
#[derive(Debug, Default)]
struct BatchBarrierDecodedBuffer {
    bytes: Vec<u8>,
    retained_buffer: Option<Vec<u8>>,
}

impl BatchBarrierDecodedBuffer {
    /// Prepare this row slot and report whether its retained allocation can
    /// hold the requested frame without growth.
    fn prepare(&mut self, requested_bytes: usize, retention_limit: usize) -> bool {
        debug_assert!(self.retained_buffer.is_none());
        debug_assert!(self.bytes.is_empty());
        if requested_bytes > retention_limit {
            self.retained_buffer = Some(std::mem::take(&mut self.bytes));
        }
        self.bytes.capacity() >= requested_bytes
    }

    fn recycle(&mut self) {
        self.bytes.clear();
        if let Some(mut retained) = self.retained_buffer.take() {
            retained.clear();
            self.bytes = retained;
        }
    }
}

fn live_batch_barrier_bytes(buffers: &[BatchBarrierDecodedBuffer]) -> Result<usize> {
    buffers.iter().try_fold(0usize, |total, buffer| {
        total.checked_add(buffer.bytes.len()).ok_or(Error::Overflow(
            "batch-barrier live decompressed byte count",
        ))
    })
}

fn prepare_batch_transaction_state_offsets(
    rows: &[ArchiveV2HotBlockIndexRow],
    offsets: &mut Vec<usize>,
    transaction_state_size: usize,
    transaction_state_budget_bytes: usize,
) -> Result<(usize, usize)> {
    let transaction_count = rows.iter().try_fold(0usize, |count, row| {
        let row_transactions = usize::try_from(row.tx_count)
            .map_err(|_| Error::Overflow("batch-barrier transaction count"))?;
        count
            .checked_add(row_transactions)
            .ok_or(Error::Overflow("batch-barrier transaction count"))
    })?;
    let transaction_state_bytes = transaction_count
        .checked_mul(transaction_state_size)
        .ok_or(Error::Overflow(
            "batch-barrier transaction-state byte count",
        ))?;
    if transaction_state_bytes > transaction_state_budget_bytes {
        return Err(Error::InvalidManifest(format!(
            "batch-barrier transaction state needs {transaction_state_bytes} bytes, exceeding its {transaction_state_budget_bytes}-byte budget",
        )));
    }

    let offset_count = rows.len().checked_add(1).ok_or(Error::Overflow(
        "batch-barrier transaction-state offset count",
    ))?;
    offsets.clear();
    offsets.try_reserve_exact(offset_count).map_err(|error| {
        Error::InvalidManifest(format!(
            "cannot reserve {offset_count} batch-barrier transaction-state offsets: {error}",
        ))
    })?;
    offsets.push(0);
    let mut prefix = 0usize;
    for row in rows {
        prefix = prefix
            .checked_add(row.tx_count as usize)
            .ok_or(Error::Overflow("batch-barrier transaction count"))?;
        offsets.push(prefix);
    }
    if prefix != transaction_count {
        return Err(Error::InvalidIndex(
            "batch-barrier transaction-state prefix total changed".into(),
        ));
    }
    Ok((transaction_count, transaction_state_bytes))
}

fn ensure_transaction_state_len(
    row: ArchiveV2HotBlockIndexRow,
    block: &BorrowedDecodedBlock<'_>,
    transaction_state_len: usize,
) -> Result<()> {
    let block_transaction_count = usize::try_from(block.tx_count())
        .map_err(|_| Error::Overflow("batch-barrier block transaction count"))?;
    if transaction_state_len != block_transaction_count {
        return Err(Error::InvalidBlock {
            slot: row.slot,
            message: format!(
                "transaction-state slice has {transaction_state_len} entries for {block_transaction_count} storage transactions",
            ),
        });
    }
    Ok(())
}

#[derive(Debug, Default)]
struct BatchBarrierTransactionStateBuffer<T> {
    states: Vec<T>,
}

impl<T: Copy + Default> BatchBarrierTransactionStateBuffer<T> {
    /// Reset this block-row slot and report whether its retained allocation
    /// can hold the requested storage transaction count without growth.
    fn prepare(&mut self, transaction_count: usize) -> Result<bool> {
        self.states.clear();
        let reused = self.states.capacity() >= transaction_count;
        if !reused {
            self.states
                .try_reserve_exact(transaction_count)
                .map_err(|error| {
                    Error::InvalidManifest(format!(
                        "cannot reserve {transaction_count} batch-barrier transaction states: {error}",
                    ))
                })?;
        }
        self.states.resize(transaction_count, T::default());
        Ok(reused)
    }
}

fn recycle_batch_barrier_transaction_state_buffers<T>(
    buffers: &mut [BatchBarrierTransactionStateBuffer<T>],
    retention_limit_bytes: usize,
) -> Result<usize> {
    let state_size = std::mem::size_of::<T>();
    let mut retained = 0usize;
    for buffer in buffers {
        buffer.states.clear();
        let capacity_bytes =
            buffer
                .states
                .capacity()
                .checked_mul(state_size)
                .ok_or(Error::Overflow(
                    "batch-barrier retained transaction-state bytes",
                ))?;
        match retained.checked_add(capacity_bytes) {
            Some(next) if next <= retention_limit_bytes => retained = next,
            _ => buffer.states = Vec::new(),
        }
    }
    Ok(retained)
}

/// Clear active buffers and keep at most `retention_limit` bytes of aggregate
/// capacity. Iteration order is stable, and no temporary sorting allocation is
/// needed on the recycling path.
fn recycle_batch_barrier_buffers(
    buffers: &mut [BatchBarrierDecodedBuffer],
    retention_limit: usize,
) -> usize {
    for buffer in &mut *buffers {
        buffer.recycle();
    }
    let mut retained = 0usize;
    for buffer in buffers {
        let capacity = buffer.bytes.capacity();
        match retained.checked_add(capacity) {
            Some(next) if next <= retention_limit => retained = next,
            _ => buffer.bytes = Vec::new(),
        }
    }
    retained
}

struct BatchBarrierWorker<T> {
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
    caller: T,
}

impl<T> BatchBarrierWorker<T> {
    #[allow(clippy::too_many_arguments)]
    fn decompress_and_project<S, CoordinatorState, TransactionState, Output, Project>(
        &mut self,
        archive: &ArchiveReader<S>,
        row: ArchiveV2HotBlockIndexRow,
        compressed: &[u8],
        decompressed: &mut Vec<u8>,
        row_number: usize,
        discard_rewards: bool,
        coordinator_state: &CoordinatorState,
        transaction_state: &mut [TransactionState],
        project: &Project,
    ) -> Result<OrderedParallelProjection<Output>>
    where
        S: RangeSource,
        Project: for<'block> Fn(
            &mut T,
            &CoordinatorState,
            usize,
            BorrowedDecodedBlock<'block>,
            &mut [TransactionState],
        ) -> Result<Output>,
    {
        if self.decompressor.is_none() {
            self.decompressor =
                Some(
                    zstd::bulk::Decompressor::new().map_err(|error| Error::DecodeBlock {
                        slot: row.slot,
                        message: format!("create zstd decompressor: {error}"),
                    })?,
                );
        }
        let block = archive.decode_compressed_block_borrowed_reusing(
            row,
            compressed,
            self.decompressor
                .as_mut()
                .expect("decompressor was initialized above"),
            decompressed,
            discard_rewards,
        )?;
        ensure_transaction_state_len(row, &block, transaction_state.len())?;
        let used_owned_schema_fallback = block.uses_owned_fallback();
        let output = project(
            &mut self.caller,
            coordinator_state,
            row_number,
            block,
            transaction_state,
        )?;
        Ok(OrderedParallelProjection {
            output,
            used_owned_schema_fallback,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn project_decompressed<S, CoordinatorState, TransactionState, Output, Project>(
        &mut self,
        archive: &ArchiveReader<S>,
        row: ArchiveV2HotBlockIndexRow,
        decompressed: &[u8],
        row_number: usize,
        discard_rewards: bool,
        coordinator_state: &CoordinatorState,
        transaction_state: &[TransactionState],
        project: &Project,
    ) -> Result<OrderedParallelProjection<Output>>
    where
        S: RangeSource,
        Project: for<'block> Fn(
            &mut T,
            &CoordinatorState,
            usize,
            BorrowedDecodedBlock<'block>,
            &[TransactionState],
        ) -> Result<Output>,
    {
        let block =
            archive.decode_uncompressed_block_borrowed(row, decompressed, discard_rewards)?;
        ensure_transaction_state_len(row, &block, transaction_state.len())?;
        let used_owned_schema_fallback = block.uses_owned_fallback();
        let output = project(
            &mut self.caller,
            coordinator_state,
            row_number,
            block,
            transaction_state,
        )?;
        Ok(OrderedParallelProjection {
            output,
            used_owned_schema_fallback,
        })
    }
}

struct OrderedParallelWorker<T> {
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
    decompressed: Vec<u8>,
    caller: T,
}

struct OrderedParallelProjection<T> {
    output: T,
    used_owned_schema_fallback: bool,
}

impl<T> OrderedParallelWorker<T> {
    #[allow(clippy::too_many_arguments)]
    fn decode_and_project<S, Output, Project>(
        &mut self,
        archive: &ArchiveReader<S>,
        row: ArchiveV2HotBlockIndexRow,
        compressed: &[u8],
        row_number: usize,
        discard_rewards: bool,
        retained_decompressed_bytes: usize,
        project: &Project,
    ) -> Result<OrderedParallelProjection<Output>>
    where
        S: RangeSource,
        Project: for<'block> Fn(&mut T, usize, BorrowedDecodedBlock<'block>) -> Result<Output>,
    {
        if self.decompressor.is_none() {
            self.decompressor =
                Some(
                    zstd::bulk::Decompressor::new().map_err(|error| Error::DecodeBlock {
                        slot: row.slot,
                        message: format!("create zstd decompressor: {error}"),
                    })?,
                );
        }
        if row.uncompressed_len as usize > retained_decompressed_bytes {
            // Keep the normal worker allocation intact when one admitted frame
            // is larger than its bounded retention limit.
            let mut oversized = Vec::new();
            Self::decode_and_project_in_buffer(
                archive,
                row,
                compressed,
                self.decompressor
                    .as_mut()
                    .expect("decompressor was initialized above"),
                &mut oversized,
                discard_rewards,
                &mut self.caller,
                row_number,
                project,
            )
        } else {
            let result = Self::decode_and_project_in_buffer(
                archive,
                row,
                compressed,
                self.decompressor
                    .as_mut()
                    .expect("decompressor was initialized above"),
                &mut self.decompressed,
                discard_rewards,
                &mut self.caller,
                row_number,
                project,
            );
            self.decompressed.clear();
            if self.decompressed.capacity() > retained_decompressed_bytes {
                self.decompressed.shrink_to(retained_decompressed_bytes);
            }
            result
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_and_project_in_buffer<S, Output, Project>(
        archive: &ArchiveReader<S>,
        row: ArchiveV2HotBlockIndexRow,
        compressed: &[u8],
        decompressor: &mut zstd::bulk::Decompressor<'static>,
        decompressed: &mut Vec<u8>,
        discard_rewards: bool,
        caller: &mut T,
        row_number: usize,
        project: &Project,
    ) -> Result<OrderedParallelProjection<Output>>
    where
        S: RangeSource,
        Project: for<'block> Fn(&mut T, usize, BorrowedDecodedBlock<'block>) -> Result<Output>,
    {
        let block = archive.decode_compressed_block_borrowed_reusing(
            row,
            compressed,
            decompressor,
            decompressed,
            discard_rewards,
        )?;
        let used_owned_schema_fallback = block.uses_owned_fallback();
        let output = project(caller, row_number, block)?;
        Ok(OrderedParallelProjection {
            output,
            used_owned_schema_fallback,
        })
    }
}

#[derive(Default)]
struct OrderedParallelCoordinator {
    stats: OrderedParallelBlockStats,
    error: Option<Error>,
    producer_disconnected: bool,
}

pub struct BlockIterator<'a, S> {
    archive: &'a ArchiveReader<S>,
    next: usize,
    end: usize,
    batch_first: usize,
    batch_end: usize,
    batch_offset: u64,
    batch: Vec<u8>,
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
    decompressed: Vec<u8>,
}

impl<S: RangeSource> Iterator for BlockIterator<'_, S> {
    type Item = Result<DecodedBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.end {
            return None;
        }
        if self.next < self.batch_first || self.next >= self.batch_end {
            match self.refill() {
                Ok(()) => {}
                Err(error) => {
                    self.next = self.end;
                    return Some(Err(error));
                }
            }
        }
        let row_number = self.next;
        self.next += 1;
        let row = self.archive.index.rows[row_number];
        let relative_offset = match row.compressed_offset.checked_sub(self.batch_offset) {
            Some(offset) => offset as usize,
            None => {
                return Some(Err(Error::InvalidIndex(
                    "prefetched block offset underflow".into(),
                )));
            }
        };
        let end = match relative_offset.checked_add(row.compressed_len as usize) {
            Some(end) => end,
            None => return Some(Err(Error::Overflow("prefetched block range"))),
        };
        let Some(compressed) = self.batch.get(relative_offset..end) else {
            return Some(Err(Error::InvalidIndex(
                "prefetched block range is outside batch".into(),
            )));
        };
        if self.decompressor.is_none() {
            match zstd::bulk::Decompressor::new() {
                Ok(decompressor) => self.decompressor = Some(decompressor),
                Err(error) => {
                    self.next = self.end;
                    return Some(Err(Error::DecodeBlock {
                        slot: row.slot,
                        message: format!("create zstd decompressor: {error}"),
                    }));
                }
            }
        }
        Some(
            self.archive.decode_compressed_block_reusing(
                row,
                compressed,
                self.decompressor
                    .as_mut()
                    .expect("decompressor was initialized above"),
                &mut self.decompressed,
            ),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end - self.next;
        (remaining, Some(remaining))
    }
}

impl<S: RangeSource> ExactSizeIterator for BlockIterator<'_, S> {}

impl<S: RangeSource> BlockIterator<'_, S> {
    fn refill(&mut self) -> Result<()> {
        self.batch_first = self.next;
        let first = self.archive.index.rows[self.next];
        let mut end = self.next + 1;
        let mut length = first.compressed_len as usize;
        while end < self.end {
            let next_length = self.archive.index.rows[end].compressed_len as usize;
            let Some(combined) = length.checked_add(next_length) else {
                break;
            };
            if combined > self.archive.options.prefetch_bytes {
                break;
            }
            length = combined;
            end += 1;
        }
        self.batch_offset = first.compressed_offset;
        self.archive.source.read_range_into(
            BLOCKS_FILE,
            self.batch_offset,
            length,
            &mut self.batch,
        )?;
        if self.batch.len() != length {
            return Err(Error::InvalidIndex(format!(
                "prefetched block range returned {} bytes, expected {length}",
                self.batch.len()
            )));
        }
        self.batch_end = end;
        Ok(())
    }
}

/// A lending block stream backed by reusable coalesced-read and decompression buffers.
///
/// This type intentionally does not implement [`Iterator`], because each returned block borrows
/// the decompressed bytes held by the stream.
pub struct BorrowedBlockStream<'a, S> {
    archive: &'a ArchiveReader<S>,
    discard_rewards: bool,
    next: usize,
    end: usize,
    batch_first: usize,
    batch_end: usize,
    batch_offset: u64,
    batch: Vec<u8>,
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
    decompressed: Vec<u8>,
}

impl<S: RangeSource> BorrowedBlockStream<'_, S> {
    /// Decode and lend the next block. The returned value must be dropped before this method can
    /// be called again, which makes reuse of the decompression buffer safe without self-references.
    pub fn next_block(&mut self) -> Option<Result<BorrowedDecodedBlock<'_>>> {
        if self.next == self.end {
            return None;
        }
        if self.next < self.batch_first || self.next >= self.batch_end {
            match self.refill() {
                Ok(()) => {}
                Err(error) => {
                    self.next = self.end;
                    return Some(Err(error));
                }
            }
        }
        let row_number = self.next;
        self.next += 1;
        let row = self.archive.index.rows[row_number];
        let relative_offset = match row.compressed_offset.checked_sub(self.batch_offset) {
            Some(offset) => offset as usize,
            None => {
                return Some(Err(Error::InvalidIndex(
                    "prefetched block offset underflow".into(),
                )));
            }
        };
        let end = match relative_offset.checked_add(row.compressed_len as usize) {
            Some(end) => end,
            None => return Some(Err(Error::Overflow("prefetched block range"))),
        };
        let Some(compressed) = self.batch.get(relative_offset..end) else {
            return Some(Err(Error::InvalidIndex(
                "prefetched block range is outside batch".into(),
            )));
        };
        if self.decompressor.is_none() {
            match zstd::bulk::Decompressor::new() {
                Ok(decompressor) => self.decompressor = Some(decompressor),
                Err(error) => {
                    self.next = self.end;
                    return Some(Err(Error::DecodeBlock {
                        slot: row.slot,
                        message: format!("create zstd decompressor: {error}"),
                    }));
                }
            }
        }
        Some(
            self.archive.decode_compressed_block_borrowed_reusing(
                row,
                compressed,
                self.decompressor
                    .as_mut()
                    .expect("decompressor was initialized above"),
                &mut self.decompressed,
                self.discard_rewards,
            ),
        )
    }

    /// Number of blocks not yet requested from the stream.
    pub fn len(&self) -> usize {
        self.end - self.next
    }

    pub fn is_empty(&self) -> bool {
        self.next == self.end
    }

    fn refill(&mut self) -> Result<()> {
        self.batch_first = self.next;
        let first = self.archive.index.rows[self.next];
        let mut end = self.next + 1;
        let mut length = first.compressed_len as usize;
        while end < self.end {
            let next_length = self.archive.index.rows[end].compressed_len as usize;
            let Some(combined) = length.checked_add(next_length) else {
                break;
            };
            if combined > self.archive.options.prefetch_bytes {
                break;
            }
            length = combined;
            end += 1;
        }
        self.batch_offset = first.compressed_offset;
        self.archive.source.read_range_into(
            BLOCKS_FILE,
            self.batch_offset,
            length,
            &mut self.batch,
        )?;
        if self.batch.len() != length {
            return Err(Error::InvalidIndex(format!(
                "prefetched block range returned {} bytes, expected {length}",
                self.batch.len()
            )));
        }
        self.batch_end = end;
        Ok(())
    }
}

pub struct ScanIterator<'a, S> {
    archive: &'a ArchiveReader<S>,
    filter: &'a CompiledPubkeyFilter,
    blocks: BlockIterator<'a, S>,
}

impl<S: RangeSource> Iterator for ScanIterator<'_, S> {
    type Item = Result<ScannedBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        self.blocks.next().map(|block| {
            block.and_then(|block| self.archive.scan_decoded_block(self.filter, block))
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.blocks.size_hint()
    }
}

impl<S: RangeSource> ExactSizeIterator for ScanIterator<'_, S> {}

pub fn validate_generation_structure<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
    options: &OpenOptions,
) -> Result<ValidatedGeneration> {
    validate_generation_structure_with_metadata_admission(
        source,
        manifest,
        options,
        ArchiveV2MetadataProfileAdmission::RequireCurrentTypedErrors,
    )
}

/// Validate structural bindings with an explicit metadata authority policy.
/// This does not perform the complete semantic scan required for publication.
pub fn validate_generation_structure_with_metadata_admission<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
    options: &OpenOptions,
    metadata_admission: ArchiveV2MetadataProfileAdmission,
) -> Result<ValidatedGeneration> {
    let wire_profile = ArchiveV2WireProfile::for_published_manifest(manifest)?;
    let metadata_wire_profile =
        ArchiveV2MetadataWireProfile::for_manifest(manifest, metadata_admission)?;
    validate_generation_structure_with_profiles(
        source,
        manifest,
        options,
        wire_profile,
        metadata_wire_profile,
    )
}

fn validate_generation_structure_with_profiles<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
    options: &OpenOptions,
    wire_profile: ArchiveV2WireProfile,
    _metadata_wire_profile: ArchiveV2MetadataWireProfile,
) -> Result<ValidatedGeneration> {
    validate_options(options)?;
    manifest.validate()?;
    if !manifest.complete {
        return Err(Error::IncompleteGeneration);
    }
    for required in REQUIRED_GENERATION_FILES {
        manifest.required_file(required)?;
    }
    validate_manifest_files(source, manifest, options)?;

    let registry_file = manifest.required_file(REGISTRY_FILE)?;
    if registry_file.size % 32 != 0 {
        return Err(Error::InvalidRegistry(format!(
            "registry.bin is {} bytes, not a multiple of 32",
            registry_file.size
        )));
    }
    let registry_entries_u64 = registry_file.size / 32;
    let registry_entries = u32::try_from(registry_entries_u64).map_err(|_| {
        Error::InvalidRegistry(format!(
            "registry has {registry_entries_u64} entries, exceeding the u32 id space"
        ))
    })?;

    let index_file = manifest.required_file(BLOCK_INDEX_FILE)?;
    let max_index_size = (ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64)
        .checked_add(
            manifest
                .slots_per_epoch
                .checked_mul(ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64)
                .ok_or(Error::Overflow("maximum block index size"))?,
        )
        .ok_or(Error::Overflow("maximum block index size"))?;
    if index_file.size > max_index_size {
        return Err(Error::InvalidIndex(format!(
            "index is {} bytes, above the epoch maximum {}",
            index_file.size, max_index_size
        )));
    }
    let index_length = usize::try_from(index_file.size)
        .map_err(|_| Error::InvalidIndex("index size exceeds usize".into()))?;
    let index_bytes = source.read_range(BLOCK_INDEX_FILE, 0, index_length)?;
    let blocks_size = manifest.required_file(BLOCKS_FILE)?.size;
    let (index, total_signatures) =
        parse_and_validate_index(&index_bytes, blocks_size, manifest, options)?;

    let signatures_available = if let Some(signatures) = manifest.file(SIGNATURES_FILE) {
        let expected = total_signatures
            .checked_mul(64)
            .ok_or(Error::Overflow("signature sidecar size"))?;
        if signatures.size != expected {
            return Err(Error::InvalidIndex(format!(
                "signatures.bin is {} bytes, expected {} for {} signatures",
                signatures.size, expected, total_signatures
            )));
        }
        true
    } else {
        false
    };

    let (metadata_footer, genesis) = validate_metadata(source, manifest, &index, options)?;
    let genesis_bin = validate_genesis_bin(source, manifest, genesis.as_ref())?;
    let binding = GenerationBinding {
        generation_digest: decode_sha256(&manifest.generation_digest)
            .map_err(Error::InvalidManifest)?,
        registry_sha256: decode_sha256(&registry_file.sha256).map_err(Error::InvalidManifest)?,
        wire_profile,
    };
    Ok(ValidatedGeneration {
        index,
        genesis,
        genesis_bin,
        metadata_footer,
        binding,
        registry_entries,
        total_signatures,
        signatures_available,
        wire_profile,
    })
}

fn validate_options(options: &OpenOptions) -> Result<()> {
    if options.io_chunk_size == 0
        || options.max_block_bytes == 0
        || options.max_compressed_frame_bytes == 0
        || options.max_meta_frame_bytes == 0
        || options.prefetch_bytes == 0
    {
        return Err(Error::InvalidManifest(
            "reader size limits must be non-zero".into(),
        ));
    }
    if options.prefetch_bytes > MAX_GATEWAY_RANGE_BYTES {
        return Err(Error::InvalidManifest(format!(
            "prefetch_bytes {} exceeds the gateway's {} byte range limit",
            options.prefetch_bytes, MAX_GATEWAY_RANGE_BYTES
        )));
    }
    if options.io_chunk_size > MAX_IO_CHUNK_SIZE
        || options.max_block_bytes > DEFAULT_MAX_BLOCK_BYTES
        || options.max_compressed_frame_bytes > DEFAULT_MAX_COMPRESSED_FRAME_BYTES
        || options.max_meta_frame_bytes > DEFAULT_MAX_META_FRAME_BYTES
    {
        return Err(Error::InvalidManifest(
            "reader size limits exceed the library hard maximum".into(),
        ));
    }
    Ok(())
}

fn validate_manifest_files<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
    options: &OpenOptions,
) -> Result<()> {
    for file in &manifest.files {
        let actual = source
            .size(&file.name)?
            .ok_or_else(|| Error::MissingFile(file.name.clone()))?;
        if actual != file.size {
            return Err(Error::FileSize {
                name: file.name.clone(),
                expected: file.size,
                actual,
            });
        }
        let verify_hash = match options.hash_verification {
            HashVerification::AllFiles => true,
            HashVerification::ControlFiles => {
                matches!(
                    file.name.as_str(),
                    BLOCK_INDEX_FILE
                        | META_FILE
                        | REGISTRY_FILE
                        | REGISTRY_INDEX_FILE
                        | GENESIS_BIN_FILE
                        | PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE
                        | POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE
                        | CURRENT_TYPED_ERRORS_MARKER_FILE
                )
            }
            HashVerification::SizesOnly => false,
        };
        if verify_hash {
            let actual_hash =
                hash_source_file(source, &file.name, file.size, options.io_chunk_size)?;
            if actual_hash != file.sha256 {
                return Err(Error::FileHash {
                    name: file.name.clone(),
                    expected: file.sha256.clone(),
                    actual: actual_hash,
                });
            }
        }
    }
    Ok(())
}

fn validate_genesis_bin<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
    inline: Option<&WincodeArchiveV2Genesis>,
) -> Result<Option<Vec<u8>>> {
    let Some(file) = manifest.file(GENESIS_BIN_FILE) else {
        return Ok(None);
    };
    if manifest.epoch != 0 {
        return Err(Error::InvalidMetadata(format!(
            "{GENESIS_BIN_FILE} is only valid for epoch 0"
        )));
    }
    let inline = inline.ok_or_else(|| {
        Error::InvalidMetadata(format!(
            "{GENESIS_BIN_FILE} is published without inline genesis metadata"
        ))
    })?;
    let length = usize::try_from(file.size)
        .map_err(|_| Error::InvalidMetadata("genesis.bin size exceeds usize".into()))?;
    if length > MAX_GENESIS_BIN_BYTES {
        return Err(Error::InvalidMetadata(format!(
            "{GENESIS_BIN_FILE} is {length} bytes, above the {MAX_GENESIS_BIN_BYTES} byte limit"
        )));
    }
    if file.size != inline.genesis_bin_len {
        return Err(Error::InvalidMetadata(format!(
            "{GENESIS_BIN_FILE} is {} bytes, inline genesis reports {}",
            file.size, inline.genesis_bin_len
        )));
    }
    let bytes = source.read_range(GENESIS_BIN_FILE, 0, length)?;
    let hash: [u8; 32] = Sha256::digest(&bytes).into();
    if hash != inline.genesis_hash {
        return Err(Error::InvalidMetadata(format!(
            "{GENESIS_BIN_FILE} hash does not match inline genesis metadata"
        )));
    }
    Ok(Some(bytes))
}

fn hash_source_file<S: RangeSource>(
    source: &S,
    name: &str,
    size: u64,
    chunk_size: usize,
) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut offset = 0u64;
    while offset < size {
        let length = usize::try_from((size - offset).min(chunk_size as u64))
            .expect("hash chunk is bounded by usize");
        hasher.update(source.read_range(name, offset, length)?);
        offset += length as u64;
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn parse_and_validate_index(
    bytes: &[u8],
    blocks_size: u64,
    manifest: &GenerationManifest,
    options: &OpenOptions,
) -> Result<(ArchiveV2HotBlockIndex, u64)> {
    if bytes.len() < ARCHIVE_V2_HOT_INDEX_HEADER_LEN {
        return Err(Error::InvalidIndex("index header is truncated".into()));
    }
    if &bytes[..8] != ARCHIVE_V2_HOT_INDEX_MAGIC {
        return Err(Error::InvalidIndex("bad index magic".into()));
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    if version != ARCHIVE_V2_HOT_INDEX_VERSION {
        return Err(Error::InvalidIndex(format!(
            "unsupported index version {version}"
        )));
    }
    if bytes[10..12] != [0, 0] {
        return Err(Error::InvalidIndex(
            "index header reserved bytes are non-zero".into(),
        ));
    }
    let row_count = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let blob_file_bytes = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
    let level = i32::from_le_bytes(bytes[28..32].try_into().unwrap());
    let flags = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
    let unsupported_flags =
        flags & (ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY | ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS);
    if unsupported_flags != 0 {
        return Err(Error::InvalidIndex(format!(
            "reader requires independent dictionary-free zstd frames; flags={flags:#x}"
        )));
    }
    if flags != 0 {
        return Err(Error::InvalidIndex(format!(
            "unknown index flags {flags:#x}"
        )));
    }
    if blob_file_bytes != blocks_size {
        return Err(Error::InvalidIndex(format!(
            "index declares {blob_file_bytes} block bytes, manifest declares {blocks_size}"
        )));
    }
    if row_count > manifest.slots_per_epoch {
        return Err(Error::InvalidIndex(format!(
            "index has {row_count} rows for {} epoch slots",
            manifest.slots_per_epoch
        )));
    }
    let expected_length = (ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64)
        .checked_add(
            row_count
                .checked_mul(ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64)
                .ok_or(Error::Overflow("block index rows"))?,
        )
        .ok_or(Error::Overflow("block index length"))?;
    if bytes.len() as u64 != expected_length {
        return Err(Error::InvalidIndex(format!(
            "index is {} bytes, expected {expected_length}",
            bytes.len()
        )));
    }

    let mut rows = Vec::with_capacity(row_count as usize);
    let mut expected_offset = 0u64;
    let mut expected_tx_ordinal = 0u64;
    let mut expected_signature_ordinal = 0u64;
    let mut previous_slot = None;
    for (number, row_bytes) in bytes[ARCHIVE_V2_HOT_INDEX_HEADER_LEN..]
        .chunks_exact(ARCHIVE_V2_HOT_INDEX_ROW_LEN)
        .enumerate()
    {
        let row = ArchiveV2HotBlockIndexRow {
            block_id: u32::from_le_bytes(row_bytes[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(row_bytes[4..12].try_into().unwrap()),
            compressed_offset: u64::from_le_bytes(row_bytes[12..20].try_into().unwrap()),
            compressed_len: u32::from_le_bytes(row_bytes[20..24].try_into().unwrap()),
            uncompressed_len: u32::from_le_bytes(row_bytes[24..28].try_into().unwrap()),
            tx_count: u32::from_le_bytes(row_bytes[28..32].try_into().unwrap()),
            first_tx_ordinal: u64::from_le_bytes(row_bytes[32..40].try_into().unwrap()),
            first_signature_ordinal: u64::from_le_bytes(row_bytes[40..48].try_into().unwrap()),
            signature_count: u32::from_le_bytes(row_bytes[48..52].try_into().unwrap()),
        };
        let expected_block_id = u32::try_from(number)
            .map_err(|_| Error::InvalidIndex("block id exceeds u32".into()))?;
        if row.block_id != expected_block_id {
            return Err(Error::InvalidIndex(format!(
                "row {number} has block_id {}, expected {expected_block_id}",
                row.block_id
            )));
        }
        if row.slot < manifest.epoch_start_slot() || row.slot > manifest.epoch_end_slot() {
            return Err(Error::InvalidIndex(format!(
                "slot {} is outside epoch {} range {}..={}",
                row.slot,
                manifest.epoch,
                manifest.epoch_start_slot(),
                manifest.epoch_end_slot()
            )));
        }
        if previous_slot.is_some_and(|slot| row.slot <= slot) {
            return Err(Error::InvalidIndex(format!(
                "slots are not strictly increasing at {}",
                row.slot
            )));
        }
        if row.compressed_len == 0 || row.uncompressed_len == 0 {
            return Err(Error::InvalidIndex(format!(
                "slot {} has an empty block frame",
                row.slot
            )));
        }
        if row.uncompressed_len as usize > options.max_block_bytes {
            return Err(Error::InvalidIndex(format!(
                "slot {} declares {} uncompressed bytes above the {} byte limit",
                row.slot, row.uncompressed_len, options.max_block_bytes
            )));
        }
        if row.compressed_len as usize > options.max_compressed_frame_bytes {
            return Err(Error::InvalidIndex(format!(
                "slot {} declares {} compressed bytes above the {} byte limit",
                row.slot, row.compressed_len, options.max_compressed_frame_bytes
            )));
        }
        if row.compressed_offset != expected_offset {
            return Err(Error::InvalidIndex(format!(
                "slot {} starts at {}, expected contiguous offset {}",
                row.slot, row.compressed_offset, expected_offset
            )));
        }
        expected_offset = expected_offset
            .checked_add(u64::from(row.compressed_len))
            .ok_or(Error::Overflow("compressed block range"))?;
        if expected_offset > blocks_size {
            return Err(Error::InvalidIndex(format!(
                "slot {} block range exceeds blocks file",
                row.slot
            )));
        }
        if row.first_tx_ordinal != expected_tx_ordinal {
            return Err(Error::InvalidIndex(format!(
                "slot {} first_tx_ordinal is {}, expected {}",
                row.slot, row.first_tx_ordinal, expected_tx_ordinal
            )));
        }
        expected_tx_ordinal = expected_tx_ordinal
            .checked_add(u64::from(row.tx_count))
            .ok_or(Error::Overflow("transaction ordinal"))?;
        if row.first_signature_ordinal != expected_signature_ordinal {
            return Err(Error::InvalidIndex(format!(
                "slot {} first_signature_ordinal is {}, expected {}",
                row.slot, row.first_signature_ordinal, expected_signature_ordinal
            )));
        }
        expected_signature_ordinal = expected_signature_ordinal
            .checked_add(u64::from(row.signature_count))
            .ok_or(Error::Overflow("signature ordinal"))?;
        previous_slot = Some(row.slot);
        rows.push(row);
    }
    if expected_offset != blocks_size {
        return Err(Error::InvalidIndex(format!(
            "indexed frames cover {expected_offset} bytes, blocks file has {blocks_size}"
        )));
    }
    Ok((
        ArchiveV2HotBlockIndex {
            blob_file_bytes,
            level,
            flags,
            rows,
        },
        expected_signature_ordinal,
    ))
}

fn validate_metadata<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
    index: &ArchiveV2HotBlockIndex,
    options: &OpenOptions,
) -> Result<(WincodeArchiveV2Footer, Option<WincodeArchiveV2Genesis>)> {
    let meta = manifest.required_file(META_FILE)?;
    let mut reader = RangeSourceReader::new(source, META_FILE, meta.size, options.io_chunk_size);
    let mut position = 0usize;
    let mut saw_genesis = false;
    let mut genesis = None;
    let mut footer = None;
    while let Some(mut frame) = read_frame(&mut reader, options.max_meta_frame_bytes)? {
        let record = decode_hot_metadata_record(&mut frame, manifest.epoch, position)?;
        if footer.is_some() {
            return Err(Error::InvalidMetadata(
                "metadata contains records after its footer".into(),
            ));
        }
        match (position, record) {
            (0, ArchiveV2HotMetaRecord::Header(header)) => {
                if header.version != WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION {
                    return Err(Error::InvalidMetadata(format!(
                        "unsupported hot-block metadata version {}",
                        header.version
                    )));
                }
                if header.flags & WINCODE_ARCHIVE_V2_FLAG_LEB128 == 0 {
                    return Err(Error::InvalidMetadata(
                        "metadata header does not declare LEB128 encoding".into(),
                    ));
                }
                if header.flags & WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY != 0 {
                    return Err(Error::InvalidMetadata(
                        "metadata describes a no-registry archive".into(),
                    ));
                }
                let known_flags = WINCODE_ARCHIVE_V2_FLAG_LEB128
                    | WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY
                    | WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY
                    | WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS;
                if header.flags & !known_flags != 0 {
                    return Err(Error::InvalidMetadata(format!(
                        "metadata header has unknown flags {:#x}",
                        header.flags & !known_flags
                    )));
                }
            }
            (0, _) => {
                return Err(Error::InvalidMetadata(
                    "metadata does not begin with a header".into(),
                ));
            }
            (_, ArchiveV2HotMetaRecord::Header(_)) => {
                return Err(Error::InvalidMetadata("duplicate metadata header".into()));
            }
            (_, ArchiveV2HotMetaRecord::Genesis(value)) => {
                if saw_genesis || manifest.epoch != 0 {
                    return Err(Error::InvalidMetadata(
                        "unexpected or duplicate genesis metadata".into(),
                    ));
                }
                saw_genesis = true;
                genesis = Some(value);
            }
            (_, ArchiveV2HotMetaRecord::Footer(value)) => footer = Some(value),
        }
        position += 1;
    }
    let footer =
        footer.ok_or_else(|| Error::InvalidMetadata("metadata does not end in a footer".into()))?;
    let transactions = index.rows.iter().try_fold(0u64, |total, row| {
        total
            .checked_add(u64::from(row.tx_count))
            .ok_or(Error::Overflow("metadata transaction total"))
    })?;
    if footer.blocks != index.rows.len() as u64 || footer.transactions != transactions {
        return Err(Error::InvalidMetadata(format!(
            "footer reports {} blocks/{} transactions; index reports {}/{}",
            footer.blocks,
            footer.transactions,
            index.rows.len(),
            transactions
        )));
    }
    Ok((footer, genesis))
}

fn decode_hot_metadata_record(
    frame: &mut [u8],
    epoch: u64,
    position: usize,
) -> Result<ArchiveV2HotMetaRecord> {
    // One historical epoch-0 Archive V2 generation encoded its Genesis record
    // with discriminant 4. Interpret that byte only in the one position where
    // Genesis is valid; unknown tags in every other context remain errors.
    if epoch == 0 && position == 1 && frame.first() == Some(&HISTORICAL_EPOCH0_HOT_META_GENESIS_TAG)
    {
        frame[0] = ARCHIVE_V2_HOT_META_GENESIS_TAG;
    }

    wincode::config::deserialize_exact(
        frame,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )
    .map_err(|error| Error::InvalidMetadata(format!("decode record {position}: {error}")))
}

fn read_frame(reader: &mut impl Read, max_length: usize) -> Result<Option<Vec<u8>>> {
    let Some(first) = read_byte(reader)? else {
        return Ok(None);
    };
    let mut length = u32::from(first & 0x7f);
    let mut byte = first;
    let mut shift = 7u32;
    while byte & 0x80 != 0 {
        if shift > 28 {
            return Err(Error::InvalidMetadata(
                "metadata frame length varint overflows u32".into(),
            ));
        }
        byte = read_byte(reader)?
            .ok_or_else(|| Error::InvalidMetadata("truncated metadata frame length".into()))?;
        if shift == 28 && byte & 0xf0 != 0 {
            return Err(Error::InvalidMetadata(
                "metadata frame length varint overflows u32".into(),
            ));
        }
        length |= u32::from(byte & 0x7f) << shift;
        shift += 7;
    }
    if shift > 7 && byte & 0x7f == 0 {
        return Err(Error::InvalidMetadata(
            "metadata frame length uses a non-minimal varint".into(),
        ));
    }
    let length = length as usize;
    if length > max_length {
        return Err(Error::InvalidMetadata(format!(
            "metadata frame is {length} bytes, above the {max_length} byte limit"
        )));
    }
    let mut bytes = vec![0u8; length];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| Error::InvalidMetadata(format!("truncated metadata frame: {error}")))?;
    Ok(Some(bytes))
}

fn read_byte(reader: &mut impl Read) -> Result<Option<u8>> {
    let mut byte = [0u8; 1];
    match reader.read(&mut byte) {
        Ok(0) => Ok(None),
        Ok(1) => Ok(Some(byte[0])),
        Ok(_) => unreachable!("one-byte buffer"),
        Err(error) => Err(Error::InvalidMetadata(format!("read metadata: {error}"))),
    }
}

fn validate_decoded_block(
    index: &ArchiveV2HotBlockIndexRow,
    block: &ArchiveV2HotBlockBlob,
) -> Result<()> {
    validate_decoded_block_parts(
        index,
        &block.header,
        block.tx_count,
        block.tx_rows.len(),
        block.tx_rows.iter().copied(),
        &block.message_bytes,
        &block.metadata_bytes,
    )
}

fn validate_borrowed_decoded_block(
    index: &ArchiveV2HotBlockIndexRow,
    block: &BorrowedDecodedBlockPayload<'_>,
) -> Result<()> {
    match block {
        BorrowedDecodedBlockPayload::Current(block) => validate_decoded_block_parts(
            index,
            &block.header,
            block.tx_count,
            block.tx_rows_len(),
            block.tx_rows(),
            block.message_bytes,
            block.metadata_bytes,
        ),
        BorrowedDecodedBlockPayload::CurrentWithoutRewards(block) => validate_decoded_block_parts(
            index,
            &block.header,
            block.tx_count,
            block.tx_rows_len(),
            block.tx_rows(),
            block.message_bytes,
            block.metadata_bytes,
        ),
        BorrowedDecodedBlockPayload::OwnedFallback(block) => validate_decoded_block(index, block),
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_decoded_block_parts<I>(
    index: &ArchiveV2HotBlockIndexRow,
    header: &ArchiveV2HotBlockHeader,
    tx_count: u32,
    tx_rows_len: usize,
    tx_rows: I,
    message_bytes: &[u8],
    metadata_bytes: &[u8],
) -> Result<()>
where
    I: ExactSizeIterator<Item = ArchiveV2HotTxRow> + Clone,
{
    let fail = |message: String| Error::InvalidBlock {
        slot: index.slot,
        message,
    };
    if header.slot != index.slot {
        return Err(fail(format!(
            "payload slot {} does not match index",
            header.slot
        )));
    }
    if tx_count != index.tx_count || tx_rows_len != index.tx_count as usize {
        return Err(fail(format!(
            "payload has tx_count {}/{} rows, index declares {}",
            tx_count, tx_rows_len, index.tx_count
        )));
    }
    let mut signatures = 0u32;
    let mut expected_message_offset = 0u32;
    let mut expected_metadata_offset = 0u32;
    // Current producers normally write canonical rows directly. Keep that hot
    // path identical to the original one-comparison-per-row validator. A rare
    // non-canonical block gets a separate exact-permutation pass below.
    let permutation_rows = tx_rows.clone();
    let mut tx_indexes_are_canonical = true;
    for (number, row) in tx_rows.enumerate() {
        if row.tx_index != number as u32 {
            tx_indexes_are_canonical = false;
        }
        if row.reserved != [0; 3] {
            return Err(fail(format!(
                "transaction {} has non-zero reserved bytes",
                row.tx_index
            )));
        }
        if row.flags & !KNOWN_HOT_TX_FLAGS != 0 {
            return Err(fail(format!(
                "transaction {} has unknown flags {:#x}",
                row.tx_index,
                row.flags & !KNOWN_HOT_TX_FLAGS
            )));
        }
        if row.message_len == 0 || row.message_offset != expected_message_offset {
            return Err(fail(format!(
                "transaction {} has an empty or non-contiguous message range",
                row.tx_index
            )));
        }
        checked_region(
            message_bytes,
            row.message_offset,
            row.message_len,
            "message",
            row.tx_index,
            index.slot,
        )?;
        expected_message_offset = row
            .message_offset
            .checked_add(row.message_len)
            .ok_or_else(|| fail("message offset overflow".into()))?;

        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            if row.metadata_len != 0 {
                return Err(fail(format!(
                    "transaction {} has metadata bytes without HAS_METADATA",
                    row.tx_index
                )));
            }
        } else {
            if row.metadata_len == 0 || row.metadata_offset != expected_metadata_offset {
                return Err(fail(format!(
                    "transaction {} has an empty or non-contiguous metadata range",
                    row.tx_index
                )));
            }
            checked_region(
                metadata_bytes,
                row.metadata_offset,
                row.metadata_len,
                "metadata",
                row.tx_index,
                index.slot,
            )?;
            expected_metadata_offset = row
                .metadata_offset
                .checked_add(row.metadata_len)
                .ok_or_else(|| fail("metadata offset overflow".into()))?;
        }
        signatures = signatures
            .checked_add(u32::from(row.signature_count))
            .ok_or_else(|| fail("signature count overflow".into()))?;
    }
    if !tx_indexes_are_canonical {
        let mut seen = vec![0u64; tx_rows_len.div_ceil(u64::BITS as usize)];
        for (number, row) in permutation_rows.enumerate() {
            let tx_index = row.tx_index as usize;
            if tx_index >= tx_rows_len {
                return Err(fail(format!(
                    "transaction row {number} has tx_index {}, outside 0..{tx_rows_len}",
                    row.tx_index,
                )));
            }
            if !mark_transaction_index_seen(&mut seen, tx_index) {
                return Err(fail(format!(
                    "transaction row {number} repeats tx_index {}",
                    row.tx_index,
                )));
            }
        }
    }
    if expected_message_offset as usize != message_bytes.len() {
        return Err(fail("message region has unindexed trailing bytes".into()));
    }
    if expected_metadata_offset as usize != metadata_bytes.len() {
        return Err(fail("metadata region has unindexed trailing bytes".into()));
    }
    if signatures != index.signature_count {
        return Err(fail(format!(
            "transaction rows report {signatures} signatures, index reports {}",
            index.signature_count
        )));
    }
    Ok(())
}

#[inline]
fn mark_transaction_index_seen(seen: &mut [u64], tx_index: usize) -> bool {
    let word = tx_index / u64::BITS as usize;
    let bit = 1u64 << (tx_index % u64::BITS as usize);
    let was_new = seen[word] & bit == 0;
    seen[word] |= bit;
    was_new
}

struct TransactionScanContext<'a> {
    filter: &'a CompiledPubkeyFilter,
    registry_entries: u32,
    message_projector: ArchiveV2MessageProjector,
    metadata_wire_profile: ArchiveV2MetadataWireProfile,
}

fn scan_transaction(
    slot: u64,
    row: ArchiveV2HotTxRow,
    block: &ArchiveV2HotBlockBlob,
    signatures: SignatureReference,
    context: &TransactionScanContext<'_>,
) -> Result<ScannedTransaction> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Ok(ScannedTransaction {
            slot,
            tx_index: row.tx_index,
            row,
            outcome: TransactionMatch::Indeterminate(IndeterminateReason::RawTransactionFallback),
            message: None,
            metadata: metadata_state(block, &row, slot, false, context.metadata_wire_profile)?,
            signatures,
        });
    }
    let message_bytes = checked_region(
        &block.message_bytes,
        row.message_offset,
        row.message_len,
        "message",
        row.tx_index,
        slot,
    )?;
    let message = context
        .message_projector
        .decode_owned_message(message_bytes)
        .map_err(|error| Error::InvalidBlock {
            slot,
            message: format!("decode message for tx {}: {error}", row.tx_index),
        })?;
    let is_v0 = matches!(message, ArchiveV2HotMessagePayload::V0(_));
    if is_v0 != (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0) {
        return Err(Error::InvalidBlock {
            slot,
            message: format!(
                "message version does not agree with flags for tx {}",
                row.tx_index
            ),
        });
    }
    let static_keys = match &message {
        ArchiveV2HotMessagePayload::Legacy(message) => message.account_keys.as_slice(),
        ArchiveV2HotMessagePayload::V0(message) => message.account_keys.as_slice(),
    };
    let static_result = evaluate_keys(static_keys, context.filter, context.registry_entries);
    let needs_loaded = is_v0 && row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0;
    let read_metadata = static_result.matched || needs_loaded;
    let metadata = metadata_state(
        block,
        &row,
        slot,
        read_metadata,
        context.metadata_wire_profile,
    )?;

    let mut loaded_result = KeyEvaluation::default();
    let loaded_unavailable = if needs_loaded {
        match &metadata {
            MetadataState::Decoded(metadata) => {
                loaded_result = evaluate_keys(
                    metadata
                        .loaded_writable_addresses
                        .iter()
                        .chain(&metadata.loaded_readonly_addresses),
                    context.filter,
                    context.registry_entries,
                );
                false
            }
            MetadataState::RawFallback | MetadataState::Absent | MetadataState::NotRead => true,
        }
    } else {
        false
    };

    let outcome = if static_result.matched || loaded_result.matched {
        TransactionMatch::Match {
            static_account: static_result.matched,
            loaded_address: loaded_result.matched,
        }
    } else if loaded_unavailable {
        TransactionMatch::Indeterminate(IndeterminateReason::V0LoadedAddressesUnavailable)
    } else if static_result.invalid || loaded_result.invalid {
        TransactionMatch::Indeterminate(IndeterminateReason::InvalidRegistryReference)
    } else {
        TransactionMatch::NoMatch
    };

    Ok(ScannedTransaction {
        slot,
        tx_index: row.tx_index,
        row,
        outcome,
        message: Some(message),
        metadata,
        signatures,
    })
}

fn metadata_state(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
    slot: u64,
    read: bool,
    metadata_wire_profile: ArchiveV2MetadataWireProfile,
) -> Result<MetadataState> {
    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 || row.metadata_len == 0 {
        return Ok(MetadataState::Absent);
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return Ok(MetadataState::RawFallback);
    }
    if !read {
        return Ok(MetadataState::NotRead);
    }
    let bytes = checked_region(
        &block.metadata_bytes,
        row.metadata_offset,
        row.metadata_len,
        "metadata",
        row.tx_index,
        slot,
    )?;
    let canonical = match metadata_wire_profile {
        ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1 => None,
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility
            if bytes.first() == Some(&0) =>
        {
            None
        }
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility => Some(
            canonicalize_archive_v2_metadata_owned(bytes)
                .map_err(|error| Error::InvalidBlock {
                    slot,
                    message: format!("select metadata schema for tx {}: {error}", row.tx_index),
                })?
                .0,
        ),
    };
    let decode_bytes = canonical.as_deref().unwrap_or(bytes);
    let metadata = wincode::config::deserialize_exact(
        decode_bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )
    .map_err(|error| Error::InvalidBlock {
        slot,
        message: format!("decode metadata for tx {}: {error}", row.tx_index),
    })?;
    Ok(MetadataState::Decoded(Box::new(metadata)))
}

#[derive(Default)]
struct KeyEvaluation {
    matched: bool,
    invalid: bool,
}

fn evaluate_keys<'a>(
    keys: impl IntoIterator<Item = &'a CompactPubkey>,
    filter: &CompiledPubkeyFilter,
    registry_entries: u32,
) -> KeyEvaluation {
    let mut result = KeyEvaluation::default();
    for key in keys {
        match key {
            CompactPubkey::Id(id) => {
                if *id == 0 || *id > registry_entries {
                    result.invalid = true;
                } else if filter.registry_ids.contains(id) {
                    result.matched = true;
                }
            }
            CompactPubkey::Raw(pubkey) => {
                if filter.raw_pubkeys.contains(pubkey) {
                    result.matched = true;
                }
            }
        }
    }
    result
}

fn checked_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    length: u32,
    kind: &str,
    tx_index: u32,
    slot: u64,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let end = start
        .checked_add(length as usize)
        .ok_or_else(|| Error::InvalidBlock {
            slot,
            message: format!("{kind} range overflow for tx {tx_index}"),
        })?;
    bytes.get(start..end).ok_or_else(|| Error::InvalidBlock {
        slot,
        message: format!(
            "{kind} range {start}..{end} is outside {} bytes for tx {tx_index}",
            bytes.len()
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::Path,
        sync::{Arc, Barrier, Mutex},
    };

    use blockzilla_format::{
        ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ArchiveV2ComputeBudgetInstructionData,
        ArchiveV2HotBlockHeader, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
        ArchiveV2HotRewards, ArchiveV2HotV0Message, ArchiveV2SystemInstructionData,
        CompactMessageHeader, CompactReward, CompactShredding, CompactTransactionError,
        OwnedCompactRecentBlockhash, WincodeArchiveV2Header, wincode_leb128_config,
        write_archive_v2_hot_block_index,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{
        LocalRangeSource, SourceError, SourceResult,
        manifest::{GenerationFile, compute_generation_digest},
    };

    const EPOCH: u64 = 1;
    const SLOTS_PER_EPOCH: u64 = 100;
    const RAW_KEY: [u8; 32] = [3; 32];
    const REGISTRY_KEY_ONE: [u8; 32] = [1; 32];
    const REGISTRY_KEY_TWO: [u8; 32] = [2; 32];

    struct Fixture {
        directory: TempDir,
    }

    impl Fixture {
        fn build() -> Self {
            Self::build_with_first_storage_tx_indexes([0, 1])
        }

        fn build_with_first_storage_tx_indexes(first_storage_tx_indexes: [u32; 2]) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            fs::write(
                root.join(REGISTRY_FILE),
                [REGISTRY_KEY_ONE.as_slice(), REGISTRY_KEY_TWO.as_slice()].concat(),
            )
            .unwrap();

            let first_message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: message_header(),
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw(RAW_KEY)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: Vec::new(),
            });
            let first_message =
                wincode::config::serialize(&first_message, wincode_leb128_config()).unwrap();
            let first_messages = [first_message.as_slice(), first_message.as_slice()].concat();
            let first_block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot: 101,
                    parent_slot: 100,
                    blockhash_id: 1,
                    previous_blockhash_id: 0,
                    block_time: Some(1_700_000_001),
                    block_height: Some(1000),
                    rewards: Some(ArchiveV2HotRewards {
                        num_partitions: Some(2),
                        decoded: vec![CompactReward {
                            pubkey: CompactPubkey::Raw([91; 32]),
                            lamports: -17,
                            post_balance: 90_000,
                            reward_type: 3,
                            commission: Some(7),
                        }],
                    }),
                },
                tx_count: 2,
                tx_rows: vec![
                    ArchiveV2HotTxRow {
                        tx_index: first_storage_tx_indexes[0],
                        flags: 0,
                        message_offset: 0,
                        message_len: first_message.len() as u32,
                        metadata_offset: 0,
                        metadata_len: 0,
                        signature_count: 2,
                        reserved: [0; 3],
                    },
                    ArchiveV2HotTxRow {
                        tx_index: first_storage_tx_indexes[1],
                        flags: 0,
                        message_offset: first_message.len() as u32,
                        message_len: first_message.len() as u32,
                        metadata_offset: 0,
                        metadata_len: 0,
                        signature_count: 1,
                        reserved: [0; 3],
                    },
                ],
                message_bytes: first_messages,
                metadata_bytes: Vec::new(),
            };

            let second_message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header: message_header(),
                account_keys: vec![CompactPubkey::Id(1)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(1),
                instructions: Vec::new(),
                address_table_lookups: Vec::new(),
            });
            let second_message =
                wincode::config::serialize(&second_message, wincode_leb128_config()).unwrap();
            let second_metadata = CompactMetaV1 {
                err: None,
                fee: 5000,
                pre_balances: Vec::new(),
                post_balances: Vec::new(),
                inner_instructions: None,
                logs: None,
                pre_token_balances: Vec::new(),
                post_token_balances: Vec::new(),
                rewards: Vec::new(),
                loaded_writable_addresses: vec![CompactPubkey::Id(2)],
                loaded_readonly_addresses: Vec::new(),
                return_data: None,
                compute_units_consumed: Some(42),
                cost_units: None,
            };
            let second_metadata =
                wincode::config::serialize(&second_metadata, wincode_leb128_config()).unwrap();
            let second_block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot: 102,
                    parent_slot: 101,
                    blockhash_id: 2,
                    previous_blockhash_id: 1,
                    block_time: Some(1_700_000_002),
                    block_height: Some(1001),
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                        | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                    message_offset: 0,
                    message_len: second_message.len() as u32,
                    metadata_offset: 0,
                    metadata_len: second_metadata.len() as u32,
                    signature_count: 1,
                    reserved: [0; 3],
                }],
                message_bytes: second_message,
                metadata_bytes: second_metadata,
            };

            let first_uncompressed =
                wincode::config::serialize(&first_block, wincode_leb128_config()).unwrap();
            let second_uncompressed =
                wincode::config::serialize(&second_block, wincode_leb128_config()).unwrap();
            let first_compressed = zstd::bulk::compress(&first_uncompressed, 3).unwrap();
            let second_compressed = zstd::bulk::compress(&second_uncompressed, 3).unwrap();
            let blocks = [first_compressed.as_slice(), second_compressed.as_slice()].concat();
            fs::write(root.join(BLOCKS_FILE), &blocks).unwrap();

            let rows = vec![
                ArchiveV2HotBlockIndexRow {
                    block_id: 0,
                    slot: 101,
                    compressed_offset: 0,
                    compressed_len: first_compressed.len() as u32,
                    uncompressed_len: first_uncompressed.len() as u32,
                    tx_count: 2,
                    first_tx_ordinal: 0,
                    first_signature_ordinal: 0,
                    signature_count: 3,
                },
                ArchiveV2HotBlockIndexRow {
                    block_id: 1,
                    slot: 102,
                    compressed_offset: first_compressed.len() as u64,
                    compressed_len: second_compressed.len() as u32,
                    uncompressed_len: second_uncompressed.len() as u32,
                    tx_count: 1,
                    first_tx_ordinal: 2,
                    first_signature_ordinal: 3,
                    signature_count: 1,
                },
            ];
            write_archive_v2_hot_block_index(
                &root.join(BLOCK_INDEX_FILE),
                blocks.len() as u64,
                3,
                0,
                &rows,
            )
            .unwrap();
            fs::write(
                root.join(SIGNATURES_FILE),
                [
                    [7u8; 64].as_slice(),
                    [70u8; 64].as_slice(),
                    [9u8; 64].as_slice(),
                    [8u8; 64].as_slice(),
                ]
                .concat(),
            )
            .unwrap();

            let metadata_records = [
                ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                    version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                    flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
                }),
                ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                    blocks: 2,
                    transactions: 3,
                    ..WincodeArchiveV2Footer::default()
                }),
            ];
            let mut metadata = Vec::new();
            for record in metadata_records {
                let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
                write_u32_varint(&mut metadata, bytes.len() as u32);
                metadata.extend_from_slice(&bytes);
            }
            fs::write(root.join(META_FILE), metadata).unwrap();

            write_manifest(root, true, None);
            Self { directory }
        }

        fn source(&self) -> LocalRangeSource {
            LocalRangeSource::new(self.directory.path())
        }
    }

    #[derive(Clone)]
    struct CountingSource {
        inner: LocalRangeSource,
        reads: Arc<Mutex<Vec<(String, u64, usize)>>>,
    }

    impl CountingSource {
        fn new(inner: LocalRangeSource) -> Self {
            Self {
                inner,
                reads: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn clear(&self) {
            self.reads.lock().unwrap().clear();
        }

        fn reads_for(&self, object: &str) -> Vec<(u64, usize)> {
            self.reads
                .lock()
                .unwrap()
                .iter()
                .filter(|(name, _, _)| name == object)
                .map(|(_, offset, length)| (*offset, *length))
                .collect()
        }
    }

    impl RangeSource for CountingSource {
        fn size(&self, object: &str) -> SourceResult<Option<u64>> {
            self.inner.size(object)
        }

        fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
            self.reads
                .lock()
                .unwrap()
                .push((object.to_owned(), offset, length));
            self.inner.read_range(object, offset, length)
        }
    }

    #[derive(Clone)]
    struct FailingBlocksSource {
        inner: LocalRangeSource,
    }

    impl RangeSource for FailingBlocksSource {
        fn size(&self, object: &str) -> SourceResult<Option<u64>> {
            self.inner.size(object)
        }

        fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
            if object == BLOCKS_FILE {
                return Err(SourceError::Protocol(
                    "injected ordered block read failure".into(),
                ));
            }
            self.inner.read_range(object, offset, length)
        }
    }

    #[test]
    fn open_trusted_opens_without_a_published_manifest() {
        let fixture = Fixture::build();
        fs::remove_file(fixture.directory.path().join(GENERATION_MANIFEST_FILE)).unwrap();

        let identity = crate::manifest::TrustedGenerationIdentity {
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "trusted-fixture".into(),
            slots_per_epoch: SLOTS_PER_EPOCH,
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        };
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_trusted(fixture.source(), identity, options).unwrap();
        assert_eq!(archive.index().rows.len(), 2);
        assert_eq!(archive.manifest().epoch, EPOCH);
        assert_eq!(
            archive.metadata_wire_profile(),
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility
        );
        assert_eq!(
            archive.profiled_binding().metadata_wire_profile,
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility
        );
        assert_eq!(
            archive.binding().wire_profile,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        );

        let raw_filter = archive.compile_pubkey_filter([RAW_KEY]).unwrap();
        let first = archive.scan(&raw_filter).unwrap().next().unwrap().unwrap();
        assert_eq!(
            first.transactions[0].outcome,
            TransactionMatch::Match {
                static_account: true,
                loaded_address: false,
            }
        );
    }

    #[test]
    fn reader_bound_metadata_profile_controls_legacy_error_decoding() {
        let fixture = Fixture::build();
        let strict = ArchiveReader::open(fixture.source()).unwrap();
        let compatible = ArchiveReader::open_trusted(
            fixture.source(),
            crate::manifest::TrustedGenerationIdentity {
                cluster_id: "testnet".into(),
                epoch: EPOCH,
                generation_id: "trusted-metadata-compatibility".into(),
                slots_per_epoch: SLOTS_PER_EPOCH,
                wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            },
            OpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap();

        let current = CompactMetaV1 {
            err: Some(CompactTransactionError::AccountInUse),
            fee: 5_000,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        };
        let current = wincode::config::serialize(&current, wincode_leb128_config()).unwrap();
        assert_eq!(&current[..2], &[1, 0]);
        let mut legacy = Vec::with_capacity(current.len() + 4);
        legacy.extend_from_slice(&[1, 4, 0, 0, 0, 0]);
        legacy.extend_from_slice(&current[2..]);
        assert_eq!(
            crate::classify_archive_v2_metadata_schema_exact(&legacy),
            crate::ArchiveV2MetadataSchemaClassification::LegacyOnly
        );
        let limits = ArchiveV2MetadataProjectionLimits {
            total_message_accounts: 0,
            top_level_instruction_count: 0,
        };

        assert!(strict.validate_metadata_exact(&legacy, limits).is_err());
        assert!(compatible.validate_metadata_exact(&legacy, limits).is_ok());
    }

    #[test]
    fn published_unmarked_metadata_requires_explicit_compatibility() {
        let fixture = Fixture::build();
        let manifest_path = fixture.directory.path().join(GENERATION_MANIFEST_FILE);
        let mut manifest = GenerationManifest::parse(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest
            .files
            .retain(|file| file.name != CURRENT_TYPED_ERRORS_MARKER_FILE);
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        fs::remove_file(
            fixture
                .directory
                .path()
                .join(CURRENT_TYPED_ERRORS_MARKER_FILE),
        )
        .unwrap();

        assert!(ArchiveReader::open(fixture.source()).is_err());
        let compatible = ArchiveReader::open_with_metadata_admission(
            fixture.source(),
            ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
        )
        .unwrap();
        assert_eq!(
            compatible.metadata_wire_profile(),
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility
        );
    }

    #[test]
    fn open_trusted_binds_the_explicit_historical_wire_profile() {
        let fixture = Fixture::build();
        fs::remove_file(fixture.directory.path().join(GENERATION_MANIFEST_FILE)).unwrap();
        let identity = crate::manifest::TrustedGenerationIdentity {
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "trusted-historical-fixture".into(),
            slots_per_epoch: SLOTS_PER_EPOCH,
            wire_profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        };
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_trusted(fixture.source(), identity, options).unwrap();
        assert_eq!(
            archive.wire_profile(),
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        );
        assert_eq!(
            archive.binding().wire_profile,
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        );
        assert_eq!(
            archive.message_projector().wire_profile(),
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        );
    }

    #[test]
    fn open_trusted_binds_additional_file_sizes_and_wire_profile() {
        let fixture = Fixture::build();
        fs::remove_file(fixture.directory.path().join(GENERATION_MANIFEST_FILE)).unwrap();
        fs::write(
            fixture.directory.path().join("required-sidecar.bin"),
            [1, 2, 3],
        )
        .unwrap();

        let identity = |wire_profile| crate::manifest::TrustedGenerationIdentity {
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "trusted-additional-files".into(),
            slots_per_epoch: SLOTS_PER_EPOCH,
            wire_profile,
        };
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let post = ArchiveReader::open_trusted_with_additional_files(
            fixture.source(),
            identity(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1),
            &[SIGNATURES_FILE, "required-sidecar.bin"],
            &["absent-optional.bin"],
            options.clone(),
        )
        .unwrap();
        assert_eq!(
            post.manifest()
                .required_file("required-sidecar.bin")
                .unwrap()
                .size,
            3
        );
        assert!(post.manifest().file("absent-optional.bin").is_none());
        let current_metadata =
            ArchiveReader::open_trusted_with_additional_files_and_metadata_profile(
                fixture.source(),
                identity(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1),
                &[SIGNATURES_FILE, "required-sidecar.bin"],
                &["absent-optional.bin"],
                ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1,
                options.clone(),
            )
            .unwrap();
        assert_ne!(
            post.binding().generation_digest,
            current_metadata.binding().generation_digest
        );
        assert_eq!(
            current_metadata.profiled_binding().metadata_wire_profile,
            ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1
        );

        let pre = ArchiveReader::open_trusted_with_additional_files(
            fixture.source(),
            identity(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1),
            &[SIGNATURES_FILE, "required-sidecar.bin"],
            &[],
            options,
        )
        .unwrap();
        assert_ne!(
            post.binding().generation_digest,
            pre.binding().generation_digest
        );

        fs::write(
            fixture.directory.path().join("required-sidecar.bin"),
            [1, 2, 3, 4],
        )
        .unwrap();
        let resized = ArchiveReader::open_trusted_with_additional_files(
            fixture.source(),
            identity(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1),
            &[SIGNATURES_FILE, "required-sidecar.bin"],
            &[],
            OpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap();
        assert_ne!(
            post.binding().generation_digest,
            resized.binding().generation_digest
        );
    }

    #[test]
    fn open_trusted_rejects_a_missing_required_additional_file() {
        let fixture = Fixture::build();
        fs::remove_file(fixture.directory.path().join(GENERATION_MANIFEST_FILE)).unwrap();
        let identity = crate::manifest::TrustedGenerationIdentity {
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "trusted-missing-file".into(),
            slots_per_epoch: SLOTS_PER_EPOCH,
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        };
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let error = ArchiveReader::open_trusted_with_additional_files(
            fixture.source(),
            identity,
            &["missing-sidecar.bin"],
            &[],
            options,
        )
        .unwrap_err();
        assert!(matches!(error, Error::MissingFile(name) if name == "missing-sidecar.bin"));
    }

    #[test]
    fn trusted_profile_cannot_synthesize_a_fixed_metadata_marker_binding() {
        let fixture = Fixture::build();
        let error = ArchiveReader::open_trusted_with_additional_files_and_metadata_profile(
            fixture.source(),
            crate::manifest::TrustedGenerationIdentity {
                cluster_id: "testnet".into(),
                epoch: EPOCH,
                generation_id: "trusted-no-synthetic-marker".into(),
                slots_per_epoch: SLOTS_PER_EPOCH,
                wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            },
            &[CURRENT_TYPED_ERRORS_MARKER_FILE],
            &[],
            ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1,
            OpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap_err();
        assert!(matches!(error, Error::InvalidManifest(message) if message.contains("synthetic")));
    }

    #[derive(wincode::SchemaWrite)]
    enum HistoricalMessagePayload {
        Legacy(HistoricalLegacyMessage),
    }

    #[derive(wincode::SchemaWrite)]
    struct HistoricalLegacyMessage {
        header: CompactMessageHeader,
        account_keys: Vec<CompactPubkey>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: Vec<HistoricalInstruction>,
    }

    #[derive(wincode::SchemaWrite)]
    struct HistoricalInstruction {
        program_id_index: u8,
        accounts: Vec<u8>,
        data: HistoricalInstructionData,
    }

    #[derive(wincode::SchemaWrite)]
    #[allow(dead_code)]
    enum HistoricalInstructionData {
        Raw(Vec<u8>),
        ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
        System(ArchiveV2SystemInstructionData),
    }

    #[test]
    fn high_level_scan_uses_the_reader_historical_message_profile() {
        let fixture = Fixture::build();
        fs::remove_file(fixture.directory.path().join(GENERATION_MANIFEST_FILE)).unwrap();
        let identity = crate::manifest::TrustedGenerationIdentity {
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "trusted-historical-scan".into(),
            slots_per_epoch: SLOTS_PER_EPOCH,
            wire_profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        };
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_trusted(fixture.source(), identity, options).unwrap();
        let historical = HistoricalMessagePayload::Legacy(HistoricalLegacyMessage {
            header: message_header(),
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw(RAW_KEY)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![HistoricalInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: HistoricalInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                    lamports: 9,
                }),
            }],
        });
        let message = wincode::config::serialize(&historical, wincode_leb128_config()).unwrap();
        let row = ArchiveV2HotTxRow {
            tx_index: 0,
            flags: 0,
            message_offset: 0,
            message_len: message.len() as u32,
            metadata_offset: 0,
            metadata_len: 0,
            signature_count: 1,
            reserved: [0; 3],
        };
        let decoded = DecodedBlock {
            index_row: ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 101,
                compressed_offset: 0,
                compressed_len: 1,
                uncompressed_len: 1,
                tx_count: 1,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 1,
            },
            block: ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot: 101,
                    parent_slot: 100,
                    blockhash_id: 1,
                    previous_blockhash_id: 0,
                    block_time: None,
                    block_height: None,
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![row],
                message_bytes: message,
                metadata_bytes: vec![],
            },
        };
        let filter = archive.compile_pubkey_filter([RAW_KEY]).unwrap();
        let scanned = archive.scan_decoded_block(&filter, decoded).unwrap();
        assert!(matches!(
            scanned.transactions[0].outcome,
            TransactionMatch::Match {
                static_account: true,
                loaded_address: false
            }
        ));
        let Some(ArchiveV2HotMessagePayload::Legacy(message)) =
            scanned.transactions[0].message.as_ref()
        else {
            panic!("expected normalized historical legacy message");
        };
        assert!(matches!(
            message.instructions[0].data,
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                lamports: 9
            })
        ));
    }

    #[test]
    fn open_trusted_rejects_non_sizes_only_verification() {
        let fixture = Fixture::build();
        fs::remove_file(fixture.directory.path().join(GENERATION_MANIFEST_FILE)).unwrap();

        let identity = crate::manifest::TrustedGenerationIdentity {
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "trusted-fixture".into(),
            slots_per_epoch: SLOTS_PER_EPOCH,
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        };
        let error = ArchiveReader::open_trusted(fixture.source(), identity, OpenOptions::default())
            .unwrap_err();
        assert!(matches!(error, Error::InvalidManifest(_)));
    }

    #[test]
    fn strict_reader_matches_registry_raw_and_v0_loaded_pubkeys_and_reads_signatures() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        assert_eq!(archive.index().rows.len(), 2);
        assert_eq!(archive.metadata_footer().transactions, 3);

        let raw_filter = archive.compile_pubkey_filter([RAW_KEY]).unwrap();
        let first = archive.scan(&raw_filter).unwrap().next().unwrap().unwrap();
        assert_eq!(
            first.transactions[0].outcome,
            TransactionMatch::Match {
                static_account: true,
                loaded_address: false
            }
        );
        assert_eq!(
            archive
                .read_transaction_signatures(first.transactions[0].signatures)
                .unwrap(),
            vec![[7u8; 64], [70u8; 64]]
        );
        assert_eq!(first.transactions[1].signatures.first_ordinal, 2);
        assert_eq!(
            archive
                .read_transaction_signatures(first.transactions[1].signatures)
                .unwrap(),
            vec![[9u8; 64]]
        );

        let loaded_filter = archive.compile_pubkey_filter([REGISTRY_KEY_TWO]).unwrap();
        let blocks: Vec<_> = archive
            .scan(&loaded_filter)
            .unwrap()
            .map(|block| block.unwrap())
            .collect();
        assert_eq!(blocks[0].transactions[0].outcome, TransactionMatch::NoMatch);
        assert_eq!(
            blocks[1].transactions[0].outcome,
            TransactionMatch::Match {
                static_account: false,
                loaded_address: true
            }
        );
        assert!(matches!(
            blocks[1].transactions[0].metadata,
            MetadataState::Decoded(_)
        ));
        assert_eq!(archive.read_signature_ordinal(3).unwrap(), [8u8; 64]);
    }

    #[test]
    fn sequential_iterator_coalesces_adjacent_frames_into_one_range_read() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();
        let blocks: Vec<_> = archive.blocks().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(blocks.len(), 2);
        let reads = source.reads_for(BLOCKS_FILE);
        assert_eq!(reads.len(), 1, "reads were {reads:?}");
        assert_eq!(reads[0].0, 0);
        assert_eq!(reads[0].1 as u64, archive.index().blob_file_bytes);
    }

    #[test]
    fn ordered_parallel_pipeline_reads_once_and_publishes_row_order() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();

        let completion_barrier = Arc::new(Barrier::new(2));
        let completed = Arc::new(Mutex::new(Vec::new()));
        let consumed = Arc::new(Mutex::new(Vec::new()));
        let project_barrier = Arc::clone(&completion_barrier);
        let project_completed = Arc::clone(&completed);
        let ordered_consumed = Arc::clone(&consumed);
        let stats = archive
            .process_borrowed_blocks_parallel_ordered(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    discard_rewards: true,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| (),
                move |_, row_number, block| {
                    project_barrier.wait();
                    if row_number == 0 {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    project_completed.lock().unwrap().push(row_number);
                    assert!(!block.uses_owned_fallback());
                    Ok(block.header().slot)
                },
                move |row_number, slot| {
                    ordered_consumed.lock().unwrap().push((row_number, slot));
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(*completed.lock().unwrap(), vec![1, 0]);
        assert_eq!(*consumed.lock().unwrap(), vec![(0, 101), (1, 102)]);
        assert_eq!(stats.block_count, 2);
        assert_eq!(stats.borrowed_storage_blocks, 2);
        assert_eq!(stats.owned_schema_fallback_blocks, 0);
        assert_eq!(stats.batch_count, 1);
        assert_eq!(stats.read_call_count, 1);
        assert_eq!(stats.compressed_bytes, archive.index().blob_file_bytes);
        assert_eq!(
            stats.max_compressed_batch_bytes as u64,
            archive.index().blob_file_bytes
        );
        assert_eq!(
            stats.max_declared_uncompressed_batch_bytes,
            archive
                .index()
                .rows
                .iter()
                .map(|row| u64::from(row.uncompressed_len))
                .sum::<u64>()
        );
        assert_eq!(
            source.reads_for(BLOCKS_FILE),
            vec![(0, archive.index().blob_file_bytes as usize)]
        );
    }

    #[derive(Debug, Default)]
    struct BatchBarrierTestState {
        stage_a_pointers: Vec<Option<usize>>,
        merged_rows: Vec<usize>,
        finished_batches: Vec<Range<usize>>,
    }

    #[test]
    fn batch_barrier_reads_and_decompresses_once_then_reuses_the_same_bytes() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();

        let mut coordinator = BatchBarrierTestState {
            stage_a_pointers: vec![None; archive.index().rows.len()],
            ..BatchBarrierTestState::default()
        };
        let mut consumed = Vec::new();
        let stats = archive
            .process_borrowed_blocks_parallel_batch_barrier(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    discard_rewards: true,
                    ..OrderedParallelBlockConfig::default()
                },
                &mut coordinator,
                |_| (),
                |_, row_number, block| {
                    Ok((
                        block.header().slot,
                        block.uncompressed_bytes().as_ptr() as usize,
                        row_number,
                    ))
                },
                |state, row_number, (slot, pointer, projected_row)| {
                    assert_eq!(projected_row, row_number);
                    assert_eq!(slot, 101 + row_number as u64);
                    state.stage_a_pointers[row_number] = Some(pointer);
                    state.merged_rows.push(row_number);
                    Ok(())
                },
                |state, rows| {
                    assert_eq!(state.merged_rows, vec![0, 1]);
                    state.finished_batches.push(rows);
                    Ok(())
                },
                |_, state, row_number, block| {
                    assert_eq!(state.merged_rows, vec![0, 1]);
                    assert_eq!(state.finished_batches, vec![0..2]);
                    assert_eq!(
                        state.stage_a_pointers[row_number],
                        Some(block.uncompressed_bytes().as_ptr() as usize),
                    );
                    Ok(block.header().slot)
                },
                |_, row_number, slot| {
                    consumed.push((row_number, slot));
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(consumed, vec![(0, 101), (1, 102)]);
        assert_eq!(coordinator.merged_rows, vec![0, 1]);
        assert_eq!(coordinator.finished_batches, vec![0..2]);
        assert_eq!(stats.block_count, 2);
        assert_eq!(stats.batch_count, 1);
        assert_eq!(stats.read_call_count, 1);
        assert_eq!(stats.decompression_count, 2);
        assert_eq!(stats.stage_a_block_count, 2);
        assert_eq!(stats.stage_b_block_count, 2);
        assert_eq!(stats.borrowed_storage_blocks, 2);
        assert_eq!(stats.owned_schema_fallback_blocks, 0);
        assert_eq!(
            stats.decompressed_bytes,
            archive
                .index()
                .rows
                .iter()
                .map(|row| u64::from(row.uncompressed_len))
                .sum::<u64>()
        );
        assert_eq!(
            stats.max_live_decompressed_batch_bytes as u64,
            stats.decompressed_bytes
        );
        assert_eq!(stats.decompressed_buffer_growth_count, 2);
        assert_eq!(stats.decompressed_buffer_reuse_count, 0);
        assert!(
            stats.max_retained_decompressed_capacity_bytes
                <= OrderedParallelBlockConfig::default().uncompressed_batch_budget_bytes
        );
        assert_eq!(
            source.reads_for(BLOCKS_FILE),
            vec![(0, archive.index().blob_file_bytes as usize)]
        );
    }

    #[derive(Debug, Default)]
    struct TransactionStateTestCoordinator {
        phase: u8,
        merged_rows: Vec<usize>,
        state_pointers: Vec<Option<usize>>,
        decompressed_pointers: Vec<Option<usize>>,
    }

    #[test]
    fn batch_barrier_transaction_state_keeps_storage_order_and_coordinator_phases() {
        let fixture = Fixture::build_with_first_storage_tx_indexes([1, 0]);
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut coordinator = TransactionStateTestCoordinator {
            phase: 7,
            state_pointers: vec![None; 2],
            decompressed_pointers: vec![None; 2],
            ..TransactionStateTestCoordinator::default()
        };
        let mut consumed = Vec::new();

        let stats = archive
            .process_borrowed_blocks_parallel_batch_barrier_with_transaction_state(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    discard_rewards: true,
                    ..OrderedParallelBlockConfig::default()
                },
                1024,
                &mut coordinator,
                |_| (),
                |_, state, row_number, block, transaction_state: &mut [u8]| {
                    assert_eq!(state.phase, 7);
                    assert!(state.merged_rows.is_empty());
                    assert_eq!(transaction_state.len(), block.tx_count() as usize);
                    for (value, located) in transaction_state
                        .iter_mut()
                        .zip(block.storage_transaction_rows())
                    {
                        *value = u8::try_from(located.row.tx_index).unwrap() + 10;
                    }
                    if row_number == 0 {
                        assert_eq!(transaction_state, [11, 10]);
                    }
                    Ok((
                        transaction_state.as_ptr() as usize,
                        block.uncompressed_bytes().as_ptr() as usize,
                    ))
                },
                |state, row_number, (state_pointer, decompressed_pointer)| {
                    state.merged_rows.push(row_number);
                    state.state_pointers[row_number] = Some(state_pointer);
                    state.decompressed_pointers[row_number] = Some(decompressed_pointer);
                    Ok(())
                },
                |state, rows| {
                    assert_eq!(rows, 0..2);
                    assert_eq!(state.phase, 7);
                    assert_eq!(state.merged_rows, [0, 1]);
                    state.phase = 9;
                    Ok(())
                },
                |_, state, row_number, block, transaction_state: &[u8]| {
                    assert_eq!(state.phase, 9);
                    assert_eq!(state.merged_rows, [0, 1]);
                    assert_eq!(
                        state.state_pointers[row_number],
                        Some(transaction_state.as_ptr() as usize)
                    );
                    assert_eq!(
                        state.decompressed_pointers[row_number],
                        Some(block.uncompressed_bytes().as_ptr() as usize)
                    );
                    for (value, located) in transaction_state
                        .iter()
                        .zip(block.storage_transaction_rows())
                    {
                        assert_eq!(*value, u8::try_from(located.row.tx_index).unwrap() + 10);
                    }
                    Ok(transaction_state.to_vec())
                },
                |_, row_number, transaction_state| {
                    consumed.push((row_number, transaction_state));
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(consumed, [(0, vec![11, 10]), (1, vec![10])]);
        assert_eq!(stats.max_live_transaction_state_bytes, 3);
        assert_eq!(stats.transaction_state_buffer_growth_count, 2);
        assert_eq!(stats.transaction_state_buffer_reuse_count, 0);
        assert!(stats.max_retained_transaction_state_capacity_bytes >= 3);
    }

    #[test]
    fn batch_barrier_transaction_state_buffers_reset_and_reuse_across_batches() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut coordinator = Vec::new();

        let stats = archive
            .process_borrowed_blocks_parallel_batch_barrier_with_transaction_state(
                0..2,
                OrderedParallelBlockConfig {
                    max_blocks_per_batch: 1,
                    decode_workers: 1,
                    discard_rewards: true,
                    ..OrderedParallelBlockConfig::default()
                },
                1024,
                &mut coordinator,
                |_| (),
                |_, _, row_number, _, transaction_state: &mut [u8]| {
                    assert!(transaction_state.iter().all(|value| *value == 0));
                    transaction_state.fill(u8::try_from(row_number).unwrap() + 1);
                    Ok(transaction_state.as_ptr() as usize)
                },
                |state, _, pointer| {
                    state.push(pointer);
                    Ok(())
                },
                |_, _| Ok(()),
                |_, state, row_number, _, transaction_state: &[u8]| {
                    assert_eq!(state[row_number], transaction_state.as_ptr() as usize);
                    assert!(
                        transaction_state
                            .iter()
                            .all(|value| *value == u8::try_from(row_number).unwrap() + 1)
                    );
                    Ok(())
                },
                |_, _, ()| Ok(()),
            )
            .unwrap();

        assert_eq!(coordinator.len(), 2);
        assert_eq!(coordinator[0], coordinator[1]);
        assert_eq!(stats.batch_count, 2);
        assert_eq!(stats.transaction_state_buffer_growth_count, 1);
        assert_eq!(stats.transaction_state_buffer_reuse_count, 1);
        assert_eq!(stats.max_live_transaction_state_bytes, 2);
        assert!(stats.max_retained_transaction_state_capacity_bytes >= 2);
    }

    #[test]
    fn batch_barrier_transaction_state_prefix_is_checked_and_budgeted() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let rows = &archive.index().rows;
        let mut offsets = Vec::new();

        assert_eq!(
            prepare_batch_transaction_state_offsets(rows, &mut offsets, 1, 3).unwrap(),
            (3, 3)
        );
        assert_eq!(offsets, [0, 2, 3]);
        let valid_offsets = offsets.clone();

        let budget_error =
            prepare_batch_transaction_state_offsets(rows, &mut offsets, 1, 2).unwrap_err();
        assert!(matches!(budget_error, Error::InvalidManifest(_)));
        assert_eq!(offsets, valid_offsets);

        let overflow =
            prepare_batch_transaction_state_offsets(rows, &mut offsets, usize::MAX, usize::MAX)
                .unwrap_err();
        assert!(matches!(overflow, Error::Overflow(_)));
        assert_eq!(offsets, valid_offsets);
    }

    #[test]
    fn batch_barrier_transaction_state_budget_error_suppresses_both_stages() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut coordinator = Vec::<usize>::new();

        let error = archive
            .process_borrowed_blocks_parallel_batch_barrier_with_transaction_state(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                2,
                &mut coordinator,
                |_| (),
                |_, _, _, _, _: &mut [u8]| -> Result<()> {
                    panic!("a transaction-state budget error ran stage A")
                },
                |_, _, ()| panic!("a transaction-state budget error merged stage A"),
                |_, _| panic!("a transaction-state budget error finalized stage A"),
                |_, _, _, _, _: &[u8]| -> Result<()> {
                    panic!("a transaction-state budget error ran stage B")
                },
                |_, _, ()| panic!("a transaction-state budget error consumed stage B"),
            )
            .unwrap_err();

        assert!(matches!(error, Error::InvalidManifest(_)));
        assert!(error.to_string().contains("exceeding its 2-byte budget"));
        assert!(coordinator.is_empty());
    }

    #[test]
    fn batch_barrier_reuses_decompressed_row_slots_across_batches() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();
        let mut coordinator = Vec::new();

        let stats = archive
            .process_borrowed_blocks_parallel_batch_barrier(
                0..2,
                OrderedParallelBlockConfig {
                    max_blocks_per_batch: 1,
                    decode_workers: 1,
                    discard_rewards: true,
                    ..OrderedParallelBlockConfig::default()
                },
                &mut coordinator,
                |_| (),
                |_, row_number, block| {
                    Ok((row_number, block.uncompressed_bytes().as_ptr() as usize))
                },
                |state, row_number, output| {
                    assert_eq!(output.0, row_number);
                    state.push(output.1);
                    Ok(())
                },
                |_, _| Ok(()),
                |_, state, row_number, block| {
                    assert_eq!(
                        state[row_number],
                        block.uncompressed_bytes().as_ptr() as usize
                    );
                    Ok(())
                },
                |_, _, ()| Ok(()),
            )
            .unwrap();

        assert_eq!(stats.batch_count, 2);
        assert_eq!(stats.read_call_count, 2);
        assert_eq!(stats.decompression_count, 2);
        assert_eq!(
            stats
                .decompressed_buffer_growth_count
                .checked_add(stats.decompressed_buffer_reuse_count),
            Some(2)
        );
        assert!(stats.decompressed_buffer_reuse_count >= 1);
        assert_eq!(source.reads_for(BLOCKS_FILE).len(), 2);
    }

    #[test]
    fn batch_barrier_stage_a_error_merges_nothing_and_skips_stage_b() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let completion_barrier = Arc::new(Barrier::new(2));
        let projected = Arc::new(AtomicUsize::new(0));
        let stage_b_visits = Arc::new(AtomicUsize::new(0));
        let project_barrier = Arc::clone(&completion_barrier);
        let projected_count = Arc::clone(&projected);
        let stage_b_count = Arc::clone(&stage_b_visits);
        let mut coordinator = Vec::new();

        let error = archive
            .process_borrowed_blocks_parallel_batch_barrier(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                &mut coordinator,
                |_| (),
                move |_, row_number, _| -> Result<()> {
                    project_barrier.wait();
                    projected_count.fetch_add(1, Ordering::SeqCst);
                    if row_number == 0 {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(Error::InvalidBlock {
                        slot: 101 + row_number as u64,
                        message: format!("stage A row {row_number} failed"),
                    })
                },
                |state, row_number, ()| {
                    state.push(row_number);
                    Ok(())
                },
                |_, _| panic!("a failed stage-A batch was finalized"),
                move |_, _, _, _| -> Result<()> {
                    stage_b_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                |_, _, ()| panic!("a failed stage-A batch published stage B"),
            )
            .unwrap_err();

        assert!(matches!(error, Error::InvalidBlock { slot: 101, .. }));
        assert!(error.to_string().contains("stage A row 0 failed"));
        assert_eq!(projected.load(Ordering::SeqCst), 2);
        assert_eq!(stage_b_visits.load(Ordering::SeqCst), 0);
        assert!(coordinator.is_empty());
    }

    #[test]
    fn batch_barrier_stage_b_error_runs_no_ordered_stage_b_consumers() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let completion_barrier = Arc::new(Barrier::new(2));
        let stage_b_barrier = Arc::clone(&completion_barrier);
        let consumed = Arc::new(Mutex::new(Vec::new()));
        let ordered_consumed = Arc::clone(&consumed);
        let mut coordinator = Vec::new();

        let error = archive
            .process_borrowed_blocks_parallel_batch_barrier(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                &mut coordinator,
                |_| (),
                |_, row_number, _| Ok(row_number),
                |state, _, row_number| {
                    state.push(row_number);
                    Ok(())
                },
                |_, _| Ok(()),
                move |_, _, row_number, _| -> Result<()> {
                    stage_b_barrier.wait();
                    if row_number == 0 {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(Error::InvalidBlock {
                        slot: 101 + row_number as u64,
                        message: format!("stage B row {row_number} failed"),
                    })
                },
                move |_, row_number, ()| {
                    ordered_consumed.lock().unwrap().push(row_number);
                    Ok(())
                },
            )
            .unwrap_err();

        assert!(matches!(error, Error::InvalidBlock { slot: 101, .. }));
        assert!(error.to_string().contains("stage B row 0 failed"));
        assert!(consumed.lock().unwrap().is_empty());
    }

    #[test]
    fn batch_barrier_recycling_preserves_normal_storage_across_an_oversized_frame() {
        let normal = Vec::<u8>::with_capacity(4096);
        let normal_pointer = normal.as_ptr();
        let normal_capacity = normal.capacity();
        let oversized_bytes = normal_capacity.checked_add(1).unwrap();
        let mut buffer = BatchBarrierDecodedBuffer {
            bytes: normal,
            retained_buffer: None,
        };

        assert!(!buffer.prepare(oversized_bytes, normal_capacity));
        assert_eq!(
            buffer.retained_buffer.as_ref().map(Vec::as_ptr),
            Some(normal_pointer)
        );
        buffer.bytes.resize(oversized_bytes, 7);
        let retained =
            recycle_batch_barrier_buffers(std::slice::from_mut(&mut buffer), normal_capacity);

        assert!(buffer.retained_buffer.is_none());
        assert!(buffer.bytes.is_empty());
        assert_eq!(buffer.bytes.capacity(), normal_capacity);
        assert_eq!(buffer.bytes.as_ptr(), normal_pointer);
        assert_eq!(retained, normal_capacity);
    }

    #[test]
    fn ordered_compressed_buffers_keep_the_normal_allocation_across_an_oversized_frame() {
        let normal = Vec::<u8>::with_capacity(4096);
        let normal_pointer = normal.as_ptr();
        let normal_capacity = normal.capacity();
        let oversized_bytes = normal_capacity.checked_add(1).unwrap();
        let (mut oversized, retained) =
            ordered_compressed_read_buffer(normal, oversized_bytes, normal_capacity);
        assert_eq!(oversized.capacity(), 0);
        oversized.resize(oversized_bytes, 7);
        let recycled = recycle_ordered_compressed_buffer(OrderedReadyBatch {
            plan: OrderedParallelBatchPlan {
                row_start: 0,
                row_end: 1,
                compressed_offset: 0,
                compressed_len: oversized_bytes,
                declared_uncompressed_bytes: oversized_bytes as u64,
            },
            bytes: oversized,
            retained_buffer: retained,
        });
        assert!(recycled.is_empty());
        assert_eq!(recycled.capacity(), normal_capacity);
        assert_eq!(recycled.as_ptr(), normal_pointer);

        let (mut normal_read, retained) =
            ordered_compressed_read_buffer(recycled, 2048, normal_capacity);
        assert!(retained.is_none());
        normal_read.resize(2048, 9);
        let recycled = recycle_ordered_compressed_buffer(OrderedReadyBatch {
            plan: OrderedParallelBatchPlan {
                row_start: 0,
                row_end: 1,
                compressed_offset: 0,
                compressed_len: 2048,
                declared_uncompressed_bytes: 2048,
            },
            bytes: normal_read,
            retained_buffer: None,
        });
        assert!(recycled.is_empty());
        assert_eq!(recycled.capacity(), normal_capacity);
        assert_eq!(recycled.as_ptr(), normal_pointer);
    }

    #[test]
    fn ordered_worker_reuses_normal_decompression_storage_and_preserves_it_on_oversize() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let row = archive.index().rows[0];
        let compressed = archive
            .source
            .read_range(
                BLOCKS_FILE,
                row.compressed_offset,
                row.compressed_len as usize,
            )
            .unwrap();
        let expected = row.uncompressed_len as usize;
        let decompressed = Vec::with_capacity(expected);
        let retained = decompressed.capacity();
        let mut worker = OrderedParallelWorker {
            decompressor: None,
            decompressed,
            caller: (),
        };
        let retained_pointer = worker.decompressed.as_ptr();
        worker
            .decode_and_project(
                &archive,
                row,
                &compressed,
                0,
                true,
                retained,
                &|_, _, block| Ok(block.tx_count()),
            )
            .unwrap();
        assert!(worker.decompressed.is_empty());
        assert_eq!(worker.decompressed.as_ptr(), retained_pointer);
        assert_eq!(worker.decompressed.capacity(), retained);

        let mut worker = OrderedParallelWorker {
            decompressor: None,
            decompressed: Vec::with_capacity(128),
            caller: (),
        };
        let normal_pointer = worker.decompressed.as_ptr();
        worker
            .decode_and_project(&archive, row, &compressed, 0, true, 128, &|_, _, block| {
                Ok(block.tx_count())
            })
            .unwrap();
        assert!(worker.decompressed.is_empty());
        assert_eq!(worker.decompressed.capacity(), 128);
        assert_eq!(worker.decompressed.as_ptr(), normal_pointer);
    }

    #[test]
    fn ordered_parallel_empty_range_does_not_start_workers_or_read_blocks() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();
        let mut worker_states_created = 0usize;

        let stats = archive
            .process_borrowed_blocks_parallel_ordered(
                1..1,
                OrderedParallelBlockConfig::default(),
                |_| {
                    worker_states_created += 1;
                },
                |_, _, _| -> Result<()> { panic!("empty range ran a projection") },
                |_, ()| -> Result<()> { panic!("empty range published a result") },
            )
            .unwrap();

        assert_eq!(worker_states_created, 0);
        assert_eq!(stats, OrderedParallelBlockStats::default());
        assert!(source.reads_for(BLOCKS_FILE).is_empty());
    }

    #[test]
    fn ordered_parallel_pipeline_splits_only_on_frame_boundaries() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();
        let first = archive.index().rows[0];
        let second = archive.index().rows[1];
        let mut slots = Vec::new();
        let stats = archive
            .process_borrowed_blocks_parallel_ordered(
                0..2,
                OrderedParallelBlockConfig {
                    compressed_batch_target_bytes: first.compressed_len as usize,
                    uncompressed_batch_budget_bytes: MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
                    compressed_buffer_count: 2,
                    decode_workers: 1,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| (),
                |_, _, block| Ok(block.header().slot),
                |_, slot| {
                    slots.push(slot);
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(slots, vec![101, 102]);
        assert_eq!(stats.batch_count, 2);
        assert_eq!(stats.read_call_count, 2);
        assert_eq!(
            source.reads_for(BLOCKS_FILE),
            vec![
                (first.compressed_offset, first.compressed_len as usize),
                (second.compressed_offset, second.compressed_len as usize),
            ]
        );
    }

    #[test]
    fn ordered_parallel_uncompressed_budget_splits_and_admits_one_oversized_frame() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();
        let rows = &archive.index().rows;
        let mut slots = Vec::new();

        let stats = archive
            .process_borrowed_blocks_parallel_ordered(
                0..rows.len(),
                OrderedParallelBlockConfig {
                    compressed_batch_target_bytes: archive.index().blob_file_bytes as usize,
                    uncompressed_batch_budget_bytes: 1,
                    compressed_buffer_count: 2,
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| (),
                |_, _, block| Ok(block.header().slot),
                |_, slot| {
                    slots.push(slot);
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(slots, vec![101, 102]);
        assert_eq!(stats.batch_count, 2);
        assert_eq!(stats.read_call_count, 2);
        assert!(stats.max_declared_uncompressed_batch_bytes > 1);
        assert_eq!(
            source.reads_for(BLOCKS_FILE),
            rows.iter()
                .map(|row| (row.compressed_offset, row.compressed_len as usize))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn ordered_parallel_block_bound_splits_before_projection_results_can_accumulate() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();
        let mut slots = Vec::new();

        let stats = archive
            .process_borrowed_blocks_parallel_ordered(
                0..archive.index().rows.len(),
                OrderedParallelBlockConfig {
                    compressed_batch_target_bytes: usize::MAX,
                    uncompressed_batch_budget_bytes: MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
                    max_blocks_per_batch: 1,
                    compressed_buffer_count: 2,
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| (),
                |_, _, block| Ok(block.header().slot),
                |_, slot| {
                    slots.push(slot);
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(slots, vec![101, 102]);
        assert_eq!(stats.batch_count, 2);
        assert_eq!(stats.read_call_count, 2);
    }

    #[test]
    fn ordered_parallel_pipeline_selects_the_first_row_error() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let completion_barrier = Arc::new(Barrier::new(2));
        let project_barrier = Arc::clone(&completion_barrier);
        let consumed = Arc::new(Mutex::new(Vec::new()));
        let ordered_consumed = Arc::clone(&consumed);
        let error = archive
            .process_borrowed_blocks_parallel_ordered(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| (),
                move |_, row_number, _| -> Result<()> {
                    project_barrier.wait();
                    if row_number == 0 {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(Error::InvalidBlock {
                        slot: 101 + row_number as u64,
                        message: format!("row {row_number} failed"),
                    })
                },
                move |row_number, ()| {
                    ordered_consumed.lock().unwrap().push(row_number);
                    Ok(())
                },
            )
            .unwrap_err();

        assert!(matches!(error, Error::InvalidBlock { slot: 101, .. }));
        assert!(error.to_string().contains("row 0 failed"));
        assert!(consumed.lock().unwrap().is_empty());
    }

    #[test]
    fn ordered_parallel_early_error_releases_the_producer() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source, options).unwrap();
        let first_frame_bytes = archive.index().rows[0].compressed_len as usize;
        let error = archive
            .process_borrowed_blocks_parallel_ordered(
                0..2,
                OrderedParallelBlockConfig {
                    compressed_batch_target_bytes: first_frame_bytes,
                    compressed_buffer_count: 1,
                    decode_workers: 1,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| (),
                |_, row_number, _| -> Result<()> {
                    Err(Error::InvalidBlock {
                        slot: 101 + row_number as u64,
                        message: "stop after the first ordered block".into(),
                    })
                },
                |_, ()| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(error, Error::InvalidBlock { slot: 101, .. }));
    }

    #[test]
    fn ordered_parallel_consume_error_releases_the_producer() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let first_frame_bytes = archive.index().rows[0].compressed_len as usize;

        let error = archive
            .process_borrowed_blocks_parallel_ordered(
                0..2,
                OrderedParallelBlockConfig {
                    compressed_batch_target_bytes: first_frame_bytes,
                    compressed_buffer_count: 1,
                    decode_workers: 1,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| (),
                |_, _, block| Ok(block.header().slot),
                |row_number, _| {
                    Err(Error::WireProfileAudit(format!(
                        "stop while consuming row {row_number}"
                    )))
                },
            )
            .unwrap_err();

        assert!(matches!(error, Error::WireProfileAudit(_)));
        assert!(error.to_string().contains("consuming row 0"));
    }

    #[test]
    fn ordered_parallel_source_error_releases_the_coordinator() {
        let fixture = Fixture::build();
        let source = FailingBlocksSource {
            inner: fixture.source(),
        };
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source, options).unwrap();

        let error = archive
            .process_borrowed_blocks_parallel_ordered(
                0..2,
                OrderedParallelBlockConfig {
                    compressed_buffer_count: 1,
                    decode_workers: 1,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| (),
                |_, _, _| -> Result<()> { panic!("failed source ran a projection") },
                |_, ()| -> Result<()> { panic!("failed source published a result") },
            )
            .unwrap_err();

        assert!(matches!(error, Error::Source(SourceError::Protocol(_))));
        assert!(
            error
                .to_string()
                .contains("injected ordered block read failure")
        );
    }

    #[test]
    fn ordered_parallel_config_rejects_excessive_resources() {
        let invalid = [
            OrderedParallelBlockConfig {
                decode_workers: MAX_ORDERED_PARALLEL_DECODE_WORKERS + 1,
                ..OrderedParallelBlockConfig::default()
            },
            OrderedParallelBlockConfig {
                compressed_buffer_count: MAX_ORDERED_PARALLEL_COMPRESSED_BUFFERS + 1,
                ..OrderedParallelBlockConfig::default()
            },
            OrderedParallelBlockConfig {
                uncompressed_batch_budget_bytes: MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES + 1,
                ..OrderedParallelBlockConfig::default()
            },
            OrderedParallelBlockConfig {
                max_blocks_per_batch: MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH + 1,
                ..OrderedParallelBlockConfig::default()
            },
            OrderedParallelBlockConfig {
                decode_workers: MAX_ORDERED_PARALLEL_DECODE_WORKERS,
                retained_decompressed_bytes_per_worker:
                    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES
                        / MAX_ORDERED_PARALLEL_DECODE_WORKERS
                        + 1,
                ..OrderedParallelBlockConfig::default()
            },
        ];
        for config in invalid {
            assert!(matches!(
                validate_ordered_parallel_config(config),
                Err(Error::InvalidManifest(_))
            ));
        }

        assert!(
            validate_ordered_parallel_config(OrderedParallelBlockConfig {
                decode_workers: MAX_ORDERED_PARALLEL_DECODE_WORKERS,
                compressed_buffer_count: MAX_ORDERED_PARALLEL_COMPRESSED_BUFFERS,
                uncompressed_batch_budget_bytes: MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
                max_blocks_per_batch: MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH,
                retained_decompressed_bytes_per_worker:
                    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES
                        / MAX_ORDERED_PARALLEL_DECODE_WORKERS,
                ..OrderedParallelBlockConfig::default()
            })
            .is_ok()
        );
    }

    #[test]
    fn lending_stream_matches_owned_blocks_and_coalesces_the_same_range() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        let owned = archive.blocks().collect::<Result<Vec<_>>>().unwrap();

        source.clear();
        let mut stream = archive.borrowed_blocks();
        assert_eq!(stream.len(), owned.len());
        let mut number = 0usize;
        while let Some(block) = stream.next_block() {
            let block = block.unwrap();
            let expected = &owned[number];
            assert!(!block.uses_owned_fallback());
            assert_eq!(block.index_row.block_id, expected.index_row.block_id);
            assert_eq!(block.index_row.slot, expected.index_row.slot);
            assert_eq!(block.header().slot, expected.block.header.slot);
            assert_eq!(
                block.header().parent_slot,
                expected.block.header.parent_slot
            );
            assert_eq!(block.tx_count(), expected.block.tx_count);
            assert_eq!(block.tx_rows_len(), expected.block.tx_rows.len());
            assert_eq!(block.tx_rows().collect::<Vec<_>>(), expected.block.tx_rows);
            assert_eq!(block.message_bytes(), expected.block.message_bytes);
            assert_eq!(block.metadata_bytes(), expected.block.metadata_bytes);
            assert_eq!(
                block
                    .header()
                    .rewards
                    .as_ref()
                    .map(|rewards| rewards.decoded.len()),
                expected
                    .block
                    .header
                    .rewards
                    .as_ref()
                    .map(|rewards| rewards.decoded.len())
            );
            number += 1;
        }
        assert_eq!(number, owned.len());
        assert!(stream.is_empty());
        assert_eq!(source.reads_for(BLOCKS_FILE).len(), 1);
    }

    #[test]
    fn reward_discarding_lending_stream_preserves_block_projection_and_borrows_regions() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let owned = archive.blocks().collect::<Result<Vec<_>>>().unwrap();
        assert!(owned[0].block.header.rewards.is_some());

        let mut stream = archive
            .borrowed_blocks_without_rewards_range(0..owned.len())
            .unwrap();
        let mut number = 0usize;
        while let Some(block) = stream.next_block() {
            let block = block.unwrap();
            let expected = &owned[number];
            assert!(!block.uses_owned_fallback());
            assert!(block.header().rewards.is_none());
            assert_eq!(block.header().slot, expected.block.header.slot);
            assert_eq!(
                block.header().parent_slot,
                expected.block.header.parent_slot
            );
            assert_eq!(
                block.header().blockhash_id,
                expected.block.header.blockhash_id
            );
            assert_eq!(
                block.header().previous_blockhash_id,
                expected.block.header.previous_blockhash_id
            );
            assert_eq!(block.header().block_time, expected.block.header.block_time);
            assert_eq!(
                block.header().block_height,
                expected.block.header.block_height
            );
            assert_eq!(block.tx_count(), expected.block.tx_count);
            assert_eq!(block.tx_rows().collect::<Vec<_>>(), expected.block.tx_rows);
            assert_eq!(block.message_bytes(), expected.block.message_bytes);
            assert_eq!(block.metadata_bytes(), expected.block.metadata_bytes);
            number += 1;
        }
        assert_eq!(number, owned.len());
    }

    #[test]
    fn lending_and_owned_paths_apply_identical_structural_validation() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut decoded = archive.read_block(0).unwrap();
        decoded.block.tx_rows[0].reserved[0] = 1;
        let bytes = wincode::config::serialize(&decoded.block, wincode_leb128_config()).unwrap();
        let mut row = decoded.index_row;
        row.uncompressed_len = bytes.len() as u32;

        let owned_error = archive.decode_uncompressed_block(row, &bytes).unwrap_err();
        let borrowed_error = archive
            .decode_uncompressed_block_borrowed(row, &bytes, false)
            .unwrap_err();
        assert_eq!(owned_error.to_string(), borrowed_error.to_string());
        assert!(owned_error.to_string().contains("non-zero reserved bytes"));
    }

    #[test]
    fn transaction_index_permutation_preserves_storage_bindings_and_exposes_canonical_order() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut decoded = archive.read_block(0).unwrap();
        decoded.block.tx_rows[0].tx_index = 1;
        decoded.block.tx_rows[1].tx_index = 0;
        let bytes = wincode::config::serialize(&decoded.block, wincode_leb128_config()).unwrap();
        let mut row = decoded.index_row;
        row.uncompressed_len = bytes.len() as u32;

        let borrowed = archive
            .decode_uncompressed_block_borrowed(row, &bytes, false)
            .unwrap();
        assert_eq!(
            borrowed
                .tx_rows()
                .map(|row| row.tx_index)
                .collect::<Vec<_>>(),
            vec![1, 0],
        );
        let (storage_rows, allocations) =
            crate::test_allocations::count_current_thread_allocations(|| {
                let mut rows = borrowed.storage_transaction_rows();
                assert_eq!(rows.len(), 2);
                let first = rows.next().unwrap();
                let second = rows.next().unwrap();
                assert!(rows.next().is_none());
                [first, second]
            });
        assert_eq!(allocations, 0);
        assert_eq!(
            storage_rows.map(|location| {
                (
                    location.row.tx_index,
                    location.storage_position,
                    location.first_signature_offset,
                )
            }),
            [(1, 0, 0), (0, 1, 2)],
        );
        let borrowed_order = borrowed.transaction_row_order();
        assert!(!borrowed_order.storage_order_is_canonical());
        assert_eq!(borrowed_order.len(), 2);
        assert!(!borrowed_order.is_empty());
        assert_eq!(
            borrowed_order
                .canonical_rows()
                .iter()
                .map(|location| location.row.tx_index)
                .collect::<Vec<_>>(),
            vec![0, 1],
        );
        assert_eq!(
            borrowed_order
                .canonical_rows()
                .iter()
                .map(|location| (location.storage_position, location.first_signature_offset))
                .collect::<Vec<_>>(),
            vec![(1, 2), (0, 0)],
        );
        assert_eq!(
            borrowed_order.row_for_tx_index(0).unwrap().storage_position,
            1,
        );
        assert!(borrowed_order.row_for_tx_index(2).is_none());
        drop(borrowed);

        let decoded = archive.decode_uncompressed_block(row, &bytes).unwrap();
        assert_eq!(decoded.transaction_row_order().unwrap(), borrowed_order);
        let filter = archive.compile_pubkey_filter([RAW_KEY]).unwrap();
        let scanned = archive.scan_decoded_block(&filter, decoded).unwrap();
        assert_eq!(
            scanned
                .transactions
                .iter()
                .map(|transaction| transaction.tx_index)
                .collect::<Vec<_>>(),
            vec![0, 1],
        );
        assert_eq!(scanned.transactions[0].signatures.first_ordinal, 2);
        assert_eq!(scanned.transactions[1].signatures.first_ordinal, 0);
        assert_eq!(
            archive
                .read_transaction_signatures(scanned.transactions[0].signatures)
                .unwrap(),
            vec![[9u8; 64]],
        );
        assert_eq!(
            archive
                .read_transaction_signatures(scanned.transactions[1].signatures)
                .unwrap(),
            vec![[7u8; 64], [70u8; 64]],
        );
    }

    #[test]
    fn canonical_storage_transaction_rows_stream_without_allocating() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut blocks = archive.borrowed_blocks_without_rewards_range(0..1).unwrap();
        let block = blocks.next_block().unwrap().unwrap();
        assert!(!block.uses_owned_fallback());

        let (locations, allocations) =
            crate::test_allocations::count_current_thread_allocations(|| {
                let mut rows = block.storage_transaction_rows();
                assert_eq!(rows.len(), 2);
                let first = rows.next().unwrap();
                let second = rows.next().unwrap();
                assert!(rows.next().is_none());
                [first, second]
            });

        assert_eq!(allocations, 0);
        assert_eq!(
            locations.map(|location| {
                (
                    location.row.tx_index,
                    location.storage_position,
                    location.first_signature_offset,
                )
            }),
            [(0, 0, 0), (1, 1, 2)],
        );
    }

    #[test]
    fn transaction_index_validation_rejects_duplicates_and_out_of_range() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();

        for (indexes, expected) in [
            ([0, 0], "transaction row 1 repeats tx_index 0"),
            ([0, 2], "transaction row 1 has tx_index 2, outside 0..2"),
        ] {
            let mut decoded = archive.read_block(0).unwrap();
            decoded.block.tx_rows[0].tx_index = indexes[0];
            decoded.block.tx_rows[1].tx_index = indexes[1];
            let bytes =
                wincode::config::serialize(&decoded.block, wincode_leb128_config()).unwrap();
            let mut row = decoded.index_row;
            row.uncompressed_len = bytes.len() as u32;

            let owned_error = archive.decode_uncompressed_block(row, &bytes).unwrap_err();
            let borrowed_error = archive
                .decode_uncompressed_block_borrowed(row, &bytes, false)
                .unwrap_err();
            assert_eq!(owned_error.to_string(), borrowed_error.to_string());
            assert!(owned_error.to_string().contains(expected));
        }
    }

    #[test]
    fn lending_stream_decoder_preserves_legacy_owned_fallback() {
        #[derive(wincode::SchemaWrite)]
        struct LegacyBlock {
            header: LegacyHeader,
            tx_count: u32,
            tx_rows: Vec<ArchiveV2HotTxRow>,
            message_bytes: Vec<u8>,
            metadata_bytes: Vec<u8>,
        }

        #[derive(wincode::SchemaWrite)]
        struct LegacyHeader {
            slot: u64,
            parent_slot: u64,
            blockhash_id: u32,
            previous_blockhash_id: u32,
            block_time: Option<i64>,
            block_height: Option<u64>,
            shredding: Vec<CompactShredding>,
            rewards: Option<ArchiveV2HotRewards>,
        }

        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let block = LegacyBlock {
            header: LegacyHeader {
                slot: 777,
                parent_slot: 776,
                blockhash_id: 3,
                previous_blockhash_id: 2,
                block_time: None,
                block_height: Some(700),
                shredding: vec![
                    CompactShredding {
                        entry_end_idx: 1,
                        shred_end_idx: 2,
                    },
                    CompactShredding {
                        entry_end_idx: 3,
                        shred_end_idx: 4,
                    },
                ],
                rewards: None,
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let bytes = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let row = ArchiveV2HotBlockIndexRow {
            block_id: 9,
            slot: 777,
            compressed_offset: 0,
            compressed_len: 0,
            uncompressed_len: bytes.len() as u32,
            tx_count: 0,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 0,
        };

        for discard_rewards in [false, true] {
            let decoded = archive
                .decode_uncompressed_block_borrowed(row, &bytes, discard_rewards)
                .unwrap();
            assert!(decoded.uses_owned_fallback());
            assert_eq!(decoded.header().slot, 777);
            assert_eq!(decoded.tx_rows_len(), 0);
        }

        let compressed = zstd::bulk::compress(&bytes, 3).unwrap();
        let mut compressed_row = row;
        compressed_row.compressed_len = compressed.len() as u32;
        let mut worker = OrderedParallelWorker {
            decompressor: None,
            decompressed: Vec::new(),
            caller: (),
        };
        let projected = worker
            .decode_and_project(
                &archive,
                compressed_row,
                &compressed,
                9,
                true,
                0,
                &|_, row_number, decoded| {
                    assert_eq!(row_number, 9);
                    assert_eq!(decoded.header().slot, 777);
                    Ok(decoded.uses_owned_fallback())
                },
            )
            .unwrap();
        assert!(projected.output);
        assert!(projected.used_owned_schema_fallback);
        assert_eq!(worker.decompressed.capacity(), 0);
    }

    #[test]
    fn bounded_sequential_iterator_fetches_only_the_requested_rows() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        let second_row = archive.index().rows[1];

        source.clear();
        let mut blocks = archive.blocks_range(1..2).unwrap();
        assert_eq!(blocks.len(), 1);
        let decoded = blocks.next().unwrap().unwrap();
        assert_eq!(decoded.index_row.block_id, second_row.block_id);
        assert_eq!(decoded.index_row.slot, second_row.slot);
        assert!(blocks.next().is_none());
        assert_eq!(
            source.reads_for(BLOCKS_FILE),
            vec![(
                second_row.compressed_offset,
                second_row.compressed_len as usize,
            )]
        );

        source.clear();
        let mut empty = archive.blocks_range(1..1).unwrap();
        assert_eq!(empty.len(), 0);
        assert!(empty.next().is_none());
        assert!(source.reads_for(BLOCKS_FILE).is_empty());

        let reversed_start = archive.index().rows.len();
        let reversed_end = reversed_start - 1;
        assert!(matches!(
            archive.blocks_range(reversed_start..reversed_end),
            Err(Error::InvalidIndex(_))
        ));
        assert!(matches!(
            archive.blocks_range(0..3),
            Err(Error::InvalidIndex(_))
        ));
    }

    #[test]
    fn control_file_policy_does_not_download_blocks_or_signatures_during_open() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::ControlFiles,
            ..OpenOptions::default()
        };
        let _archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        assert!(source.reads_for(BLOCKS_FILE).is_empty());
        assert!(source.reads_for(SIGNATURES_FILE).is_empty());
        assert!(!source.reads_for(REGISTRY_FILE).is_empty());
        assert!(!source.reads_for(BLOCK_INDEX_FILE).is_empty());
        assert!(!source.reads_for(META_FILE).is_empty());
        assert!(
            !source
                .reads_for(CURRENT_TYPED_ERRORS_MARKER_FILE)
                .is_empty()
        );
    }

    #[test]
    fn compressed_frame_limit_is_checked_before_reading_blocks() {
        let fixture = Fixture::build();
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            max_compressed_frame_bytes: 1,
            ..OpenOptions::default()
        };
        let error = ArchiveReader::open_with_options(fixture.source(), options).unwrap_err();
        assert!(matches!(error, Error::InvalidIndex(_)));
        assert!(error.to_string().contains("compressed bytes"));
    }

    #[test]
    fn all_block_decoders_reject_concatenated_zstd_frames() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut row = archive.index().rows[0];
        let blocks = fs::read(fixture.directory.path().join(BLOCKS_FILE)).unwrap();
        let mut concatenated = blocks[..row.compressed_len as usize].to_vec();
        concatenated.extend_from_slice(&zstd::bulk::compress(b"second-frame", 3).unwrap());
        row.compressed_len = concatenated.len() as u32;

        let error = archive
            .decode_compressed_block(row, &concatenated)
            .unwrap_err();
        assert!(error.to_string().contains("first zstd frame"));

        let mut decompressor = zstd::bulk::Decompressor::new().unwrap();
        let mut decompressed = Vec::new();
        let error = archive
            .decode_compressed_block_reusing(
                row,
                &concatenated,
                &mut decompressor,
                &mut decompressed,
            )
            .unwrap_err();
        assert!(error.to_string().contains("first zstd frame"));

        let error = archive
            .decode_compressed_block_borrowed_reusing(
                row,
                &concatenated,
                &mut decompressor,
                &mut decompressed,
                false,
            )
            .unwrap_err();
        assert!(error.to_string().contains("first zstd frame"));
    }

    #[test]
    fn ordered_parallel_pipeline_rejects_a_concatenated_frame() {
        let fixture = Fixture::build();
        let mut archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut blocks = fs::read(fixture.directory.path().join(BLOCKS_FILE)).unwrap();
        let first_frame_end = archive.index.rows[0].compressed_len as usize;
        let extra = zstd::bulk::compress(b"second-frame", 3).unwrap();
        blocks.splice(first_frame_end..first_frame_end, extra.iter().copied());
        fs::write(fixture.directory.path().join(BLOCKS_FILE), blocks).unwrap();

        archive.index.rows[0].compressed_len = archive.index.rows[0]
            .compressed_len
            .checked_add(extra.len() as u32)
            .unwrap();
        archive.index.rows[1].compressed_offset = archive.index.rows[1]
            .compressed_offset
            .checked_add(extra.len() as u64)
            .unwrap();
        archive.index.blob_file_bytes = archive
            .index
            .blob_file_bytes
            .checked_add(extra.len() as u64)
            .unwrap();

        let error = archive
            .process_borrowed_blocks_parallel_ordered(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| (),
                |_, _, _| Ok(()),
                |_, ()| Ok(()),
            )
            .unwrap_err();
        assert!(error.to_string().contains("first zstd frame"));
    }

    #[test]
    fn raw_transaction_and_unavailable_v0_loaded_addresses_are_indeterminate() {
        let fixture = Fixture::build();
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(fixture.source(), options).unwrap();
        let filter = archive.compile_pubkey_filter([REGISTRY_KEY_TWO]).unwrap();

        let v0_message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: message_header(),
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
            address_table_lookups: Vec::new(),
        });
        let v0_message = wincode::config::serialize(&v0_message, wincode_leb128_config()).unwrap();
        let raw_fallback = vec![0xff];
        let message_bytes = [raw_fallback.as_slice(), v0_message.as_slice()].concat();
        let rows = vec![
            ArchiveV2HotTxRow {
                tx_index: 0,
                flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                message_offset: 0,
                message_len: raw_fallback.len() as u32,
                metadata_offset: 0,
                metadata_len: 0,
                signature_count: 1,
                reserved: [0; 3],
            },
            ArchiveV2HotTxRow {
                tx_index: 1,
                flags: ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                    | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                    | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                message_offset: raw_fallback.len() as u32,
                message_len: v0_message.len() as u32,
                metadata_offset: 0,
                metadata_len: 1,
                signature_count: 1,
                reserved: [0; 3],
            },
        ];
        let decoded = DecodedBlock {
            index_row: ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 101,
                compressed_offset: 0,
                compressed_len: 1,
                uncompressed_len: 1,
                tx_count: 2,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 2,
            },
            block: ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot: 101,
                    parent_slot: 100,
                    blockhash_id: 1,
                    previous_blockhash_id: 0,
                    block_time: None,
                    block_height: None,
                    rewards: None,
                },
                tx_count: 2,
                tx_rows: rows,
                message_bytes,
                metadata_bytes: vec![0xaa],
            },
        };
        let scanned = archive.scan_decoded_block(&filter, decoded).unwrap();
        assert_eq!(
            scanned.transactions[0].outcome,
            TransactionMatch::Indeterminate(IndeterminateReason::RawTransactionFallback)
        );
        assert_eq!(
            scanned.transactions[1].outcome,
            TransactionMatch::Indeterminate(IndeterminateReason::V0LoadedAddressesUnavailable)
        );
        assert!(matches!(
            scanned.transactions[1].metadata,
            MetadataState::RawFallback
        ));
    }

    #[test]
    fn open_rejects_incomplete_and_missing_required_generation() {
        let fixture = Fixture::build();
        write_manifest(fixture.directory.path(), false, None);
        assert!(matches!(
            ArchiveReader::open(fixture.source()).unwrap_err(),
            Error::IncompleteGeneration
        ));

        write_manifest(fixture.directory.path(), true, Some(META_FILE));
        assert!(matches!(
            ArchiveReader::open(fixture.source()).unwrap_err(),
            Error::MissingFile(name) if name == META_FILE
        ));
    }

    #[test]
    fn historical_epoch0_genesis_tag_is_accepted_only_at_record_one() {
        let record = ArchiveV2HotMetaRecord::Genesis(compatibility_test_genesis());
        let canonical = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
        assert_eq!(canonical[0], ARCHIVE_V2_HOT_META_GENESIS_TAG);

        let mut historical = canonical.clone();
        historical[0] = HISTORICAL_EPOCH0_HOT_META_GENESIS_TAG;
        let decoded = decode_hot_metadata_record(&mut historical, 0, 1).unwrap();
        let ArchiveV2HotMetaRecord::Genesis(genesis) = decoded else {
            panic!("historical tag did not decode as Genesis");
        };
        assert_eq!(genesis.genesis_bin_len, 17);
        assert_eq!(historical[0], ARCHIVE_V2_HOT_META_GENESIS_TAG);

        for (epoch, position) in [(0, 0), (0, 2), (1, 1)] {
            let mut out_of_scope = canonical.clone();
            out_of_scope[0] = HISTORICAL_EPOCH0_HOT_META_GENESIS_TAG;
            assert!(matches!(
                decode_hot_metadata_record(&mut out_of_scope, epoch, position),
                Err(Error::InvalidMetadata(message))
                    if message.contains(&format!("decode record {position}"))
            ));
            assert_eq!(out_of_scope[0], HISTORICAL_EPOCH0_HOT_META_GENESIS_TAG);
        }
    }

    #[test]
    fn historical_epoch0_genesis_tag_does_not_accept_a_malformed_payload() {
        let mut malformed = vec![HISTORICAL_EPOCH0_HOT_META_GENESIS_TAG];
        assert!(matches!(
            decode_hot_metadata_record(&mut malformed, 0, 1),
            Err(Error::InvalidMetadata(message)) if message.contains("decode record 1")
        ));
        assert_eq!(malformed[0], ARCHIVE_V2_HOT_META_GENESIS_TAG);
    }

    #[test]
    fn metadata_frame_decoder_rejects_trailing_bytes_after_tag_normalization() {
        let record = ArchiveV2HotMetaRecord::Genesis(compatibility_test_genesis());
        let canonical = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();

        for first_tag in [
            ARCHIVE_V2_HOT_META_GENESIS_TAG,
            HISTORICAL_EPOCH0_HOT_META_GENESIS_TAG,
        ] {
            let mut frame = canonical.clone();
            frame[0] = first_tag;
            frame.push(0xff);
            assert!(matches!(
                decode_hot_metadata_record(&mut frame, 0, 1),
                Err(Error::InvalidMetadata(message)) if message.contains("decode record 1")
            ));
        }
    }

    #[test]
    fn metadata_frame_decoder_rejects_hostile_footer_preallocation() {
        let record = ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer::default());
        let mut frame = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
        assert_eq!(frame.last(), Some(&0));
        let hostile_count = wincode::config::serialize(&u64::MAX, wincode_leb128_config()).unwrap();
        frame.splice(frame.len() - 1..frame.len(), hostile_count);

        assert!(matches!(
            decode_hot_metadata_record(&mut frame, 1, 1),
            Err(Error::InvalidMetadata(message)) if message.contains("preallocation")
        ));
    }

    #[test]
    fn metadata_frame_reader_rejects_a_non_minimal_length_varint() {
        assert!(matches!(
            read_frame(&mut [0x80, 0x00].as_slice(), 1024),
            Err(Error::InvalidMetadata(message)) if message.contains("non-minimal varint")
        ));
    }

    #[test]
    fn transaction_metadata_decoder_rejects_trailing_bytes() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 5000,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: Some(42),
            cost_units: None,
        };
        let mut metadata_bytes =
            wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        metadata_bytes.push(0xff);
        let row = ArchiveV2HotTxRow {
            tx_index: 3,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA,
            message_offset: 0,
            message_len: 0,
            metadata_offset: 0,
            metadata_len: metadata_bytes.len() as u32,
            signature_count: 0,
            reserved: [0; 3],
        };
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: 101,
                parent_slot: 100,
                blockhash_id: 1,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: None,
            },
            tx_count: 1,
            tx_rows: vec![row],
            message_bytes: Vec::new(),
            metadata_bytes,
        };

        assert!(matches!(
            metadata_state(
                &block,
                &row,
                101,
                true,
                ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1,
            ),
            Err(Error::InvalidBlock { slot: 101, message })
                if message.contains("decode metadata for tx 3")
        ));
    }

    #[test]
    fn exact_genesis_sidecar_is_bound_to_inline_identity() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = b"launch-genesis-bytes";
        fs::write(directory.path().join(GENESIS_BIN_FILE), bytes).unwrap();
        let hash: [u8; 32] = Sha256::digest(bytes).into();
        let inline = WincodeArchiveV2Genesis {
            genesis_hash: hash,
            genesis_bin_len: bytes.len() as u64,
            creation_time_unix: 0,
            cluster_id: 0,
            ticks_per_slot: 64,
            poh_params: blockzilla_format::WincodeArchiveV2GenesisPohParams {
                tick_duration_secs: 0,
                tick_duration_nanos: 400_000_000,
                tick_count: None,
                hashes_per_tick: Some(12_500),
            },
            fees: blockzilla_format::WincodeArchiveV2GenesisFeeParams {
                target_lamports_per_sig: 10_000,
                target_sigs_per_slot: 20_000,
                min_lamports_per_sig: 5_000,
                max_lamports_per_sig: 100_000,
                burn_percent: 100,
            },
            rent: blockzilla_format::WincodeArchiveV2GenesisRentParams {
                lamports_per_byte_year: 3_480,
                exemption_threshold: 2.0,
                burn_percent: 100,
            },
            inflation: blockzilla_format::WincodeArchiveV2GenesisInflationParams {
                initial: 0.0,
                terminal: 0.0,
                taper: 0.0,
                foundation: 0.0,
                foundation_term: 0.0,
                padding: 0.0f64.to_le_bytes(),
            },
            epoch_schedule: blockzilla_format::WincodeArchiveV2GenesisEpochSchedule {
                slots_per_epoch: 432_000,
                leader_schedule_slot_offset: 432_000,
                warmup: false,
                first_normal_epoch: 0,
                first_normal_slot: 0,
            },
            accounts: Vec::new(),
            builtins: Vec::new(),
            reward_pools: Vec::new(),
        };
        let file = GenerationFile {
            name: GENESIS_BIN_FILE.into(),
            size: bytes.len() as u64,
            sha256: hex_lower(&hash),
        };
        let manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "mainnet-beta".into(),
            epoch: 0,
            generation_id: "genesis-test".into(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: 432_000,
            complete: true,
            files: vec![file],
        };
        let loaded = validate_genesis_bin(
            &LocalRangeSource::new(directory.path()),
            &manifest,
            Some(&inline),
        )
        .unwrap();
        assert_eq!(loaded.as_deref(), Some(bytes.as_slice()));

        fs::write(
            directory.path().join(GENESIS_BIN_FILE),
            b"xaunch-genesis-bytes",
        )
        .unwrap();
        let error = validate_genesis_bin(
            &LocalRangeSource::new(directory.path()),
            &manifest,
            Some(&inline),
        )
        .unwrap_err();
        assert!(error.to_string().contains("hash does not match"));
    }

    fn compatibility_test_genesis() -> WincodeArchiveV2Genesis {
        WincodeArchiveV2Genesis {
            genesis_hash: [9; 32],
            genesis_bin_len: 17,
            creation_time_unix: 1,
            cluster_id: 2,
            ticks_per_slot: 64,
            poh_params: blockzilla_format::WincodeArchiveV2GenesisPohParams {
                tick_duration_secs: 0,
                tick_duration_nanos: 400_000_000,
                tick_count: None,
                hashes_per_tick: Some(12_500),
            },
            fees: blockzilla_format::WincodeArchiveV2GenesisFeeParams {
                target_lamports_per_sig: 10_000,
                target_sigs_per_slot: 20_000,
                min_lamports_per_sig: 5_000,
                max_lamports_per_sig: 100_000,
                burn_percent: 100,
            },
            rent: blockzilla_format::WincodeArchiveV2GenesisRentParams {
                lamports_per_byte_year: 3_480,
                exemption_threshold: 2.0,
                burn_percent: 100,
            },
            inflation: blockzilla_format::WincodeArchiveV2GenesisInflationParams {
                initial: 0.0,
                terminal: 0.0,
                taper: 0.0,
                foundation: 0.0,
                foundation_term: 0.0,
                padding: [0; 8],
            },
            epoch_schedule: blockzilla_format::WincodeArchiveV2GenesisEpochSchedule {
                slots_per_epoch: 432_000,
                leader_schedule_slot_offset: 432_000,
                warmup: false,
                first_normal_epoch: 0,
                first_normal_slot: 0,
            },
            accounts: Vec::new(),
            builtins: Vec::new(),
            reward_pools: Vec::new(),
        }
    }

    fn message_header() -> CompactMessageHeader {
        CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        }
    }

    fn write_manifest(root: &Path, complete: bool, omit: Option<&str>) {
        let mut files = Vec::new();
        for name in [
            BLOCKS_FILE,
            BLOCK_INDEX_FILE,
            META_FILE,
            REGISTRY_FILE,
            SIGNATURES_FILE,
        ] {
            if omit == Some(name) {
                continue;
            }
            let bytes = fs::read(root.join(name)).unwrap();
            files.push(GenerationFile {
                name: name.into(),
                size: bytes.len() as u64,
                sha256: hex_lower(&Sha256::digest(&bytes)),
            });
        }
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let marker = crate::wire_profile_marker(profile);
        fs::write(
            root.join(&marker.name),
            crate::wire_profile_marker_bytes(profile),
        )
        .unwrap();
        files.push(marker);
        let metadata_marker = crate::metadata_wire_profile::current_typed_errors_marker();
        fs::write(
            root.join(&metadata_marker.name),
            crate::CURRENT_TYPED_ERRORS_MARKER_BYTES,
        )
        .unwrap();
        files.push(metadata_marker);
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "fixture-generation".into(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: SLOTS_PER_EPOCH,
            complete,
            files,
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        fs::write(
            root.join(GENERATION_MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
    }

    fn write_u32_varint(output: &mut Vec<u8>, mut value: u32) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }
}
