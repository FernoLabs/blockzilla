use std::{
    collections::{HashMap, HashSet},
    io::Read,
    ops::Range,
    sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread,
    time::{Duration, Instant},
};

use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY, ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS,
    ARCHIVE_V2_HOT_INDEX_HEADER_LEN, ARCHIVE_V2_HOT_INDEX_MAGIC, ARCHIVE_V2_HOT_INDEX_ROW_LEN,
    ARCHIVE_V2_HOT_INDEX_VERSION, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_PUBKEY_HOT_SEED_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SHREDDING_FILE,
    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, ArchiveV2HotBlockBlob,
    ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndex, ArchiveV2HotBlockIndexRow,
    ArchiveV2HotMessagePayload, ArchiveV2HotMetaRecord, ArchiveV2HotTxRow, ArchiveV2HotTxRowIter,
    BorrowedArchiveV2HotBlockBlob, BorrowedArchiveV2HotBlockBlobWithoutRewards, CompactMetaV1,
    CompactPubkey, WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS,
    WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY, WINCODE_ARCHIVE_V2_FLAG_LEB128,
    WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
    WincodeArchiveV2Footer, WincodeArchiveV2Genesis, deserialize_archive_v2_hot_block_blob,
    deserialize_archive_v2_hot_block_blob_borrowed_current,
    deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards, wincode_leb128_config,
};
use rayon::prelude::*;
use sha2::{Digest, Sha256};

use crate::{
    Error, Result,
    descriptor::{
        ArchiveDescriptor, ArchiveIdentity, ArchiveSourceBinding, COMPACT_V2_OPTIONAL_OBJECTS,
        COMPACT_V2_REQUIRED_OBJECTS,
    },
    manifest::{
        BLOCK_INDEX_FILE, BLOCKS_FILE, GENERATION_MANIFEST_FILE, GENESIS_BIN_FILE,
        GenerationManifest, META_FILE, OperatorTrustedLocalDescriptor, REGISTRY_FILE,
        REQUIRED_GENERATION_FILES, SIGNATURES_FILE, decode_sha256, hex_lower,
    },
    message_schema::{
        CompactV2MessageSchema, CompactV2MessageSchemaError, decode_compact_v2_message,
        select_compact_v2_message_schema,
    },
    metadata_schema::{
        CompactV2MetadataSchema, CompactV2MetadataSchemaError, decode_compact_v2_metadata,
        select_compact_v2_metadata_schema,
    },
    source::{RangeSource, RangeSourceReader},
};

const DEFAULT_IO_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_BLOCK_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_MAX_COMPRESSED_FRAME_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_PREFETCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_GATEWAY_RANGE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_META_FRAME_BYTES: usize = 256 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_GENESIS_BIN_BYTES: usize = 10_000_000;
const KNOWN_HOT_TX_FLAGS: u32 = (1 << 11) - 1;
const ARCHIVE_V2_HOT_META_GENESIS_TAG: u8 = 1;
const HISTORICAL_EPOCH0_HOT_META_GENESIS_TAG: u8 = 4;

/// Reusable storage for independent borrowed block reads.
///
/// Create one scratch value per worker. Compressed input, the zstd decoder and
/// decompressed output stay with that worker and are reused for every block.
/// A block returned by [`ArchiveReader::read_borrowed_block_reusing`] borrows
/// this storage, so it cannot be queued or retained across the next read.
#[derive(Default)]
pub struct RecycledBlockScratch {
    compressed: Vec<u8>,
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
    decompressed: Vec<u8>,
    stats: RecycledBlockStats,
}

impl RecycledBlockScratch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Measurements accumulated since construction or [`Self::reset_stats`].
    pub fn stats(&self) -> RecycledBlockStats {
        self.stats
    }

    /// Clear measurements without releasing the recycled allocations.
    pub fn reset_stats(&mut self) {
        self.stats = RecycledBlockStats::default();
    }
}

/// Coarse measurements for one worker's recycled borrowed reads.
///
/// Timers surround each source read and each exact zstd/decode operation. No
/// timing is added inside message or metadata loops.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecycledBlockStats {
    pub block_count: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub source_read_wall_time: Duration,
    pub decompress_decode_wall_time: Duration,
    pub compressed_buffer_growths: u64,
    pub decompressed_buffer_growths: u64,
    pub compressed_buffer_capacity: usize,
    pub decompressed_buffer_capacity: usize,
}

pub const MAX_ORDERED_PARALLEL_DECODE_WORKERS: usize = 64;
pub const MAX_ORDERED_PARALLEL_COMPRESSED_BUFFERS: usize = 16;
pub const MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH: usize = 65_536;
/// Hard bound for caller-owned transaction projections retained in one batch.
pub const MAX_ORDERED_PARALLEL_TRANSACTIONS_PER_BATCH: u64 = 65_536;
pub const MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES: usize = 1024 * 1024 * 1024;
pub const MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES: usize = 1024 * 1024 * 1024;

static NEXT_READER_ID: AtomicU64 = AtomicU64::new(1);

/// Bounded resources for monotonic block I/O with parallel borrowed decoding.
///
/// One producer reads frame-aligned ranges in increasing offset order. A
/// private decode pool projects the blocks in parallel, and the coordinator
/// publishes owned projection results in exact block-index order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedParallelBlockConfig {
    /// Target compressed bytes in one frame-aligned read. The reader's
    /// admitted `prefetch_bytes` option is an additional upper bound. One
    /// frame is always admitted when it alone is larger than this target.
    pub compressed_batch_target_bytes: usize,
    /// Maximum declared uncompressed bytes in one batch. One oversized block
    /// is still admitted by itself.
    pub uncompressed_batch_budget_bytes: usize,
    /// Maximum blocks and caller-owned projection results in one batch.
    pub max_blocks_per_batch: usize,
    /// Number of compressed `Vec` tokens recycled between producer and
    /// coordinator. Three permits one fill, one decode, and one queued batch.
    pub compressed_buffer_count: usize,
    /// Threads in the private borrowed-decode and projection pool.
    pub decode_workers: usize,
    /// Largest decompression-buffer capacity retained by each worker between
    /// blocks. Larger buffers are released after their projection completes.
    pub retained_decompressed_bytes_per_worker: usize,
    /// Validate current-schema rewards but do not retain them. An exact archive
    /// converter must keep this false.
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
/// Durations are measured once per range read or batch. No timing is added to
/// message, metadata, or transaction loops.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OrderedParallelBlockStats {
    pub block_count: u64,
    pub batch_count: u64,
    /// Distinct private-pool workers that decoded at least one block.
    pub effective_workers: usize,
    /// Peak simultaneous decode-and-project callbacks.
    pub max_active_workers: usize,
    /// Largest number of blocks projected before one ordered delivery pass.
    pub max_blocks_per_batch: usize,
    /// Largest transaction count projected before one ordered delivery pass.
    pub max_transactions_per_batch: u64,
    pub read_call_count: u64,
    pub compressed_bytes: u64,
    pub producer_read_wall_time: Duration,
    pub coordinator_decode_project_wall_time: Duration,
    /// Ordered sink time; overlaps decode/projection on the next bounded group.
    pub coordinator_consume_wall_time: Duration,
    /// Decoder waits for the ordered consumer to return reusable output storage.
    pub coordinator_wait_for_projection_buffer_time: Duration,
    pub coordinator_wait_to_send_result_time: Duration,
    /// Sum of worker time spent in zstd expansion and the exact outer block
    /// decode. This can exceed wall time because workers run in parallel.
    pub worker_decompress_decode_sum_time: Duration,
    /// Sum of caller projection time after the borrowed block is ready. This
    /// can exceed wall time because workers run in parallel.
    pub worker_projection_sum_time: Duration,
    pub producer_wait_for_free_buffer_time: Duration,
    pub coordinator_wait_for_ready_batch_time: Duration,
    pub max_compressed_batch_bytes: usize,
    pub max_declared_uncompressed_batch_bytes: u64,
    /// Largest decompression allocation retained by any worker after applying
    /// `retained_decompressed_bytes_per_worker`.
    pub max_retained_decompressed_buffer_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashVerification {
    /// Check object presence and exact lengths, then hash every manifest file.
    AllFiles,
    /// Hash the downloaded/cacheable control plane (`registry.bin`, the block
    /// index and publication metadata), while size-checking remote blocks and
    /// signatures. This is the intended HTTP streaming policy. The gateway
    /// must serve an immutable generation over authenticated TLS.
    ControlFiles,
    /// Check object presence and exact lengths only. This is useful when the
    /// transport already verified downloaded immutable files; block decoding
    /// and all structural checks remain enabled.
    SizesOnly,
}

/// How an [`ArchiveReader`] obtained its generation identity.
///
/// A published reader parsed a generation manifest. An operator-trusted
/// reader used caller-supplied identity and structural checks only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveReaderSourceKind {
    PublishedManifest,
    OperatorTrusted,
    ObjectSetBound,
}

/// Identity and inventory used by one open Compact V2 reader.
///
/// The local variant is intentionally not a publication manifest. It contains
/// operator-supplied identity and sizes observed through a pinned local source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveGenerationDescriptor {
    PublishedManifest(GenerationManifest),
    OperatorTrustedLocal(OperatorTrustedLocalDescriptor),
    ObjectSet(ArchiveDescriptor),
}

#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub hash_verification: HashVerification,
    /// Exact first slot for the epoch when the caller has an authoritative
    /// warm-up-aware schedule. When absent, the schema-v1 manifest's legacy
    /// fixed-width epoch window is used for compatibility.
    pub epoch_first_slot: Option<u64>,
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
            epoch_first_slot: None,
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
}

#[derive(Debug, Clone)]
pub struct CompiledPubkeyFilter {
    reader_id: u64,
    binding: GenerationBinding,
    registry_ids: HashSet<u32>,
    raw_pubkeys: HashSet<[u8; 32]>,
    resolved_ids: HashMap<[u8; 32], u32>,
}

impl CompiledPubkeyFilter {
    pub fn reader_id(&self) -> u64 {
        self.reader_id
    }

    pub fn binding(&self) -> GenerationBinding {
        self.binding
    }

    pub fn pubkey_count(&self) -> usize {
        self.raw_pubkeys.len()
    }

    pub fn registry_id_count(&self) -> usize {
        self.registry_ids.len()
    }

    /// Return the generation-local registry ID for one requested pubkey.
    pub fn registry_id_for(&self, pubkey: &[u8; 32]) -> Option<u32> {
        self.resolved_ids.get(pubkey).copied()
    }

    /// Match one compact reference against one exact requested pubkey.
    ///
    /// The reference is valid only for the generation bound to this filter.
    pub fn matches_reference(&self, reference: &CompactPubkey, pubkey: &[u8; 32]) -> bool {
        match reference {
            CompactPubkey::Id(id) => self.registry_id_for(pubkey) == Some(*id),
            CompactPubkey::Raw(raw) => raw == pubkey && self.raw_pubkeys.contains(pubkey),
        }
    }

    /// Classify one reference against all pubkeys compiled into this filter.
    pub fn classify_reference(
        &self,
        reference: &CompactPubkey,
        registry_entries: u32,
    ) -> PubkeyReferenceMatch {
        match reference {
            CompactPubkey::Id(id) if *id == 0 || *id > registry_entries => {
                PubkeyReferenceMatch::InvalidRegistryReference
            }
            CompactPubkey::Id(id) if self.registry_ids.contains(id) => PubkeyReferenceMatch::Match,
            CompactPubkey::Raw(pubkey) if self.raw_pubkeys.contains(pubkey) => {
                PubkeyReferenceMatch::Match
            }
            CompactPubkey::Id(_) | CompactPubkey::Raw(_) => PubkeyReferenceMatch::NoMatch,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubkeyReferenceMatch {
    Match,
    NoMatch,
    InvalidRegistryReference,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorIndeterminateReason {
    RawTransactionFallback,
    RawMetadataFallback,
    MessageUnavailable,
    MetadataUnavailable,
    InvalidRegistryReference,
    InvalidAccountReference,
    TokenMintUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorOutcome<T> {
    Match(T),
    NoMatch,
    Indeterminate(SelectorIndeterminateReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgramInvocationMatch {
    pub direct_count: u32,
    pub cpi_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenBalanceMatch {
    pub pre_count: u32,
    pub post_count: u32,
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
    pub transactions: Vec<ScannedTransaction>,
}

#[derive(Debug)]
pub struct DecodedBlock {
    pub index_row: ArchiveV2HotBlockIndexRow,
    pub block: ArchiveV2HotBlockBlob,
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
}

#[derive(Debug)]
enum BorrowedDecodedBlockPayload<'a> {
    Current(BorrowedArchiveV2HotBlockBlob<'a>),
    CurrentWithoutRewards(BorrowedArchiveV2HotBlockBlobWithoutRewards<'a>),
    OwnedFallback(ArchiveV2HotBlockBlob),
}

impl BorrowedDecodedBlock<'_> {
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

    /// Return the exact encoded `Option<ArchiveV2HotRewards>` field from the source block.
    ///
    /// This is available only when the block used the current-schema reward-discarding decoder.
    /// Decoded rewards and historical owned fallbacks do not retain their original wire slice.
    pub fn rewards_field_bytes(&self) -> Result<&[u8]> {
        match &self.block {
            BorrowedDecodedBlockPayload::CurrentWithoutRewards(block) => {
                Ok(block.rewards_field_bytes())
            }
            BorrowedDecodedBlockPayload::Current(_) => Err(Error::InvalidBlock {
                slot: self.index_row.slot,
                message: "exact reward field bytes are unavailable after decoding rewards".into(),
            }),
            BorrowedDecodedBlockPayload::OwnedFallback(_) => Err(Error::InvalidBlock {
                slot: self.index_row.slot,
                message: "exact reward field bytes are unavailable for an owned fallback".into(),
            }),
        }
    }

    /// Whether this block required the allocation-preserving historical-schema decoder.
    #[inline]
    pub fn uses_owned_fallback(&self) -> bool {
        matches!(&self.block, BorrowedDecodedBlockPayload::OwnedFallback(_))
    }

    /// Convert this lent block into the existing owned block representation.
    ///
    /// For the current schema, the decoded header and rewards move directly
    /// into the result. Only the compact transaction rows and the two borrowed
    /// byte lanes are copied. A historical owned fallback passes through with
    /// no copy or second decode. A reward-discarding view cannot produce an
    /// exact owned block and is rejected.
    pub fn into_owned(self) -> Result<DecodedBlock> {
        let index_row = self.index_row;
        let block = match self.block {
            BorrowedDecodedBlockPayload::Current(block) => {
                let tx_rows = block.tx_rows().collect();
                let message_bytes = block.message_bytes.to_vec();
                let metadata_bytes = block.metadata_bytes.to_vec();
                ArchiveV2HotBlockBlob {
                    header: block.header,
                    tx_count: block.tx_count,
                    tx_rows,
                    message_bytes,
                    metadata_bytes,
                }
            }
            BorrowedDecodedBlockPayload::CurrentWithoutRewards(_) => {
                return Err(Error::InvalidBlock {
                    slot: index_row.slot,
                    message: "cannot create an exact owned block after rewards were discarded"
                        .into(),
                });
            }
            BorrowedDecodedBlockPayload::OwnedFallback(block) => block,
        };
        Ok(DecodedBlock { index_row, block })
    }
}

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
}

/// Read the first Compact V2 block slot without opening the full archive.
///
/// This small structural probe is useful when an application selects a known
/// epoch schedule from the first indexed slot. The full reader still validates
/// the complete index before it exposes blocks.
pub fn compact_v2_first_slot<S: RangeSource>(source: &S) -> Result<Option<u64>> {
    let index_size = source
        .size(BLOCK_INDEX_FILE)?
        .ok_or_else(|| Error::MissingLocalFile(BLOCK_INDEX_FILE.to_owned()))?;
    if index_size < ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64 {
        return Err(Error::InvalidIndex("index header is truncated".into()));
    }
    let header = source.read_range(BLOCK_INDEX_FILE, 0, ARCHIVE_V2_HOT_INDEX_HEADER_LEN)?;
    if &header[..8] != ARCHIVE_V2_HOT_INDEX_MAGIC {
        return Err(Error::InvalidIndex("bad index magic".into()));
    }
    if u16::from_le_bytes(header[8..10].try_into().expect("two bytes"))
        != ARCHIVE_V2_HOT_INDEX_VERSION
    {
        return Err(Error::InvalidIndex("unsupported index version".into()));
    }
    let row_count = u64::from_le_bytes(header[12..20].try_into().expect("eight bytes"));
    if row_count == 0 {
        return Ok(None);
    }
    let minimum = (ARCHIVE_V2_HOT_INDEX_HEADER_LEN + 12) as u64;
    if index_size < minimum {
        return Err(Error::InvalidIndex("first index row is truncated".into()));
    }
    let prefix = source.read_range(BLOCK_INDEX_FILE, ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64, 12)?;
    Ok(Some(u64::from_le_bytes(
        prefix[4..12].try_into().expect("eight bytes"),
    )))
}

#[derive(Debug, Clone, Copy)]
struct SelectedCompactV2Schemas {
    message: CompactV2MessageSchema,
    metadata: CompactV2MetadataSchema,
}

#[derive(Debug)]
pub struct ArchiveReader<S> {
    source: S,
    generation: ArchiveGenerationDescriptor,
    reader_id: u64,
    index: ArchiveV2HotBlockIndex,
    genesis: Option<WincodeArchiveV2Genesis>,
    genesis_bin: Option<Vec<u8>>,
    metadata_footer: WincodeArchiveV2Footer,
    binding: GenerationBinding,
    registry_entries: u32,
    total_signatures: u64,
    signatures_available: bool,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    source_kind: ArchiveReaderSourceKind,
    options: OpenOptions,
}

impl<S: RangeSource> ArchiveReader<S> {
    /// Open a pinned local Compact V2 object set with the current grammars.
    ///
    /// Admission uses the fixed format object list, exact sizes, structural
    /// validation, and the source's pinned regular-file identity. It does not
    /// read a publication file or hash archive payloads.
    pub fn open_pinned(
        source: S,
        identity: ArchiveIdentity,
        mut options: OpenOptions,
    ) -> Result<Self> {
        options.hash_verification = HashVerification::SizesOnly;
        Self::open_pinned_with_schemas(
            source,
            identity,
            options,
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::CurrentTypedError,
        )
    }

    /// Open a pinned local object set with explicit Compact V2 grammars.
    pub fn open_pinned_with_schemas(
        source: S,
        identity: ArchiveIdentity,
        mut options: OpenOptions,
        message_schema: CompactV2MessageSchema,
        metadata_schema: CompactV2MetadataSchema,
    ) -> Result<Self> {
        options.hash_verification = HashVerification::SizesOnly;
        let descriptor =
            discover_archive_descriptor(&source, identity, ArchiveSourceBinding::PinnedLocal)?;
        let validated = validate_archive_descriptor_structure(&source, &descriptor, &options)?;
        let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            source,
            reader_id,
            generation: ArchiveGenerationDescriptor::ObjectSet(descriptor),
            index: validated.index,
            genesis: validated.genesis,
            genesis_bin: validated.genesis_bin,
            metadata_footer: validated.metadata_footer,
            binding: validated.binding,
            registry_entries: validated.registry_entries,
            total_signatures: validated.total_signatures,
            signatures_available: validated.signatures_available,
            message_schema,
            metadata_schema,
            source_kind: ArchiveReaderSourceKind::OperatorTrusted,
            options,
        })
    }

    pub fn open(source: S) -> Result<Self> {
        Self::open_with_options(source, OpenOptions::default())
    }

    pub fn open_with_options(source: S, options: OpenOptions) -> Result<Self> {
        let manifest_bytes =
            source.read_all_bounded(GENERATION_MANIFEST_FILE, MAX_MANIFEST_BYTES)?;
        let manifest = GenerationManifest::parse(&manifest_bytes)?;
        let message_schema = select_compact_v2_message_schema(&source, &manifest)?;
        let metadata_schema = select_compact_v2_metadata_schema(&source, &manifest)?;
        let validated = validate_generation_structure(&source, &manifest, &options)?;

        let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            source,
            reader_id,
            generation: ArchiveGenerationDescriptor::PublishedManifest(manifest),
            index: validated.index,
            genesis: validated.genesis,
            genesis_bin: validated.genesis_bin,
            metadata_footer: validated.metadata_footer,
            binding: validated.binding,
            registry_entries: validated.registry_entries,
            total_signatures: validated.total_signatures,
            signatures_available: validated.signatures_available,
            message_schema,
            metadata_schema,
            source_kind: ArchiveReaderSourceKind::PublishedManifest,
            options,
        })
    }

    /// Open a generation without a published `archive-v2-generation.json` and
    /// without hashing any file content, for a source the caller already
    /// trusts (e.g. a local NAS directory).
    ///
    /// This does not load or synthesize a generation manifest. `identity` is
    /// taken as given and is not verified against file content. The reader
    /// builds a separate in-memory local descriptor from exact file sizes.
    /// Structural validation (index/metadata bounds, registry shape, footer
    /// totals, signature length, and epoch geometry) still runs. Requires
    /// `options.hash_verification == HashVerification::SizesOnly` because the
    /// local descriptor has no content-digest fields.
    pub fn open_trusted(
        source: S,
        identity: crate::manifest::TrustedGenerationIdentity,
        options: OpenOptions,
    ) -> Result<Self> {
        Self::open_trusted_with_schemas(
            source,
            identity,
            options,
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::CurrentTypedError,
        )
    }

    /// Open a trusted unpublished generation with explicit wire grammars.
    ///
    /// Use this method for historical local generations. The caller selects
    /// one message grammar and one metadata grammar for the full generation;
    /// the reader never infers a grammar from individual records.
    pub fn open_trusted_with_schemas(
        source: S,
        identity: crate::manifest::TrustedGenerationIdentity,
        options: OpenOptions,
        message_schema: CompactV2MessageSchema,
        metadata_schema: CompactV2MetadataSchema,
    ) -> Result<Self> {
        if options.hash_verification != HashVerification::SizesOnly {
            return Err(Error::InvalidLocalDescriptor(
                "open_trusted requires HashVerification::SizesOnly".into(),
            ));
        }

        let mut files = Vec::with_capacity(REQUIRED_GENERATION_FILES.len() + 5);
        for name in REQUIRED_GENERATION_FILES {
            let size = source
                .size(name)?
                .ok_or_else(|| Error::MissingLocalFile(name.to_owned()))?;
            files.push((name.to_owned(), size));
        }
        for name in [
            SIGNATURES_FILE,
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
            ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
            ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
            ARCHIVE_V2_POH_FILE,
            ARCHIVE_V2_SHREDDING_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
            ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
            ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
            ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
        ] {
            if files.iter().any(|(present, _)| present == name) {
                continue;
            }
            if let Some(size) = source.size(name)? {
                files.push((name.to_owned(), size));
            }
        }
        if identity.epoch == 0
            && let Some(size) = source.size(GENESIS_BIN_FILE)?
        {
            files.push((GENESIS_BIN_FILE.to_owned(), size));
        }

        let descriptor = OperatorTrustedLocalDescriptor::new(identity, files)?;
        let validated = validate_operator_trusted_local_structure(&source, &descriptor, &options)?;

        let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            source,
            reader_id,
            generation: ArchiveGenerationDescriptor::OperatorTrustedLocal(descriptor),
            index: validated.index,
            genesis: validated.genesis,
            genesis_bin: validated.genesis_bin,
            metadata_footer: validated.metadata_footer,
            binding: validated.binding,
            registry_entries: validated.registry_entries,
            total_signatures: validated.total_signatures,
            signatures_available: validated.signatures_available,
            message_schema,
            metadata_schema,
            source_kind: ArchiveReaderSourceKind::OperatorTrusted,
            options,
        })
    }

    /// Open a format-defined object set whose HTTP lengths and strong ETags
    /// were pinned by the caller.
    ///
    /// `object_set_id` is an opaque label for those validators. No object is
    /// read only to compute a content hash.
    pub fn open_object_set(
        source: S,
        identity: ArchiveIdentity,
        object_set_id: impl Into<String>,
        options: OpenOptions,
    ) -> Result<Self> {
        Self::open_object_set_with_schemas(
            source,
            identity,
            object_set_id,
            options,
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::CurrentTypedError,
        )
    }

    /// Open a strongly bound object set with explicit Compact V2 grammars.
    pub fn open_object_set_with_schemas(
        source: S,
        identity: ArchiveIdentity,
        object_set_id: impl Into<String>,
        options: OpenOptions,
        message_schema: CompactV2MessageSchema,
        metadata_schema: CompactV2MetadataSchema,
    ) -> Result<Self> {
        if options.hash_verification != HashVerification::SizesOnly {
            return Err(Error::InvalidLocalDescriptor(
                "object-set readers require size-only structural admission".into(),
            ));
        }
        let descriptor = discover_archive_descriptor(
            &source,
            identity,
            ArchiveSourceBinding::StrongEtags {
                object_set_id: object_set_id.into(),
            },
        )?;
        let validated = validate_archive_descriptor_structure(&source, &descriptor, &options)?;

        let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            source,
            reader_id,
            generation: ArchiveGenerationDescriptor::ObjectSet(descriptor),
            index: validated.index,
            genesis: validated.genesis,
            genesis_bin: validated.genesis_bin,
            metadata_footer: validated.metadata_footer,
            binding: validated.binding,
            registry_entries: validated.registry_entries,
            total_signatures: validated.total_signatures,
            signatures_available: validated.signatures_available,
            message_schema,
            metadata_schema,
            source_kind: ArchiveReaderSourceKind::ObjectSetBound,
            options,
        })
    }

    pub fn source(&self) -> &S {
        &self.source
    }

    /// Return the opaque runtime identifier for this reader instance.
    pub const fn reader_id(&self) -> u64 {
        self.reader_id
    }

    /// Return the published manifest.
    ///
    /// This legacy accessor is only valid for a published reader. New code
    /// that can receive a local reader must use [`Self::published_manifest`]
    /// or the common descriptor accessors.
    pub fn manifest(&self) -> &GenerationManifest {
        self.published_manifest()
            .expect("operator-trusted local readers have no published manifest")
    }

    pub fn published_manifest(&self) -> Option<&GenerationManifest> {
        match &self.generation {
            ArchiveGenerationDescriptor::PublishedManifest(manifest) => Some(manifest),
            ArchiveGenerationDescriptor::OperatorTrustedLocal(_)
            | ArchiveGenerationDescriptor::ObjectSet(_) => None,
        }
    }

    pub fn local_descriptor(&self) -> Option<&OperatorTrustedLocalDescriptor> {
        match &self.generation {
            ArchiveGenerationDescriptor::PublishedManifest(_) => None,
            ArchiveGenerationDescriptor::OperatorTrustedLocal(descriptor) => Some(descriptor),
            ArchiveGenerationDescriptor::ObjectSet(_) => None,
        }
    }

    pub fn archive_descriptor(&self) -> Option<&ArchiveDescriptor> {
        match &self.generation {
            ArchiveGenerationDescriptor::ObjectSet(descriptor) => Some(descriptor),
            ArchiveGenerationDescriptor::PublishedManifest(_)
            | ArchiveGenerationDescriptor::OperatorTrustedLocal(_) => None,
        }
    }

    pub const fn generation_descriptor(&self) -> &ArchiveGenerationDescriptor {
        &self.generation
    }

    pub fn cluster_id(&self) -> &str {
        match &self.generation {
            ArchiveGenerationDescriptor::PublishedManifest(manifest) => &manifest.cluster_id,
            ArchiveGenerationDescriptor::OperatorTrustedLocal(descriptor) => {
                &descriptor.identity.cluster_id
            }
            ArchiveGenerationDescriptor::ObjectSet(descriptor) => &descriptor.identity.cluster_id,
        }
    }

    pub fn epoch(&self) -> u64 {
        match &self.generation {
            ArchiveGenerationDescriptor::PublishedManifest(manifest) => manifest.epoch,
            ArchiveGenerationDescriptor::OperatorTrustedLocal(descriptor) => {
                descriptor.identity.epoch
            }
            ArchiveGenerationDescriptor::ObjectSet(descriptor) => descriptor.identity.epoch,
        }
    }

    pub fn generation_label(&self) -> &str {
        match &self.generation {
            ArchiveGenerationDescriptor::PublishedManifest(manifest) => &manifest.generation_id,
            ArchiveGenerationDescriptor::OperatorTrustedLocal(descriptor) => {
                &descriptor.identity.generation_id
            }
            ArchiveGenerationDescriptor::ObjectSet(descriptor) => {
                &descriptor.identity.generation_id
            }
        }
    }

    pub fn slots_per_epoch(&self) -> u64 {
        match &self.generation {
            ArchiveGenerationDescriptor::PublishedManifest(manifest) => manifest.slots_per_epoch,
            ArchiveGenerationDescriptor::OperatorTrustedLocal(descriptor) => {
                descriptor.identity.slots_per_epoch
            }
            ArchiveGenerationDescriptor::ObjectSet(descriptor) => {
                descriptor.identity.slots_per_epoch
            }
        }
    }

    pub fn file_size(&self, name: &str) -> Option<u64> {
        match &self.generation {
            ArchiveGenerationDescriptor::PublishedManifest(manifest) => {
                manifest.file(name).map(|file| file.size)
            }
            ArchiveGenerationDescriptor::OperatorTrustedLocal(descriptor) => {
                descriptor.file(name).map(|file| file.size)
            }
            ArchiveGenerationDescriptor::ObjectSet(descriptor) => {
                descriptor.object(name).map(|object| object.size)
            }
        }
    }

    pub fn required_file_size(&self, name: &str) -> Result<u64> {
        match &self.generation {
            ArchiveGenerationDescriptor::PublishedManifest(manifest) => {
                Ok(manifest.required_file(name)?.size)
            }
            ArchiveGenerationDescriptor::OperatorTrustedLocal(descriptor) => {
                Ok(descriptor.required_file(name)?.size)
            }
            ArchiveGenerationDescriptor::ObjectSet(descriptor) => {
                Ok(descriptor.required_object(name)?.size)
            }
        }
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

    pub fn registry_entries(&self) -> u32 {
        self.registry_entries
    }

    pub fn total_signatures(&self) -> u64 {
        self.total_signatures
    }

    pub fn signatures_available(&self) -> bool {
        self.signatures_available
    }

    pub fn message_schema(&self) -> CompactV2MessageSchema {
        self.message_schema
    }

    pub fn metadata_schema(&self) -> CompactV2MetadataSchema {
        self.metadata_schema
    }

    pub const fn source_kind(&self) -> ArchiveReaderSourceKind {
        self.source_kind
    }

    /// Decode one complete transaction message with this generation's grammar.
    pub fn decode_message(
        &self,
        bytes: &[u8],
    ) -> std::result::Result<ArchiveV2HotMessagePayload, CompactV2MessageSchemaError> {
        decode_compact_v2_message(self.message_schema, bytes)
    }

    /// Decode one complete transaction metadata record with this generation's grammar.
    pub fn decode_metadata(
        &self,
        bytes: &[u8],
    ) -> std::result::Result<CompactMetaV1, CompactV2MetadataSchemaError> {
        decode_compact_v2_metadata(self.metadata_schema, bytes)
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
        let mut resolved_ids = HashMap::with_capacity(raw_pubkeys.len());
        if !raw_pubkeys.is_empty() && self.registry_entries != 0 {
            let mut offset = 0u64;
            let registry_size = self.required_file_size(REGISTRY_FILE)?;
            let chunk_size = (self.options.io_chunk_size / 32).max(1) * 32;
            while offset < registry_size {
                let length = usize::try_from((registry_size - offset).min(chunk_size as u64))
                    .expect("registry chunk is bounded by usize");
                let bytes = self.source.read_range(REGISTRY_FILE, offset, length)?;
                if bytes.len() % 32 != 0 {
                    return Err(Error::InvalidRegistry(
                        "range source split registry on a partial pubkey".into(),
                    ));
                }
                for (position, key_bytes) in bytes.chunks_exact(32).enumerate() {
                    let mut key = [0u8; 32];
                    key.copy_from_slice(key_bytes);
                    if raw_pubkeys.contains(&key) {
                        let zero_based = offset / 32 + position as u64;
                        let id = u32::try_from(zero_based + 1)
                            .map_err(|_| Error::InvalidRegistry("registry id overflow".into()))?;
                        if resolved_ids.insert(key, id).is_some() {
                            return Err(Error::InvalidRegistry(
                                "a requested pubkey occurs more than once in registry.bin".into(),
                            ));
                        }
                        registry_ids.insert(id);
                    }
                }
                offset += length as u64;
            }
        }
        Ok(CompiledPubkeyFilter {
            reader_id: self.reader_id,
            binding: self.binding,
            registry_ids,
            raw_pubkeys,
            resolved_ids,
        })
    }

    /// Resolve one compact pubkey with one exact bounded registry read.
    pub fn resolve_pubkey(&self, reference: &CompactPubkey) -> Result<[u8; 32]> {
        match reference {
            CompactPubkey::Raw(pubkey) => Ok(*pubkey),
            CompactPubkey::Id(id) => {
                if *id == 0 || *id > self.registry_entries {
                    return Err(Error::InvalidRegistry(format!(
                        "registry id {id} is outside 1..={}",
                        self.registry_entries
                    )));
                }
                let offset = u64::from(*id - 1)
                    .checked_mul(32)
                    .ok_or(Error::Overflow("registry pubkey offset"))?;
                let bytes = self.source.read_range(REGISTRY_FILE, offset, 32)?;
                let mut pubkey = [0u8; 32];
                pubkey.copy_from_slice(&bytes);
                Ok(pubkey)
            }
        }
    }

    /// Select transactions that invoke a requested program directly or by CPI.
    ///
    /// A required raw or missing component returns `Indeterminate`; it never
    /// becomes a silent non-match.
    pub fn select_program_invocations(
        &self,
        filter: &CompiledPubkeyFilter,
        row: &ArchiveV2HotTxRow,
        message: Option<&ArchiveV2HotMessagePayload>,
        metadata: Option<&CompactMetaV1>,
    ) -> Result<SelectorOutcome<ProgramInvocationMatch>> {
        self.ensure_filter_binding(filter)?;
        Ok(select_program_invocations(
            filter,
            self.registry_entries,
            row,
            message,
            metadata,
        ))
    }

    /// Select transactions with pre- or post-token balances for a mint.
    ///
    /// This selector uses recorded token-balance mints. It does not infer a
    /// mint from instruction bytes or token account addresses.
    pub fn select_token_balances(
        &self,
        filter: &CompiledPubkeyFilter,
        row: &ArchiveV2HotTxRow,
        metadata: Option<&CompactMetaV1>,
    ) -> Result<SelectorOutcome<TokenBalanceMatch>> {
        self.ensure_filter_binding(filter)?;
        Ok(select_token_balances(
            filter,
            self.registry_entries,
            row,
            metadata,
        ))
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
            io_stats: BorrowedBlockStreamIoStats::default(),
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
            io_stats: BorrowedBlockStreamIoStats::default(),
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
            io_stats: BorrowedBlockStreamIoStats::default(),
        })
    }

    /// Read, decompress, validate and lend one indexed block with worker-local
    /// recycled buffers.
    ///
    /// This is the random-access companion to [`Self::borrowed_blocks_range`].
    /// It is intended for an existing ordered worker pipeline: give each
    /// worker one [`RecycledBlockScratch`], then transform the borrowed block
    /// into an owned result before the worker returns. The returned block
    /// cannot outlive `scratch`, which prevents a borrowed source lane from
    /// entering an ordered result queue. Historical outer schemas can still
    /// use the SDK's owned compatibility fallback; a converter that admits
    /// only the current outer schema must reject
    /// [`BorrowedDecodedBlock::uses_owned_fallback`].
    pub fn read_borrowed_block_reusing<'scratch>(
        &self,
        row_number: usize,
        scratch: &'scratch mut RecycledBlockScratch,
        discard_rewards: bool,
    ) -> Result<BorrowedDecodedBlock<'scratch>> {
        let row = *self.index.rows.get(row_number).ok_or_else(|| {
            Error::InvalidIndex(format!("block row {row_number} is out of bounds"))
        })?;
        let RecycledBlockScratch {
            compressed,
            decompressor,
            decompressed,
            stats,
        } = scratch;

        let old_compressed_capacity = compressed.capacity();
        let read_started = Instant::now();
        self.source.read_range_into(
            BLOCKS_FILE,
            row.compressed_offset,
            row.compressed_len as usize,
            compressed,
        )?;
        stats.source_read_wall_time = stats
            .source_read_wall_time
            .saturating_add(read_started.elapsed());
        if compressed.capacity() > old_compressed_capacity {
            stats.compressed_buffer_growths = stats
                .compressed_buffer_growths
                .checked_add(1)
                .ok_or(Error::Overflow("compressed buffer growth count"))?;
        }
        stats.compressed_buffer_capacity = compressed.capacity();

        if decompressor.is_none() {
            *decompressor =
                Some(
                    zstd::bulk::Decompressor::new().map_err(|error| Error::DecodeBlock {
                        slot: row.slot,
                        message: format!("create zstd decompressor: {error}"),
                    })?,
                );
        }
        let old_decompressed_capacity = decompressed.capacity();
        let expected_decompressed_capacity = row.uncompressed_len as usize;
        if old_decompressed_capacity < expected_decompressed_capacity {
            decompressed.reserve_exact(expected_decompressed_capacity);
            stats.decompressed_buffer_growths = stats
                .decompressed_buffer_growths
                .checked_add(1)
                .ok_or(Error::Overflow("decompressed buffer growth count"))?;
        }
        stats.decompressed_buffer_capacity = decompressed.capacity();
        let decode_started = Instant::now();
        let block = self.decode_compressed_block_borrowed_reusing(
            row,
            compressed,
            decompressor
                .as_mut()
                .expect("decompressor was initialized above"),
            decompressed,
            discard_rewards,
        )?;
        stats.decompress_decode_wall_time = stats
            .decompress_decode_wall_time
            .saturating_add(decode_started.elapsed());
        stats.block_count = stats
            .block_count
            .checked_add(1)
            .ok_or(Error::Overflow("recycled block count"))?;
        stats.compressed_bytes = stats
            .compressed_bytes
            .checked_add(u64::from(row.compressed_len))
            .ok_or(Error::Overflow("recycled compressed byte count"))?;
        stats.uncompressed_bytes = stats
            .uncompressed_bytes
            .checked_add(u64::from(row.uncompressed_len))
            .ok_or(Error::Overflow("recycled uncompressed byte count"))?;
        Ok(block)
    }

    /// Read blocks through one monotonic I/O stream, project borrowed blocks
    /// in a private parallel pool, and publish owned results in exact index
    /// order.
    ///
    /// `make_worker_state` creates one caller state for each decode worker and
    /// can fail before any block read starts.
    /// `project` runs in parallel with that state and a block backed by the
    /// worker's recycled decompression buffer. The block cannot escape the
    /// callback. `Output` must own all data that `consume_ordered` needs.
    /// Outputs for at most one bounded batch remain live before ordered
    /// delivery.
    ///
    /// A projection error is selected by row order, not worker completion
    /// order. No later result is delivered after that error.
    pub fn process_borrowed_blocks_parallel_ordered<
        WorkerState,
        Output,
        E,
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
    ) -> std::result::Result<OrderedParallelBlockStats, E>
    where
        WorkerState: Send,
        Output: Send,
        E: From<Error> + Send,
        MakeWorkerState: FnMut(usize) -> std::result::Result<WorkerState, E>,
        Project: for<'block> Fn(
                &mut WorkerState,
                usize,
                BorrowedDecodedBlock<'block>,
            ) -> std::result::Result<Output, E>
            + Send
            + Sync,
        Consume: FnMut(usize, Output) -> std::result::Result<(), E>,
    {
        let row_count = self.index.rows.len();
        if range.start > range.end || range.end > row_count {
            return Err(E::from(Error::InvalidIndex(format!(
                "block row range {}..{} is outside 0..{row_count}",
                range.start, range.end,
            ))));
        }
        validate_ordered_parallel_config(config).map_err(E::from)?;
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
        )
        .map_err(E::from)?;
        let decode_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.decode_workers)
            .thread_name(|index| format!("blockzilla-block-decode-{index}"))
            .build()
            .map_err(|error| {
                E::from(Error::InvalidManifest(format!(
                    "cannot create ordered parallel block decode pool: {error}"
                )))
            })?;
        let workers: Vec<_> = (0..config.decode_workers)
            .map(|worker| {
                make_worker_state(worker).map(|caller| {
                    Mutex::new(Some(OrderedParallelWorker {
                        decompressor: None,
                        decompressed: Vec::new(),
                        caller,
                        used: false,
                        max_retained_decompressed_buffer_bytes: 0,
                        decompress_decode_sum_time: Duration::ZERO,
                        projection_sum_time: Duration::ZERO,
                    }))
                })
            })
            .collect::<std::result::Result<_, E>>()?;

        let (free_sender, free_receiver) = sync_channel(config.compressed_buffer_count);
        for _ in 0..config.compressed_buffer_count {
            free_sender
                .send(Vec::new())
                .expect("the new recycled-buffer channel has a receiver");
        }
        let (ready_sender, ready_receiver) = sync_channel(config.compressed_buffer_count);
        let active_workers = AtomicUsize::new(0);
        let max_active_workers = AtomicUsize::new(0);

        thread::scope(|scope| {
            let producer = scope.spawn(|| {
                produce_ordered_compressed_batches(self, &plans, free_receiver, ready_sender)
            });
            let (decoded_sender, decoded_receiver) = sync_channel(1);
            let (projection_free_sender, projection_free_receiver) = sync_channel(2);
            let _ = projection_free_sender.send(Vec::new());
            let _ = projection_free_sender.send(Vec::new());
            let plans = &plans;
            let decoder = scope.spawn(move || {
                let mut coordinator: OrderedParallelCoordinator<E> =
                    OrderedParallelCoordinator::default();

                'batches: for expected in plans {
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
                        coordinator.error = Some(E::from(Error::InvalidIndex(format!(
                            "ordered block producer returned rows {}..{}, expected {}..{}",
                            ready.plan.row_start,
                            ready.plan.row_end,
                            expected.row_start,
                            expected.row_end,
                        ))));
                        break;
                    }

                    coordinator.stats.batch_count =
                        match coordinator.stats.batch_count.checked_add(1) {
                            Some(value) => value,
                            None => {
                                coordinator.error =
                                    Some(E::from(Error::Overflow("parallel batch count")));
                                break;
                            }
                        };
                    let batch_blocks = match u64::try_from(expected.row_end - expected.row_start) {
                        Ok(value) => value,
                        Err(_) => {
                            coordinator.error =
                                Some(E::from(Error::Overflow("parallel block count")));
                            break;
                        }
                    };
                    coordinator.stats.block_count =
                        match coordinator.stats.block_count.checked_add(batch_blocks) {
                            Some(value) => value,
                            None => {
                                coordinator.error =
                                    Some(E::from(Error::Overflow("parallel block count")));
                                break;
                            }
                        };

                    for wave_start in (ready.plan.row_start..ready.plan.row_end)
                        .step_by(config.decode_workers.saturating_mul(4))
                    {
                        let wave_end = wave_start
                            .saturating_add(config.decode_workers.saturating_mul(4))
                            .min(ready.plan.row_end);
                        let wait_started = Instant::now();
                        let Ok(mut projected) = projection_free_receiver.recv() else {
                            break 'batches;
                        };
                        coordinator
                            .stats
                            .coordinator_wait_for_projection_buffer_time += wait_started.elapsed();
                        let decode_started = Instant::now();
                        decode_pool.install(|| {
                            self.index.rows[wave_start..wave_end]
                                .par_iter()
                                .enumerate()
                                .map(|(wave_row, row)| {
                                    let row_number = wave_start + wave_row;
                                    let relative_offset = row
                                        .compressed_offset
                                        .checked_sub(ready.plan.compressed_offset)
                                        .ok_or_else(|| {
                                            Error::InvalidIndex(
                                                "parallel block frame offset underflow".into(),
                                            )
                                        })?;
                                    let relative_offset = usize::try_from(relative_offset)
                                        .map_err(|_| {
                                            Error::Overflow("parallel block frame offset")
                                        })?;
                                    let frame_end = relative_offset
                                        .checked_add(row.compressed_len as usize)
                                        .ok_or(Error::Overflow("parallel block frame range"))?;
                                    let compressed =
                                        ready.bytes.get(relative_offset..frame_end).ok_or_else(
                                            || {
                                                Error::InvalidIndex(
                                            "parallel block frame is outside its read batch".into(),
                                        )
                                            },
                                        )?;
                                    let worker_number =
                                        rayon::current_thread_index().ok_or_else(|| {
                                            Error::InvalidIndex(
                                                "parallel block task ran outside its decode pool"
                                                    .into(),
                                            )
                                        })?;
                                    let mut worker_guard =
                                        workers[worker_number].lock().map_err(|_| {
                                            Error::InvalidIndex(
                                                "parallel block worker state is poisoned".into(),
                                            )
                                        })?;
                                    let mut worker = worker_guard.take().ok_or_else(|| {
                                        Error::InvalidIndex(
                                            "parallel block worker was re-entered by nested work"
                                                .into(),
                                        )
                                    })?;
                                    drop(worker_guard);
                                    worker.used = true;
                                    let active = active_workers.fetch_add(1, Ordering::AcqRel) + 1;
                                    max_active_workers.fetch_max(active, Ordering::Relaxed);
                                    let result = worker.decode_and_project(
                                        self,
                                        *row,
                                        compressed,
                                        row_number,
                                        config.discard_rewards,
                                        config.retained_decompressed_bytes_per_worker,
                                        &project,
                                    );
                                    let previous = active_workers.fetch_sub(1, Ordering::AcqRel);
                                    debug_assert!(previous > 0);
                                    let mut worker_guard =
                                        workers[worker_number].lock().map_err(|_| {
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
                        coordinator.stats.max_blocks_per_batch = coordinator
                            .stats
                            .max_blocks_per_batch
                            .max(wave_end - wave_start);
                        let wave_transactions = self.index.rows[wave_start..wave_end]
                            .iter()
                            .try_fold(0_u64, |total, row| {
                                total
                                    .checked_add(u64::from(row.tx_count))
                                    .ok_or(Error::Overflow("parallel transaction wave count"))
                            })
                            .map_err(E::from)?;
                        coordinator.stats.max_transactions_per_batch = coordinator
                            .stats
                            .max_transactions_per_batch
                            .max(wave_transactions);

                        let wait_started = Instant::now();
                        let sent = decoded_sender.send((wave_start, projected));
                        coordinator.stats.coordinator_wait_to_send_result_time +=
                            wait_started.elapsed();
                        if sent.is_err() {
                            break 'batches;
                        }
                    }

                    // Projection outputs are consumed wave by wave and cannot
                    // borrow this compressed source batch. Recycle it now.
                    let _ = free_sender.send(ready.bytes);
                }

                // Closing both directions wakes a producer blocked on either a
                // ready batch or a recycled allocation token.
                drop(ready_receiver);
                drop(free_sender);
                let producer_result = producer.join().map_err(|_| {
                    Error::InvalidIndex("ordered block producer thread panicked".into())
                });

                if let Some(error) = coordinator.error {
                    return Err(error);
                }
                let producer_stats = match producer_result {
                    Ok(result) => result.map_err(E::from)?,
                    Err(error) => return Err(E::from(error)),
                };
                if coordinator.producer_disconnected {
                    return Err(E::from(Error::InvalidIndex(
                        "ordered block producer stopped before the requested range was complete"
                            .into(),
                    )));
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
                coordinator.stats.max_active_workers = max_active_workers.load(Ordering::Relaxed);
                for worker in &workers {
                    let worker = worker.lock().map_err(|_| {
                        E::from(Error::InvalidIndex(
                            "parallel block worker state is poisoned".into(),
                        ))
                    })?;
                    let worker = worker.as_ref().ok_or_else(|| {
                        E::from(Error::InvalidIndex(
                            "parallel block worker state was not restored".into(),
                        ))
                    })?;
                    if worker.used {
                        coordinator.stats.effective_workers = coordinator
                            .stats
                            .effective_workers
                            .checked_add(1)
                            .ok_or_else(|| E::from(Error::Overflow("effective worker count")))?;
                    }
                    coordinator.stats.max_retained_decompressed_buffer_bytes = coordinator
                        .stats
                        .max_retained_decompressed_buffer_bytes
                        .max(worker.max_retained_decompressed_buffer_bytes);
                    coordinator.stats.worker_decompress_decode_sum_time = coordinator
                        .stats
                        .worker_decompress_decode_sum_time
                        .saturating_add(worker.decompress_decode_sum_time);
                    coordinator.stats.worker_projection_sum_time = coordinator
                        .stats
                        .worker_projection_sum_time
                        .saturating_add(worker.projection_sum_time);
                }
                Ok(coordinator.stats)
            });

            let mut consumer_error = None;
            let mut consume_time = Duration::ZERO;
            'delivery: while let Ok((first_row, mut projected)) = decoded_receiver.recv() {
                let started = Instant::now();
                for (offset, result) in projected.drain(..).enumerate() {
                    let result =
                        result.and_then(|output| consume_ordered(first_row + offset, output));
                    if let Err(error) = result {
                        consumer_error = Some(error);
                        break 'delivery;
                    }
                }
                consume_time += started.elapsed();
                // The decoder can send its final result and then close the
                // recycle receiver before this result is returned. Recycling
                // is optional at that point: continue draining decoded results
                // so the final ordered wave is never dropped.
                let _ = projection_free_sender.send(projected);
            }
            // Closing both queues releases a decoder blocked on either direction.
            drop(decoded_receiver);
            drop(projection_free_sender);
            let decoded = decoder.join();
            if let Some(error) = consumer_error {
                return Err(error);
            }
            let mut stats = decoded.map_err(|_| {
                E::from(Error::InvalidIndex(
                    "ordered decoder thread panicked".into(),
                ))
            })??;
            stats.coordinator_consume_wall_time = consume_time;
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
        for row in block.tx_rows.iter().copied() {
            let signatures = SignatureReference {
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
                filter,
                self.registry_entries,
                SelectedCompactV2Schemas {
                    message: self.message_schema,
                    metadata: self.metadata_schema,
                },
                signatures,
            )?);
        }
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
        if filter.reader_id != self.reader_id || filter.binding != self.binding {
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
    transaction_count: u64,
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
        let mut transaction_count = 0u64;
        while end < range.end {
            let row = rows[end];
            if u64::from(row.tx_count) > MAX_ORDERED_PARALLEL_TRANSACTIONS_PER_BATCH {
                return Err(Error::InvalidIndex(format!(
                    "slot {} has {} transactions, above the ordered parallel per-batch limit {}",
                    row.slot, row.tx_count, MAX_ORDERED_PARALLEL_TRANSACTIONS_PER_BATCH,
                )));
            }
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
            let next_transactions = transaction_count
                .checked_add(u64::from(row.tx_count))
                .ok_or(Error::Overflow("parallel transaction batch count"))?;
            if end > start
                && (end - start >= max_blocks_per_batch
                    || next_compressed > compressed_target
                    || next_uncompressed > uncompressed_budget
                    || next_transactions > MAX_ORDERED_PARALLEL_TRANSACTIONS_PER_BATCH)
            {
                break;
            }
            compressed_len = next_compressed;
            declared_uncompressed_bytes = next_uncompressed;
            transaction_count = next_transactions;
            end += 1;
        }
        plans.push(OrderedParallelBatchPlan {
            row_start: start,
            row_end: end,
            compressed_offset: first.compressed_offset,
            compressed_len,
            declared_uncompressed_bytes,
            transaction_count,
        });
        start = end;
    }
    Ok(plans)
}

struct OrderedReadyBatch {
    plan: OrderedParallelBatchPlan,
    bytes: Vec<u8>,
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
    free_receiver: Receiver<Vec<u8>>,
    ready_sender: SyncSender<OrderedReadyBatch>,
) -> Result<OrderedProducerStats> {
    let mut stats = OrderedProducerStats::default();
    for plan in plans {
        let wait_started = Instant::now();
        let Ok(mut bytes) = free_receiver.recv() else {
            // The coordinator stopped after an earlier ordered error.
            return Ok(stats);
        };
        stats.wait_for_free_buffer_time = stats
            .wait_for_free_buffer_time
            .saturating_add(wait_started.elapsed());

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
            .send(OrderedReadyBatch { plan: *plan, bytes })
            .is_err()
        {
            // The coordinator selected an earlier row-order error.
            return Ok(stats);
        }
    }
    Ok(stats)
}

struct OrderedParallelWorker<T> {
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
    decompressed: Vec<u8>,
    caller: T,
    used: bool,
    max_retained_decompressed_buffer_bytes: usize,
    decompress_decode_sum_time: Duration,
    projection_sum_time: Duration,
}

impl<T> OrderedParallelWorker<T> {
    #[allow(clippy::too_many_arguments)]
    fn decode_and_project<S, Output, E, Project>(
        &mut self,
        archive: &ArchiveReader<S>,
        row: ArchiveV2HotBlockIndexRow,
        compressed: &[u8],
        row_number: usize,
        discard_rewards: bool,
        retained_decompressed_bytes: usize,
        project: &Project,
    ) -> std::result::Result<Output, E>
    where
        S: RangeSource,
        E: From<Error>,
        Project: for<'block> Fn(
            &mut T,
            usize,
            BorrowedDecodedBlock<'block>,
        ) -> std::result::Result<Output, E>,
    {
        let result = (|| {
            if self.decompressor.is_none() {
                self.decompressor = Some(
                    zstd::bulk::Decompressor::new()
                        .map_err(|error| Error::DecodeBlock {
                            slot: row.slot,
                            message: format!("create zstd decompressor: {error}"),
                        })
                        .map_err(E::from)?,
                );
            }
            let decode_started = Instant::now();
            let block_result = archive.decode_compressed_block_borrowed_reusing(
                row,
                compressed,
                self.decompressor
                    .as_mut()
                    .expect("decompressor was initialized above"),
                &mut self.decompressed,
                discard_rewards,
            );
            self.decompress_decode_sum_time = self
                .decompress_decode_sum_time
                .saturating_add(decode_started.elapsed());
            let block = block_result.map_err(E::from)?;
            let projection_started = Instant::now();
            let projected = project(&mut self.caller, row_number, block);
            self.projection_sum_time = self
                .projection_sum_time
                .saturating_add(projection_started.elapsed());
            projected
        })();
        if self.decompressed.capacity() > retained_decompressed_bytes {
            self.decompressed = Vec::new();
        } else {
            self.max_retained_decompressed_buffer_bytes = self
                .max_retained_decompressed_buffer_bytes
                .max(self.decompressed.capacity());
        }
        result
    }
}

struct OrderedParallelCoordinator<E> {
    stats: OrderedParallelBlockStats,
    error: Option<E>,
    producer_disconnected: bool,
}

impl<E> Default for OrderedParallelCoordinator<E> {
    fn default() -> Self {
        Self {
            stats: OrderedParallelBlockStats::default(),
            error: None,
            producer_disconnected: false,
        }
    }
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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedBlockStreamIoStats {
    /// Exact coalesced reads made against `blocks.wincode.zst`.
    pub source_read_calls: u64,
    /// Exact compressed bytes returned by those reads.
    pub source_read_bytes: u64,
    /// Exact bytes produced by successful block decompressions.
    pub decoded_bytes: u64,
}

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
    io_stats: BorrowedBlockStreamIoStats,
}

impl<S: RangeSource> BorrowedBlockStream<'_, S> {
    /// Return exact I/O totals accumulated by this stream.
    pub const fn io_stats(&self) -> BorrowedBlockStreamIoStats {
        self.io_stats
    }

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
        let decoded = self.archive.decode_compressed_block_borrowed_reusing(
            row,
            compressed,
            self.decompressor
                .as_mut()
                .expect("decompressor was initialized above"),
            &mut self.decompressed,
            self.discard_rewards,
        );
        if decoded.is_ok() {
            self.io_stats.decoded_bytes = match self
                .io_stats
                .decoded_bytes
                .checked_add(u64::from(row.uncompressed_len))
            {
                Some(total) => total,
                None => {
                    self.next = self.end;
                    return Some(Err(Error::Overflow("borrowed block decoded-byte count")));
                }
            };
        }
        Some(decoded)
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
        self.io_stats.source_read_calls = self
            .io_stats
            .source_read_calls
            .checked_add(1)
            .ok_or(Error::Overflow("borrowed block source-read count"))?;
        self.io_stats.source_read_bytes = self
            .io_stats
            .source_read_bytes
            .checked_add(
                u64::try_from(length)
                    .map_err(|_| Error::Overflow("borrowed block source-read bytes"))?,
            )
            .ok_or(Error::Overflow("borrowed block source-read bytes"))?;
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
    validate_options(options)?;
    manifest.validate()?;
    if !manifest.complete {
        return Err(Error::IncompleteGeneration);
    }
    for required in REQUIRED_GENERATION_FILES {
        manifest.required_file(required)?;
    }
    validate_manifest_files(source, manifest, options)?;

    validate_generation_layout(source, GenerationLayout::Published(manifest), options)
}

fn validate_operator_trusted_local_structure<S: RangeSource>(
    source: &S,
    descriptor: &OperatorTrustedLocalDescriptor,
    options: &OpenOptions,
) -> Result<ValidatedGeneration> {
    validate_options(options)?;
    if options.hash_verification != HashVerification::SizesOnly {
        return Err(Error::InvalidLocalDescriptor(
            "operator-trusted local readers require HashVerification::SizesOnly".into(),
        ));
    }
    descriptor.validate()?;
    validate_local_descriptor_files(source, descriptor)?;
    validate_generation_layout(source, GenerationLayout::Local(descriptor), options)
}

fn discover_archive_descriptor<S: RangeSource>(
    source: &S,
    identity: ArchiveIdentity,
    source_binding: ArchiveSourceBinding,
) -> Result<ArchiveDescriptor> {
    let mut objects = Vec::with_capacity(
        COMPACT_V2_REQUIRED_OBJECTS.len() + COMPACT_V2_OPTIONAL_OBJECTS.len() + 1,
    );
    for name in COMPACT_V2_REQUIRED_OBJECTS {
        let size = source
            .size(name)?
            .ok_or_else(|| Error::MissingLocalFile(name.to_owned()))?;
        objects.push((name.to_owned(), size));
    }
    for name in COMPACT_V2_OPTIONAL_OBJECTS {
        if let Some(size) = source.size(name)? {
            objects.push((name.to_owned(), size));
        }
    }
    if identity.epoch == 0
        && let Some(size) = source.size(GENESIS_BIN_FILE)?
    {
        objects.push((GENESIS_BIN_FILE.to_owned(), size));
    }
    ArchiveDescriptor::new(identity, objects, source_binding)
}

fn validate_archive_descriptor_structure<S: RangeSource>(
    source: &S,
    descriptor: &ArchiveDescriptor,
    options: &OpenOptions,
) -> Result<ValidatedGeneration> {
    validate_options(options)?;
    descriptor.validate()?;
    for object in &descriptor.objects {
        let actual = source
            .size(&object.name)?
            .ok_or_else(|| Error::MissingLocalFile(object.name.clone()))?;
        if actual != object.size {
            return Err(Error::FileSize {
                name: object.name.clone(),
                expected: object.size,
                actual,
            });
        }
    }
    validate_generation_layout(source, GenerationLayout::ObjectSet(descriptor), options)
}

#[derive(Clone, Copy)]
enum GenerationLayout<'a> {
    Published(&'a GenerationManifest),
    Local(&'a OperatorTrustedLocalDescriptor),
    ObjectSet(&'a ArchiveDescriptor),
}

impl GenerationLayout<'_> {
    fn epoch(self) -> u64 {
        match self {
            Self::Published(manifest) => manifest.epoch,
            Self::Local(descriptor) => descriptor.identity.epoch,
            Self::ObjectSet(descriptor) => descriptor.identity.epoch,
        }
    }

    fn slots_per_epoch(self) -> u64 {
        match self {
            Self::Published(manifest) => manifest.slots_per_epoch,
            Self::Local(descriptor) => descriptor.identity.slots_per_epoch,
            Self::ObjectSet(descriptor) => descriptor.identity.slots_per_epoch,
        }
    }

    fn epoch_start_slot(self) -> u64 {
        match self {
            Self::Published(manifest) => manifest.epoch_start_slot(),
            Self::Local(descriptor) => descriptor.epoch_start_slot(),
            Self::ObjectSet(descriptor) => descriptor.epoch_start_slot(),
        }
    }

    fn file_size(self, name: &str) -> Option<u64> {
        match self {
            Self::Published(manifest) => manifest.file(name).map(|file| file.size),
            Self::Local(descriptor) => descriptor.file(name).map(|file| file.size),
            Self::ObjectSet(descriptor) => descriptor.object(name).map(|object| object.size),
        }
    }

    fn required_file_size(self, name: &str) -> Result<u64> {
        match self {
            Self::Published(manifest) => Ok(manifest.required_file(name)?.size),
            Self::Local(descriptor) => Ok(descriptor.required_file(name)?.size),
            Self::ObjectSet(descriptor) => Ok(descriptor.required_object(name)?.size),
        }
    }

    fn binding(self) -> Result<GenerationBinding> {
        match self {
            Self::Published(manifest) => {
                let registry = manifest.required_file(REGISTRY_FILE)?;
                Ok(GenerationBinding {
                    generation_digest: decode_sha256(&manifest.generation_digest)
                        .map_err(Error::InvalidManifest)?,
                    registry_sha256: decode_sha256(&registry.sha256)
                        .map_err(Error::InvalidManifest)?,
                })
            }
            Self::Local(descriptor) => Ok(operator_trusted_runtime_binding(descriptor)),
            Self::ObjectSet(descriptor) => Ok(object_set_runtime_binding(descriptor)),
        }
    }
}

fn validate_generation_layout<S: RangeSource>(
    source: &S,
    generation: GenerationLayout<'_>,
    options: &OpenOptions,
) -> Result<ValidatedGeneration> {
    let registry_size = generation.required_file_size(REGISTRY_FILE)?;

    if registry_size % 32 != 0 {
        return Err(Error::InvalidRegistry(format!(
            "registry.bin is {} bytes, not a multiple of 32",
            registry_size
        )));
    }
    let registry_entries_u64 = registry_size / 32;
    let registry_entries = u32::try_from(registry_entries_u64).map_err(|_| {
        Error::InvalidRegistry(format!(
            "registry has {registry_entries_u64} entries, exceeding the u32 id space"
        ))
    })?;

    let index_size = generation.required_file_size(BLOCK_INDEX_FILE)?;
    let max_index_size = (ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64)
        .checked_add(
            generation
                .slots_per_epoch()
                .checked_mul(ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64)
                .ok_or(Error::Overflow("maximum block index size"))?,
        )
        .ok_or(Error::Overflow("maximum block index size"))?;
    if index_size > max_index_size {
        return Err(Error::InvalidIndex(format!(
            "index is {} bytes, above the epoch maximum {}",
            index_size, max_index_size
        )));
    }
    let index_length = usize::try_from(index_size)
        .map_err(|_| Error::InvalidIndex("index size exceeds usize".into()))?;
    let index_bytes = source.read_range(BLOCK_INDEX_FILE, 0, index_length)?;
    let blocks_size = generation.required_file_size(BLOCKS_FILE)?;
    let (index, total_signatures) = parse_and_validate_index(
        &index_bytes,
        blocks_size,
        generation.epoch(),
        generation.slots_per_epoch(),
        generation.epoch_start_slot(),
        options,
    )?;

    let signatures_available = if let Some(signatures_size) = generation.file_size(SIGNATURES_FILE)
    {
        let expected = total_signatures
            .checked_mul(64)
            .ok_or(Error::Overflow("signature sidecar size"))?;
        if signatures_size != expected {
            return Err(Error::InvalidIndex(format!(
                "signatures.bin is {} bytes, expected {} for {} signatures",
                signatures_size, expected, total_signatures
            )));
        }
        true
    } else {
        false
    };

    let metadata_footer = generation.required_file_size(META_FILE)?;
    let (metadata_footer, genesis) =
        validate_metadata(source, metadata_footer, generation.epoch(), &index, options)?;
    let genesis_bin = validate_genesis_bin(
        source,
        generation.file_size(GENESIS_BIN_FILE),
        generation.epoch(),
        genesis.as_ref(),
    )?;
    let binding = generation.binding()?;
    Ok(ValidatedGeneration {
        index,
        genesis,
        genesis_bin,
        metadata_footer,
        binding,
        registry_entries,
        total_signatures,
        signatures_available,
    })
}

fn validate_local_descriptor_files<S: RangeSource>(
    source: &S,
    descriptor: &OperatorTrustedLocalDescriptor,
) -> Result<()> {
    for file in &descriptor.files {
        let actual = source
            .size(&file.name)?
            .ok_or_else(|| Error::MissingLocalFile(file.name.clone()))?;
        if actual != file.size {
            return Err(Error::FileSize {
                name: file.name.clone(),
                expected: file.size,
                actual,
            });
        }
    }
    Ok(())
}

fn operator_trusted_runtime_binding(
    descriptor: &OperatorTrustedLocalDescriptor,
) -> GenerationBinding {
    const DOMAIN: &[u8] = b"blockzilla/operator-trusted-local-runtime-binding/v1\0";
    let mut files: Vec<_> = descriptor.files.iter().collect();
    files.sort_unstable_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN);
    for value in [
        descriptor.identity.cluster_id.as_bytes(),
        descriptor.identity.generation_id.as_bytes(),
    ] {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value);
    }
    hasher.update(descriptor.identity.epoch.to_le_bytes());
    hasher.update(descriptor.identity.slots_per_epoch.to_le_bytes());
    for file in files {
        hasher.update((file.name.len() as u64).to_le_bytes());
        hasher.update(file.name.as_bytes());
        hasher.update(file.size.to_le_bytes());
    }
    let generation_digest: [u8; 32] = hasher.finalize().into();

    let mut registry_hasher = Sha256::new();
    registry_hasher.update(DOMAIN);
    registry_hasher.update(b"registry-size\0");
    registry_hasher.update(generation_digest);
    registry_hasher.update(
        descriptor
            .required_file(REGISTRY_FILE)
            .expect("validated local descriptor has registry.bin")
            .size
            .to_le_bytes(),
    );
    GenerationBinding {
        generation_digest,
        registry_sha256: registry_hasher.finalize().into(),
    }
}

fn object_set_runtime_binding(descriptor: &ArchiveDescriptor) -> GenerationBinding {
    const OBJECT_SET_DOMAIN: &[u8] = b"blockzilla/compact-v2/object-set-runtime-binding/v1\0";
    const PINNED_LOCAL_DOMAIN: &[u8] = b"blockzilla/compact-v2/pinned-local-runtime-binding/v1\0";
    let mut objects: Vec<_> = descriptor.objects.iter().collect();
    objects.sort_unstable_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
    let mut hasher = Sha256::new();
    let source_id = match &descriptor.source_binding {
        ArchiveSourceBinding::PinnedLocal => {
            hasher.update(PINNED_LOCAL_DOMAIN);
            None
        }
        ArchiveSourceBinding::StrongEtags { object_set_id } => {
            hasher.update(OBJECT_SET_DOMAIN);
            Some(object_set_id.as_bytes())
        }
    };
    for value in [
        descriptor.identity.cluster_id.as_bytes(),
        descriptor.identity.generation_id.as_bytes(),
    ] {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value);
    }
    if let Some(source_id) = source_id {
        hasher.update((source_id.len() as u64).to_le_bytes());
        hasher.update(source_id);
    }
    hasher.update(descriptor.identity.epoch.to_le_bytes());
    hasher.update(descriptor.identity.slots_per_epoch.to_le_bytes());
    for object in objects {
        hasher.update((object.name.len() as u64).to_le_bytes());
        hasher.update(object.name.as_bytes());
        hasher.update(object.size.to_le_bytes());
    }
    let generation_digest: [u8; 32] = hasher.finalize().into();

    let mut registry_binding = Sha256::new();
    registry_binding.update(match &descriptor.source_binding {
        ArchiveSourceBinding::PinnedLocal => PINNED_LOCAL_DOMAIN,
        ArchiveSourceBinding::StrongEtags { .. } => OBJECT_SET_DOMAIN,
    });
    registry_binding.update(b"registry-object\0");
    registry_binding.update(generation_digest);
    registry_binding.update(
        descriptor
            .required_object(REGISTRY_FILE)
            .expect("validated descriptor has registry.bin")
            .size
            .to_le_bytes(),
    );
    GenerationBinding {
        generation_digest,
        registry_sha256: registry_binding.finalize().into(),
    }
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
                    BLOCK_INDEX_FILE | META_FILE | REGISTRY_FILE | GENESIS_BIN_FILE
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
    file_size: Option<u64>,
    epoch: u64,
    inline: Option<&WincodeArchiveV2Genesis>,
) -> Result<Option<Vec<u8>>> {
    let Some(file_size) = file_size else {
        return Ok(None);
    };
    if epoch != 0 {
        return Err(Error::InvalidMetadata(format!(
            "{GENESIS_BIN_FILE} is only valid for epoch 0"
        )));
    }
    let inline = inline.ok_or_else(|| {
        Error::InvalidMetadata(format!(
            "{GENESIS_BIN_FILE} is published without inline genesis metadata"
        ))
    })?;
    let length = usize::try_from(file_size)
        .map_err(|_| Error::InvalidMetadata("genesis.bin size exceeds usize".into()))?;
    if length > MAX_GENESIS_BIN_BYTES {
        return Err(Error::InvalidMetadata(format!(
            "{GENESIS_BIN_FILE} is {length} bytes, above the {MAX_GENESIS_BIN_BYTES} byte limit"
        )));
    }
    if file_size != inline.genesis_bin_len {
        return Err(Error::InvalidMetadata(format!(
            "{GENESIS_BIN_FILE} is {} bytes, inline genesis reports {}",
            file_size, inline.genesis_bin_len
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
    epoch: u64,
    slots_per_epoch: u64,
    default_epoch_first_slot: u64,
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
    if row_count > slots_per_epoch {
        return Err(Error::InvalidIndex(format!(
            "index has {row_count} rows for {} epoch slots",
            slots_per_epoch
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

    let epoch_first_slot = options.epoch_first_slot.unwrap_or(default_epoch_first_slot);
    let epoch_last_slot = epoch_first_slot
        .checked_add(slots_per_epoch - 1)
        .ok_or(Error::Overflow("explicit epoch slot range"))?;
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
        if row.slot < epoch_first_slot || row.slot > epoch_last_slot {
            return Err(Error::InvalidIndex(format!(
                "slot {} is outside epoch {} range {}..={}",
                row.slot, epoch, epoch_first_slot, epoch_last_slot
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
    metadata_size: u64,
    epoch: u64,
    index: &ArchiveV2HotBlockIndex,
    options: &OpenOptions,
) -> Result<(WincodeArchiveV2Footer, Option<WincodeArchiveV2Genesis>)> {
    let mut reader =
        RangeSourceReader::new(source, META_FILE, metadata_size, options.io_chunk_size);
    let mut position = 0usize;
    let mut saw_genesis = false;
    let mut genesis = None;
    let mut footer = None;
    while let Some(mut frame) = read_frame(&mut reader, options.max_meta_frame_bytes)? {
        let record = decode_hot_metadata_record(&mut frame, epoch, position)?;
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
                if saw_genesis || epoch != 0 {
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

    wincode::config::deserialize(frame, wincode_leb128_config())
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
fn validate_decoded_block_parts(
    index: &ArchiveV2HotBlockIndexRow,
    header: &ArchiveV2HotBlockHeader,
    tx_count: u32,
    tx_rows_len: usize,
    tx_rows: impl ExactSizeIterator<Item = ArchiveV2HotTxRow>,
    message_bytes: &[u8],
    metadata_bytes: &[u8],
) -> Result<()> {
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
    for (number, row) in tx_rows.enumerate() {
        if row.tx_index != number as u32 {
            return Err(fail(format!(
                "transaction row {number} has tx_index {}",
                row.tx_index
            )));
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

fn scan_transaction(
    slot: u64,
    row: ArchiveV2HotTxRow,
    block: &ArchiveV2HotBlockBlob,
    filter: &CompiledPubkeyFilter,
    registry_entries: u32,
    schemas: SelectedCompactV2Schemas,
    signatures: SignatureReference,
) -> Result<ScannedTransaction> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Ok(ScannedTransaction {
            slot,
            tx_index: row.tx_index,
            row,
            outcome: TransactionMatch::Indeterminate(IndeterminateReason::RawTransactionFallback),
            message: None,
            metadata: metadata_state(block, &row, slot, false, schemas.metadata)?,
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
    let message: ArchiveV2HotMessagePayload =
        decode_compact_v2_message(schemas.message, message_bytes).map_err(|error| {
            Error::InvalidBlock {
                slot,
                message: format!("decode message for tx {}: {error}", row.tx_index),
            }
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
        ArchiveV2HotMessagePayload::V1(message) => message.account_keys.as_slice(),
    };
    let static_result = evaluate_keys(static_keys, filter, registry_entries);
    let needs_loaded = is_v0 && row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0;
    let read_metadata = static_result.matched || needs_loaded;
    let metadata = metadata_state(block, &row, slot, read_metadata, schemas.metadata)?;

    let mut loaded_result = KeyEvaluation::default();
    let loaded_unavailable = if needs_loaded {
        match &metadata {
            MetadataState::Decoded(metadata) => {
                loaded_result = evaluate_keys(
                    metadata
                        .loaded_writable_addresses
                        .iter()
                        .chain(&metadata.loaded_readonly_addresses),
                    filter,
                    registry_entries,
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

fn select_program_invocations(
    filter: &CompiledPubkeyFilter,
    registry_entries: u32,
    row: &ArchiveV2HotTxRow,
    message: Option<&ArchiveV2HotMessagePayload>,
    metadata: Option<&CompactMetaV1>,
) -> SelectorOutcome<ProgramInvocationMatch> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return SelectorOutcome::Indeterminate(SelectorIndeterminateReason::RawTransactionFallback);
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return SelectorOutcome::Indeterminate(SelectorIndeterminateReason::RawMetadataFallback);
    }
    let Some(message) = message else {
        return SelectorOutcome::Indeterminate(SelectorIndeterminateReason::MessageUnavailable);
    };
    let (static_keys, instructions, is_v0) = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => (
            message.account_keys.as_slice(),
            message.instructions.as_slice(),
            false,
        ),
        ArchiveV2HotMessagePayload::V0(message) => (
            message.account_keys.as_slice(),
            message.instructions.as_slice(),
            true,
        ),
        ArchiveV2HotMessagePayload::V1(message) => (
            message.account_keys.as_slice(),
            message.instructions.as_slice(),
            false,
        ),
    };

    let mut direct_count = 0u32;
    for instruction in instructions {
        let reference = match message_account_reference(
            static_keys,
            is_v0,
            usize::from(instruction.program_id_index),
            row,
            metadata,
        ) {
            Ok(reference) => reference,
            Err(reason) => return SelectorOutcome::Indeterminate(reason),
        };
        match filter.classify_reference(reference, registry_entries) {
            PubkeyReferenceMatch::Match => {
                let Some(next) = direct_count.checked_add(1) else {
                    return SelectorOutcome::Indeterminate(
                        SelectorIndeterminateReason::InvalidAccountReference,
                    );
                };
                direct_count = next;
            }
            PubkeyReferenceMatch::NoMatch => {}
            PubkeyReferenceMatch::InvalidRegistryReference => {
                return SelectorOutcome::Indeterminate(
                    SelectorIndeterminateReason::InvalidRegistryReference,
                );
            }
        }
    }

    // A decoded metadata record is required even when the semantic inner-IX
    // flag is clear. Without it, this selector cannot prove CPI coverage and
    // cannot report complete direct/CPI counts.
    let Some(metadata) = metadata else {
        return SelectorOutcome::Indeterminate(SelectorIndeterminateReason::MetadataUnavailable);
    };

    let mut cpi_count = 0u32;
    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0 {
        let Some(groups) = metadata.inner_instructions.as_deref() else {
            return SelectorOutcome::Indeterminate(
                SelectorIndeterminateReason::MetadataUnavailable,
            );
        };
        for group in groups {
            let Ok(outer_index) = usize::try_from(group.index) else {
                return SelectorOutcome::Indeterminate(
                    SelectorIndeterminateReason::InvalidAccountReference,
                );
            };
            if outer_index >= instructions.len() {
                return SelectorOutcome::Indeterminate(
                    SelectorIndeterminateReason::InvalidAccountReference,
                );
            }
            for instruction in &group.instructions {
                let Ok(program_index) = usize::try_from(instruction.program_id_index) else {
                    return SelectorOutcome::Indeterminate(
                        SelectorIndeterminateReason::InvalidAccountReference,
                    );
                };
                let reference = match message_account_reference(
                    static_keys,
                    is_v0,
                    program_index,
                    row,
                    Some(metadata),
                ) {
                    Ok(reference) => reference,
                    Err(reason) => return SelectorOutcome::Indeterminate(reason),
                };
                match filter.classify_reference(reference, registry_entries) {
                    PubkeyReferenceMatch::Match => {
                        let Some(next) = cpi_count.checked_add(1) else {
                            return SelectorOutcome::Indeterminate(
                                SelectorIndeterminateReason::InvalidAccountReference,
                            );
                        };
                        cpi_count = next;
                    }
                    PubkeyReferenceMatch::NoMatch => {}
                    PubkeyReferenceMatch::InvalidRegistryReference => {
                        return SelectorOutcome::Indeterminate(
                            SelectorIndeterminateReason::InvalidRegistryReference,
                        );
                    }
                }
            }
        }
    }

    if direct_count != 0 || cpi_count != 0 {
        SelectorOutcome::Match(ProgramInvocationMatch {
            direct_count,
            cpi_count,
        })
    } else {
        SelectorOutcome::NoMatch
    }
}

fn message_account_reference<'a>(
    static_keys: &'a [CompactPubkey],
    is_v0: bool,
    index: usize,
    row: &ArchiveV2HotTxRow,
    metadata: Option<&'a CompactMetaV1>,
) -> std::result::Result<&'a CompactPubkey, SelectorIndeterminateReason> {
    if let Some(reference) = static_keys.get(index) {
        return Ok(reference);
    }
    if !is_v0 {
        return Err(SelectorIndeterminateReason::InvalidAccountReference);
    }
    let metadata = required_metadata(row, metadata)?;
    let loaded_index = index - static_keys.len();
    metadata
        .loaded_writable_addresses
        .get(loaded_index)
        .or_else(|| {
            loaded_index
                .checked_sub(metadata.loaded_writable_addresses.len())
                .and_then(|index| metadata.loaded_readonly_addresses.get(index))
        })
        .ok_or(SelectorIndeterminateReason::InvalidAccountReference)
}

fn required_metadata<'a>(
    row: &ArchiveV2HotTxRow,
    metadata: Option<&'a CompactMetaV1>,
) -> std::result::Result<&'a CompactMetaV1, SelectorIndeterminateReason> {
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return Err(SelectorIndeterminateReason::RawMetadataFallback);
    }
    metadata.ok_or(SelectorIndeterminateReason::MetadataUnavailable)
}

fn select_token_balances(
    filter: &CompiledPubkeyFilter,
    registry_entries: u32,
    row: &ArchiveV2HotTxRow,
    metadata: Option<&CompactMetaV1>,
) -> SelectorOutcome<TokenBalanceMatch> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return SelectorOutcome::Indeterminate(SelectorIndeterminateReason::RawTransactionFallback);
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return SelectorOutcome::Indeterminate(SelectorIndeterminateReason::RawMetadataFallback);
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES == 0 {
        return SelectorOutcome::NoMatch;
    }
    let metadata = match required_metadata(row, metadata) {
        Ok(metadata) => metadata,
        Err(reason) => return SelectorOutcome::Indeterminate(reason),
    };
    let mut pre_count = 0u32;
    let mut post_count = 0u32;
    for (balances, count) in [
        (metadata.pre_token_balances.as_slice(), &mut pre_count),
        (metadata.post_token_balances.as_slice(), &mut post_count),
    ] {
        for balance in balances {
            let Some(mint) = balance.mint.as_ref() else {
                return SelectorOutcome::Indeterminate(
                    SelectorIndeterminateReason::TokenMintUnavailable,
                );
            };
            match filter.classify_reference(mint, registry_entries) {
                PubkeyReferenceMatch::Match => {
                    let Some(next) = count.checked_add(1) else {
                        return SelectorOutcome::Indeterminate(
                            SelectorIndeterminateReason::InvalidAccountReference,
                        );
                    };
                    *count = next;
                }
                PubkeyReferenceMatch::NoMatch => {}
                PubkeyReferenceMatch::InvalidRegistryReference => {
                    return SelectorOutcome::Indeterminate(
                        SelectorIndeterminateReason::InvalidRegistryReference,
                    );
                }
            }
        }
    }
    if pre_count != 0 || post_count != 0 {
        SelectorOutcome::Match(TokenBalanceMatch {
            pre_count,
            post_count,
        })
    } else {
        SelectorOutcome::NoMatch
    }
}

fn metadata_state(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
    slot: u64,
    read: bool,
    metadata_schema: CompactV2MetadataSchema,
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
    let metadata = decode_compact_v2_metadata(metadata_schema, bytes).map_err(|error| {
        Error::InvalidBlock {
            slot,
            message: format!("decode metadata for tx {}: {error}", row.tx_index),
        }
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
        ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ArchiveV2HotBlockHeader, ArchiveV2HotInstruction,
        ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotRewards,
        ArchiveV2HotV0Message, CompactInnerInstruction, CompactInnerInstructions,
        CompactMessageHeader, CompactReward, CompactShredding, CompactTokenBalance,
        OwnedCompactRecentBlockhash, WincodeArchiveV2Header, write_archive_v2_hot_block_index,
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
                        tx_index: 0,
                        flags: 0,
                        message_offset: 0,
                        message_len: first_message.len() as u32,
                        metadata_offset: 0,
                        metadata_len: 0,
                        signature_count: 2,
                        reserved: [0; 3],
                    },
                    ArchiveV2HotTxRow {
                        tx_index: 1,
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

        fn parallel_blocks(block_count: usize) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            fs::write(root.join(REGISTRY_FILE), []).unwrap();

            let mut blocks = Vec::new();
            let mut rows = Vec::with_capacity(block_count);
            for block_id in 0..block_count {
                let slot = 101 + block_id as u64;
                let block = ArchiveV2HotBlockBlob {
                    header: ArchiveV2HotBlockHeader {
                        slot,
                        parent_slot: slot - 1,
                        blockhash_id: block_id as u32 + 1,
                        previous_blockhash_id: block_id as u32,
                        block_time: None,
                        block_height: None,
                        rewards: None,
                    },
                    tx_count: 0,
                    tx_rows: Vec::new(),
                    message_bytes: Vec::new(),
                    metadata_bytes: Vec::new(),
                };
                let uncompressed =
                    wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
                let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
                rows.push(ArchiveV2HotBlockIndexRow {
                    block_id: block_id as u32,
                    slot,
                    compressed_offset: blocks.len() as u64,
                    compressed_len: compressed.len() as u32,
                    uncompressed_len: uncompressed.len() as u32,
                    tx_count: 0,
                    first_tx_ordinal: 0,
                    first_signature_ordinal: 0,
                    signature_count: 0,
                });
                blocks.extend_from_slice(&compressed);
            }
            fs::write(root.join(BLOCKS_FILE), &blocks).unwrap();
            write_archive_v2_hot_block_index(
                &root.join(BLOCK_INDEX_FILE),
                blocks.len() as u64,
                1,
                0,
                &rows,
            )
            .unwrap();
            let records = [
                ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                    version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                    flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
                }),
                ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                    blocks: block_count as u64,
                    transactions: 0,
                    ..WincodeArchiveV2Footer::default()
                }),
            ];
            let mut metadata = Vec::new();
            for record in records {
                let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
                write_u32_varint(&mut metadata, bytes.len() as u32);
                metadata.extend_from_slice(&bytes);
            }
            fs::write(root.join(META_FILE), metadata).unwrap();
            fs::write(root.join(SIGNATURES_FILE), []).unwrap();
            write_manifest(root, true, None);
            Self { directory }
        }

        fn source(&self) -> LocalRangeSource {
            LocalRangeSource::new(self.directory.path())
        }
    }

    fn selector_row(flags: u32) -> ArchiveV2HotTxRow {
        ArchiveV2HotTxRow {
            tx_index: 0,
            flags,
            message_offset: 0,
            message_len: 0,
            metadata_offset: 0,
            metadata_len: 0,
            signature_count: 1,
            reserved: [0; 3],
        }
    }

    fn selector_instruction(program_id_index: u8) -> ArchiveV2HotInstruction {
        ArchiveV2HotInstruction {
            program_id_index,
            accounts: Vec::new(),
            data: ArchiveV2HotInstructionData::Raw(Vec::new()),
        }
    }

    fn selector_message(
        account_keys: Vec<CompactPubkey>,
        instructions: Vec<ArchiveV2HotInstruction>,
    ) -> ArchiveV2HotMessagePayload {
        ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: message_header(),
            account_keys,
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions,
        })
    }

    fn selector_metadata() -> CompactMetaV1 {
        CompactMetaV1 {
            err: None,
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
        }
    }

    fn decoded_single_transaction(
        message_bytes: Vec<u8>,
        metadata_bytes: Vec<u8>,
        flags: u32,
    ) -> DecodedBlock {
        DecodedBlock {
            index_row: ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 101,
                compressed_offset: 0,
                compressed_len: 0,
                uncompressed_len: 0,
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
                tx_rows: vec![ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags,
                    message_offset: 0,
                    message_len: message_bytes.len() as u32,
                    metadata_offset: 0,
                    metadata_len: metadata_bytes.len() as u32,
                    signature_count: 1,
                    reserved: [0; 3],
                }],
                message_bytes,
                metadata_bytes,
            },
        }
    }

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

    #[test]
    fn strict_program_selector_matches_direct_and_cpi_invocations() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let filter = archive.compile_pubkey_filter([REGISTRY_KEY_TWO]).unwrap();

        let direct = selector_message(vec![CompactPubkey::Id(2)], vec![selector_instruction(0)]);
        assert_eq!(
            archive
                .select_program_invocations(
                    &filter,
                    &selector_row(ARCHIVE_V2_TX_FLAG_HAS_METADATA),
                    Some(&direct),
                    Some(&selector_metadata()),
                )
                .unwrap(),
            SelectorOutcome::Match(ProgramInvocationMatch {
                direct_count: 1,
                cpi_count: 0,
            })
        );

        let cpi = selector_message(
            vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            vec![selector_instruction(0)],
        );
        let mut metadata = selector_metadata();
        metadata.inner_instructions = Some(vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 1,
                accounts: Vec::new(),
                data: Vec::new(),
                stack_height: Some(2),
            }],
        }]);
        assert_eq!(
            archive
                .select_program_invocations(
                    &filter,
                    &selector_row(
                        ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                    ),
                    Some(&cpi),
                    Some(&metadata),
                )
                .unwrap(),
            SelectorOutcome::Match(ProgramInvocationMatch {
                direct_count: 0,
                cpi_count: 1,
            })
        );
    }

    #[test]
    fn explicit_epoch_first_slot_controls_index_bounds() {
        let fixture = Fixture::build();
        let accepted = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            epoch_first_slot: Some(100),
            ..OpenOptions::default()
        };
        ArchiveReader::open_with_options(fixture.source(), accepted).unwrap();

        let rejected = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            epoch_first_slot: Some(0),
            ..OpenOptions::default()
        };
        let error = ArchiveReader::open_with_options(fixture.source(), rejected).unwrap_err();
        assert!(error.to_string().contains("outside epoch 1 range 0..=99"));
    }

    #[test]
    fn strict_token_selector_matches_pre_post_and_raw_mint_references() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let filter = archive.compile_pubkey_filter([REGISTRY_KEY_TWO]).unwrap();
        assert_eq!(filter.registry_id_for(&REGISTRY_KEY_TWO), Some(2));
        assert!(
            filter.matches_reference(&CompactPubkey::Raw(REGISTRY_KEY_TWO), &REGISTRY_KEY_TWO,)
        );

        let balance = |mint| CompactTokenBalance {
            account_index: 0,
            mint: Some(mint),
            owner: None,
            program_id: None,
            amount: 1,
            decimals: 6,
        };
        let mut metadata = selector_metadata();
        metadata.pre_token_balances = vec![balance(CompactPubkey::Id(2))];
        metadata.post_token_balances = vec![balance(CompactPubkey::Raw(REGISTRY_KEY_TWO))];
        assert_eq!(
            archive
                .select_token_balances(
                    &filter,
                    &selector_row(
                        ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES,
                    ),
                    Some(&metadata),
                )
                .unwrap(),
            SelectorOutcome::Match(TokenBalanceMatch {
                pre_count: 1,
                post_count: 1,
            })
        );
    }

    #[test]
    fn strict_selectors_report_raw_invalid_and_missing_coverage() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let filter = archive.compile_pubkey_filter([REGISTRY_KEY_TWO]).unwrap();
        let message = selector_message(vec![CompactPubkey::Id(2)], vec![selector_instruction(0)]);

        for flags in [
            ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
            ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        ] {
            assert_eq!(
                archive
                    .select_program_invocations(
                        &filter,
                        &selector_row(flags),
                        Some(&message),
                        None,
                    )
                    .unwrap(),
                SelectorOutcome::Indeterminate(
                    SelectorIndeterminateReason::RawMetadataFallback,
                )
            );
            assert_eq!(
                archive
                    .select_token_balances(&filter, &selector_row(flags), None)
                    .unwrap(),
                SelectorOutcome::Indeterminate(SelectorIndeterminateReason::RawMetadataFallback,)
            );
        }

        assert_eq!(
            archive
                .select_program_invocations(&filter, &selector_row(0), Some(&message), None,)
                .unwrap(),
            SelectorOutcome::Indeterminate(SelectorIndeterminateReason::MetadataUnavailable)
        );
        assert_eq!(
            archive
                .select_program_invocations(
                    &filter,
                    &selector_row(ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK),
                    None,
                    None,
                )
                .unwrap(),
            SelectorOutcome::Indeterminate(SelectorIndeterminateReason::RawTransactionFallback,)
        );

        let invalid = selector_message(vec![CompactPubkey::Id(99)], vec![selector_instruction(0)]);
        assert_eq!(
            archive
                .select_program_invocations(
                    &filter,
                    &selector_row(ARCHIVE_V2_TX_FLAG_HAS_METADATA),
                    Some(&invalid),
                    Some(&selector_metadata()),
                )
                .unwrap(),
            SelectorOutcome::Indeterminate(SelectorIndeterminateReason::InvalidRegistryReference,)
        );

        let mut invalid_token = selector_metadata();
        invalid_token.pre_token_balances = vec![CompactTokenBalance {
            account_index: 0,
            mint: Some(CompactPubkey::Id(99)),
            owner: None,
            program_id: None,
            amount: 1,
            decimals: 6,
        }];
        assert_eq!(
            archive
                .select_token_balances(
                    &filter,
                    &selector_row(
                        ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES,
                    ),
                    Some(&invalid_token),
                )
                .unwrap(),
            SelectorOutcome::Indeterminate(SelectorIndeterminateReason::InvalidRegistryReference,)
        );
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

    #[test]
    fn compact_pubkey_resolution_uses_one_exact_bounded_registry_read() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let observed = source.clone();
        let archive = ArchiveReader::open(source).unwrap();
        observed.clear();

        assert_eq!(
            archive
                .resolve_pubkey(&CompactPubkey::Raw(RAW_KEY))
                .unwrap(),
            RAW_KEY
        );
        assert!(observed.reads_for(REGISTRY_FILE).is_empty());

        assert_eq!(
            archive.resolve_pubkey(&CompactPubkey::Id(2)).unwrap(),
            REGISTRY_KEY_TWO
        );
        assert_eq!(observed.reads_for(REGISTRY_FILE), [(32, 32)]);

        observed.clear();
        assert!(matches!(
            archive.resolve_pubkey(&CompactPubkey::Id(0)).unwrap_err(),
            Error::InvalidRegistry(_)
        ));
        assert!(observed.reads_for(REGISTRY_FILE).is_empty());
    }

    #[test]
    fn published_reader_binds_message_schema_and_uses_it_in_transaction_scan() {
        let fixture = Fixture::build();
        bind_schema_marker(
            fixture.directory.path(),
            crate::COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
            crate::COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES,
        );
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        assert_eq!(
            archive.message_schema(),
            CompactV2MessageSchema::May24PreUnknownFallbacks
        );
        assert_eq!(
            archive.metadata_schema(),
            CompactV2MetadataSchema::CurrentTypedError
        );

        let historical_message = decode_hex(
            "0002010206121813150e0d00c0e60c02040202000209ccf1736d29ad6e301871d2d5a34e01709272ebdc60b9b855a31b7c3036fae9360131c80106a1d8179137542a983437bdfe2a7ab2557f535c8a78722b68a49dc0000000000503030201000c030000000080c6a47e8d0300",
        );
        let filter = archive
            .compile_pubkey_filter(std::iter::empty::<[u8; 32]>())
            .unwrap();
        let scanned = archive
            .scan_decoded_block(
                &filter,
                decoded_single_transaction(historical_message, Vec::new(), 0),
            )
            .unwrap();
        assert!(matches!(
            scanned.transactions[0].message,
            Some(ArchiveV2HotMessagePayload::Legacy(_))
        ));
    }

    #[test]
    fn published_reader_accepts_explicit_current_message_schema_marker() {
        let fixture = Fixture::build();
        bind_schema_marker(
            fixture.directory.path(),
            crate::COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
            crate::COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES,
        );
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        assert_eq!(archive.message_schema(), CompactV2MessageSchema::Current);
    }

    #[test]
    fn published_reader_binds_legacy_metadata_schema_and_uses_it_in_scan() {
        let fixture = Fixture::build();
        bind_schema_marker(
            fixture.directory.path(),
            crate::COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE,
            crate::COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES,
        );
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        assert_eq!(
            archive.metadata_schema(),
            CompactV2MetadataSchema::LegacyRawError
        );

        let message = selector_message(vec![CompactPubkey::Raw(RAW_KEY)], Vec::new());
        let message = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
        let epoch_900_metadata = vec![
            0x01, 0x0d, 0x08, 0x00, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x90, 0x4e, 0x03, 0xcc, 0xef, 0xf0, 0xf2, 0x32, 0x80, 0xf6, 0xed, 0xdf, 0x09,
            0x01, 0x03, 0xbc, 0xa1, 0xf0, 0xf2, 0x32, 0x80, 0xf6, 0xed, 0xdf, 0x09, 0x01, 0x01,
            0x00, 0x01, 0x02, 0x0e, 0x03, 0x01, 0x16, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xb4, 0x10, 0x01, 0xe4, 0x1a,
        ];
        let filter = archive.compile_pubkey_filter([RAW_KEY]).unwrap();
        let scanned = archive
            .scan_decoded_block(
                &filter,
                decoded_single_transaction(
                    message,
                    epoch_900_metadata,
                    ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                ),
            )
            .unwrap();
        assert!(matches!(
            scanned.transactions[0].metadata,
            MetadataState::Decoded(ref metadata) if metadata.fee == 10_000
        ));
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
        };
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_trusted(fixture.source(), identity, options).unwrap();
        assert_eq!(archive.index().rows.len(), 2);
        assert!(archive.published_manifest().is_none());
        assert_eq!(
            archive
                .local_descriptor()
                .expect("local descriptor")
                .identity
                .epoch,
            EPOCH
        );
        assert_eq!(archive.epoch(), EPOCH);

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
    fn open_trusted_rejects_non_sizes_only_verification() {
        let fixture = Fixture::build();
        fs::remove_file(fixture.directory.path().join(GENERATION_MANIFEST_FILE)).unwrap();

        let identity = crate::manifest::TrustedGenerationIdentity {
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "trusted-fixture".into(),
            slots_per_epoch: SLOTS_PER_EPOCH,
        };
        let error = ArchiveReader::open_trusted(fixture.source(), identity, OpenOptions::default())
            .unwrap_err();
        assert!(matches!(error, Error::InvalidLocalDescriptor(_)));
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
                |_| Ok(()),
                move |_, row_number, block| -> Result<u64> {
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
        assert_eq!(stats.batch_count, 1);
        assert_eq!(stats.effective_workers, 2);
        assert_eq!(stats.max_active_workers, 2);
        assert_eq!(stats.max_blocks_per_batch, 2);
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
        assert!(
            stats.max_retained_decompressed_buffer_bytes
                >= archive
                    .index()
                    .rows
                    .iter()
                    .map(|row| row.uncompressed_len as usize)
                    .max()
                    .unwrap()
        );
        assert_eq!(
            source.reads_for(BLOCKS_FILE),
            vec![(0, archive.index().blob_file_bytes as usize)]
        );
    }

    #[test]
    fn ordered_parallel_uses_twelve_workers_across_twenty_four_delayed_tasks() {
        const WORKERS: usize = 12;
        const BLOCKS: usize = WORKERS * 2;

        let fixture = Fixture::parallel_blocks(BLOCKS);
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let completion_barrier = Arc::new(Barrier::new(WORKERS));
        let project_barrier = Arc::clone(&completion_barrier);
        let consumed = Arc::new(Mutex::new(Vec::new()));
        let ordered_consumed = Arc::clone(&consumed);

        let stats = archive
            .process_borrowed_blocks_parallel_ordered(
                0..BLOCKS,
                OrderedParallelBlockConfig {
                    decode_workers: WORKERS,
                    max_blocks_per_batch: BLOCKS,
                    discard_rewards: true,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok(()),
                move |_, row_number, block| -> Result<usize> {
                    project_barrier.wait();
                    std::thread::sleep(Duration::from_millis(2));
                    assert!(!block.uses_owned_fallback());
                    Ok(row_number)
                },
                move |row_number, projected_row| {
                    assert_eq!(projected_row, row_number);
                    ordered_consumed.lock().unwrap().push(row_number);
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(*consumed.lock().unwrap(), (0..BLOCKS).collect::<Vec<_>>());
        assert_eq!(stats.block_count, BLOCKS as u64);
        assert_eq!(stats.effective_workers, WORKERS);
        assert_eq!(stats.max_active_workers, WORKERS);
    }

    #[test]
    fn ordered_parallel_drains_final_wave_after_recycle_receiver_closes() {
        const BLOCKS: usize = 5;
        let fixture = Fixture::parallel_blocks(BLOCKS);
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut consumed = Vec::new();

        archive
            .process_borrowed_blocks_parallel_ordered(
                0..BLOCKS,
                OrderedParallelBlockConfig {
                    decode_workers: 1,
                    max_blocks_per_batch: BLOCKS,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok(()),
                |_, row_number, _| -> Result<usize> { Ok(row_number) },
                |row_number, projected_row| {
                    if row_number == 0 {
                        // Let the decoder queue its short final wave and close
                        // the optional projection-buffer recycle receiver.
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    assert_eq!(projected_row, row_number);
                    consumed.push(row_number);
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(consumed, (0..BLOCKS).collect::<Vec<_>>());
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
                    Ok(())
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
    fn ordered_parallel_fallible_worker_setup_stops_before_block_reads() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();

        let error = archive
            .process_borrowed_blocks_parallel_ordered(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                |worker| -> Result<()> {
                    Err(Error::InvalidRegistry(format!(
                        "worker {worker} setup failed"
                    )))
                },
                |_, _, _| -> Result<()> { panic!("failed setup ran a projection") },
                |_, ()| -> Result<()> { panic!("failed setup published a result") },
            )
            .unwrap_err();

        assert!(matches!(error, Error::InvalidRegistry(_)));
        assert!(error.to_string().contains("worker 0 setup failed"));
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
                |_| Ok(()),
                |_, _, block| -> Result<u64> { Ok(block.header().slot) },
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
                |_| Ok(()),
                |_, _, block| -> Result<u64> { Ok(block.header().slot) },
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
                    retained_decompressed_bytes_per_worker: 0,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok(()),
                |_, _, block| -> Result<u64> { Ok(block.header().slot) },
                |_, slot| {
                    slots.push(slot);
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(slots, vec![101, 102]);
        assert_eq!(stats.batch_count, 2);
        assert_eq!(stats.read_call_count, 2);
        assert_eq!(stats.max_blocks_per_batch, 1);
        assert_eq!(stats.max_retained_decompressed_buffer_bytes, 0);
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
                |_| Ok(()),
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
    fn ordered_parallel_transaction_budget_splits_and_rejects_one_oversized_row() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut rows = archive.index().rows.clone();
        rows[0].tx_count = 40_000;
        rows[1].tx_count = 40_000;

        let plans = ordered_parallel_batch_plans(
            &rows,
            0..rows.len(),
            usize::MAX,
            MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
            MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH,
        )
        .unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].transaction_count, 40_000);
        assert_eq!(plans[1].transaction_count, 40_000);

        rows[0].tx_count = u32::try_from(MAX_ORDERED_PARALLEL_TRANSACTIONS_PER_BATCH + 1).unwrap();
        let error = ordered_parallel_batch_plans(
            &rows,
            0..1,
            usize::MAX,
            MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
            MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH,
        )
        .unwrap_err();
        assert!(matches!(error, Error::InvalidIndex(_)));
    }

    #[test]
    fn ordered_parallel_preserves_typed_project_and_consume_errors() {
        #[derive(Debug)]
        struct TypedError(&'static str);

        impl From<Error> for TypedError {
            fn from(_: Error) -> Self {
                Self("reader")
            }
        }

        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let project_error = archive
            .process_borrowed_blocks_parallel_ordered(
                0..1,
                OrderedParallelBlockConfig {
                    decode_workers: 1,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok::<_, TypedError>(()),
                |_, _, _| Err::<(), _>(TypedError("project")),
                |_, ()| -> std::result::Result<(), TypedError> {
                    panic!("failed projection published a result")
                },
            )
            .unwrap_err();
        assert_eq!(project_error.0, "project");

        let consume_error = archive
            .process_borrowed_blocks_parallel_ordered(
                0..1,
                OrderedParallelBlockConfig {
                    decode_workers: 1,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok::<_, TypedError>(()),
                |_, _, _| Ok::<_, TypedError>(()),
                |_, ()| Err(TypedError("consume")),
            )
            .unwrap_err();
        assert_eq!(consume_error.0, "consume");
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
                |_| Ok(()),
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
                |_| Ok(()),
                |_, _, block| Ok(block.header().slot),
                |row_number, _| {
                    Err(Error::InvalidMetadata(format!(
                        "stop while consuming row {row_number}"
                    )))
                },
            )
            .unwrap_err();

        assert!(matches!(error, Error::InvalidMetadata(_)));
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
                |_| Ok(()),
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
    fn recycled_borrowed_read_matches_owned_and_reuses_both_buffers() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let expected = archive.read_block(0).unwrap();
        let mut scratch = RecycledBlockScratch::new();

        for pass in 0..2 {
            let block = archive
                .read_borrowed_block_reusing(0, &mut scratch, false)
                .unwrap();
            assert!(!block.uses_owned_fallback());
            assert_eq!(block.index_row.block_id, expected.index_row.block_id);
            assert_eq!(block.index_row.slot, expected.index_row.slot);
            assert_eq!(
                block.index_row.compressed_offset,
                expected.index_row.compressed_offset
            );
            assert_eq!(
                block.index_row.first_tx_ordinal,
                expected.index_row.first_tx_ordinal
            );
            assert_eq!(
                block.index_row.first_signature_ordinal,
                expected.index_row.first_signature_ordinal
            );
            assert_eq!(block.header().slot, expected.block.header.slot);
            assert_eq!(
                block.header().parent_slot,
                expected.block.header.parent_slot
            );
            assert_eq!(block.tx_count(), expected.block.tx_count);
            assert_eq!(block.tx_rows().collect::<Vec<_>>(), expected.block.tx_rows);
            assert_eq!(block.message_bytes(), expected.block.message_bytes);
            assert_eq!(block.metadata_bytes(), expected.block.metadata_bytes);
            drop(block);

            let stats = scratch.stats();
            assert_eq!(stats.block_count, pass + 1);
            assert_eq!(stats.compressed_buffer_growths, 1);
            assert_eq!(stats.decompressed_buffer_growths, 1);
            assert!(stats.compressed_buffer_capacity >= expected.index_row.compressed_len as usize);
            assert!(
                stats.decompressed_buffer_capacity >= expected.index_row.uncompressed_len as usize
            );
        }

        let stats = scratch.stats();
        assert_eq!(
            stats.compressed_bytes,
            u64::from(expected.index_row.compressed_len) * 2
        );
        assert_eq!(
            stats.uncompressed_bytes,
            u64::from(expected.index_row.uncompressed_len) * 2
        );
    }

    #[test]
    fn borrowed_into_owned_matches_owned_decode_and_rejects_discarded_rewards() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let expected = archive.read_block(0).unwrap();
        let expected_bytes =
            wincode::config::serialize(&expected.block, wincode_leb128_config()).unwrap();
        let expected_rewards_field =
            wincode::config::serialize(&expected.block.header.rewards, wincode_leb128_config())
                .unwrap();
        let mut scratch = RecycledBlockScratch::new();

        let decoded = archive
            .read_borrowed_block_reusing(0, &mut scratch, false)
            .unwrap();
        assert!(
            decoded
                .rewards_field_bytes()
                .unwrap_err()
                .to_string()
                .contains("after decoding rewards")
        );
        let converted = decoded.into_owned().unwrap();
        assert_eq!(converted.index_row.block_id, expected.index_row.block_id);
        assert_eq!(converted.index_row.slot, expected.index_row.slot);
        assert_eq!(
            wincode::config::serialize(&converted.block, wincode_leb128_config()).unwrap(),
            expected_bytes
        );

        let discarded = archive
            .read_borrowed_block_reusing(0, &mut scratch, true)
            .unwrap();
        assert_eq!(
            discarded.rewards_field_bytes().unwrap(),
            expected_rewards_field
        );
        let error = discarded.into_owned().unwrap_err();
        assert!(error.to_string().contains("rewards were discarded"));
    }

    #[test]
    fn borrowed_recycled_read_requires_one_exact_zstd_frame() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let mut row = archive.index().rows[0];
        let source = fixture.source();
        let mut compressed = source
            .read_range(
                BLOCKS_FILE,
                row.compressed_offset,
                row.compressed_len as usize,
            )
            .unwrap();
        let second_frame = compressed.clone();
        compressed.extend_from_slice(&second_frame);
        row.compressed_len = compressed.len() as u32;

        let error = archive
            .decode_compressed_block(row, &compressed)
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
                |_| Ok(()),
                |_, _, _| -> Result<()> { Ok(()) },
                |_, ()| -> Result<()> { Ok(()) },
            )
            .unwrap_err();
        assert!(error.to_string().contains("first zstd frame"));
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
            assert!(
                decoded
                    .rewards_field_bytes()
                    .unwrap_err()
                    .to_string()
                    .contains("owned fallback")
            );
            assert_eq!(decoded.header().slot, 777);
            assert_eq!(decoded.tx_rows_len(), 0);
        }
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
            manifest.file(GENESIS_BIN_FILE).map(|file| file.size),
            manifest.epoch,
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
            manifest.file(GENESIS_BIN_FILE).map(|file| file.size),
            manifest.epoch,
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

    fn bind_schema_marker(root: &Path, name: &str, bytes: &[u8]) {
        fs::write(root.join(name), bytes).unwrap();
        let manifest_bytes = fs::read(root.join(GENERATION_MANIFEST_FILE)).unwrap();
        let mut manifest = GenerationManifest::parse(&manifest_bytes).unwrap();
        manifest.files.push(GenerationFile {
            name: name.to_owned(),
            size: bytes.len() as u64,
            sha256: hex_lower(&Sha256::digest(bytes)),
        });
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
