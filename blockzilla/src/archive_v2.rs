use anyhow::{Context, Result, anyhow, bail};
use bincode::Options;
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES, ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC, ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_RAW_BLOCKS_FILE,
    ARCHIVE_V2_RAW_BLOCKS_ZSTD_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX, ARCHIVE_V2_TX_FLAG_HAS_ERROR,
    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
    ARCHIVE_V2_TX_FLAG_HAS_LOGS, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA, ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ArchiveV2BlockAccessBlob, ArchiveV2BlockAccessBlockhash, ArchiveV2BlockAccessIndexRow,
    ArchiveV2BlockAccessPubkey, ArchiveV2BlockAccessVoteHash,
    ArchiveV2ComputeBudgetInstructionData, ArchiveV2GetBlockIndexRow, ArchiveV2HotBlockBlob,
    ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotInstruction,
    ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
    ArchiveV2HotMetaRecord, ArchiveV2HotRewards, ArchiveV2HotTxRow, ArchiveV2HotV0Message,
    ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef, ArchiveV2VoteLockoutOffset,
    ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync, BLOCK_TIME_GAP_FILE, CompactBlockHeader,
    CompactInnerInstruction, CompactInnerInstructions, CompactLogStream, CompactMessageHeader,
    CompactMetaV1, CompactPohEntry, CompactPubkey, CompactReturnData, CompactReward,
    CompactShredding, CompactTokenBalance, CompactTransactionError, KeyIndex, KeyStore,
    LIVE_PRE_HOT_BLOCK_VERSION, LIVE_PUBKEY_RUN_HOT_FILE, LIVE_PUBKEY_RUN_RECORD_LEN,
    LIVE_PUBKEY_RUNS_DIR, LivePreHotBlock, LivePreHotRecord, LogEvent,
    OwnedCompactAddressTableLookup, OwnedCompactInstruction, OwnedCompactLegacyMessage,
    OwnedCompactMessage, OwnedCompactRecentBlockhash, OwnedCompactTransaction,
    OwnedCompactV0Message, SplitCompactIndexRecord, WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
    WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS, WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY,
    WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY,
    WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WINCODE_ARCHIVE_V2_VERSION, WincodeArchiveV2Block,
    WincodeArchiveV2BlockHeader, WincodeArchiveV2Footer, WincodeArchiveV2Genesis,
    WincodeArchiveV2GenesisAccount, WincodeArchiveV2GenesisBuiltin,
    WincodeArchiveV2GenesisEpochSchedule, WincodeArchiveV2GenesisFeeParams,
    WincodeArchiveV2GenesisInflationParams, WincodeArchiveV2GenesisPohParams,
    WincodeArchiveV2GenesisRentParams, WincodeArchiveV2Header,
    WincodeArchiveV2NoRegistryAddressTableLookup, WincodeArchiveV2NoRegistryBlock,
    WincodeArchiveV2NoRegistryBlockHeader, WincodeArchiveV2NoRegistryGenesis,
    WincodeArchiveV2NoRegistryGenesisAccount, WincodeArchiveV2NoRegistryGenesisBuiltin,
    WincodeArchiveV2NoRegistryInstruction, WincodeArchiveV2NoRegistryLegacyMessage,
    WincodeArchiveV2NoRegistryLogs, WincodeArchiveV2NoRegistryMessage,
    WincodeArchiveV2NoRegistryMeta, WincodeArchiveV2NoRegistryRecord,
    WincodeArchiveV2NoRegistryReturnData, WincodeArchiveV2NoRegistryReward,
    WincodeArchiveV2NoRegistryRewards, WincodeArchiveV2NoRegistryTokenBalance,
    WincodeArchiveV2NoRegistryTransaction, WincodeArchiveV2NoRegistryTx,
    WincodeArchiveV2NoRegistryV0Message, WincodeArchiveV2Payload, WincodeArchiveV2PohRecord,
    WincodeArchiveV2Record, WincodeArchiveV2Rewards, WincodeArchiveV2ShreddingRecord,
    WincodeArchiveV2Transaction, WincodeLeb128FramedReader, WincodeLeb128FramedWriter,
    archive_v2_get_block_index_path, archive_v2_hot_index_path,
    deserialize_archive_v2_hot_block_blob, encode_with_scratch,
    live_producer::LivePubkeyCountRecord,
    program_logs::{
        ProgramLog,
        system_program::{PubkeyOrString, SystemAddress, SystemProgramLog},
        token_2022::Token2022Log,
    },
    read_archive_v2_block_access_index, read_archive_v2_hot_block_index, wincode_leb128_config,
    write_archive_v2_block_access_index, write_archive_v2_get_block_index,
    write_archive_v2_hot_block_index, write_registry_iter, write_u32_varint,
};
use core::mem::MaybeUninit;
use gxhash::{GxBuildHasher, HashMap as GxHashMap, gxhash128};
use hashbrown::HashTable;
use memmap2::{Mmap, MmapOptions};
use of_car_reader::{
    CarBlockReader,
    confirmed_block::{Rewards, TransactionStatusMeta},
    genesis::{GenesisAccountEntry, GenesisArchive},
    metadata_decoder::{
        InnerInstructionVisit, ReturnDataVisit, TokenBalanceVisit, TransactionStatusMetaVisitor,
        ZstdReusableDecoder, decode_rewards_from_frame, decode_transaction_status_meta_from_frame,
        slot_uses_protobuf_metadata, visit_protobuf_transaction_status_meta,
    },
    node::{decode_entry_hash, is_block_node, is_entry_node},
    reader::CarPayloadRead,
    reconstruct::{
        Cid36, RawBlockNode, RawDataFrame, RawEntryNode, RawNode, RawRewardsNode,
        RawTransactionNode, StandaloneDataFrame, decode_raw_node,
        decode_raw_node_with_data_buffers,
    },
    versioned_transaction::{VersionedMessage, VersionedTransaction},
};
use prost::Message;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use solana_pubkey::Pubkey;
use solana_vote_interface::{
    instruction::VoteInstruction,
    state::{TowerSync as SolanaTowerSync, VoteStateUpdate as SolanaVoteStateUpdate},
};
#[cfg(unix)]
use std::os::unix::fs::{FileExt, MetadataExt};
use std::{
    cell::RefCell,
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque},
    fs::{File, OpenOptions},
    hash::BuildHasher,
    io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime},
};
use tracing::{info, warn};
use wincode::{
    ReadResult, SchemaRead,
    config::{Config, ConfigCore},
    io::Reader,
    len::SeqLen,
};

use crate::{
    BUFFER_SIZE, CarBenchInputFormat, ProgressTracker, genesis_epoch0,
    split_compact::{
        ExternalBlockhashOverrides, build_registry_and_blockhash_for_input,
        load_blockhash_registry_plain, load_external_blockhash_overrides,
    },
};

pub(crate) mod repair;

const ARCHIVE_FILE: &str = "archive-v2.wincode";
const ARCHIVE_NO_REGISTRY_FILE: &str = "archive-v2-no-registry.wincode";
const ARCHIVE_PRE_HOT_FILE: &str = "archive-v2-pre-hot.zstd";
const FIRST_SEEN_REGISTRY_MANIFEST_FILE: &str = ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE;
const FIRST_SEEN_HOT_SEED_FILE: &str = ARCHIVE_V2_PUBKEY_HOT_SEED_FILE;
const FIRST_SEEN_SEED_READ_BUFFER_MIN: usize = 64 << 10;
const ARCHIVE_PRE_HOT_MAGIC: &[u8; 8] = b"BZPHOT01";
const ARCHIVE_PRE_HOT_IO_BUFFER: usize = 8 << 20;
const ARCHIVE_PRE_HOT_MAX_RECORD_BYTES: usize = 512 << 20;
const DETAILED_TIMINGS_ENV: &str = "BLOCKZILLA_DETAILED_TIMINGS";
const FIRST_SEEN_DECODE_WORKERS_MAX: usize = 8;
const FIRST_SEEN_ZSTD_PREFETCH_MIB_MAX: usize = 64;
const FIRST_SEEN_PARALLEL_MIN_TRANSACTIONS: usize = 64;
const FIRST_SEEN_WORKER_SCRATCH_MAX_RETAINED_BYTES: usize = 32 << 20;
const FIRST_SEEN_WORKER_TX_SCRATCH_INITIAL_BYTES: usize = 2 << 10;
const FIRST_SEEN_WORKER_METADATA_SCRATCH_INITIAL_BYTES: usize = 8 << 10;
const RAW_DATAFRAME_POOL_MAX_RETAINED_BYTES: usize = 256 << 20;
const RAW_DATAFRAME_POOL_MAX_BUFFER_BYTES: usize = 32 << 20;
const RAW_DATAFRAME_POOL_MAX_BUFFERS_PER_CLASS: usize = 8_192;
const RAW_DATAFRAME_POOL_MAX_CLASS_ZERO_BUFFERS: usize = 4_096;
const RAW_DATAFRAME_POOL_LARGER_CLASS_PROBES: usize = 2;
const ARCHIVE_ZSTD_BLOCKS_FILE: &str = "archive-v2-blocks.zstd";
const ARCHIVE_ZSTD_INDEX_FILE: &str = "archive-v2-blocks.index";
const ARCHIVE_ZSTD_META_FILE: &str = "archive-v2-meta.wincode";
const ARCHIVE_RAW_BLOCKS_FILE: &str = ARCHIVE_V2_RAW_BLOCKS_FILE;
const ARCHIVE_RAW_BLOCKS_ZSTD_FILE: &str = ARCHIVE_V2_RAW_BLOCKS_ZSTD_FILE;
const POH_FILE: &str = ARCHIVE_V2_POH_FILE;
const SHREDDING_FILE: &str = ARCHIVE_V2_SHREDDING_FILE;
const REGISTRY_FILE: &str = ARCHIVE_V2_PUBKEY_REGISTRY_FILE;
const REGISTRY_COUNTS_FILE: &str = ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE;
const REGISTRY_INDEX_FILE: &str = ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE;
const BLOCKHASH_REGISTRY_FILE: &str = ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE;
const BLOCKHASH_INDEX_V3_FILE: &str = ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE;
const PREV_BLOCKHASH_TAIL_FILE: &str = ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE;
const ARCHIVE_INDEX_FILE: &str = "archive-v2.index";
const ARCHIVE_FLAGS_LEB128: u32 = WINCODE_ARCHIVE_V2_FLAG_LEB128;
const ARCHIVE_FLAGS_NO_REGISTRY: u32 = WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY;
const ARCHIVE_FLAGS_FIRST_SEEN_REGISTRY: u32 = WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY;
const ARCHIVE_FLAGS_ALL_PUBKEY_REF_COUNTS: u32 = WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS;
const ROLLING_BLOCKHASH_CAPACITY: usize = 300;
const RECENT_BLOCKHASH_SLOT_WINDOW: u64 = 150;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum LiveRegistrySource {
    Auto,
    Counts,
    Runs,
    Touches,
    Scan,
}

const ARCHIVE_V2_INDEX_MAGIC: &[u8; 8] = b"BZV2IDX1";
const ARCHIVE_V2_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8;
const ARCHIVE_V2_INDEX_ROW_LEN: usize = 4 + 8 + 8 + 8 + 4 + 4;
const ARCHIVE_V2_ZSTD_INDEX_MAGIC: &[u8; 8] = b"BZV2ZIX1";
const ARCHIVE_V2_ZSTD_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY: u32 = 1 << 0;
const ARCHIVE_V2_INDEX_SCAN_BUFFER_SIZE: usize = 4 << 10;
const BLOCKHASH_SCAN_BUFFER_SIZE: usize = 4 << 20;
// The legacy CAR hot-block rewrite keeps one buffer for each output sidecar.
// Using the process-wide 256 MiB buffer here reserved 1.25 GiB before access
// output (1.5 GiB with it), even though all writes are sequential. 16 MiB is
// large enough to amortize NAS writes while keeping parallel rewrite lanes
// bounded.
const HOT_BLOCK_OUTPUT_BUFFER_SIZE: usize = 16 << 20;
const LIVE_FINALIZER_IO_BUFFER_SIZE: usize = 8 << 20;
const LIVE_PUBKEY_RUN_READER_BUFFER_SIZE: usize = 64 << 10;
const LIVE_FINALIZER_MAX_FRAME_SIZE: usize = 256 << 20;
const LIVE_PUBKEY_RUN_MERGE_FAN_IN: usize = 64;
const LIVE_REGISTRY_PREPARED_MARKER: &str = "archive-v2-live-registry-prepared.v1";
const LIVE_REGISTRY_PREPARE_TEMP_DIR: &str = ".archive-v2-live-registry-prepare.tmp";
const ZSTD_LONG_WINDOW_LOG_MAX: u32 = 31;
const NODE_KIND_PREFIX_LEN: usize = 2;
const BLOCKHASH_SCAN_PREFIX_LEN: usize = 96;
const HOT_REUSABLE_BUFFER_RETAIN_LIMIT: usize = 64 << 20;
const VOTE_INSTRUCTION_DECODE_LIMIT: u64 = 1232;
const WINCODE_ARCHIVE_V2_RECORD_HEADER_TAG: u8 = 0;
const WINCODE_ARCHIVE_V2_RECORD_BLOCK_TAG: u8 = 1;
const WINCODE_ARCHIVE_V2_RECORD_INDEX_TAG: u8 = 2;
const WINCODE_ARCHIVE_V2_RECORD_FOOTER_TAG: u8 = 3;
const WINCODE_ARCHIVE_V2_RECORD_GENESIS_TAG: u8 = 4;

fn zstd_decoder_with_long_window<R: BufRead>(
    reader: R,
) -> Result<zstd::stream::read::Decoder<'static, R>> {
    let mut decoder = zstd::stream::read::Decoder::with_buffer(reader)
        .context("create owned zstd stream decoder")?;
    decoder
        .window_log_max(ZSTD_LONG_WINDOW_LOG_MAX)
        .with_context(|| format!("set zstd WindowLogMax={ZSTD_LONG_WINDOW_LOG_MAX}"))?;
    Ok(decoder)
}

#[derive(Clone, Debug)]
struct StoredIoError {
    kind: ErrorKind,
    message: Arc<str>,
}

impl StoredIoError {
    fn new(kind: ErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn to_io_error(&self) -> std::io::Error {
        std::io::Error::new(self.kind, SharedIoError(Arc::clone(&self.message)))
    }
}

impl From<std::io::Error> for StoredIoError {
    fn from(error: std::io::Error) -> Self {
        Self::new(error.kind(), Arc::<str>::from(error.to_string()))
    }
}

#[derive(Clone, Debug)]
struct SharedIoError(Arc<str>);

impl std::fmt::Display for SharedIoError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SharedIoError {}

enum ZstdPrefetchEvent {
    Data { buffer: Box<[u8]>, len: usize },
    Terminal(std::result::Result<(), StoredIoError>),
}

#[derive(Default)]
struct ZstdPrefetchStats {
    worker_bytes: AtomicU64,
    worker_chunks: AtomicU64,
    consumer_bytes: AtomicU64,
    consumer_reads: AtomicU64,
    worker_ready_wait_ns: AtomicU64,
    worker_recycle_wait_ns: AtomicU64,
    consumer_ready_wait_ns: AtomicU64,
    consumer_recycle_wait_ns: AtomicU64,
    #[cfg(test)]
    worker_buffer_addresses: std::sync::Mutex<HashSet<usize>>,
}

#[derive(Copy, Clone, Debug, Default)]
struct ZstdPrefetchStatsSnapshot {
    worker_bytes: u64,
    worker_chunks: u64,
    consumer_bytes: u64,
    consumer_reads: u64,
    worker_ready_wait: Duration,
    worker_recycle_wait: Duration,
    consumer_ready_wait: Duration,
    consumer_recycle_wait: Duration,
}

impl ZstdPrefetchStats {
    fn snapshot(&self) -> ZstdPrefetchStatsSnapshot {
        ZstdPrefetchStatsSnapshot {
            worker_bytes: self.worker_bytes.load(Ordering::Relaxed),
            worker_chunks: self.worker_chunks.load(Ordering::Relaxed),
            consumer_bytes: self.consumer_bytes.load(Ordering::Relaxed),
            consumer_reads: self.consumer_reads.load(Ordering::Relaxed),
            worker_ready_wait: Duration::from_nanos(
                self.worker_ready_wait_ns.load(Ordering::Relaxed),
            ),
            worker_recycle_wait: Duration::from_nanos(
                self.worker_recycle_wait_ns.load(Ordering::Relaxed),
            ),
            consumer_ready_wait: Duration::from_nanos(
                self.consumer_ready_wait_ns.load(Ordering::Relaxed),
            ),
            consumer_recycle_wait: Duration::from_nanos(
                self.consumer_recycle_wait_ns.load(Ordering::Relaxed),
            ),
        }
    }
}

fn add_prefetch_wait(counter: &AtomicU64, elapsed: Duration) {
    let nanoseconds = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
    counter.fetch_add(nanoseconds, Ordering::Relaxed);
}

struct ZstdPrefetchChunk {
    buffer: Box<[u8]>,
    len: usize,
    offset: usize,
}

/// A two-buffer, allocation-stable bridge between sequential zstd decoding and
/// the CAR parser. The worker is strictly ordered: there is only one decoder
/// and chunks are delivered through a bounded FIFO channel.
struct ZstdPrefetchReader {
    ready_rx: Option<mpsc::Receiver<ZstdPrefetchEvent>>,
    recycle_tx: Option<mpsc::SyncSender<Box<[u8]>>>,
    current: Option<ZstdPrefetchChunk>,
    terminal: Option<std::result::Result<(), StoredIoError>>,
    worker: Option<thread::JoinHandle<()>>,
    stats: Arc<ZstdPrefetchStats>,
}

impl ZstdPrefetchReader {
    fn start<R: Read + Send + 'static>(
        reader: R,
        chunk_bytes: usize,
    ) -> std::io::Result<(Self, Arc<ZstdPrefetchStats>)> {
        if chunk_bytes == 0 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "zstd prefetch chunk size must be non-zero",
            ));
        }

        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (recycle_tx, recycle_rx) = mpsc::sync_channel(2);
        // These are the only chunk allocations for the lifetime of the
        // prefetcher. Ownership rotates worker -> consumer -> worker.
        recycle_tx
            .send(vec![0_u8; chunk_bytes].into_boxed_slice())
            .expect("fresh recycle channel must accept first buffer");
        recycle_tx
            .send(vec![0_u8; chunk_bytes].into_boxed_slice())
            .expect("fresh recycle channel must accept second buffer");

        let stats = Arc::new(ZstdPrefetchStats::default());
        let worker_stats = Arc::clone(&stats);
        let worker = thread::Builder::new()
            .name("first-seen-zstd-prefetch".to_string())
            .spawn(move || {
                run_zstd_prefetch_worker(reader, ready_tx, recycle_rx, &worker_stats);
            })?;
        Ok((
            Self {
                ready_rx: Some(ready_rx),
                recycle_tx: Some(recycle_tx),
                current: None,
                terminal: None,
                worker: Some(worker),
                stats: Arc::clone(&stats),
            },
            stats,
        ))
    }

    fn join_worker(&mut self) -> std::result::Result<(), StoredIoError> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker.join().map_err(|_| {
            StoredIoError::new(
                ErrorKind::Other,
                Arc::<str>::from("zstd prefetch worker panicked"),
            )
        })
    }

    fn recycle(&self, buffer: Box<[u8]>) {
        let Some(recycle_tx) = self.recycle_tx.as_ref() else {
            return;
        };
        let wait_started = Instant::now();
        let _ = recycle_tx.send(buffer);
        add_prefetch_wait(&self.stats.consumer_recycle_wait_ns, wait_started.elapsed());
    }

    fn terminal_read(&self) -> std::io::Result<usize> {
        match self.terminal.as_ref() {
            Some(Ok(())) => Ok(0),
            Some(Err(error)) => Err(error.to_io_error()),
            None => Err(std::io::Error::other(
                "zstd prefetch reader has no terminal state",
            )),
        }
    }
}

impl Read for ZstdPrefetchReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }

        loop {
            if let Some(current) = self.current.as_mut() {
                let available = current.len - current.offset;
                let copied = available.min(output.len());
                output[..copied]
                    .copy_from_slice(&current.buffer[current.offset..current.offset + copied]);
                current.offset += copied;
                self.stats
                    .consumer_bytes
                    .fetch_add(copied as u64, Ordering::Relaxed);
                self.stats.consumer_reads.fetch_add(1, Ordering::Relaxed);
                if current.offset == current.len {
                    let current = self.current.take().expect("current chunk exists");
                    self.recycle(current.buffer);
                }
                return Ok(copied);
            }

            if self.terminal.is_some() {
                return self.terminal_read();
            }

            let wait_started = Instant::now();
            let event = self
                .ready_rx
                .as_ref()
                .expect("active prefetch reader must have a ready receiver")
                .recv();
            add_prefetch_wait(&self.stats.consumer_ready_wait_ns, wait_started.elapsed());
            match event {
                Ok(ZstdPrefetchEvent::Data { buffer, len }) => {
                    if len == 0 || len > buffer.len() {
                        self.terminal = Some(Err(StoredIoError::new(
                            ErrorKind::InvalidData,
                            Arc::<str>::from("zstd prefetch worker returned an invalid chunk"),
                        )));
                        drop(buffer);
                        self.recycle_tx.take();
                        self.ready_rx.take();
                        if let Err(join_error) = self.join_worker() {
                            self.terminal = Some(Err(join_error));
                        }
                        continue;
                    }
                    self.current = Some(ZstdPrefetchChunk {
                        buffer,
                        len,
                        offset: 0,
                    });
                }
                Ok(ZstdPrefetchEvent::Terminal(terminal)) => {
                    self.terminal = Some(terminal);
                    if let Err(join_error) = self.join_worker() {
                        self.terminal = Some(Err(join_error));
                    }
                }
                Err(_) => {
                    self.terminal = Some(Err(match self.join_worker() {
                        Ok(()) => StoredIoError::new(
                            ErrorKind::UnexpectedEof,
                            Arc::<str>::from(
                                "zstd prefetch worker disconnected without a terminal event",
                            ),
                        ),
                        Err(error) => error,
                    }));
                }
            }
        }
    }
}

impl Drop for ZstdPrefetchReader {
    fn drop(&mut self) {
        // Closing both directions before join wakes a worker blocked on either
        // a ready send or a recycle receive. The worker may still be inside the
        // underlying Read until that call returns.
        self.current.take();
        self.recycle_tx.take();
        self.ready_rx.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn run_zstd_prefetch_worker<R: Read>(
    mut reader: R,
    ready_tx: mpsc::SyncSender<ZstdPrefetchEvent>,
    recycle_rx: mpsc::Receiver<Box<[u8]>>,
    stats: &ZstdPrefetchStats,
) {
    loop {
        let wait_started = Instant::now();
        let Ok(mut buffer) = recycle_rx.recv() else {
            return;
        };
        add_prefetch_wait(&stats.worker_recycle_wait_ns, wait_started.elapsed());
        #[cfg(test)]
        stats
            .worker_buffer_addresses
            .lock()
            .expect("prefetch buffer address lock poisoned")
            .insert(buffer.as_ptr() as usize);

        let mut filled = 0;
        let terminal = loop {
            match reader.read(&mut buffer[filled..]) {
                Ok(0) => break Some(Ok(())),
                Ok(read) => {
                    filled += read;
                    if filled == buffer.len() {
                        break None;
                    }
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error) => break Some(Err(StoredIoError::from(error))),
            }
        };

        if filled != 0 {
            let wait_started = Instant::now();
            if ready_tx
                .send(ZstdPrefetchEvent::Data {
                    buffer,
                    len: filled,
                })
                .is_err()
            {
                return;
            }
            add_prefetch_wait(&stats.worker_ready_wait_ns, wait_started.elapsed());
            stats
                .worker_bytes
                .fetch_add(filled as u64, Ordering::Relaxed);
            stats.worker_chunks.fetch_add(1, Ordering::Relaxed);
        }

        if let Some(terminal) = terminal {
            let wait_started = Instant::now();
            let _ = ready_tx.send(ZstdPrefetchEvent::Terminal(terminal));
            add_prefetch_wait(&stats.worker_ready_wait_ns, wait_started.elapsed());
            return;
        }
    }
}

pub(crate) fn build(
    input: &Path,
    output_dir: &Path,
    previous_car: Option<&Path>,
    resume: bool,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let previous_tail = load_or_build_previous_tail(output_dir, previous_car, resume)?;
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;
    let external_blockhashes = ExternalBlockhashOverrides::default();

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let blockhash_has_required_genesis = match &genesis {
        Some(genesis) if crate::file_nonempty(&blockhash_registry_path) => {
            genesis_epoch0::blockhash_registry_starts_with(
                &blockhash_registry_path,
                &genesis.genesis_hash,
            )?
        }
        Some(_) => false,
        None => true,
    };
    if !(resume
        && crate::file_nonempty(&registry_path)
        && crate::file_nonempty(&registry_counts_path)
        && crate::file_nonempty(&blockhash_registry_path)
        && blockhash_has_required_genesis)
    {
        build_registry_and_blockhash_for_input(
            input,
            &registry_path,
            Some(&registry_counts_path),
            &blockhash_registry_path,
            &external_blockhashes,
            None,
        )?;
    }

    let store = KeyStore::load(&registry_path)?;
    let key_index = KeyIndex::build(store.keys);
    let blockhashes = load_blockhash_registry_plain(&blockhash_registry_path)?;
    let blockhash_id_offset = blockhash_id_offset_for_genesis(&genesis, &blockhashes)?;

    let archive_path = output_dir.join(ARCHIVE_FILE);
    let poh_path = output_dir.join(POH_FILE);
    let archive_file = File::create(&archive_path)
        .with_context(|| format!("create {}", archive_path.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, archive_file));
    let poh_file =
        File::create(&poh_path).with_context(|| format!("create {}", poh_path.display()))?;
    let mut poh_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, poh_file));
    writer.write(&WincodeArchiveV2Record::Header(WincodeArchiveV2Header {
        version: WINCODE_ARCHIVE_V2_VERSION,
        flags: ARCHIVE_FLAGS_LEB128,
    }))?;
    if let Some(genesis) = &genesis {
        writer.write(&WincodeArchiveV2Record::Genesis(compact_genesis_record(
            genesis, &key_index,
        )?))?;
    }

    let mut scanner = RawCarScanner::open_with_buffer(input, BLOCKHASH_SCAN_BUFFER_SIZE)?;
    scanner.skip_header()?;
    let mut pending = PendingBlock::default();
    let mut footer = WincodeArchiveV2Footer::default();
    let mut timings = ArchiveV2Timings::from_env();
    let mut progress = ProgressTracker::new("Archive V2 Write");
    let mut block_offset = 0u64;
    let mut block_id = 0u32;
    let mut block_scratch = Vec::with_capacity(8 << 20);
    let mut metadata_zstd = ZstdReusableDecoder::new();
    let mut rolling_blockhashes = RollingBlockhashIndex::new(ROLLING_BLOCKHASH_CAPACITY);
    rolling_blockhashes.seed_previous_tail(&previous_tail)?;
    if let Some(genesis) = &genesis {
        rolling_blockhashes.insert(genesis.genesis_hash, 0, 0)?;
    }

    while let Some(raw) = scanner
        .next_node_timed(Some(&mut timings))
        .with_context(|| {
            format!(
                "scan hot archive input {} after blocks={} car_entries={}",
                input.display(),
                footer.blocks,
                footer.car_entries
            )
        })?
    {
        footer.car_entries += 1;
        footer.car_payload_bytes += raw.payload_len as u64;
        footer.decoded_node_payload_bytes += raw.payload_len as u64;

        let classify_started = timings.detail_timer();
        match raw.node {
            RawNode::Transaction(tx) => {
                footer.transactions += 1;
                pending.transactions.push(PendingTx {
                    tx,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Entry(entry) => {
                footer.entries += 1;
                pending.entries.push(PendingEntry {
                    entry,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Rewards(rewards) => {
                footer.rewards += 1;
                anyhow::ensure!(
                    pending.rewards.is_none(),
                    "duplicate rewards node before block"
                );
                pending.rewards = Some(PendingRewards {
                    rewards,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::DataFrame(frame) => {
                footer.dataframes += 1;
                pending.dataframes.insert(frame.cid, frame);
            }
            RawNode::Block(block) => {
                footer.blocks += 1;
                rolling_blockhashes.prune_for_slot(block.slot)?;
                timings.classify += classify_started.elapsed();
                let blockhash_index = block_id as usize + blockhash_id_offset as usize;
                let previous_blockhash = blockhash_index
                    .checked_sub(1)
                    .and_then(|index| blockhashes.get(index))
                    .copied();
                let (record, tx_count, sidecar) = build_block_record(
                    &mut pending,
                    block,
                    &key_index,
                    &rolling_blockhashes,
                    block_id.saturating_add(blockhash_id_offset),
                    &mut footer,
                    &mut timings,
                    &external_blockhashes,
                    previous_blockhash,
                    &mut metadata_zstd,
                    None,
                    None,
                )?;
                let expected_blockhash = blockhashes.get(blockhash_index).with_context(|| {
                    format!("missing blockhash registry entry for blockhash id {blockhash_index}")
                })?;
                anyhow::ensure!(
                    sidecar.blockhash == *expected_blockhash,
                    "blockhash registry mismatch at block_id {} slot {}",
                    block_id,
                    pending.last_slot
                );
                let encode_started = timings.detail_timer();
                let record = WincodeArchiveV2Record::Block(record);
                encode_with_scratch(&record, &mut block_scratch)?;
                let block_len = u32::try_from(block_scratch.len())
                    .context("archive v2 frame exceeds u32::MAX")?;
                writer.write_bytes(&block_scratch)?;
                writer.write(&WincodeArchiveV2Record::Index(SplitCompactIndexRecord {
                    slot: pending.last_slot,
                    block_id,
                    block_offset,
                    block_len,
                    runtime_offset: 0,
                    runtime_len: 0,
                    tx_count,
                }))?;
                poh_writer.write(&WincodeArchiveV2PohRecord {
                    block_id,
                    slot: pending.last_slot,
                    entries: sidecar.poh_entries,
                })?;
                timings.wincode_encode += encode_started.elapsed();
                let current_block_id = i32::try_from(block_id.saturating_add(blockhash_id_offset))
                    .context("blockhash id exceeds i32::MAX")?;
                rolling_blockhashes.insert(
                    sidecar.blockhash,
                    current_block_id,
                    pending.last_slot,
                )?;
                block_offset += block_len as u64;
                block_id = block_id.wrapping_add(1);
                progress.update_slot(pending.last_slot);
                progress.update_input_bytes(footer.car_payload_bytes);
                progress.update(1, tx_count as u64);
                pending.clear();
                continue;
            }
            RawNode::Subset(_) => footer.subset_nodes_ignored += 1,
            RawNode::Epoch(_) => footer.epoch_nodes_ignored += 1,
        }
        timings.classify += classify_started.elapsed();
    }

    let encode_started = timings.detail_timer();
    writer.write(&WincodeArchiveV2Record::Footer(footer.clone()))?;
    timings.wincode_encode += encode_started.elapsed();
    writer.flush()?;
    poh_writer.flush()?;
    progress.final_report();
    info!("Archive V2 build complete");
    info!("  archive: {}", archive_path.display());
    info!("  poh: {}", poh_path.display());
    info!(
        "  blockhash_registry: {}",
        blockhash_registry_path.display()
    );
    info!("  pubkey_registry: {}", registry_path.display());
    info!(
        "  coverage: entries={} payload_bytes={} decoded_payload_bytes={} tx_raw_fallbacks={} metadata_raw_fallbacks={} rewards_raw_fallbacks={} nonce_recent_blockhashes={}",
        footer.car_entries,
        footer.car_payload_bytes,
        footer.decoded_node_payload_bytes,
        footer.tx_raw_fallbacks,
        footer.metadata_raw_fallbacks,
        footer.rewards_raw_fallbacks,
        footer.nonce_recent_blockhashes
    );
    info!(
        "  timings: scan/decode_node={:.3}s classify={:.3}s dataframe_assemble={:.3}s tx_decode_compact={:.3}s metadata_decode_compact={:.3}s rewards_decode_compact={:.3}s wincode_encode={:.3}s",
        timings.scan_decode_node.as_secs_f64(),
        timings.classify.as_secs_f64(),
        timings.dataframe_assemble.as_secs_f64(),
        timings.tx_decode_compact.as_secs_f64(),
        timings.metadata_decode_compact.as_secs_f64(),
        timings.rewards_decode_compact.as_secs_f64(),
        timings.wincode_encode.as_secs_f64(),
    );
    info!(
        "  archive_v2_stats: tx_reassembled={} metadata_reassembled={} metadata_protobuf_visit={} metadata_owned_fallback={} tx_scratch_max={} metadata_scratch_max={}",
        timings.tx_reassembled,
        timings.metadata_reassembled,
        timings.metadata_protobuf_visit,
        timings.metadata_owned_fallback,
        timings.tx_scratch_max,
        timings.metadata_scratch_max,
    );
    Ok(())
}

pub(crate) fn build_hot_blocks(
    input: &Path,
    output_dir: &Path,
    previous_car: Option<&Path>,
    registry_dir: Option<&Path>,
    external_blockhashes_path: Option<&Path>,
    level: i32,
    max_blocks: Option<u64>,
    resume: bool,
    include_access: bool,
) -> Result<()> {
    anyhow::ensure!(
        level >= 0,
        "zstd compression level must be non-negative, got {level}"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let external_blockhashes = load_external_blockhash_overrides(external_blockhashes_path)?;

    let previous_tail = load_or_build_previous_tail(output_dir, previous_car, resume)?;
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    let registry_index_path = output_dir.join(REGISTRY_INDEX_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let blockhash_has_required_genesis = match &genesis {
        Some(genesis) if crate::file_nonempty(&blockhash_registry_path) => {
            genesis_epoch0::blockhash_registry_starts_with(
                &blockhash_registry_path,
                &genesis.genesis_hash,
            )?
        }
        Some(_) => false,
        None => true,
    };
    if let Some(registry_dir) = registry_dir {
        reuse_hot_registry_sidecars(
            registry_dir,
            &registry_path,
            &registry_counts_path,
            &registry_index_path,
            &blockhash_registry_path,
        )?;
    } else if !(resume
        && crate::file_nonempty(&registry_path)
        && crate::file_nonempty(&registry_counts_path)
        && crate::file_nonempty(&blockhash_registry_path)
        && blockhash_has_required_genesis)
    {
        build_registry_and_blockhash_for_input(
            input,
            &registry_path,
            Some(&registry_counts_path),
            &blockhash_registry_path,
            &external_blockhashes,
            max_blocks,
        )?;
    }

    let key_index = load_or_build_registry_key_index(&registry_path, &registry_index_path, resume)?;
    let known_program_ids = KnownProgramIds::from_index(&key_index);
    let access_store = if include_access {
        Some(KeyStore::load(&registry_path)?)
    } else {
        None
    };
    let blockhashes = load_blockhash_registry_plain(&blockhash_registry_path)?;
    let blockhash_id_offset = blockhash_id_offset_for_genesis(&genesis, &blockhashes)?;

    let blocks_path = output_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let index_path = output_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let access_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let access_index_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let meta_path = output_dir.join(ARCHIVE_V2_META_FILE);
    let signatures_path = output_dir.join(ARCHIVE_V2_SIGNATURES_FILE);
    let poh_path = output_dir.join(POH_FILE);
    let shredding_path = output_dir.join(SHREDDING_FILE);
    let vote_hash_registry_path = output_dir.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);

    let blocks_file =
        File::create(&blocks_path).with_context(|| format!("create {}", blocks_path.display()))?;
    let mut blocks_writer = BufWriter::with_capacity(HOT_BLOCK_OUTPUT_BUFFER_SIZE, blocks_file);
    let meta_file =
        File::create(&meta_path).with_context(|| format!("create {}", meta_path.display()))?;
    let mut meta_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        HOT_BLOCK_OUTPUT_BUFFER_SIZE,
        meta_file,
    ));
    let signatures_file = File::create(&signatures_path)
        .with_context(|| format!("create {}", signatures_path.display()))?;
    let mut signatures_writer =
        BufWriter::with_capacity(HOT_BLOCK_OUTPUT_BUFFER_SIZE, signatures_file);
    let mut access_writer = if include_access {
        let access_file = File::create(&access_path)
            .with_context(|| format!("create {}", access_path.display()))?;
        Some(BufWriter::with_capacity(
            HOT_BLOCK_OUTPUT_BUFFER_SIZE,
            access_file,
        ))
    } else {
        None
    };
    let poh_file =
        File::create(&poh_path).with_context(|| format!("create {}", poh_path.display()))?;
    let mut poh_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        HOT_BLOCK_OUTPUT_BUFFER_SIZE,
        poh_file,
    ));
    let shredding_file = File::create(&shredding_path)
        .with_context(|| format!("create {}", shredding_path.display()))?;
    let mut shredding_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        HOT_BLOCK_OUTPUT_BUFFER_SIZE,
        shredding_file,
    ));
    let mut compressor = zstd::bulk::Compressor::new(level).context("create zstd compressor")?;
    let mut block_bytes = Vec::new();
    let mut compressed_buf = Vec::new();
    let mut access_bytes = Vec::new();
    let mut access_signature_bytes = Vec::new();
    let mut hot_block_buffers = HotBlockBuffers::default();

    write_hot_meta(
        &mut meta_writer,
        &ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: ARCHIVE_FLAGS_LEB128,
        }),
    )?;
    if let Some(genesis) = &genesis {
        write_hot_meta(
            &mut meta_writer,
            &ArchiveV2HotMetaRecord::Genesis(compact_genesis_record(genesis, &key_index)?),
        )?;
    }

    let mut blockhash_index_v3 = full_blockhash_index_v3_publisher(output_dir, max_blocks)?;
    let mut scanner = RawCarScanner::open_with_buffer(input, BLOCKHASH_SCAN_BUFFER_SIZE)?;
    scanner.skip_header()?;
    let mut pending = PendingBlock::default();
    let mut footer = WincodeArchiveV2Footer::default();
    let mut timings = ArchiveV2Timings::from_env();
    let mut progress = ProgressTracker::new("Archive V2 Hot Write");
    let mut block_id = 0u32;
    let mut rolling_blockhashes = RollingBlockhashIndex::new(ROLLING_BLOCKHASH_CAPACITY);
    rolling_blockhashes.seed_previous_tail(&previous_tail)?;
    if let Some(genesis) = &genesis {
        rolling_blockhashes.insert(genesis.genesis_hash, 0, 0)?;
    }

    let mut rows = Vec::new();
    let mut access_rows = Vec::new();
    let mut blob_offset = 0u64;
    let mut access_offset = 0u64;
    let mut uncompressed_bytes = 0u64;
    let mut compressed_bytes = 0u64;
    let mut access_file_bytes = 0u64;
    let mut first_tx_ordinal = 0u64;
    let mut first_signature_ordinal = 0u64;
    let mut slot_to_block_id: GxHashMap<u64, u32> =
        GxHashMap::with_hasher(GxBuildHasher::default());
    let mut vote_hashes = VoteHashRegistryBuilder::default();
    let mut metadata_zstd = ZstdReusableDecoder::new();
    let started = Instant::now();

    while let Some(raw) = scanner.next_node_timed(Some(&mut timings))? {
        footer.car_entries += 1;
        footer.car_payload_bytes += raw.payload_len as u64;
        footer.decoded_node_payload_bytes += raw.payload_len as u64;

        let classify_started = timings.detail_timer();
        match raw.node {
            RawNode::Transaction(tx) => {
                footer.transactions += 1;
                pending.transactions.push(PendingTx {
                    tx,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Entry(entry) => {
                footer.entries += 1;
                pending.entries.push(PendingEntry {
                    entry,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Rewards(rewards) => {
                footer.rewards += 1;
                anyhow::ensure!(
                    pending.rewards.is_none(),
                    "duplicate rewards node before block"
                );
                pending.rewards = Some(PendingRewards {
                    rewards,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::DataFrame(frame) => {
                footer.dataframes += 1;
                pending.dataframes.insert(frame.cid, frame);
            }
            RawNode::Block(block) => {
                footer.blocks += 1;
                rolling_blockhashes.prune_for_slot(block.slot)?;
                timings.classify += classify_started.elapsed();
                let blockhash_index = block_id as usize + blockhash_id_offset as usize;
                let previous_blockhash = blockhash_index
                    .checked_sub(1)
                    .and_then(|index| blockhashes.get(index))
                    .copied();
                let (mut record, tx_count, sidecar) = build_block_record(
                    &mut pending,
                    block,
                    &key_index,
                    &rolling_blockhashes,
                    block_id.saturating_add(blockhash_id_offset),
                    &mut footer,
                    &mut timings,
                    &external_blockhashes,
                    previous_blockhash,
                    &mut metadata_zstd,
                    None,
                    None,
                )?;
                let slot = pending.last_slot;
                let block_time = record.header.compact.block_time;
                let expected_blockhash = blockhashes.get(blockhash_index).with_context(|| {
                    format!("missing blockhash registry entry for blockhash id {blockhash_index}")
                })?;
                anyhow::ensure!(
                    sidecar.blockhash == *expected_blockhash,
                    "blockhash registry mismatch at block_id {} slot {}",
                    block_id,
                    slot
                );

                let encode_started = timings.detail_timer();
                vote_hashes.ensure_block(block_id);
                let block_shredding = std::mem::take(&mut record.header.compact.shredding);
                access_signature_bytes.clear();
                let block_signature_bytes = if include_access {
                    Some(&mut access_signature_bytes)
                } else {
                    None
                };
                let (hot_block, block_signature_count) = hot_block_from_archive_block(
                    record,
                    &known_program_ids,
                    &slot_to_block_id,
                    &mut vote_hashes,
                    Some(&mut signatures_writer as &mut dyn Write),
                    block_signature_bytes,
                    &mut hot_block_buffers,
                    &mut timings,
                )
                .with_context(|| format!("slot {slot} hot block encode"))?;
                block_bytes.clear();
                let block_serialize_started = timings.detail_timer();
                wincode::config::serialize_into(
                    &mut block_bytes,
                    &hot_block,
                    wincode_leb128_config(),
                )?;
                timings.hot_block_serialize += block_serialize_started.elapsed();
                let uncompressed_len = u32::try_from(block_bytes.len())
                    .context("hot archive v2 block payload exceeds u32::MAX")?;
                let compress_bound = zstd::zstd_safe::compress_bound(block_bytes.len());
                if compressed_buf.capacity() < compress_bound {
                    compressed_buf.reserve(compress_bound.saturating_sub(compressed_buf.len()));
                    timings.hot_zstd_buffer_reserves += 1;
                }
                timings.hot_zstd_buffer_capacity_max = timings
                    .hot_zstd_buffer_capacity_max
                    .max(compressed_buf.capacity());
                let compress_started = timings.detail_timer();
                compressor
                    .compress_to_buffer(&block_bytes, &mut compressed_buf)
                    .with_context(|| format!("zstd compress hot block_id {block_id}"))?;
                timings.hot_zstd_compress += compress_started.elapsed();
                let compressed_len = u32::try_from(compressed_buf.len())
                    .context("compressed hot archive v2 block exceeds u32::MAX")?;
                let write_started = timings.detail_timer();
                blocks_writer
                    .write_all(&compressed_buf)
                    .with_context(|| format!("write {}", blocks_path.display()))?;
                timings.hot_block_write += write_started.elapsed();
                let poh_started = timings.detail_timer();
                poh_writer.write(&WincodeArchiveV2PohRecord {
                    block_id,
                    slot,
                    entries: sidecar.poh_entries,
                })?;
                shredding_writer.write(&WincodeArchiveV2ShreddingRecord {
                    block_id,
                    slot,
                    shredding: block_shredding,
                })?;
                if let Some(index) = blockhash_index_v3.as_mut() {
                    index.push(slot, &sidecar.blockhash, block_time)?;
                }
                timings.hot_poh_write += poh_started.elapsed();
                if let (Some(access_writer), Some(store)) =
                    (access_writer.as_mut(), access_store.as_ref())
                {
                    access_bytes.clear();
                    let access_blob = build_archive_v2_block_access_blob(
                        &hot_block,
                        &store.keys,
                        &blockhashes,
                        &previous_tail,
                        &access_signature_bytes,
                        &vote_hashes.rows,
                    )
                    .with_context(|| format!("slot {slot} block access sidecar"))?;
                    wincode::config::serialize_into(
                        &mut access_bytes,
                        &access_blob,
                        wincode_leb128_config(),
                    )?;
                    let access_len =
                        checked_archive_v2_block_access_frame_len(access_bytes.len(), slot)?;
                    access_writer
                        .write_all(&access_bytes)
                        .with_context(|| format!("write {}", access_path.display()))?;
                    access_rows.push(ArchiveV2BlockAccessIndexRow {
                        block_id,
                        slot,
                        access_offset,
                        access_len,
                        tx_count,
                        signature_count: block_signature_count,
                    });
                    access_offset += access_len as u64;
                    access_file_bytes += access_len as u64;
                }
                timings.wincode_encode += encode_started.elapsed();
                hot_block_buffers.recycle(hot_block);

                rows.push(ArchiveV2HotBlockIndexRow {
                    block_id,
                    slot,
                    compressed_offset: blob_offset,
                    compressed_len,
                    uncompressed_len,
                    tx_count,
                    first_tx_ordinal,
                    first_signature_ordinal,
                    signature_count: block_signature_count,
                });
                blob_offset += compressed_len as u64;
                uncompressed_bytes += uncompressed_len as u64;
                compressed_bytes += compressed_len as u64;
                first_tx_ordinal += tx_count as u64;
                first_signature_ordinal += block_signature_count as u64;

                let current_block_id = i32::try_from(block_id.saturating_add(blockhash_id_offset))
                    .context("blockhash id exceeds i32::MAX")?;
                rolling_blockhashes.insert(sidecar.blockhash, current_block_id, slot)?;
                slot_to_block_id.insert(slot, block_id);
                block_id = block_id.wrapping_add(1);
                progress.update_slot(slot);
                progress.update_input_bytes(footer.car_payload_bytes);
                progress.update(1, tx_count as u64);
                pending.clear();
                trim_hot_memory(
                    block_id,
                    &mut block_bytes,
                    &mut compressed_buf,
                    &mut access_bytes,
                    include_access.then_some(&mut access_signature_bytes),
                );
                hot_block_buffers.trim();
                if max_blocks.is_some_and(|limit| rows.len() as u64 >= limit) {
                    break;
                }
                continue;
            }
            RawNode::Subset(_) => footer.subset_nodes_ignored += 1,
            RawNode::Epoch(_) => footer.epoch_nodes_ignored += 1,
        }
        timings.classify += classify_started.elapsed();
    }

    blocks_writer
        .flush()
        .with_context(|| format!("flush {}", blocks_path.display()))?;
    signatures_writer
        .flush()
        .with_context(|| format!("flush {}", signatures_path.display()))?;
    if let Some(access_writer) = access_writer.as_mut() {
        access_writer
            .flush()
            .with_context(|| format!("flush {}", access_path.display()))?;
    }
    poh_writer.flush()?;
    shredding_writer.flush()?;
    vote_hashes.write(&vote_hash_registry_path)?;
    write_archive_v2_hot_block_index(&index_path, blob_offset, level, 0, &rows)?;
    if include_access {
        write_archive_v2_block_access_index(&access_index_path, access_offset, 0, &access_rows)?;
    }
    // The metadata footer is the completion marker for this archive. Publish and
    // durably sync the complete timestamp index and its derived gap sidecar first,
    // so a failed publication can never leave an apparently complete archive.
    if let Some(index) = blockhash_index_v3 {
        index.publish(rows.len() as u64)?;
    }
    write_hot_meta(
        &mut meta_writer,
        &ArchiveV2HotMetaRecord::Footer(footer.clone()),
    )?;
    meta_writer.flush()?;
    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    let ratio = if uncompressed_bytes > 0 {
        compressed_bytes as f64 * 100.0 / uncompressed_bytes as f64
    } else {
        0.0
    };
    info!(
        "Archive V2 hot-block build complete in {:.2}s: blocks={} txs={} signatures={} level={} max_blocks={:?} include_access={} uncompressed_bytes={} compressed_bytes={} access_bytes={} ratio_pct={:.2} compact_vote_ix={} vote_bank_hash_refs={} vote_bank_hash_raw={} vote_bank_hash_conflict_raw={} vote_block_id_refs={} vote_block_id_raw={} vote_block_id_zero={} vote_block_id_conflict_raw={} blocks_file={} index={} access={} access_index={} meta={} signatures={} vote_hash_registry={} poh={} shredding={}",
        elapsed,
        rows.len(),
        first_tx_ordinal,
        first_signature_ordinal,
        level,
        max_blocks,
        include_access,
        uncompressed_bytes,
        compressed_bytes,
        access_file_bytes,
        ratio,
        vote_hashes.compact_vote_ix,
        vote_hashes.bank_hash_refs,
        vote_hashes.bank_hash_raw,
        vote_hashes.bank_hash_conflict_raw,
        vote_hashes.block_id_refs,
        vote_hashes.block_id_raw,
        vote_hashes.block_id_zero,
        vote_hashes.block_id_conflict_raw,
        blocks_path.display(),
        index_path.display(),
        if include_access {
            access_path.display().to_string()
        } else {
            "skipped".to_string()
        },
        if include_access {
            access_index_path.display().to_string()
        } else {
            "skipped".to_string()
        },
        meta_path.display(),
        signatures_path.display(),
        vote_hash_registry_path.display(),
        poh_path.display(),
        shredding_path.display()
    );
    info!(
        "Archive V2 hot timings: scan/decode_node={:.3}s classify={:.3}s dataframe_assemble={:.3}s tx_decode_compact={:.3}s metadata_decode_compact={:.3}s rewards_decode_compact={:.3}s hot_message_build={:.3}s hot_message_encode={:.3}s hot_metadata_encode={:.3}s hot_signature_write={:.3}s hot_block_serialize={:.3}s hot_zstd_compress={:.3}s hot_block_write={:.3}s hot_poh_write={:.3}s total_hot_encode_scope={:.3}s",
        timings.scan_decode_node.as_secs_f64(),
        timings.classify.as_secs_f64(),
        timings.dataframe_assemble.as_secs_f64(),
        timings.tx_decode_compact.as_secs_f64(),
        timings.metadata_decode_compact.as_secs_f64(),
        timings.rewards_decode_compact.as_secs_f64(),
        timings.hot_message_build.as_secs_f64(),
        timings.hot_message_encode.as_secs_f64(),
        timings.hot_metadata_encode.as_secs_f64(),
        timings.hot_signature_write.as_secs_f64(),
        timings.hot_block_serialize.as_secs_f64(),
        timings.hot_zstd_compress.as_secs_f64(),
        timings.hot_block_write.as_secs_f64(),
        timings.hot_poh_write.as_secs_f64(),
        timings.wincode_encode.as_secs_f64(),
    );
    info!(
        "Archive V2 hot stats: tx_reassembled={} metadata_reassembled={} metadata_protobuf_visit={} metadata_owned_fallback={} tx_scratch_max={} metadata_scratch_max={} zstd_buffer_reserves={} zstd_buffer_capacity_max={}",
        timings.tx_reassembled,
        timings.metadata_reassembled,
        timings.metadata_protobuf_visit,
        timings.metadata_owned_fallback,
        timings.tx_scratch_max,
        timings.metadata_scratch_max,
        timings.hot_zstd_buffer_reserves,
        timings.hot_zstd_buffer_capacity_max,
    );
    Ok(())
}

struct FirstSeenBlockWriteJob {
    block_bytes: Vec<u8>,
    block_id: u32,
    slot: u64,
    tx_count: u32,
    first_tx_ordinal: u64,
    first_signature_ordinal: u64,
    signature_count: u32,
}

#[derive(Default)]
struct FirstSeenBlockWriteSummary {
    rows: Vec<ArchiveV2HotBlockIndexRow>,
    blob_offset: u64,
    uncompressed_bytes: u64,
    compressed_bytes: u64,
    zstd_compress: Duration,
    block_write: Duration,
    zstd_buffer_reserves: u64,
    zstd_buffer_capacity_max: usize,
}

/// A bounded, ordered compression stage for the first-seen builder.
///
/// The producer and worker own one encoded block buffer each. Once the first
/// block is submitted, CAR decode/encoding can fill the other buffer while the
/// worker compresses and writes. The worker is deliberately single-threaded:
/// it owns one reusable zstd context, one compressed buffer, and the sequential
/// output writer, so output bytes and index order remain deterministic.
struct FirstSeenAsyncBlockWriter {
    jobs: Option<mpsc::SyncSender<FirstSeenBlockWriteJob>>,
    recycled: mpsc::Receiver<Vec<u8>>,
    spare: Option<Vec<u8>>,
    worker: Option<thread::JoinHandle<Result<FirstSeenBlockWriteSummary>>>,
}

impl FirstSeenAsyncBlockWriter {
    fn start(
        blocks_file: File,
        blocks_path: PathBuf,
        level: i32,
        detailed_timings: bool,
    ) -> Result<Self> {
        // One queued job plus one buffer being processed bounds the stage to
        // two uncompressed block buffers irrespective of CAR size.
        let (jobs_tx, jobs_rx) = mpsc::sync_channel(1);
        // This channel cannot block the worker if the producer exits early.
        // It still holds at most the two buffers owned by this pipeline.
        let (recycled_tx, recycled_rx) = mpsc::sync_channel(2);
        let worker = thread::Builder::new()
            .name("first-seen-zstd".to_owned())
            .spawn(move || {
                run_first_seen_block_writer(
                    blocks_file,
                    &blocks_path,
                    level,
                    detailed_timings,
                    jobs_rx,
                    recycled_tx,
                )
            })
            .context("spawn first-seen zstd writer")?;
        Ok(Self {
            jobs: Some(jobs_tx),
            recycled: recycled_rx,
            spare: Some(Vec::new()),
            worker: Some(worker),
        })
    }

    fn submit(&mut self, job: FirstSeenBlockWriteJob) -> Result<Vec<u8>> {
        let Some(jobs) = self.jobs.as_ref() else {
            bail!("first-seen zstd writer is already closed")
        };
        if jobs.send(job).is_err() {
            return Err(self.worker_stopped_error("submit first-seen block"));
        }

        let mut next = if let Some(spare) = self.spare.take() {
            spare
        } else {
            match self.recycled.recv() {
                Ok(buffer) => buffer,
                Err(_) => {
                    return Err(self.worker_stopped_error("wait for first-seen encoded buffer"));
                }
            }
        };
        next.clear();
        Ok(next)
    }

    fn finish(mut self) -> Result<FirstSeenBlockWriteSummary> {
        self.jobs.take();
        self.join_worker()
    }

    fn join_worker(&mut self) -> Result<FirstSeenBlockWriteSummary> {
        let worker = self
            .worker
            .take()
            .context("first-seen zstd writer join handle is missing")?;
        worker
            .join()
            .map_err(|_| anyhow!("first-seen zstd writer thread panicked"))?
    }

    fn worker_stopped_error(&mut self, operation: &'static str) -> anyhow::Error {
        self.jobs.take();
        match self.join_worker() {
            Ok(_) => anyhow!("{operation}: first-seen zstd writer exited early"),
            Err(error) => error.context(operation),
        }
    }
}

impl Drop for FirstSeenAsyncBlockWriter {
    fn drop(&mut self) {
        // Ensure an error in CAR decode or a sidecar writer cannot leave a
        // detached compressor writing into an incomplete candidate archive.
        self.jobs.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn run_first_seen_block_writer(
    blocks_file: File,
    blocks_path: &Path,
    level: i32,
    detailed_timings: bool,
    jobs: mpsc::Receiver<FirstSeenBlockWriteJob>,
    recycled: mpsc::SyncSender<Vec<u8>>,
) -> Result<FirstSeenBlockWriteSummary> {
    let mut writer = BufWriter::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, blocks_file);
    let mut compressor =
        zstd::bulk::Compressor::new(level).context("create first-seen zstd compressor")?;
    let mut compressed = Vec::new();
    let mut summary = FirstSeenBlockWriteSummary::default();

    while let Ok(mut job) = jobs.recv() {
        let result = (|| -> Result<()> {
            let uncompressed_len = u32::try_from(job.block_bytes.len())
                .context("first-seen hot archive block exceeds u32::MAX")?;
            compressed.clear();
            let compress_bound = zstd::zstd_safe::compress_bound(job.block_bytes.len());
            if compressed.capacity() < compress_bound {
                compressed.reserve(compress_bound.saturating_sub(compressed.len()));
                summary.zstd_buffer_reserves += 1;
            }
            summary.zstd_buffer_capacity_max =
                summary.zstd_buffer_capacity_max.max(compressed.capacity());

            let compress_started = DetailTimer::start(detailed_timings);
            compressor
                .compress_to_buffer(&job.block_bytes, &mut compressed)
                .with_context(|| format!("zstd compress first-seen block_id {}", job.block_id))?;
            summary.zstd_compress += compress_started.elapsed();
            let compressed_len = u32::try_from(compressed.len())
                .context("compressed first-seen hot block exceeds u32::MAX")?;

            let write_started = DetailTimer::start(detailed_timings);
            writer
                .write_all(&compressed)
                .with_context(|| format!("write {}", blocks_path.display()))?;
            summary.block_write += write_started.elapsed();

            summary.rows.push(ArchiveV2HotBlockIndexRow {
                block_id: job.block_id,
                slot: job.slot,
                compressed_offset: summary.blob_offset,
                compressed_len,
                uncompressed_len,
                tx_count: job.tx_count,
                first_tx_ordinal: job.first_tx_ordinal,
                first_signature_ordinal: job.first_signature_ordinal,
                signature_count: job.signature_count,
            });
            summary.blob_offset += u64::from(compressed_len);
            summary.uncompressed_bytes += u64::from(uncompressed_len);
            summary.compressed_bytes += u64::from(compressed_len);
            Ok(())
        })();

        job.block_bytes.clear();
        let _ = recycled.send(job.block_bytes);
        result?;
    }

    writer
        .flush()
        .with_context(|| format!("flush {}", blocks_path.display()))?;
    Ok(summary)
}

/// Builds hot blocks in one current-epoch CAR pass by assigning final pubkey
/// IDs in deterministic first-reference order.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_hot_blocks_first_seen(
    input: &Path,
    output_dir: &Path,
    previous_car: Option<&Path>,
    external_blockhashes_path: Option<&Path>,
    level: i32,
    max_blocks: Option<u64>,
    resume: bool,
    include_access: bool,
    seed_registry_path: Option<&Path>,
    seed_key_limit: usize,
    registry_capacity: usize,
    decode_workers: usize,
    car_zstd_prefetch_mib: usize,
    scan_only: bool,
) -> Result<()> {
    anyhow::ensure!(
        level >= 0,
        "zstd compression level must be non-negative, got {level}"
    );
    anyhow::ensure!(
        registry_capacity > 0,
        "first-seen registry capacity must be non-zero"
    );
    anyhow::ensure!(
        (1..=FIRST_SEEN_DECODE_WORKERS_MAX).contains(&decode_workers),
        "first-seen decode workers must be in 1..={FIRST_SEEN_DECODE_WORKERS_MAX}, got {decode_workers}"
    );
    anyhow::ensure!(
        car_zstd_prefetch_mib <= FIRST_SEEN_ZSTD_PREFETCH_MIB_MAX,
        "CAR zstd prefetch must be in 0..={FIRST_SEEN_ZSTD_PREFETCH_MIB_MAX} MiB, got {car_zstd_prefetch_mib}"
    );
    anyhow::ensure!(
        car_zstd_prefetch_mib == 0 || input_path_is_car_zstd(input),
        "--car-zstd-prefetch-mib requires a local .car.zst input"
    );
    anyhow::ensure!(
        !max_blocks.is_some_and(|limit| limit == 0),
        "--max-blocks must be greater than zero for a first-seen build"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let external_blockhashes = load_external_blockhash_overrides(external_blockhashes_path)?;
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;
    anyhow::ensure!(
        genesis.is_none(),
        "experimental first-seen hot builder does not yet support epoch-0 genesis"
    );

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    let registry_index_path = output_dir.join(REGISTRY_INDEX_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let blocks_path = output_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let index_path = output_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let access_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let access_index_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let meta_path = output_dir.join(ARCHIVE_V2_META_FILE);
    let signatures_path = output_dir.join(ARCHIVE_V2_SIGNATURES_FILE);
    let poh_path = output_dir.join(POH_FILE);
    let shredding_path = output_dir.join(SHREDDING_FILE);
    let vote_hash_registry_path = output_dir.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);
    let first_seen_manifest_path = output_dir.join(FIRST_SEEN_REGISTRY_MANIFEST_FILE);
    let blockhash_index_v3_path = output_dir.join(BLOCKHASH_INDEX_V3_FILE);
    let block_time_gap_path = output_dir.join(BLOCK_TIME_GAP_FILE);
    let next_seed_path = output_dir.join(FIRST_SEEN_HOT_SEED_FILE);
    let previous_tail_path = output_dir.join(PREV_BLOCKHASH_TAIL_FILE);
    let scan_complete_path =
        output_dir.join(crate::first_seen_finalization::FIRST_SEEN_SCAN_COMPLETE_FILE);

    for path in [
        &registry_path,
        &registry_counts_path,
        &registry_index_path,
        &blockhash_registry_path,
        &blockhash_index_v3_path,
        &block_time_gap_path,
        &blocks_path,
        &index_path,
        &access_path,
        &access_index_path,
        &meta_path,
        &signatures_path,
        &poh_path,
        &shredding_path,
        &vote_hash_registry_path,
        &first_seen_manifest_path,
        &next_seed_path,
        &previous_tail_path,
        &scan_complete_path,
    ] {
        anyhow::ensure!(
            !path.exists(),
            "first-seen builds require a fresh candidate output directory; artifact already exists: {}",
            path.display()
        );
    }

    let previous_tail = load_or_build_previous_tail(output_dir, previous_car, resume)?;

    let registry_tmp = pre_hot_tmp_path(&registry_path);
    let registry_counts_tmp = pre_hot_tmp_path(&registry_counts_path);
    let blockhash_tmp = pre_hot_tmp_path(&blockhash_registry_path);
    let meta_tmp = pre_hot_tmp_path(&meta_path);
    let first_seen_manifest_tmp = pre_hot_tmp_path(&first_seen_manifest_path);
    let next_seed_tmp = pre_hot_tmp_path(&next_seed_path);
    for path in [
        &registry_tmp,
        &registry_counts_tmp,
        &blockhash_tmp,
        &meta_tmp,
        &first_seen_manifest_tmp,
        &next_seed_tmp,
    ] {
        if path.exists() {
            std::fs::remove_file(path)
                .with_context(|| format!("remove stale first-seen temp {}", path.display()))?;
        }
    }

    let mut registry = FirstSeenRegistry::new(registry_capacity, &registry_tmp)?;
    seed_first_seen_registry(&mut registry, seed_registry_path, seed_key_limit)?;
    let mut known_program_ids = KnownProgramIds::from_first_seen(&registry);
    info!(
        "Archive V2 first-seen registry initialized: capacity={} seeded_keys={} seed_hash={} seed_source={}",
        registry.table_capacity(),
        registry.seeded_keys,
        registry.seed_digest(),
        seed_registry_path
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "builtins-only".to_string())
    );

    let raw_key_index = KeyIndex::build(Vec::new());
    let blockhash_file = File::create(&blockhash_tmp)
        .with_context(|| format!("create {}", blockhash_tmp.display()))?;
    let mut blockhash_writer = BufWriter::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, blockhash_file);
    let blocks_file =
        File::create(&blocks_path).with_context(|| format!("create {}", blocks_path.display()))?;
    let meta_file =
        File::create(&meta_tmp).with_context(|| format!("create {}", meta_tmp.display()))?;
    let mut meta_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        ARCHIVE_PRE_HOT_IO_BUFFER,
        meta_file,
    ));
    let signatures_file = File::create(&signatures_path)
        .with_context(|| format!("create {}", signatures_path.display()))?;
    let mut signatures_writer =
        BufWriter::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, signatures_file);
    let mut access_writer = if include_access {
        let access_file = File::create(&access_path)
            .with_context(|| format!("create {}", access_path.display()))?;
        Some(BufWriter::with_capacity(
            ARCHIVE_PRE_HOT_IO_BUFFER,
            access_file,
        ))
    } else {
        None
    };
    let poh_file =
        File::create(&poh_path).with_context(|| format!("create {}", poh_path.display()))?;
    let mut poh_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        ARCHIVE_PRE_HOT_IO_BUFFER,
        poh_file,
    ));
    let shredding_file = File::create(&shredding_path)
        .with_context(|| format!("create {}", shredding_path.display()))?;
    let mut shredding_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        ARCHIVE_PRE_HOT_IO_BUFFER,
        shredding_file,
    ));

    write_hot_meta(
        &mut meta_writer,
        &ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: ARCHIVE_FLAGS_LEB128
                | ARCHIVE_FLAGS_FIRST_SEEN_REGISTRY
                | ARCHIVE_FLAGS_ALL_PUBKEY_REF_COUNTS,
        }),
    )?;

    let mut block_bytes = Vec::new();
    let mut compressed_buf = Vec::new();
    let mut access_bytes = Vec::new();
    let mut first_seen_signatures = FirstSeenBlockSignatures::default();
    let mut hot_block_buffers = HotBlockBuffers::default();
    let mut first_seen_access_pubkeys = Vec::<ArchiveV2BlockAccessPubkey>::new();
    let mut blockhashes = Vec::<[u8; 32]>::new();

    let mut blockhash_index_v3 = full_blockhash_index_v3_publisher(output_dir, max_blocks)?;
    let prefetch_bytes = car_zstd_prefetch_mib << 20;
    let mut scanner = if prefetch_bytes == 0 {
        RawCarScanner::open_with_buffer(input, BLOCKHASH_SCAN_BUFFER_SIZE)?
    } else {
        RawCarScanner::open_zstd_prefetch_with_buffer(
            input,
            BLOCKHASH_SCAN_BUFFER_SIZE,
            prefetch_bytes,
        )?
    };
    let zstd_prefetch_stats = scanner.zstd_prefetch_stats.clone();
    scanner.skip_header()?;
    let mut pending = PendingBlock::default();
    let mut footer = WincodeArchiveV2Footer::default();
    let mut timings = ArchiveV2Timings::from_env();
    let mut parallel_tx_decoder = if decode_workers > 1 {
        Some(FirstSeenTxDecodePool::new(decode_workers)?)
    } else {
        None
    };
    info!(
        "Archive V2 first-seen async ordered zstd writer enabled (two-buffer pipeline, include_access={include_access})"
    );
    let mut blocks_writer = None::<BufWriter<File>>;
    let mut compressor = None::<zstd::bulk::Compressor<'static>>;
    let mut async_blocks_writer = Some(FirstSeenAsyncBlockWriter::start(
        blocks_file,
        blocks_path.clone(),
        level,
        timings.detailed,
    )?);
    let mut progress = ProgressTracker::new("Archive V2 FirstSeen Hot Write");
    let mut block_id = 0u32;
    let mut rolling_blockhashes = RollingBlockhashIndex::new(ROLLING_BLOCKHASH_CAPACITY);
    rolling_blockhashes.seed_previous_tail(&previous_tail)?;
    let mut last_blockhash = None;

    let mut rows = Vec::new();
    let mut access_rows = Vec::new();
    let mut blob_offset = 0u64;
    let mut access_offset = 0u64;
    let mut uncompressed_bytes = 0u64;
    let mut compressed_bytes = 0u64;
    let mut access_file_bytes = 0u64;
    let mut first_tx_ordinal = 0u64;
    let mut first_signature_ordinal = 0u64;
    let mut raw_pubkey_refs = 0u64;
    let mut slot_to_block_id: GxHashMap<u64, u32> =
        GxHashMap::with_hasher(GxBuildHasher::default());
    let mut vote_hashes = VoteHashRegistryBuilder::default();
    let mut metadata_zstd = ZstdReusableDecoder::new();
    let started = Instant::now();

    while let Some(raw) = scanner.next_node_timed_with_data_buffers(
        &mut pending.raw_dataframe_payloads,
        Some(&mut timings),
    )? {
        footer.car_entries += 1;
        footer.car_payload_bytes += raw.payload_len as u64;
        footer.decoded_node_payload_bytes += raw.payload_len as u64;

        let classify_started = timings.detail_timer();
        match raw.node {
            RawNode::Transaction(tx) => {
                footer.transactions += 1;
                pending.transactions.push(PendingTx {
                    tx,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Entry(entry) => {
                footer.entries += 1;
                pending.entries.push(PendingEntry {
                    entry,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Rewards(rewards) => {
                footer.rewards += 1;
                anyhow::ensure!(
                    pending.rewards.is_none(),
                    "duplicate rewards node before block"
                );
                pending.rewards = Some(PendingRewards {
                    rewards,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::DataFrame(frame) => {
                footer.dataframes += 1;
                pending.insert_dataframe_recycling(frame)?;
            }
            RawNode::Block(block) => {
                footer.blocks += 1;
                rolling_blockhashes.prune_for_slot(block.slot)?;
                timings.classify += classify_started.elapsed();
                let previous_blockhash =
                    last_blockhash.or_else(|| previous_tail.last().map(|previous| previous.hash));
                let (mut record, tx_count, sidecar) = build_block_record(
                    &mut pending,
                    block,
                    &raw_key_index,
                    &rolling_blockhashes,
                    block_id,
                    &mut footer,
                    &mut timings,
                    &external_blockhashes,
                    previous_blockhash,
                    &mut metadata_zstd,
                    parallel_tx_decoder.as_mut(),
                    Some(&mut first_seen_signatures),
                )?;
                let slot = pending.last_slot;
                let block_time = record.header.compact.block_time;

                let intern_started = timings.detail_timer();
                first_seen_access_pubkeys.clear();
                let intern_stats = crate::pre_hot::intern_block_pubkeys(&mut record, |key| {
                    let id = registry.intern(key)?;
                    if include_access {
                        first_seen_access_pubkeys
                            .push(ArchiveV2BlockAccessPubkey { id, pubkey: *key });
                    }
                    Ok(id)
                })
                .with_context(|| format!("slot {slot} intern first-seen pubkeys"))?;
                anyhow::ensure!(
                    intern_stats.raw_remaining == 0
                        && intern_stats.rekeyed == intern_stats.raw_seen,
                    "slot {slot} first-seen traversal left {} of {} pubkeys raw",
                    intern_stats.raw_remaining,
                    intern_stats.raw_seen
                );
                raw_pubkey_refs = raw_pubkey_refs
                    .checked_add(intern_stats.raw_seen)
                    .context("first-seen traversal reference count overflow")?;
                if include_access {
                    normalize_first_seen_access_pubkeys(&mut first_seen_access_pubkeys)
                        .with_context(|| {
                            format!("slot {slot} validate first-seen access pubkeys")
                        })?;
                }
                timings.metadata_pubkey_compact += intern_started.elapsed();

                known_program_ids.refresh_from_first_seen(&registry);
                vote_hashes.ensure_block(block_id);
                let block_shredding = std::mem::take(&mut record.header.compact.shredding);
                let (hot_block, block_signature_count) = hot_block_from_first_seen_archive_block(
                    record,
                    &first_seen_signatures,
                    &known_program_ids,
                    &slot_to_block_id,
                    &mut vote_hashes,
                    Some(&mut signatures_writer as &mut dyn Write),
                    &mut hot_block_buffers,
                    &mut timings,
                )
                .with_context(|| format!("slot {slot} first-seen hot block encode"))?;
                first_seen_signatures.record_block_write();

                block_bytes.clear();
                let serialize_started = timings.detail_timer();
                wincode::config::serialize_into(
                    &mut block_bytes,
                    &hot_block,
                    wincode_leb128_config(),
                )?;
                timings.hot_block_serialize += serialize_started.elapsed();
                let uncompressed_len = u32::try_from(block_bytes.len())
                    .context("first-seen hot archive block exceeds u32::MAX")?;
                if let Some(async_writer) = async_blocks_writer.as_mut() {
                    block_bytes = async_writer.submit(FirstSeenBlockWriteJob {
                        block_bytes,
                        block_id,
                        slot,
                        tx_count,
                        first_tx_ordinal,
                        first_signature_ordinal,
                        signature_count: block_signature_count,
                    })?;
                } else {
                    let compress_bound = zstd::zstd_safe::compress_bound(block_bytes.len());
                    if compressed_buf.capacity() < compress_bound {
                        compressed_buf.reserve(compress_bound.saturating_sub(compressed_buf.len()));
                        timings.hot_zstd_buffer_reserves += 1;
                    }
                    timings.hot_zstd_buffer_capacity_max = timings
                        .hot_zstd_buffer_capacity_max
                        .max(compressed_buf.capacity());
                    let compress_started = timings.detail_timer();
                    compressor
                        .as_mut()
                        .context("missing synchronous first-seen zstd compressor")?
                        .compress_to_buffer(&block_bytes, &mut compressed_buf)
                        .with_context(|| format!("zstd compress first-seen block_id {block_id}"))?;
                    timings.hot_zstd_compress += compress_started.elapsed();
                    let compressed_len = u32::try_from(compressed_buf.len())
                        .context("compressed first-seen hot block exceeds u32::MAX")?;
                    let write_started = timings.detail_timer();
                    blocks_writer
                        .as_mut()
                        .context("missing synchronous first-seen blocks writer")?
                        .write_all(&compressed_buf)
                        .with_context(|| format!("write {}", blocks_path.display()))?;
                    timings.hot_block_write += write_started.elapsed();

                    rows.push(ArchiveV2HotBlockIndexRow {
                        block_id,
                        slot,
                        compressed_offset: blob_offset,
                        compressed_len,
                        uncompressed_len,
                        tx_count,
                        first_tx_ordinal,
                        first_signature_ordinal,
                        signature_count: block_signature_count,
                    });
                    blob_offset += u64::from(compressed_len);
                    uncompressed_bytes += u64::from(uncompressed_len);
                    compressed_bytes += u64::from(compressed_len);
                }

                let poh_started = timings.detail_timer();
                poh_writer.write(&WincodeArchiveV2PohRecord {
                    block_id,
                    slot,
                    entries: sidecar.poh_entries,
                })?;
                shredding_writer.write(&WincodeArchiveV2ShreddingRecord {
                    block_id,
                    slot,
                    shredding: block_shredding,
                })?;
                blockhash_writer
                    .write_all(&sidecar.blockhash)
                    .with_context(|| format!("write {}", blockhash_tmp.display()))?;
                if let Some(index) = blockhash_index_v3.as_mut() {
                    index.push(slot, &sidecar.blockhash, block_time)?;
                }
                blockhashes.push(sidecar.blockhash);
                timings.hot_poh_write += poh_started.elapsed();

                if let Some(access_writer) = access_writer.as_mut() {
                    access_bytes.clear();
                    let access_blob = build_archive_v2_block_access_blob_with_pubkey_resolver(
                        &hot_block,
                        |id| {
                            let index = first_seen_access_pubkeys
                                .binary_search_by_key(&id, |entry| entry.id)
                                .map_err(|_| {
                                    anyhow!(
                                        "pubkey registry id {id} is absent from this block's exact first-seen map"
                                    )
                                })?;
                            Ok(first_seen_access_pubkeys[index].pubkey)
                        },
                        &blockhashes,
                        &previous_tail,
                        &first_seen_signatures.bytes,
                        &vote_hashes.rows,
                    )
                    .with_context(|| format!("slot {slot} first-seen block access sidecar"))?;
                    wincode::config::serialize_into(
                        &mut access_bytes,
                        &access_blob,
                        wincode_leb128_config(),
                    )?;
                    let access_len =
                        checked_archive_v2_block_access_frame_len(access_bytes.len(), slot)?;
                    access_writer
                        .write_all(&access_bytes)
                        .with_context(|| format!("write {}", access_path.display()))?;
                    access_rows.push(ArchiveV2BlockAccessIndexRow {
                        block_id,
                        slot,
                        access_offset,
                        access_len,
                        tx_count,
                        signature_count: block_signature_count,
                    });
                    access_offset += u64::from(access_len);
                    access_file_bytes += u64::from(access_len);
                }
                hot_block_buffers.recycle(hot_block);

                first_tx_ordinal += u64::from(tx_count);
                first_signature_ordinal += u64::from(block_signature_count);

                let current_block_id =
                    i32::try_from(block_id).context("first-seen blockhash id exceeds i32::MAX")?;
                rolling_blockhashes.insert(sidecar.blockhash, current_block_id, slot)?;
                last_blockhash = Some(sidecar.blockhash);
                slot_to_block_id.insert(slot, block_id);
                block_id = block_id
                    .checked_add(1)
                    .context("first-seen hot block id overflow")?;
                progress.update_slot(slot);
                progress.update_input_bytes(footer.car_payload_bytes);
                progress.update(1, u64::from(tx_count));
                pending.clear_recycling_frame_data();
                trim_hot_memory(
                    block_id,
                    &mut block_bytes,
                    &mut compressed_buf,
                    &mut access_bytes,
                    None,
                );
                hot_block_buffers.trim();
                if max_blocks.is_some_and(|limit| u64::from(block_id) >= limit) {
                    break;
                }
                continue;
            }
            RawNode::Subset(_) => footer.subset_nodes_ignored += 1,
            RawNode::Epoch(_) => footer.epoch_nodes_ignored += 1,
        }
        timings.classify += classify_started.elapsed();
    }

    // Closing the scanner first joins the optional decompression worker and
    // releases both large prefetch buffers before peak-memory finalization.
    drop(scanner);
    if let Some(stats) = zstd_prefetch_stats {
        let stats = stats.snapshot();
        info!(
            "Archive V2 first-seen CAR zstd prefetch: chunk_mib={} worker_bytes={} worker_chunks={} consumer_bytes={} consumer_reads={} worker_ready_wait_s={:.3} worker_recycle_wait_s={:.3} consumer_ready_wait_s={:.3} consumer_recycle_wait_s={:.3}",
            car_zstd_prefetch_mib,
            stats.worker_bytes,
            stats.worker_chunks,
            stats.consumer_bytes,
            stats.consumer_reads,
            stats.worker_ready_wait.as_secs_f64(),
            stats.worker_recycle_wait.as_secs_f64(),
            stats.consumer_ready_wait.as_secs_f64(),
            stats.consumer_recycle_wait.as_secs_f64(),
        );
    }

    // Release decoder threads and their retained metadata/zstd scratch before
    // MPHF construction and other peak-memory finalization work.
    drop(parallel_tx_decoder);

    if let Some(async_writer) = async_blocks_writer.take() {
        let summary = async_writer.finish()?;
        rows = summary.rows;
        blob_offset = summary.blob_offset;
        uncompressed_bytes = summary.uncompressed_bytes;
        compressed_bytes = summary.compressed_bytes;
        timings.hot_zstd_compress += summary.zstd_compress;
        timings.hot_block_write += summary.block_write;
        timings.hot_zstd_buffer_reserves += summary.zstd_buffer_reserves;
        timings.hot_zstd_buffer_capacity_max = timings
            .hot_zstd_buffer_capacity_max
            .max(summary.zstd_buffer_capacity_max);
    }

    anyhow::ensure!(
        pending.transactions.is_empty()
            && pending.entries.is_empty()
            && pending.rewards.is_none()
            && pending.dataframes.is_empty(),
        "first-seen CAR scan ended with uncommitted trailing nodes"
    );
    anyhow::ensure!(
        footer.blocks == rows.len() as u64,
        "first-seen footer block count {} != written rows {}",
        footer.blocks,
        rows.len()
    );
    anyhow::ensure!(
        footer.transactions == first_tx_ordinal,
        "first-seen footer transaction count {} != written transactions {}",
        footer.transactions,
        first_tx_ordinal
    );
    anyhow::ensure!(
        registry.references == raw_pubkey_refs,
        "first-seen registry references {} != traversal references {}",
        registry.references,
        raw_pubkey_refs
    );

    if let Some(blocks_writer) = blocks_writer.as_mut() {
        blocks_writer.flush()?;
    }
    signatures_writer.flush()?;
    if let Some(access_writer) = access_writer.as_mut() {
        access_writer.flush()?;
    }
    poh_writer.flush()?;
    shredding_writer.flush()?;
    blockhash_writer
        .flush()
        .with_context(|| format!("flush {}", blockhash_tmp.display()))?;
    vote_hashes.write(&vote_hash_registry_path)?;
    write_archive_v2_hot_block_index(&index_path, blob_offset, level, 0, &rows)?;
    if include_access {
        write_archive_v2_block_access_index(&access_index_path, access_offset, 0, &access_rows)?;
    }
    write_hot_meta(
        &mut meta_writer,
        &ArchiveV2HotMetaRecord::Footer(footer.clone()),
    )?;
    meta_writer.flush()?;
    let completed_blocks = rows.len();
    let raw_dataframe_pool_stats = pending.raw_dataframe_payloads.stats();
    anyhow::ensure!(
        raw_dataframe_pool_stats.current_buffers == 0
            && raw_dataframe_pool_stats.current_capacity == 0,
        "first-seen RawDataFrame payload pool still has {} live buffers with capacity {} after the final block",
        raw_dataframe_pool_stats.current_buffers,
        raw_dataframe_pool_stats.current_capacity,
    );
    drop(blocks_writer);
    drop(signatures_writer);
    drop(access_writer);
    drop(poh_writer);
    drop(shredding_writer);
    drop(blockhash_writer);
    drop(meta_writer);
    drop(raw_key_index);
    drop(compressor);
    drop(block_bytes);
    drop(compressed_buf);
    drop(access_bytes);
    let first_seen_signature_arena_stats = first_seen_signatures.stats;
    anyhow::ensure!(
        first_seen_signature_arena_stats.blocks == completed_blocks as u64
            && first_seen_signature_arena_stats.transactions == first_tx_ordinal
            && first_seen_signature_arena_stats.signatures == first_signature_ordinal,
        "first-seen signature arena totals blocks={} txs={} signatures={} do not match archive totals blocks={} txs={} signatures={}",
        first_seen_signature_arena_stats.blocks,
        first_seen_signature_arena_stats.transactions,
        first_seen_signature_arena_stats.signatures,
        completed_blocks,
        first_tx_ordinal,
        first_signature_ordinal,
    );
    drop(first_seen_signatures);
    drop(hot_block_buffers);
    drop(first_seen_access_pubkeys);
    drop(blockhashes);
    drop(rolling_blockhashes);
    drop(slot_to_block_id);
    drop(vote_hashes);
    drop(rows);
    drop(access_rows);
    drop(pending);
    drop(footer);
    trim_process_heap();

    let registry_finalize_started = Instant::now();
    let FirstSeenRegistryParts {
        keys: registry_keys,
        counts,
        seed_hash,
        seeded_keys,
        references,
        table_capacity,
        observed_audit,
        count_heap_bytes,
        count_u16_chunks,
        count_u32_chunks,
    } = registry.into_parts()?;
    anyhow::ensure!(
        registry_keys == counts.len(),
        "first-seen registry has {} keys but {} counts",
        registry_keys,
        counts.len(),
    );
    let counted_references = counts.iter().try_fold(0u64, |sum, count| {
        sum.checked_add(u64::from(count))
            .context("first-seen counted reference sum overflow")
    })?;
    anyhow::ensure!(
        counted_references == references,
        "first-seen registry counts sum {} does not match {} observed references",
        counted_references,
        references,
    );
    let verified_audit = verify_first_seen_reference_audit(&registry_tmp, &counts, observed_audit)?;
    write_first_seen_registry_counts(&registry_counts_tmp, counts.iter())?;
    std::fs::rename(&registry_tmp, &registry_path).with_context(|| {
        format!(
            "rename {} to {}",
            registry_tmp.display(),
            registry_path.display()
        )
    })?;
    std::fs::rename(&registry_counts_tmp, &registry_counts_path).with_context(|| {
        format!(
            "rename {} to {}",
            registry_counts_tmp.display(),
            registry_counts_path.display()
        )
    })?;
    std::fs::rename(&blockhash_tmp, &blockhash_registry_path).with_context(|| {
        format!(
            "rename {} to {}",
            blockhash_tmp.display(),
            blockhash_registry_path.display()
        )
    })?;
    if let Some(index) = blockhash_index_v3 {
        index.publish(completed_blocks as u64)?;
    }
    let registry_write_elapsed = registry_finalize_started.elapsed();
    let next_seed_started = Instant::now();
    let (next_seed_keys, next_seed_hash) =
        write_first_seen_hot_seed(&next_seed_tmp, &registry_path, &counts, seed_key_limit)?;
    std::fs::rename(&next_seed_tmp, &next_seed_path).with_context(|| {
        format!(
            "rename {} to {}",
            next_seed_tmp.display(),
            next_seed_path.display()
        )
    })?;
    let next_seed_elapsed = next_seed_started.elapsed();
    write_first_seen_manifest(
        &first_seen_manifest_tmp,
        input,
        seed_registry_path,
        &seed_hash,
        seeded_keys,
        &next_seed_hash,
        next_seed_keys,
        registry_keys,
        references,
        verified_audit,
    )?;
    drop(counts);
    trim_process_heap();

    let mphf_elapsed = if scan_only {
        let marker = crate::first_seen_finalization::FirstSeenScanMarker::new(
            registry_keys,
            references,
            include_access,
        )?;
        crate::first_seen_finalization::write_scan_complete_marker(output_dir, marker)?;
        Duration::ZERO
    } else {
        let mphf_started = Instant::now();
        let registry_store = KeyStore::load(&registry_path)
            .with_context(|| format!("load {}", registry_path.display()))?;
        let registry_index = KeyIndex::build_from_slice(&registry_store.keys);
        write_registry_key_index_atomic(&registry_index, &registry_index_path)?;
        drop(registry_index);
        drop(registry_store);
        trim_process_heap();
        let mphf_elapsed = mphf_started.elapsed();
        crate::first_seen_finalization::sync_candidate_files(output_dir)?;
        std::fs::rename(&first_seen_manifest_tmp, &first_seen_manifest_path).with_context(
            || {
                format!(
                    "rename {} to {}",
                    first_seen_manifest_tmp.display(),
                    first_seen_manifest_path.display()
                )
            },
        )?;
        crate::first_seen_finalization::sync_directory(output_dir)?;
        std::fs::rename(&meta_tmp, &meta_path)
            .with_context(|| format!("rename {} to {}", meta_tmp.display(), meta_path.display()))?;
        crate::first_seen_finalization::sync_directory(output_dir)?;
        mphf_elapsed
    };

    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    let ratio = if uncompressed_bytes > 0 {
        compressed_bytes as f64 * 100.0 / uncompressed_bytes as f64
    } else {
        0.0
    };
    info!(
        "Archive V2 first-seen hot {} complete in {:.2}s: blocks={} txs={} signatures={} keys={} seeded_keys={} seed_hash={} refs={} table_capacity={} count_heap_bytes={} count_u16_chunks={} count_u32_chunks={} level={} decode_workers={} car_zstd_prefetch_mib={} include_access={} uncompressed_bytes={} compressed_bytes={} access_bytes={} ratio_pct={:.2} registry_write_s={:.3} next_seed_keys={} next_seed_hash={} next_seed_s={:.3} mphf_s={:.3} output={}",
        if scan_only { "scan" } else { "build" },
        elapsed,
        completed_blocks,
        first_tx_ordinal,
        first_signature_ordinal,
        registry_keys,
        seeded_keys,
        seed_hash,
        references,
        table_capacity,
        count_heap_bytes,
        count_u16_chunks,
        count_u32_chunks,
        level,
        decode_workers,
        car_zstd_prefetch_mib,
        include_access,
        uncompressed_bytes,
        compressed_bytes,
        access_file_bytes,
        ratio,
        registry_write_elapsed.as_secs_f64(),
        next_seed_keys,
        next_seed_hash,
        next_seed_elapsed.as_secs_f64(),
        mphf_elapsed.as_secs_f64(),
        output_dir.display()
    );
    info!(
        "Archive V2 first-seen timings: scan_decode={:.3}s classify={:.3}s dataframe_assemble={:.3}s tx_decode_compact={:.3}s metadata_decode_compact={:.3}s parallel_decode_wall={:.3}s rewards_decode_compact={:.3}s pubkey_intern={:.3}s hot_message_build={:.3}s hot_message_encode={:.3}s hot_metadata_encode={:.3}s hot_signature_write={:.3}s hot_block_serialize={:.3}s hot_zstd_compress={:.3}s hot_block_write={:.3}s hot_poh_write={:.3}s",
        timings.scan_decode_node.as_secs_f64(),
        timings.classify.as_secs_f64(),
        timings.dataframe_assemble.as_secs_f64(),
        timings.tx_decode_compact.as_secs_f64(),
        timings.metadata_decode_compact.as_secs_f64(),
        timings.first_seen_parallel_decode_wall.as_secs_f64(),
        timings.rewards_decode_compact.as_secs_f64(),
        timings.metadata_pubkey_compact.as_secs_f64(),
        timings.hot_message_build.as_secs_f64(),
        timings.hot_message_encode.as_secs_f64(),
        timings.hot_metadata_encode.as_secs_f64(),
        timings.hot_signature_write.as_secs_f64(),
        timings.hot_block_serialize.as_secs_f64(),
        timings.hot_zstd_compress.as_secs_f64(),
        timings.hot_block_write.as_secs_f64(),
        timings.hot_poh_write.as_secs_f64(),
    );
    info!(
        "Archive V2 first-seen RawDataFrame payload pool: retained_buffers={} retained_capacity={} current_buffers={} current_capacity={} peak_current_buffers={} peak_current_capacity={} takes={} reused_buffers={} fresh_buffers={} allocation_events={} growth_events={} discarded_buffers={} discarded_capacity={}",
        raw_dataframe_pool_stats.retained_buffers,
        raw_dataframe_pool_stats.retained_capacity,
        raw_dataframe_pool_stats.current_buffers,
        raw_dataframe_pool_stats.current_capacity,
        raw_dataframe_pool_stats.peak_current_buffers,
        raw_dataframe_pool_stats.peak_current_capacity,
        raw_dataframe_pool_stats.takes,
        raw_dataframe_pool_stats.reused_buffers,
        raw_dataframe_pool_stats.fresh_buffers,
        raw_dataframe_pool_stats.allocation_events,
        raw_dataframe_pool_stats.growth_events,
        raw_dataframe_pool_stats.discarded_buffers,
        raw_dataframe_pool_stats.discarded_capacity,
    );
    info!(
        "Archive V2 first-seen signature arena: blocks={} transactions={} signatures={} decoder_signature_ref_vec_allocations_avoided={} owned_signature_outer_vec_allocations_avoided={} owned_signature_inner_vec_allocations_avoided={} block_write_calls={} peak_counts_capacity={} peak_bytes_capacity={}",
        first_seen_signature_arena_stats.blocks,
        first_seen_signature_arena_stats.transactions,
        first_seen_signature_arena_stats.signatures,
        first_seen_signature_arena_stats.decoder_signature_ref_vec_allocations_avoided,
        first_seen_signature_arena_stats.owned_signature_outer_vec_allocations_avoided,
        first_seen_signature_arena_stats.owned_signature_inner_vec_allocations_avoided,
        first_seen_signature_arena_stats.block_write_calls,
        first_seen_signature_arena_stats.peak_counts_capacity,
        first_seen_signature_arena_stats.peak_bytes_capacity,
    );
    Ok(())
}

#[derive(Debug, Default)]
struct PreHotSpoolStats {
    records: u64,
    blocks: u64,
    txs: u64,
    raw_pubkey_refs: u64,
    uncompressed_bytes: u64,
    compressed_bytes: u64,
    max_uncompressed_record: usize,
    max_compressed_record: usize,
}

struct PreHotRecordWriter<W: Write> {
    writer: BufWriter<W>,
    compressor: zstd::bulk::Compressor<'static>,
    encoded: Vec<u8>,
    compressed: Vec<u8>,
    stats: PreHotSpoolStats,
}

impl<W: Write> PreHotRecordWriter<W> {
    fn new(inner: W) -> Result<Self> {
        let mut writer = BufWriter::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, inner);
        writer.write_all(ARCHIVE_PRE_HOT_MAGIC)?;
        writer.write_all(&LIVE_PRE_HOT_BLOCK_VERSION.to_le_bytes())?;
        Ok(Self {
            writer,
            compressor: zstd::bulk::Compressor::new(1).context("create PreHot zstd compressor")?,
            encoded: Vec::new(),
            compressed: Vec::new(),
            stats: PreHotSpoolStats::default(),
        })
    }

    fn write(&mut self, record: &LivePreHotRecord) -> Result<()> {
        self.encoded.clear();
        wincode::config::serialize_into(&mut self.encoded, record, wincode_leb128_config())?;
        anyhow::ensure!(
            self.encoded.len() <= ARCHIVE_PRE_HOT_MAX_RECORD_BYTES,
            "PreHot record is {} bytes, maximum is {}",
            self.encoded.len(),
            ARCHIVE_PRE_HOT_MAX_RECORD_BYTES
        );
        self.compressed.clear();
        let compress_bound = zstd::zstd_safe::compress_bound(self.encoded.len());
        if self.compressed.capacity() < compress_bound {
            self.compressed
                .reserve(compress_bound.saturating_sub(self.compressed.len()));
        }
        self.compressor
            .compress_to_buffer(&self.encoded, &mut self.compressed)
            .context("compress PreHot record")?;
        let uncompressed_len =
            u32::try_from(self.encoded.len()).context("PreHot record exceeds u32::MAX")?;
        let compressed_len = u32::try_from(self.compressed.len())
            .context("compressed PreHot record exceeds u32::MAX")?;
        write_u32_varint(&mut self.writer, uncompressed_len)?;
        write_u32_varint(&mut self.writer, compressed_len)?;
        self.writer.write_all(&self.compressed)?;
        self.stats.records = self.stats.records.saturating_add(1);
        self.stats.uncompressed_bytes = self
            .stats
            .uncompressed_bytes
            .saturating_add(u64::from(uncompressed_len));
        self.stats.compressed_bytes = self
            .stats
            .compressed_bytes
            .saturating_add(u64::from(compressed_len));
        self.stats.max_uncompressed_record =
            self.stats.max_uncompressed_record.max(self.encoded.len());
        self.stats.max_compressed_record =
            self.stats.max_compressed_record.max(self.compressed.len());
        Ok(())
    }

    fn finish(mut self) -> Result<PreHotSpoolStats> {
        self.writer.flush()?;
        Ok(self.stats)
    }
}

struct PreHotRecordReader<R: Read> {
    reader: BufReader<R>,
    decompressor: zstd::bulk::Decompressor<'static>,
    compressed: Vec<u8>,
    decoded: Vec<u8>,
}

impl<R: Read> PreHotRecordReader<R> {
    fn new(inner: R) -> Result<Self> {
        let mut reader = BufReader::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, inner);
        let mut magic = [0u8; ARCHIVE_PRE_HOT_MAGIC.len()];
        reader.read_exact(&mut magic)?;
        anyhow::ensure!(
            &magic == ARCHIVE_PRE_HOT_MAGIC,
            "invalid PreHot magic {:?}",
            magic
        );
        let mut version = [0u8; 2];
        reader.read_exact(&mut version)?;
        let version = u16::from_le_bytes(version);
        anyhow::ensure!(
            version == LIVE_PRE_HOT_BLOCK_VERSION,
            "unsupported PreHot file version {version}"
        );
        Ok(Self {
            reader,
            decompressor: zstd::bulk::Decompressor::new()
                .context("create PreHot zstd decompressor")?,
            compressed: Vec::new(),
            decoded: Vec::new(),
        })
    }

    fn read(&mut self) -> Result<Option<LivePreHotRecord>> {
        let Some(uncompressed_len) = read_pre_hot_u32_varint(&mut self.reader)? else {
            return Ok(None);
        };
        let compressed_len = read_pre_hot_u32_varint(&mut self.reader)?
            .context("truncated PreHot compressed-length prefix")?;
        let uncompressed_len = uncompressed_len as usize;
        let compressed_len = compressed_len as usize;
        anyhow::ensure!(
            uncompressed_len <= ARCHIVE_PRE_HOT_MAX_RECORD_BYTES,
            "PreHot record length {uncompressed_len} exceeds maximum {}",
            ARCHIVE_PRE_HOT_MAX_RECORD_BYTES
        );
        anyhow::ensure!(
            compressed_len <= ARCHIVE_PRE_HOT_MAX_RECORD_BYTES,
            "compressed PreHot record length {compressed_len} exceeds maximum {}",
            ARCHIVE_PRE_HOT_MAX_RECORD_BYTES
        );
        self.compressed.resize(compressed_len, 0);
        self.reader
            .read_exact(&mut self.compressed)
            .context("truncated PreHot compressed record")?;
        self.decoded.clear();
        if self.decoded.capacity() < uncompressed_len {
            self.decoded.reserve(uncompressed_len);
        }
        let decoded_len = self
            .decompressor
            .decompress_to_buffer(&self.compressed, &mut self.decoded)
            .context("decompress PreHot record")?;
        anyhow::ensure!(
            decoded_len == uncompressed_len && self.decoded.len() == uncompressed_len,
            "PreHot decoded length mismatch: decoded={} buffer={} expected={}",
            decoded_len,
            self.decoded.len(),
            uncompressed_len
        );
        let record = wincode::config::deserialize(&self.decoded, wincode_leb128_config())
            .context("decode PreHot record")?;
        Ok(Some(record))
    }

    fn trim(&mut self) {
        trim_hot_reusable_buffer(&mut self.compressed);
        trim_hot_reusable_buffer(&mut self.decoded);
    }
}

fn read_pre_hot_u32_varint(reader: &mut impl Read) -> Result<Option<u32>> {
    let mut value = 0u32;
    for index in 0..5u32 {
        let mut byte = [0u8; 1];
        match reader.read_exact(&mut byte) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof && index == 0 => return Ok(None),
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                bail!("truncated PreHot varint after {index} bytes")
            }
            Err(err) => return Err(err.into()),
        }
        let byte = byte[0];
        if index == 4 && byte > 0x0f {
            bail!("PreHot u32 varint overflow")
        }
        value |= u32::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(Some(value));
        }
    }
    bail!("PreHot u32 varint overflow")
}

pub(crate) fn build_hot_blocks_pre_hot(
    input: &Path,
    output_dir: &Path,
    previous_car: Option<&Path>,
    registry_dir: Option<&Path>,
    external_blockhashes_path: Option<&Path>,
    level: i32,
    max_blocks: Option<u64>,
    resume: bool,
    include_access: bool,
    keep_pre_hot: bool,
    pre_hot_dir: Option<&Path>,
    pre_hot_registry_capacity: usize,
    reuse_pre_hot: bool,
) -> Result<()> {
    anyhow::ensure!(
        registry_dir.is_none(),
        "--registry-dir is not supported with --pre-hot because the PreHot pass builds the registry while decoding CAR"
    );
    anyhow::ensure!(
        !(reuse_pre_hot && max_blocks.is_some()),
        "--reuse-pre-hot cannot be combined with --max-blocks because a reused spool must represent a complete epoch"
    );
    anyhow::ensure!(
        level >= 0,
        "zstd compression level must be non-negative, got {level}"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let pre_hot_dir = pre_hot_dir.unwrap_or(output_dir);
    std::fs::create_dir_all(pre_hot_dir)
        .with_context(|| format!("create PreHot dir {}", pre_hot_dir.display()))?;
    let started = Instant::now();
    let external_blockhashes = load_external_blockhash_overrides(external_blockhashes_path)?;
    let previous_tail = load_or_build_previous_tail(output_dir, previous_car, resume)?;
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;
    let pre_hot_path = pre_hot_dir.join(ARCHIVE_PRE_HOT_FILE);
    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let poh_path = output_dir.join(POH_FILE);

    let can_resume_spool = reuse_pre_hot
        && resume
        && crate::file_nonempty(&pre_hot_path)
        && crate::file_nonempty(&registry_path)
        && crate::file_nonempty(&registry_counts_path)
        && crate::file_nonempty(&blockhash_registry_path)
        && crate::file_nonempty(&poh_path)
        && crate::file_nonempty(&output_dir.join(BLOCKHASH_INDEX_V3_FILE));
    if reuse_pre_hot {
        anyhow::ensure!(
            can_resume_spool,
            "--reuse-pre-hot requires a non-empty spool, registry/counts, blockhash registry, PoH sidecar, and complete V3 timestamp index in {}",
            output_dir.display()
        );
        ensure_default_block_time_gaps(output_dir)?;
        info!("Reusing completed PreHot spool: {}", pre_hot_path.display());
    } else {
        build_pre_hot_spool(
            input,
            &pre_hot_path,
            &registry_path,
            &registry_counts_path,
            &blockhash_registry_path,
            &poh_path,
            &previous_tail,
            genesis.as_ref(),
            &external_blockhashes,
            max_blocks,
            pre_hot_registry_capacity,
        )?;
    }

    finalize_pre_hot_blocks(
        &pre_hot_path,
        output_dir,
        &previous_tail,
        genesis.as_ref(),
        level,
        include_access,
        can_resume_spool,
    )?;

    if !keep_pre_hot {
        std::fs::remove_file(&pre_hot_path)
            .with_context(|| format!("remove completed PreHot spool {}", pre_hot_path.display()))?;
    }
    info!(
        "Archive V2 PreHot one-shot complete in {:.2}s: input={} output={} spool_kept={}",
        started.elapsed().as_secs_f64(),
        input.display(),
        output_dir.display(),
        keep_pre_hot
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_pre_hot_spool(
    input: &Path,
    pre_hot_path: &Path,
    registry_path: &Path,
    registry_counts_path: &Path,
    blockhash_registry_path: &Path,
    poh_path: &Path,
    previous_tail: &[PreviousBlockhash],
    genesis: Option<&GenesisArchive>,
    external_blockhashes: &ExternalBlockhashOverrides,
    max_blocks: Option<u64>,
    registry_capacity: usize,
) -> Result<()> {
    let started = Instant::now();
    let output_dir = blockhash_registry_path
        .parent()
        .context("PreHot blockhash registry path has no output directory")?;
    let mut blockhash_index_v3 = full_blockhash_index_v3_publisher(output_dir, max_blocks)?;
    let pre_hot_tmp = pre_hot_tmp_path(pre_hot_path);
    let registry_tmp = pre_hot_tmp_path(registry_path);
    let registry_counts_tmp = pre_hot_tmp_path(registry_counts_path);
    let blockhash_tmp = pre_hot_tmp_path(blockhash_registry_path);
    let poh_tmp = pre_hot_tmp_path(poh_path);
    for path in [
        &pre_hot_tmp,
        &registry_tmp,
        &registry_counts_tmp,
        &blockhash_tmp,
        &poh_tmp,
    ] {
        if path.exists() {
            std::fs::remove_file(path)
                .with_context(|| format!("remove stale PreHot temp {}", path.display()))?;
        }
    }

    let file =
        File::create(&pre_hot_tmp).with_context(|| format!("create {}", pre_hot_tmp.display()))?;
    let mut writer = PreHotRecordWriter::new(file)?;
    writer.write(&LivePreHotRecord::Header(WincodeArchiveV2Header {
        version: LIVE_PRE_HOT_BLOCK_VERSION,
        flags: ARCHIVE_FLAGS_LEB128 | ARCHIVE_FLAGS_NO_REGISTRY,
    }))?;

    let blockhash_file = File::create(&blockhash_tmp)
        .with_context(|| format!("create {}", blockhash_tmp.display()))?;
    let mut blockhash_writer = BufWriter::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, blockhash_file);
    let poh_file =
        File::create(&poh_tmp).with_context(|| format!("create {}", poh_tmp.display()))?;
    let mut poh_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        ARCHIVE_PRE_HOT_IO_BUFFER,
        poh_file,
    ));

    let mut counter = ArchivePubkeyCounter::new(registry_capacity);
    let mut blockhash_id_offset = 0u32;
    let mut last_blockhash = None;
    if let Some(genesis) = genesis {
        blockhash_writer
            .write_all(&genesis.genesis_hash)
            .with_context(|| format!("write {}", blockhash_tmp.display()))?;
        blockhash_id_offset = 1;
        last_blockhash = Some(genesis.genesis_hash);
        genesis_epoch0::add_pubkeys_to(genesis, |key| counter.add32(key));
    }

    let raw_key_index = KeyIndex::build(Vec::new());
    let mut rolling_blockhashes = RollingBlockhashIndex::new(ROLLING_BLOCKHASH_CAPACITY);
    rolling_blockhashes.seed_previous_tail(previous_tail)?;
    if let Some(genesis) = genesis {
        rolling_blockhashes.insert(genesis.genesis_hash, 0, 0)?;
    }

    let mut scanner = RawCarScanner::open_with_buffer(input, BLOCKHASH_SCAN_BUFFER_SIZE)?;
    scanner.skip_header()?;
    let mut pending = PendingBlock::default();
    let mut footer = WincodeArchiveV2Footer::default();
    let mut timings = ArchiveV2Timings::from_env();
    let mut progress = ProgressTracker::new("Archive V2 PreHot Extract");
    let mut block_id = 0u32;
    let mut metadata_zstd = ZstdReusableDecoder::new();

    while let Some(raw) = scanner.next_node_timed(Some(&mut timings))? {
        footer.car_entries += 1;
        footer.car_payload_bytes += raw.payload_len as u64;
        footer.decoded_node_payload_bytes += raw.payload_len as u64;

        let classify_started = timings.detail_timer();
        match raw.node {
            RawNode::Transaction(tx) => {
                footer.transactions += 1;
                pending.transactions.push(PendingTx {
                    tx,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Entry(entry) => {
                footer.entries += 1;
                pending.entries.push(PendingEntry {
                    entry,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Rewards(rewards) => {
                footer.rewards += 1;
                anyhow::ensure!(
                    pending.rewards.is_none(),
                    "duplicate rewards node before block"
                );
                pending.rewards = Some(PendingRewards {
                    rewards,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::DataFrame(frame) => {
                footer.dataframes += 1;
                pending.dataframes.insert(frame.cid, frame);
            }
            RawNode::Block(block) => {
                footer.blocks += 1;
                timings.classify += classify_started.elapsed();
                let archive_block_id = block_id.saturating_add(blockhash_id_offset);
                let previous_blockhash = (archive_block_id > 0).then_some(last_blockhash).flatten();
                rolling_blockhashes.prune_for_slot(block.slot)?;
                let (record, tx_count, sidecar) = build_block_record(
                    &mut pending,
                    block,
                    &raw_key_index,
                    &rolling_blockhashes,
                    archive_block_id,
                    &mut footer,
                    &mut timings,
                    external_blockhashes,
                    previous_blockhash,
                    &mut metadata_zstd,
                    None,
                    None,
                )?;
                let slot = pending.last_slot;
                let block_time = record.header.compact.block_time;
                crate::pre_hot::count_registry_pubkeys(&record, |key| counter.add32(key))
                    .with_context(|| format!("slot {slot} count PreHot registry pubkeys"))?;
                let raw_pubkey_refs = crate::pre_hot::count_all_raw_pubkey_refs(&record)
                    .with_context(|| format!("slot {slot} count PreHot raw pubkeys"))?;
                let raw_pubkey_refs_u32 = u32::try_from(raw_pubkey_refs)
                    .context("PreHot block raw pubkey reference count exceeds u32::MAX")?;
                writer.write(&LivePreHotRecord::Block(LivePreHotBlock::new(
                    block_id,
                    record,
                    raw_pubkey_refs_u32,
                )))?;
                writer.stats.blocks = writer.stats.blocks.saturating_add(1);
                writer.stats.txs = writer.stats.txs.saturating_add(u64::from(tx_count));
                writer.stats.raw_pubkey_refs =
                    writer.stats.raw_pubkey_refs.saturating_add(raw_pubkey_refs);
                poh_writer.write(&WincodeArchiveV2PohRecord {
                    block_id,
                    slot,
                    entries: sidecar.poh_entries,
                })?;
                blockhash_writer
                    .write_all(&sidecar.blockhash)
                    .with_context(|| {
                        format!("write {} block_id {block_id}", blockhash_tmp.display())
                    })?;
                if let Some(index) = blockhash_index_v3.as_mut() {
                    index.push(slot, &sidecar.blockhash, block_time)?;
                }
                let current_block_id = i32::try_from(archive_block_id)
                    .context("PreHot blockhash id exceeds i32::MAX")?;
                rolling_blockhashes.insert(sidecar.blockhash, current_block_id, slot)?;
                last_blockhash = Some(sidecar.blockhash);
                block_id = block_id
                    .checked_add(1)
                    .context("PreHot block id overflow")?;
                progress.update_slot(slot);
                progress.update_input_bytes(footer.car_payload_bytes);
                progress.update(1, u64::from(tx_count));
                pending.clear();
                if max_blocks.is_some_and(|limit| u64::from(block_id) >= limit) {
                    break;
                }
                continue;
            }
            RawNode::Subset(_) => footer.subset_nodes_ignored += 1,
            RawNode::Epoch(_) => footer.epoch_nodes_ignored += 1,
        }
        timings.classify += classify_started.elapsed();
    }

    anyhow::ensure!(
        pending.transactions.is_empty()
            && pending.entries.is_empty()
            && pending.rewards.is_none()
            && pending.dataframes.is_empty(),
        "PreHot CAR scan ended with uncommitted trailing nodes"
    );
    writer.write(&LivePreHotRecord::Footer(footer.clone()))?;
    let spool_stats = writer.finish()?;
    poh_writer.flush()?;
    blockhash_writer
        .flush()
        .with_context(|| format!("flush {}", blockhash_tmp.display()))?;

    let registry_started = Instant::now();
    let registry_keys = write_pre_hot_registry(counter, &registry_tmp, &registry_counts_tmp)?;
    let registry_elapsed = registry_started.elapsed();

    std::fs::rename(&registry_tmp, registry_path).with_context(|| {
        format!(
            "rename {} to {}",
            registry_tmp.display(),
            registry_path.display()
        )
    })?;
    std::fs::rename(&registry_counts_tmp, registry_counts_path).with_context(|| {
        format!(
            "rename {} to {}",
            registry_counts_tmp.display(),
            registry_counts_path.display()
        )
    })?;
    std::fs::rename(&blockhash_tmp, blockhash_registry_path).with_context(|| {
        format!(
            "rename {} to {}",
            blockhash_tmp.display(),
            blockhash_registry_path.display()
        )
    })?;
    std::fs::rename(&poh_tmp, poh_path)
        .with_context(|| format!("rename {} to {}", poh_tmp.display(), poh_path.display()))?;
    std::fs::rename(&pre_hot_tmp, pre_hot_path).with_context(|| {
        format!(
            "rename {} to {}",
            pre_hot_tmp.display(),
            pre_hot_path.display()
        )
    })?;
    if let Some(index) = blockhash_index_v3 {
        index.publish(spool_stats.blocks)?;
    }
    progress.final_report();
    info!(
        "Archive V2 PreHot extract complete in {:.2}s: blocks={} txs={} keys={} raw_pubkey_refs={} spool_uncompressed={} spool_compressed={} ratio_pct={:.2} max_record_uncompressed={} max_record_compressed={} registry_sort_write={:.3}s path={}",
        started.elapsed().as_secs_f64(),
        spool_stats.blocks,
        spool_stats.txs,
        registry_keys,
        spool_stats.raw_pubkey_refs,
        spool_stats.uncompressed_bytes,
        spool_stats.compressed_bytes,
        if spool_stats.uncompressed_bytes == 0 {
            0.0
        } else {
            spool_stats.compressed_bytes as f64 * 100.0 / spool_stats.uncompressed_bytes as f64
        },
        spool_stats.max_uncompressed_record,
        spool_stats.max_compressed_record,
        registry_elapsed.as_secs_f64(),
        pre_hot_path.display()
    );
    info!(
        "Archive V2 PreHot extract timings: scan_decode={:.3}s classify={:.3}s dataframe_assemble={:.3}s tx_decode_compact={:.3}s metadata_decode_compact={:.3}s rewards_decode_compact={:.3}s",
        timings.scan_decode_node.as_secs_f64(),
        timings.classify.as_secs_f64(),
        timings.dataframe_assemble.as_secs_f64(),
        timings.tx_decode_compact.as_secs_f64(),
        timings.metadata_decode_compact.as_secs_f64(),
        timings.rewards_decode_compact.as_secs_f64(),
    );
    Ok(())
}

fn pre_hot_tmp_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("prehot");
    path.with_file_name(format!("{name}.prehot.tmp"))
}

fn write_pre_hot_registry(
    counter: ArchivePubkeyCounter,
    registry_path: &Path,
    registry_counts_path: &Path,
) -> Result<usize> {
    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });
    let missing_builtins = live_builtin_program_keys()
        .iter()
        .copied()
        .filter(|builtin| !items.iter().any(|(key, _)| key == builtin))
        .collect::<Vec<_>>();
    write_registry_iter(
        registry_path,
        missing_builtins
            .iter()
            .copied()
            .chain(items.iter().map(|(key, _)| *key)),
    )?;
    write_live_registry_counts(
        registry_counts_path,
        std::iter::repeat_n(0, missing_builtins.len()).chain(items.iter().map(|(_, count)| *count)),
    )?;
    Ok(missing_builtins.len() + items.len())
}

#[allow(clippy::too_many_arguments)]
fn finalize_pre_hot_blocks(
    pre_hot_path: &Path,
    output_dir: &Path,
    previous_tail: &[PreviousBlockhash],
    genesis: Option<&GenesisArchive>,
    level: i32,
    include_access: bool,
    resume_registry_index: bool,
) -> Result<()> {
    let started = Instant::now();
    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_index_path = output_dir.join(REGISTRY_INDEX_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let key_index = load_or_build_registry_key_index(
        &registry_path,
        &registry_index_path,
        resume_registry_index,
    )?;
    let known_program_ids = KnownProgramIds::from_index(&key_index);
    let access_store = if include_access {
        Some(KeyStore::load(&registry_path)?)
    } else {
        None
    };
    let blockhashes = load_blockhash_registry_plain(&blockhash_registry_path)?;
    let blockhash_id_offset = blockhash_id_offset_for_genesis(&genesis.cloned(), &blockhashes)?;

    let blocks_path = output_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let index_path = output_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let access_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let access_index_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let meta_path = output_dir.join(ARCHIVE_V2_META_FILE);
    let signatures_path = output_dir.join(ARCHIVE_V2_SIGNATURES_FILE);
    let shredding_path = output_dir.join(SHREDDING_FILE);
    let vote_hash_registry_path = output_dir.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);

    let mut reader = PreHotRecordReader::new(
        File::open(pre_hot_path).with_context(|| format!("open {}", pre_hot_path.display()))?,
    )?;
    let mut blocks_writer = BufWriter::with_capacity(
        ARCHIVE_PRE_HOT_IO_BUFFER,
        File::create(&blocks_path).with_context(|| format!("create {}", blocks_path.display()))?,
    );
    let mut meta_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        ARCHIVE_PRE_HOT_IO_BUFFER,
        File::create(&meta_path).with_context(|| format!("create {}", meta_path.display()))?,
    ));
    let mut signatures_writer = BufWriter::with_capacity(
        ARCHIVE_PRE_HOT_IO_BUFFER,
        File::create(&signatures_path)
            .with_context(|| format!("create {}", signatures_path.display()))?,
    );
    let mut shredding_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        ARCHIVE_PRE_HOT_IO_BUFFER,
        File::create(&shredding_path)
            .with_context(|| format!("create {}", shredding_path.display()))?,
    ));
    let mut access_writer = if include_access {
        Some(BufWriter::with_capacity(
            ARCHIVE_PRE_HOT_IO_BUFFER,
            File::create(&access_path)
                .with_context(|| format!("create {}", access_path.display()))?,
        ))
    } else {
        None
    };

    write_hot_meta(
        &mut meta_writer,
        &ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: ARCHIVE_FLAGS_LEB128,
        }),
    )?;
    if let Some(genesis) = genesis {
        write_hot_meta(
            &mut meta_writer,
            &ArchiveV2HotMetaRecord::Genesis(compact_genesis_record(genesis, &key_index)?),
        )?;
    }

    let mut compressor = zstd::bulk::Compressor::new(level).context("create zstd compressor")?;
    let mut block_bytes = Vec::new();
    let mut compressed_buf = Vec::new();
    let mut access_bytes = Vec::new();
    let mut access_signature_bytes = Vec::new();
    let mut hot_block_buffers = HotBlockBuffers::default();
    let mut timings = ArchiveV2Timings::from_env();
    let mut progress = ProgressTracker::new("Archive V2 PreHot Finalize");
    let mut rows = Vec::new();
    let mut access_rows = Vec::new();
    let mut blob_offset = 0u64;
    let mut access_offset = 0u64;
    let mut uncompressed_bytes = 0u64;
    let mut compressed_bytes = 0u64;
    let mut access_file_bytes = 0u64;
    let mut first_tx_ordinal = 0u64;
    let mut first_signature_ordinal = 0u64;
    let mut block_id = 0u32;
    let mut saw_header = false;
    let mut footer = None;
    let mut raw_seen = 0u64;
    let mut raw_rekeyed = 0u64;
    let mut raw_remaining = 0u64;
    let mut slot_to_block_id: GxHashMap<u64, u32> =
        GxHashMap::with_hasher(GxBuildHasher::default());
    let mut vote_hashes = VoteHashRegistryBuilder::default();

    while let Some(record) = reader.read()? {
        match record {
            LivePreHotRecord::Header(header) => {
                anyhow::ensure!(
                    !saw_header && rows.is_empty(),
                    "duplicate or late PreHot header"
                );
                anyhow::ensure!(
                    header.version == LIVE_PRE_HOT_BLOCK_VERSION,
                    "unsupported PreHot record version {}",
                    header.version
                );
                saw_header = true;
            }
            LivePreHotRecord::Block(mut pre_hot) => {
                anyhow::ensure!(saw_header, "PreHot block appeared before header");
                anyhow::ensure!(footer.is_none(), "PreHot block appeared after footer");
                anyhow::ensure!(
                    pre_hot.version == LIVE_PRE_HOT_BLOCK_VERSION,
                    "block_id {} has unsupported PreHot version {}",
                    pre_hot.block_id,
                    pre_hot.version
                );
                anyhow::ensure!(
                    pre_hot.block_id == block_id,
                    "PreHot block id {} is not contiguous; expected {}",
                    pre_hot.block_id,
                    block_id
                );
                let expected_archive_block_id = block_id.saturating_add(blockhash_id_offset);
                anyhow::ensure!(
                    pre_hot.block.header.compact.blockhash == expected_archive_block_id,
                    "PreHot block_id {} blockhash id {} != expected {}",
                    block_id,
                    pre_hot.block.header.compact.blockhash,
                    expected_archive_block_id
                );
                let slot = pre_hot.block.header.compact.slot;
                let block_shredding = std::mem::take(&mut pre_hot.block.header.compact.shredding);
                let rekey_started = timings.detail_timer();
                let rekey = crate::pre_hot::rekey_block_pubkeys(&mut pre_hot.block, &key_index)
                    .with_context(|| format!("slot {slot} rekey PreHot pubkeys"))?;
                timings.metadata_pubkey_compact += rekey_started.elapsed();
                anyhow::ensure!(
                    rekey.raw_seen == u64::from(pre_hot.raw_pubkey_refs),
                    "slot {} PreHot raw pubkey count changed: encoded={} visited={}",
                    slot,
                    pre_hot.raw_pubkey_refs,
                    rekey.raw_seen
                );
                raw_seen = raw_seen.saturating_add(rekey.raw_seen);
                raw_rekeyed = raw_rekeyed.saturating_add(rekey.rekeyed);
                raw_remaining = raw_remaining.saturating_add(rekey.raw_remaining);

                access_signature_bytes.clear();
                let block_signature_bytes = if include_access {
                    Some(&mut access_signature_bytes)
                } else {
                    None
                };
                vote_hashes.ensure_block(block_id);
                let (hot_block, block_signature_count) = hot_block_from_archive_block(
                    pre_hot.block,
                    &known_program_ids,
                    &slot_to_block_id,
                    &mut vote_hashes,
                    Some(&mut signatures_writer as &mut dyn Write),
                    block_signature_bytes,
                    &mut hot_block_buffers,
                    &mut timings,
                )
                .with_context(|| format!("slot {slot} PreHot hot-block encode"))?;
                let tx_count = hot_block.tx_count;

                block_bytes.clear();
                let serialize_started = timings.detail_timer();
                wincode::config::serialize_into(
                    &mut block_bytes,
                    &hot_block,
                    wincode_leb128_config(),
                )?;
                timings.hot_block_serialize += serialize_started.elapsed();
                let uncompressed_len = u32::try_from(block_bytes.len())
                    .context("PreHot finalized block exceeds u32::MAX")?;
                let compress_bound = zstd::zstd_safe::compress_bound(block_bytes.len());
                if compressed_buf.capacity() < compress_bound {
                    compressed_buf.reserve(compress_bound.saturating_sub(compressed_buf.len()));
                    timings.hot_zstd_buffer_reserves += 1;
                }
                timings.hot_zstd_buffer_capacity_max = timings
                    .hot_zstd_buffer_capacity_max
                    .max(compressed_buf.capacity());
                let compress_started = timings.detail_timer();
                compressor
                    .compress_to_buffer(&block_bytes, &mut compressed_buf)
                    .with_context(|| format!("compress finalized PreHot block_id {block_id}"))?;
                timings.hot_zstd_compress += compress_started.elapsed();
                let compressed_len = u32::try_from(compressed_buf.len())
                    .context("compressed finalized PreHot block exceeds u32::MAX")?;
                let write_started = timings.detail_timer();
                blocks_writer.write_all(&compressed_buf)?;
                timings.hot_block_write += write_started.elapsed();

                shredding_writer.write(&WincodeArchiveV2ShreddingRecord {
                    block_id,
                    slot,
                    shredding: block_shredding,
                })?;
                if let (Some(access_writer), Some(store)) =
                    (access_writer.as_mut(), access_store.as_ref())
                {
                    access_bytes.clear();
                    let access_blob = build_archive_v2_block_access_blob(
                        &hot_block,
                        &store.keys,
                        &blockhashes,
                        previous_tail,
                        &access_signature_bytes,
                        &vote_hashes.rows,
                    )?;
                    wincode::config::serialize_into(
                        &mut access_bytes,
                        &access_blob,
                        wincode_leb128_config(),
                    )?;
                    let access_len =
                        checked_archive_v2_block_access_frame_len(access_bytes.len(), slot)?;
                    access_writer.write_all(&access_bytes)?;
                    access_rows.push(ArchiveV2BlockAccessIndexRow {
                        block_id,
                        slot,
                        access_offset,
                        access_len,
                        tx_count,
                        signature_count: block_signature_count,
                    });
                    access_offset += u64::from(access_len);
                    access_file_bytes += u64::from(access_len);
                }
                hot_block_buffers.recycle(hot_block);

                rows.push(ArchiveV2HotBlockIndexRow {
                    block_id,
                    slot,
                    compressed_offset: blob_offset,
                    compressed_len,
                    uncompressed_len,
                    tx_count,
                    first_tx_ordinal,
                    first_signature_ordinal,
                    signature_count: block_signature_count,
                });
                blob_offset += u64::from(compressed_len);
                uncompressed_bytes += u64::from(uncompressed_len);
                compressed_bytes += u64::from(compressed_len);
                first_tx_ordinal += u64::from(tx_count);
                first_signature_ordinal += u64::from(block_signature_count);
                slot_to_block_id.insert(slot, block_id);
                block_id = block_id
                    .checked_add(1)
                    .context("PreHot block id overflow")?;
                progress.update_slot(slot);
                progress.update(1, u64::from(tx_count));
                trim_hot_memory(
                    block_id,
                    &mut block_bytes,
                    &mut compressed_buf,
                    &mut access_bytes,
                    include_access.then_some(&mut access_signature_bytes),
                );
                hot_block_buffers.trim();
                reader.trim();
            }
            LivePreHotRecord::Footer(value) => {
                anyhow::ensure!(saw_header, "PreHot footer appeared before header");
                anyhow::ensure!(footer.is_none(), "duplicate PreHot footer");
                footer = Some(value);
            }
        }
    }

    let footer = footer.context("PreHot stream is missing its footer")?;
    anyhow::ensure!(
        rows.len() + blockhash_id_offset as usize == blockhashes.len(),
        "PreHot block count {} plus genesis offset {} != blockhash registry count {}",
        rows.len(),
        blockhash_id_offset,
        blockhashes.len()
    );
    anyhow::ensure!(
        footer.blocks == rows.len() as u64,
        "PreHot footer block count {} != finalized rows {}",
        footer.blocks,
        rows.len()
    );
    anyhow::ensure!(
        footer.transactions == first_tx_ordinal,
        "PreHot footer transaction count {} != finalized transactions {}",
        footer.transactions,
        first_tx_ordinal
    );

    blocks_writer.flush()?;
    signatures_writer.flush()?;
    shredding_writer.flush()?;
    if let Some(writer) = access_writer.as_mut() {
        writer.flush()?;
    }
    vote_hashes.write(&vote_hash_registry_path)?;
    write_archive_v2_hot_block_index(&index_path, blob_offset, level, 0, &rows)?;
    if include_access {
        write_archive_v2_block_access_index(&access_index_path, access_offset, 0, &access_rows)?;
    }
    write_hot_meta(&mut meta_writer, &ArchiveV2HotMetaRecord::Footer(footer))?;
    meta_writer.flush()?;
    progress.final_report();
    info!(
        "Archive V2 PreHot finalize complete in {:.2}s: blocks={} txs={} signatures={} uncompressed_bytes={} compressed_bytes={} access_bytes={} raw_seen={} rekeyed={} raw_remaining={} output={}",
        started.elapsed().as_secs_f64(),
        rows.len(),
        first_tx_ordinal,
        first_signature_ordinal,
        uncompressed_bytes,
        compressed_bytes,
        access_file_bytes,
        raw_seen,
        raw_rekeyed,
        raw_remaining,
        output_dir.display()
    );
    info!(
        "Archive V2 PreHot finalize timings: rekey={:.3}s hot_message_build={:.3}s hot_message_encode={:.3}s hot_metadata_encode={:.3}s hot_signature_write={:.3}s hot_block_serialize={:.3}s hot_zstd_compress={:.3}s hot_block_write={:.3}s",
        timings.metadata_pubkey_compact.as_secs_f64(),
        timings.hot_message_build.as_secs_f64(),
        timings.hot_message_encode.as_secs_f64(),
        timings.hot_metadata_encode.as_secs_f64(),
        timings.hot_signature_write.as_secs_f64(),
        timings.hot_block_serialize.as_secs_f64(),
        timings.hot_zstd_compress.as_secs_f64(),
        timings.hot_block_write.as_secs_f64(),
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LiveRegistryRunSnapshot {
    files: usize,
    bytes: u64,
    fingerprint: [u8; 32],
}

fn snapshot_live_pubkey_runs(run_dir: &Path) -> Result<LiveRegistryRunSnapshot> {
    let runs = collect_pubkey_run_paths(run_dir)?;
    anyhow::ensure!(
        !runs.is_empty(),
        "live pubkey run sidecar missing or empty: {}",
        run_dir.display()
    );

    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    for path in &runs {
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("stat pubkey run {}", path.display()))?;
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .with_context(|| format!("pubkey run name is not UTF-8: {}", path.display()))?;
        let modified_nanos = metadata
            .modified()
            .with_context(|| format!("read pubkey run mtime {}", path.display()))?
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(metadata.len().to_le_bytes());
        hasher.update(modified_nanos.to_le_bytes());
        bytes = bytes.saturating_add(metadata.len());
    }

    Ok(LiveRegistryRunSnapshot {
        files: runs.len(),
        bytes,
        fingerprint: hasher.finalize().into(),
    })
}

fn live_registry_marker_field<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    text.lines().find_map(|line| {
        line.strip_prefix(name)
            .and_then(|value| value.strip_prefix('='))
    })
}

fn validate_prepared_live_registry(
    output_dir: &Path,
    marker_path: &Path,
    source: LiveRegistryRunSnapshot,
) -> Result<(u64, u64)> {
    let marker = std::fs::read_to_string(marker_path)
        .with_context(|| format!("read live registry marker {}", marker_path.display()))?;
    anyhow::ensure!(
        live_registry_marker_field(&marker, "version") == Some("1"),
        "unsupported live registry marker version in {}",
        marker_path.display()
    );
    let marker_files = live_registry_marker_field(&marker, "source_run_files")
        .context("live registry marker is missing source_run_files")?
        .parse::<usize>()
        .context("invalid live registry marker source_run_files")?;
    let marker_source_bytes = live_registry_marker_field(&marker, "source_run_bytes")
        .context("live registry marker is missing source_run_bytes")?
        .parse::<u64>()
        .context("invalid live registry marker source_run_bytes")?;
    let marker_fingerprint = live_registry_marker_field(&marker, "source_fingerprint")
        .context("live registry marker is missing source_fingerprint")?;
    anyhow::ensure!(
        marker_files == source.files
            && marker_source_bytes == source.bytes
            && marker_fingerprint == hex32(&source.fingerprint),
        "live pubkey runs changed after registry preparation: marker={} current_files={} current_bytes={} current_fingerprint={}",
        marker_path.display(),
        source.files,
        source.bytes,
        hex32(&source.fingerprint),
    );

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    let registry_bytes = std::fs::metadata(&registry_path)
        .with_context(|| format!("stat prepared registry {}", registry_path.display()))?
        .len();
    let registry_counts_bytes = std::fs::metadata(&registry_counts_path)
        .with_context(|| {
            format!(
                "stat prepared registry counts {}",
                registry_counts_path.display()
            )
        })?
        .len();
    anyhow::ensure!(
        registry_bytes > 0 && registry_bytes % 32 == 0,
        "prepared live registry has invalid byte length {}: {}",
        registry_bytes,
        registry_path.display()
    );
    anyhow::ensure!(
        registry_counts_bytes > 0,
        "prepared live registry counts is empty: {}",
        registry_counts_path.display()
    );
    let marker_registry_bytes = live_registry_marker_field(&marker, "registry_bytes")
        .context("live registry marker is missing registry_bytes")?
        .parse::<u64>()
        .context("invalid live registry marker registry_bytes")?;
    let marker_registry_counts_bytes = live_registry_marker_field(&marker, "registry_counts_bytes")
        .context("live registry marker is missing registry_counts_bytes")?
        .parse::<u64>()
        .context("invalid live registry marker registry_counts_bytes")?;
    anyhow::ensure!(
        registry_bytes == marker_registry_bytes
            && registry_counts_bytes == marker_registry_counts_bytes,
        "prepared live registry files do not match marker {}",
        marker_path.display()
    );
    Ok((registry_bytes, registry_counts_bytes))
}

fn write_live_registry_prepared_marker(
    output_dir: &Path,
    marker_path: &Path,
    source: LiveRegistryRunSnapshot,
    registry_bytes: u64,
    registry_counts_bytes: u64,
) -> Result<()> {
    let marker_tmp = output_dir.join(format!("{LIVE_REGISTRY_PREPARED_MARKER}.tmp"));
    if marker_tmp.exists() {
        std::fs::remove_file(&marker_tmp)
            .with_context(|| format!("remove stale marker temp {}", marker_tmp.display()))?;
    }
    let mut file = File::create(&marker_tmp)
        .with_context(|| format!("create live registry marker {}", marker_tmp.display()))?;
    writeln!(file, "version=1")?;
    writeln!(file, "source_run_files={}", source.files)?;
    writeln!(file, "source_run_bytes={}", source.bytes)?;
    writeln!(file, "source_fingerprint={}", hex32(&source.fingerprint))?;
    writeln!(file, "registry_bytes={registry_bytes}")?;
    writeln!(file, "registry_counts_bytes={registry_counts_bytes}")?;
    file.sync_all()
        .with_context(|| format!("sync live registry marker {}", marker_tmp.display()))?;
    drop(file);
    std::fs::rename(&marker_tmp, marker_path).with_context(|| {
        format!(
            "publish live registry marker {} -> {}",
            marker_tmp.display(),
            marker_path.display()
        )
    })?;
    crate::first_seen_finalization::sync_directory(output_dir)?;
    Ok(())
}

pub(crate) fn prepare_live_registry_from_runs(
    capture_dir: &Path,
    output_dir: &Path,
    resume: bool,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let run_dir = capture_dir.join("index").join(LIVE_PUBKEY_RUNS_DIR);
    let source = snapshot_live_pubkey_runs(&run_dir)?;
    let marker_path = output_dir.join(LIVE_REGISTRY_PREPARED_MARKER);

    if resume && marker_path.is_file() {
        let (registry_bytes, registry_counts_bytes) =
            validate_prepared_live_registry(output_dir, &marker_path, source)?;
        info!(
            "Reusing prepared live registry: runs={} run_bytes={} registry_bytes={} counts_bytes={} output={}",
            source.files,
            source.bytes,
            registry_bytes,
            registry_counts_bytes,
            output_dir.display()
        );
        return Ok(());
    }

    if marker_path.exists() {
        std::fs::remove_file(&marker_path).with_context(|| {
            format!(
                "remove stale live registry marker {}",
                marker_path.display()
            )
        })?;
        crate::first_seen_finalization::sync_directory(output_dir)?;
    }
    let marker_tmp = output_dir.join(format!("{LIVE_REGISTRY_PREPARED_MARKER}.tmp"));
    if marker_tmp.exists() {
        std::fs::remove_file(&marker_tmp)
            .with_context(|| format!("remove stale marker temp {}", marker_tmp.display()))?;
    }

    let prepare_dir = output_dir.join(LIVE_REGISTRY_PREPARE_TEMP_DIR);
    if prepare_dir.exists() {
        std::fs::remove_dir_all(&prepare_dir)
            .with_context(|| format!("remove stale prepare dir {}", prepare_dir.display()))?;
    }
    std::fs::create_dir_all(&prepare_dir)
        .with_context(|| format!("create prepare dir {}", prepare_dir.display()))?;
    let registry_candidate = prepare_dir.join(REGISTRY_FILE);
    let counts_candidate = prepare_dir.join(REGISTRY_COUNTS_FILE);
    build_live_pubkey_registry_from_runs(
        &run_dir,
        &registry_candidate,
        &counts_candidate,
        &prepare_dir.join("sort"),
    )?;

    let registry_bytes = std::fs::metadata(&registry_candidate)
        .with_context(|| format!("stat registry candidate {}", registry_candidate.display()))?
        .len();
    let registry_counts_bytes = std::fs::metadata(&counts_candidate)
        .with_context(|| format!("stat counts candidate {}", counts_candidate.display()))?
        .len();
    anyhow::ensure!(
        registry_bytes > 0 && registry_bytes % 32 == 0,
        "live registry candidate has invalid byte length {registry_bytes}"
    );
    anyhow::ensure!(
        registry_counts_bytes > 0,
        "live registry counts candidate is empty"
    );
    for path in [&registry_candidate, &counts_candidate] {
        File::open(path)
            .with_context(|| format!("open candidate for sync {}", path.display()))?
            .sync_all()
            .with_context(|| format!("sync candidate {}", path.display()))?;
    }

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    std::fs::rename(&registry_candidate, &registry_path).with_context(|| {
        format!(
            "publish live registry {} -> {}",
            registry_candidate.display(),
            registry_path.display()
        )
    })?;
    std::fs::rename(&counts_candidate, &registry_counts_path).with_context(|| {
        format!(
            "publish live registry counts {} -> {}",
            counts_candidate.display(),
            registry_counts_path.display()
        )
    })?;
    std::fs::remove_dir(&prepare_dir)
        .with_context(|| format!("remove empty prepare dir {}", prepare_dir.display()))?;
    crate::first_seen_finalization::sync_directory(output_dir)?;
    write_live_registry_prepared_marker(
        output_dir,
        &marker_path,
        source,
        registry_bytes,
        registry_counts_bytes,
    )?;
    info!(
        "Prepared live registry stage complete: runs={} run_bytes={} registry_bytes={} counts_bytes={} marker={}",
        source.files,
        source.bytes,
        registry_bytes,
        registry_counts_bytes,
        marker_path.display()
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiveFinalizationSidecars {
    Canonical,
    DegradedRepair,
}

struct LiveHotBuildReport {
    blocks: u64,
    transactions: u64,
    signatures: u64,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
}

pub(crate) fn build_hot_blocks_from_live_capture(
    capture_dir: &Path,
    output_dir: &Path,
    level: i32,
    max_blocks: Option<u64>,
    resume: bool,
    registry_source: LiveRegistrySource,
) -> Result<()> {
    build_hot_blocks_from_live_capture_inner(
        capture_dir,
        output_dir,
        level,
        max_blocks,
        resume,
        registry_source,
        LiveFinalizationSidecars::Canonical,
    )
    .map(|_| ())
}

pub(crate) fn build_degraded_hot_blocks_from_repair(
    materialized_dir: &Path,
    output_dir: &Path,
    level: i32,
    max_blocks: Option<u64>,
) -> Result<()> {
    let receipt = repair::validate_materialized_for_hot(materialized_dir)?;
    let Some(stage) = repair::begin_degraded_hot_stage(materialized_dir, output_dir, &receipt)?
    else {
        info!(
            "Degraded repair hot archive already complete: {}",
            output_dir.display()
        );
        return Ok(());
    };
    let report = build_hot_blocks_from_live_capture_inner(
        materialized_dir,
        &stage.path,
        level,
        max_blocks,
        false,
        LiveRegistrySource::Runs,
        LiveFinalizationSidecars::DegradedRepair,
    )?;
    if max_blocks.is_some() {
        info!(
            "Degraded repair hot smoke build left hidden and unpublished: blocks={} stage={}",
            report.blocks,
            stage.path.display()
        );
        return Ok(());
    }
    anyhow::ensure!(
        report.blocks == receipt.produced_blocks,
        "degraded hot block count {} != materialized {}",
        report.blocks,
        receipt.produced_blocks
    );
    repair::publish_degraded_hot_stage(
        materialized_dir,
        output_dir,
        stage,
        &receipt,
        repair::DegradedHotStats {
            blocks: report.blocks,
            transactions: report.transactions,
            signatures: report.signatures,
            compressed_bytes: report.compressed_bytes,
            uncompressed_bytes: report.uncompressed_bytes,
            zstd_level: level,
        },
    )
}

fn build_hot_blocks_from_live_capture_inner(
    capture_dir: &Path,
    output_dir: &Path,
    level: i32,
    max_blocks: Option<u64>,
    resume: bool,
    registry_source: LiveRegistrySource,
    sidecars: LiveFinalizationSidecars,
) -> Result<LiveHotBuildReport> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let live_blocks_path = capture_dir
        .join("blocks")
        .join("live-no-registry-blocks.bin");
    let live_pubkey_counts_path = capture_dir.join("index").join("pubkey-counts.bin");
    let live_pubkey_touches_path = capture_dir.join("index").join("pubkey-touches.bin");
    let live_pubkey_runs_dir = capture_dir.join("index").join(LIVE_PUBKEY_RUNS_DIR);
    let live_blockhash_registry_path = capture_dir.join("index").join(BLOCKHASH_REGISTRY_FILE);
    let live_signatures_path = capture_dir.join("index").join(ARCHIVE_V2_SIGNATURES_FILE);
    let live_poh_path = capture_dir.join("poh").join(POH_FILE);
    anyhow::ensure!(
        crate::file_nonempty(&live_blocks_path),
        "live block file missing or empty: {}",
        live_blocks_path.display()
    );
    anyhow::ensure!(
        crate::file_nonempty(&live_blockhash_registry_path),
        "live blockhash registry missing or empty: {}",
        live_blockhash_registry_path.display()
    );
    let live_blockhash_bytes = std::fs::metadata(&live_blockhash_registry_path)
        .with_context(|| format!("stat {}", live_blockhash_registry_path.display()))?
        .len();
    anyhow::ensure!(
        live_blockhash_bytes % 32 == 0,
        "live blockhash registry has invalid byte length {}: {}",
        live_blockhash_bytes,
        live_blockhash_registry_path.display()
    );
    let live_block_count = live_blockhash_bytes / 32;
    anyhow::ensure!(
        max_blocks.is_some() || live_block_count <= crate::SLOTS_PER_EPOCH,
        "live capture contains {} blockhash rows, exceeding one epoch's {} slots",
        live_block_count,
        crate::SLOTS_PER_EPOCH
    );
    if sidecars == LiveFinalizationSidecars::Canonical {
        anyhow::ensure!(
            crate::file_nonempty(&live_poh_path),
            "live PoH sidecar missing or empty: {}",
            live_poh_path.display()
        );
    }

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    if resume
        && !matches!(registry_source, LiveRegistrySource::Runs)
        && crate::file_nonempty(&registry_path)
        && crate::file_nonempty(&registry_counts_path)
    {
        info!(
            "Reusing existing live pubkey registry: {}",
            registry_path.display()
        );
    } else {
        let registry_source = if max_blocks.is_some()
            && matches!(
                registry_source,
                LiveRegistrySource::Auto | LiveRegistrySource::Counts | LiveRegistrySource::Touches
            ) {
            warn!(
                "Bounded live finalization ignores unbounded whole-capture registry sidecars and scans only the selected block prefix; pass --registry-source runs with a bounded run directory to use the low-memory merge path"
            );
            LiveRegistrySource::Scan
        } else {
            registry_source
        };
        match registry_source {
            LiveRegistrySource::Scan => build_live_no_registry_pubkey_registry(
                &live_blocks_path,
                &registry_path,
                &registry_counts_path,
                max_blocks,
            )?,
            LiveRegistrySource::Counts => build_live_pubkey_registry_from_counts(
                &live_pubkey_counts_path,
                &registry_path,
                &registry_counts_path,
            )?,
            LiveRegistrySource::Runs => {
                prepare_live_registry_from_runs(capture_dir, output_dir, resume)?
            }
            LiveRegistrySource::Touches => build_live_pubkey_registry_from_touches(
                &live_pubkey_touches_path,
                &registry_path,
                &registry_counts_path,
                &output_dir.join(".pubkey-touch-sort"),
            )?,
            LiveRegistrySource::Auto => match preferred_auto_live_registry_source(
                &live_pubkey_counts_path,
                &live_pubkey_runs_dir,
                &live_pubkey_touches_path,
            )? {
                LiveRegistrySource::Runs => {
                    prepare_live_registry_from_runs(capture_dir, output_dir, resume)?;
                }
                LiveRegistrySource::Counts => {
                    build_live_pubkey_registry_from_counts(
                        &live_pubkey_counts_path,
                        &registry_path,
                        &registry_counts_path,
                    )?;
                }
                LiveRegistrySource::Touches => {
                    build_live_pubkey_registry_from_touches(
                        &live_pubkey_touches_path,
                        &registry_path,
                        &registry_counts_path,
                        &output_dir.join(".pubkey-touch-sort"),
                    )?;
                }
                LiveRegistrySource::Scan => {
                    warn!(
                        "Live pubkey-count/run/touch sidecars missing or empty; falling back to block scan: counts={} runs={} touches={}",
                        live_pubkey_counts_path.display(),
                        live_pubkey_runs_dir.display(),
                        live_pubkey_touches_path.display()
                    );
                    build_live_no_registry_pubkey_registry(
                        &live_blocks_path,
                        &registry_path,
                        &registry_counts_path,
                        max_blocks,
                    )?;
                }
                LiveRegistrySource::Auto => unreachable!("auto source selection returned auto"),
            },
        }
    }

    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    if !(resume && crate::file_nonempty(&blockhash_registry_path)) {
        if let Some(limit) = max_blocks {
            copy_file_prefix(
                &live_blockhash_registry_path,
                &blockhash_registry_path,
                limit
                    .checked_mul(32)
                    .context("live blockhash prefix length overflow")?,
            )?;
        } else {
            link_or_copy_registry_sidecar(&live_blockhash_registry_path, &blockhash_registry_path)?;
        }
    }
    let poh_path = output_dir.join(POH_FILE);
    if sidecars == LiveFinalizationSidecars::Canonical {
        if !(resume && crate::file_nonempty(&poh_path)) {
            if let Some(limit) = max_blocks {
                copy_live_poh_prefix(&live_poh_path, &poh_path, limit)?;
            } else {
                link_or_copy_registry_sidecar(&live_poh_path, &poh_path)?;
            }
        }
    } else {
        anyhow::ensure!(
            !poh_path.exists(),
            "degraded repair finalization refuses canonical PoH output {}",
            poh_path.display()
        );
    }

    let registry_index_path = output_dir.join(REGISTRY_INDEX_FILE);
    let key_index = load_or_build_registry_key_index(&registry_path, &registry_index_path, resume)?;
    let known_program_ids = KnownProgramIds::from_index(&key_index);
    let blockhashes = load_blockhash_registry_plain(&blockhash_registry_path)?;

    let blocks_path = output_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let index_path = output_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let meta_path = output_dir.join(ARCHIVE_V2_META_FILE);
    let meta_tmp = crate::first_seen_finalization::first_seen_temp_path(&meta_path);
    let signatures_path = output_dir.join(ARCHIVE_V2_SIGNATURES_FILE);
    let shredding_path = output_dir.join(SHREDDING_FILE);
    let vote_hash_registry_path = output_dir.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);

    let input_file = File::open(&live_blocks_path)
        .with_context(|| format!("open {}", live_blocks_path.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(
        LIVE_FINALIZER_IO_BUFFER_SIZE,
        input_file,
    ));
    let blocks_file =
        File::create(&blocks_path).with_context(|| format!("create {}", blocks_path.display()))?;
    let mut blocks_writer = BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, blocks_file);
    if meta_tmp.exists() {
        std::fs::remove_file(&meta_tmp)
            .with_context(|| format!("remove stale live metadata temp {}", meta_tmp.display()))?;
    }
    let meta_file =
        File::create(&meta_tmp).with_context(|| format!("create {}", meta_tmp.display()))?;
    let mut meta_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        LIVE_FINALIZER_IO_BUFFER_SIZE,
        meta_file,
    ));
    let mut reused_live_signatures = false;
    let mut signatures_writer =
        if max_blocks.is_none() && crate::file_nonempty(&live_signatures_path) {
            link_or_copy_registry_sidecar(&live_signatures_path, &signatures_path)?;
            reused_live_signatures = true;
            None
        } else {
            let signatures_file = File::create(&signatures_path)
                .with_context(|| format!("create {}", signatures_path.display()))?;
            Some(BufWriter::with_capacity(
                LIVE_FINALIZER_IO_BUFFER_SIZE,
                signatures_file,
            ))
        };
    let mut shredding_writer = if sidecars == LiveFinalizationSidecars::Canonical {
        let shredding_file = File::create(&shredding_path)
            .with_context(|| format!("create {}", shredding_path.display()))?;
        Some(WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
            LIVE_FINALIZER_IO_BUFFER_SIZE,
            shredding_file,
        )))
    } else {
        anyhow::ensure!(
            !shredding_path.exists(),
            "degraded repair finalization refuses canonical shredding output {}",
            shredding_path.display()
        );
        None
    };

    write_hot_meta(
        &mut meta_writer,
        &ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: ARCHIVE_FLAGS_LEB128,
        }),
    )?;

    let mut compressor = zstd::bulk::Compressor::new(level).context("create zstd compressor")?;
    let mut rolling_blockhashes = RollingBlockhashIndex::new(ROLLING_BLOCKHASH_CAPACITY);
    let mut progress = ProgressTracker::new("Archive V2 Live Hot Write");
    let mut timings = ArchiveV2Timings::from_env();
    let mut rows = Vec::new();
    let mut slot_to_block_id: GxHashMap<u64, u32> =
        GxHashMap::with_hasher(GxBuildHasher::default());
    let mut vote_hashes = VoteHashRegistryBuilder::default();
    let mut footer = WincodeArchiveV2Footer::default();
    let mut block_bytes = Vec::new();
    let mut compressed_buf = Vec::new();
    let mut hot_block_buffers = HotBlockBuffers::default();
    let mut blob_offset = 0u64;
    let mut uncompressed_bytes = 0u64;
    let mut compressed_bytes = 0u64;
    let mut first_tx_ordinal = 0u64;
    let mut first_signature_ordinal = 0u64;
    let mut nonce_recent_blockhashes = 0u64;
    let mut block_id = 0u32;
    let started = Instant::now();

    let read_signature_counts_only = signatures_writer.is_none();
    while let Some((_len, block)) = if read_signature_counts_only {
        read_live_no_registry_block_signature_counts(&mut reader)?
            .map(|(len, block)| (len, LiveFinalizerBlock::SignatureCounts(block)))
    } else {
        read_live_no_registry_block(&mut reader)?
            .map(|(len, block)| (len, LiveFinalizerBlock::Full(block)))
    } {
        let slot = block.slot();
        anyhow::ensure!(
            block.blockhash_id() == block_id,
            "live block slot {} has blockhash id {}, expected {}",
            slot,
            block.blockhash_id(),
            block_id
        );
        let current_blockhash = *blockhashes.get(block_id as usize).with_context(|| {
            format!("missing live blockhash registry row for block_id {block_id}")
        })?;

        rolling_blockhashes.prune_for_slot(slot)?;
        let (hot_block, block_signature_count) = hot_block_from_live_finalizer_block(
            block,
            &key_index,
            &rolling_blockhashes,
            block_id,
            &mut nonce_recent_blockhashes,
            &known_program_ids,
            &slot_to_block_id,
            &mut vote_hashes,
            signatures_writer
                .as_mut()
                .map(|writer| writer as &mut dyn Write),
            None,
            &mut hot_block_buffers,
            &mut timings,
        )
        .with_context(|| format!("slot {slot} live hot block encode"))?;
        let tx_count = hot_block.tx_count;

        block_bytes.clear();
        let serialize_started = timings.detail_timer();
        wincode::config::serialize_into(&mut block_bytes, &hot_block, wincode_leb128_config())?;
        timings.hot_block_serialize += serialize_started.elapsed();
        hot_block_buffers.recycle(hot_block);
        let uncompressed_len =
            u32::try_from(block_bytes.len()).context("hot live block payload exceeds u32::MAX")?;
        let compress_bound = zstd::zstd_safe::compress_bound(block_bytes.len());
        if compressed_buf.capacity() < compress_bound {
            compressed_buf.reserve(compress_bound.saturating_sub(compressed_buf.len()));
            timings.hot_zstd_buffer_reserves += 1;
        }
        timings.hot_zstd_buffer_capacity_max = timings
            .hot_zstd_buffer_capacity_max
            .max(compressed_buf.capacity());
        let compress_started = timings.detail_timer();
        compressor
            .compress_to_buffer(&block_bytes, &mut compressed_buf)
            .with_context(|| format!("zstd compress live hot block_id {block_id}"))?;
        timings.hot_zstd_compress += compress_started.elapsed();
        let compressed_len = u32::try_from(compressed_buf.len())
            .context("compressed live hot block exceeds u32::MAX")?;
        let write_started = timings.detail_timer();
        blocks_writer
            .write_all(&compressed_buf)
            .with_context(|| format!("write {}", blocks_path.display()))?;
        timings.hot_block_write += write_started.elapsed();
        if let Some(shredding_writer) = shredding_writer.as_mut() {
            let shredding_started = timings.detail_timer();
            shredding_writer.write(&WincodeArchiveV2ShreddingRecord {
                block_id,
                slot,
                shredding: Vec::new(),
            })?;
            timings.hot_poh_write += shredding_started.elapsed();
        }

        rows.push(ArchiveV2HotBlockIndexRow {
            block_id,
            slot,
            compressed_offset: blob_offset,
            compressed_len,
            uncompressed_len,
            tx_count,
            first_tx_ordinal,
            first_signature_ordinal,
            signature_count: block_signature_count,
        });
        blob_offset += compressed_len as u64;
        uncompressed_bytes += uncompressed_len as u64;
        compressed_bytes += compressed_len as u64;
        first_tx_ordinal += tx_count as u64;
        first_signature_ordinal += block_signature_count as u64;
        footer.blocks += 1;
        footer.transactions += tx_count as u64;

        rolling_blockhashes.insert(current_blockhash, block_id as i32, slot)?;
        slot_to_block_id.insert(slot, block_id);
        block_id = block_id.wrapping_add(1);
        progress.update_slot(slot);
        progress.update(1, tx_count as u64);
        trim_hot_memory(
            block_id,
            &mut block_bytes,
            &mut compressed_buf,
            &mut Vec::new(),
            None,
        );
        hot_block_buffers.trim();
        if max_blocks.is_some_and(|limit| rows.len() as u64 >= limit) {
            break;
        }
    }

    if max_blocks.is_none() {
        anyhow::ensure!(
            block_id as usize == blockhashes.len(),
            "live blockhash registry count {} does not match live blocks {}",
            blockhashes.len(),
            block_id
        );
    }

    footer.nonce_recent_blockhashes += nonce_recent_blockhashes;
    blocks_writer
        .flush()
        .with_context(|| format!("flush {}", blocks_path.display()))?;
    if let Some(writer) = signatures_writer.as_mut() {
        writer
            .flush()
            .with_context(|| format!("flush {}", signatures_path.display()))?;
    } else if reused_live_signatures {
        let expected_signature_bytes = first_signature_ordinal
            .checked_mul(64)
            .context("signature byte count overflow")?;
        let actual_signature_bytes = std::fs::metadata(&signatures_path)
            .with_context(|| format!("stat {}", signatures_path.display()))?
            .len();
        anyhow::ensure!(
            actual_signature_bytes == expected_signature_bytes,
            "live signature sidecar byte length {} does not match expected {} signatures bytes {}",
            actual_signature_bytes,
            first_signature_ordinal,
            expected_signature_bytes
        );
    }
    if let Some(shredding_writer) = shredding_writer.as_mut() {
        shredding_writer.flush()?;
    }
    vote_hashes.write(&vote_hash_registry_path)?;
    write_archive_v2_hot_block_index(&index_path, blob_offset, level, 0, &rows)?;
    write_hot_meta(
        &mut meta_writer,
        &ArchiveV2HotMetaRecord::Footer(footer.clone()),
    )?;
    meta_writer.flush()?;
    drop(meta_writer);
    // Metadata is the publication marker for readers and the pipeline. Make every candidate
    // sidecar durable first, then atomically reveal metadata last. A killed live finalizer can
    // therefore leave only an unmistakable partial directory, never a false completed epoch.
    crate::first_seen_finalization::sync_candidate_files(output_dir)?;
    std::fs::rename(&meta_tmp, &meta_path)
        .with_context(|| format!("rename {} to {}", meta_tmp.display(), meta_path.display()))?;
    crate::first_seen_finalization::sync_directory(output_dir)?;
    progress.final_report();
    let ratio = if uncompressed_bytes > 0 {
        compressed_bytes as f64 * 100.0 / uncompressed_bytes as f64
    } else {
        0.0
    };
    let poh_display = if sidecars == LiveFinalizationSidecars::Canonical {
        poh_path.display().to_string()
    } else {
        "omitted_noncanonical".to_string()
    };
    let shredding_display = if sidecars == LiveFinalizationSidecars::Canonical {
        shredding_path.display().to_string()
    } else {
        "omitted_noncanonical".to_string()
    };
    info!(
        "Archive V2 live hot-block build complete in {:.2}s: blocks={} txs={} signatures={} level={} max_blocks={:?} uncompressed_bytes={} compressed_bytes={} ratio_pct={:.2} nonce_recent_blockhashes={} blocks_file={} index={} meta={} signatures={} vote_hash_registry={} poh={} shredding={}",
        started.elapsed().as_secs_f64(),
        rows.len(),
        first_tx_ordinal,
        first_signature_ordinal,
        level,
        max_blocks,
        uncompressed_bytes,
        compressed_bytes,
        ratio,
        nonce_recent_blockhashes,
        blocks_path.display(),
        index_path.display(),
        meta_path.display(),
        signatures_path.display(),
        vote_hash_registry_path.display(),
        poh_display,
        shredding_display
    );
    info!(
        "Archive V2 live hot timings: optimize_no_registry_total={:.3}s tx_decode_compact={:.3}s metadata_decode_compact={:.3}s metadata_logs_decode_compact={:.3}s metadata_pubkey_compact={:.3}s rewards_decode_compact={:.3}s hot_message_build={:.3}s hot_message_encode={:.3}s hot_metadata_encode={:.3}s hot_signature_write={:.3}s hot_block_serialize={:.3}s hot_zstd_compress={:.3}s hot_block_write={:.3}s hot_shredding_write={:.3}s zstd_buffer_reserves={} zstd_buffer_capacity_max={}",
        (timings.tx_decode_compact
            + timings.metadata_decode_compact
            + timings.rewards_decode_compact)
            .as_secs_f64(),
        timings.tx_decode_compact.as_secs_f64(),
        timings.metadata_decode_compact.as_secs_f64(),
        timings.metadata_logs_decode_compact.as_secs_f64(),
        timings.metadata_pubkey_compact.as_secs_f64(),
        timings.rewards_decode_compact.as_secs_f64(),
        timings.hot_message_build.as_secs_f64(),
        timings.hot_message_encode.as_secs_f64(),
        timings.hot_metadata_encode.as_secs_f64(),
        timings.hot_signature_write.as_secs_f64(),
        timings.hot_block_serialize.as_secs_f64(),
        timings.hot_zstd_compress.as_secs_f64(),
        timings.hot_block_write.as_secs_f64(),
        timings.hot_poh_write.as_secs_f64(),
        timings.hot_zstd_buffer_reserves,
        timings.hot_zstd_buffer_capacity_max,
    );
    Ok(LiveHotBuildReport {
        blocks: rows.len() as u64,
        transactions: first_tx_ordinal,
        signatures: first_signature_ordinal,
        compressed_bytes,
        uncompressed_bytes,
    })
}

fn build_live_no_registry_pubkey_registry(
    input: &Path,
    registry_path: &Path,
    registry_counts_path: &Path,
    max_blocks: Option<u64>,
) -> Result<()> {
    info!("Building live pubkey registry from {}", input.display());
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(
        LIVE_FINALIZER_IO_BUFFER_SIZE,
        file,
    ));
    let mut counter = ArchivePubkeyCounter::new(8_000_000);
    let mut progress = ProgressTracker::new("Live Archive V2 Registry");
    let started = Instant::now();
    let mut blocks = 0u64;
    let mut txs = 0u64;

    while let Some((_len, block)) = read_live_no_registry_block(&mut reader)? {
        let tx_count = count_no_registry_block_pubkeys(&block, &mut counter)
            .with_context(|| format!("slot {} count pubkeys", block.header.compact.slot))?;
        blocks += 1;
        txs += tx_count;
        progress.update_slot(block.header.compact.slot);
        progress.update(1, tx_count);
        if max_blocks.is_some_and(|limit| blocks >= limit) {
            break;
        }
    }

    progress.final_report();
    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });

    const BUILTIN_PROGRAM_KEYS: &[Pubkey] = &[solana_pubkey::pubkey!(
        "ComputeBudget111111111111111111111111111111"
    )];
    let missing_builtins = BUILTIN_PROGRAM_KEYS
        .iter()
        .map(|key| key.to_bytes())
        .filter(|builtin| !items.iter().any(|(key, _)| key == builtin))
        .collect::<Vec<_>>();
    let registry_len = missing_builtins.len() + items.len();

    write_registry_iter(
        registry_path,
        missing_builtins
            .iter()
            .copied()
            .chain(items.iter().map(|(key, _)| *key)),
    )
    .with_context(|| format!("write {}", registry_path.display()))?;
    write_live_registry_counts(
        registry_counts_path,
        std::iter::repeat(0)
            .take(missing_builtins.len())
            .chain(items.iter().map(|(_, count)| *count)),
    )
    .with_context(|| format!("write {}", registry_counts_path.display()))?;
    info!(
        "Live pubkey registry built in {:.2}s: blocks={} txs={} keys={} registry={} counts={}",
        started.elapsed().as_secs_f64(),
        blocks,
        txs,
        registry_len,
        registry_path.display(),
        registry_counts_path.display()
    );
    Ok(())
}

fn build_live_pubkey_registry_from_counts(
    input: &Path,
    registry_path: &Path,
    registry_counts_path: &Path,
) -> Result<()> {
    info!(
        "Building live pubkey registry from counts {}",
        input.display()
    );
    anyhow::ensure!(
        crate::file_nonempty(input),
        "live pubkey-count sidecar missing or empty: {}",
        input.display()
    );

    let started = Instant::now();
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(
        LIVE_FINALIZER_IO_BUFFER_SIZE,
        file,
    ));
    let mut counts = GxHashMap::<[u8; 32], u32>::with_hasher(GxBuildHasher::default());
    let mut records = 0u64;
    while let Some((_len, record)) = reader.read::<LivePubkeyCountRecord>()? {
        records += 1;
        let count = u32::try_from(record.count).unwrap_or(u32::MAX);
        counts
            .entry(record.pubkey)
            .and_modify(|existing| *existing = existing.saturating_add(count))
            .or_insert(count);
    }
    let mut items = counts.into_iter().collect::<Vec<_>>();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });

    const BUILTIN_PROGRAM_KEYS: &[Pubkey] = &[solana_pubkey::pubkey!(
        "ComputeBudget111111111111111111111111111111"
    )];
    let missing_builtins = BUILTIN_PROGRAM_KEYS
        .iter()
        .map(|key| key.to_bytes())
        .filter(|builtin| !items.iter().any(|(key, _)| key == builtin))
        .collect::<Vec<_>>();
    let registry_len = missing_builtins.len() + items.len();

    write_registry_iter(
        registry_path,
        missing_builtins
            .iter()
            .copied()
            .chain(items.iter().map(|(key, _)| *key)),
    )
    .with_context(|| format!("write {}", registry_path.display()))?;
    write_live_registry_counts(
        registry_counts_path,
        std::iter::repeat(0)
            .take(missing_builtins.len())
            .chain(items.iter().map(|(_, count)| *count)),
    )
    .with_context(|| format!("write {}", registry_counts_path.display()))?;

    info!(
        "Live pubkey registry built from counts in {:.2}s: records={} keys={} registry={} counts={}",
        started.elapsed().as_secs_f64(),
        records,
        registry_len,
        registry_path.display(),
        registry_counts_path.display()
    );
    Ok(())
}

fn build_live_pubkey_registry_from_runs(
    run_dir: &Path,
    registry_path: &Path,
    registry_counts_path: &Path,
    temp_dir: &Path,
) -> Result<()> {
    info!(
        "Building live pubkey registry from sorted runs {}",
        run_dir.display()
    );
    let runs = collect_pubkey_run_paths(run_dir)?;
    anyhow::ensure!(
        !runs.is_empty(),
        "live pubkey run sidecar missing or empty: {}",
        run_dir.display()
    );

    if temp_dir.exists() {
        std::fs::remove_dir_all(temp_dir)
            .with_context(|| format!("remove {}", temp_dir.display()))?;
    }
    let freq_run_dir = temp_dir.join("freq-runs");
    std::fs::create_dir_all(&freq_run_dir)
        .with_context(|| format!("create {}", freq_run_dir.display()))?;

    let started = Instant::now();
    let bounded_key_runs = reduce_key_runs_to_fan_in(
        &runs,
        &temp_dir.join("key-merge-passes"),
        LIVE_PUBKEY_RUN_MERGE_FAN_IN,
    )?;
    let merge_report = merge_key_runs_to_frequency_runs(&bounded_key_runs, &freq_run_dir)?;
    let bounded_frequency_runs = reduce_frequency_runs_to_fan_in(
        &merge_report.frequency_runs,
        &temp_dir.join("frequency-merge-passes"),
        LIVE_PUBKEY_RUN_MERGE_FAN_IN,
    )?;
    write_registry_from_frequency_runs(
        &bounded_frequency_runs,
        registry_path,
        registry_counts_path,
        &merge_report.missing_builtins,
    )?;
    std::fs::remove_dir_all(temp_dir).with_context(|| format!("remove {}", temp_dir.display()))?;

    info!(
        "Live pubkey registry built from sorted runs in {:.2}s: unique_keys={} key_runs={} frequency_runs={} registry={} counts={}",
        started.elapsed().as_secs_f64(),
        merge_report.unique_keys,
        runs.len(),
        merge_report.frequency_runs.len(),
        registry_path.display(),
        registry_counts_path.display()
    );
    Ok(())
}

fn collect_pubkey_run_paths(run_dir: &Path) -> Result<Vec<PathBuf>> {
    if !run_dir.exists() {
        return Ok(Vec::new());
    }
    let mut runs = Vec::new();
    for entry in
        std::fs::read_dir(run_dir).with_context(|| format!("read {}", run_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == LIVE_PUBKEY_RUN_HOT_FILE || (name.starts_with("run-") && name.ends_with(".bin"))
        {
            let bytes = entry
                .metadata()
                .with_context(|| format!("stat {}", path.display()))?
                .len();
            anyhow::ensure!(
                bytes % LIVE_PUBKEY_RUN_RECORD_LEN as u64 == 0,
                "pubkey run {} has non-record-aligned length {}",
                path.display(),
                bytes
            );
            if bytes > 0 {
                runs.push(path);
            }
        }
    }
    runs.sort();
    Ok(runs)
}

fn pubkey_run_dir_nonempty(run_dir: &Path) -> Result<bool> {
    Ok(!collect_pubkey_run_paths(run_dir)?.is_empty())
}

fn preferred_auto_live_registry_source(
    counts_path: &Path,
    runs_dir: &Path,
    touches_path: &Path,
) -> Result<LiveRegistrySource> {
    if pubkey_run_dir_nonempty(runs_dir)? {
        Ok(LiveRegistrySource::Runs)
    } else if crate::file_nonempty(counts_path) {
        Ok(LiveRegistrySource::Counts)
    } else if crate::file_nonempty(touches_path) {
        Ok(LiveRegistrySource::Touches)
    } else {
        Ok(LiveRegistrySource::Scan)
    }
}

const PUBKEY_TOUCH_SORT_CHUNK_KEYS: usize = 1_000_000;
const PUBKEY_COUNT_SORT_CHUNK_RECORDS: usize = 1_000_000;

#[derive(Clone, Copy)]
struct PubkeyCountRecord {
    key: [u8; 32],
    count: u32,
}

fn build_live_pubkey_registry_from_touches(
    input: &Path,
    registry_path: &Path,
    registry_counts_path: &Path,
    temp_dir: &Path,
) -> Result<()> {
    info!(
        "Building live pubkey registry from touch log {}",
        input.display()
    );
    anyhow::ensure!(
        crate::file_nonempty(input),
        "live pubkey-touch sidecar missing or empty: {}",
        input.display()
    );
    let bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        bytes % 32 == 0,
        "pubkey touch log length {} is not a multiple of 32: {}",
        bytes,
        input.display()
    );

    if temp_dir.exists() {
        std::fs::remove_dir_all(temp_dir)
            .with_context(|| format!("remove {}", temp_dir.display()))?;
    }
    let key_run_dir = temp_dir.join("key-runs");
    let freq_run_dir = temp_dir.join("freq-runs");
    std::fs::create_dir_all(&key_run_dir)
        .with_context(|| format!("create {}", key_run_dir.display()))?;
    std::fs::create_dir_all(&freq_run_dir)
        .with_context(|| format!("create {}", freq_run_dir.display()))?;

    let started = Instant::now();
    let (key_runs, touch_count) = spill_pubkey_touch_key_runs(input, &key_run_dir)?;
    let merge_started = Instant::now();
    let bounded_key_runs = reduce_key_runs_to_fan_in(
        &key_runs,
        &temp_dir.join("key-merge-passes"),
        LIVE_PUBKEY_RUN_MERGE_FAN_IN,
    )?;
    let merge_report = merge_key_runs_to_frequency_runs(&bounded_key_runs, &freq_run_dir)?;
    let bounded_frequency_runs = reduce_frequency_runs_to_fan_in(
        &merge_report.frequency_runs,
        &temp_dir.join("frequency-merge-passes"),
        LIVE_PUBKEY_RUN_MERGE_FAN_IN,
    )?;
    write_registry_from_frequency_runs(
        &bounded_frequency_runs,
        registry_path,
        registry_counts_path,
        &merge_report.missing_builtins,
    )?;
    std::fs::remove_dir_all(temp_dir).with_context(|| format!("remove {}", temp_dir.display()))?;

    info!(
        "Live pubkey registry built from touches in {:.2}s: touches={} unique_keys={} key_runs={} frequency_runs={} key_merge_s={:.2} registry={} counts={}",
        started.elapsed().as_secs_f64(),
        touch_count,
        merge_report.unique_keys,
        key_runs.len(),
        merge_report.frequency_runs.len(),
        merge_started.elapsed().as_secs_f64(),
        registry_path.display(),
        registry_counts_path.display()
    );
    Ok(())
}

fn spill_pubkey_touch_key_runs(input: &Path, run_dir: &Path) -> Result<(Vec<PathBuf>, u64)> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let total_touches = file
        .metadata()
        .with_context(|| format!("stat {}", input.display()))?
        .len()
        / 32;
    let mut reader = BufReader::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, file);
    let mut keys = Vec::<[u8; 32]>::with_capacity(PUBKEY_TOUCH_SORT_CHUNK_KEYS);
    let mut runs = Vec::new();
    let mut remaining = total_touches;
    let mut run_index = 0usize;

    while remaining > 0 {
        keys.clear();
        let take = remaining.min(PUBKEY_TOUCH_SORT_CHUNK_KEYS as u64) as usize;
        for _ in 0..take {
            let mut key = [0u8; 32];
            reader
                .read_exact(&mut key)
                .with_context(|| format!("read pubkey touch from {}", input.display()))?;
            keys.push(key);
        }
        keys.sort_unstable();
        let path = run_dir.join(format!("key-run-{run_index:05}.bin"));
        write_sorted_key_count_run(&path, &keys)?;
        runs.push(path);
        run_index += 1;
        remaining -= take as u64;
    }

    Ok((runs, total_touches))
}

fn write_sorted_key_count_run(path: &Path, keys: &[[u8; 32]]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, file);
    let mut index = 0usize;
    while index < keys.len() {
        let key = keys[index];
        let mut count = 1u32;
        index += 1;
        while index < keys.len() && keys[index] == key {
            count = count.saturating_add(1);
            index += 1;
        }
        write_pubkey_count_record(&mut writer, PubkeyCountRecord { key, count })?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn validate_live_run_merge_fan_in(fan_in: usize) -> Result<()> {
    anyhow::ensure!(
        fan_in >= 2,
        "live pubkey run merge fan-in must be at least 2"
    );
    anyhow::ensure!(
        fan_in <= LIVE_PUBKEY_RUN_MERGE_FAN_IN,
        "live pubkey run merge fan-in {fan_in} exceeds cursor cap {}",
        LIVE_PUBKEY_RUN_MERGE_FAN_IN
    );
    Ok(())
}

fn reduce_key_runs_to_fan_in(
    key_runs: &[PathBuf],
    merge_root: &Path,
    fan_in: usize,
) -> Result<Vec<PathBuf>> {
    validate_live_run_merge_fan_in(fan_in)?;
    if key_runs.len() <= fan_in {
        return Ok(key_runs.to_vec());
    }
    if merge_root.exists() {
        std::fs::remove_dir_all(merge_root)
            .with_context(|| format!("remove stale key merge root {}", merge_root.display()))?;
    }
    std::fs::create_dir_all(merge_root)
        .with_context(|| format!("create key merge root {}", merge_root.display()))?;

    let mut current = key_runs.to_vec();
    let mut pass = 0usize;
    while current.len() > fan_in {
        let pass_dir = merge_root.join(format!("pass-{pass:03}"));
        std::fs::create_dir_all(&pass_dir)
            .with_context(|| format!("create key merge pass {}", pass_dir.display()))?;
        let mut next = Vec::with_capacity(current.len().div_ceil(fan_in));
        for (group_index, group) in current.chunks(fan_in).enumerate() {
            let output = pass_dir.join(format!("key-run-{group_index:06}.bin"));
            merge_key_run_group(group, &output)?;
            next.push(output);
        }
        current = next;
        pass += 1;
    }
    Ok(current)
}

fn merge_key_run_group(key_runs: &[PathBuf], output: &Path) -> Result<()> {
    anyhow::ensure!(
        !key_runs.is_empty() && key_runs.len() <= LIVE_PUBKEY_RUN_MERGE_FAN_IN,
        "key-run merge group size {} exceeds cursor cap {}",
        key_runs.len(),
        LIVE_PUBKEY_RUN_MERGE_FAN_IN
    );
    let mut cursors = Vec::with_capacity(key_runs.len());
    let mut heap = BinaryHeap::<KeyRunHeapItem>::new();
    for path in key_runs {
        let cursor = PubkeyCountRunCursor::open(path)?;
        if let Some(record) = cursor.current {
            let run_index = cursors.len();
            heap.push(KeyRunHeapItem {
                key: record.key,
                run_index,
            });
        }
        cursors.push(cursor);
    }

    let file = File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut writer = BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, file);
    while let Some(item) = heap.pop() {
        let key = item.key;
        let mut count = take_current_and_advance(&mut cursors, item.run_index, &mut heap)?
            .context("key-run heap pointed at empty cursor")?
            .count;
        while heap.peek().is_some_and(|next| next.key == key) {
            let next = heap.pop().expect("heap peeked Some");
            let record = take_current_and_advance(&mut cursors, next.run_index, &mut heap)?
                .context("key-run heap pointed at empty cursor")?;
            count = count.saturating_add(record.count);
        }
        write_pubkey_count_record(&mut writer, PubkeyCountRecord { key, count })?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", output.display()))?;
    Ok(())
}

fn reduce_frequency_runs_to_fan_in(
    frequency_runs: &[PathBuf],
    merge_root: &Path,
    fan_in: usize,
) -> Result<Vec<PathBuf>> {
    validate_live_run_merge_fan_in(fan_in)?;
    if frequency_runs.len() <= fan_in {
        return Ok(frequency_runs.to_vec());
    }
    if merge_root.exists() {
        std::fs::remove_dir_all(merge_root).with_context(|| {
            format!("remove stale frequency merge root {}", merge_root.display())
        })?;
    }
    std::fs::create_dir_all(merge_root)
        .with_context(|| format!("create frequency merge root {}", merge_root.display()))?;

    let mut current = frequency_runs.to_vec();
    let mut pass = 0usize;
    while current.len() > fan_in {
        let pass_dir = merge_root.join(format!("pass-{pass:03}"));
        std::fs::create_dir_all(&pass_dir)
            .with_context(|| format!("create frequency merge pass {}", pass_dir.display()))?;
        let mut next = Vec::with_capacity(current.len().div_ceil(fan_in));
        for (group_index, group) in current.chunks(fan_in).enumerate() {
            let output = pass_dir.join(format!("freq-run-{group_index:06}.bin"));
            merge_frequency_run_group(group, &output)?;
            next.push(output);
        }
        current = next;
        pass += 1;
    }
    Ok(current)
}

fn merge_frequency_run_group(frequency_runs: &[PathBuf], output: &Path) -> Result<()> {
    anyhow::ensure!(
        !frequency_runs.is_empty() && frequency_runs.len() <= LIVE_PUBKEY_RUN_MERGE_FAN_IN,
        "frequency-run merge group size {} exceeds cursor cap {}",
        frequency_runs.len(),
        LIVE_PUBKEY_RUN_MERGE_FAN_IN
    );
    let mut cursors = Vec::with_capacity(frequency_runs.len());
    let mut heap = BinaryHeap::<FrequencyRunHeapItem>::new();
    for path in frequency_runs {
        let cursor = PubkeyCountRunCursor::open(path)?;
        if let Some(record) = cursor.current {
            let run_index = cursors.len();
            heap.push(FrequencyRunHeapItem {
                count: record.count,
                key: record.key,
                run_index,
            });
        }
        cursors.push(cursor);
    }

    let file = File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut writer = BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, file);
    while let Some(item) = heap.pop() {
        write_pubkey_count_record(
            &mut writer,
            PubkeyCountRecord {
                key: item.key,
                count: item.count,
            },
        )?;
        let cursor = cursors
            .get_mut(item.run_index)
            .ok_or_else(|| anyhow!("invalid frequency-run cursor index {}", item.run_index))?;
        cursor.advance()?;
        if let Some(next) = cursor.current {
            heap.push(FrequencyRunHeapItem {
                count: next.count,
                key: next.key,
                run_index: item.run_index,
            });
        }
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", output.display()))?;
    Ok(())
}

struct TouchMergeReport {
    frequency_runs: Vec<PathBuf>,
    missing_builtins: Vec<[u8; 32]>,
    unique_keys: u64,
}

fn merge_key_runs_to_frequency_runs(
    key_runs: &[PathBuf],
    freq_run_dir: &Path,
) -> Result<TouchMergeReport> {
    merge_key_runs_to_frequency_runs_with_chunk_records(
        key_runs,
        freq_run_dir,
        PUBKEY_COUNT_SORT_CHUNK_RECORDS,
    )
}

fn merge_key_runs_to_frequency_runs_with_chunk_records(
    key_runs: &[PathBuf],
    freq_run_dir: &Path,
    frequency_chunk_records: usize,
) -> Result<TouchMergeReport> {
    anyhow::ensure!(
        !key_runs.is_empty() && key_runs.len() <= LIVE_PUBKEY_RUN_MERGE_FAN_IN,
        "key-to-frequency merge input count {} exceeds cursor cap {}",
        key_runs.len(),
        LIVE_PUBKEY_RUN_MERGE_FAN_IN
    );
    anyhow::ensure!(
        frequency_chunk_records > 0,
        "frequency chunk record count must be positive"
    );
    let mut cursors = Vec::with_capacity(key_runs.len());
    let mut heap = BinaryHeap::<KeyRunHeapItem>::new();
    for path in key_runs {
        let cursor = PubkeyCountRunCursor::open(path)?;
        if let Some(record) = cursor.current {
            let run_index = cursors.len();
            heap.push(KeyRunHeapItem {
                key: record.key,
                run_index,
            });
        }
        cursors.push(cursor);
    }

    let builtin_keys = live_builtin_program_keys();
    let mut builtin_seen = vec![false; builtin_keys.len()];
    let mut frequency_chunk = Vec::<PubkeyCountRecord>::with_capacity(frequency_chunk_records);
    let mut frequency_runs = Vec::new();
    let mut unique_keys = 0u64;

    while let Some(item) = heap.pop() {
        let key = item.key;
        let mut count = take_current_and_advance(&mut cursors, item.run_index, &mut heap)?
            .context("key-run heap pointed at empty cursor")?
            .count;

        while heap.peek().is_some_and(|next| next.key == key) {
            let next = heap.pop().expect("heap peeked Some");
            let record = take_current_and_advance(&mut cursors, next.run_index, &mut heap)?
                .context("key-run heap pointed at empty cursor")?;
            count = count.saturating_add(record.count);
        }

        for (index, builtin) in builtin_keys.iter().enumerate() {
            if key == *builtin {
                builtin_seen[index] = true;
            }
        }
        frequency_chunk.push(PubkeyCountRecord { key, count });
        unique_keys += 1;
        if frequency_chunk.len() >= frequency_chunk_records {
            spill_frequency_run(freq_run_dir, &mut frequency_runs, &mut frequency_chunk)?;
        }
    }
    if !frequency_chunk.is_empty() {
        spill_frequency_run(freq_run_dir, &mut frequency_runs, &mut frequency_chunk)?;
    }

    let missing_builtins = builtin_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| (!builtin_seen[index]).then_some(*key))
        .collect::<Vec<_>>();

    Ok(TouchMergeReport {
        frequency_runs,
        missing_builtins,
        unique_keys,
    })
}

fn take_current_and_advance(
    cursors: &mut [PubkeyCountRunCursor],
    run_index: usize,
    heap: &mut BinaryHeap<KeyRunHeapItem>,
) -> Result<Option<PubkeyCountRecord>> {
    let cursor = cursors
        .get_mut(run_index)
        .ok_or_else(|| anyhow!("invalid key-run cursor index {run_index}"))?;
    let record = cursor.current.take();
    cursor.advance()?;
    if let Some(next) = cursor.current {
        heap.push(KeyRunHeapItem {
            key: next.key,
            run_index,
        });
    }
    Ok(record)
}

fn spill_frequency_run(
    freq_run_dir: &Path,
    frequency_runs: &mut Vec<PathBuf>,
    chunk: &mut Vec<PubkeyCountRecord>,
) -> Result<()> {
    chunk.sort_unstable_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.key.cmp(&right.key))
    });
    let path = freq_run_dir.join(format!("freq-run-{:05}.bin", frequency_runs.len()));
    let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, file);
    for record in chunk.drain(..) {
        write_pubkey_count_record(&mut writer, record)?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    frequency_runs.push(path);
    Ok(())
}

fn write_registry_from_frequency_runs(
    frequency_runs: &[PathBuf],
    registry_path: &Path,
    registry_counts_path: &Path,
    missing_builtins: &[[u8; 32]],
) -> Result<()> {
    anyhow::ensure!(
        !frequency_runs.is_empty() && frequency_runs.len() <= LIVE_PUBKEY_RUN_MERGE_FAN_IN,
        "final frequency-run input count {} exceeds cursor cap {}",
        frequency_runs.len(),
        LIVE_PUBKEY_RUN_MERGE_FAN_IN
    );
    let mut cursors = Vec::with_capacity(frequency_runs.len());
    let mut heap = BinaryHeap::<FrequencyRunHeapItem>::new();
    for path in frequency_runs {
        let cursor = PubkeyCountRunCursor::open(path)?;
        if let Some(record) = cursor.current {
            let run_index = cursors.len();
            heap.push(FrequencyRunHeapItem {
                count: record.count,
                key: record.key,
                run_index,
            });
        }
        cursors.push(cursor);
    }

    let registry_file = File::create(registry_path)
        .with_context(|| format!("create {}", registry_path.display()))?;
    let mut registry_writer =
        BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, registry_file);
    let counts_file = File::create(registry_counts_path)
        .with_context(|| format!("create {}", registry_counts_path.display()))?;
    let mut counts_writer = BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, counts_file);

    for builtin in missing_builtins {
        registry_writer.write_all(builtin)?;
        blockzilla_format::framed::write_u32_varint(&mut counts_writer, 0)?;
    }

    while let Some(item) = heap.pop() {
        registry_writer.write_all(&item.key)?;
        blockzilla_format::framed::write_u32_varint(&mut counts_writer, item.count)?;
        let cursor = cursors
            .get_mut(item.run_index)
            .ok_or_else(|| anyhow!("invalid frequency-run cursor index {}", item.run_index))?;
        cursor.advance()?;
        if let Some(next) = cursor.current {
            heap.push(FrequencyRunHeapItem {
                count: next.count,
                key: next.key,
                run_index: item.run_index,
            });
        }
    }

    registry_writer
        .flush()
        .with_context(|| format!("flush {}", registry_path.display()))?;
    counts_writer
        .flush()
        .with_context(|| format!("flush {}", registry_counts_path.display()))?;
    Ok(())
}

struct PubkeyCountRunCursor {
    reader: BufReader<File>,
    current: Option<PubkeyCountRecord>,
}

impl PubkeyCountRunCursor {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let mut cursor = Self {
            reader: BufReader::with_capacity(LIVE_PUBKEY_RUN_READER_BUFFER_SIZE, file),
            current: None,
        };
        cursor.advance()?;
        Ok(cursor)
    }

    fn advance(&mut self) -> Result<()> {
        self.current = read_pubkey_count_record(&mut self.reader)?;
        Ok(())
    }
}

fn write_pubkey_count_record(writer: &mut impl Write, record: PubkeyCountRecord) -> Result<()> {
    writer.write_all(&record.key)?;
    writer.write_all(&record.count.to_le_bytes())?;
    Ok(())
}

fn read_pubkey_count_record(reader: &mut impl Read) -> Result<Option<PubkeyCountRecord>> {
    let mut key = [0u8; 32];
    let read = reader.read(&mut key)?;
    if read == 0 {
        return Ok(None);
    }
    if read < key.len() {
        reader.read_exact(&mut key[read..])?;
    }
    let mut count_bytes = [0u8; 4];
    reader.read_exact(&mut count_bytes)?;
    Ok(Some(PubkeyCountRecord {
        key,
        count: u32::from_le_bytes(count_bytes),
    }))
}

#[derive(Eq, PartialEq)]
struct KeyRunHeapItem {
    key: [u8; 32],
    run_index: usize,
}

impl Ord for KeyRunHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for KeyRunHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Eq, PartialEq)]
struct FrequencyRunHeapItem {
    count: u32,
    key: [u8; 32],
    run_index: usize,
}

impl Ord for FrequencyRunHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count
            .cmp(&other.count)
            .then_with(|| other.key.cmp(&self.key))
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for FrequencyRunHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn live_builtin_program_keys() -> &'static [[u8; 32]] {
    const BUILTIN_PROGRAM_KEYS: [[u8; 32]; 1] =
        [solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111").to_bytes()];
    &BUILTIN_PROGRAM_KEYS
}

#[cfg(test)]
mod live_touch_registry_tests {
    use super::*;

    #[test]
    fn auto_live_registry_source_prefers_bounded_runs() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "blockzilla-auto-live-registry-test-{}-{unique}",
            std::process::id()
        ));
        let runs_dir = dir.join(LIVE_PUBKEY_RUNS_DIR);
        let counts_path = dir.join("pubkey-counts.bin");
        let touches_path = dir.join("pubkey-touches.bin");
        std::fs::create_dir_all(&runs_dir).unwrap();
        std::fs::write(&counts_path, [1u8]).unwrap();
        std::fs::write(&touches_path, [2u8]).unwrap();
        let run_path = runs_dir.join("run-000000.bin");
        let mut run = File::create(&run_path).unwrap();
        write_pubkey_count_record(
            &mut run,
            PubkeyCountRecord {
                key: [3u8; 32],
                count: 1,
            },
        )
        .unwrap();
        drop(run);

        assert_eq!(
            preferred_auto_live_registry_source(&counts_path, &runs_dir, &touches_path).unwrap(),
            LiveRegistrySource::Runs
        );
        std::fs::remove_file(&run_path).unwrap();
        assert_eq!(
            preferred_auto_live_registry_source(&counts_path, &runs_dir, &touches_path).unwrap(),
            LiveRegistrySource::Counts
        );
        std::fs::remove_file(&counts_path).unwrap();
        assert_eq!(
            preferred_auto_live_registry_source(&counts_path, &runs_dir, &touches_path).unwrap(),
            LiveRegistrySource::Touches
        );
        std::fs::remove_file(&touches_path).unwrap();
        assert_eq!(
            preferred_auto_live_registry_source(&counts_path, &runs_dir, &touches_path).unwrap(),
            LiveRegistrySource::Scan
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn touch_log_registry_builds_frequency_order() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "blockzilla-touch-registry-test-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let touches_path = dir.join("pubkey-touches.bin");
        let registry_path = dir.join("registry.bin");
        let counts_path = dir.join("registry_counts.bin");
        let temp_dir = dir.join("sort");

        let key_a = [1u8; 32];
        let key_b = [2u8; 32];
        let key_c = [3u8; 32];
        {
            let mut file = File::create(&touches_path).unwrap();
            for key in [key_a, key_b, key_b, key_c, key_a, key_b] {
                file.write_all(&key).unwrap();
            }
        }

        build_live_pubkey_registry_from_touches(
            &touches_path,
            &registry_path,
            &counts_path,
            &temp_dir,
        )
        .unwrap();

        let registry = std::fs::read(&registry_path).unwrap();
        let keys = registry
            .chunks_exact(32)
            .map(|chunk| {
                let mut key = [0u8; 32];
                key.copy_from_slice(chunk);
                key
            })
            .collect::<Vec<_>>();
        assert_eq!(keys[0], live_builtin_program_keys()[0]);
        assert_eq!(keys[1], key_b);
        assert_eq!(keys[2], key_a);
        assert_eq!(keys[3], key_c);

        let mut counts_reader = BufReader::new(File::open(&counts_path).unwrap());
        let mut counts = Vec::new();
        while let Some(count) =
            blockzilla_format::framed::read_u32_varint(&mut counts_reader).unwrap()
        {
            counts.push(count);
        }
        assert_eq!(counts, vec![0, 3, 2, 1]);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn pubkey_runs_registry_builds_frequency_order() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "blockzilla-run-registry-test-{}-{unique}",
            std::process::id()
        ));
        let run_dir = dir.join(LIVE_PUBKEY_RUNS_DIR);
        std::fs::create_dir_all(&run_dir).unwrap();
        let registry_path = dir.join("registry.bin");
        let counts_path = dir.join("registry_counts.bin");
        let temp_dir = dir.join("merge");

        let key_a = [1u8; 32];
        let key_b = [2u8; 32];
        let key_c = [3u8; 32];
        {
            let mut file = File::create(run_dir.join("run-000000.bin")).unwrap();
            write_pubkey_count_record(
                &mut file,
                PubkeyCountRecord {
                    key: key_a,
                    count: 1,
                },
            )
            .unwrap();
            write_pubkey_count_record(
                &mut file,
                PubkeyCountRecord {
                    key: key_b,
                    count: 2,
                },
            )
            .unwrap();
        }
        {
            let mut file = File::create(run_dir.join("run-000001.bin")).unwrap();
            write_pubkey_count_record(
                &mut file,
                PubkeyCountRecord {
                    key: key_a,
                    count: 2,
                },
            )
            .unwrap();
            write_pubkey_count_record(
                &mut file,
                PubkeyCountRecord {
                    key: key_c,
                    count: 1,
                },
            )
            .unwrap();
        }
        {
            let mut file = File::create(run_dir.join(LIVE_PUBKEY_RUN_HOT_FILE)).unwrap();
            write_pubkey_count_record(
                &mut file,
                PubkeyCountRecord {
                    key: key_b,
                    count: 2,
                },
            )
            .unwrap();
        }

        build_live_pubkey_registry_from_runs(&run_dir, &registry_path, &counts_path, &temp_dir)
            .unwrap();

        let registry = std::fs::read(&registry_path).unwrap();
        let keys = registry
            .chunks_exact(32)
            .map(|chunk| {
                let mut key = [0u8; 32];
                key.copy_from_slice(chunk);
                key
            })
            .collect::<Vec<_>>();
        assert_eq!(keys[0], live_builtin_program_keys()[0]);
        assert_eq!(keys[1], key_b);
        assert_eq!(keys[2], key_a);
        assert_eq!(keys[3], key_c);

        let mut counts_reader = BufReader::new(File::open(&counts_path).unwrap());
        let mut counts = Vec::new();
        while let Some(count) =
            blockzilla_format::framed::read_u32_varint(&mut counts_reader).unwrap()
        {
            counts.push(count);
        }
        assert_eq!(counts, vec![0, 4, 3, 1]);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn multi_pass_run_merges_preserve_counts_and_frequency_order() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "blockzilla-multi-pass-run-merge-test-{}-{unique}",
            std::process::id()
        ));
        let run_dir = dir.join("runs");
        let frequency_dir = dir.join("frequency");
        std::fs::create_dir_all(&run_dir).unwrap();
        std::fs::create_dir_all(&frequency_dir).unwrap();

        let key_a = [1u8; 32];
        let key_b = [2u8; 32];
        let key_c = [3u8; 32];
        let key_d = [4u8; 32];
        let key_e = [5u8; 32];
        let input_runs = [
            vec![(key_a, 1), (key_c, 1)],
            vec![(key_a, 2), (key_b, 5)],
            vec![(key_c, 4), (key_d, 2)],
            vec![(key_e, 3)],
            vec![(key_b, 1), (key_d, 4)],
        ];
        let mut run_paths = Vec::new();
        for (index, records) in input_runs.iter().enumerate() {
            let path = run_dir.join(format!("run-{index:06}.bin"));
            let mut file = File::create(&path).unwrap();
            for &(key, count) in records {
                write_pubkey_count_record(&mut file, PubkeyCountRecord { key, count }).unwrap();
            }
            run_paths.push(path);
        }

        let bounded_key_runs =
            reduce_key_runs_to_fan_in(&run_paths, &dir.join("key-passes"), 2).unwrap();
        assert_eq!(bounded_key_runs.len(), 2);
        let report = merge_key_runs_to_frequency_runs_with_chunk_records(
            &bounded_key_runs,
            &frequency_dir,
            2,
        )
        .unwrap();
        assert_eq!(report.unique_keys, 5);
        assert_eq!(report.frequency_runs.len(), 3);

        let bounded_frequency_runs = reduce_frequency_runs_to_fan_in(
            &report.frequency_runs,
            &dir.join("frequency-passes"),
            2,
        )
        .unwrap();
        assert_eq!(bounded_frequency_runs.len(), 2);
        let registry_path = dir.join("registry.bin");
        let counts_path = dir.join("registry_counts.bin");
        write_registry_from_frequency_runs(
            &bounded_frequency_runs,
            &registry_path,
            &counts_path,
            &report.missing_builtins,
        )
        .unwrap();

        let registry = std::fs::read(&registry_path).unwrap();
        let keys = registry
            .chunks_exact(32)
            .map(|chunk| {
                let mut key = [0u8; 32];
                key.copy_from_slice(chunk);
                key
            })
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                live_builtin_program_keys()[0],
                key_b,
                key_d,
                key_c,
                key_a,
                key_e,
            ]
        );
        let mut counts_reader = BufReader::new(File::open(&counts_path).unwrap());
        let mut counts = Vec::new();
        while let Some(count) =
            blockzilla_format::framed::read_u32_varint(&mut counts_reader).unwrap()
        {
            counts.push(count);
        }
        assert_eq!(counts, vec![0, 6, 6, 5, 3, 3]);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn prepared_live_registry_is_marked_reusable_and_rejects_corrupt_output() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "blockzilla-prepare-live-registry-test-{}-{unique}",
            std::process::id()
        ));
        let capture_dir = dir.join("capture");
        let run_dir = capture_dir.join("index").join(LIVE_PUBKEY_RUNS_DIR);
        let output_dir = dir.join("output");
        std::fs::create_dir_all(&run_dir).unwrap();

        let key_a = [1u8; 32];
        let key_b = [2u8; 32];
        let mut run = File::create(run_dir.join("run-000000.bin")).unwrap();
        write_pubkey_count_record(
            &mut run,
            PubkeyCountRecord {
                key: key_a,
                count: 2,
            },
        )
        .unwrap();
        write_pubkey_count_record(
            &mut run,
            PubkeyCountRecord {
                key: key_b,
                count: 3,
            },
        )
        .unwrap();
        drop(run);

        prepare_live_registry_from_runs(&capture_dir, &output_dir, true).unwrap();
        let registry_path = output_dir.join(REGISTRY_FILE);
        let marker_path = output_dir.join(LIVE_REGISTRY_PREPARED_MARKER);
        let expected_registry = std::fs::read(&registry_path).unwrap();
        assert!(marker_path.is_file());
        assert!(!output_dir.join(LIVE_REGISTRY_PREPARE_TEMP_DIR).exists());

        prepare_live_registry_from_runs(&capture_dir, &output_dir, true).unwrap();
        assert_eq!(std::fs::read(&registry_path).unwrap(), expected_registry);

        std::fs::write(&registry_path, [0u8]).unwrap();
        let error = prepare_live_registry_from_runs(&capture_dir, &output_dir, true).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("prepared live registry has invalid byte length")
        );

        prepare_live_registry_from_runs(&capture_dir, &output_dir, false).unwrap();
        assert_eq!(std::fs::read(&registry_path).unwrap(), expected_registry);
        assert!(marker_path.is_file());

        std::fs::remove_dir_all(&dir).unwrap();
    }
}

fn write_live_registry_counts<I>(path: &Path, counts: I) -> Result<()>
where
    I: IntoIterator<Item = u32>,
{
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, file);
    for count in counts {
        blockzilla_format::framed::write_u32_varint(&mut writer, count)?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn write_first_seen_registry_counts<I>(path: &Path, counts: I) -> Result<()>
where
    I: IntoIterator<Item = u32>,
{
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, file);
    for count in counts {
        blockzilla_format::framed::write_u32_varint(&mut writer, count)?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn read_live_no_registry_block<R: Read>(
    reader: &mut WincodeLeb128FramedReader<R>,
) -> Result<Option<(usize, WincodeArchiveV2NoRegistryBlock)>> {
    reader.read_bytes_with_limit(LIVE_FINALIZER_MAX_FRAME_SIZE, |bytes| {
        match wincode::config::deserialize::<WincodeArchiveV2NoRegistryBlock, _>(
            bytes,
            wincode_leb128_config(),
        ) {
            Ok(block) => Ok(block),
            Err(current_err) => {
                let legacy: LiveNoRegistryBlockLegacy = wincode::config::deserialize(
                    bytes,
                    wincode_leb128_config(),
                )
                .with_context(|| {
                    format!("decode live no-registry block failed; current error: {current_err}")
                })?;
                Ok(legacy.into_current())
            }
        }
    })
}

fn read_live_no_registry_block_signature_counts<R: Read>(
    reader: &mut WincodeLeb128FramedReader<R>,
) -> Result<Option<(usize, LiveNoRegistryBlockSignatureCounts)>> {
    reader.read_bytes_with_limit(LIVE_FINALIZER_MAX_FRAME_SIZE, |bytes| {
        match wincode::config::deserialize::<LiveNoRegistryBlockSignatureCounts, _>(
            bytes,
            wincode_leb128_config(),
        ) {
            Ok(block) => Ok(block),
            Err(current_err) => {
                let legacy: LiveNoRegistryBlockLegacy = wincode::config::deserialize(
                    bytes,
                    wincode_leb128_config(),
                )
                .with_context(|| {
                    format!(
                        "decode live no-registry signature-count block failed; current error: {current_err}"
                    )
                })?;
                live_signature_counts_from_full_block(legacy.into_current())
            }
        }
    })
}

enum LiveFinalizerBlock {
    Full(WincodeArchiveV2NoRegistryBlock),
    SignatureCounts(LiveNoRegistryBlockSignatureCounts),
}

impl LiveFinalizerBlock {
    fn slot(&self) -> u64 {
        match self {
            Self::Full(block) => block.header.compact.slot,
            Self::SignatureCounts(block) => block.header.compact.slot,
        }
    }

    fn blockhash_id(&self) -> u32 {
        match self {
            Self::Full(block) => block.header.compact.blockhash,
            Self::SignatureCounts(block) => block.header.compact.blockhash,
        }
    }
}

#[derive(Debug, SchemaRead)]
struct LiveNoRegistryBlockSignatureCounts {
    header: WincodeArchiveV2NoRegistryBlockHeader,
    txs: Vec<LiveNoRegistryTransactionSignatureCounts>,
}

#[derive(Debug, SchemaRead)]
struct LiveNoRegistryTransactionSignatureCounts {
    tx_index: u32,
    tx: WincodeArchiveV2Payload<LiveNoRegistryTxSignatureCounts>,
    metadata: Option<WincodeArchiveV2Payload<WincodeArchiveV2NoRegistryMeta>>,
}

#[derive(Debug, SchemaRead)]
struct LiveNoRegistryTxSignatureCounts {
    signatures: LiveSignatureCount,
    message: WincodeArchiveV2NoRegistryMessage,
}

#[derive(Debug, Clone, Copy)]
struct LiveSignatureCount {
    count: u8,
}

unsafe impl<'de, C: Config> SchemaRead<'de, C> for LiveSignatureCount {
    type Dst = Self;

    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let len = <C::LengthEncoding as SeqLen<C>>::read(reader.by_ref())?;
        let count = u8::try_from(len)
            .map_err(|_| wincode::ReadError::InvalidValue("signature count exceeds u8::MAX"))?;
        for _ in 0..len {
            let signature_len = <C::LengthEncoding as SeqLen<C>>::read(reader.by_ref())?;
            reader.take_scoped(signature_len)?;
        }
        dst.write(Self { count });
        Ok(())
    }
}

fn live_signature_counts_from_full_block(
    block: WincodeArchiveV2NoRegistryBlock,
) -> Result<LiveNoRegistryBlockSignatureCounts> {
    Ok(LiveNoRegistryBlockSignatureCounts {
        header: block.header,
        txs: block
            .txs
            .into_iter()
            .map(live_signature_count_transaction_from_full)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn live_signature_count_transaction_from_full(
    transaction: WincodeArchiveV2NoRegistryTransaction,
) -> Result<LiveNoRegistryTransactionSignatureCounts> {
    Ok(LiveNoRegistryTransactionSignatureCounts {
        tx_index: transaction.tx_index,
        tx: map_live_signature_count_payload(transaction.tx)?,
        metadata: transaction.metadata,
    })
}

fn map_live_signature_count_payload(
    payload: WincodeArchiveV2Payload<WincodeArchiveV2NoRegistryTx>,
) -> Result<WincodeArchiveV2Payload<LiveNoRegistryTxSignatureCounts>> {
    Ok(match payload {
        WincodeArchiveV2Payload::Decoded { source_len, value } => {
            WincodeArchiveV2Payload::Decoded {
                source_len,
                value: LiveNoRegistryTxSignatureCounts {
                    signatures: LiveSignatureCount {
                        count: u8::try_from(value.signatures.len())
                            .context("signature count exceeds u8::MAX")?,
                    },
                    message: value.message,
                },
            }
        }
        WincodeArchiveV2Payload::Raw { bytes, error } => {
            WincodeArchiveV2Payload::Raw { bytes, error }
        }
    })
}

#[derive(Debug, SchemaRead)]
struct LiveNoRegistryBlockLegacy {
    header: WincodeArchiveV2NoRegistryBlockHeader,
    txs: Vec<LiveNoRegistryTransactionLegacy>,
}

#[derive(Debug, SchemaRead)]
struct LiveNoRegistryTransactionLegacy {
    tx_index: u32,
    tx: WincodeArchiveV2Payload<WincodeArchiveV2NoRegistryTx>,
    metadata: Option<WincodeArchiveV2Payload<LiveNoRegistryMetaLegacy>>,
}

#[derive(Debug, SchemaRead)]
struct LiveNoRegistryMetaLegacy {
    err: Option<CompactTransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<CompactInnerInstructions>>,
    logs: Option<Vec<String>>,
    pre_token_balances: Vec<WincodeArchiveV2NoRegistryTokenBalance>,
    post_token_balances: Vec<WincodeArchiveV2NoRegistryTokenBalance>,
    rewards: Vec<WincodeArchiveV2NoRegistryReward>,
    loaded_writable_addresses: Vec<[u8; 32]>,
    loaded_readonly_addresses: Vec<[u8; 32]>,
    return_data: Option<WincodeArchiveV2NoRegistryReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

impl LiveNoRegistryBlockLegacy {
    fn into_current(self) -> WincodeArchiveV2NoRegistryBlock {
        WincodeArchiveV2NoRegistryBlock {
            header: self.header,
            txs: self
                .txs
                .into_iter()
                .map(LiveNoRegistryTransactionLegacy::into_current)
                .collect(),
        }
    }
}

impl LiveNoRegistryTransactionLegacy {
    fn into_current(self) -> WincodeArchiveV2NoRegistryTransaction {
        WincodeArchiveV2NoRegistryTransaction {
            tx_index: self.tx_index,
            tx: self.tx,
            metadata: self.metadata.map(|metadata| match metadata {
                WincodeArchiveV2Payload::Decoded { source_len, value } => {
                    WincodeArchiveV2Payload::Decoded {
                        source_len,
                        value: value.into_current(),
                    }
                }
                WincodeArchiveV2Payload::Raw { bytes, error } => {
                    WincodeArchiveV2Payload::Raw { bytes, error }
                }
            }),
        }
    }
}

impl LiveNoRegistryMetaLegacy {
    fn into_current(self) -> WincodeArchiveV2NoRegistryMeta {
        WincodeArchiveV2NoRegistryMeta {
            err: self.err,
            fee: self.fee,
            pre_balances: self.pre_balances,
            post_balances: self.post_balances,
            inner_instructions: self.inner_instructions,
            logs: self.logs.map(WincodeArchiveV2NoRegistryLogs::Raw),
            pre_token_balances: self.pre_token_balances,
            post_token_balances: self.post_token_balances,
            rewards: self.rewards,
            loaded_writable_addresses: self.loaded_writable_addresses,
            loaded_readonly_addresses: self.loaded_readonly_addresses,
            return_data: self.return_data,
            compute_units_consumed: self.compute_units_consumed,
            cost_units: self.cost_units,
        }
    }
}

fn trim_hot_memory(
    _next_block_id: u32,
    block_bytes: &mut Vec<u8>,
    compressed_buf: &mut Vec<u8>,
    access_bytes: &mut Vec<u8>,
    access_signature_bytes: Option<&mut Vec<u8>>,
) {
    let mut released = release_hot_reusable_buffer(block_bytes);
    released |= release_hot_reusable_buffer(compressed_buf);
    released |= release_hot_reusable_buffer(access_bytes);
    if let Some(access_signature_bytes) = access_signature_bytes {
        released |= release_hot_reusable_buffer(access_signature_bytes);
    }

    if released {
        trim_process_heap();
    }
}

fn trim_hot_reusable_buffer(buf: &mut Vec<u8>) {
    let _ = release_hot_reusable_buffer(buf);
}

fn release_hot_reusable_buffer(buf: &mut Vec<u8>) -> bool {
    buf.clear();
    if buf.capacity() > HOT_REUSABLE_BUFFER_RETAIN_LIMIT {
        *buf = Vec::with_capacity(HOT_REUSABLE_BUFFER_RETAIN_LIMIT);
        true
    } else {
        false
    }
}

fn trim_hot_reusable_vec<T>(buf: &mut Vec<T>) {
    buf.clear();
    let retain_limit = HOT_REUSABLE_BUFFER_RETAIN_LIMIT / std::mem::size_of::<T>().max(1);
    if buf.capacity() > retain_limit {
        *buf = Vec::with_capacity(retain_limit);
    }
}

#[cfg(all(unix, target_env = "gnu"))]
fn trim_process_heap() {
    unsafe {
        let _ = malloc_trim(0);
    }
}

#[cfg(not(all(unix, target_env = "gnu")))]
fn trim_process_heap() {}

#[cfg(all(unix, target_env = "gnu"))]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

fn reuse_hot_registry_sidecars(
    registry_dir: &Path,
    registry_path: &Path,
    registry_counts_path: &Path,
    registry_index_path: &Path,
    blockhash_registry_path: &Path,
) -> Result<()> {
    let source_registry = registry_dir.join(REGISTRY_FILE);
    let source_counts = registry_dir.join(REGISTRY_COUNTS_FILE);
    let source_index = registry_dir.join(REGISTRY_INDEX_FILE);
    let source_blockhash = registry_dir.join(BLOCKHASH_REGISTRY_FILE);
    anyhow::ensure!(
        crate::file_nonempty(&source_registry)
            && crate::file_nonempty(&source_counts)
            && crate::file_nonempty(&source_blockhash),
        "registry dir {} must contain non-empty {}, {}, and {}",
        registry_dir.display(),
        REGISTRY_FILE,
        REGISTRY_COUNTS_FILE,
        BLOCKHASH_REGISTRY_FILE
    );
    link_or_copy_registry_sidecar(&source_registry, registry_path)?;
    link_or_copy_registry_sidecar(&source_counts, registry_counts_path)?;
    if crate::file_nonempty(&source_index) {
        link_or_copy_registry_sidecar(&source_index, registry_index_path)?;
    }
    link_or_copy_registry_sidecar(&source_blockhash, blockhash_registry_path)?;
    info!(
        "Reusing Archive V2 registry sidecars from {}",
        registry_dir.display()
    );
    Ok(())
}

fn link_or_copy_registry_sidecar(source: &Path, target: &Path) -> Result<()> {
    if source == target {
        return Ok(());
    }
    if crate::file_nonempty(target) {
        std::fs::remove_file(target).with_context(|| format!("remove {}", target.display()))?;
    }
    match std::fs::hard_link(source, target) {
        Ok(()) => Ok(()),
        Err(link_err) => {
            std::fs::copy(source, target)
                .with_context(|| format!("copy {} to {}", source.display(), target.display()))?;
            info!(
                "Copied registry sidecar {} to {} after hard-link failed: {}",
                source.display(),
                target.display(),
                link_err
            );
            Ok(())
        }
    }
}

fn copy_file_prefix(source: &Path, target: &Path, bytes: u64) -> Result<()> {
    if source == target {
        anyhow::ensure!(
            std::fs::metadata(source)
                .with_context(|| format!("stat {}", source.display()))?
                .len()
                == bytes,
            "cannot truncate live sidecar in place: {}",
            source.display()
        );
        return Ok(());
    }
    if target.exists() {
        std::fs::remove_file(target).with_context(|| format!("remove {}", target.display()))?;
    }
    let source_file = File::open(source).with_context(|| format!("open {}", source.display()))?;
    let target_file =
        File::create(target).with_context(|| format!("create {}", target.display()))?;
    let mut reader =
        BufReader::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, source_file).take(bytes);
    let mut writer = BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, target_file);
    let copied = std::io::copy(&mut reader, &mut writer)
        .with_context(|| format!("copy prefix {} to {}", source.display(), target.display()))?;
    anyhow::ensure!(
        copied == bytes,
        "live sidecar {} ended after {} bytes, expected at least {}",
        source.display(),
        copied,
        bytes
    );
    writer
        .flush()
        .with_context(|| format!("flush {}", target.display()))?;
    Ok(())
}

fn copy_live_poh_prefix(source: &Path, target: &Path, records: u64) -> Result<()> {
    if target.exists() {
        std::fs::remove_file(target).with_context(|| format!("remove {}", target.display()))?;
    }
    let source_file = File::open(source).with_context(|| format!("open {}", source.display()))?;
    let target_file =
        File::create(target).with_context(|| format!("create {}", target.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(
        LIVE_FINALIZER_IO_BUFFER_SIZE,
        source_file,
    ));
    let mut writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        LIVE_FINALIZER_IO_BUFFER_SIZE,
        target_file,
    ));
    let mut copied = 0u64;
    while copied < records {
        let Some((_len, record)) = reader.read::<WincodeArchiveV2PohRecord>()? else {
            break;
        };
        writer.write(&record)?;
        copied += 1;
    }
    anyhow::ensure!(
        copied == records,
        "live PoH sidecar {} ended after {} records, expected at least {}",
        source.display(),
        copied,
        records
    );
    writer.flush()?;
    Ok(())
}

fn load_or_build_registry_key_index(
    registry_path: &Path,
    registry_index_path: &Path,
    resume: bool,
) -> Result<KeyIndex> {
    let expected_keys = registry_key_count(registry_path)?;
    if resume && crate::file_nonempty(registry_index_path) {
        let started = Instant::now();
        match KeyIndex::load(registry_index_path) {
            Ok(index) if index.len() == expected_keys => {
                info!(
                    "Loaded Archive V2 registry MPHF index in {:.2}s: keys={} path={}",
                    started.elapsed().as_secs_f64(),
                    index.len(),
                    registry_index_path.display()
                );
                return Ok(index);
            }
            Ok(index) => {
                warn!(
                    "Ignoring stale Archive V2 registry MPHF index: path={} keys={} expected={}",
                    registry_index_path.display(),
                    index.len(),
                    expected_keys
                );
            }
            Err(err) => {
                warn!(
                    "Ignoring unreadable Archive V2 registry MPHF index: path={} error={:#}",
                    registry_index_path.display(),
                    err
                );
            }
        }
    }

    let started = Instant::now();
    info!(
        "Building missing Archive V2 registry MPHF index in-process: {}",
        registry_index_path.display()
    );
    let registry = MappedRegistryKeys::open(registry_path)?;
    let index = KeyIndex::build_from_slice_low_memory(registry.keys());
    write_registry_key_index_atomic_checked(&index, registry_index_path, || {
        registry.ensure_unchanged()
    })?;
    info!(
        "Built Archive V2 registry MPHF index in {:.2}s: keys={} path={}",
        started.elapsed().as_secs_f64(),
        index.len(),
        registry_index_path.display()
    );
    Ok(index)
}

fn registry_key_count(registry_path: &Path) -> Result<usize> {
    let len = std::fs::metadata(registry_path)
        .with_context(|| format!("stat registry {}", registry_path.display()))?
        .len();
    anyhow::ensure!(
        len % 32 == 0,
        "invalid registry size {} (not multiple of 32): {}",
        len,
        registry_path.display()
    );
    let keys = len / 32;
    anyhow::ensure!(
        keys <= u64::from(u32::MAX),
        "registry key count {keys} exceeds compact ID space"
    );
    usize::try_from(keys).context("registry key count exceeds usize")
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryFileStamp {
    len: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl RegistryFileStamp {
    fn read(file: &File) -> Result<Self> {
        let metadata = file.metadata().context("stat open registry")?;
        Ok(Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
        })
    }
}

struct MappedRegistryKeys {
    path: PathBuf,
    file: File,
    mmap: Option<Mmap>,
    stamp: RegistryFileStamp,
}

impl MappedRegistryKeys {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("open registry for mmap {}", path.display()))?;
        let stamp = RegistryFileStamp::read(&file)
            .with_context(|| format!("stat registry before mmap {}", path.display()))?;
        anyhow::ensure!(
            stamp.len % 32 == 0,
            "invalid registry size {} (not multiple of 32): {}",
            stamp.len,
            path.display()
        );
        let key_count = stamp.len / 32;
        anyhow::ensure!(
            key_count <= u64::from(u32::MAX),
            "registry key count {key_count} exceeds compact ID space"
        );
        usize::try_from(key_count).context("registry key count exceeds usize")?;

        let mmap = if stamp.len == 0 {
            None
        } else {
            let len = usize::try_from(stamp.len).context("registry mmap length exceeds usize")?;
            // SAFETY: the mapping is read-only, `file` stays open for the life of
            // the mapping, and the pipeline treats a scan-complete registry as
            // immutable under its exclusive finalizer lock. We verify the open
            // file and its path identity again before publishing the index.
            Some(
                unsafe { MmapOptions::new().len(len).map(&file) }
                    .with_context(|| format!("mmap registry {}", path.display()))?,
            )
        };
        let mapped = Self {
            path: path.to_path_buf(),
            file,
            mmap,
            stamp,
        };
        mapped.ensure_unchanged()?;
        Ok(mapped)
    }

    fn keys(&self) -> &[[u8; 32]] {
        let bytes = self.mmap.as_deref().unwrap_or(&[]);
        debug_assert_eq!(bytes.len() % 32, 0);
        // SAFETY: `[u8; 32]` has alignment 1, the byte length was validated as
        // a multiple of 32, and the returned slice cannot outlive `self.mmap`.
        unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<[u8; 32]>(), bytes.len() / 32) }
    }

    fn ensure_unchanged(&self) -> Result<()> {
        let open_stamp = RegistryFileStamp::read(&self.file)
            .with_context(|| format!("restat open registry {}", self.path.display()))?;
        anyhow::ensure!(
            open_stamp == self.stamp,
            "registry changed while building MPHF: {}",
            self.path.display()
        );
        let current_file = File::open(&self.path)
            .with_context(|| format!("reopen registry after MPHF build {}", self.path.display()))?;
        let current_stamp = RegistryFileStamp::read(&current_file)
            .with_context(|| format!("restat registry path {}", self.path.display()))?;
        anyhow::ensure!(
            current_stamp == self.stamp,
            "registry path was replaced while building MPHF: {}",
            self.path.display()
        );
        Ok(())
    }
}

fn write_registry_key_index_atomic(index: &KeyIndex, path: &Path) -> Result<()> {
    write_registry_key_index_atomic_checked(index, path, || Ok(()))
}

fn write_registry_key_index_atomic_checked<F>(
    index: &KeyIndex,
    path: &Path,
    validate_before_publish: F,
) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    let tmp_path = path.with_extension("mphf.tmp");
    if crate::file_nonempty(&tmp_path) {
        std::fs::remove_file(&tmp_path)
            .with_context(|| format!("remove {}", tmp_path.display()))?;
    }
    index.write(&tmp_path)?;
    File::open(&tmp_path)
        .with_context(|| format!("open {} for sync", tmp_path.display()))?
        .sync_all()
        .with_context(|| format!("sync {}", tmp_path.display()))?;
    validate_before_publish()?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .with_context(|| format!("open {} for sync", parent.display()))?
        .sync_all()
        .with_context(|| format!("sync {}", parent.display()))?;
    Ok(())
}

fn write_hot_meta<W: Write>(
    writer: &mut WincodeLeb128FramedWriter<W>,
    record: &ArchiveV2HotMetaRecord,
) -> Result<()> {
    let bytes = wincode::config::serialize(record, wincode_leb128_config())?;
    writer.write_bytes(&bytes)
}

#[derive(Default)]
struct VoteHashRegistryBuilder {
    rows: Vec<VoteHashRegistryRow>,
    compact_vote_ix: u64,
    bank_hash_refs: u64,
    bank_hash_raw: u64,
    bank_hash_conflict_raw: u64,
    block_id_refs: u64,
    block_id_raw: u64,
    block_id_zero: u64,
    block_id_conflict_raw: u64,
}

#[derive(Clone, Copy, Default)]
struct VoteHashRegistryRow {
    bank_hash: Option<[u8; 32]>,
    block_id_hash: Option<[u8; 32]>,
}

impl VoteHashRegistryBuilder {
    fn ensure_block(&mut self, block_id: u32) {
        let needed = block_id as usize + 1;
        if self.rows.len() < needed {
            self.rows.resize(needed, VoteHashRegistryRow::default());
        }
    }

    fn ref_bank_hash(
        &mut self,
        slot: Option<u64>,
        hash: [u8; 32],
        slot_to_block_id: &GxHashMap<u64, u32>,
    ) -> Result<ArchiveV2VoteHashRef> {
        if hash == [0u8; 32] {
            return Ok(ArchiveV2VoteHashRef::Zero);
        }
        let Some(slot) = slot else {
            self.bank_hash_raw += 1;
            return Ok(ArchiveV2VoteHashRef::Raw(hash));
        };
        let Some(&block_id) = slot_to_block_id.get(&slot) else {
            self.bank_hash_raw += 1;
            return Ok(ArchiveV2VoteHashRef::Raw(hash));
        };
        self.ensure_block(block_id);
        let row = &mut self.rows[block_id as usize];
        if let Some(existing) = row.bank_hash {
            if existing != hash {
                self.bank_hash_raw += 1;
                self.bank_hash_conflict_raw += 1;
                return Ok(ArchiveV2VoteHashRef::Raw(hash));
            }
        } else {
            row.bank_hash = Some(hash);
        }
        self.bank_hash_refs += 1;
        Ok(ArchiveV2VoteHashRef::Block(block_id))
    }

    fn ref_block_id_hash(
        &mut self,
        slot: Option<u64>,
        hash: [u8; 32],
        slot_to_block_id: &GxHashMap<u64, u32>,
    ) -> Result<ArchiveV2VoteHashRef> {
        if hash == [0u8; 32] {
            self.block_id_zero += 1;
            return Ok(ArchiveV2VoteHashRef::Zero);
        }
        let Some(slot) = slot else {
            self.block_id_raw += 1;
            return Ok(ArchiveV2VoteHashRef::Raw(hash));
        };
        let Some(&block_id) = slot_to_block_id.get(&slot) else {
            self.block_id_raw += 1;
            return Ok(ArchiveV2VoteHashRef::Raw(hash));
        };
        self.ensure_block(block_id);
        let row = &mut self.rows[block_id as usize];
        if let Some(existing) = row.block_id_hash {
            if existing != hash {
                self.block_id_raw += 1;
                self.block_id_conflict_raw += 1;
                return Ok(ArchiveV2VoteHashRef::Raw(hash));
            }
        } else {
            row.block_id_hash = Some(hash);
        }
        self.block_id_refs += 1;
        Ok(ArchiveV2VoteHashRef::Block(block_id))
    }

    fn ref_aux_hash(&self, hash: [u8; 32]) -> ArchiveV2VoteHashRef {
        if hash == [0u8; 32] {
            ArchiveV2VoteHashRef::Zero
        } else {
            ArchiveV2VoteHashRef::Raw(hash)
        }
    }

    fn write(&self, path: &Path) -> Result<()> {
        let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
        let mut writer = BufWriter::with_capacity(LIVE_FINALIZER_IO_BUFFER_SIZE, file);
        for row in &self.rows {
            let flags =
                u8::from(row.bank_hash.is_some()) | (u8::from(row.block_id_hash.is_some()) << 1);
            writer.write_all(&[flags])?;
            writer.write_all(&row.bank_hash.unwrap_or([0u8; 32]))?;
            writer.write_all(&row.block_id_hash.unwrap_or([0u8; 32]))?;
        }
        writer
            .flush()
            .with_context(|| format!("flush {}", path.display()))?;
        Ok(())
    }
}

fn load_vote_hash_registry(path: &Path) -> Result<Vec<VoteHashRegistryRow>> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(
        bytes.len() % 65 == 0,
        "vote hash registry {} has invalid byte length {}",
        path.display(),
        bytes.len()
    );
    let mut rows = Vec::with_capacity(bytes.len() / 65);
    for chunk in bytes.chunks_exact(65) {
        let flags = chunk[0];
        let mut bank_hash = [0u8; 32];
        bank_hash.copy_from_slice(&chunk[1..33]);
        let mut block_id_hash = [0u8; 32];
        block_id_hash.copy_from_slice(&chunk[33..65]);
        rows.push(VoteHashRegistryRow {
            bank_hash: (flags & 1 != 0).then_some(bank_hash),
            block_id_hash: (flags & 2 != 0).then_some(block_id_hash),
        });
    }
    Ok(rows)
}

#[derive(Default)]
struct HotBlockBuffers {
    tx_rows: Vec<ArchiveV2HotTxRow>,
    message_bytes: Vec<u8>,
    metadata_bytes: Vec<u8>,
    hot_instructions: Vec<ArchiveV2HotInstruction>,
}

impl HotBlockBuffers {
    fn take(&mut self) -> (Vec<ArchiveV2HotTxRow>, Vec<u8>, Vec<u8>) {
        (
            std::mem::take(&mut self.tx_rows),
            std::mem::take(&mut self.message_bytes),
            std::mem::take(&mut self.metadata_bytes),
        )
    }

    fn recycle(&mut self, mut block: ArchiveV2HotBlockBlob) {
        block.tx_rows.clear();
        block.message_bytes.clear();
        block.metadata_bytes.clear();
        self.tx_rows = block.tx_rows;
        self.message_bytes = block.message_bytes;
        self.metadata_bytes = block.metadata_bytes;
    }

    fn recycle_message_instructions(&mut self, message: ArchiveV2HotMessagePayload) {
        debug_assert!(self.hot_instructions.is_empty());
        self.hot_instructions = match message {
            ArchiveV2HotMessagePayload::Legacy(message) => message.instructions,
            ArchiveV2HotMessagePayload::V0(message) => message.instructions,
        };
        self.hot_instructions.clear();
    }

    fn trim(&mut self) {
        trim_hot_reusable_buffer(&mut self.message_bytes);
        trim_hot_reusable_buffer(&mut self.metadata_bytes);
        trim_hot_reusable_vec(&mut self.tx_rows);
        trim_hot_reusable_vec(&mut self.hot_instructions);
    }
}

#[inline]
fn reserve_total_capacity<T>(values: &mut Vec<T>, desired_len: usize) {
    values.reserve(desired_len.saturating_sub(values.len()));
}

fn hot_block_from_archive_block(
    block: WincodeArchiveV2Block,
    known_program_ids: &KnownProgramIds,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
    signatures_writer: Option<&mut dyn Write>,
    block_signature_bytes: Option<&mut Vec<u8>>,
    buffers: &mut HotBlockBuffers,
    timings: &mut ArchiveV2Timings,
) -> Result<(ArchiveV2HotBlockBlob, u32)> {
    hot_block_from_archive_block_with_signatures(
        block,
        HotBlockSignatureSource::Owned,
        known_program_ids,
        slot_to_block_id,
        vote_hashes,
        signatures_writer,
        block_signature_bytes,
        buffers,
        timings,
    )
}

fn hot_block_from_first_seen_archive_block(
    block: WincodeArchiveV2Block,
    signatures: &FirstSeenBlockSignatures,
    known_program_ids: &KnownProgramIds,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
    signatures_writer: Option<&mut dyn Write>,
    buffers: &mut HotBlockBuffers,
    timings: &mut ArchiveV2Timings,
) -> Result<(ArchiveV2HotBlockBlob, u32)> {
    hot_block_from_archive_block_with_signatures(
        block,
        HotBlockSignatureSource::FirstSeen {
            counts: &signatures.counts,
            bytes: &signatures.bytes,
        },
        known_program_ids,
        slot_to_block_id,
        vote_hashes,
        signatures_writer,
        None,
        buffers,
        timings,
    )
}

#[derive(Clone, Copy)]
enum HotBlockSignatureSource<'a> {
    Owned,
    FirstSeen { counts: &'a [u8], bytes: &'a [u8] },
}

#[allow(clippy::too_many_arguments)]
fn hot_block_from_archive_block_with_signatures(
    block: WincodeArchiveV2Block,
    signature_source: HotBlockSignatureSource<'_>,
    known_program_ids: &KnownProgramIds,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
    mut signatures_writer: Option<&mut dyn Write>,
    mut block_signature_bytes: Option<&mut Vec<u8>>,
    buffers: &mut HotBlockBuffers,
    timings: &mut ArchiveV2Timings,
) -> Result<(ArchiveV2HotBlockBlob, u32)> {
    let compact = block.header.compact;
    let rewards = match block.header.rewards {
        Some(rewards) => Some(hot_rewards_from_archive(rewards)?),
        None => None,
    };
    let header = ArchiveV2HotBlockHeader {
        slot: compact.slot,
        parent_slot: compact.parent_slot,
        blockhash_id: compact.blockhash,
        previous_blockhash_id: compact.previous_blockhash,
        block_time: compact.block_time,
        block_height: compact.block_height,
        rewards,
    };

    if let HotBlockSignatureSource::FirstSeen { counts, bytes } = signature_source {
        anyhow::ensure!(
            counts.len() == block.txs.len(),
            "slot {} collected {} signature counts for {} hot transactions",
            header.slot,
            counts.len(),
            block.txs.len(),
        );
        let signature_count = counts.iter().try_fold(0usize, |total, count| {
            total
                .checked_add(usize::from(*count))
                .context("first-seen block signature count overflow")
        })?;
        let expected_bytes = signature_count
            .checked_mul(FIRST_SEEN_SIGNATURE_BYTES)
            .context("first-seen block signature byte length overflow")?;
        anyhow::ensure!(
            bytes.len() == expected_bytes,
            "slot {} collected {} signature bytes, expected {} for {} signatures",
            header.slot,
            bytes.len(),
            expected_bytes,
            signature_count,
        );
    }

    let (mut tx_rows, mut message_bytes, mut metadata_bytes) = buffers.take();
    reserve_total_capacity(&mut tx_rows, block.txs.len());
    let mut block_signature_count = 0u32;
    for (tx_position, transaction) in block.txs.into_iter().enumerate() {
        let WincodeArchiveV2Transaction {
            tx_index,
            tx,
            metadata,
        } = transaction;
        let WincodeArchiveV2Payload::Decoded { value: tx, .. } = tx else {
            anyhow::bail!(
                "hot block writer requires decoded transactions; raw transaction at tx_index {}",
                tx_index
            );
        };
        let OwnedCompactTransaction {
            signatures,
            message,
        } = tx;
        let message_offset = u32::try_from(message_bytes.len())
            .context("hot message byte region exceeds u32::MAX")?;
        let message_build_started = timings.detail_timer();
        let (message, mut flags) = hot_message_from_owned_with_instruction_scratch(
            message,
            &mut buffers.hot_instructions,
            known_program_ids,
            slot_to_block_id,
            vote_hashes,
        )?;
        timings.hot_message_build += message_build_started.elapsed();
        let message_encode_started = timings.detail_timer();
        let before_message_len = message_bytes.len();
        let message_encode_result =
            wincode::config::serialize_into(&mut message_bytes, &message, wincode_leb128_config());
        buffers.recycle_message_instructions(message);
        message_encode_result?;
        let message_len = u32::try_from(message_bytes.len() - before_message_len)
            .context("hot message payload exceeds u32::MAX")?;
        timings.hot_message_encode += message_encode_started.elapsed();

        let signature_count = match signature_source {
            HotBlockSignatureSource::Owned => {
                let signature_count =
                    u8::try_from(signatures.len()).context("signature count exceeds u8::MAX")?;
                let signature_write_started = timings.detail_timer();
                for (signature_index, signature) in signatures.iter().enumerate() {
                    anyhow::ensure!(
                        signature.len() == FIRST_SEEN_SIGNATURE_BYTES,
                        "tx_index {} signature#{} is {} bytes, expected {}",
                        tx_index,
                        signature_index,
                        signature.len(),
                        FIRST_SEEN_SIGNATURE_BYTES,
                    );
                    if let Some(writer) = signatures_writer.as_deref_mut() {
                        writer.write_all(signature)?;
                    }
                    if let Some(bytes) = block_signature_bytes.as_deref_mut() {
                        bytes.extend_from_slice(signature);
                    }
                }
                timings.hot_signature_write += signature_write_started.elapsed();
                signature_count
            }
            HotBlockSignatureSource::FirstSeen { counts, .. } => {
                anyhow::ensure!(
                    signatures.is_empty(),
                    "slot {} tx#{} retained {} owned signatures in first-seen mode",
                    header.slot,
                    tx_position,
                    signatures.len(),
                );
                counts[tx_position]
            }
        };
        block_signature_count = block_signature_count
            .checked_add(u32::from(signature_count))
            .context("block signature count overflow")?;

        let metadata_offset = u32::try_from(metadata_bytes.len())
            .context("hot metadata byte region exceeds u32::MAX")?;
        let mut metadata_len = 0u32;
        if let Some(metadata) = metadata {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_METADATA;
            let metadata_encode_started = timings.detail_timer();
            match metadata {
                WincodeArchiveV2Payload::Decoded { value, .. } => {
                    flags |= hot_metadata_flags(&value);
                    let before_metadata_len = metadata_bytes.len();
                    wincode::config::serialize_into(
                        &mut metadata_bytes,
                        &value,
                        wincode_leb128_config(),
                    )?;
                    metadata_len = u32::try_from(metadata_bytes.len() - before_metadata_len)
                        .context("hot metadata payload exceeds u32::MAX")?;
                }
                WincodeArchiveV2Payload::Raw { bytes, .. } => {
                    flags |= ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK;
                    metadata_len = u32::try_from(bytes.len())
                        .context("hot raw metadata payload exceeds u32::MAX")?;
                    metadata_bytes.extend_from_slice(&bytes);
                }
            }
            timings.hot_metadata_encode += metadata_encode_started.elapsed();
        }

        tx_rows.push(ArchiveV2HotTxRow {
            tx_index,
            flags,
            message_offset,
            message_len,
            metadata_offset,
            metadata_len,
            signature_count,
            reserved: [0; 3],
        });
    }

    if let HotBlockSignatureSource::FirstSeen { bytes, .. } = signature_source {
        let signature_write_started = timings.detail_timer();
        if !bytes.is_empty() {
            if let Some(writer) = signatures_writer.as_deref_mut() {
                writer.write_all(bytes)?;
            }
            if let Some(block_bytes) = block_signature_bytes.as_deref_mut() {
                block_bytes.extend_from_slice(bytes);
            }
        }
        timings.hot_signature_write += signature_write_started.elapsed();
    }

    let tx_count = u32::try_from(tx_rows.len()).context("hot block tx count exceeds u32::MAX")?;
    Ok((
        ArchiveV2HotBlockBlob {
            header,
            tx_count,
            tx_rows,
            message_bytes,
            metadata_bytes,
        },
        block_signature_count,
    ))
}

fn hot_block_from_live_no_registry_block(
    block: WincodeArchiveV2NoRegistryBlock,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    block_id: u32,
    nonce_recent_blockhashes: &mut u64,
    known_program_ids: &KnownProgramIds,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
    mut signatures_writer: Option<&mut dyn Write>,
    mut block_signature_bytes: Option<&mut Vec<u8>>,
    buffers: &mut HotBlockBuffers,
    timings: &mut ArchiveV2Timings,
) -> Result<(ArchiveV2HotBlockBlob, u32)> {
    let WincodeArchiveV2NoRegistryBlock { header, txs } = block;
    let slot = header.compact.slot;
    let rewards_decode_started = timings.detail_timer();
    let rewards = header
        .rewards
        .as_ref()
        .map(|rewards| {
            optimize_no_registry_rewards(rewards, key_index).and_then(hot_rewards_from_archive)
        })
        .transpose()?;
    timings.rewards_decode_compact += rewards_decode_started.elapsed();

    let mut compact = header.compact;
    compact.blockhash = block_id;
    compact.previous_blockhash = block_id.saturating_sub(1);
    let header = ArchiveV2HotBlockHeader {
        slot: compact.slot,
        parent_slot: compact.parent_slot,
        blockhash_id: compact.blockhash,
        previous_blockhash_id: compact.previous_blockhash,
        block_time: compact.block_time,
        block_height: compact.block_height,
        rewards,
    };

    vote_hashes.ensure_block(block_id);
    let tx_capacity = txs.len();
    let (mut tx_rows, mut message_bytes, mut metadata_bytes) = buffers.take();
    tx_rows.clear();
    message_bytes.clear();
    metadata_bytes.clear();
    reserve_total_capacity(&mut tx_rows, tx_capacity);

    let mut block_signature_count = 0u32;
    for (tx_position, transaction) in txs.into_iter().enumerate() {
        let WincodeArchiveV2NoRegistryTransaction {
            tx_index,
            tx,
            metadata,
        } = transaction;
        let tx_decode_started = timings.detail_timer();
        let WincodeArchiveV2Payload::Decoded { value: tx, .. } = tx else {
            bail!("slot {slot} tx#{tx_position} raw transaction payload");
        };
        let tx = optimize_no_registry_tx(
            slot,
            tx_position,
            tx,
            key_index,
            rolling_blockhashes,
            nonce_recent_blockhashes,
        )?;
        timings.tx_decode_compact += tx_decode_started.elapsed();

        let metadata_decode_started = timings.detail_timer();
        let metadata = metadata
            .map(|payload| -> Result<CompactMetaV1> {
                match payload {
                    WincodeArchiveV2Payload::Decoded { value, .. } => optimize_no_registry_meta(
                        slot,
                        tx_position,
                        value,
                        key_index,
                        Some(timings),
                    ),
                    WincodeArchiveV2Payload::Raw { error, .. } => {
                        bail!("slot {slot} tx#{tx_position} raw metadata payload: {error}");
                    }
                }
            })
            .transpose()?;
        timings.metadata_decode_compact += metadata_decode_started.elapsed();

        let message_offset = u32::try_from(message_bytes.len())
            .context("hot message byte region exceeds u32::MAX")?;
        let message_build_started = timings.detail_timer();
        let (message, mut flags) =
            hot_message_from_owned(tx.message, known_program_ids, slot_to_block_id, vote_hashes)?;
        timings.hot_message_build += message_build_started.elapsed();
        let message_encode_started = timings.detail_timer();
        let before_message_len = message_bytes.len();
        wincode::config::serialize_into(&mut message_bytes, &message, wincode_leb128_config())?;
        let message_len = u32::try_from(message_bytes.len() - before_message_len)
            .context("hot message payload exceeds u32::MAX")?;
        timings.hot_message_encode += message_encode_started.elapsed();

        let signature_count =
            u8::try_from(tx.signatures.len()).context("signature count exceeds u8::MAX")?;
        let signature_write_started = timings.detail_timer();
        if signatures_writer.is_some() || block_signature_bytes.is_some() {
            for (signature_index, signature) in tx.signatures.iter().enumerate() {
                anyhow::ensure!(
                    signature.len() == 64,
                    "tx_index {} signature#{} is {} bytes, expected 64",
                    tx_index,
                    signature_index,
                    signature.len()
                );
                if let Some(writer) = signatures_writer.as_deref_mut() {
                    writer.write_all(signature)?;
                }
                if let Some(bytes) = block_signature_bytes.as_deref_mut() {
                    bytes.extend_from_slice(signature);
                }
            }
        }
        timings.hot_signature_write += signature_write_started.elapsed();
        block_signature_count = block_signature_count
            .checked_add(u32::from(signature_count))
            .context("block signature count overflow")?;

        let metadata_offset = u32::try_from(metadata_bytes.len())
            .context("hot metadata byte region exceeds u32::MAX")?;
        let mut metadata_len = 0u32;
        if let Some(metadata) = metadata {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_METADATA;
            flags |= hot_metadata_flags(&metadata);
            let metadata_encode_started = timings.detail_timer();
            let before_metadata_len = metadata_bytes.len();
            wincode::config::serialize_into(
                &mut metadata_bytes,
                &metadata,
                wincode_leb128_config(),
            )?;
            metadata_len = u32::try_from(metadata_bytes.len() - before_metadata_len)
                .context("hot metadata payload exceeds u32::MAX")?;
            timings.hot_metadata_encode += metadata_encode_started.elapsed();
        }

        tx_rows.push(ArchiveV2HotTxRow {
            tx_index,
            flags,
            message_offset,
            message_len,
            metadata_offset,
            metadata_len,
            signature_count,
            reserved: [0; 3],
        });
    }

    let tx_count = u32::try_from(tx_rows.len()).context("hot block tx count exceeds u32::MAX")?;
    Ok((
        ArchiveV2HotBlockBlob {
            header,
            tx_count,
            tx_rows,
            message_bytes,
            metadata_bytes,
        },
        block_signature_count,
    ))
}

fn hot_block_from_live_finalizer_block(
    block: LiveFinalizerBlock,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    block_id: u32,
    nonce_recent_blockhashes: &mut u64,
    known_program_ids: &KnownProgramIds,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
    signatures_writer: Option<&mut dyn Write>,
    block_signature_bytes: Option<&mut Vec<u8>>,
    buffers: &mut HotBlockBuffers,
    timings: &mut ArchiveV2Timings,
) -> Result<(ArchiveV2HotBlockBlob, u32)> {
    match block {
        LiveFinalizerBlock::Full(block) => hot_block_from_live_no_registry_block(
            block,
            key_index,
            rolling_blockhashes,
            block_id,
            nonce_recent_blockhashes,
            known_program_ids,
            slot_to_block_id,
            vote_hashes,
            signatures_writer,
            block_signature_bytes,
            buffers,
            timings,
        ),
        LiveFinalizerBlock::SignatureCounts(block) => {
            anyhow::ensure!(
                signatures_writer.is_none() && block_signature_bytes.is_none(),
                "signature-count live decode cannot rewrite signature sidecars"
            );
            hot_block_from_live_no_registry_block_signature_counts(
                block,
                key_index,
                rolling_blockhashes,
                block_id,
                nonce_recent_blockhashes,
                known_program_ids,
                slot_to_block_id,
                vote_hashes,
                buffers,
                timings,
            )
        }
    }
}

fn hot_block_from_live_no_registry_block_signature_counts(
    block: LiveNoRegistryBlockSignatureCounts,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    block_id: u32,
    nonce_recent_blockhashes: &mut u64,
    known_program_ids: &KnownProgramIds,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
    buffers: &mut HotBlockBuffers,
    timings: &mut ArchiveV2Timings,
) -> Result<(ArchiveV2HotBlockBlob, u32)> {
    let LiveNoRegistryBlockSignatureCounts { header, txs } = block;
    let slot = header.compact.slot;
    let rewards_decode_started = timings.detail_timer();
    let rewards = header
        .rewards
        .as_ref()
        .map(|rewards| {
            optimize_no_registry_rewards(rewards, key_index).and_then(hot_rewards_from_archive)
        })
        .transpose()?;
    timings.rewards_decode_compact += rewards_decode_started.elapsed();

    let mut compact = header.compact;
    compact.blockhash = block_id;
    compact.previous_blockhash = block_id.saturating_sub(1);
    let header = ArchiveV2HotBlockHeader {
        slot: compact.slot,
        parent_slot: compact.parent_slot,
        blockhash_id: compact.blockhash,
        previous_blockhash_id: compact.previous_blockhash,
        block_time: compact.block_time,
        block_height: compact.block_height,
        rewards,
    };

    vote_hashes.ensure_block(block_id);
    let tx_capacity = txs.len();
    let (mut tx_rows, mut message_bytes, mut metadata_bytes) = buffers.take();
    tx_rows.clear();
    message_bytes.clear();
    metadata_bytes.clear();
    reserve_total_capacity(&mut tx_rows, tx_capacity);

    let mut block_signature_count = 0u32;
    for (tx_position, transaction) in txs.into_iter().enumerate() {
        let LiveNoRegistryTransactionSignatureCounts {
            tx_index,
            tx,
            metadata,
        } = transaction;
        let tx_decode_started = timings.detail_timer();
        let WincodeArchiveV2Payload::Decoded { value: tx, .. } = tx else {
            bail!("slot {slot} tx#{tx_position} raw transaction payload");
        };
        let message = optimize_no_registry_message(
            slot,
            tx_position,
            tx.message,
            key_index,
            rolling_blockhashes,
            nonce_recent_blockhashes,
        )?;
        timings.tx_decode_compact += tx_decode_started.elapsed();

        let metadata_decode_started = timings.detail_timer();
        let metadata = metadata
            .map(|payload| -> Result<CompactMetaV1> {
                match payload {
                    WincodeArchiveV2Payload::Decoded { value, .. } => optimize_no_registry_meta(
                        slot,
                        tx_position,
                        value,
                        key_index,
                        Some(timings),
                    ),
                    WincodeArchiveV2Payload::Raw { error, .. } => {
                        bail!("slot {slot} tx#{tx_position} raw metadata payload: {error}");
                    }
                }
            })
            .transpose()?;
        timings.metadata_decode_compact += metadata_decode_started.elapsed();

        let message_offset = u32::try_from(message_bytes.len())
            .context("hot message byte region exceeds u32::MAX")?;
        let message_build_started = timings.detail_timer();
        let (message, mut flags) =
            hot_message_from_owned(message, known_program_ids, slot_to_block_id, vote_hashes)?;
        timings.hot_message_build += message_build_started.elapsed();
        let message_encode_started = timings.detail_timer();
        let before_message_len = message_bytes.len();
        wincode::config::serialize_into(&mut message_bytes, &message, wincode_leb128_config())?;
        let message_len = u32::try_from(message_bytes.len() - before_message_len)
            .context("hot message payload exceeds u32::MAX")?;
        timings.hot_message_encode += message_encode_started.elapsed();

        let signature_count = tx.signatures.count;
        block_signature_count = block_signature_count
            .checked_add(u32::from(signature_count))
            .context("block signature count overflow")?;

        let metadata_offset = u32::try_from(metadata_bytes.len())
            .context("hot metadata byte region exceeds u32::MAX")?;
        let mut metadata_len = 0u32;
        if let Some(metadata) = metadata {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_METADATA;
            flags |= hot_metadata_flags(&metadata);
            let metadata_encode_started = timings.detail_timer();
            let before_metadata_len = metadata_bytes.len();
            wincode::config::serialize_into(
                &mut metadata_bytes,
                &metadata,
                wincode_leb128_config(),
            )?;
            metadata_len = u32::try_from(metadata_bytes.len() - before_metadata_len)
                .context("hot metadata payload exceeds u32::MAX")?;
            timings.hot_metadata_encode += metadata_encode_started.elapsed();
        }

        tx_rows.push(ArchiveV2HotTxRow {
            tx_index,
            flags,
            message_offset,
            message_len,
            metadata_offset,
            metadata_len,
            signature_count,
            reserved: [0; 3],
        });
    }

    let tx_count = u32::try_from(tx_rows.len()).context("hot block tx count exceeds u32::MAX")?;
    Ok((
        ArchiveV2HotBlockBlob {
            header,
            tx_count,
            tx_rows,
            message_bytes,
            metadata_bytes,
        },
        block_signature_count,
    ))
}

fn normalize_first_seen_access_pubkeys(
    entries: &mut Vec<ArchiveV2BlockAccessPubkey>,
) -> Result<()> {
    entries.sort_unstable_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.pubkey.cmp(&right.pubkey))
    });
    let mut unique_len = 0usize;
    for index in 0..entries.len() {
        let entry = entries[index];
        if unique_len > 0 && entries[unique_len - 1].id == entry.id {
            anyhow::ensure!(
                entries[unique_len - 1].pubkey == entry.pubkey,
                "first-seen ID {} aliases two pubkeys within one block: existing={} incoming={}",
                entry.id,
                hex32(&entries[unique_len - 1].pubkey),
                hex32(&entry.pubkey),
            );
            continue;
        }
        entries[unique_len] = entry;
        unique_len += 1;
    }
    entries.truncate(unique_len);
    Ok(())
}

fn checked_archive_v2_block_access_frame_len(frame_bytes: usize, slot: u64) -> Result<u32> {
    let frame_bytes_u64 = u64::try_from(frame_bytes).context("block-access size exceeds u64")?;
    anyhow::ensure!(
        frame_bytes_u64 <= ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
        "slot {slot} block-access payload is {frame_bytes_u64} bytes, exceeding the shared {} byte limit",
        ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES
    );
    u32::try_from(frame_bytes).context("block-access payload exceeds u32::MAX")
}

fn build_archive_v2_block_access_blob(
    block: &ArchiveV2HotBlockBlob,
    registry_keys: &[[u8; 32]],
    blockhashes: &[[u8; 32]],
    previous_tail: &[PreviousBlockhash],
    block_signature_bytes: &[u8],
    vote_hash_rows: &[VoteHashRegistryRow],
) -> Result<ArchiveV2BlockAccessBlob> {
    build_archive_v2_block_access_blob_with_pubkey_resolver(
        block,
        |id| {
            registry_keys
                .get(
                    id.checked_sub(1)
                        .context("pubkey registry ID zero is reserved")?
                        as usize,
                )
                .copied()
                .with_context(|| format!("pubkey registry id {id} is outside loaded registry"))
        },
        blockhashes,
        previous_tail,
        block_signature_bytes,
        vote_hash_rows,
    )
}

fn build_archive_v2_block_access_blob_with_pubkey_resolver(
    block: &ArchiveV2HotBlockBlob,
    mut resolve_pubkey: impl FnMut(u32) -> Result<[u8; 32]>,
    blockhashes: &[[u8; 32]],
    previous_tail: &[PreviousBlockhash],
    block_signature_bytes: &[u8],
    vote_hash_rows: &[VoteHashRegistryRow],
) -> Result<ArchiveV2BlockAccessBlob> {
    let expected_signature_bytes = block.tx_rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.signature_count as usize * 64)
            .context("block signature bytes overflow")
    })?;
    anyhow::ensure!(
        expected_signature_bytes == block_signature_bytes.len(),
        "block access signature length mismatch for slot {}: rows={} bytes={}",
        block.header.slot,
        expected_signature_bytes,
        block_signature_bytes.len()
    );

    let blockhash_id = block.header.blockhash_id;
    let blockhash = resolve_access_blockhash_id(blockhash_id as i32, blockhashes, previous_tail)
        .with_context(|| {
            format!(
                "resolve blockhash id {} for slot {}",
                blockhash_id, block.header.slot
            )
        })?;
    let previous_blockhash_id = block.header.previous_blockhash_id;
    let previous_blockhash = resolve_access_previous_blockhash(block, blockhashes, previous_tail)
        .with_context(|| {
        format!(
            "resolve previous blockhash id {} for slot {}",
            previous_blockhash_id, block.header.slot
        )
    })?;

    let mut pubkey_ids = Vec::new();
    let mut blockhash_ids = Vec::new();
    let mut vote_hash_block_ids = Vec::new();
    collect_access_blockhash_id(block.header.blockhash_id as i32, &mut blockhash_ids);
    collect_access_blockhash_id(
        block.header.previous_blockhash_id as i32,
        &mut blockhash_ids,
    );
    if let Some(rewards) = &block.header.rewards {
        for reward in &rewards.decoded {
            collect_access_pubkey_id(reward.pubkey, &mut pubkey_ids);
        }
    }

    for row in &block.tx_rows {
        if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK == 0 {
            let message_slice =
                hot_block_access_region_slice(block, row, block.header.slot, "message")?;
            let message: ArchiveV2HotMessagePayload =
                wincode::config::deserialize(message_slice, wincode_leb128_config())
                    .with_context(|| format!("decode hot message tx_index={}", row.tx_index))?;
            collect_access_message_refs(&message, &mut pubkey_ids, &mut blockhash_ids);
            collect_access_message_vote_hash_refs(&message, &mut vote_hash_block_ids);
        }

        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0
            && row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK == 0
        {
            let metadata_slice =
                hot_block_access_region_slice(block, row, block.header.slot, "metadata")?;
            let metadata: CompactMetaV1 =
                wincode::config::deserialize(metadata_slice, wincode_leb128_config())
                    .with_context(|| format!("decode hot metadata tx_index={}", row.tx_index))?;
            collect_access_metadata_refs(&metadata, &mut pubkey_ids);
        }
    }

    pubkey_ids.sort_unstable();
    pubkey_ids.dedup();
    blockhash_ids.sort_unstable();
    blockhash_ids.dedup();
    vote_hash_block_ids.sort_unstable();
    vote_hash_block_ids.dedup();

    let pubkeys = pubkey_ids
        .into_iter()
        .map(|id| {
            let pubkey = resolve_pubkey(id)?;
            Ok(ArchiveV2BlockAccessPubkey { id, pubkey })
        })
        .collect::<Result<Vec<_>>>()?;
    let blockhashes = blockhash_ids
        .into_iter()
        .map(|id| {
            let blockhash = resolve_access_blockhash_id(id, blockhashes, previous_tail)
                .with_context(|| format!("blockhash id {id} is outside loaded registry"))?;
            Ok(ArchiveV2BlockAccessBlockhash { id, blockhash })
        })
        .collect::<Result<Vec<_>>>()?;
    let vote_hashes = vote_hash_block_ids
        .into_iter()
        .map(|block_id| {
            let row = vote_hash_rows
                .get(block_id as usize)
                .copied()
                .with_context(|| {
                    format!("vote hash registry row {block_id} is outside loaded registry")
                })?;
            Ok(ArchiveV2BlockAccessVoteHash {
                block_id,
                bank_hash: row.bank_hash,
                block_id_hash: row.block_id_hash,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ArchiveV2BlockAccessBlob {
        version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
        flags: 0,
        blockhash,
        previous_blockhash,
        signature_counts: block
            .tx_rows
            .iter()
            .map(|row| row.signature_count)
            .collect(),
        signatures: block_signature_bytes.to_vec(),
        pubkeys,
        blockhashes,
        vote_hashes,
    })
}

fn hot_block_access_region_slice<'a>(
    block: &'a ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
    slot: u64,
    region: &'static str,
) -> Result<&'a [u8]> {
    let (bytes, offset, len) = match region {
        "message" => (&block.message_bytes, row.message_offset, row.message_len),
        "metadata" => (&block.metadata_bytes, row.metadata_offset, row.metadata_len),
        _ => anyhow::bail!("unsupported hot block region {region}"),
    };
    let start = offset as usize;
    let len = len as usize;
    let end = start.checked_add(len).with_context(|| {
        format!(
            "{region} slice overflow slot={slot} tx_index={}",
            row.tx_index
        )
    })?;
    bytes.get(start..end).with_context(|| {
        format!(
            "{region} slice out of bounds slot={slot} tx_index={} offset={offset} len={len} region_len={}",
            row.tx_index,
            bytes.len()
        )
    })
}

fn collect_access_message_refs(
    message: &ArchiveV2HotMessagePayload,
    pubkey_ids: &mut Vec<u32>,
    blockhash_ids: &mut Vec<i32>,
) {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => {
            collect_access_pubkeys(message.account_keys.iter().copied(), pubkey_ids);
            collect_access_recent_blockhash_id(&message.recent_blockhash, blockhash_ids);
        }
        ArchiveV2HotMessagePayload::V0(message) => {
            collect_access_pubkeys(message.account_keys.iter().copied(), pubkey_ids);
            collect_access_recent_blockhash_id(&message.recent_blockhash, blockhash_ids);
            for lookup in &message.address_table_lookups {
                collect_access_pubkey_id(lookup.account_key, pubkey_ids);
            }
        }
    }
}

fn collect_access_message_vote_hash_refs(
    message: &ArchiveV2HotMessagePayload,
    vote_hash_block_ids: &mut Vec<u32>,
) {
    let instructions = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => message.instructions.as_slice(),
        ArchiveV2HotMessagePayload::V0(message) => message.instructions.as_slice(),
    };
    for instruction in instructions {
        collect_access_instruction_vote_hash_refs(&instruction.data, vote_hash_block_ids);
    }
}

fn collect_access_instruction_vote_hash_refs(
    data: &ArchiveV2HotInstructionData,
    vote_hash_block_ids: &mut Vec<u32>,
) {
    match data {
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update) => {
            collect_access_vote_hash_ref(update.hash, vote_hash_block_ids);
        }
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch { update, .. } => {
            collect_access_vote_hash_ref(update.hash, vote_hash_block_ids);
        }
        ArchiveV2HotInstructionData::VoteTowerSync(tower) => {
            collect_access_vote_hash_ref(tower.update.hash, vote_hash_block_ids);
            collect_access_vote_hash_ref(tower.block_id_hash, vote_hash_block_ids);
        }
        ArchiveV2HotInstructionData::VoteTowerSyncSwitch { tower, .. } => {
            collect_access_vote_hash_ref(tower.update.hash, vote_hash_block_ids);
            collect_access_vote_hash_ref(tower.block_id_hash, vote_hash_block_ids);
        }
        ArchiveV2HotInstructionData::Raw(_)
        | ArchiveV2HotInstructionData::UnknownSystem(_)
        | ArchiveV2HotInstructionData::UnknownVote(_)
        | ArchiveV2HotInstructionData::ComputeBudget(_)
        | ArchiveV2HotInstructionData::System(_) => {}
    }
}

fn collect_access_vote_hash_ref(value: ArchiveV2VoteHashRef, vote_hash_block_ids: &mut Vec<u32>) {
    if let ArchiveV2VoteHashRef::Block(block_id) = value {
        vote_hash_block_ids.push(block_id);
    }
}

fn collect_access_metadata_refs(metadata: &CompactMetaV1, pubkey_ids: &mut Vec<u32>) {
    collect_access_pubkeys(
        metadata
            .loaded_writable_addresses
            .iter()
            .chain(metadata.loaded_readonly_addresses.iter())
            .copied(),
        pubkey_ids,
    );
    for balance in metadata
        .pre_token_balances
        .iter()
        .chain(metadata.post_token_balances.iter())
    {
        if let Some(key) = balance.mint {
            collect_access_pubkey_id(key, pubkey_ids);
        }
        if let Some(key) = balance.owner {
            collect_access_pubkey_id(key, pubkey_ids);
        }
        if let Some(key) = balance.program_id {
            collect_access_pubkey_id(key, pubkey_ids);
        }
    }
    for reward in &metadata.rewards {
        collect_access_pubkey_id(reward.pubkey, pubkey_ids);
    }
    if let Some(return_data) = &metadata.return_data {
        collect_access_pubkey_id(return_data.program_id, pubkey_ids);
    }
    if let Some(logs) = &metadata.logs {
        collect_access_log_refs(logs, pubkey_ids);
    }
}

fn collect_access_pubkeys(keys: impl Iterator<Item = CompactPubkey>, pubkey_ids: &mut Vec<u32>) {
    for key in keys {
        collect_access_pubkey_id(key, pubkey_ids);
    }
}

fn collect_access_pubkey_id(key: CompactPubkey, pubkey_ids: &mut Vec<u32>) {
    if let CompactPubkey::Id(id) = key {
        pubkey_ids.push(id);
    }
}

fn collect_access_log_refs(logs: &CompactLogStream, pubkey_ids: &mut Vec<u32>) {
    for event in &logs.events {
        match event {
            LogEvent::LoaderUpgradedProgram { program }
            | LogEvent::BpfInvoke { program }
            | LogEvent::Consumed { program, .. }
            | LogEvent::Success { program }
            | LogEvent::BpfSuccess { program }
            | LogEvent::Failure { program, .. }
            | LogEvent::BpfFailure { program, .. }
            | LogEvent::FailureCustomProgramError { program, .. }
            | LogEvent::BpfFailureCustomProgramError { program, .. }
            | LogEvent::FailureInvalidAccountData { program }
            | LogEvent::BpfFailureInvalidAccountData { program }
            | LogEvent::FailureInvalidProgramArgument { program }
            | LogEvent::BpfFailureInvalidProgramArgument { program }
            | LogEvent::Return { program, .. }
            | LogEvent::Invoke { program, .. } => collect_access_pubkey_id(*program, pubkey_ids),
            LogEvent::LoaderFinalizedAccount { account }
            | LogEvent::RuntimeWritablePrivilegeEscalated { account }
            | LogEvent::RuntimeSignerPrivilegeEscalated { account }
            | LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => {
                collect_access_pubkey_id(*account, pubkey_ids)
            }
            LogEvent::ProgramIdLog { program, log } => {
                collect_access_pubkey_id(*program, pubkey_ids);
                collect_access_program_log_refs(log, pubkey_ids);
            }
            LogEvent::ProgramLog(log) | LogEvent::ProgramPlainLog(log) => {
                collect_access_program_log_refs(log, pubkey_ids);
            }
            LogEvent::ProgramNotDeployed { program } | LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    collect_access_pubkey_id(*program, pubkey_ids);
                }
            }
            LogEvent::System(log) => collect_access_system_log_refs(log, pubkey_ids),
            LogEvent::LogTruncated
            | LogEvent::StakeMergingAccounts
            | LogEvent::ProgramLogError { .. }
            | LogEvent::ProgramAccountNotWritable
            | LogEvent::ProgramIdMismatch
            | LogEvent::ProgramNotUpgradeable
            | LogEvent::ProgramAndProgramDataAccountMismatch
            | LogEvent::ProgramWasExtendedInThisBlockAlready
            | LogEvent::BpfConsumed { .. }
            | LogEvent::FailedToComplete { .. }
            | LogEvent::CustomProgramError { .. }
            | LogEvent::Data { .. }
            | LogEvent::Consumption { .. }
            | LogEvent::CbRequestUnits { .. }
            | LogEvent::UnknownProgram { .. }
            | LogEvent::UnknownAccount { .. }
            | LogEvent::VerifyEd25519
            | LogEvent::VerifySecp256k1
            | LogEvent::CloseContextState
            | LogEvent::Plain { .. }
            | LogEvent::Unparsed { .. } => {}
        }
    }
}

fn collect_access_program_log_refs(log: &ProgramLog, pubkey_ids: &mut Vec<u32>) {
    if let ProgramLog::Token2022(log) = log {
        match log {
            Token2022Log::ErrorHarvestingFrom { account_key, .. }
            | Token2022Log::ErrorHarvestingFrom2 { account_key, .. }
            | Token2022Log::ErrorHarvestingFrom3 { account_key, .. }
            | Token2022Log::ErrorHarvestingFrom4 { account_key, .. } => {
                collect_access_pubkey_id(*account_key, pubkey_ids);
            }
            _ => {}
        }
    }
}

fn collect_access_system_log_refs(log: &SystemProgramLog, pubkey_ids: &mut Vec<u32>) {
    match log {
        SystemProgramLog::CreateAddressMismatch {
            provided_addr,
            derived_addr,
        }
        | SystemProgramLog::TransferFromAddressMismatch {
            provided_addr,
            derived_addr,
        } => {
            collect_access_pubkey_id(*provided_addr, pubkey_ids);
            collect_access_pubkey_or_string_ref(*derived_addr, pubkey_ids);
        }
        SystemProgramLog::CreateAccountAlreadyInUse { addr }
        | SystemProgramLog::AllocateAlreadyInUse { addr }
        | SystemProgramLog::AllocateToMustSign { addr }
        | SystemProgramLog::AllocateAccountAlreadyInUse { addr }
        | SystemProgramLog::AssignAccountMustSign { addr }
        | SystemProgramLog::CreateAccountAccountAlreadyInUse { addr } => {
            collect_access_system_address_refs(*addr, pubkey_ids);
        }
        SystemProgramLog::TransferFromMustSign { from } => {
            collect_access_pubkey_id(*from, pubkey_ids);
        }
        SystemProgramLog::NonceAccountMustBeWriteable { account, .. }
        | SystemProgramLog::NonceAccountMustBeSigner { account, .. }
        | SystemProgramLog::NonceAccountMustSign { account, .. }
        | SystemProgramLog::NonceAccountStateInvalid { account, .. } => {
            collect_access_pubkey_or_string_ref(*account, pubkey_ids);
        }
        SystemProgramLog::Instruction(_)
        | SystemProgramLog::AllocateRequestedTooLarge { .. }
        | SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions { .. }
        | SystemProgramLog::TransferFromMustNotCarryData
        | SystemProgramLog::TransferInsufficient { .. }
        | SystemProgramLog::AdvanceNonceRecentBlockhashesEmpty
        | SystemProgramLog::InitializeNonceRecentBlockhashesEmpty
        | SystemProgramLog::AuthorizeNonceAccount { .. }
        | SystemProgramLog::NonceInsufficientLamports { .. }
        | SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot { .. } => {}
    }
}

fn collect_access_system_address_refs(address: SystemAddress, pubkey_ids: &mut Vec<u32>) {
    match address {
        SystemAddress::Pubkey(pubkey) => collect_access_pubkey_or_string_ref(pubkey, pubkey_ids),
        SystemAddress::Debug { address, base } => {
            collect_access_pubkey_or_string_ref(address, pubkey_ids);
            if let Some(base) = base {
                collect_access_pubkey_or_string_ref(base, pubkey_ids);
            }
        }
    }
}

fn collect_access_pubkey_or_string_ref(value: PubkeyOrString, pubkey_ids: &mut Vec<u32>) {
    if let PubkeyOrString::Pubkey(pubkey) = value {
        collect_access_pubkey_id(pubkey, pubkey_ids);
    }
}

fn collect_access_recent_blockhash_id(
    value: &OwnedCompactRecentBlockhash,
    blockhash_ids: &mut Vec<i32>,
) {
    if let OwnedCompactRecentBlockhash::Id(id) = value {
        collect_access_blockhash_id(*id, blockhash_ids);
    }
}

fn collect_access_blockhash_id(id: i32, blockhash_ids: &mut Vec<i32>) {
    blockhash_ids.push(id);
}

fn resolve_access_previous_blockhash(
    block: &ArchiveV2HotBlockBlob,
    blockhashes: &[[u8; 32]],
    previous_tail: &[PreviousBlockhash],
) -> Result<[u8; 32]> {
    if block.header.blockhash_id == 0
        && block.header.previous_blockhash_id == 0
        && let Some(previous) = previous_tail.last()
    {
        return Ok(previous.hash);
    }
    resolve_access_blockhash_id(
        block.header.previous_blockhash_id as i32,
        blockhashes,
        previous_tail,
    )
}

fn resolve_access_blockhash_id(
    id: i32,
    blockhashes: &[[u8; 32]],
    previous_tail: &[PreviousBlockhash],
) -> Result<[u8; 32]> {
    if id >= 0 {
        return blockhashes
            .get(id as usize)
            .copied()
            .with_context(|| format!("blockhash id {id} is outside loaded registry"));
    }
    let index = i32::try_from(previous_tail.len())
        .context("previous blockhash tail exceeds i32::MAX")?
        .checked_add(id)
        .filter(|index| *index >= 0)
        .with_context(|| format!("previous-tail blockhash id {id} is outside loaded tail"))?
        as usize;
    previous_tail
        .get(index)
        .map(|item| item.hash)
        .with_context(|| format!("previous-tail blockhash id {id} is outside loaded tail"))
}

fn hot_rewards_from_archive(rewards: WincodeArchiveV2Rewards) -> Result<ArchiveV2HotRewards> {
    anyhow::ensure!(
        rewards.raw_fallback.is_none() && rewards.decode_error.is_none(),
        "hot block writer requires decoded rewards"
    );
    Ok(ArchiveV2HotRewards {
        num_partitions: rewards.num_partitions,
        decoded: rewards.decoded.unwrap_or_default(),
    })
}

fn hot_message_from_owned(
    message: OwnedCompactMessage,
    known_program_ids: &KnownProgramIds,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<(ArchiveV2HotMessagePayload, u32)> {
    hot_message_from_owned_with_instruction_scratch(
        message,
        &mut Vec::new(),
        known_program_ids,
        slot_to_block_id,
        vote_hashes,
    )
}

fn hot_message_from_owned_with_instruction_scratch(
    message: OwnedCompactMessage,
    instruction_scratch: &mut Vec<ArchiveV2HotInstruction>,
    known_program_ids: &KnownProgramIds,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<(ArchiveV2HotMessagePayload, u32)> {
    match message {
        OwnedCompactMessage::Legacy(message) => {
            let has_compact_vote = match hot_instructions_from_owned_into(
                message.instructions,
                &message.account_keys,
                known_program_ids,
                slot_to_block_id,
                vote_hashes,
                instruction_scratch,
            ) {
                Ok(has_compact_vote) => has_compact_vote,
                Err(error) => {
                    instruction_scratch.clear();
                    return Err(error);
                }
            };
            let flags = if has_compact_vote {
                ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX
            } else {
                0
            };
            Ok((
                ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                    header: message.header,
                    account_keys: message.account_keys,
                    recent_blockhash: message.recent_blockhash,
                    instructions: std::mem::take(instruction_scratch),
                }),
                flags,
            ))
        }
        OwnedCompactMessage::V0(message) => {
            let has_compact_vote = match hot_instructions_from_owned_into(
                message.instructions,
                &message.account_keys,
                known_program_ids,
                slot_to_block_id,
                vote_hashes,
                instruction_scratch,
            ) {
                Ok(has_compact_vote) => has_compact_vote,
                Err(error) => {
                    instruction_scratch.clear();
                    return Err(error);
                }
            };
            let mut flags = ARCHIVE_V2_TX_FLAG_MESSAGE_V0;
            if has_compact_vote {
                flags |= ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX;
            }
            Ok((
                ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                    header: message.header,
                    account_keys: message.account_keys,
                    recent_blockhash: message.recent_blockhash,
                    instructions: std::mem::take(instruction_scratch),
                    address_table_lookups: message.address_table_lookups,
                }),
                flags,
            ))
        }
    }
}

fn hot_instructions_from_owned_into(
    instructions: Vec<OwnedCompactInstruction>,
    account_keys: &[CompactPubkey],
    known_program_ids: &KnownProgramIds,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
    out: &mut Vec<ArchiveV2HotInstruction>,
) -> Result<bool> {
    out.clear();
    reserve_total_capacity(out, instructions.len());
    let mut has_compact_vote = false;
    for instruction in instructions {
        let program_id =
            resolve_static_program_compact_id(account_keys, instruction.program_id_index);
        let data = if known_program_ids.is_vote(program_id) {
            let data =
                parse_hot_vote_instruction_data(&instruction.data, slot_to_block_id, vote_hashes)
                    .with_context(|| "parse vote instruction data")?;
            if matches!(
                data,
                ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(_)
                    | ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch { .. }
                    | ArchiveV2HotInstructionData::VoteTowerSync(_)
                    | ArchiveV2HotInstructionData::VoteTowerSyncSwitch { .. }
            ) {
                has_compact_vote = true;
                vote_hashes.compact_vote_ix += 1;
            }
            data
        } else if known_program_ids.is_compute_budget(program_id) {
            parse_hot_compute_budget_instruction_data(&instruction.data)
                .with_context(|| "parse compute budget instruction data")?
        } else if known_program_ids.is_system(program_id) {
            parse_hot_system_instruction_data(&instruction.data)
                .with_context(|| "parse system instruction data")?
        } else {
            ArchiveV2HotInstructionData::Raw(instruction.data)
        };
        out.push(ArchiveV2HotInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts,
            data,
        });
    }
    Ok(has_compact_vote)
}

fn resolve_static_program_compact_id(
    account_keys: &[CompactPubkey],
    program_id_index: u8,
) -> Option<u32> {
    match account_keys.get(program_id_index as usize)? {
        CompactPubkey::Id(id) => Some(*id),
        CompactPubkey::Raw(_) => None,
    }
}

#[derive(Clone, Copy, Debug)]
struct KnownProgramIds {
    vote: Option<u32>,
    compute_budget: Option<u32>,
    system: Option<u32>,
}

impl KnownProgramIds {
    fn from_index(index: &KeyIndex) -> Self {
        Self {
            vote: index.lookup(&vote_program_id_bytes()),
            compute_budget: index.lookup(&compute_budget_program_id_bytes()),
            system: index.lookup(&system_program_id_bytes()),
        }
    }

    fn from_first_seen(registry: &FirstSeenRegistry) -> Self {
        Self {
            vote: registry.lookup(&vote_program_id_bytes()),
            compute_budget: registry.lookup(&compute_budget_program_id_bytes()),
            system: registry.lookup(&system_program_id_bytes()),
        }
    }

    /// First-seen IDs are append-only, so an ID that has been discovered never
    /// needs another registry lookup. Missing programs are refreshed after the
    /// current block has been interned so instructions in that same block keep
    /// the historical parsing behavior.
    fn refresh_from_first_seen(&mut self, registry: &FirstSeenRegistry) {
        if self.vote.is_none() {
            self.vote = registry.lookup(&vote_program_id_bytes());
        }
        if self.compute_budget.is_none() {
            self.compute_budget = registry.lookup(&compute_budget_program_id_bytes());
        }
        if self.system.is_none() {
            self.system = registry.lookup(&system_program_id_bytes());
        }
    }

    #[inline]
    fn is_vote(&self, id: Option<u32>) -> bool {
        id.is_some() && id == self.vote
    }

    #[inline]
    fn is_compute_budget(&self, id: Option<u32>) -> bool {
        id.is_some() && id == self.compute_budget
    }

    #[inline]
    fn is_system(&self, id: Option<u32>) -> bool {
        id.is_some() && id == self.system
    }
}

fn vote_program_id_bytes() -> [u8; 32] {
    solana_pubkey::pubkey!("Vote111111111111111111111111111111111111111").to_bytes()
}

fn compute_budget_program_id_bytes() -> [u8; 32] {
    solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111").to_bytes()
}

fn system_program_id_bytes() -> [u8; 32] {
    solana_pubkey::pubkey!("11111111111111111111111111111111").to_bytes()
}

fn hot_metadata_flags(metadata: &CompactMetaV1) -> u32 {
    let mut flags = 0;
    if metadata.err.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_ERROR;
    }
    if metadata.return_data.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA;
    }
    if metadata.logs.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_LOGS;
    }
    if metadata.inner_instructions.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
    }
    if !metadata.pre_token_balances.is_empty() || !metadata.post_token_balances.is_empty() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
    }
    if !metadata.loaded_writable_addresses.is_empty()
        || !metadata.loaded_readonly_addresses.is_empty()
    {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
    }
    flags
}

fn parse_hot_vote_instruction_data(
    data: &[u8],
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<ArchiveV2HotInstructionData> {
    if data.is_empty() {
        return Ok(ArchiveV2HotInstructionData::UnknownVote(Vec::new()));
    }

    let instruction_result: std::result::Result<VoteInstruction, _> = bincode::options()
        .with_limit(VOTE_INSTRUCTION_DECODE_LIMIT)
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(data);
    let instruction = match instruction_result {
        Ok(instruction) => instruction,
        Err(err) => {
            match try_parse_historical_tower_sync_instruction_data(
                data,
                slot_to_block_id,
                vote_hashes,
            )
            .with_context(|| {
                format!(
                    "parse historical vote tower sync instruction variant={} len={} prefix={}",
                    vote_instruction_variant_label(data),
                    data.len(),
                    hex_prefix(data, 32)
                )
            }) {
                Ok(Some(parsed)) => return Ok(parsed),
                Ok(None) if is_tower_sync_vote_variant(data) => {
                    return Ok(ArchiveV2HotInstructionData::UnknownVote(data.to_vec()));
                }
                Ok(None) => {}
                Err(_) if is_tower_sync_vote_variant(data) => {
                    return Ok(ArchiveV2HotInstructionData::UnknownVote(data.to_vec()));
                }
                Err(historical_err) => return Err(historical_err),
            }
            Err::<VoteInstruction, _>(err).with_context(|| {
                format!(
                    "canonical vote instruction decode failed: variant={} len={} prefix={}",
                    vote_instruction_variant_label(data),
                    data.len(),
                    hex_prefix(data, 32)
                )
            })?
        }
    };

    let parsed = match instruction {
        VoteInstruction::CompactUpdateVoteState(update) => {
            let update =
                match archive_vote_state_update_from_solana(&update, slot_to_block_id, vote_hashes)
                {
                    Ok(update) => update,
                    Err(err) if is_uncompactable_vote_update_error(&err) => {
                        return Ok(ArchiveV2HotInstructionData::UnknownVote(data.to_vec()));
                    }
                    Err(err) => return Err(err),
                };
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update)
        }
        VoteInstruction::CompactUpdateVoteStateSwitch(update, switch_proof_hash) => {
            let update =
                match archive_vote_state_update_from_solana(&update, slot_to_block_id, vote_hashes)
                {
                    Ok(update) => update,
                    Err(err) if is_uncompactable_vote_update_error(&err) => {
                        return Ok(ArchiveV2HotInstructionData::UnknownVote(data.to_vec()));
                    }
                    Err(err) => return Err(err),
                };
            let switch_proof_hash = vote_hashes.ref_aux_hash(switch_proof_hash.to_bytes());
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            }
        }
        VoteInstruction::TowerSync(tower) => {
            let tower = match archive_tower_sync_from_solana(&tower, slot_to_block_id, vote_hashes)
            {
                Ok(tower) => tower,
                Err(err) if is_uncompactable_vote_update_error(&err) => {
                    return Ok(ArchiveV2HotInstructionData::UnknownVote(data.to_vec()));
                }
                Err(err) => return Err(err),
            };
            ArchiveV2HotInstructionData::VoteTowerSync(tower)
        }
        VoteInstruction::TowerSyncSwitch(tower, switch_proof_hash) => {
            let tower = match archive_tower_sync_from_solana(&tower, slot_to_block_id, vote_hashes)
            {
                Ok(tower) => tower,
                Err(err) if is_uncompactable_vote_update_error(&err) => {
                    return Ok(ArchiveV2HotInstructionData::UnknownVote(data.to_vec()));
                }
                Err(err) => return Err(err),
            };
            let switch_proof_hash = vote_hashes.ref_aux_hash(switch_proof_hash.to_bytes());
            ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            }
        }
        _ => ArchiveV2HotInstructionData::Raw(data.to_vec()),
    };
    Ok(parsed)
}

fn is_uncompactable_vote_update_error(err: &anyhow::Error) -> bool {
    let message = format!("{err:#}");
    message.contains("vote lockout history length")
        && message.contains("exceeds MAX_LOCKOUT_HISTORY")
}

fn try_parse_historical_tower_sync_instruction_data(
    data: &[u8],
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<Option<ArchiveV2HotInstructionData>> {
    if data.len() < 4 {
        return Ok(None);
    }
    let mut cursor = VoteInstructionCursor::new(data);
    let variant = cursor.read_u32_le()?;
    let parsed = match variant {
        14 => {
            let tower = read_historical_tower_sync(&mut cursor, slot_to_block_id, vote_hashes)?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::VoteTowerSync(tower)
        }
        15 => {
            let tower = read_historical_tower_sync(&mut cursor, slot_to_block_id, vote_hashes)?;
            let switch_proof_hash = vote_hashes.ref_aux_hash(cursor.read_pubkey()?);
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(parsed))
}

fn read_historical_tower_sync(
    cursor: &mut VoteInstructionCursor<'_>,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<ArchiveV2VoteTowerSync> {
    let lockout_len = cursor.read_u64_le()?;
    anyhow::ensure!(
        lockout_len <= 31,
        "historical tower sync lockout history length {} exceeds MAX_LOCKOUT_HISTORY",
        lockout_len
    );
    let mut lockouts = Vec::with_capacity(lockout_len as usize);
    for _ in 0..lockout_len {
        let slot = cursor.read_u64_le()?;
        let confirmation_count = cursor.read_u32_le()?;
        let confirmation_count = u8::try_from(confirmation_count)
            .context("historical vote confirmation count exceeds u8::MAX")?;
        lockouts.push((slot, confirmation_count));
    }
    let root = cursor.read_option_u64_le()?;
    let hash = cursor.read_pubkey()?;
    let timestamp = cursor.read_option_i64_le()?;
    let block_id = cursor.read_pubkey()?;

    let (update, last_slot) = archive_vote_state_update_from_parts(
        root,
        lockouts.into_iter().map(Ok),
        hash,
        timestamp,
        slot_to_block_id,
        vote_hashes,
    )?;
    let block_id_hash = vote_hashes.ref_block_id_hash(last_slot, block_id, slot_to_block_id)?;
    Ok(ArchiveV2VoteTowerSync {
        update,
        block_id_hash,
    })
}

fn vote_instruction_variant_label(data: &[u8]) -> String {
    if data.len() < 4 {
        return "short".to_string();
    }
    u32::from_le_bytes([data[0], data[1], data[2], data[3]]).to_string()
}

fn is_tower_sync_vote_variant(data: &[u8]) -> bool {
    data.len() >= 4
        && matches!(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            14 | 15
        )
}

fn hex_prefix(data: &[u8], max_len: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = &data[..data.len().min(max_len)];
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    if data.len() > max_len {
        out.push_str("...");
    }
    out
}

fn parse_hot_compute_budget_instruction_data(data: &[u8]) -> Result<ArchiveV2HotInstructionData> {
    match try_parse_hot_compute_budget_instruction_data(data).with_context(|| {
        format!(
            "parse compute budget instruction data len={} prefix={}",
            data.len(),
            hex_prefix(data, 32)
        )
    })? {
        Some(parsed) => Ok(parsed),
        None => Ok(ArchiveV2HotInstructionData::Raw(data.to_vec())),
    }
}

fn try_parse_hot_compute_budget_instruction_data(
    data: &[u8],
) -> Result<Option<ArchiveV2HotInstructionData>> {
    let Some((&tag, rest)) = data.split_first() else {
        anyhow::bail!("empty compute budget instruction data");
    };
    let mut cursor = VoteInstructionCursor::new(rest);
    let parsed = match tag {
        0 => {
            if !cursor.is_eof() {
                return Ok(None);
            }
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::Unused,
            )
        }
        1 => {
            let bytes = cursor.read_u32_le()?;
            if !cursor.is_eof() {
                return Ok(None);
            }
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::RequestHeapFrame(bytes),
            )
        }
        2 => {
            let units = cursor.read_u32_le()?;
            if !cursor.is_eof() {
                return Ok(None);
            }
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(units),
            )
        }
        3 => {
            let micro_lamports = cursor.read_u64_le()?;
            if !cursor.is_eof() {
                return Ok(None);
            }
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(micro_lamports),
            )
        }
        4 => {
            let bytes = cursor.read_u32_le()?;
            if !cursor.is_eof() {
                return Ok(None);
            }
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetLoadedAccountsDataSizeLimit(bytes),
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(parsed))
}

fn parse_hot_system_instruction_data(data: &[u8]) -> Result<ArchiveV2HotInstructionData> {
    match try_parse_hot_system_instruction_data(data) {
        Some(parsed) => Ok(parsed),
        None => Ok(ArchiveV2HotInstructionData::UnknownSystem(data.to_vec())),
    }
}

fn try_parse_hot_system_instruction_data(data: &[u8]) -> Option<ArchiveV2HotInstructionData> {
    try_parse_hot_system_instruction_data_inner(data)
        .ok()
        .flatten()
}

fn try_parse_hot_system_instruction_data_inner(
    data: &[u8],
) -> Result<Option<ArchiveV2HotInstructionData>> {
    let mut cursor = VoteInstructionCursor::new(data);
    let variant = cursor.read_u32_le()?;
    macro_rules! preserve_raw_on_trailing_bytes {
        () => {
            if !cursor.is_eof() {
                return Ok(None);
            }
        };
    }
    let parsed = match variant {
        0 => {
            let lamports = cursor.read_u64_le()?;
            let space = cursor.read_u64_le()?;
            let owner = cursor.read_pubkey()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::CreateAccount {
                lamports,
                space,
                owner,
            })
        }
        1 => {
            let owner = cursor.read_pubkey()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Assign { owner })
        }
        2 => {
            let lamports = cursor.read_u64_le()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                lamports,
            })
        }
        3 => {
            let base = cursor.read_pubkey()?;
            let seed = cursor.read_system_seed()?;
            let lamports = cursor.read_u64_le()?;
            let space = cursor.read_u64_le()?;
            let owner = cursor.read_pubkey()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::CreateAccountWithSeed {
                    base,
                    seed,
                    lamports,
                    space,
                    owner,
                },
            )
        }
        4 => {
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::AdvanceNonceAccount)
        }
        5 => {
            let lamports = cursor.read_u64_le()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports },
            )
        }
        6 => {
            let authority = cursor.read_pubkey()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::InitializeNonceAccount { authority },
            )
        }
        7 => {
            let authority = cursor.read_pubkey()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AuthorizeNonceAccount { authority },
            )
        }
        8 => {
            let space = cursor.read_u64_le()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Allocate { space })
        }
        9 => {
            let base = cursor.read_pubkey()?;
            let seed = cursor.read_system_seed()?;
            let space = cursor.read_u64_le()?;
            let owner = cursor.read_pubkey()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::AllocateWithSeed {
                base,
                seed,
                space,
                owner,
            })
        }
        10 => {
            let base = cursor.read_pubkey()?;
            let seed = cursor.read_system_seed()?;
            let owner = cursor.read_pubkey()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::AssignWithSeed {
                base,
                seed,
                owner,
            })
        }
        11 => {
            let lamports = cursor.read_u64_le()?;
            let from_seed = cursor.read_system_seed()?;
            let from_owner = cursor.read_pubkey()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::TransferWithSeed {
                lamports,
                from_seed,
                from_owner,
            })
        }
        12 => {
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::UpgradeNonceAccount)
        }
        13 => {
            let lamports = cursor.read_u64_le()?;
            let space = cursor.read_u64_le()?;
            let owner = cursor.read_pubkey()?;
            preserve_raw_on_trailing_bytes!();
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::CreateAccountAllowPrefund {
                    lamports,
                    space,
                    owner,
                },
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(parsed))
}

fn archive_tower_sync_from_solana(
    tower: &SolanaTowerSync,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<ArchiveV2VoteTowerSync> {
    let (update, last_slot) = archive_vote_state_update_from_parts(
        tower.root,
        tower.lockouts.iter().map(|lockout| {
            Ok((
                lockout.slot(),
                u8::try_from(lockout.confirmation_count())
                    .context("vote confirmation count exceeds u8::MAX")?,
            ))
        }),
        tower.hash.to_bytes(),
        tower.timestamp,
        slot_to_block_id,
        vote_hashes,
    )?;
    let block_id_hash =
        vote_hashes.ref_block_id_hash(last_slot, tower.block_id.to_bytes(), slot_to_block_id)?;
    Ok(ArchiveV2VoteTowerSync {
        update,
        block_id_hash,
    })
}

fn archive_vote_state_update_from_solana(
    update: &SolanaVoteStateUpdate,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<ArchiveV2VoteStateUpdate> {
    Ok(archive_vote_state_update_from_parts(
        update.root,
        update.lockouts.iter().map(|lockout| {
            Ok((
                lockout.slot(),
                u8::try_from(lockout.confirmation_count())
                    .context("vote confirmation count exceeds u8::MAX")?,
            ))
        }),
        update.hash.to_bytes(),
        update.timestamp,
        slot_to_block_id,
        vote_hashes,
    )?
    .0)
}

fn archive_vote_state_update_from_parts<I>(
    root: Option<u64>,
    lockouts: I,
    hash: [u8; 32],
    timestamp: Option<i64>,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<(ArchiveV2VoteStateUpdate, Option<u64>)>
where
    I: IntoIterator<Item = Result<(u64, u8)>>,
{
    let lockouts = lockouts.into_iter().collect::<Result<Vec<_>>>()?;
    anyhow::ensure!(
        lockouts.len() <= 31,
        "vote lockout history length {} exceeds MAX_LOCKOUT_HISTORY",
        lockouts.len()
    );
    let mut lockout_offsets = Vec::with_capacity(lockouts.len());
    let mut slot = root.unwrap_or_default();
    let mut last_slot = None;
    for (next_slot, confirmation_count) in lockouts {
        let offset = next_slot
            .checked_sub(slot)
            .context("vote lockout slots are not monotonic")?;
        slot = next_slot;
        lockout_offsets.push(ArchiveV2VoteLockoutOffset {
            offset,
            confirmation_count,
        });
        last_slot = Some(slot);
    }
    let hash = vote_hashes.ref_bank_hash(last_slot, hash, slot_to_block_id)?;
    Ok((
        ArchiveV2VoteStateUpdate {
            root,
            lockout_offsets,
            hash,
            timestamp,
        },
        last_slot,
    ))
}

struct VoteInstructionCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> VoteInstructionCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos == self.bytes.len()
    }

    fn ensure_eof(&self) -> Result<()> {
        anyhow::ensure!(
            self.pos == self.bytes.len(),
            "trailing {} bytes in instruction data",
            self.bytes.len().saturating_sub(self.pos)
        );
        Ok(())
    }

    fn read_u32_le(&mut self) -> Result<u32> {
        let bytes = self.read_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64_le(&mut self) -> Result<u64> {
        let bytes = self.read_array::<8>()?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_i64_le(&mut self) -> Result<i64> {
        let bytes = self.read_array::<8>()?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_option_u64_le(&mut self) -> Result<Option<u64>> {
        match self.read_option_tag()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64_le()?)),
            tag => anyhow::bail!("invalid bincode Option<u64> tag {tag}"),
        }
    }

    fn read_option_i64_le(&mut self) -> Result<Option<i64>> {
        match self.read_option_tag()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_i64_le()?)),
            tag => anyhow::bail!("invalid bincode Option<i64> tag {tag}"),
        }
    }

    fn read_option_tag(&mut self) -> Result<u8> {
        let tag = self.read_array::<1>()?;
        Ok(tag[0])
    }

    fn read_pubkey(&mut self) -> Result<[u8; 32]> {
        self.read_array::<32>()
    }

    fn read_system_seed(&mut self) -> Result<String> {
        let len = self.read_u64_le()?;
        let bytes = self.read_slice(len as usize)?;
        String::from_utf8(bytes.to_vec()).context("system instruction seed is not UTF-8")
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .context("instruction cursor overflow")?;
        anyhow::ensure!(
            end <= self.bytes.len(),
            "unexpected EOF in instruction data while reading {len} bytes"
        );
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let end = self
            .pos
            .checked_add(N)
            .context("instruction cursor overflow")?;
        anyhow::ensure!(
            end <= self.bytes.len(),
            "unexpected EOF in instruction data while reading {N} bytes"
        );
        let mut out = [0u8; N];
        out.copy_from_slice(&self.bytes[self.pos..end]);
        self.pos = end;
        Ok(out)
    }
}

pub(crate) fn build_no_registry(input: &Path, output_dir: &Path) -> Result<()> {
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;
    let scanner = RawCarScanner::open(input)?;
    build_no_registry_from_scanner(scanner, output_dir, genesis, &input.display().to_string())
}

pub(crate) fn build_no_registry_from_url(url: &str, output_dir: &Path) -> Result<()> {
    anyhow::ensure!(
        url.starts_with("http://") || url.starts_with("https://"),
        "no-registry URL input must start with http:// or https://, got {url}"
    );
    let genesis = genesis_epoch0::maybe_load_for_label(url, output_dir)?;
    let scanner = RawCarScanner::open_url(url)?;
    build_no_registry_from_scanner(scanner, output_dir, genesis, url)
}

fn build_no_registry_from_scanner<R: Read>(
    mut scanner: RawCarScanner<R>,
    output_dir: &Path,
    genesis: Option<GenesisArchive>,
    input_label: &str,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let blockhash_id_offset = genesis.as_ref().map(|_| 1).unwrap_or(0);

    let archive_path = output_dir.join(ARCHIVE_NO_REGISTRY_FILE);
    let poh_path = output_dir.join(POH_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let archive_file = File::create(&archive_path)
        .with_context(|| format!("create {}", archive_path.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, archive_file));
    let poh_file =
        File::create(&poh_path).with_context(|| format!("create {}", poh_path.display()))?;
    let mut poh_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, poh_file));
    let blockhash_registry_file = File::create(&blockhash_registry_path)
        .with_context(|| format!("create {}", blockhash_registry_path.display()))?;
    let mut blockhash_registry_writer =
        BufWriter::with_capacity(BUFFER_SIZE, blockhash_registry_file);
    writer.write(&WincodeArchiveV2NoRegistryRecord::Header(
        WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_VERSION,
            flags: ARCHIVE_FLAGS_LEB128 | ARCHIVE_FLAGS_NO_REGISTRY,
        },
    ))?;
    if let Some(genesis) = &genesis {
        writer.write(&WincodeArchiveV2NoRegistryRecord::Genesis(
            no_registry_genesis_record(genesis),
        ))?;
        blockhash_registry_writer
            .write_all(&genesis.genesis_hash)
            .with_context(|| format!("write {}", blockhash_registry_path.display()))?;
    }

    scanner.skip_header()?;
    let mut pending = PendingBlock::default();
    let mut footer = WincodeArchiveV2Footer::default();
    let mut timings = ArchiveV2Timings::from_env();
    let mut progress = ProgressTracker::new("Archive V2 NoRegistry Write");
    let mut block_offset = 0u64;
    let mut block_id = 0u32;
    let mut block_scratch = Vec::with_capacity(8 << 20);
    let mut metadata_zstd = ZstdReusableDecoder::new();

    while let Some(raw) = scanner.next_node_timed(Some(&mut timings))? {
        footer.car_entries += 1;
        footer.car_payload_bytes += raw.payload_len as u64;
        footer.decoded_node_payload_bytes += raw.payload_len as u64;

        let classify_started = timings.detail_timer();
        match raw.node {
            RawNode::Transaction(tx) => {
                footer.transactions += 1;
                pending.transactions.push(PendingTx {
                    tx,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Entry(entry) => {
                footer.entries += 1;
                pending.entries.push(PendingEntry {
                    entry,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Rewards(rewards) => {
                footer.rewards += 1;
                anyhow::ensure!(
                    pending.rewards.is_none(),
                    "duplicate rewards node before block"
                );
                pending.rewards = Some(PendingRewards {
                    rewards,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::DataFrame(frame) => {
                footer.dataframes += 1;
                pending.dataframes.insert(frame.cid, frame);
            }
            RawNode::Block(block) => {
                footer.blocks += 1;
                timings.classify += classify_started.elapsed();
                let (record, tx_count, sidecar) = build_no_registry_block_record(
                    &mut pending,
                    block,
                    block_id.saturating_add(blockhash_id_offset),
                    &mut footer,
                    &mut timings,
                    &mut metadata_zstd,
                )?;
                let encode_started = timings.detail_timer();
                let record = WincodeArchiveV2NoRegistryRecord::Block(record);
                encode_with_scratch(&record, &mut block_scratch)?;
                let block_len = u32::try_from(block_scratch.len())
                    .context("archive v2 no-registry frame exceeds u32::MAX")?;
                writer.write_bytes(&block_scratch)?;
                writer.write(&WincodeArchiveV2NoRegistryRecord::Index(
                    SplitCompactIndexRecord {
                        slot: pending.last_slot,
                        block_id,
                        block_offset,
                        block_len,
                        runtime_offset: 0,
                        runtime_len: 0,
                        tx_count,
                    },
                ))?;
                poh_writer.write(&WincodeArchiveV2PohRecord {
                    block_id,
                    slot: pending.last_slot,
                    entries: sidecar.poh_entries,
                })?;
                blockhash_registry_writer
                    .write_all(&sidecar.blockhash)
                    .with_context(|| {
                        format!(
                            "write {} block_id {}",
                            blockhash_registry_path.display(),
                            block_id
                        )
                    })?;
                timings.wincode_encode += encode_started.elapsed();
                block_offset += block_len as u64;
                block_id = block_id.wrapping_add(1);
                progress.update_slot(pending.last_slot);
                progress.update_input_bytes(footer.car_payload_bytes);
                progress.update(1, tx_count as u64);
                pending.clear();
                continue;
            }
            RawNode::Subset(_) => footer.subset_nodes_ignored += 1,
            RawNode::Epoch(_) => footer.epoch_nodes_ignored += 1,
        }
        timings.classify += classify_started.elapsed();
    }

    let encode_started = timings.detail_timer();
    writer.write(&WincodeArchiveV2NoRegistryRecord::Footer(footer.clone()))?;
    timings.wincode_encode += encode_started.elapsed();
    writer.flush()?;
    poh_writer.flush()?;
    blockhash_registry_writer
        .flush()
        .with_context(|| format!("flush {}", blockhash_registry_path.display()))?;
    progress.final_report();
    info!("Archive V2 no-registry build complete");
    info!("  input: {input_label}");
    info!("  archive: {}", archive_path.display());
    info!("  poh: {}", poh_path.display());
    info!(
        "  blockhash_registry: {}",
        blockhash_registry_path.display()
    );
    info!(
        "  coverage: entries={} payload_bytes={} decoded_payload_bytes={} tx_raw_fallbacks={} metadata_raw_fallbacks={} rewards_raw_fallbacks={} nonce_recent_blockhashes={}",
        footer.car_entries,
        footer.car_payload_bytes,
        footer.decoded_node_payload_bytes,
        footer.tx_raw_fallbacks,
        footer.metadata_raw_fallbacks,
        footer.rewards_raw_fallbacks,
        footer.nonce_recent_blockhashes
    );
    info!(
        "  timings: scan/decode_node={:.3}s classify={:.3}s dataframe_assemble={:.3}s tx_decode={:.3}s metadata_decode={:.3}s rewards_decode={:.3}s wincode_encode={:.3}s",
        timings.scan_decode_node.as_secs_f64(),
        timings.classify.as_secs_f64(),
        timings.dataframe_assemble.as_secs_f64(),
        timings.tx_decode_compact.as_secs_f64(),
        timings.metadata_decode_compact.as_secs_f64(),
        timings.rewards_decode_compact.as_secs_f64(),
        timings.wincode_encode.as_secs_f64(),
    );
    info!(
        "  archive_v2_stats: tx_reassembled={} metadata_reassembled={} metadata_protobuf_visit={} metadata_owned_fallback={} tx_scratch_max={} metadata_scratch_max={}",
        timings.tx_reassembled,
        timings.metadata_reassembled,
        timings.metadata_protobuf_visit,
        timings.metadata_owned_fallback,
        timings.tx_scratch_max,
        timings.metadata_scratch_max,
    );
    Ok(())
}

pub(crate) fn build_registries(
    input: &Path,
    output_dir: &Path,
    external_blockhashes_path: Option<&Path>,
    force: bool,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let external_blockhashes = load_external_blockhash_overrides(external_blockhashes_path)?;

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    if !force
        && crate::file_nonempty(&registry_path)
        && crate::file_nonempty(&registry_counts_path)
        && crate::file_nonempty(&blockhash_registry_path)
    {
        info!(
            "Reusing existing Archive V2 registries: {}, {}, {}",
            registry_path.display(),
            registry_counts_path.display(),
            blockhash_registry_path.display()
        );
        return Ok(());
    }

    build_registry_and_blockhash_for_input(
        input,
        &registry_path,
        Some(&registry_counts_path),
        &blockhash_registry_path,
        &external_blockhashes,
        None,
    )?;
    info!(
        "Archive V2 registries built: registry={} registry_counts={} blockhash_registry={}",
        registry_path.display(),
        registry_counts_path.display(),
        blockhash_registry_path.display()
    );
    Ok(())
}

pub(crate) fn build_registry_index(
    registry_path: &Path,
    output: Option<&Path>,
    force: bool,
) -> Result<()> {
    let output_path = output
        .map(Path::to_path_buf)
        .unwrap_or_else(|| registry_path.with_file_name(REGISTRY_INDEX_FILE));
    if !force && crate::file_nonempty(&output_path) {
        info!(
            "Reusing existing Archive V2 registry index: {}",
            output_path.display()
        );
        return Ok(());
    }
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }

    let started = Instant::now();
    info!(
        "Building Archive V2 registry MPHF index: registry={} output={}",
        registry_path.display(),
        output_path.display()
    );
    let load_started = Instant::now();
    let registry = MappedRegistryKeys::open(registry_path)?;
    let load_elapsed = load_started.elapsed();
    let build_started = Instant::now();
    let index = KeyIndex::build_from_slice_low_memory(registry.keys());
    let build_elapsed = build_started.elapsed();
    write_registry_key_index_atomic_checked(&index, &output_path, || registry.ensure_unchanged())?;
    info!(
        "Archive V2 registry MPHF index built in {:.2}s: keys={} load={:.2}s build={:.2}s output={}",
        started.elapsed().as_secs_f64(),
        index.len(),
        load_elapsed.as_secs_f64(),
        build_elapsed.as_secs_f64(),
        output_path.display()
    );
    Ok(())
}

pub(crate) fn build_blockhash_registry(
    input: &Path,
    output_dir: &Path,
    external_blockhashes_path: Option<&Path>,
    force: bool,
) -> Result<()> {
    let external_blockhashes = load_external_blockhash_overrides(external_blockhashes_path)?;
    build_blockhash_registry_with_tail(input, output_dir, force, 0, &external_blockhashes)?;
    ensure_default_block_time_gaps(output_dir)
}

fn build_blockhash_registry_with_tail(
    input: &Path,
    output_dir: &Path,
    force: bool,
    tail_len: usize,
    external_blockhashes: &ExternalBlockhashOverrides,
) -> Result<Vec<PreviousBlockhash>> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;

    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let blockhash_index_v3_path = output_dir.join(BLOCKHASH_INDEX_V3_FILE);
    let blockhash_tmp = output_dir.join(format!("{BLOCKHASH_REGISTRY_FILE}.tmp"));
    let blockhash_index_v3_tmp = output_dir.join(format!("{BLOCKHASH_INDEX_V3_FILE}.tmp"));

    let have_v2 = crate::file_nonempty(&blockhash_registry_path);
    let have_v3 = crate::file_nonempty(&blockhash_index_v3_path);
    if !force && have_v2 && have_v3 {
        info!(
            "Reusing blockhash registry sidecars: {}, {}",
            blockhash_registry_path.display(),
            blockhash_index_v3_path.display()
        );
        return Ok(Vec::new());
    }

    let write_v2 = force || !have_v2;
    let write_v3 = force || !have_v3;

    let mut blockhash_writer = if write_v2 {
        let blockhash_file = File::create(&blockhash_tmp)
            .with_context(|| format!("create {}", blockhash_tmp.display()))?;
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, blockhash_file);
        if let Some(genesis) = &genesis {
            writer
                .write_all(&genesis.genesis_hash)
                .with_context(|| format!("write {}", blockhash_tmp.display()))?;
        }
        Some(writer)
    } else {
        info!(
            "Keeping existing V2 blockhash registry: {}",
            blockhash_registry_path.display()
        );
        None
    };

    let mut blockhash_index_v3_writer = if write_v3 {
        let mut file = File::create(&blockhash_index_v3_tmp)
            .with_context(|| format!("create {}", blockhash_index_v3_tmp.display()))?;
        write_blockhash_index_v3_header(&mut file, 0)
            .with_context(|| format!("write {}", blockhash_index_v3_tmp.display()))?;
        Some(BufWriter::with_capacity(BUFFER_SIZE, file))
    } else {
        info!(
            "Keeping existing V3 blockhash index: {}",
            blockhash_index_v3_path.display()
        );
        None
    };

    let mut scanner = RawCarScanner::open_with_buffer(input, BLOCKHASH_SCAN_BUFFER_SIZE)?;
    scanner.skip_header()?;
    let mut progress = ProgressTracker::new("Blockhash Registry");
    let mut latest_entry_hash: Option<[u8; 32]> = None;
    let mut tail = VecDeque::with_capacity(tail_len);
    let mut blocks = 0u64;
    let mut entry_nodes = 0u64;
    let mut skipped_nodes = 0u64;
    let mut car_payload_bytes = 0u64;
    let started = Instant::now();

    while let Some(raw) = scanner.next_blockhash_node()? {
        car_payload_bytes += raw.payload_len as u64;
        if is_entry_node(raw.prefix) {
            let hash = decode_entry_hash(raw.prefix)
                .map_err(|err| anyhow!("{err}"))
                .with_context(|| format!("entry {} decode hash", raw.cid))?;
            let hash = hash_32(hash).with_context(|| format!("entry {} hash", raw.cid))?;
            latest_entry_hash = Some(hash);
            entry_nodes += 1;
        } else if is_block_node(raw.prefix) {
            let (slot, block_time) = decode_block_slot_time(raw.prefix)
                .with_context(|| format!("block #{blocks} decode block slot/time"))?;
            let blockhash = if let Some(blockhash) = latest_entry_hash {
                anyhow::ensure!(
                    !external_blockhashes.contains_key(&slot),
                    "slot {slot} has an Entry hash and must not use an external blockhash override"
                );
                blockhash
            } else {
                let override_entry = external_blockhashes.get(&slot).ok_or_else(|| {
                    anyhow!("slot {slot} block #{blocks} has no latest Entry hash and no external blockhash override")
                })?;
                info!(
                    "Using external blockhash override for PoH-gap slot {} source={} blockhash={}",
                    slot,
                    override_entry.source,
                    Pubkey::new_from_array(override_entry.hash)
                );
                override_entry.hash
            };
            if let Some(writer) = blockhash_writer.as_mut() {
                writer
                    .write_all(&blockhash)
                    .with_context(|| format!("write {}", blockhash_tmp.display()))?;
            }
            if let Some(writer) = blockhash_index_v3_writer.as_mut() {
                write_blockhash_index_v3_row(writer, slot, &blockhash, block_time.unwrap_or(0))
                    .with_context(|| format!("write {}", blockhash_index_v3_tmp.display()))?;
            }
            if tail_len > 0 {
                tail.push_back(PreviousBlockhash {
                    hash: blockhash,
                    slot,
                });
                while tail.len() > tail_len {
                    tail.pop_front();
                }
            }

            blocks += 1;
            progress.update_slot(slot);
            progress.update_input_bytes(car_payload_bytes);
            progress.update(1, 0);
        } else {
            skipped_nodes += 1;
        }
    }

    if let Some(mut writer) = blockhash_writer {
        writer
            .flush()
            .with_context(|| format!("flush {}", blockhash_tmp.display()))?;
        std::fs::rename(&blockhash_tmp, &blockhash_registry_path).with_context(|| {
            format!(
                "rename {} to {}",
                blockhash_tmp.display(),
                blockhash_registry_path.display()
            )
        })?;
    }
    if let Some(mut writer) = blockhash_index_v3_writer {
        writer
            .flush()
            .with_context(|| format!("flush {}", blockhash_index_v3_tmp.display()))?;
        drop(writer);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&blockhash_index_v3_tmp)
            .with_context(|| format!("open {}", blockhash_index_v3_tmp.display()))?;
        file.seek(SeekFrom::Start(
            (ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC.len() + 2 + 2) as u64,
        ))
        .with_context(|| format!("seek {}", blockhash_index_v3_tmp.display()))?;
        file.write_all(&blocks.to_le_bytes())
            .with_context(|| format!("write row count {}", blockhash_index_v3_tmp.display()))?;
        file.flush()
            .with_context(|| format!("flush {}", blockhash_index_v3_tmp.display()))?;
        std::fs::rename(&blockhash_index_v3_tmp, &blockhash_index_v3_path).with_context(|| {
            format!(
                "rename {} to {}",
                blockhash_index_v3_tmp.display(),
                blockhash_index_v3_path.display()
            )
        })?;
    }

    progress.final_report();
    info!(
        "Blockhash registry complete in {:.2}s: blocks={} entry_nodes={} skipped_nodes={} payload_bytes={} blockhash_registry={} blockhash_index_v3={}",
        started.elapsed().as_secs_f64(),
        blocks,
        entry_nodes,
        skipped_nodes,
        car_payload_bytes,
        if write_v2 {
            blockhash_registry_path.display().to_string()
        } else {
            "reused".to_string()
        },
        if write_v3 {
            blockhash_index_v3_path.display().to_string()
        } else {
            "reused".to_string()
        }
    );
    Ok(tail.into_iter().collect())
}

pub(crate) fn optimize_no_registry(
    input: &Path,
    output_dir: &Path,
    previous_car: Option<&Path>,
    resume: bool,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let previous_tail = load_or_build_previous_tail(output_dir, previous_car, resume)?;

    let registry_path = output_dir.join(REGISTRY_FILE);
    if resume && crate::file_nonempty(&registry_path) {
        info!(
            "Reusing existing pubkey registry: {}",
            registry_path.display()
        );
    } else {
        build_no_registry_pubkey_registry(input, &registry_path)?;
    }

    let store = KeyStore::load(&registry_path)?;
    let key_count = store.len();
    let key_index = KeyIndex::build(store.keys);
    info!("Archive V2 pubkey registry loaded: {} keys", key_count);

    let input_dir = input.parent().unwrap_or_else(|| Path::new("."));
    let poh_path = copy_required_sidecar(input_dir, output_dir, POH_FILE)?;
    let blockhash_registry_path =
        copy_required_sidecar(input_dir, output_dir, BLOCKHASH_REGISTRY_FILE)?;
    let blockhashes = load_blockhash_registry_plain(&blockhash_registry_path)?;

    let archive_path = output_dir.join(ARCHIVE_FILE);
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let archive_file = File::create(&archive_path)
        .with_context(|| format!("create {}", archive_path.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, archive_file));

    let mut rolling_blockhashes = RollingBlockhashIndex::new(ROLLING_BLOCKHASH_CAPACITY);
    rolling_blockhashes.seed_previous_tail(&previous_tail)?;
    let mut progress = ProgressTracker::new("Archive V2 Optimize");
    let mut block_offset = 0u64;
    let mut block_id = 0u32;
    let mut blockhash_id_offset = 0u32;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut records = 0u64;
    let mut nonce_recent_blockhashes = 0u64;
    let mut block_scratch = Vec::with_capacity(8 << 20);
    let started = Instant::now();

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2NoRegistryRecord>()? {
        records += 1;
        match record {
            WincodeArchiveV2NoRegistryRecord::Header(header) => {
                anyhow::ensure!(
                    header.version == WINCODE_ARCHIVE_V2_VERSION,
                    "unsupported no-registry archive version {}",
                    header.version
                );
                writer.write(&WincodeArchiveV2Record::Header(WincodeArchiveV2Header {
                    version: WINCODE_ARCHIVE_V2_VERSION,
                    flags: ARCHIVE_FLAGS_LEB128,
                }))?;
            }
            WincodeArchiveV2NoRegistryRecord::Genesis(genesis) => {
                anyhow::ensure!(
                    block_id == 0,
                    "genesis record appeared after archive blocks had started"
                );
                anyhow::ensure!(
                    blockhashes.first() == Some(&genesis.genesis_hash),
                    "blockhash registry does not start with no-registry genesis hash"
                );
                blockhash_id_offset = 1;
                rolling_blockhashes.insert(genesis.genesis_hash, 0, 0)?;
                writer.write(&WincodeArchiveV2Record::Genesis(
                    compact_no_registry_genesis(genesis, &key_index)?,
                ))?;
            }
            WincodeArchiveV2NoRegistryRecord::Block(block) => {
                let slot = block.header.compact.slot;
                let blockhash_index = block_id as usize + blockhash_id_offset as usize;
                let current_blockhash = *blockhashes.get(blockhash_index).with_context(|| {
                    format!(
                        "missing blockhash registry entry for blockhash id {blockhash_index} slot {slot}"
                    )
                })?;
                rolling_blockhashes.prune_for_slot(slot)?;
                let (record, tx_count) = optimize_no_registry_block(
                    block,
                    &key_index,
                    &rolling_blockhashes,
                    block_id.saturating_add(blockhash_id_offset),
                    &mut nonce_recent_blockhashes,
                )
                .with_context(|| format!("slot {slot} optimize block"))?;

                let block_record = WincodeArchiveV2Record::Block(record);
                encode_with_scratch(&block_record, &mut block_scratch)?;
                let block_len = u32::try_from(block_scratch.len())
                    .context("archive v2 optimized frame exceeds u32::MAX")?;
                writer.write_bytes(&block_scratch)?;
                writer.write(&WincodeArchiveV2Record::Index(SplitCompactIndexRecord {
                    slot,
                    block_id,
                    block_offset,
                    block_len,
                    runtime_offset: 0,
                    runtime_len: 0,
                    tx_count,
                }))?;

                let current_block_id = i32::try_from(block_id.saturating_add(blockhash_id_offset))
                    .context("blockhash id exceeds i32::MAX")?;
                rolling_blockhashes.insert(current_blockhash, current_block_id, slot)?;
                block_offset += block_len as u64;
                block_id = block_id.wrapping_add(1);
                blocks += 1;
                txs += tx_count as u64;
                progress.update_slot(slot);
                progress.update(1, tx_count as u64);
            }
            WincodeArchiveV2NoRegistryRecord::Index(_) => {}
            WincodeArchiveV2NoRegistryRecord::Footer(mut footer) => {
                footer.nonce_recent_blockhashes += nonce_recent_blockhashes;
                writer.write(&WincodeArchiveV2Record::Footer(footer))?;
            }
        }
    }

    anyhow::ensure!(
        blocks as usize + blockhash_id_offset as usize == blockhashes.len(),
        "blockhash registry count {} does not match archive blocks {} plus genesis offset {}",
        blockhashes.len(),
        blocks,
        blockhash_id_offset
    );
    writer.flush()?;
    progress.final_report();
    info!(
        "Archive V2 optimize complete in {:.2}s: records={} blocks={} txs={} prev_tail={} nonce_recent_blockhashes={} archive={} registry={} poh={} blockhash_registry={}",
        started.elapsed().as_secs_f64(),
        records,
        blocks,
        txs,
        previous_tail.len(),
        nonce_recent_blockhashes,
        archive_path.display(),
        registry_path.display(),
        poh_path.display(),
        blockhash_registry_path.display()
    );
    Ok(())
}

fn copy_required_sidecar(input_dir: &Path, output_dir: &Path, file_name: &str) -> Result<PathBuf> {
    let source = input_dir.join(file_name);
    anyhow::ensure!(
        crate::file_nonempty(&source),
        "required Archive V2 sidecar missing or empty: {}",
        source.display()
    );
    let dest = output_dir.join(file_name);
    if source != dest {
        std::fs::copy(&source, &dest)
            .with_context(|| format!("copy {} to {}", source.display(), dest.display()))?;
    }
    Ok(dest)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreviousBlockhash {
    hash: [u8; 32],
    slot: u64,
}

fn load_or_build_previous_tail(
    output_dir: &Path,
    previous_car: Option<&Path>,
    resume: bool,
) -> Result<Vec<PreviousBlockhash>> {
    let Some(previous_car) = previous_car else {
        return Ok(Vec::new());
    };

    if resume && let Some(tail) = read_prev_blockhash_tail(output_dir)? {
        info!(
            "Reusing previous blockhash tail: tail={} path={}",
            tail.len(),
            output_dir.join(PREV_BLOCKHASH_TAIL_FILE).display()
        );
        return Ok(tail);
    }

    if let Some(previous_epoch) = parse_epoch_from_path(previous_car) {
        if let Some(sidecar_dir) = find_previous_epoch_sidecar_dir(output_dir, previous_epoch)? {
            match read_blockhash_tail_from_sidecars(&sidecar_dir, ROLLING_BLOCKHASH_CAPACITY) {
                Ok(tail) => {
                    write_prev_blockhash_tail(output_dir, &tail)?;
                    info!(
                        "Loaded previous blockhash tail from sidecars: epoch={} tail={} dir={}",
                        previous_epoch,
                        tail.len(),
                        sidecar_dir.display()
                    );
                    return Ok(tail);
                }
                Err(error) => warn!(
                    "Previous epoch sidecars are incomplete or invalid; falling back to CAR: epoch={} dir={} error={:#}",
                    previous_epoch,
                    sidecar_dir.display(),
                    error
                ),
            }
        }
    }

    let tail = read_blockhash_tail_from_car(previous_car, ROLLING_BLOCKHASH_CAPACITY)
        .with_context(|| {
            format!(
                "read previous blockhash tail from {}",
                previous_car.display()
            )
        })?;
    write_prev_blockhash_tail(output_dir, &tail)?;
    Ok(tail)
}

fn parse_epoch_from_path(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_string_lossy();
    let (_, after) = name.split_once("epoch-")?;
    let digits: String = after.chars().take_while(|ch| ch.is_ascii_digit()).collect();
    (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
}

fn find_previous_epoch_sidecar_dir(
    output_dir: &Path,
    previous_epoch: u64,
) -> Result<Option<PathBuf>> {
    let Some(parent) = output_dir.parent() else {
        return Ok(None);
    };
    let marker = format!("epoch-{previous_epoch}");
    let canonical = parent.join(&marker);
    if has_blockhash_seed_sidecars(&canonical) {
        return Ok(Some(canonical));
    }

    let mut candidates = Vec::new();
    for entry in
        std::fs::read_dir(parent).with_context(|| format!("read dir {}", parent.display()))?
    {
        let entry = entry.with_context(|| format!("read dir entry from {}", parent.display()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == marker || !matches_epoch_dir_name(&name, &marker) {
            continue;
        }
        if has_completed_fuzzy_blockhash_seed_sidecars(&path) {
            candidates.push((name.to_string(), path));
        }
    }
    candidates.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(candidates.pop().map(|(_, path)| path))
}

fn matches_epoch_dir_name(name: &str, marker: &str) -> bool {
    let Some(rest) = name.strip_prefix(marker) else {
        return false;
    };
    rest.is_empty() || rest.starts_with('-')
}

fn has_blockhash_seed_sidecars(sidecar_dir: &Path) -> bool {
    crate::file_nonempty(&sidecar_dir.join(BLOCKHASH_REGISTRY_FILE))
        && (crate::file_nonempty(&sidecar_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE))
            || crate::file_nonempty(&sidecar_dir.join(POH_FILE)))
}

fn has_completed_fuzzy_blockhash_seed_sidecars(sidecar_dir: &Path) -> bool {
    has_blockhash_seed_sidecars(sidecar_dir)
        && crate::file_nonempty(&sidecar_dir.join(ARCHIVE_V2_META_FILE))
        && crate::file_nonempty(&sidecar_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE))
}

#[cfg(test)]
mod previous_tail_selection_tests {
    use super::*;

    fn write_required_sidecars(path: &Path, completion_meta: bool) {
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(path.join(BLOCKHASH_REGISTRY_FILE), [1]).unwrap();
        std::fs::write(path.join(ARCHIVE_V2_BLOCK_INDEX_FILE), [1]).unwrap();
        if completion_meta {
            std::fs::write(path.join(ARCHIVE_V2_META_FILE), [1]).unwrap();
        }
    }

    #[test]
    fn canonical_sidecars_win_and_fuzzy_candidates_require_completion() {
        let root = std::env::temp_dir().join(format!(
            "blockzilla-previous-tail-selection-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let output = root.join("epoch-855-candidate");
        let canonical = root.join("epoch-854");
        let completed_fuzzy = root.join("epoch-854-complete");
        let incomplete_fuzzy = root.join("epoch-854-zz-incomplete");
        std::fs::create_dir_all(&output).unwrap();
        write_required_sidecars(&canonical, false);
        write_required_sidecars(&completed_fuzzy, true);
        write_required_sidecars(&incomplete_fuzzy, false);

        assert_eq!(
            find_previous_epoch_sidecar_dir(&output, 854).unwrap(),
            Some(canonical.clone())
        );

        std::fs::remove_file(canonical.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        assert_eq!(
            find_previous_epoch_sidecar_dir(&output, 854).unwrap(),
            Some(completed_fuzzy.clone())
        );

        std::fs::remove_file(completed_fuzzy.join(ARCHIVE_V2_META_FILE)).unwrap();
        assert_eq!(find_previous_epoch_sidecar_dir(&output, 854).unwrap(), None);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn invalid_canonical_sidecars_fall_back_to_previous_car() {
        let root = std::env::temp_dir().join(format!(
            "blockzilla-previous-tail-fallback-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let output = root.join("epoch-823");
        let invalid_canonical = root.join("epoch-822");
        std::fs::create_dir_all(&output).unwrap();
        write_required_sidecars(&invalid_canonical, false);
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../crates/old-faithful/car-reader/benches/fixtures/epoch-822-biggest.car");

        let tail = load_or_build_previous_tail(&output, Some(&fixture), true).unwrap();
        assert!(!tail.is_empty());
        assert!(output.join(PREV_BLOCKHASH_TAIL_FILE).is_file());
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn read_blockhash_tail_from_sidecars(
    sidecar_dir: &Path,
    max_entries: usize,
) -> Result<Vec<PreviousBlockhash>> {
    let blockhashes = load_blockhash_registry_plain(&sidecar_dir.join(BLOCKHASH_REGISTRY_FILE))?;
    let hot_index_path = sidecar_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    if crate::file_nonempty(&hot_index_path) {
        return read_blockhash_tail_from_hot_index(&blockhashes, &hot_index_path, max_entries);
    }
    read_blockhash_tail_from_poh_sidecar(&blockhashes, &sidecar_dir.join(POH_FILE), max_entries)
}

fn read_blockhash_tail_from_hot_index(
    blockhashes: &[[u8; 32]],
    index_path: &Path,
    max_entries: usize,
) -> Result<Vec<PreviousBlockhash>> {
    let index = read_archive_v2_hot_block_index(index_path)?;
    if index.rows.is_empty() {
        return Ok(Vec::new());
    }
    let offset = infer_blockhash_registry_offset(blockhashes.len(), index.rows.len())?;
    let start = index.rows.len().saturating_sub(max_entries);
    let mut tail = Vec::with_capacity(index.rows.len() - start);
    for row in &index.rows[start..] {
        let hash_index = row.block_id as usize + offset;
        let hash = *blockhashes.get(hash_index).with_context(|| {
            format!(
                "{} references missing blockhash index {} for block_id {}",
                index_path.display(),
                hash_index,
                row.block_id
            )
        })?;
        tail.push(PreviousBlockhash {
            hash,
            slot: row.slot,
        });
    }
    Ok(tail)
}

fn read_blockhash_tail_from_poh_sidecar(
    blockhashes: &[[u8; 32]],
    poh_path: &Path,
    max_entries: usize,
) -> Result<Vec<PreviousBlockhash>> {
    let file = File::open(poh_path).with_context(|| format!("open {}", poh_path.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let mut tail = VecDeque::with_capacity(max_entries);
    let mut rows = 0usize;
    while let Some((_len, record)) = reader.read::<WincodeArchiveV2PohRecord>()? {
        rows += 1;
        tail.push_back((record.block_id, record.slot));
        while tail.len() > max_entries {
            tail.pop_front();
        }
    }
    let offset = infer_blockhash_registry_offset(blockhashes.len(), rows)?;
    tail.into_iter()
        .map(|(block_id, slot)| {
            let hash_index = block_id as usize + offset;
            let hash = *blockhashes.get(hash_index).with_context(|| {
                format!(
                    "{} references missing blockhash index {} for block_id {}",
                    poh_path.display(),
                    hash_index,
                    block_id
                )
            })?;
            Ok(PreviousBlockhash { hash, slot })
        })
        .collect()
}

fn infer_blockhash_registry_offset(blockhash_count: usize, block_count: usize) -> Result<usize> {
    if blockhash_count == block_count {
        Ok(0)
    } else if blockhash_count == block_count + 1 {
        Ok(1)
    } else {
        anyhow::bail!(
            "blockhash registry count {} does not match block count {} with supported genesis offsets",
            blockhash_count,
            block_count
        )
    }
}

fn read_blockhash_tail_from_car(
    input: &Path,
    max_entries: usize,
) -> Result<Vec<PreviousBlockhash>> {
    let mut scanner = RawCarScanner::open_with_buffer(input, BLOCKHASH_SCAN_BUFFER_SIZE)?;
    scanner.skip_header()?;
    let mut tail: VecDeque<PreviousBlockhash> = VecDeque::with_capacity(max_entries);
    let mut progress = ProgressTracker::new("Prev Blockhash Seed");
    let mut latest_entry_hash: Option<[u8; 32]> = None;
    let mut blocks = 0u64;
    let mut entry_nodes = 0u64;
    let started = Instant::now();

    while let Some(raw) = scanner.next_blockhash_node()? {
        if is_entry_node(raw.prefix) {
            let hash = decode_entry_hash(raw.prefix)
                .map_err(|err| anyhow!("{err}"))
                .with_context(|| format!("entry {} decode hash", raw.cid))?;
            let hash = hash_32(hash).with_context(|| format!("entry {} hash", raw.cid))?;
            latest_entry_hash = Some(hash);
            entry_nodes += 1;
        } else if is_block_node(raw.prefix) {
            let (slot, _) = decode_block_slot_time(raw.prefix)
                .with_context(|| format!("previous seed block #{blocks} decode block slot/time"))?;
            let blockhash = latest_entry_hash.ok_or_else(|| {
                anyhow!("previous seed slot {slot} block #{blocks} has no latest Entry hash")
            })?;
            tail.push_back(PreviousBlockhash {
                hash: blockhash,
                slot,
            });
            while tail.len() > max_entries {
                tail.pop_front();
            }
            blocks += 1;
            progress.update_slot(slot);
            progress.update(1, 0);
        }
    }

    progress.final_report();
    info!(
        "Previous blockhash seed complete in {:.2}s: blocks={} entry_nodes={} tail={} input={}",
        started.elapsed().as_secs_f64(),
        blocks,
        entry_nodes,
        tail.len(),
        input.display()
    );
    Ok(tail.into_iter().collect())
}

fn read_prev_blockhash_tail(output_dir: &Path) -> Result<Option<Vec<PreviousBlockhash>>> {
    let path = output_dir.join(PREV_BLOCKHASH_TAIL_FILE);
    let Ok(mut file) = File::open(&path) else {
        return Ok(None);
    };
    let len = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    if len == 0 {
        return Ok(None);
    }
    if len % 40 != 0 {
        if len % 32 == 0 {
            info!(
                "Ignoring legacy hash-only previous blockhash tail without slots: {}",
                path.display()
            );
            return Ok(None);
        }
        anyhow::bail!(
            "malformed previous blockhash tail {}: length {} is not a multiple of 40",
            path.display(),
            len
        );
    }

    let mut tail = Vec::with_capacity((len / 40) as usize);
    let mut row = [0u8; 40];
    loop {
        match file.read_exact(&mut row) {
            Ok(()) => {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&row[..32]);
                let slot = u64::from_le_bytes(row[32..40].try_into().unwrap());
                tail.push(PreviousBlockhash { hash, slot });
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
        }
    }
    Ok(Some(tail))
}

fn write_prev_blockhash_tail(output_dir: &Path, tail: &[PreviousBlockhash]) -> Result<()> {
    let path = output_dir.join(PREV_BLOCKHASH_TAIL_FILE);
    let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    for item in tail {
        writer
            .write_all(&item.hash)
            .with_context(|| format!("write {}", path.display()))?;
        writer
            .write_all(&item.slot.to_le_bytes())
            .with_context(|| format!("write {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn build_no_registry_pubkey_registry(input: &Path, registry_path: &Path) -> Result<()> {
    info!(
        "Building Archive V2 pubkey registry from {}",
        input.display()
    );
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let mut counter = ArchivePubkeyCounter::new(8_000_000);
    let mut progress = ProgressTracker::new("Archive V2 Registry");
    let started = Instant::now();
    let mut blocks = 0u64;
    let mut txs = 0u64;

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2NoRegistryRecord>()? {
        match record {
            WincodeArchiveV2NoRegistryRecord::Block(block) => {
                let tx_count = count_no_registry_block_pubkeys(&block, &mut counter)
                    .with_context(|| format!("slot {} count pubkeys", block.header.compact.slot))?;
                blocks += 1;
                txs += tx_count;
                progress.update_slot(block.header.compact.slot);
                progress.update(1, tx_count);
            }
            WincodeArchiveV2NoRegistryRecord::Header(header) => {
                anyhow::ensure!(
                    header.version == WINCODE_ARCHIVE_V2_VERSION,
                    "unsupported no-registry archive version {}",
                    header.version
                );
            }
            WincodeArchiveV2NoRegistryRecord::Genesis(genesis) => {
                count_no_registry_genesis_pubkeys(&genesis, &mut counter);
            }
            WincodeArchiveV2NoRegistryRecord::Index(_)
            | WincodeArchiveV2NoRegistryRecord::Footer(_) => {}
        }
    }

    progress.final_report();
    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });

    const BUILTIN_PROGRAM_KEYS: &[Pubkey] = &[solana_pubkey::pubkey!(
        "ComputeBudget111111111111111111111111111111"
    )];
    let missing_builtins = BUILTIN_PROGRAM_KEYS
        .iter()
        .map(|key| key.to_bytes())
        .filter(|builtin| !items.iter().any(|(key, _)| key == builtin))
        .collect::<Vec<_>>();
    let registry_len = missing_builtins.len() + items.len();

    write_registry_iter(
        registry_path,
        missing_builtins
            .iter()
            .copied()
            .chain(items.iter().map(|(key, _)| *key)),
    )
    .with_context(|| format!("write {}", registry_path.display()))?;
    info!(
        "Archive V2 pubkey registry built in {:.2}s: blocks={} txs={} keys={} path={}",
        started.elapsed().as_secs_f64(),
        blocks,
        txs,
        registry_len,
        registry_path.display()
    );
    Ok(())
}

fn count_no_registry_block_pubkeys(
    block: &WincodeArchiveV2NoRegistryBlock,
    counter: &mut ArchivePubkeyCounter,
) -> Result<u64> {
    if let Some(rewards) = &block.header.rewards {
        count_no_registry_rewards_pubkeys(rewards, counter)?;
    }

    for (tx_index, tx) in block.txs.iter().enumerate() {
        let WincodeArchiveV2Payload::Decoded { value, .. } = &tx.tx else {
            anyhow::bail!(
                "slot {} tx#{tx_index} raw transaction payload in no-registry archive",
                block.header.compact.slot
            );
        };
        count_no_registry_tx_pubkeys(value, counter);

        if let Some(metadata) = &tx.metadata {
            let WincodeArchiveV2Payload::Decoded { value, .. } = metadata else {
                anyhow::bail!(
                    "slot {} tx#{tx_index} raw metadata payload in no-registry archive",
                    block.header.compact.slot
                );
            };
            count_no_registry_meta_pubkeys(value, counter);
        }
    }

    Ok(block.txs.len() as u64)
}

fn count_no_registry_tx_pubkeys(
    tx: &WincodeArchiveV2NoRegistryTx,
    counter: &mut ArchivePubkeyCounter,
) {
    match &tx.message {
        WincodeArchiveV2NoRegistryMessage::Legacy(message) => {
            for key in &message.account_keys {
                counter.add32(key);
            }
        }
        WincodeArchiveV2NoRegistryMessage::V0(message) => {
            for key in &message.account_keys {
                counter.add32(key);
            }
            for lookup in &message.address_table_lookups {
                counter.add32(&lookup.account_key);
            }
        }
    }
}

fn count_no_registry_genesis_pubkeys(
    genesis: &WincodeArchiveV2NoRegistryGenesis,
    counter: &mut ArchivePubkeyCounter,
) {
    for account in genesis.accounts.iter().chain(genesis.reward_pools.iter()) {
        counter.add32(&account.pubkey);
        counter.add32(&account.owner);
    }
    for builtin in &genesis.builtins {
        counter.add32(&builtin.pubkey);
    }
}

fn count_no_registry_meta_pubkeys(
    meta: &WincodeArchiveV2NoRegistryMeta,
    counter: &mut ArchivePubkeyCounter,
) {
    for key in &meta.loaded_writable_addresses {
        counter.add32(key);
    }
    for key in &meta.loaded_readonly_addresses {
        counter.add32(key);
    }
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if let Some(key) = &balance.mint {
            counter.add32(key);
        }
        if let Some(key) = &balance.owner {
            counter.add32(key);
        }
        if let Some(key) = &balance.program_id {
            counter.add32(key);
        }
    }
    for reward in &meta.rewards {
        counter.add32(&reward.pubkey);
    }
    if let Some(return_data) = &meta.return_data {
        counter.add32(&return_data.program_id);
    }
}

fn count_no_registry_rewards_pubkeys(
    rewards: &WincodeArchiveV2NoRegistryRewards,
    counter: &mut ArchivePubkeyCounter,
) -> Result<()> {
    anyhow::ensure!(
        rewards.raw_fallback.is_none() && rewards.decode_error.is_none(),
        "raw rewards fallback in no-registry archive"
    );
    let Some(decoded) = &rewards.decoded else {
        anyhow::bail!("rewards record present without decoded rewards");
    };
    for reward in decoded {
        counter.add32(&reward.pubkey);
    }
    Ok(())
}

fn optimize_no_registry_block(
    block: WincodeArchiveV2NoRegistryBlock,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    block_id: u32,
    nonce_recent_blockhashes: &mut u64,
) -> Result<(WincodeArchiveV2Block, u32)> {
    let slot = block.header.compact.slot;
    let rewards = block
        .header
        .rewards
        .as_ref()
        .map(|rewards| optimize_no_registry_rewards(rewards, key_index))
        .transpose()?;
    let mut compact = block.header.compact;
    compact.blockhash = block_id;
    compact.previous_blockhash = block_id.saturating_sub(1);
    compact.poh_entries.clear();
    compact.rewards = None;

    let mut txs = Vec::with_capacity(block.txs.len());
    for (tx_index, tx) in block.txs.into_iter().enumerate() {
        txs.push(optimize_no_registry_transaction(
            slot,
            tx_index,
            tx,
            key_index,
            rolling_blockhashes,
            nonce_recent_blockhashes,
        )?);
    }
    let tx_count = u32::try_from(txs.len()).context("transaction count exceeds u32::MAX")?;

    Ok((
        WincodeArchiveV2Block {
            header: WincodeArchiveV2BlockHeader { compact, rewards },
            txs,
        },
        tx_count,
    ))
}

fn optimize_no_registry_transaction(
    slot: u64,
    tx_index: usize,
    tx: WincodeArchiveV2NoRegistryTransaction,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    nonce_recent_blockhashes: &mut u64,
) -> Result<WincodeArchiveV2Transaction> {
    let tx_payload = match tx.tx {
        WincodeArchiveV2Payload::Decoded { source_len, value } => {
            WincodeArchiveV2Payload::Decoded {
                source_len,
                value: optimize_no_registry_tx(
                    slot,
                    tx_index,
                    value,
                    key_index,
                    rolling_blockhashes,
                    nonce_recent_blockhashes,
                )?,
            }
        }
        WincodeArchiveV2Payload::Raw { error, .. } => {
            anyhow::bail!("slot {slot} tx#{tx_index} raw transaction payload: {error}");
        }
    };

    let metadata = tx
        .metadata
        .map(
            |payload| -> Result<WincodeArchiveV2Payload<blockzilla_format::CompactMetaV1>> {
                match payload {
                    WincodeArchiveV2Payload::Decoded { source_len, value } => {
                        Ok(WincodeArchiveV2Payload::Decoded {
                            source_len,
                            value: optimize_no_registry_meta(
                                slot, tx_index, value, key_index, None,
                            )?,
                        })
                    }
                    WincodeArchiveV2Payload::Raw { error, .. } => {
                        anyhow::bail!("slot {slot} tx#{tx_index} raw metadata payload: {error}");
                    }
                }
            },
        )
        .transpose()?;

    Ok(WincodeArchiveV2Transaction {
        tx_index: tx.tx_index,
        tx: tx_payload,
        metadata,
    })
}

fn optimize_no_registry_tx(
    slot: u64,
    tx_index: usize,
    tx: WincodeArchiveV2NoRegistryTx,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    nonce_recent_blockhashes: &mut u64,
) -> Result<OwnedCompactTransaction> {
    let message = optimize_no_registry_message(
        slot,
        tx_index,
        tx.message,
        key_index,
        rolling_blockhashes,
        nonce_recent_blockhashes,
    )?;

    Ok(OwnedCompactTransaction {
        signatures: tx.signatures,
        message,
    })
}

fn optimize_no_registry_message(
    slot: u64,
    tx_index: usize,
    message: WincodeArchiveV2NoRegistryMessage,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    nonce_recent_blockhashes: &mut u64,
) -> Result<OwnedCompactMessage> {
    Ok(match message {
        WincodeArchiveV2NoRegistryMessage::Legacy(message) => {
            OwnedCompactMessage::Legacy(OwnedCompactLegacyMessage {
                header: message.header,
                account_keys: compact_required_keys(
                    key_index,
                    &message.account_keys,
                    "transaction account key",
                )?,
                recent_blockhash: resolve_recent_blockhash(
                    rolling_blockhashes,
                    &message.recent_blockhash,
                    slot,
                    tx_index,
                    nonce_recent_blockhashes,
                )?,
                instructions: message
                    .instructions
                    .into_iter()
                    .map(owned_no_registry_instruction)
                    .collect(),
            })
        }
        WincodeArchiveV2NoRegistryMessage::V0(message) => {
            OwnedCompactMessage::V0(OwnedCompactV0Message {
                header: message.header,
                account_keys: compact_required_keys(
                    key_index,
                    &message.account_keys,
                    "transaction account key",
                )?,
                recent_blockhash: resolve_recent_blockhash(
                    rolling_blockhashes,
                    &message.recent_blockhash,
                    slot,
                    tx_index,
                    nonce_recent_blockhashes,
                )?,
                instructions: message
                    .instructions
                    .into_iter()
                    .map(owned_no_registry_instruction)
                    .collect(),
                address_table_lookups: message
                    .address_table_lookups
                    .into_iter()
                    .map(|lookup| optimize_no_registry_lookup(lookup, key_index))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
    })
}

fn optimize_no_registry_lookup(
    lookup: WincodeArchiveV2NoRegistryAddressTableLookup,
    key_index: &KeyIndex,
) -> Result<OwnedCompactAddressTableLookup> {
    Ok(OwnedCompactAddressTableLookup {
        account_key: compact_required(key_index, &lookup.account_key, "address table lookup")?,
        writable_indexes: lookup.writable_indexes,
        readonly_indexes: lookup.readonly_indexes,
    })
}

fn owned_no_registry_instruction(
    instruction: WincodeArchiveV2NoRegistryInstruction,
) -> OwnedCompactInstruction {
    OwnedCompactInstruction {
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts,
        data: instruction.data,
    }
}

fn optimize_no_registry_meta(
    _slot: u64,
    _tx_index: usize,
    meta: WincodeArchiveV2NoRegistryMeta,
    key_index: &KeyIndex,
    mut timings: Option<&mut ArchiveV2Timings>,
) -> Result<blockzilla_format::CompactMetaV1> {
    let logs_started = ArchiveV2Timings::optional_detail_timer(timings.as_deref());
    let logs = meta
        .logs
        .map(|logs| decode_no_registry_logs(logs, _slot, _tx_index, key_index))
        .transpose()?
        .map(|logs| logs);
    if let Some(timings) = timings.as_mut() {
        timings.metadata_logs_decode_compact += logs_started.elapsed();
    }

    let pubkey_started = ArchiveV2Timings::optional_detail_timer(timings.as_deref());
    let pre_token_balances = meta
        .pre_token_balances
        .into_iter()
        .map(|balance| optimize_no_registry_token_balance(balance, key_index))
        .collect::<Result<Vec<_>>>()?;
    let post_token_balances = meta
        .post_token_balances
        .into_iter()
        .map(|balance| optimize_no_registry_token_balance(balance, key_index))
        .collect::<Result<Vec<_>>>()?;
    let rewards = meta
        .rewards
        .into_iter()
        .map(|reward| optimize_no_registry_reward(&reward, key_index))
        .collect::<Result<Vec<_>>>()?;
    let loaded_writable_addresses = meta
        .loaded_writable_addresses
        .into_iter()
        .map(|key| compact_required(key_index, &key, "loaded writable address"))
        .collect::<Result<Vec<_>>>()?;
    let loaded_readonly_addresses = meta
        .loaded_readonly_addresses
        .into_iter()
        .map(|key| compact_required(key_index, &key, "loaded readonly address"))
        .collect::<Result<Vec<_>>>()?;
    let return_data = meta
        .return_data
        .map(|return_data| {
            Ok::<CompactReturnData, anyhow::Error>(CompactReturnData {
                program_id: compact_required(
                    key_index,
                    &return_data.program_id,
                    "return data program id",
                )?,
                data: return_data.data,
            })
        })
        .transpose()?;
    if let Some(timings) = timings.as_mut() {
        timings.metadata_pubkey_compact += pubkey_started.elapsed();
    }

    Ok(blockzilla_format::CompactMetaV1 {
        err: meta.err,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions: meta.inner_instructions,
        logs,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_writable_addresses,
        loaded_readonly_addresses,
        return_data,
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

fn decode_no_registry_logs(
    logs: WincodeArchiveV2NoRegistryLogs,
    slot: u64,
    tx_index: usize,
    key_index: &KeyIndex,
) -> Result<CompactLogStream> {
    match logs {
        WincodeArchiveV2NoRegistryLogs::Raw(logs) => {
            Ok(blockzilla_format::parse_logs(&logs, key_index))
        }
        WincodeArchiveV2NoRegistryLogs::WincodeZstd {
            uncompressed_len,
            bytes,
        } => {
            let decoded = zstd::stream::decode_all(bytes.as_slice())
                .with_context(|| format!("slot {slot} tx#{tx_index} live log zstd"))?;
            if decoded.len() as u64 != uncompressed_len {
                bail!(
                    "slot {slot} tx#{tx_index} live log zstd decoded length {} != expected {uncompressed_len}",
                    decoded.len()
                );
            }
            let logs: Vec<String> = wincode::config::deserialize(&decoded, wincode_leb128_config())
                .with_context(|| format!("slot {slot} tx#{tx_index} live log strings"))?;
            Ok(blockzilla_format::parse_logs(&logs, key_index))
        }
        WincodeArchiveV2NoRegistryLogs::Compact(logs) => Ok(rekey_compact_logs(logs, key_index)),
    }
}

fn rekey_compact_logs(mut logs: CompactLogStream, key_index: &KeyIndex) -> CompactLogStream {
    for event in &mut logs.events {
        rekey_log_event(event, key_index);
    }
    logs
}

fn rekey_log_event(event: &mut LogEvent, key_index: &KeyIndex) {
    match event {
        LogEvent::LoaderUpgradedProgram { program }
        | LogEvent::Invoke { program, .. }
        | LogEvent::BpfInvoke { program }
        | LogEvent::Consumed { program, .. }
        | LogEvent::Success { program }
        | LogEvent::BpfSuccess { program }
        | LogEvent::Failure { program, .. }
        | LogEvent::BpfFailure { program, .. }
        | LogEvent::FailureCustomProgramError { program, .. }
        | LogEvent::BpfFailureCustomProgramError { program, .. }
        | LogEvent::FailureInvalidAccountData { program }
        | LogEvent::BpfFailureInvalidAccountData { program }
        | LogEvent::FailureInvalidProgramArgument { program }
        | LogEvent::BpfFailureInvalidProgramArgument { program }
        | LogEvent::Return { program, .. } => {
            rekey_compact_pubkey(program, key_index);
        }
        LogEvent::ProgramIdLog { program, log } => {
            rekey_compact_pubkey(program, key_index);
            rekey_program_log(log, key_index);
        }
        LogEvent::LoaderFinalizedAccount { account }
        | LogEvent::RuntimeWritablePrivilegeEscalated { account }
        | LogEvent::RuntimeSignerPrivilegeEscalated { account }
        | LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => {
            rekey_compact_pubkey(account, key_index);
        }
        LogEvent::ProgramNotDeployed { program } | LogEvent::ProgramNotCached { program } => {
            if let Some(program) = program {
                rekey_compact_pubkey(program, key_index);
            }
        }
        LogEvent::System(log) => rekey_system_program_log(log, key_index),
        LogEvent::ProgramLog(log) | LogEvent::ProgramPlainLog(log) => {
            rekey_program_log(log, key_index);
        }
        LogEvent::ProgramLogError { .. }
        | LogEvent::ProgramAccountNotWritable
        | LogEvent::ProgramIdMismatch
        | LogEvent::ProgramNotUpgradeable
        | LogEvent::ProgramAndProgramDataAccountMismatch
        | LogEvent::ProgramWasExtendedInThisBlockAlready
        | LogEvent::BpfConsumed { .. }
        | LogEvent::FailedToComplete { .. }
        | LogEvent::CustomProgramError { .. }
        | LogEvent::Data { .. }
        | LogEvent::Consumption { .. }
        | LogEvent::CbRequestUnits { .. }
        | LogEvent::UnknownProgram { .. }
        | LogEvent::UnknownAccount { .. }
        | LogEvent::VerifyEd25519
        | LogEvent::VerifySecp256k1
        | LogEvent::LogTruncated
        | LogEvent::StakeMergingAccounts
        | LogEvent::CloseContextState
        | LogEvent::Plain { .. }
        | LogEvent::Unparsed { .. } => {}
    }
}

fn rekey_program_log(log: &mut ProgramLog, key_index: &KeyIndex) {
    match log {
        ProgramLog::Token2022(log) => rekey_token_2022_log(log, key_index),
        ProgramLog::Empty
        | ProgramLog::Token(_)
        | ProgramLog::Ata(_)
        | ProgramLog::AddressLookupTable(_)
        | ProgramLog::LoaderV3(_)
        | ProgramLog::LoaderV4(_)
        | ProgramLog::Memo(_)
        | ProgramLog::Record(_)
        | ProgramLog::TransferHook(_)
        | ProgramLog::AccountCompression(_)
        | ProgramLog::Stake(_)
        | ProgramLog::ZkElgamalProof(_)
        | ProgramLog::AnchorInstruction { .. }
        | ProgramLog::AnchorErrorOccurred { .. }
        | ProgramLog::AnchorErrorThrown { .. }
        | ProgramLog::Unknown(_) => {}
        #[cfg(feature = "known-program-logs")]
        ProgramLog::Known(_) => {}
    }
}

fn rekey_token_2022_log(log: &mut Token2022Log, key_index: &KeyIndex) {
    match log {
        Token2022Log::ErrorHarvestingFrom { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom2 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom3 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom4 { account_key, .. } => {
            rekey_compact_pubkey(account_key, key_index);
        }
        Token2022Log::Error(_)
        | Token2022Log::Static(_)
        | Token2022Log::CalculatedFee { .. }
        | Token2022Log::AccountNeedsResizePlusBytesDebug { .. }
        | Token2022Log::AccountNeedsResizePlusBytesDebug2 { .. } => {}
    }
}

fn rekey_system_program_log(log: &mut SystemProgramLog, key_index: &KeyIndex) {
    match log {
        SystemProgramLog::CreateAddressMismatch {
            provided_addr,
            derived_addr,
        }
        | SystemProgramLog::TransferFromAddressMismatch {
            provided_addr,
            derived_addr,
        } => {
            rekey_compact_pubkey(provided_addr, key_index);
            rekey_pubkey_or_string(derived_addr, key_index);
        }
        SystemProgramLog::CreateAccountAlreadyInUse { addr }
        | SystemProgramLog::AllocateAlreadyInUse { addr }
        | SystemProgramLog::AllocateToMustSign { addr }
        | SystemProgramLog::AllocateAccountAlreadyInUse { addr }
        | SystemProgramLog::AssignAccountMustSign { addr }
        | SystemProgramLog::CreateAccountAccountAlreadyInUse { addr } => {
            rekey_system_address(addr, key_index);
        }
        SystemProgramLog::TransferFromMustSign { from } => {
            rekey_compact_pubkey(from, key_index);
        }
        SystemProgramLog::NonceAccountMustBeWriteable { account, .. }
        | SystemProgramLog::NonceAccountMustBeSigner { account, .. }
        | SystemProgramLog::NonceAccountMustSign { account, .. }
        | SystemProgramLog::NonceAccountStateInvalid { account, .. } => {
            rekey_pubkey_or_string(account, key_index);
        }
        SystemProgramLog::Instruction(_)
        | SystemProgramLog::AllocateRequestedTooLarge { .. }
        | SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions { .. }
        | SystemProgramLog::TransferFromMustNotCarryData
        | SystemProgramLog::TransferInsufficient { .. }
        | SystemProgramLog::AdvanceNonceRecentBlockhashesEmpty
        | SystemProgramLog::InitializeNonceRecentBlockhashesEmpty
        | SystemProgramLog::AuthorizeNonceAccount { .. }
        | SystemProgramLog::NonceInsufficientLamports { .. }
        | SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot { .. } => {}
    }
}

fn rekey_system_address(address: &mut SystemAddress, key_index: &KeyIndex) {
    match address {
        SystemAddress::Pubkey(pubkey) => rekey_pubkey_or_string(pubkey, key_index),
        SystemAddress::Debug { address, base } => {
            rekey_pubkey_or_string(address, key_index);
            if let Some(base) = base {
                rekey_pubkey_or_string(base, key_index);
            }
        }
    }
}

fn rekey_pubkey_or_string(value: &mut PubkeyOrString, key_index: &KeyIndex) {
    if let PubkeyOrString::Pubkey(pubkey) = value {
        rekey_compact_pubkey(pubkey, key_index);
    }
}

fn rekey_compact_pubkey(pubkey: &mut CompactPubkey, key_index: &KeyIndex) {
    let CompactPubkey::Raw(raw) = pubkey else {
        return;
    };
    if let Some(id) = key_index.lookup(raw) {
        *pubkey = CompactPubkey::id(id);
    }
}

fn optimize_no_registry_token_balance(
    balance: WincodeArchiveV2NoRegistryTokenBalance,
    key_index: &KeyIndex,
) -> Result<CompactTokenBalance> {
    Ok(CompactTokenBalance {
        account_index: balance.account_index,
        mint: balance
            .mint
            .map(|key| compact_required(key_index, &key, "token balance mint"))
            .transpose()?,
        owner: balance
            .owner
            .map(|key| compact_required(key_index, &key, "token balance owner"))
            .transpose()?,
        program_id: balance
            .program_id
            .map(|key| compact_required(key_index, &key, "token balance program id"))
            .transpose()?,
        amount: balance.amount,
        decimals: balance.decimals,
    })
}

fn optimize_no_registry_reward(
    reward: &WincodeArchiveV2NoRegistryReward,
    key_index: &KeyIndex,
) -> Result<CompactReward> {
    Ok(CompactReward {
        pubkey: compact_required(key_index, &reward.pubkey, "reward pubkey")?,
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: reward.commission,
    })
}

fn optimize_no_registry_rewards(
    rewards: &WincodeArchiveV2NoRegistryRewards,
    key_index: &KeyIndex,
) -> Result<WincodeArchiveV2Rewards> {
    anyhow::ensure!(
        rewards.raw_fallback.is_none() && rewards.decode_error.is_none(),
        "raw rewards fallback in no-registry archive"
    );
    let Some(decoded) = &rewards.decoded else {
        anyhow::bail!("rewards record present without decoded rewards");
    };
    Ok(WincodeArchiveV2Rewards {
        source_len: rewards.source_len,
        num_partitions: rewards.num_partitions,
        decoded: Some(
            decoded
                .iter()
                .map(|reward| optimize_no_registry_reward(reward, key_index))
                .collect::<Result<Vec<_>>>()?,
        ),
        raw_fallback: None,
        decode_error: None,
    })
}

fn compact_required_keys(
    key_index: &KeyIndex,
    keys: &[[u8; 32]],
    label: &str,
) -> Result<Vec<CompactPubkey>> {
    keys.iter()
        .map(|key| compact_required(key_index, key, label))
        .collect()
}

fn compact_required(key_index: &KeyIndex, key: &[u8; 32], label: &str) -> Result<CompactPubkey> {
    key_index
        .lookup(key)
        .map(CompactPubkey::id)
        .ok_or_else(|| anyhow!("pubkey registry missing {label}: {}", hex32(key)))
}

fn blockhash_id_offset_for_genesis(
    genesis: &Option<GenesisArchive>,
    blockhashes: &[[u8; 32]],
) -> Result<u32> {
    let Some(genesis) = genesis else {
        return Ok(0);
    };
    anyhow::ensure!(
        blockhashes.first() == Some(&genesis.genesis_hash),
        "epoch-0 blockhash registry does not start with genesis hash {}",
        hex32(&genesis.genesis_hash)
    );
    Ok(1)
}

fn compact_genesis_record(
    archive: &GenesisArchive,
    key_index: &KeyIndex,
) -> Result<WincodeArchiveV2Genesis> {
    let genesis = &archive.genesis;
    Ok(WincodeArchiveV2Genesis {
        genesis_hash: archive.genesis_hash,
        genesis_bin_len: archive.genesis_bin_len as u64,
        creation_time_unix: genesis.creation_time_unix,
        cluster_id: genesis.cluster_id,
        ticks_per_slot: genesis.ticks_per_slot,
        poh_params: WincodeArchiveV2GenesisPohParams {
            tick_duration_secs: genesis.poh_params.tick_duration_secs,
            tick_duration_nanos: genesis.poh_params.tick_duration_nanos,
            tick_count: genesis.poh_params.tick_count,
            hashes_per_tick: genesis.poh_params.hashes_per_tick,
        },
        fees: WincodeArchiveV2GenesisFeeParams {
            target_lamports_per_sig: genesis.fees.target_lamports_per_sig,
            target_sigs_per_slot: genesis.fees.target_sigs_per_slot,
            min_lamports_per_sig: genesis.fees.min_lamports_per_sig,
            max_lamports_per_sig: genesis.fees.max_lamports_per_sig,
            burn_percent: genesis.fees.burn_percent,
        },
        rent: WincodeArchiveV2GenesisRentParams {
            lamports_per_byte_year: genesis.rent.lamports_per_byte_year,
            exemption_threshold: genesis.rent.exemption_threshold,
            burn_percent: genesis.rent.burn_percent,
        },
        inflation: WincodeArchiveV2GenesisInflationParams {
            initial: genesis.inflation.initial,
            terminal: genesis.inflation.terminal,
            taper: genesis.inflation.taper,
            foundation: genesis.inflation.foundation,
            foundation_term: genesis.inflation.foundation_term,
            padding: genesis.inflation.padding,
        },
        epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
            slots_per_epoch: genesis.epoch_schedule.slots_per_epoch,
            leader_schedule_slot_offset: genesis.epoch_schedule.leader_schedule_slot_offset,
            warmup: genesis.epoch_schedule.warmup,
            first_normal_epoch: genesis.epoch_schedule.first_normal_epoch,
            first_normal_slot: genesis.epoch_schedule.first_normal_slot,
        },
        accounts: compact_genesis_accounts(&genesis.accounts, key_index, "genesis account")?,
        builtins: genesis
            .builtins
            .iter()
            .enumerate()
            .map(|(index, builtin)| {
                Ok(WincodeArchiveV2GenesisBuiltin {
                    key: builtin.key.clone(),
                    pubkey: compact_required(
                        key_index,
                        &builtin.pubkey,
                        &format!("genesis builtin #{index} pubkey"),
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        reward_pools: compact_genesis_accounts(
            &genesis.reward_pools,
            key_index,
            "genesis reward pool",
        )?,
    })
}

fn compact_genesis_accounts(
    accounts: &[GenesisAccountEntry],
    key_index: &KeyIndex,
    label: &str,
) -> Result<Vec<WincodeArchiveV2GenesisAccount>> {
    accounts
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            Ok(WincodeArchiveV2GenesisAccount {
                pubkey: compact_required(
                    key_index,
                    &entry.pubkey,
                    &format!("{label} #{index} pubkey"),
                )?,
                lamports: entry.account.lamports,
                owner: compact_required(
                    key_index,
                    &entry.account.owner,
                    &format!("{label} #{index} owner"),
                )?,
                executable: entry.account.executable,
                rent_epoch: entry.account.rent_epoch,
                data: entry.account.data.clone(),
            })
        })
        .collect()
}

fn no_registry_genesis_record(archive: &GenesisArchive) -> WincodeArchiveV2NoRegistryGenesis {
    let genesis = &archive.genesis;
    WincodeArchiveV2NoRegistryGenesis {
        genesis_hash: archive.genesis_hash,
        genesis_bin_len: archive.genesis_bin_len as u64,
        creation_time_unix: genesis.creation_time_unix,
        cluster_id: genesis.cluster_id,
        ticks_per_slot: genesis.ticks_per_slot,
        poh_params: WincodeArchiveV2GenesisPohParams {
            tick_duration_secs: genesis.poh_params.tick_duration_secs,
            tick_duration_nanos: genesis.poh_params.tick_duration_nanos,
            tick_count: genesis.poh_params.tick_count,
            hashes_per_tick: genesis.poh_params.hashes_per_tick,
        },
        fees: WincodeArchiveV2GenesisFeeParams {
            target_lamports_per_sig: genesis.fees.target_lamports_per_sig,
            target_sigs_per_slot: genesis.fees.target_sigs_per_slot,
            min_lamports_per_sig: genesis.fees.min_lamports_per_sig,
            max_lamports_per_sig: genesis.fees.max_lamports_per_sig,
            burn_percent: genesis.fees.burn_percent,
        },
        rent: WincodeArchiveV2GenesisRentParams {
            lamports_per_byte_year: genesis.rent.lamports_per_byte_year,
            exemption_threshold: genesis.rent.exemption_threshold,
            burn_percent: genesis.rent.burn_percent,
        },
        inflation: WincodeArchiveV2GenesisInflationParams {
            initial: genesis.inflation.initial,
            terminal: genesis.inflation.terminal,
            taper: genesis.inflation.taper,
            foundation: genesis.inflation.foundation,
            foundation_term: genesis.inflation.foundation_term,
            padding: genesis.inflation.padding,
        },
        epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
            slots_per_epoch: genesis.epoch_schedule.slots_per_epoch,
            leader_schedule_slot_offset: genesis.epoch_schedule.leader_schedule_slot_offset,
            warmup: genesis.epoch_schedule.warmup,
            first_normal_epoch: genesis.epoch_schedule.first_normal_epoch,
            first_normal_slot: genesis.epoch_schedule.first_normal_slot,
        },
        accounts: no_registry_genesis_accounts(&genesis.accounts),
        builtins: genesis
            .builtins
            .iter()
            .map(|builtin| WincodeArchiveV2NoRegistryGenesisBuiltin {
                key: builtin.key.clone(),
                pubkey: builtin.pubkey,
            })
            .collect(),
        reward_pools: no_registry_genesis_accounts(&genesis.reward_pools),
    }
}

fn no_registry_genesis_accounts(
    accounts: &[GenesisAccountEntry],
) -> Vec<WincodeArchiveV2NoRegistryGenesisAccount> {
    accounts
        .iter()
        .map(|entry| WincodeArchiveV2NoRegistryGenesisAccount {
            pubkey: entry.pubkey,
            lamports: entry.account.lamports,
            owner: entry.account.owner,
            executable: entry.account.executable,
            rent_epoch: entry.account.rent_epoch,
            data: entry.account.data.clone(),
        })
        .collect()
}

fn compact_no_registry_genesis(
    genesis: WincodeArchiveV2NoRegistryGenesis,
    key_index: &KeyIndex,
) -> Result<WincodeArchiveV2Genesis> {
    Ok(WincodeArchiveV2Genesis {
        genesis_hash: genesis.genesis_hash,
        genesis_bin_len: genesis.genesis_bin_len,
        creation_time_unix: genesis.creation_time_unix,
        cluster_id: genesis.cluster_id,
        ticks_per_slot: genesis.ticks_per_slot,
        poh_params: genesis.poh_params,
        fees: genesis.fees,
        rent: genesis.rent,
        inflation: genesis.inflation,
        epoch_schedule: genesis.epoch_schedule,
        accounts: compact_no_registry_genesis_accounts(
            genesis.accounts,
            key_index,
            "genesis account",
        )?,
        builtins: genesis
            .builtins
            .into_iter()
            .enumerate()
            .map(|(index, builtin)| {
                Ok(WincodeArchiveV2GenesisBuiltin {
                    key: builtin.key,
                    pubkey: compact_required(
                        key_index,
                        &builtin.pubkey,
                        &format!("genesis builtin #{index} pubkey"),
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        reward_pools: compact_no_registry_genesis_accounts(
            genesis.reward_pools,
            key_index,
            "genesis reward pool",
        )?,
    })
}

fn compact_no_registry_genesis_accounts(
    accounts: Vec<WincodeArchiveV2NoRegistryGenesisAccount>,
    key_index: &KeyIndex,
    label: &str,
) -> Result<Vec<WincodeArchiveV2GenesisAccount>> {
    accounts
        .into_iter()
        .enumerate()
        .map(|(index, account)| {
            Ok(WincodeArchiveV2GenesisAccount {
                pubkey: compact_required(
                    key_index,
                    &account.pubkey,
                    &format!("{label} #{index} pubkey"),
                )?,
                lamports: account.lamports,
                owner: compact_required(
                    key_index,
                    &account.owner,
                    &format!("{label} #{index} owner"),
                )?,
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: account.data,
            })
        })
        .collect()
}

fn resolve_recent_blockhash(
    rolling_blockhashes: &RollingBlockhashIndex,
    hash: &[u8; 32],
    slot: u64,
    tx_index: usize,
    nonce_recent_blockhashes: &mut u64,
) -> Result<OwnedCompactRecentBlockhash> {
    let resolved = rolling_blockhashes.resolve_or_nonce(hash, slot, tx_index)?;
    if matches!(resolved, OwnedCompactRecentBlockhash::Nonce(_)) {
        *nonce_recent_blockhashes += 1;
    }
    Ok(resolved)
}

struct RollingBlockhashIndex {
    max_entries: usize,
    map: GxHashMap<[u8; 32], (i32, u64)>,
    order: VecDeque<RollingBlockhashEntry>,
}

/// Staged Archive V2 stores recent blockhashes as producing block ids when they
/// are in the live blockhash window. Durable nonce values are stored inline as
/// raw 32-byte hashes and counted in the footer for later nonce-account checks.
#[derive(Clone, Copy)]
struct RollingBlockhashEntry {
    hash: [u8; 32],
    block_id: i32,
    slot: u64,
}

impl RollingBlockhashIndex {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            map: GxHashMap::with_capacity_and_hasher(max_entries, GxBuildHasher::default()),
            order: VecDeque::with_capacity(max_entries),
        }
    }

    fn seed_previous_tail(&mut self, tail: &[PreviousBlockhash]) -> Result<()> {
        for (index, item) in tail.iter().enumerate() {
            let distance_from_newest =
                i32::try_from(tail.len() - index).context("previous tail exceeds i32::MAX")?;
            self.insert(item.hash, -distance_from_newest, item.slot)?;
        }
        Ok(())
    }

    fn insert(&mut self, hash: [u8; 32], block_id: i32, slot: u64) -> Result<()> {
        anyhow::ensure!(
            !self.map.contains_key(&hash),
            "duplicate blockhash {} at block_id {} slot {}",
            hex32(&hash),
            block_id,
            slot
        );
        self.map.insert(hash, (block_id, slot));
        self.order.push_back(RollingBlockhashEntry {
            hash,
            block_id,
            slot,
        });
        self.enforce_capacity();
        Ok(())
    }

    fn prune_for_slot(&mut self, current_slot: u64) -> Result<()> {
        loop {
            let Some(entry) = self.order.front().copied() else {
                return Ok(());
            };
            anyhow::ensure!(
                entry.slot <= current_slot,
                "block slots went backwards: rolling blockhash slot {} is ahead of current slot {}",
                entry.slot,
                current_slot
            );
            if current_slot - entry.slot <= RECENT_BLOCKHASH_SLOT_WINDOW {
                return Ok(());
            }
            self.remove_front();
        }
    }

    fn enforce_capacity(&mut self) {
        while self.order.len() > self.max_entries {
            self.remove_front();
        }
    }

    fn remove_front(&mut self) {
        if let Some(old) = self.order.pop_front()
            && self.map.get(&old.hash).copied() == Some((old.block_id, old.slot))
        {
            self.map.remove(&old.hash);
        }
    }

    fn resolve_or_nonce(
        &self,
        hash: &[u8; 32],
        slot: u64,
        tx_index: usize,
    ) -> Result<OwnedCompactRecentBlockhash> {
        let Some((block_id, hash_slot)) = self.map.get(hash).copied() else {
            return Ok(OwnedCompactRecentBlockhash::Nonce(*hash));
        };
        anyhow::ensure!(
            hash_slot <= slot,
            "slot {slot} tx#{tx_index} recent_blockhash {} points at future block slot {}",
            hex32(hash),
            hash_slot
        );
        if slot - hash_slot > RECENT_BLOCKHASH_SLOT_WINDOW {
            return Ok(OwnedCompactRecentBlockhash::Nonce(*hash));
        }
        Ok(OwnedCompactRecentBlockhash::Id(block_id))
    }
}

struct ArchivePubkeyCounter {
    counts: GxHashMap<[u8; 32], u32>,
}

impl ArchivePubkeyCounter {
    fn new(capacity: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(capacity, GxBuildHasher::default()),
        }
    }

    fn add32(&mut self, key: &[u8; 32]) {
        let count = self.counts.entry(*key).or_insert(0);
        *count = count.saturating_add(1);
    }
}

const FIRST_SEEN_HASH_DOMAIN: u64 = 0x425a_4653_4931;
const FIRST_SEEN_COUNT_CHUNK_LEN: usize = 128;
const FIRST_SEEN_DIRECT_COUNT_IDS: usize = 65_536;

enum FirstSeenPromotedCountChunk {
    U16(Box<[u16; FIRST_SEEN_COUNT_CHUNK_LEN]>),
    U32(Box<[u32; FIRST_SEEN_COUNT_CHUNK_LEN]>),
}

/// Exact adaptive counters. The hot 65,536-ID prefix stays in a direct u32
/// array; later IDs cost one byte until their 128-ID chunk promotes to u16/u32.
struct FirstSeenCounts {
    direct: Vec<u32>,
    direct_limit: usize,
    base: Vec<u8>,
    promoted: Vec<Option<FirstSeenPromotedCountChunk>>,
    u16_chunks: usize,
    u32_chunks: usize,
}

impl FirstSeenCounts {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_direct_limit(capacity, FIRST_SEEN_DIRECT_COUNT_IDS)
    }

    fn with_capacity_and_direct_limit(capacity: usize, direct_limit: usize) -> Self {
        let direct_capacity = capacity.min(direct_limit);
        let adaptive_capacity = capacity.saturating_sub(direct_capacity);
        Self {
            direct: Vec::with_capacity(direct_capacity),
            direct_limit,
            base: Vec::with_capacity(adaptive_capacity),
            promoted: Vec::with_capacity(adaptive_capacity.div_ceil(FIRST_SEEN_COUNT_CHUNK_LEN)),
            u16_chunks: 0,
            u32_chunks: 0,
        }
    }

    fn len(&self) -> usize {
        self.direct.len() + self.base.len()
    }

    fn push(&mut self, count: u8) {
        if self.direct.len() < self.direct_limit {
            self.direct.push(u32::from(count));
            return;
        }
        let index = self.base.len();
        let chunk_index = index / FIRST_SEEN_COUNT_CHUNK_LEN;
        let chunk_offset = index % FIRST_SEEN_COUNT_CHUNK_LEN;
        if chunk_offset == 0 {
            self.promoted.push(None);
        }
        self.base.push(count);
        if let Some(promoted) = self.promoted[chunk_index].as_mut() {
            match promoted {
                FirstSeenPromotedCountChunk::U16(values) => {
                    values[chunk_offset] = u16::from(count);
                }
                FirstSeenPromotedCountChunk::U32(values) => {
                    values[chunk_offset] = u32::from(count);
                }
            }
        }
    }

    #[inline]
    fn get(&self, index: usize) -> u32 {
        if index < self.direct.len() {
            return self.direct[index];
        }
        let index = index - self.direct.len();
        let chunk_index = index / FIRST_SEEN_COUNT_CHUNK_LEN;
        let chunk_offset = index % FIRST_SEEN_COUNT_CHUNK_LEN;
        match self.promoted[chunk_index].as_ref() {
            Some(FirstSeenPromotedCountChunk::U16(values)) => u32::from(values[chunk_offset]),
            Some(FirstSeenPromotedCountChunk::U32(values)) => values[chunk_offset],
            None => u32::from(self.base[index]),
        }
    }

    #[inline]
    fn increment(&mut self, index: usize) -> Result<u32> {
        if index < self.direct.len() {
            self.direct[index] = self.direct[index]
                .checked_add(1)
                .context("first-seen direct reference count overflow")?;
            return Ok(self.direct[index]);
        }
        let index = index - self.direct.len();
        let chunk_index = index / FIRST_SEEN_COUNT_CHUNK_LEN;
        let chunk_offset = index % FIRST_SEEN_COUNT_CHUNK_LEN;
        if self.promoted[chunk_index].is_none() {
            let count = &mut self.base[index];
            if *count < u8::MAX {
                *count += 1;
                return Ok(u32::from(*count));
            }

            let chunk_start = chunk_index * FIRST_SEEN_COUNT_CHUNK_LEN;
            let chunk_end = self
                .base
                .len()
                .min(chunk_start + FIRST_SEEN_COUNT_CHUNK_LEN);
            let mut values = Box::new([0u16; FIRST_SEEN_COUNT_CHUNK_LEN]);
            for (target, &source) in values.iter_mut().zip(&self.base[chunk_start..chunk_end]) {
                *target = u16::from(source);
            }
            values[chunk_offset] = u16::from(u8::MAX) + 1;
            self.promoted[chunk_index] = Some(FirstSeenPromotedCountChunk::U16(values));
            self.u16_chunks += 1;
            return Ok(u32::from(u8::MAX) + 1);
        }

        let promoted = &mut self.promoted[chunk_index];
        match promoted.as_mut().expect("promoted count chunk exists") {
            FirstSeenPromotedCountChunk::U16(values) if values[chunk_offset] < u16::MAX => {
                values[chunk_offset] += 1;
                Ok(u32::from(values[chunk_offset]))
            }
            FirstSeenPromotedCountChunk::U16(_) => {
                let Some(FirstSeenPromotedCountChunk::U16(values)) = promoted.take() else {
                    unreachable!()
                };
                let mut widened = Box::new([0u32; FIRST_SEEN_COUNT_CHUNK_LEN]);
                for (target, source) in widened.iter_mut().zip(values.iter()) {
                    *target = u32::from(*source);
                }
                widened[chunk_offset] = u32::from(u16::MAX) + 1;
                *promoted = Some(FirstSeenPromotedCountChunk::U32(widened));
                self.u16_chunks -= 1;
                self.u32_chunks += 1;
                Ok(u32::from(u16::MAX) + 1)
            }
            FirstSeenPromotedCountChunk::U32(values) => {
                values[chunk_offset] = values[chunk_offset]
                    .checked_add(1)
                    .context("first-seen adaptive reference count overflow")?;
                Ok(values[chunk_offset])
            }
        }
    }

    fn iter(&self) -> FirstSeenCountsIter<'_> {
        FirstSeenCountsIter {
            counts: self,
            index: 0,
        }
    }

    fn estimated_heap_bytes(&self) -> usize {
        self.direct.capacity() * std::mem::size_of::<u32>()
            + self.base.capacity()
            + self.promoted.capacity() * std::mem::size_of::<Option<FirstSeenPromotedCountChunk>>()
            + self.u16_chunks * FIRST_SEEN_COUNT_CHUNK_LEN * std::mem::size_of::<u16>()
            + self.u32_chunks * FIRST_SEEN_COUNT_CHUNK_LEN * std::mem::size_of::<u32>()
    }

    #[cfg(test)]
    fn set_for_test(&mut self, index: usize, value: u32) {
        assert!(index < self.len());
        if index < self.direct.len() {
            self.direct[index] = value;
            return;
        }
        let index = index - self.direct.len();
        let chunk_index = index / FIRST_SEEN_COUNT_CHUNK_LEN;
        let chunk_offset = index % FIRST_SEEN_COUNT_CHUNK_LEN;
        let chunk_start = chunk_index * FIRST_SEEN_COUNT_CHUNK_LEN;
        let chunk_end = self
            .base
            .len()
            .min(chunk_start + FIRST_SEEN_COUNT_CHUNK_LEN);
        let mut exact = [0u32; FIRST_SEEN_COUNT_CHUNK_LEN];
        for (offset, target) in exact[..chunk_end - chunk_start].iter_mut().enumerate() {
            *target = self.get(chunk_start + offset);
        }
        exact[chunk_offset] = value;

        let was_u16 = matches!(
            self.promoted[chunk_index].as_ref(),
            Some(FirstSeenPromotedCountChunk::U16(_))
        );
        let was_u32 = matches!(
            self.promoted[chunk_index].as_ref(),
            Some(FirstSeenPromotedCountChunk::U32(_))
        );
        if was_u16 {
            self.u16_chunks -= 1;
        } else if was_u32 {
            self.u32_chunks -= 1;
        }

        if exact.iter().all(|&count| count <= u32::from(u8::MAX)) {
            for (offset, &count) in exact[..chunk_end - chunk_start].iter().enumerate() {
                self.base[chunk_start + offset] = count as u8;
            }
            self.promoted[chunk_index] = None;
        } else if exact.iter().all(|&count| count <= u32::from(u16::MAX)) {
            let mut values = Box::new([0u16; FIRST_SEEN_COUNT_CHUNK_LEN]);
            for (target, source) in values.iter_mut().zip(exact) {
                *target = source as u16;
            }
            self.promoted[chunk_index] = Some(FirstSeenPromotedCountChunk::U16(values));
            self.u16_chunks += 1;
        } else {
            self.promoted[chunk_index] = Some(FirstSeenPromotedCountChunk::U32(Box::new(exact)));
            self.u32_chunks += 1;
        }
    }
}

struct FirstSeenCountsIter<'a> {
    counts: &'a FirstSeenCounts,
    index: usize,
}

impl Iterator for FirstSeenCountsIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.counts.len() {
            return None;
        }
        let count = self.counts.get(self.index);
        self.index += 1;
        Some(count)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.counts.len() - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for FirstSeenCountsIter<'_> {}

/// Low-memory mutable pubkey interner for the one-pass hot builder.
///
/// The table stores zero-based IDs and compares keyed 128-bit fingerprints.
/// Full keys are streamed once to disk. A whole-reference checksum is verified
/// against that exact registry before any completion marker is published, so a
/// fingerprint alias makes the candidate fail and requires reprocessing (or an
/// exact-key fallback).
struct FirstSeenRegistry {
    table: HashTable<u32>,
    overflow_table: Option<HashTable<u32>>,
    fingerprints: Vec<u128>,
    counts: FirstSeenCounts,
    key_writer: BufWriter<File>,
    hash_seed: i64,
    seed_keys: Vec<[u8; 32]>,
    seed_hasher: Sha256,
    seeded_keys: usize,
    references: u64,
    observed_audit: FirstSeenReferenceAudit,
}

impl FirstSeenRegistry {
    fn new(capacity: usize, registry_path: &Path) -> Result<Self> {
        let hash_seed =
            std::collections::hash_map::RandomState::new().hash_one(FIRST_SEEN_HASH_DOMAIN) as i64;
        let key_file = File::create(registry_path)
            .with_context(|| format!("create {}", registry_path.display()))?;
        Ok(Self {
            // Half the expected unique-key hint selects the Swiss-table plateau
            // below the old monolithic allocation. For the normal 34M hint the
            // primary accepts 29,360,128 rows (~160 MiB); overflow is lazy.
            table: HashTable::with_capacity(capacity.div_ceil(2)),
            overflow_table: None,
            fingerprints: Vec::with_capacity(capacity),
            counts: FirstSeenCounts::with_capacity(capacity),
            key_writer: BufWriter::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, key_file),
            hash_seed,
            seed_keys: Vec::with_capacity(FIRST_SEEN_SEED_READ_BUFFER_MIN / 32),
            seed_hasher: Sha256::new(),
            seeded_keys: 0,
            references: 0,
            observed_audit: FirstSeenReferenceAudit::default(),
        })
    }

    #[inline]
    fn fingerprint(&self, key: &[u8; 32]) -> u128 {
        gxhash128(key, self.hash_seed)
    }

    #[inline]
    fn table_hash(fingerprint: u128) -> u64 {
        fingerprint as u64
    }

    #[inline]
    fn lookup(&self, key: &[u8; 32]) -> Option<u32> {
        self.lookup_fingerprint(self.fingerprint(key))
    }

    #[inline]
    fn lookup_fingerprint(&self, fingerprint: u128) -> Option<u32> {
        let hash = Self::table_hash(fingerprint);
        self.table
            .find(hash, |candidate| {
                self.fingerprints[*candidate as usize] == fingerprint
            })
            .or_else(|| {
                self.overflow_table.as_ref()?.find(hash, |candidate| {
                    self.fingerprints[*candidate as usize] == fingerprint
                })
            })
            .map(|zero_based| zero_based.saturating_add(1))
    }

    fn table_capacity(&self) -> usize {
        self.table.capacity() + self.overflow_table.as_ref().map_or(0, HashTable::capacity)
    }

    fn seed(&mut self, key: [u8; 32]) -> Result<u32> {
        let fingerprint = self.fingerprint(&key);
        self.seed_with_fingerprint(key, fingerprint)
    }

    fn seed_with_fingerprint(&mut self, key: [u8; 32], fingerprint: u128) -> Result<u32> {
        if let Some(id) = self.lookup_fingerprint(fingerprint) {
            let existing = self
                .seed_keys
                .get(id as usize - 1)
                .with_context(|| format!("seed fingerprint matched non-seed first-seen id {id}"))?;
            anyhow::ensure!(
                *existing == key,
                "first-seen 128-bit fingerprint collision while seeding id {id}: existing={} incoming={}",
                hex32(existing),
                hex32(&key),
            );
            return Ok(id);
        }
        let id = self.insert_new(key, fingerprint, 0)?;
        self.seed_keys.push(key);
        self.seed_hasher.update(key);
        self.seeded_keys = self.fingerprints.len();
        Ok(id)
    }

    #[inline]
    fn intern(&mut self, key: &[u8; 32]) -> Result<u32> {
        let fingerprint = self.fingerprint(key);
        self.intern_with_fingerprint(key, fingerprint)
    }

    fn intern_with_fingerprint(&mut self, key: &[u8; 32], fingerprint: u128) -> Result<u32> {
        self.references = self
            .references
            .checked_add(1)
            .context("first-seen reference count overflow")?;
        self.observed_audit.add(key);
        if let Some(zero_based) = self.lookup_fingerprint(fingerprint) {
            let zero_based = zero_based - 1;
            self.counts
                .increment(zero_based as usize)
                .with_context(|| {
                    format!(
                        "first-seen reference count overflow for id {}",
                        zero_based + 1
                    )
                })?;
            return Ok(zero_based + 1);
        }
        self.insert_new(*key, fingerprint, 1)
    }

    fn insert_new(&mut self, key: [u8; 32], fingerprint: u128, count: u32) -> Result<u32> {
        let zero_based = u32::try_from(self.fingerprints.len())
            .context("first-seen registry exhausted the u32 compact-ID space")?;
        let id = zero_based
            .checked_add(1)
            .context("first-seen registry exhausted the one-based u32 compact-ID space")?;
        let use_primary = self.overflow_table.is_none() && self.table.len() < self.table.capacity();
        if !use_primary {
            let fingerprints = &self.fingerprints;
            self.overflow_table
                .get_or_insert_with(HashTable::new)
                .try_reserve(1, |candidate| {
                    Self::table_hash(fingerprints[*candidate as usize])
                })
                .map_err(|error| anyhow!("reserve first-seen overflow table: {error:?}"))?;
        }
        self.key_writer
            .write_all(&key)
            .context("append first-seen registry key")?;
        let hash = Self::table_hash(fingerprint);
        self.fingerprints.push(fingerprint);
        self.counts.push(
            u8::try_from(count).expect("new first-seen count is always a seed zero or first one"),
        );
        let fingerprints = &self.fingerprints;
        if use_primary {
            self.table.insert_unique(hash, zero_based, |candidate| {
                Self::table_hash(fingerprints[*candidate as usize])
            });
        } else {
            let overflow = self
                .overflow_table
                .as_mut()
                .expect("overflow table reserved before first insertion");
            overflow.insert_unique(hash, zero_based, |candidate| {
                Self::table_hash(fingerprints[*candidate as usize])
            });
        }
        Ok(id)
    }

    fn seed_digest(&self) -> String {
        let digest = self.seed_hasher.clone().finalize();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&digest);
        hex32(&bytes)
    }

    fn into_parts(mut self) -> Result<FirstSeenRegistryParts> {
        self.key_writer
            .flush()
            .context("flush first-seen registry keys")?;
        let keys = self.fingerprints.len();
        let seed_hash = self.seed_digest();
        let seeded_keys = self.seeded_keys;
        let references = self.references;
        let table_capacity = self.table_capacity();
        let observed_audit = self.observed_audit;
        let count_heap_bytes = self.counts.estimated_heap_bytes();
        let count_u16_chunks = self.counts.u16_chunks;
        let count_u32_chunks = self.counts.u32_chunks;
        Ok(FirstSeenRegistryParts {
            keys,
            counts: self.counts,
            seed_hash,
            seeded_keys,
            references,
            table_capacity,
            observed_audit,
            count_heap_bytes,
            count_u16_chunks,
            count_u32_chunks,
        })
    }
}

struct FirstSeenRegistryParts {
    keys: usize,
    counts: FirstSeenCounts,
    seed_hash: String,
    seeded_keys: usize,
    references: u64,
    table_capacity: usize,
    observed_audit: FirstSeenReferenceAudit,
    count_heap_bytes: usize,
    count_u16_chunks: usize,
    count_u32_chunks: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct FirstSeenReferenceAudit {
    low: u128,
    high: u128,
}

impl FirstSeenReferenceAudit {
    #[inline]
    fn add(&mut self, key: &[u8; 32]) {
        let low = u128::from_le_bytes(key[..16].try_into().unwrap());
        let high = u128::from_le_bytes(key[16..].try_into().unwrap());
        self.low = self.low.wrapping_add(low);
        self.high = self.high.wrapping_add(high);
    }

    #[inline]
    fn add_counted(&mut self, key: &[u8; 32], count: u32) {
        let count = u128::from(count);
        let low = u128::from_le_bytes(key[..16].try_into().unwrap());
        let high = u128::from_le_bytes(key[16..].try_into().unwrap());
        self.low = self.low.wrapping_add(low.wrapping_mul(count));
        self.high = self.high.wrapping_add(high.wrapping_mul(count));
    }
}

fn seed_first_seen_registry(
    registry: &mut FirstSeenRegistry,
    seed_registry_path: Option<&Path>,
    seed_key_limit: usize,
) -> Result<()> {
    for key in live_builtin_program_keys() {
        registry.seed(*key)?;
    }
    let Some(path) = seed_registry_path else {
        return Ok(());
    };
    let file =
        File::open(path).with_context(|| format!("open seed registry {}", path.display()))?;
    let bytes = file
        .metadata()
        .with_context(|| format!("stat seed registry {}", path.display()))?
        .len();
    anyhow::ensure!(
        bytes.is_multiple_of(32),
        "seed registry {} length {} is not divisible by 32",
        path.display(),
        bytes
    );
    let available = usize::try_from(bytes / 32).context("seed registry key count exceeds usize")?;
    let take = available.min(seed_key_limit);
    registry.seed_keys.reserve(take);
    let mut reader = BufReader::with_capacity(first_seen_seed_reader_capacity(take), file);
    for index in 0..take {
        let mut key = [0u8; 32];
        reader
            .read_exact(&mut key)
            .with_context(|| format!("read seed registry {} key {index}", path.display()))?;
        registry.seed(key)?;
    }
    Ok(())
}

fn first_seen_seed_reader_capacity(keys: usize) -> usize {
    keys.saturating_mul(32)
        .clamp(FIRST_SEEN_SEED_READ_BUFFER_MIN, ARCHIVE_PRE_HOT_IO_BUFFER)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FirstSeenHotSeedCandidate {
    key: [u8; 32],
    count: u32,
}

impl Ord for FirstSeenHotSeedCandidate {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // BinaryHeap keeps the greatest item at the top. Reverse count ordering
        // makes the lowest-frequency (and then lexicographically greatest) key
        // the eviction candidate.
        other
            .count
            .cmp(&self.count)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl PartialOrd for FirstSeenHotSeedCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

fn first_seen_hot_seed_better(
    candidate: &FirstSeenHotSeedCandidate,
    worst: &FirstSeenHotSeedCandidate,
) -> bool {
    candidate.count > worst.count || (candidate.count == worst.count && candidate.key < worst.key)
}

fn write_first_seen_hot_seed(
    path: &Path,
    registry_path: &Path,
    counts: &FirstSeenCounts,
    limit: usize,
) -> Result<(usize, String)> {
    let registry_bytes = std::fs::metadata(registry_path)
        .with_context(|| format!("stat {}", registry_path.display()))?
        .len();
    anyhow::ensure!(
        registry_bytes.is_multiple_of(32),
        "hot-seed registry length {} is not divisible by 32: {}",
        registry_bytes,
        registry_path.display(),
    );
    let registry_keys = usize::try_from(registry_bytes / 32)
        .context("hot-seed registry key count exceeds usize")?;
    anyhow::ensure!(
        registry_keys == counts.len(),
        "hot-seed registry has {} keys but counts has {} rows",
        registry_keys,
        counts.len(),
    );
    let take = limit.min(registry_keys);
    let registry_file =
        File::open(registry_path).with_context(|| format!("open {}", registry_path.display()))?;
    let mut reader = BufReader::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, registry_file);
    let mut selected = BinaryHeap::<FirstSeenHotSeedCandidate>::with_capacity(take);
    for count in counts.iter() {
        let mut key = [0u8; 32];
        reader
            .read_exact(&mut key)
            .with_context(|| format!("read {}", registry_path.display()))?;
        if take == 0 {
            continue;
        }
        let candidate = FirstSeenHotSeedCandidate { key, count };
        if selected.len() < take {
            selected.push(candidate);
        } else if first_seen_hot_seed_better(&candidate, selected.peek().unwrap()) {
            selected.pop();
            selected.push(candidate);
        }
    }
    let mut selected = selected.into_vec();
    selected.sort_unstable_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.key.cmp(&right.key))
    });

    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, file);
    let mut hasher = Sha256::new();
    for candidate in selected {
        writer
            .write_all(&candidate.key)
            .with_context(|| format!("write {}", path.display()))?;
        hasher.update(candidate.key);
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    let digest = hasher.finalize();
    let mut digest_bytes = [0u8; 32];
    digest_bytes.copy_from_slice(&digest);
    Ok((take, hex32(&digest_bytes)))
}

fn verify_first_seen_reference_audit(
    registry_path: &Path,
    counts: &FirstSeenCounts,
    observed: FirstSeenReferenceAudit,
) -> Result<FirstSeenReferenceAudit> {
    let file = File::open(registry_path)
        .with_context(|| format!("open first-seen audit registry {}", registry_path.display()))?;
    let mut reader = BufReader::with_capacity(ARCHIVE_PRE_HOT_IO_BUFFER, file);
    let mut expected = FirstSeenReferenceAudit::default();
    for (id, count) in counts.iter().enumerate() {
        let mut key = [0u8; 32];
        reader.read_exact(&mut key).with_context(|| {
            format!(
                "read first-seen audit registry {} id {}",
                registry_path.display(),
                id + 1,
            )
        })?;
        expected.add_counted(&key, count);
    }
    let mut trailing = [0u8; 1];
    anyhow::ensure!(
        reader.read(&mut trailing)? == 0,
        "first-seen audit registry {} has more keys than {} counts",
        registry_path.display(),
        counts.len(),
    );
    anyhow::ensure!(
        observed == expected,
        "first-seen fingerprint collision audit failed: observed_low={:032x} expected_low={:032x} observed_high={:032x} expected_high={:032x}",
        observed.low,
        expected.low,
        observed.high,
        expected.high,
    );
    Ok(expected)
}

#[allow(clippy::too_many_arguments)]
fn write_first_seen_manifest(
    path: &Path,
    input: &Path,
    seed_source: Option<&Path>,
    seed_hash: &str,
    seeded_keys: usize,
    next_seed_hash: &str,
    next_seed_keys: usize,
    registry_keys: usize,
    references: u64,
    reference_audit: FirstSeenReferenceAudit,
) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(64 << 10, file);
    writeln!(writer, "version=1")?;
    writeln!(writer, "registry_order=first_seen_v1")?;
    writeln!(writer, "count_semantics=all_compact_pubkey_refs_v1")?;
    writeln!(writer, "fingerprint=gxhash128_random_seed_full_key_v1")?;
    writeln!(writer, "reference_audit=wrapping_u128_halves_le_v1")?;
    writeln!(
        writer,
        "timestamp_artifacts={}",
        crate::first_seen_finalization::FIRST_SEEN_TIMESTAMP_ARTIFACTS_VALUE
    )?;
    writeln!(writer, "input={}", input.display())?;
    writeln!(
        writer,
        "seed_source={}",
        seed_source
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "builtins-only".to_string())
    )?;
    writeln!(writer, "assigned_seed_sha256={seed_hash}")?;
    writeln!(writer, "seeded_keys={seeded_keys}")?;
    writeln!(writer, "next_seed_file={FIRST_SEEN_HOT_SEED_FILE}")?;
    writeln!(writer, "next_seed_file_sha256={next_seed_hash}")?;
    writeln!(writer, "next_seed_keys={next_seed_keys}")?;
    writeln!(writer, "registry_keys={registry_keys}")?;
    writeln!(writer, "references={references}")?;
    writeln!(writer, "reference_audit_low={:032x}", reference_audit.low)?;
    writeln!(writer, "reference_audit_high={:032x}", reference_audit.high)?;
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod first_seen_registry_tests {
    use super::*;

    fn temp_file(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "blockzilla-first-seen-{label}-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn assigns_stable_one_based_ids_and_aligned_counts() {
        let path = temp_file("registry");
        let mut registry = FirstSeenRegistry::new(8, &path).unwrap();
        assert_eq!(registry.seed([9; 32]).unwrap(), 1);
        assert_eq!(registry.intern(&[1; 32]).unwrap(), 2);
        assert_eq!(registry.intern(&[9; 32]).unwrap(), 1);
        assert_eq!(registry.intern(&[1; 32]).unwrap(), 2);
        assert_eq!(registry.intern(&[2; 32]).unwrap(), 3);
        assert_eq!(registry.lookup(&[9; 32]), Some(1));
        assert_eq!(registry.lookup(&[3; 32]), None);

        let parts = registry.into_parts().unwrap();
        assert_eq!(parts.keys, 3);
        assert_eq!(parts.counts.iter().collect::<Vec<_>>(), vec![1, 2, 1]);
        assert_eq!(parts.seeded_keys, 1);
        assert_eq!(parts.references, 4);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            [[9u8; 32], [1u8; 32], [2u8; 32]].concat()
        );
        verify_first_seen_reference_audit(&path, &parts.counts, parts.observed_audit).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn injected_fingerprint_alias_is_rejected_by_post_scan_audit() {
        let path = temp_file("collision-audit");
        let mut registry = FirstSeenRegistry::new(8, &path).unwrap();
        let fingerprint = 0x1234_u128;
        assert_eq!(
            registry
                .intern_with_fingerprint(&[1u8; 32], fingerprint)
                .unwrap(),
            1
        );
        assert_eq!(
            registry
                .intern_with_fingerprint(&[2u8; 32], fingerprint)
                .unwrap(),
            1
        );
        let parts = registry.into_parts().unwrap();
        let error = verify_first_seen_reference_audit(&path, &parts.counts, parts.observed_audit)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("fingerprint collision audit failed")
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn injected_seed_fingerprint_alias_fails_immediately() {
        let path = temp_file("seed-collision");
        let mut registry = FirstSeenRegistry::new(8, &path).unwrap();
        let fingerprint = 0x5678_u128;
        registry
            .seed_with_fingerprint([1u8; 32], fingerprint)
            .unwrap();
        let error = registry
            .seed_with_fingerprint([2u8; 32], fingerprint)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("fingerprint collision while seeding")
        );
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn per_key_reference_count_overflow_is_an_error() {
        let path = temp_file("count-overflow");
        let mut registry = FirstSeenRegistry::new(8, &path).unwrap();
        registry.intern(&[1u8; 32]).unwrap();
        registry.counts.set_for_test(0, u32::MAX);
        let error = registry.intern(&[1u8; 32]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("reference count overflow for id 1")
        );
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn reference_audit_covers_both_key_halves() {
        let mut base = FirstSeenReferenceAudit::default();
        base.add(&[0u8; 32]);
        let mut low_changed = [0u8; 32];
        low_changed[0] = 1;
        let mut low = FirstSeenReferenceAudit::default();
        low.add(&low_changed);
        let mut high_changed = [0u8; 32];
        high_changed[16] = 1;
        let mut high = FirstSeenReferenceAudit::default();
        high.add(&high_changed);
        assert_ne!(base, low);
        assert_ne!(base, high);
        assert_ne!(low, high);
    }

    #[test]
    fn overflow_table_preserves_first_seen_ids_and_counts() {
        let path = temp_file("table-overflow");
        let mut registry = FirstSeenRegistry::new(1, &path).unwrap();
        let primary_capacity = registry.table.capacity();
        let mut keys = Vec::new();
        for index in 0..=primary_capacity {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&(index as u64).to_le_bytes());
            keys.push(key);
            assert_eq!(registry.intern(&key).unwrap(), index as u32 + 1);
        }
        assert!(registry.overflow_table.is_some());
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(registry.lookup(key), Some(index as u32 + 1));
        }
        assert_eq!(
            registry.intern(keys.last().unwrap()).unwrap(),
            keys.len() as u32
        );
        let parts = registry.into_parts().unwrap();
        assert_eq!(parts.counts.get(parts.counts.len() - 1), 2);
        verify_first_seen_reference_audit(&path, &parts.counts, parts.observed_audit).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn block_local_access_pubkeys_deduplicate_and_reject_aliases() {
        let mut entries = vec![
            ArchiveV2BlockAccessPubkey {
                id: 2,
                pubkey: [2u8; 32],
            },
            ArchiveV2BlockAccessPubkey {
                id: 1,
                pubkey: [1u8; 32],
            },
            ArchiveV2BlockAccessPubkey {
                id: 2,
                pubkey: [2u8; 32],
            },
        ];
        normalize_first_seen_access_pubkeys(&mut entries).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, 1);
        assert_eq!(entries[1].id, 2);

        entries.push(ArchiveV2BlockAccessPubkey {
            id: 2,
            pubkey: [3u8; 32],
        });
        let error = normalize_first_seen_access_pubkeys(&mut entries).unwrap_err();
        assert!(error.to_string().contains("aliases two pubkeys"));
    }

    #[test]
    fn adaptive_counts_promote_without_losing_neighbor_values() {
        let mut counts = FirstSeenCounts::with_capacity_and_direct_limit(4, 0);
        counts.push(1);
        for _ in 1..u8::MAX {
            counts.increment(0).unwrap();
        }
        assert_eq!(counts.get(0), u32::from(u8::MAX));
        assert_eq!(counts.u16_chunks, 0);
        counts.increment(0).unwrap();
        assert_eq!(counts.get(0), u32::from(u8::MAX) + 1);
        assert_eq!(counts.u16_chunks, 1);

        // Sequential IDs can still enter the final chunk after it promoted.
        counts.push(7);
        assert_eq!(counts.get(1), 7);
        counts.set_for_test(0, u32::from(u16::MAX));
        counts.increment(0).unwrap();
        assert_eq!(counts.get(0), u32::from(u16::MAX) + 1);
        assert_eq!(counts.get(1), 7);
        assert_eq!(counts.u16_chunks, 0);
        assert_eq!(counts.u32_chunks, 1);
    }

    #[test]
    fn hot_seed_selects_frequency_head_with_key_tie_break() {
        let path = temp_file("seed");
        let registry_path = temp_file("seed-registry");
        let keys = vec![[3u8; 32], [2u8; 32], [1u8; 32], [4u8; 32]];
        let mut counts = FirstSeenCounts::with_capacity(4);
        for count in [1, 9, 9, 2] {
            counts.push(count);
        }
        std::fs::write(&registry_path, keys.concat()).unwrap();
        let (written, hash) = write_first_seen_hot_seed(&path, &registry_path, &counts, 2).unwrap();
        assert_eq!(written, 2);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            [[1u8; 32], [2u8; 32]].concat()
        );
        assert_eq!(hash.len(), 64);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(registry_path).unwrap();
    }

    #[test]
    fn seed_reader_capacity_tracks_requested_prefix_and_is_bounded() {
        assert_eq!(
            first_seen_seed_reader_capacity(0),
            FIRST_SEEN_SEED_READ_BUFFER_MIN
        );
        assert_eq!(first_seen_seed_reader_capacity(65_536), 2 << 20);
        assert_eq!(
            first_seen_seed_reader_capacity(usize::MAX),
            ARCHIVE_PRE_HOT_IO_BUFFER
        );
    }
}

fn hex32(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn bench(input: &Path, iterations: usize) -> Result<()> {
    let iterations = iterations.max(1);
    let input_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let start = Instant::now();
    let mut bytes = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut records = 0u64;

    for _ in 0..iterations {
        let mut reader = WincodeLeb128FramedReader::new(open_wincode_bench_input(input)?);
        while let Some((len, record)) = reader.read::<WincodeArchiveV2Record>()? {
            records += 1;
            bytes += len as u64;
            if let WincodeArchiveV2Record::Block(block) = record {
                blocks += 1;
                txs += block.txs.len() as u64;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mib_s = if elapsed > 0.0 {
        bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let input_mib_s = if elapsed > 0.0 {
        input_bytes as f64 * iterations as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let block_s = if elapsed > 0.0 {
        blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        txs as f64 / elapsed
    } else {
        0.0
    };
    println!(
        "archive_v2_read iterations={iterations} records={records} blocks={blocks} txs={txs} input_bytes={input_bytes} payload_bytes={bytes} elapsed_s={elapsed:.3} input_MiB_s={input_mib_s:.2} payload_MiB_s={mib_s:.2} blocks_s={block_s:.2} tx_s={tx_s:.2}"
    );
    Ok(())
}

pub(crate) fn bench_no_registry(input: &Path, iterations: usize) -> Result<()> {
    let iterations = iterations.max(1);
    let input_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let start = Instant::now();
    let mut bytes = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut records = 0u64;

    for _ in 0..iterations {
        let mut reader = WincodeLeb128FramedReader::new(open_wincode_bench_input(input)?);
        while let Some((len, record)) = reader.read::<WincodeArchiveV2NoRegistryRecord>()? {
            records += 1;
            bytes += len as u64;
            if let WincodeArchiveV2NoRegistryRecord::Block(block) = record {
                blocks += 1;
                txs += block.txs.len() as u64;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mib_s = if elapsed > 0.0 {
        bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let input_mib_s = if elapsed > 0.0 {
        input_bytes as f64 * iterations as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let block_s = if elapsed > 0.0 {
        blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        txs as f64 / elapsed
    } else {
        0.0
    };
    println!(
        "archive_v2_no_registry_read iterations={iterations} records={records} blocks={blocks} txs={txs} input_bytes={input_bytes} payload_bytes={bytes} elapsed_s={elapsed:.3} input_MiB_s={input_mib_s:.2} payload_MiB_s={mib_s:.2} blocks_s={block_s:.2} tx_s={tx_s:.2}"
    );
    Ok(())
}

fn open_wincode_bench_input(input: &Path) -> Result<Box<dyn Read>> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let reader = BufReader::with_capacity(BUFFER_SIZE, file);
    if input.extension().is_some_and(|ext| ext == "zst") {
        return Ok(Box::new(
            zstd_decoder_with_long_window(reader)
                .with_context(|| format!("zstd decode {}", input.display()))?,
        ));
    }
    Ok(Box::new(reader))
}

#[derive(Debug)]
struct ArchiveV2BlockIndex {
    archive_file_bytes: u64,
    rows: Vec<ArchiveV2BlockIndexRow>,
}

#[derive(Debug, Clone, Copy)]
struct ArchiveV2BlockIndexRow {
    block_id: u32,
    slot: u64,
    frame_offset: u64,
    payload_offset: u64,
    payload_len: u32,
    tx_count: u32,
}

#[derive(Debug)]
struct ArchiveV2ZstdBlockIndex {
    blob_file_bytes: u64,
    level: i32,
    flags: u32,
    rows: Vec<ArchiveV2ZstdBlockIndexRow>,
}

#[derive(Debug, Clone, Copy)]
struct ArchiveV2ZstdBlockIndexRow {
    block_id: u32,
    slot: u64,
    compressed_offset: u64,
    compressed_len: u32,
    uncompressed_len: u32,
    tx_count: u32,
}

#[derive(Debug, Default)]
struct ArchiveV2IndexedBenchStats {
    payload_bytes: u64,
    blocks: u64,
    txs: u64,
}

#[derive(Debug, Default)]
struct ArchiveV2ZstdBenchStats {
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    blocks: u64,
    txs: u64,
}

pub(crate) fn build_index(input: &Path, output: Option<&Path>) -> Result<()> {
    anyhow::ensure!(
        input.extension().is_none_or(|ext| ext != "zst"),
        "indexed Archive V2 reads require an uncompressed .wincode file, got {}",
        input.display()
    );

    let output_path = output
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_index_path(input));
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = BufReader::with_capacity(ARCHIVE_V2_INDEX_SCAN_BUFFER_SIZE, file);
    let mut frame_buf = Vec::with_capacity(256);
    let mut rows = Vec::new();
    let mut pending_block: Option<ArchiveV2BlockIndexRow> = None;
    let mut records = 0u64;
    let mut embedded_indexes = 0u64;
    let mut stale_embedded_indexes = 0u64;
    let mut offset = 0u64;
    let mut expected_logical_block_offset = 0u64;
    let mut progress = ProgressTracker::new("Archive V2 Index");
    let started = Instant::now();

    while let Some((payload_len, prefix_len)) = read_u32_varint_with_encoded_len(&mut reader)? {
        let frame_offset = offset;
        let payload_offset = frame_offset + prefix_len as u64;
        offset = payload_offset;
        anyhow::ensure!(
            payload_len > 0,
            "empty archive v2 frame at offset {frame_offset}"
        );
        let mut tag = [0u8; 1];
        reader
            .read_exact(&mut tag)
            .with_context(|| format!("read frame tag at offset {payload_offset}"))?;
        offset += 1;
        records += 1;

        if tag[0] == WINCODE_ARCHIVE_V2_RECORD_BLOCK_TAG {
            anyhow::ensure!(
                pending_block.is_none(),
                "block at offset {frame_offset} appeared before previous block index"
            );
            let block_id = u32::try_from(rows.len()).context("block_id exceeds u32::MAX")?;
            pending_block = Some(ArchiveV2BlockIndexRow {
                block_id,
                slot: 0,
                frame_offset,
                payload_offset,
                payload_len,
                tx_count: 0,
            });
            reader
                .seek(SeekFrom::Current(i64::from(payload_len - 1)))
                .with_context(|| format!("skip block payload at offset {payload_offset}"))?;
            offset += u64::from(payload_len - 1);
            continue;
        }
        anyhow::ensure!(
            matches!(
                tag[0],
                WINCODE_ARCHIVE_V2_RECORD_HEADER_TAG
                    | WINCODE_ARCHIVE_V2_RECORD_INDEX_TAG
                    | WINCODE_ARCHIVE_V2_RECORD_FOOTER_TAG
                    | WINCODE_ARCHIVE_V2_RECORD_GENESIS_TAG
            ),
            "unknown archive v2 record tag {} at offset {payload_offset}",
            tag[0]
        );

        frame_buf.resize(payload_len as usize, 0);
        frame_buf[0] = tag[0];
        reader
            .read_exact(&mut frame_buf[1..])
            .with_context(|| format!("read frame payload at offset {payload_offset}"))?;
        offset += u64::from(payload_len - 1);
        let record: WincodeArchiveV2Record =
            wincode::config::deserialize(&frame_buf, wincode_leb128_config())
                .with_context(|| format!("decode archive v2 frame at offset {frame_offset}"))?;
        match record {
            WincodeArchiveV2Record::Block(_) => unreachable!("block tag handled before decode"),
            WincodeArchiveV2Record::Index(index) => {
                embedded_indexes += 1;
                let Some(mut row) = pending_block.take() else {
                    anyhow::bail!(
                        "embedded index appeared before any block at offset {frame_offset}"
                    );
                };
                anyhow::ensure!(
                    index.block_id == row.block_id,
                    "embedded index mismatch at offset {frame_offset}: embedded={index:?} physical={row:?}"
                );
                if index.block_len != row.payload_len
                    || index.block_offset != expected_logical_block_offset
                {
                    stale_embedded_indexes += 1;
                }
                row.slot = index.slot;
                row.tx_count = index.tx_count;
                expected_logical_block_offset += row.payload_len as u64;
                progress.update_slot(row.slot);
                progress.update(1, row.tx_count as u64);
                rows.push(row);
            }
            WincodeArchiveV2Record::Header(_) | WincodeArchiveV2Record::Genesis(_) => {
                anyhow::ensure!(
                    pending_block.is_none(),
                    "non-index record at offset {frame_offset} appeared before previous block index"
                );
            }
            WincodeArchiveV2Record::Footer(_) => {
                anyhow::ensure!(
                    pending_block.is_none(),
                    "footer at offset {frame_offset} appeared before previous block index"
                );
            }
        }
    }

    anyhow::ensure!(
        pending_block.is_none(),
        "archive ended before the last block index record"
    );
    anyhow::ensure!(
        offset == archive_file_bytes,
        "archive scan ended at byte {offset}, but file size is {archive_file_bytes}"
    );
    write_archive_v2_block_index(&output_path, archive_file_bytes, &rows)?;
    progress.final_report();
    info!(
        "Archive V2 index complete in {:.2}s: records={} blocks={} embedded_indexes={} stale_embedded_indexes={} archive_bytes={} index={} index_bytes={}",
        started.elapsed().as_secs_f64(),
        records,
        rows.len(),
        embedded_indexes,
        stale_embedded_indexes,
        archive_file_bytes,
        output_path.display(),
        std::fs::metadata(&output_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    );
    Ok(())
}

pub(crate) fn bench_indexed(
    input: &Path,
    index: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (input, index, workers, chunk_size);
        anyhow::bail!("indexed Archive V2 benchmark currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        bench_indexed_unix(input, index, workers, chunk_size)
    }
}

#[cfg(unix)]
fn bench_indexed_unix(
    input: &Path,
    index: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    anyhow::ensure!(
        input.extension().is_none_or(|ext| ext != "zst"),
        "indexed Archive V2 reads require an uncompressed .wincode file, got {}",
        input.display()
    );
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_index_path(input));
    let index = read_archive_v2_block_index(&index_path)?;
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        archive_file_bytes == index.archive_file_bytes,
        "index was built for {} bytes, but archive is {} bytes",
        index.archive_file_bytes,
        archive_file_bytes
    );

    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let rows = Arc::new(index.rows);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let file = Arc::clone(&file);
        let rows = Arc::clone(&rows);
        let next = Arc::clone(&next);
        handles.push(thread::spawn(
            move || -> Result<ArchiveV2IndexedBenchStats> {
                let mut stats = ArchiveV2IndexedBenchStats::default();
                let mut frame_buf = Vec::with_capacity(2 << 20);
                loop {
                    let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                    if start >= rows.len() {
                        break;
                    }
                    let end = start.saturating_add(chunk_size).min(rows.len());
                    for row in &rows[start..end] {
                        frame_buf.resize(row.payload_len as usize, 0);
                        read_exact_at(&file, &mut frame_buf, row.payload_offset).with_context(
                            || {
                                format!(
                                    "worker {worker_id} read block_id {} at payload offset {}",
                                    row.block_id, row.payload_offset
                                )
                            },
                        )?;
                        let record: WincodeArchiveV2Record =
                            wincode::config::deserialize(&frame_buf, wincode_leb128_config())
                                .with_context(|| {
                                    format!("worker {worker_id} decode block_id {}", row.block_id)
                                })?;
                        let WincodeArchiveV2Record::Block(block) = record else {
                            anyhow::bail!(
                                "worker {worker_id} expected block at payload offset {}, got different record",
                                row.payload_offset
                            );
                        };
                        anyhow::ensure!(
                            block.header.compact.slot == row.slot,
                            "worker {worker_id} block_id {} slot mismatch: decoded={} index={}",
                            row.block_id,
                            block.header.compact.slot,
                            row.slot
                        );
                        anyhow::ensure!(
                            block.txs.len() == row.tx_count as usize,
                            "worker {worker_id} block_id {} tx count mismatch: decoded={} index={}",
                            row.block_id,
                            block.txs.len(),
                            row.tx_count
                        );
                        stats.payload_bytes += row.payload_len as u64;
                        stats.blocks += 1;
                        stats.txs += block.txs.len() as u64;
                    }
                }
                Ok(stats)
            },
        ));
    }

    let mut stats = ArchiveV2IndexedBenchStats::default();
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow!("indexed Archive V2 benchmark worker panicked"))??;
        stats.payload_bytes += worker_stats.payload_bytes;
        stats.blocks += worker_stats.blocks;
        stats.txs += worker_stats.txs;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let mib_s = if elapsed > 0.0 {
        stats.payload_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let block_s = if elapsed > 0.0 {
        stats.blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        stats.txs as f64 / elapsed
    } else {
        0.0
    };
    println!(
        "archive_v2_indexed_read workers={workers} chunk_size={chunk_size} index_rows={} blocks={} txs={} archive_bytes={archive_file_bytes} payload_bytes={} elapsed_s={elapsed:.3} payload_MiB_s={mib_s:.2} blocks_s={block_s:.2} tx_s={tx_s:.2}",
        rows.len(),
        stats.blocks,
        stats.txs,
        stats.payload_bytes
    );
    Ok(())
}

pub(crate) fn repack_zstd_blocks(
    input: &Path,
    output_dir: &Path,
    level: i32,
    dict: Option<&Path>,
    max_blocks: Option<u64>,
) -> Result<()> {
    anyhow::ensure!(
        level >= 0,
        "zstd compression level must be non-negative, got {level}"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let blocks_path = output_dir.join(ARCHIVE_ZSTD_BLOCKS_FILE);
    let index_path = output_dir.join(ARCHIVE_ZSTD_INDEX_FILE);
    let meta_path = output_dir.join(ARCHIVE_ZSTD_META_FILE);
    let dict_bytes = load_optional_dict(dict)?;
    let flags = if dict_bytes.is_some() {
        ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY
    } else {
        0
    };

    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(
        BUFFER_SIZE,
        open_wincode_bench_input(input)?,
    ));
    let blocks_file =
        File::create(&blocks_path).with_context(|| format!("create {}", blocks_path.display()))?;
    let mut blocks_writer = BufWriter::with_capacity(BUFFER_SIZE, blocks_file);
    let meta_file =
        File::create(&meta_path).with_context(|| format!("create {}", meta_path.display()))?;
    let mut meta_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, meta_file));
    let mut compressor = if let Some(dict_bytes) = dict_bytes.as_deref() {
        zstd::bulk::Compressor::with_dictionary(level, dict_bytes)
            .context("create zstd dictionary compressor")?
    } else {
        zstd::bulk::Compressor::new(level).context("create zstd compressor")?
    };

    let mut rows = Vec::new();
    let mut progress = ProgressTracker::new("Archive V2 Zstd Repack");
    let mut records = 0u64;
    let mut embedded_indexes_skipped = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut blob_offset = 0u64;
    let mut uncompressed_bytes = 0u64;
    let mut compressed_bytes = 0u64;
    let mut largest_row: Option<ArchiveV2ZstdBlockIndexRow> = None;
    let mut block_scratch = Vec::with_capacity(8 << 20);
    let started = Instant::now();

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        records += 1;
        match record {
            WincodeArchiveV2Record::Header(_)
            | WincodeArchiveV2Record::Genesis(_)
            | WincodeArchiveV2Record::Footer(_) => {
                meta_writer.write(&record)?;
            }
            WincodeArchiveV2Record::Index(_) => {
                embedded_indexes_skipped += 1;
            }
            WincodeArchiveV2Record::Block(block) => {
                let slot = block.header.compact.slot;
                let tx_count =
                    u32::try_from(block.txs.len()).context("block tx count exceeds u32::MAX")?;
                let block_record = WincodeArchiveV2Record::Block(block);
                encode_with_scratch(&block_record, &mut block_scratch)?;
                let uncompressed_len = u32::try_from(block_scratch.len())
                    .context("archive v2 block payload exceeds u32::MAX")?;
                let compressed = compressor
                    .compress(&block_scratch)
                    .with_context(|| format!("zstd compress block_id {}", rows.len()))?;
                let compressed_len = u32::try_from(compressed.len())
                    .context("compressed archive v2 block exceeds u32::MAX")?;
                blocks_writer
                    .write_all(&compressed)
                    .with_context(|| format!("write {}", blocks_path.display()))?;

                let block_id = u32::try_from(rows.len()).context("block id exceeds u32::MAX")?;
                let row = ArchiveV2ZstdBlockIndexRow {
                    block_id,
                    slot,
                    compressed_offset: blob_offset,
                    compressed_len,
                    uncompressed_len,
                    tx_count,
                };
                if largest_row
                    .as_ref()
                    .is_none_or(|largest| row.uncompressed_len > largest.uncompressed_len)
                {
                    largest_row = Some(row);
                }
                rows.push(row);
                blob_offset += compressed_len as u64;
                uncompressed_bytes += uncompressed_len as u64;
                compressed_bytes += compressed_len as u64;
                blocks += 1;
                txs += tx_count as u64;
                progress.update_slot(slot);
                progress.update(1, tx_count as u64);
                if max_blocks.is_some_and(|limit| blocks >= limit) {
                    break;
                }
            }
        }
    }

    blocks_writer
        .flush()
        .with_context(|| format!("flush {}", blocks_path.display()))?;
    meta_writer.flush()?;
    write_archive_v2_zstd_block_index(&index_path, blob_offset, level, flags, &rows)?;
    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    let ratio = if uncompressed_bytes > 0 {
        compressed_bytes as f64 * 100.0 / uncompressed_bytes as f64
    } else {
        0.0
    };
    if let Some(row) = largest_row {
        info!(
            "Archive V2 zstd-block largest block: block_id={} slot={} txs={} uncompressed_len={} compressed_len={}",
            row.block_id, row.slot, row.tx_count, row.uncompressed_len, row.compressed_len
        );
    }
    info!(
        "Archive V2 zstd-block repack complete in {:.2}s: records={} blocks={} txs={} embedded_indexes_skipped={} level={} dict={} max_blocks={:?} uncompressed_bytes={} compressed_bytes={} ratio_pct={:.2} blocks_file={} index={} meta={}",
        elapsed,
        records,
        blocks,
        txs,
        embedded_indexes_skipped,
        level,
        dict.map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string()),
        max_blocks,
        uncompressed_bytes,
        compressed_bytes,
        ratio,
        blocks_path.display(),
        index_path.display(),
        meta_path.display()
    );
    Ok(())
}

pub(crate) fn bench_zstd_blocks(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (input, index, dict, workers, chunk_size);
        anyhow::bail!("zstd-block Archive V2 benchmark currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        bench_zstd_blocks_unix(input, index, dict, workers, chunk_size)
    }
}

#[derive(Default)]
struct CarArchiveBenchStats {
    nodes: u64,
    payload_bytes: u64,
    transactions: u64,
    entries: u64,
    blocks: u64,
    rewards: u64,
    dataframes: u64,
    subsets: u64,
    epochs: u64,
}

pub(crate) fn bench_car_archive(
    input: &Path,
    format: CarBenchInputFormat,
    max_blocks: Option<u64>,
) -> Result<()> {
    let compressed = match format {
        CarBenchInputFormat::Auto => input
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zst")),
        CarBenchInputFormat::Car => false,
        CarBenchInputFormat::CarZstd => true,
    };
    let input_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let mut scanner = RawCarScanner::open_with_compression(input, compressed)?;
    scanner.skip_header()?;

    let mut stats = CarArchiveBenchStats::default();
    let mut progress = ProgressTracker::new(if compressed {
        "CAR-ZSTD Read"
    } else {
        "CAR Read"
    });
    let block_limit = max_blocks.unwrap_or(u64::MAX);
    let started = Instant::now();

    while let Some(raw) = scanner.next_node_timed(None)? {
        stats.nodes += 1;
        stats.payload_bytes = stats.payload_bytes.saturating_add(raw.payload_len as u64);
        match raw.node {
            RawNode::Transaction(tx) => {
                stats.transactions += 1;
                progress.update_slot(tx.slot);
                progress.update(0, 1);
            }
            RawNode::Entry(_) => {
                stats.entries += 1;
            }
            RawNode::Block(block) => {
                stats.blocks += 1;
                progress.update_slot(block.slot);
                progress.update(1, 0);
                if stats.blocks >= block_limit {
                    break;
                }
            }
            RawNode::Rewards(_) => {
                stats.rewards += 1;
            }
            RawNode::DataFrame(_) => {
                stats.dataframes += 1;
            }
            RawNode::Subset(_) => {
                stats.subsets += 1;
            }
            RawNode::Epoch(_) => {
                stats.epochs += 1;
            }
        }
    }

    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    let full_scan = max_blocks.is_none();
    let input_mib_s = if full_scan && elapsed > 0.0 {
        input_file_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let payload_mib_s = if elapsed > 0.0 {
        stats.payload_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let blocks_s = if elapsed > 0.0 {
        stats.blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        stats.transactions as f64 / elapsed
    } else {
        0.0
    };
    println!(
        "car_archive_read format={} workers=1 full_scan={} nodes={} blocks={} txs={} entries={} rewards={} dataframes={} subsets={} epochs={} input_file_bytes={} payload_bytes={} elapsed_s={elapsed:.3} input_MiB_s={input_mib_s:.2} payload_MiB_s={payload_mib_s:.2} blocks_s={blocks_s:.2} tx_s={tx_s:.2}",
        if compressed { "car-zstd" } else { "car" },
        full_scan,
        stats.nodes,
        stats.blocks,
        stats.transactions,
        stats.entries,
        stats.rewards,
        stats.dataframes,
        stats.subsets,
        stats.epochs,
        input_file_bytes,
        stats.payload_bytes
    );
    Ok(())
}

#[cfg(unix)]
fn bench_zstd_blocks_unix(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_zstd_block_index(&index_path)?;
    let blob_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        blob_file_bytes == index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        index.blob_file_bytes,
        blob_file_bytes
    );
    let dict_bytes = Arc::new(load_optional_dict(dict)?.unwrap_or_default());
    anyhow::ensure!(
        (index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );

    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let level = index.level;
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let rows = Arc::new(index.rows);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let file = Arc::clone(&file);
        let rows = Arc::clone(&rows);
        let next = Arc::clone(&next);
        let dict_bytes = Arc::clone(&dict_bytes);
        handles.push(thread::spawn(
            move || -> Result<ArchiveV2ZstdBenchStats> {
                let mut stats = ArchiveV2ZstdBenchStats::default();
                let mut compressed_buf = Vec::with_capacity(2 << 20);
                let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
                    .context("create zstd dictionary decompressor")?;
                loop {
                    let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                    if start >= rows.len() {
                        break;
                    }
                    let end = start.saturating_add(chunk_size).min(rows.len());
                    for row in &rows[start..end] {
                        compressed_buf.resize(row.compressed_len as usize, 0);
                        read_exact_at(&file, &mut compressed_buf, row.compressed_offset)
                            .with_context(|| {
                                format!(
                                    "worker {worker_id} read compressed block_id {} at offset {}",
                                    row.block_id, row.compressed_offset
                                )
                            })?;
                        let block_bytes = decompressor
                            .decompress(&compressed_buf, row.uncompressed_len as usize)
                            .with_context(|| {
                                format!("worker {worker_id} zstd decompress block_id {}", row.block_id)
                            })?;
                        anyhow::ensure!(
                            block_bytes.len() == row.uncompressed_len as usize,
                            "worker {worker_id} block_id {} decompressed length mismatch: decoded={} index={}",
                            row.block_id,
                            block_bytes.len(),
                            row.uncompressed_len
                        );
                        let block: ArchiveV2HotBlockBlob = wincode::config::deserialize(
                            &block_bytes,
                            wincode_leb128_config(),
                        )
                        .with_context(|| {
                            format!("worker {worker_id} decode hot block_id {}", row.block_id)
                        })?;
                        anyhow::ensure!(
                            block.header.slot == row.slot,
                            "worker {worker_id} hot block_id {} slot mismatch: decoded={} index={}",
                            row.block_id,
                            block.header.slot,
                            row.slot
                        );
                        let decoded_txs = block.tx_rows.len();
                        anyhow::ensure!(
                            decoded_txs == row.tx_count as usize,
                            "worker {worker_id} block_id {} tx count mismatch: decoded={} index={}",
                            row.block_id,
                            decoded_txs,
                            row.tx_count
                        );
                        stats.compressed_bytes += row.compressed_len as u64;
                        stats.uncompressed_bytes += row.uncompressed_len as u64;
                        stats.blocks += 1;
                        stats.txs += decoded_txs as u64;
                    }
                }
                Ok(stats)
            },
        ));
    }

    let mut stats = ArchiveV2ZstdBenchStats::default();
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow!("zstd-block Archive V2 benchmark worker panicked"))??;
        stats.compressed_bytes += worker_stats.compressed_bytes;
        stats.uncompressed_bytes += worker_stats.uncompressed_bytes;
        stats.blocks += worker_stats.blocks;
        stats.txs += worker_stats.txs;
    }

    print_zstd_block_bench(
        "archive_v2_hot_block_read",
        workers,
        chunk_size,
        level,
        blob_file_bytes,
        &stats,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

#[derive(Debug, Default)]
struct HotBlockAccountBenchStats {
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    blocks: u64,
    txs: u64,
    account_refs: u64,
    unique_account_refs: u64,
    raw_pubkeys: u64,
    metadata_decoded: u64,
    metadata_skipped: u64,
    varint_bytes: u64,
    fixed_u32_bytes: u64,
    candidate_blocks: u64,
    candidate_txs: u64,
}

impl HotBlockAccountBenchStats {
    fn merge(&mut self, other: Self) {
        self.compressed_bytes += other.compressed_bytes;
        self.uncompressed_bytes += other.uncompressed_bytes;
        self.blocks += other.blocks;
        self.txs += other.txs;
        self.account_refs += other.account_refs;
        self.unique_account_refs += other.unique_account_refs;
        self.raw_pubkeys += other.raw_pubkeys;
        self.metadata_decoded += other.metadata_decoded;
        self.metadata_skipped += other.metadata_skipped;
        self.varint_bytes += other.varint_bytes;
        self.fixed_u32_bytes += other.fixed_u32_bytes;
        self.candidate_blocks += other.candidate_blocks;
        self.candidate_txs += other.candidate_txs;
    }
}

#[derive(Debug, SchemaRead)]
struct CompactMetaLoadedAddressView {
    // CompactTransactionError is an inline tagged enum. Treating it as a byte
    // sequence misaligns every following field for failed transactions.
    _err: Option<CompactTransactionError>,
    _fee: SkipU64,
    _pre_balances: Vec<SkipU64>,
    _post_balances: Vec<SkipU64>,
    _inner_instructions: Option<Vec<SkipCompactInnerInstructions>>,
    // In the current hot-block payload this sits before loaded addresses. A future
    // metadata prefix should move loaded addresses and inner instructions above logs.
    _logs: Option<CompactLogStream>,
    _pre_token_balances: Vec<SkipCompactTokenBalance>,
    _post_token_balances: Vec<SkipCompactTokenBalance>,
    _rewards: Vec<SkipCompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
}

#[derive(Debug)]
struct SkipU8;

#[derive(Debug)]
struct SkipU32;

#[derive(Debug)]
struct SkipU64;

#[derive(Debug)]
struct SkipI32;

#[derive(Debug)]
struct SkipI64;

macro_rules! impl_skip_primitive {
    ($name:ty, $primitive:ty) => {
        unsafe impl<'de, C: ConfigCore> SchemaRead<'de, C> for $name {
            type Dst = Self;

            #[inline]
            fn read(reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
                let _ = <$primitive as SchemaRead<'de, C>>::get(reader)?;
                dst.write(Self);
                Ok(())
            }
        }
    };
}

impl_skip_primitive!(SkipU8, u8);
impl_skip_primitive!(SkipU32, u32);
impl_skip_primitive!(SkipU64, u64);
impl_skip_primitive!(SkipI32, i32);
impl_skip_primitive!(SkipI64, i64);

#[derive(Debug)]
struct SkipBytes;

unsafe impl<'de, C: Config> SchemaRead<'de, C> for SkipBytes {
    type Dst = Self;

    #[inline]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let len = <C::LengthEncoding as SeqLen<C>>::read(reader.by_ref())?;
        let _ = reader.take_scoped(len)?;
        dst.write(Self);
        Ok(())
    }
}

#[derive(Debug)]
struct SkipCompactPubkey;

unsafe impl<'de, C: ConfigCore> SchemaRead<'de, C> for SkipCompactPubkey {
    type Dst = Self;

    #[inline]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let id = <u32 as SchemaRead<'de, C>>::get(reader.by_ref())?;
        if id == CompactPubkey::RAW_SENTINEL {
            let _ = reader.take_scoped(32)?;
        }
        dst.write(Self);
        Ok(())
    }
}

#[derive(Debug, SchemaRead)]
struct SkipCompactInnerInstructions {
    _index: SkipU32,
    _instructions: Vec<SkipCompactInnerInstruction>,
}

#[derive(Debug, SchemaRead)]
struct SkipCompactInnerInstruction {
    _program_id_index: SkipU32,
    _accounts: SkipBytes,
    _data: SkipBytes,
    _stack_height: Option<SkipU32>,
}

#[derive(Debug, SchemaRead)]
struct SkipCompactTokenBalance {
    _account_index: SkipU32,
    _mint: Option<SkipCompactPubkey>,
    _owner: Option<SkipCompactPubkey>,
    _program_id: Option<SkipCompactPubkey>,
    _amount: SkipU64,
    _decimals: SkipU8,
}

#[derive(Debug, SchemaRead)]
struct SkipCompactReward {
    _pubkey: SkipCompactPubkey,
    _lamports: SkipI64,
    _post_balance: SkipU64,
    _reward_type: SkipI32,
    _commission: Option<SkipU8>,
}

pub(crate) fn bench_hot_block_accounts(
    input: &Path,
    index: Option<&Path>,
    registry: Option<&Path>,
    target: Option<&str>,
    include_metadata: bool,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (
            input,
            index,
            registry,
            target,
            include_metadata,
            workers,
            chunk_size,
        );
        anyhow::bail!("hot-block account benchmark currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        bench_hot_block_accounts_unix(
            input,
            index,
            registry,
            target,
            include_metadata,
            workers,
            chunk_size,
        )
    }
}

#[cfg(unix)]
fn bench_hot_block_accounts_unix(
    input: &Path,
    index: Option<&Path>,
    registry: Option<&Path>,
    target: Option<&str>,
    include_metadata: bool,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_zstd_block_index(&index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0,
        "dictionary-compressed hot blocks are not supported by this prototype"
    );
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS == 0,
        "{} is a raw-block index, not independent zstd blocks",
        index_path.display()
    );
    let blob_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        blob_file_bytes == index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        index.blob_file_bytes,
        blob_file_bytes
    );

    let target_id = match target {
        Some(target) => {
            let registry_path = registry.map(Path::to_path_buf).unwrap_or_else(|| {
                input
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
            });
            let key_index = KeyIndex::build(
                KeyStore::load(&registry_path)
                    .with_context(|| format!("load registry {}", registry_path.display()))?
                    .keys,
            );
            let target_pubkey = Pubkey::from_str(target)
                .with_context(|| format!("parse target pubkey {target}"))?
                .to_bytes();
            let id = key_index.lookup(&target_pubkey).with_context(|| {
                format!(
                    "target pubkey {target} is not in {}",
                    registry_path.display()
                )
            })?;
            Some(id)
        }
        None => None,
    };

    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let rows = Arc::new(index.rows);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let file = Arc::clone(&file);
        let rows = Arc::clone(&rows);
        let next = Arc::clone(&next);
        handles.push(thread::spawn(
            move || -> Result<HotBlockAccountBenchStats> {
                let mut stats = HotBlockAccountBenchStats::default();
                let mut compressed_buf = Vec::with_capacity(2 << 20);
                let mut ids = Vec::with_capacity(4096);
                let mut decompressor =
                    zstd::bulk::Decompressor::new().context("create zstd decompressor")?;

                loop {
                    let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                    if start >= rows.len() {
                        break;
                    }
                    let end = start.saturating_add(chunk_size).min(rows.len());
                    for row in &rows[start..end] {
                        compressed_buf.resize(row.compressed_len as usize, 0);
                        read_exact_at(&file, &mut compressed_buf, row.compressed_offset)
                            .with_context(|| {
                                format!(
                                    "worker {worker_id} read compressed block_id {} at offset {}",
                                    row.block_id, row.compressed_offset
                                )
                            })?;
                        let block_bytes = decompressor
                            .decompress(&compressed_buf, row.uncompressed_len as usize)
                            .with_context(|| {
                                format!(
                                    "worker {worker_id} zstd decompress block_id {}",
                                    row.block_id
                                )
                            })?;
                        let block: ArchiveV2HotBlockBlob =
                            wincode::config::deserialize(&block_bytes, wincode_leb128_config())
                                .with_context(|| {
                                    format!(
                                        "worker {worker_id} decode hot block_id {}",
                                        row.block_id
                                    )
                                })?;
                        anyhow::ensure!(
                            block.header.slot == row.slot
                                && block.tx_rows.len() == row.tx_count as usize,
                            "worker {worker_id} block_id {} validation failed",
                            row.block_id
                        );
                        ids.clear();
                        collect_hot_block_account_ids(
                            &block,
                            include_metadata,
                            &mut ids,
                            &mut stats,
                        )
                        .with_context(|| {
                            format!(
                                "worker {worker_id} collect accounts block_id {} slot {}",
                                row.block_id, row.slot
                            )
                        })?;
                        stats.account_refs += ids.len() as u64;
                        ids.sort_unstable();
                        ids.dedup();
                        let unique = ids.len() as u64;
                        stats.unique_account_refs += unique;
                        stats.fixed_u32_bytes += unique * 4;
                        stats.varint_bytes += estimated_delta_varint_block_bytes(&ids);
                        if target_id.is_some_and(|id| ids.binary_search(&id).is_ok()) {
                            stats.candidate_blocks += 1;
                            stats.candidate_txs += row.tx_count as u64;
                        }
                        stats.compressed_bytes += row.compressed_len as u64;
                        stats.uncompressed_bytes += row.uncompressed_len as u64;
                        stats.blocks += 1;
                        stats.txs += row.tx_count as u64;
                    }
                }
                Ok(stats)
            },
        ));
    }

    let mut stats = HotBlockAccountBenchStats::default();
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow!("hot account benchmark worker panicked"))??;
        stats.merge(worker_stats);
    }

    let elapsed = started.elapsed().as_secs_f64();
    let sidecar_index_bytes = stats.blocks * 12;
    let sidecar_bytes = stats.varint_bytes + sidecar_index_bytes;
    let avg_unique = if stats.blocks > 0 {
        stats.unique_account_refs as f64 / stats.blocks as f64
    } else {
        0.0
    };
    let avg_refs = if stats.blocks > 0 {
        stats.account_refs as f64 / stats.blocks as f64
    } else {
        0.0
    };
    println!(
        "archive_v2_hot_block_account_table workers={workers} chunk_size={chunk_size} include_metadata={include_metadata} target_id={} blocks={} txs={} compressed_bytes={} uncompressed_bytes={} elapsed_s={elapsed:.3} tx_s={:.2} blocks_s={:.2} compressed_MiB_s={:.2} account_refs={} unique_account_refs={} avg_refs_per_block={avg_refs:.2} avg_unique_per_block={avg_unique:.2} raw_pubkeys={} metadata_decoded={} metadata_skipped={} fixed_u32_bytes={} delta_varint_bytes={} sidecar_index_bytes={} estimated_sidecar_bytes={} sidecar_bytes_per_block={:.2} target_candidate_blocks={} target_candidate_txs={} target_skip_blocks_pct={:.2} target_skip_txs_pct={:.2}",
        target_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        stats.blocks,
        stats.txs,
        stats.compressed_bytes,
        stats.uncompressed_bytes,
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.compressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        stats.account_refs,
        stats.unique_account_refs,
        stats.raw_pubkeys,
        stats.metadata_decoded,
        stats.metadata_skipped,
        stats.fixed_u32_bytes,
        stats.varint_bytes,
        sidecar_index_bytes,
        sidecar_bytes,
        if stats.blocks > 0 {
            sidecar_bytes as f64 / stats.blocks as f64
        } else {
            0.0
        },
        stats.candidate_blocks,
        stats.candidate_txs,
        if stats.blocks > 0 {
            100.0 - (stats.candidate_blocks as f64 * 100.0 / stats.blocks as f64)
        } else {
            0.0
        },
        if stats.txs > 0 {
            100.0 - (stats.candidate_txs as f64 * 100.0 / stats.txs as f64)
        } else {
            0.0
        },
    );
    Ok(())
}

fn collect_hot_block_account_ids(
    block: &ArchiveV2HotBlockBlob,
    include_metadata: bool,
    ids: &mut Vec<u32>,
    stats: &mut HotBlockAccountBenchStats,
) -> Result<()> {
    for tx_row in &block.tx_rows {
        if tx_row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            continue;
        }
        let message_slice = hot_block_region(
            &block.message_bytes,
            tx_row.message_offset,
            tx_row.message_len,
            "message",
            tx_row.tx_index,
        )?;
        let message: ArchiveV2HotMessagePayload =
            wincode::config::deserialize(message_slice, wincode_leb128_config())
                .with_context(|| format!("decode hot message tx_index={}", tx_row.tx_index))?;
        for key in hot_message_account_keys_for_analysis(&message) {
            push_compact_pubkey_id(*key, ids, stats);
        }
        if include_metadata && tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0 {
            if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                stats.metadata_skipped += 1;
                continue;
            }
            let metadata_slice = hot_block_region(
                &block.metadata_bytes,
                tx_row.metadata_offset,
                tx_row.metadata_len,
                "metadata",
                tx_row.tx_index,
            )?;
            let meta: CompactMetaLoadedAddressView =
                wincode::config::deserialize(metadata_slice, wincode_leb128_config())
                    .with_context(|| {
                        format!(
                            "decode hot metadata loaded-address view tx_index={}",
                            tx_row.tx_index
                        )
                    })?;
            stats.metadata_decoded += 1;
            for key in meta
                .loaded_writable_addresses
                .iter()
                .chain(meta.loaded_readonly_addresses.iter())
            {
                push_compact_pubkey_id(*key, ids, stats);
            }
        }
    }
    Ok(())
}

fn push_compact_pubkey_id(
    key: CompactPubkey,
    ids: &mut Vec<u32>,
    stats: &mut HotBlockAccountBenchStats,
) {
    match key {
        CompactPubkey::Id(id) => ids.push(id),
        CompactPubkey::Raw(_) => stats.raw_pubkeys += 1,
    }
}

fn estimated_delta_varint_block_bytes(ids: &[u32]) -> u64 {
    let mut bytes = uleb128_size(ids.len() as u64);
    let mut previous = 0u32;
    for &id in ids {
        bytes += uleb128_size(id.saturating_sub(previous) as u64);
        previous = id;
    }
    bytes
}

fn uleb128_size(mut value: u64) -> u64 {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn hot_block_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    label: &str,
    tx_index: u32,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let len = len as usize;
    let end = start
        .checked_add(len)
        .with_context(|| format!("{label} region overflow tx_index={tx_index}"))?;
    bytes
        .get(start..end)
        .with_context(|| format!("{label} region out of bounds tx_index={tx_index}"))
}

pub(crate) fn repack_hot_blocks_raw(
    input: &Path,
    output_dir: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    level: i32,
    max_blocks: Option<u64>,
) -> Result<()> {
    anyhow::ensure!(
        level >= 0,
        "whole-file zstd compression level must be non-negative, got {level}"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let input_index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let hot_index = read_archive_v2_hot_block_index(&input_index_path)?;
    let input_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        input_bytes == hot_index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        hot_index.blob_file_bytes,
        input_bytes
    );
    let dict_bytes = load_optional_dict(dict)?.unwrap_or_default();
    anyhow::ensure!(
        (hot_index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );

    copy_hot_archive_sidecars(input.parent().unwrap_or_else(|| Path::new(".")), output_dir)?;

    let raw_path = output_dir.join(ARCHIVE_RAW_BLOCKS_FILE);
    let raw_zstd_path = output_dir.join(ARCHIVE_RAW_BLOCKS_ZSTD_FILE);
    let index_path = output_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    anyhow::ensure!(
        input != raw_path && input != raw_zstd_path,
        "input and output paths must differ"
    );

    let mut input_file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let raw_file =
        File::create(&raw_path).with_context(|| format!("create {}", raw_path.display()))?;
    let mut raw_writer = BufWriter::with_capacity(BUFFER_SIZE, raw_file);
    let zstd_file = File::create(&raw_zstd_path)
        .with_context(|| format!("create {}", raw_zstd_path.display()))?;
    let zstd_writer = BufWriter::with_capacity(BUFFER_SIZE, zstd_file);
    let mut whole_zstd = zstd::stream::write::Encoder::new(zstd_writer, level)
        .with_context(|| format!("create zstd encoder {}", raw_zstd_path.display()))?;
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
        .context("create zstd dictionary decompressor")?;
    let mut compressed_buf = Vec::with_capacity(2 << 20);

    let limit = max_blocks
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(usize::MAX);
    let started = Instant::now();
    let mut progress = ProgressTracker::new("Archive V2 Hot Raw Repack");
    let mut current_input_offset = 0u64;
    let mut output_offset = 0u64;
    let mut output_rows = Vec::with_capacity(hot_index.rows.len().min(limit));
    let mut input_compressed_bytes = 0u64;
    let mut raw_bytes = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;

    for row in hot_index.rows.iter().take(limit) {
        if current_input_offset != row.compressed_offset {
            input_file
                .seek(SeekFrom::Start(row.compressed_offset))
                .with_context(|| {
                    format!(
                        "seek hot archive {} to compressed offset {}",
                        input.display(),
                        row.compressed_offset
                    )
                })?;
            current_input_offset = row.compressed_offset;
        }
        compressed_buf.resize(row.compressed_len as usize, 0);
        input_file
            .read_exact(&mut compressed_buf)
            .with_context(|| {
                format!(
                    "read hot archive block_id {} at compressed offset {}",
                    row.block_id, row.compressed_offset
                )
            })?;
        current_input_offset = current_input_offset.saturating_add(row.compressed_len as u64);

        let block_bytes = decompressor
            .decompress(&compressed_buf, row.uncompressed_len as usize)
            .with_context(|| format!("zstd decompress hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block_bytes.len() == row.uncompressed_len as usize,
            "block_id {} decompressed length mismatch: decoded={} index={}",
            row.block_id,
            block_bytes.len(),
            row.uncompressed_len
        );
        let block: ArchiveV2HotBlockBlob = deserialize_archive_v2_hot_block_blob(&block_bytes)
            .with_context(|| format!("decode hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block.header.slot == row.slot && block.tx_rows.len() == row.tx_count as usize,
            "hot block validation failed for block_id {} slot {}",
            row.block_id,
            row.slot
        );

        raw_writer
            .write_all(&block_bytes)
            .with_context(|| format!("write {}", raw_path.display()))?;
        whole_zstd
            .write_all(&block_bytes)
            .with_context(|| format!("write {}", raw_zstd_path.display()))?;
        let raw_len = u32::try_from(block_bytes.len()).context("raw hot block exceeds u32::MAX")?;
        output_rows.push(ArchiveV2HotBlockIndexRow {
            block_id: row.block_id,
            slot: row.slot,
            compressed_offset: output_offset,
            compressed_len: raw_len,
            uncompressed_len: raw_len,
            tx_count: row.tx_count,
            first_tx_ordinal: row.first_tx_ordinal,
            first_signature_ordinal: row.first_signature_ordinal,
            signature_count: row.signature_count,
        });
        output_offset += raw_len as u64;
        input_compressed_bytes += row.compressed_len as u64;
        raw_bytes += raw_len as u64;
        blocks += 1;
        txs += row.tx_count as u64;
        progress.update_slot(row.slot);
        progress.update(1, row.tx_count as u64);
    }

    raw_writer
        .flush()
        .with_context(|| format!("flush {}", raw_path.display()))?;
    let mut zstd_writer = whole_zstd
        .finish()
        .with_context(|| format!("finish {}", raw_zstd_path.display()))?;
    zstd_writer
        .flush()
        .with_context(|| format!("flush {}", raw_zstd_path.display()))?;
    let whole_zstd_bytes = std::fs::metadata(&raw_zstd_path)
        .with_context(|| format!("stat {}", raw_zstd_path.display()))?
        .len();
    write_archive_v2_hot_block_index(
        &index_path,
        output_offset,
        0,
        ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS,
        &output_rows,
    )?;
    progress.final_report();

    let elapsed = started.elapsed().as_secs_f64();
    let input_ratio = if raw_bytes > 0 {
        input_compressed_bytes as f64 * 100.0 / raw_bytes as f64
    } else {
        0.0
    };
    let whole_ratio = if raw_bytes > 0 {
        whole_zstd_bytes as f64 * 100.0 / raw_bytes as f64
    } else {
        0.0
    };
    let raw_mib_s = if elapsed > 0.0 {
        raw_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    info!(
        "Archive V2 hot raw repack complete in {:.2}s: blocks={} txs={} max_blocks={:?} input_compressed_bytes={} raw_bytes={} whole_zstd_bytes={} input_ratio_pct={:.2} whole_zstd_ratio_pct={:.2} raw_MiB_s={:.2} raw={} raw_zstd={} index={}",
        elapsed,
        blocks,
        txs,
        max_blocks,
        input_compressed_bytes,
        raw_bytes,
        whole_zstd_bytes,
        input_ratio,
        whole_ratio,
        raw_mib_s,
        raw_path.display(),
        raw_zstd_path.display(),
        index_path.display(),
    );
    println!(
        "archive_v2_hot_raw_repack blocks={blocks} txs={txs} elapsed_s={elapsed:.3} input_compressed_bytes={input_compressed_bytes} raw_bytes={raw_bytes} whole_zstd_bytes={whole_zstd_bytes} input_ratio_pct={input_ratio:.2} whole_zstd_ratio_pct={whole_ratio:.2} raw_MiB_s={raw_mib_s:.2}"
    );
    Ok(())
}

pub(crate) fn build_block_access_sidecar(
    input: &Path,
    output_dir: Option<&Path>,
    index: Option<&Path>,
    dict: Option<&Path>,
    max_blocks: Option<u64>,
) -> Result<()> {
    build_block_access_sidecar_inner(input, output_dir, index, dict, max_blocks, None)
}

fn build_block_access_sidecar_with_previous_tail(
    input: &Path,
    output_dir: &Path,
    index: &Path,
    previous_tail: &[PreviousBlockhash],
) -> Result<()> {
    anyhow::ensure!(
        !previous_tail.is_empty(),
        "explicit previous blockhash tail must not be empty"
    );
    build_block_access_sidecar_inner(
        input,
        Some(output_dir),
        Some(index),
        None,
        None,
        Some(previous_tail),
    )
}

fn build_block_access_sidecar_inner(
    input: &Path,
    output_dir: Option<&Path>,
    index: Option<&Path>,
    dict: Option<&Path>,
    max_blocks: Option<u64>,
    previous_tail_override: Option<&[PreviousBlockhash]>,
) -> Result<()> {
    let input_dir = input.parent().unwrap_or_else(|| Path::new("."));
    let output_dir = output_dir.unwrap_or(input_dir);
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| archive_v2_hot_index_path(input));
    let hot_index = read_archive_v2_hot_block_index(&index_path)?;
    let input_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        input_bytes == hot_index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        hot_index.blob_file_bytes,
        input_bytes
    );

    let store = KeyStore::load(&input_dir.join(REGISTRY_FILE))
        .with_context(|| format!("load {}", input_dir.join(REGISTRY_FILE).display()))?;
    let blockhashes = load_blockhash_registry_plain(&input_dir.join(BLOCKHASH_REGISTRY_FILE))?;
    let vote_hash_rows = load_vote_hash_registry(
        &input_dir.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
    )
    .with_context(|| {
        format!(
            "load {}",
            input_dir.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE).display()
        )
    })?;
    let loaded_previous_tail;
    let previous_tail = if let Some(previous_tail) = previous_tail_override {
        previous_tail
    } else {
        loaded_previous_tail = read_prev_blockhash_tail(input_dir)?.unwrap_or_default();
        &loaded_previous_tail
    };
    let mut signatures_file =
        File::open(input_dir.join(ARCHIVE_V2_SIGNATURES_FILE)).with_context(|| {
            format!(
                "open {}",
                input_dir.join(ARCHIVE_V2_SIGNATURES_FILE).display()
            )
        })?;

    let raw_blocks = hot_index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0;
    let dict_bytes = load_optional_dict(dict)?.unwrap_or_default();
    anyhow::ensure!(
        raw_blocks
            || (hot_index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0)
            || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );

    let access_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let access_index_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let access_file =
        File::create(&access_path).with_context(|| format!("create {}", access_path.display()))?;
    let mut access_writer = BufWriter::with_capacity(BUFFER_SIZE, access_file);
    let mut input_file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut decompressor = if raw_blocks {
        None
    } else {
        Some(
            zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
                .context("create zstd dictionary decompressor")?,
        )
    };
    let mut input_buf = Vec::with_capacity(2 << 20);
    let mut block_bytes = Vec::with_capacity(2 << 20);
    let mut signature_bytes = Vec::new();
    let mut access_bytes = Vec::new();
    let mut access_rows = Vec::new();
    let mut current_input_offset = 0u64;
    let mut access_offset = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut signatures = 0u64;
    let started = Instant::now();
    let mut progress = ProgressTracker::new("Archive V2 Block Access");
    let limit = max_blocks
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(usize::MAX);

    for row in hot_index.rows.iter().take(limit) {
        if current_input_offset != row.compressed_offset {
            input_file
                .seek(SeekFrom::Start(row.compressed_offset))
                .with_context(|| {
                    format!(
                        "seek hot archive {} to offset {}",
                        input.display(),
                        row.compressed_offset
                    )
                })?;
            current_input_offset = row.compressed_offset;
        }

        if raw_blocks {
            block_bytes.resize(row.uncompressed_len as usize, 0);
            input_file.read_exact(&mut block_bytes).with_context(|| {
                format!(
                    "read raw hot block_id {} at offset {}",
                    row.block_id, row.compressed_offset
                )
            })?;
            current_input_offset = current_input_offset.saturating_add(row.uncompressed_len as u64);
        } else {
            input_buf.resize(row.compressed_len as usize, 0);
            input_file.read_exact(&mut input_buf).with_context(|| {
                format!(
                    "read hot block_id {} at compressed offset {}",
                    row.block_id, row.compressed_offset
                )
            })?;
            current_input_offset = current_input_offset.saturating_add(row.compressed_len as u64);
            block_bytes = decompressor
                .as_mut()
                .expect("zstd decompressor initialized")
                .decompress(&input_buf, row.uncompressed_len as usize)
                .with_context(|| format!("zstd decompress hot block_id {}", row.block_id))?;
        }

        anyhow::ensure!(
            block_bytes.len() == row.uncompressed_len as usize,
            "block_id {} decoded length mismatch: decoded={} index={}",
            row.block_id,
            block_bytes.len(),
            row.uncompressed_len
        );
        let block: ArchiveV2HotBlockBlob = deserialize_archive_v2_hot_block_blob(&block_bytes)
            .with_context(|| format!("decode hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block.header.slot == row.slot && block.tx_rows.len() == row.tx_count as usize,
            "hot block validation failed for block_id {} slot {}",
            row.block_id,
            row.slot
        );

        let signature_len = row.signature_count as usize * 64;
        signature_bytes.resize(signature_len, 0);
        let signature_offset = row
            .first_signature_ordinal
            .checked_mul(64)
            .context("signature offset overflow")?;
        signatures_file
            .seek(SeekFrom::Start(signature_offset))
            .with_context(|| format!("seek signatures to {signature_offset}"))?;
        signatures_file
            .read_exact(&mut signature_bytes)
            .with_context(|| format!("read signatures for block_id {}", row.block_id))?;

        let access_blob = build_archive_v2_block_access_blob(
            &block,
            &store.keys,
            &blockhashes,
            previous_tail,
            &signature_bytes,
            &vote_hash_rows,
        )
        .with_context(|| {
            format!(
                "build block access sidecar block_id {} slot {}",
                row.block_id, row.slot
            )
        })?;
        access_bytes.clear();
        wincode::config::serialize_into(&mut access_bytes, &access_blob, wincode_leb128_config())?;
        let access_len = checked_archive_v2_block_access_frame_len(access_bytes.len(), row.slot)?;
        access_writer
            .write_all(&access_bytes)
            .with_context(|| format!("write {}", access_path.display()))?;
        access_rows.push(ArchiveV2BlockAccessIndexRow {
            block_id: row.block_id,
            slot: row.slot,
            access_offset,
            access_len,
            tx_count: row.tx_count,
            signature_count: row.signature_count,
        });
        access_offset += access_len as u64;
        blocks += 1;
        txs += row.tx_count as u64;
        signatures += row.signature_count as u64;
        progress.update_slot(row.slot);
        progress.update(1, row.tx_count as u64);
    }

    access_writer
        .flush()
        .with_context(|| format!("flush {}", access_path.display()))?;
    write_archive_v2_block_access_index(&access_index_path, access_offset, 0, &access_rows)?;
    let epoch = infer_hot_block_epoch(&hot_index.rows)?;
    let get_block_rows = build_get_block_index_rows(&hot_index.rows, &access_rows, epoch)?;
    let get_block_index_path = output_dir.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE);
    write_archive_v2_get_block_index(&get_block_index_path, &get_block_rows)?;
    progress.final_report();

    let elapsed = started.elapsed().as_secs_f64();
    let mib_s = if elapsed > 0.0 {
        access_offset as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    info!(
        "Archive V2 block-access sidecar complete in {:.2}s: blocks={} txs={} signatures={} bytes={} MiB_s={:.2} access={} index={} get_block_index={}",
        elapsed,
        blocks,
        txs,
        signatures,
        access_offset,
        mib_s,
        access_path.display(),
        access_index_path.display(),
        get_block_index_path.display(),
    );
    println!(
        "archive_v2_block_access blocks={blocks} txs={txs} signatures={signatures} bytes={access_offset} elapsed_s={elapsed:.3} MiB_s={mib_s:.2} get_block_index={}",
        get_block_index_path.display()
    );
    Ok(())
}

pub(crate) fn build_get_block_index(
    input: &Path,
    index: Option<&Path>,
    access_index: Option<&Path>,
    output: Option<&Path>,
    epoch: Option<u64>,
) -> Result<()> {
    let input_dir = input.parent().unwrap_or_else(|| Path::new("."));
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| archive_v2_hot_index_path(input));
    let access_index_path = access_index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| input_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE));
    let output_path = output
        .map(Path::to_path_buf)
        .unwrap_or_else(|| archive_v2_get_block_index_path(input));

    let hot_index = read_archive_v2_hot_block_index(&index_path)?;
    let access_index = read_archive_v2_block_access_index(&access_index_path)?;
    let epoch = match epoch {
        Some(epoch) => epoch,
        None => infer_hot_block_epoch(&hot_index.rows)?,
    };
    let rows = build_get_block_index_rows(&hot_index.rows, &access_index.rows, epoch)?;
    let present = rows.iter().filter(|row| !row.is_missing()).count();
    write_archive_v2_get_block_index(&output_path, &rows)?;
    let bytes = std::fs::metadata(&output_path)
        .with_context(|| format!("stat {}", output_path.display()))?
        .len();
    info!(
        "Archive V2 get-block slot-offset index complete: epoch={} rows={} present={} bytes={} output={}",
        epoch,
        rows.len(),
        present,
        bytes,
        output_path.display()
    );
    println!(
        "archive_v2_get_block_index epoch={epoch} rows={} present={present} bytes={bytes} output={}",
        rows.len(),
        output_path.display()
    );
    Ok(())
}

fn infer_hot_block_epoch(rows: &[ArchiveV2HotBlockIndexRow]) -> Result<u64> {
    let first = rows
        .first()
        .ok_or_else(|| anyhow!("cannot infer epoch from empty hot-block index"))?;
    Ok(first.slot / crate::SLOTS_PER_EPOCH)
}

fn build_get_block_index_rows(
    hot_rows: &[ArchiveV2HotBlockIndexRow],
    access_rows: &[ArchiveV2BlockAccessIndexRow],
    epoch: u64,
) -> Result<Vec<ArchiveV2GetBlockIndexRow>> {
    let row_count =
        usize::try_from(crate::SLOTS_PER_EPOCH).context("SLOTS_PER_EPOCH does not fit in usize")?;
    let mut rows = vec![ArchiveV2GetBlockIndexRow::missing(); row_count];
    for hot_row in hot_rows {
        anyhow::ensure!(
            hot_row.slot / crate::SLOTS_PER_EPOCH == epoch,
            "slot {} belongs to epoch {}, not requested epoch {}",
            hot_row.slot,
            hot_row.slot / crate::SLOTS_PER_EPOCH,
            epoch
        );
        anyhow::ensure!(
            hot_row.compressed_len > 0,
            "slot {} has empty hot-block payload",
            hot_row.slot
        );
        let access_row_index =
            usize::try_from(hot_row.block_id).context("block id does not fit in usize")?;
        let access_row = access_rows.get(access_row_index).ok_or_else(|| {
            anyhow!(
                "missing block-access row for block_id {} slot {}",
                hot_row.block_id,
                hot_row.slot
            )
        })?;
        anyhow::ensure!(
            access_row.block_id == hot_row.block_id
                && access_row.slot == hot_row.slot
                && access_row.tx_count == hot_row.tx_count
                && access_row.signature_count == hot_row.signature_count,
            "block-access index mismatch for block_id {} slot {}",
            hot_row.block_id,
            hot_row.slot
        );
        anyhow::ensure!(
            access_row.access_len > 0,
            "slot {} has empty block-access payload",
            hot_row.slot
        );
        let slot_index = usize::try_from(hot_row.slot % crate::SLOTS_PER_EPOCH)
            .context("slot index does not fit in usize")?;
        anyhow::ensure!(
            rows[slot_index].is_missing(),
            "duplicate get-block index entry for slot {}",
            hot_row.slot
        );
        rows[slot_index] = ArchiveV2GetBlockIndexRow {
            block_offset: hot_row.compressed_offset,
            block_len: hot_row.compressed_len,
            access_offset: access_row.access_offset,
            access_len: access_row.access_len,
        };
    }
    Ok(rows)
}

pub(crate) fn bench_raw_hot_blocks(
    input: &Path,
    index: Option<&Path>,
    whole_zstd: bool,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    if whole_zstd {
        return bench_raw_hot_blocks_whole_zstd(input, index);
    }

    #[cfg(not(unix))]
    {
        let _ = (input, index, workers, chunk_size);
        anyhow::bail!("raw Archive V2 benchmark currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        bench_raw_hot_blocks_unix(input, index, workers, chunk_size)
    }
}

#[cfg(unix)]
fn bench_raw_hot_blocks_unix(
    input: &Path,
    index: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_hot_block_index(&index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
        "{} is not a raw-block Archive V2 index",
        index_path.display()
    );
    let file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        file_bytes == index.blob_file_bytes,
        "index was built for {} raw bytes, but archive is {} bytes",
        index.blob_file_bytes,
        file_bytes
    );

    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let rows = Arc::new(index.rows);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let file = Arc::clone(&file);
        let rows = Arc::clone(&rows);
        let next = Arc::clone(&next);
        handles.push(thread::spawn(move || -> Result<ArchiveV2ZstdBenchStats> {
            let mut stats = ArchiveV2ZstdBenchStats::default();
            let mut block_buf = Vec::with_capacity(2 << 20);
            loop {
                let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                if start >= rows.len() {
                    break;
                }
                let end = start.saturating_add(chunk_size).min(rows.len());
                for row in &rows[start..end] {
                    block_buf.resize(row.uncompressed_len as usize, 0);
                    read_exact_at(&file, &mut block_buf, row.compressed_offset).with_context(
                        || {
                            format!(
                                "worker {worker_id} read raw block_id {} at offset {}",
                                row.block_id, row.compressed_offset
                            )
                        },
                    )?;
                    let block: ArchiveV2HotBlockBlob =
                        wincode::config::deserialize(&block_buf, wincode_leb128_config())
                            .with_context(|| {
                                format!(
                                    "worker {worker_id} decode raw hot block_id {}",
                                    row.block_id
                                )
                            })?;
                    anyhow::ensure!(
                        block.header.slot == row.slot
                            && block.tx_rows.len() == row.tx_count as usize,
                        "worker {worker_id} raw hot block validation failed for block_id {}",
                        row.block_id
                    );
                    stats.compressed_bytes += row.uncompressed_len as u64;
                    stats.uncompressed_bytes += row.uncompressed_len as u64;
                    stats.blocks += 1;
                    stats.txs += row.tx_count as u64;
                }
            }
            Ok(stats)
        }));
    }

    let mut stats = ArchiveV2ZstdBenchStats::default();
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow!("raw Archive V2 benchmark worker panicked"))??;
        stats.compressed_bytes += worker_stats.compressed_bytes;
        stats.uncompressed_bytes += worker_stats.uncompressed_bytes;
        stats.blocks += worker_stats.blocks;
        stats.txs += worker_stats.txs;
    }

    print_zstd_block_bench(
        "archive_v2_hot_raw_block_read",
        workers,
        chunk_size,
        0,
        file_bytes,
        &stats,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

fn bench_raw_hot_blocks_whole_zstd(input: &Path, index: Option<&Path>) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_hot_block_index(&index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
        "{} is not a raw-block Archive V2 index",
        index_path.display()
    );
    let file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut decoder = zstd_decoder_with_long_window(reader)
        .with_context(|| format!("zstd decode {}", input.display()))?;
    let mut block_buf = Vec::with_capacity(2 << 20);
    let mut stats = ArchiveV2ZstdBenchStats::default();
    let started = Instant::now();
    let mut progress = ProgressTracker::new("Archive V2 Hot Raw Whole-Zstd Read");

    for row in &index.rows {
        block_buf.resize(row.uncompressed_len as usize, 0);
        decoder.read_exact(&mut block_buf).with_context(|| {
            format!(
                "read whole-zstd raw block_id {} at decompressed offset {}",
                row.block_id, row.compressed_offset
            )
        })?;
        let block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&block_buf, wincode_leb128_config())
                .with_context(|| format!("decode whole-zstd raw hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block.header.slot == row.slot && block.tx_rows.len() == row.tx_count as usize,
            "whole-zstd raw hot block validation failed for block_id {}",
            row.block_id
        );
        stats.uncompressed_bytes += row.uncompressed_len as u64;
        stats.blocks += 1;
        stats.txs += row.tx_count as u64;
        progress.update_slot(row.slot);
        progress.update(1, row.tx_count as u64);
    }
    let mut trailing = [0u8; 1];
    anyhow::ensure!(
        decoder.read(&mut trailing)? == 0,
        "{} has trailing decompressed bytes after indexed raw blocks",
        input.display()
    );
    stats.compressed_bytes = file_bytes;
    progress.final_report();
    print_zstd_block_bench(
        "archive_v2_hot_raw_whole_zstd_read",
        1,
        1,
        0,
        file_bytes,
        &stats,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

fn copy_hot_archive_sidecars(input_dir: &Path, output_dir: &Path) -> Result<()> {
    for name in [
        ARCHIVE_ZSTD_META_FILE,
        ARCHIVE_V2_BLOCK_ACCESS_FILE,
        ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
        POH_FILE,
        SHREDDING_FILE,
        REGISTRY_FILE,
        REGISTRY_COUNTS_FILE,
        BLOCKHASH_REGISTRY_FILE,
        PREV_BLOCKHASH_TAIL_FILE,
    ] {
        let source = input_dir.join(name);
        if !crate::file_nonempty(&source) {
            continue;
        }
        let dest = output_dir.join(name);
        if source != dest {
            std::fs::copy(&source, &dest)
                .with_context(|| format!("copy {} to {}", source.display(), dest.display()))?;
        }
    }
    Ok(())
}

pub(crate) fn extract_largest_zstd_block(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    output: &Path,
    by_tx_count: bool,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (input, index, dict, output, by_tx_count);
        anyhow::bail!("zstd-block extraction currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        extract_largest_zstd_block_unix(input, index, dict, output, by_tx_count)
    }
}

#[cfg(unix)]
fn extract_largest_zstd_block_unix(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    output: &Path,
    by_tx_count: bool,
) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_zstd_block_index(&index_path)?;
    let row = index
        .rows
        .iter()
        .max_by_key(|row| {
            if by_tx_count {
                (row.tx_count as u64, row.uncompressed_len as u64)
            } else {
                (row.uncompressed_len as u64, row.tx_count as u64)
            }
        })
        .copied()
        .context("zstd-block index has no block rows")?;
    let dict_bytes = load_optional_dict(dict)?.unwrap_or_default();
    anyhow::ensure!(
        (index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut compressed = vec![0u8; row.compressed_len as usize];
    read_exact_at(&file, &mut compressed, row.compressed_offset).with_context(|| {
        format!(
            "read largest block_id {} at compressed offset {}",
            row.block_id, row.compressed_offset
        )
    })?;
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
        .context("create zstd dictionary decompressor")?;
    let block_bytes = decompressor
        .decompress(&compressed, row.uncompressed_len as usize)
        .with_context(|| format!("zstd decompress block_id {}", row.block_id))?;
    let block: ArchiveV2HotBlockBlob =
        wincode::config::deserialize(&block_bytes, wincode_leb128_config())
            .with_context(|| format!("decode hot block_id {}", row.block_id))?;
    anyhow::ensure!(
        block.header.slot == row.slot && block.tx_rows.len() == row.tx_count as usize,
        "largest hot block validation failed for block_id {}",
        row.block_id
    );
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let output_file =
        File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, output_file));
    writer.write_bytes(&block_bytes)?;
    writer.flush()?;
    println!(
        "archive_v2_largest_hot_block block_id={} slot={} txs={} uncompressed_len={} compressed_len={} by_tx_count={} output={}",
        row.block_id,
        row.slot,
        row.tx_count,
        row.uncompressed_len,
        row.compressed_len,
        by_tx_count,
        output.display()
    );
    Ok(())
}

pub(crate) fn bench_single_block(input: &Path, iterations: usize) -> Result<()> {
    let iterations = iterations.max(1);
    let file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let Some((payload_len, payload)) = reader.read_bytes()? else {
        anyhow::bail!("{} is empty", input.display());
    };
    anyhow::ensure!(
        reader.read_bytes()?.is_none(),
        "{} contains more than one framed record",
        input.display()
    );
    let block: ArchiveV2HotBlockBlob =
        wincode::config::deserialize(&payload, wincode_leb128_config())
            .with_context(|| format!("{} is not a single Archive V2 hot block", input.display()))?;
    let slot = block.header.slot;
    let tx_count = block.tx_rows.len();
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iterations {
        let block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&payload, wincode_leb128_config())?;
        checksum = checksum.wrapping_add(block.tx_rows.len());
    }
    let elapsed = started.elapsed().as_secs_f64();
    let mib_s = if elapsed > 0.0 {
        payload.len() as f64 * iterations as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    println!(
        "archive_v2_hot_single_block_decode iterations={iterations} slot={slot} txs={tx_count} file_bytes={file_bytes} payload_len={payload_len} elapsed_s={elapsed:.6} payload_MiB_s={mib_s:.2} checksum={checksum}"
    );
    Ok(())
}

fn scan_archive_v2_hot_blocks<F>(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    max_blocks: Option<u64>,
    progress_label: &'static str,
    mut visit: F,
) -> Result<ArchiveV2ZstdBenchStats>
where
    F: FnMut(&ArchiveV2ZstdBlockIndexRow, &ArchiveV2HotBlockBlob) -> Result<()>,
{
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_zstd_block_index(&index_path)?;
    let blob_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        blob_file_bytes == index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        index.blob_file_bytes,
        blob_file_bytes
    );
    let dict_bytes = load_optional_dict(dict)?.unwrap_or_default();
    anyhow::ensure!(
        (index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );

    let mut file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut compressed_buf = Vec::with_capacity(2 << 20);
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
        .context("create zstd dictionary decompressor")?;
    let mut progress = ProgressTracker::new(progress_label);
    let mut stats = ArchiveV2ZstdBenchStats::default();
    let limit = max_blocks
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(usize::MAX);
    let mut current_offset = 0u64;

    for row in index.rows.iter().take(limit) {
        if current_offset != row.compressed_offset {
            file.seek(SeekFrom::Start(row.compressed_offset))
                .with_context(|| {
                    format!(
                        "seek hot archive {} to compressed offset {}",
                        input.display(),
                        row.compressed_offset
                    )
                })?;
            current_offset = row.compressed_offset;
        }
        compressed_buf.resize(row.compressed_len as usize, 0);
        file.read_exact(&mut compressed_buf).with_context(|| {
            format!(
                "read hot archive block_id {} at compressed offset {}",
                row.block_id, row.compressed_offset
            )
        })?;
        current_offset = current_offset.saturating_add(row.compressed_len as u64);

        let block_bytes = decompressor
            .decompress(&compressed_buf, row.uncompressed_len as usize)
            .with_context(|| format!("zstd decompress hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block_bytes.len() == row.uncompressed_len as usize,
            "block_id {} decompressed length mismatch: decoded={} index={}",
            row.block_id,
            block_bytes.len(),
            row.uncompressed_len
        );
        let block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&block_bytes, wincode_leb128_config())
                .with_context(|| format!("decode hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block.header.slot == row.slot,
            "hot block_id {} slot mismatch: decoded={} index={}",
            row.block_id,
            block.header.slot,
            row.slot
        );
        anyhow::ensure!(
            block.tx_rows.len() == row.tx_count as usize,
            "hot block_id {} tx count mismatch: decoded={} index={}",
            row.block_id,
            block.tx_rows.len(),
            row.tx_count
        );

        visit(row, &block)?;
        stats.compressed_bytes += row.compressed_len as u64;
        stats.uncompressed_bytes += row.uncompressed_len as u64;
        stats.blocks += 1;
        stats.txs += row.tx_count as u64;
        progress.update_slot(row.slot);
        progress.update(1, row.tx_count as u64);
    }

    progress.final_report();
    Ok(stats)
}

fn hot_block_message_slice<'a>(
    block: &'a ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
    block_id: u32,
) -> Result<&'a [u8]> {
    hot_block_region_slice(
        &block.message_bytes,
        row.message_offset,
        row.message_len,
        "message",
        block_id,
        row.tx_index,
    )
}

fn hot_block_metadata_slice<'a>(
    block: &'a ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
    block_id: u32,
) -> Result<&'a [u8]> {
    hot_block_region_slice(
        &block.metadata_bytes,
        row.metadata_offset,
        row.metadata_len,
        "metadata",
        block_id,
        row.tx_index,
    )
}

fn hot_block_region_slice<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    region: &'static str,
    block_id: u32,
    tx_index: u32,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let len = len as usize;
    let end = start.checked_add(len).with_context(|| {
        format!("{region} slice overflow block_id={block_id} tx_index={tx_index}")
    })?;
    bytes.get(start..end).with_context(|| {
        format!(
            "{region} slice out of bounds block_id={block_id} tx_index={tx_index} offset={offset} len={len} region_len={}",
            bytes.len()
        )
    })
}

fn print_zstd_block_bench(
    label: &'static str,
    workers: usize,
    chunk_size: usize,
    level: i32,
    blob_file_bytes: u64,
    stats: &ArchiveV2ZstdBenchStats,
    elapsed: f64,
) {
    let compressed_mib_s = if elapsed > 0.0 {
        stats.compressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let uncompressed_mib_s = if elapsed > 0.0 {
        stats.uncompressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let block_s = if elapsed > 0.0 {
        stats.blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        stats.txs as f64 / elapsed
    } else {
        0.0
    };
    let ratio = if stats.uncompressed_bytes > 0 {
        stats.compressed_bytes as f64 * 100.0 / stats.uncompressed_bytes as f64
    } else {
        0.0
    };
    println!(
        "{label} workers={workers} chunk_size={chunk_size} level={level} blocks={} txs={} blob_file_bytes={blob_file_bytes} compressed_bytes={} uncompressed_bytes={} ratio_pct={ratio:.2} elapsed_s={elapsed:.3} compressed_MiB_s={compressed_mib_s:.2} uncompressed_MiB_s={uncompressed_mib_s:.2} blocks_s={block_s:.2} tx_s={tx_s:.2}",
        stats.blocks, stats.txs, stats.compressed_bytes, stats.uncompressed_bytes
    );
}

fn default_archive_v2_index_path(input: &Path) -> PathBuf {
    let index_name = input
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            name.strip_suffix(".wincode")
                .map(|stem| format!("{stem}.index"))
                .unwrap_or_else(|| format!("{name}.index"))
        })
        .unwrap_or_else(|| ARCHIVE_INDEX_FILE.to_string());
    input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(index_name)
}

fn default_archive_v2_zstd_index_path(input: &Path) -> PathBuf {
    archive_v2_hot_index_path(input)
}

fn read_u32_varint_with_encoded_len<R: Read>(reader: &mut R) -> Result<Option<(u32, usize)>> {
    let mut value = 0u32;
    let mut shift = 0;
    let mut encoded_len = 0usize;

    loop {
        let mut byte = [0u8; 1];
        match reader.read(&mut byte) {
            Ok(0) if encoded_len == 0 => return Ok(None),
            Ok(0) => anyhow::bail!("unexpected EOF in archive v2 frame length varint"),
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(err).context("read archive v2 frame length varint"),
        }
        encoded_len += 1;
        value |= ((byte[0] & 0x7f) as u32) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(Some((value, encoded_len)));
        }
        shift += 7;
        anyhow::ensure!(shift <= 28, "archive v2 frame length varint overflow");
    }
}

fn write_archive_v2_block_index(
    path: &Path,
    archive_file_bytes: u64,
    rows: &[ArchiveV2BlockIndexRow],
) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let tmp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(ARCHIVE_INDEX_FILE)
    ));
    let file = File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    writer.write_all(ARCHIVE_V2_INDEX_MAGIC)?;
    writer.write_all(&ARCHIVE_V2_INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(rows.len() as u64).to_le_bytes())?;
    writer.write_all(&archive_file_bytes.to_le_bytes())?;
    for row in rows {
        writer.write_all(&row.block_id.to_le_bytes())?;
        writer.write_all(&row.slot.to_le_bytes())?;
        writer.write_all(&row.frame_offset.to_le_bytes())?;
        writer.write_all(&row.payload_offset.to_le_bytes())?;
        writer.write_all(&row.payload_len.to_le_bytes())?;
        writer.write_all(&row.tx_count.to_le_bytes())?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn write_archive_v2_zstd_block_index(
    path: &Path,
    blob_file_bytes: u64,
    level: i32,
    flags: u32,
    rows: &[ArchiveV2ZstdBlockIndexRow],
) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let tmp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(ARCHIVE_ZSTD_INDEX_FILE)
    ));
    let file = File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    writer.write_all(ARCHIVE_V2_ZSTD_INDEX_MAGIC)?;
    writer.write_all(&ARCHIVE_V2_ZSTD_INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(rows.len() as u64).to_le_bytes())?;
    writer.write_all(&blob_file_bytes.to_le_bytes())?;
    writer.write_all(&level.to_le_bytes())?;
    writer.write_all(&flags.to_le_bytes())?;
    for row in rows {
        writer.write_all(&row.block_id.to_le_bytes())?;
        writer.write_all(&row.slot.to_le_bytes())?;
        writer.write_all(&row.compressed_offset.to_le_bytes())?;
        writer.write_all(&row.compressed_len.to_le_bytes())?;
        writer.write_all(&row.uncompressed_len.to_le_bytes())?;
        writer.write_all(&row.tx_count.to_le_bytes())?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn read_archive_v2_block_index(path: &Path) -> Result<ArchiveV2BlockIndex> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut header = [0u8; ARCHIVE_V2_INDEX_HEADER_LEN];
    reader
        .read_exact(&mut header)
        .with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(
        &header[..8] == ARCHIVE_V2_INDEX_MAGIC,
        "{} is not an Archive V2 block index",
        path.display()
    );
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    anyhow::ensure!(
        version == ARCHIVE_V2_INDEX_VERSION,
        "{} has unsupported Archive V2 index version {version}",
        path.display()
    );
    let row_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let archive_file_bytes = u64::from_le_bytes(header[20..28].try_into().unwrap());
    let row_count_usize = usize::try_from(row_count).context("index row count exceeds usize")?;
    let expected_len =
        ARCHIVE_V2_INDEX_HEADER_LEN as u64 + row_count * ARCHIVE_V2_INDEX_ROW_LEN as u64;
    let actual_len = std::fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    anyhow::ensure!(
        actual_len == expected_len,
        "{} has size {}, expected {} for {} rows",
        path.display(),
        actual_len,
        expected_len,
        row_count
    );

    let mut rows = Vec::with_capacity(row_count_usize);
    let mut row_buf = [0u8; ARCHIVE_V2_INDEX_ROW_LEN];
    for _ in 0..row_count_usize {
        reader
            .read_exact(&mut row_buf)
            .with_context(|| format!("read row from {}", path.display()))?;
        rows.push(ArchiveV2BlockIndexRow {
            block_id: u32::from_le_bytes(row_buf[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(row_buf[4..12].try_into().unwrap()),
            frame_offset: u64::from_le_bytes(row_buf[12..20].try_into().unwrap()),
            payload_offset: u64::from_le_bytes(row_buf[20..28].try_into().unwrap()),
            payload_len: u32::from_le_bytes(row_buf[28..32].try_into().unwrap()),
            tx_count: u32::from_le_bytes(row_buf[32..36].try_into().unwrap()),
        });
    }
    Ok(ArchiveV2BlockIndex {
        archive_file_bytes,
        rows,
    })
}

fn read_archive_v2_zstd_block_index(path: &Path) -> Result<ArchiveV2ZstdBlockIndex> {
    let hot_index = read_archive_v2_hot_block_index(path)?;
    let rows = hot_index
        .rows
        .into_iter()
        .map(|row| ArchiveV2ZstdBlockIndexRow {
            block_id: row.block_id,
            slot: row.slot,
            compressed_offset: row.compressed_offset,
            compressed_len: row.compressed_len,
            uncompressed_len: row.uncompressed_len,
            tx_count: row.tx_count,
        })
        .collect();
    Ok(ArchiveV2ZstdBlockIndex {
        blob_file_bytes: hot_index.blob_file_bytes,
        level: hot_index.level,
        flags: hot_index.flags,
        rows,
    })
}

fn load_optional_dict(path: Option<&Path>) -> Result<Option<Vec<u8>>> {
    path.map(|path| {
        std::fs::read(path).with_context(|| format!("read zstd dict {}", path.display()))
    })
    .transpose()
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    let mut read = 0usize;
    while read < buf.len() {
        match file.read_at(&mut buf[read..], offset + read as u64) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "short read from archive v2 file",
                ));
            }
            Ok(n) => read += n,
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveV2LogReparseMode {
    Targeted,
    Full,
}

pub(crate) fn reparse_logs(
    input: &Path,
    output: &Path,
    registry: Option<&Path>,
    mode: ArchiveV2LogReparseMode,
) -> Result<()> {
    anyhow::ensure!(
        input != output,
        "input and output must be different paths: {}",
        input.display()
    );

    let registry_path = registry.map(Path::to_path_buf).unwrap_or_else(|| {
        input
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(REGISTRY_FILE)
    });
    let store = KeyStore::load(&registry_path)?;
    let key_index = KeyIndex::build(store.keys.clone());

    let input_file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, input_file));

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let output_file =
        File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, output_file));

    let started = Instant::now();
    let mut progress = ProgressTracker::new("Archive V2 Log Reparse");
    let mut records = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut metadata_with_logs = 0u64;
    let mut log_streams_reparsed = 0u64;
    let mut log_streams_skipped = 0u64;
    let mut before_log_bytes = 0u64;
    let mut after_log_bytes = 0u64;
    let mut output_block_offset = 0u64;
    let mut pending_index_update: Option<(u64, u32, u32)> = None;
    let mut index_records_rewritten = 0u64;
    let mut block_scratch = Vec::with_capacity(8 << 20);

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        records += 1;
        match record {
            WincodeArchiveV2Record::Block(mut block) => {
                anyhow::ensure!(
                    pending_index_update.is_none(),
                    "archive v2 log reparse saw a block before the previous block index"
                );
                blocks += 1;
                progress.update_slot(block.header.compact.slot);
                txs += block.txs.len() as u64;
                for tx in &mut block.txs {
                    let Some(WincodeArchiveV2Payload::Decoded { value: meta, .. }) =
                        tx.metadata.as_mut()
                    else {
                        continue;
                    };
                    let Some(logs) = meta.logs.as_mut() else {
                        continue;
                    };

                    metadata_with_logs += 1;
                    if mode == ArchiveV2LogReparseMode::Targeted
                        && !log_stream_needs_targeted_reparse(logs)
                    {
                        log_streams_skipped += 1;
                        continue;
                    }
                    before_log_bytes +=
                        wincode::config::serialized_size(&*logs, wincode_leb128_config())?;
                    let rendered = blockzilla_format::render_logs(logs, &store);
                    let reparsed = blockzilla_format::parse_logs(&rendered, &key_index);
                    after_log_bytes +=
                        wincode::config::serialized_size(&reparsed, wincode_leb128_config())?;
                    *logs = reparsed;
                    log_streams_reparsed += 1;
                }
                let slot = block.header.compact.slot;
                let tx_count =
                    u32::try_from(block.txs.len()).context("block tx count exceeds u32::MAX")?;
                progress.update(1, block.txs.len() as u64);

                let record = WincodeArchiveV2Record::Block(block);
                encode_with_scratch(&record, &mut block_scratch)?;
                let block_len = u32::try_from(block_scratch.len())
                    .context("archive v2 frame exceeds u32::MAX")?;
                writer.write_bytes(&block_scratch)?;
                pending_index_update = Some((slot, block_len, tx_count));
            }
            WincodeArchiveV2Record::Index(mut index) => {
                if let Some((slot, block_len, tx_count)) = pending_index_update.take() {
                    index.slot = slot;
                    index.block_offset = output_block_offset;
                    index.block_len = block_len;
                    index.tx_count = tx_count;
                    output_block_offset += block_len as u64;
                    index_records_rewritten += 1;
                }
                writer.write(&WincodeArchiveV2Record::Index(index))?;
            }
            record => {
                writer.write(&record)?;
            }
        }
    }
    anyhow::ensure!(
        pending_index_update.is_none(),
        "archive v2 log reparse ended before the last block index"
    );
    writer.flush()?;
    progress.final_report();

    let elapsed = started.elapsed().as_secs_f64();
    let delta = before_log_bytes as i128 - after_log_bytes as i128;
    info!(
        "Archive V2 log reparse complete in {:.2}s: mode={:?} records={} blocks={} txs={} metadata_with_logs={} streams_reparsed={} streams_skipped={} index_records_rewritten={} before_log_bytes={} after_log_bytes={} delta_bytes={} output={}",
        elapsed,
        mode,
        records,
        blocks,
        txs,
        metadata_with_logs,
        log_streams_reparsed,
        log_streams_skipped,
        index_records_rewritten,
        before_log_bytes,
        after_log_bytes,
        delta,
        output.display()
    );
    Ok(())
}

pub(crate) fn repack_hot_logs(
    input: &Path,
    output_dir: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    level: i32,
    mode: ArchiveV2LogReparseMode,
) -> Result<()> {
    anyhow::ensure!(
        level >= 0,
        "zstd compression level must be non-negative, got {level}"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let blocks_path = output_dir.join(ARCHIVE_ZSTD_BLOCKS_FILE);
    let index_path = output_dir.join(ARCHIVE_ZSTD_INDEX_FILE);
    anyhow::ensure!(
        input != blocks_path,
        "input and output blocks must be different paths: {}",
        input.display()
    );

    let input_index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let hot_index = read_archive_v2_hot_block_index(&input_index_path)?;
    let blob_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        blob_file_bytes == hot_index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        hot_index.blob_file_bytes,
        blob_file_bytes
    );

    let dict_bytes = load_optional_dict(dict)?.unwrap_or_default();
    anyhow::ensure!(
        (hot_index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );
    let output_flags = if dict_bytes.is_empty() {
        0
    } else {
        ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY
    };

    let registry_path = registry.map(Path::to_path_buf).unwrap_or_else(|| {
        input
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(REGISTRY_FILE)
    });
    let store = KeyStore::load(&registry_path)?;
    let key_index = KeyIndex::build(store.keys.clone());

    let mut input_file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let output_file =
        File::create(&blocks_path).with_context(|| format!("create {}", blocks_path.display()))?;
    let mut blocks_writer = BufWriter::with_capacity(BUFFER_SIZE, output_file);
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
        .context("create zstd dictionary decompressor")?;
    let mut compressor = if dict_bytes.is_empty() {
        zstd::bulk::Compressor::new(level).context("create zstd compressor")?
    } else {
        zstd::bulk::Compressor::with_dictionary(level, &dict_bytes)
            .context("create zstd dictionary compressor")?
    };

    let started = Instant::now();
    let mut progress = ProgressTracker::new("Archive V2 Hot Log Repack");
    let limit = max_blocks
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(usize::MAX);
    let mut current_offset = 0u64;
    let mut output_offset = 0u64;
    let mut output_rows = Vec::with_capacity(hot_index.rows.len().min(limit));
    let mut compressed_buf = Vec::with_capacity(2 << 20);
    let mut block_scratch = Vec::with_capacity(8 << 20);

    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut metadata_raw = 0u64;
    let mut metadata_none = 0u64;
    let mut metadata_decoded = 0u64;
    let mut metadata_without_logs = 0u64;
    let mut metadata_copied_without_decode = 0u64;
    let mut metadata_with_logs = 0u64;
    let mut log_streams_reparsed = 0u64;
    let mut log_streams_skipped = 0u64;
    let mut before_log_bytes = 0u64;
    let mut after_log_bytes = 0u64;
    let mut before_metadata_bytes = 0u64;
    let mut after_metadata_bytes = 0u64;
    let mut before_uncompressed_bytes = 0u64;
    let mut after_uncompressed_bytes = 0u64;
    let mut before_compressed_bytes = 0u64;
    let mut after_compressed_bytes = 0u64;

    for input_row in hot_index.rows.iter().take(limit) {
        if current_offset != input_row.compressed_offset {
            input_file
                .seek(SeekFrom::Start(input_row.compressed_offset))
                .with_context(|| {
                    format!(
                        "seek hot archive {} to compressed offset {}",
                        input.display(),
                        input_row.compressed_offset
                    )
                })?;
            current_offset = input_row.compressed_offset;
        }
        compressed_buf.resize(input_row.compressed_len as usize, 0);
        input_file
            .read_exact(&mut compressed_buf)
            .with_context(|| {
                format!(
                    "read hot archive block_id {} at compressed offset {}",
                    input_row.block_id, input_row.compressed_offset
                )
            })?;
        current_offset = current_offset.saturating_add(input_row.compressed_len as u64);

        let block_bytes = decompressor
            .decompress(&compressed_buf, input_row.uncompressed_len as usize)
            .with_context(|| format!("zstd decompress hot block_id {}", input_row.block_id))?;
        anyhow::ensure!(
            block_bytes.len() == input_row.uncompressed_len as usize,
            "block_id {} decompressed length mismatch: decoded={} index={}",
            input_row.block_id,
            block_bytes.len(),
            input_row.uncompressed_len
        );
        let mut block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&block_bytes, wincode_leb128_config())
                .with_context(|| format!("decode hot block_id {}", input_row.block_id))?;
        anyhow::ensure!(
            block.header.slot == input_row.slot,
            "hot block_id {} slot mismatch: decoded={} index={}",
            input_row.block_id,
            block.header.slot,
            input_row.slot
        );
        anyhow::ensure!(
            block.tx_rows.len() == input_row.tx_count as usize,
            "hot block_id {} tx count mismatch: decoded={} index={}",
            input_row.block_id,
            block.tx_rows.len(),
            input_row.tx_count
        );

        before_uncompressed_bytes += input_row.uncompressed_len as u64;
        before_compressed_bytes += input_row.compressed_len as u64;

        let old_tx_rows = std::mem::take(&mut block.tx_rows);
        let old_metadata_bytes = std::mem::take(&mut block.metadata_bytes);
        let mut new_tx_rows = Vec::with_capacity(old_tx_rows.len());
        let mut new_metadata_bytes = Vec::with_capacity(old_metadata_bytes.len());

        for tx_row in old_tx_rows {
            let mut new_tx_row = tx_row;
            new_tx_row.metadata_offset = u32::try_from(new_metadata_bytes.len())
                .context("hot metadata offset exceeds u32::MAX")?;

            if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
                metadata_none += 1;
                new_tx_row.metadata_len = 0;
                new_tx_rows.push(new_tx_row);
                continue;
            }

            let old_metadata_slice = hot_block_region_slice(
                &old_metadata_bytes,
                tx_row.metadata_offset,
                tx_row.metadata_len,
                "metadata",
                input_row.block_id,
                tx_row.tx_index,
            )?;
            before_metadata_bytes += old_metadata_slice.len() as u64;
            let start = new_metadata_bytes.len();

            if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                metadata_raw += 1;
                new_metadata_bytes.extend_from_slice(old_metadata_slice);
            } else if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOGS == 0 {
                metadata_copied_without_decode += 1;
                new_metadata_bytes.extend_from_slice(old_metadata_slice);
            } else {
                let mut meta: CompactMetaV1 =
                    wincode::config::deserialize(old_metadata_slice, wincode_leb128_config())
                        .with_context(|| {
                            format!(
                                "decode hot metadata block_id={} slot={} tx_index={}",
                                input_row.block_id, input_row.slot, tx_row.tx_index
                            )
                        })?;
                metadata_decoded += 1;
                if let Some(logs) = meta.logs.as_mut() {
                    metadata_with_logs += 1;
                    if mode == ArchiveV2LogReparseMode::Targeted
                        && !log_stream_needs_targeted_reparse(logs)
                    {
                        log_streams_skipped += 1;
                        new_metadata_bytes.extend_from_slice(old_metadata_slice);
                    } else {
                        before_log_bytes +=
                            wincode::config::serialized_size(&*logs, wincode_leb128_config())?;
                        let rendered = blockzilla_format::render_logs(logs, &store);
                        let reparsed = blockzilla_format::parse_logs(&rendered, &key_index);
                        after_log_bytes +=
                            wincode::config::serialized_size(&reparsed, wincode_leb128_config())?;
                        *logs = reparsed;
                        wincode::config::serialize_into(
                            &mut new_metadata_bytes,
                            &meta,
                            wincode_leb128_config(),
                        )?;
                        log_streams_reparsed += 1;
                    }
                } else {
                    metadata_without_logs += 1;
                    new_metadata_bytes.extend_from_slice(old_metadata_slice);
                }
            }

            let metadata_len = new_metadata_bytes.len() - start;
            new_tx_row.metadata_len =
                u32::try_from(metadata_len).context("hot metadata length exceeds u32::MAX")?;
            after_metadata_bytes += metadata_len as u64;
            new_tx_rows.push(new_tx_row);
        }

        block.tx_rows = new_tx_rows;
        block.metadata_bytes = new_metadata_bytes;
        encode_with_scratch(&block, &mut block_scratch)?;
        let uncompressed_len =
            u32::try_from(block_scratch.len()).context("hot block payload exceeds u32::MAX")?;
        let compressed = compressor
            .compress(&block_scratch)
            .with_context(|| format!("zstd compress hot block_id {}", input_row.block_id))?;
        let compressed_len =
            u32::try_from(compressed.len()).context("compressed hot block exceeds u32::MAX")?;
        blocks_writer
            .write_all(&compressed)
            .with_context(|| format!("write {}", blocks_path.display()))?;

        output_rows.push(ArchiveV2HotBlockIndexRow {
            block_id: input_row.block_id,
            slot: input_row.slot,
            compressed_offset: output_offset,
            compressed_len,
            uncompressed_len,
            tx_count: input_row.tx_count,
            first_tx_ordinal: input_row.first_tx_ordinal,
            first_signature_ordinal: input_row.first_signature_ordinal,
            signature_count: input_row.signature_count,
        });
        output_offset += compressed_len as u64;
        after_uncompressed_bytes += uncompressed_len as u64;
        after_compressed_bytes += compressed_len as u64;
        blocks += 1;
        txs += input_row.tx_count as u64;
        progress.update_slot(input_row.slot);
        progress.update(1, input_row.tx_count as u64);
    }

    blocks_writer
        .flush()
        .with_context(|| format!("flush {}", blocks_path.display()))?;
    write_archive_v2_hot_block_index(
        &index_path,
        output_offset,
        level,
        output_flags,
        &output_rows,
    )?;
    progress.final_report();

    let elapsed = started.elapsed().as_secs_f64();
    let log_delta = before_log_bytes as i128 - after_log_bytes as i128;
    let metadata_delta = before_metadata_bytes as i128 - after_metadata_bytes as i128;
    let uncompressed_delta = before_uncompressed_bytes as i128 - after_uncompressed_bytes as i128;
    let compressed_delta = before_compressed_bytes as i128 - after_compressed_bytes as i128;
    let before_ratio = if before_uncompressed_bytes > 0 {
        before_compressed_bytes as f64 * 100.0 / before_uncompressed_bytes as f64
    } else {
        0.0
    };
    let after_ratio = if after_uncompressed_bytes > 0 {
        after_compressed_bytes as f64 * 100.0 / after_uncompressed_bytes as f64
    } else {
        0.0
    };
    info!(
        "Archive V2 hot log repack complete in {:.2}s: mode={:?} blocks={} txs={} metadata_decoded={} metadata_raw={} metadata_none={} metadata_copied_without_decode={} metadata_without_logs={} metadata_with_logs={} streams_reparsed={} streams_skipped={} before_log_bytes={} after_log_bytes={} log_delta_bytes={} before_metadata_bytes={} after_metadata_bytes={} metadata_delta_bytes={} before_uncompressed_bytes={} after_uncompressed_bytes={} uncompressed_delta_bytes={} before_compressed_bytes={} after_compressed_bytes={} compressed_delta_bytes={} before_ratio_pct={:.2} after_ratio_pct={:.2} level={} dict={} blocks_file={} index={}",
        elapsed,
        mode,
        blocks,
        txs,
        metadata_decoded,
        metadata_raw,
        metadata_none,
        metadata_copied_without_decode,
        metadata_without_logs,
        metadata_with_logs,
        log_streams_reparsed,
        log_streams_skipped,
        before_log_bytes,
        after_log_bytes,
        log_delta,
        before_metadata_bytes,
        after_metadata_bytes,
        metadata_delta,
        before_uncompressed_bytes,
        after_uncompressed_bytes,
        uncompressed_delta,
        before_compressed_bytes,
        after_compressed_bytes,
        compressed_delta,
        before_ratio,
        after_ratio,
        level,
        dict.map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string()),
        blocks_path.display(),
        index_path.display(),
    );
    println!(
        "archive_v2_hot_log_repack mode={mode:?} blocks={blocks} txs={txs} elapsed_s={elapsed:.3} streams_reparsed={log_streams_reparsed} streams_skipped={log_streams_skipped} before_log_bytes={before_log_bytes} after_log_bytes={after_log_bytes} log_delta_bytes={log_delta} before_metadata_bytes={before_metadata_bytes} after_metadata_bytes={after_metadata_bytes} metadata_delta_bytes={metadata_delta} before_uncompressed_bytes={before_uncompressed_bytes} after_uncompressed_bytes={after_uncompressed_bytes} uncompressed_delta_bytes={uncompressed_delta} before_compressed_bytes={before_compressed_bytes} after_compressed_bytes={after_compressed_bytes} compressed_delta_bytes={compressed_delta} before_ratio_pct={before_ratio:.2} after_ratio_pct={after_ratio:.2}"
    );
    Ok(())
}

fn log_stream_needs_targeted_reparse(logs: &blockzilla_format::CompactLogStream) -> bool {
    logs.events.iter().any(log_event_needs_targeted_reparse)
}

fn log_event_needs_targeted_reparse(event: &blockzilla_format::LogEvent) -> bool {
    match event {
        blockzilla_format::LogEvent::ProgramLog(log)
        | blockzilla_format::LogEvent::ProgramPlainLog(log) => program_log_is_unknown(log),
        blockzilla_format::LogEvent::ProgramIdLog { log, .. } => program_log_is_unknown(log),
        blockzilla_format::LogEvent::UnknownProgram { .. }
        | blockzilla_format::LogEvent::UnknownAccount { .. }
        | blockzilla_format::LogEvent::Plain { .. }
        | blockzilla_format::LogEvent::Unparsed { .. } => true,
        _ => false,
    }
}

fn program_log_is_unknown(log: &blockzilla_format::program_logs::ProgramLog) -> bool {
    matches!(log, blockzilla_format::program_logs::ProgramLog::Unknown(_))
}

pub(crate) fn analyze_logs(
    input: &Path,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    top: usize,
    max_keys: usize,
) -> Result<()> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let store = load_archive_v2_analysis_registry(input, registry)?;
    let mut stats = ArchiveV2LogPatternStats::new(max_keys.max(1));
    let mut progress = ProgressTracker::new("Archive V2 Log Analyze");
    let started = Instant::now();

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        stats.records += 1;
        let WincodeArchiveV2Record::Block(block) = record else {
            continue;
        };

        stats.blocks += 1;
        stats.txs += block.txs.len() as u64;
        progress.update_slot(block.header.compact.slot);

        for tx in &block.txs {
            match &tx.metadata {
                Some(WincodeArchiveV2Payload::Decoded { value: meta, .. }) => {
                    stats.metadata_decoded += 1;
                    if let Some(logs) = &meta.logs {
                        stats.metadata_with_logs += 1;
                        analyze_log_stream_patterns(logs, store.as_ref(), &mut stats)?;
                    } else {
                        stats.metadata_without_logs += 1;
                    }
                }
                Some(WincodeArchiveV2Payload::Raw { .. }) => stats.metadata_raw += 1,
                None => stats.metadata_none += 1,
            }
        }

        progress.update(1, block.txs.len() as u64);
        if max_blocks.is_some_and(|limit| stats.blocks >= limit) {
            break;
        }
    }

    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "archive_v2_log_patterns records={} blocks={} txs={} archive_file_bytes={} elapsed_s={elapsed:.3} MiB_s={:.2} blocks_s={:.2} tx_s={:.2}",
        stats.records,
        stats.blocks,
        stats.txs,
        archive_file_bytes,
        if elapsed > 0.0 {
            archive_file_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
    );
    println!(
        "known_program_logs_enabled={}",
        blockzilla_format::program_logs::KNOWN_PROGRAM_LOGS_ENABLED
    );
    println!(
        "log_pattern_summary metadata_decoded={} metadata_raw={} metadata_none={} metadata_with_logs={} metadata_without_logs={} log_streams={} log_events={} unknown_program_events={} unknown_account_events={} unparsed_events={} plain_events={} program_log_unknown_events={} program_plain_log_unknown_events={} program_id_log_unknown_events={} program_log_unknown_with_active_program={} program_log_unknown_without_active_program={}",
        stats.metadata_decoded,
        stats.metadata_raw,
        stats.metadata_none,
        stats.metadata_with_logs,
        stats.metadata_without_logs,
        stats.log_streams,
        stats.log_events,
        stats.unknown_program_events,
        stats.unknown_account_events,
        stats.unparsed_events,
        stats.plain_events,
        stats.program_log_unknown_events,
        stats.program_plain_log_unknown_events,
        stats.program_id_log_unknown_events,
        stats.program_log_unknown_with_active_program,
        stats.program_log_unknown_without_active_program,
    );
    stats.print(top.max(1));
    Ok(())
}

pub(crate) fn analyze_hot_logs(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    top: usize,
    max_keys: usize,
) -> Result<()> {
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let store = load_archive_v2_analysis_registry(input, registry)?;
    let mut stats = ArchiveV2LogPatternStats::new(max_keys.max(1));
    let started = Instant::now();

    let scan = scan_archive_v2_hot_blocks(
        input,
        index,
        dict,
        max_blocks,
        "Archive V2 Hot Log Analyze",
        |row, block| {
            stats.records += 1;
            stats.blocks += 1;
            stats.txs += block.tx_rows.len() as u64;
            for tx_row in &block.tx_rows {
                if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
                    stats.metadata_none += 1;
                    continue;
                }
                if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                    stats.metadata_raw += 1;
                    continue;
                }

                let metadata_slice = hot_block_metadata_slice(block, tx_row, row.block_id)?;
                let meta: CompactMetaV1 =
                    wincode::config::deserialize(metadata_slice, wincode_leb128_config())
                        .with_context(|| {
                            format!(
                                "decode hot metadata block_id={} slot={} tx_index={}",
                                row.block_id, row.slot, tx_row.tx_index
                            )
                        })?;
                stats.metadata_decoded += 1;
                if let Some(logs) = &meta.logs {
                    stats.metadata_with_logs += 1;
                    analyze_log_stream_patterns(logs, store.as_ref(), &mut stats)?;
                } else {
                    stats.metadata_without_logs += 1;
                }
            }
            Ok(())
        },
    )?;

    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "archive_v2_hot_log_patterns records={} blocks={} txs={} archive_file_bytes={} compressed_bytes={} uncompressed_bytes={} elapsed_s={elapsed:.3} compressed_MiB_s={:.2} uncompressed_MiB_s={:.2} blocks_s={:.2} tx_s={:.2}",
        stats.records,
        stats.blocks,
        stats.txs,
        archive_file_bytes,
        scan.compressed_bytes,
        scan.uncompressed_bytes,
        if elapsed > 0.0 {
            scan.compressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            scan.uncompressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
    );
    println!(
        "known_program_logs_enabled={}",
        blockzilla_format::program_logs::KNOWN_PROGRAM_LOGS_ENABLED
    );
    println!(
        "log_pattern_summary metadata_decoded={} metadata_raw={} metadata_none={} metadata_with_logs={} metadata_without_logs={} log_streams={} log_events={} unknown_program_events={} unknown_account_events={} unparsed_events={} plain_events={} program_log_unknown_events={} program_plain_log_unknown_events={} program_id_log_unknown_events={} program_log_unknown_with_active_program={} program_log_unknown_without_active_program={}",
        stats.metadata_decoded,
        stats.metadata_raw,
        stats.metadata_none,
        stats.metadata_with_logs,
        stats.metadata_without_logs,
        stats.log_streams,
        stats.log_events,
        stats.unknown_program_events,
        stats.unknown_account_events,
        stats.unparsed_events,
        stats.plain_events,
        stats.program_log_unknown_events,
        stats.program_plain_log_unknown_events,
        stats.program_id_log_unknown_events,
        stats.program_log_unknown_with_active_program,
        stats.program_log_unknown_without_active_program,
    );
    stats.print(top.max(1));
    Ok(())
}

fn load_archive_v2_analysis_registry(
    input: &Path,
    registry: Option<&Path>,
) -> Result<Option<KeyStore>> {
    if let Some(path) = registry {
        info!(
            "Archive V2 log analysis loading registry {}",
            path.display()
        );
        return KeyStore::load(path).map(Some);
    }

    let default_path = input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(REGISTRY_FILE);
    if crate::file_nonempty(&default_path) {
        info!(
            "Archive V2 log analysis loading default registry {}",
            default_path.display()
        );
        KeyStore::load(&default_path).map(Some)
    } else {
        info!(
            "Archive V2 log analysis has no registry at {}; compact ids will be printed as id:<n>",
            default_path.display()
        );
        Ok(None)
    }
}

fn analyze_log_stream_patterns(
    logs: &blockzilla_format::CompactLogStream,
    store: Option<&KeyStore>,
    stats: &mut ArchiveV2LogPatternStats,
) -> Result<()> {
    stats.log_streams += 1;
    let strings = logs.strings.iter().collect::<Vec<_>>();
    let mut program_stack = Vec::<CompactPubkey>::new();

    for event in &logs.events {
        stats.log_events += 1;
        match event {
            blockzilla_format::LogEvent::UnknownProgram { program } => {
                let text = log_string(&strings, *program, "UnknownProgram.program")?;
                stats.unknown_program_events += 1;
                stats.unknown_program_exact.bump(text, text);
                stats
                    .unknown_program_pattern
                    .bump_owned(normalize_log_text_pattern(text), text);
            }
            blockzilla_format::LogEvent::UnknownAccount { account } => {
                let text = log_string(&strings, *account, "UnknownAccount.account")?;
                stats.unknown_account_events += 1;
                stats.unknown_account_exact.bump(text, text);
                stats
                    .unknown_account_pattern
                    .bump_owned(normalize_log_text_pattern(text), text);
            }
            blockzilla_format::LogEvent::Unparsed { text } => {
                let text = log_string(&strings, *text, "Unparsed.text")?;
                stats.unparsed_events += 1;
                stats.unparsed_exact.bump(text, text);
                stats
                    .unparsed_pattern
                    .bump_owned(normalize_log_text_pattern(text), text);
            }
            blockzilla_format::LogEvent::Plain { text } => {
                let text = log_string(&strings, *text, "Plain.text")?;
                stats.plain_events += 1;
                stats.plain_exact.bump(text, text);
                stats
                    .plain_pattern
                    .bump_owned(normalize_log_text_pattern(text), text);
            }
            blockzilla_format::LogEvent::ProgramLog(log) => {
                if let Some(text) = unknown_program_log_text(log, &strings, "ProgramLog")? {
                    stats.program_log_unknown_events += 1;
                    stats.program_unknown_exact.bump(text, text);
                    stats
                        .program_unknown_pattern
                        .bump_owned(normalize_log_text_pattern(text), text);
                    let program = if let Some(program) = program_stack.last().copied() {
                        stats.program_log_unknown_with_active_program += 1;
                        render_compact_pubkey_for_analysis(program, store)?
                    } else {
                        stats.program_log_unknown_without_active_program += 1;
                        "<no-active-program>".to_string()
                    };
                    stats.program_unknown_by_program.bump(&program, &program);
                }
            }
            blockzilla_format::LogEvent::ProgramPlainLog(log) => {
                if let Some(text) = unknown_program_log_text(log, &strings, "ProgramPlainLog")? {
                    stats.program_plain_log_unknown_events += 1;
                    stats.program_unknown_exact.bump(text, text);
                    stats
                        .program_unknown_pattern
                        .bump_owned(normalize_log_text_pattern(text), text);
                    let program = if let Some(program) = program_stack.last().copied() {
                        stats.program_log_unknown_with_active_program += 1;
                        render_compact_pubkey_for_analysis(program, store)?
                    } else {
                        stats.program_log_unknown_without_active_program += 1;
                        "<no-active-program>".to_string()
                    };
                    stats.program_unknown_by_program.bump(&program, &program);
                }
            }
            blockzilla_format::LogEvent::ProgramIdLog { program, log } => {
                if let Some(text) = unknown_program_log_text(log, &strings, "ProgramIdLog")? {
                    stats.program_id_log_unknown_events += 1;
                    stats.program_unknown_exact.bump(text, text);
                    stats
                        .program_unknown_pattern
                        .bump_owned(normalize_log_text_pattern(text), text);
                    let rendered = render_compact_pubkey_for_analysis(*program, store)?;
                    stats.program_unknown_by_program.bump(&rendered, &rendered);
                }
            }
            _ => {}
        }

        update_log_analysis_program_stack(event, &mut program_stack);
    }

    Ok(())
}

fn unknown_program_log_text<'a>(
    log: &blockzilla_format::program_logs::ProgramLog,
    strings: &'a [&'a str],
    context: &'static str,
) -> Result<Option<&'a str>> {
    if let blockzilla_format::program_logs::ProgramLog::Unknown(id) = log {
        return log_string(strings, *id, context).map(Some);
    }
    Ok(None)
}

fn log_string<'a>(strings: &'a [&'a str], id: u32, context: &'static str) -> Result<&'a str> {
    strings.get(id as usize).copied().with_context(|| {
        format!(
            "{context} string id {id} out of bounds len={}",
            strings.len()
        )
    })
}

fn update_log_analysis_program_stack(
    event: &blockzilla_format::LogEvent,
    stack: &mut Vec<CompactPubkey>,
) {
    match event {
        blockzilla_format::LogEvent::Invoke { program, depth } => {
            let depth = *depth as usize;
            if depth == 0 {
                stack.clear();
                stack.push(*program);
                return;
            }
            stack.truncate(depth.saturating_sub(1));
            stack.push(*program);
        }
        blockzilla_format::LogEvent::BpfInvoke { program } => {
            stack.push(*program);
        }
        blockzilla_format::LogEvent::Success { program }
        | blockzilla_format::LogEvent::Failure { program, .. }
        | blockzilla_format::LogEvent::FailureCustomProgramError { program, .. }
        | blockzilla_format::LogEvent::FailureInvalidAccountData { program }
        | blockzilla_format::LogEvent::FailureInvalidProgramArgument { program }
        | blockzilla_format::LogEvent::BpfSuccess { program }
        | blockzilla_format::LogEvent::BpfFailure { program, .. }
        | blockzilla_format::LogEvent::BpfFailureCustomProgramError { program, .. }
        | blockzilla_format::LogEvent::BpfFailureInvalidAccountData { program }
        | blockzilla_format::LogEvent::BpfFailureInvalidProgramArgument { program } => {
            pop_log_analysis_program_stack(stack, *program);
        }
        _ => {}
    }
}

fn pop_log_analysis_program_stack(stack: &mut Vec<CompactPubkey>, program: CompactPubkey) {
    if stack.last().is_some_and(|active| *active == program) {
        stack.pop();
    } else if let Some(position) = stack.iter().rposition(|active| *active == program) {
        stack.truncate(position);
    } else {
        stack.clear();
    }
}

fn render_compact_pubkey_for_analysis(
    key: CompactPubkey,
    store: Option<&KeyStore>,
) -> Result<String> {
    let bytes = match key {
        CompactPubkey::Id(id) => {
            let Some(store) = store else {
                return Ok(format!("id:{id}"));
            };
            *store.get(id).with_context(|| {
                format!("compact pubkey id {id} out of bounds len={}", store.len())
            })?
        }
        CompactPubkey::Raw(bytes) => bytes,
    };
    Ok(Pubkey::new_from_array(bytes).to_string())
}

pub(crate) fn analyze_instruction_data(
    input: &Path,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    top: usize,
    max_keys: usize,
    prefix_len: usize,
) -> Result<()> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let store = load_archive_v2_analysis_registry(input, registry)?;
    let mut stats = ArchiveV2InstructionDataStats::new(max_keys.max(1), prefix_len.clamp(1, 32));
    let mut progress = ProgressTracker::new("Archive V2 Instruction Data Analyze");
    let started = Instant::now();

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        stats.records += 1;
        let WincodeArchiveV2Record::Block(block) = record else {
            continue;
        };

        stats.blocks += 1;
        stats.txs += block.txs.len() as u64;
        progress.update_slot(block.header.compact.slot);

        for tx_record in &block.txs {
            let decoded_meta = match &tx_record.metadata {
                Some(WincodeArchiveV2Payload::Decoded { value, .. }) => {
                    stats.metadata_decoded += 1;
                    Some(value)
                }
                Some(WincodeArchiveV2Payload::Raw { .. }) => {
                    stats.metadata_raw += 1;
                    None
                }
                None => {
                    stats.metadata_none += 1;
                    None
                }
            };

            let decoded_tx = match &tx_record.tx {
                WincodeArchiveV2Payload::Decoded { value, .. } => {
                    stats.tx_payload_decoded += 1;
                    Some(value)
                }
                WincodeArchiveV2Payload::Raw { .. } => {
                    stats.tx_payload_raw += 1;
                    None
                }
            };

            let full_keys = decoded_tx.map(|tx| collect_instruction_message_keys(tx, decoded_meta));
            if let (Some(tx), Some(full_keys)) = (decoded_tx, full_keys.as_deref()) {
                analyze_top_level_instruction_data(tx, decoded_meta, full_keys, &mut stats)
                    .with_context(|| {
                        format!(
                            "analyze tx instruction data slot={} tx_index={}",
                            block.header.compact.slot, tx_record.tx_index
                        )
                    })?;
            }

            if let Some(meta) = decoded_meta {
                analyze_inner_instruction_data(
                    meta,
                    full_keys.as_deref().unwrap_or(&[]),
                    decoded_tx.is_none(),
                    &mut stats,
                )
                .with_context(|| {
                    format!(
                        "analyze inner instruction data slot={} tx_index={}",
                        block.header.compact.slot, tx_record.tx_index
                    )
                })?;
            }
        }

        progress.update(1, block.txs.len() as u64);
        if max_blocks.is_some_and(|limit| stats.blocks >= limit) {
            break;
        }
    }

    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "archive_v2_instruction_data_patterns records={} blocks={} txs={} archive_file_bytes={} elapsed_s={elapsed:.3} MiB_s={:.2} blocks_s={:.2} tx_s={:.2}",
        stats.records,
        stats.blocks,
        stats.txs,
        archive_file_bytes,
        if elapsed > 0.0 {
            archive_file_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
    );
    println!(
        "instruction_data_payload_summary tx_payload_decoded={} tx_payload_raw={} metadata_decoded={} metadata_raw={} metadata_none={} metadata_with_inner={} inner_groups={} prefix_len={} max_keys={}",
        stats.tx_payload_decoded,
        stats.tx_payload_raw,
        stats.metadata_decoded,
        stats.metadata_raw,
        stats.metadata_none,
        stats.metadata_with_inner,
        stats.inner_groups,
        stats.prefix_len,
        stats.max_keys,
    );
    stats
        .tx
        .print("tx_instruction_data", top.max(1), store.as_ref())?;
    stats
        .inner
        .print("inner_instruction_data", top.max(1), store.as_ref())?;
    Ok(())
}

pub(crate) fn analyze_hot_instruction_data(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    top: usize,
    max_keys: usize,
    prefix_len: usize,
) -> Result<()> {
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let store = load_archive_v2_analysis_registry(input, registry)?;
    let mut stats = ArchiveV2InstructionDataStats::new(max_keys.max(1), prefix_len.clamp(1, 32));
    let started = Instant::now();

    let scan = scan_archive_v2_hot_blocks(
        input,
        index,
        dict,
        max_blocks,
        "Archive V2 Hot Instruction Data Analyze",
        |row, block| {
            stats.records += 1;
            stats.blocks += 1;
            stats.txs += block.tx_rows.len() as u64;

            for tx_row in &block.tx_rows {
                let decoded_meta = if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
                    stats.metadata_none += 1;
                    None
                } else if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                    stats.metadata_raw += 1;
                    None
                } else {
                    let metadata_slice = hot_block_metadata_slice(block, tx_row, row.block_id)?;
                    let meta: CompactMetaV1 =
                        wincode::config::deserialize(metadata_slice, wincode_leb128_config())
                            .with_context(|| {
                                format!(
                                    "decode hot metadata block_id={} slot={} tx_index={}",
                                    row.block_id, row.slot, tx_row.tx_index
                                )
                            })?;
                    stats.metadata_decoded += 1;
                    Some(meta)
                };

                let message_slice = hot_block_message_slice(block, tx_row, row.block_id)?;
                let message: ArchiveV2HotMessagePayload =
                    wincode::config::deserialize(message_slice, wincode_leb128_config())
                        .with_context(|| {
                            format!(
                                "decode hot message block_id={} slot={} tx_index={}",
                                row.block_id, row.slot, tx_row.tx_index
                            )
                        })?;
                stats.tx_payload_decoded += 1;

                let full_keys = collect_hot_message_keys(&message, decoded_meta.as_ref());
                analyze_hot_top_level_instruction_data(
                    &message,
                    decoded_meta.as_ref(),
                    &full_keys,
                    &mut stats,
                )
                .with_context(|| {
                    format!(
                        "analyze hot tx instruction data slot={} tx_index={}",
                        row.slot, tx_row.tx_index
                    )
                })?;

                if let Some(meta) = decoded_meta.as_ref() {
                    analyze_inner_instruction_data(meta, &full_keys, false, &mut stats)
                        .with_context(|| {
                            format!(
                                "analyze hot inner instruction data slot={} tx_index={}",
                                row.slot, tx_row.tx_index
                            )
                        })?;
                }
            }
            Ok(())
        },
    )?;

    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "archive_v2_hot_instruction_data_patterns records={} blocks={} txs={} archive_file_bytes={} compressed_bytes={} uncompressed_bytes={} elapsed_s={elapsed:.3} compressed_MiB_s={:.2} uncompressed_MiB_s={:.2} blocks_s={:.2} tx_s={:.2}",
        stats.records,
        stats.blocks,
        stats.txs,
        archive_file_bytes,
        scan.compressed_bytes,
        scan.uncompressed_bytes,
        if elapsed > 0.0 {
            scan.compressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            scan.uncompressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
    );
    println!(
        "instruction_data_payload_summary tx_payload_decoded={} tx_payload_raw={} metadata_decoded={} metadata_raw={} metadata_none={} metadata_with_inner={} inner_groups={} hot_compact_vote_update_vote_state={} hot_compact_vote_update_vote_state_switch={} hot_compact_vote_tower_sync={} hot_compact_vote_tower_sync_switch={} hot_compact_compute_budget={} hot_compact_system={} hot_unknown_system={} hot_unknown_vote={} prefix_len={} max_keys={}",
        stats.tx_payload_decoded,
        stats.tx_payload_raw,
        stats.metadata_decoded,
        stats.metadata_raw,
        stats.metadata_none,
        stats.metadata_with_inner,
        stats.inner_groups,
        stats.hot_compact_vote_update_vote_state,
        stats.hot_compact_vote_update_vote_state_switch,
        stats.hot_compact_vote_tower_sync,
        stats.hot_compact_vote_tower_sync_switch,
        stats.hot_compact_compute_budget_total(),
        stats.hot_compact_system_total(),
        stats.hot_unknown_system,
        stats.hot_unknown_vote,
        stats.prefix_len,
        stats.max_keys,
    );
    stats.print_hot_compact_instruction_summary();
    stats
        .tx
        .print("tx_instruction_data", top.max(1), store.as_ref())?;
    stats
        .inner
        .print("inner_instruction_data", top.max(1), store.as_ref())?;
    Ok(())
}

fn analyze_top_level_instruction_data(
    tx: &OwnedCompactTransaction,
    meta: Option<&CompactMetaV1>,
    full_keys: &[CompactPubkey],
    stats: &mut ArchiveV2InstructionDataStats,
) -> Result<()> {
    let allow_unresolved_program = meta.is_none();
    for instruction in owned_message_instructions_for_analysis(&tx.message) {
        let program = resolve_instruction_program_key(
            instruction.program_id_index as u32,
            full_keys,
            allow_unresolved_program,
        )?;
        stats.tx.bump(&instruction.data, program);
    }
    Ok(())
}

fn analyze_hot_top_level_instruction_data(
    message: &ArchiveV2HotMessagePayload,
    meta: Option<&CompactMetaV1>,
    full_keys: &[CompactPubkey],
    stats: &mut ArchiveV2InstructionDataStats,
) -> Result<()> {
    let allow_unresolved_program = meta.is_none();
    for instruction in hot_message_instructions_for_analysis(message) {
        match &instruction.data {
            ArchiveV2HotInstructionData::Raw(data) => {
                let program = resolve_instruction_program_key(
                    instruction.program_id_index as u32,
                    full_keys,
                    allow_unresolved_program,
                )?;
                stats.tx.bump(data, program);
            }
            ArchiveV2HotInstructionData::UnknownSystem(data) => {
                stats.hot_unknown_system += 1;
                stats.tx.bump(
                    data,
                    InstructionProgramKey::Pubkey(CompactPubkey::Raw(system_program_id_bytes())),
                );
            }
            ArchiveV2HotInstructionData::UnknownVote(data) => {
                stats.hot_unknown_vote += 1;
                stats.tx.bump(
                    data,
                    InstructionProgramKey::Pubkey(CompactPubkey::Raw(vote_program_id_bytes())),
                );
            }
            ArchiveV2HotInstructionData::ComputeBudget(data) => {
                stats.bump_hot_compact_compute_budget(data);
            }
            ArchiveV2HotInstructionData::System(data) => {
                stats.bump_hot_compact_system(data);
            }
            data => stats.bump_hot_compact_vote(data),
        }
    }
    Ok(())
}

fn analyze_inner_instruction_data(
    meta: &CompactMetaV1,
    full_keys: &[CompactPubkey],
    allow_unresolved_program: bool,
    stats: &mut ArchiveV2InstructionDataStats,
) -> Result<()> {
    let Some(groups) = &meta.inner_instructions else {
        return Ok(());
    };
    stats.metadata_with_inner += 1;
    stats.inner_groups += groups.len() as u64;
    for group in groups {
        for instruction in &group.instructions {
            let program = resolve_instruction_program_key(
                instruction.program_id_index,
                full_keys,
                allow_unresolved_program,
            )
            .with_context(|| {
                format!(
                    "inner instruction group index={} program_id_index={}",
                    group.index, instruction.program_id_index
                )
            })?;
            stats.inner.bump(&instruction.data, program);
        }
    }
    Ok(())
}

fn collect_instruction_message_keys(
    tx: &OwnedCompactTransaction,
    meta: Option<&CompactMetaV1>,
) -> Vec<CompactPubkey> {
    let mut keys = owned_message_account_keys_for_analysis(&tx.message).to_vec();
    if let Some(meta) = meta {
        keys.extend_from_slice(&meta.loaded_writable_addresses);
        keys.extend_from_slice(&meta.loaded_readonly_addresses);
    }
    keys
}

fn collect_hot_message_keys(
    message: &ArchiveV2HotMessagePayload,
    meta: Option<&CompactMetaV1>,
) -> Vec<CompactPubkey> {
    let mut keys = hot_message_account_keys_for_analysis(message).to_vec();
    if let Some(meta) = meta {
        keys.extend_from_slice(&meta.loaded_writable_addresses);
        keys.extend_from_slice(&meta.loaded_readonly_addresses);
    }
    keys
}

fn owned_message_account_keys_for_analysis(message: &OwnedCompactMessage) -> &[CompactPubkey] {
    match message {
        OwnedCompactMessage::Legacy(message) => &message.account_keys,
        OwnedCompactMessage::V0(message) => &message.account_keys,
    }
}

fn hot_message_account_keys_for_analysis(message: &ArchiveV2HotMessagePayload) -> &[CompactPubkey] {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => &message.account_keys,
        ArchiveV2HotMessagePayload::V0(message) => &message.account_keys,
    }
}

fn owned_message_instructions_for_analysis(
    message: &OwnedCompactMessage,
) -> &[OwnedCompactInstruction] {
    match message {
        OwnedCompactMessage::Legacy(message) => &message.instructions,
        OwnedCompactMessage::V0(message) => &message.instructions,
    }
}

fn hot_message_instructions_for_analysis(
    message: &ArchiveV2HotMessagePayload,
) -> &[ArchiveV2HotInstruction] {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => &message.instructions,
        ArchiveV2HotMessagePayload::V0(message) => &message.instructions,
    }
}

fn resolve_instruction_program_key(
    program_id_index: u32,
    full_keys: &[CompactPubkey],
    allow_unresolved_program: bool,
) -> Result<InstructionProgramKey> {
    if let Some(key) = full_keys.get(program_id_index as usize).copied() {
        return Ok(InstructionProgramKey::Pubkey(key));
    }
    if allow_unresolved_program {
        return Ok(InstructionProgramKey::Unresolved(program_id_index));
    }
    anyhow::bail!(
        "program id index {} out of bounds for {} message keys",
        program_id_index,
        full_keys.len()
    );
}

struct ArchiveV2InstructionDataStats {
    records: u64,
    blocks: u64,
    txs: u64,
    tx_payload_decoded: u64,
    tx_payload_raw: u64,
    metadata_decoded: u64,
    metadata_raw: u64,
    metadata_none: u64,
    metadata_with_inner: u64,
    inner_groups: u64,
    hot_compact_vote_update_vote_state: u64,
    hot_compact_vote_update_vote_state_switch: u64,
    hot_compact_vote_tower_sync: u64,
    hot_compact_vote_tower_sync_switch: u64,
    hot_unknown_system: u64,
    hot_unknown_vote: u64,
    hot_compute_budget_unused: u64,
    hot_compute_budget_request_heap_frame: u64,
    hot_compute_budget_set_compute_unit_limit: u64,
    hot_compute_budget_set_compute_unit_price: u64,
    hot_compute_budget_set_loaded_accounts_data_size_limit: u64,
    hot_system_create_account: u64,
    hot_system_assign: u64,
    hot_system_transfer: u64,
    hot_system_create_account_with_seed: u64,
    hot_system_advance_nonce_account: u64,
    hot_system_withdraw_nonce_account: u64,
    hot_system_initialize_nonce_account: u64,
    hot_system_authorize_nonce_account: u64,
    hot_system_allocate: u64,
    hot_system_allocate_with_seed: u64,
    hot_system_assign_with_seed: u64,
    hot_system_transfer_with_seed: u64,
    hot_system_upgrade_nonce_account: u64,
    hot_system_create_account_allow_prefund: u64,
    tx: InstructionDataPatternStats,
    inner: InstructionDataPatternStats,
    prefix_len: usize,
    max_keys: usize,
}

impl ArchiveV2InstructionDataStats {
    fn new(max_keys: usize, prefix_len: usize) -> Self {
        Self {
            records: 0,
            blocks: 0,
            txs: 0,
            tx_payload_decoded: 0,
            tx_payload_raw: 0,
            metadata_decoded: 0,
            metadata_raw: 0,
            metadata_none: 0,
            metadata_with_inner: 0,
            inner_groups: 0,
            hot_compact_vote_update_vote_state: 0,
            hot_compact_vote_update_vote_state_switch: 0,
            hot_compact_vote_tower_sync: 0,
            hot_compact_vote_tower_sync_switch: 0,
            hot_unknown_system: 0,
            hot_unknown_vote: 0,
            hot_compute_budget_unused: 0,
            hot_compute_budget_request_heap_frame: 0,
            hot_compute_budget_set_compute_unit_limit: 0,
            hot_compute_budget_set_compute_unit_price: 0,
            hot_compute_budget_set_loaded_accounts_data_size_limit: 0,
            hot_system_create_account: 0,
            hot_system_assign: 0,
            hot_system_transfer: 0,
            hot_system_create_account_with_seed: 0,
            hot_system_advance_nonce_account: 0,
            hot_system_withdraw_nonce_account: 0,
            hot_system_initialize_nonce_account: 0,
            hot_system_authorize_nonce_account: 0,
            hot_system_allocate: 0,
            hot_system_allocate_with_seed: 0,
            hot_system_assign_with_seed: 0,
            hot_system_transfer_with_seed: 0,
            hot_system_upgrade_nonce_account: 0,
            hot_system_create_account_allow_prefund: 0,
            tx: InstructionDataPatternStats::new(max_keys, prefix_len),
            inner: InstructionDataPatternStats::new(max_keys, prefix_len),
            prefix_len,
            max_keys,
        }
    }

    fn bump_hot_compact_vote(&mut self, data: &ArchiveV2HotInstructionData) {
        match data {
            ArchiveV2HotInstructionData::Raw(_) => {}
            ArchiveV2HotInstructionData::UnknownSystem(_) => {}
            ArchiveV2HotInstructionData::UnknownVote(_) => {}
            ArchiveV2HotInstructionData::ComputeBudget(_) => {}
            ArchiveV2HotInstructionData::System(_) => {}
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(_) => {
                self.hot_compact_vote_update_vote_state += 1;
            }
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch { .. } => {
                self.hot_compact_vote_update_vote_state_switch += 1;
            }
            ArchiveV2HotInstructionData::VoteTowerSync(_) => {
                self.hot_compact_vote_tower_sync += 1;
            }
            ArchiveV2HotInstructionData::VoteTowerSyncSwitch { .. } => {
                self.hot_compact_vote_tower_sync_switch += 1;
            }
        }
    }

    fn bump_hot_compact_compute_budget(&mut self, data: &ArchiveV2ComputeBudgetInstructionData) {
        match data {
            ArchiveV2ComputeBudgetInstructionData::Unused => self.hot_compute_budget_unused += 1,
            ArchiveV2ComputeBudgetInstructionData::RequestHeapFrame(_) => {
                self.hot_compute_budget_request_heap_frame += 1;
            }
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(_) => {
                self.hot_compute_budget_set_compute_unit_limit += 1;
            }
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(_) => {
                self.hot_compute_budget_set_compute_unit_price += 1;
            }
            ArchiveV2ComputeBudgetInstructionData::SetLoadedAccountsDataSizeLimit(_) => {
                self.hot_compute_budget_set_loaded_accounts_data_size_limit += 1;
            }
        }
    }

    fn bump_hot_compact_system(&mut self, data: &ArchiveV2SystemInstructionData) {
        match data {
            ArchiveV2SystemInstructionData::CreateAccount { .. } => {
                self.hot_system_create_account += 1;
            }
            ArchiveV2SystemInstructionData::Assign { .. } => self.hot_system_assign += 1,
            ArchiveV2SystemInstructionData::Transfer { .. } => self.hot_system_transfer += 1,
            ArchiveV2SystemInstructionData::CreateAccountWithSeed { .. } => {
                self.hot_system_create_account_with_seed += 1;
            }
            ArchiveV2SystemInstructionData::AdvanceNonceAccount => {
                self.hot_system_advance_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::WithdrawNonceAccount { .. } => {
                self.hot_system_withdraw_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::InitializeNonceAccount { .. } => {
                self.hot_system_initialize_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::AuthorizeNonceAccount { .. } => {
                self.hot_system_authorize_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::Allocate { .. } => self.hot_system_allocate += 1,
            ArchiveV2SystemInstructionData::AllocateWithSeed { .. } => {
                self.hot_system_allocate_with_seed += 1;
            }
            ArchiveV2SystemInstructionData::AssignWithSeed { .. } => {
                self.hot_system_assign_with_seed += 1;
            }
            ArchiveV2SystemInstructionData::TransferWithSeed { .. } => {
                self.hot_system_transfer_with_seed += 1;
            }
            ArchiveV2SystemInstructionData::UpgradeNonceAccount => {
                self.hot_system_upgrade_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::CreateAccountAllowPrefund { .. } => {
                self.hot_system_create_account_allow_prefund += 1;
            }
        }
    }

    fn hot_compact_compute_budget_total(&self) -> u64 {
        self.hot_compute_budget_unused
            + self.hot_compute_budget_request_heap_frame
            + self.hot_compute_budget_set_compute_unit_limit
            + self.hot_compute_budget_set_compute_unit_price
            + self.hot_compute_budget_set_loaded_accounts_data_size_limit
    }

    fn hot_compact_system_total(&self) -> u64 {
        self.hot_system_create_account
            + self.hot_system_assign
            + self.hot_system_transfer
            + self.hot_system_create_account_with_seed
            + self.hot_system_advance_nonce_account
            + self.hot_system_withdraw_nonce_account
            + self.hot_system_initialize_nonce_account
            + self.hot_system_authorize_nonce_account
            + self.hot_system_allocate
            + self.hot_system_allocate_with_seed
            + self.hot_system_assign_with_seed
            + self.hot_system_transfer_with_seed
            + self.hot_system_upgrade_nonce_account
            + self.hot_system_create_account_allow_prefund
    }

    fn print_hot_compact_instruction_summary(&self) {
        println!(
            "hot_compute_budget_variants unused={} request_heap_frame={} set_compute_unit_limit={} set_compute_unit_price={} set_loaded_accounts_data_size_limit={}",
            self.hot_compute_budget_unused,
            self.hot_compute_budget_request_heap_frame,
            self.hot_compute_budget_set_compute_unit_limit,
            self.hot_compute_budget_set_compute_unit_price,
            self.hot_compute_budget_set_loaded_accounts_data_size_limit,
        );
        println!(
            "hot_system_variants create_account={} assign={} transfer={} create_account_with_seed={} advance_nonce_account={} withdraw_nonce_account={} initialize_nonce_account={} authorize_nonce_account={} allocate={} allocate_with_seed={} assign_with_seed={} transfer_with_seed={} upgrade_nonce_account={} create_account_allow_prefund={}",
            self.hot_system_create_account,
            self.hot_system_assign,
            self.hot_system_transfer,
            self.hot_system_create_account_with_seed,
            self.hot_system_advance_nonce_account,
            self.hot_system_withdraw_nonce_account,
            self.hot_system_initialize_nonce_account,
            self.hot_system_authorize_nonce_account,
            self.hot_system_allocate,
            self.hot_system_allocate_with_seed,
            self.hot_system_assign_with_seed,
            self.hot_system_transfer_with_seed,
            self.hot_system_upgrade_nonce_account,
            self.hot_system_create_account_allow_prefund,
        );
    }
}

struct InstructionDataPatternStats {
    instructions: u64,
    data_bytes: u128,
    zero_len: u64,
    prefix_len: usize,
    exact: CappedInstructionDataCounts,
    prefix: CappedInstructionPrefixCounts,
    by_program: CappedInstructionProgramCounts,
    by_length: InstructionLengthCounts,
}

impl InstructionDataPatternStats {
    fn new(max_keys: usize, prefix_len: usize) -> Self {
        Self {
            instructions: 0,
            data_bytes: 0,
            zero_len: 0,
            prefix_len,
            exact: CappedInstructionDataCounts::new(max_keys),
            prefix: CappedInstructionPrefixCounts::new(max_keys),
            by_program: CappedInstructionProgramCounts::new(max_keys),
            by_length: InstructionLengthCounts::default(),
        }
    }

    fn bump(&mut self, data: &[u8], program: InstructionProgramKey) {
        self.instructions += 1;
        self.data_bytes += data.len() as u128;
        if data.is_empty() {
            self.zero_len += 1;
        }
        self.exact.bump(data, program);
        self.prefix.bump(data, self.prefix_len);
        self.by_program.bump(program, data.len() as u64);
        self.by_length.bump(data.len() as u32);
    }

    fn print(&self, name: &'static str, top: usize, store: Option<&KeyStore>) -> Result<()> {
        println!(
            "instruction_data_summary kind={name} instructions={} data_bytes={} zero_len={} nonzero_len={} avg_data_len={:.2}",
            self.instructions,
            self.data_bytes,
            self.zero_len,
            self.instructions.saturating_sub(self.zero_len),
            if self.instructions > 0 {
                self.data_bytes as f64 / self.instructions as f64
            } else {
                0.0
            },
        );
        self.exact.print(name, top, store)?;
        self.prefix.print(name, top)?;
        self.by_program.print(name, top, store)?;
        self.by_length.print(name, top);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InstructionDataKey {
    len: u32,
    hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InstructionPrefixKey {
    len: u8,
    bytes: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum InstructionProgramKey {
    Pubkey(CompactPubkey),
    Unresolved(u32),
}

impl InstructionProgramKey {
    fn render(self, store: Option<&KeyStore>) -> Result<String> {
        match self {
            Self::Pubkey(key) => render_compact_pubkey_for_analysis(key, store),
            Self::Unresolved(index) => Ok(format!("unresolved_program_index:{index}")),
        }
    }
}

struct CountedInstructionData {
    count: u64,
    sample_prefix: [u8; 32],
    sample_prefix_len: u8,
    program_example: InstructionProgramKey,
}

struct CappedInstructionDataCounts {
    counts: GxHashMap<InstructionDataKey, CountedInstructionData>,
    observations: u64,
    observed_bytes: u128,
    skipped_new_keys: u64,
    skipped_new_key_bytes: u128,
    max_keys: usize,
}

impl CappedInstructionDataCounts {
    fn new(max_keys: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(1024, GxBuildHasher::default()),
            observations: 0,
            observed_bytes: 0,
            skipped_new_keys: 0,
            skipped_new_key_bytes: 0,
            max_keys,
        }
    }

    fn bump(&mut self, data: &[u8], program: InstructionProgramKey) {
        self.observations += 1;
        self.observed_bytes += data.len() as u128;
        let key = InstructionDataKey {
            len: data.len() as u32,
            hash: stable_hash_bytes(data),
        };
        if let Some(entry) = self.counts.get_mut(&key) {
            entry.count = entry.count.saturating_add(1);
            return;
        }
        if self.counts.len() >= self.max_keys {
            self.skipped_new_keys += 1;
            self.skipped_new_key_bytes += data.len() as u128;
            return;
        }
        let (sample_prefix, sample_prefix_len) = instruction_data_prefix(data, 32);
        self.counts.insert(
            key,
            CountedInstructionData {
                count: 1,
                sample_prefix,
                sample_prefix_len,
                program_example: program,
            },
        );
    }

    fn print(&self, parent: &'static str, top: usize, store: Option<&KeyStore>) -> Result<()> {
        let tracked_bytes = self.tracked_bytes();
        let duplicate_bytes = self.tracked_duplicate_bytes();
        let repeated_keys = self.counts.values().filter(|entry| entry.count > 1).count();
        println!(
            "instruction_data_exact_bucket kind={parent}_exact observations={} observed_bytes={} distinct_tracked={} repeated_keys={} skipped_new_keys={} skipped_new_key_bytes={} tracked_bytes={} tracked_duplicate_bytes={} duplicate_pct_observed={:.4} duplicate_pct_tracked={:.4} max_keys={}",
            self.observations,
            self.observed_bytes,
            self.counts.len(),
            repeated_keys,
            self.skipped_new_keys,
            self.skipped_new_key_bytes,
            tracked_bytes,
            duplicate_bytes,
            pct_u128(duplicate_bytes, self.observed_bytes),
            pct_u128(duplicate_bytes, tracked_bytes),
            self.max_keys,
        );

        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            let a_duplicate = exact_duplicate_bytes(*a.0, a.1.count);
            let b_duplicate = exact_duplicate_bytes(*b.0, b.1.count);
            b_duplicate
                .cmp(&a_duplicate)
                .then_with(|| {
                    exact_total_bytes(*b.0, b.1.count).cmp(&exact_total_bytes(*a.0, a.1.count))
                })
                .then_with(|| b.1.count.cmp(&a.1.count))
                .then_with(|| a.0.hash.cmp(&b.0.hash))
        });
        for (rank, (key, entry)) in rows.into_iter().take(top).enumerate() {
            let total_bytes = exact_total_bytes(*key, entry.count);
            let duplicate_bytes = exact_duplicate_bytes(*key, entry.count);
            println!(
                "instruction_data_exact_top kind={parent}_exact rank={} count={} len={} total_bytes={} duplicate_bytes={} hash={:016x} pct_observations={:.4} pct_bytes={:.4} sample_hex={} program_example={:?}",
                rank + 1,
                entry.count,
                key.len,
                total_bytes,
                duplicate_bytes,
                key.hash,
                pct_u64(entry.count, self.observations),
                pct_u128(total_bytes, self.observed_bytes),
                render_instruction_prefix(entry.sample_prefix, entry.sample_prefix_len),
                entry.program_example.render(store)?,
            );
        }
        Ok(())
    }

    fn tracked_bytes(&self) -> u128 {
        self.counts
            .iter()
            .map(|(key, entry)| exact_total_bytes(*key, entry.count))
            .sum()
    }

    fn tracked_duplicate_bytes(&self) -> u128 {
        self.counts
            .iter()
            .map(|(key, entry)| exact_duplicate_bytes(*key, entry.count))
            .sum()
    }
}

struct CappedInstructionPrefixCounts {
    counts: GxHashMap<InstructionPrefixKey, CountedBytes>,
    observations: u64,
    observed_bytes: u128,
    skipped_new_keys: u64,
    skipped_new_key_bytes: u128,
    max_keys: usize,
}

impl CappedInstructionPrefixCounts {
    fn new(max_keys: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(1024, GxBuildHasher::default()),
            observations: 0,
            observed_bytes: 0,
            skipped_new_keys: 0,
            skipped_new_key_bytes: 0,
            max_keys,
        }
    }

    fn bump(&mut self, data: &[u8], prefix_len: usize) {
        self.observations += 1;
        self.observed_bytes += data.len() as u128;
        let key = InstructionPrefixKey::new(data, prefix_len);
        if let Some(entry) = self.counts.get_mut(&key) {
            entry.count = entry.count.saturating_add(1);
            entry.bytes += data.len() as u128;
            return;
        }
        if self.counts.len() >= self.max_keys {
            self.skipped_new_keys += 1;
            self.skipped_new_key_bytes += data.len() as u128;
            return;
        }
        self.counts.insert(
            key,
            CountedBytes {
                count: 1,
                bytes: data.len() as u128,
            },
        );
    }

    fn print(&self, parent: &'static str, top: usize) -> Result<()> {
        println!(
            "instruction_data_prefix_bucket kind={parent}_prefix observations={} observed_bytes={} distinct_tracked={} skipped_new_keys={} skipped_new_key_bytes={} max_keys={}",
            self.observations,
            self.observed_bytes,
            self.counts.len(),
            self.skipped_new_keys,
            self.skipped_new_key_bytes,
            self.max_keys,
        );
        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b.1.bytes
                .cmp(&a.1.bytes)
                .then_with(|| b.1.count.cmp(&a.1.count))
                .then_with(|| a.0.len.cmp(&b.0.len))
        });
        for (rank, (key, entry)) in rows.into_iter().take(top).enumerate() {
            println!(
                "instruction_data_prefix_top kind={parent}_prefix rank={} count={} bytes={} pct_observations={:.4} pct_bytes={:.4} prefix_hex={}",
                rank + 1,
                entry.count,
                entry.bytes,
                pct_u64(entry.count, self.observations),
                pct_u128(entry.bytes, self.observed_bytes),
                render_instruction_prefix(key.bytes, key.len),
            );
        }
        Ok(())
    }
}

impl InstructionPrefixKey {
    fn new(data: &[u8], prefix_len: usize) -> Self {
        let (bytes, len) = instruction_data_prefix(data, prefix_len);
        Self { len, bytes }
    }
}

struct CappedInstructionProgramCounts {
    counts: GxHashMap<InstructionProgramKey, CountedBytes>,
    observations: u64,
    observed_bytes: u128,
    skipped_new_keys: u64,
    skipped_new_key_bytes: u128,
    max_keys: usize,
}

impl CappedInstructionProgramCounts {
    fn new(max_keys: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(1024, GxBuildHasher::default()),
            observations: 0,
            observed_bytes: 0,
            skipped_new_keys: 0,
            skipped_new_key_bytes: 0,
            max_keys,
        }
    }

    fn bump(&mut self, program: InstructionProgramKey, data_len: u64) {
        self.observations += 1;
        self.observed_bytes += data_len as u128;
        if let Some(entry) = self.counts.get_mut(&program) {
            entry.count = entry.count.saturating_add(1);
            entry.bytes += data_len as u128;
            return;
        }
        if self.counts.len() >= self.max_keys {
            self.skipped_new_keys += 1;
            self.skipped_new_key_bytes += data_len as u128;
            return;
        }
        self.counts.insert(
            program,
            CountedBytes {
                count: 1,
                bytes: data_len as u128,
            },
        );
    }

    fn print(&self, parent: &'static str, top: usize, store: Option<&KeyStore>) -> Result<()> {
        println!(
            "instruction_data_program_bucket kind={parent}_program observations={} observed_bytes={} distinct_tracked={} skipped_new_keys={} skipped_new_key_bytes={} max_keys={}",
            self.observations,
            self.observed_bytes,
            self.counts.len(),
            self.skipped_new_keys,
            self.skipped_new_key_bytes,
            self.max_keys,
        );
        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b.1.bytes
                .cmp(&a.1.bytes)
                .then_with(|| b.1.count.cmp(&a.1.count))
        });
        for (rank, (program, entry)) in rows.into_iter().take(top).enumerate() {
            println!(
                "instruction_data_program_top kind={parent}_program rank={} count={} bytes={} pct_observations={:.4} pct_bytes={:.4} program={:?}",
                rank + 1,
                entry.count,
                entry.bytes,
                pct_u64(entry.count, self.observations),
                pct_u128(entry.bytes, self.observed_bytes),
                program.render(store)?,
            );
        }
        Ok(())
    }
}

#[derive(Default)]
struct InstructionLengthCounts {
    counts: BTreeMap<u32, CountedBytes>,
    observations: u64,
    observed_bytes: u128,
}

impl InstructionLengthCounts {
    fn bump(&mut self, len: u32) {
        self.observations += 1;
        self.observed_bytes += len as u128;
        let entry = self.counts.entry(len).or_default();
        entry.count = entry.count.saturating_add(1);
        entry.bytes += len as u128;
    }

    fn print(&self, parent: &'static str, top: usize) {
        println!(
            "instruction_data_length_bucket kind={parent}_length observations={} observed_bytes={} distinct_lengths={}",
            self.observations,
            self.observed_bytes,
            self.counts.len(),
        );
        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b.1.bytes
                .cmp(&a.1.bytes)
                .then_with(|| b.1.count.cmp(&a.1.count))
                .then_with(|| a.0.cmp(b.0))
        });
        for (rank, (len, entry)) in rows.into_iter().take(top).enumerate() {
            println!(
                "instruction_data_length_top kind={parent}_length rank={} len={} count={} bytes={} pct_observations={:.4} pct_bytes={:.4}",
                rank + 1,
                len,
                entry.count,
                entry.bytes,
                pct_u64(entry.count, self.observations),
                pct_u128(entry.bytes, self.observed_bytes),
            );
        }
    }
}

#[derive(Default)]
struct CountedBytes {
    count: u64,
    bytes: u128,
}

fn exact_total_bytes(key: InstructionDataKey, count: u64) -> u128 {
    key.len as u128 * count as u128
}

fn exact_duplicate_bytes(key: InstructionDataKey, count: u64) -> u128 {
    key.len as u128 * count.saturating_sub(1) as u128
}

fn stable_hash_bytes(data: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn instruction_data_prefix(data: &[u8], max_len: usize) -> ([u8; 32], u8) {
    let len = data.len().min(max_len).min(32);
    let mut bytes = [0u8; 32];
    bytes[..len].copy_from_slice(&data[..len]);
    (bytes, len as u8)
}

fn render_instruction_prefix(bytes: [u8; 32], len: u8) -> String {
    if len == 0 {
        return "<empty>".to_string();
    }
    let mut out = String::with_capacity(len as usize * 2);
    for byte in &bytes[..len as usize] {
        out.push(hex_nibble(byte >> 4));
        out.push(hex_nibble(byte & 0x0f));
    }
    out
}

fn hex_nibble(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => unreachable!("hex nibble out of range"),
    }
}

fn pct_u64(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

fn pct_u128(part: u128, total: u128) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

struct ArchiveV2LogPatternStats {
    records: u64,
    blocks: u64,
    txs: u64,
    metadata_decoded: u64,
    metadata_raw: u64,
    metadata_none: u64,
    metadata_with_logs: u64,
    metadata_without_logs: u64,
    log_streams: u64,
    log_events: u64,
    unknown_program_events: u64,
    unknown_account_events: u64,
    unparsed_events: u64,
    plain_events: u64,
    program_log_unknown_events: u64,
    program_plain_log_unknown_events: u64,
    program_id_log_unknown_events: u64,
    program_log_unknown_with_active_program: u64,
    program_log_unknown_without_active_program: u64,
    unknown_program_exact: CappedTextCounts,
    unknown_program_pattern: CappedTextCounts,
    unknown_account_exact: CappedTextCounts,
    unknown_account_pattern: CappedTextCounts,
    unparsed_exact: CappedTextCounts,
    unparsed_pattern: CappedTextCounts,
    plain_exact: CappedTextCounts,
    plain_pattern: CappedTextCounts,
    program_unknown_exact: CappedTextCounts,
    program_unknown_pattern: CappedTextCounts,
    program_unknown_by_program: CappedTextCounts,
}

impl ArchiveV2LogPatternStats {
    fn new(max_keys: usize) -> Self {
        Self {
            records: 0,
            blocks: 0,
            txs: 0,
            metadata_decoded: 0,
            metadata_raw: 0,
            metadata_none: 0,
            metadata_with_logs: 0,
            metadata_without_logs: 0,
            log_streams: 0,
            log_events: 0,
            unknown_program_events: 0,
            unknown_account_events: 0,
            unparsed_events: 0,
            plain_events: 0,
            program_log_unknown_events: 0,
            program_plain_log_unknown_events: 0,
            program_id_log_unknown_events: 0,
            program_log_unknown_with_active_program: 0,
            program_log_unknown_without_active_program: 0,
            unknown_program_exact: CappedTextCounts::new(max_keys),
            unknown_program_pattern: CappedTextCounts::new(max_keys),
            unknown_account_exact: CappedTextCounts::new(max_keys),
            unknown_account_pattern: CappedTextCounts::new(max_keys),
            unparsed_exact: CappedTextCounts::new(max_keys),
            unparsed_pattern: CappedTextCounts::new(max_keys),
            plain_exact: CappedTextCounts::new(max_keys),
            plain_pattern: CappedTextCounts::new(max_keys),
            program_unknown_exact: CappedTextCounts::new(max_keys),
            program_unknown_pattern: CappedTextCounts::new(max_keys),
            program_unknown_by_program: CappedTextCounts::new(max_keys),
        }
    }

    fn print(&self, top: usize) {
        self.unknown_program_exact
            .print("unknown_program_exact", top);
        self.unknown_program_pattern
            .print("unknown_program_pattern", top);
        self.unknown_account_exact
            .print("unknown_account_exact", top);
        self.unknown_account_pattern
            .print("unknown_account_pattern", top);
        self.unparsed_exact.print("unparsed_exact", top);
        self.unparsed_pattern.print("unparsed_pattern", top);
        self.plain_exact.print("plain_exact", top);
        self.plain_pattern.print("plain_pattern", top);
        self.program_unknown_by_program
            .print("program_unknown_by_program", top);
        self.program_unknown_exact
            .print("program_unknown_exact", top);
        self.program_unknown_pattern
            .print("program_unknown_pattern", top);
    }
}

struct CountedText {
    count: u64,
    example: String,
}

struct CappedTextCounts {
    counts: GxHashMap<String, CountedText>,
    observations: u64,
    skipped_new_keys: u64,
    max_keys: usize,
}

impl CappedTextCounts {
    fn new(max_keys: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(1024, GxBuildHasher::default()),
            observations: 0,
            skipped_new_keys: 0,
            max_keys,
        }
    }

    fn bump(&mut self, key: &str, example: &str) {
        self.bump_owned(key.to_string(), example);
    }

    fn bump_owned(&mut self, key: String, example: &str) {
        self.observations += 1;
        if let Some(entry) = self.counts.get_mut(&key) {
            entry.count = entry.count.saturating_add(1);
            return;
        }
        if self.counts.len() >= self.max_keys {
            self.skipped_new_keys += 1;
            return;
        }
        self.counts.insert(
            key,
            CountedText {
                count: 1,
                example: truncate_report_text(example, 360),
            },
        );
    }

    fn print(&self, name: &'static str, top: usize) {
        println!(
            "log_pattern_bucket kind={name} observations={} distinct_tracked={} skipped_new_keys={} max_keys={}",
            self.observations,
            self.counts.len(),
            self.skipped_new_keys,
            self.max_keys
        );
        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| b.1.count.cmp(&a.1.count).then_with(|| a.0.cmp(b.0)));
        for (rank, (key, entry)) in rows.into_iter().take(top).enumerate() {
            let pct = if self.observations > 0 {
                entry.count as f64 * 100.0 / self.observations as f64
            } else {
                0.0
            };
            println!(
                "log_pattern_top kind={name} rank={} count={} pct={pct:.4} key={:?} example={:?}",
                rank + 1,
                entry.count,
                truncate_report_text(key, 360),
                entry.example
            );
        }
    }
}

fn normalize_log_text_pattern(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len().min(512));
    for (idx, token) in text.split_whitespace().enumerate() {
        if idx > 0 {
            normalized.push(' ');
        }
        normalized.push_str(normalize_log_token(token));
        if normalized.len() >= 512 {
            return truncate_report_text(&normalized, 512);
        }
    }
    if normalized.is_empty() {
        "<empty>".to_string()
    } else {
        normalized
    }
}

fn normalize_log_token(token: &str) -> &str {
    let trimmed = token.trim_matches(|c: char| {
        matches!(
            c,
            ',' | ';' | ':' | '.' | '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\''
        )
    });
    if looks_like_pubkey(trimmed) {
        "<pubkey>"
    } else if looks_like_hex(trimmed) {
        "<hex>"
    } else if looks_like_decimal(trimmed) {
        "<num>"
    } else if looks_like_base64_blob(trimmed) {
        "<blob>"
    } else {
        token
    }
}

fn looks_like_pubkey(token: &str) -> bool {
    (32..=44).contains(&token.len())
        && token.bytes().all(|byte| {
            b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(&byte)
        })
}

fn looks_like_hex(token: &str) -> bool {
    let Some(rest) = token
        .strip_prefix("0x")
        .or_else(|| token.strip_prefix("0X"))
    else {
        return false;
    };
    !rest.is_empty() && rest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn looks_like_decimal(token: &str) -> bool {
    token.len() >= 3 && token.bytes().all(|byte| byte.is_ascii_digit())
}

fn looks_like_base64_blob(token: &str) -> bool {
    token.len() >= 32
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
}

fn truncate_report_text(text: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in text.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

pub(crate) fn inspect(input: &Path, max_blocks: Option<u64>, top: usize) -> Result<()> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();

    let mut records = 0u64;
    let mut genesis_records = 0u64;
    let mut genesis_accounts = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut poh_entries = 0u64;
    let mut poh_tx_count_sum = 0u64;
    let mut poh_tx_count_zero = 0u64;
    let mut poh_tx_count_one = 0u64;
    let mut poh_tx_count_multi = 0u64;
    let mut poh_tx_count_max = 0u32;
    let mut poh_num_hashes_zero = 0u64;
    let mut poh_num_hashes_one = 0u64;
    let mut poh_num_hashes_sum = 0u128;
    let mut max_poh_entries_per_block = 0usize;
    let mut poh_entries_serialized_bytes = 0u64;
    let mut shredding_serialized_bytes = 0u64;
    let mut compact_header_serialized_bytes = 0u64;
    let mut legacy_raw_rewards_frames = 0u64;

    let mut reward_blocks = 0u64;
    let mut reward_items = 0u64;
    let mut reward_num_partitions_present = 0u64;
    let mut reward_source_bytes = 0u64;
    let mut reward_serialized_bytes = 0u64;
    let mut reward_raw_fallbacks = 0u64;
    let mut reward_raw_fallback_bytes = 0u64;
    let mut space = ArchiveV2SpaceStats::default();

    let sidecar_dir = input.parent().unwrap_or_else(|| Path::new("."));
    let poh_sidecar_path = sidecar_dir.join(POH_FILE);
    let blockhash_registry_path = sidecar_dir.join(BLOCKHASH_REGISTRY_FILE);
    let mut poh_sidecar_records = 0u64;
    let mut has_poh_sidecar = false;
    if crate::file_nonempty(&poh_sidecar_path) {
        has_poh_sidecar = true;
        let file = File::open(&poh_sidecar_path)
            .with_context(|| format!("open {}", poh_sidecar_path.display()))?;
        let mut poh_reader =
            WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
        while let Some((len, record)) = poh_reader.read::<WincodeArchiveV2PohRecord>()? {
            poh_sidecar_records += 1;
            poh_entries_serialized_bytes += len as u64;
            max_poh_entries_per_block = max_poh_entries_per_block.max(record.entries.len());
            poh_entries += record.entries.len() as u64;
            for entry in &record.entries {
                poh_tx_count_sum += entry.tx_count as u64;
                poh_tx_count_max = poh_tx_count_max.max(entry.tx_count);
                match entry.tx_count {
                    0 => poh_tx_count_zero += 1,
                    1 => poh_tx_count_one += 1,
                    _ => poh_tx_count_multi += 1,
                }
                poh_num_hashes_sum += entry.num_hashes as u128;
                if entry.num_hashes == 0 {
                    poh_num_hashes_zero += 1;
                } else if entry.num_hashes == 1 {
                    poh_num_hashes_one += 1;
                }
            }
        }
    }
    let blockhash_registry_bytes = std::fs::metadata(&blockhash_registry_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let blockhash_registry_entries = blockhash_registry_bytes / 32;

    while let Some((len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        records += 1;
        space.observed_record_bytes += len as u64;
        let block = match record {
            WincodeArchiveV2Record::Header(_) => {
                space.add("record/header", len as u64);
                continue;
            }
            WincodeArchiveV2Record::Genesis(genesis) => {
                space.add("record/genesis", len as u64);
                genesis_records += 1;
                genesis_accounts += (genesis.accounts.len() + genesis.reward_pools.len()) as u64;
                continue;
            }
            WincodeArchiveV2Record::Block(block) => {
                space.add("record/block", len as u64);
                block
            }
            WincodeArchiveV2Record::Index(_) => {
                space.add("record/index", len as u64);
                continue;
            }
            WincodeArchiveV2Record::Footer(_) => {
                space.add("record/footer", len as u64);
                continue;
            }
        };

        blocks += 1;
        txs += block.txs.len() as u64;
        analyze_archive_v2_block_space(&block, &mut space)?;

        let compact = &block.header.compact;
        if !has_poh_sidecar {
            max_poh_entries_per_block = max_poh_entries_per_block.max(compact.poh_entries.len());
            poh_entries += compact.poh_entries.len() as u64;
            poh_entries_serialized_bytes +=
                wincode::config::serialized_size(&compact.poh_entries, wincode_leb128_config())?;
            for entry in &compact.poh_entries {
                poh_tx_count_sum += entry.tx_count as u64;
                poh_tx_count_max = poh_tx_count_max.max(entry.tx_count);
                match entry.tx_count {
                    0 => poh_tx_count_zero += 1,
                    1 => poh_tx_count_one += 1,
                    _ => poh_tx_count_multi += 1,
                }
                poh_num_hashes_sum += entry.num_hashes as u128;
                if entry.num_hashes == 0 {
                    poh_num_hashes_zero += 1;
                } else if entry.num_hashes == 1 {
                    poh_num_hashes_one += 1;
                }
            }
        }
        shredding_serialized_bytes +=
            wincode::config::serialized_size(&compact.shredding, wincode_leb128_config())?;
        compact_header_serialized_bytes +=
            wincode::config::serialized_size(compact, wincode_leb128_config())?;
        if compact.rewards.is_some() {
            legacy_raw_rewards_frames += 1;
        }

        reward_serialized_bytes +=
            wincode::config::serialized_size(&block.header.rewards, wincode_leb128_config())?;
        if let Some(rewards) = &block.header.rewards {
            reward_blocks += 1;
            reward_source_bytes += rewards.source_len;
            if rewards.num_partitions.is_some() {
                reward_num_partitions_present += 1;
            }
            if let Some(decoded) = &rewards.decoded {
                reward_items += decoded.len() as u64;
            }
            if let Some(raw) = &rewards.raw_fallback {
                reward_raw_fallbacks += 1;
                reward_raw_fallback_bytes += raw.len() as u64;
            }
        }

        if max_blocks.is_some_and(|limit| blocks >= limit) {
            break;
        }
    }

    println!(
        "archive_v2_inspect records={records} genesis_records={genesis_records} genesis_accounts={genesis_accounts} blocks={blocks} txs={txs} archive_file_bytes={archive_file_bytes} observed_record_bytes={}",
        space.observed_record_bytes
    );
    println!(
        "known_program_logs_enabled={}",
        blockzilla_format::program_logs::KNOWN_PROGRAM_LOGS_ENABLED
    );
    println!(
        "sidecars poh_path={} poh_records={} blockhash_registry_path={} blockhash_entries={} blockhash_bytes={}",
        poh_sidecar_path.display(),
        poh_sidecar_records,
        blockhash_registry_path.display(),
        blockhash_registry_entries,
        blockhash_registry_bytes
    );
    println!(
        "poh entries={poh_entries} tx_count_sum={poh_tx_count_sum} tx_count_zero={poh_tx_count_zero} tx_count_one={poh_tx_count_one} tx_count_multi={poh_tx_count_multi} tx_count_max={poh_tx_count_max} max_entries_per_block={max_poh_entries_per_block} num_hashes_zero={poh_num_hashes_zero} num_hashes_one={poh_num_hashes_one} num_hashes_sum={poh_num_hashes_sum}"
    );
    println!(
        "poh serialized_bytes={poh_entries_serialized_bytes} avg_bytes_per_entry={:.2} compact_header_bytes={compact_header_serialized_bytes} shredding_bytes={shredding_serialized_bytes}",
        if poh_entries > 0 {
            poh_entries_serialized_bytes as f64 / poh_entries as f64
        } else {
            0.0
        }
    );
    println!(
        "rewards blocks={reward_blocks} reward_items={reward_items} num_partitions_present={reward_num_partitions_present} source_bytes={reward_source_bytes} serialized_bytes={reward_serialized_bytes} raw_fallbacks={reward_raw_fallbacks} raw_fallback_bytes={reward_raw_fallback_bytes} legacy_raw_frames={legacy_raw_rewards_frames}"
    );
    space.print(top);
    Ok(())
}

macro_rules! add_wincode_size {
    ($stats:expr, $name:expr, $value:expr) => {{
        let size = wincode::config::serialized_size($value, wincode_leb128_config())?;
        $stats.add($name, size);
    }};
}

#[derive(Default)]
struct ArchiveV2SpaceStats {
    observed_record_bytes: u64,
    components: BTreeMap<&'static str, u128>,
    log_events: BTreeMap<&'static str, u64>,
    program_logs: BTreeMap<&'static str, u64>,
    log_streams: u64,
    log_events_total: u64,
    program_logs_total: u64,
    pubkey_id_refs: u64,
    pubkey_raw_refs: u64,
    recent_blockhash_ids: u64,
    recent_blockhash_nonces: u64,
    instructions: u64,
    inner_instructions: u64,
}

impl ArchiveV2SpaceStats {
    fn add(&mut self, name: &'static str, bytes: u64) {
        *self.components.entry(name).or_default() += bytes as u128;
    }

    fn bump_log_event(&mut self, name: &'static str) {
        *self.log_events.entry(name).or_default() += 1;
        self.log_events_total += 1;
    }

    fn bump_program_log(&mut self, name: &'static str) {
        *self.program_logs.entry(name).or_default() += 1;
        self.program_logs_total += 1;
    }

    fn print(&self, top: usize) {
        println!(
            "space_summary log_streams={} log_events={} program_logs={} instructions={} inner_instructions={} pubkey_id_refs={} pubkey_raw_refs={} recent_blockhash_ids={} recent_blockhash_nonces={}",
            self.log_streams,
            self.log_events_total,
            self.program_logs_total,
            self.instructions,
            self.inner_instructions,
            self.pubkey_id_refs,
            self.pubkey_raw_refs,
            self.recent_blockhash_ids,
            self.recent_blockhash_nonces
        );

        let mut components = self.components.iter().collect::<Vec<_>>();
        components.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        for (name, bytes) in components.into_iter().take(top.max(1)) {
            let pct = if self.observed_record_bytes > 0 {
                *bytes as f64 * 100.0 / self.observed_record_bytes as f64
            } else {
                0.0
            };
            println!("space_bucket name={name} bytes={bytes} pct_observed={pct:.2}");
        }

        let mut events = self.log_events.iter().collect::<Vec<_>>();
        events.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        for (name, count) in events.into_iter().take(top.max(1)) {
            println!("log_event_kind name={name} count={count}");
        }

        let mut program_logs = self.program_logs.iter().collect::<Vec<_>>();
        program_logs.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        for (name, count) in program_logs.into_iter().take(top.max(1)) {
            println!("program_log_kind name={name} count={count}");
        }
    }
}

fn analyze_archive_v2_block_space(
    block: &WincodeArchiveV2Block,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    add_wincode_size!(stats, "block/header", &block.header);
    add_wincode_size!(stats, "block/header_compact", &block.header.compact);
    add_wincode_size!(stats, "block/header_rewards", &block.header.rewards);
    add_wincode_size!(stats, "block/tx_vec_full", &block.txs);

    for tx in &block.txs {
        add_wincode_size!(stats, "tx/index", &tx.tx_index);
        add_wincode_size!(stats, "tx/payload", &tx.tx);
        add_wincode_size!(stats, "meta/option_payload", &tx.metadata);
        analyze_tx_payload_space(&tx.tx, stats)?;
        if let Some(metadata) = &tx.metadata {
            analyze_meta_payload_space(metadata, stats)?;
        }
    }

    Ok(())
}

fn analyze_tx_payload_space(
    payload: &WincodeArchiveV2Payload<OwnedCompactTransaction>,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    match payload {
        WincodeArchiveV2Payload::Decoded { source_len, value } => {
            stats.add("tx/source_bytes", *source_len);
            add_wincode_size!(stats, "tx/signatures", &value.signatures);
            add_wincode_size!(stats, "tx/message", &value.message);
            analyze_owned_message_space(&value.message, stats)?;
        }
        WincodeArchiveV2Payload::Raw { bytes, error } => {
            stats.add("tx/raw_fallback_bytes", bytes.len() as u64);
            stats.add("tx/raw_fallback_error", error.len() as u64);
        }
    }
    Ok(())
}

fn analyze_owned_message_space(
    message: &OwnedCompactMessage,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    match message {
        OwnedCompactMessage::Legacy(message) => {
            add_wincode_size!(stats, "tx/message_legacy_header", &message.header);
            add_wincode_size!(stats, "tx/account_keys", &message.account_keys);
            add_wincode_size!(stats, "tx/recent_blockhash", &message.recent_blockhash);
            add_wincode_size!(stats, "tx/instructions", &message.instructions);
            for key in &message.account_keys {
                count_compact_pubkey(*key, stats);
            }
            count_recent_blockhash(&message.recent_blockhash, stats);
            analyze_owned_instructions(&message.instructions, stats)?;
        }
        OwnedCompactMessage::V0(message) => {
            add_wincode_size!(stats, "tx/message_v0_header", &message.header);
            add_wincode_size!(stats, "tx/account_keys", &message.account_keys);
            add_wincode_size!(stats, "tx/recent_blockhash", &message.recent_blockhash);
            add_wincode_size!(stats, "tx/instructions", &message.instructions);
            add_wincode_size!(
                stats,
                "tx/address_table_lookups",
                &message.address_table_lookups
            );
            for key in &message.account_keys {
                count_compact_pubkey(*key, stats);
            }
            for lookup in &message.address_table_lookups {
                count_compact_pubkey(lookup.account_key, stats);
            }
            count_recent_blockhash(&message.recent_blockhash, stats);
            analyze_owned_instructions(&message.instructions, stats)?;
        }
    }
    Ok(())
}

fn analyze_owned_instructions(
    instructions: &[OwnedCompactInstruction],
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    stats.instructions += instructions.len() as u64;
    for instruction in instructions {
        add_wincode_size!(stats, "tx/instruction", instruction);
        stats.add(
            "tx/instruction_accounts_raw_bytes",
            instruction.accounts.len() as u64,
        );
        stats.add(
            "tx/instruction_data_raw_bytes",
            instruction.data.len() as u64,
        );
    }
    Ok(())
}

fn analyze_meta_payload_space(
    payload: &WincodeArchiveV2Payload<blockzilla_format::CompactMetaV1>,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    match payload {
        WincodeArchiveV2Payload::Decoded { source_len, value } => {
            stats.add("meta/source_bytes", *source_len);
            analyze_meta_space(value, stats)?;
        }
        WincodeArchiveV2Payload::Raw { bytes, error } => {
            stats.add("meta/raw_fallback_bytes", bytes.len() as u64);
            stats.add("meta/raw_fallback_error", error.len() as u64);
        }
    }
    Ok(())
}

fn analyze_meta_space(
    meta: &blockzilla_format::CompactMetaV1,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    add_wincode_size!(stats, "meta/err", &meta.err);
    add_wincode_size!(stats, "meta/fee", &meta.fee);
    add_wincode_size!(stats, "meta/pre_balances", &meta.pre_balances);
    add_wincode_size!(stats, "meta/post_balances", &meta.post_balances);
    add_wincode_size!(stats, "meta/inner_instructions", &meta.inner_instructions);
    add_wincode_size!(stats, "meta/logs", &meta.logs);
    add_wincode_size!(stats, "meta/pre_token_balances", &meta.pre_token_balances);
    add_wincode_size!(stats, "meta/post_token_balances", &meta.post_token_balances);
    add_wincode_size!(stats, "meta/rewards", &meta.rewards);
    add_wincode_size!(
        stats,
        "meta/loaded_writable_addresses",
        &meta.loaded_writable_addresses
    );
    add_wincode_size!(
        stats,
        "meta/loaded_readonly_addresses",
        &meta.loaded_readonly_addresses
    );
    add_wincode_size!(stats, "meta/return_data", &meta.return_data);
    add_wincode_size!(
        stats,
        "meta/compute_units_consumed",
        &meta.compute_units_consumed
    );
    add_wincode_size!(stats, "meta/cost_units", &meta.cost_units);

    if let Some(inner) = &meta.inner_instructions {
        for group in inner {
            stats.inner_instructions += group.instructions.len() as u64;
            for instruction in &group.instructions {
                stats.add(
                    "meta/inner_instruction_accounts_raw_bytes",
                    instruction.accounts.len() as u64,
                );
                stats.add(
                    "meta/inner_instruction_data_raw_bytes",
                    instruction.data.len() as u64,
                );
            }
        }
    }

    for key in meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
    {
        count_compact_pubkey(*key, stats);
    }
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if let Some(key) = balance.mint {
            count_compact_pubkey(key, stats);
        }
        if let Some(key) = balance.owner {
            count_compact_pubkey(key, stats);
        }
        if let Some(key) = balance.program_id {
            count_compact_pubkey(key, stats);
        }
    }
    for reward in &meta.rewards {
        count_compact_pubkey(reward.pubkey, stats);
    }
    if let Some(return_data) = &meta.return_data {
        count_compact_pubkey(return_data.program_id, stats);
        stats.add("meta/return_data_raw_bytes", return_data.data.len() as u64);
    }

    if let Some(logs) = &meta.logs {
        analyze_log_stream_space(logs, stats)?;
    }

    Ok(())
}

fn analyze_log_stream_space(
    logs: &blockzilla_format::CompactLogStream,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    stats.log_streams += 1;
    add_wincode_size!(stats, "logs/events", &logs.events);
    add_wincode_size!(stats, "logs/string_table", &logs.strings);
    add_wincode_size!(stats, "logs/string_lengths", &logs.strings.lengths);
    stats.add("logs/string_utf8_bytes", logs.strings.bytes.len() as u64);
    add_wincode_size!(stats, "logs/data_table", &logs.data);
    add_wincode_size!(stats, "logs/data_arrays", &logs.data.arrays);
    add_wincode_size!(stats, "logs/data_chunk_lengths", &logs.data.chunk_lengths);
    stats.add("logs/data_raw_bytes", logs.data.bytes.len() as u64);

    for event in &logs.events {
        stats.bump_log_event(log_event_kind(event));
        match event {
            blockzilla_format::LogEvent::System(_)
            | blockzilla_format::LogEvent::ProgramAccountNotWritable
            | blockzilla_format::LogEvent::ProgramIdMismatch
            | blockzilla_format::LogEvent::ProgramNotUpgradeable
            | blockzilla_format::LogEvent::ProgramAndProgramDataAccountMismatch
            | blockzilla_format::LogEvent::ProgramWasExtendedInThisBlockAlready
            | blockzilla_format::LogEvent::LogTruncated
            | blockzilla_format::LogEvent::StakeMergingAccounts
            | blockzilla_format::LogEvent::VerifyEd25519
            | blockzilla_format::LogEvent::VerifySecp256k1
            | blockzilla_format::LogEvent::CloseContextState => {}
            blockzilla_format::LogEvent::ProgramLog(log)
            | blockzilla_format::LogEvent::ProgramPlainLog(log) => {
                stats.bump_program_log(program_log_kind(log));
            }
            blockzilla_format::LogEvent::ProgramIdLog { program, log } => {
                count_compact_pubkey(*program, stats);
                stats.bump_program_log(program_log_kind(log));
            }
            blockzilla_format::LogEvent::LoaderUpgradedProgram { program }
            | blockzilla_format::LogEvent::LoaderFinalizedAccount { account: program }
            | blockzilla_format::LogEvent::Invoke { program, .. }
            | blockzilla_format::LogEvent::BpfInvoke { program }
            | blockzilla_format::LogEvent::Consumed { program, .. }
            | blockzilla_format::LogEvent::Success { program }
            | blockzilla_format::LogEvent::BpfSuccess { program }
            | blockzilla_format::LogEvent::Failure { program, .. }
            | blockzilla_format::LogEvent::BpfFailure { program, .. }
            | blockzilla_format::LogEvent::FailureCustomProgramError { program, .. }
            | blockzilla_format::LogEvent::BpfFailureCustomProgramError { program, .. }
            | blockzilla_format::LogEvent::FailureInvalidAccountData { program }
            | blockzilla_format::LogEvent::BpfFailureInvalidAccountData { program }
            | blockzilla_format::LogEvent::FailureInvalidProgramArgument { program }
            | blockzilla_format::LogEvent::BpfFailureInvalidProgramArgument { program }
            | blockzilla_format::LogEvent::RuntimeWritablePrivilegeEscalated { account: program }
            | blockzilla_format::LogEvent::RuntimeSignerPrivilegeEscalated { account: program }
            | blockzilla_format::LogEvent::RuntimeAccountOwnerBalanceVerificationFailed {
                account: program,
            } => {
                count_compact_pubkey(*program, stats);
            }
            blockzilla_format::LogEvent::Return { program, .. } => {
                count_compact_pubkey(*program, stats);
            }
            blockzilla_format::LogEvent::ProgramNotDeployed { program }
            | blockzilla_format::LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    count_compact_pubkey(*program, stats);
                }
            }
            blockzilla_format::LogEvent::ProgramLogError { .. }
            | blockzilla_format::LogEvent::FailedToComplete { .. }
            | blockzilla_format::LogEvent::CustomProgramError { .. }
            | blockzilla_format::LogEvent::Data { .. }
            | blockzilla_format::LogEvent::Consumption { .. }
            | blockzilla_format::LogEvent::CbRequestUnits { .. }
            | blockzilla_format::LogEvent::UnknownProgram { .. }
            | blockzilla_format::LogEvent::UnknownAccount { .. }
            | blockzilla_format::LogEvent::Plain { .. }
            | blockzilla_format::LogEvent::Unparsed { .. }
            | blockzilla_format::LogEvent::BpfConsumed { .. } => {}
        }
    }

    Ok(())
}

fn count_compact_pubkey(key: CompactPubkey, stats: &mut ArchiveV2SpaceStats) {
    match key {
        CompactPubkey::Id(_) => stats.pubkey_id_refs += 1,
        CompactPubkey::Raw(_) => stats.pubkey_raw_refs += 1,
    }
}

fn count_recent_blockhash(hash: &OwnedCompactRecentBlockhash, stats: &mut ArchiveV2SpaceStats) {
    match hash {
        OwnedCompactRecentBlockhash::Id(_) => stats.recent_blockhash_ids += 1,
        OwnedCompactRecentBlockhash::Nonce(_) => stats.recent_blockhash_nonces += 1,
    }
}

fn log_event_kind(event: &blockzilla_format::LogEvent) -> &'static str {
    match event {
        blockzilla_format::LogEvent::System(_) => "System",
        blockzilla_format::LogEvent::LogTruncated => "LogTruncated",
        blockzilla_format::LogEvent::StakeMergingAccounts => "StakeMergingAccounts",
        blockzilla_format::LogEvent::LoaderUpgradedProgram { .. } => "LoaderUpgradedProgram",
        blockzilla_format::LogEvent::LoaderFinalizedAccount { .. } => "LoaderFinalizedAccount",
        blockzilla_format::LogEvent::ProgramLog(_) => "ProgramLog",
        blockzilla_format::LogEvent::ProgramLogError { .. } => "ProgramLogError",
        blockzilla_format::LogEvent::ProgramIdLog { .. } => "ProgramIdLog",
        blockzilla_format::LogEvent::ProgramPlainLog(_) => "ProgramPlainLog",
        blockzilla_format::LogEvent::ProgramAccountNotWritable => "ProgramAccountNotWritable",
        blockzilla_format::LogEvent::ProgramIdMismatch => "ProgramIdMismatch",
        blockzilla_format::LogEvent::ProgramNotUpgradeable => "ProgramNotUpgradeable",
        blockzilla_format::LogEvent::ProgramAndProgramDataAccountMismatch => {
            "ProgramAndProgramDataAccountMismatch"
        }
        blockzilla_format::LogEvent::ProgramWasExtendedInThisBlockAlready => {
            "ProgramWasExtendedInThisBlockAlready"
        }
        blockzilla_format::LogEvent::Invoke { .. } => "Invoke",
        blockzilla_format::LogEvent::BpfInvoke { .. } => "BpfInvoke",
        blockzilla_format::LogEvent::Consumed { .. } => "Consumed",
        blockzilla_format::LogEvent::BpfConsumed { .. } => "BpfConsumed",
        blockzilla_format::LogEvent::Success { .. } => "Success",
        blockzilla_format::LogEvent::BpfSuccess { .. } => "BpfSuccess",
        blockzilla_format::LogEvent::Failure { .. } => "Failure",
        blockzilla_format::LogEvent::BpfFailure { .. } => "BpfFailure",
        blockzilla_format::LogEvent::FailureCustomProgramError { .. } => {
            "FailureCustomProgramError"
        }
        blockzilla_format::LogEvent::BpfFailureCustomProgramError { .. } => {
            "BpfFailureCustomProgramError"
        }
        blockzilla_format::LogEvent::FailureInvalidAccountData { .. } => {
            "FailureInvalidAccountData"
        }
        blockzilla_format::LogEvent::BpfFailureInvalidAccountData { .. } => {
            "BpfFailureInvalidAccountData"
        }
        blockzilla_format::LogEvent::FailureInvalidProgramArgument { .. } => {
            "FailureInvalidProgramArgument"
        }
        blockzilla_format::LogEvent::BpfFailureInvalidProgramArgument { .. } => {
            "BpfFailureInvalidProgramArgument"
        }
        blockzilla_format::LogEvent::FailedToComplete { .. } => "FailedToComplete",
        blockzilla_format::LogEvent::CustomProgramError { .. } => "CustomProgramError",
        blockzilla_format::LogEvent::Return { .. } => "Return",
        blockzilla_format::LogEvent::Data { .. } => "Data",
        blockzilla_format::LogEvent::Consumption { .. } => "Consumption",
        blockzilla_format::LogEvent::CbRequestUnits { .. } => "CbRequestUnits",
        blockzilla_format::LogEvent::ProgramNotDeployed { .. } => "ProgramNotDeployed",
        blockzilla_format::LogEvent::ProgramNotCached { .. } => "ProgramNotCached",
        blockzilla_format::LogEvent::UnknownProgram { .. } => "UnknownProgram",
        blockzilla_format::LogEvent::UnknownAccount { .. } => "UnknownAccount",
        blockzilla_format::LogEvent::VerifyEd25519 => "VerifyEd25519",
        blockzilla_format::LogEvent::VerifySecp256k1 => "VerifySecp256k1",
        blockzilla_format::LogEvent::RuntimeWritablePrivilegeEscalated { .. } => {
            "RuntimeWritablePrivilegeEscalated"
        }
        blockzilla_format::LogEvent::RuntimeSignerPrivilegeEscalated { .. } => {
            "RuntimeSignerPrivilegeEscalated"
        }
        blockzilla_format::LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { .. } => {
            "RuntimeAccountOwnerBalanceVerificationFailed"
        }
        blockzilla_format::LogEvent::CloseContextState => "CloseContextState",
        blockzilla_format::LogEvent::Plain { .. } => "Plain",
        blockzilla_format::LogEvent::Unparsed { .. } => "Unparsed",
    }
}

fn program_log_kind(log: &blockzilla_format::program_logs::ProgramLog) -> &'static str {
    match log {
        blockzilla_format::program_logs::ProgramLog::Empty => "Empty",
        blockzilla_format::program_logs::ProgramLog::Token(_) => "Token",
        blockzilla_format::program_logs::ProgramLog::Token2022(_) => "Token2022",
        blockzilla_format::program_logs::ProgramLog::Ata(_) => "Ata",
        blockzilla_format::program_logs::ProgramLog::AddressLookupTable(_) => "AddressLookupTable",
        blockzilla_format::program_logs::ProgramLog::LoaderV3(_) => "LoaderV3",
        blockzilla_format::program_logs::ProgramLog::LoaderV4(_) => "LoaderV4",
        blockzilla_format::program_logs::ProgramLog::Memo(_) => "Memo",
        blockzilla_format::program_logs::ProgramLog::Record(_) => "Record",
        blockzilla_format::program_logs::ProgramLog::TransferHook(_) => "TransferHook",
        blockzilla_format::program_logs::ProgramLog::AccountCompression(_) => "AccountCompression",
        blockzilla_format::program_logs::ProgramLog::Stake(_) => "Stake",
        blockzilla_format::program_logs::ProgramLog::ZkElgamalProof(_) => "ZkElgamalProof",
        blockzilla_format::program_logs::ProgramLog::AnchorInstruction { .. } => {
            "AnchorInstruction"
        }
        blockzilla_format::program_logs::ProgramLog::AnchorErrorOccurred { .. } => {
            "AnchorErrorOccurred"
        }
        blockzilla_format::program_logs::ProgramLog::AnchorErrorThrown { .. } => {
            "AnchorErrorThrown"
        }
        blockzilla_format::program_logs::ProgramLog::Unknown(_) => "Unknown",
        blockzilla_format::program_logs::ProgramLog::Known(known) => known_program_log_kind(known),
    }
}

fn known_program_log_kind(
    log: &blockzilla_format::program_logs::known_programs::KnownProgramLog,
) -> &'static str {
    match log {
        blockzilla_format::program_logs::known_programs::KnownProgramLog::Drift(_) => "Known/Drift",
        blockzilla_format::program_logs::known_programs::KnownProgramLog::OkxRouter(_) => {
            "Known/OkxRouter"
        }
        blockzilla_format::program_logs::known_programs::KnownProgramLog::PhoenixPerps(_) => {
            "Known/PhoenixPerps"
        }
        blockzilla_format::program_logs::known_programs::KnownProgramLog::PhoenixV1(_) => {
            "Known/PhoenixV1"
        }
        blockzilla_format::program_logs::known_programs::KnownProgramLog::RaydiumAmm(_) => {
            "Known/RaydiumAmm"
        }
        blockzilla_format::program_logs::known_programs::KnownProgramLog::Static(_) => {
            "Known/Static"
        }
    }
}

struct ArchiveV2BlockSidecar {
    poh_entries: Vec<CompactPohEntry>,
    blockhash: [u8; 32],
}

fn block_sidecar_from_entries(
    slot: u64,
    entries: &[PendingEntry],
    external_blockhashes: &ExternalBlockhashOverrides,
    previous_blockhash: Option<[u8; 32]>,
) -> Result<ArchiveV2BlockSidecar> {
    if entries.is_empty() {
        let override_entry = external_blockhashes.get(&slot).ok_or_else(|| {
            anyhow!("slot {slot} block has no PoH entries and no external blockhash override")
        })?;
        if let (Some(ticks), Some(hashes_per_tick), Some(previous_blockhash)) = (
            override_entry.reconstruct_ticks,
            override_entry.hashes_per_tick,
            previous_blockhash,
        ) {
            let poh_entries =
                reconstruct_tick_only_poh(slot, previous_blockhash, ticks, hashes_per_tick)?;
            let reconstructed_blockhash = poh_entries
                .last()
                .map(|entry| entry.hash)
                .context("reconstructed PoH had no entries")?;
            anyhow::ensure!(
                reconstructed_blockhash == override_entry.hash,
                "slot {slot} reconstructed tick-only PoH ended at {} but override blockhash is {}",
                Pubkey::new_from_array(reconstructed_blockhash),
                Pubkey::new_from_array(override_entry.hash)
            );
            info!(
                "Reconstructed tick-only PoH for gap slot {} ticks={} hashes_per_tick={} source={} blockhash={}",
                slot,
                ticks,
                hashes_per_tick,
                override_entry.source,
                Pubkey::new_from_array(override_entry.hash)
            );
            return Ok(ArchiveV2BlockSidecar {
                poh_entries,
                blockhash: override_entry.hash,
            });
        }
        info!(
            "Using external blockhash override for PoH-gap slot {} source={} blockhash={}",
            slot,
            override_entry.source,
            Pubkey::new_from_array(override_entry.hash)
        );
        return Ok(ArchiveV2BlockSidecar {
            poh_entries: Vec::new(),
            blockhash: override_entry.hash,
        });
    }
    anyhow::ensure!(
        !external_blockhashes.contains_key(&slot),
        "slot {slot} has PoH entries and must not use an external blockhash override"
    );
    let poh_entries = entries
        .iter()
        .map(|entry| CompactPohEntry {
            num_hashes: entry.entry.num_hashes,
            hash: entry.entry.hash,
            tx_count: entry.entry.transactions.len() as u32,
        })
        .collect::<Vec<_>>();
    let blockhash = poh_entries
        .last()
        .map(|entry| entry.hash)
        .ok_or_else(|| anyhow!("slot {slot} block has no PoH entries"))?;
    Ok(ArchiveV2BlockSidecar {
        poh_entries,
        blockhash,
    })
}

fn reconstruct_tick_only_poh(
    slot: u64,
    previous_blockhash: [u8; 32],
    ticks: u32,
    hashes_per_tick: u64,
) -> Result<Vec<CompactPohEntry>> {
    anyhow::ensure!(
        ticks > 0,
        "slot {slot} reconstructed PoH ticks must be non-zero"
    );
    anyhow::ensure!(
        hashes_per_tick > 0,
        "slot {slot} reconstructed PoH hashes_per_tick must be non-zero"
    );
    let mut hash = previous_blockhash;
    let mut entries = Vec::with_capacity(ticks as usize);
    for _ in 0..ticks {
        for _ in 0..hashes_per_tick {
            let digest = Sha256::digest(hash);
            hash.copy_from_slice(&digest);
        }
        entries.push(CompactPohEntry {
            num_hashes: hashes_per_tick,
            hash,
            tx_count: 0,
        });
    }
    Ok(entries)
}

fn decode_block_slot_time(payload: &[u8]) -> Result<(u64, Option<i64>)> {
    let mut decoder = minicbor::Decoder::new(payload);
    let array_len = decoder
        .array()?
        .ok_or_else(|| anyhow!("indefinite block arrays are not supported"))?;
    anyhow::ensure!(array_len >= 5, "block array too short: {array_len}");
    let kind = decoder.u64()?;
    anyhow::ensure!(kind == 2, "expected block kind 2, got {kind}");
    let slot = decoder.u64().context("decode block slot")?;
    decoder.skip().context("skip block shredding")?;
    decoder.skip().context("skip block entry refs")?;
    let meta: of_car_reader::node::SlotMeta = decoder.decode().context("decode block meta")?;
    Ok((slot, meta.blocktime))
}

fn hash_32(bytes: &[u8]) -> Result<[u8; 32]> {
    let mut out = [0u8; 32];
    anyhow::ensure!(
        bytes.len() == out.len(),
        "expected 32 byte hash, got {}",
        bytes.len()
    );
    out.copy_from_slice(bytes);
    Ok(out)
}

fn write_blockhash_index_v3_header<W: Write>(writer: &mut W, rows: u64) -> Result<()> {
    writer.write_all(ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC)?;
    writer.write_all(&ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION.to_le_bytes())?;
    writer.write_all(&(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN as u16).to_le_bytes())?;
    writer.write_all(&rows.to_le_bytes())?;
    debug_assert_eq!(ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN, 20);
    Ok(())
}

fn write_blockhash_index_v3_row<W: Write>(
    writer: &mut W,
    slot: u64,
    blockhash: &[u8; 32],
    block_time: i64,
) -> Result<()> {
    writer.write_all(&slot.to_le_bytes())?;
    writer.write_all(blockhash)?;
    writer.write_all(&block_time.to_le_bytes())?;
    Ok(())
}

/// Transactional V3 timestamp-index publication shared by full Archive V2 compactors.
///
/// Rows are accumulated during the compactor's existing CAR pass. Partial `--max-blocks`
/// builds never create this writer, so a published V3 index always describes the complete
/// source epoch and can immediately become the source of `block-time-gaps.bin`.
struct BlockhashIndexV3Publisher {
    output_dir: PathBuf,
    path: PathBuf,
    temp_path: PathBuf,
    writer: BufWriter<File>,
    rows: u64,
}

impl BlockhashIndexV3Publisher {
    fn create(output_dir: &Path) -> Result<Self> {
        let path = output_dir.join(BLOCKHASH_INDEX_V3_FILE);
        let temp_path = output_dir.join(format!("{BLOCKHASH_INDEX_V3_FILE}.tmp"));

        // Invalidate the old source first and sync that deletion before touching
        // any other artifact. In particular, a crashed PreHot rebuild must not be
        // reusable with a new spool and an old-but-valid V3/gap pair.
        if path.exists() {
            std::fs::remove_file(&path)
                .with_context(|| format!("remove stale {}", path.display()))?;
            crate::first_seen_finalization::sync_directory(output_dir)?;
        }
        let mut removed_derived_artifact = false;
        for stale_path in [output_dir.join(BLOCK_TIME_GAP_FILE), temp_path.clone()] {
            if stale_path.exists() {
                std::fs::remove_file(&stale_path)
                    .with_context(|| format!("remove stale {}", stale_path.display()))?;
                removed_derived_artifact = true;
            }
        }
        if removed_derived_artifact {
            crate::first_seen_finalization::sync_directory(output_dir)?;
        }
        let file =
            File::create(&temp_path).with_context(|| format!("create {}", temp_path.display()))?;
        let mut writer = BufWriter::with_capacity(1 << 20, file);
        write_blockhash_index_v3_header(&mut writer, 0)
            .with_context(|| format!("write {}", temp_path.display()))?;
        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            path,
            temp_path,
            writer,
            rows: 0,
        })
    }

    fn push(&mut self, slot: u64, blockhash: &[u8; 32], block_time: Option<i64>) -> Result<()> {
        write_blockhash_index_v3_row(&mut self.writer, slot, blockhash, block_time.unwrap_or(0))
            .with_context(|| format!("write {} row {}", self.temp_path.display(), self.rows))?;
        self.rows = self
            .rows
            .checked_add(1)
            .context("blockhash index V3 row count overflow")?;
        Ok(())
    }

    fn publish(mut self, expected_rows: u64) -> Result<()> {
        anyhow::ensure!(
            self.rows == expected_rows,
            "blockhash index V3 collected {} rows but compactor completed {expected_rows}",
            self.rows
        );
        self.writer
            .flush()
            .with_context(|| format!("flush {}", self.temp_path.display()))?;
        self.writer
            .seek(SeekFrom::Start(
                (ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC.len() + 2 + 2) as u64,
            ))
            .with_context(|| format!("seek {}", self.temp_path.display()))?;
        self.writer
            .write_all(&self.rows.to_le_bytes())
            .with_context(|| format!("write row count {}", self.temp_path.display()))?;
        self.writer
            .flush()
            .with_context(|| format!("flush row count {}", self.temp_path.display()))?;
        self.writer
            .get_ref()
            .sync_all()
            .with_context(|| format!("sync {}", self.temp_path.display()))?;
        drop(self.writer);
        std::fs::rename(&self.temp_path, &self.path).with_context(|| {
            format!(
                "publish blockhash index V3 {} -> {}",
                self.temp_path.display(),
                self.path.display()
            )
        })?;
        crate::first_seen_finalization::sync_directory(&self.output_dir)?;
        ensure_default_block_time_gaps(&self.output_dir)?;
        info!(
            "Published default block-time gap artifacts: rows={} v3={} gaps={}",
            self.rows,
            self.path.display(),
            self.output_dir.join(BLOCK_TIME_GAP_FILE).display()
        );
        Ok(())
    }
}

fn full_blockhash_index_v3_publisher(
    output_dir: &Path,
    max_blocks: Option<u64>,
) -> Result<Option<BlockhashIndexV3Publisher>> {
    if max_blocks.is_some() {
        let mut removed = false;
        for path in [
            output_dir.join(BLOCKHASH_INDEX_V3_FILE),
            output_dir.join(BLOCK_TIME_GAP_FILE),
            output_dir.join(format!("{BLOCKHASH_INDEX_V3_FILE}.tmp")),
        ] {
            if path.exists() {
                std::fs::remove_file(&path).with_context(|| {
                    format!(
                        "remove complete-epoch artifact from bounded compaction {}",
                        path.display()
                    )
                })?;
                removed = true;
            }
        }
        if removed {
            crate::first_seen_finalization::sync_directory(output_dir)?;
            info!(
                "Removed complete-epoch timestamp artifacts before bounded --max-blocks compaction: {}",
                output_dir.display()
            );
        }
        return Ok(None);
    }
    BlockhashIndexV3Publisher::create(output_dir).map(Some)
}

pub(crate) fn ensure_default_block_time_gaps(output_dir: &Path) -> Result<()> {
    crate::block_time_gaps::build_block_time_gaps(
        crate::block_time_gaps::BuildBlockTimeGapsConfig {
            input: output_dir,
            epoch: None,
            output: None,
            // This is a deterministic derived artifact. Automatic compaction may safely repair
            // a stale or interrupted prior sidecar once the complete V3 source is validated.
            force: true,
            source: crate::block_time_gaps::BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        },
    )?;
    Ok(())
}

fn build_block_record(
    pending: &mut PendingBlock,
    block: RawBlockNode,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    block_id: u32,
    footer: &mut WincodeArchiveV2Footer,
    timings: &mut ArchiveV2Timings,
    external_blockhashes: &ExternalBlockhashOverrides,
    previous_blockhash: Option<[u8; 32]>,
    zstd: &mut ZstdReusableDecoder,
    parallel_tx_decoder: Option<&mut FirstSeenTxDecodePool>,
    mut first_seen_signatures: Option<&mut FirstSeenBlockSignatures>,
) -> Result<(WincodeArchiveV2Block, u32, ArchiveV2BlockSidecar)> {
    pending.last_slot = block.slot;
    let entries = ordered_entries(pending, &block)?;
    let sidecar = block_sidecar_from_entries(
        block.slot,
        entries,
        external_blockhashes,
        previous_blockhash,
    )?;
    validate_ordered_transactions(pending, entries, block.slot)?;
    let ordered_txs = &pending.transactions;
    let rewards = block_rewards(pending, &block, key_index, zstd, footer, timings)?;
    if let Some(signatures) = first_seen_signatures.as_deref_mut() {
        signatures.collect_block(ordered_txs, &pending.dataframes, block.slot)?;
    }
    let first_seen_signature_counts = first_seen_signatures
        .as_deref()
        .map(|signatures| signatures.counts.as_slice());
    if let Some(signature_counts) = first_seen_signature_counts {
        anyhow::ensure!(
            signature_counts.len() == ordered_txs.len(),
            "slot {} collected {} signature counts for {} transactions",
            block.slot,
            signature_counts.len(),
            ordered_txs.len(),
        );
    }

    let compact_header = CompactBlockHeader {
        slot: block.slot,
        parent_slot: block.meta.parent_slot.unwrap_or(0),
        blockhash: block_id,
        previous_blockhash: block_id.saturating_sub(1),
        block_time: block.meta.blocktime,
        block_height: block.meta.block_height,
        shredding: block
            .shredding
            .iter()
            .map(|item| CompactShredding {
                entry_end_idx: item.entry_end_idx,
                shred_end_idx: item.shred_end_idx,
            })
            .collect(),
        poh_entries: Vec::new(),
        rewards: None,
    };

    let txs = if let Some(decoder) =
        parallel_tx_decoder.filter(|_| ordered_txs.len() >= FIRST_SEEN_PARALLEL_MIN_TRANSACTIONS)
    {
        let signature_counts = first_seen_signature_counts
            .context("parallel first-seen decode requires collected signature counts")?;
        decoder.decode_block_transactions(
            ordered_txs,
            &pending.dataframes,
            signature_counts,
            block.slot,
            key_index,
            rolling_blockhashes,
            footer,
            timings,
        )?
    } else {
        let mut txs = Vec::with_capacity(ordered_txs.len());
        let scratch = &mut pending.record_scratch;
        for (tx_index, pending_tx) in ordered_txs.iter().enumerate() {
            let assemble_started = timings.detail_timer();
            let (tx_bytes, tx_scratch_capacity) = dataframe_bytes_for_decode(
                &pending_tx.tx.data,
                &pending.dataframes,
                &mut scratch.tx_bytes,
                &mut scratch.reassemble_visited,
            )
            .with_context(|| {
                format!(
                    "slot {} tx#{tx_index} reassemble transaction bytes",
                    block.slot
                )
            })?;
            timings.dataframe_assemble += assemble_started.elapsed();
            footer.tx_source_bytes += tx_bytes.len() as u64;
            timings.tx_reassembled += 1;
            timings.tx_scratch_max = timings.tx_scratch_max.max(tx_scratch_capacity);
            let tx_started = timings.detail_timer();
            let value = if let Some(signature_counts) = first_seen_signature_counts {
                decode_first_seen_compact_transaction(
                    block.slot,
                    tx_index,
                    tx_bytes,
                    signature_counts[tx_index],
                    key_index,
                    rolling_blockhashes,
                    &mut footer.nonce_recent_blockhashes,
                )
            } else {
                wincode::deserialize::<VersionedTransaction<'_>>(tx_bytes)
                    .map_err(|err| anyhow!("{err}"))
                    .and_then(|versioned| {
                        to_owned_compact_transaction(
                            block.slot,
                            tx_index,
                            versioned,
                            key_index,
                            rolling_blockhashes,
                            &mut footer.nonce_recent_blockhashes,
                        )
                    })
            }
            .with_context(|| format!("slot {} tx#{tx_index} transaction", block.slot))?;
            let tx = WincodeArchiveV2Payload::Decoded {
                source_len: tx_bytes.len() as u64,
                value,
            };
            timings.tx_decode_compact += tx_started.elapsed();

            let assemble_started = timings.detail_timer();
            let (metadata_bytes, metadata_scratch_capacity) = dataframe_bytes_for_decode(
                &pending_tx.tx.metadata,
                &pending.dataframes,
                &mut scratch.metadata_bytes,
                &mut scratch.reassemble_visited,
            )
            .with_context(|| {
                format!(
                    "slot {} tx#{tx_index} reassemble metadata bytes",
                    block.slot
                )
            })?;
            timings.dataframe_assemble += assemble_started.elapsed();
            footer.metadata_source_bytes += metadata_bytes.len() as u64;
            timings.metadata_reassembled += 1;
            timings.metadata_scratch_max =
                timings.metadata_scratch_max.max(metadata_scratch_capacity);
            let metadata = if metadata_bytes.is_empty() {
                None
            } else {
                let metadata_started = timings.detail_timer();
                let payload = decode_metadata_payload(
                    block.slot,
                    tx_index,
                    metadata_bytes,
                    key_index,
                    zstd,
                    timings,
                )?;
                timings.metadata_decode_compact += metadata_started.elapsed();
                Some(payload)
            };

            txs.push(WincodeArchiveV2Transaction {
                tx_index: pending_tx.tx.index.unwrap_or(tx_index as u64) as u32,
                tx,
                metadata,
            });
        }
        txs
    };

    Ok((
        WincodeArchiveV2Block {
            header: WincodeArchiveV2BlockHeader {
                compact: compact_header,
                rewards,
            },
            txs,
        },
        ordered_txs.len() as u32,
        sidecar,
    ))
}

fn build_no_registry_block_record(
    pending: &mut PendingBlock,
    block: RawBlockNode,
    block_id: u32,
    footer: &mut WincodeArchiveV2Footer,
    timings: &mut ArchiveV2Timings,
    zstd: &mut ZstdReusableDecoder,
) -> Result<(WincodeArchiveV2NoRegistryBlock, u32, ArchiveV2BlockSidecar)> {
    pending.last_slot = block.slot;
    let entries = ordered_entries(pending, &block)?;
    let sidecar = block_sidecar_from_entries(
        block.slot,
        entries,
        &ExternalBlockhashOverrides::default(),
        None,
    )?;
    validate_ordered_transactions(pending, entries, block.slot)?;
    let ordered_txs = &pending.transactions;
    let rewards = block_rewards_no_registry(pending, &block, zstd, footer, timings)?;

    let compact_header = CompactBlockHeader {
        slot: block.slot,
        parent_slot: block.meta.parent_slot.unwrap_or(0),
        blockhash: block_id,
        previous_blockhash: block_id.saturating_sub(1),
        block_time: block.meta.blocktime,
        block_height: block.meta.block_height,
        shredding: block
            .shredding
            .iter()
            .map(|item| CompactShredding {
                entry_end_idx: item.entry_end_idx,
                shred_end_idx: item.shred_end_idx,
            })
            .collect(),
        poh_entries: Vec::new(),
        rewards: None,
    };

    let mut txs = Vec::with_capacity(ordered_txs.len());
    let mut tx_bytes = Vec::new();
    let mut metadata_bytes = Vec::new();
    let mut reassemble_visited = HashSet::new();
    for (tx_index, pending_tx) in ordered_txs.iter().enumerate() {
        let assemble_started = timings.detail_timer();
        pending_tx
            .tx
            .transaction_bytes_into(&pending.dataframes, &mut tx_bytes, &mut reassemble_visited)
            .with_context(|| {
                format!(
                    "slot {} tx#{tx_index} reassemble transaction bytes",
                    block.slot
                )
            })?;
        timings.dataframe_assemble += assemble_started.elapsed();
        footer.tx_source_bytes += tx_bytes.len() as u64;
        timings.tx_reassembled += 1;
        timings.tx_scratch_max = timings.tx_scratch_max.max(tx_bytes.capacity());
        let tx_started = timings.detail_timer();
        let value = wincode::deserialize::<VersionedTransaction<'_>>(&tx_bytes)
            .map_err(|err| anyhow!("{err}"))
            .map(to_no_registry_transaction)
            .with_context(|| format!("slot {} tx#{tx_index} transaction", block.slot))?;
        let tx = WincodeArchiveV2Payload::Decoded {
            source_len: tx_bytes.len() as u64,
            value,
        };
        timings.tx_decode_compact += tx_started.elapsed();

        let assemble_started = timings.detail_timer();
        pending_tx
            .tx
            .metadata_bytes_into(
                &pending.dataframes,
                &mut metadata_bytes,
                &mut reassemble_visited,
            )
            .with_context(|| {
                format!(
                    "slot {} tx#{tx_index} reassemble metadata bytes",
                    block.slot
                )
            })?;
        timings.dataframe_assemble += assemble_started.elapsed();
        footer.metadata_source_bytes += metadata_bytes.len() as u64;
        timings.metadata_reassembled += 1;
        timings.metadata_scratch_max = timings.metadata_scratch_max.max(metadata_bytes.capacity());
        let metadata = if metadata_bytes.is_empty() {
            None
        } else {
            let metadata_started = timings.detail_timer();
            let payload = decode_no_registry_metadata_payload(
                block.slot,
                tx_index,
                &metadata_bytes,
                zstd,
                timings,
            )?;
            timings.metadata_decode_compact += metadata_started.elapsed();
            Some(payload)
        };

        txs.push(WincodeArchiveV2NoRegistryTransaction {
            tx_index: pending_tx.tx.index.unwrap_or(tx_index as u64) as u32,
            tx,
            metadata,
        });
    }

    Ok((
        WincodeArchiveV2NoRegistryBlock {
            header: WincodeArchiveV2NoRegistryBlockHeader {
                compact: compact_header,
                rewards,
            },
            txs,
        },
        ordered_txs.len() as u32,
        sidecar,
    ))
}

fn to_owned_compact_transaction(
    slot: u64,
    tx_index: usize,
    vtx: VersionedTransaction<'_>,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    nonce_recent_blockhashes: &mut u64,
) -> Result<OwnedCompactTransaction> {
    let VersionedTransaction {
        signatures,
        message,
    } = vtx;
    let signatures = signatures
        .into_iter()
        .map(|signature| signature.to_vec())
        .collect();
    let message = to_owned_compact_message(
        slot,
        tx_index,
        message,
        key_index,
        rolling_blockhashes,
        nonce_recent_blockhashes,
    )?;

    Ok(OwnedCompactTransaction {
        signatures,
        message,
    })
}

fn decode_first_seen_compact_transaction(
    slot: u64,
    tx_index: usize,
    transaction_bytes: &[u8],
    expected_signature_count: u8,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    nonce_recent_blockhashes: &mut u64,
) -> Result<OwnedCompactTransaction> {
    let (decoded_signature_count, prefix_len) =
        solana_short_vec::decode_shortu16_len(transaction_bytes)
            .map_err(|()| anyhow!("invalid transaction signature ShortU16 prefix"))?;
    let decoded_signature_count = u8::try_from(decoded_signature_count)
        .context("transaction signature count exceeds hot-row u8 maximum")?;
    anyhow::ensure!(
        decoded_signature_count == expected_signature_count,
        "decoded signature count {} does not match collected count {}",
        decoded_signature_count,
        expected_signature_count,
    );
    let signature_bytes_len = usize::from(decoded_signature_count)
        .checked_mul(FIRST_SEEN_SIGNATURE_BYTES)
        .context("transaction signature byte length overflow")?;
    let message_offset = prefix_len
        .checked_add(signature_bytes_len)
        .context("transaction message offset overflow")?;
    anyhow::ensure!(
        message_offset <= transaction_bytes.len(),
        "truncated transaction signatures: have {} bytes after prefix, expected {}",
        transaction_bytes.len().saturating_sub(prefix_len),
        signature_bytes_len,
    );
    let message =
        wincode::deserialize::<VersionedMessage<'_>>(&transaction_bytes[message_offset..])
            .map_err(|error| anyhow!("{error}"))?;
    let message = to_owned_compact_message(
        slot,
        tx_index,
        message,
        key_index,
        rolling_blockhashes,
        nonce_recent_blockhashes,
    )?;
    Ok(OwnedCompactTransaction {
        signatures: Vec::new(),
        message,
    })
}

fn to_owned_compact_message(
    slot: u64,
    tx_index: usize,
    message: VersionedMessage<'_>,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    nonce_recent_blockhashes: &mut u64,
) -> Result<OwnedCompactMessage> {
    Ok(match message {
        VersionedMessage::Legacy(message) => {
            OwnedCompactMessage::Legacy(OwnedCompactLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: message.header.num_required_signatures,
                    num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
                    num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
                },
                account_keys: message
                    .account_keys
                    .into_iter()
                    .map(|key| key_index.compact(key))
                    .collect(),
                recent_blockhash: resolve_recent_blockhash(
                    rolling_blockhashes,
                    message.recent_blockhash,
                    slot,
                    tx_index,
                    nonce_recent_blockhashes,
                )?,
                instructions: message
                    .instructions
                    .into_iter()
                    .map(owned_instruction)
                    .collect(),
            })
        }
        VersionedMessage::V0(message) => OwnedCompactMessage::V0(OwnedCompactV0Message {
            header: CompactMessageHeader {
                num_required_signatures: message.header.num_required_signatures,
                num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
            },
            account_keys: message
                .account_keys
                .into_iter()
                .map(|key| key_index.compact(key))
                .collect(),
            recent_blockhash: resolve_recent_blockhash(
                rolling_blockhashes,
                message.recent_blockhash,
                slot,
                tx_index,
                nonce_recent_blockhashes,
            )?,
            instructions: message
                .instructions
                .into_iter()
                .map(owned_instruction)
                .collect(),
            address_table_lookups: message
                .address_table_lookups
                .into_iter()
                .map(|lookup| OwnedCompactAddressTableLookup {
                    account_key: key_index.compact(lookup.account_key),
                    writable_indexes: lookup.writable_indexes,
                    readonly_indexes: lookup.readonly_indexes,
                })
                .collect(),
        }),
    })
}

fn owned_instruction(
    instruction: of_car_reader::versioned_transaction::CompiledInstruction,
) -> OwnedCompactInstruction {
    OwnedCompactInstruction {
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts,
        data: instruction.data,
    }
}

fn to_no_registry_transaction(vtx: VersionedTransaction<'_>) -> WincodeArchiveV2NoRegistryTx {
    let signatures = vtx
        .signatures
        .into_iter()
        .map(|signature| signature.to_vec())
        .collect();
    let message = match vtx.message {
        VersionedMessage::Legacy(message) => {
            WincodeArchiveV2NoRegistryMessage::Legacy(WincodeArchiveV2NoRegistryLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: message.header.num_required_signatures,
                    num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
                    num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
                },
                account_keys: message.account_keys.into_iter().map(|key| *key).collect(),
                recent_blockhash: *message.recent_blockhash,
                instructions: message
                    .instructions
                    .into_iter()
                    .map(no_registry_instruction)
                    .collect(),
            })
        }
        VersionedMessage::V0(message) => {
            WincodeArchiveV2NoRegistryMessage::V0(WincodeArchiveV2NoRegistryV0Message {
                header: CompactMessageHeader {
                    num_required_signatures: message.header.num_required_signatures,
                    num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
                    num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
                },
                account_keys: message.account_keys.into_iter().map(|key| *key).collect(),
                recent_blockhash: *message.recent_blockhash,
                instructions: message
                    .instructions
                    .into_iter()
                    .map(no_registry_instruction)
                    .collect(),
                address_table_lookups: message
                    .address_table_lookups
                    .into_iter()
                    .map(|lookup| WincodeArchiveV2NoRegistryAddressTableLookup {
                        account_key: *lookup.account_key,
                        writable_indexes: lookup.writable_indexes,
                        readonly_indexes: lookup.readonly_indexes,
                    })
                    .collect(),
            })
        }
    };

    WincodeArchiveV2NoRegistryTx {
        signatures,
        message,
    }
}

fn no_registry_instruction(
    instruction: of_car_reader::versioned_transaction::CompiledInstruction,
) -> WincodeArchiveV2NoRegistryInstruction {
    WincodeArchiveV2NoRegistryInstruction {
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts,
        data: instruction.data,
    }
}

fn decode_metadata_payload(
    slot: u64,
    tx_index: usize,
    metadata_bytes: &[u8],
    key_index: &KeyIndex,
    zstd: &mut ZstdReusableDecoder,
    timings: &mut ArchiveV2Timings,
) -> Result<WincodeArchiveV2Payload<blockzilla_format::CompactMetaV1>> {
    let mut meta = TransactionStatusMeta::default();
    let decoded = if slot_uses_protobuf_metadata(slot) {
        timings.metadata_protobuf_visit += 1;
        let protobuf_bytes = if zstd
            .decompress_if_zstd(metadata_bytes)
            .with_context(|| format!("slot {slot} tx#{tx_index} metadata zstd"))?
        {
            zstd.output()
        } else {
            metadata_bytes
        };
        blockzilla_format::compact_meta_from_protobuf_visit(protobuf_bytes, key_index)
    } else {
        timings.metadata_owned_fallback += 1;
        decode_transaction_status_meta_from_frame(slot, metadata_bytes, &mut meta, zstd)
            .map_err(|err| anyhow!("{err}"))
            .and_then(|()| blockzilla_format::compact_meta_from_proto(&meta, key_index))
    };

    let value = decoded.with_context(|| format!("slot {slot} tx#{tx_index} metadata"))?;
    Ok(WincodeArchiveV2Payload::Decoded {
        source_len: metadata_bytes.len() as u64,
        value,
    })
}

fn decode_no_registry_metadata_payload(
    slot: u64,
    tx_index: usize,
    metadata_bytes: &[u8],
    zstd: &mut ZstdReusableDecoder,
    timings: &mut ArchiveV2Timings,
) -> Result<WincodeArchiveV2Payload<WincodeArchiveV2NoRegistryMeta>> {
    let mut meta = TransactionStatusMeta::default();
    let decoded = if slot_uses_protobuf_metadata(slot) {
        timings.metadata_protobuf_visit += 1;
        let protobuf_bytes = if zstd
            .decompress_if_zstd(metadata_bytes)
            .with_context(|| format!("slot {slot} tx#{tx_index} metadata zstd"))?
        {
            zstd.output()
        } else {
            metadata_bytes
        };
        no_registry_meta_from_protobuf_visit(protobuf_bytes)
    } else {
        timings.metadata_owned_fallback += 1;
        decode_transaction_status_meta_from_frame(slot, metadata_bytes, &mut meta, zstd)
            .map_err(|err| anyhow!("{err}"))
            .and_then(|()| no_registry_meta_from_proto(&meta))
    };

    let value = decoded.with_context(|| format!("slot {slot} tx#{tx_index} metadata"))?;
    Ok(WincodeArchiveV2Payload::Decoded {
        source_len: metadata_bytes.len() as u64,
        value,
    })
}

fn no_registry_meta_from_protobuf_visit(bytes: &[u8]) -> Result<WincodeArchiveV2NoRegistryMeta> {
    let mut visitor = NoRegistryMetaVisitor::default();
    visit_protobuf_transaction_status_meta(bytes, &mut visitor)
        .map_err(|err| anyhow!("protobuf visit: {err}"))?;
    visitor.finish()
}

fn no_registry_meta_from_proto(
    meta: &TransactionStatusMeta,
) -> Result<WincodeArchiveV2NoRegistryMeta> {
    let err = meta
        .err
        .as_ref()
        .map(|err| CompactTransactionError::from_stored_wincode_bytes(&err.err))
        .transpose()?;
    let inner_instructions = if meta.inner_instructions_none {
        None
    } else {
        Some(
            meta.inner_instructions
                .iter()
                .map(|group| CompactInnerInstructions {
                    index: group.index,
                    instructions: group
                        .instructions
                        .iter()
                        .map(|instruction| CompactInnerInstruction {
                            program_id_index: instruction.program_id_index,
                            accounts: instruction.accounts.clone(),
                            data: instruction.data.clone(),
                            stack_height: instruction.stack_height,
                        })
                        .collect(),
                })
                .collect(),
        )
    };
    let logs = if meta.log_messages_none {
        None
    } else {
        Some(WincodeArchiveV2NoRegistryLogs::Raw(
            meta.log_messages.clone(),
        ))
    };
    let pre_token_balances = meta
        .pre_token_balances
        .iter()
        .map(no_registry_token_balance_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let post_token_balances = meta
        .post_token_balances
        .iter()
        .map(no_registry_token_balance_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let rewards = meta
        .rewards
        .iter()
        .map(no_registry_reward_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let loaded_writable_addresses = meta
        .loaded_writable_addresses
        .iter()
        .map(|address| bytes_to_pubkey(address, "loaded writable address"))
        .collect::<Result<Vec<_>>>()?;
    let loaded_readonly_addresses = meta
        .loaded_readonly_addresses
        .iter()
        .map(|address| bytes_to_pubkey(address, "loaded readonly address"))
        .collect::<Result<Vec<_>>>()?;
    let return_data = if meta.return_data_none {
        None
    } else {
        meta.return_data
            .as_ref()
            .map(
                |return_data| -> Result<WincodeArchiveV2NoRegistryReturnData> {
                    Ok(WincodeArchiveV2NoRegistryReturnData {
                        program_id: bytes_to_pubkey(
                            &return_data.program_id,
                            "return data program id",
                        )?,
                        data: return_data.data.clone(),
                    })
                },
            )
            .transpose()?
    };

    Ok(WincodeArchiveV2NoRegistryMeta {
        err,
        fee: meta.fee,
        pre_balances: meta.pre_balances.clone(),
        post_balances: meta.post_balances.clone(),
        inner_instructions,
        logs,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_writable_addresses,
        loaded_readonly_addresses,
        return_data,
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

fn block_rewards(
    pending: &PendingBlock,
    block: &RawBlockNode,
    key_index: &KeyIndex,
    zstd: &mut ZstdReusableDecoder,
    footer: &mut WincodeArchiveV2Footer,
    timings: &mut ArchiveV2Timings,
) -> Result<Option<WincodeArchiveV2Rewards>> {
    let Some(rewards_ref) = block.rewards.as_ref() else {
        return Ok(None);
    };
    let assemble_started = timings.detail_timer();
    let bytes = if let Some(cid) = rewards_ref.cid {
        let rewards = pending
            .rewards
            .as_ref()
            .filter(|rewards| rewards.rewards.cid == cid)
            .ok_or_else(|| anyhow!("slot {} missing rewards node {}", block.slot, cid))?;
        rewards.rewards.rewards_bytes(&pending.dataframes)?
    } else if let Some(inline) = rewards_ref.inline_raw_bytes() {
        inline.to_vec()
    } else {
        anyhow::bail!("slot {} unsupported rewards reference", block.slot);
    };
    timings.dataframe_assemble += assemble_started.elapsed();
    footer.rewards_source_bytes += bytes.len() as u64;

    let rewards_started = timings.detail_timer();
    let mut decoded = Rewards::default();
    match decode_rewards_from_frame(&bytes, &mut decoded, zstd).map(|()| decoded) {
        Ok(decoded) => {
            let num_partitions = decoded
                .num_partitions
                .as_ref()
                .map(|partitions| partitions.num_partitions);
            let mut out = Vec::with_capacity(decoded.rewards.len());
            for (reward_index, reward) in decoded.rewards.into_iter().enumerate() {
                out.push(compact_reward_from_proto(&reward, key_index).with_context(|| {
                    format!(
                        "slot {} reward#{reward_index} pubkey_len={} reward_type={} lamports={} post_balance={}",
                        block.slot,
                        reward.pubkey.len(),
                        reward.reward_type,
                        reward.lamports,
                        reward.post_balance
                    )
                })?);
            }
            let result = Ok(Some(WincodeArchiveV2Rewards {
                source_len: bytes.len() as u64,
                num_partitions,
                decoded: Some(out),
                raw_fallback: None,
                decode_error: None,
            }));
            timings.rewards_decode_compact += rewards_started.elapsed();
            result
        }
        Err(err) => {
            timings.rewards_decode_compact += rewards_started.elapsed();
            anyhow::bail!("slot {} rewards: {err}", block.slot);
        }
    }
}

fn block_rewards_no_registry(
    pending: &PendingBlock,
    block: &RawBlockNode,
    zstd: &mut ZstdReusableDecoder,
    footer: &mut WincodeArchiveV2Footer,
    timings: &mut ArchiveV2Timings,
) -> Result<Option<WincodeArchiveV2NoRegistryRewards>> {
    let Some(rewards_ref) = block.rewards.as_ref() else {
        return Ok(None);
    };
    let assemble_started = timings.detail_timer();
    let bytes = if let Some(cid) = rewards_ref.cid {
        let rewards = pending
            .rewards
            .as_ref()
            .filter(|rewards| rewards.rewards.cid == cid)
            .ok_or_else(|| anyhow!("slot {} missing rewards node {}", block.slot, cid))?;
        rewards.rewards.rewards_bytes(&pending.dataframes)?
    } else if let Some(inline) = rewards_ref.inline_raw_bytes() {
        inline.to_vec()
    } else {
        anyhow::bail!("slot {} unsupported rewards reference", block.slot);
    };
    timings.dataframe_assemble += assemble_started.elapsed();
    footer.rewards_source_bytes += bytes.len() as u64;

    let rewards_started = timings.detail_timer();
    let mut decoded = Rewards::default();
    match decode_rewards_from_frame(&bytes, &mut decoded, zstd).map(|()| decoded) {
        Ok(decoded) => {
            let num_partitions = decoded
                .num_partitions
                .as_ref()
                .map(|partitions| partitions.num_partitions);
            let mut out = Vec::with_capacity(decoded.rewards.len());
            for (reward_index, reward) in decoded.rewards.into_iter().enumerate() {
                out.push(no_registry_reward_from_proto(&reward).with_context(|| {
                    format!(
                        "slot {} reward#{reward_index} pubkey_len={} reward_type={} lamports={} post_balance={}",
                        block.slot,
                        reward.pubkey.len(),
                        reward.reward_type,
                        reward.lamports,
                        reward.post_balance
                    )
                })?);
            }
            let result = Ok(Some(WincodeArchiveV2NoRegistryRewards {
                source_len: bytes.len() as u64,
                num_partitions,
                decoded: Some(out),
                raw_fallback: None,
                decode_error: None,
            }));
            timings.rewards_decode_compact += rewards_started.elapsed();
            result
        }
        Err(err) => {
            timings.rewards_decode_compact += rewards_started.elapsed();
            anyhow::bail!("slot {} rewards: {err}", block.slot);
        }
    }
}

fn compact_reward_from_proto(
    reward: &of_car_reader::confirmed_block::Reward,
    key_index: &KeyIndex,
) -> Result<CompactReward> {
    let pubkey = Pubkey::from_str(&reward.pubkey)
        .with_context(|| format!("parse reward pubkey {}", reward.pubkey))?
        .to_bytes();
    Ok(CompactReward {
        pubkey: key_index.compact(&pubkey),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: reward.commission.parse::<u8>().ok(),
    })
}

fn no_registry_token_balance_from_proto(
    tb: &of_car_reader::confirmed_block::TokenBalance,
) -> Result<WincodeArchiveV2NoRegistryTokenBalance> {
    let (amount, decimals) = match &tb.ui_token_amount {
        None => (0u64, 0u8),
        Some(uta) => {
            let amount = uta
                .amount
                .parse::<u64>()
                .context("parse token amount u64")?;
            (amount, uta.decimals as u8)
        }
    };

    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index: tb.account_index,
        mint: optional_pubkey(&tb.mint)?,
        owner: optional_pubkey(&tb.owner)?,
        program_id: optional_pubkey(&tb.program_id)?,
        amount,
        decimals,
    })
}

fn no_registry_token_balance_from_visit(
    tb: TokenBalanceVisit<'_>,
) -> Result<WincodeArchiveV2NoRegistryTokenBalance> {
    let (amount, decimals) = match tb.ui_token_amount {
        None => (0u64, 0u8),
        Some(uta) => {
            let amount = uta
                .amount
                .parse::<u64>()
                .context("parse token amount u64")?;
            (amount, uta.decimals as u8)
        }
    };

    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index: tb.account_index,
        mint: optional_pubkey(tb.mint)?,
        owner: optional_pubkey(tb.owner)?,
        program_id: optional_pubkey(tb.program_id)?,
        amount,
        decimals,
    })
}

fn no_registry_reward_from_proto(
    reward: &of_car_reader::confirmed_block::Reward,
) -> Result<WincodeArchiveV2NoRegistryReward> {
    Ok(WincodeArchiveV2NoRegistryReward {
        pubkey: Pubkey::from_str(&reward.pubkey)
            .with_context(|| format!("parse reward pubkey {}", reward.pubkey))?
            .to_bytes(),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: reward.commission.parse::<u8>().ok(),
    })
}

fn optional_pubkey(value: &str) -> Result<Option<[u8; 32]>> {
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            Pubkey::from_str(value)
                .with_context(|| format!("parse pubkey {value}"))?
                .to_bytes(),
        ))
    }
}

fn bytes_to_pubkey(bytes: &[u8], label: &str) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .with_context(|| format!("{label} has invalid length {}", bytes.len()))
}

#[derive(Default)]
struct NoRegistryMetaVisitor {
    err: Option<CompactTransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Vec<CompactInnerInstructions>,
    inner_instructions_none: bool,
    log_messages: Vec<String>,
    log_messages_none: bool,
    pre_token_balances: Vec<WincodeArchiveV2NoRegistryTokenBalance>,
    post_token_balances: Vec<WincodeArchiveV2NoRegistryTokenBalance>,
    rewards: Vec<WincodeArchiveV2NoRegistryReward>,
    loaded_writable_addresses: Vec<[u8; 32]>,
    loaded_readonly_addresses: Vec<[u8; 32]>,
    return_data: Option<WincodeArchiveV2NoRegistryReturnData>,
    return_data_none: bool,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
    error: Option<anyhow::Error>,
}

impl NoRegistryMetaVisitor {
    fn record_error(&mut self, err: anyhow::Error) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }

    fn finish(self) -> Result<WincodeArchiveV2NoRegistryMeta> {
        if let Some(err) = self.error {
            return Err(err);
        }

        let inner_instructions = if self.inner_instructions_none {
            None
        } else {
            Some(self.inner_instructions)
        };
        let logs = if self.log_messages_none {
            None
        } else {
            Some(WincodeArchiveV2NoRegistryLogs::Raw(self.log_messages))
        };
        let return_data = if self.return_data_none {
            None
        } else {
            self.return_data
        };

        Ok(WincodeArchiveV2NoRegistryMeta {
            err: self.err,
            fee: self.fee,
            pre_balances: self.pre_balances,
            post_balances: self.post_balances,
            inner_instructions,
            logs,
            pre_token_balances: self.pre_token_balances,
            post_token_balances: self.post_token_balances,
            rewards: self.rewards,
            loaded_writable_addresses: self.loaded_writable_addresses,
            loaded_readonly_addresses: self.loaded_readonly_addresses,
            return_data,
            compute_units_consumed: self.compute_units_consumed,
            cost_units: self.cost_units,
        })
    }
}

impl<'a> TransactionStatusMetaVisitor<'a> for NoRegistryMetaVisitor {
    #[inline]
    fn wants_status_error(&self) -> bool {
        true
    }

    #[inline]
    fn wants_pre_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_inner_instructions(&self) -> bool {
        true
    }

    #[inline]
    fn wants_log_messages(&self) -> bool {
        true
    }

    #[inline]
    fn wants_pre_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_rewards(&self) -> bool {
        true
    }

    #[inline]
    fn wants_loaded_addresses(&self) -> bool {
        true
    }

    #[inline]
    fn wants_return_data(&self) -> bool {
        true
    }

    #[inline]
    fn status_error(&mut self, err: &'a [u8]) {
        match CompactTransactionError::from_stored_wincode_bytes(err) {
            Ok(err) => self.err = Some(err),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn fee(&mut self, fee: u64) {
        self.fee = fee;
    }

    #[inline]
    fn pre_balance(&mut self, _index: usize, lamports: u64) {
        self.pre_balances.push(lamports);
    }

    #[inline]
    fn post_balance(&mut self, _index: usize, lamports: u64) {
        self.post_balances.push(lamports);
    }

    #[inline]
    fn inner_instruction(&mut self, instruction: InnerInstructionVisit<'a>) {
        if self
            .inner_instructions
            .last()
            .is_none_or(|group| group.index != instruction.outer_instruction_index)
        {
            self.inner_instructions.push(CompactInnerInstructions {
                index: instruction.outer_instruction_index,
                instructions: Vec::new(),
            });
        }

        let Some(group) = self.inner_instructions.last_mut() else {
            return;
        };
        group.instructions.push(CompactInnerInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts.to_vec(),
            data: instruction.data.to_vec(),
            stack_height: instruction.stack_height,
        });
    }

    #[inline]
    fn inner_instructions_none(&mut self, none: bool) {
        self.inner_instructions_none = none;
    }

    #[inline]
    fn log_message(&mut self, message: &'a str) {
        self.log_messages.push(message.to_owned());
    }

    #[inline]
    fn log_messages_none(&mut self, none: bool) {
        self.log_messages_none = none;
    }

    #[inline]
    fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        match no_registry_token_balance_from_visit(balance) {
            Ok(balance) => self.pre_token_balances.push(balance),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn post_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        match no_registry_token_balance_from_visit(balance) {
            Ok(balance) => self.post_token_balances.push(balance),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn reward_raw(&mut self, bytes: &'a [u8]) {
        match of_car_reader::confirmed_block::Reward::decode(bytes)
            .map_err(anyhow::Error::from)
            .and_then(|reward| no_registry_reward_from_proto(&reward))
        {
            Ok(reward) => self.rewards.push(reward),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn loaded_writable_address(&mut self, address: &'a [u8]) {
        match bytes_to_pubkey(address, "loaded writable address") {
            Ok(address) => self.loaded_writable_addresses.push(address),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn loaded_readonly_address(&mut self, address: &'a [u8]) {
        match bytes_to_pubkey(address, "loaded readonly address") {
            Ok(address) => self.loaded_readonly_addresses.push(address),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn return_data(&mut self, return_data: ReturnDataVisit<'a>) {
        match bytes_to_pubkey(return_data.program_id, "return data program id") {
            Ok(program_id) => {
                self.return_data = Some(WincodeArchiveV2NoRegistryReturnData {
                    program_id,
                    data: return_data.data.to_vec(),
                });
            }
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn return_data_none(&mut self, none: bool) {
        self.return_data_none = none;
    }

    #[inline]
    fn compute_units_consumed(&mut self, units: u64) {
        self.compute_units_consumed = Some(units);
    }

    #[inline]
    fn cost_units(&mut self, units: u64) {
        self.cost_units = Some(units);
    }
}

fn ordered_entries<'a>(
    pending: &'a PendingBlock,
    block: &RawBlockNode,
) -> Result<&'a [PendingEntry]> {
    anyhow::ensure!(
        pending.entries.len() == block.entries.len(),
        "slot {} entry count mismatch: stream={} block_refs={}",
        block.slot,
        pending.entries.len(),
        block.entries.len()
    );
    for (index, (entry_ref, pending_entry)) in
        block.entries.iter().zip(pending.entries.iter()).enumerate()
    {
        let cid = entry_ref
            .cid
            .ok_or_else(|| anyhow!("slot {} inline entry ref unsupported", block.slot))?;
        anyhow::ensure!(
            pending_entry.entry.cid == cid,
            "slot {} entry order mismatch at entry #{}",
            block.slot,
            index
        );
    }
    Ok(&pending.entries)
}

fn validate_ordered_transactions(
    pending: &PendingBlock,
    entries: &[PendingEntry],
    slot: u64,
) -> Result<()> {
    let mut tx_index = 0usize;
    for (entry_index, entry) in entries.iter().enumerate() {
        for tx_ref in &entry.entry.transactions {
            let cid = tx_ref
                .cid
                .ok_or_else(|| anyhow!("slot {slot} inline transaction ref unsupported"))?;
            let pending_tx = pending.transactions.get(tx_index).ok_or_else(|| {
                anyhow!(
                    "slot {slot} missing transaction at stream tx #{} referenced by entry #{}",
                    tx_index,
                    entry_index
                )
            })?;
            anyhow::ensure!(
                pending_tx.tx.cid == cid,
                "slot {slot} transaction order mismatch at tx #{} entry #{}",
                tx_index,
                entry_index
            );
            tx_index += 1;
        }
    }
    anyhow::ensure!(
        tx_index == pending.transactions.len(),
        "slot {slot} transaction count mismatch: stream={} entry_refs={}",
        pending.transactions.len(),
        tx_index
    );
    Ok(())
}

pub(crate) fn inspect_car_order(input: &Path, max_blocks: Option<u64>) -> Result<()> {
    let mut scanner = RawCarScanner::open(input)?;
    scanner.skip_header()?;

    let mut pending_txs = Vec::<Cid36>::new();
    let mut pending_entries = Vec::<PendingEntryOrder>::new();
    let mut pending_rewards = Vec::<Cid36>::new();
    let mut pending_dataframes = 0u64;
    let mut stats = CarOrderStats::default();
    let started = Instant::now();

    while let Some(raw) = scanner.next_node_timed(None)? {
        stats.car_entries += 1;
        stats.car_payload_bytes += raw.payload_len as u64;
        match raw.node {
            RawNode::Transaction(tx) => {
                stats.transactions += 1;
                stats.tx_data_continuation_refs += tx.data.next.len() as u64;
                stats.tx_metadata_continuation_refs += tx.metadata.next.len() as u64;
                pending_txs.push(tx.cid);
            }
            RawNode::Entry(entry) => {
                stats.entries += 1;
                let mut tx_cids = Vec::with_capacity(entry.transactions.len());
                for tx_ref in &entry.transactions {
                    let cid = tx_ref
                        .cid
                        .ok_or_else(|| anyhow!("inline transaction ref unsupported"))?;
                    tx_cids.push(cid);
                }
                pending_entries.push(PendingEntryOrder {
                    cid: entry.cid,
                    tx_cids,
                });
            }
            RawNode::Rewards(rewards) => {
                stats.rewards += 1;
                stats.rewards_continuation_refs += rewards.data.next.len() as u64;
                pending_rewards.push(rewards.cid);
            }
            RawNode::DataFrame(frame) => {
                stats.dataframes += 1;
                stats.dataframe_continuation_refs += frame.frame.next.len() as u64;
                pending_dataframes += 1;
            }
            RawNode::Block(block) => {
                stats.blocks += 1;
                stats.max_pending_txs = stats.max_pending_txs.max(pending_txs.len() as u64);
                stats.max_pending_entries =
                    stats.max_pending_entries.max(pending_entries.len() as u64);
                stats.max_pending_rewards =
                    stats.max_pending_rewards.max(pending_rewards.len() as u64);
                stats.max_pending_dataframes = stats.max_pending_dataframes.max(pending_dataframes);

                let mut block_entry_cids = Vec::with_capacity(block.entries.len());
                for entry_ref in &block.entries {
                    let cid = entry_ref.cid.ok_or_else(|| {
                        anyhow!("slot {} inline entry ref unsupported", block.slot)
                    })?;
                    block_entry_cids.push(cid);
                }

                if block_entry_cids
                    .iter()
                    .copied()
                    .eq(pending_entries.iter().map(|entry| entry.cid))
                {
                    stats.entry_order_matches += 1;
                } else {
                    stats.entry_order_mismatches += 1;
                    if stats.first_entry_order_mismatch_slot.is_none() {
                        stats.first_entry_order_mismatch_slot = Some(block.slot);
                    }
                }

                let mut entry_by_cid = GxHashMap::with_capacity_and_hasher(
                    pending_entries.len(),
                    GxBuildHasher::default(),
                );
                for entry in &pending_entries {
                    entry_by_cid.insert(entry.cid, entry);
                }
                let expected_tx_count = pending_entries
                    .iter()
                    .map(|entry| entry.tx_cids.len())
                    .sum::<usize>();
                let mut block_order_txs = Vec::with_capacity(expected_tx_count);
                let mut missing_entry = false;
                for entry_cid in &block_entry_cids {
                    let Some(entry) = entry_by_cid.get(entry_cid) else {
                        missing_entry = true;
                        continue;
                    };
                    block_order_txs.extend(entry.tx_cids.iter().copied());
                }
                if !missing_entry && block_order_txs == pending_txs {
                    stats.tx_order_matches += 1;
                } else {
                    stats.tx_order_mismatches += 1;
                    if stats.first_tx_order_mismatch_slot.is_none() {
                        stats.first_tx_order_mismatch_slot = Some(block.slot);
                    }
                }

                match block.rewards.as_ref().and_then(|rewards| rewards.cid) {
                    Some(rewards_cid) if pending_rewards.as_slice() == [rewards_cid] => {
                        stats.rewards_order_matches += 1;
                    }
                    Some(_) => {
                        stats.rewards_order_mismatches += 1;
                        if stats.first_rewards_order_mismatch_slot.is_none() {
                            stats.first_rewards_order_mismatch_slot = Some(block.slot);
                        }
                    }
                    None if pending_rewards.is_empty() => {
                        stats.rewards_order_matches += 1;
                    }
                    None => {
                        stats.rewards_order_mismatches += 1;
                        if stats.first_rewards_order_mismatch_slot.is_none() {
                            stats.first_rewards_order_mismatch_slot = Some(block.slot);
                        }
                    }
                }

                pending_txs.clear();
                pending_entries.clear();
                pending_rewards.clear();
                pending_dataframes = 0;

                if max_blocks.is_some_and(|limit| stats.blocks >= limit) {
                    break;
                }
            }
            RawNode::Subset(_) => stats.subsets += 1,
            RawNode::Epoch(_) => stats.epochs += 1,
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    info!(
        "CAR order inspection complete in {:.2}s: blocks={} car_entries={} payload_bytes={} txs={} entries={} rewards={} dataframes={} subsets={} epochs={}",
        elapsed,
        stats.blocks,
        stats.car_entries,
        stats.car_payload_bytes,
        stats.transactions,
        stats.entries,
        stats.rewards,
        stats.dataframes,
        stats.subsets,
        stats.epochs,
    );
    info!(
        "CAR order matches: entries={}/{} mismatches={} first_entry_mismatch_slot={:?} txs={}/{} mismatches={} first_tx_mismatch_slot={:?} rewards={}/{} mismatches={} first_rewards_mismatch_slot={:?}",
        stats.entry_order_matches,
        stats.blocks,
        stats.entry_order_mismatches,
        stats.first_entry_order_mismatch_slot,
        stats.tx_order_matches,
        stats.blocks,
        stats.tx_order_mismatches,
        stats.first_tx_order_mismatch_slot,
        stats.rewards_order_matches,
        stats.blocks,
        stats.rewards_order_mismatches,
        stats.first_rewards_order_mismatch_slot,
    );
    info!(
        "CAR order pending maxima: txs={} entries={} rewards={} dataframes={} continuation_refs tx_data={} tx_metadata={} rewards={} dataframe={}",
        stats.max_pending_txs,
        stats.max_pending_entries,
        stats.max_pending_rewards,
        stats.max_pending_dataframes,
        stats.tx_data_continuation_refs,
        stats.tx_metadata_continuation_refs,
        stats.rewards_continuation_refs,
        stats.dataframe_continuation_refs,
    );

    Ok(())
}

pub(crate) fn find_poh_gaps(input: &Path, output: Option<&Path>) -> Result<()> {
    let mut scanner = RawCarScanner::open(input)?;
    scanner.skip_header()?;

    let mut writer: Box<dyn Write> = if let Some(path) = output {
        Box::new(BufWriter::with_capacity(
            BUFFER_SIZE,
            File::create(path).with_context(|| format!("create {}", path.display()))?,
        ))
    } else {
        Box::new(BufWriter::with_capacity(BUFFER_SIZE, std::io::stdout()))
    };
    writeln!(
        writer,
        "slot\tblock_index\ttx_count\tentry_count\tprevious_blockhash"
    )?;

    let mut pending_txs = 0u64;
    let mut pending_entries = Vec::<[u8; 32]>::new();
    let mut last_blockhash: Option<[u8; 32]> = None;
    let mut blocks = 0u64;
    let mut gaps = 0u64;
    let mut tx_gaps = 0u64;
    let started = Instant::now();

    while let Some(raw) = scanner.next_node_timed(None)? {
        match raw.node {
            RawNode::Transaction(_) => pending_txs += 1,
            RawNode::Entry(entry) => pending_entries.push(entry.hash),
            RawNode::Block(block) => {
                if block.entries.is_empty() {
                    gaps += 1;
                    if pending_txs > 0 {
                        tx_gaps += 1;
                    }
                    let previous_blockhash = last_blockhash
                        .map(|hash| Pubkey::new_from_array(hash).to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    writeln!(
                        writer,
                        "{}\t{}\t{}\t{}\t{}",
                        block.slot,
                        blocks,
                        pending_txs,
                        block.entries.len(),
                        previous_blockhash
                    )?;
                } else if let Some(blockhash) = pending_entries.last().copied() {
                    last_blockhash = Some(blockhash);
                } else {
                    warn!(
                        "slot {} block has {} entry refs but no pending entry nodes while scanning PoH gaps",
                        block.slot,
                        block.entries.len()
                    );
                }

                pending_txs = 0;
                pending_entries.clear();
                blocks += 1;
            }
            RawNode::Rewards(_)
            | RawNode::DataFrame(_)
            | RawNode::Subset(_)
            | RawNode::Epoch(_) => {}
        }
    }
    writer.flush()?;
    info!(
        "PoH gap scan complete in {:.2}s: blocks={} gaps={} gaps_with_txs={} input={}",
        started.elapsed().as_secs_f64(),
        blocks,
        gaps,
        tx_gaps,
        input.display()
    );
    Ok(())
}

struct PendingEntryOrder {
    cid: Cid36,
    tx_cids: Vec<Cid36>,
}

#[derive(Default)]
struct CarOrderStats {
    car_entries: u64,
    car_payload_bytes: u64,
    blocks: u64,
    entries: u64,
    transactions: u64,
    rewards: u64,
    dataframes: u64,
    subsets: u64,
    epochs: u64,
    entry_order_matches: u64,
    entry_order_mismatches: u64,
    tx_order_matches: u64,
    tx_order_mismatches: u64,
    rewards_order_matches: u64,
    rewards_order_mismatches: u64,
    first_entry_order_mismatch_slot: Option<u64>,
    first_tx_order_mismatch_slot: Option<u64>,
    first_rewards_order_mismatch_slot: Option<u64>,
    max_pending_txs: u64,
    max_pending_entries: u64,
    max_pending_rewards: u64,
    max_pending_dataframes: u64,
    tx_data_continuation_refs: u64,
    tx_metadata_continuation_refs: u64,
    rewards_continuation_refs: u64,
    dataframe_continuation_refs: u64,
}

#[derive(Default)]
struct BlockRecordScratch {
    tx_bytes: Vec<u8>,
    metadata_bytes: Vec<u8>,
    reassemble_visited: HashSet<Cid36>,
}

/// Return the frame payload in place when it has no continuations. The CAR
/// scanner already owns these bytes for the lifetime of the pending block, so
/// copying them into a second reusable `Vec` does no useful work. Frames with
/// any continuation retain the exact recursive reassembly behavior.
#[inline]
fn dataframe_bytes_for_decode<'a>(
    frame: &'a RawDataFrame,
    dataframes: &HashMap<Cid36, StandaloneDataFrame>,
    scratch: &'a mut Vec<u8>,
    visited: &mut HashSet<Cid36>,
) -> Result<(&'a [u8], usize)> {
    if frame.next.is_empty() {
        scratch.clear();
        visited.clear();
        return Ok((&frame.data, scratch.capacity()));
    }

    frame.reassemble_bytes_into(dataframes, scratch, visited)?;
    let scratch_capacity = scratch.capacity();
    Ok((scratch.as_slice(), scratch_capacity))
}

#[derive(Clone, Copy, Debug, Default)]
struct RawDataFramePayloadPoolStats {
    retained_buffers: usize,
    retained_capacity: usize,
    current_buffers: usize,
    current_capacity: usize,
    peak_current_buffers: usize,
    peak_current_capacity: usize,
    takes: u64,
    reused_buffers: u64,
    fresh_buffers: u64,
    allocation_events: u64,
    growth_events: u64,
    discarded_buffers: u64,
    discarded_capacity: u64,
}

#[derive(Clone, Copy)]
struct RawDataFramePayloadPoolLimits {
    max_retained_capacity: usize,
    max_buffer_capacity: usize,
    max_buffers_per_class: usize,
    max_class_zero_buffers: usize,
}

const RAW_DATAFRAME_PAYLOAD_POOL_LIMITS: RawDataFramePayloadPoolLimits =
    RawDataFramePayloadPoolLimits {
        max_retained_capacity: RAW_DATAFRAME_POOL_MAX_RETAINED_BYTES,
        max_buffer_capacity: RAW_DATAFRAME_POOL_MAX_BUFFER_BYTES,
        max_buffers_per_class: RAW_DATAFRAME_POOL_MAX_BUFFERS_PER_CLASS,
        max_class_zero_buffers: RAW_DATAFRAME_POOL_MAX_CLASS_ZERO_BUFFERS,
    };

struct RawDataFramePayloadPool {
    // Class zero is reserved for zero-capacity buffers. Positive class N owns
    // buffers with capacity in [2^(N-1), 2^N), while requests are rounded up
    // to a power-of-two lower bound. Every buffer popped from the requested
    // class (or a larger one) is therefore guaranteed to fit.
    free_by_capacity: Vec<Vec<Vec<u8>>>,
    stats: RawDataFramePayloadPoolStats,
}

impl Default for RawDataFramePayloadPool {
    fn default() -> Self {
        Self {
            free_by_capacity: (0..=usize::BITS + 1).map(|_| Vec::new()).collect(),
            stats: RawDataFramePayloadPoolStats::default(),
        }
    }
}

impl RawDataFramePayloadPool {
    fn take(&mut self, required: usize) -> Vec<u8> {
        self.stats.takes += 1;
        let class = raw_dataframe_required_capacity_class(required);
        let last_probe = class
            .saturating_add(RAW_DATAFRAME_POOL_LARGER_CLASS_PROBES)
            .min(self.free_by_capacity.len() - 1);
        let mut reusable_class = None;
        for candidate_class in class..=last_probe {
            let Some(buffer) = self.free_by_capacity[candidate_class].last() else {
                continue;
            };
            debug_assert!(
                buffer.capacity() >= required,
                "payload-pool class {candidate_class} capacity {} cannot satisfy {required}",
                buffer.capacity(),
            );
            reusable_class = Some(candidate_class);
            break;
        }

        let mut buffer = if let Some(reusable_class) = reusable_class {
            let buffer = self.free_by_capacity[reusable_class]
                .pop()
                .expect("RawDataFrame payload pool selected an empty capacity class");
            self.stats.reused_buffers += 1;
            debug_assert!(self.stats.retained_buffers >= 1);
            self.stats.retained_buffers = self
                .stats
                .retained_buffers
                .checked_sub(1)
                .expect("RawDataFrame payload pool retained-buffer accounting underflow");
            debug_assert!(self.stats.retained_capacity >= buffer.capacity());
            self.stats.retained_capacity = self
                .stats
                .retained_capacity
                .checked_sub(buffer.capacity())
                .expect("RawDataFrame payload pool retained-capacity accounting underflow");
            buffer
        } else {
            self.stats.fresh_buffers += 1;
            let allocation_capacity = if required > RAW_DATAFRAME_POOL_MAX_BUFFER_BYTES {
                required
            } else {
                raw_dataframe_class_allocation_capacity(class, required)
            };
            if allocation_capacity != 0 {
                self.stats.allocation_events += 1;
            }
            Vec::with_capacity(allocation_capacity)
        };

        buffer.clear();
        if buffer.capacity() < required {
            let target = if required > RAW_DATAFRAME_POOL_MAX_BUFFER_BYTES {
                required
            } else {
                raw_dataframe_class_allocation_capacity(class, required)
            };
            buffer.reserve(target);
            self.stats.allocation_events += 1;
            self.stats.growth_events += 1;
        }
        self.stats.current_buffers = self
            .stats
            .current_buffers
            .checked_add(1)
            .expect("RawDataFrame payload pool current-buffer accounting overflow");
        self.stats.current_capacity = self
            .stats
            .current_capacity
            .checked_add(buffer.capacity())
            .expect("RawDataFrame payload pool current-capacity accounting overflow");
        self.stats.peak_current_buffers = self
            .stats
            .peak_current_buffers
            .max(self.stats.current_buffers);
        self.stats.peak_current_capacity = self
            .stats
            .peak_current_capacity
            .max(self.stats.current_capacity);
        buffer
    }

    fn recycle(&mut self, buffer: Vec<u8>) {
        self.recycle_with_limits(buffer, RAW_DATAFRAME_PAYLOAD_POOL_LIMITS);
    }

    fn recycle_with_limits(&mut self, mut buffer: Vec<u8>, limits: RawDataFramePayloadPoolLimits) {
        buffer.clear();
        let capacity = buffer.capacity();
        debug_assert!(self.stats.current_buffers >= 1);
        self.stats.current_buffers = self
            .stats
            .current_buffers
            .checked_sub(1)
            .expect("RawDataFrame payload pool current-buffer accounting underflow");
        debug_assert!(self.stats.current_capacity >= capacity);
        self.stats.current_capacity = self
            .stats
            .current_capacity
            .checked_sub(capacity)
            .expect("RawDataFrame payload pool current-capacity accounting underflow");

        let class = raw_dataframe_recycled_capacity_class(capacity);
        let class_limit = if class == 0 {
            limits.max_class_zero_buffers
        } else {
            limits.max_buffers_per_class
        };
        let retained_capacity = self.stats.retained_capacity.checked_add(capacity);
        let retain = capacity <= limits.max_buffer_capacity
            && self.free_by_capacity[class].len() < class_limit
            && retained_capacity.is_some_and(|total| total <= limits.max_retained_capacity);
        if !retain {
            self.stats.discarded_buffers = self
                .stats
                .discarded_buffers
                .checked_add(1)
                .expect("RawDataFrame payload pool discarded-buffer accounting overflow");
            self.stats.discarded_capacity = self
                .stats
                .discarded_capacity
                .checked_add(capacity as u64)
                .expect("RawDataFrame payload pool discarded-capacity accounting overflow");
            return;
        }

        self.stats.retained_buffers = self
            .stats
            .retained_buffers
            .checked_add(1)
            .expect("RawDataFrame payload pool retained-buffer accounting overflow");
        self.stats.retained_capacity = retained_capacity
            .expect("RawDataFrame payload pool retained-capacity accounting overflow");
        self.free_by_capacity[class].push(buffer);
    }

    fn stats(&self) -> RawDataFramePayloadPoolStats {
        self.stats
    }
}

#[inline]
fn raw_dataframe_required_capacity_class(required: usize) -> usize {
    if required == 0 {
        0
    } else {
        1 + (usize::BITS - (required - 1).leading_zeros()) as usize
    }
}

#[inline]
fn raw_dataframe_recycled_capacity_class(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        1 + (usize::BITS - 1 - capacity.leading_zeros()) as usize
    }
}

#[inline]
fn raw_dataframe_class_allocation_capacity(class: usize, required: usize) -> usize {
    if class == 0 {
        0
    } else {
        1usize.checked_shl((class - 1) as u32).unwrap_or(required)
    }
}

#[derive(Default)]
struct PendingBlock {
    transactions: Vec<PendingTx>,
    entries: Vec<PendingEntry>,
    rewards: Option<PendingRewards>,
    dataframes: HashMap<Cid36, StandaloneDataFrame>,
    last_slot: u64,
    record_scratch: BlockRecordScratch,
    raw_dataframe_payloads: RawDataFramePayloadPool,
}

impl PendingBlock {
    fn clear(&mut self) {
        self.transactions.clear();
        self.entries.clear();
        self.rewards = None;
        self.dataframes.clear();
    }

    fn pending_slot_hint(&self) -> Option<u64> {
        self.transactions
            .last()
            .map(|pending| pending.tx.slot)
            .or_else(|| self.rewards.as_ref().map(|pending| pending.rewards.slot))
    }

    fn insert_dataframe_recycling(&mut self, frame: StandaloneDataFrame) -> Result<()> {
        let slot_hint = self.pending_slot_hint();
        let cid = frame.cid;
        let location = frame.location;
        match self.dataframes.entry(cid) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(frame);
                Ok(())
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                self.raw_dataframe_payloads.recycle(frame.frame.data);
                let slot = slot_hint
                    .map(|slot| slot.to_string())
                    .unwrap_or_else(|| "unknown".to_owned());
                anyhow::bail!(
                    "duplicate standalone dataframe CID {cid} while collecting slot {slot} at CAR entry {} offset {}",
                    location.entry_index,
                    location.car_offset,
                )
            }
        }
    }

    fn clear_recycling_frame_data(&mut self) {
        for pending_tx in self.transactions.drain(..) {
            let RawTransactionNode { data, metadata, .. } = pending_tx.tx;
            self.raw_dataframe_payloads.recycle(data.data);
            self.raw_dataframe_payloads.recycle(metadata.data);
        }
        self.entries.clear();
        if let Some(pending_rewards) = self.rewards.take() {
            self.raw_dataframe_payloads
                .recycle(pending_rewards.rewards.data.data);
        }
        for (_, frame) in self.dataframes.drain() {
            self.raw_dataframe_payloads.recycle(frame.frame.data);
        }
    }
}

struct PendingTx {
    tx: RawTransactionNode,
    #[allow(dead_code)]
    payload_len: usize,
}

struct PendingEntry {
    entry: RawEntryNode,
    #[allow(dead_code)]
    payload_len: usize,
}

struct PendingRewards {
    rewards: RawRewardsNode,
    #[allow(dead_code)]
    payload_len: usize,
}

const FIRST_SEEN_SIGNATURE_BYTES: usize = 64;

#[derive(Clone, Copy, Debug, Default)]
struct FirstSeenSignatureArenaStats {
    blocks: u64,
    transactions: u64,
    signatures: u64,
    decoder_signature_ref_vec_allocations_avoided: u64,
    owned_signature_outer_vec_allocations_avoided: u64,
    owned_signature_inner_vec_allocations_avoided: u64,
    block_write_calls: u64,
    peak_counts_capacity: usize,
    peak_bytes_capacity: usize,
}

#[derive(Default)]
struct FirstSeenBlockSignatures {
    counts: Vec<u8>,
    bytes: Vec<u8>,
    visited: HashSet<Cid36>,
    stats: FirstSeenSignatureArenaStats,
}

impl FirstSeenBlockSignatures {
    fn collect_block(
        &mut self,
        transactions: &[PendingTx],
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
        slot: u64,
    ) -> Result<()> {
        self.counts.clear();
        self.bytes.clear();
        reserve_total_capacity(&mut self.counts, transactions.len());
        self.bytes.reserve(
            transactions
                .len()
                .saturating_mul(FIRST_SEEN_SIGNATURE_BYTES)
                .saturating_sub(self.bytes.len()),
        );

        let mut signatures = 0u64;
        let mut nonempty_transactions = 0u64;
        for (tx_index, pending_tx) in transactions.iter().enumerate() {
            self.visited.clear();
            let output_start = self.bytes.len();
            let count = collect_first_seen_transaction_signatures(
                &pending_tx.tx.data,
                dataframes,
                &mut self.visited,
                &mut self.bytes,
            )
            .with_context(|| format!("slot {slot} tx#{tx_index} collect signatures"))?;
            let expected_len = usize::from(count)
                .checked_mul(FIRST_SEEN_SIGNATURE_BYTES)
                .context("first-seen transaction signature byte length overflow")?;
            anyhow::ensure!(
                self.bytes.len().saturating_sub(output_start) == expected_len,
                "slot {slot} tx#{tx_index} collected {} signature bytes, expected {expected_len}",
                self.bytes.len().saturating_sub(output_start),
            );
            self.counts.push(count);
            signatures = signatures
                .checked_add(u64::from(count))
                .context("first-seen signature count overflow")?;
            nonempty_transactions += u64::from(count != 0);
        }

        self.stats.blocks = self.stats.blocks.saturating_add(1);
        self.stats.transactions = self
            .stats
            .transactions
            .saturating_add(transactions.len() as u64);
        self.stats.signatures = self.stats.signatures.saturating_add(signatures);
        self.stats.decoder_signature_ref_vec_allocations_avoided = self
            .stats
            .decoder_signature_ref_vec_allocations_avoided
            .saturating_add(nonempty_transactions);
        self.stats.owned_signature_outer_vec_allocations_avoided = self
            .stats
            .owned_signature_outer_vec_allocations_avoided
            .saturating_add(nonempty_transactions);
        self.stats.owned_signature_inner_vec_allocations_avoided = self
            .stats
            .owned_signature_inner_vec_allocations_avoided
            .saturating_add(signatures);
        self.stats.peak_counts_capacity =
            self.stats.peak_counts_capacity.max(self.counts.capacity());
        self.stats.peak_bytes_capacity = self.stats.peak_bytes_capacity.max(self.bytes.capacity());
        Ok(())
    }

    fn record_block_write(&mut self) {
        if !self.bytes.is_empty() {
            self.stats.block_write_calls = self.stats.block_write_calls.saturating_add(1);
        }
    }
}

struct FirstSeenSignatureCollector<'a> {
    output: &'a mut Vec<u8>,
    prefix: [u8; 3],
    prefix_len: usize,
    count: Option<u8>,
    remaining_signature_bytes: usize,
}

impl<'a> FirstSeenSignatureCollector<'a> {
    fn new(output: &'a mut Vec<u8>) -> Self {
        Self {
            output,
            prefix: [0; 3],
            prefix_len: 0,
            count: None,
            remaining_signature_bytes: 0,
        }
    }

    fn consume(&mut self, chunk: &[u8]) -> Result<bool> {
        let mut offset = 0usize;
        while self.count.is_none() && offset < chunk.len() {
            anyhow::ensure!(
                self.prefix_len < self.prefix.len(),
                "transaction signature ShortU16 prefix exceeds three bytes"
            );
            let byte = chunk[offset];
            self.prefix[self.prefix_len] = byte;
            self.prefix_len += 1;
            offset += 1;
            if byte & 0x80 == 0 || self.prefix_len == self.prefix.len() {
                let (count, encoded_len) =
                    solana_short_vec::decode_shortu16_len(&self.prefix[..self.prefix_len])
                        .map_err(|()| anyhow!("invalid transaction signature ShortU16 prefix"))?;
                anyhow::ensure!(
                    encoded_len == self.prefix_len,
                    "transaction signature ShortU16 consumed {encoded_len} bytes, collected {}",
                    self.prefix_len,
                );
                let count = u8::try_from(count)
                    .context("transaction signature count exceeds hot-row u8 maximum")?;
                self.remaining_signature_bytes = usize::from(count)
                    .checked_mul(FIRST_SEEN_SIGNATURE_BYTES)
                    .context("transaction signature byte length overflow")?;
                self.count = Some(count);
            }
        }

        if self.count.is_some() && self.remaining_signature_bytes != 0 && offset < chunk.len() {
            let copied = self
                .remaining_signature_bytes
                .min(chunk.len().saturating_sub(offset));
            self.output
                .extend_from_slice(&chunk[offset..offset + copied]);
            self.remaining_signature_bytes -= copied;
        }
        Ok(self.count.is_some() && self.remaining_signature_bytes == 0)
    }

    fn finish(self) -> Result<u8> {
        let count = self
            .count
            .context("truncated transaction signature ShortU16 prefix")?;
        anyhow::ensure!(
            self.remaining_signature_bytes == 0,
            "truncated transaction signatures: {} bytes missing",
            self.remaining_signature_bytes,
        );
        Ok(count)
    }
}

fn collect_first_seen_transaction_signatures(
    frame: &RawDataFrame,
    dataframes: &HashMap<Cid36, StandaloneDataFrame>,
    visited: &mut HashSet<Cid36>,
    output: &mut Vec<u8>,
) -> Result<u8> {
    let output_start = output.len();
    let mut collector = FirstSeenSignatureCollector::new(output);
    let collected = visit_first_seen_dataframe_chunks(frame, dataframes, visited, &mut |chunk| {
        collector.consume(chunk)
    });
    match collected.and_then(|_| collector.finish()) {
        Ok(count) => Ok(count),
        Err(error) => {
            output.truncate(output_start);
            Err(error)
        }
    }
}

fn visit_first_seen_dataframe_chunks(
    frame: &RawDataFrame,
    dataframes: &HashMap<Cid36, StandaloneDataFrame>,
    visited: &mut HashSet<Cid36>,
    visitor: &mut impl FnMut(&[u8]) -> Result<bool>,
) -> Result<bool> {
    if visitor(&frame.data)? {
        return Ok(true);
    }
    for next in &frame.next {
        if let Some(inline) = next.inline_raw_bytes() {
            if visitor(inline)? {
                return Ok(true);
            }
            continue;
        }

        let cid = next.cid.with_context(|| {
            format!(
                "unsupported dataframe CID reference with {} normalized bytes",
                next.normalized_bytes().len()
            )
        })?;
        anyhow::ensure!(visited.insert(cid), "dataframe cycle at {cid}");
        let continuation = dataframes
            .get(&cid)
            .with_context(|| format!("missing dataframe {cid}"));
        let result = continuation.and_then(|continuation| {
            visit_first_seen_dataframe_chunks(&continuation.frame, dataframes, visited, visitor)
        });
        visited.remove(&cid);
        if result? {
            return Ok(true);
        }
    }
    Ok(false)
}

struct RawNodeWithLen {
    node: RawNode,
    payload_len: usize,
}

struct RawPrefix<'a> {
    cid: Cid36,
    prefix: &'a [u8],
    payload_len: usize,
}

struct RawCarScanner<R: Read> {
    reader: CarBlockReader<R>,
    scratch: Vec<u8>,
    zstd_prefetch_stats: Option<Arc<ZstdPrefetchStats>>,
}

impl RawCarScanner<Box<dyn Read>> {
    fn open(path: &Path) -> Result<Self> {
        Self::open_with_buffer(path, BUFFER_SIZE)
    }

    fn open_with_buffer(path: &Path, io_buf_bytes: usize) -> Result<Self> {
        let compressed = path
            .extension()
            .and_then(|s| s.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"));
        Self::open_with_compression_and_buffer(path, compressed, io_buf_bytes)
    }

    fn open_with_compression(path: &Path, compressed: bool) -> Result<Self> {
        Self::open_with_compression_and_buffer(path, compressed, BUFFER_SIZE)
    }

    fn open_with_compression_and_buffer(
        path: &Path,
        compressed: bool,
        io_buf_bytes: usize,
    ) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let reader = BufReader::with_capacity(io_buf_bytes, file);
        let input: Box<dyn Read> = if compressed {
            Box::new(
                zstd_decoder_with_long_window(reader)
                    .with_context(|| format!("zstd decode {}", path.display()))?,
            )
        } else {
            Box::new(reader)
        };
        Ok(RawCarScanner {
            reader: CarBlockReader::with_capacity(input, io_buf_bytes),
            scratch: Vec::new(),
            zstd_prefetch_stats: None,
        })
    }

    fn open_zstd_prefetch_with_buffer(
        path: &Path,
        io_buf_bytes: usize,
        prefetch_bytes: usize,
    ) -> Result<Self> {
        anyhow::ensure!(
            input_path_is_car_zstd(path),
            "zstd prefetch requires a local .car.zst input"
        );
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let reader = BufReader::with_capacity(io_buf_bytes, file);
        let decoder = zstd_decoder_with_long_window(reader)
            .with_context(|| format!("zstd decode {}", path.display()))?;
        let (prefetch_reader, stats) = ZstdPrefetchReader::start(decoder, prefetch_bytes)
            .with_context(|| {
                format!(
                    "start ordered zstd prefetch for {} with {} bytes per buffer",
                    path.display(),
                    prefetch_bytes
                )
            })?;
        let input: Box<dyn Read> = Box::new(prefetch_reader);
        Ok(RawCarScanner {
            reader: CarBlockReader::with_capacity(input, io_buf_bytes),
            scratch: Vec::new(),
            zstd_prefetch_stats: Some(stats),
        })
    }

    fn open_url(url: &str) -> Result<Self> {
        let response = reqwest::blocking::Client::builder()
            .user_agent(concat!("blockzilla/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("build HTTP client")?
            .get(url)
            .send()
            .with_context(|| format!("GET {url}"))?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("{url} returned HTTP {status}");
        }
        if let Some(len) = response.content_length() {
            info!("Streaming CAR URL: {url} content_length={len}");
        } else {
            info!("Streaming CAR URL: {url} content_length=unknown");
        }

        let reader = BufReader::with_capacity(BUFFER_SIZE, response);
        let input: Box<dyn Read> = if input_label_is_zstd(url) {
            Box::new(
                zstd_decoder_with_long_window(reader)
                    .with_context(|| format!("zstd decode {url}"))?,
            )
        } else {
            Box::new(reader)
        };
        Ok(RawCarScanner {
            reader: CarBlockReader::with_capacity(input, BUFFER_SIZE),
            scratch: Vec::new(),
            zstd_prefetch_stats: None,
        })
    }
}

impl<R: Read> RawCarScanner<R> {
    fn skip_header(&mut self) -> Result<()> {
        self.reader.skip_header().context("skip CAR header")
    }

    fn next_blockhash_node(&mut self) -> Result<Option<RawPrefix<'_>>> {
        self.reader
            .read_entry_payload_select_with_scratch(
                &mut self.scratch,
                NODE_KIND_PREFIX_LEN,
                |prefix| {
                    if is_block_node(prefix) {
                        CarPayloadRead::Full
                    } else if is_entry_node(prefix) {
                        CarPayloadRead::Prefix(BLOCKHASH_SCAN_PREFIX_LEN)
                    } else {
                        CarPayloadRead::Skip
                    }
                },
            )
            .map(|entry| {
                entry.map(|entry| RawPrefix {
                    cid: entry.cid,
                    prefix: entry.prefix,
                    payload_len: entry.payload_len,
                })
            })
            .map_err(|err| anyhow!("{err}"))
    }

    fn next_node_timed(
        &mut self,
        timings: Option<&mut ArchiveV2Timings>,
    ) -> Result<Option<RawNodeWithLen>> {
        let started = ArchiveV2Timings::optional_detail_timer(timings.as_deref());
        let Some(entry) = self
            .reader
            .read_entry_payload_with_scratch(&mut self.scratch)
            .map_err(|err| anyhow!("{err}"))?
        else {
            return Ok(None);
        };
        let node =
            decode_raw_node(entry.location, entry.cid, entry.payload).with_context(|| {
                format!(
                    "decode node at entry {} offset {}",
                    entry.location.entry_index, entry.location.car_offset
                )
            })?;
        if let Some(timings) = timings {
            timings.scan_decode_node += started.elapsed();
        }
        Ok(Some(RawNodeWithLen {
            node,
            payload_len: entry.payload_len,
        }))
    }

    fn next_node_timed_with_data_buffers(
        &mut self,
        pool: &mut RawDataFramePayloadPool,
        timings: Option<&mut ArchiveV2Timings>,
    ) -> Result<Option<RawNodeWithLen>> {
        let started = ArchiveV2Timings::optional_detail_timer(timings.as_deref());
        let Some(entry) = self
            .reader
            .read_entry_payload_with_scratch(&mut self.scratch)
            .map_err(|err| anyhow!("{err}"))?
        else {
            return Ok(None);
        };
        let node = decode_raw_node_with_data_buffers(
            entry.location,
            entry.cid,
            entry.payload,
            &mut |required| pool.take(required),
        )
        .with_context(|| {
            format!(
                "decode node at entry {} offset {}",
                entry.location.entry_index, entry.location.car_offset
            )
        })?;
        if let Some(timings) = timings {
            timings.scan_decode_node += started.elapsed();
        }
        Ok(Some(RawNodeWithLen {
            node,
            payload_len: entry.payload_len,
        }))
    }
}

fn input_label_is_zstd(label: &str) -> bool {
    let without_query = label.split_once('?').map(|(head, _)| head).unwrap_or(label);
    without_query.to_ascii_lowercase().ends_with(".zst")
}

fn input_path_is_car_zstd(path: &Path) -> bool {
    const SUFFIX: &str = ".car.zst";
    if path.to_str().is_some_and(|label| label.contains("://")) {
        return false;
    }
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.get(name.len().checked_sub(SUFFIX.len())?..))
        .is_some_and(|suffix| suffix.eq_ignore_ascii_case(SUFFIX))
}

fn detailed_timings_enabled_from_value(value: Option<&str>) -> bool {
    matches!(value, Some("1"))
}

fn detailed_timings_enabled() -> bool {
    detailed_timings_enabled_from_value(std::env::var(DETAILED_TIMINGS_ENV).ok().as_deref())
}

struct DetailTimer(Option<Instant>);

impl DetailTimer {
    #[inline]
    fn start(enabled: bool) -> Self {
        Self(enabled.then(Instant::now))
    }

    #[inline]
    fn elapsed(&self) -> Duration {
        self.0.as_ref().map_or(Duration::ZERO, Instant::elapsed)
    }
}

#[derive(Default)]
struct ArchiveV2Timings {
    detailed: bool,
    scan_decode_node: Duration,
    classify: Duration,
    dataframe_assemble: Duration,
    tx_decode_compact: Duration,
    metadata_decode_compact: Duration,
    first_seen_parallel_decode_wall: Duration,
    metadata_logs_decode_compact: Duration,
    metadata_pubkey_compact: Duration,
    rewards_decode_compact: Duration,
    wincode_encode: Duration,
    hot_message_build: Duration,
    hot_message_encode: Duration,
    hot_metadata_encode: Duration,
    hot_signature_write: Duration,
    hot_block_serialize: Duration,
    hot_zstd_compress: Duration,
    hot_block_write: Duration,
    hot_poh_write: Duration,
    tx_reassembled: u64,
    metadata_reassembled: u64,
    metadata_protobuf_visit: u64,
    metadata_owned_fallback: u64,
    tx_scratch_max: usize,
    metadata_scratch_max: usize,
    hot_zstd_buffer_reserves: u64,
    hot_zstd_buffer_capacity_max: usize,
}

impl ArchiveV2Timings {
    fn from_env() -> Self {
        Self {
            detailed: detailed_timings_enabled(),
            ..Self::default()
        }
    }

    #[inline]
    fn detail_timer(&self) -> DetailTimer {
        DetailTimer::start(self.detailed)
    }

    #[inline]
    fn optional_detail_timer(timings: Option<&Self>) -> DetailTimer {
        DetailTimer::start(timings.is_some_and(|timings| timings.detailed))
    }
}

#[derive(Default)]
struct FirstSeenTxDecodeStats {
    timings: ArchiveV2Timings,
    tx_source_bytes: u64,
    metadata_source_bytes: u64,
    nonce_recent_blockhashes: u64,
}

struct FirstSeenTxWorkerScratch {
    generation: u64,
    record: BlockRecordScratch,
    metadata_zstd: ZstdReusableDecoder,
    stats: FirstSeenTxDecodeStats,
}

impl Default for FirstSeenTxWorkerScratch {
    fn default() -> Self {
        Self {
            generation: 0,
            record: BlockRecordScratch {
                tx_bytes: Vec::with_capacity(FIRST_SEEN_WORKER_TX_SCRATCH_INITIAL_BYTES),
                metadata_bytes: Vec::with_capacity(
                    FIRST_SEEN_WORKER_METADATA_SCRATCH_INITIAL_BYTES,
                ),
                reassemble_visited: HashSet::with_capacity(8),
            },
            metadata_zstd: ZstdReusableDecoder::new(),
            stats: FirstSeenTxDecodeStats::default(),
        }
    }
}

impl FirstSeenTxWorkerScratch {
    fn prepare(&mut self, generation: u64, detailed_timings: bool) {
        if self.generation == generation {
            return;
        }
        self.generation = generation;
        self.stats = FirstSeenTxDecodeStats::default();
        self.stats.timings.detailed = detailed_timings;
    }

    fn take_stats(&mut self, generation: u64) -> FirstSeenTxDecodeStats {
        let stats = if self.generation == generation {
            std::mem::take(&mut self.stats)
        } else {
            FirstSeenTxDecodeStats::default()
        };
        self.trim_oversized_buffers();
        stats
    }

    fn trim_oversized_buffers(&mut self) {
        if self.record.tx_bytes.capacity() > FIRST_SEEN_WORKER_SCRATCH_MAX_RETAINED_BYTES {
            self.record.tx_bytes = Vec::with_capacity(FIRST_SEEN_WORKER_TX_SCRATCH_INITIAL_BYTES);
        }
        if self.record.metadata_bytes.capacity() > FIRST_SEEN_WORKER_SCRATCH_MAX_RETAINED_BYTES {
            self.record.metadata_bytes =
                Vec::with_capacity(FIRST_SEEN_WORKER_METADATA_SCRATCH_INITIAL_BYTES);
        }
        self.metadata_zstd.trim_oversized_output();
    }
}

thread_local! {
    static FIRST_SEEN_TX_WORKER_SCRATCH: RefCell<FirstSeenTxWorkerScratch> =
        RefCell::new(FirstSeenTxWorkerScratch::default());
}

struct FirstSeenTxDecodePool {
    pool: rayon::ThreadPool,
    generation: u64,
}

impl FirstSeenTxDecodePool {
    fn new(workers: usize) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("first-seen-decode-{index}"))
            .build()
            .context("create first-seen transaction decode pool")?;
        info!("Archive V2 first-seen parallel transaction decode enabled: workers={workers}");
        Ok(Self {
            pool,
            generation: 0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_block_transactions(
        &mut self,
        transactions: &[PendingTx],
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
        signature_counts: &[u8],
        slot: u64,
        key_index: &KeyIndex,
        rolling_blockhashes: &RollingBlockhashIndex,
        footer: &mut WincodeArchiveV2Footer,
        timings: &mut ArchiveV2Timings,
    ) -> Result<Vec<WincodeArchiveV2Transaction>> {
        anyhow::ensure!(
            signature_counts.len() == transactions.len(),
            "slot {slot} collected {} signature counts for {} transactions",
            signature_counts.len(),
            transactions.len(),
        );
        self.generation = self
            .generation
            .checked_add(1)
            .context("first-seen decode generation overflow")?;
        let generation = self.generation;
        let detailed_timings = timings.detailed;
        let wall_started = timings.detail_timer();

        // Indexed collection preserves transaction order. If several malformed
        // transactions fail concurrently, Rayon does not define which error is
        // returned; the candidate build still aborts before completion/promotion.
        let decoded = self.pool.install(|| {
            transactions
                .par_iter()
                .enumerate()
                .map(|(tx_index, transaction)| {
                    FIRST_SEEN_TX_WORKER_SCRATCH.with(|scratch| {
                        let mut scratch = scratch.borrow_mut();
                        scratch.prepare(generation, detailed_timings);
                        decode_first_seen_transaction(
                            transaction,
                            dataframes,
                            signature_counts[tx_index],
                            slot,
                            tx_index,
                            key_index,
                            rolling_blockhashes,
                            &mut scratch,
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()
        })?;
        timings.first_seen_parallel_decode_wall += wall_started.elapsed();

        let worker_stats = self.pool.broadcast(|_| {
            FIRST_SEEN_TX_WORKER_SCRATCH.with(|scratch| scratch.borrow_mut().take_stats(generation))
        });
        for stats in worker_stats {
            merge_first_seen_tx_decode_stats(stats, footer, timings);
        }
        Ok(decoded)
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_first_seen_transaction(
    pending_tx: &PendingTx,
    dataframes: &HashMap<Cid36, StandaloneDataFrame>,
    expected_signature_count: u8,
    slot: u64,
    tx_index: usize,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    worker: &mut FirstSeenTxWorkerScratch,
) -> Result<WincodeArchiveV2Transaction> {
    let FirstSeenTxWorkerScratch {
        record,
        metadata_zstd,
        stats,
        ..
    } = worker;

    let assemble_started = stats.timings.detail_timer();
    let (tx_bytes, tx_scratch_capacity) = dataframe_bytes_for_decode(
        &pending_tx.tx.data,
        dataframes,
        &mut record.tx_bytes,
        &mut record.reassemble_visited,
    )
    .with_context(|| format!("slot {slot} tx#{tx_index} reassemble transaction bytes"))?;
    stats.timings.dataframe_assemble += assemble_started.elapsed();
    stats.tx_source_bytes += tx_bytes.len() as u64;
    stats.timings.tx_reassembled += 1;
    stats.timings.tx_scratch_max = stats.timings.tx_scratch_max.max(tx_scratch_capacity);

    let tx_started = stats.timings.detail_timer();
    let value = decode_first_seen_compact_transaction(
        slot,
        tx_index,
        tx_bytes,
        expected_signature_count,
        key_index,
        rolling_blockhashes,
        &mut stats.nonce_recent_blockhashes,
    )
    .with_context(|| format!("slot {slot} tx#{tx_index} transaction"))?;
    let tx = WincodeArchiveV2Payload::Decoded {
        source_len: tx_bytes.len() as u64,
        value,
    };
    stats.timings.tx_decode_compact += tx_started.elapsed();

    let assemble_started = stats.timings.detail_timer();
    let (metadata_bytes, metadata_scratch_capacity) = dataframe_bytes_for_decode(
        &pending_tx.tx.metadata,
        dataframes,
        &mut record.metadata_bytes,
        &mut record.reassemble_visited,
    )
    .with_context(|| format!("slot {slot} tx#{tx_index} reassemble metadata bytes"))?;
    stats.timings.dataframe_assemble += assemble_started.elapsed();
    stats.metadata_source_bytes += metadata_bytes.len() as u64;
    stats.timings.metadata_reassembled += 1;
    stats.timings.metadata_scratch_max = stats
        .timings
        .metadata_scratch_max
        .max(metadata_scratch_capacity);
    let metadata = if metadata_bytes.is_empty() {
        None
    } else {
        let metadata_started = stats.timings.detail_timer();
        let payload = decode_metadata_payload(
            slot,
            tx_index,
            metadata_bytes,
            key_index,
            metadata_zstd,
            &mut stats.timings,
        )?;
        stats.timings.metadata_decode_compact += metadata_started.elapsed();
        Some(payload)
    };

    Ok(WincodeArchiveV2Transaction {
        tx_index: pending_tx.tx.index.unwrap_or(tx_index as u64) as u32,
        tx,
        metadata,
    })
}

fn merge_first_seen_tx_decode_stats(
    stats: FirstSeenTxDecodeStats,
    footer: &mut WincodeArchiveV2Footer,
    timings: &mut ArchiveV2Timings,
) {
    footer.tx_source_bytes += stats.tx_source_bytes;
    footer.metadata_source_bytes += stats.metadata_source_bytes;
    footer.nonce_recent_blockhashes += stats.nonce_recent_blockhashes;

    timings.dataframe_assemble += stats.timings.dataframe_assemble;
    timings.tx_decode_compact += stats.timings.tx_decode_compact;
    timings.metadata_decode_compact += stats.timings.metadata_decode_compact;
    timings.metadata_logs_decode_compact += stats.timings.metadata_logs_decode_compact;
    timings.metadata_pubkey_compact += stats.timings.metadata_pubkey_compact;
    timings.tx_reassembled += stats.timings.tx_reassembled;
    timings.metadata_reassembled += stats.timings.metadata_reassembled;
    timings.metadata_protobuf_visit += stats.timings.metadata_protobuf_visit;
    timings.metadata_owned_fallback += stats.timings.metadata_owned_fallback;
    timings.tx_scratch_max = timings.tx_scratch_max.max(stats.timings.tx_scratch_max);
    timings.metadata_scratch_max = timings
        .metadata_scratch_max
        .max(stats.timings.metadata_scratch_max);
}

#[cfg(test)]
mod tests {
    use super::*;
    use of_car_reader::reconstruct::{NodeLocation, RawCidRef, RawDataFrame};
    use of_car_reader::versioned_transaction::{
        CompiledInstruction, MessageAddressTableLookup, MessageHeader, V0Message,
    };
    use solana_vote_interface::state::Lockout;

    #[test]
    fn block_access_frame_limit_is_shared_and_inclusive() {
        let limit = usize::try_from(ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES).unwrap();
        assert_eq!(
            checked_archive_v2_block_access_frame_len(limit, 42).unwrap(),
            u32::try_from(limit).unwrap()
        );
        let error = checked_archive_v2_block_access_frame_len(limit + 1, 43).unwrap_err();
        assert!(error.to_string().contains("exceeding the shared"));
    }

    struct ScriptedTerminalReader {
        bytes: Vec<u8>,
        offset: usize,
        max_read: usize,
        interrupt_once: bool,
        terminal_error: Option<ErrorKind>,
    }

    impl Read for ScriptedTerminalReader {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            if self.interrupt_once {
                self.interrupt_once = false;
                return Err(std::io::Error::new(
                    ErrorKind::Interrupted,
                    "scripted interrupt",
                ));
            }
            if self.offset < self.bytes.len() {
                let read = output
                    .len()
                    .min(self.max_read)
                    .min(self.bytes.len() - self.offset);
                output[..read].copy_from_slice(&self.bytes[self.offset..self.offset + read]);
                self.offset += read;
                return Ok(read);
            }
            match self.terminal_error {
                Some(kind) => Err(std::io::Error::new(kind, "scripted terminal error")),
                None => Ok(0),
            }
        }
    }

    struct PanicReader;

    impl Read for PanicReader {
        fn read(&mut self, _output: &mut [u8]) -> std::io::Result<usize> {
            panic!("intentional prefetch worker panic")
        }
    }

    struct DropObservedReader {
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Read for DropObservedReader {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            output.fill(0x5a);
            Ok(output.len())
        }
    }

    impl Drop for DropObservedReader {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    fn encode_zstd_frame_with_window(payload: &[u8], window_log: u32) -> Vec<u8> {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.include_contentsize(false).unwrap();
        encoder.window_log(window_log).unwrap();
        encoder.write_all(payload).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn owned_long_window_decoder_reads_concatenated_frames() {
        const TEST_WINDOW_LOG: u32 = 23;
        let first = (0..1_048_579)
            .map(|index| ((index * 31 + index / 251) & 0xff) as u8)
            .collect::<Vec<_>>();
        let second = b"second concatenated zstd frame".repeat(257);
        let first_frame = encode_zstd_frame_with_window(&first, TEST_WINDOW_LOG);
        let second_frame = zstd::stream::encode_all(second.as_slice(), 1).unwrap();
        let mut compressed = first_frame.clone();
        compressed.extend_from_slice(&second_frame);

        // Verify the first frame actually requires the declared long window,
        // so this test exercises the decoder limit rather than only roundtrip.
        let mut too_small =
            zstd::stream::read::Decoder::with_buffer(BufReader::new(first_frame.as_slice()))
                .unwrap();
        too_small.window_log_max(TEST_WINDOW_LOG - 1).unwrap();
        let mut ignored = Vec::new();
        assert!(too_small.read_to_end(&mut ignored).is_err());

        let reader = BufReader::with_capacity(17, compressed.as_slice());
        let mut decoder = zstd_decoder_with_long_window(reader).unwrap();
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();

        let mut expected = first;
        expected.extend_from_slice(&second);
        assert_eq!(decoded, expected);
    }

    #[test]
    fn owned_long_window_decoder_can_borrow_and_drop_repeatedly() {
        let expected = b"owned decoder drop regression".repeat(32);
        let compressed = zstd::stream::encode_all(expected.as_slice(), 1).unwrap();

        for _ in 0..128 {
            // A borrowed reader proves the decoder context is owned independently
            // rather than requiring the input reader itself to be `'static`.
            let reader = BufReader::with_capacity(11, compressed.as_slice());
            let mut decoder = zstd_decoder_with_long_window(reader).unwrap();
            let mut decoded = Vec::new();
            decoder.read_to_end(&mut decoded).unwrap();
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn zstd_prefetch_partial_reads_are_exact_and_reuse_two_buffers() {
        let expected = (0..257)
            .map(|index| ((index * 31 + index / 7) & 0xff) as u8)
            .collect::<Vec<_>>();
        let (mut reader, stats) =
            ZstdPrefetchReader::start(std::io::Cursor::new(expected.clone()), 13).unwrap();

        assert_eq!(reader.read(&mut []).unwrap(), 0);
        let mut decoded = Vec::new();
        let mut scratch = [0_u8; 19];
        let read_sizes = [1, 7, 2, 19, 3, 11, 5];
        for size in read_sizes.into_iter().cycle() {
            let read = reader.read(&mut scratch[..size]).unwrap();
            if read == 0 {
                break;
            }
            assert!(read <= size);
            decoded.extend_from_slice(&scratch[..read]);
        }

        assert_eq!(decoded, expected);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.worker_bytes, expected.len() as u64);
        assert_eq!(snapshot.consumer_bytes, expected.len() as u64);
        assert_eq!(snapshot.worker_chunks, 20);
        let addresses = stats
            .worker_buffer_addresses
            .lock()
            .expect("prefetch buffer address lock poisoned");
        assert_eq!(
            addresses.len(),
            2,
            "only the two startup buffers may appear"
        );
    }

    #[test]
    fn zstd_prefetch_retries_interrupted_and_returns_bytes_before_error() {
        let expected = b"bytes must be observable before the terminal read error".repeat(3);
        let source = ScriptedTerminalReader {
            bytes: expected.clone(),
            offset: 0,
            max_read: 5,
            interrupt_once: true,
            terminal_error: Some(ErrorKind::InvalidData),
        };
        let (mut reader, _stats) = ZstdPrefetchReader::start(source, 256).unwrap();
        let mut decoded = Vec::new();
        let error = reader
            .read_to_end(&mut decoded)
            .expect_err("scripted terminal error must be preserved");
        assert_eq!(decoded, expected);
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("scripted terminal error"));
    }

    #[test]
    fn zstd_prefetch_worker_panic_is_not_reported_as_eof() {
        let (mut reader, _stats) = ZstdPrefetchReader::start(PanicReader, 32).unwrap();
        let error = reader
            .read(&mut [0_u8; 1])
            .expect_err("worker panic must be a read error");
        assert_eq!(error.kind(), ErrorKind::Other);
        assert!(error.to_string().contains("worker panicked"));
    }

    #[test]
    fn zstd_prefetch_drop_closes_channels_before_join() {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let source = DropObservedReader {
            dropped: Arc::clone(&dropped),
        };
        let (reader, _stats) = ZstdPrefetchReader::start(source, 32).unwrap();
        // The worker can be blocked sending its second chunk. Drop must close
        // the ready receiver before joining it.
        drop(reader);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn zstd_prefetch_preserves_truncated_stream_prefix_and_error() {
        let payload = (0..1_048_579)
            .map(|index| ((index * 17 + index / 251) & 0xff) as u8)
            .collect::<Vec<_>>();
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.write_all(&payload).unwrap();
        let mut compressed = encoder.finish().unwrap();
        compressed.truncate(compressed.len() - 2);

        let mut synchronous = zstd_decoder_with_long_window(BufReader::with_capacity(
            17,
            std::io::Cursor::new(compressed.clone()),
        ))
        .unwrap();
        let mut synchronous_prefix = Vec::new();
        synchronous
            .read_to_end(&mut synchronous_prefix)
            .expect_err("truncated synchronous decoder must fail");

        let decoder = zstd_decoder_with_long_window(BufReader::with_capacity(
            17,
            std::io::Cursor::new(compressed),
        ))
        .unwrap();
        let (mut prefetched, _stats) = ZstdPrefetchReader::start(decoder, 32 << 10).unwrap();
        let mut prefetched_prefix = Vec::new();
        prefetched
            .read_to_end(&mut prefetched_prefix)
            .expect_err("truncated prefetched decoder must fail");

        assert!(!synchronous_prefix.is_empty());
        assert_eq!(prefetched_prefix, synchronous_prefix);
    }

    #[test]
    fn detailed_timings_require_exact_opt_in_value() {
        assert!(!detailed_timings_enabled_from_value(None));
        assert!(detailed_timings_enabled_from_value(Some("1")));
        for value in ["", "0", "true", "TRUE", "yes", " 1", "1 "] {
            assert!(
                !detailed_timings_enabled_from_value(Some(value)),
                "{value:?}"
            );
        }
    }

    #[test]
    fn mmap_registry_index_round_trips_ids() {
        static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "blockzilla-mmap-registry-index-test-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed),
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let registry_path = root.join(REGISTRY_FILE);
        let index_path = root.join(REGISTRY_INDEX_FILE);
        let keys = (0u64..2_048)
            .map(|value| {
                let mut key = [0u8; 32];
                key[..8].copy_from_slice(&value.to_le_bytes());
                key[8..16].copy_from_slice(&value.wrapping_mul(31).to_be_bytes());
                key
            })
            .collect::<Vec<_>>();
        write_registry_iter(&registry_path, keys.iter().copied()).unwrap();

        build_registry_index(&registry_path, Some(&index_path), true).unwrap();
        let index = KeyIndex::load(&index_path).unwrap();
        for (position, key) in keys.iter().enumerate() {
            assert_eq!(index.lookup(key), Some(position as u32 + 1));
        }
        assert_eq!(index.lookup(&[0xff; 32]), None);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn mapped_registry_detects_path_replacement() {
        static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "blockzilla-mmap-registry-replace-test-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed),
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let registry_path = root.join(REGISTRY_FILE);
        let replacement_path = root.join("registry.replacement");
        std::fs::write(&registry_path, [1u8; 64]).unwrap();
        let registry = MappedRegistryKeys::open(&registry_path).unwrap();
        assert_eq!(registry.keys(), &[[1u8; 32], [1u8; 32]]);

        std::fs::write(&replacement_path, [2u8; 64]).unwrap();
        std::fs::rename(&replacement_path, &registry_path).unwrap();
        let error = registry
            .ensure_unchanged()
            .expect_err("replaced registry path must be rejected");
        assert!(
            error
                .to_string()
                .contains("registry path was replaced while building MPHF"),
            "unexpected error: {error:#}"
        );
        drop(registry);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn first_seen_decode_workers_are_bounded() {
        for workers in [0, FIRST_SEEN_DECODE_WORKERS_MAX + 1] {
            let error = build_hot_blocks_first_seen(
                Path::new("unused-input.car"),
                Path::new("unused-output"),
                None,
                None,
                1,
                None,
                false,
                false,
                None,
                65_536,
                1,
                workers,
                0,
                false,
            )
            .expect_err("out-of-range decode workers must fail before filesystem access");
            assert!(
                error
                    .to_string()
                    .contains("first-seen decode workers must be in 1..=8"),
                "unexpected error: {error:#}"
            );
        }
    }

    #[test]
    fn first_seen_zstd_prefetch_is_bounded_and_local_car_zst_only() {
        for (input, prefetch_mib, expected) in [
            (
                "unused-input.car",
                FIRST_SEEN_ZSTD_PREFETCH_MIB_MAX + 1,
                "CAR zstd prefetch must be in 0..=64 MiB",
            ),
            ("unused-input.car", 1, "requires a local .car.zst input"),
            (
                "https://example.invalid/epoch-822.car.zst",
                1,
                "requires a local .car.zst input",
            ),
        ] {
            let error = build_hot_blocks_first_seen(
                Path::new(input),
                Path::new("unused-output"),
                None,
                None,
                1,
                None,
                false,
                false,
                None,
                65_536,
                1,
                1,
                prefetch_mib,
                false,
            )
            .expect_err("invalid prefetch setting must fail before filesystem access");
            assert!(
                error.to_string().contains(expected),
                "unexpected error for {input}: {error:#}"
            );
        }
    }

    #[test]
    fn parallel_first_seen_fixture_is_byte_identical_to_one_worker() {
        static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);

        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../crates/old-faithful/car-reader/benches/fixtures/epoch-822-biggest.car");
        let root = std::env::temp_dir().join(format!(
            "blockzilla-first-seen-parallel-test-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed),
        ));
        let one_worker = root.join("workers-1");
        let four_workers = root.join("workers-4");
        let _ = std::fs::remove_dir_all(&root);

        for (output, workers) in [(&one_worker, 1), (&four_workers, 4)] {
            build_hot_blocks_first_seen(
                &fixture,
                output,
                None,
                None,
                1,
                Some(1),
                false,
                true,
                None,
                65_536,
                100_000,
                workers,
                0,
                false,
            )
            .unwrap_or_else(|error| panic!("workers={workers} fixture build failed: {error:#}"));
        }

        let artifact_names = |dir: &Path| {
            let mut names = std::fs::read_dir(dir)
                .unwrap()
                .map(|entry| {
                    entry
                        .unwrap()
                        .file_name()
                        .into_string()
                        .expect("fixture artifact name must be UTF-8")
                })
                .collect::<Vec<_>>();
            names.sort_unstable();
            names
        };
        let expected_names = artifact_names(&one_worker);
        assert_eq!(artifact_names(&four_workers), expected_names);
        for name in expected_names {
            assert_eq!(
                std::fs::read(one_worker.join(&name)).unwrap(),
                std::fs::read(four_workers.join(&name)).unwrap(),
                "first-seen artifact differs with four decode workers: {name}"
            );
        }

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn first_seen_scan_only_defers_mphf_and_metadata_until_finalizer() {
        static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);

        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../crates/old-faithful/car-reader/benches/fixtures/epoch-822-biggest.car");
        let root = std::env::temp_dir().join(format!(
            "blockzilla-first-seen-deferred-test-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed),
        ));
        let lock_path = root.with_extension("lock");
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_file(&lock_path);

        build_hot_blocks_first_seen(
            &fixture, &root, None, None, 1, None, false, false, None, 65_536, 100_000, 1, 0, true,
        )
        .unwrap();

        assert!(
            root.join(crate::first_seen_finalization::FIRST_SEEN_SCAN_COMPLETE_FILE)
                .is_file()
        );
        assert!(root.join(BLOCKHASH_INDEX_V3_FILE).is_file());
        assert!(root.join(BLOCK_TIME_GAP_FILE).is_file());
        assert!(!root.join(REGISTRY_INDEX_FILE).exists());
        assert!(!root.join(ARCHIVE_V2_META_FILE).exists());
        assert!(pre_hot_tmp_path(&root.join(ARCHIVE_V2_META_FILE)).is_file());
        assert!(pre_hot_tmp_path(&root.join(FIRST_SEEN_REGISTRY_MANIFEST_FILE)).is_file());

        crate::first_seen_finalization::finalize_first_seen_scan(&root, Some(&lock_path)).unwrap();
        assert!(root.join(REGISTRY_INDEX_FILE).is_file());
        assert!(root.join(FIRST_SEEN_REGISTRY_MANIFEST_FILE).is_file());
        assert!(root.join(ARCHIVE_V2_META_FILE).is_file());
        assert!(
            !root
                .join(crate::first_seen_finalization::FIRST_SEEN_SCAN_COMPLETE_FILE)
                .exists()
        );

        // A completed candidate is an idempotent no-op for retrying supervisors.
        crate::first_seen_finalization::finalize_first_seen_scan(&root, Some(&lock_path)).unwrap();
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::remove_file(lock_path).unwrap();
    }

    #[test]
    fn prefetched_epoch_822_first_seen_fixture_is_byte_identical() {
        static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);

        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../crates/old-faithful/car-reader/benches/fixtures/epoch-822-biggest.car");
        let root = std::env::temp_dir().join(format!(
            "blockzilla-first-seen-prefetch-test-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed),
        ));
        let compressed_fixture = root.join("epoch-822-biggest.car.zst");
        let no_prefetch = root.join("prefetch-0");
        let prefetch_16 = root.join("prefetch-16");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let fixture_bytes = std::fs::read(&fixture).unwrap();
        let compressed = zstd::stream::encode_all(fixture_bytes.as_slice(), 1).unwrap();
        std::fs::write(&compressed_fixture, compressed).unwrap();

        for (output, prefetch_mib) in [(&no_prefetch, 0), (&prefetch_16, 16)] {
            build_hot_blocks_first_seen(
                &compressed_fixture,
                output,
                None,
                None,
                1,
                Some(1),
                false,
                false,
                None,
                65_536,
                100_000,
                4,
                prefetch_mib,
                false,
            )
            .unwrap_or_else(|error| {
                panic!("prefetch_mib={prefetch_mib} fixture build failed: {error:#}")
            });
        }

        let artifact_names = |dir: &Path| {
            let mut names = std::fs::read_dir(dir)
                .unwrap()
                .map(|entry| {
                    entry
                        .unwrap()
                        .file_name()
                        .into_string()
                        .expect("fixture artifact name must be UTF-8")
                })
                .collect::<Vec<_>>();
            names.sort_unstable();
            names
        };
        let expected_names = artifact_names(&no_prefetch);
        assert_eq!(artifact_names(&prefetch_16), expected_names);
        for name in expected_names {
            assert_eq!(
                std::fs::read(no_prefetch.join(&name)).unwrap(),
                std::fs::read(prefetch_16.join(&name)).unwrap(),
                "first-seen artifact differs with 16 MiB zstd prefetch: {name}"
            );
        }

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn async_first_seen_block_writer_matches_ordered_sync_bytes_and_rows() {
        static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);

        let path = std::env::temp_dir().join(format!(
            "blockzilla-first-seen-zstd-test-{}-{}.zstd",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed),
        ));
        let _ = std::fs::remove_file(&path);
        let level = 3;
        let payloads = [
            (0..32_769)
                .map(|index| (index as u8).wrapping_mul(31))
                .collect::<Vec<_>>(),
            vec![0x5a; 131_071],
            (0..777_777)
                .map(|index| ((index * 17 + index / 251) & 0xff) as u8)
                .collect::<Vec<_>>(),
            b"final-small-block".repeat(97),
        ];

        let mut expected_bytes = Vec::new();
        let mut expected_rows = Vec::new();
        let mut expected_compressor = zstd::bulk::Compressor::new(level).unwrap();
        let mut expected_compressed = Vec::new();
        let mut expected_offset = 0u64;
        let mut first_tx_ordinal = 0u64;
        let mut first_signature_ordinal = 0u64;
        for (index, payload) in payloads.iter().enumerate() {
            expected_compressed.clear();
            let compress_bound = zstd::zstd_safe::compress_bound(payload.len());
            if expected_compressed.capacity() < compress_bound {
                expected_compressed
                    .reserve(compress_bound.saturating_sub(expected_compressed.len()));
            }
            expected_compressor
                .compress_to_buffer(payload, &mut expected_compressed)
                .unwrap();
            let compressed_len = u32::try_from(expected_compressed.len()).unwrap();
            expected_bytes.extend_from_slice(&expected_compressed);
            expected_rows.push(ArchiveV2HotBlockIndexRow {
                block_id: index as u32,
                slot: 10_000 + index as u64 * 3,
                compressed_offset: expected_offset,
                compressed_len,
                uncompressed_len: u32::try_from(payload.len()).unwrap(),
                tx_count: index as u32 + 1,
                first_tx_ordinal,
                first_signature_ordinal,
                signature_count: index as u32 + 2,
            });
            expected_offset += u64::from(compressed_len);
            first_tx_ordinal += index as u64 + 1;
            first_signature_ordinal += index as u64 + 2;
        }

        let file = File::create(&path).unwrap();
        let mut pipeline =
            FirstSeenAsyncBlockWriter::start(file, path.clone(), level, true).unwrap();
        let mut block_bytes = Vec::new();
        first_tx_ordinal = 0;
        first_signature_ordinal = 0;
        for (index, payload) in payloads.iter().enumerate() {
            block_bytes.clear();
            block_bytes.extend_from_slice(payload);
            block_bytes = pipeline
                .submit(FirstSeenBlockWriteJob {
                    block_bytes,
                    block_id: index as u32,
                    slot: 10_000 + index as u64 * 3,
                    tx_count: index as u32 + 1,
                    first_tx_ordinal,
                    first_signature_ordinal,
                    signature_count: index as u32 + 2,
                })
                .unwrap();
            first_tx_ordinal += index as u64 + 1;
            first_signature_ordinal += index as u64 + 2;
        }
        let summary = pipeline.finish().unwrap();
        let actual_bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(actual_bytes, expected_bytes);
        assert_eq!(summary.blob_offset, expected_offset);
        assert_eq!(summary.compressed_bytes, expected_offset);
        assert_eq!(
            summary.uncompressed_bytes,
            payloads
                .iter()
                .map(|payload| payload.len() as u64)
                .sum::<u64>()
        );
        assert_eq!(summary.rows.len(), expected_rows.len());
        for (actual, expected) in summary.rows.iter().zip(&expected_rows) {
            assert_eq!(actual.block_id, expected.block_id);
            assert_eq!(actual.slot, expected.slot);
            assert_eq!(actual.compressed_offset, expected.compressed_offset);
            assert_eq!(actual.compressed_len, expected.compressed_len);
            assert_eq!(actual.uncompressed_len, expected.uncompressed_len);
            assert_eq!(actual.tx_count, expected.tx_count);
            assert_eq!(actual.first_tx_ordinal, expected.first_tx_ordinal);
            assert_eq!(
                actual.first_signature_ordinal,
                expected.first_signature_ordinal
            );
            assert_eq!(actual.signature_count, expected.signature_count);
        }
    }

    #[test]
    fn async_first_seen_block_writer_propagates_flush_error() {
        let read_only = File::open(std::env::current_exe().unwrap()).unwrap();
        let mut pipeline = FirstSeenAsyncBlockWriter::start(
            read_only,
            PathBuf::from("read-only-test-output"),
            1,
            false,
        )
        .unwrap();
        let mut block_bytes = Vec::with_capacity(4096);
        block_bytes.extend_from_slice(&[0x44; 4096]);
        let _recycled = pipeline
            .submit(FirstSeenBlockWriteJob {
                block_bytes,
                block_id: 0,
                slot: 42,
                tx_count: 1,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 1,
            })
            .unwrap();
        let error = match pipeline.finish() {
            Ok(_) => panic!("read-only output unexpectedly accepted a write"),
            Err(error) => error,
        };
        assert!(
            format!("{error:#}").contains("flush read-only-test-output"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn compressed_pre_hot_record_stream_roundtrips() {
        let mut bytes = Vec::new();
        let mut writer = PreHotRecordWriter::new(&mut bytes).unwrap();
        writer
            .write(&LivePreHotRecord::Header(WincodeArchiveV2Header {
                version: LIVE_PRE_HOT_BLOCK_VERSION,
                flags: ARCHIVE_FLAGS_LEB128 | ARCHIVE_FLAGS_NO_REGISTRY,
            }))
            .unwrap();
        writer
            .write(&LivePreHotRecord::Block(LivePreHotBlock::new(
                0,
                empty_archive_v2_block(42),
                0,
            )))
            .unwrap();
        writer
            .write(&LivePreHotRecord::Footer(WincodeArchiveV2Footer {
                blocks: 1,
                ..WincodeArchiveV2Footer::default()
            }))
            .unwrap();
        let stats = writer.finish().unwrap();
        assert_eq!(stats.records, 3);

        let mut reader = PreHotRecordReader::new(std::io::Cursor::new(bytes)).unwrap();
        assert!(matches!(
            reader.read().unwrap(),
            Some(LivePreHotRecord::Header(_))
        ));
        let Some(LivePreHotRecord::Block(block)) = reader.read().unwrap() else {
            panic!("expected PreHot block")
        };
        assert_eq!(block.block_id, 0);
        assert_eq!(block.block.header.compact.slot, 42);
        assert!(matches!(
            reader.read().unwrap(),
            Some(LivePreHotRecord::Footer(_))
        ));
        assert!(reader.read().unwrap().is_none());
    }

    #[test]
    fn access_collector_includes_nested_system_log_pubkeys() {
        let logs = CompactLogStream {
            events: vec![
                LogEvent::System(SystemProgramLog::CreateAddressMismatch {
                    provided_addr: CompactPubkey::id(1),
                    derived_addr: PubkeyOrString::Pubkey(CompactPubkey::id(2)),
                }),
                LogEvent::System(SystemProgramLog::AllocateAccountAlreadyInUse {
                    addr: SystemAddress::Debug {
                        address: PubkeyOrString::Pubkey(CompactPubkey::id(3)),
                        base: Some(PubkeyOrString::Pubkey(CompactPubkey::id(4))),
                    },
                }),
                LogEvent::System(SystemProgramLog::NonceAccountMustBeSigner {
                    action: blockzilla_format::program_logs::system_program::NonceAction::Advance,
                    account: PubkeyOrString::Pubkey(CompactPubkey::id(5)),
                }),
                LogEvent::System(SystemProgramLog::TransferFromMustSign {
                    from: CompactPubkey::id(6),
                }),
            ],
            strings: blockzilla_format::StringTable::default(),
            data: blockzilla_format::DataTable::default(),
        };
        let mut ids = Vec::new();
        collect_access_log_refs(&logs, &mut ids);
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
    }

    fn hex_to_vec(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0);
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn archive_v2_record_tags_match_index_fast_scanner() {
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_VERSION,
                flags: ARCHIVE_FLAGS_LEB128,
            })),
            WINCODE_ARCHIVE_V2_RECORD_HEADER_TAG
        );
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Block(empty_archive_v2_block(0))),
            WINCODE_ARCHIVE_V2_RECORD_BLOCK_TAG
        );
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Index(SplitCompactIndexRecord {
                slot: 0,
                block_id: 0,
                block_offset: 0,
                block_len: 0,
                runtime_offset: 0,
                runtime_len: 0,
                tx_count: 0,
            })),
            WINCODE_ARCHIVE_V2_RECORD_INDEX_TAG
        );
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Footer(
                WincodeArchiveV2Footer::default()
            )),
            WINCODE_ARCHIVE_V2_RECORD_FOOTER_TAG
        );
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Genesis(WincodeArchiveV2Genesis {
                genesis_hash: [0; 32],
                genesis_bin_len: 0,
                creation_time_unix: 0,
                cluster_id: 0,
                ticks_per_slot: 0,
                poh_params: WincodeArchiveV2GenesisPohParams {
                    tick_duration_secs: 0,
                    tick_duration_nanos: 0,
                    tick_count: None,
                    hashes_per_tick: None,
                },
                fees: WincodeArchiveV2GenesisFeeParams {
                    target_lamports_per_sig: 0,
                    target_sigs_per_slot: 0,
                    min_lamports_per_sig: 0,
                    max_lamports_per_sig: 0,
                    burn_percent: 0,
                },
                rent: WincodeArchiveV2GenesisRentParams {
                    lamports_per_byte_year: 0,
                    exemption_threshold: 0.0,
                    burn_percent: 0,
                },
                inflation: WincodeArchiveV2GenesisInflationParams {
                    initial: 0.0,
                    terminal: 0.0,
                    taper: 0.0,
                    foundation: 0.0,
                    foundation_term: 0.0,
                    padding: [0; 8],
                },
                epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
                    slots_per_epoch: 0,
                    leader_schedule_slot_offset: 0,
                    warmup: false,
                    first_normal_epoch: 0,
                    first_normal_slot: 0,
                },
                accounts: Vec::new(),
                builtins: Vec::new(),
                reward_pools: Vec::new(),
            })),
            WINCODE_ARCHIVE_V2_RECORD_GENESIS_TAG
        );
    }

    fn archive_v2_record_tag(record: WincodeArchiveV2Record) -> u8 {
        wincode::config::serialize(&record, wincode_leb128_config()).unwrap()[0]
    }

    #[test]
    fn build_index_records_physical_block_offsets() {
        let dir = std::env::temp_dir().join(format!(
            "blockzilla-archive-v2-index-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let archive_path = dir.join("archive-v2.wincode");
        let index_path = dir.join("archive-v2.index");

        let block_record = WincodeArchiveV2Record::Block(empty_archive_v2_block(123));
        let block_bytes =
            wincode::config::serialize(&block_record, wincode_leb128_config()).unwrap();
        {
            let file = File::create(&archive_path).unwrap();
            let mut writer = WincodeLeb128FramedWriter::new(BufWriter::new(file));
            writer
                .write(&WincodeArchiveV2Record::Header(WincodeArchiveV2Header {
                    version: WINCODE_ARCHIVE_V2_VERSION,
                    flags: ARCHIVE_FLAGS_LEB128,
                }))
                .unwrap();
            writer.write_bytes(&block_bytes).unwrap();
            writer
                .write(&WincodeArchiveV2Record::Index(SplitCompactIndexRecord {
                    slot: 123,
                    block_id: 0,
                    block_offset: 0,
                    block_len: u32::try_from(block_bytes.len()).unwrap(),
                    runtime_offset: 0,
                    runtime_len: 0,
                    tx_count: 0,
                }))
                .unwrap();
            writer
                .write(&WincodeArchiveV2Record::Footer(WincodeArchiveV2Footer {
                    blocks: 1,
                    ..WincodeArchiveV2Footer::default()
                }))
                .unwrap();
            writer.flush().unwrap();
        }

        build_index(&archive_path, Some(&index_path)).unwrap();
        let index = read_archive_v2_block_index(&index_path).unwrap();
        assert_eq!(index.rows.len(), 1);
        let row = index.rows[0];
        assert_eq!(row.block_id, 0);
        assert_eq!(row.slot, 123);
        assert_eq!(row.payload_len as usize, block_bytes.len());
        assert_eq!(row.tx_count, 0);
        assert!(row.frame_offset < row.payload_offset);

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn empty_archive_v2_block(slot: u64) -> WincodeArchiveV2Block {
        WincodeArchiveV2Block {
            header: WincodeArchiveV2BlockHeader {
                compact: CompactBlockHeader {
                    slot,
                    parent_slot: 0,
                    blockhash: 0,
                    previous_blockhash: 0,
                    block_time: None,
                    block_height: None,
                    shredding: Vec::new(),
                    poh_entries: Vec::new(),
                    rewards: None,
                },
                rewards: None,
            },
            txs: Vec::new(),
        }
    }

    #[test]
    fn rolling_blockhash_resolves_block_id_and_falls_back_to_nonce() {
        let mut rolling = RollingBlockhashIndex::new(300);
        let hash = [7u8; 32];
        rolling.insert(hash, 42, 100).unwrap();

        match rolling.resolve_or_nonce(&hash, 101, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 42),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }

        let missing = [8u8; 32];
        match rolling.resolve_or_nonce(&missing, 101, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(_) => panic!("unexpected blockhash id"),
            OwnedCompactRecentBlockhash::Nonce(nonce) => assert_eq!(nonce, missing),
        }
    }

    #[test]
    fn rolling_blockhash_evicts_old_hashes() {
        let mut rolling = RollingBlockhashIndex::new(2);
        let first = [1u8; 32];
        let second = [2u8; 32];
        let third = [3u8; 32];

        rolling.insert(first, 1, 1).unwrap();
        rolling.insert(second, 2, 2).unwrap();
        rolling.insert(third, 3, 3).unwrap();

        match rolling.resolve_or_nonce(&first, 3, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(_) => panic!("unexpected blockhash id"),
            OwnedCompactRecentBlockhash::Nonce(nonce) => assert_eq!(nonce, first),
        }
        match rolling.resolve_or_nonce(&second, 3, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 2),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }
        match rolling.resolve_or_nonce(&third, 3, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 3),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }
    }

    #[test]
    fn rolling_blockhash_uses_nonce_for_hash_outside_slot_window() {
        let mut rolling = RollingBlockhashIndex::new(300);
        let hash = [7u8; 32];
        rolling.insert(hash, 42, 10).unwrap();

        match rolling.resolve_or_nonce(&hash, 160, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 42),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }
        match rolling.resolve_or_nonce(&hash, 161, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(_) => panic!("unexpected blockhash id"),
            OwnedCompactRecentBlockhash::Nonce(nonce) => assert_eq!(nonce, hash),
        }
    }

    #[test]
    fn rolling_blockhash_rejects_future_hash() {
        let mut rolling = RollingBlockhashIndex::new(300);
        let hash = [7u8; 32];
        rolling.insert(hash, 42, 20).unwrap();

        let err = rolling.resolve_or_nonce(&hash, 19, 0).unwrap_err();
        assert!(err.to_string().contains("future block slot"));
    }

    #[test]
    fn optimize_no_registry_tx_uses_required_recent_block_id() {
        let account = [9u8; 32];
        let recent_hash = [4u8; 32];
        let key_index = KeyIndex::build(vec![account]);
        let mut rolling = RollingBlockhashIndex::new(300);
        rolling.insert(recent_hash, 7, 9).unwrap();

        let tx = WincodeArchiveV2NoRegistryTx {
            signatures: vec![vec![1u8; 64]],
            message: WincodeArchiveV2NoRegistryMessage::Legacy(
                WincodeArchiveV2NoRegistryLegacyMessage {
                    header: CompactMessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys: vec![account],
                    recent_blockhash: recent_hash,
                    instructions: Vec::new(),
                },
            ),
        };

        let mut nonce_recent_blockhashes = 0;
        let optimized = optimize_no_registry_tx(
            10,
            0,
            tx,
            &key_index,
            &rolling,
            &mut nonce_recent_blockhashes,
        )
        .unwrap();
        let OwnedCompactMessage::Legacy(message) = optimized.message else {
            panic!("expected legacy message");
        };
        assert_eq!(message.account_keys, vec![CompactPubkey::id(1)]);
        assert_eq!(nonce_recent_blockhashes, 0);
        match message.recent_blockhash {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 7),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }
    }

    #[test]
    fn optimize_no_registry_tx_uses_nonce_for_missing_recent_blockhash() {
        let account = [9u8; 32];
        let recent_hash = [4u8; 32];
        let key_index = KeyIndex::build(vec![account]);
        let rolling = RollingBlockhashIndex::new(300);

        let tx = WincodeArchiveV2NoRegistryTx {
            signatures: vec![vec![1u8; 64]],
            message: WincodeArchiveV2NoRegistryMessage::Legacy(
                WincodeArchiveV2NoRegistryLegacyMessage {
                    header: CompactMessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys: vec![account],
                    recent_blockhash: recent_hash,
                    instructions: Vec::new(),
                },
            ),
        };

        let mut nonce_recent_blockhashes = 0;
        let optimized = optimize_no_registry_tx(
            10,
            0,
            tx,
            &key_index,
            &rolling,
            &mut nonce_recent_blockhashes,
        )
        .unwrap();
        let OwnedCompactMessage::Legacy(message) = optimized.message else {
            panic!("expected legacy message");
        };
        assert_eq!(nonce_recent_blockhashes, 1);
        match message.recent_blockhash {
            OwnedCompactRecentBlockhash::Id(_) => panic!("unexpected blockhash id"),
            OwnedCompactRecentBlockhash::Nonce(nonce) => assert_eq!(nonce, recent_hash),
        }
    }

    #[test]
    fn owned_compact_transaction_moves_v0_instruction_and_lookup_buffers() {
        let signature = [0x11; 64];
        let account_key = [0x22; 32];
        let lookup_key = [0x33; 32];
        let recent_blockhash = [0x44; 32];
        let instruction = CompiledInstruction {
            program_id_index: 0,
            accounts: vec![1, 3, 5, 7],
            data: vec![2, 4, 6, 8, 10],
        };
        let instruction_accounts_ptr = instruction.accounts.as_ptr();
        let instruction_data_ptr = instruction.data.as_ptr();
        let lookup = MessageAddressTableLookup {
            account_key: &lookup_key,
            writable_indexes: vec![9, 11, 13],
            readonly_indexes: vec![10, 12],
        };
        let writable_indexes_ptr = lookup.writable_indexes.as_ptr();
        let readonly_indexes_ptr = lookup.readonly_indexes.as_ptr();
        let transaction = VersionedTransaction {
            signatures: vec![&signature],
            message: VersionedMessage::V0(V0Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![&account_key],
                recent_blockhash: &recent_blockhash,
                instructions: vec![instruction],
                address_table_lookups: vec![lookup],
            }),
        };

        let key_index = KeyIndex::build(Vec::new());
        let rolling = RollingBlockhashIndex::new(300);
        let mut nonce_recent_blockhashes = 0;
        let compact = to_owned_compact_transaction(
            42,
            7,
            transaction,
            &key_index,
            &rolling,
            &mut nonce_recent_blockhashes,
        )
        .unwrap();

        assert_eq!(compact.signatures, vec![signature.to_vec()]);
        assert_eq!(nonce_recent_blockhashes, 1);
        let OwnedCompactMessage::V0(message) = compact.message else {
            panic!("expected v0 message")
        };
        assert_eq!(message.header.num_required_signatures, 1);
        assert_eq!(message.header.num_readonly_unsigned_accounts, 1);
        assert_eq!(message.account_keys, vec![CompactPubkey::raw(account_key)]);
        match message.recent_blockhash {
            OwnedCompactRecentBlockhash::Nonce(nonce) => assert_eq!(nonce, recent_blockhash),
            OwnedCompactRecentBlockhash::Id(_) => panic!("unexpected recent blockhash id"),
        }
        assert_eq!(message.instructions.len(), 1);
        assert_eq!(message.instructions[0].program_id_index, 0);
        assert_eq!(message.instructions[0].accounts, vec![1, 3, 5, 7]);
        assert_eq!(message.instructions[0].data, vec![2, 4, 6, 8, 10]);
        assert_eq!(
            message.instructions[0].accounts.as_ptr(),
            instruction_accounts_ptr
        );
        assert_eq!(message.instructions[0].data.as_ptr(), instruction_data_ptr);
        assert_eq!(message.address_table_lookups.len(), 1);
        assert_eq!(
            message.address_table_lookups[0].account_key,
            CompactPubkey::raw(lookup_key)
        );
        assert_eq!(
            message.address_table_lookups[0].writable_indexes,
            vec![9, 11, 13]
        );
        assert_eq!(
            message.address_table_lookups[0].readonly_indexes,
            vec![10, 12]
        );
        assert_eq!(
            message.address_table_lookups[0].writable_indexes.as_ptr(),
            writable_indexes_ptr
        );
        assert_eq!(
            message.address_table_lookups[0].readonly_indexes.as_ptr(),
            readonly_indexes_ptr
        );
    }

    fn test_shortu16(value: u16) -> Vec<u8> {
        let mut remaining = value;
        let mut encoded = Vec::with_capacity(3);
        loop {
            let byte = (remaining & 0x7f) as u8;
            remaining >>= 7;
            encoded.push(byte | u8::from(remaining != 0) * 0x80);
            if remaining == 0 {
                return encoded;
            }
        }
    }

    fn test_legacy_transaction_bytes(signature_count: u16) -> Vec<u8> {
        let mut bytes = test_shortu16(signature_count);
        for signature_index in 0..signature_count {
            bytes.extend(std::iter::repeat_n(
                signature_index as u8,
                FIRST_SEEN_SIGNATURE_BYTES,
            ));
        }

        let required_signatures = u8::try_from(signature_count.min(127)).unwrap();
        bytes.extend_from_slice(&[required_signatures, 0, 0]);
        bytes.extend_from_slice(&test_shortu16(u16::from(required_signatures)));
        for account_index in 0..required_signatures {
            bytes.extend_from_slice(&[account_index.wrapping_add(0x40); 32]);
        }
        bytes.extend_from_slice(&[0x63; 32]);
        bytes.push(0);
        bytes
    }

    #[test]
    fn first_seen_direct_message_decode_matches_full_transaction_decode() {
        let transaction_bytes = test_legacy_transaction_bytes(2);
        let key_index = KeyIndex::build(Vec::new());
        let rolling = RollingBlockhashIndex::new(300);
        let mut direct_nonce_recent_blockhashes = 0;
        let direct = decode_first_seen_compact_transaction(
            42,
            7,
            &transaction_bytes,
            2,
            &key_index,
            &rolling,
            &mut direct_nonce_recent_blockhashes,
        )
        .unwrap();

        let decoded = wincode::deserialize::<VersionedTransaction<'_>>(&transaction_bytes).unwrap();
        let mut full_nonce_recent_blockhashes = 0;
        let full = to_owned_compact_transaction(
            42,
            7,
            decoded,
            &key_index,
            &rolling,
            &mut full_nonce_recent_blockhashes,
        )
        .unwrap();

        assert!(direct.signatures.is_empty());
        assert_eq!(full.signatures, vec![vec![0; 64], vec![1; 64]]);
        assert_eq!(direct_nonce_recent_blockhashes, 1);
        assert_eq!(full_nonce_recent_blockhashes, 1);
        assert_eq!(
            wincode::config::serialize(&direct.message, wincode_leb128_config()).unwrap(),
            wincode::config::serialize(&full.message, wincode_leb128_config()).unwrap(),
        );
    }

    #[test]
    fn first_seen_direct_message_decode_validates_shortu16_boundaries_and_truncation() {
        let key_index = KeyIndex::build(Vec::new());
        let rolling = RollingBlockhashIndex::new(300);
        let mut nonce_recent_blockhashes = 0;
        let accepted_255 = decode_first_seen_compact_transaction(
            42,
            7,
            &test_legacy_transaction_bytes(255),
            255,
            &key_index,
            &rolling,
            &mut nonce_recent_blockhashes,
        )
        .unwrap();
        let OwnedCompactMessage::Legacy(message) = accepted_255.message else {
            panic!("expected legacy message")
        };
        assert_eq!(message.header.num_required_signatures, 127);
        assert_eq!(message.account_keys.len(), 127);

        for (transaction_bytes, expected_count, expected_error) in [
            (
                test_legacy_transaction_bytes(256),
                0,
                "signature count exceeds hot-row u8 maximum",
            ),
            (
                vec![0x80],
                0,
                "invalid transaction signature ShortU16 prefix",
            ),
            (
                vec![0x80, 0x00],
                0,
                "invalid transaction signature ShortU16 prefix",
            ),
            (
                std::iter::once(1)
                    .chain(std::iter::repeat_n(0, 63))
                    .collect(),
                1,
                "truncated transaction signatures",
            ),
            (
                test_legacy_transaction_bytes(2),
                1,
                "decoded signature count 2 does not match collected count 1",
            ),
        ] {
            let error = decode_first_seen_compact_transaction(
                42,
                7,
                &transaction_bytes,
                expected_count,
                &key_index,
                &rolling,
                &mut nonce_recent_blockhashes,
            )
            .expect_err("invalid first-seen transaction prefix must fail");
            assert!(
                error.to_string().contains(expected_error),
                "unexpected error: {error:#}"
            );
        }
    }

    #[test]
    fn no_registry_transaction_moves_v0_instruction_and_lookup_buffers() {
        let signature = [0x51; 64];
        let account_key = [0x52; 32];
        let lookup_key = [0x53; 32];
        let recent_blockhash = [0x54; 32];
        let instruction = CompiledInstruction {
            program_id_index: 2,
            accounts: vec![21, 23],
            data: vec![22, 24, 26],
        };
        let instruction_accounts_ptr = instruction.accounts.as_ptr();
        let instruction_data_ptr = instruction.data.as_ptr();
        let lookup = MessageAddressTableLookup {
            account_key: &lookup_key,
            writable_indexes: vec![31, 33],
            readonly_indexes: vec![32, 34, 36],
        };
        let writable_indexes_ptr = lookup.writable_indexes.as_ptr();
        let readonly_indexes_ptr = lookup.readonly_indexes.as_ptr();
        let transaction = VersionedTransaction {
            signatures: vec![&signature],
            message: VersionedMessage::V0(V0Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 2,
                },
                account_keys: vec![&account_key],
                recent_blockhash: &recent_blockhash,
                instructions: vec![instruction],
                address_table_lookups: vec![lookup],
            }),
        };

        let no_registry = to_no_registry_transaction(transaction);
        assert_eq!(no_registry.signatures, vec![signature.to_vec()]);
        let WincodeArchiveV2NoRegistryMessage::V0(message) = no_registry.message else {
            panic!("expected no-registry v0 message")
        };
        assert_eq!(message.header.num_required_signatures, 1);
        assert_eq!(message.header.num_readonly_unsigned_accounts, 2);
        assert_eq!(message.account_keys, vec![account_key]);
        assert_eq!(message.recent_blockhash, recent_blockhash);
        assert_eq!(message.instructions.len(), 1);
        assert_eq!(message.instructions[0].program_id_index, 2);
        assert_eq!(message.instructions[0].accounts, vec![21, 23]);
        assert_eq!(message.instructions[0].data, vec![22, 24, 26]);
        assert_eq!(
            message.instructions[0].accounts.as_ptr(),
            instruction_accounts_ptr
        );
        assert_eq!(message.instructions[0].data.as_ptr(), instruction_data_ptr);
        assert_eq!(message.address_table_lookups.len(), 1);
        assert_eq!(message.address_table_lookups[0].account_key, lookup_key);
        assert_eq!(
            message.address_table_lookups[0].writable_indexes,
            vec![31, 33]
        );
        assert_eq!(
            message.address_table_lookups[0].readonly_indexes,
            vec![32, 34, 36]
        );
        assert_eq!(
            message.address_table_lookups[0].writable_indexes.as_ptr(),
            writable_indexes_ptr
        );
        assert_eq!(
            message.address_table_lookups[0].readonly_indexes.as_ptr(),
            readonly_indexes_ptr
        );
    }

    #[test]
    fn rekey_log_event_rewrites_nested_raw_pubkeys() {
        let system_provided = [1u8; 32];
        let system_derived = [2u8; 32];
        let token_account = [3u8; 32];
        let program = [4u8; 32];
        let key_index = KeyIndex::build(vec![
            system_provided,
            system_derived,
            token_account,
            program,
        ]);

        let mut system_event = LogEvent::System(SystemProgramLog::CreateAddressMismatch {
            provided_addr: CompactPubkey::raw(system_provided),
            derived_addr: PubkeyOrString::Pubkey(CompactPubkey::raw(system_derived)),
        });
        rekey_log_event(&mut system_event, &key_index);
        match system_event {
            LogEvent::System(SystemProgramLog::CreateAddressMismatch {
                provided_addr,
                derived_addr,
            }) => {
                assert_eq!(provided_addr, CompactPubkey::id(1));
                assert_eq!(derived_addr, PubkeyOrString::Pubkey(CompactPubkey::id(2)));
            }
            other => panic!("unexpected system event: {other:?}"),
        }

        let mut program_event = LogEvent::ProgramIdLog {
            program: CompactPubkey::raw(program),
            log: ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom {
                account_key: CompactPubkey::raw(token_account),
                error: 0,
            }),
        };
        rekey_log_event(&mut program_event, &key_index);
        match program_event {
            LogEvent::ProgramIdLog {
                program,
                log: ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom { account_key, .. }),
            } => {
                assert_eq!(program, CompactPubkey::id(4));
                assert_eq!(account_key, CompactPubkey::id(3));
            }
            other => panic!("unexpected program event: {other:?}"),
        }
    }

    #[test]
    fn hot_vote_parser_compacts_canonical_solana_vote_instructions() {
        let root = 1_000u64;
        let last_slot = root + 31;
        let mut lockouts = VecDeque::new();
        lockouts.push_back(Lockout::new_with_confirmation_count(root + 1, 31));
        lockouts.push_back(Lockout::new_with_confirmation_count(last_slot, 1));
        let mut update = SolanaVoteStateUpdate::new(lockouts.clone(), Some(root), [7u8; 32].into());
        update.timestamp = Some(1_234_567);

        let mut slots = GxHashMap::with_hasher(GxBuildHasher::default());
        slots.insert(last_slot, 42);

        let data =
            bincode::serialize(&VoteInstruction::CompactUpdateVoteState(update.clone())).unwrap();
        assert_eq!(vote_instruction_variant_label(&data), "12");
        let mut vote_hashes = VoteHashRegistryBuilder::default();
        let parsed = parse_hot_vote_instruction_data(&data, &slots, &mut vote_hashes).unwrap();
        let ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(parsed_update) = parsed else {
            panic!("expected CompactUpdateVoteState");
        };
        assert_eq!(parsed_update.root, Some(root));
        assert_eq!(parsed_update.lockout_offsets.len(), 2);
        assert_eq!(parsed_update.lockout_offsets[0].offset, 1);
        assert_eq!(parsed_update.lockout_offsets[1].offset, 30);
        assert_eq!(parsed_update.timestamp, Some(1_234_567));
        assert_eq!(parsed_update.hash, ArchiveV2VoteHashRef::Block(42));
        assert_eq!(vote_hashes.rows[42].bank_hash, Some([7u8; 32]));

        let mut tower =
            SolanaTowerSync::new(lockouts, Some(root), [7u8; 32].into(), [8u8; 32].into());
        tower.timestamp = Some(1_234_567);
        let data = bincode::serialize(&VoteInstruction::TowerSync(tower)).unwrap();
        assert_eq!(vote_instruction_variant_label(&data), "14");
        let mut vote_hashes = VoteHashRegistryBuilder::default();
        let parsed = parse_hot_vote_instruction_data(&data, &slots, &mut vote_hashes).unwrap();
        let ArchiveV2HotInstructionData::VoteTowerSync(tower) = parsed else {
            panic!("expected TowerSync");
        };
        assert_eq!(tower.update.root, Some(root));
        assert_eq!(tower.update.lockout_offsets.len(), 2);
        assert_eq!(tower.update.timestamp, Some(1_234_567));
        assert_eq!(tower.update.hash, ArchiveV2VoteHashRef::Block(42));
        assert_eq!(tower.block_id_hash, ArchiveV2VoteHashRef::Block(42));
        assert_eq!(vote_hashes.rows[42].bank_hash, Some([7u8; 32]));
        assert_eq!(vote_hashes.rows[42].block_id_hash, Some([8u8; 32]));
    }

    #[test]
    fn hot_vote_parser_compacts_historical_tower_sync_wire_form() {
        let data = test_hex_bytes(
            "0e0000000200000000000000adbf62180000000001000000bcbf6218000000000000000000f2b429ebfc9b4b2e1ee705e8d03d1bad2198ae2dc41a8b8e27a7ec0dab033621019c365d2d9d010000f2b429ebfc9b4b2e1ee705e8d03d1bad2198ae2dc41a8b8e27a7ec0dab033621",
        );
        let mut slots = GxHashMap::with_hasher(GxBuildHasher::default());
        slots.insert(409_124_796, 17);
        let mut vote_hashes = VoteHashRegistryBuilder::default();

        let parsed = parse_hot_vote_instruction_data(&data, &slots, &mut vote_hashes).unwrap();
        let ArchiveV2HotInstructionData::VoteTowerSync(tower) = parsed else {
            panic!("expected historical TowerSync");
        };
        assert_eq!(tower.update.root, None);
        assert_eq!(tower.update.lockout_offsets.len(), 2);
        assert_eq!(tower.update.lockout_offsets[0].offset, 409_124_781);
        assert_eq!(tower.update.lockout_offsets[0].confirmation_count, 1);
        assert_eq!(tower.update.lockout_offsets[1].offset, 15);
        assert_eq!(tower.update.lockout_offsets[1].confirmation_count, 0);
        assert_eq!(tower.update.timestamp, Some(1_774_582_576_796));
        assert_eq!(tower.update.hash, ArchiveV2VoteHashRef::Block(17));
        assert_eq!(tower.block_id_hash, ArchiveV2VoteHashRef::Block(17));
        assert_eq!(
            vote_hashes.rows[17].bank_hash,
            Some([
                0xf2, 0xb4, 0x29, 0xeb, 0xfc, 0x9b, 0x4b, 0x2e, 0x1e, 0xe7, 0x05, 0xe8, 0xd0, 0x3d,
                0x1b, 0xad, 0x21, 0x98, 0xae, 0x2d, 0xc4, 0x1a, 0x8b, 0x8e, 0x27, 0xa7, 0xec, 0x0d,
                0xab, 0x03, 0x36, 0x21,
            ])
        );
        assert_eq!(
            vote_hashes.rows[17].block_id_hash,
            vote_hashes.rows[17].bank_hash
        );
    }

    #[test]
    fn hot_compute_budget_parser_compacts_observed_wire_forms() {
        let parsed =
            parse_hot_compute_budget_instruction_data(&[3, 0x10, 0x27, 0, 0, 0, 0, 0, 0]).unwrap();
        let ArchiveV2HotInstructionData::ComputeBudget(
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(price),
        ) = parsed
        else {
            panic!("expected compact compute budget price");
        };
        assert_eq!(price, 10_000);

        let parsed = parse_hot_compute_budget_instruction_data(&[2, 0xf1, 0x01, 0, 0]).unwrap();
        let ArchiveV2HotInstructionData::ComputeBudget(
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(limit),
        ) = parsed
        else {
            panic!("expected compact compute unit limit");
        };
        assert_eq!(limit, 497);
    }

    #[test]
    fn hot_system_parser_compacts_common_bincode_forms() {
        let mut transfer = Vec::new();
        transfer.extend_from_slice(&2u32.to_le_bytes());
        transfer.extend_from_slice(&1u64.to_le_bytes());
        let parsed = parse_hot_system_instruction_data(&transfer).unwrap();
        let ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
            lamports,
        }) = parsed
        else {
            panic!("expected compact system transfer");
        };
        assert_eq!(lamports, 1);

        let parsed = parse_hot_system_instruction_data(&4u32.to_le_bytes()).unwrap();
        assert!(matches!(
            parsed,
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AdvanceNonceAccount
            )
        ));
    }

    #[test]
    fn hot_system_parser_compacts_seed_forms_without_extra_allocation_guessing() {
        let mut create = Vec::new();
        create.extend_from_slice(&3u32.to_le_bytes());
        create.extend_from_slice(&[1u8; 32]);
        create.extend_from_slice(&3u64.to_le_bytes());
        create.extend_from_slice(b"abc");
        create.extend_from_slice(&5u64.to_le_bytes());
        create.extend_from_slice(&9u64.to_le_bytes());
        create.extend_from_slice(&[2u8; 32]);

        let parsed = parse_hot_system_instruction_data(&create).unwrap();
        let ArchiveV2HotInstructionData::System(
            ArchiveV2SystemInstructionData::CreateAccountWithSeed {
                base,
                seed,
                lamports,
                space,
                owner,
            },
        ) = parsed
        else {
            panic!("expected compact create account with seed");
        };
        assert_eq!(base, [1u8; 32]);
        assert_eq!(seed, "abc");
        assert_eq!(lamports, 5);
        assert_eq!(space, 9);
        assert_eq!(owner, [2u8; 32]);
    }

    #[test]
    fn hot_known_program_parsers_preserve_uncompact_unknown_payloads_raw() {
        let parsed = parse_hot_compute_budget_instruction_data(&[255, 1, 2, 3]).unwrap();
        assert!(
            matches!(parsed, ArchiveV2HotInstructionData::Raw(raw) if raw == vec![255, 1, 2, 3])
        );

        let noncanonical_compute_unit_limit = [2, 0x40, 0x42, 0x0f, 0, 0, 0, 0, 0].to_vec();
        let parsed =
            parse_hot_compute_budget_instruction_data(&noncanonical_compute_unit_limit).unwrap();
        assert!(
            matches!(parsed, ArchiveV2HotInstructionData::Raw(raw) if raw == noncanonical_compute_unit_limit)
        );

        let mut unknown_system = Vec::new();
        unknown_system.extend_from_slice(&99u32.to_le_bytes());
        unknown_system.extend_from_slice(&[1, 2, 3]);
        let parsed = parse_hot_system_instruction_data(&unknown_system).unwrap();
        assert!(
            matches!(parsed, ArchiveV2HotInstructionData::UnknownSystem(raw) if raw == unknown_system)
        );

        let noncanonical_system_transfer = vec![
            0x02, 0x00, 0x00, 0x00, 0x00, 0x0e, 0x27, 0x07, 0x00, 0x00, 0x00, 0x00, 0xcf, 0x18,
            0x15, 0x58, 0xd4, 0x4e, 0x99, 0x18,
        ];
        let parsed = parse_hot_system_instruction_data(&noncanonical_system_transfer).unwrap();
        assert!(
            matches!(parsed, ArchiveV2HotInstructionData::UnknownSystem(raw) if raw == noncanonical_system_transfer)
        );

        let parsed = parse_hot_system_instruction_data(&[]).unwrap();
        assert!(
            matches!(parsed, ArchiveV2HotInstructionData::UnknownSystem(raw) if raw.is_empty())
        );

        let truncated_system_transfer = vec![2, 0, 0];
        let parsed = parse_hot_system_instruction_data(&truncated_system_transfer).unwrap();
        assert!(
            matches!(parsed, ArchiveV2HotInstructionData::UnknownSystem(raw) if raw == truncated_system_transfer)
        );

        let unknown_vote = hex_to_vec(
            "0e0000000001a0afe1c0011f1029d1fd40bb7c95deb2380109cbb9cd198ec64594369d169f0a9d7af7e2aa3001b4b1a869000000001029d1fd40bb7c95deb2380109cbb9cd198ec64594369d169f0a9d7af7e2aa30",
        );
        let slots = GxHashMap::with_hasher(GxBuildHasher::default());
        let mut vote_hashes = VoteHashRegistryBuilder::default();
        let parsed = parse_hot_vote_instruction_data(&unknown_vote, &slots, &mut vote_hashes)
            .expect("invalid on-chain vote payload should preserve as UnknownVote");
        assert!(
            matches!(parsed, ArchiveV2HotInstructionData::UnknownVote(raw) if raw == unknown_vote)
        );
        assert_eq!(vote_hashes.compact_vote_ix, 0);
        assert!(vote_hashes.rows.is_empty());

        let root = 1_000u64;
        let mut lockouts = VecDeque::new();
        for i in 1..=32 {
            lockouts.push_back(Lockout::new_with_confirmation_count(root + i, 1));
        }
        let update = SolanaVoteStateUpdate::new(lockouts, Some(root), [7u8; 32].into());
        let too_many_lockouts =
            bincode::serialize(&VoteInstruction::CompactUpdateVoteState(update)).unwrap();
        let mut vote_hashes = VoteHashRegistryBuilder::default();
        let parsed = parse_hot_vote_instruction_data(&too_many_lockouts, &slots, &mut vote_hashes)
            .expect("uncompactable valid vote payload should preserve as UnknownVote");
        assert!(
            matches!(parsed, ArchiveV2HotInstructionData::UnknownVote(raw) if raw == too_many_lockouts)
        );
        assert_eq!(vote_hashes.compact_vote_ix, 0);
        assert!(vote_hashes.rows.is_empty());

        let mut vote_hashes = VoteHashRegistryBuilder::default();
        let parsed = parse_hot_vote_instruction_data(&[], &slots, &mut vote_hashes)
            .expect("observed empty on-chain vote payload should preserve as UnknownVote");
        assert!(matches!(
            parsed,
            ArchiveV2HotInstructionData::UnknownVote(raw) if raw.is_empty()
        ));
        assert_eq!(vote_hashes.compact_vote_ix, 0);
        assert!(vote_hashes.rows.is_empty());
    }

    #[test]
    fn hot_known_program_parsers_reject_invalid_instruction_payloads() {
        let err = parse_hot_compute_budget_instruction_data(&[2, 1])
            .expect_err("truncated compute budget payload must hard fail");
        assert!(format!("{err:#}").contains("parse compute budget instruction data"));

        let err = parse_hot_compute_budget_instruction_data(&[])
            .expect_err("empty compute budget payload must hard fail");
        assert!(format!("{err:#}").contains("empty compute budget instruction data"));
    }

    #[test]
    fn hot_vote_parser_rejects_invalid_instruction_payloads() {
        let mut slots = GxHashMap::with_hasher(GxBuildHasher::default());
        slots.insert(1_000, 42);
        let mut vote_hashes = VoteHashRegistryBuilder::default();

        let mut eof_vote = 12u32.to_le_bytes().to_vec();
        eof_vote.extend_from_slice(&0u64.to_le_bytes());
        eof_vote.push(0);
        let err = parse_hot_vote_instruction_data(&eof_vote, &slots, &mut vote_hashes)
            .expect_err("invalid vote payload must hard fail");
        assert!(format!("{err:#}").contains("canonical vote instruction decode failed"));

        let mut overflowing_short_vec = 12u32.to_le_bytes().to_vec();
        overflowing_short_vec.extend_from_slice(&0u64.to_le_bytes());
        overflowing_short_vec.extend_from_slice(&[0xff, 0xff, 0x7c]);
        let err = parse_hot_vote_instruction_data(&overflowing_short_vec, &slots, &mut vote_hashes)
            .expect_err("invalid vote payload must hard fail");
        assert!(format!("{err:#}").contains("canonical vote instruction decode failed"));

        assert_eq!(vote_hashes.compact_vote_ix, 0);
        assert!(vote_hashes.rows.is_empty());
    }

    #[test]
    fn vote_hash_registry_keeps_conflicting_tower_block_id_hash_raw() {
        let mut slots = GxHashMap::with_hasher(GxBuildHasher::default());
        slots.insert(1_000, 42);
        let mut vote_hashes = VoteHashRegistryBuilder::default();

        let first = vote_hashes
            .ref_block_id_hash(Some(1_000), [8u8; 32], &slots)
            .unwrap();
        let second = vote_hashes
            .ref_block_id_hash(Some(1_000), [9u8; 32], &slots)
            .unwrap();

        assert_eq!(first, ArchiveV2VoteHashRef::Block(42));
        assert_eq!(second, ArchiveV2VoteHashRef::Raw([9u8; 32]));
        assert_eq!(vote_hashes.rows[42].block_id_hash, Some([8u8; 32]));
        assert_eq!(vote_hashes.block_id_refs, 1);
        assert_eq!(vote_hashes.block_id_raw, 1);
        assert_eq!(vote_hashes.block_id_conflict_raw, 1);
    }

    #[test]
    fn reserve_total_capacity_grows_from_a_partially_sized_buffer() {
        let mut values = Vec::<u8>::with_capacity(8);
        reserve_total_capacity(&mut values, 12);
        assert!(values.capacity() >= 12);
    }

    #[test]
    fn hot_instruction_scratch_reuses_pointer_and_capacity_across_messages() {
        fn message(v0: bool, instruction_count: usize, seed: u8) -> OwnedCompactMessage {
            let instructions = (0..instruction_count)
                .map(|index| OwnedCompactInstruction {
                    program_id_index: 0,
                    accounts: vec![index as u8],
                    data: vec![seed, index as u8],
                })
                .collect();
            let header = CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            };
            let account_keys = vec![CompactPubkey::raw([seed; 32])];
            let recent_blockhash = OwnedCompactRecentBlockhash::Nonce([seed; 32]);
            if v0 {
                OwnedCompactMessage::V0(OwnedCompactV0Message {
                    header,
                    account_keys,
                    recent_blockhash,
                    instructions,
                    address_table_lookups: Vec::new(),
                })
            } else {
                OwnedCompactMessage::Legacy(OwnedCompactLegacyMessage {
                    header,
                    account_keys,
                    recent_blockhash,
                    instructions,
                })
            }
        }

        fn instructions(message: &ArchiveV2HotMessagePayload) -> &Vec<ArchiveV2HotInstruction> {
            match message {
                ArchiveV2HotMessagePayload::Legacy(message) => &message.instructions,
                ArchiveV2HotMessagePayload::V0(message) => &message.instructions,
            }
        }

        let known_program_ids = KnownProgramIds {
            vote: None,
            compute_budget: None,
            system: None,
        };
        let slot_to_block_id = GxHashMap::with_hasher(GxBuildHasher::default());
        let mut buffers = HotBlockBuffers::default();

        let (first, first_flags) = hot_message_from_owned_with_instruction_scratch(
            message(false, 4, 0x51),
            &mut buffers.hot_instructions,
            &known_program_ids,
            &slot_to_block_id,
            &mut VoteHashRegistryBuilder::default(),
        )
        .unwrap();
        assert_eq!(first_flags, 0);
        assert_eq!(instructions(&first).len(), 4);
        let pointer = instructions(&first).as_ptr();
        let capacity = instructions(&first).capacity();
        assert!(capacity >= 4);
        assert!(
            !wincode::config::serialize(&first, wincode_leb128_config())
                .unwrap()
                .is_empty()
        );
        buffers.recycle_message_instructions(first);
        assert_eq!(buffers.hot_instructions.as_ptr(), pointer);
        assert_eq!(buffers.hot_instructions.capacity(), capacity);
        assert!(buffers.hot_instructions.is_empty());

        let (second, second_flags) = hot_message_from_owned_with_instruction_scratch(
            message(true, 2, 0x52),
            &mut buffers.hot_instructions,
            &known_program_ids,
            &slot_to_block_id,
            &mut VoteHashRegistryBuilder::default(),
        )
        .unwrap();
        assert_eq!(second_flags, ARCHIVE_V2_TX_FLAG_MESSAGE_V0);
        assert_eq!(instructions(&second).len(), 2);
        assert_eq!(instructions(&second).as_ptr(), pointer);
        assert_eq!(instructions(&second).capacity(), capacity);
        assert!(
            !wincode::config::serialize(&second, wincode_leb128_config())
                .unwrap()
                .is_empty()
        );
        buffers.recycle_message_instructions(second);
        assert_eq!(buffers.hot_instructions.as_ptr(), pointer);
        assert_eq!(buffers.hot_instructions.capacity(), capacity);
        assert!(buffers.hot_instructions.is_empty());
    }

    #[test]
    fn raw_dataframe_payload_pool_reuses_without_growth_within_size_class() {
        assert_eq!(raw_dataframe_required_capacity_class(0), 0);
        assert_eq!(raw_dataframe_required_capacity_class(1), 1);
        assert_eq!(raw_dataframe_required_capacity_class(2), 2);
        assert_eq!(raw_dataframe_required_capacity_class(3), 3);
        assert_eq!(raw_dataframe_required_capacity_class(4), 3);
        assert_eq!(raw_dataframe_required_capacity_class(5), 4);

        let mut pool = RawDataFramePayloadPool::default();
        let mut first = pool.take(3);
        first.extend_from_slice(&[1, 2, 3]);
        pool.recycle(first);
        let second = pool.take(4);
        assert!(second.capacity() >= 4);
        pool.recycle(second);

        let stats = pool.stats();
        assert_eq!(stats.takes, 2);
        assert_eq!(stats.fresh_buffers, 1);
        assert_eq!(stats.reused_buffers, 1);
        assert_eq!(stats.allocation_events, 1);
        assert_eq!(stats.growth_events, 0);
        assert_eq!(stats.current_buffers, 0);
        assert_eq!(stats.current_capacity, 0);
        assert_eq!(stats.retained_buffers, 1);
        assert!(stats.retained_capacity >= 4);
        assert_eq!(stats.peak_current_buffers, 1);
    }

    #[test]
    fn raw_dataframe_payload_pool_does_not_accumulate_across_varied_rounds() {
        let lengths: Vec<usize> = (0..512).map(|index| (index * 37 % 1_500) + 1).collect();
        let mut pool = RawDataFramePayloadPool::default();

        for _ in 0..64 {
            let buffers: Vec<Vec<u8>> = lengths
                .iter()
                .map(|&required| pool.take(required))
                .collect();
            for buffer in buffers {
                pool.recycle(buffer);
            }
        }

        let stats = pool.stats();
        assert_eq!(stats.current_buffers, 0);
        assert_eq!(stats.current_capacity, 0);
        assert_eq!(stats.peak_current_buffers, lengths.len());
        assert_eq!(stats.retained_buffers, lengths.len());
        assert_eq!(stats.retained_capacity, stats.peak_current_capacity);
        assert_eq!(stats.fresh_buffers, lengths.len() as u64);
        assert_eq!(stats.growth_events, 0);
        assert_eq!(stats.discarded_buffers, 0);
        assert_eq!(stats.reused_buffers, (lengths.len() * 63) as u64,);
    }

    #[test]
    fn raw_dataframe_payload_pool_enforces_retention_limits() {
        let limits = RawDataFramePayloadPoolLimits {
            max_retained_capacity: 16,
            max_buffer_capacity: 8,
            max_buffers_per_class: 1,
            max_class_zero_buffers: 1,
        };
        let mut pool = RawDataFramePayloadPool::default();
        let first = pool.take(5);
        let second = pool.take(5);
        let oversized = pool.take(33);
        let zero_a = pool.take(0);
        let zero_b = pool.take(0);

        pool.recycle_with_limits(first, limits);
        pool.recycle_with_limits(second, limits);
        pool.recycle_with_limits(oversized, limits);
        pool.recycle_with_limits(zero_a, limits);
        pool.recycle_with_limits(zero_b, limits);

        let stats = pool.stats();
        assert_eq!(stats.current_buffers, 0);
        assert_eq!(stats.current_capacity, 0);
        assert_eq!(stats.retained_buffers, 2);
        assert_eq!(stats.retained_capacity, 8);
        assert_eq!(stats.discarded_buffers, 3);
        assert_eq!(stats.discarded_capacity, 72);
    }

    #[test]
    fn pending_block_recycles_all_dataframe_payloads_and_duplicate_cids() {
        fn take_payload(
            pool: &mut RawDataFramePayloadPool,
            len: usize,
            byte: u8,
        ) -> (Vec<u8>, *const u8) {
            let mut buffer = pool.take(len);
            buffer.resize(len, byte);
            let pointer = buffer.as_ptr();
            (buffer, pointer)
        }

        fn raw_frame(data: Vec<u8>) -> RawDataFrame {
            RawDataFrame {
                hash: None,
                hash_was_negative: false,
                index: None,
                total: None,
                data,
                next: Vec::new(),
            }
        }

        let mut pending = PendingBlock::default();
        let (tx_data, tx_data_ptr) = take_payload(&mut pending.raw_dataframe_payloads, 2, 0x12);
        let (metadata_data, metadata_ptr) =
            take_payload(&mut pending.raw_dataframe_payloads, 5, 0x15);
        let (rewards_data, rewards_ptr) =
            take_payload(&mut pending.raw_dataframe_payloads, 9, 0x19);
        let (old_standalone_data, old_standalone_ptr) =
            take_payload(&mut pending.raw_dataframe_payloads, 17, 0x21);
        let (new_standalone_data, new_standalone_ptr) =
            take_payload(&mut pending.raw_dataframe_payloads, 33, 0x33);

        let location = NodeLocation {
            entry_index: 1,
            car_offset: 2,
        };
        pending.transactions.push(PendingTx {
            tx: RawTransactionNode {
                location,
                cid: Cid36::from_car_bytes([1; 36]),
                slot: 42,
                index: Some(0),
                data: raw_frame(tx_data),
                metadata: raw_frame(metadata_data),
            },
            payload_len: 0,
        });
        pending.rewards = Some(PendingRewards {
            rewards: RawRewardsNode {
                location,
                cid: Cid36::from_car_bytes([2; 36]),
                slot: 42,
                data: raw_frame(rewards_data),
            },
            payload_len: 0,
        });

        let duplicate_cid = Cid36::from_car_bytes([3; 36]);
        pending
            .insert_dataframe_recycling(StandaloneDataFrame {
                location,
                cid: duplicate_cid,
                frame: raw_frame(old_standalone_data),
            })
            .unwrap();
        let duplicate_error = pending
            .insert_dataframe_recycling(StandaloneDataFrame {
                location,
                cid: duplicate_cid,
                frame: raw_frame(new_standalone_data),
            })
            .expect_err("duplicate standalone dataframe CID must fail");
        assert!(
            duplicate_error
                .to_string()
                .contains("duplicate standalone dataframe CID")
        );
        assert_eq!(pending.dataframes.len(), 1);
        assert_eq!(
            pending.dataframes[&duplicate_cid].frame.data,
            vec![0x21; 17]
        );

        pending.clear_recycling_frame_data();
        assert!(pending.transactions.is_empty());
        assert!(pending.entries.is_empty());
        assert!(pending.rewards.is_none());
        assert!(pending.dataframes.is_empty());
        let recycled = pending.raw_dataframe_payloads.stats();
        assert_eq!(recycled.current_buffers, 0);
        assert_eq!(recycled.current_capacity, 0);
        assert_eq!(recycled.retained_buffers, 5);
        assert_eq!(recycled.fresh_buffers, 5);
        assert_eq!(recycled.reused_buffers, 0);
        assert_eq!(recycled.peak_current_buffers, 5);

        let expected = [
            (2, tx_data_ptr),
            (5, metadata_ptr),
            (9, rewards_ptr),
            (17, old_standalone_ptr),
            (33, new_standalone_ptr),
        ];
        let mut taken = Vec::new();
        for (len, pointer) in expected {
            let buffer = pending.raw_dataframe_payloads.take(len);
            assert_eq!(buffer.as_ptr(), pointer);
            taken.push(buffer);
        }
        for buffer in taken {
            pending.raw_dataframe_payloads.recycle(buffer);
        }
        let reused = pending.raw_dataframe_payloads.stats();
        assert_eq!(reused.takes, 10);
        assert_eq!(reused.reused_buffers, 5);
        assert_eq!(reused.fresh_buffers, 5);
        assert_eq!(reused.current_buffers, 0);
        assert_eq!(reused.retained_buffers, 5);
    }

    #[test]
    fn pending_block_clear_retains_record_scratch_allocations() {
        let mut pending = PendingBlock::default();
        pending.record_scratch.tx_bytes.reserve(1_024);
        pending.record_scratch.metadata_bytes.reserve(2_048);
        pending.record_scratch.reassemble_visited.reserve(32);
        let capacities = (
            pending.record_scratch.tx_bytes.capacity(),
            pending.record_scratch.metadata_bytes.capacity(),
            pending.record_scratch.reassemble_visited.capacity(),
        );

        pending.clear();

        assert_eq!(pending.record_scratch.tx_bytes.capacity(), capacities.0);
        assert_eq!(
            pending.record_scratch.metadata_bytes.capacity(),
            capacities.1
        );
        assert_eq!(
            pending.record_scratch.reassemble_visited.capacity(),
            capacities.2
        );
    }

    #[test]
    fn dataframe_decode_bytes_borrows_direct_and_reassembles_continuations() {
        let direct = signature_raw_frame(vec![1, 2, 3, 4], Vec::new());
        let direct_pointer = direct.data.as_ptr();
        let mut scratch = Vec::with_capacity(128);
        scratch.extend_from_slice(&[0xaa; 32]);
        let direct_scratch_capacity = scratch.capacity();
        let mut visited = HashSet::from([Cid36::from_car_bytes([0x71; 36])]);

        let (bytes, reported_capacity) =
            dataframe_bytes_for_decode(&direct, &HashMap::new(), &mut scratch, &mut visited)
                .unwrap();
        assert_eq!(bytes, [1, 2, 3, 4]);
        assert_eq!(
            bytes.as_ptr(),
            direct_pointer,
            "direct frame must be borrowed"
        );
        assert_eq!(reported_capacity, direct_scratch_capacity);
        assert!(scratch.is_empty());
        assert!(visited.is_empty());

        let continuation_cid = Cid36::from_car_bytes([0x72; 36]);
        let location = NodeLocation {
            entry_index: 7,
            car_offset: 11,
        };
        let dataframes = HashMap::from([(
            continuation_cid,
            StandaloneDataFrame {
                location,
                cid: continuation_cid,
                frame: signature_raw_frame(vec![5, 6, 7], Vec::new()),
            },
        )]);
        let continued = signature_raw_frame(
            vec![1, 2, 3, 4],
            vec![RawCidRef::from_car_cid(continuation_cid)],
        );
        scratch.extend_from_slice(&[0xbb; 16]);
        visited.insert(Cid36::from_car_bytes([0x73; 36]));

        let (bytes, reported_capacity) =
            dataframe_bytes_for_decode(&continued, &dataframes, &mut scratch, &mut visited)
                .unwrap();
        assert_eq!(bytes, [1, 2, 3, 4, 5, 6, 7]);
        let assembled_pointer = bytes.as_ptr();
        assert_eq!(reported_capacity, scratch.capacity());
        assert_eq!(assembled_pointer, scratch.as_ptr());
        assert!(visited.is_empty());
    }

    fn signature_raw_frame(data: Vec<u8>, next: Vec<RawCidRef>) -> RawDataFrame {
        RawDataFrame {
            hash: None,
            hash_was_negative: false,
            index: None,
            total: None,
            data,
            next,
        }
    }

    fn raw_cid_ref_from_serialized_parts(
        cid: Option<Cid36>,
        normalized_bytes: &[u8],
        cbor_bytes: &[u8],
        tagged: bool,
    ) -> RawCidRef {
        #[derive(serde::Serialize)]
        struct SerializedRawCidRef<'a> {
            cid: Option<Cid36>,
            normalized_bytes: &'a [u8],
            cbor_bytes: &'a [u8],
            tagged: bool,
        }

        let encoded = postcard::to_allocvec(&SerializedRawCidRef {
            cid,
            normalized_bytes,
            cbor_bytes,
            tagged,
        })
        .unwrap();
        postcard::from_bytes(&encoded).unwrap()
    }

    fn inline_identity_raw_cid(bytes: &[u8]) -> RawCidRef {
        assert!(bytes.len() < 128);
        let mut normalized = vec![1, 0x55, 0, bytes.len() as u8];
        normalized.extend_from_slice(bytes);
        let mut cbor = vec![0];
        cbor.extend_from_slice(&normalized);
        raw_cid_ref_from_serialized_parts(None, &normalized, &cbor, true)
    }

    #[test]
    fn first_seen_signature_collector_validates_shortu16_and_reuses_arena() {
        let dataframes = HashMap::new();
        for (prefix, count) in [(vec![0], 0u8), (vec![2], 2), (vec![0x80, 0x01], 128)] {
            let signature_bytes = (0..usize::from(count) * FIRST_SEEN_SIGNATURE_BYTES)
                .map(|index| (index.wrapping_mul(37) & 0xff) as u8)
                .collect::<Vec<_>>();
            let mut transaction_bytes = prefix;
            transaction_bytes.extend_from_slice(&signature_bytes);
            transaction_bytes.extend_from_slice(b"message bytes must not be copied");
            let frame = signature_raw_frame(transaction_bytes, Vec::new());
            let mut visited = HashSet::new();
            let mut output = vec![0xee];
            let decoded = collect_first_seen_transaction_signatures(
                &frame,
                &dataframes,
                &mut visited,
                &mut output,
            )
            .unwrap();
            assert_eq!(decoded, count);
            assert_eq!(&output[1..], signature_bytes);
            assert!(visited.is_empty());
        }

        for (prefix, expected_error) in [
            (vec![0x80], "truncated transaction signature ShortU16"),
            (vec![0x80, 0x00], "invalid transaction signature ShortU16"),
            (
                vec![0xff, 0xff, 0x04],
                "invalid transaction signature ShortU16",
            ),
            (
                vec![0x80, 0x80, 0x01],
                "signature count exceeds hot-row u8 maximum",
            ),
        ] {
            let frame = signature_raw_frame(prefix, Vec::new());
            let mut visited = HashSet::new();
            let mut output = vec![0xdd];
            let error = collect_first_seen_transaction_signatures(
                &frame,
                &dataframes,
                &mut visited,
                &mut output,
            )
            .expect_err("malformed or unsupported signature count must fail");
            assert!(
                error.to_string().contains(expected_error),
                "unexpected error: {error:#}"
            );
            assert_eq!(output, vec![0xdd], "failed collection must roll back");
        }

        let location = NodeLocation {
            entry_index: 1,
            car_offset: 2,
        };
        let transaction = |index: u8, count: u8| PendingTx {
            tx: RawTransactionNode {
                location,
                cid: Cid36::from_car_bytes([index; 36]),
                slot: 42,
                index: Some(u64::from(index)),
                data: signature_raw_frame(
                    std::iter::once(count)
                        .chain(std::iter::repeat_n(
                            index,
                            usize::from(count) * FIRST_SEEN_SIGNATURE_BYTES,
                        ))
                        .collect(),
                    Vec::new(),
                ),
                metadata: signature_raw_frame(Vec::new(), Vec::new()),
            },
            payload_len: 0,
        };
        let transactions = vec![transaction(1, 1), transaction(2, 0)];
        let mut arena = FirstSeenBlockSignatures::default();
        arena.collect_block(&transactions, &dataframes, 42).unwrap();
        let counts_pointer = arena.counts.as_ptr();
        let bytes_pointer = arena.bytes.as_ptr();
        let capacities = (arena.counts.capacity(), arena.bytes.capacity());
        arena.collect_block(&transactions, &dataframes, 42).unwrap();
        assert_eq!(arena.counts, vec![1, 0]);
        assert_eq!(arena.bytes, vec![1; FIRST_SEEN_SIGNATURE_BYTES]);
        assert_eq!(arena.counts.as_ptr(), counts_pointer);
        assert_eq!(arena.bytes.as_ptr(), bytes_pointer);
        assert_eq!(
            (arena.counts.capacity(), arena.bytes.capacity()),
            capacities
        );
        assert_eq!(arena.stats.blocks, 2);
        assert_eq!(arena.stats.transactions, 4);
        assert_eq!(arena.stats.signatures, 2);
        assert_eq!(arena.stats.decoder_signature_ref_vec_allocations_avoided, 2);
        assert_eq!(arena.stats.owned_signature_outer_vec_allocations_avoided, 2);
        assert_eq!(arena.stats.owned_signature_inner_vec_allocations_avoided, 2);
    }

    #[test]
    fn first_seen_signature_collector_streams_cid_and_inline_chunks() {
        let signature = (0..FIRST_SEEN_SIGNATURE_BYTES)
            .map(|index| (index * 3) as u8)
            .collect::<Vec<_>>();
        let continuation_cid = Cid36::from_car_bytes([0x31; 36]);
        let location = NodeLocation {
            entry_index: 3,
            car_offset: 4,
        };
        let continuation = StandaloneDataFrame {
            location,
            cid: continuation_cid,
            frame: signature_raw_frame(
                signature[11..28].to_vec(),
                vec![inline_identity_raw_cid(&signature[28..])],
            ),
        };
        let dataframes = HashMap::from([(continuation_cid, continuation)]);
        let mut root_bytes = vec![1];
        root_bytes.extend_from_slice(&signature[..11]);
        let root = signature_raw_frame(root_bytes, vec![RawCidRef::from_car_cid(continuation_cid)]);
        let mut visited = HashSet::new();
        let mut output = Vec::new();
        assert_eq!(
            collect_first_seen_transaction_signatures(
                &root,
                &dataframes,
                &mut visited,
                &mut output,
            )
            .unwrap(),
            1,
        );
        assert_eq!(output, signature);
        assert!(visited.is_empty());

        let split_prefix_cid = Cid36::from_car_bytes([0x32; 36]);
        let split_signature_bytes = (0..128 * FIRST_SEEN_SIGNATURE_BYTES)
            .map(|index| (index.wrapping_mul(11) & 0xff) as u8)
            .collect::<Vec<_>>();
        let mut continuation_bytes = vec![0x01];
        continuation_bytes.extend_from_slice(&split_signature_bytes);
        let split_dataframes = HashMap::from([(
            split_prefix_cid,
            StandaloneDataFrame {
                location,
                cid: split_prefix_cid,
                frame: signature_raw_frame(continuation_bytes, Vec::new()),
            },
        )]);
        let split_root =
            signature_raw_frame(vec![0x80], vec![RawCidRef::from_car_cid(split_prefix_cid)]);
        output.clear();
        assert_eq!(
            collect_first_seen_transaction_signatures(
                &split_root,
                &split_dataframes,
                &mut visited,
                &mut output,
            )
            .unwrap(),
            128,
        );
        assert_eq!(output, split_signature_bytes);
        assert!(visited.is_empty());
    }

    #[test]
    fn first_seen_signature_collector_rejects_missing_malformed_and_cycles() {
        let location = NodeLocation {
            entry_index: 5,
            car_offset: 6,
        };
        let missing_cid = Cid36::from_car_bytes([0x41; 36]);
        let cycle_cid = Cid36::from_car_bytes([0x42; 36]);
        let cycle = StandaloneDataFrame {
            location,
            cid: cycle_cid,
            frame: signature_raw_frame(Vec::new(), vec![RawCidRef::from_car_cid(cycle_cid)]),
        };
        let malformed =
            raw_cid_ref_from_serialized_parts(None, &[0xff, 0x01], &[0, 0xff, 0x01], false);
        let scenarios = [
            (
                signature_raw_frame(vec![1], vec![RawCidRef::from_car_cid(missing_cid)]),
                HashMap::new(),
                "missing dataframe",
            ),
            (
                signature_raw_frame(vec![1], vec![malformed]),
                HashMap::new(),
                "unsupported dataframe CID reference",
            ),
            (
                signature_raw_frame(vec![1], vec![RawCidRef::from_car_cid(cycle_cid)]),
                HashMap::from([(cycle_cid, cycle)]),
                "dataframe cycle",
            ),
        ];

        for (root, dataframes, expected_error) in scenarios {
            let mut visited = HashSet::new();
            let mut output = vec![0xcc];
            let error = collect_first_seen_transaction_signatures(
                &root,
                &dataframes,
                &mut visited,
                &mut output,
            )
            .expect_err("invalid dataframe chain must fail");
            assert!(
                error.to_string().contains(expected_error),
                "unexpected error: {error:#}"
            );
            assert_eq!(output, vec![0xcc]);
            assert!(visited.is_empty());
        }
    }

    #[derive(Default)]
    struct CountingWriter {
        bytes: Vec<u8>,
        writes: usize,
    }

    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn signature_test_sets() -> Vec<Vec<Vec<u8>>> {
        vec![
            vec![vec![0x71; 64], vec![0x72; 64]],
            Vec::new(),
            vec![vec![0x73; 64]],
        ]
    }

    fn signature_test_block(owned_signatures: bool) -> WincodeArchiveV2Block {
        let mut block = empty_archive_v2_block(99);
        block.txs = signature_test_sets()
            .into_iter()
            .enumerate()
            .map(|(tx_index, signatures)| {
                let signature_count = signatures.len() as u8;
                WincodeArchiveV2Transaction {
                    tx_index: tx_index as u32,
                    tx: WincodeArchiveV2Payload::Decoded {
                        source_len: 100,
                        value: OwnedCompactTransaction {
                            signatures: if owned_signatures {
                                signatures
                            } else {
                                Vec::new()
                            },
                            message: OwnedCompactMessage::Legacy(OwnedCompactLegacyMessage {
                                header: CompactMessageHeader {
                                    num_required_signatures: signature_count,
                                    num_readonly_signed_accounts: 0,
                                    num_readonly_unsigned_accounts: 0,
                                },
                                account_keys: (0..signature_count)
                                    .map(|index| CompactPubkey::raw([index.wrapping_add(1); 32]))
                                    .collect(),
                                recent_blockhash: OwnedCompactRecentBlockhash::Nonce(
                                    [tx_index as u8; 32],
                                ),
                                instructions: Vec::new(),
                            }),
                        },
                    },
                    metadata: None,
                }
            })
            .collect();
        block
    }

    fn signature_test_arena() -> FirstSeenBlockSignatures {
        let signatures = signature_test_sets();
        FirstSeenBlockSignatures {
            counts: signatures
                .iter()
                .map(|signatures| signatures.len() as u8)
                .collect(),
            bytes: signatures
                .iter()
                .flatten()
                .flat_map(|signature| signature.iter().copied())
                .collect(),
            ..FirstSeenBlockSignatures::default()
        }
    }

    #[test]
    fn first_seen_flat_signatures_match_owned_hot_output_with_one_write() {
        let known_program_ids = KnownProgramIds {
            vote: None,
            compute_budget: None,
            system: None,
        };
        let slot_to_block_id = GxHashMap::with_hasher(GxBuildHasher::default());

        let mut owned_writer = CountingWriter::default();
        let mut owned_block_bytes = Vec::new();
        let (owned_hot, owned_count) = hot_block_from_archive_block(
            signature_test_block(true),
            &known_program_ids,
            &slot_to_block_id,
            &mut VoteHashRegistryBuilder::default(),
            Some(&mut owned_writer),
            Some(&mut owned_block_bytes),
            &mut HotBlockBuffers::default(),
            &mut ArchiveV2Timings::default(),
        )
        .unwrap();

        let arena = signature_test_arena();
        let mut first_seen_writer = CountingWriter::default();
        let (first_seen_hot, first_seen_count) = hot_block_from_first_seen_archive_block(
            signature_test_block(false),
            &arena,
            &known_program_ids,
            &slot_to_block_id,
            &mut VoteHashRegistryBuilder::default(),
            Some(&mut first_seen_writer),
            &mut HotBlockBuffers::default(),
            &mut ArchiveV2Timings::default(),
        )
        .unwrap();

        assert_eq!(owned_count, 3);
        assert_eq!(first_seen_count, owned_count);
        assert_eq!(owned_writer.bytes, arena.bytes);
        assert_eq!(owned_block_bytes, arena.bytes);
        assert_eq!(first_seen_writer.bytes, arena.bytes);
        assert_eq!(owned_writer.writes, 3);
        assert_eq!(first_seen_writer.writes, 1);
        assert_eq!(
            wincode::config::serialize(&first_seen_hot, wincode_leb128_config()).unwrap(),
            wincode::config::serialize(&owned_hot, wincode_leb128_config()).unwrap(),
        );
        assert_eq!(
            first_seen_hot
                .tx_rows
                .iter()
                .map(|row| row.signature_count)
                .collect::<Vec<_>>(),
            vec![2, 0, 1],
        );
    }

    #[test]
    fn first_seen_flat_signature_errors_do_not_write_partial_output() {
        let known_program_ids = KnownProgramIds {
            vote: None,
            compute_budget: None,
            system: None,
        };
        let slot_to_block_id = GxHashMap::with_hasher(GxBuildHasher::default());
        let mut truncated_arena = signature_test_arena();
        truncated_arena.bytes.pop();
        let mut writer = CountingWriter::default();
        let error = hot_block_from_first_seen_archive_block(
            signature_test_block(false),
            &truncated_arena,
            &known_program_ids,
            &slot_to_block_id,
            &mut VoteHashRegistryBuilder::default(),
            Some(&mut writer),
            &mut HotBlockBuffers::default(),
            &mut ArchiveV2Timings::default(),
        )
        .expect_err("signature byte/count mismatch must fail");
        assert!(error.to_string().contains("collected 191 signature bytes"));
        assert_eq!(writer.writes, 0);
        assert!(writer.bytes.is_empty());

        let mut late_failure_block = signature_test_block(false);
        late_failure_block.txs[2].tx = WincodeArchiveV2Payload::Raw {
            bytes: vec![0xff],
            error: "scripted late transaction failure".to_owned(),
        };
        let arena = signature_test_arena();
        let mut writer = CountingWriter::default();
        let error = hot_block_from_first_seen_archive_block(
            late_failure_block,
            &arena,
            &known_program_ids,
            &slot_to_block_id,
            &mut VoteHashRegistryBuilder::default(),
            Some(&mut writer),
            &mut HotBlockBuffers::default(),
            &mut ArchiveV2Timings::default(),
        )
        .expect_err("late transaction failure must abort the block");
        assert!(error.to_string().contains("raw transaction at tx_index 2"));
        assert_eq!(writer.writes, 0);
        assert!(writer.bytes.is_empty());

        let mut malformed_owned_block = signature_test_block(true);
        let WincodeArchiveV2Payload::Decoded { value, .. } = &mut malformed_owned_block.txs[0].tx
        else {
            unreachable!()
        };
        value.signatures[0].pop();
        let error = hot_block_from_archive_block(
            malformed_owned_block,
            &known_program_ids,
            &slot_to_block_id,
            &mut VoteHashRegistryBuilder::default(),
            None,
            None,
            &mut HotBlockBuffers::default(),
            &mut ArchiveV2Timings::default(),
        )
        .expect_err("owned signatures must be validated without an output sink");
        assert!(error.to_string().contains("is 63 bytes, expected 64"));
    }

    #[test]
    fn known_program_ids_refresh_after_first_seen_interning() {
        let registry_path = std::env::temp_dir().join(format!(
            "blockzilla-first-seen-program-ids-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut registry = FirstSeenRegistry::new(32, &registry_path).unwrap();
        registry.seed(compute_budget_program_id_bytes()).unwrap();
        let mut known = KnownProgramIds::from_first_seen(&registry);
        assert!(known.compute_budget.is_some());
        assert!(known.system.is_none());
        assert!(known.vote.is_none());

        let system_id = registry.intern(&system_program_id_bytes()).unwrap();
        known.refresh_from_first_seen(&registry);
        assert_eq!(known.system, Some(system_id));
        assert!(known.vote.is_none());

        let vote_id = registry.intern(&vote_program_id_bytes()).unwrap();
        known.refresh_from_first_seen(&registry);
        assert_eq!(known.vote, Some(vote_id));

        let stable = known;
        registry.intern(&[7u8; 32]).unwrap();
        known.refresh_from_first_seen(&registry);
        assert_eq!(known.vote, stable.vote);
        assert_eq!(known.compute_budget, stable.compute_budget);
        assert_eq!(known.system, stable.system);
        drop(registry);
        std::fs::remove_file(registry_path).unwrap();
    }

    #[test]
    fn full_compaction_timestamp_index_publishes_gap_sidecar() {
        let root = std::env::temp_dir().join(format!(
            "blockzilla-v3-gap-publisher-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let epoch = 314;
        let base = epoch * crate::SLOTS_PER_EPOCH;
        std::fs::write(root.join(BLOCKHASH_INDEX_V3_FILE), b"stale-v3").unwrap();
        std::fs::write(root.join(BLOCK_TIME_GAP_FILE), b"stale-gaps").unwrap();
        let mut publisher = BlockhashIndexV3Publisher::create(&root).unwrap();
        assert!(!root.join(BLOCKHASH_INDEX_V3_FILE).exists());
        assert!(!root.join(BLOCK_TIME_GAP_FILE).exists());
        publisher.push(base, &[1; 32], Some(100)).unwrap();
        publisher.push(base + 3, &[2; 32], Some(103)).unwrap();
        publisher.publish(2).unwrap();

        let sidecar = blockzilla_format::read_block_time_gap_sidecar(
            File::open(root.join(BLOCK_TIME_GAP_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(sidecar.header.epoch, epoch);
        assert_eq!(sidecar.header.block_count, 2);
        assert_eq!(sidecar.header.missing_slot_count, 2);
        assert_eq!(sidecar.rows.len(), 1);
        assert!(root.join(BLOCKHASH_INDEX_V3_FILE).is_file());
        assert!(!root.join(format!("{BLOCKHASH_INDEX_V3_FILE}.tmp")).exists());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn partial_compaction_does_not_start_timestamp_index() {
        let root = std::env::temp_dir().join(format!(
            "blockzilla-partial-v3-gap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join(BLOCKHASH_INDEX_V3_FILE), b"stale-v3").unwrap();
        std::fs::write(root.join(BLOCK_TIME_GAP_FILE), b"stale-gaps").unwrap();
        assert!(
            full_blockhash_index_v3_publisher(&root, Some(1))
                .unwrap()
                .is_none()
        );
        assert!(!root.join(BLOCKHASH_INDEX_V3_FILE).exists());
        assert!(!root.join(BLOCK_TIME_GAP_FILE).exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    fn test_hex_bytes(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0);
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(pair, 16).unwrap()
            })
            .collect()
    }
}
