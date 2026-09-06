//! Non-publishable Archive V2 per-block account-projection canary.
//!
//! This binary does not convert canonical archive facts. It reads immutable
//! Archive V2 block frames and writes only a derived per-block account page
//! stream, its fixed-row index, and a benchmark report.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufWriter, Write},
    ops::Range,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_firebase_indexer::decode::{self, MAX_MESSAGE_ACCOUNTS, MetadataDecodeLimits};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_GENESIS_BIN_FILE, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotBlockIndexRow, ArchiveV2HotTxRow,
    CompactPubkey,
};
use blockzilla_compact_v2_reader::archive_integrity::{
    ArchiveIntegrityConfig, ArchiveIntegrityReport, PohProtocolBounds, PohSidecarSchema,
    verify_archive_v2_integrity,
};
use blockzilla_compact_v2_reader::{
    ArchiveReader, BorrowedDecodedBlock, CompactV2MessageSchema, CompactV2MetadataSchema,
    HashVerification, OpenOptions, OrderedParallelBlockConfig, OrderedParallelBlockStats,
    PinnedLocalRangeSource, RangeSource, SourceError, SourceResult,
    manifest::TrustedGenerationIdentity,
};
use clap::{Parser, ValueEnum};
use rustix::fs::{CWD, Dir, Mode, OFlags, RenameFlags, renameat_with};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use std::{
    collections::HashMap,
    io::{BufReader, Read},
};

const STATUS: &str = "unverified-nonpublishable";
const ACCOUNT_SEMANTICS: &str = "message-accounts-with-recorded-program-roles-v1";
const PAGES_FILE: &str = "archive-v2-resolved-accounts.pages";
const INDEX_FILE: &str = "archive-v2-resolved-accounts.index";
const REPORT_FILE: &str = "benchmark-report.json";
const PAGE_MAGIC: [u8; 8] = *b"BZV2ACP1";
const INDEX_MAGIC: [u8; 8] = *b"BZV2ACX1";
const FORMAT_VERSION: u16 = 1;
const PAGE_HEADER_LEN: usize = 24;
const INDEX_HEADER_LEN: usize = 40;
const INDEX_ROW_LEN: usize = 44;
const INDEX_ROW_FLAG_ZSTD: u32 = 1;
const ZSTD_LEVEL: i32 = 1;
const MAX_RETAINED_PAGE_SCRATCH_BYTES: usize = 32 << 20;
#[allow(dead_code)] // Used by the separate verifier binary that imports this file as a module.
const MAX_VERIFIER_DECODED_PAGE_BYTES: u64 = 512 << 20;
#[allow(dead_code)] // Used by the separate verifier binary that imports this file as a module.
const MAX_VERIFIER_DIRECTORY_ENTRIES: usize = 256;

const ROLE_SIGNER: u8 = 1 << 0;
const ROLE_WRITABLE: u8 = 1 << 1;
const ROLE_TOP_LEVEL_PROGRAM: u8 = 1 << 2;
const ROLE_CPI_PROGRAM: u8 = 1 << 3;
const ROLE_MASK: u8 = ROLE_SIGNER | ROLE_WRITABLE | ROLE_TOP_LEVEL_PROGRAM | ROLE_CPI_PROGRAM;
const NO_ACCOUNT_POSITION: u16 = u16::MAX;
const ACCOUNT_COVERAGE_MASK: u32 = (1 << 4) - 1;
const CPI_COVERAGE_MASK: u32 = ((1 << 5) - 1) << 8;
const COVERAGE_MASK: u32 = ACCOUNT_COVERAGE_MASK | CPI_COVERAGE_MASK;
const SOURCE_TX_FLAG_MASK: u32 = (1 << 11) - 1;

const SPLIT_CANARY_KIND: &str = "source-preserving-metadata-effects-v1";
const SPLIT_FILE_HEADER_LEN: usize = 64;
const SPLIT_INDEX_ROW_LEN: usize = 160;
const SPLIT_FRAME_HEADER_LEN: usize = 32;
const SPLIT_FORMAT_VERSION: u16 = 1;
const SPLIT_FRAME_FLAG_ZSTD: u32 = 1;
const SPLIT_TX_CHUNK: usize = 256;
const MAX_SPLIT_RAW_BYTES_PER_WORKER: usize = 512 << 20;
const MAX_RETAINED_SPLIT_RAW_CAPACITY_PER_WORKER: usize = 128 << 20;
const MAX_RETAINED_SPLIT_CHUNK_CAPACITY_PER_WORKER: usize = 8 << 20;
const MAX_SPLIT_TOTAL_SCRATCH_CAPACITY_PER_WORKER: usize =
    MAX_SPLIT_RAW_BYTES_PER_WORKER + MAX_RETAINED_SPLIT_CHUNK_CAPACITY_PER_WORKER;
const MAX_RETAINED_SPLIT_TOTAL_SCRATCH_CAPACITY_PER_WORKER: usize =
    MAX_RETAINED_SPLIT_RAW_CAPACITY_PER_WORKER + MAX_RETAINED_SPLIT_CHUNK_CAPACITY_PER_WORKER;
const MAX_SPLIT_PACKED_BYTES_PER_BLOCK: usize = 576 << 20;
const SPLIT_DATA_MAGIC: [u8; 8] = *b"BZV2SP01";
const SPLIT_INDEX_MAGIC: [u8; 8] = *b"BZV2SX01";
const SPLIT_INDEX_FILE: &str = "archive-v2-source-split.index";

const LEAN_CANARY_KIND: &str = "source-preserving-block-chunks-v1";
const LEAN_FILE_HEADER_LEN: usize = 64;
const LEAN_INDEX_ROW_LEN: usize = 160;
const LEAN_DIRECTORY_ROW_LEN: usize = 24;
const LEAN_FORMAT_VERSION: u16 = 1;
const LEAN_ZSTD_CODEC_BIT: u32 = 1 << 31;
const LEAN_STORED_LEN_MASK: u32 = LEAN_ZSTD_CODEC_BIT - 1;
const LEAN_DATA_MAGIC: [u8; 8] = *b"BZV2LN01";
const LEAN_INDEX_MAGIC: [u8; 8] = *b"BZV2LI01";
const LEAN_INDEX_FILE: &str = "archive-v2-lean-blocks.index";
const MAX_LEAN_SCRATCH_BYTES_PER_WORKER: usize = 512 << 20;
const MAX_RETAINED_LEAN_SCRATCH_BYTES_PER_WORKER: usize = 128 << 20;
const MAX_LEAN_PACKED_BYTES_PER_BLOCK: usize = 576 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LeanCompressionArg {
    Raw,
    Zstd,
    Adaptive,
    Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LeanZstdLevelArg {
    #[value(name = "1")]
    One,
    #[value(name = "3")]
    Three,
    #[value(name = "5")]
    Five,
    #[value(name = "9")]
    Nine,
}

impl LeanZstdLevelArg {
    const fn level(self) -> i32 {
        match self {
            Self::One => 1,
            Self::Three => 3,
            Self::Five => 5,
            Self::Nine => 9,
        }
    }

    const fn header_code(self) -> u8 {
        match self {
            Self::One => 0,
            Self::Three => 3,
            Self::Five => 5,
            Self::Nine => 9,
        }
    }

    const fn hybrid_policy_name(self) -> &'static str {
        match self {
            Self::One => {
                "hybrid-v1-directory-and-five-dense-zstd1-two-sparse-adaptive-zstd1-when-smaller-block-rewards-raw-no-attempt"
            }
            Self::Three => {
                "hybrid-v1-directory-and-five-dense-zstd3-two-sparse-adaptive-zstd3-when-smaller-block-rewards-raw-no-attempt"
            }
            Self::Five => {
                "hybrid-v1-directory-and-five-dense-zstd5-two-sparse-adaptive-zstd5-when-smaller-block-rewards-raw-no-attempt"
            }
            Self::Nine => {
                "hybrid-v1-directory-and-five-dense-zstd9-two-sparse-adaptive-zstd9-when-smaller-block-rewards-raw-no-attempt"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeanObjectCompression {
    Raw,
    Zstd,
    Adaptive,
}

impl LeanObjectCompression {
    const fn name(self, level: LeanZstdLevelArg) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Zstd => match level {
                LeanZstdLevelArg::One => "zstd1",
                LeanZstdLevelArg::Three => "zstd3",
                LeanZstdLevelArg::Five => "zstd5",
                LeanZstdLevelArg::Nine => "zstd9",
            },
            Self::Adaptive => match level {
                LeanZstdLevelArg::One => "adaptive-zstd1-when-smaller",
                LeanZstdLevelArg::Three => "adaptive-zstd3-when-smaller",
                LeanZstdLevelArg::Five => "adaptive-zstd5-when-smaller",
                LeanZstdLevelArg::Nine => "adaptive-zstd9-when-smaller",
            },
        }
    }
}

impl LeanCompressionArg {
    const fn code(self) -> u8 {
        match self {
            Self::Raw => 0,
            Self::Zstd => 1,
            Self::Adaptive => 2,
            Self::Hybrid => 3,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Zstd => "zstd",
            Self::Adaptive => "adaptive",
            Self::Hybrid => "hybrid",
        }
    }

    const fn object_compression(self, object: LeanObject) -> LeanObjectCompression {
        match self {
            Self::Raw => LeanObjectCompression::Raw,
            Self::Zstd => LeanObjectCompression::Zstd,
            Self::Adaptive => LeanObjectCompression::Adaptive,
            Self::Hybrid => match object {
                LeanObject::TransactionDirectory
                | LeanObject::InnerInstructions
                | LeanObject::Logs
                | LeanObject::TokenBalances
                | LeanObject::Balances
                | LeanObject::Outcomes => LeanObjectCompression::Zstd,
                LeanObject::TransactionRewards | LeanObject::RawMetadataFallbacks => {
                    LeanObjectCompression::Adaptive
                }
                LeanObject::BlockRewards => LeanObjectCompression::Raw,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
enum LeanObject {
    TransactionDirectory = 0,
    InnerInstructions = 1,
    Logs = 2,
    TokenBalances = 3,
    Balances = 4,
    Outcomes = 5,
    TransactionRewards = 6,
    RawMetadataFallbacks = 7,
    BlockRewards = 8,
}

impl LeanObject {
    const ALL: [Self; 9] = [
        Self::TransactionDirectory,
        Self::InnerInstructions,
        Self::Logs,
        Self::TokenBalances,
        Self::Balances,
        Self::Outcomes,
        Self::TransactionRewards,
        Self::RawMetadataFallbacks,
        Self::BlockRewards,
    ];

    const DENSE_TX_PLANES: [Self; 5] = [
        Self::InnerInstructions,
        Self::Logs,
        Self::TokenBalances,
        Self::Balances,
        Self::Outcomes,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    const fn name(self) -> &'static str {
        match self {
            Self::TransactionDirectory => "transaction-directory",
            Self::InnerInstructions => "inner-instructions",
            Self::Logs => "logs",
            Self::TokenBalances => "token-balances",
            Self::Balances => "balances",
            Self::Outcomes => "outcomes",
            Self::TransactionRewards => "transaction-rewards",
            Self::RawMetadataFallbacks => "raw-metadata-fallbacks",
            Self::BlockRewards => "block-rewards",
        }
    }

    const fn file_name(self) -> &'static str {
        match self {
            Self::TransactionDirectory => "archive-v2-lean-transaction-directory.wincode",
            Self::InnerInstructions => "archive-v2-lean-inner-instructions.wincode",
            Self::Logs => "archive-v2-lean-logs.wincode",
            Self::TokenBalances => "archive-v2-lean-token-balances.wincode",
            Self::Balances => "archive-v2-lean-balances.wincode",
            Self::Outcomes => "archive-v2-lean-outcomes.wincode",
            Self::TransactionRewards => "archive-v2-lean-transaction-rewards.wincode",
            Self::RawMetadataFallbacks => "archive-v2-lean-raw-metadata-fallbacks.wincode",
            Self::BlockRewards => "archive-v2-lean-block-rewards.wincode",
        }
    }
}

const LEAN_OBJECT_COUNT: usize = LeanObject::ALL.len();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
enum SplitPlane {
    MetadataStates = 0,
    InnerInstructions = 1,
    Logs = 2,
    TokenBalances = 3,
    Balances = 4,
    Outcomes = 5,
    TransactionRewards = 6,
    BlockRewards = 7,
    RawMetadataFallbacks = 8,
}

impl SplitPlane {
    const ALL: [Self; 9] = [
        Self::MetadataStates,
        Self::InnerInstructions,
        Self::Logs,
        Self::TokenBalances,
        Self::Balances,
        Self::Outcomes,
        Self::TransactionRewards,
        Self::BlockRewards,
        Self::RawMetadataFallbacks,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    const fn file_name(self) -> &'static str {
        match self {
            Self::MetadataStates => "archive-v2-metadata-states.pages",
            Self::InnerInstructions => "archive-v2-inner-instructions.pages",
            Self::Logs => "archive-v2-logs.pages",
            Self::TokenBalances => "archive-v2-token-balances.pages",
            Self::Balances => "archive-v2-balances.pages",
            Self::Outcomes => "archive-v2-outcomes.pages",
            Self::TransactionRewards => "archive-v2-transaction-rewards.pages",
            Self::BlockRewards => "archive-v2-block-rewards.pages",
            Self::RawMetadataFallbacks => "archive-v2-raw-metadata-fallbacks.pages",
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::MetadataStates => "metadata-states",
            Self::InnerInstructions => "inner-instructions",
            Self::Logs => "logs",
            Self::TokenBalances => "token-balances",
            Self::Balances => "balances",
            Self::Outcomes => "outcomes",
            Self::TransactionRewards => "transaction-rewards",
            Self::BlockRewards => "block-rewards",
            Self::RawMetadataFallbacks => "raw-metadata-fallbacks",
        }
    }
}

const SPLIT_PLANE_COUNT: usize = SplitPlane::ALL.len();

#[derive(Debug, Clone, Copy)]
struct RawSplitChunk {
    first_tx: u32,
    tx_count: u32,
    dense_count: u32,
    start: usize,
    end: usize,
}

#[derive(Debug)]
struct SplitWorkerScratch {
    raw: [Vec<u8>; SPLIT_PLANE_COUNT],
    chunks: [Vec<RawSplitChunk>; SPLIT_PLANE_COUNT],
    chunk_starts: [usize; SPLIT_PLANE_COUNT],
    dense_counts: [u32; SPLIT_PLANE_COUNT],
    current_first_tx: u32,
    current_tx_count: u32,
    total_tx_count: u32,
    compression_scratch: Vec<u8>,
    max_aggregate_raw_bytes: usize,
    max_aggregate_scratch_capacity: usize,
    max_retained_raw_capacity: usize,
    max_retained_chunk_capacity: usize,
    max_total_scratch_capacity: usize,
    max_retained_total_scratch_capacity: usize,
    source_field_bytes: [u64; SPLIT_PLANE_COUNT],
    missing_metadata: u64,
    decoded_metadata: u64,
    raw_metadata: u64,
    tx_raw_flags: u64,
}

impl Default for SplitWorkerScratch {
    fn default() -> Self {
        Self {
            raw: std::array::from_fn(|_| Vec::new()),
            chunks: std::array::from_fn(|_| Vec::new()),
            chunk_starts: [0; SPLIT_PLANE_COUNT],
            dense_counts: [0; SPLIT_PLANE_COUNT],
            current_first_tx: 0,
            current_tx_count: 0,
            total_tx_count: 0,
            compression_scratch: Vec::new(),
            max_aggregate_raw_bytes: 0,
            max_aggregate_scratch_capacity: 0,
            max_retained_raw_capacity: 0,
            max_retained_chunk_capacity: 0,
            max_total_scratch_capacity: 0,
            max_retained_total_scratch_capacity: 0,
            source_field_bytes: [0; SPLIT_PLANE_COUNT],
            missing_metadata: 0,
            decoded_metadata: 0,
            raw_metadata: 0,
            tx_raw_flags: 0,
        }
    }
}

impl SplitWorkerScratch {
    fn begin_block(&mut self) {
        for plane in SplitPlane::ALL {
            self.raw[plane.index()].clear();
            self.chunks[plane.index()].clear();
        }
        self.chunk_starts.fill(0);
        self.dense_counts.fill(0);
        self.current_first_tx = 0;
        self.current_tx_count = 0;
        self.total_tx_count = 0;
        self.max_aggregate_raw_bytes = 0;
        self.max_aggregate_scratch_capacity = 0;
        self.source_field_bytes.fill(0);
        self.missing_metadata = 0;
        self.decoded_metadata = 0;
        self.raw_metadata = 0;
        self.tx_raw_flags = 0;
    }

    fn note_tx_raw(&mut self, source_flags: u32) {
        if source_flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            self.tx_raw_flags += 1;
        }
    }

    fn reserve_raw_append(
        &mut self,
        plane: SplitPlane,
        additional: usize,
        record_kind: &'static str,
    ) -> Result<()> {
        let aggregate = self.raw.iter().try_fold(0usize, |total, bytes| {
            total
                .checked_add(bytes.len())
                .context("aggregate split raw bytes overflow")
        })?;
        let next = aggregate
            .checked_add(additional)
            .context("aggregate split raw append overflow")?;
        ensure!(
            next <= MAX_SPLIT_RAW_BYTES_PER_WORKER,
            "aggregate split raw scratch {next} bytes exceeds the {MAX_SPLIT_RAW_BYTES_PER_WORKER}-byte worker cap"
        );
        let index = plane.index();
        let required = self.raw[index]
            .len()
            .checked_add(additional)
            .context("split plane raw length overflow")?;
        if required > self.raw[index].capacity() {
            let current_capacity = self.raw[index].capacity();
            let aggregate_capacity =
                self.raw
                    .iter()
                    .try_fold(self.compression_scratch.capacity(), |total, bytes| {
                        total
                            .checked_add(bytes.capacity())
                            .context("aggregate split scratch capacity overflow")
                    })?;
            let without_current = aggregate_capacity
                .checked_sub(current_capacity)
                .context("split scratch capacity accounting underflow")?;
            let available = MAX_SPLIT_RAW_BYTES_PER_WORKER
                .checked_sub(without_current)
                .context("split scratch capacities already exceed worker cap")?;
            ensure!(
                required <= available,
                "aggregate split scratch capacity cannot fit {} {record_kind}",
                plane.name()
            );
            let geometric = current_capacity.saturating_mul(2).max(4 << 10);
            let desired = required.max(geometric).min(available);
            let reserve = desired
                .checked_sub(self.raw[index].len())
                .context("split raw reserve underflow")?;
            self.raw[index]
                .try_reserve_exact(reserve)
                .with_context(|| format!("reserve {} {record_kind}", plane.name()))?;
            ensure!(
                self.raw[index].capacity() <= available,
                "{} raw scratch capacity exceeds aggregate worker cap",
                plane.name()
            );
        }
        self.note_scratch_capacity()
    }

    fn note_scratch_capacity(&mut self) -> Result<()> {
        let raw_and_compression_capacity =
            self.raw
                .iter()
                .try_fold(self.compression_scratch.capacity(), |total, bytes| {
                    total
                        .checked_add(bytes.capacity())
                        .context("aggregate split scratch capacity overflow")
                })?;
        ensure!(
            raw_and_compression_capacity <= MAX_SPLIT_RAW_BYTES_PER_WORKER,
            "aggregate split scratch capacity {raw_and_compression_capacity} bytes exceeds the {MAX_SPLIT_RAW_BYTES_PER_WORKER}-byte worker cap"
        );
        let chunk_capacity = self.chunk_capacity_bytes()?;
        ensure!(
            chunk_capacity <= MAX_RETAINED_SPLIT_CHUNK_CAPACITY_PER_WORKER,
            "aggregate split chunk descriptor capacity {chunk_capacity} bytes exceeds the worker cap"
        );
        let total_capacity = raw_and_compression_capacity
            .checked_add(chunk_capacity)
            .context("aggregate split total scratch capacity overflow")?;
        ensure!(
            total_capacity <= MAX_SPLIT_TOTAL_SCRATCH_CAPACITY_PER_WORKER,
            "aggregate split total scratch capacity {total_capacity} bytes exceeds the worker cap"
        );
        self.max_aggregate_scratch_capacity = self
            .max_aggregate_scratch_capacity
            .max(raw_and_compression_capacity);
        self.max_total_scratch_capacity = self.max_total_scratch_capacity.max(total_capacity);
        Ok(())
    }

    fn chunk_capacity_bytes(&self) -> Result<usize> {
        self.chunks.iter().try_fold(0usize, |total, chunks| {
            let bytes = chunks
                .capacity()
                .checked_mul(std::mem::size_of::<RawSplitChunk>())
                .context("split chunk descriptor capacity overflow")?;
            total
                .checked_add(bytes)
                .context("aggregate split chunk capacity overflow")
        })
    }

    fn push_chunk(&mut self, plane: SplitPlane, chunk: RawSplitChunk) -> Result<()> {
        let index = plane.index();
        self.chunks[index]
            .try_reserve(1)
            .with_context(|| format!("reserve {} chunk descriptor", plane.name()))?;
        let retained = self.chunk_capacity_bytes()?;
        ensure!(
            retained <= MAX_RETAINED_SPLIT_CHUNK_CAPACITY_PER_WORKER,
            "aggregate split chunk descriptor capacity {retained} bytes exceeds the worker cap"
        );
        self.max_retained_chunk_capacity = self.max_retained_chunk_capacity.max(retained);
        self.note_scratch_capacity()?;
        self.chunks[index].push(chunk);
        Ok(())
    }

    fn append_dense_bytes(&mut self, plane: SplitPlane, bytes: &[u8]) -> Result<()> {
        self.reserve_raw_append(plane, bytes.len(), "split record")?;
        self.raw[plane.index()].extend_from_slice(bytes);
        self.dense_counts[plane.index()] = self.dense_counts[plane.index()]
            .checked_add(1)
            .context("split dense record count overflow")?;
        self.source_field_bytes[plane.index()] = self.source_field_bytes[plane.index()]
            .checked_add(u64::try_from(bytes.len()).context("split source range exceeds u64")?)
            .context("split source-field byte count overflow")?;
        Ok(())
    }

    fn append_pair_record(&mut self, plane: SplitPlane, first: &[u8], second: &[u8]) -> Result<()> {
        let first_len = u32::try_from(first.len())
            .with_context(|| format!("{} first source range exceeds u32", plane.name()))?;
        let second_len = u32::try_from(second.len())
            .with_context(|| format!("{} second source range exceeds u32", plane.name()))?;
        let record_len = 8usize
            .checked_add(first.len())
            .and_then(|length| length.checked_add(second.len()))
            .context("split composite record length overflow")?;
        self.reserve_raw_append(plane, record_len, "composite record")?;
        self.raw[plane.index()].extend_from_slice(&first_len.to_le_bytes());
        self.raw[plane.index()].extend_from_slice(&second_len.to_le_bytes());
        self.raw[plane.index()].extend_from_slice(first);
        self.raw[plane.index()].extend_from_slice(second);
        self.dense_counts[plane.index()] = self.dense_counts[plane.index()]
            .checked_add(1)
            .context("split dense record count overflow")?;
        let source_bytes = first
            .len()
            .checked_add(second.len())
            .context("split source-field length overflow")?;
        self.source_field_bytes[plane.index()] = self.source_field_bytes[plane.index()]
            .checked_add(u64::try_from(source_bytes).context("split source bytes exceed u64")?)
            .context("split source-field byte count overflow")?;
        Ok(())
    }

    fn append_state(
        &mut self,
        metadata_code: u8,
        effect_state: u8,
        source_flags: u32,
    ) -> Result<()> {
        let tx_raw = u8::from(source_flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0) << 2;
        self.reserve_raw_append(SplitPlane::MetadataStates, 2, "state record")?;
        self.raw[SplitPlane::MetadataStates.index()].push(metadata_code | tx_raw);
        self.raw[SplitPlane::MetadataStates.index()].push(effect_state);
        self.dense_counts[SplitPlane::MetadataStates.index()] = self.dense_counts
            [SplitPlane::MetadataStates.index()]
        .checked_add(1)
        .context("metadata state dense count overflow")?;
        self.note_tx_raw(source_flags);
        Ok(())
    }

    fn record_missing_metadata(&mut self, source_flags: u32) -> Result<()> {
        self.missing_metadata = self
            .missing_metadata
            .checked_add(1)
            .context("missing metadata count overflow")?;
        self.append_state(0, 0, source_flags)?;
        self.finish_transaction()
    }

    fn record_raw_metadata(&mut self, source_flags: u32, bytes: &[u8]) -> Result<()> {
        self.raw_metadata = self
            .raw_metadata
            .checked_add(1)
            .context("raw metadata count overflow")?;
        let length = u32::try_from(bytes.len()).context("raw metadata range exceeds u32")?;
        let plane = SplitPlane::RawMetadataFallbacks;
        let record_len = 4usize
            .checked_add(bytes.len())
            .context("raw metadata fallback record length overflow")?;
        self.reserve_raw_append(plane, record_len, "fallback record")?;
        self.raw[plane.index()].extend_from_slice(&length.to_le_bytes());
        self.raw[plane.index()].extend_from_slice(bytes);
        self.dense_counts[plane.index()] = self.dense_counts[plane.index()]
            .checked_add(1)
            .context("raw metadata dense count overflow")?;
        self.source_field_bytes[plane.index()] = self.source_field_bytes[plane.index()]
            .checked_add(u64::from(length))
            .context("raw metadata byte count overflow")?;
        self.append_state(2, 0, source_flags)?;
        self.finish_transaction()
    }

    fn record_decoded_metadata(
        &mut self,
        source_flags: u32,
        effects: &decode::StreamedMetadataEffects<'_>,
    ) -> Result<()> {
        self.decoded_metadata = self
            .decoded_metadata
            .checked_add(1)
            .context("decoded metadata count overflow")?;
        let fields = effects.fields;
        self.append_pair_record(
            SplitPlane::Outcomes,
            fields.outcome_head,
            fields.outcome_tail,
        )?;
        self.append_pair_record(
            SplitPlane::Balances,
            fields.pre_balances,
            fields.post_balances,
        )?;

        let cpi_state = if !effects.shape.inner_instructions_present {
            1
        } else if effects.inner_group_count == 0 {
            2
        } else {
            self.append_dense_bytes(SplitPlane::InnerInstructions, fields.inner_instructions)?;
            3
        };
        let token_present = effects
            .pre_token_balance_count
            .checked_add(effects.post_token_balance_count)
            .context("token-balance count overflow")?
            != 0;
        if token_present {
            self.append_pair_record(
                SplitPlane::TokenBalances,
                fields.pre_token_balances,
                fields.post_token_balances,
            )?;
        }
        if effects.logs_present {
            self.append_dense_bytes(SplitPlane::Logs, fields.logs)?;
        }
        if effects.transaction_reward_count != 0 {
            self.append_dense_bytes(SplitPlane::TransactionRewards, fields.transaction_rewards)?;
        }
        let mut effect_state = cpi_state;
        effect_state |= 1 << 3; // outcome
        effect_state |= 1 << 4; // balances
        if token_present {
            effect_state |= 1 << 5;
        }
        if effects.logs_present {
            effect_state |= 1 << 6;
        }
        if effects.transaction_reward_count != 0 {
            effect_state |= 1 << 7;
        }
        self.append_state(1, effect_state, source_flags)?;
        self.finish_transaction()
    }

    fn finish_transaction(&mut self) -> Result<()> {
        self.current_tx_count = self
            .current_tx_count
            .checked_add(1)
            .context("split chunk transaction count overflow")?;
        self.total_tx_count = self
            .total_tx_count
            .checked_add(1)
            .context("split transaction count overflow")?;
        self.update_max_raw_bytes()?;
        if self.current_tx_count == SPLIT_TX_CHUNK as u32 {
            self.finish_transaction_chunk()?;
        }
        Ok(())
    }

    fn finish_transaction_chunk(&mut self) -> Result<()> {
        if self.current_tx_count == 0 {
            return Ok(());
        }
        ensure!(
            self.current_tx_count <= SPLIT_TX_CHUNK as u32,
            "split transaction chunk exceeds {SPLIT_TX_CHUNK} rows"
        );
        for plane in SplitPlane::ALL {
            if plane == SplitPlane::BlockRewards {
                continue;
            }
            let index = plane.index();
            let dense_count = self.dense_counts[index];
            if dense_count != 0 {
                self.push_chunk(
                    plane,
                    RawSplitChunk {
                        first_tx: self.current_first_tx,
                        tx_count: self.current_tx_count,
                        dense_count,
                        start: self.chunk_starts[index],
                        end: self.raw[index].len(),
                    },
                )?;
            }
            self.chunk_starts[index] = self.raw[index].len();
            self.dense_counts[index] = 0;
        }
        self.current_first_tx = self
            .current_first_tx
            .checked_add(self.current_tx_count)
            .context("split chunk first transaction overflow")?;
        self.current_tx_count = 0;
        Ok(())
    }

    fn record_block_rewards(&mut self, exact_option_bytes: &[u8]) -> Result<()> {
        ensure!(
            !exact_option_bytes.is_empty(),
            "block reward Option field is empty"
        );
        match exact_option_bytes[0] {
            0 => ensure!(
                exact_option_bytes.len() == 1,
                "absent block rewards have trailing bytes"
            ),
            1 => {
                let plane = SplitPlane::BlockRewards;
                self.append_dense_bytes(plane, exact_option_bytes)?;
                self.push_chunk(
                    plane,
                    RawSplitChunk {
                        first_tx: 0,
                        tx_count: 0,
                        dense_count: 1,
                        start: 0,
                        end: self.raw[plane.index()].len(),
                    },
                )?;
            }
            other => bail!("invalid block reward Option tag {other}"),
        }
        self.update_max_raw_bytes()
    }

    fn finish_block_transactions(&mut self, expected: u32) -> Result<()> {
        self.finish_transaction_chunk()?;
        ensure!(
            self.total_tx_count == expected,
            "split state lane has {} transactions, expected {expected}",
            self.total_tx_count
        );
        Ok(())
    }

    fn update_max_raw_bytes(&mut self) -> Result<()> {
        let aggregate = self.raw.iter().try_fold(0usize, |total, plane| {
            total
                .checked_add(plane.len())
                .context("aggregate split raw bytes overflow")
        })?;
        ensure!(
            aggregate <= MAX_SPLIT_RAW_BYTES_PER_WORKER,
            "aggregate split raw scratch {aggregate} bytes exceeds the {MAX_SPLIT_RAW_BYTES_PER_WORKER}-byte worker cap"
        );
        self.max_aggregate_raw_bytes = self.max_aggregate_raw_bytes.max(aggregate);
        Ok(())
    }

    fn trim_retained_scratch(&mut self) -> Result<(usize, usize)> {
        for raw in &mut self.raw {
            raw.clear();
        }
        for chunks in &mut self.chunks {
            chunks.clear();
        }
        self.compression_scratch.clear();
        let raw_and_compression_capacity = loop {
            let raw_capacity = self.raw.iter().try_fold(0usize, |total, raw| {
                total
                    .checked_add(raw.capacity())
                    .context("aggregate split raw capacity overflow")
            })?;
            let total_capacity = raw_capacity
                .checked_add(self.compression_scratch.capacity())
                .context("aggregate split retained capacity overflow")?;
            if total_capacity <= MAX_RETAINED_SPLIT_RAW_CAPACITY_PER_WORKER {
                self.max_retained_raw_capacity = self.max_retained_raw_capacity.max(total_capacity);
                break total_capacity;
            }
            let mut largest_index = None;
            let mut largest_capacity = self.compression_scratch.capacity();
            for (index, raw) in self.raw.iter().enumerate() {
                if raw.capacity() > largest_capacity {
                    largest_index = Some(index);
                    largest_capacity = raw.capacity();
                }
            }
            ensure!(
                largest_capacity != 0,
                "split retained-capacity trim made no progress"
            );
            if let Some(index) = largest_index {
                self.raw[index] = Vec::new();
            } else {
                self.compression_scratch = Vec::new();
            }
        };
        let chunk_capacity = loop {
            let total_capacity = self.chunk_capacity_bytes()?;
            if total_capacity <= MAX_RETAINED_SPLIT_CHUNK_CAPACITY_PER_WORKER {
                self.max_retained_chunk_capacity =
                    self.max_retained_chunk_capacity.max(total_capacity);
                break total_capacity;
            }
            let (largest_index, largest_capacity) = self
                .chunks
                .iter()
                .enumerate()
                .max_by_key(|(_, chunks)| chunks.capacity())
                .map(|(index, chunks)| (index, chunks.capacity()))
                .context("split chunk-capacity trim has no buffers")?;
            ensure!(
                largest_capacity != 0,
                "split chunk-capacity trim made no progress"
            );
            self.chunks[largest_index] = Vec::new();
        };
        let retained_total = raw_and_compression_capacity
            .checked_add(chunk_capacity)
            .context("aggregate retained split scratch capacity overflow")?;
        ensure!(
            retained_total <= MAX_RETAINED_SPLIT_TOTAL_SCRATCH_CAPACITY_PER_WORKER,
            "aggregate retained split scratch capacity {retained_total} bytes exceeds the worker cap"
        );
        self.max_retained_total_scratch_capacity =
            self.max_retained_total_scratch_capacity.max(retained_total);
        Ok((raw_and_compression_capacity, chunk_capacity))
    }
}

fn encode_split_effects(
    scratch: &mut SplitWorkerScratch,
    compressor: &mut zstd::bulk::Compressor<'static>,
    row: ArchiveV2HotBlockIndexRow,
) -> Result<ProjectedSplitEffects> {
    let mut packed = Vec::new();
    let mut ranges: [Range<usize>; SPLIT_PLANE_COUNT] = std::array::from_fn(|_| 0usize..0usize);
    let mut stats = SplitProjectionStats {
        missing_metadata: scratch.missing_metadata,
        decoded_metadata: scratch.decoded_metadata,
        raw_metadata: scratch.raw_metadata,
        tx_raw_flags: scratch.tx_raw_flags,
        max_worker_aggregate_raw_bytes: scratch.max_aggregate_raw_bytes,
        ..SplitProjectionStats::default()
    };
    let mut compression_time = Duration::ZERO;
    for plane in SplitPlane::ALL {
        let plane_index = plane.index();
        let range_start = packed.len();
        stats.planes[plane_index].source_field_bytes = scratch.source_field_bytes[plane_index];
        for chunk_index in 0..scratch.chunks[plane_index].len() {
            let chunk = scratch.chunks[plane_index][chunk_index];
            ensure!(
                chunk.start <= chunk.end && chunk.end <= scratch.raw[plane_index].len(),
                "{} raw chunk geometry is invalid",
                plane.name()
            );
            ensure!(
                chunk.dense_count != 0,
                "{} zero-dense frame was not omitted",
                plane.name()
            );
            if plane != SplitPlane::BlockRewards {
                ensure!(
                    (1..=SPLIT_TX_CHUNK as u32).contains(&chunk.tx_count),
                    "{} frame transaction count is outside 1..={SPLIT_TX_CHUNK}",
                    plane.name()
                );
                ensure!(
                    chunk.dense_count <= chunk.tx_count,
                    "{} frame dense count exceeds transaction count",
                    plane.name()
                );
            } else {
                ensure!(
                    chunk.first_tx == 0 && chunk.tx_count == 0 && chunk.dense_count == 1,
                    "block reward frame geometry is invalid"
                );
            }
            let raw_len = chunk
                .end
                .checked_sub(chunk.start)
                .context("split raw chunk length underflow")?;
            ensure!(raw_len != 0, "{} dense frame is empty", plane.name());
            let decoded_len = u32::try_from(raw_len)
                .with_context(|| format!("{} decoded frame exceeds u32", plane.name()))?;
            let compression_started = Instant::now();
            scratch.compression_scratch.clear();
            let bound = zstd::zstd_safe::compress_bound(raw_len);
            let aggregate_raw = scratch.raw.iter().try_fold(0usize, |total, raw| {
                total
                    .checked_add(raw.len())
                    .context("aggregate split raw bytes overflow")
            })?;
            let maximum_live = aggregate_raw
                .checked_add(bound)
                .context("split raw plus compression scratch overflow")?;
            ensure!(
                maximum_live <= MAX_SPLIT_RAW_BYTES_PER_WORKER,
                "aggregate split live scratch {maximum_live} bytes exceeds the {MAX_SPLIT_RAW_BYTES_PER_WORKER}-byte worker cap"
            );
            scratch.max_aggregate_raw_bytes = scratch.max_aggregate_raw_bytes.max(maximum_live);
            if scratch.compression_scratch.capacity() < bound {
                let raw_capacity = scratch.raw.iter().try_fold(0usize, |total, raw| {
                    total
                        .checked_add(raw.capacity())
                        .context("aggregate split raw capacity overflow")
                })?;
                ensure!(
                    raw_capacity
                        .checked_add(bound)
                        .is_some_and(|capacity| capacity <= MAX_SPLIT_RAW_BYTES_PER_WORKER),
                    "aggregate split raw capacity plus zstd bound exceeds the worker cap"
                );
                let mut replacement = Vec::new();
                replacement
                    .try_reserve_exact(bound)
                    .with_context(|| format!("reserve {} zstd scratch", plane.name()))?;
                scratch.compression_scratch = replacement;
            }
            ensure!(
                scratch.compression_scratch.capacity() >= bound,
                "{} zstd scratch reserve did not reach compress bound",
                plane.name()
            );
            scratch.note_scratch_capacity()?;
            let raw = &scratch.raw[plane_index][chunk.start..chunk.end];
            let written = compressor
                .compress_to_buffer(raw, &mut scratch.compression_scratch)
                .with_context(|| format!("compress {} split frame", plane.name()))?;
            ensure!(
                written == scratch.compression_scratch.len(),
                "{} zstd wrote {written} bytes but exposed {}",
                plane.name(),
                scratch.compression_scratch.len()
            );
            compression_time = compression_time.saturating_add(compression_started.elapsed());
            let (flags, stored) = if scratch.compression_scratch.len() < raw_len {
                (
                    SPLIT_FRAME_FLAG_ZSTD,
                    scratch.compression_scratch.as_slice(),
                )
            } else {
                (0, raw)
            };
            let stored_len = u32::try_from(stored.len())
                .with_context(|| format!("{} stored frame exceeds u32", plane.name()))?;
            let frame_len = SPLIT_FRAME_HEADER_LEN
                .checked_add(stored.len())
                .context("split stored frame length overflow")?;
            let required_packed = packed
                .len()
                .checked_add(frame_len)
                .context("split packed block length overflow")?;
            ensure!(
                required_packed <= MAX_SPLIT_PACKED_BYTES_PER_BLOCK,
                "split packed block exceeds the {MAX_SPLIT_PACKED_BYTES_PER_BLOCK}-byte cap"
            );
            if required_packed > packed.capacity() {
                let desired_capacity = required_packed
                    .max(packed.capacity().saturating_mul(2))
                    .min(MAX_SPLIT_PACKED_BYTES_PER_BLOCK);
                let additional_capacity = desired_capacity
                    .checked_sub(packed.len())
                    .context("split packed reserve underflow")?;
                packed
                    .try_reserve_exact(additional_capacity)
                    .with_context(|| format!("reserve {} stored frame", plane.name()))?;
                ensure!(
                    packed.capacity() <= MAX_SPLIT_PACKED_BYTES_PER_BLOCK,
                    "split packed block capacity exceeds the worker cap"
                );
            }
            packed.extend_from_slice(&(plane as u16).to_le_bytes());
            packed.extend_from_slice(&SPLIT_FORMAT_VERSION.to_le_bytes());
            packed.extend_from_slice(&flags.to_le_bytes());
            packed.extend_from_slice(&row.block_id.to_le_bytes());
            packed.extend_from_slice(&chunk.first_tx.to_le_bytes());
            packed.extend_from_slice(&chunk.tx_count.to_le_bytes());
            packed.extend_from_slice(&chunk.dense_count.to_le_bytes());
            packed.extend_from_slice(&decoded_len.to_le_bytes());
            packed.extend_from_slice(&stored_len.to_le_bytes());
            packed.extend_from_slice(stored);
            stats.planes[plane_index].frames = stats.planes[plane_index]
                .frames
                .checked_add(1)
                .context("split frame count overflow")?;
            stats.planes[plane_index].records = stats.planes[plane_index]
                .records
                .checked_add(u64::from(chunk.dense_count))
                .context("split record count overflow")?;
            stats.planes[plane_index].decoded_bytes = stats.planes[plane_index]
                .decoded_bytes
                .checked_add(u64::from(decoded_len))
                .context("split decoded byte count overflow")?;
            stats.planes[plane_index].stored_bytes = stats.planes[plane_index]
                .stored_bytes
                .checked_add(
                    u64::try_from(SPLIT_FRAME_HEADER_LEN)
                        .expect("fixed frame header fits u64")
                        .checked_add(u64::from(stored_len))
                        .context("split stored frame length overflow")?,
                )
                .context("split stored byte count overflow")?;
        }
        ranges[plane_index] = range_start..packed.len();
    }
    stats.max_block_stored_bytes = packed.len();
    stats.max_worker_aggregate_raw_bytes = scratch.max_aggregate_raw_bytes;
    stats.max_worker_aggregate_scratch_capacity = scratch.max_aggregate_scratch_capacity;
    stats.max_worker_total_scratch_capacity = scratch.max_total_scratch_capacity;
    let (retained_raw, retained_chunks) = scratch.trim_retained_scratch()?;
    stats.max_worker_retained_raw_capacity = retained_raw;
    stats.max_worker_retained_chunk_capacity = retained_chunks;
    stats.max_worker_retained_total_scratch_capacity = scratch.max_retained_total_scratch_capacity;
    Ok(ProjectedSplitEffects {
        packed,
        ranges,
        stats,
        compression_time,
    })
}

#[derive(Debug)]
struct LeanWorkerScratch {
    raw: [Vec<u8>; LEAN_OBJECT_COUNT],
    compression_scratch: Vec<u8>,
    source_field_bytes: [u64; LEAN_OBJECT_COUNT],
    record_counts: [u64; LEAN_OBJECT_COUNT],
    transaction_count: u32,
    missing_metadata: u64,
    decoded_metadata: u64,
    raw_metadata: u64,
    tx_raw_flags: u64,
    nonempty_semantic_transaction_rewards: u64,
    max_live_bytes: usize,
    max_scratch_capacity: usize,
    max_retained_capacity: usize,
}

impl Default for LeanWorkerScratch {
    fn default() -> Self {
        Self {
            raw: std::array::from_fn(|_| Vec::new()),
            compression_scratch: Vec::new(),
            source_field_bytes: [0; LEAN_OBJECT_COUNT],
            record_counts: [0; LEAN_OBJECT_COUNT],
            transaction_count: 0,
            missing_metadata: 0,
            decoded_metadata: 0,
            raw_metadata: 0,
            tx_raw_flags: 0,
            nonempty_semantic_transaction_rewards: 0,
            max_live_bytes: 0,
            max_scratch_capacity: 0,
            max_retained_capacity: 0,
        }
    }
}

impl LeanWorkerScratch {
    fn begin_block(&mut self) {
        for raw in &mut self.raw {
            raw.clear();
        }
        self.source_field_bytes.fill(0);
        self.record_counts.fill(0);
        self.transaction_count = 0;
        self.missing_metadata = 0;
        self.decoded_metadata = 0;
        self.raw_metadata = 0;
        self.tx_raw_flags = 0;
        self.nonempty_semantic_transaction_rewards = 0;
        self.max_live_bytes = 0;
        self.max_scratch_capacity = 0;
    }

    fn raw_length(&self) -> Result<usize> {
        self.raw.iter().try_fold(0usize, |total, bytes| {
            total
                .checked_add(bytes.len())
                .context("aggregate lean decoded length overflow")
        })
    }

    fn raw_capacity(&self) -> Result<usize> {
        self.raw
            .iter()
            .try_fold(self.compression_scratch.capacity(), |total, bytes| {
                total
                    .checked_add(bytes.capacity())
                    .context("aggregate lean scratch capacity overflow")
            })
    }

    fn note_scratch(&mut self) -> Result<()> {
        let length = self
            .raw_length()?
            .checked_add(self.compression_scratch.len())
            .context("aggregate lean live scratch length overflow")?;
        ensure!(
            length <= MAX_LEAN_SCRATCH_BYTES_PER_WORKER,
            "aggregate lean decoded scratch {length} exceeds the worker cap"
        );
        self.max_live_bytes = self.max_live_bytes.max(length);
        let capacity = self.raw_capacity()?;
        ensure!(
            capacity <= MAX_LEAN_SCRATCH_BYTES_PER_WORKER,
            "aggregate lean scratch capacity {capacity} exceeds the worker cap"
        );
        self.max_scratch_capacity = self.max_scratch_capacity.max(capacity);
        Ok(())
    }

    fn reserve_append(
        &mut self,
        object: LeanObject,
        additional: usize,
        kind: &'static str,
    ) -> Result<()> {
        let next_length = self
            .raw_length()?
            .checked_add(additional)
            .context("aggregate lean append length overflow")?;
        ensure!(
            next_length <= MAX_LEAN_SCRATCH_BYTES_PER_WORKER,
            "aggregate lean decoded scratch {next_length} exceeds the worker cap"
        );
        let index = object.index();
        let required = self.raw[index]
            .len()
            .checked_add(additional)
            .context("lean object append length overflow")?;
        if required > self.raw[index].capacity() {
            let old_capacity = self.raw[index].capacity();
            let other_capacity = self
                .raw_capacity()?
                .checked_sub(old_capacity)
                .context("lean scratch capacity accounting underflow")?;
            let available = MAX_LEAN_SCRATCH_BYTES_PER_WORKER
                .checked_sub(other_capacity)
                .context("lean scratch capacity already exceeds worker cap")?;
            ensure!(
                required <= available,
                "aggregate lean scratch cannot fit {} {kind}",
                object.name()
            );
            let desired = required
                .max(old_capacity.saturating_mul(2))
                .max(4 << 10)
                .min(available);
            let reserve = desired
                .checked_sub(self.raw[index].len())
                .context("lean reserve underflow")?;
            self.raw[index]
                .try_reserve_exact(reserve)
                .with_context(|| format!("reserve {} {kind}", object.name()))?;
        }
        self.note_scratch()
    }

    fn append_exact(&mut self, object: LeanObject, parts: &[&[u8]], prefix: &[u8]) -> Result<()> {
        let source_length = parts.iter().try_fold(0usize, |total, part| {
            total
                .checked_add(part.len())
                .context("lean exact source-field length overflow")
        })?;
        let decoded_length = prefix
            .len()
            .checked_add(source_length)
            .context("lean decoded record length overflow")?;
        self.reserve_append(object, decoded_length, "exact record")?;
        let raw = &mut self.raw[object.index()];
        raw.extend_from_slice(prefix);
        for part in parts {
            raw.extend_from_slice(part);
        }
        self.source_field_bytes[object.index()] = self.source_field_bytes[object.index()]
            .checked_add(
                u64::try_from(source_length).context("lean source-field bytes exceed u64")?,
            )
            .context("lean source-field byte count overflow")?;
        self.record_counts[object.index()] = self.record_counts[object.index()]
            .checked_add(1)
            .context("lean record count overflow")?;
        Ok(())
    }

    fn finish_transaction(&mut self, effect_state: u8, source_flags: u32) -> Result<()> {
        ensure!(
            source_flags & !SOURCE_TX_FLAG_MASK == 0,
            "lean transaction has unknown source flag bits"
        );
        let source_flags =
            u16::try_from(source_flags).context("lean source transaction flags exceed u16")?;
        let mut ends = [0_u32; 5];
        for (position, object) in LeanObject::DENSE_TX_PLANES.into_iter().enumerate() {
            ends[position] = u32::try_from(self.raw[object.index()].len())
                .with_context(|| format!("{} block chunk exceeds u32", object.name()))?;
        }
        self.reserve_append(
            LeanObject::TransactionDirectory,
            LEAN_DIRECTORY_ROW_LEN,
            "transaction row",
        )?;
        let directory = &mut self.raw[LeanObject::TransactionDirectory.index()];
        directory.extend_from_slice(&source_flags.to_le_bytes());
        directory.push(effect_state);
        directory.push(0);
        for end in ends {
            directory.extend_from_slice(&end.to_le_bytes());
        }
        self.record_counts[LeanObject::TransactionDirectory.index()] = self.record_counts
            [LeanObject::TransactionDirectory.index()]
        .checked_add(1)
        .context("lean directory record count overflow")?;
        self.transaction_count = self
            .transaction_count
            .checked_add(1)
            .context("lean transaction count overflow")?;
        if u32::from(source_flags) & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            self.tx_raw_flags = self
                .tx_raw_flags
                .checked_add(1)
                .context("lean raw-transaction flag count overflow")?;
        }
        self.note_scratch()
    }

    fn record_missing_metadata(&mut self, source_flags: u32) -> Result<()> {
        self.missing_metadata = self
            .missing_metadata
            .checked_add(1)
            .context("lean missing-metadata count overflow")?;
        self.finish_transaction(0, source_flags)
    }

    fn record_raw_metadata(
        &mut self,
        tx_index: u32,
        source_flags: u32,
        bytes: &[u8],
    ) -> Result<()> {
        self.raw_metadata = self
            .raw_metadata
            .checked_add(1)
            .context("lean raw-metadata count overflow")?;
        let length = u32::try_from(bytes.len()).context("lean raw metadata exceeds u32")?;
        let mut prefix = [0_u8; 8];
        prefix[..4].copy_from_slice(&tx_index.to_le_bytes());
        prefix[4..].copy_from_slice(&length.to_le_bytes());
        self.append_exact(LeanObject::RawMetadataFallbacks, &[bytes], &prefix)?;
        self.finish_transaction(0, source_flags)
    }

    fn record_decoded_metadata(
        &mut self,
        tx_index: u32,
        source_flags: u32,
        effects: &decode::StreamedMetadataEffects<'_>,
    ) -> Result<()> {
        self.decoded_metadata = self
            .decoded_metadata
            .checked_add(1)
            .context("lean decoded-metadata count overflow")?;
        let fields = effects.fields;
        self.append_exact(
            LeanObject::Outcomes,
            &[fields.outcome_head, fields.outcome_tail],
            &[],
        )?;
        self.append_exact(
            LeanObject::Balances,
            &[fields.pre_balances, fields.post_balances],
            &[],
        )?;

        let cpi_state = if !effects.shape.inner_instructions_present {
            1
        } else if effects.inner_group_count == 0 {
            2
        } else {
            3
        };
        self.append_exact(
            LeanObject::InnerInstructions,
            &[fields.inner_instructions],
            &[],
        )?;
        let token_present = effects
            .pre_token_balance_count
            .checked_add(effects.post_token_balance_count)
            .context("lean token-balance count overflow")?
            != 0;
        self.append_exact(
            LeanObject::TokenBalances,
            &[fields.pre_token_balances, fields.post_token_balances],
            &[],
        )?;
        self.append_exact(LeanObject::Logs, &[fields.logs], &[])?;
        // Store a parser-accepted noncanonical empty Vec for exact source-byte
        // preservation, but keep effect bit 7 tied to a semantic nonempty Vec.
        let semantic_reward_present = effects.transaction_reward_count != 0;
        if semantic_reward_present {
            self.nonempty_semantic_transaction_rewards = self
                .nonempty_semantic_transaction_rewards
                .checked_add(1)
                .context("lean nonempty transaction-reward count overflow")?;
        }
        let reward_record_present = semantic_reward_present || fields.transaction_rewards != [0];
        if reward_record_present {
            self.append_exact(
                LeanObject::TransactionRewards,
                &[fields.transaction_rewards],
                &tx_index.to_le_bytes(),
            )?;
        }
        let mut effect_state = cpi_state | (1 << 3) | (1 << 4);
        if token_present {
            effect_state |= 1 << 5;
        }
        if effects.logs_present {
            effect_state |= 1 << 6;
        }
        if semantic_reward_present {
            effect_state |= 1 << 7;
        }
        self.finish_transaction(effect_state, source_flags)
    }

    fn record_block_rewards(&mut self, exact_option_bytes: &[u8]) -> Result<()> {
        ensure!(
            !exact_option_bytes.is_empty(),
            "lean block reward Option field is empty"
        );
        if exact_option_bytes[0] == 0 {
            ensure!(
                exact_option_bytes.len() == 1,
                "absent lean block rewards have trailing bytes"
            );
        } else {
            ensure!(
                exact_option_bytes[0] == 1,
                "lean block rewards have an invalid Option tag"
            );
        }
        self.append_exact(LeanObject::BlockRewards, &[exact_option_bytes], &[])
    }

    fn finish_block(&mut self, expected_transactions: u32) -> Result<()> {
        ensure!(
            self.transaction_count == expected_transactions,
            "lean directory has {} transactions, expected {expected_transactions}",
            self.transaction_count
        );
        let expected_directory_len = usize::try_from(expected_transactions)
            .context("lean transaction count exceeds usize")?
            .checked_mul(LEAN_DIRECTORY_ROW_LEN)
            .context("lean directory length overflow")?;
        ensure!(
            self.raw[LeanObject::TransactionDirectory.index()].len() == expected_directory_len,
            "lean directory geometry differs from its transaction count"
        );
        self.note_scratch()
    }

    fn trim_retained(&mut self) -> Result<usize> {
        for raw in &mut self.raw {
            raw.clear();
        }
        self.compression_scratch.clear();
        loop {
            let capacity = self.raw_capacity()?;
            if capacity <= MAX_RETAINED_LEAN_SCRATCH_BYTES_PER_WORKER {
                self.max_retained_capacity = self.max_retained_capacity.max(capacity);
                return Ok(capacity);
            }
            let mut largest = None;
            let mut largest_capacity = self.compression_scratch.capacity();
            for (index, raw) in self.raw.iter().enumerate() {
                if raw.capacity() > largest_capacity {
                    largest = Some(index);
                    largest_capacity = raw.capacity();
                }
            }
            ensure!(largest_capacity != 0, "lean scratch trim made no progress");
            if let Some(index) = largest {
                self.raw[index] = Vec::new();
            } else {
                self.compression_scratch = Vec::new();
            }
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct LeanObjectStats {
    blocks: u64,
    records: u64,
    source_field_bytes: u64,
    decoded_bytes: u64,
    stored_bytes: u64,
    raw_blocks: u64,
    zstd_blocks: u64,
    compression_attempts: u64,
    compression_time: Duration,
}

impl LeanObjectStats {
    fn merge(&mut self, other: Self) {
        self.blocks += other.blocks;
        self.records += other.records;
        self.source_field_bytes += other.source_field_bytes;
        self.decoded_bytes += other.decoded_bytes;
        self.stored_bytes += other.stored_bytes;
        self.raw_blocks += other.raw_blocks;
        self.zstd_blocks += other.zstd_blocks;
        self.compression_attempts += other.compression_attempts;
        self.compression_time = self.compression_time.saturating_add(other.compression_time);
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct LeanProjectionStats {
    objects: [LeanObjectStats; LEAN_OBJECT_COUNT],
    missing_metadata: u64,
    decoded_metadata: u64,
    raw_metadata: u64,
    tx_raw_flags: u64,
    nonempty_semantic_transaction_rewards: u64,
    max_live_scratch_bytes: usize,
    max_scratch_capacity: usize,
    max_retained_scratch_capacity: usize,
    max_block_packed_bytes: usize,
}

impl LeanProjectionStats {
    fn merge(&mut self, other: Self) {
        for object in LeanObject::ALL {
            self.objects[object.index()].merge(other.objects[object.index()]);
        }
        self.missing_metadata += other.missing_metadata;
        self.decoded_metadata += other.decoded_metadata;
        self.raw_metadata += other.raw_metadata;
        self.tx_raw_flags += other.tx_raw_flags;
        self.nonempty_semantic_transaction_rewards += other.nonempty_semantic_transaction_rewards;
        self.max_live_scratch_bytes = self
            .max_live_scratch_bytes
            .max(other.max_live_scratch_bytes);
        self.max_scratch_capacity = self.max_scratch_capacity.max(other.max_scratch_capacity);
        self.max_retained_scratch_capacity = self
            .max_retained_scratch_capacity
            .max(other.max_retained_scratch_capacity);
        self.max_block_packed_bytes = self
            .max_block_packed_bytes
            .max(other.max_block_packed_bytes);
    }
}

#[derive(Debug)]
struct ProjectedLeanEffects {
    packed: Vec<u8>,
    ranges: [Range<usize>; LEAN_OBJECT_COUNT],
    decoded_lengths: [u32; LEAN_OBJECT_COUNT],
    compressed: [bool; LEAN_OBJECT_COUNT],
    stats: LeanProjectionStats,
}

fn encode_lean_effects(
    scratch: &mut LeanWorkerScratch,
    compressor: &mut zstd::bulk::Compressor<'static>,
    mode: LeanCompressionArg,
) -> Result<ProjectedLeanEffects> {
    let mut packed = Vec::new();
    let mut ranges: [Range<usize>; LEAN_OBJECT_COUNT] = std::array::from_fn(|_| 0usize..0usize);
    let mut decoded_lengths = [0_u32; LEAN_OBJECT_COUNT];
    let mut compressed = [false; LEAN_OBJECT_COUNT];
    let mut stats = LeanProjectionStats {
        missing_metadata: scratch.missing_metadata,
        decoded_metadata: scratch.decoded_metadata,
        raw_metadata: scratch.raw_metadata,
        tx_raw_flags: scratch.tx_raw_flags,
        nonempty_semantic_transaction_rewards: scratch.nonempty_semantic_transaction_rewards,
        max_live_scratch_bytes: scratch.max_live_bytes,
        max_scratch_capacity: scratch.max_scratch_capacity,
        ..LeanProjectionStats::default()
    };

    for object in LeanObject::ALL {
        let index = object.index();
        let raw_len = scratch.raw[index].len();
        let range_start = packed.len();
        stats.objects[index].records = scratch.record_counts[index];
        stats.objects[index].source_field_bytes = scratch.source_field_bytes[index];
        if raw_len == 0 {
            ranges[index] = range_start..range_start;
            continue;
        }
        ensure!(
            raw_len <= LEAN_STORED_LEN_MASK as usize,
            "{} decoded chunk exceeds the packed locator limit",
            object.name()
        );
        decoded_lengths[index] = u32::try_from(raw_len)
            .with_context(|| format!("{} decoded chunk exceeds u32", object.name()))?;
        stats.objects[index].blocks = 1;
        stats.objects[index].decoded_bytes = u64::from(decoded_lengths[index]);

        let mut compression_elapsed = Duration::ZERO;
        let object_compression = mode.object_compression(object);
        let should_try_zstd = object_compression != LeanObjectCompression::Raw;
        if should_try_zstd {
            stats.objects[index].compression_attempts = 1;
            scratch.compression_scratch.clear();
            let bound = zstd::zstd_safe::compress_bound(raw_len);
            let raw_capacity = scratch.raw.iter().try_fold(0usize, |total, raw| {
                total
                    .checked_add(raw.capacity())
                    .context("aggregate lean raw capacity overflow")
            })?;
            ensure!(
                raw_capacity
                    .checked_add(bound)
                    .is_some_and(|capacity| capacity <= MAX_LEAN_SCRATCH_BYTES_PER_WORKER),
                "aggregate lean raw capacity plus zstd bound exceeds the worker cap"
            );
            if scratch.compression_scratch.capacity() < bound {
                let mut replacement = Vec::new();
                replacement
                    .try_reserve_exact(bound)
                    .with_context(|| format!("reserve {} lean zstd scratch", object.name()))?;
                scratch.compression_scratch = replacement;
            }
            scratch.note_scratch()?;
            let started = Instant::now();
            compressor
                .compress_to_buffer(&scratch.raw[index], &mut scratch.compression_scratch)
                .with_context(|| format!("compress {} lean block chunk", object.name()))?;
            compression_elapsed = started.elapsed();
            scratch.note_scratch()?;
        }
        let use_zstd = match object_compression {
            LeanObjectCompression::Raw => false,
            LeanObjectCompression::Zstd => true,
            LeanObjectCompression::Adaptive => scratch.compression_scratch.len() < raw_len,
        };
        let stored = if use_zstd {
            scratch.compression_scratch.as_slice()
        } else {
            scratch.raw[index].as_slice()
        };
        ensure!(
            stored.len() <= LEAN_STORED_LEN_MASK as usize,
            "{} stored chunk exceeds the packed locator limit",
            object.name()
        );
        let required = packed
            .len()
            .checked_add(stored.len())
            .context("lean packed block length overflow")?;
        ensure!(
            required <= MAX_LEAN_PACKED_BYTES_PER_BLOCK,
            "lean packed block exceeds the worker cap"
        );
        if required > packed.capacity() {
            let desired = required
                .max(packed.capacity().saturating_mul(2))
                .min(MAX_LEAN_PACKED_BYTES_PER_BLOCK);
            packed
                .try_reserve_exact(
                    desired
                        .checked_sub(packed.len())
                        .context("lean packed reserve underflow")?,
                )
                .with_context(|| format!("reserve {} lean stored chunk", object.name()))?;
        }
        packed.extend_from_slice(stored);
        ranges[index] = range_start..packed.len();
        compressed[index] = use_zstd;
        stats.objects[index].stored_bytes =
            u64::try_from(stored.len()).context("lean stored bytes exceed u64")?;
        stats.objects[index].raw_blocks = u64::from(!use_zstd);
        stats.objects[index].zstd_blocks = u64::from(use_zstd);
        stats.objects[index].compression_time = compression_elapsed;
    }

    stats.max_live_scratch_bytes = stats
        .max_live_scratch_bytes
        .max(scratch.max_live_bytes)
        .max(
            scratch
                .raw_length()?
                .checked_add(scratch.compression_scratch.len())
                .context("aggregate lean live scratch overflow")?,
        );
    stats.max_scratch_capacity = stats.max_scratch_capacity.max(scratch.raw_capacity()?);
    stats.max_block_packed_bytes = packed.len();
    stats.max_retained_scratch_capacity = scratch.trim_retained()?;
    Ok(ProjectedLeanEffects {
        packed,
        ranges,
        decoded_lengths,
        compressed,
        stats,
    })
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct SplitPlaneStats {
    frames: u64,
    records: u64,
    source_field_bytes: u64,
    decoded_bytes: u64,
    stored_bytes: u64,
}

impl SplitPlaneStats {
    fn merge(&mut self, other: Self) {
        self.frames += other.frames;
        self.records += other.records;
        self.source_field_bytes += other.source_field_bytes;
        self.decoded_bytes += other.decoded_bytes;
        self.stored_bytes += other.stored_bytes;
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct SplitProjectionStats {
    planes: [SplitPlaneStats; SPLIT_PLANE_COUNT],
    missing_metadata: u64,
    decoded_metadata: u64,
    raw_metadata: u64,
    tx_raw_flags: u64,
    max_worker_aggregate_raw_bytes: usize,
    max_worker_aggregate_scratch_capacity: usize,
    max_worker_total_scratch_capacity: usize,
    max_worker_retained_raw_capacity: usize,
    max_worker_retained_chunk_capacity: usize,
    max_worker_retained_total_scratch_capacity: usize,
    max_block_stored_bytes: usize,
}

impl SplitProjectionStats {
    fn merge(&mut self, other: Self) {
        for plane in SplitPlane::ALL {
            self.planes[plane.index()].merge(other.planes[plane.index()]);
        }
        self.missing_metadata += other.missing_metadata;
        self.decoded_metadata += other.decoded_metadata;
        self.raw_metadata += other.raw_metadata;
        self.tx_raw_flags += other.tx_raw_flags;
        self.max_worker_aggregate_raw_bytes = self
            .max_worker_aggregate_raw_bytes
            .max(other.max_worker_aggregate_raw_bytes);
        self.max_worker_aggregate_scratch_capacity = self
            .max_worker_aggregate_scratch_capacity
            .max(other.max_worker_aggregate_scratch_capacity);
        self.max_worker_total_scratch_capacity = self
            .max_worker_total_scratch_capacity
            .max(other.max_worker_total_scratch_capacity);
        self.max_worker_retained_raw_capacity = self
            .max_worker_retained_raw_capacity
            .max(other.max_worker_retained_raw_capacity);
        self.max_worker_retained_chunk_capacity = self
            .max_worker_retained_chunk_capacity
            .max(other.max_worker_retained_chunk_capacity);
        self.max_worker_retained_total_scratch_capacity = self
            .max_worker_retained_total_scratch_capacity
            .max(other.max_worker_retained_total_scratch_capacity);
        self.max_block_stored_bytes = self
            .max_block_stored_bytes
            .max(other.max_block_stored_bytes);
    }
}

#[derive(Debug)]
struct ProjectedSplitEffects {
    packed: Vec<u8>,
    ranges: [Range<usize>; SPLIT_PLANE_COUNT],
    stats: SplitProjectionStats,
    compression_time: Duration,
}

struct SplitWriters {
    plane_writers: Vec<BufWriter<File>>,
    index_writer: BufWriter<File>,
    plane_offsets: [u64; SPLIT_PLANE_COUNT],
    row_count: usize,
    stats: SplitProjectionStats,
    compression_time: Duration,
    ordered_write_time: Duration,
}

struct SplitOutputSummary {
    plane_file_bytes: [u64; SPLIT_PLANE_COUNT],
    index_bytes: u64,
    output_bytes: u64,
    stats: SplitProjectionStats,
    compression_time: Duration,
    ordered_write_time: Duration,
    finalize_time: Duration,
}

#[derive(Clone, Copy)]
struct SplitHeaderBinding {
    epoch: u64,
    slots_per_epoch: u64,
    selected_blocks: u64,
    selected_transactions: u64,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    prefix: bool,
}

impl SplitWriters {
    fn create(staging: &Path, binding: SplitHeaderBinding) -> Result<Self> {
        let mut plane_writers = Vec::new();
        plane_writers
            .try_reserve_exact(SPLIT_PLANE_COUNT)
            .context("reserve split plane writers")?;
        for plane in SplitPlane::ALL {
            let path = staging.join(plane.file_name());
            let file = File::create(&path)
                .with_context(|| format!("create split plane {}", path.display()))?;
            let mut writer = BufWriter::with_capacity(8 << 20, file);
            write_split_file_header(&mut writer, SPLIT_DATA_MAGIC, plane as u16, binding)?;
            plane_writers.push(writer);
        }
        let index_path = staging.join(SPLIT_INDEX_FILE);
        let index_file = File::create(&index_path)
            .with_context(|| format!("create split index {}", index_path.display()))?;
        let mut index_writer = BufWriter::with_capacity(8 << 20, index_file);
        write_split_file_header(&mut index_writer, SPLIT_INDEX_MAGIC, u16::MAX, binding)?;
        Ok(Self {
            plane_writers,
            index_writer,
            plane_offsets: [SPLIT_FILE_HEADER_LEN as u64; SPLIT_PLANE_COUNT],
            row_count: 0,
            stats: SplitProjectionStats::default(),
            compression_time: Duration::ZERO,
            ordered_write_time: Duration::ZERO,
        })
    }

    fn append(
        &mut self,
        row: ArchiveV2HotBlockIndexRow,
        split: ProjectedSplitEffects,
    ) -> Result<()> {
        let started = Instant::now();
        ensure!(
            row.block_id as usize == self.row_count,
            "split index block id {} is not row {}",
            row.block_id,
            self.row_count
        );
        let mut packed_position = 0usize;
        let mut spans = [(0_u64, 0_u64); SPLIT_PLANE_COUNT];
        for plane in SplitPlane::ALL {
            let index = plane.index();
            let range = &split.ranges[index];
            ensure!(
                range.start == packed_position
                    && range.start <= range.end
                    && range.end <= split.packed.len(),
                "{} packed split range is not contiguous",
                plane.name()
            );
            let bytes = &split.packed[range.clone()];
            let length = u64::try_from(bytes.len())
                .with_context(|| format!("{} packed split range exceeds u64", plane.name()))?;
            ensure!(
                length == split.stats.planes[index].stored_bytes,
                "{} packed split range has {length} bytes but stats report {}",
                plane.name(),
                split.stats.planes[index].stored_bytes
            );
            let offset = self.plane_offsets[index];
            self.plane_writers[index]
                .write_all(bytes)
                .with_context(|| {
                    format!("append block {} {} frames", row.block_id, plane.name())
                })?;
            self.plane_offsets[index] = offset
                .checked_add(length)
                .context("split plane offset overflow")?;
            spans[index] = (offset, length);
            packed_position = range.end;
        }
        ensure!(
            packed_position == split.packed.len(),
            "packed split block has trailing bytes"
        );

        self.index_writer.write_all(&row.block_id.to_le_bytes())?;
        self.index_writer.write_all(&row.slot.to_le_bytes())?;
        self.index_writer.write_all(&row.tx_count.to_le_bytes())?;
        for (offset, length) in spans {
            self.index_writer.write_all(&offset.to_le_bytes())?;
            self.index_writer.write_all(&length.to_le_bytes())?;
        }
        self.row_count = self
            .row_count
            .checked_add(1)
            .context("split index row count overflow")?;
        self.stats.merge(split.stats);
        self.compression_time = self.compression_time.saturating_add(split.compression_time);
        self.ordered_write_time = self.ordered_write_time.saturating_add(started.elapsed());
        Ok(())
    }

    fn finish(mut self, expected_rows: usize) -> Result<SplitOutputSummary> {
        let started = Instant::now();
        ensure!(
            self.row_count == expected_rows,
            "split index has {} rows, expected {expected_rows}",
            self.row_count
        );
        for (index, mut writer) in self.plane_writers.drain(..).enumerate() {
            writer
                .flush()
                .with_context(|| format!("flush {} split plane", SplitPlane::ALL[index].name()))?;
            let file = writer
                .into_inner()
                .with_context(|| format!("finish {} split plane", SplitPlane::ALL[index].name()))?;
            file.sync_all()
                .with_context(|| format!("sync {} split plane", SplitPlane::ALL[index].name()))?;
        }
        self.index_writer.flush().context("flush split index")?;
        let index_file = self
            .index_writer
            .into_inner()
            .context("finish split index writer")?;
        index_file.sync_all().context("sync split index")?;
        let index_bytes = u64::try_from(SPLIT_FILE_HEADER_LEN)
            .expect("fixed split header fits u64")
            .checked_add(
                u64::try_from(self.row_count)
                    .context("split row count exceeds u64")?
                    .checked_mul(
                        u64::try_from(SPLIT_INDEX_ROW_LEN).expect("fixed split index row fits u64"),
                    )
                    .context("split index row bytes overflow")?,
            )
            .context("split index byte count overflow")?;
        let plane_bytes = self.plane_offsets.iter().try_fold(0_u64, |total, length| {
            total
                .checked_add(*length)
                .context("split plane byte total overflow")
        })?;
        let output_bytes = plane_bytes
            .checked_add(index_bytes)
            .context("split output byte total overflow")?;
        Ok(SplitOutputSummary {
            plane_file_bytes: self.plane_offsets,
            index_bytes,
            output_bytes,
            stats: self.stats,
            compression_time: self.compression_time,
            ordered_write_time: self.ordered_write_time,
            finalize_time: started.elapsed(),
        })
    }
}

fn write_split_file_header(
    writer: &mut impl Write,
    magic: [u8; 8],
    plane: u16,
    binding: SplitHeaderBinding,
) -> Result<()> {
    writer.write_all(&magic)?;
    writer.write_all(&SPLIT_FORMAT_VERSION.to_le_bytes())?;
    writer.write_all(&plane.to_le_bytes())?;
    writer.write_all(&0_u32.to_le_bytes())?;
    writer.write_all(&binding.epoch.to_le_bytes())?;
    writer.write_all(&binding.selected_blocks.to_le_bytes())?;
    writer.write_all(&binding.selected_transactions.to_le_bytes())?;
    writer.write_all(&[message_schema_code(binding.message_schema)])?;
    writer.write_all(&[metadata_schema_code(binding.metadata_schema)])?;
    writer.write_all(&[1])?; // Current outer Archive V2 profile.
    writer.write_all(&[u8::from(binding.prefix)])?;
    writer.write_all(&binding.slots_per_epoch.to_le_bytes())?;
    writer.write_all(&[0_u8; 12])?;
    debug_assert_eq!(SPLIT_FILE_HEADER_LEN, 64);
    Ok(())
}

struct LeanWriters {
    object_writers: Vec<BufWriter<File>>,
    index_writer: BufWriter<File>,
    object_offsets: [u64; LEAN_OBJECT_COUNT],
    row_count: usize,
    stats: LeanProjectionStats,
    ordered_write_time: Duration,
}

struct LeanOutputSummary {
    object_file_bytes: [u64; LEAN_OBJECT_COUNT],
    index_bytes: u64,
    output_bytes: u64,
    stats: LeanProjectionStats,
    ordered_write_time: Duration,
    finalize_time: Duration,
}

impl LeanWriters {
    fn create(
        staging: &Path,
        binding: SplitHeaderBinding,
        mode: LeanCompressionArg,
        zstd_level: LeanZstdLevelArg,
    ) -> Result<Self> {
        let mut object_writers = Vec::new();
        object_writers
            .try_reserve_exact(LEAN_OBJECT_COUNT)
            .context("reserve lean object writers")?;
        for object in LeanObject::ALL {
            let path = staging.join(object.file_name());
            let file = File::create(&path)
                .with_context(|| format!("create lean object {}", path.display()))?;
            let mut writer = BufWriter::with_capacity(8 << 20, file);
            write_lean_file_header(
                &mut writer,
                LEAN_DATA_MAGIC,
                object as u16,
                binding,
                mode,
                zstd_level,
            )?;
            object_writers.push(writer);
        }
        let index_path = staging.join(LEAN_INDEX_FILE);
        let index_file = File::create(&index_path)
            .with_context(|| format!("create lean index {}", index_path.display()))?;
        let mut index_writer = BufWriter::with_capacity(8 << 20, index_file);
        write_lean_file_header(
            &mut index_writer,
            LEAN_INDEX_MAGIC,
            u16::MAX,
            binding,
            mode,
            zstd_level,
        )?;
        Ok(Self {
            object_writers,
            index_writer,
            object_offsets: [LEAN_FILE_HEADER_LEN as u64; LEAN_OBJECT_COUNT],
            row_count: 0,
            stats: LeanProjectionStats::default(),
            ordered_write_time: Duration::ZERO,
        })
    }

    fn append(&mut self, row: ArchiveV2HotBlockIndexRow, lean: ProjectedLeanEffects) -> Result<()> {
        let started = Instant::now();
        ensure!(
            row.block_id as usize == self.row_count,
            "lean index block id {} is not row {}",
            row.block_id,
            self.row_count
        );
        self.index_writer.write_all(&row.block_id.to_le_bytes())?;
        self.index_writer.write_all(&row.tx_count.to_le_bytes())?;
        self.index_writer.write_all(&row.slot.to_le_bytes())?;

        let mut packed_position = 0usize;
        for object in LeanObject::ALL {
            let index = object.index();
            let range = &lean.ranges[index];
            ensure!(
                range.start == packed_position
                    && range.start <= range.end
                    && range.end <= lean.packed.len(),
                "{} packed lean range is not contiguous",
                object.name()
            );
            let bytes = &lean.packed[range.clone()];
            ensure!(
                bytes.len() <= LEAN_STORED_LEN_MASK as usize,
                "{} stored lean chunk exceeds locator capacity",
                object.name()
            );
            ensure!(
                u64::try_from(bytes.len()).context("lean stored bytes exceed u64")?
                    == lean.stats.objects[index].stored_bytes,
                "{} packed length differs from its stats",
                object.name()
            );
            let decoded_len = lean.decoded_lengths[index];
            ensure!(
                decoded_len <= LEAN_STORED_LEN_MASK,
                "{} decoded lean chunk exceeds locator capacity",
                object.name()
            );
            let offset = self.object_offsets[index];
            let stored_len = u32::try_from(bytes.len())
                .with_context(|| format!("{} stored lean chunk exceeds u32", object.name()))?;
            let stored_len_and_codec = if lean.compressed[index] {
                ensure!(stored_len != 0, "empty lean chunk cannot be zstd");
                stored_len | LEAN_ZSTD_CODEC_BIT
            } else {
                ensure!(
                    stored_len == decoded_len,
                    "raw {} lean chunk stored and decoded lengths differ",
                    object.name()
                );
                stored_len
            };
            if stored_len == 0 {
                ensure!(
                    decoded_len == 0 && !lean.compressed[index],
                    "absent {} lean chunk has inconsistent geometry",
                    object.name()
                );
            }
            self.object_writers[index]
                .write_all(bytes)
                .with_context(|| {
                    format!("append block {} {} lean chunk", row.block_id, object.name())
                })?;
            self.object_offsets[index] = offset
                .checked_add(u64::from(stored_len))
                .context("lean object offset overflow")?;
            self.index_writer.write_all(&offset.to_le_bytes())?;
            self.index_writer
                .write_all(&stored_len_and_codec.to_le_bytes())?;
            self.index_writer.write_all(&decoded_len.to_le_bytes())?;
            packed_position = range.end;
        }
        ensure!(
            packed_position == lean.packed.len(),
            "packed lean block has trailing bytes"
        );
        self.row_count = self
            .row_count
            .checked_add(1)
            .context("lean index row count overflow")?;
        self.stats.merge(lean.stats);
        self.ordered_write_time = self.ordered_write_time.saturating_add(started.elapsed());
        Ok(())
    }

    fn finish(mut self, expected_rows: usize) -> Result<LeanOutputSummary> {
        let started = Instant::now();
        ensure!(
            self.row_count == expected_rows,
            "lean index has {} rows, expected {expected_rows}",
            self.row_count
        );
        for (index, mut writer) in self.object_writers.drain(..).enumerate() {
            writer
                .flush()
                .with_context(|| format!("flush {} lean object", LeanObject::ALL[index].name()))?;
            let file = writer
                .into_inner()
                .with_context(|| format!("finish {} lean object", LeanObject::ALL[index].name()))?;
            file.sync_all()
                .with_context(|| format!("sync {} lean object", LeanObject::ALL[index].name()))?;
        }
        self.index_writer.flush().context("flush lean index")?;
        let index_file = self
            .index_writer
            .into_inner()
            .context("finish lean index writer")?;
        index_file.sync_all().context("sync lean index")?;
        let index_bytes = u64::try_from(LEAN_FILE_HEADER_LEN)
            .expect("fixed lean header fits u64")
            .checked_add(
                u64::try_from(self.row_count)
                    .context("lean row count exceeds u64")?
                    .checked_mul(
                        u64::try_from(LEAN_INDEX_ROW_LEN).expect("fixed lean row fits u64"),
                    )
                    .context("lean index row byte count overflow")?,
            )
            .context("lean index byte count overflow")?;
        let object_bytes = self.object_offsets.iter().try_fold(0_u64, |total, bytes| {
            total
                .checked_add(*bytes)
                .context("lean object byte total overflow")
        })?;
        let output_bytes = object_bytes
            .checked_add(index_bytes)
            .context("lean output byte total overflow")?;
        Ok(LeanOutputSummary {
            object_file_bytes: self.object_offsets,
            index_bytes,
            output_bytes,
            stats: self.stats,
            ordered_write_time: self.ordered_write_time,
            finalize_time: started.elapsed(),
        })
    }
}

fn write_lean_file_header(
    writer: &mut impl Write,
    magic: [u8; 8],
    object: u16,
    binding: SplitHeaderBinding,
    mode: LeanCompressionArg,
    zstd_level: LeanZstdLevelArg,
) -> Result<()> {
    writer.write_all(&magic)?;
    writer.write_all(&LEAN_FORMAT_VERSION.to_le_bytes())?;
    writer.write_all(&object.to_le_bytes())?;
    writer.write_all(&[mode.code()])?;
    writer.write_all(&[message_schema_code(binding.message_schema)])?;
    writer.write_all(&[metadata_schema_code(binding.metadata_schema)])?;
    writer.write_all(&[1])?; // Current outer Archive V2 profile.
    writer.write_all(&binding.epoch.to_le_bytes())?;
    writer.write_all(&binding.slots_per_epoch.to_le_bytes())?;
    writer.write_all(&binding.selected_blocks.to_le_bytes())?;
    writer.write_all(&binding.selected_transactions.to_le_bytes())?;
    writer.write_all(&[u8::from(binding.prefix)])?;
    writer.write_all(&(LEAN_DIRECTORY_ROW_LEN as u16).to_le_bytes())?;
    writer.write_all(&[LeanObject::DENSE_TX_PLANES.len() as u8])?;
    writer.write_all(&[2])?; // Sparse transaction planes.
    writer.write_all(&[LEAN_OBJECT_COUNT as u8])?;
    writer.write_all(&[zstd_level.header_code()])?;
    writer.write_all(&[0_u8; 9])?;
    debug_assert_eq!(LEAN_FILE_HEADER_LEN, 64);
    Ok(())
}

#[allow(clippy::large_enum_variant)] // Boxing adds one allocation to every decoded transaction.
enum PendingSplitMetadata<'de> {
    Missing,
    Raw(&'de [u8]),
    Decoded(decode::StreamedMetadataEffects<'de>),
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MessageSchemaArg {
    Current,
    May24PreUnknownFallbacks,
}

impl From<MessageSchemaArg> for CompactV2MessageSchema {
    fn from(value: MessageSchemaArg) -> Self {
        match value {
            MessageSchemaArg::Current => Self::Current,
            MessageSchemaArg::May24PreUnknownFallbacks => Self::May24PreUnknownFallbacks,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MetadataSchemaArg {
    CurrentTypedError,
    LegacyRawError,
}

impl From<MetadataSchemaArg> for CompactV2MetadataSchema {
    fn from(value: MetadataSchemaArg) -> Self {
        match value {
            MetadataSchemaArg::CurrentTypedError => Self::CurrentTypedError,
            MetadataSchemaArg::LegacyRawError => Self::LegacyRawError,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Build a non-publishable Archive V2 account-projection canary")]
struct Args {
    /// Immutable Archive V2 generation directory.
    source: PathBuf,
    /// Final output directory. A sibling staging directory is renamed here.
    output: PathBuf,
    /// Epoch identity used by the trusted local structural reader.
    #[arg(long)]
    epoch: u64,
    /// Slot count for this epoch schedule.
    #[arg(long)]
    slots_per_epoch: u64,
    /// Exact hot-message grammar. It is never inferred from transaction bytes.
    #[arg(long, value_enum)]
    message_schema: MessageSchemaArg,
    /// Exact transaction-metadata grammar. It is never inferred from rows.
    #[arg(long, value_enum)]
    metadata_schema: MetadataSchemaArg,
    /// Parallel borrowed block decode and account-projection workers.
    #[arg(long, default_value_t = 12)]
    workers: usize,
    /// Process only the first N blocks. The result remains non-publishable.
    #[arg(long)]
    benchmark_prefix_blocks: Option<usize>,
    /// Also write the provisional source-preserving metadata-effect planes.
    #[arg(long)]
    source_split_effects: bool,
    /// Write one provisional exact-field chunk per source block and object.
    #[arg(long)]
    lean_block_chunks: bool,
    /// Storage choice for each nonempty lean block chunk.
    #[arg(long, value_enum)]
    lean_compression: Option<LeanCompressionArg>,
    /// Zstd level for zstd, adaptive, and hybrid lean chunks.
    #[arg(long, value_enum)]
    lean_zstd_level: Option<LeanZstdLevelArg>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Outcome {
    Unknown = 0,
    Success = 1,
    Failed = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum AccountCoverage {
    Complete = 0,
    MissingLoadedMetadata = 1,
    RawTransactionFallback = 2,
    RawMetadataLoadedFallback = 3,
}

impl AccountCoverage {
    const fn bit(self) -> u32 {
        1 << self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum CpiCoverage {
    Recorded = 0,
    NotRecorded = 1,
    MissingMetadata = 2,
    RawTransactionFallback = 3,
    RawMetadataFallback = 4,
}

impl CpiCoverage {
    const fn bit(self) -> u32 {
        1 << (8 + self as u8)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AccountUse {
    key: CompactPubkey,
    roles: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectedTransaction {
    source_flags: u32,
    outcome: Outcome,
    account_coverage: AccountCoverage,
    cpi_coverage: CpiCoverage,
    accounts: Vec<AccountUse>,
}

#[derive(Debug)]
struct ProjectedBlock {
    row: ArchiveV2HotBlockIndexRow,
    page: Vec<u8>,
    decoded_page_len: u32,
    compressed: bool,
    stats: ProjectionStats,
    timing: ProjectionTiming,
    split_effects: Option<ProjectedSplitEffects>,
    lean_effects: Option<ProjectedLeanEffects>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct ProjectionStats {
    transactions: u64,
    account_refs: u64,
    id_refs: u64,
    raw_refs: u64,
    success: u64,
    failed: u64,
    unknown: u64,
    fully_covered: u64,
    account_complete: u64,
    account_missing_loaded_metadata: u64,
    account_raw_transaction_fallback: u64,
    account_raw_metadata_loaded_fallback: u64,
    cpi_recorded: u64,
    cpi_not_recorded: u64,
    cpi_missing_metadata: u64,
    cpi_raw_transaction_fallback: u64,
    cpi_raw_metadata_fallback: u64,
    source_raw_transaction_flags: u64,
    source_raw_metadata_flags: u64,
    source_fallback_rows: u64,
    max_resolved_source_positions: usize,
    max_unique_output_accounts: usize,
    duplicate_account_merges: u64,
}

impl ProjectionStats {
    fn merge(&mut self, other: Self) {
        self.transactions += other.transactions;
        self.account_refs += other.account_refs;
        self.id_refs += other.id_refs;
        self.raw_refs += other.raw_refs;
        self.success += other.success;
        self.failed += other.failed;
        self.unknown += other.unknown;
        self.fully_covered += other.fully_covered;
        self.account_complete += other.account_complete;
        self.account_missing_loaded_metadata += other.account_missing_loaded_metadata;
        self.account_raw_transaction_fallback += other.account_raw_transaction_fallback;
        self.account_raw_metadata_loaded_fallback += other.account_raw_metadata_loaded_fallback;
        self.cpi_recorded += other.cpi_recorded;
        self.cpi_not_recorded += other.cpi_not_recorded;
        self.cpi_missing_metadata += other.cpi_missing_metadata;
        self.cpi_raw_transaction_fallback += other.cpi_raw_transaction_fallback;
        self.cpi_raw_metadata_fallback += other.cpi_raw_metadata_fallback;
        self.source_raw_transaction_flags += other.source_raw_transaction_flags;
        self.source_raw_metadata_flags += other.source_raw_metadata_flags;
        self.source_fallback_rows += other.source_fallback_rows;
        self.max_resolved_source_positions = self
            .max_resolved_source_positions
            .max(other.max_resolved_source_positions);
        self.max_unique_output_accounts = self
            .max_unique_output_accounts
            .max(other.max_unique_output_accounts);
        self.duplicate_account_merges += other.duplicate_account_merges;
    }

    fn incomplete(&self) -> u64 {
        self.transactions.saturating_sub(self.fully_covered)
    }
}

#[derive(Debug, Clone, Copy)]
struct ProjectionConfig {
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    registry_entries: u32,
}

struct ProjectionWorker {
    config: ProjectionConfig,
    role_by_source_position: [u8; MAX_MESSAGE_ACCOUNTS],
    top_program_indexes: U8IndexSummary,
    top_account_indexes: U8IndexSummary,
    unique_position_by_source: [u16; MAX_MESSAGE_ACCOUNTS],
    accounts: Vec<AccountUse>,
    first_invalid_pubkey_id: Option<u32>,
    source_position_count: usize,
    source_position_overflow: bool,
    duplicate_merges: u64,
    decoded_page_scratch: Vec<u8>,
    compressor: zstd::bulk::Compressor<'static>,
    split: Option<SplitWorkerScratch>,
    lean: Option<LeanWorkerScratch>,
    lean_compression: LeanCompressionArg,
    lean_zstd_level: LeanZstdLevelArg,
    lean_compressor: Option<zstd::bulk::Compressor<'static>>,
}

impl ProjectionWorker {
    fn new(config: ProjectionConfig) -> Result<Self> {
        Ok(Self {
            config,
            role_by_source_position: [0; MAX_MESSAGE_ACCOUNTS],
            top_program_indexes: U8IndexSummary::default(),
            top_account_indexes: U8IndexSummary::default(),
            unique_position_by_source: [NO_ACCOUNT_POSITION; MAX_MESSAGE_ACCOUNTS],
            accounts: Vec::new(),
            first_invalid_pubkey_id: None,
            source_position_count: 0,
            source_position_overflow: false,
            duplicate_merges: 0,
            decoded_page_scratch: Vec::new(),
            compressor: zstd::bulk::Compressor::new(ZSTD_LEVEL)
                .context("create worker zstd compressor")?,
            split: None,
            lean: None,
            lean_compression: LeanCompressionArg::Adaptive,
            lean_zstd_level: LeanZstdLevelArg::One,
            lean_compressor: None,
        })
    }

    fn new_with_split(config: ProjectionConfig) -> Result<Self> {
        let mut worker = Self::new(config)?;
        worker.split = Some(SplitWorkerScratch::default());
        Ok(worker)
    }

    fn new_with_lean(
        config: ProjectionConfig,
        compression: LeanCompressionArg,
        zstd_level: LeanZstdLevelArg,
    ) -> Result<Self> {
        let mut worker = Self::new(config)?;
        worker.lean = Some(LeanWorkerScratch::default());
        worker.lean_compression = compression;
        worker.lean_zstd_level = zstd_level;
        if zstd_level != LeanZstdLevelArg::One {
            worker.lean_compressor = Some(
                zstd::bulk::Compressor::new(zstd_level.level())
                    .context("create worker lean zstd compressor")?,
            );
        }
        Ok(worker)
    }

    fn effects_requested(&self) -> bool {
        self.split.is_some() || self.lean.is_some()
    }

    fn record_pending_effects(
        &mut self,
        tx_index: u32,
        source_flags: u32,
        pending: PendingSplitMetadata<'_>,
    ) -> Result<()> {
        ensure!(
            !(self.split.is_some() && self.lean.is_some()),
            "source-split and lean effects cannot share a worker"
        );
        match pending {
            PendingSplitMetadata::Missing => {
                if let Some(split) = self.split.as_mut() {
                    split.record_missing_metadata(source_flags)?;
                }
                if let Some(lean) = self.lean.as_mut() {
                    lean.record_missing_metadata(source_flags)?;
                }
            }
            PendingSplitMetadata::Raw(bytes) => {
                if let Some(split) = self.split.as_mut() {
                    split.record_raw_metadata(source_flags, bytes)?;
                }
                if let Some(lean) = self.lean.as_mut() {
                    lean.record_raw_metadata(tx_index, source_flags, bytes)?;
                }
            }
            PendingSplitMetadata::Decoded(effects) => {
                if let Some(split) = self.split.as_mut() {
                    split.record_decoded_metadata(source_flags, &effects)?;
                }
                if let Some(lean) = self.lean.as_mut() {
                    lean.record_decoded_metadata(tx_index, source_flags, &effects)?;
                }
            }
        }
        Ok(())
    }

    fn reset_transaction(&mut self) {
        self.role_by_source_position.fill(0);
        self.top_program_indexes.clear();
        self.top_account_indexes.clear();
        self.unique_position_by_source.fill(NO_ACCOUNT_POSITION);
        self.accounts.clear();
        self.first_invalid_pubkey_id = None;
        self.source_position_count = 0;
        self.source_position_overflow = false;
        self.duplicate_merges = 0;
    }

    fn reserve_account_scratch(&mut self) -> Result<()> {
        self.accounts
            .try_reserve_exact(MAX_MESSAGE_ACCOUNTS)
            .context("reserve worker account scratch")
    }

    fn ingest_account(
        &mut self,
        source_position: usize,
        key: CompactPubkey,
        registry_entries: u32,
    ) -> Result<()> {
        if source_position >= MAX_MESSAGE_ACCOUNTS {
            self.source_position_overflow = true;
            return Ok(());
        }
        self.source_position_count = self.source_position_count.max(source_position + 1);
        if let CompactPubkey::Id(id) = key
            && (id == 0 || id > registry_entries)
            && self.first_invalid_pubkey_id.is_none()
        {
            self.first_invalid_pubkey_id = Some(id);
        }
        let roles = self.role_by_source_position[source_position];
        let unique_position =
            if let Some(position) = self.accounts.iter().position(|account| account.key == key) {
                self.accounts[position].roles |= roles;
                self.duplicate_merges = self
                    .duplicate_merges
                    .checked_add(1)
                    .context("duplicate account merge count overflow")?;
                position
            } else {
                let position = self.accounts.len();
                ensure!(
                    position < MAX_MESSAGE_ACCOUNTS,
                    "unique account count exceeds account cap"
                );
                self.accounts.push(AccountUse { key, roles });
                position
            };
        self.unique_position_by_source[source_position] =
            u16::try_from(unique_position).context("unique account position exceeds u16")?;
        Ok(())
    }

    fn apply_role(&mut self, source_position: usize, role: u8) -> Result<()> {
        ensure!(
            source_position < MAX_MESSAGE_ACCOUNTS,
            "account role source position exceeds account cap"
        );
        self.role_by_source_position[source_position] |= role;
        let unique_position = self.unique_position_by_source[source_position];
        if unique_position != NO_ACCOUNT_POSITION {
            self.accounts[usize::from(unique_position)].roles |= role;
        }
        Ok(())
    }

    fn retain_decoded_page_scratch(&mut self, mut page: Vec<u8>) {
        page.clear();
        self.decoded_page_scratch = if page.capacity() <= MAX_RETAINED_PAGE_SCRATCH_BYTES {
            page
        } else {
            Vec::new()
        };
    }
}

struct U8IndexSummary {
    distinct_order: [u8; MAX_MESSAGE_ACCOUNTS],
    seen: [u64; MAX_MESSAGE_ACCOUNTS / 64],
    len: usize,
    maximum: Option<u8>,
}

impl Default for U8IndexSummary {
    fn default() -> Self {
        Self {
            distinct_order: [0; MAX_MESSAGE_ACCOUNTS],
            seen: [0; MAX_MESSAGE_ACCOUNTS / 64],
            len: 0,
            maximum: None,
        }
    }
}

impl U8IndexSummary {
    fn clear(&mut self) {
        self.seen.fill(0);
        self.len = 0;
        self.maximum = None;
    }

    fn observe(&mut self, index: u8) {
        let position = usize::from(index);
        let word = position / 64;
        let bit = 1_u64 << (position % 64);
        if self.seen[word] & bit == 0 {
            self.seen[word] |= bit;
            self.distinct_order[self.len] = index;
            self.len += 1;
        }
        self.maximum = Some(self.maximum.map_or(index, |maximum| maximum.max(index)));
    }

    fn observe_all(&mut self, indexes: &[u8]) {
        for &index in indexes {
            self.observe(index);
        }
    }

    fn first_out_of_bounds(&self, bound: usize) -> Option<u8> {
        if self
            .maximum
            .is_none_or(|maximum| usize::from(maximum) < bound)
        {
            return None;
        }
        self.distinct_order[..self.len]
            .iter()
            .copied()
            .find(|index| usize::from(*index) >= bound)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AccountIndexRow {
    block_id: u32,
    slot: u64,
    page_offset: u64,
    stored_len: u32,
    decoded_len: u32,
    tx_count: u32,
    account_ref_count: u32,
    coverage_flags: u32,
    flags: u32,
}

/// Source wrapper that permits a signature size probe during admission but
/// rejects every signature-content read before it reaches the pinned file.
#[derive(Debug, Clone)]
struct NoSignatureContentSource {
    inner: PinnedLocalRangeSource,
    rejected_signature_content_reads: Arc<AtomicU64>,
    rejected_unrelated_content_reads: Arc<AtomicU64>,
    block_content_reads: Arc<Mutex<Vec<(u64, usize)>>>,
}

impl NoSignatureContentSource {
    fn new(inner: PinnedLocalRangeSource) -> Self {
        Self {
            inner,
            rejected_signature_content_reads: Arc::new(AtomicU64::new(0)),
            rejected_unrelated_content_reads: Arc::new(AtomicU64::new(0)),
            block_content_reads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn reset_block_content_reads(&self) -> Result<()> {
        self.block_content_reads
            .lock()
            .map_err(|_| anyhow::anyhow!("block-read trace lock is poisoned"))?
            .clear();
        Ok(())
    }

    fn validate_block_content_reads(
        &self,
        expected_start: u64,
        expected_end: u64,
        expected_bytes: u64,
    ) -> Result<u64> {
        let reads = self
            .block_content_reads
            .lock()
            .map_err(|_| anyhow::anyhow!("block-read trace lock is poisoned"))?;
        let mut next_offset = expected_start;
        let mut total = 0_u64;
        for &(offset, length) in reads.iter() {
            ensure!(
                offset == next_offset,
                "block payload reads are not gapless and monotonic: expected offset {next_offset}, got {offset}"
            );
            let length = u64::try_from(length).context("block read length exceeds u64")?;
            total = total
                .checked_add(length)
                .context("block read byte count overflow")?;
            next_offset = next_offset
                .checked_add(length)
                .context("block read end overflow")?;
        }
        ensure!(
            next_offset == expected_end,
            "block payload reads stop at {next_offset}, expected {expected_end}"
        );
        ensure!(
            total == expected_bytes,
            "block payload reads total {total} bytes, expected {expected_bytes}"
        );
        u64::try_from(reads.len()).context("block read call count exceeds u64")
    }

    fn rejected_signature_content_reads(&self) -> u64 {
        self.rejected_signature_content_reads
            .load(Ordering::Relaxed)
    }

    fn rejected_unrelated_content_reads(&self) -> u64 {
        self.rejected_unrelated_content_reads
            .load(Ordering::Relaxed)
    }
}

impl RangeSource for NoSignatureContentSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        self.inner.size(object)
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        let mut bytes = Vec::new();
        self.read_range_into(object, offset, length, &mut bytes)?;
        Ok(bytes)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        if object == ARCHIVE_V2_SIGNATURES_FILE {
            self.rejected_signature_content_reads
                .fetch_add(1, Ordering::Relaxed);
            return Err(SourceError::Protocol(
                "account projection forbids signature-content reads".to_owned(),
            ));
        }
        if !matches!(
            object,
            ARCHIVE_V2_BLOCKS_FILE
                | ARCHIVE_V2_BLOCK_INDEX_FILE
                | ARCHIVE_V2_META_FILE
                | ARCHIVE_V2_GENESIS_BIN_FILE
        ) {
            self.rejected_unrelated_content_reads
                .fetch_add(1, Ordering::Relaxed);
            return Err(SourceError::Protocol(format!(
                "account projection does not allow {object} content reads"
            )));
        }
        if object == ARCHIVE_V2_BLOCKS_FILE {
            self.block_content_reads
                .lock()
                .map_err(|_| SourceError::Protocol("block-read trace lock is poisoned".to_owned()))?
                .push((offset, length));
        }
        self.inner
            .read_range_into(object, offset, length, destination)
    }
}

#[derive(Debug, Serialize)]
struct TimingReport {
    total_wall_ms: u64,
    source_read_wall_ms: u64,
    decode_and_project_wall_ms: u64,
    worker_decode_sum_ms: u64,
    worker_project_sum_ms: u64,
    worker_message_account_stream_sum_ms: u64,
    worker_metadata_account_stream_sum_ms: u64,
    worker_account_finalize_sum_ms: u64,
    worker_page_encode_sum_ms: u64,
    worker_page_zstd_sum_ms: u64,
    ordered_write_sum_ms: u64,
    source_producer_wait_for_free_buffer_ms: u64,
    ordered_writer_wait_for_ready_batch_ms: u64,
    worker_busy_sum_ms: u64,
    decode_pool_utilization_percent: f64,
    finalize_ms: u64,
}

#[derive(Debug, Serialize)]
struct SplitPlaneBenchmarkReport {
    plane: &'static str,
    file: &'static str,
    frames: u64,
    records: u64,
    exact_source_field_bytes: u64,
    decoded_payload_bytes: u64,
    stored_frame_bytes: u64,
    file_bytes: u64,
}

#[derive(Debug, Serialize)]
struct SplitBenchmarkReport {
    canary_kind: &'static str,
    candidate_status: &'static str,
    format_status: &'static str,
    index_file: &'static str,
    index_bytes: u64,
    split_output_bytes: u64,
    account_projection_output_bytes: u64,
    combined_candidate_output_bytes: u64,
    metadata_reconstructable: bool,
    loaded_address_lanes_preserved: bool,
    raw_transaction_decoded_metadata_validation: &'static str,
    raw_transaction_structural_account_cap: usize,
    missing_metadata_transactions: u64,
    decoded_metadata_transactions: u64,
    raw_metadata_transactions: u64,
    raw_transaction_flags: u64,
    worker_metadata_account_and_effect_stream_sum_ms: u64,
    worker_split_compression_sum_ms: u64,
    split_copy_and_other_worker_residual_sum_ms: u64,
    ordered_split_write_sum_ms: u64,
    split_finalize_ms: u64,
    max_worker_live_raw_length_plus_compress_bound_bytes: usize,
    max_worker_raw_and_compression_scratch_capacity_bytes: usize,
    max_worker_retained_raw_and_compression_capacity_bytes: usize,
    retained_raw_and_compression_capacity_limit_bytes: usize,
    max_worker_retained_chunk_descriptor_capacity_bytes: usize,
    retained_chunk_descriptor_capacity_limit_bytes: usize,
    max_worker_total_scratch_capacity_bytes: usize,
    total_scratch_capacity_limit_bytes: usize,
    max_worker_retained_total_scratch_capacity_bytes: usize,
    retained_total_scratch_capacity_limit_bytes: usize,
    max_block_owned_packed_output_bytes: usize,
    owned_packed_output_limit_bytes: usize,
    planes: Vec<SplitPlaneBenchmarkReport>,
}

#[derive(Debug, Serialize)]
struct LeanObjectBenchmarkReport {
    object: &'static str,
    file: &'static str,
    blocks_with_bytes: u64,
    records: u64,
    exact_source_field_bytes: u64,
    decoded_bytes: u64,
    stored_payload_bytes: u64,
    file_bytes: u64,
    raw_blocks: u64,
    zstd_blocks: u64,
    compression_sum_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    declared_compression_policy: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compression_attempts: Option<u64>,
    stored_to_decoded_ratio: f64,
    compression_savings_percent: f64,
}

#[derive(Debug, Serialize)]
struct LeanBenchmarkReport {
    canary_kind: &'static str,
    candidate_status: &'static str,
    format_status: &'static str,
    compression_mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    compression_policy: Option<&'static str>,
    zstd_level: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    zstd_context_memory_accounting: Option<&'static str>,
    index_file: &'static str,
    index_row_bytes: usize,
    directory_row_bytes_per_transaction: usize,
    transaction_paging: &'static str,
    index_bytes: u64,
    lean_output_bytes: u64,
    account_projection_output_bytes: u64,
    combined_candidate_output_bytes: u64,
    exact_source_field_bytes: u64,
    decoded_payload_bytes: u64,
    stored_payload_bytes: u64,
    fixed_header_and_locator_overhead_bytes: u64,
    raw_block_chunks: u64,
    zstd_block_chunks: u64,
    stored_to_decoded_ratio: f64,
    compression_savings_percent: f64,
    missing_metadata_transactions: u64,
    decoded_metadata_transactions: u64,
    raw_metadata_transactions: u64,
    raw_transaction_flags: u64,
    nonempty_semantic_transaction_reward_transactions: u64,
    stored_transaction_reward_records: u64,
    worker_metadata_account_and_effect_stream_sum_ms: u64,
    worker_lean_compression_sum_ms: u64,
    lean_copy_and_other_worker_residual_sum_ms: u64,
    ordered_lean_write_sum_ms: u64,
    lean_finalize_ms: u64,
    max_worker_live_scratch_bytes: usize,
    scratch_capacity_limit_bytes: usize,
    max_worker_scratch_capacity_bytes: usize,
    max_worker_retained_scratch_capacity_bytes: usize,
    retained_scratch_capacity_limit_bytes: usize,
    max_block_owned_packed_output_bytes: usize,
    owned_packed_output_limit_bytes: usize,
    objects: Vec<LeanObjectBenchmarkReport>,
}

#[derive(Debug, Default, Clone, Copy)]
struct ProjectionTiming {
    message_traversal: Duration,
    metadata_traversal: Duration,
    account_role_assembly: Duration,
    page_encode: Duration,
    page_zstd: Duration,
}

impl ProjectionTiming {
    fn merge(&mut self, other: Self) {
        self.message_traversal = self
            .message_traversal
            .saturating_add(other.message_traversal);
        self.metadata_traversal = self
            .metadata_traversal
            .saturating_add(other.metadata_traversal);
        self.account_role_assembly = self
            .account_role_assembly
            .saturating_add(other.account_role_assembly);
        self.page_encode = self.page_encode.saturating_add(other.page_encode);
        self.page_zstd = self.page_zstd.saturating_add(other.page_zstd);
    }
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    status: &'static str,
    output_validation: &'static str,
    content_hashing: &'static str,
    account_semantics: &'static str,
    epoch: u64,
    slots_per_epoch: u64,
    message_schema: &'static str,
    metadata_schema: &'static str,
    workers: usize,
    benchmark_prefix_blocks: Option<usize>,
    source_total_blocks: usize,
    selected_blocks: usize,
    transactions: u64,
    account_refs: u64,
    id_refs: u64,
    raw_refs: u64,
    success_transactions: u64,
    failed_transactions: u64,
    unknown_transactions: u64,
    fully_covered_transactions: u64,
    incomplete_coverage_transactions: u64,
    account_complete_transactions: u64,
    account_missing_loaded_metadata_transactions: u64,
    account_raw_transaction_fallbacks: u64,
    account_raw_metadata_loaded_fallbacks: u64,
    cpi_recorded_transactions: u64,
    cpi_not_recorded_transactions: u64,
    cpi_missing_metadata_transactions: u64,
    cpi_raw_transaction_fallbacks: u64,
    cpi_raw_metadata_fallbacks: u64,
    source_raw_transaction_fallback_flags: u64,
    source_raw_metadata_fallback_flags: u64,
    max_resolved_source_positions_per_transaction: usize,
    max_unique_output_accounts_per_transaction: usize,
    duplicate_account_merges: u64,
    complete_coverage: bool,
    source_compressed_bytes: u64,
    source_decoded_bytes: u64,
    page_decoded_bytes: u64,
    page_stored_bytes: u64,
    index_bytes: u64,
    output_bytes: u64,
    transactions_per_second: f64,
    source_compressed_mib_per_second: f64,
    source_block_read_calls: u64,
    reader_batches: u64,
    reader_max_blocks_per_batch: usize,
    reader_max_compressed_batch_bytes: usize,
    reader_max_declared_uncompressed_batch_bytes: u64,
    reader_max_retained_decompressed_buffer_bytes: usize,
    signature_content_reads: u64,
    unrelated_source_content_reads: u64,
    source_unchanged: bool,
    timing: TimingReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_split: Option<SplitBenchmarkReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lean_block_chunks: Option<LeanBenchmarkReport>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(deny_unknown_fields)]
struct CandidateBenchmarkReport {
    status: String,
    output_validation: String,
    content_hashing: String,
    account_semantics: String,
    epoch: u64,
    slots_per_epoch: u64,
    message_schema: String,
    metadata_schema: String,
    workers: usize,
    benchmark_prefix_blocks: Option<usize>,
    source_total_blocks: usize,
    selected_blocks: usize,
    transactions: u64,
    account_refs: u64,
    id_refs: u64,
    raw_refs: u64,
    success_transactions: u64,
    failed_transactions: u64,
    unknown_transactions: u64,
    fully_covered_transactions: u64,
    incomplete_coverage_transactions: u64,
    account_complete_transactions: u64,
    account_missing_loaded_metadata_transactions: u64,
    account_raw_transaction_fallbacks: u64,
    account_raw_metadata_loaded_fallbacks: u64,
    cpi_recorded_transactions: u64,
    cpi_not_recorded_transactions: u64,
    cpi_missing_metadata_transactions: u64,
    cpi_raw_transaction_fallbacks: u64,
    cpi_raw_metadata_fallbacks: u64,
    source_raw_transaction_fallback_flags: u64,
    source_raw_metadata_fallback_flags: u64,
    max_resolved_source_positions_per_transaction: usize,
    max_unique_output_accounts_per_transaction: usize,
    duplicate_account_merges: u64,
    complete_coverage: bool,
    source_compressed_bytes: u64,
    source_decoded_bytes: u64,
    page_decoded_bytes: u64,
    page_stored_bytes: u64,
    index_bytes: u64,
    output_bytes: u64,
    transactions_per_second: f64,
    source_compressed_mib_per_second: f64,
    source_block_read_calls: u64,
    reader_batches: u64,
    reader_max_blocks_per_batch: usize,
    reader_max_compressed_batch_bytes: usize,
    reader_max_declared_uncompressed_batch_bytes: u64,
    reader_max_retained_decompressed_buffer_bytes: usize,
    signature_content_reads: u64,
    unrelated_source_content_reads: u64,
    source_unchanged: bool,
    timing: serde_json::Value,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PohSchemaArg {
    Current,
    CurrentAllZeroDerived,
    LegacyNoSignatureCount,
}

impl From<PohSchemaArg> for PohSidecarSchema {
    fn from(value: PohSchemaArg) -> Self {
        match value {
            PohSchemaArg::Current => Self::Current,
            PohSchemaArg::CurrentAllZeroDerived => Self::CurrentAllZeroDerived,
            PohSchemaArg::LegacyNoSignatureCount => Self::LegacyNoSignatureCount,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Verify an unverified Archive V2 account projection")]
struct VerifyArgs {
    /// Immutable Archive V2 generation directory.
    source: PathBuf,
    /// Existing unverified account-projection candidate directory.
    candidate: PathBuf,
    /// Exact predecessor Archive V2 generation, mandatory after epoch zero.
    #[arg(long)]
    predecessor_source: Option<PathBuf>,
    #[arg(long)]
    epoch: u64,
    #[arg(long)]
    slots_per_epoch: u64,
    #[arg(long, value_enum)]
    message_schema: MessageSchemaArg,
    #[arg(long, value_enum)]
    metadata_schema: MetadataSchemaArg,
    #[arg(long, value_enum)]
    poh_schema: PohSchemaArg,
    /// Trusted PoH hashes-per-tick protocol bound.
    #[arg(long)]
    poh_hashes_per_tick: u64,
    /// Trusted PoH ticks-per-slot protocol bound.
    #[arg(long)]
    poh_ticks_per_slot: u64,
    /// Operational guard applied before one block's PoH recomputation starts.
    #[arg(long)]
    poh_max_hash_rounds_per_block: u64,
    /// Operational guard applied to the selected verification run.
    #[arg(long)]
    poh_max_total_hash_rounds: u64,
    #[arg(long, default_value_t = 12)]
    workers: usize,
    /// Verify exactly this explicit candidate prefix. Omit for a full source.
    #[arg(long)]
    expected_prefix_blocks: Option<usize>,
}

#[allow(dead_code)] // Verifier-only shared module item.
#[derive(Debug, Serialize)]
struct AccountSemanticVerificationReport {
    blocks_verified: u64,
    transactions_verified: u64,
    candidate_page_bytes_read: u64,
    candidate_decoded_bytes_compared: u64,
    source_message_metadata_grammar: &'static str,
    source_fallback_rows: u64,
    reader_blocks: u64,
    reader_batches: u64,
    reader_compressed_bytes: u64,
    elapsed_millis: u64,
    stats: ProjectionStats,
}

#[allow(dead_code)] // Verifier-only shared module item.
#[derive(Debug, Serialize)]
struct VerificationReport {
    status: &'static str,
    verification_passed: bool,
    diagnostic_prefix_verified: bool,
    complete_source_verified: bool,
    issues_found: u64,
    publishable: bool,
    candidate_status: &'static str,
    source_epoch: u64,
    selected_blocks: usize,
    source_total_blocks: usize,
    explicit_prefix: Option<usize>,
    message_schema: &'static str,
    metadata_schema: &'static str,
    account_semantics: &'static str,
    deterministic_semantic_equality: &'static str,
    candidate_structure_and_eof: &'static str,
    source_identity_recheck: &'static str,
    candidate_identity_recheck: &'static str,
    poh: ArchiveIntegrityReport,
    account_projection: AccountSemanticVerificationReport,
    ed25519_signature_verification: &'static str,
    signature_bytes_hashed_for_poh: u64,
    output_content_hashing: &'static str,
    seal_written: bool,
    mutation: &'static str,
    total_elapsed_millis: u64,
}

#[allow(dead_code)] // Verifier-only shared module item.
struct SemanticVerifierWorker {
    page_file: File,
    stored: Vec<u8>,
    decoded: Vec<u8>,
    candidate_accounts: Vec<AccountUse>,
    reference_accounts: Vec<AccountUse>,
    reference_role_by_source: [u8; MAX_MESSAGE_ACCOUNTS],
    reference_unique_by_source: [u16; MAX_MESSAGE_ACCOUNTS],
    reference_first_invalid_id: Option<u32>,
    reference_source_positions: usize,
    reference_source_overflow: bool,
    reference_duplicate_merges: u64,
    reference_top_program_indexes: VerifierU8IndexSummary,
    reference_top_account_indexes: VerifierU8IndexSummary,
    reference_cpi_account_indexes: VerifierU8IndexSummary,
    decompressor: zstd::bulk::Decompressor<'static>,
}

#[allow(dead_code)] // Verifier-only shared module item.
impl SemanticVerifierWorker {
    fn new(page_file: File) -> Result<Self> {
        let mut candidate_accounts = Vec::new();
        candidate_accounts
            .try_reserve_exact(MAX_MESSAGE_ACCOUNTS)
            .context("reserve verifier candidate account scratch")?;
        let mut reference_accounts = Vec::new();
        reference_accounts
            .try_reserve_exact(MAX_MESSAGE_ACCOUNTS)
            .context("reserve verifier reference account scratch")?;
        Ok(Self {
            page_file,
            stored: Vec::new(),
            decoded: Vec::new(),
            candidate_accounts,
            reference_accounts,
            reference_role_by_source: [0; MAX_MESSAGE_ACCOUNTS],
            reference_unique_by_source: [NO_ACCOUNT_POSITION; MAX_MESSAGE_ACCOUNTS],
            reference_first_invalid_id: None,
            reference_source_positions: 0,
            reference_source_overflow: false,
            reference_duplicate_merges: 0,
            reference_top_program_indexes: VerifierU8IndexSummary::default(),
            reference_top_account_indexes: VerifierU8IndexSummary::default(),
            reference_cpi_account_indexes: VerifierU8IndexSummary::default(),
            decompressor: zstd::bulk::Decompressor::new()
                .context("create verifier zstd decompressor")?,
        })
    }

    fn reset_reference_transaction(&mut self) {
        self.reference_accounts.clear();
        self.reference_role_by_source.fill(0);
        self.reference_unique_by_source.fill(NO_ACCOUNT_POSITION);
        self.reference_first_invalid_id = None;
        self.reference_source_positions = 0;
        self.reference_source_overflow = false;
        self.reference_duplicate_merges = 0;
        self.reference_top_program_indexes.clear();
        self.reference_top_account_indexes.clear();
        self.reference_cpi_account_indexes.clear();
    }

    fn ingest_reference_account(
        &mut self,
        source_position: usize,
        key: CompactPubkey,
        registry_entries: u32,
    ) -> Result<()> {
        if source_position >= MAX_MESSAGE_ACCOUNTS {
            self.reference_source_overflow = true;
            return Ok(());
        }
        self.reference_source_positions = self.reference_source_positions.max(source_position + 1);
        if let CompactPubkey::Id(id) = key
            && (id == 0 || id > registry_entries)
            && self.reference_first_invalid_id.is_none()
        {
            self.reference_first_invalid_id = Some(id);
        }
        let unique_position = if let Some(position) = self
            .reference_accounts
            .iter()
            .position(|account| account.key == key)
        {
            self.reference_accounts[position].roles |=
                self.reference_role_by_source[source_position];
            self.reference_duplicate_merges = self
                .reference_duplicate_merges
                .checked_add(1)
                .context("reference duplicate merge count overflow")?;
            position
        } else {
            let position = self.reference_accounts.len();
            ensure!(
                position < MAX_MESSAGE_ACCOUNTS,
                "reference unique account count exceeds cap"
            );
            self.reference_accounts.push(AccountUse {
                key,
                roles: self.reference_role_by_source[source_position],
            });
            position
        };
        self.reference_unique_by_source[source_position] =
            u16::try_from(unique_position).context("reference unique position exceeds u16")?;
        Ok(())
    }

    fn apply_reference_role(&mut self, source_position: usize, role: u8) -> Result<()> {
        ensure!(
            source_position < MAX_MESSAGE_ACCOUNTS,
            "reference role source position exceeds account cap"
        );
        self.reference_role_by_source[source_position] |= role;
        let unique_position = self.reference_unique_by_source[source_position];
        if unique_position != NO_ACCOUNT_POSITION {
            self.reference_accounts[usize::from(unique_position)].roles |= role;
        }
        Ok(())
    }
}

#[allow(dead_code)] // Verifier-only shared module item.
struct VerifierU8IndexSummary {
    distinct_order: [u8; MAX_MESSAGE_ACCOUNTS],
    seen: [u64; MAX_MESSAGE_ACCOUNTS / 64],
    len: usize,
    maximum: Option<u8>,
}

#[allow(dead_code)] // Verifier-only shared module item.
impl Default for VerifierU8IndexSummary {
    fn default() -> Self {
        Self {
            distinct_order: [0; MAX_MESSAGE_ACCOUNTS],
            seen: [0; MAX_MESSAGE_ACCOUNTS / 64],
            len: 0,
            maximum: None,
        }
    }
}

#[allow(dead_code)] // Verifier-only shared module item.
impl VerifierU8IndexSummary {
    fn clear(&mut self) {
        self.seen.fill(0);
        self.len = 0;
        self.maximum = None;
    }

    fn observe(&mut self, index: u8) {
        let position = usize::from(index);
        let word = position / 64;
        let bit = 1_u64 << (position % 64);
        if self.seen[word] & bit == 0 {
            self.seen[word] |= bit;
            self.distinct_order[self.len] = index;
            self.len += 1;
        }
        self.maximum = Some(self.maximum.map_or(index, |maximum| maximum.max(index)));
    }

    fn observe_all(&mut self, indexes: &[u8]) {
        for &index in indexes {
            self.observe(index);
        }
    }

    fn first_out_of_bounds(&self, bound: usize) -> Option<u8> {
        if self
            .maximum
            .is_none_or(|maximum| usize::from(maximum) < bound)
        {
            return None;
        }
        self.distinct_order[..self.len]
            .iter()
            .copied()
            .find(|index| usize::from(*index) >= bound)
    }
}

#[allow(dead_code)] // Verifier-only shared module item.
#[derive(Debug)]
struct VerifiedSemanticBlock {
    stats: ProjectionStats,
    stored_bytes: u64,
    decoded_bytes: u64,
}

fn main() -> Result<()> {
    run(Args::parse())
}

#[allow(dead_code)] // Entry point for the separate verifier binary.
pub(crate) fn verify_main() -> Result<()> {
    verify_run(VerifyArgs::parse())
}

#[allow(dead_code)] // Entry point for the separate verifier binary.
fn verify_run(args: VerifyArgs) -> Result<()> {
    let total_started = Instant::now();
    ensure!(args.source.is_dir(), "source must be a directory");
    ensure!(args.candidate.is_dir(), "candidate must be a directory");
    ensure!(
        args.slots_per_epoch > 0,
        "--slots-per-epoch must be positive"
    );
    ensure!(
        args.poh_max_hash_rounds_per_block > 0 && args.poh_max_total_hash_rounds > 0,
        "PoH hash-round limits must be positive"
    );
    ensure!(
        (1..=64).contains(&args.workers),
        "--workers must be in 1..=64"
    );
    ensure!(
        args.expected_prefix_blocks != Some(0),
        "--expected-prefix-blocks must be positive"
    );
    ensure!(
        (args.epoch == 0) == args.predecessor_source.is_none(),
        "epoch zero must not have --predecessor-source; later epochs require it"
    );

    let source_canonical = fs::canonicalize(&args.source)
        .with_context(|| format!("canonicalize source {}", args.source.display()))?;
    let candidate_canonical = fs::canonicalize(&args.candidate)
        .with_context(|| format!("canonicalize candidate {}", args.candidate.display()))?;
    ensure!(
        !candidate_canonical.starts_with(&source_canonical),
        "candidate must be outside the immutable source directory"
    );

    let predecessor_canonical = args
        .predecessor_source
        .as_ref()
        .map(|path| {
            fs::canonicalize(path)
                .with_context(|| format!("canonicalize predecessor {}", path.display()))
        })
        .transpose()?;

    let message_schema: CompactV2MessageSchema = args.message_schema.into();
    let metadata_schema: CompactV2MetadataSchema = args.metadata_schema.into();
    let mut source_required_objects = vec![
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ARCHIVE_V2_POH_FILE,
    ];
    source_required_objects.push(if args.epoch == 0 {
        ARCHIVE_V2_GENESIS_BIN_FILE
    } else {
        ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE
    });
    let source = PinnedLocalRangeSource::new_anchored(&source_canonical, &source_required_objects)?;
    let source_path_snapshot = capture_anchored_directory_snapshot(&source, "source")?;
    bind_pinned_objects_to_snapshot(
        &source,
        &source_path_snapshot,
        "source",
        &source_required_objects,
        &[],
    )?;
    let archive = open_verifier_archive(
        source.clone(),
        args.epoch,
        args.slots_per_epoch,
        "account-projection-verifier-source",
    )?;
    let predecessor_and_snapshot = predecessor_canonical
        .as_ref()
        .map(|canonical| -> Result<_> {
            let required = [
                ARCHIVE_V2_BLOCKS_FILE,
                ARCHIVE_V2_BLOCK_INDEX_FILE,
                ARCHIVE_V2_META_FILE,
                ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
                ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            ];
            let optional = if args.epoch == 1 {
                [ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_GENESIS_BIN_FILE].as_slice()
            } else {
                [ARCHIVE_V2_SIGNATURES_FILE].as_slice()
            };
            let mut allowlist = required.to_vec();
            allowlist.extend_from_slice(optional);
            let pinned = PinnedLocalRangeSource::new_anchored(canonical, &allowlist)?;
            let snapshot = capture_anchored_directory_snapshot(&pinned, "predecessor source")?;
            bind_pinned_objects_to_snapshot(
                &pinned,
                &snapshot,
                "predecessor source",
                &required,
                optional,
            )?;
            let reader = open_verifier_archive(
                pinned,
                args.epoch - 1,
                args.slots_per_epoch,
                "account-projection-verifier-predecessor",
            )?;
            Ok((reader, snapshot))
        })
        .transpose()?;
    let (predecessor, predecessor_path_snapshot) = match predecessor_and_snapshot {
        Some((reader, snapshot)) => (Some(reader), Some(snapshot)),
        None => (None, None),
    };

    let candidate_allowlist = [INDEX_FILE, PAGES_FILE, REPORT_FILE];
    let candidate =
        PinnedLocalRangeSource::new_anchored(&candidate_canonical, &candidate_allowlist)?;
    let candidate_path_snapshot = capture_anchored_directory_snapshot(&candidate, "candidate")?;
    validate_candidate_file_set(&candidate_path_snapshot)?;
    bind_pinned_objects_to_snapshot(
        &candidate,
        &candidate_path_snapshot,
        "candidate",
        &candidate_allowlist,
        &[],
    )?;
    let candidate_report_bytes = candidate
        .read_all_bounded(REPORT_FILE, 1 << 20)
        .context("read bounded candidate report")?;
    let candidate_report: CandidateBenchmarkReport =
        serde_json::from_slice(&candidate_report_bytes).context("decode candidate report")?;
    let source_total_blocks = archive.index().rows.len();
    ensure!(source_total_blocks > 0, "source block index is empty");
    let selected_blocks = args.expected_prefix_blocks.unwrap_or(source_total_blocks);
    ensure!(
        selected_blocks <= source_total_blocks,
        "--expected-prefix-blocks {selected_blocks} exceeds {source_total_blocks} source blocks"
    );
    match args.expected_prefix_blocks {
        Some(expected) => ensure!(
            candidate_report.benchmark_prefix_blocks == Some(expected),
            "candidate report prefix {:?} does not match explicit expected prefix {expected}",
            candidate_report.benchmark_prefix_blocks
        ),
        None => ensure!(
            candidate_report.benchmark_prefix_blocks.is_none()
                && candidate_report.selected_blocks == source_total_blocks,
            "full verification requires a full candidate with benchmark_prefix_blocks=null"
        ),
    }

    let pages_size = candidate
        .size(PAGES_FILE)?
        .context("candidate pages file is missing")?;
    let expected_index_size = u64::try_from(INDEX_HEADER_LEN)
        .expect("fixed header fits u64")
        .checked_add(
            u64::try_from(selected_blocks)
                .context("selected block count exceeds u64")?
                .checked_mul(u64::try_from(INDEX_ROW_LEN).expect("fixed row fits u64"))
                .context("candidate index size overflow")?,
        )
        .context("candidate index size overflow")?;
    let index_size = candidate
        .size(INDEX_FILE)?
        .context("candidate index file is missing")?;
    ensure!(
        index_size == expected_index_size,
        "candidate index has {index_size} bytes, expected {expected_index_size}"
    );
    let index_bytes = candidate
        .read_all_bounded(
            INDEX_FILE,
            usize::try_from(expected_index_size).context("candidate index exceeds usize")?,
        )
        .context("read bounded candidate index")?;
    let index_rows = decode_index_exact(
        &index_bytes,
        pages_size,
        archive.registry_entries(),
        message_schema,
        metadata_schema,
    )?;
    ensure!(
        index_rows.len() == selected_blocks,
        "candidate index has {} rows, expected {selected_blocks}",
        index_rows.len()
    );
    for (position, (candidate_row, source_row)) in index_rows
        .iter()
        .zip(&archive.index().rows[..selected_blocks])
        .enumerate()
    {
        ensure!(
            candidate_row.block_id == source_row.block_id
                && candidate_row.slot == source_row.slot
                && candidate_row.tx_count == source_row.tx_count,
            "candidate index row {position} identity or transaction count differs from source"
        );
        let (minimum, maximum) = page_decoded_length_bounds(source_row.tx_count)?;
        ensure!(
            u64::from(candidate_row.decoded_len) >= minimum
                && u64::from(candidate_row.decoded_len) <= maximum
                && u64::from(candidate_row.decoded_len) <= MAX_VERIFIER_DECODED_PAGE_BYTES,
            "candidate page {position} decoded length is outside bounded geometry"
        );
        if candidate_row.flags & INDEX_ROW_FLAG_ZSTD != 0 {
            ensure!(
                candidate_row.stored_len < candidate_row.decoded_len,
                "candidate compressed page {position} is not smaller than its decoded page"
            );
        } else {
            ensure!(
                candidate_row.stored_len == candidate_row.decoded_len,
                "candidate raw page {position} stored and decoded lengths differ"
            );
        }
    }

    let account_started = Instant::now();
    let projection_config = ProjectionConfig {
        message_schema,
        metadata_schema,
        registry_entries: archive.registry_entries(),
    };
    let candidate_pages = candidate
        .pinned_file_clone(PAGES_FILE)?
        .context("candidate pages file is missing")?;
    let mut account_stats = ProjectionStats::default();
    let mut candidate_page_bytes_read = 0u64;
    let mut candidate_decoded_bytes_compared = 0u64;
    let reader_stats = archive.process_borrowed_blocks_parallel_ordered(
        Range {
            start: 0,
            end: selected_blocks,
        },
        OrderedParallelBlockConfig {
            decode_workers: args.workers,
            discard_rewards: true,
            max_blocks_per_batch: 1_024,
            ..OrderedParallelBlockConfig::default()
        },
        |_| {
            let file = candidate_pages
                .try_clone()
                .context("clone pinned candidate pages file")?;
            SemanticVerifierWorker::new(file)
        },
        |worker, sequence, block| {
            verify_semantic_block(worker, block, index_rows[sequence], projection_config)
        },
        |_sequence, verified| {
            account_stats.merge(verified.stats);
            candidate_page_bytes_read = candidate_page_bytes_read
                .checked_add(verified.stored_bytes)
                .context("candidate page byte total overflow")?;
            candidate_decoded_bytes_compared = candidate_decoded_bytes_compared
                .checked_add(verified.decoded_bytes)
                .context("candidate decoded byte total overflow")?;
            Ok::<_, anyhow::Error>(())
        },
    )?;
    ensure!(
        reader_stats.block_count == selected_blocks as u64,
        "account verifier reader completed {} blocks, expected {selected_blocks}",
        reader_stats.block_count
    );
    ensure!(
        candidate_page_bytes_read == pages_size,
        "account verifier read {candidate_page_bytes_read} candidate page bytes, expected {pages_size}"
    );
    let selected_rows = &archive.index().rows[..selected_blocks];
    let source_compressed_bytes = checked_row_byte_sum(selected_rows, |row| row.compressed_len)?;
    let source_decoded_bytes = checked_row_byte_sum(selected_rows, |row| row.uncompressed_len)?;
    let expected_transactions = selected_rows.iter().try_fold(0u64, |total, row| {
        total
            .checked_add(u64::from(row.tx_count))
            .context("source transaction count overflow")
    })?;
    ensure!(
        account_stats.transactions == expected_transactions,
        "semantic verifier completed {} transactions, expected {expected_transactions}",
        account_stats.transactions
    );
    compare_candidate_report(
        &candidate_report,
        &args,
        source_total_blocks,
        selected_blocks,
        source_compressed_bytes,
        source_decoded_bytes,
        candidate_decoded_bytes_compared,
        pages_size,
        index_size,
        &account_stats,
    )?;
    source.verify_unchanged()?;
    candidate.verify_unchanged()?;
    let account_elapsed = account_started.elapsed();

    let poh = verify_archive_v2_integrity(
        &archive,
        predecessor.as_ref(),
        ArchiveIntegrityConfig {
            epoch: args.epoch,
            slots_per_epoch: args.slots_per_epoch,
            selected_blocks,
            workers: args.workers,
            poh: PohProtocolBounds {
                ticks_per_slot: args.poh_ticks_per_slot,
                hashes_per_tick: args.poh_hashes_per_tick,
            },
            poh_schema: args.poh_schema.into(),
            max_hash_rounds_per_block: args.poh_max_hash_rounds_per_block,
            max_total_hash_rounds: args.poh_max_total_hash_rounds,
        },
    )?;
    source.verify_unchanged()?;
    candidate.verify_unchanged()?;
    if let Some(predecessor) = &predecessor {
        predecessor.source().verify_unchanged()?;
    }
    bind_pinned_objects_to_snapshot(
        &source,
        &source_path_snapshot,
        "source",
        &source_required_objects,
        &[],
    )?;
    bind_pinned_objects_to_snapshot(
        &candidate,
        &candidate_path_snapshot,
        "candidate",
        &candidate_allowlist,
        &[],
    )?;
    verify_anchored_directory_unchanged(
        &source,
        &source_canonical,
        &source_path_snapshot,
        "source",
    )?;
    verify_anchored_directory_unchanged(
        &candidate,
        &candidate_canonical,
        &candidate_path_snapshot,
        "candidate",
    )?;
    if let (Some(canonical), Some(snapshot), Some(predecessor)) = (
        predecessor_canonical.as_deref(),
        predecessor_path_snapshot.as_ref(),
        predecessor.as_ref(),
    ) {
        let required = [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_META_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ];
        let optional = if args.epoch == 1 {
            [ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_GENESIS_BIN_FILE].as_slice()
        } else {
            [ARCHIVE_V2_SIGNATURES_FILE].as_slice()
        };
        bind_pinned_objects_to_snapshot(
            predecessor.source(),
            snapshot,
            "predecessor source",
            &required,
            optional,
        )?;
        verify_anchored_directory_unchanged(
            predecessor.source(),
            canonical,
            snapshot,
            "predecessor source",
        )?;
    }

    let source_fallback_rows = account_stats.source_fallback_rows;
    let source_grammar = if source_fallback_rows == 0 {
        "complete"
    } else {
        "decoded-rows-complete; raw-fallback-rows-explicit"
    };
    let full_source = args.expected_prefix_blocks.is_none();
    let report = VerificationReport {
        status: if full_source {
            "verified-read-only-unsealed"
        } else {
            "verified-prefix-diagnostic-read-only-unsealed"
        },
        verification_passed: full_source,
        diagnostic_prefix_verified: !full_source,
        complete_source_verified: full_source,
        issues_found: 0,
        publishable: false,
        candidate_status: STATUS,
        source_epoch: args.epoch,
        selected_blocks,
        source_total_blocks,
        explicit_prefix: args.expected_prefix_blocks,
        message_schema: message_schema_name(message_schema),
        metadata_schema: metadata_schema_name(metadata_schema),
        account_semantics: ACCOUNT_SEMANTICS,
        deterministic_semantic_equality: "complete-for-selected-rows",
        candidate_structure_and_eof: "complete",
        source_identity_recheck: "unchanged",
        candidate_identity_recheck: "unchanged",
        signature_bytes_hashed_for_poh: poh.signature_bytes_hashed_for_poh,
        poh,
        account_projection: AccountSemanticVerificationReport {
            blocks_verified: selected_blocks as u64,
            transactions_verified: account_stats.transactions,
            candidate_page_bytes_read,
            candidate_decoded_bytes_compared,
            source_message_metadata_grammar: source_grammar,
            source_fallback_rows,
            reader_blocks: reader_stats.block_count,
            reader_batches: reader_stats.batch_count,
            reader_compressed_bytes: reader_stats.compressed_bytes,
            elapsed_millis: duration_millis(account_elapsed),
            stats: account_stats,
        },
        ed25519_signature_verification: "off",
        output_content_hashing: "none",
        seal_written: false,
        mutation: "none",
        total_elapsed_millis: duration_millis(total_started.elapsed()),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).context("encode verifier report")?
    );
    Ok(())
}

#[allow(dead_code)] // Verifier-only shared module item.
fn open_verifier_archive(
    source: PinnedLocalRangeSource,
    epoch: u64,
    slots_per_epoch: u64,
    generation_prefix: &str,
) -> Result<ArchiveReader<PinnedLocalRangeSource>> {
    ArchiveReader::open_trusted(
        source,
        TrustedGenerationIdentity {
            cluster_id: "mainnet-beta".to_owned(),
            epoch,
            generation_id: format!("{generation_prefix}-{epoch}"),
            slots_per_epoch,
        },
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
    .with_context(|| format!("admit trusted local Archive V2 epoch {epoch}"))
}

#[allow(dead_code)] // Verifier-only shared module item.
fn validate_candidate_file_set(candidate: &DirectoryPathSnapshot) -> Result<()> {
    let expected = BTreeSet::from([
        INDEX_FILE.to_owned(),
        PAGES_FILE.to_owned(),
        REPORT_FILE.to_owned(),
    ]);
    let actual = candidate.entries.keys().cloned().collect::<BTreeSet<_>>();
    ensure!(
        actual == expected,
        "candidate must contain exactly {expected:?}, found {actual:?}"
    );
    Ok(())
}

#[allow(dead_code)] // Verifier-only shared module item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PathIdentity {
    device: u64,
    inode: u64,
    mode: u32,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[allow(dead_code)] // Verifier-only shared module item.
impl PathIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            mode: metadata.mode(),
            size: metadata.size(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }
}

#[allow(dead_code)] // Verifier-only shared module item.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectoryPathSnapshot {
    directory: PathIdentity,
    entries: BTreeMap<String, PathIdentity>,
}

#[allow(dead_code)] // Verifier-only shared module item.
fn capture_anchored_directory_snapshot(
    source: &PinnedLocalRangeSource,
    label: &str,
) -> Result<DirectoryPathSnapshot> {
    capture_anchored_directory_snapshot_with_hook(source, label, || Ok(()))
}

#[allow(dead_code)] // The hook is used by the deterministic path-swap regression.
fn capture_anchored_directory_snapshot_with_hook(
    source: &PinnedLocalRangeSource,
    label: &str,
    after_root_fstat: impl FnOnce() -> Result<()>,
) -> Result<DirectoryPathSnapshot> {
    let root = source
        .pinned_root_file_clone()?
        .with_context(|| format!("{label} source is not anchored to a directory descriptor"))?;
    let directory_metadata = root
        .metadata()
        .with_context(|| format!("inspect anchored {label} directory"))?;
    ensure!(
        directory_metadata.file_type().is_dir(),
        "anchored {label} source is not a directory"
    );
    let directory_identity = PathIdentity::from_metadata(&directory_metadata);
    after_root_fstat()?;
    let mut stream = Dir::read_from(&root)
        .map_err(std::io::Error::from)
        .with_context(|| format!("enumerate anchored {label} directory"))?;
    let mut entries = BTreeMap::new();
    for entry in &mut stream {
        let entry = entry
            .map_err(std::io::Error::from)
            .with_context(|| format!("enumerate anchored {label} directory"))?;
        let name_bytes = entry.file_name().to_bytes();
        if matches!(name_bytes, b"." | b"..") {
            continue;
        }
        ensure!(
            entries.len() < MAX_VERIFIER_DIRECTORY_ENTRIES,
            "anchored {label} directory exceeds the {MAX_VERIFIER_DIRECTORY_ENTRIES}-entry limit"
        );
        let name = std::str::from_utf8(name_bytes)
            .with_context(|| format!("anchored {label} directory has a non-UTF-8 entry"))?
            .to_owned();
        let file = rustix::fs::openat(
            &root,
            name.as_str(),
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map(File::from)
        .map_err(std::io::Error::from)
        .with_context(|| format!("open anchored {label} entry {name}"))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("inspect anchored {label} entry {name}"))?;
        ensure!(
            metadata.is_file(),
            "anchored {label} entry {name} is not a regular file"
        );
        ensure!(
            entries
                .insert(name.clone(), PathIdentity::from_metadata(&metadata))
                .is_none(),
            "anchored {label} directory contains duplicate entry {name}"
        );
    }
    let directory_after = root
        .metadata()
        .with_context(|| format!("reinspect anchored {label} directory"))?;
    ensure!(
        PathIdentity::from_metadata(&directory_after) == directory_identity,
        "anchored {label} directory changed while it was enumerated"
    );
    Ok(DirectoryPathSnapshot {
        directory: directory_identity,
        entries,
    })
}

/// Pin the exact admitted object set and bind each descriptor (or explicit
/// absence) to the one directory-FD-relative snapshot.
#[allow(dead_code)] // Verifier-only shared module item.
fn bind_pinned_objects_to_snapshot(
    source: &PinnedLocalRangeSource,
    snapshot: &DirectoryPathSnapshot,
    label: &str,
    required: &[&str],
    optional: &[&str],
) -> Result<()> {
    ensure!(
        required.len().saturating_add(optional.len()) <= 64,
        "{label} pinned object allowlist is too large"
    );
    let mut observed = BTreeSet::new();
    for object in required {
        ensure!(
            observed.insert(*object),
            "{label} pinned object allowlist contains duplicate {object}"
        );
        let expected = snapshot.entries.get(*object).with_context(|| {
            format!("{label} required object {object} was absent from the anchored snapshot")
        })?;
        let file = source
            .pinned_file_clone(object)?
            .with_context(|| format!("pinned {label} object {object} disappeared"))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("inspect pinned {label} object {object}"))?;
        ensure!(
            PathIdentity::from_metadata(&metadata) == *expected,
            "pinned {label} object {object} identity differs from the initial path snapshot"
        );
    }

    for object in optional {
        ensure!(
            observed.insert(*object),
            "{label} pinned object allowlist contains duplicate {object}"
        );
        let pinned = source.pinned_file_clone(object)?;
        match (snapshot.entries.get(*object), pinned) {
            (None, None) => {}
            (Some(expected), Some(file)) => {
                let metadata = file
                    .metadata()
                    .with_context(|| format!("inspect pinned {label} object {object}"))?;
                ensure!(
                    PathIdentity::from_metadata(&metadata) == *expected,
                    "pinned {label} optional object {object} identity differs from the initial path snapshot"
                );
            }
            (None, Some(_)) => {
                bail!("pinned {label} optional object {object} appeared after the initial snapshot")
            }
            (Some(_), None) => {
                bail!("pinned {label} optional object {object} disappeared")
            }
        }
    }
    source.verify_unchanged()?;
    Ok(())
}

#[allow(dead_code)] // Verifier-only shared module item.
fn verify_anchored_directory_unchanged(
    source: &PinnedLocalRangeSource,
    canonical_directory: &Path,
    original: &DirectoryPathSnapshot,
    label: &str,
) -> Result<()> {
    source.verify_unchanged()?;
    let current = capture_anchored_directory_snapshot(source, label)?;
    ensure!(
        &current == original,
        "anchored {label} directory entries or identities changed during verification"
    );
    let recanonicalized = fs::canonicalize(canonical_directory)
        .with_context(|| format!("recanonicalize {label} {}", canonical_directory.display()))?;
    ensure!(
        recanonicalized == canonical_directory,
        "{label} directory path now resolves to {} instead of {}",
        recanonicalized.display(),
        canonical_directory.display()
    );
    let rebound = rustix::fs::open(
        canonical_directory,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(std::io::Error::from)
    .with_context(|| format!("reopen final {label} directory path"))?;
    let rebound_metadata = rebound
        .metadata()
        .with_context(|| format!("inspect final {label} directory path"))?;
    ensure!(
        PathIdentity::from_metadata(&rebound_metadata) == original.directory,
        "final {label} directory path does not refer to the admitted directory descriptor"
    );
    Ok(())
}

#[allow(dead_code)] // Verifier-only shared module item.
fn verify_semantic_block(
    worker: &mut SemanticVerifierWorker,
    block: BorrowedDecodedBlock<'_>,
    candidate_row: AccountIndexRow,
    config: ProjectionConfig,
) -> Result<VerifiedSemanticBlock> {
    ensure!(
        !block.uses_owned_fallback(),
        "account verifier requires borrowed Archive V2 block lanes"
    );
    let source_row = block.index_row;
    ensure!(
        candidate_row.block_id == source_row.block_id
            && candidate_row.slot == source_row.slot
            && candidate_row.tx_count == source_row.tx_count,
        "candidate row identity differs from source block {}",
        source_row.block_id
    );
    let stored_len = candidate_row.stored_len as usize;
    resize_for_exact_read(&mut worker.stored, stored_len)?;
    read_file_exact_at(
        &worker.page_file,
        &mut worker.stored,
        candidate_row.page_offset,
        "candidate account pages",
    )?;
    let (candidate_page, raw_page) = if candidate_row.flags & INDEX_ROW_FLAG_ZSTD != 0 {
        let frame_len =
            zstd::zstd_safe::find_frame_compressed_size(&worker.stored).map_err(|code| {
                anyhow::anyhow!(
                    "candidate page {} has invalid zstd: {}",
                    source_row.block_id,
                    zstd::zstd_safe::get_error_name(code)
                )
            })?;
        ensure!(
            frame_len == worker.stored.len(),
            "candidate page {} zstd frame has trailing bytes",
            source_row.block_id
        );
        let mut decoded = std::mem::take(&mut worker.decoded);
        decoded.clear();
        if decoded.capacity() < candidate_row.decoded_len as usize {
            decoded
                .try_reserve_exact(candidate_row.decoded_len as usize)
                .context("reserve candidate decoded page")?;
        }
        let decoded_len = worker
            .decompressor
            .decompress_to_buffer(&worker.stored, &mut decoded)
            .with_context(|| format!("decompress candidate page {}", source_row.block_id))?;
        ensure!(
            decoded_len == candidate_row.decoded_len as usize
                && decoded.len() == candidate_row.decoded_len as usize,
            "candidate page {} zstd output length differs from index",
            source_row.block_id
        );
        (decoded, false)
    } else {
        ensure!(
            candidate_row.stored_len == candidate_row.decoded_len,
            "candidate raw page {} stored and decoded lengths differ",
            source_row.block_id
        );
        (std::mem::take(&mut worker.stored), true)
    };
    ensure!(
        candidate_page.len() == candidate_row.decoded_len as usize,
        "candidate page {} decoded byte count differs from index",
        source_row.block_id
    );

    let mut cursor = candidate_page.as_slice();
    ensure!(
        take(&mut cursor, 8)? == PAGE_MAGIC,
        "bad candidate page magic"
    );
    ensure!(
        read_u16(&mut cursor)? == FORMAT_VERSION,
        "bad candidate page version"
    );
    ensure!(
        read_u16(&mut cursor)? == 0,
        "non-zero candidate page reserved bytes"
    );
    ensure!(
        read_u32(&mut cursor)? == source_row.block_id,
        "candidate page block id differs from source"
    );
    ensure!(
        read_u32(&mut cursor)? == source_row.tx_count,
        "candidate page transaction count differs from source"
    );
    let declared_coverage = read_u32(&mut cursor)?;
    ensure!(
        declared_coverage & !COVERAGE_MASK == 0,
        "candidate page has unknown coverage bits"
    );

    let mut stats = ProjectionStats::default();
    let mut expected_coverage = 0u32;
    let mut transactions = 0usize;
    for row in block.tx_rows() {
        let (mut expected, resolved_positions, duplicate_merges) = reference_project_transaction(
            worker,
            config,
            source_row.slot,
            row,
            block.message_bytes(),
            block.metadata_bytes(),
        )?;
        let candidate_accounts = std::mem::take(&mut worker.candidate_accounts);
        let mut candidate = parse_candidate_transaction_exact(
            &mut cursor,
            config.registry_entries,
            candidate_accounts,
        )?;
        ensure!(
            candidate == expected,
            "candidate page {} transaction {} differs from independent source projection",
            source_row.block_id,
            row.tx_index
        );
        add_reference_transaction_stats(&mut stats, &expected)?;
        stats.max_resolved_source_positions =
            stats.max_resolved_source_positions.max(resolved_positions);
        stats.max_unique_output_accounts = stats
            .max_unique_output_accounts
            .max(expected.accounts.len());
        stats.duplicate_account_merges = stats
            .duplicate_account_merges
            .checked_add(duplicate_merges)
            .context("verifier duplicate merge count overflow")?;
        expected_coverage |= reference_transaction_coverage_bits(&expected);
        expected.accounts.clear();
        worker.reference_accounts = expected.accounts;
        candidate.accounts.clear();
        worker.candidate_accounts = candidate.accounts;
        transactions = transactions
            .checked_add(1)
            .context("verifier transaction count overflow")?;
    }
    ensure!(
        transactions == source_row.tx_count as usize,
        "source block {} yielded {transactions} rows, expected {}",
        source_row.block_id,
        source_row.tx_count
    );
    ensure!(cursor.is_empty(), "candidate page has trailing bytes");
    ensure!(
        declared_coverage == expected_coverage
            && declared_coverage == candidate_row.coverage_flags
            && reference_block_coverage_flags(&stats) == candidate_row.coverage_flags,
        "candidate page {} coverage flags differ from source or index",
        source_row.block_id
    );
    ensure!(
        stats.account_refs == u64::from(candidate_row.account_ref_count),
        "candidate page {} account reference count differs from independent projection",
        source_row.block_id
    );
    if raw_page {
        worker.stored = candidate_page;
    } else {
        worker.decoded = candidate_page;
    }
    Ok(VerifiedSemanticBlock {
        stats,
        stored_bytes: u64::from(candidate_row.stored_len),
        decoded_bytes: u64::from(candidate_row.decoded_len),
    })
}

#[allow(dead_code)] // Verifier-only shared module item.
fn parse_candidate_transaction_exact(
    cursor: &mut &[u8],
    registry_entries: u32,
    mut accounts: Vec<AccountUse>,
) -> Result<ProjectedTransaction> {
    let outcome = match read_u8(cursor)? {
        0 => Outcome::Unknown,
        1 => Outcome::Success,
        2 => Outcome::Failed,
        other => bail!("unknown candidate outcome {other}"),
    };
    let account_coverage = match read_u8(cursor)? {
        0 => AccountCoverage::Complete,
        1 => AccountCoverage::MissingLoadedMetadata,
        2 => AccountCoverage::RawTransactionFallback,
        3 => AccountCoverage::RawMetadataLoadedFallback,
        other => bail!("unknown candidate account coverage {other}"),
    };
    let cpi_coverage = match read_u8(cursor)? {
        0 => CpiCoverage::Recorded,
        1 => CpiCoverage::NotRecorded,
        2 => CpiCoverage::MissingMetadata,
        3 => CpiCoverage::RawTransactionFallback,
        4 => CpiCoverage::RawMetadataFallback,
        other => bail!("unknown candidate CPI coverage {other}"),
    };
    ensure!(
        read_u8(cursor)? == 0,
        "non-zero candidate transaction reserved byte"
    );
    let source_flags = read_u32(cursor)?;
    ensure!(
        source_flags & !SOURCE_TX_FLAG_MASK == 0,
        "candidate transaction has unknown source flag bits"
    );
    let account_count =
        usize::try_from(read_u32(cursor)?).context("candidate account count exceeds usize")?;
    ensure!(
        account_count <= MAX_MESSAGE_ACCOUNTS,
        "candidate transaction account count exceeds cap"
    );
    accounts.clear();
    accounts
        .try_reserve(account_count)
        .context("reserve candidate transaction accounts")?;
    for _ in 0..account_count {
        let kind = read_u8(cursor)?;
        let roles = read_u8(cursor)?;
        ensure!(
            roles & !ROLE_MASK == 0,
            "candidate account has unknown role bits"
        );
        ensure!(
            read_u16(cursor)? == 0,
            "non-zero candidate account reserved bytes"
        );
        let key = match kind {
            0 => {
                let id = read_u32(cursor)?;
                ensure!(
                    id != 0 && id <= registry_entries,
                    "candidate pubkey id {id} is outside 1..={registry_entries}"
                );
                CompactPubkey::Id(id)
            }
            1 => CompactPubkey::Raw(
                take(cursor, 32)?
                    .try_into()
                    .expect("candidate raw pubkey has exactly 32 bytes"),
            ),
            other => bail!("unknown candidate account key kind {other}"),
        };
        ensure!(
            !accounts.iter().any(|account| account.key == key),
            "candidate transaction contains a duplicate account key"
        );
        accounts.push(AccountUse { key, roles });
    }
    let transaction = ProjectedTransaction {
        source_flags,
        outcome,
        account_coverage,
        cpi_coverage,
        accounts,
    };
    validate_verifier_transaction_state(&transaction)?;
    Ok(transaction)
}

#[allow(dead_code)] // Verifier-only shared module item.
fn validate_verifier_transaction_state(transaction: &ProjectedTransaction) -> Result<()> {
    let raw_transaction = transaction.source_flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0;
    let raw_metadata = transaction.source_flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0;
    let has_metadata = transaction.source_flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
    let has_inner = transaction.source_flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0;
    let has_error = transaction.source_flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;
    if raw_transaction {
        ensure!(
            transaction.account_coverage == AccountCoverage::RawTransactionFallback
                && transaction.cpi_coverage == CpiCoverage::RawTransactionFallback
                && transaction.accounts.is_empty(),
            "candidate raw-transaction state is inconsistent"
        );
        let expected_outcome = if raw_metadata || !has_metadata {
            Outcome::Unknown
        } else if has_error {
            Outcome::Failed
        } else {
            Outcome::Success
        };
        ensure!(
            transaction.outcome == expected_outcome && (!raw_metadata || has_metadata),
            "candidate raw-transaction outcome is inconsistent"
        );
        return Ok(());
    }
    ensure!(
        transaction.account_coverage != AccountCoverage::RawTransactionFallback
            && transaction.cpi_coverage != CpiCoverage::RawTransactionFallback,
        "candidate non-raw transaction uses raw-transaction coverage"
    );
    if raw_metadata {
        ensure!(
            has_metadata
                && transaction.outcome == Outcome::Unknown
                && transaction.cpi_coverage == CpiCoverage::RawMetadataFallback
                && matches!(
                    transaction.account_coverage,
                    AccountCoverage::Complete | AccountCoverage::RawMetadataLoadedFallback
                ),
            "candidate raw-metadata state is inconsistent"
        );
        return Ok(());
    }
    if !has_metadata {
        ensure!(
            transaction.outcome == Outcome::Unknown
                && transaction.cpi_coverage == CpiCoverage::MissingMetadata
                && matches!(
                    transaction.account_coverage,
                    AccountCoverage::Complete | AccountCoverage::MissingLoadedMetadata
                ),
            "candidate missing-metadata state is inconsistent"
        );
        return Ok(());
    }
    ensure!(
        transaction.account_coverage == AccountCoverage::Complete,
        "candidate decoded metadata has incomplete account coverage"
    );
    ensure!(
        transaction.outcome
            == if has_error {
                Outcome::Failed
            } else {
                Outcome::Success
            },
        "candidate decoded metadata outcome differs from flags"
    );
    ensure!(
        transaction.cpi_coverage
            == if has_inner {
                CpiCoverage::Recorded
            } else {
                CpiCoverage::NotRecorded
            },
        "candidate decoded metadata CPI coverage differs from flags"
    );
    Ok(())
}

#[allow(dead_code)] // Verifier-only shared module item.
fn add_reference_transaction_stats(
    stats: &mut ProjectionStats,
    transaction: &ProjectedTransaction,
) -> Result<()> {
    stats.transactions = stats
        .transactions
        .checked_add(1)
        .context("reference transaction count overflow")?;
    let raw_transaction = transaction.source_flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0;
    let raw_metadata = transaction.source_flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0;
    stats.source_raw_transaction_flags = stats
        .source_raw_transaction_flags
        .checked_add(u64::from(raw_transaction))
        .context("reference raw-transaction count overflow")?;
    stats.source_raw_metadata_flags = stats
        .source_raw_metadata_flags
        .checked_add(u64::from(raw_metadata))
        .context("reference raw-metadata count overflow")?;
    stats.source_fallback_rows = stats
        .source_fallback_rows
        .checked_add(u64::from(raw_transaction || raw_metadata))
        .context("reference fallback-row count overflow")?;
    stats.account_refs = stats
        .account_refs
        .checked_add(
            u64::try_from(transaction.accounts.len()).context("account count exceeds u64")?,
        )
        .context("reference account count overflow")?;
    for account in &transaction.accounts {
        match account.key {
            CompactPubkey::Id(_) => {
                stats.id_refs = stats.id_refs.checked_add(1).context("ID count overflow")?;
            }
            CompactPubkey::Raw(_) => {
                stats.raw_refs = stats
                    .raw_refs
                    .checked_add(1)
                    .context("Raw count overflow")?;
            }
        }
    }
    match transaction.outcome {
        Outcome::Unknown => stats.unknown += 1,
        Outcome::Success => stats.success += 1,
        Outcome::Failed => stats.failed += 1,
    }
    match transaction.account_coverage {
        AccountCoverage::Complete => stats.account_complete += 1,
        AccountCoverage::MissingLoadedMetadata => stats.account_missing_loaded_metadata += 1,
        AccountCoverage::RawTransactionFallback => stats.account_raw_transaction_fallback += 1,
        AccountCoverage::RawMetadataLoadedFallback => {
            stats.account_raw_metadata_loaded_fallback += 1;
        }
    }
    match transaction.cpi_coverage {
        CpiCoverage::Recorded => stats.cpi_recorded += 1,
        CpiCoverage::NotRecorded => stats.cpi_not_recorded += 1,
        CpiCoverage::MissingMetadata => stats.cpi_missing_metadata += 1,
        CpiCoverage::RawTransactionFallback => stats.cpi_raw_transaction_fallback += 1,
        CpiCoverage::RawMetadataFallback => stats.cpi_raw_metadata_fallback += 1,
    }
    if transaction.account_coverage == AccountCoverage::Complete
        && transaction.cpi_coverage == CpiCoverage::Recorded
    {
        stats.fully_covered += 1;
    }
    Ok(())
}

#[allow(dead_code)] // Verifier-only shared module item.
fn reference_transaction_coverage_bits(transaction: &ProjectedTransaction) -> u32 {
    (1_u32 << transaction.account_coverage as u8) | (1_u32 << (8 + transaction.cpi_coverage as u8))
}

#[allow(dead_code)] // Verifier-only shared module item.
fn reference_block_coverage_flags(stats: &ProjectionStats) -> u32 {
    let account_counts = [
        stats.account_complete,
        stats.account_missing_loaded_metadata,
        stats.account_raw_transaction_fallback,
        stats.account_raw_metadata_loaded_fallback,
    ];
    let cpi_counts = [
        stats.cpi_recorded,
        stats.cpi_not_recorded,
        stats.cpi_missing_metadata,
        stats.cpi_raw_transaction_fallback,
        stats.cpi_raw_metadata_fallback,
    ];
    let mut flags = 0u32;
    for (index, count) in account_counts.into_iter().enumerate() {
        if count != 0 {
            flags |= 1 << index;
        }
    }
    for (index, count) in cpi_counts.into_iter().enumerate() {
        if count != 0 {
            flags |= 1 << (8 + index);
        }
    }
    flags
}

#[allow(dead_code)] // Verifier-only shared module item.
fn reference_project_transaction(
    worker: &mut SemanticVerifierWorker,
    config: ProjectionConfig,
    slot: u64,
    row: ArchiveV2HotTxRow,
    message_region: &[u8],
    metadata_region: &[u8],
) -> Result<(ProjectedTransaction, usize, u64)> {
    worker.reset_reference_transaction();
    let has_metadata = row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
    let raw_metadata = row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0;
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        let outcome = if raw_metadata {
            ensure!(
                has_metadata && row.metadata_len != 0,
                "raw metadata fallback has no bytes at slot {slot} tx {}",
                row.tx_index
            );
            checked_region(
                metadata_region,
                row.metadata_offset,
                row.metadata_len,
                "raw metadata",
                slot,
                row.tx_index,
            )?;
            Outcome::Unknown
        } else if has_metadata {
            let metadata = checked_region(
                metadata_region,
                row.metadata_offset,
                row.metadata_len,
                "metadata",
                slot,
                row.tx_index,
            )?;
            let mut cursor = metadata;
            let limits = MetadataDecodeLimits {
                total_message_accounts: MAX_MESSAGE_ACCOUNTS,
                top_level_instruction_count: usize::MAX,
            };
            let decoded_metadata = decode::stream_metadata_accounts_with_schema(
                &mut cursor,
                config.metadata_schema,
                true,
                limits,
                |_event| Ok::<(), anyhow::Error>(()),
            )
            .with_context(|| {
                format!(
                    "decode raw-transaction metadata slot {slot} tx {}",
                    row.tx_index
                )
            })?;
            decode::finish_metadata_tail_exact(&mut cursor, true, limits).with_context(|| {
                format!(
                    "finish raw-transaction metadata slot {slot} tx {}",
                    row.tx_index
                )
            })?;
            ensure!(
                cursor.is_empty(),
                "raw-transaction metadata has {} trailing bytes at slot {slot} tx {}",
                cursor.len(),
                row.tx_index
            );
            ensure!(
                decoded_metadata.has_error == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0),
                "outcome disagrees with flags at slot {slot} tx {}",
                row.tx_index
            );
            ensure!(
                decoded_metadata.inner_instructions_present
                    == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0),
                "inner-instruction flag disagrees at slot {slot} tx {}",
                row.tx_index
            );
            if decoded_metadata.has_error {
                Outcome::Failed
            } else {
                Outcome::Success
            }
        } else {
            Outcome::Unknown
        };
        let transaction = ProjectedTransaction {
            source_flags: row.flags,
            outcome,
            account_coverage: AccountCoverage::RawTransactionFallback,
            cpi_coverage: CpiCoverage::RawTransactionFallback,
            accounts: std::mem::take(&mut worker.reference_accounts),
        };
        validate_verifier_transaction_state(&transaction)?;
        return Ok((transaction, 0, 0));
    }

    let message = checked_region(
        message_region,
        row.message_offset,
        row.message_len,
        "message",
        slot,
        row.tx_index,
    )?;
    let mut message_cursor = message;
    let message_shape = decode::stream_message_accounts_with_schema(
        &mut message_cursor,
        config.message_schema,
        |event| {
            match event {
                decode::MessageAccountEvent::StaticAccountCount(count) => {
                    ensure!(
                        count <= MAX_MESSAGE_ACCOUNTS,
                        "static account count exceeds verifier cap"
                    );
                }
                decode::MessageAccountEvent::StaticAccount {
                    source_position,
                    key,
                } => worker.ingest_reference_account(
                    source_position,
                    key,
                    config.registry_entries,
                )?,
                decode::MessageAccountEvent::Instruction(instruction) => {
                    worker.apply_reference_role(
                        usize::from(instruction.program_id_index),
                        ROLE_TOP_LEVEL_PROGRAM,
                    )?;
                    worker
                        .reference_top_program_indexes
                        .observe(instruction.program_id_index);
                    worker
                        .reference_top_account_indexes
                        .observe_all(instruction.accounts);
                }
            }
            Ok::<(), anyhow::Error>(())
        },
    )
    .with_context(|| format!("decode verifier message slot {slot} tx {}", row.tx_index))?;
    ensure!(
        message_cursor.is_empty(),
        "message has {} trailing bytes at slot {slot} tx {}",
        message_cursor.len(),
        row.tx_index
    );
    ensure!(
        message_shape.is_v0 == (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
        "message version disagrees with flags at slot {slot} tx {}",
        row.tx_index
    );
    ensure!(
        row.signature_count == message_shape.num_required_signatures,
        "signature count disagrees with header at slot {slot} tx {}",
        row.tx_index
    );
    let expected_loaded = message_shape
        .expected_loaded_writable
        .checked_add(message_shape.expected_loaded_readonly)
        .context("loaded address count overflow")?;
    let expected_accounts = message_shape
        .static_account_count
        .checked_add(expected_loaded)
        .context("resolved account count overflow")?;
    ensure!(
        expected_accounts <= MAX_MESSAGE_ACCOUNTS,
        "resolved account count exceeds cap at slot {slot} tx {}",
        row.tx_index
    );
    ensure!(
        (row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0) == (expected_loaded != 0),
        "loaded-address flag disagrees with message at slot {slot} tx {}",
        row.tx_index
    );
    let needs_loaded = message_shape.is_v0 && expected_loaded != 0;
    let mut loaded_writable_count = 0usize;
    let mut loaded_readonly_count = 0usize;
    let (outcome, account_coverage, cpi_coverage) = if raw_metadata {
        ensure!(
            has_metadata && row.metadata_len != 0,
            "raw metadata fallback has no bytes at slot {slot} tx {}",
            row.tx_index
        );
        checked_region(
            metadata_region,
            row.metadata_offset,
            row.metadata_len,
            "raw metadata",
            slot,
            row.tx_index,
        )?;
        (
            Outcome::Unknown,
            if needs_loaded {
                AccountCoverage::RawMetadataLoadedFallback
            } else {
                AccountCoverage::Complete
            },
            CpiCoverage::RawMetadataFallback,
        )
    } else if !has_metadata {
        (
            Outcome::Unknown,
            if needs_loaded {
                AccountCoverage::MissingLoadedMetadata
            } else {
                AccountCoverage::Complete
            },
            CpiCoverage::MissingMetadata,
        )
    } else {
        ensure!(
            row.metadata_len != 0,
            "decoded metadata has no bytes at slot {slot} tx {}",
            row.tx_index
        );
        let metadata = checked_region(
            metadata_region,
            row.metadata_offset,
            row.metadata_len,
            "metadata",
            slot,
            row.tx_index,
        )?;
        let mut metadata_cursor = metadata;
        let metadata_shape = decode::stream_metadata_accounts_with_schema(
            &mut metadata_cursor,
            config.metadata_schema,
            message_shape.is_v0,
            MetadataDecodeLimits {
                total_message_accounts: expected_accounts,
                top_level_instruction_count: message_shape.instruction_count,
            },
            |event| {
                match event {
                    decode::MetadataAccountEvent::InnerInstruction(instruction) => {
                        let program = usize::try_from(instruction.program_id_index)
                            .context("CPI program index exceeds usize")?;
                        worker.apply_reference_role(program, ROLE_CPI_PROGRAM)?;
                        worker
                            .reference_cpi_account_indexes
                            .observe_all(instruction.accounts);
                    }
                    decode::MetadataAccountEvent::LoadedWritableCount(_count) => {}
                    decode::MetadataAccountEvent::LoadedWritable(key) => {
                        let position = message_shape
                            .static_account_count
                            .checked_add(loaded_writable_count)
                            .context("loaded writable position overflow")?;
                        worker.ingest_reference_account(position, key, config.registry_entries)?;
                        loaded_writable_count = loaded_writable_count
                            .checked_add(1)
                            .context("loaded writable count overflow")?;
                    }
                    decode::MetadataAccountEvent::LoadedReadonlyCount(_count) => {}
                    decode::MetadataAccountEvent::LoadedReadonly(key) => {
                        let position = message_shape
                            .static_account_count
                            .checked_add(message_shape.expected_loaded_writable)
                            .and_then(|position| position.checked_add(loaded_readonly_count))
                            .context("loaded readonly position overflow")?;
                        worker.ingest_reference_account(position, key, config.registry_entries)?;
                        loaded_readonly_count = loaded_readonly_count
                            .checked_add(1)
                            .context("loaded readonly count overflow")?;
                    }
                }
                Ok::<(), anyhow::Error>(())
            },
        )
        .with_context(|| format!("decode verifier metadata slot {slot} tx {}", row.tx_index))?;
        let metadata_tail = decode::finish_metadata_tail_exact(
            &mut metadata_cursor,
            message_shape.is_v0,
            MetadataDecodeLimits {
                total_message_accounts: expected_accounts,
                top_level_instruction_count: message_shape.instruction_count,
            },
        )
        .with_context(|| format!("finish verifier metadata slot {slot} tx {}", row.tx_index))?;
        if !message_shape.is_v0 {
            ensure!(
                metadata_tail.unstreamed_loaded_writable_count == 0
                    && metadata_tail.unstreamed_loaded_readonly_count == 0,
                "non-V0 metadata contains loaded addresses at slot {slot} tx {}",
                row.tx_index
            );
        }
        ensure!(
            metadata_cursor.is_empty(),
            "metadata has {} trailing bytes at slot {slot} tx {}",
            metadata_cursor.len(),
            row.tx_index
        );
        ensure!(
            metadata_shape.inner_instructions_present
                == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0),
            "inner-instruction flag disagrees at slot {slot} tx {}",
            row.tx_index
        );
        if message_shape.is_v0 {
            ensure!(
                loaded_writable_count == message_shape.expected_loaded_writable
                    && loaded_readonly_count == message_shape.expected_loaded_readonly,
                "loaded address shape differs at slot {slot} tx {}",
                row.tx_index
            );
        }
        ensure!(
            metadata_shape.has_error == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0),
            "outcome disagrees with flags at slot {slot} tx {}",
            row.tx_index
        );
        (
            if metadata_shape.has_error {
                Outcome::Failed
            } else {
                Outcome::Success
            },
            AccountCoverage::Complete,
            if metadata_shape.inner_instructions_present {
                CpiCoverage::Recorded
            } else {
                CpiCoverage::NotRecorded
            },
        )
    };
    finalize_reference_accounts(
        worker,
        &message_shape,
        loaded_writable_count,
        loaded_readonly_count,
        account_coverage,
        config.registry_entries,
        slot,
        row.tx_index,
    )?;
    let resolved_positions = worker.reference_source_positions;
    let duplicate_merges = worker.reference_duplicate_merges;
    let transaction = ProjectedTransaction {
        source_flags: row.flags,
        outcome,
        account_coverage,
        cpi_coverage,
        accounts: std::mem::take(&mut worker.reference_accounts),
    };
    validate_verifier_transaction_state(&transaction)?;
    Ok((transaction, resolved_positions, duplicate_merges))
}

#[allow(dead_code, clippy::too_many_arguments)] // Verifier-only shared module item.
fn finalize_reference_accounts(
    worker: &mut SemanticVerifierWorker,
    shape: &decode::StreamedMessageShape,
    loaded_writable_count: usize,
    loaded_readonly_count: usize,
    account_coverage: AccountCoverage,
    registry_entries: u32,
    slot: u64,
    tx_index: u32,
) -> Result<()> {
    let static_count = shape.static_account_count;
    let required = usize::from(shape.num_required_signatures);
    let readonly_signed = usize::from(shape.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(shape.num_readonly_unsigned_accounts);
    ensure!(
        readonly_signed <= required,
        "readonly signed count exceeds required signatures at slot {slot} tx {tx_index}"
    );
    ensure!(
        required <= static_count,
        "required signatures exceed static keys at slot {slot} tx {tx_index}"
    );
    ensure!(
        readonly_unsigned <= static_count - required,
        "readonly unsigned count exceeds unsigned static keys at slot {slot} tx {tx_index}"
    );
    ensure!(
        loaded_writable_count == shape.expected_loaded_writable
            || loaded_writable_count == 0 && shape.expected_loaded_writable != 0,
        "loaded writable projection shape is invalid at slot {slot} tx {tx_index}"
    );
    ensure!(
        loaded_readonly_count == shape.expected_loaded_readonly
            || loaded_readonly_count == 0 && shape.expected_loaded_readonly != 0,
        "loaded readonly projection shape is invalid at slot {slot} tx {tx_index}"
    );
    ensure!(
        !worker.reference_source_overflow,
        "available source account count exceeds cap at slot {slot} tx {tx_index}"
    );
    let expected_account_count = static_count
        .checked_add(shape.expected_loaded_writable)
        .and_then(|count| count.checked_add(shape.expected_loaded_readonly))
        .context("expected account count overflow")?;
    ensure!(
        expected_account_count <= MAX_MESSAGE_ACCOUNTS,
        "resolved account count exceeds {MAX_MESSAGE_ACCOUNTS} at slot {slot} tx {tx_index}"
    );
    let available_account_count = static_count
        .checked_add(loaded_writable_count)
        .and_then(|count| count.checked_add(loaded_readonly_count))
        .context("available account count overflow")?;
    ensure!(
        worker.reference_source_positions == available_account_count,
        "available source account positions are not contiguous at slot {slot} tx {tx_index}"
    );
    if account_coverage == AccountCoverage::Complete {
        ensure!(
            available_account_count == expected_account_count,
            "complete projection has {available_account_count} positions, expected {expected_account_count} at slot {slot} tx {tx_index}"
        );
    }

    let writable_signed_end = required - readonly_signed;
    let writable_unsigned_end = static_count - readonly_unsigned;
    for source_position in 0..static_count {
        if source_position < required {
            worker.apply_reference_role(source_position, ROLE_SIGNER)?;
        }
        if source_position < writable_signed_end
            || source_position >= required && source_position < writable_unsigned_end
        {
            worker.apply_reference_role(source_position, ROLE_WRITABLE)?;
        }
    }
    for source_position in static_count..static_count + loaded_writable_count {
        worker.apply_reference_role(source_position, ROLE_WRITABLE)?;
    }
    if let Some(id) = worker.reference_first_invalid_id {
        bail!("pubkey id {id} is outside 1..={registry_entries} at slot {slot} tx {tx_index}");
    }
    if let Some(index) = worker
        .reference_top_program_indexes
        .first_out_of_bounds(expected_account_count)
    {
        bail!(
            "top-level program index {index} is outside {expected_account_count} resolved accounts at slot {slot} tx {tx_index}"
        );
    }
    if let Some(index) = worker
        .reference_top_account_indexes
        .first_out_of_bounds(expected_account_count)
    {
        bail!(
            "top-level instruction account index {index} is outside {expected_account_count} resolved accounts at slot {slot} tx {tx_index}"
        );
    }
    if let Some(index) = worker
        .reference_cpi_account_indexes
        .first_out_of_bounds(expected_account_count)
    {
        bail!(
            "CPI instruction account index {index} is outside {expected_account_count} resolved accounts at slot {slot} tx {tx_index}"
        );
    }
    ensure!(
        worker
            .reference_accounts
            .iter()
            .all(|account| account.roles & !ROLE_MASK == 0),
        "reference account role mask overflow"
    );
    Ok(())
}

#[allow(dead_code, clippy::too_many_arguments)] // Verifier-only shared module item.
fn compare_candidate_report(
    report: &CandidateBenchmarkReport,
    args: &VerifyArgs,
    source_total_blocks: usize,
    selected_blocks: usize,
    source_compressed_bytes: u64,
    source_decoded_bytes: u64,
    page_decoded_bytes: u64,
    page_stored_bytes: u64,
    index_bytes: u64,
    stats: &ProjectionStats,
) -> Result<()> {
    ensure!(
        report.status == STATUS,
        "candidate report has status {}",
        report.status
    );
    ensure!(
        report.output_validation == "not-run" && report.content_hashing == "none",
        "candidate report does not describe an unverified no-hash build"
    );
    ensure!(
        report.account_semantics == ACCOUNT_SEMANTICS
            && report.epoch == args.epoch
            && report.slots_per_epoch == args.slots_per_epoch,
        "candidate report source identity or account semantics differs"
    );
    ensure!(
        report.message_schema == message_schema_name(args.message_schema.into())
            && report.metadata_schema == metadata_schema_name(args.metadata_schema.into()),
        "candidate report schema profile differs"
    );
    ensure!(
        report.source_total_blocks == source_total_blocks
            && report.selected_blocks == selected_blocks,
        "candidate report block counts differ"
    );
    ensure!(
        report.transactions == stats.transactions
            && report.account_refs == stats.account_refs
            && report.id_refs == stats.id_refs
            && report.raw_refs == stats.raw_refs
            && report.success_transactions == stats.success
            && report.failed_transactions == stats.failed
            && report.unknown_transactions == stats.unknown,
        "candidate report transaction or reference totals differ"
    );
    ensure!(
        report.fully_covered_transactions == stats.fully_covered
            && report.incomplete_coverage_transactions == stats.incomplete()
            && report.account_complete_transactions == stats.account_complete
            && report.account_missing_loaded_metadata_transactions
                == stats.account_missing_loaded_metadata
            && report.account_raw_transaction_fallbacks == stats.account_raw_transaction_fallback
            && report.account_raw_metadata_loaded_fallbacks
                == stats.account_raw_metadata_loaded_fallback
            && report.cpi_recorded_transactions == stats.cpi_recorded
            && report.cpi_not_recorded_transactions == stats.cpi_not_recorded
            && report.cpi_missing_metadata_transactions == stats.cpi_missing_metadata
            && report.cpi_raw_transaction_fallbacks == stats.cpi_raw_transaction_fallback
            && report.cpi_raw_metadata_fallbacks == stats.cpi_raw_metadata_fallback,
        "candidate report coverage totals differ"
    );
    ensure!(
        report.source_raw_transaction_fallback_flags == stats.source_raw_transaction_flags
            && report.source_raw_metadata_fallback_flags == stats.source_raw_metadata_flags
            && report.max_resolved_source_positions_per_transaction
                == stats.max_resolved_source_positions
            && report.max_unique_output_accounts_per_transaction
                == stats.max_unique_output_accounts
            && report.duplicate_account_merges == stats.duplicate_account_merges
            && report.complete_coverage == (stats.incomplete() == 0),
        "candidate report fallback or account-shape totals differ"
    );
    ensure!(
        report.source_compressed_bytes == source_compressed_bytes
            && report.source_decoded_bytes == source_decoded_bytes
            && report.page_decoded_bytes == page_decoded_bytes
            && report.page_stored_bytes == page_stored_bytes
            && report.index_bytes == index_bytes
            && report.output_bytes == page_stored_bytes + index_bytes,
        "candidate report byte totals differ"
    );
    ensure!(
        report.signature_content_reads == 0
            && report.unrelated_source_content_reads == 0
            && report.source_unchanged,
        "candidate report source access claims are not valid for this candidate"
    );
    Ok(())
}

#[allow(dead_code)] // Verifier-only shared module item.
fn resize_for_exact_read(bytes: &mut Vec<u8>, length: usize) -> Result<()> {
    if bytes.len() < length {
        bytes
            .try_reserve_exact(length - bytes.len())
            .context("reserve exact candidate range")?;
        bytes.resize(length, 0);
    } else {
        bytes.truncate(length);
    }
    Ok(())
}

#[allow(dead_code)] // Verifier-only shared module item.
fn read_file_exact_at(file: &File, bytes: &mut [u8], offset: u64, label: &str) -> Result<()> {
    let mut read = 0usize;
    while read < bytes.len() {
        let count = file
            .read_at(&mut bytes[read..], offset + read as u64)
            .with_context(|| format!("read {label} at offset {}", offset + read as u64))?;
        ensure!(count != 0, "short read for {label}");
        read += count;
    }
    Ok(())
}

fn run(args: Args) -> Result<()> {
    ensure!(args.source.is_dir(), "source must be a directory");
    ensure!(
        args.slots_per_epoch > 0,
        "--slots-per-epoch must be positive"
    );
    ensure!(
        (1..=64).contains(&args.workers),
        "--workers must be in 1..=64"
    );
    ensure!(
        args.benchmark_prefix_blocks != Some(0),
        "--benchmark-prefix-blocks must be positive"
    );
    ensure!(
        !(args.source_split_effects && args.lean_block_chunks),
        "--source-split-effects and --lean-block-chunks are separate benchmark modes"
    );
    ensure!(
        args.lean_block_chunks || args.lean_compression.is_none(),
        "--lean-compression requires --lean-block-chunks"
    );
    ensure!(
        args.lean_block_chunks || args.lean_zstd_level.is_none(),
        "--lean-zstd-level requires --lean-block-chunks"
    );
    let lean_compression = args
        .lean_compression
        .unwrap_or(LeanCompressionArg::Adaptive);
    let lean_zstd_level = args.lean_zstd_level.unwrap_or(LeanZstdLevelArg::One);
    ensure!(
        lean_compression != LeanCompressionArg::Raw || lean_zstd_level == LeanZstdLevelArg::One,
        "--lean-zstd-level must be 1 when --lean-compression is raw"
    );
    ensure!(
        !path_entry_exists(&args.output)?,
        "output {} already exists",
        args.output.display()
    );
    let source_canonical = fs::canonicalize(&args.source)
        .with_context(|| format!("canonicalize source {}", args.source.display()))?;
    let output_parent = usable_parent(&args.output);
    ensure!(
        output_parent.is_dir(),
        "output parent {} must already exist",
        output_parent.display()
    );
    let output_parent_canonical = fs::canonicalize(output_parent)
        .with_context(|| format!("canonicalize output parent {}", output_parent.display()))?;
    let output_name = args
        .output
        .file_name()
        .context("output needs a final path component")?;
    let resolved_output = output_parent_canonical.join(output_name);
    ensure!(
        !resolved_output.starts_with(&source_canonical),
        "output must be outside the source archive directory {}",
        source_canonical.display()
    );
    let staging = staging_path(&args.output)?;
    ensure!(
        !path_entry_exists(&staging)?,
        "staging output {} already exists; inspect or move it before retrying",
        staging.display()
    );

    let total_started = Instant::now();
    let pinned = PinnedLocalRangeSource::new(&args.source);
    let guarded = NoSignatureContentSource::new(pinned.clone());
    let archive = ArchiveReader::open_trusted(
        guarded.clone(),
        TrustedGenerationIdentity {
            cluster_id: "mainnet-beta".to_owned(),
            epoch: args.epoch,
            generation_id: format!("{STATUS}-{}", args.epoch),
            slots_per_epoch: args.slots_per_epoch,
        },
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
    .context("admit trusted local Archive V2 source")?;
    let source_total_blocks = archive.index().rows.len();
    ensure!(source_total_blocks > 0, "source block index is empty");
    let selected_blocks = args.benchmark_prefix_blocks.unwrap_or(source_total_blocks);
    ensure!(
        selected_blocks <= source_total_blocks,
        "--benchmark-prefix-blocks {selected_blocks} exceeds {source_total_blocks} source blocks"
    );

    let selected_rows = &archive.index().rows[..selected_blocks];
    let source_compressed_bytes = checked_row_byte_sum(selected_rows, |row| row.compressed_len)?;
    let source_decoded_bytes = checked_row_byte_sum(selected_rows, |row| row.uncompressed_len)?;
    let expected_transactions = selected_rows.iter().try_fold(0_u64, |total, row| {
        total
            .checked_add(u64::from(row.tx_count))
            .context("selected transaction count overflow")
    })?;

    fs::create_dir(&staging)
        .with_context(|| format!("create staging directory {}", staging.display()))?;
    let pages_path = staging.join(PAGES_FILE);
    let pages_file =
        File::create(&pages_path).with_context(|| format!("create {}", pages_path.display()))?;
    let mut pages = BufWriter::with_capacity(8 << 20, pages_file);
    let mut page_offset = 0_u64;
    let mut page_decoded_bytes = 0_u64;
    let mut index_rows = Vec::with_capacity(selected_blocks);
    let mut projection_stats = ProjectionStats::default();
    let mut projection_timing = ProjectionTiming::default();
    let mut ordered_write_time = Duration::ZERO;

    let projection_config = ProjectionConfig {
        message_schema: args.message_schema.into(),
        metadata_schema: args.metadata_schema.into(),
        registry_entries: archive.registry_entries(),
    };
    let source_split_effects = args.source_split_effects;
    let lean_block_chunks = args.lean_block_chunks;
    let effect_binding = SplitHeaderBinding {
        epoch: args.epoch,
        slots_per_epoch: args.slots_per_epoch,
        selected_blocks: u64::try_from(selected_blocks)
            .context("selected block count exceeds u64")?,
        selected_transactions: expected_transactions,
        message_schema: projection_config.message_schema,
        metadata_schema: projection_config.metadata_schema,
        prefix: args.benchmark_prefix_blocks.is_some(),
    };
    let mut split_writers = if source_split_effects {
        Some(SplitWriters::create(&staging, effect_binding)?)
    } else {
        None
    };
    let mut lean_writers = if lean_block_chunks {
        Some(LeanWriters::create(
            &staging,
            effect_binding,
            lean_compression,
            lean_zstd_level,
        )?)
    } else {
        None
    };
    guarded.reset_block_content_reads()?;
    let pipeline_started = Instant::now();
    let reader_stats = archive
        .process_borrowed_blocks_parallel_ordered(
            Range {
                start: 0,
                end: selected_blocks,
            },
            OrderedParallelBlockConfig {
                max_blocks_per_batch: 1_024,
                decode_workers: args.workers,
                discard_rewards: true,
                ..OrderedParallelBlockConfig::default()
            },
            |_| {
                if source_split_effects {
                    ProjectionWorker::new_with_split(projection_config)
                } else if lean_block_chunks {
                    ProjectionWorker::new_with_lean(
                        projection_config,
                        lean_compression,
                        lean_zstd_level,
                    )
                } else {
                    ProjectionWorker::new(projection_config)
                }
            },
            |worker, _sequence, block| project_block(worker, block),
            |_sequence, mut block| {
                let write_started = Instant::now();
                pages
                    .write_all(&block.page)
                    .with_context(|| format!("append block {} account page", block.row.block_id))?;
                let stored_len =
                    u32::try_from(block.page.len()).context("stored account page exceeds u32")?;
                let ref_count = u32::try_from(block.stats.account_refs)
                    .context("block account-reference count exceeds u32")?;
                let coverage_flags = block_coverage_flags(&block.stats);
                index_rows.push(AccountIndexRow {
                    block_id: block.row.block_id,
                    slot: block.row.slot,
                    page_offset,
                    stored_len,
                    decoded_len: block.decoded_page_len,
                    tx_count: block.row.tx_count,
                    account_ref_count: ref_count,
                    coverage_flags,
                    flags: u32::from(block.compressed) * INDEX_ROW_FLAG_ZSTD,
                });
                page_offset = page_offset
                    .checked_add(u64::from(stored_len))
                    .context("account page offset overflow")?;
                page_decoded_bytes = page_decoded_bytes
                    .checked_add(u64::from(block.decoded_page_len))
                    .context("decoded account page bytes overflow")?;
                ordered_write_time = ordered_write_time.saturating_add(write_started.elapsed());
                match (&mut split_writers, block.split_effects.take()) {
                    (Some(writers), Some(split)) => writers.append(block.row, split)?,
                    (Some(_), None) => bail!(
                        "block {} did not return requested source split effects",
                        block.row.block_id
                    ),
                    (None, Some(_)) => bail!(
                        "block {} returned unrequested source split effects",
                        block.row.block_id
                    ),
                    (None, None) => {}
                }
                match (&mut lean_writers, block.lean_effects.take()) {
                    (Some(writers), Some(lean)) => writers.append(block.row, lean)?,
                    (Some(_), None) => bail!(
                        "block {} did not return requested lean effects",
                        block.row.block_id
                    ),
                    (None, Some(_)) => bail!(
                        "block {} returned unrequested lean effects",
                        block.row.block_id
                    ),
                    (None, None) => {}
                }
                projection_stats.merge(block.stats);
                projection_timing.merge(block.timing);
                Ok::<_, anyhow::Error>(())
            },
        )
        .context("run ordered borrowed account projection")?;
    let pipeline_elapsed = pipeline_started.elapsed();

    ensure!(
        index_rows.len() == selected_blocks,
        "projected {} blocks, expected {selected_blocks}",
        index_rows.len()
    );
    ensure!(
        reader_stats.block_count == selected_blocks as u64,
        "reader projected {} blocks, expected {selected_blocks}",
        reader_stats.block_count
    );
    ensure!(
        projection_stats.transactions == expected_transactions,
        "projected {} transactions, expected {expected_transactions}",
        projection_stats.transactions
    );
    ensure!(
        reader_stats.compressed_bytes == source_compressed_bytes,
        "reader consumed {} compressed bytes, expected {source_compressed_bytes}",
        reader_stats.compressed_bytes
    );
    let first_source_offset = selected_rows
        .first()
        .context("selected block prefix is empty")?
        .compressed_offset;
    let last_source_row = selected_rows
        .last()
        .context("selected block prefix is empty")?;
    let source_end_offset = last_source_row
        .compressed_offset
        .checked_add(u64::from(last_source_row.compressed_len))
        .context("selected block source end overflow")?;
    let source_block_read_calls = guarded.validate_block_content_reads(
        first_source_offset,
        source_end_offset,
        source_compressed_bytes,
    )?;
    ensure!(
        source_block_read_calls == reader_stats.read_call_count,
        "source wrapper observed {source_block_read_calls} block reads, reader reports {}",
        reader_stats.read_call_count
    );
    pages.flush().context("flush account pages")?;
    let pages_file = pages.into_inner().context("finish account pages writer")?;
    pages_file.sync_all().context("sync account pages")?;
    let split_summary = match split_writers.take() {
        Some(writers) => {
            let summary = writers.finish(selected_blocks)?;
            let classified_transactions = summary
                .stats
                .missing_metadata
                .checked_add(summary.stats.decoded_metadata)
                .and_then(|count| count.checked_add(summary.stats.raw_metadata))
                .context("split metadata-state transaction count overflow")?;
            ensure!(
                classified_transactions == expected_transactions,
                "split metadata states classify {classified_transactions} transactions, expected {expected_transactions}"
            );
            ensure!(
                summary.stats.planes[SplitPlane::MetadataStates.index()].records
                    == expected_transactions,
                "split metadata state plane does not have one record per transaction"
            );
            ensure!(
                summary.stats.tx_raw_flags == projection_stats.source_raw_transaction_flags,
                "split TX_RAW count differs from account projection source flags"
            );
            Some(summary)
        }
        None => None,
    };
    let lean_summary = match lean_writers.take() {
        Some(writers) => {
            let summary = writers.finish(selected_blocks)?;
            let classified_transactions = summary
                .stats
                .missing_metadata
                .checked_add(summary.stats.decoded_metadata)
                .and_then(|count| count.checked_add(summary.stats.raw_metadata))
                .context("lean metadata-state transaction count overflow")?;
            ensure!(
                classified_transactions == expected_transactions,
                "lean directory classifies {classified_transactions} transactions, expected {expected_transactions}"
            );
            ensure!(
                summary.stats.objects[LeanObject::TransactionDirectory.index()].records
                    == expected_transactions,
                "lean directory does not have one row per transaction"
            );
            ensure!(
                summary.stats.tx_raw_flags == projection_stats.source_raw_transaction_flags,
                "lean TX_RAW count differs from account projection source flags"
            );
            Some(summary)
        }
        None => None,
    };

    let finalize_started = Instant::now();
    let index_path = staging.join(INDEX_FILE);
    write_index(
        &index_path,
        &index_rows,
        page_offset,
        archive.registry_entries(),
        projection_config.message_schema,
        projection_config.metadata_schema,
    )?;
    let index_bytes = u64::try_from(INDEX_HEADER_LEN)
        .context("index header size exceeds u64")?
        .checked_add(
            u64::try_from(index_rows.len())
                .context("index row count exceeds u64")?
                .checked_mul(u64::try_from(INDEX_ROW_LEN).expect("fixed row length fits u64"))
                .context("account index row bytes overflow")?,
        )
        .context("account index size overflow")?;
    ensure!(
        guarded.rejected_signature_content_reads() == 0,
        "the projection attempted to read signature content"
    );
    ensure!(
        guarded.rejected_unrelated_content_reads() == 0,
        "the projection attempted to read registry, blockhash, PoH, or shredding content"
    );
    pinned
        .verify_unchanged()
        .context("source object changed during account projection")?;

    let finalize_elapsed = finalize_started.elapsed();
    let total_elapsed = total_started.elapsed();
    let account_output_bytes = page_offset
        .checked_add(index_bytes)
        .context("account projection output byte count overflow")?;
    let lean_compression_time = lean_summary.as_ref().map_or(Duration::ZERO, |summary| {
        summary
            .stats
            .objects
            .iter()
            .fold(Duration::ZERO, |total, stats| {
                total.saturating_add(stats.compression_time)
            })
    });
    let effect_accounted_worker_time = projection_timing
        .message_traversal
        .saturating_add(projection_timing.metadata_traversal)
        .saturating_add(projection_timing.account_role_assembly)
        .saturating_add(projection_timing.page_encode)
        .saturating_add(projection_timing.page_zstd)
        .saturating_add(
            split_summary
                .as_ref()
                .map_or(Duration::ZERO, |summary| summary.compression_time),
        )
        .saturating_add(lean_compression_time);
    let split_copy_and_other_worker_residual = reader_stats
        .worker_projection_sum_time
        .saturating_sub(effect_accounted_worker_time);
    let source_split = split_summary
        .map(|summary| {
            split_benchmark_report(
                summary,
                account_output_bytes,
                projection_timing.metadata_traversal,
                split_copy_and_other_worker_residual,
            )
        })
        .transpose()?;
    let lean_copy_and_other_worker_residual = reader_stats
        .worker_projection_sum_time
        .saturating_sub(effect_accounted_worker_time);
    let lean_block_chunks = lean_summary
        .map(|summary| {
            lean_benchmark_report(
                summary,
                lean_compression,
                lean_zstd_level,
                account_output_bytes,
                projection_timing.metadata_traversal,
                lean_copy_and_other_worker_residual,
            )
        })
        .transpose()?;
    let report = BenchmarkReport {
        status: STATUS,
        output_validation: "not-run",
        content_hashing: "none",
        account_semantics: ACCOUNT_SEMANTICS,
        epoch: args.epoch,
        slots_per_epoch: args.slots_per_epoch,
        message_schema: message_schema_name(projection_config.message_schema),
        metadata_schema: metadata_schema_name(projection_config.metadata_schema),
        workers: args.workers,
        benchmark_prefix_blocks: args.benchmark_prefix_blocks,
        source_total_blocks,
        selected_blocks,
        transactions: projection_stats.transactions,
        account_refs: projection_stats.account_refs,
        id_refs: projection_stats.id_refs,
        raw_refs: projection_stats.raw_refs,
        success_transactions: projection_stats.success,
        failed_transactions: projection_stats.failed,
        unknown_transactions: projection_stats.unknown,
        fully_covered_transactions: projection_stats.fully_covered,
        incomplete_coverage_transactions: projection_stats.incomplete(),
        account_complete_transactions: projection_stats.account_complete,
        account_missing_loaded_metadata_transactions: projection_stats
            .account_missing_loaded_metadata,
        account_raw_transaction_fallbacks: projection_stats.account_raw_transaction_fallback,
        account_raw_metadata_loaded_fallbacks: projection_stats
            .account_raw_metadata_loaded_fallback,
        cpi_recorded_transactions: projection_stats.cpi_recorded,
        cpi_not_recorded_transactions: projection_stats.cpi_not_recorded,
        cpi_missing_metadata_transactions: projection_stats.cpi_missing_metadata,
        cpi_raw_transaction_fallbacks: projection_stats.cpi_raw_transaction_fallback,
        cpi_raw_metadata_fallbacks: projection_stats.cpi_raw_metadata_fallback,
        source_raw_transaction_fallback_flags: projection_stats.source_raw_transaction_flags,
        source_raw_metadata_fallback_flags: projection_stats.source_raw_metadata_flags,
        max_resolved_source_positions_per_transaction: projection_stats
            .max_resolved_source_positions,
        max_unique_output_accounts_per_transaction: projection_stats.max_unique_output_accounts,
        duplicate_account_merges: projection_stats.duplicate_account_merges,
        complete_coverage: projection_stats.incomplete() == 0,
        source_compressed_bytes,
        source_decoded_bytes,
        page_decoded_bytes,
        page_stored_bytes: page_offset,
        index_bytes,
        output_bytes: account_output_bytes,
        transactions_per_second: rate(projection_stats.transactions, pipeline_elapsed),
        source_compressed_mib_per_second: mib_rate(source_compressed_bytes, pipeline_elapsed),
        source_block_read_calls,
        reader_batches: reader_stats.batch_count,
        reader_max_blocks_per_batch: reader_stats.max_blocks_per_batch,
        reader_max_compressed_batch_bytes: reader_stats.max_compressed_batch_bytes,
        reader_max_declared_uncompressed_batch_bytes: reader_stats
            .max_declared_uncompressed_batch_bytes,
        reader_max_retained_decompressed_buffer_bytes: reader_stats
            .max_retained_decompressed_buffer_bytes,
        signature_content_reads: 0,
        unrelated_source_content_reads: 0,
        source_unchanged: true,
        timing: timing_report(
            total_elapsed,
            reader_stats,
            args.workers,
            projection_timing,
            ordered_write_time,
            finalize_elapsed,
        ),
        source_split,
        lean_block_chunks,
    };
    let report_json = serde_json::to_vec_pretty(&report).context("encode benchmark report")?;
    let report_path = staging.join(REPORT_FILE);
    write_synced(&report_path, &report_json)?;
    sync_directory(&staging)?;
    pinned
        .verify_unchanged()
        .context("source object changed before account projection rename")?;
    publish_staging_directory(&staging, &args.output)?;
    sync_directory(usable_parent(&args.output))?;
    println!("{}", String::from_utf8_lossy(&report_json));
    Ok(())
}

fn project_block(
    worker: &mut ProjectionWorker,
    block: BorrowedDecodedBlock<'_>,
) -> Result<ProjectedBlock> {
    ensure!(
        !block.uses_owned_fallback(),
        "account projection requires borrowed Archive V2 block lanes"
    );
    let row = block.index_row;
    if let Some(split) = worker.split.as_mut() {
        split.begin_block();
    }
    if let Some(lean) = worker.lean.as_mut() {
        lean.begin_block();
    }
    let mut stats = ProjectionStats::default();
    let mut timing = ProjectionTiming::default();
    let page_header_started = Instant::now();
    let minimum_page_len = usize::try_from(row.tx_count)
        .context("block transaction count exceeds usize")?
        .checked_mul(12)
        .and_then(|length| length.checked_add(PAGE_HEADER_LEN))
        .context("minimum account page length overflow")?;
    let mut decoded = std::mem::take(&mut worker.decoded_page_scratch);
    decoded.clear();
    decoded
        .try_reserve_exact(minimum_page_len)
        .context("reserve minimum account page")?;
    append_page_header(&mut decoded, row, 0)?;
    timing.page_encode = timing
        .page_encode
        .saturating_add(page_header_started.elapsed());
    let mut transaction_count = 0_usize;
    let mut coverage_flags = 0_u32;
    for tx_row in block.tx_rows() {
        let transaction = project_transaction_timed(
            worker,
            row.slot,
            tx_row,
            block.message_bytes(),
            block.metadata_bytes(),
            &mut timing,
        )?;
        add_transaction_stats(&mut stats, &transaction)?;
        stats.max_resolved_source_positions = stats
            .max_resolved_source_positions
            .max(worker.source_position_count);
        stats.max_unique_output_accounts = stats
            .max_unique_output_accounts
            .max(transaction.accounts.len());
        stats.duplicate_account_merges = stats
            .duplicate_account_merges
            .checked_add(worker.duplicate_merges)
            .context("block duplicate-account merge count overflow")?;
        coverage_flags |= transaction_coverage_bits(&transaction);
        let page_encode_started = Instant::now();
        append_encoded_transaction(&mut decoded, &transaction)?;
        timing.page_encode = timing
            .page_encode
            .saturating_add(page_encode_started.elapsed());
        worker.accounts = transaction.accounts;
        worker.accounts.clear();
        transaction_count = transaction_count
            .checked_add(1)
            .context("projected transaction count overflow")?;
    }
    ensure!(
        transaction_count == row.tx_count as usize,
        "slot {} projected {} transaction rows, expected {}",
        row.slot,
        transaction_count,
        row.tx_count
    );
    let page_finish_started = Instant::now();
    decoded[20..24].copy_from_slice(&coverage_flags.to_le_bytes());
    timing.page_encode = timing
        .page_encode
        .saturating_add(page_finish_started.elapsed());
    let decoded_page_len =
        u32::try_from(decoded.len()).context("decoded account page exceeds u32")?;
    let page_zstd_started = Instant::now();
    let compressed = worker
        .compressor
        .compress(&decoded)
        .context("compress account page")?;
    timing.page_zstd = timing.page_zstd.saturating_add(page_zstd_started.elapsed());
    let (page, is_compressed) = if compressed.len() < decoded.len() {
        worker.retain_decoded_page_scratch(decoded);
        (compressed, true)
    } else {
        worker.retain_decoded_page_scratch(compressed);
        (decoded, false)
    };
    let split_effects = if worker.split.is_some() {
        let rewards_field = block
            .rewards_field_bytes()
            .context("borrow exact block reward field")?;
        {
            let split = worker.split.as_mut().expect("checked split worker");
            split.finish_block_transactions(row.tx_count)?;
            split.record_block_rewards(rewards_field)?;
        }
        let mut split = worker.split.take().expect("checked split worker");
        let encoded = encode_split_effects(&mut split, &mut worker.compressor, row);
        worker.split = Some(split);
        Some(encoded?)
    } else {
        None
    };
    let lean_effects = if worker.lean.is_some() {
        let rewards_field = block
            .rewards_field_bytes()
            .context("borrow exact block reward field")?;
        {
            let lean = worker.lean.as_mut().expect("checked lean worker");
            lean.finish_block(row.tx_count)?;
            lean.record_block_rewards(rewards_field)?;
        }
        let mode = worker.lean_compression;
        let zstd_level = worker.lean_zstd_level;
        let mut lean = worker.lean.take().expect("checked lean worker");
        // Level 1 deliberately keeps the accepted pre-level-selector compressor path byte-for-byte.
        let encoded = if zstd_level == LeanZstdLevelArg::One {
            encode_lean_effects(&mut lean, &mut worker.compressor, mode)
        } else {
            let compressor = worker
                .lean_compressor
                .as_mut()
                .context("non-default lean zstd compressor is missing")?;
            encode_lean_effects(&mut lean, compressor, mode)
        };
        worker.lean = Some(lean);
        Some(encoded?)
    } else {
        None
    };
    Ok(ProjectedBlock {
        row,
        page,
        decoded_page_len,
        compressed: is_compressed,
        stats,
        timing,
        split_effects,
        lean_effects,
    })
}

fn project_transaction_timed(
    worker: &mut ProjectionWorker,
    slot: u64,
    row: ArchiveV2HotTxRow,
    message_region: &[u8],
    metadata_region: &[u8],
    timing: &mut ProjectionTiming,
) -> Result<ProjectedTransaction> {
    worker.reset_transaction();
    let config = worker.config;
    let has_metadata = row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
    let raw_metadata = row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0;
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        let metadata_started = Instant::now();
        let (outcome, pending_split) = if raw_metadata {
            ensure!(
                has_metadata && row.metadata_len != 0,
                "raw metadata fallback has no source metadata range at slot {slot} tx {}",
                row.tx_index
            );
            let metadata = checked_region(
                metadata_region,
                row.metadata_offset,
                row.metadata_len,
                "raw metadata",
                slot,
                row.tx_index,
            )?;
            (
                Outcome::Unknown,
                worker
                    .effects_requested()
                    .then_some(PendingSplitMetadata::Raw(metadata)),
            )
        } else if has_metadata {
            ensure!(
                row.metadata_len != 0,
                "decoded metadata has an empty source range at slot {slot} tx {}",
                row.tx_index
            );
            let metadata = checked_region(
                metadata_region,
                row.metadata_offset,
                row.metadata_len,
                "metadata",
                slot,
                row.tx_index,
            )?;
            let (has_error, pending_split) = if worker.effects_requested() {
                let mut metadata_cursor = metadata;
                let effects = decode::stream_metadata_effects_structural_with_schema(
                    &mut metadata_cursor,
                    config.metadata_schema,
                    |_event| Ok::<(), anyhow::Error>(()),
                )
                .with_context(|| {
                    format!(
                        "decode raw-transaction metadata slot {slot} tx {}",
                        row.tx_index
                    )
                })?;
                ensure!(
                    effects.shape.inner_instructions_present
                        == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0),
                    "inner-instruction presence disagrees with row flags at slot {slot} tx {}",
                    row.tx_index
                );
                (
                    effects.shape.has_error,
                    Some(PendingSplitMetadata::Decoded(effects)),
                )
            } else {
                let mut metadata_cursor = metadata;
                let has_error = decode::decode_metadata_error_with_schema(
                    &mut metadata_cursor,
                    config.metadata_schema,
                )
                .with_context(|| {
                    format!(
                        "decode raw-transaction outcome slot {slot} tx {}",
                        row.tx_index
                    )
                })?;
                (has_error, None)
            };
            ensure!(
                has_error == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0),
                "outcome disagrees with row flags at slot {slot} tx {}",
                row.tx_index
            );
            (
                if has_error {
                    Outcome::Failed
                } else {
                    Outcome::Success
                },
                pending_split,
            )
        } else {
            (
                Outcome::Unknown,
                worker
                    .effects_requested()
                    .then_some(PendingSplitMetadata::Missing),
            )
        };
        timing.metadata_traversal = timing
            .metadata_traversal
            .saturating_add(metadata_started.elapsed());
        let transaction = ProjectedTransaction {
            source_flags: row.flags,
            outcome,
            account_coverage: AccountCoverage::RawTransactionFallback,
            cpi_coverage: CpiCoverage::RawTransactionFallback,
            accounts: std::mem::take(&mut worker.accounts),
        };
        validate_projected_transaction(&transaction)?;
        if let Some(pending) = pending_split {
            worker.record_pending_effects(row.tx_index, row.flags, pending)?;
        }
        return Ok(transaction);
    }

    worker.reserve_account_scratch()?;
    let message_started = Instant::now();
    let message = checked_region(
        message_region,
        row.message_offset,
        row.message_len,
        "message",
        slot,
        row.tx_index,
    )?;
    let mut message_cursor = message;
    let decoded = decode::stream_message_accounts_with_schema(
        &mut message_cursor,
        config.message_schema,
        |event| match event {
            decode::MessageAccountEvent::StaticAccountCount(_) => Ok(()),
            decode::MessageAccountEvent::StaticAccount {
                source_position,
                key,
            } => worker.ingest_account(source_position, key, config.registry_entries),
            decode::MessageAccountEvent::Instruction(instruction) => {
                let program_index = instruction.program_id_index;
                worker.apply_role(usize::from(program_index), ROLE_TOP_LEVEL_PROGRAM)?;
                worker.top_program_indexes.observe(program_index);
                worker.top_account_indexes.observe_all(instruction.accounts);
                Ok(())
            }
        },
    )
    .with_context(|| format!("decode message slot {slot} tx {}", row.tx_index))?;
    ensure!(
        message_cursor.is_empty(),
        "message decode left {} trailing bytes at slot {slot} tx {}",
        message_cursor.len(),
        row.tx_index
    );
    ensure!(
        decoded.is_v0 == (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
        "message version disagrees with row flags at slot {slot} tx {}",
        row.tx_index
    );
    ensure!(
        usize::from(decoded.num_required_signatures) <= decoded.static_account_count,
        "required signatures exceed static keys at slot {slot} tx {}",
        row.tx_index
    );
    ensure!(
        row.signature_count == decoded.num_required_signatures,
        "signature count disagrees with the message header at slot {slot} tx {}",
        row.tx_index
    );
    timing.message_traversal = timing
        .message_traversal
        .saturating_add(message_started.elapsed());

    let mut invalid_cpi_program_index = None;
    let mut invalid_cpi_account_index = None;
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        ensure!(
            has_metadata && row.metadata_len != 0,
            "raw metadata fallback has no source metadata range at slot {slot} tx {}",
            row.tx_index
        );
        checked_region(
            metadata_region,
            row.metadata_offset,
            row.metadata_len,
            "raw metadata",
            slot,
            row.tx_index,
        )?;
    }
    let expected_loaded = decoded
        .expected_loaded_writable
        .checked_add(decoded.expected_loaded_readonly)
        .context("loaded address count overflow")?;
    let expected_account_count = decoded
        .static_account_count
        .checked_add(expected_loaded)
        .context("resolved message account count overflow")?;
    ensure!(
        expected_account_count <= MAX_MESSAGE_ACCOUNTS,
        "resolved account count exceeds {MAX_MESSAGE_ACCOUNTS} at slot {slot} tx {}",
        row.tx_index
    );
    let message_needs_loaded_metadata = decoded.is_v0 && expected_loaded != 0;
    let metadata_started = Instant::now();
    let mut actual_loaded_writable = 0_usize;
    let mut actual_loaded_readonly = 0_usize;
    let mut pending_split = None;
    let (outcome, account_coverage, cpi_coverage) =
        if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
            if worker.effects_requested() {
                let metadata = checked_region(
                    metadata_region,
                    row.metadata_offset,
                    row.metadata_len,
                    "raw metadata",
                    slot,
                    row.tx_index,
                )?;
                pending_split = Some(PendingSplitMetadata::Raw(metadata));
            }
            (
                Outcome::Unknown,
                if message_needs_loaded_metadata {
                    AccountCoverage::RawMetadataLoadedFallback
                } else {
                    AccountCoverage::Complete
                },
                CpiCoverage::RawMetadataFallback,
            )
        } else if !has_metadata {
            if worker.effects_requested() {
                pending_split = Some(PendingSplitMetadata::Missing);
            }
            (
                Outcome::Unknown,
                if message_needs_loaded_metadata {
                    AccountCoverage::MissingLoadedMetadata
                } else {
                    AccountCoverage::Complete
                },
                CpiCoverage::MissingMetadata,
            )
        } else {
            let metadata = checked_region(
                metadata_region,
                row.metadata_offset,
                row.metadata_len,
                "metadata",
                slot,
                row.tx_index,
            )?;
            let mut metadata_cursor = metadata;
            let mut next_loaded_writable_position = decoded.static_account_count;
            let mut next_loaded_readonly_position = decoded
                .static_account_count
                .checked_add(decoded.expected_loaded_writable)
                .context("loaded readonly source position overflow")?;
            let limits = MetadataDecodeLimits {
                total_message_accounts: expected_account_count,
                top_level_instruction_count: decoded.instruction_count,
            };
            let effects_enabled = worker.effects_requested();
            let mut visit_event = |event| match event {
                decode::MetadataAccountEvent::InnerInstruction(instruction) => {
                    match usize::try_from(instruction.program_id_index) {
                        Ok(program_index) if program_index < expected_account_count => {
                            worker.apply_role(program_index, ROLE_CPI_PROGRAM)?;
                        }
                        _ if invalid_cpi_program_index.is_none() => {
                            invalid_cpi_program_index = Some(instruction.program_id_index);
                        }
                        _ => {}
                    }
                    if invalid_cpi_account_index.is_none() {
                        invalid_cpi_account_index = instruction
                            .accounts
                            .iter()
                            .copied()
                            .find(|index| usize::from(*index) >= expected_account_count);
                    }
                    Ok(())
                }
                decode::MetadataAccountEvent::LoadedWritableCount(_)
                | decode::MetadataAccountEvent::LoadedReadonlyCount(_) => Ok(()),
                decode::MetadataAccountEvent::LoadedWritable(key) => {
                    let source_position = next_loaded_writable_position;
                    next_loaded_writable_position = next_loaded_writable_position
                        .checked_add(1)
                        .context("loaded writable source position overflow")?;
                    worker.ingest_account(source_position, key, config.registry_entries)
                }
                decode::MetadataAccountEvent::LoadedReadonly(key) => {
                    let source_position = next_loaded_readonly_position;
                    next_loaded_readonly_position = next_loaded_readonly_position
                        .checked_add(1)
                        .context("loaded readonly source position overflow")?;
                    worker.ingest_account(source_position, key, config.registry_entries)
                }
            };
            let metadata = if effects_enabled {
                let effects = decode::stream_metadata_effects_with_schema(
                    &mut metadata_cursor,
                    config.metadata_schema,
                    limits,
                    &mut visit_event,
                )
                .with_context(|| format!("decode metadata slot {slot} tx {}", row.tx_index))?;
                let shape = effects.shape;
                pending_split = Some(PendingSplitMetadata::Decoded(effects));
                shape
            } else {
                decode::stream_metadata_accounts_with_schema(
                    &mut metadata_cursor,
                    config.metadata_schema,
                    decoded.is_v0,
                    limits,
                    &mut visit_event,
                )
                .with_context(|| format!("decode metadata slot {slot} tx {}", row.tx_index))?
            };
            ensure!(
                metadata.inner_instructions_present
                    == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0),
                "inner-instruction presence disagrees with row flags at slot {slot} tx {}",
                row.tx_index
            );
            if decoded.is_v0 {
                ensure!(
                    metadata.loaded_writable_count == decoded.expected_loaded_writable,
                    "loaded writable count mismatch at slot {slot} tx {}",
                    row.tx_index
                );
                ensure!(
                    metadata.loaded_readonly_count == decoded.expected_loaded_readonly,
                    "loaded readonly count mismatch at slot {slot} tx {}",
                    row.tx_index
                );
                actual_loaded_writable = metadata.loaded_writable_count;
                actual_loaded_readonly = metadata.loaded_readonly_count;
            } else if effects_enabled {
                ensure!(
                    metadata.loaded_writable_count == 0 && metadata.loaded_readonly_count == 0,
                    "non-V0 metadata has loaded addresses at slot {slot} tx {}",
                    row.tx_index
                );
            }
            ensure!(
                metadata.has_error == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0),
                "outcome disagrees with row flags at slot {slot} tx {}",
                row.tx_index
            );
            let outcome = if metadata.has_error {
                Outcome::Failed
            } else {
                Outcome::Success
            };
            (
                outcome,
                AccountCoverage::Complete,
                if metadata.inner_instructions_present {
                    CpiCoverage::Recorded
                } else {
                    CpiCoverage::NotRecorded
                },
            )
        };
    ensure!(
        invalid_cpi_program_index.is_none(),
        "CPI program index {} is outside {expected_account_count} resolved accounts at slot {slot} tx {}",
        invalid_cpi_program_index.unwrap_or_default(),
        row.tx_index
    );
    timing.metadata_traversal = timing
        .metadata_traversal
        .saturating_add(metadata_started.elapsed());

    ensure!(
        (row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0) == (expected_loaded != 0),
        "loaded-address presence disagrees with message lookups at slot {slot} tx {}",
        row.tx_index
    );
    let account_assembly_started = Instant::now();
    let invalid_top_program_index = worker
        .top_program_indexes
        .first_out_of_bounds(expected_account_count);
    let invalid_top_account_index = worker
        .top_account_indexes
        .first_out_of_bounds(expected_account_count);
    finalize_streamed_account_uses(
        worker,
        &decoded,
        actual_loaded_writable,
        actual_loaded_readonly,
        invalid_top_program_index,
        invalid_top_account_index,
        invalid_cpi_account_index,
        account_coverage,
        config.registry_entries,
        slot,
        row.tx_index,
    )?;
    timing.account_role_assembly = timing
        .account_role_assembly
        .saturating_add(account_assembly_started.elapsed());
    let transaction = ProjectedTransaction {
        source_flags: row.flags,
        outcome,
        account_coverage,
        cpi_coverage,
        accounts: std::mem::take(&mut worker.accounts),
    };
    validate_projected_transaction(&transaction)?;
    if let Some(pending) = pending_split {
        worker.record_pending_effects(row.tx_index, row.flags, pending)?;
    }
    Ok(transaction)
}

#[allow(clippy::too_many_arguments)]
fn finalize_streamed_account_uses(
    worker: &mut ProjectionWorker,
    message: &decode::StreamedMessageShape,
    loaded_writable_count: usize,
    loaded_readonly_count: usize,
    invalid_top_program_index: Option<u8>,
    invalid_top_account_index: Option<u8>,
    invalid_cpi_account_index: Option<u8>,
    account_coverage: AccountCoverage,
    registry_entries: u32,
    slot: u64,
    tx_index: u32,
) -> Result<()> {
    let static_count = message.static_account_count;
    let required = usize::from(message.num_required_signatures);
    let readonly_signed = usize::from(message.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(message.num_readonly_unsigned_accounts);
    ensure!(
        readonly_signed <= required,
        "readonly signed count exceeds required signatures at slot {slot} tx {tx_index}"
    );
    ensure!(
        required <= static_count,
        "required signatures exceed static keys at slot {slot} tx {tx_index}"
    );
    ensure!(
        readonly_unsigned <= static_count - required,
        "readonly unsigned count exceeds unsigned static keys at slot {slot} tx {tx_index}"
    );
    ensure!(
        loaded_writable_count == message.expected_loaded_writable
            || loaded_writable_count == 0 && message.expected_loaded_writable != 0,
        "loaded writable projection shape is invalid at slot {slot} tx {tx_index}"
    );
    ensure!(
        loaded_readonly_count == message.expected_loaded_readonly
            || loaded_readonly_count == 0 && message.expected_loaded_readonly != 0,
        "loaded readonly projection shape is invalid at slot {slot} tx {tx_index}"
    );
    ensure!(
        !worker.source_position_overflow,
        "streamed account positions exceed {MAX_MESSAGE_ACCOUNTS} at slot {slot} tx {tx_index}"
    );
    let expected_account_count = static_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("expected account count overflow")?;
    ensure!(
        expected_account_count <= MAX_MESSAGE_ACCOUNTS,
        "resolved account count exceeds {MAX_MESSAGE_ACCOUNTS} at slot {slot} tx {tx_index}"
    );
    let actual_account_count = static_count
        .checked_add(loaded_writable_count)
        .and_then(|count| count.checked_add(loaded_readonly_count))
        .context("available account count overflow")?;
    if account_coverage == AccountCoverage::Complete {
        ensure!(
            actual_account_count == expected_account_count,
            "complete projection has {actual_account_count} accounts, expected {expected_account_count} at slot {slot} tx {tx_index}"
        );
    }
    ensure!(
        worker.source_position_count == actual_account_count,
        "streamed projection has {} source positions, expected {actual_account_count} at slot {slot} tx {tx_index}",
        worker.source_position_count
    );

    let writable_signed_end = required - readonly_signed;
    let writable_unsigned_end = static_count - readonly_unsigned;
    for source_position in 0..static_count {
        if source_position < required {
            worker.apply_role(source_position, ROLE_SIGNER)?;
        }
        if source_position < writable_signed_end
            || source_position >= required && source_position < writable_unsigned_end
        {
            worker.apply_role(source_position, ROLE_WRITABLE)?;
        }
    }
    let loaded_writable_end = static_count
        .checked_add(loaded_writable_count)
        .context("loaded writable role range overflow")?;
    for source_position in static_count..loaded_writable_end {
        worker.apply_role(source_position, ROLE_WRITABLE)?;
    }

    if let Some(id) = worker.first_invalid_pubkey_id {
        anyhow::bail!(
            "pubkey id {id} is outside 1..={registry_entries} at slot {slot} tx {tx_index}"
        );
    }
    if let Some(index) = invalid_top_program_index {
        anyhow::bail!(
            "top-level program index {index} is outside {expected_account_count} resolved accounts at slot {slot} tx {tx_index}"
        );
    }
    if let Some(index) = invalid_top_account_index {
        anyhow::bail!(
            "top-level instruction account index {index} is outside {expected_account_count} resolved accounts at slot {slot} tx {tx_index}"
        );
    }
    ensure!(
        invalid_cpi_account_index.is_none(),
        "CPI instruction account index {} is outside {expected_account_count} resolved accounts at slot {slot} tx {tx_index}",
        invalid_cpi_account_index.unwrap_or_default()
    );
    ensure!(
        worker
            .accounts
            .iter()
            .all(|account| account.roles & !ROLE_MASK == 0),
        "account role mask overflow"
    );
    Ok(())
}

#[cfg(test)]
fn project_transaction(
    config: ProjectionConfig,
    slot: u64,
    row: ArchiveV2HotTxRow,
    message_region: &[u8],
    metadata_region: &[u8],
) -> Result<ProjectedTransaction> {
    let mut worker = ProjectionWorker::new(config)?;
    project_transaction_timed(
        &mut worker,
        slot,
        row,
        message_region,
        metadata_region,
        &mut ProjectionTiming::default(),
    )
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn build_account_uses_for_test(
    message: &decode::DecodedMessage,
    loaded_writable: &[CompactPubkey],
    loaded_readonly: &[CompactPubkey],
    top_program_indexes: &[u8],
    top_account_indexes: &[u8],
    cpi_program_indexes: &[u32],
    cpi_account_indexes: &[u8],
    account_coverage: AccountCoverage,
    registry_entries: u32,
    slot: u64,
    tx_index: u32,
) -> Result<Vec<AccountUse>> {
    let expected_account_count = message
        .account_keys
        .len()
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("expected account count overflow")?;
    ensure!(
        expected_account_count <= MAX_MESSAGE_ACCOUNTS,
        "resolved account count exceeds account cap"
    );
    let mut worker = ProjectionWorker::new(ProjectionConfig {
        message_schema: CompactV2MessageSchema::Current,
        metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
        registry_entries,
    })?;
    worker.reset_transaction();
    worker.reserve_account_scratch()?;
    for (source_position, &key) in message.account_keys.iter().enumerate() {
        worker.ingest_account(source_position, key, registry_entries)?;
    }
    for &index in top_program_indexes {
        worker.apply_role(usize::from(index), ROLE_TOP_LEVEL_PROGRAM)?;
        worker.top_program_indexes.observe(index);
    }
    worker.top_account_indexes.observe_all(top_account_indexes);
    for &index in cpi_program_indexes {
        let index = usize::try_from(index).context("CPI program index exceeds usize")?;
        ensure!(
            index < expected_account_count,
            "CPI program index {index} is outside {expected_account_count} resolved accounts at slot {slot} tx {tx_index}"
        );
        worker.apply_role(index, ROLE_CPI_PROGRAM)?;
    }
    for (offset, &key) in loaded_writable.iter().enumerate() {
        worker.ingest_account(
            message
                .account_keys
                .len()
                .checked_add(offset)
                .context("loaded writable position overflow")?,
            key,
            registry_entries,
        )?;
    }
    for (offset, &key) in loaded_readonly.iter().enumerate() {
        worker.ingest_account(
            message
                .account_keys
                .len()
                .checked_add(message.expected_loaded_writable)
                .and_then(|position| position.checked_add(offset))
                .context("loaded readonly position overflow")?,
            key,
            registry_entries,
        )?;
    }
    let invalid_cpi_account_index = cpi_account_indexes
        .iter()
        .copied()
        .find(|index| usize::from(*index) >= expected_account_count);
    let shape = decode::StreamedMessageShape {
        static_account_count: message.account_keys.len(),
        is_v0: message.is_v0,
        num_required_signatures: message.num_required_signatures,
        num_readonly_signed_accounts: message.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: message.num_readonly_unsigned_accounts,
        instruction_count: message.instruction_count,
        expected_loaded_writable: message.expected_loaded_writable,
        expected_loaded_readonly: message.expected_loaded_readonly,
    };
    let invalid_top_program_index = worker
        .top_program_indexes
        .first_out_of_bounds(expected_account_count);
    let invalid_top_account_index = worker
        .top_account_indexes
        .first_out_of_bounds(expected_account_count);
    finalize_streamed_account_uses(
        &mut worker,
        &shape,
        loaded_writable.len(),
        loaded_readonly.len(),
        invalid_top_program_index,
        invalid_top_account_index,
        invalid_cpi_account_index,
        account_coverage,
        registry_entries,
        slot,
        tx_index,
    )?;
    Ok(std::mem::take(&mut worker.accounts))
}

/// Frozen bd22804 account assembler used only as a byte-parity oracle.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn build_account_uses_reference(
    message: &decode::DecodedMessage,
    loaded_writable: &[CompactPubkey],
    loaded_readonly: &[CompactPubkey],
    top_program_indexes: &[u8],
    top_account_indexes: &[u8],
    cpi_program_indexes: &[u32],
    cpi_account_indexes: &[u8],
    account_coverage: AccountCoverage,
    registry_entries: u32,
    slot: u64,
    tx_index: u32,
) -> Result<Vec<AccountUse>> {
    let static_count = message.account_keys.len();
    let required = usize::from(message.num_required_signatures);
    let readonly_signed = usize::from(message.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(message.num_readonly_unsigned_accounts);
    ensure!(
        readonly_signed <= required,
        "readonly signed count exceeds required signatures at slot {slot} tx {tx_index}"
    );
    ensure!(
        required <= static_count,
        "required signatures exceed static keys at slot {slot} tx {tx_index}"
    );
    ensure!(
        readonly_unsigned <= static_count - required,
        "readonly unsigned count exceeds unsigned static keys at slot {slot} tx {tx_index}"
    );
    ensure!(
        loaded_writable.len() == message.expected_loaded_writable
            || loaded_writable.is_empty() && message.expected_loaded_writable != 0,
        "loaded writable projection shape is invalid at slot {slot} tx {tx_index}"
    );
    ensure!(
        loaded_readonly.len() == message.expected_loaded_readonly
            || loaded_readonly.is_empty() && message.expected_loaded_readonly != 0,
        "loaded readonly projection shape is invalid at slot {slot} tx {tx_index}"
    );

    let capacity = static_count
        .checked_add(loaded_writable.len())
        .and_then(|count| count.checked_add(loaded_readonly.len()))
        .context("available account count overflow")?;
    let mut accounts = Vec::with_capacity(capacity);
    let mut first_position = HashMap::with_capacity(capacity);
    let mut positions = Vec::with_capacity(capacity);
    for key in message
        .account_keys
        .iter()
        .chain(loaded_writable)
        .chain(loaded_readonly)
        .copied()
    {
        validate_pubkey_ref(key, registry_entries, slot, tx_index)?;
        let account_position = if let Some(position) = first_position.get(&key) {
            *position
        } else {
            let position = accounts.len();
            accounts.push(AccountUse { key, roles: 0 });
            first_position.insert(key, position);
            position
        };
        positions.push(account_position);
    }
    let expected_account_count = static_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("expected account count overflow")?;
    ensure!(
        expected_account_count <= MAX_MESSAGE_ACCOUNTS,
        "resolved account count exceeds {MAX_MESSAGE_ACCOUNTS} at slot {slot} tx {tx_index}"
    );
    if account_coverage == AccountCoverage::Complete {
        ensure!(
            positions.len() == expected_account_count,
            "complete projection has {} accounts, expected {expected_account_count} at slot {slot} tx {tx_index}",
            positions.len()
        );
    }

    let writable_signed_end = required - readonly_signed;
    let writable_unsigned_end = static_count - readonly_unsigned;
    for (source_position, &account_position) in positions.iter().take(static_count).enumerate() {
        if source_position < required {
            accounts[account_position].roles |= ROLE_SIGNER;
        }
        if source_position < writable_signed_end
            || source_position >= required && source_position < writable_unsigned_end
        {
            accounts[account_position].roles |= ROLE_WRITABLE;
        }
    }
    for &account_position in positions
        .iter()
        .skip(static_count)
        .take(loaded_writable.len())
    {
        accounts[account_position].roles |= ROLE_WRITABLE;
    }
    for &index in top_program_indexes {
        set_index_role_reference(
            &mut accounts,
            &positions,
            expected_account_count,
            usize::from(index),
            ROLE_TOP_LEVEL_PROGRAM,
            "top-level program",
            slot,
            tx_index,
        )?;
    }
    validate_account_indexes_reference(
        top_account_indexes,
        expected_account_count,
        "top-level instruction account",
        slot,
        tx_index,
    )?;
    for &index in cpi_program_indexes {
        set_index_role_reference(
            &mut accounts,
            &positions,
            expected_account_count,
            usize::try_from(index).context("CPI program index exceeds usize")?,
            ROLE_CPI_PROGRAM,
            "CPI program",
            slot,
            tx_index,
        )?;
    }
    validate_account_indexes_reference(
        cpi_account_indexes,
        expected_account_count,
        "CPI instruction account",
        slot,
        tx_index,
    )?;
    Ok(accounts)
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn set_index_role_reference(
    accounts: &mut [AccountUse],
    positions: &[usize],
    expected_account_count: usize,
    source_index: usize,
    role: u8,
    label: &str,
    slot: u64,
    tx_index: u32,
) -> Result<()> {
    ensure!(
        source_index < expected_account_count,
        "{label} index {source_index} is outside {expected_account_count} resolved accounts at slot {slot} tx {tx_index}"
    );
    let Some(&account_position) = positions.get(source_index) else {
        return Ok(());
    };
    accounts[account_position].roles |= role;
    Ok(())
}

#[cfg(test)]
fn validate_account_indexes_reference(
    indexes: &[u8],
    account_count: usize,
    label: &str,
    slot: u64,
    tx_index: u32,
) -> Result<()> {
    for &index in indexes {
        ensure!(
            usize::from(index) < account_count,
            "{label} index {index} is outside {account_count} resolved accounts at slot {slot} tx {tx_index}"
        );
    }
    Ok(())
}

#[cfg(test)]
fn validate_pubkey_ref(
    key: CompactPubkey,
    registry_entries: u32,
    slot: u64,
    tx_index: u32,
) -> Result<()> {
    if let CompactPubkey::Id(id) = key {
        ensure!(
            id != 0 && id <= registry_entries,
            "pubkey id {id} is outside 1..={registry_entries} at slot {slot} tx {tx_index}"
        );
    }
    Ok(())
}

fn add_transaction_stats(
    stats: &mut ProjectionStats,
    transaction: &ProjectedTransaction,
) -> Result<()> {
    stats.transactions = stats
        .transactions
        .checked_add(1)
        .context("transaction count overflow")?;
    if transaction.source_flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        stats.source_raw_transaction_flags += 1;
    }
    if transaction.source_flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        stats.source_raw_metadata_flags += 1;
    }
    if transaction.source_flags
        & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
        != 0
    {
        stats.source_fallback_rows = stats
            .source_fallback_rows
            .checked_add(1)
            .context("source fallback row count overflow")?;
    }
    stats.account_refs = stats
        .account_refs
        .checked_add(
            u64::try_from(transaction.accounts.len()).context("account count exceeds u64")?,
        )
        .context("account reference count overflow")?;
    for account in &transaction.accounts {
        match account.key {
            CompactPubkey::Id(_) => stats.id_refs += 1,
            CompactPubkey::Raw(_) => stats.raw_refs += 1,
        }
    }
    match transaction.outcome {
        Outcome::Unknown => stats.unknown += 1,
        Outcome::Success => stats.success += 1,
        Outcome::Failed => stats.failed += 1,
    }
    match transaction.account_coverage {
        AccountCoverage::Complete => stats.account_complete += 1,
        AccountCoverage::MissingLoadedMetadata => {
            stats.account_missing_loaded_metadata += 1;
        }
        AccountCoverage::RawTransactionFallback => {
            stats.account_raw_transaction_fallback += 1;
        }
        AccountCoverage::RawMetadataLoadedFallback => {
            stats.account_raw_metadata_loaded_fallback += 1;
        }
    }
    match transaction.cpi_coverage {
        CpiCoverage::Recorded => stats.cpi_recorded += 1,
        CpiCoverage::NotRecorded => stats.cpi_not_recorded += 1,
        CpiCoverage::MissingMetadata => stats.cpi_missing_metadata += 1,
        CpiCoverage::RawTransactionFallback => stats.cpi_raw_transaction_fallback += 1,
        CpiCoverage::RawMetadataFallback => stats.cpi_raw_metadata_fallback += 1,
    }
    if transaction.account_coverage == AccountCoverage::Complete
        && transaction.cpi_coverage == CpiCoverage::Recorded
    {
        stats.fully_covered += 1;
    }
    Ok(())
}

fn append_page_header(
    bytes: &mut Vec<u8>,
    row: ArchiveV2HotBlockIndexRow,
    coverage_flags: u32,
) -> Result<()> {
    ensure!(
        bytes.is_empty(),
        "account page header destination is not empty"
    );
    bytes.extend_from_slice(&PAGE_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&row.block_id.to_le_bytes());
    bytes.extend_from_slice(&row.tx_count.to_le_bytes());
    bytes.extend_from_slice(&coverage_flags.to_le_bytes());
    debug_assert_eq!(bytes.len(), PAGE_HEADER_LEN);
    Ok(())
}

fn append_encoded_transaction(
    bytes: &mut Vec<u8>,
    transaction: &ProjectedTransaction,
) -> Result<()> {
    let account_bytes = transaction
        .accounts
        .iter()
        .try_fold(0_usize, |total, account| {
            total
                .checked_add(match account.key {
                    CompactPubkey::Id(_) => 8,
                    CompactPubkey::Raw(_) => 36,
                })
                .context("encoded account-reference length overflow")
        })?;
    let transaction_bytes = 12_usize
        .checked_add(account_bytes)
        .context("encoded transaction length overflow")?;
    bytes
        .try_reserve(transaction_bytes)
        .context("reserve encoded transaction")?;
    bytes.push(transaction.outcome as u8);
    bytes.push(transaction.account_coverage as u8);
    bytes.push(transaction.cpi_coverage as u8);
    bytes.push(0);
    bytes.extend_from_slice(&transaction.source_flags.to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(transaction.accounts.len())
            .context("transaction account count exceeds u32")?
            .to_le_bytes(),
    );
    for account in &transaction.accounts {
        ensure!(
            account.roles & !ROLE_MASK == 0,
            "account has unknown role bits {:#x}",
            account.roles & !ROLE_MASK
        );
        match account.key {
            CompactPubkey::Id(id) => {
                bytes.push(0);
                bytes.push(account.roles);
                bytes.extend_from_slice(&0_u16.to_le_bytes());
                bytes.extend_from_slice(&id.to_le_bytes());
            }
            CompactPubkey::Raw(raw) => {
                bytes.push(1);
                bytes.push(account.roles);
                bytes.extend_from_slice(&0_u16.to_le_bytes());
                bytes.extend_from_slice(&raw);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
fn encode_page(
    row: ArchiveV2HotBlockIndexRow,
    transactions: &[ProjectedTransaction],
) -> Result<Vec<u8>> {
    ensure!(
        transactions.len() == row.tx_count as usize,
        "page transaction count does not match source row"
    );
    let coverage_flags = transactions
        .iter()
        .fold(0_u32, |flags, tx| flags | transaction_coverage_bits(tx));
    let minimum_page_len = transactions
        .len()
        .checked_mul(12)
        .and_then(|length| length.checked_add(PAGE_HEADER_LEN))
        .context("minimum account page length overflow")?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(minimum_page_len)
        .context("reserve minimum account page")?;
    append_page_header(&mut bytes, row, coverage_flags)?;
    for transaction in transactions {
        append_encoded_transaction(&mut bytes, transaction)?;
    }
    Ok(bytes)
}

#[cfg(test)]
fn decode_page_exact(
    bytes: &[u8],
    registry_entries: u32,
    expected_transaction_count: u32,
) -> Result<(u32, Vec<ProjectedTransaction>)> {
    let mut cursor = bytes;
    ensure!(
        take(&mut cursor, 8)? == PAGE_MAGIC,
        "bad account page magic"
    );
    ensure!(
        read_u16(&mut cursor)? == FORMAT_VERSION,
        "bad account page version"
    );
    ensure!(
        read_u16(&mut cursor)? == 0,
        "non-zero account page reserved bytes"
    );
    let block_id = read_u32(&mut cursor)?;
    let transaction_count = read_u32(&mut cursor)?;
    ensure!(
        transaction_count == expected_transaction_count,
        "account page has {transaction_count} transactions, expected {expected_transaction_count}"
    );
    let transaction_count = transaction_count as usize;
    let declared_coverage = read_u32(&mut cursor)?;
    let mut transactions = Vec::with_capacity(transaction_count);
    for _ in 0..transaction_count {
        let outcome = match read_u8(&mut cursor)? {
            0 => Outcome::Unknown,
            1 => Outcome::Success,
            2 => Outcome::Failed,
            other => bail!("unknown outcome {other}"),
        };
        let account_coverage = match read_u8(&mut cursor)? {
            0 => AccountCoverage::Complete,
            1 => AccountCoverage::MissingLoadedMetadata,
            2 => AccountCoverage::RawTransactionFallback,
            3 => AccountCoverage::RawMetadataLoadedFallback,
            other => bail!("unknown account coverage {other}"),
        };
        let cpi_coverage = match read_u8(&mut cursor)? {
            0 => CpiCoverage::Recorded,
            1 => CpiCoverage::NotRecorded,
            2 => CpiCoverage::MissingMetadata,
            3 => CpiCoverage::RawTransactionFallback,
            4 => CpiCoverage::RawMetadataFallback,
            other => bail!("unknown CPI coverage {other}"),
        };
        ensure!(
            read_u8(&mut cursor)? == 0,
            "non-zero transaction reserved byte"
        );
        let source_flags = read_u32(&mut cursor)?;
        ensure!(
            source_flags & !SOURCE_TX_FLAG_MASK == 0,
            "account page transaction has unknown source flag bits"
        );
        let account_count = read_u32(&mut cursor)? as usize;
        ensure!(
            account_count <= MAX_MESSAGE_ACCOUNTS,
            "account page transaction exceeds account cap"
        );
        let mut accounts = Vec::with_capacity(account_count);
        let mut seen = std::collections::HashSet::with_capacity(account_count);
        for _ in 0..account_count {
            let kind = read_u8(&mut cursor)?;
            let roles = read_u8(&mut cursor)?;
            ensure!(roles & !ROLE_MASK == 0, "unknown account role bits");
            ensure!(
                read_u16(&mut cursor)? == 0,
                "non-zero account reserved bytes"
            );
            let key = match kind {
                0 => {
                    let id = read_u32(&mut cursor)?;
                    ensure!(
                        id != 0 && id <= registry_entries,
                        "account page pubkey id {id} is outside 1..={registry_entries}"
                    );
                    CompactPubkey::Id(id)
                }
                1 => {
                    let raw: [u8; 32] = take(&mut cursor, 32)?
                        .try_into()
                        .expect("take returned exactly 32 bytes");
                    CompactPubkey::Raw(raw)
                }
                other => bail!("unknown account key kind {other}"),
            };
            ensure!(
                seen.insert(key),
                "account page contains a duplicate account key"
            );
            accounts.push(AccountUse { key, roles });
        }
        let transaction = ProjectedTransaction {
            source_flags,
            outcome,
            account_coverage,
            cpi_coverage,
            accounts,
        };
        validate_projected_transaction(&transaction)?;
        transactions.push(transaction);
    }
    ensure!(cursor.is_empty(), "account page has trailing bytes");
    let actual_coverage = transactions
        .iter()
        .fold(0_u32, |flags, tx| flags | transaction_coverage_bits(tx));
    ensure!(
        declared_coverage == actual_coverage,
        "account page coverage flags disagree with transactions"
    );
    Ok((block_id, transactions))
}

fn validate_projected_transaction(transaction: &ProjectedTransaction) -> Result<()> {
    let raw_transaction = transaction.source_flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0;
    let raw_metadata = transaction.source_flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0;
    let has_metadata = transaction.source_flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
    let has_inner = transaction.source_flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0;
    let has_error = transaction.source_flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;
    if raw_transaction {
        ensure!(
            transaction.account_coverage == AccountCoverage::RawTransactionFallback
                && transaction.cpi_coverage == CpiCoverage::RawTransactionFallback
                && transaction.accounts.is_empty(),
            "raw-transaction projection state is inconsistent"
        );
        if raw_metadata {
            ensure!(
                has_metadata && transaction.outcome == Outcome::Unknown,
                "raw-transaction raw-metadata outcome is inconsistent"
            );
        } else if has_metadata {
            ensure!(
                transaction.outcome
                    == if has_error {
                        Outcome::Failed
                    } else {
                        Outcome::Success
                    },
                "raw-transaction decoded-metadata outcome is inconsistent"
            );
        } else {
            ensure!(
                transaction.outcome == Outcome::Unknown,
                "raw-transaction missing-metadata outcome is inconsistent"
            );
        }
        return Ok(());
    }

    ensure!(
        transaction.account_coverage != AccountCoverage::RawTransactionFallback
            && transaction.cpi_coverage != CpiCoverage::RawTransactionFallback,
        "non-raw transaction uses raw-transaction coverage"
    );
    if raw_metadata {
        ensure!(
            has_metadata
                && transaction.outcome == Outcome::Unknown
                && transaction.cpi_coverage == CpiCoverage::RawMetadataFallback,
            "raw-metadata projection state is inconsistent"
        );
        ensure!(
            matches!(
                transaction.account_coverage,
                AccountCoverage::Complete | AccountCoverage::RawMetadataLoadedFallback
            ),
            "raw-metadata account coverage is inconsistent"
        );
        return Ok(());
    }

    ensure!(
        transaction.account_coverage != AccountCoverage::RawMetadataLoadedFallback
            && transaction.cpi_coverage != CpiCoverage::RawMetadataFallback,
        "non-raw metadata uses raw-metadata coverage"
    );
    if !has_metadata {
        ensure!(
            transaction.outcome == Outcome::Unknown
                && transaction.cpi_coverage == CpiCoverage::MissingMetadata,
            "missing-metadata projection state is inconsistent"
        );
        ensure!(
            matches!(
                transaction.account_coverage,
                AccountCoverage::Complete | AccountCoverage::MissingLoadedMetadata
            ),
            "missing-metadata account coverage is inconsistent"
        );
        return Ok(());
    }

    ensure!(
        transaction.account_coverage == AccountCoverage::Complete,
        "decoded metadata must have complete account coverage"
    );
    ensure!(
        transaction.outcome
            == if has_error {
                Outcome::Failed
            } else {
                Outcome::Success
            },
        "decoded metadata outcome does not match source flags"
    );
    ensure!(
        transaction.cpi_coverage
            == if has_inner {
                CpiCoverage::Recorded
            } else {
                CpiCoverage::NotRecorded
            },
        "decoded metadata CPI coverage does not match source flags"
    );
    Ok(())
}

fn decode_index_exact(
    bytes: &[u8],
    expected_pages_bytes: u64,
    expected_registry_entries: u32,
    expected_message_schema: CompactV2MessageSchema,
    expected_metadata_schema: CompactV2MetadataSchema,
) -> Result<Vec<AccountIndexRow>> {
    let mut cursor = bytes;
    ensure!(
        take(&mut cursor, 8)? == INDEX_MAGIC,
        "bad account index magic"
    );
    ensure!(
        read_u16(&mut cursor)? == FORMAT_VERSION,
        "bad account index version"
    );
    ensure!(
        read_u16(&mut cursor)? == 0,
        "non-zero account index reserved bytes"
    );
    let row_count =
        usize::try_from(read_u64(&mut cursor)?).context("account index row count exceeds usize")?;
    let pages_bytes = read_u64(&mut cursor)?;
    ensure!(
        pages_bytes == expected_pages_bytes,
        "account index declares {pages_bytes} page bytes, expected {expected_pages_bytes}"
    );
    let registry_entries = read_u32(&mut cursor)?;
    ensure!(
        registry_entries == expected_registry_entries,
        "account index registry count {registry_entries} does not match {expected_registry_entries}"
    );
    ensure!(
        read_u8(&mut cursor)? == message_schema_code(expected_message_schema),
        "account index message schema does not match"
    );
    ensure!(
        read_u8(&mut cursor)? == metadata_schema_code(expected_metadata_schema),
        "account index metadata schema does not match"
    );
    ensure!(
        take(&mut cursor, 6)?.iter().all(|byte| *byte == 0),
        "non-zero account index header reserved bytes"
    );
    let expected_row_bytes = row_count
        .checked_mul(INDEX_ROW_LEN)
        .context("account index row byte count overflow")?;
    ensure!(
        cursor.len() == expected_row_bytes,
        "account index row geometry is not exact"
    );

    let mut rows = Vec::with_capacity(row_count);
    let mut expected_offset = 0_u64;
    for position in 0..row_count {
        let row = AccountIndexRow {
            block_id: read_u32(&mut cursor)?,
            slot: read_u64(&mut cursor)?,
            page_offset: read_u64(&mut cursor)?,
            stored_len: read_u32(&mut cursor)?,
            decoded_len: read_u32(&mut cursor)?,
            tx_count: read_u32(&mut cursor)?,
            account_ref_count: read_u32(&mut cursor)?,
            coverage_flags: read_u32(&mut cursor)?,
            flags: read_u32(&mut cursor)?,
        };
        ensure!(
            row.block_id as usize == position,
            "account index block id {} is not row {position}",
            row.block_id
        );
        ensure!(
            row.page_offset == expected_offset,
            "account index page offsets are not contiguous at row {position}"
        );
        ensure!(
            row.stored_len != 0 && row.decoded_len != 0,
            "account index row {position} has an empty page"
        );
        ensure!(
            row.coverage_flags & !COVERAGE_MASK == 0,
            "account index row {position} has unknown coverage flags"
        );
        ensure!(
            row.flags & !INDEX_ROW_FLAG_ZSTD == 0,
            "account index row {position} has unknown flags"
        );
        expected_offset = expected_offset
            .checked_add(u64::from(row.stored_len))
            .context("account index page offset overflow")?;
        rows.push(row);
    }
    ensure!(cursor.is_empty(), "account index has trailing bytes");
    ensure!(
        expected_offset == expected_pages_bytes,
        "account index covers {expected_offset} page bytes, expected {expected_pages_bytes}"
    );
    Ok(rows)
}

#[cfg(test)]
fn validate_saved_output(
    pages_path: &Path,
    index_path: &Path,
    source_rows: &[ArchiveV2HotBlockIndexRow],
    expected_index_rows: &[AccountIndexRow],
    registry_entries: u32,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
) -> Result<ProjectionStats> {
    let pages_bytes = fs::metadata(pages_path)
        .with_context(|| format!("stat {}", pages_path.display()))?
        .len();
    let expected_index_bytes = u64::try_from(INDEX_HEADER_LEN)
        .expect("fixed index header fits u64")
        .checked_add(
            u64::try_from(source_rows.len())
                .context("source row count exceeds u64")?
                .checked_mul(u64::try_from(INDEX_ROW_LEN).expect("fixed index row fits u64"))
                .context("expected account index bytes overflow")?,
        )
        .context("expected account index length overflow")?;
    let actual_index_bytes = fs::metadata(index_path)
        .with_context(|| format!("stat {}", index_path.display()))?
        .len();
    ensure!(
        actual_index_bytes == expected_index_bytes,
        "saved account index has {actual_index_bytes} bytes, expected {expected_index_bytes}"
    );
    let index_bytes =
        fs::read(index_path).with_context(|| format!("read {}", index_path.display()))?;
    let index_rows = decode_index_exact(
        &index_bytes,
        pages_bytes,
        registry_entries,
        message_schema,
        metadata_schema,
    )?;
    ensure!(
        index_rows.len() == source_rows.len(),
        "account index has {} rows, source selection has {}",
        index_rows.len(),
        source_rows.len()
    );
    ensure!(
        index_rows == expected_index_rows,
        "saved account index rows do not match the generated rows"
    );

    let pages_file =
        File::open(pages_path).with_context(|| format!("open {}", pages_path.display()))?;
    let mut pages = BufReader::with_capacity(8 << 20, pages_file);
    let mut totals = ProjectionStats::default();
    for (position, (index_row, source_row)) in index_rows.iter().zip(source_rows).enumerate() {
        ensure!(
            index_row.block_id == source_row.block_id,
            "account index row {position} block id does not match source"
        );
        ensure!(
            index_row.slot == source_row.slot,
            "account index row {position} slot does not match source"
        );
        ensure!(
            index_row.tx_count == source_row.tx_count,
            "account index row {position} transaction count does not match source"
        );

        let (minimum_decoded_len, maximum_decoded_len) =
            page_decoded_length_bounds(source_row.tx_count)?;
        let decoded_len = u64::from(index_row.decoded_len);
        ensure!(
            decoded_len >= minimum_decoded_len && decoded_len <= maximum_decoded_len,
            "account page {position} decoded length {decoded_len} is outside {minimum_decoded_len}..={maximum_decoded_len}"
        );
        ensure!(
            index_row.stored_len <= index_row.decoded_len,
            "account page {position} stored length exceeds decoded length"
        );
        let stored_len = usize::try_from(index_row.stored_len)
            .context("account page stored length exceeds usize")?;
        let mut stored = vec![0_u8; stored_len];
        pages
            .read_exact(&mut stored)
            .with_context(|| format!("read account page {position}"))?;
        let decoded = if index_row.flags & INDEX_ROW_FLAG_ZSTD != 0 {
            let frame_len =
                zstd::zstd_safe::find_frame_compressed_size(&stored).map_err(|code| {
                    anyhow::anyhow!(
                        "account page {position} has an invalid zstd frame: {}",
                        zstd::zstd_safe::get_error_name(code)
                    )
                })?;
            ensure!(
                frame_len == stored.len(),
                "account page {position} zstd frame has trailing data"
            );
            let decoded = zstd::bulk::decompress(&stored, index_row.decoded_len as usize)
                .with_context(|| format!("decompress account page {position}"))?;
            ensure!(
                decoded.len() == index_row.decoded_len as usize,
                "account page {position} decoded length does not match index"
            );
            decoded
        } else {
            ensure!(
                index_row.stored_len == index_row.decoded_len,
                "raw account page {position} stored and decoded lengths differ"
            );
            stored
        };
        let (block_id, transactions) =
            decode_page_exact(&decoded, registry_entries, source_row.tx_count)
                .with_context(|| format!("validate account page {position}"))?;
        ensure!(
            block_id == source_row.block_id,
            "account page {position} block id does not match source"
        );
        ensure!(
            transactions.len() == source_row.tx_count as usize,
            "account page {position} transaction count does not match source"
        );
        let mut block_stats = ProjectionStats::default();
        for transaction in &transactions {
            add_transaction_stats(&mut block_stats, transaction)?;
        }
        ensure!(
            block_stats.account_refs == u64::from(index_row.account_ref_count),
            "account page {position} reference count does not match index"
        );
        ensure!(
            block_coverage_flags(&block_stats) == index_row.coverage_flags,
            "account page {position} coverage flags do not match index"
        );
        totals.merge(block_stats);
    }
    let mut trailing = [0_u8; 1];
    ensure!(
        pages
            .read(&mut trailing)
            .context("check account pages EOF")?
            == 0,
        "account pages file has trailing data"
    );
    Ok(totals)
}

fn page_decoded_length_bounds(transaction_count: u32) -> Result<(u64, u64)> {
    let transaction_count = u64::from(transaction_count);
    let transaction_headers = transaction_count
        .checked_mul(12)
        .context("account page transaction-header bytes overflow")?;
    let minimum = u64::try_from(PAGE_HEADER_LEN)
        .expect("fixed page header fits u64")
        .checked_add(transaction_headers)
        .context("minimum account page length overflow")?;
    let maximum_account_bytes = transaction_count
        .checked_mul(u64::try_from(MAX_MESSAGE_ACCOUNTS).expect("account cap fits u64"))
        .and_then(|count| count.checked_mul(36))
        .context("maximum account page bytes overflow")?;
    let maximum = minimum
        .checked_add(maximum_account_bytes)
        .context("maximum account page length overflow")?;
    Ok((minimum, maximum))
}

fn write_index(
    path: &Path,
    rows: &[AccountIndexRow],
    pages_bytes: u64,
    registry_entries: u32,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(8 << 20, file);
    writer.write_all(&INDEX_MAGIC)?;
    writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
    writer.write_all(&0_u16.to_le_bytes())?;
    writer.write_all(
        &u64::try_from(rows.len())
            .context("account index row count exceeds u64")?
            .to_le_bytes(),
    )?;
    writer.write_all(&pages_bytes.to_le_bytes())?;
    writer.write_all(&registry_entries.to_le_bytes())?;
    writer.write_all(&[message_schema_code(message_schema)])?;
    writer.write_all(&[metadata_schema_code(metadata_schema)])?;
    writer.write_all(&[0_u8; 6])?;
    debug_assert_eq!(INDEX_HEADER_LEN, 40);

    let mut expected_offset = 0_u64;
    for (position, row) in rows.iter().enumerate() {
        ensure!(
            row.block_id as usize == position,
            "account index block id {} is not row {position}",
            row.block_id
        );
        ensure!(
            row.page_offset == expected_offset,
            "account page offsets are not contiguous at block {}",
            row.block_id
        );
        ensure!(
            row.stored_len != 0 && row.decoded_len != 0,
            "account page {} is empty",
            row.block_id
        );
        ensure!(
            row.flags & !INDEX_ROW_FLAG_ZSTD == 0,
            "account page {} has unknown flags",
            row.block_id
        );
        writer.write_all(&row.block_id.to_le_bytes())?;
        writer.write_all(&row.slot.to_le_bytes())?;
        writer.write_all(&row.page_offset.to_le_bytes())?;
        writer.write_all(&row.stored_len.to_le_bytes())?;
        writer.write_all(&row.decoded_len.to_le_bytes())?;
        writer.write_all(&row.tx_count.to_le_bytes())?;
        writer.write_all(&row.account_ref_count.to_le_bytes())?;
        writer.write_all(&row.coverage_flags.to_le_bytes())?;
        writer.write_all(&row.flags.to_le_bytes())?;
        expected_offset = expected_offset
            .checked_add(u64::from(row.stored_len))
            .context("account page offset overflow")?;
    }
    ensure!(
        expected_offset == pages_bytes,
        "account index covers {expected_offset} page bytes, expected {pages_bytes}"
    );
    writer.flush().context("flush account index")?;
    let file = writer.into_inner().context("finish account index writer")?;
    file.sync_all().context("sync account index")?;
    Ok(())
}

fn block_coverage_flags(stats: &ProjectionStats) -> u32 {
    let mut flags = 0_u32;
    if stats.account_complete != 0 {
        flags |= AccountCoverage::Complete.bit();
    }
    if stats.account_missing_loaded_metadata != 0 {
        flags |= AccountCoverage::MissingLoadedMetadata.bit();
    }
    if stats.account_raw_transaction_fallback != 0 {
        flags |= AccountCoverage::RawTransactionFallback.bit();
    }
    if stats.account_raw_metadata_loaded_fallback != 0 {
        flags |= AccountCoverage::RawMetadataLoadedFallback.bit();
    }
    if stats.cpi_recorded != 0 {
        flags |= CpiCoverage::Recorded.bit();
    }
    if stats.cpi_not_recorded != 0 {
        flags |= CpiCoverage::NotRecorded.bit();
    }
    if stats.cpi_missing_metadata != 0 {
        flags |= CpiCoverage::MissingMetadata.bit();
    }
    if stats.cpi_raw_transaction_fallback != 0 {
        flags |= CpiCoverage::RawTransactionFallback.bit();
    }
    if stats.cpi_raw_metadata_fallback != 0 {
        flags |= CpiCoverage::RawMetadataFallback.bit();
    }
    flags
}

fn transaction_coverage_bits(transaction: &ProjectedTransaction) -> u32 {
    transaction.account_coverage.bit() | transaction.cpi_coverage.bit()
}

fn checked_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    label: &str,
    slot: u64,
    tx_index: u32,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let end = start
        .checked_add(len as usize)
        .with_context(|| format!("{label} range overflows at slot {slot} tx {tx_index}"))?;
    bytes.get(start..end).with_context(|| {
        format!(
            "{label} range {offset}+{len} is outside {} bytes at slot {slot} tx {tx_index}",
            bytes.len()
        )
    })
}

fn checked_row_byte_sum(
    rows: &[ArchiveV2HotBlockIndexRow],
    field: impl Fn(ArchiveV2HotBlockIndexRow) -> u32,
) -> Result<u64> {
    rows.iter().try_fold(0_u64, |total, row| {
        total
            .checked_add(u64::from(field(*row)))
            .context("source byte total overflow")
    })
}

fn staging_path(output: &Path) -> Result<PathBuf> {
    let name = output
        .file_name()
        .and_then(|name| name.to_str())
        .context("output needs a UTF-8 final path component")?;
    Ok(output.with_file_name(format!(".{name}.account-projection-staging")))
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write {}", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("finish {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync {}", path.display()))?;
    Ok(())
}

fn path_entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

fn publish_staging_directory(staging: &Path, output: &Path) -> Result<()> {
    renameat_with(CWD, staging, CWD, output, RenameFlags::NOREPLACE)
        .map_err(std::io::Error::from)
        .with_context(|| {
            format!(
                "atomically rename staging {} to {} without replacement",
                staging.display(),
                output.display()
            )
        })
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn usable_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn message_schema_code(schema: CompactV2MessageSchema) -> u8 {
    match schema {
        CompactV2MessageSchema::Current => 0,
        CompactV2MessageSchema::May24PreUnknownFallbacks => 1,
    }
}

fn metadata_schema_code(schema: CompactV2MetadataSchema) -> u8 {
    match schema {
        CompactV2MetadataSchema::CurrentTypedError => 0,
        CompactV2MetadataSchema::LegacyRawError => 1,
    }
}

fn message_schema_name(schema: CompactV2MessageSchema) -> &'static str {
    match schema {
        CompactV2MessageSchema::Current => "current",
        CompactV2MessageSchema::May24PreUnknownFallbacks => "may24-pre-unknown-fallbacks",
    }
}

fn metadata_schema_name(schema: CompactV2MetadataSchema) -> &'static str {
    match schema {
        CompactV2MetadataSchema::CurrentTypedError => "current-typed-error",
        CompactV2MetadataSchema::LegacyRawError => "legacy-raw-error",
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn split_benchmark_report(
    summary: SplitOutputSummary,
    account_projection_output_bytes: u64,
    metadata_account_and_effect_stream_time: Duration,
    split_copy_and_other_worker_residual: Duration,
) -> Result<SplitBenchmarkReport> {
    let planes = SplitPlane::ALL
        .into_iter()
        .map(|plane| {
            let stats = summary.stats.planes[plane.index()];
            SplitPlaneBenchmarkReport {
                plane: plane.name(),
                file: plane.file_name(),
                frames: stats.frames,
                records: stats.records,
                exact_source_field_bytes: stats.source_field_bytes,
                decoded_payload_bytes: stats.decoded_bytes,
                stored_frame_bytes: stats.stored_bytes,
                file_bytes: summary.plane_file_bytes[plane.index()],
            }
        })
        .collect();
    let combined_candidate_output_bytes = account_projection_output_bytes
        .checked_add(summary.output_bytes)
        .context("combined candidate output byte count overflow")?;
    Ok(SplitBenchmarkReport {
        canary_kind: SPLIT_CANARY_KIND,
        candidate_status: STATUS,
        format_status: "measurement-container-not-final-schema",
        index_file: SPLIT_INDEX_FILE,
        index_bytes: summary.index_bytes,
        split_output_bytes: summary.output_bytes,
        account_projection_output_bytes,
        combined_candidate_output_bytes,
        metadata_reconstructable: false,
        loaded_address_lanes_preserved: false,
        raw_transaction_decoded_metadata_validation: "structural-only-bounded-no-message-account-relations",
        raw_transaction_structural_account_cap: MAX_MESSAGE_ACCOUNTS,
        missing_metadata_transactions: summary.stats.missing_metadata,
        decoded_metadata_transactions: summary.stats.decoded_metadata,
        raw_metadata_transactions: summary.stats.raw_metadata,
        raw_transaction_flags: summary.stats.tx_raw_flags,
        worker_metadata_account_and_effect_stream_sum_ms: duration_millis(
            metadata_account_and_effect_stream_time,
        ),
        worker_split_compression_sum_ms: duration_millis(summary.compression_time),
        split_copy_and_other_worker_residual_sum_ms: duration_millis(
            split_copy_and_other_worker_residual,
        ),
        ordered_split_write_sum_ms: duration_millis(summary.ordered_write_time),
        split_finalize_ms: duration_millis(summary.finalize_time),
        max_worker_live_raw_length_plus_compress_bound_bytes: summary
            .stats
            .max_worker_aggregate_raw_bytes,
        max_worker_raw_and_compression_scratch_capacity_bytes: summary
            .stats
            .max_worker_aggregate_scratch_capacity,
        max_worker_retained_raw_and_compression_capacity_bytes: summary
            .stats
            .max_worker_retained_raw_capacity,
        retained_raw_and_compression_capacity_limit_bytes:
            MAX_RETAINED_SPLIT_RAW_CAPACITY_PER_WORKER,
        max_worker_retained_chunk_descriptor_capacity_bytes: summary
            .stats
            .max_worker_retained_chunk_capacity,
        retained_chunk_descriptor_capacity_limit_bytes:
            MAX_RETAINED_SPLIT_CHUNK_CAPACITY_PER_WORKER,
        max_worker_total_scratch_capacity_bytes: summary.stats.max_worker_total_scratch_capacity,
        total_scratch_capacity_limit_bytes: MAX_SPLIT_TOTAL_SCRATCH_CAPACITY_PER_WORKER,
        max_worker_retained_total_scratch_capacity_bytes: summary
            .stats
            .max_worker_retained_total_scratch_capacity,
        retained_total_scratch_capacity_limit_bytes:
            MAX_RETAINED_SPLIT_TOTAL_SCRATCH_CAPACITY_PER_WORKER,
        max_block_owned_packed_output_bytes: summary.stats.max_block_stored_bytes,
        owned_packed_output_limit_bytes: MAX_SPLIT_PACKED_BYTES_PER_BLOCK,
        planes,
    })
}

fn lean_benchmark_report(
    summary: LeanOutputSummary,
    mode: LeanCompressionArg,
    zstd_level: LeanZstdLevelArg,
    account_projection_output_bytes: u64,
    metadata_account_and_effect_stream_time: Duration,
    lean_copy_and_other_worker_residual: Duration,
) -> Result<LeanBenchmarkReport> {
    let objects = LeanObject::ALL
        .into_iter()
        .map(|object| {
            let stats = summary.stats.objects[object.index()];
            let stored_to_decoded_ratio = if stats.decoded_bytes == 0 {
                1.0
            } else {
                stats.stored_bytes as f64 / stats.decoded_bytes as f64
            };
            let compression_savings_percent = if stats.decoded_bytes == 0 {
                0.0
            } else {
                (1.0 - stored_to_decoded_ratio) * 100.0
            };
            LeanObjectBenchmarkReport {
                object: object.name(),
                file: object.file_name(),
                blocks_with_bytes: stats.blocks,
                records: stats.records,
                exact_source_field_bytes: stats.source_field_bytes,
                decoded_bytes: stats.decoded_bytes,
                stored_payload_bytes: stats.stored_bytes,
                file_bytes: summary.object_file_bytes[object.index()],
                raw_blocks: stats.raw_blocks,
                zstd_blocks: stats.zstd_blocks,
                compression_sum_ms: duration_millis(stats.compression_time),
                declared_compression_policy: (mode == LeanCompressionArg::Hybrid)
                    .then_some(mode.object_compression(object).name(zstd_level)),
                compression_attempts: (mode == LeanCompressionArg::Hybrid)
                    .then_some(stats.compression_attempts),
                stored_to_decoded_ratio,
                compression_savings_percent,
            }
        })
        .collect();
    let combined_candidate_output_bytes = account_projection_output_bytes
        .checked_add(summary.output_bytes)
        .context("combined account and lean output bytes overflow")?;
    let exact_source_field_bytes =
        summary
            .stats
            .objects
            .iter()
            .try_fold(0_u64, |total, stats| {
                total
                    .checked_add(stats.source_field_bytes)
                    .context("lean exact source-field byte total overflow")
            })?;
    let decoded_payload_bytes = summary
        .stats
        .objects
        .iter()
        .try_fold(0_u64, |total, stats| {
            total
                .checked_add(stats.decoded_bytes)
                .context("lean decoded payload byte total overflow")
        })?;
    let stored_payload_bytes = summary
        .stats
        .objects
        .iter()
        .try_fold(0_u64, |total, stats| {
            total
                .checked_add(stats.stored_bytes)
                .context("lean stored payload byte total overflow")
        })?;
    let raw_block_chunks = summary
        .stats
        .objects
        .iter()
        .try_fold(0_u64, |total, stats| {
            total
                .checked_add(stats.raw_blocks)
                .context("lean raw block-chunk total overflow")
        })?;
    let zstd_block_chunks = summary
        .stats
        .objects
        .iter()
        .try_fold(0_u64, |total, stats| {
            total
                .checked_add(stats.zstd_blocks)
                .context("lean zstd block-chunk total overflow")
        })?;
    let fixed_header_and_locator_overhead_bytes = summary
        .output_bytes
        .checked_sub(stored_payload_bytes)
        .context("lean output is smaller than its stored payload")?;
    let stored_to_decoded_ratio = if decoded_payload_bytes == 0 {
        1.0
    } else {
        stored_payload_bytes as f64 / decoded_payload_bytes as f64
    };
    let compression_time = summary
        .stats
        .objects
        .iter()
        .fold(Duration::ZERO, |total, stats| {
            total.saturating_add(stats.compression_time)
        });
    Ok(LeanBenchmarkReport {
        canary_kind: LEAN_CANARY_KIND,
        candidate_status: STATUS,
        format_status: "measurement-container-not-final-schema",
        compression_mode: mode.name(),
        compression_policy: (mode == LeanCompressionArg::Hybrid)
            .then_some(zstd_level.hybrid_policy_name()),
        zstd_level: zstd_level.level(),
        zstd_context_memory_accounting: (zstd_level != LeanZstdLevelArg::One).then_some(
            "worker Vec scratch metrics exclude the additional nondefault zstd compressor context; measure process RSS externally",
        ),
        index_file: LEAN_INDEX_FILE,
        index_row_bytes: LEAN_INDEX_ROW_LEN,
        directory_row_bytes_per_transaction: LEAN_DIRECTORY_ROW_LEN,
        transaction_paging: "none-one-chunk-per-source-block-and-object",
        index_bytes: summary.index_bytes,
        lean_output_bytes: summary.output_bytes,
        account_projection_output_bytes,
        combined_candidate_output_bytes,
        exact_source_field_bytes,
        decoded_payload_bytes,
        stored_payload_bytes,
        fixed_header_and_locator_overhead_bytes,
        raw_block_chunks,
        zstd_block_chunks,
        stored_to_decoded_ratio,
        compression_savings_percent: if decoded_payload_bytes == 0 {
            0.0
        } else {
            (1.0 - stored_to_decoded_ratio) * 100.0
        },
        missing_metadata_transactions: summary.stats.missing_metadata,
        decoded_metadata_transactions: summary.stats.decoded_metadata,
        raw_metadata_transactions: summary.stats.raw_metadata,
        raw_transaction_flags: summary.stats.tx_raw_flags,
        nonempty_semantic_transaction_reward_transactions: summary
            .stats
            .nonempty_semantic_transaction_rewards,
        stored_transaction_reward_records: summary.stats.objects
            [LeanObject::TransactionRewards.index()]
        .records,
        worker_metadata_account_and_effect_stream_sum_ms: duration_millis(
            metadata_account_and_effect_stream_time,
        ),
        worker_lean_compression_sum_ms: duration_millis(compression_time),
        lean_copy_and_other_worker_residual_sum_ms: duration_millis(
            lean_copy_and_other_worker_residual,
        ),
        ordered_lean_write_sum_ms: duration_millis(summary.ordered_write_time),
        lean_finalize_ms: duration_millis(summary.finalize_time),
        max_worker_live_scratch_bytes: summary.stats.max_live_scratch_bytes,
        scratch_capacity_limit_bytes: MAX_LEAN_SCRATCH_BYTES_PER_WORKER,
        max_worker_scratch_capacity_bytes: summary.stats.max_scratch_capacity,
        max_worker_retained_scratch_capacity_bytes: summary.stats.max_retained_scratch_capacity,
        retained_scratch_capacity_limit_bytes: MAX_RETAINED_LEAN_SCRATCH_BYTES_PER_WORKER,
        max_block_owned_packed_output_bytes: summary.stats.max_block_packed_bytes,
        owned_packed_output_limit_bytes: MAX_LEAN_PACKED_BYTES_PER_BLOCK,
        objects,
    })
}

fn timing_report(
    total: Duration,
    reader: OrderedParallelBlockStats,
    reader_workers: usize,
    projection: ProjectionTiming,
    ordered_write: Duration,
    finalize: Duration,
) -> TimingReport {
    let worker_busy = reader
        .worker_decompress_decode_sum_time
        .saturating_add(reader.worker_projection_sum_time);
    let pool_capacity_seconds =
        reader.coordinator_decode_project_wall_time.as_secs_f64() * reader_workers.max(1) as f64;
    let decode_pool_utilization_percent = if pool_capacity_seconds == 0.0 {
        0.0
    } else {
        worker_busy.as_secs_f64() * 100.0 / pool_capacity_seconds
    };
    TimingReport {
        total_wall_ms: duration_millis(total),
        source_read_wall_ms: duration_millis(reader.producer_read_wall_time),
        decode_and_project_wall_ms: duration_millis(reader.coordinator_decode_project_wall_time),
        worker_decode_sum_ms: duration_millis(reader.worker_decompress_decode_sum_time),
        worker_project_sum_ms: duration_millis(reader.worker_projection_sum_time),
        worker_message_account_stream_sum_ms: duration_millis(projection.message_traversal),
        worker_metadata_account_stream_sum_ms: duration_millis(projection.metadata_traversal),
        worker_account_finalize_sum_ms: duration_millis(projection.account_role_assembly),
        worker_page_encode_sum_ms: duration_millis(projection.page_encode),
        worker_page_zstd_sum_ms: duration_millis(projection.page_zstd),
        ordered_write_sum_ms: duration_millis(ordered_write),
        source_producer_wait_for_free_buffer_ms: duration_millis(
            reader.producer_wait_for_free_buffer_time,
        ),
        ordered_writer_wait_for_ready_batch_ms: duration_millis(
            reader.coordinator_wait_for_ready_batch_time,
        ),
        worker_busy_sum_ms: duration_millis(worker_busy),
        decode_pool_utilization_percent,
        finalize_ms: duration_millis(finalize),
    }
}

fn rate(count: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        0.0
    } else {
        count as f64 / seconds
    }
}

fn mib_rate(bytes: u64, elapsed: Duration) -> f64 {
    rate(bytes, elapsed) / (1024.0 * 1024.0)
}

fn take<'a>(cursor: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    let (value, rest) = cursor
        .split_at_checked(len)
        .context("account page is truncated")?;
    *cursor = rest;
    Ok(value)
}

fn read_u8(cursor: &mut &[u8]) -> Result<u8> {
    Ok(take(cursor, 1)?[0])
}

fn read_u16(cursor: &mut &[u8]) -> Result<u16> {
    Ok(u16::from_le_bytes(
        take(cursor, 2)?.try_into().expect("two bytes"),
    ))
}

fn read_u32(cursor: &mut &[u8]) -> Result<u32> {
    Ok(u32::from_le_bytes(
        take(cursor, 4)?.try_into().expect("four bytes"),
    ))
}

fn read_u64(cursor: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(
        take(cursor, 8)?.try_into().expect("eight bytes"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{
        ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader,
        ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
        ArchiveV2HotMessagePayload, ArchiveV2HotMetaRecord, ArchiveV2HotRewards,
        ArchiveV2HotV0Message, ArchiveV2HotV1Message, CompactInnerInstruction,
        CompactInnerInstructions, CompactLogStream, CompactMessageHeader, CompactMetaV1,
        CompactReturnData, CompactReward, CompactTokenBalance, CompactTransactionConfig,
        CompactTransactionError, DataTable, LogEvent, OwnedCompactAddressTableLookup,
        OwnedCompactRecentBlockhash, StringTable, WINCODE_ARCHIVE_V2_FLAG_LEB128,
        WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header,
        WincodeLeb128Config, wincode_leb128_config, write_archive_v2_hot_block_index,
    };
    use smallvec::SmallVec;
    use wincode::SchemaWrite;

    #[derive(Debug, SchemaWrite)]
    enum May24MessagePayload {
        Legacy(May24LegacyMessage),
    }

    #[derive(Debug, SchemaWrite)]
    struct May24LegacyMessage {
        header: CompactMessageHeader,
        account_keys: SmallVec<[CompactPubkey; 8]>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: SmallVec<[May24Instruction; 2]>,
    }

    #[derive(Debug, SchemaWrite)]
    struct May24Instruction {
        program_id_index: u8,
        accounts: SmallVec<[u8; 8]>,
        data: May24InstructionData,
    }

    #[allow(dead_code)]
    #[derive(Debug, SchemaWrite)]
    enum May24InstructionData {
        Raw(SmallVec<[u8; 64]>),
        ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
    }

    fn serialize<T: SchemaWrite<WincodeLeb128Config, Src = T>>(value: &T) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn metadata(
        error: Option<CompactTransactionError>,
        inner_instructions: Option<Vec<CompactInnerInstructions>>,
        loaded_writable_addresses: Vec<CompactPubkey>,
        loaded_readonly_addresses: Vec<CompactPubkey>,
    ) -> CompactMetaV1 {
        CompactMetaV1 {
            err: error,
            fee: 5_000,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
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

    fn instruction(program_id_index: u8, accounts: &[u8]) -> ArchiveV2HotInstruction {
        ArchiveV2HotInstruction {
            program_id_index,
            accounts: accounts.to_vec(),
            data: ArchiveV2HotInstructionData::Raw(Vec::new()),
        }
    }

    fn tx_row(
        message_len: usize,
        metadata_len: usize,
        flags: u32,
        signature_count: u8,
    ) -> ArchiveV2HotTxRow {
        ArchiveV2HotTxRow {
            tx_index: 0,
            flags,
            message_offset: 0,
            message_len: message_len as u32,
            metadata_offset: 0,
            metadata_len: metadata_len as u32,
            signature_count,
            reserved: [0; 3],
        }
    }

    fn projection_config(registry_entries: u32) -> ProjectionConfig {
        ProjectionConfig {
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
            registry_entries,
        }
    }

    fn index_row(block_id: u32, slot: u64, tx_count: u32) -> ArchiveV2HotBlockIndexRow {
        ArchiveV2HotBlockIndexRow {
            block_id,
            slot,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 0,
        }
    }

    fn write_u32_varint(bytes: &mut Vec<u8>, mut value: u32) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn write_reader_fixture(
        root: &Path,
        record_second_cpi: bool,
        block_pairs: usize,
        include_transaction_rewards: bool,
    ) {
        assert!(block_pairs > 0);
        fs::write(
            root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            vec![0_u8; 32 * 10],
        )
        .unwrap();

        let first_message = serialize(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw([9; 32])],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![instruction(1, &[0])],
            },
        ));
        let mut first_metadata_value = metadata(
            None,
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: Vec::new(),
                    stack_height: Some(2),
                }],
            }]),
            Vec::new(),
            Vec::new(),
        );
        if include_transaction_rewards {
            first_metadata_value.rewards.push(CompactReward {
                pubkey: CompactPubkey::Id(5),
                lamports: 6,
                post_balance: 7,
                reward_type: 8,
                commission: None,
            });
        }
        let first_metadata = serialize(&first_metadata_value);
        let second_message = serialize(&ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(1),
            instructions: vec![instruction(2, &[0, 1])],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(4),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        }));
        let second_inner = record_second_cpi.then(|| {
            vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 1,
                    accounts: vec![2],
                    data: Vec::new(),
                    stack_height: Some(2),
                }],
            }]
        });
        let mut second_metadata_value = metadata(
            Some(CompactTransactionError::AccountInUse),
            second_inner,
            vec![CompactPubkey::Raw([7; 32])],
            vec![CompactPubkey::Id(3)],
        );
        if include_transaction_rewards {
            second_metadata_value.rewards = (0..128)
                .map(|_| CompactReward {
                    pubkey: CompactPubkey::Id(5),
                    lamports: 6,
                    post_balance: 7,
                    reward_type: 8,
                    commission: None,
                })
                .collect();
        }
        let second_metadata = serialize(&second_metadata_value);
        let block_count = block_pairs.checked_mul(2).unwrap();
        let mut blocks = Vec::new();
        let mut rows = Vec::with_capacity(block_count);
        for block_id in 0..block_count {
            let slot = 101_u64 + block_id as u64;
            let is_first_shape = block_id % 2 == 0;
            let (message_bytes, metadata_bytes, mut flags) = if is_first_shape {
                (
                    first_message.clone(),
                    first_metadata.clone(),
                    ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                )
            } else {
                (
                    second_message.clone(),
                    second_metadata.clone(),
                    ARCHIVE_V2_TX_FLAG_HAS_METADATA
                        | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                        | ARCHIVE_V2_TX_FLAG_HAS_ERROR,
                )
            };
            if !is_first_shape && record_second_cpi {
                flags |= ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
            }
            let block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot,
                    parent_slot: slot - 1,
                    blockhash_id: 1 + block_id as u32,
                    previous_blockhash_id: block_id as u32,
                    block_time: None,
                    block_height: Some(1 + block_id as u64),
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![tx_row(message_bytes.len(), metadata_bytes.len(), flags, 1)],
                message_bytes,
                metadata_bytes,
            };
            let uncompressed = serialize(&block);
            let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
            rows.push(ArchiveV2HotBlockIndexRow {
                block_id: block_id as u32,
                slot,
                compressed_offset: blocks.len() as u64,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: 1,
                first_tx_ordinal: block_id as u64,
                first_signature_ordinal: block_id as u64,
                signature_count: 1,
            });
            blocks.extend_from_slice(&compressed);
        }
        fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &blocks).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            blocks.len() as u64,
            1,
            0,
            &rows,
        )
        .unwrap();
        fs::write(
            root.join(ARCHIVE_V2_SIGNATURES_FILE),
            vec![0x55; block_count * 64],
        )
        .unwrap();

        let records = [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks: block_count as u64,
                transactions: block_count as u64,
                ..WincodeArchiveV2Footer::default()
            }),
        ];
        let mut encoded_records = Vec::new();
        for record in records {
            let record = serialize(&record);
            write_u32_varint(&mut encoded_records, record.len() as u32);
            encoded_records.extend_from_slice(&record);
        }
        fs::write(root.join(ARCHIVE_V2_META_FILE), encoded_records).unwrap();
    }

    #[derive(Debug)]
    struct TestSplitFrame {
        plane: u16,
        flags: u32,
        block_id: u32,
        first_tx: u32,
        tx_count: u32,
        dense_count: u32,
        decoded: Vec<u8>,
    }

    fn read_split_frames(path: &Path, expected_plane: SplitPlane) -> Vec<TestSplitFrame> {
        let bytes = fs::read(path).unwrap();
        assert!(bytes.len() >= SPLIT_FILE_HEADER_LEN);
        let mut header = &bytes[..SPLIT_FILE_HEADER_LEN];
        assert_eq!(take(&mut header, 8).unwrap(), SPLIT_DATA_MAGIC);
        assert_eq!(read_u16(&mut header).unwrap(), SPLIT_FORMAT_VERSION);
        assert_eq!(read_u16(&mut header).unwrap(), expected_plane as u16);
        assert_eq!(read_u32(&mut header).unwrap(), 0);
        take(&mut header, 8 * 3 + 4 + 8 + 12).unwrap();
        assert!(header.is_empty());

        let mut cursor = &bytes[SPLIT_FILE_HEADER_LEN..];
        let mut frames = Vec::new();
        while !cursor.is_empty() {
            assert!(cursor.len() >= SPLIT_FRAME_HEADER_LEN);
            let plane = read_u16(&mut cursor).unwrap();
            assert_eq!(read_u16(&mut cursor).unwrap(), SPLIT_FORMAT_VERSION);
            let flags = read_u32(&mut cursor).unwrap();
            let block_id = read_u32(&mut cursor).unwrap();
            let first_tx = read_u32(&mut cursor).unwrap();
            let tx_count = read_u32(&mut cursor).unwrap();
            let dense_count = read_u32(&mut cursor).unwrap();
            let decoded_len = read_u32(&mut cursor).unwrap() as usize;
            let stored_len = read_u32(&mut cursor).unwrap() as usize;
            assert_eq!(plane, expected_plane as u16);
            assert_eq!(flags & !SPLIT_FRAME_FLAG_ZSTD, 0);
            let stored = take(&mut cursor, stored_len).unwrap();
            let decoded = if flags & SPLIT_FRAME_FLAG_ZSTD != 0 {
                assert!(stored_len < decoded_len);
                zstd::bulk::decompress(stored, decoded_len).unwrap()
            } else {
                assert_eq!(stored_len, decoded_len);
                stored.to_vec()
            };
            assert_eq!(decoded.len(), decoded_len);
            frames.push(TestSplitFrame {
                plane,
                flags,
                block_id,
                first_tx,
                tx_count,
                dense_count,
                decoded,
            });
        }
        frames
    }

    fn split_output_files() -> Vec<&'static str> {
        let mut files = SplitPlane::ALL
            .into_iter()
            .map(SplitPlane::file_name)
            .collect::<Vec<_>>();
        files.push(SPLIT_INDEX_FILE);
        files
    }

    fn lean_output_files() -> Vec<&'static str> {
        let mut files = LeanObject::ALL
            .into_iter()
            .map(LeanObject::file_name)
            .collect::<Vec<_>>();
        files.push(LEAN_INDEX_FILE);
        files
    }

    #[derive(Debug)]
    struct TestLeanBlock {
        block_id: u32,
        tx_count: u32,
        slot: u64,
        decoded: [Vec<u8>; LEAN_OBJECT_COUNT],
        compressed: [bool; LEAN_OBJECT_COUNT],
    }

    fn assert_lean_header_at_level(
        bytes: &[u8],
        magic: [u8; 8],
        object: u16,
        mode: LeanCompressionArg,
        zstd_level: LeanZstdLevelArg,
        binding: SplitHeaderBinding,
    ) {
        assert!(bytes.len() >= LEAN_FILE_HEADER_LEN);
        let mut header = &bytes[..LEAN_FILE_HEADER_LEN];
        assert_eq!(take(&mut header, 8).unwrap(), magic);
        assert_eq!(read_u16(&mut header).unwrap(), LEAN_FORMAT_VERSION);
        assert_eq!(read_u16(&mut header).unwrap(), object);
        assert_eq!(take(&mut header, 1).unwrap(), [mode.code()]);
        assert_eq!(take(&mut header, 1).unwrap(), [0]); // Current message schema.
        assert_eq!(take(&mut header, 1).unwrap(), [0]); // Current typed-error metadata.
        assert_eq!(take(&mut header, 1).unwrap(), [1]); // Current outer Archive V2.
        assert_eq!(read_u64(&mut header).unwrap(), binding.epoch);
        assert_eq!(read_u64(&mut header).unwrap(), binding.slots_per_epoch);
        assert_eq!(read_u64(&mut header).unwrap(), binding.selected_blocks);
        assert_eq!(
            read_u64(&mut header).unwrap(),
            binding.selected_transactions
        );
        assert_eq!(take(&mut header, 1).unwrap(), [u8::from(binding.prefix)]);
        assert_eq!(
            read_u16(&mut header).unwrap(),
            LEAN_DIRECTORY_ROW_LEN as u16
        );
        assert_eq!(
            take(&mut header, 1).unwrap(),
            [LeanObject::DENSE_TX_PLANES.len() as u8]
        );
        assert_eq!(take(&mut header, 1).unwrap(), [2]);
        assert_eq!(take(&mut header, 1).unwrap(), [LEAN_OBJECT_COUNT as u8]);
        assert_eq!(take(&mut header, 1).unwrap(), [zstd_level.header_code()]);
        assert_eq!(take(&mut header, 9).unwrap(), [0; 9]);
        assert!(header.is_empty());
    }

    fn read_lean_output(
        root: &Path,
        mode: LeanCompressionArg,
        epoch: u64,
        slots_per_epoch: u64,
        prefix: bool,
        expected: &[(u32, u64, u32)],
    ) -> Vec<TestLeanBlock> {
        read_lean_output_at_level(
            root,
            mode,
            LeanZstdLevelArg::One,
            epoch,
            slots_per_epoch,
            prefix,
            expected,
        )
    }

    fn read_lean_output_at_level(
        root: &Path,
        mode: LeanCompressionArg,
        zstd_level: LeanZstdLevelArg,
        epoch: u64,
        slots_per_epoch: u64,
        prefix: bool,
        expected: &[(u32, u64, u32)],
    ) -> Vec<TestLeanBlock> {
        let selected_transactions = expected
            .iter()
            .map(|&(_, _, count)| u64::from(count))
            .sum::<u64>();
        let binding = SplitHeaderBinding {
            epoch,
            slots_per_epoch,
            selected_blocks: expected.len() as u64,
            selected_transactions,
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
            prefix,
        };
        let object_files = LeanObject::ALL.map(|object| {
            let bytes = fs::read(root.join(object.file_name())).unwrap();
            assert_lean_header_at_level(
                &bytes,
                LEAN_DATA_MAGIC,
                object as u16,
                mode,
                zstd_level,
                binding,
            );
            bytes
        });
        let index = fs::read(root.join(LEAN_INDEX_FILE)).unwrap();
        assert_lean_header_at_level(
            &index,
            LEAN_INDEX_MAGIC,
            u16::MAX,
            mode,
            zstd_level,
            binding,
        );
        assert_eq!(
            index.len(),
            LEAN_FILE_HEADER_LEN + expected.len() * LEAN_INDEX_ROW_LEN
        );
        let mut cursor = &index[LEAN_FILE_HEADER_LEN..];
        let mut offsets = [LEAN_FILE_HEADER_LEN as u64; LEAN_OBJECT_COUNT];
        let mut blocks = Vec::with_capacity(expected.len());
        for &(expected_block_id, expected_slot, expected_tx_count) in expected {
            let block_id = read_u32(&mut cursor).unwrap();
            let tx_count = read_u32(&mut cursor).unwrap();
            let slot = read_u64(&mut cursor).unwrap();
            assert_eq!(block_id, expected_block_id);
            assert_eq!(slot, expected_slot);
            assert_eq!(tx_count, expected_tx_count);
            let mut decoded: [Vec<u8>; LEAN_OBJECT_COUNT] = std::array::from_fn(|_| Vec::new());
            let mut compressed = [false; LEAN_OBJECT_COUNT];
            for object in LeanObject::ALL {
                let object_index = object.index();
                let offset = read_u64(&mut cursor).unwrap();
                let stored_len_and_codec = read_u32(&mut cursor).unwrap();
                let decoded_len = read_u32(&mut cursor).unwrap();
                let is_zstd = stored_len_and_codec & LEAN_ZSTD_CODEC_BIT != 0;
                let stored_len = stored_len_and_codec & LEAN_STORED_LEN_MASK;
                assert_eq!(offset, offsets[object_index], "{} offset", object.name());
                let start = usize::try_from(offset).unwrap();
                let end = start.checked_add(stored_len as usize).unwrap();
                let stored = &object_files[object_index][start..end];
                decoded[object_index] = if is_zstd {
                    assert_ne!(stored_len, 0);
                    zstd::bulk::decompress(stored, decoded_len as usize).unwrap()
                } else {
                    assert_eq!(stored_len, decoded_len);
                    stored.to_vec()
                };
                assert_eq!(decoded[object_index].len(), decoded_len as usize);
                if stored_len == 0 {
                    assert!(!is_zstd);
                }
                compressed[object_index] = is_zstd;
                offsets[object_index] = offset.checked_add(u64::from(stored_len)).unwrap();
            }
            assert_eq!(
                decoded[LeanObject::TransactionDirectory.index()].len(),
                tx_count as usize * LEAN_DIRECTORY_ROW_LEN
            );
            blocks.push(TestLeanBlock {
                block_id,
                tx_count,
                slot,
                decoded,
                compressed,
            });
        }
        assert!(cursor.is_empty());
        for object in LeanObject::ALL {
            assert_eq!(
                object_files[object.index()].len() as u64,
                offsets[object.index()],
                "{} EOF",
                object.name()
            );
        }
        blocks
    }

    fn decode_golden_hex(encoded: &str) -> Vec<u8> {
        assert_eq!(encoded.len() % 2, 0);
        encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("invalid golden hex digit"),
                };
                digit(pair[0]) << 4 | digit(pair[1])
            })
            .collect()
    }

    fn assert_split_index_geometry(root: &Path, expected: &[(u32, u64, u32)]) {
        let bytes = fs::read(root.join(SPLIT_INDEX_FILE)).unwrap();
        let mut cursor = bytes.as_slice();
        assert_eq!(take(&mut cursor, 8).unwrap(), SPLIT_INDEX_MAGIC);
        assert_eq!(read_u16(&mut cursor).unwrap(), SPLIT_FORMAT_VERSION);
        assert_eq!(read_u16(&mut cursor).unwrap(), u16::MAX);
        assert_eq!(read_u32(&mut cursor).unwrap(), 0);
        take(&mut cursor, 8 * 3 + 4 + 8 + 12).unwrap();
        let mut offsets = [SPLIT_FILE_HEADER_LEN as u64; SPLIT_PLANE_COUNT];
        for &(block_id, slot, tx_count) in expected {
            assert_eq!(read_u32(&mut cursor).unwrap(), block_id);
            assert_eq!(read_u64(&mut cursor).unwrap(), slot);
            assert_eq!(read_u32(&mut cursor).unwrap(), tx_count);
            for plane in SplitPlane::ALL {
                let index = plane.index();
                let offset = read_u64(&mut cursor).unwrap();
                let length = read_u64(&mut cursor).unwrap();
                assert_eq!(offset, offsets[index], "{} offset", plane.name());
                offsets[index] = offsets[index].checked_add(length).unwrap();
            }
        }
        assert!(cursor.is_empty());
        for plane in SplitPlane::ALL {
            assert_eq!(
                fs::metadata(root.join(plane.file_name())).unwrap().len(),
                offsets[plane.index()],
                "{} EOF",
                plane.name()
            );
        }
    }

    fn assert_frozen_default_report_schema(report: &serde_json::Value) {
        let expected = [
            "status",
            "output_validation",
            "content_hashing",
            "account_semantics",
            "epoch",
            "slots_per_epoch",
            "message_schema",
            "metadata_schema",
            "workers",
            "benchmark_prefix_blocks",
            "source_total_blocks",
            "selected_blocks",
            "transactions",
            "account_refs",
            "id_refs",
            "raw_refs",
            "success_transactions",
            "failed_transactions",
            "unknown_transactions",
            "fully_covered_transactions",
            "incomplete_coverage_transactions",
            "account_complete_transactions",
            "account_missing_loaded_metadata_transactions",
            "account_raw_transaction_fallbacks",
            "account_raw_metadata_loaded_fallbacks",
            "cpi_recorded_transactions",
            "cpi_not_recorded_transactions",
            "cpi_missing_metadata_transactions",
            "cpi_raw_transaction_fallbacks",
            "cpi_raw_metadata_fallbacks",
            "source_raw_transaction_fallback_flags",
            "source_raw_metadata_fallback_flags",
            "max_resolved_source_positions_per_transaction",
            "max_unique_output_accounts_per_transaction",
            "duplicate_account_merges",
            "complete_coverage",
            "source_compressed_bytes",
            "source_decoded_bytes",
            "page_decoded_bytes",
            "page_stored_bytes",
            "index_bytes",
            "output_bytes",
            "transactions_per_second",
            "source_compressed_mib_per_second",
            "source_block_read_calls",
            "reader_batches",
            "reader_max_blocks_per_batch",
            "reader_max_compressed_batch_bytes",
            "reader_max_declared_uncompressed_batch_bytes",
            "reader_max_retained_decompressed_buffer_bytes",
            "signature_content_reads",
            "unrelated_source_content_reads",
            "source_unchanged",
            "timing",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let actual = report
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        let expected_timing = [
            "total_wall_ms",
            "source_read_wall_ms",
            "decode_and_project_wall_ms",
            "worker_decode_sum_ms",
            "worker_project_sum_ms",
            "worker_message_account_stream_sum_ms",
            "worker_metadata_account_stream_sum_ms",
            "worker_account_finalize_sum_ms",
            "worker_page_encode_sum_ms",
            "worker_page_zstd_sum_ms",
            "ordered_write_sum_ms",
            "source_producer_wait_for_free_buffer_ms",
            "ordered_writer_wait_for_ready_batch_ms",
            "worker_busy_sum_ms",
            "decode_pool_utilization_percent",
            "finalize_ms",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let actual_timing = report["timing"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(actual_timing, expected_timing);
    }

    #[test]
    fn legacy_projection_preserves_source_order_variants_and_unions_roles() {
        let raw = [9; 32];
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
                CompactPubkey::Id(1),
                CompactPubkey::Raw(raw),
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![instruction(3, &[0, 4])],
        });
        let metadata = metadata(
            None,
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 0,
                    accounts: vec![1],
                    data: Vec::new(),
                    stack_height: Some(2),
                }],
            }]),
            Vec::new(),
            Vec::new(),
        );
        let message = serialize(&message);
        let metadata = serialize(&metadata);
        let projected = project_transaction(
            projection_config(20),
            101,
            tx_row(
                message.len(),
                metadata.len(),
                ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                2,
            ),
            &message,
            &metadata,
        )
        .unwrap();

        assert_eq!(projected.outcome, Outcome::Success);
        assert_eq!(projected.account_coverage, AccountCoverage::Complete);
        assert_eq!(projected.cpi_coverage, CpiCoverage::Recorded);
        assert_eq!(
            projected.accounts,
            vec![
                AccountUse {
                    key: CompactPubkey::Id(1),
                    roles: ROLE_SIGNER | ROLE_WRITABLE | ROLE_TOP_LEVEL_PROGRAM | ROLE_CPI_PROGRAM,
                },
                AccountUse {
                    key: CompactPubkey::Id(2),
                    roles: ROLE_SIGNER,
                },
                AccountUse {
                    key: CompactPubkey::Id(3),
                    roles: ROLE_WRITABLE,
                },
                AccountUse {
                    key: CompactPubkey::Raw(raw),
                    roles: 0,
                },
            ]
        );
    }

    #[test]
    fn v0_projection_uses_loaded_lanes_and_keeps_failed_outcome() {
        let raw_loaded = [7; 32];
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![instruction(1, &[0, 2])],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(9),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let metadata = metadata(
            Some(CompactTransactionError::AccountInUse),
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 2,
                    accounts: vec![3],
                    data: Vec::new(),
                    stack_height: Some(2),
                }],
            }]),
            vec![CompactPubkey::Raw(raw_loaded)],
            vec![CompactPubkey::Id(3)],
        );
        let message = serialize(&message);
        let metadata = serialize(&metadata);
        let projected = project_transaction(
            projection_config(20),
            102,
            tx_row(
                message.len(),
                metadata.len(),
                ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
                    | ARCHIVE_V2_TX_FLAG_HAS_ERROR,
                1,
            ),
            &message,
            &metadata,
        )
        .unwrap();

        assert_eq!(projected.outcome, Outcome::Failed);
        assert_eq!(projected.account_coverage, AccountCoverage::Complete);
        assert_eq!(projected.cpi_coverage, CpiCoverage::Recorded);
        assert_eq!(
            projected.accounts,
            vec![
                AccountUse {
                    key: CompactPubkey::Id(1),
                    roles: ROLE_SIGNER | ROLE_WRITABLE,
                },
                AccountUse {
                    key: CompactPubkey::Id(2),
                    roles: ROLE_TOP_LEVEL_PROGRAM,
                },
                AccountUse {
                    key: CompactPubkey::Raw(raw_loaded),
                    roles: ROLE_WRITABLE | ROLE_CPI_PROGRAM,
                },
                AccountUse {
                    key: CompactPubkey::Id(3),
                    roles: 0,
                },
            ]
        );
    }

    #[test]
    fn independent_streaming_verifier_matches_projection_and_reuses_scratch() {
        let message = serialize(&ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw([4; 32])],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![instruction(2, &[0, 3])],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(9),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        }));
        let metadata = serialize(&metadata(
            Some(CompactTransactionError::AccountInUse),
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 3,
                    accounts: vec![1],
                    data: Vec::new(),
                    stack_height: Some(2),
                }],
            }]),
            vec![CompactPubkey::Id(2)],
            vec![CompactPubkey::Raw([4; 32])],
        ));
        let row = tx_row(
            message.len(),
            metadata.len(),
            ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
                | ARCHIVE_V2_TX_FLAG_HAS_ERROR,
            1,
        );
        let config = projection_config(20);
        let expected = project_transaction(config, 108, row, &message, &metadata).unwrap();
        let mut worker = SemanticVerifierWorker::new(tempfile::tempfile().unwrap()).unwrap();
        let initial_capacity = worker.reference_accounts.capacity();
        for _ in 0..2 {
            let (mut actual, positions, merges) =
                reference_project_transaction(&mut worker, config, 108, row, &message, &metadata)
                    .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(positions, 4);
            assert_eq!(merges, 1);
            actual.accounts.clear();
            worker.reference_accounts = actual.accounts;
            assert_eq!(worker.reference_accounts.capacity(), initial_capacity);
        }

        let mut trailing_metadata = metadata.clone();
        trailing_metadata.push(0);
        assert!(
            reference_project_transaction(
                &mut worker,
                config,
                108,
                tx_row(
                    message.len(),
                    trailing_metadata.len(),
                    row.flags,
                    row.signature_count,
                ),
                &message,
                &trailing_metadata,
            )
            .unwrap_err()
            .to_string()
            .contains("trailing bytes")
        );
    }

    #[test]
    fn independent_candidate_transaction_parser_rejects_every_mutable_field() {
        let expected = ProjectedTransaction {
            source_flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA,
            outcome: Outcome::Success,
            account_coverage: AccountCoverage::Complete,
            cpi_coverage: CpiCoverage::NotRecorded,
            accounts: vec![AccountUse {
                key: CompactPubkey::Id(1),
                roles: ROLE_SIGNER | ROLE_WRITABLE,
            }],
        };
        let bytes = [
            1, 0, 1, 0, // outcome, account coverage, CPI coverage, reserved
            1, 0, 0, 0, // HAS_METADATA
            1, 0, 0, 0, // account count
            0, 3, 0, 0, // Id kind, roles, reserved
            1, 0, 0, 0, // Id 1
        ];
        let mut cursor = bytes.as_slice();
        assert_eq!(
            parse_candidate_transaction_exact(&mut cursor, 2, Vec::new()).unwrap(),
            expected
        );
        assert!(cursor.is_empty());
        for (offset, value) in [
            (0, 9),
            (1, 9),
            (2, 9),
            (3, 1),
            (7, 0x80),
            (12, 9),
            (13, 0x80),
            (14, 1),
            (16, 0),
        ] {
            let mut corrupted = bytes;
            corrupted[offset] = value;
            assert!(
                parse_candidate_transaction_exact(&mut corrupted.as_slice(), 2, Vec::new())
                    .is_err(),
                "mutation at byte {offset} was accepted"
            );
        }
        let mut high_id = bytes;
        high_id[16] = 3;
        assert!(parse_candidate_transaction_exact(&mut high_id.as_slice(), 2, Vec::new()).is_err());
        let mut truncated = bytes.as_slice();
        truncated = &truncated[..truncated.len() - 1];
        assert!(parse_candidate_transaction_exact(&mut truncated, 2, Vec::new()).is_err());
    }

    #[test]
    fn anchored_snapshot_cannot_mix_roots_during_aba_swap() {
        for label in ["source", "candidate", "predecessor source"] {
            let parent = tempfile::tempdir().unwrap();
            let active_parent = parent.path().join("active-parent");
            let replacement_parent = parent.path().join("replacement-parent");
            let held_original_parent = parent.path().join("held-original-parent");
            let held_replacement_parent = parent.path().join("held-replacement-parent");
            let active_root = active_parent.join("root");
            let replacement_root = replacement_parent.join("root");
            fs::create_dir_all(&active_root).unwrap();
            fs::create_dir_all(&replacement_root).unwrap();
            fs::write(active_root.join("object.bin"), b"original").unwrap();
            fs::write(replacement_root.join("object.bin"), b"different").unwrap();
            let canonical_root = fs::canonicalize(&active_root).unwrap();
            let pinned =
                PinnedLocalRangeSource::new_anchored(&canonical_root, &["object.bin"]).unwrap();

            let snapshot = capture_anchored_directory_snapshot_with_hook(&pinned, label, || {
                fs::rename(&active_parent, &held_original_parent)?;
                fs::rename(&replacement_parent, &active_parent)?;
                Ok(())
            })
            .unwrap();
            assert_eq!(pinned.read_range("object.bin", 0, 8).unwrap(), b"original");

            fs::rename(&active_parent, &held_replacement_parent).unwrap();
            fs::rename(&held_original_parent, &active_parent).unwrap();
            bind_pinned_objects_to_snapshot(&pinned, &snapshot, label, &["object.bin"], &[])
                .unwrap();
            verify_anchored_directory_unchanged(&pinned, &canonical_root, &snapshot, label)
                .unwrap();
        }
    }

    #[test]
    fn current_v1_projection_skips_config_and_keeps_exact_roles() {
        let message = serialize(&ArchiveV2HotMessagePayload::V1(ArchiveV2HotV1Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            config: CompactTransactionConfig {
                priority_fee: Some(7),
                compute_unit_limit: Some(200_000),
                loaded_accounts_data_size_limit: Some(1_024),
                heap_size: Some(64 * 1_024),
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw([8; 32])],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![instruction(1, &[0])],
        }));
        let metadata = serialize(&metadata(
            None,
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: Vec::new(),
                    stack_height: Some(2),
                }],
            }]),
            Vec::new(),
            Vec::new(),
        ));
        let projected = project_transaction(
            projection_config(10),
            105,
            tx_row(
                message.len(),
                metadata.len(),
                ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                1,
            ),
            &message,
            &metadata,
        )
        .unwrap();
        assert_eq!(projected.outcome, Outcome::Success);
        assert_eq!(projected.account_coverage, AccountCoverage::Complete);
        assert_eq!(projected.cpi_coverage, CpiCoverage::Recorded);
        assert_eq!(
            projected.accounts,
            vec![
                AccountUse {
                    key: CompactPubkey::Id(1),
                    roles: ROLE_SIGNER | ROLE_WRITABLE,
                },
                AccountUse {
                    key: CompactPubkey::Raw([8; 32]),
                    roles: ROLE_TOP_LEVEL_PROGRAM | ROLE_CPI_PROGRAM,
                },
            ]
        );
    }

    #[test]
    fn may24_message_and_legacy_raw_error_metadata_use_explicit_profiles() {
        let message = serialize(&May24MessagePayload::Legacy(May24LegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: SmallVec::from_vec(vec![
                CompactPubkey::Id(1),
                CompactPubkey::Raw([6; 32]),
            ]),
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: SmallVec::from_vec(vec![May24Instruction {
                program_id_index: 1,
                accounts: SmallVec::from_slice(&[0]),
                // Tag 1 is ComputeBudget only in the selected May24 grammar.
                data: May24InstructionData::ComputeBudget(
                    ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(123_456),
                ),
            }]),
        }));
        let mut legacy_metadata = serialize(&Some(vec![8, 0, 0, 0]));
        legacy_metadata.extend(serialize(&5_000_u64));
        legacy_metadata.extend(serialize(&Vec::<u64>::new()));
        legacy_metadata.extend(serialize(&Vec::<u64>::new()));
        legacy_metadata.extend(serialize(&Some(vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: Vec::new(),
                stack_height: Some(2),
            }],
        }])));
        let projected = project_transaction(
            ProjectionConfig {
                message_schema: CompactV2MessageSchema::May24PreUnknownFallbacks,
                metadata_schema: CompactV2MetadataSchema::LegacyRawError,
                registry_entries: 10,
            },
            106,
            tx_row(
                message.len(),
                legacy_metadata.len(),
                ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
                    | ARCHIVE_V2_TX_FLAG_HAS_ERROR,
                1,
            ),
            &message,
            &legacy_metadata,
        )
        .unwrap();
        assert_eq!(projected.outcome, Outcome::Failed);
        assert_eq!(projected.account_coverage, AccountCoverage::Complete);
        assert_eq!(projected.cpi_coverage, CpiCoverage::Recorded);
        assert_eq!(
            projected.accounts,
            vec![
                AccountUse {
                    key: CompactPubkey::Id(1),
                    roles: ROLE_SIGNER | ROLE_WRITABLE,
                },
                AccountUse {
                    key: CompactPubkey::Raw([6; 32]),
                    roles: ROLE_TOP_LEVEL_PROGRAM | ROLE_CPI_PROGRAM,
                },
            ]
        );
    }

    #[test]
    fn unavailable_v0_metadata_emits_known_static_accounts_without_loaded_index_failure() {
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![instruction(1, &[0, 2])],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(9),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let message = serialize(&message);
        for (flags, account_coverage, cpi_coverage, metadata_bytes) in [
            (
                ARCHIVE_V2_TX_FLAG_MESSAGE_V0 | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                AccountCoverage::MissingLoadedMetadata,
                CpiCoverage::MissingMetadata,
                &[][..],
            ),
            (
                ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                    | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                    | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                AccountCoverage::RawMetadataLoadedFallback,
                CpiCoverage::RawMetadataFallback,
                &[0xaa][..],
            ),
        ] {
            let projected = project_transaction(
                projection_config(20),
                103,
                tx_row(message.len(), metadata_bytes.len(), flags, 1),
                &message,
                metadata_bytes,
            )
            .unwrap();
            assert_eq!(projected.outcome, Outcome::Unknown);
            assert_eq!(projected.account_coverage, account_coverage);
            assert_eq!(projected.cpi_coverage, cpi_coverage);
            assert_eq!(
                projected.accounts,
                vec![AccountUse {
                    key: CompactPubkey::Id(1),
                    roles: ROLE_SIGNER | ROLE_WRITABLE,
                }]
            );
        }
        assert!(
            project_transaction(
                projection_config(20),
                103,
                tx_row(
                    message.len(),
                    0,
                    ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                        | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                    1,
                ),
                &message,
                &[],
            )
            .unwrap_err()
            .to_string()
            .contains("no source metadata range")
        );
    }

    #[test]
    fn raw_transaction_and_missing_cpi_have_independent_outcome_and_coverage() {
        let raw = project_transaction(
            projection_config(20),
            104,
            tx_row(
                3,
                1,
                ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                    | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                0,
            ),
            &[1, 2, 3],
            &[4],
        )
        .unwrap();
        assert_eq!(raw.outcome, Outcome::Unknown);
        assert_eq!(
            raw.account_coverage,
            AccountCoverage::RawTransactionFallback
        );
        assert_eq!(raw.cpi_coverage, CpiCoverage::RawTransactionFallback);
        assert!(raw.accounts.is_empty());
        let mut raw_stats = ProjectionStats::default();
        add_transaction_stats(&mut raw_stats, &raw).unwrap();
        assert_eq!(raw_stats.source_raw_transaction_flags, 1);
        assert_eq!(raw_stats.source_raw_metadata_flags, 1);

        let decoded_success_metadata = serialize(&metadata(None, None, Vec::new(), Vec::new()));
        let decoded_failed_metadata = serialize(&metadata(
            Some(CompactTransactionError::AccountInUse),
            None,
            Vec::new(),
            Vec::new(),
        ));
        for (flags, metadata_bytes, expected_outcome) in [
            (
                ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                &[][..],
                Outcome::Unknown,
            ),
            (
                ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK | ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                decoded_success_metadata.as_slice(),
                Outcome::Success,
            ),
            (
                ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                    | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_HAS_ERROR,
                decoded_failed_metadata.as_slice(),
                Outcome::Failed,
            ),
        ] {
            let projected = project_transaction(
                projection_config(20),
                104,
                tx_row(3, metadata_bytes.len(), flags, 0),
                &[1, 2, 3],
                metadata_bytes,
            )
            .unwrap();
            assert_eq!(projected.outcome, expected_outcome);
            assert_eq!(
                projected.account_coverage,
                AccountCoverage::RawTransactionFallback
            );
            assert_eq!(projected.cpi_coverage, CpiCoverage::RawTransactionFallback);
        }

        let message = serialize(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![CompactPubkey::Id(1)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: Vec::new(),
            },
        ));
        let metadata = serialize(&metadata(None, None, Vec::new(), Vec::new()));
        let projected = project_transaction(
            projection_config(20),
            104,
            tx_row(
                message.len(),
                metadata.len(),
                ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                1,
            ),
            &message,
            &metadata,
        )
        .unwrap();
        assert_eq!(projected.outcome, Outcome::Success);
        assert_eq!(projected.account_coverage, AccountCoverage::Complete);
        assert_eq!(projected.cpi_coverage, CpiCoverage::NotRecorded);
    }

    #[test]
    fn projection_rejects_bad_pubkey_and_instruction_bounds() {
        let mut decoded = decode::DecodedMessage {
            account_keys: vec![CompactPubkey::Id(0)],
            is_v0: false,
            num_required_signatures: 0,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
            instruction_count: 0,
            expected_loaded_writable: 0,
            expected_loaded_readonly: 0,
        };
        assert!(
            build_account_uses_for_test(
                &decoded,
                &[],
                &[],
                &[],
                &[],
                &[],
                &[],
                AccountCoverage::Complete,
                5,
                1,
                0,
            )
            .unwrap_err()
            .to_string()
            .contains("outside 1..=5")
        );
        decoded.account_keys[0] = CompactPubkey::Id(6);
        assert!(
            build_account_uses_for_test(
                &decoded,
                &[],
                &[],
                &[],
                &[],
                &[],
                &[],
                AccountCoverage::Complete,
                5,
                1,
                0,
            )
            .is_err()
        );
        decoded.account_keys[0] = CompactPubkey::Id(1);
        assert!(
            build_account_uses_for_test(
                &decoded,
                &[],
                &[],
                &[1],
                &[],
                &[],
                &[],
                AccountCoverage::Complete,
                5,
                1,
                0,
            )
            .unwrap_err()
            .to_string()
            .contains("top-level program")
        );
        assert!(
            build_account_uses_for_test(
                &decoded,
                &[],
                &[],
                &[],
                &[],
                &[1],
                &[],
                AccountCoverage::Complete,
                5,
                1,
                0,
            )
            .unwrap_err()
            .to_string()
            .contains("CPI program")
        );
    }

    #[test]
    fn optimized_account_assembly_and_page_bytes_match_bd22804_reference() {
        #[allow(clippy::too_many_arguments)]
        fn assert_case(
            message: &decode::DecodedMessage,
            loaded_writable: &[CompactPubkey],
            loaded_readonly: &[CompactPubkey],
            top_program_indexes: &[u8],
            top_account_indexes: &[u8],
            cpi_program_indexes: &[u32],
            cpi_account_indexes: &[u8],
            coverage: AccountCoverage,
        ) {
            let optimized = build_account_uses_for_test(
                message,
                loaded_writable,
                loaded_readonly,
                top_program_indexes,
                top_account_indexes,
                cpi_program_indexes,
                cpi_account_indexes,
                coverage,
                20,
                111,
                0,
            )
            .unwrap();
            let reference = build_account_uses_reference(
                message,
                loaded_writable,
                loaded_readonly,
                top_program_indexes,
                top_account_indexes,
                cpi_program_indexes,
                cpi_account_indexes,
                coverage,
                20,
                111,
                0,
            )
            .unwrap();
            assert_eq!(optimized, reference);

            let (source_flags, outcome, cpi_coverage) = match coverage {
                AccountCoverage::Complete => (
                    ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                    Outcome::Success,
                    CpiCoverage::Recorded,
                ),
                AccountCoverage::MissingLoadedMetadata => (
                    ARCHIVE_V2_TX_FLAG_MESSAGE_V0 | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                    Outcome::Unknown,
                    CpiCoverage::MissingMetadata,
                ),
                AccountCoverage::RawTransactionFallback => (
                    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                    Outcome::Unknown,
                    CpiCoverage::RawTransactionFallback,
                ),
                AccountCoverage::RawMetadataLoadedFallback => (
                    ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                        | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                        | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                    Outcome::Unknown,
                    CpiCoverage::RawMetadataFallback,
                ),
            };
            let optimized_ref_count = u32::try_from(optimized.len()).unwrap();
            let make_transaction = |accounts| ProjectedTransaction {
                source_flags,
                outcome,
                account_coverage: coverage,
                cpi_coverage,
                accounts,
            };
            let optimized_page =
                encode_page(index_row(0, 111, 1), &[make_transaction(optimized)]).unwrap();
            let reference_page =
                encode_page(index_row(0, 111, 1), &[make_transaction(reference)]).unwrap();
            assert_eq!(optimized_page, reference_page);

            let store = |page: &[u8]| {
                let compressed = zstd::bulk::compress(page, ZSTD_LEVEL).unwrap();
                if compressed.len() < page.len() {
                    (compressed, INDEX_ROW_FLAG_ZSTD)
                } else {
                    (page.to_vec(), 0)
                }
            };
            let (optimized_stored, optimized_flags) = store(&optimized_page);
            let (reference_stored, reference_flags) = store(&reference_page);
            assert_eq!(optimized_stored, reference_stored);
            assert_eq!(optimized_flags, reference_flags);
            let row = AccountIndexRow {
                block_id: 0,
                slot: 111,
                page_offset: 0,
                stored_len: u32::try_from(optimized_stored.len()).unwrap(),
                decoded_len: u32::try_from(optimized_page.len()).unwrap(),
                tx_count: 1,
                account_ref_count: optimized_ref_count,
                coverage_flags: coverage.bit() | cpi_coverage.bit(),
                flags: optimized_flags,
            };
            let directory = tempfile::tempdir().unwrap();
            let optimized_index = directory.path().join("optimized.index");
            let reference_index = directory.path().join("reference.index");
            for path in [&optimized_index, &reference_index] {
                write_index(
                    path,
                    &[row],
                    u64::try_from(optimized_stored.len()).unwrap(),
                    20,
                    CompactV2MessageSchema::Current,
                    CompactV2MetadataSchema::CurrentTypedError,
                )
                .unwrap();
            }
            assert_eq!(
                fs::read(optimized_index).unwrap(),
                fs::read(reference_index).unwrap()
            );
        }

        let message = decode::DecodedMessage {
            account_keys: vec![
                CompactPubkey::Id(1),
                CompactPubkey::Raw([1; 32]),
                CompactPubkey::Id(2),
                CompactPubkey::Raw([1; 32]),
                CompactPubkey::Id(3),
            ],
            is_v0: true,
            num_required_signatures: 2,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 1,
            instruction_count: 2,
            expected_loaded_writable: 2,
            expected_loaded_readonly: 2,
        };
        assert_case(
            &message,
            &[CompactPubkey::Id(2), CompactPubkey::Raw([2; 32])],
            &[CompactPubkey::Raw([1; 32]), CompactPubkey::Id(4)],
            &[5, 2, 5],
            &[0, 8, 3, 8],
            &[6, 1, 6],
            &[7, 0, 7],
            AccountCoverage::Complete,
        );
        assert_case(
            &message,
            &[],
            &[],
            &[6, 2],
            &[8, 0],
            &[7, 1],
            &[6, 0],
            AccountCoverage::MissingLoadedMetadata,
        );

        let invalid_message = decode::DecodedMessage {
            account_keys: vec![CompactPubkey::Id(1)],
            is_v0: false,
            num_required_signatures: 0,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
            instruction_count: 3,
            expected_loaded_writable: 0,
            expected_loaded_readonly: 0,
        };
        let optimized_error = build_account_uses_for_test(
            &invalid_message,
            &[],
            &[],
            &[1, 2, 1],
            &[],
            &[],
            &[],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        let reference_error = build_account_uses_reference(
            &invalid_message,
            &[],
            &[],
            &[1, 2, 1],
            &[],
            &[],
            &[],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(optimized_error, reference_error);
        assert!(optimized_error.contains("index 1 is outside 1 resolved"));

        let optimized_account_error = build_account_uses_for_test(
            &invalid_message,
            &[],
            &[],
            &[],
            &[1, 2, 1],
            &[],
            &[],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        let reference_account_error = build_account_uses_reference(
            &invalid_message,
            &[],
            &[],
            &[],
            &[1, 2, 1],
            &[],
            &[],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(optimized_account_error, reference_account_error);
        assert!(optimized_account_error.contains("account index 1 is outside 1 resolved"));

        let bad_id_message = decode::DecodedMessage {
            account_keys: vec![CompactPubkey::Id(21)],
            is_v0: false,
            num_required_signatures: 0,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
            instruction_count: 3,
            expected_loaded_writable: 0,
            expected_loaded_readonly: 0,
        };
        let optimized_bad_id = build_account_uses_for_test(
            &bad_id_message,
            &[],
            &[],
            &[1],
            &[],
            &[],
            &[],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        let reference_bad_id = build_account_uses_reference(
            &bad_id_message,
            &[],
            &[],
            &[1],
            &[],
            &[],
            &[],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(optimized_bad_id, reference_bad_id);
        assert!(optimized_bad_id.contains("pubkey id 21"));

        let optimized_index_precedence = build_account_uses_for_test(
            &invalid_message,
            &[],
            &[],
            &[1],
            &[2],
            &[],
            &[1],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        let reference_index_precedence = build_account_uses_reference(
            &invalid_message,
            &[],
            &[],
            &[1],
            &[2],
            &[],
            &[1],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(optimized_index_precedence, reference_index_precedence);
        assert!(optimized_index_precedence.contains("top-level program index 1"));

        let bad_header_and_id = decode::DecodedMessage {
            account_keys: vec![CompactPubkey::Id(21)],
            num_required_signatures: 1,
            num_readonly_signed_accounts: 2,
            ..invalid_message
        };
        let optimized_header = build_account_uses_for_test(
            &bad_header_and_id,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        let reference_header = build_account_uses_reference(
            &bad_header_and_id,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            AccountCoverage::Complete,
            20,
            111,
            0,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(optimized_header, reference_header);
        assert!(optimized_header.contains("readonly signed count"));
    }

    #[test]
    fn streamed_projection_defers_ids_behind_later_structural_errors() {
        let message = serialize(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 0,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![CompactPubkey::Id(21)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: Vec::new(),
            },
        ));
        let truncated = &message[..message.len() - 1];
        let error = project_transaction(
            projection_config(20),
            111,
            tx_row(truncated.len(), 0, 0, 0),
            truncated,
            &[],
        )
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("decode message"));
        assert!(!error.contains("pubkey id 21"));

        let error = project_transaction(
            projection_config(20),
            111,
            tx_row(message.len(), 1, ARCHIVE_V2_TX_FLAG_HAS_METADATA, 0),
            &message,
            &[0],
        )
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("decode metadata"), "{error}");
        assert!(!error.contains("pubkey id 21"));

        let v0 = serialize(&ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 0,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(2),
                writable_indexes: vec![0, 1],
                readonly_indexes: Vec::new(),
            }],
        }));
        let mismatched_metadata = serialize(&metadata(
            None,
            None,
            vec![CompactPubkey::Id(21)],
            Vec::new(),
        ));
        let error = project_transaction(
            projection_config(20),
            111,
            tx_row(
                v0.len(),
                mismatched_metadata.len(),
                ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                    | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                0,
            ),
            &v0,
            &mismatched_metadata,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("loaded writable count mismatch"));
        assert!(!error.contains("pubkey id 21"));
    }

    #[test]
    fn reused_zstd_context_matches_fresh_compression_for_consecutive_pages() {
        let pages = [
            vec![7_u8; 32_768],
            (0_u8..=255).cycle().take(65_537).collect(),
        ];
        let mut compressor = zstd::bulk::Compressor::new(ZSTD_LEVEL).unwrap();
        for page in pages {
            assert_eq!(
                compressor.compress(&page).unwrap(),
                zstd::bulk::compress(&page, ZSTD_LEVEL).unwrap()
            );
        }
    }

    #[test]
    fn page_decoder_is_exact_and_rejects_bad_ids_and_duplicate_keys() {
        let transactions = vec![ProjectedTransaction {
            source_flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            outcome: Outcome::Success,
            account_coverage: AccountCoverage::Complete,
            cpi_coverage: CpiCoverage::Recorded,
            accounts: vec![AccountUse {
                key: CompactPubkey::Id(1),
                roles: ROLE_SIGNER,
            }],
        }];
        let page = encode_page(index_row(7, 107, 1), &transactions).unwrap();
        assert_eq!(decode_page_exact(&page, 5, 1).unwrap(), (7, transactions));

        let mut trailing = page.clone();
        trailing.push(0);
        assert!(decode_page_exact(&trailing, 5, 1).is_err());

        let mut zero_id = page.clone();
        zero_id[40..44].copy_from_slice(&0_u32.to_le_bytes());
        assert!(decode_page_exact(&zero_id, 5, 1).is_err());
        let mut high_id = page.clone();
        high_id[40..44].copy_from_slice(&6_u32.to_le_bytes());
        assert!(decode_page_exact(&high_id, 5, 1).is_err());
        let mut unknown_source_flag = page.clone();
        let source_flags = u32::from_le_bytes(unknown_source_flag[28..32].try_into().unwrap());
        unknown_source_flag[28..32].copy_from_slice(&(source_flags | (1 << 31)).to_le_bytes());
        assert!(decode_page_exact(&unknown_source_flag, 5, 1).is_err());

        let duplicate_transactions = vec![ProjectedTransaction {
            source_flags: 0,
            outcome: Outcome::Unknown,
            account_coverage: AccountCoverage::Complete,
            cpi_coverage: CpiCoverage::MissingMetadata,
            accounts: vec![
                AccountUse {
                    key: CompactPubkey::Id(1),
                    roles: 0,
                },
                AccountUse {
                    key: CompactPubkey::Id(1),
                    roles: ROLE_WRITABLE,
                },
            ],
        }];
        let duplicate_page = encode_page(index_row(0, 100, 1), &duplicate_transactions).unwrap();
        assert!(decode_page_exact(&duplicate_page, 5, 1).is_err());
    }

    #[test]
    fn index_decoder_rejects_bad_geometry_and_offsets() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("index");
        let rows = vec![
            AccountIndexRow {
                block_id: 0,
                slot: 100,
                page_offset: 0,
                stored_len: 10,
                decoded_len: 12,
                tx_count: 1,
                account_ref_count: 2,
                coverage_flags: AccountCoverage::Complete.bit() | CpiCoverage::Recorded.bit(),
                flags: INDEX_ROW_FLAG_ZSTD,
            },
            AccountIndexRow {
                block_id: 1,
                slot: 101,
                page_offset: 10,
                stored_len: 8,
                decoded_len: 8,
                tx_count: 0,
                account_ref_count: 0,
                coverage_flags: 0,
                flags: 0,
            },
        ];
        write_index(
            &path,
            &rows,
            18,
            5,
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::CurrentTypedError,
        )
        .unwrap();
        let bytes = fs::read(&path).unwrap();
        assert_eq!(
            decode_index_exact(
                &bytes,
                18,
                5,
                CompactV2MessageSchema::Current,
                CompactV2MetadataSchema::CurrentTypedError,
            )
            .unwrap(),
            rows
        );

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(
            decode_index_exact(
                &trailing,
                18,
                5,
                CompactV2MessageSchema::Current,
                CompactV2MetadataSchema::CurrentTypedError,
            )
            .is_err()
        );
        let mut bad_offset = bytes;
        let second_offset_start = INDEX_HEADER_LEN + INDEX_ROW_LEN + 12;
        bad_offset[second_offset_start..second_offset_start + 8]
            .copy_from_slice(&11_u64.to_le_bytes());
        assert!(
            decode_index_exact(
                &bad_offset,
                18,
                5,
                CompactV2MessageSchema::Current,
                CompactV2MetadataSchema::CurrentTypedError,
            )
            .is_err()
        );
    }

    #[test]
    fn saved_output_validator_bounds_lengths_and_counts_before_allocation() {
        let directory = tempfile::tempdir().unwrap();
        let pages_path = directory.path().join("pages");
        let index_path = directory.path().join("index");
        let transaction = ProjectedTransaction {
            source_flags: 0,
            outcome: Outcome::Unknown,
            account_coverage: AccountCoverage::Complete,
            cpi_coverage: CpiCoverage::MissingMetadata,
            accounts: vec![AccountUse {
                key: CompactPubkey::Id(1),
                roles: ROLE_SIGNER,
            }],
        };
        let source_row = index_row(0, 100, 1);
        let page = encode_page(source_row, std::slice::from_ref(&transaction)).unwrap();
        fs::write(&pages_path, &page).unwrap();
        let rows = [AccountIndexRow {
            block_id: 0,
            slot: 100,
            page_offset: 0,
            stored_len: page.len() as u32,
            decoded_len: page.len() as u32,
            tx_count: 1,
            account_ref_count: 1,
            coverage_flags: transaction_coverage_bits(&transaction),
            flags: 0,
        }];
        write_index(
            &index_path,
            &rows,
            page.len() as u64,
            5,
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::CurrentTypedError,
        )
        .unwrap();
        validate_saved_output(
            &pages_path,
            &index_path,
            &[source_row],
            &rows,
            5,
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::CurrentTypedError,
        )
        .unwrap();

        let valid_index = fs::read(&index_path).unwrap();
        let mut huge_decoded = valid_index.clone();
        let decoded_len_start = INDEX_HEADER_LEN + 24;
        huge_decoded[decoded_len_start..decoded_len_start + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&index_path, huge_decoded).unwrap();
        let mut huge_expected = rows;
        huge_expected[0].decoded_len = u32::MAX;
        assert!(
            validate_saved_output(
                &pages_path,
                &index_path,
                &[source_row],
                &huge_expected,
                5,
                CompactV2MessageSchema::Current,
                CompactV2MetadataSchema::CurrentTypedError,
            )
            .unwrap_err()
            .to_string()
            .contains("decoded length")
        );

        fs::write(&index_path, valid_index).unwrap();
        let mut huge_tx_count = page;
        huge_tx_count[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&pages_path, huge_tx_count).unwrap();
        let error = decode_page_exact(&fs::read(&pages_path).unwrap(), 5, 1).unwrap_err();
        assert!(format!("{error:#}").contains("transactions, expected"));
    }

    #[test]
    fn split_frames_cover_255_256_and_257_transaction_boundaries() {
        for transaction_count in [255_u32, 256, 257] {
            let directory = tempfile::tempdir().unwrap();
            let mut scratch = SplitWorkerScratch::default();
            scratch.begin_block();
            for _ in 0..transaction_count {
                scratch.record_missing_metadata(0).unwrap();
            }
            scratch
                .finish_block_transactions(transaction_count)
                .unwrap();
            scratch.record_block_rewards(&[0]).unwrap();
            let row = index_row(0, 88, transaction_count);
            let mut compressor = zstd::bulk::Compressor::new(ZSTD_LEVEL).unwrap();
            let split = encode_split_effects(&mut scratch, &mut compressor, row).unwrap();
            let mut writers = SplitWriters::create(
                directory.path(),
                SplitHeaderBinding {
                    epoch: 2,
                    slots_per_epoch: 100,
                    selected_blocks: 1,
                    selected_transactions: u64::from(transaction_count),
                    message_schema: CompactV2MessageSchema::Current,
                    metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
                    prefix: true,
                },
            )
            .unwrap();
            writers.append(row, split).unwrap();
            let summary = writers.finish(1).unwrap();
            assert_eq!(
                summary.index_bytes,
                (SPLIT_FILE_HEADER_LEN + SPLIT_INDEX_ROW_LEN) as u64
            );
            assert_split_index_geometry(directory.path(), &[(0, 88, transaction_count)]);
            let frames = read_split_frames(
                &directory
                    .path()
                    .join(SplitPlane::MetadataStates.file_name()),
                SplitPlane::MetadataStates,
            );
            let expected_frame_count = usize::from(transaction_count == 257) + 1;
            assert_eq!(frames.len(), expected_frame_count);
            assert_eq!(frames[0].first_tx, 0);
            assert_eq!(frames[0].tx_count, transaction_count.min(256));
            assert_eq!(frames[0].dense_count, transaction_count.min(256));
            assert_eq!(
                frames[0].decoded.len(),
                transaction_count.min(256) as usize * 2
            );
            assert_eq!(frames[0].flags, SPLIT_FRAME_FLAG_ZSTD);
            if transaction_count == 257 {
                assert_eq!(frames[1].first_tx, 256);
                assert_eq!(frames[1].tx_count, 1);
                assert_eq!(frames[1].dense_count, 1);
                assert_eq!(frames[1].decoded, [0, 0]);
            }
            for plane in SplitPlane::ALL {
                if plane != SplitPlane::MetadataStates {
                    assert_eq!(
                        fs::metadata(directory.path().join(plane.file_name()))
                            .unwrap()
                            .len(),
                        SPLIT_FILE_HEADER_LEN as u64
                    );
                }
            }
        }
    }

    #[test]
    fn block_reward_frames_preserve_present_exact_bytes_and_small_raw_storage() {
        let encoded_rewards = serialize(&Some(ArchiveV2HotRewards {
            num_partitions: Some(2),
            decoded: vec![
                CompactReward {
                    pubkey: CompactPubkey::Id(7),
                    lamports: 8,
                    post_balance: 9,
                    reward_type: 10,
                    commission: Some(11),
                },
                CompactReward {
                    pubkey: CompactPubkey::Raw([12; 32]),
                    lamports: -13,
                    post_balance: 14,
                    reward_type: 15,
                    commission: None,
                },
            ],
        }));
        // Same logical Some(empty) value with a padded zero LEB128 vector
        // length. The measurement container must preserve these source bytes.
        let noncanonical_empty = vec![1, 0, 0x80, 0];
        for (case, exact) in [
            ("noncanonical-empty", noncanonical_empty),
            ("id-and-raw", encoded_rewards),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut scratch = SplitWorkerScratch::default();
            scratch.begin_block();
            scratch.finish_block_transactions(0).unwrap();
            scratch.record_block_rewards(&exact).unwrap();
            let row = index_row(0, 77, 0);
            let mut compressor = zstd::bulk::Compressor::new(ZSTD_LEVEL).unwrap();
            let split = encode_split_effects(&mut scratch, &mut compressor, row).unwrap();
            let mut writers = SplitWriters::create(
                directory.path(),
                SplitHeaderBinding {
                    epoch: 3,
                    slots_per_epoch: 100,
                    selected_blocks: 1,
                    selected_transactions: 0,
                    message_schema: CompactV2MessageSchema::Current,
                    metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
                    prefix: true,
                },
            )
            .unwrap();
            writers.append(row, split).unwrap();
            writers.finish(1).unwrap();
            let frames = read_split_frames(
                &directory.path().join(SplitPlane::BlockRewards.file_name()),
                SplitPlane::BlockRewards,
            );
            assert_eq!(frames.len(), 1, "{case}");
            assert_eq!(frames[0].first_tx, 0);
            assert_eq!(frames[0].tx_count, 0);
            assert_eq!(frames[0].dense_count, 1);
            assert_eq!(frames[0].decoded, exact);
            if case == "noncanonical-empty" {
                assert_eq!(frames[0].flags, 0);
            }
        }
    }

    #[test]
    fn split_state_lane_and_sparse_records_cover_all_presence_states() {
        fn record_decoded(scratch: &mut SplitWorkerScratch, value: &CompactMetaV1) {
            let encoded = serialize(value);
            let effects = decode::stream_metadata_effects_with_schema(
                &mut encoded.as_slice(),
                CompactV2MetadataSchema::CurrentTypedError,
                MetadataDecodeLimits {
                    total_message_accounts: 1,
                    top_level_instruction_count: 1,
                },
                |_| Ok::<(), anyhow::Error>(()),
            )
            .unwrap();
            scratch.record_decoded_metadata(0, &effects).unwrap();
        }

        let mut scratch = SplitWorkerScratch::default();
        scratch.begin_block();
        scratch.record_missing_metadata(0).unwrap();
        scratch
            .record_raw_metadata(ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, &[0xaa, 0xbb])
            .unwrap();
        record_decoded(&mut scratch, &metadata(None, None, Vec::new(), Vec::new()));
        let mut empty_present = metadata(None, Some(Vec::new()), Vec::new(), Vec::new());
        empty_present.logs = Some(CompactLogStream {
            events: Vec::new(),
            strings: StringTable::default(),
            data: DataTable::default(),
        });
        record_decoded(&mut scratch, &empty_present);
        let mut dense = metadata(
            None,
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 0,
                    accounts: vec![0],
                    data: vec![1],
                    stack_height: Some(2),
                }],
            }]),
            Vec::new(),
            Vec::new(),
        );
        dense.pre_token_balances.push(CompactTokenBalance {
            account_index: 0,
            mint: Some(CompactPubkey::Id(1)),
            owner: None,
            program_id: Some(CompactPubkey::Raw([2; 32])),
            amount: 3,
            decimals: 4,
        });
        dense.rewards.push(CompactReward {
            pubkey: CompactPubkey::Raw([5; 32]),
            lamports: 6,
            post_balance: 7,
            reward_type: 8,
            commission: None,
        });
        record_decoded(&mut scratch, &dense);
        scratch.finish_block_transactions(5).unwrap();

        assert_eq!(
            scratch.raw[SplitPlane::MetadataStates.index()],
            [
                0,
                0,
                2 | (1 << 2),
                0,
                1,
                1 | (1 << 3) | (1 << 4),
                1,
                2 | (1 << 3) | (1 << 4) | (1 << 6),
                1,
                3 | (1 << 3) | (1 << 4) | (1 << 5) | (1 << 7),
            ]
        );
        assert_eq!(
            scratch.raw[SplitPlane::RawMetadataFallbacks.index()],
            [2, 0, 0, 0, 0xaa, 0xbb]
        );
        assert_eq!(
            scratch.chunks[SplitPlane::Outcomes.index()][0].dense_count,
            3
        );
        assert_eq!(
            scratch.chunks[SplitPlane::Balances.index()][0].dense_count,
            3
        );
        assert_eq!(
            scratch.chunks[SplitPlane::InnerInstructions.index()][0].dense_count,
            1
        );
        assert_eq!(scratch.chunks[SplitPlane::Logs.index()][0].dense_count, 1);
        assert_eq!(
            scratch.chunks[SplitPlane::TokenBalances.index()][0].dense_count,
            1
        );
        assert_eq!(
            scratch.chunks[SplitPlane::TransactionRewards.index()][0].dense_count,
            1
        );
    }

    #[test]
    fn raw_transaction_with_decoded_metadata_preserves_nonempty_effects_structurally() {
        let metadata_value = CompactMetaV1 {
            err: None,
            fee: 5,
            pre_balances: vec![1],
            post_balances: vec![2],
            inner_instructions: Some(vec![CompactInnerInstructions {
                index: 300,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 500,
                    accounts: vec![255],
                    data: vec![3],
                    stack_height: Some(2),
                }],
            }]),
            logs: Some(CompactLogStream {
                events: vec![
                    LogEvent::LoaderUpgradedProgram {
                        program: CompactPubkey::Id(3),
                    },
                    LogEvent::Success {
                        program: CompactPubkey::Raw([4; 32]),
                    },
                ],
                strings: StringTable::default(),
                data: DataTable::default(),
            }),
            pre_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(1)),
                owner: Some(CompactPubkey::Raw([2; 32])),
                program_id: None,
                amount: 4,
                decimals: 5,
            }],
            post_token_balances: Vec::new(),
            rewards: vec![CompactReward {
                pubkey: CompactPubkey::Raw([6; 32]),
                lamports: 7,
                post_balance: 8,
                reward_type: 9,
                commission: None,
            }],
            loaded_writable_addresses: vec![CompactPubkey::Id(2)],
            loaded_readonly_addresses: Vec::new(),
            return_data: Some(CompactReturnData {
                program_id: CompactPubkey::Raw([10; 32]),
                data: vec![11],
            }),
            compute_units_consumed: Some(12),
            cost_units: Some(13),
        };
        let metadata = serialize(&metadata_value);
        let mut worker = ProjectionWorker::new_with_split(projection_config(20)).unwrap();
        worker.split.as_mut().unwrap().begin_block();
        let mut timing = ProjectionTiming::default();
        let projected = project_transaction_timed(
            &mut worker,
            99,
            tx_row(
                0,
                metadata.len(),
                ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                    | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                0,
            ),
            &[],
            &metadata,
            &mut timing,
        )
        .unwrap();
        assert_eq!(projected.outcome, Outcome::Success);
        assert_eq!(
            projected.account_coverage,
            AccountCoverage::RawTransactionFallback
        );
        assert!(projected.accounts.is_empty());
        let split = worker.split.as_mut().unwrap();
        split.finish_block_transactions(1).unwrap();
        assert_eq!(
            split.raw[SplitPlane::MetadataStates.index()],
            [
                1 | (1 << 2),
                3 | (1 << 3) | (1 << 4) | (1 << 5) | (1 << 6) | (1 << 7)
            ]
        );
        for plane in [
            SplitPlane::InnerInstructions,
            SplitPlane::Logs,
            SplitPlane::TokenBalances,
            SplitPlane::Balances,
            SplitPlane::Outcomes,
            SplitPlane::TransactionRewards,
        ] {
            assert_eq!(split.chunks[plane.index()].len(), 1, "{}", plane.name());
            assert_eq!(split.chunks[plane.index()][0].dense_count, 1);
        }
        assert!(split.chunks[SplitPlane::RawMetadataFallbacks.index()].is_empty());
        assert_eq!(
            split.raw[SplitPlane::InnerInstructions.index()],
            serialize(&metadata_value.inner_instructions)
        );
        assert_eq!(
            split.raw[SplitPlane::Logs.index()],
            serialize(&metadata_value.logs)
        );
        assert_eq!(
            split.raw[SplitPlane::TransactionRewards.index()],
            serialize(&metadata_value.rewards)
        );
        let pre_token = serialize(&metadata_value.pre_token_balances);
        let post_token = serialize(&metadata_value.post_token_balances);
        let mut expected_token = Vec::new();
        expected_token.extend_from_slice(&(pre_token.len() as u32).to_le_bytes());
        expected_token.extend_from_slice(&(post_token.len() as u32).to_le_bytes());
        expected_token.extend_from_slice(&pre_token);
        expected_token.extend_from_slice(&post_token);
        assert_eq!(split.raw[SplitPlane::TokenBalances.index()], expected_token);
        let pre_balances = serialize(&metadata_value.pre_balances);
        let post_balances = serialize(&metadata_value.post_balances);
        let mut expected_balances = Vec::new();
        expected_balances.extend_from_slice(&(pre_balances.len() as u32).to_le_bytes());
        expected_balances.extend_from_slice(&(post_balances.len() as u32).to_le_bytes());
        expected_balances.extend_from_slice(&pre_balances);
        expected_balances.extend_from_slice(&post_balances);
        assert_eq!(split.raw[SplitPlane::Balances.index()], expected_balances);
        let mut outcome_head = serialize(&metadata_value.err);
        outcome_head.extend(serialize(&metadata_value.fee));
        let mut outcome_tail = serialize(&metadata_value.return_data);
        outcome_tail.extend(serialize(&metadata_value.compute_units_consumed));
        outcome_tail.extend(serialize(&metadata_value.cost_units));
        let mut expected_outcome = Vec::new();
        expected_outcome.extend_from_slice(&(outcome_head.len() as u32).to_le_bytes());
        expected_outcome.extend_from_slice(&(outcome_tail.len() as u32).to_le_bytes());
        expected_outcome.extend_from_slice(&outcome_head);
        expected_outcome.extend_from_slice(&outcome_tail);
        assert_eq!(split.raw[SplitPlane::Outcomes.index()], expected_outcome);

        let block_rewards = serialize(&Some(ArchiveV2HotRewards {
            num_partitions: Some(2),
            decoded: vec![
                CompactReward {
                    pubkey: CompactPubkey::Id(7),
                    lamports: 8,
                    post_balance: 9,
                    reward_type: 10,
                    commission: Some(11),
                },
                CompactReward {
                    pubkey: CompactPubkey::Raw([12; 32]),
                    lamports: -13,
                    post_balance: 14,
                    reward_type: 15,
                    commission: None,
                },
            ],
        }));
        split.record_block_rewards(&block_rewards).unwrap();
        assert_eq!(split.raw[SplitPlane::BlockRewards.index()], block_rewards);
    }

    #[test]
    fn lean_directory_is_headerless_and_preserves_exact_effect_records() {
        let mut metadata_value = metadata(
            Some(CompactTransactionError::AccountInUse),
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 0,
                    accounts: vec![0],
                    data: vec![1, 2],
                    stack_height: Some(2),
                }],
            }]),
            Vec::new(),
            Vec::new(),
        );
        metadata_value.pre_balances = vec![3, 4];
        metadata_value.post_balances = vec![5, 6];
        metadata_value.logs = Some(CompactLogStream {
            events: vec![
                LogEvent::LoaderUpgradedProgram {
                    program: CompactPubkey::Id(3),
                },
                LogEvent::Success {
                    program: CompactPubkey::Raw([4; 32]),
                },
            ],
            strings: StringTable::default(),
            data: DataTable::default(),
        });
        metadata_value.pre_token_balances.push(CompactTokenBalance {
            account_index: 0,
            mint: Some(CompactPubkey::Id(5)),
            owner: Some(CompactPubkey::Raw([6; 32])),
            program_id: None,
            amount: 7,
            decimals: 8,
        });
        metadata_value
            .post_token_balances
            .push(CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(5)),
                owner: None,
                program_id: Some(CompactPubkey::Raw([9; 32])),
                amount: 10,
                decimals: 11,
            });
        metadata_value.rewards.push(CompactReward {
            pubkey: CompactPubkey::Raw([12; 32]),
            lamports: 13,
            post_balance: 14,
            reward_type: 15,
            commission: Some(16),
        });
        metadata_value.return_data = Some(CompactReturnData {
            program_id: CompactPubkey::Id(7),
            data: vec![17, 18],
        });
        metadata_value.compute_units_consumed = Some(19);
        metadata_value.cost_units = Some(20);
        let encoded_metadata = serialize(&metadata_value);
        let mut metadata_cursor = encoded_metadata.as_slice();
        let effects = decode::stream_metadata_effects_with_schema(
            &mut metadata_cursor,
            CompactV2MetadataSchema::CurrentTypedError,
            MetadataDecodeLimits {
                total_message_accounts: 2,
                top_level_instruction_count: 1,
            },
            |_| Ok::<(), anyhow::Error>(()),
        )
        .unwrap();
        assert!(metadata_cursor.is_empty());

        let raw_flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK
            | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK;
        let decoded_flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_ERROR;
        let mut scratch = LeanWorkerScratch::default();
        scratch.begin_block();
        scratch.record_missing_metadata(0).unwrap();
        scratch
            .record_raw_metadata(1, raw_flags, &[0xaa, 0xbb])
            .unwrap();
        scratch
            .record_decoded_metadata(2, decoded_flags, &effects)
            .unwrap();
        scratch.finish_block(3).unwrap();
        let exact_block_rewards = serialize(&Some(ArchiveV2HotRewards {
            num_partitions: None,
            decoded: vec![CompactReward {
                pubkey: CompactPubkey::Id(8),
                lamports: 21,
                post_balance: 22,
                reward_type: 23,
                commission: None,
            }],
        }));
        scratch.record_block_rewards(&exact_block_rewards).unwrap();

        let inner = serialize(&metadata_value.inner_instructions);
        let logs = serialize(&metadata_value.logs);
        let mut token = serialize(&metadata_value.pre_token_balances);
        token.extend(serialize(&metadata_value.post_token_balances));
        let mut balances = serialize(&metadata_value.pre_balances);
        balances.extend(serialize(&metadata_value.post_balances));
        let mut outcome = serialize(&metadata_value.err);
        outcome.extend(serialize(&metadata_value.fee));
        outcome.extend(serialize(&metadata_value.return_data));
        outcome.extend(serialize(&metadata_value.compute_units_consumed));
        outcome.extend(serialize(&metadata_value.cost_units));
        assert_eq!(scratch.raw[LeanObject::InnerInstructions.index()], inner);
        assert_eq!(scratch.raw[LeanObject::Logs.index()], logs);
        assert_eq!(scratch.raw[LeanObject::TokenBalances.index()], token);
        assert_eq!(scratch.raw[LeanObject::Balances.index()], balances);
        assert_eq!(scratch.raw[LeanObject::Outcomes.index()], outcome);
        let mut expected_rewards = 2_u32.to_le_bytes().to_vec();
        expected_rewards.extend(serialize(&metadata_value.rewards));
        assert_eq!(
            scratch.raw[LeanObject::TransactionRewards.index()],
            expected_rewards
        );
        assert_eq!(
            scratch.raw[LeanObject::RawMetadataFallbacks.index()],
            [1, 0, 0, 0, 2, 0, 0, 0, 0xaa, 0xbb]
        );
        assert_eq!(
            scratch.raw[LeanObject::BlockRewards.index()],
            exact_block_rewards
        );

        let directory = &scratch.raw[LeanObject::TransactionDirectory.index()];
        assert_eq!(directory.len(), 3 * LEAN_DIRECTORY_ROW_LEN);
        let expected_ends = [
            inner.len(),
            logs.len(),
            token.len(),
            balances.len(),
            outcome.len(),
        ]
        .map(|length| u32::try_from(length).unwrap());
        let mut cursor = directory.as_slice();
        for (flags, state, ends) in [
            (0_u16, 0_u8, [0_u32; 5]),
            (u16::try_from(raw_flags).unwrap(), 0_u8, [0_u32; 5]),
            (
                u16::try_from(decoded_flags).unwrap(),
                0xfb_u8,
                expected_ends,
            ),
        ] {
            assert_eq!(read_u16(&mut cursor).unwrap(), flags);
            assert_eq!(take(&mut cursor, 1).unwrap(), [state]);
            assert_eq!(take(&mut cursor, 1).unwrap(), [0]);
            for end in ends {
                assert_eq!(read_u32(&mut cursor).unwrap(), end);
            }
        }
        assert!(cursor.is_empty());
        assert_eq!(
            scratch.record_counts[LeanObject::TransactionDirectory.index()],
            3
        );
    }

    #[test]
    fn lean_legacy_bytes_and_hybrid_object_policy_are_exact() {
        let make_scratch = || {
            let mut scratch = LeanWorkerScratch::default();
            scratch.raw[LeanObject::TransactionDirectory.index()] = vec![0x11; 1_024];
            scratch.raw[LeanObject::InnerInstructions.index()] = vec![0x22];
            scratch.raw[LeanObject::Logs.index()] = vec![0x33; 1_026];
            scratch.raw[LeanObject::TokenBalances.index()] = vec![0x44; 1_027];
            scratch.raw[LeanObject::Balances.index()] = vec![0x55; 1_028];
            scratch.raw[LeanObject::Outcomes.index()] = vec![0x66; 1_029];
            scratch.raw[LeanObject::TransactionRewards.index()] = vec![0x77];
            scratch.raw[LeanObject::RawMetadataFallbacks.index()] = vec![0x88; 2_048];
            scratch.raw[LeanObject::BlockRewards.index()] = vec![0];
            scratch
        };

        for mode in [
            LeanCompressionArg::Raw,
            LeanCompressionArg::Zstd,
            LeanCompressionArg::Adaptive,
            LeanCompressionArg::Hybrid,
        ] {
            let mut scratch = make_scratch();
            let source: [Vec<u8>; LEAN_OBJECT_COUNT] =
                std::array::from_fn(|index| scratch.raw[index].clone());
            let expected_zstd: [Vec<u8>; LEAN_OBJECT_COUNT] = std::array::from_fn(|index| {
                zstd::bulk::compress(&source[index], ZSTD_LEVEL).unwrap()
            });
            let mut compressor = zstd::bulk::Compressor::new(ZSTD_LEVEL).unwrap();
            let encoded = encode_lean_effects(&mut scratch, &mut compressor, mode).unwrap();
            for object in LeanObject::ALL {
                let index = object.index();
                let expected_compressed = match mode {
                    LeanCompressionArg::Raw => false,
                    LeanCompressionArg::Zstd => true,
                    LeanCompressionArg::Adaptive => {
                        expected_zstd[index].len() < source[index].len()
                    }
                    LeanCompressionArg::Hybrid => match object {
                        LeanObject::TransactionDirectory
                        | LeanObject::InnerInstructions
                        | LeanObject::Logs
                        | LeanObject::TokenBalances
                        | LeanObject::Balances
                        | LeanObject::Outcomes => true,
                        LeanObject::TransactionRewards | LeanObject::RawMetadataFallbacks => {
                            expected_zstd[index].len() < source[index].len()
                        }
                        LeanObject::BlockRewards => false,
                    },
                };
                let expected = if expected_compressed {
                    expected_zstd[index].as_slice()
                } else {
                    source[index].as_slice()
                };
                assert_eq!(
                    &encoded.packed[encoded.ranges[index].clone()],
                    expected,
                    "{} {} bytes",
                    mode.name(),
                    object.name()
                );
                assert_eq!(
                    encoded.compressed[index],
                    expected_compressed,
                    "{} {} codec",
                    mode.name(),
                    object.name()
                );
            }
            if mode == LeanCompressionArg::Hybrid {
                assert_eq!(
                    encoded.stats.objects[LeanObject::BlockRewards.index()].compression_time,
                    Duration::ZERO
                );
                assert!(!encoded.compressed[LeanObject::TransactionRewards.index()]);
                assert!(encoded.compressed[LeanObject::RawMetadataFallbacks.index()]);
                assert!(!encoded.compressed[LeanObject::BlockRewards.index()]);
                assert!(encoded.compressed[LeanObject::InnerInstructions.index()]);
                assert!(
                    encoded.ranges[LeanObject::InnerInstructions.index()].len()
                        >= source[LeanObject::InnerInstructions.index()].len()
                );
                for object in LeanObject::ALL {
                    assert_eq!(
                        encoded.stats.objects[object.index()].compression_attempts,
                        u64::from(object != LeanObject::BlockRewards),
                        "{} compression attempts",
                        object.name()
                    );
                }
            }
        }
    }

    #[test]
    fn lean_zstd_levels_have_frozen_level_one_wire_and_distinct_nondefault_bytes() {
        let binding = SplitHeaderBinding {
            epoch: 7,
            slots_per_epoch: 100,
            selected_blocks: 1,
            selected_transactions: 1,
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
            prefix: true,
        };
        let mut default_header = Vec::new();
        write_lean_file_header(
            &mut default_header,
            LEAN_INDEX_MAGIC,
            u16::MAX,
            binding,
            LeanCompressionArg::Raw,
            LeanZstdLevelArg::One,
        )
        .unwrap();
        assert_eq!(
            default_header,
            decode_golden_hex(concat!(
                "425a56324c4930310100ffff00000001",
                "07000000000000006400000000000000",
                "01000000000000000100000000000000",
                "01180005020900000000000000000000",
            ))
        );

        let mut legacy_scratch = LeanWorkerScratch::default();
        legacy_scratch.raw[LeanObject::TransactionDirectory.index()] = vec![0x11; 1_024];
        let mut legacy_compressor = zstd::bulk::Compressor::new(1).unwrap();
        legacy_compressor.compress(b"account-page-prelude").unwrap();
        let legacy = encode_lean_effects(
            &mut legacy_scratch,
            &mut legacy_compressor,
            LeanCompressionArg::Zstd,
        )
        .unwrap();
        let legacy_directory =
            &legacy.packed[legacy.ranges[LeanObject::TransactionDirectory.index()].clone()];
        assert_eq!(
            legacy_directory,
            decode_golden_hex("28b52ffd6000034d00001011110100fb2b8005")
        );

        let mut patterned = Vec::new();
        for index in 0..4_096_u32 {
            patterned.extend_from_slice(b"archive-v2 transaction metadata effects ");
            patterned.extend_from_slice(&(index % 257).to_le_bytes());
            patterned.extend(std::iter::repeat_n(
                ((index * 17) % 251) as u8,
                (index as usize % 64) + 1,
            ));
        }
        let mut level_outputs = Vec::new();
        for level in [
            LeanZstdLevelArg::One,
            LeanZstdLevelArg::Three,
            LeanZstdLevelArg::Five,
            LeanZstdLevelArg::Nine,
        ] {
            let mut header = Vec::new();
            write_lean_file_header(
                &mut header,
                LEAN_DATA_MAGIC,
                LeanObject::Logs as u16,
                binding,
                LeanCompressionArg::Hybrid,
                level,
            )
            .unwrap();
            assert_eq!(header[54], level.header_code());
            assert_eq!(&header[55..], &[0; 9]);

            let expected_zstd = zstd::bulk::compress(&patterned, level.level()).unwrap();
            assert!(expected_zstd.len() < patterned.len());
            for mode in [
                LeanCompressionArg::Zstd,
                LeanCompressionArg::Adaptive,
                LeanCompressionArg::Hybrid,
            ] {
                let mut scratch = LeanWorkerScratch::default();
                scratch.raw[LeanObject::Logs.index()] = patterned.clone();
                scratch.raw[LeanObject::BlockRewards.index()] = vec![0];
                let mut compressor = zstd::bulk::Compressor::new(level.level()).unwrap();
                let encoded = encode_lean_effects(&mut scratch, &mut compressor, mode).unwrap();
                assert!(encoded.compressed[LeanObject::Logs.index()]);
                assert_eq!(
                    &encoded.packed[encoded.ranges[LeanObject::Logs.index()].clone()],
                    expected_zstd
                );
                if mode == LeanCompressionArg::Hybrid {
                    assert!(!encoded.compressed[LeanObject::BlockRewards.index()]);
                    assert_eq!(
                        encoded.stats.objects[LeanObject::BlockRewards.index()]
                            .compression_attempts,
                        0
                    );
                }
            }
            level_outputs.push(expected_zstd);
        }
        for left in 0..level_outputs.len() {
            for right in left + 1..level_outputs.len() {
                assert_ne!(level_outputs[left], level_outputs[right]);
            }
        }
    }

    #[test]
    fn lean_block_chunks_have_one_locator_per_block_and_strict_codec_modes() {
        for mode in [
            LeanCompressionArg::Raw,
            LeanCompressionArg::Zstd,
            LeanCompressionArg::Adaptive,
            LeanCompressionArg::Hybrid,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut scratch = LeanWorkerScratch::default();
            scratch.begin_block();
            for _ in 0..257 {
                scratch.record_missing_metadata(0).unwrap();
            }
            scratch.finish_block(257).unwrap();
            scratch.record_block_rewards(&[0]).unwrap();
            let raw_live = scratch.raw_length().unwrap();
            let directory_zstd_len = zstd::bulk::compress(
                &scratch.raw[LeanObject::TransactionDirectory.index()],
                ZSTD_LEVEL,
            )
            .unwrap()
            .len();
            let mut compressor = zstd::bulk::Compressor::new(ZSTD_LEVEL).unwrap();
            let lean = encode_lean_effects(&mut scratch, &mut compressor, mode).unwrap();
            if mode != LeanCompressionArg::Raw {
                assert!(
                    lean.stats.max_live_scratch_bytes >= raw_live + directory_zstd_len,
                    "live scratch peak lost an earlier compressed object"
                );
            }
            let mut writers = LeanWriters::create(
                directory.path(),
                SplitHeaderBinding {
                    epoch: 2,
                    slots_per_epoch: 100,
                    selected_blocks: 1,
                    selected_transactions: 257,
                    message_schema: CompactV2MessageSchema::Current,
                    metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
                    prefix: true,
                },
                mode,
                LeanZstdLevelArg::One,
            )
            .unwrap();
            writers.append(index_row(0, 88, 257), lean).unwrap();
            let summary = writers.finish(1).unwrap();
            assert_eq!(
                summary.index_bytes,
                (LEAN_FILE_HEADER_LEN + LEAN_INDEX_ROW_LEN) as u64
            );
            let blocks = read_lean_output(directory.path(), mode, 2, 100, true, &[(0, 88, 257)]);
            assert_eq!(blocks.len(), 1);
            let block = &blocks[0];
            assert_eq!((block.block_id, block.slot, block.tx_count), (0, 88, 257));
            let directory_bytes = &block.decoded[LeanObject::TransactionDirectory.index()];
            assert_eq!(directory_bytes.len(), 257 * LEAN_DIRECTORY_ROW_LEN);
            assert!(directory_bytes.iter().all(|&byte| byte == 0));
            assert_eq!(block.decoded[LeanObject::BlockRewards.index()], [0]);
            for object in LeanObject::ALL {
                if !matches!(
                    object,
                    LeanObject::TransactionDirectory | LeanObject::BlockRewards
                ) {
                    assert!(
                        block.decoded[object.index()].is_empty(),
                        "{}",
                        object.name()
                    );
                    assert!(!block.compressed[object.index()]);
                }
            }
            match mode {
                LeanCompressionArg::Raw => {
                    assert!(!block.compressed[LeanObject::TransactionDirectory.index()]);
                    assert!(!block.compressed[LeanObject::BlockRewards.index()]);
                }
                LeanCompressionArg::Zstd => {
                    assert!(block.compressed[LeanObject::TransactionDirectory.index()]);
                    assert!(block.compressed[LeanObject::BlockRewards.index()]);
                }
                LeanCompressionArg::Adaptive => {
                    assert!(block.compressed[LeanObject::TransactionDirectory.index()]);
                    assert!(!block.compressed[LeanObject::BlockRewards.index()]);
                }
                LeanCompressionArg::Hybrid => {
                    assert!(block.compressed[LeanObject::TransactionDirectory.index()]);
                    assert!(!block.compressed[LeanObject::BlockRewards.index()]);
                }
            }
        }
    }

    #[test]
    fn lean_decoded_none_empty_and_noncanonical_empty_fields_advance_dense_ends() {
        let none = metadata(None, None, Vec::new(), Vec::new());
        let mut present_empty = metadata(None, Some(Vec::new()), Vec::new(), Vec::new());
        present_empty.logs = Some(CompactLogStream {
            events: Vec::new(),
            strings: StringTable::default(),
            data: DataTable::default(),
        });
        let canonical_none = serialize(&none);
        let canonical_present_empty = serialize(&present_empty);
        let (pre_token_range, reward_range) = {
            let mut cursor = canonical_none.as_slice();
            let effects = decode::stream_metadata_effects_with_schema(
                &mut cursor,
                CompactV2MetadataSchema::CurrentTypedError,
                MetadataDecodeLimits {
                    total_message_accounts: 0,
                    top_level_instruction_count: 0,
                },
                |_| Ok::<(), anyhow::Error>(()),
            )
            .unwrap();
            assert!(cursor.is_empty());
            assert_eq!(effects.fields.pre_token_balances, [0]);
            assert_eq!(effects.fields.transaction_rewards, [0]);
            let range = |field: &[u8]| {
                let start = field.as_ptr() as usize - canonical_none.as_ptr() as usize;
                start..start + field.len()
            };
            (
                range(effects.fields.pre_token_balances),
                range(effects.fields.transaction_rewards),
            )
        };
        assert!(pre_token_range.start < reward_range.start);
        let mut noncanonical_empty = canonical_none.clone();
        noncanonical_empty.splice(reward_range, [0x80, 0]);
        noncanonical_empty.splice(pre_token_range, [0x80, 0]);
        let (inner_range, logs_range) = {
            let mut cursor = canonical_present_empty.as_slice();
            let effects = decode::stream_metadata_effects_with_schema(
                &mut cursor,
                CompactV2MetadataSchema::CurrentTypedError,
                MetadataDecodeLimits {
                    total_message_accounts: 0,
                    top_level_instruction_count: 0,
                },
                |_| Ok::<(), anyhow::Error>(()),
            )
            .unwrap();
            assert!(cursor.is_empty());
            let range = |field: &[u8]| {
                let start = field.as_ptr() as usize - canonical_present_empty.as_ptr() as usize;
                start..start + field.len()
            };
            (
                range(effects.fields.inner_instructions),
                range(effects.fields.logs),
            )
        };
        assert_eq!(
            &canonical_present_empty[inner_range.start..inner_range.start + 2],
            [1, 0]
        );
        assert_eq!(
            &canonical_present_empty[logs_range.start..logs_range.start + 2],
            [1, 0]
        );
        assert!(inner_range.start < logs_range.start);
        let mut noncanonical_present_empty = canonical_present_empty.clone();
        noncanonical_present_empty.splice(logs_range.start + 1..logs_range.start + 2, [0x80, 0]);
        noncanonical_present_empty.splice(inner_range.start + 1..inner_range.start + 2, [0x80, 0]);

        let mut scratch = LeanWorkerScratch::default();
        scratch.begin_block();
        for (tx_index, encoded) in [
            (0_u32, canonical_none.as_slice()),
            (1, canonical_present_empty.as_slice()),
            (2, noncanonical_empty.as_slice()),
            (3, noncanonical_present_empty.as_slice()),
        ] {
            let mut cursor = encoded;
            let effects = decode::stream_metadata_effects_with_schema(
                &mut cursor,
                CompactV2MetadataSchema::CurrentTypedError,
                MetadataDecodeLimits {
                    total_message_accounts: 0,
                    top_level_instruction_count: 0,
                },
                |_| Ok::<(), anyhow::Error>(()),
            )
            .unwrap();
            assert!(cursor.is_empty());
            scratch
                .record_decoded_metadata(tx_index, ARCHIVE_V2_TX_FLAG_HAS_METADATA, &effects)
                .unwrap();
        }
        scratch.finish_block(4).unwrap();

        let inner_none = serialize(&none.inner_instructions);
        let inner_empty = serialize(&present_empty.inner_instructions);
        let mut inner_noncanonical_empty = inner_empty.clone();
        inner_noncanonical_empty.splice(1..2, [0x80, 0]);
        let logs_none = serialize(&none.logs);
        let logs_empty = serialize(&present_empty.logs);
        let mut logs_noncanonical_empty = logs_empty.clone();
        logs_noncanonical_empty.splice(1..2, [0x80, 0]);
        let mut token_canonical = serialize(&none.pre_token_balances);
        token_canonical.extend(serialize(&none.post_token_balances));
        let mut token_noncanonical = vec![0x80, 0];
        token_noncanonical.extend(serialize(&none.post_token_balances));
        assert_eq!(
            scratch.raw[LeanObject::InnerInstructions.index()],
            [
                inner_none.as_slice(),
                inner_empty.as_slice(),
                inner_none.as_slice(),
                inner_noncanonical_empty.as_slice(),
            ]
            .concat()
        );
        assert_eq!(
            scratch.raw[LeanObject::Logs.index()],
            [
                logs_none.as_slice(),
                logs_empty.as_slice(),
                logs_none.as_slice(),
                logs_noncanonical_empty.as_slice(),
            ]
            .concat()
        );
        assert_eq!(
            scratch.raw[LeanObject::TokenBalances.index()],
            [
                token_canonical.as_slice(),
                token_canonical.as_slice(),
                token_noncanonical.as_slice(),
                token_canonical.as_slice(),
            ]
            .concat()
        );
        assert_eq!(
            scratch.raw[LeanObject::TransactionRewards.index()],
            [2, 0, 0, 0, 0x80, 0]
        );
        assert_eq!(
            scratch.record_counts[LeanObject::TransactionRewards.index()],
            1
        );
        assert_eq!(scratch.nonempty_semantic_transaction_rewards, 0);
        assert_eq!(
            scratch.raw[LeanObject::TransactionDirectory.index()][2 * LEAN_DIRECTORY_ROW_LEN + 2]
                & (1 << 7),
            0
        );

        let directory = &scratch.raw[LeanObject::TransactionDirectory.index()];
        let first_inner_end = u32::from_le_bytes(directory[4..8].try_into().unwrap());
        let first_logs_end = u32::from_le_bytes(directory[8..12].try_into().unwrap());
        let first_token_end = u32::from_le_bytes(directory[12..16].try_into().unwrap());
        assert_eq!(first_inner_end as usize, inner_none.len());
        assert_eq!(first_logs_end as usize, logs_none.len());
        assert_eq!(first_token_end as usize, token_canonical.len());
        let second = &directory[LEAN_DIRECTORY_ROW_LEN..];
        assert_eq!(
            u32::from_le_bytes(second[4..8].try_into().unwrap()) as usize,
            inner_none.len() + inner_empty.len()
        );
        assert_eq!(
            u32::from_le_bytes(second[8..12].try_into().unwrap()) as usize,
            logs_none.len() + logs_empty.len()
        );
        assert_eq!(
            u32::from_le_bytes(second[12..16].try_into().unwrap()) as usize,
            token_canonical.len() * 2
        );
        let third = &directory[2 * LEAN_DIRECTORY_ROW_LEN..];
        assert_eq!(
            u32::from_le_bytes(third[4..8].try_into().unwrap()) as usize,
            inner_none.len() * 2 + inner_empty.len()
        );
        assert_eq!(
            u32::from_le_bytes(third[8..12].try_into().unwrap()) as usize,
            logs_none.len() * 2 + logs_empty.len()
        );
        assert_eq!(
            u32::from_le_bytes(third[12..16].try_into().unwrap()) as usize,
            token_canonical.len() * 2 + token_noncanonical.len()
        );
        let fourth = &directory[3 * LEAN_DIRECTORY_ROW_LEN..];
        assert_eq!(
            u32::from_le_bytes(fourth[4..8].try_into().unwrap()) as usize,
            inner_none.len() * 2 + inner_empty.len() + inner_noncanonical_empty.len()
        );
        assert_eq!(
            u32::from_le_bytes(fourth[8..12].try_into().unwrap()) as usize,
            logs_none.len() * 2 + logs_empty.len() + logs_noncanonical_empty.len()
        );
    }

    #[test]
    fn lean_bounds_reject_unknown_flags_and_locator_overflow() {
        let mut scratch = LeanWorkerScratch::default();
        scratch.begin_block();
        let error = scratch
            .record_missing_metadata(SOURCE_TX_FLAG_MASK + 1)
            .unwrap_err();
        assert!(error.to_string().contains("unknown source flag"));
        scratch.begin_block();
        scratch
            .record_missing_metadata(SOURCE_TX_FLAG_MASK)
            .unwrap();
        scratch.finish_block(1).unwrap();
        assert_eq!(
            u16::from_le_bytes(
                scratch.raw[LeanObject::TransactionDirectory.index()][..2]
                    .try_into()
                    .unwrap()
            ),
            SOURCE_TX_FLAG_MASK as u16
        );

        let directory = tempfile::tempdir().unwrap();
        let mut writers = LeanWriters::create(
            directory.path(),
            SplitHeaderBinding {
                epoch: 2,
                slots_per_epoch: 100,
                selected_blocks: 1,
                selected_transactions: 0,
                message_schema: CompactV2MessageSchema::Current,
                metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
                prefix: true,
            },
            LeanCompressionArg::Zstd,
            LeanZstdLevelArg::One,
        )
        .unwrap();
        let mut decoded_lengths = [0_u32; LEAN_OBJECT_COUNT];
        decoded_lengths[LeanObject::BlockRewards.index()] = LEAN_ZSTD_CODEC_BIT;
        let mut compressed = [false; LEAN_OBJECT_COUNT];
        compressed[LeanObject::BlockRewards.index()] = true;
        let mut ranges: [Range<usize>; LEAN_OBJECT_COUNT] = std::array::from_fn(|_| 0usize..0usize);
        ranges[LeanObject::BlockRewards.index()] = 0..1;
        let mut stats = LeanProjectionStats::default();
        stats.objects[LeanObject::BlockRewards.index()].stored_bytes = 1;
        let error = writers
            .append(
                index_row(0, 88, 0),
                ProjectedLeanEffects {
                    packed: vec![1],
                    ranges,
                    decoded_lengths,
                    compressed,
                    stats,
                },
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("decoded lean chunk exceeds locator")
        );
    }

    #[test]
    fn source_guard_allows_sizes_but_rejects_signature_and_unrelated_content() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join(ARCHIVE_V2_SIGNATURES_FILE), [1; 64]).unwrap();
        fs::write(directory.path().join(ARCHIVE_V2_POH_FILE), [2; 4]).unwrap();
        fs::write(
            directory.path().join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE),
            [4; 4],
        )
        .unwrap();
        fs::write(directory.path().join("future-sidecar.bin"), [5; 4]).unwrap();
        fs::write(directory.path().join(ARCHIVE_V2_BLOCKS_FILE), [3; 8]).unwrap();
        let guard = NoSignatureContentSource::new(PinnedLocalRangeSource::new(directory.path()));

        assert_eq!(guard.size(ARCHIVE_V2_SIGNATURES_FILE).unwrap(), Some(64));
        assert_eq!(guard.rejected_signature_content_reads(), 0);
        assert!(guard.read_range(ARCHIVE_V2_SIGNATURES_FILE, 0, 1).is_err());
        assert!(guard.read_range(ARCHIVE_V2_POH_FILE, 0, 1).is_err());
        assert!(
            guard
                .read_range(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, 0, 1)
                .is_err()
        );
        assert!(guard.read_range("future-sidecar.bin", 0, 1).is_err());
        assert_eq!(guard.rejected_signature_content_reads(), 1);
        assert_eq!(guard.rejected_unrelated_content_reads(), 3);

        guard.reset_block_content_reads().unwrap();
        assert_eq!(
            guard.read_range(ARCHIVE_V2_BLOCKS_FILE, 0, 8).unwrap(),
            vec![3; 8]
        );
        assert_eq!(guard.validate_block_content_reads(0, 8, 8).unwrap(), 1);
    }

    #[test]
    fn default_b6_baseline_and_one_vs_twelve_workers_are_byte_identical() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        fs::create_dir(&source).unwrap();
        write_reader_fixture(&source, true, 6, false);
        let source_files = [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_META_FILE,
        ];
        let before = source_files
            .iter()
            .map(|name| (*name, fs::read(source.join(name)).unwrap()))
            .collect::<Vec<_>>();
        let protected_metadata =
            [ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_SIGNATURES_FILE].map(|name| {
                let metadata = fs::metadata(source.join(name)).unwrap();
                (name, metadata.len(), metadata.modified().unwrap())
            });

        let output_one = directory.path().join("projection-one");
        let output_many = directory.path().join("projection-many");
        for (workers, output) in [(1, &output_one), (12, &output_many)] {
            run(Args {
                source: source.clone(),
                output: output.clone(),
                epoch: 1,
                slots_per_epoch: 100,
                message_schema: MessageSchemaArg::Current,
                metadata_schema: MetadataSchemaArg::CurrentTypedError,
                workers,
                benchmark_prefix_blocks: None,
                source_split_effects: false,
                lean_block_chunks: false,
                lean_compression: None,
                lean_zstd_level: None,
            })
            .unwrap();
            let report: serde_json::Value =
                serde_json::from_slice(&fs::read(output.join(REPORT_FILE)).unwrap()).unwrap();
            assert_frozen_default_report_schema(&report);
            assert_eq!(report["status"], STATUS);
            assert_eq!(report["complete_coverage"], true);
            assert_eq!(report["signature_content_reads"], 0);
            assert_eq!(report["unrelated_source_content_reads"], 0);
            assert_eq!(report["source_unchanged"], true);
            assert_eq!(report["selected_blocks"], 12);
            assert_eq!(report["transactions"], 12);
            assert!(report.get("source_split").is_none());
            let mut broadened_candidate_report = report.clone();
            broadened_candidate_report["source_split"] = serde_json::json!({});
            let error =
                serde_json::from_value::<CandidateBenchmarkReport>(broadened_candidate_report)
                    .expect_err("the account verifier must reject split-only report fields");
            assert!(error.to_string().contains("unknown field `source_split`"));
            let mut broadened_candidate_report = report.clone();
            broadened_candidate_report["lean_block_chunks"] = serde_json::json!({});
            let error =
                serde_json::from_value::<CandidateBenchmarkReport>(broadened_candidate_report)
                    .expect_err("the account verifier must reject lean-only report fields");
            assert!(
                error
                    .to_string()
                    .contains("unknown field `lean_block_chunks`")
            );
            for file in split_output_files().into_iter().chain(lean_output_files()) {
                assert!(!output.join(file).exists());
            }
        }

        assert_eq!(
            fs::read(output_one.join(PAGES_FILE)).unwrap(),
            fs::read(output_many.join(PAGES_FILE)).unwrap()
        );
        assert_eq!(
            fs::read(output_one.join(INDEX_FILE)).unwrap(),
            fs::read(output_many.join(INDEX_FILE)).unwrap()
        );
        const B6_GOLDEN_PAGES: &str = concat!(
            "28b52ffd20505d0100f8425a563241435031010001000000010141000000020000000003010c0000090400b8a6670eb432ca0090",
            "28b52ffd2058b50100b402425a56324143503101000000010000020000004303000003000002000000010a000007000400000300",
            "00000300b83e50aeba704928b52ffd20505d0100f0425a5632414350310100000002000000010000000101410003010c00000904",
            "00b8a6670ebc99990d450c28b52ffd2058cd0100d402425a56324143503101000000030000000100000001010000020000004303",
            "0003010a00000700040000030000000300b8a6670eec0dd30928b52ffd20508d01005402425a5632414350310100000004000000",
            "01000000010141000000020000000003010c0000090300b8a6670e6443690228b52ffd2058ed01004403425a5632414350310100",
            "0000050000000100000001010000020000004303000003000002000000010a00000700040000030000000200b83ec3559e28b52f",
            "fd20508d01005402425a563241435031010000000600000001000000010141000000020000000003010c0000090300b8a6670e64",
            "43690228b52ffd2058ed01004403425a56324143503101000000070000000100000001010000020000004303000003000002000000",
            "010a00000700040000030000000200b83ec3559e28b52ffd20508d01005402425a5632414350310100000008000000010000000101",
            "41000000020000000003010c0000090300b8a6670e6443690228b52ffd2058ed01004403425a563241435031010000000900000001",
            "00000001010000020000004303000003000002000000010a00000700040000030000000200b83ec3559e28b52ffd20508d010054",
            "02425a563241435031010000000a00000001000000010141000000020000000003010c0000090300b8a6670e6443690228b52ffd",
            "2058ed01004403425a563241435031010000000b0000000100000001010000020000004303000003000002000000010a00000700",
            "040000030000000200b83ec3559e",
        );
        const B6_GOLDEN_INDEX: &str = concat!(
            "425a563241435831010000000c00000000000000e9020000000000000a0000000000000000000000000000006500000000000000",
            "00000000000000003400000050000000010000000200000001010000010000000100000066000000000000003400000000000000",
            "3f000000580000000100000003000000010100000100000002000000670000000000000073000000000000003400000050000000",
            "01000000020000000101000001000000030000006800000000000000a70000000000000042000000580000000100000003000000",
            "0101000001000000040000006900000000000000e9000000000000003a0000005000000001000000020000000101000001000000",
            "050000006a000000000000002301000000000000460000005800000001000000030000000101000001000000060000006b000000",
            "0000000069010000000000003a0000005000000001000000020000000101000001000000070000006c00000000000000a3010000",
            "00000000460000005800000001000000030000000101000001000000080000006d00000000000000e9010000000000003a000000",
            "5000000001000000020000000101000001000000090000006e000000000000002302000000000000460000005800000001000000",
            "0300000001010000010000000a0000006f0000000000000069020000000000003a00000050000000010000000200000001010000",
            "010000000b0000007000000000000000a302000000000000460000005800000001000000030000000101000001000000",
        );
        assert_eq!(
            fs::read(output_one.join(PAGES_FILE)).unwrap(),
            decode_golden_hex(B6_GOLDEN_PAGES)
        );
        assert_eq!(
            fs::read(output_one.join(INDEX_FILE)).unwrap(),
            decode_golden_hex(B6_GOLDEN_INDEX)
        );
        for (name, bytes) in before {
            assert_eq!(fs::read(source.join(name)).unwrap(), bytes);
        }
        for (name, length, modified) in protected_metadata {
            let metadata = fs::metadata(source.join(name)).unwrap();
            assert_eq!(metadata.len(), length);
            assert_eq!(metadata.modified().unwrap(), modified);
        }
    }

    #[test]
    fn split_canary_one_and_twelve_workers_write_identical_all_output_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        fs::create_dir(&source).unwrap();
        write_reader_fixture(&source, true, 6, false);
        let output_default = directory.path().join("account-default");
        let output_one = directory.path().join("split-one");
        let output_many = directory.path().join("split-many");
        run(Args {
            source: source.clone(),
            output: output_default.clone(),
            epoch: 1,
            slots_per_epoch: 100,
            message_schema: MessageSchemaArg::Current,
            metadata_schema: MetadataSchemaArg::CurrentTypedError,
            workers: 1,
            benchmark_prefix_blocks: None,
            source_split_effects: false,
            lean_block_chunks: false,
            lean_compression: None,
            lean_zstd_level: None,
        })
        .unwrap();
        for (workers, output) in [(1, &output_one), (12, &output_many)] {
            run(Args {
                source: source.clone(),
                output: output.clone(),
                epoch: 1,
                slots_per_epoch: 100,
                message_schema: MessageSchemaArg::Current,
                metadata_schema: MetadataSchemaArg::CurrentTypedError,
                workers,
                benchmark_prefix_blocks: None,
                source_split_effects: true,
                lean_block_chunks: false,
                lean_compression: None,
                lean_zstd_level: None,
            })
            .unwrap();
            let report: serde_json::Value =
                serde_json::from_slice(&fs::read(output.join(REPORT_FILE)).unwrap()).unwrap();
            let split = &report["source_split"];
            assert_eq!(split["canary_kind"], SPLIT_CANARY_KIND);
            assert_eq!(split["candidate_status"], STATUS);
            assert_eq!(
                split["format_status"],
                "measurement-container-not-final-schema"
            );
            assert_eq!(split["metadata_reconstructable"], false);
            assert_eq!(split["loaded_address_lanes_preserved"], false);
            assert_eq!(split["decoded_metadata_transactions"], 12);
            assert_eq!(split["raw_transaction_flags"], 0);
            assert_eq!(
                split["account_projection_output_bytes"],
                report["output_bytes"]
            );
            assert_eq!(
                split["combined_candidate_output_bytes"].as_u64().unwrap(),
                split["account_projection_output_bytes"].as_u64().unwrap()
                    + split["split_output_bytes"].as_u64().unwrap()
            );
        }

        for file in [PAGES_FILE, INDEX_FILE]
            .into_iter()
            .chain(split_output_files())
        {
            assert_eq!(
                fs::read(output_one.join(file)).unwrap(),
                fs::read(output_many.join(file)).unwrap(),
                "{file} differs between one and twelve workers"
            );
        }
        for file in [PAGES_FILE, INDEX_FILE] {
            let baseline = fs::read(output_default.join(file)).unwrap();
            assert_eq!(fs::read(output_one.join(file)).unwrap(), baseline);
            assert_eq!(fs::read(output_many.join(file)).unwrap(), baseline);
        }
        let state_frames = read_split_frames(
            &output_one.join(SplitPlane::MetadataStates.file_name()),
            SplitPlane::MetadataStates,
        );
        assert_eq!(state_frames.len(), 12);
        for (block_id, frame) in state_frames.iter().enumerate() {
            assert_eq!(frame.plane, SplitPlane::MetadataStates as u16);
            assert_eq!(frame.block_id, block_id as u32);
            assert_eq!(frame.first_tx, 0);
            assert_eq!(frame.tx_count, 1);
            assert_eq!(frame.dense_count, 1);
            assert_eq!(frame.decoded.len(), 2);
            assert_eq!(frame.flags & !SPLIT_FRAME_FLAG_ZSTD, 0);
        }
        let expected_rows = (0..12)
            .map(|block_id| (block_id, 101 + u64::from(block_id), 1))
            .collect::<Vec<_>>();
        assert_split_index_geometry(&output_one, &expected_rows);
    }

    #[test]
    fn lean_canary_one_and_twelve_workers_is_deterministic_and_keeps_b6_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        fs::create_dir(&source).unwrap();
        write_reader_fixture(&source, true, 6, false);
        let output_default = directory.path().join("account-default");
        let output_one = directory.path().join("lean-one");
        let output_many = directory.path().join("lean-many");
        let output_explicit_one = directory.path().join("lean-explicit-level-one");
        let output_adaptive_default = directory.path().join("lean-adaptive-default");
        run(Args {
            source: source.clone(),
            output: output_default.clone(),
            epoch: 1,
            slots_per_epoch: 100,
            message_schema: MessageSchemaArg::Current,
            metadata_schema: MetadataSchemaArg::CurrentTypedError,
            workers: 1,
            benchmark_prefix_blocks: None,
            source_split_effects: false,
            lean_block_chunks: false,
            lean_compression: None,
            lean_zstd_level: None,
        })
        .unwrap();
        for (workers, output) in [(1, &output_one), (12, &output_many)] {
            run(Args {
                source: source.clone(),
                output: output.clone(),
                epoch: 1,
                slots_per_epoch: 100,
                message_schema: MessageSchemaArg::Current,
                metadata_schema: MetadataSchemaArg::CurrentTypedError,
                workers,
                benchmark_prefix_blocks: None,
                source_split_effects: false,
                lean_block_chunks: true,
                lean_compression: Some(LeanCompressionArg::Zstd),
                lean_zstd_level: None,
            })
            .unwrap();
            let report: serde_json::Value =
                serde_json::from_slice(&fs::read(output.join(REPORT_FILE)).unwrap()).unwrap();
            assert!(report.get("source_split").is_none());
            let lean = &report["lean_block_chunks"];
            assert_eq!(lean["canary_kind"], LEAN_CANARY_KIND);
            assert_eq!(lean["candidate_status"], STATUS);
            assert_eq!(
                lean["format_status"],
                "measurement-container-not-final-schema"
            );
            assert_eq!(lean["compression_mode"], "zstd");
            assert!(lean.get("compression_policy").is_none());
            assert!(lean["zstd_block_chunks"].as_u64().unwrap() > 0);
            assert_eq!(
                lean["transaction_paging"],
                "none-one-chunk-per-source-block-and-object"
            );
            assert_eq!(lean["directory_row_bytes_per_transaction"], 24);
            assert_eq!(lean["decoded_metadata_transactions"], 12);
            assert_eq!(lean["raw_transaction_flags"], 0);
            assert_eq!(
                lean["account_projection_output_bytes"],
                report["output_bytes"]
            );
            assert_eq!(
                lean["combined_candidate_output_bytes"].as_u64().unwrap(),
                lean["account_projection_output_bytes"].as_u64().unwrap()
                    + lean["lean_output_bytes"].as_u64().unwrap()
            );
            for object in lean["objects"].as_array().unwrap() {
                assert!(object.get("declared_compression_policy").is_none());
                assert!(object.get("compression_attempts").is_none());
            }
            for file in split_output_files() {
                assert!(!output.join(file).exists());
            }
        }

        for file in [PAGES_FILE, INDEX_FILE]
            .into_iter()
            .chain(lean_output_files())
        {
            assert_eq!(
                fs::read(output_one.join(file)).unwrap(),
                fs::read(output_many.join(file)).unwrap(),
                "{file} differs between one and twelve workers"
            );
        }
        for file in [PAGES_FILE, INDEX_FILE] {
            let baseline = fs::read(output_default.join(file)).unwrap();
            assert_eq!(fs::read(output_one.join(file)).unwrap(), baseline);
            assert_eq!(fs::read(output_many.join(file)).unwrap(), baseline);
        }
        run(Args {
            source: source.clone(),
            output: output_explicit_one.clone(),
            epoch: 1,
            slots_per_epoch: 100,
            message_schema: MessageSchemaArg::Current,
            metadata_schema: MetadataSchemaArg::CurrentTypedError,
            workers: 1,
            benchmark_prefix_blocks: None,
            source_split_effects: false,
            lean_block_chunks: true,
            lean_compression: Some(LeanCompressionArg::Zstd),
            lean_zstd_level: Some(LeanZstdLevelArg::One),
        })
        .unwrap();
        for file in [PAGES_FILE, INDEX_FILE]
            .into_iter()
            .chain(lean_output_files())
        {
            assert_eq!(
                fs::read(output_one.join(file)).unwrap(),
                fs::read(output_explicit_one.join(file)).unwrap(),
                "omitted and explicit level 1 differ in {file}"
            );
        }
        let deterministic_lean_report = |output: &Path| {
            let mut report: serde_json::Value =
                serde_json::from_slice(&fs::read(output.join(REPORT_FILE)).unwrap()).unwrap();
            let lean = report["lean_block_chunks"].as_object_mut().unwrap();
            for field in [
                "worker_metadata_account_and_effect_stream_sum_ms",
                "worker_lean_compression_sum_ms",
                "lean_copy_and_other_worker_residual_sum_ms",
                "ordered_lean_write_sum_ms",
                "lean_finalize_ms",
            ] {
                lean.remove(field);
            }
            for object in lean["objects"].as_array_mut().unwrap() {
                object.as_object_mut().unwrap().remove("compression_sum_ms");
            }
            report["lean_block_chunks"].clone()
        };
        assert_eq!(
            deterministic_lean_report(&output_one),
            deterministic_lean_report(&output_explicit_one)
        );
        assert!(
            deterministic_lean_report(&output_explicit_one)
                .get("zstd_context_memory_accounting")
                .is_none()
        );
        let expected_rows = (0..12)
            .map(|block_id| (block_id, 101 + u64::from(block_id), 1))
            .collect::<Vec<_>>();
        let blocks = read_lean_output(
            &output_one,
            LeanCompressionArg::Zstd,
            1,
            100,
            false,
            &expected_rows,
        );
        assert_eq!(blocks.len(), 12);
        for block in blocks {
            assert!(block.compressed[LeanObject::TransactionDirectory.index()]);
            assert!(block.compressed[LeanObject::BlockRewards.index()]);
            assert_eq!(
                block.decoded[LeanObject::TransactionDirectory.index()].len(),
                LEAN_DIRECTORY_ROW_LEN
            );
            assert!(!block.decoded[LeanObject::Outcomes.index()].is_empty());
            assert!(!block.decoded[LeanObject::Balances.index()].is_empty());
            assert_eq!(block.decoded[LeanObject::BlockRewards.index()], [0]);
        }

        run(Args {
            source,
            output: output_adaptive_default.clone(),
            epoch: 1,
            slots_per_epoch: 100,
            message_schema: MessageSchemaArg::Current,
            metadata_schema: MetadataSchemaArg::CurrentTypedError,
            workers: 1,
            benchmark_prefix_blocks: None,
            source_split_effects: false,
            lean_block_chunks: true,
            lean_compression: None,
            lean_zstd_level: None,
        })
        .unwrap();
        let report: serde_json::Value =
            serde_json::from_slice(&fs::read(output_adaptive_default.join(REPORT_FILE)).unwrap())
                .unwrap();
        assert_eq!(report["lean_block_chunks"]["compression_mode"], "adaptive");
        assert!(
            report["lean_block_chunks"]
                .get("compression_policy")
                .is_none()
        );
        for object in report["lean_block_chunks"]["objects"].as_array().unwrap() {
            assert!(object.get("declared_compression_policy").is_none());
            assert!(object.get("compression_attempts").is_none());
        }
        read_lean_output(
            &output_adaptive_default,
            LeanCompressionArg::Adaptive,
            1,
            100,
            false,
            &expected_rows,
        );
    }

    #[test]
    fn lean_hybrid_one_and_twelve_workers_are_byte_identical_across_all_policy_lanes() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        fs::create_dir(&source).unwrap();
        write_reader_fixture(&source, true, 6, true);
        let output_default = directory.path().join("account-default");
        let output_one = directory.path().join("hybrid-one");
        let output_many = directory.path().join("hybrid-many");

        run(Args {
            source: source.clone(),
            output: output_default.clone(),
            epoch: 1,
            slots_per_epoch: 100,
            message_schema: MessageSchemaArg::Current,
            metadata_schema: MetadataSchemaArg::CurrentTypedError,
            workers: 1,
            benchmark_prefix_blocks: None,
            source_split_effects: false,
            lean_block_chunks: false,
            lean_compression: None,
            lean_zstd_level: None,
        })
        .unwrap();
        for (workers, output) in [(1, &output_one), (12, &output_many)] {
            run(Args {
                source: source.clone(),
                output: output.clone(),
                epoch: 1,
                slots_per_epoch: 100,
                message_schema: MessageSchemaArg::Current,
                metadata_schema: MetadataSchemaArg::CurrentTypedError,
                workers,
                benchmark_prefix_blocks: None,
                source_split_effects: false,
                lean_block_chunks: true,
                lean_compression: Some(LeanCompressionArg::Hybrid),
                lean_zstd_level: None,
            })
            .unwrap();
            let report: serde_json::Value =
                serde_json::from_slice(&fs::read(output.join(REPORT_FILE)).unwrap()).unwrap();
            let lean = &report["lean_block_chunks"];
            assert_eq!(lean["compression_mode"], "hybrid");
            assert_eq!(
                lean["compression_policy"],
                "hybrid-v1-directory-and-five-dense-zstd1-two-sparse-adaptive-zstd1-when-smaller-block-rewards-raw-no-attempt"
            );
            let objects = lean["objects"].as_array().unwrap();
            let directory_object = objects
                .iter()
                .find(|object| object["object"] == LeanObject::TransactionDirectory.name())
                .unwrap();
            assert_eq!(directory_object["declared_compression_policy"], "zstd1");
            assert_eq!(directory_object["compression_attempts"], 12);
            let transaction_rewards = objects
                .iter()
                .find(|object| object["object"] == LeanObject::TransactionRewards.name())
                .unwrap();
            assert_eq!(
                transaction_rewards["declared_compression_policy"],
                "adaptive-zstd1-when-smaller"
            );
            assert_eq!(transaction_rewards["compression_attempts"], 12);
            assert!(transaction_rewards["raw_blocks"].as_u64().unwrap() > 0);
            assert!(transaction_rewards["zstd_blocks"].as_u64().unwrap() > 0);
            assert_eq!(
                transaction_rewards["blocks_with_bytes"].as_u64().unwrap(),
                transaction_rewards["raw_blocks"].as_u64().unwrap()
                    + transaction_rewards["zstd_blocks"].as_u64().unwrap()
            );
            let block_rewards = objects
                .iter()
                .find(|object| object["object"] == LeanObject::BlockRewards.name())
                .unwrap();
            assert_eq!(block_rewards["declared_compression_policy"], "raw");
            assert_eq!(block_rewards["compression_attempts"], 0);
            assert_eq!(block_rewards["raw_blocks"], 12);
            assert_eq!(block_rewards["zstd_blocks"], 0);
        }

        for file in [PAGES_FILE, INDEX_FILE]
            .into_iter()
            .chain(lean_output_files())
        {
            assert_eq!(
                fs::read(output_one.join(file)).unwrap(),
                fs::read(output_many.join(file)).unwrap(),
                "{file} differs between one and twelve hybrid workers"
            );
        }
        for file in [PAGES_FILE, INDEX_FILE] {
            let baseline = fs::read(output_default.join(file)).unwrap();
            assert_eq!(fs::read(output_one.join(file)).unwrap(), baseline);
            assert_eq!(fs::read(output_many.join(file)).unwrap(), baseline);
        }

        let expected_rows = (0..12)
            .map(|block_id| (block_id, 101 + u64::from(block_id), 1))
            .collect::<Vec<_>>();
        let blocks = read_lean_output(
            &output_one,
            LeanCompressionArg::Hybrid,
            1,
            100,
            false,
            &expected_rows,
        );
        assert!(blocks.iter().all(|block| {
            block.compressed[LeanObject::TransactionDirectory.index()]
                && !block.compressed[LeanObject::BlockRewards.index()]
        }));
        assert!(
            blocks
                .iter()
                .any(|block| block.compressed[LeanObject::TransactionRewards.index()])
        );
        assert!(
            blocks
                .iter()
                .any(|block| !block.compressed[LeanObject::TransactionRewards.index()])
        );
    }

    #[test]
    fn lean_level_nine_hybrid_is_deterministic_and_keeps_account_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        fs::create_dir(&source).unwrap();
        write_reader_fixture(&source, true, 6, true);
        let account = directory.path().join("account");
        let one = directory.path().join("hybrid-nine-one");
        let twelve = directory.path().join("hybrid-nine-twelve");

        run(Args {
            source: source.clone(),
            output: account.clone(),
            epoch: 1,
            slots_per_epoch: 100,
            message_schema: MessageSchemaArg::Current,
            metadata_schema: MetadataSchemaArg::CurrentTypedError,
            workers: 1,
            benchmark_prefix_blocks: None,
            source_split_effects: false,
            lean_block_chunks: false,
            lean_compression: None,
            lean_zstd_level: None,
        })
        .unwrap();
        for (workers, output) in [(1, &one), (12, &twelve)] {
            run(Args {
                source: source.clone(),
                output: output.clone(),
                epoch: 1,
                slots_per_epoch: 100,
                message_schema: MessageSchemaArg::Current,
                metadata_schema: MetadataSchemaArg::CurrentTypedError,
                workers,
                benchmark_prefix_blocks: None,
                source_split_effects: false,
                lean_block_chunks: true,
                lean_compression: Some(LeanCompressionArg::Hybrid),
                lean_zstd_level: Some(LeanZstdLevelArg::Nine),
            })
            .unwrap();
            let report: serde_json::Value =
                serde_json::from_slice(&fs::read(output.join(REPORT_FILE)).unwrap()).unwrap();
            let lean = &report["lean_block_chunks"];
            assert_eq!(lean["zstd_level"], 9);
            assert_eq!(
                lean["compression_policy"],
                LeanZstdLevelArg::Nine.hybrid_policy_name()
            );
            assert!(lean.get("zstd_context_memory_accounting").is_some());
            let objects = lean["objects"].as_array().unwrap();
            let directory_object = objects
                .iter()
                .find(|object| object["object"] == LeanObject::TransactionDirectory.name())
                .unwrap();
            assert_eq!(directory_object["declared_compression_policy"], "zstd9");
            let transaction_rewards = objects
                .iter()
                .find(|object| object["object"] == LeanObject::TransactionRewards.name())
                .unwrap();
            assert_eq!(
                transaction_rewards["declared_compression_policy"],
                "adaptive-zstd9-when-smaller"
            );
            let block_rewards = objects
                .iter()
                .find(|object| object["object"] == LeanObject::BlockRewards.name())
                .unwrap();
            assert_eq!(block_rewards["declared_compression_policy"], "raw");
            assert_eq!(block_rewards["compression_attempts"], 0);
        }

        for file in [PAGES_FILE, INDEX_FILE]
            .into_iter()
            .chain(lean_output_files())
        {
            assert_eq!(
                fs::read(one.join(file)).unwrap(),
                fs::read(twelve.join(file)).unwrap(),
                "level-nine {file} differs between one and twelve workers"
            );
        }
        for file in [PAGES_FILE, INDEX_FILE] {
            let baseline = fs::read(account.join(file)).unwrap();
            assert_eq!(fs::read(one.join(file)).unwrap(), baseline);
            assert_eq!(fs::read(twelve.join(file)).unwrap(), baseline);
        }
        let expected_rows = (0..12)
            .map(|block_id| (block_id, 101 + u64::from(block_id), 1))
            .collect::<Vec<_>>();
        let blocks = read_lean_output_at_level(
            &one,
            LeanCompressionArg::Hybrid,
            LeanZstdLevelArg::Nine,
            1,
            100,
            false,
            &expected_rows,
        );
        assert!(blocks.iter().all(|block| {
            block.compressed[LeanObject::TransactionDirectory.index()]
                && !block.compressed[LeanObject::BlockRewards.index()]
        }));
    }

    #[test]
    fn lean_cli_requires_an_explicit_separate_mode() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        fs::create_dir(&source).unwrap();
        let base = |output: &str| Args {
            source: source.clone(),
            output: directory.path().join(output),
            epoch: 1,
            slots_per_epoch: 100,
            message_schema: MessageSchemaArg::Current,
            metadata_schema: MetadataSchemaArg::CurrentTypedError,
            workers: 1,
            benchmark_prefix_blocks: Some(1),
            source_split_effects: false,
            lean_block_chunks: false,
            lean_compression: None,
            lean_zstd_level: None,
        };
        let error = run(Args {
            source_split_effects: true,
            lean_block_chunks: true,
            ..base("output")
        })
        .unwrap_err();
        assert!(error.to_string().contains("separate benchmark modes"));
        let error = run(Args {
            lean_compression: Some(LeanCompressionArg::Raw),
            lean_zstd_level: None,
            ..base("other-output")
        })
        .unwrap_err();
        assert!(error.to_string().contains("requires --lean-block-chunks"));
        let error = run(Args {
            lean_zstd_level: Some(LeanZstdLevelArg::Three),
            ..base("level-without-lean")
        })
        .unwrap_err();
        assert!(error.to_string().contains("requires --lean-block-chunks"));
        let error = run(Args {
            lean_block_chunks: true,
            lean_compression: Some(LeanCompressionArg::Raw),
            lean_zstd_level: Some(LeanZstdLevelArg::Three),
            ..base("raw-nondefault-level")
        })
        .unwrap_err();
        assert!(error.to_string().contains("must be 1"));
    }

    #[test]
    fn full_incomplete_canary_keeps_every_row_and_writes_nonpublishable_report() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        fs::create_dir(&source).unwrap();
        write_reader_fixture(&source, false, 1, false);
        let output = directory.path().join("projection");
        run(Args {
            source,
            output: output.clone(),
            epoch: 1,
            slots_per_epoch: 100,
            message_schema: MessageSchemaArg::Current,
            metadata_schema: MetadataSchemaArg::CurrentTypedError,
            workers: 2,
            benchmark_prefix_blocks: None,
            source_split_effects: false,
            lean_block_chunks: false,
            lean_compression: None,
            lean_zstd_level: None,
        })
        .unwrap();
        let report: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join(REPORT_FILE)).unwrap()).unwrap();
        assert_eq!(report["status"], STATUS);
        assert_eq!(report["transactions"], 2);
        assert_eq!(report["fully_covered_transactions"], 1);
        assert_eq!(report["incomplete_coverage_transactions"], 1);
        assert_eq!(report["cpi_not_recorded_transactions"], 1);
        assert_eq!(report["complete_coverage"], false);
    }

    #[test]
    fn output_inside_source_and_broken_symlink_targets_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        fs::create_dir(&source).unwrap();
        write_reader_fixture(&source, true, 1, false);
        let inside = source.join("projection");
        let error = run(Args {
            source: source.clone(),
            output: inside,
            epoch: 1,
            slots_per_epoch: 100,
            message_schema: MessageSchemaArg::Current,
            metadata_schema: MetadataSchemaArg::CurrentTypedError,
            workers: 1,
            benchmark_prefix_blocks: Some(1),
            source_split_effects: false,
            lean_block_chunks: false,
            lean_compression: None,
            lean_zstd_level: None,
        })
        .unwrap_err();
        assert!(error.to_string().contains("outside the source"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let output = directory.path().join("broken-output");
            symlink(directory.path().join("missing-target"), &output).unwrap();
            let error = run(Args {
                source,
                output,
                epoch: 1,
                slots_per_epoch: 100,
                message_schema: MessageSchemaArg::Current,
                metadata_schema: MetadataSchemaArg::CurrentTypedError,
                workers: 1,
                benchmark_prefix_blocks: Some(1),
                source_split_effects: false,
                lean_block_chunks: false,
                lean_compression: None,
                lean_zstd_level: None,
            })
            .unwrap_err();
            assert!(error.to_string().contains("already exists"));
        }
    }
}
