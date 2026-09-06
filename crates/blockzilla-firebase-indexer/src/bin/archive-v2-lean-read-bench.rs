use std::{
    fs::{self, File},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::{
    array,
    cell::Cell,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use anyhow::{Context, Result, anyhow, ensure};
use clap::{Parser, ValueEnum};
use serde::Serialize;

const STATUS: &str = "read-only-unsealed-benchmark";
const INDEX_FILE: &str = "archive-v2-lean-blocks.index";
const FILE_HEADER_LEN: usize = 64;
const INDEX_ROW_LEN: usize = 160;
const DIRECTORY_ROW_LEN: usize = 24;
const FORMAT_VERSION: u16 = 1;
const ZSTD_CODEC_BIT: u32 = 1 << 31;
const STORED_LEN_MASK: u32 = ZSTD_CODEC_BIT - 1;
const DATA_MAGIC: [u8; 8] = *b"BZV2LN01";
const INDEX_MAGIC: [u8; 8] = *b"BZV2LI01";
const OBJECT_COUNT: usize = 9;
const DENSE_PLANE_COUNT: u8 = 5;
const SPARSE_PLANE_COUNT: u8 = 2;
const MAX_INDEX_BYTES: usize = 128 << 20;
const MAX_BLOCK_COUNT: u64 = (MAX_INDEX_BYTES - FILE_HEADER_LEN) as u64 / INDEX_ROW_LEN as u64;
const MAX_TRANSACTION_COUNT: u64 = 10_000_000_000;
const MAX_CHUNK_BYTES: usize = 512 << 20;
const MAX_AGGREGATE_WORKER_BUFFER_BYTES: usize = 512 << 20;
const ZSTD_WINDOW_LOG_MAX: u32 = 29;
const MAX_ITERATIONS: u32 = 1_000;

#[cfg(test)]
thread_local! {
    static TEST_BEFORE_SEND_DELAY: Cell<Duration> = const { Cell::new(Duration::ZERO) };
}

#[cfg(test)]
static TEST_DELAY_DECODE_BLOCK: AtomicU32 = AtomicU32::new(u32::MAX);
#[cfg(test)]
static TEST_DELAY_DECODE_MICROS: AtomicU64 = AtomicU64::new(0);

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
    const ALL: [Self; OBJECT_COUNT] = [
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ObjectArg {
    All,
    TransactionDirectory,
    InnerInstructions,
    Logs,
    TokenBalances,
    Balances,
    Outcomes,
    TransactionRewards,
    RawMetadataFallbacks,
    BlockRewards,
}

impl ObjectArg {
    fn objects(self) -> Vec<LeanObject> {
        match self {
            Self::All => LeanObject::ALL.to_vec(),
            Self::TransactionDirectory => vec![LeanObject::TransactionDirectory],
            Self::InnerInstructions => vec![LeanObject::InnerInstructions],
            Self::Logs => vec![LeanObject::Logs],
            Self::TokenBalances => vec![LeanObject::TokenBalances],
            Self::Balances => vec![LeanObject::Balances],
            Self::Outcomes => vec![LeanObject::Outcomes],
            Self::TransactionRewards => vec![LeanObject::TransactionRewards],
            Self::RawMetadataFallbacks => vec![LeanObject::RawMetadataFallbacks],
            Self::BlockRewards => vec![LeanObject::BlockRewards],
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::TransactionDirectory => LeanObject::TransactionDirectory.name(),
            Self::InnerInstructions => LeanObject::InnerInstructions.name(),
            Self::Logs => LeanObject::Logs.name(),
            Self::TokenBalances => LeanObject::TokenBalances.name(),
            Self::Balances => LeanObject::Balances.name(),
            Self::Outcomes => LeanObject::Outcomes.name(),
            Self::TransactionRewards => LeanObject::TransactionRewards.name(),
            Self::RawMetadataFallbacks => LeanObject::RawMetadataFallbacks.name(),
            Self::BlockRewards => LeanObject::BlockRewards.name(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompressionMode {
    Raw,
    Zstd,
    Adaptive,
    Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZstdLevel {
    One,
    Three,
    Five,
    Nine,
}

impl ZstdLevel {
    fn from_header_code(code: u8) -> Result<Self> {
        match code {
            0 => Ok(Self::One),
            3 => Ok(Self::Three),
            5 => Ok(Self::Five),
            9 => Ok(Self::Nine),
            other => Err(anyhow!("unknown lean zstd-level code {other}")),
        }
    }

    #[cfg(test)]
    const fn header_code(self) -> u8 {
        match self {
            Self::One => 0,
            Self::Three => 3,
            Self::Five => 5,
            Self::Nine => 9,
        }
    }

    const fn level(self) -> i32 {
        match self {
            Self::One => 1,
            Self::Three => 3,
            Self::Five => 5,
            Self::Nine => 9,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectCompression {
    Raw,
    Zstd,
    Adaptive,
}

impl ObjectCompression {
    const fn name(self, level: ZstdLevel) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Zstd => match level {
                ZstdLevel::One => "zstd1",
                ZstdLevel::Three => "zstd3",
                ZstdLevel::Five => "zstd5",
                ZstdLevel::Nine => "zstd9",
            },
            Self::Adaptive => match level {
                ZstdLevel::One => "adaptive-zstd1-when-smaller",
                ZstdLevel::Three => "adaptive-zstd3-when-smaller",
                ZstdLevel::Five => "adaptive-zstd5-when-smaller",
                ZstdLevel::Nine => "adaptive-zstd9-when-smaller",
            },
        }
    }
}

impl CompressionMode {
    fn from_code(code: u8) -> Result<Self> {
        match code {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Zstd),
            2 => Ok(Self::Adaptive),
            3 => Ok(Self::Hybrid),
            other => Err(anyhow!("unknown lean compression mode {other}")),
        }
    }

    #[cfg(test)]
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

    const fn policy_name(self, level: ZstdLevel) -> &'static str {
        match self {
            Self::Raw => "raw-all",
            Self::Zstd => match level {
                ZstdLevel::One => "zstd1-all-nonempty",
                ZstdLevel::Three => "zstd3-all-nonempty",
                ZstdLevel::Five => "zstd5-all-nonempty",
                ZstdLevel::Nine => "zstd9-all-nonempty",
            },
            Self::Adaptive => match level {
                ZstdLevel::One => "adaptive-zstd1-when-smaller-all-nonempty",
                ZstdLevel::Three => "adaptive-zstd3-when-smaller-all-nonempty",
                ZstdLevel::Five => "adaptive-zstd5-when-smaller-all-nonempty",
                ZstdLevel::Nine => "adaptive-zstd9-when-smaller-all-nonempty",
            },
            Self::Hybrid => match level {
                ZstdLevel::One => {
                    "hybrid-v1-directory-and-five-dense-zstd1-two-sparse-adaptive-zstd1-when-smaller-block-rewards-raw-no-attempt"
                }
                ZstdLevel::Three => {
                    "hybrid-v1-directory-and-five-dense-zstd3-two-sparse-adaptive-zstd3-when-smaller-block-rewards-raw-no-attempt"
                }
                ZstdLevel::Five => {
                    "hybrid-v1-directory-and-five-dense-zstd5-two-sparse-adaptive-zstd5-when-smaller-block-rewards-raw-no-attempt"
                }
                ZstdLevel::Nine => {
                    "hybrid-v1-directory-and-five-dense-zstd9-two-sparse-adaptive-zstd9-when-smaller-block-rewards-raw-no-attempt"
                }
            },
        }
    }

    const fn object_compression(self, object: LeanObject) -> ObjectCompression {
        match self {
            Self::Raw => ObjectCompression::Raw,
            Self::Zstd => ObjectCompression::Zstd,
            Self::Adaptive => ObjectCompression::Adaptive,
            Self::Hybrid => match object {
                LeanObject::TransactionDirectory
                | LeanObject::InnerInstructions
                | LeanObject::Logs
                | LeanObject::TokenBalances
                | LeanObject::Balances
                | LeanObject::Outcomes => ObjectCompression::Zstd,
                LeanObject::TransactionRewards | LeanObject::RawMetadataFallbacks => {
                    ObjectCompression::Adaptive
                }
                LeanObject::BlockRewards => ObjectCompression::Raw,
            },
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Read-only throughput benchmark for provisional Archive V2 lean block chunks")]
struct Args {
    /// Candidate directory written by archive-v2-account-projection.
    #[arg(long)]
    candidate: PathBuf,

    /// One object to scan, or all objects in fixed wire order.
    #[arg(long, value_enum, default_value_t = ObjectArg::All)]
    object: ObjectArg,

    /// Decode worker count. The reader accepts only the measured 1, 4, and 12 worker modes.
    #[arg(long, default_value_t = 12, value_parser = parse_workers)]
    workers: usize,

    /// Number of complete scans for each selected object.
    #[arg(long, default_value_t = 1, value_parser = parse_iterations)]
    iterations: u32,
}

fn parse_workers(value: &str) -> std::result::Result<usize, String> {
    let workers = value
        .parse::<usize>()
        .map_err(|error| format!("invalid worker count: {error}"))?;
    if matches!(workers, 1 | 4 | 12) {
        Ok(workers)
    } else {
        Err("workers must be 1, 4, or 12".to_owned())
    }
}

fn parse_iterations(value: &str) -> std::result::Result<u32, String> {
    let iterations = value
        .parse::<u32>()
        .map_err(|error| format!("invalid iteration count: {error}"))?;
    if (1..=MAX_ITERATIONS).contains(&iterations) {
        Ok(iterations)
    } else {
        Err(format!("iterations must be in 1..={MAX_ITERATIONS}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LeanHeader {
    compression: CompressionMode,
    zstd_level: ZstdLevel,
    message_schema: u8,
    metadata_schema: u8,
    outer_schema: u8,
    epoch: u64,
    slots_per_epoch: u64,
    selected_blocks: u64,
    selected_transactions: u64,
    prefix: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Locator {
    offset: u64,
    stored_len: u32,
    decoded_len: u32,
    zstd: bool,
}

#[derive(Debug, Clone, Copy)]
struct IndexRow {
    block_id: u32,
    locators: [Locator; OBJECT_COUNT],
}

#[derive(Debug)]
struct CandidateIndex {
    header: LeanHeader,
    rows: Vec<IndexRow>,
    object_ends: [u64; OBJECT_COUNT],
}

fn take<const N: usize>(cursor: &mut &[u8], field: &str) -> Result<[u8; N]> {
    ensure!(cursor.len() >= N, "truncated lean {field}");
    let (head, tail) = cursor.split_at(N);
    *cursor = tail;
    Ok(head.try_into().expect("fixed-length split"))
}

fn read_u16(cursor: &mut &[u8], field: &str) -> Result<u16> {
    Ok(u16::from_le_bytes(take(cursor, field)?))
}

fn read_u32(cursor: &mut &[u8], field: &str) -> Result<u32> {
    Ok(u32::from_le_bytes(take(cursor, field)?))
}

fn read_u64(cursor: &mut &[u8], field: &str) -> Result<u64> {
    Ok(u64::from_le_bytes(take(cursor, field)?))
}

fn parse_header(bytes: &[u8], magic: [u8; 8], object: u16) -> Result<LeanHeader> {
    ensure!(
        bytes.len() == FILE_HEADER_LEN,
        "lean file header is not 64 bytes"
    );
    let mut cursor = bytes;
    ensure!(
        take::<8>(&mut cursor, "magic")? == magic,
        "lean file magic differs"
    );
    ensure!(
        read_u16(&mut cursor, "version")? == FORMAT_VERSION,
        "unknown lean format version"
    );
    ensure!(
        read_u16(&mut cursor, "object")? == object,
        "lean object id differs"
    );
    let compression = CompressionMode::from_code(take::<1>(&mut cursor, "mode")?[0])?;
    let message_schema = take::<1>(&mut cursor, "message schema")?[0];
    ensure!(
        message_schema <= 1,
        "unknown lean message schema {message_schema}"
    );
    let metadata_schema = take::<1>(&mut cursor, "metadata schema")?[0];
    ensure!(
        metadata_schema <= 1,
        "unknown lean metadata schema {metadata_schema}"
    );
    let outer_schema = take::<1>(&mut cursor, "outer schema")?[0];
    ensure!(
        outer_schema == 1,
        "unknown lean outer schema {outer_schema}"
    );
    let epoch = read_u64(&mut cursor, "epoch")?;
    let slots_per_epoch = read_u64(&mut cursor, "slots per epoch")?;
    ensure!(slots_per_epoch != 0, "lean slots per epoch is zero");
    let selected_blocks = read_u64(&mut cursor, "selected block count")?;
    ensure!(
        (1..=MAX_BLOCK_COUNT).contains(&selected_blocks),
        "lean selected block count {selected_blocks} exceeds the reader bound {MAX_BLOCK_COUNT}"
    );
    let selected_transactions = read_u64(&mut cursor, "selected transaction count")?;
    ensure!(
        selected_transactions <= MAX_TRANSACTION_COUNT,
        "lean selected transaction count exceeds the reader bound"
    );
    let prefix = match take::<1>(&mut cursor, "prefix flag")?[0] {
        0 => false,
        1 => true,
        other => return Err(anyhow!("invalid lean prefix flag {other}")),
    };
    ensure!(
        read_u16(&mut cursor, "directory row size")? == DIRECTORY_ROW_LEN as u16,
        "lean directory row size differs"
    );
    ensure!(
        take::<1>(&mut cursor, "dense plane count")?[0] == DENSE_PLANE_COUNT,
        "lean dense plane count differs"
    );
    ensure!(
        take::<1>(&mut cursor, "sparse plane count")?[0] == SPARSE_PLANE_COUNT,
        "lean sparse plane count differs"
    );
    ensure!(
        take::<1>(&mut cursor, "object count")?[0] == OBJECT_COUNT as u8,
        "lean object count differs"
    );
    let zstd_level = ZstdLevel::from_header_code(take::<1>(&mut cursor, "zstd level")?[0])?;
    ensure!(
        compression != CompressionMode::Raw || zstd_level == ZstdLevel::One,
        "raw lean compression must use zstd level 1"
    );
    ensure!(
        take::<9>(&mut cursor, "reserved header bytes")? == [0; 9],
        "lean file header has nonzero reserved bytes"
    );
    ensure!(cursor.is_empty(), "lean file header has trailing bytes");
    Ok(LeanHeader {
        compression,
        zstd_level,
        message_schema,
        metadata_schema,
        outer_schema,
        epoch,
        slots_per_epoch,
        selected_blocks,
        selected_transactions,
        prefix,
    })
}

fn open_regular(path: &Path) -> Result<(File, u64)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("stat open file {}", path.display()))?;
    ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    Ok((file, metadata.len()))
}

fn read_exact_at(file: &File, bytes: &mut [u8], offset: u64, label: &str) -> Result<()> {
    file.read_exact_at(bytes, offset)
        .with_context(|| format!("read {label} at offset {offset}"))
}

fn read_index(candidate: &Path) -> Result<CandidateIndex> {
    let path = candidate.join(INDEX_FILE);
    let (file, file_len) = open_regular(&path)?;
    ensure!(
        file_len >= FILE_HEADER_LEN as u64 && file_len <= MAX_INDEX_BYTES as u64,
        "lean index size {file_len} is outside the reader bound"
    );
    let mut bytes = Vec::new();
    let file_len_usize = usize::try_from(file_len).context("lean index size exceeds usize")?;
    bytes
        .try_reserve_exact(file_len_usize)
        .context("reserve lean index")?;
    bytes.resize(file_len_usize, 0);
    read_exact_at(&file, &mut bytes, 0, "lean index")?;
    let header = parse_header(&bytes[..FILE_HEADER_LEN], INDEX_MAGIC, u16::MAX)?;
    let row_count = usize::try_from(header.selected_blocks)
        .context("lean selected block count exceeds usize")?;
    let expected_len = row_count
        .checked_mul(INDEX_ROW_LEN)
        .and_then(|length| length.checked_add(FILE_HEADER_LEN))
        .context("lean index length overflow")?;
    ensure!(
        bytes.len() == expected_len,
        "lean index is {} bytes, expected {expected_len}",
        bytes.len()
    );

    let mut cursor = &bytes[FILE_HEADER_LEN..];
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .context("reserve lean index rows")?;
    let mut object_ends = [FILE_HEADER_LEN as u64; OBJECT_COUNT];
    let mut transaction_count = 0_u64;
    let mut previous_slot = None;
    let epoch_first_slot = header
        .epoch
        .checked_mul(header.slots_per_epoch)
        .context("lean epoch first slot overflow")?;
    let epoch_end_slot = epoch_first_slot
        .checked_add(header.slots_per_epoch)
        .context("lean epoch end slot overflow")?;
    for ordinal in 0..row_count {
        let block_id = read_u32(&mut cursor, "block id")?;
        ensure!(
            block_id as usize == ordinal,
            "lean block id {block_id} differs from row {ordinal}"
        );
        let tx_count = read_u32(&mut cursor, "block transaction count")?;
        transaction_count = transaction_count
            .checked_add(u64::from(tx_count))
            .context("lean transaction total overflow")?;
        let slot = read_u64(&mut cursor, "slot")?;
        ensure!(
            (epoch_first_slot..epoch_end_slot).contains(&slot),
            "lean block {block_id} slot {slot} is outside epoch {} range {epoch_first_slot}..{epoch_end_slot}",
            header.epoch
        );
        if let Some(previous) = previous_slot {
            ensure!(slot > previous, "lean slots are not strictly increasing");
        }
        previous_slot = Some(slot);
        let mut locators = [Locator::default(); OBJECT_COUNT];
        for object in LeanObject::ALL {
            let index = object.index();
            let offset = read_u64(&mut cursor, "object offset")?;
            let packed_len = read_u32(&mut cursor, "stored length and codec")?;
            let decoded_len = read_u32(&mut cursor, "decoded length")?;
            let zstd = packed_len & ZSTD_CODEC_BIT != 0;
            let stored_len = packed_len & STORED_LEN_MASK;
            let object_compression = header.compression.object_compression(object);
            ensure!(
                offset == object_ends[index],
                "{} block {block_id} offset is not gapless",
                object.name()
            );
            ensure!(
                stored_len as usize <= MAX_CHUNK_BYTES && decoded_len as usize <= MAX_CHUNK_BYTES,
                "{} block {block_id} exceeds the {MAX_CHUNK_BYTES}-byte chunk bound",
                object.name()
            );
            if stored_len == 0 || decoded_len == 0 {
                ensure!(
                    stored_len == 0 && decoded_len == 0 && !zstd,
                    "{} block {block_id} has inconsistent zero geometry",
                    object.name()
                );
            } else if zstd {
                ensure!(
                    object_compression != ObjectCompression::Raw,
                    "{} policy requires raw but block {block_id} is zstd",
                    object.name()
                );
                if object_compression == ObjectCompression::Adaptive {
                    ensure!(
                        stored_len < decoded_len,
                        "adaptive {} block {block_id} zstd chunk is not smaller than raw",
                        object.name()
                    );
                }
            } else {
                ensure!(
                    stored_len == decoded_len,
                    "raw {} block {block_id} lengths differ",
                    object.name()
                );
                ensure!(
                    object_compression != ObjectCompression::Zstd,
                    "{} policy requires zstd but block {block_id} is raw",
                    object.name()
                );
            }
            if object == LeanObject::BlockRewards {
                ensure!(
                    stored_len != 0 && decoded_len != 0,
                    "lean block {block_id} is missing its exact block-reward Option field"
                );
            }
            if object == LeanObject::TransactionDirectory {
                let expected = usize::try_from(tx_count)
                    .context("lean block transaction count exceeds usize")?
                    .checked_mul(DIRECTORY_ROW_LEN)
                    .context("lean directory decoded length overflow")?;
                ensure!(
                    decoded_len as usize == expected,
                    "lean block {block_id} directory length differs from tx_count"
                );
            }
            object_ends[index] = offset
                .checked_add(u64::from(stored_len))
                .context("lean object end offset overflow")?;
            locators[index] = Locator {
                offset,
                stored_len,
                decoded_len,
                zstd,
            };
        }
        rows.push(IndexRow { block_id, locators });
    }
    ensure!(cursor.is_empty(), "lean index has trailing bytes");
    ensure!(
        transaction_count == header.selected_transactions,
        "lean index transaction total is {transaction_count}, expected {}",
        header.selected_transactions
    );
    Ok(CandidateIndex {
        header,
        rows,
        object_ends,
    })
}

fn open_object(candidate: &Path, index: &CandidateIndex, object: LeanObject) -> Result<File> {
    let path = candidate.join(object.file_name());
    let (file, file_len) = open_regular(&path)?;
    ensure!(
        file_len == index.object_ends[object.index()],
        "{} is {file_len} bytes, expected {}",
        object.name(),
        index.object_ends[object.index()]
    );
    let mut header_bytes = [0_u8; FILE_HEADER_LEN];
    read_exact_at(&file, &mut header_bytes, 0, object.name())?;
    let header = parse_header(&header_bytes, DATA_MAGIC, object as u16)?;
    ensure!(
        header == index.header,
        "{} header binding differs from the lean index",
        object.name()
    );
    Ok(file)
}

#[derive(Debug)]
struct Task {
    block_id: u32,
    locator: Locator,
    stored: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct ScanMemoryPlan {
    max_stored_capacity: usize,
    max_decoded_capacity: usize,
    per_worker_capacity_limit: usize,
}

fn scan_memory_plan(
    rows: &[IndexRow],
    object: LeanObject,
    workers: usize,
) -> Result<ScanMemoryPlan> {
    let mut max_stored_capacity = 0usize;
    let mut max_decoded_capacity = 0usize;
    for row in rows {
        let locator = row.locators[object.index()];
        max_stored_capacity = max_stored_capacity.max(locator.stored_len as usize);
        max_decoded_capacity = max_decoded_capacity.max(locator.decoded_len as usize);
    }
    let per_worker_capacity_limit = MAX_AGGREGATE_WORKER_BUFFER_BYTES / workers;
    let admitted_per_worker = max_stored_capacity
        .checked_add(max_decoded_capacity)
        .context("lean per-worker buffer admission overflow")?;
    ensure!(
        admitted_per_worker <= per_worker_capacity_limit,
        "{} needs up to {admitted_per_worker} stored-plus-decoded bytes per worker; {workers} workers allow {per_worker_capacity_limit} bytes each under the {MAX_AGGREGATE_WORKER_BUFFER_BYTES}-byte aggregate cap",
        object.name()
    );
    Ok(ScanMemoryPlan {
        max_stored_capacity,
        max_decoded_capacity,
        per_worker_capacity_limit,
    })
}

fn trim_and_check_capacity(bytes: &mut Vec<u8>, limit: usize, label: &str) -> Result<()> {
    if bytes.capacity() > limit {
        bytes.shrink_to(limit);
    }
    ensure!(
        bytes.capacity() <= limit,
        "{label} retained capacity {} exceeds its {limit}-byte admission",
        bytes.capacity()
    );
    Ok(())
}

#[derive(Debug, Default)]
struct WorkerStats {
    blocks: u64,
    raw_blocks: u64,
    zstd_blocks: u64,
    stored_bytes: u64,
    decoded_bytes: u64,
    decode_sum: Duration,
    first_decode: Option<Duration>,
    last_decode: Duration,
    max_stored_capacity: usize,
    max_decoded_capacity: usize,
    retained_decoded_capacity: usize,
}

struct WorkerFailure {
    block_id: u32,
    error: anyhow::Error,
}

struct WorkerOutcome {
    stats: WorkerStats,
    failure: Option<WorkerFailure>,
}

fn record_lowest_failure(lowest: &mut Option<WorkerFailure>, block_id: u32, error: anyhow::Error) {
    if lowest
        .as_ref()
        .is_none_or(|failure| block_id < failure.block_id)
    {
        *lowest = Some(WorkerFailure { block_id, error });
    }
}

fn decode_task(
    task: &Task,
    decoded: &mut Vec<u8>,
    decompressor: &mut Option<zstd::bulk::Decompressor<'static>>,
    memory: ScanMemoryPlan,
) -> Result<()> {
    #[cfg(test)]
    if task.block_id == TEST_DELAY_DECODE_BLOCK.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_micros(
            TEST_DELAY_DECODE_MICROS.load(Ordering::Relaxed),
        ));
    }
    if task.locator.zstd {
        let frame_len =
            zstd::zstd_safe::find_frame_compressed_size(&task.stored).map_err(|code| {
                anyhow!(
                    "block {} has invalid zstd: {}",
                    task.block_id,
                    zstd::zstd_safe::get_error_name(code)
                )
            })?;
        ensure!(
            frame_len == task.stored.len(),
            "block {} zstd chunk has trailing bytes",
            task.block_id
        );
        decoded.clear();
        let decoded_len = task.locator.decoded_len as usize;
        if decoded.capacity() < decoded_len {
            decoded
                .try_reserve_exact(decoded_len)
                .context("reserve decoded lean chunk")?;
        }
        trim_and_check_capacity(
            decoded,
            memory.max_decoded_capacity,
            "decoded worker buffer",
        )?;
        let combined_capacity = task
            .stored
            .capacity()
            .checked_add(decoded.capacity())
            .context("lean worker buffer capacity overflow")?;
        ensure!(
            combined_capacity <= memory.per_worker_capacity_limit,
            "block {} retained stored-plus-decoded capacity {combined_capacity} exceeds the per-worker limit {}",
            task.block_id,
            memory.per_worker_capacity_limit
        );
        if decompressor.is_none() {
            let mut created =
                zstd::bulk::Decompressor::new().context("create zstd decompressor")?;
            created
                .set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(
                    ZSTD_WINDOW_LOG_MAX,
                ))
                .context("set bounded zstd window")?;
            *decompressor = Some(created);
        }
        let written = decompressor
            .as_mut()
            .expect("zstd decompressor was initialized")
            .decompress_to_buffer(&task.stored, decoded)
            .with_context(|| format!("decompress lean block {}", task.block_id))?;
        ensure!(
            written == decoded_len && decoded.len() == decoded_len,
            "block {} decoded length differs from its locator",
            task.block_id
        );
    } else {
        ensure!(
            task.stored.len() == task.locator.decoded_len as usize,
            "raw block {} length differs from its locator",
            task.block_id
        );
        decoded.clear();
        if decoded.capacity() < task.stored.len() {
            decoded
                .try_reserve_exact(task.stored.len())
                .context("reserve raw-copy lean chunk")?;
        }
        trim_and_check_capacity(
            decoded,
            memory.max_decoded_capacity,
            "decoded worker buffer",
        )?;
        let combined_capacity = task
            .stored
            .capacity()
            .checked_add(decoded.capacity())
            .context("lean worker buffer capacity overflow")?;
        ensure!(
            combined_capacity <= memory.per_worker_capacity_limit,
            "block {} retained stored-plus-decoded capacity {combined_capacity} exceeds the per-worker limit {}",
            task.block_id,
            memory.per_worker_capacity_limit
        );
        decoded.extend_from_slice(&task.stored);
    }
    std::hint::black_box(decoded.as_slice());
    Ok(())
}

fn worker_loop(
    receiver: mpsc::Receiver<Task>,
    recycle: mpsc::Sender<Vec<u8>>,
    scan_started: Instant,
    memory: ScanMemoryPlan,
) -> WorkerOutcome {
    let mut stats = WorkerStats::default();
    let mut decoded = Vec::new();
    let mut decompressor = None;
    let mut lowest_failure = None;
    for mut task in receiver {
        let decode_started_offset = scan_started.elapsed();
        let decode_started = Instant::now();
        if let Err(error) = decode_task(&task, &mut decoded, &mut decompressor, memory) {
            record_lowest_failure(&mut lowest_failure, task.block_id, error);
        } else {
            if task.locator.zstd {
                stats.zstd_blocks += 1;
            } else {
                stats.raw_blocks += 1;
            }
            stats.decode_sum = stats.decode_sum.saturating_add(decode_started.elapsed());
            stats.first_decode = Some(stats.first_decode.map_or(decode_started_offset, |first| {
                first.min(decode_started_offset)
            }));
            stats.last_decode = scan_started.elapsed();
            stats.blocks += 1;
            if let Some(total) = stats.stored_bytes.checked_add(task.stored.len() as u64) {
                stats.stored_bytes = total;
            } else {
                record_lowest_failure(
                    &mut lowest_failure,
                    task.block_id,
                    anyhow!("worker stored byte count overflow"),
                );
            }
            if let Some(total) = stats
                .decoded_bytes
                .checked_add(u64::from(task.locator.decoded_len))
            {
                stats.decoded_bytes = total;
            } else {
                record_lowest_failure(
                    &mut lowest_failure,
                    task.block_id,
                    anyhow!("worker decoded byte count overflow"),
                );
            }
        }
        stats.max_stored_capacity = stats.max_stored_capacity.max(task.stored.capacity());
        stats.max_decoded_capacity = stats.max_decoded_capacity.max(decoded.capacity());
        if let Err(error) = trim_and_check_capacity(
            &mut task.stored,
            memory.max_stored_capacity,
            "stored worker buffer",
        ) {
            record_lowest_failure(&mut lowest_failure, task.block_id, error);
        }
        if recycle.send(task.stored).is_err() {
            record_lowest_failure(
                &mut lowest_failure,
                task.block_id,
                anyhow!("lean read buffer recycler stopped"),
            );
            break;
        }
    }
    if let Err(error) = trim_and_check_capacity(
        &mut decoded,
        memory.max_decoded_capacity,
        "decoded worker buffer",
    ) {
        record_lowest_failure(&mut lowest_failure, u32::MAX, error);
    }
    stats.retained_decoded_capacity = decoded.capacity();
    WorkerOutcome {
        stats,
        failure: lowest_failure,
    }
}

#[derive(Debug, Default)]
struct ScanStats {
    blocks: u64,
    raw_blocks: u64,
    zstd_blocks: u64,
    stored_bytes: u64,
    decoded_bytes: u64,
    read_calls: u64,
    scan_wall: Duration,
    producer_wall: Duration,
    range_read_sum: Duration,
    decode_wall: Duration,
    decode_worker_sum: Duration,
    max_stored_capacity: usize,
    max_decoded_capacity: usize,
    aggregate_retained_worker_buffer_capacity: usize,
    aggregate_admitted_worker_buffer_capacity: usize,
}

impl ScanStats {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.blocks = self
            .blocks
            .checked_add(other.blocks)
            .context("block count overflow")?;
        self.raw_blocks = self
            .raw_blocks
            .checked_add(other.raw_blocks)
            .context("raw block count overflow")?;
        self.zstd_blocks = self
            .zstd_blocks
            .checked_add(other.zstd_blocks)
            .context("zstd block count overflow")?;
        self.stored_bytes = self
            .stored_bytes
            .checked_add(other.stored_bytes)
            .context("stored byte count overflow")?;
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(other.decoded_bytes)
            .context("decoded byte count overflow")?;
        self.read_calls = self
            .read_calls
            .checked_add(other.read_calls)
            .context("read call count overflow")?;
        self.scan_wall = self.scan_wall.saturating_add(other.scan_wall);
        self.producer_wall = self.producer_wall.saturating_add(other.producer_wall);
        self.range_read_sum = self.range_read_sum.saturating_add(other.range_read_sum);
        self.decode_wall = self.decode_wall.saturating_add(other.decode_wall);
        self.decode_worker_sum = self
            .decode_worker_sum
            .saturating_add(other.decode_worker_sum);
        self.max_stored_capacity = self.max_stored_capacity.max(other.max_stored_capacity);
        self.max_decoded_capacity = self.max_decoded_capacity.max(other.max_decoded_capacity);
        self.aggregate_retained_worker_buffer_capacity = self
            .aggregate_retained_worker_buffer_capacity
            .max(other.aggregate_retained_worker_buffer_capacity);
        self.aggregate_admitted_worker_buffer_capacity = self
            .aggregate_admitted_worker_buffer_capacity
            .max(other.aggregate_admitted_worker_buffer_capacity);
        Ok(())
    }
}

fn resize_for_read(bytes: &mut Vec<u8>, length: usize, capacity_limit: usize) -> Result<()> {
    ensure!(
        length <= MAX_CHUNK_BYTES,
        "stored chunk exceeds the reader bound"
    );
    bytes.clear();
    if bytes.capacity() < length {
        bytes
            .try_reserve_exact(length)
            .context("reserve stored lean chunk")?;
    }
    trim_and_check_capacity(bytes, capacity_limit, "stored worker buffer")?;
    bytes.resize(length, 0);
    Ok(())
}

fn scan_once(
    file: &File,
    rows: &[IndexRow],
    object: LeanObject,
    workers: usize,
    memory: ScanMemoryPlan,
) -> Result<ScanStats> {
    let scan_started = Instant::now();
    let (recycle_sender, recycle_receiver) = mpsc::channel::<Vec<u8>>();
    for _ in 0..workers {
        recycle_sender
            .send(Vec::new())
            .expect("the recycle receiver is live");
    }

    thread::scope(|scope| {
        let mut task_senders = Vec::with_capacity(workers);
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (sender, receiver) = mpsc::sync_channel::<Task>(1);
            task_senders.push(sender);
            let recycle = recycle_sender.clone();
            handles.push(scope.spawn(move || worker_loop(receiver, recycle, scan_started, memory)));
        }
        drop(recycle_sender);

        let mut read_sum = Duration::ZERO;
        let mut read_calls = 0_u64;
        let mut producer_error = None;
        let mut dispatched = 0usize;
        let producer_started = scan_started.elapsed();
        for row in rows {
            let locator = row.locators[object.index()];
            if locator.stored_len == 0 {
                continue;
            }
            let mut stored = match recycle_receiver.recv() {
                Ok(bytes) => bytes,
                Err(_) => {
                    producer_error = Some(WorkerFailure {
                        block_id: row.block_id,
                        error: anyhow!("all lean read workers stopped"),
                    });
                    break;
                }
            };
            if let Err(error) = resize_for_read(
                &mut stored,
                locator.stored_len as usize,
                memory.max_stored_capacity,
            ) {
                producer_error = Some(WorkerFailure {
                    block_id: row.block_id,
                    error,
                });
                break;
            }
            let read_started = Instant::now();
            if let Err(error) = read_exact_at(file, &mut stored, locator.offset, object.name()) {
                producer_error = Some(WorkerFailure {
                    block_id: row.block_id,
                    error,
                });
                break;
            }
            read_sum = read_sum.saturating_add(read_started.elapsed());
            read_calls = match read_calls.checked_add(1) {
                Some(count) => count,
                None => {
                    producer_error = Some(WorkerFailure {
                        block_id: row.block_id,
                        error: anyhow!("lean read call count overflow"),
                    });
                    break;
                }
            };
            let worker = dispatched % workers;
            #[cfg(test)]
            TEST_BEFORE_SEND_DELAY.with(|delay| thread::sleep(delay.get()));
            if task_senders[worker]
                .send(Task {
                    block_id: row.block_id,
                    locator,
                    stored,
                })
                .is_err()
            {
                producer_error = Some(WorkerFailure {
                    block_id: row.block_id,
                    error: anyhow!("lean read worker {worker} stopped"),
                });
                break;
            }
            dispatched += 1;
        }
        drop(task_senders);
        let producer_wall = scan_started.elapsed().saturating_sub(producer_started);

        let mut worker_outcomes = Vec::with_capacity(workers);
        let mut panic_error = None;
        for (worker, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(outcome) => worker_outcomes.push(outcome),
                Err(_) if panic_error.is_none() => {
                    panic_error = Some(anyhow!("lean read worker {worker} panicked"));
                }
                Err(_) => {}
            }
        }
        if let Some(error) = panic_error {
            return Err(error);
        }
        let mut lowest_failure = producer_error;
        for outcome in &mut worker_outcomes {
            if let Some(failure) = outcome.failure.take() {
                record_lowest_failure(&mut lowest_failure, failure.block_id, failure.error);
            }
        }
        if let Some(failure) = lowest_failure {
            return Err(failure.error.context(format!(
                "{} scan first failed at block {}",
                object.name(),
                failure.block_id
            )));
        }

        let retained_stored_capacity = recycle_receiver.try_iter().try_fold(
            (0usize, 0usize),
            |(count, capacity), bytes| -> Result<_> {
                Ok((
                    count
                        .checked_add(1)
                        .context("stored buffer count overflow")?,
                    capacity
                        .checked_add(bytes.capacity())
                        .context("retained stored capacity overflow")?,
                ))
            },
        )?;
        ensure!(
            retained_stored_capacity.0 == workers,
            "lean stored buffer pool retained {} buffers, expected {workers}",
            retained_stored_capacity.0
        );

        let mut stats = ScanStats {
            read_calls,
            scan_wall: scan_started.elapsed(),
            producer_wall,
            range_read_sum: read_sum,
            ..ScanStats::default()
        };
        let mut first_decode = None;
        let mut last_decode = Duration::ZERO;
        let mut retained_decoded_capacity = 0usize;
        for outcome in worker_outcomes {
            let worker = outcome.stats;
            stats.blocks += worker.blocks;
            stats.raw_blocks += worker.raw_blocks;
            stats.zstd_blocks += worker.zstd_blocks;
            stats.stored_bytes += worker.stored_bytes;
            stats.decoded_bytes += worker.decoded_bytes;
            stats.decode_worker_sum = stats.decode_worker_sum.saturating_add(worker.decode_sum);
            stats.max_stored_capacity = stats.max_stored_capacity.max(worker.max_stored_capacity);
            stats.max_decoded_capacity =
                stats.max_decoded_capacity.max(worker.max_decoded_capacity);
            retained_decoded_capacity = retained_decoded_capacity
                .checked_add(worker.retained_decoded_capacity)
                .context("retained decoded capacity overflow")?;
            if let Some(worker_first) = worker.first_decode {
                first_decode = Some(
                    first_decode.map_or(worker_first, |first: Duration| first.min(worker_first)),
                );
                last_decode = last_decode.max(worker.last_decode);
            }
        }
        stats.decode_wall =
            first_decode.map_or(Duration::ZERO, |first| last_decode.saturating_sub(first));
        stats.aggregate_retained_worker_buffer_capacity = retained_stored_capacity
            .1
            .checked_add(retained_decoded_capacity)
            .context("aggregate retained worker buffer capacity overflow")?;
        ensure!(
            stats.aggregate_retained_worker_buffer_capacity <= MAX_AGGREGATE_WORKER_BUFFER_BYTES,
            "aggregate retained worker buffer capacity {} exceeds the {MAX_AGGREGATE_WORKER_BUFFER_BYTES}-byte cap",
            stats.aggregate_retained_worker_buffer_capacity
        );
        stats.aggregate_admitted_worker_buffer_capacity = memory
            .max_stored_capacity
            .checked_add(memory.max_decoded_capacity)
            .and_then(|capacity| capacity.checked_mul(workers))
            .context("aggregate admitted worker buffer capacity overflow")?;
        Ok(stats)
    })
}

#[derive(Debug, Serialize)]
struct ObjectReport {
    object: &'static str,
    file: &'static str,
    declared_compression_policy: &'static str,
    blocks_per_iteration: u64,
    empty_blocks_per_iteration: u64,
    raw_blocks_per_iteration: u64,
    zstd_blocks_per_iteration: u64,
    stored_bytes_per_iteration: u64,
    decoded_bytes_per_iteration: u64,
    scanned_blocks: u64,
    scanned_stored_bytes: u64,
    scanned_decoded_bytes: u64,
    read_calls: u64,
    scan_wall_ms: u64,
    producer_wall_ms: u64,
    read_wall_ms: u64,
    decode_wall_ms: u64,
    decode_worker_sum_ms: u64,
    stored_mib_per_second: f64,
    decoded_mib_per_second: f64,
    max_stored_worker_buffer_capacity_bytes: usize,
    max_decoded_worker_buffer_capacity_bytes: usize,
    admitted_aggregate_worker_buffer_capacity_bytes: usize,
    max_retained_aggregate_worker_buffer_capacity_bytes: usize,
    aggregate_worker_buffer_capacity_limit_bytes: usize,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    status: &'static str,
    mutation: &'static str,
    content_hashing: &'static str,
    candidate: String,
    object_selection: &'static str,
    workers: usize,
    iterations: u32,
    compression_mode: &'static str,
    compression_policy: &'static str,
    zstd_level: i32,
    epoch: u64,
    slots_per_epoch: u64,
    prefix: bool,
    selected_blocks: u64,
    selected_transactions: u64,
    total_scan_wall_ms: u64,
    objects: Vec<ObjectReport>,
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn mib_per_second(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        0.0
    } else {
        bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
    }
}

fn geometry(rows: &[IndexRow], object: LeanObject) -> Result<(u64, u64, u64, u64)> {
    rows.iter().try_fold(
        (0_u64, 0_u64, 0_u64, 0_u64),
        |(empty, raw, zstd, stored), row| {
            let locator = row.locators[object.index()];
            Ok(if locator.stored_len == 0 {
                (
                    empty.checked_add(1).context("empty block count overflow")?,
                    raw,
                    zstd,
                    stored,
                )
            } else if locator.zstd {
                (
                    empty,
                    raw,
                    zstd.checked_add(1).context("zstd block count overflow")?,
                    stored
                        .checked_add(u64::from(locator.stored_len))
                        .context("stored byte count overflow")?,
                )
            } else {
                (
                    empty,
                    raw.checked_add(1).context("raw block count overflow")?,
                    zstd,
                    stored
                        .checked_add(u64::from(locator.stored_len))
                        .context("stored byte count overflow")?,
                )
            })
        },
    )
}

fn decoded_geometry(rows: &[IndexRow], object: LeanObject) -> Result<u64> {
    rows.iter().try_fold(0_u64, |total, row| {
        total
            .checked_add(u64::from(row.locators[object.index()].decoded_len))
            .context("decoded byte count overflow")
    })
}

fn benchmark_object(
    candidate: &Path,
    index: &CandidateIndex,
    object: LeanObject,
    workers: usize,
    iterations: u32,
) -> Result<ObjectReport> {
    let file = open_object(candidate, index, object)?;
    let (empty, raw, zstd, stored) = geometry(&index.rows, object)?;
    let decoded = decoded_geometry(&index.rows, object)?;
    let memory = scan_memory_plan(&index.rows, object, workers)?;
    let mut aggregate = ScanStats::default();
    for _ in 0..iterations {
        aggregate.merge(scan_once(&file, &index.rows, object, workers, memory)?)?;
    }
    let multiplier = u64::from(iterations);
    ensure!(
        aggregate.blocks == (raw + zstd) * multiplier
            && aggregate.raw_blocks == raw * multiplier
            && aggregate.zstd_blocks == zstd * multiplier
            && aggregate.stored_bytes == stored * multiplier
            && aggregate.decoded_bytes == decoded * multiplier,
        "{} scan totals differ from the index",
        object.name()
    );
    Ok(ObjectReport {
        object: object.name(),
        file: object.file_name(),
        declared_compression_policy: index
            .header
            .compression
            .object_compression(object)
            .name(index.header.zstd_level),
        blocks_per_iteration: index.header.selected_blocks,
        empty_blocks_per_iteration: empty,
        raw_blocks_per_iteration: raw,
        zstd_blocks_per_iteration: zstd,
        stored_bytes_per_iteration: stored,
        decoded_bytes_per_iteration: decoded,
        scanned_blocks: aggregate.blocks,
        scanned_stored_bytes: aggregate.stored_bytes,
        scanned_decoded_bytes: aggregate.decoded_bytes,
        read_calls: aggregate.read_calls,
        scan_wall_ms: duration_millis(aggregate.scan_wall),
        producer_wall_ms: duration_millis(aggregate.producer_wall),
        read_wall_ms: duration_millis(aggregate.range_read_sum),
        decode_wall_ms: duration_millis(aggregate.decode_wall),
        decode_worker_sum_ms: duration_millis(aggregate.decode_worker_sum),
        stored_mib_per_second: mib_per_second(aggregate.stored_bytes, aggregate.scan_wall),
        decoded_mib_per_second: mib_per_second(aggregate.decoded_bytes, aggregate.scan_wall),
        max_stored_worker_buffer_capacity_bytes: aggregate.max_stored_capacity,
        max_decoded_worker_buffer_capacity_bytes: aggregate.max_decoded_capacity,
        admitted_aggregate_worker_buffer_capacity_bytes: aggregate
            .aggregate_admitted_worker_buffer_capacity,
        max_retained_aggregate_worker_buffer_capacity_bytes: aggregate
            .aggregate_retained_worker_buffer_capacity,
        aggregate_worker_buffer_capacity_limit_bytes: MAX_AGGREGATE_WORKER_BUFFER_BYTES,
    })
}

fn run(args: Args) -> Result<BenchmarkReport> {
    ensure!(
        matches!(args.workers, 1 | 4 | 12),
        "workers must be 1, 4, or 12"
    );
    ensure!(args.candidate.is_dir(), "candidate must be a directory");
    let candidate = fs::canonicalize(&args.candidate)
        .with_context(|| format!("canonicalize candidate {}", args.candidate.display()))?;
    let index = read_index(&candidate)?;
    for object in LeanObject::ALL {
        drop(open_object(&candidate, &index, object)?);
    }
    let selected = args.object.objects();
    let started = Instant::now();
    let mut objects = Vec::new();
    objects
        .try_reserve_exact(selected.len())
        .context("reserve object reports")?;
    for object in selected {
        objects.push(benchmark_object(
            &candidate,
            &index,
            object,
            args.workers,
            args.iterations,
        )?);
    }
    Ok(BenchmarkReport {
        status: STATUS,
        mutation: "none",
        content_hashing: "none",
        candidate: candidate.display().to_string(),
        object_selection: args.object.name(),
        workers: args.workers,
        iterations: args.iterations,
        compression_mode: index.header.compression.name(),
        compression_policy: index
            .header
            .compression
            .policy_name(index.header.zstd_level),
        zstd_level: index.header.zstd_level.level(),
        epoch: index.header.epoch,
        slots_per_epoch: index.header.slots_per_epoch,
        prefix: index.header.prefix,
        selected_blocks: index.header.selected_blocks,
        selected_transactions: index.header.selected_transactions,
        total_scan_wall_ms: duration_millis(started.elapsed()),
        objects,
    })
}

fn main() -> Result<()> {
    let report = run(Args::parse())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).context("encode lean read report")?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[derive(Clone)]
    struct FixtureChunk {
        decoded: Vec<u8>,
        zstd: bool,
    }

    fn write_header_at_level(
        writer: &mut impl Write,
        magic: [u8; 8],
        object: u16,
        mode: CompressionMode,
        zstd_level: ZstdLevel,
        blocks: u64,
        transactions: u64,
    ) {
        writer.write_all(&magic).unwrap();
        writer.write_all(&FORMAT_VERSION.to_le_bytes()).unwrap();
        writer.write_all(&object.to_le_bytes()).unwrap();
        writer.write_all(&[mode.code(), 0, 0, 1]).unwrap();
        writer.write_all(&7_u64.to_le_bytes()).unwrap();
        writer.write_all(&100_u64.to_le_bytes()).unwrap();
        writer.write_all(&blocks.to_le_bytes()).unwrap();
        writer.write_all(&transactions.to_le_bytes()).unwrap();
        writer.write_all(&[1]).unwrap();
        writer
            .write_all(&(DIRECTORY_ROW_LEN as u16).to_le_bytes())
            .unwrap();
        writer
            .write_all(&[DENSE_PLANE_COUNT, SPARSE_PLANE_COUNT, OBJECT_COUNT as u8])
            .unwrap();
        writer.write_all(&[zstd_level.header_code()]).unwrap();
        writer.write_all(&[0; 9]).unwrap();
    }

    fn fixture_chunks(mode: CompressionMode, blocks: usize) -> Vec<[FixtureChunk; OBJECT_COUNT]> {
        (0..blocks)
            .map(|block| {
                array::from_fn(|object| {
                    let decoded = if object == LeanObject::TransactionDirectory.index() {
                        vec![block as u8; DIRECTORY_ROW_LEN]
                    } else if object == LeanObject::RawMetadataFallbacks.index() && block % 3 == 0 {
                        Vec::new()
                    } else if block % 2 == 0 {
                        vec![object as u8 + 1]
                    } else {
                        vec![object as u8; 1_024]
                    };
                    let zstd = !decoded.is_empty()
                        && match mode {
                            CompressionMode::Raw => false,
                            CompressionMode::Zstd => true,
                            CompressionMode::Adaptive => block % 2 == 1,
                            CompressionMode::Hybrid => match LeanObject::ALL[object] {
                                LeanObject::TransactionDirectory
                                | LeanObject::InnerInstructions
                                | LeanObject::Logs
                                | LeanObject::TokenBalances
                                | LeanObject::Balances
                                | LeanObject::Outcomes => true,
                                LeanObject::TransactionRewards
                                | LeanObject::RawMetadataFallbacks => block % 2 == 1,
                                LeanObject::BlockRewards => false,
                            },
                        };
                    FixtureChunk { decoded, zstd }
                })
            })
            .collect()
    }

    fn write_fixture(root: &Path, mode: CompressionMode, blocks: usize) {
        write_fixture_at_level(root, mode, ZstdLevel::One, blocks);
    }

    fn write_fixture_at_level(
        root: &Path,
        mode: CompressionMode,
        zstd_level: ZstdLevel,
        blocks: usize,
    ) {
        let chunks = fixture_chunks(mode, blocks);
        let mut stored_by_object: [Vec<Vec<u8>>; OBJECT_COUNT] = array::from_fn(|_| Vec::new());
        for block in &chunks {
            for object in LeanObject::ALL {
                let chunk = &block[object.index()];
                stored_by_object[object.index()].push(if chunk.zstd {
                    zstd::bulk::compress(&chunk.decoded, zstd_level.level()).unwrap()
                } else {
                    chunk.decoded.clone()
                });
            }
        }
        for object in LeanObject::ALL {
            let mut file = File::create(root.join(object.file_name())).unwrap();
            write_header_at_level(
                &mut file,
                DATA_MAGIC,
                object as u16,
                mode,
                zstd_level,
                blocks as u64,
                blocks as u64,
            );
            for stored in &stored_by_object[object.index()] {
                file.write_all(stored).unwrap();
            }
        }

        let mut index = File::create(root.join(INDEX_FILE)).unwrap();
        write_header_at_level(
            &mut index,
            INDEX_MAGIC,
            u16::MAX,
            mode,
            zstd_level,
            blocks as u64,
            blocks as u64,
        );
        let mut offsets = [FILE_HEADER_LEN as u64; OBJECT_COUNT];
        for (block_id, block) in chunks.iter().enumerate() {
            index.write_all(&(block_id as u32).to_le_bytes()).unwrap();
            index.write_all(&1_u32.to_le_bytes()).unwrap();
            index
                .write_all(&(700_u64 + block_id as u64).to_le_bytes())
                .unwrap();
            for object in LeanObject::ALL {
                let object_index = object.index();
                let stored = &stored_by_object[object_index][block_id];
                let chunk = &block[object_index];
                index
                    .write_all(&offsets[object_index].to_le_bytes())
                    .unwrap();
                let stored_len = stored.len() as u32;
                let packed = stored_len | (u32::from(chunk.zstd) * ZSTD_CODEC_BIT);
                index.write_all(&packed.to_le_bytes()).unwrap();
                index
                    .write_all(&(chunk.decoded.len() as u32).to_le_bytes())
                    .unwrap();
                offsets[object_index] += u64::from(stored_len);
            }
        }
    }

    fn args(candidate: &Path, object: ObjectArg, workers: usize) -> Args {
        Args {
            candidate: candidate.to_owned(),
            object,
            workers,
            iterations: 1,
        }
    }

    fn decode_hex(encoded: &str) -> Vec<u8> {
        assert_eq!(encoded.len() % 2, 0);
        encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("invalid golden hex"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }

    #[test]
    fn frozen_converter_header_and_index_wire_is_accepted() {
        // This byte fixture is independent of write_header/write_fixture and freezes the B6 lean
        // converter's 64-byte headers plus one 160-byte raw locator row.
        let index_bytes = decode_hex(concat!(
            "425a56324c4930310100ffff00000001",
            "07000000000000006400000000000000",
            "01000000000000000100000000000000",
            "01180005020900000000000000000000",
            "0000000001000000bc02000000000000",
            "40000000000000001800000018000000",
            "40000000000000000100000001000000",
            "40000000000000000100000001000000",
            "40000000000000000100000001000000",
            "40000000000000000100000001000000",
            "40000000000000000100000001000000",
            "40000000000000000100000001000000",
            "40000000000000000000000000000000",
            "40000000000000000100000001000000",
        ));
        assert_eq!(index_bytes.len(), FILE_HEADER_LEN + INDEX_ROW_LEN);
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join(INDEX_FILE), index_bytes).unwrap();
        let index = read_index(directory.path()).unwrap();
        assert_eq!(index.header.epoch, 7);
        assert_eq!(index.rows.len(), 1);
        assert_eq!(index.rows[0].block_id, 0);
        assert_eq!(
            index.rows[0].locators[LeanObject::TransactionDirectory.index()].decoded_len,
            DIRECTORY_ROW_LEN as u32
        );

        let logs_header = decode_hex(concat!(
            "425a56324c4e30310100020000000001",
            "07000000000000006400000000000000",
            "01000000000000000100000000000000",
            "01180005020900000000000000000000",
        ));
        assert_eq!(logs_header.len(), FILE_HEADER_LEN);
        assert_eq!(
            parse_header(&logs_header, DATA_MAGIC, LeanObject::Logs as u16).unwrap(),
            index.header
        );
    }

    #[test]
    fn raw_zstd_and_adaptive_fixtures_scan_exactly() {
        for mode in [
            CompressionMode::Raw,
            CompressionMode::Zstd,
            CompressionMode::Adaptive,
            CompressionMode::Hybrid,
        ] {
            let directory = tempfile::tempdir().unwrap();
            write_fixture(directory.path(), mode, 4);
            let mut arguments = args(directory.path(), ObjectArg::All, 4);
            arguments.iterations = if mode == CompressionMode::Raw { 2 } else { 1 };
            let report = run(arguments).unwrap();
            assert_eq!(report.compression_mode, mode.name());
            assert_eq!(report.zstd_level, 1);
            assert_eq!(report.compression_policy, mode.policy_name(ZstdLevel::One));
            assert_eq!(report.objects.len(), OBJECT_COUNT);
            for (object_index, object) in report.objects.iter().enumerate() {
                assert_eq!(
                    object.declared_compression_policy,
                    mode.object_compression(LeanObject::ALL[object_index])
                        .name(ZstdLevel::One)
                );
                assert_eq!(object.blocks_per_iteration, 4);
                assert_eq!(
                    object.raw_blocks_per_iteration + object.zstd_blocks_per_iteration,
                    object.blocks_per_iteration - object.empty_blocks_per_iteration
                );
                assert_eq!(
                    object.scanned_stored_bytes,
                    object.stored_bytes_per_iteration * u64::from(report.iterations)
                );
                assert_eq!(
                    object.scanned_decoded_bytes,
                    object.decoded_bytes_per_iteration * u64::from(report.iterations)
                );
                assert!(
                    object.max_retained_aggregate_worker_buffer_capacity_bytes
                        <= object.admitted_aggregate_worker_buffer_capacity_bytes
                );
                assert!(
                    object.admitted_aggregate_worker_buffer_capacity_bytes
                        <= object.aggregate_worker_buffer_capacity_limit_bytes
                );
            }
            let directory_plane = &report.objects[LeanObject::TransactionDirectory.index()];
            assert_eq!(
                directory_plane.decoded_bytes_per_iteration,
                4 * DIRECTORY_ROW_LEN as u64
            );
            match mode {
                CompressionMode::Raw => assert_eq!(directory_plane.raw_blocks_per_iteration, 4),
                CompressionMode::Zstd => assert_eq!(directory_plane.zstd_blocks_per_iteration, 4),
                CompressionMode::Adaptive => {
                    assert_eq!(directory_plane.raw_blocks_per_iteration, 2);
                    assert_eq!(directory_plane.zstd_blocks_per_iteration, 2);
                }
                CompressionMode::Hybrid => {
                    assert_eq!(directory_plane.raw_blocks_per_iteration, 0);
                    assert_eq!(directory_plane.zstd_blocks_per_iteration, 4);
                    let block_rewards = &report.objects[LeanObject::BlockRewards.index()];
                    assert_eq!(block_rewards.raw_blocks_per_iteration, 4);
                    assert_eq!(block_rewards.zstd_blocks_per_iteration, 0);
                    let transaction_rewards =
                        &report.objects[LeanObject::TransactionRewards.index()];
                    assert!(transaction_rewards.raw_blocks_per_iteration != 0);
                    assert!(transaction_rewards.zstd_blocks_per_iteration != 0);
                }
            }
        }
    }

    #[test]
    fn nondefault_zstd_levels_are_bound_reported_and_scanned() {
        for level in [ZstdLevel::Three, ZstdLevel::Five, ZstdLevel::Nine] {
            let directory = tempfile::tempdir().unwrap();
            write_fixture_at_level(directory.path(), CompressionMode::Hybrid, level, 12);
            let report = run(args(directory.path(), ObjectArg::All, 4)).unwrap();
            assert_eq!(report.compression_mode, "hybrid");
            assert_eq!(report.zstd_level, level.level());
            assert_eq!(
                report.compression_policy,
                CompressionMode::Hybrid.policy_name(level)
            );
            for (index, object) in report.objects.iter().enumerate() {
                assert_eq!(
                    object.declared_compression_policy,
                    CompressionMode::Hybrid
                        .object_compression(LeanObject::ALL[index])
                        .name(level)
                );
                assert_eq!(object.blocks_per_iteration, 12);
            }
        }
    }

    #[test]
    fn one_and_twelve_workers_report_identical_counts() {
        for mode in [CompressionMode::Adaptive, CompressionMode::Hybrid] {
            let directory = tempfile::tempdir().unwrap();
            write_fixture(directory.path(), mode, 24);
            let one = run(args(directory.path(), ObjectArg::All, 1)).unwrap();
            let twelve = run(args(directory.path(), ObjectArg::All, 12)).unwrap();
            assert_eq!(one.compression_policy, mode.policy_name(ZstdLevel::One));
            assert_eq!(one.compression_policy, twelve.compression_policy);
            assert_eq!(one.objects.len(), twelve.objects.len());
            for (left, right) in one.objects.iter().zip(&twelve.objects) {
                assert_eq!(left.object, right.object);
                assert_eq!(
                    left.declared_compression_policy,
                    right.declared_compression_policy
                );
                assert_eq!(left.blocks_per_iteration, right.blocks_per_iteration);
                assert_eq!(
                    left.empty_blocks_per_iteration,
                    right.empty_blocks_per_iteration
                );
                assert_eq!(
                    left.raw_blocks_per_iteration,
                    right.raw_blocks_per_iteration
                );
                assert_eq!(
                    left.zstd_blocks_per_iteration,
                    right.zstd_blocks_per_iteration
                );
                assert_eq!(
                    left.stored_bytes_per_iteration,
                    right.stored_bytes_per_iteration
                );
                assert_eq!(
                    left.decoded_bytes_per_iteration,
                    right.decoded_bytes_per_iteration
                );
                assert_eq!(left.scanned_blocks, right.scanned_blocks);
                assert_eq!(left.scanned_stored_bytes, right.scanned_stored_bytes);
                assert_eq!(left.scanned_decoded_bytes, right.scanned_decoded_bytes);
                assert_eq!(left.read_calls, right.read_calls);
            }
        }
    }

    #[test]
    fn producer_wall_includes_dispatch_delay() {
        let directory = tempfile::tempdir().unwrap();
        write_fixture(directory.path(), CompressionMode::Raw, 1);
        TEST_BEFORE_SEND_DELAY.with(|delay| delay.set(Duration::from_millis(25)));
        let result = run(args(directory.path(), ObjectArg::Logs, 1));
        TEST_BEFORE_SEND_DELAY.with(|delay| delay.set(Duration::ZERO));
        let report = result.unwrap();
        assert!(report.objects[0].producer_wall_ms >= 20);
        assert!(report.objects[0].producer_wall_ms >= report.objects[0].read_wall_ms);
    }

    #[test]
    fn aggregate_worker_memory_admission_and_trim_are_bounded() {
        let mut locators = [Locator::default(); OBJECT_COUNT];
        locators[LeanObject::Logs.index()] = Locator {
            offset: FILE_HEADER_LEN as u64,
            stored_len: 30 << 20,
            decoded_len: 20 << 20,
            zstd: true,
        };
        let rows = [IndexRow {
            block_id: 0,
            locators,
        }];
        assert!(scan_memory_plan(&rows, LeanObject::Logs, 12).is_err());
        let plan = scan_memory_plan(&rows, LeanObject::Logs, 4).unwrap();
        assert_eq!(plan.max_stored_capacity, 30 << 20);
        assert_eq!(plan.max_decoded_capacity, 20 << 20);

        let mut retained = Vec::with_capacity(1_024);
        retained.push(1);
        trim_and_check_capacity(&mut retained, 16, "test buffer").unwrap();
        assert!(retained.capacity() <= 16);
    }

    #[test]
    fn twelve_worker_errors_report_the_lowest_block_even_when_it_finishes_later() {
        let directory = tempfile::tempdir().unwrap();
        write_fixture(directory.path(), CompressionMode::Zstd, 48);
        let index = read_index(directory.path()).unwrap();
        let logs_path = directory.path().join(LeanObject::Logs.file_name());
        let mut logs = fs::read(&logs_path).unwrap();
        for block_id in [25usize, 36] {
            let locator = index.rows[block_id].locators[LeanObject::Logs.index()];
            logs[locator.offset as usize] = 0;
        }
        fs::write(logs_path, logs).unwrap();
        TEST_DELAY_DECODE_BLOCK.store(25, Ordering::Relaxed);
        TEST_DELAY_DECODE_MICROS.store(50_000, Ordering::Relaxed);
        let result = run(args(directory.path(), ObjectArg::Logs, 12));
        TEST_DELAY_DECODE_MICROS.store(0, Ordering::Relaxed);
        TEST_DELAY_DECODE_BLOCK.store(u32::MAX, Ordering::Relaxed);
        let error = result.unwrap_err();
        assert!(format!("{error:#}").contains("scan first failed at block 25"));
    }

    #[test]
    fn strict_parser_rejects_header_index_and_chunk_corruption() {
        let directory = tempfile::tempdir().unwrap();
        write_fixture(directory.path(), CompressionMode::Adaptive, 2);

        let index_path = directory.path().join(INDEX_FILE);
        let original_index = fs::read(&index_path).unwrap();
        let mut bad_reserved = original_index.clone();
        bad_reserved[FILE_HEADER_LEN - 1] = 1;
        fs::write(&index_path, &bad_reserved).unwrap();
        assert!(
            read_index(directory.path())
                .unwrap_err()
                .to_string()
                .contains("reserved")
        );

        fs::write(&index_path, &original_index).unwrap();
        let mut bad_raw_geometry = original_index.clone();
        let logs_locator = FILE_HEADER_LEN + 16 + LeanObject::Logs.index() * 16;
        bad_raw_geometry[logs_locator + 12..logs_locator + 16]
            .copy_from_slice(&2_u32.to_le_bytes());
        fs::write(&index_path, &bad_raw_geometry).unwrap();
        assert!(
            read_index(directory.path())
                .unwrap_err()
                .to_string()
                .contains("lengths differ")
        );

        fs::write(&index_path, &original_index).unwrap();
        let mut oversized = original_index.clone();
        oversized[logs_locator + 12..logs_locator + 16]
            .copy_from_slice(&((MAX_CHUNK_BYTES as u32) + 1).to_le_bytes());
        fs::write(&index_path, &oversized).unwrap();
        assert!(
            read_index(directory.path())
                .unwrap_err()
                .to_string()
                .contains("chunk bound")
        );

        fs::write(&index_path, &original_index).unwrap();
        let mut noncanonical_adaptive = original_index.clone();
        let second_logs_locator =
            FILE_HEADER_LEN + INDEX_ROW_LEN + 16 + LeanObject::Logs.index() * 16;
        let stored_start = second_logs_locator + 8;
        let stored = u32::from_le_bytes(
            noncanonical_adaptive[stored_start..stored_start + 4]
                .try_into()
                .unwrap(),
        ) & STORED_LEN_MASK;
        noncanonical_adaptive[second_logs_locator + 12..second_logs_locator + 16]
            .copy_from_slice(&stored.to_le_bytes());
        fs::write(&index_path, &noncanonical_adaptive).unwrap();
        assert!(
            read_index(directory.path())
                .unwrap_err()
                .to_string()
                .contains("not smaller")
        );

        fs::write(&index_path, &original_index).unwrap();
        let mut outside_epoch = original_index.clone();
        outside_epoch[FILE_HEADER_LEN + 8..FILE_HEADER_LEN + 16]
            .copy_from_slice(&699_u64.to_le_bytes());
        fs::write(&index_path, &outside_epoch).unwrap();
        assert!(
            read_index(directory.path())
                .unwrap_err()
                .to_string()
                .contains("outside epoch")
        );

        fs::write(&index_path, &original_index).unwrap();
        let mut missing_block_rewards = original_index.clone();
        let rewards_locator = FILE_HEADER_LEN + 16 + LeanObject::BlockRewards.index() * 16;
        missing_block_rewards[rewards_locator + 8..rewards_locator + 16].fill(0);
        fs::write(&index_path, &missing_block_rewards).unwrap();
        assert!(
            read_index(directory.path())
                .unwrap_err()
                .to_string()
                .contains("missing its exact block-reward")
        );

        fs::write(&index_path, &original_index).unwrap();
        let logs_path = directory.path().join(LeanObject::Logs.file_name());
        let mut logs = fs::read(&logs_path).unwrap();
        logs.push(0);
        fs::write(&logs_path, logs).unwrap();
        let index = read_index(directory.path()).unwrap();
        assert!(
            open_object(directory.path(), &index, LeanObject::Logs)
                .unwrap_err()
                .to_string()
                .contains("expected")
        );
    }

    #[test]
    fn zstd_level_header_corruption_and_cross_file_mismatch_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        write_fixture_at_level(directory.path(), CompressionMode::Zstd, ZstdLevel::Three, 2);
        let index_path = directory.path().join(INDEX_FILE);
        let index_bytes = fs::read(&index_path).unwrap();

        for code in [1, 2] {
            let mut unknown_level = index_bytes.clone();
            unknown_level[54] = code;
            fs::write(&index_path, unknown_level).unwrap();
            assert!(
                read_index(directory.path())
                    .unwrap_err()
                    .to_string()
                    .contains("zstd-level code")
            );
        }

        fs::write(&index_path, &index_bytes).unwrap();
        read_index(directory.path()).unwrap();
        let logs_path = directory.path().join(LeanObject::Logs.file_name());
        let mut logs = fs::read(&logs_path).unwrap();
        logs[54] = ZstdLevel::Five.header_code();
        fs::write(&logs_path, logs).unwrap();
        assert!(
            run(args(directory.path(), ObjectArg::Outcomes, 1))
                .unwrap_err()
                .to_string()
                .contains("header binding differs")
        );

        let raw_directory = tempfile::tempdir().unwrap();
        write_fixture(raw_directory.path(), CompressionMode::Raw, 1);
        let raw_index_path = raw_directory.path().join(INDEX_FILE);
        let mut raw_index = fs::read(&raw_index_path).unwrap();
        raw_index[54] = ZstdLevel::Three.header_code();
        fs::write(raw_index_path, raw_index).unwrap();
        assert!(
            read_index(raw_directory.path())
                .unwrap_err()
                .to_string()
                .contains("raw lean compression must use zstd level 1")
        );
    }

    #[test]
    fn hybrid_policy_corruptions_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        write_fixture(directory.path(), CompressionMode::Hybrid, 2);
        let index_path = directory.path().join(INDEX_FILE);
        let original = fs::read(&index_path).unwrap();

        let dense_locator = FILE_HEADER_LEN + 16 + LeanObject::Logs.index() * 16;
        let dense_decoded = u32::from_le_bytes(
            original[dense_locator + 12..dense_locator + 16]
                .try_into()
                .unwrap(),
        );
        let mut raw_dense = original.clone();
        raw_dense[dense_locator + 8..dense_locator + 12]
            .copy_from_slice(&dense_decoded.to_le_bytes());
        fs::write(&index_path, raw_dense).unwrap();
        assert!(
            read_index(directory.path())
                .unwrap_err()
                .to_string()
                .contains("logs policy requires zstd")
        );

        let rewards_locator = FILE_HEADER_LEN + 16 + LeanObject::BlockRewards.index() * 16;
        let mut zstd_rewards = original.clone();
        let rewards_stored = u32::from_le_bytes(
            zstd_rewards[rewards_locator + 8..rewards_locator + 12]
                .try_into()
                .unwrap(),
        );
        zstd_rewards[rewards_locator + 8..rewards_locator + 12]
            .copy_from_slice(&(rewards_stored | ZSTD_CODEC_BIT).to_le_bytes());
        fs::write(&index_path, zstd_rewards).unwrap();
        assert!(
            read_index(directory.path())
                .unwrap_err()
                .to_string()
                .contains("block-rewards policy requires raw")
        );

        let sparse_locator =
            FILE_HEADER_LEN + INDEX_ROW_LEN + 16 + LeanObject::TransactionRewards.index() * 16;
        let mut non_smaller_sparse = original;
        let sparse_stored = u32::from_le_bytes(
            non_smaller_sparse[sparse_locator + 8..sparse_locator + 12]
                .try_into()
                .unwrap(),
        ) & STORED_LEN_MASK;
        non_smaller_sparse[sparse_locator + 12..sparse_locator + 16]
            .copy_from_slice(&sparse_stored.to_le_bytes());
        fs::write(&index_path, non_smaller_sparse).unwrap();
        assert!(
            read_index(directory.path())
                .unwrap_err()
                .to_string()
                .contains("adaptive transaction-rewards")
        );
    }

    #[test]
    fn exact_zstd_frame_rejects_trailing_bytes() {
        let directory = tempfile::tempdir().unwrap();
        write_fixture(directory.path(), CompressionMode::Zstd, 1);
        let index_path = directory.path().join(INDEX_FILE);
        let mut index_bytes = fs::read(&index_path).unwrap();
        let locator = FILE_HEADER_LEN + 16 + LeanObject::Logs.index() * 16;
        let stored_start = locator + 8;
        let stored = u32::from_le_bytes(
            index_bytes[stored_start..stored_start + 4]
                .try_into()
                .unwrap(),
        ) & STORED_LEN_MASK;
        index_bytes[stored_start..stored_start + 4]
            .copy_from_slice(&((stored + 1) | ZSTD_CODEC_BIT).to_le_bytes());
        fs::write(&index_path, index_bytes).unwrap();
        let logs_path = directory.path().join(LeanObject::Logs.file_name());
        let mut logs = fs::read(&logs_path).unwrap();
        logs.push(0);
        fs::write(logs_path, logs).unwrap();
        let error = run(args(directory.path(), ObjectArg::Logs, 1)).unwrap_err();
        assert!(format!("{error:#}").contains("trailing bytes"));
    }

    #[test]
    fn cli_bounds_are_exact() {
        assert_eq!(parse_workers("1").unwrap(), 1);
        assert_eq!(parse_workers("4").unwrap(), 4);
        assert_eq!(parse_workers("12").unwrap(), 12);
        assert!(parse_workers("2").is_err());
        assert_eq!(parse_iterations("1").unwrap(), 1);
        assert_eq!(
            parse_iterations(&MAX_ITERATIONS.to_string()).unwrap(),
            MAX_ITERATIONS
        );
        assert!(parse_iterations("0").is_err());
    }
}
