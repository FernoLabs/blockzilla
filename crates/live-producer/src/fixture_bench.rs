use std::{
    collections::HashMap as StdHashMap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use blockzilla_format::{
    LivePubkeyCountRecord, SplitCompactIndexHeader, SplitCompactIndexRecord,
    WincodeArchiveV2NoRegistryBlock, WincodeArchiveV2Payload, WincodeArchiveV2PohRecord,
    WincodeLeb128FramedWriter, encode_with_scratch, read_u32_varint, wincode_leb128_config,
};
use gxhash::{GxBuildHasher, HashMap as GxHashMap};
use prost::Message;
use serde::{Deserialize, Serialize};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

use crate::{
    grpc::{convert_grpc_block, visit_pubkeys_from_block, write_signatures},
    layout::ProducerLayout,
};

#[derive(Debug, Clone)]
pub struct GrpcFixtureBenchConfig {
    pub archive_dir: PathBuf,
    pub iterations: usize,
    pub max_blocks: Option<usize>,
    pub initial_pubkey_capacity: usize,
    pub hash_backends: Vec<GrpcFixtureHashBackend>,
    pub write_mode: GrpcFixtureWriteMode,
    pub block_write_strategies: Vec<GrpcFixtureBlockWriteStrategy>,
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrpcFixtureHashBackend {
    StdRandom,
    StdAhash,
    StdFxhash,
    HashbrownAhash,
    GxhashU64,
    GxhashU32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrpcFixtureWriteMode {
    None,
    Archive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrpcFixtureBlockWriteStrategy {
    Current,
    ScratchOnce,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcFixtureBenchReport {
    pub archive_dir: PathBuf,
    pub raw_blocks_path: PathBuf,
    pub raw_frames: usize,
    pub raw_payload_bytes: u64,
    pub raw_file_bytes: u64,
    pub preload_ms: u128,
    pub iterations: usize,
    pub initial_pubkey_capacity: usize,
    pub write_mode: GrpcFixtureWriteMode,
    pub block_write_strategies: Vec<GrpcFixtureBlockWriteStrategy>,
    pub runs: Vec<GrpcFixtureBenchRunReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcFixtureBenchRunReport {
    pub backend: GrpcFixtureHashBackend,
    pub iteration: usize,
    pub block_write_strategy: Option<GrpcFixtureBlockWriteStrategy>,
    pub output_dir: Option<PathBuf>,
    pub blocks: usize,
    pub transactions: usize,
    pub poh_entries: usize,
    pub signatures: usize,
    pub raw_payload_bytes: u64,
    pub normalized_block_bytes: u64,
    pub pubkey_touches: u64,
    pub unique_pubkeys: usize,
    pub registry_capacity: usize,
    pub registry_count_value_bytes: usize,
    pub registry_estimated_heap_bytes: u64,
    pub registry_count_sum: u128,
    pub registry_counts_file_bytes: Option<u64>,
    pub total_ms: u128,
    pub decode_ms: u128,
    pub convert_ms: u128,
    pub registry_count_ms: u128,
    pub block_encode_ms: u128,
    pub archive_write_ms: u128,
    pub signature_write_ms: u128,
    pub registry_sort_ms: u128,
    pub registry_write_ms: u128,
    pub blocks_per_sec: f64,
    pub payload_mb_per_sec: f64,
}

impl GrpcFixtureHashBackend {
    pub fn all() -> Vec<Self> {
        vec![
            Self::StdRandom,
            Self::StdAhash,
            Self::StdFxhash,
            Self::HashbrownAhash,
            Self::GxhashU64,
            Self::GxhashU32,
        ]
    }

    fn label(self) -> &'static str {
        match self {
            Self::StdRandom => "std-random",
            Self::StdAhash => "std-ahash",
            Self::StdFxhash => "std-fxhash",
            Self::HashbrownAhash => "hashbrown-ahash",
            Self::GxhashU64 => "gxhash-u64",
            Self::GxhashU32 => "gxhash-u32",
        }
    }
}

impl GrpcFixtureBlockWriteStrategy {
    pub fn all() -> Vec<Self> {
        vec![Self::Current, Self::ScratchOnce]
    }

    fn label(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::ScratchOnce => "scratch-once",
        }
    }
}

pub fn bench_grpc_fixture(config: GrpcFixtureBenchConfig) -> Result<GrpcFixtureBenchReport> {
    let layout = ProducerLayout::new(&config.archive_dir);
    let raw_blocks_path = layout.blocks_dir.join("grpc-raw-blocks.bin");
    let preload_start = Instant::now();
    let frames = read_raw_frames(&raw_blocks_path, config.max_blocks)?;
    let preload_ms = preload_start.elapsed().as_millis();
    let raw_payload_bytes = frames.iter().map(|frame| frame.len() as u64).sum::<u64>();
    let raw_file_bytes = fs::metadata(&raw_blocks_path)
        .with_context(|| format!("stat {}", raw_blocks_path.display()))?
        .len();
    let iterations = config.iterations.max(1);
    let mut strategies = match config.write_mode {
        GrpcFixtureWriteMode::None => vec![GrpcFixtureBlockWriteStrategy::Current],
        GrpcFixtureWriteMode::Archive => config.block_write_strategies.clone(),
    };
    if strategies.is_empty() {
        strategies.push(GrpcFixtureBlockWriteStrategy::Current);
    }

    let mut runs = Vec::new();
    for backend in &config.hash_backends {
        for strategy in &strategies {
            for iteration in 0..iterations {
                let run = match backend {
                    GrpcFixtureHashBackend::StdRandom => {
                        bench_one::<StdRandomCounter>(&frames, &config, *strategy, iteration)?
                    }
                    GrpcFixtureHashBackend::StdAhash => {
                        bench_one::<StdAhashCounter>(&frames, &config, *strategy, iteration)?
                    }
                    GrpcFixtureHashBackend::StdFxhash => {
                        bench_one::<StdFxhashCounter>(&frames, &config, *strategy, iteration)?
                    }
                    GrpcFixtureHashBackend::HashbrownAhash => {
                        bench_one::<HashbrownAhashCounter>(&frames, &config, *strategy, iteration)?
                    }
                    GrpcFixtureHashBackend::GxhashU64 => {
                        bench_one::<GxhashU64Counter>(&frames, &config, *strategy, iteration)?
                    }
                    GrpcFixtureHashBackend::GxhashU32 => {
                        bench_one::<GxhashU32Counter>(&frames, &config, *strategy, iteration)?
                    }
                };
                runs.push(run);
            }
        }
    }

    Ok(GrpcFixtureBenchReport {
        archive_dir: config.archive_dir,
        raw_blocks_path,
        raw_frames: frames.len(),
        raw_payload_bytes,
        raw_file_bytes,
        preload_ms,
        iterations,
        initial_pubkey_capacity: config.initial_pubkey_capacity,
        write_mode: config.write_mode,
        block_write_strategies: strategies,
        runs,
    })
}

fn bench_one<C: FixturePubkeyCounter>(
    frames: &[Vec<u8>],
    config: &GrpcFixtureBenchConfig,
    block_write_strategy: GrpcFixtureBlockWriteStrategy,
    iteration: usize,
) -> Result<GrpcFixtureBenchRunReport> {
    let total_started_at = Instant::now();
    let mut counter = C::with_capacity(config.initial_pubkey_capacity);
    let mut metrics = BenchTiming::default();
    let mut archive = match config.write_mode {
        GrpcFixtureWriteMode::None => None,
        GrpcFixtureWriteMode::Archive => Some(BenchArchiveWriters::create(
            &config.output_dir,
            C::BACKEND,
            block_write_strategy,
            iteration,
        )?),
    };

    let mut blocks = 0usize;
    let mut transactions = 0usize;
    let mut poh_entries = 0usize;
    let mut signatures = 0usize;
    let mut raw_payload_bytes = 0u64;
    let mut normalized_block_bytes = 0u64;
    let mut block_scratch = Vec::with_capacity(2 * 1024 * 1024);

    for frame in frames {
        raw_payload_bytes += frame.len() as u64;
        let decode_started_at = Instant::now();
        let block =
            SubscribeUpdateBlock::decode(frame.as_slice()).context("decode raw gRPC block")?;
        metrics.decode += decode_started_at.elapsed();

        let block_id = u32::try_from(blocks).context("fixture block count exceeds u32::MAX")?;
        let convert_started_at = Instant::now();
        let converted = convert_grpc_block(&block, block_id)?;
        metrics.convert += convert_started_at.elapsed();

        let count_started_at = Instant::now();
        counter.count_block(&converted.block);
        metrics.registry_count += count_started_at.elapsed();

        signatures += count_signatures(&converted.block);
        transactions += converted.transaction_count;
        poh_entries += converted.poh_entries.len();

        if let Some(archive) = archive.as_mut() {
            let encoded_len = archive.write_block(
                frame,
                &block,
                &converted,
                block_write_strategy,
                &mut block_scratch,
                &mut metrics,
            )?;
            normalized_block_bytes += encoded_len as u64;
        } else {
            let encode_started_at = Instant::now();
            encode_with_scratch(&converted.block, &mut block_scratch)
                .context("measure normalized block wincode size")?;
            let encoded_len = u64::try_from(block_scratch.len())
                .context("normalized block size exceeds u64::MAX")?;
            metrics.block_encode += encode_started_at.elapsed();
            normalized_block_bytes += encoded_len;
        }

        blocks += 1;
    }

    if let Some(archive) = archive.as_mut() {
        archive.flush()?;
    }

    let sort_started_at = Instant::now();
    let registry_capacity = counter.capacity();
    let registry_estimated_heap_bytes = counter.estimated_heap_bytes();
    let registry_count_sum = counter.count_sum();
    let records = counter.into_sorted_records();
    metrics.registry_sort += sort_started_at.elapsed();

    let (output_dir, registry_counts_file_bytes) = if let Some(archive) = archive {
        let write_started_at = Instant::now();
        write_pubkey_count_records(&archive.pubkey_counts_path, &records)?;
        metrics.registry_write += write_started_at.elapsed();
        (
            Some(archive.output_dir),
            Some(file_len(&archive.pubkey_counts_path)?),
        )
    } else {
        (None, None)
    };

    let total = total_started_at.elapsed();
    let total_secs = total.as_secs_f64().max(0.001);
    Ok(GrpcFixtureBenchRunReport {
        backend: C::BACKEND,
        iteration,
        block_write_strategy: match config.write_mode {
            GrpcFixtureWriteMode::None => None,
            GrpcFixtureWriteMode::Archive => Some(block_write_strategy),
        },
        output_dir,
        blocks,
        transactions,
        poh_entries,
        signatures,
        raw_payload_bytes,
        normalized_block_bytes,
        pubkey_touches: C::touches_from_sum(registry_count_sum),
        unique_pubkeys: records.len(),
        registry_capacity,
        registry_count_value_bytes: C::COUNT_VALUE_BYTES,
        registry_estimated_heap_bytes,
        registry_count_sum,
        registry_counts_file_bytes,
        total_ms: total.as_millis(),
        decode_ms: metrics.decode.as_millis(),
        convert_ms: metrics.convert.as_millis(),
        registry_count_ms: metrics.registry_count.as_millis(),
        block_encode_ms: metrics.block_encode.as_millis(),
        archive_write_ms: metrics.archive_write.as_millis(),
        signature_write_ms: metrics.signature_write.as_millis(),
        registry_sort_ms: metrics.registry_sort.as_millis(),
        registry_write_ms: metrics.registry_write.as_millis(),
        blocks_per_sec: blocks as f64 / total_secs,
        payload_mb_per_sec: (raw_payload_bytes as f64 / 1_048_576.0) / total_secs,
    })
}

#[derive(Default)]
struct BenchTiming {
    decode: Duration,
    convert: Duration,
    registry_count: Duration,
    block_encode: Duration,
    archive_write: Duration,
    signature_write: Duration,
    registry_sort: Duration,
    registry_write: Duration,
}

struct BenchArchiveWriters {
    output_dir: PathBuf,
    block_writer: WincodeLeb128FramedWriter<BufWriter<File>>,
    raw_blocks_writer: WincodeLeb128FramedWriter<BufWriter<File>>,
    poh_writer: WincodeLeb128FramedWriter<BufWriter<File>>,
    block_index_writer: WincodeLeb128FramedWriter<BufWriter<File>>,
    blockhash_writer: BufWriter<File>,
    signatures_writer: BufWriter<File>,
    signature_index_writer: WincodeLeb128FramedWriter<BufWriter<File>>,
    pubkey_counts_path: PathBuf,
    block_offset: u64,
    signature_ordinal: u64,
}

impl BenchArchiveWriters {
    fn create(
        output_root: &Path,
        backend: GrpcFixtureHashBackend,
        strategy: GrpcFixtureBlockWriteStrategy,
        iteration: usize,
    ) -> Result<Self> {
        let output_dir = output_root.join(format!(
            "{}-{}-iter-{}",
            backend.label(),
            strategy.label(),
            iteration
        ));
        if output_dir.exists() {
            fs::remove_dir_all(&output_dir)
                .with_context(|| format!("remove {}", output_dir.display()))?;
        }
        let layout = ProducerLayout::create(&output_dir)?;
        let block_path = layout.blocks_dir.join("live-no-registry-blocks.bin");
        let raw_blocks_path = layout.blocks_dir.join("grpc-raw-blocks.bin");
        let poh_path = layout.poh_dir.join("poh.wincode");
        let block_index_path = layout.index_dir.join("block-index.bin");
        let blockhash_registry_path = layout.index_dir.join("blockhash_registry.bin");
        let signatures_path = layout.index_dir.join("signatures.bin");
        let signature_index_path = layout.index_dir.join("signature-index.bin");
        let pubkey_counts_path = layout.index_dir.join("pubkey-counts.bin");

        let mut block_index_writer =
            WincodeLeb128FramedWriter::new(BufWriter::new(File::create(&block_index_path)?));
        block_index_writer.write(&SplitCompactIndexHeader { version: 1 })?;

        Ok(Self {
            output_dir,
            block_writer: WincodeLeb128FramedWriter::new(BufWriter::new(File::create(block_path)?)),
            raw_blocks_writer: WincodeLeb128FramedWriter::new(BufWriter::new(File::create(
                raw_blocks_path,
            )?)),
            poh_writer: WincodeLeb128FramedWriter::new(BufWriter::new(File::create(poh_path)?)),
            block_index_writer,
            blockhash_writer: BufWriter::new(File::create(blockhash_registry_path)?),
            signatures_writer: BufWriter::new(File::create(signatures_path)?),
            signature_index_writer: WincodeLeb128FramedWriter::new(BufWriter::new(File::create(
                signature_index_path,
            )?)),
            pubkey_counts_path,
            block_offset: 0,
            signature_ordinal: 0,
        })
    }

    fn write_block(
        &mut self,
        raw_frame: &[u8],
        grpc_block: &SubscribeUpdateBlock,
        converted: &crate::grpc::ConvertedGrpcBlock,
        strategy: GrpcFixtureBlockWriteStrategy,
        block_scratch: &mut Vec<u8>,
        metrics: &mut BenchTiming,
    ) -> Result<u32> {
        let block_id = converted.block.header.compact.blockhash;
        let encode_started_at = Instant::now();
        let block_len = match strategy {
            GrpcFixtureBlockWriteStrategy::Current => usize::try_from(
                wincode::config::serialized_size(&converted.block, wincode_leb128_config())
                    .context("measure normalized block wincode size")?,
            )
            .context("normalized block size exceeds usize::MAX")?,
            GrpcFixtureBlockWriteStrategy::ScratchOnce => {
                encode_with_scratch(&converted.block, block_scratch)
                    .context("wincode encode normalized block")?;
                block_scratch.len()
            }
        };
        let block_len = u32::try_from(block_len).context("normalized block exceeds u32::MAX")?;
        metrics.block_encode += encode_started_at.elapsed();

        let tx_count = u32::try_from(converted.transaction_count)
            .context("transaction count exceeds u32::MAX")?;

        let write_started_at = Instant::now();
        self.raw_blocks_writer.write_bytes(raw_frame)?;
        self.poh_writer.write(&WincodeArchiveV2PohRecord {
            block_id,
            slot: grpc_block.slot,
            entries: converted.poh_entries.clone(),
        })?;
        match strategy {
            GrpcFixtureBlockWriteStrategy::Current => {
                self.block_writer.write(&converted.block)?;
            }
            GrpcFixtureBlockWriteStrategy::ScratchOnce => {
                self.block_writer.write_bytes(block_scratch.as_slice())?;
            }
        }
        self.block_index_writer.write(&SplitCompactIndexRecord {
            slot: grpc_block.slot,
            block_id,
            block_offset: self.block_offset,
            block_len,
            runtime_offset: 0,
            runtime_len: 0,
            tx_count,
        })?;
        self.blockhash_writer.write_all(&converted.blockhash)?;
        self.block_offset = self
            .block_offset
            .checked_add(frame_len_u64(block_len))
            .context("block offset overflow")?;
        metrics.archive_write += write_started_at.elapsed();

        let signature_started_at = Instant::now();
        write_signatures(
            &converted.block,
            block_id,
            grpc_block.slot,
            &mut self.signature_ordinal,
            &mut self.signatures_writer,
            &mut self.signature_index_writer,
        )?;
        metrics.signature_write += signature_started_at.elapsed();

        Ok(block_len)
    }

    fn flush(&mut self) -> Result<()> {
        self.block_writer.flush()?;
        self.raw_blocks_writer.flush()?;
        self.poh_writer.flush()?;
        self.block_index_writer.flush()?;
        self.blockhash_writer.flush()?;
        self.signatures_writer.flush()?;
        self.signature_index_writer.flush()?;
        Ok(())
    }
}

trait FixturePubkeyCounter: Sized {
    const BACKEND: GrpcFixtureHashBackend;
    const COUNT_VALUE_BYTES: usize;

    fn with_capacity(capacity: usize) -> Self;
    fn increment(&mut self, pubkey: [u8; 32]);
    fn capacity(&self) -> usize;
    fn count_sum(&self) -> u128;
    fn into_sorted_records(self) -> Vec<LivePubkeyCountRecord>;

    fn count_block(&mut self, block: &WincodeArchiveV2NoRegistryBlock) {
        visit_pubkeys_from_block(block, |pubkey| self.increment(pubkey));
    }

    fn estimated_heap_bytes(&self) -> u64 {
        let entry_bytes = size_of::<[u8; 32]>() + Self::COUNT_VALUE_BYTES;
        let control_bytes = 1usize;
        self.capacity().saturating_mul(entry_bytes + control_bytes) as u64
    }

    fn touches_from_sum(sum: u128) -> u64 {
        u64::try_from(sum).unwrap_or(u64::MAX)
    }
}

struct StdRandomCounter {
    map: StdHashMap<[u8; 32], u64>,
}

impl FixturePubkeyCounter for StdRandomCounter {
    const BACKEND: GrpcFixtureHashBackend = GrpcFixtureHashBackend::StdRandom;
    const COUNT_VALUE_BYTES: usize = size_of::<u64>();

    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: StdHashMap::with_capacity(capacity),
        }
    }

    fn increment(&mut self, pubkey: [u8; 32]) {
        *self.map.entry(pubkey).or_insert(0) += 1;
    }

    fn capacity(&self) -> usize {
        self.map.capacity()
    }

    fn count_sum(&self) -> u128 {
        self.map.values().map(|count| *count as u128).sum()
    }

    fn into_sorted_records(self) -> Vec<LivePubkeyCountRecord> {
        sorted_records_u64(self.map)
    }
}

struct StdAhashCounter {
    map: StdHashMap<[u8; 32], u64, ahash::RandomState>,
}

impl FixturePubkeyCounter for StdAhashCounter {
    const BACKEND: GrpcFixtureHashBackend = GrpcFixtureHashBackend::StdAhash;
    const COUNT_VALUE_BYTES: usize = size_of::<u64>();

    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: StdHashMap::with_capacity_and_hasher(capacity, ahash::RandomState::new()),
        }
    }

    fn increment(&mut self, pubkey: [u8; 32]) {
        *self.map.entry(pubkey).or_insert(0) += 1;
    }

    fn capacity(&self) -> usize {
        self.map.capacity()
    }

    fn count_sum(&self) -> u128 {
        self.map.values().map(|count| *count as u128).sum()
    }

    fn into_sorted_records(self) -> Vec<LivePubkeyCountRecord> {
        sorted_records_u64(self.map)
    }
}

struct StdFxhashCounter {
    map: StdHashMap<[u8; 32], u64, rustc_hash::FxBuildHasher>,
}

impl FixturePubkeyCounter for StdFxhashCounter {
    const BACKEND: GrpcFixtureHashBackend = GrpcFixtureHashBackend::StdFxhash;
    const COUNT_VALUE_BYTES: usize = size_of::<u64>();

    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: StdHashMap::with_capacity_and_hasher(capacity, rustc_hash::FxBuildHasher),
        }
    }

    fn increment(&mut self, pubkey: [u8; 32]) {
        *self.map.entry(pubkey).or_insert(0) += 1;
    }

    fn capacity(&self) -> usize {
        self.map.capacity()
    }

    fn count_sum(&self) -> u128 {
        self.map.values().map(|count| *count as u128).sum()
    }

    fn into_sorted_records(self) -> Vec<LivePubkeyCountRecord> {
        sorted_records_u64(self.map)
    }
}

struct HashbrownAhashCounter {
    map: hashbrown::HashMap<[u8; 32], u64, ahash::RandomState>,
}

impl FixturePubkeyCounter for HashbrownAhashCounter {
    const BACKEND: GrpcFixtureHashBackend = GrpcFixtureHashBackend::HashbrownAhash;
    const COUNT_VALUE_BYTES: usize = size_of::<u64>();

    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: hashbrown::HashMap::with_capacity_and_hasher(capacity, ahash::RandomState::new()),
        }
    }

    fn increment(&mut self, pubkey: [u8; 32]) {
        *self.map.entry(pubkey).or_insert(0) += 1;
    }

    fn capacity(&self) -> usize {
        self.map.capacity()
    }

    fn count_sum(&self) -> u128 {
        self.map.values().map(|count| *count as u128).sum()
    }

    fn into_sorted_records(self) -> Vec<LivePubkeyCountRecord> {
        sorted_records_u64(self.map)
    }
}

struct GxhashU64Counter {
    map: GxHashMap<[u8; 32], u64>,
}

impl FixturePubkeyCounter for GxhashU64Counter {
    const BACKEND: GrpcFixtureHashBackend = GrpcFixtureHashBackend::GxhashU64;
    const COUNT_VALUE_BYTES: usize = size_of::<u64>();

    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: GxHashMap::with_capacity_and_hasher(capacity, GxBuildHasher::default()),
        }
    }

    fn increment(&mut self, pubkey: [u8; 32]) {
        *self.map.entry(pubkey).or_insert(0) += 1;
    }

    fn capacity(&self) -> usize {
        self.map.capacity()
    }

    fn count_sum(&self) -> u128 {
        self.map.values().map(|count| *count as u128).sum()
    }

    fn into_sorted_records(self) -> Vec<LivePubkeyCountRecord> {
        sorted_records_u64(self.map)
    }
}

struct GxhashU32Counter {
    map: GxHashMap<[u8; 32], u32>,
}

impl FixturePubkeyCounter for GxhashU32Counter {
    const BACKEND: GrpcFixtureHashBackend = GrpcFixtureHashBackend::GxhashU32;
    const COUNT_VALUE_BYTES: usize = size_of::<u32>();

    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: GxHashMap::with_capacity_and_hasher(capacity, GxBuildHasher::default()),
        }
    }

    fn increment(&mut self, pubkey: [u8; 32]) {
        let entry = self.map.entry(pubkey).or_insert(0);
        *entry = entry.saturating_add(1);
    }

    fn capacity(&self) -> usize {
        self.map.capacity()
    }

    fn count_sum(&self) -> u128 {
        self.map.values().map(|count| *count as u128).sum()
    }

    fn into_sorted_records(self) -> Vec<LivePubkeyCountRecord> {
        let mut records = self
            .map
            .into_iter()
            .map(|(pubkey, count)| LivePubkeyCountRecord {
                pubkey,
                count: u64::from(count),
            })
            .collect::<Vec<_>>();
        sort_pubkey_count_records(&mut records);
        records
    }
}

fn sorted_records_u64<I>(iterable: I) -> Vec<LivePubkeyCountRecord>
where
    I: IntoIterator<Item = ([u8; 32], u64)>,
{
    let mut records = iterable
        .into_iter()
        .map(|(pubkey, count)| LivePubkeyCountRecord { pubkey, count })
        .collect::<Vec<_>>();
    sort_pubkey_count_records(&mut records);
    records
}

fn sort_pubkey_count_records(records: &mut [LivePubkeyCountRecord]) {
    records.sort_unstable_by(|a, b| b.count.cmp(&a.count).then_with(|| a.pubkey.cmp(&b.pubkey)));
}

fn write_pubkey_count_records(path: &Path, records: &[LivePubkeyCountRecord]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = WincodeLeb128FramedWriter::new(BufWriter::new(file));
    for record in records {
        writer.write(record)?;
    }
    writer.flush()
}

fn count_signatures(block: &WincodeArchiveV2NoRegistryBlock) -> usize {
    block
        .txs
        .iter()
        .map(|tx| {
            let WincodeArchiveV2Payload::Decoded { value, .. } = &tx.tx else {
                return 0;
            };
            value.signatures.len()
        })
        .sum()
}

fn read_raw_frames(path: &Path, max_frames: Option<usize>) -> Result<Vec<Vec<u8>>> {
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    let mut frames = Vec::new();
    loop {
        if max_frames.is_some_and(|max_frames| frames.len() >= max_frames) {
            break;
        }
        let Some(len) = read_u32_varint(&mut reader)
            .with_context(|| format!("read frame len from {}", path.display()))?
        else {
            break;
        };
        let len = len as usize;
        let mut frame = vec![0u8; len];
        reader
            .read_exact(&mut frame)
            .with_context(|| format!("read frame payload from {}", path.display()))?;
        frames.push(frame);
    }
    Ok(frames)
}

fn file_len(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .len())
}

fn frame_len_u64(payload_len: u32) -> u64 {
    u64::from(payload_len) + u64::from(u32_varint_len(payload_len))
}

fn u32_varint_len(mut value: u32) -> u8 {
    let mut len = 1u8;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}
