use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap as StdHashMap},
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
    grpc::{
        GrpcConvertTimingReport, GrpcConvertTimings,
        convert_grpc_block_frame_timed_with_pubkey_cache,
        convert_grpc_block_frame_with_pubkey_cache, convert_grpc_block_timed_with_pubkey_cache,
        visit_pubkeys_from_block, write_signatures,
    },
    layout::ProducerLayout,
};

#[derive(Debug, Clone)]
pub struct GrpcFixtureBenchConfig {
    pub archive_dir: PathBuf,
    pub iterations: usize,
    pub max_blocks: Option<usize>,
    pub decode_mode: GrpcFixtureDecodeMode,
    pub initial_pubkey_capacity: usize,
    pub hash_backends: Vec<GrpcFixtureHashBackend>,
    pub heavy_hitter_capacities: Vec<usize>,
    pub pubkey_string_cache: GrpcFixturePubkeyStringCache,
    pub log_parse_stats: bool,
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
pub enum GrpcFixtureDecodeMode {
    Prost,
    Borrowed,
    BorrowedFast,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrpcFixturePubkeyStringCache {
    Enabled,
    Disabled,
}

impl GrpcFixturePubkeyStringCache {
    fn enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
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
    pub decode_mode: GrpcFixtureDecodeMode,
    pub initial_pubkey_capacity: usize,
    pub heavy_hitter_capacities: Vec<usize>,
    pub pubkey_string_cache: GrpcFixturePubkeyStringCache,
    pub log_parse_stats: bool,
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
    pub exact_order_id_varint_bytes: u128,
    pub lex_order_id_varint_bytes: u128,
    pub heavy_hitter_orderings: Vec<GrpcFixtureHeavyHitterOrderingReport>,
    pub total_ms: u128,
    pub decode_ms: u128,
    pub convert_ms: u128,
    pub convert_breakdown: GrpcConvertTimingReport,
    pub registry_count_ms: u128,
    pub block_encode_ms: u128,
    pub archive_write_ms: u128,
    pub signature_write_ms: u128,
    pub registry_sort_ms: u128,
    pub registry_write_ms: u128,
    pub blocks_per_sec: f64,
    pub payload_mb_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcFixtureHeavyHitterOrderingReport {
    pub capacity: usize,
    pub candidates: usize,
    pub candidate_exact_touches: u128,
    pub candidate_touch_ratio_pct: f64,
    pub estimated_heap_bytes: u64,
    pub heap_rebuilds: u64,
    pub approximate_order_id_varint_bytes: u128,
    pub extra_vs_exact_bytes: u128,
    pub extra_vs_exact_pct: f64,
    pub saved_vs_lex_bytes: u128,
    pub saved_vs_lex_pct: f64,
    pub one_byte_id_overlap: usize,
    pub two_byte_id_overlap: usize,
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
        decode_mode: config.decode_mode,
        initial_pubkey_capacity: config.initial_pubkey_capacity,
        heavy_hitter_capacities: config.heavy_hitter_capacities.clone(),
        log_parse_stats: config.log_parse_stats,
        write_mode: config.write_mode,
        block_write_strategies: strategies,
        pubkey_string_cache: config.pubkey_string_cache,
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
    let mut heavy_hitters = config
        .heavy_hitter_capacities
        .iter()
        .copied()
        .filter(|capacity| *capacity > 0)
        .map(SpaceSavingPubkeyTracker::new)
        .collect::<Vec<_>>();
    let mut metrics = BenchTiming::default();
    metrics
        .convert_detail
        .set_collect_log_parse_stats(config.log_parse_stats);
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

        let block_id = u32::try_from(blocks).context("fixture block count exceeds u32::MAX")?;
        let converted = match config.decode_mode {
            GrpcFixtureDecodeMode::Prost => {
                let decode_started_at = Instant::now();
                let block = SubscribeUpdateBlock::decode(frame.as_slice())
                    .context("decode raw gRPC block")?;
                metrics.decode += decode_started_at.elapsed();

                let convert_started_at = Instant::now();
                let converted = convert_grpc_block_timed_with_pubkey_cache(
                    &block,
                    block_id,
                    &mut metrics.convert_detail,
                    config.pubkey_string_cache.enabled(),
                )?;
                metrics.convert += convert_started_at.elapsed();
                converted
            }
            GrpcFixtureDecodeMode::Borrowed => {
                let convert_started_at = Instant::now();
                let converted = convert_grpc_block_frame_timed_with_pubkey_cache(
                    frame,
                    block_id,
                    &mut metrics.convert_detail,
                    config.pubkey_string_cache.enabled(),
                )?;
                metrics.convert += convert_started_at.elapsed();
                converted
            }
            GrpcFixtureDecodeMode::BorrowedFast => {
                let convert_started_at = Instant::now();
                let converted = convert_grpc_block_frame_with_pubkey_cache(
                    frame,
                    block_id,
                    config.pubkey_string_cache.enabled(),
                )?;
                metrics.convert += convert_started_at.elapsed();
                converted
            }
        };

        let count_started_at = Instant::now();
        visit_pubkeys_from_block(&converted.block, |pubkey| {
            counter.increment(pubkey);
            for tracker in &mut heavy_hitters {
                tracker.increment(pubkey);
            }
            Ok(())
        })?;
        metrics.registry_count += count_started_at.elapsed();

        signatures += count_signatures(&converted.block);
        transactions += converted.transaction_count;
        poh_entries += converted.poh_entries.len();

        if let Some(archive) = archive.as_mut() {
            let encoded_len = archive.write_block(
                frame,
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
    let exact_order_id_varint_bytes = registry_id_varint_bytes(records.iter())?;
    let lex_order_id_varint_bytes = lex_order_id_varint_bytes(&records)?;
    let heavy_hitter_orderings = build_heavy_hitter_ordering_reports(
        &records,
        exact_order_id_varint_bytes,
        lex_order_id_varint_bytes,
        heavy_hitters,
    )?;
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
        exact_order_id_varint_bytes,
        lex_order_id_varint_bytes,
        heavy_hitter_orderings,
        total_ms: total.as_millis(),
        decode_ms: metrics.decode.as_millis(),
        convert_ms: metrics.convert.as_millis(),
        convert_breakdown: metrics.convert_detail.report(),
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
    convert_detail: GrpcConvertTimings,
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
            slot: converted.block.header.compact.slot,
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
            slot: converted.block.header.compact.slot,
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
            converted.block.header.compact.slot,
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

    fn estimated_heap_bytes(&self) -> u64 {
        let entry_bytes = size_of::<[u8; 32]>() + Self::COUNT_VALUE_BYTES;
        let control_bytes = 1usize;
        self.capacity().saturating_mul(entry_bytes + control_bytes) as u64
    }

    fn touches_from_sum(sum: u128) -> u64 {
        u64::try_from(sum).unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone)]
struct SpaceSavingCandidate {
    pubkey: [u8; 32],
    estimate: u64,
    error: u64,
}

#[derive(Debug, Clone)]
struct SpaceSavingEntry {
    pubkey: [u8; 32],
    estimate: u64,
    error: u64,
    generation: u64,
}

struct SpaceSavingPubkeyTracker {
    capacity: usize,
    entries: Vec<SpaceSavingEntry>,
    index: GxHashMap<[u8; 32], usize>,
    heap: BinaryHeap<Reverse<(u64, u64, usize)>>,
    heap_rebuilds: u64,
}

impl SpaceSavingPubkeyTracker {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Vec::with_capacity(capacity),
            index: GxHashMap::with_capacity_and_hasher(capacity, GxBuildHasher::default()),
            heap: BinaryHeap::with_capacity(capacity),
            heap_rebuilds: 0,
        }
    }

    fn increment(&mut self, pubkey: [u8; 32]) {
        if self.capacity == 0 {
            return;
        }
        if let Some(slot) = self.index.get(&pubkey).copied() {
            let entry = &mut self.entries[slot];
            entry.estimate = entry.estimate.saturating_add(1);
            entry.generation = entry.generation.saturating_add(1);
            self.push_heap_entry(slot);
            self.rebuild_heap_if_stale_heavy();
            return;
        }
        if self.entries.len() < self.capacity {
            let slot = self.entries.len();
            self.entries.push(SpaceSavingEntry {
                pubkey,
                estimate: 1,
                error: 0,
                generation: 0,
            });
            self.index.insert(pubkey, slot);
            self.push_heap_entry(slot);
            return;
        }

        let slot = self.min_slot();
        let previous = &mut self.entries[slot];
        let min_estimate = previous.estimate;
        self.index.remove(&previous.pubkey);
        previous.pubkey = pubkey;
        previous.estimate = min_estimate.saturating_add(1);
        previous.error = min_estimate;
        previous.generation = previous.generation.saturating_add(1);
        self.index.insert(pubkey, slot);
        self.push_heap_entry(slot);
        self.rebuild_heap_if_stale_heavy();
    }

    fn min_slot(&mut self) -> usize {
        loop {
            let Some(Reverse((estimate, generation, slot))) = self.heap.pop() else {
                self.rebuild_heap();
                continue;
            };
            let Some(entry) = self.entries.get(slot) else {
                continue;
            };
            if entry.estimate == estimate && entry.generation == generation {
                return slot;
            }
        }
    }

    fn push_heap_entry(&mut self, slot: usize) {
        let entry = &self.entries[slot];
        self.heap
            .push(Reverse((entry.estimate, entry.generation, slot)));
    }

    fn rebuild_heap_if_stale_heavy(&mut self) {
        let max_heap_len = self.entries.len().saturating_mul(4).max(1024);
        if self.heap.len() > max_heap_len {
            self.rebuild_heap();
        }
    }

    fn rebuild_heap(&mut self) {
        self.heap.clear();
        for slot in 0..self.entries.len() {
            self.push_heap_entry(slot);
        }
        self.heap_rebuilds = self.heap_rebuilds.saturating_add(1);
    }

    fn heap_rebuilds(&self) -> u64 {
        self.heap_rebuilds
    }

    fn estimated_heap_bytes(&self) -> u64 {
        let entry_bytes = size_of::<SpaceSavingEntry>();
        let heap_bytes = size_of::<Reverse<(u64, u64, usize)>>();
        let index_entry_bytes = size_of::<[u8; 32]>() + size_of::<usize>() + 1;
        let entries = self.entries.capacity().saturating_mul(entry_bytes);
        let heap = self.heap.capacity().saturating_mul(heap_bytes);
        let index = self.index.capacity().saturating_mul(index_entry_bytes);
        entries.saturating_add(heap).saturating_add(index) as u64
    }

    fn into_candidates(self) -> Vec<SpaceSavingCandidate> {
        let mut candidates = self
            .entries
            .into_iter()
            .map(|entry| SpaceSavingCandidate {
                pubkey: entry.pubkey,
                estimate: entry.estimate,
                error: entry.error,
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by(|a, b| {
            b.estimate
                .cmp(&a.estimate)
                .then_with(|| a.error.cmp(&b.error))
                .then_with(|| a.pubkey.cmp(&b.pubkey))
        });
        candidates
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

fn build_heavy_hitter_ordering_reports(
    records: &[LivePubkeyCountRecord],
    exact_order_id_varint_bytes: u128,
    lex_order_id_varint_bytes: u128,
    trackers: Vec<SpaceSavingPubkeyTracker>,
) -> Result<Vec<GrpcFixtureHeavyHitterOrderingReport>> {
    let mut lex_records = records.iter().collect::<Vec<_>>();
    lex_records.sort_unstable_by(|a, b| a.pubkey.cmp(&b.pubkey));
    let total_touches = records
        .iter()
        .map(|record| u128::from(record.count))
        .sum::<u128>();

    let mut reports = Vec::with_capacity(trackers.len());
    for tracker in trackers {
        let capacity = tracker.capacity;
        let estimated_heap_bytes = tracker.estimated_heap_bytes();
        let heap_rebuilds = tracker.heap_rebuilds();
        let candidates = tracker.into_candidates();
        let candidate_set = candidates
            .iter()
            .map(|candidate| (candidate.pubkey, ()))
            .collect::<GxHashMap<_, _>>();

        let mut approximate_order_id_varint_bytes = 0u128;
        let mut candidate_exact_touches = 0u128;
        let mut next_index = 0usize;
        for candidate in &candidates {
            let Some(record) = find_lex_record(&lex_records, &candidate.pubkey) else {
                continue;
            };
            let id = registry_id_for_index(next_index)?;
            approximate_order_id_varint_bytes = approximate_order_id_varint_bytes
                .saturating_add(u128::from(record.count) * u128::from(varint_len_u32(id)));
            candidate_exact_touches =
                candidate_exact_touches.saturating_add(u128::from(record.count));
            next_index += 1;
        }
        for record in &lex_records {
            if candidate_set.contains_key(&record.pubkey) {
                continue;
            }
            let id = registry_id_for_index(next_index)?;
            approximate_order_id_varint_bytes = approximate_order_id_varint_bytes
                .saturating_add(u128::from(record.count) * u128::from(varint_len_u32(id)));
            next_index += 1;
        }

        let extra_vs_exact_bytes =
            approximate_order_id_varint_bytes.saturating_sub(exact_order_id_varint_bytes);
        let saved_vs_lex_bytes =
            lex_order_id_varint_bytes.saturating_sub(approximate_order_id_varint_bytes);
        reports.push(GrpcFixtureHeavyHitterOrderingReport {
            capacity,
            candidates: candidates.len(),
            candidate_exact_touches,
            candidate_touch_ratio_pct: pct_u128(candidate_exact_touches, total_touches),
            estimated_heap_bytes,
            heap_rebuilds,
            approximate_order_id_varint_bytes,
            extra_vs_exact_bytes,
            extra_vs_exact_pct: pct_u128(extra_vs_exact_bytes, exact_order_id_varint_bytes),
            saved_vs_lex_bytes,
            saved_vs_lex_pct: pct_u128(saved_vs_lex_bytes, lex_order_id_varint_bytes),
            one_byte_id_overlap: top_overlap(records, &candidates, 127),
            two_byte_id_overlap: top_overlap(records, &candidates, 16_383),
        });
    }

    Ok(reports)
}

fn registry_id_varint_bytes<'a, I>(records: I) -> Result<u128>
where
    I: IntoIterator<Item = &'a LivePubkeyCountRecord>,
{
    let mut total = 0u128;
    for (index, record) in records.into_iter().enumerate() {
        let id = registry_id_for_index(index)?;
        total = total.saturating_add(u128::from(record.count) * u128::from(varint_len_u32(id)));
    }
    Ok(total)
}

fn lex_order_id_varint_bytes(records: &[LivePubkeyCountRecord]) -> Result<u128> {
    let mut lex_records = records.iter().collect::<Vec<_>>();
    lex_records.sort_unstable_by(|a, b| a.pubkey.cmp(&b.pubkey));
    registry_id_varint_bytes(lex_records.into_iter())
}

fn registry_id_for_index(index: usize) -> Result<u32> {
    let id = index.checked_add(1).context("registry id index overflow")?;
    u32::try_from(id).context("registry id exceeds u32::MAX")
}

fn varint_len_u32(mut value: u32) -> u32 {
    let mut len = 1u32;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn find_lex_record<'a>(
    lex_records: &[&'a LivePubkeyCountRecord],
    pubkey: &[u8; 32],
) -> Option<&'a LivePubkeyCountRecord> {
    lex_records
        .binary_search_by(|record| record.pubkey.cmp(pubkey))
        .ok()
        .map(|index| lex_records[index])
}

fn top_overlap(
    exact_records: &[LivePubkeyCountRecord],
    candidates: &[SpaceSavingCandidate],
    n: usize,
) -> usize {
    let n = n.min(exact_records.len());
    if n == 0 {
        return 0;
    }
    let exact_top = exact_records
        .iter()
        .take(n)
        .map(|record| (record.pubkey, ()))
        .collect::<GxHashMap<_, _>>();
    candidates
        .iter()
        .take(n)
        .filter(|candidate| exact_top.contains_key(&candidate.pubkey))
        .count()
}

fn pct_u128(value: u128, total: u128) -> f64 {
    if total == 0 {
        0.0
    } else {
        value as f64 * 100.0 / total as f64
    }
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
