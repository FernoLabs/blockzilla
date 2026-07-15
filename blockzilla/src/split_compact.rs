use anyhow::{Context, Result, anyhow};
use blockzilla_format::{write_registry_iter, write_u32_varint};
use gxhash::{GxBuildHasher, HashMap as GxHashMap};
use of_car_reader::{
    CarBlockReader,
    confirmed_block::{Reward, TransactionStatusMeta},
    metadata_decoder::{
        ReturnDataVisit, TokenBalanceVisit, TransactionStatusMetaVisitor, ZstdReusableDecoder,
        decode_transaction_status_meta_from_frame, slot_uses_protobuf_metadata,
        visit_protobuf_transaction_status_meta,
    },
    reconstruct::LosslessCarBlock,
};
use prost::Message;
use solana_pubkey::{Pubkey, pubkey};
use solana_short_vec::decode_shortu16_len;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
    str::FromStr,
    time::{Duration, Instant},
};
use tracing::info;

use crate::{BUFFER_SIZE, ProgressTracker, genesis_epoch0};

const MAX_BLOCKHASHES_PER_EPOCH: usize = 432_000;
const MESSAGE_VERSION_PREFIX: u8 = 0x80;
const ZSTD_WINDOW_LOG_MAX: u32 = 31;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CarRegistryBenchStrategy {
    ExactOld,
    ExactStream,
    UniqueSpaceSaving,
}

#[derive(Debug, Clone)]
pub(crate) struct CarRegistryBenchConfig<'a> {
    pub(crate) input: &'a Path,
    pub(crate) output_dir: Option<&'a Path>,
    pub(crate) strategy: CarRegistryBenchStrategy,
    pub(crate) initial_capacity: usize,
    pub(crate) heavy_hitter_capacity: usize,
    pub(crate) max_blocks: Option<u64>,
}

fn require_block(block: &LosslessCarBlock) -> Result<&of_car_reader::reconstruct::RawBlockNode> {
    block
        .block
        .as_ref()
        .ok_or_else(|| anyhow!("lossless block was missing its terminal block node"))
}

fn tx_context(slot: u64, tx_index: usize, action: &str) -> String {
    format!("slot {slot} tx#{tx_index} {action}")
}

fn with_lossless_block_stream<F>(input: &Path, mut f: F) -> Result<()>
where
    F: FnMut(&LosslessCarBlock) -> Result<bool>,
{
    let input = open_car_input(input)?;
    let mut reader = CarBlockReader::with_capacity(input, BUFFER_SIZE);
    reader.skip_header()?;

    let mut block = LosslessCarBlock::default();
    while reader.read_until_block_lossless(&mut block)? {
        if f(&block)? {
            break;
        }
    }

    Ok(())
}

fn open_car_input(path: &Path) -> Result<Box<dyn Read>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let file = BufReader::with_capacity(BUFFER_SIZE, file);

    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
    {
        let mut decoder = zstd::Decoder::with_buffer(file)
            .with_context(|| format!("init zstd decoder for {}", path.display()))?;
        decoder
            .window_log_max(ZSTD_WINDOW_LOG_MAX)
            .with_context(|| format!("set zstd window_log_max for {}", path.display()))?;
        Ok(Box::new(decoder))
    } else {
        Ok(Box::new(file))
    }
}

pub(crate) fn build_registry_and_blockhash_for_input(
    input: &Path,
    registry_path: &Path,
    registry_counts_path: Option<&Path>,
    blockhash_registry_path: &Path,
    external_blockhashes: &ExternalBlockhashOverrides,
    max_blocks: Option<u64>,
) -> Result<()> {
    info!("Building registry + blockhash for {}", input.display());

    let mut blockhash_out: Vec<u8> = Vec::with_capacity(MAX_BLOCKHASHES_PER_EPOCH * 32);
    let mut counter = PubkeyCounter::new(8_000_000);
    let start = Instant::now();
    let mut progress = ProgressTracker::new("Split Compact Registry");
    let mut timings = RegistryBuildTimings::default();
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;
    if let Some(genesis) = &genesis {
        blockhash_out.extend_from_slice(&genesis.genesis_hash);
        genesis_epoch0::add_pubkeys_to(genesis, |key| counter.add32(key));
        info!(
            "Seeded epoch-0 genesis: accounts={} reward_pools={} builtins={} genesis_hash={}",
            genesis.genesis.accounts.len(),
            genesis.genesis.reward_pools.len(),
            genesis.genesis.builtins.len(),
            hex32(&genesis.genesis_hash)
        );
    }

    let stream_started = Instant::now();
    with_lossless_block_stream(input, |block| {
        let raw_block = require_block(block)?;
        let blockhash = blockhash_for_block(block, external_blockhashes)?;
        blockhash_out.extend_from_slice(&blockhash);

        let count_started = Instant::now();
        let txs = count_block_pubkeys(block, &mut counter, &mut timings)?;
        timings.count_block_total += count_started.elapsed();
        timings.blocks += 1;
        timings.txs += txs;
        progress.update_slot(raw_block.slot);
        progress.update(1, txs);
        Ok(max_blocks.is_some_and(|limit| timings.blocks >= limit))
    })?;
    timings.stream_total = stream_started.elapsed();

    progress.final_report();

    {
        let write_started = Instant::now();
        let mut file = File::create(blockhash_registry_path)
            .with_context(|| format!("create {}", blockhash_registry_path.display()))?;
        file.write_all(&blockhash_out)
            .with_context(|| format!("write {}", blockhash_registry_path.display()))?;
        file.flush()
            .with_context(|| format!("flush {}", blockhash_registry_path.display()))?;
        timings.write_blockhash = write_started.elapsed();
    }

    let sort_started = Instant::now();
    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });
    timings.sort_registry = sort_started.elapsed();

    const BUILTIN_PROGRAM_KEYS: &[Pubkey] =
        &[pubkey!("ComputeBudget111111111111111111111111111111")];
    let missing_builtins = BUILTIN_PROGRAM_KEYS
        .iter()
        .map(|key| key.to_bytes())
        .filter(|builtin| !items.iter().any(|(key, _)| key == builtin))
        .collect::<Vec<_>>();
    let registry_len = missing_builtins.len() + items.len();

    {
        let write_started = Instant::now();
        write_registry_iter(
            registry_path,
            missing_builtins
                .iter()
                .copied()
                .chain(items.iter().map(|(key, _)| *key)),
        )
        .with_context(|| format!("write {}", registry_path.display()))?;
        if let Some(path) = registry_counts_path {
            write_registry_counts(
                path,
                std::iter::repeat(0)
                    .take(missing_builtins.len())
                    .chain(items.iter().map(|(_, count)| *count)),
            )
            .with_context(|| format!("write {}", path.display()))?;
        }
        timings.write_registry = write_started.elapsed();
    }
    info!(
        "Registry + counts + blockhash built in {:.2}s: keys={} blockhashes={}",
        start.elapsed().as_secs_f64(),
        registry_len,
        blockhash_out.len() / 32
    );
    info!(
        "Registry instrumentation: stream_total={:.3}s stream_other={:.3}s count_block={:.3}s tx_reassemble={:.3}s tx_pubkey_scan={:.3}s metadata_reassemble={:.3}s metadata_decode_count={:.3}s sort={:.3}s write_blockhash={:.3}s write_registry={:.3}s blocks={} txs={} metadata_frames={} tx_scratch_max={} metadata_scratch_max={}",
        timings.stream_total.as_secs_f64(),
        timings
            .stream_total
            .saturating_sub(timings.count_block_total)
            .as_secs_f64(),
        timings.count_block_total.as_secs_f64(),
        timings.tx_reassemble.as_secs_f64(),
        timings.tx_pubkey_scan.as_secs_f64(),
        timings.metadata_reassemble.as_secs_f64(),
        timings.metadata_decode_count.as_secs_f64(),
        timings.sort_registry.as_secs_f64(),
        timings.write_blockhash.as_secs_f64(),
        timings.write_registry.as_secs_f64(),
        timings.blocks,
        timings.txs,
        timings.metadata_frames,
        timings.tx_scratch_max,
        timings.metadata_scratch_max,
    );
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct ExternalBlockhashOverride {
    pub(crate) hash: [u8; 32],
    pub(crate) source: String,
    pub(crate) reconstruct_ticks: Option<u32>,
    pub(crate) hashes_per_tick: Option<u64>,
}

pub(crate) type ExternalBlockhashOverrides = GxHashMap<u64, ExternalBlockhashOverride>;

pub(crate) fn load_external_blockhash_overrides(
    path: Option<&Path>,
) -> Result<ExternalBlockhashOverrides> {
    let Some(path) = path else {
        return Ok(ExternalBlockhashOverrides::default());
    };
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut overrides = ExternalBlockhashOverrides::default();
    for (line_index, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("read {} line {}", path.display(), line_index + 1))?;
        let line = line
            .split_once('#')
            .map_or(line.as_str(), |(value, _)| value)
            .trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let slot = parts
            .next()
            .ok_or_else(|| anyhow!("{} line {} missing slot", path.display(), line_index + 1))?
            .parse::<u64>()
            .with_context(|| format!("{} line {} parse slot", path.display(), line_index + 1))?;
        let blockhash = parts.next().ok_or_else(|| {
            anyhow!(
                "{} line {} missing blockhash",
                path.display(),
                line_index + 1
            )
        })?;
        let source_parts = parts.collect::<Vec<_>>();
        let source = source_parts.join(" ");
        anyhow::ensure!(
            !source.is_empty(),
            "{} line {} missing provenance source",
            path.display(),
            line_index + 1
        );
        let hash = Pubkey::from_str(blockhash)
            .with_context(|| {
                format!(
                    "{} line {} parse base58 blockhash",
                    path.display(),
                    line_index + 1
                )
            })?
            .to_bytes();
        let mut reconstruct_ticks = None;
        let mut hashes_per_tick = None;
        for part in &source_parts {
            if let Some(value) = part.strip_prefix("ticks=") {
                reconstruct_ticks = Some(value.parse::<u32>().with_context(|| {
                    format!("{} line {} parse ticks", path.display(), line_index + 1)
                })?);
            } else if let Some(value) = part.strip_prefix("hashes_per_tick=") {
                hashes_per_tick = Some(value.parse::<u64>().with_context(|| {
                    format!(
                        "{} line {} parse hashes_per_tick",
                        path.display(),
                        line_index + 1
                    )
                })?);
            }
        }
        anyhow::ensure!(
            reconstruct_ticks.is_some() == hashes_per_tick.is_some(),
            "{} line {} must provide both ticks= and hashes_per_tick=, or neither",
            path.display(),
            line_index + 1
        );
        let previous = overrides.insert(
            slot,
            ExternalBlockhashOverride {
                hash,
                source,
                reconstruct_ticks,
                hashes_per_tick,
            },
        );
        anyhow::ensure!(
            previous.is_none(),
            "{} line {} duplicates slot {}",
            path.display(),
            line_index + 1,
            slot
        );
    }
    info!(
        "Loaded external blockhash overrides: path={} slots={}",
        path.display(),
        overrides.len()
    );
    Ok(overrides)
}

fn write_registry_counts<I>(path: &Path, counts: I) -> Result<()>
where
    I: IntoIterator<Item = u32>,
{
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    for count in counts {
        write_u32_varint(&mut writer, count)?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn blockhash_for_block(
    block: &LosslessCarBlock,
    external_blockhashes: &ExternalBlockhashOverrides,
) -> Result<[u8; 32]> {
    let raw_block = require_block(block)?;
    if let Some(entry) = block.entries.last() {
        anyhow::ensure!(
            !external_blockhashes.contains_key(&raw_block.slot),
            "slot {} has PoH entries and must not use an external blockhash override",
            raw_block.slot
        );
        return Ok(entry.hash);
    }
    let override_entry = external_blockhashes.get(&raw_block.slot).ok_or_else(|| {
        anyhow!(
            "slot {} block has no PoH entries and no external blockhash override",
            raw_block.slot
        )
    })?;
    info!(
        "Using external blockhash override for PoH-gap slot {} source={} blockhash={}",
        raw_block.slot,
        override_entry.source,
        Pubkey::new_from_array(override_entry.hash)
    );
    Ok(override_entry.hash)
}

fn count_block_pubkeys(
    block: &LosslessCarBlock,
    counter: &mut impl PubkeySink,
    timings: &mut RegistryBuildTimings,
) -> Result<u64> {
    let raw_block = require_block(block)?;
    let mut zstd = ZstdReusableDecoder::new();
    let mut txs = 0u64;
    let mut tx_bytes = Vec::new();
    let mut metadata_bytes = Vec::new();
    let mut reassemble_visited = std::collections::HashSet::new();

    for (tx_index, tx_node) in block.transactions.iter().enumerate() {
        txs += 1;

        let reassemble_started = Instant::now();
        tx_node
            .transaction_bytes_into(&block.dataframes, &mut tx_bytes, &mut reassemble_visited)
            .with_context(|| {
                tx_context(raw_block.slot, tx_index, "reassemble transaction bytes")
            })?;
        timings.tx_reassemble += reassemble_started.elapsed();
        timings.tx_scratch_max = timings.tx_scratch_max.max(tx_bytes.capacity());
        let scan_started = Instant::now();
        count_transaction_pubkeys_from_bytes(&tx_bytes, counter)
            .with_context(|| tx_context(raw_block.slot, tx_index, "count transaction pubkeys"))?;
        timings.tx_pubkey_scan += scan_started.elapsed();

        let reassemble_started = Instant::now();
        tx_node
            .metadata_bytes_into(
                &block.dataframes,
                &mut metadata_bytes,
                &mut reassemble_visited,
            )
            .with_context(|| tx_context(raw_block.slot, tx_index, "reassemble metadata bytes"))?;
        timings.metadata_reassemble += reassemble_started.elapsed();
        timings.metadata_scratch_max = timings.metadata_scratch_max.max(metadata_bytes.capacity());
        if metadata_bytes.is_empty() {
            continue;
        }

        timings.metadata_frames += 1;
        let metadata_started = Instant::now();
        count_metadata_pubkeys_from_frame(raw_block.slot, &metadata_bytes, &mut zstd, counter)
            .with_context(|| tx_context(raw_block.slot, tx_index, "count metadata pubkeys"))?;
        timings.metadata_decode_count += metadata_started.elapsed();
    }

    Ok(txs)
}

pub(crate) fn bench_car_registry(config: CarRegistryBenchConfig<'_>) -> Result<()> {
    if let Some(output_dir) = config.output_dir {
        std::fs::create_dir_all(output_dir)
            .with_context(|| format!("create output dir {}", output_dir.display()))?;
    }
    let started = Instant::now();
    let rss_start = peak_rss_bytes();
    match config.strategy {
        CarRegistryBenchStrategy::ExactOld | CarRegistryBenchStrategy::ExactStream => {
            let mut counter = PubkeyCounter::new(config.initial_capacity);
            let mut timings = RegistryBuildTimings::default();
            stream_car_pubkeys(config.input, config.max_blocks, &mut counter, &mut timings)?;
            let rss_after_stream = peak_rss_bytes();
            let count_sum = counter.count_sum();
            let map_capacity = counter.counts.capacity();
            let estimated_counter_heap_bytes =
                exact_counter_estimated_heap_bytes(map_capacity, size_of::<u32>());
            let finalize_started = Instant::now();
            let final_report = match config.strategy {
                CarRegistryBenchStrategy::ExactOld => {
                    finalize_exact_old(counter, config.output_dir)?
                }
                CarRegistryBenchStrategy::ExactStream => {
                    finalize_exact_stream(counter, config.output_dir)?
                }
                CarRegistryBenchStrategy::UniqueSpaceSaving => unreachable!(),
            };
            let finalize_elapsed = finalize_started.elapsed();
            let rss_after_finalize = peak_rss_bytes();
            print_registry_bench_report(
                config.input,
                config.output_dir,
                config.strategy,
                &timings,
                RegistryBenchMemory {
                    rss_start,
                    rss_after_stream,
                    rss_after_finalize,
                    rss_end: peak_rss_bytes(),
                    estimated_counter_heap_bytes,
                    estimated_tracker_heap_bytes: 0,
                    map_capacity,
                },
                RegistryBenchFinalize {
                    registry_len: final_report.registry_len,
                    count_sum,
                    candidate_count: 0,
                    copied_key_count_bytes: final_report.copied_key_count_bytes,
                    finalize_elapsed,
                    total_elapsed: started.elapsed(),
                },
            );
        }
        CarRegistryBenchStrategy::UniqueSpaceSaving => {
            let mut counter = UniqueSpaceSavingCounter::new(
                config.initial_capacity,
                config.heavy_hitter_capacity,
            );
            let mut timings = RegistryBuildTimings::default();
            stream_car_pubkeys(config.input, config.max_blocks, &mut counter, &mut timings)?;
            let rss_after_stream = peak_rss_bytes();
            let touches = counter.touches;
            let map_capacity = counter.keys.capacity();
            let estimated_counter_heap_bytes =
                exact_counter_estimated_heap_bytes(map_capacity, size_of::<()>());
            let estimated_tracker_heap_bytes = counter.tracker.estimated_heap_bytes();
            let finalize_started = Instant::now();
            let final_report = finalize_unique_space_saving(counter, config.output_dir)?;
            let finalize_elapsed = finalize_started.elapsed();
            let rss_after_finalize = peak_rss_bytes();
            print_registry_bench_report(
                config.input,
                config.output_dir,
                config.strategy,
                &timings,
                RegistryBenchMemory {
                    rss_start,
                    rss_after_stream,
                    rss_after_finalize,
                    rss_end: peak_rss_bytes(),
                    estimated_counter_heap_bytes,
                    estimated_tracker_heap_bytes,
                    map_capacity,
                },
                RegistryBenchFinalize {
                    registry_len: final_report.registry_len,
                    count_sum: touches,
                    candidate_count: final_report.candidate_count,
                    copied_key_count_bytes: final_report.copied_key_count_bytes,
                    finalize_elapsed,
                    total_elapsed: started.elapsed(),
                },
            );
        }
    }
    Ok(())
}

fn stream_car_pubkeys(
    input: &Path,
    max_blocks: Option<u64>,
    counter: &mut impl PubkeySink,
    timings: &mut RegistryBuildTimings,
) -> Result<()> {
    let input_file = open_car_input(input)?;
    let mut reader = CarBlockReader::with_capacity(input_file, BUFFER_SIZE);
    reader.skip_header()?;
    let mut progress = ProgressTracker::new("CAR Registry Bench");
    let mut block = LosslessCarBlock::default();
    let stream_started = Instant::now();

    while reader.read_until_block_lossless(&mut block)? {
        let raw_block = require_block(&block)?;
        let count_started = Instant::now();
        let txs = count_block_pubkeys(&block, counter, timings)?;
        timings.count_block_total += count_started.elapsed();
        timings.blocks += 1;
        timings.txs += txs;
        progress.update_slot(raw_block.slot);
        progress.update(1, txs);
        if max_blocks.is_some_and(|limit| timings.blocks >= limit) {
            break;
        }
    }

    timings.stream_total = stream_started.elapsed();
    progress.final_report();
    Ok(())
}

struct RegistryBenchFinalizeReport {
    registry_len: usize,
    candidate_count: usize,
    copied_key_count_bytes: u64,
}

fn finalize_exact_old(
    counter: PubkeyCounter,
    output_dir: Option<&Path>,
) -> Result<RegistryBenchFinalizeReport> {
    let mut items = sorted_counter_items(counter);
    let missing_builtins = missing_builtin_keys(&items);
    let mut keys = Vec::with_capacity(missing_builtins.len() + items.len());
    let mut counts = Vec::with_capacity(missing_builtins.len() + items.len());
    for key in &missing_builtins {
        keys.push(*key);
        counts.push(0);
    }
    for (key, count) in items.drain(..) {
        keys.push(key);
        counts.push(count);
    }

    let copied_key_count_bytes =
        keys.capacity()
            .saturating_mul(size_of::<[u8; 32]>())
            .saturating_add(counts.capacity().saturating_mul(size_of::<u32>())) as u64;
    if let Some(output_dir) = output_dir {
        write_registry_iter(&output_dir.join("registry.bin"), keys.iter().copied())?;
        write_registry_counts(
            &output_dir.join("registry_counts.bin"),
            counts.iter().copied(),
        )?;
    }
    Ok(RegistryBenchFinalizeReport {
        registry_len: keys.len(),
        candidate_count: 0,
        copied_key_count_bytes,
    })
}

fn finalize_exact_stream(
    counter: PubkeyCounter,
    output_dir: Option<&Path>,
) -> Result<RegistryBenchFinalizeReport> {
    let items = sorted_counter_items(counter);
    let missing_builtins = missing_builtin_keys(&items);
    if let Some(output_dir) = output_dir {
        write_registry_iter(
            &output_dir.join("registry.bin"),
            missing_builtins
                .iter()
                .copied()
                .chain(items.iter().map(|(key, _)| *key)),
        )?;
        write_registry_counts(
            &output_dir.join("registry_counts.bin"),
            std::iter::repeat(0)
                .take(missing_builtins.len())
                .chain(items.iter().map(|(_, count)| *count)),
        )?;
    }
    Ok(RegistryBenchFinalizeReport {
        registry_len: missing_builtins.len() + items.len(),
        candidate_count: 0,
        copied_key_count_bytes: 0,
    })
}

fn finalize_unique_space_saving(
    counter: UniqueSpaceSavingCounter,
    output_dir: Option<&Path>,
) -> Result<RegistryBenchFinalizeReport> {
    let mut keys = counter.keys.into_keys().collect::<Vec<_>>();
    keys.sort_unstable();
    let candidates = counter.tracker.into_candidates();
    let candidate_estimates = candidates
        .iter()
        .map(|candidate| (candidate.pubkey, candidate.estimate))
        .collect::<GxHashMap<_, _>>();
    let missing_builtins = missing_builtin_raw_keys(&keys);

    if let Some(output_dir) = output_dir {
        write_registry_iter(
            &output_dir.join("registry.bin"),
            missing_builtins
                .iter()
                .copied()
                .chain(candidates.iter().map(|candidate| candidate.pubkey))
                .chain(
                    keys.iter()
                        .copied()
                        .filter(|key| !candidate_estimates.contains_key(key)),
                ),
        )?;
        write_registry_counts(
            &output_dir.join("registry_counts_estimated.bin"),
            std::iter::repeat(0)
                .take(missing_builtins.len())
                .chain(
                    candidates
                        .iter()
                        .map(|candidate| u32::try_from(candidate.estimate).unwrap_or(u32::MAX)),
                )
                .chain(
                    keys.iter()
                        .filter(|key| !candidate_estimates.contains_key(*key))
                        .map(|_| 0),
                ),
        )?;
    }

    Ok(RegistryBenchFinalizeReport {
        registry_len: missing_builtins.len() + keys.len(),
        candidate_count: candidates.len(),
        copied_key_count_bytes: keys.capacity().saturating_mul(size_of::<[u8; 32]>()) as u64,
    })
}

fn sorted_counter_items(counter: PubkeyCounter) -> Vec<([u8; 32], u32)> {
    let mut items = counter.counts.into_iter().collect::<Vec<_>>();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });
    items
}

fn missing_builtin_keys(items: &[([u8; 32], u32)]) -> Vec<[u8; 32]> {
    builtin_program_keys()
        .iter()
        .copied()
        .filter(|builtin| !items.iter().any(|(key, _)| key == builtin))
        .collect()
}

fn missing_builtin_raw_keys(keys: &[[u8; 32]]) -> Vec<[u8; 32]> {
    builtin_program_keys()
        .iter()
        .copied()
        .filter(|builtin| keys.binary_search(builtin).is_err())
        .collect()
}

fn builtin_program_keys() -> &'static [[u8; 32]] {
    const BUILTIN_PROGRAM_KEYS: [[u8; 32]; 1] =
        [pubkey!("ComputeBudget111111111111111111111111111111").to_bytes()];
    &BUILTIN_PROGRAM_KEYS
}

struct RegistryBenchMemory {
    rss_start: Option<u64>,
    rss_after_stream: Option<u64>,
    rss_after_finalize: Option<u64>,
    rss_end: Option<u64>,
    estimated_counter_heap_bytes: u64,
    estimated_tracker_heap_bytes: u64,
    map_capacity: usize,
}

struct RegistryBenchFinalize {
    registry_len: usize,
    count_sum: u128,
    candidate_count: usize,
    copied_key_count_bytes: u64,
    finalize_elapsed: Duration,
    total_elapsed: Duration,
}

fn print_registry_bench_report(
    input: &Path,
    output_dir: Option<&Path>,
    strategy: CarRegistryBenchStrategy,
    timings: &RegistryBuildTimings,
    memory: RegistryBenchMemory,
    finalize: RegistryBenchFinalize,
) {
    println!("strategy={strategy:?}");
    println!("input={}", input.display());
    if let Some(output_dir) = output_dir {
        println!("output_dir={}", output_dir.display());
    }
    println!("blocks={}", timings.blocks);
    println!("txs={}", timings.txs);
    println!("metadata_frames={}", timings.metadata_frames);
    println!("registry_len={}", finalize.registry_len);
    println!("pubkey_touches={}", finalize.count_sum);
    println!("candidate_count={}", finalize.candidate_count);
    println!("map_capacity={}", memory.map_capacity);
    println!(
        "estimated_counter_heap_bytes={}",
        memory.estimated_counter_heap_bytes
    );
    println!(
        "estimated_tracker_heap_bytes={}",
        memory.estimated_tracker_heap_bytes
    );
    println!(
        "copied_finalize_key_count_bytes={}",
        finalize.copied_key_count_bytes
    );
    println!("rss_start_bytes={}", format_optional_u64(memory.rss_start));
    println!(
        "rss_after_stream_bytes={}",
        format_optional_u64(memory.rss_after_stream)
    );
    println!(
        "rss_after_finalize_bytes={}",
        format_optional_u64(memory.rss_after_finalize)
    );
    println!("rss_end_bytes={}", format_optional_u64(memory.rss_end));
    println!("stream_total_s={:.6}", timings.stream_total.as_secs_f64());
    println!(
        "stream_other_s={:.6}",
        timings
            .stream_total
            .saturating_sub(timings.count_block_total)
            .as_secs_f64()
    );
    println!(
        "count_block_s={:.6}",
        timings.count_block_total.as_secs_f64()
    );
    println!("tx_reassemble_s={:.6}", timings.tx_reassemble.as_secs_f64());
    println!(
        "tx_pubkey_scan_s={:.6}",
        timings.tx_pubkey_scan.as_secs_f64()
    );
    println!(
        "metadata_reassemble_s={:.6}",
        timings.metadata_reassemble.as_secs_f64()
    );
    println!(
        "metadata_decode_count_s={:.6}",
        timings.metadata_decode_count.as_secs_f64()
    );
    println!("finalize_s={:.6}", finalize.finalize_elapsed.as_secs_f64());
    println!("total_s={:.6}", finalize.total_elapsed.as_secs_f64());
    println!("tx_scratch_max={}", timings.tx_scratch_max);
    println!("metadata_scratch_max={}", timings.metadata_scratch_max);
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn exact_counter_estimated_heap_bytes(capacity: usize, value_bytes: usize) -> u64 {
    let entry_bytes = size_of::<[u8; 32]>() + value_bytes;
    let control_bytes = 1usize;
    capacity.saturating_mul(entry_bytes + control_bytes) as u64
}

#[derive(Default)]
struct RegistryBuildTimings {
    stream_total: Duration,
    count_block_total: Duration,
    tx_reassemble: Duration,
    tx_pubkey_scan: Duration,
    metadata_reassemble: Duration,
    metadata_decode_count: Duration,
    sort_registry: Duration,
    write_blockhash: Duration,
    write_registry: Duration,
    blocks: u64,
    txs: u64,
    metadata_frames: u64,
    tx_scratch_max: usize,
    metadata_scratch_max: usize,
}

fn count_transaction_pubkeys_from_bytes(
    tx_data: &[u8],
    counter: &mut impl PubkeySink,
) -> Result<()> {
    let mut cursor = TxPubkeyCursor::new(tx_data);
    let signature_count = cursor.read_short_len()?;
    cursor.skip(
        signature_count
            .checked_mul(64)
            .context("signature byte count overflow")?,
    )?;

    let first = cursor.read_u8()?;
    if first & MESSAGE_VERSION_PREFIX != 0 {
        let version = first & !MESSAGE_VERSION_PREFIX;
        if version != 0 {
            anyhow::bail!("unsupported transaction message version {version}");
        }
        cursor.skip(3)?;
        count_compiled_message_pubkeys(&mut cursor, counter)?;
        skip_compiled_instructions(&mut cursor)?;
        count_address_table_lookup_pubkeys(&mut cursor, counter)?;
    } else {
        cursor.skip(2)?;
        count_compiled_message_pubkeys(&mut cursor, counter)?;
        skip_compiled_instructions(&mut cursor)?;
    }

    if !cursor.is_empty() {
        anyhow::bail!("transaction has {} trailing bytes", cursor.remaining());
    }
    Ok(())
}

fn count_compiled_message_pubkeys(
    cursor: &mut TxPubkeyCursor<'_>,
    counter: &mut impl PubkeySink,
) -> Result<()> {
    let account_count = cursor.read_short_len()?;
    for _ in 0..account_count {
        counter.add32(cursor.take_array::<32>()?);
    }
    cursor.skip(32)?;
    Ok(())
}

fn skip_compiled_instructions(cursor: &mut TxPubkeyCursor<'_>) -> Result<()> {
    let instruction_count = cursor.read_short_len()?;
    for _ in 0..instruction_count {
        cursor.skip(1)?;
        let account_index_count = cursor.read_short_len()?;
        cursor.skip(account_index_count)?;
        let data_len = cursor.read_short_len()?;
        cursor.skip(data_len)?;
    }
    Ok(())
}

fn count_address_table_lookup_pubkeys(
    cursor: &mut TxPubkeyCursor<'_>,
    counter: &mut impl PubkeySink,
) -> Result<()> {
    let lookup_count = cursor.read_short_len()?;
    for _ in 0..lookup_count {
        counter.add32(cursor.take_array::<32>()?);
        let writable_count = cursor.read_short_len()?;
        cursor.skip(writable_count)?;
        let readonly_count = cursor.read_short_len()?;
        cursor.skip(readonly_count)?;
    }
    Ok(())
}

fn count_metadata_pubkeys_from_frame(
    slot: u64,
    metadata_bytes: &[u8],
    zstd: &mut ZstdReusableDecoder,
    counter: &mut impl PubkeySink,
) -> Result<()> {
    if slot_uses_protobuf_metadata(slot) {
        let protobuf_bytes = if zstd.decompress_if_zstd(metadata_bytes)? {
            zstd.output()
        } else {
            metadata_bytes
        };
        let mut visitor = RegistryPubkeyVisitor::new(counter);
        visit_protobuf_transaction_status_meta(protobuf_bytes, &mut visitor)
            .map_err(|err| anyhow!("protobuf visit: {err}"))?;
        visitor.finish()
    } else {
        let mut meta = TransactionStatusMeta::default();
        decode_transaction_status_meta_from_frame(slot, metadata_bytes, &mut meta, zstd)
            .map_err(|err| anyhow!("{err}"))?;
        count_metadata_pubkeys(&meta, counter)
    }
}

fn count_metadata_pubkeys(
    meta: &TransactionStatusMeta,
    counter: &mut impl PubkeySink,
) -> Result<()> {
    for pk in &meta.loaded_writable_addresses {
        let key: &[u8; 32] = pk
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("loaded writable address had len {}", pk.len()))?;
        counter.add32(key);
    }

    for pk in &meta.loaded_readonly_addresses {
        let key: &[u8; 32] = pk
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("loaded readonly address had len {}", pk.len()))?;
        counter.add32(key);
    }

    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        maybe_count_pubkey_string(counter, &balance.mint);
        maybe_count_pubkey_string(counter, &balance.owner);
        maybe_count_pubkey_string(counter, &balance.program_id);
    }

    for reward in &meta.rewards {
        maybe_count_pubkey_string(counter, &reward.pubkey);
    }

    if !meta.return_data_none
        && let Some(return_data) = meta.return_data.as_ref()
    {
        let key: &[u8; 32] = return_data.program_id.as_slice().try_into().map_err(|_| {
            anyhow!(
                "return data program_id had len {}",
                return_data.program_id.len()
            )
        })?;
        counter.add32(key);
    }

    Ok(())
}

fn maybe_count_pubkey_string<S: PubkeySink + ?Sized>(counter: &mut S, value: &str) {
    if value.is_empty() {
        return;
    }
    if let Ok(pubkey) = Pubkey::from_str(value) {
        counter.add32(pubkey.as_array());
    }
}

struct RegistryPubkeyVisitor<'a, S: PubkeySink + ?Sized> {
    counter: &'a mut S,
    error: Option<anyhow::Error>,
}

impl<'a, S: PubkeySink + ?Sized> RegistryPubkeyVisitor<'a, S> {
    fn new(counter: &'a mut S) -> Self {
        Self {
            counter,
            error: None,
        }
    }

    fn record_error(&mut self, err: anyhow::Error) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }

    fn finish(self) -> Result<()> {
        if let Some(err) = self.error {
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl<'a, 'b, S: PubkeySink + ?Sized> TransactionStatusMetaVisitor<'b>
    for RegistryPubkeyVisitor<'a, S>
{
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
    fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'b>) {
        maybe_count_pubkey_string(self.counter, balance.mint);
        maybe_count_pubkey_string(self.counter, balance.owner);
        maybe_count_pubkey_string(self.counter, balance.program_id);
    }

    #[inline]
    fn post_token_balance(&mut self, balance: TokenBalanceVisit<'b>) {
        maybe_count_pubkey_string(self.counter, balance.mint);
        maybe_count_pubkey_string(self.counter, balance.owner);
        maybe_count_pubkey_string(self.counter, balance.program_id);
    }

    #[inline]
    fn reward_raw(&mut self, bytes: &'b [u8]) {
        match Reward::decode(bytes) {
            Ok(reward) => maybe_count_pubkey_string(self.counter, &reward.pubkey),
            Err(err) => self.record_error(anyhow!("decode reward: {err}")),
        }
    }

    #[inline]
    fn loaded_writable_address(&mut self, address: &'b [u8]) {
        match address.try_into() {
            Ok(address) => self.counter.add32(address),
            Err(_) => {
                self.record_error(anyhow!("loaded writable address had len {}", address.len()))
            }
        }
    }

    #[inline]
    fn loaded_readonly_address(&mut self, address: &'b [u8]) {
        match address.try_into() {
            Ok(address) => self.counter.add32(address),
            Err(_) => {
                self.record_error(anyhow!("loaded readonly address had len {}", address.len()))
            }
        }
    }

    #[inline]
    fn return_data(&mut self, return_data: ReturnDataVisit<'b>) {
        match return_data.program_id.try_into() {
            Ok(program_id) => self.counter.add32(program_id),
            Err(_) => self.record_error(anyhow!(
                "return data program_id had len {}",
                return_data.program_id.len()
            )),
        }
    }
}

struct TxPubkeyCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> TxPubkeyCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn read_short_len(&mut self) -> Result<usize> {
        let (len, used) = decode_shortu16_len(&self.bytes[self.position..])
            .map_err(|()| anyhow!("decode short vec len"))?;
        self.position += used;
        Ok(len)
    }

    fn take_array<const N: usize>(&mut self) -> Result<&'a [u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| anyhow!("invalid fixed array length"))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .context("transaction cursor overflow")?;
        if end > self.bytes.len() {
            anyhow::bail!(
                "transaction cursor out of bounds: need {}, have {}",
                len,
                self.remaining()
            );
        }
        let out = &self.bytes[self.position..end];
        self.position = end;
        Ok(out)
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        self.take(len).map(|_| ())
    }
}

struct PubkeyCounter {
    counts: GxHashMap<[u8; 32], u32>,
}

trait PubkeySink {
    fn add32(&mut self, key: &[u8; 32]);
}

impl PubkeyCounter {
    fn new(capacity: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(capacity, GxBuildHasher::default()),
        }
    }

    fn count_sum(&self) -> u128 {
        self.counts.values().map(|count| u128::from(*count)).sum()
    }
}

impl PubkeySink for PubkeyCounter {
    fn add32(&mut self, key: &[u8; 32]) {
        let count = self.counts.entry(*key).or_insert(0);
        *count = count.saturating_add(1);
    }
}

struct UniqueSpaceSavingCounter {
    keys: GxHashMap<[u8; 32], ()>,
    tracker: SpaceSavingPubkeyTracker,
    touches: u128,
}

impl UniqueSpaceSavingCounter {
    fn new(key_capacity: usize, heavy_hitter_capacity: usize) -> Self {
        Self {
            keys: GxHashMap::with_capacity_and_hasher(key_capacity, GxBuildHasher::default()),
            tracker: SpaceSavingPubkeyTracker::new(heavy_hitter_capacity),
            touches: 0,
        }
    }
}

impl PubkeySink for UniqueSpaceSavingCounter {
    fn add32(&mut self, key: &[u8; 32]) {
        self.touches = self.touches.saturating_add(1);
        self.keys.entry(*key).or_insert(());
        self.tracker.increment(*key);
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

    fn estimated_heap_bytes(&self) -> u64 {
        let entry_bytes = size_of::<SpaceSavingEntry>();
        let heap_bytes = size_of::<Reverse<(u64, u64, usize)>>();
        let index_entry_bytes = size_of::<[u8; 32]>() + size_of::<usize>() + 1;
        self.entries
            .capacity()
            .saturating_mul(entry_bytes)
            .saturating_add(self.heap.capacity().saturating_mul(heap_bytes))
            .saturating_add(self.index.capacity().saturating_mul(index_entry_bytes)) as u64
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    #[cfg(target_os = "macos")]
    {
        u64::try_from(usage.ru_maxrss).ok()
    }
    #[cfg(target_os = "linux")]
    {
        u64::try_from(usage.ru_maxrss)
            .ok()
            .map(|kb| kb.saturating_mul(1024))
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn peak_rss_bytes() -> Option<u64> {
    None
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

pub(crate) fn load_blockhash_registry_plain(path: &Path) -> Result<Vec<[u8; 32]>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let bytes = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    if bytes % 32 != 0 {
        anyhow::bail!(
            "invalid blockhash registry length: {} (not multiple of 32) path={}",
            bytes,
            path.display()
        );
    }
    let count = usize::try_from(bytes / 32).context("blockhash registry length exceeds usize")?;
    let mut reader = BufReader::with_capacity(8 << 20, file);
    let mut hashes = Vec::with_capacity(count);
    for _ in 0..count {
        let mut hash = [0u8; 32];
        reader
            .read_exact(&mut hash)
            .with_context(|| format!("read {}", path.display()))?;
        hashes.push(hash);
    }
    Ok(hashes)
}
