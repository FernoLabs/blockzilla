use anyhow::{Context, Result};
use blockzilla_format::{
    CAR_BLOCK_INDEX_FILENAME, CAR_BLOCK_INDEX_VERSION, CAR_SLOT_RANGES_FILENAME,
    CarBlockIndexFooter, CarBlockIndexHeader, CarBlockIndexRecord, CarBlockIndexRow,
    CarBlockIndexSourceKind, WincodeLeb128FramedWriter,
};
use of_car_reader::{
    CarBlockReader,
    node::{Node, decode_node},
    slot_ranges::{
        SLOTS_PER_EPOCH, SlotRange, epoch_for_slot, slot_in_epoch, write_slot_ranges_raw,
    },
};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File},
    io::{BufWriter, Read},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

use crate::{Cli, file_nonempty, format_duration};

const INDEX_IO_BUFFER_SIZE: usize = 16 << 20;

#[derive(Debug, Clone)]
pub(crate) struct CarBlockIndexBuildStats {
    pub(crate) blocks: u64,
    pub(crate) car_entries: u64,
    pub(crate) source_logical_bytes: u64,
    pub(crate) elapsed_secs: f64,
}

#[derive(Debug, Clone)]
struct EpochCar {
    epoch: u64,
    path: PathBuf,
    source_kind: CarBlockIndexSourceKind,
}

#[derive(Debug, Clone)]
struct IndexJob {
    epoch: u64,
    input: PathBuf,
    output_dir: PathBuf,
    source_kind: CarBlockIndexSourceKind,
}

#[derive(Debug, Default)]
struct IndexBatchStats {
    successful: u64,
    skipped: u64,
    failed: Vec<(u64, String)>,
}

#[derive(Debug, Default)]
struct PendingBlockStats {
    first_entry_index: u64,
    first_car_offset: u64,
    node_count: u32,
    transaction_node_count: u32,
    entry_node_count: u32,
    dataframe_node_count: u32,
    rewards_node_count: u32,
    other_node_count: u32,
    expected_transaction_count: u32,
    poh_entry_count: u32,
    dataframe_ref_count: u32,
    transaction_payload_bytes: u64,
    transaction_data_bytes: u64,
    transaction_metadata_bytes: u64,
    entry_payload_bytes: u64,
    dataframe_payload_bytes: u64,
    dataframe_data_bytes: u64,
    rewards_payload_bytes: u64,
    rewards_data_bytes: u64,
    max_payload_len: u32,
    max_entry_total_len: u32,
}

#[derive(Debug, Default)]
struct FooterTotals {
    blocks: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    max_car_range_len: u64,
    max_payload_len: u32,
    max_entry_total_len: u32,
    transaction_node_count: u64,
    entry_node_count: u64,
    dataframe_node_count: u64,
    rewards_node_count: u64,
    other_node_count: u64,
    expected_transaction_count: u64,
    poh_entry_count: u64,
    shredding_count: u64,
    dataframe_ref_count: u64,
    transaction_payload_bytes: u64,
    transaction_data_bytes: u64,
    transaction_metadata_bytes: u64,
    entry_payload_bytes: u64,
    dataframe_payload_bytes: u64,
    dataframe_data_bytes: u64,
    rewards_payload_bytes: u64,
    rewards_data_bytes: u64,
}

pub(crate) fn build(
    input: &Path,
    output_dir: &Path,
    epoch: Option<u64>,
    max_blocks: Option<u64>,
    resume: bool,
) -> Result<CarBlockIndexBuildStats> {
    let source_kind = source_kind_for_path(input);
    build_one(input, output_dir, epoch, source_kind, max_blocks, resume)
}

pub(crate) fn build_all(
    cli: &Cli,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
    jobs: usize,
    overwrite: bool,
    max_blocks: Option<u64>,
) -> Result<()> {
    info!(
        "Scanning cache directory for CAR epochs: {}",
        cli.cache_dir.display()
    );

    if !cli.cache_dir.exists() {
        anyhow::bail!("Cache directory not found: {}", cli.cache_dir.display());
    }

    let epochs = discover_epoch_cars(&cli.cache_dir)?
        .into_iter()
        .filter(|car| start_epoch.is_none_or(|start| car.epoch >= start))
        .filter(|car| end_epoch.is_none_or(|end| car.epoch <= end))
        .collect::<Vec<_>>();

    if epochs.is_empty() {
        warn!("No CAR epoch files found in {}", cli.cache_dir.display());
        return Ok(());
    }

    let jobs_to_run = epochs
        .into_iter()
        .map(|car| IndexJob {
            epoch: car.epoch,
            input: car.path,
            output_dir: cli.output_dir.join(format!("epoch-{}", car.epoch)),
            source_kind: car.source_kind,
        })
        .collect::<VecDeque<_>>();

    let worker_count = jobs.max(1).min(jobs_to_run.len().max(1));
    info!(
        "Building CAR block indexes for {} epoch(s) with {} worker(s)",
        jobs_to_run.len(),
        worker_count
    );

    let queue = Arc::new(Mutex::new(jobs_to_run));
    let batch_stats = Arc::new(Mutex::new(IndexBatchStats::default()));
    let batch_start = Instant::now();
    let resume = cli.resume && !overwrite;

    thread::scope(|scope| {
        for worker_id in 0..worker_count {
            let queue = Arc::clone(&queue);
            let batch_stats = Arc::clone(&batch_stats);
            scope.spawn(move || {
                loop {
                    let Some(job) = queue.lock().expect("index queue poisoned").pop_front() else {
                        break;
                    };

                    let index_path = job.output_dir.join(CAR_BLOCK_INDEX_FILENAME);
                    let slot_ranges_path = job.output_dir.join(CAR_SLOT_RANGES_FILENAME);
                    if resume && file_nonempty(&index_path) && file_nonempty(&slot_ranges_path) {
                        info!(
                            "worker {}: resume skip epoch {} ({}, {})",
                            worker_id,
                            job.epoch,
                            index_path.display(),
                            slot_ranges_path.display()
                        );
                        batch_stats.lock().expect("index stats poisoned").skipped += 1;
                        continue;
                    }

                    match build_one(
                        &job.input,
                        &job.output_dir,
                        Some(job.epoch),
                        job.source_kind,
                        max_blocks,
                        false,
                    ) {
                        Ok(stats) => {
                            info!(
                                "worker {}: indexed epoch {}: {} blocks, {} CAR entries, {} logical bytes in {}",
                                worker_id,
                                job.epoch,
                                stats.blocks,
                                stats.car_entries,
                                stats.source_logical_bytes,
                                format_duration(stats.elapsed_secs)
                            );
                            batch_stats.lock().expect("index stats poisoned").successful += 1;
                        }
                        Err(err) => {
                            error!("worker {}: failed epoch {}: {err:?}", worker_id, job.epoch);
                            batch_stats
                                .lock()
                                .expect("index stats poisoned")
                                .failed
                                .push((job.epoch, format!("{err:?}")));
                        }
                    }
                }
            });
        }
    });

    let elapsed = batch_start.elapsed().as_secs_f64();
    let stats = batch_stats.lock().expect("index stats poisoned");
    info!("CAR block index batch complete");
    info!("  successful: {}", stats.successful);
    info!("  skipped:    {}", stats.skipped);
    info!("  failed:     {}", stats.failed.len());
    info!("  elapsed:    {}", format_duration(elapsed));

    if !stats.failed.is_empty() {
        anyhow::bail!("{} epoch index build(s) failed", stats.failed.len());
    }

    Ok(())
}

fn build_one(
    input: &Path,
    output_dir: &Path,
    epoch: Option<u64>,
    source_kind: CarBlockIndexSourceKind,
    max_blocks: Option<u64>,
    resume: bool,
) -> Result<CarBlockIndexBuildStats> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("create output directory {}", output_dir.display()))?;

    let index_path = output_dir.join(CAR_BLOCK_INDEX_FILENAME);
    let slot_ranges_path = output_dir.join(CAR_SLOT_RANGES_FILENAME);
    if resume && file_nonempty(&index_path) && file_nonempty(&slot_ranges_path) {
        info!(
            "Resume: CAR block index exists, skipping: {}",
            index_path.display()
        );
        return Ok(CarBlockIndexBuildStats {
            blocks: 0,
            car_entries: 0,
            source_logical_bytes: 0,
            elapsed_secs: 0.0,
        });
    }

    let started = Instant::now();
    let source_file_len = fs::metadata(input)
        .with_context(|| format!("stat input CAR {}", input.display()))?
        .len();

    let index_tmp = tmp_path(&index_path);
    let slot_ranges_tmp = tmp_path(&slot_ranges_path);
    let input_reader = open_car_input(input)?;
    let mut reader = CarBlockReader::with_capacity(input_reader, INDEX_IO_BUFFER_SIZE);
    reader
        .skip_header()
        .with_context(|| format!("read CAR header from {}", input.display()))?;

    let index_file = File::create(&index_tmp)
        .with_context(|| format!("create temp index {}", index_tmp.display()))?;
    let mut writer = WincodeLeb128FramedWriter::new(BufWriter::new(index_file));
    writer.write(&CarBlockIndexRecord::Header(CarBlockIndexHeader {
        version: CAR_BLOCK_INDEX_VERSION,
        source_kind,
        source_epoch: epoch,
        source_file_len,
        slots_per_epoch: SLOTS_PER_EPOCH,
    }))?;

    let mut scratch = Vec::with_capacity(256 << 10);
    let mut pending = None;
    let mut totals = FooterTotals::default();
    let mut slot_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut last_report = Instant::now();
    let mut truncated = false;

    while let Some(entry) = reader
        .read_entry_payload_with_scratch(&mut scratch)
        .with_context(|| format!("read CAR entry from {}", input.display()))?
    {
        let node = decode_node(entry.payload).with_context(|| {
            format!(
                "decode CAR node at entry {} offset {}",
                entry.location.entry_index, entry.location.car_offset
            )
        })?;

        match node {
            Node::Transaction(tx) => {
                let stats = pending_for(&mut pending, &entry);
                stats.observe_entry(&entry);
                stats.transaction_node_count = stats.transaction_node_count.saturating_add(1);
                stats.transaction_payload_bytes = stats
                    .transaction_payload_bytes
                    .saturating_add(entry.payload_len as u64);
                stats.transaction_data_bytes = stats
                    .transaction_data_bytes
                    .saturating_add(tx.data.data.len() as u64);
                stats.transaction_metadata_bytes = stats
                    .transaction_metadata_bytes
                    .saturating_add(tx.metadata.data.len() as u64);
                stats.dataframe_ref_count = stats
                    .dataframe_ref_count
                    .saturating_add(frame_ref_count(tx.data.next.as_ref()))
                    .saturating_add(frame_ref_count(tx.metadata.next.as_ref()));
            }
            Node::Entry(entry_node) => {
                let stats = pending_for(&mut pending, &entry);
                stats.observe_entry(&entry);
                stats.entry_node_count = stats.entry_node_count.saturating_add(1);
                stats.poh_entry_count = stats.poh_entry_count.saturating_add(1);
                stats.expected_transaction_count = stats
                    .expected_transaction_count
                    .saturating_add(saturating_u32(entry_node.transactions.len()));
                stats.entry_payload_bytes = stats
                    .entry_payload_bytes
                    .saturating_add(entry.payload_len as u64);
            }
            Node::Rewards(rewards) => {
                let stats = pending_for(&mut pending, &entry);
                stats.observe_entry(&entry);
                stats.rewards_node_count = stats.rewards_node_count.saturating_add(1);
                stats.rewards_payload_bytes = stats
                    .rewards_payload_bytes
                    .saturating_add(entry.payload_len as u64);
                stats.rewards_data_bytes = stats
                    .rewards_data_bytes
                    .saturating_add(rewards.data.data.len() as u64);
                stats.dataframe_ref_count = stats
                    .dataframe_ref_count
                    .saturating_add(frame_ref_count(rewards.data.next.as_ref()));
            }
            Node::DataFrame(frame) => {
                let stats = pending_for(&mut pending, &entry);
                stats.observe_entry(&entry);
                stats.dataframe_node_count = stats.dataframe_node_count.saturating_add(1);
                stats.dataframe_payload_bytes = stats
                    .dataframe_payload_bytes
                    .saturating_add(entry.payload_len as u64);
                stats.dataframe_data_bytes = stats
                    .dataframe_data_bytes
                    .saturating_add(frame.data.len() as u64);
                stats.dataframe_ref_count = stats
                    .dataframe_ref_count
                    .saturating_add(frame_ref_count(frame.next.as_ref()));
            }
            Node::Block(block) => {
                let mut stats = pending
                    .take()
                    .unwrap_or_else(|| PendingBlockStats::new(&entry));
                stats.observe_entry(&entry);

                let row = stats.finish(
                    block.slot,
                    u32::try_from(totals.blocks).context("block id exceeds u32")?,
                    entry.location.entry_index,
                    entry.location.car_offset,
                    entry.payload_len,
                    entry
                        .location
                        .car_offset
                        .saturating_add(entry.total_len as u64),
                    saturating_u32(block.shredding.len()),
                );
                write_row(&mut writer, &row, &mut totals)?;
                write_slot_range(epoch, &row, &mut slot_ranges)?;

                if let Some(max_blocks) = max_blocks
                    && totals.blocks >= max_blocks
                {
                    truncated = true;
                    break;
                }
            }
            Node::Subset(_) | Node::Epoch(_) => {
                if let Some(stats) = pending.as_mut() {
                    stats.observe_entry(&entry);
                    stats.other_node_count = stats.other_node_count.saturating_add(1);
                }
            }
        }

        if last_report.elapsed() >= Duration::from_secs(crate::PROGRESS_REPORT_INTERVAL_SECS) {
            let elapsed = started.elapsed().as_secs_f64();
            info!(
                "indexing {}: {} blocks, {} CAR entries, {:.1} MiB logical, {:.1} blocks/s",
                input.display(),
                totals.blocks,
                reader.entry_index,
                reader.offset as f64 / 1024.0 / 1024.0,
                totals.blocks as f64 / elapsed.max(0.001)
            );
            last_report = Instant::now();
        }
    }

    if pending.is_some() {
        warn!(
            "input ended with an unterminated pending block group: {}",
            input.display()
        );
    }

    writer.write(&CarBlockIndexRecord::Footer(CarBlockIndexFooter {
        version: CAR_BLOCK_INDEX_VERSION,
        truncated,
        blocks: totals.blocks,
        car_entries: reader.entry_index,
        source_logical_bytes: reader.offset,
        first_slot: totals.first_slot,
        last_slot: totals.last_slot,
        max_car_range_len: totals.max_car_range_len,
        max_payload_len: totals.max_payload_len,
        max_entry_total_len: totals.max_entry_total_len,
        transaction_node_count: totals.transaction_node_count,
        entry_node_count: totals.entry_node_count,
        dataframe_node_count: totals.dataframe_node_count,
        rewards_node_count: totals.rewards_node_count,
        other_node_count: totals.other_node_count,
        expected_transaction_count: totals.expected_transaction_count,
        poh_entry_count: totals.poh_entry_count,
        shredding_count: totals.shredding_count,
        dataframe_ref_count: totals.dataframe_ref_count,
        transaction_payload_bytes: totals.transaction_payload_bytes,
        transaction_data_bytes: totals.transaction_data_bytes,
        transaction_metadata_bytes: totals.transaction_metadata_bytes,
        entry_payload_bytes: totals.entry_payload_bytes,
        dataframe_payload_bytes: totals.dataframe_payload_bytes,
        dataframe_data_bytes: totals.dataframe_data_bytes,
        rewards_payload_bytes: totals.rewards_payload_bytes,
        rewards_data_bytes: totals.rewards_data_bytes,
    }))?;
    writer.flush()?;
    drop(writer);

    let slot_file = File::create(&slot_ranges_tmp)
        .with_context(|| format!("create temp slot ranges {}", slot_ranges_tmp.display()))?;
    let mut slot_writer = BufWriter::new(slot_file);
    write_slot_ranges_raw(&mut slot_writer, &slot_ranges)
        .with_context(|| format!("write slot ranges {}", slot_ranges_tmp.display()))?;
    drop(slot_writer);

    fs::rename(&index_tmp, &index_path).with_context(|| {
        format!(
            "publish CAR block index {} -> {}",
            index_tmp.display(),
            index_path.display()
        )
    })?;
    fs::rename(&slot_ranges_tmp, &slot_ranges_path).with_context(|| {
        format!(
            "publish CAR slot ranges {} -> {}",
            slot_ranges_tmp.display(),
            slot_ranges_path.display()
        )
    })?;

    Ok(CarBlockIndexBuildStats {
        blocks: totals.blocks,
        car_entries: reader.entry_index,
        source_logical_bytes: reader.offset,
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

fn write_row(
    writer: &mut WincodeLeb128FramedWriter<BufWriter<File>>,
    row: &CarBlockIndexRow,
    totals: &mut FooterTotals,
) -> Result<()> {
    writer.write(&CarBlockIndexRecord::Row(row.clone()))?;

    totals.blocks = totals.blocks.saturating_add(1);
    totals.first_slot.get_or_insert(row.slot);
    totals.last_slot = Some(row.slot);
    totals.max_car_range_len = totals.max_car_range_len.max(row.car_range_len);
    totals.max_payload_len = totals.max_payload_len.max(row.max_payload_len);
    totals.max_entry_total_len = totals.max_entry_total_len.max(row.max_entry_total_len);
    totals.transaction_node_count = totals
        .transaction_node_count
        .saturating_add(row.transaction_node_count as u64);
    totals.entry_node_count = totals
        .entry_node_count
        .saturating_add(row.entry_node_count as u64);
    totals.dataframe_node_count = totals
        .dataframe_node_count
        .saturating_add(row.dataframe_node_count as u64);
    totals.rewards_node_count = totals
        .rewards_node_count
        .saturating_add(row.rewards_node_count as u64);
    totals.other_node_count = totals
        .other_node_count
        .saturating_add(row.other_node_count as u64);
    totals.expected_transaction_count = totals
        .expected_transaction_count
        .saturating_add(row.expected_transaction_count as u64);
    totals.poh_entry_count = totals
        .poh_entry_count
        .saturating_add(row.poh_entry_count as u64);
    totals.shredding_count = totals
        .shredding_count
        .saturating_add(row.shredding_count as u64);
    totals.dataframe_ref_count = totals
        .dataframe_ref_count
        .saturating_add(row.dataframe_ref_count as u64);
    totals.transaction_payload_bytes = totals
        .transaction_payload_bytes
        .saturating_add(row.transaction_payload_bytes);
    totals.transaction_data_bytes = totals
        .transaction_data_bytes
        .saturating_add(row.transaction_data_bytes);
    totals.transaction_metadata_bytes = totals
        .transaction_metadata_bytes
        .saturating_add(row.transaction_metadata_bytes);
    totals.entry_payload_bytes = totals
        .entry_payload_bytes
        .saturating_add(row.entry_payload_bytes);
    totals.dataframe_payload_bytes = totals
        .dataframe_payload_bytes
        .saturating_add(row.dataframe_payload_bytes);
    totals.dataframe_data_bytes = totals
        .dataframe_data_bytes
        .saturating_add(row.dataframe_data_bytes);
    totals.rewards_payload_bytes = totals
        .rewards_payload_bytes
        .saturating_add(row.rewards_payload_bytes);
    totals.rewards_data_bytes = totals
        .rewards_data_bytes
        .saturating_add(row.rewards_data_bytes);

    Ok(())
}

fn write_slot_range(
    source_epoch: Option<u64>,
    row: &CarBlockIndexRow,
    slot_ranges: &mut [SlotRange],
) -> Result<()> {
    let row_epoch = epoch_for_slot(row.slot);
    if let Some(source_epoch) = source_epoch
        && row_epoch != source_epoch
    {
        warn!(
            "slot {} belongs to epoch {}, not source epoch {}; skipping slot-range row",
            row.slot, row_epoch, source_epoch
        );
        return Ok(());
    }

    let in_epoch = slot_in_epoch(row.slot);
    let idx = usize::try_from(in_epoch).context("slot-in-epoch exceeds usize")?;
    let len = u32::try_from(row.car_range_len).context("CAR block range exceeds u32")?;
    slot_ranges[idx] = SlotRange {
        offset: row.first_car_offset,
        len,
    };
    Ok(())
}

fn pending_for<'a>(
    pending: &'a mut Option<PendingBlockStats>,
    entry: &of_car_reader::reader::CarEntryPayload<'_>,
) -> &'a mut PendingBlockStats {
    pending.get_or_insert_with(|| PendingBlockStats::new(entry))
}

impl PendingBlockStats {
    fn new(entry: &of_car_reader::reader::CarEntryPayload<'_>) -> Self {
        Self {
            first_entry_index: entry.location.entry_index,
            first_car_offset: entry.location.car_offset,
            ..Self::default()
        }
    }

    fn observe_entry(&mut self, entry: &of_car_reader::reader::CarEntryPayload<'_>) {
        self.node_count = self.node_count.saturating_add(1);
        self.max_payload_len = self.max_payload_len.max(saturating_u32(entry.payload_len));
        self.max_entry_total_len = self
            .max_entry_total_len
            .max(saturating_u32(entry.total_len));
    }

    fn finish(
        self,
        slot: u64,
        block_id: u32,
        block_entry_index: u64,
        block_car_offset: u64,
        block_payload_len: usize,
        block_end_offset: u64,
        shredding_count: u32,
    ) -> CarBlockIndexRow {
        CarBlockIndexRow {
            slot,
            block_id,
            first_entry_index: self.first_entry_index,
            block_entry_index,
            first_car_offset: self.first_car_offset,
            block_car_offset,
            car_range_len: block_end_offset.saturating_sub(self.first_car_offset),
            node_count: self.node_count,
            transaction_node_count: self.transaction_node_count,
            entry_node_count: self.entry_node_count,
            dataframe_node_count: self.dataframe_node_count,
            rewards_node_count: self.rewards_node_count,
            other_node_count: self.other_node_count,
            expected_transaction_count: self.expected_transaction_count,
            poh_entry_count: self.poh_entry_count,
            shredding_count,
            dataframe_ref_count: self.dataframe_ref_count,
            transaction_payload_bytes: self.transaction_payload_bytes,
            transaction_data_bytes: self.transaction_data_bytes,
            transaction_metadata_bytes: self.transaction_metadata_bytes,
            entry_payload_bytes: self.entry_payload_bytes,
            dataframe_payload_bytes: self.dataframe_payload_bytes,
            dataframe_data_bytes: self.dataframe_data_bytes,
            rewards_payload_bytes: self.rewards_payload_bytes,
            rewards_data_bytes: self.rewards_data_bytes,
            block_payload_len: saturating_u32(block_payload_len),
            max_payload_len: self.max_payload_len,
            max_entry_total_len: self.max_entry_total_len,
        }
    }
}

fn frame_ref_count(
    view: Option<&of_car_reader::node::CborArrayView<'_, of_car_reader::node::CborCidRef<'_>>>,
) -> u32 {
    view.map(|v| saturating_u32(v.len())).unwrap_or(0)
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn tmp_path(path: &Path) -> PathBuf {
    path.with_extension(format!(
        "{}.tmp.{}",
        path.extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("out"),
        std::process::id()
    ))
}

fn open_car_input(path: &Path) -> Result<Box<dyn Read>> {
    let file = File::open(path).with_context(|| format!("open CAR input {}", path.display()))?;
    if path.to_string_lossy().ends_with(".zst") {
        let decoder = zstd::stream::read::Decoder::new(file)
            .with_context(|| format!("open zstd CAR input {}", path.display()))?;
        Ok(Box::new(decoder))
    } else {
        Ok(Box::new(file))
    }
}

fn discover_epoch_cars(cache_dir: &Path) -> Result<Vec<EpochCar>> {
    let mut by_epoch = BTreeMap::<u64, EpochCar>::new();

    for entry in fs::read_dir(cache_dir)
        .with_context(|| format!("read cache directory {}", cache_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some((epoch, source_kind)) = parse_epoch_car_filename(name) else {
            continue;
        };

        by_epoch
            .entry(epoch)
            .and_modify(|existing| {
                if should_replace_source(existing.source_kind, source_kind) {
                    existing.path = path.clone();
                    existing.source_kind = source_kind;
                }
            })
            .or_insert(EpochCar {
                epoch,
                path,
                source_kind,
            });
    }

    Ok(by_epoch.into_values().collect())
}

fn parse_epoch_car_filename(filename: &str) -> Option<(u64, CarBlockIndexSourceKind)> {
    let rest = filename.strip_prefix("epoch-")?;
    if let Some(number) = rest.strip_suffix(".car") {
        return number
            .parse()
            .ok()
            .map(|epoch| (epoch, CarBlockIndexSourceKind::PlainCar));
    }
    if let Some(number) = rest.strip_suffix(".car.zst") {
        return number
            .parse()
            .ok()
            .map(|epoch| (epoch, CarBlockIndexSourceKind::ZstdCar));
    }
    None
}

fn source_kind_for_path(path: &Path) -> CarBlockIndexSourceKind {
    let path = path.to_string_lossy();
    if path.ends_with(".car") {
        CarBlockIndexSourceKind::PlainCar
    } else if path.ends_with(".car.zst") || path.ends_with(".zst") {
        CarBlockIndexSourceKind::ZstdCar
    } else {
        CarBlockIndexSourceKind::Unknown
    }
}

fn should_replace_source(
    existing: CarBlockIndexSourceKind,
    candidate: CarBlockIndexSourceKind,
) -> bool {
    source_priority(candidate) > source_priority(existing)
}

fn source_priority(kind: CarBlockIndexSourceKind) -> u8 {
    match kind {
        CarBlockIndexSourceKind::PlainCar => 2,
        CarBlockIndexSourceKind::ZstdCar => 1,
        CarBlockIndexSourceKind::Unknown => 0,
    }
}
