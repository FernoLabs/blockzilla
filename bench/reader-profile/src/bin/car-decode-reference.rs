//! Full CAR transaction/metadata decode probe. This is a benchmark adapter,
//! not a new public SDK path. See README-car-decode.md for comparison limits.
#![recursion_limit = "256"]
use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
#[path = "../allocation.rs"]
// The mimalloc build rejects counters; keep the shared receipt types available.
#[cfg_attr(feature = "reference-mimalloc", allow(dead_code))]
mod allocation;
#[path = "car-decode-reference/decode_metrics.rs"]
mod decode_metrics;
#[path = "car-decode-reference/dynamic.rs"]
mod dynamic;
#[path = "car-decode-reference/metadata_visit.rs"]
mod metadata_visit;
#[path = "car-decode-reference/profile_control.rs"]
mod profile_control;
#[path = "car-decode-reference/request_retry.rs"]
mod request_retry;
#[path = "car-decode-reference/resident.rs"]
mod resident;
#[path = "car-decode-reference/resident_run.rs"]
mod resident_run;
#[path = "car-decode-reference/sharded.rs"]
mod sharded;
use metadata_visit::FullMetadataVisitor;
#[path = "../../../car-export/shared.rs"]
mod common_export;
use of_car_reader::{
    CarBlockReader, LosslessBlockReadLimits, OrderedLosslessCarBlock,
    confirmed_block::{Rewards, TransactionStatusMeta},
    metadata_decoder::{
        TransactionStatusMetaReuse, ZstdReusableDecoder, decode_rewards_from_frame,
        decode_transaction_status_meta_from_frame_reusing, slot_uses_protobuf_metadata,
        visit_protobuf_transaction_status_meta,
    },
    query_sdk_http::{CarHttpOptions, CarHttpSession, CarHttpStatsHandle},
    short_vec::decode_shortu16_len,
    versioned_transaction::{VersionedMessage, VersionedTransactionReuse},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

const MAX_DECODE_WORKERS: usize = 256;

#[cfg(feature = "reference-mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(not(feature = "reference-mimalloc"))]
#[global_allocator]
static SYSTEM_ALLOCATOR: allocation::Allocator = allocation::Allocator;

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum MetadataMode {
    #[default]
    Owned,
    Visitor,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
enum Scheduling {
    #[default]
    Static,
    Dynamic,
}
impl Scheduling {
    fn name(self) -> &'static str {
        match self {
            Self::Static => "static",
            Self::Dynamic => "dynamic",
        }
    }
}

#[derive(Parser)]
struct Args {
    #[arg(long, required_unless_present = "file", conflicts_with = "file")]
    url: Option<String>,
    #[arg(long)]
    file: Option<PathBuf>,
    /// Completed local cache manifest; require a fully mlocked CAR and index.
    #[arg(long, requires = "file", requires = "raw_index")]
    resident_cache_manifest: Option<PathBuf>,
    /// Local raw slot index for resident dynamic input.
    #[arg(long, requires = "resident_cache_manifest")]
    raw_index: Option<PathBuf>,
    /// Sample one transaction in approximately N for decode-stage wall times (0 disables).
    #[arg(long, default_value_t = 0)]
    stage_sample_every: u64,
    /// New directory for a scan-only external perf start/stop handshake.
    #[arg(long, requires = "resident_cache_manifest")]
    profile_control: Option<PathBuf>,
    /// Explicit trust for public Old Faithful sources without a strong ETag.
    #[arg(long, requires = "url")]
    operator_trusted: bool,
    /// Permit indexed HTTP only on literal 127.0.0.1 or ::1 for a local cache.
    #[arg(long, requires = "url")]
    allow_loopback_http: bool,
    /// Canonical prefix plan from extract_index_plan.py, starting at epoch start.
    #[arg(long)]
    plan: PathBuf,
    /// Full-decode workers (1..=256); raw block storage scales with this count.
    #[arg(long, default_value_t = 12)]
    workers: usize,
    /// Independent CAR parsers; split the total decode worker budget (1..=128).
    #[arg(long, default_value_t = 1)]
    parser_workers: usize,
    /// Static shards or dynamically assigned block-boundary jobs.
    #[arg(long, value_enum, default_value_t = Scheduling::Static)]
    scheduling: Scheduling,
    /// Independently built HTTP clients for indexed input (1..=parser-workers).
    #[arg(long, default_value_t = 1)]
    http_clients: usize,
    /// Target dynamic job size in MiB; a whole block is never split (1..=4096).
    #[arg(long, default_value_t = 256)]
    job_mib: usize,
    /// Decode all protobuf metadata fields through borrowed callbacks.
    #[arg(long, value_enum, default_value_t = MetadataMode::Owned)]
    metadata_mode: MetadataMode,
    /// HTTP range workers (1..=16), no more than the chunk window.
    #[arg(long, default_value_t = 4)]
    http_workers: usize,
    /// Maximum number of HTTP range bodies in flight (1..=16).
    #[arg(long, default_value_t = 8)]
    http_window_chunks: usize,
    /// Range body size in MiB: 1..=128 for indexed input, 1..=32 for SDK input.
    #[arg(long, default_value_t = 32)]
    http_chunk_mib: usize,
    /// Allocate each HTTP range body separately for a controlled comparison.
    #[arg(long)]
    legacy_http_buffers: bool,
    /// Count Rust allocations in a separate System allocator diagnostic run.
    #[arg(long)]
    allocations: bool,
    /// New output file. Never overwrites an earlier receipt.
    #[arg(long)]
    output: PathBuf,
    /// Identical binary transaction export for comparison with Jetstreamer.
    #[arg(long)]
    export: Option<PathBuf>,
    /// Temporary export storage. Memory mode is an explicit high-memory experiment.
    #[arg(long, default_value = "disk", value_parser = ["disk", "memory"])]
    export_mode: String,
    /// Maximum retained export payload in memory; block buffers and metadata are extra.
    #[arg(long, default_value_t = 196608)]
    export_memory_mib: usize,
}

struct HttpSettings {
    chunk_bytes: usize,
    body_window_bytes: usize,
    // Indexed reads do not construct options for the ordered SDK transport.
    ordered: Option<CarHttpOptions>,
}

impl Args {
    fn indexed(&self) -> bool {
        self.parser_workers > 1 || self.scheduling == Scheduling::Dynamic
    }

    fn validate_scheduling(&self) -> Result<()> {
        ensure!(
            (1..=self.parser_workers).contains(&self.http_clients),
            "http-clients must be 1..=parser-workers"
        );
        ensure!(
            (1..=4096).contains(&self.job_mib),
            "job-mib must be 1..=4096"
        );
        ensure!(
            !self.indexed()
                || (self.url.is_some() && self.operator_trusted)
                || self.resident_cache_manifest.is_some(),
            "indexed parser input requires --url and --operator-trusted"
        );
        ensure!(
            self.resident_cache_manifest.is_none() || self.scheduling == Scheduling::Dynamic,
            "resident input requires dynamic scheduling"
        );
        ensure!(
            self.stage_sample_every == 0 || self.resident_cache_manifest.is_some(),
            "stage metrics currently require resident input"
        );
        ensure!(
            self.resident_cache_manifest.is_none() || self.http_clients == 1,
            "resident input uses one shared mapping; use --http-clients 1"
        );
        ensure!(
            !self.allow_loopback_http || self.indexed(),
            "allow-loopback-http requires indexed parser input"
        );
        Ok(())
    }

    fn transport_mode(&self) -> &'static str {
        if self.resident_cache_manifest.is_some() {
            "resident-range"
        } else if self.scheduling == Scheduling::Dynamic {
            "dynamic-range"
        } else if self.parser_workers > 1 {
            "sharded-range"
        } else {
            "ordered-prefetch"
        }
    }

    fn http_settings(&self) -> Result<HttpSettings> {
        let chunk_bytes = self
            .http_chunk_mib
            .checked_mul(1 << 20)
            .context("http-chunk-mib overflows the byte count")?;
        let mut options = CarHttpOptions {
            workers: self.http_workers,
            window_chunks: self.http_window_chunks,
            reuse_body_buffers: !self.legacy_http_buffers,
            ..CarHttpOptions::default()
        };
        // Keep the requested worker/window controls valid even when indexed
        // input does not use them. This check uses the unchanged SDK chunk size.
        options.body_window_bytes()?;
        if self.indexed() {
            sharded::validate_chunk_bytes(chunk_bytes)?;
            return Ok(HttpSettings {
                chunk_bytes,
                body_window_bytes: self
                    .parser_workers
                    .checked_mul(chunk_bytes)
                    .context("indexed HTTP body window overflows the byte count")?,
                ordered: None,
            });
        }
        options.chunk_bytes = chunk_bytes;
        Ok(HttpSettings {
            chunk_bytes,
            body_window_bytes: options.body_window_bytes()?,
            ordered: Some(options),
        })
    }
}

fn validate_decode_workers(workers: usize) -> Result<()> {
    ensure!(
        (1..=MAX_DECODE_WORKERS).contains(&workers),
        "workers must be 1..={MAX_DECODE_WORKERS}"
    );
    Ok(())
}

fn http_receipt(
    transport: Option<&CarHttpStatsHandle>,
    parallel: Option<&sharded::Prepared>,
    scan_complete: bool,
) -> Option<Value> {
    if let Some(prepared) = parallel {
        return Some(prepared.stats(scan_complete));
    }
    transport.map(|handle| {
        let stats = handle.snapshot();
        json!({"body_buffer_allocations":stats.body_buffer_allocations,"body_buffer_allocated_bytes":stats.body_buffer_allocated_bytes,
            "get_requests":stats.get_requests,"body_bytes":stats.get_body_bytes_received,"bytes_delivered":stats.bytes_delivered,
            "incomplete_body_retries":stats.incomplete_body_retries,"workers_finished":stats.workers_finished})
    })
}

#[derive(Debug, PartialEq, Eq)]
struct Row {
    slot: u64,
    tx: u64,
    votes: u64,
    failed: u64,
    digest: String,
}

#[inline]
fn update_receipt_digest(
    digest: &mut blake3::Hasher,
    slot: u64,
    index: u64,
    signature: &[u8; 64],
    message_hash: &[u8; 32],
    vote: bool,
    failed: bool,
) {
    // Preserve the exact byte stream of the five separate receipt updates.
    let mut record = [0u8; 114];
    record[..8].copy_from_slice(&slot.to_le_bytes());
    record[8..16].copy_from_slice(&index.to_le_bytes());
    record[16..80].copy_from_slice(signature);
    record[80..112].copy_from_slice(message_hash);
    record[112] = u8::from(vote);
    record[113] = u8::from(failed);
    digest.update(&record);
}

#[derive(Default)]
struct Decoder {
    transaction: VersionedTransactionReuse,
    metadata: TransactionStatusMeta,
    metadata_reuse: TransactionStatusMetaReuse,
    zstd: ZstdReusableDecoder,
    rewards: Rewards,
    export_buffer: Vec<u8>,
    metrics: Option<decode_metrics::Worker>,
}
impl Decoder {
    fn with_metrics(metrics: Option<std::sync::Arc<decode_metrics::Metrics>>) -> Self {
        Self {
            metrics: metrics.map(|metrics| metrics.worker()),
            ..Self::default()
        }
    }

    fn record_stage(&mut self, stage: decode_metrics::Stage, started: Option<Instant>) {
        if let Some(started) = started {
            self.metrics
                .as_mut()
                .expect("timer requires a worker accumulator")
                .record(stage, started.elapsed());
        }
    }

    fn decode_owned_metadata(&mut self, slot: u64, frame: &[u8], sampled: bool) -> Result<bool> {
        if !sampled {
            decode_transaction_status_meta_from_frame_reusing(
                slot,
                frame,
                &mut self.metadata,
                &mut self.zstd,
                &mut self.metadata_reuse,
            )?;
        } else {
            // Match the frame decoder exactly. Retain its
            // empty-frame and legacy-slot behavior while timing zstd separately.
            let reset_started = Instant::now();
            decode_transaction_status_meta_from_frame_reusing(
                slot,
                &[],
                &mut self.metadata,
                &mut self.zstd,
                &mut self.metadata_reuse,
            )?;
            let mut deserialize_time = reset_started.elapsed();
            if !frame.is_empty() {
                let decompress_started = Instant::now();
                let compressed = self.zstd.decompress_if_zstd(frame).map_err(
                    of_car_reader::metadata_decoder::MetadataDecodeError::ZstdDecompress,
                )?;
                self.record_stage(
                    decode_metrics::Stage::MetadataDecompress,
                    Some(decompress_started),
                );
                let bytes = if compressed {
                    self.zstd.output()
                } else {
                    frame
                };
                let deserialize_started = Instant::now();
                of_car_reader::metadata_decoder::decode_transaction_status_meta_into_reusing(
                    slot,
                    bytes,
                    &mut self.metadata,
                    &mut self.metadata_reuse,
                )?;
                deserialize_time += deserialize_started.elapsed();
            }
            self.metrics
                .as_mut()
                .unwrap()
                .record(decode_metrics::Stage::MetadataDeserialize, deserialize_time);
        }
        std::hint::black_box(&self.metadata);
        Ok(self.metadata.err.is_some())
    }

    fn decode(
        &mut self,
        raw: &OrderedLosslessCarBlock,
        mode: MetadataMode,
        export: Option<&common_export::Export>,
    ) -> Result<Row> {
        if let Some(metrics) = &mut self.metrics {
            metrics.block_started();
        }
        let block_timing = self.metrics.is_some();
        self.export_buffer.clear();
        let slot = raw.block.as_ref().context("terminal block missing")?.slot;
        let mut votes = 0;
        let mut failed = 0;
        let mut digest = blake3::Hasher::new();
        const VOTE_KEY: [u8; 32] = [
            7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179, 216, 53, 94,
            115, 209, 16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0,
        ];
        for (index, raw_tx) in raw.transactions.iter().enumerate() {
            ensure!(
                raw_tx.slot == slot && raw_tx.index == Some(index as u64),
                "transaction order differs at slot {slot}"
            );
            ensure!(
                raw_tx.data.next.is_empty() && raw_tx.metadata.next.is_empty(),
                "transaction continuation requires the general lossless reader"
            );
            let sampled = self
                .metrics
                .as_mut()
                .is_some_and(|metrics| metrics.sample_transaction(slot, index as u64));
            let started = sampled.then(Instant::now);
            let tx = self
                .transaction
                .deserialize_transaction(&raw_tx.data.data)?;
            self.record_stage(decode_metrics::Stage::TransactionDeserialize, started);
            ensure!(!tx.signatures.is_empty(), "transaction has no signature");
            let is_failed =
                if matches!(mode, MetadataMode::Visitor) && slot_uses_protobuf_metadata(slot) {
                    let mut visitor = FullMetadataVisitor::default();
                    let started = sampled.then(Instant::now);
                    let compressed = self.zstd.decompress_if_zstd(&raw_tx.metadata.data)?;
                    self.record_stage(decode_metrics::Stage::MetadataDecompress, started);
                    let bytes = if compressed {
                        self.zstd.output()
                    } else {
                        &raw_tx.metadata.data
                    };
                    let started = sampled.then(Instant::now);
                    visit_protobuf_transaction_status_meta(bytes, &mut visitor)?;
                    let failed = visitor.finish()?;
                    self.record_stage(decode_metrics::Stage::MetadataDeserialize, started);
                    failed
                } else {
                    self.decode_owned_metadata(slot, &raw_tx.metadata.data, sampled)?
                };
            let started = sampled.then(Instant::now);
            let vote = match &tx.message {
                VersionedMessage::Legacy(m)
                    if (1..=2).contains(&tx.signatures.len()) && m.instructions.len() == 1 =>
                {
                    m.account_keys
                        .get(m.instructions[0].program_id_index as usize)
                        .is_some_and(|key| **key == VOTE_KEY)
                }
                _ => false,
            };
            self.record_stage(decode_metrics::Stage::VoteClassify, started);
            // Same message hash domain and encoded message bytes as Solana's
            // VersionedMessage::hash; avoid serializing data already on the wire.
            let started = sampled.then(Instant::now);
            let (count, prefix) = decode_shortu16_len(&raw_tx.data.data)
                .map_err(|_| anyhow::anyhow!("bad signature prefix"))?;
            let message = &raw_tx.data.data[prefix + count * 64..];
            let mut hash = blake3::Hasher::new();
            hash.update(b"solana-tx-message-v1");
            hash.update(message);
            self.record_stage(decode_metrics::Stage::MessageHash, started);
            if export.is_some() {
                let started = sampled.then(Instant::now);
                let export_hash = hash.finalize();
                self.record_stage(decode_metrics::Stage::MessageHash, started);
                let started = sampled.then(Instant::now);
                common_export::encode(
                    &mut self.export_buffer,
                    common_export::Transaction {
                        slot,
                        index: index as u64,
                        signature: tx.signatures[0],
                        message_hash: export_hash.as_bytes(),
                        vote,
                        failed: is_failed,
                        fee: self.metadata.fee,
                        pre_balances: &self.metadata.pre_balances,
                        post_balances: &self.metadata.post_balances,
                    },
                )?;
                self.record_stage(decode_metrics::Stage::ExportEncode, started);
            }
            let started = sampled.then(Instant::now);
            let receipt_hash = hash.finalize();
            self.record_stage(decode_metrics::Stage::MessageHash, started);
            let started = sampled.then(Instant::now);
            update_receipt_digest(
                &mut digest,
                slot,
                index as u64,
                tx.signatures[0],
                receipt_hash.as_bytes(),
                vote,
                is_failed,
            );
            self.record_stage(decode_metrics::Stage::ReceiptDigest, started);
            votes += u64::from(vote);
            failed += u64::from(is_failed);
            let started = sampled.then(Instant::now);
            self.transaction.recycle_transaction(tx);
            self.record_stage(decode_metrics::Stage::TransactionRecycle, started);
        }
        if let Some(rewards) = &raw.rewards {
            ensure!(
                rewards.data.next.is_empty(),
                "unresolved rewards continuation"
            );
            let started = block_timing.then(Instant::now);
            decode_rewards_from_frame(&rewards.data.data, &mut self.rewards, &mut self.zstd)?;
            std::hint::black_box(&self.rewards);
            self.record_stage(decode_metrics::Stage::Rewards, started);
        }
        ensure!(
            raw.entries
                .iter()
                .map(|e| u64::from(e.transaction_count))
                .sum::<u64>()
                == raw.transactions.len() as u64,
            "entry counts differ"
        );
        if let Some(export) = export {
            let started = block_timing.then(Instant::now);
            export.block(slot, raw.transactions.len() as u64, &self.export_buffer)?;
            self.record_stage(decode_metrics::Stage::BlockExport, started);
        }
        let started = block_timing.then(Instant::now);
        let digest = digest.finalize().to_hex().to_string();
        self.record_stage(decode_metrics::Stage::BlockDigest, started);
        Ok(Row {
            slot,
            tx: raw.transactions.len() as u64,
            votes,
            failed,
            digest,
        })
    }
}

fn scan(
    input: impl Read,
    end_slot: u64,
    workers: usize,
    mode: MetadataMode,
    export: Option<&common_export::Export>,
) -> Result<Vec<Row>> {
    validate_decode_workers(workers)?;
    let mut reader = CarBlockReader::with_capacity(input, 8 << 20);
    reader.skip_header_bounded(1 << 20)?;
    scan_reader(reader, end_slot, workers, mode, export, None)
}

fn scan_reader(
    mut reader: CarBlockReader<impl Read>,
    end_slot: u64,
    workers: usize,
    mode: MetadataMode,
    export: Option<&common_export::Export>,
    ranges: Option<&[sharded::BlockRange]>,
) -> Result<Vec<Row>> {
    validate_decode_workers(workers)?;
    let limits = LosslessBlockReadLimits {
        max_entry_payload_bytes: 16 << 20,
        max_block_payload_bytes: 16 << 20,
        max_entries_per_block: 65536,
        max_transactions_per_block: 65535,
    };
    // Exactly workers+2 reusable raw blocks. A large block above the stated
    // probe bound fails the run; it is never silently skipped.
    let buffers = workers + 2;
    let (free_tx, free_rx) = mpsc::sync_channel(buffers);
    let (work_tx, work_rx) = mpsc::sync_channel(buffers);
    let work_rx = Mutex::new(work_rx);
    let cancel = AtomicBool::new(false);
    for _ in 0..buffers {
        free_tx.send(OrderedLosslessCarBlock::default())?;
    }
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..workers {
            let free = free_tx.clone();
            let work = &work_rx;
            let cancel = &cancel;
            handles.push(scope.spawn(move || -> Result<Vec<Row>> {
                let mut decoder = Decoder::default();
                let mut rows = Vec::new();
                loop {
                    let next = {
                        work.lock()
                            .map_err(|_| anyhow::anyhow!("input queue poisoned"))?
                            .recv()
                    };
                    let Ok(mut block) = next else {
                        break;
                    };
                    if cancel.load(Ordering::Acquire) {
                        break;
                    }
                    let result = decoder.decode(&block, mode, export);
                    block.clear();
                    if block.data_buffer_pool_stats().retained_capacity > 16 << 20 {
                        block.release_reusable_data_buffers();
                    }
                    if result.is_err() {
                        cancel.store(true, Ordering::Release);
                    }
                    if free.send(block).is_err() {
                        break;
                    }
                    rows.push(result?);
                }
                Ok(rows)
            }));
        }
        drop(free_tx);
        let input_result = (|| -> Result<()> {
            let mut physical_blocks = 0;
            loop {
                if cancel.load(Ordering::Acquire) {
                    break;
                }
                let mut block = free_rx.recv().context("all decode workers stopped")?;
                if cancel.load(Ordering::Acquire) {
                    break;
                }
                let block_start = reader.offset;
                if !reader.read_until_block_ordered_lossless_bounded(&mut block, limits)? {
                    ensure!(
                        ranges.is_none_or(|ranges| physical_blocks == ranges.len()),
                        "shard ended before every indexed block was read"
                    );
                    break;
                }
                let slot = block.block.as_ref().context("missing block")?.slot;
                if let Some(ranges) = ranges {
                    let expected = ranges
                        .get(physical_blocks)
                        .context("extra block in shard")?;
                    ensure!(
                        block_start == expected.start && reader.offset == expected.end,
                        "CAR block byte range differs from index at slot {slot}"
                    );
                    ensure!(
                        slot == expected.slot
                            && block.transactions.len() as u64 == expected.transactions,
                        "CAR shard block differs from canonical plan at slot {slot}"
                    );
                }
                if slot >= end_slot {
                    ensure!(
                        ranges.is_none(),
                        "CAR shard contains a block beyond its plan"
                    );
                    break;
                }
                work_tx.send(block).context("decode input closed")?;
                physical_blocks += 1;
                if ranges.is_some_and(|ranges| physical_blocks == ranges.len())
                    || (ranges.is_none() && slot + 1 == end_slot)
                {
                    break;
                }
            }
            Ok(())
        })();
        drop(work_tx);
        let mut rows = Vec::new();
        let mut error = input_result.err();
        for handle in handles {
            match handle.join() {
                Ok(Ok(mut batch)) => rows.append(&mut batch),
                Ok(Err(e)) => {
                    error.get_or_insert(e);
                }
                Err(_) => {
                    error.get_or_insert_with(|| anyhow::anyhow!("decode worker panicked"));
                }
            }
        }
        if let Some(e) = error {
            return Err(e);
        }
        rows.sort_unstable_by_key(|row| row.slot);
        ensure!(
            rows.windows(2).all(|r| r[0].slot < r[1].slot),
            "duplicate or unordered blocks"
        );
        Ok(rows)
    })
}

fn main() -> Result<()> {
    let args = Args::parse();
    validate_decode_workers(args.workers)?;
    if args.scheduling == Scheduling::Dynamic {
        sharded::validate_parser_count(args.parser_workers)?;
    } else {
        sharded::validate_worker_budget(args.parser_workers, args.workers)?;
    }
    args.validate_scheduling()?;
    let http_settings = args.http_settings()?;
    let http_body_window_bytes = http_settings.body_window_bytes;
    ensure!(
        !args.allocations || !cfg!(feature = "reference-mimalloc"),
        "allocation counters require the System build"
    );
    ensure!(
        args.export.is_none() || matches!(args.metadata_mode, MetadataMode::Owned),
        "export requires owned metadata"
    );
    ensure!(
        (1..=262144).contains(&args.export_memory_mib),
        "export memory limit must be 1..=262144 MiB"
    );
    ensure!(
        args.export_mode == "disk" || args.export.is_some(),
        "memory export requires --export"
    );
    let plan: Value = serde_json::from_reader(File::open(&args.plan)?)?;
    let epoch = plan["epoch"].as_u64().context("plan epoch")?;
    let end = plan["end_slot_exclusive"].as_u64().context("plan end")?;
    ensure!(
        plan["start_slot"].as_u64() == epoch.checked_mul(432000),
        "only epoch prefixes are supported"
    );
    let expected: Vec<[u64; 2]> = serde_json::from_value(plan["block_transaction_rows"].clone())?;
    ensure!(
        !expected.is_empty() && expected.windows(2).all(|r| r[0][0] < r[1][0]),
        "invalid plan order"
    );
    ensure!(
        expected.last().unwrap()[0].checked_add(1) == Some(end),
        "plan end differs"
    );
    let mut output = File::options()
        .write(true)
        .create_new(true)
        .open(&args.output)?;
    let started = Instant::now();
    let export = args
        .export
        .as_deref()
        .map(|path| {
            if args.export_mode == "memory" {
                common_export::Export::create_memory(path, args.export_memory_mib << 20)
            } else {
                common_export::Export::create(path)
            }
        })
        .transpose()?;
    let mut transport = None;
    let mut identity = json!(null);
    let mut parallel_input = None;
    let mut resident_input = None;
    let input: Option<Box<dyn Read>> = if args.resident_cache_manifest.is_some() {
        let prepared = resident_run::Prepared::open(&args, epoch, &expected)?;
        identity = prepared.identity();
        resident_input = Some(prepared);
        None
    } else if args.indexed() {
        let prepared = sharded::Prepared::open(
            args.url.as_deref().unwrap(),
            epoch,
            &expected,
            args.parser_workers,
            args.workers,
            http_settings.chunk_bytes,
            !args.legacy_http_buffers,
            args.scheduling,
            args.http_clients,
            (args.job_mib as u64) << 20,
            args.allow_loopback_http,
        )?;
        identity = prepared.identity();
        parallel_input = Some(prepared);
        None
    } else if let Some(url) = &args.url {
        let session = CarHttpSession::new(
            http_settings
                .ordered
                .context("missing ordered HTTP options")?,
        )?;
        if args.operator_trusted {
            let stream = session.open_operator_trusted(url)?;
            identity = json!({"url": stream.identity().normalized_url, "bytes": stream.identity().content_length, "operator_trusted": true});
            transport = Some(stream.stats_handle());
            Some(Box::new(stream))
        } else {
            let stream = session.open(url)?;
            identity = json!({"url": stream.identity().normalized_url, "bytes": stream.identity().content_length, "etag": stream.identity().strong_etag, "operator_trusted": false});
            transport = Some(stream.stats_handle());
            Some(Box::new(stream))
        }
    } else {
        Some(Box::new(File::open(args.file.as_ref().unwrap())?))
    };
    let setup_s = started.elapsed().as_secs_f64();
    let stage_metrics = (args.stage_sample_every != 0)
        .then(|| decode_metrics::Metrics::new(args.stage_sample_every).map(std::sync::Arc::new))
        .transpose()?;
    let gate = args
        .profile_control
        .as_deref()
        .map(profile_control::Gate::prepare)
        .transpose()?;
    let scan_usage_before = profile_control::Usage::now()?;
    let scan_start = Instant::now();
    if args.allocations {
        allocation::start();
    }
    let result = if let Some(prepared) = &resident_input {
        prepared.scan(args.metadata_mode, export.as_ref(), stage_metrics.clone())
    } else if let Some(prepared) = &parallel_input {
        prepared.scan(args.metadata_mode, export.as_ref())
    } else {
        scan(
            input.unwrap(),
            end,
            args.workers,
            args.metadata_mode,
            export.as_ref(),
        )
    };
    let allocations = args.allocations.then(allocation::stop);
    let scan_s = scan_start.elapsed().as_secs_f64();
    let scan_usage_after = profile_control::Usage::now()?;
    let scan_resources = scan_usage_after.since(&scan_usage_before, scan_s);
    if let Some(gate) = &gate {
        gate.complete(scan_s, result.is_ok())?;
    }
    let resident_receipt = resident_input
        .as_ref()
        .map(resident_run::Prepared::finish_receipt)
        .transpose()?;
    let rows = match result {
        Ok(rows) => rows,
        Err(error) => {
            let http = http_receipt(transport.as_ref(), parallel_input.as_ref(), false);
            serde_json::to_writer_pretty(
                &mut output,
                &json!({"schema":"blockzilla-car-decode-reference-v1", "valid":false, "failure_stage":"scan",
                    "error":format!("{error:#}"), "epoch":epoch, "identity":identity, "http":http,
                    "setup_seconds":setup_s, "scan_seconds":scan_s, "total_seconds":started.elapsed().as_secs_f64(),
                    "workers":args.workers, "parser_workers":args.parser_workers,
                    "scheduling":args.scheduling.name(), "http_clients":args.http_clients, "job_target_bytes":(args.job_mib as u64) << 20,
                    "allow_loopback_http":args.allow_loopback_http,
                    "job_count":parallel_input.as_ref().map(sharded::Prepared::job_count),
                    "pipeline":parallel_input.as_ref().map(sharded::Prepared::pipeline_receipt),
                    "http_transport_mode":args.transport_mode(),
                    "effective_http_workers":if args.indexed() {args.parser_workers} else {args.http_workers},
                    "http_chunk_bytes":http_settings.chunk_bytes, "http_body_window_bytes":http_body_window_bytes,
                    "shards":parallel_input.as_ref().map(sharded::Prepared::shard_receipts)}),
            )?;
            return Err(error);
        }
    };
    let by_slot: BTreeMap<_, _> = rows.iter().map(|r| (r.slot, r)).collect();
    let expected_by_slot: BTreeMap<_, _> = expected.iter().map(|r| (r[0], r[1])).collect();
    let valid = rows
        .iter()
        .all(|r| expected_by_slot.get(&r.slot) == Some(&r.tx))
        && expected.iter().all(|r| {
            by_slot
                .get(&r[0])
                .map_or(r[1] == 0, |actual| actual.tx == r[1])
        });
    ensure!(valid, "decoded rows do not match the canonical plan");
    let export_finish_started = Instant::now();
    let export_bytes = export.as_ref().map(|e| e.finish(&expected)).transpose()?;
    let export_finish_seconds = export_finish_started.elapsed().as_secs_f64();
    let total_seconds = started.elapsed().as_secs_f64();
    let export_storage = export.as_ref().map(|e| e.stats()).transpose()?.map(|s| {
        json!({
            "mode":s.mode, "memory_limit_bytes":s.memory_limit_bytes,
            "peak_memory_bytes":s.peak_memory_bytes, "spool_bytes":s.spool_bytes,
        })
    });
    let tx = rows.iter().map(|r| r.tx).sum::<u64>();
    let http = http_receipt(transport.as_ref(), parallel_input.as_ref(), true);
    let allocation_counts = allocations.map(|s| json!({
        "calls": s.allocation_calls, "requested_bytes": s.allocation_bytes,
        "buckets": s.size_buckets.iter().zip(allocation::BUCKET_UPPER_BOUNDS).map(|(b, upper)| json!({"max_bytes": upper, "calls": b.allocation_calls, "requested_bytes": b.allocation_bytes})).collect::<Vec<_>>(),
        "timing_is_instrumented": true, "includes_c_zstd_allocations": false,
    }));
    let mut receipt = json!({"schema":"blockzilla-car-decode-reference-v1", "valid":valid, "epoch":epoch,
        "allocator": if cfg!(feature="reference-mimalloc") {"mimalloc"} else {"system"},
        "export_schema":args.export.as_ref().map(|_| common_export::SCHEMA), "export_bytes":export_bytes, "total_seconds":total_seconds,
        "export_storage":export_storage, "export_finish_seconds":export_finish_seconds,
        "scheduling":args.scheduling.name(), "http_clients":args.http_clients, "job_target_bytes":(args.job_mib as u64) << 20,
        "allow_loopback_http":args.allow_loopback_http,
        "job_count":parallel_input.as_ref().map(sharded::Prepared::job_count), "pipeline":parallel_input.as_ref().map(sharded::Prepared::pipeline_receipt),
        "parser_workers":args.parser_workers, "effective_http_workers":if args.indexed() { args.parser_workers } else { args.http_workers }, "http_transport_mode":args.transport_mode(),
        "shards":parallel_input.as_ref().map(sharded::Prepared::shard_receipts),
        "workers":args.workers, "metadata_mode":format!("{:?}",args.metadata_mode), "http_workers":args.http_workers,"reuse_http_buffers":!args.legacy_http_buffers, "http_window_chunks":args.http_window_chunks, "http_chunk_bytes":http_settings.chunk_bytes, "http_body_window_bytes":http_body_window_bytes, "allocations":allocation_counts, "setup_seconds":setup_s, "scan_seconds":scan_s,
        "transactions":tx, "blocks":expected.len(), "physical_blocks":rows.len(),
        "scan_tps": if valid {Some(tx as f64 / scan_s)} else {None},
        "votes":rows.iter().map(|r|r.votes).sum::<u64>(),"failed":rows.iter().map(|r|r.failed).sum::<u64>(),
        "identity":identity,"http":http,"signature_verification":false,
        "block_transaction_rows":expected,"block_decode_digests":rows.iter().map(|r|json!([r.slot,r.digest])).collect::<Vec<_>>()});
    receipt["scan_resources"] = scan_resources;
    receipt["decode_stages"] = stage_metrics.as_ref().map_or(Value::Null, |m| m.receipt());
    receipt["resident_input"] = resident_receipt.unwrap_or(Value::Null);
    if let Some(prepared) = &resident_input {
        receipt["pipeline"] = prepared.pipeline_receipt();
        receipt["job_count"] = receipt["pipeline"]["job_count"].clone();
        receipt["effective_http_workers"] = json!(0);
        receipt["http_body_window_bytes"] = json!(0);
        receipt["workload"] = json!(if export.is_some() {
            "full-owned-decode-hashes-and-export"
        } else {
            "full-owned-decode-message-hashes-and-receipt-digests-no-export"
        });
        receipt["resident_timing_scope"] = json!(
            "scan_seconds includes memory range copies, CAR parsing, full transaction/metadata/rewards decode, hashes and worker startup/join; excludes mlock/preload, residency walks and profiler handshake"
        );
    }
    serde_json::to_writer_pretty(&mut output, &receipt)?;
    writeln!(output)?;
    ensure!(valid, "decoded rows do not match the canonical plan");
    println!("{tx} transactions in {scan_s:.3} s; exact block counts passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update_receipt_digest_reference(
        digest: &mut blake3::Hasher,
        slot: u64,
        index: u64,
        signature: &[u8; 64],
        message_hash: &[u8; 32],
        vote: bool,
        failed: bool,
    ) {
        digest.update(&slot.to_le_bytes());
        digest.update(&index.to_le_bytes());
        digest.update(signature);
        digest.update(message_hash);
        digest.update(&[u8::from(vote), u8::from(failed)]);
    }

    #[test]
    fn receipt_digest_batch_matches_five_updates_at_blake3_boundaries() {
        let signature = std::array::from_fn(|i| (i as u8).wrapping_mul(17));
        let message_hash = std::array::from_fn(|i| (i as u8).wrapping_mul(29));
        // Place record fields around BLAKE3's 64-byte blocks and 1024-byte chunks.
        for prefix_len in [0, 1, 63, 64, 65, 909, 910, 911, 1023, 1024, 1025] {
            let prefix = vec![0xa7; prefix_len];
            for (slot, index) in [
                (0, 0),
                (0x0123456789abcdef, 0xfedcba9876543210),
                (u64::MAX, u64::MAX),
            ] {
                for (vote, failed) in [(false, false), (false, true), (true, false), (true, true)] {
                    let mut reference = blake3::Hasher::new();
                    reference.update(&prefix);
                    let mut batched = reference.clone();
                    update_receipt_digest_reference(
                        &mut reference,
                        slot,
                        index,
                        &signature,
                        &message_hash,
                        vote,
                        failed,
                    );
                    update_receipt_digest(
                        &mut batched,
                        slot,
                        index,
                        &signature,
                        &message_hash,
                        vote,
                        failed,
                    );
                    assert_eq!(
                        reference.finalize(),
                        batched.finalize(),
                        "prefix length {prefix_len}"
                    );
                }
            }
        }
    }

    #[test]
    fn receipt_digest_batch_matches_empty_and_long_block_streams() {
        let mut reference = blake3::Hasher::new();
        let mut batched = blake3::Hasher::new();
        assert_eq!(reference.finalize(), batched.finalize());
        for index in 0..1025u64 {
            let slot = u64::MAX - index;
            let signature =
                std::array::from_fn(|i| (index as u8).wrapping_add((i as u8).wrapping_mul(17)));
            let message_hash = std::array::from_fn(|i| {
                (index.rotate_right(8) as u8).wrapping_add((i as u8).wrapping_mul(29))
            });
            let vote = index & 1 != 0;
            let failed = index & 2 != 0;
            update_receipt_digest_reference(
                &mut reference,
                slot,
                index,
                &signature,
                &message_hash,
                vote,
                failed,
            );
            update_receipt_digest(
                &mut batched,
                slot,
                index,
                &signature,
                &message_hash,
                vote,
                failed,
            );
            // Check every prefix, including repeated chunk and tree boundaries.
            assert_eq!(
                reference.finalize(),
                batched.finalize(),
                "transaction {index}"
            );
        }
    }

    #[test]
    fn real_car_decode_and_export_are_identical_across_worker_counts() {
        for bytes in [include_bytes!("../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-157-biggest.car").as_slice(),
            include_bytes!("../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-822-biggest.car").as_slice()] {
            let one = scan(bytes, u64::MAX, 1, MetadataMode::Owned, None).unwrap();
            assert!(!one.is_empty());
            assert_eq!(one, scan(bytes, u64::MAX, 12, MetadataMode::Visitor, None).unwrap());
            let plan: Vec<_> = one.iter().map(|r| [r.slot, r.tx]).collect();
            let mut reference = None;
            for (workers, memory) in [1, 12, 64].into_iter().flat_map(|w| [(w, false), (w, true)]) {
                let path = std::env::temp_dir().join(format!("car-real-export-{}-{}-{workers}-{memory}.bin", std::process::id(), one[0].slot));
                let export = if memory {
                    common_export::Export::create_memory(&path, 64 << 20).unwrap()
                } else {
                    common_export::Export::create(&path).unwrap()
                };
                assert_eq!(one, scan(bytes, u64::MAX, workers, MetadataMode::Owned, Some(&export)).unwrap());
                export.finish(&plan).unwrap();
                let output = std::fs::read(&path).unwrap();
                if let Some(previous) = &reference { assert_eq!(&output, previous); }
                reference = Some(output);
                std::fs::remove_file(path).unwrap();
            }
        }
    }

    #[test]
    fn decode_worker_count_is_bounded_before_input_is_read() {
        for workers in [0, MAX_DECODE_WORKERS + 1, usize::MAX] {
            let error = scan(&[][..], u64::MAX, workers, MetadataMode::Owned, None).unwrap_err();
            assert!(error.to_string().contains("workers must be"));
        }
        for workers in [1, 12, 64, 128, MAX_DECODE_WORKERS] {
            validate_decode_workers(workers).unwrap();
        }
    }

    #[test]
    fn cli_http_controls_keep_defaults_and_enforce_transport_bounds() {
        let parse = |extra: &[&str]| {
            Args::try_parse_from(
                [
                    "car-decode-reference",
                    "--file",
                    "unused.car",
                    "--plan",
                    "unused.json",
                    "--output",
                    "unused-output.json",
                ]
                .into_iter()
                .chain(extra.iter().copied()),
            )
            .unwrap()
        };
        let defaults = parse(&[]).http_settings().unwrap().ordered.unwrap();
        assert_eq!(defaults.workers, 4);
        assert_eq!(defaults.window_chunks, 8);
        assert_eq!(defaults.chunk_bytes, 32 << 20);
        assert_eq!(defaults.body_window_bytes().unwrap(), 256 << 20);

        let wide = parse(&[
            "--http-workers",
            "16",
            "--http-window-chunks",
            "16",
            "--http-chunk-mib",
            "8",
        ])
        .http_settings()
        .unwrap()
        .ordered
        .unwrap();
        assert_eq!(wide.workers, 16);
        assert_eq!(wide.body_window_bytes().unwrap(), 128 << 20);

        for extra in [
            vec!["--http-workers", "0"],
            vec!["--http-workers", "17", "--http-window-chunks", "16"],
            vec!["--http-workers", "16"],
            vec!["--http-window-chunks", "0"],
            vec!["--http-window-chunks", "17"],
            vec!["--http-chunk-mib", "0"],
            vec!["--http-chunk-mib", "33"],
            vec!["--http-chunk-mib", "128"],
        ] {
            assert!(parse(&extra).http_settings().is_err(), "accepted {extra:?}");
        }
        let mut overflow = parse(&[]);
        overflow.http_chunk_mib = usize::MAX;
        assert!(overflow.http_settings().is_err());
    }

    #[test]
    fn indexed_chunks_have_separate_bounds_and_never_create_sdk_options() {
        for (scheduling, parsers) in [("static", "2"), ("dynamic", "1")] {
            let mut args = Args::try_parse_from([
                "car-decode-reference",
                "--url",
                "https://example.test/1000/epoch-1000.car",
                "--operator-trusted",
                "--plan",
                "unused.json",
                "--output",
                "unused-output.json",
                "--scheduling",
                scheduling,
                "--parser-workers",
                parsers,
            ])
            .unwrap();
            args.validate_scheduling().unwrap();
            let defaults = args.http_settings().unwrap();
            assert_eq!(defaults.chunk_bytes, 32 << 20);
            assert!(defaults.ordered.is_none());
            for mib in [1, 32, 33, 64, 128] {
                args.http_chunk_mib = mib;
                let settings = args.http_settings().unwrap();
                assert_eq!(settings.chunk_bytes, mib << 20);
                assert_eq!(
                    settings.body_window_bytes,
                    args.parser_workers * (mib << 20)
                );
                assert!(settings.ordered.is_none());
            }
            for mib in [0, 129, usize::MAX] {
                args.http_chunk_mib = mib;
                assert!(
                    args.http_settings().is_err(),
                    "accepted indexed chunk MiB={mib}"
                );
            }
            args.http_chunk_mib = 128;
            args.http_workers = 17;
            assert!(args.http_settings().is_err());
            args.http_workers = 16;
            assert!(args.http_settings().is_err()); // Default window is still eight.
            args.http_window_chunks = 16;
            assert!(args.http_settings().is_ok());
        }
    }

    #[test]
    fn loopback_flag_cannot_enable_the_ordered_sdk_transport() {
        let mut args = Args::try_parse_from([
            "car-decode-reference",
            "--url",
            "http://127.0.0.1:8123/1000/epoch-1000.car",
            "--operator-trusted",
            "--allow-loopback-http",
            "--plan",
            "unused.json",
            "--output",
            "unused-output.json",
        ])
        .unwrap();
        assert!(args.validate_scheduling().is_err());
        args.scheduling = Scheduling::Dynamic;
        args.validate_scheduling().unwrap();
        assert!(args.http_settings().unwrap().ordered.is_none());
        args.scheduling = Scheduling::Static;
        args.parser_workers = 2;
        args.validate_scheduling().unwrap();
        assert!(args.http_settings().unwrap().ordered.is_none());
    }
}
