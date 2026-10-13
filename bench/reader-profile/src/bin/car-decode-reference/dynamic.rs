//! Fixed worker pools with dynamic block-boundary jobs and bounded raw storage.
use super::{
    Decoder, MetadataMode, Row, common_export,
    sharded::{BlockRange, RangeRead},
};
use anyhow::{Context, Result, anyhow, ensure};
use blockzilla_source::RangeSource;
use crossbeam_channel::{Receiver, Sender, bounded, select_biased};
use of_car_reader::{CarBlockReader, LosslessBlockReadLimits, OrderedLosslessCarBlock};
use serde_json::{Value, json};
use std::{
    io::Write,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::Instant,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct Config {
    pub parsers: usize,
    pub decoders: usize,
    pub job_bytes: u64,
    pub chunk_bytes: usize,
    pub reuse: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Job {
    pub first: usize,
    pub end: usize,
}

pub(super) fn plan_jobs(ranges: &[BlockRange], target: u64) -> Result<Vec<Job>> {
    ensure!(
        target > 0 && !ranges.is_empty(),
        "dynamic jobs require a positive target and nonempty ranges"
    );
    ensure!(
        ranges.iter().all(|r| r.start < r.end),
        "dynamic range is empty or reversed"
    );
    ensure!(
        ranges
            .windows(2)
            .all(|r| r[0].slot < r[1].slot && r[0].end == r[1].start),
        "dynamic ranges must have unique increasing slots and contiguous bytes"
    );
    let mut jobs = Vec::new();
    let mut first = 0;
    for end in 1..=ranges.len() {
        if ranges[end - 1].end - ranges[first].start >= target || end == ranges.len() {
            jobs.push(Job { first, end });
            first = end;
        }
    }
    Ok(jobs)
}

/// Dropping the sole sender wakes every select waiting on a receiver clone.
struct Cancel {
    sender: Mutex<Option<Sender<()>>>,
    receiver: Receiver<()>,
    error: Mutex<Option<anyhow::Error>>,
    cancelled: AtomicBool,
}
impl Cancel {
    fn new() -> Self {
        let (sender, receiver) = bounded(0);
        Self {
            sender: Mutex::new(Some(sender)),
            receiver,
            error: Mutex::new(None),
            cancelled: AtomicBool::new(false),
        }
    }
    fn receiver(&self) -> &Receiver<()> {
        &self.receiver
    }
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
    fn fail(&self, error: anyhow::Error) {
        self.error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert(error);
        self.cancelled.store(true, Ordering::Release);
        self.sender.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
    fn take_error(&self) -> Option<anyhow::Error> {
        self.error.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

// The scope body can unwind while it creates workers. Wake existing workers
// before thread::scope waits for them; otherwise a queue wait could never end.
struct CancelOnDrop<'a> {
    cancel: &'a Cancel,
    armed: bool,
}
impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.cancel
                .fail(anyhow!("dynamic worker setup or coordinator panicked"));
        }
    }
}

fn send_or_cancel<T>(
    sender: &Sender<T>,
    item: T,
    cancel: &Cancel,
) -> std::result::Result<bool, crossbeam_channel::SendError<T>> {
    select_biased! {
        recv(cancel.receiver()) -> _ => Ok(false),
        send(sender, item) -> result => result.map(|()| true),
    }
}

#[derive(Default)]
struct ParserStats {
    id: usize,
    jobs_claimed: usize,
    jobs_completed: usize,
    blocks: usize,
    wait_seconds: f64,
    work_seconds: f64,
    http_seconds: f64,
    max_job_seconds: f64,
    finished_seconds: f64,
    cpu_seconds: f64,
}
#[derive(Default)]
struct DecoderStats {
    id: usize,
    blocks: usize,
    wait_seconds: f64,
    work_seconds: f64,
    finished_seconds: f64,
    cpu_seconds: f64,
}
#[derive(Default)]
struct Snapshot {
    raw_block_pool_size: usize,
    job_count: usize,
    min_job_bytes: u64,
    max_job_bytes: u64,
    parsers: Vec<ParserStats>,
    decoders: Vec<DecoderStats>,
}
#[derive(Default)]
pub(super) struct Metrics {
    snapshot: Mutex<Snapshot>,
    max_ready: AtomicUsize,
}
impl Metrics {
    pub(super) fn receipt(&self) -> Value {
        let s = self.snapshot.lock().unwrap_or_else(|e| e.into_inner());
        json!({
            "raw_block_pool_size":s.raw_block_pool_size, "work_queue_capacity":s.raw_block_pool_size,
            "job_count":s.job_count, "min_job_bytes":s.min_job_bytes, "max_job_bytes":s.max_job_bytes,
            "jobs_claimed":s.parsers.iter().map(|p| p.jobs_claimed).sum::<usize>(),
            "jobs_completed":s.parsers.iter().map(|p| p.jobs_completed).sum::<usize>(),
            "blocks_parsed":s.parsers.iter().map(|p| p.blocks).sum::<usize>(),
            "blocks_decoded":s.decoders.iter().map(|d| d.blocks).sum::<usize>(),
            "max_ready_queue_depth":self.max_ready.load(Ordering::Relaxed),
            "queue_depth_observation":"maximum observed immediately after enqueue, not a continuous peak",
            "parser_wait_seconds":s.parsers.iter().map(|p| p.wait_seconds).sum::<f64>(),
            "parser_work_seconds":s.parsers.iter().map(|p| p.work_seconds).sum::<f64>(),
            "http_read_seconds":s.parsers.iter().map(|p| p.http_seconds).sum::<f64>(),
            "decoder_wait_seconds":s.decoders.iter().map(|d| d.wait_seconds).sum::<f64>(),
            "decoder_work_seconds":s.decoders.iter().map(|d| d.work_seconds).sum::<f64>(),
            "parser_cpu_seconds":s.parsers.iter().map(|p| p.cpu_seconds).sum::<f64>(),
            "decoder_cpu_seconds":s.decoders.iter().map(|d| d.cpu_seconds).sum::<f64>(),
            "timing_scope":"work, wait and input durations sum worker wall times; parser work includes input reads, decoder work includes optional export; these wall durations overlap",
            "cpu_timing_scope":"cpu_seconds fields use CLOCK_THREAD_CPUTIME_ID per worker; parser_cpu_seconds and decoder_cpu_seconds sum those worker CPU times",
            "jobs_completed_scope":"jobs fully parsed and queued; only a successful scan proves complete decoding",
            "parser_workers_finished":s.parsers.len(), "decode_workers_finished":s.decoders.len(),
            "parser_workers":s.parsers.iter().map(|p| json!({"worker":p.id,"jobs_claimed":p.jobs_claimed,
                "jobs_completed":p.jobs_completed,"blocks":p.blocks,"queue_wait_seconds":p.wait_seconds,
                "work_seconds":p.work_seconds,"http_read_seconds":p.http_seconds,
                "max_job_seconds":p.max_job_seconds,"finished_seconds":p.finished_seconds,"cpu_seconds":p.cpu_seconds})).collect::<Vec<_>>(),
            "decode_workers":s.decoders.iter().map(|d| json!({"worker":d.id,"blocks":d.blocks,
                "queue_wait_seconds":d.wait_seconds,"work_seconds":d.work_seconds,
                "finished_seconds":d.finished_seconds,"cpu_seconds":d.cpu_seconds})).collect::<Vec<_>>(),
        })
    }
}

fn caught(result: std::thread::Result<Result<()>>, context: &str, cancel: &Cancel) {
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error.context(context.to_owned()),
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            anyhow!("{context} panicked: {message}")
        }
    };
    let message = format!("[car-dynamic-error] {error:#}");
    cancel.fail(error);
    // A broken/full stderr must not prevent the cancellation broadcast.
    let _ = writeln!(std::io::stderr().lock(), "{message}");
}

pub(super) fn scan_dynamic<S: RangeSource>(
    sources: &[S],
    object: &str,
    ranges: &[BlockRange],
    config: Config,
    mode: MetadataMode,
    export: Option<&common_export::Export>,
    retries: &AtomicU64,
    metrics: &Metrics,
) -> Result<Vec<Row>> {
    scan_dynamic_profiled(
        sources, object, ranges, config, mode, export, retries, metrics, None,
    )
}

pub(super) fn scan_dynamic_profiled<S: RangeSource>(
    sources: &[S],
    object: &str,
    ranges: &[BlockRange],
    config: Config,
    mode: MetadataMode,
    export: Option<&common_export::Export>,
    retries: &AtomicU64,
    metrics: &Metrics,
    stages: Option<std::sync::Arc<super::decode_metrics::Metrics>>,
) -> Result<Vec<Row>> {
    super::validate_decode_workers(config.decoders)?;
    super::sharded::validate_parser_count(config.parsers)?;
    ensure!(
        !sources.is_empty() && sources.len() <= config.parsers,
        "invalid dynamic HTTP client count"
    );
    super::sharded::validate_chunk_bytes(config.chunk_bytes)?;
    let jobs = plan_jobs(ranges, config.job_bytes)?;
    let pool_size = config.decoders + 2 * config.parsers;
    {
        let mut s = metrics.snapshot.lock().unwrap();
        ensure!(
            s.raw_block_pool_size == 0,
            "dynamic metrics cannot be reused for another scan"
        );
        s.raw_block_pool_size = pool_size;
        s.job_count = jobs.len();
        s.min_job_bytes = jobs
            .iter()
            .map(|j| ranges[j.end - 1].end - ranges[j.first].start)
            .min()
            .unwrap();
        s.max_job_bytes = jobs
            .iter()
            .map(|j| ranges[j.end - 1].end - ranges[j.first].start)
            .max()
            .unwrap();
    }
    let (free_tx, free_rx) = bounded(pool_size);
    let (work_tx, work_rx) = bounded(pool_size);
    for _ in 0..pool_size {
        free_tx.send(OrderedLosslessCarBlock::default()).unwrap();
    }
    let next_job = AtomicUsize::new(0);
    let cancel = Cancel::new();
    let started = Instant::now();
    let joined = catch_unwind(AssertUnwindSafe(|| {
        thread::scope(|scope| {
            let mut guard = CancelOnDrop {
                cancel: &cancel,
                armed: true,
            };
            let mut decoders = Vec::with_capacity(config.decoders);
            for id in 0..config.decoders {
                let free = free_tx.clone();
                let work = work_rx.clone();
                let cancel = &cancel;
                let stages = stages.clone();
                decoders.push(scope.spawn(move || {
                    let cpu_start = super::profile_control::thread_cpu_seconds();
                    let mut stats = DecoderStats {
                        id,
                        ..DecoderStats::default()
                    };
                    let mut rows = Vec::new();
                    let result = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
                        let mut decoder = Decoder::with_metrics(stages);
                        loop {
                            let waiting = Instant::now();
                            let block = select_biased! {
                                recv(cancel.receiver()) -> _ => return Ok(()),
                                recv(work) -> block => block,
                            };
                            stats.wait_seconds += waiting.elapsed().as_secs_f64();
                            let Ok(mut block) = block else {
                                return Ok(());
                            };
                            if cancel.is_cancelled() {
                                return Ok(());
                            }
                            let working = Instant::now();
                            let decoded = decoder.decode(&block, mode, export);
                            stats.work_seconds += working.elapsed().as_secs_f64();
                            let row = decoded?;
                            stats.blocks += 1;
                            rows.push(row);
                            block.clear();
                            if block.data_buffer_pool_stats().retained_capacity > 16 << 20 {
                                block.release_reusable_data_buffers();
                            }
                            // With no parsers left, a closed free pool is normal; drain all queued work.
                            if matches!(send_or_cancel(&free, block, cancel), Ok(false)) {
                                return Ok(());
                            }
                        }
                    }));
                    caught(result, &format!("CAR dynamic decoder {id}"), cancel);
                    stats.finished_seconds = started.elapsed().as_secs_f64();
                    stats.cpu_seconds = super::profile_control::thread_cpu_seconds() - cpu_start;
                    metrics
                        .snapshot
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .decoders
                        .push(stats);
                    rows
                }));
            }
            let mut parsers = Vec::with_capacity(config.parsers);
            for id in 0..config.parsers {
                let free = free_rx.clone();
                let work = work_tx.clone();
                let cancel = &cancel;
                let jobs = &jobs;
                let next_job = &next_job;
                parsers.push(scope.spawn(move || {
                let cpu_start = super::profile_control::thread_cpu_seconds();
                let mut stats = ParserStats { id, ..ParserStats::default() };
                let source = &sources[id % sources.len()];
                let result = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
                    let input = RangeRead::new(source, object, ranges[0].start, ranges.last().unwrap().end,
                        config.chunk_bytes, config.reuse, retries)?.with_cancel(&cancel.cancelled);
                    let mut reader = CarBlockReader::with_capacity(input, 8 << 20);
                    let limits = LosslessBlockReadLimits { max_entry_payload_bytes:16 << 20,
                        max_block_payload_bytes:16 << 20, max_entries_per_block:65536, max_transactions_per_block:65535 };
                    loop {
                        if cancel.is_cancelled() { return Ok(()); }
                        let job_id = next_job.fetch_add(1, Ordering::Relaxed);
                        let Some(job) = jobs.get(job_id) else { return Ok(()); };
                        stats.jobs_claimed += 1;
                        let first = &ranges[job.first];
                        let last = &ranges[job.end - 1];
                        let job_started = Instant::now();
                        let before_http = reader.reader.get_ref().read_seconds();
                        let result = (|| -> Result<()> {
                            ensure!(reader.reader.buffer().is_empty(), "unconsumed parser bytes between jobs");
                            reader.reader.get_mut().reset_span(first.start, last.end)?;
                            reader.offset = first.start;
                            reader.entry_index = 0;
                            for expected in &ranges[job.first..job.end] {
                                let waiting = Instant::now();
                                let block = select_biased! {
                                    recv(cancel.receiver()) -> _ => return Ok(()),
                                    recv(free) -> block => block.context("dynamic raw-block pool closed")?,
                                };
                                stats.wait_seconds += waiting.elapsed().as_secs_f64();
                                let mut block = block;
                                if cancel.is_cancelled() { return Ok(()); }
                                let working = Instant::now();
                                let block_start = reader.offset;
                                let read = reader.read_until_block_ordered_lossless_bounded(&mut block, limits);
                                stats.work_seconds += working.elapsed().as_secs_f64();
                                ensure!(read?, "dynamic job ended before its indexed block");
                                let slot = block.block.as_ref().context("missing dynamic terminal block")?.slot;
                                ensure!(block_start == expected.start && reader.offset == expected.end,
                                    "CAR block byte range differs from index at slot {slot}");
                                ensure!(slot == expected.slot && block.transactions.len() as u64 == expected.transactions,
                                    "CAR dynamic block differs from canonical plan at slot {slot}");
                                stats.blocks += 1;
                                if !send_or_cancel(&work, block, cancel).map_err(|_| anyhow!("dynamic decode queue closed"))? {
                                    return Ok(());
                                }
                                metrics.max_ready.fetch_max(work.len(), Ordering::Relaxed);
                            }
                            ensure!(reader.offset == last.end && reader.reader.buffer().is_empty()
                                && reader.reader.get_ref().span_complete(), "dynamic job has unconsumed bytes");
                            Ok(())
                        })();
                        stats.http_seconds += reader.reader.get_ref().read_seconds() - before_http;
                        result.with_context(|| format!("job={job_id} slots={}..{} bytes={}..{}", first.slot,last.slot+1,first.start,last.end))?;
                        if cancel.is_cancelled() { return Ok(()); }
                        stats.jobs_completed += 1;
                        stats.max_job_seconds = stats.max_job_seconds.max(job_started.elapsed().as_secs_f64());
                    }
                }));
                caught(result, &format!("CAR dynamic parser {id}"), cancel);
                stats.finished_seconds = started.elapsed().as_secs_f64();
                stats.cpu_seconds = super::profile_control::thread_cpu_seconds() - cpu_start;
                metrics.snapshot.lock().unwrap_or_else(|e| e.into_inner()).parsers.push(stats);
            }));
            }
            // No root sender/receiver may keep a closed stage alive after its workers exit.
            drop(work_tx);
            drop(work_rx);
            drop(free_tx);
            drop(free_rx);
            for parser in parsers {
                if parser.join().is_err() {
                    cancel.fail(anyhow!("dynamic parser panicked outside its handler"));
                }
            }
            let mut rows = Vec::new();
            for decoder in decoders {
                match decoder.join() {
                    Ok(mut batch) => rows.append(&mut batch),
                    Err(_) => cancel.fail(anyhow!("dynamic decoder panicked outside its handler")),
                }
            }
            guard.armed = false;
            rows
        })
    }));
    let mut rows = match joined {
        Ok(rows) => rows,
        Err(_) => {
            cancel.fail(anyhow!("dynamic worker setup or coordinator panicked"));
            Vec::new()
        }
    };
    if let Some(error) = cancel.take_error() {
        return Err(error);
    }
    rows.sort_unstable_by_key(|row| row.slot);
    ensure!(
        rows.len() == ranges.len()
            && rows
                .iter()
                .zip(ranges)
                .all(|(row, range)| row.slot == range.slot && row.tx == range.transactions),
        "dynamic output rows differ from all physical canonical blocks"
    );
    let s = metrics.snapshot.lock().unwrap();
    ensure!(
        s.parsers.iter().map(|p| p.jobs_completed).sum::<usize>() == jobs.len(),
        "dynamic jobs were not all completed"
    );
    Ok(rows)
}

#[cfg(test)]
#[path = "dynamic/tests.rs"]
mod tests;
