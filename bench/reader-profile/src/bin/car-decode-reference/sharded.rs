//! Benchmark-only independent CAR ranges. The default SDK path is unchanged.
use super::{MetadataMode, Row, Scheduling, common_export, dynamic, request_retry, scan_reader};
use anyhow::{Context, Result, ensure};
use blockzilla_source::{RangeSource, SourceError};
use blockzilla_source_http::{
    HttpObjectPathLayout, HttpRangeSource, HttpRangeSourceOptions, HttpRangeSourceStats,
};
use of_car_reader::{
    CarBlockReader,
    slot_ranges::{SLOT_RANGE_ENTRY_SIZE, SLOTS_PER_EPOCH, decode_slot_range_entry},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{self, Cursor, Read},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    thread,
    time::Instant,
};

const MAX_PARSERS: usize = 128;
const MAX_CHUNK_BYTES: usize = 128 << 20;
const INDEX_BYTES: usize = SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE;

pub(super) fn validate_chunk_bytes(chunk_bytes: usize) -> Result<()> {
    ensure!(
        (1..=MAX_CHUNK_BYTES).contains(&chunk_bytes),
        "indexed HTTP chunk bytes must be 1..={MAX_CHUNK_BYTES}"
    );
    Ok(())
}

// Use the literal authority, before URL normalization. This excludes DNS names,
// shorthand IPv4 forms, IPv4-mapped IPv6, credentials, and remote HTTP origins.
fn loopback_http_base(base: &str) -> bool {
    let Some(rest) = base.strip_prefix("http://") else {
        return false;
    };
    if base
        .chars()
        .any(|c| c.is_control() || matches!(c, '\\' | '?' | '#'))
    {
        return false;
    }
    let authority = rest.split('/').next().unwrap_or_default();
    let suffix = authority
        .strip_prefix("127.0.0.1")
        .or_else(|| authority.strip_prefix("[::1]"));
    match suffix {
        Some("") => true,
        Some(suffix) => suffix.strip_prefix(':').is_some_and(|port| {
            !port.is_empty()
                && port.bytes().all(|c| c.is_ascii_digit())
                && port.parse::<u16>().is_ok_and(|value| value != 0)
        }),
        None => false,
    }
}

fn source_base(url: &str, epoch: u64, allow_loopback_http: bool) -> Result<(&str, bool)> {
    let suffix = format!("/{epoch}/epoch-{epoch}.car");
    let base = url
        .strip_suffix(&suffix)
        .context("indexed URL must end in /<epoch>/epoch-<epoch>.car without query or fragment")?;
    let loopback = loopback_http_base(base);
    ensure!(
        base.starts_with("https://") || (allow_loopback_http && loopback),
        "indexed input requires HTTPS or explicit HTTP on literal 127.0.0.1 or ::1"
    );
    Ok((base, loopback))
}

fn read_index(source: &impl RangeSource, object: &str) -> Result<Vec<u8>> {
    let mut index = vec![0; INDEX_BYTES];
    let middle = INDEX_BYTES / 2;
    // Triton can return 200 for a range that covers the complete object.
    // Two partial requests retain the source's strict 206 and identity checks.
    source.read_range_into_slice(object, 0, &mut index[..middle])?;
    source.read_range_into_slice(object, middle as u64, &mut index[middle..])?;
    Ok(index)
}

#[derive(Clone, Debug)]
pub(super) struct BlockRange {
    pub(super) slot: u64,
    pub(super) transactions: u64,
    pub(super) start: u64,
    pub(super) end: u64,
}

#[derive(Debug)]
struct Shard {
    blocks: Vec<BlockRange>,
    workers: usize,
}

pub(super) fn validate_parser_count(parsers: usize) -> Result<()> {
    ensure!(
        (1..=MAX_PARSERS).contains(&parsers),
        "parser-workers must be 1..={MAX_PARSERS}"
    );
    Ok(())
}

pub(super) fn validate_worker_budget(parsers: usize, decoders: usize) -> Result<()> {
    validate_parser_count(parsers)?;
    ensure!(
        parsers <= decoders,
        "parser-workers must not exceed the total decode workers"
    );
    Ok(())
}

pub(super) fn plan_ranges(
    index: &[u8],
    epoch: u64,
    expected: &[[u64; 2]],
    car_bytes: u64,
    header_bytes: u64,
) -> Result<Vec<BlockRange>> {
    ensure!(
        index.len() == INDEX_BYTES,
        "raw slot index must have exactly {INDEX_BYTES} bytes"
    );
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .context("epoch overflow")?;
    let epoch_end = epoch_start
        .checked_add(SLOTS_PER_EPOCH)
        .context("epoch end overflow")?;
    ensure!(
        header_bytes > 0 && header_bytes < car_bytes,
        "invalid CAR header boundary"
    );
    ensure!(
        !expected.is_empty()
            && expected.windows(2).all(|rows| rows[0][0] < rows[1][0])
            && expected
                .iter()
                .all(|row| row[0] >= epoch_start && row[0] < epoch_end),
        "sharded plan must be ordered and contained in the epoch"
    );
    let plan: BTreeMap<_, _> = expected.iter().map(|row| (row[0], row[1])).collect();
    let end_slot = expected.last().unwrap()[0];
    let mut ranges = Vec::new();
    let mut previous_end = header_bytes;
    let mut prefix_end = header_bytes;
    for (number, bytes) in index.chunks_exact(SLOT_RANGE_ENTRY_SIZE).enumerate() {
        let range = decode_slot_range_entry(bytes)?;
        let slot = epoch_start + number as u64;
        if range.len == 0 {
            ensure!(
                range.offset == 0,
                "empty index row has a nonzero offset at slot {slot}"
            );
            ensure!(
                slot > end_slot || plan.get(&slot).is_none_or(|count| *count == 0),
                "nonempty planned block has no physical index range at slot {slot}"
            );
            continue;
        }
        let end = range
            .offset
            .checked_add(range.len as u64)
            .context("slot range overflow")?;
        ensure!(
            range.offset >= previous_end && end <= car_bytes,
            "overlapping or out-of-bounds index range at slot {slot}"
        );
        previous_end = end;
        if slot <= end_slot {
            let transactions = *plan
                .get(&slot)
                .with_context(|| format!("index slot {slot} is absent from the prefix plan"))?;
            ensure!(
                range.offset == prefix_end,
                "CAR prefix index has a byte gap before slot {slot}"
            );
            ranges.push(BlockRange {
                slot,
                transactions,
                start: range.offset,
                end,
            });
            prefix_end = end;
        }
    }
    ensure!(!ranges.is_empty(), "prefix has no physical CAR blocks");
    Ok(ranges)
}

fn plan_shards(
    index: &[u8],
    epoch: u64,
    expected: &[[u64; 2]],
    car_bytes: u64,
    header_bytes: u64,
    parsers: usize,
    decoders: usize,
) -> Result<Vec<Shard>> {
    validate_worker_budget(parsers, decoders)?;
    let ranges = plan_ranges(index, epoch, expected, car_bytes, header_bytes)?;
    ensure!(
        parsers <= ranges.len(),
        "parser-workers exceeds the number of physical prefix blocks"
    );
    let count = ranges.len();
    let mut shards = Vec::with_capacity(parsers);
    for worker in 0..parsers {
        let first = worker * count / parsers;
        let last = (worker + 1) * count / parsers;
        shards.push(Shard {
            blocks: ranges[first..last].to_vec(),
            workers: decoders / parsers + usize::from(worker < decoders % parsers),
        });
    }
    Ok(shards)
}

/// Read one exact, bounded source span using a single reusable range buffer.
pub(super) struct RangeRead<'a, S: RangeSource + ?Sized> {
    source: &'a S,
    object: &'a str,
    next: u64,
    end: u64,
    chunk_bytes: usize,
    reuse: bool,
    body: Vec<u8>,
    position: usize,
    request_send_retries: &'a AtomicU64,
    cancel: Option<&'a AtomicBool>,
    read_seconds: f64,
}

impl<'a, S: RangeSource + ?Sized> RangeRead<'a, S> {
    pub(super) fn new(
        source: &'a S,
        object: &'a str,
        start: u64,
        end: u64,
        chunk_bytes: usize,
        reuse: bool,
        request_send_retries: &'a AtomicU64,
    ) -> Result<Self> {
        validate_chunk_bytes(chunk_bytes)?;
        ensure!(start < end, "invalid bounded CAR input range");
        Ok(Self {
            source,
            object,
            next: start,
            end,
            chunk_bytes,
            reuse,
            body: Vec::new(),
            position: 0,
            request_send_retries,
            cancel: None,
            read_seconds: 0.0,
        })
    }

    pub(super) fn with_cancel(mut self, cancel: &'a AtomicBool) -> Self {
        self.cancel = Some(cancel);
        self
    }
    pub(super) fn reset_span(&mut self, start: u64, end: u64) -> Result<()> {
        ensure!(
            start < end && self.position == self.body.len(),
            "invalid or unconsumed dynamic range reset"
        );
        self.next = start;
        self.end = end;
        self.body.clear();
        self.position = 0;
        Ok(())
    }
    pub(super) fn span_complete(&self) -> bool {
        self.next == self.end && self.position == self.body.len()
    }
    pub(super) fn read_seconds(&self) -> f64 {
        self.read_seconds
    }
}

impl<S: RangeSource + ?Sized> Read for RangeRead<'_, S> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.position == self.body.len() {
            if self
                .cancel
                .is_some_and(|cancel| cancel.load(Ordering::Acquire))
            {
                return Err(io::Error::other("dynamic CAR scan cancelled"));
            }
            if self.next == self.end {
                return Ok(0);
            }
            let length = (self.end - self.next).min(self.chunk_bytes as u64) as usize;
            if !self.reuse {
                self.body = Vec::new();
            }
            let requested = Instant::now();
            let result = request_retry::read_range(
                self.source,
                self.object,
                self.next,
                length,
                &mut self.body,
                self.request_send_retries,
            );
            self.read_seconds += requested.elapsed().as_secs_f64();
            result.with_context(|| {
                format!(
                    "CAR range request failed: object={} bytes={}..{} expected_body_bytes={length}",
                    self.object,
                    self.next,
                    self.next + length as u64,
                )
            })
            .map_err(|error| io::Error::other(format!("{error:#}")))?;
            if self.body.len() != length {
                return Err(io::Error::other(format!(
                    "CAR range body length mismatch: object={} bytes={}..{} expected={length} actual={}",
                    self.object,
                    self.next,
                    self.next + length as u64,
                    self.body.len(),
                )));
            }
            self.next += length as u64;
            self.position = 0;
        }
        let count = out.len().min(self.body.len() - self.position);
        out[..count].copy_from_slice(&self.body[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

fn scan_shards<S: RangeSource>(
    source: &S,
    object: &str,
    shards: &[Shard],
    chunk_bytes: usize,
    reuse: bool,
    mode: MetadataMode,
    export: Option<&common_export::Export>,
    request_send_retries: &AtomicU64,
) -> Result<Vec<Row>> {
    scan_shards_with_sources(
        std::slice::from_ref(source),
        object,
        shards,
        chunk_bytes,
        reuse,
        mode,
        export,
        request_send_retries,
    )
}

fn scan_shards_with_sources<S: RangeSource>(
    sources: &[S],
    object: &str,
    shards: &[Shard],
    chunk_bytes: usize,
    reuse: bool,
    mode: MetadataMode,
    export: Option<&common_export::Export>,
    request_send_retries: &AtomicU64,
) -> Result<Vec<Row>> {
    ensure!(
        !sources.is_empty() && sources.len() <= shards.len(),
        "invalid static client count"
    );
    let shards_started = std::time::Instant::now();
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(shards.len());
        for (shard_index, shard) in shards.iter().enumerate() {
            let source = &sources[shard_index % sources.len()];
            handles.push(scope.spawn(move || {
                let first = shard.blocks.first().context("empty shard")?;
                let last = shard.blocks.last().context("empty shard")?;
                let input =
                    RangeRead::new(source, object, first.start, last.end, chunk_bytes, reuse, request_send_retries)?;
                let mut reader = CarBlockReader::with_capacity(input, 8 << 20);
                reader.offset = first.start;
                scan_reader(
                    reader,
                    last.slot + 1,
                    shard.workers,
                    mode,
                    export,
                    Some(&shard.blocks),
                )
                .with_context(|| {
                    format!(
                        "CAR shard {shard_index} failed: slots={}..{} bytes={}..{} decode_workers={}",
                        first.slot, last.slot + 1, first.start, last.end, shard.workers,
                    )
                })
                .inspect_err(|error| {
                    eprintln!(
                        "[car-shard-error elapsed_seconds={:.3}] {error:#}",
                        shards_started.elapsed().as_secs_f64(),
                    );
                })
            }));
        }
        let mut rows = Vec::new();
        let mut error = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(mut shard_rows)) => rows.append(&mut shard_rows),
                Ok(Err(failure)) => {
                    error.get_or_insert(failure);
                }
                Err(_) => {
                    error.get_or_insert_with(|| anyhow::anyhow!("CAR parser worker panicked"));
                }
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        rows.sort_unstable_by_key(|row| row.slot);
        ensure!(
            rows.windows(2).all(|rows| rows[0].slot < rows[1].slot),
            "duplicate shard output slot"
        );
        Ok(rows)
    })
}

// `strong_identity` is the public identity API. Its one missing-ETag error is
// accepted only in this already operator-trusted path; every other error fails.
fn optional_strong_etag(source: &HttpRangeSource, object: &str) -> Result<Option<String>> {
    match source.strong_identity(object) {
        Ok(identity) => Ok(Some(identity.strong_etag)),
        Err(SourceError::Protocol(message))
            if message == format!("HEAD for {object} requires one exact strong ETag") =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) struct Prepared {
    sources: Vec<HttpRangeSource>,
    car_object: String,
    index_object: String,
    car_bytes: u64,
    header_bytes: u64,
    index_digest: String,
    shards: Vec<Shard>,
    ranges: Vec<BlockRange>,
    config: dynamic::Config,
    scheduling: Scheduling,
    jobs: usize,
    metrics: dynamic::Metrics,
    pool_etags: Option<(Option<String>, Option<String>)>,
    loopback_http: bool,
    request_send_retries: AtomicU64,
}

impl Prepared {
    pub(super) fn open(
        url: &str,
        epoch: u64,
        expected: &[[u64; 2]],
        parsers: usize,
        decoders: usize,
        chunk_bytes: usize,
        reuse: bool,
        scheduling: Scheduling,
        clients: usize,
        job_bytes: u64,
        allow_loopback_http: bool,
    ) -> Result<Self> {
        validate_chunk_bytes(chunk_bytes)?;
        ensure!(
            (1..=parsers).contains(&clients),
            "HTTP clients must be 1..=parsers"
        );
        let car_object = format!("epoch-{epoch}.car");
        let (base, loopback_http) = source_base(url, epoch, allow_loopback_http)?;
        let make_source = || {
            HttpRangeSource::with_options(
                base,
                epoch,
                None,
                HttpRangeSourceOptions {
                    object_path_layout: HttpObjectPathLayout::FlatEpoch,
                    allow_insecure_http: loopback_http,
                    ..HttpRangeSourceOptions::default()
                },
            )
        };
        let source = make_source()?;
        let car_bytes = source.size(&car_object)?.context("CAR object missing")?;
        let index_object = format!("epoch-{epoch}-slot-ranges.raw");
        ensure!(
            source.size(&index_object)? == Some(INDEX_BYTES as u64),
            "raw slot index has an incorrect length"
        );
        let index = read_index(&source, &index_object)?;
        let header = source.read_range(&car_object, 0, car_bytes.min(1 << 20) as usize)?;
        let mut header_reader = CarBlockReader::with_capacity(Cursor::new(header.as_slice()), 8192);
        header_reader.skip_header_bounded(1 << 20)?;
        let header_bytes = header_reader.offset;
        let (ranges, shards) = if scheduling == Scheduling::Dynamic {
            validate_parser_count(parsers)?;
            super::validate_decode_workers(decoders)?;
            (
                plan_ranges(&index, epoch, expected, car_bytes, header_bytes)?,
                Vec::new(),
            )
        } else {
            let shards = plan_shards(
                &index,
                epoch,
                expected,
                car_bytes,
                header_bytes,
                parsers,
                decoders,
            )?;
            (
                shards
                    .iter()
                    .flat_map(|shard| shard.blocks.iter().cloned())
                    .collect(),
                shards,
            )
        };
        let jobs = if scheduling == Scheduling::Dynamic {
            dynamic::plan_jobs(&ranges, job_bytes)?.len()
        } else {
            shards.len()
        };
        let pool_etags = if clients > 1 {
            Some((
                optional_strong_etag(&source, &car_object)?,
                optional_strong_etag(&source, &index_object)?,
            ))
        } else {
            None
        };
        let mut sources = vec![source];
        for client in 1..clients {
            // A new builder is required: Clone would share the same blocking runtime.
            let source = make_source()?;
            ensure!(
                source.size(&car_object)? == Some(car_bytes)
                    && source.size(&index_object)? == Some(INDEX_BYTES as u64),
                "HTTP client {client} sees a different object length"
            );
            ensure!(
                read_index(&source, &index_object)? == index,
                "HTTP client {client} sees a different slot index"
            );
            ensure!(
                source.read_range(&car_object, 0, header.len())? == header,
                "HTTP client {client} sees different CAR header bytes"
            );
            ensure!(
                Some((
                    optional_strong_etag(&source, &car_object)?,
                    optional_strong_etag(&source, &index_object)?
                )) == pool_etags,
                "HTTP client {client} sees different strong ETags"
            );
            sources.push(source);
        }
        Ok(Self {
            sources,
            car_object,
            index_object,
            car_bytes,
            header_bytes,
            index_digest: blake3::hash(&index).to_hex().to_string(),
            shards,
            ranges,
            config: dynamic::Config {
                parsers,
                decoders,
                job_bytes,
                chunk_bytes,
                reuse,
            },
            scheduling,
            jobs,
            metrics: dynamic::Metrics::default(),
            pool_etags,
            loopback_http,
            request_send_retries: AtomicU64::new(0),
        })
    }

    pub(super) fn scan(
        &self,
        mode: MetadataMode,
        export: Option<&common_export::Export>,
    ) -> Result<Vec<Row>> {
        let rows = if self.scheduling == Scheduling::Dynamic {
            dynamic::scan_dynamic(
                &self.sources,
                &self.car_object,
                &self.ranges,
                self.config,
                mode,
                export,
                &self.request_send_retries,
                &self.metrics,
            )?
        } else {
            scan_shards_with_sources(
                &self.sources,
                &self.car_object,
                &self.shards,
                self.config.chunk_bytes,
                self.config.reuse,
                mode,
                export,
                &self.request_send_retries,
            )?
        };
        for (client, source) in self.sources.iter().enumerate() {
            ensure!(
                source.size(&self.car_object)? == Some(self.car_bytes),
                "CAR length changed after scan on client {client}"
            );
            ensure!(
                source.size(&self.index_object)? == Some(INDEX_BYTES as u64),
                "slot index length changed after scan on client {client}"
            );
            if let Some(expected) = &self.pool_etags {
                ensure!(
                    &(
                        optional_strong_etag(source, &self.car_object)?,
                        optional_strong_etag(source, &self.index_object)?
                    ) == expected,
                    "HTTP client {client} identity changed after scan"
                );
            }
        }
        Ok(rows)
    }

    pub(super) fn identity(&self) -> Value {
        let source = &self.sources[0];
        let root = source.base_url().as_str().trim_end_matches('/');
        json!({ "url":format!("{root}/{}/{}", source.epoch(), self.car_object), "bytes":self.car_bytes, "operator_trusted":true,
            "slot_index_url":format!("{root}/{}/{}", source.epoch(), self.index_object), "slot_index_bytes":INDEX_BYTES,
            "slot_index_blake3":self.index_digest, "car_header_bytes":self.header_bytes,
            "http_clients":self.sources.len(), "client_identity_check":"all clients read equal full index, header sample, and lengths; optional strong ETags match",
            "loopback_http":self.loopback_http,
            "client_strong_etags":self.pool_etags })
    }

    pub(super) fn stats(&self, scan_complete: bool) -> Value {
        let stats = self.sources.iter().map(HttpRangeSource::stats).fold(
            HttpRangeSourceStats::default(),
            |mut total, next| {
                total.head_requests += next.head_requests;
                total.get_requests += next.get_requests;
                total.returned_body_bytes += next.returned_body_bytes;
                total.incomplete_body_retries += next.incomplete_body_retries;
                total.server_error_retries += next.server_error_retries;
                total
            },
        );
        let completed = scan_complete.then(|| {
            (
                self.ranges.last().unwrap().end - self.ranges[0].start,
                self.config.parsers,
            )
        });
        stats_receipt(
            stats,
            completed,
            self.request_send_retries.load(Ordering::Relaxed),
        )
    }
    pub(super) fn job_count(&self) -> usize {
        self.jobs
    }
    pub(super) fn pipeline_receipt(&self) -> Value {
        if self.scheduling == Scheduling::Dynamic {
            self.metrics.receipt()
        } else {
            Value::Null
        }
    }
    pub(super) fn shard_receipts(&self) -> Value {
        if self.scheduling == Scheduling::Dynamic {
            return Value::Null;
        }
        json!(self.shards.iter().map(|shard| json!({ "start_slot":shard.blocks.first().unwrap().slot,
            "end_slot_exclusive":shard.blocks.last().unwrap().slot + 1, "start_byte":shard.blocks.first().unwrap().start,
            "end_byte_exclusive":shard.blocks.last().unwrap().end, "physical_blocks":shard.blocks.len(), "decode_workers":shard.workers })).collect::<Vec<_>>())
    }
}

fn stats_receipt(
    stats: HttpRangeSourceStats,
    completed: Option<(u64, usize)>,
    request_send_retries: u64,
) -> Value {
    let mut receipt = json!({
        "head_requests":stats.head_requests, "get_requests":stats.get_requests,
        "body_bytes":stats.returned_body_bytes, "incomplete_body_retries":stats.incomplete_body_retries,
        "server_error_retries":stats.server_error_retries, "scan_completed":completed.is_some(),
        "snapshot_scope":"after all parser workers joined",
        "request_send_retries":request_send_retries,
        "request_send_retry_limit":request_retry::REQUEST_SEND_RETRY_LIMIT,
    });
    // On failure, planned spans do not prove which bytes reached the parser.
    if let Some((bytes, workers)) = completed {
        receipt["bytes_delivered"] = json!(bytes);
        receipt["workers_finished"] = json!(workers);
    }
    receipt
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_source::{SourceError, SourceResult};
    use of_car_reader::slot_ranges::SlotRange;

    #[test]
    fn loopback_http_requires_explicit_trust_and_exact_literal_authority() {
        for base in [
            "http://127.0.0.1",
            "http://127.0.0.1:8123",
            "http://[::1]",
            "http://[::1]:8123",
        ] {
            let url = format!("{base}/1000/epoch-1000.car");
            assert_eq!(source_base(&url, 1000, true).unwrap(), (base, true));
            assert!(source_base(&url, 1000, false).is_err());
        }
        let remote = "https://files.old-faithful.net/1000/epoch-1000.car";
        assert_eq!(
            source_base(remote, 1000, false).unwrap(),
            ("https://files.old-faithful.net", false)
        );
        for base in [
            "http://localhost:8123",
            "http://127.1:8123",
            "http://2130706433:8123",
            "http://127.0.0.2:8123",
            "http://[::ffff:127.0.0.1]:8123",
            "http://192.0.2.1:8123",
            "http://user@127.0.0.1:8123",
            "http://127.0.0.1:8123@remote.test",
            "http://127.0.0.1.evil.test:8123",
            "http://127.0.0.1:0",
            "http://127.0.0.1:65536",
            "http://127.0.0.1:",
            "http://127.0.0.1:+8123",
            "http://127.0.0.1\\remote.test",
            "http://127.0.0.1:8123?query",
            "http://127.0.0.1:8123#fragment",
            "http://127.0.0.1:\n8123",
        ] {
            assert!(
                source_base(&format!("{base}/1000/epoch-1000.car"), 1000, true).is_err(),
                "accepted {base}"
            );
        }
        for tail in ["?query", "#fragment"] {
            assert!(
                source_base(
                    &format!("http://127.0.0.1:8123/1000/epoch-1000.car{tail}"),
                    1000,
                    true
                )
                .is_err()
            );
        }
    }

    struct Memory(Vec<u8>);
    impl RangeSource for Memory {
        fn size(&self, _: &str) -> SourceResult<Option<u64>> {
            Ok(Some(self.0.len() as u64))
        }
        fn read_range(&self, _: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
            self.0
                .get(offset as usize..offset as usize + length)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| SourceError::Protocol("test range outside source".into()))
        }
    }

    fn index_rows(rows: &[(usize, u64, u32)]) -> Vec<u8> {
        let mut index = vec![0; INDEX_BYTES];
        for &(slot, offset, len) in rows {
            index[slot * 12..slot * 12 + 12].copy_from_slice(&SlotRange { offset, len }.encode());
        }
        index
    }

    #[test]
    fn failed_body_read_reports_the_requested_range_and_shard_span() {
        struct FailedBody;
        impl RangeSource for FailedBody {
            fn size(&self, _: &str) -> SourceResult<Option<u64>> {
                Ok(Some(200))
            }
            fn read_range(&self, object: &str, _: u64, _: usize) -> SourceResult<Vec<u8>> {
                Err(SourceError::Io {
                    object: object.to_owned(),
                    source: io::Error::other("injected response body failure"),
                })
            }
        }
        let shards = [Shard {
            blocks: vec![BlockRange {
                slot: 42,
                transactions: 1,
                start: 100,
                end: 200,
            }],
            workers: 1,
        }];
        let error = scan_shards(
            &FailedBody,
            "epoch-0.car",
            &shards,
            32,
            true,
            MetadataMode::Owned,
            None,
            &AtomicU64::new(0),
        )
        .unwrap_err();
        let message = format!("{error:#}");
        for expected in [
            "CAR shard 0 failed",
            "slots=42..43",
            "bytes=100..200",
            "decode_workers=1",
            "CAR range request failed",
            "object=epoch-0.car",
            "bytes=100..132",
            "expected_body_bytes=32",
            "injected response body failure",
        ] {
            assert!(
                message.contains(expected),
                "missing {expected:?} in {message}"
            );
        }
    }

    #[test]
    fn failed_scan_stats_report_observed_bytes_without_planned_delivery() {
        let stats = HttpRangeSourceStats {
            head_requests: 2,
            get_requests: 3,
            returned_body_bytes: 17,
            incomplete_body_retries: 2,
            server_error_retries: 1,
        };
        let failed = stats_receipt(stats, None, 7);
        assert_eq!(failed["body_bytes"], 17);
        assert_eq!(failed["get_requests"], 3);
        assert_eq!(failed["incomplete_body_retries"], 2);
        assert_eq!(failed["server_error_retries"], 1);
        assert_eq!(failed["request_send_retries"], 7);
        assert_eq!(failed["request_send_retry_limit"], 2);
        assert_eq!(failed["scan_completed"], false);
        assert!(failed.get("bytes_delivered").is_none());
        assert!(failed.get("workers_finished").is_none());
        let completed = stats_receipt(stats, Some((13, 1)), 2);
        assert_eq!(completed["bytes_delivered"], 13);
        assert_eq!(completed["workers_finished"], 1);
        assert_eq!(completed["scan_completed"], true);
        assert_eq!(completed["request_send_retries"], 2);
        assert_eq!(completed["request_send_retry_limit"], 2);
    }

    #[test]
    fn index_download_uses_two_exact_partial_requests() {
        struct PartialOnly(std::sync::Mutex<Vec<(u64, usize)>>);
        impl RangeSource for PartialOnly {
            fn size(&self, _: &str) -> SourceResult<Option<u64>> {
                Ok(Some(INDEX_BYTES as u64))
            }
            fn read_range(&self, _: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
                assert!(length < INDEX_BYTES);
                assert!(offset + length as u64 <= INDEX_BYTES as u64);
                self.0.lock().unwrap().push((offset, length));
                Ok(vec![if offset == 0 { 11 } else { 23 }; length])
            }
        }
        let source = PartialOnly(std::sync::Mutex::new(Vec::new()));
        let index = read_index(&source, "raw").unwrap();
        let middle = INDEX_BYTES / 2;
        assert!(index[..middle].iter().all(|byte| *byte == 11));
        assert!(index[middle..].iter().all(|byte| *byte == 23));
        assert_eq!(
            *source.0.lock().unwrap(),
            [(0, middle), (middle as u64, INDEX_BYTES - middle)]
        );
    }

    #[test]
    fn shard_plan_checks_boundaries_and_splits_the_total_worker_budget() {
        let index = index_rows(&[(2, 59, 40), (4, 99, 20), (5, 119, 30)]);
        let expected = [[2, 3], [3, 0], [4, 4], [5, 5]];
        let shards = plan_shards(&index, 0, &expected, 149, 59, 2, 5).unwrap();
        assert_eq!(
            shards.iter().map(|shard| shard.workers).collect::<Vec<_>>(),
            [3, 2]
        );
        assert_eq!(
            shards[0].blocks.last().unwrap().end,
            shards[1].blocks[0].start
        );
        assert_eq!(
            shards.iter().map(|shard| shard.blocks.len()).sum::<usize>(),
            3
        );
        for invalid in [
            index_rows(&[(2, 58, 40), (4, 98, 20), (5, 118, 30)]),
            index_rows(&[(2, 59, 40), (4, 98, 20), (5, 118, 30)]),
            index_rows(&[(2, 59, 40), (4, 100, 20), (5, 120, 29)]),
            index_rows(&[(2, 59, 40), (4, 99, 20), (5, 119, 31)]),
            index_rows(&[(2, 59, 40), (3, 99, 0), (4, 99, 20), (5, 119, 30)]),
            index_rows(&[(2, 59, 40), (5, 99, 30)]),
            index_rows(&[(1, 59, 10), (2, 69, 30), (4, 99, 20), (5, 119, 30)]),
        ] {
            assert!(plan_shards(&invalid, 0, &expected, 149, 59, 2, 5).is_err());
        }
        assert!(plan_shards(&index[..INDEX_BYTES - 1], 0, &expected, 149, 59, 2, 5).is_err());
        assert!(plan_shards(&index, 0, &expected, 149, 59, 4, 5).is_err());
        for (parsers, decoders) in [(0, 12), (129, 256), (32, 16)] {
            assert!(validate_worker_budget(parsers, decoders).is_err());
        }
        for parsers in [1, 8, 16, 32, 64, 128] {
            validate_worker_budget(parsers, parsers).unwrap();
        }
    }

    #[test]
    fn range_reader_never_reads_outside_its_span() {
        let memory = Memory((0..100).collect());
        let retries = AtomicU64::new(0);
        for reuse in [false, true] {
            let mut input = RangeRead::new(&memory, "car", 7, 73, 11, reuse, &retries).unwrap();
            let mut actual = Vec::new();
            input.read_to_end(&mut actual).unwrap();
            assert_eq!(actual, memory.0[7..73]);
        }
        assert_eq!(retries.load(Ordering::Relaxed), 0);
        assert!(RangeRead::new(&memory, "car", 9, 8, 11, true, &retries).is_err());
        assert!(RangeRead::new(&memory, "car", 7, 73, 0, true, &retries).is_err());
    }

    #[test]
    fn indexed_128_mib_chunks_preserve_exact_request_and_final_tail_bounds() {
        struct VirtualSource(std::sync::Mutex<Vec<(u64, usize)>>);
        impl RangeSource for VirtualSource {
            fn size(&self, _: &str) -> SourceResult<Option<u64>> {
                Ok(Some((128 << 20) + 100))
            }
            fn read_range(&self, _: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
                self.0.lock().unwrap().push((offset, length));
                Ok(vec![42; length])
            }
        }
        let source = VirtualSource(std::sync::Mutex::new(Vec::new()));
        let retries = AtomicU64::new(0);
        let start = 7;
        let end = start + (128 << 20) + 17;
        for reuse in [false, true] {
            let mut reader =
                RangeRead::new(&source, "car", start, end, 128 << 20, reuse, &retries).unwrap();
            assert_eq!(io::copy(&mut reader, &mut io::sink()).unwrap(), end - start);
            assert!(reader.span_complete());
            assert_eq!(
                std::mem::take(&mut *source.0.lock().unwrap()),
                [(start, 128 << 20), (start + (128 << 20), 17)]
            );
        }
        for invalid in [0, (128 << 20) + 1, usize::MAX] {
            assert!(RangeRead::new(&source, "car", start, end, invalid, true, &retries).is_err());
        }
        assert!(source.0.lock().unwrap().is_empty());
        assert_eq!(retries.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn real_car_shards_match_sequential_export_and_reject_bad_boundaries() {
        let mut car = Vec::new();
        let mut blocks = Vec::new();
        for fixture in [
            include_bytes!("../../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-157-biggest.car").as_slice(),
            include_bytes!("../../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-822-biggest.car").as_slice(),
        ] {
            let mut reader = CarBlockReader::with_capacity(fixture, 8192);
            reader.skip_header_bounded(1 << 20).unwrap();
            let header = reader.offset as usize;
            if car.is_empty() { car.extend_from_slice(&fixture[..header]); }
            let start = car.len() as u64;
            car.extend_from_slice(&fixture[header..]);
            let rows = super::super::scan(fixture, u64::MAX, 1, MetadataMode::Owned, None).unwrap();
            assert_eq!(rows.len(), 1);
            blocks.push(BlockRange { slot: rows[0].slot, transactions: rows[0].tx, start, end: car.len() as u64 });
        }
        let memory = Memory(car);
        let expected: Vec<_> = blocks
            .iter()
            .map(|block| [block.slot, block.transactions])
            .collect();
        let shards: Vec<_> = blocks
            .iter()
            .map(|block| Shard {
                blocks: vec![block.clone()],
                workers: 2,
            })
            .collect();
        let mut reference = None;
        for parallel in [false, true] {
            let output = std::env::temp_dir().join(format!(
                "car-shard-fixture-{}-{parallel}.bin",
                std::process::id()
            ));
            let export = common_export::Export::create(&output).unwrap();
            let rows = if parallel {
                scan_shards(
                    &memory,
                    "car",
                    &shards,
                    1 << 20,
                    true,
                    MetadataMode::Owned,
                    Some(&export),
                    &AtomicU64::new(0),
                )
                .unwrap()
            } else {
                super::super::scan(
                    memory.0.as_slice(),
                    u64::MAX,
                    4,
                    MetadataMode::Owned,
                    Some(&export),
                )
                .unwrap()
            };
            export.finish(&expected).unwrap();
            let bytes = std::fs::read(&output).unwrap();
            if let Some((old_rows, old_bytes)) = &reference {
                assert_eq!(&rows, old_rows);
                assert_eq!(&bytes, old_bytes);
            }
            reference = Some((rows, bytes));
            std::fs::remove_file(output).unwrap();
        }
        let mut malformed = vec![Shard {
            blocks: vec![blocks[0].clone()],
            workers: 1,
        }];
        malformed[0].blocks[0].end -= 1;
        assert!(
            scan_shards(
                &memory,
                "car",
                &malformed,
                1 << 20,
                true,
                MetadataMode::Owned,
                None,
                &AtomicU64::new(0)
            )
            .is_err()
        );
        malformed[0].blocks[0] = blocks[0].clone();
        malformed[0].blocks[0].transactions += 1;
        assert!(
            scan_shards(
                &memory,
                "car",
                &malformed,
                1 << 20,
                true,
                MetadataMode::Owned,
                None,
                &AtomicU64::new(0)
            )
            .is_err()
        );
    }
}
