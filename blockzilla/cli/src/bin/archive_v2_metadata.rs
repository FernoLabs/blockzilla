//! Write one `archive_v2_tx_metadata` row per Compact V2 transaction.
//!
//! Each decode worker calls `decode_compact_v2_metadata`, then
//! `stream_metadata_value`. That writes the get-transaction `meta` JSON:
//! balances, fees, log lines, and base58 addresses. The `CompactMetaV1` is
//! dropped before the ordered window publishes the row. The table is created
//! beforehand. This binary does not run `CREATE TABLE`.
//!
//! Expected columns, in database `blockzilla`:
//!
//! ```sql
//! CREATE TABLE archive_v2_tx_metadata (
//!     slot UInt64,
//!     tx_index UInt32,
//!     metadata String,
//!     indexed_at DateTime('UTC') DEFAULT now()
//! ) ENGINE = ReplacingMergeTree(indexed_at)
//! ORDER BY (slot, tx_index)
//! ```

use std::future::Future;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use blockzilla_archive_v2::{
    ARCHIVE_V2_GENESIS_BIN_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ArchiveV2HotTxRow,
};
use blockzilla_get_block::metadata_json::{
    ByteWriter, MetadataPubkeyResolver, stream_metadata_value,
};
use blockzilla_primitives::CompactPubkey;
use blockzilla_compact_v2_reader::{
    ArchiveIdentity, ArchiveReader, COMPACT_V2_OPTIONAL_OBJECTS, COMPACT_V2_PARALLEL_COMPRESSED_BUFFERS,
    COMPACT_V2_PARALLEL_MAX_BLOCKS_PER_BATCH, COMPACT_V2_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
    COMPACT_V2_REQUIRED_OBJECTS, CompactV2MetadataSchema, HttpObjectPathLayout, HttpRangeSource,
    HttpRangeSourceOptions, MAX_ORDERED_PARALLEL_DECODE_WORKERS,
    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES, NetworkInputConfig, OpenOptions,
    OrderedParallelBlockConfig, PinnedLocalRangeSource, RangeSource, decode_compact_v2_metadata,
};
use clap::Parser;

const DEFAULT_ORIGIN: &str = "https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev";
const TABLE: &str = "archive_v2_tx_metadata";
const RETRY_INITIAL: Duration = Duration::from_millis(500);
const RETRY_MAX: Duration = Duration::from_secs(15);
const RETRY_HORIZON: Duration = Duration::from_secs(600);
const DEFAULT_MAX_IN_FLIGHT: u64 = 128;

#[derive(Parser, Debug)]
#[command(
    name = "archive-v2-metadata",
    about = "Fill ClickHouse archive_v2_tx_metadata from a Compact V2 epoch"
)]
struct Args {
    /// Epoch to read. The public source supplies selected sample epochs.
    #[arg(long)]
    epoch: u64,
    /// Compatible HTTPS archive origin. Defaults to the public sample source.
    #[arg(long, env = "BLOCKZILLA_ORIGIN", conflicts_with = "archive_dir")]
    origin: Option<String>,
    /// Read this local epoch directory instead of the HTTPS source.
    #[arg(long, requires = "source_id")]
    archive_dir: Option<PathBuf>,
    /// Stable ID for a local archive build. Change it when you replace that build.
    #[arg(long, requires = "archive_dir")]
    source_id: Option<String>,
    /// Decode workers (1 through 64).
    #[arg(
        long,
        default_value_t = 4,
        value_parser = clap::value_parser!(u16).range(1..=64)
    )]
    threads: u16,
    /// Read at most this many block rows. Omit to scan the epoch.
    #[arg(long)]
    max_blocks: Option<NonZeroU32>,
    /// ClickHouse HTTP URL. User and password may be included.
    #[arg(long)]
    clickhouse_dsn: String,
    /// Database that receives `archive_v2_tx_metadata`.
    #[arg(long)]
    database: String,
    /// Rows per ClickHouse insert. One full batch is one HTTP request.
    #[arg(long, default_value_t = 1000)]
    batch_rows: usize,
    /// ClickHouse HTTP requests allowed at the same time.
    ///
    /// The reader waits when this many batches are still in flight. The cap
    /// counts requests, so it stays independent of `--batch-rows`.
    #[arg(long, default_value_t = DEFAULT_MAX_IN_FLIGHT, value_parser = clap::value_parser!(u64).range(1..))]
    max_in_flight: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut sink = MetadataSink::connect(
        &args.clickhouse_dsn,
        &args.database,
        args.batch_rows,
        args.max_in_flight,
    )?;
    let workers = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(2)
        .max(2);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    let started = Instant::now();
    let scan_result = runtime.block_on(async {
        tokio::task::block_in_place(|| match open(&args)? {
            OpenedArchive::Local(reader) => scan(&reader, &mut sink, &args),
            OpenedArchive::Http(reader) => scan(&reader, &mut sink, &args),
        })
    });
    let finish_result = runtime.block_on(sink.finish());
    match (scan_result, finish_result) {
        (_, Err(error)) => Err(error),
        (Err(error), Ok(_)) => Err(error),
        (Ok(summary), Ok(written)) => {
            let elapsed = started.elapsed().as_secs_f64().max(1e-9);
            eprintln!(
                "database={} table={TABLE} blocks={} rows={written} transactions={} with_metadata={} without_metadata={} first_slot={} last_slot={} elapsed={elapsed:.1}s",
                args.database,
                summary.blocks,
                summary.transactions,
                summary.with_metadata,
                summary.transactions.saturating_sub(summary.with_metadata),
                summary.first_slot,
                summary.last_slot,
            );
            Ok(())
        }
    }
}

enum OpenedArchive {
    Local(ArchiveReader<PinnedLocalRangeSource>),
    Http(ArchiveReader<HttpRangeSource>),
}

fn open(args: &Args) -> Result<OpenedArchive> {
    eprintln!("Opening epoch {}...", args.epoch);
    let source_id = args
        .source_id
        .clone()
        .unwrap_or_else(|| format!("public-compact-v2-epoch-{}", args.epoch));
    let identity = ArchiveIdentity::mainnet(args.epoch, source_id)?;
    let options = OpenOptions {
        epoch_first_slot: Some(identity.first_slot),
        ..OpenOptions::default()
    };
    if let Some(root) = &args.archive_dir {
        let root = std::fs::canonicalize(root)
            .with_context(|| format!("Cannot open archive directory {}", root.display()))?;
        eprintln!("source={}", root.display());
        let source = PinnedLocalRangeSource::new_anchored(&root, &object_allowlist())
            .context("Cannot pin the local Compact V2 directory")?;
        let reader = ArchiveReader::open_pinned(source, identity, options)?;
        return Ok(OpenedArchive::Local(reader));
    }
    let origin = args.origin.as_deref().unwrap_or(DEFAULT_ORIGIN);
    let base = format!("{}/compact-v2", origin.trim_end_matches('/'));
    eprintln!("Source: {origin}");
    let source = HttpRangeSource::with_options(
        &base,
        args.epoch,
        None,
        HttpRangeSourceOptions {
            object_path_layout: HttpObjectPathLayout::FlatEpoch,
            ..HttpRangeSourceOptions::default()
        },
    )
    .context("Cannot open the Compact V2 HTTPS source")?;
    let reader = ArchiveReader::open_pinned(source, identity, options)?;
    Ok(OpenedArchive::Http(reader))
}

fn object_allowlist() -> Vec<&'static str> {
    let mut names = Vec::with_capacity(
        COMPACT_V2_REQUIRED_OBJECTS.len() + COMPACT_V2_OPTIONAL_OBJECTS.len() + 1,
    );
    names.extend(COMPACT_V2_REQUIRED_OBJECTS);
    names.extend(COMPACT_V2_OPTIONAL_OBJECTS);
    names.push(ARCHIVE_V2_GENESIS_BIN_FILE);
    names
}

struct ScanSummary {
    blocks: u64,
    transactions: u64,
    with_metadata: u64,
    first_slot: u64,
    last_slot: u64,
}

struct DecodedBlock {
    slot: u64,
    rows: Vec<TxMetadataRow>,
}

#[derive(Debug)]
struct ScanError(String);

impl std::fmt::Display for ScanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ScanError {}

impl From<blockzilla_compact_v2_reader::Error> for ScanError {
    fn from(error: blockzilla_compact_v2_reader::Error) -> Self {
        Self(error.to_string())
    }
}

fn scan<S: RangeSource>(
    reader: &ArchiveReader<S>,
    sink: &mut MetadataSink,
    args: &Args,
) -> Result<ScanSummary> {
    let registry = load_registry(reader, args)?;
    let schema = reader.metadata_schema();
    let block_count = reader.index().rows.len();
    let end = match args.max_blocks {
        Some(limit) => block_count.min(usize::try_from(limit.get()).unwrap_or(block_count)),
        None => block_count,
    };
    let threads = usize::from(args.threads);
    eprintln!(
        "Scanning {end} blocks with {threads} decode workers into {TABLE}, batch_rows={}, max_in_flight={}",
        args.batch_rows, args.max_in_flight
    );

    let mut blocks = 0u64;
    let mut transactions = 0u64;
    let mut with_metadata = 0u64;
    let mut first_slot = None;
    let mut last_slot = 0u64;
    let mut mark = Instant::now();
    let mut mark_blocks = 0u64;
    let mut mark_transactions = 0u64;
    reader
        .process_borrowed_blocks_parallel_ordered(
            0..end,
            parallel_config(threads),
            move |_| Ok((schema, Arc::clone(&registry))),
            |(schema, registry), _index, block| decode_block(*schema, registry, &block),
            |_, decoded| {
                if first_slot.is_none() {
                    first_slot = Some(decoded.slot);
                }
                last_slot = decoded.slot;
                blocks += 1;
                for row in decoded.rows {
                    if row.metadata != "null" {
                        with_metadata += 1;
                    }
                    transactions += 1;
                    sink.queue(row)
                        .map_err(|error| ScanError(error.to_string()))?;
                }
                if blocks % 10_000 == 0 {
                    let window = mark.elapsed().as_secs_f64().max(1e-9);
                    let blocks_per_sec = (blocks - mark_blocks) as f64 / window;
                    let tx_per_sec = (transactions - mark_transactions) as f64 / window;
                    eprintln!(
                        "blocks={blocks} slot={last_slot} transactions={transactions} blocks_per_sec={blocks_per_sec:.1} tx_per_sec={tx_per_sec:.0}",
                    );
                    mark = Instant::now();
                    mark_blocks = blocks;
                    mark_transactions = transactions;
                }
                Ok(())
            },
        )
        .map_err(|error: ScanError| anyhow::Error::new(error))?;
    if blocks != end as u64 {
        bail!("scan published {blocks} blocks, expected {end}");
    }
    Ok(ScanSummary {
        blocks,
        transactions,
        with_metadata,
        first_slot: first_slot.unwrap_or(0),
        last_slot,
    })
}

fn parallel_config(workers: usize) -> OrderedParallelBlockConfig {
    let workers = workers.clamp(1, MAX_ORDERED_PARALLEL_DECODE_WORKERS);
    OrderedParallelBlockConfig {
        network_input: Some(NetworkInputConfig {
            workers: 8,
            range_bytes: 32 * 1024 * 1024,
            max_buffer_bytes: 256 * 1024 * 1024,
        }),
        decode_workers: workers,
        compressed_buffer_count: workers.clamp(1, COMPACT_V2_PARALLEL_COMPRESSED_BUFFERS),
        max_blocks_per_batch: COMPACT_V2_PARALLEL_MAX_BLOCKS_PER_BATCH,
        uncompressed_batch_budget_bytes: COMPACT_V2_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
        retained_decompressed_bytes_per_worker: (32 * 1024 * 1024)
            .min(MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES / workers),
        discard_rewards: true,
        ..OrderedParallelBlockConfig::default()
    }
}

fn load_registry<S: RangeSource>(reader: &ArchiveReader<S>, args: &Args) -> Result<Arc<[u8]>> {
    let size = reader.required_file_size(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)?;
    let bytes = if let Some(dir) = &args.archive_dir {
        let path = dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
        std::fs::read(&path).with_context(|| format!("Cannot read {}", path.display()))?
    } else {
        let source = http_source(args)?;
        read_object(&source, ARCHIVE_V2_PUBKEY_REGISTRY_FILE, size)?
    };
    let expected = u64::from(reader.registry_entries())
        .checked_mul(32)
        .context("registry entry count overflow")?;
    if bytes.len() as u64 != size || size != expected {
        bail!(
            "registry.bin is {} bytes, expected {expected} for {} entries",
            bytes.len(),
            reader.registry_entries()
        );
    }
    eprintln!(
        "Loaded registry.bin: {} entries, {:.1} MiB",
        reader.registry_entries(),
        bytes.len() as f64 / (1024.0 * 1024.0)
    );
    Ok(Arc::from(bytes))
}

fn http_source(args: &Args) -> Result<HttpRangeSource> {
    let origin = args.origin.as_deref().unwrap_or(DEFAULT_ORIGIN);
    let base = format!("{}/compact-v2", origin.trim_end_matches('/'));
    HttpRangeSource::with_options(
        &base,
        args.epoch,
        None,
        HttpRangeSourceOptions {
            object_path_layout: HttpObjectPathLayout::FlatEpoch,
            ..HttpRangeSourceOptions::default()
        },
    )
    .context("Cannot open the Compact V2 HTTPS source")
}

fn read_object<S: RangeSource>(source: &S, name: &str, size: u64) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut offset = 0u64;
    const CHUNK: usize = 8 * 1024 * 1024;
    while offset < size {
        let len = usize::try_from((size - offset).min(CHUNK as u64))
            .context("registry range does not fit")?;
        let chunk = source.read_range(name, offset, len).map_err(|error| {
            anyhow::anyhow!("Cannot read {name} at {offset}: {error}")
        })?;
        if chunk.len() != len {
            bail!("{name} returned {} bytes at {offset}, expected {len}", chunk.len());
        }
        out.extend_from_slice(&chunk);
        offset += len as u64;
    }
    Ok(out)
}

fn decode_block(
    schema: CompactV2MetadataSchema,
    registry: &[u8],
    block: &blockzilla_compact_v2_reader::BorrowedDecodedBlock<'_>,
) -> std::result::Result<DecodedBlock, ScanError> {
    let header = block.header();
    let metadata_bytes = block.metadata_bytes();
    let mut rows = Vec::with_capacity(block.tx_rows_len());
    for row in block.tx_rows() {
        rows.push(decode_transaction(
            schema,
            registry,
            header.slot,
            &row,
            metadata_bytes,
        )?);
    }
    let expected = usize::try_from(block.tx_count()).unwrap_or(usize::MAX);
    if rows.len() != expected {
        return Err(ScanError(format!(
            "slot {} decoded {} transactions, index declares {}",
            header.slot,
            rows.len(),
            block.tx_count()
        )));
    }
    Ok(DecodedBlock {
        slot: header.slot,
        rows,
    })
}

fn decode_transaction(
    schema: CompactV2MetadataSchema,
    registry: &[u8],
    slot: u64,
    row: &ArchiveV2HotTxRow,
    metadata_bytes: &[u8],
) -> std::result::Result<TxMetadataRow, ScanError> {
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return Err(ScanError(format!(
            "slot {slot} transaction {} metadata is a raw fallback and cannot be decoded",
            row.tx_index
        )));
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
        if row.metadata_len != 0 {
            return Err(ScanError(format!(
                "slot {slot} transaction {} has metadata bytes without HAS_METADATA",
                row.tx_index
            )));
        }
        return Ok(TxMetadataRow {
            slot,
            tx_index: row.tx_index,
            metadata: "null".to_string(),
        });
    }
    if row.metadata_len == 0 {
        return Err(ScanError(format!(
            "slot {slot} transaction {} has HAS_METADATA and an empty metadata range",
            row.tx_index
        )));
    }
    let bytes = metadata_region(slot, row, metadata_bytes)?;
    let metadata = decode_compact_v2_metadata(schema, bytes).map_err(|error| {
        ScanError(format!(
            "slot {slot} transaction {} metadata: {error}",
            row.tx_index
        ))
    })?;
    let rendered = render_metadata(registry, Some(&metadata))?;
    Ok(TxMetadataRow {
        slot,
        tx_index: row.tx_index,
        metadata: rendered,
    })
}

fn render_metadata(
    registry: &[u8],
    metadata: Option<&blockzilla_compact::CompactMetaV1>,
) -> std::result::Result<String, ScanError> {
    let mut resolver = RegistryResolver { keys: registry };
    let mut bytes = Vec::new();
    let mut writer = ByteWriter(&mut bytes);
    let rendered = drive_ready(stream_metadata_value(
        &mut writer,
        &mut resolver,
        metadata,
        true,
    ));
    drop(writer);
    rendered.map_err(ScanError)?;
    String::from_utf8(bytes).map_err(|error| ScanError(format!("metadata JSON is not UTF-8: {error}")))
}

fn drive_ready<F: Future>(future: F) -> F::Output {
    let mut future = Pin::from(Box::pin(future));
    let waker = std::task::Waker::noop();
    let mut context = TaskContext::from_waker(&waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => unreachable!("registry pubkey resolution does not wait"),
    }
}

struct RegistryResolver<'a> {
    keys: &'a [u8],
}

impl MetadataPubkeyResolver for RegistryResolver<'_> {
    async fn resolve_pubkey_bytes(
        &mut self,
        key: CompactPubkey,
    ) -> std::result::Result<[u8; 32], String> {
        match key {
            CompactPubkey::Raw(bytes) => Ok(bytes),
            CompactPubkey::Id(id) => {
                if id == 0 {
                    return Err("registry id 0 is reserved".to_string());
                }
                let start = usize::try_from(id - 1)
                    .map_err(|_| format!("registry id {id} does not fit"))?
                    .checked_mul(32)
                    .ok_or_else(|| format!("registry id {id} offset overflow"))?;
                let end = start
                    .checked_add(32)
                    .ok_or_else(|| format!("registry id {id} offset overflow"))?;
                let bytes = self.keys.get(start..end).ok_or_else(|| {
                    format!("registry id {id} is outside {} entries", self.keys.len() / 32)
                })?;
                let mut pubkey = [0u8; 32];
                pubkey.copy_from_slice(bytes);
                Ok(pubkey)
            }
        }
    }
}

fn metadata_region<'a>(
    slot: u64,
    row: &ArchiveV2HotTxRow,
    metadata_bytes: &'a [u8],
) -> std::result::Result<&'a [u8], ScanError> {
    let start = usize::try_from(row.metadata_offset)
        .map_err(|_| ScanError(format!("slot {slot} metadata offset does not fit")))?;
    let len = usize::try_from(row.metadata_len)
        .map_err(|_| ScanError(format!("slot {slot} metadata length does not fit")))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| ScanError(format!("slot {slot} metadata range overflows")))?;
    metadata_bytes.get(start..end).ok_or_else(|| {
        ScanError(format!(
            "slot {slot} transaction {} metadata range is outside the block",
            row.tx_index
        ))
    })
}

#[derive(Debug, clickhouse::Row, serde::Serialize)]
struct TxMetadataRow {
    slot: u64,
    tx_index: u32,
    metadata: String,
}

struct MetadataSink {
    client: clickhouse::Client,
    pending: Vec<TxMetadataRow>,
    batch_rows: usize,
    max_in_flight: u64,
    writes: Arc<WriteState>,
}

struct WriteState {
    in_flight: AtomicU64,
    written: AtomicU64,
    failed: Mutex<Option<String>>,
}

struct InFlightGuard(Arc<WriteState>);

impl InFlightGuard {
    fn begin(state: &Arc<WriteState>) -> Self {
        state.in_flight.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(state))
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ClickHouseEndpoint {
    url: String,
    user: Option<String>,
    password: Option<String>,
}

impl MetadataSink {
    fn connect(dsn: &str, database: &str, batch_rows: usize, max_in_flight: u64) -> Result<Self> {
        check_identifier("database", database)?;
        check_identifier("table", TABLE)?;
        let endpoint = split_clickhouse_dsn(dsn)?;
        let mut client = clickhouse::Client::default()
            .with_url(endpoint.url)
            .with_database(database)
            .with_setting("async_insert", "1")
            .with_setting("wait_for_async_insert", "1");
        if let Some(user) = endpoint.user {
            client = client.with_user(user);
        }
        if let Some(password) = endpoint.password {
            client = client.with_password(password);
        }
        Ok(Self {
            client,
            pending: Vec::with_capacity(batch_rows.max(1)),
            batch_rows: batch_rows.max(1),
            max_in_flight: max_in_flight.max(1),
            writes: Arc::new(WriteState {
                in_flight: AtomicU64::new(0),
                written: AtomicU64::new(0),
                failed: Mutex::new(None),
            }),
        })
    }

    fn queue(&mut self, row: TxMetadataRow) -> Result<()> {
        self.raise_background_error()?;
        self.pending.push(row);
        if self.pending.len() >= self.batch_rows {
            self.spawn_batch()?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<u64> {
        if !self.pending.is_empty() {
            self.spawn_batch()?;
        }
        loop {
            if self.writes.in_flight.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.raise_background_error()?;
        Ok(self.writes.written.load(Ordering::SeqCst))
    }

    fn spawn_batch(&mut self) -> Result<()> {
        self.raise_background_error()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        while self.writes.in_flight.load(Ordering::SeqCst) >= self.max_in_flight {
            self.raise_background_error()?;
            std::thread::sleep(Duration::from_millis(1));
        }
        let rows = std::mem::take(&mut self.pending);
        self.pending.reserve(self.batch_rows);
        let row_count = rows.len() as u64;
        let client = self.client.clone();
        let writes = Arc::clone(&self.writes);
        let guard = InFlightGuard::begin(&writes);
        tokio::spawn(async move {
            let _guard = guard;
            match retry_insert(client, rows).await {
                Ok(()) => {
                    writes.written.fetch_add(row_count, Ordering::SeqCst);
                }
                Err(error) => {
                    eprintln!("{error}");
                    let mut failed = writes.failed.lock().expect("metadata insert error lock");
                    if failed.is_none() {
                        *failed = Some(error);
                    }
                }
            }
        });
        Ok(())
    }

    fn raise_background_error(&self) -> Result<()> {
        let failed = self.writes.failed.lock().expect("metadata insert error lock");
        match failed.as_ref() {
            Some(message) => bail!("{message}"),
            None => Ok(()),
        }
    }
}

async fn retry_insert(client: clickhouse::Client, rows: Vec<TxMetadataRow>) -> Result<(), String> {
    let started = Instant::now();
    let mut delay = RETRY_INITIAL;
    let mut attempt = 1u32;
    loop {
        match insert_rows(&client, &rows).await {
            Ok(()) => {
                if attempt > 1 {
                    eprintln!("ClickHouse insert succeeded on attempt {attempt}");
                }
                return Ok(());
            }
            Err(error) => {
                if started.elapsed() >= RETRY_HORIZON {
                    return Err(format!(
                        "ClickHouse insert still failing after {attempt} attempts: {error}"
                    ));
                }
                eprintln!(
                    "ClickHouse insert failed (attempt {attempt}); retrying in {:.1}s: {error}",
                    delay.as_secs_f64()
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(RETRY_MAX);
                attempt += 1;
            }
        }
    }
}

async fn insert_rows(
    client: &clickhouse::Client,
    rows: &[TxMetadataRow],
) -> Result<(), clickhouse::error::Error> {
    let mut insert = client.insert::<TxMetadataRow>(TABLE).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

fn split_clickhouse_dsn(dsn: &str) -> Result<ClickHouseEndpoint> {
    let dsn = dsn.trim();
    if dsn.is_empty() {
        bail!("ClickHouse DSN is empty");
    }
    let Some(rest) = dsn
        .strip_prefix("http://")
        .or_else(|| dsn.strip_prefix("https://"))
    else {
        bail!("ClickHouse DSN must start with http:// or https://");
    };
    let scheme_len = dsn.len() - rest.len();
    let (userinfo, host) = match rest.rsplit_once('@') {
        Some((userinfo, host)) if !host.is_empty() => (Some(userinfo), host),
        _ => (None, rest),
    };
    let host = host.split(['/', '?']).next().unwrap_or(host);
    if host.is_empty() {
        bail!("ClickHouse DSN has no host");
    }
    let (user, password) = match userinfo {
        Some(userinfo) => {
            let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
            if user.is_empty() {
                bail!("ClickHouse DSN user is empty");
            }
            (Some(user.to_owned()), Some(password.to_owned()))
        }
        None => (None, None),
    };
    Ok(ClickHouseEndpoint {
        url: format!("{}{host}", &dsn[..scheme_len]),
        user,
        password,
    })
}

fn check_identifier(kind: &str, name: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        bail!("{kind} name is empty");
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        bail!("{kind} name '{name}' must start with a letter or underscore");
    }
    if !chars.all(|character| character.is_ascii_alphanumeric() || character == '_') {
        bail!("{kind} name '{name}' may contain only letters, digits, and underscores");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_compact::{
        CompactInnerInstruction, CompactInnerInstructions, CompactLogStream, CompactMetaV1,
        CompactReturnData, CompactReward, CompactTokenBalance, CompactTransactionError, DataTable,
        LogEvent,
    };
    use blockzilla_primitives::{CompactPubkey, StringTable, historical_source_wincode_leb128_config};

    fn row(flags: u32, metadata_len: u32) -> ArchiveV2HotTxRow {
        ArchiveV2HotTxRow {
            tx_index: 7,
            flags,
            message_offset: 0,
            message_len: 0,
            metadata_offset: 0,
            metadata_len,
            signature_count: 1,
            reserved: [0; 3],
        }
    }

    fn sample_metadata() -> CompactMetaV1 {
        CompactMetaV1 {
            err: Some(CompactTransactionError::AlreadyProcessed),
            fee: 5_000,
            pre_balances: vec![1, 2, 3],
            post_balances: vec![4, 5],
            inner_instructions: Some(vec![
                CompactInnerInstructions {
                    index: 0,
                    instructions: vec![inner_instruction(), inner_instruction()],
                },
                CompactInnerInstructions {
                    index: 1,
                    instructions: vec![inner_instruction()],
                },
            ]),
            logs: Some(CompactLogStream {
                events: vec![LogEvent::LogTruncated, LogEvent::StakeMergingAccounts],
                strings: StringTable::default(),
                data: DataTable::default(),
            }),
            pre_token_balances: vec![token_balance()],
            post_token_balances: vec![token_balance(), token_balance()],
            rewards: vec![reward()],
            loaded_writable_addresses: vec![CompactPubkey::Raw([1; 32])],
            loaded_readonly_addresses: vec![
                CompactPubkey::Raw([2; 32]),
                CompactPubkey::Raw([3; 32]),
            ],
            return_data: Some(CompactReturnData {
                program_id: CompactPubkey::Raw([4; 32]),
                data: vec![9],
            }),
            compute_units_consumed: Some(12),
            cost_units: None,
        }
    }

    fn inner_instruction() -> CompactInnerInstruction {
        CompactInnerInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: vec![1],
            stack_height: Some(2),
        }
    }

    fn token_balance() -> CompactTokenBalance {
        CompactTokenBalance {
            account_index: 1,
            mint: None,
            owner: None,
            program_id: None,
            amount: 10,
            decimals: 6,
        }
    }

    fn reward() -> CompactReward {
        CompactReward {
            pubkey: CompactPubkey::Raw([5; 32]),
            lamports: 1,
            post_balance: 2,
            reward_type: 1,
            commission: None,
        }
    }

    #[test]
    fn decoded_metadata_row_is_get_transaction_json() {
        let mut metadata = sample_metadata();
        metadata.pre_token_balances[0].mint = Some(CompactPubkey::Id(1));
        let bytes = wincode::config::serialize(&metadata, historical_source_wincode_leb128_config())
            .unwrap();
        let registry = [9u8; 32];
        let decoded = decode_transaction(
            CompactV2MetadataSchema::CurrentTypedError,
            &registry,
            388_800_000,
            &row(ARCHIVE_V2_TX_FLAG_HAS_METADATA, bytes.len() as u32),
            &bytes,
        )
        .unwrap();
        assert_eq!(decoded.slot, 388_800_000);
        assert_eq!(decoded.tx_index, 7);
        assert!(decoded.metadata.contains(r#""fee":5000"#));
        assert!(decoded.metadata.contains(r#""err":"AlreadyProcessed""#));
        assert!(decoded.metadata.contains(r#""preBalances":[1,2,3]"#));
        assert!(decoded.metadata.contains(r#""postBalances":[4,5]"#));
        assert!(decoded.metadata.contains("Log truncated"));
        assert!(decoded.metadata.contains("Merging stake accounts"));
        assert!(decoded.metadata.contains(r#""computeUnitsConsumed":12"#));
        assert!(decoded.metadata.contains(r#""rewardType":"Fee""#));
        assert!(decoded.metadata.contains(r#""data":["CQ==","base64"]"#));
        let mint = bs58::encode([9u8; 32]).into_string();
        assert!(decoded.metadata.contains(&mint));
        assert!(!decoded.metadata.contains("\"Id\""));
    }

    #[test]
    fn missing_metadata_row_is_null() {
        let decoded = decode_transaction(
            CompactV2MetadataSchema::CurrentTypedError,
            &[],
            42,
            &row(0, 0),
            &[],
        )
        .unwrap();
        assert_eq!(decoded.metadata, "null");
        let error = decode_transaction(
            CompactV2MetadataSchema::CurrentTypedError,
            &[],
            42,
            &row(0, 4),
            &[1, 2, 3, 4],
        )
        .unwrap_err();
        assert!(error.to_string().contains("without HAS_METADATA"));
    }

    #[test]
    fn raw_fallback_stops_the_block() {
        let error = decode_transaction(
            CompactV2MetadataSchema::CurrentTypedError,
            &[],
            42,
            &row(
                ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                1,
            ),
            &[0],
        )
        .unwrap_err();
        assert!(error.to_string().contains("raw fallback"));
    }

    #[test]
    fn table_name_rejects_a_hyphen() {
        assert_eq!(TABLE, "archive_v2_tx_metadata");
        assert!(check_identifier("table", TABLE).is_ok());
        assert!(check_identifier("table", "archive-v2").is_err());
    }

    #[test]
    fn dsn_keeps_the_password_out_of_the_url() {
        let endpoint = split_clickhouse_dsn("http://default:secret@127.0.0.1:8123").unwrap();
        assert_eq!(endpoint.url, "http://127.0.0.1:8123");
        assert_eq!(endpoint.user.as_deref(), Some("default"));
        assert_eq!(endpoint.password.as_deref(), Some("secret"));
    }
}
