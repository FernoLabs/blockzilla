//! Write one `archive_v2_slot_status` row per Compact V2 block.
//!
//! The columns match `jetstreamer_slot_status`. Inserts use the `clickhouse`
//! crate: `RowBinary` rows on `tokio::spawn`, with `async_insert=1` and
//! `wait_for_async_insert=1`. A transaction is a vote when it has one or two
//! signatures, a legacy message, and one instruction whose program is the vote
//! program. `block_time` comes from the compact block header.

use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use blockzilla_archive_v2::{
    ARCHIVE_V2_GENESIS_BIN_FILE, ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ArchiveV2HotTxRow,
};
use blockzilla_compact_v2_reader::{
    ArchiveIdentity, ArchiveReader, COMPACT_V2_OPTIONAL_OBJECTS, COMPACT_V2_PARALLEL_COMPRESSED_BUFFERS,
    COMPACT_V2_PARALLEL_MAX_BLOCKS_PER_BATCH, COMPACT_V2_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
    COMPACT_V2_REQUIRED_OBJECTS, CompactPubkey, CompactV2MessageProjector, CompactV2MessageSchema,
    HttpObjectPathLayout, HttpRangeSource, HttpRangeSourceOptions, MAX_ORDERED_PARALLEL_DECODE_WORKERS,
    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES, NetworkInputConfig, OpenOptions,
    OrderedParallelBlockConfig, PinnedLocalRangeSource, ProjectedCompactV2MessageVersion,
    RangeSource,
};
use clap::Parser;

const DEFAULT_ORIGIN: &str = "https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev";
const TABLE: &str = "archive_v2_slot_status";
/// Vote111111111111111111111111111111111111111
const VOTE_PROGRAM_ID: [u8; 32] = [
    7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179, 216, 53, 94, 115, 209,
    16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0,
];
const RETRY_INITIAL: Duration = Duration::from_millis(500);
const RETRY_MAX: Duration = Duration::from_secs(15);
const RETRY_HORIZON: Duration = Duration::from_secs(600);
const DEFAULT_MAX_IN_FLIGHT: u64 = 128;

#[derive(Parser, Debug)]
#[command(
    name = "archive-v2-slot-status",
    about = "Fill ClickHouse archive_v2_slot_status from a Compact V2 epoch"
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
    /// Database that receives `archive_v2_slot_status`.
    #[arg(long)]
    database: String,
    /// Value stored in `thread_id`. A single reader uses 0.
    #[arg(long, default_value_t = 0)]
    thread_id: u8,
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
    let mut sink = SlotStatusSink::connect(
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
                "database={} table={TABLE} blocks={} rows={written} transactions={} first_slot={} last_slot={} elapsed={elapsed:.1}s",
                args.database,
                summary.blocks,
                summary.transactions,
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
    first_slot: u64,
    last_slot: u64,
}

struct Worker {
    projector: CompactV2MessageProjector,
    vote_registry_id: Option<u32>,
}

struct CountedBlock {
    slot: u64,
    votes: u64,
    non_votes: u64,
    block_time: Option<i64>,
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
    sink: &mut SlotStatusSink,
    args: &Args,
) -> Result<ScanSummary> {
    let vote_filter = reader
        .compile_pubkey_filter([VOTE_PROGRAM_ID])
        .context("Cannot resolve the vote program in registry.bin")?;
    let schema = reader.message_schema();
    let registry_entries = reader.registry_entries();
    let vote_registry_id = vote_filter.registry_id_for(&VOTE_PROGRAM_ID);
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
    let mut first_slot = None;
    let mut last_slot = 0u64;
    let mut mark = Instant::now();
    let mut mark_blocks = 0u64;
    let mut mark_transactions = 0u64;
    let thread_id = args.thread_id;
    reader
        .process_borrowed_blocks_parallel_ordered(
            0..end,
            parallel_config(threads),
            move |_| {
                Ok(Worker {
                    projector: CompactV2MessageProjector::new(schema, registry_entries),
                    vote_registry_id,
                })
            },
            |worker, _index, block| count_block(worker, &block),
            |_, counted| {
                let row = status_row(
                    counted.slot,
                    counted.votes,
                    counted.non_votes,
                    thread_id,
                    counted.block_time,
                );
                if first_slot.is_none() {
                    first_slot = Some(row.slot);
                }
                last_slot = row.slot;
                blocks += 1;
                transactions += u64::from(row.transaction_count);
                if blocks % 10_000 == 0 {
                    let window = mark.elapsed().as_secs_f64().max(1e-9);
                    let blocks_per_sec = (blocks - mark_blocks) as f64 / window;
                    let tx_per_sec = (transactions - mark_transactions) as f64 / window;
                    eprintln!(
                        "blocks={blocks} slot={} blocks_per_sec={blocks_per_sec:.1} tx_per_sec={tx_per_sec:.0}",
                        row.slot,
                    );
                    mark = Instant::now();
                    mark_blocks = blocks;
                    mark_transactions = transactions;
                }
                sink.queue(row).map_err(|error| ScanError(error.to_string()))?;
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

fn count_block(
    worker: &mut Worker,
    block: &blockzilla_compact_v2_reader::BorrowedDecodedBlock<'_>,
) -> std::result::Result<CountedBlock, ScanError> {
    let header = block.header();
    let messages = block.message_bytes();
    let mut votes = 0u64;
    let mut non_votes = 0u64;
    for row in block.tx_rows() {
        if is_simple_vote(worker, header.slot, &row, messages)? {
            votes += 1;
        } else {
            non_votes += 1;
        }
    }
    let transactions = votes
        .checked_add(non_votes)
        .ok_or_else(|| ScanError(format!("slot {} transaction count overflow", header.slot)))?;
    if transactions != u64::from(block.tx_count()) {
        return Err(ScanError(format!(
            "slot {} classified {transactions} transactions, index declares {}",
            header.slot,
            block.tx_count()
        )));
    }
    Ok(CountedBlock {
        slot: header.slot,
        votes,
        non_votes,
        block_time: header.block_time,
    })
}

fn is_simple_vote(
    worker: &Worker,
    slot: u64,
    row: &ArchiveV2HotTxRow,
    messages: &[u8],
) -> std::result::Result<bool, ScanError> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Err(ScanError(format!(
            "slot {slot} transaction {} is a raw fallback and has no vote classification",
            row.tx_index
        )));
    }
    let message_bytes = message_region(slot, row, messages)?;
    let message = worker
        .projector
        .project_with_instruction_data_for_programs(message_bytes, &[], None)
        .map_err(|error| {
            ScanError(format!(
                "slot {slot} transaction {} message: {error}",
                row.tx_index
            ))
        })?;
    let is_v0 = matches!(
        message.version(),
        ProjectedCompactV2MessageVersion::V0 { .. }
    );
    if is_v0 != (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0) {
        return Err(ScanError(format!(
            "slot {slot} transaction {} message version differs from its row flags",
            row.tx_index
        )));
    }
    if message.header().num_required_signatures != row.signature_count {
        return Err(ScanError(format!(
            "slot {slot} transaction {} has {} signatures but requires {}",
            row.tx_index,
            row.signature_count,
            message.header().num_required_signatures
        )));
    }
    if !(1..=2).contains(&row.signature_count) {
        return Ok(false);
    }
    if !matches!(message.version(), ProjectedCompactV2MessageVersion::Legacy) {
        return Ok(false);
    }
    let [instruction] = message.instructions() else {
        return Ok(false);
    };
    let program_index = usize::from(instruction.program_id_index());
    let Some(program) = message.static_account_keys().get(program_index) else {
        return Err(ScanError(format!(
            "slot {slot} transaction {} program index {program_index} is outside the message accounts",
            row.tx_index
        )));
    };
    Ok(program_is_vote(program, worker.vote_registry_id))
}

fn program_is_vote(program: &CompactPubkey, vote_registry_id: Option<u32>) -> bool {
    match program {
        CompactPubkey::Raw(bytes) => bytes == &VOTE_PROGRAM_ID,
        CompactPubkey::Id(id) => vote_registry_id == Some(*id),
    }
}

fn message_region<'a>(
    slot: u64,
    row: &ArchiveV2HotTxRow,
    messages: &'a [u8],
) -> std::result::Result<&'a [u8], ScanError> {
    let start = usize::try_from(row.message_offset)
        .map_err(|_| ScanError(format!("slot {slot} message offset does not fit")))?;
    let len = usize::try_from(row.message_len)
        .map_err(|_| ScanError(format!("slot {slot} message length does not fit")))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| ScanError(format!("slot {slot} message range overflows")))?;
    messages.get(start..end).ok_or_else(|| {
        ScanError(format!(
            "slot {slot} transaction {} message range is outside the block",
            row.tx_index
        ))
    })
}

fn status_row(
    slot: u64,
    votes: u64,
    non_votes: u64,
    thread_id: u8,
    block_time: Option<i64>,
) -> StatusRow {
    StatusRow {
        slot,
        transaction_count: cap_u32(votes.saturating_add(non_votes)),
        vote_transaction_count: cap_u32(votes),
        non_vote_transaction_count: cap_u32(non_votes),
        thread_id,
        block_time: clamp_block_time(block_time),
    }
}

fn cap_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn clamp_block_time(block_time: Option<i64>) -> u32 {
    match block_time {
        Some(timestamp) if timestamp > 0 && timestamp <= i64::from(u32::MAX) => timestamp as u32,
        Some(timestamp) if timestamp > i64::from(u32::MAX) => u32::MAX,
        _ => 0,
    }
}

#[derive(clickhouse::Row, serde::Serialize)]
struct StatusRow {
    slot: u64,
    transaction_count: u32,
    vote_transaction_count: u32,
    non_vote_transaction_count: u32,
    thread_id: u8,
    block_time: u32,
}

struct SlotStatusSink {
    client: clickhouse::Client,
    pending: Vec<StatusRow>,
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

impl SlotStatusSink {
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

    fn queue(&mut self, row: StatusRow) -> Result<()> {
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
                    let mut failed = writes.failed.lock().expect("slot status error lock");
                    if failed.is_none() {
                        *failed = Some(error);
                    }
                }
            }
        });
        Ok(())
    }

    fn raise_background_error(&self) -> Result<()> {
        let failed = self.writes.failed.lock().expect("slot status error lock");
        match failed.as_ref() {
            Some(message) => bail!("{message}"),
            None => Ok(()),
        }
    }
}

async fn retry_insert(client: clickhouse::Client, rows: Vec<StatusRow>) -> Result<(), String> {
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
    rows: &[StatusRow],
) -> Result<(), clickhouse::error::Error> {
    let mut insert = client.insert::<StatusRow>(TABLE).await?;
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
    use blockzilla_archive_v2::{
        ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
        ArchiveV2HotMessagePayload, ArchiveV2HotV0Message,
    };
    use blockzilla_compact::{CompactMessageHeader, OwnedCompactRecentBlockhash};
    use blockzilla_primitives::wincode_leb128_config;

    fn header(signatures: u8, readonly_unsigned: u8) -> CompactMessageHeader {
        CompactMessageHeader {
            num_required_signatures: signatures,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: readonly_unsigned,
        }
    }

    fn instruction(program_id_index: u8) -> ArchiveV2HotInstruction {
        ArchiveV2HotInstruction {
            program_id_index,
            accounts: vec![0],
            data: ArchiveV2HotInstructionData::Raw(Vec::new()),
        }
    }

    fn classify(
        message: &ArchiveV2HotMessagePayload,
        signature_count: u8,
        flags: u32,
        vote_registry_id: Option<u32>,
        registry_entries: u32,
    ) -> std::result::Result<bool, ScanError> {
        let bytes = wincode::config::serialize(message, wincode_leb128_config()).unwrap();
        let worker = Worker {
            projector: CompactV2MessageProjector::new(CompactV2MessageSchema::Current, registry_entries),
            vote_registry_id,
        };
        let row = ArchiveV2HotTxRow {
            tx_index: 0,
            flags,
            message_offset: 0,
            message_len: u32::try_from(bytes.len()).unwrap(),
            metadata_offset: 0,
            metadata_len: 0,
            signature_count,
            reserved: [0; 3],
        };
        is_simple_vote(&worker, 388_800_000, &row, &bytes)
    }

    fn legacy_vote(program: CompactPubkey, signatures: u8) -> ArchiveV2HotMessagePayload {
        let mut keys = vec![CompactPubkey::Raw([1; 32]); usize::from(signatures)];
        keys.push(program);
        ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: header(signatures, 1),
            account_keys: keys,
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: vec![instruction(signatures)],
        })
    }

    #[test]
    fn legacy_vote_program_matches_the_car_rule() {
        let vote = CompactPubkey::Raw(VOTE_PROGRAM_ID);
        assert!(classify(&legacy_vote(vote, 1), 1, 0, None, 0).unwrap());
        assert!(classify(&legacy_vote(vote, 2), 2, 0, None, 0).unwrap());
        let too_many = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: header(3, 1),
            account_keys: vec![
                CompactPubkey::Raw([1; 32]),
                CompactPubkey::Raw([2; 32]),
                CompactPubkey::Raw([4; 32]),
                vote,
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: vec![instruction(3)],
        });
        assert!(!classify(&too_many, 3, 0, None, 0).unwrap());
        let other = CompactPubkey::Raw([9; 32]);
        assert!(!classify(&legacy_vote(other, 1), 1, 0, None, 0).unwrap());
        let two = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: header(1, 2),
            account_keys: vec![CompactPubkey::Raw([1; 32]), vote, other],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: vec![instruction(1), instruction(2)],
        });
        assert!(!classify(&two, 1, 0, None, 0).unwrap());
    }

    #[test]
    fn registry_id_and_versioned_message_follow_the_car_rule() {
        let vote = CompactPubkey::Id(7);
        assert!(classify(&legacy_vote(vote, 1), 1, 0, Some(7), 7).unwrap());
        assert!(!classify(&legacy_vote(vote, 1), 1, 0, Some(8), 8).unwrap());
        let versioned = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: header(1, 1),
            account_keys: vec![CompactPubkey::Raw([1; 32]), CompactPubkey::Raw(VOTE_PROGRAM_ID)],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: vec![instruction(1)],
            address_table_lookups: Vec::new(),
        });
        assert!(!classify(&versioned, 1, ARCHIVE_V2_TX_FLAG_MESSAGE_V0, None, 0).unwrap());
    }

    #[test]
    fn raw_fallback_and_flag_mismatch_stop_the_classification() {
        let vote = CompactPubkey::Raw(VOTE_PROGRAM_ID);
        let error = classify(
            &legacy_vote(vote, 1),
            1,
            ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
            None,
            0,
        )
        .unwrap_err();
        assert!(error.to_string().contains("raw fallback"));
        let error = classify(&legacy_vote(vote, 1), 1, ARCHIVE_V2_TX_FLAG_MESSAGE_V0, None, 0)
            .unwrap_err();
        assert!(error.to_string().contains("row flags"));
    }

    #[test]
    fn table_name_rejects_a_hyphen() {
        assert_eq!(TABLE, "archive_v2_slot_status");
        assert!(check_identifier("table", TABLE).is_ok());
        assert!(check_identifier("table", "archive-v2").is_err());
    }

    #[test]
    fn dsn_keeps_the_password_out_of_the_url() {
        let endpoint =
            split_clickhouse_dsn("http://default:secret@127.0.0.1:8123").unwrap();
        assert_eq!(endpoint.url, "http://127.0.0.1:8123");
        assert_eq!(endpoint.user.as_deref(), Some("default"));
        assert_eq!(endpoint.password.as_deref(), Some("secret"));
    }
}
