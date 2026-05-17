#![allow(clippy::too_many_arguments)]

use anyhow::{Context, Result, bail};
#[cfg(test)]
use of_car_reader::versioned_transaction::{VersionedMessage, VersionedTransaction};
use of_car_reader::{
    CarBlockReader,
    node::{Node, decode_node, is_transaction_node},
    versioned_transaction::MessageHeader,
};
use serde::{Deserialize, Serialize};
use solana_short_vec::decode_shortu16_len;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const CAR_BUF: usize = 128 << 20;
const PREFETCH_CHUNK: usize = 16 << 20;
const PREFETCH_DEPTH: usize = 8;
const MESSAGE_VERSION_PREFIX: u8 = 0x80;
const FORMAT_VERSION: u32 = 1;
const DEFAULT_BUCKET_COUNT: usize = 256;
const DEFAULT_FLUSH_ACCOUNT_THRESHOLD: usize = 1_000_000;
const MAX_BUCKET_COUNT: usize = 65_536;
const BUCKET_WRITER_BUF: usize = 64 << 10;
const TOUCH_RECORD_BYTES: usize = 32 + 8 + 8 + 32 + 64;
const GLOBAL_RECORD_BYTES: usize = 32 + 8 + 8 + 8 + 32 + 64;
const SIGNATURE_RECORD_BYTES: usize = 64 + 8 + 8 + 8 + 8;
const NO_TX_INDEX: u64 = u64::MAX;

#[derive(Debug, Clone)]
pub struct DumpConfig {
    pub car_path: PathBuf,
    pub out_dir: PathBuf,
    pub bucket_count: usize,
    pub flush_account_threshold: usize,
    pub dump_signatures: bool,
    pub prefetch: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpSummary {
    pub format_version: u32,
    pub car_path: String,
    pub compressed: bool,
    pub accounts_path: String,
    pub first_seen_path: String,
    pub signatures_path: Option<String>,
    pub signature_record_bytes: Option<usize>,
    pub offset_kind: String,
    pub bucket_count: usize,
    pub flush_account_threshold: usize,
    pub temp_bucket_bytes: u64,
    pub scanned_txs: usize,
    pub skipped_txs: usize,
    pub skipped_tx_data_continuations: usize,
    pub tx_decode_failures: usize,
    pub unique_accounts: usize,
    pub elapsed_secs: f64,
    #[serde(default)]
    pub scan_elapsed_secs: f64,
    #[serde(default)]
    pub spool_flush_elapsed_secs: f64,
    #[serde(default)]
    pub reduce_elapsed_secs: f64,
    #[serde(default)]
    pub car_stream_bytes: u64,
    #[serde(default)]
    pub car_entries: usize,
    #[serde(default)]
    pub transaction_entries: usize,
    #[serde(default)]
    pub transaction_payload_bytes: u64,
    #[serde(default)]
    pub tx_data_bytes: u64,
    #[serde(default)]
    pub legacy_txs: usize,
    #[serde(default)]
    pub v0_txs: usize,
    #[serde(default)]
    pub message_account_touches: usize,
    #[serde(default)]
    pub lookup_table_account_touches: usize,
    #[serde(default)]
    pub txs_without_signature: usize,
    #[serde(default)]
    pub txs_without_signer: usize,
    #[serde(default)]
    pub temp_touch_records: usize,
    #[serde(default)]
    pub spool_flushes: usize,
    #[serde(default)]
    pub max_pending_accounts: usize,
}

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub car_dir: PathBuf,
    pub out_dir: PathBuf,
    pub resume: bool,
    pub jobs: usize,
    pub bucket_count: usize,
    pub flush_account_threshold: usize,
    pub dump_signatures: bool,
    pub prefetch: bool,
}

#[derive(Debug, Clone)]
pub struct BatchResult {
    pub epoch: u64,
    pub car_path: PathBuf,
    pub out_dir: PathBuf,
    pub unique_accounts: usize,
    pub scanned_txs: usize,
    pub elapsed_secs: f64,
    pub reused_existing: bool,
}

#[derive(Debug, Clone)]
pub struct MergeConfig {
    pub dump_dir: PathBuf,
    pub out_dir: PathBuf,
    pub bucket_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeSummary {
    pub format_version: u32,
    pub dump_dir: String,
    pub global_first_seen_path: String,
    pub bucket_count: usize,
    pub temp_bucket_bytes: u64,
    pub epoch_dirs: usize,
    pub input_rows: usize,
    pub unique_accounts: usize,
    pub elapsed_secs: f64,
}

#[derive(Debug, Clone)]
pub struct ProfileConfig {
    pub car_path: PathBuf,
    pub mode: ProfileMode,
    pub max_entries: Option<usize>,
    pub max_txs: Option<usize>,
    pub max_bytes: Option<u64>,
    pub temp_dir: Option<PathBuf>,
    pub prefetch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProfileMode {
    RawRead,
    DecodeRead,
    CarFraming,
    NodeDecode,
    TxParse,
    AccountSpool,
}

impl FromStr for ProfileMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "raw-read" => Ok(Self::RawRead),
            "decode-read" => Ok(Self::DecodeRead),
            "car-framing" => Ok(Self::CarFraming),
            "node-decode" => Ok(Self::NodeDecode),
            "tx-parse" => Ok(Self::TxParse),
            "account-spool" => Ok(Self::AccountSpool),
            _ => bail!(
                "unknown profile mode {value}; expected raw-read, decode-read, car-framing, node-decode, tx-parse, or account-spool"
            ),
        }
    }
}

impl ProfileMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RawRead => "raw-read",
            Self::DecodeRead => "decode-read",
            Self::CarFraming => "car-framing",
            Self::NodeDecode => "node-decode",
            Self::TxParse => "tx-parse",
            Self::AccountSpool => "account-spool",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSummary {
    pub mode: ProfileMode,
    pub car_path: String,
    pub compressed: bool,
    pub input_file_bytes: u64,
    pub elapsed_secs: f64,
    pub copied_bytes: u64,
    pub car_stream_bytes: u64,
    pub car_entries: usize,
    pub transaction_entries: usize,
    pub transaction_payload_bytes: u64,
    pub tx_data_bytes: u64,
    pub scanned_txs: usize,
    pub skipped_txs: usize,
    pub skipped_tx_data_continuations: usize,
    pub tx_decode_failures: usize,
    pub legacy_txs: usize,
    pub v0_txs: usize,
    pub message_account_touches: usize,
    pub lookup_table_account_touches: usize,
    pub temp_bucket_bytes: u64,
    pub temp_touch_records: usize,
    pub spool_flushes: usize,
    pub max_pending_accounts: usize,
    pub input_file_gbps: f64,
    pub car_stream_gbps: f64,
    pub copied_gbps: f64,
    pub txs_per_sec: f64,
}

#[derive(Debug, Clone, Copy)]
struct FirstSeen {
    slot: u64,
    tx_index: Option<u64>,
    signer: [u8; 32],
    signature: [u8; 64],
}

#[derive(Debug, Clone, Copy)]
struct GlobalFirstSeen {
    epoch: u64,
    first_seen: FirstSeen,
}

#[derive(Debug, Clone, Copy)]
struct TouchRecord {
    account: [u8; 32],
    first_seen: FirstSeen,
}

#[derive(Debug, Clone, Copy)]
struct GlobalRecord {
    account: [u8; 32],
    first_seen: GlobalFirstSeen,
}

#[derive(Debug, Default)]
struct ScanState {
    scanned_txs: usize,
    skipped_txs: usize,
    skipped_tx_data_continuations: usize,
    tx_decode_failures: usize,
    car_stream_bytes: u64,
    car_entries: usize,
    transaction_entries: usize,
    transaction_payload_bytes: u64,
    tx_data_bytes: u64,
    legacy_txs: usize,
    v0_txs: usize,
    message_account_touches: usize,
    lookup_table_account_touches: usize,
    txs_without_signature: usize,
    txs_without_signer: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct SpoolStats {
    touch_records: usize,
    flushes: usize,
    max_pending_accounts: usize,
}

pub fn dump_accounts(config: DumpConfig) -> Result<DumpSummary> {
    let started = Instant::now();
    fs::create_dir_all(&config.out_dir)
        .with_context(|| format!("create {}", config.out_dir.display()))?;

    let bucket_count = normalized_bucket_count(config.bucket_count)?;
    let flush_account_threshold =
        normalized_flush_account_threshold(config.flush_account_threshold)?;
    let compressed = is_zstd_path(&config.car_path);
    let offset_kind = if compressed {
        "decompressed_car_stream_offset"
    } else {
        "car_file_offset"
    };

    let bucket_dir = config.out_dir.join(".tmp-of-account-index-buckets");
    reset_dir(&bucket_dir)?;

    let mut state = ScanState::default();
    let mut account_spool = AccountSpool::new(&bucket_dir, bucket_count, flush_account_threshold)?;

    let signatures_path = config.out_dir.join("signatures.bin");
    let mut signature_writer = if config.dump_signatures {
        Some(BufWriter::new(
            File::create(&signatures_path)
                .with_context(|| format!("create {}", signatures_path.display()))?,
        ))
    } else {
        None
    };

    let scan_started = Instant::now();
    scan_car(
        &config.car_path,
        compressed,
        config.prefetch,
        &mut state,
        &mut account_spool,
        signature_writer.as_mut(),
    )?;
    let scan_elapsed_secs = scan_started.elapsed().as_secs_f64();

    let spool_flush_started = Instant::now();
    if let Some(writer) = signature_writer.as_mut() {
        writer.flush().context("flush signatures.bin")?;
    }
    let (temp_bucket_bytes, spool_stats) = account_spool.finish()?;
    let spool_flush_elapsed_secs = spool_flush_started.elapsed().as_secs_f64();

    let accounts_path = config.out_dir.join("accounts.txt");
    let first_seen_path = config.out_dir.join("first-seen.tsv");
    let mut accounts_writer = BufWriter::new(
        File::create(&accounts_path)
            .with_context(|| format!("create {}", accounts_path.display()))?,
    );
    let mut first_seen_writer = BufWriter::new(
        File::create(&first_seen_path)
            .with_context(|| format!("create {}", first_seen_path.display()))?,
    );
    writeln!(
        first_seen_writer,
        "account\tfirst_slot\tfirst_tx_index\tfirst_signer\tfirst_signature"
    )
    .context("write first-seen header")?;

    let reduce_started = Instant::now();
    let unique_accounts = reduce_epoch_buckets(
        &bucket_dir,
        bucket_count,
        &mut accounts_writer,
        &mut first_seen_writer,
    )?;
    accounts_writer.flush().context("flush accounts.txt")?;
    first_seen_writer.flush().context("flush first-seen.tsv")?;
    let reduce_elapsed_secs = reduce_started.elapsed().as_secs_f64();
    fs::remove_dir_all(&bucket_dir).with_context(|| format!("remove {}", bucket_dir.display()))?;

    let summary = DumpSummary {
        format_version: FORMAT_VERSION,
        car_path: config.car_path.display().to_string(),
        compressed,
        accounts_path: accounts_path.display().to_string(),
        first_seen_path: first_seen_path.display().to_string(),
        signatures_path: config
            .dump_signatures
            .then(|| signatures_path.display().to_string()),
        signature_record_bytes: config.dump_signatures.then_some(SIGNATURE_RECORD_BYTES),
        offset_kind: offset_kind.to_string(),
        bucket_count,
        flush_account_threshold,
        temp_bucket_bytes,
        scanned_txs: state.scanned_txs,
        skipped_txs: state.skipped_txs,
        skipped_tx_data_continuations: state.skipped_tx_data_continuations,
        tx_decode_failures: state.tx_decode_failures,
        unique_accounts,
        elapsed_secs: started.elapsed().as_secs_f64(),
        scan_elapsed_secs,
        spool_flush_elapsed_secs,
        reduce_elapsed_secs,
        car_stream_bytes: state.car_stream_bytes,
        car_entries: state.car_entries,
        transaction_entries: state.transaction_entries,
        transaction_payload_bytes: state.transaction_payload_bytes,
        tx_data_bytes: state.tx_data_bytes,
        legacy_txs: state.legacy_txs,
        v0_txs: state.v0_txs,
        message_account_touches: state.message_account_touches,
        lookup_table_account_touches: state.lookup_table_account_touches,
        txs_without_signature: state.txs_without_signature,
        txs_without_signer: state.txs_without_signer,
        temp_touch_records: spool_stats.touch_records,
        spool_flushes: spool_stats.flushes,
        max_pending_accounts: spool_stats.max_pending_accounts,
    };

    write_json(&config.out_dir.join("meta.json"), &summary)?;
    Ok(summary)
}

pub fn dump_accounts_in_dir(config: BatchConfig) -> Result<Vec<BatchResult>> {
    fs::create_dir_all(&config.out_dir)
        .with_context(|| format!("create {}", config.out_dir.display()))?;

    let epoch_cars = discover_epoch_cars(&config.car_dir)?;
    let total = epoch_cars.len();
    eprintln!(
        "of-account-index: discovered {} epoch car files in {}",
        total,
        config.car_dir.display()
    );

    let jobs = config.jobs.max(1).min(total.max(1));
    let work = Arc::new(Mutex::new(
        epoch_cars.into_iter().enumerate().collect::<VecDeque<_>>(),
    ));
    let (tx, rx) = mpsc::channel();

    for worker in 0..jobs {
        let work = Arc::clone(&work);
        let tx = tx.clone();
        let config = config.clone();
        thread::spawn(move || {
            loop {
                let next = {
                    let mut guard = work.lock().expect("work queue lock poisoned");
                    guard.pop_front()
                };
                let Some((index, epoch_car)) = next else {
                    break;
                };

                eprintln!(
                    "of-account-index: worker {} [{}/{}] epoch {} from {}",
                    worker + 1,
                    index + 1,
                    total,
                    epoch_car.epoch,
                    epoch_car.path.display()
                );
                let result = dump_epoch(&config, epoch_car);
                if tx.send((index, result)).is_err() {
                    break;
                }
            }
        });
    }
    drop(tx);

    let mut by_index: Vec<Option<Result<BatchResult>>> = (0..total).map(|_| None).collect();
    for (index, result) in rx {
        by_index[index] = Some(result);
    }

    let mut results = Vec::with_capacity(total);
    for result in by_index {
        results.push(result.context("worker did not return a result")??);
    }
    Ok(results)
}

pub fn merge_first_seen(config: MergeConfig) -> Result<MergeSummary> {
    let started = Instant::now();
    fs::create_dir_all(&config.out_dir)
        .with_context(|| format!("create {}", config.out_dir.display()))?;

    let bucket_count = normalized_bucket_count(config.bucket_count)?;
    let bucket_dir = config.out_dir.join(".tmp-global-account-buckets");
    reset_dir(&bucket_dir)?;

    let epoch_dirs = discover_epoch_dump_dirs(&config.dump_dir)?;
    let mut writers = BucketWriters::new(&bucket_dir, bucket_count)?;
    let mut input_rows = 0usize;

    for (index, epoch_dir) in epoch_dirs.iter().enumerate() {
        eprintln!(
            "of-account-index: merge [{}/{}] {}",
            index + 1,
            epoch_dirs.len(),
            epoch_dir.path.display()
        );
        input_rows += spool_global_first_seen(&epoch_dir.path, epoch_dir.epoch, &mut writers)?;
    }
    let temp_bucket_bytes = writers.flush_all()?;

    let global_first_seen_path = config.out_dir.join("global-first-seen.tsv");
    let mut writer = BufWriter::new(
        File::create(&global_first_seen_path)
            .with_context(|| format!("create {}", global_first_seen_path.display()))?,
    );
    writeln!(
        writer,
        "account\tfirst_epoch\tfirst_slot\tfirst_tx_index\tfirst_signer\tfirst_signature"
    )
    .context("write global first-seen header")?;
    let unique_accounts = reduce_global_buckets(&bucket_dir, bucket_count, &mut writer)?;
    writer.flush().context("flush global-first-seen.tsv")?;
    fs::remove_dir_all(&bucket_dir).with_context(|| format!("remove {}", bucket_dir.display()))?;

    let summary = MergeSummary {
        format_version: FORMAT_VERSION,
        dump_dir: config.dump_dir.display().to_string(),
        global_first_seen_path: global_first_seen_path.display().to_string(),
        bucket_count,
        temp_bucket_bytes,
        epoch_dirs: epoch_dirs.len(),
        input_rows,
        unique_accounts,
        elapsed_secs: started.elapsed().as_secs_f64(),
    };
    write_json(&config.out_dir.join("meta.json"), &summary)?;
    Ok(summary)
}

pub fn profile_car(config: ProfileConfig) -> Result<ProfileSummary> {
    let input_file_bytes = fs::metadata(&config.car_path)
        .with_context(|| format!("stat {}", config.car_path.display()))?
        .len();
    let compressed = is_zstd_path(&config.car_path);
    let started = Instant::now();
    let mut state = ScanState::default();
    let mut copied_bytes = 0u64;
    let mut temp_bucket_bytes = 0u64;
    let mut spool_stats = SpoolStats::default();

    match config.mode {
        ProfileMode::RawRead => {
            let file = File::open(&config.car_path)
                .with_context(|| format!("open {}", config.car_path.display()))?;
            if config.prefetch {
                let reader = ReadAheadReader::new(file, PREFETCH_CHUNK, PREFETCH_DEPTH);
                let reader = BufReader::with_capacity(CAR_BUF, reader);
                copied_bytes = copy_reader_to_sink(reader, config.max_bytes)?;
            } else {
                let reader = BufReader::with_capacity(CAR_BUF, file);
                copied_bytes = copy_reader_to_sink(reader, config.max_bytes)?;
            }
        }
        ProfileMode::DecodeRead => {
            let file = File::open(&config.car_path)
                .with_context(|| format!("open {}", config.car_path.display()))?;
            if compressed {
                if config.prefetch {
                    let reader = ReadAheadReader::new(file, PREFETCH_CHUNK, PREFETCH_DEPTH);
                    let reader = BufReader::with_capacity(CAR_BUF, reader);
                    let decoder = zstd::Decoder::with_buffer(reader)
                        .with_context(|| format!("open {}", config.car_path.display()))?;
                    let reader = BufReader::with_capacity(CAR_BUF, decoder);
                    copied_bytes = copy_reader_to_sink(reader, config.max_bytes)?;
                } else {
                    let reader = BufReader::with_capacity(CAR_BUF, file);
                    let decoder = zstd::Decoder::with_buffer(reader)
                        .with_context(|| format!("open {}", config.car_path.display()))?;
                    let reader = BufReader::with_capacity(CAR_BUF, decoder);
                    copied_bytes = copy_reader_to_sink(reader, config.max_bytes)?;
                }
            } else {
                let reader = BufReader::with_capacity(CAR_BUF, file);
                copied_bytes = copy_reader_to_sink(reader, config.max_bytes)?;
            }
        }
        ProfileMode::CarFraming | ProfileMode::NodeDecode | ProfileMode::TxParse => {
            profile_scan_without_spool(
                &config.car_path,
                compressed,
                config.mode,
                config.max_entries,
                config.max_txs,
                config.prefetch,
                &mut state,
            )?;
        }
        ProfileMode::AccountSpool => {
            let temp_dir = config.temp_dir.unwrap_or_else(default_profile_temp_dir);
            reset_dir(&temp_dir)?;
            let mut spool = AccountSpool::new(&temp_dir, DEFAULT_BUCKET_COUNT, 2_000_000)?;
            profile_scan_with_recorder(
                &config.car_path,
                compressed,
                config.mode,
                config.max_entries,
                config.max_txs,
                config.prefetch,
                &mut state,
                &mut spool,
            )?;
            let finished = spool.finish()?;
            temp_bucket_bytes = finished.0;
            spool_stats = finished.1;
            fs::remove_dir_all(&temp_dir)
                .with_context(|| format!("remove {}", temp_dir.display()))?;
        }
    }

    let elapsed_secs = started.elapsed().as_secs_f64();
    Ok(ProfileSummary {
        mode: config.mode,
        car_path: config.car_path.display().to_string(),
        compressed,
        input_file_bytes,
        elapsed_secs,
        copied_bytes,
        car_stream_bytes: state.car_stream_bytes,
        car_entries: state.car_entries,
        transaction_entries: state.transaction_entries,
        transaction_payload_bytes: state.transaction_payload_bytes,
        tx_data_bytes: state.tx_data_bytes,
        scanned_txs: state.scanned_txs,
        skipped_txs: state.skipped_txs,
        skipped_tx_data_continuations: state.skipped_tx_data_continuations,
        tx_decode_failures: state.tx_decode_failures,
        legacy_txs: state.legacy_txs,
        v0_txs: state.v0_txs,
        message_account_touches: state.message_account_touches,
        lookup_table_account_touches: state.lookup_table_account_touches,
        temp_bucket_bytes,
        temp_touch_records: spool_stats.touch_records,
        spool_flushes: spool_stats.flushes,
        max_pending_accounts: spool_stats.max_pending_accounts,
        input_file_gbps: input_file_bytes as f64 / elapsed_secs / 1e9,
        car_stream_gbps: state.car_stream_bytes as f64 / elapsed_secs / 1e9,
        copied_gbps: copied_bytes as f64 / elapsed_secs / 1e9,
        txs_per_sec: state.scanned_txs as f64 / elapsed_secs,
    })
}

fn dump_epoch(config: &BatchConfig, epoch_car: EpochCar) -> Result<BatchResult> {
    let out_dir = config.out_dir.join(format!("epoch-{}", epoch_car.epoch));

    if config.resume
        && let Some(summary) =
            reusable_dump_summary(&out_dir, &epoch_car.path, config.dump_signatures)?
    {
        eprintln!(
            "of-account-index: epoch {} already dumped at {}, skipping",
            epoch_car.epoch,
            out_dir.display()
        );
        return Ok(BatchResult {
            epoch: epoch_car.epoch,
            car_path: epoch_car.path,
            out_dir,
            unique_accounts: summary.unique_accounts,
            scanned_txs: summary.scanned_txs,
            elapsed_secs: summary.elapsed_secs,
            reused_existing: true,
        });
    }

    let summary = dump_accounts(DumpConfig {
        car_path: epoch_car.path.clone(),
        out_dir: out_dir.clone(),
        bucket_count: config.bucket_count,
        flush_account_threshold: config.flush_account_threshold,
        dump_signatures: config.dump_signatures,
        prefetch: config.prefetch,
    })?;
    eprintln!(
        "of-account-index: epoch {} complete ({} unique accounts from {} txs)",
        epoch_car.epoch, summary.unique_accounts, summary.scanned_txs
    );

    Ok(BatchResult {
        epoch: epoch_car.epoch,
        car_path: epoch_car.path,
        out_dir,
        unique_accounts: summary.unique_accounts,
        scanned_txs: summary.scanned_txs,
        elapsed_secs: summary.elapsed_secs,
        reused_existing: false,
    })
}

fn scan_car(
    path: &Path,
    compressed: bool,
    prefetch: bool,
    state: &mut ScanState,
    account_spool: &mut AccountSpool,
    signature_writer: Option<&mut BufWriter<File>>,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;

    if compressed {
        if prefetch {
            let reader = ReadAheadReader::new(file, PREFETCH_CHUNK, PREFETCH_DEPTH);
            let reader = BufReader::with_capacity(CAR_BUF, reader);
            let decoder = zstd::Decoder::with_buffer(reader)
                .with_context(|| format!("open {}", path.display()))?;
            return visit_reader(decoder, state, account_spool, signature_writer);
        }
        let reader = BufReader::with_capacity(CAR_BUF, file);
        let decoder = zstd::Decoder::with_buffer(reader)
            .with_context(|| format!("open {}", path.display()))?;
        visit_reader(decoder, state, account_spool, signature_writer)
    } else {
        visit_reader(file, state, account_spool, signature_writer)
    }
}

fn visit_reader<R: Read>(
    reader: R,
    state: &mut ScanState,
    account_spool: &mut AccountSpool,
    mut signature_writer: Option<&mut BufWriter<File>>,
) -> Result<()> {
    let mut reader = CarBlockReader::with_capacity(reader, CAR_BUF);
    reader.skip_header().context("skip CAR header")?;
    let mut scratch = Vec::with_capacity(1 << 20);

    while let Some(entry) = reader
        .read_entry_payload_with_scratch(&mut scratch)
        .context("read CAR entry")?
    {
        state.car_stream_bytes = reader.offset;
        state.car_entries += 1;

        if !is_transaction_node(entry.payload) {
            continue;
        }

        state.transaction_entries += 1;
        state.transaction_payload_bytes = state
            .transaction_payload_bytes
            .saturating_add(entry.payload_len as u64);

        let Node::Transaction(tx) =
            decode_node(entry.payload).context("decode transaction node")?
        else {
            continue;
        };
        state.scanned_txs += 1;

        if tx.data.next.is_some() {
            state.skipped_txs += 1;
            state.skipped_tx_data_continuations += 1;
            continue;
        }
        state.tx_data_bytes = state
            .tx_data_bytes
            .saturating_add(tx.data.data.len() as u64);

        if collect_transaction_accounts_from_bytes(
            tx.data.data,
            tx.slot,
            tx.index,
            entry.location.car_offset,
            entry.total_len,
            state,
            account_spool,
            signature_writer.as_deref_mut(),
        )
        .is_err()
        {
            state.skipped_txs += 1;
            state.tx_decode_failures += 1;
            continue;
        }
    }

    Ok(())
}

fn copy_reader_to_sink<R: Read>(reader: R, max_bytes: Option<u64>) -> Result<u64> {
    let mut sink = io::sink();
    match max_bytes {
        Some(max_bytes) => {
            let mut reader = reader.take(max_bytes);
            io::copy(&mut reader, &mut sink).context("copy profile bytes")
        }
        None => {
            let mut reader = reader;
            io::copy(&mut reader, &mut sink).context("copy profile bytes")
        }
    }
}

fn profile_scan_without_spool(
    path: &Path,
    compressed: bool,
    mode: ProfileMode,
    max_entries: Option<usize>,
    max_txs: Option<usize>,
    prefetch: bool,
    state: &mut ScanState,
) -> Result<()> {
    let mut recorder = NullAccountRecorder;
    profile_scan_with_recorder(
        path,
        compressed,
        mode,
        max_entries,
        max_txs,
        prefetch,
        state,
        &mut recorder,
    )
}

fn profile_scan_with_recorder<R: AccountRecorder>(
    path: &Path,
    compressed: bool,
    mode: ProfileMode,
    max_entries: Option<usize>,
    max_txs: Option<usize>,
    prefetch: bool,
    state: &mut ScanState,
    recorder: &mut R,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    if compressed {
        if prefetch {
            let reader = ReadAheadReader::new(file, PREFETCH_CHUNK, PREFETCH_DEPTH);
            let reader = BufReader::with_capacity(CAR_BUF, reader);
            let decoder = zstd::Decoder::with_buffer(reader)
                .with_context(|| format!("open {}", path.display()))?;
            return profile_visit_reader(decoder, mode, max_entries, max_txs, state, recorder);
        }
        let reader = BufReader::with_capacity(CAR_BUF, file);
        let decoder = zstd::Decoder::with_buffer(reader)
            .with_context(|| format!("open {}", path.display()))?;
        profile_visit_reader(decoder, mode, max_entries, max_txs, state, recorder)
    } else {
        profile_visit_reader(file, mode, max_entries, max_txs, state, recorder)
    }
}

fn profile_visit_reader<R: Read, A: AccountRecorder>(
    reader: R,
    mode: ProfileMode,
    max_entries: Option<usize>,
    max_txs: Option<usize>,
    state: &mut ScanState,
    recorder: &mut A,
) -> Result<()> {
    let mut reader = CarBlockReader::with_capacity(reader, CAR_BUF);
    reader.skip_header().context("skip CAR header")?;
    let mut scratch = Vec::with_capacity(1 << 20);

    loop {
        if max_entries.is_some_and(|max_entries| state.car_entries >= max_entries) {
            break;
        }
        if max_txs.is_some_and(|max_txs| state.scanned_txs >= max_txs) {
            break;
        }
        let Some(entry) = reader
            .read_entry_payload_with_scratch(&mut scratch)
            .context("read CAR entry")?
        else {
            break;
        };

        state.car_stream_bytes = reader.offset;
        state.car_entries += 1;

        if mode == ProfileMode::CarFraming {
            continue;
        }

        if !is_transaction_node(entry.payload) {
            continue;
        }

        state.transaction_entries += 1;
        state.transaction_payload_bytes = state
            .transaction_payload_bytes
            .saturating_add(entry.payload_len as u64);

        let Node::Transaction(tx) =
            decode_node(entry.payload).context("decode transaction node")?
        else {
            continue;
        };
        state.scanned_txs += 1;

        if tx.data.next.is_some() {
            state.skipped_txs += 1;
            state.skipped_tx_data_continuations += 1;
            continue;
        }
        state.tx_data_bytes = state
            .tx_data_bytes
            .saturating_add(tx.data.data.len() as u64);

        if mode == ProfileMode::NodeDecode {
            continue;
        }

        if collect_transaction_accounts_from_bytes(
            tx.data.data,
            tx.slot,
            tx.index,
            entry.location.car_offset,
            entry.total_len,
            state,
            recorder,
            None,
        )
        .is_err()
        {
            state.skipped_txs += 1;
            state.tx_decode_failures += 1;
        }
    }

    Ok(())
}

fn collect_transaction_accounts_from_bytes(
    tx_data: &[u8],
    slot: u64,
    tx_index: Option<u64>,
    car_offset: u64,
    car_size: usize,
    state: &mut ScanState,
    account_recorder: &mut impl AccountRecorder,
    signature_writer: Option<&mut BufWriter<File>>,
) -> Result<()> {
    let mut cursor = TxCursor::new(tx_data);
    let signature_count = cursor.read_short_len()?;
    let signature = if signature_count == 0 {
        state.txs_without_signature += 1;
        None
    } else {
        let signature = *cursor.take_array::<64>()?;
        cursor.skip(
            signature_count
                .checked_sub(1)
                .and_then(|count| count.checked_mul(64))
                .context("signature byte count overflow")?,
        )?;
        Some(signature)
    };

    let Some(signature) = signature else {
        return Ok(());
    };

    if let Some(writer) = signature_writer {
        write_signature_record(writer, &signature, slot, tx_index, car_offset, car_size)?;
    }

    let first = cursor.read_u8()?;
    if first & MESSAGE_VERSION_PREFIX != 0 {
        let version = first & !MESSAGE_VERSION_PREFIX;
        if version != 0 {
            bail!("unsupported transaction message version {version}");
        }
        state.v0_txs += 1;
        let header = MessageHeader {
            num_required_signatures: cursor.read_u8()?,
            num_readonly_signed_accounts: cursor.read_u8()?,
            num_readonly_unsigned_accounts: cursor.read_u8()?,
        };
        let first_seen = collect_compiled_message_accounts(
            &mut cursor,
            header,
            signature,
            slot,
            tx_index,
            state,
            account_recorder,
        )?;
        skip_compiled_instructions(&mut cursor)?;
        if let Some(first_seen) = first_seen {
            collect_address_table_lookup_accounts(
                &mut cursor,
                first_seen,
                state,
                account_recorder,
            )?;
        } else {
            skip_address_table_lookups(&mut cursor)?;
        }
    } else {
        state.legacy_txs += 1;
        let header = MessageHeader {
            num_required_signatures: first,
            num_readonly_signed_accounts: cursor.read_u8()?,
            num_readonly_unsigned_accounts: cursor.read_u8()?,
        };
        collect_compiled_message_accounts(
            &mut cursor,
            header,
            signature,
            slot,
            tx_index,
            state,
            account_recorder,
        )?;
        skip_compiled_instructions(&mut cursor)?;
    }

    if !cursor.is_empty() {
        bail!("transaction has {} trailing bytes", cursor.remaining());
    }

    Ok(())
}

fn collect_compiled_message_accounts(
    cursor: &mut TxCursor<'_>,
    header: MessageHeader,
    signature: [u8; 64],
    slot: u64,
    tx_index: Option<u64>,
    state: &mut ScanState,
    account_recorder: &mut impl AccountRecorder,
) -> Result<Option<FirstSeen>> {
    let account_count = cursor.read_short_len()?;
    if header.num_required_signatures == 0 || account_count == 0 {
        state.txs_without_signer += 1;
        cursor.skip(
            account_count
                .checked_mul(32)
                .context("account key byte count overflow")?,
        )?;
        cursor.skip(32)?;
        return Ok(None);
    }

    let signer = *cursor.take_array::<32>()?;
    let first_seen = FirstSeen {
        slot,
        tx_index,
        signer,
        signature,
    };
    account_recorder.record_account(signer, first_seen)?;
    for _ in 1..account_count {
        account_recorder.record_account(*cursor.take_array::<32>()?, first_seen)?;
    }
    state.message_account_touches += account_count;
    cursor.skip(32)?;
    Ok(Some(first_seen))
}

fn skip_compiled_instructions(cursor: &mut TxCursor<'_>) -> Result<()> {
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

fn collect_address_table_lookup_accounts(
    cursor: &mut TxCursor<'_>,
    first_seen: FirstSeen,
    state: &mut ScanState,
    account_recorder: &mut impl AccountRecorder,
) -> Result<()> {
    let lookup_count = cursor.read_short_len()?;
    state.lookup_table_account_touches += lookup_count;
    for _ in 0..lookup_count {
        account_recorder.record_account(*cursor.take_array::<32>()?, first_seen)?;
        let writable_count = cursor.read_short_len()?;
        cursor.skip(writable_count)?;
        let readonly_count = cursor.read_short_len()?;
        cursor.skip(readonly_count)?;
    }
    Ok(())
}

fn skip_address_table_lookups(cursor: &mut TxCursor<'_>) -> Result<()> {
    let lookup_count = cursor.read_short_len()?;
    for _ in 0..lookup_count {
        cursor.skip(32)?;
        let writable_count = cursor.read_short_len()?;
        cursor.skip(writable_count)?;
        let readonly_count = cursor.read_short_len()?;
        cursor.skip(readonly_count)?;
    }
    Ok(())
}

struct TxCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> TxCursor<'a> {
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
            .map_err(|()| anyhow::anyhow!("decode short vec len"))?;
        self.position += used;
        Ok(len)
    }

    fn take_array<const N: usize>(&mut self) -> Result<&'a [u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid fixed array length"))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .context("transaction cursor overflow")?;
        if end > self.bytes.len() {
            bail!(
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

enum ReadAheadChunk {
    Data(Vec<u8>),
    Eof,
    Err(io::Error),
}

struct ReadAheadReader {
    rx: mpsc::Receiver<ReadAheadChunk>,
    current: Vec<u8>,
    position: usize,
    done: bool,
}

impl ReadAheadReader {
    fn new<R>(mut reader: R, chunk_size: usize, depth: usize) -> Self
    where
        R: Read + Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel(depth.max(1));
        thread::spawn(move || {
            loop {
                let mut chunk = vec![0u8; chunk_size.max(1)];
                match reader.read(&mut chunk) {
                    Ok(0) => {
                        let _ = tx.send(ReadAheadChunk::Eof);
                        break;
                    }
                    Ok(read) => {
                        chunk.truncate(read);
                        if tx.send(ReadAheadChunk::Data(chunk)).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(ReadAheadChunk::Err(err));
                        break;
                    }
                }
            }
        });

        Self {
            rx,
            current: Vec::new(),
            position: 0,
            done: false,
        }
    }
}

impl Read for ReadAheadReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }

        loop {
            if self.position < self.current.len() {
                let available = self.current.len() - self.position;
                let to_copy = available.min(out.len());
                out[..to_copy]
                    .copy_from_slice(&self.current[self.position..self.position + to_copy]);
                self.position += to_copy;
                return Ok(to_copy);
            }

            if self.done {
                return Ok(0);
            }

            match self.rx.recv() {
                Ok(ReadAheadChunk::Data(chunk)) => {
                    self.current = chunk;
                    self.position = 0;
                }
                Ok(ReadAheadChunk::Eof) => {
                    self.done = true;
                    return Ok(0);
                }
                Ok(ReadAheadChunk::Err(err)) => {
                    self.done = true;
                    return Err(err);
                }
                Err(_) => {
                    self.done = true;
                    return Ok(0);
                }
            }
        }
    }
}

#[cfg(test)]
fn collect_message_accounts(
    tx: &VersionedTransaction<'_>,
    slot: u64,
    tx_index: Option<u64>,
    state: &mut ScanState,
    account_spool: &mut AccountSpool,
) -> Result<()> {
    let Some(signature) = tx.signatures.first().map(|signature| **signature) else {
        state.txs_without_signature += 1;
        return Ok(());
    };

    match &tx.message {
        VersionedMessage::Legacy(message) => {
            state.legacy_txs += 1;
            let Some(signer) = first_signer(&message.header, &message.account_keys) else {
                state.txs_without_signer += 1;
                return Ok(());
            };
            let first_seen = FirstSeen {
                slot,
                tx_index,
                signer,
                signature,
            };
            state.message_account_touches += message.account_keys.len();
            for key in &message.account_keys {
                account_spool.record(**key, first_seen)?;
            }
        }
        VersionedMessage::V0(message) => {
            state.v0_txs += 1;
            let Some(signer) = first_signer(&message.header, &message.account_keys) else {
                state.txs_without_signer += 1;
                return Ok(());
            };
            let first_seen = FirstSeen {
                slot,
                tx_index,
                signer,
                signature,
            };
            state.message_account_touches += message.account_keys.len();
            for key in &message.account_keys {
                account_spool.record(**key, first_seen)?;
            }
            state.lookup_table_account_touches += message.address_table_lookups.len();
            for lookup in &message.address_table_lookups {
                account_spool.record(*lookup.account_key, first_seen)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
fn first_signer(header: &MessageHeader, account_keys: &[&[u8; 32]]) -> Option<[u8; 32]> {
    if header.num_required_signatures == 0 {
        return None;
    }
    account_keys.first().map(|key| **key)
}

fn write_signature_record(
    writer: &mut BufWriter<File>,
    signature: &[u8; 64],
    slot: u64,
    tx_index: Option<u64>,
    car_offset: u64,
    car_size: usize,
) -> Result<()> {
    let mut out = [0u8; SIGNATURE_RECORD_BYTES];
    out[0..64].copy_from_slice(signature);
    out[64..72].copy_from_slice(&slot.to_le_bytes());
    out[72..80].copy_from_slice(&tx_index.unwrap_or(NO_TX_INDEX).to_le_bytes());
    out[80..88].copy_from_slice(&car_offset.to_le_bytes());
    out[88..96].copy_from_slice(&(car_size as u64).to_le_bytes());
    writer.write_all(&out).context("write signature record")
}

struct BucketWriters {
    dir: PathBuf,
    bucket_count: usize,
    writers: Vec<BufWriter<File>>,
}

impl BucketWriters {
    fn new(dir: &Path, bucket_count: usize) -> Result<Self> {
        let mut writers = Vec::with_capacity(bucket_count);
        for bucket in 0..bucket_count {
            let path = bucket_path(dir, bucket);
            let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
            writers.push(BufWriter::with_capacity(BUCKET_WRITER_BUF, file));
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            bucket_count,
            writers,
        })
    }

    fn write_touch(&mut self, account: [u8; 32], first_seen: FirstSeen) -> Result<()> {
        let bucket = bucket_for_account(&account, self.bucket_count);
        let record = encode_touch_record(TouchRecord {
            account,
            first_seen,
        });
        self.writers[bucket]
            .write_all(&record)
            .with_context(|| format!("write {}", bucket_path(&self.dir, bucket).display()))
    }

    fn write_global(&mut self, account: [u8; 32], first_seen: GlobalFirstSeen) -> Result<()> {
        let bucket = bucket_for_account(&account, self.bucket_count);
        let record = encode_global_record(GlobalRecord {
            account,
            first_seen,
        });
        self.writers[bucket]
            .write_all(&record)
            .with_context(|| format!("write {}", bucket_path(&self.dir, bucket).display()))
    }

    fn flush_all(&mut self) -> Result<u64> {
        let mut bytes = 0u64;
        for (bucket, writer) in self.writers.iter_mut().enumerate() {
            writer
                .flush()
                .with_context(|| format!("flush {}", bucket_path(&self.dir, bucket).display()))?;
            let path = bucket_path(&self.dir, bucket);
            bytes = bytes
                .checked_add(
                    fs::metadata(&path)
                        .with_context(|| format!("stat {}", path.display()))?
                        .len(),
                )
                .context("bucket byte count overflow")?;
        }
        Ok(bytes)
    }
}

struct AccountSpool {
    pending: HashMap<[u8; 32], FirstSeen>,
    writers: BucketWriters,
    flush_threshold: usize,
    stats: SpoolStats,
}

trait AccountRecorder {
    fn record_account(&mut self, account: [u8; 32], first_seen: FirstSeen) -> Result<()>;
}

struct NullAccountRecorder;

impl AccountRecorder for NullAccountRecorder {
    fn record_account(&mut self, _account: [u8; 32], _first_seen: FirstSeen) -> Result<()> {
        Ok(())
    }
}

impl AccountRecorder for AccountSpool {
    fn record_account(&mut self, account: [u8; 32], first_seen: FirstSeen) -> Result<()> {
        self.record(account, first_seen)
    }
}

impl AccountSpool {
    fn new(dir: &Path, bucket_count: usize, flush_threshold: usize) -> Result<Self> {
        Ok(Self {
            pending: HashMap::with_capacity(flush_threshold.min(64_000)),
            writers: BucketWriters::new(dir, bucket_count)?,
            flush_threshold,
            stats: SpoolStats::default(),
        })
    }

    fn record(&mut self, account: [u8; 32], first_seen: FirstSeen) -> Result<()> {
        self.pending.entry(account).or_insert(first_seen);
        self.stats.max_pending_accounts = self.stats.max_pending_accounts.max(self.pending.len());
        if self.pending.len() >= self.flush_threshold {
            self.flush_pending()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(u64, SpoolStats)> {
        self.flush_pending()?;
        let bytes = self.writers.flush_all()?;
        Ok((bytes, self.stats))
    }

    fn flush_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.stats.flushes += 1;
        self.stats.touch_records += self.pending.len();
        for (account, first_seen) in self.pending.drain() {
            self.writers.write_touch(account, first_seen)?;
        }
        Ok(())
    }
}

fn reduce_epoch_buckets(
    bucket_dir: &Path,
    bucket_count: usize,
    accounts_writer: &mut BufWriter<File>,
    first_seen_writer: &mut BufWriter<File>,
) -> Result<usize> {
    let mut unique_accounts = 0usize;
    for bucket in 0..bucket_count {
        let path = bucket_path(bucket_dir, bucket);
        let mut first_seen_by_account = HashMap::new();
        read_epoch_bucket(&path, &mut first_seen_by_account)?;

        let mut entries = first_seen_by_account.into_iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(account, _)| *account);
        for (account, first_seen) in entries {
            let account = bs58::encode(account).into_string();
            writeln!(accounts_writer, "{account}").context("write account")?;
            writeln!(
                first_seen_writer,
                "{}\t{}\t{}\t{}\t{}",
                account,
                first_seen.slot,
                first_seen
                    .tx_index
                    .map(|index| index.to_string())
                    .unwrap_or_default(),
                bs58::encode(first_seen.signer).into_string(),
                bs58::encode(first_seen.signature).into_string()
            )
            .context("write first-seen row")?;
            unique_accounts += 1;
        }
    }
    Ok(unique_accounts)
}

fn reduce_global_buckets(
    bucket_dir: &Path,
    bucket_count: usize,
    writer: &mut BufWriter<File>,
) -> Result<usize> {
    let mut unique_accounts = 0usize;
    for bucket in 0..bucket_count {
        let path = bucket_path(bucket_dir, bucket);
        let mut first_seen_by_account = HashMap::new();
        read_global_bucket(&path, &mut first_seen_by_account)?;

        let mut entries = first_seen_by_account.into_iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(account, _)| *account);
        for (account, first_seen) in entries {
            let account = bs58::encode(account).into_string();
            writeln!(
                writer,
                "{}\t{}\t{}\t{}\t{}\t{}",
                account,
                first_seen.epoch,
                first_seen.first_seen.slot,
                first_seen
                    .first_seen
                    .tx_index
                    .map(|index| index.to_string())
                    .unwrap_or_default(),
                bs58::encode(first_seen.first_seen.signer).into_string(),
                bs58::encode(first_seen.first_seen.signature).into_string()
            )
            .context("write global first-seen row")?;
            unique_accounts += 1;
        }
    }
    Ok(unique_accounts)
}

fn read_epoch_bucket(
    path: &Path,
    first_seen_by_account: &mut HashMap<[u8; 32], FirstSeen>,
) -> Result<()> {
    validate_record_file(path, TOUCH_RECORD_BYTES)?;
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(CAR_BUF.min(8 << 20), file);
    let mut bytes = [0u8; TOUCH_RECORD_BYTES];

    loop {
        match reader.read_exact(&mut bytes) {
            Ok(()) => {
                let record = decode_touch_record(&bytes);
                first_seen_by_account
                    .entry(record.account)
                    .or_insert(record.first_seen);
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
        }
    }
    Ok(())
}

fn read_global_bucket(
    path: &Path,
    first_seen_by_account: &mut HashMap<[u8; 32], GlobalFirstSeen>,
) -> Result<()> {
    validate_record_file(path, GLOBAL_RECORD_BYTES)?;
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(CAR_BUF.min(8 << 20), file);
    let mut bytes = [0u8; GLOBAL_RECORD_BYTES];

    loop {
        match reader.read_exact(&mut bytes) {
            Ok(()) => {
                let record = decode_global_record(&bytes);
                first_seen_by_account
                    .entry(record.account)
                    .or_insert(record.first_seen);
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
        }
    }
    Ok(())
}

fn validate_record_file(path: &Path, record_bytes: usize) -> Result<()> {
    let len = fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    if len % record_bytes as u64 != 0 {
        bail!(
            "{} has partial records: {} is not a multiple of {}",
            path.display(),
            len,
            record_bytes
        );
    }
    Ok(())
}

fn spool_global_first_seen(
    epoch_dir: &Path,
    epoch: u64,
    writers: &mut BucketWriters,
) -> Result<usize> {
    let path = epoch_dir.join("first-seen.tsv");
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::with_capacity(8 << 20, file);
    let mut rows = 0usize;

    for (line_index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        if line_index == 0 {
            continue;
        }
        if line.is_empty() {
            continue;
        }

        let mut fields = line.split('\t');
        let account = parse_pubkey_field(fields.next(), "account", &path, line_index + 1)?;
        let slot = parse_u64_field(fields.next(), "first_slot", &path, line_index + 1)?;
        let tx_index =
            parse_optional_u64_field(fields.next(), "first_tx_index", &path, line_index + 1)?;
        let signer = parse_pubkey_field(fields.next(), "first_signer", &path, line_index + 1)?;
        let signature =
            parse_signature_field(fields.next(), "first_signature", &path, line_index + 1)?;

        writers.write_global(
            account,
            GlobalFirstSeen {
                epoch,
                first_seen: FirstSeen {
                    slot,
                    tx_index,
                    signer,
                    signature,
                },
            },
        )?;
        rows += 1;
    }

    Ok(rows)
}

fn parse_pubkey_field(
    value: Option<&str>,
    name: &str,
    path: &Path,
    line: usize,
) -> Result<[u8; 32]> {
    let value =
        value.ok_or_else(|| anyhow::anyhow!("missing {name} at {}:{line}", path.display()))?;
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode {name} at {}:{line}", path.display()))?;
    if bytes.len() != 32 {
        bail!(
            "{} at {}:{} decoded to {} bytes, expected 32",
            name,
            path.display(),
            line,
            bytes.len()
        );
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn parse_signature_field(
    value: Option<&str>,
    name: &str,
    path: &Path,
    line: usize,
) -> Result<[u8; 64]> {
    let value =
        value.ok_or_else(|| anyhow::anyhow!("missing {name} at {}:{line}", path.display()))?;
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode {name} at {}:{line}", path.display()))?;
    if bytes.len() != 64 {
        bail!(
            "{} at {}:{} decoded to {} bytes, expected 64",
            name,
            path.display(),
            line,
            bytes.len()
        );
    }
    let mut out = [0u8; 64];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn parse_u64_field(value: Option<&str>, name: &str, path: &Path, line: usize) -> Result<u64> {
    let value =
        value.ok_or_else(|| anyhow::anyhow!("missing {name} at {}:{line}", path.display()))?;
    value
        .parse::<u64>()
        .with_context(|| format!("parse {name} at {}:{line}", path.display()))
}

fn parse_optional_u64_field(
    value: Option<&str>,
    name: &str,
    path: &Path,
    line: usize,
) -> Result<Option<u64>> {
    let Some(value) = value else {
        bail!("missing {name} at {}:{line}", path.display());
    };
    if value.is_empty() {
        Ok(None)
    } else {
        value
            .parse::<u64>()
            .map(Some)
            .with_context(|| format!("parse {name} at {}:{line}", path.display()))
    }
}

fn encode_touch_record(record: TouchRecord) -> [u8; TOUCH_RECORD_BYTES] {
    let mut out = [0u8; TOUCH_RECORD_BYTES];
    out[0..32].copy_from_slice(&record.account);
    out[32..40].copy_from_slice(&record.first_seen.slot.to_le_bytes());
    out[40..48].copy_from_slice(
        &record
            .first_seen
            .tx_index
            .unwrap_or(NO_TX_INDEX)
            .to_le_bytes(),
    );
    out[48..80].copy_from_slice(&record.first_seen.signer);
    out[80..144].copy_from_slice(&record.first_seen.signature);
    out
}

fn decode_touch_record(bytes: &[u8; TOUCH_RECORD_BYTES]) -> TouchRecord {
    let mut account = [0u8; 32];
    account.copy_from_slice(&bytes[0..32]);
    let slot = u64::from_le_bytes(bytes[32..40].try_into().expect("slot range"));
    let raw_tx_index = u64::from_le_bytes(bytes[40..48].try_into().expect("tx index range"));
    let mut signer = [0u8; 32];
    signer.copy_from_slice(&bytes[48..80]);
    let mut signature = [0u8; 64];
    signature.copy_from_slice(&bytes[80..144]);
    TouchRecord {
        account,
        first_seen: FirstSeen {
            slot,
            tx_index: (raw_tx_index != NO_TX_INDEX).then_some(raw_tx_index),
            signer,
            signature,
        },
    }
}

fn encode_global_record(record: GlobalRecord) -> [u8; GLOBAL_RECORD_BYTES] {
    let mut out = [0u8; GLOBAL_RECORD_BYTES];
    out[0..32].copy_from_slice(&record.account);
    out[32..40].copy_from_slice(&record.first_seen.epoch.to_le_bytes());
    out[40..48].copy_from_slice(&record.first_seen.first_seen.slot.to_le_bytes());
    out[48..56].copy_from_slice(
        &record
            .first_seen
            .first_seen
            .tx_index
            .unwrap_or(NO_TX_INDEX)
            .to_le_bytes(),
    );
    out[56..88].copy_from_slice(&record.first_seen.first_seen.signer);
    out[88..152].copy_from_slice(&record.first_seen.first_seen.signature);
    out
}

fn decode_global_record(bytes: &[u8; GLOBAL_RECORD_BYTES]) -> GlobalRecord {
    let mut account = [0u8; 32];
    account.copy_from_slice(&bytes[0..32]);
    let epoch = u64::from_le_bytes(bytes[32..40].try_into().expect("epoch range"));
    let slot = u64::from_le_bytes(bytes[40..48].try_into().expect("slot range"));
    let raw_tx_index = u64::from_le_bytes(bytes[48..56].try_into().expect("tx index range"));
    let mut signer = [0u8; 32];
    signer.copy_from_slice(&bytes[56..88]);
    let mut signature = [0u8; 64];
    signature.copy_from_slice(&bytes[88..152]);
    GlobalRecord {
        account,
        first_seen: GlobalFirstSeen {
            epoch,
            first_seen: FirstSeen {
                slot,
                tx_index: (raw_tx_index != NO_TX_INDEX).then_some(raw_tx_index),
                signer,
                signature,
            },
        },
    }
}

#[derive(Debug, Clone)]
struct EpochCar {
    epoch: u64,
    path: PathBuf,
    compressed: bool,
}

#[derive(Debug, Clone)]
struct EpochDumpDir {
    epoch: u64,
    path: PathBuf,
}

fn discover_epoch_cars(car_dir: &Path) -> Result<Vec<EpochCar>> {
    let mut cars_by_epoch: BTreeMap<u64, EpochCar> = BTreeMap::new();
    for entry in fs::read_dir(car_dir).with_context(|| format!("read {}", car_dir.display()))? {
        let entry = entry.with_context(|| format!("read entry in {}", car_dir.display()))?;
        if !entry.path().is_file() {
            continue;
        }

        let path = entry.path();
        let Some(epoch) = epoch_from_car_path(&path) else {
            continue;
        };
        let candidate = EpochCar {
            epoch,
            compressed: is_zstd_path(&path),
            path,
        };
        match cars_by_epoch.get(&epoch) {
            Some(existing) if existing.compressed && !candidate.compressed => {}
            _ => {
                cars_by_epoch.insert(epoch, candidate);
            }
        }
    }

    if cars_by_epoch.is_empty() {
        bail!(
            "no epoch car files found in {} (expected epoch-N.car or epoch-N.car.zst)",
            car_dir.display()
        );
    }
    Ok(cars_by_epoch.into_values().collect())
}

fn discover_epoch_dump_dirs(dump_dir: &Path) -> Result<Vec<EpochDumpDir>> {
    let mut dirs = Vec::new();
    for entry in fs::read_dir(dump_dir).with_context(|| format!("read {}", dump_dir.display()))? {
        let entry = entry.with_context(|| format!("read entry in {}", dump_dir.display()))?;
        if !entry.path().is_dir() {
            continue;
        }
        let path = entry.path();
        let Some(epoch) = epoch_from_dir_path(&path) else {
            continue;
        };
        if path.join("first-seen.tsv").is_file() {
            dirs.push(EpochDumpDir { epoch, path });
        }
    }
    dirs.sort_unstable_by_key(|dir| dir.epoch);
    if dirs.is_empty() {
        bail!(
            "no epoch-* directories with first-seen.tsv found in {}",
            dump_dir.display()
        );
    }
    Ok(dirs)
}

fn reusable_dump_summary(
    out_dir: &Path,
    car_path: &Path,
    dump_signatures: bool,
) -> Result<Option<DumpSummary>> {
    if !out_dir.exists() {
        return Ok(None);
    }

    let meta_path = out_dir.join("meta.json");
    let summary = match fs::read(&meta_path)
        .with_context(|| format!("read {}", meta_path.display()))
        .and_then(|bytes| serde_json::from_slice::<DumpSummary>(&bytes).context("parse meta.json"))
    {
        Ok(summary) => summary,
        Err(err) => {
            eprintln!(
                "of-account-index: existing dump at {} is incomplete or unreadable ({}), rebuilding",
                out_dir.display(),
                err
            );
            return Ok(None);
        }
    };

    if summary.car_path != car_path.display().to_string() {
        eprintln!(
            "of-account-index: existing dump at {} was built from {}, expected {}, rebuilding",
            out_dir.display(),
            summary.car_path,
            car_path.display()
        );
        return Ok(None);
    }

    for filename in ["accounts.txt", "first-seen.tsv"] {
        let path = out_dir.join(filename);
        if !path.is_file() {
            eprintln!(
                "of-account-index: existing dump at {} is missing {}, rebuilding",
                out_dir.display(),
                path.display()
            );
            return Ok(None);
        }
    }

    if dump_signatures {
        let Some(signatures_path) = summary.signatures_path.as_ref() else {
            eprintln!(
                "of-account-index: existing dump at {} has no signature index, rebuilding",
                out_dir.display()
            );
            return Ok(None);
        };
        let path = Path::new(signatures_path);
        if !path.is_file() {
            eprintln!(
                "of-account-index: existing dump at {} is missing signature index {}, rebuilding",
                out_dir.display(),
                path.display()
            );
            return Ok(None);
        }
    }

    Ok(Some(summary))
}

fn epoch_from_car_path(path: &Path) -> Option<u64> {
    let filename = path.file_name()?.to_str()?;
    let rest = filename.strip_prefix("epoch-")?;
    let epoch = rest
        .strip_suffix(".car")
        .or_else(|| rest.strip_suffix(".car.zst"))?;
    epoch.parse().ok()
}

fn epoch_from_dir_path(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .strip_prefix("epoch-")?
        .parse()
        .ok()
}

fn is_zstd_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| matches!(ext, "zst" | "zstd"))
        .unwrap_or(false)
}

fn normalized_bucket_count(bucket_count: usize) -> Result<usize> {
    let bucket_count = if bucket_count == 0 {
        DEFAULT_BUCKET_COUNT
    } else {
        bucket_count
    };
    if !bucket_count.is_power_of_two() {
        bail!("bucket count must be a power of two, got {}", bucket_count);
    }
    if bucket_count > MAX_BUCKET_COUNT {
        bail!(
            "bucket count must be <= {}, got {}",
            MAX_BUCKET_COUNT,
            bucket_count
        );
    }
    Ok(bucket_count)
}

fn normalized_flush_account_threshold(flush_account_threshold: usize) -> Result<usize> {
    Ok(if flush_account_threshold == 0 {
        DEFAULT_FLUSH_ACCOUNT_THRESHOLD
    } else {
        flush_account_threshold
    })
}

fn bucket_for_account(account: &[u8; 32], bucket_count: usize) -> usize {
    let prefix = u16::from_be_bytes([account[0], account[1]]) as usize;
    let bits = bucket_count.trailing_zeros() as usize;
    prefix >> (16 - bits)
}

fn bucket_path(dir: &Path, bucket: usize) -> PathBuf {
    dir.join(format!("bucket-{bucket:05}.bin"))
}

fn reset_dir(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path).with_context(|| format!("remove {}", path.display()))?;
    }
    fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))
}

fn default_profile_temp_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "of-account-index-profile-{}-{}",
        std::process::id(),
        nanos
    ))
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    fs::write(
        path,
        serde_json::to_vec_pretty(value)
            .with_context(|| format!("serialize {}", path.display()))?,
    )
    .with_context(|| format!("write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{
        AccountSpool, FirstSeen, ScanState, bucket_path, collect_message_accounts,
        collect_transaction_accounts_from_bytes, read_epoch_bucket,
    };
    use of_car_reader::versioned_transaction::{
        LegacyMessage, MessageAddressTableLookup, MessageHeader, V0Message, VersionedMessage,
        VersionedTransaction,
    };
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn collects_legacy_static_account_keys() {
        let payer = &[1u8; 32];
        let program = &[2u8; 32];
        let tx = VersionedTransaction {
            signatures: vec![&[9u8; 64]],
            message: VersionedMessage::Legacy(LegacyMessage {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![payer, program],
                recent_blockhash: &[7u8; 32],
                instructions: vec![],
            }),
        };

        let accounts = collect_for_test(&tx, 42, Some(7));
        assert_eq!(accounts.len(), 2);
        assert!(accounts.contains_key(&[1u8; 32]));
        assert!(accounts.contains_key(&[2u8; 32]));
        let first_seen = accounts.get(&[2u8; 32]).unwrap();
        assert_eq!(first_seen.slot, 42);
        assert_eq!(first_seen.tx_index, Some(7));
        assert_eq!(first_seen.signer, [1u8; 32]);
        assert_eq!(first_seen.signature, [9u8; 64]);
    }

    #[test]
    fn collects_v0_static_keys_and_lookup_table_accounts() {
        let payer = &[1u8; 32];
        let lookup_account = &[3u8; 32];
        let tx = VersionedTransaction {
            signatures: vec![&[9u8; 64]],
            message: VersionedMessage::V0(V0Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![payer],
                recent_blockhash: &[7u8; 32],
                instructions: vec![],
                address_table_lookups: vec![MessageAddressTableLookup {
                    account_key: lookup_account,
                    writable_indexes: vec![0],
                    readonly_indexes: vec![1],
                }],
            }),
        };

        let accounts = collect_for_test(&tx, 43, None);
        assert_eq!(accounts.len(), 2);
        assert!(accounts.contains_key(&[1u8; 32]));
        assert!(accounts.contains_key(&[3u8; 32]));
        let first_seen = accounts.get(&[3u8; 32]).unwrap();
        assert_eq!(first_seen.slot, 43);
        assert_eq!(first_seen.tx_index, None);
        assert_eq!(first_seen.signer, [1u8; 32]);
    }

    #[test]
    fn keeps_earliest_signer_for_repeated_account() {
        let first_payer = &[1u8; 32];
        let second_payer = &[2u8; 32];
        let account = &[3u8; 32];

        let first_tx = VersionedTransaction {
            signatures: vec![&[8u8; 64]],
            message: VersionedMessage::Legacy(LegacyMessage {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![first_payer, account],
                recent_blockhash: &[7u8; 32],
                instructions: vec![],
            }),
        };
        let second_tx = VersionedTransaction {
            signatures: vec![&[9u8; 64]],
            message: VersionedMessage::Legacy(LegacyMessage {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![second_payer, account],
                recent_blockhash: &[7u8; 32],
                instructions: vec![],
            }),
        };

        let dir = tempdir().unwrap();
        let mut spool = AccountSpool::new(dir.path(), 1, 1024).unwrap();
        let mut state = ScanState::default();
        collect_message_accounts(&first_tx, 42, Some(7), &mut state, &mut spool).unwrap();
        collect_message_accounts(&second_tx, 43, Some(8), &mut state, &mut spool).unwrap();
        spool.finish().unwrap();
        let mut accounts = HashMap::new();
        read_epoch_bucket(&bucket_path(dir.path(), 0), &mut accounts).unwrap();

        let first_seen = accounts.get(&[3u8; 32]).unwrap();
        assert_eq!(first_seen.slot, 42);
        assert_eq!(first_seen.tx_index, Some(7));
        assert_eq!(first_seen.signer, [1u8; 32]);
        assert_eq!(first_seen.signature, [8u8; 64]);
    }

    #[test]
    fn parses_legacy_transaction_bytes() {
        let payer = [1u8; 32];
        let account = [2u8; 32];
        let signature = [9u8; 64];
        let mut tx = Vec::new();
        push_short_len(&mut tx, 1);
        tx.extend_from_slice(&signature);
        tx.extend_from_slice(&[1, 0, 0]);
        push_short_len(&mut tx, 2);
        tx.extend_from_slice(&payer);
        tx.extend_from_slice(&account);
        tx.extend_from_slice(&[7u8; 32]);
        push_short_len(&mut tx, 1);
        tx.push(1);
        push_short_len(&mut tx, 1);
        tx.push(0);
        push_short_len(&mut tx, 4);
        tx.extend_from_slice(&[3, 4, 5, 6]);

        let accounts = collect_bytes_for_test(&tx, 44, Some(9));
        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts.get(&payer).unwrap().signer, payer);
        assert_eq!(accounts.get(&account).unwrap().signature, signature);
    }

    #[test]
    fn parses_v0_transaction_bytes_and_lookup_table_accounts() {
        let payer = [1u8; 32];
        let lookup_account = [3u8; 32];
        let signature = [8u8; 64];
        let mut tx = Vec::new();
        push_short_len(&mut tx, 1);
        tx.extend_from_slice(&signature);
        tx.push(0x80);
        tx.extend_from_slice(&[1, 0, 0]);
        push_short_len(&mut tx, 1);
        tx.extend_from_slice(&payer);
        tx.extend_from_slice(&[7u8; 32]);
        push_short_len(&mut tx, 0);
        push_short_len(&mut tx, 1);
        tx.extend_from_slice(&lookup_account);
        push_short_len(&mut tx, 2);
        tx.extend_from_slice(&[0, 1]);
        push_short_len(&mut tx, 1);
        tx.push(2);

        let accounts = collect_bytes_for_test(&tx, 45, None);
        assert_eq!(accounts.len(), 2);
        assert!(accounts.contains_key(&payer));
        assert!(accounts.contains_key(&lookup_account));
        assert_eq!(accounts.get(&lookup_account).unwrap().signer, payer);
    }

    fn collect_for_test(
        tx: &VersionedTransaction<'_>,
        slot: u64,
        tx_index: Option<u64>,
    ) -> HashMap<[u8; 32], FirstSeen> {
        let dir = tempdir().unwrap();
        let mut spool = AccountSpool::new(dir.path(), 1, 1024).unwrap();
        let mut state = ScanState::default();
        collect_message_accounts(tx, slot, tx_index, &mut state, &mut spool).unwrap();
        spool.finish().unwrap();
        let mut accounts = HashMap::new();
        read_epoch_bucket(&bucket_path(dir.path(), 0), &mut accounts).unwrap();
        accounts
    }

    fn collect_bytes_for_test(
        tx_data: &[u8],
        slot: u64,
        tx_index: Option<u64>,
    ) -> HashMap<[u8; 32], FirstSeen> {
        let dir = tempdir().unwrap();
        let mut spool = AccountSpool::new(dir.path(), 1, 1024).unwrap();
        let mut state = ScanState::default();
        collect_transaction_accounts_from_bytes(
            tx_data,
            slot,
            tx_index,
            0,
            tx_data.len(),
            &mut state,
            &mut spool,
            None,
        )
        .unwrap();
        spool.finish().unwrap();
        let mut accounts = HashMap::new();
        read_epoch_bucket(&bucket_path(dir.path(), 0), &mut accounts).unwrap();
        accounts
    }

    fn push_short_len(out: &mut Vec<u8>, len: u8) {
        assert!(len < 0x80);
        out.push(len);
    }
}
