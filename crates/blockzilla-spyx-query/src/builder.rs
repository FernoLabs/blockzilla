use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX, ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_LOGS, ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA, ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK};
use blockzilla_token_transaction_dump::{
    DUMP_SCHEMA_VERSION, DumpStreamKind, PUBKEY_REGISTRY_ID_BASE, TokenTransactionBlockContext,
    consolidated_reader::{BorrowedDumpRecord, ConsolidatedFrameReader},
};
use sha2::{Digest, Sha256};

use crate::{
    index_format::{
        BLOCK_HEIGHT_NONE, BLOCK_TIME_NONE, INDEX_FLAG_COMPLETE, INDEX_HEADER_BYTES,
        INDEX_MANIFEST_FILE, IndexFileBinding, IndexHeader, IndexManifest, LOCATOR_MAGIC,
        LOCATOR_RECORD_BYTES, LOCATORS_FILE, LocatorRecord, SIGNATURE_LOOKUP_FILE, SIGNATURE_MAGIC,
        SIGNATURE_RECORD_BYTES, SignatureIndexRecord, SourceIndexBinding, TransactionCoordinate,
        encoded_file_bytes, hex_digest,
    },
    source::{SourceDump, load_source_dump, require_hash},
};

const WORK_DIRECTORY: &str = ".build-v1";
const SIGNATURE_RUNS_DIRECTORY: &str = "signature-runs";
const SIGNATURE_SORT_MEMORY_BYTES: usize = 256 << 20;
pub(crate) const IO_BUFFER_BYTES: usize = 8 << 20;
const SIGNATURE_BYTES: usize = 64;
const PROGRESS_TRANSACTION_INTERVAL: u64 = 250_000;
const PROGRESS_MERGE_INTERVAL: u64 = 250_000;
pub(crate) const KNOWN_TRANSACTION_FLAGS: u32 = ARCHIVE_V2_TX_FLAG_HAS_METADATA
    | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
    | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
    | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK
    | ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA
    | ARCHIVE_V2_TX_FLAG_HAS_LOGS
    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
    | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES
    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
    | ARCHIVE_V2_TX_FLAG_HAS_ERROR
    | ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX;

#[derive(Debug, Clone)]
pub struct BuildConfig {
    pub dump: PathBuf,
    pub output: PathBuf,
    pub max_transactions: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BuildSummary {
    pub output: PathBuf,
    pub complete: bool,
    pub transactions: u64,
    pub signature_occurrences: u64,
    pub locator_bytes: u64,
    pub signature_lookup_bytes: u64,
}

pub fn build_index(config: &BuildConfig) -> Result<BuildSummary> {
    if let Some(maximum) = config.max_transactions {
        ensure!(maximum != 0, "--max-transactions must be positive");
    }
    let source = load_source_dump(&config.dump)?;
    require_hash(&source.registry_handle, source.registry_sha256, "registry")?;
    require_hash(&source.accounts_handle, source.accounts_sha256, "accounts")?;

    let output = prepare_output(&config.output, &source.root)?;
    let work = output.join(WORK_DIRECTORY);
    fs::create_dir(&work).with_context(|| format!("create {}", work.display()))?;
    let signature_runs = work.join(SIGNATURE_RUNS_DIRECTORY);
    fs::create_dir(&signature_runs)
        .with_context(|| format!("create {}", signature_runs.display()))?;

    let complete = config.max_transactions.is_none();
    let target_transactions = config
        .max_transactions
        .map_or(source.manifest.transactions, |maximum| {
            maximum.min(source.manifest.transactions)
        });
    let flags = if complete { INDEX_FLAG_COMPLETE } else { 0 };
    let locator_partial = work.join(format!("{LOCATORS_FILE}.partial"));
    let signature_partial = work.join(format!("{SIGNATURE_LOOKUP_FILE}.partial"));
    let locator_header = IndexHeader {
        magic: LOCATOR_MAGIC,
        flags,
        record_bytes: LOCATOR_RECORD_BYTES as u16,
        record_count: target_transactions,
        source_manifest_sha256: source.manifest_sha256,
        source_transaction_sha256: source.transaction_sha256,
    };
    let mut locator_writer = DigestFileWriter::create(&locator_partial)?;
    locator_writer.write_all(&locator_header.encode())?;
    let mut signature_sorter = SignatureSorter::new(&signature_runs)?;

    let transaction_hashing_reader = HashingReader::new(source.transaction_handle.file());
    let mut transactions = ConsolidatedFrameReader::new(transaction_hashing_reader);
    let mut signatures = BufReader::with_capacity(IO_BUFFER_BYTES, source.signature_handle.file());
    let mut signature_hasher = Sha256::new();
    validate_stream_header(&source, &mut transactions)?;

    let started = Instant::now();
    let mut transaction_count = 0u64;
    let mut signature_count = 0u64;
    let mut previous_coordinate = None;
    let mut previous_block = None::<ObservedBlock>;
    while transaction_count < target_transactions {
        let frame = transactions
            .next_frame()?
            .context("consolidated stream ended before the requested transaction count")?;
        let locator = frame.locator;
        let BorrowedDumpRecord::Transaction(record) = frame.record else {
            bail!("consolidated stream has a non-transaction before the requested count")
        };
        let coordinate = TransactionCoordinate {
            epoch: record.source_epoch,
            slot: record.block.slot,
            source_block_id: record.source_block_id,
            tx_index: record.tx_index,
        };
        ensure!(
            previous_coordinate.is_none_or(|previous| previous < coordinate),
            "consolidated transactions are not in canonical coordinate order"
        );
        previous_coordinate = Some(coordinate);
        validate_transaction_record(&source, &record, signature_count)?;
        validate_block_context(&record.block, coordinate, &mut previous_block)?;
        ensure!(
            record.block.block_time != Some(BLOCK_TIME_NONE)
                && record.block.block_height != Some(BLOCK_HEIGHT_NONE),
            "transaction block context collides with an index option sentinel"
        );

        locator_writer.write_all(
            &LocatorRecord {
                coordinate,
                frame: locator,
                first_signature_ordinal: signature_count,
                flags: record.flags,
                parent_slot: record.block.parent_slot,
                block_time: record.block.block_time,
                block_height: record.block.block_height,
                transaction_count: record.block.transaction_count,
                signature_count: record.signature_count,
                source_wire_profile: record.source_wire_profile,
            }
            .encode(),
        )?;

        for signature_position in 0..record.signature_count {
            let mut signature = [0u8; SIGNATURE_BYTES];
            signatures
                .read_exact(&mut signature)
                .context("read contiguous dump signature occurrence")?;
            signature_hasher.update(signature);
            signature_sorter.push(SignatureIndexRecord {
                signature,
                transaction_id: transaction_count,
                signature_position,
            })?;
        }
        signature_count = signature_count
            .checked_add(u64::from(record.signature_count))
            .context("indexed signature count overflow")?;
        transaction_count = transaction_count
            .checked_add(1)
            .context("indexed transaction count overflow")?;
        if transaction_count.is_multiple_of(PROGRESS_TRANSACTION_INTERVAL)
            || transaction_count == target_transactions
        {
            report_scan_progress(
                transaction_count,
                target_transactions,
                signature_count,
                transactions.logical_offset(),
                started,
            );
        }
    }
    ensure!(
        transaction_count == target_transactions,
        "indexed transaction count differs from its target"
    );

    if complete {
        let footer_frame = transactions
            .next_frame()?
            .context("consolidated transaction stream has no footer")?;
        let BorrowedDumpRecord::Footer(footer) = footer_frame.record else {
            bail!("consolidated stream does not end after the manifest transaction count")
        };
        ensure!(
            transactions.next_frame()?.is_none(),
            "consolidated stream has records after its footer"
        );
        validate_footer(&source, footer, transaction_count, signature_count)?;
        ensure!(
            transactions.logical_offset() == source.transaction_bytes,
            "transaction stream byte length changed while it was scanned"
        );
        let observed_transaction_sha256 = transactions.get_ref().digest();
        ensure!(
            observed_transaction_sha256 == source.transaction_sha256,
            "transaction stream digest differs from its manifest"
        );
        ensure!(
            signature_count == source.signatures,
            "indexed signature count differs from its manifest"
        );
        let mut extra = [0u8; 1];
        ensure!(
            signatures.read(&mut extra)? == 0,
            "signature stream has bytes after its manifest occurrence count"
        );
        let observed_signature_sha256: [u8; 32] = signature_hasher.finalize().into();
        ensure!(
            observed_signature_sha256 == source.signature_sha256,
            "signature stream digest differs from its manifest"
        );
    }
    source.verify_file_identities()?;

    let locator_binding = locator_writer.finish(
        LOCATORS_FILE,
        transaction_count,
        LOCATOR_RECORD_BYTES as u16,
    )?;
    ensure!(
        locator_binding.bytes == encoded_file_bytes(transaction_count, LOCATOR_RECORD_BYTES)?,
        "locator index byte length differs"
    );
    let signature_header = IndexHeader {
        magic: SIGNATURE_MAGIC,
        flags,
        record_bytes: SIGNATURE_RECORD_BYTES as u16,
        record_count: signature_count,
        source_manifest_sha256: source.manifest_sha256,
        source_transaction_sha256: source.transaction_sha256,
    };
    let signature_binding =
        signature_sorter.finish(&signature_partial, signature_header, signature_count)?;

    let source_binding = SourceIndexBinding {
        manifest_sha256: hex_digest(source.manifest_sha256),
        transaction_file: source.manifest.transaction_stream.clone(),
        transaction_bytes: source.transaction_bytes,
        transaction_sha256: hex_digest(source.transaction_sha256),
        signature_file: source
            .manifest
            .signature_stream
            .clone()
            .expect("validated signature file binding"),
        signature_bytes: source.signature_bytes,
        signature_sha256: hex_digest(source.signature_sha256),
        registry_file: source
            .manifest
            .pubkey_registry
            .clone()
            .expect("validated registry file binding"),
        registry_bytes: source.registry_bytes,
        registry_sha256: hex_digest(source.registry_sha256),
        accounts_file: source
            .manifest
            .discovered_accounts
            .clone()
            .expect("validated accounts file binding"),
        accounts_bytes: source.accounts_bytes,
        accounts_sha256: hex_digest(source.accounts_sha256),
        manifest_transactions: source.manifest.transactions,
        manifest_signatures: source.signatures,
        manifest_pubkeys: source.pubkeys,
        transaction_hash_verified_during_build: complete,
        signature_hash_verified_during_build: complete,
    };
    let manifest = IndexManifest {
        schema_version: crate::index_format::INDEX_SCHEMA_VERSION,
        artifact_kind: IndexManifest::ARTIFACT_KIND.to_owned(),
        complete,
        canary_max_transactions: config.max_transactions,
        transactions: transaction_count,
        signature_occurrences: signature_count,
        created_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time is before Unix epoch")?
            .as_secs(),
        source: source_binding,
        locators: locator_binding,
        signature_lookup: signature_binding,
    };

    publish_index(&output, &work, &signature_runs, &manifest, &source)?;
    Ok(BuildSummary {
        output,
        complete,
        transactions: transaction_count,
        signature_occurrences: signature_count,
        locator_bytes: manifest.locators.bytes,
        signature_lookup_bytes: manifest.signature_lookup.bytes,
    })
}

fn report_scan_progress(
    transactions: u64,
    target_transactions: u64,
    signatures: u64,
    transaction_bytes: u64,
    started: Instant,
) {
    let elapsed = started.elapsed().as_secs_f64();
    let rate_mib = if elapsed > 0.0 {
        transaction_bytes as f64 / (1024.0 * 1024.0) / elapsed
    } else {
        0.0
    };
    let eta_seconds = if transactions == 0 {
        0.0
    } else {
        elapsed * (target_transactions.saturating_sub(transactions)) as f64 / transactions as f64
    };
    eprintln!(
        "index progress: tx {transactions}/{target_transactions}, signatures {signatures}, transaction bytes {transaction_bytes}, {rate_mib:.1} MiB/s, elapsed {elapsed:.0}s, ETA {eta_seconds:.0}s"
    );
}

pub(crate) fn validate_stream_header(
    source: &SourceDump,
    reader: &mut ConsolidatedFrameReader<HashingReader<&File>>,
) -> Result<()> {
    let frame = reader
        .next_frame()?
        .context("consolidated transaction stream is empty")?;
    let BorrowedDumpRecord::Header(header) = frame.record else {
        bail!("consolidated stream does not start with a header")
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::Consolidated
            && header.mint == source.mint
            && header.mint_slot == source.manifest.mint_slot
            && header.mint_signature == source.mint_signature
            && header.source_epoch.is_none()
            && header.source_generation_digest.is_none()
            && header.source_wire_profile.is_none()
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE,
        "consolidated stream header differs from its manifest"
    );
    Ok(())
}

pub(crate) fn validate_transaction_record(
    source: &SourceDump,
    record: &blockzilla_token_transaction_dump::consolidated_reader::BorrowedTransactionRecord<'_>,
    expected_signature_ordinal: u64,
) -> Result<()> {
    source.validate_record_binding(
        record.source_epoch,
        record.block.slot,
        record.source_block_id,
        record.source_wire_profile,
    )?;
    ensure!(
        record.block.parent_slot < record.block.slot
            && record.block.transaction_count != 0
            && record.tx_index < record.block.transaction_count
            && record.signature_count != 0
            && !record.message_bytes.is_empty()
            && record.flags & !KNOWN_TRANSACTION_FLAGS == 0
            && record.flags
                & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
                == 0,
        "consolidated transaction has invalid source fields"
    );
    ensure!(
        (record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0) == !record.metadata_bytes.is_empty()
            && (record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                == (record.metadata_bytes.first() == Some(&1)),
        "consolidated transaction flags differ from metadata bytes"
    );
    ensure!(
        record.dump_signature_ordinal == Some(expected_signature_ordinal),
        "consolidated dump signature ordinals are not contiguous"
    );
    let end = expected_signature_ordinal
        .checked_add(u64::from(record.signature_count))
        .context("dump signature range overflow")?;
    ensure!(
        end <= source.signatures
            && end
                .checked_mul(SIGNATURE_BYTES as u64)
                .is_some_and(|bytes| bytes <= source.signature_bytes),
        "transaction signature range is outside signatures.bin"
    );
    record
        .source_first_signature_ordinal
        .checked_add(u64::from(record.signature_count))
        .context("source signature range overflow")?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedBlock {
    epoch: u64,
    slot: u64,
    source_block_id: u32,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    transaction_count: u32,
}

pub(crate) fn validate_block_context(
    block: &TokenTransactionBlockContext,
    coordinate: TransactionCoordinate,
    previous: &mut Option<ObservedBlock>,
) -> Result<()> {
    let observed = ObservedBlock {
        epoch: coordinate.epoch,
        slot: coordinate.slot,
        source_block_id: coordinate.source_block_id,
        parent_slot: block.parent_slot,
        blockhash_id: block.blockhash_id,
        previous_blockhash_id: block.previous_blockhash_id,
        block_time: block.block_time,
        block_height: block.block_height,
        transaction_count: block.transaction_count,
    };
    if previous.is_some_and(|value| {
        value.epoch == coordinate.epoch && value.slot == coordinate.slot && value != observed
    }) {
        bail!("one source slot has conflicting block context")
    }
    *previous = Some(observed);
    Ok(())
}

pub(crate) fn validate_footer(
    source: &SourceDump,
    footer: blockzilla_token_transaction_dump::TokenTransactionDumpFooter,
    transactions: u64,
    signatures: u64,
) -> Result<()> {
    let epochs = source
        .manifest
        .last_epoch
        .checked_sub(source.manifest.first_epoch)
        .and_then(|span| span.checked_add(1))
        .context("manifest epoch count overflow")?;
    ensure!(
        transactions == source.manifest.transactions
            && signatures == source.signatures
            && footer.epochs == epochs
            && footer.transactions_written == transactions
            && footer.transactions_scanned >= transactions
            && footer.pubkeys == source.pubkeys
            && footer.signatures == signatures
            && footer.owned_block_fallbacks <= footer.blocks_scanned
            && footer.raw_transaction_fallbacks == 0
            && footer.raw_metadata_fallbacks == 0,
        "consolidated stream footer counts differ from its manifest"
    );
    Ok(())
}

pub(crate) fn prepare_output(output: &Path, source_root: &Path) -> Result<PathBuf> {
    let absolute = if output.is_absolute() {
        output.to_path_buf()
    } else {
        std::env::current_dir()?.join(output)
    };
    let mut existing = absolute.as_path();
    while !existing.exists() {
        existing = existing
            .parent()
            .context("index output has no existing parent")?;
    }
    let resolved_parent = fs::canonicalize(existing)?;
    let unresolved = absolute
        .strip_prefix(existing)
        .expect("existing parent is an output prefix");
    let planned = normalize_path(&resolved_parent.join(unresolved));
    ensure!(
        !planned.starts_with(source_root),
        "index output must not modify the immutable source dump"
    );
    if output.exists() {
        let metadata = fs::symlink_metadata(output)?;
        ensure!(
            metadata.file_type().is_dir(),
            "index output is not a directory"
        );
        ensure!(
            fs::read_dir(output)?.next().is_none(),
            "index output must be new or empty"
        );
    } else {
        fs::create_dir(output).with_context(|| format!("create {}", output.display()))?;
    }
    let output = fs::canonicalize(output)?;
    ensure!(
        !output.starts_with(source_root),
        "index output must not modify the immutable source dump"
    );
    Ok(output)
}

fn normalize_path(path: &Path) -> PathBuf {
    use std::path::Component;

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn publish_index(
    output: &Path,
    work: &Path,
    signature_runs: &Path,
    manifest: &IndexManifest,
    source: &SourceDump,
) -> Result<()> {
    let locator_partial = work.join(format!("{LOCATORS_FILE}.partial"));
    let signature_partial = work.join(format!("{SIGNATURE_LOOKUP_FILE}.partial"));
    fs::rename(&locator_partial, output.join(LOCATORS_FILE))?;
    fs::rename(&signature_partial, output.join(SIGNATURE_LOOKUP_FILE))?;
    ensure!(
        fs::read_dir(signature_runs)?.next().is_none(),
        "signature run directory is not empty after merge"
    );
    fs::remove_dir(signature_runs)?;
    fs::remove_dir(work)?;
    sync_directory(output)?;
    source.verify_file_identities()?;

    let manifest_partial = output.join(format!("{INDEX_MANIFEST_FILE}.partial"));
    let mut bytes = serde_json::to_vec_pretty(manifest)?;
    bytes.push(b'\n');
    let mut file = BufWriter::new(create_new_file(&manifest_partial)?);
    file.write_all(&bytes)?;
    file.flush()?;
    file.get_ref().sync_all()?;
    drop(file);
    fs::rename(&manifest_partial, output.join(INDEX_MANIFEST_FILE))?;
    sync_directory(output)?;
    Ok(())
}

pub(crate) struct DigestFileWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    hasher: Sha256,
    bytes: u64,
}

impl DigestFileWriter {
    pub(crate) fn create(path: &Path) -> Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_file(path)?),
            hasher: Sha256::new(),
            bytes: 0,
        })
    }

    pub(crate) fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes)?;
        self.hasher.update(bytes);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes.len()).context("written byte count exceeds u64")?)
            .context("written byte count overflow")?;
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
        file: &str,
        records: u64,
        record_bytes: u16,
    ) -> Result<IndexFileBinding> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        ensure!(
            fs::metadata(&self.path)?.len() == self.bytes,
            "index file size differs after its only final write"
        );
        Ok(IndexFileBinding {
            file: file.to_owned(),
            bytes: self.bytes,
            sha256: hex_digest(self.hasher.finalize().into()),
            records,
            record_bytes,
        })
    }
}

pub(crate) struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R> HashingReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    pub(crate) fn digest(&self) -> [u8; 32] {
        self.hasher.clone().finalize().into()
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(bytes)?;
        self.hasher.update(&bytes[..read]);
        Ok(read)
    }
}

struct SignatureSorter {
    root: PathBuf,
    capacity: usize,
    records: Vec<SignatureIndexRecord>,
    runs: Vec<PathBuf>,
    total: u64,
}

impl SignatureSorter {
    fn new(root: &Path) -> Result<Self> {
        let maximum = SIGNATURE_SORT_MEMORY_BYTES / std::mem::size_of::<SignatureIndexRecord>();
        ensure!(maximum != 0, "signature sort memory cannot hold one row");
        let capacity = 1usize << maximum.ilog2();
        Ok(Self {
            root: root.to_path_buf(),
            capacity,
            records: Vec::new(),
            runs: Vec::new(),
            total: 0,
        })
    }

    fn push(&mut self, record: SignatureIndexRecord) -> Result<()> {
        if self.records.len() == self.capacity {
            self.flush_run()?;
        }
        self.records.push(record);
        self.total = self
            .total
            .checked_add(1)
            .context("signature sort row overflow")?;
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        self.records.sort_unstable();
        ensure!(
            self.records.windows(2).all(|pair| pair[0] < pair[1]),
            "signature index contains one duplicate occurrence"
        );
        let path = self.root.join(format!("run-{:06}.bin", self.runs.len()));
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_file(&path)?);
        for record in &self.records {
            writer.write_all(&record.encode())?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        eprintln!(
            "signature sort: flushed run {} with {} occurrences",
            self.runs.len() + 1,
            self.records.len()
        );
        self.records.clear();
        self.runs.push(path);
        Ok(())
    }

    fn finish(
        mut self,
        output: &Path,
        header: IndexHeader,
        expected_records: u64,
    ) -> Result<IndexFileBinding> {
        self.flush_run()?;
        ensure!(
            self.total == expected_records && header.record_count == expected_records,
            "signature sorter count differs from its header"
        );
        let mut readers = self
            .runs
            .iter()
            .map(|path| SignatureRunReader::open(path))
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::<Reverse<(SignatureIndexRecord, usize)>>::new();
        for (index, reader) in readers.iter().enumerate() {
            if let Some(record) = reader.current {
                heap.push(Reverse((record, index)));
            }
        }
        let mut writer = DigestFileWriter::create(output)?;
        writer.write_all(&header.encode())?;
        let mut previous = None;
        let mut written = 0u64;
        eprintln!(
            "signature sort: merging {} runs and {} occurrences",
            self.runs.len(),
            expected_records
        );
        while let Some(Reverse((record, reader_index))) = heap.pop() {
            ensure!(
                previous.is_none_or(|value| value < record),
                "merged signature index is not strictly sorted"
            );
            writer.write_all(&record.encode())?;
            previous = Some(record);
            written = written
                .checked_add(1)
                .context("merged signature count overflow")?;
            if written.is_multiple_of(PROGRESS_MERGE_INTERVAL) || written == expected_records {
                eprintln!("signature sort: merged {written}/{expected_records} occurrences");
            }
            readers[reader_index].advance()?;
            if let Some(next) = readers[reader_index].current {
                heap.push(Reverse((next, reader_index)));
            }
        }
        ensure!(
            written == expected_records,
            "merged signature count differs"
        );
        for path in &self.runs {
            fs::remove_file(path)?;
        }
        let binding = writer.finish(
            SIGNATURE_LOOKUP_FILE,
            expected_records,
            SIGNATURE_RECORD_BYTES as u16,
        )?;
        ensure!(
            binding.bytes == encoded_file_bytes(expected_records, SIGNATURE_RECORD_BYTES)?,
            "signature index byte length differs"
        );
        Ok(binding)
    }
}

struct SignatureRunReader {
    reader: BufReader<File>,
    remaining: u64,
    current: Option<SignatureIndexRecord>,
}

impl SignatureRunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let bytes = file.metadata()?.len();
        ensure!(
            bytes.is_multiple_of(SIGNATURE_RECORD_BYTES as u64),
            "signature sort run has a partial row"
        );
        let mut this = Self {
            reader: BufReader::with_capacity(IO_BUFFER_BYTES, file),
            remaining: bytes / SIGNATURE_RECORD_BYTES as u64,
            current: None,
        };
        this.advance()?;
        Ok(this)
    }

    fn advance(&mut self) -> Result<()> {
        if self.remaining == 0 {
            self.current = None;
            return Ok(());
        }
        let mut bytes = [0u8; SIGNATURE_RECORD_BYTES];
        self.reader.read_exact(&mut bytes)?;
        self.current = Some(SignatureIndexRecord::decode(&bytes)?);
        self.remaining -= 1;
        Ok(())
    }
}

pub(crate) fn create_new_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create new file {}", path.display()))
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

const _: () = assert!(INDEX_HEADER_BYTES == 128);
