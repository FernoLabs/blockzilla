//! Build `indexes/accounts.pages`: account → the transactions that touch it.
//!
//! This is the premise the format exists for — filtering blocks and
//! transactions by account without decoding the archive.
//!
//! It reads the merged transaction and CPI streams once, emits bounded
//! fixed-width sort runs, and merges them by `(account ID, transaction
//! ordinal)`. Sort memory is independent of epoch size. Hot accounts continue
//! through bounded single-key pages, so page assembly also stays independent
//! of epoch size.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    mem,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use crate::{
    canonical_reader::{DEFAULT_MAX_BLOCK_DECODED_BYTES, scan_transactions_with_inner},
    container::{HeaderedWriter, decode_zstd_exact, validate_open_file},
    transaction_view::ResolvedAccounts,
};
use anyhow::{Context, Result, ensure};
use blockzilla_archive_v3::{
    ArchiveId, FILE_HEADER_LEN, FileHeader, indexes::accounts as postings,
    ledger::transactions::Transaction, runtime::inner_instructions::TransactionInner,
};

const DEFAULT_SORT_MEMORY_BYTES: usize = 128 << 20;
const SORT_RECORD_LEN: usize = 16;
const IO_BUFFER_BYTES: usize = 1 << 20;
const MERGE_FAN_IN: usize = 128;
const DEFAULT_MAX_POSTINGS_PER_PAGE: usize = postings::MAX_POSTINGS_PER_PAGE as usize;
const DEFAULT_MAX_KEYS_PER_PAGE: usize = postings::MAX_KEYS_PER_PAGE as usize;
/// Schema 2 counts logical account-to-transaction postings in the common
/// header's `record_count`. Page count is separate in the payload footer.
const ACCOUNT_INDEX_SCHEMA: u16 = postings::SCHEMA;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountIndexBuildOptions {
    pub sort_memory_bytes: usize,
}

impl Default for AccountIndexBuildOptions {
    fn default() -> Self {
        Self {
            sort_memory_bytes: DEFAULT_SORT_MEMORY_BYTES,
        }
    }
}

fn archive_id_from_catalog_header(root: &Path) -> Result<ArchiveId> {
    use blockzilla_archive_v3::catalog::blocks as catalog_blocks;

    let path = root.join(catalog_blocks::PATH);
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let mut bytes = [0_u8; FILE_HEADER_LEN];
    file.read_exact_at(&mut bytes, 0)
        .context("read catalog common header")?;
    let header = FileHeader::decode(&bytes).context("decode catalog common header")?;
    validate_open_file(&file, catalog_blocks::PATH, header.archive_id)?;
    Ok(header.archive_id)
}

fn read_directory(
    file: &File,
    header: FileHeader,
) -> Result<(postings::DirectoryFooter, Vec<postings::PageDirectoryEntry>)> {
    ensure!(
        header.schema == ACCOUNT_INDEX_SCHEMA,
        "account-index header has schema {}, expected {ACCOUNT_INDEX_SCHEMA}",
        header.schema
    );
    let file_len = file.metadata()?.len();
    let footer_offset = file_len
        .checked_sub(postings::DIRECTORY_FOOTER_LEN as u64)
        .context("account-index object is too short for its directory footer")?;
    ensure!(
        footer_offset >= FILE_HEADER_LEN as u64,
        "account-index directory footer overlaps its common header"
    );
    let mut footer_bytes = [0_u8; postings::DIRECTORY_FOOTER_LEN];
    file.read_exact_at(&mut footer_bytes, footer_offset)
        .context("read account-index directory footer")?;
    let footer = postings::DirectoryFooter::decode(&footer_bytes)?;
    let directory_bytes_len = footer
        .page_count
        .checked_mul(postings::DIRECTORY_ENTRY_LEN as u64)
        .context("account-index directory length overflow")?;
    ensure!(
        footer.directory_offset >= FILE_HEADER_LEN as u64,
        "account-index directory overlaps its common header"
    );
    ensure!(
        footer
            .directory_offset
            .checked_add(directory_bytes_len)
            .is_some_and(|end| end == footer_offset),
        "account-index directory does not end at its footer"
    );

    let directory_len = usize::try_from(directory_bytes_len)
        .context("account-index directory does not fit in memory")?;
    let mut directory_bytes = vec![0_u8; directory_len];
    file.read_exact_at(&mut directory_bytes, footer.directory_offset)
        .context("read account-index directory")?;
    let entries = directory_bytes
        .chunks_exact(postings::DIRECTORY_ENTRY_LEN)
        .map(postings::PageDirectoryEntry::decode)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("decode account-index directory")?;

    postings::validate_directory(&entries, FILE_HEADER_LEN as u64, footer.directory_offset)?;
    let minimum_postings = entries.iter().try_fold(0_u64, |total, entry| {
        total.checked_add(u64::from(entry.key_count))
    });
    let maximum_postings = u64::try_from(entries.len())
        .ok()
        .and_then(|pages| pages.checked_mul(u64::from(postings::MAX_POSTINGS_PER_PAGE)));
    let minimum_postings = minimum_postings.context("account minimum posting count overflow")?;
    let maximum_postings = maximum_postings.context("account maximum posting count overflow")?;
    ensure!(
        header.record_count >= minimum_postings && header.record_count <= maximum_postings,
        "account-index header declares {} postings, page bounds allow {minimum_postings}..={maximum_postings}",
        header.record_count
    );
    let decoded_pages = entries.iter().try_fold(0_u64, |total, entry| {
        total.checked_add(u64::from(entry.decoded_len))
    });
    let expected_decoded = decoded_pages
        .and_then(|total| total.checked_add(directory_bytes_len))
        .and_then(|total| total.checked_add(postings::DIRECTORY_FOOTER_LEN as u64))
        .context("account-index decoded-byte count overflow")?;
    ensure!(
        header.decoded_bytes == expected_decoded,
        "account-index header declares {} decoded bytes, directory accounts for {expected_decoded}",
        header.decoded_bytes
    );
    Ok((footer, entries))
}

/// Which roles an account holds in one transaction.
///
/// Signer and writable follow the message header: the first
/// `num_required_signatures` accounts sign, of which the last
/// `num_readonly_signed` are readonly, and the tail of the static keys has
/// `num_readonly_unsigned` readonly entries. Loaded writable addresses are
/// writable by construction; loaded readonly are not.
fn transaction_roles(
    transaction: &Transaction,
    inner: Option<&TransactionInner>,
    lo: u32,
    hi: u64,
) -> Result<BTreeMap<u32, u8>> {
    // Roles are merged per transaction. An account that is a signer, writable,
    // and a program produces one posting with the union of those roles.
    let mut merged = BTreeMap::new();
    let accounts = ResolvedAccounts::new(transaction);
    ensure!(
        accounts.is_complete(),
        "loaded-address pubkeys are unavailable; a complete account index cannot be built"
    );
    for (position, id) in accounts.iter().enumerate() {
        if id < lo || u64::from(id) >= hi {
            continue;
        }
        // Entry insertion is intentional even when positional roles are zero:
        // the posting itself records read-only, non-signer presence.
        *merged.entry(id).or_default() |= accounts.positional_roles(transaction.header, position);
    }
    for instruction in transaction.message.instructions() {
        if let Some(id) = accounts.get(instruction.program_position as usize)
            && id >= lo
            && u64::from(id) < hi
        {
            *merged.entry(id).or_default() |= postings::ROLE_TOP_LEVEL_PROGRAM;
        }
    }
    if let Some(inner) = inner {
        for group in &inner.groups {
            for inner_instruction in &group.instructions {
                let instruction = &inner_instruction.instruction;
                if let Some(id) = accounts.get(instruction.program_position as usize)
                    && id >= lo
                    && u64::from(id) < hi
                {
                    *merged.entry(id).or_default() |= postings::ROLE_CPI_PROGRAM;
                }
            }
        }
    }
    Ok(merged)
}

fn read_posting_page(
    file: &File,
    entry: postings::PageDirectoryEntry,
) -> Result<Vec<postings::KeyPostings>> {
    let mut stored = vec![0_u8; entry.stored_len as usize];
    file.read_exact_at(&mut stored, entry.offset)
        .context("read account posting page")?;
    let decoded = if entry.is_compressed() {
        decode_zstd_exact(&stored, entry.decoded_len as usize, "account posting page")?
    } else {
        ensure!(
            entry.stored_len == entry.decoded_len,
            "raw account page stored and decoded lengths differ"
        );
        stored
    };
    let page = postings::decode_page(&decoded, entry.first_key, entry.key_count)?;
    ensure!(
        page.first()
            .is_some_and(|first| first.key == entry.first_key)
            && page.last().is_some_and(|last| last.key == entry.last_key),
        "account page keys do not match its directory entry"
    );
    Ok(page)
}

fn visit_posting_fragment(
    postings: &[postings::Posting],
    previous_ordinal: &mut Option<u64>,
    visit: &mut impl FnMut(postings::Posting) -> Result<()>,
) -> Result<u64> {
    let mut visited = 0_u64;
    for posting in postings.iter().copied() {
        if let Some(previous) = *previous_ordinal {
            ensure!(
                posting.transaction_ordinal > previous,
                "account continuation postings do not strictly ascend"
            );
        }
        *previous_ordinal = Some(posting.transaction_ordinal);
        visit(posting)?;
        visited = visited
            .checked_add(1)
            .context("account lookup posting count overflow")?;
    }
    Ok(visited)
}

/// Result of one incremental account posting lookup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccountLookupReport {
    pub postings: u64,
    pub pages_read: u64,
    pub stored_page_bytes: u64,
    pub directory_bytes: u64,
}

/// Visit a complete account posting chain one bounded page at a time.
///
/// The directory topology is validated before the first callback. Transaction
/// ordinals must also strictly ascend across every continuation boundary.
pub fn visit_account_postings(
    root: &Path,
    ordinal: u32,
    mut visit: impl FnMut(postings::Posting) -> Result<()>,
) -> Result<AccountLookupReport> {
    ensure!(ordinal != 0, "account dictionary ID zero is reserved");
    let archive_id = archive_id_from_catalog_header(root)?;
    let path = root.join(postings::PATH);
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let header = validate_open_file(&file, postings::PATH, archive_id)?;
    let (footer, entries) = read_directory(&file, header)?;
    let range = postings::pages_for_key(&entries, ordinal);
    let mut report = AccountLookupReport {
        directory_bytes: footer
            .page_count
            .checked_mul(postings::DIRECTORY_ENTRY_LEN as u64)
            .context("account directory byte count overflow")?,
        ..Default::default()
    };
    let mut previous_ordinal = None;
    for entry in &entries[range] {
        report.pages_read = report
            .pages_read
            .checked_add(1)
            .context("account page count overflow")?;
        report.stored_page_bytes = report
            .stored_page_bytes
            .checked_add(u64::from(entry.stored_len))
            .context("account page byte count overflow")?;
        let page = read_posting_page(&file, *entry)?;
        let Some(found) = postings::find_key(&page, ordinal) else {
            continue;
        };
        let visited = visit_posting_fragment(&found.postings, &mut previous_ordinal, &mut visit)?;
        report.postings = report
            .postings
            .checked_add(visited)
            .context("account lookup posting count overflow")?;
    }
    Ok(report)
}

/// Answer the question the index exists for: which transactions touch this
/// account, and how. Posting pages are processed incrementally.
pub fn find_account(root: &Path, ordinal: u32) -> Result<()> {
    let started = std::time::Instant::now();
    let mut first = Vec::with_capacity(6);
    let report = visit_account_postings(root, ordinal, |posting| {
        if first.len() < 6 {
            first.push(posting);
        }
        Ok(())
    })?;

    let elapsed = started.elapsed();
    if report.postings == 0 {
        println!("account {ordinal}: not referenced in this generation");
    } else {
        println!("account {ordinal}: {} transactions", report.postings);
        for posting in &first {
            let mut roles = Vec::new();
            for (bit, name) in [
                (postings::ROLE_SIGNER, "signer"),
                (postings::ROLE_WRITABLE, "writable"),
                (postings::ROLE_TOP_LEVEL_PROGRAM, "program"),
                (postings::ROLE_CPI_PROGRAM, "cpi-program"),
            ] {
                if posting.roles & bit != 0 {
                    roles.push(name);
                }
            }
            println!(
                "    tx {:<8} {}",
                posting.transaction_ordinal,
                roles.join(", ")
            );
        }
        if report.postings > first.len() as u64 {
            println!("    ... {} more", report.postings - first.len() as u64);
        }
    }
    println!();
    println!(
        "  read                1 footer + 1 directory ({} bytes) + {} page(s) ({} bytes)",
        report.directory_bytes, report.pages_read, report.stored_page_bytes
    );
    println!("  archive opened      no");
    println!("  elapsed             {elapsed:?}");
    Ok(())
}

/// One exact account lookup from the derived index.
pub fn lookup_account(root: &Path, ordinal: u32) -> Result<Vec<postings::Posting>> {
    let mut result = Vec::new();
    visit_account_postings(root, ordinal, |posting| {
        result.push(posting);
        Ok(())
    })?;
    Ok(result)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountIndexBuildReport {
    pub archive_id: ArchiveId,
    pub blocks: u64,
    pub transactions: u64,
    pub distinct_accounts: u64,
    pub postings: u64,
    pub sort_runs: u64,
    pub merge_passes: u32,
    pub pages: u64,
    pub continuation_pages: u64,
    pub max_postings_per_page: usize,
    pub peak_page_postings: usize,
    pub page_bytes: u64,
    pub directory_bytes: u64,
    pub object_bytes: u64,
}

fn compress_posting_page(payload: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = zstd::Encoder::new(Vec::new(), 3).context("create posting-page encoder")?;
    encoder
        .include_checksum(true)
        .context("enable posting-page checksum")?;
    encoder
        .write_all(payload)
        .context("compress posting page")?;
    encoder.finish().context("finish posting page")
}

#[derive(Debug, Clone, Copy)]
struct PageLimits {
    max_postings: usize,
    max_keys: usize,
}

fn page_limits() -> PageLimits {
    PageLimits {
        max_postings: DEFAULT_MAX_POSTINGS_PER_PAGE,
        max_keys: DEFAULT_MAX_KEYS_PER_PAGE,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SortRecord {
    account_id: u32,
    transaction_ordinal: u64,
    roles: u8,
}

impl SortRecord {
    fn encode(self) -> [u8; SORT_RECORD_LEN] {
        let mut out = [0_u8; SORT_RECORD_LEN];
        out[0..4].copy_from_slice(&self.account_id.to_le_bytes());
        out[4..12].copy_from_slice(&self.transaction_ordinal.to_le_bytes());
        out[12] = self.roles;
        out
    }

    fn decode(bytes: &[u8; SORT_RECORD_LEN]) -> Result<Self> {
        ensure!(
            bytes[13..16] == [0; 3],
            "account-index sort record has non-zero reserved bytes"
        );
        let roles = bytes[12];
        ensure!(
            roles & !postings::ROLE_MASK == 0,
            "account-index sort record has unknown role bits {roles:#x}"
        );
        let record = Self {
            account_id: u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes")),
            transaction_ordinal: u64::from_le_bytes(bytes[4..12].try_into().expect("8 bytes")),
            roles,
        };
        ensure!(
            record.account_id != 0,
            "account sort record has reserved ID zero"
        );
        Ok(record)
    }
}

struct StagingDirectory {
    path: PathBuf,
}

impl StagingDirectory {
    fn create(root: &Path) -> Result<Self> {
        let path = root.join(format!(".account-index.building-{}", std::process::id()));
        fs::create_dir(&path).with_context(|| {
            format!(
                "create account-index staging directory {}; remove an abandoned directory first if needed",
                path.display()
            )
        })?;
        Ok(Self { path })
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn flush_run(
    run_directory: &Path,
    run_number: u64,
    records: &mut Vec<SortRecord>,
) -> Result<PathBuf> {
    ensure!(
        !records.is_empty(),
        "cannot write an empty account sort run"
    );
    records.sort_unstable();
    for pair in records.windows(2) {
        ensure!(
            pair[0] < pair[1],
            "account sort run repeats account {} transaction {}",
            pair[0].account_id,
            pair[0].transaction_ordinal
        );
    }
    let path = run_directory.join(format!("run-{run_number:08}.bin"));
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("create account sort run {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
    for record in records.iter().copied() {
        writer.write_all(&record.encode())?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    records.clear();
    Ok(path)
}

struct RunReader {
    reader: BufReader<File>,
    remaining: u64,
    path: PathBuf,
}

impl RunReader {
    fn open(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)
            .with_context(|| format!("open account sort run {}", path.display()))?;
        let bytes = file.metadata()?.len();
        ensure!(
            bytes.is_multiple_of(SORT_RECORD_LEN as u64),
            "account sort run {} is not record-aligned",
            path.display()
        );
        Ok(Self {
            reader: BufReader::with_capacity(IO_BUFFER_BYTES, file),
            remaining: bytes / SORT_RECORD_LEN as u64,
            path,
        })
    }

    fn next(&mut self) -> Result<Option<SortRecord>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0_u8; SORT_RECORD_LEN];
        self.reader
            .read_exact(&mut bytes)
            .with_context(|| format!("read account sort run {}", self.path.display()))?;
        self.remaining -= 1;
        Ok(Some(SortRecord::decode(&bytes)?))
    }
}

struct RunMerger {
    readers: Vec<RunReader>,
    heap: BinaryHeap<Reverse<(SortRecord, usize)>>,
    previous: Option<SortRecord>,
}

impl RunMerger {
    fn open(paths: Vec<PathBuf>) -> Result<Self> {
        ensure!(
            paths.len() <= MERGE_FAN_IN,
            "account merge fan-in exceeds {MERGE_FAN_IN}"
        );
        let mut readers = paths
            .into_iter()
            .map(RunReader::open)
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some(record) = reader.next()? {
                heap.push(Reverse((record, index)));
            }
        }
        Ok(Self {
            readers,
            heap,
            previous: None,
        })
    }

    fn next(&mut self) -> Result<Option<SortRecord>> {
        let Some(Reverse((record, run))) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(previous) = self.previous {
            ensure!(
                record > previous,
                "merged account postings are not strictly sorted: {previous:?} then {record:?}"
            );
        }
        self.previous = Some(record);
        if let Some(next) = self.readers[run].next()? {
            self.heap.push(Reverse((next, run)));
        }
        Ok(Some(record))
    }
}

fn merge_run_group(inputs: Vec<PathBuf>, output: &Path) -> Result<()> {
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(output)
        .with_context(|| format!("create merged account run {}", output.display()))?;
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
    let mut merger = RunMerger::open(inputs)?;
    while let Some(record) = merger.next()? {
        writer.write_all(&record.encode())?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn reduce_runs(mut paths: Vec<PathBuf>, run_directory: &Path) -> Result<(Vec<PathBuf>, u32)> {
    let mut pass = 0_u32;
    while paths.len() > MERGE_FAN_IN {
        pass = pass.checked_add(1).context("account merge-pass overflow")?;
        let mut next = Vec::new();
        for (group, chunk) in paths.chunks(MERGE_FAN_IN).enumerate() {
            let output = run_directory.join(format!("merge-{pass:04}-{group:08}.bin"));
            merge_run_group(chunk.to_vec(), &output)?;
            next.push(output);
        }
        for path in paths {
            fs::remove_file(&path)
                .with_context(|| format!("remove consumed account run {}", path.display()))?;
        }
        paths = next;
    }
    Ok((paths, pass))
}

fn emit_runs(
    root: &Path,
    run_directory: &Path,
    sort_memory_bytes: usize,
) -> Result<(ArchiveId, Vec<PathBuf>, u64, u64, u64)> {
    ensure!(
        sort_memory_bytes >= mem::size_of::<SortRecord>(),
        "account sort memory must hold at least one in-memory record"
    );
    let capacity = sort_memory_bytes / mem::size_of::<SortRecord>();
    let mut records = Vec::with_capacity(capacity);
    let mut runs = Vec::new();
    let mut run_number = 0_u64;
    let mut posting_count = 0_u64;
    let scan = scan_transactions_with_inner(root, DEFAULT_MAX_BLOCK_DECODED_BYTES, |block| {
        for (index, transaction) in block.replay.transactions.iter().enumerate() {
            let transaction_ordinal = block
                .replay
                .catalog
                .first_transaction
                .checked_add(index as u64)
                .context("transaction ordinal overflow")?;
            let roles = transaction_roles(transaction, block.inner[index].as_ref(), 0, u64::MAX)
                .with_context(|| format!("index transaction ordinal {transaction_ordinal}"))?;
            for (account_id, roles) in roles {
                records.push(SortRecord {
                    account_id,
                    transaction_ordinal,
                    roles,
                });
                posting_count = posting_count
                    .checked_add(1)
                    .context("account posting count overflow")?;
                if records.len() == capacity {
                    runs.push(flush_run(run_directory, run_number, &mut records)?);
                    run_number = run_number
                        .checked_add(1)
                        .context("sort-run count overflow")?;
                }
            }
        }
        Ok(())
    })?;
    if !records.is_empty() {
        runs.push(flush_run(run_directory, run_number, &mut records)?);
    }
    Ok((
        scan.archive_id,
        runs,
        posting_count,
        scan.blocks,
        scan.transactions,
    ))
}

struct AccountPageWriter {
    writer: HeaderedWriter,
    directory: Vec<postings::PageDirectoryEntry>,
    pending: Vec<postings::KeyPostings>,
    pending_postings: usize,
    limits: PageLimits,
    page_bytes: u64,
    continuation_pages: u64,
    peak_page_postings: usize,
}

impl AccountPageWriter {
    fn new(root: &Path, limits: PageLimits) -> Result<Self> {
        Ok(Self {
            writer: HeaderedWriter::create(root, postings::PATH, IO_BUFFER_BYTES)?,
            directory: Vec::new(),
            pending: Vec::with_capacity(limits.max_keys),
            pending_postings: 0,
            limits,
            page_bytes: 0,
            continuation_pages: 0,
            peak_page_postings: 0,
        })
    }

    fn push_complete_key(&mut self, key: postings::KeyPostings) -> Result<()> {
        ensure!(
            !key.postings.is_empty() && key.postings.len() <= self.limits.max_postings,
            "complete account key exceeds its bounded page"
        );
        let would_exceed_postings = self
            .pending_postings
            .checked_add(key.postings.len())
            .is_none_or(|count| count > self.limits.max_postings);
        if self.pending.len() == self.limits.max_keys || would_exceed_postings {
            self.flush_complete_page()?;
        }
        self.pending_postings += key.postings.len();
        self.pending.push(key);
        Ok(())
    }

    fn push_continuation(
        &mut self,
        key: postings::KeyPostings,
        continued_from_previous: bool,
        continues_in_next: bool,
    ) -> Result<()> {
        self.flush_complete_page()?;
        let mut flags = 0_u16;
        if continued_from_previous {
            flags |= postings::PAGE_FLAG_CONTINUED_FROM_PREVIOUS;
        }
        if continues_in_next {
            flags |= postings::PAGE_FLAG_CONTINUES_IN_NEXT;
        }
        self.write_page(&[key], flags)?;
        self.continuation_pages = self
            .continuation_pages
            .checked_add(1)
            .context("account continuation-page count overflow")?;
        Ok(())
    }

    fn flush_complete_page(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let keys = std::mem::take(&mut self.pending);
        self.pending_postings = 0;
        self.write_page(&keys, 0)
    }

    fn write_page(&mut self, keys: &[postings::KeyPostings], flags: u16) -> Result<()> {
        let posting_count = keys
            .iter()
            .try_fold(0_usize, |total, key| total.checked_add(key.postings.len()))
            .context("account page posting count overflow")?;
        ensure!(
            posting_count > 0 && posting_count <= self.limits.max_postings,
            "account page has {posting_count} postings, limit is {}",
            self.limits.max_postings
        );
        self.peak_page_postings = self.peak_page_postings.max(posting_count);
        let encoded = postings::encode_page(keys).context("encode posting page")?;
        let compressed = compress_posting_page(&encoded)?;
        let stored = if compressed.len() < encoded.len() {
            compressed.as_slice()
        } else {
            encoded.as_slice()
        };
        let offset = self.writer.append(stored, encoded.len() as u64)?;
        self.directory.push(postings::PageDirectoryEntry {
            first_key: keys[0].key,
            last_key: keys[keys.len() - 1].key,
            offset,
            stored_len: u32::try_from(stored.len()).context("page exceeds u32")?,
            decoded_len: u32::try_from(encoded.len()).context("decoded page exceeds u32")?,
            key_count: u32::try_from(keys.len()).context("keys exceed u32")?,
            flags,
        });
        self.page_bytes = self
            .page_bytes
            .checked_add(stored.len() as u64)
            .context("account-index page length overflow")?;
        Ok(())
    }

    fn finish(
        mut self,
        archive_id: ArchiveId,
        blocks: u64,
        transactions: u64,
        total_keys: u64,
        total_postings: u64,
    ) -> Result<AccountIndexBuildReport> {
        self.flush_complete_page()?;
        let page_bytes_end = (FILE_HEADER_LEN as u64)
            .checked_add(self.page_bytes)
            .context("account page byte extent overflow")?;
        postings::validate_directory(&self.directory, FILE_HEADER_LEN as u64, page_bytes_end)?;
        let directory_capacity = self
            .directory
            .len()
            .checked_mul(postings::DIRECTORY_ENTRY_LEN)
            .context("account directory length overflow")?;
        let mut directory_bytes = Vec::with_capacity(directory_capacity);
        for entry in &self.directory {
            directory_bytes.extend_from_slice(&entry.encode());
        }
        let directory_offset = self
            .writer
            .append(&directory_bytes, directory_bytes.len() as u64)?;
        ensure!(
            directory_offset == page_bytes_end,
            "account-index page byte accounting drift"
        );
        let footer = postings::DirectoryFooter {
            directory_offset,
            page_count: self.directory.len() as u64,
        }
        .encode();
        self.writer.append(&footer, footer.len() as u64)?;
        let finished = self.writer.finish(archive_id, total_postings)?;
        Ok(AccountIndexBuildReport {
            archive_id,
            blocks,
            transactions,
            distinct_accounts: total_keys,
            postings: total_postings,
            sort_runs: 0,
            merge_passes: 0,
            pages: self.directory.len() as u64,
            continuation_pages: self.continuation_pages,
            max_postings_per_page: self.limits.max_postings,
            peak_page_postings: self.peak_page_postings,
            page_bytes: self.page_bytes,
            directory_bytes: directory_bytes.len() as u64,
            object_bytes: finished.file_bytes,
        })
    }
}

struct PostingPageBuffer {
    postings: Vec<postings::Posting>,
    limit: usize,
    peak: usize,
}

impl PostingPageBuffer {
    fn new(limit: usize) -> Self {
        Self {
            postings: Vec::new(),
            limit,
            peak: 0,
        }
    }

    fn push(&mut self, posting: postings::Posting) -> Result<()> {
        ensure!(
            self.postings.len() < self.limit,
            "account posting fragment exceeded its configured bound"
        );
        self.postings.push(posting);
        self.peak = self.peak.max(self.postings.len());
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.postings.len() == self.limit
    }

    fn take(&mut self) -> Vec<postings::Posting> {
        std::mem::take(&mut self.postings)
    }
}

fn write_merged_index(
    staging_root: &Path,
    archive_id: ArchiveId,
    run_paths: Vec<PathBuf>,
    expected_postings: u64,
    blocks: u64,
    transactions: u64,
    limits: PageLimits,
) -> Result<AccountIndexBuildReport> {
    let mut pages = AccountPageWriter::new(staging_root, limits)?;
    if run_paths.is_empty() {
        ensure!(
            expected_postings == 0,
            "account sort runs are unexpectedly empty"
        );
        return pages.finish(archive_id, blocks, transactions, 0, 0);
    }
    let mut merger = RunMerger::open(run_paths)?;
    let mut next = merger.next()?;
    let mut merged = 0_u64;
    let mut distinct_accounts = 0_u64;
    let mut fragment = PostingPageBuffer::new(limits.max_postings);
    while let Some(first) = next.take() {
        let key = first.account_id;
        distinct_accounts = distinct_accounts
            .checked_add(1)
            .context("distinct account count overflow")?;
        let mut first_in_fragment = Some(first);
        let mut continued_from_previous = false;
        loop {
            if let Some(record) = first_in_fragment.take() {
                fragment.push(postings::Posting {
                    transaction_ordinal: record.transaction_ordinal,
                    roles: record.roles,
                })?;
                merged = merged
                    .checked_add(1)
                    .context("merged posting count overflow")?;
            }
            while !fragment.is_full() {
                next = merger.next()?;
                let Some(record) = next else {
                    break;
                };
                if record.account_id != key {
                    break;
                }
                fragment.push(postings::Posting {
                    transaction_ordinal: record.transaction_ordinal,
                    roles: record.roles,
                })?;
                merged = merged
                    .checked_add(1)
                    .context("merged posting count overflow")?;
                next = None;
            }
            if fragment.is_full() && next.is_none() {
                next = merger.next()?;
            }
            let continues_in_next = next.is_some_and(|record| record.account_id == key);
            let entry = postings::KeyPostings {
                key,
                postings: fragment.take(),
            };
            if continued_from_previous || continues_in_next {
                pages.push_continuation(entry, continued_from_previous, continues_in_next)?;
            } else {
                pages.push_complete_key(entry)?;
            }
            if !continues_in_next {
                break;
            }
            first_in_fragment = next.take();
            continued_from_previous = true;
        }
    }
    ensure!(
        merged == expected_postings,
        "merged {merged} account postings, expected {expected_postings}"
    );
    ensure!(
        fragment.peak <= limits.max_postings,
        "account posting fragment exceeded its configured bound"
    );
    pages.finish(archive_id, blocks, transactions, distinct_accounts, merged)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

pub fn build_account_index(
    root: &Path,
    options: AccountIndexBuildOptions,
) -> Result<AccountIndexBuildReport> {
    let limits = page_limits();
    let staging = StagingDirectory::create(root)?;
    let run_directory = staging.path.join("account-runs");
    fs::create_dir(&run_directory)
        .with_context(|| format!("create account run directory {}", run_directory.display()))?;
    let (archive_id, runs, expected_postings, blocks, transactions) =
        emit_runs(root, &run_directory, options.sort_memory_bytes)?;
    let sort_runs = runs.len() as u64;
    let (runs, merge_passes) = reduce_runs(runs, &run_directory)?;
    let mut stats = write_merged_index(
        &staging.path,
        archive_id,
        runs,
        expected_postings,
        blocks,
        transactions,
        limits,
    )?;
    stats.sort_runs = sort_runs;
    stats.merge_passes = merge_passes;

    let staged = staging.path.join(postings::PATH);
    let target = root.join(postings::PATH);
    let target_parent = target.parent().context("account index has no parent")?;
    fs::create_dir_all(target_parent)?;
    fs::rename(&staged, &target).with_context(|| {
        format!(
            "replace account index {} with staged object {}",
            target.display(),
            staged.display()
        )
    })?;
    sync_directory(target_parent)?;
    sync_directory(root)?;

    let legacy_directory = root.join("indexes/accounts.idx");
    if legacy_directory.is_file() {
        fs::remove_file(&legacy_directory)
            .with_context(|| format!("remove obsolete {}", legacy_directory.display()))?;
        sync_directory(target_parent)?;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use blockzilla_archive_v3::{
        ObjectRole,
        ledger::transactions::{
            AddressTableLookup, HashOwner, HashRef, Instruction, LoadedAddresses, Message,
            MessageHeader, PubkeyId, Transaction,
        },
        runtime::inner_instructions::{InnerGroup, InnerInstruction, TransactionInner},
    };

    use crate::test_fixture::{FixtureBlock, write_merged_fixture};

    use super::*;

    fn write_fixture(root: &Path, transaction_archive_id: ArchiveId, transaction_count: usize) {
        let archive_id = ArchiveId::new([7; 16]);
        let transaction = Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 3,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::Legacy {
                static_accounts: [10, 11, 12, 13, 14]
                    .into_iter()
                    .map(|id| PubkeyId::new(id).unwrap())
                    .collect(),
                instructions: vec![Instruction {
                    program_position: 2,
                    account_positions: Vec::new(),
                    data: Vec::new(),
                }],
            },
        };
        let inner = TransactionInner {
            groups: vec![InnerGroup {
                parent_index: 0,
                instructions: vec![InnerInstruction {
                    stack_height: Some(2),
                    instruction: Instruction {
                        program_position: 3,
                        account_positions: Vec::new(),
                        data: Vec::new(),
                    },
                }],
            }],
        };
        write_merged_fixture(
            root,
            archive_id,
            transaction_archive_id,
            archive_id,
            14,
            vec![FixtureBlock {
                slot: 1,
                parent_slot: 0,
                transactions: vec![transaction; transaction_count],
                inner: vec![Some(inner); transaction_count],
            }],
        );
    }

    #[test]
    fn footer_has_one_frozen_schema_encoding() {
        let footer = postings::DirectoryFooter {
            directory_offset: 0x1112_1314_1516_1718,
            page_count: 0x2122_2324_2526_2728,
        };
        let mut expected = [0_u8; postings::DIRECTORY_FOOTER_LEN];
        expected[0..8].copy_from_slice(b"BZIAADIR");
        expected[8..16].copy_from_slice(&footer.directory_offset.to_le_bytes());
        expected[16..24].copy_from_slice(&footer.page_count.to_le_bytes());
        assert_eq!(footer.encode(), expected);
        assert_eq!(
            postings::DirectoryFooter::decode(&expected).unwrap(),
            footer
        );
        expected[0] ^= 1;
        assert!(postings::DirectoryFooter::decode(&expected).is_err());
    }

    #[test]
    fn one_and_many_runs_make_identical_bytes_and_keep_zero_and_program_roles() {
        let root = tempdir().unwrap();
        write_fixture(root.path(), ArchiveId::new([7; 16]), 20);
        let one = build_account_index(
            root.path(),
            AccountIndexBuildOptions {
                sort_memory_bytes: 1 << 20,
            },
        )
        .unwrap();
        assert_eq!(one.sort_runs, 1);
        let bytes = fs::read(root.path().join(postings::PATH)).unwrap();
        assert_eq!(lookup_account(root.path(), 10).unwrap().len(), 20);
        assert_eq!(lookup_account(root.path(), 10).unwrap()[0].roles, 3);
        assert_eq!(lookup_account(root.path(), 12).unwrap()[0].roles, 4);
        assert_eq!(lookup_account(root.path(), 13).unwrap()[0].roles, 8);
        assert_eq!(lookup_account(root.path(), 14).unwrap()[0].roles, 0);

        let many = build_account_index(
            root.path(),
            AccountIndexBuildOptions {
                sort_memory_bytes: mem::size_of::<SortRecord>() * 3,
            },
        )
        .unwrap();
        assert!(many.sort_runs > 1);
        assert_eq!(one.object_bytes, many.object_bytes);
        assert_eq!(fs::read(root.path().join(postings::PATH)).unwrap(), bytes);
    }

    #[test]
    fn repeated_account_roles_are_or_merged_into_one_posting() {
        let transaction = Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 0,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::Legacy {
                static_accounts: vec![PubkeyId::new(10).unwrap()],
                instructions: vec![Instruction {
                    program_position: 0,
                    account_positions: vec![0],
                    data: Vec::new(),
                }],
            },
        };
        let inner = TransactionInner {
            groups: vec![InnerGroup {
                parent_index: 0,
                instructions: vec![InnerInstruction {
                    stack_height: Some(2),
                    instruction: Instruction {
                        program_position: 0,
                        account_positions: vec![0],
                        data: Vec::new(),
                    },
                }],
            }],
        };
        let roles = transaction_roles(&transaction, Some(&inner), 0, u64::MAX).unwrap();
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[&10], postings::ROLE_MASK);
    }

    #[test]
    fn hot_key_uses_bounded_continuations_and_streams_lookup() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        write_fixture(root.path(), archive_id, 1);
        let run_directory = root.path().join("hot-account-run");
        fs::create_dir(&run_directory).unwrap();
        let mut records = (0..10_000_u64)
            .map(|transaction_ordinal| SortRecord {
                account_id: 10,
                transaction_ordinal,
                roles: 0,
            })
            .collect::<Vec<_>>();
        let run = flush_run(&run_directory, 0, &mut records).unwrap();
        let report = write_merged_index(
            root.path(),
            archive_id,
            vec![run],
            10_000,
            1,
            1,
            PageLimits {
                max_postings: 100,
                max_keys: 100,
            },
        )
        .unwrap();
        assert_eq!(report.pages, 100);
        assert_eq!(report.continuation_pages, 100);
        assert_eq!(report.peak_page_postings, 100);

        let mut next_ordinal = 0_u64;
        let lookup = visit_account_postings(root.path(), 10, |posting| {
            assert_eq!(posting.transaction_ordinal, next_ordinal);
            assert_eq!(posting.roles, 0);
            next_ordinal += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(next_ordinal, 10_000);
        assert_eq!(lookup.postings, 10_000);
        assert_eq!(lookup.pages_read, 100);

        let file = File::open(root.path().join(postings::PATH)).unwrap();
        let header = validate_open_file(&file, postings::PATH, archive_id).unwrap();
        let (_, directory) = read_directory(&file, header).unwrap();
        assert_eq!(directory.len(), 100);
        assert!(!directory[0].continued_from_previous());
        assert!(directory[0].continues_in_next());
        assert!(directory[50].continued_from_previous());
        assert!(directory[50].continues_in_next());
        assert!(directory[99].continued_from_previous());
        assert!(!directory[99].continues_in_next());
    }

    #[test]
    fn ten_million_posting_hot_key_stays_within_the_page_bound() {
        let mut fragment = PostingPageBuffer::new(DEFAULT_MAX_POSTINGS_PER_PAGE);
        let mut emitted = 0_usize;
        for transaction_ordinal in 0..10_000_000_u64 {
            fragment
                .push(postings::Posting {
                    transaction_ordinal,
                    roles: 0,
                })
                .unwrap();
            if fragment.is_full() {
                emitted += fragment.take().len();
            }
        }
        emitted += fragment.take().len();
        assert_eq!(emitted, 10_000_000);
        assert_eq!(fragment.peak, DEFAULT_MAX_POSTINGS_PER_PAGE);
    }

    #[test]
    fn zstd_page_expansion_above_the_declared_length_is_rejected() {
        let root = tempdir().unwrap();
        let decoded = vec![0_u8; 1 << 20];
        let stored = compress_posting_page(&decoded).unwrap();
        assert!(stored.len() < 1024);
        let path = root.path().join("page.zst");
        fs::write(&path, &stored).unwrap();
        let entry = postings::PageDirectoryEntry {
            first_key: 1,
            last_key: 1,
            offset: 0,
            stored_len: stored.len() as u32,
            decoded_len: 1024,
            key_count: 1,
            flags: 0,
        };
        assert!(read_posting_page(&File::open(path).unwrap(), entry).is_err());
    }

    #[test]
    fn fragment_visitor_rejects_cross_page_ordinal_regression() {
        let mut previous = None;
        let mut seen = 0_u64;
        let mut visit = |_posting| {
            seen += 1;
            Ok(())
        };
        visit_posting_fragment(
            &[postings::Posting {
                transaction_ordinal: 9,
                roles: 0,
            }],
            &mut previous,
            &mut visit,
        )
        .unwrap();
        let error = visit_posting_fragment(
            &[postings::Posting {
                transaction_ordinal: 8,
                roles: 0,
            }],
            &mut previous,
            &mut visit,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("do not strictly ascend"));
        assert_eq!(seen, 1);
    }

    #[test]
    fn a_target_input_from_another_archive_is_rejected_before_output() {
        let root = tempdir().unwrap();
        write_fixture(root.path(), ArchiveId::new([9; 16]), 1);
        let error = format!(
            "{:#}",
            build_account_index(root.path(), AccountIndexBuildOptions::default()).unwrap_err()
        );
        assert!(error.contains("different archive ID"), "{error}");
        assert!(!root.path().join(postings::PATH).exists());
    }

    #[test]
    fn unavailable_loaded_pubkeys_fail_before_index_publication() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        let transaction = Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 0,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::V0 {
                static_accounts: vec![PubkeyId(1)],
                loaded_addresses: LoadedAddresses::Unavailable,
                lookups: vec![AddressTableLookup {
                    table_id: PubkeyId(2),
                    writable_indexes: vec![0],
                    readonly_indexes: Vec::new(),
                }],
                instructions: vec![Instruction {
                    program_position: 0,
                    account_positions: vec![1],
                    data: Vec::new(),
                }],
            },
        };
        write_merged_fixture(
            root.path(),
            archive_id,
            archive_id,
            archive_id,
            2,
            vec![FixtureBlock {
                slot: 1,
                parent_slot: 0,
                transactions: vec![transaction],
                inner: vec![None],
            }],
        );
        let error = format!(
            "{:#}",
            build_account_index(root.path(), AccountIndexBuildOptions::default()).unwrap_err()
        );
        assert!(
            error.contains("loaded-address pubkeys are unavailable"),
            "{error}"
        );
        assert!(!root.path().join(postings::PATH).exists());
    }

    #[test]
    fn output_header_has_account_role_and_schema() {
        let root = tempdir().unwrap();
        write_fixture(root.path(), ArchiveId::new([7; 16]), 1);
        build_account_index(root.path(), AccountIndexBuildOptions::default()).unwrap();
        let file = File::open(root.path().join(postings::PATH)).unwrap();
        let header = validate_open_file(&file, postings::PATH, ArchiveId::new([7; 16])).unwrap();
        assert_eq!(header.role, ObjectRole::IndexAccounts);
        assert_eq!(header.schema, ACCOUNT_INDEX_SCHEMA);
        assert_eq!(header.record_count, 5);
    }
}
