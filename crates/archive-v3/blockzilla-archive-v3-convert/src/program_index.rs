//! Bounded builder and point reader for `indexes/programs.pages`.
//!
//! Generation writes fixed-width `(program ID, transaction, roles)` records
//! into bounded sort runs. A deterministic k-way merge then writes key-aligned
//! posting pages. The run size changes memory and temporary I/O only; it does
//! not change the final object bytes.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use blockzilla_archive_v3::{
    ArchiveId, FILE_HEADER_LEN, FileHeader,
    catalog::blocks as catalog_blocks,
    dictionary::pubkeys,
    indexes::programs as program_postings,
    ledger::transactions::{Instruction, Transaction},
    runtime::inner_instructions::TransactionInner,
};

use crate::{
    canonical_reader::{DEFAULT_MAX_BLOCK_DECODED_BYTES, scan_transactions_with_inner},
    container::{HeaderedWriter, decode_zstd_exact, validate_open_file},
    transaction_view::ResolvedAccounts,
};

const SORT_RECORD_LEN: usize = 16;
const DEFAULT_SORT_MEMORY_BYTES: usize = 128 << 20;
const DEFAULT_MAX_POSTINGS_PER_PAGE: usize = 64 * 1024;
const DEFAULT_MAX_KEYS_PER_PAGE: usize = 4096;
const IO_BUFFER_BYTES: usize = 1 << 20;
/// Fixed fan-in bounds open files and merge-heap memory independently of the
/// archive transaction count.
const MERGE_FAN_IN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgramIndexBuildOptions {
    /// Maximum bytes for unsorted external-sort records.
    pub sort_memory_bytes: usize,
    /// Maximum postings held in one output page. A hotter key continues in a
    /// chain of single-key pages.
    pub max_postings_per_page: usize,
    /// Maximum complete keys packed into one non-continuation page.
    pub max_keys_per_page: usize,
}

impl Default for ProgramIndexBuildOptions {
    fn default() -> Self {
        Self {
            sort_memory_bytes: DEFAULT_SORT_MEMORY_BYTES,
            max_postings_per_page: DEFAULT_MAX_POSTINGS_PER_PAGE,
            max_keys_per_page: DEFAULT_MAX_KEYS_PER_PAGE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgramIndexBuildReport {
    pub archive_id: ArchiveId,
    pub blocks: u64,
    pub transactions: u64,
    pub top_level_instructions: u64,
    pub cpi_instructions: u64,
    pub distinct_programs: u64,
    pub postings: u64,
    pub sort_runs: u64,
    pub pages: u64,
    pub continuation_pages: u64,
    pub object_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SortRecord {
    program_id: u32,
    transaction_ordinal: u64,
    roles: u8,
}

const SORT_RECORD_MEMORY_BYTES: usize = std::mem::size_of::<SortRecord>();

impl SortRecord {
    fn encode(self) -> [u8; SORT_RECORD_LEN] {
        let mut out = [0_u8; SORT_RECORD_LEN];
        out[0..4].copy_from_slice(&self.program_id.to_le_bytes());
        out[4] = self.roles;
        // 5..8 is reserved and stays zero.
        out[8..16].copy_from_slice(&self.transaction_ordinal.to_le_bytes());
        out
    }

    fn decode(input: &[u8; SORT_RECORD_LEN]) -> Result<Self> {
        ensure!(
            input[5..8] == [0, 0, 0],
            "sort record reserved bytes are set"
        );
        let record = Self {
            program_id: u32::from_le_bytes(input[0..4].try_into().expect("4 bytes")),
            transaction_ordinal: u64::from_le_bytes(input[8..16].try_into().expect("8 bytes")),
            roles: input[4],
        };
        ensure!(
            record.program_id != 0,
            "sort record has reserved program ID zero"
        );
        ensure!(
            record.roles != 0 && record.roles & !program_postings::ROLE_MASK == 0,
            "sort record has invalid program roles {:#x}",
            record.roles
        );
        Ok(record)
    }
}

struct StagingDirectory {
    path: PathBuf,
}

impl StagingDirectory {
    fn create(root: &Path) -> Result<Self> {
        let path = root.join(format!(".program-index.building-{}", std::process::id()));
        fs::create_dir(&path).with_context(|| {
            format!(
                "create program-index staging directory {}; remove an abandoned directory first if needed",
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

fn open_catalog(root: &Path) -> Result<(File, FileHeader)> {
    let path = root.join(catalog_blocks::PATH);
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let mut header_bytes = [0_u8; FILE_HEADER_LEN];
    file.read_exact_at(&mut header_bytes, 0)
        .context("read catalog common header")?;
    let initial = FileHeader::decode(&header_bytes).context("decode catalog common header")?;
    let header = validate_open_file(&file, catalog_blocks::PATH, initial.archive_id)?;
    ensure!(
        header.payload_bytes == header.decoded_bytes,
        "catalog fixed-width payload must be raw"
    );
    let expected = header
        .record_count
        .checked_mul(catalog_blocks::ROW_LEN as u64)
        .context("catalog length overflow")?;
    ensure!(
        header.payload_bytes == expected,
        "catalog has {} payload bytes, expected {expected}",
        header.payload_bytes
    );
    Ok((file, header))
}

fn validate_pubkey_dictionary(root: &Path, archive_id: ArchiveId) -> Result<u32> {
    let path = root.join(pubkeys::PATH);
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let header = validate_open_file(&file, pubkeys::PATH, archive_id)?;
    ensure!(
        header.payload_bytes == header.decoded_bytes,
        "pubkey dictionary must remain raw"
    );
    let count = u32::try_from(header.record_count).context("pubkey count exceeds u32")?;
    let expected = header
        .record_count
        .checked_mul(pubkeys::RECORD_LEN as u64)
        .context("pubkey dictionary length overflow")?;
    ensure!(
        header.payload_bytes == expected,
        "pubkey dictionary has {} payload bytes, expected {expected}",
        header.payload_bytes
    );
    Ok(count)
}

fn add_instruction_program(
    merged: &mut BTreeMap<u32, u8>,
    accounts: ResolvedAccounts<'_>,
    instruction: &Instruction,
    role: u8,
    pubkey_count: u32,
) -> Result<()> {
    let position = usize::try_from(instruction.program_position)
        .context("program account position exceeds usize")?;
    let program_id = accounts.get(position).with_context(|| {
        format!(
            "program account position {position} has no resolved pubkey; loaded-address coverage is {}",
            if accounts.is_complete() {
                "complete"
            } else {
                "unavailable"
            }
        )
    })?;
    ensure!(
        program_id != 0 && program_id <= pubkey_count,
        "program dictionary ID {program_id} is outside 1..={pubkey_count}"
    );
    for account_position in &instruction.account_positions {
        ensure!(
            usize::try_from(*account_position)
                .ok()
                .is_some_and(|position| position < accounts.resolved_len()),
            "instruction account position {account_position} is outside the transaction"
        );
    }
    *merged.entry(program_id).or_default() |= role;
    Ok(())
}

fn transaction_programs(
    transaction: &Transaction,
    inner: Option<&TransactionInner>,
    pubkey_count: u32,
) -> Result<BTreeMap<u32, u8>> {
    let mut merged = BTreeMap::new();
    let accounts = ResolvedAccounts::new(transaction);
    let top_level = transaction.message.instructions();
    for instruction in top_level {
        add_instruction_program(
            &mut merged,
            accounts,
            instruction,
            program_postings::ROLE_TOP_LEVEL,
            pubkey_count,
        )?;
    }
    if let Some(inner) = inner {
        for group in &inner.groups {
            ensure!(
                usize::try_from(group.parent_index)
                    .ok()
                    .is_some_and(|parent| parent < top_level.len()),
                "inner-instruction parent {} is outside {} top-level instructions",
                group.parent_index,
                top_level.len()
            );
            for inner_instruction in &group.instructions {
                add_instruction_program(
                    &mut merged,
                    accounts,
                    &inner_instruction.instruction,
                    program_postings::ROLE_CPI,
                    pubkey_count,
                )?;
            }
        }
    }
    Ok(merged)
}

fn flush_sort_run(
    run_directory: &Path,
    run_number: usize,
    records: &mut Vec<SortRecord>,
) -> Result<PathBuf> {
    ensure!(
        !records.is_empty(),
        "cannot write an empty program sort run"
    );
    records.sort_unstable();
    for pair in records.windows(2) {
        ensure!(
            pair[0] < pair[1],
            "program sort run contains duplicate record {:?}",
            pair[0]
        );
    }
    let path = run_directory.join(format!("run-{run_number:08}.bin"));
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("create program sort run {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
    for record in records.iter().copied() {
        writer
            .write_all(&record.encode())
            .with_context(|| format!("write program sort run {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush program sort run {}", path.display()))?;
    writer
        .get_ref()
        .sync_all()
        .with_context(|| format!("sync program sort run {}", path.display()))?;
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
            .with_context(|| format!("open program sort run {}", path.display()))?;
        let bytes = file.metadata()?.len();
        ensure!(
            bytes.is_multiple_of(SORT_RECORD_LEN as u64),
            "program sort run {} is not record-aligned",
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
            .with_context(|| format!("read program sort run {}", self.path.display()))?;
        self.remaining -= 1;
        Ok(Some(SortRecord::decode(&bytes)?))
    }
}

struct MergedRecords {
    readers: Vec<RunReader>,
    heap: BinaryHeap<Reverse<(SortRecord, usize)>>,
    previous: Option<SortRecord>,
    emitted: u64,
}

impl MergedRecords {
    fn open(run_paths: Vec<PathBuf>) -> Result<Self> {
        let mut readers = run_paths
            .into_iter()
            .map(RunReader::open)
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (run, reader) in readers.iter_mut().enumerate() {
            if let Some(record) = reader.next()? {
                heap.push(Reverse((record, run)));
            }
        }
        Ok(Self {
            readers,
            heap,
            previous: None,
            emitted: 0,
        })
    }

    fn next(&mut self) -> Result<Option<SortRecord>> {
        let Some(Reverse((record, run))) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(previous) = self.previous {
            ensure!(
                record > previous,
                "merged program records do not strictly ascend: {previous:?} then {record:?}"
            );
        }
        self.previous = Some(record);
        self.emitted = self
            .emitted
            .checked_add(1)
            .context("merged program record count overflow")?;
        if let Some(next) = self.readers[run].next()? {
            self.heap.push(Reverse((next, run)));
        }
        Ok(Some(record))
    }
}

fn merge_run_group(run_directory: &Path, serial: u64, inputs: Vec<PathBuf>) -> Result<PathBuf> {
    ensure!(!inputs.is_empty(), "cannot merge an empty run group");
    ensure!(
        inputs.len() <= MERGE_FAN_IN,
        "program merge group exceeds the fixed fan-in"
    );
    let output = run_directory.join(format!("merge-{serial:016}.bin"));
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(&output)
        .with_context(|| format!("create merged program run {}", output.display()))?;
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
    let mut merged = MergedRecords::open(inputs.clone())?;
    while let Some(record) = merged.next()? {
        writer
            .write_all(&record.encode())
            .with_context(|| format!("write merged program run {}", output.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush merged program run {}", output.display()))?;
    writer
        .get_ref()
        .sync_all()
        .with_context(|| format!("sync merged program run {}", output.display()))?;
    for input in inputs {
        fs::remove_file(&input)
            .with_context(|| format!("remove consumed program run {}", input.display()))?;
    }
    Ok(output)
}

/// Online leveled run set. Each level keeps fewer than `MERGE_FAN_IN` paths,
/// so path memory and open-file count stay small for very many initial runs.
struct RunAccumulator<'a> {
    run_directory: &'a Path,
    levels: Vec<Vec<PathBuf>>,
    merge_serial: u64,
}

impl<'a> RunAccumulator<'a> {
    fn new(run_directory: &'a Path) -> Self {
        Self {
            run_directory,
            levels: Vec::new(),
            merge_serial: 0,
        }
    }

    fn add(&mut self, mut path: PathBuf) -> Result<()> {
        let mut level = 0_usize;
        loop {
            if self.levels.len() == level {
                self.levels.push(Vec::new());
            }
            self.levels[level].push(path);
            if self.levels[level].len() < MERGE_FAN_IN {
                return Ok(());
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            path = merge_run_group(self.run_directory, self.merge_serial, inputs)?;
            self.merge_serial = self
                .merge_serial
                .checked_add(1)
                .context("program merge serial overflow")?;
            level += 1;
        }
    }

    fn finish(mut self) -> Result<Vec<PathBuf>> {
        let mut survivors = Vec::new();
        for inputs in self.levels {
            if inputs.is_empty() {
                continue;
            }
            if inputs.len() == 1 {
                survivors.extend(inputs);
            } else {
                survivors.push(merge_run_group(
                    self.run_directory,
                    self.merge_serial,
                    inputs,
                )?);
                self.merge_serial = self
                    .merge_serial
                    .checked_add(1)
                    .context("program merge serial overflow")?;
            }
        }
        while survivors.len() > MERGE_FAN_IN {
            let inputs = survivors.drain(..MERGE_FAN_IN).collect::<Vec<PathBuf>>();
            survivors.push(merge_run_group(
                self.run_directory,
                self.merge_serial,
                inputs,
            )?);
            self.merge_serial = self
                .merge_serial
                .checked_add(1)
                .context("program merge serial overflow")?;
        }
        Ok(survivors)
    }
}

fn compress_page(payload: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = zstd::Encoder::new(Vec::new(), 3).context("create program page encoder")?;
    encoder
        .include_checksum(true)
        .context("enable program page checksum")?;
    encoder
        .write_all(payload)
        .context("compress program posting page")?;
    encoder.finish().context("finish program posting page")
}

fn write_page(
    writer: &mut HeaderedWriter,
    directory: &mut Vec<program_postings::PageDirectoryEntry>,
    keys: &[program_postings::KeyPostings],
    flags: u16,
) -> Result<()> {
    let decoded = program_postings::encode_page(keys).context("encode program posting page")?;
    let compressed = compress_page(&decoded)?;
    let stored = if compressed.len() < decoded.len() {
        compressed.as_slice()
    } else {
        decoded.as_slice()
    };
    let offset = writer.append(stored, decoded.len() as u64)?;
    directory.push(program_postings::PageDirectoryEntry {
        first_key: keys[0].key,
        last_key: keys[keys.len() - 1].key,
        offset,
        stored_len: u32::try_from(stored.len()).context("program page exceeds u32")?,
        decoded_len: u32::try_from(decoded.len()).context("program page exceeds u32")?,
        key_count: u32::try_from(keys.len()).context("program page key count exceeds u32")?,
        flags,
    });
    Ok(())
}

fn flush_complete_page(
    writer: &mut HeaderedWriter,
    directory: &mut Vec<program_postings::PageDirectoryEntry>,
    pending: &mut Vec<program_postings::KeyPostings>,
    pending_postings: &mut usize,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    write_page(writer, directory, pending, 0)?;
    pending.clear();
    *pending_postings = 0;
    Ok(())
}

struct MergeReport {
    distinct_programs: u64,
    pages: u64,
    continuation_pages: u64,
    object_bytes: u64,
}

fn merge_runs_to_object(
    staging_root: &Path,
    archive_id: ArchiveId,
    run_paths: Vec<PathBuf>,
    expected_postings: u64,
    options: ProgramIndexBuildOptions,
) -> Result<MergeReport> {
    let mut merged = MergedRecords::open(run_paths)?;
    let mut writer = HeaderedWriter::create(staging_root, program_postings::PATH, IO_BUFFER_BYTES)?;
    let mut directory = Vec::new();
    let mut pending = Vec::new();
    let mut pending_postings = 0_usize;
    let mut next = merged.next()?;
    let mut distinct_programs = 0_u64;
    let mut continuation_pages = 0_u64;

    while let Some(first) = next {
        let key = first.program_id;
        distinct_programs = distinct_programs
            .checked_add(1)
            .context("distinct program count overflow")?;
        let mut continued_from_previous = false;
        loop {
            let mut postings = Vec::new();
            while postings.len() < options.max_postings_per_page {
                let record = next.take().expect("current key has a record");
                ensure!(
                    record.program_id == key,
                    "program merge key changed unexpectedly"
                );
                postings.push(program_postings::Posting {
                    transaction_ordinal: record.transaction_ordinal,
                    roles: record.roles,
                });
                next = merged.next()?;
                if next.is_none_or(|record| record.program_id != key) {
                    break;
                }
            }
            let continues_in_next = next.is_some_and(|record| record.program_id == key);
            let entry = program_postings::KeyPostings { key, postings };
            if continued_from_previous || continues_in_next {
                flush_complete_page(
                    &mut writer,
                    &mut directory,
                    &mut pending,
                    &mut pending_postings,
                )?;
                let mut flags = 0;
                if continued_from_previous {
                    flags |= program_postings::PAGE_FLAG_CONTINUED_FROM_PREVIOUS;
                }
                if continues_in_next {
                    flags |= program_postings::PAGE_FLAG_CONTINUES_IN_NEXT;
                }
                write_page(&mut writer, &mut directory, &[entry], flags)?;
                continuation_pages = continuation_pages
                    .checked_add(1)
                    .context("program continuation page count overflow")?;
            } else {
                let would_exceed_postings = pending_postings
                    .checked_add(entry.postings.len())
                    .is_none_or(|count| count > options.max_postings_per_page);
                if pending.len() == options.max_keys_per_page || would_exceed_postings {
                    flush_complete_page(
                        &mut writer,
                        &mut directory,
                        &mut pending,
                        &mut pending_postings,
                    )?;
                }
                pending_postings += entry.postings.len();
                pending.push(entry);
            }
            if !continues_in_next {
                break;
            }
            continued_from_previous = true;
        }
    }
    flush_complete_page(
        &mut writer,
        &mut directory,
        &mut pending,
        &mut pending_postings,
    )?;
    ensure!(
        merged.emitted == expected_postings,
        "merged {} program postings, expected {expected_postings}",
        merged.emitted
    );

    let page_bytes_end = directory.last().map_or(FILE_HEADER_LEN as u64, |entry| {
        entry.offset + u64::from(entry.stored_len)
    });
    program_postings::validate_directory(&directory, FILE_HEADER_LEN as u64, page_bytes_end)?;
    let mut directory_bytes = Vec::with_capacity(
        directory
            .len()
            .checked_mul(program_postings::DIRECTORY_ENTRY_LEN)
            .context("program directory length overflow")?,
    );
    for entry in &directory {
        directory_bytes.extend_from_slice(&entry.encode());
    }
    let directory_offset = writer.append(&directory_bytes, directory_bytes.len() as u64)?;
    ensure!(
        directory_offset == page_bytes_end,
        "program page byte accounting drift"
    );
    let footer = program_postings::DirectoryFooter {
        directory_offset,
        page_count: directory.len() as u64,
    }
    .encode();
    writer.append(&footer, footer.len() as u64)?;
    let finished = writer.finish(archive_id, expected_postings)?;
    Ok(MergeReport {
        distinct_programs,
        pages: directory.len() as u64,
        continuation_pages,
        object_bytes: finished.file_bytes,
    })
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

/// Rebuild the program index from the merged transaction and CPI streams.
/// Temporary sort space and sort memory are bounded by `options`.
pub fn build_program_index(
    root: &Path,
    options: ProgramIndexBuildOptions,
) -> Result<ProgramIndexBuildReport> {
    ensure!(
        options.sort_memory_bytes >= SORT_RECORD_MEMORY_BYTES,
        "program sort memory must hold at least one {SORT_RECORD_MEMORY_BYTES}-byte in-memory record"
    );
    ensure!(
        options.max_postings_per_page > 0,
        "a program page must allow at least one posting"
    );
    ensure!(
        options.max_keys_per_page > 0 && options.max_keys_per_page <= options.max_postings_per_page,
        "program max keys must be in 1..=max postings"
    );
    // Ten bytes for the packed ordinal plus conservative key/count overhead.
    ensure!(
        options.max_postings_per_page <= (program_postings::MAX_PAGE_DECODED_BYTES as usize) / 32,
        "program max postings can exceed the page decode guard"
    );
    ensure!(
        root.is_dir(),
        "{} is not an archive directory",
        root.display()
    );

    let archive_id =
        crate::canonical_reader::CanonicalReader::open(root, DEFAULT_MAX_BLOCK_DECODED_BYTES)?
            .archive_id();
    let pubkey_count = validate_pubkey_dictionary(root, archive_id)?;

    let staging = StagingDirectory::create(root)?;
    let run_directory = staging.path.join("program-index-runs");
    fs::create_dir(&run_directory)
        .with_context(|| format!("create program run directory {}", run_directory.display()))?;
    let run_capacity = options.sort_memory_bytes / SORT_RECORD_MEMORY_BYTES;
    let mut records = Vec::new();
    let mut run_accumulator = RunAccumulator::new(&run_directory);
    let mut sort_runs = 0_u64;
    let mut posting_count = 0_u64;
    let scan = scan_transactions_with_inner(root, DEFAULT_MAX_BLOCK_DECODED_BYTES, |block| {
        for (index, transaction) in block.replay.transactions.iter().enumerate() {
            let transaction_ordinal = block
                .replay
                .catalog
                .first_transaction
                .checked_add(index as u64)
                .context("transaction ordinal overflow")?;
            let programs =
                transaction_programs(transaction, block.inner[index].as_ref(), pubkey_count)
                    .with_context(|| {
                        format!("validate transaction ordinal {transaction_ordinal}")
                    })?;
            for (program_id, roles) in programs {
                records.push(SortRecord {
                    program_id,
                    transaction_ordinal,
                    roles,
                });
                posting_count = posting_count
                    .checked_add(1)
                    .context("program posting count overflow")?;
                if records.len() == run_capacity {
                    let path = flush_sort_run(
                        &run_directory,
                        usize::try_from(sort_runs)
                            .context("program sort run count exceeds usize")?,
                        &mut records,
                    )?;
                    sort_runs = sort_runs
                        .checked_add(1)
                        .context("program sort run count overflow")?;
                    run_accumulator.add(path)?;
                }
            }
        }
        Ok(())
    })?;
    if !records.is_empty() {
        let path = flush_sort_run(
            &run_directory,
            usize::try_from(sort_runs).context("program sort run count exceeds usize")?,
            &mut records,
        )?;
        sort_runs = sort_runs
            .checked_add(1)
            .context("program sort run count overflow")?;
        run_accumulator.add(path)?;
    }
    let run_paths = run_accumulator.finish()?;
    let merge = merge_runs_to_object(&staging.path, archive_id, run_paths, posting_count, options)?;

    fs::remove_dir_all(&run_directory).with_context(|| {
        format!(
            "remove owned program run directory {}",
            run_directory.display()
        )
    })?;
    let final_directory = root.join("indexes");
    fs::create_dir_all(&final_directory)
        .with_context(|| format!("create {}", final_directory.display()))?;
    fs::rename(
        staging.path.join(program_postings::PATH),
        root.join(program_postings::PATH),
    )
    .context("publish program index")?;
    sync_directory(&final_directory)?;

    Ok(ProgramIndexBuildReport {
        archive_id,
        blocks: scan.blocks,
        transactions: scan.transactions,
        top_level_instructions: scan.top_level_instructions,
        cpi_instructions: scan.cpi_instructions,
        distinct_programs: merge.distinct_programs,
        postings: posting_count,
        sort_runs,
        pages: merge.pages,
        continuation_pages: merge.continuation_pages,
        object_bytes: merge.object_bytes,
    })
}

fn archive_id_from_catalog(root: &Path) -> Result<ArchiveId> {
    let (file, header) = open_catalog(root)?;
    drop(file);
    Ok(header.archive_id)
}

fn read_program_directory(
    file: &File,
    header: FileHeader,
) -> Result<Vec<program_postings::PageDirectoryEntry>> {
    let file_len = file.metadata()?.len();
    let footer_offset = file_len
        .checked_sub(program_postings::DIRECTORY_FOOTER_LEN as u64)
        .context("program index is too short for its footer")?;
    ensure!(
        footer_offset >= FILE_HEADER_LEN as u64,
        "program-index footer overlaps its common header"
    );
    let mut footer_bytes = [0_u8; program_postings::DIRECTORY_FOOTER_LEN];
    file.read_exact_at(&mut footer_bytes, footer_offset)
        .context("read program-index footer")?;
    let footer = program_postings::DirectoryFooter::decode(&footer_bytes)?;
    let directory_len = footer
        .page_count
        .checked_mul(program_postings::DIRECTORY_ENTRY_LEN as u64)
        .context("program directory length overflow")?;
    ensure!(
        footer.directory_offset >= FILE_HEADER_LEN as u64
            && footer
                .directory_offset
                .checked_add(directory_len)
                .is_some_and(|end| end == footer_offset),
        "program directory does not end at its footer"
    );
    let mut bytes =
        vec![0_u8; usize::try_from(directory_len).context("program directory exceeds usize")?];
    file.read_exact_at(&mut bytes, footer.directory_offset)
        .context("read program-index directory")?;
    let entries = bytes
        .chunks_exact(program_postings::DIRECTORY_ENTRY_LEN)
        .map(program_postings::PageDirectoryEntry::decode)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    program_postings::validate_directory(
        &entries,
        FILE_HEADER_LEN as u64,
        footer.directory_offset,
    )?;
    let decoded_pages = entries.iter().try_fold(0_u64, |total, entry| {
        total.checked_add(u64::from(entry.decoded_len))
    });
    let expected_decoded = decoded_pages
        .and_then(|total| total.checked_add(directory_len))
        .and_then(|total| total.checked_add(program_postings::DIRECTORY_FOOTER_LEN as u64))
        .context("program decoded-byte count overflow")?;
    ensure!(
        header.decoded_bytes == expected_decoded,
        "program header declares {} decoded bytes, directory accounts for {expected_decoded}",
        header.decoded_bytes
    );
    Ok(entries)
}

/// Point-read all postings for one program dictionary ID.
///
/// This validates the common header against the catalog archive ID, validates
/// the full directory and continuation topology, and fetches only the pages
/// whose key range can contain `program_id`.
pub fn read_program_postings(
    root: &Path,
    program_id: u32,
) -> Result<Vec<program_postings::Posting>> {
    ensure!(program_id != 0, "program dictionary ID zero is reserved");
    let archive_id = archive_id_from_catalog(root)?;
    let path = root.join(program_postings::PATH);
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let header = validate_open_file(&file, program_postings::PATH, archive_id)?;
    let directory = read_program_directory(&file, header)?;
    let range = program_postings::pages_for_key(&directory, program_id);
    let mut postings: Vec<program_postings::Posting> = Vec::new();
    for entry in &directory[range] {
        let mut stored = vec![0_u8; entry.stored_len as usize];
        file.read_exact_at(&mut stored, entry.offset)
            .context("read program posting page")?;
        let decoded = if entry.is_compressed() {
            decode_zstd_exact(&stored, entry.decoded_len as usize, "program posting page")?
        } else {
            stored
        };
        let page = program_postings::decode_page(&decoded, entry.first_key, entry.key_count)?;
        ensure!(
            page.last().is_some_and(|last| last.key == entry.last_key),
            "program page last key does not match its directory entry"
        );
        if let Some(found) = program_postings::find_key(&page, program_id) {
            if let (Some(previous), Some(next)) = (postings.last(), found.postings.first()) {
                ensure!(
                    next.transaction_ordinal > previous.transaction_ordinal,
                    "program continuation postings do not strictly ascend"
                );
            }
            postings.extend_from_slice(&found.postings);
        }
    }
    Ok(postings)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use blockzilla_archive_v3::{
        ledger::transactions::{HashOwner, HashRef, Message, MessageHeader, PubkeyId},
        runtime::inner_instructions::{InnerGroup, InnerInstruction},
    };
    use tempfile::tempdir;

    use crate::test_fixture::{FixtureBlock, write_merged_fixture};

    use super::*;

    fn instruction(program_position: u32) -> Instruction {
        Instruction {
            program_position,
            account_positions: Vec::new(),
            data: Vec::new(),
        }
    }

    fn transaction(instructions: Vec<Instruction>) -> Transaction {
        Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 2,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::Legacy {
                static_accounts: [1, 2, 3]
                    .into_iter()
                    .map(|id| PubkeyId::new(id).unwrap())
                    .collect(),
                instructions,
            },
        }
    }

    fn write_candidate(root: &Path, inner_archive_id: ArchiveId) -> ArchiveId {
        let archive_id = ArchiveId::new([7; 16]);
        let transactions = vec![
            transaction(vec![instruction(0), instruction(0), instruction(1)]),
            transaction(vec![instruction(1)]),
            transaction(vec![instruction(0)]),
        ];
        let inner = vec![
            Some(TransactionInner {
                groups: vec![InnerGroup {
                    parent_index: 0,
                    instructions: vec![InnerInstruction {
                        stack_height: Some(2),
                        instruction: instruction(0),
                    }],
                }],
            }),
            Some(TransactionInner {
                groups: vec![InnerGroup {
                    parent_index: 0,
                    instructions: vec![InnerInstruction {
                        stack_height: Some(2),
                        instruction: instruction(2),
                    }],
                }],
            }),
            None,
        ];
        write_merged_fixture(
            root,
            archive_id,
            archive_id,
            inner_archive_id,
            3,
            vec![FixtureBlock {
                slot: 10,
                parent_slot: 9,
                transactions,
                inner,
            }],
        );
        archive_id
    }

    fn options(sort_records: usize, max_postings_per_page: usize) -> ProgramIndexBuildOptions {
        ProgramIndexBuildOptions {
            sort_memory_bytes: SORT_RECORD_MEMORY_BYTES * sort_records,
            max_postings_per_page,
            max_keys_per_page: max_postings_per_page,
        }
    }

    #[test]
    fn merges_top_level_and_cpi_roles_once_per_transaction() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        write_candidate(root.path(), archive_id);
        let report = build_program_index(root.path(), options(100, 100)).unwrap();
        assert_eq!(report.distinct_programs, 3);
        assert_eq!(report.postings, 5);
        assert_eq!(
            read_program_postings(root.path(), 1).unwrap(),
            vec![
                program_postings::Posting {
                    transaction_ordinal: 0,
                    roles: program_postings::ROLE_TOP_LEVEL | program_postings::ROLE_CPI,
                },
                program_postings::Posting {
                    transaction_ordinal: 2,
                    roles: program_postings::ROLE_TOP_LEVEL,
                },
            ]
        );
        assert_eq!(
            read_program_postings(root.path(), 3).unwrap(),
            vec![program_postings::Posting {
                transaction_ordinal: 1,
                roles: program_postings::ROLE_CPI,
            }]
        );
        assert!(read_program_postings(root.path(), 99).unwrap().is_empty());
    }

    #[test]
    fn one_and_many_sort_runs_produce_identical_bytes() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        write_candidate(root.path(), archive_id);
        let one = build_program_index(root.path(), options(100, 100)).unwrap();
        assert_eq!(one.sort_runs, 1);
        let expected = fs::read(root.path().join(program_postings::PATH)).unwrap();

        let many = build_program_index(root.path(), options(1, 100)).unwrap();
        assert_eq!(many.sort_runs, 5);
        let actual = fs::read(root.path().join(program_postings::PATH)).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn hot_program_uses_a_valid_continuation_chain() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        write_candidate(root.path(), archive_id);
        let report = build_program_index(root.path(), options(2, 1)).unwrap();
        assert_eq!(report.pages, 5);
        assert_eq!(report.continuation_pages, 4);
        let program_one = read_program_postings(root.path(), 1).unwrap();
        assert_eq!(program_one.len(), 2);
        assert_eq!(program_one[0].roles, program_postings::ROLE_MASK);
    }

    #[test]
    fn external_merge_fan_in_stays_bounded() {
        let root = tempdir().unwrap();
        let run_directory = root.path().join("runs");
        fs::create_dir(&run_directory).unwrap();
        let mut accumulator = RunAccumulator::new(&run_directory);
        for transaction_ordinal in 0..=(MERGE_FAN_IN as u64) {
            let mut records = vec![SortRecord {
                program_id: 1,
                transaction_ordinal,
                roles: program_postings::ROLE_TOP_LEVEL,
            }];
            let path =
                flush_sort_run(&run_directory, transaction_ordinal as usize, &mut records).unwrap();
            accumulator.add(path).unwrap();
        }
        let runs = accumulator.finish().unwrap();
        assert!(runs.len() <= MERGE_FAN_IN);
        let mut merged = MergedRecords::open(runs).unwrap();
        let mut ordinals = Vec::new();
        while let Some(record) = merged.next().unwrap() {
            ordinals.push(record.transaction_ordinal);
        }
        assert_eq!(ordinals, (0..=(MERGE_FAN_IN as u64)).collect::<Vec<_>>());
    }

    #[test]
    fn cross_archive_input_is_rejected_before_publication() {
        let root = tempdir().unwrap();
        write_candidate(root.path(), ArchiveId::new([8; 16]));
        let error = format!(
            "{:#}",
            build_program_index(root.path(), options(100, 100)).unwrap_err()
        );
        assert!(error.contains("different archive ID"), "{error}");
        assert!(!root.path().join(program_postings::PATH).exists());
    }
}
