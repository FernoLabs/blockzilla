//! Bounded builder and exact reader for `indexes/selectors.pages`.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    mem,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_index_archive_format::{ArchiveId, FILE_HEADER_LEN, FileHeader, indexes::selectors};

use crate::{
    canonical_reader::scan_transactions_with_inner,
    container::{HeaderedWriter, decode_zstd_exact, validate_open_file},
    transaction_view::ResolvedAccounts,
};

const SORT_RECORD_LEN: usize = 40;
const MERGE_FAN_IN: usize = 128;
const IO_BUFFER_BYTES: usize = 1 << 20;
const DEFAULT_SORT_MEMORY_BYTES: usize = 128 << 20;
const DEFAULT_PAGE_TARGET_BYTES: usize = 1 << 20;
const DEFAULT_MAX_POSTINGS_PER_PAGE: usize = 32_768;
const DEFAULT_MAX_SOURCE_PAGE_BYTES: usize = 512 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectorIndexBuildOptions {
    /// Maximum memory used for unsorted fixed-width records.
    pub sort_memory_bytes: usize,
    /// Soft decoded-byte target for pages that contain complete keys.
    pub page_target_decoded_bytes: usize,
    /// Hard posting bound for one page. Hot keys use continuation pages.
    pub max_postings_per_page: usize,
    /// Allocation guard for each canonical source page.
    pub max_source_page_decoded_bytes: usize,
}

impl Default for SelectorIndexBuildOptions {
    fn default() -> Self {
        Self {
            sort_memory_bytes: DEFAULT_SORT_MEMORY_BYTES,
            page_target_decoded_bytes: DEFAULT_PAGE_TARGET_BYTES,
            max_postings_per_page: DEFAULT_MAX_POSTINGS_PER_PAGE,
            max_source_page_decoded_bytes: DEFAULT_MAX_SOURCE_PAGE_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectorIndexBuildReport {
    pub archive_id: ArchiveId,
    pub blocks: u64,
    pub transactions: u64,
    pub top_level_instructions: u64,
    pub cpi_instructions: u64,
    pub postings: u64,
    pub sort_runs: u64,
    pub merge_passes: u32,
    pub pages: u64,
    pub continuation_pages: u64,
    pub object_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SortRecord {
    key: selectors::SelectorKey,
    posting: selectors::Posting,
}

impl SortRecord {
    fn encode(self) -> [u8; SORT_RECORD_LEN] {
        let mut out = [0_u8; SORT_RECORD_LEN];
        out[0..16].copy_from_slice(&self.key.encode());
        out[16..24].copy_from_slice(&self.posting.transaction_ordinal.to_le_bytes());
        out[24..32].copy_from_slice(&self.posting.role_local_instruction_ordinal.to_le_bytes());
        out[32] = self.posting.scope as u8;
        out
    }

    fn decode(input: &[u8]) -> Result<Self> {
        let bytes: &[u8; SORT_RECORD_LEN] = input
            .try_into()
            .map_err(|_| anyhow::anyhow!("selector sort record has wrong length"))?;
        ensure!(
            bytes[33..40] == [0; 7],
            "selector sort record has non-zero reserved bytes"
        );
        Ok(Self {
            key: selectors::SelectorKey::decode(&bytes[0..16])?,
            posting: selectors::Posting {
                transaction_ordinal: u64::from_le_bytes(bytes[16..24].try_into().expect("8 bytes")),
                scope: selectors::InstructionScope::try_from(bytes[32])?,
                role_local_instruction_ordinal: u64::from_le_bytes(
                    bytes[24..32].try_into().expect("8 bytes"),
                ),
            },
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CanonicalInstruction<'a> {
    key: selectors::SelectorKey,
    posting: selectors::Posting,
    data: &'a [u8],
}

fn selector_key(program_id: u32, data: &[u8]) -> Result<selectors::SelectorKey> {
    ensure!(
        program_id != 0,
        "canonical program registry ID zero is reserved"
    );
    selectors::SelectorKey::new(
        program_id,
        &data[..data.len().min(selectors::MAX_SELECTOR_LEN)],
    )
    .map_err(Into::into)
}

fn scan_canonical(
    root: &Path,
    max_source_page_decoded_bytes: usize,
    mut visit: impl FnMut(CanonicalInstruction<'_>) -> Result<()>,
) -> Result<ScanReport> {
    let mut top_ordinal = 0_u64;
    let mut cpi_ordinal = 0_u64;
    let scan = scan_transactions_with_inner(root, max_source_page_decoded_bytes, |block| {
        for (transaction_index, transaction) in block.replay.transactions.iter().enumerate() {
            let transaction_ordinal = block
                .replay
                .catalog
                .first_transaction
                .checked_add(transaction_index as u64)
                .context("transaction ordinal overflow")?;
            let accounts = ResolvedAccounts::new(transaction);
            let top = transaction.message.instructions();
            for instruction in top {
                let program_id = accounts
                        .get(instruction.program_position as usize)
                        .with_context(|| {
                            format!(
                                "top-level instruction {top_ordinal} program position {} has no resolved pubkey",
                                instruction.program_position
                            )
                        })?;
                visit(CanonicalInstruction {
                    key: selector_key(program_id, &instruction.data)?,
                    posting: selectors::Posting {
                        transaction_ordinal,
                        scope: selectors::InstructionScope::TopLevel,
                        role_local_instruction_ordinal: top_ordinal,
                    },
                    data: &instruction.data,
                })?;
                top_ordinal = top_ordinal
                    .checked_add(1)
                    .context("top-level instruction ordinal overflow")?;
            }

            if let Some(inner) = block.inner[transaction_index].as_ref() {
                for group in &inner.groups {
                    for inner_instruction in &group.instructions {
                        let instruction = &inner_instruction.instruction;
                        let program_id = accounts
                                .get(instruction.program_position as usize)
                                .with_context(|| {
                                    format!(
                                        "CPI instruction {cpi_ordinal} program position {} has no resolved pubkey",
                                        instruction.program_position
                                    )
                                })?;
                        visit(CanonicalInstruction {
                            key: selector_key(program_id, &instruction.data)?,
                            posting: selectors::Posting {
                                transaction_ordinal,
                                scope: selectors::InstructionScope::Cpi,
                                role_local_instruction_ordinal: cpi_ordinal,
                            },
                            data: &instruction.data,
                        })?;
                        cpi_ordinal = cpi_ordinal
                            .checked_add(1)
                            .context("CPI instruction ordinal overflow")?;
                    }
                }
            }
        }
        Ok(())
    })?;
    ensure!(
        top_ordinal == scan.top_level_instructions && cpi_ordinal == scan.cpi_instructions,
        "selector instruction counters disagree with the canonical scan"
    );

    Ok(ScanReport {
        archive_id: scan.archive_id,
        blocks: scan.blocks,
        transactions: scan.transactions,
        top_level_instructions: top_ordinal,
        cpi_instructions: cpi_ordinal,
    })
}

#[derive(Debug, Clone, Copy)]
struct ScanReport {
    archive_id: ArchiveId,
    blocks: u64,
    transactions: u64,
    top_level_instructions: u64,
    cpi_instructions: u64,
}

struct StagingDirectory {
    path: PathBuf,
}

impl StagingDirectory {
    fn create(root: &Path) -> Result<Self> {
        let path = root.join(format!(".selector-index.building-{}", std::process::id()));
        fs::create_dir(&path).with_context(|| {
            format!(
                "create selector-index staging directory {}; remove an abandoned directory first if needed",
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

fn flush_run(run_directory: &Path, name: &str, records: &mut Vec<SortRecord>) -> Result<PathBuf> {
    ensure!(
        !records.is_empty(),
        "cannot write an empty selector sort run"
    );
    records.sort_unstable();
    for pair in records.windows(2) {
        ensure!(
            pair[0] < pair[1],
            "selector sort run contains duplicate record {:?}",
            pair[0]
        );
    }
    let path = run_directory.join(format!("{name}.bin"));
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("create selector sort run {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
    for record in records.iter().copied() {
        writer
            .write_all(&record.encode())
            .with_context(|| format!("write selector sort run {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush selector sort run {}", path.display()))?;
    writer
        .get_ref()
        .sync_all()
        .with_context(|| format!("sync selector sort run {}", path.display()))?;
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
            .with_context(|| format!("open selector sort run {}", path.display()))?;
        let bytes = file.metadata()?.len();
        ensure!(
            bytes.is_multiple_of(SORT_RECORD_LEN as u64),
            "selector sort run {} is not record-aligned",
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
            .with_context(|| format!("read selector sort run {}", self.path.display()))?;
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
            "selector merge fan-in exceeds {MERGE_FAN_IN}"
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
                "merged selector records are not strictly sorted: {previous:?} then {record:?}"
            );
        }
        self.previous = Some(record);
        if let Some(next) = self.readers[run].next()? {
            self.heap.push(Reverse((next, run)));
        }
        Ok(Some(record))
    }
}

fn merge_run_group(input: Vec<PathBuf>, output: &Path) -> Result<()> {
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(output)
        .with_context(|| format!("create merged selector run {}", output.display()))?;
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
    let mut merger = RunMerger::open(input)?;
    while let Some(record) = merger.next()? {
        writer
            .write_all(&record.encode())
            .with_context(|| format!("write merged selector run {}", output.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush merged selector run {}", output.display()))?;
    writer
        .get_ref()
        .sync_all()
        .with_context(|| format!("sync merged selector run {}", output.display()))
}

fn reduce_runs(mut paths: Vec<PathBuf>, run_directory: &Path) -> Result<(Vec<PathBuf>, u32)> {
    let mut pass = 0_u32;
    while paths.len() > MERGE_FAN_IN {
        pass = pass
            .checked_add(1)
            .context("selector merge pass overflow")?;
        let mut next = Vec::new();
        for (group, chunk) in paths.chunks(MERGE_FAN_IN).enumerate() {
            let output = run_directory.join(format!("merge-{pass:04}-{group:08}.bin"));
            merge_run_group(chunk.to_vec(), &output)?;
            next.push(output);
        }
        for path in paths {
            fs::remove_file(&path)
                .with_context(|| format!("remove consumed selector run {}", path.display()))?;
        }
        paths = next;
    }
    Ok((paths, pass))
}

fn compress_page(payload: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = zstd::Encoder::new(Vec::new(), 3).context("create selector page encoder")?;
    encoder
        .include_checksum(true)
        .context("enable selector page checksum")?;
    encoder
        .write_all(payload)
        .context("compress selector page")?;
    encoder.finish().context("finish selector page")
}

struct PageWriter {
    writer: HeaderedWriter,
    directory: Vec<selectors::PageDirectoryEntry>,
    normal_keys: Vec<selectors::KeyPostings>,
    normal_decoded_bytes: usize,
    page_target_decoded_bytes: usize,
    pages: u64,
    continuation_pages: u64,
}

impl PageWriter {
    fn new(staging_root: &Path, page_target_decoded_bytes: usize) -> Result<Self> {
        Ok(Self {
            writer: HeaderedWriter::create(staging_root, selectors::PATH, IO_BUFFER_BYTES)?,
            directory: Vec::new(),
            normal_keys: Vec::new(),
            normal_decoded_bytes: 0,
            page_target_decoded_bytes,
            pages: 0,
            continuation_pages: 0,
        })
    }

    fn add_complete_key(&mut self, key: selectors::KeyPostings) -> Result<()> {
        let encoded = selectors::encode_page(std::slice::from_ref(&key))?;
        ensure!(
            encoded.len() <= selectors::MAX_PAGE_DECODED_BYTES as usize,
            "one selector key needs {} decoded page bytes, above the schema guard",
            encoded.len()
        );
        if !self.normal_keys.is_empty()
            && self
                .normal_decoded_bytes
                .checked_add(encoded.len())
                .is_none_or(|size| size > self.page_target_decoded_bytes)
        {
            self.flush_normal()?;
        }
        self.normal_decoded_bytes = self
            .normal_decoded_bytes
            .checked_add(encoded.len())
            .context("selector page length overflow")?;
        self.normal_keys.push(key);
        Ok(())
    }

    fn add_continuation(
        &mut self,
        key: selectors::SelectorKey,
        postings: Vec<selectors::Posting>,
        continued_from_previous: bool,
        continues_in_next: bool,
    ) -> Result<()> {
        self.flush_normal()?;
        let mut flags = 0_u16;
        if continued_from_previous {
            flags |= selectors::PAGE_FLAG_CONTINUED_FROM_PREVIOUS;
        }
        if continues_in_next {
            flags |= selectors::PAGE_FLAG_CONTINUES_IN_NEXT;
        }
        self.write_page(&[selectors::KeyPostings { key, postings }], flags)?;
        self.continuation_pages = self
            .continuation_pages
            .checked_add(1)
            .context("selector continuation-page count overflow")?;
        Ok(())
    }

    fn flush_normal(&mut self) -> Result<()> {
        if self.normal_keys.is_empty() {
            return Ok(());
        }
        let keys = mem::take(&mut self.normal_keys);
        self.normal_decoded_bytes = 0;
        self.write_page(&keys, 0)
    }

    fn write_page(
        &mut self,
        keys: &[selectors::KeyPostings],
        continuation_flags: u16,
    ) -> Result<()> {
        let decoded = selectors::encode_page(keys)?;
        ensure!(
            decoded.len() <= selectors::MAX_PAGE_DECODED_BYTES as usize,
            "selector page has {} decoded bytes, above the schema guard",
            decoded.len()
        );
        let compressed = compress_page(&decoded)?;
        let (stored, compression_flag): (&[u8], u16) = if compressed.len() < decoded.len() {
            (&compressed, selectors::PAGE_FLAG_ZSTD)
        } else {
            (&decoded, 0)
        };
        let offset = self.writer.append(stored, decoded.len() as u64)?;
        let posting_count = keys.iter().try_fold(0_u32, |sum, key| {
            sum.checked_add(u32::try_from(key.postings.len()).ok()?)
        });
        let posting_count = posting_count.context("selector page posting count exceeds u32")?;
        self.directory.push(selectors::PageDirectoryEntry {
            first_key: keys[0].key,
            last_key: keys[keys.len() - 1].key,
            offset,
            stored_len: u32::try_from(stored.len()).context("selector page exceeds u32")?,
            decoded_len: u32::try_from(decoded.len()).context("selector page exceeds u32")?,
            key_count: u32::try_from(keys.len()).context("selector key count exceeds u32")?,
            posting_count,
            flags: continuation_flags | compression_flag,
        });
        self.pages = self
            .pages
            .checked_add(1)
            .context("selector page count overflow")?;
        Ok(())
    }

    fn finish(mut self, archive_id: ArchiveId, posting_count: u64) -> Result<(u64, u64, u64)> {
        self.flush_normal()?;
        let mut directory_bytes = Vec::with_capacity(
            self.directory
                .len()
                .checked_mul(selectors::DIRECTORY_ENTRY_LEN)
                .context("selector directory length overflow")?,
        );
        for entry in &self.directory {
            directory_bytes.extend_from_slice(&entry.encode());
        }
        let directory_offset = self
            .writer
            .append(&directory_bytes, directory_bytes.len() as u64)?;
        selectors::validate_directory(&self.directory, FILE_HEADER_LEN as u64, directory_offset)?;
        let footer = selectors::DirectoryFooter {
            directory_offset,
            page_count: self.directory.len() as u64,
        }
        .encode();
        self.writer.append(&footer, footer.len() as u64)?;
        let finished = self.writer.finish(archive_id, posting_count)?;
        Ok((finished.file_bytes, self.pages, self.continuation_pages))
    }
}

fn write_index_from_runs(
    staging_root: &Path,
    archive_id: ArchiveId,
    run_paths: Vec<PathBuf>,
    expected_records: u64,
    options: SelectorIndexBuildOptions,
) -> Result<(u64, u64, u64)> {
    let mut pages = PageWriter::new(staging_root, options.page_target_decoded_bytes)?;
    if run_paths.is_empty() {
        ensure!(
            expected_records == 0,
            "selector run list is unexpectedly empty"
        );
        return pages.finish(archive_id, 0);
    }
    let mut merger = RunMerger::open(run_paths)?;
    let mut next = merger.next()?;
    let mut merged = 0_u64;

    while let Some(first) = next.take() {
        let key = first.key;
        let mut first_in_fragment = Some(first);
        let mut fragment_index = 0_usize;
        loop {
            let mut postings = Vec::with_capacity(options.max_postings_per_page);
            if let Some(record) = first_in_fragment.take() {
                postings.push(record.posting);
                merged = merged
                    .checked_add(1)
                    .context("selector posting count overflow")?;
            }
            while postings.len() < options.max_postings_per_page {
                next = merger.next()?;
                let Some(record) = next else {
                    break;
                };
                if record.key != key {
                    break;
                }
                postings.push(record.posting);
                merged = merged
                    .checked_add(1)
                    .context("selector posting count overflow")?;
                next = None;
            }
            if postings.len() == options.max_postings_per_page && next.is_none() {
                next = merger.next()?;
            }
            let continues = next.is_some_and(|record| record.key == key);
            if fragment_index > 0 || continues {
                pages.add_continuation(key, postings, fragment_index > 0, continues)?;
            } else {
                pages.add_complete_key(selectors::KeyPostings { key, postings })?;
            }
            if !continues {
                break;
            }
            first_in_fragment = next.take();
            fragment_index = fragment_index
                .checked_add(1)
                .context("selector continuation count overflow")?;
        }
    }
    ensure!(
        merged == expected_records,
        "merged {merged} selector postings, expected {expected_records}"
    );
    pages.finish(archive_id, merged)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

/// Rebuild the selector index from inline instruction data in the merged
/// transaction and CPI streams. Sorting and page assembly have caller-selected
/// memory bounds.
pub fn build_selector_index(
    root: &Path,
    options: SelectorIndexBuildOptions,
) -> Result<SelectorIndexBuildReport> {
    ensure!(
        root.is_dir(),
        "{} is not an archive directory",
        root.display()
    );
    ensure!(
        options.sort_memory_bytes >= mem::size_of::<SortRecord>(),
        "selector sort memory must hold at least one in-memory record"
    );
    ensure!(
        options.page_target_decoded_bytes > 0
            && options.page_target_decoded_bytes <= selectors::MAX_PAGE_DECODED_BYTES as usize,
        "selector page target must be in 1..={} bytes",
        selectors::MAX_PAGE_DECODED_BYTES
    );
    ensure!(
        options.max_postings_per_page > 0,
        "selector posting page bound must be greater than zero"
    );
    let worst_page = selectors::KEY_LEN
        .checked_add(10)
        .and_then(|size| {
            options
                .max_postings_per_page
                .checked_mul(20)
                .and_then(|postings| size.checked_add(postings))
        })
        .context("selector posting page bound overflows")?;
    ensure!(
        worst_page <= selectors::MAX_PAGE_DECODED_BYTES as usize,
        "selector posting page bound can exceed the schema decode guard"
    );
    ensure!(
        options.max_source_page_decoded_bytes > 0,
        "canonical source-page guard must be greater than zero"
    );

    let staging = StagingDirectory::create(root)?;
    let run_directory = staging.path.join("selector-runs");
    fs::create_dir(&run_directory)
        .with_context(|| format!("create selector run directory {}", run_directory.display()))?;
    let capacity = options.sort_memory_bytes / mem::size_of::<SortRecord>();
    let mut records = Vec::with_capacity(capacity);
    let mut run_paths = Vec::new();
    let mut input_runs = 0_u64;
    let scan = scan_canonical(root, options.max_source_page_decoded_bytes, |instruction| {
        records.push(SortRecord {
            key: instruction.key,
            posting: instruction.posting,
        });
        if records.len() == capacity {
            let name = format!("run-{input_runs:08}");
            run_paths.push(flush_run(&run_directory, &name, &mut records)?);
            input_runs = input_runs
                .checked_add(1)
                .context("selector sort-run count overflow")?;
        }
        Ok(())
    })?;
    if !records.is_empty() {
        let name = format!("run-{input_runs:08}");
        run_paths.push(flush_run(&run_directory, &name, &mut records)?);
        input_runs = input_runs
            .checked_add(1)
            .context("selector sort-run count overflow")?;
    }
    let expected_postings = scan
        .top_level_instructions
        .checked_add(scan.cpi_instructions)
        .context("selector posting count overflow")?;
    let (run_paths, merge_passes) = reduce_runs(run_paths, &run_directory)?;
    let (object_bytes, pages, continuation_pages) = write_index_from_runs(
        &staging.path,
        scan.archive_id,
        run_paths,
        expected_postings,
        options,
    )?;

    let staged = staging.path.join(selectors::PATH);
    let target = root.join(selectors::PATH);
    let target_parent = target.parent().context("selector index has no parent")?;
    fs::create_dir_all(target_parent)
        .with_context(|| format!("create {}", target_parent.display()))?;
    fs::rename(&staged, &target).with_context(|| {
        format!(
            "replace selector index {} with staged object {}",
            target.display(),
            staged.display()
        )
    })?;
    sync_directory(target_parent)?;
    sync_directory(root)?;

    Ok(SelectorIndexBuildReport {
        archive_id: scan.archive_id,
        blocks: scan.blocks,
        transactions: scan.transactions,
        top_level_instructions: scan.top_level_instructions,
        cpi_instructions: scan.cpi_instructions,
        postings: expected_postings,
        sort_runs: input_runs,
        merge_passes,
        pages,
        continuation_pages,
        object_bytes,
    })
}

/// One validated selector index. It keeps only the raw directory in memory.
#[derive(Debug)]
pub struct SelectorIndexReader {
    file: File,
    header: FileHeader,
    directory: Vec<selectors::PageDirectoryEntry>,
}

impl SelectorIndexReader {
    pub fn open(root: &Path) -> Result<Self> {
        let archive_id =
            crate::canonical_reader::CanonicalReader::open(root, DEFAULT_MAX_SOURCE_PAGE_BYTES)?
                .archive_id();
        let path = root.join(selectors::PATH);
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let header = validate_open_file(&file, selectors::PATH, archive_id)?;
        ensure!(
            header.payload_bytes >= selectors::DIRECTORY_FOOTER_LEN as u64,
            "selector index is too short for its directory footer"
        );
        let footer_offset =
            FILE_HEADER_LEN as u64 + header.payload_bytes - selectors::DIRECTORY_FOOTER_LEN as u64;
        let mut footer_bytes = [0_u8; selectors::DIRECTORY_FOOTER_LEN];
        file.read_exact_at(&mut footer_bytes, footer_offset)
            .context("read selector directory footer")?;
        let footer = selectors::DirectoryFooter::decode(&footer_bytes)?;
        let directory_len = footer
            .page_count
            .checked_mul(selectors::DIRECTORY_ENTRY_LEN as u64)
            .context("selector directory length overflow")?;
        ensure!(
            footer.directory_offset >= FILE_HEADER_LEN as u64,
            "selector directory overlaps the common header"
        );
        ensure!(
            footer
                .directory_offset
                .checked_add(directory_len)
                .is_some_and(|end| end == footer_offset),
            "selector directory does not end at its footer"
        );
        let mut directory_bytes = vec![
            0_u8;
            usize::try_from(directory_len)
                .context("selector directory does not fit memory")?
        ];
        file.read_exact_at(&mut directory_bytes, footer.directory_offset)
            .context("read selector directory")?;
        let directory = directory_bytes
            .chunks_exact(selectors::DIRECTORY_ENTRY_LEN)
            .map(selectors::PageDirectoryEntry::decode)
            .collect::<Result<Vec<_>, _>>()?;
        selectors::validate_directory(&directory, FILE_HEADER_LEN as u64, footer.directory_offset)?;
        let posting_count = directory.iter().try_fold(0_u64, |sum, entry| {
            sum.checked_add(u64::from(entry.posting_count))
        });
        ensure!(
            posting_count == Some(header.record_count),
            "selector directory posting count does not match its common header"
        );
        let decoded_bytes = directory.iter().try_fold(directory_len, |sum, entry| {
            sum.checked_add(u64::from(entry.decoded_len))
        });
        let decoded_bytes =
            decoded_bytes.and_then(|sum| sum.checked_add(selectors::DIRECTORY_FOOTER_LEN as u64));
        ensure!(
            decoded_bytes == Some(header.decoded_bytes),
            "selector directory decoded-byte count does not match its common header"
        );
        Ok(Self {
            file,
            header,
            directory,
        })
    }

    pub const fn archive_id(&self) -> ArchiveId {
        self.header.archive_id
    }

    pub const fn posting_count(&self) -> u64 {
        self.header.record_count
    }

    pub fn lookup(&self, key: selectors::SelectorKey) -> Result<Vec<selectors::Posting>> {
        let range = selectors::candidate_page_range(&self.directory, key);
        let mut pages = Vec::with_capacity(range.len());
        for entry in &self.directory[range] {
            let mut stored = vec![0_u8; entry.stored_len as usize];
            self.file
                .read_exact_at(&mut stored, entry.offset)
                .context("read selector posting page")?;
            let decoded = if entry.is_zstd() {
                decode_zstd_exact(&stored, entry.decoded_len as usize, "selector posting page")?
            } else {
                ensure!(
                    entry.stored_len == entry.decoded_len,
                    "raw selector page stored and decoded lengths differ"
                );
                stored
            };
            let page = selectors::decode_page(&decoded, entry.key_count, entry.posting_count)?;
            ensure!(
                page.first().is_some_and(|item| item.key == entry.first_key)
                    && page.last().is_some_and(|item| item.key == entry.last_key),
                "selector page keys do not match its directory entry"
            );
            pages.push(page);
        }
        selectors::point_lookup(key, pages).map_err(Into::into)
    }
}

/// Verify one point-lookup result against the exact canonical data owners.
/// This scans the canonical columns once and keeps only one boolean per result.
pub fn verify_selector_lookup(
    root: &Path,
    key: selectors::SelectorKey,
    postings: &[selectors::Posting],
    max_source_page_decoded_bytes: usize,
) -> Result<()> {
    for pair in postings.windows(2) {
        ensure!(
            pair[0] < pair[1],
            "selector lookup postings do not strictly ascend"
        );
    }
    let mut wanted = BTreeMap::new();
    for posting in postings.iter().copied() {
        ensure!(
            wanted
                .insert(
                    (posting.scope, posting.role_local_instruction_ordinal),
                    (posting, false),
                )
                .is_none(),
            "selector lookup repeats one role-local instruction locator"
        );
    }
    if wanted.is_empty() {
        return Ok(());
    }
    scan_canonical(root, max_source_page_decoded_bytes, |instruction| {
        let locator = (
            instruction.posting.scope,
            instruction.posting.role_local_instruction_ordinal,
        );
        if let Some((posting, found)) = wanted.get_mut(&locator) {
            ensure!(
                instruction.posting.transaction_ordinal == posting.transaction_ordinal,
                "selector posting transaction does not match its canonical instruction"
            );
            ensure!(
                instruction.key == key,
                "selector posting program or canonical data prefix does not match its key"
            );
            // Use the borrowed bytes so verification cannot accidentally check
            // only an instruction row while ignoring its canonical data owner.
            ensure!(
                selectors::SelectorKey::from_instruction(
                    instruction.key.program_id,
                    instruction.data
                ) == key,
                "selector posting canonical bytes do not reproduce its key"
            );
            *found = true;
        }
        Ok(())
    })?;
    if let Some((locator, _)) = wanted.iter().find(|(_, (_, found))| !*found) {
        bail!(
            "selector posting points outside canonical {:?} instruction ordinal {}",
            locator.0,
            locator.1
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use blockzilla_index_archive_format::{
        ObjectRole,
        ledger::transactions::{
            HashOwner, HashRef, Instruction, Message, MessageHeader, PubkeyId, Transaction,
        },
        runtime::inner_instructions::{InnerGroup, InnerInstruction, TransactionInner},
    };

    use crate::test_fixture::{FixtureBlock, write_merged_fixture};

    use super::*;

    fn write_fixture(root: &Path) -> Vec<(u32, Vec<u8>)> {
        let archive_id = ArchiveId::new([7; 16]);
        let payloads = vec![
            (1, Vec::new()),
            (2, vec![1]),
            (3, vec![2, 3]),
            (4, vec![4, 5, 6]),
            (5, vec![7, 8, 9, 10]),
            (6, vec![11, 12, 13, 14, 15]),
            (7, vec![16, 17, 18, 19, 20, 21]),
            (8, vec![22, 23, 24, 25, 26, 27, 28]),
            (9, b"12345678".to_vec()),
            (10, b"abcdefgh-more".to_vec()),
        ];
        let mut all_payloads = payloads.clone();
        for _ in 0..5 {
            all_payloads.push((11, b"hot-key!tail".to_vec()));
        }
        let top: Vec<Instruction> = all_payloads
            .iter()
            .enumerate()
            .map(|(index, (_, data))| Instruction {
                program_position: index.min(10) as u32,
                account_positions: vec![0],
                data: data.clone(),
            })
            .collect();
        let transaction = Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 10,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::Legacy {
                static_accounts: (1..=11).map(|id| PubkeyId::new(id).unwrap()).collect(),
                instructions: top,
            },
        };
        let cpi = TransactionInner {
            groups: vec![InnerGroup {
                parent_index: 0,
                instructions: payloads
                    .iter()
                    .enumerate()
                    .map(|(index, (_, data))| InnerInstruction {
                        stack_height: Some(2),
                        instruction: Instruction {
                            program_position: index as u32,
                            account_positions: vec![0],
                            data: data.clone(),
                        },
                    })
                    .collect(),
            }],
        };
        write_merged_fixture(
            root,
            archive_id,
            archive_id,
            archive_id,
            11,
            vec![FixtureBlock {
                slot: 1,
                parent_slot: 0,
                transactions: vec![transaction],
                inner: vec![Some(cpi)],
            }],
        );
        payloads
    }

    #[test]
    fn builder_is_deterministic_and_indexes_both_canonical_data_owners() {
        let root = tempdir().unwrap();
        let payloads = write_fixture(root.path());
        let options = SelectorIndexBuildOptions {
            sort_memory_bytes: mem::size_of::<SortRecord>() * 3,
            page_target_decoded_bytes: 128,
            max_postings_per_page: 2,
            max_source_page_decoded_bytes: 1 << 20,
        };
        let first = build_selector_index(root.path(), options).unwrap();
        assert_eq!(first.top_level_instructions, 15);
        assert_eq!(first.cpi_instructions, 10);
        assert_eq!(first.postings, 25);
        assert!(first.sort_runs > 1);
        assert!(first.continuation_pages >= 3);
        let bytes = fs::read(root.path().join(selectors::PATH)).unwrap();

        let reader = SelectorIndexReader::open(root.path()).unwrap();
        assert_eq!(reader.archive_id(), ArchiveId::new([7; 16]));
        assert_eq!(reader.posting_count(), 25);
        for (program_id, data) in payloads {
            let key = selector_key(program_id, &data).unwrap();
            let postings = reader.lookup(key).unwrap();
            assert_eq!(postings.len(), 2);
            assert_eq!(postings[0].scope, selectors::InstructionScope::TopLevel);
            assert_eq!(postings[1].scope, selectors::InstructionScope::Cpi);
            verify_selector_lookup(root.path(), key, &postings, 1 << 20).unwrap();
        }
        let hot_key = selector_key(11, b"hot-key!tail").unwrap();
        let hot = reader.lookup(hot_key).unwrap();
        assert_eq!(hot.len(), 5);
        verify_selector_lookup(root.path(), hot_key, &hot, 1 << 20).unwrap();
        drop(reader);

        let second = build_selector_index(root.path(), options).unwrap();
        assert_eq!(first, second);
        assert_eq!(fs::read(root.path().join(selectors::PATH)).unwrap(), bytes);
    }

    #[test]
    fn reader_rejects_directory_corruption() {
        let root = tempdir().unwrap();
        write_fixture(root.path());
        build_selector_index(root.path(), SelectorIndexBuildOptions::default()).unwrap();
        let path = root.path().join(selectors::PATH);
        let file = File::options().read(true).write(true).open(&path).unwrap();
        let header = validate_open_file(&file, selectors::PATH, ArchiveId::new([7; 16])).unwrap();
        let footer_offset =
            FILE_HEADER_LEN as u64 + header.payload_bytes - selectors::DIRECTORY_FOOTER_LEN as u64;
        let mut footer = [0_u8; selectors::DIRECTORY_FOOTER_LEN];
        file.read_exact_at(&mut footer, footer_offset).unwrap();
        let footer = selectors::DirectoryFooter::decode(&footer).unwrap();
        file.write_all_at(&[1], footer.directory_offset + 63)
            .unwrap();
        file.sync_all().unwrap();
        let error = SelectorIndexReader::open(root.path()).unwrap_err();
        assert!(format!("{error:#}").contains("reserved bytes"), "{error:#}");
    }

    #[test]
    fn output_common_header_has_frozen_selector_role_and_schema() {
        let root = tempdir().unwrap();
        write_fixture(root.path());
        build_selector_index(root.path(), SelectorIndexBuildOptions::default()).unwrap();
        let file = File::open(root.path().join(selectors::PATH)).unwrap();
        let header = validate_open_file(&file, selectors::PATH, ArchiveId::new([7; 16])).unwrap();
        assert_eq!(header.role, ObjectRole::IndexSelectors);
        assert_eq!(header.schema, selectors::SCHEMA);
        assert_eq!(header.record_count, 25);
    }
}
