//! Exact source-registry account -> standalone transaction postings canary.
//!
//! The payload uses the final account-index schema-2 page codec unchanged.
//! Transaction ordinals are resolved through the standalone block index, so
//! one posting identifies an exact `(block_id, tx_index)` without storing that
//! pair twice. This remains an unverified, non-publishable canary.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    mem,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_index_archive_format::indexes::accounts as postings;
use blockzilla_read_sdk::{
    CompactV2MessageSchema, CompactV2MetadataSchema, PinnedLocalRangeSource, RangeSource,
};
use serde::Serialize;

use super::standalone_v2;

/// Experimental codecs used only for size measurement. The active builder
/// continues to write the schema-2 ordinal pages above.
#[allow(dead_code)]
#[path = "standalone_account_postings/block_group_measurement.rs"]
pub mod block_group_measurement;

/// Measurement-only block-group headers with exact role summaries.
#[allow(dead_code)]
#[path = "standalone_account_postings/block_group_role_summary_canary.rs"]
pub mod block_group_role_summary_canary;

pub const PAGES_FILE: &str = "archive-v2-standalone-account-postings.pages";
pub const CONTROL_FILE: &str = "archive-v2-standalone-account-postings.control";
pub const COVERAGE_FILE: &str = "archive-v2-standalone-account-postings.coverage";
pub const REPORT_FILE: &str = "archive-v2-standalone-account-postings.report.json";
pub const ADAPTIVE_V3_PAGES_FILE: &str = "archive-v2-standalone-account-postings-adaptive-v3.pages";
pub const ADAPTIVE_V3_CONTROL_FILE: &str =
    "archive-v2-standalone-account-postings-adaptive-v3.control";
pub const ADAPTIVE_V3_COVERAGE_FILE: &str =
    "archive-v2-standalone-account-postings-adaptive-v3.coverage";
pub const ADAPTIVE_V3_REPORT_FILE: &str =
    "archive-v2-standalone-account-postings-adaptive-v3.report.json";
pub const STATUS: &str = "unverified-nonpublishable";
pub const CANARY_KIND: &str = "standalone-exact-account-postings-v1";
pub const ACCOUNT_SEMANTICS: &str =
    "source-registry-account-to-exact-standalone-transaction-with-role-bits-v1";
pub const DEFAULT_SORT_MEMORY_BYTES: usize = 128 << 20;
pub const ADAPTIVE_V3_OPTIMIZED_SORT_MEMORY_BYTES: usize = 2 << 30;
pub const MAX_ADAPTIVE_V3_LOOKUP_SAMPLE_POSTINGS: usize = 10_000;

const MAGIC: [u8; 8] = *b"BZV2AP02";
const CONTROL_MAGIC: [u8; 8] = *b"BZV2AC02";
const COVERAGE_MAGIC: [u8; 8] = *b"BZV2AV02";
const COVERAGE_FOOTER_MAGIC: [u8; 8] = *b"BZV2AVFT";
const FORMAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 80;
const CONTROL_LEN: usize = HEADER_LEN + 40;
const COVERAGE_RECORD_LEN: usize = 16;
const COVERAGE_FOOTER_LEN: usize = 16;
const ZSTD_LEVEL: i32 = 3;
const SORT_RECORD_LEN: usize = 16;
const IO_BUFFER_BYTES: usize = 1 << 20;
const MERGE_FAN_IN: usize = 128;
const MAX_DIRECTORY_BYTES: u64 = 512 << 20;
const MAX_COVERAGE_BYTES: u64 = 512 << 20;

const ADAPTIVE_V3_MAGIC: [u8; 8] = *b"BZV3AP03";
const ADAPTIVE_V3_CONTROL_MAGIC: [u8; 8] = *b"BZV3AC03";
const ADAPTIVE_V3_COVERAGE_MAGIC: [u8; 8] = *b"BZV3AV03";
const ADAPTIVE_V3_COVERAGE_FOOTER_MAGIC: [u8; 8] = *b"BZV3AVF3";
const ADAPTIVE_V3_PAYLOAD_SCHEMA: u16 = 3;
const ADAPTIVE_V3_FORMAT_VERSION: u16 = 1;
const ADAPTIVE_V3_MERGE_FAN_IN: usize = 63;
const ADAPTIVE_V3_MAX_MERGE_WORKERS: usize = 2;
const ADAPTIVE_V3_MAX_PAGE_WORKERS: usize = 64;
const ADAPTIVE_V3_MAX_OPEN_FILES: usize = 131;
const ADAPTIVE_V3_MAX_BLOCK_SPAN_HEAP_BYTES: usize = 64 << 20;
const ADAPTIVE_V3_ZSTD_WINDOW_LOG_MAX: u32 = 29;
const ADAPTIVE_V3_MAX_SORT_MEMORY_BYTES: usize = ADAPTIVE_V3_OPTIMIZED_SORT_MEMORY_BYTES;
const ADAPTIVE_V3_COMPRESSION_LIVE_BUDGET: usize = block_group_measurement::LIVE_BYTE_BUDGET;

#[derive(Debug, Clone, Copy)]
pub struct Binding {
    pub standalone: standalone_v2::Binding,
    pub registry_entries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SortRecord {
    account_id: u32,
    transaction_ordinal: u64,
    roles: u8,
}

impl SortRecord {
    pub fn new(account_id: u32, transaction_ordinal: u64, roles: u8) -> Result<Self> {
        ensure!(
            account_id != 0,
            "account posting uses reserved source ID zero"
        );
        ensure!(
            roles & !postings::ROLE_MASK == 0,
            "account posting has unknown role bits {roles:#x}"
        );
        Ok(Self {
            account_id,
            transaction_ordinal,
            roles,
        })
    }

    fn encode(self) -> [u8; SORT_RECORD_LEN] {
        let mut output = [0_u8; SORT_RECORD_LEN];
        output[0..4].copy_from_slice(&self.account_id.to_le_bytes());
        output[4..12].copy_from_slice(&self.transaction_ordinal.to_le_bytes());
        output[12] = self.roles;
        output
    }

    fn decode(input: &[u8; SORT_RECORD_LEN]) -> Result<Self> {
        ensure!(
            input[13..16] == [0; 3],
            "account posting sort record has nonzero reserved bytes"
        );
        Self::new(
            u32::from_le_bytes(input[0..4].try_into().expect("four bytes")),
            u64::from_le_bytes(input[4..12].try_into().expect("eight bytes")),
            input[12],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Header {
    binding: BindingHeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BindingHeader {
    epoch: u64,
    slots_per_epoch: u64,
    selected_blocks: u64,
    selected_transactions: u64,
    registry_entries: u32,
    message_schema: u8,
    metadata_schema: u8,
    prefix: bool,
}

impl Header {
    fn unfinished(binding: Binding) -> Self {
        Self {
            binding: BindingHeader {
                epoch: binding.standalone.epoch,
                slots_per_epoch: binding.standalone.slots_per_epoch,
                selected_blocks: binding.standalone.selected_blocks,
                selected_transactions: binding.standalone.selected_transactions,
                registry_entries: binding.registry_entries,
                message_schema: message_schema_code(binding.standalone.message_schema),
                metadata_schema: metadata_schema_code(binding.standalone.metadata_schema),
                prefix: binding.standalone.prefix,
            },
        }
    }

    fn encode(self) -> [u8; HEADER_LEN] {
        self.encode_with_magic(MAGIC)
    }

    fn encode_with_magic(self, magic: [u8; 8]) -> [u8; HEADER_LEN] {
        let mut output = [0_u8; HEADER_LEN];
        output[0..8].copy_from_slice(&magic);
        output[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        output[10..12].copy_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        output[12..14].copy_from_slice(&postings::SCHEMA.to_le_bytes());
        output[14] = postings::ROLE_MASK;
        output[15] = ZSTD_LEVEL as u8;
        output[16] = self.binding.message_schema;
        output[17] = self.binding.metadata_schema;
        output[18] = u8::from(self.binding.prefix);
        output[19] = 1; // Exact source-ID domain; Raw keys are rejected.
        output[24..32].copy_from_slice(&self.binding.epoch.to_le_bytes());
        output[32..40].copy_from_slice(&self.binding.slots_per_epoch.to_le_bytes());
        output[40..48].copy_from_slice(&self.binding.selected_blocks.to_le_bytes());
        output[48..56].copy_from_slice(&self.binding.selected_transactions.to_le_bytes());
        output[56..60].copy_from_slice(&self.binding.registry_entries.to_le_bytes());
        output
    }

    fn decode(input: &[u8]) -> Result<Self> {
        Self::decode_with_magic(input, MAGIC)
    }

    fn decode_with_magic(input: &[u8], magic: [u8; 8]) -> Result<Self> {
        ensure!(
            input.len() == HEADER_LEN,
            "account posting header has wrong length"
        );
        ensure!(
            input[0..8] == magic,
            "account posting header has wrong magic"
        );
        ensure!(
            u16::from_le_bytes(input[8..10].try_into().unwrap()) == FORMAT_VERSION,
            "account posting format version is not supported"
        );
        ensure!(
            usize::from(u16::from_le_bytes(input[10..12].try_into().unwrap())) == HEADER_LEN,
            "account posting header length is not supported"
        );
        ensure!(
            u16::from_le_bytes(input[12..14].try_into().unwrap()) == postings::SCHEMA,
            "account posting payload schema is not supported"
        );
        ensure!(
            input[14] == postings::ROLE_MASK,
            "account posting role mask differs"
        );
        ensure!(
            input[15] == ZSTD_LEVEL as u8,
            "account posting zstd level differs"
        );
        ensure!(
            matches!(input[16], 0 | 1),
            "unknown account posting message schema"
        );
        ensure!(
            matches!(input[17], 0 | 1),
            "unknown account posting metadata schema"
        );
        ensure!(input[18] <= 1, "account posting prefix flag is not Boolean");
        ensure!(
            input[19] == 1,
            "account posting key domain is not exact source IDs"
        );
        ensure!(
            input[20..24] == [0; 4],
            "account posting header reserved bytes are nonzero"
        );
        ensure!(
            input[60..64] == [0; 4],
            "account posting header reserved bytes are nonzero"
        );
        ensure!(
            input[64..80] == [0; 16],
            "account posting header reserved bytes are nonzero"
        );
        let header = Self {
            binding: BindingHeader {
                epoch: u64::from_le_bytes(input[24..32].try_into().unwrap()),
                slots_per_epoch: u64::from_le_bytes(input[32..40].try_into().unwrap()),
                selected_blocks: u64::from_le_bytes(input[40..48].try_into().unwrap()),
                selected_transactions: u64::from_le_bytes(input[48..56].try_into().unwrap()),
                registry_entries: u32::from_le_bytes(input[56..60].try_into().unwrap()),
                message_schema: input[16],
                metadata_schema: input[17],
                prefix: input[18] != 0,
            },
        };
        ensure!(
            header.binding.slots_per_epoch != 0,
            "account posting slots per epoch is zero"
        );
        Ok(header)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Control {
    header: Header,
    postings: u64,
    distinct_accounts: u64,
    coverage_records: u64,
    incomplete_account_transactions: u64,
    incomplete_cpi_transactions: u64,
}

impl Control {
    fn encode(self) -> [u8; CONTROL_LEN] {
        let mut output = [0_u8; CONTROL_LEN];
        output[..HEADER_LEN].copy_from_slice(&self.header.encode_with_magic(CONTROL_MAGIC));
        output[80..88].copy_from_slice(&self.postings.to_le_bytes());
        output[88..96].copy_from_slice(&self.distinct_accounts.to_le_bytes());
        output[96..104].copy_from_slice(&self.coverage_records.to_le_bytes());
        output[104..112].copy_from_slice(&self.incomplete_account_transactions.to_le_bytes());
        output[112..120].copy_from_slice(&self.incomplete_cpi_transactions.to_le_bytes());
        output
    }

    fn decode(input: &[u8]) -> Result<Self> {
        ensure!(
            input.len() == CONTROL_LEN,
            "account posting control has wrong length"
        );
        let control = Self {
            header: Header::decode_with_magic(&input[..HEADER_LEN], CONTROL_MAGIC)?,
            postings: u64::from_le_bytes(input[80..88].try_into().unwrap()),
            distinct_accounts: u64::from_le_bytes(input[88..96].try_into().unwrap()),
            coverage_records: u64::from_le_bytes(input[96..104].try_into().unwrap()),
            incomplete_account_transactions: u64::from_le_bytes(
                input[104..112].try_into().unwrap(),
            ),
            incomplete_cpi_transactions: u64::from_le_bytes(input[112..120].try_into().unwrap()),
        };
        ensure!(
            control.distinct_accounts <= control.postings,
            "distinct account count exceeds posting count"
        );
        ensure!(
            control.incomplete_account_transactions <= control.coverage_records
                && control.incomplete_cpi_transactions <= control.coverage_records,
            "coverage class count exceeds sparse record count"
        );
        Ok(control)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AdaptiveV3Header {
    binding: BindingHeader,
}

impl AdaptiveV3Header {
    fn unfinished(binding: Binding) -> Self {
        Self {
            binding: Header::unfinished(binding).binding,
        }
    }

    fn encode(self) -> [u8; HEADER_LEN] {
        self.encode_with_magic(ADAPTIVE_V3_MAGIC)
    }

    fn encode_with_magic(self, magic: [u8; 8]) -> [u8; HEADER_LEN] {
        let mut output = [0_u8; HEADER_LEN];
        output[0..8].copy_from_slice(&magic);
        output[8..10].copy_from_slice(&ADAPTIVE_V3_FORMAT_VERSION.to_le_bytes());
        output[10..12].copy_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        output[12..14].copy_from_slice(&ADAPTIVE_V3_PAYLOAD_SCHEMA.to_le_bytes());
        output[14] = postings::ROLE_MASK;
        output[15] = ZSTD_LEVEL as u8;
        output[16] = self.binding.message_schema;
        output[17] = self.binding.metadata_schema;
        output[18] = u8::from(self.binding.prefix);
        output[19] = 1;
        output[24..32].copy_from_slice(&self.binding.epoch.to_le_bytes());
        output[32..40].copy_from_slice(&self.binding.slots_per_epoch.to_le_bytes());
        output[40..48].copy_from_slice(&self.binding.selected_blocks.to_le_bytes());
        output[48..56].copy_from_slice(&self.binding.selected_transactions.to_le_bytes());
        output[56..60].copy_from_slice(&self.binding.registry_entries.to_le_bytes());
        output
    }

    fn decode(input: &[u8]) -> Result<Self> {
        Self::decode_with_magic(input, ADAPTIVE_V3_MAGIC)
    }

    fn decode_with_magic(input: &[u8], magic: [u8; 8]) -> Result<Self> {
        ensure!(
            input.len() == HEADER_LEN,
            "adaptive v3 account posting header has wrong length"
        );
        ensure!(
            input[0..8] == magic,
            "adaptive v3 account posting header has wrong magic"
        );
        ensure!(
            u16::from_le_bytes(input[8..10].try_into().unwrap()) == ADAPTIVE_V3_FORMAT_VERSION,
            "adaptive v3 account posting format version is not supported"
        );
        ensure!(
            usize::from(u16::from_le_bytes(input[10..12].try_into().unwrap())) == HEADER_LEN,
            "adaptive v3 account posting header length is not supported"
        );
        ensure!(
            u16::from_le_bytes(input[12..14].try_into().unwrap()) == ADAPTIVE_V3_PAYLOAD_SCHEMA,
            "adaptive v3 account posting payload schema is not supported"
        );
        ensure!(
            input[14] == postings::ROLE_MASK,
            "adaptive v3 account posting role mask differs"
        );
        ensure!(
            input[15] == ZSTD_LEVEL as u8,
            "adaptive v3 account posting zstd level differs"
        );
        ensure!(
            matches!(input[16], 0 | 1),
            "unknown adaptive v3 account posting message schema"
        );
        ensure!(
            matches!(input[17], 0 | 1),
            "unknown adaptive v3 account posting metadata schema"
        );
        ensure!(
            input[18] <= 1,
            "adaptive v3 account posting prefix flag is not Boolean"
        );
        ensure!(
            input[19] == 1,
            "adaptive v3 account posting key domain is not exact source IDs"
        );
        ensure!(
            input[20..24] == [0; 4] && input[60..64] == [0; 4] && input[64..80] == [0; 16],
            "adaptive v3 account posting header reserved bytes are nonzero"
        );
        let header = Self {
            binding: BindingHeader {
                epoch: u64::from_le_bytes(input[24..32].try_into().unwrap()),
                slots_per_epoch: u64::from_le_bytes(input[32..40].try_into().unwrap()),
                selected_blocks: u64::from_le_bytes(input[40..48].try_into().unwrap()),
                selected_transactions: u64::from_le_bytes(input[48..56].try_into().unwrap()),
                registry_entries: u32::from_le_bytes(input[56..60].try_into().unwrap()),
                message_schema: input[16],
                metadata_schema: input[17],
                prefix: input[18] != 0,
            },
        };
        ensure!(
            header.binding.slots_per_epoch != 0,
            "adaptive v3 account posting slots per epoch is zero"
        );
        Ok(header)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AdaptiveV3Control {
    header: AdaptiveV3Header,
    postings: u64,
    distinct_accounts: u64,
    coverage_records: u64,
    incomplete_account_transactions: u64,
    incomplete_cpi_transactions: u64,
}

impl AdaptiveV3Control {
    fn encode(self) -> [u8; CONTROL_LEN] {
        let mut output = [0_u8; CONTROL_LEN];
        output[..HEADER_LEN]
            .copy_from_slice(&self.header.encode_with_magic(ADAPTIVE_V3_CONTROL_MAGIC));
        output[80..88].copy_from_slice(&self.postings.to_le_bytes());
        output[88..96].copy_from_slice(&self.distinct_accounts.to_le_bytes());
        output[96..104].copy_from_slice(&self.coverage_records.to_le_bytes());
        output[104..112].copy_from_slice(&self.incomplete_account_transactions.to_le_bytes());
        output[112..120].copy_from_slice(&self.incomplete_cpi_transactions.to_le_bytes());
        output
    }

    fn decode(input: &[u8]) -> Result<Self> {
        ensure!(
            input.len() == CONTROL_LEN,
            "adaptive v3 account posting control has wrong length"
        );
        let control = Self {
            header: AdaptiveV3Header::decode_with_magic(
                &input[..HEADER_LEN],
                ADAPTIVE_V3_CONTROL_MAGIC,
            )?,
            postings: u64::from_le_bytes(input[80..88].try_into().unwrap()),
            distinct_accounts: u64::from_le_bytes(input[88..96].try_into().unwrap()),
            coverage_records: u64::from_le_bytes(input[96..104].try_into().unwrap()),
            incomplete_account_transactions: u64::from_le_bytes(
                input[104..112].try_into().unwrap(),
            ),
            incomplete_cpi_transactions: u64::from_le_bytes(input[112..120].try_into().unwrap()),
        };
        ensure!(
            control.distinct_accounts <= control.postings,
            "adaptive v3 distinct account count exceeds posting count"
        );
        ensure!(
            control.incomplete_account_transactions <= control.coverage_records
                && control.incomplete_cpi_transactions <= control.coverage_records,
            "adaptive v3 coverage class count exceeds sparse record count"
        );
        Ok(control)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveV3Options {
    pub merge_workers: usize,
    pub page_workers: usize,
}

impl Default for AdaptiveV3Options {
    fn default() -> Self {
        Self {
            merge_workers: ADAPTIVE_V3_MAX_MERGE_WORKERS,
            page_workers: thread::available_parallelism()
                .map_or(1, usize::from)
                .min(12),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverageRecord {
    pub transaction_ordinal: u64,
    /// Zero means complete. Values 1..=3 are the provisional account-coverage states.
    pub account_coverage: u8,
    /// Zero means recorded. Values 1..=4 are the provisional CPI-coverage states.
    pub cpi_coverage: u8,
}

impl CoverageRecord {
    pub fn new(transaction_ordinal: u64, account_coverage: u8, cpi_coverage: u8) -> Result<Self> {
        ensure!(account_coverage <= 3, "unknown account coverage state");
        ensure!(cpi_coverage <= 4, "unknown CPI coverage state");
        ensure!(
            account_coverage != 0 || cpi_coverage != 0,
            "complete transaction must not use sparse coverage lane"
        );
        Ok(Self {
            transaction_ordinal,
            account_coverage,
            cpi_coverage,
        })
    }

    fn encode(self) -> [u8; COVERAGE_RECORD_LEN] {
        let mut output = [0_u8; COVERAGE_RECORD_LEN];
        output[0..8].copy_from_slice(&self.transaction_ordinal.to_le_bytes());
        output[8] = self.account_coverage;
        output[9] = self.cpi_coverage;
        output
    }

    fn decode(input: &[u8; COVERAGE_RECORD_LEN]) -> Result<Self> {
        ensure!(
            input[10..16] == [0; 6],
            "account coverage record has nonzero reserved bytes"
        );
        Self::new(
            u64::from_le_bytes(input[0..8].try_into().unwrap()),
            input[8],
            input[9],
        )
    }
}

fn message_schema_code(schema: CompactV2MessageSchema) -> u8 {
    match schema {
        CompactV2MessageSchema::Current => 0,
        CompactV2MessageSchema::May24PreUnknownFallbacks => 1,
    }
}

fn metadata_schema_code(schema: CompactV2MetadataSchema) -> u8 {
    match schema {
        CompactV2MetadataSchema::CurrentTypedError => 0,
        CompactV2MetadataSchema::LegacyRawError => 1,
    }
}

struct ScratchDirectory {
    path: PathBuf,
}

impl ScratchDirectory {
    fn create(root: &Path) -> Result<Self> {
        Self::create_named(root, ".standalone-account-postings-runs")
    }

    fn create_adaptive_v3(root: &Path) -> Result<Self> {
        Self::create_named(root, ".standalone-account-postings-adaptive-v3-runs")
    }

    fn create_named(root: &Path, name: &str) -> Result<Self> {
        let path = root.join(name);
        fs::create_dir(&path).with_context(|| {
            format!(
                "create account posting scratch {}; remove an abandoned staging directory before retrying",
                path.display()
            )
        })?;
        Ok(Self { path })
    }
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub struct Builder {
    binding: Binding,
    scratch: ScratchDirectory,
    output: File,
    coverage: File,
    records: Vec<SortRecord>,
    record_capacity: usize,
    runs: Vec<PathBuf>,
    next_run: u64,
    blocks: u64,
    transactions: u64,
    posting_count: u64,
    coverage_count: u64,
    incomplete_account_transactions: u64,
    incomplete_cpi_transactions: u64,
    previous_coverage_ordinal: Option<u64>,
    sort_memory_bytes: usize,
}

impl Builder {
    pub fn create(root: &Path, binding: Binding, sort_memory_bytes: usize) -> Result<Self> {
        ensure!(
            binding.standalone.slots_per_epoch != 0,
            "slots per epoch is zero"
        );
        ensure!(
            sort_memory_bytes >= mem::size_of::<SortRecord>(),
            "account posting sort memory must hold one record"
        );
        let record_capacity = sort_memory_bytes / mem::size_of::<SortRecord>();
        let scratch = ScratchDirectory::create(root)?;
        let path = root.join(PAGES_FILE);
        let mut output = File::options()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("create {}", path.display()))?;
        output.write_all(&Header::unfinished(binding).encode())?;
        let coverage_path = root.join(COVERAGE_FILE);
        let mut coverage = File::options()
            .write(true)
            .create_new(true)
            .open(&coverage_path)
            .with_context(|| format!("create {}", coverage_path.display()))?;
        coverage.write_all(&Header::unfinished(binding).encode_with_magic(COVERAGE_MAGIC))?;
        Ok(Self {
            binding,
            scratch,
            output,
            coverage,
            records: Vec::with_capacity(record_capacity),
            record_capacity,
            runs: Vec::new(),
            next_run: 0,
            blocks: 0,
            transactions: 0,
            posting_count: 0,
            coverage_count: 0,
            incomplete_account_transactions: 0,
            incomplete_cpi_transactions: 0,
            previous_coverage_ordinal: None,
            sort_memory_bytes,
        })
    }

    pub fn append_block(
        &mut self,
        block_id: u32,
        first_tx_ordinal: u64,
        tx_count: u32,
        records: Vec<SortRecord>,
        coverage: Vec<CoverageRecord>,
    ) -> Result<()> {
        ensure!(
            u64::from(block_id) == self.blocks,
            "account posting blocks are not ordered"
        );
        ensure!(
            first_tx_ordinal == self.transactions,
            "account posting transaction ordinals are not contiguous"
        );
        let transaction_end = first_tx_ordinal
            .checked_add(u64::from(tx_count))
            .context("account posting transaction range overflow")?;
        for record in records {
            ensure!(
                record.account_id <= self.binding.registry_entries,
                "account ID {} exceeds source registry size {}",
                record.account_id,
                self.binding.registry_entries
            );
            ensure!(
                record.transaction_ordinal >= first_tx_ordinal
                    && record.transaction_ordinal < transaction_end,
                "account posting ordinal {} is outside block {}",
                record.transaction_ordinal,
                block_id
            );
            self.records.push(record);
            self.posting_count = self
                .posting_count
                .checked_add(1)
                .context("account posting count overflow")?;
            if self.records.len() == self.record_capacity {
                self.flush_run()?;
            }
        }
        for record in coverage {
            ensure!(
                record.transaction_ordinal >= first_tx_ordinal
                    && record.transaction_ordinal < transaction_end,
                "coverage ordinal {} is outside block {}",
                record.transaction_ordinal,
                block_id
            );
            if let Some(previous) = self.previous_coverage_ordinal {
                ensure!(
                    record.transaction_ordinal > previous,
                    "sparse coverage ordinals do not strictly ascend"
                );
            }
            let next_coverage_count = self
                .coverage_count
                .checked_add(1)
                .context("coverage record count overflow")?;
            ensure!(
                next_coverage_count
                    .checked_mul(COVERAGE_RECORD_LEN as u64)
                    .is_some_and(|bytes| bytes <= MAX_COVERAGE_BYTES),
                "coverage lane exceeds reader cache guard"
            );
            self.coverage.write_all(&record.encode())?;
            self.previous_coverage_ordinal = Some(record.transaction_ordinal);
            self.coverage_count = next_coverage_count;
            self.incomplete_account_transactions = self
                .incomplete_account_transactions
                .checked_add(u64::from(record.account_coverage != 0))
                .context("incomplete account count overflow")?;
            self.incomplete_cpi_transactions = self
                .incomplete_cpi_transactions
                .checked_add(u64::from(record.cpi_coverage != 0))
                .context("incomplete CPI count overflow")?;
        }
        self.blocks = self
            .blocks
            .checked_add(1)
            .context("account block count overflow")?;
        self.transactions = transaction_end;
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        self.records.sort_unstable();
        for pair in self.records.windows(2) {
            ensure!(
                pair[0] < pair[1],
                "account posting repeats account {} transaction {}",
                pair[0].account_id,
                pair[0].transaction_ordinal
            );
        }
        let path = self
            .scratch
            .path
            .join(format!("run-{:08}.bin", self.next_run));
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("create account posting run {}", path.display()))?;
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
        for record in self.records.iter().copied() {
            writer.write_all(&record.encode())?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        self.records.clear();
        self.runs.push(path);
        self.next_run = self
            .next_run
            .checked_add(1)
            .context("sort run count overflow")?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<OutputSummary> {
        ensure!(
            self.blocks == self.binding.standalone.selected_blocks,
            "account posting block count differs from standalone binding"
        );
        ensure!(
            self.transactions == self.binding.standalone.selected_transactions,
            "account posting transaction count differs from standalone binding"
        );
        self.flush_run()?;
        drop(std::mem::take(&mut self.records));
        let sort_runs = u64::try_from(self.runs.len()).context("sort run count exceeds u64")?;
        let (runs, merge_passes) = reduce_runs(std::mem::take(&mut self.runs), &self.scratch.path)?;
        let directory_path = self.scratch.path.join("directory.bin");
        let mut pages = PageWriter::new(self.output, &directory_path)?;
        let (distinct_accounts, merged) = write_merged_pages(&mut pages, runs)?;
        ensure!(
            merged == self.posting_count,
            "merged posting count differs from input"
        );
        let page_summary = pages.finish()?;
        let coverage_footer = {
            let mut bytes = [0_u8; COVERAGE_FOOTER_LEN];
            bytes[0..8].copy_from_slice(&COVERAGE_FOOTER_MAGIC);
            bytes[8..16].copy_from_slice(&self.coverage_count.to_le_bytes());
            bytes
        };
        self.coverage.write_all(&coverage_footer)?;
        self.coverage.flush()?;
        self.coverage.sync_all()?;
        let coverage_file_bytes = (HEADER_LEN as u64)
            .checked_add(
                self.coverage_count
                    .checked_mul(COVERAGE_RECORD_LEN as u64)
                    .context("coverage byte count overflow")?,
            )
            .and_then(|value| value.checked_add(COVERAGE_FOOTER_LEN as u64))
            .context("coverage file length overflow")?;
        ensure!(
            self.coverage_count
                .checked_mul(COVERAGE_RECORD_LEN as u64)
                .is_some_and(|bytes| bytes <= MAX_COVERAGE_BYTES),
            "coverage lane exceeds reader cache guard"
        );
        ensure!(
            self.coverage.metadata()?.len() == coverage_file_bytes,
            "coverage file length differs"
        );
        let control = Control {
            header: Header::unfinished(self.binding),
            postings: merged,
            distinct_accounts,
            coverage_records: self.coverage_count,
            incomplete_account_transactions: self.incomplete_account_transactions,
            incomplete_cpi_transactions: self.incomplete_cpi_transactions,
        };
        let control_path = self
            .scratch
            .path
            .parent()
            .context("account posting scratch has no output parent")?
            .join(CONTROL_FILE);
        let mut control_file = File::options()
            .write(true)
            .create_new(true)
            .open(&control_path)
            .with_context(|| format!("create {}", control_path.display()))?;
        control_file.write_all(&control.encode())?;
        control_file.flush()?;
        control_file.sync_all()?;
        let initial_run_bytes = merged
            .checked_mul(SORT_RECORD_LEN as u64)
            .context("initial account posting run bytes overflow")?;
        let peak_scratch_upper_bound_bytes = initial_run_bytes
            .checked_add(initial_run_bytes.max(page_summary.directory_bytes))
            .context("account posting scratch bound overflow")?;
        let total_index_bytes = page_summary
            .file_bytes
            .checked_add(CONTROL_LEN as u64)
            .and_then(|value| value.checked_add(coverage_file_bytes))
            .context("total account posting byte count overflow")?;
        Ok(OutputSummary {
            status: STATUS,
            canary_kind: CANARY_KIND,
            format_status: "measurement-container-not-final-schema",
            account_semantics: ACCOUNT_SEMANTICS,
            coverage_semantics: "sparse-incomplete-transaction-coverage-v1",
            source_registry_binding: "entry-count-only-no-identity-digest",
            canonical_forward_account_source: "standalone-messages-plus-loaded-addresses",
            forward_projection_disposition: "measurement_only_excluded_from_required_cloud_bytes",
            file_name: PAGES_FILE,
            control_file_name: CONTROL_FILE,
            coverage_file_name: COVERAGE_FILE,
            report_file_name: REPORT_FILE,
            payload_schema: postings::SCHEMA,
            zstd_level: ZSTD_LEVEL,
            exact_source_ids_only: true,
            program_roles_in_account_postings: true,
            separate_program_postings_file: false,
            blocks: self.blocks,
            transactions: self.transactions,
            registry_entries: self.binding.registry_entries,
            postings: merged,
            distinct_accounts,
            raw_references: 0,
            coverage_records: self.coverage_count,
            incomplete_account_transactions: self.incomplete_account_transactions,
            incomplete_cpi_transactions: self.incomplete_cpi_transactions,
            sort_memory_bytes: u64::try_from(self.sort_memory_bytes)?,
            initial_run_bytes,
            peak_scratch_upper_bound_bytes,
            max_open_index_files: MERGE_FAN_IN as u32 + 3,
            reader_max_coverage_cache_bytes: MAX_COVERAGE_BYTES,
            sort_runs,
            merge_passes,
            pages: page_summary.pages,
            continuation_pages: page_summary.continuation_pages,
            peak_page_postings: page_summary.peak_page_postings,
            decoded_page_bytes: page_summary.decoded_page_bytes,
            stored_page_bytes: page_summary.page_bytes,
            page_bytes: page_summary.page_bytes,
            directory_bytes: page_summary.directory_bytes,
            file_bytes: page_summary.file_bytes,
            control_file_bytes: CONTROL_LEN as u64,
            coverage_file_bytes,
            total_index_bytes,
            bytes_per_posting: if merged == 0 {
                0.0
            } else {
                total_index_bytes as f64 / merged as f64
            },
            page_stored_to_decoded_ratio: if page_summary.decoded_page_bytes == 0 {
                0.0
            } else {
                page_summary.page_bytes as f64 / page_summary.decoded_page_bytes as f64
            },
            compression_time_ms: u64::try_from(page_summary.compression_time.as_millis())
                .unwrap_or(u64::MAX),
        })
    }
}

/// Additive candidate writer for adaptive reverse postings schema 3.
///
/// It accepts the same ordered source records as [`Builder`], but all final
/// objects use separate candidate names and magics.
pub struct AdaptiveV3Builder {
    binding: Binding,
    options: AdaptiveV3Options,
    scratch: ScratchDirectory,
    output: File,
    coverage: File,
    records: Vec<SortRecord>,
    record_capacity: usize,
    runs: Vec<PathBuf>,
    next_run: u64,
    spans: Vec<block_group_measurement::BlockSpan>,
    blocks: u64,
    transactions: u64,
    posting_count: u64,
    coverage_count: u64,
    incomplete_account_transactions: u64,
    incomplete_cpi_transactions: u64,
    previous_coverage_ordinal: Option<u64>,
    sort_memory_bytes: usize,
    peak_input_record_heap_bytes: usize,
    peak_input_coverage_heap_bytes: usize,
    sort_run_time: Duration,
    fill_started: Instant,
    sort_run_phases: Vec<AdaptiveV3SortRunPhase>,
    started: Instant,
}

impl AdaptiveV3Builder {
    pub fn create(
        root: &Path,
        binding: Binding,
        sort_memory_bytes: usize,
        options: AdaptiveV3Options,
    ) -> Result<Self> {
        ensure!(
            binding.standalone.slots_per_epoch != 0,
            "adaptive v3 slots per epoch is zero"
        );
        ensure!(
            sort_memory_bytes >= mem::size_of::<SortRecord>()
                && sort_memory_bytes <= ADAPTIVE_V3_MAX_SORT_MEMORY_BYTES,
            "adaptive v3 sort memory is outside {}..={} bytes",
            mem::size_of::<SortRecord>(),
            ADAPTIVE_V3_MAX_SORT_MEMORY_BYTES
        );
        ensure!(
            options.merge_workers != 0 && options.merge_workers <= ADAPTIVE_V3_MAX_MERGE_WORKERS,
            "adaptive v3 merge workers must be in 1..={ADAPTIVE_V3_MAX_MERGE_WORKERS}"
        );
        ensure!(
            options.page_workers != 0 && options.page_workers <= ADAPTIVE_V3_MAX_PAGE_WORKERS,
            "adaptive v3 page workers must be in 1..={ADAPTIVE_V3_MAX_PAGE_WORKERS}"
        );
        let selected_blocks = usize::try_from(binding.standalone.selected_blocks)
            .context("adaptive v3 selected block count exceeds usize")?;
        let span_heap_bytes = selected_blocks
            .checked_mul(mem::size_of::<block_group_measurement::BlockSpan>())
            .context("adaptive v3 block-span heap length overflow")?;
        ensure!(
            span_heap_bytes <= ADAPTIVE_V3_MAX_BLOCK_SPAN_HEAP_BYTES,
            "adaptive v3 block-span heap exceeds {ADAPTIVE_V3_MAX_BLOCK_SPAN_HEAP_BYTES} bytes"
        );
        let scratch = ScratchDirectory::create_adaptive_v3(root)?;
        let output_path = root.join(ADAPTIVE_V3_PAGES_FILE);
        let mut output = File::options()
            .write(true)
            .create_new(true)
            .open(&output_path)
            .with_context(|| format!("create {}", output_path.display()))?;
        output.write_all(&AdaptiveV3Header::unfinished(binding).encode())?;
        let coverage_path = root.join(ADAPTIVE_V3_COVERAGE_FILE);
        let mut coverage = File::options()
            .write(true)
            .create_new(true)
            .open(&coverage_path)
            .with_context(|| format!("create {}", coverage_path.display()))?;
        coverage.write_all(
            &AdaptiveV3Header::unfinished(binding).encode_with_magic(ADAPTIVE_V3_COVERAGE_MAGIC),
        )?;
        let record_capacity = sort_memory_bytes / mem::size_of::<SortRecord>();
        let mut records = Vec::new();
        records
            .try_reserve_exact(record_capacity)
            .context("reserve adaptive v3 sort records")?;
        let mut spans = Vec::new();
        spans
            .try_reserve_exact(selected_blocks)
            .context("reserve adaptive v3 block spans")?;
        ensure!(
            spans
                .capacity()
                .checked_mul(mem::size_of::<block_group_measurement::BlockSpan>())
                .is_some_and(|bytes| bytes <= ADAPTIVE_V3_MAX_BLOCK_SPAN_HEAP_BYTES),
            "adaptive v3 reserved block-span heap exceeds its cap"
        );
        let started = Instant::now();
        Ok(Self {
            binding,
            options,
            scratch,
            output,
            coverage,
            records,
            record_capacity,
            runs: Vec::new(),
            next_run: 0,
            spans,
            blocks: 0,
            transactions: 0,
            posting_count: 0,
            coverage_count: 0,
            incomplete_account_transactions: 0,
            incomplete_cpi_transactions: 0,
            previous_coverage_ordinal: None,
            sort_memory_bytes,
            peak_input_record_heap_bytes: 0,
            peak_input_coverage_heap_bytes: 0,
            sort_run_time: Duration::ZERO,
            fill_started: started,
            sort_run_phases: Vec::new(),
            started,
        })
    }

    pub fn append_block(
        &mut self,
        block_id: u32,
        first_tx_ordinal: u64,
        tx_count: u32,
        records: Vec<SortRecord>,
        coverage: Vec<CoverageRecord>,
    ) -> Result<()> {
        let input_record_heap_bytes = records
            .capacity()
            .checked_mul(mem::size_of::<SortRecord>())
            .context("adaptive v3 input record heap overflow")?;
        ensure!(
            input_record_heap_bytes <= ADAPTIVE_V3_MAX_SORT_MEMORY_BYTES,
            "adaptive v3 input record heap exceeds its cap"
        );
        self.peak_input_record_heap_bytes = self
            .peak_input_record_heap_bytes
            .max(input_record_heap_bytes);
        let input_coverage_heap_bytes = coverage
            .capacity()
            .checked_mul(mem::size_of::<CoverageRecord>())
            .context("adaptive v3 input coverage heap overflow")?;
        ensure!(
            input_coverage_heap_bytes <= MAX_COVERAGE_BYTES as usize,
            "adaptive v3 input coverage heap exceeds coverage cap"
        );
        self.peak_input_coverage_heap_bytes = self
            .peak_input_coverage_heap_bytes
            .max(input_coverage_heap_bytes);
        ensure!(
            u64::from(block_id) == self.blocks,
            "adaptive v3 account posting blocks are not ordered"
        );
        ensure!(
            first_tx_ordinal == self.transactions,
            "adaptive v3 transaction ordinals are not contiguous"
        );
        let transaction_end = first_tx_ordinal
            .checked_add(u64::from(tx_count))
            .context("adaptive v3 transaction range overflow")?;
        self.spans.push(block_group_measurement::BlockSpan {
            block_id,
            first_tx_ordinal,
            tx_count,
        });
        ensure!(
            self.spans
                .capacity()
                .checked_mul(mem::size_of::<block_group_measurement::BlockSpan>())
                .is_some_and(|bytes| bytes <= ADAPTIVE_V3_MAX_BLOCK_SPAN_HEAP_BYTES),
            "adaptive v3 block-span heap exceeds its cap"
        );
        for record in records {
            ensure!(
                record.account_id <= self.binding.registry_entries,
                "adaptive v3 account ID {} exceeds source registry size {}",
                record.account_id,
                self.binding.registry_entries
            );
            ensure!(
                record.transaction_ordinal >= first_tx_ordinal
                    && record.transaction_ordinal < transaction_end,
                "adaptive v3 account posting ordinal {} is outside block {}",
                record.transaction_ordinal,
                block_id
            );
            self.records.push(record);
            self.posting_count = self
                .posting_count
                .checked_add(1)
                .context("adaptive v3 posting count overflow")?;
            if self.records.len() == self.record_capacity {
                self.flush_run()?;
            }
        }
        for record in coverage {
            ensure!(
                record.transaction_ordinal >= first_tx_ordinal
                    && record.transaction_ordinal < transaction_end,
                "adaptive v3 coverage ordinal {} is outside block {}",
                record.transaction_ordinal,
                block_id
            );
            if let Some(previous) = self.previous_coverage_ordinal {
                ensure!(
                    record.transaction_ordinal > previous,
                    "adaptive v3 sparse coverage ordinals do not strictly ascend"
                );
            }
            let next_count = self
                .coverage_count
                .checked_add(1)
                .context("adaptive v3 coverage record count overflow")?;
            ensure!(
                next_count
                    .checked_mul(COVERAGE_RECORD_LEN as u64)
                    .is_some_and(|bytes| bytes <= MAX_COVERAGE_BYTES),
                "adaptive v3 coverage lane exceeds reader cache guard"
            );
            self.coverage.write_all(&record.encode())?;
            self.previous_coverage_ordinal = Some(record.transaction_ordinal);
            self.coverage_count = next_count;
            self.incomplete_account_transactions = self
                .incomplete_account_transactions
                .checked_add(u64::from(record.account_coverage != 0))
                .context("adaptive v3 incomplete account count overflow")?;
            self.incomplete_cpi_transactions = self
                .incomplete_cpi_transactions
                .checked_add(u64::from(record.cpi_coverage != 0))
                .context("adaptive v3 incomplete CPI count overflow")?;
        }
        self.blocks = self
            .blocks
            .checked_add(1)
            .context("adaptive v3 block count overflow")?;
        self.transactions = transaction_end;
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        let fill_wall = self.fill_started.elapsed();
        let records = u64::try_from(self.records.len())?;
        let bytes = records
            .checked_mul(SORT_RECORD_LEN as u64)
            .context("adaptive v3 sort-run byte count overflow")?;
        let total_started = Instant::now();
        let sort_started = Instant::now();
        self.records.sort_unstable();
        for pair in self.records.windows(2) {
            ensure!(
                pair[0] < pair[1],
                "adaptive v3 account posting repeats account {} transaction {}",
                pair[0].account_id,
                pair[0].transaction_ordinal
            );
        }
        let sort_wall = sort_started.elapsed();
        let path = self
            .scratch
            .path
            .join(format!("v3-run-{:08}.bin", self.next_run));
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("create adaptive v3 run {}", path.display()))?;
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
        let write_started = Instant::now();
        for record in self.records.iter().copied() {
            writer.write_all(&record.encode())?;
        }
        writer.flush()?;
        let write_wall = write_started.elapsed();
        let sync_started = Instant::now();
        writer.get_ref().sync_all()?;
        let sync_wall = sync_started.elapsed();
        let sort_write_sync_wall = total_started.elapsed();
        self.records.clear();
        self.runs.push(path);
        self.sort_run_phases.push(AdaptiveV3SortRunPhase {
            run: self.next_run,
            records,
            bytes,
            buffer_capacity_records: u64::try_from(self.record_capacity)?,
            buffer_capacity_bytes: u64::try_from(self.sort_memory_bytes)?,
            full_buffer: records == u64::try_from(self.record_capacity)?,
            fill_wall_ms: duration_millis_saturating(fill_wall),
            sort_wall_ms: duration_millis_saturating(sort_wall),
            write_wall_ms: duration_millis_saturating(write_wall),
            sync_wall_ms: duration_millis_saturating(sync_wall),
            sort_write_sync_wall_ms: duration_millis_saturating(sort_write_sync_wall),
        });
        self.next_run = self
            .next_run
            .checked_add(1)
            .context("adaptive v3 sort run count overflow")?;
        self.sort_run_time = self.sort_run_time.saturating_add(sort_write_sync_wall);
        self.fill_started = Instant::now();
        Ok(())
    }

    pub fn finish(mut self) -> Result<AdaptiveV3OutputSummary> {
        ensure!(
            self.blocks == self.binding.standalone.selected_blocks,
            "adaptive v3 block count differs from standalone binding"
        );
        ensure!(
            self.transactions == self.binding.standalone.selected_transactions,
            "adaptive v3 transaction count differs from standalone binding"
        );
        self.flush_run()?;
        drop(std::mem::take(&mut self.records));
        let sort_runs = u64::try_from(self.runs.len())?;
        let (runs, merge) = reduce_runs_adaptive_v3(
            std::mem::take(&mut self.runs),
            &self.scratch.path,
            self.options.merge_workers,
        )?;
        ensure!(
            runs.len() <= ADAPTIVE_V3_MERGE_FAN_IN,
            "adaptive v3 final merge exceeds fan-in"
        );
        let final_merge_run_count = runs.len();
        let final_merge_open_files = runs
            .len()
            .checked_add(3)
            .context("adaptive v3 final merge file count overflow")?
            .max(4);
        let directory_path = self.scratch.path.join("v3-directory.bin");
        let span_heap_bytes = self
            .spans
            .capacity()
            .checked_mul(mem::size_of::<block_group_measurement::BlockSpan>())
            .context("adaptive v3 block-span heap size overflow")?;
        let layout =
            block_group_measurement::ValidatedBlockLayout::new(std::mem::take(&mut self.spans))?;
        ensure!(
            layout.transactions() == self.transactions,
            "adaptive v3 block spans differ from transaction count"
        );
        let mut pages = AdaptiveV3PageWriter::new(
            self.output,
            &directory_path,
            layout,
            self.options.page_workers,
        )?;
        let (distinct_accounts, merged, final_merge) =
            write_merged_adaptive_v3_pages(&mut pages, runs)?;
        ensure!(
            merged == self.posting_count,
            "adaptive v3 merged posting count differs from input"
        );
        let page_summary = pages.finish()?;

        let mut coverage_footer = [0_u8; COVERAGE_FOOTER_LEN];
        coverage_footer[0..8].copy_from_slice(&ADAPTIVE_V3_COVERAGE_FOOTER_MAGIC);
        coverage_footer[8..16].copy_from_slice(&self.coverage_count.to_le_bytes());
        self.coverage.write_all(&coverage_footer)?;
        self.coverage.flush()?;
        self.coverage.sync_all()?;
        let coverage_file_bytes = (HEADER_LEN as u64)
            .checked_add(
                self.coverage_count
                    .checked_mul(COVERAGE_RECORD_LEN as u64)
                    .context("adaptive v3 coverage byte count overflow")?,
            )
            .and_then(|value| value.checked_add(COVERAGE_FOOTER_LEN as u64))
            .context("adaptive v3 coverage file length overflow")?;
        ensure!(
            self.coverage.metadata()?.len() == coverage_file_bytes,
            "adaptive v3 coverage file length differs"
        );

        let control = AdaptiveV3Control {
            header: AdaptiveV3Header::unfinished(self.binding),
            postings: merged,
            distinct_accounts,
            coverage_records: self.coverage_count,
            incomplete_account_transactions: self.incomplete_account_transactions,
            incomplete_cpi_transactions: self.incomplete_cpi_transactions,
        };
        let control_path = self
            .scratch
            .path
            .parent()
            .context("adaptive v3 scratch has no output parent")?
            .join(ADAPTIVE_V3_CONTROL_FILE);
        let mut control_file = File::options()
            .write(true)
            .create_new(true)
            .open(&control_path)
            .with_context(|| format!("create {}", control_path.display()))?;
        control_file.write_all(&control.encode())?;
        control_file.flush()?;
        control_file.sync_all()?;

        let initial_run_bytes = merged
            .checked_mul(SORT_RECORD_LEN as u64)
            .context("adaptive v3 initial run bytes overflow")?;
        let peak_scratch_upper_bound_bytes = initial_run_bytes
            .checked_add(initial_run_bytes.max(page_summary.directory_bytes))
            .context("adaptive v3 scratch bound overflow")?;
        let total_index_bytes = page_summary
            .file_bytes
            .checked_add(CONTROL_LEN as u64)
            .and_then(|value| value.checked_add(coverage_file_bytes))
            .context("adaptive v3 total byte count overflow")?;
        let peak_open_files_upper_bound = merge
            .peak_open_files_upper_bound
            .max(final_merge_open_files);
        ensure!(
            peak_open_files_upper_bound <= ADAPTIVE_V3_MAX_OPEN_FILES,
            "adaptive v3 open-file bound exceeds {ADAPTIVE_V3_MAX_OPEN_FILES}"
        );
        let append_memory_upper_bound = self
            .sort_memory_bytes
            .checked_add(self.peak_input_record_heap_bytes)
            .and_then(|bytes| bytes.checked_add(self.peak_input_coverage_heap_bytes))
            .and_then(|bytes| bytes.checked_add(span_heap_bytes))
            .context("adaptive v3 append memory bound overflow")?;
        let merge_memory_upper_bound = merge
            .peak_buffer_bytes_upper_bound
            .checked_add(span_heap_bytes)
            .context("adaptive v3 merge memory bound overflow")?;
        let page_key_heap_upper_bound = (postings::MAX_KEYS_PER_PAGE as usize)
            .checked_mul(mem::size_of::<postings::KeyPostings>())
            .and_then(|bytes| {
                bytes.checked_add(
                    (postings::MAX_POSTINGS_PER_PAGE as usize)
                        .checked_mul(mem::size_of::<postings::Posting>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    (postings::MAX_POSTINGS_PER_PAGE as usize)
                        .checked_mul(mem::size_of::<postings::Posting>())?
                        .checked_mul(2)?,
                )
            })
            .context("adaptive v3 page-key heap bound overflow")?;
        let final_merge_buffer_upper_bound = final_merge_run_count
            .checked_mul(IO_BUFFER_BYTES)
            .context("adaptive v3 final merge buffer bound overflow")?;
        let page_memory_upper_bound = page_summary
            .peak_live_bytes_upper_bound
            .checked_add(span_heap_bytes)
            .and_then(|bytes| bytes.checked_add(page_key_heap_upper_bound))
            .and_then(|bytes| bytes.checked_add(final_merge_buffer_upper_bound))
            .and_then(|bytes| bytes.checked_add(postings::MAX_PAGE_DECODED_BYTES as usize))
            .and_then(|bytes| bytes.checked_add(9 * IO_BUFFER_BYTES))
            .and_then(|bytes| bytes.checked_add(page_summary.peak_block_codec_scratch_bytes))
            .context("adaptive v3 page memory bound overflow")?;
        let peak_accounted_memory_upper_bound = append_memory_upper_bound
            .max(merge_memory_upper_bound)
            .max(page_memory_upper_bound);
        Ok(AdaptiveV3OutputSummary {
            status: STATUS,
            canary_kind: "standalone-exact-account-postings-adaptive-v3",
            format_status: "candidate-adaptive-v3-not-publishable",
            account_semantics: ACCOUNT_SEMANTICS,
            coverage_semantics: "sparse-incomplete-transaction-coverage-v1",
            file_name: ADAPTIVE_V3_PAGES_FILE,
            control_file_name: ADAPTIVE_V3_CONTROL_FILE,
            coverage_file_name: ADAPTIVE_V3_COVERAGE_FILE,
            report_file_name: ADAPTIVE_V3_REPORT_FILE,
            payload_schema: ADAPTIVE_V3_PAYLOAD_SCHEMA,
            zstd_level: ZSTD_LEVEL,
            blocks: self.blocks,
            transactions: self.transactions,
            registry_entries: self.binding.registry_entries,
            postings: merged,
            distinct_accounts,
            coverage_records: self.coverage_count,
            incomplete_account_transactions: self.incomplete_account_transactions,
            incomplete_cpi_transactions: self.incomplete_cpi_transactions,
            sort_memory_bytes: u64::try_from(self.sort_memory_bytes)?,
            peak_input_record_heap_bytes: u64::try_from(self.peak_input_record_heap_bytes)?,
            peak_input_coverage_heap_bytes: u64::try_from(self.peak_input_coverage_heap_bytes)?,
            block_span_heap_bytes: u64::try_from(span_heap_bytes)?,
            block_span_heap_cap_bytes: ADAPTIVE_V3_MAX_BLOCK_SPAN_HEAP_BYTES as u64,
            page_key_heap_upper_bound_bytes: u64::try_from(page_key_heap_upper_bound)?,
            final_merge_buffer_upper_bound_bytes: u64::try_from(final_merge_buffer_upper_bound)?,
            append_memory_upper_bound_bytes: u64::try_from(append_memory_upper_bound)?,
            merge_memory_upper_bound_bytes: u64::try_from(merge_memory_upper_bound)?,
            page_memory_upper_bound_bytes: u64::try_from(page_memory_upper_bound)?,
            peak_accounted_memory_upper_bound_bytes: u64::try_from(
                peak_accounted_memory_upper_bound,
            )?,
            memory_accounting_scope: "major owned buffers and fixed codec workspaces; excludes allocator metadata, path and heap nodes, thread stacks, and OS page cache",
            initial_run_bytes,
            initial_run_count: sort_runs,
            initial_run_records: merged,
            peak_scratch_upper_bound_bytes,
            sort_runs,
            sort_run_phase_scope: "fill wall starts after builder creation or prior run sync and includes upstream block processing until this buffer flush; sort/write/sync fields are exclusive local wall intervals",
            sort_run_phases: self.sort_run_phases,
            merge_passes: merge.passes,
            merge_pass_phase_scope: "pass_wall is elapsed pass time; read/write/sync and worker_wall_sum fields add task-local worker intervals and can exceed pass wall",
            merge_pass_phases: merge.pass_phases,
            merge_fan_in: ADAPTIVE_V3_MERGE_FAN_IN,
            merge_workers: self.options.merge_workers,
            peak_merge_workers_active: merge.peak_active_workers,
            page_workers: self.options.page_workers,
            page_work_window: page_summary.work_window,
            peak_open_files_upper_bound,
            open_file_cap: ADAPTIVE_V3_MAX_OPEN_FILES,
            peak_merge_buffer_bytes_upper_bound: u64::try_from(
                merge.peak_buffer_bytes_upper_bound,
            )?,
            compression_live_budget_bytes: ADAPTIVE_V3_COMPRESSION_LIVE_BUDGET as u64,
            peak_compression_live_bytes_upper_bound: u64::try_from(
                page_summary.peak_live_bytes_upper_bound,
            )?,
            peak_zstd_queue_jobs: page_summary.peak_queue_jobs,
            peak_zstd_workspace_bytes: u64::try_from(page_summary.peak_zstd_workspace_bytes)?,
            zstd_workspace_cap_bytes: block_group_measurement::MAX_ZSTD_COMPRESSION_WORKSPACE_BYTES
                as u64,
            peak_block_codec_scratch_bytes: u64::try_from(
                page_summary.peak_block_codec_scratch_bytes,
            )?,
            block_codec_scratch_cap_bytes: block_group_measurement::MAX_BLOCK_CODEC_SCRATCH_BYTES
                as u64,
            pages: page_summary.pages,
            continuation_pages: page_summary.continuation_pages,
            peak_page_postings: page_summary.peak_page_postings,
            ordinal_key_fragments: page_summary.codec.ordinal_key_fragments,
            block_group_key_fragments: page_summary.codec.block_group_key_fragments,
            local_varint_groups: page_summary.codec.local_varint_groups,
            local_bitpack_groups: page_summary.codec.local_bitpack_groups,
            local_bitmap_groups: page_summary.codec.local_bitmap_groups,
            current_ordinal_decoded_page_bytes: page_summary.current_decoded_page_bytes,
            adaptive_decoded_page_bytes: page_summary.decoded_page_bytes,
            zstd_frame_bytes: page_summary.zstd_frame_bytes,
            stored_page_bytes: page_summary.page_bytes,
            raw_selected_pages: page_summary.raw_selected_pages,
            zstd_selected_pages: page_summary.zstd_selected_pages,
            directory_bytes: page_summary.directory_bytes,
            file_bytes: page_summary.file_bytes,
            control_file_bytes: CONTROL_LEN as u64,
            coverage_file_bytes,
            total_index_bytes,
            bytes_per_posting: if merged == 0 {
                0.0
            } else {
                total_index_bytes as f64 / merged as f64
            },
            sort_run_wall_ms: duration_millis_saturating(self.sort_run_time),
            merge_wall_ms: duration_millis_saturating(merge.wall_time),
            merge_worker_ms: duration_millis_saturating(merge.worker_time),
            final_merge_phase_scope: "read wall measures underlying buffered file-read calls; record_coalescing_and_heap is the non-page residual after file reads and includes run open plus heap operations; all are elapsed wall intervals on the final merge thread",
            final_merge,
            page_phase_scope: "adaptive encode and queue are serial wall intervals; compression_worker is a concurrent worker sum; compression_wall, page_write, and page_sync are elapsed wall intervals",
            adaptive_encode_ms: duration_millis_saturating(page_summary.encode_time),
            adaptive_codec_encode_wall_ms: duration_millis_saturating(page_summary.encode_time),
            zstd_queue_wall_ms: duration_millis_saturating(page_summary.queue_time),
            zstd_queue_flushes: page_summary.queue_flushes,
            compression_wall_ms: duration_millis_saturating(page_summary.compression_wall_time),
            compression_worker_ms: duration_millis_saturating(page_summary.compression_worker_time),
            page_write_ms: duration_millis_saturating(page_summary.write_time),
            page_write_bytes: page_summary.write_bytes,
            page_sync_ms: duration_millis_saturating(page_summary.sync_time),
            total_wall_ms: duration_millis_saturating(self.started.elapsed()),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveV3OutputSummary {
    pub status: &'static str,
    pub canary_kind: &'static str,
    pub format_status: &'static str,
    pub account_semantics: &'static str,
    pub coverage_semantics: &'static str,
    pub file_name: &'static str,
    pub control_file_name: &'static str,
    pub coverage_file_name: &'static str,
    pub report_file_name: &'static str,
    pub payload_schema: u16,
    pub zstd_level: i32,
    pub blocks: u64,
    pub transactions: u64,
    pub registry_entries: u32,
    pub postings: u64,
    pub distinct_accounts: u64,
    pub coverage_records: u64,
    pub incomplete_account_transactions: u64,
    pub incomplete_cpi_transactions: u64,
    pub sort_memory_bytes: u64,
    pub peak_input_record_heap_bytes: u64,
    pub peak_input_coverage_heap_bytes: u64,
    pub block_span_heap_bytes: u64,
    pub block_span_heap_cap_bytes: u64,
    pub page_key_heap_upper_bound_bytes: u64,
    pub final_merge_buffer_upper_bound_bytes: u64,
    pub append_memory_upper_bound_bytes: u64,
    pub merge_memory_upper_bound_bytes: u64,
    pub page_memory_upper_bound_bytes: u64,
    pub peak_accounted_memory_upper_bound_bytes: u64,
    pub memory_accounting_scope: &'static str,
    pub initial_run_bytes: u64,
    pub initial_run_count: u64,
    pub initial_run_records: u64,
    pub peak_scratch_upper_bound_bytes: u64,
    pub sort_runs: u64,
    pub sort_run_phase_scope: &'static str,
    pub sort_run_phases: Vec<AdaptiveV3SortRunPhase>,
    pub merge_passes: u32,
    pub merge_pass_phase_scope: &'static str,
    pub merge_pass_phases: Vec<AdaptiveV3MergePassPhase>,
    pub merge_fan_in: usize,
    pub merge_workers: usize,
    pub peak_merge_workers_active: usize,
    pub page_workers: usize,
    pub page_work_window: usize,
    pub peak_open_files_upper_bound: usize,
    pub open_file_cap: usize,
    pub peak_merge_buffer_bytes_upper_bound: u64,
    pub compression_live_budget_bytes: u64,
    pub peak_compression_live_bytes_upper_bound: u64,
    pub peak_zstd_queue_jobs: usize,
    pub peak_zstd_workspace_bytes: u64,
    pub zstd_workspace_cap_bytes: u64,
    pub peak_block_codec_scratch_bytes: u64,
    pub block_codec_scratch_cap_bytes: u64,
    pub pages: u64,
    pub continuation_pages: u64,
    pub peak_page_postings: usize,
    pub ordinal_key_fragments: u64,
    pub block_group_key_fragments: u64,
    pub local_varint_groups: u64,
    pub local_bitpack_groups: u64,
    pub local_bitmap_groups: u64,
    pub current_ordinal_decoded_page_bytes: u64,
    pub adaptive_decoded_page_bytes: u64,
    pub zstd_frame_bytes: u64,
    pub stored_page_bytes: u64,
    pub raw_selected_pages: u64,
    pub zstd_selected_pages: u64,
    pub directory_bytes: u64,
    pub file_bytes: u64,
    pub control_file_bytes: u64,
    pub coverage_file_bytes: u64,
    pub total_index_bytes: u64,
    pub bytes_per_posting: f64,
    pub sort_run_wall_ms: u64,
    pub merge_wall_ms: u64,
    pub merge_worker_ms: u64,
    pub final_merge_phase_scope: &'static str,
    pub final_merge: AdaptiveV3FinalMergePhase,
    pub page_phase_scope: &'static str,
    pub adaptive_encode_ms: u64,
    pub adaptive_codec_encode_wall_ms: u64,
    pub zstd_queue_wall_ms: u64,
    pub zstd_queue_flushes: u64,
    pub compression_wall_ms: u64,
    pub compression_worker_ms: u64,
    pub page_write_ms: u64,
    pub page_write_bytes: u64,
    pub page_sync_ms: u64,
    pub total_wall_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct AdaptiveV3SortRunPhase {
    pub run: u64,
    pub records: u64,
    pub bytes: u64,
    pub buffer_capacity_records: u64,
    pub buffer_capacity_bytes: u64,
    pub full_buffer: bool,
    pub fill_wall_ms: u64,
    pub sort_wall_ms: u64,
    pub write_wall_ms: u64,
    pub sync_wall_ms: u64,
    pub sort_write_sync_wall_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct AdaptiveV3MergePassPhase {
    pub pass: u32,
    pub input_runs: u64,
    pub output_runs: u64,
    pub records: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub read_worker_sum_ms: u64,
    pub write_worker_sum_ms: u64,
    pub sync_worker_sum_ms: u64,
    pub worker_wall_sum_ms: u64,
    pub pass_wall_ms: u64,
    pub effective_read_mib_per_second: f64,
    pub effective_write_mib_per_second: f64,
    pub effective_total_io_mib_per_second: f64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct AdaptiveV3FinalMergePhase {
    pub input_runs: u64,
    pub records: u64,
    pub read_bytes: u64,
    pub read_wall_ms: u64,
    pub k_way_heap_and_coalescing_wall_ms: u64,
    pub record_coalescing_and_heap_wall_ms: u64,
    pub total_wall_ms: u64,
    pub effective_read_mib_per_second: f64,
}

fn duration_millis_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn mib_rate(bytes: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        0.0
    } else {
        bytes as f64 / (1024.0 * 1024.0) / seconds
    }
}

struct RunReader {
    reader: BufReader<TimedFileReader>,
    remaining: u64,
    path: PathBuf,
}

struct TimedFileReader {
    file: File,
    read_bytes: u64,
    read_wall_time: Duration,
}

impl Read for TimedFileReader {
    fn read(&mut self, destination: &mut [u8]) -> std::io::Result<usize> {
        let started = Instant::now();
        let result = self.file.read(destination);
        self.read_wall_time = self.read_wall_time.saturating_add(started.elapsed());
        if let Ok(bytes) = result {
            self.read_bytes = self.read_bytes.saturating_add(bytes as u64);
        }
        result
    }
}

struct TimedFileWriter {
    file: File,
    write_bytes: u64,
    write_wall_time: Duration,
}

impl Write for TimedFileWriter {
    fn write(&mut self, source: &[u8]) -> std::io::Result<usize> {
        let started = Instant::now();
        let result = self.file.write(source);
        self.write_wall_time = self.write_wall_time.saturating_add(started.elapsed());
        if let Ok(bytes) = result {
            self.write_bytes = self.write_bytes.saturating_add(bytes as u64);
        }
        result
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let started = Instant::now();
        let result = self.file.flush();
        self.write_wall_time = self.write_wall_time.saturating_add(started.elapsed());
        result
    }
}

impl RunReader {
    fn open(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)
            .with_context(|| format!("open account posting run {}", path.display()))?;
        let bytes = file.metadata()?.len();
        ensure!(
            bytes.is_multiple_of(SORT_RECORD_LEN as u64),
            "account posting run {} is not record aligned",
            path.display()
        );
        Ok(Self {
            reader: BufReader::with_capacity(
                IO_BUFFER_BYTES,
                TimedFileReader {
                    file,
                    read_bytes: 0,
                    read_wall_time: Duration::ZERO,
                },
            ),
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
            .with_context(|| format!("read account posting run {}", self.path.display()))?;
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
            "account posting merge fan-in is too large"
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
                "merged account postings are not strictly sorted"
            );
        }
        self.previous = Some(record);
        if let Some(next) = self.readers[run].next()? {
            self.heap.push(Reverse((next, run)));
        }
        Ok(Some(record))
    }

    fn read_bytes(&self) -> u64 {
        self.readers.iter().fold(0_u64, |total, reader| {
            total.saturating_add(reader.reader.get_ref().read_bytes)
        })
    }

    fn read_wall_time(&self) -> Duration {
        self.readers.iter().fold(Duration::ZERO, |total, reader| {
            total.saturating_add(reader.reader.get_ref().read_wall_time)
        })
    }
}

fn merge_run_group(inputs: Vec<PathBuf>, output: &Path) -> Result<()> {
    merge_run_group_instrumented(inputs, output).map(|_| ())
}

#[derive(Debug, Clone, Copy, Default)]
struct AdaptiveV3MergeTaskMetrics {
    input_runs: usize,
    records: u64,
    read_bytes: u64,
    write_bytes: u64,
    read_wall_time: Duration,
    write_wall_time: Duration,
    sync_wall_time: Duration,
    worker_wall_time: Duration,
}

fn merge_run_group_instrumented(
    inputs: Vec<PathBuf>,
    output: &Path,
) -> Result<AdaptiveV3MergeTaskMetrics> {
    let worker_started = Instant::now();
    let input_runs = inputs.len();
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(output)
        .with_context(|| format!("create merged account posting run {}", output.display()))?;
    let mut writer = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        TimedFileWriter {
            file,
            write_bytes: 0,
            write_wall_time: Duration::ZERO,
        },
    );
    let mut merger = RunMerger::open(inputs)?;
    let mut records = 0_u64;
    while let Some(record) = merger.next()? {
        writer.write_all(&record.encode())?;
        records = records
            .checked_add(1)
            .context("adaptive v3 merge record count overflow")?;
    }
    writer.flush()?;
    let sync_started = Instant::now();
    writer.get_ref().file.sync_all()?;
    let sync_wall_time = sync_started.elapsed();
    let write_bytes = records
        .checked_mul(SORT_RECORD_LEN as u64)
        .context("adaptive v3 merge output byte count overflow")?;
    ensure!(
        merger.read_bytes() == write_bytes,
        "adaptive v3 merge input and output byte counts differ"
    );
    ensure!(
        writer.get_ref().write_bytes == write_bytes,
        "adaptive v3 merge physical write-byte accounting differs"
    );
    Ok(AdaptiveV3MergeTaskMetrics {
        input_runs,
        records,
        read_bytes: merger.read_bytes(),
        write_bytes,
        read_wall_time: merger.read_wall_time(),
        write_wall_time: writer.get_ref().write_wall_time,
        sync_wall_time,
        worker_wall_time: worker_started.elapsed(),
    })
}

fn reduce_runs(mut paths: Vec<PathBuf>, directory: &Path) -> Result<(Vec<PathBuf>, u32)> {
    let mut pass = 0_u32;
    while paths.len() > MERGE_FAN_IN {
        pass = pass
            .checked_add(1)
            .context("account posting merge pass overflow")?;
        let mut next = Vec::new();
        for (group, chunk) in paths.chunks(MERGE_FAN_IN).enumerate() {
            let output = directory.join(format!("merge-{pass:04}-{group:08}.bin"));
            merge_run_group(chunk.to_vec(), &output)?;
            for path in chunk {
                fs::remove_file(path).with_context(|| {
                    format!("remove consumed account posting run {}", path.display())
                })?;
            }
            next.push(output);
        }
        paths = next;
    }
    Ok((paths, pass))
}

#[derive(Debug, Clone, Default)]
struct AdaptiveV3MergeSummary {
    passes: u32,
    peak_open_files_upper_bound: usize,
    peak_buffer_bytes_upper_bound: usize,
    peak_active_workers: usize,
    wall_time: Duration,
    worker_time: Duration,
    pass_phases: Vec<AdaptiveV3MergePassPhase>,
}

#[derive(Debug, Clone)]
struct AdaptiveV3MergeTask {
    inputs: Vec<PathBuf>,
    output: PathBuf,
}

fn reduce_runs_adaptive_v3(
    mut paths: Vec<PathBuf>,
    directory: &Path,
    workers: usize,
) -> Result<(Vec<PathBuf>, AdaptiveV3MergeSummary)> {
    ensure!(
        workers != 0 && workers <= ADAPTIVE_V3_MAX_MERGE_WORKERS,
        "adaptive v3 merge worker count is outside its bound"
    );
    let started = Instant::now();
    let mut summary = AdaptiveV3MergeSummary {
        peak_open_files_upper_bound: 2,
        ..AdaptiveV3MergeSummary::default()
    };
    while paths.len() > ADAPTIVE_V3_MERGE_FAN_IN {
        let pass_started = Instant::now();
        summary.passes = summary
            .passes
            .checked_add(1)
            .context("adaptive v3 merge pass overflow")?;
        let mut tasks = Vec::new();
        let mut next = Vec::new();
        for (group, chunk) in paths.chunks(ADAPTIVE_V3_MERGE_FAN_IN).enumerate() {
            let output = directory.join(format!("v3-merge-{:04}-{group:08}.bin", summary.passes));
            tasks.push(AdaptiveV3MergeTask {
                inputs: chunk.to_vec(),
                output: output.clone(),
            });
            next.push(output);
        }

        let pass_input_runs = paths.len();
        let pass_output_runs = tasks.len();
        let mut pass_metrics = AdaptiveV3MergeTaskMetrics::default();

        for batch in tasks.chunks(workers) {
            summary.peak_active_workers = summary.peak_active_workers.max(batch.len());
            let merge_files = batch.iter().try_fold(2_usize, |total, task| {
                total.checked_add(task.inputs.len().checked_add(1)?)
            });
            let merge_files = merge_files.context("adaptive v3 merge file bound overflow")?;
            ensure!(
                merge_files <= ADAPTIVE_V3_MAX_OPEN_FILES,
                "adaptive v3 merge needs {merge_files} files, above cap {ADAPTIVE_V3_MAX_OPEN_FILES}"
            );
            summary.peak_open_files_upper_bound =
                summary.peak_open_files_upper_bound.max(merge_files);
            let merge_buffers = batch.iter().try_fold(0_usize, |total, task| {
                task.inputs
                    .len()
                    .checked_add(1)
                    .and_then(|files| files.checked_mul(IO_BUFFER_BYTES))
                    .and_then(|bytes| total.checked_add(bytes))
            });
            summary.peak_buffer_bytes_upper_bound = summary
                .peak_buffer_bytes_upper_bound
                .max(merge_buffers.context("adaptive v3 merge buffer bound overflow")?);

            let results = thread::scope(|scope| {
                let handles = batch
                    .iter()
                    .cloned()
                    .map(|task| {
                        scope.spawn(move || merge_run_group_instrumented(task.inputs, &task.output))
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .map_err(|_| anyhow::anyhow!("adaptive v3 merge worker panicked"))
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            for result in results {
                let metrics = result?;
                pass_metrics.input_runs = pass_metrics
                    .input_runs
                    .checked_add(metrics.input_runs)
                    .context("adaptive v3 merge input-run count overflow")?;
                pass_metrics.records = pass_metrics
                    .records
                    .checked_add(metrics.records)
                    .context("adaptive v3 merge record count overflow")?;
                pass_metrics.read_bytes = pass_metrics
                    .read_bytes
                    .checked_add(metrics.read_bytes)
                    .context("adaptive v3 merge read-byte count overflow")?;
                pass_metrics.write_bytes = pass_metrics
                    .write_bytes
                    .checked_add(metrics.write_bytes)
                    .context("adaptive v3 merge write-byte count overflow")?;
                pass_metrics.read_wall_time = pass_metrics
                    .read_wall_time
                    .saturating_add(metrics.read_wall_time);
                pass_metrics.write_wall_time = pass_metrics
                    .write_wall_time
                    .saturating_add(metrics.write_wall_time);
                pass_metrics.sync_wall_time = pass_metrics
                    .sync_wall_time
                    .saturating_add(metrics.sync_wall_time);
                pass_metrics.worker_wall_time = pass_metrics
                    .worker_wall_time
                    .saturating_add(metrics.worker_wall_time);
                summary.worker_time = summary.worker_time.saturating_add(metrics.worker_wall_time);
            }
            for task in batch {
                for input in &task.inputs {
                    fs::remove_file(input).with_context(|| {
                        format!("remove consumed adaptive v3 run {}", input.display())
                    })?;
                }
            }
        }
        ensure!(
            pass_metrics.input_runs == pass_input_runs,
            "adaptive v3 merge pass input-run accounting differs"
        );
        let pass_wall = pass_started.elapsed();
        summary.pass_phases.push(AdaptiveV3MergePassPhase {
            pass: summary.passes,
            input_runs: u64::try_from(pass_input_runs)?,
            output_runs: u64::try_from(pass_output_runs)?,
            records: pass_metrics.records,
            read_bytes: pass_metrics.read_bytes,
            write_bytes: pass_metrics.write_bytes,
            read_worker_sum_ms: duration_millis_saturating(pass_metrics.read_wall_time),
            write_worker_sum_ms: duration_millis_saturating(pass_metrics.write_wall_time),
            sync_worker_sum_ms: duration_millis_saturating(pass_metrics.sync_wall_time),
            worker_wall_sum_ms: duration_millis_saturating(pass_metrics.worker_wall_time),
            pass_wall_ms: duration_millis_saturating(pass_wall),
            effective_read_mib_per_second: mib_rate(pass_metrics.read_bytes, pass_wall),
            effective_write_mib_per_second: mib_rate(pass_metrics.write_bytes, pass_wall),
            effective_total_io_mib_per_second: mib_rate(
                pass_metrics
                    .read_bytes
                    .saturating_add(pass_metrics.write_bytes),
                pass_wall,
            ),
        });
        paths = next;
    }
    summary.wall_time = started.elapsed();
    Ok((paths, summary))
}

#[derive(Debug, Clone, Copy)]
struct PageSummary {
    pages: u64,
    continuation_pages: u64,
    peak_page_postings: usize,
    decoded_page_bytes: u64,
    page_bytes: u64,
    directory_bytes: u64,
    file_bytes: u64,
    compression_time: Duration,
}

struct PageWriter {
    output: BufWriter<File>,
    directory: BufWriter<File>,
    directory_path: PathBuf,
    pending: Vec<postings::KeyPostings>,
    pending_postings: usize,
    next_offset: u64,
    pages: u64,
    continuation_pages: u64,
    peak_page_postings: usize,
    decoded_page_bytes: u64,
    page_bytes: u64,
    previous_entry: Option<postings::PageDirectoryEntry>,
    compression_time: Duration,
}

impl PageWriter {
    fn new(output: File, directory_path: &Path) -> Result<Self> {
        let directory = File::options()
            .write(true)
            .create_new(true)
            .open(directory_path)
            .with_context(|| {
                format!(
                    "create account posting directory spool {}",
                    directory_path.display()
                )
            })?;
        Ok(Self {
            output: BufWriter::with_capacity(8 << 20, output),
            directory: BufWriter::with_capacity(IO_BUFFER_BYTES, directory),
            directory_path: directory_path.to_path_buf(),
            pending: Vec::with_capacity(postings::MAX_KEYS_PER_PAGE as usize),
            pending_postings: 0,
            next_offset: HEADER_LEN as u64,
            pages: 0,
            continuation_pages: 0,
            peak_page_postings: 0,
            decoded_page_bytes: 0,
            page_bytes: 0,
            previous_entry: None,
            compression_time: Duration::ZERO,
        })
    }

    fn push_complete_key(&mut self, key: postings::KeyPostings) -> Result<()> {
        ensure!(!key.postings.is_empty(), "account key has no postings");
        ensure!(
            key.postings.len() <= postings::MAX_POSTINGS_PER_PAGE as usize,
            "complete account key exceeds page bound"
        );
        let exceeds_postings = self
            .pending_postings
            .checked_add(key.postings.len())
            .is_none_or(|value| value > postings::MAX_POSTINGS_PER_PAGE as usize);
        if self.pending.len() == postings::MAX_KEYS_PER_PAGE as usize || exceeds_postings {
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
        let flags = (u16::from(continued_from_previous)
            * postings::PAGE_FLAG_CONTINUED_FROM_PREVIOUS)
            | (u16::from(continues_in_next) * postings::PAGE_FLAG_CONTINUES_IN_NEXT);
        self.write_page(&[key], flags)?;
        self.continuation_pages = self
            .continuation_pages
            .checked_add(1)
            .context("continuation page count overflow")?;
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
            .context("page posting count overflow")?;
        ensure!(
            posting_count > 0 && posting_count <= postings::MAX_POSTINGS_PER_PAGE as usize,
            "account posting page exceeds posting bound"
        );
        let decoded = postings::encode_page(keys).context("encode schema-2 account page")?;
        let started = Instant::now();
        let mut encoder = zstd::Encoder::new(Vec::new(), ZSTD_LEVEL)
            .context("create account posting zstd encoder")?;
        encoder
            .include_checksum(true)
            .context("enable account posting zstd checksum")?;
        encoder
            .write_all(&decoded)
            .context("compress account posting page")?;
        let compressed = encoder.finish().context("finish account posting page")?;
        self.compression_time = self.compression_time.saturating_add(started.elapsed());
        let stored = if compressed.len() < decoded.len() {
            compressed.as_slice()
        } else {
            decoded.as_slice()
        };
        let entry = postings::PageDirectoryEntry {
            first_key: keys[0].key,
            last_key: keys[keys.len() - 1].key,
            offset: self.next_offset,
            stored_len: u32::try_from(stored.len()).context("stored account page exceeds u32")?,
            decoded_len: u32::try_from(decoded.len())
                .context("decoded account page exceeds u32")?,
            key_count: u32::try_from(keys.len()).context("account page key count exceeds u32")?,
            flags,
        };
        validate_next_entry(self.previous_entry, entry, self.pages)?;
        self.output.write_all(stored)?;
        self.directory.write_all(&entry.encode())?;
        self.next_offset = self
            .next_offset
            .checked_add(u64::from(entry.stored_len))
            .context("account page offset overflow")?;
        self.page_bytes = self
            .page_bytes
            .checked_add(u64::from(entry.stored_len))
            .context("account page byte count overflow")?;
        self.decoded_page_bytes = self
            .decoded_page_bytes
            .checked_add(u64::from(entry.decoded_len))
            .context("decoded account page byte count overflow")?;
        self.pages = self
            .pages
            .checked_add(1)
            .context("account page count overflow")?;
        self.peak_page_postings = self.peak_page_postings.max(posting_count);
        self.previous_entry = Some(entry);
        Ok(())
    }

    fn finish(mut self) -> Result<PageSummary> {
        self.flush_complete_page()?;
        ensure!(
            self.previous_entry
                .is_none_or(|entry| !entry.continues_in_next()),
            "account posting continuation chain is unfinished"
        );
        self.directory.flush()?;
        self.directory.get_ref().sync_all()?;
        let directory_bytes = self
            .pages
            .checked_mul(postings::DIRECTORY_ENTRY_LEN as u64)
            .context("account posting directory byte count overflow")?;
        ensure!(
            directory_bytes <= MAX_DIRECTORY_BYTES,
            "account posting directory exceeds guard"
        );
        ensure!(
            self.directory.get_ref().metadata()?.len() == directory_bytes,
            "account posting directory spool length differs"
        );
        let directory_offset = self.next_offset;
        let mut directory_reader = BufReader::with_capacity(
            IO_BUFFER_BYTES,
            File::open(&self.directory_path).context("open account posting directory spool")?,
        );
        let copied = std::io::copy(&mut directory_reader, &mut self.output)?;
        ensure!(
            copied == directory_bytes,
            "account posting directory copy is short"
        );
        let footer = postings::DirectoryFooter {
            directory_offset,
            page_count: self.pages,
        }
        .encode();
        self.output.write_all(&footer)?;
        self.output.flush()?;
        let output = self
            .output
            .into_inner()
            .context("finish account posting writer")?;
        let file_bytes = directory_offset
            .checked_add(directory_bytes)
            .and_then(|value| value.checked_add(postings::DIRECTORY_FOOTER_LEN as u64))
            .context("account posting file length overflow")?;
        ensure!(
            output.metadata()?.len() == file_bytes,
            "account posting file length differs"
        );
        output.sync_all()?;
        Ok(PageSummary {
            pages: self.pages,
            continuation_pages: self.continuation_pages,
            peak_page_postings: self.peak_page_postings,
            decoded_page_bytes: self.decoded_page_bytes,
            page_bytes: self.page_bytes,
            directory_bytes,
            file_bytes,
            compression_time: self.compression_time,
        })
    }
}

#[derive(Debug)]
struct AdaptiveV3CompressionJob {
    ordinal: u64,
    first_key: u32,
    last_key: u32,
    key_count: u32,
    flags: u16,
    posting_count: usize,
    decoded: Vec<u8>,
    stats: block_group_measurement::MeasurementStats,
    live_upper_bound: usize,
}

#[derive(Debug)]
struct AdaptiveV3CompressionResult {
    ordinal: u64,
    first_key: u32,
    last_key: u32,
    key_count: u32,
    flags: u16,
    posting_count: usize,
    decoded_len: usize,
    stored: Vec<u8>,
    zstd_frame_len: usize,
    compressed: bool,
    stats: block_group_measurement::MeasurementStats,
    elapsed: Duration,
    workspace_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct AdaptiveV3PageSummary {
    pages: u64,
    continuation_pages: u64,
    peak_page_postings: usize,
    current_decoded_page_bytes: u64,
    decoded_page_bytes: u64,
    zstd_frame_bytes: u64,
    page_bytes: u64,
    raw_selected_pages: u64,
    zstd_selected_pages: u64,
    directory_bytes: u64,
    file_bytes: u64,
    codec: block_group_measurement::CodecSelectionTotals,
    work_window: usize,
    peak_live_bytes_upper_bound: usize,
    peak_zstd_workspace_bytes: usize,
    peak_block_codec_scratch_bytes: usize,
    encode_time: Duration,
    queue_time: Duration,
    queue_flushes: u64,
    peak_queue_jobs: usize,
    compression_wall_time: Duration,
    compression_worker_time: Duration,
    write_time: Duration,
    write_bytes: u64,
    sync_time: Duration,
}

struct AdaptiveV3PageWriter {
    output: BufWriter<File>,
    directory: BufWriter<File>,
    directory_path: PathBuf,
    layout: block_group_measurement::ValidatedBlockLayout,
    page_workers: usize,
    work_window: usize,
    pending: Vec<postings::KeyPostings>,
    pending_postings: usize,
    compression_jobs: Vec<AdaptiveV3CompressionJob>,
    pending_live_upper_bound: usize,
    peak_live_upper_bound: usize,
    next_offset: u64,
    pages: u64,
    continuation_pages: u64,
    peak_page_postings: usize,
    current_decoded_page_bytes: u64,
    decoded_page_bytes: u64,
    zstd_frame_bytes: u64,
    page_bytes: u64,
    raw_selected_pages: u64,
    zstd_selected_pages: u64,
    previous_entry: Option<postings::PageDirectoryEntry>,
    codec: block_group_measurement::CodecSelectionTotals,
    peak_zstd_workspace_bytes: usize,
    peak_block_codec_scratch_bytes: usize,
    encode_time: Duration,
    queue_time: Duration,
    queue_flushes: u64,
    peak_queue_jobs: usize,
    compression_wall_time: Duration,
    compression_worker_time: Duration,
    write_time: Duration,
    write_bytes: u64,
    sync_time: Duration,
}

impl AdaptiveV3PageWriter {
    fn new(
        output: File,
        directory_path: &Path,
        layout: block_group_measurement::ValidatedBlockLayout,
        page_workers: usize,
    ) -> Result<Self> {
        ensure!(
            page_workers != 0 && page_workers <= ADAPTIVE_V3_MAX_PAGE_WORKERS,
            "adaptive v3 page worker count is outside its bound"
        );
        let work_window = page_workers
            .checked_mul(2)
            .context("adaptive v3 page work window overflow")?;
        let directory = File::options()
            .write(true)
            .create_new(true)
            .open(directory_path)
            .with_context(|| {
                format!(
                    "create adaptive v3 directory spool {}",
                    directory_path.display()
                )
            })?;
        Ok(Self {
            output: BufWriter::with_capacity(8 << 20, output),
            directory: BufWriter::with_capacity(IO_BUFFER_BYTES, directory),
            directory_path: directory_path.to_path_buf(),
            layout,
            page_workers,
            work_window,
            pending: Vec::with_capacity(postings::MAX_KEYS_PER_PAGE as usize),
            pending_postings: 0,
            compression_jobs: Vec::with_capacity(work_window),
            pending_live_upper_bound: 0,
            peak_live_upper_bound: 0,
            next_offset: HEADER_LEN as u64,
            pages: 0,
            continuation_pages: 0,
            peak_page_postings: 0,
            current_decoded_page_bytes: 0,
            decoded_page_bytes: 0,
            zstd_frame_bytes: 0,
            page_bytes: 0,
            raw_selected_pages: 0,
            zstd_selected_pages: 0,
            previous_entry: None,
            codec: block_group_measurement::CodecSelectionTotals::default(),
            peak_zstd_workspace_bytes: 0,
            peak_block_codec_scratch_bytes: 0,
            encode_time: Duration::ZERO,
            queue_time: Duration::ZERO,
            queue_flushes: 0,
            peak_queue_jobs: 0,
            compression_wall_time: Duration::ZERO,
            compression_worker_time: Duration::ZERO,
            write_time: Duration::ZERO,
            write_bytes: 0,
            sync_time: Duration::ZERO,
        })
    }

    fn push_complete_key(&mut self, key: postings::KeyPostings) -> Result<()> {
        ensure!(!key.postings.is_empty(), "adaptive v3 key has no postings");
        ensure!(
            key.postings.len() <= postings::MAX_POSTINGS_PER_PAGE as usize,
            "adaptive v3 complete key exceeds page bound"
        );
        let exceeds_postings = self
            .pending_postings
            .checked_add(key.postings.len())
            .is_none_or(|value| value > postings::MAX_POSTINGS_PER_PAGE as usize);
        if self.pending.len() == postings::MAX_KEYS_PER_PAGE as usize || exceeds_postings {
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
        let flags = (u16::from(continued_from_previous)
            * postings::PAGE_FLAG_CONTINUED_FROM_PREVIOUS)
            | (u16::from(continues_in_next) * postings::PAGE_FLAG_CONTINUES_IN_NEXT);
        self.queue_page(&[key], flags)?;
        self.continuation_pages = self
            .continuation_pages
            .checked_add(1)
            .context("adaptive v3 continuation page count overflow")?;
        Ok(())
    }

    fn flush_complete_page(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let keys = std::mem::take(&mut self.pending);
        self.pending_postings = 0;
        self.queue_page(&keys, 0)
    }

    fn queue_page(&mut self, keys: &[postings::KeyPostings], flags: u16) -> Result<()> {
        let posting_count = keys
            .iter()
            .try_fold(0_usize, |total, key| total.checked_add(key.postings.len()))
            .context("adaptive v3 page posting count overflow")?;
        ensure!(
            posting_count > 0 && posting_count <= postings::MAX_POSTINGS_PER_PAGE as usize,
            "adaptive v3 page exceeds posting bound"
        );
        let encode_started = Instant::now();
        let encoded = block_group_measurement::encode_page_with_layout(keys, &self.layout)
            .context("encode adaptive v3 page")?;
        self.encode_time = self.encode_time.saturating_add(encode_started.elapsed());
        let live_upper_bound = adaptive_v3_compression_live_upper_bound(encoded.bytes.len())?;
        ensure!(
            live_upper_bound <= ADAPTIVE_V3_COMPRESSION_LIVE_BUDGET,
            "adaptive v3 page compression exceeds live-byte budget"
        );
        let next_live = self
            .pending_live_upper_bound
            .checked_add(live_upper_bound)
            .context("adaptive v3 compression live-byte sum overflow")?;
        if !self.compression_jobs.is_empty()
            && (self.compression_jobs.len() == self.work_window
                || next_live > ADAPTIVE_V3_COMPRESSION_LIVE_BUDGET)
        {
            self.flush_compression_jobs()?;
        }
        let queue_started = Instant::now();
        self.pending_live_upper_bound = self
            .pending_live_upper_bound
            .checked_add(live_upper_bound)
            .context("adaptive v3 compression live-byte sum overflow")?;
        self.peak_live_upper_bound = self
            .peak_live_upper_bound
            .max(self.pending_live_upper_bound);
        self.compression_jobs.push(AdaptiveV3CompressionJob {
            ordinal: self
                .pages
                .checked_add(u64::try_from(self.compression_jobs.len())?)
                .context("adaptive v3 queued page ordinal overflow")?,
            first_key: keys[0].key,
            last_key: keys[keys.len() - 1].key,
            key_count: u32::try_from(keys.len())?,
            flags,
            posting_count,
            decoded: encoded.bytes,
            stats: encoded.stats,
            live_upper_bound,
        });
        self.peak_queue_jobs = self.peak_queue_jobs.max(self.compression_jobs.len());
        self.queue_time = self.queue_time.saturating_add(queue_started.elapsed());
        if self.compression_jobs.len() == self.work_window {
            self.flush_compression_jobs()?;
        }
        Ok(())
    }

    fn flush_compression_jobs(&mut self) -> Result<()> {
        if self.compression_jobs.is_empty() {
            return Ok(());
        }
        self.queue_flushes = self
            .queue_flushes
            .checked_add(1)
            .context("adaptive v3 compression queue flush count overflow")?;
        ensure!(
            self.compression_jobs
                .iter()
                .try_fold(0_usize, |total, job| total
                    .checked_add(job.live_upper_bound))
                == Some(self.pending_live_upper_bound),
            "adaptive v3 compression live-byte accounting differs"
        );
        let jobs = std::mem::take(&mut self.compression_jobs);
        self.pending_live_upper_bound = 0;
        let worker_count = self.page_workers.min(jobs.len());
        let mut lanes = (0..worker_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<AdaptiveV3CompressionJob>>>();
        for (index, job) in jobs.into_iter().enumerate() {
            lanes[index % worker_count].push(job);
        }
        let wall_started = Instant::now();
        let lane_results = thread::scope(|scope| {
            let handles = lanes
                .into_iter()
                .map(|lane| {
                    scope.spawn(move || {
                        lane.into_iter()
                            .map(compress_adaptive_v3_page)
                            .collect::<Result<Vec<_>>>()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .map_err(|_| anyhow::anyhow!("adaptive v3 compression worker panicked"))?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        self.compression_wall_time = self
            .compression_wall_time
            .saturating_add(wall_started.elapsed());
        let mut results = lane_results.into_iter().flatten().collect::<Vec<_>>();
        results.sort_by_key(|result| result.ordinal);
        for result in results {
            self.write_compressed_page(result)?;
        }
        Ok(())
    }

    fn write_compressed_page(&mut self, result: AdaptiveV3CompressionResult) -> Result<()> {
        ensure!(
            result.ordinal == self.pages,
            "adaptive v3 compression results are not ordered"
        );
        let entry = postings::PageDirectoryEntry {
            first_key: result.first_key,
            last_key: result.last_key,
            offset: self.next_offset,
            stored_len: u32::try_from(result.stored.len())?,
            decoded_len: u32::try_from(result.decoded_len)?,
            key_count: result.key_count,
            flags: result.flags,
        };
        validate_next_entry(self.previous_entry, entry, self.pages)?;
        let write_started = Instant::now();
        self.output.write_all(&result.stored)?;
        self.directory.write_all(&entry.encode())?;
        self.write_time = self.write_time.saturating_add(write_started.elapsed());
        self.write_bytes = self
            .write_bytes
            .checked_add(u64::try_from(result.stored.len())?)
            .and_then(|bytes| bytes.checked_add(postings::DIRECTORY_ENTRY_LEN as u64))
            .context("adaptive v3 physical page write-byte count overflow")?;
        self.next_offset = self
            .next_offset
            .checked_add(u64::from(entry.stored_len))
            .context("adaptive v3 page offset overflow")?;
        self.pages = self
            .pages
            .checked_add(1)
            .context("adaptive v3 page count overflow")?;
        self.peak_page_postings = self.peak_page_postings.max(result.posting_count);
        self.current_decoded_page_bytes = self
            .current_decoded_page_bytes
            .checked_add(u64::try_from(result.stats.current_page_bytes)?)
            .context("adaptive v3 current page byte count overflow")?;
        self.decoded_page_bytes = self
            .decoded_page_bytes
            .checked_add(u64::from(entry.decoded_len))
            .context("adaptive v3 decoded page byte count overflow")?;
        self.zstd_frame_bytes = self
            .zstd_frame_bytes
            .checked_add(u64::try_from(result.zstd_frame_len)?)
            .context("adaptive v3 zstd frame byte count overflow")?;
        self.page_bytes = self
            .page_bytes
            .checked_add(u64::from(entry.stored_len))
            .context("adaptive v3 stored page byte count overflow")?;
        self.raw_selected_pages = self
            .raw_selected_pages
            .checked_add(u64::from(!result.compressed))
            .context("adaptive v3 raw page count overflow")?;
        self.zstd_selected_pages = self
            .zstd_selected_pages
            .checked_add(u64::from(result.compressed))
            .context("adaptive v3 zstd page count overflow")?;
        self.codec.ordinal_key_fragments = self
            .codec
            .ordinal_key_fragments
            .checked_add(u64::from(result.stats.ordinal_keys))
            .context("adaptive v3 ordinal key count overflow")?;
        self.codec.block_group_key_fragments = self
            .codec
            .block_group_key_fragments
            .checked_add(u64::from(result.stats.block_group_keys))
            .context("adaptive v3 block-group key count overflow")?;
        self.codec.local_varint_groups = self
            .codec
            .local_varint_groups
            .checked_add(u64::from(result.stats.local_varint_groups))
            .context("adaptive v3 local varint count overflow")?;
        self.codec.local_bitpack_groups = self
            .codec
            .local_bitpack_groups
            .checked_add(u64::from(result.stats.local_bitpack_groups))
            .context("adaptive v3 local bitpack count overflow")?;
        self.codec.local_bitmap_groups = self
            .codec
            .local_bitmap_groups
            .checked_add(u64::from(result.stats.local_bitmap_groups))
            .context("adaptive v3 local bitmap count overflow")?;
        self.peak_zstd_workspace_bytes = self.peak_zstd_workspace_bytes.max(result.workspace_bytes);
        self.peak_block_codec_scratch_bytes = self
            .peak_block_codec_scratch_bytes
            .max(result.stats.peak_block_codec_scratch_bytes);
        self.compression_worker_time = self.compression_worker_time.saturating_add(result.elapsed);
        self.previous_entry = Some(entry);
        Ok(())
    }

    fn finish(mut self) -> Result<AdaptiveV3PageSummary> {
        self.flush_complete_page()?;
        self.flush_compression_jobs()?;
        ensure!(
            self.previous_entry
                .is_none_or(|entry| !entry.continues_in_next()),
            "adaptive v3 continuation chain is unfinished"
        );
        let directory_flush_started = Instant::now();
        self.directory.flush()?;
        self.write_time = self
            .write_time
            .saturating_add(directory_flush_started.elapsed());
        let directory_sync_started = Instant::now();
        self.directory.get_ref().sync_all()?;
        self.sync_time = self
            .sync_time
            .saturating_add(directory_sync_started.elapsed());
        let directory_bytes = self
            .pages
            .checked_mul(postings::DIRECTORY_ENTRY_LEN as u64)
            .context("adaptive v3 directory byte count overflow")?;
        ensure!(
            directory_bytes <= MAX_DIRECTORY_BYTES,
            "adaptive v3 directory exceeds guard"
        );
        ensure!(
            self.directory.get_ref().metadata()?.len() == directory_bytes,
            "adaptive v3 directory spool length differs"
        );
        let directory_offset = self.next_offset;
        let mut directory_reader = BufReader::with_capacity(
            IO_BUFFER_BYTES,
            File::open(&self.directory_path).context("open adaptive v3 directory spool")?,
        );
        let write_started = Instant::now();
        let copied = std::io::copy(&mut directory_reader, &mut self.output)?;
        ensure!(
            copied == directory_bytes,
            "adaptive v3 directory copy is short"
        );
        self.output.write_all(
            &postings::DirectoryFooter {
                directory_offset,
                page_count: self.pages,
            }
            .encode(),
        )?;
        self.output.flush()?;
        self.write_time = self.write_time.saturating_add(write_started.elapsed());
        self.write_bytes = self
            .write_bytes
            .checked_add(directory_bytes)
            .and_then(|bytes| bytes.checked_add(postings::DIRECTORY_FOOTER_LEN as u64))
            .context("adaptive v3 final page write-byte count overflow")?;
        let output = self
            .output
            .into_inner()
            .context("finish adaptive v3 page writer")?;
        let file_bytes = directory_offset
            .checked_add(directory_bytes)
            .and_then(|value| value.checked_add(postings::DIRECTORY_FOOTER_LEN as u64))
            .context("adaptive v3 file length overflow")?;
        ensure!(
            output.metadata()?.len() == file_bytes,
            "adaptive v3 file length differs"
        );
        let output_sync_started = Instant::now();
        output.sync_all()?;
        self.sync_time = self.sync_time.saturating_add(output_sync_started.elapsed());
        Ok(AdaptiveV3PageSummary {
            pages: self.pages,
            continuation_pages: self.continuation_pages,
            peak_page_postings: self.peak_page_postings,
            current_decoded_page_bytes: self.current_decoded_page_bytes,
            decoded_page_bytes: self.decoded_page_bytes,
            zstd_frame_bytes: self.zstd_frame_bytes,
            page_bytes: self.page_bytes,
            raw_selected_pages: self.raw_selected_pages,
            zstd_selected_pages: self.zstd_selected_pages,
            directory_bytes,
            file_bytes,
            codec: self.codec,
            work_window: self.work_window,
            peak_live_bytes_upper_bound: self.peak_live_upper_bound,
            peak_zstd_workspace_bytes: self.peak_zstd_workspace_bytes,
            peak_block_codec_scratch_bytes: self.peak_block_codec_scratch_bytes,
            encode_time: self.encode_time,
            queue_time: self.queue_time,
            queue_flushes: self.queue_flushes,
            peak_queue_jobs: self.peak_queue_jobs,
            compression_wall_time: self.compression_wall_time,
            compression_worker_time: self.compression_worker_time,
            write_time: self.write_time,
            write_bytes: self.write_bytes,
            sync_time: self.sync_time,
        })
    }
}

fn adaptive_v3_compression_live_upper_bound(decoded_len: usize) -> Result<usize> {
    decoded_len
        .checked_add(zstd::zstd_safe::compress_bound(decoded_len))
        .and_then(|bytes| {
            bytes.checked_add(block_group_measurement::MAX_ZSTD_COMPRESSION_WORKSPACE_BYTES)
        })
        .context("adaptive v3 compression live-byte bound overflow")
}

fn compress_adaptive_v3_page(job: AdaptiveV3CompressionJob) -> Result<AdaptiveV3CompressionResult> {
    let started = Instant::now();
    let mut compressor =
        zstd::bulk::Compressor::new(ZSTD_LEVEL).context("create adaptive v3 zstd compressor")?;
    compressor
        .include_checksum(true)
        .context("enable adaptive v3 zstd checksum")?;
    let frame = compressor
        .compress(&job.decoded)
        .context("compress adaptive v3 page")?;
    let workspace_bytes = compressor.context_mut().sizeof();
    ensure!(
        workspace_bytes <= block_group_measurement::MAX_ZSTD_COMPRESSION_WORKSPACE_BYTES,
        "adaptive v3 zstd workspace exceeds its cap"
    );
    ensure!(
        frame.len() <= zstd::zstd_safe::compress_bound(job.decoded.len()),
        "adaptive v3 zstd frame exceeds compression bound"
    );
    let zstd_frame_len = frame.len();
    let compressed = frame.len() < job.decoded.len();
    let decoded_len = job.decoded.len();
    let stored = if compressed { frame } else { job.decoded };
    Ok(AdaptiveV3CompressionResult {
        ordinal: job.ordinal,
        first_key: job.first_key,
        last_key: job.last_key,
        key_count: job.key_count,
        flags: job.flags,
        posting_count: job.posting_count,
        decoded_len,
        stored,
        zstd_frame_len,
        compressed,
        stats: job.stats,
        elapsed: started.elapsed(),
        workspace_bytes,
    })
}

fn validate_next_entry(
    previous: Option<postings::PageDirectoryEntry>,
    entry: postings::PageDirectoryEntry,
    index: u64,
) -> Result<()> {
    postings::PageDirectoryEntry::decode(&entry.encode()).context("validate account page entry")?;
    if let Some(previous) = previous {
        ensure!(
            previous.offset.checked_add(u64::from(previous.stored_len)) == Some(entry.offset),
            "account posting page offsets are not contiguous"
        );
        ensure!(
            previous.continues_in_next() == entry.continued_from_previous(),
            "account posting continuation chain is broken at page {index}"
        );
        if previous.continues_in_next() {
            ensure!(
                previous.first_key == previous.last_key
                    && entry.first_key == entry.last_key
                    && previous.first_key == entry.first_key,
                "account posting continuation changes key"
            );
        } else {
            ensure!(
                entry.first_key > previous.last_key,
                "account posting keys do not ascend"
            );
        }
    } else {
        ensure!(
            !entry.continued_from_previous(),
            "first account page is a continuation"
        );
        ensure!(
            entry.offset == HEADER_LEN as u64,
            "first account page has wrong offset"
        );
    }
    Ok(())
}

struct PostingFragment {
    postings: Vec<postings::Posting>,
}

impl PostingFragment {
    fn new() -> Self {
        Self {
            postings: Vec::with_capacity(postings::MAX_POSTINGS_PER_PAGE as usize),
        }
    }
}

fn write_merged_pages(pages: &mut PageWriter, paths: Vec<PathBuf>) -> Result<(u64, u64)> {
    if paths.is_empty() {
        return Ok((0, 0));
    }
    let mut merger = RunMerger::open(paths)?;
    let mut next = merger.next()?;
    let mut merged = 0_u64;
    let mut distinct_accounts = 0_u64;
    let mut fragment = PostingFragment::new();
    while let Some(first) = next.take() {
        let key = first.account_id;
        distinct_accounts = distinct_accounts
            .checked_add(1)
            .context("distinct account count overflow")?;
        let mut first_in_fragment = Some(first);
        let mut continued_from_previous = false;
        loop {
            if let Some(record) = first_in_fragment.take() {
                fragment.postings.push(postings::Posting {
                    transaction_ordinal: record.transaction_ordinal,
                    roles: record.roles,
                });
                merged = merged
                    .checked_add(1)
                    .context("merged posting count overflow")?;
            }
            while fragment.postings.len() < postings::MAX_POSTINGS_PER_PAGE as usize {
                next = merger.next()?;
                let Some(record) = next else {
                    break;
                };
                if record.account_id != key {
                    break;
                }
                fragment.postings.push(postings::Posting {
                    transaction_ordinal: record.transaction_ordinal,
                    roles: record.roles,
                });
                merged = merged
                    .checked_add(1)
                    .context("merged posting count overflow")?;
                next = None;
            }
            if fragment.postings.len() == postings::MAX_POSTINGS_PER_PAGE as usize && next.is_none()
            {
                next = merger.next()?;
            }
            let continues_in_next = next.is_some_and(|record| record.account_id == key);
            let entry = postings::KeyPostings {
                key,
                postings: std::mem::take(&mut fragment.postings),
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
    Ok((distinct_accounts, merged))
}

fn write_merged_adaptive_v3_pages(
    pages: &mut AdaptiveV3PageWriter,
    paths: Vec<PathBuf>,
) -> Result<(u64, u64, AdaptiveV3FinalMergePhase)> {
    if paths.is_empty() {
        return Ok((0, 0, AdaptiveV3FinalMergePhase::default()));
    }
    ensure!(
        paths.len() <= ADAPTIVE_V3_MERGE_FAN_IN,
        "adaptive v3 final merge exceeds fan-in"
    );
    let run_count = paths.len();
    let started = Instant::now();
    let encode_before = pages.encode_time;
    let queue_before = pages.queue_time;
    let compression_before = pages.compression_wall_time;
    let write_before = pages.write_time;
    let mut merger = RunMerger::open(paths)?;
    let mut next = merger.next()?;
    let mut merged = 0_u64;
    let mut distinct_accounts = 0_u64;
    let mut fragment = PostingFragment::new();
    while let Some(first) = next.take() {
        let key = first.account_id;
        distinct_accounts = distinct_accounts
            .checked_add(1)
            .context("adaptive v3 distinct account count overflow")?;
        let mut first_in_fragment = Some(first);
        let mut continued_from_previous = false;
        loop {
            if let Some(record) = first_in_fragment.take() {
                fragment.postings.push(postings::Posting {
                    transaction_ordinal: record.transaction_ordinal,
                    roles: record.roles,
                });
                merged = merged
                    .checked_add(1)
                    .context("adaptive v3 merged posting count overflow")?;
            }
            while fragment.postings.len() < postings::MAX_POSTINGS_PER_PAGE as usize {
                next = merger.next()?;
                let Some(record) = next else {
                    break;
                };
                if record.account_id != key {
                    break;
                }
                fragment.postings.push(postings::Posting {
                    transaction_ordinal: record.transaction_ordinal,
                    roles: record.roles,
                });
                merged = merged
                    .checked_add(1)
                    .context("adaptive v3 merged posting count overflow")?;
                next = None;
            }
            if fragment.postings.len() == postings::MAX_POSTINGS_PER_PAGE as usize && next.is_none()
            {
                next = merger.next()?;
            }
            let continues_in_next = next.is_some_and(|record| record.account_id == key);
            let key_postings = postings::KeyPostings {
                key,
                postings: std::mem::take(&mut fragment.postings),
            };
            if continued_from_previous || continues_in_next {
                pages.push_continuation(
                    key_postings,
                    continued_from_previous,
                    continues_in_next,
                )?;
            } else {
                pages.push_complete_key(key_postings)?;
            }
            if !continues_in_next {
                break;
            }
            first_in_fragment = next.take();
            continued_from_previous = true;
        }
    }
    let final_merge_wall = started.elapsed();
    let page_phase_wall = pages
        .encode_time
        .saturating_sub(encode_before)
        .saturating_add(pages.queue_time.saturating_sub(queue_before))
        .saturating_add(
            pages
                .compression_wall_time
                .saturating_sub(compression_before),
        )
        .saturating_add(pages.write_time.saturating_sub(write_before));
    let k_way_heap_and_coalescing_wall = final_merge_wall.saturating_sub(page_phase_wall);
    let record_coalescing_and_heap_wall =
        k_way_heap_and_coalescing_wall.saturating_sub(merger.read_wall_time());
    Ok((
        distinct_accounts,
        merged,
        AdaptiveV3FinalMergePhase {
            input_runs: u64::try_from(run_count)?,
            records: merged,
            read_bytes: merger.read_bytes(),
            read_wall_ms: duration_millis_saturating(merger.read_wall_time()),
            k_way_heap_and_coalescing_wall_ms: duration_millis_saturating(
                k_way_heap_and_coalescing_wall,
            ),
            record_coalescing_and_heap_wall_ms: duration_millis_saturating(
                record_coalescing_and_heap_wall,
            ),
            total_wall_ms: duration_millis_saturating(final_merge_wall),
            effective_read_mib_per_second: mib_rate(merger.read_bytes(), final_merge_wall),
        },
    ))
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct OutputSummary {
    pub status: &'static str,
    pub canary_kind: &'static str,
    pub format_status: &'static str,
    pub account_semantics: &'static str,
    pub coverage_semantics: &'static str,
    pub source_registry_binding: &'static str,
    pub canonical_forward_account_source: &'static str,
    pub forward_projection_disposition: &'static str,
    pub file_name: &'static str,
    pub control_file_name: &'static str,
    pub coverage_file_name: &'static str,
    pub report_file_name: &'static str,
    pub payload_schema: u16,
    pub zstd_level: i32,
    pub exact_source_ids_only: bool,
    pub program_roles_in_account_postings: bool,
    pub separate_program_postings_file: bool,
    pub blocks: u64,
    pub transactions: u64,
    pub registry_entries: u32,
    pub postings: u64,
    pub distinct_accounts: u64,
    pub raw_references: u64,
    pub coverage_records: u64,
    pub incomplete_account_transactions: u64,
    pub incomplete_cpi_transactions: u64,
    pub sort_memory_bytes: u64,
    pub initial_run_bytes: u64,
    pub peak_scratch_upper_bound_bytes: u64,
    pub max_open_index_files: u32,
    pub reader_max_coverage_cache_bytes: u64,
    pub sort_runs: u64,
    pub merge_passes: u32,
    pub pages: u64,
    pub continuation_pages: u64,
    pub peak_page_postings: usize,
    pub decoded_page_bytes: u64,
    pub stored_page_bytes: u64,
    pub page_bytes: u64,
    pub directory_bytes: u64,
    pub file_bytes: u64,
    pub control_file_bytes: u64,
    pub coverage_file_bytes: u64,
    pub total_index_bytes: u64,
    pub bytes_per_posting: f64,
    pub page_stored_to_decoded_ratio: f64,
    pub compression_time_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedPosting {
    pub block_id: u32,
    pub tx_index: u32,
    pub roles: u8,
    pub account_coverage: u8,
    pub cpi_coverage: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupResult {
    pub postings: Vec<ResolvedPosting>,
    pub incomplete_account_transactions: u64,
    pub incomplete_cpi_transactions: u64,
    /// False means an omitted account can be in an incomplete source transaction.
    pub absence_is_complete: bool,
    /// False means a returned role mask can omit an unavailable CPI-program role.
    pub cpi_role_bits_are_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitedLookupResult {
    /// At most the caller-supplied limit. A hot key is never fully materialized.
    pub postings: Vec<ResolvedPosting>,
    /// True when at least one additional posting follows the returned sample.
    pub has_more: bool,
    pub incomplete_account_transactions: u64,
    pub incomplete_cpi_transactions: u64,
    /// False means an omitted account can be in an incomplete source transaction.
    pub absence_is_complete: bool,
    /// False means a returned role mask can omit an unavailable CPI-program role.
    pub cpi_role_bits_are_complete: bool,
}

/// Exact read receipt for one streaming adaptive-V3 account lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PostingVisitSummary {
    pub postings: u64,
    pub pages_read: u64,
    pub read_calls: u64,
    pub stored_bytes: u64,
    pub decoded_bytes: u64,
    pub incomplete_account_transactions: u64,
    pub incomplete_cpi_transactions: u64,
    pub absence_is_complete: bool,
    pub cpi_role_bits_are_complete: bool,
}

/// One role-filtered block from the adaptive reverse index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleMatchedBlock {
    pub block_id: u32,
    pub matching_postings: u64,
}

/// Exact read and match work for one role-filtered block visit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RoleBlockVisitSummary {
    pub postings: u64,
    pub posting_blocks: u64,
    pub matching_postings: u64,
    pub matching_blocks: u64,
    pub pages_read: u64,
    pub read_calls: u64,
    pub stored_bytes: u64,
    pub decoded_bytes: u64,
    pub incomplete_account_transactions: u64,
    pub incomplete_cpi_transactions: u64,
    pub absence_is_complete: bool,
    pub cpi_role_bits_are_complete: bool,
}

/// Exact logical read work performed while the adaptive reader opened and
/// validated its control plane and pinned standalone ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AdaptiveOpenReadStats {
    pub read_calls: u64,
    pub stored_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedCoverage {
    pub block_id: u32,
    pub tx_index: u32,
    pub account_coverage: u8,
    pub cpi_coverage: u8,
}

pub struct Reader {
    file: File,
    header: Header,
    control: Control,
    directory: Vec<postings::PageDirectoryEntry>,
    coverage: Vec<CoverageRecord>,
    standalone: standalone_v2::Reader,
}

impl Reader {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let file = File::open(root.join(PAGES_FILE)).context("open standalone account postings")?;
        let mut header_bytes = [0_u8; HEADER_LEN];
        read_exact_at(&file, &mut header_bytes, 0, "account posting header")?;
        let header = Header::decode(&header_bytes)?;
        let control_file = File::open(root.join(CONTROL_FILE))
            .context("open standalone account posting control")?;
        ensure!(
            control_file.metadata()?.len() == CONTROL_LEN as u64,
            "account posting control file has wrong length"
        );
        let mut control_bytes = [0_u8; CONTROL_LEN];
        read_exact_at(
            &control_file,
            &mut control_bytes,
            0,
            "account posting control",
        )?;
        let control = Control::decode(&control_bytes)?;
        ensure!(
            control.header == header,
            "account posting control binding differs"
        );
        let standalone = standalone_v2::Reader::open(root)?;
        ensure!(
            header.binding.epoch == standalone.header.epoch
                && header.binding.slots_per_epoch == standalone.header.slots_per_epoch
                && header.binding.selected_blocks == standalone.header.selected_blocks
                && header.binding.selected_transactions == standalone.header.selected_transactions
                && header.binding.message_schema == standalone.header.message_schema
                && header.binding.metadata_schema == standalone.header.metadata_schema
                && header.binding.prefix == standalone.header.prefix,
            "account posting binding differs from standalone ledger"
        );
        let file_len = file.metadata()?.len();
        let footer_offset = file_len
            .checked_sub(postings::DIRECTORY_FOOTER_LEN as u64)
            .context("account posting file is shorter than its footer")?;
        ensure!(
            footer_offset >= HEADER_LEN as u64,
            "account posting footer overlaps header"
        );
        let mut footer_bytes = [0_u8; postings::DIRECTORY_FOOTER_LEN];
        read_exact_at(
            &file,
            &mut footer_bytes,
            footer_offset,
            "account posting footer",
        )?;
        let footer = postings::DirectoryFooter::decode(&footer_bytes)?;
        let directory_bytes = footer
            .page_count
            .checked_mul(postings::DIRECTORY_ENTRY_LEN as u64)
            .context("account posting directory length overflow")?;
        ensure!(
            directory_bytes <= MAX_DIRECTORY_BYTES,
            "account posting directory exceeds guard"
        );
        ensure!(
            footer.directory_offset >= HEADER_LEN as u64
                && footer.directory_offset.checked_add(directory_bytes) == Some(footer_offset),
            "account posting directory does not end at its footer"
        );
        let directory_len = usize::try_from(directory_bytes)
            .context("account posting directory does not fit memory")?;
        let mut directory_storage = vec![0_u8; directory_len];
        read_exact_at(
            &file,
            &mut directory_storage,
            footer.directory_offset,
            "account posting directory",
        )?;
        let directory = directory_storage
            .chunks_exact(postings::DIRECTORY_ENTRY_LEN)
            .map(postings::PageDirectoryEntry::decode)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        postings::validate_directory(&directory, HEADER_LEN as u64, footer.directory_offset)?;
        validate_control_counts(control, &directory)?;
        ensure!(
            directory
                .last()
                .is_none_or(|entry| entry.last_key <= header.binding.registry_entries),
            "account posting key exceeds source registry size"
        );
        let coverage = File::open(root.join(COVERAGE_FILE))
            .context("open standalone account posting coverage")?;
        let coverage_len = coverage.metadata()?.len();
        let coverage_payload_bytes = control
            .coverage_records
            .checked_mul(COVERAGE_RECORD_LEN as u64)
            .context("coverage byte count overflow")?;
        ensure!(
            coverage_payload_bytes <= MAX_COVERAGE_BYTES,
            "coverage cache exceeds reader guard"
        );
        let expected_coverage_len = (HEADER_LEN as u64)
            .checked_add(coverage_payload_bytes)
            .and_then(|value| value.checked_add(COVERAGE_FOOTER_LEN as u64))
            .context("coverage file length overflow")?;
        ensure!(
            coverage_len == expected_coverage_len,
            "coverage file length differs"
        );
        let mut coverage_header = [0_u8; HEADER_LEN];
        read_exact_at(&coverage, &mut coverage_header, 0, "coverage header")?;
        ensure!(
            Header::decode_with_magic(&coverage_header, COVERAGE_MAGIC)? == header,
            "coverage binding differs from account postings"
        );
        let footer_offset = coverage_len - COVERAGE_FOOTER_LEN as u64;
        let mut coverage_footer = [0_u8; COVERAGE_FOOTER_LEN];
        read_exact_at(
            &coverage,
            &mut coverage_footer,
            footer_offset,
            "coverage footer",
        )?;
        ensure!(
            coverage_footer[0..8] == COVERAGE_FOOTER_MAGIC,
            "coverage footer has wrong magic"
        );
        ensure!(
            u64::from_le_bytes(coverage_footer[8..16].try_into().unwrap())
                == control.coverage_records,
            "coverage footer count differs from control"
        );
        let coverage_storage_len = usize::try_from(coverage_payload_bytes)
            .context("coverage cache does not fit memory")?;
        let mut coverage_storage = vec![0_u8; coverage_storage_len];
        read_exact_at(
            &coverage,
            &mut coverage_storage,
            HEADER_LEN as u64,
            "coverage records",
        )?;
        let coverage = decode_coverage_records(
            &coverage_storage,
            control,
            header.binding.selected_transactions,
        )?;
        Ok(Self {
            file,
            header,
            control,
            directory,
            coverage,
            standalone,
        })
    }

    pub fn lookup(&self, account_id: u32) -> Result<LookupResult> {
        ensure!(account_id != 0, "account source ID zero is reserved");
        ensure!(
            account_id <= self.header.binding.registry_entries,
            "account source ID exceeds registry size"
        );
        let range = postings::pages_for_key(&self.directory, account_id);
        let mut result = Vec::new();
        let mut previous_ordinal = None;
        for entry in &self.directory[range] {
            let page = self.read_page(*entry)?;
            let Some(key) = postings::find_key(&page, account_id) else {
                continue;
            };
            for posting in &key.postings {
                if let Some(previous) = previous_ordinal {
                    ensure!(
                        posting.transaction_ordinal > previous,
                        "account postings do not ascend across pages"
                    );
                }
                previous_ordinal = Some(posting.transaction_ordinal);
                let (block_id, tx_index) =
                    resolve_transaction_ordinal(&self.standalone, posting.transaction_ordinal)?;
                let coverage = self
                    .coverage_for_ordinal(posting.transaction_ordinal)?
                    .unwrap_or(CoverageRecord {
                        transaction_ordinal: posting.transaction_ordinal,
                        account_coverage: 0,
                        cpi_coverage: 0,
                    });
                result.push(ResolvedPosting {
                    block_id,
                    tx_index,
                    roles: posting.roles,
                    account_coverage: coverage.account_coverage,
                    cpi_coverage: coverage.cpi_coverage,
                });
            }
        }
        Ok(LookupResult {
            postings: result,
            incomplete_account_transactions: self.control.incomplete_account_transactions,
            incomplete_cpi_transactions: self.control.incomplete_cpi_transactions,
            absence_is_complete: self.control.incomplete_account_transactions == 0,
            cpi_role_bits_are_complete: self.control.incomplete_cpi_transactions == 0,
        })
    }

    /// Visit each source transaction for which account presence or CPI role
    /// coverage is incomplete. Records are in transaction order.
    pub fn visit_incomplete_transactions(
        &self,
        mut visit: impl FnMut(ResolvedCoverage) -> Result<()>,
    ) -> Result<()> {
        for coverage in &self.coverage {
            let (block_id, tx_index) =
                resolve_transaction_ordinal(&self.standalone, coverage.transaction_ordinal)?;
            visit(ResolvedCoverage {
                block_id,
                tx_index,
                account_coverage: coverage.account_coverage,
                cpi_coverage: coverage.cpi_coverage,
            })?;
        }
        Ok(())
    }

    fn coverage_for_ordinal(&self, transaction_ordinal: u64) -> Result<Option<CoverageRecord>> {
        Ok(self
            .coverage
            .binary_search_by_key(&transaction_ordinal, |record| record.transaction_ordinal)
            .ok()
            .map(|index| self.coverage[index]))
    }

    fn read_page(&self, entry: postings::PageDirectoryEntry) -> Result<Vec<postings::KeyPostings>> {
        let mut stored = vec![0_u8; entry.stored_len as usize];
        read_exact_at(
            &self.file,
            &mut stored,
            entry.offset,
            "account posting page",
        )?;
        let decoded = if entry.is_compressed() {
            let frame_len =
                zstd::zstd_safe::find_frame_compressed_size(&stored).map_err(|code| {
                    anyhow::anyhow!(
                        "account posting page has an invalid zstd frame: {}",
                        zstd::zstd_safe::get_error_name(code)
                    )
                })?;
            ensure!(
                frame_len == stored.len(),
                "account posting zstd frame has trailing data"
            );
            let decoded = zstd::bulk::decompress(&stored, entry.decoded_len as usize)
                .context("decompress account posting page")?;
            ensure!(
                decoded.len() == entry.decoded_len as usize,
                "account posting page decoded length differs"
            );
            decoded
        } else {
            ensure!(
                entry.stored_len == entry.decoded_len,
                "raw account page lengths differ"
            );
            stored
        };
        let page = postings::decode_page(&decoded, entry.first_key, entry.key_count)?;
        ensure!(
            page.first().is_some_and(|key| key.key == entry.first_key)
                && page.last().is_some_and(|key| key.key == entry.last_key),
            "account posting page keys differ from directory"
        );
        Ok(page)
    }
}

fn validate_control_counts(
    control: Control,
    directory: &[postings::PageDirectoryEntry],
) -> Result<()> {
    if directory.is_empty() {
        ensure!(
            control.postings == 0 && control.distinct_accounts == 0,
            "empty account posting directory has nonzero counts"
        );
        return Ok(());
    }
    let minimum_postings = directory
        .iter()
        .try_fold(0_u64, |total, entry| {
            total.checked_add(u64::from(entry.key_count))
        })
        .context("minimum account posting count overflow")?;
    let maximum_postings = u64::try_from(directory.len())?
        .checked_mul(u64::from(postings::MAX_POSTINGS_PER_PAGE))
        .context("maximum account posting count overflow")?;
    ensure!(
        control.postings >= minimum_postings && control.postings <= maximum_postings,
        "account posting count is outside page bounds"
    );
    let distinct_accounts = directory
        .iter()
        .try_fold(0_u64, |total, entry| {
            let new_keys = u64::from(entry.key_count) - u64::from(entry.continued_from_previous());
            total.checked_add(new_keys)
        })
        .context("distinct account count overflow")?;
    ensure!(
        control.distinct_accounts == distinct_accounts,
        "distinct account count differs from directory"
    );
    Ok(())
}

fn decode_coverage_records(
    storage: &[u8],
    control: Control,
    selected_transactions: u64,
) -> Result<Vec<CoverageRecord>> {
    ensure!(
        storage.len() == usize::try_from(control.coverage_records)? * COVERAGE_RECORD_LEN,
        "coverage cache length differs from control"
    );
    let mut records = Vec::with_capacity(usize::try_from(control.coverage_records)?);
    let mut previous = None;
    let mut incomplete_account = 0_u64;
    let mut incomplete_cpi = 0_u64;
    for bytes in storage.chunks_exact(COVERAGE_RECORD_LEN) {
        let record = CoverageRecord::decode(bytes.try_into().expect("fixed coverage row"))?;
        ensure!(
            record.transaction_ordinal < selected_transactions,
            "coverage ordinal is outside standalone ledger"
        );
        if let Some(previous) = previous {
            ensure!(
                record.transaction_ordinal > previous,
                "coverage ordinals do not strictly ascend"
            );
        }
        previous = Some(record.transaction_ordinal);
        incomplete_account = incomplete_account
            .checked_add(u64::from(record.account_coverage != 0))
            .context("incomplete account count overflow")?;
        incomplete_cpi = incomplete_cpi
            .checked_add(u64::from(record.cpi_coverage != 0))
            .context("incomplete CPI count overflow")?;
        records.push(record);
    }
    ensure!(
        incomplete_account == control.incomplete_account_transactions
            && incomplete_cpi == control.incomplete_cpi_transactions,
        "coverage class counts differ from control"
    );
    Ok(records)
}

fn resolve_transaction_ordinal(
    standalone: &standalone_v2::Reader,
    transaction_ordinal: u64,
) -> Result<(u32, u32)> {
    ensure!(
        transaction_ordinal < standalone.header.selected_transactions,
        "account posting transaction ordinal is outside standalone ledger"
    );
    let mut low = 0_usize;
    let mut high = usize::try_from(standalone.header.selected_blocks)?;
    while low < high {
        let middle = low + (high - low) / 2;
        let row = standalone
            .block(middle)
            .context("standalone block row is missing")?;
        let end = row
            .first_tx_ordinal
            .checked_add(u64::from(row.tx_count))
            .context("standalone transaction range overflow")?;
        if transaction_ordinal < row.first_tx_ordinal {
            high = middle;
        } else if transaction_ordinal >= end {
            low = middle + 1;
        } else {
            let tx_index = u32::try_from(transaction_ordinal - row.first_tx_ordinal)?;
            ensure!(
                tx_index < row.tx_count,
                "resolved transaction index is outside block"
            );
            return Ok((row.block_id, tx_index));
        }
    }
    bail!("account posting transaction ordinal is not covered by a standalone block")
}

fn read_exact_at(file: &File, output: &mut [u8], offset: u64, label: &str) -> Result<()> {
    let mut read = 0_usize;
    while read < output.len() {
        let count = file
            .read_at(&mut output[read..], offset + read as u64)
            .with_context(|| format!("read {label}"))?;
        ensure!(count != 0, "short read for {label}");
        read += count;
    }
    Ok(())
}

fn adaptive_v3_source_objects() -> Vec<String> {
    [
        ADAPTIVE_V3_PAGES_FILE,
        ADAPTIVE_V3_CONTROL_FILE,
        ADAPTIVE_V3_COVERAGE_FILE,
        standalone_v2::INDEX_FILE,
    ]
    .into_iter()
    .map(str::to_owned)
    .chain(standalone_v2::Object::ALL.map(|object| object.file_name().to_owned()))
    .collect()
}

fn required_range_size(source: &dyn RangeSource, object: &str, label: &str) -> Result<u64> {
    source
        .size(object)
        .with_context(|| format!("read {label} size"))?
        .with_context(|| format!("{label} is missing"))
}

fn read_range_exact(
    source: &dyn RangeSource,
    object: &str,
    offset: u64,
    length: usize,
    label: &str,
) -> Result<Vec<u8>> {
    if length == 0 {
        return Ok(Vec::new());
    }
    let bytes = source
        .read_range(object, offset, length)
        .with_context(|| format!("read {label}"))?;
    ensure!(
        bytes.len() == length,
        "short read for {label}: got {}, expected {length}",
        bytes.len()
    );
    Ok(bytes)
}

/// Reader for the additive adaptive reverse-postings candidate.
pub struct AdaptiveV3Reader {
    source: Arc<dyn RangeSource>,
    header: AdaptiveV3Header,
    control: AdaptiveV3Control,
    directory: Vec<postings::PageDirectoryEntry>,
    coverage: Vec<CoverageRecord>,
    standalone: Arc<standalone_v2::Reader>,
    standalone_open_read_stats: standalone_v2::OpenReadStats,
    layout: block_group_measurement::ValidatedBlockLayout,
}

struct AdaptiveV3PageSession<'reader> {
    reader: &'reader AdaptiveV3Reader,
    stored: Vec<u8>,
    decoded: Vec<u8>,
    decompressor: zstd::bulk::Decompressor<'static>,
}

impl<'reader> AdaptiveV3PageSession<'reader> {
    fn new(reader: &'reader AdaptiveV3Reader) -> Result<Self> {
        let mut decompressor =
            zstd::bulk::Decompressor::new().context("create adaptive v3 zstd decoder")?;
        decompressor
            .set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(
                ADAPTIVE_V3_ZSTD_WINDOW_LOG_MAX,
            ))
            .context("set adaptive v3 zstd window limit")?;
        Ok(Self {
            reader,
            stored: Vec::new(),
            decoded: Vec::new(),
            decompressor,
        })
    }

    fn with_page<T>(
        &mut self,
        entry: postings::PageDirectoryEntry,
        visit: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<T> {
        self.reader
            .source
            .read_range_into(
                ADAPTIVE_V3_PAGES_FILE,
                entry.offset,
                entry.stored_len as usize,
                &mut self.stored,
            )
            .context("read adaptive v3 account posting page")?;
        ensure!(
            self.stored.len() == entry.stored_len as usize,
            "short read for adaptive v3 account posting page"
        );
        if entry.is_compressed() {
            let frame_len =
                zstd::zstd_safe::find_frame_compressed_size(&self.stored).map_err(|code| {
                    anyhow::anyhow!(
                        "adaptive v3 account page has an invalid zstd frame: {}",
                        zstd::zstd_safe::get_error_name(code)
                    )
                })?;
            ensure!(
                frame_len == self.stored.len(),
                "adaptive v3 account zstd frame has trailing data"
            );
            let expected = entry.decoded_len as usize;
            self.decoded.clear();
            if self.decoded.capacity() < expected {
                self.decoded
                    .try_reserve_exact(expected)
                    .context("reserve adaptive v3 decoded page")?;
            }
            let written = self
                .decompressor
                .decompress_to_buffer(&self.stored, &mut self.decoded)
                .context("decompress adaptive v3 account page")?;
            ensure!(
                written == expected && self.decoded.len() == expected,
                "adaptive v3 account page decoded length differs"
            );
            visit(&self.decoded)
        } else {
            ensure!(
                entry.stored_len == entry.decoded_len,
                "adaptive v3 raw account page lengths differ"
            );
            visit(&self.stored)
        }
    }

    fn visit_key(
        &mut self,
        entry: postings::PageDirectoryEntry,
        account_id: u32,
        mut visit: impl FnMut(block_group_measurement::ExactPosting) -> Result<()>,
    ) -> Result<block_group_measurement::TargetKeyVisitSummary> {
        let layout = &self.reader.layout;
        self.with_page(entry, |decoded| {
            let summary = block_group_measurement::visit_page_key_with_layout(
                decoded,
                entry.first_key,
                entry.key_count,
                layout,
                account_id,
                &mut visit,
            )?;
            validate_streamed_page_directory(entry, summary)?;
            Ok(summary)
        })
    }

    fn visit_key_blocks(
        &mut self,
        entry: postings::PageDirectoryEntry,
        account_id: u32,
        required_roles: u8,
        mut visit: impl FnMut(block_group_measurement::RoleMatchedBlock) -> Result<()>,
    ) -> Result<block_group_measurement::TargetKeyBlockVisitSummary> {
        let layout = &self.reader.layout;
        self.with_page(entry, |decoded| {
            let summary = block_group_measurement::visit_page_key_blocks_with_layout(
                decoded,
                entry.first_key,
                entry.key_count,
                layout,
                account_id,
                required_roles,
                &mut visit,
            )?;
            validate_streamed_page_directory(entry, summary.page)?;
            Ok(summary)
        })
    }
}

fn validate_streamed_page_directory(
    entry: postings::PageDirectoryEntry,
    summary: block_group_measurement::TargetKeyVisitSummary,
) -> Result<()> {
    ensure!(
        summary.first_key == entry.first_key && summary.last_key == entry.last_key,
        "adaptive v3 account page keys differ from directory"
    );
    Ok(())
}

impl AdaptiveV3Reader {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let allowed_objects = adaptive_v3_source_objects();
        let source = PinnedLocalRangeSource::new_anchored(
            &root,
            &allowed_objects
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        )
        .context("open pinned adaptive v3 source directory")?;
        Self::open_from_source(Arc::new(source), root, false, None)
    }

    /// Open the adaptive index and its standalone ledger through one source.
    pub fn open_with_source(
        source: Arc<dyn RangeSource>,
        source_label: impl Into<PathBuf>,
    ) -> Result<Self> {
        Self::open_from_source(source, source_label.into(), true, None)
    }

    /// Open only the adaptive objects and reuse an already validated
    /// standalone ledger. This is crate-private because the caller must prove
    /// that both readers use the same pinned source.
    pub(crate) fn open_with_shared_standalone(
        source: Arc<dyn RangeSource>,
        source_label: impl Into<PathBuf>,
        standalone: Arc<standalone_v2::Reader>,
    ) -> Result<Self> {
        Self::open_from_source(source, source_label.into(), true, Some(standalone))
    }

    fn open_from_source(
        source: Arc<dyn RangeSource>,
        root: PathBuf,
        use_bounded_index_ranges: bool,
        shared_standalone: Option<Arc<standalone_v2::Reader>>,
    ) -> Result<Self> {
        let file_len = required_range_size(
            source.as_ref(),
            ADAPTIVE_V3_PAGES_FILE,
            "adaptive v3 account postings",
        )?;
        let header_bytes = read_range_exact(
            source.as_ref(),
            ADAPTIVE_V3_PAGES_FILE,
            0,
            HEADER_LEN,
            "adaptive v3 account posting header",
        )?;
        let header = AdaptiveV3Header::decode(&header_bytes)?;
        let control_len = required_range_size(
            source.as_ref(),
            ADAPTIVE_V3_CONTROL_FILE,
            "adaptive v3 account posting control",
        )?;
        ensure!(
            control_len == CONTROL_LEN as u64,
            "adaptive v3 account posting control file has wrong length"
        );
        let control_bytes = read_range_exact(
            source.as_ref(),
            ADAPTIVE_V3_CONTROL_FILE,
            0,
            CONTROL_LEN,
            "adaptive v3 account posting control",
        )?;
        let control = AdaptiveV3Control::decode(&control_bytes)?;
        ensure!(
            control.header == header,
            "adaptive v3 account posting control binding differs"
        );
        let (standalone, standalone_open_read_stats) = if let Some(standalone) = shared_standalone {
            (standalone, standalone_v2::OpenReadStats::default())
        } else if use_bounded_index_ranges {
            let standalone = Arc::new(standalone_v2::Reader::open_with_source(
                source.clone(),
                root.clone(),
            )?);
            let stats = standalone.open_read_stats();
            (standalone, stats)
        } else {
            let standalone = Arc::new(standalone_v2::Reader::open_with_local_source(
                source.clone(),
                root.clone(),
            )?);
            let stats = standalone.open_read_stats();
            (standalone, stats)
        };
        ensure!(
            header.binding.epoch == standalone.header.epoch
                && header.binding.slots_per_epoch == standalone.header.slots_per_epoch
                && header.binding.selected_blocks == standalone.header.selected_blocks
                && header.binding.selected_transactions == standalone.header.selected_transactions
                && header.binding.message_schema == standalone.header.message_schema
                && header.binding.metadata_schema == standalone.header.metadata_schema
                && header.binding.prefix == standalone.header.prefix,
            "adaptive v3 account posting binding differs from standalone ledger"
        );

        let footer_offset = file_len
            .checked_sub(postings::DIRECTORY_FOOTER_LEN as u64)
            .context("adaptive v3 account posting file is shorter than its footer")?;
        ensure!(
            footer_offset >= HEADER_LEN as u64,
            "adaptive v3 account posting footer overlaps header"
        );
        let footer_bytes = read_range_exact(
            source.as_ref(),
            ADAPTIVE_V3_PAGES_FILE,
            footer_offset,
            postings::DIRECTORY_FOOTER_LEN,
            "adaptive v3 account posting footer",
        )?;
        let footer = postings::DirectoryFooter::decode(&footer_bytes)?;
        let directory_bytes = footer
            .page_count
            .checked_mul(postings::DIRECTORY_ENTRY_LEN as u64)
            .context("adaptive v3 account posting directory length overflow")?;
        ensure!(
            directory_bytes <= MAX_DIRECTORY_BYTES,
            "adaptive v3 account posting directory exceeds guard"
        );
        ensure!(
            footer.directory_offset >= HEADER_LEN as u64
                && footer.directory_offset.checked_add(directory_bytes) == Some(footer_offset),
            "adaptive v3 account posting directory does not end at its footer"
        );
        let directory_len = usize::try_from(directory_bytes)
            .context("adaptive v3 account posting directory does not fit memory")?;
        let directory_storage = read_range_exact(
            source.as_ref(),
            ADAPTIVE_V3_PAGES_FILE,
            footer.directory_offset,
            directory_len,
            "adaptive v3 account posting directory",
        )?;
        let directory = directory_storage
            .chunks_exact(postings::DIRECTORY_ENTRY_LEN)
            .map(postings::PageDirectoryEntry::decode)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        postings::validate_directory(&directory, HEADER_LEN as u64, footer.directory_offset)?;
        validate_adaptive_v3_control_counts(control, &directory)?;
        ensure!(
            directory
                .last()
                .is_none_or(|entry| entry.last_key <= header.binding.registry_entries),
            "adaptive v3 account posting key exceeds source registry size"
        );

        let coverage_len = required_range_size(
            source.as_ref(),
            ADAPTIVE_V3_COVERAGE_FILE,
            "adaptive v3 account posting coverage",
        )?;
        let coverage_payload_bytes = control
            .coverage_records
            .checked_mul(COVERAGE_RECORD_LEN as u64)
            .context("adaptive v3 coverage byte count overflow")?;
        ensure!(
            coverage_payload_bytes <= MAX_COVERAGE_BYTES,
            "adaptive v3 coverage cache exceeds reader guard"
        );
        let expected_coverage_len = (HEADER_LEN as u64)
            .checked_add(coverage_payload_bytes)
            .and_then(|value| value.checked_add(COVERAGE_FOOTER_LEN as u64))
            .context("adaptive v3 coverage file length overflow")?;
        ensure!(
            coverage_len == expected_coverage_len,
            "adaptive v3 coverage file length differs"
        );
        let coverage_header = read_range_exact(
            source.as_ref(),
            ADAPTIVE_V3_COVERAGE_FILE,
            0,
            HEADER_LEN,
            "adaptive v3 coverage header",
        )?;
        ensure!(
            AdaptiveV3Header::decode_with_magic(&coverage_header, ADAPTIVE_V3_COVERAGE_MAGIC,)?
                == header,
            "adaptive v3 coverage binding differs from account postings"
        );
        let coverage_footer_offset = coverage_len - COVERAGE_FOOTER_LEN as u64;
        let coverage_footer = read_range_exact(
            source.as_ref(),
            ADAPTIVE_V3_COVERAGE_FILE,
            coverage_footer_offset,
            COVERAGE_FOOTER_LEN,
            "adaptive v3 coverage footer",
        )?;
        ensure!(
            coverage_footer[0..8] == ADAPTIVE_V3_COVERAGE_FOOTER_MAGIC,
            "adaptive v3 coverage footer has wrong magic"
        );
        ensure!(
            u64::from_le_bytes(coverage_footer[8..16].try_into().unwrap())
                == control.coverage_records,
            "adaptive v3 coverage footer count differs from control"
        );
        let coverage_storage_len = usize::try_from(coverage_payload_bytes)
            .context("adaptive v3 coverage cache does not fit memory")?;
        let coverage_storage = read_range_exact(
            source.as_ref(),
            ADAPTIVE_V3_COVERAGE_FILE,
            HEADER_LEN as u64,
            coverage_storage_len,
            "adaptive v3 coverage records",
        )?;
        let coverage = decode_adaptive_v3_coverage_records(
            &coverage_storage,
            control,
            header.binding.selected_transactions,
        )?;
        let spans = read_adaptive_v3_block_spans(&standalone, header.binding.selected_blocks)?;
        let layout = block_group_measurement::ValidatedBlockLayout::new(spans)?;
        ensure!(
            layout.transactions() == header.binding.selected_transactions,
            "adaptive v3 block spans differ from transaction binding"
        );
        Ok(Self {
            source,
            header,
            control,
            directory,
            coverage,
            standalone,
            standalone_open_read_stats,
            layout,
        })
    }

    /// Return the epoch bound to this reader and its pinned standalone ledger.
    pub fn epoch(&self) -> u64 {
        self.header.binding.epoch
    }

    pub fn slots_per_epoch(&self) -> u64 {
        self.header.binding.slots_per_epoch
    }

    pub fn registry_entries(&self) -> u32 {
        self.header.binding.registry_entries
    }

    /// Return the format of the standalone ledger pinned by this reader.
    pub fn standalone_format(&self) -> standalone_v2::StandaloneFormat {
        self.standalone.header.format
    }

    /// Return the standalone block bound to this already-open adaptive reader.
    /// This lets a candidate query confirm postings without reopening the
    /// ledger through a second path.
    pub fn standalone_block(&self, ordinal: usize) -> Option<&standalone_v2::BlockRow> {
        self.standalone.block(ordinal)
    }

    pub fn standalone_selected_blocks(&self) -> u64 {
        self.standalone.header.selected_blocks
    }

    pub fn standalone_selected_transactions(&self) -> u64 {
        self.standalone.header.selected_transactions
    }

    pub fn standalone_message_schema(&self) -> CompactV2MessageSchema {
        self.standalone.message_schema()
    }

    pub fn standalone_metadata_schema(&self) -> CompactV2MetadataSchema {
        self.standalone.metadata_schema()
    }

    /// Confirm a sorted transaction subset against the standalone ledger
    /// pinned by this adaptive reader. No second open occurs.
    pub fn visit_standalone_semantic_transactions(
        &self,
        block_ordinal: usize,
        transaction_indexes: Option<&[u32]>,
        visit: impl FnMut(standalone_v2::SemanticTransaction<'_>) -> Result<()>,
    ) -> Result<standalone_v2::SemanticBlockReadStats> {
        self.standalone
            .visit_semantic_transactions(block_ordinal, transaction_indexes, visit)
    }

    pub fn open_read_stats(&self) -> AdaptiveOpenReadStats {
        let directory_bytes =
            (self.directory.len() as u64).saturating_mul(postings::DIRECTORY_ENTRY_LEN as u64);
        let coverage_bytes =
            (self.coverage.len() as u64).saturating_mul(COVERAGE_RECORD_LEN as u64);
        let standalone = self.standalone_open_read_stats;
        AdaptiveOpenReadStats {
            read_calls: standalone
                .read_calls
                .saturating_add(5)
                .saturating_add(u64::from(directory_bytes != 0))
                .saturating_add(u64::from(coverage_bytes != 0)),
            stored_bytes: standalone
                .stored_bytes
                .saturating_add(HEADER_LEN as u64)
                .saturating_add(CONTROL_LEN as u64)
                .saturating_add(postings::DIRECTORY_FOOTER_LEN as u64)
                .saturating_add(directory_bytes)
                .saturating_add(HEADER_LEN as u64)
                .saturating_add(COVERAGE_FOOTER_LEN as u64)
                .saturating_add(coverage_bytes),
        }
    }

    /// Stream every posting for one source-registry account without retaining
    /// the complete posting list.
    ///
    /// Postings are visited in strict `(block_id, tx_index)` order. The return
    /// value includes exact candidate page bytes read by this lookup. The
    /// visitor can run before validation reaches a later key or page. If this
    /// method returns an error, the caller must discard all callback results
    /// from the call.
    pub fn visit_account_postings(
        &self,
        account_id: u32,
        mut visit: impl FnMut(ResolvedPosting) -> Result<()>,
    ) -> Result<PostingVisitSummary> {
        ensure!(
            account_id != 0,
            "adaptive v3 account source ID zero is reserved"
        );
        ensure!(
            account_id <= self.header.binding.registry_entries,
            "adaptive v3 account source ID exceeds registry size"
        );
        let range = postings::pages_for_key(&self.directory, account_id);
        let mut summary = PostingVisitSummary {
            postings: 0,
            pages_read: 0,
            read_calls: 0,
            stored_bytes: 0,
            decoded_bytes: 0,
            incomplete_account_transactions: self.control.incomplete_account_transactions,
            incomplete_cpi_transactions: self.control.incomplete_cpi_transactions,
            absence_is_complete: self.control.incomplete_account_transactions == 0,
            cpi_role_bits_are_complete: self.control.incomplete_cpi_transactions == 0,
        };
        let mut previous = None;
        let mut pages = AdaptiveV3PageSession::new(self)?;
        for entry in &self.directory[range] {
            let postings_before_page = summary.postings;
            let page = pages.visit_key(*entry, account_id, |posting| {
                let position = (posting.block_id, posting.tx_index);
                if let Some(previous) = previous {
                    ensure!(
                        position > previous,
                        "adaptive v3 account postings do not ascend across pages"
                    );
                }
                previous = Some(position);
                let transaction_ordinal = adaptive_v3_exact_ordinal(self.layout.spans(), posting)?;
                let coverage =
                    self.coverage_for_ordinal(transaction_ordinal)
                        .unwrap_or(CoverageRecord {
                            transaction_ordinal,
                            account_coverage: 0,
                            cpi_coverage: 0,
                        });
                visit(ResolvedPosting {
                    block_id: posting.block_id,
                    tx_index: posting.tx_index,
                    roles: posting.roles,
                    account_coverage: coverage.account_coverage,
                    cpi_coverage: coverage.cpi_coverage,
                })?;
                summary.postings = summary
                    .postings
                    .checked_add(1)
                    .context("adaptive v3 posting count overflow")?;
                Ok(())
            })?;
            summary.pages_read = summary
                .pages_read
                .checked_add(1)
                .context("adaptive v3 visited-page count overflow")?;
            summary.read_calls = summary
                .read_calls
                .checked_add(1)
                .context("adaptive v3 read-call count overflow")?;
            summary.stored_bytes = summary
                .stored_bytes
                .checked_add(u64::from(entry.stored_len))
                .context("adaptive v3 stored-byte count overflow")?;
            summary.decoded_bytes = summary
                .decoded_bytes
                .checked_add(u64::from(entry.decoded_len))
                .context("adaptive v3 decoded-byte count overflow")?;
            ensure!(
                summary.postings - postings_before_page == page.postings,
                "adaptive v3 page posting count differs from visit"
            );
        }
        Ok(summary)
    }

    /// Stream one record per block that has a posting with a requested role.
    ///
    /// This candidate path validates every page and every posting, but it does
    /// not load sparse coverage or resolve coverage for each posting. A block
    /// split across continuation pages is emitted once. The visitor can run
    /// before validation reaches a later key or page. If this method returns
    /// an error, the caller must discard all callback results from the call.
    pub fn visit_account_role_blocks(
        &self,
        account_id: u32,
        required_roles: u8,
        mut visit: impl FnMut(RoleMatchedBlock) -> Result<()>,
    ) -> Result<RoleBlockVisitSummary> {
        ensure!(
            account_id != 0,
            "adaptive v3 account source ID zero is reserved"
        );
        ensure!(
            account_id <= self.header.binding.registry_entries,
            "adaptive v3 account source ID exceeds registry size"
        );
        ensure!(
            required_roles != 0 && required_roles & !postings::ROLE_MASK == 0,
            "adaptive v3 required role mask is invalid"
        );
        let range = postings::pages_for_key(&self.directory, account_id);
        let mut summary = RoleBlockVisitSummary {
            postings: 0,
            posting_blocks: 0,
            matching_postings: 0,
            matching_blocks: 0,
            pages_read: 0,
            read_calls: 0,
            stored_bytes: 0,
            decoded_bytes: 0,
            incomplete_account_transactions: self.control.incomplete_account_transactions,
            incomplete_cpi_transactions: self.control.incomplete_cpi_transactions,
            absence_is_complete: self.control.incomplete_account_transactions == 0,
            cpi_role_bits_are_complete: self.control.incomplete_cpi_transactions == 0,
        };
        let mut previous_posting: Option<(u32, u32)> = None;
        let mut pending: Option<block_group_measurement::RoleMatchedBlock> = None;
        let mut pages = AdaptiveV3PageSession::new(self)?;
        for entry in &self.directory[range] {
            let page = pages.visit_key_blocks(*entry, account_id, required_roles, |matched| {
                if let Some(current) = pending.as_mut()
                    && current.block_id == matched.block_id
                {
                    current.matching_postings = current
                        .matching_postings
                        .checked_add(matched.matching_postings)
                        .context("adaptive v3 block posting count overflow")?;
                    return Ok(());
                }
                if let Some(current) = pending.replace(matched) {
                    visit(RoleMatchedBlock {
                        block_id: current.block_id,
                        matching_postings: current.matching_postings,
                    })?;
                    summary.matching_blocks = summary
                        .matching_blocks
                        .checked_add(1)
                        .context("adaptive v3 matching block count overflow")?;
                }
                Ok(())
            })?;
            let continued_block = previous_posting
                .zip(page.page.first_posting)
                .is_some_and(|(previous, first)| previous.0 == first.0);
            if let Some(first) = page.page.first_posting
                && let Some(previous) = previous_posting
            {
                ensure!(
                    first > previous,
                    "adaptive v3 account postings do not ascend across pages"
                );
            }
            if page.page.last_posting.is_some() {
                previous_posting = page.page.last_posting;
            }
            summary.postings = summary
                .postings
                .checked_add(page.page.postings)
                .context("adaptive v3 posting count overflow")?;
            summary.posting_blocks = summary
                .posting_blocks
                .checked_add(page.page.blocks - u64::from(continued_block))
                .context("adaptive v3 posting-block count overflow")?;
            summary.matching_postings = summary
                .matching_postings
                .checked_add(page.matching_postings)
                .context("adaptive v3 matching posting count overflow")?;
            summary.pages_read = summary
                .pages_read
                .checked_add(1)
                .context("adaptive v3 visited-page count overflow")?;
            summary.read_calls = summary
                .read_calls
                .checked_add(1)
                .context("adaptive v3 read-call count overflow")?;
            summary.stored_bytes = summary
                .stored_bytes
                .checked_add(u64::from(entry.stored_len))
                .context("adaptive v3 stored-byte count overflow")?;
            summary.decoded_bytes = summary
                .decoded_bytes
                .checked_add(u64::from(entry.decoded_len))
                .context("adaptive v3 decoded-byte count overflow")?;
        }
        if let Some(current) = pending {
            visit(RoleMatchedBlock {
                block_id: current.block_id,
                matching_postings: current.matching_postings,
            })?;
            summary.matching_blocks = summary
                .matching_blocks
                .checked_add(1)
                .context("adaptive v3 matching block count overflow")?;
        }
        Ok(summary)
    }

    /// Read at most `limit` postings and stop after validating one extra match.
    ///
    /// Page decoding remains bounded by the frozen page codec guard. This API
    /// does not materialize all postings for a hot account.
    pub fn lookup_limited(&self, account_id: u32, limit: usize) -> Result<LimitedLookupResult> {
        ensure!(
            account_id != 0,
            "adaptive v3 account source ID zero is reserved"
        );
        ensure!(
            account_id <= self.header.binding.registry_entries,
            "adaptive v3 account source ID exceeds registry size"
        );
        ensure!(
            limit <= MAX_ADAPTIVE_V3_LOOKUP_SAMPLE_POSTINGS,
            "adaptive v3 account sample limit exceeds {MAX_ADAPTIVE_V3_LOOKUP_SAMPLE_POSTINGS}"
        );
        let range = postings::pages_for_key(&self.directory, account_id);
        let mut result = Vec::with_capacity(limit);
        let mut previous = None;
        let mut has_more = false;
        let mut pages = AdaptiveV3PageSession::new(self)?;
        'pages: for entry in &self.directory[range] {
            pages.visit_key(*entry, account_id, |posting| {
                let position = (posting.block_id, posting.tx_index);
                if let Some(previous) = previous {
                    ensure!(
                        position > previous,
                        "adaptive v3 account postings do not ascend across pages"
                    );
                }
                previous = Some(position);
                if result.len() == limit {
                    has_more = true;
                    return Ok(());
                }
                let transaction_ordinal = adaptive_v3_exact_ordinal(self.layout.spans(), posting)?;
                let coverage =
                    self.coverage_for_ordinal(transaction_ordinal)
                        .unwrap_or(CoverageRecord {
                            transaction_ordinal,
                            account_coverage: 0,
                            cpi_coverage: 0,
                        });
                result.push(ResolvedPosting {
                    block_id: posting.block_id,
                    tx_index: posting.tx_index,
                    roles: posting.roles,
                    account_coverage: coverage.account_coverage,
                    cpi_coverage: coverage.cpi_coverage,
                });
                Ok(())
            })?;
            if has_more {
                break 'pages;
            }
        }
        Ok(LimitedLookupResult {
            postings: result,
            has_more,
            incomplete_account_transactions: self.control.incomplete_account_transactions,
            incomplete_cpi_transactions: self.control.incomplete_cpi_transactions,
            absence_is_complete: self.control.incomplete_account_transactions == 0,
            cpi_role_bits_are_complete: self.control.incomplete_cpi_transactions == 0,
        })
    }

    pub fn lookup(&self, account_id: u32) -> Result<LookupResult> {
        ensure!(
            account_id != 0,
            "adaptive v3 account source ID zero is reserved"
        );
        ensure!(
            account_id <= self.header.binding.registry_entries,
            "adaptive v3 account source ID exceeds registry size"
        );
        let range = postings::pages_for_key(&self.directory, account_id);
        let mut result = Vec::new();
        let mut previous = None;
        let mut pages = AdaptiveV3PageSession::new(self)?;
        for entry in &self.directory[range] {
            pages.visit_key(*entry, account_id, |posting| {
                let position = (posting.block_id, posting.tx_index);
                if let Some(previous) = previous {
                    ensure!(
                        position > previous,
                        "adaptive v3 account postings do not ascend across pages"
                    );
                }
                previous = Some(position);
                let transaction_ordinal = adaptive_v3_exact_ordinal(self.layout.spans(), posting)?;
                let coverage =
                    self.coverage_for_ordinal(transaction_ordinal)
                        .unwrap_or(CoverageRecord {
                            transaction_ordinal,
                            account_coverage: 0,
                            cpi_coverage: 0,
                        });
                result.push(ResolvedPosting {
                    block_id: posting.block_id,
                    tx_index: posting.tx_index,
                    roles: posting.roles,
                    account_coverage: coverage.account_coverage,
                    cpi_coverage: coverage.cpi_coverage,
                });
                Ok(())
            })?;
        }
        Ok(LookupResult {
            postings: result,
            incomplete_account_transactions: self.control.incomplete_account_transactions,
            incomplete_cpi_transactions: self.control.incomplete_cpi_transactions,
            absence_is_complete: self.control.incomplete_account_transactions == 0,
            cpi_role_bits_are_complete: self.control.incomplete_cpi_transactions == 0,
        })
    }

    pub fn visit_incomplete_transactions(
        &self,
        mut visit: impl FnMut(ResolvedCoverage) -> Result<()>,
    ) -> Result<()> {
        let mut cursor = self.layout.sorted_ordinal_cursor();
        for coverage in &self.coverage {
            let posting = cursor.resolve(coverage.transaction_ordinal)?;
            visit(ResolvedCoverage {
                block_id: posting.block_id,
                tx_index: posting.tx_index,
                account_coverage: coverage.account_coverage,
                cpi_coverage: coverage.cpi_coverage,
            })?;
        }
        Ok(())
    }

    fn coverage_for_ordinal(&self, transaction_ordinal: u64) -> Option<CoverageRecord> {
        self.coverage
            .binary_search_by_key(&transaction_ordinal, |record| record.transaction_ordinal)
            .ok()
            .map(|index| self.coverage[index])
    }
}

fn read_adaptive_v3_block_spans(
    standalone: &standalone_v2::Reader,
    selected_blocks: u64,
) -> Result<Vec<block_group_measurement::BlockSpan>> {
    let block_count =
        usize::try_from(selected_blocks).context("adaptive v3 block count exceeds usize")?;
    let heap_bytes = block_count
        .checked_mul(mem::size_of::<block_group_measurement::BlockSpan>())
        .context("adaptive v3 block-span heap length overflow")?;
    ensure!(
        heap_bytes <= ADAPTIVE_V3_MAX_BLOCK_SPAN_HEAP_BYTES,
        "adaptive v3 block-span heap exceeds its cap"
    );
    let mut spans = Vec::new();
    spans
        .try_reserve_exact(block_count)
        .context("reserve adaptive v3 reader block spans")?;
    for ordinal in 0..block_count {
        let row = standalone
            .block(ordinal)
            .context("adaptive v3 standalone block row is missing")?;
        spans.push(block_group_measurement::BlockSpan {
            block_id: row.block_id,
            first_tx_ordinal: row.first_tx_ordinal,
            tx_count: row.tx_count,
        });
    }
    ensure!(
        spans
            .capacity()
            .checked_mul(mem::size_of::<block_group_measurement::BlockSpan>())
            .is_some_and(|bytes| bytes <= ADAPTIVE_V3_MAX_BLOCK_SPAN_HEAP_BYTES),
        "adaptive v3 reader block-span capacity exceeds its cap"
    );
    Ok(spans)
}

fn adaptive_v3_exact_ordinal(
    spans: &[block_group_measurement::BlockSpan],
    posting: block_group_measurement::ExactPosting,
) -> Result<u64> {
    let span = spans
        .binary_search_by_key(&posting.block_id, |span| span.block_id)
        .ok()
        .map(|index| spans[index])
        .context("adaptive v3 posting block is outside standalone layout")?;
    ensure!(
        posting.tx_index < span.tx_count,
        "adaptive v3 posting transaction index is outside block"
    );
    span.first_tx_ordinal
        .checked_add(u64::from(posting.tx_index))
        .context("adaptive v3 posting ordinal overflow")
}

fn validate_adaptive_v3_control_counts(
    control: AdaptiveV3Control,
    directory: &[postings::PageDirectoryEntry],
) -> Result<()> {
    if directory.is_empty() {
        ensure!(
            control.postings == 0 && control.distinct_accounts == 0,
            "empty adaptive v3 directory has nonzero counts"
        );
        return Ok(());
    }
    let minimum_postings = directory
        .iter()
        .try_fold(0_u64, |total, entry| {
            total.checked_add(u64::from(entry.key_count))
        })
        .context("adaptive v3 minimum posting count overflow")?;
    let maximum_postings = u64::try_from(directory.len())?
        .checked_mul(u64::from(postings::MAX_POSTINGS_PER_PAGE))
        .context("adaptive v3 maximum posting count overflow")?;
    ensure!(
        control.postings >= minimum_postings && control.postings <= maximum_postings,
        "adaptive v3 posting count is outside page bounds"
    );
    let distinct_accounts = directory
        .iter()
        .try_fold(0_u64, |total, entry| {
            let new_keys = u64::from(entry.key_count) - u64::from(entry.continued_from_previous());
            total.checked_add(new_keys)
        })
        .context("adaptive v3 distinct account count overflow")?;
    ensure!(
        control.distinct_accounts == distinct_accounts,
        "adaptive v3 distinct account count differs from directory"
    );
    Ok(())
}

fn decode_adaptive_v3_coverage_records(
    storage: &[u8],
    control: AdaptiveV3Control,
    selected_transactions: u64,
) -> Result<Vec<CoverageRecord>> {
    ensure!(
        storage.len() == usize::try_from(control.coverage_records)? * COVERAGE_RECORD_LEN,
        "adaptive v3 coverage cache length differs from control"
    );
    let mut records = Vec::new();
    records
        .try_reserve_exact(usize::try_from(control.coverage_records)?)
        .context("reserve adaptive v3 coverage records")?;
    let mut previous = None;
    let mut incomplete_account = 0_u64;
    let mut incomplete_cpi = 0_u64;
    for bytes in storage.chunks_exact(COVERAGE_RECORD_LEN) {
        let record = CoverageRecord::decode(bytes.try_into().expect("fixed coverage row"))?;
        ensure!(
            record.transaction_ordinal < selected_transactions,
            "adaptive v3 coverage ordinal is outside standalone ledger"
        );
        if let Some(previous) = previous {
            ensure!(
                record.transaction_ordinal > previous,
                "adaptive v3 coverage ordinals do not strictly ascend"
            );
        }
        previous = Some(record.transaction_ordinal);
        incomplete_account = incomplete_account
            .checked_add(u64::from(record.account_coverage != 0))
            .context("adaptive v3 incomplete account count overflow")?;
        incomplete_cpi = incomplete_cpi
            .checked_add(u64::from(record.cpi_coverage != 0))
            .context("adaptive v3 incomplete CPI count overflow")?;
        records.push(record);
    }
    ensure!(
        incomplete_account == control.incomplete_account_transactions
            && incomplete_cpi == control.incomplete_cpi_transactions,
        "adaptive v3 coverage class counts differ from control"
    );
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotBlockIndexRow};
    use blockzilla_read_sdk::{LocalRangeSource, SourceResult};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::tempdir;

    struct IndexReadCountingSource {
        inner: LocalRangeSource,
        index_reads: AtomicU64,
    }

    impl IndexReadCountingSource {
        fn new(root: &Path) -> Self {
            Self {
                inner: LocalRangeSource::new(root),
                index_reads: AtomicU64::new(0),
            }
        }

        fn index_reads(&self) -> u64 {
            self.index_reads.load(Ordering::Relaxed)
        }

        fn record(&self, object: &str) {
            if object == standalone_v2::INDEX_FILE {
                self.index_reads.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    impl RangeSource for IndexReadCountingSource {
        fn size(&self, object: &str) -> SourceResult<Option<u64>> {
            self.inner.size(object)
        }

        fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
            self.record(object);
            self.inner.read_range(object, offset, length)
        }

        fn read_range_into(
            &self,
            object: &str,
            offset: u64,
            length: usize,
            destination: &mut Vec<u8>,
        ) -> SourceResult<()> {
            self.record(object);
            self.inner
                .read_range_into(object, offset, length, destination)
        }

        fn read_range_into_slice(
            &self,
            object: &str,
            offset: u64,
            destination: &mut [u8],
        ) -> SourceResult<()> {
            self.record(object);
            self.inner
                .read_range_into_slice(object, offset, destination)
        }
    }

    fn standalone_binding() -> standalone_v2::Binding {
        standalone_v2::Binding {
            epoch: 9,
            slots_per_epoch: 432_000,
            selected_blocks: 2,
            selected_transactions: 3,
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
            prefix: true,
        }
    }

    fn binding() -> Binding {
        Binding {
            standalone: standalone_binding(),
            registry_entries: 10,
        }
    }

    fn write_standalone(root: &Path) {
        let plan = standalone_v2::CompressionPlan::default_level_three();
        let mut writers = standalone_v2::Writers::create(root, standalone_binding(), plan).unwrap();
        let mut scratch = standalone_v2::WorkerScratch::default();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        let mut first_tx = 0_u64;
        for (block_id, tx_count) in [(0_u32, 2_u32), (1, 1)] {
            scratch.begin_block();
            for tx_index in 0..tx_count {
                scratch
                    .begin_transaction(
                        ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                        0,
                        &[block_id as u8 + 1, tx_index as u8 + 1],
                    )
                    .unwrap();
                scratch.record_missing_metadata().unwrap();
            }
            scratch.record_block_rewards(&[0]).unwrap();
            scratch.finish_block(tx_count).unwrap();
            let projected =
                standalone_v2::encode_block(&mut scratch, &mut compressor, plan).unwrap();
            writers
                .append(
                    ArchiveV2HotBlockIndexRow {
                        block_id,
                        slot: 10 + u64::from(block_id),
                        compressed_offset: u64::from(block_id),
                        compressed_len: 1,
                        uncompressed_len: 1,
                        tx_count,
                        first_tx_ordinal: first_tx,
                        first_signature_ordinal: 0,
                        signature_count: 0,
                    },
                    standalone_v2::SourceBlockCore {
                        parent_slot: 9 + u64::from(block_id),
                        blockhash_id: 2 + block_id,
                        previous_blockhash_id: 1 + block_id,
                        block_time: None,
                        block_height: Some(100 + u64::from(block_id)),
                    },
                    projected,
                )
                .unwrap();
            first_tx += u64::from(tx_count);
        }
        writers.finish(2, 3).unwrap();
    }

    fn write_postings(root: &Path, sort_records: usize) -> OutputSummary {
        let mut builder =
            Builder::create(root, binding(), sort_records * mem::size_of::<SortRecord>()).unwrap();
        builder
            .append_block(
                0,
                0,
                2,
                vec![
                    SortRecord::new(8, 0, 0).unwrap(),
                    SortRecord::new(7, 0, postings::ROLE_SIGNER | postings::ROLE_WRITABLE).unwrap(),
                    SortRecord::new(7, 1, postings::ROLE_TOP_LEVEL_PROGRAM).unwrap(),
                ],
                vec![CoverageRecord::new(1, 1, 1).unwrap()],
            )
            .unwrap();
        builder
            .append_block(
                1,
                2,
                1,
                vec![
                    SortRecord::new(9, 2, postings::ROLE_WRITABLE).unwrap(),
                    SortRecord::new(7, 2, postings::ROLE_CPI_PROGRAM).unwrap(),
                ],
                Vec::new(),
            )
            .unwrap();
        builder.finish().unwrap()
    }

    fn write_adaptive_v3(
        root: &Path,
        sort_records: usize,
        options: AdaptiveV3Options,
    ) -> AdaptiveV3OutputSummary {
        let mut builder = AdaptiveV3Builder::create(
            root,
            binding(),
            sort_records * mem::size_of::<SortRecord>(),
            options,
        )
        .unwrap();
        builder
            .append_block(
                0,
                0,
                2,
                vec![
                    SortRecord::new(8, 0, 0).unwrap(),
                    SortRecord::new(7, 0, postings::ROLE_SIGNER | postings::ROLE_WRITABLE).unwrap(),
                    SortRecord::new(7, 1, postings::ROLE_TOP_LEVEL_PROGRAM).unwrap(),
                ],
                vec![CoverageRecord::new(1, 1, 1).unwrap()],
            )
            .unwrap();
        builder
            .append_block(
                1,
                2,
                1,
                vec![
                    SortRecord::new(9, 2, postings::ROLE_WRITABLE).unwrap(),
                    SortRecord::new(7, 2, postings::ROLE_CPI_PROGRAM).unwrap(),
                ],
                Vec::new(),
            )
            .unwrap();
        builder.finish().unwrap()
    }

    #[test]
    fn header_and_sparse_coverage_parsers_are_strict() {
        let header = Header::unfinished(binding());
        assert_eq!(Header::decode(&header.encode()).unwrap(), header);
        let mut corrupt = header.encode();
        corrupt[79] = 1;
        assert!(Header::decode(&corrupt).is_err());
        let record = CoverageRecord::new(7, 1, 4).unwrap();
        assert_eq!(CoverageRecord::decode(&record.encode()).unwrap(), record);
        let mut corrupt = record.encode();
        corrupt[15] = 1;
        assert!(CoverageRecord::decode(&corrupt).is_err());
        assert!(CoverageRecord::new(0, 0, 0).is_err());
        assert!(SortRecord::new(0, 0, 0).is_err());
        assert!(SortRecord::new(1, 0, 0x10).is_err());
    }

    #[test]
    fn exact_lookup_resolves_blocks_transactions_roles_and_coverage() {
        let directory = tempdir().unwrap();
        write_standalone(directory.path());
        let summary = write_postings(directory.path(), 2);
        assert_eq!(summary.postings, 5);
        assert_eq!(summary.distinct_accounts, 3);
        assert_eq!(summary.coverage_records, 1);
        assert_eq!(summary.raw_references, 0);
        assert!(summary.sort_runs > 1);

        let reader = Reader::open(directory.path()).unwrap();
        let found = reader.lookup(7).unwrap();
        assert_eq!(
            found.postings,
            vec![
                ResolvedPosting {
                    block_id: 0,
                    tx_index: 0,
                    roles: postings::ROLE_SIGNER | postings::ROLE_WRITABLE,
                    account_coverage: 0,
                    cpi_coverage: 0,
                },
                ResolvedPosting {
                    block_id: 0,
                    tx_index: 1,
                    roles: postings::ROLE_TOP_LEVEL_PROGRAM,
                    account_coverage: 1,
                    cpi_coverage: 1,
                },
                ResolvedPosting {
                    block_id: 1,
                    tx_index: 0,
                    roles: postings::ROLE_CPI_PROGRAM,
                    account_coverage: 0,
                    cpi_coverage: 0,
                },
            ]
        );
        assert!(!found.absence_is_complete);
        assert!(!found.cpi_role_bits_are_complete);
        let mut incomplete = Vec::new();
        reader
            .visit_incomplete_transactions(|coverage| {
                incomplete.push(coverage);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            incomplete,
            vec![ResolvedCoverage {
                block_id: 0,
                tx_index: 1,
                account_coverage: 1,
                cpi_coverage: 1,
            }]
        );
        assert!(reader.lookup(10).unwrap().postings.is_empty());
        assert!(reader.lookup(0).is_err());
        assert!(reader.lookup(11).is_err());
    }

    #[test]
    fn finalized_objects_do_not_depend_on_sort_run_layout() {
        let small = tempdir().unwrap();
        let large = tempdir().unwrap();
        write_standalone(small.path());
        write_standalone(large.path());
        write_postings(small.path(), 2);
        write_postings(large.path(), 64);
        for file in [PAGES_FILE, CONTROL_FILE, COVERAGE_FILE] {
            assert_eq!(
                fs::read(small.path().join(file)).unwrap(),
                fs::read(large.path().join(file)).unwrap(),
                "account posting object differs for {file}"
            );
        }
    }

    #[test]
    fn builder_uses_create_new_for_every_final_object() {
        let directory = tempdir().unwrap();
        let _first =
            Builder::create(directory.path(), binding(), DEFAULT_SORT_MEMORY_BYTES).unwrap();
        assert!(Builder::create(directory.path(), binding(), DEFAULT_SORT_MEMORY_BYTES).is_err());
    }

    #[test]
    fn adaptive_v3_uses_a_separate_two_gib_sort_memory_cap() {
        assert_eq!(DEFAULT_SORT_MEMORY_BYTES, 128 << 20);
        assert_eq!(ADAPTIVE_V3_OPTIMIZED_SORT_MEMORY_BYTES, 2 << 30);
        let directory = tempdir().unwrap();
        let error = AdaptiveV3Builder::create(
            directory.path(),
            binding(),
            ADAPTIVE_V3_OPTIMIZED_SORT_MEMORY_BYTES + 1,
            AdaptiveV3Options::default(),
        )
        .err()
        .expect("sort memory above the adaptive v3 cap must fail");
        assert!(error.to_string().contains("sort memory is outside"));
    }

    #[test]
    fn builder_rejects_coverage_above_reader_cache_guard() {
        let directory = tempdir().unwrap();
        let mut builder =
            Builder::create(directory.path(), binding(), DEFAULT_SORT_MEMORY_BYTES).unwrap();
        builder.coverage_count = MAX_COVERAGE_BYTES / COVERAGE_RECORD_LEN as u64;
        let error = builder
            .append_block(
                0,
                0,
                2,
                Vec::new(),
                vec![CoverageRecord::new(0, 1, 0).unwrap()],
            )
            .unwrap_err();
        assert!(error.to_string().contains("reader cache guard"));
    }

    #[test]
    fn adaptive_v3_lookup_preserves_exact_roles_and_sparse_coverage() {
        let directory = tempdir().unwrap();
        write_standalone(directory.path());
        let summary = write_adaptive_v3(
            directory.path(),
            2,
            AdaptiveV3Options {
                merge_workers: 2,
                page_workers: 2,
            },
        );
        assert_eq!(summary.postings, 5);
        assert_eq!(summary.distinct_accounts, 3);
        assert_eq!(summary.coverage_records, 1);
        assert_eq!(summary.payload_schema, ADAPTIVE_V3_PAYLOAD_SCHEMA);
        assert_eq!(summary.initial_run_count, summary.sort_runs);
        assert_eq!(summary.initial_run_records, summary.postings);
        assert_eq!(summary.sort_run_phases.len() as u64, summary.sort_runs);
        assert_eq!(
            summary
                .sort_run_phases
                .iter()
                .map(|run| run.bytes)
                .sum::<u64>(),
            summary.initial_run_bytes
        );
        assert_eq!(summary.final_merge.records, summary.postings);
        assert_eq!(
            summary.final_merge.read_bytes,
            summary.postings * SORT_RECORD_LEN as u64
        );
        assert!(summary.peak_zstd_queue_jobs <= summary.page_work_window);
        assert_eq!(
            summary.page_write_bytes,
            summary.stored_page_bytes
                + 2 * summary.directory_bytes
                + postings::DIRECTORY_FOOTER_LEN as u64
        );
        assert!(summary.block_span_heap_bytes <= summary.block_span_heap_cap_bytes);
        assert!(summary.peak_open_files_upper_bound <= ADAPTIVE_V3_MAX_OPEN_FILES);
        assert!(
            summary.peak_compression_live_bytes_upper_bound
                <= summary.compression_live_budget_bytes
        );

        let reader = AdaptiveV3Reader::open(directory.path()).unwrap();
        let found = reader.lookup(7).unwrap();
        assert_eq!(
            found.postings,
            vec![
                ResolvedPosting {
                    block_id: 0,
                    tx_index: 0,
                    roles: postings::ROLE_SIGNER | postings::ROLE_WRITABLE,
                    account_coverage: 0,
                    cpi_coverage: 0,
                },
                ResolvedPosting {
                    block_id: 0,
                    tx_index: 1,
                    roles: postings::ROLE_TOP_LEVEL_PROGRAM,
                    account_coverage: 1,
                    cpi_coverage: 1,
                },
                ResolvedPosting {
                    block_id: 1,
                    tx_index: 0,
                    roles: postings::ROLE_CPI_PROGRAM,
                    account_coverage: 0,
                    cpi_coverage: 0,
                },
            ]
        );
        assert!(!found.absence_is_complete);
        assert!(!found.cpi_role_bits_are_complete);
        let mut streamed = Vec::new();
        let streamed_summary = reader
            .visit_account_postings(7, |posting| {
                streamed.push(posting);
                Ok(())
            })
            .unwrap();
        assert_eq!(streamed, found.postings);
        assert_eq!(streamed_summary.postings, streamed.len() as u64);
        assert!(streamed_summary.pages_read > 0);
        assert_eq!(streamed_summary.read_calls, streamed_summary.pages_read);
        assert!(streamed_summary.stored_bytes > 0);
        assert!(streamed_summary.decoded_bytes > 0);
        assert!(!streamed_summary.absence_is_complete);
        assert!(!streamed_summary.cpi_role_bits_are_complete);
        let mut program_blocks = Vec::new();
        let program_summary = reader
            .visit_account_role_blocks(
                7,
                postings::ROLE_TOP_LEVEL_PROGRAM | postings::ROLE_CPI_PROGRAM,
                |block| {
                    program_blocks.push(block);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(program_summary.postings, 3);
        assert_eq!(program_summary.posting_blocks, 2);
        assert_eq!(program_summary.matching_postings, 2);
        assert_eq!(program_summary.matching_blocks, 2);
        assert_eq!(
            program_blocks,
            vec![
                RoleMatchedBlock {
                    block_id: 0,
                    matching_postings: 1,
                },
                RoleMatchedBlock {
                    block_id: 1,
                    matching_postings: 1,
                },
            ]
        );
        assert!(reader.visit_account_role_blocks(7, 0, |_| Ok(())).is_err());
        let open_stats = reader.open_read_stats();
        assert!(open_stats.read_calls > 0);
        assert!(open_stats.stored_bytes > 0);
        let mut incomplete = Vec::new();
        reader
            .visit_incomplete_transactions(|coverage| {
                incomplete.push(coverage);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            incomplete,
            vec![ResolvedCoverage {
                block_id: 0,
                tx_index: 1,
                account_coverage: 1,
                cpi_coverage: 1,
            }]
        );
        assert!(reader.lookup(10).unwrap().postings.is_empty());
    }

    #[test]
    fn shared_standalone_open_does_not_read_or_retain_a_second_block_index() {
        let directory = tempdir().unwrap();
        write_standalone(directory.path());
        write_adaptive_v3(
            directory.path(),
            2,
            AdaptiveV3Options {
                merge_workers: 1,
                page_workers: 1,
            },
        );
        let counted = Arc::new(IndexReadCountingSource::new(directory.path()));
        let source: Arc<dyn RangeSource> = counted.clone();
        let standalone = Arc::new(
            standalone_v2::Reader::open_with_source(source.clone(), "shared-test").unwrap(),
        );
        let index_reads_after_standalone = counted.index_reads();
        assert!(index_reads_after_standalone > 0);

        let adaptive = AdaptiveV3Reader::open_with_shared_standalone(
            source,
            "shared-test",
            Arc::clone(&standalone),
        )
        .unwrap();
        assert_eq!(counted.index_reads(), index_reads_after_standalone);
        assert!(Arc::ptr_eq(&adaptive.standalone, &standalone));
        assert_eq!(
            adaptive.standalone_open_read_stats,
            standalone_v2::OpenReadStats::default()
        );
    }

    #[test]
    fn adaptive_v3_magics_and_page_codec_corruption_are_rejected() {
        assert!(AdaptiveV3Header::decode(&Header::unfinished(binding()).encode()).is_err());
        assert!(Header::decode(&AdaptiveV3Header::unfinished(binding()).encode()).is_err());

        let directory = tempdir().unwrap();
        write_standalone(directory.path());
        write_adaptive_v3(
            directory.path(),
            16,
            AdaptiveV3Options {
                merge_workers: 1,
                page_workers: 1,
            },
        );
        let reader = AdaptiveV3Reader::open(directory.path()).unwrap();
        let first = reader.directory[0];
        assert_eq!(first.stored_len, first.decoded_len);
        drop(reader);
        let page_path = directory.path().join(ADAPTIVE_V3_PAGES_FILE);
        let page_file = File::options().write(true).open(&page_path).unwrap();
        page_file.write_all_at(&[0xff], first.offset + 2).unwrap();
        let reader = AdaptiveV3Reader::open(directory.path()).unwrap();
        assert!(reader.lookup(first.first_key).is_err());

        let coverage_path = directory.path().join(ADAPTIVE_V3_COVERAGE_FILE);
        let coverage = File::options().write(true).open(coverage_path).unwrap();
        coverage.write_all_at(b"BADMAGIC", 0).unwrap();
        assert!(AdaptiveV3Reader::open(directory.path()).is_err());
    }

    #[test]
    fn adaptive_v3_hot_key_uses_a_bounded_continuation_chain() {
        let directory = tempdir().unwrap();
        let tx_count = postings::MAX_POSTINGS_PER_PAGE + 1;
        let binding = Binding {
            standalone: standalone_v2::Binding {
                epoch: 4,
                slots_per_epoch: 432_000,
                selected_blocks: 1,
                selected_transactions: u64::from(tx_count),
                message_schema: CompactV2MessageSchema::Current,
                metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
                prefix: true,
            },
            registry_entries: 3,
        };
        let plan = standalone_v2::CompressionPlan::default_level_three();
        let mut scratch = standalone_v2::WorkerScratch::default();
        scratch.begin_block();
        for _ in 0..tx_count {
            scratch
                .begin_transaction(ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, 0, &[1])
                .unwrap();
            scratch.record_missing_metadata().unwrap();
        }
        scratch.record_block_rewards(&[0]).unwrap();
        scratch.finish_block(tx_count).unwrap();
        let mut compressor = zstd::bulk::Compressor::new(plan.zstd_level).unwrap();
        let block = standalone_v2::encode_block(&mut scratch, &mut compressor, plan).unwrap();
        let mut writers =
            standalone_v2::Writers::create(directory.path(), binding.standalone, plan).unwrap();
        writers
            .append(
                ArchiveV2HotBlockIndexRow {
                    block_id: 0,
                    slot: 10,
                    compressed_offset: 0,
                    compressed_len: 1,
                    uncompressed_len: 1,
                    tx_count,
                    first_tx_ordinal: 0,
                    first_signature_ordinal: 0,
                    signature_count: 0,
                },
                standalone_v2::SourceBlockCore {
                    parent_slot: 9,
                    blockhash_id: 2,
                    previous_blockhash_id: 1,
                    block_time: None,
                    block_height: None,
                },
                block,
            )
            .unwrap();
        writers.finish(1, u64::from(tx_count)).unwrap();

        let mut records = (0..tx_count)
            .map(|tx_index| {
                SortRecord::new(
                    1,
                    u64::from(tx_index),
                    (tx_index as u8) & postings::ROLE_MASK,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        records.push(SortRecord::new(2, 0, postings::ROLE_SIGNER).unwrap());
        let sort_memory = records
            .len()
            .checked_mul(mem::size_of::<SortRecord>())
            .unwrap();
        let mut builder = AdaptiveV3Builder::create(
            directory.path(),
            binding,
            sort_memory,
            AdaptiveV3Options {
                merge_workers: 1,
                page_workers: 2,
            },
        )
        .unwrap();
        builder
            .append_block(0, 0, tx_count, records, Vec::new())
            .unwrap();
        let summary = builder.finish().unwrap();
        assert_eq!(summary.postings, u64::from(tx_count) + 1);
        assert_eq!(summary.distinct_accounts, 2);
        assert_eq!(summary.pages, 3);
        assert_eq!(summary.continuation_pages, 2);
        assert_eq!(summary.peak_page_postings, 65_536);
        assert!(summary.block_group_key_fragments >= 1);
        assert!(summary.local_bitmap_groups >= 1);

        let reader = AdaptiveV3Reader::open(directory.path()).unwrap();
        let exact = reader.lookup_limited(2, 1).unwrap();
        assert_eq!(exact.postings.len(), 1);
        assert!(!exact.has_more);
        let missing = reader.lookup_limited(3, 1).unwrap();
        assert!(missing.postings.is_empty());
        assert!(!missing.has_more);
        assert!(
            reader
                .lookup_limited(1, MAX_ADAPTIVE_V3_LOOKUP_SAMPLE_POSTINGS + 1)
                .is_err()
        );
        let mut signer_blocks = Vec::new();
        let signer_summary = reader
            .visit_account_role_blocks(1, postings::ROLE_SIGNER, |block| {
                signer_blocks.push(block);
                Ok(())
            })
            .unwrap();
        assert_eq!(signer_summary.postings, u64::from(tx_count));
        assert_eq!(signer_summary.posting_blocks, 1);
        assert_eq!(signer_summary.matching_blocks, 1);
        assert_eq!(
            signer_blocks,
            vec![RoleMatchedBlock {
                block_id: 0,
                matching_postings: u64::from(tx_count / 2),
            }]
        );

        let second_page = reader.directory[1];
        let page_path = directory.path().join(ADAPTIVE_V3_PAGES_FILE);
        let page_file = File::options().write(true).open(page_path).unwrap();
        page_file.write_all_at(&[0xff], second_page.offset).unwrap();
        let count_only = reader.lookup_limited(1, 0).unwrap();
        assert!(count_only.postings.is_empty());
        assert!(count_only.has_more);
        let limited = reader
            .lookup_limited(1, MAX_ADAPTIVE_V3_LOOKUP_SAMPLE_POSTINGS)
            .unwrap();
        assert_eq!(
            limited.postings.len(),
            MAX_ADAPTIVE_V3_LOOKUP_SAMPLE_POSTINGS
        );
        assert!(limited.has_more);
        assert_eq!(limited.postings[0].tx_index, 0);
        assert_eq!(
            limited.postings.last().unwrap().tx_index,
            MAX_ADAPTIVE_V3_LOOKUP_SAMPLE_POSTINGS as u32 - 1
        );
        assert!(reader.lookup(1).is_err());
    }

    #[test]
    fn adaptive_v3_bytes_ignore_merge_and_page_worker_counts() {
        const KEYS: u32 = 70_000;
        const SORT_RECORDS: usize = 512;
        let first = tempdir().unwrap();
        let second = tempdir().unwrap();
        let binding = Binding {
            standalone: standalone_v2::Binding {
                epoch: 7,
                slots_per_epoch: 432_000,
                selected_blocks: 1,
                selected_transactions: 1,
                message_schema: CompactV2MessageSchema::Current,
                metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
                prefix: true,
            },
            registry_entries: KEYS,
        };
        let build = |root: &Path, options| {
            let mut builder = AdaptiveV3Builder::create(
                root,
                binding,
                SORT_RECORDS * mem::size_of::<SortRecord>(),
                options,
            )
            .unwrap();
            let records = (1..=KEYS)
                .rev()
                .map(|key| SortRecord::new(key, 0, (key as u8) & postings::ROLE_MASK).unwrap())
                .collect();
            builder
                .append_block(
                    0,
                    0,
                    1,
                    records,
                    vec![CoverageRecord::new(0, 1, 2).unwrap()],
                )
                .unwrap();
            builder.finish().unwrap()
        };
        let serial = build(
            first.path(),
            AdaptiveV3Options {
                merge_workers: 1,
                page_workers: 1,
            },
        );
        let parallel = build(
            second.path(),
            AdaptiveV3Options {
                merge_workers: 2,
                page_workers: 12,
            },
        );
        assert!(serial.sort_runs > ADAPTIVE_V3_MERGE_FAN_IN as u64);
        assert_eq!(serial.merge_passes, 1);
        assert_eq!(parallel.merge_passes, 1);
        assert_eq!(serial.merge_pass_phases.len(), 1);
        assert_eq!(parallel.merge_pass_phases.len(), 1);
        for summary in [&serial, &parallel] {
            let pass = summary.merge_pass_phases[0];
            assert_eq!(pass.read_bytes, pass.write_bytes);
            assert_eq!(pass.records * SORT_RECORD_LEN as u64, pass.read_bytes);
            assert!(pass.effective_total_io_mib_per_second.is_finite());
            assert_eq!(summary.final_merge.records, summary.postings);
            assert_eq!(
                summary.final_merge.read_bytes,
                summary.postings * SORT_RECORD_LEN as u64
            );
        }
        assert_eq!(parallel.peak_open_files_upper_bound, 130);
        assert_eq!(
            parallel.peak_merge_buffer_bytes_upper_bound,
            (128 * IO_BUFFER_BYTES) as u64
        );
        assert!(parallel.pages >= 12);
        for file in [
            ADAPTIVE_V3_PAGES_FILE,
            ADAPTIVE_V3_CONTROL_FILE,
            ADAPTIVE_V3_COVERAGE_FILE,
        ] {
            assert_eq!(
                fs::read(first.path().join(file)).unwrap(),
                fs::read(second.path().join(file)).unwrap(),
                "adaptive v3 object differs for {file}"
            );
        }
    }
}
