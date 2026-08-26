//! Read-only Archive V2 PoH and blockhash-chain verification.
//!
//! The verifier recomputes PoH entry hashes from the exact transaction
//! signature partition in the hot block rows. It does not verify Ed25519
//! signatures and it does not compute archive seal hashes.

use std::{
    fs::File,
    io::{BufReader, Read},
    ops::Range,
    os::unix::fs::FileExt,
    time::{Duration, Instant},
};

use blockzilla_format::{
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_SIGNATURES_FILE, ArchiveV2HotBlockIndexRow, CompactPohEntry,
    MAX_REPLAY_NUM_HASHES_PER_ENTRY, ReplaySignatureMixinBuilder, WincodeArchiveV2PohRecord,
};
use blockzilla_index_archive_format::sidecars::poh::{
    DecodedPohFrame, PohWireProfile, decode_payload as decode_retained_poh_payload,
};
use rayon::prelude::*;
use serde::Serialize;
use sha2::{Digest, Sha256, block_api::compress256};
use thiserror::Error;

use crate::{
    ArchiveReader, BorrowedDecodedBlock, Error as ReaderError, OrderedParallelBlockConfig,
    OrderedParallelBlockStats, PinnedLocalRangeSource, RangeSource, SourceError,
};

const SIGNATURE_BYTES: usize = 64;
const HASH_BYTES: usize = 32;
const TAIL_RECORDS: usize = 300;
const TAIL_RECORD_BYTES: usize = 40;
const MAX_POH_FRAME_BYTES: usize = 64 << 20;
const POH_READER_BUFFER_BYTES: usize = 8 << 20;
const MAX_SIGNATURE_BYTES_PER_BLOCK: usize = 256 << 20;

/// Trusted protocol bounds supplied by the verifier operator.
///
/// Nonzero epochs do not embed genesis configuration, so these values must be
/// explicit. Epoch zero cross-checks them against its embedded genesis record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PohProtocolBounds {
    pub ticks_per_slot: u64,
    pub hashes_per_tick: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PohSidecarSchema {
    Current,
    /// Current wire grammar whose stored signature counts are all the
    /// explicit legacy-unknown value zero. Exact counts are derived from hot
    /// transaction rows; a nonzero stored count is rejected.
    CurrentAllZeroDerived,
    LegacyNoSignatureCount,
}

impl PohProtocolBounds {
    fn hashes_per_slot(self) -> IntegrityResult<u64> {
        if self.ticks_per_slot == 0 || self.hashes_per_tick == 0 {
            return Err(ArchiveIntegrityError::Invalid(
                "PoH ticks-per-slot and hashes-per-tick must be positive".to_owned(),
            ));
        }
        self.ticks_per_slot
            .checked_mul(self.hashes_per_tick)
            .ok_or_else(|| {
                ArchiveIntegrityError::Invalid("PoH hashes-per-slot overflow".to_owned())
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveIntegrityConfig {
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub selected_blocks: usize,
    pub workers: usize,
    pub poh: PohProtocolBounds,
    pub poh_schema: PohSidecarSchema,
    pub max_hash_rounds_per_block: u64,
    pub max_total_hash_rounds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveIntegrityReport {
    pub complete_source: bool,
    pub selected_blocks: usize,
    pub source_total_blocks: usize,
    pub blockhash_registry_records: u64,
    pub blockhash_registry_offset: u64,
    pub chain_blocks_verified: u64,
    pub predecessor_tail_records_verified: u64,
    pub poh_blocks_verified: u64,
    pub poh_entries_verified: u64,
    pub poh_transactions_partitioned: u64,
    pub signature_bytes_hashed_for_poh: u64,
    pub poh_hash_rounds_recomputed: u128,
    pub legacy_poh_records: u64,
    pub reader: IntegrityReaderReport,
    pub elapsed_millis: u64,
    pub poh_entry_hashes: &'static str,
    pub blockhash_continuity: &'static str,
    pub tick_schedule_verification: &'static str,
    pub ed25519_signature_verification: &'static str,
    pub seal_content_hashing: &'static str,
    pub protocol_bounds: PohProtocolBounds,
    pub max_hash_rounds_per_block: u64,
    pub max_total_hash_rounds: u64,
    pub poh_schema: PohSidecarSchema,
    pub block_decode_worker_threads: usize,
    pub poh_recompute_worker_threads: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct IntegrityReaderReport {
    pub blocks: u64,
    pub batches: u64,
    pub compressed_bytes: u64,
    pub producer_read_millis: u64,
    pub decode_project_wall_millis: u64,
    pub worker_decode_sum_millis: u64,
    pub worker_projection_sum_millis: u64,
    pub producer_wait_millis: u64,
    pub max_blocks_per_batch: usize,
    pub max_compressed_batch_bytes: usize,
    pub max_declared_uncompressed_batch_bytes: u64,
    pub max_retained_decompressed_buffer_bytes: usize,
}

impl From<OrderedParallelBlockStats> for IntegrityReaderReport {
    fn from(value: OrderedParallelBlockStats) -> Self {
        Self {
            blocks: value.block_count,
            batches: value.batch_count,
            compressed_bytes: value.compressed_bytes,
            producer_read_millis: duration_millis(value.producer_read_wall_time),
            decode_project_wall_millis: duration_millis(value.coordinator_decode_project_wall_time),
            worker_decode_sum_millis: duration_millis(value.worker_decompress_decode_sum_time),
            worker_projection_sum_millis: duration_millis(value.worker_projection_sum_time),
            producer_wait_millis: duration_millis(value.producer_wait_for_free_buffer_time),
            max_blocks_per_batch: value.max_blocks_per_batch,
            max_compressed_batch_bytes: value.max_compressed_batch_bytes,
            max_declared_uncompressed_batch_bytes: value.max_declared_uncompressed_batch_bytes,
            max_retained_decompressed_buffer_bytes: value.max_retained_decompressed_buffer_bytes,
        }
    }
}

#[derive(Debug, Error)]
pub enum ArchiveIntegrityError {
    #[error("Archive V2 reader error: {0}")]
    Reader(#[from] ReaderError),
    #[error("Archive V2 source error: {0}")]
    Source(#[from] SourceError),
    #[error("Archive V2 integrity error: {0}")]
    Invalid(String),
    #[error("I/O error for {object}: {source}")]
    Io {
        object: &'static str,
        #[source]
        source: std::io::Error,
    },
}

pub type IntegrityResult<T> = std::result::Result<T, ArchiveIntegrityError>;

#[derive(Debug)]
struct ProjectedIntegrityBlock {
    row: ArchiveV2HotBlockIndexRow,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    signature_prefixes: Vec<u32>,
}

#[derive(Debug, Default)]
struct IntegrityWorker {
    signature_prefixes: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
struct EntryJobRange {
    start_hash: [u8; HASH_BYTES],
    expected_hash: [u8; HASH_BYTES],
    num_hashes: u64,
    transaction_count: u32,
    signature_start: usize,
    signature_end: usize,
}

#[derive(Clone, Copy)]
struct EntryJob<'a> {
    start_hash: [u8; HASH_BYTES],
    num_hashes: u64,
    transaction_count: u32,
    signatures: &'a [u8],
}

#[derive(Debug, Default)]
struct OrderedIntegrityState {
    position: usize,
    previous_slot_and_id: Option<(u64, u32)>,
    blocks_verified: u64,
    entries_verified: u64,
    transactions_partitioned: u64,
    signature_bytes_hashed: u64,
    hash_rounds_recomputed: u128,
    legacy_poh_records: u64,
    signature_bytes: Vec<u8>,
    entry_jobs: Vec<EntryJobRange>,
}

struct StrictFramedReader<R> {
    reader: R,
    payload: Vec<u8>,
}

impl<R: Read> StrictFramedReader<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            payload: Vec::new(),
        }
    }

    fn read_payload(&mut self, frame: usize) -> IntegrityResult<Option<&[u8]>> {
        let mut value = 0u32;
        let mut prefix_length = 0usize;
        for shift in [0u32, 7, 14, 21, 28] {
            let mut byte = [0u8; 1];
            let read = self
                .reader
                .read(&mut byte)
                .map_err(|source| ArchiveIntegrityError::Io {
                    object: ARCHIVE_V2_POH_FILE,
                    source,
                })?;
            if read == 0 {
                if prefix_length == 0 {
                    return Ok(None);
                }
                return Err(ArchiveIntegrityError::Invalid(format!(
                    "PoH frame {frame} has a truncated length prefix"
                )));
            }
            prefix_length += 1;
            let payload = u32::from(byte[0] & 0x7f);
            if shift == 28 && payload > 0x0f {
                return Err(ArchiveIntegrityError::Invalid(format!(
                    "PoH frame {frame} length overflows u32"
                )));
            }
            value |= payload << shift;
            if byte[0] & 0x80 == 0 {
                if prefix_length > 1 && payload == 0 {
                    return Err(ArchiveIntegrityError::Invalid(format!(
                        "PoH frame {frame} has a non-canonical length prefix"
                    )));
                }
                let length = value as usize;
                if length > MAX_POH_FRAME_BYTES {
                    return Err(ArchiveIntegrityError::Invalid(format!(
                        "PoH frame {frame} declares {length} bytes, above the {MAX_POH_FRAME_BYTES}-byte limit"
                    )));
                }
                self.payload.resize(length, 0);
                self.reader
                    .read_exact(&mut self.payload)
                    .map_err(|source| ArchiveIntegrityError::Io {
                        object: ARCHIVE_V2_POH_FILE,
                        source,
                    })?;
                return Ok(Some(&self.payload));
            }
        }
        Err(ArchiveIntegrityError::Invalid(format!(
            "PoH frame {frame} length prefix is too long"
        )))
    }
}

/// Verify block-header continuity, the predecessor boundary, and every PoH
/// entry in the selected source prefix.
///
/// A full verification requires `selected_blocks == reader.index().rows.len()`.
/// Prefix verification is explicit and reports `complete_source: false`.
pub fn verify_archive_v2_integrity(
    reader: &ArchiveReader<PinnedLocalRangeSource>,
    predecessor: Option<&ArchiveReader<PinnedLocalRangeSource>>,
    config: ArchiveIntegrityConfig,
) -> IntegrityResult<ArchiveIntegrityReport> {
    let started = Instant::now();
    let total_blocks = reader.index().rows.len();
    if config.selected_blocks == 0 || config.selected_blocks > total_blocks {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "selected block count {} is outside 1..={total_blocks}",
            config.selected_blocks
        )));
    }
    if config.workers == 0 || config.workers > 64 {
        return Err(ArchiveIntegrityError::Invalid(
            "integrity workers must be in 1..=64".to_owned(),
        ));
    }
    if config.slots_per_epoch == 0 {
        return Err(ArchiveIntegrityError::Invalid(
            "slots-per-epoch must be positive".to_owned(),
        ));
    }
    if config.max_hash_rounds_per_block == 0 || config.max_total_hash_rounds == 0 {
        return Err(ArchiveIntegrityError::Invalid(
            "PoH per-block and total hash-round limits must be positive".to_owned(),
        ));
    }
    if reader.manifest().epoch != config.epoch
        || reader.manifest().slots_per_epoch != config.slots_per_epoch
    {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "integrity config epoch/schedule ({}, {}) differs from source manifest ({}, {})",
            config.epoch,
            config.slots_per_epoch,
            reader.manifest().epoch,
            reader.manifest().slots_per_epoch
        )));
    }
    let hashes_per_slot = config.poh.hashes_per_slot()?;
    validate_genesis_poh_bounds(reader, config.poh)?;

    let (blockhashes, registry_records, registry_offset) =
        read_blockhash_registry(reader, config.epoch)?;
    let (block_start_hash, predecessor_parent_slot, tail_records_verified) =
        validate_predecessor_boundary(
            reader,
            predecessor,
            config.epoch,
            config.slots_per_epoch,
            &blockhashes,
            registry_offset,
        )?;

    let signature_size = reader
        .source()
        .size(ARCHIVE_V2_SIGNATURES_FILE)?
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid("signatures.bin is required for PoH".to_owned())
        })?;
    let expected_signature_size = reader
        .total_signatures()
        .checked_mul(SIGNATURE_BYTES as u64)
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid("signature sidecar size overflow".to_owned())
        })?;
    if signature_size != expected_signature_size {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "signatures.bin has {signature_size} bytes, expected {expected_signature_size}"
        )));
    }
    let signature_file = required_pinned_file(reader.source(), ARCHIVE_V2_SIGNATURES_FILE)?;
    let poh_file = required_pinned_file(reader.source(), ARCHIVE_V2_POH_FILE)?;
    let mut poh_reader =
        StrictFramedReader::new(BufReader::with_capacity(POH_READER_BUFFER_BYTES, poh_file));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.workers)
        .thread_name(|index| format!("archive-integrity-poh-{index}"))
        .build()
        .map_err(|error| {
            ArchiveIntegrityError::Invalid(format!(
                "cannot create PoH verification thread pool: {error}"
            ))
        })?;

    let mut state = OrderedIntegrityState::default();
    let reader_stats = reader.process_borrowed_blocks_parallel_ordered(
        Range {
            start: 0,
            end: config.selected_blocks,
        },
        OrderedParallelBlockConfig {
            decode_workers: config.workers,
            discard_rewards: true,
            max_blocks_per_batch: 1_024,
            ..OrderedParallelBlockConfig::default()
        },
        |_| Ok(IntegrityWorker::default()),
        |worker, _sequence, block| project_integrity_block(worker, block),
        |sequence, block| {
            consume_integrity_block(
                sequence,
                block,
                &signature_file,
                &mut poh_reader,
                &blockhashes,
                registry_offset,
                block_start_hash,
                predecessor_parent_slot,
                hashes_per_slot,
                config.max_hash_rounds_per_block,
                config.max_total_hash_rounds,
                config.poh_schema,
                &pool,
                &mut state,
            )
        },
    )?;

    if config.selected_blocks == total_blocks {
        let trailing = poh_reader.read_payload(config.selected_blocks)?;
        if trailing.is_some() {
            return Err(ArchiveIntegrityError::Invalid(
                "PoH sidecar has trailing records".to_owned(),
            ));
        }
    }
    if state.position != config.selected_blocks
        || state.blocks_verified != config.selected_blocks as u64
        || reader_stats.block_count != config.selected_blocks as u64
    {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "integrity pass completed {} ordered blocks and reader completed {}, expected {}",
            state.blocks_verified, reader_stats.block_count, config.selected_blocks
        )));
    }
    reader.source().verify_unchanged()?;
    if let Some(predecessor) = predecessor {
        predecessor.source().verify_unchanged()?;
    }

    let complete = config.selected_blocks == total_blocks;
    Ok(ArchiveIntegrityReport {
        complete_source: complete,
        selected_blocks: config.selected_blocks,
        source_total_blocks: total_blocks,
        blockhash_registry_records: registry_records,
        blockhash_registry_offset: registry_offset,
        chain_blocks_verified: state.blocks_verified,
        predecessor_tail_records_verified: tail_records_verified,
        poh_blocks_verified: state.blocks_verified,
        poh_entries_verified: state.entries_verified,
        poh_transactions_partitioned: state.transactions_partitioned,
        signature_bytes_hashed_for_poh: state.signature_bytes_hashed,
        poh_hash_rounds_recomputed: state.hash_rounds_recomputed,
        legacy_poh_records: state.legacy_poh_records,
        reader: reader_stats.into(),
        elapsed_millis: duration_millis(started.elapsed()),
        poh_entry_hashes: if complete {
            "complete"
        } else {
            "prefix-complete"
        },
        blockhash_continuity: if complete {
            "complete"
        } else {
            "prefix-complete"
        },
        tick_schedule_verification: "not-run",
        ed25519_signature_verification: "off",
        seal_content_hashing: "none",
        protocol_bounds: config.poh,
        max_hash_rounds_per_block: config.max_hash_rounds_per_block,
        max_total_hash_rounds: config.max_total_hash_rounds,
        poh_schema: config.poh_schema,
        block_decode_worker_threads: config.workers,
        poh_recompute_worker_threads: config.workers,
    })
}

fn project_integrity_block(
    worker: &mut IntegrityWorker,
    block: BorrowedDecodedBlock<'_>,
) -> IntegrityResult<ProjectedIntegrityBlock> {
    let row = block.index_row;
    if block.header().slot != row.slot
        || block.tx_count() != row.tx_count
        || block.tx_rows_len() != row.tx_count as usize
    {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "block {} slot {} header or transaction count differs from index",
            row.block_id, row.slot
        )));
    }
    worker.signature_prefixes.clear();
    worker
        .signature_prefixes
        .try_reserve(row.tx_count as usize + 1)
        .map_err(|error| {
            ArchiveIntegrityError::Invalid(format!(
                "reserve block {} signature prefixes: {error}",
                row.block_id
            ))
        })?;
    worker.signature_prefixes.push(0);
    let mut signature_count = 0u32;
    for tx in block.tx_rows() {
        signature_count = signature_count
            .checked_add(u32::from(tx.signature_count))
            .ok_or_else(|| {
                ArchiveIntegrityError::Invalid(format!(
                    "block {} signature count overflow",
                    row.block_id
                ))
            })?;
        worker.signature_prefixes.push(signature_count);
    }
    if signature_count != row.signature_count {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "block {} hot transaction rows have {signature_count} signatures, index has {}",
            row.block_id, row.signature_count
        )));
    }
    let header = block.header();
    Ok(ProjectedIntegrityBlock {
        row,
        parent_slot: header.parent_slot,
        blockhash_id: header.blockhash_id,
        previous_blockhash_id: header.previous_blockhash_id,
        signature_prefixes: worker.signature_prefixes.clone(),
    })
}

fn decode_poh_payload_exact(
    payload: &[u8],
    schema: PohSidecarSchema,
) -> IntegrityResult<WincodeArchiveV2PohRecord> {
    let profile = match schema {
        PohSidecarSchema::Current | PohSidecarSchema::CurrentAllZeroDerived => {
            PohWireProfile::ArchiveV2CurrentWincode055
        }
        PohSidecarSchema::LegacyNoSignatureCount => {
            PohWireProfile::ArchiveV2LegacyNoSignatureCountWincode055
        }
    };
    let decoded = decode_retained_poh_payload(profile, payload).map_err(|error| {
        ArchiveIntegrityError::Invalid(format!("selected PoH payload is not exact: {error}"))
    })?;
    Ok(match decoded {
        DecodedPohFrame::Current(record) => WincodeArchiveV2PohRecord {
            block_id: record.block_id,
            slot: record.slot,
            entries: record
                .entries
                .into_iter()
                .map(|entry| CompactPohEntry {
                    num_hashes: entry.num_hashes,
                    hash: entry.hash,
                    tx_count: entry.transaction_count,
                    signature_count: entry.signature_count,
                })
                .collect(),
        },
        DecodedPohFrame::LegacyNoSignatureCount(record) => WincodeArchiveV2PohRecord {
            block_id: record.block_id,
            slot: record.slot,
            entries: record
                .entries
                .into_iter()
                .map(|entry| CompactPohEntry {
                    num_hashes: entry.num_hashes,
                    hash: entry.hash,
                    tx_count: entry.transaction_count,
                    signature_count: 0,
                })
                .collect(),
        },
    })
}

#[allow(clippy::too_many_arguments)]
fn consume_integrity_block(
    sequence: usize,
    block: ProjectedIntegrityBlock,
    signature_file: &File,
    poh_reader: &mut StrictFramedReader<BufReader<File>>,
    blockhashes: &[u8],
    registry_offset: u64,
    first_start_hash: [u8; HASH_BYTES],
    predecessor_parent_slot: Option<u64>,
    hashes_per_slot: u64,
    max_hash_rounds_per_block: u64,
    max_total_hash_rounds: u64,
    expected_poh_schema: PohSidecarSchema,
    pool: &rayon::ThreadPool,
    state: &mut OrderedIntegrityState,
) -> IntegrityResult<()> {
    if sequence != state.position || block.row.block_id as usize != sequence {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "block sequence {sequence} does not match ordered position {} or block id {}",
            state.position, block.row.block_id
        )));
    }
    let expected_id = u64::try_from(sequence)
        .ok()
        .and_then(|value| value.checked_add(registry_offset))
        .ok_or_else(|| ArchiveIntegrityError::Invalid("blockhash id overflow".to_owned()))?;
    if u64::from(block.blockhash_id) != expected_id {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "block {sequence} slot {} has blockhash_id {}, expected {expected_id}",
            block.row.slot, block.blockhash_id
        )));
    }
    if let Some((previous_slot, previous_id)) = state.previous_slot_and_id {
        if block.previous_blockhash_id != previous_id {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "block {sequence} slot {} has previous_blockhash_id {}, previous block id is {previous_id}",
                block.row.slot, block.previous_blockhash_id
            )));
        }
        if block.parent_slot != previous_slot {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "block {sequence} slot {} has parent_slot {}, previous block slot is {previous_slot}",
                block.row.slot, block.parent_slot
            )));
        }
    } else if let Some(expected_parent_slot) = predecessor_parent_slot
        && block.parent_slot != expected_parent_slot
    {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "first block slot {} has parent_slot {}, predecessor tail ends at slot {expected_parent_slot}",
            block.row.slot, block.parent_slot
        )));
    } else if registry_offset == 1 && block.parent_slot != 0 {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "epoch-0 first block slot {} has parent_slot {}, expected genesis slot 0",
            block.row.slot, block.parent_slot
        )));
    }
    if sequence == 0 && block.previous_blockhash_id != 0 {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "first block slot {} has previous_blockhash_id {}, expected boundary id 0",
            block.row.slot, block.previous_blockhash_id
        )));
    }

    let payload = poh_reader.read_payload(sequence)?.ok_or_else(|| {
        ArchiveIntegrityError::Invalid(format!(
            "PoH sidecar ended before block {sequence} slot {}",
            block.row.slot
        ))
    })?;
    let poh = decode_poh_payload_exact(payload, expected_poh_schema).map_err(|error| {
        ArchiveIntegrityError::Invalid(format!(
            "decode PoH record for block {sequence} slot {}: {error}",
            block.row.slot
        ))
    })?;
    if poh.block_id != block.row.block_id || poh.slot != block.row.slot {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "PoH record block {} slot {} does not match block {sequence} slot {}",
            poh.block_id, poh.slot, block.row.slot
        )));
    }
    if poh.entries.is_empty() {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "block {sequence} slot {} has no PoH entries; mandatory PoH cannot use an external blockhash fallback",
            block.row.slot
        )));
    }

    let signature_start = block
        .row
        .first_signature_ordinal
        .checked_mul(SIGNATURE_BYTES as u64)
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid(format!("block {sequence} signature offset overflow"))
        })?;
    let signature_len = usize::try_from(block.row.signature_count)
        .ok()
        .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid(format!("block {sequence} signature length overflow"))
        })?;
    let maximum_from_rows = usize::try_from(block.row.tx_count)
        .ok()
        .and_then(|count| count.checked_mul(u8::MAX as usize))
        .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid(format!(
                "block {sequence} signature safety bound overflow"
            ))
        })?;
    let admitted_maximum = maximum_from_rows.min(MAX_SIGNATURE_BYTES_PER_BLOCK);
    if signature_len > admitted_maximum {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "block {sequence} declares {signature_len} signature bytes, above admitted bound {admitted_maximum}"
        )));
    }
    read_exact_at_reusing(
        signature_file,
        ARCHIVE_V2_SIGNATURES_FILE,
        signature_start,
        signature_len,
        &mut state.signature_bytes,
    )?;

    state.entry_jobs.clear();
    state
        .entry_jobs
        .try_reserve(poh.entries.len())
        .map_err(|error| {
            ArchiveIntegrityError::Invalid(format!(
                "reserve block {sequence} PoH entry jobs: {error}"
            ))
        })?;
    let block_start_hash = if sequence == 0 {
        first_start_hash
    } else {
        blockhash_at(blockhashes, sequence - 1, registry_offset)?
    };
    let mut tx_cursor = 0usize;
    let mut effective_hashes = 0u64;
    let slot_distance =
        poh_slot_distance(sequence, registry_offset, block.row.slot, block.parent_slot)?;
    let block_hash_budget = hashes_per_slot.checked_mul(slot_distance).ok_or_else(|| {
        ArchiveIntegrityError::Invalid(format!(
            "PoH block {sequence} slot-gap hash budget overflow"
        ))
    })?;
    for (entry_index, entry) in poh.entries.iter().enumerate() {
        if entry.num_hashes > MAX_REPLAY_NUM_HASHES_PER_ENTRY {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "PoH block {sequence} slot {} entry {entry_index} declares {} hashes, above absolute replay guard {MAX_REPLAY_NUM_HASHES_PER_ENTRY}",
                block.row.slot, entry.num_hashes
            )));
        }
        let entry_effective_hashes = entry.num_hashes.max(u64::from(entry.tx_count > 0));
        effective_hashes = effective_hashes
            .checked_add(entry_effective_hashes)
            .ok_or_else(|| {
                ArchiveIntegrityError::Invalid(format!(
                    "PoH block {sequence} effective hash count overflow"
                ))
            })?;
        if effective_hashes > block_hash_budget {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "PoH block {sequence} slot {} cumulative effective hashes {effective_hashes} exceed trusted gap bound {block_hash_budget} for distance {slot_distance}",
                block.row.slot,
            )));
        }
        let tx_end = tx_cursor
            .checked_add(entry.tx_count as usize)
            .ok_or_else(|| {
                ArchiveIntegrityError::Invalid(format!(
                    "PoH block {sequence} entry {entry_index} transaction range overflow"
                ))
            })?;
        if tx_end >= block.signature_prefixes.len() {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "PoH block {sequence} entry {entry_index} consumes transactions through {tx_end}, block has {}",
                block.row.tx_count
            )));
        }
        let first_signature = block.signature_prefixes[tx_cursor] as usize;
        let last_signature = block.signature_prefixes[tx_end] as usize;
        let derived_signature_count = last_signature - first_signature;
        if (entry.tx_count == 0 && derived_signature_count != 0)
            || (entry.tx_count > 0 && derived_signature_count < entry.tx_count as usize)
        {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "PoH block {sequence} entry {entry_index} partitions {} transactions into {derived_signature_count} signatures",
                entry.tx_count
            )));
        }
        match expected_poh_schema {
            PohSidecarSchema::Current => {
                if entry.signature_count as usize != derived_signature_count {
                    return Err(ArchiveIntegrityError::Invalid(format!(
                        "PoH block {sequence} entry {entry_index} records {} signatures, hot tx rows derive {derived_signature_count}",
                        entry.signature_count
                    )));
                }
            }
            PohSidecarSchema::CurrentAllZeroDerived => {
                if entry.signature_count != 0 {
                    return Err(ArchiveIntegrityError::Invalid(format!(
                        "PoH block {sequence} entry {entry_index} has signature_count {}, expected explicit all-zero derived profile",
                        entry.signature_count
                    )));
                }
            }
            PohSidecarSchema::LegacyNoSignatureCount => {}
        }
        let signature_start = first_signature
            .checked_mul(SIGNATURE_BYTES)
            .ok_or_else(|| {
                ArchiveIntegrityError::Invalid("entry signature offset overflow".to_owned())
            })?;
        let signature_end = last_signature.checked_mul(SIGNATURE_BYTES).ok_or_else(|| {
            ArchiveIntegrityError::Invalid("entry signature range overflow".to_owned())
        })?;
        let start_hash = if entry_index == 0 {
            block_start_hash
        } else {
            poh.entries[entry_index - 1].hash
        };
        state.entry_jobs.push(EntryJobRange {
            start_hash,
            expected_hash: entry.hash,
            num_hashes: entry.num_hashes,
            transaction_count: entry.tx_count,
            signature_start,
            signature_end,
        });
        tx_cursor = tx_end;
    }
    if tx_cursor != block.row.tx_count as usize {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "PoH block {sequence} entries consume {tx_cursor} of {} transactions",
            block.row.tx_count
        )));
    }
    let total_after_block = checked_poh_round_limits(
        sequence,
        effective_hashes,
        state.hash_rounds_recomputed,
        max_hash_rounds_per_block,
        max_total_hash_rounds,
    )?;

    let mismatch = pool.install(|| {
        state
            .entry_jobs
            .par_iter()
            .enumerate()
            .filter_map(|(entry_index, job)| {
                let actual = recompute_entry_hash_reusing_scratch(&EntryJob {
                    start_hash: job.start_hash,
                    num_hashes: job.num_hashes,
                    transaction_count: job.transaction_count,
                    signatures: &state.signature_bytes[job.signature_start..job.signature_end],
                });
                (actual != job.expected_hash).then_some((entry_index, actual))
            })
            .min_by_key(|(entry_index, _)| *entry_index)
    });
    if let Some((entry_index, actual)) = mismatch {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "PoH mismatch block {sequence} slot {} entry {entry_index}: expected {}, actual {}",
            block.row.slot,
            hex32(&state.entry_jobs[entry_index].expected_hash),
            hex32(&actual)
        )));
    }
    let expected_blockhash = blockhash_at(blockhashes, sequence, registry_offset)?;
    let final_hash = poh.entries.last().expect("nonempty checked").hash;
    if final_hash != expected_blockhash {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "block {sequence} slot {} final PoH hash {} differs from blockhash registry {}",
            block.row.slot,
            hex32(&final_hash),
            hex32(&expected_blockhash)
        )));
    }

    if expected_poh_schema == PohSidecarSchema::LegacyNoSignatureCount {
        state.legacy_poh_records = state.legacy_poh_records.saturating_add(1);
    }
    state.blocks_verified = state.blocks_verified.saturating_add(1);
    state.entries_verified = state
        .entries_verified
        .saturating_add(poh.entries.len() as u64);
    state.transactions_partitioned = state
        .transactions_partitioned
        .saturating_add(block.row.tx_count as u64);
    state.signature_bytes_hashed = state
        .signature_bytes_hashed
        .saturating_add(signature_len as u64);
    state.hash_rounds_recomputed = total_after_block;
    state.previous_slot_and_id = Some((block.row.slot, block.blockhash_id));
    state.position += 1;
    Ok(())
}

fn checked_poh_round_limits(
    sequence: usize,
    block_rounds: u64,
    completed_rounds: u128,
    max_hash_rounds_per_block: u64,
    max_total_hash_rounds: u64,
) -> IntegrityResult<u128> {
    if block_rounds > max_hash_rounds_per_block {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "PoH block {sequence} effective hashes {block_rounds} exceed configured per-block limit {max_hash_rounds_per_block}"
        )));
    }
    let total = completed_rounds
        .checked_add(u128::from(block_rounds))
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid("PoH total hash-round count overflow".to_owned())
        })?;
    if total > u128::from(max_total_hash_rounds) {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "PoH total effective hashes {total} exceed configured limit {max_total_hash_rounds} before block {sequence} recomputation"
        )));
    }
    Ok(total)
}

fn poh_slot_distance(
    sequence: usize,
    registry_offset: u64,
    slot: u64,
    parent_slot: u64,
) -> IntegrityResult<u64> {
    if sequence == 0 && registry_offset == 1 {
        return slot.checked_add(1).ok_or_else(|| {
            ArchiveIntegrityError::Invalid("epoch-0 first block slot-distance overflow".to_owned())
        });
    }
    slot.checked_sub(parent_slot)
        .map(|distance| distance.max(1))
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid(format!(
                "block {sequence} slot {slot} precedes parent slot {parent_slot}"
            ))
        })
}

fn validate_genesis_poh_bounds(
    reader: &ArchiveReader<PinnedLocalRangeSource>,
    bounds: PohProtocolBounds,
) -> IntegrityResult<()> {
    if let Some(genesis) = reader.genesis() {
        if genesis.epoch_schedule.warmup {
            return Err(ArchiveIntegrityError::Invalid(
                "epoch-0 genesis uses an unsupported warmup epoch schedule".to_owned(),
            ));
        }
        if genesis.epoch_schedule.slots_per_epoch != reader.manifest().slots_per_epoch {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "epoch-0 genesis slots-per-epoch {} differs from source manifest {}",
                genesis.epoch_schedule.slots_per_epoch,
                reader.manifest().slots_per_epoch
            )));
        }
        if genesis.ticks_per_slot != bounds.ticks_per_slot {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "trusted ticks-per-slot {} differs from epoch-0 genesis {}",
                bounds.ticks_per_slot, genesis.ticks_per_slot
            )));
        }
        let hashes_per_tick = genesis.poh_params.hashes_per_tick.ok_or_else(|| {
            ArchiveIntegrityError::Invalid(
                "epoch-0 genesis has no fixed hashes-per-tick value".to_owned(),
            )
        })?;
        if hashes_per_tick != bounds.hashes_per_tick {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "trusted hashes-per-tick {} differs from epoch-0 genesis {hashes_per_tick}",
                bounds.hashes_per_tick
            )));
        }
    }
    Ok(())
}

fn read_blockhash_registry(
    reader: &ArchiveReader<PinnedLocalRangeSource>,
    epoch: u64,
) -> IntegrityResult<(Vec<u8>, u64, u64)> {
    let size = reader
        .source()
        .size(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)?
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid("blockhash registry is missing".to_owned())
        })?;
    if size % HASH_BYTES as u64 != 0 {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "blockhash registry size {size} is not a multiple of {HASH_BYTES}"
        )));
    }
    let records = size / HASH_BYTES as u64;
    let rows = reader.index().rows.len() as u64;
    let offset = records.checked_sub(rows).ok_or_else(|| {
        ArchiveIntegrityError::Invalid(format!(
            "blockhash registry has {records} records for {rows} blocks"
        ))
    })?;
    let valid_offset = (epoch == 0 && offset <= 1) || (epoch > 0 && offset == 0);
    if !valid_offset {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "epoch {epoch} blockhash registry has {records} records for {rows} blocks (offset {offset})"
        )));
    }
    let length = usize::try_from(size).map_err(|_| {
        ArchiveIntegrityError::Invalid("blockhash registry exceeds address space".to_owned())
    })?;
    let bytes = reader
        .source()
        .read_all_bounded(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, length)?;
    Ok((bytes, records, offset))
}

fn validate_predecessor_boundary(
    reader: &ArchiveReader<PinnedLocalRangeSource>,
    predecessor: Option<&ArchiveReader<PinnedLocalRangeSource>>,
    epoch: u64,
    slots_per_epoch: u64,
    current_registry: &[u8],
    registry_offset: u64,
) -> IntegrityResult<([u8; HASH_BYTES], Option<u64>, u64)> {
    if epoch == 0 {
        if registry_offset != 1 {
            return Err(ArchiveIntegrityError::Invalid(
                "epoch 0 needs one leading genesis blockhash registry record for the first PoH start hash"
                    .to_owned(),
            ));
        }
        let genesis = reader.genesis().ok_or_else(|| {
            ArchiveIntegrityError::Invalid(
                "epoch 0 integrity requires embedded genesis metadata".to_owned(),
            )
        })?;
        let genesis_hash = blockhash_at(current_registry, 0, 0)?;
        if genesis_hash != genesis.genesis_hash {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "epoch 0 registry genesis hash {} differs from embedded genesis {}",
                hex32(&genesis_hash),
                hex32(&genesis.genesis_hash)
            )));
        }
        return Ok((genesis_hash, None, 0));
    }
    let predecessor = predecessor.ok_or_else(|| {
        ArchiveIntegrityError::Invalid(format!(
            "epoch {epoch} requires an explicit predecessor source"
        ))
    })?;
    let expected_predecessor_epoch = epoch - 1;
    if predecessor.manifest().slots_per_epoch != slots_per_epoch {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "predecessor slots-per-epoch {} differs from current verifier {slots_per_epoch}",
            predecessor.manifest().slots_per_epoch
        )));
    }
    if predecessor.index().rows.len() < TAIL_RECORDS {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "predecessor index has {} rows, fewer than required tail {TAIL_RECORDS}",
            predecessor.index().rows.len()
        )));
    }
    let tail = reader.source().read_all_bounded(
        ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
        TAIL_RECORDS * TAIL_RECORD_BYTES,
    )?;
    if tail.len() != TAIL_RECORDS * TAIL_RECORD_BYTES {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "predecessor tail has {} bytes, expected {}",
            tail.len(),
            TAIL_RECORDS * TAIL_RECORD_BYTES
        )));
    }
    let predecessor_size = predecessor
        .source()
        .size(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)?
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid("predecessor blockhash registry is missing".to_owned())
        })?;
    let predecessor_records = predecessor_size / HASH_BYTES as u64;
    let predecessor_rows = predecessor.index().rows.len() as u64;
    let predecessor_offset = predecessor_records
        .checked_sub(predecessor_rows)
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid(format!(
                "predecessor registry has {predecessor_records} records for {predecessor_rows} rows"
            ))
        })?;
    let expected_offset = u64::from(expected_predecessor_epoch == 0);
    if predecessor_offset != expected_offset {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "predecessor registry has {predecessor_records} records for {predecessor_rows} rows, expected offset {expected_offset}"
        )));
    }
    let hash_span = (TAIL_RECORDS * HASH_BYTES) as u64;
    if predecessor_size < hash_span || predecessor_size % HASH_BYTES as u64 != 0 {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "predecessor blockhash registry size {predecessor_size} cannot supply {TAIL_RECORDS} hashes"
        )));
    }
    let predecessor_hashes = predecessor.source().read_range(
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        predecessor_size - hash_span,
        hash_span as usize,
    )?;
    if predecessor.manifest().epoch != expected_predecessor_epoch {
        return Err(ArchiveIntegrityError::Invalid(format!(
            "predecessor source declares epoch {}, expected {expected_predecessor_epoch}",
            predecessor.manifest().epoch
        )));
    }
    let first_slot = expected_predecessor_epoch
        .checked_mul(slots_per_epoch)
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid("predecessor slot range overflow".to_owned())
        })?;
    let slot_end = epoch.checked_mul(slots_per_epoch).ok_or_else(|| {
        ArchiveIntegrityError::Invalid("predecessor slot range overflow".to_owned())
    })?;
    let predecessor_tail_start = predecessor.index().rows.len() - TAIL_RECORDS;
    let predecessor_rows = &predecessor.index().rows[predecessor_tail_start..];
    let mut predecessor_blocks = predecessor.borrowed_blocks_without_rewards_range(
        predecessor_tail_start..predecessor.index().rows.len(),
    )?;
    for (record, expected_row) in predecessor_rows.iter().enumerate() {
        let tail_offset = record * TAIL_RECORD_BYTES;
        let hash_offset = record * HASH_BYTES;
        if tail[tail_offset..tail_offset + HASH_BYTES]
            != predecessor_hashes[hash_offset..hash_offset + HASH_BYTES]
        {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "predecessor tail hash mismatch at record {record}"
            )));
        }
        let slot = u64::from_le_bytes(
            tail[tail_offset + HASH_BYTES..tail_offset + TAIL_RECORD_BYTES]
                .try_into()
                .expect("tail slot is 8 bytes"),
        );
        if slot < first_slot || slot >= slot_end {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "predecessor tail record {record} slot {slot} is outside {first_slot}..{slot_end}"
            )));
        }
        let expected_slot = expected_row.slot;
        if slot != expected_slot {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "predecessor tail record {record} has slot {slot}, predecessor index has {expected_slot}"
            )));
        }
        let block = predecessor_blocks.next_block().ok_or_else(|| {
            ArchiveIntegrityError::Invalid(format!(
                "predecessor block stream ended before tail record {record}"
            ))
        })??;
        if block.uses_owned_fallback() {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "predecessor tail record {record} used an owned outer-schema fallback"
            )));
        }
        if block.index_row.block_id != expected_row.block_id
            || block.index_row.slot != expected_row.slot
            || block.index_row.tx_count != expected_row.tx_count
            || block.header().slot != expected_slot
        {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "predecessor tail record {record} block identity differs from its index row"
            )));
        }
        let row_position = predecessor_tail_start + record;
        let expected_id = u32::try_from(
            u64::try_from(row_position)
                .ok()
                .and_then(|position| position.checked_add(predecessor_offset))
                .ok_or_else(|| {
                    ArchiveIntegrityError::Invalid(
                        "predecessor tail blockhash id overflow".to_owned(),
                    )
                })?,
        )
        .map_err(|_| {
            ArchiveIntegrityError::Invalid("predecessor tail blockhash id exceeds u32".to_owned())
        })?;
        if block.header().blockhash_id != expected_id {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "predecessor tail record {record} has blockhash_id {}, expected {expected_id}",
                block.header().blockhash_id
            )));
        }
        let expected_previous_id = expected_id.saturating_sub(1);
        if block.header().previous_blockhash_id != expected_previous_id {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "predecessor tail record {record} has previous_blockhash_id {}, expected {expected_previous_id}",
                block.header().previous_blockhash_id
            )));
        }
        if row_position > 0 {
            let expected_parent = predecessor.index().rows[row_position - 1].slot;
            if block.header().parent_slot != expected_parent {
                return Err(ArchiveIntegrityError::Invalid(format!(
                    "predecessor tail record {record} has parent_slot {}, expected {expected_parent}",
                    block.header().parent_slot
                )));
            }
        } else if expected_predecessor_epoch == 0 && block.header().parent_slot != 0 {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "epoch-0 predecessor first block has parent_slot {}, expected 0",
                block.header().parent_slot
            )));
        }
    }
    if predecessor_blocks.next_block().is_some() {
        return Err(ArchiveIntegrityError::Invalid(
            "predecessor tail block stream has extra rows".to_owned(),
        ));
    }
    let start = (TAIL_RECORDS - 1) * TAIL_RECORD_BYTES;
    Ok((
        tail[start..start + HASH_BYTES]
            .try_into()
            .expect("tail hash is 32 bytes"),
        Some(predecessor_rows.last().expect("300 rows checked").slot),
        TAIL_RECORDS as u64,
    ))
}

fn blockhash_at(
    bytes: &[u8],
    block_position: usize,
    registry_offset: u64,
) -> IntegrityResult<[u8; HASH_BYTES]> {
    let record = u64::try_from(block_position)
        .ok()
        .and_then(|position| position.checked_add(registry_offset))
        .ok_or_else(|| ArchiveIntegrityError::Invalid("blockhash position overflow".to_owned()))?;
    let start = usize::try_from(record)
        .ok()
        .and_then(|record| record.checked_mul(HASH_BYTES))
        .ok_or_else(|| ArchiveIntegrityError::Invalid("blockhash offset overflow".to_owned()))?;
    let end = start
        .checked_add(HASH_BYTES)
        .ok_or_else(|| ArchiveIntegrityError::Invalid("blockhash range overflow".to_owned()))?;
    bytes
        .get(start..end)
        .ok_or_else(|| {
            ArchiveIntegrityError::Invalid(format!("blockhash registry has no record {record}"))
        })?
        .try_into()
        .map_err(|_| ArchiveIntegrityError::Invalid("blockhash row is not 32 bytes".to_owned()))
}

fn required_pinned_file(
    source: &PinnedLocalRangeSource,
    object: &'static str,
) -> IntegrityResult<File> {
    source.pinned_file_clone(object)?.ok_or_else(|| {
        ArchiveIntegrityError::Invalid(format!("required integrity object {object} is missing"))
    })
}

fn read_exact_at_reusing(
    file: &File,
    object: &'static str,
    offset: u64,
    length: usize,
    bytes: &mut Vec<u8>,
) -> IntegrityResult<()> {
    if bytes.len() < length {
        bytes.resize(length, 0);
    } else {
        bytes.truncate(length);
    }
    let mut read = 0usize;
    while read < length {
        let count = file
            .read_at(&mut bytes[read..], offset + read as u64)
            .map_err(|source| ArchiveIntegrityError::Io { object, source })?;
        if count == 0 {
            return Err(ArchiveIntegrityError::Invalid(format!(
                "short read for {object}: got {read} bytes, expected {length}"
            )));
        }
        read += count;
    }
    Ok(())
}

#[inline]
fn recompute_entry_hash_reusing_scratch(job: &EntryJob<'_>) -> [u8; HASH_BYTES] {
    recompute_entry_hash(job)
}

fn recompute_entry_hash(job: &EntryJob<'_>) -> [u8; HASH_BYTES] {
    let mut hash = job.start_hash;
    hash_chain(&mut hash, job.num_hashes.saturating_sub(1));
    if job.transaction_count == 0 {
        if job.num_hashes == 0 {
            job.start_hash
        } else {
            hash_one(&hash)
        }
    } else {
        let mut mixin = ReplaySignatureMixinBuilder::new();
        for signature in job.signatures.chunks_exact(SIGNATURE_BYTES) {
            let signature: &[u8; SIGNATURE_BYTES] = signature
                .try_into()
                .expect("signature chunks are exactly 64 bytes");
            mixin
                .push_signature(signature)
                .expect("admitted signature count fits the fixed replay frontier");
        }
        let mixin = mixin.finish();
        hash_pair(&hash, &mixin)
    }
}

#[inline]
fn hash_one(value: &[u8; HASH_BYTES]) -> [u8; HASH_BYTES] {
    Sha256::digest(value).into()
}

const SHA256_IV: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

fn hash_chain(hash: &mut [u8; HASH_BYTES], count: u64) {
    if count == 0 {
        return;
    }
    let mut block = [0u8; 64];
    block[32] = 0x80;
    block[62] = 0x01;
    for _ in 0..count {
        block[..HASH_BYTES].copy_from_slice(hash);
        let mut state = SHA256_IV;
        compress256(&mut state, core::slice::from_ref(&block));
        for (output, word) in hash.chunks_exact_mut(4).zip(state) {
            output.copy_from_slice(&word.to_be_bytes());
        }
    }
}

#[inline]
fn hash_pair(left: &[u8; HASH_BYTES], right: &[u8; HASH_BYTES]) -> [u8; HASH_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

fn hex32(value: &[u8; HASH_BYTES]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in value {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{
        ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_GENESIS_BIN_FILE,
        ARCHIVE_V2_META_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ArchiveV2HotBlockBlob,
        ArchiveV2HotBlockHeader, ArchiveV2HotMetaRecord, ArchiveV2HotTxRow,
        ReplaySignatureMixinBuilder, WINCODE_ARCHIVE_V2_FLAG_LEB128,
        WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Genesis,
        WincodeArchiveV2GenesisEpochSchedule, WincodeArchiveV2GenesisFeeParams,
        WincodeArchiveV2GenesisInflationParams, WincodeArchiveV2GenesisPohParams,
        WincodeArchiveV2GenesisRentParams, WincodeArchiveV2Header, derive_replay_entry_hash,
        framed::write_u32_varint, wincode_leb128_config, write_archive_v2_hot_block_index,
    };
    use blockzilla_index_archive_format::{
        sidecars::poh::{CurrentPohEntry, CurrentPohRecord, LegacyPohEntry, LegacyPohRecord},
        wincode as archive_wire,
    };
    use std::{fs, io::Cursor, path::Path};
    use tempfile::TempDir;

    #[test]
    fn tick_hash_matches_repeated_sha256() {
        for count in [0, 1, 2, 31, 257] {
            let mut expected = [7u8; HASH_BYTES];
            for _ in 0..count {
                expected = hash_one(&expected);
            }
            let mut actual = [7u8; HASH_BYTES];
            hash_chain(&mut actual, count);
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn protocol_bound_rejects_zero_and_overflow() {
        assert!(
            PohProtocolBounds {
                ticks_per_slot: 0,
                hashes_per_tick: 12_500,
            }
            .hashes_per_slot()
            .is_err()
        );
        assert!(
            PohProtocolBounds {
                ticks_per_slot: u64::MAX,
                hashes_per_tick: 2,
            }
            .hashes_per_slot()
            .is_err()
        );
    }

    #[test]
    fn operational_round_limits_fail_before_recomputation() {
        assert_eq!(checked_poh_round_limits(3, 7, 11, 7, 18).unwrap(), 18);
        assert!(checked_poh_round_limits(3, 8, 0, 7, 100).is_err());
        assert!(checked_poh_round_limits(3, 7, 12, 7, 18).is_err());
        assert!(checked_poh_round_limits(3, 1, u128::MAX, 7, u64::MAX).is_err());
    }

    #[test]
    fn epoch_zero_first_produced_slot_distance_includes_genesis_interval() {
        assert_eq!(poh_slot_distance(0, 1, 7, 0).unwrap(), 8);
        assert_eq!(poh_slot_distance(0, 1, 0, 0).unwrap(), 1);
        assert_eq!(poh_slot_distance(0, 0, 7, 3).unwrap(), 4);
        assert!(poh_slot_distance(1, 0, 2, 3).is_err());
    }

    #[test]
    fn strict_framing_rejects_padding_truncation_and_overflow() {
        for bytes in [
            vec![0x80],
            vec![0x80, 0x00],
            vec![0xff, 0xff, 0xff, 0xff, 0x10],
        ] {
            let mut reader = StrictFramedReader::new(Cursor::new(bytes));
            assert!(reader.read_payload(0).is_err());
        }

        let mut reader = StrictFramedReader::new(Cursor::new(vec![1, 9]));
        assert_eq!(reader.read_payload(0).unwrap(), Some(&[9][..]));
        assert_eq!(reader.read_payload(1).unwrap(), None);
    }

    #[test]
    fn selected_poh_payload_profile_is_exact_and_allocation_bounded() {
        let current = archive_wire::encode(&CurrentPohRecord {
            block_id: 1,
            slot: 2,
            entries: vec![CurrentPohEntry {
                num_hashes: 3,
                hash: [4; 32],
                transaction_count: 1,
                signature_count: 1,
            }],
        })
        .unwrap();
        assert!(decode_poh_payload_exact(&current, PohSidecarSchema::Current).is_ok());
        assert!(
            decode_poh_payload_exact(&current, PohSidecarSchema::LegacyNoSignatureCount).is_err()
        );
        let mut trailing = current.clone();
        trailing.push(0);
        assert!(decode_poh_payload_exact(&trailing, PohSidecarSchema::Current).is_err());

        let legacy = archive_wire::encode(&LegacyPohRecord {
            block_id: 1,
            slot: 2,
            entries: vec![LegacyPohEntry {
                num_hashes: 3,
                hash: [4; 32],
                transaction_count: 1,
            }],
        })
        .unwrap();
        assert!(
            decode_poh_payload_exact(&legacy, PohSidecarSchema::LegacyNoSignatureCount).is_ok()
        );
        assert!(decode_poh_payload_exact(&legacy, PohSidecarSchema::Current).is_err());

        let mut oversized = archive_wire::encode(&CurrentPohRecord {
            block_id: 0,
            slot: 0,
            entries: Vec::new(),
        })
        .unwrap();
        assert_eq!(oversized.pop(), Some(0));
        let mut length = 64_u64 << 20;
        length += 1;
        while length >= 0x80 {
            oversized.push((length as u8 & 0x7f) | 0x80);
            length >>= 7;
        }
        oversized.push(length as u8);
        assert!(decode_poh_payload_exact(&oversized, PohSidecarSchema::Current).is_err());
    }

    #[test]
    fn optimized_poh_core_matches_public_replay_authority() {
        for (num_hashes, transaction_count, signature_count) in [
            (0, 0, 0),
            (1, 0, 0),
            (0, 1, 1),
            (2, 1, 2),
            (7, 2, 3),
            (3, 2, 4),
            (9, 3, 5),
        ] {
            let signatures: Vec<[u8; SIGNATURE_BYTES]> = (0..signature_count)
                .map(|index| [index as u8 + 3; SIGNATURE_BYTES])
                .collect();
            let mixin = if transaction_count == 0 {
                None
            } else {
                let mut builder = ReplaySignatureMixinBuilder::new();
                for signature in &signatures {
                    builder.push_signature(signature).unwrap();
                }
                Some(builder.finish())
            };
            let expected =
                derive_replay_entry_hash([5; HASH_BYTES], num_hashes, transaction_count, mixin)
                    .unwrap();
            let bytes: Vec<u8> = signatures.into_iter().flatten().collect();
            let actual = recompute_entry_hash(&EntryJob {
                start_hash: [5; HASH_BYTES],
                num_hashes,
                transaction_count,
                signatures: &bytes,
            });
            assert_eq!(actual, expected);
        }
    }

    fn write_test_metadata(root: &Path, blocks: u64, transactions: u64) {
        let records = [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks,
                transactions,
                ..WincodeArchiveV2Footer::default()
            }),
        ];
        let mut bytes = Vec::new();
        for record in records {
            let payload = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
            write_u32_varint(&mut bytes, payload.len() as u32).unwrap();
            bytes.extend_from_slice(&payload);
        }
        fs::write(root.join(ARCHIVE_V2_META_FILE), bytes).unwrap();
    }

    fn write_test_genesis_metadata(
        root: &Path,
        slots_per_epoch: u64,
        warmup: bool,
        hashes_per_tick: Option<u64>,
    ) {
        let genesis_bytes = b"integrity-test-genesis";
        fs::write(root.join(ARCHIVE_V2_GENESIS_BIN_FILE), genesis_bytes).unwrap();
        let genesis = WincodeArchiveV2Genesis {
            genesis_hash: Sha256::digest(genesis_bytes).into(),
            genesis_bin_len: genesis_bytes.len() as u64,
            creation_time_unix: 0,
            cluster_id: 0,
            ticks_per_slot: 64,
            poh_params: WincodeArchiveV2GenesisPohParams {
                tick_duration_secs: 0,
                tick_duration_nanos: 400_000_000,
                tick_count: None,
                hashes_per_tick,
            },
            fees: WincodeArchiveV2GenesisFeeParams {
                target_lamports_per_sig: 10_000,
                target_sigs_per_slot: 20_000,
                min_lamports_per_sig: 5_000,
                max_lamports_per_sig: 100_000,
                burn_percent: 100,
            },
            rent: WincodeArchiveV2GenesisRentParams {
                lamports_per_byte_year: 3_480,
                exemption_threshold: 2.0,
                burn_percent: 100,
            },
            inflation: WincodeArchiveV2GenesisInflationParams {
                initial: 0.0,
                terminal: 0.0,
                taper: 0.0,
                foundation: 0.0,
                foundation_term: 0.0,
                padding: [0; 8],
            },
            epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
                slots_per_epoch,
                leader_schedule_slot_offset: slots_per_epoch,
                warmup,
                first_normal_epoch: 0,
                first_normal_slot: 0,
            },
            accounts: Vec::new(),
            builtins: Vec::new(),
            reward_pools: Vec::new(),
        };
        let records = [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Genesis(genesis),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks: 1,
                ..WincodeArchiveV2Footer::default()
            }),
        ];
        let mut bytes = Vec::new();
        for record in records {
            let payload = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
            write_u32_varint(&mut bytes, payload.len() as u32).unwrap();
            bytes.extend_from_slice(&payload);
        }
        fs::write(root.join(ARCHIVE_V2_META_FILE), bytes).unwrap();
    }

    fn write_test_blocks(root: &Path, slots: &[u64], first_parent: u64, with_transaction: bool) {
        let mut compressed_blocks = Vec::new();
        let mut rows = Vec::new();
        let mut compressed_offset = 0u64;
        for (position, &slot) in slots.iter().enumerate() {
            let (tx_rows, message_bytes, signature_count) = if with_transaction {
                (
                    vec![ArchiveV2HotTxRow {
                        tx_index: 0,
                        flags: blockzilla_format::ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                        message_offset: 0,
                        message_len: 1,
                        metadata_offset: 0,
                        metadata_len: 0,
                        signature_count: 1,
                        reserved: [0; 3],
                    }],
                    vec![0],
                    1,
                )
            } else {
                (Vec::new(), Vec::new(), 0)
            };
            let block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot,
                    parent_slot: if position == 0 {
                        first_parent
                    } else {
                        slots[position - 1]
                    },
                    blockhash_id: position as u32,
                    previous_blockhash_id: position.saturating_sub(1) as u32,
                    block_time: None,
                    block_height: None,
                    rewards: None,
                },
                tx_count: u32::from(with_transaction),
                tx_rows,
                message_bytes,
                metadata_bytes: Vec::new(),
            };
            let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
            let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
            rows.push(ArchiveV2HotBlockIndexRow {
                block_id: position as u32,
                slot,
                compressed_offset,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: u32::from(with_transaction),
                first_tx_ordinal: position as u64 * u64::from(with_transaction),
                first_signature_ordinal: position as u64 * u64::from(with_transaction),
                signature_count,
            });
            compressed_offset += compressed.len() as u64;
            compressed_blocks.extend_from_slice(&compressed);
        }
        fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed_blocks).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed_blocks.len() as u64,
            1,
            0,
            &rows,
        )
        .unwrap();
        write_test_metadata(
            root,
            slots.len() as u64,
            slots.len() as u64 * u64::from(with_transaction),
        );
        fs::write(root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), [1u8; 32]).unwrap();
        let signatures = vec![7u8; slots.len() * usize::from(with_transaction) * SIGNATURE_BYTES];
        fs::write(root.join(ARCHIVE_V2_SIGNATURES_FILE), signatures).unwrap();
    }

    fn open_test_archive(
        root: &Path,
        epoch: u64,
        slots_per_epoch: u64,
    ) -> ArchiveReader<PinnedLocalRangeSource> {
        ArchiveReader::open_trusted(
            PinnedLocalRangeSource::new(root),
            crate::manifest::TrustedGenerationIdentity {
                cluster_id: "test".to_owned(),
                epoch,
                generation_id: format!("integrity-test-{epoch}"),
                slots_per_epoch,
            },
            crate::OpenOptions {
                hash_verification: crate::HashVerification::SizesOnly,
                ..crate::OpenOptions::default()
            },
        )
        .unwrap()
    }

    fn write_current_poh(
        root: &Path,
        profile: PohSidecarSchema,
        start_hash: [u8; 32],
        slot: u64,
    ) -> [u8; 32] {
        let signatures = [[7u8; SIGNATURE_BYTES]];
        let mut builder = ReplaySignatureMixinBuilder::new();
        builder.push_signature(&signatures[0]).unwrap();
        let hash = derive_replay_entry_hash(start_hash, 1, 1, Some(builder.finish())).unwrap();
        let payload = match profile {
            PohSidecarSchema::Current | PohSidecarSchema::CurrentAllZeroDerived => {
                archive_wire::encode(&CurrentPohRecord {
                    block_id: 0,
                    slot,
                    entries: vec![CurrentPohEntry {
                        num_hashes: 1,
                        hash,
                        transaction_count: 1,
                        signature_count: if profile == PohSidecarSchema::Current {
                            1
                        } else {
                            0
                        },
                    }],
                })
                .unwrap()
            }
            PohSidecarSchema::LegacyNoSignatureCount => archive_wire::encode(&LegacyPohRecord {
                block_id: 0,
                slot,
                entries: vec![LegacyPohEntry {
                    num_hashes: 1,
                    hash,
                    transaction_count: 1,
                }],
            })
            .unwrap(),
        };
        let mut framed = Vec::new();
        write_u32_varint(&mut framed, payload.len() as u32).unwrap();
        framed.extend_from_slice(&payload);
        fs::write(root.join(ARCHIVE_V2_POH_FILE), framed).unwrap();
        hash
    }

    fn write_custom_current_poh(
        root: &Path,
        start_hash: [u8; 32],
        slot: u64,
        transaction_count: u32,
        signature_count: u32,
        forced_entry_hash: Option<[u8; 32]>,
    ) -> [u8; 32] {
        let mixin = if transaction_count == 0 {
            None
        } else {
            let mut builder = ReplaySignatureMixinBuilder::new();
            builder.push_signature(&[7u8; SIGNATURE_BYTES]).unwrap();
            Some(builder.finish())
        };
        let hash = derive_replay_entry_hash(start_hash, 1, transaction_count, mixin).unwrap();
        let payload = archive_wire::encode(&CurrentPohRecord {
            block_id: 0,
            slot,
            entries: vec![CurrentPohEntry {
                num_hashes: 1,
                hash: forced_entry_hash.unwrap_or(hash),
                transaction_count,
                signature_count,
            }],
        })
        .unwrap();
        let mut framed = Vec::new();
        write_u32_varint(&mut framed, payload.len() as u32).unwrap();
        framed.extend_from_slice(&payload);
        fs::write(root.join(ARCHIVE_V2_POH_FILE), framed).unwrap();
        hash
    }

    fn write_predecessor_tail(
        current_root: &Path,
        predecessor_hashes: &[[u8; 32]],
        predecessor_slots: &[u64],
    ) {
        let mut tail = Vec::with_capacity(TAIL_RECORDS * TAIL_RECORD_BYTES);
        for (&hash, &slot) in predecessor_hashes.iter().zip(predecessor_slots) {
            tail.extend_from_slice(&hash);
            tail.extend_from_slice(&slot.to_le_bytes());
        }
        fs::write(current_root.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE), tail).unwrap();
    }

    fn integrity_test_config(profile: PohSidecarSchema) -> ArchiveIntegrityConfig {
        ArchiveIntegrityConfig {
            epoch: 2,
            slots_per_epoch: 1_000,
            selected_blocks: 1,
            workers: 2,
            poh: PohProtocolBounds {
                ticks_per_slot: 1,
                hashes_per_tick: 1,
            },
            poh_schema: profile,
            max_hash_rounds_per_block: 1_000,
            max_total_hash_rounds: 1_000,
        }
    }

    #[test]
    fn full_integrity_fixture_accepts_all_explicit_poh_profiles() {
        const SLOTS_PER_EPOCH: u64 = 1_000;
        let predecessor = TempDir::new().unwrap();
        let predecessor_slots: Vec<u64> = (1_000..1_300).collect();
        write_test_blocks(predecessor.path(), &predecessor_slots, 999, false);
        let predecessor_hashes: Vec<[u8; 32]> = (0..TAIL_RECORDS)
            .map(|index| Sha256::digest((index as u64).to_le_bytes()).into())
            .collect();
        fs::write(
            predecessor.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            predecessor_hashes
                .iter()
                .flatten()
                .copied()
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let predecessor_reader = open_test_archive(predecessor.path(), 1, SLOTS_PER_EPOCH);

        for profile in [
            PohSidecarSchema::Current,
            PohSidecarSchema::CurrentAllZeroDerived,
            PohSidecarSchema::LegacyNoSignatureCount,
        ] {
            let current = TempDir::new().unwrap();
            write_test_blocks(current.path(), &[2_000], 1_299, true);
            let start_hash = *predecessor_hashes.last().unwrap();
            let current_hash = write_current_poh(current.path(), profile, start_hash, 2_000);
            fs::write(
                current.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
                current_hash,
            )
            .unwrap();
            write_predecessor_tail(current.path(), &predecessor_hashes, &predecessor_slots);
            let current_reader = open_test_archive(current.path(), 2, SLOTS_PER_EPOCH);
            let report = verify_archive_v2_integrity(
                &current_reader,
                Some(&predecessor_reader),
                integrity_test_config(profile),
            )
            .unwrap();
            assert!(report.complete_source);
            assert_eq!(report.predecessor_tail_records_verified, 300);
            assert_eq!(report.poh_transactions_partitioned, 1);
            assert_eq!(report.signature_bytes_hashed_for_poh, 64);
        }
    }

    #[test]
    fn epoch_zero_genesis_bounds_require_fixed_non_warmup_schedule() {
        for (warmup, hashes_per_tick, expected_ok) in [
            (false, Some(12_500), true),
            (true, Some(12_500), false),
            (false, None, false),
            (false, Some(12_499), false),
        ] {
            let source = TempDir::new().unwrap();
            write_test_blocks(source.path(), &[0], 0, false);
            write_test_genesis_metadata(source.path(), 1_000, warmup, hashes_per_tick);
            let reader = open_test_archive(source.path(), 0, 1_000);
            let result = validate_genesis_poh_bounds(
                &reader,
                PohProtocolBounds {
                    ticks_per_slot: 64,
                    hashes_per_tick: 12_500,
                },
            );
            assert_eq!(result.is_ok(), expected_ok);
        }
    }

    #[test]
    fn full_integrity_fixture_rejects_partition_eof_chain_and_tail_corruption() {
        let predecessor = TempDir::new().unwrap();
        let predecessor_slots: Vec<u64> = (1_000..1_300).collect();
        write_test_blocks(predecessor.path(), &predecessor_slots, 999, false);
        let predecessor_hashes: Vec<[u8; 32]> = (0..TAIL_RECORDS)
            .map(|index| Sha256::digest((index as u64).to_le_bytes()).into())
            .collect();
        fs::write(
            predecessor.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            predecessor_hashes
                .iter()
                .flatten()
                .copied()
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let predecessor_reader = open_test_archive(predecessor.path(), 1, 1_000);
        let start_hash = *predecessor_hashes.last().unwrap();

        for case in [
            "partition",
            "signature-count",
            "all-zero-nonzero",
            "trailing-frame",
            "registry",
            "parent",
            "tail",
            "signature-content",
            "entry-hash",
        ] {
            let current = TempDir::new().unwrap();
            write_test_blocks(
                current.path(),
                &[2_000],
                if case == "parent" { 1_298 } else { 1_299 },
                true,
            );
            let (entry_transactions, entry_signatures) = match case {
                "partition" => (0, 0),
                "signature-count" => (1, 0),
                _ => (1, 1),
            };
            let current_hash = write_custom_current_poh(
                current.path(),
                start_hash,
                2_000,
                entry_transactions,
                entry_signatures,
                (case == "entry-hash").then_some([88; 32]),
            );
            fs::write(
                current.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
                if case == "registry" {
                    [99; 32]
                } else {
                    current_hash
                },
            )
            .unwrap();
            write_predecessor_tail(current.path(), &predecessor_hashes, &predecessor_slots);
            if case == "tail" {
                let path = current.path().join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
                let mut bytes = fs::read(&path).unwrap();
                bytes[0] ^= 1;
                fs::write(path, bytes).unwrap();
            }
            if case == "trailing-frame" {
                use std::io::Write as _;
                let mut file = fs::OpenOptions::new()
                    .append(true)
                    .open(current.path().join(ARCHIVE_V2_POH_FILE))
                    .unwrap();
                file.write_all(&[0]).unwrap();
            }
            if case == "signature-content" {
                fs::write(current.path().join(ARCHIVE_V2_SIGNATURES_FILE), [8u8; 64]).unwrap();
            }
            let current_reader = open_test_archive(current.path(), 2, 1_000);
            let profile = if case == "all-zero-nonzero" {
                PohSidecarSchema::CurrentAllZeroDerived
            } else {
                PohSidecarSchema::Current
            };
            assert!(
                verify_archive_v2_integrity(
                    &current_reader,
                    Some(&predecessor_reader),
                    integrity_test_config(profile),
                )
                .is_err(),
                "corruption case {case} was accepted"
            );
        }
    }
}
