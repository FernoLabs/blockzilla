//! Fast readers for the merged transaction stream and runtime effects.

use std::{fs::File, os::unix::fs::FileExt, path::Path};

use anyhow::{Context, Result, ensure};
use blockzilla_archive_v3::{
    ArchiveId, FILE_HEADER_LEN, FileHeader,
    catalog::blocks::{self as catalog_blocks, BlockRow, PageSpan},
    ledger::transactions::{
        self as transactions, EFFECT_CHUNK_TRANSACTIONS, EffectKind, Transaction, TransactionBlock,
    },
    runtime::{
        balances::{self, Balances},
        inner_instructions::{self, TransactionInner},
        logs::{self, LogLine},
        outcomes::{self, TransactionOutcome},
        rewards::{self, Reward},
        token_balances::{self, TokenBalance},
    },
};

use crate::{
    container::{decode_zstd_exact, validate_open_file},
    transaction_view::ResolvedAccounts,
};

pub const DEFAULT_MAX_BLOCK_DECODED_BYTES: usize = 512 << 20;

struct InputObject {
    path: &'static str,
    file: File,
    header: FileHeader,
    file_len: u64,
}

impl InputObject {
    fn open(root: &Path, path: &'static str, archive_id: ArchiveId) -> Result<Self> {
        let full_path = root.join(path);
        let file = File::open(&full_path)
            .with_context(|| format!("open canonical object {}", full_path.display()))?;
        let header = validate_open_file(&file, path, archive_id)?;
        let file_len = file.metadata()?.len();
        Ok(Self {
            path,
            file,
            header,
            file_len,
        })
    }

    fn read_page(&self, span: PageSpan, max_decoded_bytes: usize, label: &str) -> Result<Vec<u8>> {
        ensure!(
            span.offset >= FILE_HEADER_LEN as u64,
            "{label} points into the {} common header",
            self.path
        );
        let stored_len = span.stored_len as usize;
        let decoded_len = span.decoded_len as usize;
        ensure!(
            stored_len != 0 && decoded_len != 0,
            "{label} has an empty page span"
        );
        ensure!(
            stored_len <= max_decoded_bytes,
            "{label} declares {stored_len} stored bytes, above the {max_decoded_bytes}-byte guard"
        );
        ensure!(
            decoded_len <= max_decoded_bytes,
            "{label} declares {decoded_len} decoded bytes, above the {max_decoded_bytes}-byte guard"
        );
        let compressed = span.is_compressed();
        if !compressed {
            ensure!(
                stored_len == decoded_len,
                "raw {label} has {stored_len} bytes, expected {decoded_len}"
            );
        }
        let end = span
            .offset
            .checked_add(u64::from(span.stored_len))
            .context("canonical page extent overflow")?;
        ensure!(
            end <= self.file_len,
            "{label} ends at {end}, outside {} bytes for {}",
            self.file_len,
            self.path
        );
        let mut stored = vec![0_u8; stored_len];
        self.file
            .read_exact_at(&mut stored, span.offset)
            .with_context(|| format!("read {label} from {}", self.path))?;
        if compressed {
            decode_zstd_exact(&stored, decoded_len, label)
        } else {
            Ok(stored)
        }
    }

    fn read_effect_chunk(
        &self,
        offset: u64,
        frame: transactions::ChunkFrame,
        max_decoded_bytes: usize,
        label: &str,
    ) -> Result<Vec<u8>> {
        ensure!(!frame.is_empty(), "{label} has an empty chunk frame");
        ensure!(
            offset >= FILE_HEADER_LEN as u64,
            "{label} points into the {} common header",
            self.path
        );
        let stored_len = frame.stored_len() as usize;
        ensure!(
            stored_len <= max_decoded_bytes,
            "{label} declares {stored_len} stored bytes, above the {max_decoded_bytes}-byte guard"
        );
        let end = offset
            .checked_add(u64::from(frame.stored_len()))
            .context("effect chunk extent overflow")?;
        ensure!(
            end <= self.file_len,
            "{label} ends at {end}, outside {} bytes for {}",
            self.file_len,
            self.path
        );
        let mut stored = vec![0_u8; stored_len];
        self.file
            .read_exact_at(&mut stored, offset)
            .with_context(|| format!("read {label} from {}", self.path))?;
        if frame.is_raw() {
            return Ok(stored);
        }

        let decoded_len = zstd::zstd_safe::get_frame_content_size(&stored)
            .map_err(|_| anyhow::anyhow!("{label} has an invalid zstd frame header"))?
            .context("effect zstd frame does not declare its content size")?;
        let decoded_len = usize::try_from(decoded_len)
            .context("effect zstd content size does not fit in memory")?;
        ensure!(
            decoded_len <= max_decoded_bytes,
            "{label} declares {decoded_len} decoded bytes, above the {max_decoded_bytes}-byte guard"
        );
        decode_zstd_exact(&stored, decoded_len, label)
    }
}

/// One block decoded for replay. Runtime output is not opened or read.
#[derive(Debug)]
pub struct ReplayBlock {
    pub ordinal: u64,
    pub catalog: BlockRow,
    pub index: TransactionBlock,
    pub transactions: Vec<Transaction>,
}

/// Replay input plus its present CPI records, aligned by transaction index.
#[derive(Debug)]
pub struct IndexedBlock {
    pub replay: ReplayBlock,
    pub inner: Vec<Option<TransactionInner>>,
}

/// Replay input plus every transaction-scoped runtime effect.
///
/// `None` normally means no dense record. For vector effects,
/// `Some(Vec::new())` is a known-empty value. The Outcome bit also proves the
/// source metadata envelope. [`transactions::EffectState`] uses that proof to
/// distinguish omitted known-empty token balances and transaction rewards
/// from unavailable values. Logs remain independent. CPI has more states;
/// read the matching `replay.index.effect_states` entry to distinguish
/// unavailable, not-recorded, source-empty, and backfill-empty.
#[derive(Debug)]
pub struct FullBlock {
    pub replay: ReplayBlock,
    pub inner: Vec<Option<TransactionInner>>,
    pub outcomes: Vec<Option<TransactionOutcome>>,
    pub balances: Vec<Option<Balances>>,
    pub token_balances: Vec<Option<Vec<TokenBalance>>>,
    pub logs: Vec<Option<Vec<LogLine>>>,
    pub rewards: Vec<Option<Vec<Reward>>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct EffectReadStats {
    records: u64,
    decoded_bytes: u64,
}

/// Exact totals from a sequential validation of every runtime effect object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectAuditReport {
    pub archive_id: ArchiveId,
    pub blocks: u64,
    pub transactions: u64,
    pub records: [u64; transactions::EFFECT_KIND_COUNT],
    pub decoded_bytes: [u64; transactions::EFFECT_KIND_COUNT],
}

impl EffectAuditReport {
    pub const fn records_for(&self, kind: EffectKind) -> u64 {
        self.records[kind.index()]
    }

    pub const fn decoded_bytes_for(&self, kind: EffectKind) -> u64 {
        self.decoded_bytes[kind.index()]
    }
}

/// Fixed-address catalog and merged replay stream reader.
pub struct CanonicalReader {
    catalog: File,
    catalog_header: FileHeader,
    transactions: InputObject,
    max_decoded_bytes: usize,
}

impl CanonicalReader {
    pub fn open(root: &Path, max_decoded_bytes: usize) -> Result<Self> {
        ensure!(
            max_decoded_bytes != 0,
            "decode guard must be greater than zero"
        );
        let catalog_path = root.join(catalog_blocks::PATH);
        let catalog = File::open(&catalog_path)
            .with_context(|| format!("open {}", catalog_path.display()))?;
        let mut header_bytes = [0_u8; FILE_HEADER_LEN];
        catalog
            .read_exact_at(&mut header_bytes, 0)
            .context("read catalog common header")?;
        let initial = FileHeader::decode(&header_bytes).context("decode catalog common header")?;
        let catalog_header =
            validate_open_file(&catalog, catalog_blocks::PATH, initial.archive_id)?;
        ensure!(
            catalog_header.payload_bytes == catalog_header.decoded_bytes,
            "fixed-address catalog must remain raw"
        );
        let expected_bytes = catalog_header
            .record_count
            .checked_mul(catalog_blocks::ROW_LEN as u64)
            .context("catalog length overflow")?;
        ensure!(
            catalog_header.payload_bytes == expected_bytes,
            "catalog has {} payload bytes, expected {expected_bytes}",
            catalog_header.payload_bytes
        );
        let transactions = InputObject::open(root, transactions::PATH, catalog_header.archive_id)?;
        Ok(Self {
            catalog,
            catalog_header,
            transactions,
            max_decoded_bytes,
        })
    }

    pub fn archive_id(&self) -> ArchiveId {
        self.catalog_header.archive_id
    }

    pub fn block_count(&self) -> u64 {
        self.catalog_header.record_count
    }

    pub fn transaction_record_count(&self) -> u64 {
        self.transactions.header.record_count
    }

    fn read_catalog_row(&self, ordinal: u64) -> Result<BlockRow> {
        ensure!(
            ordinal < self.block_count(),
            "catalog block ordinal {ordinal} is outside {} rows",
            self.block_count()
        );
        let offset = ordinal
            .checked_mul(catalog_blocks::ROW_LEN as u64)
            .and_then(|offset| offset.checked_add(FILE_HEADER_LEN as u64))
            .context("catalog row offset overflow")?;
        let mut bytes = [0_u8; catalog_blocks::ROW_LEN];
        self.catalog
            .read_exact_at(&mut bytes, offset)
            .with_context(|| format!("read catalog block ordinal {ordinal}"))?;
        let row = BlockRow::decode(&bytes)
            .with_context(|| format!("decode catalog block ordinal {ordinal}"))?;
        row.validate_at(ordinal)
            .with_context(|| format!("validate catalog block ordinal {ordinal}"))?;
        Ok(row)
    }

    /// Read one ordinal-addressed row and validate its link to its predecessor.
    /// This reads at most two fixed 144-byte catalog rows.
    pub fn block_at(&self, ordinal: u64) -> Result<BlockRow> {
        let row = self.read_catalog_row(ordinal)?;
        if let Some(previous_ordinal) = ordinal.checked_sub(1) {
            let previous = self.read_catalog_row(previous_ordinal)?;
            catalog_blocks::validate_successor(previous, row)
                .with_context(|| format!("validate catalog link into block ordinal {ordinal}"))?;
        }
        Ok(row)
    }

    pub fn find_slot(&self, slot: u64) -> Result<Option<(u64, BlockRow)>> {
        let (mut low, mut high) = (0_u64, self.block_count());
        while low < high {
            let middle = low + (high - low) / 2;
            let row = self.block_at(middle)?;
            match row.slot.cmp(&slot) {
                std::cmp::Ordering::Equal => return Ok(Some((middle, row))),
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
            }
        }
        Ok(None)
    }

    fn read_replay_block_from_validated_row(
        &self,
        ordinal: u64,
        row: BlockRow,
    ) -> Result<ReplayBlock> {
        let label = format!("transaction block ordinal {ordinal}");
        let bytes =
            self.transactions
                .read_page(row.transactions, self.max_decoded_bytes, &label)?;
        let index = transactions::decode_block(&bytes, row.transaction_count)
            .with_context(|| format!("decode {label}"))?;
        let decoded =
            transactions::decode_transactions(&index.transaction_rows, row.transaction_count)
                .with_context(|| format!("decode transaction rows at block ordinal {ordinal}"))?;
        Ok(ReplayBlock {
            ordinal,
            catalog: row,
            index,
            transactions: decoded,
        })
    }

    /// Read one replay block by its canonical catalog ordinal.
    pub fn read_block(&self, ordinal: u64) -> Result<ReplayBlock> {
        let row = self.block_at(ordinal)?;
        self.read_replay_block_from_validated_row(ordinal, row)
    }

    pub fn read_slot(&self, slot: u64) -> Result<Option<ReplayBlock>> {
        let Some((ordinal, row)) = self.find_slot(slot)? else {
            return Ok(None);
        };
        self.read_replay_block_from_validated_row(ordinal, row)
            .map(Some)
    }

    /// Add CPI records for one already-decoded replay block. This opens and
    /// reads only the CPI effect object.
    pub fn read_indexed_block(&self, root: &Path, replay: ReplayBlock) -> Result<IndexedBlock> {
        let inner_object = InputObject::open(root, inner_instructions::PATH, self.archive_id())?;
        let (inner, _, _, _) =
            read_inner_for_block(&inner_object, &replay, self.max_decoded_bytes, None)?;
        Ok(IndexedBlock { replay, inner })
    }

    /// Decode every transaction-scoped runtime effect for one replay block.
    /// Normal replay reads do not call this method and do not open these files.
    pub fn read_full_block(&self, root: &Path, replay: ReplayBlock) -> Result<FullBlock> {
        let inner_object = InputObject::open(root, inner_instructions::PATH, self.archive_id())?;
        let outcome_object = InputObject::open(root, outcomes::PATH, self.archive_id())?;
        let balance_object = InputObject::open(root, balances::PATH, self.archive_id())?;
        let token_object = InputObject::open(root, token_balances::PATH, self.archive_id())?;
        let log_object = InputObject::open(root, logs::PATH, self.archive_id())?;
        let reward_object = InputObject::open(root, rewards::PATH, self.archive_id())?;

        let (inner, _, _, _) =
            read_inner_for_block(&inner_object, &replay, self.max_decoded_bytes, None)?;
        let (outcomes, _) = read_dense_effect_for_block(
            &outcome_object,
            &replay,
            EffectKind::Outcome,
            self.max_decoded_bytes,
            None,
            |bytes, count| Ok(outcomes::decode_chunk(bytes, count)?),
        )?;
        let (balances, _) = read_dense_effect_for_block(
            &balance_object,
            &replay,
            EffectKind::Balances,
            self.max_decoded_bytes,
            None,
            |bytes, count| Ok(balances::decode_chunk(bytes, count)?),
        )?;
        let (mut token_balances, _) = read_dense_effect_for_block(
            &token_object,
            &replay,
            EffectKind::TokenBalances,
            self.max_decoded_bytes,
            None,
            |bytes, count| Ok(token_balances::decode_chunk(bytes, count)?),
        )?;
        let (logs, _) = read_dense_effect_for_block(
            &log_object,
            &replay,
            EffectKind::Logs,
            self.max_decoded_bytes,
            None,
            |bytes, count| Ok(logs::decode_chunk(bytes, count)?),
        )?;
        let (mut rewards, _) = read_dense_effect_for_block(
            &reward_object,
            &replay,
            EffectKind::Rewards,
            self.max_decoded_bytes,
            None,
            |bytes, count| Ok(rewards::decode_chunk(bytes, count)?),
        )?;
        for (transaction_index, ((token_balances, rewards), state)) in token_balances
            .iter_mut()
            .zip(&mut rewards)
            .zip(&replay.index.effect_states)
            .enumerate()
        {
            if state.omitted_record_is_known_empty(EffectKind::TokenBalances)? {
                ensure!(
                    token_balances.is_none(),
                    "transaction {transaction_index} has a token-balance record and a known-empty state"
                );
                token_balances.get_or_insert_with(Vec::new);
            }
            if state.omitted_record_is_known_empty(EffectKind::Rewards)? {
                ensure!(
                    rewards.is_none(),
                    "transaction {transaction_index} has a reward record and a known-empty state"
                );
                rewards.get_or_insert_with(Vec::new);
            }
        }

        Ok(FullBlock {
            replay,
            inner,
            outcomes,
            balances,
            token_balances,
            logs,
            rewards,
        })
    }
}

/// Summary returned by a full canonical transaction/CPI scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalScanReport {
    pub archive_id: ArchiveId,
    pub blocks: u64,
    pub transactions: u64,
    pub top_level_instructions: u64,
    pub cpi_records: u64,
    pub cpi_instructions: u64,
}

const fn effect_path(kind: EffectKind) -> &'static str {
    match kind {
        EffectKind::InnerInstructions => inner_instructions::PATH,
        EffectKind::Outcome => outcomes::PATH,
        EffectKind::Balances => balances::PATH,
        EffectKind::TokenBalances => token_balances::PATH,
        EffectKind::Logs => logs::PATH,
        EffectKind::Rewards => rewards::PATH,
    }
}

fn read_dense_effect_for_block<T>(
    object: &InputObject,
    replay: &ReplayBlock,
    kind: EffectKind,
    max_decoded_bytes: usize,
    mut expected_next_offset: Option<&mut u64>,
    decode: impl Fn(&[u8], u32) -> Result<Vec<T>>,
) -> Result<(Vec<Option<T>>, EffectReadStats)> {
    let transaction_count = replay.transactions.len();
    let mut aligned: Vec<Option<T>> = std::iter::repeat_with(|| None)
        .take(transaction_count)
        .collect();
    let file_index = &replay.index.effect_files[kind.index()];
    let chunk_width = EFFECT_CHUNK_TRANSACTIONS as usize;
    let mut stats = EffectReadStats::default();

    for (chunk_index, frame) in file_index.chunks.iter().copied().enumerate() {
        let start = chunk_index
            .checked_mul(chunk_width)
            .context("effect chunk transaction offset overflow")?;
        let end = (start + chunk_width).min(transaction_count);
        let states = &replay.index.effect_states[start..end];
        let record_count = states.iter().try_fold(0_u32, |count, state| {
            state
                .has_record(kind)
                .map(|present| count + u32::from(present))
        })?;
        if record_count == 0 {
            ensure!(
                frame.is_empty(),
                "{kind:?} chunk {chunk_index} in block {} has bytes but no transaction owns a record",
                replay.ordinal
            );
            continue;
        }
        ensure!(
            !frame.is_empty(),
            "{kind:?} chunk {chunk_index} in block {} is empty but {record_count} transactions own records",
            replay.ordinal
        );
        let offset = file_index
            .chunk_offset(chunk_index)
            .with_context(|| format!("present {kind:?} chunk {chunk_index} has no file offset"))?;
        if let Some(expected) = expected_next_offset.as_deref_mut() {
            ensure!(
                offset == *expected,
                "{kind:?} chunk {chunk_index} in block {} starts at {offset}, expected {}",
                replay.ordinal,
                *expected
            );
        }
        let label = format!("{kind:?} chunk {chunk_index} in block {}", replay.ordinal);
        let bytes = object.read_effect_chunk(offset, frame, max_decoded_bytes, &label)?;
        stats.decoded_bytes = stats
            .decoded_bytes
            .checked_add(bytes.len() as u64)
            .context("effect decoded-byte count overflow")?;
        if let Some(expected) = expected_next_offset.as_deref_mut() {
            *expected = expected
                .checked_add(u64::from(frame.stored_len()))
                .context("effect object offset overflow")?;
        }
        let records = decode(&bytes, record_count).with_context(|| format!("decode {label}"))?;
        ensure!(
            records.len() == record_count as usize,
            "{kind:?} decoder returned {} records, expected {record_count}",
            records.len()
        );
        let mut records = records.into_iter();
        let mut dense_rank = 0_u32;
        for (relative, state) in states.iter().enumerate() {
            if !state.has_record(kind)? {
                continue;
            }
            let transaction_index = start + relative;
            let indexed_rank = replay
                .index
                .effect_rank(transaction_index as u32, kind)?
                .context("present effect state has no dense rank")?;
            ensure!(
                indexed_rank == dense_rank,
                "{kind:?} rank for transaction {transaction_index} is {indexed_rank}, expected {dense_rank}"
            );
            let record = records.next().context("dense effect record count drift")?;
            aligned[transaction_index] = Some(record);
            dense_rank = dense_rank
                .checked_add(1)
                .context("effect dense rank overflow")?;
            stats.records = stats
                .records
                .checked_add(1)
                .context("effect record count overflow")?;
        }
        ensure!(records.next().is_none(), "dense effect record count drift");
        ensure!(
            dense_rank == record_count,
            "{kind:?} dense rank ended at {dense_rank}, expected {record_count}"
        );
    }
    ensure!(
        stats.records <= object.header.record_count,
        "{} block has {} dense records but its header declares only {} for the generation",
        object.path,
        stats.records,
        object.header.record_count
    );
    Ok((aligned, stats))
}

fn read_inner_for_block(
    inner_object: &InputObject,
    replay: &ReplayBlock,
    max_decoded_bytes: usize,
    expected_next_offset: Option<&mut u64>,
) -> Result<(Vec<Option<TransactionInner>>, u64, u64, u64)> {
    let kind = EffectKind::InnerInstructions;
    let (aligned, stats) = read_dense_effect_for_block(
        inner_object,
        replay,
        kind,
        max_decoded_bytes,
        expected_next_offset,
        |bytes, record_count| Ok(inner_instructions::decode_chunk(bytes, record_count)?),
    )?;
    let mut instruction_total = 0_u64;
    for (transaction_index, record) in aligned.iter().enumerate() {
        if let Some(record) = record {
            let transaction = &replay.transactions[transaction_index];
            let accounts = ResolvedAccounts::new(transaction);
            record
                .validate(
                    transaction.message.instructions().len(),
                    accounts.resolved_len(),
                )
                .with_context(|| {
                    format!(
                        "validate CPI for transaction {}",
                        replay.catalog.first_transaction + transaction_index as u64
                    )
                })?;
            instruction_total =
                record
                    .groups
                    .iter()
                    .try_fold(instruction_total, |total, group| {
                        total
                            .checked_add(group.instructions.len() as u64)
                            .context("CPI instruction count overflow")
                    })?;
        }
    }
    Ok((
        aligned,
        stats.records,
        instruction_total,
        stats.decoded_bytes,
    ))
}

/// Scan the merged transaction stream and its CPI effect stream in block order.
///
/// The callback receives one block at a time. Memory is bounded by one decoded
/// transaction block, its CPI chunks, and the caller's own state.
pub fn scan_transactions_with_inner(
    root: &Path,
    max_decoded_bytes: usize,
    mut visit: impl FnMut(&IndexedBlock) -> Result<()>,
) -> Result<CanonicalScanReport> {
    let reader = CanonicalReader::open(root, max_decoded_bytes)?;
    let inner_object = InputObject::open(root, inner_instructions::PATH, reader.archive_id())?;
    let mut next_transaction_offset = FILE_HEADER_LEN as u64;
    let mut transaction_decoded_bytes = 0_u64;
    let mut next_inner_offset = FILE_HEADER_LEN as u64;
    let mut inner_decoded_bytes = 0_u64;
    let mut expected_transaction = 0_u64;
    let mut previous_row = None;
    let mut top_level_instructions = 0_u64;
    let mut cpi_records = 0_u64;
    let mut cpi_instructions = 0_u64;

    for ordinal in 0..reader.block_count() {
        let row = reader.read_catalog_row(ordinal)?;
        if let Some(previous) = previous_row {
            catalog_blocks::validate_successor(previous, row)
                .with_context(|| format!("validate catalog link into block ordinal {ordinal}"))?;
        }
        previous_row = Some(row);
        ensure!(
            row.first_transaction == expected_transaction,
            "block ordinal {ordinal} starts at transaction {}, expected {expected_transaction}",
            row.first_transaction
        );
        ensure!(
            row.transactions.offset == next_transaction_offset,
            "transaction block ordinal {ordinal} starts at {}, expected {next_transaction_offset}",
            row.transactions.offset
        );
        next_transaction_offset = row
            .transactions
            .offset
            .checked_add(u64::from(row.transactions.stored_len))
            .context("transaction object extent overflow")?;
        transaction_decoded_bytes = transaction_decoded_bytes
            .checked_add(u64::from(row.transactions.decoded_len))
            .context("transaction decoded-byte count overflow")?;
        let replay = reader.read_replay_block_from_validated_row(ordinal, row)?;
        top_level_instructions =
            replay
                .transactions
                .iter()
                .try_fold(top_level_instructions, |total, transaction| {
                    total
                        .checked_add(transaction.message.instructions().len() as u64)
                        .context("top-level instruction count overflow")
                })?;
        let (inner, block_cpi_records, block_cpi_instructions, block_inner_decoded_bytes) =
            read_inner_for_block(
                &inner_object,
                &replay,
                max_decoded_bytes,
                Some(&mut next_inner_offset),
            )?;
        cpi_records = cpi_records
            .checked_add(block_cpi_records)
            .context("CPI record count overflow")?;
        cpi_instructions = cpi_instructions
            .checked_add(block_cpi_instructions)
            .context("CPI instruction count overflow")?;
        inner_decoded_bytes = inner_decoded_bytes
            .checked_add(block_inner_decoded_bytes)
            .context("CPI decoded-byte count overflow")?;
        expected_transaction = replay.catalog.transactions_end()?;
        visit(&IndexedBlock { replay, inner })?;
    }

    ensure!(
        next_transaction_offset == reader.transactions.file_len,
        "catalog transaction spans end at {next_transaction_offset}, object ends at {}",
        reader.transactions.file_len
    );
    ensure!(
        next_inner_offset == inner_object.file_len,
        "CPI chunks end at {next_inner_offset}, object ends at {}",
        inner_object.file_len
    );
    ensure!(
        reader.transactions.header.record_count == expected_transaction,
        "transaction object header declares {} records, catalog declares {expected_transaction}",
        reader.transactions.header.record_count
    );
    ensure!(
        reader.transactions.header.decoded_bytes == transaction_decoded_bytes,
        "transaction object header declares {} decoded bytes, catalog declares {transaction_decoded_bytes}",
        reader.transactions.header.decoded_bytes
    );
    ensure!(
        inner_object.header.record_count == cpi_records,
        "CPI object header declares {} dense records, effect states declare {cpi_records}",
        inner_object.header.record_count
    );
    ensure!(
        inner_object.header.decoded_bytes == inner_decoded_bytes,
        "CPI object header declares {} decoded bytes, chunks decode to {inner_decoded_bytes}",
        inner_object.header.decoded_bytes
    );

    Ok(CanonicalScanReport {
        archive_id: reader.archive_id(),
        blocks: reader.block_count(),
        transactions: expected_transaction,
        top_level_instructions,
        cpi_records,
        cpi_instructions,
    })
}

/// Sequentially decode and validate every transaction-scoped effect object.
///
/// Point reads validate only the selected block. This audit also proves exact
/// generation-wide dense-record counts, decoded-byte totals, and contiguous
/// chunk coverage for all six effect files.
pub fn validate_all_effects(root: &Path, max_decoded_bytes: usize) -> Result<EffectAuditReport> {
    let reader = CanonicalReader::open(root, max_decoded_bytes)?;
    let objects = EffectKind::ALL
        .into_iter()
        .map(|kind| InputObject::open(root, effect_path(kind), reader.archive_id()))
        .collect::<Result<Vec<_>>>()?;
    let mut next_effect_offsets = [FILE_HEADER_LEN as u64; transactions::EFFECT_KIND_COUNT];
    let mut effect_records = [0_u64; transactions::EFFECT_KIND_COUNT];
    let mut effect_decoded_bytes = [0_u64; transactions::EFFECT_KIND_COUNT];
    let mut next_transaction_offset = FILE_HEADER_LEN as u64;
    let mut transaction_decoded_bytes = 0_u64;
    let mut expected_transaction = 0_u64;
    let mut previous_row = None;

    for ordinal in 0..reader.block_count() {
        let row = reader.read_catalog_row(ordinal)?;
        if let Some(previous) = previous_row {
            catalog_blocks::validate_successor(previous, row)
                .with_context(|| format!("validate catalog link into block ordinal {ordinal}"))?;
        }
        previous_row = Some(row);
        ensure!(
            row.first_transaction == expected_transaction,
            "block ordinal {ordinal} starts at transaction {}, expected {expected_transaction}",
            row.first_transaction
        );
        ensure!(
            row.transactions.offset == next_transaction_offset,
            "transaction block ordinal {ordinal} starts at {}, expected {next_transaction_offset}",
            row.transactions.offset
        );
        next_transaction_offset = row
            .transactions
            .offset
            .checked_add(u64::from(row.transactions.stored_len))
            .context("transaction object extent overflow")?;
        transaction_decoded_bytes = transaction_decoded_bytes
            .checked_add(u64::from(row.transactions.decoded_len))
            .context("transaction decoded-byte count overflow")?;
        let replay = reader.read_replay_block_from_validated_row(ordinal, row)?;

        let (_, inner_records, _, inner_bytes) = read_inner_for_block(
            &objects[EffectKind::InnerInstructions.index()],
            &replay,
            max_decoded_bytes,
            Some(&mut next_effect_offsets[EffectKind::InnerInstructions.index()]),
        )?;
        add_effect_totals(
            &mut effect_records,
            &mut effect_decoded_bytes,
            EffectKind::InnerInstructions,
            EffectReadStats {
                records: inner_records,
                decoded_bytes: inner_bytes,
            },
        )?;

        let (_, stats) = read_dense_effect_for_block(
            &objects[EffectKind::Outcome.index()],
            &replay,
            EffectKind::Outcome,
            max_decoded_bytes,
            Some(&mut next_effect_offsets[EffectKind::Outcome.index()]),
            |bytes, count| Ok(outcomes::decode_chunk(bytes, count)?),
        )?;
        add_effect_totals(
            &mut effect_records,
            &mut effect_decoded_bytes,
            EffectKind::Outcome,
            stats,
        )?;

        let (_, stats) = read_dense_effect_for_block(
            &objects[EffectKind::Balances.index()],
            &replay,
            EffectKind::Balances,
            max_decoded_bytes,
            Some(&mut next_effect_offsets[EffectKind::Balances.index()]),
            |bytes, count| Ok(balances::decode_chunk(bytes, count)?),
        )?;
        add_effect_totals(
            &mut effect_records,
            &mut effect_decoded_bytes,
            EffectKind::Balances,
            stats,
        )?;

        let (_, stats) = read_dense_effect_for_block(
            &objects[EffectKind::TokenBalances.index()],
            &replay,
            EffectKind::TokenBalances,
            max_decoded_bytes,
            Some(&mut next_effect_offsets[EffectKind::TokenBalances.index()]),
            |bytes, count| Ok(token_balances::decode_chunk(bytes, count)?),
        )?;
        add_effect_totals(
            &mut effect_records,
            &mut effect_decoded_bytes,
            EffectKind::TokenBalances,
            stats,
        )?;

        let (_, stats) = read_dense_effect_for_block(
            &objects[EffectKind::Logs.index()],
            &replay,
            EffectKind::Logs,
            max_decoded_bytes,
            Some(&mut next_effect_offsets[EffectKind::Logs.index()]),
            |bytes, count| Ok(logs::decode_chunk(bytes, count)?),
        )?;
        add_effect_totals(
            &mut effect_records,
            &mut effect_decoded_bytes,
            EffectKind::Logs,
            stats,
        )?;

        let (_, stats) = read_dense_effect_for_block(
            &objects[EffectKind::Rewards.index()],
            &replay,
            EffectKind::Rewards,
            max_decoded_bytes,
            Some(&mut next_effect_offsets[EffectKind::Rewards.index()]),
            |bytes, count| Ok(rewards::decode_chunk(bytes, count)?),
        )?;
        add_effect_totals(
            &mut effect_records,
            &mut effect_decoded_bytes,
            EffectKind::Rewards,
            stats,
        )?;
        expected_transaction = replay.catalog.transactions_end()?;
    }

    ensure!(
        next_transaction_offset == reader.transactions.file_len,
        "catalog transaction spans end at {next_transaction_offset}, object ends at {}",
        reader.transactions.file_len
    );
    ensure!(
        reader.transactions.header.record_count == expected_transaction,
        "transaction object header declares {} records, catalog declares {expected_transaction}",
        reader.transactions.header.record_count
    );
    ensure!(
        reader.transactions.header.decoded_bytes == transaction_decoded_bytes,
        "transaction object header declares {} decoded bytes, catalog declares {transaction_decoded_bytes}",
        reader.transactions.header.decoded_bytes
    );
    for kind in EffectKind::ALL {
        let index = kind.index();
        let object = &objects[index];
        ensure!(
            next_effect_offsets[index] == object.file_len,
            "{} chunks end at {}, object ends at {}",
            object.path,
            next_effect_offsets[index],
            object.file_len
        );
        ensure!(
            object.header.record_count == effect_records[index],
            "{} header declares {} dense records, effect states declare {}",
            object.path,
            object.header.record_count,
            effect_records[index]
        );
        ensure!(
            object.header.decoded_bytes == effect_decoded_bytes[index],
            "{} header declares {} decoded bytes, chunks decode to {}",
            object.path,
            object.header.decoded_bytes,
            effect_decoded_bytes[index]
        );
    }

    Ok(EffectAuditReport {
        archive_id: reader.archive_id(),
        blocks: reader.block_count(),
        transactions: expected_transaction,
        records: effect_records,
        decoded_bytes: effect_decoded_bytes,
    })
}

fn add_effect_totals(
    records: &mut [u64; transactions::EFFECT_KIND_COUNT],
    decoded_bytes: &mut [u64; transactions::EFFECT_KIND_COUNT],
    kind: EffectKind,
    stats: EffectReadStats,
) -> Result<()> {
    let index = kind.index();
    records[index] = records[index]
        .checked_add(stats.records)
        .context("effect audit record count overflow")?;
    decoded_bytes[index] = decoded_bytes[index]
        .checked_add(stats.decoded_bytes)
        .context("effect audit decoded-byte count overflow")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, os::unix::fs::FileExt};

    use blockzilla_archive_v3::{
        catalog::blocks::{self as catalog_blocks, BlockRow, PageSpan},
        ledger::transactions::{
            ChunkFrame, CpiState, EffectFileIndex, EffectState, HashOwner, HashRef, Instruction,
            Message, MessageHeader, PubkeyId, RowRestart,
        },
        runtime::{
            inner_instructions::{InnerGroup, InnerInstruction},
            outcomes::TransactionOutcome,
        },
    };
    use tempfile::tempdir;

    use crate::{
        container::write_payload,
        test_fixture::{FixtureBlock, write_merged_fixture},
        transaction_view::TransactionArenaEncoder,
    };

    use super::*;

    fn transaction() -> Transaction {
        Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 1,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::Legacy {
                static_accounts: vec![PubkeyId(1), PubkeyId(2)],
                instructions: vec![Instruction {
                    program_position: 1,
                    account_positions: vec![0],
                    data: vec![7],
                }],
            },
        }
    }

    fn write_transaction_catalog(
        root: &Path,
        archive_id: ArchiveId,
        transactions_to_write: &[Transaction],
        effect_states: Vec<EffectState>,
        effect_files: [EffectFileIndex; transactions::EFFECT_KIND_COUNT],
    ) {
        let mut transaction_rows = Vec::new();
        for transaction in transactions_to_write {
            transactions::append_transaction(&mut transaction_rows, transaction).unwrap();
        }
        let transaction_block = TransactionBlock {
            effect_states,
            row_restarts: vec![RowRestart {
                row_byte_offset: 0,
                signature_delta: 0,
            }],
            effect_files,
            transaction_rows,
        };
        let payload = transactions::encode_block(&transaction_block).unwrap();
        write_payload(
            root,
            transactions::PATH,
            archive_id,
            transactions_to_write.len() as u64,
            &payload,
        )
        .unwrap();
        let catalog = catalog_blocks::encode_table(&[BlockRow {
            slot: 100,
            parent_slot: 99,
            transaction_count: transactions_to_write.len() as u32,
            transactions: PageSpan {
                offset: FILE_HEADER_LEN as u64,
                stored_len: payload.len() as u32,
                decoded_len: payload.len() as u32,
            },
            ..BlockRow::default()
        }])
        .unwrap();
        write_payload(root, catalog_blocks::PATH, archive_id, 1, &catalog).unwrap();
    }

    fn write_effect(
        root: &Path,
        archive_id: ArchiveId,
        path: &'static str,
        record_count: u64,
        payload: &[u8],
    ) -> ChunkFrame {
        write_payload(root, path, archive_id, record_count, payload).unwrap();
        ChunkFrame::raw(payload.len() as u32).unwrap()
    }

    fn overwrite_catalog_row(root: &Path, ordinal: u64, row: BlockRow) {
        let offset = FILE_HEADER_LEN as u64 + ordinal * catalog_blocks::ROW_LEN as u64;
        let file = OpenOptions::new()
            .write(true)
            .open(root.join(catalog_blocks::PATH))
            .unwrap();
        file.write_all_at(&row.encode().unwrap(), offset).unwrap();
    }

    #[test]
    fn full_reader_and_audit_preserve_absent_and_recorded_empty() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([5; 16]);
        let transactions_to_write = vec![transaction(), transaction(), transaction()];

        let inner = TransactionInner {
            groups: vec![InnerGroup {
                parent_index: 0,
                instructions: vec![InnerInstruction {
                    stack_height: Some(2),
                    instruction: Instruction {
                        program_position: 1,
                        account_positions: vec![0],
                        data: vec![8],
                    },
                }],
            }],
        };
        let inner_payload = inner_instructions::encode_record(&inner, 1, 2).unwrap();
        let outcome = TransactionOutcome {
            error: None,
            fee: 0,
            compute_units_consumed: None,
            cost_units: None,
            return_data: None,
        };
        let mut outcome_payload = Vec::new();
        outcomes::append_record(&mut outcome_payload, &outcome).unwrap();
        outcomes::append_record(&mut outcome_payload, &outcome).unwrap();
        let unchanged_balances = Balances {
            pre: vec![10, 20],
            post: vec![10, 20],
        };
        let mut balance_payload = Vec::new();
        balances::append_record(&mut balance_payload, &unchanged_balances).unwrap();
        balances::append_record(&mut balance_payload, &unchanged_balances).unwrap();
        let log_payload = logs::encode_record(&[]).unwrap();
        let frames = [
            write_effect(
                root.path(),
                archive_id,
                inner_instructions::PATH,
                1,
                &inner_payload,
            ),
            write_effect(root.path(), archive_id, outcomes::PATH, 2, &outcome_payload),
            write_effect(root.path(), archive_id, balances::PATH, 2, &balance_payload),
            ChunkFrame::EMPTY,
            write_effect(root.path(), archive_id, logs::PATH, 1, &log_payload),
            ChunkFrame::EMPTY,
        ];
        write_payload(root.path(), token_balances::PATH, archive_id, 0, &[]).unwrap();
        write_payload(root.path(), rewards::PATH, archive_id, 0, &[]).unwrap();
        let effect_files = std::array::from_fn(|index| EffectFileIndex {
            first_chunk_offset: if frames[index].is_empty() {
                0
            } else {
                FILE_HEADER_LEN as u64
            },
            chunks: vec![frames[index]],
        });

        let mut not_recorded = EffectState::new(CpiState::NotRecorded);
        not_recorded.set_present(EffectKind::Outcome, true);
        not_recorded.set_present(EffectKind::Balances, true);
        let mut recorded_empty = EffectState::new(CpiState::SourceEmpty);
        for kind in [EffectKind::Outcome, EffectKind::Balances, EffectKind::Logs] {
            recorded_empty.set_present(kind, true);
        }
        let cpi_present = EffectState::new(CpiState::BackfillPresent);
        write_transaction_catalog(
            root.path(),
            archive_id,
            &transactions_to_write,
            vec![not_recorded, recorded_empty, cpi_present],
            effect_files,
        );

        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let outcome_object =
            InputObject::open(root.path(), outcomes::PATH, reader.archive_id()).unwrap();
        let oversized_effect = outcome_object
            .read_effect_chunk(
                FILE_HEADER_LEN as u64,
                ChunkFrame::raw(2).unwrap(),
                1,
                "corrupt effect span",
            )
            .unwrap_err();
        assert!(oversized_effect.to_string().contains("stored bytes"));
        let replay = reader.read_slot(100).unwrap().unwrap();
        let full = reader.read_full_block(root.path(), replay).unwrap();
        assert_eq!(
            full.replay.index.effect_states[0].cpi().unwrap(),
            CpiState::NotRecorded
        );
        assert_eq!(
            full.replay.index.effect_states[1].cpi().unwrap(),
            CpiState::SourceEmpty
        );
        assert!(full.inner[0].is_none());
        assert!(full.inner[1].is_none());
        assert_eq!(full.inner[2], Some(inner));
        assert!(full.outcomes[0].is_some());
        assert!(full.outcomes[1].is_some());
        assert_eq!(full.balances[0], Some(unchanged_balances.clone()));
        assert_eq!(full.balances[1], Some(unchanged_balances));
        assert!(full.token_balances[0].as_ref().unwrap().is_empty());
        assert!(full.token_balances[1].as_ref().unwrap().is_empty());
        assert!(full.token_balances[2].is_none());
        assert!(full.logs[0].is_none());
        assert!(full.logs[1].as_ref().unwrap().is_empty());
        assert!(full.rewards[0].as_ref().unwrap().is_empty());
        assert!(full.rewards[1].as_ref().unwrap().is_empty());
        assert!(full.rewards[2].is_none());

        let audit = validate_all_effects(root.path(), 1 << 20).unwrap();
        assert_eq!(audit.blocks, 1);
        assert_eq!(audit.transactions, 3);
        assert_eq!(audit.records, [1, 2, 2, 0, 1, 0]);
    }

    #[test]
    fn replay_read_does_not_open_unselected_effect_files() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([6; 16]);
        let transaction = transaction();
        let effect_files = std::array::from_fn(|_| EffectFileIndex {
            first_chunk_offset: 0,
            chunks: vec![ChunkFrame::EMPTY],
        });
        write_transaction_catalog(
            root.path(),
            archive_id,
            &[transaction],
            vec![EffectState::new(CpiState::Unavailable)],
            effect_files,
        );
        assert!(!root.path().join(inner_instructions::PATH).exists());
        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let oversized_page = reader
            .transactions
            .read_page(
                PageSpan {
                    offset: FILE_HEADER_LEN as u64,
                    stored_len: 2,
                    decoded_len: 2,
                },
                1,
                "corrupt transaction span",
            )
            .unwrap_err();
        assert!(oversized_page.to_string().contains("stored bytes"));
        let replay = reader.read_slot(100).unwrap().unwrap();
        assert_eq!(replay.transactions.len(), 1);
    }

    #[test]
    fn canonical_reader_decodes_split_transaction_frames_with_exact_indexes() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([9; 16]);
        let transactions_to_write = (0..512).map(|_| transaction()).collect::<Vec<_>>();
        let effect_states = vec![EffectState::new(CpiState::Unavailable); 512];
        let effect_files = std::array::from_fn(|_| EffectFileIndex {
            first_chunk_offset: 0,
            chunks: vec![ChunkFrame::EMPTY; 2],
        });
        let arena = TransactionArenaEncoder::new()
            .prepare(&transactions_to_write)
            .unwrap();
        let page = arena
            .into_page(effect_states.clone(), effect_files.clone())
            .unwrap();
        assert!(page.compressed);
        write_payload(
            root.path(),
            transactions::PATH,
            archive_id,
            transactions_to_write.len() as u64,
            &page.stored,
        )
        .unwrap();
        let catalog = catalog_blocks::encode_table(&[BlockRow {
            slot: 100,
            parent_slot: 99,
            transaction_count: transactions_to_write.len() as u32,
            transactions: PageSpan {
                offset: FILE_HEADER_LEN as u64,
                stored_len: page.stored.len() as u32,
                decoded_len: page.decoded_len,
            },
            ..BlockRow::default()
        }])
        .unwrap();
        write_payload(root.path(), catalog_blocks::PATH, archive_id, 1, &catalog).unwrap();

        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let replay = reader.read_slot(100).unwrap().unwrap();
        assert_eq!(replay.transactions, transactions_to_write);
        assert_eq!(replay.index.effect_states, effect_states);
        assert_eq!(replay.index.effect_files, effect_files);
    }

    #[test]
    fn point_reader_rejects_a_wrong_poh_catalog_ordinal() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        let effect_files = std::array::from_fn(|_| EffectFileIndex {
            first_chunk_offset: 0,
            chunks: vec![ChunkFrame::EMPTY],
        });
        write_transaction_catalog(
            root.path(),
            archive_id,
            &[transaction()],
            vec![EffectState::new(CpiState::Unavailable)],
            effect_files,
        );
        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let mut row = reader.block_at(0).unwrap();
        row.blockhash = HashRef {
            owner: HashOwner::PohBlockFinal,
            ordinal: 9,
        };
        drop(reader);
        overwrite_catalog_row(root.path(), 0, row);

        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let error = reader.block_at(0).unwrap_err();
        assert!(format!("{error:#}").contains("blockhash PoH block ordinal is 9, expected 0"));
    }

    #[test]
    fn point_and_sequential_readers_reject_a_broken_catalog_link() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([8; 16]);
        write_merged_fixture(
            root.path(),
            archive_id,
            archive_id,
            archive_id,
            2,
            vec![
                FixtureBlock {
                    slot: 100,
                    parent_slot: 99,
                    transactions: vec![transaction()],
                    inner: vec![None],
                },
                FixtureBlock {
                    slot: 102,
                    parent_slot: 100,
                    transactions: vec![transaction()],
                    inner: vec![None],
                },
            ],
        );
        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let mut second = reader.block_at(1).unwrap();
        second.parent_slot = 99;
        drop(reader);
        overwrite_catalog_row(root.path(), 1, second);

        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let point_error = reader.block_at(1).unwrap_err();
        assert!(format!("{point_error:#}").contains("expected 100"));

        let scan_error =
            scan_transactions_with_inner(root.path(), 1 << 20, |_| Ok(())).unwrap_err();
        assert!(format!("{scan_error:#}").contains("expected 100"));

        second.parent_slot = 100;
        second.previous_blockhash = HashRef {
            owner: HashOwner::NonPoh,
            ordinal: 77,
        };
        overwrite_catalog_row(root.path(), 1, second);
        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let hash_error = reader.block_at(1).unwrap_err();
        assert!(
            format!("{hash_error:#}")
                .contains("previous blockhash does not match the prior catalog row")
        );
    }
}
