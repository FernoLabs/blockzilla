//! Read-only Archive V2 account-projection speed oracle.
//!
//! This binary deliberately stops before Index Archive serialization. It uses
//! the SDK's ordered borrowed block reader, decodes the selected message and
//! metadata grammars, and measures the exact account projection that a basic
//! account sidecar needs. It never reads signature content, PoH records, or
//! blockhash sidecars.

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_format::{
    ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX, ARCHIVE_V2_TX_FLAG_HAS_ERROR,
    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
    ARCHIVE_V2_TX_FLAG_HAS_LOGS, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA, ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotInstruction, ArchiveV2HotInstructionData,
    ArchiveV2HotMessagePayload, ArchiveV2HotTxRow, ArchiveV2HotV0Message, CompactMessageHeader,
    CompactMetaV1, CompactPubkey,
};
use blockzilla_index_archive_convert::source_v2::{
    CompactV2MessageSchema, CompactV2MetadataSchema, decode_message_lane_with_schema,
    decode_metadata_lane, validate_v0_loaded_address_counts,
};
use blockzilla_read_sdk::{
    ArchiveReader, BorrowedDecodedBlock, HashVerification, MAX_ORDERED_PARALLEL_DECODE_WORKERS,
    OpenOptions, OrderedParallelBlockConfig, OrderedParallelBlockStats, PinnedLocalRangeSource,
    manifest::TrustedGenerationIdentity,
};
use serde::Serialize;

const MIB: f64 = 1024.0 * 1024.0;
const ROLE_SIGNER: u8 = 1 << 0;
const ROLE_WRITABLE: u8 = 1 << 1;
const ROLE_TOP_LEVEL_PROGRAM: u8 = 1 << 2;
const ROLE_CPI_PROGRAM: u8 = 1 << 3;

#[derive(Debug)]
struct Args {
    source: PathBuf,
    epoch: u64,
    slots_per_epoch: u64,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    workers: usize,
    prefix_blocks: usize,
}

#[derive(Debug, Default, Serialize)]
struct SourceCounts {
    blocks: u64,
    transactions: u64,
    compressed_bytes: u64,
    declared_uncompressed_bytes: u64,
    outer_owned_fallback_blocks: u64,
}

impl SourceCounts {
    fn merge(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.transactions = self.transactions.saturating_add(other.transactions);
        self.compressed_bytes = self.compressed_bytes.saturating_add(other.compressed_bytes);
        self.declared_uncompressed_bytes = self
            .declared_uncompressed_bytes
            .saturating_add(other.declared_uncompressed_bytes);
        self.outer_owned_fallback_blocks = self
            .outer_owned_fallback_blocks
            .saturating_add(other.outer_owned_fallback_blocks);
    }
}

#[derive(Debug, Default, Serialize)]
struct MessageCounts {
    decoded: u64,
    legacy: u64,
    v0: u64,
    v1: u64,
    raw_transaction_fallbacks: u64,
    decode_failures: u64,
}

impl MessageCounts {
    fn merge(&mut self, other: Self) {
        self.decoded = self.decoded.saturating_add(other.decoded);
        self.legacy = self.legacy.saturating_add(other.legacy);
        self.v0 = self.v0.saturating_add(other.v0);
        self.v1 = self.v1.saturating_add(other.v1);
        self.raw_transaction_fallbacks = self
            .raw_transaction_fallbacks
            .saturating_add(other.raw_transaction_fallbacks);
        self.decode_failures = self.decode_failures.saturating_add(other.decode_failures);
    }
}

#[derive(Debug, Default, Serialize)]
struct MetadataCounts {
    decoded: u64,
    absent: u64,
    raw_metadata_fallbacks: u64,
    decode_failures: u64,
    outcomes_success: u64,
    outcomes_failed: u64,
    outcomes_unavailable: u64,
}

impl MetadataCounts {
    fn merge(&mut self, other: Self) {
        self.decoded = self.decoded.saturating_add(other.decoded);
        self.absent = self.absent.saturating_add(other.absent);
        self.raw_metadata_fallbacks = self
            .raw_metadata_fallbacks
            .saturating_add(other.raw_metadata_fallbacks);
        self.decode_failures = self.decode_failures.saturating_add(other.decode_failures);
        self.outcomes_success = self.outcomes_success.saturating_add(other.outcomes_success);
        self.outcomes_failed = self.outcomes_failed.saturating_add(other.outcomes_failed);
        self.outcomes_unavailable = self
            .outcomes_unavailable
            .saturating_add(other.outcomes_unavailable);
    }
}

#[derive(Debug, Default, Serialize)]
struct AccountCounts {
    exact_coverage_transactions: u64,
    incomplete_coverage_transactions: u64,
    static_references: u64,
    loaded_writable_references: u64,
    loaded_readonly_references: u64,
    resolved_references: u64,
    registry_id_references: u64,
    raw_pubkey_references: u64,
    signer_role_references: u64,
    writable_role_references: u64,
    readonly_role_references: u64,
    top_level_program_role_references: u64,
    cpi_program_role_references: u64,
    invalid_registry_id_references: u64,
    invalid_account_indexes: u64,
}

impl AccountCounts {
    fn merge(&mut self, other: Self) {
        self.exact_coverage_transactions = self
            .exact_coverage_transactions
            .saturating_add(other.exact_coverage_transactions);
        self.incomplete_coverage_transactions = self
            .incomplete_coverage_transactions
            .saturating_add(other.incomplete_coverage_transactions);
        self.static_references = self
            .static_references
            .saturating_add(other.static_references);
        self.loaded_writable_references = self
            .loaded_writable_references
            .saturating_add(other.loaded_writable_references);
        self.loaded_readonly_references = self
            .loaded_readonly_references
            .saturating_add(other.loaded_readonly_references);
        self.resolved_references = self
            .resolved_references
            .saturating_add(other.resolved_references);
        self.registry_id_references = self
            .registry_id_references
            .saturating_add(other.registry_id_references);
        self.raw_pubkey_references = self
            .raw_pubkey_references
            .saturating_add(other.raw_pubkey_references);
        self.signer_role_references = self
            .signer_role_references
            .saturating_add(other.signer_role_references);
        self.writable_role_references = self
            .writable_role_references
            .saturating_add(other.writable_role_references);
        self.readonly_role_references = self
            .readonly_role_references
            .saturating_add(other.readonly_role_references);
        self.top_level_program_role_references = self
            .top_level_program_role_references
            .saturating_add(other.top_level_program_role_references);
        self.cpi_program_role_references = self
            .cpi_program_role_references
            .saturating_add(other.cpi_program_role_references);
        self.invalid_registry_id_references = self
            .invalid_registry_id_references
            .saturating_add(other.invalid_registry_id_references);
        self.invalid_account_indexes = self
            .invalid_account_indexes
            .saturating_add(other.invalid_account_indexes);
    }
}

#[derive(Debug, Default, Serialize)]
struct InstructionCounts {
    top_level: u64,
    top_level_account_position_references: u64,
    cpi: u64,
    cpi_account_position_references: u64,
    data_raw: u64,
    data_unknown_system: u64,
    data_unknown_vote: u64,
    data_typed: u64,
}

impl InstructionCounts {
    fn merge(&mut self, other: Self) {
        self.top_level = self.top_level.saturating_add(other.top_level);
        self.top_level_account_position_references = self
            .top_level_account_position_references
            .saturating_add(other.top_level_account_position_references);
        self.cpi = self.cpi.saturating_add(other.cpi);
        self.cpi_account_position_references = self
            .cpi_account_position_references
            .saturating_add(other.cpi_account_position_references);
        self.data_raw = self.data_raw.saturating_add(other.data_raw);
        self.data_unknown_system = self
            .data_unknown_system
            .saturating_add(other.data_unknown_system);
        self.data_unknown_vote = self
            .data_unknown_vote
            .saturating_add(other.data_unknown_vote);
        self.data_typed = self.data_typed.saturating_add(other.data_typed);
    }
}

#[derive(Debug, Default, Serialize)]
struct CoverageCounts {
    outcome_exact: u64,
    outcome_unavailable: u64,
    balances_exact: u64,
    balances_mismatch: u64,
    cpi_source_present: u64,
    cpi_source_empty: u64,
    cpi_not_recorded: u64,
    cpi_unavailable: u64,
}

impl CoverageCounts {
    fn merge(&mut self, other: Self) {
        self.outcome_exact = self.outcome_exact.saturating_add(other.outcome_exact);
        self.outcome_unavailable = self
            .outcome_unavailable
            .saturating_add(other.outcome_unavailable);
        self.balances_exact = self.balances_exact.saturating_add(other.balances_exact);
        self.balances_mismatch = self
            .balances_mismatch
            .saturating_add(other.balances_mismatch);
        self.cpi_source_present = self
            .cpi_source_present
            .saturating_add(other.cpi_source_present);
        self.cpi_source_empty = self.cpi_source_empty.saturating_add(other.cpi_source_empty);
        self.cpi_not_recorded = self.cpi_not_recorded.saturating_add(other.cpi_not_recorded);
        self.cpi_unavailable = self.cpi_unavailable.saturating_add(other.cpi_unavailable);
    }
}

#[derive(Debug, Default, Serialize)]
struct FailureCounts {
    transactions: u64,
    events: u64,
    flag_mismatches: u64,
    shape_failures: u64,
}

impl FailureCounts {
    fn merge(&mut self, other: Self) {
        self.transactions = self.transactions.saturating_add(other.transactions);
        self.events = self.events.saturating_add(other.events);
        self.flag_mismatches = self.flag_mismatches.saturating_add(other.flag_mismatches);
        self.shape_failures = self.shape_failures.saturating_add(other.shape_failures);
    }
}

#[derive(Debug, Default, Serialize)]
struct ScanCounts {
    source: SourceCounts,
    messages: MessageCounts,
    metadata: MetadataCounts,
    accounts: AccountCounts,
    instructions: InstructionCounts,
    coverage: CoverageCounts,
    strict_failures: FailureCounts,
}

impl ScanCounts {
    fn merge(&mut self, other: Self) {
        self.source.merge(other.source);
        self.messages.merge(other.messages);
        self.metadata.merge(other.metadata);
        self.accounts.merge(other.accounts);
        self.instructions.merge(other.instructions);
        self.coverage.merge(other.coverage);
        self.strict_failures.merge(other.strict_failures);
    }
}

#[derive(Debug, Default)]
struct ProjectionTimes {
    message_decode: Duration,
    metadata_decode: Duration,
    account_projection: Duration,
}

impl ProjectionTimes {
    fn merge(&mut self, other: Self) {
        self.message_decode = self.message_decode.saturating_add(other.message_decode);
        self.metadata_decode = self.metadata_decode.saturating_add(other.metadata_decode);
        self.account_projection = self
            .account_projection
            .saturating_add(other.account_projection);
    }
}

#[derive(Debug, Serialize)]
struct FailureSample {
    slot: u64,
    tx_index: u32,
    category: &'static str,
    message: String,
}

#[derive(Debug, Default)]
struct BlockProjection {
    counts: ScanCounts,
    times: ProjectionTimes,
    first_failure: Option<FailureSample>,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
}

impl BlockProjection {
    fn merge(&mut self, other: Self) {
        self.counts.merge(other.counts);
        self.times.merge(other.times);
        if self.first_failure.is_none() {
            self.first_failure = other.first_failure;
        }
        if self.first_slot.is_none() {
            self.first_slot = other.first_slot;
        }
        if other.last_slot.is_some() {
            self.last_slot = other.last_slot;
        }
    }
}

#[derive(Debug, Default)]
struct WorkerScratch {
    roles: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct ProjectionTimerReport {
    message_decode_sum_ns: u64,
    metadata_decode_sum_ns: u64,
    account_projection_sum_ns: u64,
}

#[derive(Debug, Serialize)]
struct ReaderTimerReport {
    producer_read_wall_ns: u64,
    coordinator_decode_project_wall_ns: u64,
    worker_outer_decode_sum_ns: u64,
    worker_projection_sum_ns: u64,
    producer_wait_for_free_buffer_ns: u64,
    coordinator_wait_for_ready_batch_ns: u64,
}

#[derive(Debug, Serialize)]
struct ReaderResourceReport {
    batches: u64,
    read_calls: u64,
    max_blocks_per_batch: usize,
    max_compressed_batch_bytes: usize,
    max_declared_uncompressed_batch_bytes: u64,
    max_retained_decompressed_buffer_bytes_per_worker: usize,
}

#[derive(Debug, Serialize)]
struct PhaseTimerReport {
    source_admission_ns: u64,
    block_pipeline_wall_ns: u64,
    pinned_identity_check_ns: u64,
    total_wall_ns: u64,
}

#[derive(Debug, Serialize)]
struct ThroughputReport {
    compressed_mib_per_second: f64,
    declared_uncompressed_mib_per_second: f64,
    blocks_per_second: f64,
    transactions_per_second: f64,
}

#[derive(Debug, Serialize)]
struct Report {
    kind: &'static str,
    status: &'static str,
    source: String,
    epoch: u64,
    slots_per_epoch: u64,
    message_schema: &'static str,
    metadata_schema: &'static str,
    workers: usize,
    archive_blocks: usize,
    requested_prefix_blocks: usize,
    processed_prefix_blocks: usize,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    counts: ScanCounts,
    projection_timers: ProjectionTimerReport,
    reader_timers: ReaderTimerReport,
    reader_resources: ReaderResourceReport,
    phase_timers: PhaseTimerReport,
    throughput: ThroughputReport,
    first_failure: Option<FailureSample>,
}

fn main() -> Result<()> {
    let total_started = Instant::now();
    let args = parse_args()?;

    let admission_started = Instant::now();
    let source = PinnedLocalRangeSource::new(&args.source);
    let reader = ArchiveReader::open_trusted(
        source.clone(),
        TrustedGenerationIdentity {
            cluster_id: "mainnet-beta".to_owned(),
            epoch: args.epoch,
            generation_id: format!("account-scan-bench-epoch-{}", args.epoch),
            slots_per_epoch: args.slots_per_epoch,
        },
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
    .context("open trusted Archive V2 source")?;
    let source_admission = admission_started.elapsed();

    let archive_blocks = reader.index().rows.len();
    let processed_prefix_blocks = args.prefix_blocks.min(archive_blocks);
    let requested_rows = &reader.index().rows[..processed_prefix_blocks];
    let planned_compressed_bytes = requested_rows.iter().fold(0_u64, |total, row| {
        total.saturating_add(u64::from(row.compressed_len))
    });
    let planned_uncompressed_bytes = requested_rows.iter().fold(0_u64, |total, row| {
        total.saturating_add(u64::from(row.uncompressed_len))
    });

    let message_schema = args.message_schema;
    let metadata_schema = args.metadata_schema;
    let registry_entries = reader.registry_entries();
    let mut aggregate = BlockProjection::default();
    let pipeline_started = Instant::now();
    let reader_stats = reader
        .process_borrowed_blocks_parallel_ordered(
            0..processed_prefix_blocks,
            OrderedParallelBlockConfig {
                decode_workers: args.workers,
                discard_rewards: true,
                ..OrderedParallelBlockConfig::default()
            },
            |_| Ok::<_, anyhow::Error>(WorkerScratch::default()),
            |scratch, _, block| {
                Ok::<_, anyhow::Error>(scan_block(
                    scratch,
                    block,
                    message_schema,
                    metadata_schema,
                    registry_entries,
                ))
            },
            |_, projected| {
                aggregate.merge(projected);
                Ok::<_, anyhow::Error>(())
            },
        )
        .context("scan borrowed Archive V2 blocks")?;
    let block_pipeline_wall = pipeline_started.elapsed();

    ensure!(
        reader_stats.block_count as usize == processed_prefix_blocks,
        "reader processed {} blocks, expected {processed_prefix_blocks}",
        reader_stats.block_count
    );
    ensure!(
        reader_stats.compressed_bytes == planned_compressed_bytes,
        "reader processed {} compressed bytes, expected {planned_compressed_bytes}",
        reader_stats.compressed_bytes
    );
    ensure!(
        aggregate.counts.source.declared_uncompressed_bytes == planned_uncompressed_bytes,
        "projection counted {} declared uncompressed bytes, expected {planned_uncompressed_bytes}",
        aggregate.counts.source.declared_uncompressed_bytes
    );

    let identity_check_started = Instant::now();
    source
        .verify_unchanged()
        .context("verify pinned source objects stayed unchanged")?;
    let pinned_identity_check = identity_check_started.elapsed();

    let pipeline_seconds = block_pipeline_wall.as_secs_f64();
    let throughput = ThroughputReport {
        compressed_mib_per_second: rate_mib(planned_compressed_bytes, pipeline_seconds),
        declared_uncompressed_mib_per_second: rate_mib(
            planned_uncompressed_bytes,
            pipeline_seconds,
        ),
        blocks_per_second: rate(processed_prefix_blocks as u64, pipeline_seconds),
        transactions_per_second: rate(aggregate.counts.source.transactions, pipeline_seconds),
    };
    let status = if aggregate.counts.strict_failures.events == 0 {
        "exact"
    } else {
        "incomplete"
    };
    let projection_timers = ProjectionTimerReport {
        message_decode_sum_ns: duration_ns(aggregate.times.message_decode),
        metadata_decode_sum_ns: duration_ns(aggregate.times.metadata_decode),
        account_projection_sum_ns: duration_ns(aggregate.times.account_projection),
    };
    let reader_timers = reader_timer_report(reader_stats);
    let reader_resources = reader_resource_report(reader_stats);
    let phase_timers = PhaseTimerReport {
        source_admission_ns: duration_ns(source_admission),
        block_pipeline_wall_ns: duration_ns(block_pipeline_wall),
        pinned_identity_check_ns: duration_ns(pinned_identity_check),
        total_wall_ns: duration_ns(total_started.elapsed()),
    };
    let strict_failure_events = aggregate.counts.strict_failures.events;
    let report = Report {
        kind: "archive-v2-account-scan-bench",
        status,
        source: args.source.display().to_string(),
        epoch: args.epoch,
        slots_per_epoch: args.slots_per_epoch,
        message_schema: message_schema_name(args.message_schema),
        metadata_schema: metadata_schema_name(args.metadata_schema),
        workers: args.workers,
        archive_blocks,
        requested_prefix_blocks: args.prefix_blocks,
        processed_prefix_blocks,
        first_slot: aggregate.first_slot,
        last_slot: aggregate.last_slot,
        counts: aggregate.counts,
        projection_timers,
        reader_timers,
        reader_resources,
        phase_timers,
        throughput,
        first_failure: aggregate.first_failure,
    };
    println!(
        "{}",
        serde_json::to_string(&report).context("encode scan report as JSON")?
    );
    ensure!(
        strict_failure_events == 0,
        "scan found {strict_failure_events} strict projection failure events"
    );
    Ok(())
}

fn scan_block(
    scratch: &mut WorkerScratch,
    block: BorrowedDecodedBlock<'_>,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    registry_entries: u32,
) -> BlockProjection {
    let mut projected = BlockProjection::default();
    let slot = block.header().slot;
    projected.first_slot = Some(slot);
    projected.last_slot = Some(slot);
    projected.counts.source.blocks = 1;
    projected.counts.source.transactions = u64::from(block.tx_count());
    projected.counts.source.compressed_bytes = u64::from(block.index_row.compressed_len);
    projected.counts.source.declared_uncompressed_bytes =
        u64::from(block.index_row.uncompressed_len);
    projected.counts.source.outer_owned_fallback_blocks = u64::from(block.uses_owned_fallback());

    let message_lane = block.message_bytes();
    let metadata_lane = block.metadata_bytes();
    for row in block.tx_rows() {
        scan_transaction(
            scratch,
            &mut projected,
            slot,
            row,
            message_lane,
            metadata_lane,
            message_schema,
            metadata_schema,
            registry_entries,
        );
    }
    projected
}

#[allow(clippy::too_many_arguments)]
fn scan_transaction(
    scratch: &mut WorkerScratch,
    projected: &mut BlockProjection,
    slot: u64,
    row: ArchiveV2HotTxRow,
    message_lane: &[u8],
    metadata_lane: &[u8],
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    registry_entries: u32,
) {
    let mut transaction_failed = false;

    let message = if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        projected.counts.messages.raw_transaction_fallbacks = projected
            .counts
            .messages
            .raw_transaction_fallbacks
            .saturating_add(1);
        record_failure(
            projected,
            &mut transaction_failed,
            slot,
            row.tx_index,
            "raw-transaction-fallback",
            || "raw transaction fallback has no exact structured message".to_owned(),
        );
        None
    } else {
        let started = Instant::now();
        let decoded = decode_message_lane_with_schema(message_schema, message_lane, &row);
        projected.times.message_decode = projected
            .times
            .message_decode
            .saturating_add(started.elapsed());
        match decoded {
            Ok(message) => {
                projected.counts.messages.decoded =
                    projected.counts.messages.decoded.saturating_add(1);
                match &message {
                    ArchiveV2HotMessagePayload::Legacy(_) => {
                        projected.counts.messages.legacy =
                            projected.counts.messages.legacy.saturating_add(1)
                    }
                    ArchiveV2HotMessagePayload::V0(_) => {
                        projected.counts.messages.v0 =
                            projected.counts.messages.v0.saturating_add(1)
                    }
                    ArchiveV2HotMessagePayload::V1(_) => {
                        projected.counts.messages.v1 =
                            projected.counts.messages.v1.saturating_add(1)
                    }
                }
                Some(message)
            }
            Err(error) => {
                projected.counts.messages.decode_failures =
                    projected.counts.messages.decode_failures.saturating_add(1);
                record_failure(
                    projected,
                    &mut transaction_failed,
                    slot,
                    row.tx_index,
                    "message-decode",
                    || error.to_string(),
                );
                None
            }
        }
    };

    let mut metadata_was_exactly_read = true;
    let metadata = if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        metadata_was_exactly_read = false;
        projected.counts.metadata.raw_metadata_fallbacks = projected
            .counts
            .metadata
            .raw_metadata_fallbacks
            .saturating_add(1);
        projected.counts.metadata.outcomes_unavailable = projected
            .counts
            .metadata
            .outcomes_unavailable
            .saturating_add(1);
        projected.counts.coverage.outcome_unavailable = projected
            .counts
            .coverage
            .outcome_unavailable
            .saturating_add(1);
        projected.counts.coverage.cpi_unavailable =
            projected.counts.coverage.cpi_unavailable.saturating_add(1);
        record_failure(
            projected,
            &mut transaction_failed,
            slot,
            row.tx_index,
            "raw-metadata-fallback",
            || "raw metadata fallback has no exact structured metadata".to_owned(),
        );
        None
    } else {
        let started = Instant::now();
        let decoded = decode_metadata_lane(metadata_schema, metadata_lane, &row);
        projected.times.metadata_decode = projected
            .times
            .metadata_decode
            .saturating_add(started.elapsed());
        match decoded {
            Ok(Some(metadata)) => {
                projected.counts.metadata.decoded =
                    projected.counts.metadata.decoded.saturating_add(1);
                projected.counts.coverage.outcome_exact =
                    projected.counts.coverage.outcome_exact.saturating_add(1);
                if metadata.err.is_some() {
                    projected.counts.metadata.outcomes_failed =
                        projected.counts.metadata.outcomes_failed.saturating_add(1);
                } else {
                    projected.counts.metadata.outcomes_success =
                        projected.counts.metadata.outcomes_success.saturating_add(1);
                }
                match metadata.inner_instructions.as_ref() {
                    Some(groups) if groups.is_empty() => {
                        projected.counts.coverage.cpi_source_empty =
                            projected.counts.coverage.cpi_source_empty.saturating_add(1)
                    }
                    Some(_) => {
                        projected.counts.coverage.cpi_source_present = projected
                            .counts
                            .coverage
                            .cpi_source_present
                            .saturating_add(1)
                    }
                    None => {
                        projected.counts.coverage.cpi_not_recorded =
                            projected.counts.coverage.cpi_not_recorded.saturating_add(1)
                    }
                }
                Some(metadata)
            }
            Ok(None) => {
                projected.counts.metadata.absent =
                    projected.counts.metadata.absent.saturating_add(1);
                projected.counts.metadata.outcomes_unavailable = projected
                    .counts
                    .metadata
                    .outcomes_unavailable
                    .saturating_add(1);
                projected.counts.coverage.outcome_unavailable = projected
                    .counts
                    .coverage
                    .outcome_unavailable
                    .saturating_add(1);
                projected.counts.coverage.cpi_unavailable =
                    projected.counts.coverage.cpi_unavailable.saturating_add(1);
                None
            }
            Err(error) => {
                metadata_was_exactly_read = false;
                projected.counts.metadata.decode_failures =
                    projected.counts.metadata.decode_failures.saturating_add(1);
                projected.counts.metadata.outcomes_unavailable = projected
                    .counts
                    .metadata
                    .outcomes_unavailable
                    .saturating_add(1);
                projected.counts.coverage.outcome_unavailable = projected
                    .counts
                    .coverage
                    .outcome_unavailable
                    .saturating_add(1);
                projected.counts.coverage.cpi_unavailable =
                    projected.counts.coverage.cpi_unavailable.saturating_add(1);
                record_failure(
                    projected,
                    &mut transaction_failed,
                    slot,
                    row.tx_index,
                    "metadata-decode",
                    || error.to_string(),
                );
                None
            }
        }
    };

    if let Some(message) = message.as_ref() {
        if metadata_was_exactly_read {
            let expected_flags = expected_transaction_flags(message, metadata.as_ref());
            if row.flags != expected_flags {
                projected.counts.strict_failures.flag_mismatches = projected
                    .counts
                    .strict_failures
                    .flag_mismatches
                    .saturating_add(1);
                record_failure(
                    projected,
                    &mut transaction_failed,
                    slot,
                    row.tx_index,
                    "transaction-flags",
                    || {
                        format!(
                            "stored flags {:#x} do not match decoded flags {expected_flags:#x}",
                            row.flags
                        )
                    },
                );
            }
        }

        let projection_started = Instant::now();
        project_accounts(
            scratch,
            projected,
            &mut transaction_failed,
            slot,
            row.tx_index,
            message,
            metadata.as_ref(),
            registry_entries,
        );
        projected.times.account_projection = projected
            .times
            .account_projection
            .saturating_add(projection_started.elapsed());
    } else {
        projected.counts.accounts.incomplete_coverage_transactions = projected
            .counts
            .accounts
            .incomplete_coverage_transactions
            .saturating_add(1);
    }

    if transaction_failed {
        projected.counts.strict_failures.transactions = projected
            .counts
            .strict_failures
            .transactions
            .saturating_add(1);
    }
}

#[allow(clippy::too_many_arguments)]
fn project_accounts(
    scratch: &mut WorkerScratch,
    projected: &mut BlockProjection,
    transaction_failed: &mut bool,
    slot: u64,
    tx_index: u32,
    message: &ArchiveV2HotMessagePayload,
    metadata: Option<&CompactMetaV1>,
    registry_entries: u32,
) {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => project_message_parts(
            scratch,
            projected,
            transaction_failed,
            slot,
            tx_index,
            &message.header,
            &message.account_keys,
            &message.instructions,
            None,
            metadata,
            registry_entries,
        ),
        ArchiveV2HotMessagePayload::V0(message) => project_message_parts(
            scratch,
            projected,
            transaction_failed,
            slot,
            tx_index,
            &message.header,
            &message.account_keys,
            &message.instructions,
            Some(message),
            metadata,
            registry_entries,
        ),
        ArchiveV2HotMessagePayload::V1(message) => project_message_parts(
            scratch,
            projected,
            transaction_failed,
            slot,
            tx_index,
            &message.header,
            &message.account_keys,
            &message.instructions,
            None,
            metadata,
            registry_entries,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn project_message_parts(
    scratch: &mut WorkerScratch,
    projected: &mut BlockProjection,
    transaction_failed: &mut bool,
    slot: u64,
    tx_index: u32,
    header: &CompactMessageHeader,
    static_accounts: &[CompactPubkey],
    instructions: &[ArchiveV2HotInstruction],
    v0: Option<&ArchiveV2HotV0Message>,
    metadata: Option<&CompactMetaV1>,
    registry_entries: u32,
) {
    let loaded_writable = metadata
        .map(|meta| meta.loaded_writable_addresses.as_slice())
        .unwrap_or_default();
    let loaded_readonly = metadata
        .map(|meta| meta.loaded_readonly_addresses.as_slice())
        .unwrap_or_default();
    let loaded_exact = match v0 {
        Some(message) => validate_v0_loaded_address_counts(
            message,
            metadata.map(|_| (loaded_writable, loaded_readonly)),
        )
        .map(|_| ()),
        None if loaded_writable.is_empty() && loaded_readonly.is_empty() => Ok(()),
        None => Err(blockzilla_index_archive_convert::source_v2::SourceV2Error::LoadedAddressesUnavailable {
            expected_writable: 0,
            expected_readonly: 0,
        }),
    };
    if let Err(error) = loaded_exact {
        projected.counts.accounts.incomplete_coverage_transactions = projected
            .counts
            .accounts
            .incomplete_coverage_transactions
            .saturating_add(1);
        projected.counts.strict_failures.shape_failures = projected
            .counts
            .strict_failures
            .shape_failures
            .saturating_add(1);
        record_failure(
            projected,
            transaction_failed,
            slot,
            tx_index,
            "loaded-address-coverage",
            || error.to_string(),
        );
        return;
    }

    let static_len = static_accounts.len();
    let writable_loaded_len = loaded_writable.len();
    let resolved_len = match static_len
        .checked_add(writable_loaded_len)
        .and_then(|count| count.checked_add(loaded_readonly.len()))
    {
        Some(count) => count,
        None => {
            projection_shape_failure(
                projected,
                transaction_failed,
                slot,
                tx_index,
                "resolved account count overflows this platform",
            );
            return;
        }
    };

    let required = usize::from(header.num_required_signatures);
    let readonly_signed = usize::from(header.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(header.num_readonly_unsigned_accounts);
    if required > static_len
        || readonly_signed > required
        || readonly_unsigned > static_len.saturating_sub(required)
    {
        projection_shape_failure(
            projected,
            transaction_failed,
            slot,
            tx_index,
            "message header account partitions are invalid",
        );
        projected.counts.accounts.incomplete_coverage_transactions = projected
            .counts
            .accounts
            .incomplete_coverage_transactions
            .saturating_add(1);
        return;
    }

    scratch.roles.clear();
    scratch.roles.resize(resolved_len, 0);
    let writable_signed_end = required - readonly_signed;
    let writable_unsigned_end = static_len - readonly_unsigned;
    for (position, roles) in scratch.roles.iter_mut().enumerate() {
        if position < required {
            *roles |= ROLE_SIGNER;
            if position < writable_signed_end {
                *roles |= ROLE_WRITABLE;
            }
        } else if position < static_len {
            if position < writable_unsigned_end {
                *roles |= ROLE_WRITABLE;
            }
        } else if position < static_len + writable_loaded_len {
            *roles |= ROLE_WRITABLE;
        }
    }

    let mut account_indexes_valid = true;
    for instruction in instructions {
        projected.counts.instructions.top_level =
            projected.counts.instructions.top_level.saturating_add(1);
        projected
            .counts
            .instructions
            .top_level_account_position_references = projected
            .counts
            .instructions
            .top_level_account_position_references
            .saturating_add(instruction.accounts.len() as u64);
        count_instruction_data(&mut projected.counts.instructions, &instruction.data);
        let program_position = usize::from(instruction.program_id_index);
        if let Some(roles) = scratch.roles.get_mut(program_position) {
            *roles |= ROLE_TOP_LEVEL_PROGRAM;
        } else {
            account_indexes_valid = false;
            projected.counts.accounts.invalid_account_indexes = projected
                .counts
                .accounts
                .invalid_account_indexes
                .saturating_add(1);
        }
        for &position in &instruction.accounts {
            if usize::from(position) >= resolved_len {
                account_indexes_valid = false;
                projected.counts.accounts.invalid_account_indexes = projected
                    .counts
                    .accounts
                    .invalid_account_indexes
                    .saturating_add(1);
            }
        }
    }

    if let Some(groups) = metadata.and_then(|meta| meta.inner_instructions.as_ref()) {
        for group in groups {
            if usize::try_from(group.index)
                .ok()
                .is_none_or(|index| index >= instructions.len())
            {
                account_indexes_valid = false;
                projected.counts.accounts.invalid_account_indexes = projected
                    .counts
                    .accounts
                    .invalid_account_indexes
                    .saturating_add(1);
            }
            for instruction in &group.instructions {
                projected.counts.instructions.cpi =
                    projected.counts.instructions.cpi.saturating_add(1);
                projected
                    .counts
                    .instructions
                    .cpi_account_position_references = projected
                    .counts
                    .instructions
                    .cpi_account_position_references
                    .saturating_add(instruction.accounts.len() as u64);
                let program_position = instruction.program_id_index as usize;
                if let Some(roles) = scratch.roles.get_mut(program_position) {
                    *roles |= ROLE_CPI_PROGRAM;
                } else {
                    account_indexes_valid = false;
                    projected.counts.accounts.invalid_account_indexes = projected
                        .counts
                        .accounts
                        .invalid_account_indexes
                        .saturating_add(1);
                }
                for &position in &instruction.accounts {
                    if usize::from(position) >= resolved_len {
                        account_indexes_valid = false;
                        projected.counts.accounts.invalid_account_indexes = projected
                            .counts
                            .accounts
                            .invalid_account_indexes
                            .saturating_add(1);
                    }
                }
            }
        }
    }
    if !account_indexes_valid {
        projection_shape_failure(
            projected,
            transaction_failed,
            slot,
            tx_index,
            "an instruction account position is outside the resolved account list",
        );
    }

    if let Some(metadata) = metadata {
        if metadata.pre_balances.len() == resolved_len
            && metadata.post_balances.len() == resolved_len
        {
            projected.counts.coverage.balances_exact =
                projected.counts.coverage.balances_exact.saturating_add(1);
        } else {
            projected.counts.coverage.balances_mismatch = projected
                .counts
                .coverage
                .balances_mismatch
                .saturating_add(1);
            projection_shape_failure(
                projected,
                transaction_failed,
                slot,
                tx_index,
                "pre/post balance widths do not match resolved accounts",
            );
            account_indexes_valid = false;
        }
    }

    let mut registry_id_references = 0_u64;
    let mut raw_pubkey_references = 0_u64;
    let mut pubkeys_valid = true;
    for key in static_accounts
        .iter()
        .chain(loaded_writable)
        .chain(loaded_readonly)
    {
        match key {
            CompactPubkey::Id(id) if *id > 0 && *id <= registry_entries => {
                registry_id_references = registry_id_references.saturating_add(1);
            }
            CompactPubkey::Id(_) => {
                pubkeys_valid = false;
                projected.counts.accounts.invalid_registry_id_references = projected
                    .counts
                    .accounts
                    .invalid_registry_id_references
                    .saturating_add(1);
            }
            CompactPubkey::Raw(_) => {
                raw_pubkey_references = raw_pubkey_references.saturating_add(1);
            }
        }
    }
    if !pubkeys_valid {
        projection_shape_failure(
            projected,
            transaction_failed,
            slot,
            tx_index,
            "a public-key registry id is outside the source registry",
        );
    }

    if !account_indexes_valid || !pubkeys_valid {
        projected.counts.accounts.incomplete_coverage_transactions = projected
            .counts
            .accounts
            .incomplete_coverage_transactions
            .saturating_add(1);
        return;
    }

    projected.counts.accounts.exact_coverage_transactions = projected
        .counts
        .accounts
        .exact_coverage_transactions
        .saturating_add(1);
    projected.counts.accounts.static_references = projected
        .counts
        .accounts
        .static_references
        .saturating_add(static_len as u64);
    projected.counts.accounts.loaded_writable_references = projected
        .counts
        .accounts
        .loaded_writable_references
        .saturating_add(writable_loaded_len as u64);
    projected.counts.accounts.loaded_readonly_references = projected
        .counts
        .accounts
        .loaded_readonly_references
        .saturating_add(loaded_readonly.len() as u64);
    projected.counts.accounts.resolved_references = projected
        .counts
        .accounts
        .resolved_references
        .saturating_add(resolved_len as u64);
    projected.counts.accounts.registry_id_references = projected
        .counts
        .accounts
        .registry_id_references
        .saturating_add(registry_id_references);
    projected.counts.accounts.raw_pubkey_references = projected
        .counts
        .accounts
        .raw_pubkey_references
        .saturating_add(raw_pubkey_references);

    let roles = role_counts(&scratch.roles);
    projected.counts.accounts.signer_role_references = projected
        .counts
        .accounts
        .signer_role_references
        .saturating_add(roles.signer);
    projected.counts.accounts.writable_role_references = projected
        .counts
        .accounts
        .writable_role_references
        .saturating_add(roles.writable);
    projected.counts.accounts.readonly_role_references = projected
        .counts
        .accounts
        .readonly_role_references
        .saturating_add(roles.readonly);
    projected.counts.accounts.top_level_program_role_references = projected
        .counts
        .accounts
        .top_level_program_role_references
        .saturating_add(roles.top_level_program);
    projected.counts.accounts.cpi_program_role_references = projected
        .counts
        .accounts
        .cpi_program_role_references
        .saturating_add(roles.cpi_program);
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RoleCounts {
    signer: u64,
    writable: u64,
    readonly: u64,
    top_level_program: u64,
    cpi_program: u64,
}

fn role_counts(roles: &[u8]) -> RoleCounts {
    let mut counts = RoleCounts::default();
    for roles in roles {
        counts.signer = counts
            .signer
            .saturating_add(u64::from(roles & ROLE_SIGNER != 0));
        counts.writable = counts
            .writable
            .saturating_add(u64::from(roles & ROLE_WRITABLE != 0));
        counts.readonly = counts
            .readonly
            .saturating_add(u64::from(roles & ROLE_WRITABLE == 0));
        counts.top_level_program = counts
            .top_level_program
            .saturating_add(u64::from(roles & ROLE_TOP_LEVEL_PROGRAM != 0));
        counts.cpi_program = counts
            .cpi_program
            .saturating_add(u64::from(roles & ROLE_CPI_PROGRAM != 0));
    }
    counts
}

fn count_instruction_data(counts: &mut InstructionCounts, data: &ArchiveV2HotInstructionData) {
    match data {
        ArchiveV2HotInstructionData::Raw(_) => counts.data_raw = counts.data_raw.saturating_add(1),
        ArchiveV2HotInstructionData::UnknownSystem(_) => {
            counts.data_unknown_system = counts.data_unknown_system.saturating_add(1)
        }
        ArchiveV2HotInstructionData::UnknownVote(_) => {
            counts.data_unknown_vote = counts.data_unknown_vote.saturating_add(1)
        }
        ArchiveV2HotInstructionData::ComputeBudget(_)
        | ArchiveV2HotInstructionData::System(_)
        | ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(_)
        | ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch { .. }
        | ArchiveV2HotInstructionData::VoteTowerSync(_)
        | ArchiveV2HotInstructionData::VoteTowerSyncSwitch { .. } => {
            counts.data_typed = counts.data_typed.saturating_add(1)
        }
    }
}

fn expected_transaction_flags(
    message: &ArchiveV2HotMessagePayload,
    metadata: Option<&CompactMetaV1>,
) -> u32 {
    let instructions = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => message.instructions.as_slice(),
        ArchiveV2HotMessagePayload::V0(message) => message.instructions.as_slice(),
        ArchiveV2HotMessagePayload::V1(message) => message.instructions.as_slice(),
    };
    let has_compact_vote = instructions.iter().any(|instruction| {
        matches!(
            instruction.data,
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(_)
                | ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch { .. }
                | ArchiveV2HotInstructionData::VoteTowerSync(_)
                | ArchiveV2HotInstructionData::VoteTowerSyncSwitch { .. }
        )
    });
    let mut expected = 0_u32;
    if matches!(message, ArchiveV2HotMessagePayload::V0(_)) {
        expected |= ARCHIVE_V2_TX_FLAG_MESSAGE_V0;
    }
    if has_compact_vote {
        expected |= ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX;
    }
    if let Some(metadata) = metadata {
        expected |= ARCHIVE_V2_TX_FLAG_HAS_METADATA;
        if metadata.err.is_some() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_ERROR;
        }
        if metadata.return_data.is_some() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA;
        }
        if metadata.logs.is_some() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_LOGS;
        }
        if metadata.inner_instructions.is_some() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
        }
        if !metadata.pre_token_balances.is_empty() || !metadata.post_token_balances.is_empty() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
        }
        if !metadata.loaded_writable_addresses.is_empty()
            || !metadata.loaded_readonly_addresses.is_empty()
        {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
        }
    }
    expected
}

fn record_failure(
    projected: &mut BlockProjection,
    transaction_failed: &mut bool,
    slot: u64,
    tx_index: u32,
    category: &'static str,
    message: impl FnOnce() -> String,
) {
    *transaction_failed = true;
    projected.counts.strict_failures.events =
        projected.counts.strict_failures.events.saturating_add(1);
    if projected.first_failure.is_none() {
        projected.first_failure = Some(FailureSample {
            slot,
            tx_index,
            category,
            message: message(),
        });
    }
}

fn projection_shape_failure(
    projected: &mut BlockProjection,
    transaction_failed: &mut bool,
    slot: u64,
    tx_index: u32,
    message: &'static str,
) {
    projected.counts.strict_failures.shape_failures = projected
        .counts
        .strict_failures
        .shape_failures
        .saturating_add(1);
    record_failure(
        projected,
        transaction_failed,
        slot,
        tx_index,
        "account-shape",
        || message.to_owned(),
    );
}

fn reader_timer_report(stats: OrderedParallelBlockStats) -> ReaderTimerReport {
    ReaderTimerReport {
        producer_read_wall_ns: duration_ns(stats.producer_read_wall_time),
        coordinator_decode_project_wall_ns: duration_ns(stats.coordinator_decode_project_wall_time),
        worker_outer_decode_sum_ns: duration_ns(stats.worker_decompress_decode_sum_time),
        worker_projection_sum_ns: duration_ns(stats.worker_projection_sum_time),
        producer_wait_for_free_buffer_ns: duration_ns(stats.producer_wait_for_free_buffer_time),
        coordinator_wait_for_ready_batch_ns: duration_ns(
            stats.coordinator_wait_for_ready_batch_time,
        ),
    }
}

fn reader_resource_report(stats: OrderedParallelBlockStats) -> ReaderResourceReport {
    ReaderResourceReport {
        batches: stats.batch_count,
        read_calls: stats.read_call_count,
        max_blocks_per_batch: stats.max_blocks_per_batch,
        max_compressed_batch_bytes: stats.max_compressed_batch_bytes,
        max_declared_uncompressed_batch_bytes: stats.max_declared_uncompressed_batch_bytes,
        max_retained_decompressed_buffer_bytes_per_worker: stats
            .max_retained_decompressed_buffer_bytes,
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn rate(value: u64, seconds: f64) -> f64 {
    if seconds > 0.0 {
        value as f64 / seconds
    } else {
        0.0
    }
}

fn rate_mib(bytes: u64, seconds: f64) -> f64 {
    rate(bytes, seconds) / MIB
}

fn message_schema_name(schema: CompactV2MessageSchema) -> &'static str {
    match schema {
        CompactV2MessageSchema::Current => "current",
        CompactV2MessageSchema::May24PreUnknownFallbacks => "may24",
    }
}

fn metadata_schema_name(schema: CompactV2MetadataSchema) -> &'static str {
    match schema {
        CompactV2MetadataSchema::CurrentTypedError => "current",
        CompactV2MetadataSchema::LegacyRawError => "legacy-raw",
    }
}

fn parse_args() -> Result<Args> {
    let mut source = None;
    let mut epoch = None;
    let mut slots_per_epoch = None;
    let mut message_schema = None;
    let mut metadata_schema = None;
    let mut workers = None;
    let mut prefix_blocks = None;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--source" => {
                ensure!(source.is_none(), "source directory was supplied twice");
                source = Some(PathBuf::from(
                    args.next().context("--source requires a directory")?,
                ));
            }
            "--epoch" => {
                epoch = Some(parse_next(&mut args, "--epoch")?);
            }
            "--slots-per-epoch" => {
                slots_per_epoch = Some(parse_next(&mut args, "--slots-per-epoch")?);
            }
            "--message-schema" => {
                message_schema = Some(parse_message_schema(
                    &args
                        .next()
                        .context("--message-schema requires current or may24")?,
                )?);
            }
            "--metadata-schema" => {
                metadata_schema =
                    Some(parse_metadata_schema(&args.next().context(
                        "--metadata-schema requires current or legacy-raw",
                    )?)?);
            }
            "--workers" => {
                workers = Some(parse_next(&mut args, "--workers")?);
            }
            "--prefix-blocks" => {
                prefix_blocks = Some(parse_next(&mut args, "--prefix-blocks")?);
            }
            "-h" | "--help" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            _ if argument.starts_with('-') => bail!("unknown option {argument}\n{}", usage()),
            _ => {
                ensure!(source.is_none(), "source directory was supplied twice");
                source = Some(PathBuf::from(argument));
            }
        }
    }
    let workers = workers.context("--workers is required")?;
    let prefix_blocks = prefix_blocks.context("--prefix-blocks is required")?;
    ensure!(workers > 0, "--workers must be greater than zero");
    ensure!(
        workers <= MAX_ORDERED_PARALLEL_DECODE_WORKERS,
        "--workers must not exceed {MAX_ORDERED_PARALLEL_DECODE_WORKERS}"
    );
    ensure!(
        prefix_blocks > 0,
        "--prefix-blocks must be greater than zero"
    );
    let slots_per_epoch = slots_per_epoch.context("--slots-per-epoch is required")?;
    ensure!(
        slots_per_epoch > 0,
        "--slots-per-epoch must be greater than zero"
    );
    Ok(Args {
        source: source.with_context(usage)?,
        epoch: epoch.context("--epoch is required")?,
        slots_per_epoch,
        message_schema: message_schema.context("--message-schema is required")?,
        metadata_schema: metadata_schema.context("--metadata-schema is required")?,
        workers,
        prefix_blocks,
    })
}

fn parse_next<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    option: &'static str,
) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    args.next()
        .with_context(|| format!("{option} requires a value"))?
        .parse()
        .with_context(|| format!("{option} has an invalid value"))
}

fn parse_message_schema(value: &str) -> Result<CompactV2MessageSchema> {
    match value {
        "current" => Ok(CompactV2MessageSchema::Current),
        "may24" | "may24-pre-unknown-fallbacks" => {
            Ok(CompactV2MessageSchema::May24PreUnknownFallbacks)
        }
        _ => bail!("unsupported message schema {value}; use current or may24"),
    }
}

fn parse_metadata_schema(value: &str) -> Result<CompactV2MetadataSchema> {
    match value {
        "current" | "current-typed-error" => Ok(CompactV2MetadataSchema::CurrentTypedError),
        "legacy-raw" | "legacy-raw-error" => Ok(CompactV2MetadataSchema::LegacyRawError),
        _ => bail!("unsupported metadata schema {value}; use current or legacy-raw"),
    }
}

fn usage() -> &'static str {
    "usage: archive-v2-account-scan-bench <archive-v2-dir> \\
     --epoch N --slots-per-epoch N \\
     --message-schema current|may24 \\
     --metadata-schema current|legacy-raw \\
     --workers N --prefix-blocks N"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_aliases_select_exact_grammars() {
        assert_eq!(
            parse_message_schema("may24").unwrap(),
            CompactV2MessageSchema::May24PreUnknownFallbacks
        );
        assert_eq!(
            parse_metadata_schema("legacy-raw").unwrap(),
            CompactV2MetadataSchema::LegacyRawError
        );
    }

    #[test]
    fn role_counter_counts_each_resolved_position_once_per_role() {
        let roles = [
            ROLE_SIGNER | ROLE_WRITABLE,
            ROLE_SIGNER,
            ROLE_WRITABLE | ROLE_TOP_LEVEL_PROGRAM,
            ROLE_CPI_PROGRAM,
        ];
        assert_eq!(
            role_counts(&roles),
            RoleCounts {
                signer: 2,
                writable: 2,
                readonly: 2,
                top_level_program: 1,
                cpi_program: 1,
            }
        );
    }
}
