//! Exact, read-only two-pass audit of one slot range in a frozen raw-shred journal prefix.

#[allow(dead_code)]
#[path = "shred_epoch_audit/repair_wal.rs"]
mod repair_wal;

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    fs,
    io::{self, Write},
    mem::size_of,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use hivezilla::ingest::{
    FecThresholdDeficit, LogicalKey, ShredKind, SpoolJournalIdentity, SpoolLocation, SpoolRecord,
    ZSTD_SOLANA_SHRED_V1, decode_stored_shred, deshred_complete_data_slot,
    read_spool_committed_snapshot_after, read_spool_record, recover_slot_data_shreds,
    shred_reconstruction_failure_category, spool_journal_dir_path,
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use solana_ledger::shred::Shred;

use repair_wal::{
    REPAIR_RECORD_MEMORY_OVERHEAD_BYTES, RepairMergeFailure, RepairMergeStats, RepairWalScanConfig,
    RepairWalScanReport, ValidatedRepairRecord, ensure_same_repair_prefix, merge_repair_records,
    scan_repair_wal_prefix,
};

const REPORT_SCHEMA_VERSION: u32 = 1;
const SLOT_BUFFER_OVERHEAD_BYTES: u64 = 256;
const DATAGRAM_VECTOR_OVERHEAD_BYTES: u64 = size_of::<Vec<u8>>() as u64;
// Recovery temporarily holds normalized shreds, erasure shards, recovered data and component bytes.
const RECOVERY_WORKING_SET_MULTIPLIER: u64 = 8;
const MAX_ERROR_CHARS: usize = 512;
const NO_SHREDS_OBSERVED: &str = "no_shreds_observed";

#[derive(Debug, Parser)]
#[command(about = "Exact two-pass audit of a frozen raw-shred epoch journal prefix")]
struct Args {
    #[arg(long)]
    spool_root: PathBuf,
    #[arg(long)]
    cluster_id: String,
    #[arg(long)]
    origin_node_id: String,
    #[arg(long)]
    source_id: String,
    #[arg(long, value_parser = parse_journal_id)]
    journal_id: [u8; 16],
    /// Immutable upper observation-sequence boundary used by both passes.
    #[arg(long)]
    durable_through_sequence: u64,
    #[arg(long)]
    min_slot: u64,
    #[arg(long)]
    max_slot: u64,
    /// Independently established first slot of complete capture. Never infer this from WAL edges.
    #[arg(long)]
    coverage_start_slot: u64,
    /// Independently established last slot of complete capture.
    #[arg(long)]
    coverage_end_slot: u64,
    /// Optional first-pass and second-pass resume anchor. All three fields are required together.
    #[arg(
        long,
        requires_all = [
            "after_frame_offset",
            "after_frame_len",
            "assert_anchor_precedes_all_coverage_records"
        ]
    )]
    after_segment_id: Option<u64>,
    #[arg(long, requires_all = ["after_segment_id", "after_frame_len"])]
    after_frame_offset: Option<u64>,
    #[arg(long, requires_all = ["after_segment_id", "after_frame_offset"])]
    after_frame_len: Option<u64>,
    /// Assert that an independent boundary scan proved no target-range record exists at or before
    /// the external resume anchor. Chunk-internal anchors do not require this assertion.
    #[arg(long, requires = "after_segment_id", default_value_t = false)]
    assert_anchor_precedes_all_coverage_records: bool,
    /// Plain getBlocks result array or a JSON-RPC object containing `result: [...]`.
    #[arg(long, requires = "assert_canonical_manifest_complete_finalized")]
    canonical_get_blocks_json: Option<PathBuf>,
    /// Assert that the supplied getBlocks manifest covers the entire declared epoch range and was
    /// fetched with finalized commitment after that range settled.
    #[arg(long, requires = "canonical_get_blocks_json", default_value_t = false)]
    assert_canonical_manifest_complete_finalized: bool,
    #[arg(long, default_value_t = 4096)]
    max_record_bytes: u64,
    #[arg(long, default_value_t = 250_000)]
    scan_chunk_records: usize,
    /// Fail before pass two when active shred/reconstruction buffers exceed this estimate. Index,
    /// canonical-set, outcome and JSON-report memory are additional and reported as a limitation.
    #[arg(long, default_value_t = 536_870_912)]
    max_resident_bytes: u64,
    /// Suppress per-chunk stderr progress. The final JSON always contains exact progress cursors.
    #[arg(long, default_value_t = false)]
    quiet: bool,
    /// Optional accepted-repair WAL base file (or directory containing exactly one base). When
    /// absent, reconstruction remains byte-for-byte raw-only.
    #[arg(long, requires = "repair_durable_through_sequence")]
    repair_wal: Option<PathBuf>,
    /// Immutable inclusive global repair-WAL sequence used by both verification passes.
    #[arg(long, requires = "repair_wal")]
    repair_durable_through_sequence: Option<u64>,
    #[arg(long, default_value_t = 1_000_000)]
    max_repair_records: u64,
    #[arg(long, default_value_t = 536_870_912)]
    max_repair_payload_bytes: u64,
    #[arg(long, default_value_t = 4096)]
    max_repair_segments: usize,
}

#[derive(Debug, Clone, Serialize)]
struct SlotExtent {
    first_sequence: u64,
    last_sequence: u64,
    record_count: u64,
    decoded_datagram_bytes: u64,
    estimated_buffer_bytes: u64,
}

impl SlotExtent {
    fn new(sequence: u64, datagram_bytes: usize) -> Result<Self> {
        let datagram_bytes =
            u64::try_from(datagram_bytes).context("datagram length exceeds u64")?;
        let estimated_buffer_bytes = SLOT_BUFFER_OVERHEAD_BYTES
            .checked_add(DATAGRAM_VECTOR_OVERHEAD_BYTES)
            .and_then(|bytes| bytes.checked_add(datagram_bytes))
            .context("slot buffer-byte estimate overflow")?;
        Ok(Self {
            first_sequence: sequence,
            last_sequence: sequence,
            record_count: 1,
            decoded_datagram_bytes: datagram_bytes,
            estimated_buffer_bytes,
        })
    }

    fn observe(&mut self, sequence: u64, datagram_bytes: usize) -> Result<()> {
        ensure!(
            sequence >= self.last_sequence,
            "slot observation sequence moved backwards"
        );
        let datagram_bytes =
            u64::try_from(datagram_bytes).context("datagram length exceeds u64")?;
        self.last_sequence = sequence;
        self.record_count = self
            .record_count
            .checked_add(1)
            .context("slot record count overflow")?;
        self.decoded_datagram_bytes = self
            .decoded_datagram_bytes
            .checked_add(datagram_bytes)
            .context("slot decoded-byte count overflow")?;
        self.estimated_buffer_bytes = self
            .estimated_buffer_bytes
            .checked_add(DATAGRAM_VECTOR_OVERHEAD_BYTES)
            .and_then(|bytes| bytes.checked_add(datagram_bytes))
            .context("slot buffer-byte estimate overflow")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
struct ScanReport {
    chunks: u64,
    records: u64,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    first_location: Option<SpoolLocation>,
    last_location: Option<SpoolLocation>,
    durable_through_sequence: u64,
    reached_durable_tail: bool,
    prefix_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct ScanStartReport {
    after: Option<SpoolLocation>,
    anchor_sequence: Option<u64>,
    expected_first_sequence: Option<u64>,
    external_anchor_absence_asserted: bool,
}

#[derive(Debug, Clone, Serialize)]
struct MemoryPlan {
    limit_bytes: u64,
    #[serde(skip_serializing_if = "is_zero_u64")]
    repair_prefix_buffer_bytes: u64,
    predicted_peak_buffer_bytes: u64,
    predicted_peak_with_recovery_bytes: u64,
    largest_slot_buffer_bytes: u64,
    recovery_working_set_multiplier: u64,
    actual_peak_buffer_bytes: u64,
}

#[derive(Debug, Default)]
struct MemoryEvent {
    start_bytes: u64,
    end_bytes: u64,
    recovery_extra_bytes: u64,
}

#[derive(Debug)]
struct SlotBuffer {
    datagrams: Vec<Vec<u8>>,
    decoded_datagram_bytes: u64,
    resident_bytes: u64,
}

impl SlotBuffer {
    fn new(extent: &SlotExtent) -> Result<Self> {
        let capacity = usize::try_from(extent.record_count)
            .context("slot record count exceeds addressable memory")?;
        let mut datagrams = Vec::new();
        datagrams
            .try_reserve_exact(capacity)
            .context("reserve exact slot datagram vector")?;
        let resident_bytes = SLOT_BUFFER_OVERHEAD_BYTES
            .checked_add(
                extent
                    .record_count
                    .checked_mul(DATAGRAM_VECTOR_OVERHEAD_BYTES)
                    .context("slot datagram-vector overhead overflow")?,
            )
            .context("slot fixed buffer overhead overflow")?;
        Ok(Self {
            datagrams,
            decoded_datagram_bytes: 0,
            resident_bytes,
        })
    }

    fn push(&mut self, datagram: Vec<u8>) -> Result<()> {
        let bytes = u64::try_from(datagram.len()).context("datagram length exceeds u64")?;
        self.decoded_datagram_bytes = self
            .decoded_datagram_bytes
            .checked_add(bytes)
            .context("slot buffered decoded-byte count overflow")?;
        self.resident_bytes = self
            .resident_bytes
            .checked_add(bytes)
            .context("slot resident-byte count overflow")?;
        self.datagrams.push(datagram);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Classification {
    Reconstructed,
    MissedCapture,
    NotRecorded,
    ObservedNoncanonical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReconstructionStatus {
    Reconstructed,
    Failed,
}

#[derive(Debug, Serialize)]
struct ReconstructionReport {
    status: ReconstructionStatus,
    failure_category: Option<&'static str>,
    error: Option<String>,
    recovered_data_shreds: usize,
    under_threshold_fec_sets: usize,
    total_threshold_deficit: usize,
    unknown_threshold_fec_sets: usize,
    threshold_deficits: Vec<FecThresholdDeficit>,
    block: Option<BlockReport>,
}

#[derive(Debug, Serialize)]
struct BlockReport {
    parent_slot: u64,
    first_data_shred_index: u32,
    last_data_shred_index: u32,
    components: usize,
    entries: usize,
    transactions: usize,
    block_markers: usize,
    final_poh_hash: Option<String>,
}

#[derive(Debug, Serialize)]
struct ObservedSlotReport {
    slot: u64,
    #[serde(flatten)]
    extent: SlotExtent,
    classification: Option<Classification>,
    reconstruction: ReconstructionReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    repair_merge: Option<RepairSlotReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RepairSlotOutcome {
    RawReconstructedUnchanged,
    ReconstructedWithRepair,
    StillFailedAfterRepair,
    RegressedAfterRepair,
    MergeConflict,
}

#[derive(Debug, Serialize)]
struct RepairSlotReport {
    records_considered: usize,
    unique_shreds_added: usize,
    duplicate_raw_shreds: usize,
    duplicate_repair_shreds: usize,
    unique_payload_bytes_added: u64,
    first_repair_sequence: Option<u64>,
    last_repair_sequence: Option<u64>,
    raw_reconstruction_status: ReconstructionStatus,
    raw_failure_category: Option<&'static str>,
    outcome: RepairSlotOutcome,
    merge_conflict_category: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
struct ClassificationRange {
    first_slot: u64,
    last_slot: u64,
    slot_count: u64,
    classification: Classification,
}

#[derive(Debug, Clone, Serialize)]
struct FailureRange {
    first_slot: u64,
    last_slot: u64,
    slot_count: u64,
    failure_category: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct ExactSlotRange {
    first_slot: u64,
    last_slot: u64,
    slot_count: u64,
}

#[derive(Debug, Default, Serialize)]
struct ClassificationCounts {
    reconstructed: u64,
    missed_capture: u64,
    not_recorded: u64,
    observed_noncanonical: u64,
}

impl ClassificationCounts {
    fn record(&mut self, classification: Classification) {
        let counter = match classification {
            Classification::Reconstructed => &mut self.reconstructed,
            Classification::MissedCapture => &mut self.missed_capture,
            Classification::NotRecorded => &mut self.not_recorded,
            Classification::ObservedNoncanonical => &mut self.observed_noncanonical,
        };
        *counter = counter.saturating_add(1);
    }
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalReport {
    source_path: String,
    source_sha256: String,
    produced_slots_in_epoch: usize,
    produced_slots_in_coverage: usize,
    comparison_basis: &'static str,
    operator_asserted_complete_finalized: bool,
}

#[derive(Debug, Default, Serialize)]
struct RepairOutcomeCounts {
    slots_with_records: u64,
    records_considered: u64,
    unique_shreds_added: u64,
    duplicate_raw_shreds: u64,
    duplicate_repair_shreds: u64,
    unique_payload_bytes_added: u64,
    raw_reconstructed_slots: u64,
    raw_reconstructed_unchanged: u64,
    reconstructed_with_repair: u64,
    still_failed_after_repair: u64,
    regressed_after_repair: u64,
    merge_conflict_slots: u64,
}

#[derive(Debug, Serialize)]
struct RepairAuditReport {
    mode: &'static str,
    pass_one: RepairWalScanReport,
    pass_two: RepairWalScanReport,
    outcomes: RepairOutcomeCounts,
    merge_conflict_category_counts: BTreeMap<&'static str, u64>,
    verification_scope: [&'static str; 4],
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    journal: JournalReport,
    epoch_slot_range: ExactSlotRange,
    coverage_slot_range: ExactSlotRange,
    excluded_epoch_ranges: Vec<ExactSlotRange>,
    scan_start: ScanStartReport,
    pass_one: ScanReport,
    pass_two: ScanReport,
    memory: MemoryPlan,
    canonical: Option<CanonicalReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repair: Option<RepairAuditReport>,
    classification_counts: ClassificationCounts,
    failure_category_counts: BTreeMap<&'static str, u64>,
    classification_ranges: Vec<ClassificationRange>,
    failure_ranges: Vec<FailureRange>,
    get_blocks_absent_unobserved_ranges: Vec<ExactSlotRange>,
    observed_slots: Vec<ObservedSlotReport>,
    limitations: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct JournalReport {
    spool_root: String,
    cluster_id: String,
    origin_node_id: String,
    source_id: String,
    journal_id: String,
    durable_through_sequence: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;
    let identity = SpoolJournalIdentity {
        cluster_id: args.cluster_id.clone(),
        origin_node_id: args.origin_node_id.clone(),
        source_id: args.source_id.clone(),
        journal_id: args.journal_id,
    };
    let (coverage_start, coverage_end) = coverage_range(&args)?;
    let after = after_location(&args)?;
    let scan_start = load_scan_start(&args, &identity, after)?;
    let (canonical_slots, canonical) = load_canonical_slots(
        args.canonical_get_blocks_json.as_deref(),
        args.min_slot,
        args.max_slot,
        coverage_start,
        coverage_end,
        args.assert_canonical_manifest_complete_finalized,
    )?;
    let repair_config = repair_scan_config(&args)?;
    let mut repair_slots = BTreeSet::new();
    let repair_pass_one = if let Some(config) = repair_config.as_ref() {
        Some(scan_repair_wal_prefix(
            config,
            coverage_start,
            coverage_end,
            |record| {
                repair_slots.insert(record.slot);
                Ok(())
            },
        )?)
    } else {
        None
    };

    let mut extents = BTreeMap::<u64, SlotExtent>::new();
    let pass_one = scan_frozen_prefix(&args, &identity, after, "pass1", |record| {
        let Some((slot, sequence, datagram)) =
            decode_epoch_record(record, coverage_start, coverage_end)?
        else {
            return Ok(());
        };
        match extents.entry(slot) {
            Entry::Vacant(entry) => {
                entry.insert(SlotExtent::new(sequence, datagram.len())?);
            }
            Entry::Occupied(mut entry) => entry.get_mut().observe(sequence, datagram.len())?,
        }
        Ok(())
    })?;
    ensure_expected_first_sequence(&scan_start, &pass_one)?;
    for slot in repair_slots {
        ensure!(
            extents.contains_key(&slot),
            "verified repair WAL contains target slot {slot} with no raw journal observation in the exact audit prefix"
        );
    }

    let mut memory = plan_memory(&extents, repair_pass_one.as_ref(), args.max_resident_bytes)?;
    ensure!(
        memory.predicted_peak_with_recovery_bytes <= memory.limit_bytes,
        "pass-two memory preflight requires {} bytes, above --max-resident-bytes {}; narrow coverage, increase the limit, or use a partitioned audit",
        memory.predicted_peak_with_recovery_bytes,
        memory.limit_bytes
    );

    let mut repairs_by_slot = BTreeMap::<u64, Vec<ValidatedRepairRecord>>::new();
    let repair_pass_two = if let Some(config) = repair_config.as_ref() {
        let report = scan_repair_wal_prefix(config, coverage_start, coverage_end, |record| {
            repairs_by_slot.entry(record.slot).or_default().push(record);
            Ok(())
        })?;
        ensure_same_repair_prefix(
            repair_pass_one
                .as_ref()
                .expect("repair pass one exists when its config exists"),
            &report,
        )?;
        Some(report)
    } else {
        None
    };

    let mut buffers = BTreeMap::<u64, SlotBuffer>::new();
    let mut outcomes = BTreeMap::<u64, ObservedSlotReport>::new();
    let mut resident_bytes = memory.repair_prefix_buffer_bytes;
    let mut actual_peak_buffer_bytes = resident_bytes;
    let pass_two = scan_frozen_prefix(&args, &identity, after, "pass2", |record| {
        let Some((slot, sequence, datagram)) =
            decode_epoch_record(record, coverage_start, coverage_end)?
        else {
            return Ok(());
        };
        let extent = extents
            .get(&slot)
            .with_context(|| format!("pass two observed unindexed slot {slot}"))?;
        if !buffers.contains_key(&slot) {
            ensure!(
                sequence == extent.first_sequence,
                "slot {slot} pass-two first sequence differs from pass one"
            );
            let buffer = SlotBuffer::new(extent)?;
            resident_bytes = resident_bytes
                .checked_add(buffer.resident_bytes)
                .context("resident slot-buffer count overflow")?;
            buffers.insert(slot, buffer);
        }
        let buffer = buffers
            .get_mut(&slot)
            .expect("slot buffer was inserted above");
        buffer.push(datagram)?;
        resident_bytes = resident_bytes
            .checked_add(
                u64::try_from(buffer.datagrams.last().expect("just pushed").len())
                    .context("datagram length exceeds u64")?,
            )
            .context("resident datagram-byte count overflow")?;
        actual_peak_buffer_bytes = actual_peak_buffer_bytes.max(resident_bytes);
        ensure!(
            resident_bytes <= memory.predicted_peak_buffer_bytes,
            "pass-two buffered bytes exceeded the pass-one prediction"
        );

        let observed_count = u64::try_from(buffer.datagrams.len())
            .context("slot buffered record count exceeds u64")?;
        ensure!(
            observed_count <= extent.record_count,
            "slot {slot} pass two has more records than pass one"
        );
        if observed_count != extent.record_count {
            return Ok(());
        }
        ensure!(
            sequence == extent.last_sequence,
            "slot {slot} reached its pass-one record count before its exact last sequence"
        );
        let buffer = buffers.remove(&slot).expect("completed slot buffer exists");
        ensure!(
            buffer.decoded_datagram_bytes == extent.decoded_datagram_bytes,
            "slot {slot} decoded-byte count differs between passes"
        );
        resident_bytes = resident_bytes
            .checked_sub(buffer.resident_bytes)
            .context("resident slot-buffer count underflow")?;
        let repairs = repairs_by_slot.remove(&slot).unwrap_or_default();
        let outcome = audit_slot(slot, extent.clone(), buffer.datagrams, repairs);
        ensure!(
            outcomes.insert(slot, outcome).is_none(),
            "slot {slot} was finalized more than once"
        );
        Ok(())
    })?;
    memory.actual_peak_buffer_bytes = actual_peak_buffer_bytes;
    ensure!(
        buffers.is_empty(),
        "pass two ended with unfinished slot buffers"
    );
    ensure!(
        repairs_by_slot.is_empty(),
        "repair pass two retained records for slots that were never finalized from the raw prefix"
    );
    ensure!(
        outcomes.len() == extents.len(),
        "pass two finalized {} slots but pass one indexed {}",
        outcomes.len(),
        extents.len()
    );
    ensure_same_prefix(&pass_one, &pass_two)?;
    ensure_expected_first_sequence(&scan_start, &pass_two)?;

    let classification = classify_slots(
        args.min_slot,
        args.max_slot,
        coverage_start,
        coverage_end,
        canonical_slots.as_ref(),
        &mut outcomes,
    )?;
    let repair = match (repair_pass_one, repair_pass_two) {
        (Some(first), Some(second)) => Some(build_repair_audit_report(first, second, &outcomes)?),
        (None, None) => None,
        _ => bail!("repair audit passes were not both completed"),
    };
    let schema_version = if repair.is_some() {
        REPORT_SCHEMA_VERSION + 1
    } else {
        REPORT_SCHEMA_VERSION
    };
    let mut limitations = vec![
        "reconstructed means a candidate was rebuilt for a finalized produced slot; getBlocks membership alone does not prove the candidate fork is canonical",
        "max_resident_bytes bounds active shred/reconstruction buffers only; persistent indexes, outcomes and report serialization require additional memory",
    ];
    if repair.is_some() {
        limitations.extend([
            "repair peer authorization and leader-schedule selection were enforced by the receiver at acceptance; the offline audit independently revalidates the recorded leader signature and all payload-bound provenance but has no historical gossip or schedule snapshot",
            "a repair record is merged only when its exact FEC identity and complete recorded successor-anchor chain agree with the raw/repair candidate; an ambiguity is reported as a merge conflict and never fork-selected",
        ]);
    }
    let report = Report {
        schema_version,
        journal: JournalReport {
            spool_root: args.spool_root.display().to_string(),
            cluster_id: args.cluster_id,
            origin_node_id: args.origin_node_id,
            source_id: args.source_id,
            journal_id: hex_bytes(&args.journal_id),
            durable_through_sequence: args.durable_through_sequence,
        },
        epoch_slot_range: exact_range(args.min_slot, args.max_slot)?,
        coverage_slot_range: exact_range(coverage_start, coverage_end)?,
        excluded_epoch_ranges: excluded_ranges(
            args.min_slot,
            args.max_slot,
            coverage_start,
            coverage_end,
        )?,
        scan_start,
        pass_one,
        pass_two,
        memory,
        canonical,
        repair,
        classification_counts: classification.counts,
        failure_category_counts: classification.failure_category_counts,
        classification_ranges: classification.classification_ranges,
        failure_ranges: classification.failure_ranges,
        get_blocks_absent_unobserved_ranges: classification.unlisted_unobserved_ranges,
        observed_slots: outcomes.into_values().collect(),
        limitations,
    };
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, &report).context("encode epoch audit report")?;
    writeln!(output).context("finish epoch audit report")?;
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    ensure!(args.min_slot <= args.max_slot, "min-slot exceeds max-slot");
    ensure!(
        args.max_record_bytes > 0,
        "max-record-bytes must be non-zero"
    );
    ensure!(
        args.scan_chunk_records > 0,
        "scan-chunk-records must be non-zero"
    );
    ensure!(
        args.max_resident_bytes > 0,
        "max-resident-bytes must be non-zero"
    );
    ensure!(
        args.repair_wal.is_some() == args.repair_durable_through_sequence.is_some(),
        "repair-wal and repair-durable-through-sequence are all-or-none"
    );
    if args.repair_wal.is_some() {
        ensure!(
            args.max_repair_records > 0,
            "max-repair-records must be non-zero"
        );
        ensure!(
            args.max_repair_payload_bytes > 0,
            "max-repair-payload-bytes must be non-zero"
        );
        ensure!(
            args.max_repair_segments > 0,
            "max-repair-segments must be non-zero"
        );
    }
    Ok(())
}

fn repair_scan_config(args: &Args) -> Result<Option<RepairWalScanConfig>> {
    match (
        args.repair_wal.as_ref(),
        args.repair_durable_through_sequence,
    ) {
        (None, None) => Ok(None),
        (Some(path), Some(durable_through_sequence)) => Ok(Some(RepairWalScanConfig {
            path: path.clone(),
            durable_through_sequence,
            max_records: args.max_repair_records,
            max_payload_bytes: args.max_repair_payload_bytes,
            max_segments: args.max_repair_segments,
        })),
        _ => bail!("repair-wal and repair-durable-through-sequence are all-or-none"),
    }
}

fn coverage_range(args: &Args) -> Result<(u64, u64)> {
    let range = (args.coverage_start_slot, args.coverage_end_slot);
    ensure!(
        args.min_slot <= range.0 && range.0 <= range.1 && range.1 <= args.max_slot,
        "coverage range must be non-empty and contained in the epoch slot range"
    );
    Ok(range)
}

fn after_location(args: &Args) -> Result<Option<SpoolLocation>> {
    match (
        args.after_segment_id,
        args.after_frame_offset,
        args.after_frame_len,
    ) {
        (None, None, None) => Ok(None),
        (Some(segment_id), Some(frame_offset), Some(frame_len)) => Ok(Some(SpoolLocation {
            segment_id,
            frame_offset,
            frame_len,
        })),
        _ => bail!("after-segment-id, after-frame-offset and after-frame-len are all-or-none"),
    }
}

fn load_scan_start(
    args: &Args,
    identity: &SpoolJournalIdentity,
    after: Option<SpoolLocation>,
) -> Result<ScanStartReport> {
    let Some(location) = after else {
        return Ok(ScanStartReport {
            after: None,
            anchor_sequence: None,
            expected_first_sequence: Some(0),
            external_anchor_absence_asserted: false,
        });
    };
    let journal_dir = spool_journal_dir_path(&args.spool_root, identity)?;
    let anchor = read_spool_record(journal_dir, location, args.max_record_bytes)
        .context("read epoch-audit resume anchor")?;
    ensure_record_identity(&anchor, identity)?;
    let anchor_sequence = anchor.metadata.observation.sequence;
    ensure!(
        anchor_sequence < args.durable_through_sequence,
        "resume anchor must precede the frozen durable cursor"
    );
    Ok(ScanStartReport {
        after: Some(location),
        anchor_sequence: Some(anchor_sequence),
        expected_first_sequence: Some(
            anchor_sequence
                .checked_add(1)
                .context("resume anchor sequence exhausted")?,
        ),
        external_anchor_absence_asserted: args.assert_anchor_precedes_all_coverage_records,
    })
}

fn ensure_record_identity(record: &SpoolRecord, identity: &SpoolJournalIdentity) -> Result<()> {
    ensure!(
        record.metadata.cluster_id == identity.cluster_id
            && record.metadata.observation.origin_node_id == identity.origin_node_id
            && record.metadata.source_id == identity.source_id
            && record.metadata.observation.journal_id == identity.journal_id,
        "resume anchor belongs to a different spool journal"
    );
    Ok(())
}

fn scan_frozen_prefix<F>(
    args: &Args,
    identity: &SpoolJournalIdentity,
    start_after: Option<SpoolLocation>,
    pass: &str,
    mut visit: F,
) -> Result<ScanReport>
where
    F: FnMut(SpoolRecord) -> Result<()>,
{
    let mut after = start_after;
    let mut chunks = 0u64;
    let mut records = 0u64;
    let mut first_sequence = None;
    let mut last_sequence = None;
    let mut first_location = None;
    let mut last_location = None;
    let mut reached_durable_tail = false;
    let mut fingerprint = Sha256::new();

    while !reached_durable_tail {
        let mut chunk_records = 0u64;
        let mut chunk_last_location = None;
        let snapshot = read_spool_committed_snapshot_after(
            &args.spool_root,
            identity.clone(),
            args.max_record_bytes,
            after,
            args.durable_through_sequence,
            args.scan_chunk_records,
            |record| {
                let sequence = record.metadata.observation.sequence;
                first_sequence.get_or_insert(sequence);
                last_sequence = Some(sequence);
                first_location.get_or_insert(record.location);
                last_location = Some(record.location);
                chunk_last_location = Some(record.location);
                chunk_records = chunk_records
                    .checked_add(1)
                    .context("scan chunk record count overflow")?;
                update_prefix_fingerprint(&mut fingerprint, &record);
                visit(record)
            },
        )?;
        ensure!(
            snapshot.records == chunk_records,
            "snapshot callback count differs from snapshot report"
        );
        chunks = chunks.checked_add(1).context("scan chunk count overflow")?;
        records = records
            .checked_add(chunk_records)
            .context("scan record count overflow")?;
        reached_durable_tail = snapshot.reached_durable_tail;
        if !args.quiet {
            eprintln!(
                "{pass} chunk {chunks}: {chunk_records} records, total {records}, last sequence {:?}, resume {:?}",
                snapshot.last_sequence, chunk_last_location
            );
        }
        if reached_durable_tail {
            break;
        }
        ensure!(chunk_records > 0, "chunked spool scan made no progress");
        let next_after =
            chunk_last_location.context("non-empty scan chunk has no last location")?;
        ensure!(
            Some(next_after) != after,
            "chunked spool scan resume did not advance"
        );
        after = Some(next_after);
    }
    Ok(ScanReport {
        chunks,
        records,
        first_sequence,
        last_sequence,
        first_location,
        last_location,
        durable_through_sequence: args.durable_through_sequence,
        reached_durable_tail,
        prefix_sha256: hex_bytes(&fingerprint.finalize()),
    })
}

fn update_prefix_fingerprint(fingerprint: &mut Sha256, record: &SpoolRecord) {
    fingerprint.update(record.metadata.observation.sequence.to_le_bytes());
    fingerprint.update(record.location.segment_id.to_le_bytes());
    fingerprint.update(record.location.frame_offset.to_le_bytes());
    fingerprint.update(record.location.frame_len.to_le_bytes());
    fingerprint.update(record.metadata.content_digest.0);
}

fn decode_epoch_record(
    record: SpoolRecord,
    coverage_start: u64,
    coverage_end: u64,
) -> Result<Option<(u64, u64, Vec<u8>)>> {
    ensure!(
        record.metadata.payload_format_version == ZSTD_SOLANA_SHRED_V1,
        "epoch audit accepts only canonical compressed raw shreds"
    );
    let LogicalKey::Shred {
        slot,
        kind,
        shred_index,
        fec_set_index,
    } = record.metadata.logical_key
    else {
        bail!("non-shred record found in raw shred spool");
    };
    if slot < coverage_start || slot > coverage_end {
        return Ok(None);
    }
    let datagram = decode_stored_shred(&record.payload)?;
    let shred = Shred::new_from_serialized_shred(datagram.clone())
        .map_err(|error| anyhow::anyhow!("parse stored Solana shred: {error:?}"))?;
    let decoded_kind = if shred.is_code() {
        ShredKind::Coding
    } else {
        ShredKind::Data
    };
    ensure!(
        shred.slot() == slot
            && shred.index() == shred_index
            && decoded_kind == kind
            && fec_set_index == Some(shred.fec_set_index()),
        "stored shred metadata differs from its decoded canonical payload"
    );
    Ok(Some((slot, record.metadata.observation.sequence, datagram)))
}

fn plan_memory(
    extents: &BTreeMap<u64, SlotExtent>,
    repair: Option<&RepairWalScanReport>,
    limit_bytes: u64,
) -> Result<MemoryPlan> {
    let mut events = BTreeMap::<u64, MemoryEvent>::new();
    let mut largest_slot_buffer_bytes = 0u64;
    for extent in extents.values() {
        largest_slot_buffer_bytes = largest_slot_buffer_bytes.max(extent.estimated_buffer_bytes);
        let start = events.entry(extent.first_sequence).or_default();
        start.start_bytes = start
            .start_bytes
            .checked_add(extent.estimated_buffer_bytes)
            .context("memory start event overflow")?;
        let end = events.entry(extent.last_sequence).or_default();
        end.end_bytes = end
            .end_bytes
            .checked_add(extent.estimated_buffer_bytes)
            .context("memory end event overflow")?;
        end.recovery_extra_bytes = end
            .recovery_extra_bytes
            .checked_add(
                extent
                    .estimated_buffer_bytes
                    .checked_mul(RECOVERY_WORKING_SET_MULTIPLIER - 1)
                    .context("recovery memory estimate overflow")?,
            )
            .context("recovery event overflow")?;
    }
    let mut active = 0u64;
    let mut peak_buffer = 0u64;
    let mut peak_with_recovery = 0u64;
    for event in events.values() {
        active = active
            .checked_add(event.start_bytes)
            .context("active memory estimate overflow")?;
        peak_buffer = peak_buffer.max(active);
        peak_with_recovery = peak_with_recovery.max(
            active
                .checked_add(event.recovery_extra_bytes)
                .context("peak recovery memory estimate overflow")?,
        );
        active = active
            .checked_sub(event.end_bytes)
            .context("active memory estimate underflow")?;
    }
    ensure!(active == 0, "memory interval sweep ended with active slots");
    let repair_prefix_buffer_bytes = repair.map_or(Ok(0), |report| {
        report
            .coverage_records
            .checked_mul(REPAIR_RECORD_MEMORY_OVERHEAD_BYTES)
            .and_then(|bytes| bytes.checked_add(report.coverage_payload_bytes))
            .context("repair prefix buffer estimate overflow")
    })?;
    peak_buffer = peak_buffer
        .checked_add(repair_prefix_buffer_bytes)
        .context("combined raw/repair buffer estimate overflow")?;
    peak_with_recovery = peak_with_recovery
        .checked_add(
            repair_prefix_buffer_bytes
                .checked_mul(RECOVERY_WORKING_SET_MULTIPLIER)
                .context("repair recovery memory estimate overflow")?,
        )
        .context("combined raw/repair recovery estimate overflow")?;
    Ok(MemoryPlan {
        limit_bytes,
        repair_prefix_buffer_bytes,
        predicted_peak_buffer_bytes: peak_buffer,
        predicted_peak_with_recovery_bytes: peak_with_recovery,
        largest_slot_buffer_bytes,
        recovery_working_set_multiplier: RECOVERY_WORKING_SET_MULTIPLIER,
        actual_peak_buffer_bytes: 0,
    })
}

fn audit_slot(
    slot: u64,
    extent: SlotExtent,
    datagrams: Vec<Vec<u8>>,
    repairs: Vec<ValidatedRepairRecord>,
) -> ObservedSlotReport {
    if repairs.is_empty() {
        return ObservedSlotReport {
            slot,
            extent,
            classification: None,
            reconstruction: reconstruct_slot(&datagrams),
            repair_merge: None,
        };
    }

    let raw_reconstruction = reconstruct_slot(&datagrams);
    let raw_status = raw_reconstruction.status;
    let raw_failure_category = raw_reconstruction.failure_category;
    let (reconstruction, merge_stats, outcome, merge_conflict_category) =
        match merge_repair_records(slot, datagrams, repairs) {
            Ok(merged) => {
                let reconstruction = reconstruct_slot(&merged.datagrams);
                let outcome = match (raw_status, reconstruction.status) {
                    (ReconstructionStatus::Reconstructed, ReconstructionStatus::Reconstructed) => {
                        RepairSlotOutcome::RawReconstructedUnchanged
                    }
                    (ReconstructionStatus::Failed, ReconstructionStatus::Reconstructed) => {
                        RepairSlotOutcome::ReconstructedWithRepair
                    }
                    (ReconstructionStatus::Failed, ReconstructionStatus::Failed) => {
                        RepairSlotOutcome::StillFailedAfterRepair
                    }
                    (ReconstructionStatus::Reconstructed, ReconstructionStatus::Failed) => {
                        RepairSlotOutcome::RegressedAfterRepair
                    }
                };
                (reconstruction, merged.stats, outcome, None)
            }
            Err(failure) => {
                let category = failure.category;
                let reconstruction = failed_repair_merge(&failure);
                (
                    reconstruction,
                    failure.stats,
                    RepairSlotOutcome::MergeConflict,
                    Some(category),
                )
            }
        };
    let repair_merge = Some(repair_slot_report(
        merge_stats,
        raw_status,
        raw_failure_category,
        outcome,
        merge_conflict_category,
    ));
    ObservedSlotReport {
        slot,
        extent,
        classification: None,
        reconstruction,
        repair_merge,
    }
}

fn reconstruct_slot(datagrams: &[Vec<u8>]) -> ReconstructionReport {
    match recover_slot_data_shreds(datagrams) {
        Err(error) => failed_reconstruction(error, 0, 0, 0, 0, Vec::new()),
        Ok(recovered) => {
            let recovered_data_shreds = recovered.recovered_data_shred_count;
            let under_threshold_fec_sets = recovered.under_threshold_fec_sets;
            let total_threshold_deficit = recovered.total_threshold_deficit;
            let unknown_threshold_fec_sets = recovered.unknown_threshold_fec_sets;
            let threshold_deficits = recovered.threshold_deficits;
            match deshred_complete_data_slot(recovered.data_shreds.iter()) {
                Err(error) => failed_reconstruction(
                    error,
                    recovered_data_shreds,
                    under_threshold_fec_sets,
                    total_threshold_deficit,
                    unknown_threshold_fec_sets,
                    threshold_deficits,
                ),
                Ok(block) => ReconstructionReport {
                    status: ReconstructionStatus::Reconstructed,
                    failure_category: None,
                    error: None,
                    recovered_data_shreds,
                    under_threshold_fec_sets,
                    total_threshold_deficit,
                    unknown_threshold_fec_sets,
                    threshold_deficits,
                    block: Some(BlockReport {
                        parent_slot: block.parent_slot,
                        first_data_shred_index: block.first_data_shred_index,
                        last_data_shred_index: block.last_data_shred_index,
                        components: block.components.len(),
                        entries: block.entry_count(),
                        transactions: block.transaction_count(),
                        block_markers: block.block_marker_count(),
                        final_poh_hash: block.final_poh_hash().map(|hash| hex_bytes(&hash)),
                    }),
                },
            }
        }
    }
}

fn failed_repair_merge(failure: &RepairMergeFailure) -> ReconstructionReport {
    ReconstructionReport {
        status: ReconstructionStatus::Failed,
        failure_category: Some(failure.category),
        error: Some(truncate_error(failure.message.clone())),
        recovered_data_shreds: 0,
        under_threshold_fec_sets: 0,
        total_threshold_deficit: 0,
        unknown_threshold_fec_sets: 0,
        threshold_deficits: Vec::new(),
        block: None,
    }
}

fn repair_slot_report(
    stats: RepairMergeStats,
    raw_reconstruction_status: ReconstructionStatus,
    raw_failure_category: Option<&'static str>,
    outcome: RepairSlotOutcome,
    merge_conflict_category: Option<&'static str>,
) -> RepairSlotReport {
    RepairSlotReport {
        records_considered: stats.records_considered,
        unique_shreds_added: stats.unique_shreds_added,
        duplicate_raw_shreds: stats.duplicate_raw_shreds,
        duplicate_repair_shreds: stats.duplicate_repair_shreds,
        unique_payload_bytes_added: stats.unique_payload_bytes_added,
        first_repair_sequence: stats.first_repair_sequence,
        last_repair_sequence: stats.last_repair_sequence,
        raw_reconstruction_status,
        raw_failure_category,
        outcome,
        merge_conflict_category,
    }
}

fn failed_reconstruction(
    error: anyhow::Error,
    recovered_data_shreds: usize,
    under_threshold_fec_sets: usize,
    total_threshold_deficit: usize,
    unknown_threshold_fec_sets: usize,
    threshold_deficits: Vec<FecThresholdDeficit>,
) -> ReconstructionReport {
    ReconstructionReport {
        status: ReconstructionStatus::Failed,
        failure_category: Some(shred_reconstruction_failure_category(&error)),
        error: Some(truncate_error(format!("{error:#}"))),
        recovered_data_shreds,
        under_threshold_fec_sets,
        total_threshold_deficit,
        unknown_threshold_fec_sets,
        threshold_deficits,
        block: None,
    }
}

fn build_repair_audit_report(
    pass_one: RepairWalScanReport,
    pass_two: RepairWalScanReport,
    outcomes: &BTreeMap<u64, ObservedSlotReport>,
) -> Result<RepairAuditReport> {
    ensure_same_repair_prefix(&pass_one, &pass_two)?;
    let mut counts = RepairOutcomeCounts::default();
    let mut merge_conflict_category_counts = BTreeMap::new();
    for slot in outcomes.values() {
        let Some(repair) = slot.repair_merge.as_ref() else {
            continue;
        };
        counts.slots_with_records = counts
            .slots_with_records
            .checked_add(1)
            .context("repair slot count overflow")?;
        counts.records_considered = counts
            .records_considered
            .checked_add(
                u64::try_from(repair.records_considered)
                    .context("repair record count exceeds u64")?,
            )
            .context("repair record count overflow")?;
        counts.unique_shreds_added = counts
            .unique_shreds_added
            .checked_add(
                u64::try_from(repair.unique_shreds_added)
                    .context("unique repair shred count exceeds u64")?,
            )
            .context("unique repair shred count overflow")?;
        counts.duplicate_raw_shreds = counts
            .duplicate_raw_shreds
            .checked_add(
                u64::try_from(repair.duplicate_raw_shreds)
                    .context("duplicate raw repair count exceeds u64")?,
            )
            .context("duplicate raw repair count overflow")?;
        counts.duplicate_repair_shreds = counts
            .duplicate_repair_shreds
            .checked_add(
                u64::try_from(repair.duplicate_repair_shreds)
                    .context("duplicate repair count exceeds u64")?,
            )
            .context("duplicate repair count overflow")?;
        counts.unique_payload_bytes_added = counts
            .unique_payload_bytes_added
            .checked_add(repair.unique_payload_bytes_added)
            .context("unique repair payload-byte count overflow")?;
        if repair.raw_reconstruction_status == ReconstructionStatus::Reconstructed {
            counts.raw_reconstructed_slots = counts
                .raw_reconstructed_slots
                .checked_add(1)
                .context("raw reconstructed repair-slot count overflow")?;
        }
        let counter = match repair.outcome {
            RepairSlotOutcome::RawReconstructedUnchanged => &mut counts.raw_reconstructed_unchanged,
            RepairSlotOutcome::ReconstructedWithRepair => &mut counts.reconstructed_with_repair,
            RepairSlotOutcome::StillFailedAfterRepair => &mut counts.still_failed_after_repair,
            RepairSlotOutcome::RegressedAfterRepair => &mut counts.regressed_after_repair,
            RepairSlotOutcome::MergeConflict => &mut counts.merge_conflict_slots,
        };
        *counter = counter
            .checked_add(1)
            .context("repair outcome count overflow")?;
        if let Some(category) = repair.merge_conflict_category {
            let category_count = merge_conflict_category_counts
                .entry(category)
                .or_insert(0u64);
            *category_count = category_count
                .checked_add(1)
                .context("repair merge-conflict count overflow")?;
        }
    }
    ensure!(
        counts.records_considered == pass_two.coverage_records,
        "repair merge considered {} records, but the verified coverage prefix contained {}",
        counts.records_considered,
        pass_two.coverage_records
    );
    ensure!(
        counts.slots_with_records
            == u64::try_from(pass_two.coverage_slots)
                .context("repair coverage slots exceed u64")?,
        "repair merge slot count differs from the verified coverage prefix"
    );
    Ok(RepairAuditReport {
        mode: "verified_accepted_repair_wal_merge",
        pass_one,
        pass_two,
        outcomes: counts,
        merge_conflict_category_counts,
        verification_scope: [
            "frame_crc32_and_global_sequence_continuity",
            "segment_header_crc32_and_sha256_predecessor_chain",
            "canonical_shred_request_and_all_payload_bound_provenance",
            "slot_leader_signature_fec_identity_and_complete_successor_anchor_chain",
        ],
    })
}

struct ClassificationOutput {
    counts: ClassificationCounts,
    failure_category_counts: BTreeMap<&'static str, u64>,
    classification_ranges: Vec<ClassificationRange>,
    failure_ranges: Vec<FailureRange>,
    unlisted_unobserved_ranges: Vec<ExactSlotRange>,
}

fn classify_slots(
    epoch_start: u64,
    epoch_end: u64,
    coverage_start: u64,
    coverage_end: u64,
    canonical: Option<&BTreeSet<u64>>,
    outcomes: &mut BTreeMap<u64, ObservedSlotReport>,
) -> Result<ClassificationOutput> {
    let mut output = ClassificationOutput {
        counts: ClassificationCounts::default(),
        failure_category_counts: BTreeMap::new(),
        classification_ranges: Vec::new(),
        failure_ranges: Vec::new(),
        unlisted_unobserved_ranges: Vec::new(),
    };
    let (start, end) = if canonical.is_some() {
        (epoch_start, epoch_end)
    } else {
        (coverage_start, coverage_end)
    };
    for_each_slot(start, end, |slot| {
        let in_coverage = (coverage_start..=coverage_end).contains(&slot);
        let observed = outcomes.get(&slot);
        let observed_present = observed.is_some();
        let reconstructed = observed.is_some_and(|outcome| {
            outcome.reconstruction.status == ReconstructionStatus::Reconstructed
        });
        let observed_failure_category =
            observed.and_then(|outcome| outcome.reconstruction.failure_category);
        let classification = match canonical {
            Some(canonical) if canonical.contains(&slot) => Some(if reconstructed {
                Classification::Reconstructed
            } else if in_coverage {
                Classification::MissedCapture
            } else {
                Classification::NotRecorded
            }),
            Some(_) if in_coverage && observed_present => {
                Some(Classification::ObservedNoncanonical)
            }
            Some(_) => None,
            None if reconstructed => Some(Classification::Reconstructed),
            None if observed_present => Some(Classification::MissedCapture),
            None => Some(Classification::NotRecorded),
        };
        if canonical.is_some() && classification.is_none() {
            append_exact_range(&mut output.unlisted_unobserved_ranges, slot)?;
        }
        if let Some(classification) = classification {
            output.counts.record(classification);
            append_classification_range(&mut output.classification_ranges, slot, classification)?;
            if let Some(outcome) = outcomes.get_mut(&slot) {
                outcome.classification = Some(classification);
            }
        }
        let failure_category = match canonical {
            Some(canonical) if in_coverage && canonical.contains(&slot) && !observed_present => {
                Some(NO_SHREDS_OBSERVED)
            }
            _ => observed_failure_category,
        };
        if let Some(category) = failure_category {
            let count = output.failure_category_counts.entry(category).or_default();
            *count = count.saturating_add(1);
            append_failure_range(&mut output.failure_ranges, slot, category)?;
        }
        Ok(())
    })?;
    if let Some(canonical) = canonical {
        let classified_canonical = output
            .counts
            .reconstructed
            .checked_add(output.counts.missed_capture)
            .and_then(|count| count.checked_add(output.counts.not_recorded))
            .context("canonical classification count overflow")?;
        ensure!(
            classified_canonical
                == u64::try_from(canonical.len()).context("canonical slot count exceeds u64")?,
            "canonical classification invariant failed: reconstructed + missed-capture + not-recorded must equal produced epoch slots"
        );
    }
    Ok(output)
}

fn load_canonical_slots(
    path: Option<&Path>,
    epoch_start: u64,
    epoch_end: u64,
    coverage_start: u64,
    coverage_end: u64,
    operator_asserted_complete_finalized: bool,
) -> Result<(Option<BTreeSet<u64>>, Option<CanonicalReport>)> {
    let Some(path) = path else {
        return Ok((None, None));
    };
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let source_sha256 = hex_bytes(&Sha256::digest(&bytes));
    let slots = parse_canonical_slots(&bytes, epoch_start, epoch_end)
        .with_context(|| format!("decode getBlocks JSON {}", path.display()))?;
    let produced_slots_in_coverage = slots.range(coverage_start..=coverage_end).count();
    let report = CanonicalReport {
        source_path: path.display().to_string(),
        source_sha256,
        produced_slots_in_epoch: slots.len(),
        produced_slots_in_coverage,
        comparison_basis: "finalized_getBlocks_slot_membership_only_no_blockhash_match",
        operator_asserted_complete_finalized,
    };
    Ok((Some(slots), Some(report)))
}

fn parse_canonical_slots(bytes: &[u8], epoch_start: u64, epoch_end: u64) -> Result<BTreeSet<u64>> {
    let value: Value = serde_json::from_slice(&bytes).context("decode getBlocks JSON")?;
    let values = match &value {
        Value::Array(values) => values,
        Value::Object(object) => object
            .get("result")
            .and_then(Value::as_array)
            .context("getBlocks JSON-RPC object must contain an array result")?,
        _ => bail!("getBlocks JSON must be an array or a JSON-RPC result object"),
    };
    let mut slots = BTreeSet::new();
    for value in values {
        let slot = value
            .as_u64()
            .context("getBlocks result contains a non-u64 slot")?;
        ensure!(
            (epoch_start..=epoch_end).contains(&slot),
            "getBlocks slot {slot} is outside the declared epoch range"
        );
        slots.insert(slot);
    }
    Ok(slots)
}

fn ensure_expected_first_sequence(start: &ScanStartReport, scan: &ScanReport) -> Result<()> {
    if let Some(expected) = start.expected_first_sequence {
        ensure!(
            scan.first_sequence == Some(expected),
            "resumed scan started at {:?}, expected anchor sequence + 1 ({expected})",
            scan.first_sequence
        );
    }
    Ok(())
}

fn ensure_same_prefix(first: &ScanReport, second: &ScanReport) -> Result<()> {
    ensure!(
        first.records == second.records
            && first.first_sequence == second.first_sequence
            && first.last_sequence == second.last_sequence
            && first.first_location == second.first_location
            && first.last_location == second.last_location
            && first.prefix_sha256 == second.prefix_sha256
            && first.reached_durable_tail
            && second.reached_durable_tail,
        "the journal prefix changed or was retired between audit passes"
    );
    Ok(())
}

fn append_classification_range(
    ranges: &mut Vec<ClassificationRange>,
    slot: u64,
    classification: Classification,
) -> Result<()> {
    if let Some(last) = ranges.last_mut()
        && last.classification == classification
        && last.last_slot.checked_add(1) == Some(slot)
    {
        last.last_slot = slot;
        last.slot_count = last
            .slot_count
            .checked_add(1)
            .context("classification range count overflow")?;
        return Ok(());
    }
    ranges.push(ClassificationRange {
        first_slot: slot,
        last_slot: slot,
        slot_count: 1,
        classification,
    });
    Ok(())
}

fn append_failure_range(
    ranges: &mut Vec<FailureRange>,
    slot: u64,
    failure_category: &'static str,
) -> Result<()> {
    if let Some(last) = ranges.last_mut()
        && last.failure_category == failure_category
        && last.last_slot.checked_add(1) == Some(slot)
    {
        last.last_slot = slot;
        last.slot_count = last
            .slot_count
            .checked_add(1)
            .context("failure range count overflow")?;
        return Ok(());
    }
    ranges.push(FailureRange {
        first_slot: slot,
        last_slot: slot,
        slot_count: 1,
        failure_category,
    });
    Ok(())
}

fn append_exact_range(ranges: &mut Vec<ExactSlotRange>, slot: u64) -> Result<()> {
    if let Some(last) = ranges.last_mut()
        && last.last_slot.checked_add(1) == Some(slot)
    {
        last.last_slot = slot;
        last.slot_count = last
            .slot_count
            .checked_add(1)
            .context("exact slot range count overflow")?;
        return Ok(());
    }
    ranges.push(exact_range(slot, slot)?);
    Ok(())
}

fn excluded_ranges(
    epoch_start: u64,
    epoch_end: u64,
    coverage_start: u64,
    coverage_end: u64,
) -> Result<Vec<ExactSlotRange>> {
    let mut ranges = Vec::new();
    if epoch_start < coverage_start {
        ranges.push(exact_range(epoch_start, coverage_start - 1)?);
    }
    if coverage_end < epoch_end {
        ranges.push(exact_range(coverage_end + 1, epoch_end)?);
    }
    Ok(ranges)
}

fn exact_range(first_slot: u64, last_slot: u64) -> Result<ExactSlotRange> {
    ensure!(first_slot <= last_slot, "invalid exact slot range");
    Ok(ExactSlotRange {
        first_slot,
        last_slot,
        slot_count: last_slot
            .checked_sub(first_slot)
            .and_then(|span| span.checked_add(1))
            .context("slot range count overflow")?,
    })
}

fn for_each_slot<F>(start: u64, end: u64, mut visit: F) -> Result<()>
where
    F: FnMut(u64) -> Result<()>,
{
    let mut slot = start;
    loop {
        visit(slot)?;
        if slot == end {
            return Ok(());
        }
        slot = slot.checked_add(1).context("slot range overflow")?;
    }
}

fn truncate_error(message: String) -> String {
    let mut characters = message.chars();
    let truncated = characters
        .by_ref()
        .take(MAX_ERROR_CHARS)
        .collect::<String>();
    if characters.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn parse_journal_id(value: &str) -> Result<[u8; 16], String> {
    if value.len() != 32 {
        return Err("journal id must be exactly 32 hexadecimal characters".into());
    }
    let mut journal_id = [0u8; 16];
    for (index, byte) in journal_id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "journal id must be hexadecimal")?;
    }
    Ok(journal_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_parser_accepts_plain_and_json_rpc_arrays() {
        let plain = parse_canonical_slots(b"[10,11,13]", 10, 20).unwrap();
        let wrapped =
            parse_canonical_slots(br#"{"jsonrpc":"2.0","result":[10,11,13]}"#, 10, 20).unwrap();
        assert_eq!(plain, wrapped);
        assert_eq!(plain.into_iter().collect::<Vec<_>>(), [10, 11, 13]);
    }

    #[test]
    fn interval_sweep_accounts_for_a_late_slot_observation() {
        let extents = BTreeMap::from([
            (
                10,
                SlotExtent {
                    first_sequence: 1,
                    last_sequence: 100,
                    record_count: 1,
                    decoded_datagram_bytes: 100,
                    estimated_buffer_bytes: 1_000,
                },
            ),
            (
                11,
                SlotExtent {
                    first_sequence: 2,
                    last_sequence: 3,
                    record_count: 1,
                    decoded_datagram_bytes: 100,
                    estimated_buffer_bytes: 2_000,
                },
            ),
        ]);
        let plan = plan_memory(&extents, None, u64::MAX).unwrap();
        assert_eq!(plan.predicted_peak_buffer_bytes, 3_000);
        assert!(plan.predicted_peak_with_recovery_bytes >= 17_000);
    }

    #[test]
    fn classification_ranges_split_on_category_changes() {
        let mut ranges = Vec::new();
        append_classification_range(&mut ranges, 10, Classification::NotRecorded).unwrap();
        append_classification_range(&mut ranges, 11, Classification::NotRecorded).unwrap();
        append_classification_range(&mut ranges, 12, Classification::Reconstructed).unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].slot_count, 2);
        assert_eq!(ranges[1].first_slot, 12);
    }

    #[test]
    fn explicit_coverage_excludes_partial_epoch_edges() {
        let ranges = excluded_ranges(10, 20, 12, 18).unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!((ranges[0].first_slot, ranges[0].last_slot), (10, 11));
        assert_eq!((ranges[1].first_slot, ranges[1].last_slot), (19, 20));
    }

    #[test]
    fn empty_repair_input_preserves_the_raw_only_slot_path() {
        let extent = SlotExtent {
            first_sequence: 10,
            last_sequence: 10,
            record_count: 1,
            decoded_datagram_bytes: 0,
            estimated_buffer_bytes: SLOT_BUFFER_OVERHEAD_BYTES,
        };

        let outcome = audit_slot(10, extent, Vec::new(), Vec::new());

        assert!(outcome.repair_merge.is_none());
        assert_eq!(outcome.reconstruction.status, ReconstructionStatus::Failed);
    }

    #[test]
    fn canonical_classification_accounts_for_the_whole_epoch() {
        let canonical = BTreeSet::from([10, 11, 12, 14, 15]);
        let mut outcomes = BTreeMap::from([
            (
                11,
                test_outcome(11, ReconstructionStatus::Reconstructed, None),
            ),
            (
                12,
                test_outcome(
                    12,
                    ReconstructionStatus::Failed,
                    Some("missing_slot_boundary"),
                ),
            ),
            (
                13,
                test_outcome(
                    13,
                    ReconstructionStatus::Failed,
                    Some("incomplete_data_shreds"),
                ),
            ),
        ]);

        let output = classify_slots(10, 15, 11, 14, Some(&canonical), &mut outcomes).unwrap();

        assert_eq!(output.counts.reconstructed, 1);
        assert_eq!(output.counts.missed_capture, 2);
        assert_eq!(output.counts.not_recorded, 2);
        assert_eq!(output.counts.observed_noncanonical, 1);
        assert_eq!(
            output.failure_category_counts.get(NO_SHREDS_OBSERVED),
            Some(&1)
        );
        assert!(output.failure_ranges.iter().any(|range| {
            range.first_slot == 14
                && range.last_slot == 14
                && range.failure_category == NO_SHREDS_OBSERVED
        }));
        assert_eq!(
            output.counts.reconstructed + output.counts.missed_capture + output.counts.not_recorded,
            canonical.len() as u64
        );
        assert_eq!(
            outcomes.get(&13).unwrap().classification,
            Some(Classification::ObservedNoncanonical)
        );
    }

    fn test_outcome(
        slot: u64,
        status: ReconstructionStatus,
        failure_category: Option<&'static str>,
    ) -> ObservedSlotReport {
        ObservedSlotReport {
            slot,
            extent: SlotExtent {
                first_sequence: slot,
                last_sequence: slot,
                record_count: 1,
                decoded_datagram_bytes: 1,
                estimated_buffer_bytes: 1,
            },
            classification: None,
            reconstruction: ReconstructionReport {
                status,
                failure_category,
                error: None,
                recovered_data_shreds: 0,
                under_threshold_fec_sets: 0,
                total_threshold_deficit: 0,
                unknown_threshold_fec_sets: 0,
                threshold_deficits: Vec::new(),
                block: None,
            },
            repair_merge: None,
        }
    }
}
