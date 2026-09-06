//! Lossless first-stage conversion from a complete set of raw data shreds into Solana components.
//!
//! This deliberately produces *provisional* blocks only. A shred stream is pre-execution and may
//! include non-canonical forks; archive publication must wait for independent rooted-slot evidence.

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context, Result, bail, ensure};
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::Serialize;
use solana_entry::block_component::BlockComponent;
use solana_ledger_compat::{DATA_SHREDS_PER_FEC_BLOCK, Shred};

use super::{
    LogicalKey, ShredKind, SpoolJournalIdentity, read_spool_committed_snapshot_after,
    shred_udp::{ZSTD_SOLANA_SHRED_V1, decode_stored_shred},
};

/// A block reconstructed solely from a complete, internally consistent set of data shreds.
/// Execution-only fields (transaction metadata, rewards, block time and block height) are absent
/// by construction and must never be fabricated by the shred path.
#[derive(Debug)]
pub struct ProvisionalShredBlock {
    pub slot: u64,
    /// Parent encoded by the data-shred common header. Ordered block markers are retained in
    /// `components` and may carry a later parent update that compaction must interpret.
    pub parent_slot: u64,
    /// Exact decoded component order, including entry batches and block markers.
    pub components: Vec<BlockComponent>,
    pub first_data_shred_index: u32,
    pub last_data_shred_index: u32,
}

impl ProvisionalShredBlock {
    pub fn final_poh_hash(&self) -> Option<[u8; 32]> {
        self.components.iter().rev().find_map(|component| {
            let BlockComponent::EntryBatch(entries) = component else {
                return None;
            };
            entries.last().map(|entry| entry.hash.to_bytes())
        })
    }

    pub fn entry_count(&self) -> usize {
        self.components
            .iter()
            .map(|component| match component {
                BlockComponent::EntryBatch(entries) => entries.len(),
                BlockComponent::BlockMarker(_) => 0,
            })
            .sum()
    }

    pub fn transaction_count(&self) -> usize {
        self.components
            .iter()
            .map(|component| match component {
                BlockComponent::EntryBatch(entries) => {
                    entries.iter().map(|entry| entry.transactions.len()).sum()
                }
                BlockComponent::BlockMarker(_) => 0,
            })
            .sum()
    }

    pub fn block_marker_count(&self) -> usize {
        self.components
            .iter()
            .filter(|component| matches!(component, BlockComponent::BlockMarker(_)))
            .count()
    }
}

/// Read-only bounds for trying a reconstruction against an already durable shred spool prefix.
/// No cursor, output block, or source WAL data is written by this operation.
#[derive(Debug, Clone)]
pub struct ShredSpoolTrialConfig {
    pub spool_root: PathBuf,
    pub identity: SpoolJournalIdentity,
    pub durable_through_sequence: u64,
    pub max_record_bytes: u64,
    pub max_records: usize,
    pub max_candidate_slots: usize,
    /// Ignore older slots while scanning a large durable prefix. This limits retained in-memory
    /// candidates without treating a slot number as a durability cursor.
    pub min_slot: Option<u64>,
    /// Bound representative failure details per category so diagnostics remain useful without
    /// unbounded output or one common failure hiding rarer classes.
    pub max_failure_samples: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShredSpoolTrialReport {
    pub durable_through_sequence: u64,
    pub scanned_records: u64,
    pub decoded_data_shreds: u64,
    pub decoded_coding_shreds: u64,
    pub fec_recovered_data_shreds: u64,
    pub fec_under_threshold_sets: u64,
    pub candidate_slots: usize,
    /// Candidate slots where every observed FEC set met its Reed-Solomon threshold. This is not a
    /// slot-completeness metric because an entirely absent FEC set cannot be inferred from shreds.
    pub fec_threshold_satisfied_slots: usize,
    pub reconstructed_slots: usize,
    pub failures: ShredSpoolTrialFailures,
    pub failure_samples: Vec<ShredSpoolTrialFailureSample>,
    pub reached_durable_tail: bool,
    pub reconstructed: Option<ProvisionalShredBlockSummary>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ShredSpoolTrialFailures {
    pub fec_recovery_error_slots: usize,
    pub fec_identity_conflict_slots: usize,
    pub fec_geometry_conflict_slots: usize,
    pub chained_merkle_conflict_slots: usize,
    pub conflicting_duplicate_slots: usize,
    pub fec_under_threshold_slots: usize,
    pub missing_slot_completion_slots: usize,
    pub missing_index_zero_slots: usize,
    pub data_after_completion_slots: usize,
    pub missing_data_shreds_slots: usize,
    pub incomplete_data_range_slots: usize,
    pub component_decode_slots: usize,
    pub empty_entry_slots: usize,
    pub other_slots: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShredSpoolTrialFailureSample {
    pub slot: u64,
    pub category: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProvisionalShredBlockSummary {
    pub slot: u64,
    pub parent_slot: u64,
    pub data_shred_index_start: u32,
    pub data_shred_index_end: u32,
    pub entry_count: usize,
    pub transaction_count: usize,
    pub block_marker_count: usize,
    pub final_poh_hash: Option<String>,
}

#[derive(Default)]
struct TrialSlot {
    datagrams: Vec<Vec<u8>>,
}

/// Search a bounded durable spool prefix for one complete data-shred slot and deshred it.
///
/// This is a diagnostic/recovery primitive. It uses coding shreds only to recover missing data
/// shreds in memory and never writes a compact block: a success proves entry reconstruction, not
/// canonicality.
pub fn trial_deshred_spool(config: ShredSpoolTrialConfig) -> Result<ShredSpoolTrialReport> {
    ensure!(
        config.max_record_bytes > 0,
        "max_record_bytes must be non-zero"
    );
    ensure!(config.max_records > 0, "max_records must be non-zero");
    ensure!(
        config.max_candidate_slots > 0,
        "max_candidate_slots must be non-zero"
    );

    let mut candidates = BTreeMap::<u64, TrialSlot>::new();
    let mut scanned_records = 0u64;
    let mut decoded_data_shreds = 0u64;
    let mut decoded_coding_shreds = 0u64;
    let mut fec_recovered_data_shreds = 0u64;
    let mut fec_under_threshold_sets = 0u64;
    let mut failures = ShredSpoolTrialFailures::default();
    let mut failure_samples = Vec::new();
    let snapshot = read_spool_committed_snapshot_after(
        &config.spool_root,
        config.identity,
        config.max_record_bytes,
        None,
        config.durable_through_sequence,
        config.max_records,
        |record| {
            scanned_records = scanned_records.saturating_add(1);
            let LogicalKey::Shred {
                slot,
                kind,
                shred_index,
                fec_set_index,
            } = record.metadata.logical_key
            else {
                bail!("non-shred record found in raw shred spool");
            };
            if config.min_slot.is_some_and(|minimum| slot < minimum) {
                return Ok(());
            }
            ensure!(
                record.metadata.payload_format_version == ZSTD_SOLANA_SHRED_V1,
                "trial deshredder accepts only canonical compressed raw shreds"
            );
            let datagram = decode_stored_shred(&record.payload)
                .context("decode compressed raw shred from durable spool")?;
            let shred = parse_shred(&datagram, "decode raw Solana shred from durable spool")?;
            let decoded_kind = if shred.is_code() {
                ShredKind::Coding
            } else {
                ShredKind::Data
            };
            let decoded_fec_set_index = u32_at(
                shred.payload(),
                79,
                "read decoded shred FEC-set index from durable spool",
            )?;
            ensure!(
                slot == shred.slot()
                    && kind == decoded_kind
                    && shred_index == shred.index()
                    && fec_set_index == Some(decoded_fec_set_index),
                "stored shred metadata differs from its decoded canonical payload"
            );
            if !retain_latest_candidate_slot(&mut candidates, slot, config.max_candidate_slots) {
                return Ok(());
            }
            let candidate = candidates.entry(slot).or_default();
            candidate.datagrams.push(datagram);
            if !shred.is_code() {
                decoded_data_shreds = decoded_data_shreds.saturating_add(1);
            } else {
                decoded_coding_shreds = decoded_coding_shreds.saturating_add(1);
            }
            Ok(())
        },
    )?;

    let mut fec_threshold_satisfied_slots = 0usize;
    let mut reconstructed_slots = 0usize;
    let mut reconstructed = None;
    for (&slot_number, slot) in &candidates {
        let recovered = match recover_slot_data_shreds(&slot.datagrams) {
            Ok(recovered) => recovered,
            Err(error) => {
                record_trial_failure(
                    &mut failures,
                    &mut failure_samples,
                    config.max_failure_samples,
                    slot_number,
                    &error,
                );
                continue;
            }
        };
        fec_under_threshold_sets =
            fec_under_threshold_sets.saturating_add(recovered.under_threshold_fec_sets as u64);
        if recovered.under_threshold_fec_sets == 0 {
            fec_threshold_satisfied_slots = fec_threshold_satisfied_slots.saturating_add(1);
        } else {
            failures.fec_under_threshold_slots =
                failures.fec_under_threshold_slots.saturating_add(1);
        }
        fec_recovered_data_shreds =
            fec_recovered_data_shreds.saturating_add(recovered.recovered_data_shred_count as u64);
        let block = match deshred_complete_data_slot(recovered.data_shreds.iter()) {
            Ok(block) => block,
            Err(error) => {
                record_trial_failure(
                    &mut failures,
                    &mut failure_samples,
                    config.max_failure_samples,
                    slot_number,
                    &error,
                );
                continue;
            }
        };
        reconstructed_slots = reconstructed_slots.saturating_add(1);
        if reconstructed.is_none() {
            reconstructed = Some(ProvisionalShredBlockSummary {
                slot: block.slot,
                parent_slot: block.parent_slot,
                data_shred_index_start: block.first_data_shred_index,
                data_shred_index_end: block.last_data_shred_index,
                entry_count: block.entry_count(),
                transaction_count: block.transaction_count(),
                block_marker_count: block.block_marker_count(),
                final_poh_hash: block.final_poh_hash().map(hex_hash),
            });
        }
    }

    Ok(ShredSpoolTrialReport {
        durable_through_sequence: config.durable_through_sequence,
        scanned_records,
        decoded_data_shreds,
        decoded_coding_shreds,
        fec_recovered_data_shreds,
        fec_under_threshold_sets,
        candidate_slots: candidates.len(),
        fec_threshold_satisfied_slots,
        reconstructed_slots,
        failures,
        failure_samples,
        reached_durable_tail: snapshot.reached_durable_tail,
        reconstructed,
    })
}

fn retain_latest_candidate_slot(
    candidates: &mut BTreeMap<u64, TrialSlot>,
    slot: u64,
    maximum: usize,
) -> bool {
    if candidates.contains_key(&slot) {
        return true;
    }
    if candidates.len() < maximum {
        candidates.insert(slot, TrialSlot::default());
        return true;
    }
    let oldest = *candidates
        .first_key_value()
        .expect("a full candidate map is non-empty")
        .0;
    if slot <= oldest {
        return false;
    }
    candidates.remove(&oldest);
    candidates.insert(slot, TrialSlot::default());
    true
}

const SIGNATURE_BYTES: usize = 64;
const COMMON_HEADER_BYTES: usize = 83;
const CODING_HEADER_BYTES: usize = 89;
const MERKLE_ROOT_BYTES: usize = 32;
// Agave truncates each Merkle proof node to 20 bytes on the shred wire. This is deliberately not
// `Hash::NUM_BYTES`: using the 32-byte Merkle root size here shortens every Reed-Solomon shard by
// 12 bytes per proof level and silently corrupts recovered data shreds.
const MERKLE_PROOF_ENTRY_BYTES: usize = 20;
const DATA_PAYLOAD_DELTA_FROM_CODING: usize = CODING_HEADER_BYTES - SIGNATURE_BYTES;

#[derive(Default)]
struct FecSet {
    data: BTreeMap<u32, Vec<u8>>,
    coding: BTreeMap<u16, Vec<u8>>,
    coding_config: Option<FecCodingConfig>,
    identity: Option<FecIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FecCodingConfig {
    num_data: usize,
    num_coding: usize,
    erasure_shard_len: usize,
    recovered_data_payload_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FecIdentity {
    version: u16,
    leader_signature: [u8; SIGNATURE_BYTES],
    merkle_root: [u8; MERKLE_ROOT_BYTES],
    chained_merkle_root: [u8; MERKLE_ROOT_BYTES],
    proof_size: u8,
    resigned: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FecThresholdDeficit {
    pub fec_set_index: u32,
    pub available_shards: usize,
    /// Exact data-shard threshold, using Agave's fixed width for data-only observations.
    pub required_shards: Option<usize>,
    /// Exact threshold deficit whenever `required_shards` is present.
    pub missing_shards: Option<usize>,
}

/// In-memory result of one slot's FEC recovery pass.
#[derive(Debug)]
pub struct RecoveredSlotData {
    pub data_shreds: Vec<Vec<u8>>,
    pub recovered_data_shred_count: usize,
    pub under_threshold_fec_sets: usize,
    pub total_threshold_deficit: usize,
    pub unknown_threshold_fec_sets: usize,
    pub threshold_deficits: Vec<FecThresholdDeficit>,
}

/// Recovers missing data shreds from coding shreds, entirely in memory.
///
/// Agave keeps its FEC helper private. This uses the same Reed-Solomon input layout: data shards
/// encode bytes from offset 64 and coding shards encode bytes from offset 89. The trailing Merkle
/// proof is intentionally not reconstructed because entry deserialization consumes only the
/// recovered common/data headers and ledger-data range. Every recovered data shred is reparsed and
/// checked against its expected slot/index before use.
pub fn recover_slot_data_shreds(datagrams: &[Vec<u8>]) -> Result<RecoveredSlotData> {
    let mut slot = None;
    let mut data = BTreeMap::<u32, Vec<u8>>::new();
    let mut fec_sets = BTreeMap::<u32, FecSet>::new();

    for datagram in datagrams {
        let shred = parse_shred(datagram, "decode raw Solana shred for FEC recovery")?;
        // `new_from_serialized_shred` accepts a repair response with an appended nonce and
        // normalizes it to Agave's canonical payload length. FEC geometry must be derived from that
        // canonical payload, never from the received UDP datagram length.
        let normalized = shred.payload().to_vec();
        let shred_slot = shred.slot();
        if let Some(expected) = slot {
            ensure!(
                expected == shred_slot,
                "FEC candidate contains more than one slot"
            );
        } else {
            slot = Some(shred_slot);
        }
        let fec_set_index = u32_at(&normalized, 79, "read shred FEC-set index")?;
        let index = shred.index();
        let fec_identity = fec_identity(&shred, &normalized)?;
        let fec_set = fec_sets.entry(fec_set_index).or_default();
        if let Some(existing) = fec_set.identity {
            ensure!(
                existing == fec_identity,
                "FEC set has inconsistent signature, Merkle root, version, or proof geometry"
            );
        } else {
            fec_set.identity = Some(fec_identity);
        }
        if shred.is_code() {
            let config = parse_fec_coding_config(&normalized)?;
            let position = u16_at(&normalized, 87, "read coding shred position")?;
            if let Some(existing) = fec_set.coding_config {
                ensure!(
                    existing == config,
                    "FEC set has inconsistent coding-shred geometry"
                );
            } else {
                fec_set.coding_config = Some(config);
            }
            insert_equivalent_shred(&mut fec_set.coding, position, normalized, "coding shred")?;
        } else {
            insert_equivalent_shred(&mut data, index, normalized.clone(), "data shred")?;
            insert_equivalent_shred(&mut fec_set.data, index, normalized, "FEC data shred")?;
        }
    }

    let slot = slot.context("no raw shreds supplied for FEC recovery")?;
    validate_adjacent_fec_chains(&fec_sets)?;
    let mut recovered_data_shred_count = 0usize;
    let mut threshold_deficits = Vec::new();
    for (fec_set_index, fec_set) in fec_sets {
        let Some(config) = fec_set.coding_config else {
            // Coding geometry is unnecessary when all fixed-width data shreds are already
            // present. Current Agave FEC sets have a fixed data width, so even without an
            // observed coding shred the threshold deficit is exact.
            if fec_set.data.len() < DATA_SHREDS_PER_FEC_BLOCK {
                threshold_deficits.push(FecThresholdDeficit {
                    fec_set_index,
                    available_shards: fec_set.data.len(),
                    required_shards: Some(DATA_SHREDS_PER_FEC_BLOCK),
                    missing_shards: Some(DATA_SHREDS_PER_FEC_BLOCK - fec_set.data.len()),
                });
            }
            continue;
        };
        let total = config
            .num_data
            .checked_add(config.num_coding)
            .context("FEC shard count overflow")?;
        let mut shards = vec![None; total];
        let signature = fec_set
            .identity
            .context("FEC set has no validated identity")?
            .leader_signature;
        for (index, datagram) in fec_set.data {
            let local_index = index
                .checked_sub(fec_set_index)
                .context("data shred index precedes its FEC set")?
                as usize;
            ensure!(
                local_index < config.num_data,
                "data shred is outside its FEC set"
            );
            shards[local_index] = Some(data_erasure_shard(datagram, config.erasure_shard_len)?);
        }
        for (position, datagram) in fec_set.coding {
            let local_index = config.num_data + usize::from(position);
            ensure!(local_index < total, "coding shred is outside its FEC set");
            shards[local_index] = Some(coding_erasure_shard(datagram, config.erasure_shard_len)?);
        }
        let available_shards = shards.iter().flatten().count();
        if available_shards < config.num_data {
            threshold_deficits.push(FecThresholdDeficit {
                fec_set_index,
                available_shards,
                required_shards: Some(config.num_data),
                missing_shards: Some(config.num_data - available_shards),
            });
            continue;
        }
        let decoder = ReedSolomon::new(config.num_data, config.num_coding)
            .context("create Reed-Solomon FEC decoder")?;
        decoder
            .reconstruct(&mut shards)
            .context("recover FEC shreds")?;
        let complete_shards = shards
            .iter()
            .map(|shard| {
                shard
                    .as_deref()
                    .context("Reed-Solomon did not reconstruct a required shard")
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            decoder
                .verify(&complete_shards)
                .context("verify reconstructed FEC parity")?,
            "reconstructed FEC parity is inconsistent"
        );

        for (local_index, shard) in shards.into_iter().take(config.num_data).enumerate() {
            let index = fec_set_index
                .checked_add(u32::try_from(local_index).context("FEC data index exceeds u32")?)
                .context("FEC data index overflow")?;
            if data.contains_key(&index) {
                continue;
            }
            let shard = shard.context("Reed-Solomon did not reconstruct a required data shred")?;
            let recovered =
                recovered_data_datagram(&signature, &shard, config.recovered_data_payload_len)?;
            let parsed = parse_shred(&recovered, "parse recovered FEC data shred")?;
            ensure!(
                !parsed.is_code() && parsed.slot() == slot && parsed.index() == index,
                "recovered data shred does not match its expected slot/index"
            );
            data.insert(index, recovered);
            recovered_data_shred_count = recovered_data_shred_count.saturating_add(1);
        }
    }
    let total_threshold_deficit = threshold_deficits
        .iter()
        .filter_map(|deficit| deficit.missing_shards)
        .sum();
    let unknown_threshold_fec_sets = threshold_deficits
        .iter()
        .filter(|deficit| deficit.required_shards.is_none())
        .count();
    Ok(RecoveredSlotData {
        data_shreds: data.into_values().collect(),
        recovered_data_shred_count,
        under_threshold_fec_sets: threshold_deficits.len(),
        total_threshold_deficit,
        unknown_threshold_fec_sets,
        threshold_deficits,
    })
}

/// Reject an individually valid but cross-fork mosaic before its data shreds are concatenated.
///
/// Every shred in a current Agave FEC set commits to both that set's Merkle root and the previous
/// set's root. Agave's current FEC geometry fixes the data width, so a data-only observed set still
/// anchors the following set. A wholly absent set leaves a non-adjacent index gap and is
/// intentionally not mislabeled as a fork.
fn validate_adjacent_fec_chains(fec_sets: &BTreeMap<u32, FecSet>) -> Result<()> {
    let mut previous: Option<(u32, usize, FecIdentity)> = None;
    for (&fec_set_index, fec_set) in fec_sets {
        let identity = fec_set
            .identity
            .context("FEC set has no validated identity")?;
        if let Some((previous_index, previous_data_count, previous_identity)) = previous {
            let expected_index = previous_index
                .checked_add(
                    u32::try_from(previous_data_count).context("FEC data count exceeds u32")?,
                )
                .context("next FEC-set index overflow")?;
            if fec_set_index == expected_index {
                ensure!(
                    identity.chained_merkle_root == previous_identity.merkle_root,
                    "adjacent FEC sets have conflicting chained Merkle roots at {previous_index} and {fec_set_index}"
                );
            }
        }
        previous = Some((fec_set_index, DATA_SHREDS_PER_FEC_BLOCK, identity));
    }
    Ok(())
}

fn record_trial_failure(
    failures: &mut ShredSpoolTrialFailures,
    samples: &mut Vec<ShredSpoolTrialFailureSample>,
    max_samples: usize,
    slot: u64,
    error: &anyhow::Error,
) {
    let message = format!("{error:#}");
    let category = shred_reconstruction_failure_category(error);
    let counter = if category == "chained_merkle_conflict" {
        &mut failures.chained_merkle_conflict_slots
    } else if category == "conflicting_duplicate" {
        &mut failures.conflicting_duplicate_slots
    } else if category == "fec_identity_conflict" {
        &mut failures.fec_identity_conflict_slots
    } else if category == "fec_geometry_conflict" {
        &mut failures.fec_geometry_conflict_slots
    } else if category == "missing_slot_completion" {
        &mut failures.missing_slot_completion_slots
    } else if category == "missing_index_zero" {
        &mut failures.missing_index_zero_slots
    } else if category == "data_after_completion" {
        &mut failures.data_after_completion_slots
    } else if category == "missing_data_shreds" {
        &mut failures.missing_data_shreds_slots
    } else if category == "incomplete_data_range" {
        &mut failures.incomplete_data_range_slots
    } else if category == "component_decode" {
        &mut failures.component_decode_slots
    } else if category == "empty_entries" {
        &mut failures.empty_entry_slots
    } else if category == "fec_recovery" {
        &mut failures.fec_recovery_error_slots
    } else {
        &mut failures.other_slots
    };
    *counter = counter.saturating_add(1);
    if samples
        .iter()
        .filter(|sample| sample.category == category)
        .count()
        < max_samples
    {
        samples.push(ShredSpoolTrialFailureSample {
            slot,
            category,
            message,
        });
    }
}

/// Stable, bounded-cardinality category for a reconstruction failure.
///
/// Detailed error text remains diagnostic-only; callers should aggregate this returned value.
pub fn shred_reconstruction_failure_category(error: &anyhow::Error) -> &'static str {
    let message = format!("{error:#}");
    if message.contains("conflicting chained Merkle roots") {
        "chained_merkle_conflict"
    } else if message.contains("conflicting duplicate") {
        "conflicting_duplicate"
    } else if message
        .contains("FEC set has inconsistent signature, Merkle root, version, or proof geometry")
    {
        "fec_identity_conflict"
    } else if message.contains("FEC set has inconsistent coding-shred geometry") {
        "fec_geometry_conflict"
    } else if message.contains("slot completion shred is missing") {
        "missing_slot_completion"
    } else if message.contains("slot begins at data shred") {
        "missing_index_zero"
    } else if message.contains("after declared completion") {
        "data_after_completion"
    } else if message.contains("slot has missing data shreds") {
        "missing_data_shreds"
    } else if message.contains("incomplete shred data range") {
        "incomplete_data_range"
    } else if message.contains("decode completed shred data range") {
        "component_decode"
    } else if message.contains("contains no entries")
        || message.contains("completed shred range has no entries")
    {
        "empty_entries"
    } else if message.contains("FEC") || message.contains("Reed-Solomon") {
        "fec_recovery"
    } else {
        "other"
    }
}

fn parse_fec_coding_config(datagram: &[u8]) -> Result<FecCodingConfig> {
    ensure!(
        datagram.len() >= CODING_HEADER_BYTES,
        "coding shred is shorter than its header"
    );
    let variant = *datagram
        .get(SIGNATURE_BYTES)
        .context("coding shred has no variant")?;
    ensure!(
        matches!(variant & 0xf0, 0x60 | 0x70),
        "coding shred has an unsupported variant"
    );
    let proof_size = usize::from(variant & 0x0f);
    let resigned = variant & 0xf0 == 0x70;
    let trailer_bytes = MERKLE_ROOT_BYTES
        .checked_add(
            proof_size
                .checked_mul(MERKLE_PROOF_ENTRY_BYTES)
                .context("Merkle proof length overflow")?,
        )
        .and_then(|bytes| bytes.checked_add(if resigned { SIGNATURE_BYTES } else { 0 }))
        .context("coding shred trailer length overflow")?;
    let erasure_end = datagram
        .len()
        .checked_sub(trailer_bytes)
        .context("coding shred is shorter than its Merkle trailer")?;
    let erasure_shard_len = erasure_end
        .checked_sub(CODING_HEADER_BYTES)
        .context("coding shred is shorter than its erasure header")?;
    ensure!(
        erasure_shard_len > COMMON_HEADER_BYTES,
        "coding erasure shard is too short"
    );
    Ok(FecCodingConfig {
        num_data: usize::from(u16_at(datagram, 83, "read FEC data-shred count")?),
        num_coding: usize::from(u16_at(datagram, 85, "read FEC coding-shred count")?),
        erasure_shard_len,
        recovered_data_payload_len: datagram
            .len()
            .checked_sub(DATA_PAYLOAD_DELTA_FROM_CODING)
            .context("coding payload length cannot produce a data payload length")?,
    })
}

fn data_erasure_shard(datagram: Vec<u8>, expected_len: usize) -> Result<Vec<u8>> {
    let end = SIGNATURE_BYTES
        .checked_add(expected_len)
        .context("data erasure-shard end overflow")?;
    datagram
        .get(SIGNATURE_BYTES..end)
        .filter(|bytes| bytes.len() == expected_len)
        .map(ToOwned::to_owned)
        .context("data shred is shorter than its FEC erasure shard")
}

fn coding_erasure_shard(datagram: Vec<u8>, expected_len: usize) -> Result<Vec<u8>> {
    let end = CODING_HEADER_BYTES
        .checked_add(expected_len)
        .context("coding erasure-shard end overflow")?;
    datagram
        .get(CODING_HEADER_BYTES..end)
        .filter(|bytes| bytes.len() == expected_len)
        .map(ToOwned::to_owned)
        .context("coding shred is shorter than its FEC erasure shard")
}

fn fec_identity(shred: &Shred, datagram: &[u8]) -> Result<FecIdentity> {
    let variant = *datagram
        .get(SIGNATURE_BYTES)
        .context("shred has no variant")?;
    let high = variant & 0xf0;
    ensure!(
        matches!(high, 0x60 | 0x70 | 0x90 | 0xb0),
        "shred has an unsupported Merkle variant"
    );
    let mut leader_signature = [0u8; SIGNATURE_BYTES];
    leader_signature.copy_from_slice(shred.signature().as_ref());
    Ok(FecIdentity {
        version: shred.version(),
        leader_signature,
        merkle_root: shred
            .merkle_root()
            .map_err(|error| anyhow::anyhow!("derive shred Merkle root: {error:?}"))?,
        chained_merkle_root: shred
            .chained_merkle_root()
            .map_err(|error| anyhow::anyhow!("derive chained shred Merkle root: {error:?}"))?,
        proof_size: variant & 0x0f,
        resigned: matches!(high, 0x70 | 0xb0),
    })
}

fn recovered_data_datagram(
    signature: &[u8; SIGNATURE_BYTES],
    shard: &[u8],
    data_payload_len: usize,
) -> Result<Vec<u8>> {
    let end = SIGNATURE_BYTES
        .checked_add(shard.len())
        .context("recovered data-shred end overflow")?;
    ensure!(
        end <= data_payload_len,
        "recovered FEC shard exceeds data payload length"
    );
    let mut output = vec![0u8; data_payload_len];
    output[..SIGNATURE_BYTES].copy_from_slice(signature);
    output[SIGNATURE_BYTES..end].copy_from_slice(shard);
    Ok(output)
}

fn u16_at(bytes: &[u8], offset: usize, context: &str) -> Result<u16> {
    bytes
        .get(offset..offset + 2)
        .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
        .map(u16::from_le_bytes)
        .context(context.to_owned())
}

fn u32_at(bytes: &[u8], offset: usize, context: &str) -> Result<u32> {
    bytes
        .get(offset..offset + 4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_le_bytes)
        .context(context.to_owned())
}

fn insert_equivalent_shred<K: Ord + std::fmt::Display>(
    values: &mut BTreeMap<K, Vec<u8>>,
    key: K,
    value: Vec<u8>,
    label: &str,
) -> Result<()> {
    if let Some(existing) = values.get(&key) {
        if existing == &value {
            return Ok(());
        }
        let existing = parse_shred(existing, "parse first duplicate shred")?;
        let candidate = parse_shred(&value, "parse repeated duplicate shred")?;
        ensure!(
            existing.id() == candidate.id() && !existing.is_shred_duplicate(&candidate),
            "conflicting duplicate {label} {key}"
        );
    } else {
        values.insert(key, value);
    }
    Ok(())
}

fn hex_hash(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Deshred one complete slot from raw UDP datagrams.
///
/// Coding shreds are ignored here and are never treated as entry bytes. Callers that need recovery
/// first pass their complete candidate through the FEC stage above.
pub fn deshred_complete_data_slot<I, B>(datagrams: I) -> Result<ProvisionalShredBlock>
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    let mut slot = None;
    let mut parent_slot = None;
    let mut data = BTreeMap::<u32, Vec<u8>>::new();
    let mut last_index = None;

    for datagram in datagrams {
        let shred = parse_shred(datagram.as_ref(), "decode raw Solana shred")?;
        if shred.is_code() {
            continue;
        }
        let shred_slot = shred.slot();
        let shred_parent = shred
            .parent()
            .map_err(|error| anyhow::anyhow!("read data shred parent slot: {error:?}"))?;
        if let Some(expected) = slot {
            ensure!(
                expected == shred_slot,
                "cannot deshred more than one slot at once"
            );
            ensure!(
                parent_slot == Some(shred_parent),
                "slot has conflicting parent slots"
            );
        } else {
            slot = Some(shred_slot);
            parent_slot = Some(shred_parent);
        }
        let index = shred.index();
        let payload = shred.into_payload().to_vec();
        insert_equivalent_shred(&mut data, index, payload.clone(), "data shred")?;
        if parse_shred(&payload, "re-read data shred payload")?.last_in_slot() {
            if let Some(previous) = last_index {
                ensure!(
                    previous == index,
                    "slot has conflicting last-in-slot shreds"
                );
            }
            last_index = Some(index);
        }
    }

    let slot = slot.context("no data shreds supplied")?;
    let parent_slot = parent_slot.context("data shreds omitted parent slot")?;
    let last_index = last_index.context("slot completion shred is missing")?;
    let first_index = validate_complete_data_index_range(&data, last_index)?;

    // A slot is not one serialized Vec<Entry>. Agave marks the end of every independently
    // serialized block component with DATA_COMPLETE_SHRED and deshreds each completed range on its
    // own. Concatenating the whole slot can either fail decoding or, worse, decode only the first
    // entry batch while silently ignoring the rest of the slot.
    let mut components = Vec::new();
    let mut entry_count = 0usize;
    let mut completed_range = Vec::new();
    let mut completed_range_start = None;
    for payload in data.into_values() {
        let shred = parse_shred(&payload, "read data-complete boundary")?;
        let index = shred.index();
        let data_complete = shred.data_complete();
        completed_range_start.get_or_insert(index);
        completed_range.push(payload);
        if !data_complete {
            continue;
        }
        let bytes = deshred_data_shreds(completed_range.drain(..))?;
        let start = completed_range_start
            .take()
            .expect("a completed range always has a first shred");
        // Match Agave Blockstore's production path. A completed component can retain padding after
        // the encoded value; `deserialize_exact` rejects that valid suffix and caused false decode
        // failures in the reconstruction trial.
        let component = decode_block_component(&bytes, start, index)?;
        retain_block_component(component, &mut components, &mut entry_count)?;
    }
    ensure!(
        completed_range.is_empty(),
        "slot ends with an incomplete shred data range"
    );
    if entry_count == 0 {
        bail!("deshredded slot {slot} contains no entries");
    }
    Ok(ProvisionalShredBlock {
        slot,
        parent_slot,
        components,
        first_data_shred_index: first_index,
        last_data_shred_index: last_index,
    })
}

fn retain_block_component(
    component: BlockComponent,
    components: &mut Vec<BlockComponent>,
    entry_count: &mut usize,
) -> Result<()> {
    if let BlockComponent::EntryBatch(batch) = &component {
        ensure!(!batch.is_empty(), "completed shred range has no entries");
        *entry_count = entry_count
            .checked_add(batch.len())
            .context("slot entry count overflow")?;
    }
    components.push(component);
    Ok(())
}

fn decode_block_component(bytes: &[u8], start: u32, end: u32) -> Result<BlockComponent> {
    wincode::deserialize(bytes).with_context(|| {
        format!(
            "decode completed shred data range {start}..={end} ({} bytes)",
            bytes.len()
        )
    })
}

fn validate_complete_data_index_range(
    data: &BTreeMap<u32, Vec<u8>>,
    last_index: u32,
) -> Result<u32> {
    let first_index = *data.keys().next().context("no stored data shreds")?;
    ensure!(
        first_index == 0,
        "slot begins at data shred {first_index}, FEC recovery required"
    );
    let observed_last = *data.keys().next_back().context("no stored data shreds")?;
    ensure!(
        observed_last == last_index,
        "slot contains data shred {observed_last} after declared completion index {last_index}"
    );
    ensure!(
        data.len() == usize::try_from(last_index).unwrap_or(usize::MAX) + 1,
        "slot has missing data shreds; FEC recovery required"
    );
    Ok(first_index)
}

fn parse_shred(bytes: &[u8], context: &str) -> Result<Shred> {
    Shred::new_from_serialized_shred(bytes.to_vec())
        .map_err(|error| anyhow::anyhow!("{context}: {error:?}"))
}

/// Equivalent to Agave's internal `Shredder::deshred`, implemented against its public shred
/// layout API. `Shredder` itself is intentionally private in Agave 4.
fn deshred_data_shreds<I, B>(shreds: I) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    let mut bytes = Vec::new();
    let mut previous: Option<u32> = None;
    for shred in shreds {
        let shred = shred.as_ref();
        let index = solana_ledger_compat::get_data_index(shred)
            .ok_or_else(|| anyhow::anyhow!("data shred is missing its index"))?;
        if let Some(previous) = previous {
            ensure!(
                previous.checked_add(1) == Some(index),
                "data shred indices are not contiguous"
            );
        }
        // Agave keeps `get_data` private. Data shred headers have a stable 88-byte prefix and
        // carry the logical end-of-data offset as a little-endian u16 at bytes 86..88.
        let data_end = shred
            .get(86..88)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .context("data shred is missing its data-size field")?;
        ensure!(
            (88..=shred.len()).contains(&data_end),
            "data shred payload end is outside its frame"
        );
        let data = &shred[88..data_end];
        bytes.extend_from_slice(data);
        previous = Some(index);
    }
    ensure!(!bytes.is_empty(), "deshredded data payload is empty");
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_entry::entry::Entry;

    fn fec_identity(root: u8, chained_root: u8) -> FecIdentity {
        FecIdentity {
            version: 1,
            leader_signature: [3; SIGNATURE_BYTES],
            merkle_root: [root; MERKLE_ROOT_BYTES],
            chained_merkle_root: [chained_root; MERKLE_ROOT_BYTES],
            proof_size: 6,
            resigned: false,
        }
    }

    fn fec_set(identity: FecIdentity) -> FecSet {
        FecSet {
            coding_config: Some(FecCodingConfig {
                num_data: 32,
                num_coding: 32,
                erasure_shard_len: 1_100,
                recovered_data_payload_len: 1_203,
            }),
            identity: Some(identity),
            ..FecSet::default()
        }
    }

    #[test]
    fn empty_input_cannot_be_promoted_to_a_block() {
        let error = deshred_complete_data_slot(Vec::<Vec<u8>>::new()).unwrap_err();
        assert!(error.to_string().contains("no data shreds"));
    }

    #[test]
    fn candidate_window_tracks_the_latest_slots() {
        let mut candidates = BTreeMap::new();
        for slot in [10, 11, 12, 9, 13] {
            retain_latest_candidate_slot(&mut candidates, slot, 3);
        }
        assert_eq!(candidates.keys().copied().collect::<Vec<_>>(), [11, 12, 13]);
    }

    #[test]
    fn rejects_data_after_the_declared_completion_index() {
        let data = BTreeMap::from([(0, Vec::new()), (1, Vec::new()), (2, Vec::new())]);
        let error = validate_complete_data_index_range(&data, 1).unwrap_err();
        assert!(error.to_string().contains("after declared completion"));
    }

    #[test]
    fn accepts_an_adjacent_fec_chain() {
        let sets = BTreeMap::from([
            (0, fec_set(fec_identity(7, 1))),
            (32, fec_set(fec_identity(8, 7))),
        ]);
        validate_adjacent_fec_chains(&sets).unwrap();
    }

    #[test]
    fn rejects_an_adjacent_cross_fork_fec_mosaic() {
        let sets = BTreeMap::from([
            (0, fec_set(fec_identity(7, 1))),
            (32, fec_set(fec_identity(8, 9))),
        ]);
        let error = validate_adjacent_fec_chains(&sets).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("conflicting chained Merkle roots")
        );
    }

    #[test]
    fn classifies_fec_identity_and_geometry_conflicts_separately() {
        let identity = anyhow::anyhow!(
            "FEC set has inconsistent signature, Merkle root, version, or proof geometry"
        );
        let geometry = anyhow::anyhow!("FEC set has inconsistent coding-shred geometry");
        assert_eq!(
            shred_reconstruction_failure_category(&identity),
            "fec_identity_conflict"
        );
        assert_eq!(
            shred_reconstruction_failure_category(&geometry),
            "fec_geometry_conflict"
        );
    }

    #[test]
    fn component_decode_matches_agave_and_accepts_valid_padding() {
        let component = BlockComponent::new_entry_batch(vec![Entry::default()]).unwrap();
        let mut bytes = wincode::serialize(&component).unwrap();
        bytes.extend_from_slice(&[0; 32]);
        assert!(matches!(
            decode_block_component(&bytes, 0, 31).unwrap(),
            BlockComponent::EntryBatch(entries) if entries.len() == 1
        ));
    }

    #[test]
    fn block_markers_are_retained_instead_of_discarded() {
        use solana_entry::block_component::{BlockHeaderV1, VersionedBlockMarker};

        let marker = VersionedBlockMarker::new_block_header(BlockHeaderV1 {
            parent_slot: 42,
            parent_block_id: Default::default(),
        });
        let mut components = Vec::new();
        let mut entry_count = 0;
        retain_block_component(
            BlockComponent::BlockMarker(marker.clone()),
            &mut components,
            &mut entry_count,
        )
        .unwrap();

        assert_eq!(entry_count, 0);
        assert!(matches!(
            components.as_slice(),
            [BlockComponent::BlockMarker(retained)] if retained == &marker
        ));
    }
}
