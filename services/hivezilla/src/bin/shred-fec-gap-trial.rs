//! Read-only FEC coverage report for a bounded window of slots in a durable shred spool.
//!
//! This diagnostic never opens a writer, advances a cursor, or repairs a shred. It reports which
//! data indices are absent, whether the coding shreds already held locally cross the
//! Reed-Solomon recovery threshold, and the smallest useful data-shred repair request set.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use hivezilla::ingest::{LogicalKey, SpoolJournalIdentity, read_spool_committed_snapshot_after};
use hivezilla::ingest::{
    ReplicationStreamId, ShredKind, ZSTD_SOLANA_SHRED_V1, read_receiver_durable_progress,
};
use serde::Serialize;
use solana_ledger::shred::{DATA_SHREDS_PER_FEC_BLOCK, Shred};

#[derive(Debug, Parser)]
#[command(about = "Read-only per-slot FEC coverage report for a raw-shred spool")]
struct Args {
    #[arg(long)]
    spool_root: std::path::PathBuf,
    #[arg(long)]
    cluster_id: String,
    #[arg(long)]
    origin_node_id: String,
    #[arg(long)]
    source_id: String,
    #[arg(long, value_parser = parse_journal_id)]
    journal_id: [u8; 16],
    /// Receiver progress WAL. Its fsynced cursor is the preferred live-journal read boundary.
    #[arg(long, conflicts_with = "durable_through_sequence")]
    receiver_progress_wal: Option<std::path::PathBuf>,
    /// Explicit durable cursor, for a fixed offline analysis snapshot only.
    #[arg(long, conflicts_with = "receiver_progress_wal")]
    durable_through_sequence: Option<u64>,
    #[arg(long, default_value_t = 4096)]
    max_record_bytes: u64,
    /// Maximum records scanned from the retained journal prefix. This is not a tail count.
    #[arg(long, default_value_t = 20_000_000)]
    max_records: usize,
    #[arg(long, default_value_t = 256)]
    max_candidate_slots: usize,
    #[arg(long)]
    min_slot: Option<u64>,
    /// Emit at most this many examples for each coverage class.
    #[arg(long, default_value_t = 4)]
    samples_per_class: usize,
}

#[derive(Debug, Default)]
struct SlotCoverage {
    data_indices: BTreeSet<u32>,
    last_in_slot_indices: BTreeSet<u32>,
    fec_sets: BTreeMap<u32, FecCoverage>,
}

#[derive(Debug, Default)]
struct FecCoverage {
    data_indices: BTreeSet<u32>,
    coding_positions: BTreeSet<u16>,
    geometry: Option<FecGeometry>,
    geometry_conflict: bool,
    identity: Option<FecIdentity>,
    identity_conflict: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FecGeometry {
    num_data: usize,
    num_coding: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FecIdentity {
    version: u16,
    leader_signature: [u8; 64],
    merkle_root: [u8; 32],
    chained_merkle_root: [u8; 32],
    proof_size: u8,
    resigned: bool,
}

#[derive(Debug, Clone, Copy)]
enum FecObservation {
    Data {
        index: u32,
        last_in_slot: bool,
    },
    Coding {
        position: u16,
        geometry: FecGeometry,
    },
}

#[derive(Debug, Serialize)]
struct Report {
    durable_through_sequence: u64,
    scanned_records: u64,
    reached_durable_tail: bool,
    candidate_slots: usize,
    #[serde(flatten)]
    aggregates: ReportAggregates,
    classes: BTreeMap<&'static str, usize>,
    samples: BTreeMap<&'static str, Vec<SlotReport>>,
}

/// Orthogonal counters intentionally kept separate from the mutually exclusive headline class.
///
/// A slot can, for example, have no completion shred and also be below one or more FEC recovery
/// thresholds. Keeping both facts here avoids hiding repair demand behind `completion_unknown`.
#[derive(Debug, Default, Serialize)]
struct ReportAggregates {
    completion_unknown_slots: usize,
    fec_under_threshold_slots: usize,
    total_threshold_deficit: usize,
    conflict_slots: usize,
    conflicting_fec_geometry_slots: usize,
    conflicting_fec_identity_slots: usize,
    conflicting_adjacent_fec_chain_slots: usize,
    conflicting_slot_completion_slots: usize,
}

impl ReportAggregates {
    fn record(&mut self, report: &SlotReport) {
        if report.declared_last_data_index.is_none() {
            self.completion_unknown_slots = self.completion_unknown_slots.saturating_add(1);
        }
        if report.total_threshold_deficit > 0 {
            self.fec_under_threshold_slots = self.fec_under_threshold_slots.saturating_add(1);
        }
        self.total_threshold_deficit = self
            .total_threshold_deficit
            .saturating_add(report.total_threshold_deficit);
        if report.class == "conflict" {
            self.conflict_slots = self.conflict_slots.saturating_add(1);
        }
        if report.conflicting_fec_geometry {
            self.conflicting_fec_geometry_slots =
                self.conflicting_fec_geometry_slots.saturating_add(1);
        }
        if report.conflicting_fec_identity {
            self.conflicting_fec_identity_slots =
                self.conflicting_fec_identity_slots.saturating_add(1);
        }
        if !report.conflicting_adjacent_fec_chains.is_empty() {
            self.conflicting_adjacent_fec_chain_slots =
                self.conflicting_adjacent_fec_chain_slots.saturating_add(1);
        }
        if !report.conflicting_last_in_slot_indices.is_empty()
            || !report.data_indices_beyond_declared_completion.is_empty()
        {
            self.conflicting_slot_completion_slots =
                self.conflicting_slot_completion_slots.saturating_add(1);
        }
    }
}

#[derive(Debug, Serialize)]
struct SlotReport {
    slot: u64,
    class: &'static str,
    observed_data_shreds: usize,
    observed_coding_shreds: usize,
    first_data_index: Option<u32>,
    last_observed_data_index: Option<u32>,
    declared_last_data_index: Option<u32>,
    conflicting_last_in_slot_indices: Vec<u32>,
    conflicting_fec_geometry: bool,
    conflicting_fec_identity: bool,
    conflicting_adjacent_fec_chains: Vec<[u32; 2]>,
    data_indices_beyond_declared_completion: Vec<u32>,
    missing_data_indices: Vec<u32>,
    missing_indices_without_observed_fec_geometry: Vec<u32>,
    fec_sets: usize,
    under_threshold_fec_sets: usize,
    total_threshold_deficit: usize,
    suggested_data_shred_repairs: Vec<u32>,
    fec_deficits: Vec<FecDeficit>,
}

#[derive(Debug, Serialize)]
struct FecDeficit {
    fec_set_index: u32,
    expected_data_range: Option<[u32; 2]>,
    observed_data: usize,
    observed_coding: usize,
    required_shards: Option<usize>,
    threshold_deficit: Option<usize>,
    missing_data_indices: Vec<u32>,
    missing_coding_positions: Vec<u16>,
    suggested_data_shred_repairs: Vec<u32>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.max_candidate_slots > 0,
        "max-candidate-slots must be non-zero"
    );
    ensure!(
        args.samples_per_class > 0,
        "samples-per-class must be non-zero"
    );
    let identity = SpoolJournalIdentity {
        cluster_id: args.cluster_id.clone(),
        origin_node_id: args.origin_node_id.clone(),
        source_id: args.source_id.clone(),
        journal_id: args.journal_id,
    };
    let stream = ReplicationStreamId {
        cluster_id: args.cluster_id,
        origin_node_id: args.origin_node_id,
        source_id: args.source_id,
        journal_id: args.journal_id,
    };
    let durable_through_sequence = match (
        args.receiver_progress_wal.as_deref(),
        args.durable_through_sequence,
    ) {
        (Some(progress_wal), None) => {
            read_receiver_durable_progress(progress_wal, &stream)?
                .context("receiver progress WAL has no durable records")?
                .through_sequence
        }
        (None, Some(sequence)) => sequence,
        (None, None) => bail!(
            "provide --receiver-progress-wal for a live journal or --durable-through-sequence for an offline snapshot"
        ),
        (Some(_), Some(_)) => unreachable!("clap enforces conflicting cursor options"),
    };
    let mut candidates = BTreeMap::<u64, SlotCoverage>::new();
    let mut scanned_records = 0u64;
    let snapshot = read_spool_committed_snapshot_after(
        args.spool_root,
        identity,
        args.max_record_bytes,
        None,
        durable_through_sequence,
        args.max_records,
        |record| {
            scanned_records = scanned_records.saturating_add(1);
            ensure!(
                record.metadata.payload_format_version == ZSTD_SOLANA_SHRED_V1,
                "gap trial accepts only canonical compressed raw shreds"
            );
            let LogicalKey::Shred {
                slot,
                kind,
                shred_index,
                fec_set_index: metadata_fec_set_index,
            } = record.metadata.logical_key
            else {
                bail!("non-shred record found in raw shred spool");
            };
            if args.min_slot.is_some_and(|minimum| slot < minimum) {
                return Ok(());
            }
            if !retain_latest_slot(&mut candidates, slot, args.max_candidate_slots) {
                return Ok(());
            }
            let datagram = zstd::bulk::decompress(&record.payload, args.max_record_bytes as usize)
                .context("decompress stored raw shred")?;
            let shred = Shred::new_from_serialized_shred(datagram)
                .map_err(|error| anyhow::anyhow!("parse stored raw shred: {error:?}"))?;
            // Agave accepts repair responses with a trailing nonce. All header/FEC reads below use
            // its normalized canonical payload so the nonce never changes wire geometry.
            let payload = shred.payload();
            let fec_set_index = u32_at(payload, 79, "read FEC-set index")?;
            let decoded_kind = if shred.is_code() {
                ShredKind::Coding
            } else {
                ShredKind::Data
            };
            ensure!(
                slot == shred.slot()
                    && kind == decoded_kind
                    && shred_index == shred.index()
                    && metadata_fec_set_index == Some(fec_set_index),
                "stored shred metadata differs from its decoded canonical payload"
            );
            let slot_coverage = candidates.entry(slot).or_default();
            let identity = fec_identity(&shred, payload)?;
            let observation = if shred.is_code() {
                let geometry = FecGeometry {
                    num_data: usize::from(u16_at(payload, 83, "read FEC data count")?),
                    num_coding: usize::from(u16_at(payload, 85, "read FEC coding count")?),
                };
                FecObservation::Coding {
                    position: u16_at(payload, 87, "read coding position")?,
                    geometry,
                }
            } else {
                FecObservation::Data {
                    index: shred.index(),
                    last_in_slot: shred.last_in_slot(),
                }
            };
            record_observation(slot_coverage, fec_set_index, identity, observation);
            Ok(())
        },
    )?;

    let mut classes = BTreeMap::new();
    let mut aggregates = ReportAggregates::default();
    let mut samples = BTreeMap::<&'static str, Vec<SlotReport>>::new();
    for (slot, coverage) in candidates {
        let report = analyze_slot(slot, coverage)?;
        aggregates.record(&report);
        *classes.entry(report.class).or_insert(0usize) += 1;
        let class_samples = samples.entry(report.class).or_default();
        if class_samples.len() < args.samples_per_class {
            class_samples.push(report);
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&Report {
            durable_through_sequence,
            scanned_records,
            reached_durable_tail: snapshot.reached_durable_tail,
            candidate_slots: classes.values().sum(),
            aggregates,
            classes,
            samples,
        })?
    );
    Ok(())
}

fn analyze_slot(slot: u64, coverage: SlotCoverage) -> Result<SlotReport> {
    let first_data_index = coverage.data_indices.first().copied();
    let last_observed_data_index = coverage.data_indices.last().copied();
    let declared_last_data_index = (coverage.last_in_slot_indices.len() == 1)
        .then(|| coverage.last_in_slot_indices.first().copied())
        .flatten();
    let conflicting_last_in_slot_indices: Vec<u32> = (coverage.last_in_slot_indices.len() > 1)
        .then(|| coverage.last_in_slot_indices.iter().copied().collect())
        .unwrap_or_default();
    let conflicting_fec_geometry = coverage.fec_sets.values().any(|fec| fec.geometry_conflict);
    let conflicting_fec_identity = coverage.fec_sets.values().any(|fec| fec.identity_conflict);
    let conflicting_adjacent_fec_chains = find_conflicting_adjacent_fec_chains(&coverage.fec_sets)?;
    let data_indices_beyond_declared_completion = declared_last_data_index
        .map(|last| {
            coverage
                .data_indices
                .iter()
                .copied()
                .filter(|index| *index > last)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let missing_data_indices = declared_last_data_index
        .map(|last| {
            (0..=last)
                .filter(|index| !coverage.data_indices.contains(index))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut fec_deficits = Vec::new();
    let mut covered_missing = BTreeSet::new();
    let mut suggested_repairs = BTreeSet::new();
    let mut under_threshold_fec_sets = 0usize;
    let mut total_threshold_deficit = 0usize;
    for (fec_set_index, fec) in &coverage.fec_sets {
        let (expected_data_range, missing_data, missing_coding, required, deficit, suggested) =
            if let Some(geometry) = fec.geometry {
                let end = fec_set_index
                    .checked_add(u32::try_from(geometry.num_data)?.saturating_sub(1))
                    .context("FEC data range overflow")?;
                let missing_data = (*fec_set_index..=end)
                    .filter(|index| !fec.data_indices.contains(index))
                    .collect::<Vec<_>>();
                covered_missing.extend(missing_data.iter().copied());
                let missing_coding = (0..u16::try_from(geometry.num_coding)?)
                    .filter(|position| !fec.coding_positions.contains(position))
                    .collect::<Vec<_>>();
                let present = fec
                    .data_indices
                    .len()
                    .saturating_add(fec.coding_positions.len());
                let deficit = geometry.num_data.saturating_sub(present);
                let suggested = missing_data
                    .iter()
                    .copied()
                    .take(deficit)
                    .collect::<Vec<_>>();
                if deficit > 0 {
                    under_threshold_fec_sets = under_threshold_fec_sets.saturating_add(1);
                    total_threshold_deficit = total_threshold_deficit.saturating_add(deficit);
                    suggested_repairs.extend(suggested.iter().copied());
                }
                (
                    Some([*fec_set_index, end]),
                    missing_data,
                    missing_coding,
                    Some(geometry.num_data),
                    Some(deficit),
                    suggested,
                )
            } else {
                (None, Vec::new(), Vec::new(), None, None, Vec::new())
            };
        if deficit != Some(0) || !missing_data.is_empty() {
            fec_deficits.push(FecDeficit {
                fec_set_index: *fec_set_index,
                expected_data_range,
                observed_data: fec.data_indices.len(),
                observed_coding: fec.coding_positions.len(),
                required_shards: required,
                threshold_deficit: deficit,
                missing_data_indices: missing_data,
                missing_coding_positions: missing_coding,
                suggested_data_shred_repairs: suggested,
            });
        }
    }
    let missing_without_geometry = missing_data_indices
        .iter()
        .filter(|index| !covered_missing.contains(index))
        .copied()
        .collect::<Vec<_>>();
    let observed_coding_shreds = coverage
        .fec_sets
        .values()
        .map(|fec| fec.coding_positions.len())
        .sum();
    let class = if conflicting_fec_geometry
        || conflicting_fec_identity
        || !conflicting_adjacent_fec_chains.is_empty()
        || !conflicting_last_in_slot_indices.is_empty()
        || !data_indices_beyond_declared_completion.is_empty()
    {
        "conflict"
    } else if declared_last_data_index.is_none() {
        "completion_unknown"
    } else if !missing_without_geometry.is_empty() {
        "missing_unmapped_data"
    } else if total_threshold_deficit > 0 {
        "fec_under_threshold"
    } else if missing_data_indices.is_empty() {
        "complete_data"
    } else {
        "recoverable_from_local_fec"
    };
    Ok(SlotReport {
        slot,
        class,
        observed_data_shreds: coverage.data_indices.len(),
        observed_coding_shreds,
        first_data_index,
        last_observed_data_index,
        declared_last_data_index,
        conflicting_last_in_slot_indices,
        conflicting_fec_geometry,
        conflicting_fec_identity,
        conflicting_adjacent_fec_chains,
        data_indices_beyond_declared_completion,
        missing_data_indices,
        missing_indices_without_observed_fec_geometry: missing_without_geometry,
        fec_sets: coverage.fec_sets.len(),
        under_threshold_fec_sets,
        total_threshold_deficit,
        suggested_data_shred_repairs: suggested_repairs.into_iter().collect(),
        fec_deficits,
    })
}

/// Return adjacent FEC-set pairs whose chain commitment crosses fork identities.
///
/// Current Agave geometry fixes each FEC set at `DATA_SHREDS_PER_FEC_BLOCK` data shreds, so an
/// observed data-only set still anchors the next set. We compare only exactly adjacent observed
/// indices: if an entire intermediate FEC set is absent, the index gap is evidence that the chain
/// cannot be checked, not evidence of a conflict.
fn find_conflicting_adjacent_fec_chains(
    fec_sets: &BTreeMap<u32, FecCoverage>,
) -> Result<Vec<[u32; 2]>> {
    let data_width =
        u32::try_from(DATA_SHREDS_PER_FEC_BLOCK).context("fixed FEC data width exceeds u32")?;
    let mut conflicts = Vec::new();
    let mut previous: Option<(u32, FecIdentity)> = None;
    for (&fec_set_index, fec) in fec_sets {
        let Some(identity) = fec.identity else {
            // All normal observations install an identity before entering the set. Be defensive
            // for diagnostic/test inputs: an identity-less set cannot anchor either neighbor.
            previous = None;
            continue;
        };
        if let Some((previous_index, previous_identity)) = previous {
            if previous_index.checked_add(data_width) == Some(fec_set_index)
                && identity.chained_merkle_root != previous_identity.merkle_root
            {
                conflicts.push([previous_index, fec_set_index]);
            }
        }
        previous = Some((fec_set_index, identity));
    }
    Ok(conflicts)
}

fn record_observation(
    slot: &mut SlotCoverage,
    fec_set_index: u32,
    identity: FecIdentity,
    observation: FecObservation,
) {
    let fec = slot.fec_sets.entry(fec_set_index).or_default();
    if let Some(existing) = fec.identity {
        if existing != identity {
            // Never combine data or coding shards from two fork identities. Retain the first
            // internally consistent lineage only so the diagnostic can still describe it, but
            // force the entire slot into the conservative conflict class.
            fec.identity_conflict = true;
            return;
        }
    } else {
        fec.identity = Some(identity);
    }

    match observation {
        FecObservation::Data {
            index,
            last_in_slot,
        } => {
            slot.data_indices.insert(index);
            fec.data_indices.insert(index);
            if last_in_slot {
                slot.last_in_slot_indices.insert(index);
            }
        }
        FecObservation::Coding { position, geometry } => {
            if let Some(existing) = fec.geometry {
                fec.geometry_conflict |= existing != geometry;
            } else {
                fec.geometry = Some(geometry);
            }
            fec.coding_positions.insert(position);
        }
    }
}

fn retain_latest_slot(slots: &mut BTreeMap<u64, SlotCoverage>, slot: u64, maximum: usize) -> bool {
    if slots.contains_key(&slot) {
        return true;
    }
    if slots.len() < maximum {
        slots.insert(slot, SlotCoverage::default());
        return true;
    }
    let oldest = *slots.first_key_value().expect("full map is non-empty").0;
    if slot <= oldest {
        return false;
    }
    slots.remove(&oldest);
    slots.insert(slot, SlotCoverage::default());
    true
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

fn fec_identity(shred: &Shred, payload: &[u8]) -> Result<FecIdentity> {
    let variant = *payload.get(64).context("shred has no variant")?;
    ensure!(
        matches!(variant & 0xf0, 0x60 | 0x70 | 0x90 | 0xb0),
        "gap trial accepts only Merkle shreds"
    );
    let mut leader_signature = [0u8; 64];
    leader_signature.copy_from_slice(shred.signature().as_ref());
    Ok(FecIdentity {
        version: shred.version(),
        leader_signature,
        merkle_root: shred
            .merkle_root()
            .map_err(|error| anyhow::anyhow!("derive shred Merkle root: {error:?}"))?
            .to_bytes(),
        chained_merkle_root: shred
            .chained_merkle_root()
            .map_err(|error| anyhow::anyhow!("derive chained shred Merkle root: {error:?}"))?
            .to_bytes(),
        proof_size: variant & 0x0f,
        resigned: matches!(variant & 0xf0, 0x70 | 0xb0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(root: u8) -> FecIdentity {
        identity_with_chain(root, root.wrapping_sub(1))
    }

    fn identity_with_chain(root: u8, chained_root: u8) -> FecIdentity {
        FecIdentity {
            version: 1,
            leader_signature: [root; 64],
            merkle_root: [root; 32],
            chained_merkle_root: [chained_root; 32],
            proof_size: 5,
            resigned: false,
        }
    }

    fn coverage(data: &[u32], last: Option<u32>, geometry: Option<FecGeometry>) -> SlotCoverage {
        let data_indices = data.iter().copied().collect::<BTreeSet<_>>();
        let mut fec = FecCoverage {
            data_indices: data_indices.clone(),
            geometry,
            ..FecCoverage::default()
        };
        // Repeated coding observations must count only once.
        fec.coding_positions.extend([0, 0]);
        SlotCoverage {
            data_indices,
            last_in_slot_indices: last.into_iter().collect(),
            fec_sets: [(0, fec)].into_iter().collect(),
        }
    }

    #[test]
    fn complete_data_is_not_mislabeled_by_presence_of_fec() {
        let report = analyze_slot(
            7,
            coverage(
                &[0, 1, 2],
                Some(2),
                Some(FecGeometry {
                    num_data: 3,
                    num_coding: 3,
                }),
            ),
        )
        .unwrap();
        assert_eq!(report.class, "complete_data");
        assert_eq!(report.observed_coding_shreds, 1);
        assert!(report.missing_data_indices.is_empty());
    }

    #[test]
    fn local_fec_above_threshold_needs_no_repair_request() {
        let report = analyze_slot(
            8,
            coverage(
                &[0, 2],
                Some(2),
                Some(FecGeometry {
                    num_data: 3,
                    num_coding: 3,
                }),
            ),
        )
        .unwrap();
        assert_eq!(report.class, "recoverable_from_local_fec");
        assert_eq!(report.missing_data_indices, [1]);
        assert_eq!(report.total_threshold_deficit, 0);
        assert!(report.suggested_data_shred_repairs.is_empty());
    }

    #[test]
    fn missing_index_zero_is_recoverable_when_local_fec_covers_it() {
        let report = analyze_slot(
            81,
            coverage(
                &[1, 2],
                Some(2),
                Some(FecGeometry {
                    num_data: 3,
                    num_coding: 3,
                }),
            ),
        )
        .unwrap();
        assert_eq!(report.class, "recoverable_from_local_fec");
        assert_eq!(report.missing_data_indices, [0]);
        assert!(
            report
                .missing_indices_without_observed_fec_geometry
                .is_empty()
        );
    }

    #[test]
    fn under_threshold_fec_reports_minimum_data_repair() {
        let mut value = coverage(
            &[0, 2],
            Some(2),
            Some(FecGeometry {
                num_data: 3,
                num_coding: 3,
            }),
        );
        value.fec_sets.get_mut(&0).unwrap().coding_positions.clear();
        let report = analyze_slot(9, value).unwrap();
        assert_eq!(report.class, "fec_under_threshold");
        assert_eq!(report.total_threshold_deficit, 1);
        assert_eq!(report.suggested_data_shred_repairs, [1]);
    }

    #[test]
    fn missing_index_without_geometry_is_not_claimed_recoverable() {
        let report = analyze_slot(10, coverage(&[0, 2], Some(2), None)).unwrap();
        assert_eq!(report.class, "missing_unmapped_data");
        assert_eq!(report.missing_indices_without_observed_fec_geometry, [1]);
    }

    #[test]
    fn per_slot_conflict_is_reported_without_aborting_other_slots() {
        let mut value = coverage(
            &[0, 1, 2],
            Some(2),
            Some(FecGeometry {
                num_data: 3,
                num_coding: 3,
            }),
        );
        value.last_in_slot_indices.insert(1);
        value.fec_sets.get_mut(&0).unwrap().geometry_conflict = true;
        let report = analyze_slot(11, value).unwrap();
        assert_eq!(report.class, "conflict");
        assert_eq!(report.conflicting_last_in_slot_indices, [1, 2]);
        assert!(report.conflicting_fec_geometry);
    }

    #[test]
    fn different_fec_identity_is_flagged_without_merging_its_shred() {
        let mut value = SlotCoverage::default();
        record_observation(
            &mut value,
            0,
            identity(1),
            FecObservation::Data {
                index: 0,
                last_in_slot: false,
            },
        );
        record_observation(
            &mut value,
            0,
            identity(2),
            FecObservation::Data {
                index: 1,
                last_in_slot: true,
            },
        );

        assert_eq!(value.data_indices, [0].into_iter().collect());
        assert!(value.last_in_slot_indices.is_empty());
        assert_eq!(value.fec_sets[&0].data_indices, [0].into_iter().collect());
        let report = analyze_slot(12, value).unwrap();
        assert_eq!(report.class, "conflict");
        assert!(report.conflicting_fec_identity);
    }

    #[test]
    fn data_after_unique_completion_is_a_conflict() {
        let report = analyze_slot(
            13,
            coverage(
                &[0, 1, 2, 3],
                Some(2),
                Some(FecGeometry {
                    num_data: 4,
                    num_coding: 4,
                }),
            ),
        )
        .unwrap();

        assert_eq!(report.class, "conflict");
        assert_eq!(report.declared_last_data_index, Some(2));
        assert_eq!(report.data_indices_beyond_declared_completion, [3]);
    }

    #[test]
    fn adjacent_data_only_fec_sets_validate_with_fixed_agave_geometry() {
        let mut value = SlotCoverage::default();
        value.fec_sets.insert(
            0,
            FecCoverage {
                identity: Some(identity_with_chain(7, 1)),
                ..FecCoverage::default()
            },
        );
        value.fec_sets.insert(
            DATA_SHREDS_PER_FEC_BLOCK as u32,
            FecCoverage {
                identity: Some(identity_with_chain(8, 7)),
                ..FecCoverage::default()
            },
        );

        assert!(
            find_conflicting_adjacent_fec_chains(&value.fec_sets)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn adjacent_cross_fork_fec_chain_is_a_slot_conflict() {
        let next = DATA_SHREDS_PER_FEC_BLOCK as u32;
        let mut value = SlotCoverage::default();
        value.fec_sets.insert(
            0,
            FecCoverage {
                identity: Some(identity_with_chain(7, 1)),
                ..FecCoverage::default()
            },
        );
        value.fec_sets.insert(
            next,
            FecCoverage {
                identity: Some(identity_with_chain(8, 99)),
                ..FecCoverage::default()
            },
        );

        let report = analyze_slot(14, value).unwrap();
        assert_eq!(report.class, "conflict");
        assert_eq!(report.conflicting_adjacent_fec_chains, [[0, next]]);
    }

    #[test]
    fn absent_intermediate_fec_set_is_not_a_chain_conflict() {
        let after_gap = (DATA_SHREDS_PER_FEC_BLOCK * 2) as u32;
        let sets = BTreeMap::from([
            (
                0,
                FecCoverage {
                    identity: Some(identity_with_chain(7, 1)),
                    ..FecCoverage::default()
                },
            ),
            (
                after_gap,
                FecCoverage {
                    identity: Some(identity_with_chain(9, 99)),
                    ..FecCoverage::default()
                },
            ),
        ]);

        assert!(
            find_conflicting_adjacent_fec_chains(&sets)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn aggregate_keeps_unknown_completion_and_fec_deficit_orthogonal() {
        let report = analyze_slot(
            15,
            coverage(
                &[0],
                None,
                Some(FecGeometry {
                    num_data: 3,
                    num_coding: 3,
                }),
            ),
        )
        .unwrap();
        assert_eq!(report.class, "completion_unknown");
        assert_eq!(report.total_threshold_deficit, 1);

        let mut aggregates = ReportAggregates::default();
        aggregates.record(&report);
        assert_eq!(aggregates.completion_unknown_slots, 1);
        assert_eq!(aggregates.fec_under_threshold_slots, 1);
        assert_eq!(aggregates.total_threshold_deficit, 1);
    }
}
