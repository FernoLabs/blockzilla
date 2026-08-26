//! Read-only verification and merge support for accepted repair-shred WALs.
//!
//! The writer lives in the merged `hivezilla` shred-reader module.  This module deliberately duplicates the stable
//! wire decoder at the audit boundary: immutable prefix audits do not acquire the writer lock,
//! truncate a crash tail, or otherwise mutate live ingest state. Complete terminal-cursor discovery
//! intentionally holds the writer's base-file lock and refuses an active generation.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, ErrorKind, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::fd::AsRawFd;

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};
use solana_ledger_compat::{DATA_SHREDS_PER_FEC_BLOCK, MAX_DATA_SHREDS_PER_SLOT, Shred};
use solana_pubkey::Pubkey;

const LEGACY_FILE_HEADER: &[u8; 16] = b"SHRED-REPAIR\0\0\x02\0";
const SEGMENT_FILE_MAGIC: &[u8; 16] = b"SHRED-REPAIR\0\0\x03\0";
const V3_SEAL_MAGIC: &[u8; 16] = b"SHRED-V3-SEAL\0\x01\0";
const V3_SEAL_BYTES: usize = 60;
const V3_SEAL_CRC_OFFSET: usize = 56;
const V3_HEAD_MAGIC: &[u8; 16] = b"SHRED-V3-HEAD\0\x01\0";
const V3_HEAD_BYTES: usize = 76;
const V3_HEAD_CRC_OFFSET: usize = 72;
const SEGMENT_HEADER_BYTES: usize = 76;
const SEGMENT_HEADER_CRC_OFFSET: usize = 72;
const SEGMENT_CHAIN_DOMAIN: &[u8] = b"shred-repair-wal-segment-chain-v1";
const FRAME_PREFIX_BYTES: usize = 4;
const FRAME_CHECKSUM_BYTES: usize = 4;
const MIN_FRAME_BODY_BYTES: usize = 8;
const MAX_FRAME_BODY_BYTES: usize = 16 * 1024;
const SIGNATURE_BYTES: usize = 64;
const MERKLE_ROOT_BYTES: usize = 32;
const COMMON_FEC_SET_INDEX_OFFSET: usize = 79;
const COMMON_VARIANT_OFFSET: usize = 64;
const SEGMENT_ID_DIGITS: usize = 20;

pub const REPAIR_RECORD_MEMORY_OVERHEAD_BYTES: u64 = 512;

#[derive(Debug, Clone)]
pub struct RepairWalScanConfig {
    pub path: PathBuf,
    pub durable_through_sequence: u64,
    pub max_records: u64,
    pub max_payload_bytes: u64,
    pub max_segments: usize,
}

#[derive(Debug, Clone)]
pub struct RepairWalCompleteScanConfig {
    pub path: PathBuf,
    pub max_records: u64,
    pub max_payload_bytes: u64,
    pub max_segments: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepairWalScanReport {
    pub base_path: String,
    pub seal_path: Option<String>,
    pub seal_validated: bool,
    pub head_path: Option<String>,
    pub head_validated: bool,
    pub source_paths: Vec<String>,
    pub segments: usize,
    pub records: u64,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub durable_through_sequence: u64,
    pub reached_durable_tail: bool,
    pub frame_bytes: u64,
    pub payload_bytes: u64,
    pub coverage_records: u64,
    pub coverage_slots: usize,
    pub coverage_payload_bytes: u64,
    pub checksum_verified_records: u64,
    pub provenance_validated_records: u64,
    pub learned_chained_root_records: u64,
    pub successor_anchored_records: u64,
    pub prefix_sha256: String,
}

#[derive(Debug, Clone)]
pub struct ValidatedRepairRecord {
    pub sequence: u64,
    pub slot: u64,
    pub index: u32,
    pub fec_set_index: u32,
    pub version: u16,
    pub expected_slot_leader: [u8; 32],
    pub trust_anchor_fec_set_index: u32,
    pub learned_chained_merkle_root: bool,
    pub payload: Vec<u8>,
    fec_identity: FecIdentity,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RepairMergeStats {
    pub records_considered: usize,
    pub unique_shreds_added: usize,
    pub duplicate_raw_shreds: usize,
    pub duplicate_repair_shreds: usize,
    pub unique_payload_bytes_added: u64,
    pub first_repair_sequence: Option<u64>,
    pub last_repair_sequence: Option<u64>,
}

#[derive(Debug)]
pub struct RepairMergeOutput {
    pub datagrams: Vec<Vec<u8>>,
    pub stats: RepairMergeStats,
}

#[derive(Debug, Clone)]
pub struct RepairMergeFailure {
    pub category: &'static str,
    pub message: String,
    pub stats: RepairMergeStats,
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

#[derive(Debug)]
struct CandidateFec {
    identity: FecIdentity,
    has_raw: bool,
    earliest_repair_sequence: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum RepairRequest {
    Shred { slot: u64, index: u64 },
    HighestShred { slot: u64, index: u64 },
    Orphan { slot: u64 },
}

#[derive(Debug)]
struct SegmentSource {
    id: u64,
    path: PathBuf,
}

#[derive(Debug)]
struct Frame {
    prefix: [u8; FRAME_PREFIX_BYTES],
    body: Vec<u8>,
    checksum: [u8; FRAME_CHECKSUM_BYTES],
}

#[derive(Debug)]
struct SegmentHeader {
    bytes: Vec<u8>,
    initial_chain_digest: [u8; 32],
}

#[derive(Debug)]
struct ValidatedV3Seal {
    path: PathBuf,
    bytes: [u8; V3_SEAL_BYTES],
}

#[derive(Debug)]
struct ValidatedV3Head {
    path: PathBuf,
    bytes: [u8; V3_HEAD_BYTES],
}

impl Frame {
    fn byte_len(&self) -> usize {
        self.prefix.len() + self.body.len() + self.checksum.len()
    }

    fn update_fingerprint(&self, fingerprint: &mut Sha256) {
        fingerprint.update(self.prefix);
        fingerprint.update(&self.body);
        fingerprint.update(self.checksum);
    }
}

/// Scan and validate an inclusive immutable repair-WAL prefix without ever opening it for write.
///
/// Every frame through `durable_through_sequence` is checksum checked, fully decoded, reparsed as
/// a canonical data shred, rebound to its recorded request, and independently reverified against
/// the recorded slot-leader public key, signature, Merkle root and chained root.  The callback is
/// invoked only for records in the requested coverage range.
pub fn scan_repair_wal_prefix<F>(
    config: &RepairWalScanConfig,
    coverage_start: u64,
    coverage_end: u64,
    visit: F,
) -> Result<RepairWalScanReport>
where
    F: FnMut(ValidatedRepairRecord) -> Result<()>,
{
    scan_repair_wal(
        &config.path,
        config.max_records,
        config.max_payload_bytes,
        config.max_segments,
        Some(config.durable_through_sequence),
        coverage_start,
        coverage_end,
        visit,
    )
}

/// Fully validate an immutable repair WAL and return its exact inclusive terminal sequence.
///
/// This is the read-only discovery step used before an operator freezes that sequence for a
/// two-pass prefix audit. It never opens the WAL for write or repairs/truncates a crash tail.
pub fn scan_complete_repair_wal<F>(
    config: &RepairWalCompleteScanConfig,
    coverage_start: u64,
    coverage_end: u64,
    visit: F,
) -> Result<RepairWalScanReport>
where
    F: FnMut(ValidatedRepairRecord) -> Result<()>,
{
    let (base_path, _) = discover_segment_sources(&config.path, config.max_segments)?;
    let _base_lock = acquire_complete_scan_lock(&base_path)?;
    scan_repair_wal(
        &config.path,
        config.max_records,
        config.max_payload_bytes,
        config.max_segments,
        None,
        coverage_start,
        coverage_end,
        visit,
    )
}

#[cfg(unix)]
fn acquire_complete_scan_lock(base_path: &Path) -> Result<File> {
    let file = File::open(base_path).with_context(|| {
        format!(
            "open repair WAL base for immutable-snapshot lock {}",
            base_path.display()
        )
    })?;
    // SAFETY: `file` owns a valid descriptor and is retained for the complete scan lifetime.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(file)
    } else {
        Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "repair WAL base {} is active; freeze or copy it before discovering a terminal cursor",
                base_path.display()
            )
        })
    }
}

#[cfg(not(unix))]
fn acquire_complete_scan_lock(base_path: &Path) -> Result<File> {
    let file = File::open(base_path).with_context(|| {
        format!(
            "open repair WAL base for immutable-snapshot lock {}",
            base_path.display()
        )
    })?;
    file.try_lock().with_context(|| {
        format!(
            "repair WAL base {} is active; freeze or copy it before discovering a terminal cursor",
            base_path.display()
        )
    })?;
    Ok(file)
}

#[allow(clippy::too_many_arguments)]
fn scan_repair_wal<F>(
    path: &Path,
    max_records: u64,
    max_payload_bytes: u64,
    max_segments: usize,
    durable_through_sequence: Option<u64>,
    coverage_start: u64,
    coverage_end: u64,
    mut visit: F,
) -> Result<RepairWalScanReport>
where
    F: FnMut(ValidatedRepairRecord) -> Result<()>,
{
    ensure!(
        coverage_start <= coverage_end,
        "invalid repair coverage range"
    );
    ensure!(max_records > 0, "max repair records must be non-zero");
    ensure!(
        max_payload_bytes > 0,
        "max repair payload bytes must be non-zero"
    );
    ensure!(max_segments > 0, "max repair segments must be non-zero");
    if let Some(durable_through_sequence) = durable_through_sequence {
        let required_records = durable_through_sequence
            .checked_add(1)
            .context("repair durable sequence exhausted")?;
        ensure!(
            required_records <= max_records,
            "repair durable prefix requires {required_records} records, above the configured maximum {max_records}"
        );
    }

    let (base_path, sources) = discover_segment_sources(path, max_segments)?;
    let seal = validate_v3_seal_if_present(&base_path, max_records, max_payload_bytes)?;
    let head =
        validate_v3_head_exact_if_required(&base_path, &sources, seal.is_some(), max_records)?;
    let mut source_paths = Vec::new();
    let mut expected_sequence = 0u64;
    let mut records = 0u64;
    let mut frame_bytes = 0u64;
    let mut payload_bytes = 0u64;
    let mut coverage_records = 0u64;
    let mut coverage_payload_bytes = 0u64;
    let mut coverage_slots = BTreeSet::new();
    let mut learned_chained_root_records = 0u64;
    let mut successor_anchored_records = 0u64;
    let mut fingerprint = Sha256::new();
    if let Some(seal) = &seal {
        fingerprint.update(b"repair-wal-v3-seal");
        fingerprint.update(seal.bytes);
    }
    if let Some(head) = &head {
        fingerprint.update(b"repair-wal-v3-head");
        fingerprint.update(head.bytes);
    }
    let mut reached_durable_tail = false;
    let mut previous_segment_chain_digest = None;

    for source in sources {
        if reached_durable_tail {
            break;
        }
        reject_symlink(&source.path)?;
        let mut file = File::open(&source.path)
            .with_context(|| format!("open repair WAL segment {}", source.path.display()))?;
        let header = read_segment_header(
            &mut file,
            source.id,
            expected_sequence,
            previous_segment_chain_digest,
        )
        .with_context(|| format!("validate repair WAL segment {}", source.path.display()))?;
        fingerprint.update(source.id.to_le_bytes());
        fingerprint.update(&header.bytes);
        source_paths.push(source.path.display().to_string());
        let mut segment_chain_digest = header.initial_chain_digest;

        loop {
            let Some(frame) = read_frame(&mut file)? else {
                break;
            };
            let record = decode_and_validate_body(&frame.body).with_context(|| {
                format!(
                    "validate repair WAL record expected at global sequence {expected_sequence}"
                )
            })?;
            ensure!(
                record.sequence == expected_sequence,
                "repair WAL sequence discontinuity: expected {expected_sequence}, got {}",
                record.sequence
            );
            frame.update_fingerprint(&mut fingerprint);
            segment_chain_digest = advance_segment_chain(segment_chain_digest, &frame);
            records = records
                .checked_add(1)
                .context("repair record count overflow")?;
            frame_bytes = frame_bytes
                .checked_add(u64::try_from(frame.byte_len()).context("repair frame exceeds u64")?)
                .context("repair frame-byte count overflow")?;
            let record_payload_bytes =
                u64::try_from(record.payload.len()).context("repair payload exceeds u64")?;
            payload_bytes = payload_bytes
                .checked_add(record_payload_bytes)
                .context("repair payload-byte count overflow")?;
            ensure!(
                payload_bytes <= max_payload_bytes,
                "repair durable prefix payloads require {payload_bytes} bytes, above the configured maximum {max_payload_bytes}"
            );
            ensure!(
                records <= max_records,
                "repair durable prefix exceeds the configured record maximum {max_records}"
            );
            if record.learned_chained_merkle_root {
                learned_chained_root_records = learned_chained_root_records
                    .checked_add(1)
                    .context("learned repair chained-root count overflow")?;
            }
            if record.trust_anchor_fec_set_index > record.fec_set_index {
                successor_anchored_records = successor_anchored_records
                    .checked_add(1)
                    .context("successor-anchored repair count overflow")?;
            }

            if (coverage_start..=coverage_end).contains(&record.slot) {
                coverage_records = coverage_records
                    .checked_add(1)
                    .context("repair coverage record count overflow")?;
                coverage_payload_bytes = coverage_payload_bytes
                    .checked_add(record_payload_bytes)
                    .context("repair coverage payload-byte count overflow")?;
                coverage_slots.insert(record.slot);
                visit(record)?;
            }

            if durable_through_sequence == Some(expected_sequence) {
                reached_durable_tail = true;
                break;
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .context("repair sequence overflow")?;
        }
        previous_segment_chain_digest = Some(segment_chain_digest);
    }
    let validated_through_sequence = match durable_through_sequence {
        Some(asserted) => {
            ensure!(
                reached_durable_tail,
                "repair WAL ended before asserted durable-through sequence {asserted} (last validated {:?})",
                records.checked_sub(1)
            );
            asserted
        }
        None => {
            ensure!(records > 0, "repair WAL contains no records");
            reached_durable_tail = true;
            records - 1
        }
    };

    Ok(RepairWalScanReport {
        base_path: base_path.display().to_string(),
        seal_path: seal.as_ref().map(|seal| seal.path.display().to_string()),
        seal_validated: seal.is_some(),
        head_path: head.as_ref().map(|head| head.path.display().to_string()),
        head_validated: head.is_some(),
        segments: source_paths.len(),
        source_paths,
        records,
        first_sequence: Some(0),
        last_sequence: Some(validated_through_sequence),
        durable_through_sequence: validated_through_sequence,
        reached_durable_tail,
        frame_bytes,
        payload_bytes,
        coverage_records,
        coverage_slots: coverage_slots.len(),
        coverage_payload_bytes,
        checksum_verified_records: records,
        provenance_validated_records: records,
        learned_chained_root_records,
        successor_anchored_records,
        prefix_sha256: hex_bytes(&fingerprint.finalize()),
    })
}

pub fn ensure_same_repair_prefix(
    first: &RepairWalScanReport,
    second: &RepairWalScanReport,
) -> Result<()> {
    ensure!(
        first == second && first.reached_durable_tail && second.reached_durable_tail,
        "repair WAL prefix changed between audit passes"
    );
    Ok(())
}

/// Merge verified repair data shreds into one raw slot, rejecting every ambiguity instead of
/// choosing a fork. Exact repeats are counted and omitted from the reconstruction input.
pub fn merge_repair_records(
    slot: u64,
    mut raw_datagrams: Vec<Vec<u8>>,
    repairs: Vec<ValidatedRepairRecord>,
) -> std::result::Result<RepairMergeOutput, RepairMergeFailure> {
    let mut stats = RepairMergeStats {
        records_considered: repairs.len(),
        first_repair_sequence: repairs.first().map(|record| record.sequence),
        last_repair_sequence: repairs.last().map(|record| record.sequence),
        ..RepairMergeStats::default()
    };
    if repairs.is_empty() {
        return Ok(RepairMergeOutput {
            datagrams: raw_datagrams,
            stats,
        });
    }

    let expected_leader = repairs[0].expected_slot_leader;
    if repairs
        .iter()
        .any(|record| record.expected_slot_leader != expected_leader)
    {
        return Err(merge_failure(
            "repair_slot_leader_conflict",
            format!("slot {slot} repair records name more than one expected slot leader"),
            &stats,
        ));
    }
    let leader = Pubkey::new_from_array(expected_leader);

    let mut raw_versions = BTreeSet::new();
    let mut raw_data = BTreeMap::<u32, Vec<Vec<u8>>>::new();
    let mut fecs = BTreeMap::<u32, CandidateFec>::new();
    let mut ambiguous_raw_fecs = BTreeSet::new();
    for datagram in &raw_datagrams {
        let shred = match parse_canonical_shred(datagram) {
            Ok(shred) => shred,
            Err(error) => {
                return Err(merge_failure(
                    "repair_raw_shred_invalid",
                    format!(
                        "slot {slot} raw shred cannot be validated before repair merge: {error:#}"
                    ),
                    &stats,
                ));
            }
        };
        if shred.slot() != slot {
            return Err(merge_failure(
                "repair_slot_identity_conflict",
                format!("slot {slot} raw merge input contains slot {}", shred.slot()),
                &stats,
            ));
        }
        if !shred.verify(&leader) {
            return Err(merge_failure(
                "repair_slot_leader_conflict",
                format!(
                    "slot {slot} raw shred {} does not verify against the repair-provenance leader",
                    shred.index()
                ),
                &stats,
            ));
        }
        raw_versions.insert(shred.version());
        let identity = match fec_identity(&shred) {
            Ok(identity) => identity,
            Err(error) => {
                return Err(merge_failure(
                    "repair_raw_shred_invalid",
                    format!("slot {slot} raw FEC identity cannot be derived: {error:#}"),
                    &stats,
                ));
            }
        };
        let fec_set_index = shred.fec_set_index();
        match fecs.get_mut(&fec_set_index) {
            Some(existing) if existing.identity != identity => {
                ambiguous_raw_fecs.insert(fec_set_index);
            }
            Some(existing) => existing.has_raw = true,
            None => {
                fecs.insert(
                    fec_set_index,
                    CandidateFec {
                        identity,
                        has_raw: true,
                        earliest_repair_sequence: None,
                    },
                );
            }
        }
        if shred.is_data() {
            raw_data
                .entry(shred.index())
                .or_default()
                .push(shred.payload().to_vec());
        }
    }
    if raw_versions.len() != 1 {
        return Err(merge_failure(
            "repair_slot_version_conflict",
            format!("slot {slot} raw shreds do not have one exact shred version"),
            &stats,
        ));
    }
    let raw_version = *raw_versions
        .first()
        .expect("non-empty raw slot has one version");
    if repairs.iter().any(|record| record.version != raw_version) {
        return Err(merge_failure(
            "repair_slot_version_conflict",
            format!("slot {slot} repair and raw shred versions differ"),
            &stats,
        ));
    }

    let mut repair_data = BTreeMap::<u32, Vec<u8>>::new();
    let mut previous_sequence = None;
    for record in &repairs {
        if record.slot != slot {
            return Err(merge_failure(
                "repair_slot_identity_conflict",
                format!(
                    "slot {slot} repair merge input contains slot {}",
                    record.slot
                ),
                &stats,
            ));
        }
        if previous_sequence.is_some_and(|previous| record.sequence <= previous) {
            return Err(merge_failure(
                "repair_sequence_conflict",
                format!("slot {slot} repair records are not in strictly increasing sequence order"),
                &stats,
            ));
        }
        previous_sequence = Some(record.sequence);
        if ambiguous_raw_fecs.contains(&record.fec_set_index) {
            return Err(merge_failure(
                "repair_fec_identity_conflict",
                format!(
                    "slot {slot} repair FEC {} intersects an ambiguous raw FEC identity",
                    record.fec_set_index
                ),
                &stats,
            ));
        }
        match fecs.get_mut(&record.fec_set_index) {
            Some(existing) if existing.identity != record.fec_identity => {
                return Err(merge_failure(
                    "repair_fec_identity_conflict",
                    format!(
                        "slot {slot} repair FEC {} conflicts with the exact raw/repair identity",
                        record.fec_set_index
                    ),
                    &stats,
                ));
            }
            Some(existing) => {
                existing.earliest_repair_sequence = Some(
                    existing
                        .earliest_repair_sequence
                        .map_or(record.sequence, |sequence| sequence.min(record.sequence)),
                );
            }
            None => {
                fecs.insert(
                    record.fec_set_index,
                    CandidateFec {
                        identity: record.fec_identity,
                        has_raw: false,
                        earliest_repair_sequence: Some(record.sequence),
                    },
                );
            }
        }

        if let Some(raw) = raw_data.get(&record.index) {
            if raw.iter().any(|payload| payload == &record.payload) {
                stats.duplicate_raw_shreds = stats.duplicate_raw_shreds.saturating_add(1);
                continue;
            }
            return Err(merge_failure(
                "repair_raw_duplicate_conflict",
                format!(
                    "slot {slot} repair data index {} differs from the recorded raw data shred",
                    record.index
                ),
                &stats,
            ));
        }
        if let Some(previous) = repair_data.get(&record.index) {
            if previous == &record.payload {
                stats.duplicate_repair_shreds = stats.duplicate_repair_shreds.saturating_add(1);
                continue;
            }
            return Err(merge_failure(
                "repair_duplicate_conflict",
                format!(
                    "slot {slot} accepted repair records disagree at data index {}",
                    record.index
                ),
                &stats,
            ));
        }
        repair_data.insert(record.index, record.payload.clone());
        stats.unique_shreds_added = stats.unique_shreds_added.saturating_add(1);
        stats.unique_payload_bytes_added = stats
            .unique_payload_bytes_added
            .saturating_add(record.payload.len() as u64);
    }

    for record in &repairs {
        let mut fec_set_index = record.fec_set_index;
        while fec_set_index < record.trust_anchor_fec_set_index {
            let successor_index = match fec_set_index.checked_add(DATA_SHREDS_PER_FEC_BLOCK as u32)
            {
                Some(index) => index,
                None => {
                    return Err(merge_failure(
                        "repair_anchor_chain_incomplete",
                        format!("slot {slot} repair anchor path overflows its FEC index"),
                        &stats,
                    ));
                }
            };
            if ambiguous_raw_fecs.contains(&fec_set_index)
                || ambiguous_raw_fecs.contains(&successor_index)
            {
                return Err(merge_failure(
                    "repair_fec_identity_conflict",
                    format!(
                        "slot {slot} repair anchor path intersects ambiguous raw FEC identity at {fec_set_index} or {successor_index}"
                    ),
                    &stats,
                ));
            }
            let Some(current) = fecs.get(&fec_set_index) else {
                return Err(merge_failure(
                    "repair_anchor_chain_incomplete",
                    format!("slot {slot} repair anchor path is missing FEC {fec_set_index}"),
                    &stats,
                ));
            };
            let Some(successor) = fecs.get(&successor_index) else {
                return Err(merge_failure(
                    "repair_anchor_chain_incomplete",
                    format!(
                        "slot {slot} repair FEC {fec_set_index} claims anchor {} but successor FEC {successor_index} is absent",
                        record.trust_anchor_fec_set_index
                    ),
                    &stats,
                ));
            };
            if successor.identity.chained_merkle_root != current.identity.merkle_root {
                return Err(merge_failure(
                    "repair_anchor_chain_conflict",
                    format!(
                        "slot {slot} repair anchor chain conflicts between FEC {fec_set_index} and {successor_index}"
                    ),
                    &stats,
                ));
            }
            if !successor.has_raw
                && successor
                    .earliest_repair_sequence
                    .is_none_or(|sequence| sequence >= record.sequence)
            {
                return Err(merge_failure(
                    "repair_anchor_sequence_conflict",
                    format!(
                        "slot {slot} repair FEC {fec_set_index} precedes the repair evidence for successor FEC {successor_index}"
                    ),
                    &stats,
                ));
            }
            fec_set_index = successor_index;
        }
    }

    raw_datagrams.extend(repair_data.into_values());
    Ok(RepairMergeOutput {
        datagrams: raw_datagrams,
        stats,
    })
}

fn merge_failure(
    category: &'static str,
    message: String,
    stats: &RepairMergeStats,
) -> RepairMergeFailure {
    RepairMergeFailure {
        category,
        message,
        stats: stats.clone(),
    }
}

fn discover_segment_sources(
    path: &Path,
    max_segments: usize,
) -> Result<(PathBuf, Vec<SegmentSource>)> {
    let base_path = if path.is_dir() {
        let mut candidates = Vec::new();
        for entry in fs::read_dir(path)
            .with_context(|| format!("list repair WAL directory {}", path.display()))?
        {
            let entry = entry.context("read repair WAL directory entry")?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.ends_with(".repair.wal") && !name.contains(".segment-") {
                candidates.push(entry.path());
            }
        }
        ensure!(
            candidates.len() == 1,
            "repair WAL directory {} must contain exactly one unsegmented *.repair.wal base, found {}",
            path.display(),
            candidates.len()
        );
        candidates.pop().expect("exactly one base candidate")
    } else {
        path.to_path_buf()
    };
    reject_symlink(&base_path)?;
    ensure!(
        base_path.is_file(),
        "repair WAL base is not a file: {}",
        base_path.display()
    );
    let base_name = base_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("repair WAL base name is not UTF-8")?;
    let stem = base_name
        .strip_suffix(".repair.wal")
        .filter(|stem| !stem.is_empty() && !stem.contains(".segment-"))
        .context("repair WAL base path must end in unsegmented .repair.wal")?;
    let parent = repair_wal_parent(&base_path);
    let segment_prefix = format!("{stem}.segment-");
    let mut sources = vec![SegmentSource {
        id: 0,
        path: base_path.clone(),
    }];
    for entry in fs::read_dir(parent)
        .with_context(|| format!("list repair WAL segment directory {}", parent.display()))?
    {
        let entry = entry.context("read repair WAL segment directory entry")?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let id = match parse_segment_file_name(name, &segment_prefix) {
            Some(id) => id,
            None if name.starts_with(&segment_prefix) && name.ends_with(".repair.wal") => {
                bail!("malformed repair WAL segment file name {name}")
            }
            None => continue,
        };
        sources.push(SegmentSource {
            id,
            path: entry.path(),
        });
    }
    sources.sort_by_key(|source| source.id);
    ensure!(
        sources.len() <= max_segments,
        "repair WAL has {} segments, above the configured maximum {max_segments}",
        sources.len()
    );
    for (expected, source) in sources.iter().enumerate() {
        ensure!(
            source.id == expected as u64,
            "repair WAL segment-id discontinuity: expected {expected}, found {}",
            source.id
        );
    }
    Ok((base_path, sources))
}

fn repair_wal_parent(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

fn v3_seal_path(base_path: &Path) -> PathBuf {
    let mut path = base_path.as_os_str().to_os_string();
    path.push(".v3-seal");
    PathBuf::from(path)
}

fn v3_head_path(base_path: &Path) -> PathBuf {
    let mut path = base_path.as_os_str().to_os_string();
    path.push(".v3-head");
    PathBuf::from(path)
}

/// Validate the durable terminal checkpoint for every v3 generation.
///
/// A legacy v2 base has neither control file and remains readable. Once either v3 control exists,
/// read-only tooling requires the complete seal+head pair and an exact terminal match. Only the
/// exclusive writer is allowed to finish a head-before-seal transition or advance a proven
/// WAL-ahead crash tail.
fn validate_v3_head_exact_if_required(
    base_path: &Path,
    sources: &[SegmentSource],
    seal_present: bool,
    max_records: u64,
) -> Result<Option<ValidatedV3Head>> {
    let head_path = v3_head_path(base_path);
    let head_bytes: Option<[u8; V3_HEAD_BYTES]> = match fs::symlink_metadata(&head_path) {
        Ok(metadata) => {
            ensure!(
                !metadata.file_type().is_symlink(),
                "repair WAL v3 head must not be a symbolic link: {}",
                head_path.display()
            );
            ensure!(
                metadata.is_file(),
                "repair WAL v3 head is not a file: {}",
                head_path.display()
            );
            let bytes = fs::read(&head_path)
                .with_context(|| format!("read repair WAL v3 head {}", head_path.display()))?;
            ensure!(
                bytes.len() == V3_HEAD_BYTES,
                "repair WAL v3 head {} has {} bytes, expected {V3_HEAD_BYTES}",
                head_path.display(),
                bytes.len()
            );
            Some(bytes.try_into().expect("validated v3 head length is fixed"))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat repair WAL v3 head {}", head_path.display()));
        }
    };

    match (seal_present, head_bytes.as_ref(), sources.len()) {
        (false, None, 1) => return Ok(None),
        (false, None, _) => {
            bail!("segmented repair WAL is missing its v3 seal and durable head checkpoint")
        }
        (true, None, _) => {
            bail!("repair WAL v3 seal exists without its required durable head checkpoint")
        }
        (false, Some(_), _) => {
            bail!(
                "repair WAL has a transitional v3 head without a seal; only the writer may finish this transition"
            )
        }
        (true, Some(_), _) => {}
    }

    let head_bytes = head_bytes.expect("matched a present v3 head");
    ensure!(
        head_bytes[..V3_HEAD_MAGIC.len()] == *V3_HEAD_MAGIC,
        "invalid repair WAL v3 head magic"
    );
    let head_segment_id = u64::from_le_bytes(
        head_bytes[16..24]
            .try_into()
            .expect("v3 head segment-id slice has fixed length"),
    );
    let head_file_len = u64::from_le_bytes(
        head_bytes[24..32]
            .try_into()
            .expect("v3 head file-length slice has fixed length"),
    );
    let head_next_sequence = u64::from_le_bytes(
        head_bytes[32..40]
            .try_into()
            .expect("v3 head next-sequence slice has fixed length"),
    );
    let head_chain_digest: [u8; 32] = head_bytes[40..72]
        .try_into()
        .expect("v3 head digest slice has fixed length");
    let encoded_crc = u32::from_le_bytes(
        head_bytes[V3_HEAD_CRC_OFFSET..]
            .try_into()
            .expect("v3 head CRC slice has fixed length"),
    );
    let actual_crc = crc32(&head_bytes[..V3_HEAD_CRC_OFFSET]);
    ensure!(
        encoded_crc == actual_crc,
        "repair WAL v3 head checksum mismatch: expected {encoded_crc:#010x}, got {actual_crc:#010x}"
    );

    let mut expected_sequence = 0u64;
    let mut records = 0u64;
    let mut previous_segment_chain_digest = None;
    let mut terminal = None;
    for source in sources {
        reject_symlink(&source.path)?;
        let mut file = File::open(&source.path)
            .with_context(|| format!("open repair WAL segment {}", source.path.display()))?;
        let header = read_segment_header(
            &mut file,
            source.id,
            expected_sequence,
            previous_segment_chain_digest,
        )
        .with_context(|| {
            format!(
                "validate repair WAL segment {} for durable head",
                source.path.display()
            )
        })?;
        let mut segment_chain_digest = header.initial_chain_digest;
        let mut file_len =
            u64::try_from(header.bytes.len()).context("segment header exceeds u64")?;
        loop {
            let Some(frame) = read_frame(&mut file)? else {
                break;
            };
            let sequence = u64::from_le_bytes(
                frame.body[..MIN_FRAME_BODY_BYTES]
                    .try_into()
                    .expect("validated frame contains a sequence"),
            );
            ensure!(
                sequence == expected_sequence,
                "repair WAL sequence discontinuity while validating durable head: expected {expected_sequence}, got {sequence}"
            );
            expected_sequence = expected_sequence
                .checked_add(1)
                .context("repair sequence overflow while validating durable head")?;
            records = records
                .checked_add(1)
                .context("repair record count overflow while validating durable head")?;
            ensure!(
                records <= max_records,
                "repair WAL terminal validation exceeds the configured record maximum {max_records}"
            );
            file_len = file_len
                .checked_add(u64::try_from(frame.byte_len()).context("repair frame exceeds u64")?)
                .context("repair segment length overflow")?;
            segment_chain_digest = advance_segment_chain(segment_chain_digest, &frame);
        }
        ensure!(
            file_len == file.metadata()?.len(),
            "repair WAL segment {} changed length during durable-head validation",
            source.path.display()
        );
        previous_segment_chain_digest = Some(segment_chain_digest);
        terminal = Some((source.id, file_len, expected_sequence, segment_chain_digest));
    }
    let (terminal_segment_id, terminal_file_len, terminal_next_sequence, terminal_chain_digest) =
        terminal.context("repair WAL has no segments while validating durable head")?;
    ensure!(
        head_segment_id == terminal_segment_id
            && head_file_len == terminal_file_len
            && head_next_sequence == terminal_next_sequence
            && head_chain_digest == terminal_chain_digest,
        "repair WAL durable head does not match the exact terminal state (head segment {head_segment_id}, length {head_file_len}, next sequence {head_next_sequence}; WAL segment {terminal_segment_id}, length {terminal_file_len}, next sequence {terminal_next_sequence})"
    );

    Ok(Some(ValidatedV3Head {
        path: head_path,
        bytes: head_bytes,
    }))
}

fn validate_v3_seal_if_present(
    base_path: &Path,
    max_records: u64,
    max_payload_bytes: u64,
) -> Result<Option<ValidatedV3Seal>> {
    let seal_path = v3_seal_path(base_path);
    match fs::symlink_metadata(&seal_path) {
        Ok(metadata) => {
            ensure!(
                !metadata.file_type().is_symlink(),
                "repair WAL v3 seal must not be a symbolic link: {}",
                seal_path.display()
            );
            ensure!(
                metadata.is_file(),
                "repair WAL v3 seal is not a file: {}",
                seal_path.display()
            );
        }
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat repair WAL v3 seal {}", seal_path.display()));
        }
    }

    let seal_bytes = fs::read(&seal_path)
        .with_context(|| format!("read repair WAL v3 seal {}", seal_path.display()))?;
    ensure!(
        seal_bytes.len() == V3_SEAL_BYTES,
        "repair WAL v3 seal {} has {} bytes, expected {V3_SEAL_BYTES}",
        seal_path.display(),
        seal_bytes.len()
    );
    let seal_bytes: [u8; V3_SEAL_BYTES] = seal_bytes
        .try_into()
        .expect("validated v3 seal length is fixed");
    ensure!(
        seal_bytes[..V3_SEAL_MAGIC.len()] == *V3_SEAL_MAGIC,
        "invalid repair WAL v3 seal magic"
    );
    let sealed_next_sequence = u64::from_le_bytes(
        seal_bytes[16..24]
            .try_into()
            .expect("v3 seal next-sequence slice has fixed length"),
    );
    let sealed_base_chain: [u8; 32] = seal_bytes[24..56]
        .try_into()
        .expect("v3 seal digest slice has fixed length");
    let encoded_crc = u32::from_le_bytes(
        seal_bytes[V3_SEAL_CRC_OFFSET..]
            .try_into()
            .expect("v3 seal CRC slice has fixed length"),
    );
    let actual_crc = crc32(&seal_bytes[..V3_SEAL_CRC_OFFSET]);
    ensure!(
        encoded_crc == actual_crc,
        "repair WAL v3 seal checksum mismatch: expected {encoded_crc:#010x}, got {actual_crc:#010x}"
    );

    let mut base = File::open(base_path)
        .with_context(|| format!("open sealed repair WAL base {}", base_path.display()))?;
    let mut header = [0u8; LEGACY_FILE_HEADER.len()];
    base.read_exact(&mut header)
        .context("read sealed legacy repair WAL header")?;
    ensure!(
        header == *LEGACY_FILE_HEADER,
        "invalid sealed legacy repair WAL header"
    );
    let mut expected_sequence = 0u64;
    let mut records = 0u64;
    let mut payload_bytes = 0u64;
    let mut base_chain = initial_segment_chain(&header);
    loop {
        let Some(frame) = read_frame(&mut base)? else {
            break;
        };
        let record = decode_and_validate_body(&frame.body).with_context(|| {
            format!(
                "validate sealed repair WAL base record expected at global sequence {expected_sequence}"
            )
        })?;
        ensure!(
            record.sequence == expected_sequence,
            "sealed repair WAL base sequence discontinuity: expected {expected_sequence}, got {}",
            record.sequence
        );
        records = records
            .checked_add(1)
            .context("sealed repair WAL record count overflow")?;
        ensure!(
            records <= max_records,
            "sealed repair WAL base exceeds the configured record maximum {max_records}"
        );
        payload_bytes = payload_bytes
            .checked_add(
                u64::try_from(record.payload.len()).context("sealed repair payload exceeds u64")?,
            )
            .context("sealed repair payload-byte count overflow")?;
        ensure!(
            payload_bytes <= max_payload_bytes,
            "sealed repair WAL base payloads require {payload_bytes} bytes, above the configured maximum {max_payload_bytes}"
        );
        base_chain = advance_segment_chain(base_chain, &frame);
        expected_sequence = expected_sequence
            .checked_add(1)
            .context("sealed repair WAL base exhausted the sequence space")?;
    }
    ensure!(records > 0, "repair WAL v3 seal cannot seal an empty base");
    ensure!(
        sealed_next_sequence == expected_sequence,
        "repair WAL v3 seal next sequence {sealed_next_sequence} differs from immutable base next sequence {expected_sequence}"
    );
    ensure!(
        sealed_base_chain == base_chain,
        "repair WAL v3 seal base chain digest mismatch"
    );

    Ok(Some(ValidatedV3Seal {
        path: seal_path,
        bytes: seal_bytes,
    }))
}

fn parse_segment_file_name(name: &str, prefix: &str) -> Option<u64> {
    let digits = name.strip_prefix(prefix)?.strip_suffix(".repair.wal")?;
    if digits.len() != SEGMENT_ID_DIGITS || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let id = digits.parse::<u64>().ok()?;
    (id != 0).then_some(id)
}

fn reject_symlink(path: &Path) -> Result<()> {
    let metadata = path
        .symlink_metadata()
        .with_context(|| format!("stat repair WAL path {}", path.display()))?;
    ensure!(
        !metadata.file_type().is_symlink(),
        "repair WAL path must not be a symbolic link: {}",
        path.display()
    );
    Ok(())
}

fn read_segment_header(
    file: &mut File,
    segment_id: u64,
    expected_sequence: u64,
    previous_chain_digest: Option<[u8; 32]>,
) -> Result<SegmentHeader> {
    if segment_id == 0 {
        ensure!(
            expected_sequence == 0,
            "legacy repair WAL must begin at sequence zero"
        );
        ensure!(
            previous_chain_digest.is_none(),
            "legacy repair WAL unexpectedly has a predecessor chain"
        );
        let mut header = [0u8; LEGACY_FILE_HEADER.len()];
        file.read_exact(&mut header)
            .context("read legacy repair WAL header")?;
        ensure!(
            header == *LEGACY_FILE_HEADER,
            "invalid legacy repair WAL header"
        );
        return Ok(SegmentHeader {
            bytes: header.to_vec(),
            initial_chain_digest: initial_segment_chain(&header),
        });
    }

    let previous_chain_digest = previous_chain_digest
        .context("segmented repair WAL is missing its predecessor chain digest")?;
    let mut header = [0u8; SEGMENT_HEADER_BYTES];
    file.read_exact(&mut header)
        .context("read segmented repair WAL header")?;
    ensure!(
        header[..SEGMENT_FILE_MAGIC.len()] == *SEGMENT_FILE_MAGIC,
        "invalid segmented repair WAL magic"
    );
    let encoded_segment_id = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .expect("v3 segment-id header slice has fixed length"),
    );
    let first_sequence = u64::from_le_bytes(
        header[24..32]
            .try_into()
            .expect("v3 first-sequence header slice has fixed length"),
    );
    let previous_last_sequence = u64::from_le_bytes(
        header[32..40]
            .try_into()
            .expect("v3 previous-last header slice has fixed length"),
    );
    let encoded_previous_chain: [u8; 32] = header[40..72]
        .try_into()
        .expect("v3 previous-chain header slice has fixed length");
    let encoded_crc = u32::from_le_bytes(
        header[SEGMENT_HEADER_CRC_OFFSET..]
            .try_into()
            .expect("v3 header CRC slice has fixed length"),
    );
    let actual_crc = crc32(&header[..SEGMENT_HEADER_CRC_OFFSET]);
    ensure!(
        encoded_crc == actual_crc,
        "repair WAL segment-header checksum mismatch: expected {encoded_crc:#010x}, got {actual_crc:#010x}"
    );
    ensure!(
        encoded_segment_id == segment_id,
        "repair WAL segment header id {encoded_segment_id} differs from file id {segment_id}"
    );
    ensure!(
        first_sequence == expected_sequence,
        "repair WAL segment {segment_id} starts at {first_sequence}, expected {expected_sequence}"
    );
    let expected_previous_last = expected_sequence.checked_sub(1).unwrap_or(u64::MAX);
    ensure!(
        previous_last_sequence == expected_previous_last,
        "repair WAL segment {segment_id} names previous last sequence {previous_last_sequence}, expected {expected_previous_last}"
    );
    ensure!(
        encoded_previous_chain == previous_chain_digest,
        "repair WAL segment {segment_id} predecessor chain digest mismatch"
    );
    Ok(SegmentHeader {
        bytes: header.to_vec(),
        initial_chain_digest: initial_segment_chain(&header),
    })
}

fn initial_segment_chain(header: &[u8]) -> [u8; 32] {
    sha256_parts(&[SEGMENT_CHAIN_DOMAIN, header])
}

fn advance_segment_chain(previous: [u8; 32], frame: &Frame) -> [u8; 32] {
    sha256_parts(&[
        SEGMENT_CHAIN_DOMAIN,
        &previous,
        &frame.prefix,
        &frame.body,
        &frame.checksum,
    ])
}

fn sha256_parts(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn read_frame(file: &mut File) -> Result<Option<Frame>> {
    let mut prefix = [0u8; FRAME_PREFIX_BYTES];
    let mut first = [0u8; 1];
    match file.read(&mut first) {
        Ok(0) => return Ok(None),
        Ok(1) => prefix[0] = first[0],
        Ok(_) => unreachable!("one-byte read returned more than one byte"),
        Err(error) => return Err(error).context("read repair WAL frame prefix"),
    }
    file.read_exact(&mut prefix[1..]).map_err(|error| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("truncated repair WAL frame prefix: {error}"),
        )
    })?;
    let body_len = u32::from_le_bytes(prefix) as usize;
    ensure!(
        (MIN_FRAME_BODY_BYTES..=MAX_FRAME_BODY_BYTES).contains(&body_len),
        "invalid repair WAL frame body length {body_len}"
    );
    let mut body = vec![0u8; body_len];
    file.read_exact(&mut body).map_err(|error| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("truncated repair WAL frame body: {error}"),
        )
    })?;
    let mut checksum = [0u8; FRAME_CHECKSUM_BYTES];
    file.read_exact(&mut checksum).map_err(|error| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("truncated repair WAL checksum: {error}"),
        )
    })?;
    let expected = u32::from_le_bytes(checksum);
    let actual = crc32(&body);
    ensure!(
        actual == expected,
        "repair WAL checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
    );
    Ok(Some(Frame {
        prefix,
        body,
        checksum,
    }))
}

fn decode_and_validate_body(body: &[u8]) -> Result<ValidatedRepairRecord> {
    let mut decoder = Decoder::new(body);
    let sequence = decoder.u64()?;
    let _received_at_unix_ms = decoder.u64()?;
    let _nonce = decoder.u32()?;
    let request_kind = decoder.u8()?;
    let request_slot = decoder.u64()?;
    let request_index = decoder.u64()?;
    let request = decode_request(request_kind, request_slot, request_index)?;
    let peer_addr_len = decoder.u16()? as usize;
    let peer_addr = std::str::from_utf8(decoder.bytes(peer_addr_len)?)
        .context("repair WAL peer address is not UTF-8")?;
    let parsed_peer = peer_addr
        .parse::<SocketAddr>()
        .context("repair WAL peer address is not a socket address")?;
    ensure!(
        parsed_peer.to_string() == peer_addr,
        "repair WAL peer address is not canonical"
    );
    let _peer_pubkey = decoder.array::<32>()?;
    let shred_slot = decoder.u64()?;
    let shred_index = decoder.u32()?;
    let fec_set_index = decoder.u32()?;
    let shred_version = decoder.u16()?;
    let expected_slot_leader = decoder.array::<32>()?;
    let fec_merkle_root = decoder.array::<32>()?;
    let trust_anchor_fec_set_index = decoder.u32()?;
    let learned_chained_merkle_root = decoder.boolean("learned-chain")?;
    let chained_merkle_root = match decoder.u8()? {
        0 => None,
        1 => Some(decoder.array::<32>()?),
        value => bail!("invalid repair WAL chained-root flag {value}"),
    };
    let leader_signature = decoder.array::<64>()?;
    let payload_len = decoder.u32()? as usize;
    let payload = decoder.bytes(payload_len)?.to_vec();
    ensure!(decoder.is_empty(), "repair WAL frame has trailing bytes");

    let shred = parse_canonical_shred(&payload).context("repair WAL payload is not canonical")?;
    ensure!(shred.is_data(), "repair WAL payload is not a data shred");
    ensure!(
        shred.slot() == shred_slot
            && shred.index() == shred_index
            && shred.fec_set_index() == fec_set_index
            && shred.version() == shred_version,
        "repair WAL provenance identity differs from its shred payload"
    );
    ensure!(
        request_matches(request, &shred),
        "repair WAL request provenance does not authorize its shred response"
    );
    ensure!(
        valid_fixed_data_fec_layout(&shred),
        "repair WAL shred has invalid fixed-width FEC geometry"
    );
    let leader = Pubkey::new_from_array(expected_slot_leader);
    ensure!(
        shred.verify(&leader),
        "repair WAL shred does not verify against its recorded expected slot leader"
    );
    ensure!(
        shred.signature().as_ref() == leader_signature,
        "repair WAL leader signature provenance differs from its shred payload"
    );
    let actual_merkle_root = shred
        .merkle_root()
        .map_err(|error| anyhow::anyhow!("derive repair shred Merkle root: {error:?}"))?;
    ensure!(
        actual_merkle_root == fec_merkle_root,
        "repair WAL FEC Merkle root provenance differs from its shred payload"
    );
    let actual_chained_root = shred
        .chained_merkle_root()
        .map_err(|error| anyhow::anyhow!("derive repair shred chained Merkle root: {error:?}"))?;
    let chained_merkle_root = chained_merkle_root
        .context("accepted repair WAL provenance omitted its chained Merkle root")?;
    ensure!(
        actual_chained_root == chained_merkle_root,
        "repair WAL chained Merkle root provenance differs from its shred payload"
    );
    let fec_width = DATA_SHREDS_PER_FEC_BLOCK as u32;
    ensure!(
        trust_anchor_fec_set_index >= fec_set_index
            && (trust_anchor_fec_set_index - fec_set_index).is_multiple_of(fec_width),
        "repair WAL trust anchor is not an aligned successor of its FEC set"
    );
    if learned_chained_merkle_root {
        ensure!(
            fec_set_index.checked_add(fec_width) == Some(trust_anchor_fec_set_index),
            "a learned repair chained root must be anchored by the immediate successor FEC"
        );
    }
    let fec_identity = fec_identity(&shred)?;

    Ok(ValidatedRepairRecord {
        sequence,
        slot: shred_slot,
        index: shred_index,
        fec_set_index,
        version: shred_version,
        expected_slot_leader,
        trust_anchor_fec_set_index,
        learned_chained_merkle_root,
        payload,
        fec_identity,
    })
}

fn decode_request(kind: u8, slot: u64, index: u64) -> Result<RepairRequest> {
    match kind {
        0 => Ok(RepairRequest::Shred { slot, index }),
        1 => Ok(RepairRequest::HighestShred { slot, index }),
        2 if index == 0 => Ok(RepairRequest::Orphan { slot }),
        _ => bail!("invalid repair WAL request kind/index {kind}/{index}"),
    }
}

fn request_matches(request: RepairRequest, shred: &Shred) -> bool {
    match request {
        RepairRequest::Shred { slot, index } => {
            shred.slot() == slot && u64::from(shred.index()) == index
        }
        RepairRequest::HighestShred { slot, index } => {
            shred.slot() == slot && u64::from(shred.index()) >= index
        }
        RepairRequest::Orphan { slot } => shred.slot() <= slot,
    }
}

fn parse_canonical_shred(payload: &[u8]) -> Result<Shred> {
    let shred = Shred::new_from_serialized_shred(payload.to_vec())
        .map_err(|error| anyhow::anyhow!("parse Solana shred: {error:?}"))?;
    shred
        .sanitize()
        .map_err(|error| anyhow::anyhow!("sanitize Solana shred: {error:?}"))?;
    ensure!(
        shred.payload().as_ref() == payload,
        "serialized shred has a non-canonical suffix"
    );
    Ok(shred)
}

fn valid_fixed_data_fec_layout(shred: &Shred) -> bool {
    if !shred.is_data() {
        return false;
    }
    let fec_set_index = shred.fec_set_index();
    let fec_width = DATA_SHREDS_PER_FEC_BLOCK as u32;
    fec_set_index.is_multiple_of(fec_width)
        && fec_set_index
            .checked_add(fec_width)
            .is_some_and(|end| end <= MAX_DATA_SHREDS_PER_SLOT as u32)
        && shred.index() >= fec_set_index
        && shred
            .index()
            .checked_sub(fec_set_index)
            .is_some_and(|relative| relative < fec_width)
}

fn fec_identity(shred: &Shred) -> Result<FecIdentity> {
    let variant = *shred
        .payload()
        .get(COMMON_VARIANT_OFFSET)
        .context("shred payload omitted its variant")?;
    let high = variant & 0xf0;
    ensure!(
        matches!(high, 0x60 | 0x70 | 0x90 | 0xb0),
        "shred has an unsupported Merkle variant"
    );
    let encoded_fec = shred
        .payload()
        .get(COMMON_FEC_SET_INDEX_OFFSET..COMMON_FEC_SET_INDEX_OFFSET + 4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_le_bytes)
        .context("shred omitted its encoded FEC-set index")?;
    ensure!(
        encoded_fec == shred.fec_set_index(),
        "decoded and encoded FEC-set indices differ"
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

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let Some((head, tail)) = self.remaining.split_at_checked(len) else {
            bail!("truncated repair WAL record");
        };
        self.remaining = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self
            .bytes(N)?
            .try_into()
            .expect("decoder requested a fixed checked length"))
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn boolean(&mut self, label: &str) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => bail!("invalid repair WAL {label} flag {value}"),
        }
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320u32 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_keypair::{Keypair, Signer};
    use tempfile::tempdir;

    const VERSION: u16 = 50_093;

    #[test]
    fn bare_relative_repair_wal_uses_current_directory_as_parent() {
        assert_eq!(
            repair_wal_parent(Path::new("accepted.repair.wal")),
            Path::new(".")
        );
    }

    #[test]
    fn legacy_scan_revalidates_checksum_and_payload_bound_provenance() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("accepted.repair.wal");
        let leader = Keypair::new();
        let shred = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let body = encode_test_body(0, &shred, &leader, 0, false);
        let frame = encode_frame(&body);
        let mut bytes = LEGACY_FILE_HEADER.to_vec();
        bytes.extend_from_slice(&frame);
        fs::write(&path, bytes).unwrap();

        let mut records = Vec::new();
        let report = scan_repair_wal_prefix(&scan_config(path, 0), 500, 500, |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();

        assert_eq!(report.records, 1);
        assert_eq!(report.coverage_records, 1);
        assert_eq!(report.checksum_verified_records, 1);
        assert_eq!(report.provenance_validated_records, 1);
        assert_eq!(records[0].payload.as_slice(), shred.payload().as_ref());
    }

    #[test]
    fn checksum_corruption_fails_the_read_only_scan_closed() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("accepted.repair.wal");
        let leader = Keypair::new();
        let shred = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let body = encode_test_body(0, &shred, &leader, 0, false);
        let mut frame = encode_frame(&body);
        *frame.last_mut().unwrap() ^= 0x80;
        let mut bytes = LEGACY_FILE_HEADER.to_vec();
        bytes.extend_from_slice(&frame);
        fs::write(&path, bytes).unwrap();

        let error =
            scan_repair_wal_prefix(&scan_config(path, 0), 500, 500, |_| Ok(())).unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn checksum_valid_but_false_leader_provenance_fails_closed() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("accepted.repair.wal");
        let actual_leader = Keypair::new();
        let false_leader = Keypair::new();
        let shred = signed_test_data_shred(500, 0, &actual_leader, [0; 32]);
        let body = encode_test_body(0, &shred, &false_leader, 0, false);
        let frame = encode_frame(&body);
        let mut bytes = LEGACY_FILE_HEADER.to_vec();
        bytes.extend_from_slice(&frame);
        fs::write(&path, bytes).unwrap();

        let error =
            scan_repair_wal_prefix(&scan_config(path, 0), 500, 500, |_| Ok(())).unwrap_err();

        assert!(format!("{error:#}").contains("expected slot leader"));
    }

    #[test]
    fn segmented_scan_verifies_v3_header_and_predecessor_chain() {
        let directory = tempdir().unwrap();
        let base = directory.path().join("accepted.repair.wal");
        let segment = directory
            .path()
            .join("accepted.segment-00000000000000000001.repair.wal");
        let leader = Keypair::new();
        let first = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let second = signed_test_data_shred(501, 0, &leader, [0; 32]);
        let first_frame = encode_frame(&encode_test_body(0, &first, &leader, 0, false));
        let second_frame = encode_frame(&encode_test_body(1, &second, &leader, 0, false));
        let mut base_bytes = LEGACY_FILE_HEADER.to_vec();
        base_bytes.extend_from_slice(&first_frame);
        fs::write(&base, &base_bytes).unwrap();
        let mut previous_chain = initial_segment_chain(LEGACY_FILE_HEADER);
        previous_chain = sha256_parts(&[SEGMENT_CHAIN_DOMAIN, &previous_chain, &first_frame]);
        let header = encode_segment_header(1, 1, 0, previous_chain);
        let mut segment_bytes = header.to_vec();
        segment_bytes.extend_from_slice(&second_frame);
        let segment_chain = sha256_parts(&[
            SEGMENT_CHAIN_DOMAIN,
            &initial_segment_chain(&header),
            &second_frame,
        ]);
        fs::write(&segment, &segment_bytes).unwrap();
        fs::write(v3_seal_path(&base), encode_v3_seal(1, previous_chain)).unwrap();
        fs::write(
            v3_head_path(&base),
            encode_v3_head(1, segment_bytes.len() as u64, 2, segment_chain),
        )
        .unwrap();

        let report = scan_repair_wal_prefix(&scan_config(base, 1), 500, 501, |_| Ok(())).unwrap();

        assert_eq!(report.segments, 2);
        assert_eq!(report.records, 2);
        assert_eq!(report.coverage_slots, 2);
        assert!(report.seal_validated);
        assert!(report.head_validated);
    }

    #[test]
    fn sealed_base_is_fully_validated_before_a_durable_prefix_is_accepted() {
        let directory = tempdir().unwrap();
        let base = directory.path().join("accepted.repair.wal");
        let leader = Keypair::new();
        let first = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let second = signed_test_data_shred(501, 0, &leader, [0; 32]);
        let first_frame = encode_frame(&encode_test_body(0, &first, &leader, 0, false));
        let second_frame = encode_frame(&encode_test_body(1, &second, &leader, 0, false));
        let mut base_bytes = LEGACY_FILE_HEADER.to_vec();
        base_bytes.extend_from_slice(&first_frame);
        base_bytes.extend_from_slice(&second_frame);
        fs::write(&base, &base_bytes).unwrap();
        let mut base_chain = initial_segment_chain(LEGACY_FILE_HEADER);
        base_chain = sha256_parts(&[SEGMENT_CHAIN_DOMAIN, &base_chain, &first_frame]);
        base_chain = sha256_parts(&[SEGMENT_CHAIN_DOMAIN, &base_chain, &second_frame]);
        fs::write(v3_seal_path(&base), encode_v3_seal(2, base_chain)).unwrap();
        fs::write(
            v3_head_path(&base),
            encode_v3_head(0, base_bytes.len() as u64, 2, base_chain),
        )
        .unwrap();

        let report = scan_repair_wal_prefix(&scan_config(base, 0), 500, 500, |_| Ok(())).unwrap();

        assert!(report.seal_validated);
        assert!(report.seal_path.is_some());
        assert!(report.head_validated);
        assert!(report.head_path.is_some());
        assert_eq!(report.records, 1);
        assert_eq!(report.last_sequence, Some(0));
    }

    #[test]
    fn sealed_base_rejects_a_legacy_append_after_the_seal() {
        let directory = tempdir().unwrap();
        let base = directory.path().join("accepted.repair.wal");
        let leader = Keypair::new();
        let first = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let second = signed_test_data_shred(501, 0, &leader, [0; 32]);
        let first_frame = encode_frame(&encode_test_body(0, &first, &leader, 0, false));
        let second_frame = encode_frame(&encode_test_body(1, &second, &leader, 0, false));
        let mut base_chain = initial_segment_chain(LEGACY_FILE_HEADER);
        base_chain = sha256_parts(&[SEGMENT_CHAIN_DOMAIN, &base_chain, &first_frame]);
        let mut base_bytes = LEGACY_FILE_HEADER.to_vec();
        base_bytes.extend_from_slice(&first_frame);
        fs::write(&base, &base_bytes).unwrap();
        fs::write(v3_seal_path(&base), encode_v3_seal(1, base_chain)).unwrap();
        fs::write(
            v3_head_path(&base),
            encode_v3_head(0, base_bytes.len() as u64, 1, base_chain),
        )
        .unwrap();
        base_bytes.extend_from_slice(&second_frame);
        fs::write(&base, base_bytes).unwrap();

        let error =
            scan_repair_wal_prefix(&scan_config(base, 0), 500, 500, |_| Ok(())).unwrap_err();

        assert!(error.to_string().contains("next sequence"));
    }

    #[test]
    fn sealed_generation_without_a_durable_head_is_rejected() {
        let directory = tempdir().unwrap();
        let base = directory.path().join("accepted.repair.wal");
        let leader = Keypair::new();
        let shred = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let frame = encode_frame(&encode_test_body(0, &shred, &leader, 0, false));
        let mut base_bytes = LEGACY_FILE_HEADER.to_vec();
        base_bytes.extend_from_slice(&frame);
        fs::write(&base, base_bytes).unwrap();
        let base_chain = sha256_parts(&[
            SEGMENT_CHAIN_DOMAIN,
            &initial_segment_chain(LEGACY_FILE_HEADER),
            &frame,
        ]);
        fs::write(v3_seal_path(&base), encode_v3_seal(1, base_chain)).unwrap();

        let error =
            scan_repair_wal_prefix(&scan_config(base, 0), 500, 500, |_| Ok(())).unwrap_err();

        assert!(format!("{error:#}").contains("without its required durable head"));
    }

    #[test]
    fn deleting_the_highest_segment_is_detected_by_the_durable_head() {
        let directory = tempdir().unwrap();
        let base = directory.path().join("accepted.repair.wal");
        let segment = directory
            .path()
            .join("accepted.segment-00000000000000000001.repair.wal");
        let leader = Keypair::new();
        let first = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let second = signed_test_data_shred(501, 0, &leader, [0; 32]);
        let first_frame = encode_frame(&encode_test_body(0, &first, &leader, 0, false));
        let second_frame = encode_frame(&encode_test_body(1, &second, &leader, 0, false));
        let mut base_bytes = LEGACY_FILE_HEADER.to_vec();
        base_bytes.extend_from_slice(&first_frame);
        fs::write(&base, &base_bytes).unwrap();
        let base_chain = sha256_parts(&[
            SEGMENT_CHAIN_DOMAIN,
            &initial_segment_chain(LEGACY_FILE_HEADER),
            &first_frame,
        ]);
        let header = encode_segment_header(1, 1, 0, base_chain);
        let mut segment_bytes = header.to_vec();
        segment_bytes.extend_from_slice(&second_frame);
        let segment_chain = sha256_parts(&[
            SEGMENT_CHAIN_DOMAIN,
            &initial_segment_chain(&header),
            &second_frame,
        ]);
        fs::write(&segment, &segment_bytes).unwrap();
        fs::write(v3_seal_path(&base), encode_v3_seal(1, base_chain)).unwrap();
        fs::write(
            v3_head_path(&base),
            encode_v3_head(1, segment_bytes.len() as u64, 2, segment_chain),
        )
        .unwrap();
        fs::remove_file(segment).unwrap();

        let error =
            scan_repair_wal_prefix(&scan_config(base, 0), 500, 500, |_| Ok(())).unwrap_err();

        assert!(format!("{error:#}").contains("does not match the exact terminal state"));
    }

    #[test]
    fn read_only_audit_rejects_a_wal_ahead_of_its_durable_head() {
        let directory = tempdir().unwrap();
        let base = directory.path().join("accepted.repair.wal");
        let leader = Keypair::new();
        let first = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let second = signed_test_data_shred(501, 0, &leader, [0; 32]);
        let first_frame = encode_frame(&encode_test_body(0, &first, &leader, 0, false));
        let second_frame = encode_frame(&encode_test_body(1, &second, &leader, 0, false));
        let mut first_chain = initial_segment_chain(LEGACY_FILE_HEADER);
        first_chain = sha256_parts(&[SEGMENT_CHAIN_DOMAIN, &first_chain, &first_frame]);
        let first_boundary = (LEGACY_FILE_HEADER.len() + first_frame.len()) as u64;
        let terminal_chain = sha256_parts(&[SEGMENT_CHAIN_DOMAIN, &first_chain, &second_frame]);
        let mut base_bytes = LEGACY_FILE_HEADER.to_vec();
        base_bytes.extend_from_slice(&first_frame);
        base_bytes.extend_from_slice(&second_frame);
        fs::write(&base, base_bytes).unwrap();
        fs::write(v3_seal_path(&base), encode_v3_seal(2, terminal_chain)).unwrap();
        fs::write(
            v3_head_path(&base),
            encode_v3_head(0, first_boundary, 1, first_chain),
        )
        .unwrap();

        let error =
            scan_repair_wal_prefix(&scan_config(base, 0), 500, 500, |_| Ok(())).unwrap_err();

        assert!(format!("{error:#}").contains("does not match the exact terminal state"));
    }

    #[test]
    fn complete_scan_discovers_the_exact_terminal_sequence() {
        let directory = tempdir().unwrap();
        let base = directory.path().join("accepted.repair.wal");
        let leader = Keypair::new();
        let first = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let second = signed_test_data_shred(501, 0, &leader, [0; 32]);
        let mut base_bytes = LEGACY_FILE_HEADER.to_vec();
        base_bytes.extend_from_slice(&encode_frame(&encode_test_body(
            0, &first, &leader, 0, false,
        )));
        base_bytes.extend_from_slice(&encode_frame(&encode_test_body(
            1, &second, &leader, 0, false,
        )));
        fs::write(&base, base_bytes).unwrap();
        let config = RepairWalCompleteScanConfig {
            path: base,
            max_records: 100,
            max_payload_bytes: 1024 * 1024,
            max_segments: 10,
        };

        let report = scan_complete_repair_wal(&config, 500, 501, |_| Ok(())).unwrap();

        assert_eq!(report.records, 2);
        assert_eq!(report.last_sequence, Some(1));
        assert_eq!(report.durable_through_sequence, 1);
        assert!(report.reached_durable_tail);
        assert_eq!(report.coverage_slots, 2);
    }

    #[cfg(unix)]
    #[test]
    fn complete_scan_refuses_a_wal_owned_by_an_active_writer() {
        let directory = tempdir().unwrap();
        let base = directory.path().join("accepted.repair.wal");
        let leader = Keypair::new();
        let shred = signed_test_data_shred(500, 0, &leader, [0; 32]);
        let mut base_bytes = LEGACY_FILE_HEADER.to_vec();
        base_bytes.extend_from_slice(&encode_frame(&encode_test_body(
            0, &shred, &leader, 0, false,
        )));
        fs::write(&base, base_bytes).unwrap();
        let _writer_lock = acquire_complete_scan_lock(&base).unwrap();
        let config = RepairWalCompleteScanConfig {
            path: base,
            max_records: 100,
            max_payload_bytes: 1024 * 1024,
            max_segments: 10,
        };

        let error = scan_complete_repair_wal(&config, 500, 500, |_| Ok(())).unwrap_err();

        assert!(error.to_string().contains("active"));
    }

    #[test]
    fn merge_adds_only_unique_repairs_and_requires_the_exact_anchor_chain() {
        let leader = Keypair::new();
        let raw = signed_test_data_shred(700, 0, &leader, [0; 32]);
        let raw_root = raw.merkle_root().unwrap();
        let repaired = signed_test_data_shred(700, 32, &leader, raw_root);
        let record = validated_record(1, &repaired, &leader, 32, false);

        let merged = merge_repair_records(700, vec![raw.payload().to_vec()], vec![record]).unwrap();

        assert_eq!(merged.stats.unique_shreds_added, 1);
        assert_eq!(merged.stats.duplicate_raw_shreds, 0);
        assert_eq!(merged.datagrams.len(), 2);
    }

    #[test]
    fn merge_deduplicates_an_exact_raw_repeat() {
        let leader = Keypair::new();
        let raw = signed_test_data_shred(701, 0, &leader, [0; 32]);
        let record = validated_record(1, &raw, &leader, 0, false);

        let merged = merge_repair_records(701, vec![raw.payload().to_vec()], vec![record]).unwrap();

        assert_eq!(merged.stats.unique_shreds_added, 0);
        assert_eq!(merged.stats.duplicate_raw_shreds, 1);
        assert_eq!(merged.datagrams.len(), 1);
    }

    #[test]
    fn merge_rejects_a_claimed_successor_anchor_when_the_fec_path_is_absent() {
        let leader = Keypair::new();
        let raw = signed_test_data_shred(702, 64, &leader, [0; 32]);
        let repaired = signed_test_data_shred(702, 0, &leader, [0; 32]);
        let record = validated_record(2, &repaired, &leader, 32, true);

        let failure =
            merge_repair_records(702, vec![raw.payload().to_vec()], vec![record]).unwrap_err();

        assert_eq!(failure.category, "repair_anchor_chain_incomplete");
    }

    #[test]
    fn merge_rejects_an_ambiguous_raw_successor_in_the_anchor_path() {
        let leader = Keypair::new();
        let repaired = signed_test_data_shred(703, 0, &leader, [0; 32]);
        let repaired_root = repaired.merkle_root().unwrap();
        let matching_successor = signed_test_data_shred(703, 32, &leader, repaired_root);
        let conflicting_successor = signed_test_data_shred(703, 32, &leader, [1; 32]);
        let record = validated_record(2, &repaired, &leader, 32, true);

        let failure = merge_repair_records(
            703,
            vec![
                matching_successor.payload().to_vec(),
                conflicting_successor.payload().to_vec(),
            ],
            vec![record],
        )
        .unwrap_err();

        assert_eq!(failure.category, "repair_fec_identity_conflict");
        assert!(failure.message.contains("anchor path"));
    }

    fn scan_config(path: PathBuf, durable_through_sequence: u64) -> RepairWalScanConfig {
        RepairWalScanConfig {
            path,
            durable_through_sequence,
            max_records: 100,
            max_payload_bytes: 1024 * 1024,
            max_segments: 10,
        }
    }

    fn signed_test_data_shred(
        slot: u64,
        index: u32,
        leader: &Keypair,
        chained_merkle_root: [u8; 32],
    ) -> Shred {
        let mut payload = vec![0u8; 1_203];
        payload[64] = 0x90;
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&index.to_le_bytes());
        payload[77..79].copy_from_slice(&VERSION.to_le_bytes());
        payload[79..83].copy_from_slice(&(index / 32 * 32).to_le_bytes());
        payload[83..85].copy_from_slice(&1u16.to_le_bytes());
        payload[85] = 0b1100_0000;
        payload[86..88].copy_from_slice(&88u16.to_le_bytes());
        let chained_root_offset = payload.len() - 32;
        payload[chained_root_offset..].copy_from_slice(&chained_merkle_root);
        let unsigned = Shred::new_from_serialized_shred(payload.clone()).unwrap();
        let signature = leader.sign_message(unsigned.merkle_root().unwrap().as_ref());
        payload[..64].copy_from_slice(signature.as_ref());
        let shred = Shred::new_from_serialized_shred(payload).unwrap();
        assert!(shred.verify(&leader.pubkey()));
        shred
    }

    fn validated_record(
        sequence: u64,
        shred: &Shred,
        leader: &Keypair,
        trust_anchor_fec_set_index: u32,
        learned_chained_merkle_root: bool,
    ) -> ValidatedRepairRecord {
        ValidatedRepairRecord {
            sequence,
            slot: shred.slot(),
            index: shred.index(),
            fec_set_index: shred.fec_set_index(),
            version: shred.version(),
            expected_slot_leader: leader.pubkey().to_bytes(),
            trust_anchor_fec_set_index,
            learned_chained_merkle_root,
            payload: shred.payload().to_vec(),
            fec_identity: fec_identity(shred).unwrap(),
        }
    }

    fn encode_test_body(
        sequence: u64,
        shred: &Shred,
        leader: &Keypair,
        trust_anchor_fec_set_index: u32,
        learned_chained_merkle_root: bool,
    ) -> Vec<u8> {
        let peer_addr = b"127.0.0.1:8000";
        let payload = shred.payload();
        let mut body = Vec::new();
        body.extend_from_slice(&sequence.to_le_bytes());
        body.extend_from_slice(&1_723_456_789_012u64.to_le_bytes());
        body.extend_from_slice(&42u32.to_le_bytes());
        body.push(0);
        body.extend_from_slice(&shred.slot().to_le_bytes());
        body.extend_from_slice(&u64::from(shred.index()).to_le_bytes());
        body.extend_from_slice(&(peer_addr.len() as u16).to_le_bytes());
        body.extend_from_slice(peer_addr);
        body.extend_from_slice(leader.pubkey().as_ref());
        body.extend_from_slice(&shred.slot().to_le_bytes());
        body.extend_from_slice(&shred.index().to_le_bytes());
        body.extend_from_slice(&shred.fec_set_index().to_le_bytes());
        body.extend_from_slice(&shred.version().to_le_bytes());
        body.extend_from_slice(leader.pubkey().as_ref());
        body.extend_from_slice(shred.merkle_root().unwrap().as_ref());
        body.extend_from_slice(&trust_anchor_fec_set_index.to_le_bytes());
        body.push(u8::from(learned_chained_merkle_root));
        body.push(1);
        body.extend_from_slice(shred.chained_merkle_root().unwrap().as_ref());
        body.extend_from_slice(shred.signature().as_ref());
        body.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        body.extend_from_slice(payload);
        body
    }

    fn encode_frame(body: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(body);
        frame.extend_from_slice(&crc32(body).to_le_bytes());
        frame
    }

    fn encode_segment_header(
        segment_id: u64,
        first_sequence: u64,
        previous_last_sequence: u64,
        previous_chain_digest: [u8; 32],
    ) -> [u8; SEGMENT_HEADER_BYTES] {
        let mut header = [0u8; SEGMENT_HEADER_BYTES];
        header[..16].copy_from_slice(SEGMENT_FILE_MAGIC);
        header[16..24].copy_from_slice(&segment_id.to_le_bytes());
        header[24..32].copy_from_slice(&first_sequence.to_le_bytes());
        header[32..40].copy_from_slice(&previous_last_sequence.to_le_bytes());
        header[40..72].copy_from_slice(&previous_chain_digest);
        let checksum = crc32(&header[..SEGMENT_HEADER_CRC_OFFSET]);
        header[SEGMENT_HEADER_CRC_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        header
    }

    fn encode_v3_seal(next_sequence: u64, base_chain_digest: [u8; 32]) -> [u8; V3_SEAL_BYTES] {
        let mut seal = [0u8; V3_SEAL_BYTES];
        seal[..16].copy_from_slice(V3_SEAL_MAGIC);
        seal[16..24].copy_from_slice(&next_sequence.to_le_bytes());
        seal[24..56].copy_from_slice(&base_chain_digest);
        let checksum = crc32(&seal[..V3_SEAL_CRC_OFFSET]);
        seal[V3_SEAL_CRC_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        seal
    }

    fn encode_v3_head(
        active_segment_id: u64,
        active_file_len: u64,
        next_sequence: u64,
        chain_digest: [u8; 32],
    ) -> [u8; V3_HEAD_BYTES] {
        let mut head = [0u8; V3_HEAD_BYTES];
        head[..16].copy_from_slice(V3_HEAD_MAGIC);
        head[16..24].copy_from_slice(&active_segment_id.to_le_bytes());
        head[24..32].copy_from_slice(&active_file_len.to_le_bytes());
        head[32..40].copy_from_slice(&next_sequence.to_le_bytes());
        head[40..72].copy_from_slice(&chain_digest);
        let checksum = crc32(&head[..V3_HEAD_CRC_OFFSET]);
        head[V3_HEAD_CRC_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        head
    }
}
