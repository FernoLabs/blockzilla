//! Transport-independent durable receiver for the exact compressed Yellowstone block stream.
//!
//! Every new observation crosses two ordered durability boundaries: the exact compressed payload
//! is first synced through [`SpoolWriter`], then its cumulative rolling-chain cursor is appended
//! and synced through [`ReceiverProgressWal`]. Only the second boundary may produce an ACK.

use std::{
    collections::VecDeque,
    fs::{self, DirBuilder, File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
};

use anyhow::{Context, Result, anyhow, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    CommitmentEvidence, ContentDigest, CumulativePrimaryAck, DurableSpoolRecord, IngressRecordMeta,
    LogicalKey, REPLICATION_PROTOCOL_VERSION, ReceiptDisposition, ReplicationOffer,
    ReplicationStreamId, SpoolJournalIdentity, SpoolLocation, SpoolOptions, SpoolWriter,
    compute_content_digest, cumulative_chain_next, cumulative_chain_seed,
};

/// Exact format emitted by the raw recorder: one independent zstd frame containing a protobuf
/// `SubscribeUpdate` envelope.
pub const RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1: u16 = 2;
pub const RECEIVER_PROGRESS_WAL_FILE: &str = "receiver-progress.wal";

const RECEIVER_CAPACITY_LOCK_FILE: &str = ".receiver-capacity.lock";
const PROGRESS_COMPACTION_TEMP_SUFFIX: &str = ".compact.tmp";
const PROGRESS_WAL_MAGIC: &[u8; 8] = b"BZRPRG01";
const PROGRESS_FRAME_MAGIC: &[u8; 4] = b"BZRP";
const PROGRESS_COMMIT_MAGIC: &[u8; 4] = b"CMIT";
const PROGRESS_FRAME_VERSION: u16 = 1;
const PROGRESS_WAL_HEADER_LEN: u64 = PROGRESS_WAL_MAGIC.len() as u64;
const PROGRESS_FRAME_FIXED_LEN: u64 = 4 + 2 + 4 + 4;
const PROGRESS_FRAME_TRAILER_LEN: u64 = 4 + 4;
const MAX_PROGRESS_FRAME_BYTES: usize = 1024 * 1024;
const SPOOL_SEGMENT_HEADER_PROJECTION_RESERVE: u64 = 8;
const PROGRESS_PROJECTION_RESERVE: u64 = 256;
const QUOTA_ENTRY_MIN_BYTES: u64 = 4096;
const NEW_JOURNAL_PROJECTION_BYTES: u64 = 64 * 1024;
const MAX_BLOCK_TRANSACTIONS: u64 = 100_000;
const MAX_BLOCK_ENTRIES: u64 = 100_000;

/// Read-only view of the receiver's locally synced raw-byte prefix.
///
/// This is intentionally not a signed ACK, an indexer watermark, a compaction receipt, or cleanup
/// authority. It is suitable for bounding a local read-only materializer because every named
/// sequence has crossed both the raw spool fsync and receiver-progress fsync boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiverDurableProgress {
    pub stream: ReplicationStreamId,
    pub through_sequence: u64,
    pub through_content_digest: ContentDigest,
    pub rolling_chain_digest: ContentDigest,
    pub durable_lsn: u64,
    pub spool_location: SpoolLocation,
    pub unobserved_tail_bytes: u64,
}

/// Independent memory and decompression limits enforced before a batch mutates durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawReceiverLimits {
    pub max_batch_records: usize,
    pub max_batch_compressed_bytes: u64,
    pub max_batch_uncompressed_bytes: u64,
    pub max_compressed_record_bytes: u64,
    pub max_uncompressed_record_bytes: u64,
}

impl RawReceiverLimits {
    fn validate(self) -> Result<Self> {
        ensure!(
            self.max_batch_records > 0,
            "receiver batch record limit must be non-zero"
        );
        ensure!(
            self.max_batch_records <= usize::MAX / 2,
            "receiver batch record limit is too large"
        );
        ensure!(
            self.max_batch_compressed_bytes > 0,
            "receiver batch compressed-byte limit must be non-zero"
        );
        ensure!(
            self.max_batch_uncompressed_bytes > 0,
            "receiver batch uncompressed-byte limit must be non-zero"
        );
        ensure!(
            self.max_compressed_record_bytes > 0,
            "receiver compressed-record limit must be non-zero"
        );
        ensure!(
            self.max_uncompressed_record_bytes > 0,
            "receiver uncompressed-record limit must be non-zero"
        );
        ensure!(
            self.max_compressed_record_bytes <= self.max_batch_compressed_bytes,
            "receiver compressed-record limit exceeds batch limit"
        );
        ensure!(
            self.max_uncompressed_record_bytes <= self.max_batch_uncompressed_bytes,
            "receiver uncompressed-record limit exceeds batch limit"
        );
        Ok(self)
    }
}

/// One exact payload and its sender-asserted durable metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawReplicationRecord {
    pub offer: ReplicationOffer,
    pub compressed_payload: Vec<u8>,
    pub raw_protobuf_sha256: [u8; 32],
    pub uncompressed_len: u64,
}

/// Perform the complete CPU/memory-bounded validation needed before a service admits a brand-new
/// persistent journal. This function never creates files or mutates receiver state.
///
/// `require_initial_sequence_zero` must be true only after the caller has established, under its
/// journal-admission lock, that the stream has no existing durable directory.
pub fn validate_raw_replication_batch(
    records: &[RawReplicationRecord],
    stream: &ReplicationStreamId,
    limits: RawReceiverLimits,
    require_initial_sequence_zero: bool,
) -> Result<()> {
    limits.validate()?;
    cumulative_chain_seed(stream)
        .map_err(|error| anyhow!("receiver stream identity is invalid: {error:?}"))?;
    ensure!(!records.is_empty(), "receiver batch must not be empty");
    ensure!(
        records.len() <= limits.max_batch_records,
        "receiver batch has {} records, maximum {}",
        records.len(),
        limits.max_batch_records
    );
    if require_initial_sequence_zero {
        ensure!(
            records[0].offer.record.sequence == 0,
            "receiver stream must start at sequence 0, received {}",
            records[0].offer.record.sequence
        );
    }

    let mut compressed_total = 0u64;
    let mut uncompressed_total = 0u64;
    let mut previous_sequence: Option<u64> = None;
    for record in records {
        compressed_total = compressed_total
            .checked_add(record.compressed_payload.len() as u64)
            .context("receiver batch compressed-byte accounting overflow")?;
        uncompressed_total = uncompressed_total
            .checked_add(record.uncompressed_len)
            .context("receiver batch uncompressed-byte accounting overflow")?;
        ensure!(
            compressed_total <= limits.max_batch_compressed_bytes,
            "receiver batch compressed bytes exceed configured maximum"
        );
        ensure!(
            uncompressed_total <= limits.max_batch_uncompressed_bytes,
            "receiver batch uncompressed bytes exceed configured maximum"
        );
        if let Some(previous) = previous_sequence {
            let expected = previous
                .checked_add(1)
                .context("receiver batch sequence exhausted")?;
            ensure!(
                record.offer.record.sequence == expected,
                "receiver batch sequence is not contiguous: {} after {}",
                record.offer.record.sequence,
                previous
            );
        }
        previous_sequence = Some(record.offer.record.sequence);
        validate_raw_replication_record(record, stream, limits)?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct RawReceiverConfig {
    pub spool_root: PathBuf,
    pub stream: ReplicationStreamId,
    pub primary_id: String,
    pub primary_term: u64,
    pub spool_options: SpoolOptions,
    pub max_spool_bytes: u64,
    pub reserve_free_bytes: u64,
    pub limits: RawReceiverLimits,
}

impl RawReceiverConfig {
    fn validate(self) -> Result<Self> {
        ensure!(
            self.spool_root.is_absolute() && self.spool_root != Path::new("/"),
            "receiver spool root must be an absolute non-root directory"
        );
        ensure!(
            is_stable_receiver_id(&self.primary_id),
            "receiver primary id must be 1..=64 ASCII identifier characters"
        );
        ensure!(
            self.primary_term > 0,
            "receiver primary term must be non-zero"
        );
        cumulative_chain_seed(&self.stream)
            .map_err(|error| anyhow!("receiver stream identity is invalid: {error:?}"))?;
        self.spool_options.validate()?;
        self.limits.validate()?;
        ensure!(
            self.max_spool_bytes >= self.spool_options.segment_target_bytes,
            "receiver maximum spool bytes must cover at least one target segment"
        );
        ensure!(
            self.limits.max_compressed_record_bytes <= self.spool_options.max_record_bytes,
            "receiver compressed-record limit exceeds spool maximum"
        );
        Ok(self)
    }
}

#[derive(Debug, Clone)]
struct ValidatedRawRecord {
    offer: ReplicationOffer,
    compressed_payload: Vec<u8>,
    raw_protobuf_sha256: [u8; 32],
    uncompressed_len: u64,
    metadata: IngressRecordMeta,
}

#[derive(Debug, Clone)]
struct PlannedRawRecord {
    validated: ValidatedRawRecord,
    previous_chain_digest: ContentDigest,
    rolling_chain_digest: ContentDigest,
    already_durable: bool,
}

/// Single-stream durable receiver core.
#[derive(Debug)]
pub struct BlockzillaRawReceiver {
    config: RawReceiverConfig,
    spool: SpoolWriter,
    progress: ReceiverProgressWal,
    // Mutations take an exclusive cross-process lock on this descriptor so root accounting and
    // the writes it admits are atomic while distinct receiver streams can remain open.
    capacity_lock: File,
    poisoned: bool,
}

impl BlockzillaRawReceiver {
    pub fn open(config: RawReceiverConfig) -> Result<Self> {
        let config = config.validate()?;
        ensure_receiver_spool_root_durable(&config.spool_root)?;
        let capacity_lock = open_receiver_capacity_lock(&config.spool_root)?;
        let _capacity_guard = ReceiverCapacityGuard::acquire(
            capacity_lock
                .try_clone()
                .context("clone receiver capacity lock")?,
        )?;
        admit_receiver_open(&config)?;
        let spool_identity = SpoolJournalIdentity {
            cluster_id: config.stream.cluster_id.clone(),
            origin_node_id: config.stream.origin_node_id.clone(),
            source_id: config.stream.source_id.clone(),
            journal_id: config.stream.journal_id,
        };
        let spool = SpoolWriter::open(&config.spool_root, spool_identity, config.spool_options)
            .context("open Blockzilla receiver raw spool")?;
        let progress_path = spool.journal_dir().join(RECEIVER_PROGRESS_WAL_FILE);
        let progress = ReceiverProgressWal::open(
            &progress_path,
            config.stream.clone(),
            config.limits.max_batch_records,
        )?;
        let mut receiver = Self {
            config,
            spool,
            progress,
            capacity_lock,
            poisoned: false,
        };
        receiver.reconcile_spool_and_progress()?;
        ensure_receiver_capacity_current(&receiver.config)?;
        Ok(receiver)
    }

    pub fn stream(&self) -> &ReplicationStreamId {
        &self.config.stream
    }

    pub fn spool_journal_dir(&self) -> &Path {
        self.spool.journal_dir()
    }

    pub fn progress_wal_path(&self) -> &Path {
        self.progress.path()
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned || self.spool.is_poisoned() || self.progress.is_poisoned()
    }

    /// Return the latest durable unsigned ACK without modifying either journal.
    pub fn get_ack(&self) -> Option<CumulativePrimaryAck> {
        self.progress.latest().map(|frame| self.ack_for(frame))
    }

    /// Validate and durably ingest a bounded contiguous batch.
    pub fn push_batch(
        &mut self,
        records: Vec<RawReplicationRecord>,
    ) -> Result<CumulativePrimaryAck> {
        self.push_batch_with_faults(records, &mut NoReceiverFaults)
    }

    fn push_batch_with_faults<F: ReceiverFaultInjector>(
        &mut self,
        records: Vec<RawReplicationRecord>,
        faults: &mut F,
    ) -> Result<CumulativePrimaryAck> {
        ensure!(
            !self.is_poisoned(),
            "receiver is fail-stop after an ambiguous durability failure; reopen to recover"
        );
        let validated = self.validate_batch(records)?;
        let plan = self.plan_batch(validated)?;
        let first_new = plan.iter().position(|record| !record.already_durable);
        if let Some(first_new) = first_new {
            let _capacity_guard = ReceiverCapacityGuard::acquire(
                self.capacity_lock
                    .try_clone()
                    .context("clone receiver capacity lock")?,
            )?;
            self.admit_new_records(&plan[first_new..])?;
            faults.check(
                ReceiverFaultPoint::AfterValidationBeforeSpool,
                plan[first_new].validated.offer.record.sequence,
            )?;
            for planned in &plan[first_new..] {
                ensure!(
                    !planned.already_durable,
                    "receiver batch plan placed a durable record after a new record"
                );
                self.poisoned = true;
                let durable = self.spool.append_and_sync(
                    planned.validated.metadata.clone(),
                    &planned.validated.compressed_payload,
                )?;
                faults.check(
                    ReceiverFaultPoint::AfterSpoolSyncBeforeProgress,
                    planned.validated.offer.record.sequence,
                )?;
                self.progress.append_and_sync(
                    &planned.validated,
                    planned.previous_chain_digest,
                    planned.rolling_chain_digest,
                    &durable,
                )?;
                self.poisoned = false;
                faults.check(
                    ReceiverFaultPoint::AfterProgressSync,
                    planned.validated.offer.record.sequence,
                )?;
            }
        }
        self.get_ack()
            .context("receiver batch produced no durable cumulative ACK")
    }

    fn admit_new_records(&self, records: &[PlannedRawRecord]) -> Result<()> {
        let mut projected_growth = 0u64;
        let mut projected_progress_growth = 0u64;
        let first_lsn = self.progress.latest().map_or(Ok(1), |latest| {
            latest
                .durable_lsn
                .checked_add(1)
                .context("receiver durable LSN exhausted")
        })?;
        for (index, record) in records.iter().enumerate() {
            let index = u64::try_from(index).context("receiver projected batch index overflow")?;
            let projection = self.spool.project_append(
                &record.validated.metadata,
                &record.validated.compressed_payload,
            )?;
            projected_growth = projected_growth
                .checked_add(projection.additional_bytes)
                .and_then(|bytes| bytes.checked_add(SPOOL_SEGMENT_HEADER_PROJECTION_RESERVE))
                .context("receiver spool projection overflow")?;
            let projected_frame = StoredProgressFrame {
                checkpoint: false,
                stream: self.config.stream.clone(),
                offer: record.validated.offer.clone(),
                raw_protobuf_sha256: record.validated.raw_protobuf_sha256,
                uncompressed_len: record.validated.uncompressed_len,
                previous_chain_digest: record.previous_chain_digest,
                rolling_chain_digest: record.rolling_chain_digest,
                spool_location: projection.location,
                durable_lsn: first_lsn
                    .checked_add(index)
                    .context("receiver projected durable LSN overflow")?,
            };
            let progress_payload = serde_json::to_vec(&projected_frame)
                .context("encode projected receiver progress frame")?;
            let progress_growth = PROGRESS_FRAME_FIXED_LEN
                .checked_add(progress_payload.len() as u64)
                .and_then(|bytes| bytes.checked_add(PROGRESS_FRAME_TRAILER_LEN))
                .and_then(|bytes| bytes.checked_add(PROGRESS_PROJECTION_RESERVE))
                .context("receiver progress projection overflow")?;
            projected_growth = projected_growth
                .checked_add(progress_growth)
                .context("receiver total storage projection overflow")?;
            projected_progress_growth = projected_progress_growth
                .checked_add(progress_growth)
                .context("receiver progress storage projection overflow")?;
        }
        if self.progress.will_compact_after(records.len())? {
            // Compaction writes and syncs a replacement before atomically renaming it. Account for
            // both the old WAL and a conservative replacement while they coexist.
            let replacement_projection = self
                .progress
                .file_len()?
                .checked_add(projected_progress_growth)
                .context("receiver progress compaction projection overflow")?;
            projected_growth = projected_growth
                .checked_add(replacement_projection.max(QUOTA_ENTRY_MIN_BYTES))
                .context("receiver transient compaction storage projection overflow")?;
        }
        let current_bytes = receiver_spool_root_bytes(&self.config.spool_root)?;
        ensure!(
            current_bytes
                .checked_add(projected_growth)
                .is_some_and(|bytes| bytes <= self.config.max_spool_bytes),
            "receiver spool capacity would be exceeded: current {current_bytes}, projected growth {projected_growth}, maximum {}",
            self.config.max_spool_bytes
        );
        let available_bytes = filesystem_available_bytes(&self.config.spool_root)?;
        ensure!(
            self.config
                .reserve_free_bytes
                .checked_add(projected_growth)
                .is_some_and(|required| available_bytes >= required),
            "receiver filesystem reserve would be crossed: available {available_bytes}, projected growth {projected_growth}, reserve {}",
            self.config.reserve_free_bytes
        );
        Ok(())
    }

    fn validate_batch(
        &self,
        records: Vec<RawReplicationRecord>,
    ) -> Result<Vec<ValidatedRawRecord>> {
        ensure!(!records.is_empty(), "receiver batch must not be empty");
        ensure!(
            records.len() <= self.config.limits.max_batch_records,
            "receiver batch has {} records, maximum {}",
            records.len(),
            self.config.limits.max_batch_records
        );
        let mut compressed_total = 0u64;
        let mut uncompressed_total = 0u64;
        let mut validated = Vec::with_capacity(records.len());
        let mut previous_sequence: Option<u64> = None;
        for record in records {
            compressed_total = compressed_total
                .checked_add(record.compressed_payload.len() as u64)
                .context("receiver batch compressed-byte accounting overflow")?;
            uncompressed_total = uncompressed_total
                .checked_add(record.uncompressed_len)
                .context("receiver batch uncompressed-byte accounting overflow")?;
            ensure!(
                compressed_total <= self.config.limits.max_batch_compressed_bytes,
                "receiver batch compressed bytes exceed configured maximum"
            );
            ensure!(
                uncompressed_total <= self.config.limits.max_batch_uncompressed_bytes,
                "receiver batch uncompressed bytes exceed configured maximum"
            );
            if let Some(previous) = previous_sequence {
                let expected = previous
                    .checked_add(1)
                    .context("receiver batch sequence exhausted")?;
                ensure!(
                    record.offer.record.sequence == expected,
                    "receiver batch sequence is not contiguous: {} after {}",
                    record.offer.record.sequence,
                    previous
                );
            }
            previous_sequence = Some(record.offer.record.sequence);
            validated.push(self.validate_record(record)?);
        }
        Ok(validated)
    }

    fn validate_record(&self, record: RawReplicationRecord) -> Result<ValidatedRawRecord> {
        validate_record_against_stream(record, &self.config.stream, self.config.limits)
    }

    fn plan_batch(&self, validated: Vec<ValidatedRawRecord>) -> Result<Vec<PlannedRawRecord>> {
        let first_sequence = validated
            .first()
            .expect("validated receiver batch is non-empty")
            .offer
            .record
            .sequence;
        let latest_sequence = self
            .progress
            .latest()
            .map(|frame| frame.offer.record.sequence);
        match latest_sequence {
            None => ensure!(
                first_sequence == 0,
                "receiver stream must start at sequence 0, received {first_sequence}"
            ),
            Some(latest) if first_sequence > latest => ensure!(
                first_sequence
                    == latest
                        .checked_add(1)
                        .context("receiver sequence exhausted")?,
                "receiver sequence gap: durable through {latest}, received {first_sequence}"
            ),
            Some(_) => {}
        }

        let mut previous_chain = if first_sequence == 0 {
            cumulative_chain_seed(&self.config.stream)
                .map_err(|error| anyhow!("build receiver chain seed: {error:?}"))?
        } else if latest_sequence.is_some_and(|latest| first_sequence <= latest) {
            self.progress
                .recent(first_sequence)
                .with_context(|| {
                    format!(
                        "duplicate sequence {first_sequence} is outside the exact retry window; call get_ack and resume after its cursor"
                    )
                })?
                .previous_chain_digest
        } else {
            self.progress
                .latest()
                .context("receiver progress is missing before a nonzero sequence")?
                .rolling_chain_digest
        };

        let mut plan = Vec::with_capacity(validated.len());
        for record in validated {
            let rolling_chain =
                cumulative_chain_next(&self.config.stream, previous_chain, &record.offer)
                    .map_err(|error| anyhow!("extend receiver cumulative chain: {error:?}"))?;
            let already_durable =
                latest_sequence.is_some_and(|latest| record.offer.record.sequence <= latest);
            if already_durable {
                let stored = self
                    .progress
                    .recent(record.offer.record.sequence)
                    .with_context(|| {
                        format!(
                            "duplicate sequence {} is outside the exact retry window; call get_ack and resume after its cursor",
                            record.offer.record.sequence
                        )
                    })?;
                ensure!(
                    stored.offer == record.offer
                        && stored.raw_protobuf_sha256 == record.raw_protobuf_sha256
                        && stored.uncompressed_len == record.uncompressed_len
                        && stored.previous_chain_digest == previous_chain
                        && stored.rolling_chain_digest == rolling_chain,
                    "receiver observation sequence {} was reused with different content or chain",
                    record.offer.record.sequence
                );
            }
            plan.push(PlannedRawRecord {
                validated: record,
                previous_chain_digest: previous_chain,
                rolling_chain_digest: rolling_chain,
                already_durable,
            });
            previous_chain = rolling_chain;
        }
        Ok(plan)
    }

    fn reconcile_spool_and_progress(&mut self) -> Result<()> {
        let spool_tail = self.spool.last_record().cloned();
        let progress_tail = self.progress.latest().cloned();
        match (spool_tail, progress_tail) {
            (None, None) => Ok(()),
            (None, Some(_)) => Err(anyhow!(
                "receiver progress is ahead of an empty raw spool; refusing startup"
            )),
            (Some(durable), None) => {
                ensure!(
                    durable.metadata().observation.sequence == 0,
                    "receiver raw spool is more than one record ahead of empty progress"
                );
                self.repair_progress_from_spool(durable, None)
            }
            (Some(durable), Some(progress)) => {
                let spool_sequence = durable.metadata().observation.sequence;
                let progress_sequence = progress.offer.record.sequence;
                if spool_sequence == progress_sequence {
                    let validated = self.validated_spool_tail(&durable)?;
                    ensure!(
                        progress.matches_validated(&validated, durable.location()),
                        "receiver spool and progress tails disagree"
                    );
                    Ok(())
                } else if progress_sequence
                    .checked_add(1)
                    .is_some_and(|next| spool_sequence == next)
                {
                    self.repair_progress_from_spool(durable, Some(progress))
                } else {
                    Err(anyhow!(
                        "receiver spool/progress divergence is larger than one record: spool {spool_sequence}, progress {progress_sequence}"
                    ))
                }
            }
        }
    }

    fn repair_progress_from_spool(
        &mut self,
        durable: DurableSpoolRecord,
        previous: Option<StoredProgressFrame>,
    ) -> Result<()> {
        let validated = self.validated_spool_tail(&durable)?;
        let previous_chain = match previous.as_ref() {
            Some(previous) => previous.rolling_chain_digest,
            None => cumulative_chain_seed(&self.config.stream)
                .map_err(|error| anyhow!("build recovery chain seed: {error:?}"))?,
        };
        let rolling_chain =
            cumulative_chain_next(&self.config.stream, previous_chain, &validated.offer)
                .map_err(|error| anyhow!("extend recovery cumulative chain: {error:?}"))?;
        self.progress
            .append_and_sync(&validated, previous_chain, rolling_chain, &durable)
            .context("repair receiver progress from durable spool tail")
    }

    fn validated_spool_tail(&self, durable: &DurableSpoolRecord) -> Result<ValidatedRawRecord> {
        let stored = self.spool.read_record(durable)?;
        let metadata = &stored.metadata;
        let record = RawReplicationRecord {
            offer: ReplicationOffer {
                protocol_version: REPLICATION_PROTOCOL_VERSION,
                cluster_id: metadata.cluster_id.clone(),
                record: metadata.observation.clone(),
                source_id: metadata.source_id.clone(),
                logical_key: metadata.logical_key.clone(),
                content_digest: metadata.content_digest,
                payload_len: metadata.payload_len,
                payload_format_version: metadata.payload_format_version,
                commitment: CommitmentEvidence::Confirmed,
            },
            compressed_payload: stored.payload,
            raw_protobuf_sha256: [0; 32],
            uncompressed_len: 0,
        };
        validate_recovered_record(record, &self.config.stream, self.config.limits)
    }

    fn ack_for(&self, frame: &StoredProgressFrame) -> CumulativePrimaryAck {
        CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: self.config.stream.clone(),
            primary_id: self.config.primary_id.clone(),
            primary_term: self.config.primary_term,
            through_sequence: frame.offer.record.sequence,
            through_content_digest: frame.offer.content_digest,
            rolling_chain_digest: frame.rolling_chain_digest,
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: frame.durable_lsn,
            signing_key_id: String::new(),
            signature: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiverFaultPoint {
    AfterValidationBeforeSpool,
    AfterSpoolSyncBeforeProgress,
    AfterProgressSync,
}

trait ReceiverFaultInjector {
    fn check(&mut self, point: ReceiverFaultPoint, sequence: u64) -> Result<()>;
}

struct NoReceiverFaults;

impl ReceiverFaultInjector for NoReceiverFaults {
    fn check(&mut self, _point: ReceiverFaultPoint, _sequence: u64) -> Result<()> {
        Ok(())
    }
}

fn validate_record_against_stream(
    record: RawReplicationRecord,
    stream: &ReplicationStreamId,
    limits: RawReceiverLimits,
) -> Result<ValidatedRawRecord> {
    let metadata = validate_raw_replication_record(&record, stream, limits)?;
    Ok(ValidatedRawRecord {
        offer: record.offer,
        compressed_payload: record.compressed_payload,
        raw_protobuf_sha256: record.raw_protobuf_sha256,
        uncompressed_len: record.uncompressed_len,
        metadata,
    })
}

fn validate_raw_replication_record(
    record: &RawReplicationRecord,
    stream: &ReplicationStreamId,
    limits: RawReceiverLimits,
) -> Result<IngressRecordMeta> {
    validate_offer_identity(&record.offer, stream)?;
    ensure!(
        record.compressed_payload.len() as u64 == record.offer.payload_len,
        "receiver compressed payload length differs from replication offer"
    );
    ensure!(
        record.offer.payload_len <= limits.max_compressed_record_bytes,
        "receiver compressed record exceeds configured maximum"
    );
    ensure!(
        record.uncompressed_len > 0
            && record.uncompressed_len <= limits.max_uncompressed_record_bytes,
        "receiver uncompressed record length is outside configured bounds"
    );
    let capacity = usize::try_from(record.uncompressed_len)
        .context("receiver uncompressed record length exceeds addressable memory")?;
    let raw = zstd::bulk::decompress(&record.compressed_payload, capacity)
        .context("decompress receiver raw protobuf")?;
    validate_decoded_record_with_raw(
        &record.offer,
        &record.compressed_payload,
        record.raw_protobuf_sha256,
        record.uncompressed_len,
        limits,
        &raw,
    )
}

fn validate_recovered_record(
    mut record: RawReplicationRecord,
    stream: &ReplicationStreamId,
    limits: RawReceiverLimits,
) -> Result<ValidatedRawRecord> {
    validate_offer_identity(&record.offer, stream)?;
    ensure!(
        record.compressed_payload.len() as u64 == record.offer.payload_len,
        "recovered receiver compressed payload length differs from spool metadata"
    );
    ensure!(
        record.offer.payload_len <= limits.max_compressed_record_bytes,
        "recovered receiver compressed record exceeds configured maximum"
    );
    let maximum = usize::try_from(limits.max_uncompressed_record_bytes)
        .context("receiver uncompressed limit exceeds addressable memory")?;
    let raw = zstd::bulk::decompress(&record.compressed_payload, maximum)
        .context("decompress recovered receiver protobuf")?;
    record.uncompressed_len = raw.len() as u64;
    record.raw_protobuf_sha256 = Sha256::digest(&raw).into();
    let metadata = validate_decoded_record_with_raw(
        &record.offer,
        &record.compressed_payload,
        record.raw_protobuf_sha256,
        record.uncompressed_len,
        limits,
        &raw,
    )?;
    Ok(ValidatedRawRecord {
        offer: record.offer,
        compressed_payload: record.compressed_payload,
        raw_protobuf_sha256: record.raw_protobuf_sha256,
        uncompressed_len: record.uncompressed_len,
        metadata,
    })
}

fn validate_offer_identity(offer: &ReplicationOffer, stream: &ReplicationStreamId) -> Result<()> {
    ensure!(
        offer.protocol_version == REPLICATION_PROTOCOL_VERSION,
        "unsupported receiver replication protocol version {}",
        offer.protocol_version
    );
    ensure!(
        ReplicationStreamId::from_offer(offer) == *stream,
        "receiver replication offer belongs to a different stream"
    );
    ensure!(
        offer.payload_format_version == RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
        "unsupported receiver raw payload format {}",
        offer.payload_format_version
    );
    ensure!(
        offer.commitment == CommitmentEvidence::Confirmed,
        "receiver accepts only confirmed raw block observations"
    );
    Ok(())
}

fn validate_decoded_record_with_raw(
    offer: &ReplicationOffer,
    compressed_payload: &[u8],
    raw_protobuf_sha256: [u8; 32],
    uncompressed_len: u64,
    limits: RawReceiverLimits,
    raw: &[u8],
) -> Result<IngressRecordMeta> {
    ensure!(
        uncompressed_len > 0 && uncompressed_len <= limits.max_uncompressed_record_bytes,
        "receiver uncompressed record length is outside configured bounds"
    );
    ensure!(
        raw.len() as u64 == uncompressed_len,
        "receiver raw protobuf length differs from declared uncompressed length"
    );
    ensure!(
        <[u8; 32]>::from(Sha256::digest(raw)) == raw_protobuf_sha256,
        "receiver raw protobuf SHA-256 mismatch"
    );
    ensure!(
        compute_content_digest(
            &offer.cluster_id,
            &offer.logical_key,
            offer.payload_format_version,
            compressed_payload,
        ) == offer.content_digest,
        "receiver replication offer content digest mismatch"
    );
    let block = scan_subscribe_update_block(raw)?;
    let logical_key = LogicalKey::Block {
        slot: block.slot,
        blockhash: block.blockhash,
    };
    ensure!(
        offer.logical_key == logical_key,
        "receiver protobuf block identity differs from replication offer"
    );
    Ok(IngressRecordMeta {
        cluster_id: offer.cluster_id.clone(),
        observation: offer.record.clone(),
        source_id: offer.source_id.clone(),
        logical_key,
        payload_format_version: offer.payload_format_version,
        content_digest: offer.content_digest,
        payload_len: offer.payload_len,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScannedBlockIdentity {
    slot: u64,
    blockhash: [u8; 32],
}

#[derive(Debug, Clone, Copy)]
enum ProtoValue<'a> {
    Varint(u64),
    Fixed64,
    Bytes(&'a [u8]),
    Fixed32,
}

#[derive(Debug, Clone, Copy)]
struct ProtoField<'a> {
    number: u32,
    value: ProtoValue<'a>,
}

impl<'a> ProtoField<'a> {
    fn varint(self, label: &str) -> Result<u64> {
        match self.value {
            ProtoValue::Varint(value) => Ok(value),
            _ => Err(anyhow!("receiver protobuf {label} has the wrong wire type")),
        }
    }

    fn bytes(self, label: &str) -> Result<&'a [u8]> {
        match self.value {
            ProtoValue::Bytes(value) => Ok(value),
            _ => Err(anyhow!("receiver protobuf {label} has the wrong wire type")),
        }
    }
}

struct ProtoCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ProtoCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn next(&mut self) -> Result<Option<ProtoField<'a>>> {
        if self.offset == self.bytes.len() {
            return Ok(None);
        }
        let key = read_proto_varint(self.bytes, &mut self.offset)?;
        let field_number = key >> 3;
        ensure!(
            field_number > 0 && field_number <= 0x1fff_ffff,
            "receiver protobuf contains an invalid field number"
        );
        let number = u32::try_from(field_number).expect("validated protobuf field number fits u32");
        let value = match key & 7 {
            0 => ProtoValue::Varint(read_proto_varint(self.bytes, &mut self.offset)?),
            1 => {
                self.take(8)?;
                ProtoValue::Fixed64
            }
            2 => {
                let length = usize::try_from(read_proto_varint(self.bytes, &mut self.offset)?)
                    .context("receiver protobuf field length exceeds addressable memory")?;
                ProtoValue::Bytes(self.take(length)?)
            }
            5 => {
                self.take(4)?;
                ProtoValue::Fixed32
            }
            wire_type => {
                return Err(anyhow!(
                    "receiver protobuf contains unsupported wire type {wire_type}"
                ));
            }
        };
        Ok(Some(ProtoField { number, value }))
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .context("receiver protobuf field length overflow")?;
        ensure!(
            end <= self.bytes.len(),
            "receiver protobuf contains a truncated field"
        );
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }
}

fn read_proto_varint(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for byte_index in 0..10u32 {
        let byte = *bytes
            .get(*offset)
            .context("receiver protobuf contains a truncated varint")?;
        *offset = offset
            .checked_add(1)
            .context("receiver protobuf offset overflow")?;
        if byte_index == 9 {
            ensure!(
                byte <= 1,
                "receiver protobuf contains an overflowing varint"
            );
        }
        value |= u64::from(byte & 0x7f) << (byte_index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(anyhow!("receiver protobuf contains an unterminated varint"))
}

fn mark_proto_field_once(seen: &mut bool, label: &str) -> Result<()> {
    ensure!(!*seen, "receiver protobuf repeats singular field {label}");
    *seen = true;
    Ok(())
}

fn scan_subscribe_update_block(raw: &[u8]) -> Result<ScannedBlockIdentity> {
    let mut cursor = ProtoCursor::new(raw);
    let mut block = None;
    let mut created_at_seen = false;
    let mut filter_count = 0u8;
    while let Some(field) = cursor.next()? {
        match field.number {
            1 => {
                filter_count = filter_count
                    .checked_add(1)
                    .context("receiver SubscribeUpdate has more than 255 filters")?;
                ensure!(
                    filter_count <= 64,
                    "receiver SubscribeUpdate exceeds the filter safety limit"
                );
                let filter = field.bytes("SubscribeUpdate.filters")?;
                std::str::from_utf8(filter)
                    .context("receiver SubscribeUpdate filter is not UTF-8")?;
            }
            5 => {
                ensure!(
                    block.is_none(),
                    "receiver SubscribeUpdate repeats its block payload"
                );
                block = Some(field.bytes("SubscribeUpdate.block")?);
            }
            11 => {
                mark_proto_field_once(&mut created_at_seen, "SubscribeUpdate.created_at")?;
                field.bytes("SubscribeUpdate.created_at")?;
            }
            2 | 3 | 4 | 6 | 7 | 8 | 9 | 10 => {
                return Err(anyhow!(
                    "receiver SubscribeUpdate contains a non-block oneof payload"
                ));
            }
            number => {
                return Err(anyhow!(
                    "receiver SubscribeUpdate contains unsupported field {number}"
                ));
            }
        }
    }
    scan_block_message(block.context("receiver raw protobuf is not a block update")?)
}

fn scan_block_message(bytes: &[u8]) -> Result<ScannedBlockIdentity> {
    let mut cursor = ProtoCursor::new(bytes);
    let mut slot = 0u64;
    let mut slot_seen = false;
    let mut blockhash_bytes = None;
    let mut parent_blockhash_bytes = None;
    let mut rewards_seen = false;
    let mut block_time_seen = false;
    let mut block_height_seen = false;
    let mut parent_slot_seen = false;
    let mut executed_transaction_count = 0u64;
    let mut executed_transaction_count_seen = false;
    let mut entries_count = 0u64;
    let mut entries_count_seen = false;
    let mut updated_account_count_seen = false;
    let mut transaction_count = 0u64;
    let mut entry_count = 0u64;
    let mut entry_slot = None;
    let mut expected_entry_transaction_index = 0u64;
    let mut final_entry_hash = None;

    while let Some(field) = cursor.next()? {
        match field.number {
            1 => {
                mark_proto_field_once(&mut slot_seen, "SubscribeUpdateBlock.slot")?;
                slot = field.varint("SubscribeUpdateBlock.slot")?;
            }
            2 => {
                ensure!(
                    blockhash_bytes.is_none(),
                    "receiver protobuf repeats singular field SubscribeUpdateBlock.blockhash"
                );
                blockhash_bytes = Some(field.bytes("SubscribeUpdateBlock.blockhash")?);
            }
            3 => {
                mark_proto_field_once(&mut rewards_seen, "SubscribeUpdateBlock.rewards")?;
                field.bytes("SubscribeUpdateBlock.rewards")?;
            }
            4 => {
                mark_proto_field_once(&mut block_time_seen, "SubscribeUpdateBlock.block_time")?;
                field.bytes("SubscribeUpdateBlock.block_time")?;
            }
            5 => {
                mark_proto_field_once(&mut block_height_seen, "SubscribeUpdateBlock.block_height")?;
                field.bytes("SubscribeUpdateBlock.block_height")?;
            }
            6 => {
                ensure!(
                    transaction_count < MAX_BLOCK_TRANSACTIONS,
                    "receiver block exceeds the transaction safety limit"
                );
                let transaction =
                    scan_transaction_info(field.bytes("SubscribeUpdateBlock.transactions")?)?;
                ensure!(
                    transaction.index == transaction_count,
                    "receiver block transactions are not contiguous in wire order"
                );
                transaction_count = transaction_count
                    .checked_add(1)
                    .context("receiver block transaction count overflow")?;
            }
            7 => {
                mark_proto_field_once(&mut parent_slot_seen, "SubscribeUpdateBlock.parent_slot")?;
                field.varint("SubscribeUpdateBlock.parent_slot")?;
            }
            8 => {
                ensure!(
                    parent_blockhash_bytes.is_none(),
                    "receiver protobuf repeats singular field SubscribeUpdateBlock.parent_blockhash"
                );
                parent_blockhash_bytes =
                    Some(field.bytes("SubscribeUpdateBlock.parent_blockhash")?);
            }
            9 => {
                mark_proto_field_once(
                    &mut executed_transaction_count_seen,
                    "SubscribeUpdateBlock.executed_transaction_count",
                )?;
                executed_transaction_count =
                    field.varint("SubscribeUpdateBlock.executed_transaction_count")?;
            }
            10 => {
                mark_proto_field_once(
                    &mut updated_account_count_seen,
                    "SubscribeUpdateBlock.updated_account_count",
                )?;
                ensure!(
                    field.varint("SubscribeUpdateBlock.updated_account_count")? == 0,
                    "receiver raw block declares updated account payloads"
                );
            }
            11 => {
                field.bytes("SubscribeUpdateBlock.accounts")?;
                return Err(anyhow!(
                    "receiver raw block unexpectedly contains account payloads"
                ));
            }
            12 => {
                mark_proto_field_once(
                    &mut entries_count_seen,
                    "SubscribeUpdateBlock.entries_count",
                )?;
                entries_count = field.varint("SubscribeUpdateBlock.entries_count")?;
            }
            13 => {
                ensure!(
                    entry_count < MAX_BLOCK_ENTRIES,
                    "receiver block exceeds the PoH entry safety limit"
                );
                let entry = scan_entry(field.bytes("SubscribeUpdateBlock.entries")?)?;
                ensure!(
                    entry.index == entry_count,
                    "receiver block entries are not contiguous in wire order"
                );
                if let Some(expected_slot) = entry_slot {
                    ensure!(
                        entry.slot == expected_slot,
                        "receiver block PoH entries disagree on slot"
                    );
                } else {
                    entry_slot = Some(entry.slot);
                }
                ensure!(
                    entry.starting_transaction_index == expected_entry_transaction_index,
                    "receiver block entry transaction ranges are discontinuous"
                );
                expected_entry_transaction_index = expected_entry_transaction_index
                    .checked_add(entry.executed_transaction_count)
                    .context("receiver block entry transaction count overflow")?;
                final_entry_hash = Some(entry.hash);
                entry_count = entry_count
                    .checked_add(1)
                    .context("receiver block entry count overflow")?;
            }
            number => {
                return Err(anyhow!(
                    "receiver SubscribeUpdateBlock contains unsupported field {number}"
                ));
            }
        }
    }

    let blockhash = decode_proto_base58_hash(
        blockhash_bytes.context("receiver block is missing its blockhash")?,
        "blockhash",
    )?;
    match parent_blockhash_bytes {
        Some([]) if slot == 0 => {}
        Some(parent) => {
            decode_proto_base58_hash(parent, "parent blockhash")?;
        }
        None if slot == 0 => {}
        None => return Err(anyhow!("receiver block is missing its parent blockhash")),
    }
    ensure!(
        transaction_count == executed_transaction_count,
        "receiver block transaction vector is incomplete"
    );
    ensure!(
        entries_count > 0 && entry_count == entries_count,
        "receiver block entry vector is incomplete"
    );
    ensure!(
        entry_slot == Some(slot),
        "receiver block contains a PoH entry for a different slot"
    );
    ensure!(
        expected_entry_transaction_index == executed_transaction_count,
        "receiver block entries do not cover all transactions"
    );
    ensure!(
        final_entry_hash == Some(blockhash),
        "receiver block final entry hash differs from blockhash"
    );
    Ok(ScannedBlockIdentity { slot, blockhash })
}

#[derive(Debug, Clone, Copy)]
struct ScannedTransactionInfo {
    index: u64,
}

fn scan_transaction_info(bytes: &[u8]) -> Result<ScannedTransactionInfo> {
    let mut cursor = ProtoCursor::new(bytes);
    let mut signature_seen = false;
    let mut vote_seen = false;
    let mut transaction_seen = false;
    let mut meta_seen = false;
    let mut index_seen = false;
    let mut index = 0u64;
    while let Some(field) = cursor.next()? {
        match field.number {
            1 => {
                mark_proto_field_once(
                    &mut signature_seen,
                    "SubscribeUpdateTransactionInfo.signature",
                )?;
                ensure!(
                    field
                        .bytes("SubscribeUpdateTransactionInfo.signature")?
                        .len()
                        == 64,
                    "receiver block transaction signature is not 64 bytes"
                );
            }
            2 => {
                mark_proto_field_once(&mut vote_seen, "SubscribeUpdateTransactionInfo.is_vote")?;
                field.varint("SubscribeUpdateTransactionInfo.is_vote")?;
            }
            3 => {
                mark_proto_field_once(
                    &mut transaction_seen,
                    "SubscribeUpdateTransactionInfo.transaction",
                )?;
                field.bytes("SubscribeUpdateTransactionInfo.transaction")?;
            }
            4 => {
                mark_proto_field_once(&mut meta_seen, "SubscribeUpdateTransactionInfo.meta")?;
                field.bytes("SubscribeUpdateTransactionInfo.meta")?;
            }
            5 => {
                mark_proto_field_once(&mut index_seen, "SubscribeUpdateTransactionInfo.index")?;
                index = field.varint("SubscribeUpdateTransactionInfo.index")?;
            }
            number => {
                return Err(anyhow!(
                    "receiver transaction info contains unsupported field {number}"
                ));
            }
        }
    }
    ensure!(
        signature_seen && transaction_seen && meta_seen,
        "receiver block contains an incomplete transaction"
    );
    Ok(ScannedTransactionInfo { index })
}

#[derive(Debug, Clone, Copy)]
struct ScannedEntry {
    slot: u64,
    index: u64,
    hash: [u8; 32],
    executed_transaction_count: u64,
    starting_transaction_index: u64,
}

fn scan_entry(bytes: &[u8]) -> Result<ScannedEntry> {
    let mut cursor = ProtoCursor::new(bytes);
    let mut seen = [false; 6];
    let mut slot = 0u64;
    let mut index = 0u64;
    let mut hash = None;
    let mut executed_transaction_count = 0u64;
    let mut starting_transaction_index = 0u64;
    while let Some(field) = cursor.next()? {
        ensure!(
            (1..=6).contains(&field.number),
            "receiver PoH entry contains unsupported field {}",
            field.number
        );
        let seen_index = usize::try_from(field.number - 1).expect("entry field index fits usize");
        mark_proto_field_once(&mut seen[seen_index], "SubscribeUpdateEntry field")?;
        match field.number {
            1 => slot = field.varint("SubscribeUpdateEntry.slot")?,
            2 => index = field.varint("SubscribeUpdateEntry.index")?,
            3 => {
                field.varint("SubscribeUpdateEntry.num_hashes")?;
            }
            4 => {
                let bytes = field.bytes("SubscribeUpdateEntry.hash")?;
                ensure!(
                    bytes.len() == 32,
                    "receiver block PoH entry hash is not 32 bytes"
                );
                hash = Some(bytes.try_into().expect("validated entry hash length"));
            }
            5 => {
                executed_transaction_count =
                    field.varint("SubscribeUpdateEntry.executed_transaction_count")?;
                ensure!(
                    executed_transaction_count <= u64::from(u32::MAX),
                    "receiver PoH entry transaction count exceeds u32::MAX"
                );
            }
            6 => {
                starting_transaction_index =
                    field.varint("SubscribeUpdateEntry.starting_transaction_index")?;
            }
            _ => unreachable!("entry field range was checked"),
        }
    }
    Ok(ScannedEntry {
        slot,
        index,
        hash: hash.context("receiver block PoH entry is missing its hash")?,
        executed_transaction_count,
        starting_transaction_index,
    })
}

fn decode_proto_base58_hash(bytes: &[u8], label: &str) -> Result<[u8; 32]> {
    let encoded =
        std::str::from_utf8(bytes).with_context(|| format!("receiver {label} is not UTF-8"))?;
    let mut hash = [0u8; 32];
    five8::decode_32(encoded, &mut hash)
        .map_err(|error| anyhow!("decode receiver {label}: {error:?}"))?;
    Ok(hash)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredProgressFrame {
    #[serde(default)]
    checkpoint: bool,
    stream: ReplicationStreamId,
    offer: ReplicationOffer,
    raw_protobuf_sha256: [u8; 32],
    uncompressed_len: u64,
    previous_chain_digest: ContentDigest,
    rolling_chain_digest: ContentDigest,
    spool_location: SpoolLocation,
    durable_lsn: u64,
}

impl StoredProgressFrame {
    fn matches_validated(
        &self,
        validated: &ValidatedRawRecord,
        spool_location: SpoolLocation,
    ) -> bool {
        self.offer == validated.offer
            && self.raw_protobuf_sha256 == validated.raw_protobuf_sha256
            && self.uncompressed_len == validated.uncompressed_len
            && self.spool_location == spool_location
    }
}

#[derive(Debug)]
struct RecoveredProgressWal {
    valid_len: u64,
    frames_in_file: usize,
    latest: Option<StoredProgressFrame>,
    recent: VecDeque<StoredProgressFrame>,
}

/// Crash-safe cursor journal for the receiver's exact durable prefix.
#[derive(Debug)]
pub struct ReceiverProgressWal {
    path: PathBuf,
    writer: BufWriter<File>,
    stream: ReplicationStreamId,
    recent_capacity: usize,
    frames_in_file: usize,
    latest: Option<StoredProgressFrame>,
    recent: VecDeque<StoredProgressFrame>,
    poisoned: bool,
}

impl ReceiverProgressWal {
    fn open(
        path: impl AsRef<Path>,
        stream: ReplicationStreamId,
        recent_capacity: usize,
    ) -> Result<Self> {
        ensure!(
            recent_capacity > 0,
            "receiver progress retry window must be non-zero"
        );
        let path = path.as_ref().to_path_buf();
        let (mut file, created) = open_progress_file(&path)?;
        try_lock_progress_file(&file, &path)?;
        remove_stale_progress_compaction_file(&path)?;
        initialize_progress_wal(&mut file, &path)?;
        if created {
            sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))?;
        }
        let file_len = file.metadata()?.len();
        let recovered = recover_progress_wal(&mut file, &path, &stream, recent_capacity)?;
        if recovered.valid_len != file_len {
            file.set_len(recovered.valid_len)
                .with_context(|| format!("truncate receiver progress WAL {}", path.display()))?;
            file.sync_data().with_context(|| {
                format!("sync recovered receiver progress WAL {}", path.display())
            })?;
        }
        file.seek(std::io::SeekFrom::End(0))
            .with_context(|| format!("seek receiver progress WAL {}", path.display()))?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
            stream,
            recent_capacity,
            frames_in_file: recovered.frames_in_file,
            latest: recovered.latest,
            recent: recovered.recent,
            poisoned: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    fn latest(&self) -> Option<&StoredProgressFrame> {
        self.latest.as_ref()
    }

    fn recent(&self, sequence: u64) -> Option<&StoredProgressFrame> {
        self.recent
            .iter()
            .find(|frame| frame.offer.record.sequence == sequence)
    }

    fn file_len(&self) -> Result<u64> {
        self.writer
            .get_ref()
            .metadata()
            .with_context(|| format!("inspect receiver progress WAL {}", self.path.display()))
            .map(|metadata| metadata.len())
    }

    fn compaction_threshold(&self) -> Result<usize> {
        self.recent_capacity
            .checked_mul(2)
            .context("receiver progress compaction threshold overflow")
    }

    fn will_compact_after(&self, additional_frames: usize) -> Result<bool> {
        Ok(self
            .frames_in_file
            .checked_add(additional_frames)
            .context("receiver progress frame count overflow")?
            > self.compaction_threshold()?)
    }

    fn append_and_sync(
        &mut self,
        validated: &ValidatedRawRecord,
        previous_chain_digest: ContentDigest,
        rolling_chain_digest: ContentDigest,
        durable: &DurableSpoolRecord,
    ) -> Result<()> {
        ensure!(
            !self.poisoned,
            "receiver progress WAL writer is poisoned; reopen receiver to recover"
        );
        let durable_lsn = self.latest.as_ref().map_or(Ok(1), |latest| {
            latest
                .durable_lsn
                .checked_add(1)
                .context("receiver durable LSN exhausted")
        })?;
        let frame = StoredProgressFrame {
            checkpoint: false,
            stream: self.stream.clone(),
            offer: validated.offer.clone(),
            raw_protobuf_sha256: validated.raw_protobuf_sha256,
            uncompressed_len: validated.uncompressed_len,
            previous_chain_digest,
            rolling_chain_digest,
            spool_location: durable.location(),
            durable_lsn,
        };
        validate_progress_transition(self.latest.as_ref(), &frame, &self.stream)?;
        ensure!(
            durable.metadata() == &validated.metadata,
            "receiver progress metadata differs from durable spool record"
        );

        self.poisoned = true;
        write_progress_frame(&mut self.writer, &frame, &self.path)?;
        self.writer
            .flush()
            .with_context(|| format!("flush receiver progress WAL {}", self.path.display()))?;
        self.writer
            .get_ref()
            .sync_data()
            .with_context(|| format!("sync receiver progress WAL {}", self.path.display()))?;
        self.frames_in_file = self
            .frames_in_file
            .checked_add(1)
            .context("receiver progress frame count overflow")?;
        self.record(frame);
        if self.frames_in_file > self.compaction_threshold()? {
            self.compact_and_sync()?;
        }
        self.poisoned = false;
        Ok(())
    }

    fn compact_and_sync(&mut self) -> Result<()> {
        ensure!(
            !self.recent.is_empty(),
            "receiver progress compaction requires a durable frame"
        );
        let mut compacted = self.recent.clone();
        for (index, frame) in compacted.iter_mut().enumerate() {
            frame.checkpoint = index == 0;
        }
        let mut previous = None;
        for frame in &compacted {
            validate_progress_transition(previous, frame, &self.stream)?;
            previous = Some(frame);
        }

        let temporary_path = progress_compaction_temp_path(&self.path);
        let mut temporary_file =
            open_progress_descriptor(&temporary_path, true).with_context(|| {
                format!(
                    "create receiver progress compaction file {}",
                    temporary_path.display()
                )
            })?;
        try_lock_progress_file(&temporary_file, &temporary_path)?;
        temporary_file
            .write_all(PROGRESS_WAL_MAGIC)
            .with_context(|| {
                format!(
                    "write receiver progress compaction header {}",
                    temporary_path.display()
                )
            })?;
        let mut temporary_writer = BufWriter::new(temporary_file);
        for frame in &compacted {
            write_progress_frame(&mut temporary_writer, frame, &temporary_path)?;
        }
        temporary_writer.flush().with_context(|| {
            format!(
                "flush receiver progress compaction {}",
                temporary_path.display()
            )
        })?;
        temporary_writer.get_ref().sync_data().with_context(|| {
            format!(
                "sync receiver progress compaction {}",
                temporary_path.display()
            )
        })?;
        fs::rename(&temporary_path, &self.path).with_context(|| {
            format!(
                "install receiver progress compaction {} as {}",
                temporary_path.display(),
                self.path.display()
            )
        })?;
        sync_directory(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
        self.writer = temporary_writer;
        self.frames_in_file = compacted.len();
        self.latest = compacted.back().cloned();
        self.recent = compacted;
        Ok(())
    }

    fn record(&mut self, frame: StoredProgressFrame) {
        self.latest = Some(frame.clone());
        self.recent.push_back(frame);
        while self.recent.len() > self.recent_capacity {
            self.recent.pop_front();
        }
    }
}

/// Recover a stable, read-only snapshot of a live receiver progress WAL.
///
/// The descriptor is opened without a writer lock and without write permissions. Concurrent
/// append is safe because only checksum-and-commit-complete frames are considered. Concurrent
/// compaction is safe because the open descriptor continues to reference either the complete old
/// inode or the complete atomically installed replacement. Bytes appended after this read reaches
/// EOF are reported as unobserved tail and never enter the returned cursor.
pub fn read_receiver_durable_progress(
    path: impl AsRef<Path>,
    stream: &ReplicationStreamId,
) -> Result<Option<ReceiverDurableProgress>> {
    let path = path.as_ref();
    let mut file = open_progress_descriptor_read_only(path)?;
    let recovered = recover_progress_wal(&mut file, path, stream, 1)
        .context("recover read-only receiver progress snapshot")?;
    let observed_len = file
        .metadata()
        .with_context(|| format!("inspect receiver progress WAL {}", path.display()))?
        .len();
    let unobserved_tail_bytes = observed_len.saturating_sub(recovered.valid_len);
    Ok(recovered.latest.map(|latest| ReceiverDurableProgress {
        stream: latest.stream,
        through_sequence: latest.offer.record.sequence,
        through_content_digest: latest.offer.content_digest,
        rolling_chain_digest: latest.rolling_chain_digest,
        durable_lsn: latest.durable_lsn,
        spool_location: latest.spool_location,
        unobserved_tail_bytes,
    }))
}

fn validate_progress_transition(
    previous: Option<&StoredProgressFrame>,
    frame: &StoredProgressFrame,
    stream: &ReplicationStreamId,
) -> Result<()> {
    ensure!(
        frame.stream == *stream,
        "receiver progress frame belongs to a different stream"
    );
    validate_offer_identity(&frame.offer, stream)?;
    ensure!(
        frame.offer.record.sequence.checked_add(1) == Some(frame.durable_lsn),
        "receiver progress sequence and durable LSN disagree"
    );
    match previous {
        None => {
            if !frame.checkpoint {
                ensure!(
                    frame.offer.record.sequence == 0,
                    "receiver progress must start at sequence 0 or a checkpoint"
                );
                ensure!(
                    frame.previous_chain_digest
                        == cumulative_chain_seed(stream)
                            .map_err(|error| anyhow!("build progress chain seed: {error:?}"))?,
                    "receiver first progress frame has wrong chain seed"
                );
            }
        }
        Some(previous) => {
            ensure!(
                !frame.checkpoint,
                "receiver progress checkpoint may appear only as the first frame"
            );
            ensure!(
                previous
                    .offer
                    .record
                    .sequence
                    .checked_add(1)
                    .is_some_and(|next| frame.offer.record.sequence == next),
                "receiver progress sequence is not contiguous"
            );
            ensure!(
                previous
                    .durable_lsn
                    .checked_add(1)
                    .is_some_and(|next| frame.durable_lsn == next),
                "receiver progress durable LSN is not contiguous"
            );
            ensure!(
                frame.previous_chain_digest == previous.rolling_chain_digest,
                "receiver progress rolling chain is discontinuous"
            );
        }
    }
    ensure!(
        frame.rolling_chain_digest
            == cumulative_chain_next(stream, frame.previous_chain_digest, &frame.offer)
                .map_err(|error| anyhow!("verify progress cumulative chain: {error:?}"))?,
        "receiver progress rolling-chain digest mismatch"
    );
    Ok(())
}

fn recover_progress_wal(
    file: &mut File,
    path: &Path,
    stream: &ReplicationStreamId,
    recent_capacity: usize,
) -> Result<RecoveredProgressWal> {
    file.seek(std::io::SeekFrom::Start(0))?;
    let mut reader = BufReader::new(file);
    let mut wal_magic = [0u8; 8];
    reader
        .read_exact(&mut wal_magic)
        .with_context(|| format!("read receiver progress WAL header {}", path.display()))?;
    ensure!(
        &wal_magic == PROGRESS_WAL_MAGIC,
        "invalid receiver progress WAL header in {}",
        path.display()
    );
    let mut valid_len = PROGRESS_WAL_HEADER_LEN;
    let mut frames_in_file = 0usize;
    let mut latest: Option<StoredProgressFrame> = None;
    let mut recent = VecDeque::with_capacity(recent_capacity);
    loop {
        let frame_offset = valid_len;
        let mut frame_magic = [0u8; 4];
        if !read_progress_exact_or_tail(&mut reader, &mut frame_magic, path)? {
            break;
        }
        ensure!(
            &frame_magic == PROGRESS_FRAME_MAGIC,
            "corrupt receiver progress frame magic at {frame_offset} in {}",
            path.display()
        );
        let mut version_bytes = [0u8; 2];
        if !read_progress_exact_or_tail(&mut reader, &mut version_bytes, path)? {
            break;
        }
        let mut length_bytes = [0u8; 4];
        if !read_progress_exact_or_tail(&mut reader, &mut length_bytes, path)? {
            break;
        }
        let mut expected_header_crc_bytes = [0u8; 4];
        if !read_progress_exact_or_tail(&mut reader, &mut expected_header_crc_bytes, path)? {
            break;
        }
        let mut header_crc = ReceiverCrc32c::new();
        header_crc.update(&frame_magic);
        header_crc.update(&version_bytes);
        header_crc.update(&length_bytes);
        ensure!(
            header_crc.finish() == u32::from_le_bytes(expected_header_crc_bytes),
            "receiver progress header checksum mismatch at {frame_offset} in {}",
            path.display()
        );
        ensure!(
            u16::from_le_bytes(version_bytes) == PROGRESS_FRAME_VERSION,
            "unsupported receiver progress frame version at {frame_offset}"
        );
        let payload_len = u32::from_le_bytes(length_bytes) as usize;
        ensure!(
            payload_len <= MAX_PROGRESS_FRAME_BYTES,
            "receiver progress frame length exceeds maximum at {frame_offset}"
        );
        let mut payload = vec![0u8; payload_len];
        if !read_progress_exact_or_tail(&mut reader, &mut payload, path)? {
            break;
        }
        let mut expected_payload_crc_bytes = [0u8; 4];
        if !read_progress_exact_or_tail(&mut reader, &mut expected_payload_crc_bytes, path)? {
            break;
        }
        let mut commit_magic = [0u8; 4];
        if !read_progress_exact_or_tail(&mut reader, &mut commit_magic, path)? {
            break;
        }
        let mut payload_crc = ReceiverCrc32c::new();
        payload_crc.update(&payload);
        ensure!(
            payload_crc.finish() == u32::from_le_bytes(expected_payload_crc_bytes),
            "receiver progress payload checksum mismatch at {frame_offset} in {}",
            path.display()
        );
        ensure!(
            &commit_magic == PROGRESS_COMMIT_MAGIC,
            "receiver progress frame lacks commit marker at {frame_offset}"
        );
        let frame: StoredProgressFrame = serde_json::from_slice(&payload).with_context(|| {
            format!(
                "decode committed receiver progress frame at {frame_offset} in {}",
                path.display()
            )
        })?;
        validate_progress_transition(latest.as_ref(), &frame, stream).with_context(|| {
            format!(
                "validate committed receiver progress frame at {frame_offset} in {}",
                path.display()
            )
        })?;
        latest = Some(frame.clone());
        frames_in_file = frames_in_file
            .checked_add(1)
            .context("receiver progress frame count overflow")?;
        recent.push_back(frame);
        while recent.len() > recent_capacity {
            recent.pop_front();
        }
        valid_len = valid_len
            .checked_add(PROGRESS_FRAME_FIXED_LEN)
            .and_then(|length| length.checked_add(payload_len as u64))
            .and_then(|length| length.checked_add(PROGRESS_FRAME_TRAILER_LEN))
            .context("receiver progress WAL length overflow")?;
    }
    Ok(RecoveredProgressWal {
        valid_len,
        frames_in_file,
        latest,
        recent,
    })
}

fn write_progress_frame<W: Write>(
    writer: &mut W,
    frame: &StoredProgressFrame,
    path: &Path,
) -> Result<u64> {
    let payload = serde_json::to_vec(frame).context("encode receiver progress frame")?;
    ensure!(
        payload.len() <= MAX_PROGRESS_FRAME_BYTES,
        "receiver progress frame exceeds {} bytes",
        MAX_PROGRESS_FRAME_BYTES
    );
    let payload_len = u32::try_from(payload.len()).context("receiver progress frame too large")?;
    let version_bytes = PROGRESS_FRAME_VERSION.to_le_bytes();
    let length_bytes = payload_len.to_le_bytes();
    let mut header_crc = ReceiverCrc32c::new();
    header_crc.update(PROGRESS_FRAME_MAGIC);
    header_crc.update(&version_bytes);
    header_crc.update(&length_bytes);
    let mut payload_crc = ReceiverCrc32c::new();
    payload_crc.update(&payload);

    writer
        .write_all(PROGRESS_FRAME_MAGIC)
        .with_context(|| format!("write receiver progress frame {}", path.display()))?;
    writer.write_all(&version_bytes)?;
    writer.write_all(&length_bytes)?;
    writer.write_all(&header_crc.finish().to_le_bytes())?;
    writer.write_all(&payload)?;
    writer.write_all(&payload_crc.finish().to_le_bytes())?;
    writer.write_all(PROGRESS_COMMIT_MAGIC)?;
    PROGRESS_FRAME_FIXED_LEN
        .checked_add(payload.len() as u64)
        .and_then(|length| length.checked_add(PROGRESS_FRAME_TRAILER_LEN))
        .context("receiver progress frame length overflow")
}

fn progress_compaction_temp_path(path: &Path) -> PathBuf {
    let mut temporary = path.as_os_str().to_os_string();
    temporary.push(PROGRESS_COMPACTION_TEMP_SUFFIX);
    PathBuf::from(temporary)
}

fn remove_stale_progress_compaction_file(path: &Path) -> Result<()> {
    let temporary_path = progress_compaction_temp_path(path);
    let metadata = match fs::symlink_metadata(&temporary_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "inspect stale receiver progress compaction {}",
                    temporary_path.display()
                )
            });
        }
    };
    ensure!(
        !metadata.file_type().is_symlink() && metadata.is_file(),
        "stale receiver progress compaction is not a regular file: {}",
        temporary_path.display()
    );
    fs::remove_file(&temporary_path).with_context(|| {
        format!(
            "remove stale receiver progress compaction {}",
            temporary_path.display()
        )
    })?;
    sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
}

fn initialize_progress_wal(file: &mut File, path: &Path) -> Result<()> {
    let length = file.metadata()?.len();
    if length >= PROGRESS_WAL_HEADER_LEN {
        return Ok(());
    }
    let mut existing = vec![0u8; length as usize];
    file.seek(std::io::SeekFrom::Start(0))?;
    file.read_exact(&mut existing)?;
    ensure!(
        PROGRESS_WAL_MAGIC.starts_with(&existing),
        "refusing non-progress file with invalid partial header: {}",
        path.display()
    );
    file.set_len(0)?;
    file.seek(std::io::SeekFrom::Start(0))?;
    file.write_all(PROGRESS_WAL_MAGIC)?;
    file.sync_data()
        .with_context(|| format!("sync receiver progress WAL header {}", path.display()))
}

fn read_progress_exact_or_tail<R: Read>(
    reader: &mut R,
    output: &mut [u8],
    path: &Path,
) -> Result<bool> {
    match reader.read_exact(output) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("read receiver progress WAL {}", path.display()))
        }
    }
}

fn open_progress_file(path: &Path) -> Result<(File, bool)> {
    match open_progress_descriptor(path, true) {
        Ok(file) => Ok((file, true)),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok((
            open_progress_descriptor(path, false)
                .with_context(|| format!("open receiver progress WAL {}", path.display()))?,
            false,
        )),
        Err(error) => {
            Err(error).with_context(|| format!("create receiver progress WAL {}", path.display()))
        }
    }
}

fn open_progress_descriptor(path: &Path, create_new: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create_new {
        options.create_new(true);
    }
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other(
            "receiver progress WAL is not a regular file",
        ));
    }
    Ok(file)
}

fn open_progress_descriptor_read_only(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .with_context(|| format!("open receiver progress WAL read-only {}", path.display()))?;
    ensure!(
        file.metadata()?.is_file(),
        "receiver progress WAL is not a regular file: {}",
        path.display()
    );
    Ok(file)
}

#[cfg(unix)]
fn try_lock_progress_file(file: &File, path: &Path) -> Result<()> {
    // SAFETY: this descriptor remains owned by ReceiverProgressWal for the lock lifetime.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
            .with_context(|| format!("lock receiver progress WAL {}", path.display()))
    }
}

#[cfg(not(unix))]
fn try_lock_progress_file(file: &File, path: &Path) -> Result<()> {
    file.try_lock()
        .with_context(|| format!("lock receiver progress WAL {}", path.display()))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open receiver progress directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync receiver progress directory {}", path.display()))
}

/// Create the receiver root without leaving an acknowledged journal reachable only through an
/// unsynced directory entry. Every missing component is installed one at a time and its parent is
/// synced before the receiver can append data. Existing roots are revalidated and their immediate
/// parent is synced as a conservative repair for a creator that may have crashed before fsync.
pub(crate) fn ensure_receiver_spool_root_durable(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute() && path != Path::new("/"),
        "receiver spool root must be an absolute non-root directory"
    );

    let mut missing = Vec::new();
    let mut cursor = path;
    loop {
        match fs::symlink_metadata(cursor) {
            Ok(metadata) => {
                ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "receiver spool ancestor is not a real directory: {}",
                    cursor.display()
                );
                break;
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                missing.push(cursor.to_path_buf());
                cursor = cursor.parent().with_context(|| {
                    format!(
                        "receiver spool root has no existing ancestor: {}",
                        path.display()
                    )
                })?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspect receiver spool ancestor {}", cursor.display())
                });
            }
        }
    }

    for directory in missing.iter().rev() {
        match create_receiver_directory(directory) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("create receiver spool directory {}", directory.display())
                });
            }
        }
        validate_secure_receiver_root(directory, "receiver spool directory")?;
        sync_directory(directory)?;
        if let Some(parent) = directory.parent() {
            sync_directory(parent)?;
        }
    }

    validate_secure_receiver_root(path, "receiver spool root")?;
    sync_directory(path)?;
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn create_receiver_directory(path: &Path) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(path)
}

fn validate_secure_receiver_root(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "{label} must be a real directory: {}",
        path.display()
    );
    #[cfg(unix)]
    {
        ensure!(
            metadata.mode() & 0o022 == 0,
            "{label} must not be group- or world-writable: {}",
            path.display()
        );
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            metadata.uid() == effective_uid || metadata.uid() == 0,
            "{label} must be owned by this user or root: {}",
            path.display()
        );
    }
    Ok(())
}

fn is_stable_receiver_id(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric())
        && value.len() <= 64
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}

fn open_receiver_capacity_lock(spool_root: &Path) -> Result<File> {
    let path = spool_root.join(RECEIVER_CAPACITY_LOCK_FILE);
    let (file, created) = match open_progress_descriptor(&path, true) {
        Ok(file) => (file, true),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => (
            open_progress_descriptor(&path, false)
                .with_context(|| format!("open receiver capacity lock {}", path.display()))?,
            false,
        ),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("create receiver capacity lock {}", path.display()));
        }
    };
    if created {
        sync_directory(spool_root)?;
    }
    Ok(file)
}

/// Serialize a read-only local consumer's filesystem admission with receiver batches.
///
/// The lock file must already have been durably created by the receiver. Opening it read-only
/// keeps callers such as the WAL bridge compatible with a read-only receiver bind mount; `flock`
/// coordinates the descriptor without modifying the canonical receiver tree.
pub(crate) fn with_receiver_capacity_lock<T>(
    spool_root: &Path,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let path = spool_root.join(RECEIVER_CAPACITY_LOCK_FILE);
    let metadata = fs::symlink_metadata(&path)
        .with_context(|| format!("inspect receiver capacity lock {}", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "receiver capacity lock must be a regular non-symlink file: {}",
        path.display()
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(&path)
        .with_context(|| format!("open receiver capacity lock read-only {}", path.display()))?;
    ensure!(
        file.metadata()?.is_file(),
        "opened receiver capacity lock is not a regular file: {}",
        path.display()
    );
    let _guard = ReceiverCapacityGuard::acquire(file)?;
    operation()
}

fn admit_receiver_open(config: &RawReceiverConfig) -> Result<()> {
    let journal_path = receiver_journal_path(config);
    let projected_growth = match fs::symlink_metadata(&journal_path) {
        Ok(metadata) => {
            ensure!(
                !metadata.file_type().is_symlink() && metadata.is_dir(),
                "receiver journal path is not a directory: {}",
                journal_path.display()
            );
            // A pre-existing spool may still need individual durable journal files created.
            [
                journal_path.join("writer.lock"),
                journal_path.join("segment-00000000000000000000.wal"),
                journal_path.join(RECEIVER_PROGRESS_WAL_FILE),
            ]
            .into_iter()
            .try_fold(0u64, |projection, path| {
                projection
                    .checked_add(project_missing_regular_file(&path)?)
                    .context("receiver journal-open projection overflow")
            })?
        }
        Err(error) if error.kind() == ErrorKind::NotFound => NEW_JOURNAL_PROJECTION_BYTES,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect receiver journal {}", journal_path.display()));
        }
    };
    let current_bytes = receiver_spool_root_bytes(&config.spool_root)?;
    ensure!(
        current_bytes
            .checked_add(projected_growth)
            .is_some_and(|projected| projected <= config.max_spool_bytes),
        "receiver spool capacity cannot admit journal open: current {current_bytes}, projected growth {projected_growth}, maximum {}",
        config.max_spool_bytes
    );
    let available_bytes = filesystem_available_bytes(&config.spool_root)?;
    ensure!(
        config
            .reserve_free_bytes
            .checked_add(projected_growth)
            .is_some_and(|required| available_bytes >= required),
        "receiver filesystem reserve cannot admit journal open: available {available_bytes}, projected growth {projected_growth}, reserve {}",
        config.reserve_free_bytes
    );
    Ok(())
}

fn project_missing_regular_file(path: &Path) -> Result<u64> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                !metadata.file_type().is_symlink() && metadata.is_file(),
                "receiver journal entry is not a regular file: {}",
                path.display()
            );
            Ok(0)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(QUOTA_ENTRY_MIN_BYTES),
        Err(error) => {
            Err(error).with_context(|| format!("inspect receiver journal entry {}", path.display()))
        }
    }
}

fn ensure_receiver_capacity_current(config: &RawReceiverConfig) -> Result<()> {
    let current_bytes = receiver_spool_root_bytes(&config.spool_root)?;
    ensure!(
        current_bytes <= config.max_spool_bytes,
        "receiver spool root already exceeds capacity: current {current_bytes}, maximum {}",
        config.max_spool_bytes
    );
    let available_bytes = filesystem_available_bytes(&config.spool_root)?;
    ensure!(
        available_bytes >= config.reserve_free_bytes,
        "receiver filesystem is below its reserve: available {available_bytes}, reserve {}",
        config.reserve_free_bytes
    );
    Ok(())
}

fn receiver_journal_path(config: &RawReceiverConfig) -> PathBuf {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut journal_id = String::with_capacity(32);
    for byte in config.stream.journal_id {
        journal_id.push(char::from(HEX[usize::from(byte >> 4)]));
        journal_id.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    config
        .spool_root
        .join(&config.stream.cluster_id)
        .join(&config.stream.origin_node_id)
        .join(&config.stream.source_id)
        .join(journal_id)
}

struct ReceiverCapacityGuard {
    file: File,
}

impl ReceiverCapacityGuard {
    fn acquire(file: File) -> Result<Self> {
        lock_receiver_capacity_file(&file)?;
        Ok(Self { file })
    }
}

impl Drop for ReceiverCapacityGuard {
    fn drop(&mut self) {
        unlock_receiver_capacity_file(&self.file);
    }
}

#[cfg(unix)]
fn lock_receiver_capacity_file(file: &File) -> Result<()> {
    // SAFETY: `file` remains alive in ReceiverCapacityGuard until explicitly unlocked.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).context("lock receiver root capacity")
    }
}

#[cfg(not(unix))]
fn lock_receiver_capacity_file(file: &File) -> Result<()> {
    file.lock().context("lock receiver root capacity")
}

#[cfg(unix)]
fn unlock_receiver_capacity_file(file: &File) {
    // SAFETY: best-effort unlock of the descriptor owned by ReceiverCapacityGuard.
    unsafe {
        libc::flock(file.as_raw_fd(), libc::LOCK_UN);
    }
}

#[cfg(not(unix))]
fn unlock_receiver_capacity_file(file: &File) {
    let _ = file.unlock();
}

fn receiver_spool_root_bytes(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)
        .with_context(|| format!("list receiver spool root {}", path.display()))?
    {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path).with_context(|| {
            format!("inspect receiver spool root entry {}", entry_path.display())
        })?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "receiver spool root contains a symbolic link: {}",
            entry_path.display()
        );
        if metadata.is_dir() {
            let nested = receiver_spool_root_bytes(&entry_path)?;
            total = total
                .checked_add(QUOTA_ENTRY_MIN_BYTES)
                .and_then(|total| total.checked_add(nested))
                .context("receiver spool root byte count overflow")?;
        } else {
            ensure!(
                metadata.is_file(),
                "receiver spool root contains a non-regular entry: {}",
                entry_path.display()
            );
            total = total
                .checked_add(metadata.len().max(QUOTA_ENTRY_MIN_BYTES))
                .context("receiver spool root byte count overflow")?;
        }
    }
    Ok(total)
}

#[cfg(unix)]
fn filesystem_available_bytes(path: &Path) -> Result<u64> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let path = CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("receiver filesystem path contains NUL: {}", path.display()))?;
    // SAFETY: stat is writable storage and path remains a valid NUL-terminated string.
    let mut stat = unsafe { std::mem::zeroed::<libc::statvfs>() };
    let result = unsafe { libc::statvfs(path.as_ptr(), &mut stat) };
    if result != 0 {
        return Err(io::Error::last_os_error()).context("read receiver filesystem free space");
    }
    (stat.f_bavail as u64)
        .checked_mul(stat.f_frsize as u64)
        .context("receiver filesystem available byte count overflow")
}

#[cfg(not(unix))]
fn filesystem_available_bytes(_path: &Path) -> Result<u64> {
    Ok(u64::MAX)
}

#[derive(Debug, Clone, Copy)]
struct ReceiverCrc32c(u32);

impl ReceiverCrc32c {
    fn new() -> Self {
        Self(!0)
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = (self.0 & 1).wrapping_neg();
                self.0 = (self.0 >> 1) ^ (0x82f6_3b78 & mask);
            }
        }
    }

    fn finish(self) -> u32 {
        !self.0
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{SeekFrom, Write},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::ingest::ObservationId;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(0);

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after Unix epoch")
                .as_nanos();
            let counter = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "blockzilla-receiver-{label}-{}-{nonce}-{counter}",
                std::process::id()
            )))
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn stream() -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "mainnet-beta".to_owned(),
            origin_node_id: "hetzner-replica".to_owned(),
            source_id: "yellowstone".to_owned(),
            journal_id: [7; 16],
        }
    }

    fn config(root: &TestRoot, retry_window: usize) -> RawReceiverConfig {
        RawReceiverConfig {
            spool_root: root.0.clone(),
            stream: stream(),
            primary_id: "blockzilla-primary".to_owned(),
            primary_term: 4,
            spool_options: SpoolOptions {
                segment_target_bytes: 64 * 1024,
                max_record_bytes: 2 * 1024 * 1024,
            },
            max_spool_bytes: 64 * 1024 * 1024,
            reserve_free_bytes: 0,
            limits: RawReceiverLimits {
                max_batch_records: retry_window,
                max_batch_compressed_bytes: 4 * 1024 * 1024,
                max_batch_uncompressed_bytes: 8 * 1024 * 1024,
                max_compressed_record_bytes: 2 * 1024 * 1024,
                max_uncompressed_record_bytes: 4 * 1024 * 1024,
            },
        }
    }

    fn push_varint(output: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn push_key(output: &mut Vec<u8>, field: u32, wire_type: u8) {
        push_varint(output, (u64::from(field) << 3) | u64::from(wire_type));
    }

    fn push_varint_field(output: &mut Vec<u8>, field: u32, value: u64) {
        push_key(output, field, 0);
        push_varint(output, value);
    }

    fn push_bytes_field(output: &mut Vec<u8>, field: u32, value: &[u8]) {
        push_key(output, field, 2);
        push_varint(output, value.len() as u64);
        output.extend_from_slice(value);
    }

    fn encode_hash(hash: &[u8; 32]) -> String {
        let mut output = [0u8; five8::BASE58_ENCODED_32_MAX_LEN];
        let length = five8::encode_32(hash, &mut output) as usize;
        String::from_utf8(output[..length].to_vec()).expect("base58 is ASCII")
    }

    fn raw_block(slot: u64, blockhash: [u8; 32]) -> Vec<u8> {
        let parent_hash = [blockhash[0].wrapping_sub(1); 32];
        let mut entry = Vec::new();
        push_varint_field(&mut entry, 1, slot);
        push_varint_field(&mut entry, 2, 0);
        push_varint_field(&mut entry, 3, 1);
        push_bytes_field(&mut entry, 4, &blockhash);
        push_varint_field(&mut entry, 5, 0);
        push_varint_field(&mut entry, 6, 0);

        let mut block = Vec::new();
        push_varint_field(&mut block, 1, slot);
        push_bytes_field(&mut block, 2, encode_hash(&blockhash).as_bytes());
        push_varint_field(&mut block, 7, slot.saturating_sub(1));
        push_bytes_field(&mut block, 8, encode_hash(&parent_hash).as_bytes());
        push_varint_field(&mut block, 9, 0);
        push_varint_field(&mut block, 10, 0);
        push_varint_field(&mut block, 12, 1);
        push_bytes_field(&mut block, 13, &entry);

        let mut update = Vec::new();
        push_bytes_field(&mut update, 5, &block);
        update
    }

    fn record(sequence: u64, slot: u64, hash_byte: u8) -> RawReplicationRecord {
        record_from_raw(
            sequence,
            slot,
            [hash_byte; 32],
            raw_block(slot, [hash_byte; 32]),
        )
    }

    fn record_from_raw(
        sequence: u64,
        slot: u64,
        blockhash: [u8; 32],
        raw: Vec<u8>,
    ) -> RawReplicationRecord {
        let compressed_payload = zstd::bulk::compress(&raw, 1).expect("compress test protobuf");
        let stream = stream();
        let logical_key = LogicalKey::Block { slot, blockhash };
        let content_digest = compute_content_digest(
            &stream.cluster_id,
            &logical_key,
            RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
            &compressed_payload,
        );
        RawReplicationRecord {
            offer: ReplicationOffer {
                protocol_version: REPLICATION_PROTOCOL_VERSION,
                cluster_id: stream.cluster_id.clone(),
                record: ObservationId {
                    origin_node_id: stream.origin_node_id,
                    journal_id: stream.journal_id,
                    sequence,
                },
                source_id: stream.source_id,
                logical_key,
                content_digest,
                payload_len: compressed_payload.len() as u64,
                payload_format_version: RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
                commitment: CommitmentEvidence::Confirmed,
            },
            compressed_payload,
            raw_protobuf_sha256: Sha256::digest(&raw).into(),
            uncompressed_len: raw.len() as u64,
        }
    }

    #[test]
    fn stateless_new_stream_validation_never_creates_storage() {
        let root = TestRoot::new("stateless-validation");
        let config = config(&root, 4);
        let valid = record(0, 100, 9);
        validate_raw_replication_batch(
            std::slice::from_ref(&valid),
            &config.stream,
            config.limits,
            true,
        )
        .expect("validate complete initial record");
        assert!(!root.0.exists());

        let non_initial = record(1, 101, 10);
        assert!(
            validate_raw_replication_batch(
                std::slice::from_ref(&non_initial),
                &config.stream,
                config.limits,
                true,
            )
            .is_err()
        );
        let mut corrupt = valid;
        corrupt.compressed_payload[0] ^= 0x80;
        assert!(
            validate_raw_replication_batch(
                std::slice::from_ref(&corrupt),
                &config.stream,
                config.limits,
                true,
            )
            .is_err()
        );
        assert!(!root.0.exists());
    }

    #[cfg(unix)]
    #[test]
    fn receiver_rejects_a_world_writable_existing_root() {
        let root = TestRoot::new("unsafe-root");
        fs::create_dir(&root.0).unwrap();
        fs::set_permissions(&root.0, fs::Permissions::from_mode(0o777)).unwrap();
        assert!(BlockzillaRawReceiver::open(config(&root, 4)).is_err());
        assert!(!root.0.join(RECEIVER_CAPACITY_LOCK_FILE).exists());
    }

    fn spool_segment_bytes(journal: &Path) -> u64 {
        fs::read_dir(journal)
            .expect("read journal")
            .map(|entry| entry.expect("journal entry"))
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("segment-") && name.ends_with(".wal"))
            })
            .map(|entry| entry.metadata().expect("segment metadata").len())
            .sum()
    }

    #[derive(Debug)]
    struct FailAt {
        point: ReceiverFaultPoint,
        sequence: u64,
    }

    impl ReceiverFaultInjector for FailAt {
        fn check(&mut self, point: ReceiverFaultPoint, sequence: u64) -> Result<()> {
            if point == self.point && sequence == self.sequence {
                Err(anyhow!("injected receiver fault"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn successful_ingest_returns_only_an_unsigned_durable_ack() {
        let root = TestRoot::new("success");
        let mut receiver = BlockzillaRawReceiver::open(config(&root, 8)).unwrap();
        assert!(receiver.get_ack().is_none());
        let input = record(0, 1002, 9);
        let ack = receiver.push_batch(vec![input.clone()]).unwrap();
        assert_eq!(ack.through_sequence, 0);
        assert_eq!(ack.through_content_digest, input.offer.content_digest);
        assert_eq!(ack.durable_lsn, 1);
        assert!(ack.signing_key_id.is_empty());
        assert!(ack.signature.is_empty());
        assert_eq!(receiver.get_ack(), Some(ack));
    }

    #[test]
    fn injected_durability_boundaries_recover_without_false_ack() {
        let root = TestRoot::new("fault-before");
        let configuration = config(&root, 8);
        let mut receiver = BlockzillaRawReceiver::open(configuration.clone()).unwrap();
        let before = spool_segment_bytes(receiver.spool_journal_dir());
        let error = receiver.push_batch_with_faults(
            vec![record(0, 1, 1)],
            &mut FailAt {
                point: ReceiverFaultPoint::AfterValidationBeforeSpool,
                sequence: 0,
            },
        );
        assert!(error.is_err());
        assert!(!receiver.is_poisoned());
        assert!(receiver.get_ack().is_none());
        assert_eq!(spool_segment_bytes(receiver.spool_journal_dir()), before);

        let error = receiver.push_batch_with_faults(
            vec![record(0, 1, 1)],
            &mut FailAt {
                point: ReceiverFaultPoint::AfterSpoolSyncBeforeProgress,
                sequence: 0,
            },
        );
        assert!(error.is_err());
        assert!(receiver.is_poisoned());
        assert!(receiver.get_ack().is_none());
        drop(receiver);

        let mut recovered = BlockzillaRawReceiver::open(configuration.clone()).unwrap();
        assert_eq!(recovered.get_ack().unwrap().through_sequence, 0);
        let error = recovered.push_batch_with_faults(
            vec![record(1, 2, 2)],
            &mut FailAt {
                point: ReceiverFaultPoint::AfterProgressSync,
                sequence: 1,
            },
        );
        assert!(error.is_err());
        assert_eq!(recovered.get_ack().unwrap().through_sequence, 1);
        drop(recovered);
        assert_eq!(
            BlockzillaRawReceiver::open(configuration)
                .unwrap()
                .get_ack()
                .unwrap()
                .through_sequence,
            1
        );
    }

    #[test]
    fn exact_retries_are_idempotent_but_gaps_and_sequence_reuse_fail() {
        let root = TestRoot::new("duplicates");
        let mut receiver = BlockzillaRawReceiver::open(config(&root, 4)).unwrap();
        let first = record(0, 10, 3);
        receiver.push_batch(vec![first.clone()]).unwrap();
        let after_first = spool_segment_bytes(receiver.spool_journal_dir());
        assert_eq!(
            receiver
                .push_batch(vec![first.clone()])
                .unwrap()
                .through_sequence,
            0
        );
        assert_eq!(
            spool_segment_bytes(receiver.spool_journal_dir()),
            after_first
        );

        let second = record(1, 11, 4);
        assert_eq!(
            receiver
                .push_batch(vec![first, second.clone()])
                .unwrap()
                .through_sequence,
            1
        );
        assert!(receiver.push_batch(vec![record(1, 11, 5)]).is_err());
        assert!(receiver.push_batch(vec![record(3, 13, 6)]).is_err());
        assert_eq!(receiver.get_ack().unwrap().through_sequence, 1);
    }

    #[test]
    fn distinct_fork_candidates_at_one_slot_are_preserved() {
        let root = TestRoot::new("forks");
        let mut receiver = BlockzillaRawReceiver::open(config(&root, 4)).unwrap();
        receiver
            .push_batch(vec![record(0, 1002, 10), record(1, 1002, 11)])
            .unwrap();
        assert_eq!(receiver.get_ack().unwrap().through_sequence, 1);
    }

    #[test]
    fn invalid_hashes_digests_and_shapes_do_not_mutate_spool() {
        let root = TestRoot::new("invalid");
        let mut receiver = BlockzillaRawReceiver::open(config(&root, 8)).unwrap();
        let before = spool_segment_bytes(receiver.spool_journal_dir());

        let mut bad_sha = record(0, 20, 20);
        bad_sha.raw_protobuf_sha256[0] ^= 1;
        assert!(receiver.push_batch(vec![bad_sha]).is_err());

        let mut bad_digest = record(0, 20, 20);
        bad_digest.offer.content_digest.0[0] ^= 1;
        assert!(receiver.push_batch(vec![bad_digest]).is_err());

        let mut bad_key = record(0, 20, 20);
        bad_key.offer.logical_key = LogicalKey::Block {
            slot: 21,
            blockhash: [20; 32],
        };
        bad_key.offer.content_digest = compute_content_digest(
            &bad_key.offer.cluster_id,
            &bad_key.offer.logical_key,
            bad_key.offer.payload_format_version,
            &bad_key.compressed_payload,
        );
        assert!(receiver.push_batch(vec![bad_key]).is_err());

        let mut malformed_block = Vec::new();
        push_varint_field(&mut malformed_block, 1, 20);
        push_bytes_field(&mut malformed_block, 2, encode_hash(&[20; 32]).as_bytes());
        push_bytes_field(&mut malformed_block, 8, encode_hash(&[19; 32]).as_bytes());
        push_varint_field(&mut malformed_block, 9, 1);
        push_varint_field(&mut malformed_block, 10, 0);
        push_bytes_field(&mut malformed_block, 6, &[]);
        push_varint_field(&mut malformed_block, 12, 1);
        let mut malformed_update = Vec::new();
        push_bytes_field(&mut malformed_update, 5, &malformed_block);
        assert!(
            receiver
                .push_batch(vec![record_from_raw(0, 20, [20; 32], malformed_update)])
                .is_err()
        );
        assert_eq!(spool_segment_bytes(receiver.spool_journal_dir()), before);
        assert!(receiver.get_ack().is_none());
    }

    #[test]
    fn protobuf_scanner_rejects_repeated_empty_messages_without_heap_expansion() {
        let mut block = Vec::new();
        push_varint_field(&mut block, 1, 1);
        push_bytes_field(&mut block, 2, encode_hash(&[1; 32]).as_bytes());
        push_bytes_field(&mut block, 8, encode_hash(&[0; 32]).as_bytes());
        push_varint_field(&mut block, 9, 70_000);
        push_varint_field(&mut block, 10, 0);
        for _ in 0..70_000 {
            push_bytes_field(&mut block, 6, &[]);
        }
        let mut update = Vec::new();
        push_bytes_field(&mut update, 5, &block);
        assert!(scan_subscribe_update_block(&update).is_err());
    }

    #[test]
    fn root_capacity_and_filesystem_reserve_fail_before_spool_mutation() {
        let root = TestRoot::new("capacity");
        let mut receiver = BlockzillaRawReceiver::open(config(&root, 8)).unwrap();
        let before = spool_segment_bytes(receiver.spool_journal_dir());
        receiver.config.max_spool_bytes = receiver_spool_root_bytes(&root.0).unwrap();
        assert!(receiver.push_batch(vec![record(0, 30, 30)]).is_err());
        assert_eq!(spool_segment_bytes(receiver.spool_journal_dir()), before);

        receiver.config.max_spool_bytes = u64::MAX;
        receiver.config.reserve_free_bytes = u64::MAX;
        assert!(receiver.push_batch(vec![record(0, 30, 30)]).is_err());
        assert_eq!(spool_segment_bytes(receiver.spool_journal_dir()), before);
    }

    #[test]
    fn progress_wal_repairs_partial_tail_and_fails_closed_on_committed_corruption() {
        let root = TestRoot::new("progress-recovery");
        let configuration = config(&root, 8);
        let mut receiver = BlockzillaRawReceiver::open(configuration.clone()).unwrap();
        receiver.push_batch(vec![record(0, 40, 40)]).unwrap();
        let progress_path = receiver.progress_wal_path().to_path_buf();
        let valid_len = fs::metadata(&progress_path).unwrap().len();
        drop(receiver);
        OpenOptions::new()
            .append(true)
            .open(&progress_path)
            .unwrap()
            .write_all(b"BZRP\x01")
            .unwrap();
        let recovered = BlockzillaRawReceiver::open(configuration.clone()).unwrap();
        assert_eq!(recovered.get_ack().unwrap().through_sequence, 0);
        assert_eq!(fs::metadata(&progress_path).unwrap().len(), valid_len);
        drop(recovered);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&progress_path)
            .unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_data().unwrap();
        drop(file);
        assert!(BlockzillaRawReceiver::open(configuration).is_err());
    }

    #[test]
    fn recovery_rejects_spool_more_than_one_record_ahead() {
        let root = TestRoot::new("divergence");
        let configuration = config(&root, 8);
        fs::create_dir_all(&root.0).unwrap();
        let identity = SpoolJournalIdentity {
            cluster_id: configuration.stream.cluster_id.clone(),
            origin_node_id: configuration.stream.origin_node_id.clone(),
            source_id: configuration.stream.source_id.clone(),
            journal_id: configuration.stream.journal_id,
        };
        let mut spool = SpoolWriter::open(&root.0, identity, configuration.spool_options).unwrap();
        for input in [record(0, 50, 50), record(1, 51, 51)] {
            let validated =
                validate_record_against_stream(input, &configuration.stream, configuration.limits)
                    .unwrap();
            spool
                .append_and_sync(validated.metadata, &validated.compressed_payload)
                .unwrap();
        }
        drop(spool);
        assert!(BlockzillaRawReceiver::open(configuration).is_err());
    }

    #[test]
    fn progress_compaction_is_bounded_and_keeps_exact_retry_window() {
        let root = TestRoot::new("compaction");
        let configuration = config(&root, 2);
        let mut receiver = BlockzillaRawReceiver::open(configuration.clone()).unwrap();
        for sequence in 0..20u64 {
            receiver
                .push_batch(vec![record(sequence, 100 + sequence, sequence as u8 + 1)])
                .unwrap();
        }
        assert!(receiver.progress.frames_in_file <= 4);
        let compacted_len = fs::metadata(receiver.progress_wal_path()).unwrap().len();
        assert!(compacted_len < 16 * 1024);
        drop(receiver);

        let mut recovered = BlockzillaRawReceiver::open(configuration).unwrap();
        assert_eq!(recovered.get_ack().unwrap().through_sequence, 19);
        assert_eq!(
            recovered
                .push_batch(vec![record(18, 118, 19)])
                .unwrap()
                .through_sequence,
            19
        );
        assert!(recovered.push_batch(vec![record(17, 117, 18)]).is_err());
    }

    #[test]
    fn distinct_streams_can_stay_open_under_one_serialized_root_quota() {
        let root = TestRoot::new("multi-stream");
        let first = BlockzillaRawReceiver::open(config(&root, 4)).unwrap();
        let mut second_config = config(&root, 4);
        second_config.stream.journal_id = [8; 16];
        let second = BlockzillaRawReceiver::open(second_config).unwrap();
        assert_ne!(first.spool_journal_dir(), second.spool_journal_dir());
    }
}
