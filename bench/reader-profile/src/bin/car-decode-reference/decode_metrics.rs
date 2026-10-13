//! Opt-in decode-stage wall timers with local worker accumulators.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy)]
#[repr(usize)]
pub(super) enum Stage {
    TransactionDeserialize,
    MetadataDecompress,
    MetadataDeserialize,
    VoteClassify,
    MessageHash,
    ExportEncode,
    ReceiptDigest,
    TransactionRecycle,
    Rewards,
    BlockExport,
    BlockDigest,
}

const STAGES: [(Stage, &str, &str); 11] = [
    (
        Stage::TransactionDeserialize,
        "transaction_deserialize",
        "sampled transactions",
    ),
    (
        Stage::MetadataDecompress,
        "metadata_decompress",
        "sampled nonempty owned metadata or sampled visitor metadata",
    ),
    (
        Stage::MetadataDeserialize,
        "metadata_deserialize",
        "sampled transactions; includes owned metadata reset",
    ),
    (Stage::VoteClassify, "vote_classify", "sampled transactions"),
    (
        Stage::MessageHash,
        "message_hash",
        "sampled transactions; update and both existing finalize calls are separate timed regions",
    ),
    (
        Stage::ExportEncode,
        "export_encode",
        "sampled transactions with export enabled",
    ),
    (
        Stage::ReceiptDigest,
        "receipt_digest",
        "sampled transactions",
    ),
    (
        Stage::TransactionRecycle,
        "transaction_recycle",
        "sampled transactions",
    ),
    (Stage::Rewards, "rewards", "every block that has rewards"),
    (
        Stage::BlockExport,
        "block_export",
        "every exported block; includes export lock waits and writes or memory copies",
    ),
    (
        Stage::BlockDigest,
        "block_digest",
        "every block; final digest and hex formatting",
    ),
];

#[derive(Clone, Copy, Default)]
struct Timing {
    regions: u64,
    elapsed: Duration,
}

#[derive(Default)]
struct WorkerStats {
    blocks_started: u64,
    transactions_seen: u64,
    sampled_transactions: u64,
    stages: [Timing; STAGES.len()],
}

pub(super) struct Metrics {
    sample_every: u64,
    workers: Mutex<Vec<WorkerStats>>,
}

impl Metrics {
    pub(super) fn new(sample_every: u64) -> Result<Self> {
        ensure!(
            sample_every > 0,
            "decode metrics sample interval must be positive"
        );
        Ok(Self {
            sample_every,
            workers: Mutex::new(Vec::new()),
        })
    }

    pub(super) fn worker(self: &Arc<Self>) -> Worker {
        Worker {
            metrics: Arc::clone(self),
            sample_threshold: u64::MAX / self.sample_every,
            stats: WorkerStats::default(),
        }
    }

    /// Take this snapshot after every decoder has been dropped.
    pub(super) fn receipt(&self) -> Value {
        let workers = self
            .workers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut total = WorkerStats::default();
        for worker in workers.iter() {
            total.blocks_started += worker.blocks_started;
            total.transactions_seen += worker.transactions_seen;
            total.sampled_transactions += worker.sampled_transactions;
            for (sum, stage) in total.stages.iter_mut().zip(&worker.stages) {
                sum.regions += stage.regions;
                sum.elapsed += stage.elapsed;
            }
        }
        json!({
            "enabled": true,
            "sample_every": self.sample_every,
            "sampling_policy": "deterministic SplitMix64 finalizer of slot XOR (transaction index * 0xd6e8feb86659fd93); select hash <= floor(u64::MAX / sample_every); target probability 1/sample_every, independent of worker assignment",
            "timing_scope": "sums of worker wall times, not CPU seconds; transaction stages are sampled without extrapolation; block stages cover every observed block; failed operations can leave partial counts",
            "timing_is_instrumented": true,
            "workers_finished": workers.len(),
            "blocks_started": total.blocks_started,
            "transactions_seen": total.transactions_seen,
            "sampled_transactions": total.sampled_transactions,
            "stages": stage_receipt(&total),
            "workers": workers.iter().enumerate().map(|(index, worker)| json!({
                "merge_index": index,
                "blocks_started": worker.blocks_started,
                "transactions_seen": worker.transactions_seen,
                "sampled_transactions": worker.sampled_transactions,
                "stages": stage_receipt(worker),
            })).collect::<Vec<_>>(),
        })
    }
}

fn stage_receipt(stats: &WorkerStats) -> Value {
    STAGES
        .iter()
        .map(|(stage, name, scope)| {
            let timing = stats.stages[*stage as usize];
            (
                (*name).to_owned(),
                json!({"timed_regions": timing.regions, "wall_seconds": timing.elapsed.as_secs_f64(), "scope": scope}),
            )
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

pub(super) struct Worker {
    metrics: Arc<Metrics>,
    sample_threshold: u64,
    stats: WorkerStats,
}

impl Worker {
    pub(super) fn block_started(&mut self) {
        self.stats.blocks_started += 1;
    }

    pub(super) fn sample_transaction(&mut self, slot: u64, index: u64) -> bool {
        self.stats.transactions_seen += 1;
        let mut hash = slot ^ index.wrapping_mul(0xd6e8_feb8_6659_fd93);
        hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        hash ^= hash >> 31;
        let sampled = hash <= self.sample_threshold;
        self.stats.sampled_transactions += u64::from(sampled);
        sampled
    }

    pub(super) fn record(&mut self, stage: Stage, elapsed: Duration) {
        let timing = &mut self.stats.stages[stage as usize];
        timing.regions += 1;
        timing.elapsed += elapsed;
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.metrics
            .workers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(std::mem::take(&mut self.stats));
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Decoder, MetadataMode, common_export};
    use super::*;
    use of_car_reader::{CarBlockReader, LosslessBlockReadLimits, OrderedLosslessCarBlock};

    #[test]
    fn counters_merge_once_when_workers_finish_and_keep_raw_wall_times() {
        assert!(Metrics::new(0).is_err());
        let metrics = Arc::new(Metrics::new(1).unwrap());
        let mut first = metrics.worker();
        first.block_started();
        assert!(first.sample_transaction(432000001, 2));
        first.record(Stage::MessageHash, Duration::from_millis(3));
        first.record(Stage::MessageHash, Duration::from_millis(5));
        assert_eq!(metrics.receipt()["workers_finished"], 0);
        {
            let mut second = metrics.worker();
            second.block_started();
            assert!(second.sample_transaction(432000002, 0));
            second.record(Stage::MessageHash, Duration::from_millis(7));
        }
        assert_eq!(metrics.receipt()["workers_finished"], 1);
        drop(first);
        let result = metrics.receipt();
        assert_eq!(result["workers_finished"], 2);
        assert_eq!(result["blocks_started"], 2);
        assert_eq!(result["transactions_seen"], 2);
        assert_eq!(result["sampled_transactions"], 2);
        assert_eq!(result["stages"]["message_hash"]["timed_regions"], 3);
        assert_eq!(result["stages"]["message_hash"]["wall_seconds"], 0.015);
        assert_eq!(result["stages"]["metadata_decompress"]["timed_regions"], 0);
        assert_eq!(metrics.receipt(), result);
    }

    #[test]
    fn sampling_is_stable_across_worker_assignment_and_transaction_order() {
        let metrics = Arc::new(Metrics::new(7).unwrap());
        let mut forward = metrics.worker();
        let selected: Vec<_> = (0..1024)
            .map(|index| forward.sample_transaction(432000123, index))
            .collect();
        assert!(selected.iter().any(|value| *value));
        assert!(selected.iter().any(|value| !value));
        let mut left = metrics.worker();
        let mut right = metrics.worker();
        for index in (0..1024).rev() {
            let worker = if index % 2 == 0 {
                &mut left
            } else {
                &mut right
            };
            assert_eq!(
                worker.sample_transaction(432000123, index as u64),
                selected[index]
            );
        }
    }

    #[test]
    fn sampled_owned_metadata_preserves_empty_legacy_and_protobuf_behavior() {
        // Minimal legacy bincode: success, fee, two balance vectors; all later
        // optional fields end at EOF. This layout is shared by legacy and V2.
        let mut legacy = vec![0; 4];
        legacy.extend_from_slice(&5000_u64.to_le_bytes());
        for balances in [[10_u64, 20], [5, 25]] {
            legacy.extend_from_slice(&2_u64.to_le_bytes());
            for balance in balances {
                legacy.extend_from_slice(&balance.to_le_bytes());
            }
        }
        // Protobuf fee=5000, packed pre_balances=[10,20], post_balances=[5,25].
        let protobuf = [0x10, 0x88, 0x27, 0x1a, 2, 10, 20, 0x22, 2, 5, 25];
        for slot in [1, 67_684_801, 432_000_000] {
            let frame = if slot == 432_000_000 {
                protobuf.as_slice()
            } else {
                legacy.as_slice()
            };
            let metrics = Arc::new(Metrics::new(1).unwrap());
            let mut profiled = Decoder::with_metrics(Some(metrics));
            let mut baseline = Decoder::default();
            for frame in [frame, &[][..], frame] {
                assert_eq!(
                    profiled.decode_owned_metadata(slot, frame, true).unwrap(),
                    baseline.decode_owned_metadata(slot, frame, false).unwrap(),
                );
                assert_eq!(profiled.metadata, baseline.metadata);
                assert_eq!(
                    profiled.metadata.fee,
                    if frame.is_empty() { 0 } else { 5000 }
                );
                assert_eq!(
                    profiled.metadata.pre_balances.len(),
                    if frame.is_empty() { 0 } else { 2 }
                );
            }
            // A truncated zstd frame must retain the existing error mapping.
            let bad = [0x28, 0xb5, 0x2f, 0xfd, 0];
            let profiled_error = profiled
                .decode_owned_metadata(slot, &bad, true)
                .unwrap_err();
            let baseline_error = baseline
                .decode_owned_metadata(slot, &bad, false)
                .unwrap_err();
            assert_eq!(profiled_error.to_string(), baseline_error.to_string());
        }
    }

    #[test]
    fn real_car_metrics_preserve_rows_and_export_bytes_at_full_and_sparse_sampling() {
        let limits = LosslessBlockReadLimits {
            max_entry_payload_bytes: 16 << 20,
            max_block_payload_bytes: 16 << 20,
            max_entries_per_block: 65536,
            max_transactions_per_block: 65535,
        };
        for (fixture, bytes) in [
            include_bytes!("../../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-157-biggest.car").as_slice(),
            include_bytes!("../../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-822-biggest.car").as_slice(),
        ].into_iter().enumerate() {
            let mut reference_rows = None;
            let mut reference_bytes = None;
            for sample_every in [None, Some(1), Some(7)] {
                let metrics = sample_every.map(|every| Arc::new(Metrics::new(every).unwrap()));
                let path = std::env::temp_dir().join(format!(
                    "car-decode-metrics-{}-{fixture}-{}.bin", std::process::id(), sample_every.unwrap_or(0),
                ));
                let export = common_export::Export::create_memory(&path, 64 << 20).unwrap();
                let mut decoder = Decoder::with_metrics(metrics.clone());
                let mut reader = CarBlockReader::with_capacity(bytes, 8 << 20);
                reader.skip_header_bounded(1 << 20).unwrap();
                let mut raw = OrderedLosslessCarBlock::default();
                let mut rows = Vec::new();
                while reader.read_until_block_ordered_lossless_bounded(&mut raw, limits).unwrap() {
                    rows.push(decoder.decode(&raw, MetadataMode::Owned, Some(&export)).unwrap());
                    raw.clear();
                }
                assert!(!rows.is_empty());
                let plan: Vec<_> = rows.iter().map(|row| [row.slot, row.tx]).collect();
                export.finish(&plan).unwrap();
                let bytes = std::fs::read(&path).unwrap();
                std::fs::remove_file(&path).unwrap();
                drop(decoder);
                if let Some(metrics) = metrics {
                    let result = metrics.receipt();
                    assert_eq!(result["workers_finished"], 1);
                    assert_eq!(result["blocks_started"], rows.len());
                    assert_eq!(result["transactions_seen"], rows.iter().map(|row| row.tx).sum::<u64>());
                    let sampled = result["sampled_transactions"].as_u64().unwrap();
                    assert!(sampled > 0);
                    if sample_every == Some(1) {
                        assert_eq!(result["transactions_seen"], sampled);
                    }
                    for stage in ["transaction_deserialize", "metadata_deserialize", "export_encode", "receipt_digest", "transaction_recycle"] {
                        assert_eq!(result["stages"][stage]["timed_regions"], sampled);
                    }
                    assert_eq!(result["stages"]["message_hash"]["timed_regions"], 3 * sampled);
                    assert_eq!(result["stages"]["block_export"]["timed_regions"], rows.len());
                    assert_eq!(result["stages"]["block_digest"]["timed_regions"], rows.len());
                }
                if let Some(previous) = &reference_rows {
                    assert_eq!(&rows, previous);
                    assert_eq!(Some(&bytes), reference_bytes.as_ref());
                }
                reference_rows = Some(rows);
                reference_bytes = Some(bytes);
            }
        }
    }
}
