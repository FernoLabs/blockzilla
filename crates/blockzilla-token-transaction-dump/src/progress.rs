//! Line-oriented progress events for unattended extraction runs.

use std::{
    io::{self, Write},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use blockzilla_read_sdk::BatchBarrierBlockStats;
use serde::Serialize;

use crate::extract::SingleReadExtractorStats;

const COMPONENT: &str = "blockzilla-token-transaction-dump";

#[derive(Debug, Default, Clone, Copy)]
pub struct PassMetrics {
    pub blocks: u64,
    pub transactions: u64,
    pub selected_transactions: u64,
    pub tracked_accounts: usize,
    pub compressed_bytes: u64,
    pub output_transactions: u64,
}

#[derive(Debug)]
pub struct ExtractionProgress {
    started: Instant,
    phase: &'static str,
    total_epochs: usize,
    completed_at_start: usize,
    completed_this_process: usize,
}

#[derive(Debug)]
pub struct ProgressTimer {
    run_started: Instant,
    started: Instant,
    phase: &'static str,
    epoch: u64,
    pass: &'static str,
}

#[derive(Serialize)]
struct ProgressEvent<'a> {
    component: &'static str,
    event: &'a str,
    unix_ms: u128,
    run_elapsed_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pass: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_epochs: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_epochs: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resumed_epochs: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    eta_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blocks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transactions: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    selected_transactions: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tracked_accounts: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compressed_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compressed_mib_per_second: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_transactions: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    single_read_reader: Option<SingleReadReaderEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    single_read_extractor: Option<SingleReadExtractorEvent>,
}

#[derive(Debug, Serialize)]
struct SingleReadReaderEvent {
    block_count: u64,
    borrowed_storage_blocks: u64,
    owned_schema_fallback_blocks: u64,
    batch_count: u64,
    read_call_count: u64,
    compressed_bytes: u64,
    decompression_count: u64,
    decompressed_bytes: u64,
    stage_a_block_count: u64,
    stage_b_block_count: u64,
    producer_read_seconds: f64,
    stage_a_seconds: f64,
    merge_seconds: f64,
    stage_b_seconds: f64,
    producer_wait_for_free_buffer_seconds: f64,
    coordinator_wait_for_ready_batch_seconds: f64,
    max_compressed_batch_bytes: usize,
    max_declared_uncompressed_batch_bytes: u64,
    max_live_decompressed_batch_bytes: usize,
    max_retained_decompressed_capacity_bytes: usize,
    decompressed_buffer_reuse_count: u64,
    decompressed_buffer_growth_count: u64,
    transaction_state_buffer_reuse_count: u64,
    transaction_state_buffer_growth_count: u64,
    max_live_transaction_state_bytes: usize,
    max_retained_transaction_state_capacity_bytes: usize,
}

#[derive(Debug, Serialize)]
struct SingleReadExtractorEvent {
    creation_candidates: u64,
    unique_candidate_ids: u64,
    unique_candidate_raw_refs: u64,
    new_accounts: u64,
    registry_rows_read: u64,
    registry_coalesced_read_calls: u64,
    registry_read_bytes: u64,
    mphf_lookups: u64,
    registry_resolution_seconds: f64,
    target_build_seconds: f64,
    target_finalize_seconds: f64,
    discovery_validation_seconds: f64,
    raw_validation_seconds: f64,
    clean_hint_batches: u64,
    dirty_hint_batches: u64,
    hint_direct_matches: u64,
    hint_skips_without_decode: u64,
    hint_exact_reparses: u64,
    metadata_owned_fallbacks: u64,
}

impl From<&BatchBarrierBlockStats> for SingleReadReaderEvent {
    fn from(stats: &BatchBarrierBlockStats) -> Self {
        Self {
            block_count: stats.block_count,
            borrowed_storage_blocks: stats.borrowed_storage_blocks,
            owned_schema_fallback_blocks: stats.owned_schema_fallback_blocks,
            batch_count: stats.batch_count,
            read_call_count: stats.read_call_count,
            compressed_bytes: stats.compressed_bytes,
            decompression_count: stats.decompression_count,
            decompressed_bytes: stats.decompressed_bytes,
            stage_a_block_count: stats.stage_a_block_count,
            stage_b_block_count: stats.stage_b_block_count,
            producer_read_seconds: stats.producer_read_wall_time.as_secs_f64(),
            stage_a_seconds: stats.coordinator_stage_a_wall_time.as_secs_f64(),
            merge_seconds: stats.coordinator_merge_wall_time.as_secs_f64(),
            stage_b_seconds: stats.coordinator_stage_b_wall_time.as_secs_f64(),
            producer_wait_for_free_buffer_seconds: stats
                .producer_wait_for_free_buffer_time
                .as_secs_f64(),
            coordinator_wait_for_ready_batch_seconds: stats
                .coordinator_wait_for_ready_batch_time
                .as_secs_f64(),
            max_compressed_batch_bytes: stats.max_compressed_batch_bytes,
            max_declared_uncompressed_batch_bytes: stats.max_declared_uncompressed_batch_bytes,
            max_live_decompressed_batch_bytes: stats.max_live_decompressed_batch_bytes,
            max_retained_decompressed_capacity_bytes: stats
                .max_retained_decompressed_capacity_bytes,
            decompressed_buffer_reuse_count: stats.decompressed_buffer_reuse_count,
            decompressed_buffer_growth_count: stats.decompressed_buffer_growth_count,
            transaction_state_buffer_reuse_count: stats.transaction_state_buffer_reuse_count,
            transaction_state_buffer_growth_count: stats.transaction_state_buffer_growth_count,
            max_live_transaction_state_bytes: stats.max_live_transaction_state_bytes,
            max_retained_transaction_state_capacity_bytes: stats
                .max_retained_transaction_state_capacity_bytes,
        }
    }
}

impl SingleReadExtractorEvent {
    fn from_stats(
        stats: &SingleReadExtractorStats,
        discovery_validation: Duration,
        raw_validation: Duration,
    ) -> Self {
        Self {
            creation_candidates: stats.creation_candidates,
            unique_candidate_ids: stats.unique_candidate_ids,
            unique_candidate_raw_refs: stats.unique_candidate_raw_refs,
            new_accounts: stats.new_accounts,
            registry_rows_read: stats.registry.registry_rows_read,
            registry_coalesced_read_calls: stats.registry.registry_coalesced_read_calls,
            registry_read_bytes: stats.registry.registry_read_bytes,
            mphf_lookups: stats.registry.mphf_lookups,
            registry_resolution_seconds: stats.registry_resolution_time.as_secs_f64(),
            target_build_seconds: stats.target_build_time.as_secs_f64(),
            target_finalize_seconds: stats.target_finalize_time.as_secs_f64(),
            discovery_validation_seconds: discovery_validation.as_secs_f64(),
            raw_validation_seconds: raw_validation.as_secs_f64(),
            clean_hint_batches: stats.clean_hint_batches,
            dirty_hint_batches: stats.dirty_hint_batches,
            hint_direct_matches: stats.hint_direct_matches,
            hint_skips_without_decode: stats.hint_skips_without_decode,
            hint_exact_reparses: stats.hint_exact_reparses,
            metadata_owned_fallbacks: stats.metadata_owned_fallbacks,
        }
    }
}

impl ExtractionProgress {
    pub fn start(first_epoch: u64, last_epoch: u64, resumed_epochs: usize) -> Self {
        Self::start_phase(first_epoch, last_epoch, resumed_epochs, "extraction")
    }

    pub fn start_phase(
        first_epoch: u64,
        last_epoch: u64,
        resumed_epochs: usize,
        phase: &'static str,
    ) -> Self {
        let total_epochs = last_epoch
            .checked_sub(first_epoch)
            .and_then(|span| span.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .unwrap_or(usize::MAX);
        let progress = Self {
            started: Instant::now(),
            phase,
            total_epochs,
            completed_at_start: resumed_epochs,
            completed_this_process: 0,
        };
        progress.emit(ProgressEvent {
            component: COMPONENT,
            event: "run_start",
            unix_ms: unix_ms(),
            run_elapsed_seconds: 0.0,
            epoch: None,
            pass: None,
            phase: Some(phase),
            status: Some(if resumed_epochs == 0 {
                "new"
            } else {
                "resumed"
            }),
            first_epoch: Some(first_epoch),
            last_epoch: Some(last_epoch),
            total_epochs: Some(total_epochs),
            completed_epochs: Some(resumed_epochs),
            resumed_epochs: Some(resumed_epochs),
            elapsed_seconds: None,
            eta_seconds: None,
            blocks: None,
            transactions: None,
            selected_transactions: None,
            tracked_accounts: None,
            compressed_bytes: None,
            compressed_mib_per_second: None,
            output_transactions: None,
            message: None,
            single_read_reader: None,
            single_read_extractor: None,
        });
        progress
    }

    pub fn epoch_start(&self, epoch: u64, tracked_accounts: usize) {
        self.emit(ProgressEvent {
            component: COMPONENT,
            event: "epoch_start",
            unix_ms: unix_ms(),
            run_elapsed_seconds: self.started.elapsed().as_secs_f64(),
            epoch: Some(epoch),
            pass: None,
            phase: Some(self.phase),
            status: Some("running"),
            first_epoch: None,
            last_epoch: None,
            total_epochs: Some(self.total_epochs),
            completed_epochs: Some(self.completed_count()),
            resumed_epochs: Some(self.completed_at_start),
            elapsed_seconds: None,
            eta_seconds: None,
            blocks: None,
            transactions: None,
            selected_transactions: None,
            tracked_accounts: Some(tracked_accounts),
            compressed_bytes: None,
            compressed_mib_per_second: None,
            output_transactions: None,
            message: None,
            single_read_reader: None,
            single_read_extractor: None,
        });
    }

    pub fn pass_start(&self, epoch: u64, pass: &'static str) -> ProgressTimer {
        self.emit(ProgressEvent {
            component: COMPONENT,
            event: "pass_start",
            unix_ms: unix_ms(),
            run_elapsed_seconds: self.started.elapsed().as_secs_f64(),
            epoch: Some(epoch),
            pass: Some(pass),
            phase: Some(self.phase),
            status: Some("running"),
            first_epoch: None,
            last_epoch: None,
            total_epochs: None,
            completed_epochs: Some(self.completed_count()),
            resumed_epochs: None,
            elapsed_seconds: None,
            eta_seconds: None,
            blocks: None,
            transactions: None,
            selected_transactions: None,
            tracked_accounts: None,
            compressed_bytes: None,
            compressed_mib_per_second: None,
            output_transactions: None,
            message: None,
            single_read_reader: None,
            single_read_extractor: None,
        });
        ProgressTimer {
            run_started: self.started,
            started: Instant::now(),
            phase: self.phase,
            epoch,
            pass,
        }
    }

    pub fn epoch_complete(&mut self, epoch: u64, elapsed: Duration, metrics: PassMetrics) {
        self.completed_this_process = self.completed_this_process.saturating_add(1);
        let measured_seconds = self.started.elapsed().as_secs_f64();
        let remaining = self.total_epochs.saturating_sub(self.completed_count());
        let eta_seconds = (self.completed_this_process != 0)
            .then(|| measured_seconds / self.completed_this_process as f64 * remaining as f64);
        self.emit(ProgressEvent {
            component: COMPONENT,
            event: "epoch_complete",
            unix_ms: unix_ms(),
            run_elapsed_seconds: measured_seconds,
            epoch: Some(epoch),
            pass: None,
            phase: Some(self.phase),
            status: Some("complete"),
            first_epoch: None,
            last_epoch: None,
            total_epochs: Some(self.total_epochs),
            completed_epochs: Some(self.completed_count()),
            resumed_epochs: Some(self.completed_at_start),
            elapsed_seconds: Some(elapsed.as_secs_f64()),
            eta_seconds,
            blocks: Some(metrics.blocks),
            transactions: Some(metrics.transactions),
            selected_transactions: Some(metrics.selected_transactions),
            tracked_accounts: Some(metrics.tracked_accounts),
            compressed_bytes: Some(metrics.compressed_bytes),
            compressed_mib_per_second: rate_mib(metrics.compressed_bytes, elapsed),
            output_transactions: Some(metrics.output_transactions),
            message: None,
            single_read_reader: None,
            single_read_extractor: None,
        });
    }

    pub fn single_read_reader_stats(&self, epoch: u64, stats: &BatchBarrierBlockStats) {
        self.emit(ProgressEvent {
            component: COMPONENT,
            event: "single_read_reader_stats",
            unix_ms: unix_ms(),
            run_elapsed_seconds: self.started.elapsed().as_secs_f64(),
            epoch: Some(epoch),
            pass: Some("discover_then_copy_retained_batches"),
            phase: Some(self.phase),
            status: Some("complete"),
            first_epoch: None,
            last_epoch: None,
            total_epochs: Some(self.total_epochs),
            completed_epochs: Some(self.completed_count()),
            resumed_epochs: Some(self.completed_at_start),
            elapsed_seconds: None,
            eta_seconds: None,
            blocks: Some(stats.block_count),
            transactions: None,
            selected_transactions: None,
            tracked_accounts: None,
            compressed_bytes: Some(stats.compressed_bytes),
            compressed_mib_per_second: None,
            output_transactions: None,
            message: None,
            single_read_reader: Some(SingleReadReaderEvent::from(stats)),
            single_read_extractor: None,
        });
    }

    pub(crate) fn single_read_extractor_stats(
        &self,
        epoch: u64,
        stats: &SingleReadExtractorStats,
        discovery_validation: Duration,
        raw_validation: Duration,
    ) {
        self.emit(ProgressEvent {
            component: COMPONENT,
            event: "single_read_extractor_stats",
            unix_ms: unix_ms(),
            run_elapsed_seconds: self.started.elapsed().as_secs_f64(),
            epoch: Some(epoch),
            pass: Some("discover_then_copy_retained_batches"),
            phase: Some(self.phase),
            status: Some("complete"),
            first_epoch: None,
            last_epoch: None,
            total_epochs: Some(self.total_epochs),
            completed_epochs: Some(self.completed_count()),
            resumed_epochs: Some(self.completed_at_start),
            elapsed_seconds: None,
            eta_seconds: None,
            blocks: None,
            transactions: None,
            selected_transactions: None,
            tracked_accounts: None,
            compressed_bytes: None,
            compressed_mib_per_second: None,
            output_transactions: None,
            message: None,
            single_read_reader: None,
            single_read_extractor: Some(SingleReadExtractorEvent::from_stats(
                stats,
                discovery_validation,
                raw_validation,
            )),
        });
    }

    pub fn run_complete(&self) {
        self.emit(ProgressEvent {
            component: COMPONENT,
            event: "run_complete",
            unix_ms: unix_ms(),
            run_elapsed_seconds: self.started.elapsed().as_secs_f64(),
            epoch: None,
            pass: None,
            phase: Some(self.phase),
            status: Some("complete"),
            first_epoch: None,
            last_epoch: None,
            total_epochs: Some(self.total_epochs),
            completed_epochs: Some(self.completed_count()),
            resumed_epochs: Some(self.completed_at_start),
            elapsed_seconds: Some(self.started.elapsed().as_secs_f64()),
            eta_seconds: Some(0.0),
            blocks: None,
            transactions: None,
            selected_transactions: None,
            tracked_accounts: None,
            compressed_bytes: None,
            compressed_mib_per_second: None,
            output_transactions: None,
            message: None,
            single_read_reader: None,
            single_read_extractor: None,
        });
    }

    pub fn note(&self, epoch: Option<u64>, status: &'static str, message: &str) {
        self.emit(ProgressEvent {
            component: COMPONENT,
            event: "recovery",
            unix_ms: unix_ms(),
            run_elapsed_seconds: self.started.elapsed().as_secs_f64(),
            epoch,
            pass: None,
            phase: Some(self.phase),
            status: Some(status),
            first_epoch: None,
            last_epoch: None,
            total_epochs: Some(self.total_epochs),
            completed_epochs: Some(self.completed_count()),
            resumed_epochs: Some(self.completed_at_start),
            elapsed_seconds: None,
            eta_seconds: None,
            blocks: None,
            transactions: None,
            selected_transactions: None,
            tracked_accounts: None,
            compressed_bytes: None,
            compressed_mib_per_second: None,
            output_transactions: None,
            message: Some(message),
            single_read_reader: None,
            single_read_extractor: None,
        });
    }

    fn completed_count(&self) -> usize {
        self.completed_at_start
            .saturating_add(self.completed_this_process)
    }

    fn emit(&self, event: ProgressEvent<'_>) {
        emit(event);
    }
}

impl ProgressTimer {
    pub fn complete(self, metrics: PassMetrics) -> Duration {
        let elapsed = self.started.elapsed();
        emit(ProgressEvent {
            component: COMPONENT,
            event: "pass_complete",
            unix_ms: unix_ms(),
            run_elapsed_seconds: self.run_started.elapsed().as_secs_f64(),
            epoch: Some(self.epoch),
            pass: Some(self.pass),
            phase: Some(self.phase),
            status: Some("complete"),
            first_epoch: None,
            last_epoch: None,
            total_epochs: None,
            completed_epochs: None,
            resumed_epochs: None,
            elapsed_seconds: Some(elapsed.as_secs_f64()),
            eta_seconds: None,
            blocks: Some(metrics.blocks),
            transactions: Some(metrics.transactions),
            selected_transactions: Some(metrics.selected_transactions),
            tracked_accounts: Some(metrics.tracked_accounts),
            compressed_bytes: Some(metrics.compressed_bytes),
            compressed_mib_per_second: rate_mib(metrics.compressed_bytes, elapsed),
            output_transactions: Some(metrics.output_transactions),
            message: None,
            single_read_reader: None,
            single_read_extractor: None,
        });
        elapsed
    }
}

fn emit(event: ProgressEvent<'_>) {
    let line = serde_json::to_string(&event).unwrap_or_else(|error| {
        format!("{{\"component\":\"{COMPONENT}\",\"event\":\"log_error\",\"message\":\"{error}\"}}")
    });
    let mut stderr = io::stderr().lock();
    let _ = stderr.write_all(line.as_bytes());
    let _ = stderr.write_all(b"\n");
    let _ = stderr.flush();
}

fn rate_mib(bytes: u64, elapsed: Duration) -> Option<f64> {
    let seconds = elapsed.as_secs_f64();
    (seconds > 0.0).then(|| bytes as f64 / (1024.0 * 1024.0) / seconds)
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_elapsed_has_no_rate() {
        assert_eq!(rate_mib(1024, Duration::ZERO), None);
    }

    #[test]
    fn rate_uses_binary_mebibytes() {
        assert_eq!(rate_mib(2 * 1024 * 1024, Duration::from_secs(2)), Some(1.0));
    }

    #[test]
    fn single_read_reader_event_exposes_physical_read_and_reuse_proof() {
        let stats = BatchBarrierBlockStats {
            block_count: 5,
            batch_count: 2,
            read_call_count: 2,
            compressed_bytes: 11,
            decompression_count: 5,
            decompressed_bytes: 23,
            stage_a_block_count: 5,
            stage_b_block_count: 5,
            producer_read_wall_time: Duration::from_millis(3),
            coordinator_stage_a_wall_time: Duration::from_millis(4),
            coordinator_merge_wall_time: Duration::from_millis(5),
            coordinator_stage_b_wall_time: Duration::from_millis(6),
            max_live_decompressed_batch_bytes: 17,
            max_retained_decompressed_capacity_bytes: 19,
            decompressed_buffer_reuse_count: 3,
            decompressed_buffer_growth_count: 2,
            transaction_state_buffer_reuse_count: 7,
            transaction_state_buffer_growth_count: 1,
            max_live_transaction_state_bytes: 29,
            max_retained_transaction_state_capacity_bytes: 31,
            ..BatchBarrierBlockStats::default()
        };
        let value = serde_json::to_value(SingleReadReaderEvent::from(&stats)).unwrap();
        assert_eq!(value["batch_count"], 2);
        assert_eq!(value["read_call_count"], 2);
        assert_eq!(value["decompression_count"], 5);
        assert_eq!(value["stage_a_block_count"], 5);
        assert_eq!(value["stage_b_block_count"], 5);
        assert_eq!(value["max_live_decompressed_batch_bytes"], 17);
        assert_eq!(value["max_retained_decompressed_capacity_bytes"], 19);
        assert_eq!(value["decompressed_buffer_reuse_count"], 3);
        assert_eq!(value["decompressed_buffer_growth_count"], 2);
        assert_eq!(value["transaction_state_buffer_reuse_count"], 7);
        assert_eq!(value["transaction_state_buffer_growth_count"], 1);
        assert_eq!(value["max_live_transaction_state_bytes"], 29);
        assert_eq!(value["max_retained_transaction_state_capacity_bytes"], 31);
    }

    #[test]
    fn single_read_extractor_event_exposes_resolution_and_validation_costs() {
        let stats = SingleReadExtractorStats {
            creation_candidates: 9,
            unique_candidate_ids: 4,
            unique_candidate_raw_refs: 2,
            new_accounts: 3,
            registry: crate::extract::RegistryResolutionStats {
                registry_rows_read: 6,
                registry_coalesced_read_calls: 2,
                registry_read_bytes: 192,
                mphf_lookups: 7,
            },
            registry_resolution_time: Duration::from_millis(11),
            target_build_time: Duration::from_millis(12),
            target_finalize_time: Duration::from_millis(13),
            clean_hint_batches: 16,
            dirty_hint_batches: 2,
            hint_direct_matches: 17,
            hint_skips_without_decode: 18,
            hint_exact_reparses: 19,
            metadata_owned_fallbacks: 20,
        };
        let event = SingleReadExtractorEvent::from_stats(
            &stats,
            Duration::from_millis(14),
            Duration::from_millis(15),
        );
        let value = serde_json::to_value(event).unwrap();

        assert_eq!(value["creation_candidates"], 9);
        assert_eq!(value["unique_candidate_ids"], 4);
        assert_eq!(value["unique_candidate_raw_refs"], 2);
        assert_eq!(value["registry_rows_read"], 6);
        assert_eq!(value["registry_coalesced_read_calls"], 2);
        assert_eq!(value["registry_read_bytes"], 192);
        assert_eq!(value["mphf_lookups"], 7);
        assert_eq!(value["registry_resolution_seconds"], 0.011);
        assert_eq!(value["target_build_seconds"], 0.012);
        assert_eq!(value["target_finalize_seconds"], 0.013);
        assert_eq!(value["discovery_validation_seconds"], 0.014);
        assert_eq!(value["raw_validation_seconds"], 0.015);
        assert_eq!(value["clean_hint_batches"], 16);
        assert_eq!(value["dirty_hint_batches"], 2);
        assert_eq!(value["hint_direct_matches"], 17);
        assert_eq!(value["hint_skips_without_decode"], 18);
        assert_eq!(value["hint_exact_reparses"], 19);
        assert_eq!(value["metadata_owned_fallbacks"], 20);
    }
}
