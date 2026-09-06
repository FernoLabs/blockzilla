//! Size-based ETA estimates for the Firewatch index controller.
//!
//! The controller cannot use Linux process I/O counters as durable progress:
//! those counters report physical storage activity, so page-cache hits can
//! make an active build appear idle. This module instead learns wall-clock
//! seconds per phase-specific input byte from completed work. Callers must use
//! the same byte domain for a phase's history and pending work. The current
//! controller can use its captured `archive-v2-blocks.zstd` size for all
//! phases. This scalar is already durable and available for every candidate.
//!
//! A native usage-sorted direct build should use [`EtaPhase::TargetBuild`] as
//! its calibration phase because it runs the same `build-dense` workload. A
//! direct first-seen build should use [`EtaPhase::SourceControlBuild`]. This
//! lets the all-archive queue use the completed migration samples immediately.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Durable ETA history schema written by the controller.
pub const ETA_HISTORY_SCHEMA_VERSION: u32 = 1;

/// Keep the estimator bounded if a damaged history file contains excessive
/// rows or a caller accidentally retains every attempt forever.
pub const MAX_ETA_HISTORY_SAMPLES: usize = 100_000;

/// Recent samples adapt to changed binaries and storage conditions without
/// letting one old machine profile dominate the current NAS.
pub const MAX_SAMPLES_PER_GROUP: usize = 64;

/// No public ETA can exceed ten leap years. This is a defensive numeric bound,
/// not an operational promise.
pub const MAX_ETA_SECS: f64 = 10.0 * 366.0 * 24.0 * 60.0 * 60.0;

const LOW_QUANTILE: f64 = 0.25;
const HIGH_QUANTILE: f64 = 0.75;
const LOW_RATE_FLOOR_RATIO: f64 = 0.5;
const HIGH_RATE_CEILING_RATIO: f64 = 2.0;

/// Calibration phases. Direct native work deliberately reuses one of the two
/// build phases; see the module documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EtaPhase {
    TargetBuild,
    SourceControlBuild,
    Parity,
}

/// One successfully completed phase. `wall_secs` is measured with a monotonic
/// clock and includes any controller-owned pause. Admission delay is not part
/// of it; queue estimation adds that delay explicitly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedPhaseSample {
    pub epoch: u64,
    pub phase: EtaPhase,
    pub worker_threads: u32,
    pub input_bytes: u64,
    pub started_unix_secs: u64,
    pub completed_unix_secs: u64,
    pub wall_secs: f64,
    /// Informational pause time for audits and later models. The current model
    /// uses wall time, so it never adds this value a second time.
    #[serde(default)]
    pub paused_secs: f64,
}

/// Serializable durable history. The controller can publish this atomically
/// or encode each sample in an append-only record with the same fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EtaHistory {
    pub schema_version: u32,
    pub samples: Vec<CompletedPhaseSample>,
}

impl EtaHistory {
    pub fn new(samples: Vec<CompletedPhaseSample>) -> Self {
        Self {
            schema_version: ETA_HISTORY_SCHEMA_VERSION,
            samples,
        }
    }

    pub fn validate(&self) -> Result<(), EtaError> {
        if self.schema_version != ETA_HISTORY_SCHEMA_VERSION {
            return Err(EtaError::UnsupportedHistorySchema(self.schema_version));
        }
        if self.samples.len() > MAX_ETA_HISTORY_SAMPLES {
            return Err(EtaError::TooManyHistorySamples(self.samples.len()));
        }
        for (index, sample) in self.samples.iter().enumerate() {
            validate_sample(sample).map_err(|reason| EtaError::InvalidSample { index, reason })?;
        }
        Ok(())
    }
}

/// A phase workload. `input_bytes` is a stable phase-specific work scalar, not
/// a live process-I/O counter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseWork {
    pub phase: EtaPhase,
    pub worker_threads: u32,
    pub input_bytes: u64,
}

/// A phase that has already started. Elapsed time uses wall time so it is in
/// the same domain as [`CompletedPhaseSample::wall_secs`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivePhaseWork {
    pub work: PhaseWork,
    pub elapsed_secs: f64,
}

/// Serial phases for one epoch that has not started. Phases from different
/// items can overlap; phases inside one item cannot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuedItemWork {
    pub phases: Vec<PhaseWork>,
}

/// One active epoch plus the phases that remain after its current phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveItemWork {
    pub current: ActivePhaseWork,
    #[serde(default)]
    pub future_phases: Vec<PhaseWork>,
}

/// Full queue input. `effective_concurrency` can be fractional when it also
/// represents the observed duty cycle. It must be positive. A value of `1.0`
/// is the conservative production input while cgroup pressure prevents a
/// second Firewatch worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueEtaInput {
    #[serde(default)]
    pub active_items: Vec<ActiveItemWork>,
    #[serde(default)]
    pub queued_items: Vec<QueuedItemWork>,
    pub stable_admission_gap_secs: f64,
    pub effective_concurrency: f64,
}

/// A central ETA and a deliberately bounded uncertainty interval.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EtaEstimate {
    pub expected_secs: f64,
    pub low_secs: f64,
    pub high_secs: f64,
    /// Number of completed samples in the weakest calibration group used by
    /// this estimate. Zero is used only for an empty queue.
    pub basis_samples: usize,
    /// True when an active phase has already exceeded the calibrated upper
    /// total. Its numeric remaining ETA is then zero, but the caller can show
    /// an explicit overrun state instead of claiming completion.
    pub overdue: bool,
}

#[derive(Debug, Error)]
pub enum EtaError {
    #[error("unsupported Firewatch ETA history schema {0}")]
    UnsupportedHistorySchema(u32),
    #[error("Firewatch ETA history has {0} samples, above the safety limit")]
    TooManyHistorySamples(usize),
    #[error("invalid Firewatch ETA sample {index}: {reason}")]
    InvalidSample { index: usize, reason: &'static str },
    #[error("invalid Firewatch ETA work: {0}")]
    InvalidWork(&'static str),
    #[error("invalid Firewatch ETA queue: {0}")]
    InvalidQueue(&'static str),
}

#[derive(Debug, Clone, Copy)]
struct RateSummary {
    expected_secs_per_byte: f64,
    low_secs_per_byte: f64,
    high_secs_per_byte: f64,
    sample_count: usize,
}

/// Immutable calibration table built from a validated durable history.
#[derive(Debug, Clone, Default)]
pub struct EtaEstimator {
    groups: BTreeMap<(EtaPhase, u32), RateSummary>,
}

impl EtaEstimator {
    pub fn from_history(history: &EtaHistory) -> Result<Self, EtaError> {
        history.validate()?;

        let mut grouped: BTreeMap<(EtaPhase, u32), Vec<&CompletedPhaseSample>> = BTreeMap::new();
        for sample in &history.samples {
            grouped
                .entry((sample.phase, sample.worker_threads))
                .or_default()
                .push(sample);
        }

        let mut groups = BTreeMap::new();
        for (key, mut samples) in grouped {
            samples.sort_by(|left, right| {
                right
                    .completed_unix_secs
                    .cmp(&left.completed_unix_secs)
                    .then_with(|| right.epoch.cmp(&left.epoch))
            });
            samples.truncate(MAX_SAMPLES_PER_GROUP);

            let mut rates = samples
                .iter()
                .map(|sample| sample.wall_secs / sample.input_bytes as f64)
                .collect::<Vec<_>>();
            rates.sort_by(f64::total_cmp);
            let expected = quantile(&rates, 0.5);
            let raw_low = quantile(&rates, LOW_QUANTILE);
            let raw_high = quantile(&rates, HIGH_QUANTILE);
            // Quartiles remain robust, and these ratio clamps keep one sparse
            // group from producing a meaningless public interval.
            let low = raw_low.max(expected * LOW_RATE_FLOOR_RATIO).min(expected);
            let high = raw_high
                .min(expected * HIGH_RATE_CEILING_RATIO)
                .max(expected);
            groups.insert(
                key,
                RateSummary {
                    expected_secs_per_byte: expected,
                    low_secs_per_byte: low,
                    high_secs_per_byte: high,
                    sample_count: rates.len(),
                },
            );
        }
        Ok(Self { groups })
    }

    /// Number of recent samples used for one phase/thread calibration group.
    #[cfg(test)]
    pub fn sample_count(&self, phase: EtaPhase, worker_threads: u32) -> usize {
        self.groups
            .get(&(phase, worker_threads))
            .map_or(0, |summary| summary.sample_count)
    }

    /// Predict a complete phase. `Ok(None)` means the exact phase/thread group
    /// has no completed calibration sample yet.
    pub fn estimate_phase(&self, work: &PhaseWork) -> Result<Option<EtaEstimate>, EtaError> {
        validate_work(work)?;
        let Some(summary) = self.groups.get(&(work.phase, work.worker_threads)) else {
            return Ok(None);
        };
        let input = work.input_bytes as f64;
        Ok(Some(normalize_estimate(EtaEstimate {
            expected_secs: cap_secs(summary.expected_secs_per_byte * input),
            low_secs: cap_secs(summary.low_secs_per_byte * input),
            high_secs: cap_secs(summary.high_secs_per_byte * input),
            basis_samples: summary.sample_count,
            overdue: false,
        })))
    }

    /// Estimate only the remaining time of a running phase. The estimate does
    /// not inspect live bytes or `/proc`; it subtracts elapsed wall time from
    /// the size-scaled completed-phase prediction.
    pub fn estimate_active_phase(
        &self,
        active: &ActivePhaseWork,
    ) -> Result<Option<EtaEstimate>, EtaError> {
        validate_elapsed(active.elapsed_secs)?;
        let Some(total) = self.estimate_phase(&active.work)? else {
            return Ok(None);
        };
        Ok(Some(normalize_estimate(EtaEstimate {
            expected_secs: (total.expected_secs - active.elapsed_secs).max(0.0),
            low_secs: (total.low_secs - active.elapsed_secs).max(0.0),
            high_secs: (total.high_secs - active.elapsed_secs).max(0.0),
            basis_samples: total.basis_samples,
            overdue: active.elapsed_secs > total.high_secs,
        })))
    }

    /// Estimate the remaining queue makespan.
    ///
    /// Every queued phase and every future phase of an active item receives
    /// one full stable-admission gap. The active current phase is already
    /// admitted and does not. Serial phases are first summed per epoch. The
    /// final makespan is the larger of the longest serial chain and total
    /// worker-seconds divided by observed effective concurrency.
    pub fn estimate_queue(&self, input: &QueueEtaInput) -> Result<Option<EtaEstimate>, EtaError> {
        validate_queue_scalars(input)?;
        if input.active_items.is_empty() && input.queued_items.is_empty() {
            return Ok(Some(EtaEstimate {
                expected_secs: 0.0,
                low_secs: 0.0,
                high_secs: 0.0,
                basis_samples: 0,
                overdue: false,
            }));
        }

        let mut chains = Vec::with_capacity(
            input
                .active_items
                .len()
                .saturating_add(input.queued_items.len()),
        );

        for item in &input.active_items {
            let Some(mut chain) = self.estimate_active_phase(&item.current)? else {
                return Ok(None);
            };
            for phase in &item.future_phases {
                let Some(estimate) = self.estimate_phase(phase)? else {
                    return Ok(None);
                };
                add_component(
                    &mut chain,
                    with_exact_delay(estimate, input.stable_admission_gap_secs),
                );
            }
            chains.push(chain);
        }

        for item in &input.queued_items {
            if item.phases.is_empty() {
                return Err(EtaError::InvalidQueue("a queued item has no phases"));
            }
            let mut chain: Option<EtaEstimate> = None;
            for phase in &item.phases {
                let Some(estimate) = self.estimate_phase(phase)? else {
                    return Ok(None);
                };
                let estimate = with_exact_delay(estimate, input.stable_admission_gap_secs);
                match &mut chain {
                    Some(chain) => add_component(chain, estimate),
                    None => chain = Some(estimate),
                }
            }
            chains.push(chain.expect("a nonempty phase list produced a chain"));
        }

        let effective_concurrency = input.effective_concurrency.min(chains.len() as f64);
        let mut total = EtaEstimate {
            expected_secs: 0.0,
            low_secs: 0.0,
            high_secs: 0.0,
            basis_samples: usize::MAX,
            overdue: false,
        };
        let mut longest = total;
        for chain in chains {
            add_component(&mut total, chain);
            longest.expected_secs = longest.expected_secs.max(chain.expected_secs);
            longest.low_secs = longest.low_secs.max(chain.low_secs);
            longest.high_secs = longest.high_secs.max(chain.high_secs);
        }

        Ok(Some(normalize_estimate(EtaEstimate {
            expected_secs: longest
                .expected_secs
                .max(total.expected_secs / effective_concurrency),
            low_secs: longest.low_secs.max(total.low_secs / effective_concurrency),
            high_secs: longest
                .high_secs
                .max(total.high_secs / effective_concurrency),
            basis_samples: total.basis_samples,
            overdue: total.overdue,
        })))
    }
}

fn validate_sample(sample: &CompletedPhaseSample) -> Result<(), &'static str> {
    if sample.worker_threads == 0 {
        return Err("worker_threads is zero");
    }
    if sample.input_bytes == 0 {
        return Err("input_bytes is zero");
    }
    if sample.completed_unix_secs < sample.started_unix_secs {
        return Err("completion time is before start time");
    }
    if !sample.wall_secs.is_finite() || sample.wall_secs <= 0.0 {
        return Err("wall_secs is not finite and positive");
    }
    if !sample.paused_secs.is_finite()
        || sample.paused_secs < 0.0
        || sample.paused_secs > sample.wall_secs
    {
        return Err("paused_secs is outside 0..=wall_secs");
    }
    Ok(())
}

fn validate_work(work: &PhaseWork) -> Result<(), EtaError> {
    if work.worker_threads == 0 {
        return Err(EtaError::InvalidWork("worker_threads is zero"));
    }
    if work.input_bytes == 0 {
        return Err(EtaError::InvalidWork("input_bytes is zero"));
    }
    Ok(())
}

fn validate_elapsed(elapsed_secs: f64) -> Result<(), EtaError> {
    if !elapsed_secs.is_finite() || elapsed_secs < 0.0 {
        return Err(EtaError::InvalidWork(
            "elapsed_secs is not finite and nonnegative",
        ));
    }
    Ok(())
}

fn validate_queue_scalars(input: &QueueEtaInput) -> Result<(), EtaError> {
    if !input.stable_admission_gap_secs.is_finite() || input.stable_admission_gap_secs < 0.0 {
        return Err(EtaError::InvalidQueue(
            "stable admission gap is not finite and nonnegative",
        ));
    }
    if !input.effective_concurrency.is_finite() || input.effective_concurrency <= 0.0 {
        return Err(EtaError::InvalidQueue(
            "effective concurrency is not finite and positive",
        ));
    }
    Ok(())
}

fn quantile(sorted: &[f64], quantile: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    debug_assert!((0.0..=1.0).contains(&quantile));
    let position = quantile * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let fraction = position - lower as f64;
        sorted[lower] + (sorted[upper] - sorted[lower]) * fraction
    }
}

fn with_exact_delay(mut estimate: EtaEstimate, delay_secs: f64) -> EtaEstimate {
    estimate.expected_secs = cap_secs(estimate.expected_secs + delay_secs);
    estimate.low_secs = cap_secs(estimate.low_secs + delay_secs);
    estimate.high_secs = cap_secs(estimate.high_secs + delay_secs);
    estimate
}

fn add_component(total: &mut EtaEstimate, component: EtaEstimate) {
    total.expected_secs = cap_secs(total.expected_secs + component.expected_secs);
    total.low_secs = cap_secs(total.low_secs + component.low_secs);
    total.high_secs = cap_secs(total.high_secs + component.high_secs);
    total.basis_samples = total.basis_samples.min(component.basis_samples);
    total.overdue |= component.overdue;
}

fn cap_secs(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, MAX_ETA_SECS)
    } else {
        MAX_ETA_SECS
    }
}

fn normalize_estimate(mut estimate: EtaEstimate) -> EtaEstimate {
    estimate.expected_secs = cap_secs(estimate.expected_secs);
    estimate.low_secs = cap_secs(estimate.low_secs).min(estimate.expected_secs);
    estimate.high_secs = cap_secs(estimate.high_secs).max(estimate.expected_secs);
    estimate
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(
        epoch: u64,
        phase: EtaPhase,
        worker_threads: u32,
        input_bytes: u64,
        wall_secs: f64,
    ) -> CompletedPhaseSample {
        CompletedPhaseSample {
            epoch,
            phase,
            worker_threads,
            input_bytes,
            started_unix_secs: 1_000 + epoch,
            completed_unix_secs: 2_000 + epoch,
            wall_secs,
            paused_secs: 0.0,
        }
    }

    fn work(phase: EtaPhase, worker_threads: u32, input_bytes: u64) -> PhaseWork {
        PhaseWork {
            phase,
            worker_threads,
            input_bytes,
        }
    }

    #[test]
    fn robust_median_is_grouped_by_phase_and_thread_count() {
        let history = EtaHistory::new(vec![
            sample(1, EtaPhase::TargetBuild, 4, 10, 10.0),
            sample(2, EtaPhase::TargetBuild, 4, 10, 20.0),
            sample(3, EtaPhase::TargetBuild, 4, 10, 1_000.0),
            sample(4, EtaPhase::TargetBuild, 2, 10, 50.0),
            sample(5, EtaPhase::Parity, 4, 10, 70.0),
        ]);
        let estimator = EtaEstimator::from_history(&history).unwrap();

        let estimate = estimator
            .estimate_phase(&work(EtaPhase::TargetBuild, 4, 100))
            .unwrap()
            .unwrap();
        assert_eq!(estimate.expected_secs, 200.0);
        assert_eq!(estimate.basis_samples, 3);

        let two_threads = estimator
            .estimate_phase(&work(EtaPhase::TargetBuild, 2, 100))
            .unwrap()
            .unwrap();
        assert_eq!(two_threads.expected_secs, 500.0);

        let parity = estimator
            .estimate_phase(&work(EtaPhase::Parity, 4, 100))
            .unwrap()
            .unwrap();
        assert_eq!(parity.expected_secs, 700.0);
    }

    #[test]
    fn uncertainty_bounds_are_clamped_around_the_median() {
        let history = EtaHistory::new(vec![
            sample(1, EtaPhase::TargetBuild, 4, 100, 1.0),
            sample(2, EtaPhase::TargetBuild, 4, 100, 1.0),
            sample(3, EtaPhase::TargetBuild, 4, 100, 100.0),
            sample(4, EtaPhase::TargetBuild, 4, 100, 10_000.0),
            sample(5, EtaPhase::TargetBuild, 4, 100, 10_000.0),
        ]);
        let estimator = EtaEstimator::from_history(&history).unwrap();
        let estimate = estimator
            .estimate_phase(&work(EtaPhase::TargetBuild, 4, 100))
            .unwrap()
            .unwrap();

        assert_eq!(estimate.expected_secs, 100.0);
        assert_eq!(estimate.low_secs, 50.0);
        assert_eq!(estimate.high_secs, 200.0);
    }

    #[test]
    fn active_eta_subtracts_elapsed_time_and_marks_overrun() {
        let estimator = EtaEstimator::from_history(&EtaHistory::new(vec![sample(
            1,
            EtaPhase::TargetBuild,
            4,
            100,
            100.0,
        )]))
        .unwrap();
        let active = ActivePhaseWork {
            work: work(EtaPhase::TargetBuild, 4, 100),
            elapsed_secs: 30.0,
        };
        let remaining = estimator.estimate_active_phase(&active).unwrap().unwrap();
        assert_eq!(remaining.expected_secs, 70.0);
        assert!(!remaining.overdue);

        let overdue = estimator
            .estimate_active_phase(&ActivePhaseWork {
                elapsed_secs: 101.0,
                ..active
            })
            .unwrap()
            .unwrap();
        assert_eq!(overdue.expected_secs, 0.0);
        assert_eq!(overdue.high_secs, 0.0);
        assert!(overdue.overdue);
    }

    #[test]
    fn queue_adds_each_admission_gap_and_applies_effective_concurrency() {
        let estimator = EtaEstimator::from_history(&EtaHistory::new(vec![sample(
            1,
            EtaPhase::TargetBuild,
            4,
            100,
            100.0,
        )]))
        .unwrap();
        let item = QueuedItemWork {
            phases: vec![work(EtaPhase::TargetBuild, 4, 100)],
        };
        let one_worker = estimator
            .estimate_queue(&QueueEtaInput {
                active_items: vec![],
                queued_items: vec![item.clone(), item.clone()],
                stable_admission_gap_secs: 60.0,
                effective_concurrency: 1.0,
            })
            .unwrap()
            .unwrap();
        assert_eq!(one_worker.expected_secs, 320.0);

        let two_workers = estimator
            .estimate_queue(&QueueEtaInput {
                active_items: vec![],
                queued_items: vec![item.clone(), item],
                stable_admission_gap_secs: 60.0,
                effective_concurrency: 2.0,
            })
            .unwrap()
            .unwrap();
        assert_eq!(two_workers.expected_secs, 160.0);
    }

    #[test]
    fn serial_phases_remain_a_critical_path_with_two_workers() {
        let estimator = EtaEstimator::from_history(&EtaHistory::new(vec![sample(
            1,
            EtaPhase::TargetBuild,
            4,
            100,
            100.0,
        )]))
        .unwrap();
        let estimate = estimator
            .estimate_queue(&QueueEtaInput {
                active_items: vec![],
                queued_items: vec![QueuedItemWork {
                    phases: vec![
                        work(EtaPhase::TargetBuild, 4, 100),
                        work(EtaPhase::TargetBuild, 4, 100),
                    ],
                }],
                stable_admission_gap_secs: 60.0,
                effective_concurrency: 2.0,
            })
            .unwrap()
            .unwrap();
        assert_eq!(estimate.expected_secs, 320.0);
    }

    #[test]
    fn active_item_current_phase_has_no_new_gap_but_future_phase_does() {
        let estimator = EtaEstimator::from_history(&EtaHistory::new(vec![sample(
            1,
            EtaPhase::TargetBuild,
            4,
            100,
            100.0,
        )]))
        .unwrap();
        let estimate = estimator
            .estimate_queue(&QueueEtaInput {
                active_items: vec![ActiveItemWork {
                    current: ActivePhaseWork {
                        work: work(EtaPhase::TargetBuild, 4, 100),
                        elapsed_secs: 40.0,
                    },
                    future_phases: vec![work(EtaPhase::TargetBuild, 4, 100)],
                }],
                queued_items: vec![],
                stable_admission_gap_secs: 60.0,
                effective_concurrency: 1.0,
            })
            .unwrap()
            .unwrap();
        assert_eq!(estimate.expected_secs, 220.0);
    }

    #[test]
    fn missing_exact_calibration_group_returns_none() {
        let estimator = EtaEstimator::from_history(&EtaHistory::new(vec![sample(
            1,
            EtaPhase::TargetBuild,
            4,
            100,
            100.0,
        )]))
        .unwrap();
        assert!(
            estimator
                .estimate_phase(&work(EtaPhase::TargetBuild, 2, 100))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn durable_history_round_trips_and_rejects_invalid_samples() {
        let history = EtaHistory::new(vec![sample(
            301,
            EtaPhase::SourceControlBuild,
            4,
            123,
            45.0,
        )]);
        let encoded = serde_json::to_vec(&history).unwrap();
        let decoded: EtaHistory = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, history);
        decoded.validate().unwrap();

        let mut invalid = history;
        invalid.samples[0].input_bytes = 0;
        assert!(matches!(
            EtaEstimator::from_history(&invalid),
            Err(EtaError::InvalidSample { .. })
        ));
    }

    #[test]
    fn empty_queue_has_a_zero_eta_without_calibration() {
        let estimator = EtaEstimator::default();
        let estimate = estimator
            .estimate_queue(&QueueEtaInput {
                active_items: vec![],
                queued_items: vec![],
                stable_admission_gap_secs: 60.0,
                effective_concurrency: 1.0,
            })
            .unwrap()
            .unwrap();
        assert_eq!(estimate.expected_secs, 0.0);
        assert_eq!(estimate.basis_samples, 0);
    }

    #[test]
    fn recent_sample_cap_is_applied_per_group() {
        let samples = (0..MAX_SAMPLES_PER_GROUP + 10)
            .map(|epoch| sample(epoch as u64, EtaPhase::TargetBuild, 4, 100, 100.0))
            .collect();
        let estimator = EtaEstimator::from_history(&EtaHistory::new(samples)).unwrap();
        assert_eq!(
            estimator.sample_count(EtaPhase::TargetBuild, 4),
            MAX_SAMPLES_PER_GROUP
        );
    }
}
