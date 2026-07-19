//! Disabled-by-default scheduler model for moving derived live indexing out of
//! the long-lived gRPC recorder.
//!
//! This module deliberately has no executor or runtime hook yet. Enabling a
//! task before the raw WAL has a read-only committed-prefix reader would risk
//! racing the recorder or publishing a partial capture. The intended seams are:
//!
//! 1. Inventory emits one [`LiveMaterializeTask`] only for a closed epoch in a
//!    verified, immutable WAL prefix.
//! 2. A finite `materialize-grpc-raw-epoch` child replays that prefix into a
//!    hidden staging directory, compacts derived runs with bounded buffers,
//!    publishes a source-bound completion receipt last, and atomically renames
//!    the result.
//! 3. Hivezilla owns task/lane state, progress, pause/resume, retry, and memory
//!    admission. The recorder owns only transport health and durable append.
//! 4. Scheduling consumes this module's start decisions and then continues to
//!    independent work. A deferred materializer is never an exclusivity hold
//!    and must never cause an early return before historical compaction.

use std::collections::BTreeSet;

pub(crate) const LIVE_MATERIALIZER_LANE_KIND: &str = "live_materializer";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LiveMaterializeTaskId {
    pub(crate) source_id: String,
    pub(crate) epoch: u64,
}

impl LiveMaterializeTaskId {
    pub(crate) fn new(source_id: impl Into<String>, epoch: u64) -> Option<Self> {
        let source_id = source_id.into();
        let safe = !source_id.is_empty()
            && source_id.len() <= 128
            && source_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        safe.then_some(Self { source_id, epoch })
    }

    pub(crate) fn lane_id(&self) -> String {
        format!("live_materialize:{}:{}", self.source_id, self.epoch)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveMaterializePhase {
    /// Replay a verified, committed raw-WAL epoch into bounded derived runs.
    ReplayCommittedWal,
    /// Merge those runs into the capture layout consumed by live finalizers.
    CompactDerivedRuns,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveMaterializeTaskState {
    RawReady,
    Materializing,
    RepairGate,
    ReadyToPackage,
    Packaging,
    Packaged,
    Complete,
    Failed,
    Blocked,
}

impl LiveMaterializeTaskState {
    fn is_startable(self) -> bool {
        self == Self::RawReady
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveMaterializeTask {
    pub(crate) id: LiveMaterializeTaskId,
    pub(crate) phase: LiveMaterializePhase,
    pub(crate) state: LiveMaterializeTaskState,
    /// Peak task RSS reservation, measured/tuned per phase once benchmarks
    /// exist. Zero is rejected rather than silently running unbounded.
    pub(crate) estimated_peak_memory_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveMaterializerLaneState {
    Running,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveMaterializerLane {
    pub(crate) task_id: LiveMaterializeTaskId,
    pub(crate) state: LiveMaterializerLaneState,
    /// Missing RSS telemetry reserves the full peak estimate. A paused process
    /// remains resident, so it is admitted exactly like a running process.
    pub(crate) rss_bytes: Option<u64>,
    pub(crate) estimated_peak_memory_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveMaterializePolicy {
    /// The migration stays inert until the WAL reader, executor, receipts, and
    /// shadow comparison all exist and the operator explicitly enables it.
    pub(crate) enabled: bool,
    pub(crate) max_lanes: usize,
    pub(crate) memory_reserve_bytes: u64,
}

impl Default for LiveMaterializePolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            max_lanes: 1,
            memory_reserve_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveMaterializeResources {
    /// `None` means the controller cannot prove memory headroom. The new task
    /// fails closed in that case, while independent scheduler work continues.
    pub(crate) memory_available_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LiveMaterializeDeferral {
    FeatureDisabled,
    AlreadyActive,
    StateNotStartable(LiveMaterializeTaskState),
    LaneCapacity,
    MissingMemoryEstimate,
    MemoryTelemetryUnavailable,
    InsufficientMemory {
        headroom_bytes: u64,
        required_bytes: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LiveMaterializeAdmission {
    Start,
    Deferred(LiveMaterializeDeferral),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveMaterializeTaskDecision {
    pub(crate) id: LiveMaterializeTaskId,
    pub(crate) admission: LiveMaterializeAdmission,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveMaterializePlan {
    pub(crate) decisions: Vec<LiveMaterializeTaskDecision>,
}

impl LiveMaterializePlan {
    /// Live materialization is independent finite work. Neither a disabled,
    /// blocked, failed, paused, nor memory-deferred task owns an exclusivity
    /// edge over historical compaction.
    pub(crate) const fn blocks_historical_compaction(&self) -> bool {
        false
    }

    pub(crate) fn starts(&self) -> impl Iterator<Item = &LiveMaterializeTaskId> {
        self.decisions.iter().filter_map(|decision| {
            matches!(decision.admission, LiveMaterializeAdmission::Start).then_some(&decision.id)
        })
    }
}

pub(crate) fn plan_live_materialize_pass(
    policy: &LiveMaterializePolicy,
    resources: LiveMaterializeResources,
    tasks: &[LiveMaterializeTask],
    lanes: &[LiveMaterializerLane],
) -> LiveMaterializePlan {
    if !policy.enabled {
        return LiveMaterializePlan {
            decisions: tasks
                .iter()
                .map(|task| LiveMaterializeTaskDecision {
                    id: task.id.clone(),
                    admission: LiveMaterializeAdmission::Deferred(
                        LiveMaterializeDeferral::FeatureDisabled,
                    ),
                })
                .collect(),
        };
    }

    let mut active_ids = BTreeSet::new();
    let mut active_future_growth = 0u64;
    for lane in lanes {
        if active_ids.insert(lane.task_id.clone()) {
            // SIGSTOP does not release RSS. Both lane states reserve possible
            // future growth; the match also makes that policy explicit.
            match lane.state {
                LiveMaterializerLaneState::Running | LiveMaterializerLaneState::Paused => {
                    active_future_growth = active_future_growth.saturating_add(
                        lane.estimated_peak_memory_bytes
                            .saturating_sub(lane.rss_bytes.unwrap_or(0)),
                    );
                }
            }
        }
    }

    let mut remaining_slots = policy.max_lanes.saturating_sub(active_ids.len());
    let mut memory_headroom = resources.memory_available_bytes.map(|available| {
        available
            .saturating_sub(policy.memory_reserve_bytes)
            .saturating_sub(active_future_growth)
    });
    let mut decisions = Vec::with_capacity(tasks.len());

    // Preserve inventory priority, but continue past every inadmissible head.
    // That makes the materializer queue work-conserving internally as well as
    // non-blocking for historical work.
    for task in tasks {
        let admission = if active_ids.contains(&task.id) {
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::AlreadyActive)
        } else if !task.state.is_startable() {
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::StateNotStartable(
                task.state,
            ))
        } else if task.estimated_peak_memory_bytes == 0 {
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::MissingMemoryEstimate)
        } else if remaining_slots == 0 {
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::LaneCapacity)
        } else if let Some(headroom) = memory_headroom {
            if headroom < task.estimated_peak_memory_bytes {
                LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::InsufficientMemory {
                    headroom_bytes: headroom,
                    required_bytes: task.estimated_peak_memory_bytes,
                })
            } else {
                remaining_slots = remaining_slots.saturating_sub(1);
                memory_headroom = Some(headroom.saturating_sub(task.estimated_peak_memory_bytes));
                active_ids.insert(task.id.clone());
                LiveMaterializeAdmission::Start
            }
        } else {
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::MemoryTelemetryUnavailable)
        };
        decisions.push(LiveMaterializeTaskDecision {
            id: task.id.clone(),
            admission,
        });
    }

    LiveMaterializePlan { decisions }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mib(value: u64) -> u64 {
        value * 1024 * 1024
    }

    fn id(source: &str, epoch: u64) -> LiveMaterializeTaskId {
        LiveMaterializeTaskId::new(source, epoch).unwrap()
    }

    fn task(
        source: &str,
        epoch: u64,
        state: LiveMaterializeTaskState,
        memory_mib: u64,
    ) -> LiveMaterializeTask {
        LiveMaterializeTask {
            id: id(source, epoch),
            phase: LiveMaterializePhase::ReplayCommittedWal,
            state,
            estimated_peak_memory_bytes: mib(memory_mib),
        }
    }

    #[test]
    fn model_is_disabled_by_default_and_never_blocks_historical_compaction() {
        let plan = plan_live_materialize_pass(
            &LiveMaterializePolicy::default(),
            LiveMaterializeResources {
                memory_available_bytes: Some(mib(8_192)),
            },
            &[task(
                "grpc-primary",
                1_002,
                LiveMaterializeTaskState::RawReady,
                512,
            )],
            &[],
        );

        assert!(plan.starts().next().is_none());
        assert_eq!(
            plan.decisions[0].admission,
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::FeatureDisabled)
        );
        assert!(!plan.blocks_historical_compaction());
    }

    #[test]
    fn blocked_and_too_large_heads_yield_to_a_smaller_runnable_task() {
        let policy = LiveMaterializePolicy {
            enabled: true,
            max_lanes: 1,
            memory_reserve_bytes: mib(1_024),
        };
        let tasks = [
            task(
                "grpc-primary",
                1_004,
                LiveMaterializeTaskState::Blocked,
                128,
            ),
            task(
                "grpc-primary",
                1_003,
                LiveMaterializeTaskState::RawReady,
                2_048,
            ),
            task(
                "grpc-backup",
                1_003,
                LiveMaterializeTaskState::RawReady,
                256,
            ),
        ];
        let plan = plan_live_materialize_pass(
            &policy,
            LiveMaterializeResources {
                memory_available_bytes: Some(mib(1_512)),
            },
            &tasks,
            &[],
        );

        assert_eq!(
            plan.starts().cloned().collect::<Vec<_>>(),
            vec![id("grpc-backup", 1_003)]
        );
        assert!(matches!(
            plan.decisions[0].admission,
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::StateNotStartable(
                LiveMaterializeTaskState::Blocked
            ))
        ));
        assert!(matches!(
            plan.decisions[1].admission,
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::InsufficientMemory { .. })
        ));
        assert!(!plan.blocks_historical_compaction());
    }

    #[test]
    fn paused_lane_keeps_its_future_growth_reservation() {
        let policy = LiveMaterializePolicy {
            enabled: true,
            max_lanes: 2,
            memory_reserve_bytes: mib(100),
        };
        let paused = LiveMaterializerLane {
            task_id: id("grpc-primary", 1_002),
            state: LiveMaterializerLaneState::Paused,
            rss_bytes: Some(mib(100)),
            estimated_peak_memory_bytes: mib(500),
        };
        let candidate = task(
            "grpc-backup",
            1_002,
            LiveMaterializeTaskState::RawReady,
            500,
        );
        let plan = plan_live_materialize_pass(
            &policy,
            LiveMaterializeResources {
                memory_available_bytes: Some(mib(900)),
            },
            &[candidate],
            &[paused],
        );

        assert!(plan.starts().next().is_none());
        assert_eq!(
            plan.decisions[0].admission,
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::InsufficientMemory {
                headroom_bytes: mib(400),
                required_bytes: mib(500),
            })
        );
        assert!(!plan.blocks_historical_compaction());
    }

    #[test]
    fn unknown_memory_fails_closed_only_for_materialization() {
        let policy = LiveMaterializePolicy {
            enabled: true,
            max_lanes: 1,
            memory_reserve_bytes: mib(1_024),
        };
        let plan = plan_live_materialize_pass(
            &policy,
            LiveMaterializeResources {
                memory_available_bytes: None,
            },
            &[task(
                "grpc-primary",
                1_002,
                LiveMaterializeTaskState::RawReady,
                512,
            )],
            &[],
        );

        assert_eq!(
            plan.decisions[0].admission,
            LiveMaterializeAdmission::Deferred(LiveMaterializeDeferral::MemoryTelemetryUnavailable)
        );
        assert!(!plan.blocks_historical_compaction());
    }

    #[test]
    fn task_identity_is_stable_and_path_safe() {
        let task_id = id("grpc-primary.eu_1", 1_002);
        assert_eq!(task_id.lane_id(), "live_materialize:grpc-primary.eu_1:1002");
        assert_eq!(LIVE_MATERIALIZER_LANE_KIND, "live_materializer");
        assert!(LiveMaterializeTaskId::new("../grpc", 1_002).is_none());
        assert!(LiveMaterializeTaskId::new("grpc:primary", 1_002).is_none());
    }
}
