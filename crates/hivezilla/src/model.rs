use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const PLAN_VERSION: u32 = 1;
pub const SLOTS_PER_EPOCH: u64 = 432_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
#[value(rename_all = "kebab-case")]
pub enum BuildMode {
    WholeEpoch,
    OldFaithfulStreamNoRegistry,
    OldFaithfulSlotSlices,
}

impl BuildMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WholeEpoch => "whole-epoch",
            Self::OldFaithfulStreamNoRegistry => "old-faithful-stream-no-registry",
            Self::OldFaithfulSlotSlices => "old-faithful-slot-slices",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
#[value(rename_all = "kebab-case")]
pub enum ProviderKind {
    Manual,
    Hetzner,
    AwsEc2,
    GcpCompute,
    AzureVm,
    VastAi,
    RunPod,
    LambdaLabs,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MachineSpec {
    pub cpu_cores: u16,
    pub memory_gib: u16,
    pub disk_gib: u32,
    pub disk_kind: String,
}

impl Default for MachineSpec {
    fn default() -> Self {
        Self {
            cpu_cores: 16,
            memory_gib: 64,
            disk_gib: 2_000,
            disk_kind: "nvme".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSpec {
    pub kind: ProviderKind,
    pub region: Option<String>,
    pub machine_type: Option<String>,
    pub image: Option<String>,
    pub max_price_per_hour_usd: Option<f64>,
    pub machine: MachineSpec,
}

impl Default for ProviderSpec {
    fn default() -> Self {
        Self {
            kind: ProviderKind::Manual,
            region: None,
            machine_type: None,
            image: None,
            max_price_per_hour_usd: None,
            machine: MachineSpec::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerSpec {
    pub id: String,
    pub provider: ProviderSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSpec {
    pub id: String,
    pub worker_id: String,
    pub build_mode: BuildMode,
    pub epoch: u64,
    pub shard_index: u32,
    pub shard_count: u32,
    pub slot_start: u64,
    pub slot_end: u64,
    pub source_uri: String,
    pub previous_car_uri: Option<String>,
    pub output_uri: String,
    pub compression_level: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanStrategy {
    pub build_mode: BuildMode,
    pub slots_per_epoch: u64,
    pub chunk_slots: u64,
    pub old_faithful_base_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HivezillaPlan {
    pub version: u32,
    pub run_id: String,
    pub created_unix_secs: u64,
    pub strategy: PlanStrategy,
    pub workers: Vec<WorkerSpec>,
    pub jobs: Vec<JobSpec>,
    pub labels: BTreeMap<String, String>,
}

impl HivezillaPlan {
    pub fn jobs_for_worker<'a>(&'a self, worker_id: &str) -> Vec<&'a JobSpec> {
        self.jobs
            .iter()
            .filter(|job| job.worker_id == worker_id)
            .collect()
    }
}
