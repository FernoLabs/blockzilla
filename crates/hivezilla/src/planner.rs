use crate::model::{
    BuildMode, HivezillaPlan, JobSpec, PLAN_VERSION, PlanStrategy, ProviderSpec, SLOTS_PER_EPOCH,
    WorkerSpec,
};
use anyhow::{Context, Result, anyhow};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone)]
pub struct PlanRequest {
    pub run_id: Option<String>,
    pub epochs: Vec<u64>,
    pub input_template: String,
    pub previous_car_template: Option<String>,
    pub output_template: String,
    pub worker_count: usize,
    pub provider: ProviderSpec,
    pub build_mode: BuildMode,
    pub chunk_slots: Option<u64>,
    pub compression_level: i32,
    pub slots_per_epoch: u64,
    pub old_faithful_base_url: Option<String>,
    pub labels: BTreeMap<String, String>,
}

pub fn build_plan(request: PlanRequest) -> Result<HivezillaPlan> {
    anyhow::ensure!(!request.epochs.is_empty(), "at least one epoch is required");
    anyhow::ensure!(
        request.worker_count > 0,
        "worker-count must be greater than zero"
    );
    anyhow::ensure!(
        request.slots_per_epoch > 0,
        "slots-per-epoch must be greater than zero"
    );

    let created_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs();
    let run_id = request
        .run_id
        .clone()
        .unwrap_or_else(|| format!("hivezilla-{created_unix_secs}"));
    let chunk_slots = chunk_slots_for(&request)?;
    let workers = build_workers(request.worker_count, request.provider.clone());
    let mut jobs = Vec::new();

    for epoch in request.epochs.iter().copied() {
        let ranges = slot_ranges_for_epoch(epoch, request.slots_per_epoch, chunk_slots)?;
        let shard_count = u32::try_from(ranges.len()).context("too many shards for u32")?;

        for (shard_index, (slot_start, slot_end)) in ranges.into_iter().enumerate() {
            let worker = &workers[jobs.len() % workers.len()];
            let shard_index_u32 = u32::try_from(shard_index).context("shard index exceeds u32")?;
            let source_uri = render_template(
                &request.input_template,
                epoch,
                shard_index_u32,
                slot_start,
                slot_end,
                &worker.id,
            );
            let previous_car_uri = previous_car_uri(&request, epoch, slot_start)?;
            let output_uri = render_output_template(
                &request.output_template,
                request.build_mode,
                epoch,
                shard_index_u32,
                slot_start,
                slot_end,
                &worker.id,
            );

            jobs.push(JobSpec {
                id: format!("epoch-{epoch}-shard-{shard_index_u32:03}"),
                worker_id: worker.id.clone(),
                build_mode: request.build_mode,
                epoch,
                shard_index: shard_index_u32,
                shard_count,
                slot_start,
                slot_end,
                source_uri,
                previous_car_uri,
                output_uri,
                compression_level: request.compression_level,
            });
        }
    }

    Ok(HivezillaPlan {
        version: PLAN_VERSION,
        run_id,
        created_unix_secs,
        strategy: PlanStrategy {
            build_mode: request.build_mode,
            slots_per_epoch: request.slots_per_epoch,
            chunk_slots,
            old_faithful_base_url: request.old_faithful_base_url,
        },
        workers,
        jobs,
        labels: request.labels,
    })
}

fn build_workers(worker_count: usize, provider: ProviderSpec) -> Vec<WorkerSpec> {
    (0..worker_count)
        .map(|worker_index| WorkerSpec {
            id: format!("hive-{worker_index:03}"),
            provider: provider.clone(),
        })
        .collect()
}

fn chunk_slots_for(request: &PlanRequest) -> Result<u64> {
    let chunk_slots = match (request.build_mode, request.chunk_slots) {
        (BuildMode::WholeEpoch | BuildMode::OldFaithfulStreamNoRegistry, Some(chunk_slots)) => {
            anyhow::ensure!(
                chunk_slots == request.slots_per_epoch,
                "chunk-slots smaller than slots-per-epoch requires --mode old-faithful-slot-slices"
            );
            request.slots_per_epoch
        }
        (BuildMode::WholeEpoch | BuildMode::OldFaithfulStreamNoRegistry, None) => {
            request.slots_per_epoch
        }
        (BuildMode::OldFaithfulSlotSlices, Some(chunk_slots)) => chunk_slots,
        (BuildMode::OldFaithfulSlotSlices, None) => request
            .slots_per_epoch
            .div_ceil(request.worker_count as u64),
    };

    anyhow::ensure!(chunk_slots > 0, "chunk-slots must be greater than zero");
    anyhow::ensure!(
        chunk_slots <= request.slots_per_epoch,
        "chunk-slots cannot exceed slots-per-epoch"
    );
    Ok(chunk_slots)
}

fn slot_ranges_for_epoch(
    epoch: u64,
    slots_per_epoch: u64,
    chunk_slots: u64,
) -> Result<Vec<(u64, u64)>> {
    let epoch_start = epoch
        .checked_mul(slots_per_epoch)
        .ok_or_else(|| anyhow!("epoch start slot overflow"))?;
    let epoch_end = epoch_start
        .checked_add(slots_per_epoch - 1)
        .ok_or_else(|| anyhow!("epoch end slot overflow"))?;
    let mut ranges = Vec::new();
    let mut start = epoch_start;

    while start <= epoch_end {
        let end = start
            .checked_add(chunk_slots - 1)
            .ok_or_else(|| anyhow!("slot range end overflow"))?
            .min(epoch_end);
        ranges.push((start, end));
        if end == epoch_end {
            break;
        }
        start = end
            .checked_add(1)
            .ok_or_else(|| anyhow!("next slot overflow"))?;
    }

    Ok(ranges)
}

fn previous_car_uri(request: &PlanRequest, epoch: u64, slot_start: u64) -> Result<Option<String>> {
    let Some(template) = request.previous_car_template.as_deref() else {
        return Ok(None);
    };
    let epoch_start = epoch
        .checked_mul(request.slots_per_epoch)
        .ok_or_else(|| anyhow!("epoch start slot overflow"))?;

    if slot_start != epoch_start || epoch == 0 {
        return Ok(None);
    }

    let previous_epoch = epoch - 1;
    let previous_epoch_start = previous_epoch
        .checked_mul(request.slots_per_epoch)
        .ok_or_else(|| anyhow!("previous epoch start slot overflow"))?;
    Ok(Some(render_template(
        template,
        previous_epoch,
        0,
        previous_epoch_start,
        previous_epoch_start + request.slots_per_epoch - 1,
        "",
    )))
}

fn render_output_template(
    template: &str,
    build_mode: BuildMode,
    epoch: u64,
    shard_index: u32,
    slot_start: u64,
    slot_end: u64,
    worker_id: &str,
) -> String {
    let rendered = render_template(
        template,
        epoch,
        shard_index,
        slot_start,
        slot_end,
        worker_id,
    );
    if build_mode == BuildMode::OldFaithfulSlotSlices
        && !template.contains("{shard}")
        && !template.contains("{start_slot}")
        && !template.contains("{end_slot}")
    {
        format!("{}/shard-{shard_index:03}", rendered.trim_end_matches('/'))
    } else {
        rendered
    }
}

pub fn render_template(
    template: &str,
    epoch: u64,
    shard_index: u32,
    slot_start: u64,
    slot_end: u64,
    worker_id: &str,
) -> String {
    template
        .replace("{epoch}", &epoch.to_string())
        .replace("{shard}", &format!("{shard_index:03}"))
        .replace("{start_slot}", &slot_start.to_string())
        .replace("{end_slot}", &slot_end.to_string())
        .replace("{worker}", worker_id)
}

impl Default for PlanRequest {
    fn default() -> Self {
        Self {
            run_id: None,
            epochs: Vec::new(),
            input_template: "/data/old-faithful/epoch-{epoch}.car.zst".to_string(),
            previous_car_template: None,
            output_template: "/data/blockzilla/epoch-{epoch}".to_string(),
            worker_count: 1,
            provider: ProviderSpec::default(),
            build_mode: BuildMode::WholeEpoch,
            chunk_slots: None,
            compression_level: 1,
            slots_per_epoch: SLOTS_PER_EPOCH,
            old_faithful_base_url: None,
            labels: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_epoch_plan_assigns_one_job_per_epoch() {
        let plan = build_plan(PlanRequest {
            epochs: vec![900, 901],
            worker_count: 2,
            ..PlanRequest::default()
        })
        .unwrap();

        assert_eq!(plan.jobs.len(), 2);
        assert_eq!(plan.jobs[0].worker_id, "hive-000");
        assert_eq!(plan.jobs[1].worker_id, "hive-001");
        assert_eq!(plan.jobs[0].slot_start, 388_800_000);
        assert_eq!(plan.jobs[0].slot_end, 389_231_999);
    }

    #[test]
    fn slot_slice_plan_splits_epoch_into_shards() {
        let plan = build_plan(PlanRequest {
            epochs: vec![1],
            worker_count: 2,
            build_mode: BuildMode::OldFaithfulSlotSlices,
            chunk_slots: Some(200_000),
            output_template: "r2:blockzilla/epoch-{epoch}".to_string(),
            ..PlanRequest::default()
        })
        .unwrap();

        assert_eq!(plan.jobs.len(), 3);
        assert_eq!(plan.jobs[0].slot_start, 432_000);
        assert_eq!(plan.jobs[0].slot_end, 631_999);
        assert_eq!(plan.jobs[1].slot_start, 632_000);
        assert_eq!(plan.jobs[1].slot_end, 831_999);
        assert_eq!(plan.jobs[2].slot_start, 832_000);
        assert_eq!(plan.jobs[2].slot_end, 863_999);
        assert_eq!(plan.jobs[0].output_uri, "r2:blockzilla/epoch-1/shard-000");
    }

    #[test]
    fn template_rendering_uses_worker_and_slot_values() {
        let rendered = render_template(
            "epoch-{epoch}/shard-{shard}/{start_slot}-{end_slot}/{worker}",
            7,
            3,
            10,
            20,
            "hive-003",
        );

        assert_eq!(rendered, "epoch-7/shard-003/10-20/hive-003");
    }

    #[test]
    fn whole_epoch_plan_rejects_sub_epoch_chunks() {
        let err = build_plan(PlanRequest {
            epochs: vec![900],
            chunk_slots: Some(54_000),
            ..PlanRequest::default()
        })
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("requires --mode old-faithful-slot-slices")
        );
    }
}
