use crate::hetzner::HetznerServerType;
use anyhow::{Result, anyhow};

#[derive(Debug, Clone)]
pub struct CapacityEstimateRequest {
    pub epoch_count: u64,
    pub target_hours: f64,
    pub hours_per_reference_epoch: f64,
    pub reference_input_gb: f64,
    pub input_gb_per_epoch: f64,
    pub output_gb_per_epoch: f64,
    pub builder_scratch_gb_per_epoch: f64,
    pub scratch_overhead_pct: f64,
    pub include_previous_car: bool,
    pub previous_car_gb: f64,
    pub max_machines: Option<u64>,
    pub hourly_billing: bool,
    pub machine: HetznerServerType,
}

#[derive(Debug, Clone)]
pub struct CapacityEstimate {
    pub epoch_count: u64,
    pub target_hours: f64,
    pub hours_per_reference_epoch: f64,
    pub reference_input_gb: f64,
    pub hours_per_epoch: f64,
    pub input_gb_per_epoch: f64,
    pub output_gb_per_epoch: f64,
    pub builder_scratch_gb_per_epoch: f64,
    pub scratch_required_gb: f64,
    pub disk_margin_gb: f64,
    pub disk_fits: bool,
    pub selected_machines: u64,
    pub machines_for_target: u64,
    pub parallelism_limit: u64,
    pub estimated_elapsed_hours: f64,
    pub total_machine_hours: f64,
    pub total_billable_hours: f64,
    pub total_cost_eur: f64,
    pub total_input_tb: f64,
    pub total_output_tb: f64,
    pub machine: HetznerServerType,
    pub machine_costs: Vec<MachineCostEstimate>,
}

#[derive(Debug, Clone)]
pub struct MachineCostEstimate {
    pub machine_index: u64,
    pub jobs: u64,
    pub runtime_hours: f64,
    pub billable_hours: f64,
    pub cost_eur: f64,
}

pub fn estimate_capacity(request: CapacityEstimateRequest) -> Result<CapacityEstimate> {
    anyhow::ensure!(
        request.epoch_count > 0,
        "epoch-count must be greater than zero"
    );
    ensure_positive(request.target_hours, "target-hours")?;
    ensure_positive(
        request.hours_per_reference_epoch,
        "hours-per-reference-epoch",
    )?;
    ensure_positive(request.reference_input_gb, "reference-input-gb")?;
    ensure_positive(request.input_gb_per_epoch, "input-gb-per-epoch")?;
    ensure_non_negative(request.output_gb_per_epoch, "output-gb-per-epoch")?;
    ensure_non_negative(
        request.builder_scratch_gb_per_epoch,
        "builder-scratch-gb-per-epoch",
    )?;
    ensure_non_negative(request.scratch_overhead_pct, "scratch-overhead-pct")?;
    ensure_non_negative(request.previous_car_gb, "previous-car-gb")?;

    let parallelism_limit = request.epoch_count;
    let hours_per_epoch =
        request.hours_per_reference_epoch * request.input_gb_per_epoch / request.reference_input_gb;
    let max_machines = request
        .max_machines
        .unwrap_or(parallelism_limit)
        .clamp(1, parallelism_limit);
    let machines_for_target = machines_for_target(&request, max_machines).unwrap_or(max_machines);
    let selected_machines = machines_for_target;
    let machine_costs = distribute_costs(&request, selected_machines);
    let estimated_elapsed_hours = machine_costs
        .iter()
        .map(|cost| cost.runtime_hours)
        .fold(0.0, f64::max);
    let total_machine_hours = request.epoch_count as f64 * hours_per_epoch;
    let total_billable_hours = machine_costs
        .iter()
        .map(|cost| cost.billable_hours)
        .sum::<f64>();
    let total_cost_eur = total_billable_hours * request.machine.hourly_eur;
    let scratch_required_gb = scratch_required_gb(&request);
    let disk_margin_gb = request.machine.local_disk_gb as f64 - scratch_required_gb;

    Ok(CapacityEstimate {
        epoch_count: request.epoch_count,
        target_hours: request.target_hours,
        hours_per_reference_epoch: request.hours_per_reference_epoch,
        reference_input_gb: request.reference_input_gb,
        hours_per_epoch,
        input_gb_per_epoch: request.input_gb_per_epoch,
        output_gb_per_epoch: request.output_gb_per_epoch,
        builder_scratch_gb_per_epoch: request.builder_scratch_gb_per_epoch,
        scratch_required_gb,
        disk_margin_gb,
        disk_fits: disk_margin_gb >= 0.0,
        selected_machines,
        machines_for_target,
        parallelism_limit,
        estimated_elapsed_hours,
        total_machine_hours,
        total_billable_hours,
        total_cost_eur,
        total_input_tb: request.epoch_count as f64 * request.input_gb_per_epoch / 1000.0,
        total_output_tb: request.epoch_count as f64 * request.output_gb_per_epoch / 1000.0,
        machine: request.machine,
        machine_costs,
    })
}

fn machines_for_target(request: &CapacityEstimateRequest, max_machines: u64) -> Option<u64> {
    (1..=max_machines).find(|machines| {
        distribute_costs(request, *machines)
            .iter()
            .map(|cost| cost.runtime_hours)
            .fold(0.0, f64::max)
            <= request.target_hours
    })
}

fn distribute_costs(
    request: &CapacityEstimateRequest,
    machine_count: u64,
) -> Vec<MachineCostEstimate> {
    let base_jobs = request.epoch_count / machine_count;
    let extra_jobs = request.epoch_count % machine_count;
    let hours_per_epoch =
        request.hours_per_reference_epoch * request.input_gb_per_epoch / request.reference_input_gb;

    (0..machine_count)
        .map(|machine_index| {
            let jobs = base_jobs + u64::from(machine_index < extra_jobs);
            let runtime_hours = jobs as f64 * hours_per_epoch;
            let billable_hours = if jobs == 0 {
                0.0
            } else if request.hourly_billing {
                runtime_hours.ceil()
            } else {
                runtime_hours
            };
            MachineCostEstimate {
                machine_index,
                jobs,
                runtime_hours,
                billable_hours,
                cost_eur: billable_hours * request.machine.hourly_eur,
            }
        })
        .collect()
}

fn scratch_required_gb(request: &CapacityEstimateRequest) -> f64 {
    let previous_car_gb = if request.include_previous_car {
        request.previous_car_gb
    } else {
        0.0
    };
    (request.input_gb_per_epoch
        + request.output_gb_per_epoch
        + request.builder_scratch_gb_per_epoch
        + previous_car_gb)
        * (1.0 + request.scratch_overhead_pct / 100.0)
}

fn ensure_positive(value: f64, name: &str) -> Result<()> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(anyhow!("{name} must be a positive finite number"))
    }
}

fn ensure_non_negative(value: f64, name: &str) -> Result<()> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(anyhow!("{name} must be a non-negative finite number"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hetzner::hetzner_server_type;

    #[test]
    fn estimates_machine_count_for_deadline() {
        let estimate = estimate_capacity(CapacityEstimateRequest {
            epoch_count: 964,
            target_hours: 24.0,
            hours_per_reference_epoch: 3.0,
            reference_input_gb: 390.8,
            input_gb_per_epoch: 390.8,
            output_gb_per_epoch: 163.6,
            builder_scratch_gb_per_epoch: 0.0,
            scratch_overhead_pct: 25.0,
            include_previous_car: false,
            previous_car_gb: 390.8,
            max_machines: None,
            hourly_billing: true,
            machine: hetzner_server_type("ccx63").unwrap(),
        })
        .unwrap();

        assert_eq!(estimate.selected_machines, 121);
        assert_eq!(estimate.estimated_elapsed_hours, 24.0);
        assert!(estimate.disk_fits);
    }

    #[test]
    fn detects_disk_that_is_too_small() {
        let estimate = estimate_capacity(CapacityEstimateRequest {
            epoch_count: 1,
            target_hours: 4.0,
            hours_per_reference_epoch: 3.0,
            reference_input_gb: 390.8,
            input_gb_per_epoch: 390.8,
            output_gb_per_epoch: 163.6,
            builder_scratch_gb_per_epoch: 0.0,
            scratch_overhead_pct: 25.0,
            include_previous_car: false,
            previous_car_gb: 390.8,
            max_machines: None,
            hourly_billing: true,
            machine: hetzner_server_type("ccx53").unwrap(),
        })
        .unwrap();

        assert!(!estimate.disk_fits);
        assert!(estimate.disk_margin_gb < 0.0);
    }
}
