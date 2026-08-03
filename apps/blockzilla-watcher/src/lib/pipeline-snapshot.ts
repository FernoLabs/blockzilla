import {
  type CompactionHistoryEntry,
  type CompactionWorkflow
} from '$lib/compaction-history';
import {
  hasUniqueEpochCalendarEpochs,
  type EpochCalendarEntry
} from '$lib/epoch-calendar';
import type { ProcessIoEntry } from '$lib/process-telemetry';
import type { SnapshotPatch } from '$lib/snapshot-patch';

export type HistoricalState =
  | 'queued'
  | 'scanning'
  | 'scan_ready'
  | 'finalizing'
  | 'complete'
  | 'failed'
  | 'blocked';

export type LiveState =
  | 'capturing'
  | 'repair_gate'
  | 'repair_required'
  | 'ready_to_package'
  | 'packaging'
  | 'packaged'
  | 'complete'
  | 'failed'
  | 'blocked';

export type ArtifactStatus = {
  kind: string;
  state: string;
  requirement: string;
  required_now: boolean;
  bytes: number;
  modified_unix_secs: number | null;
  message: string | null;
};

export type SchedulerStageSnapshot = {
  state?: string | null;
  phase?: string | null;
  completed?: number | null;
  done?: number | null;
  epochs_done?: number | null;
  scanned?: number | null;
  observed?: number | null;
  total?: number | null;
  epochs_total?: number | null;
  progress_pct?: number | null;
  current_epoch?: number | null;
  complete?: boolean | null;
  message?: string | null;
  updated_unix_secs?: number | null;
  epochs_discovered?: number | null;
  epochs_classified?: number | null;
  errors?: unknown[];
  pending?: number | null;
  active?: number | null;
  terminal_gaps?: number | null;
  deferred_finalizers?: number | null;
  blocked_reason?: string | null;
  wait_reason?: string | null;
  [key: string]: unknown;
};

export type ProgressSnapshot = {
  phase: string | null;
  state: string | null;
  pid: number | null;
  blocks_done: number;
  blocks_total: number;
  transactions_done: number;
  first_slot: number | null;
  last_slot: number | null;
  progress_pct: number | null;
  blocks_per_sec: number | null;
  input_mib_per_sec: number | null;
  disk_read_mib_per_sec?: number | null;
  disk_write_mib_per_sec?: number | null;
  eta_secs: number | null;
  rss_bytes: number | null;
  peak_rss_bytes?: number | null;
  updated_unix_secs: number | null;
  elapsed_secs: number | null;
};

export type EpochStatus = {
  epoch: number;
  state: HistoricalState;
  registry_order: 'usage_sorted' | 'first_seen' | 'unknown';
  input_path: string | null;
  output_path: string;
  car_bytes: number;
  progress: ProgressSnapshot;
  artifacts?: ArtifactStatus[];
  updated_unix_secs: number;
  message: string | null;
};

export type LaneStatus = {
  id: string;
  kind: string;
  epoch: number | null;
  capture_id: string | null;
  phase: string;
  state: string;
  pid: number | null;
  progress: ProgressSnapshot;
  rss_bytes: number | null;
  started_unix_secs: number | null;
  updated_unix_secs: number;
  auto_paused?: boolean;
  auto_pause_reason?: string | null;
};

export type LiveStatus = {
  id: string;
  epoch: number | null;
  is_current?: boolean;
  state: LiveState;
  capture_dir: string;
  output_path: string | null;
  ready_to_package: boolean;
  repair_gate: boolean;
  source_capture_ids?: string[];
  superseded_by?: string | null;
  first_slot: number | null;
  last_slot: number | null;
  blocks_written: number;
  eta_secs?: number | null;
  slots_per_sec?: number | null;
  rss_bytes?: number | null;
  peak_rss_bytes?: number | null;
  progress: ProgressSnapshot;
  artifacts?: ArtifactStatus[];
  message: string | null;
  updated_unix_secs: number;
};

export type FinalizerItem = {
  kind: string;
  epoch: number | null;
  id: string;
  phase?: string;
  state: string;
  estimated_memory_bytes?: number;
  deferred_reason?: string | null;
};

export type MachineStatus = {
  memory_total_bytes: number;
  memory_used_bytes: number;
  memory_available_bytes: number;
  swap_total_bytes: number;
  swap_used_bytes: number;
  disk_total_bytes: number;
  disk_used_bytes: number;
  disk_available_bytes: number;
  car_disk_total_bytes?: number;
  car_disk_used_bytes?: number;
  car_disk_available_bytes?: number;
  car_disk_shared_with_archive?: boolean;
  archive_device_major?: number | null;
  archive_device_minor?: number | null;
  archive_device_name?: string | null;
  archive_device_read_mib_per_sec?: number | null;
  archive_device_write_mib_per_sec?: number | null;
  load_1m: number;
  service_rss_bytes: number;
  children_rss_bytes: number;
  memory_pressure_some_avg10?: number | null;
  memory_pressure_full_avg10?: number | null;
  io_pressure_some_avg10?: number | null;
  io_pressure_full_avg10?: number | null;
};

export type ProcessIoSnapshot = {
  state: 'collecting' | 'ready' | 'unavailable';
  sampled_unix_secs: number | null;
  sample_window_secs: number | null;
  active_count: number;
  inaccessible_count: number;
  truncated: boolean;
  processes: ProcessIoEntry[];
  message?: string | null;
};

export type PipelineError = {
  at_unix_secs: number;
  scope: string;
  message: string;
};

export type PipelineSummary = {
  epochs_total: number;
  queued: number;
  scanning: number;
  scan_ready: number;
  finalizing: number;
  complete: number;
  failed: number;
  blocked: number;
  progress_pct: number;
  eta_secs: number | null;
  queue_eta_secs?: number | null;
  queue_eta_reason?: string | null;
  queue_jobs_remaining?: number;
  queue_capacity?: number;
  queue_job_duration_secs?: number | null;
  queue_duration_samples?: number;
  queue_bytes_remaining?: number;
  queue_read_mib_per_sec?: number | null;
  queue_read_active_workers?: number;
  queue_read_sampled_workers?: number;
  blocks_done: number;
  blocks_total: number;
  blocks_per_sec: number;
  disk_read_mib_per_sec?: number | null;
  disk_write_mib_per_sec?: number | null;
  disk_io_active_roots?: number;
  disk_io_sampled_roots?: number;
  scan_eta_secs?: number | null;
  scan_capacity_configured: number;
  scan_capacity_admitted: number;
  admission_blocked_reason: string | null;
  finalizer_admission_blocked_reason?: string | null;
  legacy_compact_running?: number;
  legacy_compact_paused?: number;
  legacy_compact_auto_paused?: number;
  legacy_compact_capacity_configured?: number;
  legacy_compact_capacity_unbounded?: boolean;
  legacy_compact_capacity_effective?: number;
  legacy_compact_capacity_admitted?: number;
  legacy_compact_tuning_enabled?: boolean;
  legacy_compact_tuning_state?: string | null;
  legacy_compact_tuning_target?: number;
  legacy_compact_tuning_accepted_lanes?: number;
  legacy_compact_tuning_baseline_mib_per_sec?: number | null;
  legacy_compact_tuning_objective_mib_per_sec?: number | null;
  legacy_compact_tuning_rate_source?: string | null;
  legacy_compact_useful_input_mib_per_sec?: number | null;
  legacy_compact_useful_input_active_lanes?: number;
  legacy_compact_useful_input_sampled_lanes?: number;
  legacy_compact_tuning_backoff_until_unix_secs?: number | null;
  legacy_compact_tuning_last_decision?: string | null;
  legacy_compact_admission_blocked_reason?: string | null;
  legacy_compact_auto_pause_enabled?: boolean;
  legacy_compact_min_running?: number;
  legacy_compact_memory_guard_mib?: number;
  legacy_compact_memory_pause_available_mib?: number;
  legacy_compact_memory_resume_available_mib?: number;
  legacy_compact_io_pause_full_avg10?: number;
  legacy_compact_io_resume_full_avg10?: number;
  legacy_compact_cpu_budget_cores?: number;
  legacy_compact_pause_cooldown_secs?: number;
  legacy_compact_last_action?: string | null;
  legacy_compact_last_action_unix_secs?: number | null;
};

export type SchedulerSnapshot = {
  paused: boolean;
  updated_unix_secs: number;
  inventory?: SchedulerStageSnapshot | null;
  scan_sweep?: SchedulerStageSnapshot | null;
};

export type PipelineSnapshot = {
  schema_version: number;
  sequence: number;
  now_unix_secs: number;
  current_epoch?: number | null;
  observer_mode: boolean;
  scheduler: SchedulerSnapshot;
  inventory?: SchedulerStageSnapshot | null;
  scan_sweep?: SchedulerStageSnapshot | null;
  summary: PipelineSummary;
  machine: MachineStatus;
  live: LiveStatus[];
  epochs: EpochStatus[];
  lanes: LaneStatus[];
  finalizer_queue: FinalizerItem[];
  errors: PipelineError[];
  epoch_calendar?: EpochCalendarEntry[];
  recent_compactions?: CompactionHistoryEntry[];
  process_io?: ProcessIoSnapshot;
};

export type PipelineSnapshotPatch = SnapshotPatch<PipelineSnapshot, EpochStatus> & {
  current_epoch: number | null;
  inventory: SchedulerStageSnapshot | null;
  scan_sweep: SchedulerStageSnapshot | null;
};

export type JsonRecord = Record<string, unknown>;

export function parsePipelineSnapshot(value: unknown): PipelineSnapshot | null {
  const root = asRecord(value);
  if (
    !root ||
    integerValue(root.schema_version) === null ||
    integerValue(root.sequence) === null ||
    integerValue(root.now_unix_secs) === null ||
    typeof root.observer_mode !== 'boolean' ||
    !asRecord(root.scheduler) ||
    !asRecord(root.summary) ||
    !asRecord(root.machine) ||
    !Array.isArray(root.epochs) || !root.epochs.every(isEpochStatusValue) ||
    !Array.isArray(root.lanes) || !root.lanes.every(isLaneStatusValue) ||
    !Array.isArray(root.live) || !root.live.every(isLiveStatusValue) ||
    !Array.isArray(root.finalizer_queue) || !root.finalizer_queue.every(isFinalizerItemValue) ||
    !Array.isArray(root.errors) || !root.errors.every(isPipelineErrorValue) ||
    (root.epoch_calendar !== undefined && !isEpochCalendarValue(root.epoch_calendar)) ||
    (root.recent_compactions !== undefined && !isCompactionHistoryValue(root.recent_compactions)) ||
    (root.process_io !== undefined && !isProcessIoSnapshotValue(root.process_io))
  ) {
    return null;
  }
  return value as PipelineSnapshot;
}

export function parsePipelineSnapshotPatch(value: unknown): PipelineSnapshotPatch | null {
  const root = asRecord(value);
  if (!root) return null;

  const schemaVersion = integerValue(root.schema_version);
  const sequence = integerValue(root.sequence);
  const nowUnixSecs = integerValue(root.now_unix_secs);
  const currentEpoch = root.current_epoch === null ? null : integerValue(root.current_epoch);
  const scheduler = asRecord(root.scheduler);
  const inventory = root.inventory === null ? null : asRecord(root.inventory);
  const scanSweep = root.scan_sweep === null ? null : asRecord(root.scan_sweep);
  const summary = asRecord(root.summary);
  const machine = asRecord(root.machine);
  const changed = Array.isArray(root.epochs_changed) ? root.epochs_changed : null;
  const removed = Array.isArray(root.epochs_removed) ? root.epochs_removed : null;
  const lanes = Array.isArray(root.lanes) ? root.lanes : null;
  const live = Array.isArray(root.live) ? root.live : null;
  const finalizerQueue = Array.isArray(root.finalizer_queue) ? root.finalizer_queue : null;
  const errors = Array.isArray(root.errors) ? root.errors : null;
  const epochCalendar = root.epoch_calendar === undefined
    ? undefined
    : isEpochCalendarValue(root.epoch_calendar)
      ? root.epoch_calendar
      : null;
  const recentCompactions = root.recent_compactions === undefined
    ? undefined
    : isCompactionHistoryValue(root.recent_compactions)
      ? root.recent_compactions
      : null;
  const processIo = root.process_io === undefined
    ? undefined
    : isProcessIoSnapshotValue(root.process_io)
      ? root.process_io
      : null;

  if (
    schemaVersion === null ||
    sequence === null ||
    nowUnixSecs === null ||
    !('current_epoch' in root) ||
    (root.current_epoch !== null && currentEpoch === null) ||
    typeof root.observer_mode !== 'boolean' ||
    !scheduler ||
    typeof scheduler.paused !== 'boolean' ||
    !('inventory' in root) ||
    (root.inventory !== null && !inventory) ||
    !('scan_sweep' in root) ||
    (root.scan_sweep !== null && !scanSweep) ||
    !summary ||
    !machine ||
    !changed ||
    !changed.every(isEpochStatusValue) ||
    !removed ||
    !removed.every((epoch) => integerValue(epoch) !== null) ||
    !lanes ||
    !lanes.every(isLaneStatusValue) ||
    !live ||
    !live.every(isLiveStatusValue) ||
    !finalizerQueue ||
    !finalizerQueue.every(isFinalizerItemValue) ||
    !errors ||
    !errors.every(isPipelineErrorValue) ||
    epochCalendar === null ||
    recentCompactions === null ||
    processIo === null
  ) {
    return null;
  }

  const changedEpochs = changed.map((epoch) => (epoch as EpochStatus).epoch);
  const removedEpochs = removed as number[];
  const uniqueChanged = new Set(changedEpochs);
  const uniqueRemoved = new Set(removedEpochs);
  if (
    uniqueChanged.size !== changedEpochs.length ||
    uniqueRemoved.size !== removedEpochs.length ||
    changedEpochs.some((epoch) => uniqueRemoved.has(epoch))
  ) {
    return null;
  }

  return {
    schema_version: schemaVersion,
    sequence,
    now_unix_secs: nowUnixSecs,
    current_epoch: currentEpoch,
    observer_mode: root.observer_mode,
    scheduler: scheduler as SchedulerSnapshot,
    inventory: inventory as SchedulerStageSnapshot | null,
    scan_sweep: scanSweep as SchedulerStageSnapshot | null,
    summary: summary as PipelineSummary,
    machine: machine as MachineStatus,
    epochs_changed: changed as EpochStatus[],
    epochs_removed: removedEpochs,
    lanes: lanes as LaneStatus[],
    live: live as LiveStatus[],
    finalizer_queue: finalizerQueue as FinalizerItem[],
    errors: errors as PipelineError[],
    ...(epochCalendar === undefined
      ? {}
      : { epoch_calendar: epochCalendar as EpochCalendarEntry[] }),
    ...(recentCompactions === undefined
      ? {}
      : { recent_compactions: recentCompactions }),
    ...(processIo === undefined ? {} : { process_io: processIo })
  };
}

export function snapshotContainsEpoch(value: PipelineSnapshot, epoch: number) {
  return value.epochs.some((item) => item.epoch === epoch) ||
    value.live.some((capture) => capture.epoch === epoch);
}

export function asRecord(value: unknown): JsonRecord | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as JsonRecord)
    : null;
}

export function numberValue(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

export function positiveNumberValue(value: unknown): number | null {
  const parsed = numberValue(value);
  return parsed !== null && parsed > 0 ? parsed : null;
}

export function integerValue(value: unknown): number | null {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function isCompactionHistoryValue(value: unknown): value is CompactionHistoryEntry[] {
  if (!Array.isArray(value) || !value.every(isCompactionHistoryEntryValue)) return false;
  const ids = value.map((entry) => (entry as CompactionHistoryEntry).id);
  return new Set(ids).size === ids.length;
}

function isCompactionHistoryEntryValue(value: unknown): value is CompactionHistoryEntry {
  const entry = asRecord(value);
  const completedUnixSecs = entry ? integerValue(entry.completed_unix_secs) : null;
  return Boolean(
    entry &&
    typeof entry.id === 'string' &&
    entry.id.trim().length > 0 &&
    integerValue(entry.epoch) !== null &&
    isCompactionWorkflow(entry.workflow) &&
    completedUnixSecs !== null &&
    completedUnixSecs > 0 &&
    numberValue(entry.duration_secs) !== null &&
    Number(entry.duration_secs) >= 0
  );
}

function isCompactionWorkflow(value: unknown): value is CompactionWorkflow {
  return value === 'historical' || value === 'live' || value === 'recompact';
}

function isProcessIoSnapshotValue(value: unknown): value is ProcessIoSnapshot {
  const processIo = asRecord(value);
  if (!processIo || !Array.isArray(processIo.processes)) return false;
  const processes = processIo.processes;
  const processIds = processes
    .map((process) => asRecord(process)?.id)
    .filter((id): id is string => typeof id === 'string');
  return (
    (processIo.state === 'collecting' || processIo.state === 'ready' || processIo.state === 'unavailable') &&
    (processIo.sampled_unix_secs === null || integerValue(processIo.sampled_unix_secs) !== null) &&
    (processIo.sample_window_secs === null || positiveNumberValue(processIo.sample_window_secs) !== null) &&
    integerValue(processIo.active_count) !== null &&
    integerValue(processIo.inaccessible_count) !== null &&
    typeof processIo.truncated === 'boolean' &&
    processes.every(isProcessIoEntryValue) &&
    processIds.length === processes.length &&
    new Set(processIds).size === processIds.length &&
    (processIo.message === undefined || processIo.message === null || typeof processIo.message === 'string')
  );
}

function isProcessIoEntryValue(value: unknown): value is ProcessIoEntry {
  const process = asRecord(value);
  return Boolean(
    process &&
    typeof process.id === 'string' &&
    process.id.trim().length > 0 &&
    integerValue(process.pid) !== null &&
    Number(process.pid) > 0 &&
    typeof process.name === 'string' &&
    process.name.trim().length > 0 &&
    optionalNonNegativeMetric(process.read_mib_per_sec) &&
    optionalNonNegativeMetric(process.write_mib_per_sec) &&
    optionalNonNegativeMetric(process.cpu_percent) &&
    optionalNonNegativeMetric(process.rss_bytes) &&
    (process.blockzilla_owned === undefined || typeof process.blockzilla_owned === 'boolean')
  );
}

function optionalNonNegativeMetric(value: unknown) {
  return value === undefined || value === null ||
    (numberValue(value) !== null && Number(value) >= 0);
}

function isEpochCalendarValue(value: unknown): value is EpochCalendarEntry[] {
  return Array.isArray(value) &&
    value.every(isEpochCalendarEntryValue) &&
    hasUniqueEpochCalendarEpochs(value);
}

function isEpochCalendarEntryValue(value: unknown): value is EpochCalendarEntry {
  const timing = asRecord(value);
  if (!timing) return false;
  const epoch = integerValue(timing.epoch);
  const start = integerValue(timing.start_unix_secs);
  const end = timing.end_unix_secs === null ? null : integerValue(timing.end_unix_secs);
  return epoch !== null &&
    start !== null &&
    start > 0 &&
    (timing.end_unix_secs === null || (end !== null && end >= start)) &&
    (timing.precision === 'observed' || timing.precision === 'estimated');
}

function isEpochStatusValue(value: unknown) {
  const epoch = asRecord(value);
  return Boolean(
    epoch &&
    integerValue(epoch.epoch) !== null &&
    typeof epoch.state === 'string' &&
    typeof epoch.registry_order === 'string' &&
    (epoch.input_path === null || typeof epoch.input_path === 'string') &&
    typeof epoch.output_path === 'string' &&
    numberValue(epoch.car_bytes) !== null &&
    asRecord(epoch.progress) &&
    integerValue(epoch.updated_unix_secs) !== null &&
    (epoch.message === null || typeof epoch.message === 'string') &&
    (epoch.artifacts === undefined || Array.isArray(epoch.artifacts))
  );
}

function isLaneStatusValue(value: unknown) {
  const lane = asRecord(value);
  return Boolean(
    lane &&
    typeof lane.id === 'string' &&
    typeof lane.kind === 'string' &&
    typeof lane.state === 'string' &&
    asRecord(lane.progress)
  );
}

function isLiveStatusValue(value: unknown) {
  const capture = asRecord(value);
  return Boolean(
    capture &&
    typeof capture.id === 'string' &&
    typeof capture.state === 'string' &&
    (capture.epoch === null || integerValue(capture.epoch) !== null) &&
    asRecord(capture.progress)
  );
}

function isFinalizerItemValue(value: unknown) {
  const item = asRecord(value);
  return Boolean(
    item &&
    typeof item.id === 'string' &&
    typeof item.kind === 'string' &&
    typeof item.state === 'string' &&
    (item.epoch === null || integerValue(item.epoch) !== null)
  );
}

function isPipelineErrorValue(value: unknown) {
  const error = asRecord(value);
  return Boolean(
    error &&
    integerValue(error.at_unix_secs) !== null &&
    typeof error.scope === 'string' &&
    typeof error.message === 'string'
  );
}
