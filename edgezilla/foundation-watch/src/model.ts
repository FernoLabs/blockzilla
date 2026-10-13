export const HEARTBEAT_TIMEOUT_MS = 90_000;
export const ISSUE_CONFIRM_MS = 60_000;
export const GIB = 1024 ** 3;
export const REQUIRED_SERVICES = [
  "hivezilla-validator.service", "hivezilla-direct-worker.service",
  "hivezilla-bank-v2-r2-publisher.service", "hivezilla-native-cold-storage-20261012-live.service",
  "hivezilla-live-priority-20261012.service",
  "hivezilla-bank-v2-producer.service",
] as const;

type NullableNumber = number | null;
export interface ArchiveStatus {
  state: string | null;
  epoch: NullableNumber;
  updated_unix_ms: NullableNumber;
  stage_updated_unix_ms: NullableNumber;
  progress_updated_unix_ms: NullableNumber;
  progress_percent: NullableNumber;
  bytes_per_second: NullableNumber;
  eta_seconds: NullableNumber;
  eta_scope: "download" | "upload" | "stage" | null;
  paused: boolean | null;
  epoch_eta_min_seconds: NullableNumber;
  epoch_eta_max_seconds: NullableNumber;
  queue_eta_min_seconds: NullableNumber;
  queue_eta_max_seconds: NullableNumber;
  last_completed_epoch: NullableNumber;
  last_compact_bytes: NullableNumber;
  completed_epochs: number[];
  upload_queue_epochs: number[];
}
export interface Heartbeat {
  schema: "blockzilla-foundation-heartbeat-v1";
  machine_id: "foundation";
  sampled_unix_ms: number;
  monotonic_ms: number;
  boot_id: string | null;
  free_disk_bytes: NullableNumber;
  capture_reserve_bytes: NullableNumber;
  mem_available_bytes: NullableNumber;
  capture: { session: string | null; state: string | null; durable_sequence: NullableNumber;
    latest_rooted_slot: NullableNumber; updated_unix_ms: NullableNumber };
  compact: { state: string | null; last_compact_slot: NullableNumber; event_sequence: NullableNumber;
    source_sequence: NullableNumber; updated_unix_ms: NullableNumber };
  publisher: { state: string | null; sequence: NullableNumber; updated_unix_ms: NullableNumber };
  cold_storage: { state: string | null; retired_banks: NullableNumber; updated_unix_ms: NullableNumber };
  services: Record<string, string | null>;
  rpc: { healthy: boolean | null; finalized_slot: NullableNumber };
  errors: string[];
  archive?: ArchiveStatus;
}

export interface Progress {
  session: string | null;
  captureSequence: NullableNumber;
  captureRoot: NullableNumber;
  publisherSequence: NullableNumber;
  captureSequenceChangedMs: number;
  captureRootChangedMs: number;
  publisherChangedMs: number;
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid_object");
  return value as Record<string, unknown>;
}
function number(value: unknown): NullableNumber {
  if (value === null) return null;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new Error("invalid_number");
  return value;
}
function text(value: unknown): string | null {
  if (value === null) return null;
  if (typeof value !== "string" || value.length > 120 || !/^[a-zA-Z0-9_.:-]+$/.test(value)) throw new Error("invalid_text");
  return value;
}
function decimal(value: unknown, maximum: number): NullableNumber {
  if (value === null) return null;
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > maximum) throw new Error("invalid_decimal");
  return value;
}
function epoch(value: unknown): NullableNumber {
  const result = number(value);
  if (result !== null && result > 100_000) throw new Error("invalid_epoch");
  return result;
}
function epochList(value: unknown): number[] {
  if (!Array.isArray(value) || value.length > 64) throw new Error("invalid_epoch_list");
  return [...new Set(value.map(item => {
    const parsed = epoch(item);
    if (parsed === null) throw new Error("invalid_epoch_list");
    return parsed;
  }))].sort((a, b) => a - b);
}
function archiveStatus(value: unknown): ArchiveStatus {
  const v = object(value);
  if (v.eta_scope !== null && !["download", "upload", "stage"].includes(String(v.eta_scope))) throw new Error("invalid_eta_scope");
  if (v.paused !== undefined && v.paused !== null && typeof v.paused !== "boolean") throw new Error("invalid_archive_paused");
  const range = (minimum: unknown, maximum: unknown) => {
    const min = decimal(minimum ?? null, 365 * 24 * 60 * 60), max = decimal(maximum ?? null, 365 * 24 * 60 * 60);
    if (min !== null && max !== null && min > max) throw new Error("invalid_eta_range");
    return [min, max];
  };
  const [epochMin, epochMax] = range(v.epoch_eta_min_seconds, v.epoch_eta_max_seconds);
  const [queueMin, queueMax] = range(v.queue_eta_min_seconds, v.queue_eta_max_seconds);
  return { state: text(v.state), epoch: epoch(v.epoch), updated_unix_ms: number(v.updated_unix_ms),
    stage_updated_unix_ms: number(v.stage_updated_unix_ms), progress_updated_unix_ms: number(v.progress_updated_unix_ms ?? null),
    progress_percent: decimal(v.progress_percent, 100),
    bytes_per_second: decimal(v.bytes_per_second, 1e12), eta_seconds: decimal(v.eta_seconds, 365 * 24 * 60 * 60),
    eta_scope: v.eta_scope as ArchiveStatus["eta_scope"], paused: v.paused === undefined ? null : v.paused as boolean | null,
    epoch_eta_min_seconds: epochMin, epoch_eta_max_seconds: epochMax, queue_eta_min_seconds: queueMin, queue_eta_max_seconds: queueMax,
    last_completed_epoch: epoch(v.last_completed_epoch),
    last_compact_bytes: number(v.last_compact_bytes), completed_epochs: epochList(v.completed_epochs),
    upload_queue_epochs: epochList(v.upload_queue_epochs) };
}

export function heartbeat(value: unknown, now: number): Heartbeat {
  const v = object(value);
  if (v.schema !== "blockzilla-foundation-heartbeat-v1" || v.machine_id !== "foundation") throw new Error("invalid_schema");
  const sampled = number(v.sampled_unix_ms);
  const monotonic = number(v.monotonic_ms);
  if (sampled === null || monotonic === null) throw new Error("invalid_sample_clock");
  const c = object(v.capture), w = object(v.compact), p = object(v.publisher), s = object(v.cold_storage), rpc = object(v.rpc);
  const services: Record<string, string | null> = {};
  const incoming = object(v.services);
  for (const name of REQUIRED_SERVICES) services[name] = text(incoming[name]);
  if (!Array.isArray(v.errors) || v.errors.length > 40 || v.errors.some(x => typeof x !== "string" || !/^[a-z0-9_]{1,80}$/.test(x))) throw new Error("invalid_errors");
  const session = text(c.session);
  if (session !== null && !/^[0-9a-f]{32}-[0-9a-f]+$/.test(session)) throw new Error("invalid_session");
  if (rpc.healthy !== null && typeof rpc.healthy !== "boolean") throw new Error("invalid_rpc_health");
  return {
    schema: "blockzilla-foundation-heartbeat-v1", machine_id: "foundation", sampled_unix_ms: sampled, monotonic_ms: monotonic,
    boot_id: text(v.boot_id), free_disk_bytes: number(v.free_disk_bytes),
    capture_reserve_bytes: number(v.capture_reserve_bytes), mem_available_bytes: number(v.mem_available_bytes),
    capture: { session, state: text(c.state), durable_sequence: number(c.durable_sequence),
      latest_rooted_slot: number(c.latest_rooted_slot), updated_unix_ms: number(c.updated_unix_ms) },
    compact: { state: text(w.state), last_compact_slot: number(w.last_compact_slot), event_sequence: number(w.event_sequence),
      source_sequence: number(w.source_sequence), updated_unix_ms: number(w.updated_unix_ms) },
    publisher: { state: text(p.state), sequence: number(p.sequence), updated_unix_ms: number(p.updated_unix_ms) },
    cold_storage: { state: text(s.state), retired_banks: number(s.retired_banks), updated_unix_ms: number(s.updated_unix_ms) },
    services, rpc: { healthy: rpc.healthy, finalized_slot: number(rpc.finalized_slot) }, errors: [...new Set(v.errors)].sort(),
    ...(v.archive === undefined ? {} : { archive: archiveStatus(v.archive) }),
  };
}

export function progress(previous: Progress | null, sample: Heartbeat, now: number): Progress {
  const values = [sample.capture.durable_sequence, sample.capture.latest_rooted_slot, sample.publisher.sequence];
  if (!previous || previous.session !== sample.capture.session) return {
    session: sample.capture.session, captureSequence: values[0], captureRoot: values[1], publisherSequence: values[2],
    captureSequenceChangedMs: now, captureRootChangedMs: now, publisherChangedMs: now,
  };
  function changed(value: NullableNumber, prior: NullableNumber, at: number) {
    return value !== null && (prior === null || value > prior) ? now : at;
  }
  const maximum = (value: NullableNumber, prior: NullableNumber) => value === null ? prior : prior === null ? value : Math.max(value, prior);
  return { session: sample.capture.session, captureSequence: maximum(values[0], previous.captureSequence),
    captureRoot: maximum(values[1], previous.captureRoot), publisherSequence: maximum(values[2], previous.publisherSequence),
    captureSequenceChangedMs: changed(values[0], previous.captureSequence, previous.captureSequenceChangedMs),
    captureRootChangedMs: changed(values[1], previous.captureRoot, previous.captureRootChangedMs),
    publisherChangedMs: changed(values[2], previous.publisherSequence, previous.publisherChangedMs) };
}

export function issues(sample: Heartbeat | null, lastReceivedMs: number, startedMs: number,
  counters: Progress | null, streamIssue: string | null, now: number): string[] {
  if (now - (lastReceivedMs || startedMs) >= HEARTBEAT_TIMEOUT_MS) return ["machine_heartbeat_missing"];
  if (!sample) return [];
  const problems = new Set<string>();
  for (const code of sample.errors) problems.add("telemetry:" + code);
  for (const name of REQUIRED_SERVICES) if (sample.services[name] !== "active") problems.add("service:" + name);
  if (sample.rpc.healthy !== true) problems.add("validator_rpc_unhealthy");
  if (sample.capture.state !== "CAPTURING") problems.add("capture_not_running");
  if (sample.compact.state !== "RUNNING") problems.add("compaction_not_running");
  if (sample.publisher.state !== "RUNNING") problems.add("publisher_not_running");
  if (sample.cold_storage.state !== "RUNNING") problems.add("cloud_storage_not_running");
  if (sample.compact.updated_unix_ms === null || now - sample.compact.updated_unix_ms > 120_000) problems.add("compaction_status_stale");
  if (sample.publisher.updated_unix_ms === null || now - sample.publisher.updated_unix_ms > 120_000) problems.add("publisher_status_stale");
  if (sample.cold_storage.updated_unix_ms === null || now - sample.cold_storage.updated_unix_ms > 300_000) problems.add("cloud_storage_status_stale");
  if (sample.free_disk_bytes !== null && sample.free_disk_bytes < (sample.capture_reserve_bytes ?? 400 * GIB) + 100 * GIB) problems.add("disk_headroom_low");
  if (sample.mem_available_bytes !== null && sample.mem_available_bytes < 64 * GIB) problems.add("memory_headroom_low");
  if (sample.capture.latest_rooted_slot !== null && sample.compact.last_compact_slot !== null &&
      sample.capture.latest_rooted_slot - sample.compact.last_compact_slot > 64) problems.add("compaction_behind");
  if (sample.compact.event_sequence !== null && sample.publisher.sequence !== null &&
      sample.compact.event_sequence - sample.publisher.sequence > 8192) problems.add("cloud_upload_behind");
  if (counters) {
    if (sample.capture.durable_sequence !== null && counters.captureSequence !== null && sample.capture.durable_sequence < counters.captureSequence) problems.add("capture_counter_regressed");
    if (sample.capture.latest_rooted_slot !== null && counters.captureRoot !== null && sample.capture.latest_rooted_slot < counters.captureRoot) problems.add("capture_counter_regressed");
    if (sample.publisher.sequence !== null && counters.publisherSequence !== null && sample.publisher.sequence < counters.publisherSequence) problems.add("cloud_upload_counter_regressed");
    if (now - counters.captureSequenceChangedMs > 90_000 || now - counters.captureRootChangedMs > 90_000) problems.add("capture_stalled");
    if (sample.compact.event_sequence !== null && sample.publisher.sequence !== null &&
        sample.compact.event_sequence > sample.publisher.sequence && now - counters.publisherChangedMs > 300_000) problems.add("cloud_upload_stalled");
  }
  if (streamIssue) problems.add(streamIssue);
  return [...problems].sort();
}

export interface Alert {
  id: string;
  kind: "failure" | "recovery" | "test";
  issues: string[];
  createdMs: number;
}
export interface Incident {
  active: string[];
  candidate: string[];
  candidateSinceMs: number;
  // Optional for compatibility with records saved before per-issue timers.
  issueSinceMs?: Record<string, number>;
}

export function transition(prior: Incident, current: string[], now: number): { incident: Incident; event: Omit<Alert, "id"> | null } {
  const currentKey = JSON.stringify(current);
  const previousTimes = prior.issueSinceMs ?? Object.fromEntries(prior.candidate.map(code => [code, prior.candidateSinceMs]));
  const issueSinceMs = Object.fromEntries(current.map(code => [code, previousTimes[code] ?? now]));
  const incident = { ...prior, candidate: current, issueSinceMs,
    candidateSinceMs: currentKey === JSON.stringify(prior.candidate) ? prior.candidateSinceMs : now };
  if (currentKey === JSON.stringify(prior.active)) return { incident: { ...incident, candidateSinceMs: now }, event: null };
  if (current[0] === "machine_heartbeat_missing") return {
    incident: { ...incident, active: current, candidateSinceMs: now },
    event: { kind: "failure", issues: current, createdMs: now },
  };
  if (!current.length) {
    // Recovery requires a continuous all-clear period, even if fault sets varied.
    if (now - incident.candidateSinceMs < ISSUE_CONFIRM_MS) return { incident, event: null };
    return { incident: { ...incident, active: [], candidateSinceMs: now },
      event: { kind: "recovery", issues: [], createdMs: now } };
  }
  // Confirm each fault independently. An intermittent second fault must not
  // reset the timer for a persistent capture or storage failure.
  const confirmed = current.filter(code => now - issueSinceMs[code] >= ISSUE_CONFIRM_MS);
  if (!confirmed.length) return { incident, event: null };
  const newFault = confirmed.some(code => !prior.active.includes(code));
  return { incident: { ...incident, active: confirmed },
    event: newFault ? { kind: "failure", issues: confirmed, createdMs: now } : null };
}
