export type IngestOverallState = 'healthy' | 'degraded' | 'failed' | 'unknown';
export type UpstreamState = 'connected' | 'reconnecting' | 'stalled' | 'unavailable';
export type RecorderState = 'recording' | 'paused' | 'stalled' | 'unavailable';
export type ReplicationState = 'caught_up' | 'syncing' | 'stalled' | 'unavailable';
export type IndexerState = 'indexing' | 'caught_up' | 'stalled' | 'unavailable';
export type ObjectStoreState = 'standby' | 'uploading' | 'degraded' | 'unavailable';
export type FallbackState = 'capturing' | 'standby' | 'stalled' | 'unavailable';
export type GapCoverage = 'raw' | 'normalized' | 'rpc_recoverable' | 'unproven';
export type IncidentSeverity = 'warning' | 'error' | 'critical';

export type IngestPipelineStatus = {
  schema_version: 1;
  updated_unix_secs: number;
  overall_state: IngestOverallState;
  upstream: {
    state: UpstreamState;
    updated_unix_secs: number | null;
    reconnects_1h: number | null;
  };
  recorder: {
    state: RecorderState;
    durable_slot: number | null;
    updated_unix_secs: number | null;
    active_bytes: number;
    sealed_generations: number;
    unacknowledged_bytes: number;
    disk_free_bytes: number | null;
    disk_total_bytes: number | null;
  };
  replication: {
    state: ReplicationState;
    ack_through_sequence: number | null;
    ack_slot: number | null;
    updated_unix_secs: number | null;
    lag_records: number | null;
  };
  indexer: {
    state: IndexerState;
    last_slot: number | null;
    updated_unix_secs: number | null;
    lag_slots: number | null;
  };
  object_store: {
    state: ObjectStoreState;
    provider: 'r2';
    committed_bytes: number | null;
    pending_bytes: number;
    updated_unix_secs: number | null;
  };
  fallback: {
    state: FallbackState;
    last_slot: number | null;
    updated_unix_secs: number | null;
    lag_slots: number | null;
  };
  gaps: Array<{
    from_slot: number;
    to_slot: number;
    produced_blocks: number | null;
    coverage: GapCoverage;
  }>;
  gaps_truncated: boolean;
  incidents: Array<{
    id: string;
    severity: IncidentSeverity;
    started_unix_secs: number;
    resolved_unix_secs: number | null;
  }>;
};

const OVERALL_STATES = new Set<IngestOverallState>(['healthy', 'degraded', 'failed', 'unknown']);
const UPSTREAM_STATES = new Set<UpstreamState>(['connected', 'reconnecting', 'stalled', 'unavailable']);
const RECORDER_STATES = new Set<RecorderState>(['recording', 'paused', 'stalled', 'unavailable']);
const REPLICATION_STATES = new Set<ReplicationState>(['caught_up', 'syncing', 'stalled', 'unavailable']);
const INDEXER_STATES = new Set<IndexerState>(['indexing', 'caught_up', 'stalled', 'unavailable']);
const OBJECT_STORE_STATES = new Set<ObjectStoreState>(['standby', 'uploading', 'degraded', 'unavailable']);
const FALLBACK_STATES = new Set<FallbackState>(['capturing', 'standby', 'stalled', 'unavailable']);
const GAP_COVERAGE = new Set<GapCoverage>(['raw', 'normalized', 'rpc_recoverable', 'unproven']);
const INCIDENT_SEVERITIES = new Set<IncidentSeverity>(['warning', 'error', 'critical']);
const INCIDENT_IDS = new Set([
  'grpc_stale',
  'upstream_access_blocked',
  'replay_gap',
  'resume_coverage',
  'replay_recovery_failed',
  'disk_space',
  'disk_check_failed',
  'volume_invalid',
  'cache_rotation_failed',
  'generation_rotation_failed',
  'generation_backlog',
  'object_store',
  'receiver_ack_stale'
]);
const MAX_GAPS = 32;
const MAX_INCIDENTS = 32;

export function parseIngestPipelineStatus(value: unknown): IngestPipelineStatus | null {
  const root = asRecord(value);
  if (!root || root.schema_version !== 1 || !enumValue(root.overall_state, OVERALL_STATES)) return null;
  const updatedUnixSecs = positiveInteger(root.updated_unix_secs);
  const upstream = parseUpstream(root.upstream);
  const recorder = parseRecorder(root.recorder);
  const replication = parseReplication(root.replication);
  const indexer = parseIndexer(root.indexer);
  const objectStore = parseObjectStore(root.object_store);
  const fallback = parseFallback(root.fallback);
  const gaps = parseGaps(root.gaps);
  const incidents = parseIncidents(root.incidents);
  if (
    updatedUnixSecs === null ||
    !upstream ||
    !recorder ||
    !replication ||
    !indexer ||
    !objectStore ||
    !fallback ||
    !gaps ||
    typeof root.gaps_truncated !== 'boolean' ||
    !incidents
  ) return null;
  if (root.gaps_truncated && gaps.length !== MAX_GAPS) return null;
  const typed = value as IngestPipelineStatus;
  const futureLimit = updatedUnixSecs + 5;
  const stageTimestamps = [
    typed.upstream.updated_unix_secs,
    typed.recorder.updated_unix_secs,
    typed.replication.updated_unix_secs,
    typed.indexer.updated_unix_secs,
    typed.object_store.updated_unix_secs,
    typed.fallback.updated_unix_secs
  ];
  if (stageTimestamps.some((timestamp) => timestamp !== null && timestamp > futureLimit)) return null;
  if (incidents.some((incident) =>
    incident.started_unix_secs > futureLimit ||
    (incident.resolved_unix_secs !== null && incident.resolved_unix_secs > futureLimit)
  )) return null;
  const hasUnprovenGap = gaps.some((gap) => gap.coverage === 'unproven');
  const hasPartialGap = gaps.some((gap) => gap.coverage !== 'raw');
  const hasFailureIncident = incidents.some(
    (incident) => incident.resolved_unix_secs === null && ['error', 'critical'].includes(incident.severity)
  );
  const hasWarningIncident = incidents.some(
    (incident) => incident.resolved_unix_secs === null && incident.severity === 'warning'
  );
  if ((hasUnprovenGap || hasFailureIncident) && root.overall_state !== 'failed') return null;
  if ((hasPartialGap || hasWarningIncident) && root.overall_state === 'healthy') return null;
  return typed;
}

export function ingestPipelineStatusIsFresh(
  value: IngestPipelineStatus,
  nowUnixSecs: number,
  maxAgeSecs = 30,
  maxFutureSkewSecs = 5
) {
  return value.updated_unix_secs <= nowUnixSecs + maxFutureSkewSecs &&
    nowUnixSecs <= value.updated_unix_secs + maxAgeSecs;
}

function parseUpstream(value: unknown) {
  const stage = asRecord(value);
  if (!stage || !enumValue(stage.state, UPSTREAM_STATES)) return null;
  const updated = nullableTimestamp(stage.updated_unix_secs);
  const reconnects = nullableNonNegativeInteger(stage.reconnects_1h);
  if (updated === undefined || reconnects === undefined) return null;
  if (stage.state === 'unavailable' ? updated !== null : updated === null) return null;
  return stage;
}

function parseRecorder(value: unknown) {
  const stage = asRecord(value);
  if (!stage || !enumValue(stage.state, RECORDER_STATES)) return null;
  const free = nullableNonNegativeInteger(stage.disk_free_bytes);
  const total = nullableNonNegativeInteger(stage.disk_total_bytes);
  const durableSlot = nullableNonNegativeInteger(stage.durable_slot);
  const updated = nullableTimestamp(stage.updated_unix_secs);
  if (
    durableSlot === undefined ||
    updated === undefined ||
    nonNegativeInteger(stage.active_bytes) === null ||
    nonNegativeInteger(stage.sealed_generations) === null ||
    nonNegativeInteger(stage.unacknowledged_bytes) === null ||
    free === undefined ||
    total === undefined ||
    (free !== null && total !== null && free > total)
  ) return null;
  if (stage.state === 'unavailable') {
    return [durableSlot, updated, free, total].every((item) => item === null) ? stage : null;
  }
  if ([durableSlot, updated, free, total].some((item) => item === null)) return null;
  return stage;
}

function parseReplication(value: unknown) {
  const stage = asRecord(value);
  if (!stage || !enumValue(stage.state, REPLICATION_STATES)) return null;
  const sequence = nullableNonNegativeInteger(stage.ack_through_sequence);
  const slot = nullableNonNegativeInteger(stage.ack_slot);
  const updated = nullableTimestamp(stage.updated_unix_secs);
  const lag = nullableNonNegativeInteger(stage.lag_records);
  if ([sequence, slot, updated, lag].some((item) => item === undefined)) return null;
  if (stage.state === 'unavailable') {
    return [sequence, slot, updated, lag].every((item) => item === null) ? stage : null;
  }
  if (sequence === null || updated === null || lag === null) return null;
  if (lag === 0 ? slot === null : slot !== null) return null;
  if (stage.state === 'caught_up' && lag !== 0) return null;
  if (stage.state === 'syncing' && lag === 0) return null;
  return stage;
}

function parseIndexer(value: unknown) {
  const stage = asRecord(value);
  if (!stage || !enumValue(stage.state, INDEXER_STATES)) return null;
  const slot = nullableNonNegativeInteger(stage.last_slot);
  const updated = nullableTimestamp(stage.updated_unix_secs);
  const lag = nullableNonNegativeInteger(stage.lag_slots);
  if ([slot, updated, lag].some((item) => item === undefined)) return null;
  if (stage.state === 'unavailable') {
    return [slot, updated, lag].every((item) => item === null) ? stage : null;
  }
  if (slot === null || updated === null || lag === null) return null;
  if (stage.state === 'caught_up' && lag !== 0) return null;
  return stage;
}

function parseObjectStore(value: unknown) {
  const stage = asRecord(value);
  if (!stage || stage.provider !== 'r2' || !enumValue(stage.state, OBJECT_STORE_STATES)) return null;
  const committed = nullableNonNegativeInteger(stage.committed_bytes);
  const pending = nonNegativeInteger(stage.pending_bytes);
  const updated = nullableTimestamp(stage.updated_unix_secs);
  if (committed === undefined || pending === null || updated === undefined) return null;
  if (stage.state === 'unavailable') {
    return committed === null && pending === 0 && updated === null ? stage : null;
  }
  if (updated === null) return null;
  if (stage.state === 'standby' && pending !== 0) return null;
  if (stage.state === 'uploading' && pending === 0) return null;
  return stage;
}

function parseFallback(value: unknown) {
  const stage = asRecord(value);
  if (!stage || !enumValue(stage.state, FALLBACK_STATES)) return null;
  const slot = nullableNonNegativeInteger(stage.last_slot);
  const updated = nullableTimestamp(stage.updated_unix_secs);
  const lag = nullableNonNegativeInteger(stage.lag_slots);
  if ([slot, updated, lag].some((item) => item === undefined)) return null;
  if (stage.state === 'unavailable') {
    return [slot, updated, lag].every((item) => item === null) ? stage : null;
  }
  if (stage.state === 'capturing' && (slot === null || updated === null || lag === null)) return null;
  const evidence = [slot, updated, lag];
  if (evidence.some((item) => item === null) && evidence.some((item) => item !== null)) return null;
  return stage;
}

function parseGaps(value: unknown) {
  if (!Array.isArray(value) || value.length > MAX_GAPS) return null;
  const gaps: IngestPipelineStatus['gaps'] = [];
  const ranges = new Set<string>();
  for (const entry of value) {
    const gap = asRecord(entry);
    if (!gap || !enumValue(gap.coverage, GAP_COVERAGE)) return null;
    const from = nonNegativeInteger(gap.from_slot);
    const to = nonNegativeInteger(gap.to_slot);
    const produced = nullableNonNegativeInteger(gap.produced_blocks);
    if (from === null || to === null || to < from || produced === undefined) return null;
    const range = `${from}-${to}`;
    if (ranges.has(range)) return null;
    ranges.add(range);
    gaps.push(entry as IngestPipelineStatus['gaps'][number]);
  }
  return gaps;
}

function parseIncidents(value: unknown) {
  if (!Array.isArray(value) || value.length > MAX_INCIDENTS) return null;
  const incidents: IngestPipelineStatus['incidents'] = [];
  const ids = new Set<string>();
  for (const entry of value) {
    const incident = asRecord(entry);
    if (!incident || !enumValue(incident.severity, INCIDENT_SEVERITIES)) return null;
    const id = boundedString(incident.id, 80);
    const started = positiveInteger(incident.started_unix_secs);
    const resolved = nullableTimestamp(incident.resolved_unix_secs);
    if (!id || !INCIDENT_IDS.has(id) || started === null || resolved === undefined || ids.has(id)) return null;
    if (resolved !== null && resolved < started) return null;
    ids.add(id);
    incidents.push(entry as IngestPipelineStatus['incidents'][number]);
  }
  return incidents;
}

function enumValue<T extends string>(value: unknown, allowed: Set<T>): value is T {
  return typeof value === 'string' && allowed.has(value as T);
}

function boundedString(value: unknown, maximum: number) {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= maximum
    ? value
    : null;
}

function nonNegativeInteger(value: unknown) {
  const parsed = integerValue(value);
  return parsed !== null && parsed >= 0 ? parsed : null;
}

function positiveInteger(value: unknown) {
  const parsed = integerValue(value);
  return parsed !== null && parsed > 0 ? parsed : null;
}

function nullableNonNegativeInteger(value: unknown): number | null | undefined {
  if (value === null) return null;
  const parsed = nonNegativeInteger(value);
  return parsed === null ? undefined : parsed;
}

function nullableTimestamp(value: unknown): number | null | undefined {
  if (value === null) return null;
  const parsed = positiveInteger(value);
  return parsed === null ? undefined : parsed;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function integerValue(value: unknown) {
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : null;
}
