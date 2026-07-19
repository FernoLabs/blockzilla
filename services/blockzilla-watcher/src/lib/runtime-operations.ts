import type { ProcessIoSnapshot } from './pipeline-snapshot.ts';

export type RuntimeLiveCaptureState = 'capturing' | 'stalled';
export type RuntimeJobKind = 'car_download' | 'car_verify';
export type RuntimeJobState = 'running' | 'waiting' | 'failed';

export type RuntimeLiveCapture = {
  state: RuntimeLiveCaptureState;
  mode: 'raw_wal';
  source: 'grpc';
  pid: number;
  epoch: number;
  last_slot: number;
  blocks_written: number;
  bytes_written: number;
  write_mib_per_sec: number | null;
  started_unix_secs: number;
  updated_unix_secs: number;
};

export type RuntimeJob = {
  id: string;
  kind: RuntimeJobKind;
  epoch: number;
  phase: 'download' | 'checksum';
  state: RuntimeJobState;
  pid: number;
  bytes_done: number;
  bytes_total: number | null;
  progress_pct: number | null;
  read_mib_per_sec: number | null;
  write_mib_per_sec: number | null;
  eta_secs: number | null;
  rss_bytes: number | null;
  started_unix_secs: number;
  updated_unix_secs: number;
};

export type RuntimeOperations = {
  schema_version: 1;
  updated_unix_secs: number;
  live_capture: RuntimeLiveCapture | null;
  jobs: RuntimeJob[];
  process_io: ProcessIoSnapshot;
};

const LIVE_CAPTURE_STATES = new Set<RuntimeLiveCaptureState>(['capturing', 'stalled']);
const JOB_KINDS = new Set<RuntimeJobKind>(['car_download', 'car_verify']);
const JOB_STATES = new Set<RuntimeJobState>(['running', 'waiting', 'failed']);
const JOB_PHASES = new Set<RuntimeJob['phase']>(['download', 'checksum']);

export function parseRuntimeOperations(value: unknown): RuntimeOperations | null {
  const root = asRecord(value);
  if (!root || root.schema_version !== 1 || !Array.isArray(root.jobs)) return null;
  const updatedUnixSecs = integerValue(root.updated_unix_secs);
  const liveCapture = root.live_capture === null ? null : parseRuntimeLiveCapture(root.live_capture);
  const jobs = root.jobs.map(parseRuntimeJob);
  const processIo = parseProcessIoSnapshot(root.process_io);
  if (
    updatedUnixSecs === null ||
    updatedUnixSecs <= 0 ||
    (root.live_capture !== null && liveCapture === null) ||
    jobs.some((job) => job === null) ||
    processIo === null
  ) return null;
  const typedJobs = jobs as RuntimeJob[];
  if (new Set(typedJobs.map((job) => job.id)).size !== typedJobs.length) return null;
  return {
    schema_version: 1,
    updated_unix_secs: updatedUnixSecs,
    live_capture: liveCapture,
    jobs: typedJobs,
    process_io: processIo
  };
}

export function runtimeOperationsIsFresh(
  value: RuntimeOperations,
  nowUnixSecs: number,
  maxAgeSecs = 20
) {
  return nowUnixSecs <= value.updated_unix_secs + maxAgeSecs;
}

function parseRuntimeLiveCapture(value: unknown): RuntimeLiveCapture | null {
  const capture = asRecord(value);
  if (!capture || typeof capture.state !== 'string') return null;
  const pid = positiveInteger(capture.pid);
  const epoch = integerValue(capture.epoch);
  const lastSlot = integerValue(capture.last_slot);
  const blocksWritten = integerValue(capture.blocks_written);
  const bytesWritten = integerValue(capture.bytes_written);
  const writeRate = nullableNonNegativeNumber(capture.write_mib_per_sec);
  const startedUnixSecs = positiveInteger(capture.started_unix_secs);
  const updatedUnixSecs = positiveInteger(capture.updated_unix_secs);
  if (
    !LIVE_CAPTURE_STATES.has(capture.state as RuntimeLiveCaptureState) ||
    capture.mode !== 'raw_wal' ||
    capture.source !== 'grpc' ||
    pid === null ||
    epoch === null ||
    lastSlot === null ||
    Math.floor(lastSlot / 432_000) !== epoch ||
    blocksWritten === null ||
    bytesWritten === null ||
    writeRate === undefined ||
    startedUnixSecs === null ||
    updatedUnixSecs === null ||
    updatedUnixSecs < startedUnixSecs
  ) return null;
  return value as RuntimeLiveCapture;
}

function parseRuntimeJob(value: unknown): RuntimeJob | null {
  const job = asRecord(value);
  if (!job || typeof job.id !== 'string' || job.id.trim().length === 0) return null;
  const pid = positiveInteger(job.pid);
  const epoch = integerValue(job.epoch);
  const bytesDone = integerValue(job.bytes_done);
  const bytesTotal = nullableNonNegativeInteger(job.bytes_total);
  const progressPct = nullableBoundedPercent(job.progress_pct);
  const readRate = nullableNonNegativeNumber(job.read_mib_per_sec);
  const writeRate = nullableNonNegativeNumber(job.write_mib_per_sec);
  const etaSecs = nullableNonNegativeInteger(job.eta_secs);
  const rssBytes = nullableNonNegativeInteger(job.rss_bytes);
  const startedUnixSecs = positiveInteger(job.started_unix_secs);
  const updatedUnixSecs = positiveInteger(job.updated_unix_secs);
  if (
    typeof job.kind !== 'string' || !JOB_KINDS.has(job.kind as RuntimeJobKind) ||
    typeof job.phase !== 'string' || !JOB_PHASES.has(job.phase as RuntimeJob['phase']) ||
    typeof job.state !== 'string' || !JOB_STATES.has(job.state as RuntimeJobState) ||
    pid === null ||
    epoch === null ||
    bytesDone === null ||
    bytesTotal === undefined ||
    (bytesTotal !== null && bytesDone > bytesTotal) ||
    progressPct === undefined ||
    readRate === undefined ||
    writeRate === undefined ||
    etaSecs === undefined ||
    rssBytes === undefined ||
    startedUnixSecs === null ||
    updatedUnixSecs === null ||
    updatedUnixSecs < startedUnixSecs
  ) return null;
  return value as RuntimeJob;
}

function positiveInteger(value: unknown) {
  const parsed = integerValue(value);
  return parsed !== null && parsed > 0 ? parsed : null;
}

function nullableNonNegativeInteger(value: unknown): number | null | undefined {
  if (value === null) return null;
  const parsed = integerValue(value);
  return parsed === null ? undefined : parsed;
}

function nullableNonNegativeNumber(value: unknown): number | null | undefined {
  if (value === null) return null;
  const parsed = numberValue(value);
  return parsed === null || parsed < 0 ? undefined : parsed;
}

function nullableBoundedPercent(value: unknown): number | null | undefined {
  const parsed = nullableNonNegativeNumber(value);
  return parsed === null || parsed === undefined || parsed <= 100 ? parsed : undefined;
}

function parseProcessIoSnapshot(value: unknown): ProcessIoSnapshot | null {
  const processIo = asRecord(value);
  if (!processIo || !Array.isArray(processIo.processes)) return null;
  const processes = processIo.processes;
  const ids = processes
    .map((process) => asRecord(process)?.id)
    .filter((id): id is string => typeof id === 'string');
  if (
    !['collecting', 'ready', 'unavailable'].includes(String(processIo.state)) ||
    (processIo.sampled_unix_secs !== null && integerValue(processIo.sampled_unix_secs) === null) ||
    (processIo.sample_window_secs !== null && positiveNumber(processIo.sample_window_secs) === null) ||
    integerValue(processIo.active_count) === null ||
    integerValue(processIo.inaccessible_count) === null ||
    typeof processIo.truncated !== 'boolean' ||
    !processes.every(isProcessIoEntry) ||
    ids.length !== processes.length ||
    new Set(ids).size !== ids.length ||
    (processIo.message !== undefined && processIo.message !== null && typeof processIo.message !== 'string')
  ) return null;
  return value as ProcessIoSnapshot;
}

function isProcessIoEntry(value: unknown) {
  const process = asRecord(value);
  return Boolean(
    process &&
    typeof process.id === 'string' && process.id.trim().length > 0 &&
    positiveInteger(process.pid) !== null &&
    typeof process.name === 'string' && process.name.trim().length > 0 &&
    (process.user === undefined || process.user === null || typeof process.user === 'string') &&
    optionalNonNegativeNumber(process.read_mib_per_sec) &&
    optionalNonNegativeNumber(process.write_mib_per_sec) &&
    optionalNonNegativeNumber(process.cpu_percent) &&
    optionalNonNegativeNumber(process.rss_bytes) &&
    (process.blockzilla_owned === undefined || typeof process.blockzilla_owned === 'boolean')
  );
}

function optionalNonNegativeNumber(value: unknown) {
  return value === undefined || value === null ||
    (numberValue(value) !== null && Number(value) >= 0);
}

function positiveNumber(value: unknown) {
  const parsed = numberValue(value);
  return parsed !== null && parsed > 0 ? parsed : null;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function numberValue(value: unknown) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function integerValue(value: unknown) {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null;
}
