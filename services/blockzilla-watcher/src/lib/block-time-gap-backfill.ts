export type BlockTimeGapBackfillState =
  | 'starting'
  | 'waiting_for_resources'
  | 'running'
  | 'paused_for_resources'
  | 'failed'
  | 'complete';

export type BlockTimeGapBackfill = {
  schema_version: 1;
  state: BlockTimeGapBackfillState;
  started_unix_secs: number;
  updated_unix_secs: number;
  backfill: {
    epochs_done: number;
    epochs_total: number;
    source_bytes_done: number;
    source_bytes_total: number;
    throughput_bytes_per_sec: number;
    eta_secs: number | null;
    eta_reliable: boolean;
  };
  current: {
    epoch: number | null;
    progress_state: string | null;
  };
  paused_secs: number;
  last_error: string | null;
};

const BACKFILL_STATES = new Set<BlockTimeGapBackfillState>([
  'starting',
  'waiting_for_resources',
  'running',
  'paused_for_resources',
  'failed',
  'complete'
]);

export function parseBlockTimeGapBackfill(value: unknown): BlockTimeGapBackfill | null {
  const root = asRecord(value);
  const backfill = asRecord(root?.backfill);
  const current = asRecord(root?.current);
  if (!root || !backfill || !current) return null;

  const state = root.state;
  const startedUnixSecs = integerValue(root.started_unix_secs);
  const updatedUnixSecs = integerValue(root.updated_unix_secs);
  const epochsDone = integerValue(backfill.epochs_done);
  const epochsTotal = integerValue(backfill.epochs_total);
  const sourceBytesDone = integerValue(backfill.source_bytes_done);
  const sourceBytesTotal = integerValue(backfill.source_bytes_total);
  const throughput = numberValue(backfill.throughput_bytes_per_sec);
  const etaSecs = backfill.eta_secs === null ? null : integerValue(backfill.eta_secs);
  const currentEpoch = current.epoch === null ? null : integerValue(current.epoch);
  const progressState = current.progress_state;
  const pausedSecs = integerValue(root.paused_secs);

  if (
    root.schema_version !== 1 ||
    typeof state !== 'string' ||
    !BACKFILL_STATES.has(state as BlockTimeGapBackfillState) ||
    startedUnixSecs === null ||
    updatedUnixSecs === null ||
    updatedUnixSecs < startedUnixSecs ||
    epochsDone === null ||
    epochsTotal === null ||
    epochsDone > epochsTotal ||
    sourceBytesDone === null ||
    sourceBytesTotal === null ||
    sourceBytesDone > sourceBytesTotal ||
    throughput === null ||
    throughput < 0 ||
    (backfill.eta_secs !== null && etaSecs === null) ||
    typeof backfill.eta_reliable !== 'boolean' ||
    (current.epoch !== null && currentEpoch === null) ||
    (progressState !== null && typeof progressState !== 'string') ||
    pausedSecs === null ||
    (root.last_error !== null && typeof root.last_error !== 'string')
  ) {
    return null;
  }

  return value as BlockTimeGapBackfill;
}

export function blockTimeGapBackfillPercent(value: BlockTimeGapBackfill) {
  if (value.backfill.source_bytes_total <= 0) return value.state === 'complete' ? 100 : 0;
  return Math.max(
    0,
    Math.min(100, value.backfill.source_bytes_done * 100 / value.backfill.source_bytes_total)
  );
}

export function blockTimeGapBackfillIsFresh(
  value: BlockTimeGapBackfill,
  nowUnixSecs: number,
  maxAgeSecs = 60
) {
  return nowUnixSecs <= value.updated_unix_secs + maxAgeSecs;
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
