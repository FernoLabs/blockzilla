export type BlockTimeInterruptionKind = 'intra_epoch' | 'epoch_boundary';

export type BlockTimeInterruptionDigest = {
  id: number;
  kind: BlockTimeInterruptionKind;
  previous_slot: number;
  next_slot: number;
  previous_block_time: number;
  next_block_time: number;
  elapsed_secs: number;
  missing_slots: number;
};

export type BlockTimeInterruptionDay = {
  day_start_unix_secs: number;
  interruption_count: number;
  boundary_interruption_count: number;
  interruption_seconds: number;
  longest_interruption_secs: number;
  largest_missing_slots: number;
  longest_interruption: BlockTimeInterruptionDigest;
};

export type BlockTimeGapIndex = {
  schema_version: 1;
  generated_unix_secs: number;
  minimum_interruption_secs: number;
  source_sha256: string;
  coverage: {
    start_epoch: number;
    end_epoch: number;
    expected_epoch_count: number;
    indexed_epoch_count: number;
    missing_epochs: number[];
    indexed_boundary_count: number;
    source_sidecar_bytes: number;
    source_gap_rows: number;
  };
  days: BlockTimeInterruptionDay[];
};

const SECONDS_PER_DAY = 86_400;

export function parseBlockTimeGapIndex(value: unknown): BlockTimeGapIndex | null {
  const root = record(value);
  const coverage = record(root?.coverage);
  const days = root?.days;
  const interruptions = root?.interruptions;
  const schemaVersion = integer(root?.schema_version);
  const generatedUnixSecs = integer(root?.generated_unix_secs);
  const minimumInterruptionSecs = integer(root?.minimum_interruption_secs);
  const sourceSha256 = string(root?.source_sha256);
  const startEpoch = integer(coverage?.start_epoch);
  const endEpoch = integer(coverage?.end_epoch);
  const expectedEpochCount = integer(coverage?.expected_epoch_count);
  const indexedEpochCount = integer(coverage?.indexed_epoch_count);
  const indexedBoundaryCount = integer(coverage?.indexed_boundary_count);
  const sourceSidecarBytes = integer(coverage?.source_sidecar_bytes);
  const sourceGapRows = integer(coverage?.source_gap_rows);
  if (
    schemaVersion !== 1 ||
    generatedUnixSecs === null || generatedUnixSecs <= 0 ||
    minimumInterruptionSecs === null || minimumInterruptionSecs <= 1 ||
    !sourceSha256?.match(/^[0-9a-f]{64}$/) ||
    startEpoch === null || startEpoch < 0 ||
    endEpoch === null || endEpoch < startEpoch ||
    expectedEpochCount === null || expectedEpochCount !== endEpoch - startEpoch + 1 ||
    indexedEpochCount === null || indexedEpochCount < 0 || indexedEpochCount > expectedEpochCount ||
    indexedBoundaryCount === null || indexedBoundaryCount < 0 ||
    sourceSidecarBytes === null || sourceSidecarBytes < 0 ||
    sourceGapRows === null || sourceGapRows < 0 ||
    !Array.isArray(coverage?.missing_epochs) ||
    !Array.isArray(days) ||
    !Array.isArray(interruptions)
  ) return null;

  const missingEpochs = coverage.missing_epochs.map(integer);
  if (
    missingEpochs.some((epoch) => epoch === null || epoch < startEpoch || epoch > endEpoch) ||
    missingEpochs.some((epoch, index) => index > 0 && epoch! <= missingEpochs[index - 1]!) ||
    indexedEpochCount + missingEpochs.length !== expectedEpochCount
  ) return null;

  const parsedDays: BlockTimeInterruptionDay[] = [];
  for (const value of days) {
    const day = parseDay(value, minimumInterruptionSecs);
    if (!day) return null;
    const previous = parsedDays.at(-1);
    if (previous && day.day_start_unix_secs <= previous.day_start_unix_secs) return null;
    parsedDays.push(day);
  }

  return {
    schema_version: 1,
    generated_unix_secs: generatedUnixSecs,
    minimum_interruption_secs: minimumInterruptionSecs,
    source_sha256: sourceSha256,
    coverage: {
      start_epoch: startEpoch,
      end_epoch: endEpoch,
      expected_epoch_count: expectedEpochCount,
      indexed_epoch_count: indexedEpochCount,
      missing_epochs: missingEpochs as number[],
      indexed_boundary_count: indexedBoundaryCount,
      source_sidecar_bytes: sourceSidecarBytes,
      source_gap_rows: sourceGapRows
    },
    days: parsedDays
  };
}

function parseDay(value: unknown, minimumInterruptionSecs: number): BlockTimeInterruptionDay | null {
  const day = record(value);
  const dayStartUnixSecs = integer(day?.day_start_unix_secs);
  const interruptionCount = integer(day?.interruption_count);
  const boundaryInterruptionCount = integer(day?.boundary_interruption_count);
  const interruptionSeconds = integer(day?.interruption_seconds);
  const longestInterruptionSecs = integer(day?.longest_interruption_secs);
  const largestMissingSlots = integer(day?.largest_missing_slots);
  const longestInterruption = parseDigest(day?.longest_interruption);
  if (
    dayStartUnixSecs === null || dayStartUnixSecs < 0 || dayStartUnixSecs % SECONDS_PER_DAY !== 0 ||
    interruptionCount === null || interruptionCount <= 0 ||
    boundaryInterruptionCount === null || boundaryInterruptionCount < 0 || boundaryInterruptionCount > interruptionCount ||
    interruptionSeconds === null || interruptionSeconds <= 0 ||
    longestInterruptionSecs === null || longestInterruptionSecs < minimumInterruptionSecs ||
    largestMissingSlots === null || largestMissingSlots < 0 ||
    !longestInterruption || longestInterruption.elapsed_secs !== longestInterruptionSecs
  ) return null;
  return {
    day_start_unix_secs: dayStartUnixSecs,
    interruption_count: interruptionCount,
    boundary_interruption_count: boundaryInterruptionCount,
    interruption_seconds: interruptionSeconds,
    longest_interruption_secs: longestInterruptionSecs,
    largest_missing_slots: largestMissingSlots,
    longest_interruption: longestInterruption
  };
}

function parseDigest(value: unknown): BlockTimeInterruptionDigest | null {
  const digest = record(value);
  const id = integer(digest?.id);
  const kind = digest?.kind;
  const previousSlot = integer(digest?.previous_slot);
  const nextSlot = integer(digest?.next_slot);
  const previousBlockTime = integer(digest?.previous_block_time);
  const nextBlockTime = integer(digest?.next_block_time);
  const elapsedSecs = integer(digest?.elapsed_secs);
  const missingSlots = integer(digest?.missing_slots);
  if (
    id === null || id < 0 ||
    (kind !== 'intra_epoch' && kind !== 'epoch_boundary') ||
    previousSlot === null || previousSlot < 0 ||
    nextSlot === null || nextSlot <= previousSlot ||
    previousBlockTime === null || previousBlockTime < 0 ||
    nextBlockTime === null || nextBlockTime <= previousBlockTime ||
    elapsedSecs === null || elapsedSecs !== nextBlockTime - previousBlockTime ||
    missingSlots === null || missingSlots !== nextSlot - previousSlot - 1
  ) return null;
  return {
    id,
    kind,
    previous_slot: previousSlot,
    next_slot: nextSlot,
    previous_block_time: previousBlockTime,
    next_block_time: nextBlockTime,
    elapsed_secs: elapsedSecs,
    missing_slots: missingSlots
  };
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function integer(value: unknown): number | null {
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : null;
}

function string(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}
