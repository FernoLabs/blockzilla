#!/usr/bin/env node

import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const MAX_GET_BLOCKS_SLOTS = 500_000;
const COMMITMENT = 'finalized';
const MAINNET_BETA_GENESIS_HASH = '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d';
const NULL_BLOCK_TIME_RPC_CODES = new Set([-32004, -32007, -32009, -32011]);
const RETRYABLE_HTTP_STATUSES = new Set([408, 425, 429, 500, 502, 503, 504]);
const RETRYABLE_RPC_CODES = new Set([-32005, -32014, -32603]);

const DEFAULTS = Object.freeze({
  allowEmptyEpochs: false,
  cacheDir: 'target/epoch-calendar',
  cluster: 'mainnet-beta',
  concurrency: 1,
  datesOnly: false,
  estimateSlotMs: 400,
  forceRescan: false,
  maxAttempts: 8,
  output: 'src/lib/data/mainnet-epoch-calendar.json',
  refreshTimes: false,
  requestIntervalMs: 250,
  retryBaseMs: 1_000,
  slotEndpoint: '@SOLANA_RPC_URL',
  startSlot: 0,
  timeEndpoint: null,
  timeRequestIntervalMs: null,
  timeoutMs: 120_000,
  windowSlots: MAX_GET_BLOCKS_SLOTS
});

export function scanWindows(startSlot, endSlot, windowSlots = MAX_GET_BLOCKS_SLOTS) {
  assertSafeSlot(startSlot, 'start slot');
  assertSafeSlot(endSlot, 'end slot');
  assertPositiveInteger(windowSlots, 'window slots');
  if (windowSlots > MAX_GET_BLOCKS_SLOTS) {
    throw new Error(`window slots must be at most ${MAX_GET_BLOCKS_SLOTS}`);
  }
  if (endSlot < startSlot) throw new Error('end slot must be at least start slot');

  const windows = [];
  for (let start = startSlot; start <= endSlot;) {
    const end = Math.min(endSlot, start + windowSlots - 1);
    windows.push({ start_slot: start, end_slot: end });
    if (end === Number.MAX_SAFE_INTEGER) break;
    start = end + 1;
  }
  return windows;
}

export function normalizeEpochSchedule(value) {
  if (!isObject(value)) throw new Error('getEpochSchedule returned a non-object result');
  const schedule = {
    first_normal_epoch: value.firstNormalEpoch,
    first_normal_slot: value.firstNormalSlot,
    leader_schedule_slot_offset: value.leaderScheduleSlotOffset,
    slots_per_epoch: value.slotsPerEpoch,
    warmup: value.warmup
  };

  assertNonNegativeInteger(schedule.first_normal_epoch, 'firstNormalEpoch');
  assertSafeSlot(schedule.first_normal_slot, 'firstNormalSlot');
  assertNonNegativeInteger(schedule.leader_schedule_slot_offset, 'leaderScheduleSlotOffset');
  assertPositiveInteger(schedule.slots_per_epoch, 'slotsPerEpoch');
  if (typeof schedule.warmup !== 'boolean') throw new Error('warmup must be a boolean');

  if (!schedule.warmup) {
    if (schedule.first_normal_epoch !== 0 || schedule.first_normal_slot !== 0) {
      throw new Error('a non-warmup epoch schedule must start its normal schedule at epoch and slot 0');
    }
  } else if (schedule.first_normal_epoch === 0) {
    if (schedule.first_normal_slot !== 0) {
      throw new Error('firstNormalSlot must be 0 when firstNormalEpoch is 0');
    }
  } else {
    warmupBaseSlots(schedule);
  }

  return schedule;
}

export function assertClusterGenesis(cluster, genesisHash) {
  if (cluster === 'mainnet-beta' && genesisHash !== MAINNET_BETA_GENESIS_HASH) {
    throw new Error(
      `mainnet-beta requires genesis hash ${MAINNET_BETA_GENESIS_HASH}; ` +
      'refusing to write mainnet calendar data from another cluster'
    );
  }
}

export function epochBounds(epoch, schedule) {
  assertNonNegativeInteger(epoch, 'epoch');
  assertNormalizedSchedule(schedule);

  let firstSlot;
  let slotsInEpoch;
  if (schedule.warmup && epoch < schedule.first_normal_epoch) {
    const base = warmupBaseSlots(schedule);
    const multiplier = 2 ** epoch;
    firstSlot = base * (multiplier - 1);
    slotsInEpoch = base * multiplier;
  } else {
    firstSlot = schedule.first_normal_slot +
      (epoch - schedule.first_normal_epoch) * schedule.slots_per_epoch;
    slotsInEpoch = schedule.slots_per_epoch;
  }

  const lastSlot = firstSlot + slotsInEpoch - 1;
  assertSafeSlot(firstSlot, `first slot for epoch ${epoch}`);
  assertSafeSlot(lastSlot, `last slot for epoch ${epoch}`);
  return { epoch, first_slot: firstSlot, last_slot: lastSlot, slot_count: slotsInEpoch };
}

export function epochForSlot(slot, schedule) {
  assertSafeSlot(slot, 'slot');
  assertNormalizedSchedule(schedule);

  if (!schedule.warmup || slot >= schedule.first_normal_slot) {
    return schedule.first_normal_epoch +
      Math.floor((slot - schedule.first_normal_slot) / schedule.slots_per_epoch);
  }

  for (let epoch = 0; epoch < schedule.first_normal_epoch; epoch += 1) {
    const bounds = epochBounds(epoch, schedule);
    if (slot <= bounds.last_slot) return epoch;
  }
  throw new Error(`slot ${slot} is not covered by the epoch schedule`);
}

export function validateBlockSlots(value, startSlot, endSlot) {
  if (!Array.isArray(value)) throw new Error('getBlocks returned a non-array result');
  let previous = null;
  for (const slot of value) {
    assertSafeSlot(slot, 'getBlocks slot');
    if (slot < startSlot || slot > endSlot) {
      throw new Error(`getBlocks returned slot ${slot} outside ${startSlot}..${endSlot}`);
    }
    if (previous !== null && slot <= previous) {
      throw new Error(`getBlocks slots must be strictly sorted and unique (${previous}, ${slot})`);
    }
    previous = slot;
  }
  return value;
}

export function summarizeWindow(window, blockSlots, schedule) {
  const { start_slot: startSlot, end_slot: endSlot } = window;
  assertSafeSlot(startSlot, 'window start slot');
  assertSafeSlot(endSlot, 'window end slot');
  if (endSlot < startSlot) throw new Error('window end slot must be at least its start slot');
  validateBlockSlots(blockSlots, startSlot, endSlot);

  const summaries = [];
  let slotCursor = startSlot;
  let blockCursor = 0;
  while (slotCursor <= endSlot) {
    const epoch = epochForSlot(slotCursor, schedule);
    const bounds = epochBounds(epoch, schedule);
    const spanEnd = Math.min(endSlot, bounds.last_slot);
    const scannedSlotCount = spanEnd - slotCursor + 1;
    const firstBlockIndex = blockCursor;
    while (blockCursor < blockSlots.length && blockSlots[blockCursor] <= spanEnd) {
      blockCursor += 1;
    }
    const producedBlockCount = blockCursor - firstBlockIndex;
    summaries.push({
      epoch,
      first_produced_slot: producedBlockCount === 0 ? null : blockSlots[firstBlockIndex],
      last_produced_slot: producedBlockCount === 0 ? null : blockSlots[blockCursor - 1],
      produced_block_count: producedBlockCount,
      scanned_slot_count: scannedSlotCount,
      skipped_slot_count: scannedSlotCount - producedBlockCount
    });
    slotCursor = spanEnd + 1;
  }

  if (blockCursor !== blockSlots.length) {
    throw new Error('not all getBlocks slots were consumed while summarizing the window');
  }
  return summaries;
}

export function mergeEpochSummaries(summaryGroups, schedule) {
  const merged = new Map();
  for (const group of summaryGroups) {
    for (const summary of group) {
      validateEpochSummary(summary);
      const current = merged.get(summary.epoch);
      if (!current) {
        merged.set(summary.epoch, { ...summary });
        continue;
      }
      current.scanned_slot_count += summary.scanned_slot_count;
      current.produced_block_count += summary.produced_block_count;
      current.skipped_slot_count += summary.skipped_slot_count;
      current.first_produced_slot = minNullable(
        current.first_produced_slot,
        summary.first_produced_slot
      );
      current.last_produced_slot = maxNullable(
        current.last_produced_slot,
        summary.last_produced_slot
      );
    }
  }

  return [...merged.values()]
    .sort((left, right) => left.epoch - right.epoch)
    .map((summary) => {
      const bounds = epochBounds(summary.epoch, schedule);
      if (summary.scanned_slot_count > bounds.slot_count) {
        throw new Error(`epoch ${summary.epoch} was scanned more than once`);
      }
      return {
        epoch: summary.epoch,
        complete: summary.scanned_slot_count === bounds.slot_count,
        first_produced_slot: summary.first_produced_slot,
        last_produced_slot: summary.last_produced_slot,
        produced_block_count: summary.produced_block_count,
        scanned_slot_count: summary.scanned_slot_count,
        skipped_slot_count: summary.skipped_slot_count
      };
    });
}

export function buildEpochCalendar(epochStats, blockTimes, estimateSlotMs = 400) {
  if (!(blockTimes instanceof Map)) throw new Error('blockTimes must be a Map');
  if (!Number.isFinite(estimateSlotMs) || estimateSlotMs <= 0) {
    throw new Error('estimateSlotMs must be a positive number');
  }

  const observed = [...blockTimes.entries()]
    .filter(([slot, unixSecs]) => Number.isSafeInteger(slot) && Number.isSafeInteger(unixSecs) && unixSecs > 0)
    .map(([slot, unixSecs]) => ({ slot, unix_secs: unixSecs }))
    .sort((left, right) => left.slot - right.slot);
  if (observed.length === 0) return [];

  return [...epochStats]
    .sort((left, right) => left.epoch - right.epoch)
    .flatMap((stat) => {
      if (stat.first_produced_slot === null || stat.last_produced_slot === null) return [];
      const startObserved = validBlockTime(blockTimes.get(stat.first_produced_slot));
      const endObserved = validBlockTime(blockTimes.get(stat.last_produced_slot));
      const start = startObserved ?? estimateBlockTime(stat.first_produced_slot, observed, estimateSlotMs);
      let end = endObserved ?? estimateBlockTime(stat.last_produced_slot, observed, estimateSlotMs);
      if (start === null || end === null) return [];

      if (stat.complete === false) {
        return [{
          epoch: stat.epoch,
          start_unix_secs: start,
          end_unix_secs: null,
          precision: 'estimated'
        }];
      }

      let precision = startObserved !== null && endObserved !== null ? 'observed' : 'estimated';
      if (end < start) {
        end = start;
        precision = 'estimated';
      }
      return [{
        epoch: stat.epoch,
        start_unix_secs: start,
        end_unix_secs: end,
        precision
      }];
    });
}

export function assertCalendarCoverage(epochCalendar, epochStats) {
  const expectedEpochs = epochStats
    .filter((stat) => stat.first_produced_slot !== null && stat.last_produced_slot !== null)
    .map((stat) => stat.epoch)
    .sort((left, right) => left - right);
  const actualEpochs = epochCalendar.map((entry) => entry.epoch).sort((left, right) => left - right);
  if (expectedEpochs.length === 0) {
    throw new Error('calendar generation found no epochs with produced blocks');
  }
  if (stableJson(actualEpochs) !== stableJson(expectedEpochs)) {
    throw new Error(
      `calendar has ${actualEpochs.length} entries but ${expectedEpochs.length} epochs have block boundaries`
    );
  }
}

export function assertHistoryCoverage(firstAvailableBlock, startSlot, phase = 'before scan') {
  assertSafeSlot(firstAvailableBlock, 'first available block');
  if (firstAvailableBlock > startSlot) {
    throw new Error(
      `slot RPC history starts at block ${firstAvailableBlock}, after requested start slot ` +
      `${startSlot} (${phase}); refusing to count pruned history as skipped slots`
    );
  }
}

export function datesOnlyEpochRange(startSlot, cutoffSlot, finalizedSlot, schedule) {
  const firstEpoch = epochForSlot(startSlot, schedule);
  const startBounds = epochBounds(firstEpoch, schedule);
  if (startSlot !== startBounds.first_slot) {
    throw new Error('dates-only start slot must be an epoch boundary');
  }
  const cutoffEpoch = epochForSlot(cutoffSlot, schedule);
  const cutoffBounds = epochBounds(cutoffEpoch, schedule);
  let lastCompleteEpoch = cutoffSlot === cutoffBounds.last_slot
    ? cutoffEpoch
    : cutoffEpoch - 1;
  while (lastCompleteEpoch >= firstEpoch &&
         epochBounds(lastCompleteEpoch + 1, schedule).first_slot > finalizedSlot) {
    lastCompleteEpoch -= 1;
  }
  if (lastCompleteEpoch < firstEpoch) {
    throw new Error('dates-only mode needs at least one completed epoch after start slot');
  }
  return {
    first_epoch: firstEpoch,
    last_complete_epoch: lastCompleteEpoch,
    boundary_epochs: Array.from(
      { length: lastCompleteEpoch - firstEpoch + 2 },
      (_, index) => firstEpoch + index
    )
  };
}

export function createEnvelope({
  cluster,
  cutoffSlot,
  epochCalendar,
  epochSchedule,
  epochStats,
  firstAvailableBlock,
  generationMode,
  genesisHash,
  startSlot,
  windowSlots
}) {
  return {
    schema_version: 1,
    cluster,
    genesis_hash: genesisHash,
    generation_mode: generationMode,
    commitment: COMMITMENT,
    start_slot: startSlot,
    cutoff_slot: cutoffSlot,
    first_available_block: firstAvailableBlock,
    window_slots: windowSlots,
    epoch_schedule: { ...epochSchedule },
    epoch_calendar: [...epochCalendar]
      .sort((left, right) => left.epoch - right.epoch)
      .map((entry) => ({
        epoch: entry.epoch,
        start_unix_secs: entry.start_unix_secs,
        end_unix_secs: entry.end_unix_secs,
        precision: entry.precision
      })),
    ...(epochStats === null ? {} : {
      epoch_stats: [...epochStats]
        .sort((left, right) => left.epoch - right.epoch)
        .map((stat) => ({
          epoch: stat.epoch,
          complete: stat.complete,
          first_produced_slot: stat.first_produced_slot,
          last_produced_slot: stat.last_produced_slot,
          produced_block_count: stat.produced_block_count,
          scanned_slot_count: stat.scanned_slot_count,
          skipped_slot_count: stat.skipped_slot_count
        }))
    })
  };
}

export function stableJson(value) {
  return `${JSON.stringify(sortJsonValue(value), null, 2)}\n`;
}

class JsonRpcClient {
  constructor(endpoint, options) {
    this.endpoint = endpoint;
    this.maxAttempts = options.maxAttempts;
    this.minimumIntervalMs = options.minimumIntervalMs;
    this.retryBaseMs = options.retryBaseMs;
    this.timeoutMs = options.timeoutMs;
    this.lastRequestStartedAt = 0;
    this.nextId = 1;
    this.throttleTail = Promise.resolve();
  }

  async call(method, params = [], options = {}) {
    let lastError = null;
    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      try {
        await this.#throttle();
        const requestId = this.nextId;
        this.nextId += 1;
        const response = await fetch(this.endpoint, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ jsonrpc: '2.0', id: requestId, method, params }),
          signal: AbortSignal.timeout(this.timeoutMs)
        });

        if (!response.ok) {
          const retryAfterMs = parseRetryAfter(response.headers.get('retry-after'));
          throw new RequestError(
            `${method} failed with HTTP ${response.status}`,
            RETRYABLE_HTTP_STATUSES.has(response.status),
            retryAfterMs
          );
        }

        const payload = await response.json();
        if (!isObject(payload) || payload.jsonrpc !== '2.0' || payload.id !== requestId) {
          throw new RequestError(`${method} returned an invalid JSON-RPC envelope`, false);
        }
        if (isObject(payload.error)) {
          const code = Number(payload.error.code);
          if (options.nullOnRpcCodes?.has(code)) return null;
          const message = typeof payload.error.message === 'string'
            ? payload.error.message
            : 'unknown JSON-RPC error';
          throw new RequestError(
            `${method} failed with RPC ${code}: ${message}`,
            RETRYABLE_RPC_CODES.has(code)
          );
        }
        if (!Object.hasOwn(payload, 'result')) {
          throw new RequestError(`${method} returned no result`, false);
        }
        return payload.result;
      } catch (error) {
        lastError = normalizeRequestError(error, method);
        if (!lastError.retryable || attempt === this.maxAttempts) throw lastError;
        const exponentialDelay = this.retryBaseMs * (2 ** (attempt - 1));
        await delay(Math.max(exponentialDelay, lastError.retryAfterMs ?? 0));
      }
    }
    throw lastError ?? new Error(`${method} failed`);
  }

  async #throttle() {
    let release;
    const previous = this.throttleTail;
    this.throttleTail = new Promise((resolveThrottle) => {
      release = resolveThrottle;
    });
    await previous;
    try {
      const waitMs = this.lastRequestStartedAt + this.minimumIntervalMs - Date.now();
      if (waitMs > 0) await delay(waitMs);
      this.lastRequestStartedAt = Date.now();
    } finally {
      release();
    }
  }
}

class RequestError extends Error {
  constructor(message, retryable, retryAfterMs = null, cause = undefined) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'RequestError';
    this.retryable = retryable;
    this.retryAfterMs = retryAfterMs;
  }
}

async function main() {
  const options = parseCli(process.argv.slice(2));
  if (options.help) {
    process.stdout.write(helpText());
    return;
  }

  const slotEndpoint = resolveEndpoint(options.slotEndpoint, 'slot endpoint');
  const timeEndpoint = options.timeEndpoint === null
    ? slotEndpoint
    : resolveEndpoint(options.timeEndpoint, 'time endpoint');
  const commonClientOptions = {
    maxAttempts: options.maxAttempts,
    minimumIntervalMs: options.requestIntervalMs,
    retryBaseMs: options.retryBaseMs,
    timeoutMs: options.timeoutMs
  };
  const slotClient = new JsonRpcClient(slotEndpoint, commonClientOptions);
  const timeClient = timeEndpoint === slotEndpoint
    ? slotClient
    : new JsonRpcClient(timeEndpoint, {
        ...commonClientOptions,
        minimumIntervalMs: options.timeRequestIntervalMs ?? options.requestIntervalMs
      });

  const genesisHash = validateGenesisHash(await slotClient.call('getGenesisHash'));
  assertClusterGenesis(options.cluster, genesisHash);
  if (timeClient !== slotClient) {
    const timeGenesisHash = validateGenesisHash(await timeClient.call('getGenesisHash'));
    if (timeGenesisHash !== genesisHash) {
      throw new Error('slot and time RPC endpoints report different genesis hashes');
    }
  }
  const timeClients = timeClient === slotClient
    ? [timeClient]
    : [timeClient, slotClient];
  const epochSchedule = normalizeEpochSchedule(await slotClient.call('getEpochSchedule'));
  if (epochSchedule.warmup ||
      epochSchedule.first_normal_epoch !== 0 ||
      epochSchedule.first_normal_slot !== 0 ||
      epochSchedule.slots_per_epoch !== 432_000) {
    throw new Error(
      'RPC epoch schedule does not match Blockzilla epoch numbering ' +
      '(432000 slots, epoch 0 at slot 0, no warmup)'
    );
  }
  const firstAvailableBlock = await slotClient.call('getFirstAvailableBlock');
  assertHistoryCoverage(firstAvailableBlock, options.startSlot);
  const finalizedSlot = await slotClient.call('getSlot', [{ commitment: COMMITMENT }]);
  assertSafeSlot(finalizedSlot, 'finalized slot');
  const cutoffSlot = options.cutoffSlot ?? finalizedSlot;
  if (cutoffSlot > finalizedSlot) {
    throw new Error(`cutoff slot ${cutoffSlot} is newer than finalized slot ${finalizedSlot}`);
  }
  if (cutoffSlot < options.startSlot) {
    throw new Error('cutoff slot must be at least start slot');
  }

  const chainCacheDir = resolve(options.cacheDir, sanitizePathSegment(genesisHash));
  const startEpochBounds = epochBounds(epochForSlot(options.startSlot, epochSchedule), epochSchedule);
  if (options.startSlot !== startEpochBounds.first_slot) {
    throw new Error(
      `start slot ${options.startSlot} is inside epoch ${startEpochBounds.epoch}; ` +
      `use its first slot ${startEpochBounds.first_slot} so calendar starts remain truthful`
    );
  }
  if (options.datesOnly) {
    const epochCalendar = await generateDatesOnlyCalendar({
      chainCacheDir,
      concurrency: options.concurrency,
      cutoffSlot,
      epochSchedule,
      finalizedSlot,
      forceRescan: options.forceRescan,
      genesisHash,
      refreshTimes: options.refreshTimes,
      slotClient,
      startSlot: options.startSlot,
      timeClients,
      estimateSlotMs: options.estimateSlotMs
    });
    const envelope = createEnvelope({
      cluster: options.cluster,
      cutoffSlot,
      epochCalendar,
      epochSchedule,
      epochStats: null,
      firstAvailableBlock,
      generationMode: 'dates-only',
      genesisHash,
      startSlot: options.startSlot,
      windowSlots: options.windowSlots
    });
    const outputPath = resolve(options.output);
    await atomicWriteJson(outputPath, envelope);
    process.stderr.write(
      `Wrote ${epochCalendar.length} dates-only calendar entries to ${outputPath}.\n`
    );
    return;
  }

  const windowCacheDir = join(chainCacheDir, 'windows');
  const windows = scanWindows(options.startSlot, cutoffSlot, options.windowSlots);
  const summaryGroups = new Array(windows.length);
  process.stderr.write(
    `Scanning ${options.startSlot}..${cutoffSlot} in ${windows.length} finalized window(s) ` +
    `with concurrency ${options.concurrency}.\n`
  );

  let nextWindowIndex = 0;
  const scanWorker = async () => {
    while (nextWindowIndex < windows.length) {
      const index = nextWindowIndex;
      nextWindowIndex += 1;
      const window = windows[index];
      const checkpointPath = join(windowCacheDir, checkpointFilename(window));
      let checkpoint = options.forceRescan ? null : await readJsonIfExists(checkpointPath);
      if (checkpoint !== null) {
        checkpoint = validateWindowCheckpoint(
          checkpoint,
          window,
          genesisHash,
          epochSchedule
        );
        process.stderr.write(
          `[${index + 1}/${windows.length}] reused ${window.start_slot}..${window.end_slot}\n`
        );
      } else {
        const summaries = await scanOneWindow(slotClient, window, epochSchedule);
        checkpoint = {
          schema_version: 1,
          genesis_hash: genesisHash,
          commitment: COMMITMENT,
          start_slot: window.start_slot,
          end_slot: window.end_slot,
          epoch_schedule: epochSchedule,
          epoch_stats: summaries
        };
        await atomicWriteJson(checkpointPath, checkpoint);
        process.stderr.write(
          `[${index + 1}/${windows.length}] saved ${window.start_slot}..${window.end_slot}\n`
        );
      }
      summaryGroups[index] = checkpoint.epoch_stats;
    }
  };
  await Promise.all(Array.from({ length: options.concurrency }, () => scanWorker()));
  const finalFirstAvailableBlock = await slotClient.call('getFirstAvailableBlock');
  assertHistoryCoverage(finalFirstAvailableBlock, options.startSlot, 'after scan');

  const epochStats = mergeEpochSummaries(summaryGroups, epochSchedule);
  const emptyCompletedEpochs = epochStats
    .filter((stat) => stat.complete && stat.produced_block_count === 0)
    .map((stat) => stat.epoch);
  if (emptyCompletedEpochs.length > 0 && !options.allowEmptyEpochs) {
    throw new Error(
      `completed epoch(s) ${emptyCompletedEpochs.join(', ')} have no produced blocks; ` +
      `verify RPC history or pass --allow-empty-epochs if this is expected`
    );
  }
  const boundarySlots = [...new Set(epochStats.flatMap((stat) => [
    stat.first_produced_slot,
    stat.last_produced_slot
  ]).filter((slot) => slot !== null))].sort((left, right) => left - right);
  const blockTimes = await fetchBlockTimes(
    timeClients,
    join(chainCacheDir, 'block-times.v1.json'),
    genesisHash,
    boundarySlots,
    options.refreshTimes,
    options.concurrency
  );

  const relevantTimes = new Map(boundarySlots.map((slot) => [slot, blockTimes.get(slot) ?? null]));
  const epochCalendar = buildEpochCalendar(
    epochStats,
    relevantTimes,
    options.estimateSlotMs
  );
  assertCalendarCoverage(epochCalendar, epochStats);
  const envelope = createEnvelope({
    cluster: options.cluster,
    cutoffSlot,
    epochCalendar,
    epochSchedule,
    epochStats,
    firstAvailableBlock,
    generationMode: 'full-scan',
    genesisHash,
    startSlot: options.startSlot,
    windowSlots: options.windowSlots
  });
  const outputPath = resolve(options.output);
  await atomicWriteJson(outputPath, envelope);
  process.stderr.write(
    `Wrote ${epochCalendar.length} calendar entries and ${epochStats.length} epoch stats to ${outputPath}.\n`
  );
}

async function generateDatesOnlyCalendar({
  chainCacheDir,
  concurrency,
  cutoffSlot,
  epochSchedule,
  estimateSlotMs,
  finalizedSlot,
  forceRescan,
  genesisHash,
  refreshTimes,
  slotClient,
  startSlot,
  timeClients
}) {
  const range = datesOnlyEpochRange(startSlot, cutoffSlot, finalizedSlot, epochSchedule);
  const firstEpoch = range.first_epoch;
  const lastCompleteEpoch = range.last_complete_epoch;
  const boundaryEpochs = range.boundary_epochs;
  const boundaryCachePath = join(chainCacheDir, 'epoch-boundaries.v1.json');
  const boundaries = forceRescan || refreshTimes
    ? new Map()
    : await loadEpochBoundaries(boundaryCachePath, genesisHash, epochSchedule);
  let saveTail = Promise.resolve();
  let nextIndex = 0;
  const worker = async () => {
    while (nextIndex < boundaryEpochs.length) {
      const index = nextIndex;
      nextIndex += 1;
      const epoch = boundaryEpochs[index];
      if (boundaries.has(epoch)) continue;
      const boundary = await fetchEpochBoundary(slotClient, epoch, epochSchedule);
      boundaries.set(epoch, boundary);
      saveTail = saveTail.then(() => saveEpochBoundaries(
        boundaryCachePath,
        genesisHash,
        epochSchedule,
        boundaries
      ));
      await saveTail;
      process.stderr.write(`Fetched epoch boundary ${index + 1}/${boundaryEpochs.length}.\n`);
    }
  };
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  await saveTail;

  const dateStats = [];
  for (let epoch = firstEpoch; epoch <= lastCompleteEpoch; epoch += 1) {
    const current = boundaries.get(epoch);
    const next = boundaries.get(epoch + 1);
    const bounds = epochBounds(epoch, epochSchedule);
    if (!current || !next || current.first_produced_slot === null || next.parent_slot === null) {
      throw new Error(`cannot derive dates for empty or unavailable epoch ${epoch}`);
    }
    if (next.parent_slot < current.first_produced_slot || next.parent_slot > bounds.last_slot) {
      throw new Error(`epoch ${epoch} has an invalid last produced slot ${next.parent_slot}`);
    }
    dateStats.push({
      epoch,
      complete: true,
      first_produced_slot: current.first_produced_slot,
      last_produced_slot: next.parent_slot
    });
  }

  // Calendar placement needs an observed start for each epoch. The final end is
  // fetched as an anchor; intermediate ends are interpolated between starts.
  const boundarySlots = [...new Set([
    ...dateStats.map((stat) => stat.first_produced_slot),
    dateStats.at(-1).last_produced_slot
  ])].sort((left, right) => left - right);
  const boundaryTimes = await fetchBlockTimes(
    timeClients,
    join(chainCacheDir, 'block-times.v1.json'),
    genesisHash,
    boundarySlots,
    refreshTimes,
    concurrency
  );
  const relevantTimes = new Map();
  for (const stat of dateStats) {
    relevantTimes.set(stat.first_produced_slot, boundaryTimes.get(stat.first_produced_slot) ?? null);
    relevantTimes.set(stat.last_produced_slot, boundaryTimes.get(stat.last_produced_slot) ?? null);
  }
  const epochCalendar = buildEpochCalendar(dateStats, relevantTimes, estimateSlotMs);
  assertCalendarCoverage(epochCalendar, dateStats);
  return epochCalendar;
}

async function fetchEpochBoundary(client, epoch, schedule) {
  const bounds = epochBounds(epoch, schedule);
  const slots = await client.call('getBlocksWithLimit', [
    bounds.first_slot,
    1,
    { commitment: COMMITMENT }
  ]);
  if (!Array.isArray(slots) || slots.length > 1) {
    throw new Error(`getBlocksWithLimit returned an invalid result for epoch ${epoch}`);
  }
  if (slots.length === 0 || slots[0] > bounds.last_slot) {
    return { epoch, first_produced_slot: null, parent_slot: null };
  }
  validateBlockSlots(slots, bounds.first_slot, bounds.last_slot);
  const firstProducedSlot = slots[0];
  const block = await client.call('getBlock', [firstProducedSlot, {
    commitment: COMMITMENT,
    maxSupportedTransactionVersion: 0,
    rewards: false,
    transactionDetails: 'none'
  }]);
  if (!isObject(block)) throw new Error(`getBlock returned no metadata for slot ${firstProducedSlot}`);
  assertSafeSlot(block.parentSlot, `parent slot for block ${firstProducedSlot}`);
  return {
    epoch,
    first_produced_slot: firstProducedSlot,
    parent_slot: block.parentSlot
  };
}

async function scanOneWindow(client, window, schedule) {
  let blockSlots = await client.call('getBlocks', [
    window.start_slot,
    window.end_slot,
    { commitment: COMMITMENT }
  ]);
  validateBlockSlots(blockSlots, window.start_slot, window.end_slot);
  const summaries = summarizeWindow(window, blockSlots, schedule);
  blockSlots = null;
  return summaries;
}

function validateWindowCheckpoint(checkpoint, window, genesisHash, epochSchedule) {
  if (!isObject(checkpoint) ||
      checkpoint.schema_version !== 1 ||
      checkpoint.genesis_hash !== genesisHash ||
      checkpoint.commitment !== COMMITMENT ||
      checkpoint.start_slot !== window.start_slot ||
      checkpoint.end_slot !== window.end_slot ||
      stableJson(checkpoint.epoch_schedule) !== stableJson(epochSchedule) ||
      !Array.isArray(checkpoint.epoch_stats)) {
    throw new Error(
      `checkpoint ${window.start_slot}..${window.end_slot} does not match this scan`
    );
  }

  const expectedSpans = summarizeWindow(window, [], epochSchedule);
  if (checkpoint.epoch_stats.length !== expectedSpans.length) {
    throw new Error(`checkpoint ${window.start_slot}..${window.end_slot} has invalid epoch spans`);
  }
  for (let index = 0; index < checkpoint.epoch_stats.length; index += 1) {
    const summary = checkpoint.epoch_stats[index];
    const expected = expectedSpans[index];
    validateEpochSummary(summary);
    if (summary.epoch !== expected.epoch ||
        summary.scanned_slot_count !== expected.scanned_slot_count) {
      throw new Error(`checkpoint ${window.start_slot}..${window.end_slot} has invalid coverage`);
    }
    const epochSpan = epochBounds(summary.epoch, epochSchedule);
    const spanStart = Math.max(window.start_slot, epochSpan.first_slot);
    const spanEnd = Math.min(window.end_slot, epochSpan.last_slot);
    if (summary.first_produced_slot !== null &&
        (summary.first_produced_slot < spanStart ||
         summary.first_produced_slot > spanEnd)) {
      throw new Error(`checkpoint ${window.start_slot}..${window.end_slot} has an invalid first slot`);
    }
    if (summary.last_produced_slot !== null &&
        (summary.last_produced_slot < spanStart ||
         summary.last_produced_slot > spanEnd)) {
      throw new Error(`checkpoint ${window.start_slot}..${window.end_slot} has an invalid last slot`);
    }
  }
  return checkpoint;
}

function validateEpochSummary(summary) {
  if (!isObject(summary)) throw new Error('epoch summary must be an object');
  assertNonNegativeInteger(summary.epoch, 'summary epoch');
  assertPositiveInteger(summary.scanned_slot_count, 'summary scanned slot count');
  assertNonNegativeInteger(summary.produced_block_count, 'summary produced block count');
  assertNonNegativeInteger(summary.skipped_slot_count, 'summary skipped slot count');
  if (summary.produced_block_count + summary.skipped_slot_count !== summary.scanned_slot_count) {
    throw new Error(`epoch ${summary.epoch} summary counts do not add up`);
  }
  if (summary.first_produced_slot !== null) {
    assertSafeSlot(summary.first_produced_slot, 'summary first produced slot');
  }
  if (summary.last_produced_slot !== null) {
    assertSafeSlot(summary.last_produced_slot, 'summary last produced slot');
  }
  if (summary.produced_block_count === 0 &&
      (summary.first_produced_slot !== null || summary.last_produced_slot !== null)) {
    throw new Error(`epoch ${summary.epoch} has block bounds but no produced blocks`);
  }
  if (summary.produced_block_count > 0 &&
      (summary.first_produced_slot === null || summary.last_produced_slot === null ||
       summary.first_produced_slot > summary.last_produced_slot)) {
    throw new Error(`epoch ${summary.epoch} has invalid produced block bounds`);
  }
  if (summary.produced_block_count === 1 &&
      summary.first_produced_slot !== summary.last_produced_slot) {
    throw new Error(`epoch ${summary.epoch} has implausible bounds for one produced block`);
  }
  if (summary.produced_block_count > 1 &&
      (summary.first_produced_slot === summary.last_produced_slot ||
       summary.last_produced_slot - summary.first_produced_slot + 1 < summary.produced_block_count)) {
    throw new Error(`epoch ${summary.epoch} produced block count does not fit its bounds`);
  }
}

async function loadEpochBoundaries(path, genesisHash, epochSchedule) {
  const cache = await readJsonIfExists(path);
  if (cache === null) return new Map();
  if (!isObject(cache) ||
      cache.schema_version !== 1 ||
      cache.genesis_hash !== genesisHash ||
      cache.commitment !== COMMITMENT ||
      stableJson(cache.epoch_schedule) !== stableJson(epochSchedule) ||
      !Array.isArray(cache.epoch_boundaries)) {
    throw new Error('epoch-boundary cache does not match this chain');
  }

  const boundaries = new Map();
  for (const boundary of cache.epoch_boundaries) {
    validateEpochBoundary(boundary, epochSchedule);
    if (boundaries.has(boundary.epoch)) {
      throw new Error(`duplicate cached boundary for epoch ${boundary.epoch}`);
    }
    boundaries.set(boundary.epoch, {
      epoch: boundary.epoch,
      first_produced_slot: boundary.first_produced_slot,
      parent_slot: boundary.parent_slot
    });
  }
  return boundaries;
}

async function saveEpochBoundaries(path, genesisHash, epochSchedule, boundaries) {
  await atomicWriteJson(path, {
    schema_version: 1,
    genesis_hash: genesisHash,
    commitment: COMMITMENT,
    epoch_schedule: epochSchedule,
    epoch_boundaries: [...boundaries.values()].sort((left, right) => left.epoch - right.epoch)
  });
}

function validateEpochBoundary(boundary, schedule) {
  if (!isObject(boundary)) throw new Error('epoch boundary must be an object');
  assertNonNegativeInteger(boundary.epoch, 'boundary epoch');
  const bounds = epochBounds(boundary.epoch, schedule);
  if (boundary.first_produced_slot === null) {
    if (boundary.parent_slot !== null) {
      throw new Error(`empty epoch ${boundary.epoch} has unexpected block metadata`);
    }
    return;
  }
  assertSafeSlot(boundary.first_produced_slot, 'boundary first produced slot');
  if (boundary.first_produced_slot < bounds.first_slot ||
      boundary.first_produced_slot > bounds.last_slot) {
    throw new Error(`epoch ${boundary.epoch} boundary slot is outside the epoch`);
  }
  assertSafeSlot(boundary.parent_slot, 'boundary parent slot');
}

async function fetchBlockTimes(clients, path, genesisHash, slots, refresh, concurrency = 1) {
  const blockTimes = await loadBlockTimes(path, genesisHash, refresh);
  const missing = slots
    .map((slot, index) => ({ index, slot }))
    .filter(({ slot }) => !blockTimes.has(slot));
  let completedCount = slots.length - missing.length;
  let nextIndex = 0;
  let saveTail = Promise.resolve();
  const worker = async () => {
    while (nextIndex < missing.length) {
      const missingIndex = nextIndex;
      nextIndex += 1;
      const { slot } = missing[missingIndex];
      const client = clients[missingIndex % clients.length];
      const value = await client.call('getBlockTime', [slot], {
        nullOnRpcCodes: NULL_BLOCK_TIME_RPC_CODES
      });
      if (value !== null) assertPositiveInteger(value, `getBlockTime(${slot})`);
      blockTimes.set(slot, value);
      saveTail = saveTail.then(() => saveBlockTimes(path, genesisHash, blockTimes));
      await saveTail;
      completedCount += 1;
      process.stderr.write(`Fetched block time ${completedCount}/${slots.length}.\n`);
    }
  };
  const workerCount = Math.min(concurrency, missing.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  await saveTail;
  return blockTimes;
}

async function loadBlockTimes(path, genesisHash, refresh) {
  if (refresh) return new Map();
  const cache = await readJsonIfExists(path);
  if (cache === null) return new Map();
  if (!isObject(cache) ||
      cache.schema_version !== 1 ||
      cache.genesis_hash !== genesisHash ||
      cache.commitment !== COMMITMENT ||
      !Array.isArray(cache.block_times)) {
    throw new Error('block-time cache does not match this chain');
  }

  const times = new Map();
  for (const entry of cache.block_times) {
    if (!isObject(entry)) throw new Error('block-time cache contains a non-object entry');
    assertSafeSlot(entry.slot, 'cached block-time slot');
    if (entry.unix_secs !== null) {
      assertPositiveInteger(entry.unix_secs, 'cached block time');
    }
    if (times.has(entry.slot)) throw new Error(`duplicate cached block time for slot ${entry.slot}`);
    times.set(entry.slot, entry.unix_secs);
  }
  return times;
}

async function saveBlockTimes(path, genesisHash, blockTimes) {
  await atomicWriteJson(path, {
    schema_version: 1,
    genesis_hash: genesisHash,
    commitment: COMMITMENT,
    block_times: [...blockTimes.entries()]
      .sort(([left], [right]) => left - right)
      .map(([slot, unixSecs]) => ({ slot, unix_secs: unixSecs }))
  });
}

function estimateBlockTime(slot, observed, estimateSlotMs) {
  if (observed.length === 0) return null;
  let upperIndex = observed.findIndex((entry) => entry.slot > slot);
  if (upperIndex === -1) upperIndex = observed.length;
  const lower = upperIndex > 0 ? observed[upperIndex - 1] : null;
  const upper = upperIndex < observed.length ? observed[upperIndex] : null;

  let anchor;
  let slope;
  if (lower !== null && upper !== null) {
    anchor = lower;
    slope = observedSlope(lower, upper, estimateSlotMs);
  } else if (lower !== null) {
    anchor = lower;
    const previous = observed.length > 1 ? observed[observed.length - 2] : null;
    slope = previous === null ? estimateSlotMs / 1_000 : observedSlope(previous, lower, estimateSlotMs);
  } else {
    anchor = upper;
    const next = observed.length > 1 ? observed[1] : null;
    slope = next === null ? estimateSlotMs / 1_000 : observedSlope(upper, next, estimateSlotMs);
  }

  return Math.max(1, Math.round(anchor.unix_secs + (slot - anchor.slot) * slope));
}

function observedSlope(left, right, estimateSlotMs) {
  const slotDelta = right.slot - left.slot;
  const timeDelta = right.unix_secs - left.unix_secs;
  if (slotDelta <= 0 || timeDelta <= 0) return estimateSlotMs / 1_000;
  return timeDelta / slotDelta;
}

function validBlockTime(value) {
  return Number.isSafeInteger(value) && value > 0 ? value : null;
}

function warmupBaseSlots(schedule) {
  const divisor = (2 ** schedule.first_normal_epoch) - 1;
  if (!Number.isSafeInteger(divisor) || divisor <= 0 || schedule.first_normal_slot % divisor !== 0) {
    throw new Error('warmup epoch schedule has inconsistent normal epoch and slot values');
  }
  const base = schedule.first_normal_slot / divisor;
  if (!Number.isSafeInteger(base) || base <= 0) {
    throw new Error('warmup epoch schedule has an invalid base epoch length');
  }
  return base;
}

function assertNormalizedSchedule(schedule) {
  if (!isObject(schedule) ||
      !Number.isSafeInteger(schedule.first_normal_epoch) ||
      !Number.isSafeInteger(schedule.first_normal_slot) ||
      !Number.isSafeInteger(schedule.leader_schedule_slot_offset) ||
      !Number.isSafeInteger(schedule.slots_per_epoch) ||
      typeof schedule.warmup !== 'boolean') {
    throw new Error('invalid normalized epoch schedule');
  }
}

function parseCli(args) {
  const options = { ...DEFAULTS, cutoffSlot: null, help: false };
  const valueOptions = new Map([
    ['--cache-dir', ['cacheDir', String]],
    ['--cluster', ['cluster', String]],
    ['--concurrency', ['concurrency', parseIntegerOption]],
    ['--cutoff-slot', ['cutoffSlot', parseIntegerOption]],
    ['--estimate-slot-ms', ['estimateSlotMs', parseNumberOption]],
    ['--max-attempts', ['maxAttempts', parseIntegerOption]],
    ['--output', ['output', String]],
    ['--request-interval-ms', ['requestIntervalMs', parseIntegerOption]],
    ['--retry-base-ms', ['retryBaseMs', parseIntegerOption]],
    ['--slot-endpoint', ['slotEndpoint', String]],
    ['--start-slot', ['startSlot', parseIntegerOption]],
    ['--time-endpoint', ['timeEndpoint', String]],
    ['--time-request-interval-ms', ['timeRequestIntervalMs', parseIntegerOption]],
    ['--timeout-ms', ['timeoutMs', parseIntegerOption]],
    ['--window-slots', ['windowSlots', parseIntegerOption]]
  ]);

  for (let index = 0; index < args.length; index += 1) {
    const raw = args[index];
    if (raw === '--help' || raw === '-h') {
      options.help = true;
      continue;
    }
    if (raw === '--force-rescan') {
      options.forceRescan = true;
      continue;
    }
    if (raw === '--refresh-times') {
      options.refreshTimes = true;
      continue;
    }
    if (raw === '--dates-only') {
      options.datesOnly = true;
      continue;
    }
    if (raw === '--allow-empty-epochs') {
      options.allowEmptyEpochs = true;
      continue;
    }
    const [name, inlineValue] = splitOption(raw);
    const spec = valueOptions.get(name);
    if (!spec) throw new Error(`unknown option: ${name}`);
    const value = inlineValue ?? args[++index];
    if (value === undefined || value.startsWith('--')) throw new Error(`${name} requires a value`);
    options[spec[0]] = spec[1](value, name);
  }

  assertSafeSlot(options.startSlot, 'start slot');
  if (options.cutoffSlot !== null) assertSafeSlot(options.cutoffSlot, 'cutoff slot');
  assertPositiveInteger(options.windowSlots, 'window slots');
  if (options.windowSlots > MAX_GET_BLOCKS_SLOTS) {
    throw new Error(`window slots must be at most ${MAX_GET_BLOCKS_SLOTS}`);
  }
  assertNonNegativeInteger(options.requestIntervalMs, 'request interval');
  assertPositiveInteger(options.concurrency, 'concurrency');
  const maxConcurrency = options.datesOnly ? 16 : 8;
  if (options.concurrency > maxConcurrency) {
    throw new Error(
      `concurrency must be at most ${maxConcurrency} in ` +
      `${options.datesOnly ? 'dates-only' : 'full-scan'} mode`
    );
  }
  if (options.timeRequestIntervalMs !== null) {
    assertNonNegativeInteger(options.timeRequestIntervalMs, 'time request interval');
  }
  assertPositiveInteger(options.maxAttempts, 'max attempts');
  assertPositiveInteger(options.retryBaseMs, 'retry base');
  assertPositiveInteger(options.timeoutMs, 'timeout');
  if (!Number.isFinite(options.estimateSlotMs) || options.estimateSlotMs <= 0) {
    throw new Error('estimate slot milliseconds must be positive');
  }
  options.cluster = options.cluster.trim();
  if (!options.cluster) throw new Error('cluster must not be empty');
  return options;
}

function helpText() {
  return `Usage: npm run generate:epoch-calendar -- [options]\n\n` +
    `Writes a deterministic watcher calendar. The default full scan visits every finalized\n` +
    `slot exactly once; --dates-only fetches only epoch boundaries for a quick bootstrap.\n` +
    `Endpoint values may be URLs or @ENV_VAR references; the latter\n` +
    `keeps credentials out of shell history and generated files.\n\n` +
    `Options:\n` +
    `  --slot-endpoint VALUE          Slot RPC (default @SOLANA_RPC_URL)\n` +
    `  --time-endpoint VALUE          Optional separate getBlockTime RPC\n` +
    `  --output PATH                  Output JSON (default src/lib/data/mainnet-epoch-calendar.json)\n` +
    `  --cache-dir PATH               Checkpoint root (default target/epoch-calendar)\n` +
    `  --cluster NAME                 Output cluster label (default mainnet-beta)\n` +
    `  --concurrency N                Concurrent RPC calls; max 16 dates-only, 8 full\n` +
    `  --start-slot N                 First scanned slot (default 0)\n` +
    `  --cutoff-slot N                Stop at this finalized slot (default current finalized)\n` +
    `  --window-slots N               getBlocks window, max/default 500000\n` +
    `  --request-interval-ms N        Minimum delay between slot RPC calls (default 250)\n` +
    `  --time-request-interval-ms N   Delay for a separate time RPC\n` +
    `  --max-attempts N               Request attempts (default 8)\n` +
    `  --retry-base-ms N              Exponential retry base (default 1000)\n` +
    `  --timeout-ms N                 Per-request timeout (default 120000)\n` +
    `  --estimate-slot-ms N           Fallback slot duration (default 400)\n` +
    `  --dates-only                   Fetch epoch boundaries; omit exact skipped-slot stats\n` +
    `  --allow-empty-epochs           Accept a completed epoch with zero blocks\n` +
    `  --force-rescan                 Ignore scan and boundary checkpoints\n` +
    `  --refresh-times                Ignore cached block-time results\n` +
    `  -h, --help                     Show this help\n`;
}

function resolveEndpoint(spec, label) {
  if (typeof spec !== 'string' || spec.length === 0) throw new Error(`${label} must not be empty`);
  let value = spec;
  if (spec.startsWith('@')) {
    const variable = spec.slice(1);
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(variable)) {
      throw new Error(`${label} has an invalid environment variable reference`);
    }
    value = process.env[variable];
    if (!value) throw new Error(`${label} environment variable ${variable} is not set`);
  }
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`${label} must be an HTTP(S) URL or @ENV_VAR reference`);
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(`${label} must use HTTP(S)`);
  }
  return parsed.href;
}

function validateGenesisHash(value) {
  if (typeof value !== 'string' || !/^[1-9A-HJ-NP-Za-km-z]{32,64}$/.test(value)) {
    throw new Error('getGenesisHash returned an invalid hash');
  }
  return value;
}

async function readJsonIfExists(path) {
  try {
    return JSON.parse(await readFile(path, 'utf8'));
  } catch (error) {
    if (error?.code === 'ENOENT') return null;
    if (error instanceof SyntaxError) throw new Error(`invalid JSON checkpoint at ${path}`, { cause: error });
    throw error;
  }
}

async function atomicWriteJson(path, value) {
  await mkdir(dirname(path), { recursive: true });
  const temporaryPath = `${path}.${process.pid}.tmp`;
  await writeFile(temporaryPath, stableJson(value), { encoding: 'utf8', mode: 0o644 });
  await rename(temporaryPath, path);
}

function checkpointFilename(window) {
  const width = 15;
  return `${String(window.start_slot).padStart(width, '0')}-${String(window.end_slot).padStart(width, '0')}.v1.json`;
}

function sanitizePathSegment(value) {
  return value.replaceAll(/[^A-Za-z0-9_-]/g, '_');
}

function normalizeRequestError(error, method) {
  if (error instanceof RequestError) return error;
  if (error?.name === 'AbortError' || error?.name === 'TimeoutError' || error instanceof TypeError) {
    return new RequestError(`${method} request failed`, true, null, error);
  }
  return new RequestError(
    error instanceof Error ? error.message : `${method} request failed`,
    false,
    null,
    error
  );
}

function parseRetryAfter(value) {
  if (value === null) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds * 1_000);
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? Math.max(0, timestamp - Date.now()) : null;
}

function sortJsonValue(value) {
  if (Array.isArray(value)) return value.map(sortJsonValue);
  if (!isObject(value)) return value;
  return Object.fromEntries(
    Object.keys(value)
      .sort()
      .map((key) => [key, sortJsonValue(value[key])])
  );
}

function splitOption(value) {
  const separator = value.indexOf('=');
  return separator === -1
    ? [value, undefined]
    : [value.slice(0, separator), value.slice(separator + 1)];
}

function parseIntegerOption(value, name) {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) throw new Error(`${name} must be a non-negative integer`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`${name} is too large`);
  return parsed;
}

function parseNumberOption(value, name) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`${name} must be a finite number`);
  return parsed;
}

function assertSafeSlot(value, label) {
  assertNonNegativeInteger(value, label);
}

function assertNonNegativeInteger(value, label) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} must be a non-negative safe integer`);
  }
}

function assertPositiveInteger(value, label) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive safe integer`);
  }
}

function isObject(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function minNullable(left, right) {
  if (left === null) return right;
  if (right === null) return left;
  return Math.min(left, right);
}

function maxNullable(left, right) {
  if (left === null) return right;
  if (right === null) return left;
  return Math.max(left, right);
}

function delay(milliseconds) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}

const entrypoint = process.argv[1] ? pathToFileURL(resolve(process.argv[1])).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    process.stderr.write(`epoch calendar generation failed: ${error.message}\n`);
    process.exitCode = 1;
  });
}
