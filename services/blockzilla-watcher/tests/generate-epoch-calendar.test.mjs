import assert from 'node:assert/strict';
import test from 'node:test';

import {
  assertCalendarCoverage,
  assertClusterGenesis,
  assertHistoryCoverage,
  buildEpochCalendar,
  createEnvelope,
  datesOnlyEpochRange,
  epochBounds,
  epochForSlot,
  mergeEpochSummaries,
  normalizeEpochSchedule,
  scanWindows,
  stableJson,
  summarizeWindow,
  validateBlockSlots
} from '../scripts/generate-epoch-calendar.mjs';

const schedule = normalizeEpochSchedule({
  firstNormalEpoch: 0,
  firstNormalSlot: 0,
  leaderScheduleSlotOffset: 10,
  slotsPerEpoch: 10,
  warmup: false
});

test('mainnet output is pinned to the canonical mainnet genesis', () => {
  assert.doesNotThrow(() => assertClusterGenesis(
    'mainnet-beta',
    '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d'
  ));
  assert.throws(() => assertClusterGenesis(
    'mainnet-beta',
    'EtWTRABZaYq6iMfeYKouRu166VU2xqa1'
  ), /requires genesis hash/);
  assert.doesNotThrow(() => assertClusterGenesis(
    'devnet',
    'EtWTRABZaYq6iMfeYKouRu166VU2xqa1'
  ));
});

test('history coverage and calendar completeness fail closed', () => {
  assert.doesNotThrow(() => assertHistoryCoverage(0, 0));
  assert.throws(() => assertHistoryCoverage(1, 0, 'after scan'), /after scan/);

  const stats = [
    { epoch: 0, first_produced_slot: 0, last_produced_slot: 9 },
    { epoch: 1, first_produced_slot: 10, last_produced_slot: 19 }
  ];
  assert.doesNotThrow(() => assertCalendarCoverage([
    { epoch: 0 },
    { epoch: 1 }
  ], stats));
  assert.throws(() => assertCalendarCoverage([{ epoch: 0 }], stats), /2 epochs/);
  assert.throws(() => assertCalendarCoverage([], []), /no epochs/);
});

test('warmup epoch boundaries follow the RPC epoch schedule', () => {
  const warmupSchedule = normalizeEpochSchedule({
    firstNormalEpoch: 14,
    firstNormalSlot: 524_256,
    leaderScheduleSlotOffset: 432_000,
    slotsPerEpoch: 432_000,
    warmup: true
  });

  assert.deepEqual(epochBounds(0, warmupSchedule), {
    epoch: 0,
    first_slot: 0,
    last_slot: 31,
    slot_count: 32
  });
  assert.equal(epochForSlot(32, warmupSchedule), 1);
  assert.equal(epochBounds(13, warmupSchedule).last_slot, 524_255);
  assert.equal(epochBounds(14, warmupSchedule).first_slot, 524_256);
});

test('scan windows are contiguous, non-overlapping, and never exceed 500,000 slots', () => {
  assert.deepEqual(scanWindows(0, 1_000_005), [
    { start_slot: 0, end_slot: 499_999 },
    { start_slot: 500_000, end_slot: 999_999 },
    { start_slot: 1_000_000, end_slot: 1_000_005 }
  ]);
  assert.deepEqual(scanWindows(42, 42), [{ start_slot: 42, end_slot: 42 }]);
  assert.throws(() => scanWindows(0, 10, 500_001), /at most 500000/);
});

test('dates-only mode emits complete epochs and fetches one following boundary', () => {
  assert.deepEqual(datesOnlyEpochRange(0, 12, 25, schedule), {
    first_epoch: 0,
    last_complete_epoch: 0,
    boundary_epochs: [0, 1]
  });
  assert.deepEqual(datesOnlyEpochRange(0, 19, 25, schedule), {
    first_epoch: 0,
    last_complete_epoch: 1,
    boundary_epochs: [0, 1, 2]
  });
  assert.throws(() => datesOnlyEpochRange(0, 5, 25, schedule), /at least one completed epoch/);
});

test('window aggregation counts produced and skipped slots across epoch boundaries', () => {
  const summary = summarizeWindow(
    { start_slot: 8, end_slot: 16 },
    [8, 9, 11, 14, 16],
    schedule
  );
  assert.deepEqual(summary, [
    {
      epoch: 0,
      first_produced_slot: 8,
      last_produced_slot: 9,
      produced_block_count: 2,
      scanned_slot_count: 2,
      skipped_slot_count: 0
    },
    {
      epoch: 1,
      first_produced_slot: 11,
      last_produced_slot: 16,
      produced_block_count: 3,
      scanned_slot_count: 7,
      skipped_slot_count: 4
    }
  ]);

  assert.throws(() => validateBlockSlots([8, 8], 8, 16), /strictly sorted and unique/);
  assert.throws(() => validateBlockSlots([7], 8, 16), /outside/);
});

test('epoch aggregation merges window fragments and marks only full epochs complete', () => {
  const merged = mergeEpochSummaries([
    summarizeWindow({ start_slot: 0, end_slot: 5 }, [0, 2, 5], schedule),
    summarizeWindow({ start_slot: 6, end_slot: 12 }, [6, 9, 10, 12], schedule)
  ], schedule);

  assert.deepEqual(merged, [
    {
      epoch: 0,
      complete: true,
      first_produced_slot: 0,
      last_produced_slot: 9,
      produced_block_count: 5,
      scanned_slot_count: 10,
      skipped_slot_count: 5
    },
    {
      epoch: 1,
      complete: false,
      first_produced_slot: 10,
      last_produced_slot: 12,
      produced_block_count: 2,
      scanned_slot_count: 3,
      skipped_slot_count: 1
    }
  ]);

  assert.throws(() => mergeEpochSummaries([[
    {
      epoch: 0,
      first_produced_slot: 1,
      last_produced_slot: 1,
      produced_block_count: 2,
      scanned_slot_count: 10,
      skipped_slot_count: 8
    }
  ]], schedule), /does not fit its bounds/);
});

test('nullable boundary times are estimated from observed anchors', () => {
  const stats = [
    { epoch: 0, first_produced_slot: 0, last_produced_slot: 9 },
    { epoch: 1, first_produced_slot: 10, last_produced_slot: 19 },
    { epoch: 2, first_produced_slot: 20, last_produced_slot: 29 }
  ];
  const times = new Map([
    [0, 1_000],
    [9, null],
    [10, null],
    [19, 1_019],
    [20, null],
    [29, 1_029]
  ]);

  assert.deepEqual(buildEpochCalendar(stats, times), [
    { epoch: 0, start_unix_secs: 1_000, end_unix_secs: 1_009, precision: 'estimated' },
    { epoch: 1, start_unix_secs: 1_010, end_unix_secs: 1_019, precision: 'estimated' },
    { epoch: 2, start_unix_secs: 1_020, end_unix_secs: 1_029, precision: 'estimated' }
  ]);
  assert.deepEqual(buildEpochCalendar(stats, new Map([[0, null], [29, null]])), []);
});

test('an incomplete epoch never presents its cutoff as an observed completion', () => {
  const calendar = buildEpochCalendar([
    { epoch: 3, complete: false, first_produced_slot: 30, last_produced_slot: 35 }
  ], new Map([[30, 1_030], [35, 1_035]]));

  assert.deepEqual(calendar, [{
    epoch: 3,
    start_unix_secs: 1_030,
    end_unix_secs: null,
    precision: 'estimated'
  }]);
});

test('envelope serialization is sorted and deterministic without runtime metadata', () => {
  const stats = [
    {
      epoch: 1,
      complete: false,
      first_produced_slot: 10,
      last_produced_slot: 12,
      produced_block_count: 2,
      scanned_slot_count: 3,
      skipped_slot_count: 1
    },
    {
      epoch: 0,
      complete: true,
      first_produced_slot: 0,
      last_produced_slot: 9,
      produced_block_count: 5,
      scanned_slot_count: 10,
      skipped_slot_count: 5
    }
  ];
  const calendar = [
    { epoch: 1, start_unix_secs: 1_010, end_unix_secs: 1_012, precision: 'observed' },
    { epoch: 0, start_unix_secs: 1_000, end_unix_secs: 1_009, precision: 'observed' }
  ];
  const args = {
    cluster: 'testnet',
    cutoffSlot: 12,
    epochCalendar: calendar,
    epochSchedule: schedule,
    epochStats: stats,
    firstAvailableBlock: 0,
    generationMode: 'full-scan',
    genesisHash: 'EtWTRABZaYq6iMfeYKouRu166VU2xqa1',
    startSlot: 0,
    windowSlots: 500_000
  };
  const first = stableJson(createEnvelope(args));
  const second = stableJson(createEnvelope({
    windowSlots: 500_000,
    startSlot: 0,
    genesisHash: args.genesisHash,
    firstAvailableBlock: 0,
    generationMode: 'full-scan',
    epochStats: [...stats].reverse(),
    epochSchedule: { ...schedule },
    epochCalendar: [...calendar].reverse(),
    cutoffSlot: 12,
    cluster: 'testnet'
  }));

  assert.equal(first, second);
  assert.doesNotMatch(first, /generated_at|endpoint|https?:/);
  const parsed = JSON.parse(first);
  assert.deepEqual(parsed.epoch_calendar.map((entry) => entry.epoch), [0, 1]);
  assert.deepEqual(parsed.epoch_stats.map((entry) => entry.epoch), [0, 1]);

  const datesOnly = createEnvelope({
    ...args,
    epochStats: null,
    generationMode: 'dates-only'
  });
  assert.equal(datesOnly.generation_mode, 'dates-only');
  assert.equal(Object.hasOwn(datesOnly, 'epoch_stats'), false);
});
