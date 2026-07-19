import assert from 'node:assert/strict';
import test from 'node:test';

import {
  blockTimeGapBackfillIsFresh,
  blockTimeGapBackfillPercent,
  parseBlockTimeGapBackfill
} from '../src/lib/block-time-gap-backfill.ts';

function status(overrides = {}) {
  return {
    schema_version: 1,
    state: 'running',
    started_unix_secs: 1_000,
    updated_unix_secs: 1_100,
    backfill: {
      epochs_done: 265,
      epochs_total: 526,
      workers_configured: 2,
      active_workers: 2,
      source_bytes_done: 2_500,
      source_bytes_total: 10_000,
      throughput_bytes_per_sec: 120,
      eta_secs: 63,
      eta_reliable: true
    },
    current: { epoch: 289, progress_state: 'running' },
    paused_secs: 182,
    last_error: null,
    ...overrides
  };
}

test('parses the allowlisted public backfill status', () => {
  assert.deepEqual(parseBlockTimeGapBackfill(status()), status());
});

test('accepts an intentionally stopped backfill', () => {
  assert.deepEqual(parseBlockTimeGapBackfill(status({ state: 'stopped' })), status({ state: 'stopped' }));
});

test('rejects invalid state and inconsistent counters', () => {
  assert.equal(parseBlockTimeGapBackfill(status({ state: 'mystery' })), null);
  assert.equal(parseBlockTimeGapBackfill(status({
    backfill: { ...status().backfill, epochs_done: 527 }
  })), null);
  assert.equal(parseBlockTimeGapBackfill(status({
    backfill: { ...status().backfill, source_bytes_done: 10_001 }
  })), null);
  assert.equal(parseBlockTimeGapBackfill(status({
    backfill: { ...status().backfill, active_workers: 3 }
  })), null);
});

test('computes byte-weighted progress instead of treating every epoch as equal', () => {
  const parsed = parseBlockTimeGapBackfill(status());
  assert.ok(parsed);
  assert.equal(blockTimeGapBackfillPercent(parsed), 25);
});

test('freshness has an inclusive one-minute boundary', () => {
  const parsed = parseBlockTimeGapBackfill(status());
  assert.ok(parsed);
  assert.equal(blockTimeGapBackfillIsFresh(parsed, 1_160), true);
  assert.equal(blockTimeGapBackfillIsFresh(parsed, 1_161), false);
});
