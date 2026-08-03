import assert from 'node:assert/strict';
import test from 'node:test';

import { parseBlockTimeGapIndex } from '../src/lib/block-time-gap-index.ts';

const validIndex = () => ({
  schema_version: 1,
  generated_unix_secs: 1_700_000_000,
  minimum_interruption_secs: 300,
  source_sha256: 'ab'.repeat(32),
  coverage: {
    start_epoch: 0,
    end_epoch: 2,
    expected_epoch_count: 3,
    indexed_epoch_count: 2,
    missing_epochs: [1],
    indexed_boundary_count: 0,
    source_sidecar_bytes: 1_000,
    source_gap_rows: 20
  },
  interruptions: [{ id: 0 }],
  days: [{
    day_start_unix_secs: 1_699_920_000,
    interruption_count: 1,
    boundary_interruption_count: 0,
    interruption_seconds: 900,
    longest_interruption_secs: 900,
    largest_missing_slots: 4,
    longest_interruption: {
      id: 0,
      kind: 'intra_epoch',
      previous_slot: 100,
      next_slot: 105,
      previous_block_time: 1_699_930_000,
      next_block_time: 1_699_930_900,
      elapsed_secs: 900,
      missing_slots: 4
    }
  }]
});

test('parses a validated aggregate index', () => {
  const value = validIndex();
  const { interruptions: _, ...expected } = value;
  assert.deepEqual(parseBlockTimeGapIndex(value), expected);
});

test('rejects incomplete coverage accounting', () => {
  const value = validIndex();
  value.coverage.indexed_epoch_count = 3;
  assert.equal(parseBlockTimeGapIndex(value), null);
});

test('rejects unsorted or malformed day summaries', () => {
  const duplicate = validIndex();
  duplicate.days.push(structuredClone(duplicate.days[0]));
  assert.equal(parseBlockTimeGapIndex(duplicate), null);

  const malformed = validIndex();
  malformed.days[0].longest_interruption.next_slot = 104;
  assert.equal(parseBlockTimeGapIndex(malformed), null);
});
