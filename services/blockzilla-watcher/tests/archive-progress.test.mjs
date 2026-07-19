import assert from 'node:assert/strict';
import test from 'node:test';

import { archiveProgressState } from '../src/lib/archive-progress.ts';

const progress = (overrides = {}) => archiveProgressState({
  reportedPercent: 62.85,
  completedEpochs: 626,
  totalEpochs: 1_001,
  inventoryComplete: true,
  ...overrides
});

test('reported archive progress retains partial active epoch work', () => {
  assert.deepEqual(progress(), { state: 'ready', percent: 62.85 });
});

test('archive progress is indeterminate while inventory changes the denominator', () => {
  assert.deepEqual(progress({ inventoryComplete: false }), { state: 'scanning', percent: null });
});

test('an empty inventory does not present a determinate zero percent', () => {
  assert.deepEqual(progress({ totalEpochs: 0 }), { state: 'empty', percent: null });
});

test('invalid reported progress falls back to completed epochs', () => {
  assert.deepEqual(progress({ reportedPercent: Number.NaN, completedEpochs: 25, totalEpochs: 50 }), {
    state: 'ready',
    percent: 50
  });
});

test('archive progress is clamped to its valid range', () => {
  assert.equal(progress({ reportedPercent: -4 }).percent, 0);
  assert.equal(progress({ reportedPercent: 104 }).percent, 100);
});
