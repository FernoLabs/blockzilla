import assert from 'node:assert/strict';
import test from 'node:test';

import { archiveProgressState } from '../src/lib/archive-progress.ts';

const progress = (overrides = {}) => archiveProgressState({
  epochs: [
    { state: 'complete', car_bytes: 100, progress: { progress_pct: 100 } },
    { state: 'scanning', car_bytes: 300, progress: { progress_pct: 50 } }
  ],
  inventoryComplete: true,
  ...overrides
});

test('archive progress is weighted by source bytes and retains partial active work', () => {
  assert.deepEqual(progress(), {
    state: 'ready',
    percent: 62.5,
    bytesDone: 250,
    bytesTotal: 400
  });
});

test('archive progress is indeterminate while inventory changes the denominator', () => {
  assert.deepEqual(progress({ inventoryComplete: false }), { state: 'scanning', percent: null });
});

test('an empty inventory does not present a determinate zero percent', () => {
  assert.deepEqual(progress({ epochs: [] }), { state: 'empty', percent: null });
});

test('completed epochs count as fully processed regardless of stale per-epoch progress', () => {
  assert.deepEqual(progress({
    epochs: [{ state: 'complete', car_bytes: 200, progress: { progress_pct: null } }]
  }), {
    state: 'ready',
    percent: 100,
    bytesDone: 200,
    bytesTotal: 200
  });
});

test('partial epoch progress is clamped to its valid range', () => {
  assert.equal(progress({
    epochs: [{ state: 'scanning', car_bytes: 200, progress: { progress_pct: -4 } }]
  }).percent, 0);
  assert.equal(progress({
    epochs: [{ state: 'scanning', car_bytes: 200, progress: { progress_pct: 104 } }]
  }).percent, 100);
});

test('epoch count does not influence byte-weighted progress', () => {
  assert.equal(progress({
    epochs: [
      { state: 'complete', car_bytes: 10, progress: { progress_pct: 100 } },
      { state: 'complete', car_bytes: 10, progress: { progress_pct: 100 } },
      { state: 'queued', car_bytes: 980, progress: { progress_pct: 0 } }
    ]
  }).percent, 2);
});
