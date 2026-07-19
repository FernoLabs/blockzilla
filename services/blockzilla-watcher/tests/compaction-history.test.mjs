import assert from 'node:assert/strict';
import test from 'node:test';

import {
  selectCompactionHistory,
  selectRecentCompactions
} from '../src/lib/compaction-history.ts';

const entry = (epoch, completed, duration = 30, id = `run-${epoch}`) => ({
  id,
  epoch,
  workflow: 'historical',
  completed_unix_secs: completed,
  duration_secs: duration
});

const epoch = (number, state = 'complete') => ({
  epoch: number,
  state
});

test('reported compactions are ordered by completion time without mutating the snapshot', () => {
  const reported = [entry(800, 100, 30), entry(801, 120, 40), entry(802, 120, 50)];
  const original = [...reported];

  assert.deepEqual(selectRecentCompactions(reported, [], 2).map((item) => item.epoch), [802, 801]);
  assert.deepEqual(reported, original);
});

test('older snapshots fall back to the highest complete epochs without inventing timings', () => {
  const result = selectRecentCompactions(undefined, [
    epoch(12, 'queued'),
    epoch(10),
    epoch(11)
  ]);

  assert.deepEqual(result, [
    { ...entry(11, 0, 0, 'legacy-epoch-11'), completed_unix_secs: null, duration_secs: null },
    { ...entry(10, 0, 0, 'legacy-epoch-10'), completed_unix_secs: null, duration_secs: null }
  ]);
});

test('an empty persisted history still exposes the highest known completed epoch', () => {
  assert.equal(selectRecentCompactions([], [epoch(10)])[0]?.epoch, 10);
});

test('the history selector returns every recorded completion newest first', () => {
  const reported = Array.from({ length: 12 }, (_, index) => entry(800 + index, 100 + index));

  assert.equal(selectCompactionHistory(reported, []).length, 12);
  assert.equal(selectCompactionHistory(reported, [])[0]?.epoch, 811);
  assert.equal(selectRecentCompactions(reported, [], 5).length, 5);
});
