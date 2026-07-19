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

const epochWithFileDate = (number, modified) => ({
  ...epoch(number),
  artifacts: [{
    kind: 'metadata',
    state: 'present',
    modified_unix_secs: modified
  }]
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
    {
      ...entry(11, 0, 0, 'legacy-epoch-11'),
      completed_unix_secs: null,
      duration_secs: null,
      timestamp_source: 'unknown'
    },
    {
      ...entry(10, 0, 0, 'legacy-epoch-10'),
      completed_unix_secs: null,
      duration_secs: null,
      timestamp_source: 'unknown'
    }
  ]);
});

test('archive metadata times fill history gaps without inventing a duration or duplicate run', () => {
  const result = selectCompactionHistory(
    [entry(10, 100, 30)],
    [epochWithFileDate(10, 90), epochWithFileDate(11, 120)]
  );

  assert.deepEqual(result, [
    {
      id: 'legacy-epoch-11',
      epoch: 11,
      workflow: 'historical',
      completed_unix_secs: 120,
      duration_secs: null,
      timestamp_source: 'archive_file'
    },
    { ...entry(10, 100, 30), timestamp_source: 'recorded' }
  ]);
});

test('archive metadata wins over later repair files and source CAR dates are not completion times', () => {
  const withMetadata = {
    ...epoch(12),
    artifacts: [
      { kind: 'metadata', state: 'present', modified_unix_secs: 100 },
      { kind: 'registry_index', state: 'present', modified_unix_secs: 300 },
      { kind: 'car', state: 'present', modified_unix_secs: 200 }
    ]
  };
  const withoutMetadata = {
    ...epoch(13),
    artifacts: [
      { kind: 'registry_index', state: 'present', modified_unix_secs: 400 },
      { kind: 'car', state: 'present', modified_unix_secs: 350 }
    ]
  };

  const byEpoch = new Map(selectCompactionHistory(undefined, [withMetadata, withoutMetadata])
    .map((item) => [item.epoch, item]));
  assert.equal(byEpoch.get(12)?.completed_unix_secs, 100);
  assert.equal(byEpoch.get(12)?.timestamp_source, 'archive_file');
  assert.equal(byEpoch.get(13)?.completed_unix_secs, null);
  assert.equal(byEpoch.get(13)?.timestamp_source, 'unknown');
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
