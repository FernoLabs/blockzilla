import assert from 'node:assert/strict';
import test from 'node:test';

import {
  hasProcessResourceMetrics,
  processDiskIoRate,
  processMetric,
  rankProcessIo
} from '../src/lib/process-telemetry.ts';

const process = (overrides = {}) => ({
  id: '100:1',
  pid: 100,
  name: 'worker',
  read_mib_per_sec: 0,
  write_mib_per_sec: 0,
  ...overrides
});

test('disk I/O totals preserve zero and use a single available direction', () => {
  assert.equal(processDiskIoRate(process()), 0);
  assert.equal(processDiskIoRate(process({ read_mib_per_sec: 4, write_mib_per_sec: null })), 4);
  assert.equal(processDiskIoRate(process({ read_mib_per_sec: null, write_mib_per_sec: 3 })), 3);
  assert.equal(processDiskIoRate(process({ read_mib_per_sec: null, write_mib_per_sec: null })), null);
});

test('processes rank by combined I/O without mutating the API snapshot', () => {
  const rows = [
    process({ id: '1:1', pid: 1, name: 'slow', read_mib_per_sec: 1 }),
    process({ id: '2:1', pid: 2, name: 'fast', read_mib_per_sec: 5, write_mib_per_sec: 4 }),
    process({ id: '3:1', pid: 3, name: 'write-heavy', write_mib_per_sec: 7 })
  ];
  const original = [...rows];

  assert.deepEqual(rankProcessIo(rows).map((row) => row.name), ['fast', 'write-heavy', 'slow']);
  assert.deepEqual(rows, original);
});

test('only explicit Blockzilla ownership is excluded and limiting happens afterward', () => {
  const rows = [
    process({ id: '1:1', pid: 1, name: 'blockzilla', read_mib_per_sec: 20, blockzilla_owned: true }),
    process({ id: '2:1', pid: 2, name: 'blockzilla', read_mib_per_sec: 10 }),
    process({ id: '3:1', pid: 3, name: 'unknown-owner', read_mib_per_sec: 9 }),
    process({ id: '4:1', pid: 4, name: 'external', read_mib_per_sec: 8, blockzilla_owned: false })
  ];

  assert.deepEqual(rankProcessIo(rows, 1).map((row) => row.id), ['2:1']);
});

test('invalid metrics are unavailable and sort after known zero', () => {
  const rows = [
    process({ id: '1:1', pid: 1, name: 'negative', read_mib_per_sec: -1, write_mib_per_sec: null }),
    process({ id: '2:1', pid: 2, name: 'nan', read_mib_per_sec: Number.NaN, write_mib_per_sec: null }),
    process({ id: '3:1', pid: 3, name: 'zero' })
  ];

  assert.equal(processMetric(Number.POSITIVE_INFINITY), null);
  assert.deepEqual(rankProcessIo(rows).map((row) => row.name), ['zero', 'nan', 'negative']);
});

test('CPU and RSS availability stays optional', () => {
  assert.equal(hasProcessResourceMetrics([process()]), false);
  assert.equal(hasProcessResourceMetrics([process({ cpu_percent: 0 })]), true);
  assert.equal(hasProcessResourceMetrics([process({ rss_bytes: 1024 })]), true);
});
