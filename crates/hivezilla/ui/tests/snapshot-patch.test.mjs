import assert from 'node:assert/strict';
import test from 'node:test';

import {
  applySnapshotPatch,
  snapshotPatchSequenceAction
} from '../src/lib/snapshot-patch.ts';

function snapshot() {
  return {
    schema_version: 3,
    sequence: 7,
    now_unix_secs: 100,
    current_epoch: 1000,
    observer_mode: false,
    capabilities: { control_enabled: true },
    scheduler: { paused: false },
    inventory: { complete: true },
    scan_sweep: { complete: false },
    summary: { queued: 3 },
    machine: { load_1m: 1 },
    epochs: [
      { epoch: 3, state: 'queued' },
      { epoch: 1, state: 'complete' },
      { epoch: 2, state: 'scanning' }
    ],
    lanes: [{ id: 'old-lane' }],
    live: [{ id: 'old-live' }],
    finalizer_queue: [],
    errors: []
  };
}

function patch(overrides = {}) {
  return {
    schema_version: 3,
    sequence: 8,
    now_unix_secs: 105,
    current_epoch: 1001,
    observer_mode: true,
    capabilities: { control_enabled: false },
    scheduler: { paused: true },
    inventory: { complete: true },
    scan_sweep: { complete: true },
    summary: { queued: 1 },
    machine: { load_1m: 2 },
    epochs_changed: [
      { epoch: 2, state: 'complete' },
      { epoch: 4, state: 'queued' }
    ],
    epochs_removed: [3],
    lanes: [{ id: 'new-lane' }],
    live: [{ id: 'new-live' }],
    finalizer_queue: [{ id: 'finalizer' }],
    errors: [{ message: 'new' }],
    ...overrides
  };
}

test('queued patches at or behind the initial snapshot are ignored without a resync', () => {
  assert.equal(snapshotPatchSequenceAction(10, 9), 'ignore');
  assert.equal(snapshotPatchSequenceAction(10, 10), 'ignore');
  assert.equal(snapshotPatchSequenceAction(10, 11), 'apply');
  assert.equal(snapshotPatchSequenceAction(10, 12), 'resync');
  assert.equal(snapshotPatchSequenceAction(-1, 0), 'resync');
});

test('snapshot patches upsert and remove epochs by key in sorted order', () => {
  const base = snapshot();
  const baseEpochs = base.epochs;
  const result = applySnapshotPatch(base, patch());

  assert.notEqual(result, base);
  assert.notEqual(result.epochs, baseEpochs);
  assert.deepEqual(result.epochs, [
    { epoch: 1, state: 'complete' },
    { epoch: 2, state: 'complete' },
    { epoch: 4, state: 'queued' }
  ]);
  assert.deepEqual(base.epochs, [
    { epoch: 3, state: 'queued' },
    { epoch: 1, state: 'complete' },
    { epoch: 2, state: 'scanning' }
  ]);
});

test('snapshot patches replace every non-epoch top-level field', () => {
  const result = applySnapshotPatch(snapshot(), patch());

  assert.equal(result.sequence, 8);
  assert.equal(result.now_unix_secs, 105);
  assert.equal(result.current_epoch, 1001);
  assert.equal(result.observer_mode, true);
  assert.deepEqual(result.capabilities, { control_enabled: false });
  assert.deepEqual(result.scheduler, { paused: true });
  assert.deepEqual(result.inventory, { complete: true });
  assert.deepEqual(result.scan_sweep, { complete: true });
  assert.deepEqual(result.summary, { queued: 1 });
  assert.deepEqual(result.machine, { load_1m: 2 });
  assert.deepEqual(result.lanes, [{ id: 'new-lane' }]);
  assert.deepEqual(result.live, [{ id: 'new-live' }]);
  assert.deepEqual(result.finalizer_queue, [{ id: 'finalizer' }]);
  assert.deepEqual(result.errors, [{ message: 'new' }]);
});
