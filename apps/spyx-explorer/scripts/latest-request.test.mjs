import assert from 'node:assert/strict';
import test from 'node:test';
import { createLatestRequestObserver } from '../src/lib/latest-request.js';

test('a stale health success cannot replace a newer failed request', async () => {
  const observeLatest = createLatestRequestObserver();
  const older = deferred();
  const newer = deferred();
  let health = null;

  observeLatest(older.promise, (value) => (health = value), () => (health = null));
  observeLatest(newer.promise, (value) => (health = value), () => (health = null));

  newer.reject(new Error('newer health request failed'));
  await newer.settled;
  older.resolve('stale matching health');
  await older.settled;

  assert.equal(health, null);
});

test('a stale health failure cannot clear a newer successful request', async () => {
  const observeLatest = createLatestRequestObserver();
  const older = deferred();
  const newer = deferred();
  let health = null;

  observeLatest(older.promise, (value) => (health = value), () => (health = null));
  observeLatest(newer.promise, (value) => (health = value), () => (health = null));

  newer.resolve('new matching health');
  await newer.settled;
  older.reject(new Error('stale health request failed'));
  await older.settled;

  assert.equal(health, 'new matching health');
});

function deferred() {
  let resolvePromise;
  let rejectPromise;
  const promise = new Promise((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return {
    promise,
    settled: promise.then(
      () => undefined,
      () => undefined
    ),
    resolve: resolvePromise,
    reject: rejectPromise
  };
}
