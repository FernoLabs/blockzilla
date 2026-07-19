import assert from 'node:assert/strict';
import test from 'node:test';

import { formatBytes } from '../src/lib/format.ts';
import {
  liveEtaSecs,
  liveEtaStatus,
  livePeakRssBytes,
  liveRate,
  liveRssBytes
} from '../src/lib/live-metrics.ts';

function capture(overrides = {}) {
  return {
    state: 'capturing',
    progress: {
      eta_secs: null,
      blocks_per_sec: null,
      rss_bytes: null,
      peak_rss_bytes: null
    },
    ...overrides
  };
}

test('capturing uses the explicit slot rate and never the nested block rate', () => {
  const value = capture({
    eta_secs: 45,
    slots_per_sec: 18.5,
    rss_bytes: 200,
    peak_rss_bytes: 350,
    progress: {
      eta_secs: 90,
      blocks_per_sec: 4,
      rss_bytes: 100,
      peak_rss_bytes: 150
    }
  });

  assert.equal(liveEtaSecs(value), 45);
  assert.equal(liveRate(value), 18.5);
  assert.equal(liveRssBytes(value), 200);
  assert.equal(livePeakRssBytes(value), 350);
});

test('older capture snapshots retain ETA and memory fallbacks without inventing a slot rate', () => {
  const value = capture({
    progress: {
      eta_secs: 90,
      blocks_per_sec: 4,
      rss_bytes: 100,
      peak_rss_bytes: 150
    }
  });

  assert.equal(liveEtaSecs(value), 90);
  assert.equal(liveRate(value), null);
  assert.equal(liveRssBytes(value), 100);
  assert.equal(livePeakRssBytes(value), 150);
});

test('packaging uses block-processing rate and ignores slot rate', () => {
  const value = capture({
    state: 'packaging',
    slots_per_sec: 18.5,
    progress: {
      eta_secs: null,
      blocks_per_sec: 4,
      rss_bytes: null,
      peak_rss_bytes: null
    }
  });

  assert.equal(liveRate(value), 4);
  assert.equal(liveRate(capture({ state: 'packaging', slots_per_sec: 18.5 })), null);
});

test('ETA reports estimated, stalled, unknown, and complete states', () => {
  assert.equal(liveEtaStatus(capture({ eta_secs: 30 })), 'estimated');
  assert.equal(liveEtaStatus(capture({ slots_per_sec: 0 })), 'stalled');
  assert.equal(liveEtaStatus(capture({ progress: { blocks_per_sec: 0 } })), 'unknown');
  assert.equal(liveEtaStatus(capture({ state: 'packaging', progress: { blocks_per_sec: 0 } })), 'stalled');
  assert.equal(liveEtaStatus(capture()), 'unknown');
  assert.equal(liveEtaStatus(capture({ state: 'complete', eta_secs: 0 })), 'complete');
  assert.equal(liveEtaStatus(capture({ state: 'repair_gate', eta_secs: 30 })), 'unknown');
  assert.equal(liveEtaStatus(capture({ state: 'ready_to_package', eta_secs: 30 })), 'unknown');
  assert.equal(liveEtaStatus(capture({ state: 'packaged', eta_secs: 30 })), 'unknown');
});

test('non-finite metrics are treated as unavailable', () => {
  const value = capture({
    eta_secs: Number.NaN,
    rss_bytes: Number.POSITIVE_INFINITY,
    progress: {
      eta_secs: 15,
      blocks_per_sec: null,
      rss_bytes: 1024,
      peak_rss_bytes: null
    }
  });

  assert.equal(liveEtaSecs(value), 15);
  assert.equal(liveRssBytes(value), 1024);
  assert.equal(liveEtaStatus(value), 'estimated');
});

test('memory byte values use compact binary units and preserve unknown values', () => {
  assert.equal(formatBytes(null), '—');
  assert.equal(formatBytes(-1), '—');
  assert.equal(formatBytes(0), '0 B');
  assert.equal(formatBytes(512 * 1024 * 1024), '512 MiB');
  assert.equal(formatBytes(1.5 * 1024 * 1024 * 1024), '1.5 GiB');
});

test('negative live metrics are unavailable', () => {
  const value = capture({ eta_secs: -1, slots_per_sec: -2, rss_bytes: -3, peak_rss_bytes: -4 });

  assert.equal(liveEtaSecs(value), null);
  assert.equal(liveRate(value), null);
  assert.equal(liveRssBytes(value), null);
  assert.equal(livePeakRssBytes(value), null);
  assert.equal(liveEtaStatus(value), 'unknown');
});
