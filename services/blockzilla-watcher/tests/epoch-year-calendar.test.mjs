import assert from 'node:assert/strict';
import test from 'node:test';

import { buildEpochYearCalendars } from '../src/lib/epoch-year-calendar.ts';

const utc = (year, month, day, hour = 0) => Date.UTC(year, month - 1, day, hour) / 1_000;
const status = (epoch, tone, label, priority, archived = tone === 'complete') => ({
  epoch,
  tone,
  label,
  priority,
  archived,
  tracked: true
});

test('year heatmap contains every UTC day and places them by week and weekday', () => {
  const years = buildEpochYearCalendars([
    status(1, 'complete', 'complete', 1)
  ], [{
    epoch: 1,
    start_unix_secs: utc(2024, 1, 1),
    end_unix_secs: utc(2024, 12, 31, 23),
    precision: 'observed'
  }], utc(2024, 6, 1));

  assert.equal(years.length, 1);
  assert.equal(years[0].year, 2024);
  assert.equal(years[0].days.length, 366);
  assert.equal(years[0].months.length, 12);
  assert.deepEqual(
    years[0].days.slice(0, 2).map((day) => [day.date, day.week, day.weekday]),
    [['2024-01-01', 0, 1], ['2024-01-02', 0, 2]]
  );
});

test('boundary days retain both epochs and use the highest-priority state', () => {
  const years = buildEpochYearCalendars([
    status(10, 'complete', 'complete', 1),
    status(11, 'active', 'live indexing', 9, false)
  ], [
    {
      epoch: 10,
      start_unix_secs: utc(2026, 7, 14, 12),
      end_unix_secs: utc(2026, 7, 16, 15),
      precision: 'observed'
    },
    {
      epoch: 11,
      start_unix_secs: utc(2026, 7, 16, 15),
      end_unix_secs: null,
      precision: 'estimated'
    }
  ], utc(2026, 7, 17, 12));

  const boundary = years[0].days.find((day) => day.date === '2026-07-16');
  assert.deepEqual(boundary?.epochs, [10, 11]);
  assert.equal(boundary?.primary_epoch, 11);
  assert.equal(boundary?.tone, 'active');
  assert.equal(boundary?.estimated, true);
  assert.match(boundary?.label ?? '', /Epoch 10: complete · Epoch 11: live indexing/);
});

test('current open epoch stops at today and future days remain empty', () => {
  const years = buildEpochYearCalendars([
    status(20, 'active', 'live indexing', 9, false)
  ], [{
    epoch: 20,
    start_unix_secs: utc(2026, 7, 16, 15),
    end_unix_secs: null,
    precision: 'observed'
  }], utc(2026, 7, 17, 12));

  const current = years[0].days.find((day) => day.date === '2026-07-17');
  const future = years[0].days.find((day) => day.date === '2026-07-18');
  assert.equal(current?.primary_epoch, 20);
  assert.equal(current?.today, true);
  assert.equal(future?.primary_epoch, null);
  assert.equal(future?.future, true);
});

test('years follow chronological reading order and source arrays are not mutated', () => {
  const statuses = [
    status(1, 'complete', 'complete', 1),
    status(2, 'complete', 'complete', 1)
  ];
  const calendar = [
    { epoch: 1, start_unix_secs: utc(2025, 12, 30), end_unix_secs: utc(2026, 1, 1), precision: 'observed' },
    { epoch: 2, start_unix_secs: utc(2026, 1, 1), end_unix_secs: utc(2026, 1, 3), precision: 'observed' }
  ];

  const years = buildEpochYearCalendars(statuses, calendar, utc(2026, 1, 3));

  assert.deepEqual(years.map((year) => year.year), [2025, 2026]);
  assert.deepEqual(years.map((year) => year.epoch_count), [1, 1]);
  assert.deepEqual(statuses.map((status) => status.epoch), [1, 2]);
  assert.deepEqual(calendar.map((timing) => timing.epoch), [1, 2]);
});

test('calendar timing without watcher status remains visible as untracked', () => {
  const years = buildEpochYearCalendars([], [{
    epoch: 1001,
    start_unix_secs: utc(2026, 7, 14),
    end_unix_secs: utc(2026, 7, 16),
    precision: 'observed'
  }], utc(2026, 7, 17));

  const day = years[0].days.find((entry) => entry.date === '2026-07-15');
  assert.equal(day?.primary_epoch, 1001);
  assert.equal(day?.tone, 'untracked');
  assert.equal(day?.items[0].tracked, false);
  assert.equal(years[0].epoch_count, 1);
  assert.equal(years[0].tracked_epoch_count, 0);
  assert.equal(years[0].archived_epoch_count, 0);
});

test('an inclusive midnight end touches that UTC day without spilling farther', () => {
  const years = buildEpochYearCalendars([
    status(7, 'complete', 'complete', 1)
  ], [{
    epoch: 7,
    start_unix_secs: utc(2026, 7, 14, 12),
    end_unix_secs: utc(2026, 7, 16),
    precision: 'observed'
  }], utc(2026, 7, 17));

  assert.equal(years[0].days.find((day) => day.date === '2026-07-16')?.primary_epoch, 7);
  assert.equal(years[0].days.find((day) => day.date === '2026-07-17')?.primary_epoch, null);
});

test('invalid snapshot time does not fall back to wall-clock time', () => {
  assert.deepEqual(buildEpochYearCalendars([], [], 0), []);
});
