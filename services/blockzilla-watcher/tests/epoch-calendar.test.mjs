import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildEpochCalendarMonths,
  epochCalendarStartDate,
  formatEpochCalendarDay,
  formatEpochCalendarRange,
  hasUniqueEpochCalendarEpochs
} from '../src/lib/epoch-calendar.ts';

const utc = (year, month, day) => Date.UTC(year, month - 1, day) / 1000;

test('calendar groups newest UTC months first and leaves undated epochs explicit', () => {
  const months = buildEpochCalendarMonths(
    [{ epoch: 10 }, { epoch: 11 }, { epoch: 12 }, { epoch: 13 }],
    [
      { epoch: 10, start_unix_secs: utc(2026, 6, 29), end_unix_secs: utc(2026, 7, 1), precision: 'observed' },
      { epoch: 11, start_unix_secs: utc(2026, 7, 1), end_unix_secs: utc(2026, 7, 3), precision: 'observed' },
      { epoch: 12, start_unix_secs: utc(2026, 7, 3), end_unix_secs: utc(2026, 7, 5), precision: 'estimated' }
    ]
  );

  assert.deepEqual(months.map((month) => month.label), ['Jul 2026', 'Jun 2026', 'Dates unavailable']);
  assert.deepEqual(months[0].items.map((item) => item.value.epoch), [11, 12]);
  assert.equal(months[0].has_estimates, true);
  assert.equal(months[1].has_estimates, false);
  assert.equal(months[2].items[0].value.epoch, 13);
});

test('calendar formatting stays in UTC and marks estimated ranges', () => {
  const timing = {
    epoch: 840,
    start_unix_secs: utc(2026, 7, 16),
    end_unix_secs: utc(2026, 7, 18),
    precision: 'estimated'
  };

  assert.equal(formatEpochCalendarDay(timing), '16');
  assert.equal(formatEpochCalendarRange(timing), 'Estimated Jul 16, 2026 – Jul 18, 2026 UTC');
  assert.equal(epochCalendarStartDate(timing), '2026-07-16');
});

test('invalid and reversed timings fall back to the undated band', () => {
  const months = buildEpochCalendarMonths(
    [{ epoch: 1 }, { epoch: 2 }],
    [
      { epoch: 1, start_unix_secs: 0, end_unix_secs: null, precision: 'observed' },
      { epoch: 2, start_unix_secs: utc(2026, 7, 3), end_unix_secs: utc(2026, 7, 1), precision: 'observed' }
    ]
  );

  assert.equal(months.length, 1);
  assert.equal(months[0].key, 'undated');
  assert.deepEqual(months[0].items.map((item) => item.value.epoch), [1, 2]);
});

test('open-ended ranges are explicit and duplicate epoch timings are rejected', () => {
  const openEnded = {
    epoch: 840,
    start_unix_secs: utc(2026, 7, 16),
    end_unix_secs: null,
    precision: 'observed'
  };

  assert.equal(formatEpochCalendarRange(openEnded), 'From Jul 16, 2026 UTC');
  assert.equal(
    formatEpochCalendarRange({ ...openEnded, precision: 'estimated' }),
    'Estimated from Jul 16, 2026 UTC'
  );
  assert.equal(hasUniqueEpochCalendarEpochs([openEnded]), true);
  assert.equal(hasUniqueEpochCalendarEpochs([openEnded, { ...openEnded }]), false);
});
