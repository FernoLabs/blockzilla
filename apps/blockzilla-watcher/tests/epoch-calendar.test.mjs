import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
  buildEpochCalendarMonths,
  epochCalendarStartDate,
  extendEpochCalendarTail,
  formatEpochCalendarDay,
  formatEpochCalendarRange,
  hasUniqueEpochCalendarEpochs,
  mergeEpochCalendars,
  parseEpochCalendarEnvelope
} from '../src/lib/epoch-calendar.ts';

const utc = (year, month, day) => Date.UTC(year, month - 1, day) / 1000;

const bundledMainnetCalendar = JSON.parse(readFileSync(
  new URL('../src/lib/data/mainnet-epoch-calendar.json', import.meta.url),
  'utf8'
));

test('calendar keeps newest groups first and orders epochs oldest-first within each group', () => {
  const months = buildEpochCalendarMonths(
    [{ epoch: 10 }, { epoch: 11 }, { epoch: 12 }, { epoch: 13 }, { epoch: 14 }],
    [
      { epoch: 10, start_unix_secs: utc(2026, 6, 29), end_unix_secs: utc(2026, 7, 1), precision: 'observed' },
      { epoch: 11, start_unix_secs: utc(2026, 7, 1), end_unix_secs: utc(2026, 7, 3), precision: 'observed' },
      { epoch: 12, start_unix_secs: utc(2026, 7, 3), end_unix_secs: utc(2026, 7, 5), precision: 'estimated' }
    ]
  );

  assert.deepEqual(months.map((month) => month.label), ['Undated', 'Jul 2026', 'Jun 2026']);
  assert.deepEqual(months[0].items.map((item) => item.value.epoch), [13, 14]);
  assert.deepEqual(months[1].items.map((item) => item.value.epoch), [11, 12]);
  assert.equal(months[1].has_estimates, true);
  assert.equal(months[2].has_estimates, false);
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

test('reference envelope accepts valid entries and rejects duplicate epochs', () => {
  const entry = {
    epoch: 840,
    start_unix_secs: utc(2026, 7, 16),
    end_unix_secs: utc(2026, 7, 18),
    precision: 'observed'
  };
  const envelope = parseEpochCalendarEnvelope({
    schema_version: 1,
    cluster: 'mainnet-beta',
    epoch_calendar: [entry]
  });

  assert.deepEqual(envelope?.epoch_calendar, [entry]);
  assert.equal(parseEpochCalendarEnvelope({
    schema_version: 1,
    epoch_calendar: [entry, { ...entry }]
  }), null);
  assert.equal(parseEpochCalendarEnvelope({
    schema_version: 2,
    epoch_calendar: [entry]
  }), null);
});

test('API calendar entries override matching references and retain reference gaps', () => {
  const reference = [
    {
      epoch: 839,
      start_unix_secs: utc(2026, 7, 14),
      end_unix_secs: utc(2026, 7, 16),
      precision: 'estimated'
    },
    {
      epoch: 840,
      start_unix_secs: utc(2026, 7, 16),
      end_unix_secs: utc(2026, 7, 18),
      precision: 'estimated'
    }
  ];
  const api = [{
    epoch: 840,
    start_unix_secs: utc(2026, 7, 17),
    end_unix_secs: utc(2026, 7, 19),
    precision: 'observed'
  }];

  assert.deepEqual(mergeEpochCalendars(reference, api), [reference[0], api[0]]);
  assert.deepEqual(mergeEpochCalendars(reference, []), reference);
});

test('missing tail epochs use recent duration estimates without changing source entries', () => {
  const source = [
    { epoch: 7, start_unix_secs: 1_000, end_unix_secs: 1_170, precision: 'observed' },
    { epoch: 8, start_unix_secs: 1_170, end_unix_secs: 1_350, precision: 'observed' },
    { epoch: 9, start_unix_secs: 1_350, end_unix_secs: 1_540, precision: 'observed' }
  ];

  const extended = extendEpochCalendarTail(source, 11);

  assert.deepEqual(source.map((entry) => entry.epoch), [7, 8, 9]);
  assert.deepEqual(extended.slice(-2), [
    { epoch: 10, start_unix_secs: 1_540, end_unix_secs: 1_720, precision: 'estimated' },
    { epoch: 11, start_unix_secs: 1_720, end_unix_secs: null, precision: 'estimated' }
  ]);
});

test('tail estimation falls back to the configured slot schedule', () => {
  const extended = extendEpochCalendarTail([
    { epoch: 4, start_unix_secs: 1_000, end_unix_secs: null, precision: 'observed' }
  ], 5, 10, 500);

  assert.deepEqual(extended, [
    { epoch: 4, start_unix_secs: 1_000, end_unix_secs: 1_005, precision: 'estimated' },
    { epoch: 5, start_unix_secs: 1_005, end_unix_secs: null, precision: 'estimated' }
  ]);
});

test('bundled mainnet reference has canonical provenance and current coverage', () => {
  const envelope = parseEpochCalendarEnvelope(bundledMainnetCalendar);
  assert.ok(envelope);
  assert.equal(bundledMainnetCalendar.cluster, 'mainnet-beta');
  assert.equal(
    bundledMainnetCalendar.genesis_hash,
    '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d'
  );
  assert.ok(bundledMainnetCalendar.cutoff_slot >= 432_431_999);
  assert.ok(
    bundledMainnetCalendar.generation_mode === 'dates-only' ||
    bundledMainnetCalendar.generation_mode === 'full-scan'
  );

  const byEpoch = new Map(envelope.epoch_calendar.map((entry) => [entry.epoch, entry]));
  for (let epoch = 0; epoch <= 1003; epoch += 1) {
    assert.ok(byEpoch.has(epoch), `missing bundled date for epoch ${epoch}`);
  }
  assert.deepEqual(byEpoch.get(1003), {
    epoch: 1003,
    start_unix_secs: 1784214528,
    end_unix_secs: null,
    precision: 'observed'
  });
});
