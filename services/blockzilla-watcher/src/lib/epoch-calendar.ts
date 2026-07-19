export type EpochCalendarEntry = {
  epoch: number;
  start_unix_secs: number;
  end_unix_secs: number | null;
  precision: 'observed' | 'estimated';
};

export type EpochCalendarItem<T extends { epoch: number }> = {
  value: T;
  timing: EpochCalendarEntry | null;
};

export type EpochCalendarMonth<T extends { epoch: number }> = {
  key: string;
  label: string;
  has_estimates: boolean;
  items: EpochCalendarItem<T>[];
};

const monthFormatter = new Intl.DateTimeFormat('en-US', {
  month: 'short',
  timeZone: 'UTC',
  year: 'numeric'
});

const dayFormatter = new Intl.DateTimeFormat('en-US', {
  day: 'numeric',
  timeZone: 'UTC'
});

const fullDateFormatter = new Intl.DateTimeFormat('en-US', {
  day: 'numeric',
  month: 'short',
  timeZone: 'UTC',
  year: 'numeric'
});

export function buildEpochCalendarMonths<T extends { epoch: number }>(
  values: T[],
  calendar: EpochCalendarEntry[]
): EpochCalendarMonth<T>[] {
  const timingByEpoch = new Map(
    calendar
      .filter(isUsableTiming)
      .map((timing) => [timing.epoch, timing] as const)
  );
  const months = new Map<string, EpochCalendarMonth<T>>();

  for (const value of values) {
    const timing = timingByEpoch.get(value.epoch) ?? null;
    const key = timing ? monthKey(timing.start_unix_secs) : 'undated';
    const month = months.get(key) ?? {
      key,
      label: timing ? monthFormatter.format(toDate(timing.start_unix_secs)) : 'Dates unavailable',
      has_estimates: false,
      items: []
    };
    month.has_estimates ||= timing?.precision === 'estimated';
    month.items.push({ value, timing });
    months.set(key, month);
  }

  return [...months.values()]
    .sort((left, right) => {
      if (left.key === 'undated') return 1;
      if (right.key === 'undated') return -1;
      return right.key.localeCompare(left.key);
    })
    .map((month) => ({
      ...month,
      items: [...month.items].sort((left, right) => left.value.epoch - right.value.epoch)
    }));
}

export function formatEpochCalendarDay(timing: EpochCalendarEntry) {
  return dayFormatter.format(toDate(timing.start_unix_secs));
}

export function formatEpochCalendarRange(timing: EpochCalendarEntry) {
  const start = fullDateFormatter.format(toDate(timing.start_unix_secs));
  if (timing.end_unix_secs === null) {
    return `${timing.precision === 'estimated' ? 'Estimated from' : 'From'} ${start} UTC`;
  }
  const end = fullDateFormatter.format(toDate(timing.end_unix_secs));
  const range = end !== start ? `${start} – ${end}` : start;
  return `${timing.precision === 'estimated' ? 'Estimated ' : ''}${range} UTC`;
}

export function hasUniqueEpochCalendarEpochs(calendar: EpochCalendarEntry[]) {
  const epochs = new Set<number>();
  for (const timing of calendar) {
    if (epochs.has(timing.epoch)) return false;
    epochs.add(timing.epoch);
  }
  return true;
}

export function epochCalendarStartDate(timing: EpochCalendarEntry) {
  return new Date(timing.start_unix_secs * 1000).toISOString().slice(0, 10);
}

function isUsableTiming(timing: EpochCalendarEntry) {
  return Number.isInteger(timing.epoch) &&
    Number.isFinite(timing.start_unix_secs) &&
    timing.start_unix_secs > 0 &&
    (timing.end_unix_secs === null || (
      Number.isFinite(timing.end_unix_secs) &&
      timing.end_unix_secs >= timing.start_unix_secs
    ));
}

function monthKey(unixSecs: number) {
  const date = toDate(unixSecs);
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}`;
}

function toDate(unixSecs: number) {
  return new Date(unixSecs * 1000);
}
