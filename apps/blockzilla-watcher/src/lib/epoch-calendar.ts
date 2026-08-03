export type EpochCalendarEntry = {
  epoch: number;
  start_unix_secs: number;
  end_unix_secs: number | null;
  precision: 'observed' | 'estimated';
};

export type EpochCalendarEnvelope = {
  schema_version: 1;
  epoch_calendar: EpochCalendarEntry[];
  [field: string]: unknown;
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
      label: timing ? monthFormatter.format(toDate(timing.start_unix_secs)) : 'Undated',
      has_estimates: false,
      items: []
    };
    month.has_estimates ||= timing?.precision === 'estimated';
    month.items.push({ value, timing });
    months.set(key, month);
  }

  return [...months.values()]
    .sort((left, right) => {
      if (left.key === 'undated') return -1;
      if (right.key === 'undated') return 1;
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

export function parseEpochCalendarEnvelope(value: unknown): EpochCalendarEnvelope | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  const envelope = value as Record<string, unknown>;
  const calendar = envelope.epoch_calendar;
  if (
    envelope.schema_version !== 1 ||
    !Array.isArray(calendar) ||
    !calendar.every(isEpochCalendarEntry) ||
    !hasUniqueEpochCalendarEpochs(calendar)
  ) {
    return null;
  }

  return {
    ...envelope,
    schema_version: 1,
    epoch_calendar: calendar
  };
}

export function mergeEpochCalendars(
  reference: EpochCalendarEntry[],
  authoritative: EpochCalendarEntry[]
) {
  const merged = new Map<number, EpochCalendarEntry>();
  for (const timing of reference) {
    if (isEpochCalendarEntry(timing)) merged.set(timing.epoch, timing);
  }
  for (const timing of authoritative) {
    if (isEpochCalendarEntry(timing)) merged.set(timing.epoch, timing);
  }
  return [...merged.values()].sort((left, right) => left.epoch - right.epoch);
}

export function extendEpochCalendarTail(
  calendar: EpochCalendarEntry[],
  throughEpoch: number | null,
  slotsPerEpoch = 432_000,
  fallbackSlotMs = 400
) {
  const extended = mergeEpochCalendars([], calendar);
  const last = extended.at(-1);
  if (
    !last ||
    throughEpoch === null ||
    !Number.isSafeInteger(throughEpoch) ||
    throughEpoch <= last.epoch
  ) {
    return extended;
  }

  const recentDurations = extended
    .flatMap((entry) => entry.end_unix_secs === null
      ? []
      : [entry.end_unix_secs - entry.start_unix_secs])
    .filter((duration) => Number.isSafeInteger(duration) && duration > 0)
    .slice(-16)
    .sort((left, right) => left - right);
  const fallbackDuration = Math.round(slotsPerEpoch * fallbackSlotMs / 1_000);
  const epochDuration = median(recentDurations) ?? fallbackDuration;
  if (!Number.isSafeInteger(epochDuration) || epochDuration <= 0) return extended;

  let start: number;
  if (last.end_unix_secs === null) {
    start = last.start_unix_secs + epochDuration;
    extended[extended.length - 1] = {
      ...last,
      end_unix_secs: start,
      precision: 'estimated'
    };
  } else {
    start = last.end_unix_secs;
  }
  for (let epoch = last.epoch + 1; epoch <= throughEpoch; epoch += 1) {
    const end = start + epochDuration;
    extended.push({
      epoch,
      start_unix_secs: start,
      end_unix_secs: epoch === throughEpoch ? null : end,
      precision: 'estimated'
    });
    start = end;
  }
  return extended;
}

export function epochCalendarStartDate(timing: EpochCalendarEntry) {
  return new Date(timing.start_unix_secs * 1000).toISOString().slice(0, 10);
}

function isEpochCalendarEntry(value: unknown): value is EpochCalendarEntry {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const timing = value as Record<string, unknown>;
  return Number.isSafeInteger(timing.epoch) &&
    Number(timing.epoch) >= 0 &&
    Number.isSafeInteger(timing.start_unix_secs) &&
    Number(timing.start_unix_secs) > 0 &&
    (timing.end_unix_secs === null || (
      Number.isSafeInteger(timing.end_unix_secs) &&
      Number(timing.end_unix_secs) >= Number(timing.start_unix_secs)
    )) &&
    (timing.precision === 'observed' || timing.precision === 'estimated');
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

function median(values: number[]) {
  if (values.length === 0) return null;
  const middle = Math.floor(values.length / 2);
  return values.length % 2 === 1
    ? values[middle]
    : Math.round((values[middle - 1] + values[middle]) / 2);
}

function toDate(unixSecs: number) {
  return new Date(unixSecs * 1000);
}
