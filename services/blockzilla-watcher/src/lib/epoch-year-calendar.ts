import type { EpochCalendarEntry } from './epoch-calendar';

export type EpochYearStatus = {
  epoch: number;
  tone: string;
  label: string;
  priority: number;
  archived: boolean;
  tracked: boolean;
};

export type EpochYearDay = {
  date: string;
  day: number;
  month: number;
  week: number;
  weekday: number;
  epochs: number[];
  items: EpochYearStatus[];
  primary_epoch: number | null;
  tone: string | null;
  label: string;
  estimated: boolean;
  future: boolean;
  today: boolean;
};

export type EpochYearCalendar = {
  year: number;
  week_count: number;
  epoch_count: number;
  tracked_epoch_count: number;
  archived_epoch_count: number;
  months: { month: number; label: string; week: number }[];
  days: EpochYearDay[];
};

const dayLabelFormatter = new Intl.DateTimeFormat('en-US', {
  day: 'numeric',
  month: 'short',
  timeZone: 'UTC',
  year: 'numeric'
});

const monthLabelFormatter = new Intl.DateTimeFormat('en-US', {
  month: 'short',
  timeZone: 'UTC'
});

export function buildEpochYearCalendars(
  statuses: EpochYearStatus[],
  calendar: EpochCalendarEntry[],
  nowUnixSecs: number
): EpochYearCalendar[] {
  if (!Number.isSafeInteger(nowUnixSecs) || nowUnixSecs <= 0) return [];
  const timings = calendar.filter(isUsableTiming).sort((left, right) => left.epoch - right.epoch);
  const statusByEpoch = new Map(statuses.map((status) => [status.epoch, status] as const));
  const resolvedStatusByEpoch = new Map(
    timings.map((timing) => [timing.epoch, statusByEpoch.get(timing.epoch) ?? untrackedStatus(timing.epoch)] as const)
  );
  const dayStatuses = new Map<string, Array<{
    status: EpochYearStatus;
    timing: EpochCalendarEntry;
  }>>();
  let firstDayUnixSecs: number | null = null;

  for (const [index, timing] of timings.entries()) {
    if (timing.start_unix_secs > nowUnixSecs) continue;
    const status = resolvedStatusByEpoch.get(timing.epoch)!;
    const nextStart = timings[index + 1]?.start_unix_secs ?? null;
    const openEnd = nextStart === null
      ? nowUnixSecs
      : Math.min(nowUnixSecs, Math.max(timing.start_unix_secs, nextStart - 1));
    const end = Math.min(timing.end_unix_secs ?? openEnd, nowUnixSecs);
    const startDay = utcDayStart(timing.start_unix_secs);
    const endDay = utcDayStart(end);
    firstDayUnixSecs = firstDayUnixSecs === null ? startDay : Math.min(firstDayUnixSecs, startDay);

    for (let day = startDay; day <= endDay; day += 86_400) {
      const key = isoDate(day);
      const entries = dayStatuses.get(key) ?? [];
      entries.push({ status, timing });
      dayStatuses.set(key, entries);
    }
  }

  if (firstDayUnixSecs === null) return [];
  const firstYear = new Date(firstDayUnixSecs * 1_000).getUTCFullYear();
  const lastYear = new Date(nowUnixSecs * 1_000).getUTCFullYear();
  const today = isoDate(utcDayStart(nowUnixSecs));
  const years: EpochYearCalendar[] = [];

  for (let year = firstYear; year <= lastYear; year += 1) {
    const yearStart = Date.UTC(year, 0, 1) / 1_000;
    const nextYearStart = Date.UTC(year + 1, 0, 1) / 1_000;
    const firstWeekday = new Date(yearStart * 1_000).getUTCDay();
    const days: EpochYearDay[] = [];

    for (let day = yearStart; day < nextYearStart; day += 86_400) {
      const date = isoDate(day);
      const dateObject = new Date(day * 1_000);
      const dayOfYear = Math.round((day - yearStart) / 86_400);
      const entries = [...(dayStatuses.get(date) ?? [])]
        .sort((left, right) =>
          right.status.priority - left.status.priority ||
          right.status.epoch - left.status.epoch
        );
      const primary = entries[0] ?? null;
      const items = entries
        .sort((left, right) => left.status.epoch - right.status.epoch)
        .map(({ status }) => status);
      const statusSummary = items
        .map((status) => `Epoch ${status.epoch}: ${status.label}`)
        .join(' · ');
      days.push({
        date,
        day: dateObject.getUTCDate(),
        month: dateObject.getUTCMonth(),
        week: Math.floor((firstWeekday + dayOfYear) / 7),
        weekday: dateObject.getUTCDay(),
        epochs: items.map((status) => status.epoch),
        items,
        primary_epoch: primary?.status.epoch ?? null,
        tone: primary?.status.tone ?? null,
        label: `${dayLabelFormatter.format(dateObject)}${statusSummary ? ` · ${statusSummary}` : ''}`,
        estimated: entries.some(({ timing }) => timing.precision === 'estimated'),
        future: date > today,
        today: date === today
      });
    }

    const yearStatuses = new Map(
      timings
        .filter((timing) =>
          timing.start_unix_secs <= nowUnixSecs &&
          new Date(timing.start_unix_secs * 1_000).getUTCFullYear() === year
        )
        .map((timing) => [timing.epoch, resolvedStatusByEpoch.get(timing.epoch)!] as const)
    );

    years.push({
      year,
      week_count: days.at(-1)?.week === undefined ? 0 : days.at(-1)!.week + 1,
      epoch_count: yearStatuses.size,
      tracked_epoch_count: [...yearStatuses.values()].filter((status) => status.tracked).length,
      archived_epoch_count: [...yearStatuses.values()].filter((status) => status.archived).length,
      months: Array.from({ length: 12 }, (_, month) => {
        const first = Date.UTC(year, month, 1) / 1_000;
        const dayOfYear = Math.round((first - yearStart) / 86_400);
        return {
          month,
          label: monthLabelFormatter.format(new Date(first * 1_000)),
          week: Math.floor((firstWeekday + dayOfYear) / 7)
        };
      }),
      days
    });
  }

  return years;
}

function untrackedStatus(epoch: number): EpochYearStatus {
  return {
    epoch,
    tone: 'untracked',
    label: 'untracked',
    priority: 4,
    archived: false,
    tracked: false
  };
}

function utcDayStart(unixSecs: number) {
  const date = new Date(unixSecs * 1_000);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()) / 1_000;
}

function isoDate(unixSecs: number) {
  return new Date(unixSecs * 1_000).toISOString().slice(0, 10);
}

function isUsableTiming(timing: EpochCalendarEntry) {
  return Number.isSafeInteger(timing.epoch) &&
    Number.isSafeInteger(timing.start_unix_secs) &&
    timing.start_unix_secs > 0 &&
    (timing.end_unix_secs === null || (
      Number.isSafeInteger(timing.end_unix_secs) &&
      timing.end_unix_secs >= timing.start_unix_secs
    ));
}
