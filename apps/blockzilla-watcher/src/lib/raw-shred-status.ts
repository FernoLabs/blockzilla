export type RawShredStatus = {
  schema_version: 1;
  updated_unix_secs: number;
  hivezilla: {
    availability: 'available' | 'unavailable';
    status_fresh: boolean;
    state: 'waiting' | 'receiving' | 'stalled' | 'stopped' | 'unavailable';
    latest_slot: number | null;
    spool_bytes: number | null;
    spool_max_bytes: number | null;
    filesystem_free_bytes: number | null;
    filesystem_total_bytes: number | null;
    reserve_free_bytes: number | null;
    accepted_total: number | null;
    invalid_total: number | null;
  };
};

const STATES = new Set(['waiting', 'receiving', 'stalled', 'stopped', 'unavailable']);

export function parseRawShredStatus(value: unknown): RawShredStatus | null {
  const root = record(value);
  const hivezilla = record(root?.hivezilla);
  if (!root || root.schema_version !== 1 || !hivezilla) return null;
  const updated = positiveInteger(root.updated_unix_secs);
  const availability = hivezilla.availability;
  const state = hivezilla.state;
  if (updated === null || (availability !== 'available' && availability !== 'unavailable') ||
    typeof state !== 'string' || !STATES.has(state) || typeof hivezilla.status_fresh !== 'boolean') return null;
  const fields = [
    'latest_slot', 'spool_bytes', 'spool_max_bytes', 'filesystem_free_bytes',
    'filesystem_total_bytes', 'reserve_free_bytes', 'accepted_total', 'invalid_total'
  ] as const;
  const parsed = Object.fromEntries(fields.map((field) => [field, nullableInteger(hivezilla[field])])) as Record<typeof fields[number], number | null | undefined>;
  if (fields.some((field) => parsed[field] === undefined)) return null;
  const values = parsed as Record<typeof fields[number], number | null>;
  if (availability === 'unavailable' && fields.some((field) => parsed[field] !== null)) return null;
  if (availability === 'available' && fields.some((field) => parsed[field] === null)) return null;
  if (values.filesystem_free_bytes !== null && values.filesystem_total_bytes !== null && values.filesystem_free_bytes > values.filesystem_total_bytes) return null;
  if (values.reserve_free_bytes !== null && values.filesystem_total_bytes !== null && values.reserve_free_bytes > values.filesystem_total_bytes) return null;
  if (values.spool_bytes !== null && values.spool_max_bytes !== null && values.spool_bytes > values.spool_max_bytes) return null;
  return value as RawShredStatus;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function positiveInteger(value: unknown) {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : null;
}

function nullableInteger(value: unknown): number | null | undefined {
  if (value === null) return null;
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}
