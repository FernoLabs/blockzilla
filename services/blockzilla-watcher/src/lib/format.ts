export function formatBytes(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value) || value < 0) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let amount = value;
  let unit = 0;
  while (amount >= 1024 && unit < units.length - 1) {
    amount /= 1024;
    unit += 1;
  }
  return `${amount.toLocaleString('en-US', { maximumFractionDigits: amount >= 100 ? 0 : 1 })} ${units[unit]}`;
}

/** Human-facing decimal units, matching disk and object-store billing. */
export function formatStorageBytes(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value) || value < 0) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let amount = value;
  let unit = 0;
  while (amount >= 1_000 && unit < units.length - 1) {
    amount /= 1_000;
    unit += 1;
  }
  return `${amount.toLocaleString('en-US', { maximumFractionDigits: amount >= 100 ? 0 : 1 })} ${units[unit]}`;
}
