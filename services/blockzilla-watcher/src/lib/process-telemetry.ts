export type ProcessIoEntry = {
  id: string;
  pid: number;
  name: string;
  user?: string | null;
  read_mib_per_sec?: number | null;
  write_mib_per_sec?: number | null;
  cpu_percent?: number | null;
  rss_bytes?: number | null;
  blockzilla_owned?: boolean;
};

export function processDiskIoRate(process: ProcessIoEntry) {
  const read = nonNegativeMetric(process.read_mib_per_sec);
  const write = nonNegativeMetric(process.write_mib_per_sec);
  if (read === null && write === null) return null;
  return (read ?? 0) + (write ?? 0);
}

export function rankProcessIo(processes: ProcessIoEntry[], limit = 10) {
  const boundedLimit = Number.isFinite(limit) ? Math.max(0, Math.floor(limit)) : processes.length;
  return processes
    .filter((process) => process.blockzilla_owned !== true)
    .slice()
    .sort((left, right) => {
      const rateOrder = compareMetricsDescending(
        processDiskIoRate(left),
        processDiskIoRate(right)
      );
      if (rateOrder !== 0) return rateOrder;
      if (left.name !== right.name) return left.name < right.name ? -1 : 1;
      return left.pid - right.pid || left.id.localeCompare(right.id);
    })
    .slice(0, boundedLimit);
}

export function hasProcessResourceMetrics(processes: ProcessIoEntry[]) {
  return processes.some((process) =>
    nonNegativeMetric(process.cpu_percent) !== null ||
    nonNegativeMetric(process.rss_bytes) !== null
  );
}

export function processMetric(value: number | null | undefined) {
  return nonNegativeMetric(value);
}

function compareMetricsDescending(left: number | null, right: number | null) {
  if (left === null) return right === null ? 0 : 1;
  if (right === null) return -1;
  return right - left;
}

function nonNegativeMetric(value: number | null | undefined) {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;
}
