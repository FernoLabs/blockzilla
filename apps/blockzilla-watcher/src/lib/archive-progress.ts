export type ArchiveProgressState =
  | { state: 'ready'; percent: number; bytesDone: number; bytesTotal: number }
  | { state: 'scanning' | 'empty'; percent: null };

type ArchiveProgressEpoch = {
  state: string;
  car_bytes: number;
  progress: { progress_pct: number | null };
};

type ArchiveProgressInput = {
  epochs: ArchiveProgressEpoch[];
  inventoryComplete: boolean | null | undefined;
};

export function archiveProgressState({
  epochs,
  inventoryComplete
}: ArchiveProgressInput): ArchiveProgressState {
  if (inventoryComplete === false) return { state: 'scanning', percent: null };

  const bytesTotal = epochs.reduce(
    (total, epoch) => total + validBytes(epoch.car_bytes),
    0
  );
  if (bytesTotal <= 0) {
    return { state: 'empty', percent: null };
  }

  const bytesDone = epochs.reduce((total, epoch) => {
    const bytes = validBytes(epoch.car_bytes);
    if (epoch.state === 'complete') return total + bytes;

    const progress = Number.isFinite(epoch.progress.progress_pct)
      ? clampPercent(epoch.progress.progress_pct ?? 0)
      : 0;
    return total + bytes * progress / 100;
  }, 0);

  return {
    state: 'ready',
    percent: clampPercent(bytesDone * 100 / bytesTotal),
    bytesDone,
    bytesTotal
  };
}

function validBytes(value: number) {
  return Number.isFinite(value) && value > 0 ? value : 0;
}

function clampPercent(value: number) {
  return Math.max(0, Math.min(100, value));
}
