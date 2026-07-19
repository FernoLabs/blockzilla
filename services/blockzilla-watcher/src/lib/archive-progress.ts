export type ArchiveProgressState =
  | { state: 'ready'; percent: number }
  | { state: 'scanning' | 'empty'; percent: null };

type ArchiveProgressInput = {
  reportedPercent: number | null | undefined;
  completedEpochs: number | null | undefined;
  totalEpochs: number | null | undefined;
  inventoryComplete: boolean | null | undefined;
};

export function archiveProgressState({
  reportedPercent,
  completedEpochs,
  totalEpochs,
  inventoryComplete
}: ArchiveProgressInput): ArchiveProgressState {
  if (inventoryComplete === false) return { state: 'scanning', percent: null };
  if (!Number.isFinite(totalEpochs) || (totalEpochs ?? 0) <= 0) {
    return { state: 'empty', percent: null };
  }

  const fallbackPercent = Number.isFinite(completedEpochs)
    ? ((completedEpochs ?? 0) * 100) / (totalEpochs ?? 1)
    : 0;
  const percent = Number.isFinite(reportedPercent) ? (reportedPercent ?? 0) : fallbackPercent;

  return { state: 'ready', percent: Math.max(0, Math.min(100, percent)) };
}
