export type CompactionWorkflow = 'historical' | 'live' | 'recompact';

export type CompactionHistoryEntry = {
  id: string;
  epoch: number;
  workflow: CompactionWorkflow;
  completed_unix_secs: number;
  duration_secs: number;
};

type CompletedEpochCandidate = {
  epoch: number;
  state: string;
};

export type RecentCompaction = Omit<CompactionHistoryEntry, 'completed_unix_secs' | 'duration_secs'> & {
  completed_unix_secs: number | null;
  duration_secs: number | null;
};

export function selectRecentCompactions(
  reported: CompactionHistoryEntry[] | undefined,
  epochs: CompletedEpochCandidate[],
  limit = 5
): RecentCompaction[] {
  const boundedLimit = Number.isFinite(limit) ? Math.max(0, Math.floor(limit)) : 5;
  return selectCompactionHistory(reported, epochs).slice(0, boundedLimit);
}

export function selectCompactionHistory(
  reported: CompactionHistoryEntry[] | undefined,
  epochs: CompletedEpochCandidate[]
): RecentCompaction[] {
  if (reported && reported.length > 0) {
    return reported
      .slice()
      .sort((left, right) =>
        right.completed_unix_secs - left.completed_unix_secs ||
        right.epoch - left.epoch
      );
  }

  return epochs
    .filter((epoch) => epoch.state === 'complete')
    .slice()
    .sort((left, right) => right.epoch - left.epoch)
    .map((epoch) => ({
      id: `legacy-epoch-${epoch.epoch}`,
      epoch: epoch.epoch,
      workflow: 'historical' as const,
      completed_unix_secs: null,
      duration_secs: null
    }));
}
