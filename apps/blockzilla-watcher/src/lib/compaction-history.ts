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
  artifacts?: Array<{
    kind: string;
    state: string;
    modified_unix_secs: number | null;
  }>;
};

export type RecentCompaction = Omit<CompactionHistoryEntry, 'completed_unix_secs' | 'duration_secs'> & {
  completed_unix_secs: number | null;
  duration_secs: number | null;
  timestamp_source: 'recorded' | 'archive_file' | 'unknown';
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
  const recorded = (reported ?? []).map((entry) => ({
    ...entry,
    timestamp_source: 'recorded' as const
  }));
  const recordedEpochs = new Set(recorded.map((entry) => entry.epoch));
  const inferred = epochs
    .filter((epoch) => epoch.state === 'complete')
    .filter((epoch) => !recordedEpochs.has(epoch.epoch))
    .map((epoch) => {
      const completedUnixSecs = archiveFileTimestamp(epoch);
      return {
        id: `legacy-epoch-${epoch.epoch}`,
        epoch: epoch.epoch,
        workflow: 'historical' as const,
        completed_unix_secs: completedUnixSecs,
        duration_secs: null,
        timestamp_source: completedUnixSecs === null ? 'unknown' as const : 'archive_file' as const
      };
    });

  return [...recorded, ...inferred].sort((left, right) =>
    (right.completed_unix_secs ?? Number.NEGATIVE_INFINITY) -
      (left.completed_unix_secs ?? Number.NEGATIVE_INFINITY) ||
    right.epoch - left.epoch
  );
}

function archiveFileTimestamp(epoch: CompletedEpochCandidate) {
  const metadata = (epoch.artifacts ?? []).find((artifact) =>
    artifact.kind === 'metadata' &&
    artifact.state === 'present' &&
    Number.isSafeInteger(artifact.modified_unix_secs) &&
    Number(artifact.modified_unix_secs) > 0
  );
  return metadata ? Number(metadata.modified_unix_secs) : null;
}
