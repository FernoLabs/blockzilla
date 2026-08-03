export type EpochRecord = {
  epoch: number;
};

export type SnapshotPatch<TSnapshot, TEpoch> = Omit<TSnapshot, 'epochs'> & {
  epochs_changed: TEpoch[];
  epochs_removed: number[];
};

export type SnapshotPatchSequenceAction = 'apply' | 'ignore' | 'resync';

export function snapshotPatchSequenceAction(
  lastSequence: number,
  incomingSequence: number
): SnapshotPatchSequenceAction {
  if (incomingSequence <= lastSequence) return 'ignore';
  if (lastSequence < 0 || incomingSequence > lastSequence + 1) return 'resync';
  return 'apply';
}

/**
 * Apply an incremental status event without mutating the current raw snapshot.
 * Epochs are reconciled by key; every other field in the patch replaces the
 * corresponding snapshot field.
 */
export function applySnapshotPatch<
  TEpoch extends EpochRecord,
  TSnapshot extends { epochs: TEpoch[] }
>(
  snapshot: TSnapshot,
  patch: SnapshotPatch<TSnapshot, TEpoch>
): TSnapshot {
  const { epochs_changed: changed, epochs_removed: removed, ...replacement } = patch;
  const epochs = new Map(snapshot.epochs.map((epoch) => [epoch.epoch, epoch]));

  for (const epoch of removed) epochs.delete(epoch);
  for (const epoch of changed) epochs.set(epoch.epoch, epoch);

  return {
    ...snapshot,
    ...replacement,
    epochs: [...epochs.values()].sort((left, right) => left.epoch - right.epoch)
  } as TSnapshot;
}
