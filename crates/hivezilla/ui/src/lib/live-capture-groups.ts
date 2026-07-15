export type GroupableLiveCapture = {
  id: string;
  epoch: number | null;
  state: string;
  is_current?: boolean;
  blocks_written?: number;
  updated_unix_secs: number;
  message?: string | null;
  source_capture_ids?: string[];
  superseded_by?: string | null;
};

export type GroupedLiveCaptures<T extends GroupableLiveCapture> = {
  visible: T[];
  sourcesByBundle: Map<string, T[]>;
};

const INACTIVE_CAPTURE_MESSAGE = 'capture is neither active nor marked ready for packaging';

export function isBenignLiveDiagnostic(capture: GroupableLiveCapture) {
  const knownDiagnosticId = capture.id.startsWith('.') ||
    capture.id.startsWith('codex-') ||
    capture.id.startsWith('debug-') ||
    capture.id.startsWith('bench-');
  return capture.state === 'blocked' &&
    capture.message === INACTIVE_CAPTURE_MESSAGE &&
    (capture.epoch === null || capture.blocks_written === 0 || knownDiagnosticId);
}

export function isLiveWorkflowCapture(capture: GroupableLiveCapture) {
  return capture.state !== 'blocked' || !isBenignLiveDiagnostic(capture);
}

const NEVER_HIDE_STATES = new Set(['capturing', 'repair_gate', 'packaging']);
const REPAIR_BUNDLE_STATES = new Set([
  'repair_required',
  'ready_to_package',
  'packaging',
  'packaged'
]);
const ALWAYS_SHOW_STATES = new Set([
  'capturing',
  'repair_gate',
  'repair_required',
  'ready_to_package',
  'packaging',
  'packaged'
]);

/**
 * Apply the backend's repair-bundle relationship defensively. A stale or
 * malformed snapshot cannot hide active, repair-gated, or cross-epoch data.
 */
export function groupLiveCaptures<T extends GroupableLiveCapture>(
  captures: T[]
): GroupedLiveCaptures<T> {
  const byId = new Map(captures.map((capture) => [capture.id, capture]));
  const validBundles = new Map(
    captures
      .filter((capture) => REPAIR_BUNDLE_STATES.has(capture.state))
      .map((capture) => [capture.id, capture])
  );
  const sourcesByBundle = new Map<string, T[]>();
  for (const bundle of validBundles.values()) {
    const sources = (bundle.source_capture_ids ?? [])
      .map((id) => byId.get(id))
      .filter((capture): capture is T => capture !== undefined);
    sourcesByBundle.set(bundle.id, sources);
  }

  const visible = captures.filter((capture) => {
    if (!capture.superseded_by || NEVER_HIDE_STATES.has(capture.state)) return true;
    const bundle = validBundles.get(capture.superseded_by);
    return !bundle ||
      capture.epoch === null ||
      capture.epoch !== bundle.epoch ||
      !(bundle.source_capture_ids ?? []).includes(capture.id);
  });
  return { visible, sourcesByBundle };
}

/**
 * The epoch map needs one representative per epoch, but the live-work panel
 * must additionally retain every active or repair-required workflow.
 */
export function selectVisibleLiveCaptures<T extends GroupableLiveCapture>(
  primaryByEpoch: T[],
  captures: T[],
  current: T | null
): T[] {
  const selected = new Map<string, T>();
  for (const capture of primaryByEpoch) {
    if (capture.state !== 'complete') selected.set(capture.id, capture);
  }
  for (const capture of captures) {
    if (ALWAYS_SHOW_STATES.has(capture.state) || isLiveWorkflowCapture(capture)) {
      if (capture.state !== 'complete' && capture.state !== 'failed') selected.set(capture.id, capture);
    }
  }
  if (current) selected.set(current.id, current);

  return [...selected.values()].sort((left, right) => {
    if (current) {
      if (left.id === current.id) return -1;
      if (right.id === current.id) return 1;
    }
    return (right.epoch ?? -1) - (left.epoch ?? -1) ||
      right.updated_unix_secs - left.updated_unix_secs ||
      right.id.localeCompare(left.id);
  });
}
