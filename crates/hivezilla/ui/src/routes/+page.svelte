<script lang="ts">
  import { onMount, tick } from 'svelte';
  import {
    liveEtaSecs,
    liveEtaStatus,
    livePeakRssBytes,
    liveRate,
    liveRssBytes
  } from '$lib/live-metrics';
  import {
    groupLiveCaptures,
    isBenignLiveDiagnostic,
    isLiveWorkflowCapture,
    selectVisibleLiveCaptures
  } from '$lib/live-capture-groups';
  import {
    applySnapshotPatch,
    snapshotPatchSequenceAction,
    type SnapshotPatch
  } from '$lib/snapshot-patch';
  import { formatBytes } from '$lib/format';

  type HistoricalState = 'queued' | 'scanning' | 'scan_ready' | 'finalizing' | 'complete' | 'failed' | 'blocked';
  type LiveState = 'capturing' | 'repair_gate' | 'repair_required' | 'ready_to_package' | 'packaging' | 'packaged' | 'complete' | 'failed' | 'blocked';
  type VisualState = 'complete' | 'first-seen-complete' | 'legacy-complete' | 'active' | 'ready' | 'finalizing' | 'partial' | 'queued' | 'missing' | 'na' | 'attention' | 'failed';

  type ArtifactStatus = {
    kind: string;
    state: string;
    requirement: string;
    required_now: boolean;
    bytes: number;
    modified_unix_secs: number | null;
    message: string | null;
  };

  type SchedulerStageSnapshot = {
    state?: string | null;
    phase?: string | null;
    completed?: number | null;
    done?: number | null;
    epochs_done?: number | null;
    scanned?: number | null;
    observed?: number | null;
    total?: number | null;
    epochs_total?: number | null;
    progress_pct?: number | null;
    current_epoch?: number | null;
    complete?: boolean | null;
    message?: string | null;
    updated_unix_secs?: number | null;
    epochs_discovered?: number | null;
    epochs_classified?: number | null;
    errors?: unknown[];
    pending?: number | null;
    active?: number | null;
    terminal_gaps?: number | null;
    deferred_finalizers?: number | null;
    blocked_reason?: string | null;
    wait_reason?: string | null;
    [key: string]: unknown;
  };

  type ArtifactGroup = {
    id: 'car' | 'preflight' | 'source' | 'archive';
    label: string;
    artifacts: ArtifactStatus[];
  };

  type ProgressSnapshot = {
    phase: string | null;
    state: string | null;
    pid: number | null;
    blocks_done: number;
    blocks_total: number;
    transactions_done: number;
    first_slot: number | null;
    last_slot: number | null;
    progress_pct: number | null;
    blocks_per_sec: number | null;
    input_mib_per_sec: number | null;
    disk_read_mib_per_sec?: number | null;
    disk_write_mib_per_sec?: number | null;
    eta_secs: number | null;
    rss_bytes: number | null;
    peak_rss_bytes?: number | null;
    updated_unix_secs: number | null;
    elapsed_secs: number | null;
  };

  type EpochStatus = {
    epoch: number;
    state: HistoricalState;
    registry_order: 'usage_sorted' | 'first_seen' | 'unknown';
    input_path: string | null;
    output_path: string;
    car_bytes: number;
    progress: ProgressSnapshot;
    artifacts?: ArtifactStatus[];
    updated_unix_secs: number;
    message: string | null;
  };

  type LaneStatus = {
    id: string;
    kind: string;
    epoch: number | null;
    capture_id: string | null;
    phase: string;
    state: string;
    pid: number | null;
    progress: ProgressSnapshot;
    rss_bytes: number | null;
    started_unix_secs: number | null;
    updated_unix_secs: number;
    auto_paused?: boolean;
    auto_pause_reason?: string | null;
  };

  type LiveStatus = {
    id: string;
    epoch: number | null;
    is_current?: boolean;
    state: LiveState;
    capture_dir: string;
    output_path: string | null;
    ready_to_package: boolean;
    repair_gate: boolean;
    source_capture_ids?: string[];
    superseded_by?: string | null;
    first_slot: number | null;
    last_slot: number | null;
    blocks_written: number;
    eta_secs?: number | null;
    slots_per_sec?: number | null;
    rss_bytes?: number | null;
    peak_rss_bytes?: number | null;
    progress: ProgressSnapshot;
    artifacts?: ArtifactStatus[];
    message: string | null;
    updated_unix_secs: number;
  };

  type EpochMapEntry =
    | { epoch: number; kind: 'historical'; status: EpochStatus }
    | { epoch: number; kind: 'live'; status: LiveStatus };

  type FinalizerItem = {
    kind: string;
    epoch: number | null;
    id: string;
    phase?: string;
    state: string;
    estimated_memory_bytes?: number;
    deferred_reason?: string | null;
  };

  type MachineStatus = {
    memory_total_bytes: number;
    memory_used_bytes: number;
    memory_available_bytes: number;
    swap_total_bytes: number;
    swap_used_bytes: number;
    disk_total_bytes: number;
    disk_used_bytes: number;
    disk_available_bytes: number;
    car_disk_total_bytes?: number;
    car_disk_used_bytes?: number;
    car_disk_available_bytes?: number;
    car_disk_shared_with_archive?: boolean;
    archive_device_major?: number | null;
    archive_device_minor?: number | null;
    archive_device_name?: string | null;
    archive_device_read_mib_per_sec?: number | null;
    archive_device_write_mib_per_sec?: number | null;
    load_1m: number;
    service_rss_bytes: number;
    children_rss_bytes: number;
    memory_pressure_some_avg10?: number | null;
    memory_pressure_full_avg10?: number | null;
    io_pressure_some_avg10?: number | null;
    io_pressure_full_avg10?: number | null;
  };

  type PipelineError = {
    at_unix_secs: number;
    scope: string;
    message: string;
  };

  type PipelineSummary = {
    epochs_total: number;
    queued: number;
    scanning: number;
    scan_ready: number;
    finalizing: number;
    complete: number;
    failed: number;
    blocked: number;
    progress_pct: number;
    eta_secs: number | null;
    queue_eta_secs?: number | null;
    queue_eta_reason?: string | null;
    queue_jobs_remaining?: number;
    queue_capacity?: number;
    queue_job_duration_secs?: number | null;
    queue_duration_samples?: number;
    queue_bytes_remaining?: number;
    queue_read_mib_per_sec?: number | null;
    queue_read_active_workers?: number;
    queue_read_sampled_workers?: number;
    blocks_done: number;
    blocks_total: number;
    blocks_per_sec: number;
    disk_read_mib_per_sec?: number | null;
    disk_write_mib_per_sec?: number | null;
    disk_io_active_roots?: number;
    disk_io_sampled_roots?: number;
    scan_eta_secs?: number | null;
    scan_capacity_configured: number;
    scan_capacity_admitted: number;
    admission_blocked_reason: string | null;
    finalizer_admission_blocked_reason?: string | null;
    legacy_compact_running?: number;
    legacy_compact_paused?: number;
    legacy_compact_auto_paused?: number;
    legacy_compact_capacity_configured?: number;
    legacy_compact_capacity_unbounded?: boolean;
    legacy_compact_capacity_effective?: number;
    legacy_compact_capacity_admitted?: number;
    legacy_compact_tuning_enabled?: boolean;
    legacy_compact_tuning_state?: string | null;
    legacy_compact_tuning_target?: number;
    legacy_compact_tuning_accepted_lanes?: number;
    legacy_compact_tuning_baseline_mib_per_sec?: number | null;
    legacy_compact_tuning_objective_mib_per_sec?: number | null;
    legacy_compact_tuning_rate_source?: string | null;
    legacy_compact_useful_input_mib_per_sec?: number | null;
    legacy_compact_useful_input_active_lanes?: number;
    legacy_compact_useful_input_sampled_lanes?: number;
    legacy_compact_tuning_backoff_until_unix_secs?: number | null;
    legacy_compact_tuning_last_decision?: string | null;
    legacy_compact_admission_blocked_reason?: string | null;
    legacy_compact_auto_pause_enabled?: boolean;
    legacy_compact_min_running?: number;
    legacy_compact_memory_guard_mib?: number;
    legacy_compact_memory_pause_available_mib?: number;
    legacy_compact_memory_resume_available_mib?: number;
    legacy_compact_io_pause_full_avg10?: number;
    legacy_compact_io_resume_full_avg10?: number;
    legacy_compact_cpu_budget_cores?: number;
    legacy_compact_pause_cooldown_secs?: number;
    legacy_compact_last_action?: string | null;
    legacy_compact_last_action_unix_secs?: number | null;
  };

  type CapabilitySnapshot = {
    control_enabled: boolean;
    authenticated_controls_required: boolean;
    can_pause_scheduler: boolean;
    can_retry_failed: boolean;
  };

  type SchedulerSnapshot = {
    paused: boolean;
    updated_unix_secs: number;
    inventory?: SchedulerStageSnapshot | null;
    scan_sweep?: SchedulerStageSnapshot | null;
  };

  type PipelineSnapshot = {
    schema_version: number;
    sequence: number;
    now_unix_secs: number;
    current_epoch?: number | null;
    observer_mode: boolean;
    capabilities: CapabilitySnapshot;
    scheduler: SchedulerSnapshot;
    inventory?: SchedulerStageSnapshot | null;
    scan_sweep?: SchedulerStageSnapshot | null;
    summary: PipelineSummary;
    machine: MachineStatus;
    live: LiveStatus[];
    epochs: EpochStatus[];
    lanes: LaneStatus[];
    finalizer_queue: FinalizerItem[];
    errors: PipelineError[];
  };

  type PipelineSnapshotPatch = SnapshotPatch<PipelineSnapshot, EpochStatus> & {
    current_epoch: number | null;
    inventory: SchedulerStageSnapshot | null;
    scan_sweep: SchedulerStageSnapshot | null;
  };

  type ConnectionState = 'connecting' | 'live' | 'retrying' | 'offline';
  type JsonRecord = Record<string, unknown>;

  const TOKEN_STORAGE_KEY = 'blockzilla_nas_control_token';
  const SLOTS_PER_EPOCH = 432_000;
  const VISUAL_META: Record<VisualState, { label: string; icon: string }> = {
    complete: { label: 'complete', icon: '✓' },
    'first-seen-complete': { label: 'complete, recompactable', icon: 'R' },
    'legacy-complete': { label: 'legacy complete', icon: 'L' },
    active: { label: 'active', icon: '▶' },
    ready: { label: 'ready', icon: '◆' },
    finalizing: { label: 'finalizing', icon: '◐' },
    partial: { label: 'partial', icon: '◒' },
    queued: { label: 'queued', icon: '○' },
    missing: { label: 'source missing', icon: '−' },
    na: { label: 'not applicable', icon: '·' },
    attention: { label: 'attention', icon: '!' },
    failed: { label: 'failed', icon: '×' }
  };
  const EPOCH_LEGEND: { tone: VisualState; label: string }[] = [
    { tone: 'complete', label: 'complete' },
    { tone: 'first-seen-complete', label: 'complete, recompactable' },
    { tone: 'legacy-complete', label: 'legacy complete' },
    { tone: 'active', label: 'active' },
    { tone: 'ready', label: 'ready' },
    { tone: 'finalizing', label: 'finalizing' },
    { tone: 'partial', label: 'partial' },
    { tone: 'queued', label: 'queued' },
    { tone: 'missing', label: 'source missing' },
    { tone: 'attention', label: 'needs action' },
    { tone: 'failed', label: 'failed' }
  ];
  const ARTIFACT_GROUP_ORDER: ArtifactGroup['id'][] = ['car', 'preflight', 'source', 'archive'];
  const ARTIFACT_GROUP_LABELS: Record<ArtifactGroup['id'], string> = {
    car: 'CAR',
    preflight: 'Preflight',
    source: 'Source PoH + shred',
    archive: 'Archive sidecars'
  };

  let snapshot = $state.raw<PipelineSnapshot | null>(null);
  let connectionState = $state<ConnectionState>('connecting');
  let selectedEpoch = $state<number | null>(null);
  let connectionMessage = $state('Connecting');
  let tokenDialog = $state<HTMLDialogElement>();
  let tokenDraft = $state('');
  let hasToken = $state(false);
  let actionBusy = $state<string | null>(null);
  let actionNotice = $state<string | null>(null);
  let selectionAnnouncement = $state('');
  let epochTabStop = $state<number | null>(null);

  const groupedLiveCaptures = $derived(groupLiveCaptures(snapshot?.live ?? []));
  const liveCapturesByEpoch = $derived(
    canonicalLiveCaptures(
      groupedLiveCaptures.visible.filter(isLiveWorkflowCapture),
      snapshot?.current_epoch ?? null
    )
  );
  const currentLiveCapture = $derived(
    groupedLiveCaptures.visible.find((capture) => capture.is_current) ??
      groupedLiveCaptures.visible.find((capture) => capture.epoch === snapshot?.current_epoch && capture.state === 'capturing') ??
      groupedLiveCaptures.visible.find((capture) => capture.state === 'capturing') ??
      null
  );
  const visibleLiveCaptures = $derived(
    selectVisibleLiveCaptures(
      liveCapturesByEpoch,
      groupedLiveCaptures.visible,
      currentLiveCapture
    )
  );
  const waitingLiveCaptureCount = $derived(
    visibleLiveCaptures.filter((capture) =>
      capture !== currentLiveCapture && ['repair_gate', 'ready_to_package', 'packaging', 'packaged'].includes(capture.state)
    ).length
  );
  const liveNeedsActionCount = $derived(
    groupedLiveCaptures.visible.filter((capture) =>
      capture.state === 'repair_required' ||
      capture.state === 'failed' ||
      (capture.state === 'blocked' && !isBenignLiveDiagnostic(capture))
    ).length
  );
  const completedLiveCaptureCount = $derived(
    liveCapturesByEpoch.filter((capture) => capture.state === 'complete').length
  );
  const epochMap = $derived(buildEpochMap(snapshot?.epochs ?? [], liveCapturesByEpoch));
  const latestTrackedEpoch = $derived(epochMap.at(-1)?.epoch ?? null);
  const selectedEpochEntry = $derived(
    selectedEpoch === null ? null : (epochMap.find((entry) => entry.epoch === selectedEpoch) ?? null)
  );
  const selectedEpochStatus = $derived(
    selectedEpochEntry?.kind === 'historical' ? selectedEpochEntry.status : null
  );
  const selectedLiveStatus = $derived(
    selectedEpochEntry?.kind === 'live' ? selectedEpochEntry.status : null
  );
  const selectedArtifactGroups = $derived(
    selectedEpochStatus
      ? groupArtifacts(selectedEpochStatus.artifacts ?? [])
      : selectedLiveStatus
        ? groupArtifacts(selectedLiveStatus.artifacts ?? [])
        : []
  );
  const selectedSourceRetired = $derived(
    selectedEpochStatus ? epochHasRetiredSource(selectedEpochStatus) : false
  );
  const selectedLegacyNoAccessMessage = $derived(
    selectedEpochStatus ? legacyNoAccessCompletionMessage(selectedEpochStatus) : null
  );
  const selectedRegistryOrderContext = $derived(
    selectedEpochStatus ? registryOrderContext(selectedEpochStatus) : null
  );
  const epochToneCounts = $derived.by(() => {
    const counts: Record<VisualState, number> = {
      complete: 0,
      'first-seen-complete': 0,
      'legacy-complete': 0,
      active: 0,
      ready: 0,
      finalizing: 0,
      partial: 0,
      queued: 0,
      missing: 0,
      na: 0,
      attention: 0,
      failed: 0
    };
    for (const entry of epochMap) {
      const tone = epochMapVisualState(entry);
      counts[tone] += 1;
    }
    return counts;
  });
  const activeLanes = $derived(
    snapshot?.lanes.filter((lane) =>
      !['idle', 'done', 'complete', 'completed', 'failed', 'stopped', 'cancelled'].includes(
        normalizedState(lane.state)
      )
    ) ?? []
  );
  const activeHistoricalLanes = $derived(
    activeLanes.filter((lane) => lane.epoch !== null && !lane.kind.startsWith('live_'))
  );
  const legacyCompactLanes = $derived(
    activeLanes.filter((lane) => lane.kind === 'historical_compact_reuse')
  );
  const legacyCompactRunning = $derived(
    snapshot?.summary.legacy_compact_running ??
      legacyCompactLanes.filter((lane) => lane.state !== 'paused').length
  );
  const legacyCompactPaused = $derived(
    snapshot?.summary.legacy_compact_paused ??
      legacyCompactLanes.filter((lane) => lane.state === 'paused').length
  );
  const legacyCompactAutoPaused = $derived(
    snapshot?.summary.legacy_compact_auto_paused ??
      legacyCompactLanes.filter((lane) => lane.auto_paused === true).length
  );
  const activeEpochs = $derived(
    new Set(
      activeHistoricalLanes
        .map((lane) => lane.epoch)
        .filter((epoch): epoch is number => epoch !== null)
    ).size
  );
  const historicalNeedsAction = $derived(
    snapshot ? snapshot.summary.blocked + snapshot.summary.failed : 0
  );
  const liveCaptureDiagnostics = $derived(
    groupedLiveCaptures.visible.filter((capture) =>
      ['blocked', 'failed', 'repair_required'].includes(capture.state)
    )
  );
  const hiddenLiveCaptureDiagnostics = $derived(
    liveCaptureDiagnostics.filter((issue) => !visibleLiveCaptures.some((capture) => capture.id === issue.id))
  );
  const runnableQueueEtaSecs = $derived(queueEtaSecs(snapshot?.summary));
  const runnableQueueEtaReason = $derived(queueEtaReason(snapshot?.summary));
  const runnableQueueEtaTitle = $derived(
    queueEtaExplanation(runnableQueueEtaReason, historicalNeedsAction)
  );
  const historicalCompletionPct = $derived(
    snapshot ? percent(snapshot.summary.complete, snapshot.summary.epochs_total) : 0
  );
  const machineMemoryPct = $derived(percent(snapshot?.machine.memory_used_bytes, snapshot?.machine.memory_total_bytes));
  const machineDiskPct = $derived(percent(snapshot?.machine.disk_used_bytes, snapshot?.machine.disk_total_bytes));
  const machineSwapPct = $derived(percent(snapshot?.machine.swap_used_bytes, snapshot?.machine.swap_total_bytes));
  const carDiskPct = $derived(percent(snapshot?.machine.car_disk_used_bytes, snapshot?.machine.car_disk_total_bytes));
  const hasSeparateCarStorage = $derived(Boolean(
    snapshot?.machine.car_disk_total_bytes &&
    (snapshot.machine.car_disk_shared_with_archive === false ||
      (snapshot.machine.car_disk_shared_with_archive === undefined &&
        snapshot.machine.car_disk_total_bytes !== snapshot.machine.disk_total_bytes))
  ));
  const controlsDisabledReason = $derived.by(() => {
    if (!snapshot || snapshot.observer_mode) return 'Controls are disabled in observer mode.';
    if (!snapshot.capabilities.control_enabled) return 'Controls are not configured on this pipeline service.';
    if (snapshot.capabilities.authenticated_controls_required && !hasToken) return 'Add the control token to enable actions.';
    return null;
  });

  onMount(() => {
    let disposed = false;
    let lastSequence = -1;
    let statusFetchInFlight = false;

    tokenDraft = window.sessionStorage.getItem(TOKEN_STORAGE_KEY) ?? '';
    hasToken = tokenDraft.length > 0;

    function acceptPayload(value: unknown, sequence?: number) {
      const normalized = parseSnapshot(value);
      if (!normalized) return false;
      const incomingSequence = sequence ?? normalized.sequence;
      if (sequence !== undefined && sequence !== normalized.sequence) return false;
      if (incomingSequence <= lastSequence) {
        const currentSnapshotTime = snapshot?.now_unix_secs ?? 0;
        // A restarted service resets its process-local sequence. A newer
        // snapshot timestamp is the evidence that this is a restart rather
        // than an out-of-order event from the current process.
        if (normalized.now_unix_secs <= currentSnapshotTime) return true;
      }
      lastSequence = incomingSequence;
      if (selectedEpoch !== null && !snapshotContainsEpoch(normalized, selectedEpoch)) {
        selectionAnnouncement = `Epoch ${selectedEpoch} details closed because that epoch is no longer tracked.`;
        selectedEpoch = null;
      }
      snapshot = normalized;
      return true;
    }

    function requestSnapshotResync(message: string) {
      connectionState = 'retrying';
      connectionMessage = message;
      void resyncSnapshot();
    }

    function acceptSnapshotPatch(patch: PipelineSnapshotPatch, envelopeSequence: number) {
      if (!snapshot) {
        requestSnapshotResync('Incremental event arrived before its base snapshot; resyncing.');
        return;
      }
      const sequenceAction = snapshotPatchSequenceAction(lastSequence, envelopeSequence);
      if (sequenceAction === 'ignore') return;
      if (patch.sequence !== envelopeSequence) {
        requestSnapshotResync('Incremental event sequence did not match its envelope; resyncing.');
        return;
      }
      if (patch.schema_version !== snapshot.schema_version) {
        requestSnapshotResync('Incremental event schema changed; resyncing.');
        return;
      }
      if (sequenceAction === 'resync') {
        requestSnapshotResync(`Incremental event gap after sequence ${lastSequence}; resyncing.`);
        return;
      }

      const merged = applySnapshotPatch(snapshot, patch);
      lastSequence = envelopeSequence;
      if (selectedEpoch !== null && !snapshotContainsEpoch(merged, selectedEpoch)) {
        selectionAnnouncement = `Epoch ${selectedEpoch} details closed because that epoch is no longer tracked.`;
        selectedEpoch = null;
      }
      snapshot = merged;
      connectionState = 'live';
      connectionMessage = 'Live event stream';
    }

    async function resyncSnapshot() {
      if (statusFetchInFlight) return;
      statusFetchInFlight = true;
      try {
        const response = await fetch('/api/v1/status', { headers: { accept: 'application/json' } });
        if (!response.ok) throw new Error(`status ${response.status}`);
        if (!acceptPayload(await response.json())) throw new Error('invalid status snapshot');
        if (events.readyState === EventSource.OPEN) {
          connectionState = 'live';
          connectionMessage = 'Live event stream';
        } else {
          connectionState = 'retrying';
          connectionMessage = 'Snapshot current; event stream reconnecting';
        }
      } catch (error) {
        if (!disposed) {
          connectionState = events.readyState === EventSource.CLOSED ? 'offline' : 'retrying';
          connectionMessage = `Event stream resync failed: ${errorMessage(error)}`;
        }
      } finally {
        statusFetchInFlight = false;
      }
    }

    const events = new EventSource('/api/v1/events');
    events.onopen = () => {
      if (disposed) return;
      // The service sequence is process-local and restarts from zero. Reset
      // the client-side guard whenever EventSource establishes a new stream
      // so a service restart cannot leave the dashboard frozen on old state.
      lastSequence = -1;
      connectionState = snapshot ? 'retrying' : 'connecting';
      connectionMessage = 'Event stream connected; waiting for snapshot';
    };
    events.addEventListener('snapshot', (event) => {
      try {
        const envelope = asRecord(JSON.parse(event.data) as unknown);
        const sequence = integerValue(envelope?.sequence);
        if (envelope?.type !== 'snapshot' || sequence === null) {
          requestSnapshotResync('Ignored an invalid full snapshot event; resyncing.');
          return;
        }
        if (!acceptPayload(envelope.data, sequence)) {
          requestSnapshotResync('Rejected an invalid full snapshot event; resyncing.');
          return;
        }
        connectionState = 'live';
        connectionMessage = 'Live event stream';
      } catch (error) {
        requestSnapshotResync(`Ignored an invalid full snapshot event: ${errorMessage(error)}`);
      }
    });
    events.addEventListener('snapshot_patch', (event) => {
      try {
        const envelope = asRecord(JSON.parse(event.data) as unknown);
        const patch = parseSnapshotPatch(envelope?.data);
        const sequence = integerValue(envelope?.sequence);
        if (envelope?.type !== 'snapshot_patch' || !patch || sequence === null) {
          requestSnapshotResync('Ignored an invalid incremental event; resyncing.');
          return;
        }
        acceptSnapshotPatch(patch, sequence);
      } catch (error) {
        requestSnapshotResync(`Ignored an invalid incremental event: ${errorMessage(error)}`);
      }
    });
    events.addEventListener('resync', (event) => {
      try {
        const envelope = asRecord(JSON.parse(event.data) as unknown);
        if (envelope?.type !== 'resync') {
          requestSnapshotResync('Ignored an invalid resync event; resyncing.');
          return;
        }
        requestSnapshotResync('Event stream requested a full resync.');
      } catch (error) {
        requestSnapshotResync(`Ignored an invalid resync event: ${errorMessage(error)}`);
      }
    });
    events.onerror = () => {
      if (disposed) return;
      connectionState = events.readyState === EventSource.CLOSED ? 'offline' : 'retrying';
      connectionMessage = events.readyState === EventSource.CLOSED ? 'Event stream closed' : 'Event stream reconnecting';
      void resyncSnapshot();
    };

    return () => {
      disposed = true;
      events.close();
    };
  });

  function parseSnapshot(value: unknown): PipelineSnapshot | null {
    const root = asRecord(value);
    if (
      !root ||
      integerValue(root.schema_version) === null ||
      integerValue(root.sequence) === null ||
      integerValue(root.now_unix_secs) === null ||
      typeof root.observer_mode !== 'boolean' ||
      !asRecord(root.capabilities) ||
      !asRecord(root.scheduler) ||
      !asRecord(root.summary) ||
      !asRecord(root.machine) ||
      !Array.isArray(root.epochs) || !root.epochs.every(isEpochStatusValue) ||
      !Array.isArray(root.lanes) || !root.lanes.every(isLaneStatusValue) ||
      !Array.isArray(root.live) || !root.live.every(isLiveStatusValue) ||
      !Array.isArray(root.finalizer_queue) || !root.finalizer_queue.every(isFinalizerItemValue) ||
      !Array.isArray(root.errors) || !root.errors.every(isPipelineErrorValue)
    ) {
      return null;
    }
    return value as PipelineSnapshot;
  }

  function parseSnapshotPatch(value: unknown): PipelineSnapshotPatch | null {
    const root = asRecord(value);
    if (!root) return null;

    const schemaVersion = integerValue(root.schema_version);
    const sequence = integerValue(root.sequence);
    const nowUnixSecs = integerValue(root.now_unix_secs);
    const currentEpoch = root.current_epoch === null ? null : integerValue(root.current_epoch);
    const capabilities = asRecord(root.capabilities);
    const scheduler = asRecord(root.scheduler);
    const inventory = root.inventory === null ? null : asRecord(root.inventory);
    const scanSweep = root.scan_sweep === null ? null : asRecord(root.scan_sweep);
    const summary = asRecord(root.summary);
    const machine = asRecord(root.machine);
    const changed = Array.isArray(root.epochs_changed) ? root.epochs_changed : null;
    const removed = Array.isArray(root.epochs_removed) ? root.epochs_removed : null;
    const lanes = Array.isArray(root.lanes) ? root.lanes : null;
    const live = Array.isArray(root.live) ? root.live : null;
    const finalizerQueue = Array.isArray(root.finalizer_queue) ? root.finalizer_queue : null;
    const errors = Array.isArray(root.errors) ? root.errors : null;

    if (
      schemaVersion === null ||
      sequence === null ||
      nowUnixSecs === null ||
      !('current_epoch' in root) ||
      (root.current_epoch !== null && currentEpoch === null) ||
      typeof root.observer_mode !== 'boolean' ||
      !capabilities ||
      typeof capabilities.control_enabled !== 'boolean' ||
      typeof capabilities.authenticated_controls_required !== 'boolean' ||
      typeof capabilities.can_pause_scheduler !== 'boolean' ||
      typeof capabilities.can_retry_failed !== 'boolean' ||
      !scheduler ||
      typeof scheduler.paused !== 'boolean' ||
      !('inventory' in root) ||
      (root.inventory !== null && !inventory) ||
      !('scan_sweep' in root) ||
      (root.scan_sweep !== null && !scanSweep) ||
      !summary ||
      !machine ||
      !changed ||
      !changed.every(isEpochStatusValue) ||
      !removed ||
      !removed.every((epoch) => integerValue(epoch) !== null) ||
      !lanes ||
      !lanes.every(isLaneStatusValue) ||
      !live ||
      !live.every(isLiveStatusValue) ||
      !finalizerQueue ||
      !finalizerQueue.every(isFinalizerItemValue) ||
      !errors ||
      !errors.every(isPipelineErrorValue)
    ) {
      return null;
    }

    const changedEpochs = changed.map((epoch) => (epoch as EpochStatus).epoch);
    const removedEpochs = removed as number[];
    const uniqueChanged = new Set(changedEpochs);
    const uniqueRemoved = new Set(removedEpochs);
    if (
      uniqueChanged.size !== changedEpochs.length ||
      uniqueRemoved.size !== removedEpochs.length ||
      changedEpochs.some((epoch) => uniqueRemoved.has(epoch))
    ) {
      return null;
    }

    return {
      schema_version: schemaVersion,
      sequence,
      now_unix_secs: nowUnixSecs,
      current_epoch: currentEpoch,
      observer_mode: root.observer_mode,
      capabilities: capabilities as CapabilitySnapshot,
      scheduler: scheduler as SchedulerSnapshot,
      inventory: inventory as SchedulerStageSnapshot | null,
      scan_sweep: scanSweep as SchedulerStageSnapshot | null,
      summary: summary as PipelineSummary,
      machine: machine as MachineStatus,
      epochs_changed: changed as EpochStatus[],
      epochs_removed: removedEpochs,
      lanes: lanes as LaneStatus[],
      live: live as LiveStatus[],
      finalizer_queue: finalizerQueue as FinalizerItem[],
      errors: errors as PipelineError[]
    };
  }

  function isEpochStatusValue(value: unknown) {
    const epoch = asRecord(value);
    return Boolean(
      epoch &&
      integerValue(epoch.epoch) !== null &&
      typeof epoch.state === 'string' &&
      typeof epoch.registry_order === 'string' &&
      (epoch.input_path === null || typeof epoch.input_path === 'string') &&
      typeof epoch.output_path === 'string' &&
      numberValue(epoch.car_bytes) !== null &&
      asRecord(epoch.progress) &&
      integerValue(epoch.updated_unix_secs) !== null &&
      (epoch.message === null || typeof epoch.message === 'string') &&
      (epoch.artifacts === undefined || Array.isArray(epoch.artifacts))
    );
  }

  function isLaneStatusValue(value: unknown) {
    const lane = asRecord(value);
    return Boolean(
      lane &&
      typeof lane.id === 'string' &&
      typeof lane.kind === 'string' &&
      typeof lane.state === 'string' &&
      asRecord(lane.progress)
    );
  }

  function isLiveStatusValue(value: unknown) {
    const capture = asRecord(value);
    return Boolean(
      capture &&
      typeof capture.id === 'string' &&
      typeof capture.state === 'string' &&
      (capture.epoch === null || integerValue(capture.epoch) !== null) &&
      asRecord(capture.progress)
    );
  }

  function isFinalizerItemValue(value: unknown) {
    const item = asRecord(value);
    return Boolean(
      item &&
      typeof item.id === 'string' &&
      typeof item.kind === 'string' &&
      typeof item.state === 'string' &&
      (item.epoch === null || integerValue(item.epoch) !== null)
    );
  }

  function isPipelineErrorValue(value: unknown) {
    const error = asRecord(value);
    return Boolean(
      error &&
      integerValue(error.at_unix_secs) !== null &&
      typeof error.scope === 'string' &&
      typeof error.message === 'string'
    );
  }

  function canonicalLiveCaptures(captures: LiveStatus[], currentEpoch: number | null) {
    const byEpoch = new Map<string, LiveStatus>();
    for (const capture of captures) {
      const key = capture.epoch === null ? `capture:${capture.id}` : `epoch:${capture.epoch}`;
      const existing = byEpoch.get(key);
      if (!existing || compareLiveCapturePriority(capture, existing, currentEpoch) > 0) {
        byEpoch.set(key, capture);
      }
    }
    return [...byEpoch.values()].sort(compareLiveCapturesNewestFirst);
  }

  function compareLiveCapturePriority(left: LiveStatus, right: LiveStatus, currentEpoch: number | null) {
    const rank = (capture: LiveStatus) => {
      const stateRank: Record<LiveState, number> = {
        capturing: 90,
        packaging: 80,
        repair_required: 75,
        complete: 70,
        ready_to_package: 60,
        repair_gate: 50,
        packaged: 40,
        blocked: 30,
        failed: 20
      };
      return (capture.is_current ? 10_000 : 0) +
        (capture.epoch === currentEpoch && capture.state === 'capturing' ? 1_000 : 0) +
        stateRank[capture.state];
    };
    return rank(left) - rank(right) || left.updated_unix_secs - right.updated_unix_secs || left.id.localeCompare(right.id);
  }

  function compareLiveCapturesNewestFirst(left: LiveStatus, right: LiveStatus) {
    return (right.epoch ?? -1) - (left.epoch ?? -1) || right.updated_unix_secs - left.updated_unix_secs || right.id.localeCompare(left.id);
  }

  function buildEpochMap(epochs: EpochStatus[], captures: LiveStatus[]): EpochMapEntry[] {
    const entries = new Map<number, EpochMapEntry>();
    for (const epoch of epochs) {
      entries.set(epoch.epoch, { epoch: epoch.epoch, kind: 'historical', status: epoch });
    }
    for (const capture of captures) {
      if (capture.epoch === null) continue;
      const historical = entries.get(capture.epoch);
      if (capture.state === 'complete' && historical?.kind === 'historical' && historical.status.state === 'complete') {
        continue;
      }
      entries.set(capture.epoch, { epoch: capture.epoch, kind: 'live', status: capture });
    }
    return [...entries.values()].sort((left, right) => left.epoch - right.epoch);
  }

  function snapshotContainsEpoch(value: PipelineSnapshot, epoch: number) {
    return value.epochs.some((item) => item.epoch === epoch) || value.live.some((capture) => capture.epoch === epoch);
  }

  function epochMapVisualState(entry: EpochMapEntry) {
    return entry.kind === 'historical' ? historicalVisualState(entry.status) : liveVisualState(entry.status);
  }

  function epochMapStateLabel(entry: EpochMapEntry) {
    return entry.kind === 'historical' ? historicalStateLabel(entry.status) : liveStateLabel(entry.status);
  }

  function epochMapProgress(entry: EpochMapEntry) {
    return entry.kind === 'historical' ? entry.status.progress.progress_pct : liveProgress(entry.status);
  }

  function epochMapMessage(entry: EpochMapEntry) {
    return entry.status.message;
  }

  function epochMapTooltip(entry: EpochMapEntry) {
    const progress = epochMapProgress(entry);
    const parts = [
      `Epoch ${entry.epoch}`,
      epochMapStateLabel(entry),
      progress === null ? null : `${formatDecimal(progress)}%`,
      epochMapMessage(entry)
    ];
    return parts.filter((part): part is string => Boolean(part)).join(' · ');
  }

  function epochArtifactVisualState(entry: EpochMapEntry, artifact: ArtifactStatus) {
    return entry.kind === 'historical'
      ? historicalArtifactVisualState(entry.status, artifact)
      : artifactVisualState(artifact);
  }

  function epochArtifactStateLabel(entry: EpochMapEntry, artifact: ArtifactStatus) {
    return entry.kind === 'historical'
      ? historicalArtifactStateLabel(entry.status, artifact)
      : humanize(artifact.state);
  }

  function epochArtifactRequirementLabel(entry: EpochMapEntry, artifact: ArtifactStatus) {
    if (entry.kind === 'historical') return historicalArtifactRequirementLabel(entry.status, artifact);
    if (artifact.required_now) return 'required now';
    return artifact.requirement ? humanize(artifact.requirement) : null;
  }

  function epochArtifactTooltip(entry: EpochMapEntry, artifact: ArtifactStatus) {
    return entry.kind === 'historical'
      ? historicalArtifactTooltip(entry.status, artifact)
      : artifactTooltip(artifact);
  }

  async function toggleEpochDetails(epoch: number) {
    epochTabStop = epoch;
    if (selectedEpoch === epoch) {
      selectedEpoch = null;
      selectionAnnouncement = `Epoch ${epoch} details closed.`;
      return;
    }
    selectedEpoch = epoch;
    selectionAnnouncement = `Showing epoch ${epoch} details.`;
    await tick();
    document.getElementById(`epoch-detail-${epoch}`)?.focus();
  }

  function handleEpochGridKeydown(event: KeyboardEvent, epoch: number) {
    if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'].includes(event.key)) return;
    const current = event.currentTarget;
    if (!(current instanceof HTMLButtonElement) || !(current.parentElement instanceof HTMLElement)) return;
    const cells = [...current.parentElement.querySelectorAll<HTMLButtonElement>('.epoch-cell')];
    const index = cells.indexOf(current);
    if (index < 0) return;

    const columnCount = getComputedStyle(current.parentElement).gridTemplateColumns
      .split(' ')
      .filter(Boolean).length || 1;
    const targetIndex = event.key === 'Home'
      ? 0
      : event.key === 'End'
        ? cells.length - 1
        : index + ({ ArrowLeft: -1, ArrowRight: 1, ArrowUp: -columnCount, ArrowDown: columnCount }[event.key] ?? 0);
    const target = cells[Math.max(0, Math.min(cells.length - 1, targetIndex))];
    if (!target || target === current) return;
    event.preventDefault();
    epochTabStop = Number(target.dataset.epoch);
    target.focus();
  }

  async function closeEpochDetails(restoreFocus = false) {
    const epoch = selectedEpoch;
    selectedEpoch = null;
    if (epoch !== null) selectionAnnouncement = `Epoch ${epoch} details closed.`;
    if (!restoreFocus || epoch === null) return;
    await tick();
    document.getElementById(`epoch-cell-${epoch}`)?.focus();
  }

  function handlePageKeydown(event: KeyboardEvent) {
    if (event.key !== 'Escape' || selectedEpoch === null || tokenDialog?.open) return;
    event.preventDefault();
    void closeEpochDetails(true);
  }

  function queueEtaSecs(summary: PipelineSummary | null | undefined) {
    if (!summary) return null;
    if ('queue_eta_secs' in summary) return summary.queue_eta_secs ?? null;
    return summary.scan_eta_secs ?? summary.eta_secs ?? null;
  }

  function queueEtaReason(summary: PipelineSummary | null | undefined) {
    if (!summary) return 'Waiting for the first pipeline snapshot.';
    if ('queue_eta_secs' in summary) {
      if (summary.queue_eta_reason) return summary.queue_eta_reason;
      if (summary.queue_eta_secs === null || summary.queue_eta_secs === undefined) {
        return 'The runnable queue is learning a stable aggregate CAR-source read rate.';
      }
      return 'Remaining runnable CAR bytes divided by aggregate CAR-source read speed; worker count is not used.';
    }
    if (summary.scan_eta_secs !== null && summary.scan_eta_secs !== undefined) {
      return 'Legacy fallback based on the remaining scan queue.';
    }
    if (summary.eta_secs !== null && summary.eta_secs !== undefined) {
      return 'Legacy fallback based on the service queue estimate.';
    }
    return 'The connected service does not expose a runnable-queue estimate.';
  }

  function queueEtaExplanation(reason: string, needsAction: number) {
    const exclusion = needsAction > 0
      ? `${needsAction} action-required ${needsAction === 1 ? 'item is' : 'items are'} excluded from this ETA.`
      : 'Action-required items are excluded from this ETA.';
    return `${reason} ${exclusion}`;
  }

  function historicalVisualState(epoch: EpochStatus): VisualState {
    if (epoch.state === 'complete') {
      if (epoch.registry_order === 'first_seen') return 'first-seen-complete';
      return legacyNoAccessCompletionMessage(epoch) ? 'legacy-complete' : 'complete';
    }
    if (epoch.state === 'scanning') return 'active';
    if (epoch.state === 'scan_ready') return 'ready';
    if (epoch.state === 'finalizing') return 'finalizing';
    if (epoch.state === 'queued') return 'queued';
    if (epoch.state === 'failed') return 'failed';
    if (epoch.state === 'blocked' && epochHasMissingSource(epoch)) return 'missing';
    return 'attention';
  }

  function liveVisualState(capture: LiveStatus): VisualState {
    if (capture.state === 'blocked') return 'attention';
    if (capture.state === 'capturing') return capture.progress.pid === null ? 'queued' : 'active';
    if (capture.state === 'repair_required') return 'attention';
    if (capture.repair_gate || capture.state === 'repair_gate') return 'queued';
    if (capture.state === 'ready_to_package') return 'ready';
    if (capture.state === 'packaging') return 'finalizing';
    if (capture.state === 'packaged') return 'partial';
    if (capture.state === 'complete') return 'complete';
    return 'failed';
  }

  function liveStateLabel(capture: LiveStatus) {
    const labels: Record<LiveState, string> = {
      capturing: capture.progress.pid === null
        ? 'waiting for producer'
        : capture.is_current ? 'live indexing' : 'indexing',
      repair_gate: 'waiting for compact',
      repair_required: 'repair required',
      ready_to_package: 'queued for compact',
      packaging: 'compacting',
      packaged: 'compact; verification pending',
      complete: 'archive complete',
      blocked: isBenignLiveDiagnostic(capture) ? 'retained diagnostic' : 'action required',
      failed: 'compaction failed'
    };
    return labels[capture.state];
  }

  function liveNextStep(capture: LiveStatus) {
    if (capture.state === 'capturing') {
      return capture.progress.pid === null
        ? 'Waiting for the producer supervisor to reconnect'
        : 'Indexing until the epoch boundary';
    }
    if (capture.state === 'repair_gate') return 'Repair approval required before compaction';
    if (capture.state === 'repair_required') return 'Build degraded compact archive; attach missing PoH/shreds later';
    if (capture.state === 'ready_to_package') return 'Waiting for the exclusive compactor';
    if (capture.state === 'packaging') return 'Building the compact archive';
    if (capture.state === 'packaged') return 'Canonical repair and index sidecars are still pending';
    if (capture.state === 'complete') return 'Canonical archive is complete';
    if (capture.state === 'failed') return 'Safe retry is required';
    if (isBenignLiveDiagnostic(capture)) return 'Retained for recovery inspection only';
    return 'Resolve the reported blocker before packaging';
  }

  function liveEtaLabel(capture: LiveStatus) {
    return capture.state === 'capturing' ? 'Epoch boundary ETA' : 'Compaction ETA';
  }

  function liveEtaValue(capture: LiveStatus) {
    if (capture.state === 'capturing' && capture.progress.pid === null) return 'waiting';
    if (['capturing', 'packaging'].includes(capture.state) && !progressMetricsFresh(capture.progress)) return 'unknown';
    const status = liveEtaStatus(capture);
    if (status !== 'estimated') return status;
    return formatDuration(liveEtaSecs(capture));
  }

  function liveRateValue(capture: LiveStatus) {
    if (!['capturing', 'packaging'].includes(capture.state)) return 'not active';
    if (capture.state === 'capturing' && capture.progress.pid === null) return 'waiting';
    if (!progressMetricsFresh(capture.progress)) return 'unknown';
    const rate = liveRate(capture);
    if (rate === null) return 'unknown';
    if (rate === 0) return 'stalled';
    if (capture.state === 'capturing') return `${formatDecimal(rate, 2)} slots/s · 60s avg`;
    return `${formatDecimal(rate, 2)} blocks/s`;
  }

  function liveMemoryValue(capture: LiveStatus) {
    const current = liveRssBytes(capture);
    const peak = livePeakRssBytes(capture);
    if (current === null && peak === null) return 'unknown';
    if (current === null) return `${formatBytes(peak)} peak`;
    if (peak === null) return `${formatBytes(current)} RSS`;
    return `${formatBytes(current)} RSS · ${formatBytes(peak)} peak`;
  }

  function liveDiagnosticMessage(capture: LiveStatus) {
    if (capture.message) return capture.message;
    if (capture.state === 'failed') return 'Packaging failed; the source folder was retained for a safe retry.';
    if (capture.state === 'repair_required') return 'This repair bundle is still moving through the packaging workflow.';
    return 'This retained capture folder is reported for inspection and does not affect the historical queue ETA.';
  }

  function progressMetricsFresh(progress: ProgressSnapshot) {
    if (!snapshot || progress.updated_unix_secs === null) return false;
    return snapshot.now_unix_secs <= progress.updated_unix_secs + 120;
  }

  function laneMetricsFresh(lane: LaneStatus) {
    return normalizedState(lane.state) !== 'paused' &&
      normalizedState(lane.progress.state ?? '') !== 'paused' &&
      progressMetricsFresh(lane.progress);
  }

  function laneDiskReadRate(progress: ProgressSnapshot) {
    return numberValue(progress.disk_read_mib_per_sec);
  }

  function laneDiskWriteRate(progress: ProgressSnapshot) {
    return numberValue(progress.disk_write_mib_per_sec);
  }

  function laneDiskMetricsAvailable(lane: LaneStatus) {
    if (
      normalizedState(lane.state) === 'paused' ||
      normalizedState(lane.progress.state ?? '') === 'paused'
    ) return false;
    return numberValue(lane.progress.disk_read_mib_per_sec) !== null ||
      numberValue(lane.progress.disk_write_mib_per_sec) !== null;
  }

  function compactLaneInputRate(lane: LaneStatus) {
    if (!normalizedState(lane.kind).includes('compact')) return null;
    return numberValue(lane.progress.input_mib_per_sec);
  }

  function archiveDeviceLabel(machine: MachineStatus) {
    const name = machine.archive_device_name?.trim() || null;
    const major = integerValue(machine.archive_device_major);
    const minor = integerValue(machine.archive_device_minor);
    const deviceNumber = major !== null && minor !== null ? `${major}:${minor}` : null;
    return [name, deviceNumber].filter((value): value is string => value !== null).join(' · ') || 'resolving';
  }

  function diskRateAriaLabel(readRate: number | null, writeRate: number | null, fresh: boolean) {
    if (!fresh) return 'Storage I/O is not yet sampled or the worker is paused';
    const read = readRate === null ? 'read unavailable' : `read ${formatDecimal(readRate)} mebibytes per second`;
    const write = writeRate === null ? 'write unavailable' : `write ${formatDecimal(writeRate)} mebibytes per second`;
    return `${read}, ${write}`;
  }

  function epochHasMissingSource(epoch: EpochStatus) {
    const car = (epoch.artifacts ?? []).find((artifact) => artifactGroupId(artifact.kind) === 'car');
    if (car && ['missing', 'absent', 'not_found', 'unavailable'].includes(normalizedState(car.state))) {
      return true;
    }
    return epoch.message?.toLowerCase().includes('input car is missing') ?? false;
  }

  function groupArtifacts(artifacts: ArtifactStatus[]): ArtifactGroup[] {
    const groups: Partial<Record<ArtifactGroup['id'], ArtifactStatus[]>> = {};
    for (const artifact of artifacts) {
      const id = artifactGroupId(artifact.kind);
      const group = groups[id] ?? [];
      group.push(artifact);
      groups[id] = group;
    }
    return ARTIFACT_GROUP_ORDER
      .filter((id) => groups[id] !== undefined)
      .map((id) => ({
        id,
        label: ARTIFACT_GROUP_LABELS[id],
        artifacts: [...(groups[id] ?? [])].sort((left, right) => artifactLabel(left.kind).localeCompare(artifactLabel(right.kind)))
      }));
  }

  function artifactGroupId(kind: string): ArtifactGroup['id'] {
    const normalized = normalizedState(kind);
    if (/(preflight|checksum|verify|verification|receipt)/.test(normalized)) return 'preflight';
    if (['source_poh_info', 'source_shredding_info'].includes(normalized)) return 'source';
    if (normalized === 'car' || normalized.endsWith('_car') || normalized.startsWith('car_')) return 'car';
    return 'archive';
  }

  function artifactVisualState(artifact: ArtifactStatus): VisualState {
    const state = normalizedState(artifact.state);
    if (state === 'not_applicable') return 'na';
    if (['failed', 'error', 'invalid', 'corrupt', 'checksum_mismatch'].includes(state)) return 'failed';
    if (['blocked', 'repair', 'repair_gate', 'stale'].includes(state)) return 'attention';
    if (['missing', 'absent', 'not_found', 'unavailable'].includes(state)) return 'missing';
    if (['downloading', 'verifying', 'extracting', 'building', 'scanning', 'running', 'working'].includes(state)) return 'active';
    if (['queued', 'pending', 'waiting', 'unknown'].includes(state)) return 'queued';
    if (['complete', 'completed', 'published'].includes(state)) return 'complete';
    if (['candidate', 'present', 'ready', 'verified', 'valid', 'available'].includes(state)) return 'ready';
    return 'partial';
  }

  function historicalArtifactVisualState(epoch: EpochStatus, artifact: ArtifactStatus): VisualState {
    if (isLegacyNoAccessArtifact(epoch, artifact) || isRetiredSourceArtifact(epoch, artifact)) return 'na';
    return artifactVisualState(artifact);
  }

  function isRetiredSourceArtifact(epoch: EpochStatus, artifact: ArtifactStatus) {
    if (epoch.state !== 'complete' || artifact.required_now) return false;
    const group = artifactGroupId(artifact.kind);
    const state = normalizedState(artifact.state);
    if (group === 'car') {
      return ['missing', 'absent', 'not_found', 'unavailable', 'not_applicable'].includes(state);
    }
    return epochHasRetiredSource(epoch) && ['preflight', 'source'].includes(group) &&
      ['missing', 'absent', 'not_found', 'unavailable', 'not_applicable', 'pending'].includes(state);
  }

  function epochHasRetiredSource(epoch: EpochStatus) {
    if (epoch.state !== 'complete') return false;
    return (epoch.artifacts ?? []).some((artifact) => {
      const state = normalizedState(artifact.state);
      return artifactGroupId(artifact.kind) === 'car' &&
        !artifact.required_now &&
        ['missing', 'absent', 'not_found', 'unavailable', 'not_applicable'].includes(state);
    });
  }

  function isLegacyNoAccessArtifact(epoch: EpochStatus, artifact: ArtifactStatus) {
    return epoch.state === 'complete' && isLegacyNoAccessMessage(artifact.message);
  }

  function isLegacyNoAccessMessage(message: string | null | undefined) {
    if (!message) return false;
    const normalized = message.toLowerCase();
    return normalized.includes('legacy') && (
      normalized.includes('no-access') ||
      normalized.includes('no access') ||
      normalized.includes('block-access') ||
      normalized.includes('block access')
    );
  }

  function legacyNoAccessCompletionMessage(epoch: EpochStatus) {
    if (epoch.state !== 'complete') return null;
    if (isLegacyNoAccessMessage(epoch.message)) return epoch.message;
    return (epoch.artifacts ?? []).find((artifact) => isLegacyNoAccessArtifact(epoch, artifact))?.message ?? null;
  }

  function registryOrderContext(epoch: EpochStatus) {
    if (epoch.state !== 'complete') return null;
    if (epoch.registry_order === 'first_seen') {
      return 'First-seen IDs are not usage-sorted; this complete archive can be re-compacted.';
    }
    if (epoch.registry_order === 'usage_sorted') {
      return 'Registry IDs are usage-sorted; no registry recompact is needed.';
    }
    return 'Registry ordering is not reported for this archive.';
  }

  function historicalStateLabel(epoch: EpochStatus) {
    const tone = historicalVisualState(epoch);
    if (tone === 'first-seen-complete') return 'complete · recompactable';
    if (tone === 'legacy-complete') return 'legacy complete';
    if (tone === 'missing') return 'source missing';
    if (epoch.state === 'blocked') return 'needs action';
    return humanize(epoch.state);
  }

  function historicalArtifactStateLabel(epoch: EpochStatus, artifact: ArtifactStatus) {
    if (isLegacyNoAccessArtifact(epoch, artifact)) return 'legacy no-access';
    if (isRetiredSourceArtifact(epoch, artifact)) return 'source retired';
    return humanize(artifact.state);
  }

  function historicalArtifactRequirementLabel(epoch: EpochStatus, artifact: ArtifactStatus) {
    if (isLegacyNoAccessArtifact(epoch, artifact)) return 'archive complete';
    if (isRetiredSourceArtifact(epoch, artifact)) return 'not required';
    if (artifact.required_now) return 'required now';
    return artifact.requirement ? humanize(artifact.requirement) : null;
  }

  function artifactSatisfied(artifact: ArtifactStatus) {
    return ['candidate', 'present', 'complete', 'completed', 'published', 'ready', 'verified', 'valid', 'available'].includes(
      normalizedState(artifact.state)
    );
  }

  function artifactLabel(kind: string) {
    const normalized = normalizedState(kind);
    const known: Record<string, string> = {
      car: 'CAR',
      car_preflight: 'CAR preflight',
      poh: 'PoH',
      source_poh: 'Source PoH',
      source_poh_info: 'Source PoH info',
      shred: 'Shred',
      shredding: 'Shredding',
      source_shred: 'Source shred',
      source_shredding: 'Source shredding',
      source_shredding_info: 'Source shredding info',
      registry_mphf: 'Registry MPHF',
      registry_index: 'Registry MPHF',
      blockhash_registry: 'Blockhash registry',
      block_index: 'Block index',
      registry_counts: 'Registry counts',
      first_seen_manifest: 'First-seen manifest',
      block_access: 'Block access',
      block_access_index: 'Block access index',
      vote_hash_registry: 'Vote-hash registry'
    };
    return known[normalized] ?? humanize(kind);
  }

  function artifactTooltip(artifact: ArtifactStatus) {
    const parts = [
      `${artifactLabel(artifact.kind)}: ${humanize(artifact.state)}`,
      artifact.required_now ? 'required now' : humanize(artifact.requirement),
      artifact.bytes > 0 ? formatBytes(artifact.bytes) : null,
      artifact.modified_unix_secs ? `updated ${formatClock(artifact.modified_unix_secs)}` : null,
      artifact.message
    ];
    return parts.filter((part): part is string => Boolean(part)).join(' · ');
  }

  function historicalArtifactTooltip(epoch: EpochStatus, artifact: ArtifactStatus) {
    const parts = [
      `${artifactLabel(artifact.kind)}: ${historicalArtifactStateLabel(epoch, artifact)}`,
      historicalArtifactRequirementLabel(epoch, artifact),
      artifact.bytes > 0 ? formatBytes(artifact.bytes) : null,
      artifact.modified_unix_secs ? `updated ${formatClock(artifact.modified_unix_secs)}` : null,
      artifact.message
    ];
    return parts.filter((part): part is string => Boolean(part)).join(' · ');
  }

  function artifactSummary(artifacts: ArtifactStatus[]) {
    const applicable = artifacts.filter((artifact) => normalizedState(artifact.state) !== 'not_applicable');
    const available = applicable.filter(artifactSatisfied).length;
    const requiredIssues = artifacts.filter((artifact) => artifactNeedsAttention(artifact)).length;
    return `${formatInteger(available)} / ${formatInteger(applicable.length)} available${
      requiredIssues > 0 ? ` · ${formatInteger(requiredIssues)} required issue${requiredIssues === 1 ? '' : 's'}` : ''
    }`;
  }

  function artifactNeedsAttention(artifact: ArtifactStatus) {
    return artifact.required_now && ['missing', 'attention', 'failed'].includes(artifactVisualState(artifact));
  }

  function liveArtifactsOpen(artifacts: ArtifactStatus[]) {
    return artifacts.some(artifactNeedsAttention);
  }

  function taskLabel(kind: string) {
    const labels: Record<string, string> = {
      historical_scan: 'Historical scan',
      historical_finalizer: 'Historical finalizer',
      live_finalizer: 'Live finalizer',
      car_download: 'CAR download',
      car_preflight: 'CAR preflight',
      car_verify: 'CAR verification',
      car_extract: 'PoH + shred extraction'
    };
    return labels[kind] ?? humanize(kind);
  }

  function taskStateIcon(state: string) {
    const normalized = normalizedState(state);
    if (normalized === 'paused') return 'Ⅱ';
    if (['failed', 'error'].includes(normalized)) return '×';
    if (['done', 'complete', 'completed'].includes(normalized)) return '✓';
    if (['ready', 'scan_ready', 'ready_to_package'].includes(normalized)) return '◆';
    if (['queued', 'waiting', 'pending'].includes(normalized)) return '○';
    return '▶';
  }

  function humanize(value: string | null | undefined) {
    if (!value) return '—';
    return value.replaceAll('-', ' ').replaceAll('_', ' ');
  }

  function normalizedState(value: string) {
    return value.trim().toLowerCase().replaceAll('-', '_').replaceAll(' ', '_');
  }

  function liveProgress(capture: LiveStatus) {
    if (capture.progress.progress_pct !== null) return clampPercent(capture.progress.progress_pct);
    if (capture.epoch === null || capture.last_slot === null) return 0;
    const epochStart = capture.epoch * SLOTS_PER_EPOCH;
    return clampPercent(((capture.last_slot - epochStart + 1) * 100) / SLOTS_PER_EPOCH);
  }

  function epochStartSlot(epoch: number | null) {
    return epoch === null ? null : epoch * SLOTS_PER_EPOCH;
  }

  function epochEndSlot(epoch: number | null) {
    const start = epochStartSlot(epoch);
    return start === null ? null : start + SLOTS_PER_EPOCH - 1;
  }

  function laneControlId(lane: LaneStatus) {
    if (lane.kind === 'live_finalizer') return lane.capture_id;
    return lane.epoch === null ? null : String(lane.epoch);
  }

  function laneControlUnavailableReason() {
    return 'This task has no controllable job id.';
  }

  function openTokenDialog() {
    tokenDraft = window.sessionStorage.getItem(TOKEN_STORAGE_KEY) ?? '';
    tokenDialog?.showModal();
  }

  function saveToken() {
    const token = tokenDraft.trim();
    if (token) {
      window.sessionStorage.setItem(TOKEN_STORAGE_KEY, token);
      hasToken = true;
    } else {
      window.sessionStorage.removeItem(TOKEN_STORAGE_KEY);
      hasToken = false;
    }
    tokenDialog?.close();
  }

  function clearToken() {
    tokenDraft = '';
    window.sessionStorage.removeItem(TOKEN_STORAGE_KEY);
    hasToken = false;
  }

  async function postControl(path: string, actionKey: string) {
    if (controlsDisabledReason || actionBusy) return;
    actionBusy = actionKey;
    actionNotice = null;
    try {
      const headers = new Headers({ accept: 'application/json' });
      const token = window.sessionStorage.getItem(TOKEN_STORAGE_KEY);
      if (token) headers.set('authorization', `Bearer ${token}`);
      const response = await fetch(path, { method: 'POST', headers });
      const body = (await response.json().catch(() => null)) as { message?: string } | null;
      if (!response.ok) throw new Error(body?.message ?? `request failed with status ${response.status}`);
      actionNotice = body?.message ?? 'Control request accepted.';
    } catch (error) {
      actionNotice = `Control failed: ${errorMessage(error)}`;
    } finally {
      actionBusy = null;
    }
  }

  function toggleScheduler() {
    if (!snapshot) return;
    const action = snapshot.scheduler.paused ? 'resume' : 'pause';
    void postControl(`/api/v1/control/${action}`, `scheduler-${action}`);
  }

  function toggleLane(lane: LaneStatus) {
    const id = laneControlId(lane);
    if (!id) return;
    const action = lane.state === 'paused' ? 'resume' : 'pause';
    void postControl(
      `/api/v1/jobs/${encodeURIComponent(lane.kind)}/${encodeURIComponent(id)}/${action}`,
      `${lane.id}-${action}`
    );
  }

  function failedEpochRetryKind(epoch: EpochStatus) {
    const message = normalizedState(epoch.message ?? '');
    const artifacts = epoch.artifacts ?? [];
    const preflightInvalid = artifacts.some((artifact) =>
      ['car_preflight', 'source_poh_info', 'source_shredding_info'].includes(normalizedState(artifact.kind)) &&
      ['invalid', 'failed', 'error'].includes(normalizedState(artifact.state))
    );
    if (message.includes('preflight') || preflightInvalid) return 'car_preflight';

    const carUnavailable = artifacts.some((artifact) =>
      normalizedState(artifact.kind) === 'car' && artifact.required_now &&
      ['missing', 'pending', 'building', 'invalid', 'failed', 'error'].includes(normalizedState(artifact.state))
    );
    if (message.includes('download') || carUnavailable) return 'car_download';
    return 'historical_scan';
  }

  function failedEpochRetryLabel(epoch: EpochStatus) {
    const kind = failedEpochRetryKind(epoch);
    if (kind === 'car_download') return 'Retry download';
    if (kind === 'car_preflight') return 'Retry preflight';
    return 'Retry safely';
  }

  function failedEpochRetryTitle(epoch: EpochStatus) {
    const kind = failedEpochRetryKind(epoch);
    if (kind === 'car_download') return 'Clear the acquisition failure and resume the preserved partial download.';
    if (kind === 'car_preflight') return 'Clear the acquisition failure and rerun CAR structural preflight.';
    return 'Retry quarantines pipeline-owned partial output and preserves the source CAR.';
  }

  function failedEpochRetryHelp(epoch: EpochStatus) {
    const kind = failedEpochRetryKind(epoch);
    if (kind === 'car_download') return 'The resumable .part file and any valid CAR are preserved.';
    if (kind === 'car_preflight') return 'The CAR and resumable download state are preserved while preflight is rerun.';
    return 'The partial output is quarantined; the source CAR is preserved.';
  }

  function retryEpoch(epoch: EpochStatus) {
    const kind = failedEpochRetryKind(epoch);
    void postControl(`/api/v1/jobs/${kind}/${epoch.epoch}/retry`, `epoch-${epoch.epoch}-retry`);
  }

  function retryLive(capture: LiveStatus) {
    void postControl(
      `/api/v1/jobs/live_finalizer/${encodeURIComponent(capture.id)}/retry`,
      `live-${capture.id}-retry`
    );
  }

  function asRecord(value: unknown): JsonRecord | null {
    return typeof value === 'object' && value !== null && !Array.isArray(value) ? (value as JsonRecord) : null;
  }

  function numberValue(value: unknown): number | null {
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
  }

  function integerValue(value: unknown): number | null {
    return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null;
  }

  function percent(value: number | null | undefined, total: number | null | undefined) {
    if (value === null || value === undefined || total === null || total === undefined || total <= 0) return 0;
    return clampPercent((value * 100) / total);
  }

  function clampPercent(value: number) {
    return Math.max(0, Math.min(100, value));
  }

  function formatInteger(value: number | null | undefined) {
    return value === null || value === undefined ? '—' : Math.round(value).toLocaleString('en-US');
  }

  function formatDecimal(value: number | null | undefined, digits = 1) {
    return value === null || value === undefined ? '—' : value.toLocaleString('en-US', { maximumFractionDigits: digits });
  }

  function formatDuration(value: number | null | undefined) {
    if (value === null || value === undefined || !Number.isFinite(value)) return '—';
    let seconds = Math.max(0, Math.round(value));
    const days = Math.floor(seconds / 86_400);
    seconds %= 86_400;
    const hours = Math.floor(seconds / 3_600);
    seconds %= 3_600;
    const minutes = Math.floor(seconds / 60);
    seconds %= 60;
    if (days > 0) return `${days}d ${hours}h`;
    if (hours > 0) return `${hours}h ${minutes}m`;
    if (minutes > 0) return `${minutes}m ${seconds}s`;
    return `${seconds}s`;
  }

  function formatClock(value: number | null | undefined) {
    if (!value) return '—';
    return new Intl.DateTimeFormat(undefined, {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).format(new Date(value * 1000));
  }

  function errorMessage(error: unknown) {
    return error instanceof Error ? error.message : String(error);
  }
</script>

<svelte:window onkeydown={handlePageKeydown} />

<svelte:head>
  <title>Blockzilla NAS monitor</title>
  <meta
    name="description"
    content="Live status for Blockzilla historical compaction, live capture, and archive finalization."
  />
</svelte:head>

<div class="shell">
  <header class="topbar">
    <div class="identity">
      <h1>Blockzilla NAS</h1>
      <span class="scheduler" class:paused={snapshot?.observer_mode || snapshot?.scheduler.paused}>
        {snapshot?.observer_mode ? 'observer mode' : snapshot?.scheduler.paused ? 'scheduler paused' : 'scheduler active'}
      </span>
    </div>

    <div class="toolbar" aria-label="Pipeline controls">
      <button
        type="button"
        disabled={Boolean(controlsDisabledReason) || !snapshot?.capabilities.can_pause_scheduler || actionBusy !== null}
        title={controlsDisabledReason ?? (snapshot?.capabilities.can_pause_scheduler ? 'Pausing stops new admissions; active processes continue draining.' : 'Scheduler control is unavailable.')}
        onclick={toggleScheduler}
      >
        {actionBusy?.startsWith('scheduler-')
          ? 'Working…'
          : snapshot?.scheduler.paused
            ? 'Resume scheduler'
            : 'Pause scheduler'}
      </button>
      <button type="button" onclick={openTokenDialog}>{hasToken ? 'Access set' : 'Set access'}</button>
    </div>

    <div class="top-status">
      <span class="connection" title={connectionMessage} role="status" aria-live="polite" aria-atomic="true">
        <span class={`connection-mark connection-${connectionState}`} aria-hidden="true"></span>
        {connectionState} · event stream
      </span>
      <span
        class="queue-eta-status"
        title={runnableQueueEtaTitle}
      >
        Runnable queue ETA {formatDuration(runnableQueueEtaSecs)}
      </span>
      {#if historicalNeedsAction > 0}
        <strong class="danger" title="Action-required items are excluded from the runnable queue ETA.">
          {historicalNeedsAction} historical {historicalNeedsAction === 1 ? 'item' : 'items'} outside ETA
        </strong>
      {/if}
    </div>
  </header>

  {#if snapshot}
    <main>
      <section class="summary-line" aria-label="Compaction summary">
        <div class="summary-complete"><strong>{snapshot.summary.complete}</strong> / {snapshot.summary.epochs_total} historical epochs complete</div>
        <div class="summary-active"><strong>{activeEpochs}</strong> historical active</div>
        <div class="summary-queued"><strong>{snapshot.summary.queued}</strong> historical queued</div>
        <div
          class="summary-action"
          class:danger={historicalNeedsAction > 0}
          title={`${snapshot.summary.blocked} blocked · ${snapshot.summary.failed} failed`}
        >
          <strong>{historicalNeedsAction}</strong> {historicalNeedsAction === 1 ? 'needs' : 'need'} action
        </div>
        <div class="summary-io" aria-label="Archive storage throughput">
          <div
            class="summary-io-row summary-io-device-row"
            title={`Physical archive-volume device throughput from Linux /proc/diskstats for ${archiveDeviceLabel(snapshot.machine)}. This is the global archive-device rate.`}
          >
            <span class="summary-io-source">
              <span class="summary-io-heading">Archive device</span>
              <span class="summary-io-device">{archiveDeviceLabel(snapshot.machine)}</span>
            </span>
            <span><strong>{formatDecimal(snapshot.machine.archive_device_read_mib_per_sec)}</strong> R MiB/s</span>
            <span><strong>{formatDecimal(snapshot.machine.archive_device_write_mib_per_sec)}</strong> W MiB/s</span>
          </div>
          <div
            class="summary-io-row"
            title={`Aggregate Linux /proc process-tree-attributed I/O demand for active workers and the live capture producer, not archive-device throughput. ${snapshot.summary.disk_io_sampled_roots ?? 0}/${snapshot.summary.disk_io_active_roots ?? 0} active process roots have a complete rate sample. I/O PSI below remains the saturation signal.`}
          >
            <span class="summary-io-heading">Active processes</span>
            <span><strong>{formatDecimal(snapshot.summary.disk_read_mib_per_sec)}</strong> R MiB/s</span>
            <span><strong>{formatDecimal(snapshot.summary.disk_write_mib_per_sec)}</strong> W MiB/s</span>
          </div>
        </div>
        <div class="summary-progress">
          <span>{formatDecimal(historicalCompletionPct)}%</span>
          <progress aria-label="Historical archives complete" max="100" value={historicalCompletionPct}> {historicalCompletionPct}% </progress>
        </div>
      </section>

      {#if actionNotice}
        <div class="action-notice" role="status">{actionNotice}</div>
      {/if}

      {#if controlsDisabledReason}
        <div class="control-note">
          <span>{controlsDisabledReason}</span>
          {#if snapshot.capabilities.authenticated_controls_required && !hasToken}
            <button type="button" onclick={openTokenDialog}>Set access token</button>
          {/if}
        </div>
      {/if}

      {#if snapshot.summary.admission_blocked_reason}
        <div class="admission-note" role="status">
          <strong>Scan admission paused</strong>
          <span>{snapshot.summary.admission_blocked_reason}</span>
        </div>
      {/if}

      {#if snapshot.summary.finalizer_admission_blocked_reason}
        <div class="admission-note finalizer-admission-note" role="status">
          <strong>Finalizer deferred</strong>
          <span>{snapshot.summary.finalizer_admission_blocked_reason}</span>
        </div>
      {/if}

      {#if snapshot.scheduler.paused}
        <div class="admission-note" role="status">
          <strong>Scheduler paused</strong>
          <span>New work is not being admitted. Active processes continue draining unless paused individually.</span>
        </div>
      {/if}

      <section class="panel epoch-panel">
        <div class="section-heading">
          <div>
            <h2>Epoch archive status</h2>
            <p>
              {#if latestTrackedEpoch !== null}Through epoch {formatInteger(latestTrackedEpoch)}. {/if}
              Select an epoch for details; select it again or press Escape to close.
            </p>
          </div>
          <div class="legend" aria-label="Epoch status legend">
            {#each EPOCH_LEGEND as item (item.tone)}
              <span>
                <i class={`legend-swatch tone-${item.tone}`} aria-hidden="true">{VISUAL_META[item.tone].icon}</i>
                {item.label}
                <b>{epochToneCounts[item.tone]}</b>
              </span>
            {/each}
          </div>
        </div>

        {#if epochMap.length > 0}
          <div class="epoch-grid" aria-label="Epoch status map">
            {#each epochMap as entry (entry.epoch)}
              {@const tone = epochMapVisualState(entry)}
              <button
                id={`epoch-cell-${entry.epoch}`}
                type="button"
                class={`epoch-cell tone-${tone}`}
                class:selected={selectedEpoch === entry.epoch}
                aria-expanded={selectedEpoch === entry.epoch}
                aria-controls={`epoch-detail-${entry.epoch}`}
                aria-label={epochMapTooltip(entry)}
                title={epochMapTooltip(entry)}
                data-epoch={entry.epoch}
                tabindex={(epochTabStop ?? latestTrackedEpoch) === entry.epoch ? 0 : -1}
                onfocus={() => epochTabStop = entry.epoch}
                onkeydown={(event) => handleEpochGridKeydown(event, entry.epoch)}
                onclick={() => void toggleEpochDetails(entry.epoch)}
              >
                <span aria-hidden="true">{VISUAL_META[tone].icon}</span>
                <b>{entry.epoch}</b>
              </button>
            {/each}
          </div>

          <span class="visually-hidden" aria-live="polite">{selectionAnnouncement}</span>

          {#if selectedEpochEntry}
            {@const selectedEpochTone = epochMapVisualState(selectedEpochEntry)}
            <div
              id={`epoch-detail-${selectedEpochEntry.epoch}`}
              class="epoch-detail"
              role="region"
              tabindex="-1"
              aria-labelledby={`epoch-detail-title-${selectedEpochEntry.epoch}`}
            >
              <strong id={`epoch-detail-title-${selectedEpochEntry.epoch}`}>Epoch {selectedEpochEntry.epoch}</strong>
              <span class={`detail-status tone-${selectedEpochTone}`}>
                <span aria-hidden="true">{VISUAL_META[selectedEpochTone].icon}</span>
                {epochMapStateLabel(selectedEpochEntry)}
              </span>
              {#if selectedEpochStatus}
                {#if selectedEpochStatus.progress.progress_pct !== null}
                  <span>{formatDecimal(selectedEpochStatus.progress.progress_pct)}%</span>
                {/if}
                {#if selectedEpochStatus.progress.blocks_done > 0}
                  <span>{formatInteger(selectedEpochStatus.progress.blocks_done)} blocks processed</span>
                {/if}
                {#if ['scanning', 'finalizing'].includes(selectedEpochStatus.state) && progressMetricsFresh(selectedEpochStatus.progress)}
                  <span>Task ETA {formatDuration(selectedEpochStatus.progress.eta_secs)}</span>
                {/if}
                <span>{formatBytes(selectedEpochStatus.car_bytes)} CAR</span>
                {#if selectedEpochStatus.state === 'failed'}
                  <button
                    class="row-action"
                    type="button"
                    disabled={Boolean(controlsDisabledReason) || !snapshot.capabilities.can_retry_failed || actionBusy !== null}
                    title={controlsDisabledReason ?? failedEpochRetryTitle(selectedEpochStatus)}
                    onclick={() => retryEpoch(selectedEpochStatus)}
                  >
                    {actionBusy === `epoch-${selectedEpochStatus.epoch}-retry`
                      ? 'Retrying…'
                      : failedEpochRetryLabel(selectedEpochStatus)}
                  </button>
                  <span>{failedEpochRetryHelp(selectedEpochStatus)}</span>
                {/if}
              {:else if selectedLiveStatus}
                <span>{formatDecimal(liveProgress(selectedLiveStatus))}%</span>
                {#if selectedLiveStatus.state === 'capturing'}
                  <span>{formatInteger(selectedLiveStatus.blocks_written)} blocks indexed</span>
                {:else}
                  <span>Source blocks {formatInteger(selectedLiveStatus.blocks_written)}</span>
                  {#if selectedLiveStatus.progress.blocks_total > 0}
                    <span>
                      Processed {formatInteger(selectedLiveStatus.progress.blocks_done)} /
                      {formatInteger(selectedLiveStatus.progress.blocks_total)} blocks
                    </span>
                  {/if}
                {/if}
                <span>{liveRateValue(selectedLiveStatus)}</span>
                <span>{liveEtaLabel(selectedLiveStatus)} {liveEtaValue(selectedLiveStatus)}</span>
                <span>Memory {liveMemoryValue(selectedLiveStatus)}</span>
                <span>latest slot {formatInteger(selectedLiveStatus.last_slot)}</span>
                <span>{liveNextStep(selectedLiveStatus)}</span>
                {#if selectedLiveStatus.state === 'failed'}
                  <button
                    class="row-action"
                    type="button"
                    disabled={Boolean(controlsDisabledReason) || !snapshot.capabilities.can_retry_failed || actionBusy !== null}
                    title={controlsDisabledReason ?? 'Retry compaction without modifying the source capture.'}
                    onclick={() => retryLive(selectedLiveStatus)}
                  >
                    {actionBusy === `live-${selectedLiveStatus.id}-retry` ? 'Retrying…' : 'Retry compact'}
                  </button>
                {/if}
              {/if}
              <button
                class="epoch-detail-close"
                type="button"
                aria-label={`Close epoch ${selectedEpochEntry.epoch} details`}
                onclick={() => void closeEpochDetails(true)}
              >
                Close details
              </button>
              {#if epochMapMessage(selectedEpochEntry)}
                <span class="epoch-message">
                  <strong>{selectedEpochTone === 'missing' ? 'Missing source' : selectedEpochTone === 'attention' ? 'Why this needs action' : 'Status'}</strong>
                  {epochMapMessage(selectedEpochEntry)}
                </span>
              {/if}
            </div>
            {#if selectedEpochStatus && (selectedRegistryOrderContext || selectedSourceRetired || selectedLegacyNoAccessMessage)}
              <div class="archive-context" role="note">
                {#if selectedRegistryOrderContext}
                  <span>
                    <strong>Registry order</strong>
                    {selectedRegistryOrderContext}
                  </span>
                {/if}
                {#if selectedSourceRetired}
                  <span>
                    <strong>Source retired</strong>
                    Finalized archive retained; the source CAR was removed and is no longer required.
                  </span>
                {/if}
                {#if selectedLegacyNoAccessMessage}
                  <span>
                    <strong>Legacy no-access</strong>
                    {selectedLegacyNoAccessMessage}
                  </span>
                {/if}
              </div>
            {/if}
            {#if selectedArtifactGroups.length > 0}
              <div class="artifact-groups" aria-label={`Epoch ${selectedEpochEntry.epoch} artifact state`}>
                {#each selectedArtifactGroups as group (group.id)}
                  <section class="artifact-group">
                    <h3>{group.label}</h3>
                    <ul>
                      {#each group.artifacts as artifact (artifact.kind)}
                        {@const tone = epochArtifactVisualState(selectedEpochEntry, artifact)}
                        {@const requirementLabel = epochArtifactRequirementLabel(selectedEpochEntry, artifact)}
                        <li
                          class={`tone-${tone}`}
                          title={epochArtifactTooltip(selectedEpochEntry, artifact)}
                          aria-label={epochArtifactTooltip(selectedEpochEntry, artifact)}
                        >
                          <span class="artifact-icon" aria-hidden="true">{VISUAL_META[tone].icon}</span>
                          <strong>{artifactLabel(artifact.kind)}</strong>
                          <span class="artifact-state">{epochArtifactStateLabel(selectedEpochEntry, artifact)}</span>
                          {#if requirementLabel}
                            <span class="artifact-requirement">{requirementLabel}</span>
                          {/if}
                          <span class="artifact-bytes">{formatBytes(artifact.bytes)}</span>
                        </li>
                      {/each}
                    </ul>
                  </section>
                {/each}
              </div>
            {/if}
          {/if}
        {:else}
          <p class="empty">No epoch plan has been loaded.</p>
        {/if}
      </section>

      <section class="panel lanes-panel">
        <div class="section-heading">
          <div>
            <h2>Active tasks</h2>
            <p>{activeLanes.length} scanner, compactor, downloader, extractor, or finalizer processes are active.</p>
          </div>
        </div>

        {#if legacyCompactLanes.length > 0 || snapshot.summary.legacy_compact_auto_pause_enabled}
          <div class="worker-policy" aria-label="Legacy compaction worker policy">
            <div>
              <strong>Legacy compaction</strong>
              <span>{legacyCompactRunning} running · {legacyCompactPaused} paused{legacyCompactAutoPaused > 0 ? ` · ${legacyCompactAutoPaused} automatic` : ''}</span>
            </div>
            {#if snapshot.summary.legacy_compact_capacity_admitted !== undefined}
              <div>
                <strong>Resource envelope</strong>
                <span>
                  {#if snapshot.summary.legacy_compact_capacity_unbounded}
                    target {formatInteger(snapshot.summary.legacy_compact_capacity_effective)} lanes now ·
                    {formatInteger(snapshot.summary.legacy_compact_capacity_admitted)} admitted · no lane cap
                  {:else}
                    up to {formatInteger(snapshot.summary.legacy_compact_capacity_admitted)} lanes now ·
                    {formatInteger(snapshot.summary.legacy_compact_capacity_effective)} effective ·
                    {formatInteger(snapshot.summary.legacy_compact_capacity_configured)} configured
                  {/if}
                </span>
              </div>
            {/if}
            {#if snapshot.summary.legacy_compact_tuning_enabled}
              <div>
                <strong>Throughput tuner</strong>
                <span>
                  {(snapshot.summary.legacy_compact_tuning_state ?? 'observing').replaceAll('_', ' ')} ·
                  {formatInteger(snapshot.summary.legacy_compact_tuning_accepted_lanes)} accepted ·
                  {#if snapshot.summary.legacy_compact_tuning_rate_source}
                    {formatDecimal(snapshot.summary.legacy_compact_tuning_objective_mib_per_sec)}
                    {snapshot.summary.legacy_compact_tuning_rate_source === 'process_io' ? 'process I/O MiB/s' : 'logical input MiB/s'}
                  {:else}
                    metric pending
                  {/if}
                  ({formatInteger(snapshot.summary.legacy_compact_useful_input_sampled_lanes)}/{formatInteger(snapshot.summary.legacy_compact_useful_input_active_lanes)} lanes sampled)
                </span>
              </div>
              {#if snapshot.summary.legacy_compact_tuning_last_decision}
                <div>
                  <strong>Tuner decision</strong>
                  <span>{snapshot.summary.legacy_compact_tuning_last_decision}</span>
                </div>
              {/if}
            {/if}
            {#if snapshot.summary.legacy_compact_admission_blocked_reason}
              <div>
                <strong>Additional lane</strong>
                <span>{snapshot.summary.legacy_compact_admission_blocked_reason}</span>
              </div>
            {/if}
            {#if snapshot.summary.legacy_compact_auto_pause_enabled}
              <div>
                <strong>Adaptive pause enabled</strong>
                <span>
                  I/O full avg10 pauses at {formatDecimal(snapshot.summary.legacy_compact_io_pause_full_avg10, 2)}%,
                  resumes at {formatDecimal(snapshot.summary.legacy_compact_io_resume_full_avg10, 2)}%
                </span>
              </div>
              <div>
                <strong>CPU load guard</strong>
                <span>
                  current {formatDecimal(snapshot.machine.load_1m, 2)} ·
                  pause at {formatInteger(snapshot.summary.legacy_compact_cpu_budget_cores)} · resume below 85%
                </span>
              </div>
              <div>
                <strong>Memory hysteresis</strong>
                <span>
                  pause below {formatInteger(snapshot.summary.legacy_compact_memory_pause_available_mib)} MiB available,
                  resume at {formatInteger(snapshot.summary.legacy_compact_memory_resume_available_mib)} MiB
                </span>
              </div>
              <div>
                <strong>Policy</strong>
                <span>
                  bootstrap {formatInteger(snapshot.summary.legacy_compact_min_running)} lanes ·
                  {formatDuration(snapshot.summary.legacy_compact_pause_cooldown_secs)} cooldown
                </span>
              </div>
              {#if snapshot.summary.legacy_compact_last_action}
                <div class="worker-policy-action">
                  <strong>Last automatic action</strong>
                  <span>
                    {snapshot.summary.legacy_compact_last_action}
                    {#if snapshot.summary.legacy_compact_last_action_unix_secs}
                      · {formatClock(snapshot.summary.legacy_compact_last_action_unix_secs)}
                    {/if}
                  </span>
                </div>
              {/if}
            {:else}
              <div>
                <strong>Adaptive pause disabled</strong>
                <span>Workers use the explicit compatibility lane setting and normal task admission.</span>
              </div>
            {/if}
          </div>
        {/if}

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Task</th>
                <th>Epoch</th>
                <th>Phase</th>
                <th class="progress-column">Progress</th>
                <th title="Blocks emitted or processed by the worker. Slot coverage is shown by Progress.">Blocks processed</th>
                <th
                  class="io-rate-column"
                  title="Linux /proc process-tree-attributed storage I/O, not raw storage-device bus throughput. I/O PSI remains the saturation signal."
                >Process I/O (MiB/s)</th>
                <th class="eta-column">ETA</th>
                <th class="rss-column">RSS</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {#each activeLanes as lane (lane.id)}
                {@const metricsFresh = laneMetricsFresh(lane)}
                {@const diskMetricsAvailable = laneDiskMetricsAvailable(lane)}
                {@const diskReadRate = laneDiskReadRate(lane.progress)}
                {@const diskWriteRate = laneDiskWriteRate(lane.progress)}
                {@const logicalInputRate = compactLaneInputRate(lane)}
                <tr>
                  <td>
                    <div class="task-name">
                      <strong>{taskLabel(lane.kind)}</strong>
                      <span class="mono">{lane.id}</span>
                    </div>
                  </td>
                  <td>{formatInteger(lane.epoch)}</td>
                  <td>
                    <div class="task-phase-cell">
                      <span
                        class="task-phase"
                        class:task-paused={lane.state === 'paused'}
                        class:auto-paused={lane.auto_paused === true}
                        title={`${humanize(lane.state)} · ${humanize(lane.phase)}${lane.auto_pause_reason ? ` · ${lane.auto_pause_reason}` : ''}`}
                      >
                        <span aria-hidden="true">{taskStateIcon(lane.state)}</span>
                        {humanize(lane.phase)}
                      </span>
                      {#if lane.state === 'paused'}
                        <span class="pause-detail" title={lane.auto_pause_reason ?? undefined}>
                          {lane.auto_paused ? 'auto-paused' : 'manually paused'}{lane.auto_pause_reason ? ` · ${lane.auto_pause_reason}` : ''}
                        </span>
                      {/if}
                    </div>
                  </td>
                  <td class="progress-column">
                    <div class="inline-progress">
                      <progress max="100" value={lane.progress.progress_pct ?? 0}>{lane.progress.progress_pct ?? 0}%</progress>
                      <span>{formatDecimal(lane.progress.progress_pct)}%</span>
                    </div>
                  </td>
                  <td>{formatInteger(lane.progress.blocks_done)}</td>
                  <td
                    class="io-rate-column"
                    title={diskMetricsAvailable
                      ? 'Linux /proc process-tree-attributed storage I/O.'
                      : 'Storage I/O is waiting for a complete process-counter sample, or this task is paused.'}
                  >
                    <span
                      class="io-rate-pair"
                      aria-label={diskRateAriaLabel(diskReadRate, diskWriteRate, diskMetricsAvailable)}
                    >
                      <span aria-hidden="true">R {diskMetricsAvailable ? formatDecimal(diskReadRate) : '—'}</span>
                      <span aria-hidden="true">W {diskMetricsAvailable ? formatDecimal(diskWriteRate) : '—'}</span>
                    </span>
                    <span class="logical-input-rate" aria-hidden={logicalInputRate === null}>
                      {logicalInputRate === null ? '' : `Logical input ${formatDecimal(logicalInputRate)} MiB/s`}
                    </span>
                  </td>
                  <td class="eta-column" title={metricsFresh ? undefined : 'ETA hidden because this task is paused or its progress sample is stale.'}>{metricsFresh ? formatDuration(lane.progress.eta_secs) : '—'}</td>
                  <td class="rss-column">{formatBytes(lane.rss_bytes ?? lane.progress.rss_bytes)}</td>
                  <td>
                    <button
                      class="row-action"
                      type="button"
                      disabled={Boolean(controlsDisabledReason) || laneControlId(lane) === null || actionBusy !== null}
                      title={controlsDisabledReason ?? (laneControlId(lane) === null ? laneControlUnavailableReason() : `${lane.state === 'paused' ? 'Resume' : 'Pause'} this process safely.`)}
                      onclick={() => toggleLane(lane)}
                    >
                      {laneControlId(lane) === null
                        ? 'Unavailable'
                        : actionBusy === `${lane.id}-${lane.state === 'paused' ? 'resume' : 'pause'}`
                        ? 'Working…'
                        : lane.state === 'paused'
                          ? 'Resume'
                          : 'Pause'}
                    </button>
                  </td>
                </tr>
              {:else}
                <tr><td colspan="9" class="empty-cell">No task is active.</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>

      <div class="two-column" class:single-column={snapshot.finalizer_queue.length === 0}>
        <section class="panel live-panel">
          <div class="section-heading">
            <div>
              <h2>Live indexing and compaction</h2>
              <p>Current live capture status, plus closed epochs still moving into the compact archive.</p>
            </div>
            <span title={`${completedLiveCaptureCount} completed live capture${completedLiveCaptureCount === 1 ? '' : 's'} are represented in the epoch map.`}>
              {#if currentLiveCapture}
                Epoch {formatInteger(currentLiveCapture.epoch)}
                {currentLiveCapture.progress.pid === null ? 'waiting for producer' : 'indexing'}
              {:else}
                no live indexer
              {/if}
              {#if waitingLiveCaptureCount > 0}
                · {waitingLiveCaptureCount} in pipeline
              {/if}
              {#if liveNeedsActionCount > 0}
                · {liveNeedsActionCount} {liveNeedsActionCount === 1 ? 'needs' : 'need'} action
              {/if}
            </span>
          </div>

          {#each visibleLiveCaptures as capture (capture.id)}
            {@const liveArtifacts = capture.artifacts ?? []}
            {@const bundledSources = groupedLiveCaptures.sourcesByBundle.get(capture.id) ?? []}
            <div class="capture">
              <div class="live-progress">
                <div>
                  <span class="capture-title">
                    <strong>Epoch {formatInteger(capture.epoch)}</strong>
                    <span>{capture.id}</span>
                  </span>
                  <span class={`plain-status tone-${liveVisualState(capture)}`}>
                    <span aria-hidden="true">{VISUAL_META[liveVisualState(capture)].icon}</span>
                    {liveStateLabel(capture)}
                  </span>
                </div>
                <progress max="100" value={liveProgress(capture)}>{liveProgress(capture)}%</progress>
                <div class="slot-range">
                  <span>{formatInteger(epochStartSlot(capture.epoch))}</span>
                  <span>latest {formatInteger(capture.last_slot)}</span>
                  <span>{formatInteger(epochEndSlot(capture.epoch))}</span>
                </div>
              </div>

              <dl class="facts">
                <div>
                  <dt>{capture.state === 'capturing' ? 'Blocks indexed' : 'Source blocks'}</dt>
                  <dd>{formatInteger(capture.blocks_written)}</dd>
                </div>
                {#if capture.state !== 'capturing' && capture.progress.blocks_total > 0}
                  <div>
                    <dt>Processed</dt>
                    <dd>{formatInteger(capture.progress.blocks_done)} / {formatInteger(capture.progress.blocks_total)}</dd>
                  </div>
                {/if}
                <div><dt>First captured slot</dt><dd>{formatInteger(capture.first_slot)}</dd></div>
                <div><dt>{capture.state === 'capturing' ? 'Transactions this process' : 'Transactions'}</dt><dd>{formatInteger(capture.progress.transactions_done)}</dd></div>
                <div><dt>{capture.state === 'capturing' ? 'Index rate' : 'Processing rate'}</dt><dd>{liveRateValue(capture)}</dd></div>
                <div><dt>{liveEtaLabel(capture)}</dt><dd>{liveEtaValue(capture)}</dd></div>
                <div><dt>Memory</dt><dd title="Current resident memory and peak resident memory">{liveMemoryValue(capture)}</dd></div>
                <div><dt>Next step</dt><dd>{liveNextStep(capture)}</dd></div>
              </dl>

              {#if liveArtifacts.length > 0}
                <details class="live-artifacts" open={liveArtifactsOpen(liveArtifacts)}>
                  <summary>
                    <strong>Artifacts</strong>
                    <span>{artifactSummary(liveArtifacts)}</span>
                  </summary>
                  <div class="live-artifact-groups">
                    {#each groupArtifacts(liveArtifacts) as group (group.id)}
                      <section class="live-artifact-group">
                        <h3>{group.label}</h3>
                        <ul>
                          {#each group.artifacts as artifact (artifact.kind)}
                            {@const tone = artifactVisualState(artifact)}
                            <li
                              class={`tone-${tone}`}
                              title={artifactTooltip(artifact)}
                              aria-label={artifactTooltip(artifact)}
                            >
                              <span aria-hidden="true">{VISUAL_META[tone].icon}</span>
                              <strong>{artifactLabel(artifact.kind)}</strong>
                              <span>{humanize(artifact.state)}</span>
                              {#if artifact.required_now}<em>required</em>{/if}
                            </li>
                          {/each}
                        </ul>
                      </section>
                    {/each}
                  </div>
                </details>
              {/if}

              {#if (capture.source_capture_ids?.length ?? 0) > 0}
                <details class="live-artifacts">
                  <summary>
                    <strong>Source capture folders</strong>
                    <span>{capture.source_capture_ids?.length ?? 0} retained by this bundle</span>
                  </summary>
                  <div class="live-artifact-groups">
                    <section class="live-artifact-group">
                      <ul>
                        {#each bundledSources as source (source.id)}
                          <li class="tone-na" title={source.capture_dir}>
                            <span aria-hidden="true">·</span>
                            <strong>{source.id}</strong>
                            <span>{source.superseded_by === capture.id ? 'superseded by bundle' : liveStateLabel(source)}</span>
                            <em>epoch {formatInteger(source.epoch)}</em>
                          </li>
                        {/each}
                      </ul>
                    </section>
                  </div>
                </details>
              {/if}

              {#if (capture.repair_gate || capture.message) && capture.state !== 'packaged'}
                <div class="repair-gate" class:retained-diagnostic={isBenignLiveDiagnostic(capture)} role="status">
                  <strong>{capture.repair_gate ? 'Waiting for compact' : capture.state === 'repair_required' ? 'Repair required' : isBenignLiveDiagnostic(capture) ? 'Retained diagnostic' : capture.state === 'blocked' ? 'Action required' : 'Capture note'}</strong>
                  <span>{capture.message ?? 'Coverage repair must complete before this epoch can be packaged.'}</span>
                </div>
              {/if}

              {#if capture.state === 'packaged'}
                <div class="packaged-note" role="status">
                  <strong>Compact package exists</strong>
                  <span>Canonical repair and index sidecars are still pending. This output is not canonical complete.</span>
                </div>
              {/if}

              {#if capture.state === 'failed'}
                <div class="live-retry">
                  <button
                    class="row-action"
                    type="button"
                    disabled={Boolean(controlsDisabledReason) || !snapshot.capabilities.can_retry_failed || actionBusy !== null}
                    title={controlsDisabledReason ?? 'Retry quarantines pipeline-owned partial output and preserves the live capture.'}
                    onclick={() => retryLive(capture)}
                  >
                    {actionBusy === `live-${capture.id}-retry` ? 'Retrying…' : 'Retry packaging safely'}
                  </button>
                  <span>The partial output is quarantined; the source capture is preserved.</span>
                </div>
              {/if}

              <div class="path" title={capture.capture_dir}>{capture.capture_dir}</div>
            </div>
          {:else}
            <p class="empty">No current indexing or pending compaction was detected.</p>
          {/each}

          {#if hiddenLiveCaptureDiagnostics.length > 0}
            <div class="live-capture-diagnostics" role="note">
              <strong>Retained capture diagnostics</strong>
              <ul>
                {#each hiddenLiveCaptureDiagnostics as issue (issue.id)}
                  <li>
                    <span>Epoch {formatInteger(issue.epoch)} · {issue.id}</span>
                    <span>{liveDiagnosticMessage(issue)}</span>
                  </li>
                {/each}
              </ul>
            </div>
          {/if}
        </section>

        {#if snapshot.finalizer_queue.length > 0}
          <section class="panel queue-panel">
            <div class="section-heading">
              <div>
                <h2>Finalizer queue</h2>
                <p>Finalizer and live-compaction tasks share one serial lane; historical finalizers wait for scan work to drain.</p>
              </div>
              <span>{snapshot.finalizer_queue.length} waiting</span>
            </div>

            <ol class="queue">
              {#each snapshot.finalizer_queue as item, index (`${item.kind}:${item.id}`)}
                <li>
                  <span class="queue-position">{index + 1}</span>
                  <strong>{item.epoch === null ? item.id : `Epoch ${item.epoch}`}</strong>
                  <span>{humanize(item.kind)}</span>
                  <span
                    class="queue-phase"
                    class:queue-deferred={Boolean(item.deferred_reason)}
                    title={item.deferred_reason ?? `${humanize(item.state)} · ${humanize(item.phase)}`}
                  >
                    <b aria-hidden="true">{item.deferred_reason ? '!' : taskStateIcon(item.state)}</b>
                    <span class="queue-phase-copy">
                      <span>{humanize(item.phase ?? item.state)}</span>
                      <em>{item.deferred_reason ? `deferred · ${item.deferred_reason}` : humanize(item.state)}</em>
                    </span>
                  </span>
                  <span class="queue-eta" title="Estimated finalizer memory">
                    {item.estimated_memory_bytes === undefined ? '—' : formatBytes(item.estimated_memory_bytes)}
                  </span>
                </li>
              {/each}
            </ol>
          </section>
        {/if}
      </div>

      <div class="two-column lower-grid">
        <section class="panel machine-panel">
          <div class="section-heading">
            <div>
              <h2>NAS resources</h2>
              <p>Memory, volume headroom, and Linux pressure observed by the scheduler.</p>
            </div>
            {#if snapshot.machine.load_1m !== null}<span>load {formatDecimal(snapshot.machine.load_1m, 2)}</span>{/if}
          </div>

          <div class="resources">
            <div class="resource-row">
              <div><strong>Memory</strong><span>{formatBytes(snapshot.machine.memory_used_bytes)} / {formatBytes(snapshot.machine.memory_total_bytes)}</span></div>
              <progress max="100" value={machineMemoryPct}>{machineMemoryPct}%</progress>
              <span>{formatBytes(snapshot.machine.memory_available_bytes)} available</span>
            </div>
            {#if snapshot.machine.memory_pressure_full_avg10 !== null && snapshot.machine.memory_pressure_full_avg10 !== undefined}
              <div class="resource-row pressure-row">
                <div>
                  <strong>Memory pressure</strong>
                  <span>full avg10 {formatDecimal(snapshot.machine.memory_pressure_full_avg10, 2)}%</span>
                </div>
                <progress max="100" value={snapshot.machine.memory_pressure_full_avg10}>
                  {snapshot.machine.memory_pressure_full_avg10}%
                </progress>
                <span>some avg10 {formatDecimal(snapshot.machine.memory_pressure_some_avg10, 2)}%</span>
              </div>
            {/if}
            <div class="resource-row">
                <div><strong>{snapshot.machine.car_disk_total_bytes && !hasSeparateCarStorage ? 'Archive + CAR storage' : 'Archive storage'}</strong><span>{formatBytes(snapshot.machine.disk_used_bytes)} / {formatBytes(snapshot.machine.disk_total_bytes)}</span></div>
              <progress max="100" value={machineDiskPct}>{machineDiskPct}%</progress>
              <span>{formatBytes(snapshot.machine.disk_available_bytes)} available</span>
            </div>
            {#if snapshot.machine.io_pressure_full_avg10 !== null && snapshot.machine.io_pressure_full_avg10 !== undefined}
              <div class="resource-row pressure-row">
                <div>
                  <strong>I/O pressure</strong>
                  <span>full avg10 {formatDecimal(snapshot.machine.io_pressure_full_avg10, 2)}%</span>
                </div>
                <progress max="100" value={snapshot.machine.io_pressure_full_avg10}>
                  {snapshot.machine.io_pressure_full_avg10}%
                </progress>
                <span>
                  {#if snapshot.summary.legacy_compact_auto_pause_enabled}
                    pause at {formatDecimal(snapshot.summary.legacy_compact_io_pause_full_avg10, 2)}%
                  {:else}
                    some avg10 {formatDecimal(snapshot.machine.io_pressure_some_avg10, 2)}%
                  {/if}
                </span>
              </div>
            {/if}
            {#if hasSeparateCarStorage}
              <div class="resource-row">
                <div><strong>CAR storage</strong><span>{formatBytes(snapshot.machine.car_disk_used_bytes)} / {formatBytes(snapshot.machine.car_disk_total_bytes)}</span></div>
                <progress max="100" value={carDiskPct}>{carDiskPct}%</progress>
                <span>{formatBytes(snapshot.machine.car_disk_available_bytes)} available</span>
              </div>
            {/if}
            <div class="resource-row">
              <div><strong>Swap</strong><span>{formatBytes(snapshot.machine.swap_used_bytes)} / {formatBytes(snapshot.machine.swap_total_bytes)}</span></div>
              <progress max="100" value={machineSwapPct}>{machineSwapPct}%</progress>
              <span>{formatDecimal(machineSwapPct)}% used</span>
            </div>
          </div>
        </section>

        <section class="panel errors-panel">
          <div class="section-heading">
            <div>
              <h2>Recent error log</h2>
              <p>Recorded pipeline errors; resolved entries may remain in this bounded history.</p>
            </div>
            <span>{snapshot.errors.length}</span>
          </div>

          <ul class="errors">
            {#each snapshot.errors as error (`${error.at_unix_secs}-${error.scope}-${error.message}`)}
              <li>
                <div><strong>{error.scope}</strong><time datetime={new Date(error.at_unix_secs * 1000).toISOString()}>{formatClock(error.at_unix_secs)}</time></div>
                <p>{error.message}</p>
              </li>
            {:else}
              <li class="empty">No current pipeline errors.</li>
            {/each}
          </ul>
        </section>
      </div>
    </main>
  {:else}
    <main class="loading" aria-live="polite">
      <h2>Waiting for NAS status</h2>
      <p>{connectionMessage}</p>
      <p>API: <code>/api/v1/status</code></p>
    </main>
  {/if}
</div>

<dialog bind:this={tokenDialog} class="token-dialog" aria-labelledby="token-title">
  <form onsubmit={(event) => { event.preventDefault(); saveToken(); }}>
    <div class="dialog-heading">
      <h2 id="token-title">Control access</h2>
      <button type="button" class="dialog-close" aria-label="Close" onclick={() => tokenDialog?.close()}>×</button>
    </div>
    <p>The bearer token is kept only in this tab's session storage and is sent only with control requests.</p>
    <label for="control-token">Bearer token</label>
    <input id="control-token" type="password" autocomplete="off" bind:value={tokenDraft} />
    <div class="dialog-actions">
      <button type="button" onclick={clearToken}>Clear</button>
      <button type="submit" class="primary-action">Save for this tab</button>
    </div>
  </form>
</dialog>

<style>
  .shell {
    min-height: 100vh;
  }

  .topbar {
    min-height: 54px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 24px;
    padding: 0 24px;
    border-bottom: 1px solid var(--border);
    background: #141416;
  }

  .identity,
  .toolbar,
  .top-status,
  .connection,
  .summary-line,
  .section-heading,
  .legend,
  .epoch-detail,
  .inline-progress,
  .live-progress > div,
  .slot-range,
  .resource-row > div,
  .errors li > div {
    display: flex;
    align-items: center;
  }

  .identity {
    gap: 14px;
    flex: 0 0 auto;
  }

  h1,
  h2,
  p {
    margin: 0;
  }

  h1 {
    font-size: 15px;
    font-weight: 680;
    letter-spacing: -0.01em;
  }

  h2 {
    font-size: 14px;
    font-weight: 650;
  }

  .scheduler {
    color: var(--green);
    font-size: 12px;
  }

  .plain-status,
  .detail-status {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 1px 5px;
    border: 1px solid var(--tone-accent);
    border-radius: 3px;
    background: var(--tone-bg);
    color: #f4f4f5;
    font-size: 11px;
  }

  .scheduler.paused,
  .danger {
    color: var(--red) !important;
  }

  .top-status {
    justify-content: flex-end;
    gap: 16px;
    color: var(--muted);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .top-status strong {
    color: var(--text);
    font-weight: 560;
  }

  .top-status > * {
    white-space: nowrap;
  }

  .queue-eta-status {
    min-width: 148px;
  }

  .toolbar {
    gap: 6px;
    margin-left: auto;
  }

  .toolbar button,
  .row-action,
  .epoch-detail-close,
  .dialog-actions button,
  .dialog-close {
    min-height: 28px;
    padding: 0 9px;
    border: 1px solid var(--border-strong);
    border-radius: 5px;
    background: #202023;
    color: #d8d8dc;
    font-size: 11px;
    cursor: pointer;
  }

  .toolbar button:hover:not(:disabled),
  .row-action:hover:not(:disabled),
  .epoch-detail-close:hover:not(:disabled),
  .dialog-actions button:hover:not(:disabled),
  .dialog-close:hover:not(:disabled) {
    border-color: #62626a;
    background: #28282c;
  }

  .toolbar button:disabled,
  .row-action:disabled,
  .epoch-detail-close:disabled,
  .dialog-actions button:disabled {
    color: var(--faint);
    cursor: not-allowed;
    opacity: 0.7;
  }

  .connection {
    gap: 6px;
    color: var(--text);
  }

  .connection-mark {
    width: 7px;
    height: 7px;
    border-radius: 2px;
    background: var(--slate);
  }

  .connection-live {
    background: var(--green);
  }

  .connection-retrying,
  .connection-connecting {
    background: var(--amber);
  }

  .connection-offline {
    background: var(--red);
  }

  main {
    width: min(1600px, 100%);
    margin: 0 auto;
    padding: 18px 24px 32px;
  }

  .summary-line {
    min-height: 38px;
    flex-wrap: wrap;
    gap: 9px 22px;
    margin-bottom: 14px;
    color: var(--muted);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .summary-line strong {
    color: var(--text);
    font-size: 14px;
  }

  .summary-line > div {
    white-space: nowrap;
  }

  .summary-complete {
    min-width: 220px;
  }

  .summary-active {
    min-width: 110px;
  }

  .summary-queued {
    min-width: 135px;
  }

  .summary-action {
    min-width: 88px;
  }

  .summary-io {
    min-width: 436px;
    display: grid;
    gap: 2px;
  }

  .summary-io-row {
    display: grid;
    grid-template-columns: 188px 112px 112px;
    align-items: baseline;
    gap: 8px;
  }

  .summary-io-source {
    min-width: 0;
    display: flex;
    align-items: baseline;
    gap: 5px;
  }

  .summary-io-heading {
    color: var(--text);
  }

  .summary-io-device {
    min-width: 0;
    overflow: hidden;
    color: var(--faint);
    font-family: ui-monospace, "SFMono-Regular", Consolas, monospace;
    font-size: 10px;
    text-overflow: ellipsis;
  }

  .summary-io-row > span:not(.summary-io-source) {
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .summary-progress {
    min-width: 180px;
    display: grid;
    grid-template-columns: auto minmax(110px, 1fr);
    align-items: center;
    gap: 8px;
    margin-left: auto;
  }

  .visually-hidden {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  }

  progress {
    width: 100%;
    height: 6px;
    border: 0;
    border-radius: 2px;
    overflow: hidden;
    background: #29292d;
  }

  progress::-webkit-progress-bar {
    background: #29292d;
  }

  progress::-webkit-progress-value {
    background: var(--green);
  }

  progress::-moz-progress-bar {
    background: var(--green);
  }

  .panel {
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface);
  }

  .admission-note {
    display: flex;
    align-items: baseline;
    gap: 12px;
    margin: -2px 0 14px;
    padding: 8px 10px;
    border: 1px solid #775235;
    border-radius: 5px;
    background: #2b211a;
    color: #d7b78f;
    font-size: 12px;
  }

  .finalizer-admission-note {
    border-color: #67567e;
    background: #251f31;
    color: #d4c5ea;
  }

  .tone-complete {
    --tone-bg: var(--status-complete-bg);
    --tone-accent: var(--status-complete-accent);
  }

  .tone-first-seen-complete {
    --tone-bg: var(--status-first-seen-complete-bg);
    --tone-accent: var(--status-first-seen-complete-accent);
  }

  .tone-legacy-complete {
    --tone-bg: var(--status-legacy-complete-bg);
    --tone-accent: var(--status-legacy-complete-accent);
  }

  .tone-active {
    --tone-bg: var(--status-active-bg);
    --tone-accent: var(--status-active-accent);
  }

  .tone-ready {
    --tone-bg: var(--status-ready-bg);
    --tone-accent: var(--status-ready-accent);
  }

  .tone-finalizing {
    --tone-bg: var(--status-finalizing-bg);
    --tone-accent: var(--status-finalizing-accent);
  }

  .tone-partial {
    --tone-bg: var(--status-partial-bg);
    --tone-accent: var(--status-partial-accent);
  }

  .tone-queued {
    --tone-bg: var(--status-queued-bg);
    --tone-accent: var(--status-queued-accent);
  }

  .tone-missing {
    --tone-bg: var(--status-missing-bg);
    --tone-accent: var(--status-missing-accent);
  }

  .tone-na {
    --tone-bg: var(--status-na-bg);
    --tone-accent: var(--status-na-accent);
  }

  .tone-attention {
    --tone-bg: var(--status-attention-bg);
    --tone-accent: var(--status-attention-accent);
  }

  .tone-failed {
    --tone-bg: var(--status-failed-bg);
    --tone-accent: var(--status-failed-accent);
  }

  .action-notice {
    margin: -2px 0 14px;
    padding: 8px 10px;
    border: 1px solid var(--border);
    border-radius: 5px;
    background: var(--surface);
    color: #c8c8cc;
    font-size: 12px;
  }

  .control-note {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin: -2px 0 14px;
    color: var(--muted);
    font-size: 12px;
  }

  .control-note button {
    border: 0;
    background: transparent;
    color: var(--green);
    cursor: pointer;
  }

  .section-heading {
    min-height: 58px;
    justify-content: space-between;
    gap: 20px;
    padding: 11px 14px;
    border-bottom: 1px solid var(--border);
  }

  .section-heading p {
    margin-top: 2px;
    color: var(--muted);
    font-size: 12px;
  }

  .section-heading > span {
    color: var(--muted);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .legend {
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: 5px 12px;
    color: var(--muted);
    font-size: 11px;
  }

  .legend span {
    display: inline-flex;
    align-items: center;
    gap: 5px;
  }

  .legend b {
    color: #d4d4d8;
    font-weight: 550;
    font-variant-numeric: tabular-nums;
  }

  .legend-swatch {
    width: 15px;
    height: 15px;
    display: inline-grid;
    place-items: center;
    border: 1px solid var(--tone-accent);
    border-radius: 2px;
    background: var(--tone-bg);
    color: #f4f4f5;
    font-size: 9px;
    font-style: normal;
    line-height: 1;
  }

  .epoch-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(42px, 1fr));
    gap: 3px;
    max-height: 310px;
    overflow-y: auto;
    padding: 13px 14px;
  }

  .epoch-cell {
    min-width: 0;
    height: 24px;
    display: grid;
    grid-template-columns: 10px minmax(0, 1fr);
    align-items: center;
    gap: 2px;
    padding: 0 3px;
    border: 1px solid var(--tone-accent);
    border-radius: 3px;
    background: var(--tone-bg);
    color: #f4f4f5;
    font-family: ui-monospace, "SFMono-Regular", Consolas, monospace;
    font-size: 9px;
    font-variant-numeric: tabular-nums;
    line-height: 22px;
    cursor: pointer;
  }

  .epoch-cell > span {
    color: var(--tone-accent);
    font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif;
    font-size: 10px;
    text-align: center;
  }

  .epoch-cell > b {
    overflow: hidden;
    font-weight: 560;
    text-align: center;
    text-overflow: clip;
  }

  .epoch-cell:hover,
  .epoch-cell.selected {
    outline: 1px solid #f4f4f5;
    outline-offset: -2px;
  }

  .epoch-cell.tone-first-seen-complete,
  .legend-swatch.tone-first-seen-complete {
    border-width: 2px;
  }

  .epoch-cell.tone-missing,
  .legend-swatch.tone-missing,
  .artifact-group li.tone-missing {
    border-style: dashed;
  }

  .epoch-detail {
    min-height: 40px;
    flex-wrap: wrap;
    gap: 8px 18px;
    padding: 8px 14px;
    border-top: 1px solid var(--border);
    color: var(--muted);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .epoch-detail strong {
    color: var(--text);
  }

  .epoch-message {
    flex: 1 1 100%;
    color: var(--amber);
  }

  .epoch-message strong {
    margin-right: 5px;
    color: inherit;
    font-weight: 620;
  }

  .epoch-detail-close {
    margin-left: auto;
    white-space: nowrap;
  }

  .archive-context {
    display: grid;
    gap: 5px;
    padding: 8px 14px;
    border-top: 1px solid var(--border);
    background: var(--status-na-bg);
    color: #b8b8be;
    font-size: 11px;
  }

  .archive-context span {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
  }

  .archive-context strong {
    color: #dedee2;
    font-weight: 600;
  }

  .artifact-groups {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
    border-top: 1px solid var(--border);
    background: #141416;
  }

  .artifact-group {
    min-width: 0;
    padding: 9px 12px 11px;
    border-right: 1px solid var(--border);
  }

  .artifact-group:last-child {
    border-right: 0;
  }

  .artifact-group h3 {
    margin: 0 0 6px;
    color: #d5d5d9;
    font-size: 11px;
    font-weight: 600;
  }

  .artifact-group ul {
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .artifact-group li {
    min-height: 27px;
    display: grid;
    grid-template-columns: 14px minmax(78px, 1fr) auto auto;
    align-items: center;
    gap: 5px 8px;
    padding: 4px 6px;
    border-bottom: 1px solid #34363b;
    border-left: 2px solid var(--tone-accent);
    background: var(--tone-bg);
    color: #f4f4f5;
    font-size: 10px;
  }

  .artifact-group li:last-child {
    border-bottom: 0;
  }

  .artifact-icon {
    color: var(--tone-accent);
    text-align: center;
  }

  .artifact-group li strong {
    overflow: hidden;
    font-weight: 560;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .artifact-state,
  .artifact-requirement,
  .artifact-bytes {
    color: #c7c7cd;
    white-space: nowrap;
  }

  .artifact-requirement {
    color: var(--tone-accent);
  }

  .artifact-bytes {
    min-width: 46px;
    text-align: right;
  }

  .lanes-panel,
  .two-column {
    margin-top: 14px;
  }

  .worker-policy {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 24px;
    padding: 9px 14px;
    border-bottom: 1px solid var(--border);
    background: #19191c;
    color: var(--muted);
    font-size: 11px;
    font-variant-numeric: tabular-nums;
  }

  .worker-policy > div {
    min-width: 190px;
    display: grid;
    gap: 2px;
  }

  .worker-policy strong {
    color: #dddddf;
    font-size: 11px;
    font-weight: 600;
  }

  .worker-policy-action {
    flex: 1 1 300px;
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  th,
  td {
    height: 39px;
    padding: 7px 12px;
    border-bottom: 1px solid #29292d;
    text-align: left;
    white-space: nowrap;
  }

  th {
    color: var(--muted);
    font-weight: 520;
  }

  tbody tr:last-child td {
    border-bottom: 0;
  }

  .progress-column {
    width: 220px;
  }

  th:nth-child(5),
  td:nth-child(5) {
    min-width: 132px;
  }

  .io-rate-column {
    width: 166px;
    min-width: 166px;
    max-width: 166px;
  }

  .io-rate-pair {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
  }

  .io-rate-pair > span {
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .logical-input-rate {
    min-height: 13px;
    display: block;
    margin-top: 1px;
    overflow: hidden;
    color: var(--faint);
    font-size: 10px;
    line-height: 12px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .eta-column {
    width: 92px;
    min-width: 92px;
  }

  .rss-column {
    width: 105px;
    min-width: 105px;
  }

  .inline-progress {
    gap: 8px;
  }

  .inline-progress progress {
    min-width: 90px;
  }

  .inline-progress span {
    width: 40px;
    color: var(--muted);
    text-align: right;
  }

  .task-name {
    min-width: 150px;
    display: grid;
    gap: 1px;
  }

  .task-name strong {
    color: var(--text);
    font-weight: 560;
  }

  .task-name .mono {
    color: var(--faint);
    font-size: 10px;
  }

  .task-phase,
  .queue-phase {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    color: #d5d5d9;
  }

  .task-phase > span {
    color: var(--green);
  }

  .task-phase-cell {
    max-width: 290px;
    display: grid;
    gap: 2px;
  }

  .task-phase.task-paused > span {
    color: var(--amber);
  }

  .pause-detail {
    max-width: 270px;
    overflow: hidden;
    color: #d7b78f;
    font-size: 10px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .queue-phase {
    min-width: 0;
    width: 100%;
  }

  .queue-phase > b {
    color: var(--status-ready-accent);
    font-size: 12px;
    font-weight: 600;
  }

  .queue-phase.queue-deferred > b {
    color: var(--amber);
  }

  .queue-phase-copy {
    min-width: 0;
    display: grid;
  }

  .queue-phase-copy em {
    overflow: hidden;
    color: var(--faint);
    font-size: 10px;
    font-style: normal;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .queue-deferred .queue-phase-copy em {
    color: #d7b78f;
  }

  .row-action {
    white-space: nowrap;
  }

  .mono,
  .path {
    font-family: ui-monospace, "SFMono-Regular", Consolas, monospace;
  }

  .empty,
  .empty-cell {
    color: var(--faint);
  }

  .empty {
    padding: 18px 14px;
  }

  .empty-cell {
    height: 56px;
    text-align: center;
  }

  .two-column {
    display: grid;
    grid-template-columns: minmax(0, 1.3fr) minmax(330px, 0.7fr);
    gap: 14px;
  }

  .two-column.single-column {
    grid-template-columns: 1fr;
  }

  .live-progress {
    padding: 14px;
  }

  .capture {
    border-bottom: 1px solid var(--border);
  }

  .capture:last-child {
    border-bottom: 0;
  }

  .live-progress > div:first-child {
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 8px;
  }

  .capture-title {
    min-width: 0;
    display: grid;
    gap: 1px;
  }

  .capture-title > span {
    overflow: hidden;
    color: var(--faint);
    font-family: ui-monospace, "SFMono-Regular", Consolas, monospace;
    font-size: 10px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .slot-range {
    justify-content: space-between;
    margin-top: 6px;
    color: var(--muted);
    font-size: 11px;
    font-variant-numeric: tabular-nums;
  }

  .facts {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    margin: 0;
    border-top: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
  }

  .facts div {
    min-width: 0;
    padding: 10px 14px;
    border-right: 1px solid var(--border);
  }

  .facts div:nth-child(4n) {
    border-right: 0;
  }

  .facts div:nth-child(n + 5) {
    border-top: 1px solid var(--border);
  }

  .facts dt {
    color: var(--muted);
    font-size: 11px;
  }

  .facts dd {
    margin: 3px 0 0;
    font-variant-numeric: tabular-nums;
    line-height: 1.35;
  }

  .live-artifacts {
    border-bottom: 1px solid var(--border);
    background: #141416;
  }

  .live-artifacts summary {
    min-height: 34px;
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 14px;
    color: var(--muted);
    font-size: 11px;
    cursor: pointer;
  }

  .live-artifacts summary strong {
    color: #d7d7db;
    font-weight: 600;
  }

  .live-artifact-groups {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    border-top: 1px solid var(--border);
  }

  .live-artifact-group {
    min-width: 0;
    padding: 7px 10px 9px;
    border-right: 1px solid var(--border);
  }

  .live-artifact-group:last-child {
    border-right: 0;
  }

  .live-artifact-group h3 {
    margin: 0 0 5px;
    color: #cfcfd4;
    font-size: 10px;
    font-weight: 600;
  }

  .live-artifact-group ul {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 2px;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .live-artifact-group li {
    min-width: 0;
    min-height: 23px;
    display: grid;
    grid-template-columns: 12px minmax(60px, 1fr) auto auto;
    align-items: center;
    gap: 5px;
    padding: 3px 5px;
    border-left: 2px solid var(--tone-accent);
    background: var(--tone-bg);
    color: #f4f4f5;
    font-size: 9px;
  }

  .live-artifact-group li > span:first-child {
    color: var(--tone-accent);
    text-align: center;
  }

  .live-artifact-group li strong {
    overflow: hidden;
    font-weight: 560;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .live-artifact-group li > span:nth-child(3) {
    color: #c7c7cd;
    white-space: nowrap;
  }

  .live-artifact-group li em {
    color: var(--tone-accent);
    font-style: normal;
    white-space: nowrap;
  }

  .live-capture-diagnostics {
    display: grid;
    gap: 6px;
    padding: 10px 14px;
    border-top: 1px solid var(--border);
    background: #19191c;
    color: var(--muted);
    font-size: 11px;
  }

  .live-capture-diagnostics > strong {
    color: #d7d7db;
  }

  .live-capture-diagnostics ul {
    display: grid;
    gap: 5px;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .live-capture-diagnostics li {
    display: grid;
    grid-template-columns: minmax(180px, 0.45fr) minmax(0, 1fr);
    gap: 10px;
  }

  .live-capture-diagnostics li span:first-child {
    overflow: hidden;
    color: #c8c8cc;
    font-family: ui-monospace, "SFMono-Regular", Consolas, monospace;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .repair-gate {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 6px 14px;
    margin: 12px 14px;
    padding: 9px 10px;
    border: 1px solid #775235;
    border-radius: 5px;
    background: #2b211a;
    color: #e2bd90;
    font-size: 12px;
  }

  .repair-gate.retained-diagnostic {
    border-color: var(--border);
    background: #19191c;
    color: #c8c8cc;
  }

  .packaged-note {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 6px 14px;
    margin: 12px 14px;
    padding: 9px 10px;
    border: 1px solid #575a60;
    border-radius: 5px;
    background: #242528;
    color: #c4c5c8;
    font-size: 12px;
  }

  .live-retry {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 0 14px 12px;
    color: var(--muted);
    font-size: 11px;
  }

  .path {
    overflow: hidden;
    padding: 0 14px 13px;
    color: var(--faint);
    font-size: 10px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .queue {
    max-height: 322px;
    overflow-y: auto;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .queue li {
    min-height: 42px;
    display: grid;
    grid-template-columns: 24px 90px 62px minmax(120px, 1fr) auto;
    align-items: center;
    gap: 8px;
    padding: 7px 14px;
    border-bottom: 1px solid #29292d;
    color: var(--muted);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .queue li:last-child {
    border-bottom: 0;
  }

  .queue-position {
    color: var(--faint);
  }

  .queue strong {
    color: var(--text);
  }

  .queue-eta {
    min-width: 64px;
    text-align: right;
  }

  .lower-grid {
    grid-template-columns: minmax(0, 1fr) minmax(360px, 1fr);
  }

  .resources {
    padding: 4px 14px 12px;
  }

  .resource-row {
    display: grid;
    grid-template-columns: minmax(180px, 0.75fr) minmax(120px, 1fr) minmax(110px, auto);
    align-items: center;
    gap: 14px;
    min-height: 42px;
    border-bottom: 1px solid #29292d;
    color: var(--muted);
    font-size: 11px;
    font-variant-numeric: tabular-nums;
  }

  .resource-row:last-child {
    border-bottom: 0;
  }

  .resource-row > div {
    justify-content: space-between;
    gap: 12px;
  }

  .resource-row strong {
    color: var(--text);
    font-size: 12px;
  }

  .pressure-row progress::-webkit-progress-value {
    background: var(--amber);
  }

  .pressure-row progress::-moz-progress-bar {
    background: var(--amber);
  }

  .errors {
    max-height: 180px;
    overflow-y: auto;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .errors li {
    padding: 9px 14px;
    border-bottom: 1px solid #29292d;
  }

  .errors li:last-child {
    border-bottom: 0;
  }

  .errors li > div {
    justify-content: space-between;
    gap: 16px;
  }

  .errors strong {
    color: var(--red);
    font-size: 12px;
  }

  .errors time {
    color: var(--faint);
    font-size: 11px;
  }

  .errors p {
    margin-top: 3px;
    color: #c8c8cc;
    font-size: 12px;
  }

  .loading {
    min-height: calc(100vh - 54px);
    display: grid;
    place-content: center;
    gap: 6px;
    color: var(--muted);
    text-align: center;
  }

  .loading h2 {
    color: var(--text);
  }

  .loading code {
    color: #bdbdc2;
  }

  .token-dialog {
    width: min(430px, calc(100vw - 28px));
    padding: 0;
    border: 1px solid var(--border-strong);
    border-radius: 8px;
    background: var(--surface-raised);
    color: var(--text);
  }

  .token-dialog::backdrop {
    background: rgb(0 0 0 / 68%);
  }

  .token-dialog form {
    padding: 16px;
  }

  .dialog-heading {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
  }

  .dialog-close {
    width: 28px;
    padding: 0;
    font-size: 18px;
    line-height: 1;
  }

  .token-dialog p {
    margin: 8px 0 14px;
    color: var(--muted);
    font-size: 12px;
  }

  .token-dialog label {
    display: block;
    margin-bottom: 5px;
    color: #d1d1d5;
    font-size: 12px;
  }

  .token-dialog input {
    width: 100%;
    height: 34px;
    padding: 0 9px;
    border: 1px solid var(--border-strong);
    border-radius: 5px;
    outline: 0;
    background: #121214;
    color: var(--text);
    font-family: ui-monospace, "SFMono-Regular", Consolas, monospace;
  }

  .token-dialog input:focus {
    border-color: var(--green);
    outline: 2px solid #315b4a;
  }

  .dialog-actions {
    display: flex;
    justify-content: flex-end;
    gap: 7px;
    margin-top: 14px;
  }

  .dialog-actions .primary-action {
    border-color: #477a64;
    background: #315b4a;
    color: #ffffff;
  }

  @media (max-width: 1050px) {
    .topbar {
      align-items: flex-start;
      padding-block: 10px;
    }

    .top-status {
      flex-wrap: wrap;
      row-gap: 4px;
    }

    .two-column,
    .lower-grid {
      grid-template-columns: 1fr;
    }

  }

  @media (max-width: 700px) {
    .topbar {
      display: block;
      padding: 10px 14px;
    }

    .top-status {
      justify-content: flex-start;
      margin-top: 7px;
    }

    .toolbar {
      margin: 8px 0 0;
    }

    .top-status span:not(.connection),
    .top-status strong {
      display: none;
    }

    main {
      padding: 12px 10px 24px;
    }

    .summary-line {
      gap: 7px 14px;
    }

    .summary-progress {
      flex: 1 1 100%;
      margin-left: 0;
    }

    .section-heading {
      align-items: flex-start;
    }

    .epoch-panel .section-heading {
      display: block;
    }

    .legend {
      max-width: none;
      justify-content: flex-start;
      margin-top: 8px;
    }

    .epoch-grid {
      grid-template-columns: repeat(auto-fill, minmax(40px, 1fr));
      max-height: 260px;
      padding: 10px;
    }

    .facts {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .facts div,
    .facts div:nth-child(4n) {
      border-right: 1px solid var(--border);
      border-top: 0;
    }

    .facts div:nth-child(odd) {
      border-right: 1px solid var(--border);
    }

    .facts div:nth-child(even) {
      border-right: 0;
    }

    .facts div:nth-child(n + 3) {
      border-top: 1px solid var(--border);
    }

    .resource-row {
      grid-template-columns: 1fr;
      gap: 5px;
      padding: 8px 0;
    }

    .queue li {
      grid-template-columns: 20px 80px 54px minmax(120px, 1fr);
    }

    .queue-eta {
      display: none;
    }
  }
</style>
