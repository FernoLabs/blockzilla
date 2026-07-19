<script lang="ts">
  import { tick } from 'svelte';
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
    hasProcessResourceMetrics,
    processDiskIoRate,
    processMetric,
    rankProcessIo,
    type ProcessIoEntry
  } from '$lib/process-telemetry';
  import {
    buildEpochCalendarMonths,
    epochCalendarStartDate,
    formatEpochCalendarDay,
    formatEpochCalendarRange,
    type EpochCalendarEntry
  } from '$lib/epoch-calendar';
  import {
    integerValue,
    numberValue,
    type ArtifactStatus,
    type EpochStatus,
    type LaneStatus,
    type LiveState,
    type LiveStatus,
    type MachineStatus,
    type PipelineSummary,
    type ProcessIoSnapshot,
    type ProgressSnapshot
  } from '$lib/pipeline-snapshot';
  import { useWatcherClient } from '$lib/watcher-client.svelte';
  import { formatBytes } from '$lib/format';

  type VisualState = 'complete' | 'first-seen-complete' | 'legacy-complete' | 'active' | 'ready' | 'finalizing' | 'partial' | 'queued' | 'missing' | 'na' | 'attention' | 'failed';

  type ArtifactGroup = {
    id: 'car' | 'preflight' | 'source' | 'archive';
    label: string;
    artifacts: ArtifactStatus[];
  };

  type EpochMapEntry =
    | { epoch: number; kind: 'historical'; status: EpochStatus }
    | { epoch: number; kind: 'live'; status: LiveStatus };

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

  const watcher = useWatcherClient();
  const snapshot = $derived(watcher.snapshot);
  const connectionState = $derived(watcher.connectionState);
  const connectionMessage = $derived(watcher.connectionMessage);
  let selectedEpoch = $state<number | null>(null);
  let selectionAnnouncement = $state('');
  let epochTabStop = $state<number | null>(null);
  let showProcessResources = $state(false);

  const groupedLiveCaptures = $derived(groupLiveCaptures(snapshot?.live ?? []));
  const liveCapturesByEpoch = $derived(
    canonicalLiveCaptures(
      groupedLiveCaptures.visible.filter(isLiveWorkflowCapture),
      snapshot?.current_epoch ?? null
    )
  );
  const currentLiveCapture = $derived(
    groupedLiveCaptures.visible.find((capture) => capture.is_current && capture.state === 'capturing') ??
      groupedLiveCaptures.visible.find((capture) => capture.epoch === snapshot?.current_epoch && capture.state === 'capturing') ??
      groupedLiveCaptures.visible.find((capture) => capture.state === 'capturing') ??
      groupedLiveCaptures.visible.find((capture) => capture.is_current) ??
      null
  );
  const visibleLiveCaptures = $derived(
    selectVisibleLiveCaptures(
      liveCapturesByEpoch,
      groupedLiveCaptures.visible,
      currentLiveCapture
    )
  );
  const pendingLiveCaptures = $derived(
    visibleLiveCaptures.filter((capture) => capture !== currentLiveCapture)
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
  const calendarMonths = $derived(
    buildEpochCalendarMonths(epochMap, snapshot?.epoch_calendar ?? [])
  );
  const epochCalendarSummary = $derived.by(() => {
    let dated = 0;
    let estimated = 0;
    for (const month of calendarMonths) {
      for (const item of month.items) {
        if (!item.timing) continue;
        dated += 1;
        if (item.timing.precision === 'estimated') estimated += 1;
      }
    }
    return { dated, estimated, undated: epochMap.length - dated };
  });
  const latestTrackedEpoch = $derived(epochMap.at(-1)?.epoch ?? null);
  const selectedEpochEntry = $derived(
    selectedEpoch === null ? null : (epochMap.find((entry) => entry.epoch === selectedEpoch) ?? null)
  );
  const selectedEpochTiming = $derived(
    selectedEpoch === null
      ? null
      : (snapshot?.epoch_calendar?.find((timing) => timing.epoch === selectedEpoch) ?? null)
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
  const externalProcessIo = $derived(rankProcessIo(snapshot?.process_io?.processes ?? [], 10));
  const hasProcessResources = $derived(hasProcessResourceMetrics(externalProcessIo));
  const showProcessResourceColumns = $derived(showProcessResources && hasProcessResources);
  const processIoSampleAgeSecs = $derived(
    snapshot?.process_io?.sampled_unix_secs === null ||
      snapshot?.process_io?.sampled_unix_secs === undefined
      ? null
      : Math.max(0, snapshot.now_unix_secs - snapshot.process_io.sampled_unix_secs)
  );
  const processIoStale = $derived(
    processIoSampleAgeSecs !== null &&
      processIoSampleAgeSecs > Math.max(30, (snapshot?.process_io?.sample_window_secs ?? 0) * 3)
  );
  const processIoMeta = $derived(
    processIoSummary(snapshot?.process_io, externalProcessIo.length, processIoSampleAgeSecs, processIoStale)
  );
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

  function epochMapTooltip(entry: EpochMapEntry, timing: EpochCalendarEntry | null = null) {
    const progress = epochMapProgress(entry);
    const parts = [
      `Epoch ${entry.epoch}`,
      timing ? formatEpochCalendarRange(timing) : 'Chain date unavailable',
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

  function handleEpochGridKeydown(event: KeyboardEvent) {
    if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'].includes(event.key)) return;
    const current = event.currentTarget;
    if (!(current instanceof HTMLButtonElement)) return;
    const calendar = current.closest('.epoch-calendar');
    const monthGrid = current.closest('.epoch-month-grid');
    if (!(calendar instanceof HTMLElement) || !(monthGrid instanceof HTMLElement)) return;
    const cells = [...calendar.querySelectorAll<HTMLButtonElement>('.epoch-cell')];
    const index = cells.indexOf(current);
    if (index < 0) return;

    const columnCount = getComputedStyle(monthGrid).gridTemplateColumns
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
    if (event.key !== 'Escape' || selectedEpoch === null) return;
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
    return capture.state === 'capturing' ? 'Epoch ETA' : 'Compact ETA';
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
    return `${formatDecimal(rate, 2)} ${capture.state === 'capturing' ? 'slots/s' : 'blocks/s'}`;
  }

  function compactLiveMetric(value: string) {
    return value === 'unknown' || value === 'not active' ? '—' : value;
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

  function processIoRateAriaLabel(process: ProcessIoEntry) {
    const read = processMetric(process.read_mib_per_sec);
    const write = processMetric(process.write_mib_per_sec);
    const total = processDiskIoRate(process);
    return [
      read === null ? 'read unavailable' : `read ${formatDecimal(read)} mebibytes per second`,
      write === null ? 'write unavailable' : `write ${formatDecimal(write)} mebibytes per second`,
      total === null
        ? 'total unavailable'
        : `total ${formatDecimal(total)} mebibytes per second`
    ].join(', ');
  }

  function processIoSummary(
    processIo: ProcessIoSnapshot | null | undefined,
    visibleCount: number,
    sampleAgeSecs: number | null,
    stale: boolean
  ) {
    if (!processIo) return 'not reported';
    if (processIo.state === 'collecting') return 'collecting';
    if (processIo.state === 'unavailable') return 'unavailable';
    const count = processIo.truncated
      ? `${visibleCount} of ${formatInteger(processIo.active_count)} active`
      : `${formatInteger(processIo.active_count)} active`;
    if (sampleAgeSecs === null) return count;
    return `${count} · ${stale ? 'stale · ' : ''}sampled ${formatDuration(sampleAgeSecs)} ago`;
  }

  function processIoEmptyMessage(processIo: ProcessIoSnapshot | null | undefined) {
    if (!processIo) return 'Process I/O is not exposed by this watcher API.';
    if (processIo.state === 'collecting') return 'Collecting process counters; rates require two samples.';
    if (processIo.state === 'unavailable') return processIo.message ?? 'Process I/O is unavailable on this host.';
    const window = processIo.sample_window_secs === null
      ? 'the latest sample'
      : `the last ${formatDuration(processIo.sample_window_secs)}`;
    return `No other process used measurable storage I/O during ${window}.`;
  }

  function formatProcessCpu(value: number | null | undefined) {
    const metric = processMetric(value);
    return metric === null ? '—' : `${formatDecimal(metric)}%`;
  }

  function toggleProcessResources(event: Event) {
    const input = event.currentTarget;
    if (input instanceof HTMLInputElement) showProcessResources = input.checked;
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

</script>

<svelte:window onkeydown={handlePageKeydown} />

<svelte:head>
  <title>Blockzilla Watcher</title>
  <meta
    name="description"
    content="Live status for Blockzilla historical compaction, live capture, and archive finalization."
  />
</svelte:head>

<div class="shell">
  {#if snapshot}
    <main>
      <section class="priority-panel" aria-label="Current Blockzilla work">
        <div class="priority-summary">
          <div class="queue-eta-primary" title={runnableQueueEtaTitle}>
            <span>Queue ETA</span>
            <strong>{formatDuration(runnableQueueEtaSecs)}</strong>
          </div>
          <div>
            <span>Complete</span>
            <strong>{snapshot.summary.complete} / {snapshot.summary.epochs_total}</strong>
          </div>
          <div>
            <span>Queued</span>
            <strong>{snapshot.summary.queued}</strong>
          </div>
          <div class:danger={historicalNeedsAction > 0} title={`${snapshot.summary.blocked} blocked · ${snapshot.summary.failed} failed`}>
            <span>Needs action</span>
            <strong>{historicalNeedsAction}</strong>
          </div>
        </div>

        <div class="work-lines">
          {#if currentLiveCapture}
            <article class="work-line live-work-line">
              <div class="work-identity">
                <span class="work-kind">Live index</span>
                <strong>Epoch {formatInteger(currentLiveCapture.epoch)}</strong>
              </div>
              <span class={`plain-status tone-${liveVisualState(currentLiveCapture)}`}>
                <span aria-hidden="true">{VISUAL_META[liveVisualState(currentLiveCapture)].icon}</span>
                {liveStateLabel(currentLiveCapture)}
              </span>
              <div class="work-progress" title={`${formatDecimal(liveProgress(currentLiveCapture))}%`}>
                <progress
                  max="100"
                  value={liveProgress(currentLiveCapture)}
                  aria-label={`Live epoch ${formatInteger(currentLiveCapture.epoch)} progress`}
                  aria-valuetext={`${formatDecimal(liveProgress(currentLiveCapture))} percent`}
                >{liveProgress(currentLiveCapture)}%</progress>
              </div>
              <div class="work-metric">
                <span>Rate</span>
                <strong>{connectionState === 'live' ? compactLiveMetric(liveRateValue(currentLiveCapture)) : 'paused'}</strong>
              </div>
              <div class="work-metric work-eta">
                <span>{liveEtaLabel(currentLiveCapture)}</span>
                <strong>{connectionState === 'live' ? compactLiveMetric(liveEtaValue(currentLiveCapture)) : 'paused'}</strong>
              </div>
            </article>
          {:else}
            <article class="work-line idle-work-line">
              <div class="work-identity">
                <span class="work-kind">Live index</span>
                <strong>Idle</strong>
              </div>
            </article>
          {/if}

          {#each activeHistoricalLanes as lane (lane.id)}
            {@const metricsFresh = connectionState === 'live' && laneMetricsFresh(lane)}
            <article class="work-line archive-work-line">
              <div class="work-identity">
                <span class="work-kind">Archive</span>
                <strong>Epoch {formatInteger(lane.epoch)}</strong>
              </div>
              <span class="task-phase" class:task-paused={lane.state === 'paused'}>
                <span aria-hidden="true">{taskStateIcon(lane.state)}</span>
                {humanize(lane.phase)}
              </span>
              <div class="work-progress" title={`${formatDecimal(lane.progress.progress_pct)}%`}>
                <progress
                  max="100"
                  value={lane.progress.progress_pct ?? 0}
                  aria-label={`Archive epoch ${formatInteger(lane.epoch)} progress`}
                  aria-valuetext={`${formatDecimal(lane.progress.progress_pct)} percent`}
                >{lane.progress.progress_pct ?? 0}%</progress>
              </div>
              <div class="work-metric">
                <span>Blocks</span>
                <strong>{formatInteger(lane.progress.blocks_done)}</strong>
              </div>
              <div class="work-metric work-eta">
                <span>Task ETA</span>
                <strong>{metricsFresh ? formatDuration(lane.progress.eta_secs) : '—'}</strong>
              </div>
            </article>
          {/each}
        </div>
      </section>

      {#if snapshot.summary.admission_blocked_reason || snapshot.summary.finalizer_admission_blocked_reason || snapshot.scheduler.paused}
        <div class="operations-alerts" role="status">
          {#if snapshot.scheduler.paused}<span><strong>Scheduler paused</strong> · active work is draining</span>{/if}
          {#if snapshot.summary.admission_blocked_reason}<span><strong>Scan admission</strong> · {snapshot.summary.admission_blocked_reason}</span>{/if}
          {#if snapshot.summary.finalizer_admission_blocked_reason}<span><strong>Finalizer</strong> · {snapshot.summary.finalizer_admission_blocked_reason}</span>{/if}
        </div>
      {/if}

      <section class="panel epoch-panel">
        <div class="section-heading">
          <div>
            <h2>Epoch calendar</h2>
            <p>
              {#if latestTrackedEpoch !== null}Through epoch {formatInteger(latestTrackedEpoch)}{/if}
              {#if epochCalendarSummary.dated > 0}
                · UTC dates
                {#if epochCalendarSummary.estimated > 0} · ~ estimated{/if}
                {#if epochCalendarSummary.undated > 0} · {epochCalendarSummary.undated} undated{/if}
              {:else}
                · dates unavailable
              {/if}
            </p>
          </div>
          <div class="legend" aria-label="Epoch status legend">
            {#each EPOCH_LEGEND as item (item.tone)}
              {#if epochToneCounts[item.tone] > 0}
                <span>
                  <i class={`legend-swatch tone-${item.tone}`} aria-hidden="true"></i>
                  {item.label}
                </span>
              {/if}
            {/each}
          </div>
        </div>

        {#if epochMap.length > 0}
          <div class="epoch-calendar" aria-label="Epoch calendar with UTC dates">
            {#each calendarMonths as month (month.key)}
              <section class="epoch-month">
                <h3
                  class:estimated={month.has_estimates}
                  aria-label={`${month.label}${month.has_estimates ? ', contains estimated dates' : ''}`}
                  title={month.key === 'undated'
                    ? 'Chain dates unavailable'
                    : month.has_estimates
                      ? 'Contains estimated UTC dates'
                      : 'Observed UTC dates'}
                >
                  {#if month.has_estimates}<span aria-hidden="true">~&nbsp;</span>{/if}{month.label}
                </h3>
                <div class="epoch-month-grid">
                  {#each month.items as item (item.value.epoch)}
                    {@const entry = item.value}
                    {@const timing = item.timing}
                    {@const tone = epochMapVisualState(entry)}
                    <button
                      id={`epoch-cell-${entry.epoch}`}
                      type="button"
                      class={`epoch-cell tone-${tone}`}
                      class:selected={selectedEpoch === entry.epoch}
                      class:current={entry.kind === 'live' && entry.status.is_current === true}
                      aria-expanded={selectedEpoch === entry.epoch}
                      aria-controls={`epoch-detail-${entry.epoch}`}
                      aria-current={entry.kind === 'live' && entry.status.is_current ? 'step' : undefined}
                      aria-label={epochMapTooltip(entry, timing)}
                      title={epochMapTooltip(entry, timing)}
                      data-epoch={entry.epoch}
                      tabindex={(epochTabStop ?? latestTrackedEpoch) === entry.epoch ? 0 : -1}
                      onfocus={() => epochTabStop = entry.epoch}
                      onkeydown={handleEpochGridKeydown}
                      onclick={() => void toggleEpochDetails(entry.epoch)}
                    >
                      <b>E{entry.epoch}</b>
                      {#if timing}
                        <time
                          class:estimated={timing.precision === 'estimated'}
                          datetime={timing.precision === 'observed' ? epochCalendarStartDate(timing) : undefined}
                        >{timing.precision === 'estimated' ? '~' : ''}{formatEpochCalendarDay(timing)}</time>
                      {:else}
                        <span class="epoch-day-unavailable" aria-hidden="true">—</span>
                      {/if}
                    </button>
                  {/each}
                </div>
              </section>
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
              {#if selectedEpochTiming}
                <span>{formatEpochCalendarRange(selectedEpochTiming)}</span>
              {/if}
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
                <span>{connectionState === 'live' ? liveRateValue(selectedLiveStatus) : 'rate paused'}</span>
                <span>{liveEtaLabel(selectedLiveStatus)} {connectionState === 'live' ? liveEtaValue(selectedLiveStatus) : 'paused'}</span>
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
            {#if (selectedEpochStatus && (selectedRegistryOrderContext || selectedSourceRetired || selectedLegacyNoAccessMessage)) || selectedArtifactGroups.length > 0}
              <details class="epoch-inspection">
                <summary>Archive files and context</summary>
                {#if selectedEpochStatus && (selectedRegistryOrderContext || selectedSourceRetired || selectedLegacyNoAccessMessage)}
                  <div class="archive-context" role="note">
                    {#if selectedRegistryOrderContext}
                      <span><strong>Registry order</strong> {selectedRegistryOrderContext}</span>
                    {/if}
                    {#if selectedSourceRetired}
                      <span><strong>Source retired</strong> Finalized archive retained; source CAR removed.</span>
                    {/if}
                    {#if selectedLegacyNoAccessMessage}
                      <span><strong>Legacy no-access</strong> {selectedLegacyNoAccessMessage}</span>
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
                              {#if requirementLabel}<span class="artifact-requirement">{requirementLabel}</span>{/if}
                              <span class="artifact-bytes">{formatBytes(artifact.bytes)}</span>
                            </li>
                          {/each}
                        </ul>
                      </section>
                    {/each}
                  </div>
                {/if}
              </details>
            {/if}
          {/if}
        {:else}
          <p class="empty">No epoch plan has been loaded.</p>
        {/if}
      </section>

      <details class="panel disclosure-panel lanes-panel">
        <summary>
          <strong>Tasks</strong>
          <b>{activeLanes.length} active</b>
        </summary>

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
                </tr>
              {:else}
                <tr><td colspan="8" class="empty-cell">No task is active.</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      </details>

      <details class="panel disclosure-panel pipeline-disclosure">
        <summary>
          <strong>Live pipeline</strong>
          {#if waitingLiveCaptureCount + snapshot.finalizer_queue.length > 0}
            <b>{waitingLiveCaptureCount + snapshot.finalizer_queue.length} waiting</b>
          {/if}
        </summary>
        <div class="two-column" class:single-column={snapshot.finalizer_queue.length === 0}>
        <section class="live-panel">
          <div class="section-heading">
            <div>
              <h2>Closed captures</h2>
            </div>
            <span>
              {waitingLiveCaptureCount} in pipeline
              {#if liveNeedsActionCount > 0}
                · {liveNeedsActionCount} {liveNeedsActionCount === 1 ? 'needs' : 'need'} action
              {/if}
            </span>
          </div>

          {#each pendingLiveCaptures as capture (capture.id)}
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

              <div class="path" title={capture.capture_dir}>{capture.capture_dir}</div>
            </div>
          {:else}
            <p class="empty">No closed capture is waiting.</p>
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
          <section class="queue-panel">
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
      </details>

      <div class="system-monitor">
        <div class="two-column lower-grid">
        <section class="panel machine-panel">
          <div class="section-heading">
            <div>
              <h2>NAS resources</h2>
            </div>
            {#if snapshot.machine.load_1m !== null}<span>load {formatDecimal(snapshot.machine.load_1m, 2)}</span>{/if}
          </div>

          <div class="resources">
            <div class="resource-row">
              <div><strong>Memory</strong><span>{formatBytes(snapshot.machine.memory_used_bytes)} / {formatBytes(snapshot.machine.memory_total_bytes)}</span></div>
              <progress
                max="100"
                value={machineMemoryPct}
                aria-label="Memory used"
                aria-valuetext={`${formatBytes(snapshot.machine.memory_used_bytes)} of ${formatBytes(snapshot.machine.memory_total_bytes)}`}
              >{machineMemoryPct}%</progress>
              <span>{formatBytes(snapshot.machine.memory_available_bytes)} available</span>
            </div>
            {#if snapshot.machine.memory_pressure_full_avg10 !== null && snapshot.machine.memory_pressure_full_avg10 !== undefined}
              <div class="resource-row pressure-row">
                <div>
                  <strong>Memory pressure</strong>
                  <span>full avg10 {formatDecimal(snapshot.machine.memory_pressure_full_avg10, 2)}%</span>
                </div>
                <progress
                  max="100"
                  value={snapshot.machine.memory_pressure_full_avg10}
                  aria-label="Full memory pressure average over 10 seconds"
                  aria-valuetext={`${formatDecimal(snapshot.machine.memory_pressure_full_avg10, 2)} percent`}
                >
                  {snapshot.machine.memory_pressure_full_avg10}%
                </progress>
                <span>some avg10 {formatDecimal(snapshot.machine.memory_pressure_some_avg10, 2)}%</span>
              </div>
            {/if}
            <div class="resource-row">
                <div><strong>{snapshot.machine.car_disk_total_bytes && !hasSeparateCarStorage ? 'Archive + CAR storage' : 'Archive storage'}</strong><span>{formatBytes(snapshot.machine.disk_used_bytes)} / {formatBytes(snapshot.machine.disk_total_bytes)}</span></div>
              <progress
                max="100"
                value={machineDiskPct}
                aria-label="Archive storage used"
                aria-valuetext={`${formatBytes(snapshot.machine.disk_used_bytes)} of ${formatBytes(snapshot.machine.disk_total_bytes)}`}
              >{machineDiskPct}%</progress>
              <span>{formatBytes(snapshot.machine.disk_available_bytes)} available</span>
            </div>
            {#if snapshot.machine.io_pressure_full_avg10 !== null && snapshot.machine.io_pressure_full_avg10 !== undefined}
              <div class="resource-row pressure-row">
                <div>
                  <strong>I/O pressure</strong>
                  <span>full avg10 {formatDecimal(snapshot.machine.io_pressure_full_avg10, 2)}%</span>
                </div>
                <progress
                  max="100"
                  value={snapshot.machine.io_pressure_full_avg10}
                  aria-label="Full I/O pressure average over 10 seconds"
                  aria-valuetext={`${formatDecimal(snapshot.machine.io_pressure_full_avg10, 2)} percent`}
                >
                  {snapshot.machine.io_pressure_full_avg10}%
                </progress>
                <span>
                  some avg10 {formatDecimal(snapshot.machine.io_pressure_some_avg10, 2)}%
                </span>
              </div>
            {/if}
            {#if hasSeparateCarStorage}
              <div class="resource-row">
                <div><strong>CAR storage</strong><span>{formatBytes(snapshot.machine.car_disk_used_bytes)} / {formatBytes(snapshot.machine.car_disk_total_bytes)}</span></div>
              <progress
                max="100"
                value={carDiskPct}
                aria-label="CAR storage used"
                aria-valuetext={`${formatBytes(snapshot.machine.car_disk_used_bytes)} of ${formatBytes(snapshot.machine.car_disk_total_bytes)}`}
              >{carDiskPct}%</progress>
                <span>{formatBytes(snapshot.machine.car_disk_available_bytes)} available</span>
              </div>
            {/if}
            <div class="resource-row">
              <div><strong>Swap</strong><span>{formatBytes(snapshot.machine.swap_used_bytes)} / {formatBytes(snapshot.machine.swap_total_bytes)}</span></div>
              <progress
                max="100"
                value={machineSwapPct}
                aria-label="Swap used"
                aria-valuetext={`${formatBytes(snapshot.machine.swap_used_bytes)} of ${formatBytes(snapshot.machine.swap_total_bytes)}`}
              >{machineSwapPct}%</progress>
            </div>
          </div>
        </section>

        <section class="panel errors-panel">
          <div class="section-heading">
            <div>
              <h2>Recent error log</h2>
            </div>
            {#if snapshot.errors.length > 0}<span>{snapshot.errors.length}</span>{/if}
          </div>

          <ul class="errors">
            {#each snapshot.errors as error (`${error.at_unix_secs}-${error.scope}-${error.message}`)}
              <li>
                <div><strong>{error.scope}</strong><time datetime={new Date(error.at_unix_secs * 1000).toISOString()}>{formatClock(error.at_unix_secs)}</time></div>
                <p>{error.message}</p>
              </li>
            {:else}
              <li class="empty">No recent errors.</li>
            {/each}
          </ul>
        </section>
        </div>

        <section class="panel process-panel">
          <div class="section-heading process-heading">
            <div>
              <h2>External process I/O</h2>
            </div>
            <div class="process-controls">
              <span class:stale={processIoStale}>{processIoMeta}</span>
              <label
                class="process-option"
                class:disabled={!hasProcessResources}
                title={hasProcessResources
                  ? 'Show optional CPU and resident-memory samples.'
                  : 'CPU and memory samples are not available.'}
              >
                <input
                  type="checkbox"
                  checked={showProcessResourceColumns}
                  disabled={!hasProcessResources}
                  aria-controls="process-io-table"
                  onchange={toggleProcessResources}
                />
                CPU &amp; memory
              </label>
            </div>
          </div>

          {#if snapshot.process_io?.state === 'ready' && externalProcessIo.length > 0}
            <div
              id="process-io-table"
              class="table-wrap process-table-wrap"
              class:stale={processIoStale}
              role="region"
              aria-label="External process disk I/O"
            >
              <table class="process-table" class:show-resources={showProcessResourceColumns}>
                <caption class="visually-hidden">
                  External processes ordered by combined filesystem read and write rate.
                </caption>
                <thead>
                  <tr>
                    <th scope="col">Process</th>
                    <th
                      scope="col"
                      aria-sort="descending"
                      title="Combined process filesystem read and write rate across all storage, not archive-device attribution."
                    >Disk I/O (MiB/s)</th>
                    {#if showProcessResourceColumns}
                      <th scope="col" title="Top-style CPU usage; 100% is one logical core and multithreaded processes may exceed it.">CPU</th>
                      <th scope="col">RSS</th>
                    {/if}
                  </tr>
                </thead>
                <tbody>
                  {#each externalProcessIo as process (process.id)}
                    {@const readRate = processMetric(process.read_mib_per_sec)}
                    {@const writeRate = processMetric(process.write_mib_per_sec)}
                    <tr>
                      <td>
                        <div class="process-name">
                          <strong>{process.name}</strong>
                          <span>PID {process.pid}{process.user ? ` · ${process.user}` : ''}</span>
                        </div>
                      </td>
                      <td class="process-io-cell">
                        <span class="io-rate-pair" aria-label={processIoRateAriaLabel(process)}>
                          <span aria-hidden="true">R {readRate === null ? '—' : formatDecimal(readRate)}</span>
                          <span aria-hidden="true">W {writeRate === null ? '—' : formatDecimal(writeRate)}</span>
                        </span>
                      </td>
                      {#if showProcessResourceColumns}
                        <td class="numeric-cell">{formatProcessCpu(process.cpu_percent)}</td>
                        <td class="numeric-cell">{formatBytes(processMetric(process.rss_bytes))}</td>
                      {/if}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {:else}
            <p class="process-empty" aria-live="polite">{processIoEmptyMessage(snapshot.process_io)}</p>
          {/if}

          {#if snapshot.process_io && (snapshot.process_io.inaccessible_count > 0 || snapshot.process_io.truncated)}
            <p class="process-note">
              {#if snapshot.process_io.truncated}
                Showing the {externalProcessIo.length} busiest of {snapshot.process_io.active_count} active external processes.
              {/if}
              {#if snapshot.process_io.inaccessible_count > 0}
                {snapshot.process_io.inaccessible_count} {snapshot.process_io.inaccessible_count === 1 ? 'process was' : 'processes were'} inaccessible.
              {/if}
            </p>
          {/if}
        </section>
      </div>
    </main>
  {:else}
    <main class="loading" aria-live="polite">
      <h2>Waiting for watcher status</h2>
      <p>{connectionMessage}</p>
    </main>
  {/if}
</div>

<style>
  .shell {
    min-height: calc(100vh - 54px);
  }

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

  h2,
  p {
    margin: 0;
  }

  h2 {
    font-size: 14px;
    font-weight: 650;
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

  .danger {
    color: var(--red) !important;
  }

  .epoch-detail-close {
    min-height: 28px;
    padding: 0 9px;
    border: 1px solid var(--border-strong);
    border-radius: 5px;
    background: #202023;
    color: #d8d8dc;
    font-size: 11px;
    cursor: pointer;
  }

  .epoch-detail-close:hover:not(:disabled) {
    border-color: #62626a;
    background: #28282c;
  }

  main {
    width: min(1600px, 100%);
    margin: 0 auto;
    padding: 18px 24px 32px;
  }

  .priority-panel {
    overflow: hidden;
    margin-bottom: 14px;
    border: 1px solid var(--border-strong);
    border-radius: 8px;
    background: var(--surface);
    font-variant-numeric: tabular-nums;
  }

  .priority-summary {
    display: grid;
    grid-template-columns: minmax(190px, 1.4fr) repeat(3, minmax(90px, 0.7fr));
    border-bottom: 1px solid var(--border);
  }

  .priority-summary > div {
    min-width: 0;
    display: grid;
    align-content: center;
    gap: 2px;
    min-height: 58px;
    padding: 9px 13px;
    border-right: 1px solid var(--border);
  }

  .priority-summary > div:last-child {
    border-right: 0;
  }

  .priority-summary span,
  .work-kind,
  .work-metric > span {
    color: var(--muted);
    font-size: 10px;
  }

  .priority-summary strong {
    overflow: hidden;
    color: var(--text);
    font-size: 14px;
    font-weight: 620;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .queue-eta-primary strong {
    color: #f3f3f4;
    font-size: 20px;
    letter-spacing: -0.02em;
  }

  .work-lines {
    display: grid;
  }

  .work-line {
    min-width: 0;
    min-height: 60px;
    display: grid;
    grid-template-columns: minmax(108px, 0.85fr) minmax(120px, 0.9fr) minmax(190px, 1.8fr) minmax(100px, 0.8fr) minmax(140px, 1fr);
    align-items: center;
    gap: 12px;
    padding: 9px 13px;
    border-bottom: 1px solid #29292d;
  }

  .work-line:last-child {
    border-bottom: 0;
  }

  .live-work-line {
    background: #181d1c;
  }

  .work-identity,
  .work-metric {
    min-width: 0;
    display: grid;
    gap: 2px;
  }

  .work-identity strong,
  .work-metric strong {
    overflow: hidden;
    font-size: 12px;
    font-weight: 590;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .work-kind {
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  .work-progress {
    min-width: 0;
    display: grid;
    grid-template-columns: minmax(90px, 1fr);
    align-items: center;
    gap: 8px;
    color: var(--muted);
    font-size: 11px;
  }

  .work-eta strong {
    color: var(--status-active-accent);
    font-size: 14px;
  }

  .idle-work-line {
    grid-template-columns: 1fr;
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

  .operations-alerts {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 5px 18px;
    margin: -2px 0 14px;
    padding: 8px 10px;
    border: 1px solid #775235;
    border-radius: 5px;
    background: #2b211a;
    color: #d7b78f;
    font-size: 12px;
  }

  .operations-alerts strong {
    color: #efd0a8;
    font-weight: 620;
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

  .section-heading {
    min-height: 50px;
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

  .legend-swatch {
    width: 13px;
    height: 5px;
    border: 0;
    border-radius: 1px;
    background: var(--tone-bg);
    box-shadow: inset 0 -2px var(--tone-accent);
    font-style: normal;
  }

  .epoch-calendar {
    max-height: 540px;
    overflow-y: auto;
    background: #141416;
  }

  .epoch-month {
    display: grid;
    grid-template-columns: 82px minmax(0, 1fr);
    gap: 12px;
    padding: 9px 14px;
    border-bottom: 1px solid var(--border);
  }

  .epoch-month:last-child {
    border-bottom: 0;
  }

  .epoch-month h3 {
    margin: 0;
    padding-top: 6px;
    color: #d0d0d4;
    font-size: 11px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }

  .epoch-month h3.estimated {
    color: var(--muted);
  }

  .epoch-month-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, 48px);
    gap: 3px;
    justify-content: start;
  }

  .epoch-cell {
    min-width: 0;
    height: 36px;
    display: grid;
    place-items: center;
    grid-template-rows: auto auto;
    align-content: center;
    gap: 2px;
    padding: 3px;
    border: 1px solid #35363b;
    border-bottom: 2px solid var(--tone-accent);
    border-radius: 3px;
    background: var(--tone-bg);
    color: #f4f4f5;
    font-family: ui-monospace, "SFMono-Regular", Consolas, monospace;
    font-size: 9px;
    font-variant-numeric: tabular-nums;
    line-height: 1;
    cursor: pointer;
  }

  .epoch-cell > b {
    overflow: hidden;
    font-weight: 560;
    text-align: center;
    text-overflow: clip;
  }

  .epoch-cell time,
  .epoch-day-unavailable {
    color: #d0d0d4;
    font-size: 9px;
    font-weight: 450;
  }

  .epoch-cell time.estimated,
  .epoch-day-unavailable {
    color: var(--muted);
  }

  .epoch-cell:hover,
  .epoch-cell.selected {
    outline: 1px solid #f4f4f5;
    outline-offset: -2px;
  }

  .epoch-cell.current {
    box-shadow: 0 0 0 1px var(--status-active-accent);
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

  .epoch-inspection {
    border-top: 1px solid var(--border);
    background: #141416;
  }

  .epoch-inspection > summary {
    padding: 8px 14px;
    color: var(--muted);
    font-size: 11px;
    cursor: pointer;
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

  .two-column {
    margin-top: 14px;
  }

  .disclosure-panel {
    overflow: hidden;
    margin-top: 14px;
  }

  .disclosure-panel > summary {
    min-height: 48px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 20px;
    padding: 9px 14px;
    color: var(--muted);
    cursor: pointer;
    list-style: none;
  }

  .disclosure-panel > summary::-webkit-details-marker {
    display: none;
  }

  .disclosure-panel > summary::after {
    content: '+';
    color: var(--faint);
    font-size: 16px;
  }

  .disclosure-panel[open] > summary {
    border-bottom: 1px solid var(--border);
  }

  .disclosure-panel[open] > summary::after {
    content: '−';
  }

  .disclosure-panel > summary strong {
    color: var(--text);
    font-size: 13px;
    font-weight: 620;
  }

  .disclosure-panel > summary > b {
    margin-left: auto;
    color: var(--muted);
    font-size: 11px;
    font-weight: 520;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }

  .disclosure-panel .two-column {
    gap: 0;
    margin-top: 0;
  }

  .disclosure-panel .two-column > section + section {
    border-left: 1px solid var(--border);
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

  .system-monitor {
    margin-top: 14px;
  }

  .system-monitor .lower-grid {
    margin-top: 0;
  }

  .process-panel {
    overflow: hidden;
    margin-top: 14px;
  }

  .process-controls,
  .process-option {
    display: flex;
    align-items: center;
  }

  .process-controls {
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: 8px 16px;
  }

  .process-controls > span {
    color: var(--muted);
    font-size: 11px;
    font-variant-numeric: tabular-nums;
  }

  .process-controls > span.stale {
    color: var(--amber);
  }

  .process-option {
    gap: 6px;
    color: #d4d4d8;
    font-size: 11px;
    white-space: nowrap;
    cursor: pointer;
  }

  .process-option.disabled {
    color: var(--faint);
    cursor: not-allowed;
  }

  .process-option input {
    width: 14px;
    height: 14px;
    margin: 0;
    accent-color: var(--green);
  }

  .process-option input:focus-visible {
    outline: 2px solid #5f8f7a;
    outline-offset: 2px;
  }

  .process-table-wrap.stale {
    opacity: 0.62;
  }

  .process-table {
    min-width: 430px;
  }

  .process-table.show-resources {
    min-width: 620px;
  }

  .process-table th:nth-child(n + 2),
  .process-table td:nth-child(n + 2) {
    text-align: right;
  }

  .process-table th:nth-child(2),
  .process-table td:nth-child(2) {
    width: 190px;
  }

  .process-table .io-rate-pair {
    max-width: 170px;
    margin-left: auto;
    text-align: right;
  }

  .process-name {
    min-width: 190px;
    display: grid;
    gap: 2px;
  }

  .process-name strong {
    overflow: hidden;
    color: var(--text);
    font-weight: 560;
    text-overflow: ellipsis;
  }

  .process-name span {
    color: var(--faint);
    font-size: 10px;
  }

  .numeric-cell {
    width: 110px;
  }

  .process-empty {
    padding: 18px 14px;
    color: var(--faint);
    font-size: 12px;
  }

  .process-note {
    padding: 7px 14px;
    border-top: 1px solid var(--border);
    color: var(--faint);
    font-size: 10px;
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

  @media (max-width: 1050px) {
    .two-column,
    .lower-grid {
      grid-template-columns: 1fr;
    }

    .disclosure-panel .two-column > section + section {
      border-top: 1px solid var(--border);
      border-left: 0;
    }

  }

  @media (max-width: 700px) {
    main {
      padding: 12px 10px 24px;
    }

    .priority-summary {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .queue-eta-primary {
      grid-column: 1 / -1;
    }

    .priority-summary > div:nth-child(3) {
      border-right: 0;
    }

    .priority-summary > div:last-child {
      grid-column: 1 / -1;
    }

    .work-line {
      grid-template-columns: minmax(110px, 1fr) minmax(120px, 1fr);
      gap: 8px 12px;
    }

    .work-progress {
      grid-column: 1 / -1;
    }

    .work-eta {
      grid-column: span 2;
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

    .process-heading {
      display: block;
    }

    .process-controls {
      justify-content: space-between;
      margin-top: 8px;
    }

    .epoch-calendar {
      max-height: 520px;
    }

    .epoch-month {
      display: block;
      padding: 9px 10px;
    }

    .epoch-month h3 {
      margin-bottom: 7px;
      padding-top: 0;
    }

    .epoch-month-grid {
      grid-template-columns: repeat(auto-fill, 46px);
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
