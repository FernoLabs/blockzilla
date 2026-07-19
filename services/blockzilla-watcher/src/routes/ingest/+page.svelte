<script lang="ts">
  import { onMount } from 'svelte';
  import { formatStorageBytes } from '$lib/format';
  import {
    ingestPipelineStatusIsFresh,
    type IngestOverallState
  } from '$lib/ingest-pipeline-status';
  import { runtimeOperationsIsFresh } from '$lib/runtime-operations';
  import { useWatcherClient } from '$lib/watcher-client.svelte';

  type Tone = 'healthy' | 'warning' | 'error' | 'unknown';
  type Stage = {
    id: string;
    name: string;
    role: string;
    tone: Tone;
    state: string;
    primary: string;
    secondary: string;
    updatedUnixSecs: number | null;
  };

  const watcher = useWatcherClient();
  let nowUnixSecs = $state(Math.floor(Date.now() / 1_000));
  const status = $derived(watcher.ingestPipeline);
  const fresh = $derived(
    status !== null &&
      watcher.ingestFeedState === 'live' &&
      ingestPipelineStatusIsFresh(status, nowUnixSecs)
  );
  const activeIncidents = $derived(
    status?.incidents.filter((incident) => incident.resolved_unix_secs === null) ?? []
  );
  const watcherSnapshotFresh = $derived(
    watcher.snapshot !== null &&
      watcher.snapshot.now_unix_secs <= nowUnixSecs + 5 &&
      nowUnixSecs <= watcher.snapshot.now_unix_secs + 20
  );
  const currentIndexer = $derived(
    watcherSnapshotFresh
      ? watcher.snapshot?.live.find((capture) =>
          capture.is_current === true &&
          capture.state === 'capturing' &&
          capture.last_slot !== null &&
          capture.updated_unix_secs <= nowUnixSecs + 5 &&
          nowUnixSecs <= capture.updated_unix_secs + 20
        ) ?? null
      : null
  );
  const runtimeFallback = $derived(
    watcher.runtimeOperations && runtimeOperationsIsFresh(watcher.runtimeOperations, nowUnixSecs)
      ? watcher.runtimeOperations.live_capture
      : null
  );
  const stages = $derived.by<Stage[]>(() => {
    if (!status) return [];
    const rows: Stage[] = [
      {
        id: 'upstream',
        name: 'Triton ingest',
        role: 'Block arrival observed at Hetzner',
        tone: toneForState(status.upstream.state),
        state: status.upstream.state === 'connected' ? 'data arriving' : labelState(status.upstream.state),
        primary: status.upstream.state === 'connected'
          ? throughSlotLabel('Blocks observed', status.recorder.durable_slot)
          : 'No recent block arrival',
        secondary: status.upstream.reconnects_1h === null
          ? 'Socket reconnect count not sampled'
          : `${formatInteger(status.upstream.reconnects_1h)} reconnects in 1h`,
        updatedUnixSecs: status.upstream.updated_unix_secs
      },
      {
        id: 'recorder',
        name: 'Hetzner WAL',
        role: 'Durable raw block and entry capture',
        tone: toneForState(status.recorder.state),
        state: labelState(status.recorder.state),
        primary: throughSlotLabel('Saved', status.recorder.durable_slot),
        secondary: `${formatStorageBytes(status.recorder.active_bytes)} active · ${formatStorageBytes(status.recorder.disk_free_bytes)} free`,
        updatedUnixSecs: status.recorder.updated_unix_secs
      },
      {
        id: 'replication',
        name: 'Blockzilla ACK',
        role: 'Signed durable receiver confirmation',
        tone: toneForState(status.replication.state),
        state: labelState(status.replication.state),
        primary: status.replication.ack_slot !== null
          ? `Received through slot ${formatInteger(status.replication.ack_slot)}`
          : status.replication.ack_through_sequence === null
            ? 'No signed ACK'
            : `Received through sequence ${formatInteger(status.replication.ack_through_sequence)}`,
        secondary: recordLagLabel(status.replication.lag_records),
        updatedUnixSecs: status.replication.updated_unix_secs
      },
      {
        id: 'indexer',
        name: 'Blockzilla indexer',
        role: 'Normalized block processing',
        tone: currentIndexer ? 'healthy' : toneForState(status.indexer.state),
        state: currentIndexer ? 'indexing' : labelState(status.indexer.state),
        primary: throughSlotLabel('Indexed', currentIndexer?.last_slot ?? status.indexer.last_slot),
        secondary: currentIndexer && status.recorder.durable_slot !== null && currentIndexer.last_slot !== null
          ? lagLabel(Math.max(0, status.recorder.durable_slot - currentIndexer.last_slot))
          : lagLabel(status.indexer.lag_slots),
        updatedUnixSecs: currentIndexer?.updated_unix_secs ?? status.indexer.updated_unix_secs
      },
      {
        id: 'object-store',
        name: 'Cloudflare R2',
        role: 'Pressure-path archive backup',
        tone: toneForState(status.object_store.state),
        state: labelState(status.object_store.state),
        primary: status.object_store.committed_bytes === null
          ? 'R2 usage not verified'
          : `${formatStorageBytes(status.object_store.committed_bytes)} verified`,
        secondary: status.object_store.state === 'unavailable'
          ? 'Uploader telemetry unavailable'
          : status.object_store.pending_bytes > 0
            ? `${formatStorageBytes(status.object_store.pending_bytes)} waiting to upload`
            : 'No upload waiting',
        updatedUnixSecs: status.object_store.updated_unix_secs
      }
    ];
    if (fresh) return rows;
    return rows.map((stage) =>
      stage.id === 'indexer' && currentIndexer
        ? stage
        : { ...stage, tone: 'unknown', state: 'status stale' }
    );
  });
  const overallTone = $derived.by<Tone>(() => {
    if (!status) return 'unknown';
    const hasLastKnownFailure =
      status.overall_state === 'failed' ||
      activeIncidents.some((incident) => ['error', 'critical'].includes(incident.severity)) ||
      status.gaps.some((gap) => gap.coverage === 'unproven');
    // Staleness makes a healthy-looking sample uncertain, but it must never
    // downgrade the last known failed state while telemetry is unavailable.
    if (!fresh) return hasLastKnownFailure ? 'error' : 'warning';
    if (hasLastKnownFailure) {
      return 'error';
    }
    const stageTones = stages
      .filter((stage) => stage.id !== 'object-store' || stage.tone !== 'unknown')
      .map((stage) => stage.tone);
    const tones = [
      toneForOverall(status.overall_state),
      activeIncidents.some((incident) => incident.severity === 'warning') ? 'warning' : 'healthy',
      status.gaps.some((gap) => gap.coverage !== 'raw') ? 'warning' : 'healthy',
      ...stageTones
    ];
    if (tones.includes('error')) return 'error';
    if (tones.includes('warning') || tones.includes('unknown')) return 'warning';
    return 'healthy';
  });
  const overallLabel = $derived(
    overallTone === 'healthy'
      ? 'healthy'
      : overallTone === 'error'
        ? 'failed'
        : overallTone === 'warning'
          ? 'degraded'
          : 'unknown'
  );
  const overallDisplayLabel = $derived(
    !status
      ? 'status unavailable'
      : fresh
        ? overallLabel
        : overallTone === 'error'
          ? 'failed · status stale'
          : 'status stale'
  );

  onMount(() => {
    const timer = window.setInterval(() => {
      nowUnixSecs = Math.floor(Date.now() / 1_000);
    }, 5_000);
    return () => window.clearInterval(timer);
  });

  function toneForOverall(state: IngestOverallState): Tone {
    if (state === 'healthy') return 'healthy';
    if (state === 'degraded') return 'warning';
    if (state === 'failed') return 'error';
    return 'unknown';
  }

  function toneForState(state: string): Tone {
    if (['connected', 'recording', 'caught_up', 'indexing', 'standby', 'capturing'].includes(state)) return 'healthy';
    if (['reconnecting', 'syncing', 'paused', 'uploading', 'degraded'].includes(state)) return 'warning';
    if (state === 'stalled') return 'error';
    return 'unknown';
  }

  function labelState(state: string) {
    const labels: Record<string, string> = {
      connected: 'connected',
      reconnecting: 'reconnecting',
      recording: 'recording',
      caught_up: 'caught up',
      syncing: 'catching up',
      indexing: 'indexing',
      standby: 'standby',
      uploading: 'uploading',
      capturing: 'capturing',
      paused: 'paused',
      degraded: 'degraded',
      stalled: 'stalled',
      unavailable: 'unknown'
    };
    return labels[state] ?? state.replaceAll('_', ' ');
  }

  function formatInteger(value: number | null | undefined) {
    return value === null || value === undefined ? '—' : value.toLocaleString('en-US');
  }

  function slotLabel(slot: number | null) {
    return slot === null ? 'Slot unknown' : `Slot ${formatInteger(slot)}`;
  }

  function throughSlotLabel(action: string, slot: number | null) {
    return slot === null ? `${action} slot unknown` : `${action} through slot ${formatInteger(slot)}`;
  }

  function lagLabel(lag: number | null) {
    if (lag === null) return 'Lag unknown';
    if (lag === 0) return 'No lag';
    return `${formatInteger(lag)} ${lag === 1 ? 'slot' : 'slots'} behind`;
  }

  function recordLagLabel(lag: number | null) {
    if (lag === null) return 'ACK lag unknown';
    if (lag === 0) return 'No unacknowledged records';
    return `${formatInteger(lag)} unacknowledged ${lag === 1 ? 'record' : 'records'}`;
  }

  function fallbackState() {
    return runtimeFallback?.state ?? status?.fallback.state ?? 'unavailable';
  }

  function fallbackSlot() {
    return runtimeFallback?.last_slot ?? status?.fallback.last_slot ?? null;
  }

  function fallbackLag() {
    if (runtimeFallback && status?.recorder.durable_slot !== null && status?.recorder.durable_slot !== undefined) {
      return Math.max(0, status.recorder.durable_slot - runtimeFallback.last_slot);
    }
    return status?.fallback.lag_slots ?? null;
  }

  function ageLabel(timestamp: number | null) {
    if (timestamp === null) return 'never updated';
    const seconds = Math.max(0, nowUnixSecs - timestamp);
    if (seconds < 10) return 'updated now';
    if (seconds < 60) return `updated ${seconds}s ago`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `updated ${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    return `updated ${hours}h ago`;
  }

  function coverageLabel(coverage: string) {
    const labels: Record<string, string> = {
      raw: 'exact raw copy',
      normalized: 'normalized blocks',
      rpc_recoverable: 'block data recoverable',
      unproven: 'coverage unproven'
    };
    return labels[coverage] ?? coverage;
  }

  function incidentSummary(id: string) {
    const summaries: Record<string, string> = {
      grpc_stale: 'No new gRPC block is being saved.',
      upstream_access_blocked: 'The gRPC provider is rejecting access.',
      replay_gap: 'The provider could not replay a requested slot.',
      resume_coverage: 'A reconnect has not yet proven continuous coverage.',
      replay_recovery_failed: 'The recorder could not complete replay recovery.',
      disk_space: 'Hetzner backup disk space is low.',
      disk_check_failed: 'Hetzner disk safety could not be checked.',
      volume_invalid: 'The recorder data volume failed validation.',
      cache_rotation_failed: 'The recorder could not rotate its active generation.',
      generation_rotation_failed: 'The recorder could not rotate its active generation.',
      generation_backlog: 'Saved generations are waiting for safe retirement.',
      object_store: 'The object-store backup needs attention.',
      receiver_ack_stale: 'Blockzilla durable ACK stopped advancing.'
    };
    return summaries[id] ?? 'The ingest pipeline reported an operational incident.';
  }
</script>

<svelte:head>
  <title>Ingest · Blockzilla Watcher</title>
  <meta
    name="description"
    content="Live status for the Blockzilla gRPC capture, replication, indexing, and backup pipeline."
  />
</svelte:head>

<main>
  <div class="page-heading">
    <div>
      <h2>Ingest pipeline</h2>
      <p>Triton capture through durable Blockzilla receipt and indexing.</p>
    </div>
    <div class={`overall tone-${overallTone}`}>
      <span class="status-mark" aria-hidden="true"></span>
      <span>
        <strong role="status" aria-live="polite">
          {overallDisplayLabel}
        </strong>
        <small aria-hidden="true">{status ? ageLabel(status.updated_unix_secs) : watcher.ingestFeedMessage}</small>
      </span>
    </div>
  </div>

  {#if status}
    {#if !fresh}
      <div class="feed-warning" role="alert">
        The live status feed is unavailable. Showing the last valid sample; current state cannot be confirmed.
      </div>
    {/if}

    <ol class="pipeline" aria-label="Ingest stages">
      {#each stages as stage (stage.id)}
        <li>
          <div class="stage-name">
            <strong>{stage.name}</strong>
            <span>{stage.role}</span>
          </div>
          <span class={`stage-state tone-${stage.tone}`}>
            <span class="status-mark" aria-hidden="true"></span>
            {stage.state}
          </span>
          <div class="stage-value">
            <strong>{stage.primary}</strong>
            <span>{stage.secondary}</span>
          </div>
          <time datetime={stage.updatedUnixSecs ? new Date(stage.updatedUnixSecs * 1_000).toISOString() : undefined}>
            {ageLabel(stage.updatedUnixSecs)}
          </time>
        </li>
      {/each}
    </ol>

    <div class="lower-sections">
      <section aria-labelledby="retention-title">
        <div class="section-heading">
          <h3 id="retention-title">Retention and fallback</h3>
        </div>
        <dl class="facts">
          <div>
            <dt>Hetzner disk</dt>
            <dd>{formatStorageBytes(status.recorder.disk_free_bytes)} free of {formatStorageBytes(status.recorder.disk_total_bytes)}</dd>
          </div>
          <div>
            <dt>Local backup backlog</dt>
            <dd>{formatStorageBytes(status.recorder.unacknowledged_bytes)} unacknowledged · {formatInteger(status.recorder.sealed_generations)} sealed generations</dd>
          </div>
          <div>
            <dt>NAS raw fallback</dt>
            <dd>
              {labelState(fallbackState())} · {slotLabel(fallbackSlot())} · {lagLabel(fallbackLag())}
            </dd>
          </div>
          <div>
            <dt>R2 pending</dt>
            <dd>{status.object_store.state === 'unavailable' ? 'Not verified' : formatStorageBytes(status.object_store.pending_bytes)}</dd>
          </div>
        </dl>
      </section>

      <section aria-labelledby="continuity-title">
        <div class="section-heading">
          <h3 id="continuity-title">Continuity</h3>
          <span>{status.gaps_truncated ? `${status.gaps.length}+` : status.gaps.length} known {status.gaps.length === 1 && !status.gaps_truncated ? 'window' : 'windows'}</span>
        </div>
        {#if status.gaps.length > 0}
          <ul class="gap-list">
            {#each status.gaps as gap (`${gap.from_slot}-${gap.to_slot}`)}
              <li>
                <span>
                  <strong>{formatInteger(gap.from_slot)}–{formatInteger(gap.to_slot)}</strong>
                  <small>{coverageLabel(gap.coverage)}</small>
                </span>
                <b>{gap.produced_blocks === null ? 'unknown block count' : `${formatInteger(gap.produced_blocks)} blocks`}</b>
              </li>
            {/each}
          </ul>
        {:else}
          <p class="empty">No known continuity gap is reported.</p>
        {/if}
      </section>
    </div>

    <section class="incidents" aria-labelledby="incidents-title">
      <div class="section-heading">
        <h3 id="incidents-title">Active incidents</h3>
        <span>{activeIncidents.length}</span>
      </div>
      {#if activeIncidents.length > 0}
        <ul>
          {#each activeIncidents as incident (incident.id)}
            <li>
              <span class={`incident-severity severity-${incident.severity}`}>{incident.severity}</span>
              <strong>{incidentSummary(incident.id)}</strong>
              <time datetime={new Date(incident.started_unix_secs * 1_000).toISOString()}>
                opened {ageLabel(incident.started_unix_secs).replace('updated ', '')}
              </time>
            </li>
          {/each}
        </ul>
      {:else}
        <p class="empty">No active ingest incident.</p>
      {/if}
    </section>
  {:else}
    <section class="unavailable" aria-labelledby="unavailable-title">
      <h3 id="unavailable-title">Ingest status is not available yet</h3>
      <p>{watcher.ingestFeedMessage}</p>
      <p>The archive watcher remains separate; this page never uses browser-held Dokploy or infrastructure credentials.</p>
    </section>
  {/if}
</main>

<style>
  main {
    width: min(1180px, 100%);
    margin: 0 auto;
    padding: 24px 24px 40px;
  }

  .page-heading,
  .section-heading,
  .overall,
  .stage-state {
    display: flex;
    align-items: center;
  }

  .page-heading {
    justify-content: space-between;
    gap: 24px;
    margin-bottom: 18px;
  }

  h2,
  h3,
  p {
    margin: 0;
  }

  h2 {
    font-size: 20px;
    font-weight: 680;
    letter-spacing: -0.02em;
  }

  h3 {
    font-size: 13px;
    font-weight: 650;
  }

  .page-heading p,
  .stage-name span,
  .stage-value span,
  time,
  .section-heading > span,
  .empty,
  .unavailable p {
    color: var(--muted);
  }

  .page-heading p {
    margin-top: 4px;
    font-size: 13px;
  }

  .overall {
    flex: 0 0 auto;
    gap: 9px;
    min-width: 170px;
    justify-content: flex-end;
    text-align: right;
  }

  .overall > span:last-child {
    display: grid;
    gap: 1px;
  }

  .overall strong {
    font-size: 13px;
    font-weight: 650;
  }

  .overall small,
  .gap-list small {
    color: var(--muted);
    font-size: 11px;
  }

  .status-mark {
    width: 8px;
    height: 8px;
    flex: 0 0 auto;
    border-radius: 2px;
    background: var(--slate);
  }

  .tone-healthy .status-mark {
    background: var(--green);
  }

  .tone-warning .status-mark {
    background: var(--amber);
  }

  .tone-error .status-mark {
    background: var(--red);
  }

  .feed-warning {
    margin-bottom: 12px;
    padding: 10px 12px;
    border: 1px solid #66512e;
    border-radius: 8px;
    background: #2a2419;
    color: #e1c58f;
    font-size: 12px;
  }

  .pipeline {
    margin: 0;
    padding: 0;
    border: 1px solid var(--border);
    border-radius: 10px;
    background: var(--surface);
    list-style: none;
  }

  .pipeline li {
    display: grid;
    grid-template-columns: minmax(190px, 1.1fr) 120px minmax(220px, 1fr) 105px;
    align-items: center;
    gap: 20px;
    min-height: 74px;
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
  }

  .pipeline li:last-child {
    border-bottom: 0;
  }

  .stage-name,
  .stage-value {
    display: grid;
    gap: 3px;
    min-width: 0;
  }

  .stage-name strong,
  .stage-value strong {
    font-size: 13px;
    font-weight: 640;
    font-variant-numeric: tabular-nums;
  }

  .stage-name span,
  .stage-value span,
  time {
    font-size: 11px;
  }

  .stage-state {
    gap: 7px;
    color: var(--text);
    font-size: 12px;
  }

  .pipeline time {
    text-align: right;
  }

  .lower-sections {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-top: 16px;
  }

  .lower-sections section,
  .incidents,
  .unavailable {
    border: 1px solid var(--border);
    border-radius: 10px;
    background: var(--surface);
  }

  .section-heading {
    justify-content: space-between;
    min-height: 43px;
    padding: 0 14px;
    border-bottom: 1px solid var(--border);
  }

  .section-heading > span {
    font-size: 11px;
  }

  .facts {
    margin: 0;
  }

  .facts > div {
    display: grid;
    grid-template-columns: 145px 1fr;
    gap: 16px;
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
  }

  .facts > div:last-child {
    border-bottom: 0;
  }

  dt {
    color: var(--muted);
    font-size: 11px;
  }

  dd {
    margin: 0;
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .gap-list,
  .incidents ul {
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .gap-list li,
  .incidents li {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    min-height: 48px;
    padding: 9px 14px;
    border-bottom: 1px solid var(--border);
  }

  .gap-list li:last-child,
  .incidents li:last-child {
    border-bottom: 0;
  }

  .gap-list li > span {
    display: grid;
    gap: 2px;
  }

  .gap-list strong,
  .gap-list b {
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .gap-list b {
    font-weight: 520;
  }

  .incidents {
    margin-top: 16px;
  }

  .incidents li {
    display: grid;
    grid-template-columns: 70px 1fr 120px;
  }

  .incidents li strong {
    font-size: 12px;
    font-weight: 560;
  }

  .incidents li time {
    text-align: right;
  }

  .incident-severity {
    color: var(--amber);
    font-size: 11px;
  }

  .severity-error,
  .severity-critical {
    color: var(--red);
  }

  .empty {
    padding: 16px 14px;
    font-size: 12px;
  }

  .unavailable {
    padding: 20px;
  }

  .unavailable h3 {
    margin-bottom: 7px;
  }

  .unavailable p {
    max-width: 660px;
    margin-top: 4px;
    font-size: 12px;
  }

  @media (max-width: 820px) {
    .pipeline li {
      grid-template-columns: 1fr 110px;
      gap: 8px 16px;
    }

    .stage-value {
      grid-column: 1;
    }

    .pipeline time {
      grid-column: 2;
      grid-row: 2;
    }

    .lower-sections {
      grid-template-columns: 1fr;
    }
  }

  @media (max-width: 600px) {
    main {
      padding: 16px 10px 28px;
    }

    .page-heading {
      align-items: flex-start;
    }

    .page-heading p {
      display: none;
    }

    .overall {
      min-width: 0;
    }

    .overall small {
      display: none;
    }

    .pipeline li {
      grid-template-columns: 1fr;
      gap: 7px;
      padding: 13px;
    }

    .stage-value,
    .pipeline time {
      grid-column: 1;
      grid-row: auto;
      text-align: left;
    }

    .facts > div {
      grid-template-columns: 1fr;
      gap: 3px;
    }

    .incidents li {
      grid-template-columns: 64px 1fr;
    }

    .incidents li time {
      grid-column: 2;
      text-align: left;
    }
  }
</style>
