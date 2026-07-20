<script lang="ts">
  import { onMount } from 'svelte';
  import { formatBytes } from '$lib/format';
  import {
    shredIngestStatusIsFresh,
    type ShredForwardingState,
    type ShredGossipState,
    type ShredTvuState,
    type HivezillaShredState
  } from '$lib/shred-ingest-status';
  import { useWatcherClient } from '$lib/watcher-client.svelte';

  type Tone = 'healthy' | 'warning' | 'error' | 'unknown';
  type Stage = {
    id: 'gossip' | 'tvu' | 'forwarding' | 'hivezilla';
    name: string;
    role: string;
    tone: Tone;
    state: string;
    primary: string;
    secondary: string;
    observedUnixSecs: number | null;
    timeKind: 'sampled' | 'last packet' | 'last durable write';
  };

  const watcher = useWatcherClient();
  let nowUnixSecs = $state(Math.floor(Date.now() / 1_000));
  const status = $derived(watcher.shredIngestStatus);
  const fresh = $derived(
    status !== null && shredIngestStatusIsFresh(status, nowUnixSecs)
  );
  const refreshRetrying = $derived(watcher.shredIngestFeedState === 'retrying');
  const evidenceCount = $derived.by(() => {
    if (!status) return 0;
    return [
      status.gossip.state === 'observed',
      (status.tvu.parsed_total ?? 0) > 0,
      (status.forwarding.attempts_total ?? 0) > 0,
      status.hivezilla.availability === 'available' &&
        status.hivezilla.durable_through_sequence !== null
    ].filter(Boolean).length;
  });
  const stages = $derived.by<Stage[]>(() => {
    if (!status) return [];
    const lastPacketUnixSecs = status.tvu.seconds_since_last_packet === null ||
      status.tvu.updated_unix_secs === null
      ? null
      : Math.max(0, status.tvu.updated_unix_secs - status.tvu.seconds_since_last_packet);
    const rows: Stage[] = [
      {
        id: 'gossip',
        name: 'Gossip observation',
        role: 'Recent validator records learned through Solana gossip',
        tone: gossipTone(status.gossip.state),
        state: gossipStateLabel(status.gossip.state),
        primary: status.gossip.state === 'observed'
          ? `${formatInteger(status.gossip.recent_peer_count)} recent peers observed`
          : status.gossip.state === 'waiting'
            ? 'No recent gossip peer observed'
            : 'Gossip metrics unavailable',
        secondary: status.gossip.known_peer_count === null
          ? 'Peer counts unavailable'
          : `${formatInteger(status.gossip.known_peer_count)} known · ${formatInteger(status.gossip.tvu_peer_count)} with TVU records`,
        observedUnixSecs: status.gossip.updated_unix_secs,
        timeKind: 'sampled'
      },
      {
        id: 'tvu',
        name: 'TVU shred receipt',
        role: 'Inbound datagrams and structurally valid Solana shreds',
        tone: tvuTone(status.tvu.state, status.tvu.parsed_total),
        state: tvuStateLabel(status.tvu.state, status.tvu.parsed_total),
        primary: status.tvu.parsed_total === null
          ? 'TVU counters unavailable'
          : status.tvu.parsed_total > 0
            ? `${formatInteger(status.tvu.parsed_total)} valid shreds parsed`
            : status.tvu.packets_total && status.tvu.packets_total > 0
              ? 'Datagrams observed; no valid shred parsed'
              : 'No TVU datagram observed yet',
        secondary: status.tvu.packets_total === null
          ? 'Receipt details unavailable'
          : `${formatInteger(status.tvu.packets_total)} datagrams · ${formatBytes(status.tvu.bytes_total)} · ${slotLabel(status.tvu.latest_slot)}`,
        observedUnixSecs: lastPacketUnixSecs,
        timeKind: 'last packet'
      },
      {
        id: 'forwarding',
        name: 'Loopback forwarding attempts',
        role: 'Local UDP send calls only; no downstream acknowledgement',
        tone: forwardingTone(status.forwarding.state),
        state: forwardingStateLabel(status.forwarding.state),
        primary: status.forwarding.successful_datagrams_total === null
          ? 'Forwarding counters unavailable'
          : status.forwarding.successful_datagrams_total > 0
            ? `${formatInteger(status.forwarding.successful_datagrams_total)} send attempts completed`
            : 'No completed send attempt yet',
        secondary: status.forwarding.target_count === null
          ? 'Target details unavailable'
          : `${formatInteger(status.forwarding.attempts_total)} total attempts across ${formatInteger(status.forwarding.target_count)} ${status.forwarding.target_count === 1 ? 'target' : 'targets'} · ${formatInteger(status.forwarding.errors_total)} local errors`,
        observedUnixSecs: status.forwarding.updated_unix_secs,
        timeKind: 'sampled'
      },
      {
        id: 'hivezilla',
        name: 'Hivezilla durable recording',
        role: 'Accepted datagrams fsynced into the shred ingress spool',
        tone: hivezillaTone(
          status.hivezilla.availability,
          status.hivezilla.status_fresh,
          status.hivezilla.state
        ),
        state: hivezillaStateLabel(
          status.hivezilla.availability,
          status.hivezilla.status_fresh,
          status.hivezilla.state
        ),
        primary: status.hivezilla.accepted_total === null
          ? 'Recorder status unavailable'
          : status.hivezilla.accepted_total > 0
            ? `${formatInteger(status.hivezilla.accepted_total)} datagrams durably recorded this process`
            : status.hivezilla.durable_through_sequence !== null
              ? 'Recovered a durable spool tail'
              : 'No shred durably recorded yet',
        secondary: status.hivezilla.durable_through_sequence === null
          ? 'No durable sequence reported'
          : `Durable through sequence ${formatInteger(status.hivezilla.durable_through_sequence)} · ${slotLabel(status.hivezilla.latest_slot)}`,
        observedUnixSecs: status.hivezilla.last_durable_unix_secs,
        timeKind: 'last durable write'
      }
    ];

    if (fresh) return rows;
    return rows.map((stage) => stage.tone === 'error'
      ? { ...stage, state: `${stage.state} · sample stale` }
      : { ...stage, tone: 'unknown', state: 'status stale' });
  });
  const overallTone = $derived.by<Tone>(() => {
    if (!status) return 'unknown';
    if (!fresh) return 'warning';
    if (stages.some((stage) => stage.tone === 'error')) return 'error';
    if (evidenceCount === 4 && stages.every((stage) => stage.tone === 'healthy')) {
      return 'healthy';
    }
    return 'warning';
  });
  const overallLabel = $derived(
    !status
      ? 'status unavailable'
      : !fresh
        ? 'status stale'
        : overallTone === 'error'
          ? 'attention needed'
          : `${evidenceCount}/4 boundaries observed`
  );

  onMount(() => {
    const timer = window.setInterval(() => {
      nowUnixSecs = Math.floor(Date.now() / 1_000);
    }, 5_000);
    return () => window.clearInterval(timer);
  });

  function gossipTone(state: ShredGossipState): Tone {
    if (state === 'observed') return 'healthy';
    if (state === 'waiting') return 'warning';
    return 'unknown';
  }

  function gossipStateLabel(state: ShredGossipState) {
    if (state === 'observed') return 'peers observed';
    if (state === 'waiting') return 'waiting for peers';
    return 'metrics unavailable';
  }

  function tvuTone(state: ShredTvuState, parsedTotal: number | null): Tone {
    if (state === 'receiving' && parsedTotal !== null && parsedTotal > 0) return 'healthy';
    if (state === 'unavailable') return 'unknown';
    return 'warning';
  }

  function tvuStateLabel(state: ShredTvuState, parsedTotal: number | null) {
    if (state === 'receiving' && parsedTotal !== null && parsedTotal > 0) {
      return 'shreds observed recently';
    }
    if (state === 'receiving') return 'datagrams observed';
    if (state === 'idle') return 'receipt idle';
    if (state === 'waiting') return 'waiting for shreds';
    return 'metrics unavailable';
  }

  function forwardingTone(state: ShredForwardingState): Tone {
    if (state === 'sending') return 'healthy';
    if (state === 'errors') return 'error';
    if (state === 'unavailable') return 'unknown';
    return 'warning';
  }

  function forwardingStateLabel(state: ShredForwardingState) {
    const labels: Record<ShredForwardingState, string> = {
      disabled: 'forwarding disabled',
      waiting: 'waiting for a send attempt',
      sending: 'send attempts observed',
      errors: 'local send errors',
      unavailable: 'metrics unavailable'
    };
    return labels[state];
  }

  function hivezillaTone(
    availability: 'available' | 'unavailable',
    statusFresh: boolean,
    state: HivezillaShredState
  ): Tone {
    if (availability === 'unavailable') return 'unknown';
    if (state === 'stalled' || state === 'stopped') return 'error';
    if (!statusFresh || state === 'waiting') return 'warning';
    return state === 'receiving' ? 'healthy' : 'unknown';
  }

  function hivezillaStateLabel(
    availability: 'available' | 'unavailable',
    statusFresh: boolean,
    state: HivezillaShredState
  ) {
    if (availability === 'unavailable') return 'recorder status unavailable';
    if (!statusFresh && state !== 'stalled' && state !== 'stopped') return 'recorder status stale';
    const labels: Record<HivezillaShredState, string> = {
      waiting: 'waiting for a valid shred',
      receiving: 'durable writes observed',
      stalled: 'durable writes stalled',
      stopped: 'recorder stopped',
      unavailable: 'recorder status unavailable'
    };
    return labels[state];
  }

  function formatInteger(value: number | null | undefined) {
    return value === null || value === undefined ? '—' : value.toLocaleString('en-US');
  }

  function slotLabel(slot: number | null) {
    return slot === null ? 'slot unknown' : `slot ${formatInteger(slot)}`;
  }

  function ageLabel(timestamp: number | null) {
    if (timestamp === null) return 'not observed';
    const seconds = Math.max(0, nowUnixSecs - timestamp);
    if (seconds < 10) return 'now';
    if (seconds < 60) return `${seconds}s ago`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    return `${Math.floor(minutes / 60)}h ago`;
  }
</script>

<svelte:head>
  <title>Shred ingest · Blockzilla Watcher</title>
  <meta
    name="description"
    content="Observed Solana gossip, TVU shred receipt, loopback forwarding attempts, and Hivezilla durable recording."
  />
</svelte:head>

<main>
  <div class="page-heading">
    <div>
      <h2>Shred ingest</h2>
      <p>Evidence at each boundary from Solana gossip to Hivezilla's durable spool.</p>
    </div>
    <div class={`overall tone-${overallTone}`}>
      <span class="status-mark" aria-hidden="true"></span>
      <span>
        <strong role="status" aria-live="polite">{overallLabel}</strong>
        <span class="status-age">
          {status ? `sampled ${ageLabel(status.updated_unix_secs)}` : watcher.shredIngestFeedMessage}
        </span>
      </span>
    </div>
  </div>

  {#if status}
    {#if !fresh}
      <div class="feed-warning" role="alert">
        The latest status timestamp is stale. Counters remain visible as last-known evidence, not current state.
      </div>
    {:else if refreshRetrying}
      <div class="feed-notice" role="status">
        The latest sample is still current. A refresh failed and will be retried.
      </div>
    {/if}

    <ol class="pipeline" aria-label="Shred ingest evidence boundaries">
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
          <time datetime={stage.observedUnixSecs ? new Date(stage.observedUnixSecs * 1_000).toISOString() : undefined}>
            {stage.timeKind} {ageLabel(stage.observedUnixSecs)}
          </time>
        </li>
      {/each}
    </ol>

    <p class="boundary-note">
      A completed loopback send means the local UDP send call accepted the datagram. It does not confirm Hivezilla receipt; only the durable recorder evidence does.
    </p>

    <div class="details">
      <section aria-labelledby="receiver-counters-title">
        <div class="section-heading">
          <h3 id="receiver-counters-title">Receiver counters</h3>
        </div>
        <dl>
          <div>
            <dt>Valid shreds</dt>
            <dd>{formatInteger(status.tvu.parsed_total)} parsed · {formatInteger(status.tvu.unique_total)} unique</dd>
          </div>
          <div>
            <dt>Shred kinds</dt>
            <dd>{formatInteger(status.tvu.data_total)} data · {formatInteger(status.tvu.code_total)} code</dd>
          </div>
          <div>
            <dt>Rejected datagrams</dt>
            <dd>{formatInteger(status.tvu.invalid_total)} invalid · {formatInteger(status.tvu.version_mismatch_total)} version mismatch</dd>
          </div>
          <div>
            <dt>Duplicates</dt>
            <dd>{formatInteger(status.tvu.duplicates_total)}</dd>
          </div>
        </dl>
      </section>

      <section aria-labelledby="recorder-capacity-title">
        <div class="section-heading">
          <h3 id="recorder-capacity-title">Durable recorder capacity</h3>
        </div>
        <dl>
          <div>
            <dt>Spool</dt>
            <dd>{formatBytes(status.hivezilla.spool_bytes)} of {formatBytes(status.hivezilla.spool_max_bytes)}</dd>
          </div>
          <div>
            <dt>Filesystem</dt>
            <dd>{formatBytes(status.hivezilla.filesystem_free_bytes)} free of {formatBytes(status.hivezilla.filesystem_total_bytes)}</dd>
          </div>
          <div>
            <dt>Free-space reserve</dt>
            <dd>{formatBytes(status.hivezilla.reserve_free_bytes)}</dd>
          </div>
          <div>
            <dt>Recorder shred version</dt>
            <dd>{formatInteger(status.hivezilla.shred_version)}</dd>
          </div>
        </dl>
      </section>
    </div>
  {:else}
    <section class="unavailable" aria-labelledby="unavailable-title">
      <h3 id="unavailable-title">Shred ingest status is not available yet</h3>
      <p>{watcher.shredIngestFeedMessage}</p>
      <p>No connectivity, forwarding, or durable-recording claim can be made without a valid status document.</p>
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
  .status-age,
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
    min-width: 190px;
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

  .status-age {
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

  .feed-warning,
  .feed-notice,
  .boundary-note {
    margin-bottom: 12px;
    padding: 10px 12px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface);
    color: var(--muted);
    font-size: 12px;
  }

  .feed-warning {
    border-color: #66512e;
    background: #2a2419;
    color: #e1c58f;
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
    grid-template-columns: minmax(210px, 1.1fr) 150px minmax(245px, 1fr) 145px;
    align-items: center;
    gap: 20px;
    min-height: 78px;
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

  .boundary-note {
    margin-top: 12px;
    line-height: 1.5;
  }

  .details {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-top: 16px;
  }

  .details section,
  .unavailable {
    border: 1px solid var(--border);
    border-radius: 10px;
    background: var(--surface);
  }

  .section-heading {
    min-height: 43px;
    padding: 0 14px;
    border-bottom: 1px solid var(--border);
  }

  dl {
    margin: 0;
  }

  dl > div {
    display: grid;
    grid-template-columns: 145px 1fr;
    gap: 16px;
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
  }

  dl > div:last-child {
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

  .unavailable {
    padding: 20px;
  }

  .unavailable h3 {
    margin-bottom: 7px;
  }

  .unavailable p {
    max-width: 720px;
    margin-top: 4px;
    font-size: 12px;
  }

  @media (max-width: 920px) {
    .pipeline li {
      grid-template-columns: 1fr 145px;
      gap: 8px 16px;
    }

    .stage-value {
      grid-column: 1;
    }

    .pipeline time {
      grid-column: 2;
      grid-row: 2;
    }
  }

  @media (max-width: 680px) {
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

    .details {
      grid-template-columns: 1fr;
    }
  }

  @media (max-width: 480px) {
    .pipeline li {
      grid-template-columns: 1fr;
      gap: 7px;
    }

    .stage-value,
    .pipeline time {
      grid-column: 1;
      grid-row: auto;
    }

    .pipeline time {
      text-align: left;
    }

    dl > div {
      grid-template-columns: 120px 1fr;
    }
  }
</style>
