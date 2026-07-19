<script lang="ts">
  import type { RecentCompaction } from '$lib/compaction-history';

  let { entries }: { entries: RecentCompaction[] } = $props();

  const latest = $derived(entries[0] ?? null);
  const showWorkflow = $derived(
    entries.some((entry) => entry.workflow !== entries[0]?.workflow)
  );

  function formatInteger(value: number) {
    return value.toLocaleString('en-US');
  }

  function formatDuration(value: number) {
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

  function formatClock(value: number) {
    return new Intl.DateTimeFormat(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    }).format(new Date(value * 1000));
  }

  function formatClockLabel(value: number) {
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: 'full',
      timeStyle: 'long'
    }).format(new Date(value * 1000));
  }

  function workflowLabel(value: RecentCompaction['workflow']) {
    if (value === 'recompact') return 'Recompact';
    if (value === 'live') return 'Live';
    return 'Historical';
  }
</script>

<section class="history-panel">
  <header>
    <div>
      <h2>Completed compactions</h2>
      <p>{formatInteger(entries.length)} {entries.length === 1 ? 'run' : 'runs'} available</p>
    </div>
    {#if latest}
      <p class="latest">
        {latest.completed_unix_secs === null ? 'Highest complete' : 'Latest'}
        · Epoch {formatInteger(latest.epoch)}
        · {latest.duration_secs === null ? 'timing unavailable' : formatDuration(latest.duration_secs)}
      </p>
    {/if}
  </header>

  {#if entries.length > 0}
    <div class="table-wrap">
      <table>
        <caption class="visually-hidden">Completed epoch compactions, newest first.</caption>
        <thead>
          <tr>
            <th scope="col">Epoch</th>
            {#if showWorkflow}<th scope="col">Type</th>{/if}
            <th scope="col">Completed</th>
            <th scope="col">Duration</th>
          </tr>
        </thead>
        <tbody>
          {#each entries as entry (entry.id)}
            <tr>
              <td><strong>{formatInteger(entry.epoch)}</strong></td>
              {#if showWorkflow}<td>{workflowLabel(entry.workflow)}</td>{/if}
              <td>
                {#if entry.completed_unix_secs !== null}
                  <time
                    datetime={new Date(entry.completed_unix_secs * 1000).toISOString()}
                    aria-label={formatClockLabel(entry.completed_unix_secs)}
                    title={formatClockLabel(entry.completed_unix_secs)}
                  >{formatClock(entry.completed_unix_secs)}</time>
                {:else}
                  <span class="unknown" aria-label="Completion time unavailable">—</span>
                {/if}
              </td>
              <td>
                {#if entry.duration_secs !== null}
                  {formatDuration(entry.duration_secs)}
                {:else}
                  <span class="unknown" aria-label="Duration unavailable">—</span>
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {:else}
    <p class="empty">No completed compactions recorded.</p>
  {/if}
</section>

<style>
  .history-panel {
    overflow: hidden;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface);
  }

  header {
    min-height: 58px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 24px;
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
  }

  h2,
  p {
    margin: 0;
  }

  h2 {
    font-size: 14px;
    font-weight: 650;
  }

  header div p,
  .latest {
    color: var(--muted);
    font-size: 11px;
    font-variant-numeric: tabular-nums;
  }

  .latest {
    text-align: right;
  }

  .table-wrap {
    max-height: calc(100vh - 150px);
    overflow: auto;
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
    padding: 7px 14px;
    border-bottom: 1px solid #29292d;
    text-align: left;
    white-space: nowrap;
  }

  th {
    position: sticky;
    z-index: 1;
    top: 0;
    background: #19191c;
    color: var(--muted);
    font-weight: 520;
  }

  tbody tr:last-child td {
    border-bottom: 0;
  }

  th:first-child,
  td:first-child {
    width: 110px;
  }

  th:last-child,
  td:last-child {
    width: 130px;
    text-align: right;
  }

  td strong {
    color: var(--text);
    font-weight: 580;
  }

  time {
    color: #d4d4d8;
  }

  .unknown {
    color: var(--faint);
  }

  .empty {
    padding: 22px 14px;
    color: var(--muted);
    font-size: 12px;
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

  @media (max-width: 700px) {
    header {
      display: block;
    }

    .latest {
      margin-top: 4px;
      text-align: left;
    }

    th,
    td {
      padding-right: 10px;
      padding-left: 10px;
    }
  }
</style>
