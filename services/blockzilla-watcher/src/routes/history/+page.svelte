<script lang="ts">
  import CompactionHistory from '$lib/CompactionHistory.svelte';
  import { selectCompactionHistory } from '$lib/compaction-history';
  import { useWatcherClient } from '$lib/watcher-client.svelte';

  const watcher = useWatcherClient();
  const entries = $derived(
    selectCompactionHistory(
      watcher.snapshot?.recent_compactions,
      watcher.snapshot?.epochs ?? []
    )
  );
</script>

<svelte:head>
  <title>History · Blockzilla Watcher</title>
  <meta
    name="description"
    content="Completed Blockzilla archive compactions and recorded run durations."
  />
</svelte:head>

{#if watcher.snapshot}
  <main>
    <CompactionHistory {entries} />
  </main>
{:else}
  <main class="loading" aria-live="polite">
    <h2>Waiting for watcher status</h2>
    <p>{watcher.connectionMessage}</p>
  </main>
{/if}

<style>
  main {
    width: min(1200px, 100%);
    margin: 0 auto;
    padding: 18px 24px 32px;
  }

  .loading {
    min-height: calc(100vh - 54px);
    display: grid;
    place-content: center;
    gap: 6px;
    color: var(--muted);
    text-align: center;
  }

  .loading h2,
  .loading p {
    margin: 0;
  }

  .loading h2 {
    color: var(--text);
    font-size: 14px;
  }

  @media (max-width: 700px) {
    main {
      padding: 12px 10px 24px;
    }
  }
</style>
