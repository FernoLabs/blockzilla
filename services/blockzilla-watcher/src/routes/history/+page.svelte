<script lang="ts">
  import CompactionHistory from '$lib/CompactionHistory.svelte';
  import ServiceUnavailable from '$lib/ServiceUnavailable.svelte';
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
  <ServiceUnavailable
    connectionState={watcher.connectionState}
    connectionMessage={watcher.connectionMessage}
  />
{/if}

<style>
  main {
    width: min(1200px, 100%);
    margin: 0 auto;
    padding: 18px 24px 32px;
  }

  @media (max-width: 700px) {
    main {
      padding: 12px 10px 24px;
    }
  }
</style>
