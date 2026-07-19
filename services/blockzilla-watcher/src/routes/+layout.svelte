<script lang="ts">
  import { onMount } from 'svelte';
  import '../app.css';
  import WatcherHeader from '$lib/WatcherHeader.svelte';
  import {
    provideWatcherClient,
    WatcherClient
  } from '$lib/watcher-client.svelte';

  let { children } = $props();

  const watcher = new WatcherClient();
  provideWatcherClient(watcher);
  onMount(watcher.connect);
</script>

<div class="app-shell">
  <WatcherHeader
    connectionState={watcher.connectionState}
    connectionMessage={watcher.connectionMessage}
    observerMode={watcher.snapshot?.observer_mode ?? false}
  />
  {@render children()}
</div>

<style>
  .app-shell {
    min-height: 100vh;
  }
</style>
