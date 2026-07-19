<script lang="ts">
  import { resolve } from '$app/paths';
  import { page } from '$app/state';

  type ConnectionState = 'connecting' | 'live' | 'retrying' | 'offline';

  let {
    connectionState,
    connectionMessage,
    observerMode
  }: {
    connectionState: ConnectionState;
    connectionMessage: string;
    observerMode: boolean;
  } = $props();

  const historyActive = $derived(page.url.pathname.startsWith('/history'));
</script>

<header class="topbar">
  <div class="identity">
    <h1><a href={resolve('/')}>Blockzilla Watcher</a></h1>
    <nav aria-label="Watcher pages">
      <a href={resolve('/')} aria-current={!historyActive ? 'page' : undefined}>Overview</a>
      <a href={resolve('/history')} aria-current={historyActive ? 'page' : undefined}>History</a>
    </nav>
    {#if observerMode}
      <span class="observer">observer mode</span>
    {/if}
  </div>

  <span
    class="connection"
    title={connectionMessage}
    role="status"
    aria-live="polite"
    aria-atomic="true"
  >
    <span class={`connection-mark connection-${connectionState}`} aria-hidden="true"></span>
    <span class="connection-label">{connectionState}</span>
  </span>
</header>

<style>
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
  nav,
  .connection {
    display: flex;
    align-items: center;
  }

  .identity {
    min-width: 0;
    gap: 18px;
  }

  h1 {
    margin: 0;
    font-size: 15px;
    font-weight: 680;
    letter-spacing: -0.01em;
    white-space: nowrap;
  }

  h1 a {
    color: var(--text);
    text-decoration: none;
  }

  nav {
    align-self: stretch;
    gap: 16px;
  }

  nav a {
    display: grid;
    align-items: center;
    border-bottom: 2px solid transparent;
    color: var(--muted);
    font-size: 12px;
    text-decoration: none;
  }

  nav a:hover {
    color: var(--text);
  }

  nav a[aria-current='page'] {
    border-bottom-color: var(--green);
    color: var(--text);
  }

  .observer {
    color: var(--amber);
    font-size: 11px;
    white-space: nowrap;
  }

  .connection {
    flex: 0 0 auto;
    gap: 6px;
    color: var(--text);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
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

  @media (max-width: 700px) {
    .topbar {
      gap: 12px;
      padding: 0 14px;
    }

    .identity {
      gap: 12px;
    }

    nav {
      gap: 11px;
    }

    .observer,
    .connection-label {
      display: none;
    }
  }

  @media (max-width: 440px) {
    h1 {
      position: absolute;
      width: 1px;
      height: 1px;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
    }
  }
</style>
