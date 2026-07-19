<script lang="ts">
  import type { ConnectionState } from '$lib/watcher-client.svelte';

  let {
    connectionState,
    connectionMessage
  }: {
    connectionState: ConnectionState;
    connectionMessage: string;
  } = $props();

  const maintenance = $derived(
    connectionState === 'retrying' || connectionState === 'offline'
  );

  function retryNow() {
    window.location.reload();
  }
</script>

<main class="service-state" lang="fr" aria-labelledby="service-state-title">
  <div class="service-state-content">
    <svg class="service-icon" viewBox="0 0 32 32" aria-hidden="true">
      <rect x="5.5" y="5.5" width="21" height="8" rx="1.5"></rect>
      <rect x="5.5" y="18.5" width="21" height="8" rx="1.5"></rect>
      <path d="M10 9.5h.01M10 22.5h.01M14 9.5h8M14 22.5h8"></path>
    </svg>

    {#if maintenance}
      <h2 id="service-state-title">Maintenance en cours</h2>
      <p>
        Le watcher Blockzilla est temporairement indisponible. Une intervention est en cours sur le
        NAS.
      </p>
      <p class="reconnect-note" role="status" aria-live="polite">
        La reconnexion se fera automatiquement dès le retour du service.
      </p>
      <button type="button" onclick={retryNow}>Réessayer maintenant</button>

      <details>
        <summary>Détail de la connexion</summary>
        <p>{connectionMessage}</p>
      </details>
    {:else}
      <h2 id="service-state-title">Connexion au watcher</h2>
      <p class="reconnect-note" role="status" aria-live="polite">
        Récupération de l’état du service…
      </p>
    {/if}
  </div>
</main>

<style>
  .service-state {
    min-height: calc(100vh - 54px);
    display: grid;
    place-items: center;
    padding: 48px 24px;
  }

  .service-state-content {
    width: min(440px, 100%);
  }

  .service-icon {
    width: 32px;
    height: 32px;
    margin-bottom: 22px;
    fill: none;
    stroke: var(--amber);
    stroke-linecap: round;
    stroke-linejoin: round;
    stroke-width: 1.5;
  }

  h2 {
    margin: 0 0 10px;
    color: var(--text);
    font-size: 22px;
    font-weight: 650;
    letter-spacing: -0.02em;
  }

  p {
    margin: 0;
    color: var(--muted);
    font-size: 14px;
    line-height: 1.6;
  }

  .reconnect-note {
    margin-top: 10px;
    color: #c8c8cc;
  }

  button {
    margin-top: 24px;
    padding: 8px 12px;
    border: 1px solid var(--border-strong);
    border-radius: 8px;
    background: var(--surface-raised);
    color: var(--text);
    cursor: pointer;
    font-size: 13px;
    font-weight: 600;
  }

  button:hover {
    border-color: #5b5b63;
    background: #242428;
  }

  details {
    margin-top: 28px;
    padding-top: 14px;
    border-top: 1px solid var(--border);
    color: var(--faint);
    font-size: 11px;
  }

  summary {
    width: fit-content;
    cursor: pointer;
  }

  details p {
    margin-top: 8px;
    color: var(--faint);
    font-family: ui-monospace, "SFMono-Regular", Consolas, monospace;
    font-size: 11px;
    overflow-wrap: anywhere;
  }

  @media (max-width: 700px) {
    .service-state {
      place-items: start center;
      padding: 72px 24px 32px;
    }
  }
</style>
