<script lang="ts">
  import { Check, Copy } from '@lucide/svelte';
  import { onDestroy } from 'svelte';
  import { API_ROUTE_GROUPS, buildApiExamples } from '$lib/api-docs.js';

  type CopyResult = { id: string; status: 'copied' | 'failed' };

  const currentOrigin = window.location.origin;
  const examples = buildApiExamples(currentOrigin);
  let copyResult = $state.raw<CopyResult | null>(null);
  let copyResetTimer: ReturnType<typeof setTimeout> | undefined;

  onDestroy(() => {
    if (copyResetTimer !== undefined) clearTimeout(copyResetTimer);
  });

  async function copyCommand(id: string, command: string): Promise<void> {
    const copied = await writeClipboard(command);
    copyResult = { id, status: copied ? 'copied' : 'failed' };
    if (copyResetTimer !== undefined) clearTimeout(copyResetTimer);
    copyResetTimer = setTimeout(() => {
      copyResult = null;
      copyResetTimer = undefined;
    }, 2_000);
  }

  async function writeClipboard(value: string): Promise<boolean> {
    if (navigator.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(value);
        return true;
      } catch {
        // An HTTP LAN origin can block the Clipboard API. Use the selection fallback below.
      }
    }

    const field = document.createElement('textarea');
    field.value = value;
    field.readOnly = true;
    field.style.position = 'fixed';
    field.style.opacity = '0';
    field.style.pointerEvents = 'none';
    document.body.append(field);
    field.select();
    try {
      return document.execCommand('copy');
    } finally {
      field.remove();
    }
  }
</script>

<svelte:head>
  <title>SPYx API reference</title>
  <meta
    name="description"
    content="Public JSON endpoints for indexed SPYx transactions, token accounts, holder-authority classes, programs, and verified market data."
  />
</svelte:head>

<header class="topbar">
  <div>
    <h1>API reference</h1>
    <div class="address">Base URL: {currentOrigin}</div>
  </div>
</header>

<section class="panel">
  <div class="panel-toolbar">
    <h2>Public API</h2>
    <span class="panel-toolbar-detail">GET · JSON</span>
  </div>
  <p class="api-intro">
    These read-only endpoints use the same origin as this page. They return source-verified indexed Solana data
    and instruction-level market rows for SPYx.
  </p>
</section>

<section class="panel">
  <div class="panel-toolbar">
    <h2>Copyable examples</h2>
    <span class="panel-toolbar-detail">Working dataset queries</span>
  </div>
  <div class="example-list">
    {#each examples as example (example.id)}
      {@const status = copyResult?.id === example.id ? copyResult.status : null}
      <div class="example-row">
        <div class="example-detail">
          <strong>{example.label}</strong>
          <span>{example.description}</span>
        </div>
        <div class="command-row">
          <pre><code>{example.command}</code></pre>
          <button
            class="copy-button"
            type="button"
            onclick={() => void copyCommand(example.id, example.command)}
            aria-label={`Copy ${example.label} command`}
            aria-live="polite"
          >
            {#if status === 'copied'}
              <Check size={14} strokeWidth={1.8} />
              Copied
            {:else}
              <Copy size={14} strokeWidth={1.8} />
              {status === 'failed' ? 'Copy failed' : 'Copy'}
            {/if}
          </button>
        </div>
      </div>
    {/each}
  </div>
</section>

<section class="panel">
  <div class="panel-toolbar">
    <h2>Endpoint reference</h2>
    <span class="panel-toolbar-detail">All public routes</span>
  </div>
  <div class="route-groups">
    {#each API_ROUTE_GROUPS as group (group.id)}
      <details class="route-group">
        <summary>{group.title} <span>{group.routes.length} routes</span></summary>
        <div class="table-wrap">
          <table class="route-table">
            <thead>
              <tr>
                <th>Method</th>
                <th>Endpoint</th>
                <th>Returns</th>
              </tr>
            </thead>
            <tbody>
              {#each group.routes as route (route.path)}
                <tr>
                  <td class="mono">{route.method}</td>
                  <td><code>{route.path}</code></td>
                  <td>{route.description}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </details>
    {/each}
  </div>
</section>

<section class="panel">
  <div class="panel-toolbar">
    <h2>Limits and data meaning</h2>
  </div>
  <dl class="api-notes">
    <div>
      <dt>Address history pages</dt>
      <dd>
        Limit 1–200; default 100. Pass the returned opaque <code>next_cursor</code> without changes. The exact
        API path uses <code>/postings/</code> for these transaction-history indexes.
      </dd>
    </div>
    <div>
      <dt>Account owner history</dt>
      <dd>SPYx token account activity by owner. It is not signer history or all account activity.</dd>
    </div>
    <div>
      <dt>Holding history</dt>
      <dd>
        Exact transaction-final SPYx balance changes across all indexed token accounts owned by the
        address. Zero-net transactions are omitted. Estimated protocol claims are separate.
      </dd>
    </div>
    <div>
      <dt>Holder-authority summary</dt>
      <dd>
        <code>/data/spyx-summary.json</code> has four <code>class_totals</code>, a precomputed top 25
        for each class in <code>largest_25_by_class</code>, and <code>holdings_by_program</code>. An
        observed top-level signer is a wallet or user candidate. It is not proof of a human. An
        off-curve authority is attributed to a program only with committed parser or CPI evidence. This
        evidence does not prove PDA seeds. An off-curve authority stays unknown when the program evidence
        is not unambiguous.
      </dd>
    </div>
    <div>
      <dt>Market pages</dt>
      <dd>
        Trade limit 1–200; default 100. Time candles accept positive seconds. Slot candles contain
        only slots with trades. Price fields use the SPYx Token-2022 multiplier active at each swap.
        The Scaled UI Amount route gives the complete indexed multiplier history. DEX-program volume
        keeps raw on-chain SPYx units.
      </dd>
    </div>
    <div>
      <dt>Numeric values</dt>
      <dd>
        Raw amounts are decimal strings. Prices provide exact corrected and unscaled decimal fields,
        the multiplier and its exact bits, and a configuration ID. <code>chart_display</code> is not
        authoritative.
      </dd>
    </div>
    <div>
      <dt>Errors</dt>
      <dd>Errors use <code>{'{ error, message, details }'}</code>.</dd>
    </div>
  </dl>
</section>

<style>
  .api-intro {
    max-width: 880px;
    margin: 0;
    padding: 12px;
    color: var(--muted);
  }

  .example-row {
    padding: 11px 12px;
    border-bottom: 1px solid var(--border);
  }

  .example-row:last-child {
    border-bottom: 0;
  }

  .example-detail {
    display: flex;
    align-items: baseline;
    gap: 10px;
    margin-bottom: 7px;
  }

  .example-detail span {
    color: var(--muted);
    font-size: 12px;
  }

  .command-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: 8px;
    align-items: stretch;
  }

  pre {
    min-width: 0;
    margin: 0;
    padding: 8px 9px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--surface-muted);
    overflow: auto;
  }

  code {
    font-family:
      ui-monospace,
      SFMono-Regular,
      Menlo,
      Consolas,
      monospace;
    font-size: 12px;
  }

  .copy-button {
    min-width: 88px;
    padding: 0 10px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    background: var(--surface);
  }

  .copy-button:hover {
    border-color: var(--border-strong);
    background: var(--surface-muted);
  }

  .route-groups {
    background: var(--surface);
  }

  .route-group {
    border-bottom: 1px solid var(--border);
  }

  .route-group:last-child {
    border-bottom: 0;
  }

  .route-group summary {
    padding: 10px 12px;
    cursor: pointer;
    font-weight: 600;
  }

  .route-group summary span {
    margin-left: 6px;
    color: var(--muted);
    font-size: 12px;
    font-weight: 400;
  }

  .route-table th:first-child,
  .route-table td:first-child {
    width: 68px;
  }

  .route-table td:nth-child(2) {
    width: 48%;
    white-space: normal;
    overflow-wrap: anywhere;
  }

  .route-table td:last-child {
    white-space: normal;
  }

  .api-notes {
    margin: 0;
  }

  .api-notes div {
    display: grid;
    grid-template-columns: 150px minmax(0, 1fr);
    gap: 12px;
    padding: 9px 12px;
    border-bottom: 1px solid var(--border);
  }

  .api-notes div:last-child {
    border-bottom: 0;
  }

  .api-notes dd {
    margin: 0;
  }

  @media (max-width: 760px) {
    .example-detail {
      display: block;
    }

    .example-detail span {
      display: block;
      margin-top: 2px;
    }

    .command-row {
      grid-template-columns: minmax(0, 1fr);
    }

    .copy-button {
      min-height: 34px;
    }

    .route-group summary,
    .copy-button {
      min-height: 44px;
    }

    .api-notes div {
      grid-template-columns: minmax(0, 1fr);
      gap: 3px;
    }
  }
</style>
