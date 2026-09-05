<script lang="ts">
  import { onMount } from 'svelte';
  import { resolve } from '$app/paths';
  import { ChartCandlestick, ExternalLink, FileCheck2, Users } from '@lucide/svelte';
  import HistoryChart from '$lib/components/HistoryChart.svelte';
  import { formatBaseUnits, formatCompact, formatInteger, shortAddress } from '$lib/format';
  import {
    getMarketScaledUiHistory,
    resolveMarketScaledUiEventAt,
    type MarketScaledUiAmountEvent,
    type MarketScaledUiHistory
  } from '$lib/market-api';
  import { bindSearchHealthToDataset, getSearchHealth } from '$lib/search-api';
  import type { PageProps } from './$types';

  let { data }: PageProps = $props();
  const report = $derived(data.report);
  let scaledUiStatus = $state<'loading' | 'ready' | 'unavailable'>('loading');
  let scaledUiEvent = $state.raw<MarketScaledUiAmountEvent | null>(null);
  let scaledUiHistory = $state.raw<MarketScaledUiHistory | null>(null);
  let scaledUiDatasetTip = $state<number | null>(null);
  const topHolders = $derived(
    (report.final_public_balance.holder_authority?.largest_25_all ??
      report.final_public_balance.largest_25_holders).slice(0, 5)
  );

  onMount(() => {
    const controller = new AbortController();
    void loadScaledUiMultiplier(controller.signal);
    return () => controller.abort();
  });

  async function loadScaledUiMultiplier(signal: AbortSignal): Promise<void> {
    try {
      const health = await getSearchHealth(signal);
      const binding = bindSearchHealthToDataset(health, {
        transactions: report.source.transactions,
        source_transaction_sha256: report.source.transactions_file.sha256
      });
      const market = health.market;
      if (
        binding.status !== 'match' ||
        !market?.available ||
        !market.complete ||
        market.source_transactions_scanned !== report.source.transactions ||
        market.source_transaction_sha256.toLowerCase() !==
          report.source.transactions_file.sha256.toLowerCase() ||
        market.target_mint !== report.source.mint ||
        market.dataset_latest_block_time === undefined
      ) {
        scaledUiStatus = 'unavailable';
        return;
      }

      const history = await getMarketScaledUiHistory(signal);
      scaledUiHistory = history;
      scaledUiDatasetTip = market.dataset_latest_block_time;
      scaledUiEvent = resolveMarketScaledUiEventAt(
        history,
        market.dataset_latest_block_time
      );
      scaledUiStatus = scaledUiEvent ? 'ready' : 'unavailable';
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') return;
      scaledUiStatus = 'unavailable';
    }
  }

  function scaledUiEffectiveDate(event: MarketScaledUiAmountEvent): string {
    const effectiveTimestamp = Math.max(0, event.effective_timestamp);
    if (effectiveTimestamp === 0) return 'mint initialization';
    return new Intl.DateTimeFormat('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      timeZone: 'UTC'
    }).format(new Date(effectiveTimestamp * 1_000));
  }

  function scaledUiTitle(event: MarketScaledUiAmountEvent): string {
    const effectiveTimestamp = Math.max(0, event.effective_timestamp);
    const effective =
      effectiveTimestamp === 0
        ? 'mint initialization'
        : new Date(effectiveTimestamp * 1_000).toISOString().replace('.000Z', ' UTC');
    return `Active at the indexed tip since ${effective}. Configuration ${event.config_id}; exact bits ${event.multiplier.bits}. Historical prices use the multiplier active at each swap.`;
  }
</script>

<svelte:head>
  <title>SPYx explorer</title>
  <meta
    name="description"
    content="SPYx price, holder, account, program, and verified Solana transaction history."
  />
</svelte:head>

<header class="topbar">
  <div>
    <h1>SPYx</h1>
    <a
      class="address"
      href={resolve(
        `/search?posting_kind=target-address&posting_key=${encodeURIComponent(report.source.mint)}`
      )}
      title="View transactions for the SPYx mint"
    >
      {report.source.mint}
    </a>
  </div>
  <div class="controls">
    <a class="toolbar-button" href={resolve('/holders')}>
      <Users size={16} strokeWidth={1.8} />
      <span>Holders</span>
    </a>
    <a class="toolbar-button" href={resolve('/price')}>
      <ChartCandlestick size={16} strokeWidth={1.8} />
      <span>Price</span>
    </a>
    <a class="toolbar-button" href={resolve('/audit')}>
      <FileCheck2 size={16} strokeWidth={1.8} />
      <span>Integrity</span>
    </a>
    <a
      class="icon-button"
      href={`https://solscan.io/token/${report.source.mint}`}
      target="_blank"
      rel="noreferrer"
      title="Open mint in Solscan"
      aria-label="Open mint in Solscan"
    >
      <ExternalLink size={16} strokeWidth={1.8} />
    </a>
  </div>
</header>

<section class="summary" aria-label="SPYx summary">
  <div class="summary-cell">
    <div class="label">Verified raw balance</div>
    <div class="value">{formatBaseUnits(report.final_public_balance.public_raw_balance_sum.base_units, 4)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Owners with a balance</div>
    <div class="value">{formatInteger(report.final_public_balance.positive_public_balance_holders)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Open token accounts</div>
    <div class="value">{formatInteger(report.final_public_balance.active_public_token_accounts)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Account-to-account movement</div>
    <div class="value">{formatCompact(report.public_volume_totals.public_bilateral_movement.base_units)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Indexed transactions</div>
    <div class="value">{formatInteger(report.source.transactions)}</div>
  </div>
</section>

<div class="overview-grid">
  <section class="panel">
    <div class="panel-toolbar">
      <h2>Token</h2>
      <span class="panel-toolbar-detail">Verified from indexed history</span>
    </div>
    <dl class="overview-metadata">
      <div><dt>Mint</dt><dd class="mono" title={report.source.mint}>{shortAddress(report.source.mint)}</dd></div>
      <div><dt>Decimals</dt><dd>{report.final_public_balance.decimals}</dd></div>
      <div><dt>Program</dt><dd>Token-2022</dd></div>
      <div>
        <dt>UI multiplier</dt>
        <dd class="scaled-ui-value">
          {#if scaledUiStatus === 'loading'}
            <span aria-label="Loading Token-2022 UI multiplier">…</span>
          {:else if scaledUiStatus === 'ready' && scaledUiEvent}
            <a
              class="scaled-ui-link mono"
              href={resolve('/price')}
              title={scaledUiTitle(scaledUiEvent)}
            >×{scaledUiEvent.multiplier.decimal}</a>
            <span class="scaled-ui-meta">At indexed tip · since {scaledUiEffectiveDate(scaledUiEvent)}</span>
          {:else}
            <span title="The indexed Token-2022 multiplier is unavailable">—</span>
          {/if}
        </dd>
      </div>
      <div><dt>Epochs</dt><dd>{report.source.first_epoch}–{report.source.last_epoch}</dd></div>
    </dl>
  </section>

  <section class="panel">
    <div class="panel-toolbar">
      <h2>Largest current holders</h2>
      <a class="panel-link" href={resolve('/holders')}>Full holder data</a>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>#</th><th>Owner</th><th class="numeric">SPYx balance</th></tr></thead>
        <tbody>
          {#each topHolders as holder, index (holder.owner)}
            <tr>
              <td class="muted">{index + 1}</td>
              <td>
                <a
                  class="owner-link mono"
                  href={resolve(`/search?posting_kind=owner&posting_key=${encodeURIComponent(holder.owner)}`)}
                  title={holder.owner}
                >{shortAddress(holder.owner)}</a>
              </td>
              <td class="numeric">{formatBaseUnits(holder.public_balance.base_units, 6)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>
</div>

<section class="panel">
  <div class="panel-toolbar">
    <h2>History</h2>
    <span class="panel-toolbar-detail">Daily verified token state and activity</span>
  </div>
  <HistoryChart
    rows={report.daily}
    {scaledUiHistory}
    {scaledUiDatasetTip}
    {scaledUiStatus}
  />
</section>

<style>
  .overview-grid {
    display: grid;
    grid-template-columns: minmax(300px, 0.8fr) minmax(420px, 1.2fr);
    gap: 14px;
  }

  .overview-metadata {
    margin: 0;
  }

  .overview-metadata > div {
    display: grid;
    grid-template-columns: 100px minmax(0, 1fr);
    gap: 12px;
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
  }

  .overview-metadata > div:last-child {
    border-bottom: 0;
  }

  .overview-metadata dd {
    margin: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .overview-metadata .scaled-ui-value {
    white-space: normal;
  }

  .scaled-ui-link {
    display: block;
    width: fit-content;
    max-width: 100%;
    overflow: hidden;
    color: var(--text);
    font-weight: 650;
    font-variant-numeric: tabular-nums;
    text-decoration: none;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .scaled-ui-link:hover {
    color: var(--accent);
    text-decoration: underline;
  }

  .scaled-ui-meta {
    display: block;
    margin-top: 2px;
    color: var(--muted);
    font-size: 11px;
    line-height: 1.35;
  }

  .panel-link {
    color: var(--accent);
    font-size: 12px;
    font-weight: 600;
    text-decoration: none;
  }

  .panel-link:hover {
    text-decoration: underline;
  }

  @media (max-width: 980px) {
    .overview-grid {
      grid-template-columns: 1fr;
    }
  }
</style>
