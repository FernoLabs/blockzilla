<script lang="ts">
  import { resolve } from '$app/paths';
  import { ArrowLeft } from '@lucide/svelte';
  import MarketDashboard from '$lib/components/MarketDashboard.svelte';
  import type { PageProps } from './$types';

  let { data }: PageProps = $props();
  const report = $derived(data.report);
  const expectedDataset = $derived({
    transactions: report.source.transactions,
    source_transaction_sha256: report.source.transactions_file.sha256
  });
</script>

<svelte:head>
  <title>SPYx price and DEX activity</title>
  <meta
    name="description"
    content="SPYx price by slot and time interval, DEX program volume, pairs, and parsed swaps."
  />
</svelte:head>

<header class="topbar">
  <div>
    <h1>Price</h1>
    <div class="address">SPYx price, pairs, parsed swaps, and DEX program volume</div>
  </div>
  <div class="controls">
    <a class="toolbar-button" href={resolve('/')}>
      <ArrowLeft size={16} strokeWidth={1.8} />
      <span>Overview</span>
    </a>
  </div>
</header>

<MarketDashboard expectedDataset={expectedDataset} targetMint={report.source.mint} />
