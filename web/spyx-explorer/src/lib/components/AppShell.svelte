<script lang="ts">
  import { resolve } from '$app/paths';
  import {
    Activity,
    BookOpenText,
    Braces,
    ChartCandlestick,
    Database,
    FileCheck2,
    Search,
    Users
  } from '@lucide/svelte';
  import type { Snippet } from 'svelte';
  import type { SpyxReport } from '$lib/types';
  import { formatBytes, formatInteger } from '$lib/format';

  let {
    active,
    report,
    children
  }: {
    active: 'overview' | 'holders' | 'price' | 'audit' | 'programs' | 'search' | 'api-docs';
    report: SpyxReport;
    children: Snippet;
  } = $props();
</script>

<div class="app-shell">
  <aside class="sidebar">
    <div class="brand">
      <strong>SPYx explorer</strong>
      <span>Solana transaction index</span>
    </div>

    <nav class="primary-nav" aria-label="Explorer pages">
      <a class={['nav-link', active === 'overview' && 'active']} href={resolve('/')}>
        <Activity size={17} strokeWidth={1.8} />
        <span>Overview</span>
      </a>
      <a class={['nav-link', active === 'holders' && 'active']} href={resolve('/holders')}>
        <Users size={17} strokeWidth={1.8} />
        <span>Holders</span>
      </a>
      <a class={['nav-link', active === 'price' && 'active']} href={resolve('/price')}>
        <ChartCandlestick size={17} strokeWidth={1.8} />
        <span>Price</span>
      </a>
      <a class={['nav-link', active === 'audit' && 'active']} href={resolve('/audit')}>
        <FileCheck2 size={17} strokeWidth={1.8} />
        <span>Integrity</span>
      </a>
      <a class={['nav-link', active === 'programs' && 'active']} href={resolve('/programs')}>
        <Braces size={17} strokeWidth={1.8} />
        <span>Programs</span>
      </a>
      <a class={['nav-link', active === 'search' && 'active']} href={resolve('/search')}>
        <Search size={17} strokeWidth={1.8} />
        <span>Search</span>
      </a>
      <a class={['nav-link', active === 'api-docs' && 'active']} href={resolve('/api-docs')}>
        <BookOpenText size={17} strokeWidth={1.8} />
        <span>API</span>
      </a>
    </nav>

    <div class="dataset-note">
      <div class="dataset-note-title">
        <Database size={15} strokeWidth={1.8} />
        <span>Indexed history</span>
      </div>
      <dl>
        <div>
          <dt>Epochs</dt>
          <dd>{report.source.first_epoch}–{report.source.last_epoch}</dd>
        </div>
        <div>
          <dt>Transactions</dt>
          <dd>{formatInteger(report.source.transactions)}</dd>
        </div>
        <div>
          <dt>Data size</dt>
          <dd>{formatBytes(report.source.total_dump_bytes)}</dd>
        </div>
      </dl>
    </div>
  </aside>

  <main class="workspace">
    {@render children()}
  </main>
</div>
