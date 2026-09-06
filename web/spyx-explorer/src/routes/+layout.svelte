<script lang="ts">
  import { page } from '$app/state';
  import AppShell from '$lib/components/AppShell.svelte';
  import '../app.css';
  import type { LayoutProps } from './$types';

  let { data, children }: LayoutProps = $props();
  const activePage = $derived.by(() => {
    if (page.url.pathname.startsWith('/api-docs')) return 'api-docs';
    if (page.url.pathname.startsWith('/search')) return 'search';
    if (page.url.pathname.startsWith('/audit')) return 'audit';
    if (page.url.pathname.startsWith('/programs')) return 'programs';
    if (page.url.pathname.startsWith('/holders')) return 'holders';
    if (page.url.pathname.startsWith('/price')) return 'price';
    return 'overview';
  });
</script>

<AppShell active={activePage} report={data.report}>
  {@render children()}
</AppShell>
