<script lang="ts">
  import { ArrowDown, ArrowUp, ChevronsUpDown } from '@lucide/svelte';

  let {
    label,
    active = false,
    direction = 'asc',
    numeric = false,
    onclick
  }: {
    label: string;
    active?: boolean;
    direction?: 'asc' | 'desc';
    numeric?: boolean;
    onclick: () => void;
  } = $props();

  const ariaSort = $derived(active ? (direction === 'asc' ? 'ascending' : 'descending') : 'none');
</script>

<th class={['sortable-heading', numeric && 'numeric']} aria-sort={ariaSort}>
  <button
    type="button"
    class={numeric ? 'numeric' : undefined}
    onclick={onclick}
    title={`Sort by ${label}`}
  >
    <span>{label}</span>
    {#if active}
      {#if direction === 'asc'}
        <ArrowUp size={13} strokeWidth={2} aria-hidden="true" />
      {:else}
        <ArrowDown size={13} strokeWidth={2} aria-hidden="true" />
      {/if}
    {:else}
      <ChevronsUpDown size={13} strokeWidth={1.7} aria-hidden="true" />
    {/if}
  </button>
</th>

<style>
  .sortable-heading {
    padding: 0;
  }

  button {
    width: 100%;
    min-height: 36px;
    display: flex;
    align-items: center;
    gap: 5px;
    padding: 7px 12px;
    border: 0;
    color: inherit;
    background: transparent;
    font: inherit;
    font-weight: inherit;
    text-align: left;
    white-space: nowrap;
  }

  button:hover,
  button:focus-visible {
    color: var(--text);
    background: var(--surface-muted);
  }

  button:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: -2px;
  }

  button.numeric {
    justify-content: flex-end;
  }
</style>
