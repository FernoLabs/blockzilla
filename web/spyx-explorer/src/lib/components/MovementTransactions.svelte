<script lang="ts">
  import { resolve } from '$app/paths';
  import { formatBaseUnits, formatDate, formatInteger, shortAddress } from '$lib/format';
  import type { MovementTransaction } from '$lib/types';

  let { rows }: { rows: MovementTransaction[] } = $props();

  const initialLimit = 10;
  let expanded = $state(false);
  const visibleRows = $derived(expanded ? rows : rows.slice(0, initialLimit));
</script>

<section class="panel">
  <div class="panel-toolbar">
    <h2>Largest account-to-account movements</h2>
    <span class="panel-toolbar-detail">Token movement, not DEX or USD volume</span>
  </div>
  <div class="table-wrap">
    <table>
      <caption class="sr-only">
        Transactions with the largest public bilateral movement in the report
      </caption>
      <thead>
        <tr>
          <th>#</th>
          <th>Date</th>
          <th class="numeric">Token movement</th>
          <th class="numeric">Inferred mint</th>
          <th class="numeric">Inferred burn</th>
          <th>Slot</th>
          <th>Transaction index</th>
          <th>Signature</th>
        </tr>
      </thead>
      <tbody>
        {#each visibleRows as row, index (`${row.source_epoch}-${row.source_block_id}-${row.tx_index}`)}
          <tr>
            <td class="muted rank-cell" data-label="Rank">{index + 1}</td>
            <td data-label="Date">{formatDate(row.utc_date)}</td>
            <td class="numeric" data-label="Token movement">{formatBaseUnits(row.public_bilateral_movement.base_units, 2)}</td>
            <td class="numeric secondary-movement" data-label="Inferred mint">{formatBaseUnits(row.inferred_public_mint.base_units, 2)}</td>
            <td class="numeric secondary-movement" data-label="Inferred burn">{formatBaseUnits(row.inferred_public_burn.base_units, 2)}</td>
            <td class="numeric" data-label="Slot" title={`epoch ${row.source_epoch}, archive block record ${row.source_block_id}`}>
              {formatInteger(row.slot)}
            </td>
            <td class="numeric" data-label="Transaction index">{formatInteger(row.tx_index)}</td>
            <td data-label="Signature">
              <a
                class="signature-link mono"
                href={resolve(`/search?signature=${encodeURIComponent(row.first_signature)}`)}
                title={`Prefill transaction search with ${row.first_signature}`}
              >
                {shortAddress(row.first_signature)}
              </a>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
  {#if rows.length > initialLimit}
    <div class="table-footer">
      <span>Showing {formatInteger(visibleRows.length)} of {formatInteger(rows.length)} report rows</span>
      <button type="button" aria-expanded={expanded} onclick={() => (expanded = !expanded)}>
        {expanded ? 'Show first 10' : `Show all ${rows.length}`}
      </button>
    </div>
  {/if}
</section>

<style>
  table {
    min-width: 980px;
  }

  .table-footer {
    min-height: 44px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 7px 12px;
    border-top: 1px solid var(--border);
    color: var(--muted);
    font-size: 12px;
  }

  .table-footer button {
    min-height: 30px;
    padding: 0 10px;
    border: 1px solid var(--border-strong);
    border-radius: 6px;
    color: var(--text);
    background: var(--surface);
  }

  .table-footer button:hover {
    background: var(--surface-muted);
  }

  .table-footer button:focus-visible,
  .signature-link:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }

  .sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  }

  @media (max-width: 760px) {
    table {
      display: block;
      min-width: 0;
    }

    thead {
      position: absolute;
      width: 1px;
      height: 1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
    }

    tbody,
    tr {
      display: block;
    }

    tr {
      padding: 10px 12px;
      border-bottom: 1px solid var(--border);
    }

    tr:last-child {
      border-bottom: 0;
    }

    td {
      display: grid;
      grid-template-columns: 105px minmax(0, 1fr);
      gap: 10px;
      padding: 4px 0;
      border: 0;
      text-align: left;
      white-space: normal;
    }

    td::before {
      content: attr(data-label);
      color: var(--muted);
      font-size: 11px;
    }

    .rank-cell,
    .secondary-movement {
      display: none;
    }

    .table-footer {
      align-items: stretch;
      flex-direction: column;
    }

    .table-footer button {
      min-height: 44px;
    }
  }
</style>
