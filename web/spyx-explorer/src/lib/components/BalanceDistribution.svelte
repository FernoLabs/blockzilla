<script lang="ts">
  import { formatBaseUnits, formatInteger, rangeLabel } from '$lib/format';
  import type { BalanceDistributionRow } from '$lib/types';

  let { rows }: { rows: BalanceDistributionRow[] } = $props();

  const maximumOwners = $derived(Math.max(1, ...rows.map((row) => row.holder_count)));
  const maximumBalance = $derived(
    Math.max(1, ...rows.map((row) => Number(row.public_balance.base_units)))
  );

  function barWidth(value: number, maximum: number): string {
    if (value <= 0) return '0%';
    return `${(value / maximum) * 100}%`;
  }
</script>

<section class="panel">
  <div class="panel-toolbar">
    <h2>Owner balance distribution</h2>
    <span class="panel-toolbar-detail">Verified SPYx balance ranges at the final indexed state</span>
  </div>
  <div class="table-wrap">
    <table>
      <caption class="sr-only">
        Final positive public-balance owner count and public balance for each SPYx balance range
      </caption>
      <thead>
        <tr>
          <th>SPYx range</th>
          <th>Owners</th>
          <th>SPYx balance</th>
        </tr>
      </thead>
      <tbody>
        {#each rows as row (row.base_unit_range)}
          {@const balance = Number(row.public_balance.base_units)}
          <tr>
            <th scope="row">{rangeLabel(row.base_unit_range)}</th>
            <td data-label="Owners">
              <div class="bar-value">
                <span class="bar-track" aria-hidden="true">
                  <i class="owner-bar" style:width={barWidth(row.holder_count, maximumOwners)}></i>
                </span>
                <strong>{formatInteger(row.holder_count)}</strong>
              </div>
            </td>
            <td data-label="SPYx balance">
              <div class="bar-value">
                <span class="bar-track" aria-hidden="true">
                  <i class="balance-bar" style:width={barWidth(balance, maximumBalance)}></i>
                </span>
                <strong>{formatBaseUnits(row.public_balance.base_units, 2)} SPYx</strong>
              </div>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</section>

<style>
  table {
    table-layout: fixed;
  }

  th:first-child {
    width: 220px;
  }

  tbody th {
    color: var(--text);
    font-weight: 600;
  }

  .bar-value {
    display: grid;
    grid-template-columns: minmax(80px, 1fr) minmax(90px, auto);
    align-items: center;
    gap: 10px;
  }

  .bar-value strong {
    text-align: right;
    font-size: 12px;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }

  .bar-track {
    height: 8px;
    border-radius: 999px;
    background: var(--surface-muted);
    overflow: hidden;
  }

  .bar-track i {
    display: block;
    height: 100%;
    border-radius: inherit;
  }

  .owner-bar {
    background: #3568a6;
  }

  .balance-bar {
    background: var(--accent);
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
      table-layout: auto;
    }

    thead {
      position: absolute;
      width: 1px;
      height: 1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
    }

    tbody {
      display: block;
    }

    tr {
      display: grid;
      grid-template-columns: 1fr;
      padding: 10px 12px;
      border-bottom: 1px solid var(--border);
    }

    tr:last-child {
      border-bottom: 0;
    }

    tbody th,
    tbody td {
      display: block;
      width: auto;
      padding: 0;
      border: 0;
      white-space: normal;
    }

    tbody th {
      margin-bottom: 8px;
    }

    tbody td {
      display: grid;
      grid-template-columns: 92px minmax(0, 1fr);
      align-items: center;
      gap: 8px;
      padding-top: 7px;
    }

    tbody td::before {
      content: attr(data-label);
      color: var(--muted);
      font-size: 11px;
    }

    .bar-value {
      grid-template-columns: minmax(42px, 1fr) auto;
    }
  }
</style>
