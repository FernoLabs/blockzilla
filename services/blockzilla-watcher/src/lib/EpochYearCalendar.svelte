<script lang="ts">
  import type { EpochYearCalendar } from '$lib/epoch-year-calendar';

  let { years }: { years: EpochYearCalendar[] } = $props();
</script>

<div class="year-calendars" aria-label="Archive coverage by UTC day">
  {#each years as year (year.year)}
    <section class="year-calendar" aria-labelledby={`epoch-year-${year.year}`}>
      <div class="year-label">
        <h3 id={`epoch-year-${year.year}`}>{year.year}</h3>
        <span title={`${year.tracked_epoch_count} tracked · ${year.epoch_count - year.tracked_epoch_count} untracked · grouped by epoch start year`}>
          {year.archived_epoch_count}/{year.epoch_count} archived
        </span>
      </div>
      <div class="year-scroll">
        <div class="year-frame" style:--weeks={year.week_count}>
          <div class="month-labels" aria-hidden="true">
            {#each year.months as month (month.month)}
              <span style:--week={month.week + 1}>{month.label}</span>
            {/each}
          </div>
          <div
            class="day-grid"
            role="img"
            aria-label={`${year.year} daily UTC archive coverage: ${year.archived_epoch_count} of ${year.epoch_count} epochs that started this year are archived; ${year.epoch_count - year.tracked_epoch_count} untracked. Use the Epochs tab for exact details.`}
          >
            {#each year.days as day (day.date)}
              {#if day.primary_epoch !== null && !day.future}
                <span
                  class={`year-day tone-${day.tone}`}
                  class:estimated={day.estimated}
                  class:today={day.today}
                  style:--week={day.week + 1}
                  style:--weekday={day.weekday + 1}
                  title={`${day.label}${day.estimated ? ' · estimated chain dates' : ''}`}
                  data-date={day.date}
                  aria-hidden="true"
                ></span>
              {:else}
                <span
                  class:future={day.future}
                  class="empty-day"
                  style:--week={day.week + 1}
                  style:--weekday={day.weekday + 1}
                  aria-hidden="true"
                ></span>
              {/if}
            {/each}
          </div>
        </div>
      </div>
    </section>
  {/each}
</div>

<style>
  .year-calendars {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    background: #141416;
  }

  .year-calendar {
    display: grid;
    grid-template-columns: 76px minmax(0, 1fr);
    gap: 8px;
    padding: 6px 10px 7px;
    border-bottom: 1px solid var(--border);
  }

  .year-calendar:nth-child(odd) {
    border-right: 1px solid var(--border);
  }

  .year-label {
    padding-top: 15px;
  }

  h3 {
    margin: 0;
    color: #d0d0d4;
    font-size: 12px;
    font-weight: 650;
    font-variant-numeric: tabular-nums;
  }

  .year-label span {
    display: block;
    margin-top: 2px;
    color: var(--faint);
    font-size: 8px;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }

  .year-scroll {
    min-width: 0;
    overflow-x: auto;
  }

  .year-frame {
    width: max-content;
    min-width: 100%;
  }

  .month-labels,
  .day-grid {
    display: grid;
    grid-template-columns: repeat(var(--weeks), 7px);
    column-gap: 2px;
  }

  .month-labels {
    height: 14px;
    color: var(--faint);
    font-size: 8px;
    line-height: 1;
  }

  .month-labels span {
    grid-column: var(--week) / span 4;
    white-space: nowrap;
  }

  .day-grid {
    grid-template-rows: repeat(7, 8px);
    row-gap: 2px;
    width: max-content;
  }

  .year-day,
  .empty-day {
    grid-column: var(--week);
    grid-row: var(--weekday);
    width: 7px;
    height: 8px;
    border-radius: 1px;
  }

  .year-day {
    background: var(--tone-bg);
    box-shadow: inset 0 -2px var(--tone-accent);
    cursor: help;
  }

  .year-day.estimated {
    opacity: 0.84;
  }

  .year-day.today {
    box-shadow: inset 0 -2px var(--tone-accent), 0 0 0 1px #f4f4f5;
  }

  .empty-day {
    background: #222226;
  }

  .empty-day.future {
    opacity: 0.35;
  }

  .tone-complete {
    --tone-bg: var(--status-complete-bg);
    --tone-accent: var(--status-complete-accent);
  }

  .tone-first-seen-complete {
    --tone-bg: var(--status-first-seen-complete-bg);
    --tone-accent: var(--status-first-seen-complete-accent);
  }

  .tone-legacy-complete {
    --tone-bg: var(--status-legacy-complete-bg);
    --tone-accent: var(--status-legacy-complete-accent);
  }

  .tone-active {
    --tone-bg: var(--status-active-bg);
    --tone-accent: var(--status-active-accent);
  }

  .tone-ready {
    --tone-bg: var(--status-ready-bg);
    --tone-accent: var(--status-ready-accent);
  }

  .tone-finalizing {
    --tone-bg: var(--status-finalizing-bg);
    --tone-accent: var(--status-finalizing-accent);
  }

  .tone-partial {
    --tone-bg: var(--status-partial-bg);
    --tone-accent: var(--status-partial-accent);
  }

  .tone-queued {
    --tone-bg: var(--status-queued-bg);
    --tone-accent: var(--status-queued-accent);
  }

  .tone-missing {
    --tone-bg: var(--status-missing-bg);
    --tone-accent: var(--status-missing-accent);
  }

  .tone-attention {
    --tone-bg: var(--status-attention-bg);
    --tone-accent: var(--status-attention-accent);
  }

  .tone-failed {
    --tone-bg: var(--status-failed-bg);
    --tone-accent: var(--status-failed-accent);
  }

  .tone-untracked {
    --tone-bg: #2a2a2e;
    --tone-accent: #6f7078;
  }

  @media (max-width: 1240px) {
    .year-calendars {
      grid-template-columns: 1fr;
    }

    .year-calendar:nth-child(odd) {
      border-right: 0;
    }
  }

  @media (max-width: 720px) {
    .year-calendar {
      grid-template-columns: 68px minmax(0, 1fr);
      gap: 5px;
      padding-inline: 8px;
    }
  }
</style>
