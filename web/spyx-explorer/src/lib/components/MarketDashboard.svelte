<script lang="ts">
  import { onMount } from 'svelte';
  import type { Attachment } from 'svelte/attachments';
  import { afterNavigate, goto, replaceState } from '$app/navigation';
  import { resolve } from '$app/paths';
  import { page } from '$app/state';
  import { RefreshCw, ShieldCheck } from '@lucide/svelte';
  import {
    CandlestickSeries,
    ColorType,
    HistogramSeries,
    LineSeries,
    LineType,
    createChart,
    type CandlestickData,
    type HistogramData,
    type LineData,
    type Time,
    type UTCTimestamp
  } from 'lightweight-charts';
  import { formatCompact, formatInteger, formatRawAmount, shortAddress } from '$lib/format';
  import {
    getMarketCandles,
    getMarketMints,
    getMarketPairs,
    getMarketProgramVolume,
    getMarketPrograms,
    getMarketSlotCandles,
    getMarketSummary,
    getMarketTrades,
    type MarketCandle,
    type MarketInterval,
    type MarketMint,
    type MarketPair,
    type MarketPriceResolution,
    type MarketProgram,
    type MarketProgramSummary,
    type MarketProgramVolumeSeries,
    type MarketSlotCandle,
    type MarketSummary,
    type MarketTrade
  } from '$lib/market-api';
  import {
    bindSearchHealthToDataset,
    getSearchHealth,
    type SearchDatasetIdentity
  } from '$lib/search-api';

  type LoadStatus = 'loading' | 'ready' | 'unavailable' | 'error';
  type VolumeRange = '7d' | '30d' | '90d' | 'all';

  interface ProgramChartSeries {
    address: string | null;
    name: string;
    color: string;
    totalRaw: string;
    data: LineData<UTCTimestamp>[];
  }

  interface MarketUrlState {
    quoteMint: string;
    priceResolution: MarketPriceResolution;
    volumeRange: VolumeRange;
  }

  const chartHeight = 340;
  const usdcMint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
  const usdtMint = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';
  const resolutions: { id: MarketPriceResolution; label: string }[] = [
    { id: 'slot', label: 'Slot' },
    { id: '60', label: '1m' },
    { id: '1h', label: '1H' },
    { id: '4h', label: '4H' },
    { id: '1d', label: '1D' },
    { id: '1w', label: '1W' }
  ];
  const volumeRanges: { id: VolumeRange; label: string }[] = [
    { id: '7d', label: '7D' },
    { id: '30d', label: '30D' },
    { id: '90d', label: '90D' },
    { id: 'all', label: 'All' }
  ];
  const programColors = ['#0f766e', '#2563eb', '#c2410c', '#7c3aed', '#b45309', '#64748b'];

  let {
    expectedDataset,
    targetMint
  }: {
    expectedDataset: SearchDatasetIdentity;
    targetMint: string;
  } = $props();

  const routePath = page.url.pathname;
  const initialUrlState = readMarketUrlState(page.url);

  let status = $state<LoadStatus>('loading');
  let errorMessage = $state('');
  let summary = $state.raw<MarketSummary | null>(null);
  let pairs = $state.raw<MarketPair[]>([]);
  let mints = $state.raw<MarketMint[]>([]);
  let programs = $state.raw<MarketProgramSummary[]>([]);
  let candles = $state.raw<MarketCandle[]>([]);
  let slotCandles = $state.raw<MarketSlotCandle[]>([]);
  let programVolume = $state.raw<MarketProgramVolumeSeries | null>(null);
  let trades = $state.raw<MarketTrade[]>([]);
  let requestedQuoteMint = $state(initialUrlState.quoteMint);
  let selectedQuoteMint = $state('');
  let priceResolution = $state<MarketPriceResolution>(initialUrlState.priceResolution);
  let volumeRange = $state<VolumeRange>(initialUrlState.volumeRange);
  let detailLoading = $state(false);
  let detailError = $state('');
  let volumeLoading = $state(false);
  let volumeError = $state('');
  let provenance = $state.raw<{
    parserVersion: string;
    artifactSha256: string;
  } | null>(null);
  let detailController: AbortController | null = null;
  let volumeController: AbortController | null = null;
  let detailGeneration = 0;
  let volumeGeneration = 0;

  const selectedPair = $derived(
    pairs.find((pair) => pair.quote_mint === selectedQuoteMint) ?? null
  );
  const mintByAddress = $derived(new Map(mints.map((mint) => [mint.mint, mint])));
  const quoteLabel = $derived(selectedPair ? quoteSymbol(selectedPair.quote_mint) : 'quote');
  const programChartSeries = $derived(buildProgramChartSeries(programVolume));
  const attachMarketChart = marketChartAttachment(
    () => candles,
    () => slotCandles,
    () => selectedPair?.quote_decimals ?? 0,
    () => priceResolution
  );
  const attachProgramVolumeChart = programVolumeChartAttachment(() => programChartSeries);

  afterNavigate((navigation) => {
    const destination = navigation.to?.url;
    if (!destination || destination.pathname !== routePath) return;
    applyMarketUrlState(readMarketUrlState(destination));
  });

  onMount(() => {
    const controller = new AbortController();
    void loadMarket(controller.signal);
    return () => {
      controller.abort();
      detailController?.abort();
      volumeController?.abort();
    };
  });

  async function loadMarket(signal: AbortSignal): Promise<void> {
    status = 'loading';
    errorMessage = '';
    try {
      const health = await getSearchHealth(signal);
      const binding = bindSearchHealthToDataset(health, expectedDataset);
      if (binding.status === 'mismatch') {
        throw new Error('The market service is bound to different SPYx source data.');
      }
      if (!health.market?.available) {
        status = 'unavailable';
        return;
      }
      if (
        !health.market.complete ||
        health.market.source_transaction_sha256.toLowerCase() !==
          expectedDataset.source_transaction_sha256.toLowerCase()
      ) {
        throw new Error('The market database is incomplete or has a different source hash.');
      }
      provenance = {
        parserVersion: health.market.parser_semantic_version,
        artifactSha256: health.market.market_manifest_sha256
      };
      const [loadedSummary, loadedPairs, loadedMints, loadedPrograms] = await Promise.all([
        getMarketSummary(signal),
        getMarketPairs(signal),
        getMarketMints(signal),
        getMarketPrograms(signal)
      ]);
      if (loadedSummary.target_mint !== targetMint) {
        throw new Error('The market database has a different target mint.');
      }
      const orderedPairs = loadedPairs.slice().sort(
        (left, right) =>
          right.trade_count - left.trade_count || left.quote_mint_id - right.quote_mint_id
      );
      summary = loadedSummary;
      pairs = orderedPairs;
      mints = loadedMints;
      programs = loadedPrograms.slice().sort((left, right) => {
        const leftVolume = BigInt(left.target_volume_raw);
        const rightVolume = BigInt(right.target_volume_raw);
        if (leftVolume !== rightVolume) return leftVolume > rightVolume ? -1 : 1;
        return left.program.registry_id - right.program.registry_id;
      });
      const requestedPair = orderedPairs.find((pair) => pair.quote_mint === requestedQuoteMint);
      const initialPair = requestedPair ?? defaultMarketPair(orderedPairs);
      if (!initialPair) {
        status = 'ready';
        return;
      }
      if (requestedQuoteMint && !requestedPair) {
        requestedQuoteMint = '';
        const url = marketStateUrl();
        replaceState(resolve(`/price${url.search}${url.hash}` as '/price'), page.state);
      }
      selectedQuoteMint = initialPair.quote_mint;
      status = 'ready';
      await Promise.all([
        loadPair(initialPair.quote_mint, priceResolution),
        loadProgramVolume(loadedSummary, volumeRange)
      ]);
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') return;
      errorMessage = error instanceof Error ? error.message : 'The market database request failed.';
      status = 'error';
    }
  }

  async function loadPair(
    quoteMint: string,
    nextResolution: MarketPriceResolution
  ): Promise<void> {
    detailController?.abort();
    const controller = new AbortController();
    detailController = controller;
    const generation = ++detailGeneration;
    detailLoading = true;
    detailError = '';
    candles = [];
    slotCandles = [];
    trades = [];
    try {
      if (nextResolution === 'slot') {
        const [loadedSlots, loadedTrades] = await Promise.all([
          getMarketSlotCandles(quoteMint, controller.signal),
          getMarketTrades(quoteMint, 10, controller.signal)
        ]);
        if (generation !== detailGeneration) return;
        slotCandles = loadedSlots;
        trades = loadedTrades.items;
      } else {
        const [loadedCandles, loadedTrades] = await Promise.all([
          getMarketCandles(quoteMint, nextResolution, controller.signal),
          getMarketTrades(quoteMint, 10, controller.signal)
        ]);
        if (generation !== detailGeneration) return;
        candles = loadedCandles;
        trades = loadedTrades.items;
      }
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') return;
      if (generation === detailGeneration) {
        candles = [];
        slotCandles = [];
        trades = [];
        detailError = error instanceof Error ? error.message : 'The selected pair request failed.';
      }
    } finally {
      if (generation === detailGeneration) detailLoading = false;
    }
  }

  async function loadProgramVolume(
    marketSummary: MarketSummary,
    nextRange: VolumeRange
  ): Promise<void> {
    volumeController?.abort();
    const controller = new AbortController();
    volumeController = controller;
    const generation = ++volumeGeneration;
    volumeLoading = true;
    volumeError = '';
    programVolume = null;
    try {
      const bounds = programVolumeBounds(marketSummary, nextRange);
      if (!bounds) return;
      const loaded = await getMarketProgramVolume(
        bounds.interval,
        bounds.timeFrom,
        bounds.timeTo,
        controller.signal
      );
      if (generation !== volumeGeneration) return;
      programVolume = loaded;
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') return;
      if (generation === volumeGeneration) {
        programVolume = null;
        volumeError =
          error instanceof Error ? error.message : 'The DEX-program volume request failed.';
      }
    } finally {
      if (generation === volumeGeneration) volumeLoading = false;
    }
  }

  function programVolumeBounds(
    marketSummary: MarketSummary,
    range: VolumeRange
  ): { interval: MarketInterval; timeFrom: number; timeTo: number } | null {
    const first = marketSummary.first_block_time;
    const last = marketSummary.last_block_time;
    if (first === null || last === null || first > last) return null;
    const day = 86_400;
    if (range === '7d') {
      return { interval: '1h', timeFrom: Math.max(first, last - 7 * day), timeTo: last };
    }
    if (range === '30d') {
      return { interval: '4h', timeFrom: Math.max(first, last - 30 * day), timeTo: last };
    }
    if (range === '90d') {
      return { interval: '1d', timeFrom: Math.max(first, last - 90 * day), timeTo: last };
    }
    const duration = last - first;
    return {
      interval: duration <= 719 * day ? '1d' : '1w',
      timeFrom: first,
      timeTo: last
    };
  }

  function selectPair(event: Event): void {
    const quoteMint = (event.currentTarget as HTMLSelectElement).value;
    if (quoteMint === selectedQuoteMint) return;
    navigateMarketState({ quoteMint });
  }

  function selectResolution(nextResolution: MarketPriceResolution): void {
    if (priceResolution === nextResolution || !selectedQuoteMint) return;
    navigateMarketState({ priceResolution: nextResolution });
  }

  function selectVolumeRange(nextRange: VolumeRange): void {
    if (volumeRange === nextRange || !summary) return;
    navigateMarketState({ volumeRange: nextRange });
  }

  function readMarketUrlState(url: URL): MarketUrlState {
    const quoteMint = url.searchParams.get('pair') ?? '';
    const interval = url.searchParams.get('interval');
    const range = url.searchParams.get('volume');
    return {
      quoteMint: /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(quoteMint) ? quoteMint : '',
      priceResolution:
        interval === 'slot' ||
        interval === '60' ||
        interval === '1h' ||
        interval === '4h' ||
        interval === '1d' ||
        interval === '1w'
          ? interval
          : '1d',
      volumeRange:
        range === '7d' || range === '30d' || range === '90d' || range === 'all'
          ? range
          : '90d'
    };
  }

  function defaultMarketPair(sourcePairs = pairs): MarketPair | undefined {
    return sourcePairs.find((pair) => pair.direct_usd) ?? sourcePairs[0];
  }

  function marketStateUrl(overrides: Partial<MarketUrlState> = {}): URL {
    const state: MarketUrlState = {
      quoteMint: requestedQuoteMint,
      priceResolution,
      volumeRange,
      ...overrides
    };
    const url = new URL(page.url);
    for (const name of ['pair', 'interval', 'volume']) url.searchParams.delete(name);
    if (state.quoteMint) url.searchParams.set('pair', state.quoteMint);
    if (state.priceResolution !== '1d') {
      url.searchParams.set('interval', state.priceResolution);
    }
    if (state.volumeRange !== '90d') url.searchParams.set('volume', state.volumeRange);
    return url;
  }

  function navigateMarketState(overrides: Partial<MarketUrlState>): void {
    const url = marketStateUrl(overrides);
    if (url.href === page.url.href) return;
    void goto(resolve(`/price${url.search}${url.hash}` as '/price'), {
      keepFocus: true,
      noScroll: true
    });
  }

  function applyMarketUrlState(nextState: MarketUrlState): void {
    const canResolvePair = pairs.length > 0;
    const requestedPair = pairs.find((pair) => pair.quote_mint === nextState.quoteMint);
    const nextPair = canResolvePair ? requestedPair ?? defaultMarketPair() : undefined;
    const nextQuoteMint = canResolvePair ? (nextPair?.quote_mint ?? '') : selectedQuoteMint;
    const pairChanged = selectedQuoteMint !== nextQuoteMint;
    const resolutionChanged = priceResolution !== nextState.priceResolution;
    const rangeChanged = volumeRange !== nextState.volumeRange;

    requestedQuoteMint = canResolvePair
      ? requestedPair
        ? nextState.quoteMint
        : ''
      : nextState.quoteMint;
    selectedQuoteMint = nextQuoteMint;
    priceResolution = nextState.priceResolution;
    volumeRange = nextState.volumeRange;

    if (nextState.quoteMint && !requestedPair && canResolvePair) {
      const url = marketStateUrl();
      replaceState(resolve(`/price${url.search}${url.hash}` as '/price'), page.state);
    }
    if (status !== 'ready' || !summary) return;
    if ((pairChanged || resolutionChanged) && nextQuoteMint) {
      void loadPair(nextQuoteMint, nextState.priceResolution);
    }
    if (rangeChanged) void loadProgramVolume(summary, nextState.volumeRange);
  }

  function retry(): void {
    detailController?.abort();
    volumeController?.abort();
    const controller = new AbortController();
    detailController = controller;
    void loadMarket(controller.signal);
  }

  function formatPrice(value: number | undefined): string {
    if (value === undefined || !Number.isFinite(value)) return '—';
    if (value === 0) return '0';
    if (Math.abs(value) >= 1) {
      return value.toLocaleString('en-US', { maximumFractionDigits: 8 });
    }
    return value.toLocaleString('en-US', { maximumSignificantDigits: 8 });
  }

  function formatUtcDateTime(unixTime: number | null): string {
    if (unixTime === null) return 'No block time';
    return new Intl.DateTimeFormat('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      timeZone: 'UTC',
      hour12: false
    }).format(new Date(unixTime * 1_000));
  }

  function formatUtcAxis(unixTime: number, includeTime: boolean): string {
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      day: 'numeric',
      ...(includeTime ? { hour: '2-digit', minute: '2-digit', hour12: false } : {}),
      timeZone: 'UTC'
    }).format(new Date(unixTime * 1_000));
  }

  function formatSlot(slot: number): string {
    return Math.trunc(slot).toLocaleString('en-US');
  }

  function formatProgramVolume(raw: string, decimals: number): string {
    const formatted = formatRawAmount(raw, decimals, 2);
    return BigInt(raw) > 0n && formatted === '0' ? '<0.01' : formatted;
  }

  function quoteSymbol(address: string): string {
    const metadata = mintByAddress.get(address);
    if (metadata?.symbol) return metadata.symbol;
    if (metadata?.name) return metadata.name;
    if (address === usdcMint) return 'USDC';
    if (address === usdtMint) return 'USDT';
    return shortAddress(address);
  }

  function quoteIdentity(address: string): string {
    const label = quoteSymbol(address);
    const short = shortAddress(address);
    return label === short ? label : `${label} · ${short}`;
  }

  function marketChartAttachment(
    getCandles: () => MarketCandle[],
    getSlotCandles: () => MarketSlotCandle[],
    getQuoteDecimals: () => number,
    getResolution: () => MarketPriceResolution
  ): Attachment<HTMLDivElement> {
    return (node) => {
      const styles = getComputedStyle(node);
      const surface = cssColor(styles, '--surface', '#ffffff');
      const text = cssColor(styles, '--muted', '#59636e');
      const border = cssColor(styles, '--border', '#d8dee4');
      const grid = cssColor(styles, '--grid', '#eaeef2');
      const accent = cssColor(styles, '--accent', '#0f766e');
      const warning = cssColor(styles, '--warn', '#9a3412');

      const chart = createChart(node, {
        autoSize: true,
        height: chartHeight,
        layout: {
          attributionLogo: true,
          background: { type: ColorType.Solid, color: surface },
          textColor: text,
          fontFamily: styles.fontFamily
        },
        localization: {
          timeFormatter: (time: Time) => {
            const numeric = Number(time);
            return getResolution() === 'slot'
              ? `Slot ${formatSlot(numeric)}`
              : `${formatUtcAxis(numeric, true)} UTC`;
          }
        },
        grid: {
          vertLines: { color: grid },
          horzLines: { color: grid }
        },
        rightPriceScale: { borderColor: border },
        timeScale: {
          borderColor: border,
          secondsVisible: false,
          rightOffset: 3,
          tickMarkFormatter: (time: Time) => {
            const numeric = Number(time);
            return getResolution() === 'slot'
              ? formatCompact(numeric)
              : formatUtcAxis(numeric, getResolution() !== '1d' && getResolution() !== '1w');
          }
        }
      });
      const priceSeries = chart.addSeries(CandlestickSeries, {
        upColor: accent,
        downColor: warning,
        borderVisible: false,
        wickUpColor: accent,
        wickDownColor: warning,
        priceLineColor: accent
      });
      const volumeSeries = chart.addSeries(
        HistogramSeries,
        {
          priceFormat: { type: 'volume' },
          priceLineVisible: false,
          lastValueVisible: false
        },
        1
      );
      chart.panes()[0]?.setStretchFactor(3);
      chart.panes()[1]?.setStretchFactor(1);

      $effect(() => {
        const sourceCandles = getCandles();
        const sourceSlotCandles = getSlotCandles();
        const quoteDecimals = getQuoteDecimals();
        const activeResolution = getResolution();
        const { prices, volumes } = toChartData(
          sourceCandles,
          sourceSlotCandles,
          quoteDecimals,
          activeResolution
        );

        priceSeries.applyOptions({
          priceFormat: {
            type: 'custom',
            formatter: formatPrice,
            minMove: priceMinMove(prices)
          }
        });
        priceSeries.setData(prices);
        volumeSeries.setData(volumes);
        chart.applyOptions({
          timeScale: {
            timeVisible:
              activeResolution !== 'slot' &&
              (activeResolution === '60' ||
                activeResolution === '1h' ||
                activeResolution === '4h'),
            secondsVisible: false
          }
        });
        chart.timeScale().fitContent();
      });

      return () => chart.remove();
    };
  }

  function toChartData(
    sourceCandles: MarketCandle[],
    sourceSlotCandles: MarketSlotCandle[],
    quoteDecimals: number,
    resolution: MarketPriceResolution
  ): {
    prices: CandlestickData<UTCTimestamp>[];
    volumes: HistogramData<UTCTimestamp>[];
  } {
    const prices: CandlestickData<UTCTimestamp>[] = [];
    const volumes: HistogramData<UTCTimestamp>[] = [];
    const orderedCandles = (
      resolution === 'slot'
        ? sourceSlotCandles.map((candle) => ({
            axis: candle.slot,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            quote_volume_raw: candle.quote_volume_raw
          }))
        : sourceCandles.map((candle) => ({
            axis: candle.start_time,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            quote_volume_raw: candle.quote_volume_raw
          }))
    ).sort((left, right) => left.axis - right.axis);
    let previousTime = Number.NEGATIVE_INFINITY;

    for (const candle of orderedCandles) {
      const axis = Math.trunc(candle.axis);
      const { open, high, low, close } = {
        open: candle.open.display,
        high: candle.high.display,
        low: candle.low.display,
        close: candle.close.display
      };
      if (
        axis <= previousTime ||
        ![open, high, low, close].every(Number.isFinite) ||
        high < Math.max(open, close) ||
        low > Math.min(open, close)
      ) {
        continue;
      }

      const time = axis as UTCTimestamp;
      prices.push({ time, open, high, low, close });
      volumes.push({
        time,
        value: rawAmountToChartNumber(candle.quote_volume_raw, quoteDecimals),
        color:
          close >= open ? 'rgba(15, 118, 110, 0.42)' : 'rgba(154, 52, 18, 0.38)'
      });
      previousTime = axis;
    }

    return { prices, volumes };
  }

  function buildProgramChartSeries(
    source: MarketProgramVolumeSeries | null
  ): ProgramChartSeries[] {
    if (!source?.points.length || source.interval_seconds <= 0) return [];
    const pointByTime = new Map(source.points.map((point) => [point.start_time, point]));
    const firstTime = source.points[0].start_time;
    const lastTime = source.points[source.points.length - 1].start_time;
    const bucketTimes: number[] = [];
    for (
      let startTime = firstTime;
      startTime <= lastTime;
      startTime += source.interval_seconds
    ) {
      bucketTimes.push(startTime);
    }
    const totals = new Map<
      string,
      { program: MarketProgram; raw: bigint }
    >();
    for (const point of source.points) {
      for (const entry of point.programs) {
        const current = totals.get(entry.program.address);
        const raw = BigInt(entry.target_volume_raw);
        totals.set(entry.program.address, {
          program: entry.program,
          raw: (current?.raw ?? 0n) + raw
        });
      }
    }
    const ordered = [...totals.values()].sort((left, right) => {
      if (left.raw !== right.raw) return left.raw > right.raw ? -1 : 1;
      return left.program.registry_id - right.program.registry_id;
    });
    const primary = ordered.slice(0, 5);
    const primaryIds = new Set(primary.map((entry) => entry.program.address));
    const result: ProgramChartSeries[] = primary.map((entry, index) => ({
      address: entry.program.address,
      name: entry.program.name,
      color: programColors[index] ?? programColors[0],
      totalRaw: entry.raw.toString(),
      data: bucketTimes.map((startTime) => {
        const volume = pointByTime.get(startTime)?.programs.find(
          (program) => program.program.address === entry.program.address
        );
        return {
          time: Math.trunc(startTime) as UTCTimestamp,
          value: rawAmountToChartNumber(volume?.target_volume_raw ?? '0', source.target_decimals)
        };
      })
    }));
    const otherRaw = ordered
      .filter((entry) => !primaryIds.has(entry.program.address))
      .reduce((sum, entry) => sum + entry.raw, 0n);
    if (otherRaw > 0n) {
      result.push({
        address: null,
        name: 'Other DEX programs',
        color: programColors[5],
        totalRaw: otherRaw.toString(),
        data: bucketTimes.map((startTime) => ({
          time: Math.trunc(startTime) as UTCTimestamp,
          value: rawAmountToChartNumber(
            (pointByTime.get(startTime)?.programs ?? [])
              .filter((entry) => !primaryIds.has(entry.program.address))
              .reduce((sum, entry) => sum + BigInt(entry.target_volume_raw), 0n)
              .toString(),
            source.target_decimals
          )
        }))
      });
    }
    return result;
  }

  function programVolumeChartAttachment(
    getSeries: () => ProgramChartSeries[]
  ): Attachment<HTMLDivElement> {
    return (node) => {
      const styles = getComputedStyle(node);
      const chart = createChart(node, {
        autoSize: true,
        height: 270,
        layout: {
          attributionLogo: true,
          background: { type: ColorType.Solid, color: cssColor(styles, '--surface', '#ffffff') },
          textColor: cssColor(styles, '--muted', '#59636e'),
          fontFamily: styles.fontFamily
        },
        localization: {
          timeFormatter: (time: Time) => `${formatUtcAxis(Number(time), true)} UTC`
        },
        grid: {
          vertLines: { color: cssColor(styles, '--grid', '#eaeef2') },
          horzLines: { color: cssColor(styles, '--grid', '#eaeef2') }
        },
        rightPriceScale: { borderColor: cssColor(styles, '--border', '#d8dee4') },
        timeScale: {
          borderColor: cssColor(styles, '--border', '#d8dee4'),
          rightOffset: 3,
          timeVisible: true,
          secondsVisible: false,
          tickMarkFormatter: (time: Time) => formatUtcAxis(Number(time), false)
        }
      });

      $effect(() => {
        const activeSeries = getSeries().map((entry) => {
          const series = chart.addSeries(LineSeries, {
            color: entry.color,
            lineWidth: 2,
            lineType: LineType.WithSteps,
            crosshairMarkerVisible: false,
            lastValueVisible: false,
            priceLineVisible: false,
            priceFormat: {
              type: 'custom',
              formatter: (value: number) => formatCompact(value)
            }
          });
          series.setData(entry.data);
          return series;
        });
        chart.timeScale().fitContent();
        return () => {
          for (const series of activeSeries) chart.removeSeries(series);
        };
      });

      return () => chart.remove();
    };
  }

  function rawAmountToChartNumber(raw: string, decimals: number): number {
    const digits = raw.replace(/^0+(?=\d)/, '');
    if (!/^\d+$/.test(digits)) return 0;
    const scale = Math.max(0, Math.trunc(decimals));
    const padded = digits.padStart(scale + 1, '0');
    const split = padded.length - scale;
    const decimal = scale === 0 ? padded : `${padded.slice(0, split)}.${padded.slice(split)}`;
    const value = Number(decimal);
    return Number.isFinite(value) ? value : 0;
  }

  function priceMinMove(prices: CandlestickData<UTCTimestamp>[]): number {
    let smallest = Number.POSITIVE_INFINITY;
    for (const price of prices) {
      smallest = Math.min(
        smallest,
        ...[price.open, price.high, price.low, price.close].filter((value) => value > 0)
      );
    }
    if (!Number.isFinite(smallest)) return 0.01;
    const magnitude = Math.floor(Math.log10(smallest));
    const decimals = Math.min(18, Math.max(2, 7 - magnitude));
    return 10 ** -decimals;
  }

  function cssColor(styles: CSSStyleDeclaration, name: string, fallback: string): string {
    return styles.getPropertyValue(name).trim() || fallback;
  }

</script>

{#if status === 'loading'}
  <section class="panel market-state" aria-live="polite">
    <span class="spin"><RefreshCw size={18} strokeWidth={1.8} /></span>
    <div>
      <h2>Loading verified market data</h2>
      <p>The explorer is checking the market database source hash.</p>
    </div>
  </section>
{:else if status === 'unavailable'}
  <section class="panel market-state">
    <div>
      <h2>Market database is not connected</h2>
      <p>Token, wallet-owner, and integrity data remain available. Connect a verified market index to add price, volume, pair, and swap views.</p>
    </div>
    <button type="button" onclick={retry}>Check again</button>
  </section>
{:else if status === 'error'}
  <section class="panel market-state error" role="alert">
    <div>
      <h2>Market data did not pass its source check</h2>
      <p>{errorMessage}</p>
    </div>
    <button type="button" onclick={retry}>Try again</button>
  </section>
{:else if summary}
  <section class="summary market-summary" aria-label="Verified SPYx market summary">
    <div class="summary-cell">
      <div class="label">Latest executed swap price</div>
      <div class="value">
        {formatPrice(selectedPair?.latest_price?.display)}
        <span class="unit">{selectedPair ? quoteSymbol(selectedPair.quote_mint) : quoteLabel}</span>
      </div>
    </div>
    <div class="summary-cell">
      <div class="label">24-hour quote volume</div>
      <div class="value">
        {selectedPair
          ? formatRawAmount(selectedPair.quote_volume_24h_raw, selectedPair.quote_decimals, 4)
          : '—'}
      </div>
    </div>
    <div class="summary-cell">
      <div class="label">24-hour trades</div>
      <div class="value">{formatInteger(selectedPair?.trades_24h ?? summary.trades_24h)}</div>
    </div>
    <div class="summary-cell">
    <div class="label">Verified swaps</div>
      <div class="value">{formatInteger(summary.trade_count)}</div>
    </div>
    <div class="summary-cell">
    <div class="label">Token pairs / DEX programs</div>
      <div class="value">{formatInteger(summary.pair_count)} / {formatInteger(summary.program_count)}</div>
    </div>
  </section>

  <section class="panel market-chart-panel">
    <div class="panel-toolbar market-toolbar">
      <div>
        <h2>DEX-program volume over time</h2>
        <span class="panel-toolbar-detail">All SPYx pairs · executed program · SPYx volume</span>
      </div>
      <div class="intervals" aria-label="DEX volume time range">
        {#each volumeRanges as option (option.id)}
          <button
            type="button"
            class:active={volumeRange === option.id}
            aria-pressed={volumeRange === option.id}
            onclick={() => selectVolumeRange(option.id)}
          >{option.label}</button>
        {/each}
      </div>
    </div>

    {#if volumeError}
      <div class="detail-error" role="alert">{volumeError}</div>
    {/if}

    {#if volumeLoading && !programVolume}
      <div class="chart-empty compact" aria-live="polite">Loading DEX volume history…</div>
    {:else if programVolume?.points.length && programChartSeries.length}
      <div class="program-chart" {@attach attachProgramVolumeChart}></div>
      <div class="program-legend" aria-label="DEX-program chart legend">
        {#each programChartSeries as entry (entry.address ?? 'other')}
          <span title={entry.address ?? undefined}>
            <i style={`--series-color: ${entry.color}`}></i>
            <strong>{entry.name}</strong>
            {formatProgramVolume(entry.totalRaw, programVolume.target_decimals)} SPYx
          </span>
        {/each}
      </div>
      <div class="chart-footer">
        <span>{formatInteger(programVolume.points.length)} non-empty intervals</span>
        <span>Top five DEX programs; remaining programs are combined</span>
      </div>
    {:else if !volumeError}
      <div class="chart-empty compact">No DEX volume is available for this range.</div>
    {/if}
  </section>

  <section class="panel">
    <div class="panel-toolbar">
      <h2>All-time volume by DEX program</h2>
      <span class="panel-toolbar-detail">Ranked by SPYx volume · routers remain separate evidence</span>
    </div>
    <div class="table-wrap">
      <table class="dex-programs">
        <thead>
          <tr>
            <th>DEX</th>
            <th class="numeric">Trades</th>
            <th class="numeric">SPYx volume</th>
            <th class="numeric dex-secondary">Pairs</th>
            <th class="numeric dex-secondary">Primary pools</th>
            <th class="numeric dex-secondary">Routed swaps</th>
          </tr>
        </thead>
        <tbody>
          {#each programs as program (program.program.address)}
            <tr>
              <td class="identity-cell">
                <strong>{program.program.name}</strong>
                <span class="mono" title={program.program.address}>{shortAddress(program.program.address)}</span>
              </td>
              <td class="numeric">{formatInteger(program.trade_count)}</td>
              <td class="numeric">{formatRawAmount(program.target_volume_raw, summary.target_decimals, 4)}</td>
              <td class="numeric dex-secondary">{formatInteger(program.pair_count)}</td>
              <td class="numeric dex-secondary">{formatInteger(program.primary_pool_count)}</td>
              <td class="numeric dex-secondary">{formatInteger(program.routed_trade_count)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>

  <section class="panel market-chart-panel">
    <div class="panel-toolbar market-toolbar">
      <div>
        <h2>SPYx price and {quoteLabel} volume</h2>
        <span class="panel-toolbar-detail">
          Token-2022 displayed SPYx
          {#if selectedPair?.latest_price}
            · latest swap multiplier ×{selectedPair.latest_price.target_multiplier}
          {/if}
        </span>
      </div>
      <div class="market-controls">
        <label>
          <span class="sr-only">Quote mint</span>
          <select value={selectedQuoteMint} onchange={selectPair} disabled={detailLoading}>
            {#each pairs as pair (pair.quote_mint)}
              <option value={pair.quote_mint} title={pair.quote_mint}>
                SPYx / {quoteIdentity(pair.quote_mint)} · {formatCompact(pair.trade_count)} trades
              </option>
            {/each}
          </select>
        </label>
        <div class="intervals" aria-label="Price resolution">
          {#each resolutions as option (option.id)}
            <button
              type="button"
              class:active={priceResolution === option.id}
              aria-pressed={priceResolution === option.id}
              onclick={() => selectResolution(option.id)}
            >{option.label}</button>
          {/each}
        </div>
      </div>
    </div>

    {#if detailError}
      <div class="detail-error" role="alert">{detailError}</div>
    {/if}

    {#if detailLoading && !candles.length && !slotCandles.length}
      <div class="chart-empty" aria-live="polite">Loading pair history…</div>
    {:else if candles.length || slotCandles.length}
      <div class="chart-wrap market-chart-wrap">
        <div
          class="market-chart"
          role="img"
          aria-label={`Interactive SPYx price and executed quote volume chart in ${selectedPair ? quoteSymbol(selectedPair.quote_mint) : quoteLabel}. ${priceResolution === 'slot' ? `${slotCandles.length} trading slots` : `${candles.length} non-empty ${priceResolution} candles`}.`}
          {@attach attachMarketChart}
        ></div>
      </div>
      <div class="chart-footer">
        <span>
          {#if priceResolution === 'slot'}
            {formatInteger(slotCandles.length)} non-empty slots
          {:else}
            {formatInteger(candles.length)} non-empty candles
          {/if}
        </span>
        <span>
          {#if priceResolution === 'slot' && slotCandles.length}
            Latest slot: {formatSlot(slotCandles[slotCandles.length - 1].slot)}
          {:else}
            Latest executed swap: {formatUtcDateTime(selectedPair?.last_block_time ?? null)} UTC
          {/if}
        </span>
      </div>
    {:else if !detailError}
      <div class="chart-empty">No price points are available for this pair and resolution.</div>
    {/if}
  </section>

  <section class="panel">
    <div class="panel-toolbar">
      <h2>Recent verified swaps</h2>
      <span class="panel-toolbar-detail">Executed DEX, router, pool, instruction path, and source transaction</span>
    </div>
    <div class="table-wrap desktop-trades-wrap">
      <table class="market-trades">
        <thead>
          <tr>
            <th>Time (UTC)</th>
            <th>Side</th>
            <th class="numeric">Price</th>
            <th class="numeric">SPYx</th>
            <th class="numeric">Quote</th>
            <th>DEX</th>
            <th>Router</th>
            <th>Pool</th>
            <th>Instruction</th>
            <th>Transaction</th>
          </tr>
        </thead>
        <tbody>
          {#each trades as trade (trade.trade_id)}
            <tr>
              <td>{formatUtcDateTime(trade.block_time)}</td>
              <td><span class:buy={trade.side === 'buy'} class:sell={trade.side === 'sell'} class="side">{trade.side}</span></td>
              <td class="numeric">{formatPrice(trade.price.display)}</td>
              <td
                class="numeric"
                title={`On-chain amount before the UI multiplier: ${formatRawAmount(trade.target_amount_raw, trade.target_decimals)}`}
              >{formatRawAmount(trade.target_amount_scaled_ui_raw, trade.target_decimals)}</td>
              <td class="numeric">{formatRawAmount(trade.quote_amount_raw, trade.quote_decimals)}</td>
              <td class="identity-cell">
                <strong>{trade.program.name}</strong>
                <span class="mono" title={trade.program.address}>{shortAddress(trade.program.address)}</span>
              </td>
              <td class="identity-cell">
                {#if trade.router}
                  <strong>{trade.router.name}</strong>
                  <span class="mono" title={trade.router.address}>{shortAddress(trade.router.address)}</span>
                {:else}
                  <span>Direct / unattributed</span>
                {/if}
              </td>
              <td class="mono" title={trade.pool ?? undefined}>{trade.pool ? shortAddress(trade.pool) : '—'}</td>
              <td class="mono">{trade.outer_index}{trade.inner_index === null ? '' : `.${trade.inner_index}`} · h{trade.stack_height}</td>
              <td>
                <a
                  class="signature-link mono"
                  href={resolve(
                    trade.signature
                      ? `/search?signature=${encodeURIComponent(trade.signature)}`
                      : `/search?transaction_id=${trade.transaction_id}`
                  )}
                  title={trade.signature ?? `Transaction ${trade.transaction_id}`}
                >
                  {trade.signature ? shortAddress(trade.signature) : `#${formatInteger(trade.transaction_id)}`}
                </a>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <div class="mobile-trades" aria-label="Recent verified swaps">
      {#each trades as trade (trade.trade_id)}
        <article class="mobile-trade">
          <header>
            <time>{formatUtcDateTime(trade.block_time)}</time>
            <span class:buy={trade.side === 'buy'} class:sell={trade.side === 'sell'} class="side">{trade.side}</span>
          </header>
          <dl class="mobile-trade-summary">
            <div><dt>Price</dt><dd>{formatPrice(trade.price.display)} {quoteSymbol(trade.quote_mint)}</dd></div>
            <div>
              <dt>SPYx</dt>
              <dd title={`On-chain amount before the UI multiplier: ${formatRawAmount(trade.target_amount_raw, trade.target_decimals)}`}>
                {formatRawAmount(trade.target_amount_scaled_ui_raw, trade.target_decimals)}
              </dd>
            </div>
            <div><dt>DEX program</dt><dd>{trade.program.name}</dd></div>
            <div>
              <dt>Signature</dt>
              <dd>
                <a
                  class="signature-link mono"
                  href={resolve(
                    trade.signature
                      ? `/search?signature=${encodeURIComponent(trade.signature)}`
                      : `/search?transaction_id=${trade.transaction_id}`
                  )}
                  title={trade.signature ?? `Index record ${trade.transaction_id}`}
                >
                  {trade.signature ? shortAddress(trade.signature) : `#${formatInteger(trade.transaction_id)}`}
                </a>
              </dd>
            </div>
          </dl>
          <details>
            <summary>Instruction details</summary>
            <dl>
              <div><dt>Quote amount</dt><dd>{formatRawAmount(trade.quote_amount_raw, trade.quote_decimals)}</dd></div>
              <div><dt>Router</dt><dd>{trade.router?.name ?? 'Direct / unattributed'}</dd></div>
              <div><dt>Pool</dt><dd class="mono">{trade.pool ? shortAddress(trade.pool) : '—'}</dd></div>
              <div><dt>Instruction</dt><dd class="mono">{trade.outer_index}{trade.inner_index === null ? '' : `.${trade.inner_index}`} · stack {trade.stack_height}</dd></div>
            </dl>
          </details>
        </article>
      {/each}
    </div>
    {#if !trades.length}
      <div class="chart-empty">No recent swaps are available for this pair.</div>
    {/if}
  </section>

  <section class="market-proof" aria-label="Market data proof">
    <span class="market-proof-icon"><ShieldCheck size={17} strokeWidth={1.8} /></span>
    <p>
      Prices use executed token transfers and the SPYx Token-2022 multiplier active at that instruction. Raw on-chain amounts stay available as proof. Failed transactions, router-only calls, unresolved flows, and whole-transaction balance guesses are excluded.
      {#if provenance}
        Parser {provenance.parserVersion}; market index <span class="mono" title={provenance.artifactSha256}>{shortAddress(provenance.artifactSha256)}</span>.
      {/if}
    </p>
  </section>
{/if}

<style>
  .market-state {
    min-height: 86px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 14px;
    padding: 16px;
  }

  .market-state > div {
    flex: 1;
  }

  .market-state h2 {
    margin-bottom: 4px;
  }

  .market-state p,
  .market-proof p {
    margin: 0;
    color: var(--muted);
  }

  .market-state.error {
    border-color: #edc9b5;
    background: var(--warn-weak);
  }

  .market-state button,
  .intervals button,
  select {
    min-height: 32px;
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    background: var(--surface);
  }

  .market-state button {
    padding: 0 10px;
  }

  .spin {
    display: inline-flex;
    flex: 0 0 auto;
    color: var(--accent);
    animation: spin 1.2s linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  .market-summary .value {
    display: flex;
    align-items: baseline;
    gap: 6px;
  }

  .unit {
    color: var(--muted);
    font-size: 11px;
    font-weight: 500;
  }

  .market-toolbar {
    align-items: center;
  }

  .market-toolbar > div:first-child {
    display: grid;
    gap: 2px;
  }

  .market-controls,
  .intervals {
    display: flex;
    align-items: center;
    gap: 6px;
  }

  select {
    max-width: 320px;
    padding: 0 28px 0 9px;
  }

  .intervals button {
    min-width: 38px;
    padding: 0 7px;
    color: var(--muted);
  }

  .intervals button.active {
    border-color: #b7ded8;
    color: var(--accent);
    background: var(--accent-weak);
  }

  .market-chart-wrap {
    min-height: 340px;
  }

  .market-chart {
    display: block;
    width: 100%;
    height: 340px;
  }

  .program-chart {
    display: block;
    width: 100%;
    height: 270px;
  }

  .program-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 7px 16px;
    padding: 10px 12px;
    border-top: 1px solid var(--border);
    color: var(--muted);
    font-size: 11px;
  }

  .program-legend span {
    display: inline-flex;
    align-items: center;
    gap: 5px;
  }

  .program-legend strong {
    color: var(--text);
    font-weight: 650;
  }

  .program-legend i {
    width: 14px;
    height: 3px;
    border-radius: 2px;
    background: var(--series-color);
  }

  .chart-footer {
    min-height: 38px;
    display: flex;
    justify-content: space-between;
    gap: 12px;
    padding: 9px 12px;
    border-top: 1px solid var(--border);
    color: var(--muted);
    font-size: 12px;
  }

  .chart-empty {
    min-height: 150px;
    display: grid;
    place-items: center;
    padding: 20px;
    color: var(--muted);
  }

  .chart-empty.compact {
    min-height: 120px;
  }

  .detail-error {
    padding: 9px 12px;
    border-bottom: 1px solid #edc9b5;
    color: var(--warn);
    background: var(--warn-weak);
    font-size: 12px;
  }

  .market-trades {
    min-width: 1320px;
  }

  .dex-programs {
    min-width: 720px;
  }

  .mobile-trades {
    display: none;
  }

  .identity-cell {
    min-width: 150px;
  }

  .identity-cell strong,
  .identity-cell span {
    display: block;
  }

  .identity-cell span {
    margin-top: 2px;
    color: var(--muted);
    font-size: 11px;
  }

  .side {
    display: inline-block;
    min-width: 38px;
    padding: 2px 6px;
    border-radius: 5px;
    text-align: center;
    text-transform: capitalize;
    font-size: 11px;
    font-weight: 650;
  }

  .side.buy {
    color: var(--accent);
    background: var(--accent-weak);
  }

  .side.sell {
    color: var(--warn);
    background: var(--warn-weak);
  }

  .market-proof {
    display: flex;
    align-items: flex-start;
    gap: 9px;
    margin: 0 2px 16px;
    font-size: 12px;
  }

  .market-proof-icon {
    display: inline-flex;
    flex: 0 0 auto;
    color: var(--accent);
  }

  button:hover,
  select:hover {
    border-color: var(--border-strong);
  }

  button:focus-visible,
  select:focus-visible,
  a:focus-visible {
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
    .market-state {
      align-items: stretch;
      flex-direction: column;
    }

    .market-toolbar,
    .market-controls,
    .chart-footer {
      align-items: stretch;
      flex-direction: column;
    }

    select {
      width: 100%;
      max-width: none;
    }

    .intervals button {
      flex: 1;
    }

    .program-legend {
      gap: 8px 12px;
    }

    .market-state button,
    .intervals button,
    select {
      min-height: 44px;
    }

    .market-chart-wrap,
    .market-chart {
      min-height: 280px;
      height: 280px;
    }

    .dex-programs {
      min-width: 0;
      table-layout: fixed;
    }

    .dex-programs .dex-secondary {
      display: none;
    }

    .dex-programs th,
    .dex-programs td {
      padding-inline: 8px;
    }

    .dex-programs th:first-child {
      width: 45%;
    }

    .dex-programs .identity-cell {
      min-width: 0;
    }

    .desktop-trades-wrap {
      display: none;
    }

    .mobile-trades {
      display: block;
    }

    .mobile-trade {
      padding: 11px 12px;
      border-bottom: 1px solid var(--border);
    }

    .mobile-trade:last-child {
      border-bottom: 0;
    }

    .mobile-trade header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }

    .mobile-trade time {
      color: var(--muted);
      font-size: 11px;
    }

    .mobile-trade dl {
      margin: 0;
    }

    .mobile-trade-summary > div,
    .mobile-trade details dl > div {
      display: grid;
      grid-template-columns: 96px minmax(0, 1fr);
      gap: 9px;
      padding: 3px 0;
    }

    .mobile-trade dd {
      min-width: 0;
      margin: 0;
      overflow-wrap: anywhere;
    }

    .mobile-trade details {
      margin-top: 8px;
      border-top: 1px solid var(--border);
    }

    .mobile-trade summary {
      min-height: 40px;
      display: flex;
      align-items: center;
      cursor: pointer;
      color: var(--muted);
      font-size: 12px;
    }
  }

  @media (prefers-reduced-motion: reduce) {
    .spin { animation: none; }
  }
</style>
