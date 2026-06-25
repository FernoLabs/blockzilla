const state = {
  tokens: [],
  selected: null,
  mode: "line",
  interval: "1m",
};

const els = {
  list: document.querySelector("#token-list"),
  count: document.querySelector("#token-count"),
  search: document.querySelector("#token-search"),
  reload: document.querySelector("#reload-button"),
  title: document.querySelector("#token-title"),
  address: document.querySelector("#token-address"),
  price: document.querySelector("#price-value"),
  change: document.querySelector("#change-value"),
  volume: document.querySelector("#volume-value"),
  holders: document.querySelector("#holders-value"),
  range: document.querySelector("#range-value"),
  metaDecimals: document.querySelector("#meta-decimals"),
  metaProgram: document.querySelector("#meta-program"),
  metaMintId: document.querySelector("#meta-mint-id"),
  metaBalanceRows: document.querySelector("#meta-balance-rows"),
  metaSwapRows: document.querySelector("#meta-swap-rows"),
  chart: document.querySelector("#price-chart"),
  chartStatus: document.querySelector("#chart-status"),
  interval: document.querySelector("#interval-select"),
  lineMode: document.querySelector("#line-mode"),
  candleMode: document.querySelector("#candle-mode"),
  trades: document.querySelector("#trades-body"),
  openJson: document.querySelector("#open-json-button"),
};

init();

function init() {
  els.search.addEventListener("input", renderTokenList);
  els.reload.addEventListener("click", loadTokens);
  els.interval.addEventListener("change", () => {
    state.interval = els.interval.value;
    refreshSelected();
  });
  els.lineMode.addEventListener("click", () => setMode("line"));
  els.candleMode.addEventListener("click", () => setMode("candle"));
  els.openJson.addEventListener("click", () => {
    if (!state.selected) return;
    window.open(`/defi/token_overview?address=${encodeURIComponent(state.selected.address)}`, "_blank");
  });
  loadTokens();
}

async function loadTokens() {
  els.list.innerHTML = `<div class="empty">Loading tokens</div>`;
  try {
    const payload = await getJson("/defi/tokenlist?limit=200");
    state.tokens = payload.data?.items || payload.data?.tokens || [];
    renderTokenList();
    if (state.tokens.length && !state.selected) {
      selectToken(state.tokens[0].address);
    }
  } catch (err) {
    els.list.innerHTML = `<div class="empty">${escapeHtml(err.message)}</div>`;
  }
}

function renderTokenList() {
  const query = els.search.value.trim().toLowerCase();
  const tokens = state.tokens.filter((token) => {
    return !query || token.address.toLowerCase().includes(query);
  });
  els.count.textContent = String(tokens.length);
  if (!tokens.length) {
    els.list.innerHTML = `<div class="empty">No matching tokens</div>`;
    return;
  }
  els.list.innerHTML = "";
  for (const token of tokens) {
    const row = document.createElement("button");
    row.type = "button";
    row.className = `token-row${state.selected?.address === token.address ? " active" : ""}`;
    row.innerHTML = `
      <div>
        <strong>${shortAddress(token.address)}</strong>
        <span>${formatMoney(token.price)} / ${formatCompact(token.v24hUSD || token.volume24hUSD || 0)} vol</span>
      </div>
      <span>${formatCompact(token.holder || 0)}</span>
    `;
    row.addEventListener("click", () => selectToken(token.address));
    els.list.appendChild(row);
  }
}

async function selectToken(address) {
  state.selected = { address };
  renderTokenList();
  setLoadingToken(address);
  await refreshSelected();
}

async function refreshSelected() {
  if (!state.selected) return;
  const address = state.selected.address;
  try {
    const [overview, chart, trades] = await Promise.all([
      getJson(`/defi/token_overview?address=${encodeURIComponent(address)}`),
      getJson(chartPath(address)),
      getJson(`/defi/v3/token/txs?address=${encodeURIComponent(address)}&limit=40`),
    ]);
    if (state.selected?.address !== address) return;
    renderOverview(overview.data);
    renderChart(extractChartItems(chart), state.mode, chart.data?.fill || "none");
    renderTrades(trades.data?.items || []);
  } catch (err) {
    els.chartStatus.textContent = err.message;
  }
}

function chartPath(address) {
  const encoded = encodeURIComponent(address);
  const fill = state.mode === "line" ? "linear" : "flat";
  if (state.mode === "line") {
    return `/defi/history_price?address=${encoded}&type=${encodeURIComponent(state.interval)}&fill=${fill}&max_points=900`;
  }
  return `/defi/v3/ohlcv?address=${encoded}&type=${encodeURIComponent(state.interval)}&fill=${fill}&max_points=900`;
}

function setMode(mode) {
  state.mode = mode;
  els.lineMode.classList.toggle("active", mode === "line");
  els.candleMode.classList.toggle("active", mode === "candle");
  refreshSelected();
}

function setLoadingToken(address) {
  els.title.textContent = shortAddress(address);
  els.address.textContent = address;
  els.price.textContent = "-";
  els.change.textContent = "-";
  els.volume.textContent = "-";
  els.holders.textContent = "-";
  els.range.textContent = "-";
  renderMetadata(null);
  els.trades.innerHTML = `<tr><td colspan="6" class="empty">Loading swaps</td></tr>`;
  els.chartStatus.textContent = "Loading chart";
  clearCanvas();
}

function renderOverview(data) {
  const address = data?.address || state.selected.address;
  els.title.textContent = shortAddress(address);
  els.address.textContent = address;
  els.price.textContent = formatMoney(data?.price);
  els.change.textContent = formatPercent(data?.priceChange24hPercent || 0);
  els.change.classList.toggle("positive", (data?.priceChange24hPercent || 0) > 0);
  els.change.classList.toggle("negative", (data?.priceChange24hPercent || 0) < 0);
  els.volume.textContent = formatCompact(data?.volume24hUSD || data?.v24hUSD || 0);
  els.holders.textContent = formatCompact(data?.holder || 0);
  const ext = data?.extensions || {};
  els.range.textContent = ext.firstSlot ? `${ext.firstSlot} -> ${ext.lastSlot}` : "-";
  renderMetadata(data);
}

function renderMetadata(data) {
  const ext = data?.extensions || {};
  els.metaDecimals.textContent = data?.decimals ?? "-";
  els.metaProgram.textContent = shortAddress(ext.program || "");
  els.metaProgram.title = ext.program || "";
  els.metaMintId.textContent = ext.mintId ?? "-";
  els.metaBalanceRows.textContent = formatCompact(ext.balanceChangeRows || 0);
  els.metaSwapRows.textContent = formatCompact(ext.swapRows || 0);
}

function renderTrades(items) {
  if (!items.length) {
    els.trades.innerHTML = `<tr><td colspan="6" class="empty">No swaps for this token</td></tr>`;
    return;
  }
  els.trades.innerHTML = items.map((trade) => `
    <tr>
      <td>${formatTime(trade.blockUnixTime)}</td>
      <td>${escapeHtml(trade.side || "")}</td>
      <td>${formatMoney(trade.price)}</td>
      <td>${formatCompact(trade.volumeUSD || 0)}</td>
      <td class="mono">${shortAddress(trade.owner || "")}</td>
      <td class="mono">${shortAddress(trade.dexProgram || "")}</td>
    </tr>
  `).join("");
}

function extractChartItems(payload) {
  return payload.data?.items || [];
}

function renderChart(items, mode, fill) {
  const canvas = els.chart;
  const ctx = canvas.getContext("2d");
  const rect = canvas.getBoundingClientRect();
  const ratio = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * ratio));
  canvas.height = Math.max(1, Math.floor(rect.height * ratio));
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);

  const width = rect.width;
  const height = rect.height;
  ctx.clearRect(0, 0, width, height);

  const values = mode === "line"
    ? items.map((item) => Number(item.value)).filter(Number.isFinite)
    : items.flatMap((item) => [item.o, item.h, item.l, item.c].map(Number)).filter(Number.isFinite);

  if (!values.length) {
    els.chartStatus.textContent = "No chart data";
    drawEmptyChart(ctx, width, height);
    return;
  }

  els.chartStatus.textContent = `${items.length} archival points${fill && fill !== "none" ? `, ${fill} gap fill` : ""}`;
  const pad = { left: 54, right: 16, top: 18, bottom: 34 };
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || Math.max(max, 1);

  drawGrid(ctx, width, height, pad, min, max);
  if (mode === "line") {
    drawLine(ctx, items, width, height, pad, min, span);
  } else {
    drawCandles(ctx, items, width, height, pad, min, span);
  }
}

function drawGrid(ctx, width, height, pad, min, max) {
  ctx.strokeStyle = getCss("--grid");
  ctx.fillStyle = getCss("--muted");
  ctx.lineWidth = 1;
  ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
  for (let i = 0; i <= 4; i += 1) {
    const y = pad.top + ((height - pad.top - pad.bottom) * i) / 4;
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(width - pad.right, y);
    ctx.stroke();
    const value = max - ((max - min) * i) / 4;
    ctx.fillText(formatMoney(value), 8, y + 4);
  }
}

function drawLine(ctx, items, width, height, pad, min, span) {
  const usableW = width - pad.left - pad.right;
  const usableH = height - pad.top - pad.bottom;
  const points = items
    .map((item, index) => ({ x: pad.left + (usableW * index) / Math.max(items.length - 1, 1), y: pad.top + usableH - ((Number(item.value) - min) / span) * usableH }))
    .filter((point) => Number.isFinite(point.y));
  ctx.strokeStyle = getCss("--accent");
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((point, index) => {
    if (index === 0) ctx.moveTo(point.x, point.y);
    else ctx.lineTo(point.x, point.y);
  });
  ctx.stroke();
}

function drawCandles(ctx, items, width, height, pad, min, span) {
  const usableW = width - pad.left - pad.right;
  const usableH = height - pad.top - pad.bottom;
  const candleW = Math.max(3, Math.min(14, usableW / Math.max(items.length, 1) * 0.58));
  items.forEach((item, index) => {
    const x = pad.left + (usableW * (index + 0.5)) / Math.max(items.length, 1);
    const open = Number(item.o);
    const high = Number(item.h);
    const low = Number(item.l);
    const close = Number(item.c);
    if (![open, high, low, close].every(Number.isFinite)) return;
    const y = (value) => pad.top + usableH - ((value - min) / span) * usableH;
    const up = close >= open;
    ctx.strokeStyle = up ? getCss("--ok") : getCss("--danger");
    ctx.fillStyle = ctx.strokeStyle;
    ctx.beginPath();
    ctx.moveTo(x, y(high));
    ctx.lineTo(x, y(low));
    ctx.stroke();
    const top = Math.min(y(open), y(close));
    const bodyH = Math.max(1, Math.abs(y(open) - y(close)));
    ctx.fillRect(x - candleW / 2, top, candleW, bodyH);
  });
}

function drawEmptyChart(ctx, width, height) {
  ctx.fillStyle = getCss("--muted");
  ctx.font = "14px ui-sans-serif, system-ui, sans-serif";
  ctx.fillText("No archival price points for this token", 24, height / 2);
}

function clearCanvas() {
  const ctx = els.chart.getContext("2d");
  ctx.clearRect(0, 0, els.chart.width, els.chart.height);
}

async function getJson(path) {
  const response = await fetch(path, { headers: { accept: "application/json" } });
  const payload = await response.json();
  if (!response.ok || payload.success === false) {
    throw new Error(payload.message || `Request failed: ${response.status}`);
  }
  return payload;
}

function shortAddress(value) {
  if (!value) return "-";
  if (value.length <= 14) return value;
  return `${value.slice(0, 6)}...${value.slice(-6)}`;
}

function formatMoney(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  if (number === 0) return "$0";
  if (Math.abs(number) < 0.0001) return `$${number.toExponential(2)}`;
  if (Math.abs(number) < 1) return `$${number.toFixed(6)}`;
  return new Intl.NumberFormat(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 4 }).format(number);
}

function formatPercent(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  return `${number >= 0 ? "+" : ""}${number.toFixed(2)}%`;
}

function formatCompact(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 2 }).format(number);
}

function formatTime(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return "-";
  return new Date(number * 1000).toLocaleString();
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
}

function getCss(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

window.addEventListener("resize", () => {
  if (state.selected) refreshSelected();
});
