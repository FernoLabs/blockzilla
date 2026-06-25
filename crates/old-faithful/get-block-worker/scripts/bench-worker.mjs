#!/usr/bin/env node

import { performance } from "node:perf_hooks";

const DEFAULT_SLOT = 424223999;

function parseArgs(argv) {
  const args = {
    url: process.env.WORKER_URL || "",
    route: "block-lite",
    format: "json",
    rewards: "false",
    start: DEFAULT_SLOT,
    count: 100,
    concurrency: 8,
    duration: 0,
    ramp: "",
    stride: 1,
    sameSlot: false,
    cacheBust: false,
    timeoutMs: 30000,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") {
      usage("", 0);
    } else if (arg === "--same-slot") {
      args.sameSlot = true;
    } else if (arg === "--cache-bust") {
      args.cacheBust = true;
    } else if (arg.startsWith("--")) {
      const key = arg.slice(2).replace(/-([a-z])/g, (_, c) => c.toUpperCase());
      const value = argv[++i];
      if (value == null) usage(`Missing value for ${arg}`);
      args[key] = value;
    } else {
      usage(`Unknown argument: ${arg}`);
    }
  }

  args.start = Number(args.start);
  args.count = Number(args.count);
  args.concurrency = Number(args.concurrency);
  args.duration = Number(args.duration);
  args.stride = Number(args.stride);
  args.timeoutMs = Number(args.timeoutMs);

  if (!args.url) usage("Set --url or WORKER_URL");
  if (!Number.isFinite(args.start) || args.start < 0) usage("--start must be a slot number");
  if (!Number.isFinite(args.count) || args.count < 1) usage("--count must be >= 1");
  if (!Number.isFinite(args.concurrency) || args.concurrency < 1) {
    usage("--concurrency must be >= 1");
  }
  if (!["block", "block-lite"].includes(args.route)) usage("--route must be block or block-lite");
  if (!["json", "bin"].includes(args.format)) usage("--format must be json or bin");
  if (!["true", "false"].includes(args.rewards)) usage("--rewards must be true or false");
  return args;
}

function usage(message, code = 2) {
  if (message) console.error(`error: ${message}\n`);
  console.error(`Usage:
  node scripts/bench-worker.mjs --url https://worker.example.workers.dev [options]

Options:
  --route block-lite|block       Default: block-lite
  --format json|bin              Default: json
  --rewards true|false           Default: false
  --start SLOT                   Default: ${DEFAULT_SLOT}
  --count N                      Total requests per phase. Default: 100
  --concurrency N                Parallel requests. Default: 8
  --duration SECONDS             Run each phase for wall-clock duration instead of count
  --ramp LIST                    Comma list of concurrencies, for example 1,2,4,8,16,32
  --same-slot                    Reuse --start for every request; useful for hot-cache ceiling
  --cache-bust                   Add a unique query param; useful to avoid edge/browser cache
  --stride N                     Slot increment for sequential runs. Default: 1
  --timeout-ms N                 Per-request timeout. Default: 30000

Examples:
  WORKER_URL=https://x.workers.dev npm run bench -- --count 100 --concurrency 8
  npm run bench -- --url https://x.workers.dev --ramp 1,2,4,8,16,32 --duration 20 --same-slot
`);
  process.exit(code);
}

function blockUrl(args, requestIndex) {
  const slot = args.sameSlot ? args.start : args.start + requestIndex * args.stride;
  const url = new URL(`${args.url.replace(/\/+$/, "")}/${args.route}/${slot}.${args.format}`);
  url.searchParams.set("rewards", args.rewards);
  if (args.cacheBust) url.searchParams.set("_bench", String(requestIndex));
  return url;
}

function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return sorted[index];
}

async function fetchBlock(args, requestIndex) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), args.timeoutMs);
  const started = performance.now();
  try {
    const response = await fetch(blockUrl(args, requestIndex), {
      signal: controller.signal,
      headers: { accept: args.format === "bin" ? "application/x-protobuf" : "application/json" },
    });
    const body = await response.arrayBuffer();
    return {
      status: response.status,
      bytes: body.byteLength,
      ms: performance.now() - started,
      cache: response.headers.get("cf-cache-status") || "",
    };
  } catch (error) {
    return {
      status: error.name === "AbortError" ? "timeout" : "fetch_error",
      bytes: 0,
      ms: performance.now() - started,
      cache: "",
    };
  } finally {
    clearTimeout(timeout);
  }
}

function emptyStats() {
  return {
    total: 0,
    ok: 0,
    bytes: 0,
    latencies: [],
    statuses: new Map(),
    cacheStatuses: new Map(),
  };
}

function record(stats, result) {
  stats.total += 1;
  if (typeof result.status === "number" && result.status >= 200 && result.status < 300) {
    stats.ok += 1;
    stats.bytes += result.bytes;
  }
  stats.latencies.push(result.ms);
  stats.statuses.set(result.status, (stats.statuses.get(result.status) || 0) + 1);
  if (result.cache) {
    stats.cacheStatuses.set(result.cache, (stats.cacheStatuses.get(result.cache) || 0) + 1);
  }
}

async function runPhase(args, concurrency) {
  const stats = emptyStats();
  let next = 0;
  const started = performance.now();
  const stopAt = args.duration > 0 ? started + args.duration * 1000 : Infinity;

  async function workerLoop() {
    while (next < args.count && performance.now() < stopAt) {
      const requestIndex = next;
      next += 1;
      record(stats, await fetchBlock(args, requestIndex));
    }
  }

  await Promise.all(Array.from({ length: concurrency }, workerLoop));
  const elapsedMs = performance.now() - started;
  return { concurrency, elapsedMs, ...stats };
}

function printSummary(summary) {
  const seconds = summary.elapsedMs / 1000;
  const sorted = [...summary.latencies].sort((a, b) => a - b);
  const mib = summary.bytes / 1024 / 1024;
  const statuses = [...summary.statuses.entries()]
    .map(([status, count]) => `${status}:${count}`)
    .join(" ");
  const cache = [...summary.cacheStatuses.entries()]
    .map(([status, count]) => `${status}:${count}`)
    .join(" ");

  console.log(
    [
      `concurrency=${summary.concurrency}`,
      `requests=${summary.total}`,
      `ok=${summary.ok}`,
      `elapsed=${seconds.toFixed(2)}s`,
      `blocks/s=${(summary.ok / seconds).toFixed(2)}`,
      `MiB=${mib.toFixed(2)}`,
      `MiB/s=${(mib / seconds).toFixed(2)}`,
      `avgKiB=${summary.ok ? ((summary.bytes / summary.ok) / 1024).toFixed(1) : "0.0"}`,
      `p50=${percentile(sorted, 0.50).toFixed(0)}ms`,
      `p95=${percentile(sorted, 0.95).toFixed(0)}ms`,
      `p99=${percentile(sorted, 0.99).toFixed(0)}ms`,
      `status=${statuses || "none"}`,
      cache ? `cf-cache=${cache}` : "",
    ]
      .filter(Boolean)
      .join(" "),
  );
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const phases = args.ramp
    ? args.ramp.split(",").map((value) => Number(value.trim())).filter(Boolean)
    : [args.concurrency];

  console.log(
    `target=${args.url} route=${args.route} format=${args.format} rewards=${args.rewards} ` +
      `start=${args.start} sameSlot=${args.sameSlot} cacheBust=${args.cacheBust}`,
  );
  for (const concurrency of phases) {
    printSummary(await runPhase(args, concurrency));
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
