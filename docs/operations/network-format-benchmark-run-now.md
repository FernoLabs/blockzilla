# Network format benchmark execution now

Status: the epoch-0 NAS exact-slot run and the Mac cold/warm run are complete. Keep this file as the historical NAS command record. For a new Mac run, use `docs/operations/network-format-benchmark-run-mac.sh` with a new result directory. Do not reuse an existing result or cache directory.

Goal: run one controlled network comparison on epoch 0 using three readers:

- Jetstreamer network CAR scan.
- Comparator remote Compact V2 + local CAR + remote V3 scan.
- Compact V2 network baseline (full epoch).

Rules:
- Do not hash binaries or source archives.
- Do not overwrite existing files.
- Keep each run in its own named output files.
- Keep cold and warm V3 cache runs separate.
- Epoch 0 is structural-only (no Pump/USDC semantic signal).

## 1) Runbook preflight

```bash
set -euo pipefail

NAS_BIN_DIR=/volume1/blockzilla/scheduler-state/index-archive-bin
SCRATCH=/volume1/blockzilla/firewatch-parity-scratch
EPOCH=0
WALLET=11111111111111111111111111111111
ORIGIN=https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev
CAR_BASE="$ORIGIN/car"
COMPACT_BASE="$ORIGIN/compact-v2"
V3_BASE="$ORIGIN/indexer-v3"

# No-overwrite check
test ! -e "$SCRATCH/jetstreamer-epoch-${EPOCH}-slots-0-1023-r2.json"
test ! -e "$SCRATCH/jetstreamer-epoch-${EPOCH}-slots-0-1023-r2.log"
test ! -e "$SCRATCH/three-format-cloudflare-epoch-${EPOCH}-prefix-1024-cache-cold-r1.json"
test ! -e "$SCRATCH/three-format-cloudflare-epoch-${EPOCH}-prefix-1024-cache-cold-r1.log"
test ! -e "$SCRATCH/three-format-cloudflare-epoch-${EPOCH}-prefix-1024-cache-warm-r1.json"
test ! -e "$SCRATCH/three-format-cloudflare-epoch-${EPOCH}-prefix-1024-cache-warm-r1.log"
```

## 2) Install/prepare binaries

Use these exact names in NAS to avoid collisions:

```bash
cp /tmp/upload_source/archive-three-format-compare-musl \
   "$NAS_BIN_DIR/archive-three-format-network-compare-cache-r1"
cp /tmp/upload_source/jetstreamer-semantic-compare-musl \
   "$NAS_BIN_DIR/jetstreamer-semantic-compare-r2"
```

```bash
chmod 0500 "$NAS_BIN_DIR/archive-three-format-network-compare-cache-r1"
chmod 0500 "$NAS_BIN_DIR/jetstreamer-semantic-compare-r2"
```

```bash
"$NAS_BIN_DIR/archive-three-format-network-compare-cache-r1" --help
"$NAS_BIN_DIR/jetstreamer-semantic-compare-r2" --help
```

## 3) Validate worker and object availability (small smoke)

```bash
curl -fsS -D - -o /dev/null -H 'Range: bytes=0-0' "$CAR_BASE/0/epoch-${EPOCH}.car"
curl -fsS -D - -o /dev/null "$CAR_BASE/0/epoch-${EPOCH}-slot-ranges.raw"
curl -fsS -D - -o /dev/null -H 'Range: bytes=0-0' "$COMPACT_BASE/v1/epochs/0/archive-v2-blocks.zstd"
curl -fsS -D - -o /dev/null -H 'Range: bytes=0-0' "$V3_BASE/v1/epochs/0/archive-v2-standalone-messages.wincode"
```

## 4) Start Jetstreamer run (exact slots 0..1023)

```bash
wrangler tail --format json --search "benchmark_gateway_success" > "$SCRATCH/jetstreamer-r2-tail.log" &
WRANGLER_TAIL_PID=$!

"$NAS_BIN_DIR/jetstreamer-semantic-compare-r2" \
  --epoch "$EPOCH" \
  --end-slot-inclusive 1023 \
  --wallet "$WALLET" \
  --download-threads 4 \
  --buffer-window-bytes 268435456 \
  --http-base "$CAR_BASE/" \
  --index-base "$CAR_BASE/" \
  --max-transactions 5000000 \
  --max-records 10000000 \
  --sample-limit 20 \
  > "$SCRATCH/jetstreamer-epoch-${EPOCH}-slots-0-1023-r2.json" \
  2> "$SCRATCH/jetstreamer-epoch-${EPOCH}-slots-0-1023-r2.log"

kill "$WRANGLER_TAIL_PID" || true
```

## 5) V3 warm/cold comparator with cache

Create a fresh cache root and run cold then warm:

```bash
V3_CACHE_ROOT="$SCRATCH/v3-network-cache-epoch-${EPOCH}-r1"
mkdir -m 700 -p "$V3_CACHE_ROOT"
```

```bash
"$NAS_BIN_DIR/archive-three-format-network-compare-cache-r1" \
  --car /volume1/blockzilla/old-faithful/epoch-${EPOCH}.car \
  --trusted-car \
  --compact-v2-base-url "$COMPACT_BASE" \
  --v3-base-url "$V3_BASE/" \
  --v3-cache-root "$V3_CACHE_ROOT" \
  --epoch "$EPOCH" \
  --slots-per-epoch 432000 \
  --wallet "$WALLET" \
  --scratch "$SCRATCH" \
  --mismatch-limit 20 \
  --max-blocks 1024 \
  > "$SCRATCH/three-format-cloudflare-epoch-${EPOCH}-prefix-1024-cache-cold-r1.json" \
  2> "$SCRATCH/three-format-cloudflare-epoch-${EPOCH}-prefix-1024-cache-cold-r1.log"
```

```bash
"$NAS_BIN_DIR/archive-three-format-network-compare-cache-r1" \
  --car /volume1/blockzilla/old-faithful/epoch-${EPOCH}.car \
  --trusted-car \
  --compact-v2-base-url "$COMPACT_BASE" \
  --v3-base-url "$V3_BASE/" \
  --v3-cache-root "$V3_CACHE_ROOT" \
  --epoch "$EPOCH" \
  --slots-per-epoch 432000 \
  --wallet "$WALLET" \
  --scratch "$SCRATCH" \
  --mismatch-limit 20 \
  --max-blocks 1024 \
  > "$SCRATCH/three-format-cloudflare-epoch-${EPOCH}-prefix-1024-cache-warm-r1.json" \
  2> "$SCRATCH/three-format-cloudflare-epoch-${EPOCH}-prefix-1024-cache-warm-r1.log"
```

## 6) Validation checks

1. Jetstreamer output must show:
   - `possible_block_count == 1024`
   - `transactions == 4091`
   - `complete_blocks`/`slot_range` end at 1023
2. Compare digest fields between Jetstreamer and comparator reports:
   - transaction-universe checksum
   - pump/USDC/wallet coverage digests
   - exact parity flags
3. Comparator must show:
   - `exact_three_way_parity == true`
   - `all_workloads_complete == false` (epoch-0 is structural)
   - `v3_http_requests == 30` for cold and 18 for warm after coalescing (example)
   - `v3_cache` shows one cache miss then cache hits
4. Keep all `report` files for future audit; do not delete.
