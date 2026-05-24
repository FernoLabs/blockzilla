#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 1 ]]; then
  cat <<'EOF'
Usage:
  bench-rpc-epoch-sample.sh [endpoint] <epoch|start-end|comma-list>...

Examples:
  scripts/bench-rpc-epoch-sample.sh 700 800 900
  scripts/bench-rpc-epoch-sample.sh https://api.mainnet-beta.solana.com 700,800,900
  SAMPLES_PER_EPOCH=5 SAMPLE_MODE=random SEED=42 scripts/bench-rpc-epoch-sample.sh 10-20
  DRY_RUN=1 scripts/bench-rpc-epoch-sample.sh 700 800

Environment:
  SLOT_INDEX_DIR       Directory with epoch-N-slot-ranges-v2.raw files
                       (or legacy .raw files when REQUIRE_V2=0).
                       Default: /volume1/blockzilla/slot-index
  SAMPLES_PER_EPOCH   Present slots to sample from each epoch. Default: 3
  SAMPLE_MODE         spread or random. Default: spread
  SEED                Random seed for SAMPLE_MODE=random. Default: 1
  REQUIRE_V2          Only sample epoch-N-slot-ranges-v2.raw indexes. Default: 1
  PREFER_V2           Prefer epoch-N-slot-ranges-v2.raw when present. Default: 1
                       Only used when REQUIRE_V2=0.
  DRY_RUN=1           Print sampled slots and exit without curl.

All other bench-rpc-getblock.sh environment variables are forwarded, including
ROUNDS, TRANSACTION_DETAILS, REWARDS, CURL_EXTRA, and MAX_SUPPORTED_TX_VERSION.
EOF
  exit 0
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

endpoint="https://cloudflare-solana-rpc.cheron-augustin.workers.dev/"
if [[ "${1:-}" =~ ^https?:// ]]; then
  endpoint="$1"
  shift
fi

if [[ $# -lt 1 ]]; then
  echo "bench-rpc-epoch-sample.sh: at least one epoch spec is required" >&2
  exit 2
fi

slot_index_dir="${SLOT_INDEX_DIR:-/volume1/blockzilla/slot-index}"
samples_per_epoch="${SAMPLES_PER_EPOCH:-3}"
sample_mode="${SAMPLE_MODE:-spread}"
seed="${SEED:-1}"
require_v2="${REQUIRE_V2:-1}"
prefer_v2="${PREFER_V2:-1}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
slots_file="$tmpdir/slots.txt"

python3 - "$slot_index_dir" "$samples_per_epoch" "$sample_mode" "$seed" "$require_v2" "$prefer_v2" "$@" >"$slots_file" <<'PY'
import os
import random
import struct
import sys

SLOTS_PER_EPOCH = 432_000

slot_index_dir = sys.argv[1]
samples_per_epoch = int(sys.argv[2])
sample_mode = sys.argv[3]
seed = int(sys.argv[4])
require_v2 = sys.argv[5] != "0"
prefer_v2 = sys.argv[6] != "0"
epoch_specs = sys.argv[7:]

if samples_per_epoch < 1:
    raise SystemExit("SAMPLES_PER_EPOCH must be >= 1")
if sample_mode not in ("spread", "random"):
    raise SystemExit("SAMPLE_MODE must be spread or random")

def expand_epoch_specs(specs):
    seen = set()
    out = []
    for spec in specs:
        for part in spec.replace(",", " ").split():
            if not part:
                continue
            if "-" in part:
                start_s, end_s = part.split("-", 1)
                start = int(start_s)
                end = int(end_s)
                step = 1 if end >= start else -1
                values = range(start, end + step, step)
            else:
                values = [int(part)]
            for epoch in values:
                if epoch < 0:
                    raise SystemExit(f"invalid negative epoch {epoch}")
                if epoch not in seen:
                    seen.add(epoch)
                    out.append(epoch)
    return out

def index_path(epoch):
    raw = os.path.join(slot_index_dir, f"epoch-{epoch}-slot-ranges.raw")
    v2 = os.path.join(slot_index_dir, f"epoch-{epoch}-slot-ranges-v2.raw")
    if require_v2:
        if os.path.isfile(v2):
            return v2, 44
        raise FileNotFoundError(
            f"missing v2 slot index for epoch {epoch} under {slot_index_dir}"
        )
    if prefer_v2 and os.path.isfile(v2):
        return v2, 44
    if os.path.isfile(raw):
        return raw, 12
    if os.path.isfile(v2):
        return v2, 44
    raise FileNotFoundError(f"missing slot index for epoch {epoch} under {slot_index_dir}")

def present_slots(epoch, path, stride):
    with open(path, "rb") as f:
        data = f.read()
    expected = SLOTS_PER_EPOCH * stride
    if len(data) != expected:
        raise SystemExit(f"{path} has {len(data)} bytes, expected {expected}")
    slots = []
    for slot_in_epoch in range(SLOTS_PER_EPOCH):
        off = slot_in_epoch * stride
        length = struct.unpack_from("<I", data, off + 8)[0]
        if length:
            slots.append(epoch * SLOTS_PER_EPOCH + slot_in_epoch)
    return slots

rng = random.Random(seed)
sampled = []
for epoch in expand_epoch_specs(epoch_specs):
    path, stride = index_path(epoch)
    slots = present_slots(epoch, path, stride)
    if not slots:
        print(f"epoch={epoch}: no present slots in {path}", file=sys.stderr)
        continue
    count = min(samples_per_epoch, len(slots))
    if sample_mode == "random":
        picks = sorted(rng.sample(slots, count))
    elif count == 1:
        picks = [slots[len(slots) // 2]]
    else:
        picks = [slots[round(i * (len(slots) - 1) / (count - 1))] for i in range(count)]
    print(
        f"epoch={epoch} index={path} present={len(slots)} sampled={','.join(map(str, picks))}",
        file=sys.stderr,
    )
    sampled.extend(picks)

for slot in sampled:
    print(slot)
PY

slots=()
while IFS= read -r slot; do
  slots+=("$slot")
done <"$slots_file"
if [[ ${#slots[@]} -eq 0 ]]; then
  echo "bench-rpc-epoch-sample.sh: no slots sampled" >&2
  exit 1
fi

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  printf "%s\n" "${slots[@]}"
  exit 0
fi

exec "$SCRIPT_DIR/bench-rpc-getblock.sh" "$endpoint" "${slots[@]}"
