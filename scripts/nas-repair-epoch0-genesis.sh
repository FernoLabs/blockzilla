#!/usr/bin/env bash
set -euo pipefail

# Repair the one legacy metadata enum tag in the existing epoch-0 hot archive
# and add its missing registry MPHF. This intentionally does not rebuild or
# invent blocks, PoH entries, shred boundaries, signatures, or block-access
# sidecars.

EPOCH_DIR="${EPOCH_DIR:-/volume1/@home/ach/dev/blockzilla-v2/epoch-0}"
BLOCKZILLA_BIN="${BLOCKZILLA_BIN:-/volume1/@home/ach/dev/blockzilla-pipeline/releases/blockzilla-nas-pipeline-2026.07.13-resource-scheduled-reuse-ui-3/bin/blockzilla}"
GENESIS_ARCHIVE="${GENESIS_ARCHIVE:-/volume1/blockzilla/genesis.tar.bz2}"
BACKUP_ROOT="${BACKUP_ROOT:-/volume1/@home/ach/dev/blockzilla-pipeline/backups}"
HIVEZILLA_STATUS_URL="${HIVEZILLA_STATUS_URL:-http://127.0.0.1:8787/api/v1/status}"

LEGACY_META_SHA256="${LEGACY_META_SHA256:-62919229ca6cfd83019a8481de965818d825a3abd5eca3293dc11c13bf658383}"
CURRENT_META_SHA256="${CURRENT_META_SHA256:-d50c0641d9f422ee01a8f7473c2f098e69c2b3cb26788188f239065458f0ae10}"
REGISTRY_SHA256="${REGISTRY_SHA256:-f92402683e3acac1ff4979b5264e26620c452ca95e122acd53689e4f8f50face}"
GENESIS_SHA256="${GENESIS_SHA256:-133f7eaefcd59466f3b291aadd1b0d3522432072cf5b539445218c6c125ea945}"
MPHF_SHA256="${MPHF_SHA256:-077cb300d44b0a3a30cc1b953b6bcb89d563858fd49b40516a124bee6dc90a07}"

usage() {
  echo "usage: $0 --apply" >&2
  echo "The exact epoch-0 source hashes and sidecar sizes are verified before publication." >&2
}

if [[ "${1:-}" != "--apply" || $# -ne 1 ]]; then
  usage
  exit 2
fi

meta="$EPOCH_DIR/archive-v2-meta.wincode"
registry="$EPOCH_DIR/registry.bin"
registry_index="$EPOCH_DIR/registry.mphf"
lock_dir="$EPOCH_DIR/.epoch-0-genesis-repair.lock"
meta_candidate="$EPOCH_DIR/.archive-v2-meta.wincode.epoch0-repair.$$.tmp"
index_candidate="$EPOCH_DIR/.registry.mphf.epoch0-repair.$$.tmp"

if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "epoch-0 repair lock already exists: $lock_dir" >&2
  exit 1
fi

cleanup() {
  rm -f "$meta_candidate" "$index_candidate" "$index_candidate.tmp"
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for path in "$EPOCH_DIR" "$meta" "$registry" "$GENESIS_ARCHIVE" "$BLOCKZILLA_BIN"; do
  if [[ ! -e "$path" ]]; then
    echo "required path is missing: $path" >&2
    exit 1
  fi
done
if [[ ! -x "$BLOCKZILLA_BIN" ]]; then
  echo "blockzilla binary is not executable: $BLOCKZILLA_BIN" >&2
  exit 1
fi

# Refuse to touch the archive while a blockzilla process has the exact epoch-0
# directory or source CAR as an argument. The long-running Hivezilla controller
# only has the archive root in its argv and is therefore not a false positive.
python3 - "$EPOCH_DIR" <<'PY'
import os
import pathlib
import sys

epoch_dir = sys.argv[1]
blocked = []
for proc in pathlib.Path("/proc").glob("[0-9]*"):
    try:
        argv = (proc / "cmdline").read_bytes().split(b"\0")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        continue
    args = [arg.decode(errors="replace") for arg in argv if arg]
    if not args or "blockzilla" not in os.path.basename(args[0]):
        continue
    if epoch_dir in args or any(
        arg.endswith("/epoch-0.car") or arg.endswith("/epoch-0.car.zst")
        for arg in args[1:]
    ):
        blocked.append((proc.name, args))
if blocked:
    for pid, args in blocked:
        print(f"active epoch-0 worker pid={pid}: {' '.join(args)}", file=sys.stderr)
    raise SystemExit("refusing repair while epoch-0 worker is active")
PY

actual_meta_sha="$(sha256sum "$meta" | awk '{print $1}')"
actual_registry_sha="$(sha256sum "$registry" | awk '{print $1}')"
actual_genesis_sha="$(sha256sum "$GENESIS_ARCHIVE" | awk '{print $1}')"

if [[ "$actual_registry_sha" != "$REGISTRY_SHA256" ]]; then
  echo "registry hash changed: expected=$REGISTRY_SHA256 actual=$actual_registry_sha" >&2
  exit 1
fi
if [[ "$actual_genesis_sha" != "$GENESIS_SHA256" ]]; then
  echo "genesis hash changed: expected=$GENESIS_SHA256 actual=$actual_genesis_sha" >&2
  exit 1
fi

# Require every already-built, applicable sidecar to retain the exact audited
# size. Empty/synthetic substitutes would fail here.
python3 - "$EPOCH_DIR" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
expected = {
    "archive-v2-blocks.zstd": 74_044_326,
    "archive-v2-blocks.index": 22_440_532,
    "archive-v2-meta.wincode": 99_588,
    "registry.bin": 14_336,
    "registry_counts.bin": 471,
    "blockhash_registry.bin": 13_809_568,
    "poh.wincode": 1_008_339_925,
    "shredding.wincode": 63_399_571,
    "signatures.bin": 110_392_384,
    "vote_hash_registry.bin": 28_050_620,
}
for name, size in expected.items():
    path = root / name
    actual = path.stat().st_size if path.is_file() else None
    if actual != size:
        raise SystemExit(f"audited sidecar changed: {path} expected={size} actual={actual}")
print("verified applicable epoch-0 sidecars and exact audited sizes")
PY

if [[ "$actual_meta_sha" == "$CURRENT_META_SHA256" && -s "$registry_index" ]]; then
  echo "epoch-0 metadata is already migrated; validating existing registry index"
  python3 - "$registry_index" <<'PY'
import os
import struct
import sys

path = sys.argv[1]
with open(path, "rb") as source:
    header = source.read(20)
if len(header) != 20 or header[:8] != b"BZKIDX1!":
    raise SystemExit("invalid registry.mphf magic/header")
version, header_len, keys = struct.unpack("<HHQ", header[8:20])
if (version, header_len, keys) != (2, 20, 448):
    raise SystemExit(f"invalid registry.mphf header: {(version, header_len, keys)}")
if os.path.getsize(path) < 20 + keys * 12 + 1:
    raise SystemExit("registry.mphf is truncated")
PY
  actual_mphf_sha="$(sha256sum "$registry_index" | awk '{print $1}')"
  if [[ "$actual_mphf_sha" != "$MPHF_SHA256" ]]; then
    echo "registry MPHF hash changed: expected=$MPHF_SHA256 actual=$actual_mphf_sha" >&2
    exit 1
  fi
  "$BLOCKZILLA_BIN" bench-archive-v2 "$meta" --iterations 1
  echo "epoch-0 repair is already complete"
  exit 0
fi

if [[ "$actual_meta_sha" != "$LEGACY_META_SHA256" ]]; then
  echo "metadata hash is neither the audited legacy nor repaired value: $actual_meta_sha" >&2
  exit 1
fi
if [[ -e "$registry_index" ]]; then
  echo "unexpected pre-existing registry index with legacy metadata: $registry_index" >&2
  exit 1
fi

# Validate all three legacy frames, change only the Genesis enum tag (1 -> 4),
# and fsync the same-filesystem candidate before any production rename.
python3 - "$meta" "$meta_candidate" "$CURRENT_META_SHA256" <<'PY'
import hashlib
import os
import stat
import sys

source_path, candidate_path, expected_sha = sys.argv[1:]
source = bytearray(open(source_path, "rb").read())

def read_varint(buf, pos):
    value = 0
    shift = 0
    start = pos
    while True:
        if pos >= len(buf) or shift > 28:
            raise SystemExit("invalid metadata frame length")
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7f) << shift
        if byte & 0x80 == 0:
            return start, pos, value
        shift += 7

frames = []
pos = 0
while pos < len(source):
    start, payload, length = read_varint(source, pos)
    end = payload + length
    if end > len(source):
        raise SystemExit("metadata frame extends beyond EOF")
    frames.append((start, payload, length, source[payload], end))
    pos = end
expected = [
    (0, 1, 3, 0, 4),
    (4, 7, 99_539, 1, 99_546),
    (99_546, 99_547, 41, 2, 99_588),
]
if frames != expected or pos != len(source):
    raise SystemExit(f"unexpected legacy metadata frame shape: {frames}")

source[7] = 4
actual_sha = hashlib.sha256(source).hexdigest()
if actual_sha != expected_sha:
    raise SystemExit(f"candidate metadata hash mismatch: {actual_sha}")

mode = stat.S_IMODE(os.stat(source_path).st_mode)
fd = os.open(candidate_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
try:
    with os.fdopen(fd, "wb", closefd=False) as target:
        target.write(source)
        target.flush()
    os.fsync(fd)
finally:
    os.close(fd)
PY

candidate_meta_sha="$(sha256sum "$meta_candidate" | awk '{print $1}')"
if [[ "$candidate_meta_sha" != "$CURRENT_META_SHA256" ]]; then
  echo "metadata candidate hash mismatch after write: $candidate_meta_sha" >&2
  exit 1
fi
"$BLOCKZILLA_BIN" bench-archive-v2 "$meta_candidate" --iterations 1

"$BLOCKZILLA_BIN" build-archive-v2-registry-index \
  "$registry" --output "$index_candidate" --force

python3 - "$index_candidate" <<'PY'
import os
import struct
import sys

path = sys.argv[1]
with open(path, "rb") as source:
    header = source.read(20)
if len(header) != 20 or header[:8] != b"BZKIDX1!":
    raise SystemExit("invalid registry MPHF candidate magic/header")
version, header_len, keys = struct.unpack("<HHQ", header[8:20])
if (version, header_len, keys) != (2, 20, 448):
    raise SystemExit(f"invalid registry MPHF candidate header: {(version, header_len, keys)}")
if os.path.getsize(path) < 20 + keys * 12 + 1:
    raise SystemExit("registry MPHF candidate is truncated")
with open(path, "rb") as source:
    os.fsync(source.fileno())
print(f"verified registry MPHF candidate: keys={keys} bytes={os.path.getsize(path)}")
PY

candidate_mphf_sha="$(sha256sum "$index_candidate" | awk '{print $1}')"
if [[ "$candidate_mphf_sha" != "$MPHF_SHA256" ]]; then
  echo "registry MPHF candidate hash mismatch: expected=$MPHF_SHA256 actual=$candidate_mphf_sha" >&2
  exit 1
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_dir="$BACKUP_ROOT/epoch-0-genesis-repair-$timestamp-$$"
mkdir -p "$backup_dir"
cp -p "$meta" "$backup_dir/archive-v2-meta.wincode.legacy"
sha256sum \
  "$backup_dir/archive-v2-meta.wincode.legacy" \
  "$registry" \
  "$GENESIS_ARCHIVE" >"$backup_dir/source-sha256.txt"

python3 - "$backup_dir" <<'PY'
import os
import sys

path = sys.argv[1]
fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY

# Match the validated production migration: publish the deterministic MPHF,
# publish metadata second as the reader-compatibility commit, then fsync the
# containing directory.
python3 - "$meta_candidate" "$meta" "$index_candidate" "$registry_index" "$EPOCH_DIR" <<'PY'
import os
import sys

meta_candidate, meta, index_candidate, registry_index, parent = sys.argv[1:]
os.replace(index_candidate, registry_index)
os.replace(meta_candidate, meta)
fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY

final_meta_sha="$(sha256sum "$meta" | awk '{print $1}')"
if [[ "$final_meta_sha" != "$CURRENT_META_SHA256" ]]; then
  echo "published metadata hash mismatch: $final_meta_sha" >&2
  exit 1
fi
"$BLOCKZILLA_BIN" bench-archive-v2 "$meta" --iterations 1

python3 - "$HIVEZILLA_STATUS_URL" <<'PY'
import json
import sys
import time
import urllib.request

url = sys.argv[1]
last = None
for _ in range(8):
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            snapshot = json.load(response)
        epoch = next(item for item in snapshot["epochs"] if item["epoch"] == 0)
        last = epoch
        if epoch["state"] == "complete":
            print(
                "hivezilla epoch-0 state=complete "
                f"registry_order={epoch['registry_order']} message={epoch.get('message')}"
            )
            break
    except Exception as error:
        last = {"status_error": str(error)}
    time.sleep(2)
else:
    raise SystemExit(f"Hivezilla did not classify epoch 0 complete: {last}")
PY

echo "epoch-0 repair complete"
echo "legacy metadata backup: $backup_dir/archive-v2-meta.wincode.legacy"
