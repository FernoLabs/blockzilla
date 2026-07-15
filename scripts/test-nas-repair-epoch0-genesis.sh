#!/usr/bin/env bash
set -euo pipefail

# This file doubles as the fake blockzilla used by its isolated repair fixture.
case "${1:-}" in
  bench-archive-v2)
    input="$2"
    python3 - "$input" "$CURRENT_META_SHA256" <<'PY'
import hashlib
import sys

path, expected = sys.argv[1:]
payload = open(path, "rb").read()
actual = hashlib.sha256(payload).hexdigest()
if actual != expected:
    raise SystemExit(f"fake reader rejected metadata hash: {actual}")
print(
    "archive_v2_read iterations=1 records=3 blocks=0 txs=0 "
    f"input_bytes={len(payload)} payload_bytes={len(payload) - 5}"
)
PY
    exit 0
    ;;
  build-archive-v2-registry-index)
    output=""
    shift
    while [[ $# -gt 0 ]]; do
      if [[ "$1" == "--output" ]]; then
        output="$2"
        shift 2
      else
        shift
      fi
    done
    if [[ -z "$output" ]]; then
      echo "fake blockzilla did not receive --output" >&2
      exit 1
    fi
    python3 - "$output" <<'PY'
import struct
import sys

path = sys.argv[1]
header = b"BZKIDX1!" + struct.pack("<HHQ", 2, 20, 448)
with open(path, "xb") as target:
    target.write(header)
    target.write(bytes(5_548 - len(header)))
PY
    exit 0
    ;;
esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repair_script="$script_dir/nas-repair-epoch0-genesis.sh"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/epoch0-repair-test.XXXXXX")"
trap 'rm -rf "$test_root"' EXIT INT TERM

epoch_dir="$test_root/epoch-0"
backup_root="$test_root/backups"
genesis="$test_root/genesis.tar.bz2"
status="$test_root/status.json"
mkdir -p "$epoch_dir" "$backup_root"

read -r legacy_meta_sha current_meta_sha registry_sha genesis_sha mphf_sha < <(
  python3 - "$epoch_dir" "$genesis" "$status" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

epoch_dir = Path(sys.argv[1])
genesis = Path(sys.argv[2])
status = Path(sys.argv[3])

legacy = bytearray()
legacy += bytes([3, 0, 2, 1])
legacy += bytes.fromhex("d38906")
legacy += bytes([1]) + bytes(99_538)
legacy += bytes([41, 2]) + bytes(40)
if len(legacy) != 99_588:
    raise SystemExit(f"bad legacy fixture size: {len(legacy)}")
(epoch_dir / "archive-v2-meta.wincode").write_bytes(legacy)

current = bytearray(legacy)
current[7] = 4

registry = bytes(range(256)) * 56
if len(registry) != 14_336:
    raise SystemExit("bad registry fixture size")
(epoch_dir / "registry.bin").write_bytes(registry)
(epoch_dir / "registry_counts.bin").write_bytes(bytes(471))

sizes = {
    "archive-v2-blocks.zstd": 74_044_326,
    "archive-v2-blocks.index": 22_440_532,
    "blockhash_registry.bin": 13_809_568,
    "poh.wincode": 1_008_339_925,
    "shredding.wincode": 63_399_571,
    "signatures.bin": 110_392_384,
    "vote_hash_registry.bin": 28_050_620,
}
for name, size in sizes.items():
    with open(epoch_dir / name, "wb") as target:
        target.truncate(size)

genesis_payload = b"fixture genesis archive"
genesis.write_bytes(genesis_payload)
status.write_text(
    json.dumps(
        {
            "epochs": [
                {
                    "epoch": 0,
                    "state": "complete",
                    "registry_order": "usage_sorted",
                    "message": "isolated fixture",
                }
            ]
        }
    )
)

print(
    hashlib.sha256(legacy).hexdigest(),
    hashlib.sha256(current).hexdigest(),
    hashlib.sha256(registry).hexdigest(),
    hashlib.sha256(genesis_payload).hexdigest(),
    hashlib.sha256(
        b"BZKIDX1!" + __import__("struct").pack("<HHQ", 2, 20, 448) + bytes(5_528)
    ).hexdigest(),
)
PY
)

run_repair() {
  env \
    EPOCH_DIR="$epoch_dir" \
    BLOCKZILLA_BIN="$0" \
    GENESIS_ARCHIVE="$genesis" \
    BACKUP_ROOT="$backup_root" \
    HIVEZILLA_STATUS_URL="file://$status" \
    LEGACY_META_SHA256="$legacy_meta_sha" \
    CURRENT_META_SHA256="$current_meta_sha" \
    REGISTRY_SHA256="$registry_sha" \
    GENESIS_SHA256="$genesis_sha" \
    MPHF_SHA256="$mphf_sha" \
    "$repair_script" --apply
}

run_repair

test "$(sha256sum "$epoch_dir/archive-v2-meta.wincode" | awk '{print $1}')" = "$current_meta_sha"
test -s "$epoch_dir/registry.mphf"
test "$(find "$backup_root" -type f -name 'archive-v2-meta.wincode.legacy' | wc -l | tr -d ' ')" = "1"
test "$(sha256sum "$(find "$backup_root" -type f -name 'archive-v2-meta.wincode.legacy')" | awk '{print $1}')" = "$legacy_meta_sha"

# A second invocation must validate the published files and exit without a new
# backup or replacement.
run_repair
test "$(find "$backup_root" -type f -name 'archive-v2-meta.wincode.legacy' | wc -l | tr -d ' ')" = "1"
test "$(sha256sum "$epoch_dir/archive-v2-meta.wincode" | awk '{print $1}')" = "$current_meta_sha"

echo "epoch-0 repair fixture test passed"
