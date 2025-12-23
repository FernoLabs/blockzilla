#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./optimize_car.sh <start> <end> [dest_dir] [--verify] [--min-free-pct=N]
# Example:
#   ./optimize_car.sh 0 874 /volume1/blockzilla --verify --min-free-pct=10

# -------- positional args --------
START="${1:-}"
END="${2:-}"
DEST_DIR="${3:-./epochs}"

if [[ -z "${START}" || -z "${END}" ]]; then
  echo "Usage: $0 <start> <end> [dest_dir] [--verify] [--min-free-pct=N]" >&2
  exit 1
fi

# -------- defaults / options --------
VERIFY="no"
MIN_FREE_PCT=10
ZSTD_LVL=3          # zstd -3 recommended
ZSTD_THREADS=0      # 0 = all cores for this single file

# parse optional flags starting from arg #4
if (( $# >= 3 )); then
  shift 3
else
  shift $#
fi
for arg in "${@:-}"; do
  case "$arg" in
    --verify) VERIFY="yes" ;;
    --min-free-pct=*)
      MIN_FREE_PCT="${arg#*=}"
      if ! [[ "$MIN_FREE_PCT" =~ ^[0-9]+$ ]] || (( MIN_FREE_PCT <= 0 || MIN_FREE_PCT >= 50 )); then
        echo "Invalid --min-free-pct. Use integer 1..49." >&2
        exit 1
      fi
      ;;
    "")
      ;; # ignore empty
    *)
      echo "Unknown option: $arg" >&2
      exit 1
      ;;
  esac
done

# -------- requirements --------
for cmd in zstd df awk; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: $cmd not found in PATH" >&2
    exit 1
  fi
done

mkdir -p "${DEST_DIR}"

timestamp() { date +"%Y-%m-%d %H:%M:%S"; }
free_pct() { df -P "${DEST_DIR}" | awk 'NR==2 {gsub("%","",$5); print 100-$5}'; }

require_free_space() {
  local need="$1"; local cur; cur="$(free_pct)"
  if (( cur < need )); then
    echo "$(timestamp) free: ${cur}% < required ${need}%." >&2
    return 1
  fi
  return 0
}

compress_one() {
  local car="$1"
  local zst="${car}.zst"
  if [[ -f "$zst" ]]; then
    echo "$(timestamp) compress: exists $(basename "$zst") - skip"
    return 0
  fi
  echo "$(timestamp) compress: start $(basename "$car") (zstd -${ZSTD_LVL}, T=${ZSTD_THREADS})"
  zstd -T"${ZSTD_THREADS}" -"${ZSTD_LVL}" --rm "$car"
  if [[ "${VERIFY}" == "yes" ]]; then
    echo "$(timestamp) verify: $(basename "$zst")"
    zstd -t "$zst"
  fi
  echo "$(timestamp) compress: done  $(basename "$zst")  free=$(free_pct)%"
}

compress_epoch() {
  local i="$1"
  local car="${DEST_DIR}/epoch-${i}.car"
  local zst="${DEST_DIR}/epoch-${i}.car.zst"

  # Already compressed
  if [[ -f "$zst" ]]; then
    echo "$(timestamp) epoch-${i}: already compressed - skip"
    return 0
  fi

  if [[ ! -f "$car" ]]; then
    echo "$(timestamp) epoch-${i}: missing ${car} - cannot compress." >&2
    exit 2
  fi

  if ! require_free_space "${MIN_FREE_PCT}"; then
    echo "$(timestamp) epoch-${i}: low space before compression - abort." >&2
    exit 3
  fi

  compress_one "$car"
}

echo "$(timestamp) config: dest=${DEST_DIR} keep_free>=${MIN_FREE_PCT}% zstd=-${ZSTD_LVL} T=${ZSTD_THREADS} verify=${VERIFY}"
echo "$(timestamp) fs: $(df -P "${DEST_DIR}" | awk 'NR==2{print $1" on "$6", free "100-$5"%"}')"

# Process only the requested range
for i in $(seq "${START}" "${END}"); do
  compress_epoch "$i"
done

echo "$(timestamp) all done."
