#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./build_cache.sh <start> <end> [dest_dir] [base_url] [--verify] [--min-free-pct=N]
# Example:
#   ./build_cache.sh 0 874 /volume1/blockzilla https://files.old-faithful.net --verify --min-free-pct=10

# -------- positional args --------
START="${1:-}"
END="${2:-}"
DEST_DIR="${3:-./epochs}"
BASE_URL="${4:-https://files.old-faithful.net}"

if [[ -z "${START}" || -z "${END}" ]]; then
  echo "Usage: $0 <start> <end> [dest_dir] [base_url] [--verify] [--min-free-pct=N]" >&2
  exit 1
fi

# -------- defaults / options --------
VERIFY="no"
MIN_FREE_PCT=10
ZSTD_LVL=3          # zstd -3 recommended
ZSTD_THREADS=0      # 0 = all cores for this single file

# parse optional flags starting from arg #5
if (( $# >= 4 )); then
  shift 4
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
for cmd in aria2c zstd df awk; do
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

# ---- aria "unfinished download" detection / resume ----
# aria2 creates sidecar files while downloading:
#   - <file>.aria2
#   - <file>.aria2__temp (sometimes)
aria_unfinished_for() {
  local car="$1"
  [[ -f "${car}.aria2" || -f "${car}.aria2__temp" ]]
}

resume_with_aria() {
  local url="$1"
  local dest_dir="$2"
  echo "$(timestamp) aria: resume ${url}"
  aria2c -d "${dest_dir}" -c "${url}" -x 16 -s 16 --file-allocation=none
}

ensure_aria_complete() {
  local car="$1"
  local url="$2"
  local dest_dir="$3"

  if aria_unfinished_for "$car"; then
    resume_with_aria "$url" "$dest_dir"
  fi

  if aria_unfinished_for "$car"; then
    echo "$(timestamp) aria: still unfinished after aria2 run for $(basename "$car")" >&2
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

  # Safety: if aria is still writing, do not compress
  if aria_unfinished_for "$car"; then
    echo "$(timestamp) compress: refuse $(basename "$car") - aria2 sidecar present (download active/unfinished)" >&2
    return 1
  fi

  echo "$(timestamp) compress: start $(basename "$car") (zstd -${ZSTD_LVL}, T=${ZSTD_THREADS})"
  zstd -T"${ZSTD_THREADS}" -"${ZSTD_LVL}" --rm "$car"

  if [[ "${VERIFY}" == "yes" ]]; then
    echo "$(timestamp) verify: $(basename "$zst")"
    zstd -t "$zst"
  fi

  echo "$(timestamp) compress: done  $(basename "$zst")  free=$(free_pct)%"
}

download_and_compress_epoch() {
  local i="$1"
  local car="${DEST_DIR}/epoch-${i}.car"
  local zst="${DEST_DIR}/epoch-${i}.car.zst"
  local url="${BASE_URL}/${i}/epoch-${i}.car"

  # Already compressed
  if [[ -f "$zst" ]]; then
    echo "$(timestamp) epoch-${i}: already compressed - skip"
    return 0
  fi

  # If an unfinished aria download exists, resume it first
  if aria_unfinished_for "$car"; then
    echo "$(timestamp) epoch-${i}: unfinished aria download detected - resuming"
    ensure_aria_complete "$car" "$url" "$DEST_DIR" || exit 5
  fi

  # If raw exists (either previously present or resumed), compress it
  if [[ -f "$car" ]]; then
    if ! require_free_space "${MIN_FREE_PCT}"; then
      echo "$(timestamp) epoch-${i}: low space before compression - abort." >&2
      exit 2
    fi
    compress_one "$car"
    return 0
  fi

  # Ensure free space before download
  if ! require_free_space "${MIN_FREE_PCT}"; then
    echo "$(timestamp) epoch-${i}: free space < ${MIN_FREE_PCT}% - abort before download." >&2
    exit 3
  fi

  echo "$(timestamp) epoch-${i}: download ${url}"
  aria2c -d "${DEST_DIR}" -c "${url}" -x 16 -s 16 --file-allocation=none

  # After aria2 returns, ensure no unfinished markers remain (defensive)
  ensure_aria_complete "$car" "$url" "$DEST_DIR" || exit 6

  if [[ ! -f "$car" ]]; then
    echo "$(timestamp) epoch-${i}: download failed - ${car} missing." >&2
    exit 4
  fi

  # Compress the single downloaded file
  compress_one "$car"
}

echo "$(timestamp) config: dest=${DEST_DIR} base_url=${BASE_URL} keep_free>=${MIN_FREE_PCT}% zstd=-${ZSTD_LVL} T=${ZSTD_THREADS} verify=${VERIFY}"
echo "$(timestamp) fs: $(df -P "${DEST_DIR}" | awk 'NR==2{print $1" on "$6", free "100-$5"%"}')"

# Process only the requested range
for i in $(seq "${START}" "${END}"); do
  download_and_compress_epoch "$i"
done

echo "$(timestamp) all done."
