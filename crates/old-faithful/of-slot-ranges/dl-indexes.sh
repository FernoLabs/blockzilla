#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://files.old-faithful.net"
DOWNLOAD_SLOT_INDEX="${DOWNLOAD_SLOT_INDEX:-1}"
DOWNLOAD_CID_INDEX="${DOWNLOAD_CID_INDEX:-1}"

START_EPOCH="${1:-0}"
END_EPOCH="${2:-0}"
OUT_DIR="${3:-indexes}"
LIST_FILE="${OUT_DIR}/urls-${START_EPOCH}-${END_EPOCH}-$(date +%Y%m%d-%H%M%S).txt"

fetch_file() {
  local url="$1"
  local path="$2"
  local tmp="${path}.tmp.$$"

  if command -v curl >/dev/null 2>&1 && curl -fSsL "$url" -o "$tmp"; then
    mv "$tmp" "$path"
    return 0
  fi

  rm -f "$tmp"
  if command -v wget >/dev/null 2>&1 && wget -q -U "curl/8" -O "$tmp" "$url"; then
    mv "$tmp" "$path"
    return 0
  fi

  rm -f "$tmp"
  echo "failed to fetch $url" >&2
  return 1
}

mkdir -p "$OUT_DIR"
: > "$LIST_FILE"

echo "Building URL list in: $LIST_FILE"
echo "Epoch range: $START_EPOCH..$END_EPOCH"

for ((e=START_EPOCH; e<=END_EPOCH; e++)); do
  epoch_dir="$OUT_DIR/$e"
  mkdir -p "$epoch_dir"

  # Fetch epoch CID (tiny). Store locally too.
  cid_url="$BASE_URL/$e/epoch-$e.cid"
  cid_path="$epoch_dir/epoch-$e.cid"

  if [[ ! -s "$cid_path" ]]; then
    fetch_file "$cid_url" "$cid_path"
  fi

  epoch_cid="$(tr -d '\n\r ' < "$cid_path")"

  slot_idx="epoch-$e-$epoch_cid-mainnet-slot-to-cid.index"
  cid_idx="epoch-$e-$epoch_cid-mainnet-cid-to-offset-and-size.index"

  # aria2 input format: URL then indented options lines.
  if [[ "$DOWNLOAD_SLOT_INDEX" == "1" && ! -s "$epoch_dir/$slot_idx" ]]; then
    {
      echo "$BASE_URL/$e/$slot_idx"
      echo "  dir=$epoch_dir"
      echo "  out=$slot_idx"
    } >> "$LIST_FILE"
  fi

  if [[ "$DOWNLOAD_CID_INDEX" == "1" && ! -s "$epoch_dir/$cid_idx" ]]; then
    {
      echo "$BASE_URL/$e/$cid_idx"
      echo "  dir=$epoch_dir"
      echo "  out=$cid_idx"
    } >> "$LIST_FILE"
  fi
done

url_count="$(grep -c '^https\?://' "$LIST_FILE" || true)"
echo "URL list done: $url_count files"

if [[ "$url_count" -eq 0 ]]; then
  echo "All requested index files already exist."
  exit 0
fi

echo "Downloading with aria2c (4 concurrent) into per-epoch dirs under: $OUT_DIR"
aria2c \
  -c \
  -j4 \
  -x16 \
  -s16 \
  --auto-file-renaming=false \
  --allow-overwrite=true \
  --check-certificate=true \
  --file-allocation=none \
  -i "$LIST_FILE"

echo "Done."
