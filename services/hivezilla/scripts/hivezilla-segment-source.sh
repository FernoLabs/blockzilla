#!/usr/bin/env bash
set -euo pipefail

# Server-side half of whole-WAL replication. Install as root and grant the
# unprivileged transfer account passwordless sudo for only this executable.

HIVEZILLA_SEGMENT_SOURCE_ENV="${HIVEZILLA_SEGMENT_SOURCE_ENV:-/etc/hivezilla/segment-source.env}"
if [[ -r $HIVEZILLA_SEGMENT_SOURCE_ENV ]]; then
  # shellcheck source=/dev/null
  source "$HIVEZILLA_SEGMENT_SOURCE_ENV"
fi
: "${HIVEZILLA_SEGMENT_DIR:?set HIVEZILLA_SEGMENT_DIR}"
HIVEZILLA_RECORDER_LABEL="${HIVEZILLA_RECORDER_LABEL:-com.docker.compose.service=hivezilla-shred}"
HIVEZILLA_RETIRE_RECEIPTS="${HIVEZILLA_RETIRE_RECEIPTS:-/var/lib/hivezilla/segment-retire-receipts}"
HIVEZILLA_TRANSFER_USER="${HIVEZILLA_TRANSFER_USER:-hivezilla-transfer}"

segments() {
  find "$HIVEZILLA_SEGMENT_DIR" -maxdepth 1 -type f \
    -name 'segment-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].wal' \
    -printf '%f\n' | LC_ALL=C sort
}

manifest() {
  [[ $EUID -eq 0 ]] || { echo 'manifest must run as root' >&2; exit 1; }
  mapfile -t all < <(segments)
  ((${#all[@]} > 1)) || exit 0
  # The recorder intentionally creates WALs as 0600. Expose only the immutable
  # sealed prefix, read-only, immediately before producing its digest list.
  for file in "${all[@]:0:${#all[@]}-1}"; do
    setfacl -m "u:$HIVEZILLA_TRANSFER_USER:r,m:r" "$HIVEZILLA_SEGMENT_DIR/$file"
  done
  cd "$HIVEZILLA_SEGMENT_DIR"
  printf '%s\n' "${all[@]:0:${#all[@]}-1}" | xargs -r sha256sum --
}

retire() {
  [[ $EUID -eq 0 ]] || { echo 'retire must run as root' >&2; exit 1; }
  install -d -m 0750 "$HIVEZILLA_RETIRE_RECEIPTS"
  request=$(mktemp "$HIVEZILLA_RETIRE_RECEIPTS/.request.XXXXXX")
  cleanup() { unlink "$request" 2>/dev/null || true; }
  trap cleanup EXIT
  cat >"$request"
  [[ -s $request ]] || { echo 'empty retirement manifest' >&2; exit 1; }
  (($(wc -l <"$request") <= 4096)) || { echo 'retirement manifest is too large' >&2; exit 1; }

  awk '
    NF != 2 { exit 1 }
    $1 !~ /^[0-9a-f]{64}$/ { exit 1 }
    $2 !~ /^segment-[0-9]{20}\.wal$/ { exit 1 }
    seen[$2]++ { exit 1 }
  ' "$request"

  active=$(segments | tail -n1)
  [[ -n $active ]] || { echo 'source has no active segment' >&2; exit 1; }
  while read -r _ file; do
    [[ $file != "$active" ]] || { echo 'manifest includes active segment' >&2; exit 1; }
    [[ -f "$HIVEZILLA_SEGMENT_DIR/$file" ]] || { echo "missing source segment: $file" >&2; exit 1; }
  done <"$request"

  mapfile -t recorders < <(docker ps --filter "label=$HIVEZILLA_RECORDER_LABEL" --format '{{.ID}}')
  ((${#recorders[@]} == 1)) || { echo 'expected exactly one recorder container' >&2; exit 1; }
  recorder=${recorders[0]}
  docker stop -t 30 "$recorder" >/dev/null
  restart() { docker start "$recorder" >/dev/null 2>&1 || true; }
  trap 'restart; cleanup' EXIT

  cd "$HIVEZILLA_SEGMENT_DIR"
  sha256sum -c "$request" >/dev/null
  while read -r _ file; do unlink -- "$file"; done <"$request"
  sync -f "$HIVEZILLA_SEGMENT_DIR"

  manifest_sha=$(sha256sum "$request" | awk '{print $1}')
  count=$(wc -l <"$request" | tr -d ' ')
  receipt="$HIVEZILLA_RETIRE_RECEIPTS/$manifest_sha.json"
  printf '{"version":1,"manifest_sha256":"%s","segment_count":%s,"retired_at":"%s"}\n' \
    "$manifest_sha" "$count" "$(date -u +%FT%TZ)" >"$receipt"
  sync -f "$receipt"
  sync -f "$HIVEZILLA_RETIRE_RECEIPTS"
  restart
  trap cleanup EXIT
  printf 'RETIRED=%s MANIFEST_SHA256=%s\n' "$count" "$manifest_sha"
}

case "${1:-}" in
  manifest) manifest ;;
  retire) retire ;;
  *) echo 'usage: hivezilla-segment-source.sh manifest|retire' >&2; exit 2 ;;
esac
