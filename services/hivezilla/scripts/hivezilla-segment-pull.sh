#!/usr/bin/env bash
set -euo pipefail

: "${HIVEZILLA_SOURCE:?set HIVEZILLA_SOURCE}"
: "${HIVEZILLA_SOURCE_DIR:?set HIVEZILLA_SOURCE_DIR}"
: "${HIVEZILLA_MIRROR_DIR:?set HIVEZILLA_MIRROR_DIR}"
HIVEZILLA_SOURCE_HELPER="${HIVEZILLA_SOURCE_HELPER:-/usr/local/sbin/hivezilla-segment-source}"
HIVEZILLA_MIN_SEGMENTS="${HIVEZILLA_MIN_SEGMENTS:-8}"
HIVEZILLA_SSH="${HIVEZILLA_SSH:-ssh -o BatchMode=yes -o ConnectTimeout=15}"

install -d -m 0750 "$HIVEZILLA_MIRROR_DIR/segments" "$HIVEZILLA_MIRROR_DIR/receipts"
exec 9>"$HIVEZILLA_MIRROR_DIR/pull.lock"
flock -n 9 || exit 0

work=$(mktemp -d "$HIVEZILLA_MIRROR_DIR/.pull.XXXXXX")
cleanup() { find "$work" -type f -delete 2>/dev/null || true; rmdir "$work" 2>/dev/null || true; }
trap cleanup EXIT
manifest="$work/manifest.sha256"
$HIVEZILLA_SSH "$HIVEZILLA_SOURCE" "sudo $HIVEZILLA_SOURCE_HELPER manifest" >"$manifest"
count=$(wc -l <"$manifest" | tr -d ' ')
((count >= HIVEZILLA_MIN_SEGMENTS)) || exit 0
awk '{print $2}' "$manifest" >"$work/files.list"

rsync -a --partial --files-from="$work/files.list" \
  -e "$HIVEZILLA_SSH" "$HIVEZILLA_SOURCE:$HIVEZILLA_SOURCE_DIR/" \
  "$HIVEZILLA_MIRROR_DIR/segments/"
(
  cd "$HIVEZILLA_MIRROR_DIR/segments"
  sha256sum -c "$manifest" >/dev/null
)

manifest_sha=$(sha256sum "$manifest" | awk '{print $1}')
receipt="$HIVEZILLA_MIRROR_DIR/receipts/$manifest_sha.json"
receipt_tmp="$receipt.tmp"
bytes=0
while read -r _ file; do
  size=$(stat -c %s "$HIVEZILLA_MIRROR_DIR/segments/$file")
  bytes=$((bytes + size))
done <"$manifest"
printf '{"version":1,"manifest_sha256":"%s","segment_count":%s,"bytes":%s,"verified_at":"%s"}\n' \
  "$manifest_sha" "$count" "$bytes" "$(date -u +%FT%TZ)" >"$receipt_tmp"
sync -f "$receipt_tmp"
mv "$receipt_tmp" "$receipt"
sync -f "$HIVEZILLA_MIRROR_DIR/receipts"

$HIVEZILLA_SSH "$HIVEZILLA_SOURCE" "sudo $HIVEZILLA_SOURCE_HELPER retire" <"$manifest"
printf 'VERIFIED_AND_RETIRED=%s BYTES=%s MANIFEST_SHA256=%s\n' "$count" "$bytes" "$manifest_sha"
