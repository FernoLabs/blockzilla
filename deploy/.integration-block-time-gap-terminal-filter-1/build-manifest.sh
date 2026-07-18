#!/usr/bin/env bash
set -Eeuo pipefail

STATE=${STATE:-/volume1/@home/ach/dev/blockzilla-pipeline/state/block-time-gap-archive-v2-v1}
BIN=${BIN:-/volume1/@home/ach/dev/blockzilla-pipeline/releases/blockzilla-gap-archive-v2-2026.07.18-1/bin/blockzilla}
PIPELINE_STATUS=${PIPELINE_STATUS:-/volume1/@home/ach/dev/blockzilla-pipeline/state/nas-pipeline-v2/status.json}
ARCHIVE_ROOT=${ARCHIVE_ROOT:-/volume1/@home/ach/dev/blockzilla-v2}

test -s "$PIPELINE_STATUS"
tmp_manifest=$(mktemp "$STATE/.run-manifest.tsv.XXXXXX")
tmp_candidates=$(mktemp "$STATE/.run-candidates.tsv.XXXXXX")
trap 'rm -f -- "$tmp_manifest" "$tmp_candidates"' EXIT

# The scheduler's terminal state and published reader core are the authority.
# Registry-only migration directories are deliberately excluded even when a
# standalone V3/gap sidecar happens to exist in them.
jq -er '
  .epochs[]
  | select(.state == "complete")
  | select(any(.artifacts[]; .kind == "metadata" and .state == "present" and (.bytes // 0) > 0))
  | select(any(.artifacts[]; .kind == "blocks" and .state == "present" and (.bytes // 0) > 0))
  | select(any(.artifacts[]; .kind == "block_index" and .state == "present" and (.bytes // 0) > 0))
  | [.epoch, .output_path]
  | @tsv
' "$PIPELINE_STATUS" > "$tmp_candidates"
test -s "$tmp_candidates"

while IFS=$'\t' read -r epoch archive_dir; do
  case "$archive_dir" in
    "$ARCHIVE_ROOT/epoch-$epoch") ;;
    *) echo "unsafe archive path for epoch $epoch: $archive_dir" >&2; exit 65 ;;
  esac
  metadata="$archive_dir/archive-v2-meta.wincode"
  blocks="$archive_dir/archive-v2-blocks.zstd"
  index="$archive_dir/archive-v2-blocks.index"
  for required in "$metadata" "$blocks" "$index"; do
    if test ! -s "$required"; then
      echo "terminal epoch $epoch is missing nonempty reader artifact: $required" >&2
      exit 65
    fi
  done
  sidecar="$archive_dir/block-time-gaps.bin"
  if test -e "$sidecar"; then
    "$BIN" verify-block-time-gaps "$sidecar" --epoch "$epoch" >/dev/null 2>&1
  fi
  bytes=$(( $(stat -c %s "$blocks") + $(stat -c %s "$index") ))
  printf '%s\t%s\t%s\n' "$epoch" "$archive_dir" "$bytes"
done < "$tmp_candidates" | sort -n -k3,3 > "$tmp_manifest"

test -s "$tmp_manifest"
mv "$tmp_manifest" "$STATE/run-manifest.tsv"
rm -f -- "$tmp_candidates"
trap - EXIT
wc -l "$STATE/run-manifest.tsv"
awk -F '\t' '{sum += $3} END {printf "%.0f\n", sum}' "$STATE/run-manifest.tsv"
head -5 "$STATE/run-manifest.tsv"
