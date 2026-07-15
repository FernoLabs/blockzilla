#!/usr/bin/env bash
set -euo pipefail

REPO=${BLOCKZILLA_REPO:-/home/ach/dev/blockzilla-v1-registry-mphf-20260616}
VIEW=${BLOCKZILLA_EPOCH1000_VIEW:-/volume1/@home/ach/dev/blockzilla-live/epoch-1000-finalizer-view-20260712}
SOURCE=${BLOCKZILLA_EPOCH1000_SOURCE:-/volume1/@home/ach/dev/blockzilla-live/capture-20260711T061451Z-compact-v2-live}
OUTPUT=${BLOCKZILLA_EPOCH1000_OUTPUT:-/volume1/@home/ach/dev/blockzilla-v2/epoch-1000.streamed-with-rpc-repair-sidecars}
RUN_LOG=${BLOCKZILLA_EPOCH1000_RUN_LOG:?set BLOCKZILLA_EPOCH1000_RUN_LOG}
BLOCKS=${BLOCKZILLA_EPOCH1000_BLOCKS:-264316}
BIN=${BLOCKZILLA_BIN:-$REPO/target/release/blockzilla}

log() {
  printf '%s %s\n' "$(date -u +%FT%TZ)" "$*"
}

wait_for_worker() {
  local label=$1 pid_file=$2 report=$3 pid
  pid=$(cat "$pid_file")
  while kill -0 "$pid" 2>/dev/null; do
    log "waiting worker=$label pid=$pid"
    sleep 60
  done
  test -s "$report"
  local scanned truncated
  scanned=$(jq -r '.blocks_scanned // 0' "$report")
  truncated=$(jq -r 'if has("tail_truncated") then .tail_truncated else true end' "$report")
  if [[ "$scanned" != "132158" || "$truncated" != "false" ]]; then
    log "worker_invalid worker=$label blocks_scanned=$scanned tail_truncated=$truncated report=$report"
    exit 20
  fi
  log "worker_complete worker=$label blocks_scanned=$scanned"
}

consolidate_runs() {
  local output=$VIEW/index/pubkey-runs
  rm -rf "$output"
  mkdir -p "$output"
  local source path name
  for source in "$VIEW/index/pubkey-runs-a" "$VIEW/index/pubkey-runs-b"; do
    local prefix
    prefix=$(basename "$source" | sed 's/pubkey-runs-//')
    for path in "$source"/*.bin; do
      test -f "$path" || continue
      name=$(basename "$path")
      ln "$path" "$output/run-${prefix}-${name}"
    done
  done
  local files bytes
  files=$(find "$output" -maxdepth 1 -type f -name 'run-*.bin' | wc -l)
  bytes=$(find "$output" -maxdepth 1 -type f -name 'run-*.bin' -printf '%s\n' | awk '{s+=$1} END{print s+0}')
  log "runs_consolidated files=$files bytes=$bytes output=$output"
}

main() {
  wait_for_worker a "$RUN_LOG/a.pid" "$RUN_LOG/a.report.json"
  wait_for_worker b "$RUN_LOG/b.pid" "$RUN_LOG/b.report.json"
  consolidate_runs

  local source_inode view_inode
  source_inode=$(stat -c %i "$SOURCE/blocks/live-no-registry-blocks.bin")
  view_inode=$(stat -c %i "$VIEW/blocks/live-no-registry-blocks.bin")
  test "$source_inode" = "$view_inode"

  if [[ -e "$OUTPUT" ]]; then
    mv "$OUTPUT" "$OUTPUT.partial-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  mkdir -p "$OUTPUT"
  log "finalizer_start blocks=$BLOCKS view=$VIEW output=$OUTPUT"
  local command=(
    "$BIN" build-archive-v2-hot-blocks-from-live
    "$VIEW" "$OUTPUT"
    --max-blocks "$BLOCKS"
    --registry-source runs
    --level 1
  )
  if [[ -x /usr/bin/time ]]; then
    /usr/bin/time -v "${command[@]}" >"$RUN_LOG/finalizer.log" 2>&1
  else
    "${command[@]}" >"$RUN_LOG/finalizer.log" 2>&1
  fi

  cp -al "$SOURCE/repair/rpc-get-block/epoch-1000" "$OUTPUT/rpc-repair-epoch-1000"
  cp "$REPO/docs/live-epoch-rotation-20260712.md" "$OUTPUT/RPC-REPAIR-STATUS.md"
  log "finalizer_complete output=$OUTPUT"
}

main "$@"
