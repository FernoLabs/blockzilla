#!/usr/bin/env bash
set -euo pipefail

state_root=/volume1/@home/ach/dev/blockzilla-pipeline/state/nas-pipeline-v2
archive_root=/volume1/@home/ach/dev/blockzilla-v2
car=/volume1/blockzilla/epoch-1000.car
output=$archive_root/epoch-1000
retention_manifest=${1:?usage: cleanup-epoch1000-after-canonical.sh RETENTION_MANIFEST}
poll_secs=${POLL_SECS:-300}
required_nonempty=(
  archive-v2-meta.wincode
  registry.bin
  registry_counts.bin
  registry.mphf
  blockhash_registry.bin
  archive-v2-blocks.zstd
  archive-v2-blocks.index
  archive-v2-block-access.wincode
  archive-v2-block-access.index
  poh.wincode
  shredding.wincode
  registry-first-seen.manifest
)
required_files=(signatures.bin vote_hash_registry.bin)

log() {
  printf '%s %s\n' "$(date -u +%FT%TZ)" "$*"
}

canonical_ready() {
  [[ -s "$car" && -d "$output" ]] || return 1
  [[ ! -e "$output/REPAIR-COMPACTED.json" ]] || return 1
  [[ ! -e "$output/archive-v2-first-seen-scan-complete.v1" ]] || return 1
  local name
  for name in "${required_nonempty[@]}"; do
    [[ -s "$output/$name" ]] || return 1
  done
  for name in "${required_files[@]}"; do
    [[ -f "$output/$name" ]] || return 1
  done
  jq -e '.epochs[] | select(.epoch == 1000) | .state == "complete"' \
    "$state_root/status.json" >/dev/null 2>&1
}

safe_remove() {
  local path=$1
  case "$path" in
    /volume1/@home/ach/dev/blockzilla-live-retained/epoch-1000-precanonical-*/* | \
    /volume1/@home/ach/dev/blockzilla-v2/.pipeline-quarantine/epoch-1000.degraded-live-* | \
    /volume1/@home/ach/dev/blockzilla-v2/epoch-1000.streamed-with-rpc-repair-sidecars)
      ;;
    *)
      log "refusing cleanup path outside allowlist: $path"
      return 1
      ;;
  esac
  if [[ -e "$path" ]]; then
    log "removing superseded epoch-1000 artifact: $path"
    rm -rf --one-file-system -- "$path"
  fi
}

[[ -s "$retention_manifest" ]]
log "waiting for strict canonical epoch-1000 completion"
while ! canonical_ready; do
  sleep "$poll_secs"
done

# Require a second independently published scheduler sample before cleanup.
sleep "$poll_secs"
canonical_ready

while IFS= read -r path; do
  [[ -n "$path" ]] || continue
  safe_remove "$path"
done <"$retention_manifest"

log "cleanup complete; canonical archive and raw CAR preserved"
