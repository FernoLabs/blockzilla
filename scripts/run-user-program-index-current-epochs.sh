#!/bin/sh
set -u

umask 077

usage() {
    echo "usage: $0 ARCHIVE_ROOT OUTPUT_ROOT STATE_ROOT INDEXER THREADS FIRST_EPOCH LAST_EPOCH RELEASE_ID" >&2
    exit 2
}

[ "$#" -eq 8 ] || usage

archive_root=$1
output_root=$2
state_root=$3
indexer=$4
threads=$5
first_epoch=$6
last_epoch=$7
release_id=$8

case "$archive_root:$output_root:$state_root:$indexer" in
    /*:/*:/*:/*) ;;
    *) usage ;;
esac
case "$threads:$first_epoch:$last_epoch" in
    *[!0-9:]*|:|*:|:*) usage ;;
esac
case "$release_id" in
    ""|*[!A-Za-z0-9._-]*) usage ;;
esac
[ "$threads" -ge 1 ] && [ "$threads" -le 256 ] || usage
[ "$first_epoch" -le "$last_epoch" ] || usage
[ -d "$archive_root" ] && [ ! -L "$archive_root" ] || usage
[ -d "$output_root" ] && [ ! -L "$output_root" ] || usage
[ -d "$state_root" ] && [ ! -L "$state_root" ] || usage
[ -x "$indexer" ] && [ -f "$indexer" ] && [ ! -L "$indexer" ] || usage

archive_root=$(realpath "$archive_root")
output_root=$(realpath "$output_root")
state_root=$(realpath "$state_root")

exec 9>"$state_root/run.lock"
if ! flock -n 9; then
    echo "another user-program index batch owns $state_root/run.lock" >&2
    exit 1
fi

events=$state_root/epochs.tsv
status=$state_root/status.json
touch "$events"

write_status() {
    state=$1
    epoch=$2
    completed=$3
    failed=$4
    missing=$5
    skipped=$6
    python3 - "$status" "$state" "$epoch" "$completed" "$failed" "$missing" "$skipped" \
        "$first_epoch" "$last_epoch" "$threads" "$release_id" <<'PY'
import json
import os
import sys
import tempfile
import time

(path, state, epoch, completed, failed, missing, skipped,
 first_epoch, last_epoch, threads, release_id) = sys.argv[1:]
payload = {
    "schema_version": 1,
    "kind": "blockzilla-user-program-index-current-epochs",
    "state": state,
    "current_epoch": None if epoch == "-" else int(epoch),
    "first_epoch": int(first_epoch),
    "last_epoch": int(last_epoch),
    "completed_this_run": int(completed),
    "failed_this_run": int(failed),
    "missing_archives_this_run": int(missing),
    "valid_existing_this_run": int(skipped),
    "threads": int(threads),
    "release_id": release_id,
    "updated_unix_time": int(time.time()),
}
directory = os.path.dirname(path)
fd, temporary = tempfile.mkstemp(prefix=".status.", dir=directory)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
PY
}

valid_output() {
    python3 - "$1" "$2" "$3" <<'PY'
import json
import os
import sys

index_dir, epoch_text, archive = sys.argv[1:]
epoch = int(epoch_text)
manifest_path = os.path.join(index_dir, "manifest.json")
with open(manifest_path, "r", encoding="utf-8") as handle:
    manifest = json.load(handle)
if manifest.get("schema_version") != 3 or manifest.get("format_version") != 3:
    raise SystemExit(1)
if manifest.get("complete") is not True or any(manifest.get("omissions", {}).values()):
    raise SystemExit(1)
if manifest.get("epoch") != epoch or manifest.get("cluster_id") != "mainnet-beta":
    raise SystemExit(1)
if manifest.get("archive_root") != os.path.realpath(archive):
    raise SystemExit(1)
if manifest.get("binding_kind") != "trusted_local_asserted_immutable":
    raise SystemExit(1)
if not os.path.isfile(os.path.join(index_dir, "programs.map")):
    raise SystemExit(1)
for shard in manifest.get("shards", []):
    shard_dir = os.path.join(index_dir, f"shard-{shard['shard']}")
    for name in ("wallets.idx", "programs.rel"):
        if not os.path.isfile(os.path.join(shard_dir, name)):
            raise SystemExit(1)
PY
}

completed=0
failed=0
missing=0
skipped=0
write_status running - "$completed" "$failed" "$missing" "$skipped"

epoch=$first_epoch
while [ "$epoch" -le "$last_epoch" ]; do
    archive=$archive_root/epoch-$epoch
    epoch_output_root=$output_root/epoch-$epoch
    output=$epoch_output_root/current-v3-$release_id
    log=$state_root/epoch-$epoch.log
    started=$(date +%s)
    write_status running "$epoch" "$completed" "$failed" "$missing" "$skipped"

    if [ ! -d "$archive" ] || [ -L "$archive" ]; then
        missing=$((missing + 1))
        printf '%s\t%s\t%s\t%s\t%s\n' "$epoch" missing_archive "$started" "$(date +%s)" "$archive" >>"$events"
        epoch=$((epoch + 1))
        continue
    fi

    if [ -e "$output" ] || [ -L "$output" ]; then
        if [ -d "$output" ] && [ ! -L "$output" ] && valid_output "$output" "$epoch" "$archive"; then
            skipped=$((skipped + 1))
            printf '%s\t%s\t%s\t%s\t%s\n' "$epoch" valid_existing "$started" "$(date +%s)" "$output" >>"$events"
        else
            failed=$((failed + 1))
            printf '%s\t%s\t%s\t%s\t%s\n' "$epoch" invalid_existing "$started" "$(date +%s)" "$output" >>"$events"
        fi
        epoch=$((epoch + 1))
        continue
    fi

    mkdir -p "$epoch_output_root"
    generation_id=blockzilla-user-program-index-local-v3-epoch-$epoch-$release_id
    if "$indexer" build-dense \
        --epoch "$epoch" \
        --archive "$archive" \
        --out "$output" \
        --trust-local \
        --cluster-id mainnet-beta \
        --generation-id "$generation_id" \
        --threads "$threads" >>"$log" 2>&1 \
        && valid_output "$output" "$epoch" "$archive"; then
        completed=$((completed + 1))
        result=complete
    else
        failed=$((failed + 1))
        result=failed
    fi
    printf '%s\t%s\t%s\t%s\t%s\n' "$epoch" "$result" "$started" "$(date +%s)" "$output" >>"$events"
    epoch=$((epoch + 1))
done

write_status complete - "$completed" "$failed" "$missing" "$skipped"
