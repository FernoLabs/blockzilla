#!/usr/bin/env bash
# Build direct-CAR usage-sorted registries, publish them atomically, then let
# the scheduler use only its legacy compact/reuse path.

set -euo pipefail

if (( $# != 2 )); then
    echo "usage: $0 FIRST_EPOCH LAST_EPOCH" >&2
    exit 2
fi

first_epoch=$1
last_epoch=$2
if [[ ! $first_epoch =~ ^[0-9]+$ || ! $last_epoch =~ ^[0-9]+$ ]] \
    || (( first_epoch > last_epoch )); then
    echo "invalid epoch range: $first_epoch..$last_epoch" >&2
    exit 2
fi

pipeline_env=${BLOCKZILLA_PIPELINE_ENV:-/volume1/blockzilla/config/nas-pipeline-v2.env}
if [[ ! -f $pipeline_env ]]; then
    echo "pipeline environment file is missing: $pipeline_env" >&2
    exit 2
fi
# shellcheck disable=SC1090
set -a
source "$pipeline_env"
set +a

blockzilla_bin=${BLOCKZILLA_BIN:-/volume1/blockzilla/bin/blockzilla}
registry_bin=${BLOCKZILLA_REGISTRY_BIN:-$blockzilla_bin}
car_root=${BLOCKZILLA_CAR_ROOT:-/volume1/blockzilla/old-faithful}
archive_root=${BLOCKZILLA_ARCHIVE_ROOT:-/volume1/blockzilla/archive}
live_root=${BLOCKZILLA_LIVE_ROOT:-/volume1/blockzilla/live}
run_id=${ORDERED_COMPACT_RUN_ID:-e${first_epoch}-e${last_epoch}-ordered-20260831}
state_root=${ORDERED_COMPACT_STATE_ROOT:-/volume1/blockzilla/scheduler-state-${run_id}}
stage_root=${ORDERED_REGISTRY_STAGE_ROOT:-$archive_root/.ordered-registry-staging-$run_id}
compact_concurrency=${ORDERED_COMPACT_CONCURRENCY:-5}
registry_workers=${ORDERED_REGISTRY_WORKERS:-8}
validator=${BLOCKZILLA_ORDERED_REGISTRY_VALIDATOR_BIN:-$(dirname -- "$blockzilla_bin")/validate-ordered-registry}
observer_validator=${BLOCKZILLA_ORDERED_OBSERVER_VALIDATOR_BIN:-$(dirname -- "$blockzilla_bin")/validate-ordered-compact-observer}

if [[ ! -x $blockzilla_bin || ! -x $registry_bin \
    || ! -x $validator || ! -x $observer_validator ]]; then
    echo "required executable or validator is missing" >&2
    exit 2
fi
if pgrep -af -- "$blockzilla_bin scheduler" >/dev/null; then
    echo "a Blockzilla scheduler is already running; refusing registry staging" >&2
    exit 1
fi

mkdir -p "$state_root/logs" "$stage_root"

resolve_car() {
    local epoch=$1
    local plain=$car_root/epoch-$epoch.car
    local compressed=$car_root/epoch-$epoch.car.zst
    if [[ -f $plain && ! -e $compressed ]]; then
        printf '%s\n' "$plain"
    elif [[ -f $compressed && ! -e $plain ]]; then
        printf '%s\n' "$compressed"
    else
        echo "epoch $epoch must have exactly one .car or .car.zst input" >&2
        return 1
    fi
}

validate_stage_shape() {
    local stage=$1
    local entry name
    while IFS= read -r -d '' entry; do
        name=${entry##*/}
        case "$name" in
            registry.bin|registry_counts.bin|registry.mphf|blockhash_registry.bin|skipped_slots.bin) ;;
            *)
                echo "unexpected registry-stage entry: $entry" >&2
                return 1
                ;;
        esac
        [[ -f $entry && ! -L $entry ]] || {
            echo "registry-stage entry is not a regular file: $entry" >&2
            return 1
        }
    done < <(find "$stage" -mindepth 1 -maxdepth 1 -print0)
    for name in registry.bin registry_counts.bin registry.mphf blockhash_registry.bin skipped_slots.bin; do
        [[ -s $stage/$name ]] || {
            echo "missing or empty registry-stage file: $stage/$name" >&2
            return 1
        }
    done
}

epoch=$first_epoch
while (( epoch <= last_epoch )); do
    input=$(resolve_car "$epoch")
    output=$archive_root/epoch-$epoch
    stage=$stage_root/epoch-$epoch
    log=$state_root/logs/epoch-$epoch-ordered-registry.log

    if [[ -e $output ]]; then
        echo "epoch $epoch output already exists; refusing overwrite" >&2
        exit 1
    fi
    if [[ -e $stage ]]; then
        [[ -d $stage && ! -L $stage ]] || {
            echo "epoch $epoch registry stage is not a directory: $stage" >&2
            exit 1
        }
        echo "epoch $epoch: validating retained registry stage"
    else
        mkdir "$stage"
        echo "epoch $epoch: building usage-sorted registry with $registry_workers workers"
        "$registry_bin" build-archive-v2-registries \
            "$input" "$stage" \
            --workers "$registry_workers" >"$log" 2>&1
        "$blockzilla_bin" build-archive-v2-registry-index \
            "$stage/registry.bin" >>"$log" 2>&1
    fi
    validate_stage_shape "$stage"
    "$validator" \
        "$stage/registry.bin" \
        "$stage/registry_counts.bin" \
        "$stage/blockhash_registry.bin" \
        "$stage/skipped_slots.bin" | tee -a "$log"
    mv "$stage" "$output"
    echo "epoch $epoch: ordered registry published"
    epoch=$((epoch + 1))
done

rmdir "$stage_root"
echo "all ordered registries are published; checking scheduler classification"

scheduler_args=(
    --car-root "$car_root"
    --archive-root "$archive_root"
    --live-root "$live_root"
    --state-root "$state_root"
    --start-epoch "$first_epoch"
    --end-epoch "$last_epoch"
    --scan-concurrency 1
    --compact-concurrency "$compact_concurrency"
    --download-concurrency 1
    --poh-migration-concurrency 0
    --registry-reprocess-concurrency 0
)
observer_status=$state_root/ordered-observer-status.json
observer_log=$state_root/logs/ordered-observer.log
observer_bind=127.0.0.1:18796
observer_management_bind=127.0.0.1:18798
"$blockzilla_bin" scheduler "${scheduler_args[@]}" \
    --status-bind "$observer_bind" \
    --management-bind "$observer_management_bind" \
    >"$observer_log" 2>&1 &
observer_pid=$!
cleanup_observer() {
    if kill -0 "$observer_pid" 2>/dev/null; then
        kill "$observer_pid" 2>/dev/null || true
        wait "$observer_pid" 2>/dev/null || true
    fi
}
trap cleanup_observer EXIT

"$observer_validator" "$observer_status" "$first_epoch" "$last_epoch" \
    --poll-url "http://$observer_bind/api/v1/status" \
    | tee -a "$observer_log"
cleanup_observer
trap - EXIT

echo "scheduler classification is safe; starting compact/reuse execution"
exec "$blockzilla_bin" scheduler "${scheduler_args[@]}" --execute
