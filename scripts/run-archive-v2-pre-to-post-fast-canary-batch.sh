#!/bin/sh
# Run the frozen one-epoch canary in exact LegacyPre list order. The canary
# validates each existing result on resume and owns the archive-root lock.

set -u
umask 077

cb_die() {
    echo "archive-v2 fast canary batch: $*" >&2
    exit 1
}

cb_usage() {
    echo "usage: $0 HANDOFF_STATE_ROOT START_EPOCH CANARY_RUNNER CONVERTER THREADS" >&2
    exit 2
}

cb_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

cb_file() (
    cb_parent=$(cb_directory "$(dirname "$1")") || exit 1
    printf '%s/%s\n' "$cb_parent" "$(basename "$1")"
)

cb_absolute() {
    case "$2" in /*) ;; *) cb_die "$1 must be an absolute path: $2" ;; esac
    case "$2" in *'
'*) cb_die "$1 must be one line" ;; esac
}

cb_decimal() {
    case "$2" in 0) ;; ''|*[!0-9]*|0*) cb_die "$1 is not a canonical decimal" ;; esac
}

cb_publish_same() {
    cb_candidate=$1
    cb_final=$2
    chmod 400 "$cb_candidate" || return 1
    if [ -e "$cb_final" ] || [ -L "$cb_final" ]; then
        [ -f "$cb_final" ] && [ ! -L "$cb_final" ] \
            && cmp -s "$cb_candidate" "$cb_final" || return 1
        rm "$cb_candidate" || return 1
    else
        ln -n "$cb_candidate" "$cb_final" || return 1
        rm "$cb_candidate" || return 1
    fi
}

[ "$#" -eq 5 ] || cb_usage
handoff_state_root=$1
start_epoch=$2
canary_runner=$3
converter=$4
threads=$5
for cb_command in jq awk cmp chmod mkdir rmdir rm ln dd date dirname basename kill; do
    command -v "$cb_command" >/dev/null 2>&1 \
        || cb_die "required command is absent: $cb_command"
done
cb_absolute HANDOFF_STATE_ROOT "$handoff_state_root"
cb_absolute CANARY_RUNNER "$canary_runner"
cb_absolute CONVERTER "$converter"
cb_decimal START_EPOCH "$start_epoch"
cb_decimal THREADS "$threads"
[ "$threads" -ge 1 ] && [ "$threads" -le 64 ] || cb_die 'THREADS must be 1..64'
[ -d "$handoff_state_root" ] && [ ! -L "$handoff_state_root" ] \
    && [ "$(cb_directory "$handoff_state_root")" = "$handoff_state_root" ] \
    || cb_die 'handoff state root must be one canonical real directory'
for cb_tool in "$canary_runner" "$converter"; do
    [ -f "$cb_tool" ] && [ ! -L "$cb_tool" ] && [ -x "$cb_tool" ] \
        && [ "$(cb_file "$cb_tool")" = "$cb_tool" ] \
        || cb_die "tool must be one canonical real executable: $cb_tool"
done

source_list=$handoff_state_root/fast-in-place-candidate/all-legacy-pre-epochs.txt
[ -f "$source_list" ] && [ ! -L "$source_list" ] \
    || cb_die "exact LegacyPre epoch list is absent: $source_list"
selected_count=$(awk -v start="$start_epoch" '
    NF != 1 || $0 !~ /^(0|[1-9][0-9]*)$/ || seen[$0]++ { bad = 1 }
    previous != "" && ($0 + 0) <= (previous + 0) { bad = 1 }
    { previous = $0 }
    ($0 + 0) >= (start + 0) { selected++ }
    END { if (bad || NR == 0) exit 1; print selected + 0 }
' "$source_list") || cb_die 'exact LegacyPre epoch list is invalid'

supervisor_lock=$handoff_state_root/.archive-v2-pre-to-post-fast-canary-batch.lock
lock_held=0
canary_pid=
cb_release_lock() {
    if [ "$lock_held" -eq 1 ]; then
        rmdir "$supervisor_lock" 2>/dev/null \
            || echo "archive-v2 fast canary batch: cannot remove supervisor lock" >&2
        lock_held=0
    fi
}
cb_stop() {
    cb_status=$1
    trap - 0 1 2 3 15
    if [ -n "$canary_pid" ]; then
        kill -TERM "$canary_pid" 2>/dev/null || :
        wait "$canary_pid" 2>/dev/null || :
        canary_pid=
    fi
    cb_release_lock
    exit "$cb_status"
}
trap 'cb_release_lock' 0
trap 'cb_stop 129' 1
trap 'cb_stop 130' 2
trap 'cb_stop 131' 3
trap 'cb_stop 143' 15
mkdir "$supervisor_lock" 2>/dev/null \
    || cb_die "supervisor lock exists: $supervisor_lock"
lock_held=1

state_root=$handoff_state_root/fast-canary-batch-start-$start_epoch
if [ -e "$state_root" ] || [ -L "$state_root" ]; then
    [ -d "$state_root" ] && [ ! -L "$state_root" ] || cb_die 'batch state is invalid'
else
    mkdir "$state_root" || cb_die 'cannot create batch state'
fi
chmod 700 "$state_root" || cb_die 'cannot protect batch state'

epoch_list=$state_root/legacy-pre-epochs.snapshot
epoch_candidate=$epoch_list.building-$$
[ ! -e "$epoch_candidate" ] && [ ! -L "$epoch_candidate" ] \
    || cb_die 'unfinished epoch-list candidate needs review'
( set -C; dd if="$source_list" bs=1048576 >"$epoch_candidate" 2>/dev/null ) \
    || cb_die 'cannot freeze exact LegacyPre epoch list'
cb_publish_same "$epoch_candidate" "$epoch_list" \
    || cb_die 'exact LegacyPre epoch list changed on resume'

config=$state_root/config.json
config_candidate=$config.building-$$
(
    set -C
    jq -cn --arg state "$state_root" --arg handoff "$handoff_state_root" \
        --arg source_list "$source_list" --arg epoch_list "$epoch_list" \
        --arg canary "$canary_runner" --arg converter "$converter" \
        --argjson start "$start_epoch" --argjson count "$selected_count" \
        --argjson threads "$threads" '
        {schema_version:1,kind:"archive-v2-pre-to-post-fast-canary-batch-config",
         state_root:$state,handoff_state_root:$handoff,start_epoch:$start,
         selected_epoch_count:$count,
         epoch_list:{source:$source_list,snapshot:$epoch_list},
         canary_runner:$canary,converter:$converter,threads:$threads,
         one_canary_at_a_time:true,canonical:false}
    ' >"$config_candidate"
) || cb_die 'cannot build batch config'
cb_publish_same "$config_candidate" "$config" || cb_die 'batch config changed on resume'

completed_count=0
while IFS= read -r epoch || [ -n "$epoch" ]; do
    [ "$epoch" -ge "$start_epoch" ] || continue
    "$canary_runner" "$handoff_state_root" "$epoch" "$converter" "$threads" &
    canary_pid=$!
    if wait "$canary_pid"; then cb_status=0; else cb_status=$?; fi
    canary_pid=
    [ "$cb_status" -eq 0 ] || cb_die "canary failed for epoch $epoch with status $cb_status"
    completed_count=$((completed_count + 1))
done <"$epoch_list"
[ "$completed_count" -eq "$selected_count" ] || cb_die 'completed epoch count differs'

complete=$state_root/complete.json
if [ -e "$complete" ] || [ -L "$complete" ]; then
    [ -f "$complete" ] && [ ! -L "$complete" ] || cb_die 'completion record is invalid'
    jq -e --arg state "$state_root" --arg config "$config" --arg list "$epoch_list" \
        --argjson start "$start_epoch" --argjson count "$selected_count" \
        --argjson threads "$threads" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-fast-canary-batch-complete" and
        .state_root == $state and .config == $config and .epoch_list == $list and
        .start_epoch == $start and .completed_epoch_count == $count and
        .threads == $threads and .one_canary_at_a_time == true and
        .canonical == false and (.completed_at_utc | type == "string")
    ' "$complete" >/dev/null 2>&1 || cb_die 'completion record changed on resume'
else
    completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) || cb_die 'cannot read completion time'
    complete_candidate=$complete.building-$$
    (
        set -C
        jq -cn --arg state "$state_root" --arg config "$config" --arg list "$epoch_list" \
            --arg completed "$completed_at" --argjson start "$start_epoch" \
            --argjson count "$selected_count" --argjson threads "$threads" '
            {schema_version:1,kind:"archive-v2-pre-to-post-fast-canary-batch-complete",
             state_root:$state,config:$config,epoch_list:$list,start_epoch:$start,
             completed_epoch_count:$count,threads:$threads,
             one_canary_at_a_time:true,canonical:false,completed_at_utc:$completed}
        ' >"$complete_candidate"
    ) || cb_die 'cannot build completion record'
    cb_publish_same "$complete_candidate" "$complete" \
        || cb_die 'cannot publish completion record'
fi

cb_release_lock
trap - 0 1 2 3 15
jq -c . "$complete"
