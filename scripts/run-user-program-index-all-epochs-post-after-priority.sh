#!/bin/sh
set -eu

# Compatibility: legacy firewatch hash domains, generation/receipt IDs and
# lock names remain unchanged so existing plans and resumable state stay valid.

# Both service names refer to the same controller during the naming migration.
# A missing unit is allowed; an unreadable service state is not.
controller_is_inactive() {
    for controller_service in \
        blockzilla-user-program-index-controller.service \
        blockzilla-firewatch-index-controller.service; do
        controller_state=$(systemctl --user is-active "$controller_service" 2>/dev/null || true)
        case "$controller_state" in
            inactive|unknown) ;;
            *) return 1 ;;
        esac
    done
}


[ "$#" -eq 9 ] || {
    echo "usage: $0 PRIORITY_PID_FILE PRIORITY_PLAN PRIORITY_COMPLETE RUNNER FULL_PLAN OUTPUT_ROOT INDEXER STATE_ROOT THREADS" >&2
    exit 2
}

priority_pid_file=$1
priority_plan=$2
priority_complete=$3
runner=$4
full_plan=$5
output_root=$6
indexer=$7
state_root=$8
threads=$9

case "$priority_pid_file:$priority_plan:$priority_complete:$runner:$full_plan:$output_root:$indexer:$state_root" in
    /*:/*:/*:/*:/*:/*:/*:/*) ;;
    *) exit 2 ;;
esac
case "$threads" in ''|*[!0-9]*) exit 2 ;; esac
[ "$threads" -eq 12 ]

priority_pid=$(sed -n '1p' "$priority_pid_file")
case "$priority_pid" in ''|*[!0-9]*) exit 1 ;; esac
[ "$(wc -l <"$priority_pid_file" | tr -d '[:space:]')" -eq 1 ]
priority_tick=$(awk '{print $22}' "/proc/$priority_pid/stat")
priority_plan_sha=$(sha256sum "$priority_plan" | awk 'NR == 1 { print $1; exit }')

while [ -e "/proc/$priority_pid/stat" ]; do
    [ "$(awk '{print $22}' "/proc/$priority_pid/stat")" = "$priority_tick" ] || {
        echo "priority PID identity changed" >&2
        exit 1
    }
    sleep 15
done

jq -e --arg plan_sha "$priority_plan_sha" --argjson threads "$threads" '
    .kind == "firewatch-all-epochs-post-complete-v1" and
    .canonical_publication == false and
    .plan_sha256 == $plan_sha and
    .threads == $threads and
    .completed_epochs == 17
' "$priority_complete" >/dev/null

[ ! -e "$output_root/.firewatch-all-epochs-post.lock" ] \
    && [ ! -L "$output_root/.firewatch-all-epochs-post.lock" ]
[ "$(systemctl --user is-active blockzilla-archive.service 2>/dev/null || true)" = inactive ]
controller_is_inactive

echo "priority 17 complete; starting strict all-epoch batch"
exec nice -n 5 ionice -c 2 -n 4 "$runner" "$full_plan" "$output_root" \
    "$indexer" "$threads" "$state_root" 0 1018
