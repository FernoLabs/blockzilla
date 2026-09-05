#!/bin/sh
# Focused tests for ordered skip, result-based resume, first error, and locks.

set -eu
umask 077

fail() {
    echo "fast canary batch self-test: $*" >&2
    exit 1
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
runner=$script_dir/run-archive-v2-pre-to-post-fast-canary-batch.sh
test_root=$(mktemp -d "${TMPDIR:-/tmp}/archive-v2-fast-canary-batch-test.XXXXXX") \
    || fail 'cannot create test root'
test_root=$(CDPATH= cd -P "$test_root" && pwd -P)
cleanup() {
    trap - 0 1 2 3 15
    chmod -R u+w "$test_root" 2>/dev/null || :
    rm -rf "$test_root"
}
trap 'cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

for required in jq awk cmp mktemp; do
    command -v "$required" >/dev/null 2>&1 || fail "$required is required"
done

canary=$test_root/frozen-canary
cat >"$canary" <<'FAKE_CANARY'
#!/bin/sh
set -eu
[ "$#" -eq 4 ] || exit 64
handoff=$1 epoch=$2 converter=$3 threads=$4
control=${FAKE_CONTROL_ROOT:?}
printf '%s\n' "$epoch" >>"$control/invocations"
if [ -e "$control/sleep-$epoch" ]; then
    stop() {
        trap - 1 2 3 15
        : >"$control/reaped-$epoch"
        exit 143
    }
    trap 'stop' 1 2 3 15
    : >"$control/entered-$epoch"
    while :; do sleep 1; done
fi
state=$handoff/fast-canary-epoch-$epoch
result=$state/result.json
if [ -e "$result" ]; then
    jq -e --argjson epoch "$epoch" --argjson threads "$threads" '
        .kind == "archive-v2-pre-to-post-fast-canary-result" and
        .epoch == $epoch and .threads == $threads and .canonical == false
    ' "$result" >/dev/null || exit 65
    jq -c . "$result"
    exit 0
fi
if [ -e "$control/fail-once-$epoch" ] && [ ! -e "$control/failed-$epoch" ]; then
    : >"$control/failed-$epoch"
    exit 23
fi
mkdir "$state"
jq -cn --argjson epoch "$epoch" --argjson threads "$threads" \
    '{kind:"archive-v2-pre-to-post-fast-canary-result",epoch:$epoch,
      threads:$threads,canonical:false}' >"$result"
printf '%s\n' "$epoch" >>"$control/conversions"
jq -c . "$result"
FAKE_CANARY
converter=$test_root/frozen-converter
cat >"$converter" <<'FAKE_CONVERTER'
#!/bin/sh
exit 0
FAKE_CONVERTER
chmod 700 "$canary" "$converter"

run_batch() {
    test_shell=$1
    shift
    FAKE_CONTROL_ROOT="$control" "$test_shell" "$runner" "$@"
}

shells=sh
if command -v dash >/dev/null 2>&1; then shells='sh dash'; fi
for test_shell in $shells; do
    "$test_shell" -n "$runner" || fail "$test_shell syntax check failed"
    fixture=$test_root/$test_shell
    handoff=$fixture/handoff
    control=$fixture/control
    mkdir -p "$handoff/fast-in-place-candidate" "$control"
    printf '1\n2\n3\n4\n' \
        >"$handoff/fast-in-place-candidate/all-legacy-pre-epochs.txt"
    : >"$control/invocations"
    : >"$control/conversions"
    : >"$control/fail-once-3"

    if run_batch "$test_shell" "$handoff" 2 "$canary" "$converter" 8 \
        >"$fixture/first.out" 2>"$fixture/first.err"; then
        fail "$test_shell accepted the injected epoch-3 error"
    fi
    [ "$(cat "$control/invocations")" = "2
3" ] || fail "$test_shell did not stop in list order at the first error"
    [ "$(cat "$control/conversions")" = 2 ] \
        || fail "$test_shell did not skip epoch 1"
    [ ! -e "$handoff/fast-canary-batch-start-2/complete.json" ] \
        || fail "$test_shell published completion after an error"
    [ ! -e "$handoff/.archive-v2-pre-to-post-fast-canary-batch.lock" ] \
        || fail "$test_shell left the supervisor lock after an error"

    run_batch "$test_shell" "$handoff" 2 "$canary" "$converter" 8 \
        >"$fixture/resume.out" 2>"$fixture/resume.err" \
        || fail "$test_shell did not resume"
    [ "$(cat "$control/invocations")" = "2
3
2
3
4" ] || fail "$test_shell did not revalidate results in exact list order"
    [ "$(cat "$control/conversions")" = "2
3
4" ] || fail "$test_shell repeated a completed conversion on resume"
    state=$handoff/fast-canary-batch-start-2
    cmp -s "$state/legacy-pre-epochs.snapshot" \
        "$handoff/fast-in-place-candidate/all-legacy-pre-epochs.txt" \
        || fail "$test_shell did not freeze the exact list"
    jq -e --arg state "$state" '
        .kind == "archive-v2-pre-to-post-fast-canary-batch-config" and
        .state_root == $state and .start_epoch == 2 and
        .selected_epoch_count == 3 and .threads == 8 and
        .one_canary_at_a_time == true and .canonical == false
    ' "$state/config.json" >/dev/null || fail "$test_shell config is invalid"
    jq -e '
        .kind == "archive-v2-pre-to-post-fast-canary-batch-complete" and
        .start_epoch == 2 and .completed_epoch_count == 3 and .threads == 8 and
        .one_canary_at_a_time == true and .canonical == false
    ' "$state/complete.json" >/dev/null || fail "$test_shell completion is invalid"

    before_lock=$(wc -l <"$control/invocations" | tr -d '[:space:]')
    mkdir "$handoff/.archive-v2-pre-to-post-fast-canary-batch.lock"
    if run_batch "$test_shell" "$handoff" 2 "$canary" "$converter" 8 \
        >"$fixture/lock.out" 2>"$fixture/lock.err"; then
        fail "$test_shell accepted an existing supervisor lock"
    fi
    rmdir "$handoff/.archive-v2-pre-to-post-fast-canary-batch.lock"
    [ "$(wc -l <"$control/invocations" | tr -d '[:space:]')" -eq "$before_lock" ] \
        || fail "$test_shell started a canary while the supervisor lock existed"

    signal_fixture=$test_root/signal-$test_shell
    signal_handoff=$signal_fixture/handoff
    signal_control=$signal_fixture/control
    mkdir -p "$signal_handoff/fast-in-place-candidate" "$signal_control"
    printf '9\n' >"$signal_handoff/fast-in-place-candidate/all-legacy-pre-epochs.txt"
    : >"$signal_control/invocations"
    : >"$signal_control/conversions"
    : >"$signal_control/sleep-9"
    FAKE_CONTROL_ROOT="$signal_control" "$test_shell" "$runner" \
        "$signal_handoff" 9 "$canary" "$converter" 8 \
        >"$signal_fixture/out" 2>"$signal_fixture/err" &
    batch_pid=$!
    attempts=0
    while [ ! -e "$signal_control/entered-9" ]; do
        attempts=$((attempts + 1))
        [ "$attempts" -lt 100 ] || fail "$test_shell signal canary did not start"
        sleep 0.05
    done
    kill -TERM "$batch_pid"
    if wait "$batch_pid"; then fail "$test_shell signal run returned success"; fi
    [ -e "$signal_control/reaped-9" ] \
        || fail "$test_shell did not reap the active canary"
    [ ! -e "$signal_handoff/.archive-v2-pre-to-post-fast-canary-batch.lock" ] \
        || fail "$test_shell left the supervisor lock after a signal"
done

echo 'fast canary batch self-test: PASS'
