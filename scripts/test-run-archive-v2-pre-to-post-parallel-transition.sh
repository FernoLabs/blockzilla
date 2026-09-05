#!/bin/sh
# Hostile orchestration tests for the two-worker Pre-to-Post transition. The
# frozen production validator already has its own format-level tests. This
# harness uses a small validator double so it can exercise claims, failures,
# resume, signals, attestations, and ordered completion without archive data.

set -eu
umask 077

fail() {
    echo "parallel Pre-to-Post transition self-test: $*" >&2
    exit 1
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
parallel_source=$script_dir/run-archive-v2-pre-to-post-parallel-transition.sh
test_root=$(mktemp -d "${TMPDIR:-/tmp}/archive-v2-parallel-transition-test.XXXXXX") \
    || fail 'cannot create test directory'
test_root=$(CDPATH= cd -P "$test_root" && pwd -P)
case "${test_root##*/}" in
    archive-v2-parallel-transition-test.*) ;;
    *) fail "unexpected test directory: $test_root" ;;
esac
cleanup() {
    trap - 0 1 2 3 15
    if [ "${KEEP_PARALLEL_TEST_ROOT:-0}" = 1 ]; then
        echo "kept parallel test root: $test_root" >&2
        return
    fi
    chmod -R u+w "$test_root" 2>/dev/null || :
    rm -rf "$test_root"
}
trap 'cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

for required in jq awk sed wc tr dd ps; do
    command -v "$required" >/dev/null 2>&1 || fail "$required is required"
done
if [ -x /usr/bin/true ]; then
    true_program=/usr/bin/true
else
    true_program=/bin/true
fi
if ! command -v ionice >/dev/null 2>&1; then
    printf '#!/bin/sh\n[ "$1" = -c ] && shift 2\n[ "$1" = /dev/fd/4 ] && shift && exec "$FAKE_CONVERTER_PATH" "$@"\nexec "$@"\n' >"$test_root/ionice"
    chmod 700 "$test_root/ionice"
fi
# The desktop test sandbox can deny process-table reads. Production uses the
# real NAS ps. This deterministic snapshot contains no old runner or converter.
printf '#!/bin/sh\nprintf "  1 /sbin/init\\n"\n' >"$test_root/ps"
chmod 700 "$test_root/ps"

# Freeze the coordinator exactly as production deployment does.
cp "$parallel_source" "$test_root/parallel-placeholder"
parallel_sha=$(sha256_file "$test_root/parallel-placeholder")
parallel_runner=$test_root/run-archive-v2-pre-to-post-parallel-transition-$parallel_sha.sh
mv "$test_root/parallel-placeholder" "$parallel_runner"
chmod 500 "$parallel_runner"

# The coordinator extracts only the function preamble before the exact main
# sentinel. These small functions model the hardened runner contract while
# keeping this orchestration test independent of Archive V2 payload bytes.
fake_old_source=$test_root/fake-old-runner.source
cat >"$fake_old_source" <<'FAKE_OLD'
#!/bin/sh
set -u
BLOCKS_FILE=archive-v2-blocks.zstd
sha256_file() {
    if [ "$sha256_program" = sha256sum ]; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}
validate_epoch_list() {
    awk '
        !/^(0|[1-9][0-9]*)$/ || seen[$0]++ { bad = 1 }
        { count++ }
        END { if (bad || count == 0) exit 1; print count }
    ' "$1"
}
validate_target_generation() (
    epoch=$1 source=$2 target=$3 staging=$4 generation=$5 lease=$6 report=$7
    [ -d "$source" ] && [ ! -L "$source" ] || exit 1
    [ -d "$target" ] && [ ! -L "$target" ] || exit 1
    [ ! -e "$staging" ] && [ ! -L "$staging" ] || exit 1
    [ -f "$target/complete.marker" ] && [ ! -L "$target/complete.marker" ] || exit 1
    [ -f "$report" ] && [ ! -L "$report" ] || exit 1
    jq -e --argjson epoch "$epoch" --arg source "$source" --arg target "$target" \
        --arg generation "$generation" --arg lease "$lease" '
        .epoch == $epoch and .source == $source and .target == $target and
        .generation_id == $generation and .lease_id == $lease and .ok == true
    ' "$report" >/dev/null 2>&1
)
write_epoch_attestation() (
    path=$1 epoch=$2 source=$3 target=$4 staging=$5 generation=$6 lease=$7 report=$8 log=$9
    report_sha=$(sha256_file "$report") || exit 1
    log_sha=$(sha256_file "$log") || exit 1
    marker_sha=$(sha256_file "$target/complete.marker") || exit 1
    (
        set -C
        jq -cn --arg run_id "$run_id" --argjson epoch "$epoch" \
            --arg converter_origin "$converter_origin" --arg converter_pinned "$pinned_converter" \
            --arg converter_sha "$converter_sha" --arg source "$source" --arg target "$target" \
            --arg staging "$staging" --arg generation "$generation" --arg lease "$lease" \
            --arg epoch_list "$epoch_list" --arg snapshot "$epoch_list_snapshot" \
            --arg list_sha "$epoch_list_sha" --arg report "$report" --arg report_sha "$report_sha" \
            --arg log "$log" --arg log_sha "$log_sha" --arg marker_sha "$marker_sha" '
            {kind:"fake-runner-attestation",run_id:$run_id,epoch:$epoch,
             converter_origin:$converter_origin,converter_pinned:$converter_pinned,
             converter_sha256:$converter_sha,source:$source,target:$target,staging:$staging,
             generation_id:$generation,lease_id:$lease,epoch_list:$epoch_list,
             epoch_list_snapshot:$snapshot,epoch_list_sha256:$list_sha,
             report:{path:$report,sha256:$report_sha},log:{path:$log,sha256:$log_sha},
             marker_sha256:$marker_sha}
        ' >"$path"
    )
)
validate_epoch_attestation() (
    path=$1 epoch=$2 source=$3 target=$4 staging=$5 generation=$6 lease=$7 report=$8 log=$9
    [ -f "$path" ] && [ ! -L "$path" ] && [ ! -w "$path" ] || exit 1
    report_sha=$(sha256_file "$report") || exit 1
    log_sha=$(sha256_file "$log") || exit 1
    marker_sha=$(sha256_file "$target/complete.marker") || exit 1
    jq -e --arg run_id "$run_id" --argjson epoch "$epoch" \
        --arg converter_origin "$converter_origin" --arg converter_pinned "$pinned_converter" \
        --arg converter_sha "$converter_sha" --arg source "$source" --arg target "$target" \
        --arg staging "$staging" --arg generation "$generation" --arg lease "$lease" \
        --arg epoch_list "$epoch_list" --arg snapshot "$epoch_list_snapshot" \
        --arg list_sha "$epoch_list_sha" --arg report "$report" --arg report_sha "$report_sha" \
        --arg log "$log" --arg log_sha "$log_sha" --arg marker_sha "$marker_sha" '
        .kind == "fake-runner-attestation" and .run_id == $run_id and .epoch == $epoch and
        .converter_origin == $converter_origin and .converter_pinned == $converter_pinned and
        .converter_sha256 == $converter_sha and .source == $source and .target == $target and
        .staging == $staging and .generation_id == $generation and .lease_id == $lease and
        .epoch_list == $epoch_list and .epoch_list_snapshot == $snapshot and
        .epoch_list_sha256 == $list_sha and
        .report == {path:$report,sha256:$report_sha} and
        .log == {path:$log,sha256:$log_sha} and .marker_sha256 == $marker_sha
    ' "$path" >/dev/null 2>&1
)
[ "$#" -eq 7 ] || usage
exit 99
FAKE_OLD
chmod 500 "$fake_old_source"
old_runner_sha=$(sha256_file "$fake_old_source")
old_runner=$test_root/run-archive-v2-pre-to-post-manual-$old_runner_sha.sh
mv "$fake_old_source" "$old_runner"

fake_converter_source=$test_root/fake-converter.source
cat >"$fake_converter_source" <<'FAKE_CONVERTER'
#!/bin/sh
set -eu
source_path= target_path= staging_path= epoch= generation= lease=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --source) source_path=$2; shift 2 ;;
        --source-lease-id) lease=$2; shift 2 ;;
        --target) target_path=$2; shift 2 ;;
        --staging) staging_path=$2; shift 2 ;;
        --epoch) epoch=$2; shift 2 ;;
        --cluster-id) shift 2 ;;
        --generation-id) generation=$2; shift 2 ;;
        *) exit 64 ;;
    esac
done
control=${FAKE_CONTROL_ROOT:?}
mkdir -p "$control"
: >"$control/entered-$epoch"
if [ -e "$control/sleep-$epoch" ] || [ -e "$control/sleep-all" ]; then
    sleeper=
    stop() {
        trap - 1 2 3 15
        [ -z "$sleeper" ] || kill -TERM "$sleeper" 2>/dev/null || :
        [ -z "$sleeper" ] || wait "$sleeper" 2>/dev/null || :
        : >"$control/terminated-$epoch"
        exit 143
    }
    trap 'stop' 1 2 3 15
    sleep 30 &
    sleeper=$!
    wait "$sleeper"
fi
if [ -e "$control/fail-$epoch" ] && [ ! -e "$control/failed-$epoch" ]; then
    : >"$control/failed-$epoch"
    exit 23
fi
mkdir "$staging_path"
printf 'epoch %s\n' "$epoch" >"$staging_path/payload.bin"
printf 'complete %s\n' "$epoch" >"$staging_path/complete.marker"
mv "$staging_path" "$target_path"
jq -cn --argjson epoch "$epoch" --arg source "$source_path" --arg target "$target_path" \
    --arg generation "$generation" --arg lease "$lease" \
    '{epoch:$epoch,source:$source,target:$target,generation_id:$generation,lease_id:$lease,ok:true}'
FAKE_CONVERTER
chmod 500 "$fake_converter_source"
converter_sha=$(sha256_file "$fake_converter_source")
converter=$test_root/archive-v2-pre-to-post-$converter_sha
mv "$fake_converter_source" "$converter"

make_prefix_epoch() {
    mpe_root=$1
    mpe_epoch=$2
    mpe_handoff=$mpe_root/handoff
    mpe_conversion=$mpe_handoff/conversion
    mpe_source=$mpe_root/source/epoch-$mpe_epoch
    mpe_target=$mpe_root/target/epoch-$mpe_epoch
    mpe_report=$mpe_conversion/epoch-$mpe_epoch.json
    mpe_log=$mpe_conversion/epoch-$mpe_epoch.log
    mpe_attestation=$mpe_conversion/epoch-$mpe_epoch.attestation.json
    mpe_generation=archive-v2-pre-to-post-test-run-epoch-$mpe_epoch-post
    mpe_lease=archive-v2-pre-to-post-test-run-epoch-$mpe_epoch-source-leases
    mkdir "$mpe_target"
    printf 'complete %s\n' "$mpe_epoch" >"$mpe_target/complete.marker"
    jq -cn --argjson epoch "$mpe_epoch" --arg source "$mpe_source" --arg target "$mpe_target" \
        --arg generation "$mpe_generation" --arg lease "$mpe_lease" \
        '{epoch:$epoch,source:$source,target:$target,generation_id:$generation,lease_id:$lease,ok:true}' \
        >"$mpe_report"
    : >"$mpe_log"
    mpe_report_sha=$(sha256_file "$mpe_report")
    mpe_log_sha=$(sha256_file "$mpe_log")
    mpe_marker_sha=$(sha256_file "$mpe_target/complete.marker")
    mpe_snapshot=$mpe_conversion/epoch-list.snapshot
    mpe_list=$mpe_handoff/cohort/epochs.txt
    mpe_list_sha=$(sha256_file "$mpe_snapshot")
    mpe_pinned=$mpe_conversion/.archive-v2-pre-to-post-bin/${converter##*/}
    jq -cn --argjson epoch "$mpe_epoch" --arg source "$mpe_source" --arg target "$mpe_target" \
        --arg staging "$mpe_root/target/.epoch-$mpe_epoch.pre-to-post.staging" \
        --arg generation "$mpe_generation" --arg lease "$mpe_lease" \
        --arg converter_origin "$converter" --arg converter_pinned "$mpe_pinned" \
        --arg converter_sha "$converter_sha" --arg epoch_list "$mpe_list" \
        --arg snapshot "$mpe_snapshot" --arg list_sha "$mpe_list_sha" \
        --arg report "$mpe_report" --arg report_sha "$mpe_report_sha" \
        --arg log "$mpe_log" --arg log_sha "$mpe_log_sha" --arg marker_sha "$mpe_marker_sha" '
        {kind:"fake-runner-attestation",run_id:"test-run",epoch:$epoch,
         converter_origin:$converter_origin,converter_pinned:$converter_pinned,
         converter_sha256:$converter_sha,source:$source,target:$target,staging:$staging,
         generation_id:$generation,lease_id:$lease,epoch_list:$epoch_list,
         epoch_list_snapshot:$snapshot,epoch_list_sha256:$list_sha,
         report:{path:$report,sha256:$report_sha},log:{path:$log,sha256:$log_sha},
         marker_sha256:$marker_sha}
    ' >"$mpe_attestation"
    chmod 400 "$mpe_report" "$mpe_log" "$mpe_attestation"
}

make_fixture() {
    mf_root=$1
    mf_epochs=$2
    mf_fence=$3
    mkdir -p "$mf_root/source" "$mf_root/target" "$mf_root/handoff/cohort" \
        "$mf_root/handoff/conversion/.archive-v2-pre-to-post-bin" "$mf_root/control"
    printf '%s\n' "$mf_epochs" | tr ' ' '\n' >"$mf_root/handoff/cohort/epochs.txt"
    cp "$mf_root/handoff/cohort/epochs.txt" "$mf_root/handoff/conversion/epoch-list.snapshot"
    chmod 400 "$mf_root/handoff/conversion/epoch-list.snapshot"
    for mf_epoch in $mf_epochs; do
        mkdir "$mf_root/source/epoch-$mf_epoch"
        printf 'source %s\n' "$mf_epoch" >"$mf_root/source/epoch-$mf_epoch/archive-v2-blocks.zstd"
    done
    cp "$converter" "$mf_root/handoff/conversion/.archive-v2-pre-to-post-bin/${converter##*/}"
    chmod 500 "$mf_root/handoff/conversion/.archive-v2-pre-to-post-bin/${converter##*/}"
    printf '{"kind":"fake-cohort"}\n' >"$mf_root/handoff/cohort/cohort.json"
    mf_cohort_sha=$(sha256_file "$mf_root/handoff/cohort/cohort.json")
    mf_list_sha=$(sha256_file "$mf_root/handoff/cohort/epochs.txt")
    jq -cn --arg cohort_sha "$mf_cohort_sha" --arg list_sha "$mf_list_sha" \
        '{cohort_json_sha256:$cohort_sha,epochs_sha256:$list_sha}' \
        >"$mf_root/handoff/cohort.binding.json"
    jq -cn --arg archive "$mf_root/source" --arg target "$mf_root/target" \
        --arg state "$mf_root/handoff" --arg runner "$old_runner" \
        --arg runner_sha "$old_runner_sha" --arg converter "$converter" \
        --arg converter_sha "$converter_sha" '
        {archive_root:$archive,target_root:$target,state_root:$state,
         cluster_id:"mainnet-beta",run_id:"test-run",
         converter_runner:$runner,converter_runner_sha256:$runner_sha,
         converter:$converter,converter_sha256:$converter_sha,excluded_epochs:[]}
    ' >"$mf_root/handoff/request.json"
    for mf_epoch in $mf_epochs; do
        [ "$mf_epoch" -ge "$mf_fence" ] || make_prefix_epoch "$mf_root" "$mf_epoch"
    done
    mf_fence_path=$mf_root/handoff/conversion/epoch-$mf_fence.json.building-parallel-transition-$parallel_sha
    PATH="$test_root:$PATH" "$parallel_runner" --fence-json \
        "$mf_root/handoff" "$mf_fence" >"$mf_fence_path"
    chmod 400 "$mf_fence_path"
}

run_transition() {
    rt_root=$1
    FAKE_CONTROL_ROOT="$rt_root/control" FAKE_CONVERTER_PATH="$converter" PATH="$test_root:$PATH" \
        "$parallel_runner" "$rt_root/handoff" "$2" 2
}

# Worker failure: worker 1 must finish its current epoch. The next invocation
# resumes the same immutable claims and completes in master order.
resume_root=$test_root/resume
make_fixture "$resume_root" '0 1 2' 1
prefix_target_sha=$(sha256_file "$resume_root/target/epoch-0/complete.marker")
prefix_attestation_sha=$(sha256_file "$resume_root/handoff/conversion/epoch-0.attestation.json")
: >"$resume_root/control/fail-1"
if run_transition "$resume_root" 1 >"$resume_root/first.out" 2>"$resume_root/first.err"; then
    fail 'worker failure returned success'
fi
[ -s "$resume_root/handoff/conversion/parallel-transition/claims/epoch-1.claim.json" ] \
    || fail 'failed worker did not retain its immutable claim'
[ -s "$resume_root/handoff/conversion/parallel-transition/results/epoch-2.json" ] \
    || fail 'sibling worker did not finish its active epoch'
[ ! -e "$resume_root/handoff/complete.json" ] || fail 'failed round published false completion'
run_transition "$resume_root" 1 >"$resume_root/resume.out" 2>"$resume_root/resume.err" \
    || fail 'clean retry did not resume and complete'
[ "$(sha256_file "$resume_root/target/epoch-0/complete.marker")" = "$prefix_target_sha" ] \
    || fail 'parallel transition changed the sequential prefix target'
[ "$(sha256_file "$resume_root/handoff/conversion/epoch-0.attestation.json")" = "$prefix_attestation_sha" ] \
    || fail 'parallel transition changed the sequential prefix attestation'
jq -e '
    .kind == "archive-v2-pre-to-post-parallel-transition-complete" and
    .epochs == [0,1,2] and
    [.epoch_producers[].phase] == ["sequential-prefix","parallel-suffix","parallel-suffix"] and
    [.epoch_producers[].worker] == [null,0,1] and
    .converter_origin != null and .converter_pinned != null and
    .epoch_list_snapshot != null
' "$resume_root/handoff/conversion/parallel-transition-complete.json" >/dev/null \
    || fail 'ordered transition completion is invalid'
jq -e '
    .kind == "archive-v2-pre-to-post-parallel-transition-handoff-complete" and
    .converter_origin != null and .converter_pinned != null and
    .epoch_list_snapshot != null
' "$resume_root/handoff/complete.json" >/dev/null \
    || fail 'replacement handoff completion schema is invalid'
claim_sha=$(sha256_file "$resume_root/handoff/conversion/parallel-transition/claims/epoch-1.claim.json")
jq -e --arg claim_sha "$claim_sha" --arg admission_sha \
    "$(sha256_file "$resume_root/handoff/conversion/parallel-transition-admission.json")" '
    .worker == 0 and .claim.sha256 == $claim_sha and .admission.sha256 == $admission_sha
' "$resume_root/handoff/conversion/epoch-1.parallel-attestation.json" >/dev/null \
    || fail 'parallel companion attestation is not exactly claim/admission-bound'

# Duplicate/mismatched claim: immutable ownership must fail before conversion.
duplicate_root=$test_root/duplicate
make_fixture "$duplicate_root" '1' 1
: >"$duplicate_root/control/fail-1"
if run_transition "$duplicate_root" 1 >/dev/null 2>&1; then
    fail 'claim setup failure run returned success'
fi
duplicate_claim=$duplicate_root/handoff/conversion/parallel-transition/claims/epoch-1.claim.json
chmod 600 "$duplicate_claim"
jq '.worker = 1' "$duplicate_claim" >"$duplicate_claim.tampered"
mv "$duplicate_claim.tampered" "$duplicate_claim"
chmod 400 "$duplicate_claim"
if run_transition "$duplicate_root" 1 >/dev/null 2>&1; then
    fail 'mismatched duplicate claim was accepted'
fi
[ ! -e "$duplicate_root/target/epoch-1" ] \
    || fail 'mismatched duplicate claim started conversion'

# Signal: both worker shells must forward TERM and reap converters. Authority
# locks stay in place for explicit review of possible partial staging.
signal_root=$test_root/signal
make_fixture "$signal_root" '1 2' 1
: >"$signal_root/control/sleep-all"
FAKE_CONTROL_ROOT="$signal_root/control" FAKE_CONVERTER_PATH="$converter" PATH="$test_root:$PATH" \
    "$parallel_runner" "$signal_root/handoff" 1 2 \
    >"$signal_root/run.out" 2>"$signal_root/run.err" &
signal_pid=$!
signal_wait=0
while [ "$signal_wait" -lt 100 ]; do
    [ -e "$signal_root/control/entered-1" ] && [ -e "$signal_root/control/entered-2" ] && break
    sleep 0.1
    signal_wait=$((signal_wait + 1))
done
[ -e "$signal_root/control/entered-1" ] && [ -e "$signal_root/control/entered-2" ] \
    || fail 'signal test workers did not both start'
kill -TERM "$signal_pid"
if wait "$signal_pid"; then
    fail 'signaled coordinator returned success'
fi
[ -e "$signal_root/control/terminated-1" ] && [ -e "$signal_root/control/terminated-2" ] \
    || fail 'coordinator did not terminate and reap both converters'
[ -d "$signal_root/handoff/.archive-v2-pre-to-post-handoff.lock" ] \
    && [ -d "$signal_root/target/.archive-v2-pre-to-post-handoff.lock" ] \
    && [ -d "$signal_root/handoff/conversion/.archive-v2-pre-to-post-manual.lock" ] \
    || fail 'signal path did not preserve all authority locks'
[ ! -e "$signal_root/handoff/complete.json" ] || fail 'signal path published false completion'

echo 'parallel Pre-to-Post transition self-test passed'
