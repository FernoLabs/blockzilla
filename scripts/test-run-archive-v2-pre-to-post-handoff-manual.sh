#!/bin/sh

set -eu
umask 077

die() {
    echo "manual Pre-to-Post handoff test: $*" >&2
    exit 1
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
handoff=$script_dir/run-archive-v2-pre-to-post-handoff-manual.sh

for required in jq awk cat cp chmod mkdir mv rm sleep sh; do
    command -v "$required" >/dev/null 2>&1 || die "missing command: $required"
done
if command -v sha256sum >/dev/null 2>&1; then
    sha256_file() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
    sha256_file() { shasum -a 256 "$1" | awk '{print $1}'; }
else
    die 'sha256sum or shasum is required'
fi
sh -n "$handoff" || die 'handoff wrapper does not pass sh -n'
if command -v dash >/dev/null 2>&1; then
    dash -n "$handoff" || die 'handoff wrapper does not pass dash -n'
    handoff_shell=dash
else
    handoff_shell=sh
fi

test_root=$(mktemp -d "${TMPDIR:-/tmp}/archive-v2-pre-to-post-handoff-test.XXXXXX") \
    || die 'cannot create temporary test root'
test_root=$(CDPATH= cd -P "$test_root" && pwd -P)
case "${test_root##*/}" in
    archive-v2-pre-to-post-handoff-test.*) ;;
    *) die "unexpected temporary test root: $test_root" ;;
esac
cleanup() {
    cleanup_status=$?
    trap - 0 1 2 3 15
    if [ -n "${active_scan_pid:-}" ]; then
        kill "$active_scan_pid" 2>/dev/null || :
        wait "$active_scan_pid" 2>/dev/null || :
    fi
    if [ "${KEEP_HANDOFF_TEST_ROOT:-0}" = 1 ]; then
        echo "kept handoff test root: $test_root" >&2
    else
        rm -rf "$test_root"
    fi
    exit "$cleanup_status"
}
trap 'cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

tools_root=$test_root/tools
mkdir "$tools_root"
trace=$test_root/trace.log
: >"$trace"

plain_builder=$tools_root/builder.sh
cat >"$plain_builder" <<'BUILDER'
#!/bin/sh
set -eu
[ "$#" -eq 10 ] || exit 20
base=$1
rescan=$2
archive=$3
target=$4
output=$5
first=$6
last=$7
rescan_first=$8
rescan_last=$9
exclude=${10}
if kill -0 "$TEST_SCAN_PID" 2>/dev/null; then
    echo 'builder observed the scanner process alive' >&2
    exit 21
fi
printf 'builder\n' >>"$TEST_TRACE"
[ "$(sed -n '1p' "$exclude")" = 2 ] || exit 22
[ "$(wc -l <"$exclude" | tr -d '[:space:]')" -eq 1 ] || exit 23
mkdir "$output"
printf '3\n' >"$output/epochs.txt"
reports=$output/.reports.ndjson
: >"$reports"
sha_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}
epoch=$first
while [ "$epoch" -le "$last" ]; do
    if [ "$epoch" -ge "$rescan_first" ] && [ "$epoch" -le "$rescan_last" ]; then
        report_set=rescan
        root=$rescan
    else
        report_set=base
        root=$base
    fi
    report=$root/epoch-$epoch.json
    report_sha=$(sha_file "$report")
    jq -c \
        --arg report_set "$report_set" \
        --arg report_path "$report" \
        --arg report_sha "$report_sha" '
        {
            epoch: .epoch,
            report_set: $report_set,
            report_path: $report_path,
            report_sha256: $report_sha,
            archive: .archive,
            classification: .classification,
            action: .action,
            counts: .counts
        }
    ' "$report" >>"$reports"
    epoch=$((epoch + 1))
done
epoch_sha=$(sha_file "$output/epochs.txt")
exclude_sha=$(sha_file "$exclude")
jq -n \
    --slurpfile reports "$reports" \
    --arg base "$base" \
    --arg rescan "$rescan" \
    --arg archive "$archive" \
    --arg target "$target" \
    --arg exclude "$exclude" \
    --arg exclude_sha "$exclude_sha" \
    --arg epoch_sha "$epoch_sha" \
    --argjson first "$first" \
    --argjson last "$last" \
    --argjson rescan_first "$rescan_first" \
    --argjson rescan_last "$rescan_last" '
    {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-cohort",
        archive_root: $archive,
        target_root: $target,
        source_scanner_roots: {base: $base, rescan: $rescan},
        selected_epoch_range: {first: $first, last: $last, count: 4},
        rescan_epoch_range: {first: $rescan_first, last: $rescan_last, count: 2},
        selected_report_count: 4,
        selected_report_set_counts: {base: 2, rescan: 2},
        classification_totals: {
            canonical_post: 2,
            legacy_pre: 2,
            canonical_equivalent: 0
        },
        selected_counts: {
            blocks: 4,
            compressed_block_bytes: 16,
            uncompressed_block_bytes: 32,
            typed_messages: 4,
            owned_fallback_blocks: 0,
            raw_transaction_fallbacks: 0,
            post_only: 2,
            pre_only: 2,
            both_equivalent: 0,
            both_divergent: 0,
            invalid: 0
        },
        exclude_epoch_list: $exclude,
        exclude_epoch_list_sha256: $exclude_sha,
        excluded_epochs: [2],
        excluded_targets: [{
            epoch: 2,
            target: ($target + "/epoch-2"),
            source_report_sha256:
                ([$reports[] | select(.epoch == 2) | .report_sha256][0])
        }],
        conversion_epoch_count: 1,
        conversion_epochs: [3],
        epochs_file: "epochs.txt",
        epoch_list_sha256: $epoch_sha,
        reports: $reports
    }
' >"$output/cohort.json"
rm "$reports"
BUILDER
chmod 755 "$plain_builder"
builder_sha=$(sha256_file "$plain_builder")
builder=$tools_root/build-archive-v2-pre-to-post-cohort-$builder_sha.sh
mv "$plain_builder" "$builder"

plain_runner=$tools_root/runner.sh
cat >"$plain_runner" <<'RUNNER'
#!/bin/sh
set -eu
[ "$#" -eq 7 ] || exit 30
converter=$1
source=$2
target=$3
state=$4
epochs=$5
cluster=$6
run_id=$7
if kill -0 "$TEST_SCAN_PID" 2>/dev/null; then
    echo 'runner observed the scanner process alive' >&2
    exit 31
fi
printf 'runner\n' >>"$TEST_TRACE"
mkdir -p "$state"
sha_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}
converter_sha=$(sha_file "$converter")
epochs_sha=$(sha_file "$epochs")
epoch_values=$(jq -Rsc 'split("\n") | map(select(length > 0) | tonumber)' "$epochs")
epoch_count=$(jq 'length' <<EOF
$epoch_values
EOF
)
jq -cn \
    --arg run_id "$run_id" \
    --arg cluster "$cluster" \
    --arg converter "$converter" \
    --arg converter_sha "$converter_sha" \
    --arg source "$source" \
    --arg target "$target" \
    --arg state "$state" \
    --arg epochs "$epochs" \
    --arg epochs_sha "$epochs_sha" \
    --argjson epoch_count "$epoch_count" \
    --argjson epoch_values "$epoch_values" '
    {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-manual-run-complete",
        run_id: $run_id,
        cluster_id: $cluster,
        converter: $converter,
        converter_sha256: $converter_sha,
        source_root: $source,
        target_root: $target,
        state_root: $state,
        epoch_list: $epochs,
        epoch_list_sha256: $epochs_sha,
        source_authority_kind: "linux-kernel-read-leases",
        epoch_count: $epoch_count,
        completed_epochs: $epoch_count,
        epochs: $epoch_values,
        completed_at_utc: "2026-08-24T00:00:00Z"
    }
' >"$state/complete.json"
RUNNER
chmod 755 "$plain_runner"
runner_sha=$(sha256_file "$plain_runner")
runner=$tools_root/run-archive-v2-pre-to-post-manual-$runner_sha.sh
mv "$plain_runner" "$runner"

if [ -x /usr/bin/true ]; then
    true_program=/usr/bin/true
elif [ -x /bin/true ]; then
    true_program=/bin/true
else
    die 'a real true executable is required'
fi
converter_plain=$tools_root/converter
cp "$true_program" "$converter_plain"
chmod 755 "$converter_plain"
converter_sha=$(sha256_file "$converter_plain")
converter=$tools_root/archive-v2-pre-to-post-$converter_sha
mv "$converter_plain" "$converter"

make_report() {
    mr_root=$1
    mr_archive=$2
    mr_epoch=$3
    mr_class=$4
    case "$mr_class" in
        canonical-post)
            mr_action=none
            mr_post=1
            mr_pre=0
            mr_post_evidence='{"slot":1,"transaction_index":0}'
            mr_pre_evidence=null
            ;;
        legacy-pre)
            mr_action=convert-to-post
            mr_post=0
            mr_pre=1
            mr_post_evidence=null
            mr_pre_evidence='{"slot":1,"transaction_index":0}'
            ;;
        *) die "unsupported test classification: $mr_class" ;;
    esac
    jq -cn \
        --arg archive "$mr_archive/epoch-$mr_epoch" \
        --argjson epoch "$mr_epoch" \
        --arg class "$mr_class" \
        --arg action "$mr_action" \
        --argjson post "$mr_post" \
        --argjson pre "$mr_pre" \
        --argjson post_evidence "$mr_post_evidence" \
        --argjson pre_evidence "$mr_pre_evidence" '
        {
            schema_version: 1,
            kind: "archive-v2-wire-profile-scan",
            archive: $archive,
            epoch: $epoch,
            workers: 8,
            classification: $class,
            action: $action,
            counts: {
                blocks: 1,
                owned_fallback_blocks: 0,
                compressed_block_bytes: 4,
                uncompressed_block_bytes: 8,
                typed_messages: 1,
                raw_transaction_fallbacks: 0,
                post_only: $post,
                pre_only: $pre,
                both_equivalent: 0,
                both_divergent: 0,
                invalid: 0
            },
            first_evidence: {
                post_only: $post_evidence,
                pre_only: $pre_evidence,
                both_divergent: null,
                invalid: null
            },
            error: null,
            elapsed_seconds: 1,
            completed_unix_seconds: 1
        }
    ' >"$mr_root/epoch-$mr_epoch.json"
}

make_case() {
    mc_name=$1
    case_root=$test_root/$mc_name
    base=$case_root/base
    rescan=$case_root/rescan
    archive=$case_root/archive
    target=$case_root/target
    state=$case_root/state
    exclude=$case_root/exclude.txt
    pid_file=$rescan/rescan-runner.pid
    mkdir -p "$base" "$rescan" "$archive" "$target"
    epoch=0
    while [ "$epoch" -le 3 ]; do
        mkdir "$archive/epoch-$epoch"
        epoch=$((epoch + 1))
    done
    make_report "$base" "$archive" 0 canonical-post
    make_report "$rescan" "$archive" 1 canonical-post
    make_report "$rescan" "$archive" 2 legacy-pre
    make_report "$base" "$archive" 3 legacy-pre
    printf '2\n' >"$exclude"
}

publish_dead_pid() {
    sh -c 'exit 0' &
    pdp_pid=$!
    wait "$pdp_pid"
    printf '%s\n' "$pdp_pid" >"$pid_file"
    test_scan_pid=$pdp_pid
}

run_handoff() {
    TEST_SCAN_PID=$test_scan_pid TEST_TRACE=$trace "$handoff_shell" "$handoff" \
        "$builder" "$runner" "$converter" \
        "$base" "$rescan" "$archive" "$target" "$state" "$pid_file" \
        0 3 1 2 "$exclude" test-cluster test-run 1
}

make_case success
# A stale base-scan temporary file is outside this handoff's corrected-scan
# authority. The cohort builder still validates the final base report.
: >"$base/epoch-0.json.building-1203438"
sleep 2 &
active_scan_pid=$!
test_scan_pid=$active_scan_pid
printf '%s\n' "$active_scan_pid" >"$pid_file"
run_handoff >/dev/null
wait "$active_scan_pid" 2>/dev/null || :
active_scan_pid=
[ "$(sed -n '1p' "$trace")" = builder ] || die 'cohort builder did not run first'
[ "$(sed -n '2p' "$trace")" = runner ] || die 'converter runner did not run second'
[ "$(wc -l <"$trace" | tr -d '[:space:]')" -eq 2 ] \
    || die 'success path ran a dependency more than once'
[ -s "$state/complete.json" ] || die 'success path did not publish handoff complete.json'
[ ! -e "$state/.archive-v2-pre-to-post-handoff.lock" ] \
    || die 'success path left its state lock'
[ ! -e "$target/.archive-v2-pre-to-post-handoff.lock" ] \
    || die 'success path left its target lock'
jq -e '
    .kind == "archive-v2-pre-to-post-manual-handoff-complete" and
    .conversion_epoch_count == 1
' "$state/complete.json" >/dev/null || die 'handoff completion record is wrong'

# Exact resume must accept the prior private bindings without running either
# dependency a second time.
publish_dead_pid
# The request binds the original PID. Keep that exact, now-dead PID for resume.
test_scan_pid=$(jq -r '.scan_pid' "$state/request.json")
printf '%s\n' "$test_scan_pid" >"$pid_file"
run_handoff >/dev/null
[ "$(wc -l <"$trace" | tr -d '[:space:]')" -eq 2 ] \
    || die 'exact resume reran a dependency'

trace_lines=$(wc -l <"$trace" | tr -d '[:space:]')
make_case missing
rm "$rescan/epoch-1.json"
publish_dead_pid
if run_handoff >/dev/null 2>&1; then
    die 'missing corrected report was accepted after scanner exit'
fi
[ "$(wc -l <"$trace" | tr -d '[:space:]')" -eq "$trace_lines" ] \
    || die 'missing-report case invoked a dependency'

make_case invalid
jq '.error = "synthetic failure"' "$rescan/epoch-1.json" \
    >"$rescan/epoch-1.json.bad"
mv "$rescan/epoch-1.json.bad" "$rescan/epoch-1.json"
publish_dead_pid
if run_handoff >/dev/null 2>&1; then
    die 'invalid corrected report was accepted'
fi
[ "$(wc -l <"$trace" | tr -d '[:space:]')" -eq "$trace_lines" ] \
    || die 'invalid-report case invoked a dependency'

make_case building
: >"$rescan/epoch-2.json.building-999"
publish_dead_pid
if run_handoff >/dev/null 2>&1; then
    die 'stale building report was accepted after scanner exit'
fi
[ "$(wc -l <"$trace" | tr -d '[:space:]')" -eq "$trace_lines" ] \
    || die 'building-report case invoked a dependency'

make_case locked
mkdir "$target/.archive-v2-pre-to-post-handoff.lock"
publish_dead_pid
if run_handoff >/dev/null 2>&1; then
    die 'concurrent target publisher lock was ignored'
fi
[ "$(wc -l <"$trace" | tr -d '[:space:]')" -eq "$trace_lines" ] \
    || die 'target-lock case invoked a dependency'

make_case tampered-tool
publish_dead_pid
tampered_tools=$test_root/tampered-tools
mkdir "$tampered_tools"
tampered_builder=$tampered_tools/${builder##*/}
cp "$builder" "$tampered_builder"
printf '\n# changed after content addressing\n' >>"$tampered_builder"
chmod 755 "$tampered_builder"
original_builder=$builder
builder=$tampered_builder
if run_handoff >/dev/null 2>&1; then
    die 'tampered content-addressed builder was accepted'
fi
builder=$original_builder
[ "$(wc -l <"$trace" | tr -d '[:space:]')" -eq "$trace_lines" ] \
    || die 'tampered-tool case invoked a dependency'

echo 'manual Pre-to-Post handoff tests passed'
