#!/bin/sh
# Focused recovery and safety tests for the one-epoch multicore canary runner.

set -eu
umask 077

fail() {
    echo "fast canary self-test: $*" >&2
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
runner=$script_dir/run-archive-v2-pre-to-post-fast-canary.sh
test_root=$(mktemp -d "${TMPDIR:-/tmp}/archive-v2-fast-canary-test.XXXXXX") \
    || fail 'cannot create test directory'
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

for required in jq awk wc tr dd mktemp; do
    command -v "$required" >/dev/null 2>&1 || fail "$required is required"
done

test_bin=$test_root/bin
control=$test_root/control
mkdir "$test_bin" "$control"

cat >"$test_bin/ps" <<'FAKE_PS'
#!/bin/sh
case "$(cat "$FAKE_PS_MODE_FILE" 2>/dev/null || :)" in
    reader)
        printf '900 archive-reader --source %s/epoch-43\n' "$FAKE_ARCHIVE_ROOT"
        ;;
    old)
        printf '901 /bin/sh /volume1/blockzilla/bin/run-archive-v2-pre-to-post-fast-in-place-deadbeef.sh\n'
        ;;
esac
FAKE_PS
cat >"$test_bin/nice" <<'FAKE_NICE'
#!/bin/sh
[ "$1" = -n ] || exit 64
shift 2
exec "$@"
FAKE_NICE
cat >"$test_bin/ionice" <<'FAKE_IONICE'
#!/bin/sh
[ "$1" = -c ] || exit 64
shift 2
[ "$1" = -n ] || exit 64
shift 2
exec "$@"
FAKE_IONICE
chmod 700 "$test_bin/ps" "$test_bin/nice" "$test_bin/ionice"

converter=$test_root/fake-archive-v2-pre-to-post
cat >"$converter" <<'FAKE_CONVERTER'
#!/bin/sh
set -eu
source_path= staging= epoch= cluster= generation= audit= audit_sha= threads=
fast=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --fast-candidate) fast=1; shift ;;
        --threads) threads=$2; shift 2 ;;
        --source) source_path=$2; shift 2 ;;
        --source-lease-id|--target) shift 2 ;;
        --staging) staging=$2; shift 2 ;;
        --epoch) epoch=$2; shift 2 ;;
        --cluster-id) cluster=$2; shift 2 ;;
        --generation-id) generation=$2; shift 2 ;;
        --source-audit-report) audit=$2; shift 2 ;;
        --source-audit-report-sha256) audit_sha=$2; shift 2 ;;
        *) exit 64 ;;
    esac
done
[ "$fast" -eq 1 ] && [ -n "$threads" ] && [ -n "$source_path" ] || exit 64
control=${FAKE_CONTROL_ROOT:?}
count_file=$control/invocations-$epoch
count=0
[ ! -f "$count_file" ] || read -r count <"$count_file"
count=$((count + 1))
printf '%s\n' "$count" >"$count_file"
printf '%s\n' "$threads" >>"$control/threads-$epoch"
if [ -f "$control/fail-once-$epoch" ] && [ ! -f "$control/failed-$epoch" ]; then
    mkdir "$staging"
    printf 'journal\n' >"$staging/journal"
    : >"$control/failed-$epoch"
    exit 23
fi
if [ -d "$staging" ]; then
    rm -rf "$staging"
fi
backup=${source_path%/epoch-*}/.epoch-$epoch.pre-to-post.backup
mkdir "$backup"
mv "$source_path/archive-v2-blocks.zstd" "$backup/archive-v2-blocks.zstd"
mv "$source_path/archive-v2-blocks.index" "$backup/archive-v2-blocks.index"
printf 'post-blocks\n' >"$source_path/archive-v2-blocks.zstd"
printf 'post-index\n' >"$source_path/archive-v2-blocks.index"
descriptor=$source_path/archive-v2-pre-to-post.candidate.v1.json
jq -cn --argjson epoch "$epoch" --arg cluster "$cluster" \
    --arg source "$source_path" --arg backup "$backup" --arg generation "$generation" \
    --arg audit "$audit" --arg audit_sha "$audit_sha" '
    {schema_version:1,kind:"archive-v2-pre-to-post-candidate",state:"unfinalized",
     canonical:false,epoch:$epoch,cluster_id:$cluster,source:$source,candidate:$source,
     backup:$backup,prospective_generation_id:$generation,
     source_audit_report:{path:$audit,sha256:$audit_sha},
     single_decode_rewrite_pass:true,sidecars_copied:false,sidecars_rewritten:false,
     rewrite:{blocks:1,typed_messages:2,message_input_bytes:9,message_output_bytes:9},
     exact_message_length_preserved:true,exact_message_delta_proved:true,
     canonical_publication_deferred:true,target_post_audit_performed:false}
' >"$descriptor"
printf '{}\n' >"$backup/archive-v2-pre-to-post.switch-intent.v1.json"
printf '{}\n' >"$backup/archive-v2-pre-to-post.switch-complete.v1.json"
if command -v sha256sum >/dev/null 2>&1; then
    descriptor_sha=$(sha256sum "$descriptor" | awk '{print $1}')
else
    descriptor_sha=$(shasum -a 256 "$descriptor" | awk '{print $1}')
fi
if [ -f "$control/failed-$epoch" ]; then
    kind=archive-v2-pre-to-post-candidate-recovery-report
else
    kind=archive-v2-pre-to-post-candidate-report
fi
jq -cn --arg kind "$kind" --argjson epoch "$epoch" --arg cluster "$cluster" \
    --arg source "$source_path" --arg backup "$backup" --arg generation "$generation" \
    --arg descriptor "$descriptor" --arg descriptor_sha "$descriptor_sha" \
    --arg audit "$audit" --arg audit_sha "$audit_sha" '
    {schema_version:1,kind:$kind,state:"unfinalized",canonical:false,epoch:$epoch,
     cluster_id:$cluster,prospective_generation_id:$generation,candidate:$source,
     backup:$backup,candidate_descriptor:$descriptor,
     candidate_descriptor_sha256:$descriptor_sha,source_audit_report:$audit,
     source_audit_report_sha256:$audit_sha,recovered_switch:($kind | contains("recovery")),
     already_complete:false,source:$source,single_decode_rewrite_pass:true,
     canonical_publication_deferred:true}
' 
FAKE_CONVERTER
chmod 700 "$converter"

make_fixture() {
    fixture_root=$1
    fixture_epoch=$2
    handoff=$fixture_root/archive-v2-state
    archive=$fixture_root/archive
    fast=$handoff/fast-in-place-candidate
    audit_dir=$fast/source-audit-reports
    source=$archive/epoch-$fixture_epoch
    mkdir -p "$fast/results" "$fast/claims" "$audit_dir" "$source"
    jq -cn --arg archive "$archive" --arg state "$handoff" \
        '{archive_root:$archive,state_root:$state,cluster_id:"mainnet",run_id:"test-run"}' \
        >"$handoff/request.json"
    audit=$audit_dir/epoch-$fixture_epoch.json
    jq -cn --argjson epoch "$fixture_epoch" --arg archive "$source" \
        '{schema_version:1,kind:"archive-v2-wire-profile-scan",epoch:$epoch,archive:$archive,
          classification:"legacy-pre",counts:{blocks:1,typed_messages:2}}' >"$audit"
    audit_sha=$(sha256_file "$audit")
    printf 'source-blocks\n' >"$source/archive-v2-blocks.zstd"
    source_bytes=$(wc -c <"$source/archive-v2-blocks.zstd" | tr -d '[:space:]')
    dd if=/dev/zero of="$source/archive-v2-blocks.index" bs=88 count=1 2>/dev/null
    table=$fast/all-legacy-pre-reports.tsv
    printf '%s\t%s\t%s\t1\t%s\t32\t2\t1\t1\n' \
        "$fixture_epoch" "$audit" "$audit_sha" "$source_bytes" >"$table"
    jq -cn --arg archive "$archive" --arg state "$fast" --arg table "$table" \
        --arg audits "$audit_dir" '
        {schema_version:1,kind:"archive-v2-pre-to-post-fast-in-place-config",
         run_id:"test-run",cluster_id:"mainnet",archive_root:$archive,state_root:$state,
         source_audit_table:$table,pinned_source_audit_directory:$audits}
    ' >"$fast/config.json"
    chmod 400 "$audit" "$table" "$fast/config.json"
}

run_canary() {
    test_shell=$1
    shift
    PATH="$test_bin:$PATH" FAKE_CONTROL_ROOT="$control" \
        FAKE_PS_MODE_FILE="$control/ps-mode" FAKE_ARCHIVE_ROOT="$FAKE_ARCHIVE_ROOT" \
        "$test_shell" "$runner" "$@"
}

shells=sh
if command -v dash >/dev/null 2>&1; then shells='sh dash'; fi
for test_shell in $shells; do
    "$test_shell" -n "$runner" || fail "$test_shell syntax check failed"
    fixture=$test_root/recovery-$test_shell
    mkdir "$fixture"
    make_fixture "$fixture" 42
    handoff=$fixture/archive-v2-state
    FAKE_ARCHIVE_ROOT=$fixture/archive
    export FAKE_ARCHIVE_ROOT
    : >"$control/fail-once-42"
    if run_canary "$test_shell" "$handoff" 42 "$converter" 8 \
        >"$fixture/first.stdout" 2>"$fixture/first.stderr"; then
        fail "$test_shell accepted the injected converter failure"
    fi
    [ ! -e "$FAKE_ARCHIVE_ROOT/.archive-v2-pre-to-post-fast-candidate.lock" ] \
        || fail "$test_shell left the root lock after converter failure"
    [ -d "$FAKE_ARCHIVE_ROOT/.epoch-42.pre-to-post.staging" ] \
        || fail "$test_shell did not leave the recovery journal"
    run_canary "$test_shell" "$handoff" 42 "$converter" 8 \
        >"$fixture/second.stdout" 2>"$fixture/second.stderr" \
        || fail "$test_shell did not recover the canary"
    result=$handoff/fast-canary-epoch-42/result.json
    jq -e '.epoch == 42 and .threads == 8 and .recovered_switch == true and
        .source_compressed_bytes > 0 and (.source_mib_per_second | type == "number") and
        .canonical == false' "$result" >/dev/null \
        || fail "$test_shell published an invalid result"
    [ "$(cat "$control/invocations-42")" -eq 2 ] \
        || fail "$test_shell used the wrong recovery invocation count"
    [ "$(tail -n 1 "$control/threads-42")" -eq 8 ] \
        || fail "$test_shell did not pass --threads 8"
    run_canary "$test_shell" "$handoff" 42 "$converter" 8 \
        >"$fixture/resume.stdout" 2>"$fixture/resume.stderr" \
        || fail "$test_shell did not accept its completed result"
    [ "$(cat "$control/invocations-42")" -eq 2 ] \
        || fail "$test_shell reran a completed converter"
    if run_canary "$test_shell" "$handoff" 42 "$converter" 4 \
        >"$fixture/thread-change.stdout" 2>"$fixture/thread-change.stderr"; then
        fail "$test_shell accepted changed threads on resume"
    fi
    [ ! -e "$FAKE_ARCHIVE_ROOT/.archive-v2-pre-to-post-fast-candidate.lock" ] \
        || fail "$test_shell left the root lock after resume rejection"
    rm "$control/fail-once-42" "$control/failed-42" \
        "$control/invocations-42" "$control/threads-42"
done

fixture=$test_root/guards
mkdir "$fixture"
make_fixture "$fixture" 43
handoff=$fixture/archive-v2-state
FAKE_ARCHIVE_ROOT=$fixture/archive
export FAKE_ARCHIVE_ROOT
printf 'reader\n' >"$control/ps-mode"
if run_canary sh "$handoff" 43 "$converter" 8 \
    >"$fixture/reader.stdout" 2>"$fixture/reader.stderr"; then
    fail 'active archive reader was accepted'
fi
[ ! -e "$handoff/fast-canary-epoch-43" ] \
    || fail 'reader rejection created canary state'
[ ! -e "$FAKE_ARCHIVE_ROOT/.archive-v2-pre-to-post-fast-candidate.lock" ] \
    || fail 'reader rejection left the root lock'
printf 'old\n' >"$control/ps-mode"
if run_canary sh "$handoff" 43 "$converter" 8 \
    >"$fixture/old.stdout" 2>"$fixture/old.stderr"; then
    fail 'active old runner was accepted'
fi
[ ! -e "$FAKE_ARCHIVE_ROOT/.archive-v2-pre-to-post-fast-candidate.lock" ] \
    || fail 'old-runner rejection left the root lock'
rm "$control/ps-mode"

# jq 1.6 gives `false` exit status 1 with -e. A fresh report omits the field,
# so this run is the regression for the valid default value false.
fixture=$test_root/fresh-false
mkdir "$fixture"
make_fixture "$fixture" 44
handoff=$fixture/archive-v2-state
FAKE_ARCHIVE_ROOT=$fixture/archive
export FAKE_ARCHIVE_ROOT
run_canary sh "$handoff" 44 "$converter" 8 \
    >"$fixture/stdout" 2>"$fixture/stderr" || fail 'fresh false recovery flag was rejected'
jq -e '.epoch == 44 and .recovered_switch == false' \
    "$handoff/fast-canary-epoch-44/result.json" >/dev/null \
    || fail 'fresh false recovery result is invalid'
[ "$(cat "$control/invocations-44")" -eq 1 ] \
    || fail 'fresh false regression reran the converter'

echo 'fast canary self-test: PASS'
