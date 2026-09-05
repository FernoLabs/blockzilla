#!/bin/sh

set -eu
umask 077

die() {
    echo "staged R2 publisher test: $*" >&2
    exit 1
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
publisher=$script_dir/publish-network-format-benchmark-r2.sh
sh -n "$publisher" || die "publisher does not pass sh -n"
if command -v dash >/dev/null 2>&1; then
    dash -n "$publisher" || die "publisher does not pass dash -n"
    test_shell=dash
else
    test_shell=sh
fi

test_root=$(mktemp -d "${TMPDIR:-/tmp}/blockzilla-r2-publisher-test.XXXXXX") \
    || die "cannot create test root"
test_root=$(CDPATH= cd -P "$test_root" && pwd -P)
case "${test_root##*/}" in blockzilla-r2-publisher-test.*) ;; *) die "unsafe test root" ;; esac
cleanup() {
    cleanup_status=$?
    trap - 0 1 2 3 15
    if [ "${KEEP_R2_PUBLISHER_TEST_ROOT:-0}" = 1 ]; then
        echo "kept test root: $test_root" >&2
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

tools=$test_root/tools
remote_root=$test_root/remote
log=$test_root/rclone.log
mkdir "$tools" "$remote_root"
: >"$log"

flock_mock=$tools/flock
apply_mock=$tools/rclone
printf '%s\n' '#!/bin/sh' 'exit 0' >"$flock_mock"
chmod 755 "$flock_mock"

cat >"$apply_mock" <<'RCLONE'
#!/bin/sh
set -eu

printf '%s\n' "$*" >>"$MOCK_RCLONE_LOG"
command_name=$1
shift

remote_file() {
    rf_value=$1
    case "$rf_value" in
        *:*) printf '%s/%s\n' "$MOCK_R2_ROOT" "${rf_value#*:}" ;;
        *) printf '%s\n' "$rf_value" ;;
    esac
}

size_of() {
    wc -c <"$1" | tr -d '[:space:]'
}

case "$command_name" in
    version)
        echo 'rclone v1.72.0-test'
        ;;
    lsf)
        target=
        for argument in "$@"; do target=$argument; done
        mapped=$(remote_file "$target")
        [ -e "$mapped" ] || exit 0
        if [ -f "$mapped" ]; then
            printf '%s\t%s\n' "$(size_of "$mapped")" "${mapped##*/}"
            exit 0
        fi
        find "$mapped" -type f -print | sort | while IFS= read -r object; do
            relative=${object#"$mapped"/}
            printf '%s\t%s\n' "$(size_of "$object")" "$relative"
        done
        ;;
    copyto)
        source_value=$1
        destination_value=$2
        immutable=0
        for argument in "$@"; do
            [ "$argument" != --immutable ] || immutable=1
        done
        source_file=$(remote_file "$source_value")
        destination_file=$(remote_file "$destination_value")
        if [ -n "${MOCK_RCLONE_FAIL_DEST:-}" ] && [ "$destination_value" = "$MOCK_RCLONE_FAIL_DEST" ]; then
            fail_marker=$MOCK_R2_ROOT/.fail-once
            if [ ! -e "$fail_marker" ]; then
                : >"$fail_marker"
                exit 23
            fi
        fi
        [ -f "$source_file" ] || exit 24
        if [ -e "$destination_file" ] && [ "$immutable" -eq 1 ]; then
            [ -f "$destination_file" ] || exit 25
            [ "$(size_of "$source_file")" = "$(size_of "$destination_file")" ] || exit 26
            exit 0
        fi
        mkdir -p "$(dirname "$destination_file")"
        cp "$source_file" "$destination_file"
        ;;
    *)
        echo "unexpected mock rclone command: $command_name" >&2
        exit 90
        ;;
esac
RCLONE
chmod 755 "$apply_mock"

export PATH=$tools:$PATH
export MOCK_R2_ROOT=$remote_root
export MOCK_RCLONE_LOG=$log

make_release() {
    mr_name=$1
    mr_root=$test_root/$mr_name
    mkdir "$mr_root"
    printf 'payload' >"$mr_root/payload.bin"
    printf 'ctrl' >"$mr_root/control.bin"
    mr_inventory=$mr_root/inventory.tsv
    printf 'role\tsource_kind\tsource\ttarget_key\tbytes\n' >"$mr_inventory"
    printf 'payload\tlocal\t%s\tcompact-v2/releases/%s/payload.bin\t7\n' \
        "$mr_root/payload.bin" "$mr_name" >>"$mr_inventory"
    printf 'payload\tstaged-copy\tcompact-v2/releases/%s/payload.bin\tindexer-v3/releases/%s/payload-copy.bin\t7\n' \
        "$mr_name" "$mr_name" >>"$mr_inventory"
    printf 'control\tlocal\t%s\tcompact-v2/releases/%s/control.bin\t4\n' \
        "$mr_root/control.bin" "$mr_name" >>"$mr_inventory"
}

run_publisher() {
    rp_release=$1
    rp_state=$2
    rp_mode=$3
    shift 3
    "$test_shell" "$publisher" \
        --inventory "$test_root/$rp_release/inventory.tsv" \
        --state-dir "$rp_state" \
        --release-id "$rp_release" \
        --scope "compact-v2/releases/$rp_release" \
        --scope "indexer-v3/releases/$rp_release" \
        --mode "$rp_mode" \
        --rclone-remote mock \
        "$@"
}

# The publisher always requires a separate private-stage review and promotion.
release=e900-all-rejected-r1
make_release "$release"
if run_publisher "$release" "$test_root/state-all-rejected" all >/dev/null 2>&1; then
    die "removed all-in-one mode was accepted"
fi
[ ! -e "$test_root/state-all-rejected" ] \
    || die "rejected all-in-one mode created publisher state"

# A private stage and an explicit promotion both complete with zero completion
# rows. The control object is the last object copied into each bucket.
release=e900-test-r1
make_release "$release"
[ "$(awk -F '\t' '$1 == "completion" { count++ } END { print count + 0 }' "$test_root/$release/inventory.tsv")" -eq 0 ] \
    || die "zero-completion fixture contains a completion row"
state=$test_root/state-happy
run_publisher "$release" "$state" stage >/dev/null
[ -f "$remote_root/blockzilla-network-format-benchmark-staging-v1/compact-v2/releases/$release/control.bin" ] \
    || die "stage did not upload the control object"
[ ! -e "$remote_root/blockzilla-network-format-benchmark-v1/compact-v2/releases/$release/payload.bin" ] \
    || die "stage mode wrote to the serving bucket"
run_publisher "$release" "$state" promote --resume >/dev/null
[ -f "$remote_root/blockzilla-network-format-benchmark-v1/compact-v2/releases/$release/control.bin" ] \
    || die "promotion did not publish the control object"
[ -f "$state/publish.complete" ] || die "publish completion state is absent"
grep -q 'copyto .*blockzilla-network-format-benchmark-staging-v1.* --immutable --size-only --ignore-checksum .*--s3-disable-checksum .*--s3-upload-cutoff 4Gi' "$log" \
    || die "local upload did not use immutable size-only multipart flags"
grep -q 'copyto mock:blockzilla-network-format-benchmark-staging-v1/.* mock:blockzilla-network-format-benchmark-v1/.*--server-side-across-configs .*--immutable --size-only --ignore-checksum .*--s3-copy-cutoff 4Gi' "$log" \
    || die "promotion did not use same-remote immutable multipart copy flags"
last_stage_target=$(awk -v bucket="mock:blockzilla-network-format-benchmark-staging-v1" '
    $1 == "copyto" && index($3, bucket "/compact-v2/releases/e900-test-r1/") == 1 { value = $3 }
    END { print value }
' "$log")
[ "$last_stage_target" = "mock:blockzilla-network-format-benchmark-staging-v1/compact-v2/releases/$release/control.bin" ] \
    || die "staging control object was not last"
last_serving_target=$(awk -v bucket="mock:blockzilla-network-format-benchmark-v1" '
    $1 == "copyto" && index($3, bucket "/") == 1 { value = $3 }
    END { print value }
' "$log")
[ "$last_serving_target" = "mock:blockzilla-network-format-benchmark-v1/compact-v2/releases/$release/control.bin" ] \
    || die "serving control object was not last"

# An interrupted object stops the run. Resume keeps completed objects and
# uploads only the absent suffix.
: >"$log"
release=e900-resume-r1
make_release "$release"
state=$test_root/state-resume
export MOCK_RCLONE_FAIL_DEST="mock:blockzilla-network-format-benchmark-staging-v1/compact-v2/releases/$release/control.bin"
if run_publisher "$release" "$state" stage >/dev/null 2>&1; then
    die "injected staging failure was accepted"
fi
unset MOCK_RCLONE_FAIL_DEST
run_publisher "$release" "$state" stage --resume >/dev/null
payload_uploads=$(grep -c "copyto $test_root/$release/payload.bin mock:blockzilla-network-format-benchmark-staging-v1/compact-v2/releases/$release/payload.bin" "$log")
[ "$payload_uploads" -eq 1 ] || die "resume uploaded a completed payload again"
rm -f "$remote_root/.fail-once"

# A fresh run refuses an unexpected key before it acquires the remote lock.
release=e900-unexpected-r1
make_release "$release"
state=$test_root/state-unexpected
unexpected=$remote_root/blockzilla-network-format-benchmark-v1/compact-v2/releases/$release/unexpected.bin
mkdir -p "$(dirname "$unexpected")"
printf 'x' >"$unexpected"
if run_publisher "$release" "$state" stage >/dev/null 2>&1; then
    die "unexpected serving object was accepted"
fi
[ ! -e "$remote_root/blockzilla-network-format-benchmark-staging-v1/_blockzilla-publisher-locks/$release.lock" ] \
    || die "fresh preflight acquired a lock after it found an unexpected object"

# Resume refuses a wrong-size object. It never replaces the object.
release=e900-wrong-size-r1
make_release "$release"
state=$test_root/state-wrong-size
export MOCK_RCLONE_FAIL_DEST="mock:blockzilla-network-format-benchmark-staging-v1/compact-v2/releases/$release/control.bin"
if run_publisher "$release" "$state" stage >/dev/null 2>&1; then
    die "second injected staging failure was accepted"
fi
unset MOCK_RCLONE_FAIL_DEST
wrong=$remote_root/blockzilla-network-format-benchmark-staging-v1/compact-v2/releases/$release/payload.bin
printf 'wrong-size' >"$wrong"
if run_publisher "$release" "$state" stage --resume >/dev/null 2>&1; then
    die "wrong-size staged object was accepted"
fi
[ "$(wc -c <"$wrong" | tr -d '[:space:]')" -eq 10 ] \
    || die "wrong-size object was replaced"

# A control object cannot survive with a missing payload on resume.
release=e900-order-r1
make_release "$release"
state=$test_root/state-order
run_publisher "$release" "$state" stage >/dev/null
rm "$remote_root/blockzilla-network-format-benchmark-staging-v1/compact-v2/releases/$release/payload.bin"
if run_publisher "$release" "$state" stage --resume >/dev/null 2>&1; then
    die "control-before-payload state was accepted"
fi

# Old manifests and hash sidecars cannot enter a new publication inventory.
forbidden_index=1
for forbidden_name in archive-v2-generation.json benchmark-manifest.json payload.sha256; do
    release=e900-forbidden-$forbidden_index-r1
    make_release "$release"
    printf 'bad' >"$test_root/$release/$forbidden_name"
    printf 'completion\tlocal\t%s\tcompact-v2/releases/%s/%s\t3\n' \
        "$test_root/$release/$forbidden_name" "$release" "$forbidden_name" \
        >>"$test_root/$release/inventory.tsv"
    state=$test_root/state-forbidden-$forbidden_index
    if run_publisher "$release" "$state" stage >/dev/null 2>&1; then
        die "publisher accepted forbidden object $forbidden_name"
    fi
    [ ! -e "$remote_root/blockzilla-network-format-benchmark-staging-v1/_blockzilla-publisher-locks/$release.lock" ] \
        || die "publisher acquired a remote lock for forbidden object $forbidden_name"
    forbidden_index=$((forbidden_index + 1))
done

echo "staged R2 publisher tests passed"
