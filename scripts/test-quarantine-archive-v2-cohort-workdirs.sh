#!/bin/sh
# Focused fixture for exact admission, metadata-only moves, and resume.

set -eu
umask 077

fail() {
    echo "archive-v2 workdir quarantine self-test: $*" >&2
    exit 1
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
runner=$script_dir/quarantine-archive-v2-cohort-workdirs.sh
test_root=$(mktemp -d "${TMPDIR:-/tmp}/archive-v2-quarantine-test.XXXXXX") \
    || fail "cannot create test root"
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

for required in awk cmp find jq mktemp sha256sum stat; do
    command -v "$required" >/dev/null 2>&1 || fail "$required is required"
done

shim=$test_root/shim
mkdir "$shim"
cat >"$shim/systemctl" <<'SYSTEMCTL'
#!/bin/sh
case " $* " in
    *" is-active "*) exit 3 ;;
    *" --state=running "*) exit 0 ;;
esac
exit 0
SYSTEMCTL
chmod 700 "$shim/systemctl"

file_dev_inode() {
    if stat -c '%d %i' -- "$1" >/dev/null 2>&1; then
        stat -c '%d %i' -- "$1"
    else
        stat -f '%d %i' "$1"
    fi
}

run_quarantine() {
    test_shell=$1
    archive=$2
    handoff=$3
    plan=$4
    evidence=$5
    rows=$(wc -l <"$plan" | tr -d '[:space:]')
    bytes=$(wc -c <"$plan" | tr -d '[:space:]')
    sha=$(sha256sum "$plan" | awk '{print $1}')
    PATH="$shim:$PATH" \
    BLOCKZILLA_QUARANTINE_TEST_ONLY=1 \
    BLOCKZILLA_QUARANTINE_EXPECTED_ROWS=$rows \
    BLOCKZILLA_QUARANTINE_EXPECTED_BYTES=$bytes \
    BLOCKZILLA_QUARANTINE_EXPECTED_SHA256=$sha \
    BLOCKZILLA_QUARANTINE_EXPECTED_COHORT_ROWS=2 \
        "$test_shell" "$runner" "$archive" "$handoff" "$plan" "$evidence"
}

shells=sh
if command -v dash >/dev/null 2>&1; then shells='sh dash'; fi
for test_shell in $shells; do
    "$test_shell" -n "$runner" || fail "$test_shell syntax check failed"
    fixture=$test_root/$test_shell
    archive=$fixture/archive
    handoff=$fixture/handoff
    plan=$fixture/expected.tsv
    evidence=.review-evidence
    mkdir -p "$archive/epoch-7/work-a/nested" "$archive/epoch-8/work-b" \
        "$handoff/fast-in-place-candidate"
    printf '7\n8\n' >"$handoff/fast-in-place-candidate/all-legacy-pre-epochs.txt"
    printf 'core-seven\n' >"$archive/epoch-7/archive-v2-blocks.zstd"
    printf 'core-eight\n' >"$archive/epoch-8/archive-v2-blocks.zstd"
    printf 'payload\n' >"$archive/epoch-7/work-a/nested/data"
    ln "$archive/epoch-7/work-a/nested/data" "$archive/epoch-7/work-a/hard-link"
    ln -s nested/data "$archive/epoch-7/work-a/data-link"
    printf 'second\n' >"$archive/epoch-8/work-b/data"
    printf '7\twork-a\n8\twork-b\n' >"$plan"

    before_a=$(file_dev_inode "$archive/epoch-7/work-a")
    before_b=$(file_dev_inode "$archive/epoch-8/work-b")
    run_quarantine "$test_shell" "$archive" "$handoff" "$plan" "$evidence" \
        >"$fixture/first.out" || fail "$test_shell first run failed"
    root=$archive/$evidence
    [ ! -e "$archive/epoch-7/work-a" ] && [ ! -L "$archive/epoch-7/work-a" ] \
        || fail "$test_shell left the first source"
    [ ! -e "$archive/epoch-8/work-b" ] && [ ! -L "$archive/epoch-8/work-b" ] \
        || fail "$test_shell left the second source"
    [ "$(file_dev_inode "$root/epoch-7/work-a")" = "$before_a" ] \
        || fail "$test_shell changed the first directory identity"
    [ "$(file_dev_inode "$root/epoch-8/work-b")" = "$before_b" ] \
        || fail "$test_shell changed the second directory identity"
    [ "$(cat "$root/epoch-7/work-a/nested/data")" = payload ] \
        || fail "$test_shell changed file data"
    [ "$(readlink "$root/epoch-7/work-a/data-link")" = nested/data ] \
        || fail "$test_shell changed the raw link target"
    cmp -s "$plan" "$root/expected.tsv" || fail "$test_shell changed the exact plan"
    cmp -s "$root/stat-tree-before.tsv" "$root/stat-tree-after.tsv" \
        || fail "$test_shell stat tree differs"
    cmp -s "$root/regular-before.tsv" "$root/regular-after.tsv" \
        || fail "$test_shell regular inventory differs"
    jq -e '
        .kind == "archive-v2-workdir-quarantine-intent" and
        .same_device_rename == true and .content_hashed == false and
        .later_review_required == true and .canonical == false
    ' "$root/intent.json" >/dev/null || fail "$test_shell intent is invalid"
    jq -e '
        .kind == "archive-v2-workdir-quarantine-complete" and .entry_count == 2 and
        .same_device_rename == true and .content_hashed == false and
        .later_review_required == true and .canonical == false
    ' "$root/complete.json" >/dev/null || fail "$test_shell completion is invalid"

    run_quarantine "$test_shell" "$archive" "$handoff" "$plan" "$evidence" \
        >"$fixture/resume.out" || fail "$test_shell resume failed"
    cmp -s "$fixture/first.out" "$fixture/resume.out" \
        || fail "$test_shell resume output changed"

    reject=$test_root/reject-$test_shell
    reject_archive=$reject/archive
    reject_handoff=$reject/handoff
    reject_plan=$reject/expected.tsv
    mkdir -p "$reject_archive/epoch-7/work-a" "$reject_archive/epoch-7/unplanned" \
        "$reject_archive/epoch-8/work-b" "$reject_handoff/fast-in-place-candidate"
    printf '7\n8\n' >"$reject_handoff/fast-in-place-candidate/all-legacy-pre-epochs.txt"
    printf '7\twork-a\n8\twork-b\n' >"$reject_plan"
    if run_quarantine "$test_shell" "$reject_archive" "$reject_handoff" \
        "$reject_plan" .review-evidence >"$reject/out" 2>"$reject/err"; then
        fail "$test_shell accepted an unplanned non-regular entry"
    fi
    [ -d "$reject_archive/epoch-7/work-a" ] \
        && [ -d "$reject_archive/epoch-8/work-b" ] \
        || fail "$test_shell moved data after a failed preflight"
done

echo 'archive-v2 workdir quarantine self-test: PASS'
