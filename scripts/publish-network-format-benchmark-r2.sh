#!/bin/sh
# Publish one immutable benchmark release through the private R2 staging bucket.
# The inventory is the only object allowlist. Payload hashes and body readback
# are intentionally outside this tool's acceptance contract.

set -eu
umask 077

program=${0##*/}
staging_bucket=blockzilla-network-format-benchmark-staging-v1
serving_bucket=blockzilla-network-format-benchmark-v1
lock_root=_blockzilla-publisher-locks
tab=$(printf '\t')

die() {
    echo "$program: $*" >&2
    exit 1
}

usage() {
    cat >&2 <<EOF
Usage:
  $program \\
    --inventory /absolute/path/release.tsv \\
    --state-dir /absolute/path/publisher-state \\
    --release-id IMMUTABLE_RELEASE_ID \\
    --scope VERSIONED_PREFIX [--scope VERSIONED_PREFIX ...] \\
    --mode stage|promote [--resume] [--rclone-remote NAME]

The fixed private staging bucket is $staging_bucket.
The fixed serving bucket is $serving_bucket.

Inventory columns, separated by one tab:
  role  source_kind  source  target_key  bytes

role is payload, control, or optional completion. source_kind is local or
staged-copy. A staged-copy source is an exact key in the staging bucket.
Rows must already be in payload, control, completion order. A release with
zero completion rows is complete after all payload and control rows exist.
EOF
    exit 2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 \
        || die "required command is not available: $1"
}

canonical_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

require_absolute_file() {
    raf_name=$1
    raf_path=$2
    case "$raf_path" in
        /*) ;;
        *) die "$raf_name must be an absolute path: $raf_path" ;;
    esac
    case "$raf_path" in
        *"$tab"*|*'
'*) die "$raf_name must not contain a tab or line break" ;;
    esac
    [ -f "$raf_path" ] && [ ! -L "$raf_path" ] \
        || die "$raf_name is not one real file: $raf_path"
    raf_parent=$(dirname "$raf_path")
    raf_base=$(basename "$raf_path")
    raf_parent_real=$(canonical_directory "$raf_parent") \
        || die "cannot canonicalize $raf_name parent: $raf_parent"
    if [ "$raf_parent_real" = / ]; then
        raf_expected=/$raf_base
    else
        raf_expected=$raf_parent_real/$raf_base
    fi
    [ "$raf_path" = "$raf_expected" ] \
        || die "$raf_name path must already be canonical: $raf_expected"
}

require_safe_key() {
    rsk_name=$1
    rsk_key=$2
    case "$rsk_key" in
        ''|/*|*/|*//*|*'..'*|*\\*|*[!A-Za-z0-9._/-]*)
            die "$rsk_name is not a safe R2 key: $rsk_key"
            ;;
    esac
}

file_size() {
    fs_path=$1
    if fs_bytes=$(stat -f %z "$fs_path" 2>/dev/null); then
        printf '%s\n' "$fs_bytes"
    elif fs_bytes=$(stat -c %s "$fs_path" 2>/dev/null); then
        printf '%s\n' "$fs_bytes"
    else
        return 1
    fi
}

write_marker_once() {
    wmo_path=$1
    wmo_text=$2
    if [ -e "$wmo_path" ] || [ -L "$wmo_path" ]; then
        [ -f "$wmo_path" ] && [ ! -L "$wmo_path" ] \
            || die "state marker is not one real file: $wmo_path"
        [ "$(sed -n '1p' "$wmo_path")" = "$wmo_text" ] \
            || die "state marker has unexpected content: $wmo_path"
        [ "$(wc -l <"$wmo_path" | tr -d '[:space:]')" -eq 1 ] \
            || die "state marker has extra content: $wmo_path"
        return
    fi
    (
        set -C
        printf '%s\n' "$wmo_text" >"$wmo_path"
    ) || die "cannot create state marker without replacement: $wmo_path"
    chmod 400 "$wmo_path" || die "cannot make state marker read-only: $wmo_path"
}

inventory=
state_dir=
release_id=
mode=
resume=0
rclone_remote=r2
scope_values=

while [ "$#" -gt 0 ]; do
    case "$1" in
        --inventory|--state-dir|--release-id|--scope|--mode|--rclone-remote)
            [ "$#" -ge 2 ] || usage
            option=$1
            value=$2
            shift 2
            case "$option" in
                --inventory) [ -z "$inventory" ] || usage; inventory=$value ;;
                --state-dir) [ -z "$state_dir" ] || usage; state_dir=$value ;;
                --release-id) [ -z "$release_id" ] || usage; release_id=$value ;;
                --mode) [ -z "$mode" ] || usage; mode=$value ;;
                --rclone-remote) rclone_remote=$value ;;
                --scope)
                    if [ -z "$scope_values" ]; then
                        scope_values=$value
                    else
                        scope_values=$scope_values'
'$value
                    fi
                    ;;
            esac
            ;;
        --resume)
            resume=1
            shift
            ;;
        -h|--help|help) usage ;;
        *) usage ;;
    esac
done

[ -n "$inventory" ] && [ -n "$state_dir" ] && [ -n "$release_id" ] \
    && [ -n "$scope_values" ] && [ -n "$mode" ] || usage
case "$mode" in stage|promote) ;; *) usage ;; esac
[ "$mode" != promote ] || [ "$resume" -eq 1 ] \
    || die "promote mode requires --resume and a validated staged state"
case "$release_id" in
    ''|.|..|*[!a-z0-9._-]*) die "release ID must use lowercase letters, digits, dot, underscore, or hyphen" ;;
esac
[ "$(printf '%s' "$release_id" | wc -c | tr -d '[:space:]')" -le 96 ] \
    || die "release ID is longer than 96 bytes"
case "$rclone_remote" in
    ''|*[!A-Za-z0-9._-]*) die "rclone remote name is invalid: $rclone_remote" ;;
esac

for required in awk basename cat chmod cmp cp dirname flock mktemp mkdir od rclone rm sed sort stat tr wc; do
    require_command "$required"
done
rclone version >/dev/null 2>&1 || die "rclone version check failed"
require_absolute_file INVENTORY "$inventory"

case "$state_dir" in
    /*) ;;
    *) die "STATE_DIR must be an absolute path: $state_dir" ;;
esac
case "$state_dir" in *"$tab"*|*'
'*) die "STATE_DIR must not contain a tab or line break" ;; esac
state_parent=$(dirname "$state_dir")
state_base=$(basename "$state_dir")
[ -d "$state_parent" ] && [ ! -L "$state_parent" ] \
    || die "STATE_DIR parent is not one real directory: $state_parent"
state_parent_real=$(canonical_directory "$state_parent") \
    || die "cannot canonicalize STATE_DIR parent: $state_parent"
if [ "$state_parent_real" = / ]; then
    expected_state_dir=/$state_base
else
    expected_state_dir=$state_parent_real/$state_base
fi
[ "$state_dir" = "$expected_state_dir" ] \
    || die "STATE_DIR must already be canonical: $expected_state_dir"

if [ "$resume" -eq 0 ]; then
    [ ! -e "$state_dir" ] && [ ! -L "$state_dir" ] \
        || die "new STATE_DIR already exists; use a new path or review it before --resume: $state_dir"
    mkdir "$state_dir" || die "cannot create STATE_DIR: $state_dir"
    chmod 700 "$state_dir" || die "cannot set STATE_DIR mode 0700"
else
    [ -d "$state_dir" ] && [ ! -L "$state_dir" ] \
        || die "resume STATE_DIR is not one real directory: $state_dir"
    [ "$(canonical_directory "$state_dir")" = "$state_dir" ] \
        || die "resume STATE_DIR is not canonical: $state_dir"
fi

local_lock=$state_dir/publisher.lock
[ ! -L "$local_lock" ] || die "local publisher lock is a symbolic link: $local_lock"
exec 9>"$local_lock" || die "cannot open local publisher lock: $local_lock"
flock -n 9 || die "another local publisher holds the state lock: $local_lock"
chmod 600 "$local_lock" || die "cannot set local publisher lock mode"

stored_inventory=$state_dir/inventory.tsv
stored_scopes=$state_dir/scopes.txt
expected=$state_dir/expected.tsv
owner=$state_dir/remote-lock-owner.tsv
stage_complete=$state_dir/stage.complete
publish_complete=$state_dir/publish.complete

scope_candidate=$(mktemp "$state_dir/.scopes.XXXXXX") \
    || die "cannot create temporary scope file"
expected_candidate=$(mktemp "$state_dir/.expected.XXXXXX") \
    || die "cannot create temporary expected inventory"
snapshot=$(mktemp "$state_dir/.snapshot.XXXXXX") \
    || die "cannot create temporary remote snapshot"
snapshot_raw=$(mktemp "$state_dir/.snapshot-raw.XXXXXX") \
    || die "cannot create temporary remote listing"
lock_observed=$(mktemp "$state_dir/.lock-observed.XXXXXX") \
    || die "cannot create temporary lock observation"
cleanup_temporaries() {
    rm -f "$scope_candidate" "$expected_candidate" "$snapshot" "$snapshot_raw" "$lock_observed"
}
trap 'cleanup_temporaries' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

printf '%s\n' "$scope_values" | sort >"$scope_candidate" \
    || die "cannot sort scopes"
awk '
    NF != 1 || $0 == "" || $0 ~ /^\// || $0 ~ /\/$/ || $0 ~ /\/\// ||
        $0 ~ /\.\./ || $0 ~ /\\/ || $0 ~ /[^A-Za-z0-9._\/-]/ { exit 1 }
    previous == $0 { exit 1 }
    { values[++count] = $0; previous = $0 }
    END {
        if (count == 0) exit 1
        for (i = 1; i <= count; i++)
            for (j = i + 1; j <= count; j++)
                if (index(values[j] "/", values[i] "/") == 1) exit 1
    }
' "$scope_candidate" || die "scopes must be unique, safe, and non-overlapping"
while IFS= read -r scope; do
    case "/$scope/" in
        *"/$release_id/"*) ;;
        *) die "scope does not contain the release ID as one full path segment: $scope" ;;
    esac
done <"$scope_candidate"

awk -F '\t' '
    function forbidden_publication_name(key, count, parts, base) {
        count = split(key, parts, "/")
        base = parts[count]
        return base == "archive-v2-generation.json" ||
            base == "benchmark-manifest.json" || base ~ /\.sha256$/
    }
    NR == 1 {
        if ($0 != "role\tsource_kind\tsource\ttarget_key\tbytes") exit 1
        next
    }
    NF != 5 { exit 1 }
    {
        if ($1 == "payload") rank = 1
        else if ($1 == "control") rank = 2
        else if ($1 == "completion") rank = 3
        else exit 1
        if (rank < prior_rank) exit 1
        prior_rank = rank
        if ($2 != "local" && $2 != "staged-copy") exit 1
        if ($2 == "local" && $3 !~ /^\//) exit 1
        if ($2 == "staged-copy" &&
            ($3 == "" || $3 ~ /^\// || $3 ~ /\/$/ || $3 ~ /\/\// ||
             $3 ~ /\.\./ || $3 ~ /\\/ || $3 ~ /[^A-Za-z0-9._\/-]/)) exit 1
        if ($4 == "" || $4 ~ /^\// || $4 ~ /\/$/ || $4 ~ /\/\// ||
            $4 ~ /\.\./ || $4 ~ /\\/ || $4 ~ /[^A-Za-z0-9._\/-]/) exit 1
        if (forbidden_publication_name($4)) exit 1
        if ($2 == "staged-copy" && forbidden_publication_name($3)) exit 1
        if ($5 !~ /^(0|[1-9][0-9]*)$/ || length($5) > 16) exit 1
        if (seen[$4]++) exit 1
        if ($3 == $4) exit 1
        target_line[$4] = NR
        source[NR] = $3
        kind[NR] = $2
        target[NR] = $4
        bytes[NR] = $5
        role[NR] = $1
        count++
        role_count[$1]++
    }
    END {
        if (count == 0 || role_count["payload"] == 0) exit 1
        for (line in source) {
            # mawk returns array indexes as strings here. Convert the row index
            # to a number before the dependency-order comparison.
            if (kind[line] == "staged-copy" && target_line[source[line]] &&
                target_line[source[line]] >= (line + 0)) exit 1
        }
        for (line = 2; line <= NR; line++)
            if (target[line] != "")
                print target[line] "\t" bytes[line] "\t" role[line] "\t" kind[line] "\t" source[line]
    }
' "$inventory" >"$expected_candidate" \
    || die "inventory is invalid; require exact ordered five-column TSV rows"

awk -F '\t' '
    NR == FNR { scopes[++count] = $0; next }
    {
        matches = 0
        for (i = 1; i <= count; i++)
            if (index($1, scopes[i] "/") == 1) matches++
        if (matches != 1) exit 1
    }
' "$scope_candidate" "$expected_candidate" \
    || die "each target key must be below exactly one declared scope"

while IFS="$tab" read -r key bytes role source_kind source; do
    if [ "$source_kind" = local ]; then
        require_absolute_file "local inventory source" "$source"
        actual_size=$(file_size "$source") || die "cannot get local source size: $source"
        [ "$actual_size" = "$bytes" ] \
            || die "local source size differs from inventory for $key: expected $bytes, got $actual_size"
    else
        require_safe_key "staged-copy source" "$source"
    fi
done <"$expected_candidate"

if [ "$resume" -eq 0 ]; then
    cp "$inventory" "$stored_inventory" || die "cannot store inventory"
    cp "$scope_candidate" "$stored_scopes" || die "cannot store scopes"
    cp "$expected_candidate" "$expected" || die "cannot store validated inventory"
    chmod 400 "$stored_inventory" "$stored_scopes" "$expected" \
        || die "cannot make stored release definition read-only"
    token=$(od -An -N16 -tx1 /dev/urandom | tr -d '[:space:]') \
        || die "cannot create remote lock token"
    [ "$(printf '%s' "$token" | wc -c | tr -d '[:space:]')" -eq 32 ] \
        || die "remote lock token generation failed"
    (
        set -C
        printf 'schema\tblockzilla-r2-staged-publisher-lock-v1\nrelease_id\t%s\ntoken\t%s\n' \
            "$release_id" "$token" >"$owner"
    ) || die "cannot create remote lock owner record"
    chmod 400 "$owner" || die "cannot make remote lock owner record read-only"
else
    for state_file in "$stored_inventory" "$stored_scopes" "$expected" "$owner"; do
        [ -f "$state_file" ] && [ ! -L "$state_file" ] \
            || die "resume state file is absent or unsafe: $state_file"
    done
    cmp -s "$inventory" "$stored_inventory" \
        || die "resume inventory differs from the admitted inventory"
    cmp -s "$scope_candidate" "$stored_scopes" \
        || die "resume scopes differ from the admitted scopes"
    cmp -s "$expected_candidate" "$expected" \
        || die "resume inventory validation differs from stored state"
fi

remote_path() {
    printf '%s:%s/%s\n' "$rclone_remote" "$1" "$2"
}

snapshot_bucket() {
    sb_bucket=$1
    : >"$snapshot"
    while IFS= read -r sb_scope; do
        : >"$snapshot_raw"
        rclone lsf --recursive --files-only --format sp --separator "$tab" \
            "$(remote_path "$sb_bucket" "$sb_scope")" >"$snapshot_raw" \
            || die "cannot list $sb_bucket/$sb_scope"
        while IFS="$tab" read -r sb_size sb_relative; do
            [ -n "$sb_size" ] || continue
            case "$sb_size" in
                ''|*[!0-9]*|0[0-9]*) die "remote listing has an invalid size" ;;
            esac
            require_safe_key "remote relative key" "$sb_relative"
            printf '%s/%s\t%s\n' "$sb_scope" "$sb_relative" "$sb_size" >>"$snapshot"
        done <"$snapshot_raw"
    done <"$stored_scopes"
    sort "$snapshot" -o "$snapshot" || die "cannot sort remote snapshot"
    awk -F '\t' 'seen[$1]++ { exit 1 }' "$snapshot" \
        || die "remote snapshot contains duplicate keys"
}

validate_remote_subset() {
    vrs_label=$1
    awk -F '\t' -v label="$vrs_label" '
        NR == FNR {
            expected[$1] = $2
            role[$1] = $3
            keys[++count] = $1
            next
        }
        NF != 2 || !($1 in expected) {
            print label ": unexpected remote object: " $1 > "/dev/stderr"
            bad = 1
            next
        }
        $2 != expected[$1] {
            print label ": wrong remote size for " $1 ": expected " expected[$1] ", got " $2 > "/dev/stderr"
            bad = 1
        }
        { present[$1] = 1; if (role[$1] == "control") has_control = 1; if (role[$1] == "completion") has_completion = 1 }
        END {
            if (has_control || has_completion)
                for (i = 1; i <= count; i++)
                    if (role[keys[i]] == "payload" && !present[keys[i]]) {
                        print label ": a control/completion exists before payload " keys[i] > "/dev/stderr"
                        bad = 1
                    }
            if (has_completion)
                for (i = 1; i <= count; i++)
                    if (role[keys[i]] == "control" && !present[keys[i]]) {
                        print label ": a completion exists before control " keys[i] > "/dev/stderr"
                        bad = 1
                    }
            if (bad) exit 1
        }
    ' "$expected" "$snapshot" || die "$vrs_label prefix does not match an allowed resumable subset"
}

require_remote_complete() {
    rrc_label=$1
    validate_remote_subset "$rrc_label"
    rrc_expected_count=$(wc -l <"$expected" | tr -d '[:space:]')
    rrc_actual_count=$(wc -l <"$snapshot" | tr -d '[:space:]')
    [ "$rrc_actual_count" = "$rrc_expected_count" ] \
        || die "$rrc_label is incomplete: expected $rrc_expected_count objects, got $rrc_actual_count"
}

snapshot_has_key() {
    awk -F '\t' -v wanted="$1" '$1 == wanted { found = 1 } END { exit !found }' "$snapshot"
}

exact_remote_size() {
    ers_bucket=$1
    ers_key=$2
    : >"$snapshot_raw"
    rclone lsf --files-only --format sp --separator "$tab" \
        "$(remote_path "$ers_bucket" "$ers_key")" >"$snapshot_raw" \
        || die "cannot inspect exact remote source: $ers_bucket/$ers_key"
    ers_size=$(awk -F '\t' '
        NF == 2 && $1 ~ /^(0|[1-9][0-9]*)$/ { value = $1; count++ }
        END { if (count != 1) exit 1; print value }
    ' "$snapshot_raw") || die "exact remote source is absent or ambiguous: $ers_bucket/$ers_key"
    printf '%s\n' "$ers_size"
}

lock_key=$lock_root/$release_id.lock
remote_lock_path=$(remote_path "$staging_bucket" "$lock_key")

remote_lock_exists() {
    : >"$snapshot_raw"
    rclone lsf --recursive --files-only --format sp --separator "$tab" \
        "$(remote_path "$staging_bucket" "$lock_root")" >"$snapshot_raw" \
        || die "cannot inspect remote publisher locks"
    awk -F '\t' -v wanted="$release_id.lock" '$2 == wanted { found++ } END { exit found == 1 ? 0 : 1 }' "$snapshot_raw"
}

verify_remote_lock() {
    remote_lock_exists || die "remote publisher lock is absent: $remote_lock_path"
    : >"$lock_observed"
    rclone copyto "$remote_lock_path" "$lock_observed" --size-only --no-update-modtime \
        || die "cannot read the remote publisher lock"
    cmp -s "$owner" "$lock_observed" \
        || die "remote publisher lock is owned by another run"
}

upload_flags='--immutable --size-only --ignore-checksum --no-update-modtime --s3-disable-checksum --s3-upload-cutoff 4Gi --s3-chunk-size 256Mi --s3-upload-concurrency 4 --retries 10 --low-level-retries 20'
copy_flags='--server-side-across-configs --immutable --size-only --ignore-checksum --no-update-modtime --s3-disable-checksum --s3-copy-cutoff 4Gi --s3-upload-concurrency 4 --retries 10 --low-level-retries 20'

if [ "$resume" -eq 0 ]; then
    remote_lock_exists && die "remote publisher lock already exists: $remote_lock_path"
    snapshot_bucket "$staging_bucket"
    [ ! -s "$snapshot" ] || die "fresh staging scopes are not empty"
    snapshot_bucket "$serving_bucket"
    [ ! -s "$snapshot" ] || die "fresh serving scopes are not empty"
    # shellcheck disable=SC2086
    rclone copyto "$owner" "$remote_lock_path" $upload_flags \
        || die "cannot acquire remote publisher lock"
elif ! remote_lock_exists; then
    # A process can stop after it creates local state but before it creates the
    # remote lock. Resume that narrow empty-prefix state without changing the
    # admitted owner record.
    snapshot_bucket "$staging_bucket"
    [ ! -s "$snapshot" ] || die "remote lock is absent but staging scopes are not empty"
    snapshot_bucket "$serving_bucket"
    [ ! -s "$snapshot" ] || die "remote lock is absent but serving scopes are not empty"
    # shellcheck disable=SC2086
    rclone copyto "$owner" "$remote_lock_path" $upload_flags \
        || die "cannot reacquire the absent remote publisher lock"
fi
verify_remote_lock

copy_stage_role() {
    csr_role=$1
    snapshot_bucket "$staging_bucket"
    validate_remote_subset staging
    while IFS="$tab" read -r csr_key csr_bytes csr_row_role csr_kind csr_source; do
        [ "$csr_row_role" = "$csr_role" ] || continue
        if snapshot_has_key "$csr_key"; then
            echo "staging resume: $csr_key ($csr_bytes bytes)"
            continue
        fi
        verify_remote_lock
        if [ "$csr_kind" = local ]; then
            require_absolute_file "local inventory source" "$csr_source"
            csr_before=$(file_size "$csr_source") || die "cannot get local source size: $csr_source"
            [ "$csr_before" = "$csr_bytes" ] \
                || die "local source size changed before upload: $csr_source"
            # shellcheck disable=SC2086
            rclone copyto "$csr_source" "$(remote_path "$staging_bucket" "$csr_key")" $upload_flags \
                || die "staging upload failed: $csr_key"
            csr_after=$(file_size "$csr_source") || die "cannot recheck local source size: $csr_source"
            [ "$csr_after" = "$csr_bytes" ] \
                || die "local source size changed during upload: $csr_source"
        else
            csr_source_size=$(exact_remote_size "$staging_bucket" "$csr_source")
            [ "$csr_source_size" = "$csr_bytes" ] \
                || die "staged-copy source has wrong size for $csr_key: expected $csr_bytes, got $csr_source_size"
            # shellcheck disable=SC2086
            rclone copyto "$(remote_path "$staging_bucket" "$csr_source")" \
                "$(remote_path "$staging_bucket" "$csr_key")" $copy_flags \
                || die "staging server-side copy failed: $csr_key"
        fi
        snapshot_bucket "$staging_bucket"
        validate_remote_subset staging
        snapshot_has_key "$csr_key" || die "staged object is absent after copy: $csr_key"
        echo "staged: $csr_key ($csr_bytes bytes)"
    done <"$expected"
}

promote_role() {
    pr_role=$1
    snapshot_bucket "$serving_bucket"
    validate_remote_subset serving
    while IFS="$tab" read -r pr_key pr_bytes pr_row_role pr_kind pr_source; do
        [ "$pr_row_role" = "$pr_role" ] || continue
        if snapshot_has_key "$pr_key"; then
            echo "serving resume: $pr_key ($pr_bytes bytes)"
            continue
        fi
        verify_remote_lock
        pr_stage_size=$(exact_remote_size "$staging_bucket" "$pr_key")
        [ "$pr_stage_size" = "$pr_bytes" ] \
            || die "staged promotion source has wrong size for $pr_key"
        # shellcheck disable=SC2086
        rclone copyto "$(remote_path "$staging_bucket" "$pr_key")" \
            "$(remote_path "$serving_bucket" "$pr_key")" $copy_flags \
            || die "server-side promotion failed: $pr_key"
        snapshot_bucket "$serving_bucket"
        validate_remote_subset serving
        snapshot_has_key "$pr_key" || die "serving object is absent after promotion: $pr_key"
        echo "promoted: $pr_key ($pr_bytes bytes)"
    done <"$expected"
}

if [ "$mode" = stage ]; then
    snapshot_bucket "$serving_bucket"
    validate_remote_subset serving
    if [ -s "$snapshot" ] && [ ! -e "$stage_complete" ]; then
        die "serving objects exist before this state records a complete staged release"
    fi
    copy_stage_role payload
    copy_stage_role control
    copy_stage_role completion
    snapshot_bucket "$staging_bucket"
    require_remote_complete staging
    write_marker_once "$stage_complete" "stage-complete:$release_id"
    echo "private staging complete: $release_id"
fi

if [ "$mode" = promote ]; then
    snapshot_bucket "$staging_bucket"
    require_remote_complete staging
    write_marker_once "$stage_complete" "stage-complete:$release_id"
    promote_role payload
    promote_role control
    promote_role completion
    snapshot_bucket "$serving_bucket"
    require_remote_complete serving
    write_marker_once "$publish_complete" "publish-complete:$release_id"
    echo "serving publication complete: $release_id"
fi
