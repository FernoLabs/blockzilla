#!/bin/sh
# Move one reviewed set of direct epoch work directories into private evidence.

set -eu
umask 077
LC_ALL=C
export LC_ALL

q_die() {
    echo "archive-v2 workdir quarantine: $*" >&2
    exit 1
}

q_usage() {
    echo "usage: $0 ARCHIVE_ROOT HANDOFF_STATE_ROOT EXPECTED_TSV EVIDENCE_BASENAME" >&2
    exit 2
}

q_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

q_sha256() {
    sha256sum "$1" | awk '{print $1}'
}

q_rows() {
    wc -l <"$1" | tr -d '[:space:]'
}

q_bytes() {
    wc -c <"$1" | tr -d '[:space:]'
}

q_stat_fields() {
    if [ "$stat_style" = gnu ]; then
        stat -c '%F\t%d\t%i\t%a\t%u\t%g\t%h\t%s\t%Y' -- "$1"
    else
        stat -f '%HT\t%d\t%i\t%Lp\t%u\t%g\t%l\t%z\t%m' "$1"
    fi
}

q_stat_dev() {
    if [ "$stat_style" = gnu ]; then stat -c %d -- "$1"; else stat -f %d "$1"; fi
}

q_stat_inode() {
    if [ "$stat_style" = gnu ]; then stat -c %i -- "$1"; else stat -f %i "$1"; fi
}

q_publish() {
    q_from=$1
    q_to=$2
    q_build=$q_to.building
    if [ -e "$q_to" ] || [ -L "$q_to" ]; then
        [ -f "$q_to" ] && [ ! -L "$q_to" ] && cmp -s "$q_from" "$q_to" \
            || q_die "published evidence changed: $q_to"
        if [ -e "$q_build" ] || [ -L "$q_build" ]; then
            [ -f "$q_build" ] && [ ! -L "$q_build" ] \
                && cmp -s "$q_from" "$q_build" \
                || q_die "unfinished evidence changed: $q_build"
            rm "$q_build" || q_die "cannot remove verified unfinished evidence: $q_build"
        fi
        return
    fi
    if [ -e "$q_build" ] || [ -L "$q_build" ]; then
        [ -f "$q_build" ] && [ ! -L "$q_build" ] \
            && cmp -s "$q_from" "$q_build" \
            || q_die "unfinished evidence changed: $q_build"
    else
        cp "$q_from" "$q_build" || q_die "cannot stage evidence: $q_to"
    fi
    chmod 400 "$q_build" || q_die "cannot protect evidence candidate: $q_build"
    ln "$q_build" "$q_to" || q_die "cannot publish evidence without replacement: $q_to"
    rm "$q_build" || q_die "cannot remove published evidence candidate: $q_build"
}

q_validate_line_safe_tree() {
    q_tree=$1
    find "$q_tree" -xdev -exec sh -c '
        tab=$(printf "\t")
        nl="
"
        for node do
            case "$node" in *"$tab"*|*"$nl"*) exit 90 ;; esac
            if [ -L "$node" ]; then
                target=$(readlink "$node") || exit 91
                case "$target" in *"$tab"*|*"$nl"*) exit 92 ;; esac
            fi
        done
    ' sh {} + || q_die "tree has an unsafe name or link target: $q_tree"
}

q_emit_tree() {
    q_epoch=$1
    q_name=$2
    q_tree=$3
    q_raw=$4
    q_nodes=$work/nodes
    q_validate_line_safe_tree "$q_tree"
    find "$q_tree" -xdev -print >"$q_nodes.unsorted" \
        || q_die "cannot inventory tree: $q_tree"
    sort "$q_nodes.unsorted" >"$q_nodes" || q_die "cannot sort tree inventory"
    while IFS= read -r q_node || [ -n "$q_node" ]; do
        case "$q_node" in
            "$q_tree") q_rel= ;;
            "$q_tree"/*) q_rel=${q_node#"$q_tree"/} ;;
            *) q_die "tree inventory escaped its root: $q_node" ;;
        esac
        q_key=epoch-$q_epoch/$q_name
        [ -z "$q_rel" ] || q_key=$q_key/$q_rel
        q_stat=$(q_stat_fields "$q_node") \
            || q_die "cannot stat tree node: $q_node"
        q_target=
        if [ -L "$q_node" ]; then
            q_target=$(readlink -- "$q_node") || q_die "cannot read link: $q_node"
        fi
        printf '%s\t%s\t%s\n' "$q_key" "$q_stat" "$q_target" >>"$q_raw" \
            || q_die "cannot write tree inventory"
    done <"$q_nodes"
}

q_collect_tree_inventory() {
    q_output=$1
    q_raw=$work/tree.raw
    : >"$q_raw" || q_die "cannot create tree inventory"
    while IFS="$tab" read -r q_epoch q_name q_extra \
        || [ -n "$q_epoch$q_name$q_extra" ]; do
        [ -z "$q_extra" ] || q_die "plan row has too many fields"
        q_source=$archive_root/epoch-$q_epoch/$q_name
        q_destination=$evidence_root/epoch-$q_epoch/$q_name
        if [ -d "$q_source" ] && [ ! -L "$q_source" ] \
            && [ ! -e "$q_destination" ] && [ ! -L "$q_destination" ]; then
            q_tree=$q_source
        elif [ ! -e "$q_source" ] && [ ! -L "$q_source" ] \
            && [ -d "$q_destination" ] && [ ! -L "$q_destination" ]; then
            q_tree=$q_destination
        else
            q_die "planned entry is not in exactly one valid location: epoch-$q_epoch/$q_name"
        fi
        q_emit_tree "$q_epoch" "$q_name" "$q_tree" "$q_raw"
    done <"$plan_snapshot"
    sort "$q_raw" >"$q_output" || q_die "cannot sort normalized tree inventory"
}

q_collect_regular_inventory() {
    q_output=$1
    q_raw=$work/regular.raw
    : >"$q_raw" || q_die "cannot create regular inventory"
    q_previous=
    while IFS="$tab" read -r q_epoch q_name q_extra \
        || [ -n "$q_epoch$q_name$q_extra" ]; do
        [ "$q_epoch" = "$q_previous" ] && continue
        q_previous=$q_epoch
        q_epoch_dir=$archive_root/epoch-$q_epoch
        [ -d "$q_epoch_dir" ] && [ ! -L "$q_epoch_dir" ] \
            || q_die "epoch directory is invalid: epoch-$q_epoch"
        find "$q_epoch_dir" ! -path "$q_epoch_dir" -prune -type f -print \
            >"$work/regular-nodes.unsorted" || q_die "cannot list regular files"
        sort "$work/regular-nodes.unsorted" >"$work/regular-nodes" \
            || q_die "cannot sort regular files"
        while IFS= read -r q_node || [ -n "$q_node" ]; do
            q_base=${q_node##*/}
            case "$q_base" in *"$tab"*|*'
'*) q_die "regular file name is not line-safe: $q_node" ;; esac
            q_stat=$(q_stat_fields "$q_node") \
                || q_die "cannot stat regular file: $q_node"
            printf 'epoch-%s/%s\t%s\t\n' "$q_epoch" "$q_base" "$q_stat" \
                >>"$q_raw" || q_die "cannot write regular inventory"
        done <"$work/regular-nodes"
    done <"$plan_snapshot"
    sort "$q_raw" >"$q_output" || q_die "cannot sort regular inventory"
}

q_collect_unified_plan() {
    q_output=$1
    q_raw=$work/unified-plan.raw
    : >"$q_raw" || q_die "cannot create unified plan inventory"
    while IFS= read -r q_epoch || [ -n "$q_epoch" ]; do
        q_epoch_dir=$archive_root/epoch-$q_epoch
        find "$q_epoch_dir" ! -path "$q_epoch_dir" -prune ! -type f -print \
            >"$work/source-nonregular" || q_die "cannot list epoch entries"
        while IFS= read -r q_path || [ -n "$q_path" ]; do
            q_base=${q_path##*/}
            printf '%s\t%s\n' "$q_epoch" "$q_base" >>"$q_raw"
        done <"$work/source-nonregular"
    done <"$cohort_list"
    q_previous=
    while IFS="$tab" read -r q_epoch q_name q_extra \
        || [ -n "$q_epoch$q_name$q_extra" ]; do
        [ "$q_epoch" = "$q_previous" ] && continue
        q_previous=$q_epoch
        q_evidence_epoch=$evidence_root/epoch-$q_epoch
        if [ -e "$q_evidence_epoch" ] || [ -L "$q_evidence_epoch" ]; then
            [ -d "$q_evidence_epoch" ] && [ ! -L "$q_evidence_epoch" ] \
                || q_die "evidence epoch path is invalid: $q_evidence_epoch"
            find "$q_evidence_epoch" ! -path "$q_evidence_epoch" -prune ! -type f -print \
                >"$work/evidence-nonregular" || q_die "cannot list evidence entries"
            while IFS= read -r q_path || [ -n "$q_path" ]; do
                q_base=${q_path##*/}
                printf '%s\t%s\n' "$q_epoch" "$q_base" >>"$q_raw"
            done <"$work/evidence-nonregular"
        fi
    done <"$plan_snapshot"
    sort "$q_raw" >"$q_output" || q_die "cannot sort unified plan inventory"
}

[ "$#" -eq 4 ] || q_usage
archive_root=$1
handoff_state_root=$2
expected_tsv=$3
evidence_name=$4

for q_command in awk cmp cp date find jq ln mkdir mktemp mv ps readlink rm rmdir \
    sha256sum sort stat systemctl tr wc chmod; do
    command -v "$q_command" >/dev/null 2>&1 \
        || q_die "required command is absent: $q_command"
done

case "$archive_root" in /*) ;; *) q_die "ARCHIVE_ROOT must be absolute" ;; esac
case "$archive_root" in *'
'*|*"$(printf '\t')"*) q_die "ARCHIVE_ROOT is not line-safe" ;; esac
[ -d "$archive_root" ] && [ ! -L "$archive_root" ] \
    || q_die "ARCHIVE_ROOT must be one real directory"
archive_root=$(q_directory "$archive_root") || q_die "cannot resolve ARCHIVE_ROOT"
case "$handoff_state_root" in /*) ;; *) q_die "HANDOFF_STATE_ROOT must be absolute" ;; esac
[ -d "$handoff_state_root" ] && [ ! -L "$handoff_state_root" ] \
    || q_die "HANDOFF_STATE_ROOT must be one real directory"
handoff_state_root=$(q_directory "$handoff_state_root") \
    || q_die "cannot resolve HANDOFF_STATE_ROOT"
[ -f "$expected_tsv" ] && [ ! -L "$expected_tsv" ] \
    || q_die "EXPECTED_TSV must be one regular file"
case "$evidence_name" in
    .[A-Za-z0-9][A-Za-z0-9._-]*) ;;
    *) q_die "EVIDENCE_BASENAME must be one private safe basename" ;;
esac

tab=$(printf '\t')
if [ "${BLOCKZILLA_QUARANTINE_TEST_ONLY:-0}" = 1 ]; then
    expected_plan_rows=${BLOCKZILLA_QUARANTINE_EXPECTED_ROWS:?}
    expected_plan_bytes=${BLOCKZILLA_QUARANTINE_EXPECTED_BYTES:?}
    expected_plan_sha256=${BLOCKZILLA_QUARANTINE_EXPECTED_SHA256:?}
    expected_cohort_rows=${BLOCKZILLA_QUARANTINE_EXPECTED_COHORT_ROWS:?}
else
    expected_plan_rows=51
    expected_plan_bytes=2161
    expected_plan_sha256=6c565e241d5c1518fc7a12683312dd83b3a4a59dd4e043708449a3dd159cca76
    expected_cohort_rows=211
fi
case "$expected_plan_rows:$expected_plan_bytes:$expected_plan_sha256" in
    *[!0-9a-f:]*) q_die "expected plan binding is invalid" ;;
esac
[ "$(q_rows "$expected_tsv")" = "$expected_plan_rows" ] \
    || q_die "plan row count differs from the reviewed count"
[ "$(q_bytes "$expected_tsv")" = "$expected_plan_bytes" ] \
    || q_die "plan byte count differs from the reviewed count"
[ "$(q_sha256 "$expected_tsv")" = "$expected_plan_sha256" ] \
    || q_die "plan SHA-256 differs from the reviewed digest"
awk -F '\t' '
    NF != 2 || $1 !~ /^(0|[1-9][0-9]*)$/ ||
    $2 !~ /^[A-Za-z0-9][A-Za-z0-9._-]*$/ || seen[$0]++ { exit 1 }
' "$expected_tsv" || q_die "plan syntax or uniqueness is invalid"
sort -c "$expected_tsv" >/dev/null 2>&1 || q_die "plan is not bytewise sorted"

cohort_list=$handoff_state_root/fast-in-place-candidate/all-legacy-pre-epochs.txt
[ -f "$cohort_list" ] && [ ! -L "$cohort_list" ] \
    || q_die "exact LegacyPre cohort list is absent"
[ "$(q_rows "$cohort_list")" = "$expected_cohort_rows" ] \
    || q_die "LegacyPre cohort row count differs"
awk '
    NF != 1 || $0 !~ /^(0|[1-9][0-9]*)$/ || seen[$0]++ { exit 1 }
    previous != "" && ($0 + 0) <= (previous + 0) { exit 1 }
    { previous = $0 }
' "$cohort_list" || q_die "LegacyPre cohort list is invalid"

if systemctl --user is-active --quiet blockzilla-archive.service 2>/dev/null; then
    q_die "blockzilla-archive.service is active"
fi
q_services=$(systemctl --user --no-pager --plain --type=service --state=running 2>/dev/null) \
    || q_die "cannot inspect running user services"
case "$q_services" in *pre-post*) q_die "a pre-to-post service is active" ;; esac
q_converter_pids=$(ps -C archive-v2-pre-to-post -o pid= 2>/dev/null || :)
[ -z "$(printf '%s' "$q_converter_pids" | tr -d '[:space:]')" ] \
    || q_die "a pre-to-post converter process is active"

if mv --help 2>&1 | awk 'index($0, "-T") && index($0, "-n") { found=1 } END { exit !found }'; then
    move_style=gnu_no_replace
elif [ "${BLOCKZILLA_QUARANTINE_TEST_ONLY:-0}" = 1 ]; then
    move_style=test_portable
else
    q_die "mv must support -T and -n"
fi

[ ! -e "$archive_root/.archive-v2-pre-to-post-fast-candidate.lock" ] \
    && [ ! -L "$archive_root/.archive-v2-pre-to-post-fast-candidate.lock" ] \
    || q_die "fast candidate lock exists"
[ ! -e "$handoff_state_root/.archive-v2-pre-to-post-fast-canary-batch.lock" ] \
    && [ ! -L "$handoff_state_root/.archive-v2-pre-to-post-fast-canary-batch.lock" ] \
    || q_die "fast canary batch lock exists"

while IFS="$tab" read -r q_epoch q_name q_extra \
    || [ -n "$q_epoch$q_name$q_extra" ]; do
    for q_path in \
        "$archive_root/.epoch-$q_epoch.pre-to-post.staging" \
        "$archive_root/.epoch-$q_epoch.pre-to-post.backup" \
        "$archive_root/epoch-$q_epoch/archive-v2-pre-to-post.candidate.v1.json" \
        "$handoff_state_root/fast-canary-epoch-$q_epoch/result.json" \
        "$handoff_state_root/fast-in-place-candidate/results/epoch-$q_epoch.json"; do
        [ ! -e "$q_path" ] && [ ! -L "$q_path" ] \
            || q_die "affected epoch is not fresh: epoch-$q_epoch"
    done
done <"$expected_tsv"

evidence_root=$archive_root/$evidence_name
if stat -c %d -- "$archive_root" >/dev/null 2>&1; then stat_style=gnu; else stat_style=bsd; fi
archive_dev=$(q_stat_dev "$archive_root") || q_die "cannot stat ARCHIVE_ROOT"
lock=$archive_root/.archive-v2-cohort-workdir-quarantine.lock
lock_held=0
work=
q_cleanup() {
    q_status=$?
    trap - 0 1 2 3 15
    if [ -n "$work" ] && [ -d "$work" ]; then
        rm -f "$work"/* 2>/dev/null || :
        rmdir "$work" 2>/dev/null || :
    fi
    if [ "$lock_held" -eq 1 ]; then
        rmdir "$lock" 2>/dev/null || :
    fi
    exit "$q_status"
}
trap 'q_cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15
mkdir "$lock" 2>/dev/null || q_die "quarantine lock exists: $lock"
lock_held=1

if [ -e "$evidence_root" ] || [ -L "$evidence_root" ]; then
    [ -d "$evidence_root" ] && [ ! -L "$evidence_root" ] \
        || q_die "evidence root is invalid"
else
    mkdir -m 700 "$evidence_root" || q_die "cannot create evidence root"
fi
[ "$(q_stat_dev "$evidence_root")" = "$archive_dev" ] \
    || q_die "evidence root is on another device"
chmod 700 "$evidence_root" || q_die "cannot protect evidence root"
work=$(mktemp -d "${TMPDIR:-/tmp}/archive-v2-workdir-quarantine.XXXXXX") \
    || q_die "cannot create private work directory"

plan_snapshot=$evidence_root/expected.tsv
q_publish "$expected_tsv" "$plan_snapshot"

q_collect_unified_plan "$work/unified-plan.tsv"
cmp -s "$work/unified-plan.tsv" "$plan_snapshot" \
    || q_die "current direct non-regular entries differ from the exact plan"
q_collect_tree_inventory "$work/stat-tree-before.tsv"
q_collect_regular_inventory "$work/regular-before.tsv"
q_publish "$work/stat-tree-before.tsv" "$evidence_root/stat-tree-before.tsv"
q_publish "$work/regular-before.tsv" "$evidence_root/regular-before.tsv"

tree_rows=$(q_rows "$work/stat-tree-before.tsv")
tree_bytes=$(q_bytes "$work/stat-tree-before.tsv")
tree_sha256=$(q_sha256 "$work/stat-tree-before.tsv")
regular_rows=$(q_rows "$work/regular-before.tsv")
regular_bytes=$(q_bytes "$work/regular-before.tsv")
regular_sha256=$(q_sha256 "$work/regular-before.tsv")
intent=$evidence_root/intent.json
if [ -e "$intent" ] || [ -L "$intent" ]; then
    [ -f "$intent" ] && [ ! -L "$intent" ] || q_die "intent record is invalid"
    jq -e --arg archive "$archive_root" --arg evidence "$evidence_root" \
        --arg plan_sha "$expected_plan_sha256" --arg tree_sha "$tree_sha256" \
        --arg regular_sha "$regular_sha256" --argjson plan_rows "$expected_plan_rows" \
        --argjson plan_bytes "$expected_plan_bytes" --argjson tree_rows "$tree_rows" \
        --argjson tree_bytes "$tree_bytes" --argjson regular_rows "$regular_rows" \
        --argjson regular_bytes "$regular_bytes" '
        .schema_version == 1 and .kind == "archive-v2-workdir-quarantine-intent" and
        .archive_root == $archive and .evidence_root == $evidence and
        .plan == {rows:$plan_rows,bytes:$plan_bytes,sha256:$plan_sha} and
        .stat_tree_before == {rows:$tree_rows,bytes:$tree_bytes,sha256:$tree_sha} and
        .regular_before == {rows:$regular_rows,bytes:$regular_bytes,sha256:$regular_sha} and
        .same_device_rename == true and .content_hashed == false and
        .later_review_required == true and .canonical == false
    ' "$intent" >/dev/null || q_die "intent record changed"
else
    created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) || q_die "cannot read time"
    jq -cn --arg archive "$archive_root" --arg evidence "$evidence_root" \
        --arg plan_sha "$expected_plan_sha256" --arg tree_sha "$tree_sha256" \
        --arg regular_sha "$regular_sha256" --arg created "$created_at" \
        --argjson plan_rows "$expected_plan_rows" --argjson plan_bytes "$expected_plan_bytes" \
        --argjson tree_rows "$tree_rows" --argjson tree_bytes "$tree_bytes" \
        --argjson regular_rows "$regular_rows" --argjson regular_bytes "$regular_bytes" '
        {schema_version:1,kind:"archive-v2-workdir-quarantine-intent",
         archive_root:$archive,evidence_root:$evidence,
         plan:{rows:$plan_rows,bytes:$plan_bytes,sha256:$plan_sha},
         stat_tree_before:{rows:$tree_rows,bytes:$tree_bytes,sha256:$tree_sha},
         regular_before:{rows:$regular_rows,bytes:$regular_bytes,sha256:$regular_sha},
         same_device_rename:true,content_hashed:false,later_review_required:true,
         canonical:false,created_at_utc:$created}
    ' >"$work/intent.json" || q_die "cannot build intent record"
    q_publish "$work/intent.json" "$intent"
fi

moved_now=0
while IFS="$tab" read -r q_epoch q_name q_extra \
    || [ -n "$q_epoch$q_name$q_extra" ]; do
    q_source=$archive_root/epoch-$q_epoch/$q_name
    q_parent=$evidence_root/epoch-$q_epoch
    q_destination=$q_parent/$q_name
    if [ ! -e "$q_parent" ] && [ ! -L "$q_parent" ]; then
        mkdir -m 700 "$q_parent" || q_die "cannot create evidence epoch directory"
    fi
    [ -d "$q_parent" ] && [ ! -L "$q_parent" ] \
        && [ "$(q_stat_dev "$q_parent")" = "$archive_dev" ] \
        || q_die "evidence epoch directory is invalid"
    if [ -d "$q_source" ] && [ ! -L "$q_source" ] \
        && [ ! -e "$q_destination" ] && [ ! -L "$q_destination" ]; then
        q_dev=$(q_stat_dev "$q_source") || q_die "cannot stat source"
        q_inode=$(q_stat_inode "$q_source") || q_die "cannot stat source"
        [ "$q_dev" = "$archive_dev" ] || q_die "source is on another device"
        if [ "$move_style" = test_portable ]; then
            mv -n "$q_source" "$q_destination" || q_die "cannot rename planned entry"
        else
            mv -T -n "$q_source" "$q_destination" || q_die "cannot rename planned entry"
        fi
        [ -d "$q_destination" ] && [ ! -L "$q_destination" ] \
            && [ "$(q_stat_dev "$q_destination")" = "$q_dev" ] \
            && [ "$(q_stat_inode "$q_destination")" = "$q_inode" ] \
            || q_die "renamed entry did not preserve device and inode"
        moved_now=$((moved_now + 1))
    elif [ ! -e "$q_source" ] && [ ! -L "$q_source" ] \
        && [ -d "$q_destination" ] && [ ! -L "$q_destination" ]; then
        :
    else
        q_die "planned entry changed during rename: epoch-$q_epoch/$q_name"
    fi
done <"$plan_snapshot"

while IFS="$tab" read -r q_epoch q_name q_extra \
    || [ -n "$q_epoch$q_name$q_extra" ]; do
    q_source=$archive_root/epoch-$q_epoch/$q_name
    q_destination=$evidence_root/epoch-$q_epoch/$q_name
    [ ! -e "$q_source" ] && [ ! -L "$q_source" ] \
        && [ -d "$q_destination" ] && [ ! -L "$q_destination" ] \
        || q_die "final source/destination state is incomplete: epoch-$q_epoch/$q_name"
done <"$plan_snapshot"

q_collect_unified_plan "$work/unified-plan-after.tsv"
cmp -s "$work/unified-plan-after.tsv" "$plan_snapshot" \
    || q_die "direct non-regular inventory changed during rename"
q_collect_tree_inventory "$work/stat-tree-after.tsv"
q_collect_regular_inventory "$work/regular-after.tsv"
cmp -s "$evidence_root/stat-tree-before.tsv" "$work/stat-tree-after.tsv" \
    || q_die "normalized stat tree changed during rename"
cmp -s "$evidence_root/regular-before.tsv" "$work/regular-after.tsv" \
    || q_die "unaffected regular inventory changed during rename"
q_publish "$work/stat-tree-after.tsv" "$evidence_root/stat-tree-after.tsv"
q_publish "$work/regular-after.tsv" "$evidence_root/regular-after.tsv"

complete=$evidence_root/complete.json
if [ -e "$complete" ] || [ -L "$complete" ]; then
    [ -f "$complete" ] && [ ! -L "$complete" ] \
        || q_die "completion record is invalid"
    jq -e --arg plan_sha "$expected_plan_sha256" --arg tree_sha "$tree_sha256" \
        --arg regular_sha "$regular_sha256" --argjson count "$expected_plan_rows" '
        .schema_version == 1 and .kind == "archive-v2-workdir-quarantine-complete" and
        .entry_count == $count and .plan_sha256 == $plan_sha and
        .stat_tree_before_sha256 == $tree_sha and .stat_tree_after_sha256 == $tree_sha and
        .regular_before_sha256 == $regular_sha and .regular_after_sha256 == $regular_sha and
        .same_device_rename == true and .content_hashed == false and
        .later_review_required == true and .canonical == false
    ' "$complete" >/dev/null || q_die "completion record changed"
else
    completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) || q_die "cannot read time"
    jq -cn --arg plan_sha "$expected_plan_sha256" --arg tree_sha "$tree_sha256" \
        --arg regular_sha "$regular_sha256" --arg completed "$completed_at" \
        --argjson count "$expected_plan_rows" --argjson moved_now "$moved_now" '
        {schema_version:1,kind:"archive-v2-workdir-quarantine-complete",
         entry_count:$count,moved_in_final_run:$moved_now,plan_sha256:$plan_sha,
         stat_tree_before_sha256:$tree_sha,stat_tree_after_sha256:$tree_sha,
         regular_before_sha256:$regular_sha,regular_after_sha256:$regular_sha,
         same_device_rename:true,content_hashed:false,later_review_required:true,
         canonical:false,
         completed_at_utc:$completed}
    ' >"$work/complete.json" || q_die "cannot build completion record"
    q_publish "$work/complete.json" "$complete"
fi

jq -c . "$complete"
