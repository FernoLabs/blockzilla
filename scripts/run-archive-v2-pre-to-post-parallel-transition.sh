#!/bin/sh
# Continue one admitted sequential Compact Archive V2 Pre-to-Post run with at
# most two epoch workers. The immutable sequential prefix remains unchanged.
# New suffix work is authorized by one transition admission, one atomic claim
# per epoch, and one companion attestation per epoch.

set -u
umask 077

parallel_die() {
    echo "archive-v2 Pre-to-Post parallel transition: $*" >&2
    exit 1
}

parallel_usage() {
    echo "usage: $0 HANDOFF_STATE_ROOT FENCE_EPOCH WORKERS [NEW_CONVERTER]" >&2
    echo "       $0 --fence-json HANDOFF_STATE_ROOT FENCE_EPOCH [NEW_CONVERTER]" >&2
    exit 2
}

parallel_require_command() {
    command -v "$1" >/dev/null 2>&1 \
        || parallel_die "required command is not available: $1"
}

parallel_canonical_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

parallel_require_absolute_single_line() {
    pras_name=$1
    pras_value=$2
    case "$pras_value" in
        /*) ;;
        *) parallel_die "$pras_name must be an absolute path: $pras_value" ;;
    esac
    case "$pras_value" in
        *'
'*) parallel_die "$pras_name must not contain a line break" ;;
    esac
}

parallel_sha256_file() (
    if [ "$sha256_program" = sha256sum ]; then
        psf_output=$(sha256sum "$1") || exit 1
    else
        psf_output=$(shasum -a 256 "$1") || exit 1
    fi
    printf '%s\n' "$psf_output" | awk '
        NR == 1 && length($1) == 64 && $1 !~ /[^0-9a-f]/ {
            print $1
            valid = 1
        }
        END { if (!valid) exit 1 }
    '
)

parallel_publish_file_no_replace() {
    ppnr_source=$1
    ppnr_target=$2
    [ -f "$ppnr_source" ] && [ ! -L "$ppnr_source" ] || return 1
    [ ! -e "$ppnr_target" ] && [ ! -L "$ppnr_target" ] || return 1
    ln -n "$ppnr_source" "$ppnr_target" || return 1
}

parallel_copy_file_exclusive() {
    pcfe_source=$1
    pcfe_target=$2
    (
        set -C
        dd if="$pcfe_source" bs=1048576 >"$pcfe_target" 2>/dev/null
    )
}

parallel_json_field() {
    pjf_expression=$1
    pjf_file=$2
    jq -er "$pjf_expression" "$pjf_file" 2>/dev/null \
        || parallel_die "invalid or absent $pjf_expression in $pjf_file"
}

parallel_validate_decimal() {
    pvd_name=$1
    pvd_value=$2
    case "$pvd_value" in
        0) ;;
        ''|*[!0-9]*|0*) parallel_die "$pvd_name is not a canonical decimal integer" ;;
        *) ;;
    esac
    [ "$(printf '%s' "$pvd_value" | wc -c | tr -d '[:space:]')" -le 14 ] \
        || parallel_die "$pvd_name is too large"
}

for parallel_required in jq awk sed wc tr date nice ionice chmod mkdir rmdir ln \
    rm dd df python3 ps sleep cmp mv dirname basename kill; do
    parallel_require_command "$parallel_required"
done
if command -v sha256sum >/dev/null 2>&1; then
    sha256_program=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    sha256_program=shasum
else
    parallel_die 'sha256sum or shasum is required'
fi

fence_mode=0
new_converter_argument=
if { [ "$#" -eq 3 ] || [ "$#" -eq 4 ]; } && [ "$1" = --fence-json ]; then
    fence_mode=1
    handoff_state_root=$2
    fence_epoch=$3
    [ "$#" -eq 3 ] || new_converter_argument=$4
elif [ "$#" -eq 3 ] || [ "$#" -eq 4 ]; then
    handoff_state_root=$1
    fence_epoch=$2
    worker_count=$3
    [ "$#" -eq 3 ] || new_converter_argument=$4
else
    parallel_usage
fi

parallel_require_absolute_single_line HANDOFF_STATE_ROOT "$handoff_state_root"
parallel_validate_decimal FENCE_EPOCH "$fence_epoch"
if [ "$fence_mode" -eq 0 ]; then
    case "$worker_count" in
        1|2) ;;
        *) parallel_die 'WORKERS must be 1 or 2' ;;
    esac
fi
[ -d "$handoff_state_root" ] && [ ! -L "$handoff_state_root" ] \
    || parallel_die "handoff state root is not one real directory: $handoff_state_root"
handoff_state_canonical=$(parallel_canonical_directory "$handoff_state_root") \
    || parallel_die 'cannot canonicalize handoff state root'
[ "$handoff_state_canonical" = "$handoff_state_root" ] \
    || parallel_die "HANDOFF_STATE_ROOT must already be canonical: $handoff_state_canonical"

self_origin=$0
parallel_require_absolute_single_line PARALLEL_RUNNER "$self_origin"
[ -f "$self_origin" ] && [ ! -L "$self_origin" ] && [ -x "$self_origin" ] \
    && [ ! -w "$self_origin" ] \
    || parallel_die "parallel runner is not one real executable file: $self_origin"
self_parent=$(parallel_canonical_directory "$(dirname "$self_origin")") \
    || parallel_die 'cannot canonicalize parallel runner parent'
self_canonical=$self_parent/$(basename "$self_origin")
[ "$self_canonical" = "$self_origin" ] \
    || parallel_die "parallel runner path must already be canonical: $self_canonical"
self_sha=$(parallel_sha256_file "$self_origin") \
    || parallel_die 'cannot hash parallel runner'
self_expected_sha=$(basename "$self_origin" | sed -n \
    's/^run-archive-v2-pre-to-post-parallel-transition-\([0-9a-f]\{64\}\)\.sh$/\1/p')
[ -n "$self_expected_sha" ] && [ "$self_expected_sha" = "$self_sha" ] \
    || parallel_die 'parallel runner must have its exact content-addressed basename'

request=$handoff_state_root/request.json
cohort_binding=$handoff_state_root/cohort.binding.json
cohort_json=$handoff_state_root/cohort/cohort.json
master_epoch_list=$handoff_state_root/cohort/epochs.txt
conversion_state=$handoff_state_root/conversion
for parallel_control in "$request" "$cohort_binding" "$cohort_json" "$master_epoch_list"; do
    [ -f "$parallel_control" ] && [ ! -L "$parallel_control" ] \
        || parallel_die "required handoff control is not one real file: $parallel_control"
done
[ -d "$conversion_state" ] && [ ! -L "$conversion_state" ] \
    || parallel_die "sequential conversion state is absent: $conversion_state"

source_root=$(parallel_json_field '.archive_root | select(type == "string")' "$request")
target_root=$(parallel_json_field '.target_root | select(type == "string")' "$request")
request_state_root=$(parallel_json_field '.state_root | select(type == "string")' "$request")
cluster_id=$(parallel_json_field '.cluster_id | select(type == "string")' "$request")
run_id=$(parallel_json_field '.run_id | select(type == "string")' "$request")
old_runner_origin=$(parallel_json_field '.converter_runner | select(type == "string")' "$request")
old_runner_sha=$(parallel_json_field '.converter_runner_sha256 | select(type == "string")' "$request")
old_converter_origin=$(parallel_json_field '.converter | select(type == "string")' "$request")
old_converter_sha=$(parallel_json_field '.converter_sha256 | select(type == "string")' "$request")
[ "$request_state_root" = "$handoff_state_root" ] \
    || parallel_die 'request.json is bound to a different handoff state root'
for parallel_root in "$source_root" "$target_root"; do
    parallel_require_absolute_single_line ROOT "$parallel_root"
    [ -d "$parallel_root" ] && [ ! -L "$parallel_root" ] \
        || parallel_die "request root is not one real directory: $parallel_root"
    [ "$(parallel_canonical_directory "$parallel_root")" = "$parallel_root" ] \
        || parallel_die "request root is not canonical: $parallel_root"
done
[ "$source_root" != "$target_root" ] || parallel_die 'source and target roots must differ'

for parallel_digest in "$old_runner_sha" "$old_converter_sha"; do
    case "$parallel_digest" in
        *[!0-9a-f]*|'') parallel_die 'request contains an invalid SHA-256 digest' ;;
    esac
    [ "${#parallel_digest}" -eq 64 ] || parallel_die 'request contains a short SHA-256 digest'
done
[ -f "$old_runner_origin" ] && [ ! -L "$old_runner_origin" ] \
    || parallel_die "old runner is not one real file: $old_runner_origin"
[ "$(parallel_sha256_file "$old_runner_origin")" = "$old_runner_sha" ] \
    || parallel_die 'old runner changed after handoff admission'
[ "$(basename "$old_runner_origin")" = "run-archive-v2-pre-to-post-manual-$old_runner_sha.sh" ] \
    || parallel_die 'old runner path is not content-addressed as admitted'

if [ -n "$new_converter_argument" ]; then
    suffix_converter_origin=$new_converter_argument
else
    suffix_converter_origin=$old_converter_origin
fi
parallel_require_absolute_single_line NEW_CONVERTER "$suffix_converter_origin"
[ -f "$suffix_converter_origin" ] && [ ! -L "$suffix_converter_origin" ] \
    && [ -x "$suffix_converter_origin" ] && [ ! -w "$suffix_converter_origin" ] \
    || parallel_die "suffix converter is not one read-only real executable file: $suffix_converter_origin"
suffix_converter_sha=$(parallel_sha256_file "$suffix_converter_origin") \
    || parallel_die 'cannot hash suffix converter'
suffix_converter_expected_sha=$(basename "$suffix_converter_origin" | sed -n \
    's/^archive-v2-pre-to-post-\([0-9a-f]\{64\}\)$/\1/p')
[ -n "$suffix_converter_expected_sha" ] \
    && [ "$suffix_converter_expected_sha" = "$suffix_converter_sha" ] \
    || parallel_die 'suffix converter does not have its exact content-addressed basename'

request_sha=$(parallel_sha256_file "$request") || parallel_die 'cannot hash request.json'
cohort_binding_sha=$(parallel_sha256_file "$cohort_binding") \
    || parallel_die 'cannot hash cohort binding'
cohort_json_sha=$(parallel_sha256_file "$cohort_json") || parallel_die 'cannot hash cohort JSON'
master_epoch_list_sha=$(parallel_sha256_file "$master_epoch_list") \
    || parallel_die 'cannot hash master epoch list'
jq -e --arg cohort_sha "$cohort_json_sha" --arg epochs_sha "$master_epoch_list_sha" \
    '.cohort_json_sha256 == $cohort_sha and .epochs_sha256 == $epochs_sha' \
    "$cohort_binding" >/dev/null 2>&1 \
    || parallel_die 'cohort binding does not bind the current cohort files'

epoch_list_snapshot=$conversion_state/epoch-list.snapshot
[ -f "$epoch_list_snapshot" ] && [ ! -L "$epoch_list_snapshot" ] \
    && [ ! -w "$epoch_list_snapshot" ] \
    || parallel_die 'the immutable sequential epoch-list snapshot is absent'
epoch_list_sha=$(parallel_sha256_file "$epoch_list_snapshot") \
    || parallel_die 'cannot hash sequential epoch-list snapshot'
[ "$epoch_list_sha" = "$master_epoch_list_sha" ] \
    || parallel_die 'sequential snapshot differs from the admitted master cohort'
cmp -s "$epoch_list_snapshot" "$master_epoch_list" \
    || parallel_die 'sequential snapshot bytes differ from the admitted master cohort'

fence_path=$conversion_state/epoch-$fence_epoch.json.building-parallel-transition-$self_sha
if [ "$fence_mode" -eq 1 ]; then
    jq -cn \
        --arg run_id "$run_id" \
        --argjson epoch "$fence_epoch" \
        --arg request "$request" \
        --arg request_sha "$request_sha" \
        --arg cohort_binding "$cohort_binding" \
        --arg cohort_binding_sha "$cohort_binding_sha" \
        --arg epoch_list "$master_epoch_list" \
        --arg epoch_list_sha "$master_epoch_list_sha" \
        --arg old_runner "$old_runner_origin" \
        --arg old_runner_sha "$old_runner_sha" \
        --arg new_runner "$self_origin" \
        --arg new_runner_sha "$self_sha" \
        --arg suffix_converter "$suffix_converter_origin" \
        --arg suffix_converter_sha "$suffix_converter_sha" '
        {
            schema_version: 1,
            kind: "archive-v2-pre-to-post-parallel-transition-fence",
            purpose: "stop-sequential-before-parallel-transition",
            run_id: $run_id,
            epoch: $epoch,
            request: {path: $request, sha256: $request_sha},
            cohort_binding: {path: $cohort_binding, sha256: $cohort_binding_sha},
            epoch_list: {path: $epoch_list, sha256: $epoch_list_sha},
            old_runner: {path: $old_runner, sha256: $old_runner_sha},
            parallel_runner: {path: $new_runner, sha256: $new_runner_sha},
            suffix_converter: {path: $suffix_converter, sha256: $suffix_converter_sha}
        }
    '
    exit 0
fi

[ -f "$fence_path" ] && [ ! -L "$fence_path" ] && [ ! -w "$fence_path" ] \
    || parallel_die "exact read-only transition fence is absent: $fence_path"
fence_sha=$(parallel_sha256_file "$fence_path") || parallel_die 'cannot hash transition fence'
jq -e -s \
    --arg run_id "$run_id" --argjson epoch "$fence_epoch" \
    --arg request "$request" --arg request_sha "$request_sha" \
    --arg binding "$cohort_binding" --arg binding_sha "$cohort_binding_sha" \
    --arg list "$master_epoch_list" --arg list_sha "$master_epoch_list_sha" \
    --arg old "$old_runner_origin" --arg old_sha "$old_runner_sha" \
    --arg new "$self_origin" --arg new_sha "$self_sha" \
    --arg suffix_converter "$suffix_converter_origin" \
    --arg suffix_converter_sha "$suffix_converter_sha" '
    length == 1 and (.[0] as $f |
        ($f | keys) == ["cohort_binding", "epoch", "epoch_list", "kind",
            "old_runner", "parallel_runner", "purpose", "request", "run_id",
            "schema_version", "suffix_converter"] and
        $f.schema_version == 1 and
        $f.kind == "archive-v2-pre-to-post-parallel-transition-fence" and
        $f.purpose == "stop-sequential-before-parallel-transition" and
        $f.run_id == $run_id and $f.epoch == $epoch and
        $f.request == {path: $request, sha256: $request_sha} and
        $f.cohort_binding == {path: $binding, sha256: $binding_sha} and
        $f.epoch_list == {path: $list, sha256: $list_sha} and
        $f.old_runner == {path: $old, sha256: $old_sha} and
        $f.parallel_runner == {path: $new, sha256: $new_sha} and
        $f.suffix_converter == {path: $suffix_converter, sha256: $suffix_converter_sha})
' "$fence_path" >/dev/null 2>&1 || parallel_die 'transition fence is invalid'

# Load only the reviewed validation and attestation functions from the exact
# frozen sequential runner. The main program starts at the exact sentinel and
# is never sourced.
old_runner_pinned=$conversion_state/.parallel-old-runner-$old_runner_sha.sh
if [ -e "$old_runner_pinned" ] || [ -L "$old_runner_pinned" ]; then
    [ -f "$old_runner_pinned" ] && [ ! -L "$old_runner_pinned" ] \
        && [ ! -w "$old_runner_pinned" ] \
        || parallel_die 'private old runner copy is not one immutable real file'
else
    old_runner_pinned_candidate=$old_runner_pinned.building-$$
    exec 5<"$old_runner_origin" || parallel_die 'cannot pin old runner descriptor'
    parallel_copy_file_exclusive /dev/fd/5 "$old_runner_pinned_candidate" \
        || parallel_die 'cannot copy pinned old runner descriptor'
    exec 5<&-
    [ "$(parallel_sha256_file "$old_runner_pinned_candidate")" = "$old_runner_sha" ] \
        || parallel_die 'pinned old runner descriptor differs from handoff admission'
    chmod 400 "$old_runner_pinned_candidate" \
        || parallel_die 'cannot protect private old runner copy'
    parallel_publish_file_no_replace "$old_runner_pinned_candidate" "$old_runner_pinned" \
        || parallel_die 'cannot publish private old runner copy'
    rm "$old_runner_pinned_candidate" || parallel_die 'cannot remove linked old runner candidate'
fi
[ "$(parallel_sha256_file "$old_runner_pinned")" = "$old_runner_sha" ] \
    || parallel_die 'private old runner copy has the wrong digest'
validator_library=$conversion_state/.parallel-validator-library.building-$$
for stale_validator_library in "$conversion_state"/.parallel-validator-library.building-*; do
    if [ -e "$stale_validator_library" ] || [ -L "$stale_validator_library" ]; then
        parallel_die "unfinished validator extraction needs review: $stale_validator_library"
    fi
done
(
    set -C
    awk '
        $0 == "[ \"$#\" -eq 7 ] || usage" { found = 1; exit }
        { print }
        END { if (!found) exit 42 }
    ' "$old_runner_pinned" >"$validator_library"
) || parallel_die 'cannot extract the frozen runner validation library'
chmod 400 "$validator_library" || parallel_die 'cannot protect extracted validator library'
# shellcheck disable=SC1090
. "$validator_library"
rm "$validator_library" || parallel_die 'cannot remove private validator library'

# The frozen library sets these constants and functions. Restore this
# coordinator's error prefix and bind the globals that its validators use.
die() {
    parallel_die "$*"
}
epoch_list=$master_epoch_list
state_root=$conversion_state
converter_origin=$old_converter_origin
converter_sha=$old_converter_sha
old_pinned_converter=$conversion_state/.archive-v2-pre-to-post-bin/$(basename "$old_converter_origin")
pinned_converter=$old_pinned_converter
[ -f "$old_pinned_converter" ] && [ ! -L "$old_pinned_converter" ] \
    && [ -x "$old_pinned_converter" ] && [ ! -w "$old_pinned_converter" ] \
    || parallel_die 'the exact sequential pinned converter is absent'
[ "$(sha256_file "$old_pinned_converter")" = "$old_converter_sha" ] \
    || parallel_die 'the sequential pinned converter hash changed'
[ -f "$old_converter_origin" ] && [ ! -L "$old_converter_origin" ] \
    && [ -x "$old_converter_origin" ] && [ ! -w "$old_converter_origin" ] \
    || parallel_die 'the sequential converter origin is not one read-only real executable'
[ "$(sha256_file "$old_converter_origin")" = "$old_converter_sha" ] \
    || parallel_die 'the converter origin changed after handoff admission'

epoch_count=$(validate_epoch_list "$epoch_list_snapshot") \
    || parallel_die 'master epoch list is invalid'
epochs_json=$(jq -Rsc 'split("\n") | map(select(length > 0) | tonumber)' \
    "$epoch_list_snapshot") || parallel_die 'cannot encode master epoch list'
jq -e --argjson epoch "$fence_epoch" 'index($epoch) != null' <<EOF >/dev/null \
    || parallel_die 'fence epoch is not in the exact master cohort'
$epochs_json
EOF

state_lock=$conversion_state/.archive-v2-pre-to-post-manual.lock
handoff_state_lock=$handoff_state_root/.archive-v2-pre-to-post-handoff.lock
target_lock=$target_root/.archive-v2-pre-to-post-handoff.lock
state_lock_held=0
handoff_state_lock_held=0
target_lock_held=0
job_pid_0=
job_pid_1=

parallel_release_locks() {
    trap - 0 1 2 3 15
    if [ "$target_lock_held" -eq 1 ]; then
        rmdir "$target_lock" 2>/dev/null \
            || echo "parallel transition: cannot remove target authority lock: $target_lock" >&2
        target_lock_held=0
    fi
    if [ "$handoff_state_lock_held" -eq 1 ]; then
        rmdir "$handoff_state_lock" 2>/dev/null \
            || echo "parallel transition: cannot remove handoff state lock: $handoff_state_lock" >&2
        handoff_state_lock_held=0
    fi
    if [ "$state_lock_held" -eq 1 ]; then
        rmdir "$state_lock" 2>/dev/null \
            || echo "parallel transition: cannot remove conversion state lock: $state_lock" >&2
        state_lock_held=0
    fi
}

parallel_stop() {
    ps_status=$1
    trap - 0 1 2 3 15
    for ps_pid in "$job_pid_0" "$job_pid_1"; do
        [ -z "$ps_pid" ] || kill -TERM "$ps_pid" 2>/dev/null || :
    done
    for ps_pid in "$job_pid_0" "$job_pid_1"; do
        [ -z "$ps_pid" ] || wait "$ps_pid" 2>/dev/null || :
    done
    job_pid_0=
    job_pid_1=
    echo "parallel transition: interruption reaped workers and preserved all authority locks for review" >&2
    exit "$ps_status"
}
trap 'parallel_release_locks' 0
trap 'parallel_stop 129' 1
trap 'parallel_stop 130' 2
trap 'parallel_stop 131' 3
trap 'parallel_stop 143' 15

if ! mkdir "$state_lock" 2>/dev/null; then
    parallel_die "sequential or parallel conversion state lock exists: $state_lock"
fi
state_lock_held=1
if ! mkdir "$handoff_state_lock" 2>/dev/null; then
    parallel_die "old or new handoff state lock exists: $handoff_state_lock"
fi
handoff_state_lock_held=1
if ! mkdir "$target_lock" 2>/dev/null; then
    parallel_die "root-wide target publisher lock exists: $target_lock"
fi
target_lock_held=1

# Take a stable process snapshot after both locks are ours. This rejects a
# leaked old runner or converter even if its lock was removed incorrectly.
process_snapshot_dir=$conversion_state/parallel-process-snapshots
mkdir -p "$process_snapshot_dir" || parallel_die 'cannot create process snapshot directory'
chmod 700 "$process_snapshot_dir" || parallel_die 'cannot protect process snapshot directory'
process_snapshot=$process_snapshot_dir/process-$$.txt
(
    set -C
    ps -eo pid=,args= >"$process_snapshot"
) || parallel_die 'cannot take process snapshot'
chmod 400 "$process_snapshot" || parallel_die 'cannot make process snapshot read-only'
if awk -v self="$$" -v old="$old_runner_origin" -v converter="$old_converter_origin" \
    -v new_converter="$suffix_converter_origin" -v pinned="$old_pinned_converter" \
    -v staging="$target_root/.epoch-" '
    {
        pid = $1
        if (pid == self) next
        line = $0
        if (index(line, old) || index(line, converter) || index(line, new_converter) ||
            index(line, pinned) ||
            (index(line, staging) && index(line, ".pre-to-post.staging"))) {
            print line > "/dev/stderr"
            found = 1
        }
    }
    END { exit found ? 0 : 1 }
' "$process_snapshot"; then
    parallel_die 'an old runner or converter process is still present'
fi

parallel_epoch_in_json() {
    peij_epoch=$1
    peij_json=$2
    jq -e --argjson epoch "$peij_epoch" 'index($epoch) != null' <<EOF >/dev/null 2>&1
$peij_json
EOF
}

excluded_epochs_json=$(jq -c '.excluded_epochs | select(type == "array")' "$request") \
    || parallel_die 'request does not contain an excluded-epoch array'
jq -e 'all(.[]; type == "number" and . >= 0 and floor == .) and
    (length == (unique | length))' <<EOF >/dev/null \
    || parallel_die 'request excluded epochs are invalid'
$excluded_epochs_json
EOF

# No generation outside the immutable cohort or its admitted exclusions can be
# present in this target root. Any staging directory is a hard stop.
for parallel_target_entry in "$target_root"/epoch-*; do
    if [ ! -e "$parallel_target_entry" ] && [ ! -L "$parallel_target_entry" ]; then
        continue
    fi
    parallel_target_name=${parallel_target_entry##*/}
    parallel_target_epoch=$(printf '%s\n' "$parallel_target_name" | sed -n \
        's/^epoch-\([0-9][0-9]*\)$/\1/p')
    [ -n "$parallel_target_epoch" ] \
        || parallel_die "unexpected target-root epoch entry: $parallel_target_entry"
    parallel_validate_decimal TARGET_EPOCH "$parallel_target_epoch"
    if ! parallel_epoch_in_json "$parallel_target_epoch" "$epochs_json" \
        && ! parallel_epoch_in_json "$parallel_target_epoch" "$excluded_epochs_json"; then
        parallel_die "target epoch is outside the exact cohort and exclusions: $parallel_target_epoch"
    fi
done
for parallel_staging in "$target_root"/.epoch-*.pre-to-post.staging; do
    if [ -e "$parallel_staging" ] || [ -L "$parallel_staging" ]; then
        parallel_die "staging exists and needs review: $parallel_staging"
    fi
done

# The exact admitted transition fence is the only old runner building file
# that can remain. It is retained as permanent transition evidence.
for parallel_building in \
    "$conversion_state"/epoch-*.json.building-* \
    "$conversion_state"/epoch-*.log.building-* \
    "$conversion_state"/epoch-*.attestation.json.building-* \
    "$conversion_state"/complete.json.building-*; do
    if [ ! -e "$parallel_building" ] && [ ! -L "$parallel_building" ]; then
        continue
    fi
    [ "$parallel_building" = "$fence_path" ] \
        || parallel_die "unadmitted sequential building state needs review: $parallel_building"
done
for parallel_handoff_building in "$handoff_state_root"/complete.json.building-*; do
    if [ -e "$parallel_handoff_building" ] || [ -L "$parallel_handoff_building" ]; then
        parallel_die "unfinished handoff completion needs review: $parallel_handoff_building"
    fi
done

admission=$conversion_state/parallel-transition-admission.json
transition_dir=$conversion_state/parallel-transition
if [ -e "$transition_dir" ] || [ -L "$transition_dir" ]; then
    [ -d "$transition_dir" ] && [ ! -L "$transition_dir" ] \
        || parallel_die "parallel transition path is not one real directory: $transition_dir"
else
    mkdir "$transition_dir" || parallel_die 'cannot create parallel transition directory'
fi
chmod 700 "$transition_dir" || parallel_die 'cannot protect parallel transition directory'
claims_dir=$transition_dir/claims
results_dir=$transition_dir/results
attempts_dir=$transition_dir/attempts
suffix_bin_dir=$transition_dir/bin
for parallel_dir in "$claims_dir" "$results_dir" "$attempts_dir" "$suffix_bin_dir"; do
    if [ -e "$parallel_dir" ] || [ -L "$parallel_dir" ]; then
        [ -d "$parallel_dir" ] && [ ! -L "$parallel_dir" ] \
            || parallel_die "parallel state path is not one real directory: $parallel_dir"
    else
        mkdir "$parallel_dir" || parallel_die "cannot create parallel state directory: $parallel_dir"
    fi
    chmod 700 "$parallel_dir" || parallel_die "cannot protect parallel state directory: $parallel_dir"
done

suffix_pinned_converter=$suffix_bin_dir/$(basename "$suffix_converter_origin")
suffix_converter_candidate=$suffix_pinned_converter.building-$$
if [ -e "$suffix_pinned_converter" ] || [ -L "$suffix_pinned_converter" ]; then
    [ -f "$suffix_pinned_converter" ] && [ ! -L "$suffix_pinned_converter" ] \
        && [ -x "$suffix_pinned_converter" ] && [ ! -w "$suffix_pinned_converter" ] \
        || parallel_die 'suffix pinned converter is not one immutable executable'
else
    parallel_copy_file_exclusive "$suffix_converter_origin" "$suffix_converter_candidate" \
        || parallel_die 'cannot make an exclusive suffix converter copy'
    [ "$(sha256_file "$suffix_converter_candidate")" = "$suffix_converter_sha" ] \
        || parallel_die 'suffix converter changed while it was copied'
    chmod 500 "$suffix_converter_candidate" \
        || parallel_die 'cannot make suffix converter copy read-only'
    parallel_publish_file_no_replace "$suffix_converter_candidate" "$suffix_pinned_converter" \
        || parallel_die 'cannot publish suffix converter copy without replacement'
    rm "$suffix_converter_candidate" \
        || parallel_die 'cannot remove linked suffix converter candidate'
fi
[ "$(sha256_file "$suffix_pinned_converter")" = "$suffix_converter_sha" ] \
    || parallel_die 'suffix pinned converter has the wrong digest'

parallel_publish_derived_file() {
    ppdf_candidate=$1
    ppdf_final=$2
    chmod 400 "$ppdf_candidate" || return 1
    if [ -e "$ppdf_final" ] || [ -L "$ppdf_final" ]; then
        [ -f "$ppdf_final" ] && [ ! -L "$ppdf_final" ] && [ ! -w "$ppdf_final" ] \
            && cmp -s "$ppdf_candidate" "$ppdf_final" || return 1
        rm "$ppdf_candidate" || return 1
    else
        parallel_publish_file_no_replace "$ppdf_candidate" "$ppdf_final" || return 1
        rm "$ppdf_candidate" || return 1
    fi
}

suffix_epoch_list=$transition_dir/suffix.epochs
worker_0_epoch_list=$transition_dir/worker-0.epochs
worker_1_epoch_list=$transition_dir/worker-1.epochs
suffix_candidate=$suffix_epoch_list.building-$$
worker_0_candidate=$worker_0_epoch_list.building-$$
worker_1_candidate=$worker_1_epoch_list.building-$$
for parallel_candidate in "$suffix_candidate" "$worker_0_candidate" "$worker_1_candidate"; do
    [ ! -e "$parallel_candidate" ] && [ ! -L "$parallel_candidate" ] \
        || parallel_die "PID-specific partition candidate exists: $parallel_candidate"
done

prefix_proofs_json='[]'
prefix_epochs_json='[]'
suffix_epochs_json='[]'
prefix_count=0
suffix_count=0
found_fence=0
exec 3<"$epoch_list_snapshot" || parallel_die 'cannot open master epoch list'
while IFS= read -r transition_epoch <&3 || [ -n "$transition_epoch" ]; do
    source_epoch=$source_root/epoch-$transition_epoch
    target_epoch=$target_root/epoch-$transition_epoch
    staging_epoch=$target_root/.epoch-$transition_epoch.pre-to-post.staging
    report=$conversion_state/epoch-$transition_epoch.json
    log=$conversion_state/epoch-$transition_epoch.log
    attestation=$conversion_state/epoch-$transition_epoch.attestation.json
    generation_id=archive-v2-pre-to-post-$run_id-epoch-$transition_epoch-post
    lease_id=archive-v2-pre-to-post-$run_id-epoch-$transition_epoch-source-leases
    [ -d "$source_epoch" ] && [ ! -L "$source_epoch" ] \
        || parallel_die "source epoch is absent: $source_epoch"
    [ ! -e "$staging_epoch" ] && [ ! -L "$staging_epoch" ] \
        || parallel_die "staging exists and needs review: $staging_epoch"

    if [ "$transition_epoch" = "$fence_epoch" ]; then
        found_fence=1
    fi
    if [ "$found_fence" -eq 0 ]; then
        [ -d "$target_epoch" ] && [ ! -L "$target_epoch" ] \
            || parallel_die "sequential prefix target is absent: $target_epoch"
        for prefix_state in "$report" "$log" "$attestation"; do
            [ -f "$prefix_state" ] && [ ! -L "$prefix_state" ] \
                || parallel_die "sequential prefix state is absent: $prefix_state"
        done
        validate_target_generation "$transition_epoch" "$source_epoch" "$target_epoch" \
            "$staging_epoch" "$generation_id" "$lease_id" "$report" 0 \
            || parallel_die "sequential prefix target is invalid: epoch $transition_epoch"
        validate_epoch_attestation "$attestation" "$transition_epoch" "$source_epoch" \
            "$target_epoch" "$staging_epoch" "$generation_id" "$lease_id" "$report" "$log" \
            || parallel_die "sequential prefix attestation is invalid: epoch $transition_epoch"
        prefix_attestation_sha=$(sha256_file "$attestation") \
            || parallel_die "cannot hash prefix attestation: epoch $transition_epoch"
        prefix_epochs_json=$(jq -cn --argjson current "$prefix_epochs_json" \
            --argjson epoch "$transition_epoch" '$current + [$epoch]') \
            || parallel_die 'cannot build prefix epoch proof'
        prefix_proofs_json=$(jq -cn --argjson current "$prefix_proofs_json" \
            --argjson epoch "$transition_epoch" --arg path "$attestation" \
            --arg sha "$prefix_attestation_sha" \
            '$current + [{epoch: $epoch, attestation: {path: $path, sha256: $sha}}]') \
            || parallel_die 'cannot build prefix attestation proof'
        prefix_count=$((prefix_count + 1))
    else
        suffix_epochs_json=$(jq -cn --argjson current "$suffix_epochs_json" \
            --argjson epoch "$transition_epoch" '$current + [$epoch]') \
            || parallel_die 'cannot build suffix epoch list'
        suffix_count=$((suffix_count + 1))
    fi
done
exec 3<&-
[ "$found_fence" -eq 1 ] || parallel_die 'fence epoch was not found in master order'
[ "$suffix_count" -gt 0 ] || parallel_die 'transition suffix is empty'
[ $((prefix_count + suffix_count)) -eq "$epoch_count" ] \
    || parallel_die 'prefix and suffix do not cover the exact master cohort'

# On first admission, no suffix producer is allowed to have started. On
# resume, the immutable admission authorizes only claim-bound suffix work.
if [ ! -e "$admission" ] && [ ! -L "$admission" ]; then
    for transition_epoch in $(jq -r '.[]' <<EOF
$suffix_epochs_json
EOF
); do
        for suffix_absent in \
            "$target_root/epoch-$transition_epoch" \
            "$conversion_state/epoch-$transition_epoch.json" \
            "$conversion_state/epoch-$transition_epoch.log" \
            "$conversion_state/epoch-$transition_epoch.attestation.json" \
            "$conversion_state/epoch-$transition_epoch.parallel-attestation.json" \
            "$claims_dir/epoch-$transition_epoch.claim.json" \
            "$results_dir/epoch-$transition_epoch.json"; do
            [ ! -e "$suffix_absent" ] && [ ! -L "$suffix_absent" ] \
                || parallel_die "suffix state exists before transition admission: $suffix_absent"
        done
    done
fi

(
    set -C
    jq -r '.[]' <<EOF >"$suffix_candidate"
$suffix_epochs_json
EOF
) || parallel_die 'cannot create suffix epoch-list candidate'
(
    set -C
    jq -r --argjson workers "$worker_count" \
        'to_entries[] | select((.key % $workers) == 0) | .value' <<EOF >"$worker_0_candidate"
$suffix_epochs_json
EOF
) || parallel_die 'cannot create worker-0 epoch-list candidate'
(
    set -C
    if [ "$worker_count" -eq 2 ]; then
        jq -r 'to_entries[] | select((.key % 2) == 1) | .value' <<EOF
$suffix_epochs_json
EOF
    fi >"$worker_1_candidate"
) || parallel_die 'cannot create worker-1 epoch-list candidate'
parallel_publish_derived_file "$suffix_candidate" "$suffix_epoch_list" \
    || parallel_die 'cannot publish exact suffix epoch list'
parallel_publish_derived_file "$worker_0_candidate" "$worker_0_epoch_list" \
    || parallel_die 'cannot publish worker-0 epoch list'
parallel_publish_derived_file "$worker_1_candidate" "$worker_1_epoch_list" \
    || parallel_die 'cannot publish worker-1 epoch list'

suffix_epoch_list_sha=$(sha256_file "$suffix_epoch_list") \
    || parallel_die 'cannot hash suffix epoch list'
worker_0_epoch_list_sha=$(sha256_file "$worker_0_epoch_list") \
    || parallel_die 'cannot hash worker-0 epoch list'
worker_1_epoch_list_sha=$(sha256_file "$worker_1_epoch_list") \
    || parallel_die 'cannot hash worker-1 epoch list'
worker_0_epochs_json=$(jq -Rsc 'split("\n") | map(select(length > 0) | tonumber)' \
    "$worker_0_epoch_list") || parallel_die 'cannot encode worker-0 epoch list'
worker_1_epochs_json=$(jq -Rsc 'split("\n") | map(select(length > 0) | tonumber)' \
    "$worker_1_epoch_list") || parallel_die 'cannot encode worker-1 epoch list'
jq -ne --argjson suffix "$suffix_epochs_json" --argjson zero "$worker_0_epochs_json" \
    --argjson one "$worker_1_epochs_json" --argjson workers "$worker_count" '
    $zero == [$suffix | to_entries[] | select((.key % $workers) == 0) | .value] and
    $one == (if $workers == 2 then
        [$suffix | to_entries[] | select((.key % 2) == 1) | .value]
    else [] end)
' >/dev/null 2>&1 || parallel_die 'worker partitions do not exactly match the suffix'

admission_candidate=$admission.building-$$
(
    set -C
    jq -cn \
        --arg run_id "$run_id" --arg cluster "$cluster_id" \
        --arg source_root "$source_root" --arg target_root "$target_root" \
        --arg handoff_state "$handoff_state_root" --arg conversion_state "$conversion_state" \
        --arg request "$request" --arg request_sha "$request_sha" \
        --arg binding "$cohort_binding" --arg binding_sha "$cohort_binding_sha" \
        --arg cohort "$cohort_json" --arg cohort_sha "$cohort_json_sha" \
        --arg list "$master_epoch_list" --arg list_sha "$master_epoch_list_sha" \
        --arg snapshot "$epoch_list_snapshot" --arg snapshot_sha "$epoch_list_sha" \
        --arg fence "$fence_path" --arg fence_sha "$fence_sha" \
        --argjson fence_epoch "$fence_epoch" \
        --arg old_runner "$old_runner_origin" --arg old_runner_sha "$old_runner_sha" \
        --arg new_runner "$self_origin" --arg new_runner_sha "$self_sha" \
        --arg old_converter_origin "$old_converter_origin" \
        --arg old_converter_pinned "$old_pinned_converter" \
        --arg old_converter_sha "$old_converter_sha" \
        --arg suffix_converter_origin "$suffix_converter_origin" \
        --arg suffix_converter_pinned "$suffix_pinned_converter" \
        --arg suffix_converter_sha "$suffix_converter_sha" \
        --arg suffix_list "$suffix_epoch_list" --arg suffix_sha "$suffix_epoch_list_sha" \
        --arg worker0_list "$worker_0_epoch_list" --arg worker0_sha "$worker_0_epoch_list_sha" \
        --arg worker1_list "$worker_1_epoch_list" --arg worker1_sha "$worker_1_epoch_list_sha" \
        --argjson workers "$worker_count" --argjson epoch_count "$epoch_count" \
        --argjson epochs "$epochs_json" --argjson excluded "$excluded_epochs_json" \
        --argjson prefix "$prefix_epochs_json" --argjson prefix_proofs "$prefix_proofs_json" \
        --argjson suffix "$suffix_epochs_json" --argjson worker0 "$worker_0_epochs_json" \
        --argjson worker1 "$worker_1_epochs_json" '
        {
            schema_version: 1,
            kind: "archive-v2-pre-to-post-parallel-transition-admission",
            run_id: $run_id,
            cluster_id: $cluster,
            source_root: $source_root,
            target_root: $target_root,
            handoff_state_root: $handoff_state,
            conversion_state_root: $conversion_state,
            request: {path: $request, sha256: $request_sha},
            cohort_binding: {path: $binding, sha256: $binding_sha},
            cohort: {path: $cohort, sha256: $cohort_sha},
            master_epoch_list: {path: $list, sha256: $list_sha},
            epoch_list_snapshot: {path: $snapshot, sha256: $snapshot_sha},
            transition_fence: {path: $fence, sha256: $fence_sha, epoch: $fence_epoch},
            old_runner: {path: $old_runner, sha256: $old_runner_sha},
            parallel_runner: {path: $new_runner, sha256: $new_runner_sha},
            old_converter: {
                origin: $old_converter_origin,
                pinned: $old_converter_pinned,
                sha256: $old_converter_sha
            },
            suffix_converter: {
                origin: $suffix_converter_origin,
                pinned: $suffix_converter_pinned,
                sha256: $suffix_converter_sha
            },
            worker_count: $workers,
            epoch_count: $epoch_count,
            epochs: $epochs,
            excluded_epochs: $excluded,
            prefix_epochs: $prefix,
            prefix_attestations: $prefix_proofs,
            suffix_epochs: $suffix,
            suffix_epoch_list: {path: $suffix_list, sha256: $suffix_sha},
            workers: [
                {worker: 0, epoch_list: {path: $worker0_list, sha256: $worker0_sha}, epochs: $worker0},
                {worker: 1, epoch_list: {path: $worker1_list, sha256: $worker1_sha}, epochs: $worker1}
            ],
            claim_policy: "immutable-atomic-hard-link-never-reassigned",
            free_space_policy: "sum-of-converter-preflight-requirements-per-launch-round-v1"
        }
    ' >"$admission_candidate"
) || parallel_die 'cannot create transition admission candidate'
chmod 400 "$admission_candidate" || parallel_die 'cannot make admission candidate read-only'
if [ -e "$admission" ] || [ -L "$admission" ]; then
    [ -f "$admission" ] && [ ! -L "$admission" ] && [ ! -w "$admission" ] \
        || parallel_die 'transition admission is not one immutable real file'
    cmp -s "$admission_candidate" "$admission" \
        || parallel_die 'existing transition admission differs from exact current authority'
    rm "$admission_candidate" || parallel_die 'cannot remove duplicate admission candidate'
else
    parallel_publish_file_no_replace "$admission_candidate" "$admission" \
        || parallel_die 'cannot publish transition admission without replacement'
    rm "$admission_candidate" || parallel_die 'cannot remove linked admission candidate'
fi
admission_sha=$(sha256_file "$admission") || parallel_die 'cannot hash transition admission'

# All new epoch attestations use only the explicitly admitted suffix converter.
# Prefix validation above used the old request-bound converter identity.
converter_origin=$suffix_converter_origin
converter_sha=$suffix_converter_sha
pinned_converter=$suffix_pinned_converter

parallel_claim_epoch() {
    pce_worker=$1
    pce_epoch=$2
    pce_claim=$claims_dir/epoch-$pce_epoch.claim.json
    pce_candidate=$claims_dir/.epoch-$pce_epoch.worker-$pce_worker.claim.building-$$
    (
        set -C
        jq -cn \
            --arg run_id "$run_id" --argjson epoch "$pce_epoch" \
            --argjson worker "$pce_worker" \
            --arg admission "$admission" --arg admission_sha "$admission_sha" \
            --arg runner "$self_origin" --arg runner_sha "$self_sha" \
            --arg converter_origin "$suffix_converter_origin" \
            --arg converter_pinned "$suffix_pinned_converter" \
            --arg converter_sha "$suffix_converter_sha" \
            --arg list "$epoch_list_snapshot" --arg list_sha "$epoch_list_sha" '
            {
                schema_version: 1,
                kind: "archive-v2-pre-to-post-parallel-epoch-claim",
                run_id: $run_id,
                epoch: $epoch,
                worker: $worker,
                admission: {path: $admission, sha256: $admission_sha},
                parallel_runner: {path: $runner, sha256: $runner_sha},
                suffix_converter: {
                    origin: $converter_origin,
                    pinned: $converter_pinned,
                    sha256: $converter_sha
                },
                epoch_list_snapshot: {path: $list, sha256: $list_sha},
                assignment: "immutable-never-reassigned"
            }
        ' >"$pce_candidate"
    ) || return 1
    chmod 400 "$pce_candidate" || return 1
    if [ -e "$pce_claim" ] || [ -L "$pce_claim" ]; then
        [ -f "$pce_claim" ] && [ ! -L "$pce_claim" ] && [ ! -w "$pce_claim" ] \
            && cmp -s "$pce_candidate" "$pce_claim" || return 1
    else
        parallel_publish_file_no_replace "$pce_candidate" "$pce_claim" || return 1
    fi
    rm "$pce_candidate" || return 1
    claim_path=$pce_claim
    claim_sha=$(sha256_file "$pce_claim") || return 1
}

parallel_write_companion() {
    pwc_path=$1
    pwc_worker=$2
    pwc_epoch=$3
    pwc_claim=$4
    pwc_claim_sha=$5
    pwc_attestation=$6
    pwc_attestation_sha=$(sha256_file "$pwc_attestation") || return 1
    (
        set -C
        jq -cn \
            --arg run_id "$run_id" --argjson epoch "$pwc_epoch" \
            --argjson worker "$pwc_worker" \
            --arg admission "$admission" --arg admission_sha "$admission_sha" \
            --arg claim "$pwc_claim" --arg claim_sha "$pwc_claim_sha" \
            --arg runner "$self_origin" --arg runner_sha "$self_sha" \
            --arg converter_origin "$suffix_converter_origin" \
            --arg converter_pinned "$suffix_pinned_converter" \
            --arg converter_sha "$suffix_converter_sha" \
            --arg attestation "$pwc_attestation" --arg attestation_sha "$pwc_attestation_sha" '
            {
                schema_version: 1,
                kind: "archive-v2-pre-to-post-parallel-epoch-attestation",
                run_id: $run_id,
                epoch: $epoch,
                worker: $worker,
                admission: {path: $admission, sha256: $admission_sha},
                claim: {path: $claim, sha256: $claim_sha},
                parallel_runner: {path: $runner, sha256: $runner_sha},
                suffix_converter: {
                    origin: $converter_origin,
                    pinned: $converter_pinned,
                    sha256: $converter_sha
                },
                runner_epoch_attestation: {path: $attestation, sha256: $attestation_sha}
            }
        ' >"$pwc_path"
    )
}

parallel_validate_companion() (
    pvc_path=$1
    pvc_worker=$2
    pvc_epoch=$3
    pvc_claim=$4
    pvc_claim_sha=$5
    pvc_attestation=$6
    [ -f "$pvc_path" ] && [ ! -L "$pvc_path" ] && [ ! -w "$pvc_path" ] || exit 1
    pvc_attestation_sha=$(sha256_file "$pvc_attestation") || exit 1
    jq -e -s \
        --arg run_id "$run_id" --argjson epoch "$pvc_epoch" \
        --argjson worker "$pvc_worker" \
        --arg admission "$admission" --arg admission_sha "$admission_sha" \
        --arg claim "$pvc_claim" --arg claim_sha "$pvc_claim_sha" \
        --arg runner "$self_origin" --arg runner_sha "$self_sha" \
        --arg converter_origin "$suffix_converter_origin" \
        --arg converter_pinned "$suffix_pinned_converter" \
        --arg converter_sha "$suffix_converter_sha" \
        --arg attestation "$pvc_attestation" --arg attestation_sha "$pvc_attestation_sha" '
        length == 1 and (.[0] as $a |
            ($a | keys) == ["admission", "claim", "epoch", "kind",
                "parallel_runner", "run_id", "runner_epoch_attestation",
                "schema_version", "suffix_converter", "worker"] and
            $a.schema_version == 1 and
            $a.kind == "archive-v2-pre-to-post-parallel-epoch-attestation" and
            $a.run_id == $run_id and $a.epoch == $epoch and $a.worker == $worker and
            $a.admission == {path: $admission, sha256: $admission_sha} and
            $a.claim == {path: $claim, sha256: $claim_sha} and
            $a.parallel_runner == {path: $runner, sha256: $runner_sha} and
            $a.suffix_converter == {
                origin: $converter_origin,
                pinned: $converter_pinned,
                sha256: $converter_sha
            } and
            $a.runner_epoch_attestation == {path: $attestation, sha256: $attestation_sha})
    ' "$pvc_path" >/dev/null 2>&1
)

parallel_write_result() {
    pwr_path=$1
    pwr_worker=$2
    pwr_epoch=$3
    pwr_claim=$4
    pwr_claim_sha=$5
    pwr_attestation=$6
    pwr_companion=$7
    pwr_attestation_sha=$(sha256_file "$pwr_attestation") || return 1
    pwr_companion_sha=$(sha256_file "$pwr_companion") || return 1
    (
        set -C
        jq -cn \
            --arg run_id "$run_id" --argjson epoch "$pwr_epoch" \
            --argjson worker "$pwr_worker" \
            --arg admission "$admission" --arg admission_sha "$admission_sha" \
            --arg claim "$pwr_claim" --arg claim_sha "$pwr_claim_sha" \
            --arg converter_origin "$suffix_converter_origin" \
            --arg converter_pinned "$suffix_pinned_converter" \
            --arg converter_sha "$suffix_converter_sha" \
            --arg attestation "$pwr_attestation" --arg attestation_sha "$pwr_attestation_sha" \
            --arg companion "$pwr_companion" --arg companion_sha "$pwr_companion_sha" '
            {
                schema_version: 1,
                kind: "archive-v2-pre-to-post-parallel-epoch-result",
                run_id: $run_id,
                epoch: $epoch,
                worker: $worker,
                admission: {path: $admission, sha256: $admission_sha},
                claim: {path: $claim, sha256: $claim_sha},
                suffix_converter: {
                    origin: $converter_origin,
                    pinned: $converter_pinned,
                    sha256: $converter_sha
                },
                runner_epoch_attestation: {path: $attestation, sha256: $attestation_sha},
                parallel_epoch_attestation: {path: $companion, sha256: $companion_sha}
            }
        ' >"$pwr_path"
    )
}

parallel_validate_result() (
    pvr_path=$1
    pvr_worker=$2
    pvr_epoch=$3
    pvr_claim=$4
    pvr_claim_sha=$5
    pvr_attestation=$6
    pvr_companion=$7
    [ -f "$pvr_path" ] && [ ! -L "$pvr_path" ] && [ ! -w "$pvr_path" ] || exit 1
    pvr_attestation_sha=$(sha256_file "$pvr_attestation") || exit 1
    pvr_companion_sha=$(sha256_file "$pvr_companion") || exit 1
    jq -e -s \
        --arg run_id "$run_id" --argjson epoch "$pvr_epoch" \
        --argjson worker "$pvr_worker" \
        --arg admission "$admission" --arg admission_sha "$admission_sha" \
        --arg claim "$pvr_claim" --arg claim_sha "$pvr_claim_sha" \
        --arg converter_origin "$suffix_converter_origin" \
        --arg converter_pinned "$suffix_pinned_converter" \
        --arg converter_sha "$suffix_converter_sha" \
        --arg attestation "$pvr_attestation" --arg attestation_sha "$pvr_attestation_sha" \
        --arg companion "$pvr_companion" --arg companion_sha "$pvr_companion_sha" '
        length == 1 and (.[0] as $r |
            ($r | keys) == ["admission", "claim", "epoch", "kind",
                "parallel_epoch_attestation", "run_id", "runner_epoch_attestation",
                "schema_version", "suffix_converter", "worker"] and
            $r.schema_version == 1 and
            $r.kind == "archive-v2-pre-to-post-parallel-epoch-result" and
            $r.run_id == $run_id and $r.epoch == $epoch and $r.worker == $worker and
            $r.admission == {path: $admission, sha256: $admission_sha} and
            $r.claim == {path: $claim, sha256: $claim_sha} and
            $r.suffix_converter == {
                origin: $converter_origin,
                pinned: $converter_pinned,
                sha256: $converter_sha
            } and
            $r.runner_epoch_attestation == {path: $attestation, sha256: $attestation_sha} and
            $r.parallel_epoch_attestation == {path: $companion, sha256: $companion_sha})
    ' "$pvr_path" >/dev/null 2>&1
)

parallel_source_required_bytes() {
    psrb_epoch=$1
    psrb_source=$source_root/epoch-$psrb_epoch
    psrb_blocks=$psrb_source/$BLOCKS_FILE
    [ -f "$psrb_blocks" ] && [ ! -L "$psrb_blocks" ] || return 1
    psrb_blocks_bytes=$(wc -c <"$psrb_blocks" | tr -d '[:space:]') || return 1
    case "$psrb_blocks_bytes" in ''|*[!0-9]*) return 1 ;; esac
    psrb_copied=0
    for psrb_name in \
        archive-v2-meta.wincode registry.bin registry_counts.bin registry.mphf \
        signatures.bin genesis.bin blockhash_registry.bin blockhash_index_v3.bin \
        prev_blockhash_tail.bin vote_hash_registry.bin poh.wincode shredding.wincode \
        block-time-gaps.bin registry-first-seen.manifest registry-hot-seed.bin; do
        psrb_path=$psrb_source/$psrb_name
        if [ -e "$psrb_path" ] || [ -L "$psrb_path" ]; then
            [ -f "$psrb_path" ] && [ ! -L "$psrb_path" ] || return 1
            psrb_bytes=$(wc -c <"$psrb_path" | tr -d '[:space:]') || return 1
            case "$psrb_bytes" in ''|*[!0-9]*) return 1 ;; esac
            psrb_copied=$((psrb_copied + psrb_bytes))
        fi
    done
    psrb_margin=$((psrb_blocks_bytes / 100))
    [ "$psrb_margin" -ge 268435456 ] || psrb_margin=268435456
    [ "$psrb_margin" -le 4294967296 ] || psrb_margin=4294967296
    printf '%s\n' $((psrb_copied + psrb_blocks_bytes + psrb_margin))
}

parallel_admit_round_space() {
    pars_required=0
    for pars_epoch in "$@"; do
        [ -n "$pars_epoch" ] || continue
        pars_target=$target_root/epoch-$pars_epoch
        if [ ! -e "$pars_target" ] && [ ! -L "$pars_target" ]; then
            pars_one=$(parallel_source_required_bytes "$pars_epoch") \
                || parallel_die "cannot compute free-space admission for epoch $pars_epoch"
            pars_required=$((pars_required + pars_one))
        fi
    done
    pars_available_kib=$(df -Pk "$target_root" | awk 'NR == 2 { print $4; found = 1 }
        END { if (!found) exit 1 }') \
        || parallel_die 'cannot read target filesystem free space'
    case "$pars_available_kib" in ''|*[!0-9]*) parallel_die 'invalid target free-space value' ;; esac
    pars_available=$((pars_available_kib * 1024))
    [ "$pars_available" -ge "$pars_required" ] \
        || parallel_die "target filesystem has $pars_available free bytes; this launch round requires $pars_required combined bytes"
    echo "launch round free-space admission: available=$pars_available required=$pars_required"
}

parallel_find_recovery_attempt() {
    pfra_epoch=$1
    pfra_source=$2
    pfra_target=$3
    pfra_staging=$4
    pfra_generation=$5
    pfra_lease=$6
    pfra_root=$attempts_dir/epoch-$pfra_epoch
    recovery_report=
    recovery_log=
    recovery_count=0
    for pfra_attempt in "$pfra_root"/attempt-*; do
        if [ ! -e "$pfra_attempt" ] && [ ! -L "$pfra_attempt" ]; then
            continue
        fi
        [ -d "$pfra_attempt" ] && [ ! -L "$pfra_attempt" ] || return 1
        pfra_report=$pfra_attempt/report.json
        pfra_log=$pfra_attempt/converter.log
        [ -f "$pfra_report" ] && [ ! -L "$pfra_report" ] \
            && [ -f "$pfra_log" ] && [ ! -L "$pfra_log" ] || continue
        if validate_target_generation "$pfra_epoch" "$pfra_source" "$pfra_target" \
            "$pfra_staging" "$pfra_generation" "$pfra_lease" "$pfra_report" 0; then
            recovery_count=$((recovery_count + 1))
            recovery_report=$pfra_report
            recovery_log=$pfra_log
        fi
    done
    [ "$recovery_count" -eq 1 ]
}

parallel_publish_canonical_state() {
    ppcs_report_source=$1
    ppcs_log_source=$2
    ppcs_report=$3
    ppcs_log=$4
    chmod 400 "$ppcs_report_source" "$ppcs_log_source" || return 1
    if [ -e "$ppcs_log" ] || [ -L "$ppcs_log" ]; then
        [ -f "$ppcs_log" ] && [ ! -L "$ppcs_log" ] && [ ! -w "$ppcs_log" ] \
            && cmp -s "$ppcs_log_source" "$ppcs_log" || return 1
    else
        parallel_publish_file_no_replace "$ppcs_log_source" "$ppcs_log" || return 1
    fi
    if [ -e "$ppcs_report" ] || [ -L "$ppcs_report" ]; then
        [ -f "$ppcs_report" ] && [ ! -L "$ppcs_report" ] && [ ! -w "$ppcs_report" ] \
            && cmp -s "$ppcs_report_source" "$ppcs_report" || return 1
    else
        parallel_publish_file_no_replace "$ppcs_report_source" "$ppcs_report" || return 1
    fi
}

parallel_new_attempt_dir() {
    pnad_epoch=$1
    pnad_root=$attempts_dir/epoch-$pnad_epoch
    if [ -e "$pnad_root" ] || [ -L "$pnad_root" ]; then
        [ -d "$pnad_root" ] && [ ! -L "$pnad_root" ] || return 1
    else
        mkdir "$pnad_root" || return 1
        chmod 700 "$pnad_root" || return 1
    fi
    pnad_sequence=1
    while [ "$pnad_sequence" -le 999999 ]; do
        pnad_path=$pnad_root/attempt-$pnad_sequence
        if mkdir "$pnad_path" 2>/dev/null; then
            chmod 700 "$pnad_path" || return 1
            attempt_dir=$pnad_path
            return 0
        fi
        [ -d "$pnad_path" ] && [ ! -L "$pnad_path" ] || return 1
        pnad_sequence=$((pnad_sequence + 1))
    done
    return 1
}

parallel_epoch_job() {
    pej_worker=$1
    pej_epoch=$2

    # This function runs only in a background subshell. It must never execute
    # the coordinator EXIT trap or release the coordinator authority locks.
    trap - 0 1 2 3 15
    worker_converter_pid=
    parallel_worker_stop() {
        pws_status=$1
        trap - 0 1 2 3 15
        if [ -n "$worker_converter_pid" ]; then
            kill -TERM "$worker_converter_pid" 2>/dev/null || :
            wait "$worker_converter_pid" 2>/dev/null || :
            worker_converter_pid=
        fi
        exit "$pws_status"
    }
    trap 'parallel_worker_stop 129' 1
    trap 'parallel_worker_stop 130' 2
    trap 'parallel_worker_stop 131' 3
    trap 'parallel_worker_stop 143' 15

    pej_source=$source_root/epoch-$pej_epoch
    pej_target=$target_root/epoch-$pej_epoch
    pej_staging=$target_root/.epoch-$pej_epoch.pre-to-post.staging
    pej_report=$conversion_state/epoch-$pej_epoch.json
    pej_log=$conversion_state/epoch-$pej_epoch.log
    pej_attestation=$conversion_state/epoch-$pej_epoch.attestation.json
    pej_companion=$conversion_state/epoch-$pej_epoch.parallel-attestation.json
    pej_result=$results_dir/epoch-$pej_epoch.json
    pej_generation=archive-v2-pre-to-post-$run_id-epoch-$pej_epoch-post
    pej_lease=archive-v2-pre-to-post-$run_id-epoch-$pej_epoch-source-leases

    [ -d "$pej_source" ] && [ ! -L "$pej_source" ] \
        || parallel_die "worker $pej_worker epoch $pej_epoch source is absent"
    [ ! -e "$pej_staging" ] && [ ! -L "$pej_staging" ] \
        || parallel_die "worker $pej_worker epoch $pej_epoch staging needs review"
    parallel_claim_epoch "$pej_worker" "$pej_epoch" \
        || parallel_die "worker $pej_worker epoch $pej_epoch cannot publish or validate its immutable claim"
    pej_claim=$claim_path
    pej_claim_sha=$claim_sha

    if [ -e "$pej_result" ] || [ -L "$pej_result" ]; then
        validate_target_generation "$pej_epoch" "$pej_source" "$pej_target" \
            "$pej_staging" "$pej_generation" "$pej_lease" "$pej_report" 0 \
            || parallel_die "worker $pej_worker epoch $pej_epoch completed target changed"
        validate_epoch_attestation "$pej_attestation" "$pej_epoch" "$pej_source" \
            "$pej_target" "$pej_staging" "$pej_generation" "$pej_lease" \
            "$pej_report" "$pej_log" \
            || parallel_die "worker $pej_worker epoch $pej_epoch runner attestation changed"
        parallel_validate_companion "$pej_companion" "$pej_worker" "$pej_epoch" \
            "$pej_claim" "$pej_claim_sha" "$pej_attestation" \
            || parallel_die "worker $pej_worker epoch $pej_epoch companion attestation changed"
        parallel_validate_result "$pej_result" "$pej_worker" "$pej_epoch" \
            "$pej_claim" "$pej_claim_sha" "$pej_attestation" "$pej_companion" \
            || parallel_die "worker $pej_worker epoch $pej_epoch result changed"
        echo "worker $pej_worker epoch $pej_epoch: accepted completed claimed target"
        exit 0
    fi

    pej_had_target=0
    pej_report_source=
    pej_log_source=
    if [ -e "$pej_target" ] || [ -L "$pej_target" ]; then
        pej_had_target=1
        [ -d "$pej_target" ] && [ ! -L "$pej_target" ] \
            || parallel_die "worker $pej_worker epoch $pej_epoch target is not one real directory"
        if [ -f "$pej_report" ] && [ ! -L "$pej_report" ] \
            && [ -f "$pej_log" ] && [ ! -L "$pej_log" ]; then
            pej_report_source=$pej_report
            pej_log_source=$pej_log
        else
            parallel_find_recovery_attempt "$pej_epoch" "$pej_source" "$pej_target" \
                "$pej_staging" "$pej_generation" "$pej_lease" \
                || parallel_die "worker $pej_worker epoch $pej_epoch target has no unique valid recovery attempt"
            pej_report_source=$recovery_report
            pej_log_source=$recovery_log
            parallel_publish_canonical_state "$pej_report_source" "$pej_log_source" \
                "$pej_report" "$pej_log" \
                || parallel_die "worker $pej_worker epoch $pej_epoch cannot recover canonical report and log"
        fi
    else
        for pej_absent in "$pej_report" "$pej_log" "$pej_attestation" "$pej_companion"; do
            [ ! -e "$pej_absent" ] && [ ! -L "$pej_absent" ] \
                || parallel_die "worker $pej_worker epoch $pej_epoch state exists without target: $pej_absent"
        done
        [ "$(sha256_file "$pinned_converter")" = "$converter_sha" ] \
            || parallel_die "worker $pej_worker epoch $pej_epoch pinned converter changed"
        parallel_new_attempt_dir "$pej_epoch" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot create an attempt directory"
        pej_report_source=$attempt_dir/report.json
        pej_log_source=$attempt_dir/converter.log
        echo "worker $pej_worker epoch $pej_epoch: converting legacy Pre to canonical Post"
        (
            set -C
            exec nice -n 19 ionice -c 3 /dev/fd/4 \
                --source "$pej_source" \
                --source-lease-id "$pej_lease" \
                --target "$pej_target" \
                --staging "$pej_staging" \
                --epoch "$pej_epoch" \
                --cluster-id "$cluster_id" \
                --generation-id "$pej_generation" \
                >"$pej_report_source" 2>"$pej_log_source"
        ) &
        worker_converter_pid=$!
        if wait "$worker_converter_pid"; then
            pej_converter_status=0
        else
            pej_converter_status=$?
        fi
        worker_converter_pid=
        [ "$pej_converter_status" -eq 0 ] \
            || parallel_die "worker $pej_worker epoch $pej_epoch converter exited with status $pej_converter_status"
        validate_target_generation "$pej_epoch" "$pej_source" "$pej_target" \
            "$pej_staging" "$pej_generation" "$pej_lease" "$pej_report_source" 0 \
            || parallel_die "worker $pej_worker epoch $pej_epoch converter result is invalid"
        parallel_publish_canonical_state "$pej_report_source" "$pej_log_source" \
            "$pej_report" "$pej_log" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot publish canonical report and log"
    fi

    validate_target_generation "$pej_epoch" "$pej_source" "$pej_target" \
        "$pej_staging" "$pej_generation" "$pej_lease" "$pej_report" 0 \
        || parallel_die "worker $pej_worker epoch $pej_epoch canonical target validation failed"

    if [ -e "$pej_attestation" ] || [ -L "$pej_attestation" ]; then
        validate_epoch_attestation "$pej_attestation" "$pej_epoch" "$pej_source" \
            "$pej_target" "$pej_staging" "$pej_generation" "$pej_lease" \
            "$pej_report" "$pej_log" \
            || parallel_die "worker $pej_worker epoch $pej_epoch existing runner attestation is invalid"
    else
        parallel_new_attempt_dir "$pej_epoch" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot create attestation attempt"
        pej_attestation_candidate=$attempt_dir/runner-attestation.json
        write_epoch_attestation "$pej_attestation_candidate" "$pej_epoch" "$pej_source" \
            "$pej_target" "$pej_staging" "$pej_generation" "$pej_lease" \
            "$pej_report" "$pej_log" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot create runner attestation"
        chmod 400 "$pej_attestation_candidate" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot protect runner attestation"
        validate_epoch_attestation "$pej_attestation_candidate" "$pej_epoch" "$pej_source" \
            "$pej_target" "$pej_staging" "$pej_generation" "$pej_lease" \
            "$pej_report" "$pej_log" \
            || parallel_die "worker $pej_worker epoch $pej_epoch generated invalid runner attestation"
        parallel_publish_file_no_replace "$pej_attestation_candidate" "$pej_attestation" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot publish runner attestation"
    fi

    if [ -e "$pej_companion" ] || [ -L "$pej_companion" ]; then
        parallel_validate_companion "$pej_companion" "$pej_worker" "$pej_epoch" \
            "$pej_claim" "$pej_claim_sha" "$pej_attestation" \
            || parallel_die "worker $pej_worker epoch $pej_epoch existing companion is invalid"
    else
        parallel_new_attempt_dir "$pej_epoch" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot create companion attempt"
        pej_companion_candidate=$attempt_dir/parallel-attestation.json
        parallel_write_companion "$pej_companion_candidate" "$pej_worker" "$pej_epoch" \
            "$pej_claim" "$pej_claim_sha" "$pej_attestation" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot create companion attestation"
        chmod 400 "$pej_companion_candidate" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot protect companion attestation"
        parallel_validate_companion "$pej_companion_candidate" "$pej_worker" "$pej_epoch" \
            "$pej_claim" "$pej_claim_sha" "$pej_attestation" \
            || parallel_die "worker $pej_worker epoch $pej_epoch generated invalid companion attestation"
        parallel_publish_file_no_replace "$pej_companion_candidate" "$pej_companion" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot publish companion attestation"
    fi

    if [ -e "$pej_result" ] || [ -L "$pej_result" ]; then
        parallel_validate_result "$pej_result" "$pej_worker" "$pej_epoch" \
            "$pej_claim" "$pej_claim_sha" "$pej_attestation" "$pej_companion" \
            || parallel_die "worker $pej_worker epoch $pej_epoch existing result is invalid"
    else
        parallel_new_attempt_dir "$pej_epoch" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot create result attempt"
        pej_result_candidate=$attempt_dir/result.json
        parallel_write_result "$pej_result_candidate" "$pej_worker" "$pej_epoch" \
            "$pej_claim" "$pej_claim_sha" "$pej_attestation" "$pej_companion" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot create result"
        chmod 400 "$pej_result_candidate" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot protect result"
        parallel_validate_result "$pej_result_candidate" "$pej_worker" "$pej_epoch" \
            "$pej_claim" "$pej_claim_sha" "$pej_attestation" "$pej_companion" \
            || parallel_die "worker $pej_worker epoch $pej_epoch generated invalid result"
        parallel_publish_file_no_replace "$pej_result_candidate" "$pej_result" \
            || parallel_die "worker $pej_worker epoch $pej_epoch cannot publish result"
    fi
    echo "worker $pej_worker epoch $pej_epoch: canonical Post result complete"
    exit 0
}

exec 4<"$pinned_converter" || parallel_die 'cannot pin converter descriptor'
[ -e /dev/fd/4 ] || parallel_die '/dev/fd does not expose the pinned converter descriptor'
[ "$(sha256_file /dev/fd/4)" = "$converter_sha" ] \
    || parallel_die 'pinned converter descriptor differs from transition admission'

exec 6<"$worker_0_epoch_list" || parallel_die 'cannot open worker-0 epoch list'
exec 7<"$worker_1_epoch_list" || parallel_die 'cannot open worker-1 epoch list'
parallel_failed=0
while :; do
    worker_0_epoch=
    worker_1_epoch=
    if IFS= read -r worker_0_epoch <&6; then
        worker_0_has=1
    else
        worker_0_has=0
    fi
    if IFS= read -r worker_1_epoch <&7; then
        worker_1_has=1
    else
        worker_1_has=0
    fi
    [ "$worker_0_has" -eq 1 ] || [ "$worker_1_has" -eq 1 ] || break

    parallel_admit_round_space "$worker_0_epoch" "$worker_1_epoch"
    if [ "$worker_0_has" -eq 1 ]; then
        (parallel_epoch_job 0 "$worker_0_epoch") &
        job_pid_0=$!
    fi
    if [ "$worker_1_has" -eq 1 ]; then
        (parallel_epoch_job 1 "$worker_1_epoch") &
        job_pid_1=$!
    fi

    worker_0_status=0
    worker_1_status=0
    if [ -n "$job_pid_0" ]; then
        wait "$job_pid_0" || worker_0_status=$?
        job_pid_0=
    fi
    # A failed worker does not terminate its active sibling. The sibling can
    # publish its current atomic epoch before the coordinator stops new work.
    if [ -n "$job_pid_1" ]; then
        wait "$job_pid_1" || worker_1_status=$?
        job_pid_1=
    fi
    if [ "$worker_0_status" -ne 0 ] || [ "$worker_1_status" -ne 0 ]; then
        parallel_failed=1
        echo "parallel round failed: worker0=$worker_0_status worker1=$worker_1_status" >&2
        break
    fi
done
exec 6<&-
exec 7<&-
[ "$parallel_failed" -eq 0 ] || parallel_die 'parallel transition stopped after a worker failure'

# Assemble the final proof in the immutable master-list order. Prefix rows
# refer only to the untouched sequential attestations. Suffix rows must include
# the immutable claim and the parallel companion attestation.
producer_rows_json='[]'
epoch_attestations_json='[]'
final_prefix_index=0
final_suffix_index=0
exec 3<"$epoch_list_snapshot" || parallel_die 'cannot reopen master epoch list for completion'
while IFS= read -r final_epoch <&3 || [ -n "$final_epoch" ]; do
    final_source=$source_root/epoch-$final_epoch
    final_target=$target_root/epoch-$final_epoch
    final_staging=$target_root/.epoch-$final_epoch.pre-to-post.staging
    final_report=$conversion_state/epoch-$final_epoch.json
    final_log=$conversion_state/epoch-$final_epoch.log
    final_attestation=$conversion_state/epoch-$final_epoch.attestation.json
    final_generation=archive-v2-pre-to-post-$run_id-epoch-$final_epoch-post
    final_lease=archive-v2-pre-to-post-$run_id-epoch-$final_epoch-source-leases
    if [ "$final_prefix_index" -lt "$prefix_count" ]; then
        final_expected_prefix_epoch=$(jq -r --argjson index "$final_prefix_index" \
            '.[$index].epoch' <<EOF
$prefix_proofs_json
EOF
        ) || parallel_die 'cannot read prefix proof order'
        [ "$final_epoch" = "$final_expected_prefix_epoch" ] \
            || parallel_die 'master order differs from the admitted sequential prefix'
        final_is_prefix=1
        converter_origin=$old_converter_origin
        converter_sha=$old_converter_sha
        pinned_converter=$old_pinned_converter
    else
        final_expected_prefix_epoch=
        final_is_prefix=0
        converter_origin=$suffix_converter_origin
        converter_sha=$suffix_converter_sha
        pinned_converter=$suffix_pinned_converter
    fi
    validate_target_generation "$final_epoch" "$final_source" "$final_target" \
        "$final_staging" "$final_generation" "$final_lease" "$final_report" 0 \
        || parallel_die "final target validation failed: epoch $final_epoch"
    validate_epoch_attestation "$final_attestation" "$final_epoch" "$final_source" \
        "$final_target" "$final_staging" "$final_generation" "$final_lease" \
        "$final_report" "$final_log" \
        || parallel_die "final runner-attestation validation failed: epoch $final_epoch"
    final_attestation_sha=$(sha256_file "$final_attestation") \
        || parallel_die "cannot hash final runner attestation: epoch $final_epoch"
    epoch_attestations_json=$(jq -cn --argjson current "$epoch_attestations_json" \
        --argjson epoch "$final_epoch" --arg path "$final_attestation" \
        --arg sha "$final_attestation_sha" \
        '$current + [{epoch: $epoch, path: $path, sha256: $sha}]') \
        || parallel_die 'cannot assemble ordered runner-attestation rows'

    if [ "$final_is_prefix" -eq 1 ]; then
        final_expected_prefix_sha=$(jq -r --argjson index "$final_prefix_index" \
            '.[$index].attestation.sha256' <<EOF
$prefix_proofs_json
EOF
        ) || parallel_die 'cannot read prefix proof hash'
        [ "$final_attestation_sha" = "$final_expected_prefix_sha" ] \
            || parallel_die "sequential prefix attestation changed: epoch $final_epoch"
        producer_rows_json=$(jq -cn --argjson current "$producer_rows_json" \
            --argjson epoch "$final_epoch" --arg old_runner "$old_runner_origin" \
            --arg old_runner_sha "$old_runner_sha" --arg attestation "$final_attestation" \
            --arg attestation_sha "$final_attestation_sha" \
            --arg converter_origin "$old_converter_origin" \
            --arg converter_pinned "$old_pinned_converter" \
            --arg converter_sha "$old_converter_sha" '
            $current + [{
                epoch: $epoch,
                phase: "sequential-prefix",
                worker: null,
                producer: {path: $old_runner, sha256: $old_runner_sha},
                converter: {
                    origin: $converter_origin,
                    pinned: $converter_pinned,
                    sha256: $converter_sha
                },
                runner_epoch_attestation: {path: $attestation, sha256: $attestation_sha},
                claim: null,
                parallel_epoch_attestation: null
            }]
        ') || parallel_die 'cannot assemble sequential producer row'
        final_prefix_index=$((final_prefix_index + 1))
    else
        final_worker=$((final_suffix_index % worker_count))
        final_claim=$claims_dir/epoch-$final_epoch.claim.json
        final_companion=$conversion_state/epoch-$final_epoch.parallel-attestation.json
        final_result=$results_dir/epoch-$final_epoch.json
        [ -f "$final_claim" ] && [ ! -L "$final_claim" ] && [ ! -w "$final_claim" ] \
            || parallel_die "final immutable claim is absent: epoch $final_epoch"
        final_claim_sha=$(sha256_file "$final_claim") \
            || parallel_die "cannot hash final claim: epoch $final_epoch"
        parallel_validate_companion "$final_companion" "$final_worker" "$final_epoch" \
            "$final_claim" "$final_claim_sha" "$final_attestation" \
            || parallel_die "final companion validation failed: epoch $final_epoch"
        parallel_validate_result "$final_result" "$final_worker" "$final_epoch" \
            "$final_claim" "$final_claim_sha" "$final_attestation" "$final_companion" \
            || parallel_die "final result validation failed: epoch $final_epoch"
        final_companion_sha=$(sha256_file "$final_companion") \
            || parallel_die "cannot hash final companion: epoch $final_epoch"
        producer_rows_json=$(jq -cn --argjson current "$producer_rows_json" \
            --argjson epoch "$final_epoch" --argjson worker "$final_worker" \
            --arg runner "$self_origin" --arg runner_sha "$self_sha" \
            --arg attestation "$final_attestation" --arg attestation_sha "$final_attestation_sha" \
            --arg claim "$final_claim" --arg claim_sha "$final_claim_sha" \
            --arg companion "$final_companion" --arg companion_sha "$final_companion_sha" \
            --arg converter_origin "$suffix_converter_origin" \
            --arg converter_pinned "$suffix_pinned_converter" \
            --arg converter_sha "$suffix_converter_sha" '
            $current + [{
                epoch: $epoch,
                phase: "parallel-suffix",
                worker: $worker,
                producer: {path: $runner, sha256: $runner_sha},
                converter: {
                    origin: $converter_origin,
                    pinned: $converter_pinned,
                    sha256: $converter_sha
                },
                runner_epoch_attestation: {path: $attestation, sha256: $attestation_sha},
                claim: {path: $claim, sha256: $claim_sha},
                parallel_epoch_attestation: {path: $companion, sha256: $companion_sha}
            }]
        ') || parallel_die 'cannot assemble parallel producer row'
        final_suffix_index=$((final_suffix_index + 1))
    fi
done
exec 3<&-
[ "$final_prefix_index" -eq "$prefix_count" ] \
    && [ "$final_suffix_index" -eq "$suffix_count" ] \
    || parallel_die 'final producer phases do not cover the exact master cohort'
[ "$(jq 'length' <<EOF
$producer_rows_json
EOF
)" -eq "$epoch_count" ] || parallel_die 'final producer row count is inconsistent'

converter_origin=$suffix_converter_origin
converter_sha=$suffix_converter_sha
pinned_converter=$suffix_pinned_converter

# Recheck all immutable authority inputs immediately before publication.
[ "$(sha256_file "$request")" = "$request_sha" ] || parallel_die 'request changed during transition'
[ "$(sha256_file "$cohort_binding")" = "$cohort_binding_sha" ] \
    || parallel_die 'cohort binding changed during transition'
[ "$(sha256_file "$cohort_json")" = "$cohort_json_sha" ] \
    || parallel_die 'cohort JSON changed during transition'
[ "$(sha256_file "$master_epoch_list")" = "$master_epoch_list_sha" ] \
    || parallel_die 'master epoch list changed during transition'
[ "$(sha256_file "$epoch_list_snapshot")" = "$epoch_list_sha" ] \
    || parallel_die 'epoch-list snapshot changed during transition'
[ "$(sha256_file "$fence_path")" = "$fence_sha" ] \
    || parallel_die 'transition fence changed during transition'
[ "$(sha256_file "$admission")" = "$admission_sha" ] \
    || parallel_die 'transition admission changed during transition'
[ "$(sha256_file "$old_runner_origin")" = "$old_runner_sha" ] \
    || parallel_die 'old runner changed during transition'
[ "$(sha256_file "$self_origin")" = "$self_sha" ] \
    || parallel_die 'parallel runner changed during transition'
[ "$(sha256_file "$old_converter_origin")" = "$old_converter_sha" ] \
    || parallel_die 'old converter origin changed during transition'
[ "$(sha256_file "$old_pinned_converter")" = "$old_converter_sha" ] \
    || parallel_die 'old pinned converter changed during transition'
[ "$(sha256_file "$suffix_converter_origin")" = "$suffix_converter_sha" ] \
    || parallel_die 'suffix converter origin changed during transition'
[ "$(sha256_file "$suffix_pinned_converter")" = "$suffix_converter_sha" ] \
    || parallel_die 'suffix pinned converter changed during transition'

transition_complete=$conversion_state/parallel-transition-complete.json
if [ -e "$transition_complete" ] || [ -L "$transition_complete" ]; then
    [ -f "$transition_complete" ] && [ ! -L "$transition_complete" ] \
        && [ ! -w "$transition_complete" ] \
        || parallel_die 'transition completion path is not one immutable real file'
    jq -e -s \
        --arg run_id "$run_id" --arg cluster "$cluster_id" \
        --arg admission "$admission" --arg admission_sha "$admission_sha" \
        --arg old_runner "$old_runner_origin" --arg old_runner_sha "$old_runner_sha" \
        --arg runner "$self_origin" --arg runner_sha "$self_sha" \
        --arg old_converter_origin "$old_converter_origin" \
        --arg old_converter_pinned "$old_pinned_converter" \
        --arg old_converter_sha "$old_converter_sha" \
        --arg converter_origin "$converter_origin" --arg converter_pinned "$pinned_converter" \
        --arg converter_sha "$converter_sha" --arg source_root "$source_root" \
        --arg target_root "$target_root" --arg state_root "$conversion_state" \
        --arg epoch_list "$master_epoch_list" --arg snapshot "$epoch_list_snapshot" \
        --arg snapshot_sha "$epoch_list_sha" --argjson workers "$worker_count" \
        --argjson epoch_count "$epoch_count" --argjson epochs "$epochs_json" \
        --argjson producers "$producer_rows_json" '
        length == 1 and (.[0] as $c |
            ($c | keys) == ["admission", "cluster_id", "completed_at_utc",
                "completed_epochs", "converter_origin", "converter_pinned",
                "converter_sha256", "epoch_count", "epoch_list",
                "epoch_list_sha256", "epoch_list_snapshot", "epoch_producers",
                "epochs", "kind", "old_converter", "old_runner",
                "parallel_runner", "run_id", "schema_version", "source_root",
                "state_root", "suffix_converter", "target_root", "worker_count"] and
            $c.schema_version == 1 and
            $c.kind == "archive-v2-pre-to-post-parallel-transition-complete" and
            $c.run_id == $run_id and $c.cluster_id == $cluster and
            $c.admission == {path: $admission, sha256: $admission_sha} and
            $c.old_runner == {path: $old_runner, sha256: $old_runner_sha} and
            $c.parallel_runner == {path: $runner, sha256: $runner_sha} and
            $c.old_converter == {
                origin: $old_converter_origin,
                pinned: $old_converter_pinned,
                sha256: $old_converter_sha
            } and
            $c.suffix_converter == {
                origin: $converter_origin,
                pinned: $converter_pinned,
                sha256: $converter_sha
            } and
            $c.converter_origin == $converter_origin and
            $c.converter_pinned == $converter_pinned and
            $c.converter_sha256 == $converter_sha and
            $c.source_root == $source_root and $c.target_root == $target_root and
            $c.state_root == $state_root and $c.epoch_list == $epoch_list and
            $c.epoch_list_snapshot == $snapshot and
            $c.epoch_list_sha256 == $snapshot_sha and
            $c.worker_count == $workers and $c.epoch_count == $epoch_count and
            $c.completed_epochs == $epoch_count and $c.epochs == $epochs and
            $c.epoch_producers == $producers and
            ($c.completed_at_utc | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")))
    ' "$transition_complete" >/dev/null 2>&1 \
        || parallel_die 'existing transition completion record is invalid or changed'
else
    transition_complete_candidate=$transition_complete.building-$$
    transition_completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
        || parallel_die 'cannot get transition completion time'
    (
        set -C
        jq -cn \
            --arg run_id "$run_id" --arg cluster "$cluster_id" \
            --arg admission "$admission" --arg admission_sha "$admission_sha" \
            --arg old_runner "$old_runner_origin" --arg old_runner_sha "$old_runner_sha" \
            --arg runner "$self_origin" --arg runner_sha "$self_sha" \
            --arg old_converter_origin "$old_converter_origin" \
            --arg old_converter_pinned "$old_pinned_converter" \
            --arg old_converter_sha "$old_converter_sha" \
            --arg converter_origin "$converter_origin" --arg converter_pinned "$pinned_converter" \
            --arg converter_sha "$converter_sha" --arg source_root "$source_root" \
            --arg target_root "$target_root" --arg state_root "$conversion_state" \
            --arg epoch_list "$master_epoch_list" --arg snapshot "$epoch_list_snapshot" \
            --arg snapshot_sha "$epoch_list_sha" --argjson workers "$worker_count" \
            --argjson epoch_count "$epoch_count" --argjson epochs "$epochs_json" \
            --argjson producers "$producer_rows_json" --arg completed_at "$transition_completed_at" '
            {
                schema_version: 1,
                kind: "archive-v2-pre-to-post-parallel-transition-complete",
                run_id: $run_id,
                cluster_id: $cluster,
                admission: {path: $admission, sha256: $admission_sha},
                old_runner: {path: $old_runner, sha256: $old_runner_sha},
                parallel_runner: {path: $runner, sha256: $runner_sha},
                old_converter: {
                    origin: $old_converter_origin,
                    pinned: $old_converter_pinned,
                    sha256: $old_converter_sha
                },
                suffix_converter: {
                    origin: $converter_origin,
                    pinned: $converter_pinned,
                    sha256: $converter_sha
                },
                converter_origin: $converter_origin,
                converter_pinned: $converter_pinned,
                converter_sha256: $converter_sha,
                source_root: $source_root,
                target_root: $target_root,
                state_root: $state_root,
                epoch_list: $epoch_list,
                epoch_list_snapshot: $snapshot,
                epoch_list_sha256: $snapshot_sha,
                worker_count: $workers,
                epoch_count: $epoch_count,
                completed_epochs: $epoch_count,
                epochs: $epochs,
                epoch_producers: $producers,
                completed_at_utc: $completed_at
            }
        ' >"$transition_complete_candidate"
    ) || parallel_die 'cannot create transition completion candidate'
    chmod 400 "$transition_complete_candidate" \
        || parallel_die 'cannot protect transition completion candidate'
    parallel_publish_file_no_replace "$transition_complete_candidate" "$transition_complete" \
        || parallel_die 'cannot publish transition completion without replacement'
    rm "$transition_complete_candidate" \
        || parallel_die 'cannot remove linked transition completion candidate'
fi
transition_complete_sha=$(sha256_file "$transition_complete") \
    || parallel_die 'cannot hash transition completion'

# This is the replacement for the stale sequential handoff validator. It binds
# the transition-aware completion and uses the hardened converter_origin,
# converter_pinned, and epoch_list_snapshot fields.
handoff_complete=$handoff_state_root/complete.json
if [ -e "$handoff_complete" ] || [ -L "$handoff_complete" ]; then
    [ -f "$handoff_complete" ] && [ ! -L "$handoff_complete" ] \
        || parallel_die 'handoff completion path is not one real file'
    jq -e -s \
        --arg run_id "$run_id" --arg request_sha "$request_sha" \
        --arg binding_sha "$cohort_binding_sha" \
        --arg admission_sha "$admission_sha" --arg conversion_sha "$transition_complete_sha" \
        --arg old_converter_origin "$old_converter_origin" \
        --arg old_converter_pinned "$old_pinned_converter" \
        --arg old_converter_sha "$old_converter_sha" \
        --arg converter_origin "$converter_origin" --arg converter_pinned "$pinned_converter" \
        --arg converter_sha "$converter_sha" --arg snapshot "$epoch_list_snapshot" \
        --arg snapshot_sha "$epoch_list_sha" --argjson epoch_count "$epoch_count" '
        length == 1 and (.[0] as $h |
            ($h | keys) == ["cohort_binding_sha256", "completed_at_utc",
                "conversion_complete_sha256", "conversion_epoch_count",
                "converter_origin", "converter_pinned", "converter_sha256",
                "epoch_list_sha256", "epoch_list_snapshot", "kind",
                "old_converter_origin", "old_converter_pinned",
                "old_converter_sha256", "request_sha256", "run_id",
                "schema_version", "transition_admission_sha256"] and
            $h.schema_version == 1 and
            $h.kind == "archive-v2-pre-to-post-parallel-transition-handoff-complete" and
            $h.run_id == $run_id and $h.request_sha256 == $request_sha and
            $h.cohort_binding_sha256 == $binding_sha and
            $h.transition_admission_sha256 == $admission_sha and
            $h.conversion_complete_sha256 == $conversion_sha and
            $h.old_converter_origin == $old_converter_origin and
            $h.old_converter_pinned == $old_converter_pinned and
            $h.old_converter_sha256 == $old_converter_sha and
            $h.converter_origin == $converter_origin and
            $h.converter_pinned == $converter_pinned and
            $h.converter_sha256 == $converter_sha and
            $h.epoch_list_snapshot == $snapshot and
            $h.epoch_list_sha256 == $snapshot_sha and
            $h.conversion_epoch_count == $epoch_count and
            ($h.completed_at_utc | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")))
    ' "$handoff_complete" >/dev/null 2>&1 \
        || parallel_die 'existing handoff completion does not match transition authority'
else
    handoff_complete_candidate=$handoff_complete.building-$$
    handoff_completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
        || parallel_die 'cannot get handoff completion time'
    (
        set -C
        jq -cn \
            --arg run_id "$run_id" --arg request_sha "$request_sha" \
            --arg binding_sha "$cohort_binding_sha" \
            --arg admission_sha "$admission_sha" --arg conversion_sha "$transition_complete_sha" \
            --arg old_converter_origin "$old_converter_origin" \
            --arg old_converter_pinned "$old_pinned_converter" \
            --arg old_converter_sha "$old_converter_sha" \
            --arg converter_origin "$converter_origin" --arg converter_pinned "$pinned_converter" \
            --arg converter_sha "$converter_sha" --arg snapshot "$epoch_list_snapshot" \
            --arg snapshot_sha "$epoch_list_sha" --argjson epoch_count "$epoch_count" \
            --arg completed_at "$handoff_completed_at" '
            {
                schema_version: 1,
                kind: "archive-v2-pre-to-post-parallel-transition-handoff-complete",
                run_id: $run_id,
                request_sha256: $request_sha,
                cohort_binding_sha256: $binding_sha,
                transition_admission_sha256: $admission_sha,
                conversion_complete_sha256: $conversion_sha,
                old_converter_origin: $old_converter_origin,
                old_converter_pinned: $old_converter_pinned,
                old_converter_sha256: $old_converter_sha,
                converter_origin: $converter_origin,
                converter_pinned: $converter_pinned,
                converter_sha256: $converter_sha,
                epoch_list_snapshot: $snapshot,
                epoch_list_sha256: $snapshot_sha,
                conversion_epoch_count: $epoch_count,
                completed_at_utc: $completed_at
            }
        ' >"$handoff_complete_candidate"
    ) || parallel_die 'cannot create transition handoff completion candidate'
    chmod 400 "$handoff_complete_candidate" \
        || parallel_die 'cannot protect transition handoff completion candidate'
    parallel_publish_file_no_replace "$handoff_complete_candidate" "$handoff_complete" \
        || parallel_die 'cannot publish transition handoff completion without replacement'
    rm "$handoff_complete_candidate" \
        || parallel_die 'cannot remove linked handoff completion candidate'
fi

echo "all $epoch_count exact cohort epochs are canonical Post; transition completion: $transition_complete"
