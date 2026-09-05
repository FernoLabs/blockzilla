#!/bin/sh
# Run one unprocessed LegacyPre epoch through the multicore fast converter.
# This is a benchmark canary only. It publishes a non-canonical candidate and
# keeps the old blocks/index pair in the converter backup directory.

set -u
umask 077

cr_die() {
    echo "archive-v2 fast canary: $*" >&2
    exit 1
}

cr_usage() {
    echo "usage: $0 HANDOFF_STATE_ROOT EPOCH CONVERTER THREADS" >&2
    exit 2
}

cr_command() {
    command -v "$1" >/dev/null 2>&1 || cr_die "required command is absent: $1"
}

cr_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

cr_file() (
    cr_parent=$(cr_directory "$(dirname "$1")") || exit 1
    printf '%s/%s\n' "$cr_parent" "$(basename "$1")"
)

cr_absolute() {
    case "$2" in
        /*) ;;
        *) cr_die "$1 must be an absolute path: $2" ;;
    esac
    case "$2" in *'
'*) cr_die "$1 must be one line" ;; esac
}

cr_decimal() {
    case "$2" in
        0) ;;
        ''|*[!0-9]*|0*) cr_die "$1 is not a canonical decimal integer" ;;
    esac
}

cr_sha256() (
    if [ "$sha256_program" = sha256sum ]; then
        cr_digest_output=$(sha256sum "$1") || exit 1
    else
        cr_digest_output=$(shasum -a 256 "$1") || exit 1
    fi
    printf '%s\n' "$cr_digest_output" | awk '
        NR == 1 && length($1) == 64 && $1 !~ /[^0-9a-f]/ {
            print $1; valid = 1
        }
        END { if (!valid) exit 1 }
    '
)

cr_json() {
    jq -er "$1" "$2" 2>/dev/null || cr_die "invalid $1 in $2"
}

cr_table_row() {
    awk -F '\t' -v epoch="$epoch" '
        $1 == epoch { count++; row = $0 }
        END { if (count != 1) exit 1; print row }
    ' "$audit_table"
}

cr_process_guard() {
    cr_stage=$1
    cr_snapshot=$handoff_state_root/.fast-canary-processes-$cr_stage-$$
    ps -eo pid=,args= >"$cr_snapshot" || return 1
    if awk -v self="$$" -v archive="$archive_root" '
        function names_archive(line, path, at, tail) {
            while ((at = index(line, path)) != 0) {
                tail = substr(line, at + length(path), 1)
                if (tail == "" || tail == "/" || tail == " " || tail == "\t") return 1
                line = substr(line, at + length(path))
            }
            return 0
        }
        $1 != self &&
        (names_archive($0, archive) ||
         index($0, "run-archive-v2-pre-to-post-fast-in-place") ||
         index($0, "run-archive-v2-pre-to-post-fast-multicore") ||
         (index($0, "archive-v2-pre-to-post") && index($0, "--fast-candidate"))) {
            print > "/dev/stderr"; found = 1
        }
        END { exit found ? 0 : 1 }
    ' "$cr_snapshot"; then
        rm "$cr_snapshot" || :
        return 1
    fi
    rm "$cr_snapshot"
}

cr_validate_result() {
    [ -f "$result" ] && [ ! -L "$result" ] || return 1
    jq -e --argjson epoch "$epoch" --argjson threads "$threads" \
        --arg source "$source" --arg backup "$backup" --arg generation "$generation" \
        --arg audit "$audit_report" --arg audit_sha "$audit_sha" \
        --argjson source_bytes "$audit_compressed" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-fast-canary-result" and
        .epoch == $epoch and .threads == $threads and
        .source == $source and .backup == $backup and
        .prospective_generation_id == $generation and
        .source_audit == {path:$audit,sha256:$audit_sha} and
        (.elapsed_seconds | type == "number" and . >= 0) and
        .source_compressed_bytes == $source_bytes and
        (.source_mib_per_second | type == "number" and . >= 0) and
        (.recovered_switch | type == "boolean") and
        (.converter_report | type == "string" and length > 0) and
        .canonical == false
    ' "$result" >/dev/null 2>&1
}

cr_validate_candidate() {
    cr_report=$1
    descriptor=$source/archive-v2-pre-to-post.candidate.v1.json
    intent=$backup/archive-v2-pre-to-post.switch-intent.v1.json
    complete=$backup/archive-v2-pre-to-post.switch-complete.v1.json
    [ -d "$source" ] && [ ! -L "$source" ] \
        && [ -d "$backup" ] && [ ! -L "$backup" ] \
        && [ ! -e "$staging" ] && [ ! -L "$staging" ] \
        && [ -f "$descriptor" ] && [ ! -L "$descriptor" ] \
        && [ -f "$intent" ] && [ ! -L "$intent" ] \
        && [ -f "$complete" ] && [ ! -L "$complete" ] || return 1
    for cr_pair in \
        "$source/archive-v2-blocks.zstd" "$source/archive-v2-blocks.index" \
        "$backup/archive-v2-blocks.zstd" "$backup/archive-v2-blocks.index"; do
        [ -f "$cr_pair" ] && [ ! -L "$cr_pair" ] || return 1
    done
    cr_backup_blocks=$(wc -c <"$backup/archive-v2-blocks.zstd" | tr -d '[:space:]') \
        || return 1
    cr_backup_index=$(wc -c <"$backup/archive-v2-blocks.index" | tr -d '[:space:]') \
        || return 1
    [ "$cr_backup_blocks" -eq "$audit_compressed" ] \
        && [ "$cr_backup_index" -eq $((36 + audit_blocks * 52)) ] || return 1
    jq -e --argjson epoch "$epoch" --arg cluster "$cluster_id" \
        --arg source "$source" --arg backup "$backup" --arg generation "$generation" \
        --arg audit "$audit_report" --arg audit_sha "$audit_sha" \
        --argjson blocks "$audit_blocks" --argjson typed "$audit_typed" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-candidate" and
        .state == "unfinalized" and .canonical == false and
        .epoch == $epoch and .cluster_id == $cluster and
        .source == $source and .candidate == $source and .backup == $backup and
        .prospective_generation_id == $generation and
        .source_audit_report.path == $audit and
        .source_audit_report.sha256 == $audit_sha and
        .single_decode_rewrite_pass == true and
        .sidecars_copied == false and .sidecars_rewritten == false and
        .rewrite.blocks == $blocks and .rewrite.typed_messages == $typed and
        .rewrite.message_input_bytes == .rewrite.message_output_bytes and
        .exact_message_length_preserved == true and
        .exact_message_delta_proved == true and
        .canonical_publication_deferred == true and
        .target_post_audit_performed == false
    ' "$descriptor" >/dev/null 2>&1 || return 1
    cr_descriptor_sha=$(cr_sha256 "$descriptor") || return 1
    jq -e --argjson epoch "$epoch" --arg cluster "$cluster_id" \
        --arg source "$source" --arg backup "$backup" --arg generation "$generation" \
        --arg descriptor "$descriptor" --arg descriptor_sha "$cr_descriptor_sha" \
        --arg audit "$audit_report" --arg audit_sha "$audit_sha" '
        .schema_version == 1 and .state == "unfinalized" and .canonical == false and
        .epoch == $epoch and .cluster_id == $cluster and
        .prospective_generation_id == $generation and
        .candidate == $source and .backup == $backup and
        .candidate_descriptor == $descriptor and
        .candidate_descriptor_sha256 == $descriptor_sha and
        .source_audit_report == $audit and
        .source_audit_report_sha256 == $audit_sha and
        if .kind == "archive-v2-pre-to-post-candidate-report" then
            .source == $source and .single_decode_rewrite_pass == true and
            .canonical_publication_deferred == true
        elif .kind == "archive-v2-pre-to-post-candidate-recovery-report" then
            .recovered_switch == true and (.already_complete | type == "boolean")
        else false end
    ' "$cr_report" >/dev/null 2>&1
}

[ "$#" -eq 4 ] || cr_usage
handoff_state_root=$1
epoch=$2
converter=$3
threads=$4
for cr_required in jq awk wc tr date nice ionice chmod mkdir rmdir rm mv \
    ln dd cmp dirname basename kill ps; do
    cr_command "$cr_required"
done
if command -v sha256sum >/dev/null 2>&1; then
    sha256_program=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    sha256_program=shasum
else
    cr_die 'sha256sum or shasum is required'
fi
cr_absolute HANDOFF_STATE_ROOT "$handoff_state_root"
cr_absolute CONVERTER "$converter"
cr_decimal EPOCH "$epoch"
cr_decimal THREADS "$threads"
[ "$threads" -ge 1 ] && [ "$threads" -le 64 ] || cr_die 'THREADS must be 1..64'
[ -d "$handoff_state_root" ] && [ ! -L "$handoff_state_root" ] \
    && [ "$(cr_directory "$handoff_state_root")" = "$handoff_state_root" ] \
    || cr_die 'handoff state root must be one canonical real directory'
[ -f "$converter" ] && [ ! -L "$converter" ] && [ -x "$converter" ] \
    && [ "$(cr_file "$converter")" = "$converter" ] \
    || cr_die 'converter must be one canonical real executable'

request=$handoff_state_root/request.json
fast_state=$handoff_state_root/fast-in-place-candidate
fast_config=$fast_state/config.json
audit_table=$fast_state/all-legacy-pre-reports.tsv
audit_dir=$fast_state/source-audit-reports
for cr_control in "$request" "$fast_config" "$audit_table"; do
    [ -f "$cr_control" ] && [ ! -L "$cr_control" ] \
        || cr_die "fast state control is absent: $cr_control"
done
[ -d "$audit_dir" ] && [ ! -L "$audit_dir" ] || cr_die 'pinned audit directory is absent'
archive_root=$(cr_json '.archive_root | select(type == "string")' "$request")
cluster_id=$(cr_json '.cluster_id | select(type == "string")' "$request")
run_id=$(cr_json '.run_id | select(type == "string")' "$request")
request_state=$(cr_json '.state_root | select(type == "string")' "$request")
[ "$request_state" = "$handoff_state_root" ] || cr_die 'request binds another state root'
cr_absolute ARCHIVE_ROOT "$archive_root"
[ -d "$archive_root" ] && [ ! -L "$archive_root" ] \
    && [ "$(cr_directory "$archive_root")" = "$archive_root" ] \
    || cr_die 'archive root must be one canonical real directory'
jq -e --arg run "$run_id" --arg cluster "$cluster_id" --arg archive "$archive_root" \
    --arg state "$fast_state" --arg table "$audit_table" --arg audits "$audit_dir" '
    .schema_version == 1 and
    .kind == "archive-v2-pre-to-post-fast-in-place-config" and
    .run_id == $run and .cluster_id == $cluster and .archive_root == $archive and
    .state_root == $state and .source_audit_table == $table and
    .pinned_source_audit_directory == $audits
' "$fast_config" >/dev/null 2>&1 || cr_die 'fast state config is invalid'

cr_row=$(cr_table_row) || cr_die "epoch $epoch has no unique pinned audit row"
cr_old_ifs=$IFS
IFS=$(printf '\t')
read -r audit_epoch audit_source audit_sha audit_blocks audit_compressed \
    audit_uncompressed audit_typed audit_pre_only audit_equivalent <<EOF
$cr_row
EOF
IFS=$cr_old_ifs
[ "$audit_epoch" = "$epoch" ] || cr_die 'audit row epoch differs'
for cr_number in "$audit_blocks" "$audit_compressed" "$audit_uncompressed" \
    "$audit_typed" "$audit_pre_only" "$audit_equivalent"; do
    case "$cr_number" in ''|*[!0-9]*) cr_die 'audit row has an invalid count' ;; esac
done
case "$audit_sha" in *[!0-9a-f]*|'') cr_die 'audit row has an invalid digest' ;; esac
[ "${#audit_sha}" -eq 64 ] || cr_die 'audit row has a short digest'
audit_report=$audit_dir/epoch-$epoch.json
[ -f "$audit_report" ] && [ ! -L "$audit_report" ] \
    && [ "$(cr_sha256 "$audit_report")" = "$audit_sha" ] \
    || cr_die 'pinned audit report differs from its table binding'
source=$archive_root/epoch-$epoch
staging=$archive_root/.epoch-$epoch.pre-to-post.staging
backup=$archive_root/.epoch-$epoch.pre-to-post.backup
generation=archive-v2-pre-to-post-canary-$run_id-epoch-$epoch

lock_dir=$archive_root/.archive-v2-pre-to-post-fast-candidate.lock
lock_held=0
converter_pid=
cr_release_lock() {
    if [ "$lock_held" -eq 1 ]; then
        rmdir "$lock_dir" 2>/dev/null \
            || echo "archive-v2 fast canary: cannot remove root lock: $lock_dir" >&2
        lock_held=0
    fi
}
cr_stop() {
    cr_status=$1
    trap - 0 1 2 3 15
    if [ -n "$converter_pid" ]; then
        kill -TERM "$converter_pid" 2>/dev/null || :
        wait "$converter_pid" 2>/dev/null || :
        converter_pid=
    fi
    cr_release_lock
    exit "$cr_status"
}
trap 'cr_release_lock' 0
trap 'cr_stop 129' 1
trap 'cr_stop 130' 2
trap 'cr_stop 131' 3
trap 'cr_stop 143' 15
mkdir "$lock_dir" 2>/dev/null || cr_die "archive-root fast lock exists: $lock_dir"
lock_held=1

strict_lock=$handoff_state_root/conversion/.archive-v2-pre-to-post-manual.lock
[ ! -e "$strict_lock" ] && [ ! -L "$strict_lock" ] \
    || cr_die 'strict converter is active'
state_root=$handoff_state_root/fast-canary-epoch-$epoch
cr_process_guard start || cr_die 'an archive reader or old runner is active'
[ ! -e "$fast_state/results/epoch-$epoch.json" ] \
    && [ ! -L "$fast_state/results/epoch-$epoch.json" ] \
    && [ ! -e "$fast_state/claims/epoch-$epoch.json" ] \
    && [ ! -L "$fast_state/claims/epoch-$epoch.json" ] \
    || cr_die "epoch $epoch was already processed or claimed by the old runner"
new_state=0
if [ -e "$state_root" ] || [ -L "$state_root" ]; then
    [ -d "$state_root" ] && [ ! -L "$state_root" ] || cr_die 'canary state is invalid'
else
    [ -d "$source" ] && [ ! -L "$source" ] \
        && [ ! -e "$staging" ] && [ ! -L "$staging" ] \
        && [ ! -e "$backup" ] && [ ! -L "$backup" ] \
        && [ ! -e "$source/archive-v2-pre-to-post.candidate.v1.json" ] \
        || cr_die "epoch $epoch is not fresh and unprocessed"
    cr_source_bytes=$(wc -c <"$source/archive-v2-blocks.zstd" | tr -d '[:space:]') \
        || cr_die 'cannot read source blocks size'
    cr_index_bytes=$(wc -c <"$source/archive-v2-blocks.index" | tr -d '[:space:]') \
        || cr_die 'cannot read source index size'
    [ "$cr_source_bytes" -eq "$audit_compressed" ] \
        && [ "$cr_index_bytes" -eq $((36 + audit_blocks * 52)) ] \
        || cr_die 'fresh source geometry differs from the audit row'
    mkdir "$state_root" || cr_die 'cannot create canary state'
    new_state=1
fi
chmod 700 "$state_root" || cr_die 'cannot protect canary state'
converter_sha=$(cr_sha256 "$converter") || cr_die 'cannot hash converter'
config=$state_root/config.json
if [ "$new_state" -eq 1 ]; then
    config_candidate=$config.building-$$
    (
        set -C
        jq -cn --argjson epoch "$epoch" --argjson threads "$threads" \
            --arg run "$run_id" --arg archive "$archive_root" --arg source "$source" \
            --arg generation "$generation" --arg converter "$converter" \
            --arg converter_sha "$converter_sha" --arg audit "$audit_report" \
            --arg audit_sha "$audit_sha" --argjson source_bytes "$audit_compressed" '
            {schema_version:1,kind:"archive-v2-pre-to-post-fast-canary-config",
             run_id:$run,epoch:$epoch,threads:$threads,archive_root:$archive,source:$source,
             prospective_generation_id:$generation,
             converter:{path:$converter,sha256:$converter_sha},
             source_audit:{path:$audit,sha256:$audit_sha},
             source_compressed_bytes:$source_bytes,canonical:false}
        ' >"$config_candidate"
    ) || cr_die 'cannot create canary config'
    chmod 400 "$config_candidate" || cr_die 'cannot protect canary config'
    ln -n "$config_candidate" "$config" || cr_die 'cannot publish canary config'
    rm "$config_candidate" || cr_die 'cannot remove config candidate'
fi
jq -e --argjson epoch "$epoch" --argjson threads "$threads" --arg run "$run_id" \
    --arg archive "$archive_root" --arg source "$source" --arg generation "$generation" \
    --arg converter "$converter" --arg converter_sha "$converter_sha" \
    --arg audit "$audit_report" --arg audit_sha "$audit_sha" \
    --argjson source_bytes "$audit_compressed" '
    .schema_version == 1 and .kind == "archive-v2-pre-to-post-fast-canary-config" and
    .run_id == $run and .epoch == $epoch and .threads == $threads and
    .archive_root == $archive and .source == $source and
    .prospective_generation_id == $generation and
    .converter == {path:$converter,sha256:$converter_sha} and
    .source_audit == {path:$audit,sha256:$audit_sha} and
    .source_compressed_bytes == $source_bytes and .canonical == false
' "$config" >/dev/null 2>&1 || cr_die 'canary config changed on resume'

result=$state_root/result.json
if [ -e "$result" ] || [ -L "$result" ]; then
    cr_validate_result || cr_die 'canary result changed on resume'
    cr_existing_report=$(cr_json '.converter_report | select(type == "string")' "$result")
    cr_validate_candidate "$cr_existing_report" \
        || cr_die 'canary candidate changed on resume'
    cr_process_guard end || cr_die 'an archive reader or old runner is active'
    cr_release_lock
    trap - 0 1 2 3 15
    jq -c . "$result"
    exit 0
fi
attempts=$state_root/attempts
[ -e "$attempts" ] || mkdir "$attempts" || cr_die 'cannot create attempts directory'
[ -d "$attempts" ] && [ ! -L "$attempts" ] || cr_die 'attempts path is invalid'
chmod 700 "$attempts" || cr_die 'cannot protect attempts directory'
cr_sequence=1
while ! mkdir "$attempts/attempt-$cr_sequence" 2>/dev/null; do
    [ -d "$attempts/attempt-$cr_sequence" ] \
        && [ ! -L "$attempts/attempt-$cr_sequence" ] || cr_die 'invalid attempt path'
    cr_sequence=$((cr_sequence + 1))
    [ "$cr_sequence" -le 999999 ] || cr_die 'too many canary attempts'
done
attempt=$attempts/attempt-$cr_sequence
chmod 700 "$attempt" || cr_die 'cannot protect attempt'
report=$attempt/converter-report.json
stderr_log=$attempt/converter.stderr.log
started=$(date +%s) || cr_die 'cannot read start time'
[ "$(cr_sha256 "$converter")" = "$converter_sha" ] \
    || cr_die 'converter changed before execution'
nice -n 10 ionice -c 2 -n 7 "$converter" \
    --fast-candidate --threads "$threads" \
    --source "$source" \
    --source-lease-id "archive-v2-pre-to-post-canary-$run_id-epoch-$epoch-leases" \
    --target "$source" --staging "$staging" --epoch "$epoch" \
    --cluster-id "$cluster_id" --generation-id "$generation" \
    --source-audit-report "$audit_report" \
    --source-audit-report-sha256 "$audit_sha" \
    >"$report" 2>"$stderr_log" &
converter_pid=$!
if wait "$converter_pid"; then cr_status=0; else cr_status=$?; fi
converter_pid=
ended=$(date +%s) || cr_die 'cannot read end time'
[ "$cr_status" -eq 0 ] \
    || cr_die "converter failed with status $cr_status; rerun will use its journal"
cr_validate_candidate "$report" || cr_die 'converter candidate invariants failed'
chmod 400 "$report" "$stderr_log" || cr_die 'cannot protect converter output'
elapsed=$((ended - started))
# jq 1.6 exits with status 1 under -e when the valid result is false.
recovered=$(jq -r '
    if has("recovered_switch") then
        if (.recovered_switch | type) == "boolean" then .recovered_switch
        else error("recovered_switch is not boolean") end
    else false end
' "$report") \
    || cr_die 'converter report has an invalid recovery flag'
completed=$(date -u +%Y-%m-%dT%H:%M:%SZ) || cr_die 'cannot read completion time'
result_candidate=$result.building-$$
(
    set -C
    jq -cn --argjson epoch "$epoch" --argjson threads "$threads" \
        --argjson elapsed "$elapsed" --argjson bytes "$audit_compressed" \
        --argjson recovered "$recovered" --arg source "$source" --arg backup "$backup" \
        --arg generation "$generation" --arg audit "$audit_report" \
        --arg audit_sha "$audit_sha" --arg report "$report" --arg stderr "$stderr_log" \
        --arg completed "$completed" '
        {schema_version:1,kind:"archive-v2-pre-to-post-fast-canary-result",
         epoch:$epoch,threads:$threads,elapsed_seconds:$elapsed,
         source_compressed_bytes:$bytes,
         source_mib_per_second:(if $elapsed > 0 then ($bytes / 1048576 / $elapsed) else 0 end),
         source:$source,backup:$backup,prospective_generation_id:$generation,
         source_audit:{path:$audit,sha256:$audit_sha},
         converter_report:$report,converter_stderr:$stderr,recovered_switch:$recovered,
         completed_at_utc:$completed,canonical:false}
    ' >"$result_candidate"
) || cr_die 'cannot create canary result'
chmod 400 "$result_candidate" || cr_die 'cannot protect canary result'
ln -n "$result_candidate" "$result" || cr_die 'cannot publish canary result'
rm "$result_candidate" || cr_die 'cannot remove result candidate'
cr_validate_result || cr_die 'published canary result is invalid'
cr_process_guard end || cr_die 'an archive reader or old runner is active'
cr_release_lock
trap - 0 1 2 3 15
jq -c . "$result"
