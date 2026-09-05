#!/bin/sh
# Continue the validated prefix of the two-worker fast run with one epoch at a
# time. One converter uses multiple cores inside the epoch. The old prefix and
# the exact remaining audit bindings are frozen in a separate state root.

set -u
umask 077

mc_die() {
    echo "archive-v2 fast multicore Pre-to-Post: $*" >&2
    exit 1
}

mc_usage() {
    echo "usage: $0 HANDOFF_STATE_ROOT CONVERTER OPERATOR_QUIESCENCE_AUTHORITY_ID [THREADS]" >&2
    exit 2
}

mc_require_command() {
    command -v "$1" >/dev/null 2>&1 || mc_die "required command is absent: $1"
}

mc_canonical_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

mc_canonical_file() (
    mc_parent=$(mc_canonical_directory "$(dirname "$1")") || exit 1
    printf '%s/%s\n' "$mc_parent" "$(basename "$1")"
)

mc_require_absolute() {
    mc_label=$1
    mc_value=$2
    case "$mc_value" in
        /*) ;;
        *) mc_die "$mc_label must be an absolute path: $mc_value" ;;
    esac
    case "$mc_value" in
        *'
'*) mc_die "$mc_label must be one line" ;;
    esac
}

mc_require_text() {
    mc_label=$1
    mc_value=$2
    case "$mc_value" in
        ''|*'
'*) mc_die "$mc_label must be one non-empty line" ;;
    esac
}

mc_decimal() {
    case "$2" in
        0) ;;
        ''|*[!0-9]*|0*) mc_die "$1 is not a canonical decimal integer" ;;
        *) ;;
    esac
}

mc_sha256_file() (
    if [ "$sha256_program" = sha256sum ]; then
        mc_hash_output=$(sha256sum "$1") || exit 1
    else
        mc_hash_output=$(shasum -a 256 "$1") || exit 1
    fi
    printf '%s\n' "$mc_hash_output" | awk '
        NR == 1 && length($1) == 64 && $1 !~ /[^0-9a-f]/ {
            print $1; valid = 1
        }
        END { if (!valid) exit 1 }
    '
)

mc_json() {
    jq -er "$1" "$2" 2>/dev/null || mc_die "invalid $1 in $2"
}

mc_copy_exclusive() {
    (
        set -C
        dd if="$1" bs=1048576 >"$2" 2>/dev/null
    )
}

mc_publish_same() {
    mc_candidate=$1
    mc_final=$2
    [ -f "$mc_candidate" ] && [ ! -L "$mc_candidate" ] || return 1
    chmod 400 "$mc_candidate" || return 1
    if [ -e "$mc_final" ] || [ -L "$mc_final" ]; then
        [ -f "$mc_final" ] && [ ! -L "$mc_final" ] \
            && cmp -s "$mc_candidate" "$mc_final" || return 1
        rm "$mc_candidate" || return 1
    else
        ln -n "$mc_candidate" "$mc_final" || return 1
        rm "$mc_candidate" || return 1
    fi
}

mc_epoch_count() {
    awk '
        NF != 1 || $0 !~ /^(0|[1-9][0-9]*)$/ || seen[$0]++ { bad = 1 }
        previous != "" && ($0 + 0) <= (previous + 0) { bad = 1 }
        { previous = $0; count++ }
        END { if (bad) exit 1; print count + 0 }
    ' "$1"
}

mc_table_row() {
    awk -F '\t' -v epoch="$1" '
        $1 == epoch { count++; row = $0 }
        END { if (count != 1) exit 1; print row }
    ' "$old_report_table"
}

mc_load_audit_binding() {
    mc_epoch=$1
    mc_row=$(mc_table_row "$mc_epoch") || return 1
    mc_old_ifs=$IFS
    IFS=$(printf '\t')
    read -r audit_epoch audit_source audit_sha audit_blocks audit_compressed \
        audit_uncompressed audit_typed audit_pre_only audit_equivalent <<EOF
$mc_row
EOF
    IFS=$mc_old_ifs
    [ "$audit_epoch" = "$mc_epoch" ] || return 1
    mc_require_absolute AUDIT_SOURCE "$audit_source"
    case "$audit_sha" in *[!0-9a-f]*|'') return 1 ;; esac
    [ "${#audit_sha}" -eq 64 ] || return 1
    for mc_number in "$audit_blocks" "$audit_compressed" "$audit_uncompressed" \
        "$audit_typed" "$audit_pre_only" "$audit_equivalent"; do
        case "$mc_number" in ''|*[!0-9]*) return 1 ;; esac
    done
}

mc_validate_audit() {
    mc_audit=$1
    mc_epoch=$2
    mc_archive=$3
    mc_expected_sha=$4
    [ -f "$mc_audit" ] && [ ! -L "$mc_audit" ] \
        && [ "$(mc_sha256_file "$mc_audit")" = "$mc_expected_sha" ] || return 1
    jq -e -s --argjson epoch "$mc_epoch" --arg archive "$mc_archive" '
        def count: type == "number" and . >= 0 and floor == .;
        length == 1 and (.[0] as $r |
            $r.schema_version == 1 and
            $r.kind == "archive-v2-wire-profile-scan" and
            $r.epoch == $epoch and $r.archive == $archive and $r.error == null and
            $r.classification == "legacy-pre" and $r.action == "convert-to-post" and
            ($r.counts.blocks | count) and
            ($r.counts.compressed_block_bytes | count) and
            ($r.counts.uncompressed_block_bytes | count) and
            ($r.counts.typed_messages | count) and
            ($r.counts.pre_only | count) and $r.counts.pre_only > 0 and
            ($r.counts.both_equivalent | count) and
            $r.counts.owned_fallback_blocks == 0 and
            $r.counts.raw_transaction_fallbacks == 0 and
            $r.counts.post_only == 0 and $r.counts.both_divergent == 0 and
            $r.counts.invalid == 0 and
            $r.counts.typed_messages ==
                ($r.counts.pre_only + $r.counts.both_equivalent))
    ' "$mc_audit" >/dev/null 2>&1
}

mc_validate_candidate() {
    mc_epoch=$1
    mc_generation=$2
    mc_audit=$3
    mc_audit_sha=$4
    mc_report=$5
    mc_source=$archive_root/epoch-$mc_epoch
    mc_staging=$archive_root/.epoch-$mc_epoch.pre-to-post.staging
    mc_backup=$archive_root/.epoch-$mc_epoch.pre-to-post.backup
    mc_descriptor=$mc_source/archive-v2-pre-to-post.candidate.v1.json
    mc_intent=$mc_backup/archive-v2-pre-to-post.switch-intent.v1.json
    mc_complete=$mc_backup/archive-v2-pre-to-post.switch-complete.v1.json

    mc_validate_audit "$mc_audit" "$mc_epoch" "$mc_source" "$mc_audit_sha" || return 1
    [ -d "$mc_source" ] && [ ! -L "$mc_source" ] \
        && [ -d "$mc_backup" ] && [ ! -L "$mc_backup" ] \
        && [ ! -e "$mc_staging" ] && [ ! -L "$mc_staging" ] \
        && [ -f "$mc_descriptor" ] && [ ! -L "$mc_descriptor" ] \
        && [ -f "$mc_intent" ] && [ ! -L "$mc_intent" ] \
        && [ -f "$mc_complete" ] && [ ! -L "$mc_complete" ] \
        && [ -f "$mc_source/archive-v2-blocks.zstd" ] \
        && [ -f "$mc_source/archive-v2-blocks.index" ] \
        && [ -f "$mc_backup/archive-v2-blocks.zstd" ] \
        && [ -f "$mc_backup/archive-v2-blocks.index" ] || return 1
    for mc_pair in \
        "$mc_source/archive-v2-blocks.zstd" "$mc_source/archive-v2-blocks.index" \
        "$mc_backup/archive-v2-blocks.zstd" "$mc_backup/archive-v2-blocks.index"; do
        [ ! -L "$mc_pair" ] || return 1
    done
    mc_descriptor_sha=$(mc_sha256_file "$mc_descriptor") || return 1
    mc_descriptor_bytes=$(wc -c <"$mc_descriptor" | tr -d '[:space:]') || return 1
    mc_audit_bytes=$(wc -c <"$mc_audit" | tr -d '[:space:]') || return 1
    mc_counts=$(jq -c '.counts' "$mc_audit" 2>/dev/null) || return 1

    jq -e --argjson epoch "$mc_epoch" --arg cluster "$cluster_id" \
        --arg generation "$mc_generation" --arg source "$mc_source" \
        --arg backup "$mc_backup" --arg audit "$mc_audit" \
        --arg audit_sha "$mc_audit_sha" --argjson audit_bytes "$mc_audit_bytes" \
        --argjson counts "$mc_counts" '
        def count: type == "number" and . >= 0 and floor == .;
        def binding:
            type == "object" and (.bytes | count) and
            (.sha256 | type == "string" and test("^[0-9a-f]{64}$"));
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-candidate" and
        .state == "unfinalized" and .canonical == false and
        .epoch == $epoch and .cluster_id == $cluster and
        .prospective_generation_id == $generation and
        .source == $source and .candidate == $source and .backup == $backup and
        .source_audit_report.path == $audit and
        .source_audit_report.bytes == $audit_bytes and
        .source_audit_report.sha256 == $audit_sha and
        .source_audit_report.counts == $counts and
        .single_decode_rewrite_pass == true and
        .outer_block_bytes_preserved_verbatim_except_messages == true and
        .sidecars_copied == false and .sidecars_rewritten == false and
        (.source_files["archive-v2-blocks.zstd"] | binding) and
        (.source_files["archive-v2-blocks.index"] | binding) and
        (.candidate_rewrite_files["archive-v2-blocks.zstd"] | binding) and
        (.candidate_rewrite_files["archive-v2-blocks.index"] | binding) and
        (.rewrite.blocks | count) and .rewrite.blocks == $counts.blocks and
        (.rewrite.typed_messages | count) and
        .rewrite.typed_messages == $counts.typed_messages and
        .exact_message_length_preserved == true and
        .exact_message_delta_proved == true and
        .metadata_regions_copied_verbatim == true and
        .canonical_publication_deferred == true and
        .target_post_audit_performed == false and
        .canonical_manifest_written == false and
        .canonical_profile_marker_written == false and
        .canonical_migration_receipt_written == false
    ' "$mc_descriptor" >/dev/null 2>&1 || return 1

    jq -e --argjson epoch "$mc_epoch" --arg generation "$mc_generation" \
        --arg source "$mc_source" --arg staging "$mc_staging" --arg backup "$mc_backup" \
        --arg audit "$mc_audit" --arg audit_sha "$mc_audit_sha" \
        --arg descriptor_sha "$mc_descriptor_sha" \
        --argjson descriptor_bytes "$mc_descriptor_bytes" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-pair-swap-intent" and
        .epoch == $epoch and .prospective_generation_id == $generation and
        .candidate == $source and .staging == $staging and .backup == $backup and
        .candidate_descriptor.sha256 == $descriptor_sha and
        .candidate_descriptor.bytes == $descriptor_bytes and
        .source_audit_report_path == $audit and
        .source_audit_report.sha256 == $audit_sha
    ' "$mc_intent" >/dev/null 2>&1 || return 1
    mc_intent_sha=$(mc_sha256_file "$mc_intent") || return 1
    jq -e --argjson epoch "$mc_epoch" --arg source "$mc_source" --arg backup "$mc_backup" \
        --arg intent_sha "$mc_intent_sha" --arg descriptor_sha "$mc_descriptor_sha" \
        --arg audit_sha "$mc_audit_sha" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-pair-swap-complete" and
        .epoch == $epoch and .canonical == false and
        .candidate == $source and .backup == $backup and
        .intent_sha256 == $intent_sha and
        .candidate_descriptor_sha256 == $descriptor_sha and
        .source_audit_report_sha256 == $audit_sha
    ' "$mc_complete" >/dev/null 2>&1 || return 1

    mc_live_blocks_bytes=$(wc -c <"$mc_source/archive-v2-blocks.zstd" | tr -d '[:space:]') \
        || return 1
    mc_live_index_bytes=$(wc -c <"$mc_source/archive-v2-blocks.index" | tr -d '[:space:]') \
        || return 1
    mc_backup_blocks_bytes=$(wc -c <"$mc_backup/archive-v2-blocks.zstd" | tr -d '[:space:]') \
        || return 1
    mc_backup_index_bytes=$(wc -c <"$mc_backup/archive-v2-blocks.index" | tr -d '[:space:]') \
        || return 1
    jq -e --argjson live_blocks "$mc_live_blocks_bytes" \
        --argjson live_index "$mc_live_index_bytes" \
        --argjson backup_blocks "$mc_backup_blocks_bytes" \
        --argjson backup_index "$mc_backup_index_bytes" '
        .candidate_rewrite_files["archive-v2-blocks.zstd"].bytes == $live_blocks and
        .candidate_rewrite_files["archive-v2-blocks.index"].bytes == $live_index and
        .source_files["archive-v2-blocks.zstd"].bytes == $backup_blocks and
        .source_files["archive-v2-blocks.index"].bytes == $backup_index
    ' "$mc_descriptor" >/dev/null 2>&1 || return 1

    [ -f "$mc_report" ] && [ ! -L "$mc_report" ] || return 1
    jq -e --argjson epoch "$mc_epoch" --arg cluster "$cluster_id" \
        --arg generation "$mc_generation" --arg source "$mc_source" \
        --arg backup "$mc_backup" --arg descriptor "$mc_descriptor" \
        --arg descriptor_sha "$mc_descriptor_sha" \
        --argjson descriptor_bytes "$mc_descriptor_bytes" \
        --arg audit "$mc_audit" --arg audit_sha "$mc_audit_sha" \
        --argjson audit_bytes "$mc_audit_bytes" '
        .schema_version == 1 and .state == "unfinalized" and .canonical == false and
        .epoch == $epoch and .cluster_id == $cluster and
        .prospective_generation_id == $generation and
        .candidate == $source and .backup == $backup and
        .candidate_descriptor == $descriptor and
        .candidate_descriptor_sha256 == $descriptor_sha and
        .candidate_descriptor_bytes == $descriptor_bytes and
        .source_audit_report == $audit and
        .source_audit_report_sha256 == $audit_sha and
        .source_audit_report_bytes == $audit_bytes and
        if .kind == "archive-v2-pre-to-post-candidate-report" then
            .source == $source and .single_decode_rewrite_pass == true and
            .sidecars_copied == false and .sidecars_rewritten == false and
            .canonical_publication_deferred == true
        elif .kind == "archive-v2-pre-to-post-candidate-recovery-report" then
            .recovered_switch == true and (.already_complete | type == "boolean")
        else false end
    ' "$mc_report" >/dev/null 2>&1
}

mc_validate_old_result() {
    mc_epoch=$1
    mc_expected_sha=$2
    mc_result=$old_results_dir/epoch-$mc_epoch.json
    [ -f "$mc_result" ] && [ ! -L "$mc_result" ] || return 1
    mc_old_generation=archive-v2-pre-to-post-fast-$run_id-epoch-$mc_epoch
    mc_old_audit=$(jq -er '.source_audit.pinned_path | select(type == "string")' \
        "$mc_result" 2>/dev/null) || return 1
    mc_old_report=$(jq -er '.converter_report | select(type == "string")' \
        "$mc_result" 2>/dev/null) || return 1
    mc_old_log=$(jq -er '.converter_log | select(type == "string")' \
        "$mc_result" 2>/dev/null) || return 1
    [ "$mc_old_audit" = "$old_state/source-audit-reports/epoch-$mc_epoch.json" ] \
        && [ "$mc_old_report" = "$old_state/reports/epoch-$mc_epoch.json" ] \
        && [ "$mc_old_log" = "$old_state/logs/epoch-$mc_epoch.log" ] || return 1
    [ -f "$mc_old_log" ] && [ ! -L "$mc_old_log" ] || return 1
    jq -e --argjson epoch "$mc_epoch" --arg run_id "$run_id" \
        --arg source "$archive_root/epoch-$mc_epoch" \
        --arg backup "$archive_root/.epoch-$mc_epoch.pre-to-post.backup" \
        --arg generation "$mc_old_generation" --arg audit "$mc_old_audit" \
        --arg audit_sha "$mc_expected_sha" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-fast-in-place-result" and
        .run_id == $run_id and .epoch == $epoch and
        .source == $source and .backup == $backup and
        .prospective_generation_id == $generation and
        .source_audit.pinned_path == $audit and
        .source_audit.sha256 == $audit_sha and
        .target_post_audit_deferred == true and
        .canonical_manifest_deferred == true and
        .canonical_publication == false
    ' "$mc_result" >/dev/null 2>&1 || return 1
    mc_validate_candidate "$mc_epoch" "$mc_old_generation" "$mc_old_audit" \
        "$mc_expected_sha" "$mc_old_report"
}

mc_validate_new_result() {
    mc_epoch=$1
    mc_result=$results_dir/epoch-$mc_epoch.json
    mc_audit=$audit_dir/epoch-$mc_epoch.json
    mc_load_audit_binding "$mc_epoch" || return 1
    mc_generation=archive-v2-pre-to-post-multicore-$run_id-epoch-$mc_epoch
    [ -f "$mc_result" ] && [ ! -L "$mc_result" ] || return 1
    mc_report=$(jq -er '.converter_report | select(type == "string")' \
        "$mc_result" 2>/dev/null) || return 1
    mc_log=$(jq -er '.converter_log | select(type == "string")' \
        "$mc_result" 2>/dev/null) || return 1
    [ "$mc_report" = "$reports_dir/epoch-$mc_epoch.json" ] \
        && [ "$mc_log" = "$logs_dir/epoch-$mc_epoch.log" ] || return 1
    mc_report_sha=$(mc_sha256_file "$mc_report") || return 1
    mc_log_sha=$(mc_sha256_file "$mc_log") || return 1
    mc_descriptor=$archive_root/epoch-$mc_epoch/archive-v2-pre-to-post.candidate.v1.json
    mc_descriptor_sha=$(mc_sha256_file "$mc_descriptor") || return 1
    jq -e --argjson epoch "$mc_epoch" --arg run_id "$run_id" \
        --arg source "$archive_root/epoch-$mc_epoch" \
        --arg backup "$archive_root/.epoch-$mc_epoch.pre-to-post.backup" \
        --arg generation "$mc_generation" --arg audit "$mc_audit" \
        --arg audit_sha "$audit_sha" --arg report "$mc_report" \
        --arg report_sha "$mc_report_sha" --arg log "$mc_log" \
        --arg log_sha "$mc_log_sha" --arg descriptor "$mc_descriptor" \
        --arg descriptor_sha "$mc_descriptor_sha" --argjson threads "$threads" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-fast-multicore-result" and
        .run_id == $run_id and .epoch == $epoch and .threads == $threads and
        .source == $source and .backup == $backup and
        .prospective_generation_id == $generation and
        .source_audit == {path:$audit,sha256:$audit_sha} and
        .converter_report == $report and .converter_report_sha256 == $report_sha and
        .converter_log == $log and .converter_log_sha256 == $log_sha and
        .candidate_descriptor == $descriptor and
        .candidate_descriptor_sha256 == $descriptor_sha and
        .canonical_publication == false
    ' "$mc_result" >/dev/null 2>&1 || return 1
    mc_validate_candidate "$mc_epoch" "$mc_generation" "$mc_audit" "$audit_sha" "$mc_report"
}

mc_pre_geometry() {
    mc_epoch=$1
    mc_source=$archive_root/epoch-$mc_epoch
    mc_blocks=$mc_source/archive-v2-blocks.zstd
    mc_index=$mc_source/archive-v2-blocks.index
    [ -f "$mc_blocks" ] && [ ! -L "$mc_blocks" ] \
        && [ -f "$mc_index" ] && [ ! -L "$mc_index" ] || return 1
    mc_blocks_bytes=$(wc -c <"$mc_blocks" | tr -d '[:space:]') || return 1
    mc_index_bytes=$(wc -c <"$mc_index" | tr -d '[:space:]') || return 1
    [ "$mc_blocks_bytes" -eq "$audit_compressed" ] \
        && [ "$mc_index_bytes" -eq $((36 + audit_blocks * 52)) ]
}

mc_space_for_epoch() {
    mc_index_bytes=$((36 + audit_blocks * 52))
    mc_margin=$((audit_compressed / 100))
    [ "$mc_margin" -ge 268435456 ] || mc_margin=268435456
    [ "$mc_margin" -le 4294967296 ] || mc_margin=4294967296
    mc_required=$((audit_compressed + mc_index_bytes + mc_margin))
    mc_available_kib=$(df -Pk "$archive_root" | awk '
        NR == 2 { print $4; found = 1 }
        END { if (!found) exit 1 }
    ') || return 1
    case "$mc_available_kib" in ''|*[!0-9]*) return 1 ;; esac
    [ $((mc_available_kib * 1024)) -ge "$mc_required" ]
}

mc_attempt_dir() {
    mc_epoch=$1
    mc_epoch_attempts=$attempts_dir/epoch-$mc_epoch
    if [ -e "$mc_epoch_attempts" ] || [ -L "$mc_epoch_attempts" ]; then
        [ -d "$mc_epoch_attempts" ] && [ ! -L "$mc_epoch_attempts" ] || return 1
    else
        mkdir "$mc_epoch_attempts" || return 1
        chmod 700 "$mc_epoch_attempts" || return 1
    fi
    mc_sequence=1
    while [ "$mc_sequence" -le 999999 ]; do
        attempt_dir=$mc_epoch_attempts/attempt-$mc_sequence
        if mkdir "$attempt_dir" 2>/dev/null; then
            chmod 700 "$attempt_dir" || return 1
            return 0
        fi
        [ -d "$attempt_dir" ] && [ ! -L "$attempt_dir" ] || return 1
        mc_sequence=$((mc_sequence + 1))
    done
    return 1
}

mc_process_guard() {
    mc_stage=$1
    mc_snapshot=$state_root/.processes-$mc_stage-$$
    ps -eo pid=,args= >"$mc_snapshot" || return 1
    if awk -v self="$$" -v archive="$archive_root" '
        $1 != self && index($0, archive) { print > "/dev/stderr"; found = 1 }
        END { exit found ? 0 : 1 }
    ' "$mc_snapshot"; then
        rm "$mc_snapshot" || :
        return 1
    fi
    rm "$mc_snapshot"
}

[ "$#" -eq 3 ] || [ "$#" -eq 4 ] || mc_usage
handoff_state_root=$1
converter_source=$2
operator_id=$3
threads=${4:-8}

for mc_command in jq awk sed wc tr date nice ionice chmod mkdir rmdir ln rm dd df \
    cmp dirname basename kill ps sort; do
    mc_require_command "$mc_command"
done
if command -v sha256sum >/dev/null 2>&1; then
    sha256_program=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    sha256_program=shasum
else
    mc_die 'sha256sum or shasum is required'
fi

mc_require_absolute HANDOFF_STATE_ROOT "$handoff_state_root"
mc_require_absolute CONVERTER "$converter_source"
mc_require_text OPERATOR_QUIESCENCE_AUTHORITY_ID "$operator_id"
mc_decimal THREADS "$threads"
[ "$threads" -ge 1 ] && [ "$threads" -le 64 ] || mc_die 'THREADS must be 1..64'
[ -d "$handoff_state_root" ] && [ ! -L "$handoff_state_root" ] \
    && [ "$(mc_canonical_directory "$handoff_state_root")" = "$handoff_state_root" ] \
    || mc_die 'handoff state root must be one canonical real directory'
[ -f "$converter_source" ] && [ ! -L "$converter_source" ] && [ -x "$converter_source" ] \
    && [ "$(mc_canonical_file "$converter_source")" = "$converter_source" ] \
    || mc_die 'converter must be one canonical real executable'

request=$handoff_state_root/request.json
old_state=$handoff_state_root/fast-in-place-candidate
old_config=$old_state/config.json
old_epoch_list=$old_state/all-legacy-pre-epochs.txt
old_report_table=$old_state/all-legacy-pre-reports.tsv
old_results_dir=$old_state/results
for mc_path in "$request" "$old_config" "$old_epoch_list" "$old_report_table"; do
    [ -f "$mc_path" ] && [ ! -L "$mc_path" ] || mc_die "old state control is absent: $mc_path"
done
[ -d "$old_results_dir" ] && [ ! -L "$old_results_dir" ] \
    || mc_die 'old result directory is absent'
archive_root=$(mc_json '.archive_root | select(type == "string")' "$request")
cluster_id=$(mc_json '.cluster_id | select(type == "string")' "$request")
run_id=$(mc_json '.run_id | select(type == "string")' "$request")
request_state=$(mc_json '.state_root | select(type == "string")' "$request")
[ "$request_state" = "$handoff_state_root" ] || mc_die 'request binds another state root'
mc_require_absolute ARCHIVE_ROOT "$archive_root"
[ -d "$archive_root" ] && [ ! -L "$archive_root" ] \
    && [ "$(mc_canonical_directory "$archive_root")" = "$archive_root" ] \
    || mc_die 'archive root must be one canonical real directory'
jq -e --arg run "$run_id" --arg cluster "$cluster_id" --arg archive "$archive_root" \
    --arg state "$old_state" '
    .schema_version == 1 and
    .kind == "archive-v2-pre-to-post-fast-in-place-config" and
    .run_id == $run and .cluster_id == $cluster and
    .archive_root == $archive and .state_root == $state and .worker_count == 2
' "$old_config" >/dev/null 2>&1 || mc_die 'old two-worker config is invalid'

strict_lock=$handoff_state_root/conversion/.archive-v2-pre-to-post-manual.lock
[ ! -e "$strict_lock" ] && [ ! -L "$strict_lock" ] \
    || mc_die "strict converter is still locked: $strict_lock"

lock_dir=$archive_root/.archive-v2-pre-to-post-fast-candidate.lock
lock_held=0
converter_pid=
mc_release_lock() {
    trap - 0 1 2 3 15
    if [ "$lock_held" -eq 1 ]; then
        rmdir "$lock_dir" 2>/dev/null \
            || echo "fast multicore runner: cannot remove archive-root lock: $lock_dir" >&2
        lock_held=0
    fi
}
mc_stop() {
    mc_status=$1
    trap - 0 1 2 3 15
    if [ -n "$converter_pid" ]; then
        kill -TERM "$converter_pid" 2>/dev/null || :
        wait "$converter_pid" 2>/dev/null || :
        converter_pid=
    fi
    mc_release_lock
    exit "$mc_status"
}
trap 'mc_release_lock' 0
trap 'mc_stop 129' 1
trap 'mc_stop 130' 2
trap 'mc_stop 131' 3
trap 'mc_stop 143' 15
mkdir "$lock_dir" 2>/dev/null \
    || mc_die "archive-root lock exists; stop the old coordinator first: $lock_dir"
lock_held=1

state_root=$handoff_state_root/fast-in-place-multicore
if [ -e "$state_root" ] || [ -L "$state_root" ]; then
    [ -d "$state_root" ] && [ ! -L "$state_root" ] || mc_die 'new state root is invalid'
else
    mkdir "$state_root" || mc_die 'cannot create new state root'
fi
chmod 700 "$state_root" || mc_die 'cannot protect new state root'
audit_dir=$state_root/source-audit-reports
attempts_dir=$state_root/attempts
reports_dir=$state_root/reports
logs_dir=$state_root/logs
results_dir=$state_root/results
tools_dir=$state_root/tools
for mc_dir in "$audit_dir" "$attempts_dir" "$reports_dir" "$logs_dir" \
    "$results_dir" "$tools_dir"; do
    if [ -e "$mc_dir" ] || [ -L "$mc_dir" ]; then
        [ -d "$mc_dir" ] && [ ! -L "$mc_dir" ] || mc_die "invalid state directory: $mc_dir"
    else
        mkdir "$mc_dir" || mc_die "cannot create state directory: $mc_dir"
    fi
    chmod 700 "$mc_dir" || mc_die "cannot protect state directory: $mc_dir"
done
mc_process_guard start || mc_die 'an archive-root process is still active at transition start'

old_count=$(mc_epoch_count "$old_epoch_list") || mc_die 'old epoch list is invalid'
[ "$(wc -l <"$old_report_table" | tr -d '[:space:]')" -eq "$old_count" ] \
    || mc_die 'old audit table count differs from the epoch list'
old_config_sha=$(mc_sha256_file "$old_config") || mc_die 'cannot hash old config'
old_list_sha=$(mc_sha256_file "$old_epoch_list") || mc_die 'cannot hash old epoch list'
old_table_sha=$(mc_sha256_file "$old_report_table") || mc_die 'cannot hash old audit table'

prefix_candidate=$state_root/old-prefix-epochs.txt.building-$$
remaining_candidate=$state_root/remaining-epochs.txt.building-$$
( set -C; : >"$prefix_candidate" ) || mc_die 'cannot create prefix candidate'
( set -C; : >"$remaining_candidate" ) || mc_die 'cannot create remaining candidate'
prefix_count=0
remaining_count=0
prefix_open=1
old_last_json=null
while IFS= read -r mc_epoch || [ -n "$mc_epoch" ]; do
    mc_load_audit_binding "$mc_epoch" || mc_die "invalid audit binding for epoch $mc_epoch"
    mc_old_result=$old_results_dir/epoch-$mc_epoch.json
    if [ -e "$mc_old_result" ] || [ -L "$mc_old_result" ]; then
        [ "$prefix_open" -eq 1 ] \
            || mc_die "old result epoch $mc_epoch is outside the completed prefix"
        mc_validate_old_result "$mc_epoch" "$audit_sha" \
            || mc_die "old result is not valid for epoch $mc_epoch"
        printf '%s\n' "$mc_epoch" >>"$prefix_candidate" \
            || mc_die 'cannot extend old prefix candidate'
        prefix_count=$((prefix_count + 1))
        old_last_json=$mc_epoch
    else
        prefix_open=0
        printf '%s\n' "$mc_epoch" >>"$remaining_candidate" \
            || mc_die 'cannot extend remaining candidate'
        remaining_count=$((remaining_count + 1))
    fi
done <"$old_epoch_list"
[ $((prefix_count + remaining_count)) -eq "$old_count" ] \
    || mc_die 'prefix and remaining counts do not cover the old list'
old_result_file_count=0
for mc_result_file in "$old_results_dir"/epoch-*.json; do
    if [ ! -e "$mc_result_file" ] && [ ! -L "$mc_result_file" ]; then
        continue
    fi
    [ -f "$mc_result_file" ] && [ ! -L "$mc_result_file" ] \
        || mc_die "invalid old result entry: $mc_result_file"
    old_result_file_count=$((old_result_file_count + 1))
done
[ "$old_result_file_count" -eq "$prefix_count" ] \
    || mc_die 'old result directory is not the exact validated prefix'

prefix_list=$state_root/old-prefix-epochs.txt
remaining_list=$state_root/remaining-epochs.txt
mc_publish_same "$prefix_candidate" "$prefix_list" || mc_die 'old prefix changed on resume'
mc_publish_same "$remaining_candidate" "$remaining_list" || mc_die 'remaining list changed on resume'
[ "$(mc_epoch_count "$prefix_list")" -eq "$prefix_count" ] \
    || mc_die 'published prefix count differs'
[ "$(mc_epoch_count "$remaining_list")" -eq "$remaining_count" ] \
    || mc_die 'published remaining count differs'
prefix_sha=$(mc_sha256_file "$prefix_list") || mc_die 'cannot hash prefix list'
remaining_sha=$(mc_sha256_file "$remaining_list") || mc_die 'cannot hash remaining list'

bindings=$state_root/remaining-audits.tsv
bindings_candidate=$bindings.building-$$
( set -C; : >"$bindings_candidate" ) || mc_die 'cannot create audit binding candidate'
while IFS= read -r mc_epoch || [ -n "$mc_epoch" ]; do
    mc_load_audit_binding "$mc_epoch" || mc_die "invalid remaining audit binding: $mc_epoch"
    mc_source=$archive_root/epoch-$mc_epoch
    mc_pinned_audit=$audit_dir/epoch-$mc_epoch.json
    mc_audit_candidate=$mc_pinned_audit.building-$$
    if [ -e "$mc_pinned_audit" ] || [ -L "$mc_pinned_audit" ]; then
        [ -f "$mc_pinned_audit" ] && [ ! -L "$mc_pinned_audit" ] \
            || mc_die "invalid pinned audit for epoch $mc_epoch"
    else
        mc_validate_audit "$audit_source" "$mc_epoch" "$mc_source" "$audit_sha" \
            || mc_die "source audit is invalid for epoch $mc_epoch"
        mc_copy_exclusive "$audit_source" "$mc_audit_candidate" \
            || mc_die "cannot pin audit for epoch $mc_epoch"
        mc_publish_same "$mc_audit_candidate" "$mc_pinned_audit" \
            || mc_die "cannot publish audit for epoch $mc_epoch"
    fi
    mc_validate_audit "$mc_pinned_audit" "$mc_epoch" "$mc_source" "$audit_sha" \
        || mc_die "pinned audit is invalid for epoch $mc_epoch"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$mc_epoch" "$mc_pinned_audit" "$audit_sha" "$audit_blocks" \
        "$audit_compressed" "$audit_uncompressed" "$audit_typed" \
        "$audit_pre_only" "$audit_equivalent" >>"$bindings_candidate" \
        || mc_die 'cannot extend remaining audit table'
done <"$remaining_list"
mc_publish_same "$bindings_candidate" "$bindings" || mc_die 'remaining audits changed on resume'
bindings_sha=$(mc_sha256_file "$bindings") || mc_die 'cannot hash remaining audit table'

converter=$tools_dir/archive-v2-pre-to-post
converter_candidate=$converter.building-$$
converter_source_sha=$(mc_sha256_file "$converter_source") || mc_die 'cannot hash converter'
if [ -e "$converter" ] || [ -L "$converter" ]; then
    [ -f "$converter" ] && [ ! -L "$converter" ] && [ -x "$converter" ] \
        || mc_die 'pinned converter is invalid'
    [ "$(mc_sha256_file "$converter")" = "$converter_source_sha" ] \
        || mc_die 'converter differs from the pinned transition binary'
else
    mc_copy_exclusive "$converter_source" "$converter_candidate" \
        || mc_die 'cannot copy converter'
    [ "$(mc_sha256_file "$converter_candidate")" = "$converter_source_sha" ] \
        || mc_die 'converter changed while copied'
    chmod 500 "$converter_candidate" || mc_die 'cannot protect converter copy'
    ln -n "$converter_candidate" "$converter" || mc_die 'cannot publish converter copy'
    rm "$converter_candidate" || mc_die 'cannot remove converter candidate'
fi
converter_sha=$(mc_sha256_file "$converter") || mc_die 'cannot hash pinned converter'
converter_version=$("$converter" --version 2>/dev/null) || mc_die 'converter has no version'
mc_require_text CONVERTER_VERSION "$converter_version"

config=$state_root/config.json
config_candidate=$config.building-$$
(
    set -C
    jq -cn --arg run "$run_id" --arg cluster "$cluster_id" \
        --arg archive "$archive_root" --arg state "$state_root" \
        --arg old_state "$old_state" --arg old_config "$old_config" \
        --arg old_config_sha "$old_config_sha" --arg old_list "$old_epoch_list" \
        --arg old_list_sha "$old_list_sha" --arg old_table "$old_report_table" \
        --arg old_table_sha "$old_table_sha" --arg prefix "$prefix_list" \
        --arg prefix_sha "$prefix_sha" --arg remaining "$remaining_list" \
        --arg remaining_sha "$remaining_sha" --arg bindings "$bindings" \
        --arg bindings_sha "$bindings_sha" --arg converter_source "$converter_source" \
        --arg converter "$converter" --arg converter_sha "$converter_sha" \
        --arg converter_version "$converter_version" --arg operator "$operator_id" \
        --argjson prefix_count "$prefix_count" --argjson remaining_count "$remaining_count" \
        --argjson threads "$threads" '
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-fast-multicore-config",
            run_id:$run,cluster_id:$cluster,archive_root:$archive,state_root:$state,
            threads:$threads,
            old_prefix:{state_root:$old_state,config:{path:$old_config,sha256:$old_config_sha},
                epoch_list:{path:$old_list,sha256:$old_list_sha},
                audit_table:{path:$old_table,sha256:$old_table_sha},
                frozen_prefix:{path:$prefix,sha256:$prefix_sha,count:$prefix_count}},
            remaining:{epoch_list:{path:$remaining,sha256:$remaining_sha,count:$remaining_count},
                audit_bindings:{path:$bindings,sha256:$bindings_sha}},
            converter:{source_path:$converter_source,pinned_path:$converter,
                sha256:$converter_sha,version:$converter_version,mode:"--fast-candidate"},
            operator_quiescence_authority_id:$operator,
            one_converter_child_at_a_time:true,
            backups_retained:true,
            target_post_audit_deferred:true,
            canonical_publication:false
        }
    ' >"$config_candidate"
) || mc_die 'cannot build multicore config'
mc_publish_same "$config_candidate" "$config" || mc_die 'multicore config changed on resume'

# All later binding lookups use the frozen table, not the old mutable path.
old_report_table=$bindings
while IFS= read -r mc_epoch || [ -n "$mc_epoch" ]; do
    [ ! -e "$strict_lock" ] && [ ! -L "$strict_lock" ] \
        || mc_die "strict converter became active: $strict_lock"
    mc_load_audit_binding "$mc_epoch" || mc_die "cannot load frozen audit for epoch $mc_epoch"
    mc_audit=$audit_dir/epoch-$mc_epoch.json
    mc_generation=archive-v2-pre-to-post-multicore-$run_id-epoch-$mc_epoch
    mc_source=$archive_root/epoch-$mc_epoch
    mc_staging=$archive_root/.epoch-$mc_epoch.pre-to-post.staging
    mc_backup=$archive_root/.epoch-$mc_epoch.pre-to-post.backup
    mc_result=$results_dir/epoch-$mc_epoch.json
    if [ -e "$mc_result" ] || [ -L "$mc_result" ]; then
        mc_validate_new_result "$mc_epoch" \
            || mc_die "completed result changed for epoch $mc_epoch"
        echo "epoch $mc_epoch: validated existing multicore result"
        continue
    fi
    if [ ! -e "$mc_staging" ] && [ ! -L "$mc_staging" ] \
        && [ ! -e "$mc_backup" ] && [ ! -L "$mc_backup" ] \
        && [ ! -e "$mc_source/archive-v2-pre-to-post.candidate.v1.json" ]; then
        mc_validate_audit "$mc_audit" "$mc_epoch" "$mc_source" "$audit_sha" \
            && mc_pre_geometry "$mc_epoch" \
            || mc_die "fresh Pre state changed for epoch $mc_epoch"
    fi
    mc_space_for_epoch || mc_die "free-space admission failed for epoch $mc_epoch"
    mc_attempt_dir "$mc_epoch" || mc_die "cannot create attempt for epoch $mc_epoch"
    mc_attempt_report=$attempt_dir/report.json
    mc_attempt_log=$attempt_dir/converter.log
    {
        echo "epoch=$mc_epoch"
        echo "threads=$threads"
        echo "source=$mc_source"
        echo "audit=$mc_audit"
        echo "audit_sha256=$audit_sha"
        echo "converter=$converter"
        echo "converter_sha256=$converter_sha"
        echo "operator_quiescence_authority_id=$operator_id"
    } >"$mc_attempt_log" || mc_die "cannot create log for epoch $mc_epoch"
    [ "$(mc_sha256_file "$converter")" = "$converter_sha" ] \
        || mc_die 'pinned converter changed before execution'
    nice -n 10 ionice -c 2 -n 7 "$converter" \
        --fast-candidate \
        --threads "$threads" \
        --source "$mc_source" \
        --source-lease-id "archive-v2-pre-to-post-multicore-$run_id-epoch-$mc_epoch-leases" \
        --target "$mc_source" \
        --staging "$mc_staging" \
        --epoch "$mc_epoch" \
        --cluster-id "$cluster_id" \
        --generation-id "$mc_generation" \
        --source-audit-report "$mc_audit" \
        --source-audit-report-sha256 "$audit_sha" \
        >"$mc_attempt_report" 2>>"$mc_attempt_log" &
    converter_pid=$!
    if wait "$converter_pid"; then mc_status=0; else mc_status=$?; fi
    converter_pid=
    [ "$mc_status" -eq 0 ] \
        || mc_die "converter failed for epoch $mc_epoch with status $mc_status; resume will recover"
    mc_validate_candidate "$mc_epoch" "$mc_generation" "$mc_audit" \
        "$audit_sha" "$mc_attempt_report" \
        || mc_die "converter output is invalid for epoch $mc_epoch"
    chmod 400 "$mc_attempt_report" "$mc_attempt_log" \
        || mc_die "cannot protect attempt for epoch $mc_epoch"
    mc_report=$reports_dir/epoch-$mc_epoch.json
    mc_log=$logs_dir/epoch-$mc_epoch.log
    for mc_pair in "$mc_attempt_report:$mc_report" "$mc_attempt_log:$mc_log"; do
        mc_from=${mc_pair%%:*}
        mc_to=${mc_pair#*:}
        if [ -e "$mc_to" ] || [ -L "$mc_to" ]; then
            [ -f "$mc_to" ] && [ ! -L "$mc_to" ] && cmp -s "$mc_from" "$mc_to" \
                || mc_die "published attempt differs for epoch $mc_epoch"
        else
            ln -n "$mc_from" "$mc_to" || mc_die "cannot publish attempt for epoch $mc_epoch"
        fi
    done
    mc_report_sha=$(mc_sha256_file "$mc_report") || mc_die 'cannot hash converter report'
    mc_log_sha=$(mc_sha256_file "$mc_log") || mc_die 'cannot hash converter log'
    mc_descriptor=$mc_source/archive-v2-pre-to-post.candidate.v1.json
    mc_descriptor_sha=$(mc_sha256_file "$mc_descriptor") || mc_die 'cannot hash descriptor'
    mc_recovered=$(jq -r '(.recovered_switch // false) |
        if type == "boolean" then . else error("invalid recovery state") end' \
        "$mc_report") || mc_die 'cannot read recovery state'
    mc_result_candidate=$mc_result.building-$$
    (
        set -C
        jq -cn --arg run "$run_id" --argjson epoch "$mc_epoch" --argjson threads "$threads" \
            --arg source "$mc_source" --arg backup "$mc_backup" \
            --arg generation "$mc_generation" --arg audit "$mc_audit" \
            --arg audit_sha "$audit_sha" --arg report "$mc_report" \
            --arg report_sha "$mc_report_sha" --arg log "$mc_log" --arg log_sha "$mc_log_sha" \
            --arg descriptor "$mc_descriptor" --arg descriptor_sha "$mc_descriptor_sha" \
            --argjson recovered "$mc_recovered" '
            {schema_version:1,kind:"archive-v2-pre-to-post-fast-multicore-result",
             run_id:$run,epoch:$epoch,threads:$threads,source:$source,backup:$backup,
             prospective_generation_id:$generation,source_audit:{path:$audit,sha256:$audit_sha},
             converter_report:$report,converter_report_sha256:$report_sha,
             converter_log:$log,converter_log_sha256:$log_sha,
             candidate_descriptor:$descriptor,candidate_descriptor_sha256:$descriptor_sha,
             recovered_switch:$recovered,backups_retained:true,
             target_post_audit_deferred:true,canonical_publication:false}
        ' >"$mc_result_candidate"
    ) || mc_die "cannot build result for epoch $mc_epoch"
    mc_publish_same "$mc_result_candidate" "$mc_result" \
        || mc_die "cannot publish result for epoch $mc_epoch"
    mc_validate_new_result "$mc_epoch" || mc_die "published result is invalid for epoch $mc_epoch"
    echo "epoch $mc_epoch: multicore Post candidate ready"
done <"$remaining_list"

result_bindings=$state_root/result-bindings.tsv
result_bindings_candidate=$result_bindings.building-$$
( set -C; : >"$result_bindings_candidate" ) || mc_die 'cannot create result binding candidate'
completed_new=0
while IFS= read -r mc_epoch || [ -n "$mc_epoch" ]; do
    mc_validate_new_result "$mc_epoch" || mc_die "final result is invalid for epoch $mc_epoch"
    mc_result=$results_dir/epoch-$mc_epoch.json
    printf '%s\t%s\t%s\n' "$mc_epoch" "$mc_result" "$(mc_sha256_file "$mc_result")" \
        >>"$result_bindings_candidate" || mc_die 'cannot extend result bindings'
    completed_new=$((completed_new + 1))
done <"$remaining_list"
[ "$completed_new" -eq "$remaining_count" ] || mc_die 'new completion count differs'
mc_publish_same "$result_bindings_candidate" "$result_bindings" \
    || mc_die 'result bindings changed on resume'
result_bindings_sha=$(mc_sha256_file "$result_bindings") || mc_die 'cannot hash results'
mc_process_guard end || mc_die 'an archive-root process is active at transition end'

complete=$state_root/batch-complete.json
if [ -e "$complete" ] || [ -L "$complete" ]; then
    jq -e --arg run "$run_id" --arg state "$state_root" --arg old_state "$old_state" \
        --arg prefix "$prefix_list" --arg prefix_sha "$prefix_sha" \
        --arg remaining "$remaining_list" --arg remaining_sha "$remaining_sha" \
        --arg bindings "$bindings" --arg bindings_sha "$bindings_sha" \
        --arg results "$result_bindings" --arg results_sha "$result_bindings_sha" \
        --arg operator "$operator_id" --argjson prefix_count "$prefix_count" \
        --argjson remaining_count "$remaining_count" --argjson threads "$threads" \
        --argjson old_last "$old_last_json" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-fast-multicore-batch-complete" and
        .run_id == $run and .state_root == $state and .threads == $threads and
        .old_prefix == {state_root:$old_state,count:$prefix_count,last_epoch:$old_last,
            epoch_list:{path:$prefix,sha256:$prefix_sha}} and
        .remaining == {count:$remaining_count,epoch_list:{path:$remaining,sha256:$remaining_sha},
            audit_bindings:{path:$bindings,sha256:$bindings_sha}} and
        .new_results == {count:$remaining_count,path:$results,sha256:$results_sha} and
        .operator_quiescence_authority_id == $operator and
        .backups_retained == true and .canonical_publication == false and
        (.completed_at_utc | type == "string")
    ' "$complete" >/dev/null 2>&1 || mc_die 'completion record changed on resume'
else
    completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) || mc_die 'cannot get completion time'
    complete_candidate=$complete.building-$$
    (
        set -C
        jq -cn --arg run "$run_id" --arg state "$state_root" --arg old_state "$old_state" \
            --arg prefix "$prefix_list" --arg prefix_sha "$prefix_sha" \
            --arg remaining "$remaining_list" --arg remaining_sha "$remaining_sha" \
            --arg bindings "$bindings" --arg bindings_sha "$bindings_sha" \
            --arg results "$result_bindings" --arg results_sha "$result_bindings_sha" \
            --arg operator "$operator_id" --arg completed "$completed_at" \
            --argjson prefix_count "$prefix_count" --argjson remaining_count "$remaining_count" \
            --argjson threads "$threads" --argjson old_last "$old_last_json" '
            {schema_version:1,kind:"archive-v2-pre-to-post-fast-multicore-batch-complete",
             run_id:$run,state_root:$state,threads:$threads,
             old_prefix:{state_root:$old_state,count:$prefix_count,last_epoch:$old_last,
                epoch_list:{path:$prefix,sha256:$prefix_sha}},
             remaining:{count:$remaining_count,
                epoch_list:{path:$remaining,sha256:$remaining_sha},
                audit_bindings:{path:$bindings,sha256:$bindings_sha}},
             new_results:{count:$remaining_count,path:$results,sha256:$results_sha},
             operator_quiescence_authority_id:$operator,
             one_converter_child_at_a_time:true,backups_retained:true,
             target_post_audit_deferred:true,canonical_publication:false,
             completed_at_utc:$completed}
        ' >"$complete_candidate"
    ) || mc_die 'cannot build completion record'
    mc_publish_same "$complete_candidate" "$complete" || mc_die 'cannot publish completion record'
fi

echo "multicore transition complete: old_prefix=$prefix_count new_epochs=$remaining_count threads=$threads"
