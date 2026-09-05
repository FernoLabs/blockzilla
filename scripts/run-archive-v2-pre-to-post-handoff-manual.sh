#!/bin/sh
# Wait for one corrected Compact Archive V2 wire-profile scan, build the exact
# Legacy Pre cohort, and start the reviewed manual Pre-to-Post runner. This is
# a one-shot manual handoff. It is not a scheduler.

set -eu
umask 077

die() {
    echo "archive-v2 Pre-to-Post handoff: $*" >&2
    exit 1
}

usage() {
    echo "usage: $0 COHORT_BUILDER CONVERTER_RUNNER CONVERTER BASE_REPORT_ROOT RESCAN_REPORT_ROOT ARCHIVE_ROOT TARGET_ROOT STATE_ROOT SCAN_PID_FILE FIRST_EPOCH LAST_EPOCH RESCAN_FIRST RESCAN_LAST EXCLUDE_EPOCH_LIST CLUSTER_ID RUN_ID POLL_SECONDS" >&2
    exit 2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 \
        || die "required command is not available: $1"
}

require_absolute_single_line() {
    ras_name=$1
    ras_value=$2
    case "$ras_value" in
        /*) ;;
        *) die "$ras_name must be an absolute path: $ras_value" ;;
    esac
    case "$ras_value" in
        *'
'*) die "$ras_name must not contain a line break" ;;
    esac
}

require_decimal() {
    rd_name=$1
    rd_value=$2
    case "$rd_value" in
        ''|*[!0-9]*) die "$rd_name must be a canonical nonnegative decimal integer" ;;
        0) ;;
        0*) die "$rd_name must not contain leading zeroes" ;;
        *) ;;
    esac
    [ "$(printf '%s' "$rd_value" | wc -c | tr -d '[:space:]')" -le 14 ] \
        || die "$rd_name is too large"
}

canonical_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

require_canonical_directory() {
    rcd_name=$1
    rcd_value=$2
    [ -d "$rcd_value" ] && [ ! -L "$rcd_value" ] \
        || die "$rcd_name is not one real directory: $rcd_value"
    rcd_canonical=$(canonical_directory "$rcd_value") \
        || die "cannot canonicalize $rcd_name: $rcd_value"
    [ "$rcd_canonical" = "$rcd_value" ] \
        || die "$rcd_name must already be canonical: $rcd_canonical"
}

sha256_file() (
    if [ "$sha256_program" = sha256sum ]; then
        sf_output=$(sha256sum "$1") || exit 1
    else
        sf_output=$(shasum -a 256 "$1") || exit 1
    fi
    printf '%s\n' "$sf_output" | awk '
        NR == 1 && length($1) == 64 && $1 !~ /[^0-9a-f]/ {
            print $1
            valid = 1
        }
        END { if (!valid) exit 1 }
    '
)

require_content_addressed_file() {
    rcaf_path=$1
    rcaf_kind=$2
    require_absolute_single_line "$rcaf_kind" "$rcaf_path"
    [ -f "$rcaf_path" ] && [ ! -L "$rcaf_path" ] && [ -x "$rcaf_path" ] \
        || die "$rcaf_kind is not one real executable file: $rcaf_path"
    rcaf_parent=$(dirname "$rcaf_path")
    rcaf_base=$(basename "$rcaf_path")
    rcaf_parent_canonical=$(canonical_directory "$rcaf_parent") \
        || die "cannot canonicalize $rcaf_kind parent: $rcaf_parent"
    if [ "$rcaf_parent_canonical" = / ]; then
        rcaf_expected_path=/$rcaf_base
    else
        rcaf_expected_path=$rcaf_parent_canonical/$rcaf_base
    fi
    [ "$rcaf_path" = "$rcaf_expected_path" ] \
        || die "$rcaf_kind path must already be canonical: $rcaf_expected_path"

    case "$rcaf_kind" in
        COHORT_BUILDER)
            rcaf_expected_sha=$(printf '%s\n' "$rcaf_base" | sed -n \
                's/^build-archive-v2-pre-to-post-cohort-\([0-9a-f]\{64\}\)\.sh$/\1/p')
            ;;
        CONVERTER_RUNNER)
            rcaf_expected_sha=$(printf '%s\n' "$rcaf_base" | sed -n \
                's/^run-archive-v2-pre-to-post-manual-\([0-9a-f]\{64\}\)\.sh$/\1/p')
            ;;
        CONVERTER)
            rcaf_expected_sha=$(printf '%s\n' "$rcaf_base" | sed -n \
                's/^archive-v2-pre-to-post-\([0-9a-f]\{64\}\)$/\1/p')
            ;;
        *) die "internal unknown content-addressed file kind: $rcaf_kind" ;;
    esac
    [ -n "$rcaf_expected_sha" ] \
        || die "$rcaf_kind does not have the required content-addressed basename: $rcaf_base"
    rcaf_actual_sha=$(sha256_file "$rcaf_path") \
        || die "cannot hash $rcaf_kind: $rcaf_path"
    [ "$rcaf_actual_sha" = "$rcaf_expected_sha" ] \
        || die "$rcaf_kind content does not match its content-addressed basename: $rcaf_path"
    printf '%s\n' "$rcaf_actual_sha"
}

check_bound_file_hash() {
    cbfh_path=$1
    cbfh_expected=$2
    cbfh_kind=$3
    cbfh_actual=$(sha256_file "$cbfh_path") \
        || die "cannot rehash $cbfh_kind: $cbfh_path"
    [ "$cbfh_actual" = "$cbfh_expected" ] \
        || die "$cbfh_kind changed after handoff admission: $cbfh_path"
}

validate_scanner_report() (
    vsr_report=$1
    vsr_epoch=$2
    vsr_archive=$3

    [ -f "$vsr_report" ] && [ ! -L "$vsr_report" ] || exit 1
    jq -e -s \
        --argjson epoch "$vsr_epoch" \
        --arg archive "$vsr_archive" '
        def count:
            type == "number" and . >= 0 and floor == . and . <= 99999999999999;
        def location:
            type == "object" and
            (.slot | count) and
            (.transaction_index | count) and
            .transaction_index <= 4294967295;
        length == 1 and
        (.[0] as $r |
            ($r | type) == "object" and
            $r.schema_version == 1 and
            $r.kind == "archive-v2-wire-profile-scan" and
            $r.epoch == $epoch and
            $r.archive == $archive and
            $r.error == null and
            ($r.workers | count) and $r.workers > 0 and
            ($r.elapsed_seconds | type) == "number" and $r.elapsed_seconds >= 0 and
            ($r.completed_unix_seconds | count) and
            ($r.counts | type) == "object" and
            ($r.counts.blocks | count) and
            ($r.counts.compressed_block_bytes | count) and
            ($r.counts.uncompressed_block_bytes | count) and
            ($r.counts.typed_messages | count) and
            ($r.counts.owned_fallback_blocks | count) and
            ($r.counts.raw_transaction_fallbacks | count) and
            ($r.counts.post_only | count) and
            ($r.counts.pre_only | count) and
            ($r.counts.both_equivalent | count) and
            ($r.counts.both_divergent | count) and
            ($r.counts.invalid | count) and
            $r.counts.owned_fallback_blocks == 0 and
            $r.counts.raw_transaction_fallbacks == 0 and
            $r.counts.both_divergent == 0 and
            $r.counts.invalid == 0 and
            $r.counts.typed_messages ==
                ($r.counts.post_only + $r.counts.pre_only + $r.counts.both_equivalent) and
            ($r.first_evidence | type) == "object" and
            (
                ($r.classification == "canonical-post" and
                    $r.action == "none" and
                    $r.counts.post_only > 0 and
                    $r.counts.pre_only == 0 and
                    ($r.first_evidence.post_only | location) and
                    $r.first_evidence.pre_only == null and
                    $r.first_evidence.both_divergent == null and
                    $r.first_evidence.invalid == null) or
                ($r.classification == "legacy-pre" and
                    $r.action == "convert-to-post" and
                    $r.counts.pre_only > 0 and
                    $r.counts.post_only == 0 and
                    ($r.first_evidence.pre_only | location) and
                    $r.first_evidence.post_only == null and
                    $r.first_evidence.both_divergent == null and
                    $r.first_evidence.invalid == null) or
                ($r.classification == "canonical-equivalent" and
                    $r.action == "none" and
                    $r.counts.post_only == 0 and
                    $r.counts.pre_only == 0 and
                    $r.counts.both_equivalent == $r.counts.typed_messages and
                    $r.first_evidence.post_only == null and
                    $r.first_evidence.pre_only == null and
                    $r.first_evidence.both_divergent == null and
                    $r.first_evidence.invalid == null)
            )
        )
    ' "$vsr_report" >/dev/null 2>&1
)

count_building_files() {
    cbf_count=0
    cbf_epoch=$rescan_first
    while [ "$cbf_epoch" -le "$rescan_last" ]; do
        for cbf_path in \
            "$rescan_report_root/epoch-$cbf_epoch.json.building-"* \
            "$rescan_report_root/epoch-$cbf_epoch.log.building-"*; do
            if [ -e "$cbf_path" ] || [ -L "$cbf_path" ]; then
                cbf_count=$((cbf_count + 1))
            fi
        done
        cbf_epoch=$((cbf_epoch + 1))
    done
    printf '%s\n' "$cbf_count"
}

validate_pid_file() {
    [ -f "$scan_pid_file" ] && [ ! -L "$scan_pid_file" ] \
        || die "SCAN_PID_FILE is not one real file: $scan_pid_file"
    vpf_pid=$(awk '
        NR == 1 && /^(0|[1-9][0-9]*)$/ && $0 != "0" && length($0) <= 10 {
            value = $0
            valid = 1
            next
        }
        { valid = 0; extra = 1 }
        END {
            if (!valid || extra || NR != 1) exit 1
            print value
        }
    ' "$scan_pid_file") || die "SCAN_PID_FILE does not contain exactly one positive canonical PID"
    [ "$vpf_pid" -le 4294967295 ] 2>/dev/null \
        || die "SCAN_PID_FILE PID is outside the supported range"
    printf '%s\n' "$vpf_pid"
}

check_pid_file_unchanged() {
    cpf_pid=$(validate_pid_file)
    [ "$cpf_pid" = "$scan_pid" ] \
        || die "SCAN_PID_FILE changed to a different PID during the handoff"
    cpf_sha=$(sha256_file "$scan_pid_file") \
        || die "cannot hash SCAN_PID_FILE"
    [ "$cpf_sha" = "$scan_pid_file_sha" ] \
        || die "SCAN_PID_FILE changed during the handoff"
}

scan_process_is_live() {
    kill -0 "$scan_pid" 2>/dev/null
}

inspect_corrected_reports() {
    corrected_valid=0
    corrected_missing=0
    corrected_invalid_epoch=
    icr_epoch=$rescan_first
    while [ "$icr_epoch" -le "$rescan_last" ]; do
        icr_report=$rescan_report_root/epoch-$icr_epoch.json
        icr_archive=$archive_root/epoch-$icr_epoch
        if [ ! -e "$icr_report" ] && [ ! -L "$icr_report" ]; then
            corrected_missing=$((corrected_missing + 1))
        elif validate_scanner_report "$icr_report" "$icr_epoch" "$icr_archive"; then
            corrected_valid=$((corrected_valid + 1))
        else
            corrected_invalid_epoch=$icr_epoch
            return 1
        fi
        icr_epoch=$((icr_epoch + 1))
    done
}

snapshot_corrected_reports() {
    scr_output=$1
    : >"$scr_output" || return 1
    scr_epoch=$rescan_first
    while [ "$scr_epoch" -le "$rescan_last" ]; do
        scr_report=$rescan_report_root/epoch-$scr_epoch.json
        scr_archive=$archive_root/epoch-$scr_epoch
        validate_scanner_report "$scr_report" "$scr_epoch" "$scr_archive" \
            || return 1
        scr_sha=$(sha256_file "$scr_report") || return 1
        printf '%s\t%s\n' "$scr_epoch" "$scr_sha" >>"$scr_output" || return 1
        scr_epoch=$((scr_epoch + 1))
    done
}

log_status() {
    ls_timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
        || die "cannot get UTC status time"
    printf '%s phase=%s %s\n' "$ls_timestamp" "$1" "$2" >>"$status_log" \
        || die "cannot append handoff status: $status_log"
}

validate_epoch_list() {
    awk '
        !/^(0|[1-9][0-9]*)$/ || length($0) > 14 {
            failed = 1
            next
        }
        seen[$0]++ { failed = 1; next }
        previous != "" && ($0 + 0) <= (previous + 0) { failed = 1 }
        { previous = $0; count++ }
        END {
            if (count == 0 || failed) exit 1
            print count
        }
    ' "$1"
}

validate_cohort_output() {
    [ -d "$cohort_dir" ] && [ ! -L "$cohort_dir" ] || return 1
    vco_epochs=$cohort_dir/epochs.txt
    vco_json=$cohort_dir/cohort.json
    for vco_file in "$vco_epochs" "$vco_json"; do
        [ -f "$vco_file" ] && [ ! -L "$vco_file" ] || return 1
    done
    vco_actual_count=0
    for vco_entry in "$cohort_dir"/* "$cohort_dir"/.[!.]* "$cohort_dir"/..?*; do
        if [ ! -e "$vco_entry" ] && [ ! -L "$vco_entry" ]; then
            continue
        fi
        [ -f "$vco_entry" ] && [ ! -L "$vco_entry" ] || return 1
        case "${vco_entry##*/}" in
            epochs.txt|cohort.json) ;;
            *) return 1 ;;
        esac
        vco_actual_count=$((vco_actual_count + 1))
    done
    [ "$vco_actual_count" -eq 2 ] || return 1

    vco_epoch_count=$(validate_epoch_list "$vco_epochs") || return 1
    vco_epoch_sha_before=$(sha256_file "$vco_epochs") || return 1
    vco_json_sha_before=$(sha256_file "$vco_json") || return 1
    vco_epochs_json=$(jq -Rsc \
        'split("\n") | map(select(length > 0) | tonumber)' "$vco_epochs") \
        || return 1
    vco_selected_count=$((last_epoch - first_epoch + 1))
    vco_rescan_count=$((rescan_last - rescan_first + 1))
    vco_base_count=$((vco_selected_count - vco_rescan_count))
    jq -e \
        --arg base "$base_report_root" \
        --arg rescan "$rescan_report_root" \
        --arg archive "$archive_root" \
        --arg target "$target_root" \
        --arg exclude "$exclude_file" \
        --arg exclude_sha "$exclude_file_sha" \
        --arg epoch_sha "$vco_epoch_sha_before" \
        --argjson first "$first_epoch" \
        --argjson last "$last_epoch" \
        --argjson rescan_first "$rescan_first" \
        --argjson rescan_last "$rescan_last" \
        --argjson selected_count "$vco_selected_count" \
        --argjson rescan_count "$vco_rescan_count" \
        --argjson base_count "$vco_base_count" \
        --argjson excluded_epochs "$exclude_epochs_json" \
        --argjson epoch_count "$vco_epoch_count" \
        --argjson epochs "$vco_epochs_json" '
        . as $cohort |
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-cohort" and
        .archive_root == $archive and
        .target_root == $target and
        .source_scanner_roots == {base: $base, rescan: $rescan} and
        .selected_epoch_range == {first: $first, last: $last, count: $selected_count} and
        .rescan_epoch_range == {
            first: $rescan_first,
            last: $rescan_last,
            count: $rescan_count
        } and
        .selected_report_count == $selected_count and
        .selected_report_set_counts == {base: $base_count, rescan: $rescan_count} and
        (.reports | length) == $selected_count and
        ((.reports | map(.epoch) | sort) == [range($first; $last + 1)]) and
        all(.reports[];
            (.report_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
            .archive == ($archive + "/epoch-" + (.epoch | tostring)) and
            (
                (.epoch >= $rescan_first and .epoch <= $rescan_last and
                    .report_set == "rescan" and
                    .report_path == ($rescan + "/epoch-" + (.epoch | tostring) + ".json")) or
                ((.epoch < $rescan_first or .epoch > $rescan_last) and
                    .report_set == "base" and
                    .report_path == ($base + "/epoch-" + (.epoch | tostring) + ".json"))
            )) and
        .selected_counts.owned_fallback_blocks == 0 and
        .selected_counts.raw_transaction_fallbacks == 0 and
        .selected_counts.both_divergent == 0 and
        .selected_counts.invalid == 0 and
        .exclude_epoch_list == $exclude and
        .exclude_epoch_list_sha256 == $exclude_sha and
        .excluded_epochs == $excluded_epochs and
        (.excluded_targets | map(.epoch)) == $excluded_epochs and
        all(.excluded_targets[];
            . as $excluded_target |
            $excluded_target.target ==
                ($target + "/epoch-" + ($excluded_target.epoch | tostring)) and
            $excluded_target.source_report_sha256 ==
                ([$cohort.reports[] |
                    select(.epoch == $excluded_target.epoch) | .report_sha256][0])) and
        .epochs_file == "epochs.txt" and
        .epoch_list_sha256 == $epoch_sha and
        .conversion_epoch_count == $epoch_count and
        .conversion_epochs == $epochs and
        all(.conversion_epochs[];
            . as $epoch |
            any($cohort.reports[];
                .epoch == $epoch and .classification == "legacy-pre")) and
        ([.reports[] | select(.classification == "legacy-pre") | .epoch] ==
            (($excluded_epochs + $epochs) | sort)) and
        (.classification_totals.canonical_post +
            .classification_totals.legacy_pre +
            .classification_totals.canonical_equivalent == $selected_count)
    ' "$vco_json" >/dev/null 2>&1 || return 1

    vco_report_entries=$(jq -r \
        '.reports[] | [.report_path, .report_sha256] | @tsv' "$vco_json") \
        || return 1
    vco_tab=$(printf '\t')
    while IFS="$vco_tab" read -r vco_report_path vco_report_sha; do
        [ -f "$vco_report_path" ] && [ ! -L "$vco_report_path" ] || return 1
        vco_actual_report_sha=$(sha256_file "$vco_report_path") || return 1
        [ "$vco_actual_report_sha" = "$vco_report_sha" ] || return 1
    done <<EOF
$vco_report_entries
EOF

    vco_snapshot=$state_root/.cohort-rescan-snapshot-$$
    [ ! -e "$vco_snapshot" ] && [ ! -L "$vco_snapshot" ] || return 1
    jq -r \
        --argjson first "$rescan_first" \
        --argjson last "$rescan_last" '
        .reports[] |
        select(.epoch >= $first and .epoch <= $last) |
        [.epoch, .report_sha256] | @tsv
    ' "$vco_json" | sort -n >"$vco_snapshot" || return 1
    if ! cmp -s "$vco_snapshot" "$rescan_snapshot_file"; then
        rm -f "$vco_snapshot" 2>/dev/null || :
        return 1
    fi
    rm -f "$vco_snapshot" || return 1

    vco_epoch_sha_after=$(sha256_file "$vco_epochs") || return 1
    vco_json_sha_after=$(sha256_file "$vco_json") || return 1
    [ "$vco_epoch_sha_after" = "$vco_epoch_sha_before" ] || return 1
    [ "$vco_json_sha_after" = "$vco_json_sha_before" ] || return 1
    cohort_epochs_sha=$vco_epoch_sha_before
    cohort_json_sha=$vco_json_sha_before
    cohort_epoch_count=$vco_epoch_count
    cohort_epochs_json=$vco_epochs_json
}

validate_manual_complete() {
    vmc_path=$conversion_state/complete.json
    [ -f "$vmc_path" ] && [ ! -L "$vmc_path" ] || return 1
    vmc_sha_before=$(sha256_file "$vmc_path") || return 1
    jq -e -s \
        --arg run_id "$run_id" \
        --arg cluster "$cluster_id" \
        --arg converter "$converter" \
        --arg converter_sha "$converter_sha" \
        --arg source "$archive_root" \
        --arg target "$target_root" \
        --arg state "$conversion_state" \
        --arg epochs "$cohort_dir/epochs.txt" \
        --arg epochs_sha "$cohort_epochs_sha" \
        --argjson count "$cohort_epoch_count" \
        --argjson epoch_values "$cohort_epochs_json" '
        length == 1 and
        (.[0] as $c |
            $c.schema_version == 1 and
            $c.kind == "archive-v2-pre-to-post-manual-run-complete" and
            $c.run_id == $run_id and
            $c.cluster_id == $cluster and
            $c.converter == $converter and
            $c.converter_sha256 == $converter_sha and
            $c.source_root == $source and
            $c.target_root == $target and
            $c.state_root == $state and
            $c.epoch_list == $epochs and
            $c.epoch_list_sha256 == $epochs_sha and
            $c.source_authority_kind == "linux-kernel-read-leases" and
            $c.epoch_count == $count and
            $c.completed_epochs == $count and
            $c.epochs == $epoch_values and
            ($c.completed_at_utc | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")))
    ' "$vmc_path" >/dev/null 2>&1 || return 1
    vmc_sha_after=$(sha256_file "$vmc_path") || return 1
    [ "$vmc_sha_after" = "$vmc_sha_before" ] || return 1
    conversion_complete_sha=$vmc_sha_before
}

[ "$#" -eq 17 ] || usage

cohort_builder=$1
converter_runner=$2
converter=$3
base_report_root=$4
rescan_report_root=$5
archive_root=$6
target_root=$7
state_root=$8
scan_pid_file=$9
first_epoch=${10}
last_epoch=${11}
rescan_first=${12}
rescan_last=${13}
exclude_epoch_list_input=${14}
cluster_id=${15}
run_id=${16}
poll_seconds=${17}

for required in jq awk wc tr date sort dirname basename mkdir chmod mv rm rmdir \
    cmp sleep sed kill cp; do
    require_command "$required"
done
if command -v sha256sum >/dev/null 2>&1; then
    sha256_program=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    sha256_program=shasum
else
    die 'sha256sum or shasum is required'
fi

require_absolute_single_line BASE_REPORT_ROOT "$base_report_root"
require_absolute_single_line RESCAN_REPORT_ROOT "$rescan_report_root"
require_absolute_single_line ARCHIVE_ROOT "$archive_root"
require_absolute_single_line TARGET_ROOT "$target_root"
require_absolute_single_line STATE_ROOT "$state_root"
require_absolute_single_line SCAN_PID_FILE "$scan_pid_file"
require_absolute_single_line EXCLUDE_EPOCH_LIST "$exclude_epoch_list_input"
require_decimal FIRST_EPOCH "$first_epoch"
require_decimal LAST_EPOCH "$last_epoch"
require_decimal RESCAN_FIRST "$rescan_first"
require_decimal RESCAN_LAST "$rescan_last"
require_decimal POLL_SECONDS "$poll_seconds"
[ "$poll_seconds" -ge 1 ] && [ "$poll_seconds" -le 86400 ] \
    || die 'POLL_SECONDS must be between 1 and 86400'
[ "$first_epoch" -le "$last_epoch" ] || die 'FIRST_EPOCH is greater than LAST_EPOCH'
[ "$rescan_first" -le "$rescan_last" ] || die 'RESCAN_FIRST is greater than RESCAN_LAST'
[ "$rescan_first" -ge "$first_epoch" ] && [ "$rescan_last" -le "$last_epoch" ] \
    || die 'the rescan range must be inside the selected epoch range'
case "$cluster_id" in
    ''|*[!A-Za-z0-9._-]*) die 'CLUSTER_ID contains unsupported characters' ;;
esac
case "$run_id" in
    ''|*[!A-Za-z0-9._-]*) die 'RUN_ID contains unsupported characters' ;;
esac
[ "$(printf '%s' "$cluster_id" | wc -c | tr -d '[:space:]')" -le 128 ] \
    || die 'CLUSTER_ID is longer than 128 bytes'
[ "$(printf '%s' "$run_id" | wc -c | tr -d '[:space:]')" -le 128 ] \
    || die 'RUN_ID is longer than 128 bytes'

cohort_builder_sha=$(require_content_addressed_file "$cohort_builder" COHORT_BUILDER)
converter_runner_sha=$(require_content_addressed_file "$converter_runner" CONVERTER_RUNNER)
converter_sha=$(require_content_addressed_file "$converter" CONVERTER)

require_canonical_directory BASE_REPORT_ROOT "$base_report_root"
require_canonical_directory RESCAN_REPORT_ROOT "$rescan_report_root"
require_canonical_directory ARCHIVE_ROOT "$archive_root"
require_canonical_directory TARGET_ROOT "$target_root"
[ -f "$exclude_epoch_list_input" ] && [ ! -L "$exclude_epoch_list_input" ] \
    || die "EXCLUDE_EPOCH_LIST is not one real file: $exclude_epoch_list_input"
exclude_epoch_list_parent=$(dirname "$exclude_epoch_list_input")
exclude_epoch_list_base=$(basename "$exclude_epoch_list_input")
exclude_epoch_list_parent_canonical=$(canonical_directory "$exclude_epoch_list_parent") \
    || die "cannot canonicalize EXCLUDE_EPOCH_LIST parent"
if [ "$exclude_epoch_list_parent_canonical" = / ]; then
    exclude_epoch_list_expected=/$exclude_epoch_list_base
else
    exclude_epoch_list_expected=$exclude_epoch_list_parent_canonical/$exclude_epoch_list_base
fi
[ "$exclude_epoch_list_input" = "$exclude_epoch_list_expected" ] \
    || die "EXCLUDE_EPOCH_LIST path must already be canonical: $exclude_epoch_list_expected"
exclude_epoch_count=$(validate_epoch_list "$exclude_epoch_list_input") \
    || die 'EXCLUDE_EPOCH_LIST must be a nonempty, strictly increasing, unique decimal epoch list'
while IFS= read -r excluded_epoch || [ -n "$excluded_epoch" ]; do
    [ "$excluded_epoch" -ge "$first_epoch" ] && [ "$excluded_epoch" -le "$last_epoch" ] \
        || die "excluded epoch is outside the selected range: $excluded_epoch"
done <"$exclude_epoch_list_input"
exclude_epoch_list_source_sha=$(sha256_file "$exclude_epoch_list_input") \
    || die 'cannot hash EXCLUDE_EPOCH_LIST'
exclude_epochs_json=$(jq -Rsc \
    'split("\n") | map(select(length > 0) | tonumber)' "$exclude_epoch_list_input") \
    || die 'cannot encode EXCLUDE_EPOCH_LIST'
[ "$base_report_root" != "$rescan_report_root" ] \
    || die 'base and corrected report roots must differ'
[ "$archive_root" != "$target_root" ] || die 'archive and target roots must differ'

mkdir -p "$state_root" || die "cannot create handoff state root: $state_root"
chmod 700 "$state_root" || die "cannot set handoff state root mode 0700: $state_root"
require_canonical_directory STATE_ROOT "$state_root"
for protected_root in "$base_report_root" "$rescan_report_root" "$archive_root" "$target_root"; do
    case "$state_root/" in
        "$protected_root"/*) die "STATE_ROOT must not be inside protected root: $protected_root" ;;
    esac
    case "$protected_root/" in
        "$state_root"/*) die "protected root must not be inside STATE_ROOT: $protected_root" ;;
    esac
done

state_lock=$state_root/.archive-v2-pre-to-post-handoff.lock
target_lock=$target_root/.archive-v2-pre-to-post-handoff.lock
state_lock_held=0
target_lock_held=0
preserve_locks=0
release_locks() {
    rl_status=$?
    trap - 0 1 2 3 15
    if [ "${preserve_locks:-0}" -eq 0 ]; then
        if [ "${target_lock_held:-0}" -eq 1 ]; then
            rmdir "$target_lock" 2>/dev/null \
                || echo "archive-v2 Pre-to-Post handoff: cannot remove target lock: $target_lock" >&2
        fi
        if [ "${state_lock_held:-0}" -eq 1 ]; then
            rmdir "$state_lock" 2>/dev/null \
                || echo "archive-v2 Pre-to-Post handoff: cannot remove state lock: $state_lock" >&2
        fi
    else
        echo "archive-v2 Pre-to-Post handoff: interruption preserved both locks for review" >&2
    fi
    exit "$rl_status"
}
interrupted() {
    preserve_locks=1
    exit "$1"
}
trap 'release_locks' 0
trap 'interrupted 129' 1
trap 'interrupted 130' 2
trap 'interrupted 131' 3
trap 'interrupted 143' 15

if ! mkdir "$state_lock" 2>/dev/null; then
    die "handoff state lock exists; another or interrupted handoff needs review: $state_lock"
fi
state_lock_held=1
chmod 700 "$state_lock" || die "cannot set state lock mode 0700"
if ! mkdir "$target_lock" 2>/dev/null; then
    die "target publisher lock exists; another or interrupted publisher needs review: $target_lock"
fi
target_lock_held=1
chmod 700 "$target_lock" || die "cannot set target publisher lock mode 0700"

status_log=$state_root/status.log
if [ -e "$status_log" ] || [ -L "$status_log" ]; then
    [ -f "$status_log" ] && [ ! -L "$status_log" ] \
        || die "status log is not one real file: $status_log"
else
    : >"$status_log" || die "cannot create status log: $status_log"
fi
chmod 600 "$status_log" || die "cannot set status log mode 0600"
log_status admitted "run_id=$run_id scanner_range=$rescan_first..$rescan_last"

scan_pid=$(validate_pid_file)
scan_pid_file_sha=$(sha256_file "$scan_pid_file") || die 'cannot hash SCAN_PID_FILE'

request_file=$state_root/request.json
request_building=$request_file.building-$$
[ ! -e "$request_building" ] && [ ! -L "$request_building" ] \
    || die "PID-specific request file already exists: $request_building"
jq -cn \
    --arg builder "$cohort_builder" \
    --arg builder_sha "$cohort_builder_sha" \
    --arg runner "$converter_runner" \
    --arg runner_sha "$converter_runner_sha" \
    --arg converter "$converter" \
    --arg converter_sha "$converter_sha" \
    --arg base "$base_report_root" \
    --arg rescan "$rescan_report_root" \
    --arg archive "$archive_root" \
    --arg target "$target_root" \
    --arg state "$state_root" \
    --arg pid_file "$scan_pid_file" \
    --arg pid_file_sha "$scan_pid_file_sha" \
    --arg exclude_source "$exclude_epoch_list_input" \
    --arg exclude_source_sha "$exclude_epoch_list_source_sha" \
    --argjson pid "$scan_pid" \
    --argjson first "$first_epoch" \
    --argjson last "$last_epoch" \
    --argjson rescan_first "$rescan_first" \
    --argjson rescan_last "$rescan_last" \
    --argjson excluded "$exclude_epochs_json" \
    --arg cluster "$cluster_id" \
    --arg run_id "$run_id" '
    {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-manual-handoff-request",
        cohort_builder: $builder,
        cohort_builder_sha256: $builder_sha,
        converter_runner: $runner,
        converter_runner_sha256: $runner_sha,
        converter: $converter,
        converter_sha256: $converter_sha,
        base_report_root: $base,
        rescan_report_root: $rescan,
        archive_root: $archive,
        target_root: $target,
        state_root: $state,
        scan_pid_file: $pid_file,
        scan_pid_file_sha256: $pid_file_sha,
        scan_pid: $pid,
        first_epoch: $first,
        last_epoch: $last,
        rescan_first: $rescan_first,
        rescan_last: $rescan_last,
        exclude_epoch_list_source: $exclude_source,
        exclude_epoch_list_source_sha256: $exclude_source_sha,
        excluded_epochs: $excluded,
        cluster_id: $cluster,
        run_id: $run_id
    }
' >"$request_building" || die 'cannot create exact handoff request'
if [ -e "$request_file" ] || [ -L "$request_file" ]; then
    [ -f "$request_file" ] && [ ! -L "$request_file" ] \
        || die "request path is not one real file: $request_file"
    cmp -s "$request_building" "$request_file" \
        || die "existing state belongs to a different exact handoff request: $request_file"
    rm -f "$request_building" || die 'cannot remove duplicate request candidate'
else
    mv "$request_building" "$request_file" || die 'cannot atomically publish request.json'
fi
chmod 600 "$request_file" || die 'cannot set request.json mode 0600'
request_sha=$(sha256_file "$request_file") || die 'cannot hash request.json'

exclude_file=$state_root/exclude-epochs.txt
exclude_candidate=$exclude_file.building-$$
[ ! -e "$exclude_candidate" ] && [ ! -L "$exclude_candidate" ] \
    || die "PID-specific exclusion candidate already exists: $exclude_candidate"
cp "$exclude_epoch_list_input" "$exclude_candidate" \
    || die 'cannot copy exclusion-list candidate into private state'
exclude_epoch_list_source_sha_after=$(sha256_file "$exclude_epoch_list_input") \
    || die 'cannot rehash EXCLUDE_EPOCH_LIST'
[ "$exclude_epoch_list_source_sha_after" = "$exclude_epoch_list_source_sha" ] \
    || die 'EXCLUDE_EPOCH_LIST changed while it was copied'
exclude_candidate_sha=$(sha256_file "$exclude_candidate") \
    || die 'cannot hash copied exclusion-list candidate'
[ "$exclude_candidate_sha" = "$exclude_epoch_list_source_sha" ] \
    || die 'private exclusion-list copy does not match its source'
if [ -e "$exclude_file" ] || [ -L "$exclude_file" ]; then
    [ -f "$exclude_file" ] && [ ! -L "$exclude_file" ] \
        || die "exclusion list is not one real file: $exclude_file"
    cmp -s "$exclude_candidate" "$exclude_file" \
        || die 'existing private exclusion list differs from its bound source'
    rm -f "$exclude_candidate" || die 'cannot remove duplicate exclusion candidate'
else
    mv "$exclude_candidate" "$exclude_file" || die 'cannot publish exclusion list'
fi
chmod 600 "$exclude_file" || die 'cannot set exclusion-list mode 0600'
exclude_file_sha=$(sha256_file "$exclude_file") || die 'cannot hash exclusion list'

rescan_snapshot_file=$state_root/rescan-reports.sha256
snapshot_before=$state_root/.rescan-reports.before-$$
snapshot_after=$state_root/.rescan-reports.after-$$
for stale_path in "$snapshot_before" "$snapshot_after"; do
    [ ! -e "$stale_path" ] && [ ! -L "$stale_path" ] \
        || die "PID-specific rescan snapshot already exists: $stale_path"
done

last_wait_state=
while :; do
    check_pid_file_unchanged
    if ! inspect_corrected_reports; then
        die "corrected scanner report for epoch $corrected_invalid_epoch is invalid"
    fi
    building_count=$(count_building_files)
    if scan_process_is_live; then
        scan_live=1
    else
        scan_live=0
    fi
    wait_state=$corrected_valid/$corrected_missing/$building_count/$scan_live
    if [ "$wait_state" != "$last_wait_state" ]; then
        log_status waiting \
            "valid=$corrected_valid missing=$corrected_missing building=$building_count scan_pid_live=$scan_live"
        last_wait_state=$wait_state
    fi

    if [ "$corrected_missing" -gt 0 ]; then
        [ "$scan_live" -eq 1 ] \
            || die "corrected scan process stopped with $corrected_missing reports missing"
        sleep "$poll_seconds"
        continue
    fi
    if [ "$building_count" -gt 0 ]; then
        [ "$scan_live" -eq 1 ] \
            || die "corrected scan stopped but building files remain"
        sleep "$poll_seconds"
        continue
    fi
    if [ "$scan_live" -eq 1 ]; then
        sleep "$poll_seconds"
        continue
    fi
    break
done

snapshot_corrected_reports "$snapshot_before" \
    || die 'cannot create the first stable corrected-report snapshot'
log_status stabilizing "all corrected reports are valid; quiet_seconds=$poll_seconds"
sleep "$poll_seconds"
check_pid_file_unchanged
scan_process_is_live && die 'corrected scan process restarted during the quiet interval'
[ "$(count_building_files)" -eq 0 ] \
    || die 'a building report appeared during the quiet interval'
snapshot_corrected_reports "$snapshot_after" \
    || die 'cannot create the second stable corrected-report snapshot'
cmp -s "$snapshot_before" "$snapshot_after" \
    || die 'corrected reports changed during the quiet interval'
rm -f "$snapshot_after" || die 'cannot remove second corrected-report snapshot'
if [ -e "$rescan_snapshot_file" ] || [ -L "$rescan_snapshot_file" ]; then
    [ -f "$rescan_snapshot_file" ] && [ ! -L "$rescan_snapshot_file" ] \
        || die "rescan snapshot path is not one real file: $rescan_snapshot_file"
    cmp -s "$snapshot_before" "$rescan_snapshot_file" \
        || die 'corrected reports differ from the prior handoff admission'
    rm -f "$snapshot_before" || die 'cannot remove duplicate corrected-report snapshot'
else
    mv "$snapshot_before" "$rescan_snapshot_file" \
        || die 'cannot atomically publish corrected-report snapshot'
fi
chmod 600 "$rescan_snapshot_file" || die 'cannot set corrected-report snapshot mode 0600'
rescan_snapshot_sha=$(sha256_file "$rescan_snapshot_file") \
    || die 'cannot hash corrected-report snapshot'
log_status scan_ready \
    "reports=$corrected_valid snapshot_sha256=$rescan_snapshot_sha"

check_bound_file_hash "$cohort_builder" "$cohort_builder_sha" COHORT_BUILDER
check_bound_file_hash "$converter_runner" "$converter_runner_sha" CONVERTER_RUNNER
check_bound_file_hash "$converter" "$converter_sha" CONVERTER

cohort_dir=$state_root/cohort
cohort_binding=$state_root/cohort.binding.json
if [ -e "$cohort_dir" ] || [ -L "$cohort_dir" ] \
    || [ -e "$cohort_binding" ] || [ -L "$cohort_binding" ]; then
    [ -d "$cohort_dir" ] && [ ! -L "$cohort_dir" ] \
        || die 'cohort output is absent or is not one real directory'
    [ -f "$cohort_binding" ] && [ ! -L "$cohort_binding" ] \
        || die 'cohort output exists without its exact binding record'
    validate_cohort_output || die 'existing cohort output is invalid or changed'
    jq -e -s \
        --arg request_sha "$request_sha" \
        --arg builder_sha "$cohort_builder_sha" \
        --arg snapshot_sha "$rescan_snapshot_sha" \
        --arg cohort_sha "$cohort_json_sha" \
        --arg epochs_sha "$cohort_epochs_sha" \
        --argjson epoch_count "$cohort_epoch_count" '
        length == 1 and
        (.[0] as $b |
            $b.schema_version == 1 and
            $b.kind == "archive-v2-pre-to-post-cohort-binding" and
            $b.request_sha256 == $request_sha and
            $b.cohort_builder_sha256 == $builder_sha and
            $b.rescan_reports_sha256 == $snapshot_sha and
            $b.cohort_json_sha256 == $cohort_sha and
            $b.epochs_sha256 == $epochs_sha and
            $b.epoch_count == $epoch_count)
    ' "$cohort_binding" >/dev/null 2>&1 \
        || die 'existing cohort binding does not match this exact handoff'
    log_status cohort_ready "accepted existing cohort epochs=$cohort_epoch_count"
else
    for old_builder_log in "$state_root"/cohort-builder.*.building-* \
        "$state_root"/cohort-builder.*.failed-*; do
        if [ -e "$old_builder_log" ] || [ -L "$old_builder_log" ]; then
            die "old cohort-builder state needs review: $old_builder_log"
        fi
    done
    builder_stdout=$state_root/cohort-builder.stdout.log.building-$$
    builder_stderr=$state_root/cohort-builder.stderr.log.building-$$
    log_status cohort_building "output=$cohort_dir"
    if "$cohort_builder" \
        "$base_report_root" "$rescan_report_root" "$archive_root" "$target_root" \
        "$cohort_dir" "$first_epoch" "$last_epoch" "$rescan_first" "$rescan_last" \
        "$exclude_file" >"$builder_stdout" 2>"$builder_stderr"; then
        builder_status=0
    else
        builder_status=$?
    fi
    check_bound_file_hash "$cohort_builder" "$cohort_builder_sha" COHORT_BUILDER
    if [ "$builder_status" -ne 0 ]; then
        mv "$builder_stdout" "$builder_stdout.failed-$builder_status" 2>/dev/null || :
        mv "$builder_stderr" "$builder_stderr.failed-$builder_status" 2>/dev/null || :
        die "cohort builder exited with status $builder_status"
    fi
    validate_cohort_output || die 'new cohort output is invalid'
    [ ! -e "$state_root/cohort-builder.stdout.log" ] \
        && [ ! -L "$state_root/cohort-builder.stdout.log" ] \
        || die 'cohort-builder stdout log appeared during the build'
    [ ! -e "$state_root/cohort-builder.stderr.log" ] \
        && [ ! -L "$state_root/cohort-builder.stderr.log" ] \
        || die 'cohort-builder stderr log appeared during the build'
    mv "$builder_stdout" "$state_root/cohort-builder.stdout.log" \
        || die 'cannot publish cohort-builder stdout log'
    mv "$builder_stderr" "$state_root/cohort-builder.stderr.log" \
        || die 'cannot publish cohort-builder stderr log'

    binding_building=$cohort_binding.building-$$
    [ ! -e "$binding_building" ] && [ ! -L "$binding_building" ] \
        || die "PID-specific cohort binding already exists: $binding_building"
    jq -cn \
        --arg request_sha "$request_sha" \
        --arg builder_sha "$cohort_builder_sha" \
        --arg snapshot_sha "$rescan_snapshot_sha" \
        --arg cohort_sha "$cohort_json_sha" \
        --arg epochs_sha "$cohort_epochs_sha" \
        --argjson epoch_count "$cohort_epoch_count" '
        {
            schema_version: 1,
            kind: "archive-v2-pre-to-post-cohort-binding",
            request_sha256: $request_sha,
            cohort_builder_sha256: $builder_sha,
            rescan_reports_sha256: $snapshot_sha,
            cohort_json_sha256: $cohort_sha,
            epochs_sha256: $epochs_sha,
            epoch_count: $epoch_count
        }
    ' >"$binding_building" || die 'cannot create cohort binding'
    [ ! -e "$cohort_binding" ] && [ ! -L "$cohort_binding" ] \
        || die 'cohort binding appeared during publication'
    mv "$binding_building" "$cohort_binding" || die 'cannot publish cohort binding'
    chmod 600 "$cohort_binding" || die 'cannot set cohort binding mode 0600'
    log_status cohort_ready "built exact cohort epochs=$cohort_epoch_count"
fi

check_bound_file_hash "$converter_runner" "$converter_runner_sha" CONVERTER_RUNNER
check_bound_file_hash "$converter" "$converter_sha" CONVERTER
conversion_state=$state_root/conversion
if [ -e "$conversion_state/complete.json" ] || [ -L "$conversion_state/complete.json" ]; then
    validate_manual_complete \
        || die 'existing manual-runner completion record is invalid or changed'
    log_status conversion_ready "accepted existing runner completion sha256=$conversion_complete_sha"
else
    for old_runner_log in "$state_root"/converter-runner.*.building-* \
        "$state_root"/converter-runner.*.failed-*; do
        if [ -e "$old_runner_log" ] || [ -L "$old_runner_log" ]; then
            die "old converter-runner state needs review: $old_runner_log"
        fi
    done
    runner_stdout=$state_root/converter-runner.stdout.log.building-$$
    runner_stderr=$state_root/converter-runner.stderr.log.building-$$
    log_status converting "epochs=$cohort_epoch_count state=$conversion_state"
    if "$converter_runner" "$converter" "$archive_root" "$target_root" \
        "$conversion_state" "$cohort_dir/epochs.txt" "$cluster_id" "$run_id" \
        >"$runner_stdout" 2>"$runner_stderr"; then
        runner_status=0
    else
        runner_status=$?
    fi
    check_bound_file_hash "$converter_runner" "$converter_runner_sha" CONVERTER_RUNNER
    check_bound_file_hash "$converter" "$converter_sha" CONVERTER
    if [ "$runner_status" -ne 0 ]; then
        mv "$runner_stdout" "$runner_stdout.failed-$runner_status" 2>/dev/null || :
        mv "$runner_stderr" "$runner_stderr.failed-$runner_status" 2>/dev/null || :
        die "manual converter runner exited with status $runner_status"
    fi
    validate_manual_complete || die 'manual converter runner did not publish a valid completion record'
    [ ! -e "$state_root/converter-runner.stdout.log" ] \
        && [ ! -L "$state_root/converter-runner.stdout.log" ] \
        || die 'converter-runner stdout log appeared during the run'
    [ ! -e "$state_root/converter-runner.stderr.log" ] \
        && [ ! -L "$state_root/converter-runner.stderr.log" ] \
        || die 'converter-runner stderr log appeared during the run'
    mv "$runner_stdout" "$state_root/converter-runner.stdout.log" \
        || die 'cannot publish converter-runner stdout log'
    mv "$runner_stderr" "$state_root/converter-runner.stderr.log" \
        || die 'cannot publish converter-runner stderr log'
    log_status conversion_ready "runner complete sha256=$conversion_complete_sha"
fi

check_bound_file_hash "$cohort_builder" "$cohort_builder_sha" COHORT_BUILDER
check_bound_file_hash "$converter_runner" "$converter_runner_sha" CONVERTER_RUNNER
check_bound_file_hash "$converter" "$converter_sha" CONVERTER
cohort_binding_sha=$(sha256_file "$cohort_binding") || die 'cannot hash cohort binding'

handoff_complete=$state_root/complete.json
handoff_candidate=$handoff_complete.building-$$
[ ! -e "$handoff_candidate" ] && [ ! -L "$handoff_candidate" ] \
    || die "PID-specific completion candidate already exists: $handoff_candidate"
completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) || die 'cannot get handoff completion time'
jq -cn \
    --arg request_sha "$request_sha" \
    --arg builder_sha "$cohort_builder_sha" \
    --arg runner_sha "$converter_runner_sha" \
    --arg converter_sha "$converter_sha" \
    --arg snapshot_sha "$rescan_snapshot_sha" \
    --arg binding_sha "$cohort_binding_sha" \
    --arg conversion_sha "$conversion_complete_sha" \
    --argjson epoch_count "$cohort_epoch_count" \
    --arg completed_at "$completed_at" '
    {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-manual-handoff-complete",
        request_sha256: $request_sha,
        cohort_builder_sha256: $builder_sha,
        converter_runner_sha256: $runner_sha,
        converter_sha256: $converter_sha,
        rescan_reports_sha256: $snapshot_sha,
        cohort_binding_sha256: $binding_sha,
        conversion_complete_sha256: $conversion_sha,
        conversion_epoch_count: $epoch_count,
        completed_at_utc: $completed_at
    }
' >"$handoff_candidate" || die 'cannot create handoff completion record'
if [ -e "$handoff_complete" ] || [ -L "$handoff_complete" ]; then
    [ -f "$handoff_complete" ] && [ ! -L "$handoff_complete" ] \
        || die 'handoff completion path is not one real file'
    jq -e -s \
        --arg request_sha "$request_sha" \
        --arg builder_sha "$cohort_builder_sha" \
        --arg runner_sha "$converter_runner_sha" \
        --arg converter_sha "$converter_sha" \
        --arg snapshot_sha "$rescan_snapshot_sha" \
        --arg binding_sha "$cohort_binding_sha" \
        --arg conversion_sha "$conversion_complete_sha" \
        --argjson epoch_count "$cohort_epoch_count" '
        length == 1 and
        (.[0] as $c |
            $c.schema_version == 1 and
            $c.kind == "archive-v2-pre-to-post-manual-handoff-complete" and
            $c.request_sha256 == $request_sha and
            $c.cohort_builder_sha256 == $builder_sha and
            $c.converter_runner_sha256 == $runner_sha and
            $c.converter_sha256 == $converter_sha and
            $c.rescan_reports_sha256 == $snapshot_sha and
            $c.cohort_binding_sha256 == $binding_sha and
            $c.conversion_complete_sha256 == $conversion_sha and
            $c.conversion_epoch_count == $epoch_count and
            ($c.completed_at_utc | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")))
    ' "$handoff_complete" >/dev/null 2>&1 \
        || die 'existing handoff completion record does not match current bound state'
    rm -f "$handoff_candidate" || die 'cannot remove duplicate completion candidate'
else
    mv "$handoff_candidate" "$handoff_complete" \
        || die 'cannot atomically publish handoff completion record'
fi
chmod 600 "$handoff_complete" || die 'cannot set completion-record mode 0600'
log_status complete "canonical_post_epochs=$cohort_epoch_count record=$handoff_complete"
echo "manual handoff complete: $handoff_complete"
