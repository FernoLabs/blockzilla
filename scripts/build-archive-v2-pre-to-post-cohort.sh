#!/bin/sh
# Build the exact, audited list of legacy Pre epochs that still need a
# Compact Archive V2 Pre-to-Post conversion. This script never runs a
# converter and never changes an archive generation.

set -eu
umask 077

BLOCKS_FILE='archive-v2-blocks.zstd'
BLOCK_INDEX_FILE='archive-v2-blocks.index'
MANIFEST_FILE='archive-v2-generation.json'
RECEIPT_FILE='archive-v2-pre-to-post.receipt.json'
POST_MARKER='archive-v2-message-schema-post-unknown-fallbacks-v1.marker'
PRE_MARKER='archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker'
POST_MARKER_BYTES=77
POST_MARKER_SHA256='c870c4b0940b05b7bd18a134fba496c5c376f539ef7668f137112526d5c61edd'
SOURCE_PROFILE='pre-unknown-instruction-fallbacks-v1'
TARGET_PROFILE='post-unknown-instruction-fallbacks-v1'
HOT_INDEX_HEADER_BYTES=36
HOT_INDEX_ROW_BYTES=52

die() {
    echo "archive-v2 cohort builder: $*" >&2
    exit 1
}

usage() {
    echo "usage: $0 BASE_REPORT_ROOT RESCAN_REPORT_ROOT ARCHIVE_ROOT TARGET_ROOT OUTPUT_DIR FIRST_EPOCH LAST_EPOCH RESCAN_FIRST RESCAN_LAST EXCLUDE_EPOCH_LIST" >&2
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

require_canonical_root() {
    rcr_name=$1
    rcr_value=$2
    [ -d "$rcr_value" ] && [ ! -L "$rcr_value" ] \
        || die "$rcr_name is not one real directory: $rcr_value"
    rcr_canonical=$(canonical_directory "$rcr_value") \
        || die "cannot canonicalize $rcr_name: $rcr_value"
    [ "$rcr_canonical" = "$rcr_value" ] \
        || die "$rcr_name must already be canonical: $rcr_canonical"
}

sha256_file() (
    if [ "$sha256_program" = 'sha256sum' ]; then
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

validate_excluded_target() (
    vet_epoch=$1
    vet_source=$2
    vet_target=$3
    vet_source_report_sha=$4
    vet_manifest=$vet_target/$MANIFEST_FILE
    vet_receipt=$vet_target/$RECEIPT_FILE
    vet_marker=$vet_target/$POST_MARKER

    [ -d "$vet_target" ] && [ ! -L "$vet_target" ] || exit 1
    for vet_file in "$vet_manifest" "$vet_receipt" "$vet_marker"; do
        [ -f "$vet_file" ] && [ ! -L "$vet_file" ] || exit 1
    done
    [ ! -e "$vet_target/$PRE_MARKER" ] && [ ! -L "$vet_target/$PRE_MARKER" ] || exit 1

    vet_receipt_sha_before=$(sha256_file "$vet_receipt") || exit 1
    vet_receipt_bytes=$(wc -c <"$vet_receipt" | tr -d '[:space:]') || exit 1
    jq -e -s \
        --argjson epoch "$vet_epoch" \
        --arg source "$vet_source" \
        --arg target "$vet_target" \
        --arg source_profile "$SOURCE_PROFILE" \
        --arg target_profile "$TARGET_PROFILE" '
        def integer: type == "number" and . >= 0 and floor == .;
        def binding:
            type == "object" and
            (.bytes | integer) and
            (.sha256 | type == "string" and test("^[0-9a-f]{64}$"));
        length == 1 and
        (.[0] as $x |
            $x.schema_version == 1 and
            $x.kind == "archive-v2-pre-to-post-receipt" and
            $x.epoch == $epoch and
            $x.source == $source and
            $x.target == $target and
            ($x.cluster_id | type == "string" and length > 0) and
            ($x.generation_id | type == "string" and length > 0) and
            $x.source_profile == $source_profile and
            $x.target_profile == $target_profile and
            $x.source_profile_decision == "unique-full-generation-decode" and
            $x.codec == "wincode-leb128-current-block+independent-zstd-frames" and
            ($x.source_authority_id | type == "string" and length > 0) and
            (
                ($x.source_authority_kind == "linux-kernel-read-leases" and
                    $x.source_authority_scope ==
                        "all-reviewed-source-inodes-pinned-and-read-leased-on-one-local-ext4-device" and
                    $x.source_authority_filesystem == "linux-local-ext4" and
                    ($x.source_authority_device_id | integer) and
                    $x.source_provider_snapshot_required == false and
                    $x.source_linux_read_leases_required == true) or
                ($x.source_authority_kind == "provider-read-only-snapshot" and
                    $x.source_authority_scope == "provider-enforced-read-only-generation-path" and
                    $x.source_authority_filesystem == "provider-defined-read-only-filesystem" and
                    $x.source_authority_device_id == null and
                    $x.source_provider_snapshot_required == true and
                    $x.source_linux_read_leases_required == false)
            ) and
            ($x.source_audit.blocks | integer) and $x.source_audit.blocks > 0 and
            ($x.source_audit.typed_messages | integer) and $x.source_audit.typed_messages > 0 and
            ($x.source_audit.selected_only | integer) and $x.source_audit.selected_only > 0 and
            ($x.source_audit.both_semantically_equivalent | integer) and
            ($x.source_audit.both_semantically_divergent | integer) and
            ($x.source_audit.raw_transaction_fallbacks | integer) and
            ($x.source_audit.raw_metadata_fallbacks | integer) and
            $x.source_audit.both_semantically_divergent == 0 and
            $x.source_audit.raw_transaction_fallbacks == 0 and
            $x.source_audit.raw_metadata_fallbacks == 0 and
            ($x.source_audit.selected_only + $x.source_audit.both_semantically_equivalent
                == $x.source_audit.typed_messages) and
            $x.exact_message_length_preserved == true and
            $x.exact_message_delta_proved == true and
            $x.metadata_regions_copied_verbatim == true and
            $x.target_provider_immutability_required == true and
            ($x.source_files | type) == "object" and ($x.source_files | length) > 0 and
            ($x.target_files | type) == "object" and ($x.target_files | length) > 0 and
            all($x.source_files | to_entries[]; .value | binding) and
            all($x.target_files | to_entries[]; .value | binding)
        )
    ' "$vet_receipt" >/dev/null 2>&1 || exit 1

    vet_manifest_sha_before=$(sha256_file "$vet_manifest") || exit 1
    jq -e -s \
        --slurpfile receipt "$vet_receipt" \
        --argjson epoch "$vet_epoch" \
        --arg receipt_name "$RECEIPT_FILE" \
        --arg marker_name "$POST_MARKER" \
        --arg pre_marker "$PRE_MARKER" \
        --arg receipt_sha "$vet_receipt_sha_before" \
        --argjson receipt_bytes "$vet_receipt_bytes" \
        --arg marker_sha "$POST_MARKER_SHA256" \
        --argjson marker_bytes "$POST_MARKER_BYTES" '
        def integer: type == "number" and . >= 0 and floor == .;
        length == 1 and ($receipt | length) == 1 and
        (.[0] as $m | $receipt[0] as $x |
            $m.schema_version == 1 and
            $m.epoch == $epoch and
            $m.cluster_id == $x.cluster_id and
            $m.generation_id == $x.generation_id and
            $m.complete == true and
            ($m.slots_per_epoch | integer) and $m.slots_per_epoch > 0 and
            ($m.generation_digest | type == "string" and test("^[0-9a-f]{64}$")) and
            ($m.files | type) == "array" and
            ($m.files | length) >= 2 and
            all($m.files[];
                (.name | type == "string" and test("^[A-Za-z0-9._-]+$")) and
                .name != "archive-v2-generation.json" and
                (.size | integer) and
                (.sha256 | type == "string" and test("^[0-9a-f]{64}$"))) and
            (($m.files | map(.name) | unique | length) == ($m.files | length)) and
            any($m.files[];
                .name == $receipt_name and
                .size == $receipt_bytes and
                .sha256 == $receipt_sha) and
            any($m.files[];
                .name == $marker_name and
                .size == $marker_bytes and
                .sha256 == $marker_sha) and
            all($m.files[]; .name != $pre_marker) and
            all($x.target_files | to_entries[];
                . as $binding |
                any($m.files[];
                    .name == $binding.key and
                    .size == $binding.value.bytes and
                    .sha256 == $binding.value.sha256))
        )
    ' "$vet_manifest" >/dev/null 2>&1 || exit 1

    vet_entries=$(jq -r \
        '.files[] | [.name, (.size | tostring), .sha256] | @tsv' \
        "$vet_manifest") || exit 1
    vet_tab=$(printf '\t')
    while IFS="$vet_tab" read -r vet_name vet_expected_bytes vet_expected_sha; do
        vet_path=$vet_target/$vet_name
        [ -f "$vet_path" ] && [ ! -L "$vet_path" ] || exit 1
        vet_actual_bytes=$(wc -c <"$vet_path" | tr -d '[:space:]') || exit 1
        [ "$vet_actual_bytes" = "$vet_expected_bytes" ] || exit 1
        vet_actual_sha=$(sha256_file "$vet_path") || exit 1
        [ "$vet_actual_sha" = "$vet_expected_sha" ] || exit 1
    done <<EOF
$vet_entries
EOF

    vet_receipt_sha_after=$(sha256_file "$vet_receipt") || exit 1
    vet_manifest_sha_after=$(sha256_file "$vet_manifest") || exit 1
    [ "$vet_receipt_sha_after" = "$vet_receipt_sha_before" ] || exit 1
    [ "$vet_manifest_sha_after" = "$vet_manifest_sha_before" ] || exit 1
    vet_marker_sha=$(sha256_file "$vet_marker") || exit 1
    [ "$vet_marker_sha" = "$POST_MARKER_SHA256" ] || exit 1
    vet_marker_bytes=$(wc -c <"$vet_marker" | tr -d '[:space:]') || exit 1
    [ "$vet_marker_bytes" = "$POST_MARKER_BYTES" ] || exit 1

    jq -cn \
        --slurpfile receipt "$vet_receipt" \
        --slurpfile manifest "$vet_manifest" \
        --argjson epoch "$vet_epoch" \
        --arg target "$vet_target" \
        --arg source_report_sha "$vet_source_report_sha" \
        --arg receipt_sha "$vet_receipt_sha_before" \
        --arg manifest_sha "$vet_manifest_sha_before" \
        --arg marker_sha "$vet_marker_sha" '
        {
            epoch: $epoch,
            target: $target,
            source_report_sha256: $source_report_sha,
            receipt_sha256: $receipt_sha,
            manifest_sha256: $manifest_sha,
            generation_digest: $manifest[0].generation_digest,
            source_authority_kind: $receipt[0].source_authority_kind,
            source_authority_id: $receipt[0].source_authority_id,
            post_marker_sha256: $marker_sha
        }
    '
)

[ "$#" -eq 10 ] || usage

base_report_root=$1
rescan_report_root=$2
archive_root=$3
target_root=$4
output_dir=$5
first_epoch=$6
last_epoch=$7
rescan_first=$8
rescan_last=$9
exclude_epoch_list=${10}

for required in jq awk wc tr date sort dirname basename mkdir chmod mv rm rmdir; do
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
require_absolute_single_line OUTPUT_DIR "$output_dir"
require_absolute_single_line EXCLUDE_EPOCH_LIST "$exclude_epoch_list"
require_decimal FIRST_EPOCH "$first_epoch"
require_decimal LAST_EPOCH "$last_epoch"
require_decimal RESCAN_FIRST "$rescan_first"
require_decimal RESCAN_LAST "$rescan_last"

[ "$first_epoch" -le "$last_epoch" ] || die 'FIRST_EPOCH is greater than LAST_EPOCH'
[ "$rescan_first" -le "$rescan_last" ] || die 'RESCAN_FIRST is greater than RESCAN_LAST'
[ "$rescan_first" -ge "$first_epoch" ] && [ "$rescan_last" -le "$last_epoch" ] \
    || die 'the rescan range must be inside the selected epoch range'

require_canonical_root BASE_REPORT_ROOT "$base_report_root"
require_canonical_root RESCAN_REPORT_ROOT "$rescan_report_root"
require_canonical_root ARCHIVE_ROOT "$archive_root"
require_canonical_root TARGET_ROOT "$target_root"
[ -f "$exclude_epoch_list" ] && [ ! -L "$exclude_epoch_list" ] \
    || die "EXCLUDE_EPOCH_LIST is not one real file: $exclude_epoch_list"

output_parent=$(dirname "$output_dir")
output_base=$(basename "$output_dir")
[ -d "$output_parent" ] && [ ! -L "$output_parent" ] \
    || die "OUTPUT_DIR parent is not one real directory: $output_parent"
output_parent_canonical=$(canonical_directory "$output_parent") \
    || die "cannot canonicalize OUTPUT_DIR parent: $output_parent"
[ "$output_parent_canonical" = "$output_parent" ] \
    || die "OUTPUT_DIR parent must already be canonical: $output_parent_canonical"
case "$output_base" in
    ''|.|..) die 'OUTPUT_DIR must name one new child directory' ;;
esac
if [ "$output_parent" = / ]; then
    expected_output=/$output_base
    staging=/."$output_base".building-$$
else
    expected_output=$output_parent/$output_base
    staging=$output_parent/."$output_base".building-$$
fi
[ "$output_dir" = "$expected_output" ] \
    || die "OUTPUT_DIR must already be a normalized path: $expected_output"
[ ! -e "$output_dir" ] && [ ! -L "$output_dir" ] \
    || die "OUTPUT_DIR already exists: $output_dir"
[ ! -e "$staging" ] && [ ! -L "$staging" ] \
    || die "private staging path already exists: $staging"

mkdir "$staging" || die "cannot create private staging directory: $staging"
chmod 700 "$staging" || die "cannot set staging directory mode 0700: $staging"
staging_created=1
published=0
reports_tmp=$staging/.selected-reports.ndjson
pre_tmp=$staging/.legacy-pre-all.txt
exclude_sorted_tmp=$staging/.excluded.sorted.txt
excluded_targets_tmp=$staging/.excluded-targets.ndjson
epochs_file=$staging/epochs.txt
cohort_file=$staging/cohort.json
: >"$reports_tmp"
: >"$pre_tmp"
: >"$excluded_targets_tmp"

cleanup() {
    cleanup_status=$?
    trap - 0 1 2 3 15
    if [ "${staging_created:-0}" -eq 1 ] && [ "${published:-0}" -eq 0 ]; then
        rm -f "$reports_tmp" "$pre_tmp" "$exclude_sorted_tmp" \
            "$excluded_targets_tmp" "$epochs_file" "$cohort_file" 2>/dev/null || :
        rmdir "$staging" 2>/dev/null || :
    fi
    exit "$cleanup_status"
}
trap 'cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

if ! awk '
    /^(0|[1-9][0-9]*)$/ {
        if (length($0) > 14) {
            printf "excluded epoch is too large on line %d: %s\n", NR, $0 > "/dev/stderr"
            failed = 1
        } else if (seen[$0]++) {
            printf "duplicate excluded epoch on line %d: %s\n", NR, $0 > "/dev/stderr"
            failed = 1
        }
        next
    }
    {
        printf "invalid excluded epoch on line %d: %s\n", NR, $0 > "/dev/stderr"
        failed = 1
    }
    END { if (failed) exit 1 }
' "$exclude_epoch_list"
then
    die 'EXCLUDE_EPOCH_LIST is not an exact, unique decimal epoch list'
fi
sort -n "$exclude_epoch_list" >"$exclude_sorted_tmp" \
    || die 'cannot sort the excluded epoch list'
exclude_list_sha=$(sha256_file "$exclude_epoch_list") \
    || die 'cannot hash EXCLUDE_EPOCH_LIST'

epoch=$first_epoch
while [ "$epoch" -le "$last_epoch" ]; do
    if [ "$epoch" -ge "$rescan_first" ] && [ "$epoch" -le "$rescan_last" ]; then
        report_set=rescan
        report_root=$rescan_report_root
    else
        report_set=base
        report_root=$base_report_root
    fi
    report=$report_root/epoch-$epoch.json
    source_epoch=$archive_root/epoch-$epoch
    source_index=$source_epoch/$BLOCK_INDEX_FILE
    source_blocks=$source_epoch/$BLOCKS_FILE

    [ -d "$source_epoch" ] && [ ! -L "$source_epoch" ] \
        || die "epoch $epoch source directory is absent or is not real: $source_epoch"
    for source_file in "$source_index" "$source_blocks"; do
        [ -f "$source_file" ] && [ ! -L "$source_file" ] \
            || die "epoch $epoch source archive file is absent or is not real: $source_file"
    done
    [ -f "$report" ] && [ ! -L "$report" ] \
        || die "epoch $epoch has no unique selected $report_set scanner report: $report"

    report_sha_before=$(sha256_file "$report") \
        || die "epoch $epoch cannot hash selected scanner report"
    validate_scanner_report "$report" "$epoch" "$source_epoch" \
        || die "epoch $epoch selected $report_set scanner report is unsuccessful or inconsistent: $report"

    report_values=$(jq -r '[
        .classification,
        (.counts.blocks | tostring),
        (.counts.compressed_block_bytes | tostring),
        (.counts.uncompressed_block_bytes | tostring),
        (.counts.typed_messages | tostring)
    ] | @tsv' "$report") || die "epoch $epoch cannot read validated scanner counts"
    saved_ifs=$IFS
    IFS=$(printf '\t')
    read -r report_classification report_blocks report_compressed \
        report_uncompressed report_typed <<EOF
$report_values
EOF
    IFS=$saved_ifs

    source_index_bytes=$(wc -c <"$source_index" | tr -d '[:space:]') \
        || die "epoch $epoch cannot read source index size"
    expected_index_bytes=$((HOT_INDEX_HEADER_BYTES + report_blocks * HOT_INDEX_ROW_BYTES))
    [ "$source_index_bytes" -eq "$expected_index_bytes" ] \
        || die "epoch $epoch source index geometry does not match reported block count"
    source_blocks_bytes=$(wc -c <"$source_blocks" | tr -d '[:space:]') \
        || die "epoch $epoch cannot read source compressed data size"
    [ "$source_blocks_bytes" -eq "$report_compressed" ] \
        || die "epoch $epoch source data size does not match reported compressed bytes"

    jq -c \
        --arg report_set "$report_set" \
        --arg report_path "$report" \
        --arg report_sha "$report_sha_before" '
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
    ' "$report" >>"$reports_tmp" \
        || die "epoch $epoch cannot record selected scanner report"
    report_sha_after=$(sha256_file "$report") \
        || die "epoch $epoch cannot rehash selected scanner report"
    [ "$report_sha_after" = "$report_sha_before" ] \
        || die "epoch $epoch selected scanner report changed during cohort admission"

    if [ "$report_classification" = legacy-pre ]; then
        printf '%s\n' "$epoch" >>"$pre_tmp"
    fi
    epoch=$((epoch + 1))
done

if ! awk '
    FILENAME == ARGV[1] { pre[$1] = 1; next }
    FILENAME == ARGV[2] {
        if (!($1 in pre)) {
            printf "excluded epoch is not a selected legacy-pre epoch: %s\n", $1 > "/dev/stderr"
            failed = 1
        }
    }
    END { if (failed) exit 1 }
' "$pre_tmp" "$exclude_sorted_tmp"
then
    die 'the exclusion list is not an exact subset of selected legacy Pre epochs'
fi

awk '
    FILENAME == ARGV[1] { excluded[$1] = 1; next }
    FILENAME == ARGV[2] && !($1 in excluded) { print $1 }
' "$exclude_sorted_tmp" "$pre_tmp" >"$epochs_file" \
    || die 'cannot build the final legacy Pre epoch list'
epoch_list_sha=$(sha256_file "$epochs_file") || die 'cannot hash epochs.txt'

while IFS= read -r excluded_epoch || [ -n "$excluded_epoch" ]; do
    [ -n "$excluded_epoch" ] || continue
    excluded_source=$archive_root/epoch-$excluded_epoch
    excluded_target=$target_root/epoch-$excluded_epoch
    # Slurp the NDJSON first so `-e` evaluates one exact-match result. Some
    # jq builds report failure when a streaming `select` matches an early
    # document but later documents do not match.
    excluded_report_sha=$(jq -e -r -s \
        --argjson epoch "$excluded_epoch" \
        '[.[] | select(.epoch == $epoch) | .report_sha256] |
        select(length == 1 and
            (.[0] | type == "string" and test("^[0-9a-f]{64}$"))) |
        .[0]' "$reports_tmp") \
        || die "excluded epoch $excluded_epoch has no selected source report binding"
    validate_excluded_target "$excluded_epoch" "$excluded_source" \
        "$excluded_target" "$excluded_report_sha" >>"$excluded_targets_tmp" \
        || die "excluded epoch $excluded_epoch target is not a complete, hash-bound canonical Post generation"
done <"$exclude_sorted_tmp"

created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) || die 'cannot get the UTC creation time'
selected_epoch_count=$((last_epoch - first_epoch + 1))
rescan_epoch_count=$((rescan_last - rescan_first + 1))
excluded_epochs_json=$(jq -Rsc 'split("\n") | map(select(length > 0) | tonumber)' \
    "$exclude_sorted_tmp") || die 'cannot encode excluded epochs'
epochs_json=$(jq -Rsc 'split("\n") | map(select(length > 0) | tonumber)' \
    "$epochs_file") || die 'cannot encode conversion epochs'

jq -n \
    --slurpfile reports "$reports_tmp" \
    --slurpfile excluded_targets "$excluded_targets_tmp" \
    --arg created_at "$created_at" \
    --arg base_root "$base_report_root" \
    --arg rescan_root "$rescan_report_root" \
    --arg archive_root "$archive_root" \
    --arg target_root "$target_root" \
    --arg exclude_file "$exclude_epoch_list" \
    --arg exclude_sha "$exclude_list_sha" \
    --arg epoch_list_sha "$epoch_list_sha" \
    --argjson first "$first_epoch" \
    --argjson last "$last_epoch" \
    --argjson rescan_first "$rescan_first" \
    --argjson rescan_last "$rescan_last" \
    --argjson selected_count "$selected_epoch_count" \
    --argjson rescan_count "$rescan_epoch_count" \
    --argjson excluded_epochs "$excluded_epochs_json" \
    --argjson epochs "$epochs_json" '
    def class_count($name): [$reports[] | select(.classification == $name)] | length;
    def report_set_count($name): [$reports[] | select(.report_set == $name)] | length;
    def count_sum($name): [$reports[].counts[$name]] | add // 0;
    {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-cohort",
        created_at_utc: $created_at,
        archive_root: $archive_root,
        target_root: $target_root,
        source_scanner_roots: {
            base: $base_root,
            rescan: $rescan_root
        },
        selected_epoch_range: {
            first: $first,
            last: $last,
            count: $selected_count
        },
        rescan_epoch_range: {
            first: $rescan_first,
            last: $rescan_last,
            count: $rescan_count
        },
        selected_report_count: ($reports | length),
        selected_report_set_counts: {
            base: report_set_count("base"),
            rescan: report_set_count("rescan")
        },
        classification_totals: {
            canonical_post: class_count("canonical-post"),
            legacy_pre: class_count("legacy-pre"),
            canonical_equivalent: class_count("canonical-equivalent")
        },
        selected_counts: {
            blocks: count_sum("blocks"),
            compressed_block_bytes: count_sum("compressed_block_bytes"),
            uncompressed_block_bytes: count_sum("uncompressed_block_bytes"),
            typed_messages: count_sum("typed_messages"),
            owned_fallback_blocks: count_sum("owned_fallback_blocks"),
            raw_transaction_fallbacks: count_sum("raw_transaction_fallbacks"),
            post_only: count_sum("post_only"),
            pre_only: count_sum("pre_only"),
            both_equivalent: count_sum("both_equivalent"),
            both_divergent: count_sum("both_divergent"),
            invalid: count_sum("invalid")
        },
        exclude_epoch_list: $exclude_file,
        exclude_epoch_list_sha256: $exclude_sha,
        excluded_epochs: $excluded_epochs,
        excluded_targets: $excluded_targets,
        conversion_epoch_count: ($epochs | length),
        conversion_epochs: $epochs,
        epochs_file: "epochs.txt",
        epoch_list_sha256: $epoch_list_sha,
        source_geometry: {
            block_index_file: "archive-v2-blocks.index",
            blocks_file: "archive-v2-blocks.zstd",
            index_header_bytes: 36,
            index_row_bytes: 52
        },
        reports: $reports
    }
' >"$cohort_file" || die 'cannot write cohort.json'

jq -e \
    --arg epoch_list_sha "$epoch_list_sha" \
    --argjson selected_count "$selected_epoch_count" '
    .schema_version == 1 and
    .kind == "archive-v2-pre-to-post-cohort" and
    .selected_report_count == $selected_count and
    (.reports | length) == $selected_count and
    .epoch_list_sha256 == $epoch_list_sha and
    .conversion_epoch_count == (.conversion_epochs | length) and
    .excluded_epochs == ([.excluded_targets[].epoch]) and
    (.classification_totals.canonical_post +
        .classification_totals.legacy_pre +
        .classification_totals.canonical_equivalent == $selected_count)
' "$cohort_file" >/dev/null || die 'generated cohort.json is internally inconsistent'

rm -f "$reports_tmp" "$pre_tmp" "$exclude_sorted_tmp" "$excluded_targets_tmp" \
    || die 'cannot remove private cohort work files'
chmod 600 "$epochs_file" "$cohort_file" \
    || die 'cannot set cohort output files to mode 0600'
[ ! -e "$output_dir" ] && [ ! -L "$output_dir" ] \
    || die "OUTPUT_DIR appeared before publication: $output_dir"
mv "$staging" "$output_dir" || die "cannot atomically publish OUTPUT_DIR: $output_dir"
published=1
staging_created=0

echo "published cohort: $output_dir"
echo "conversion epochs: $(wc -l <"$output_dir/epochs.txt" | tr -d '[:space:]')"
