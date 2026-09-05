#!/bin/sh
# Rewrite the exact admitted LegacyPre cohort in place with two deterministic
# workers. The converter replaces only the compressed blocks/index pair, keeps
# the old pair and stale controls in a per-epoch backup, and publishes only a
# non-canonical candidate descriptor. Canonical audit and manifest work stay
# deferred to the later indexer-format conversion.

set -u
umask 077

fc_die() {
    echo "archive-v2 fast in-place Pre-to-Post: $*" >&2
    exit 1
}

fc_usage() {
    echo "usage: $0 HANDOFF_STATE_ROOT FENCE_EPOCH CONVERTER OPERATOR_QUIESCENCE_AUTHORITY_ID" >&2
    exit 2
}

fc_require_nonempty_single_line() {
    fc_name=$1
    fc_value=$2
    case "$fc_value" in
        ''|*'
'*) fc_die "$fc_name must be one non-empty line" ;;
    esac
}

fc_require_command() {
    command -v "$1" >/dev/null 2>&1 \
        || fc_die "required command is not available: $1"
}

fc_canonical_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

fc_canonical_file() (
    fc_parent=$(fc_canonical_directory "$(dirname "$1")") || exit 1
    printf '%s/%s\n' "$fc_parent" "$(basename "$1")"
)

fc_require_absolute_single_line() {
    fc_name=$1
    fc_value=$2
    case "$fc_value" in
        /*) ;;
        *) fc_die "$fc_name must be an absolute path: $fc_value" ;;
    esac
    case "$fc_value" in
        *'
'*) fc_die "$fc_name must not contain a line break" ;;
    esac
}

fc_validate_decimal() {
    fc_name=$1
    fc_value=$2
    case "$fc_value" in
        0) ;;
        ''|*[!0-9]*|0*) fc_die "$fc_name is not a canonical decimal integer" ;;
        *) ;;
    esac
    [ "$(printf '%s' "$fc_value" | wc -c | tr -d '[:space:]')" -le 14 ] \
        || fc_die "$fc_name is too large"
}

fc_sha256_file() (
    if [ "$sha256_program" = sha256sum ]; then
        fc_output=$(sha256sum "$1") || exit 1
    else
        fc_output=$(shasum -a 256 "$1") || exit 1
    fi
    printf '%s\n' "$fc_output" | awk '
        NR == 1 && length($1) == 64 && $1 !~ /[^0-9a-f]/ {
            print $1
            valid = 1
        }
        END { if (!valid) exit 1 }
    '
)

fc_json_field() {
    fc_expression=$1
    fc_file=$2
    jq -er "$fc_expression" "$fc_file" 2>/dev/null \
        || fc_die "invalid or absent $fc_expression in $fc_file"
}

fc_validate_epoch_list() {
    awk '
        !/^(0|[1-9][0-9]*)$/ { bad = 1 }
        seen[$0]++ { bad = 1 }
        previous != "" && ($0 + 0) <= (previous + 0) { bad = 1 }
        { previous = $0; count++ }
        END { if (bad || count == 0) exit 1; print count }
    ' "$1"
}

fc_copy_file_exclusive() {
    fc_source=$1
    fc_target=$2
    (
        set -C
        dd if="$fc_source" bs=1048576 >"$fc_target" 2>/dev/null
    )
}

fc_publish_derived_file() {
    fc_candidate=$1
    fc_final=$2
    [ -f "$fc_candidate" ] && [ ! -L "$fc_candidate" ] || return 1
    chmod 400 "$fc_candidate" || return 1
    if [ -e "$fc_final" ] || [ -L "$fc_final" ]; then
        [ -f "$fc_final" ] && [ ! -L "$fc_final" ] \
            && cmp -s "$fc_candidate" "$fc_final" || return 1
        rm "$fc_candidate" || return 1
    else
        ln -n "$fc_candidate" "$fc_final" || return 1
        rm "$fc_candidate" || return 1
    fi
}

fc_validate_scanner_report() {
    fc_report=$1
    fc_epoch=$2
    fc_archive=$3
    fc_expected_sha=$4
    [ -f "$fc_report" ] && [ ! -L "$fc_report" ] || return 1
    [ "$(fc_sha256_file "$fc_report")" = "$fc_expected_sha" ] || return 1
    jq -e -s --argjson epoch "$fc_epoch" --arg archive "$fc_archive" '
        def count:
            type == "number" and . >= 0 and floor == . and . <= 99999999999999;
        def location:
            type == "object" and (.slot | count) and
            (.transaction_index | count) and .transaction_index <= 4294967295;
        length == 1 and (.[0] as $r |
            $r.schema_version == 1 and
            $r.kind == "archive-v2-wire-profile-scan" and
            $r.epoch == $epoch and $r.archive == $archive and $r.error == null and
            ($r.workers | count) and $r.workers > 0 and
            ($r.elapsed_seconds | type == "number" and . >= 0) and
            ($r.completed_unix_seconds | count) and
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
            $r.classification == "legacy-pre" and
            $r.action == "convert-to-post" and
            $r.counts.owned_fallback_blocks == 0 and
            $r.counts.raw_transaction_fallbacks == 0 and
            $r.counts.post_only == 0 and $r.counts.pre_only > 0 and
            $r.counts.both_divergent == 0 and $r.counts.invalid == 0 and
            $r.counts.typed_messages ==
                ($r.counts.pre_only + $r.counts.both_equivalent) and
            ($r.first_evidence.pre_only | location) and
            $r.first_evidence.post_only == null and
            $r.first_evidence.both_divergent == null and
            $r.first_evidence.invalid == null)
    ' "$fc_report" >/dev/null 2>&1
}

fc_report_row() {
    fc_epoch=$1
    awk -F '\t' -v epoch="$fc_epoch" '
        $1 == epoch { count++; row = $0 }
        END { if (count != 1) exit 1; print row }
    ' "$report_table"
}

fc_parse_report_binding() {
    fc_epoch=$1
    fc_row=$(fc_report_row "$fc_epoch") || return 1
    fc_saved_ifs=$IFS
    IFS=$(printf '\t')
    read -r audit_epoch audit_source_report audit_sha audit_blocks audit_compressed \
        audit_uncompressed audit_typed audit_pre_only audit_equivalent <<EOF
$fc_row
EOF
    IFS=$fc_saved_ifs
    [ "$audit_epoch" = "$fc_epoch" ] || return 1
    for fc_count in "$audit_blocks" "$audit_compressed" "$audit_uncompressed" \
        "$audit_typed" "$audit_pre_only" "$audit_equivalent"; do
        case "$fc_count" in ''|*[!0-9]*) return 1 ;; esac
    done
    fc_require_absolute_single_line AUDIT_SOURCE_REPORT "$audit_source_report"
    audit_report=$audit_dir/epoch-$fc_epoch.json
    case "$audit_sha" in
        *[!0-9a-f]*|'') return 1 ;;
    esac
    [ "${#audit_sha}" -eq 64 ]
}

fc_read_report_binding() {
    fc_parse_report_binding "$1" || return 1
    [ -f "$audit_report" ] && [ ! -L "$audit_report" ] \
        && [ "$(fc_sha256_file "$audit_report")" = "$audit_sha" ]
}

fc_validate_pre_geometry() {
    fc_epoch=$1
    fc_source=$archive_root/epoch-$fc_epoch
    fc_index=$fc_source/archive-v2-blocks.index
    fc_blocks=$fc_source/archive-v2-blocks.zstd
    [ -f "$fc_index" ] && [ ! -L "$fc_index" ] \
        && [ -f "$fc_blocks" ] && [ ! -L "$fc_blocks" ] || return 1
    fc_index_bytes=$(wc -c <"$fc_index" | tr -d '[:space:]') || return 1
    fc_blocks_bytes=$(wc -c <"$fc_blocks" | tr -d '[:space:]') || return 1
    case "$fc_index_bytes:$fc_blocks_bytes" in *[!0-9:]*) return 1 ;; esac
    fc_expected_index=$((36 + audit_blocks * 52))
    [ "$fc_index_bytes" -eq "$fc_expected_index" ] \
        && [ "$fc_blocks_bytes" -eq "$audit_compressed" ]
}

fc_make_or_validate_claim() {
    fc_worker=$1
    fc_epoch=$2
    fc_claim=$claims_dir/epoch-$fc_epoch.json
    fc_candidate=$claims_dir/.epoch-$fc_epoch.worker-$fc_worker.building-$$
    [ ! -e "$fc_candidate" ] && [ ! -L "$fc_candidate" ] || return 1
    fc_read_report_binding "$fc_epoch" || return 1
    fc_source=$archive_root/epoch-$fc_epoch
    fc_staging=$archive_root/.epoch-$fc_epoch.pre-to-post.staging
    fc_backup=$archive_root/.epoch-$fc_epoch.pre-to-post.backup
    fc_generation=archive-v2-pre-to-post-fast-$run_id-epoch-$fc_epoch
    (
        set -C
        jq -cn --arg run_id "$run_id" --argjson epoch "$fc_epoch" \
            --argjson worker "$fc_worker" --argjson fence "$fence_epoch" \
            --arg source "$fc_source" --arg staging "$fc_staging" \
            --arg backup "$fc_backup" --arg generation "$fc_generation" \
            --arg converter "$converter" --arg version "$converter_version" \
            --arg converter_sha "$converter_sha" \
            --arg converter_source "$converter_source" \
            --arg converter_exec "$converter_exec" \
            --arg converter_authority "$converter_execution_authority" \
            --arg audit "$audit_report" --arg audit_source "$audit_source_report" \
            --arg audit_sha "$audit_sha" \
            --arg maintenance "$operator_quiescence_authority_id" \
            --argjson blocks "$audit_blocks" --argjson compressed "$audit_compressed" \
            --argjson uncompressed "$audit_uncompressed" --argjson typed "$audit_typed" \
            --argjson pre_only "$audit_pre_only" --argjson equivalent "$audit_equivalent" \
            --arg list "$all_epoch_list" '
            {
                schema_version: 1,
                kind: "archive-v2-pre-to-post-fast-in-place-claim",
                run_id: $run_id,
                epoch: $epoch,
                worker: $worker,
                fence_epoch: $fence,
                source: $source,
                staging: $staging,
                backup: $backup,
                prospective_generation_id: $generation,
                converter: {
                    source_path:$converter_source, pinned_path:$converter,
                    execution_path:$converter_exec,
                    execution_authority:$converter_authority,
                    version:$version, sha256:$converter_sha,
                    mode:"--fast-candidate"
                },
                source_audit: {
                    source_path:$audit_source,
                    pinned_path:$audit,
                    sha256:$audit_sha,
                    blocks:$blocks,
                    compressed_block_bytes:$compressed,
                    uncompressed_block_bytes:$uncompressed,
                    typed_messages:$typed,
                    pre_only:$pre_only,
                    both_equivalent:$equivalent
                },
                epoch_list: $list,
                assignment: "deterministic-two-worker-modulo",
                operator_maintenance_quiescence:{
                    authority_id:$maintenance,
                    required_for_entire_batch:true,
                    adversarial_proof:false
                },
                canonical_publication: false
            }
        ' >"$fc_candidate"
    ) || return 1
    fc_publish_derived_file "$fc_candidate" "$fc_claim" || return 1
    claim_path=$fc_claim
}

fc_validate_candidate_report() {
    fc_report=$1
    fc_epoch=$2
    # The worker is part of the immutable claim. It is intentionally not part
    # of the converter report contract.
    fc_worker=$3
    fc_read_report_binding "$fc_epoch" || return 1
    fc_source=$archive_root/epoch-$fc_epoch
    fc_staging=$archive_root/.epoch-$fc_epoch.pre-to-post.staging
    fc_backup=$archive_root/.epoch-$fc_epoch.pre-to-post.backup
    fc_descriptor=$fc_source/archive-v2-pre-to-post.candidate.v1.json
    fc_intent=$fc_backup/archive-v2-pre-to-post.switch-intent.v1.json
    fc_complete=$fc_backup/archive-v2-pre-to-post.switch-complete.v1.json
    fc_generation=archive-v2-pre-to-post-fast-$run_id-epoch-$fc_epoch
    fc_audit_bytes=$(wc -c <"$audit_report" | tr -d '[:space:]') || return 1
    [ -d "$fc_source" ] && [ ! -L "$fc_source" ] \
        && [ -d "$fc_backup" ] && [ ! -L "$fc_backup" ] \
        && [ ! -e "$fc_staging" ] && [ ! -L "$fc_staging" ] \
        && [ -f "$fc_descriptor" ] && [ ! -L "$fc_descriptor" ] \
        && [ -f "$fc_intent" ] && [ ! -L "$fc_intent" ] \
        && [ -f "$fc_complete" ] && [ ! -L "$fc_complete" ] \
        && [ -f "$fc_source/archive-v2-blocks.zstd" ] \
        && [ ! -L "$fc_source/archive-v2-blocks.zstd" ] \
        && [ -f "$fc_source/archive-v2-blocks.index" ] \
        && [ ! -L "$fc_source/archive-v2-blocks.index" ] \
        && [ -f "$fc_backup/archive-v2-blocks.zstd" ] \
        && [ ! -L "$fc_backup/archive-v2-blocks.zstd" ] \
        && [ -f "$fc_backup/archive-v2-blocks.index" ] \
        && [ ! -L "$fc_backup/archive-v2-blocks.index" ] || return 1
    fc_descriptor_bytes=$(wc -c <"$fc_descriptor" | tr -d '[:space:]') || return 1
    fc_descriptor_sha=$(fc_sha256_file "$fc_descriptor") || return 1
    fc_counts=$(jq -c '.counts' "$audit_report" 2>/dev/null) || return 1

    # The descriptor is the stable data contract for both a normal return and
    # a journal-recovery return. Derive the moved and retained sets from its
    # original inventory, so an omitted stale object cannot disappear from a
    # converter report without detection.
    jq -e --argjson epoch "$fc_epoch" --arg cluster "$cluster_id" \
        --arg generation "$fc_generation" --arg source "$fc_source" \
        --arg backup "$fc_backup" --arg audit "$audit_report" \
        --arg switch_lock "$archive_root/.archive-v2-pre-to-post.switch.lock" \
        --arg audit_sha "$audit_sha" --argjson audit_bytes "$fc_audit_bytes" \
        --argjson counts "$fc_counts" '
        def count: type == "number" and . >= 0 and floor == .;
        def binding:
            type == "object" and (.bytes | count) and
            (.sha256 | type == "string" and test("^[0-9a-f]{64}$"));
        def safe_names:
            type == "array" and
            all(.[]; type == "string" and length > 0 and
                (contains("/") | not) and (test("[[:space:]]") | not)) and
            length == (unique | length);
        . as $d |
        ($d.source_inventory | to_entries) as $inventory |
        ([$inventory[] |
            select(.key == "archive-v2-get-block.index" or
                .key == "archive-v2-block-access.index.pre-votehash-20260523T205501+0200" or
                .key == "archive-v2-get-block.index.pre-votehash-20260523T205501+0200" or
                .key == "block-time-gaps.bin" or
                .value.disposition == "omit-control" or
                .value.disposition == "omit-obsolete-block") | .key] | sort) as $moved |
        ([$inventory[] |
            select(.value.disposition == "copy-durable" and
                .key != "block-time-gaps.bin") | .key] | sort) as $durable |
        ([$inventory[] |
            select((.key == "archive-v2-block-access.wincode" or
                    .key == "archive-v2-block-access.index") and
                .value.disposition == "omit-edge") | .key] | sort) as $edge |
        $d.schema_version == 1 and
        $d.kind == "archive-v2-pre-to-post-candidate" and
        $d.state == "unfinalized" and $d.canonical == false and
        $d.epoch == $epoch and $d.cluster_id == $cluster and
        $d.prospective_generation_id == $generation and
        $d.source == $source and $d.candidate == $source and $d.backup == $backup and
        $d.source_profile_evidence == "external-whole-generation-scan-report" and
        $d.source_audit_report.path == $audit and
        $d.source_audit_report.bytes == $audit_bytes and
        $d.source_audit_report.sha256 == $audit_sha and
        ($d.source_audit_report.completed_unix_seconds | count) and
        ($d.source_audit_report.workers | count) and
        $d.source_audit_report.workers > 0 and
        $d.source_audit_report.counts == $counts and
        $d.expected_wire_profile_after_rewrite ==
            "post-unknown-instruction-fallbacks-v1" and
        $d.source_full_audit_performed_in_this_run == false and
        $d.source_audit_report_reused == true and
        $d.single_decode_rewrite_pass == true and
        $d.outer_block_bytes_preserved_verbatim_except_messages == true and
        $d.sidecars_copied == false and $d.sidecars_rewritten == false and
        $d.pair_swap_requires_external_reader_quiescence == true and
        $d.archive_root_switch_lock == $switch_lock and
        ($d.source_inventory | type == "object") and
        all($inventory[];
            (.key | type == "string" and length > 0 and
                (contains("/") | not) and (test("[[:space:]]") | not)) and
            (.value.bytes | count) and
            (.value.disposition == "rewrite" or
             .value.disposition == "copy-durable" or
             .value.disposition == "omit-edge" or
             .value.disposition == "omit-control" or
             .value.disposition == "omit-obsolete-block")) and
        ($d.source_files | type == "object" and
            (keys | sort) == ["archive-v2-blocks.index","archive-v2-blocks.zstd"] and
            all(.[]; binding)) and
        ($d.candidate_rewrite_files | type == "object" and
            (keys | sort) == ["archive-v2-blocks.index","archive-v2-blocks.zstd"] and
            all(.[]; binding)) and
        ($d.retained_durable_files | safe_names) and
        ($d.retained_edge_files | safe_names) and
        ($d.moved_to_backup | safe_names) and
        (($d.retained_durable_files | sort) == $durable) and
        (($d.retained_edge_files | sort) == $edge) and
        (($d.moved_to_backup | sort) == $moved) and
        ($d.rewrite | type == "object") and
        ($d.rewrite.blocks | count) and $d.rewrite.blocks == $counts.blocks and
        ($d.rewrite.typed_messages | count) and
        $d.rewrite.typed_messages == $counts.typed_messages and
        ($d.rewrite.message_input_bytes | count) and
        ($d.rewrite.message_output_bytes | count) and
        $d.rewrite.message_input_bytes == $d.rewrite.message_output_bytes and
        $d.exact_message_length_preserved == true and
        $d.exact_message_delta_proved == true and
        $d.metadata_regions_copied_verbatim == true and
        $d.retained_edge_files_authoritative == false and
        $d.retained_edge_validation_deferred == true and
        $d.canonical_publication_deferred == true and
        $d.target_post_audit_performed == false and
        $d.canonical_manifest_written == false and
        $d.canonical_profile_marker_written == false and
        $d.canonical_migration_receipt_written == false and
        $d.source_provider_snapshot_required == false and
        $d.source_linux_read_leases_required == true
    ' "$fc_descriptor" >/dev/null 2>&1 || return 1

    # Accept the full first-run report or the compact idempotent recovery
    # report. Both must bind the same small descriptor and scanner report.
    jq -e --argjson epoch "$fc_epoch" --arg cluster "$cluster_id" \
        --arg generation "$fc_generation" --arg source "$fc_source" \
        --arg backup "$fc_backup" --arg descriptor "$fc_descriptor" \
        --arg switch_lock "$archive_root/.archive-v2-pre-to-post.switch.lock" \
        --arg descriptor_sha "$fc_descriptor_sha" \
        --argjson descriptor_bytes "$fc_descriptor_bytes" \
        --arg audit "$audit_report" --arg audit_sha "$audit_sha" \
        --argjson audit_bytes "$fc_audit_bytes" --argjson counts "$fc_counts" \
        --slurpfile descriptor_doc "$fc_descriptor" '
        def count: type == "number" and . >= 0 and floor == .;
        . as $r | ($descriptor_doc[0]) as $d |
        $r.schema_version == 1 and $r.state == "unfinalized" and
        $r.canonical == false and $r.epoch == $epoch and
        $r.cluster_id == $cluster and
        $r.prospective_generation_id == $generation and
        $r.candidate == $source and $r.backup == $backup and
        $r.candidate_descriptor == $descriptor and
        $r.candidate_descriptor_bytes == $descriptor_bytes and
        $r.candidate_descriptor_sha256 == $descriptor_sha and
        $r.source_audit_report == $audit and
        $r.source_audit_report_bytes == $audit_bytes and
        $r.source_audit_report_sha256 == $audit_sha and
        if $r.kind == "archive-v2-pre-to-post-candidate-report" then
            $r.source == $source and
            $r.source_profile_evidence == "external-whole-generation-scan-report" and
            $r.source_scan_counts == $counts and
            $r.expected_wire_profile_after_rewrite ==
                "post-unknown-instruction-fallbacks-v1" and
            $r.source_full_audit_performed_in_this_run == false and
            $r.source_audit_report_reused == true and
            $r.single_decode_rewrite_pass == true and
            $r.sidecars_copied == false and $r.sidecars_rewritten == false and
            $r.pair_swap_requires_external_reader_quiescence == true and
            $r.archive_root_switch_lock == $switch_lock and
            $r.rewrite == $d.rewrite and
            $r.retained_durable_files == $d.retained_durable_files and
            $r.retained_edge_files == $d.retained_edge_files and
            $r.moved_to_backup == $d.moved_to_backup and
            $r.canonical_publication_deferred == true and
            $r.target_post_audit_performed == false and
            $r.canonical_manifest_written == false and
            $r.canonical_profile_marker_written == false and
            $r.canonical_migration_receipt_written == false and
            $r.rewritten_files_read_only == true and
            ($r.elapsed_seconds | type == "number" and . >= 0)
        elif $r.kind == "archive-v2-pre-to-post-candidate-recovery-report" then
            $r.recovered_switch == true and ($r.already_complete | type == "boolean")
        else false end
    ' "$fc_report" >/dev/null 2>&1 || return 1

    # Bind the durable swap records. Only their small JSON files and the audit
    # report are hashed. The archive payloads are checked by path and byte size.
    jq -e --argjson epoch "$fc_epoch" --arg cluster "$cluster_id" \
        --arg generation "$fc_generation" --arg source "$fc_source" \
        --arg staging "$fc_staging" --arg backup "$fc_backup" \
        --arg descriptor_sha "$fc_descriptor_sha" \
        --argjson descriptor_bytes "$fc_descriptor_bytes" \
        --arg audit "$audit_report" --arg audit_sha "$audit_sha" \
        --argjson audit_bytes "$fc_audit_bytes" --slurpfile descriptor "$fc_descriptor" '
        def count: type == "number" and . >= 0 and floor == .;
        def identity:
            type == "object" and (.bytes | count) and
            (.device_id | count) and (.inode | count);
        def binding:
            type == "object" and (.bytes | count) and
            (.sha256 | type == "string" and test("^[0-9a-f]{64}$"));
        . as $i | ($descriptor[0]) as $d |
        $i.schema_version == 1 and
        $i.kind == "archive-v2-pre-to-post-pair-swap-intent" and
        $i.epoch == $epoch and $i.cluster_id == $cluster and
        $i.prospective_generation_id == $generation and
        $i.candidate == $source and $i.staging == $staging and $i.backup == $backup and
        ($i.source_blocks | identity) and ($i.source_index | identity) and
        ($i.candidate_blocks | identity) and ($i.candidate_index | identity) and
        ($i.source_blocks_binding | binding) and
        ($i.source_index_binding | binding) and
        ($i.candidate_blocks_binding | binding) and
        ($i.candidate_index_binding | binding) and
        $i.source_blocks_binding == $d.source_files["archive-v2-blocks.zstd"] and
        $i.source_index_binding == $d.source_files["archive-v2-blocks.index"] and
        $i.candidate_blocks_binding ==
            $d.candidate_rewrite_files["archive-v2-blocks.zstd"] and
        $i.candidate_index_binding ==
            $d.candidate_rewrite_files["archive-v2-blocks.index"] and
        $i.source_blocks.bytes == $d.source_files["archive-v2-blocks.zstd"].bytes and
        $i.source_index.bytes == $d.source_files["archive-v2-blocks.index"].bytes and
        $i.candidate_blocks.bytes ==
            $d.candidate_rewrite_files["archive-v2-blocks.zstd"].bytes and
        $i.candidate_index.bytes ==
            $d.candidate_rewrite_files["archive-v2-blocks.index"].bytes and
        $i.candidate_descriptor.bytes == $descriptor_bytes and
        $i.candidate_descriptor.sha256 == $descriptor_sha and
        $i.moved_to_backup == $d.moved_to_backup and
        $i.retained_edge_files == $d.retained_edge_files and
        $i.source_audit_report_path == $audit and
        $i.source_audit_report.bytes == $audit_bytes and
        $i.source_audit_report.sha256 == $audit_sha
    ' "$fc_intent" >/dev/null 2>&1 || return 1
    fc_intent_sha=$(fc_sha256_file "$fc_intent") || return 1
    jq -e --argjson epoch "$fc_epoch" --arg source "$fc_source" \
        --arg backup "$fc_backup" --arg descriptor_sha "$fc_descriptor_sha" \
        --arg audit_sha "$audit_sha" --arg intent_sha "$fc_intent_sha" \
        --slurpfile intent "$fc_intent" '
        ($intent[0]) as $i |
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-pair-swap-complete" and
        .epoch == $epoch and .canonical == false and
        .candidate == $source and .backup == $backup and
        .intent_sha256 == $intent_sha and
        .candidate_descriptor_sha256 == $descriptor_sha and
        .source_audit_report_sha256 == $audit_sha and
        .source_blocks_sha256 == $i.source_blocks_binding.sha256 and
        .source_index_sha256 == $i.source_index_binding.sha256 and
        .candidate_blocks_sha256 == $i.candidate_blocks_binding.sha256 and
        .candidate_index_sha256 == $i.candidate_index_binding.sha256
    ' "$fc_complete" >/dev/null 2>&1 || return 1

    fc_live_blocks_bytes=$(wc -c <"$fc_source/archive-v2-blocks.zstd" | tr -d '[:space:]') \
        || return 1
    fc_live_index_bytes=$(wc -c <"$fc_source/archive-v2-blocks.index" | tr -d '[:space:]') \
        || return 1
    fc_backup_blocks_bytes=$(wc -c <"$fc_backup/archive-v2-blocks.zstd" | tr -d '[:space:]') \
        || return 1
    fc_backup_index_bytes=$(wc -c <"$fc_backup/archive-v2-blocks.index" | tr -d '[:space:]') \
        || return 1
    jq -e --argjson live_blocks "$fc_live_blocks_bytes" \
        --argjson live_index "$fc_live_index_bytes" \
        --argjson backup_blocks "$fc_backup_blocks_bytes" \
        --argjson backup_index "$fc_backup_index_bytes" '
        .candidate_blocks.bytes == $live_blocks and
        .candidate_index.bytes == $live_index and
        .source_blocks.bytes == $backup_blocks and
        .source_index.bytes == $backup_index
    ' "$fc_intent" >/dev/null 2>&1 || return 1

    # Every reported stale object must be disabled in the retained backup.
    # Stable edge and durable objects must remain in the live epoch. This is
    # path/inventory validation only; no archive content is hashed here.
    fc_moved=$(jq -r '.moved_to_backup[]' "$fc_descriptor" 2>/dev/null) || return 1
    if [ -n "$fc_moved" ]; then
        [ -d "$fc_backup/disabled" ] && [ ! -L "$fc_backup/disabled" ] || return 1
    fi
    for fc_name in $fc_moved; do
        case "$fc_name" in
            ''|*/*|.*/*|*' '*|*'	'*|*'
'*) return 1 ;;
        esac
        [ ! -e "$fc_source/$fc_name" ] && [ ! -L "$fc_source/$fc_name" ] \
            && [ -e "$fc_backup/disabled/$fc_name" ] \
            && [ ! -L "$fc_backup/disabled/$fc_name" ] || return 1
    done
    fc_retained=$(jq -r '(.retained_durable_files + .retained_edge_files)[]' \
        "$fc_descriptor" 2>/dev/null) || return 1
    for fc_name in $fc_retained; do
        case "$fc_name" in ''|*/*|*' '*|*'	'*|*'
'*) return 1 ;; esac
        [ -f "$fc_source/$fc_name" ] && [ ! -L "$fc_source/$fc_name" ] || return 1
    done
    for fc_name in \
        block-time-gaps.bin .block-time-gaps.bin.lock \
        .hivezilla-pipeline-owned.v1.json \
        .complete-hot-v2-no-access-delete-car \
        .complete-hot-v2-shredding-sidecar-v2 \
        archive-v2-generation.json archive-v2-registry-reprocess.receipt.json \
        archive-v2-pre-to-post.receipt.json \
        .archive-v2-manifest-publish.lock \
        archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker \
        archive-v2-message-schema-post-unknown-fallbacks-v1.marker \
        archive-v2-get-block.index \
        archive-v2-block-access.index.pre-votehash-20260523T205501+0200 \
        archive-v2-get-block.index.pre-votehash-20260523T205501+0200 \
        archive-v2-blocks.wincode archive-v2-blocks.wincode.zst; do
        [ ! -e "$fc_source/$fc_name" ] && [ ! -L "$fc_source/$fc_name" ] || return 1
    done
}

fc_build_result() {
    fc_path=$1
    fc_worker=$2
    fc_epoch=$3
    fc_claim=$4
    fc_report=$5
    fc_log=$6
    fc_read_report_binding "$fc_epoch" || return 1
    (
        set -C
        jq -cn --slurpfile report "$fc_report" \
            --slurpfile descriptor "$archive_root/epoch-$fc_epoch/archive-v2-pre-to-post.candidate.v1.json" \
            --arg run_id "$run_id" --argjson epoch "$fc_epoch" \
            --argjson worker "$fc_worker" --arg source "$archive_root/epoch-$fc_epoch" \
            --arg backup "$archive_root/.epoch-$fc_epoch.pre-to-post.backup" \
            --arg generation "archive-v2-pre-to-post-fast-$run_id-epoch-$fc_epoch" \
            --arg converter "$converter" --arg version "$converter_version" \
            --arg converter_sha "$converter_sha" \
            --arg converter_source "$converter_source" \
            --arg converter_exec "$converter_exec" \
            --arg converter_authority "$converter_execution_authority" \
            --arg claim "$fc_claim" --arg converter_report "$fc_report" \
            --arg converter_log "$fc_log" --arg audit "$audit_report" \
            --arg audit_source "$audit_source_report" \
            --arg audit_sha "$audit_sha" \
            --arg maintenance "$operator_quiescence_authority_id" '
            ($report[0]) as $r | ($descriptor[0]) as $d |
            {
                schema_version: 1,
                kind: "archive-v2-pre-to-post-fast-in-place-result",
                run_id: $run_id,
                epoch: $epoch,
                worker: $worker,
                source: $source,
                backup: $backup,
                prospective_generation_id: $generation,
                converter:{
                    source_path:$converter_source,pinned_path:$converter,
                    execution_path:$converter_exec,
                    execution_authority:$converter_authority,
                    version:$version,sha256:$converter_sha
                },
                claim: $claim,
                converter_report: $converter_report,
                converter_log: $converter_log,
                converter_report_kind: $r.kind,
                recovered_switch: ($r.recovered_switch // false),
                source_audit: {source_path:$audit_source,pinned_path:$audit,sha256:$audit_sha,
                    counts:$d.source_audit_report.counts},
                rewrite: $d.rewrite,
                retained_durable_files: $d.retained_durable_files,
                retained_edge_files: $d.retained_edge_files,
                moved_to_backup: $d.moved_to_backup,
                candidate_descriptor: $r.candidate_descriptor,
                operator_maintenance_quiescence:{
                    authority_id:$maintenance,
                    required_for_entire_batch:true,
                    adversarial_proof:false
                },
                target_post_audit_deferred: true,
                canonical_manifest_deferred: true,
                canonical_publication: false
            }
        ' >"$fc_path"
    )
}

fc_publish_or_validate_result() {
    fc_worker=$1
    fc_epoch=$2
    fc_claim=$3
    fc_report=$4
    fc_log=$5
    fc_result=$results_dir/epoch-$fc_epoch.json
    fc_candidate=$results_dir/.epoch-$fc_epoch.worker-$fc_worker.building-$$
    [ -f "$fc_log" ] && [ ! -L "$fc_log" ] || return 1
    [ ! -e "$fc_candidate" ] && [ ! -L "$fc_candidate" ] || return 1
    fc_build_result "$fc_candidate" "$fc_worker" "$fc_epoch" "$fc_claim" \
        "$fc_report" "$fc_log" || return 1
    fc_publish_derived_file "$fc_candidate" "$fc_result"
}

fc_validate_completed_epoch() {
    fc_worker=$1
    fc_epoch=$2
    fc_claim=$claims_dir/epoch-$fc_epoch.json
    fc_report=$reports_dir/epoch-$fc_epoch.json
    fc_log=$logs_dir/epoch-$fc_epoch.log
    fc_result=$results_dir/epoch-$fc_epoch.json
    [ -f "$fc_claim" ] && [ ! -L "$fc_claim" ] \
        && [ -f "$fc_result" ] && [ ! -L "$fc_result" ] || return 1
    fc_make_or_validate_claim "$fc_worker" "$fc_epoch" || return 1
    fc_validate_candidate_report "$fc_report" "$fc_epoch" "$fc_worker" || return 1
    fc_publish_or_validate_result "$fc_worker" "$fc_epoch" "$fc_claim" \
        "$fc_report" "$fc_log"
}

fc_new_attempt_dir() {
    fc_epoch=$1
    fc_root=$attempts_dir/epoch-$fc_epoch
    if [ -e "$fc_root" ] || [ -L "$fc_root" ]; then
        [ -d "$fc_root" ] && [ ! -L "$fc_root" ] || return 1
    else
        mkdir "$fc_root" || return 1
        chmod 700 "$fc_root" || return 1
    fi
    fc_sequence=1
    while [ "$fc_sequence" -le 999999 ]; do
        fc_path=$fc_root/attempt-$fc_sequence
        if mkdir "$fc_path" 2>/dev/null; then
            chmod 700 "$fc_path" || return 1
            attempt_dir=$fc_path
            return 0
        fi
        [ -d "$fc_path" ] && [ ! -L "$fc_path" ] || return 1
        fc_sequence=$((fc_sequence + 1))
    done
    return 1
}

fc_find_recovery_attempt() {
    fc_epoch=$1
    fc_worker=$2
    recovery_report=
    recovery_log=
    fc_count=0
    for fc_attempt in "$attempts_dir/epoch-$fc_epoch"/attempt-*; do
        if [ ! -e "$fc_attempt" ] && [ ! -L "$fc_attempt" ]; then
            continue
        fi
        [ -d "$fc_attempt" ] && [ ! -L "$fc_attempt" ] || return 1
        fc_report=$fc_attempt/report.json
        fc_log=$fc_attempt/converter.log
        [ -f "$fc_log" ] && [ ! -L "$fc_log" ] || continue
        if fc_validate_candidate_report "$fc_report" "$fc_epoch" "$fc_worker"; then
            fc_count=$((fc_count + 1))
            recovery_report=$fc_report
            recovery_log=$fc_log
        fi
    done
    [ "$fc_count" -eq 1 ]
}

fc_publish_attempt_state() {
    fc_report_source=$1
    fc_log_source=$2
    fc_report=$3
    fc_log=$4
    chmod 400 "$fc_report_source" "$fc_log_source" || return 1
    for fc_pair in "$fc_report_source:$fc_report" "$fc_log_source:$fc_log"; do
        fc_source=${fc_pair%%:*}
        fc_target=${fc_pair#*:}
        if [ -e "$fc_target" ] || [ -L "$fc_target" ]; then
            [ -f "$fc_target" ] && [ ! -L "$fc_target" ] \
                && cmp -s "$fc_source" "$fc_target" || return 1
        else
            ln -n "$fc_source" "$fc_target" || return 1
        fi
    done
}

fc_round_required_bytes() {
    fc_required=0
    for fc_pair in "$@"; do
        [ -n "$fc_pair" ] || continue
        fc_worker=${fc_pair%%:*}
        fc_epoch=${fc_pair#*:}
        if ! fc_validate_completed_epoch "$fc_worker" "$fc_epoch"; then
            fc_read_report_binding "$fc_epoch" || return 1
            fc_index_bytes=$((36 + audit_blocks * 52))
            fc_margin=$((audit_compressed / 100))
            [ "$fc_margin" -ge 268435456 ] || fc_margin=268435456
            [ "$fc_margin" -le 4294967296 ] || fc_margin=4294967296
            fc_required=$((fc_required + audit_compressed + fc_index_bytes + fc_margin))
        fi
    done
    printf '%s\n' "$fc_required"
}

fc_admit_round_space() {
    fc_required=$(fc_round_required_bytes "$@") \
        || fc_die 'cannot compute combined two-worker free-space need'
    fc_available_kib=$(df -Pk "$archive_root" | awk '
        NR == 2 { print $4; found = 1 }
        END { if (!found) exit 1 }
    ') || fc_die 'cannot read archive filesystem free space'
    case "$fc_available_kib" in ''|*[!0-9]*) fc_die 'invalid free-space value' ;; esac
    fc_available=$((fc_available_kib * 1024))
    [ "$fc_available" -ge "$fc_required" ] \
        || fc_die "archive filesystem has $fc_available free bytes; this round needs $fc_required bytes"
    echo "fast in-place launch round: available=$fc_available required=$fc_required"
}

fc_epoch_job() {
    job_worker=$1
    job_epoch=$2
    trap - 0 1 2 3 15
    worker_converter_pid=
    fc_worker_stop() {
        fc_status=$1
        trap - 0 1 2 3 15
        if [ -n "$worker_converter_pid" ]; then
            kill -TERM "$worker_converter_pid" 2>/dev/null || :
            wait "$worker_converter_pid" 2>/dev/null || :
            worker_converter_pid=
        fi
        exit "$fc_status"
    }
    trap 'fc_worker_stop 129' 1
    trap 'fc_worker_stop 130' 2
    trap 'fc_worker_stop 131' 3
    trap 'fc_worker_stop 143' 15

    fc_read_report_binding "$job_epoch" \
        || fc_die "worker $job_worker has no audit binding for epoch $job_epoch"
    job_source=$archive_root/epoch-$job_epoch
    job_staging=$archive_root/.epoch-$job_epoch.pre-to-post.staging
    job_backup=$archive_root/.epoch-$job_epoch.pre-to-post.backup
    job_descriptor=$job_source/archive-v2-pre-to-post.candidate.v1.json
    job_generation=archive-v2-pre-to-post-fast-$run_id-epoch-$job_epoch
    job_lease=archive-v2-pre-to-post-fast-$run_id-epoch-$job_epoch-source-leases
    job_report=$reports_dir/epoch-$job_epoch.json
    job_log=$logs_dir/epoch-$job_epoch.log
    job_result=$results_dir/epoch-$job_epoch.json

    [ -d "$job_source" ] && [ ! -L "$job_source" ] \
        || fc_die "worker $job_worker source is absent for epoch $job_epoch"
    for job_recovery_dir in "$job_staging" "$job_backup"; do
        if [ -e "$job_recovery_dir" ] || [ -L "$job_recovery_dir" ]; then
            [ -d "$job_recovery_dir" ] && [ ! -L "$job_recovery_dir" ] \
                || fc_die "worker $job_worker recovery path is not one real directory: $job_recovery_dir"
        fi
    done
    if [ -e "$job_descriptor" ] || [ -L "$job_descriptor" ]; then
        [ -f "$job_descriptor" ] && [ ! -L "$job_descriptor" ] \
            || fc_die "worker $job_worker candidate descriptor is not one real file"
    fi
    fc_make_or_validate_claim "$job_worker" "$job_epoch" \
        || fc_die "worker $job_worker claim differs for epoch $job_epoch"
    job_claim=$claim_path
    if [ -e "$job_result" ] || [ -L "$job_result" ]; then
        fc_validate_completed_epoch "$job_worker" "$job_epoch" \
            || fc_die "worker $job_worker completed state changed for epoch $job_epoch"
        echo "worker $job_worker epoch $job_epoch: accepted existing candidate"
        exit 0
    fi

    job_report_source=
    job_log_source=
    if fc_find_recovery_attempt "$job_epoch" "$job_worker"; then
        job_report_source=$recovery_report
        job_log_source=$recovery_log
    else
        # A fresh epoch must still match its admitted Pre scan. Any staging or
        # backup means the converter owns recovery through its durable journal.
        if [ ! -e "$job_staging" ] && [ ! -L "$job_staging" ] \
            && [ ! -e "$job_backup" ] && [ ! -L "$job_backup" ] \
            && [ ! -e "$job_descriptor" ] && [ ! -L "$job_descriptor" ]; then
            fc_validate_scanner_report "$audit_report" "$job_epoch" "$job_source" "$audit_sha" \
                && fc_validate_pre_geometry "$job_epoch" \
                || fc_die "worker $job_worker Pre admission changed for epoch $job_epoch"
        fi
        fc_new_attempt_dir "$job_epoch" \
            || fc_die "worker $job_worker cannot create attempt for epoch $job_epoch"
        job_report_source=$attempt_dir/report.json
        job_log_source=$attempt_dir/converter.log
        {
            echo "epoch=$job_epoch"
            echo "worker=$job_worker"
            echo "source=$job_source"
            echo "staging=$job_staging"
            echo "backup=$job_backup"
            echo "converter_source=$converter_source"
            echo "converter_pinned=$converter"
            echo "converter_execution_path=$converter_exec"
            echo "converter_execution_authority=$converter_execution_authority"
            echo "converter_version=$converter_version"
            echo "converter_sha256=$converter_sha"
            echo "source_audit_report_source=$audit_source_report"
            echo "source_audit_report_pinned=$audit_report"
            echo "source_audit_report_sha256=$audit_sha"
            echo "operator_quiescence_authority_id=$operator_quiescence_authority_id"
            echo "operator_quiescence_required_for_entire_batch=true"
            echo "operator_quiescence_adversarial_proof=false"
            echo "target_post_audit_deferred=true"
            echo "canonical_manifest_deferred=true"
            echo "canonical_publication=false"
        } >"$job_log_source" \
            || fc_die "worker $job_worker cannot create log for epoch $job_epoch"
        if [ "$converter_execution_authority" = \
            read-only-private-path-with-immediate-pre-exec-sha256 ]; then
            [ "$(fc_sha256_file "$converter")" = "$converter_sha" ] \
                || fc_die "worker $job_worker pinned converter changed before epoch $job_epoch"
        fi
        nice -n 10 ionice -c 2 -n 7 "$converter_exec" \
            --fast-candidate \
            --source "$job_source" \
            --source-lease-id "$job_lease" \
            --target "$job_source" \
            --staging "$job_staging" \
            --epoch "$job_epoch" \
            --cluster-id "$cluster_id" \
            --generation-id "$job_generation" \
            --source-audit-report "$audit_report" \
            --source-audit-report-sha256 "$audit_sha" \
            >"$job_report_source" 2>>"$job_log_source" &
        worker_converter_pid=$!
        if wait "$worker_converter_pid"; then fc_status=0; else fc_status=$?; fi
        worker_converter_pid=
        [ "$fc_status" -eq 0 ] \
            || fc_die "worker $job_worker converter failed for epoch $job_epoch with status $fc_status"
        fc_validate_candidate_report "$job_report_source" "$job_epoch" "$job_worker" \
            || fc_die "worker $job_worker converter report is invalid for epoch $job_epoch"
    fi
    fc_publish_attempt_state "$job_report_source" "$job_log_source" "$job_report" "$job_log" \
        || fc_die "worker $job_worker cannot publish simple state for epoch $job_epoch"
    fc_publish_or_validate_result "$job_worker" "$job_epoch" "$job_claim" "$job_report" "$job_log" \
        || fc_die "worker $job_worker cannot publish result for epoch $job_epoch"
    echo "worker $job_worker epoch $job_epoch: Post candidate ready; canonical checks deferred"
}

[ "$#" -eq 4 ] || fc_usage
handoff_state_root=$1
fence_epoch=$2
converter=$3
operator_quiescence_authority_id=$4
converter_source=$converter

for fc_command in jq awk sed wc tr date nice ionice chmod mkdir rmdir ln rm \
    dd df cmp dirname basename kill ps sort; do
    fc_require_command "$fc_command"
done
if command -v sha256sum >/dev/null 2>&1; then
    sha256_program=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    sha256_program=shasum
else
    fc_die 'sha256sum or shasum is required'
fi

fc_require_absolute_single_line HANDOFF_STATE_ROOT "$handoff_state_root"
fc_require_absolute_single_line CONVERTER "$converter"
fc_require_nonempty_single_line OPERATOR_QUIESCENCE_AUTHORITY_ID \
    "$operator_quiescence_authority_id"
fc_validate_decimal FENCE_EPOCH "$fence_epoch"
[ -d "$handoff_state_root" ] && [ ! -L "$handoff_state_root" ] \
    || fc_die "handoff state root is not one real directory: $handoff_state_root"
[ "$(fc_canonical_directory "$handoff_state_root")" = "$handoff_state_root" ] \
    || fc_die 'HANDOFF_STATE_ROOT must already be canonical'
[ -f "$converter" ] && [ ! -L "$converter" ] && [ -x "$converter" ] \
    || fc_die "converter is not one real executable: $converter"
[ "$(fc_canonical_file "$converter")" = "$converter" ] \
    || fc_die 'CONVERTER must already be canonical'

request=$handoff_state_root/request.json
cohort=$handoff_state_root/cohort/cohort.json
conversion_state=$handoff_state_root/conversion
for fc_control in "$request" "$cohort"; do
    [ -f "$fc_control" ] && [ ! -L "$fc_control" ] \
        || fc_die "handoff control is absent: $fc_control"
done
[ -d "$conversion_state" ] && [ ! -L "$conversion_state" ] \
    || fc_die 'strict conversion state is absent'
archive_root=$(fc_json_field '.archive_root | select(type == "string")' "$request")
request_state=$(fc_json_field '.state_root | select(type == "string")' "$request")
cluster_id=$(fc_json_field '.cluster_id | select(type == "string")' "$request")
run_id=$(fc_json_field '.run_id | select(type == "string")' "$request")
[ "$request_state" = "$handoff_state_root" ] \
    || fc_die 'request is bound to a different handoff state root'
fc_require_absolute_single_line ARCHIVE_ROOT "$archive_root"
[ -d "$archive_root" ] && [ ! -L "$archive_root" ] \
    && [ "$(fc_canonical_directory "$archive_root")" = "$archive_root" ] \
    || fc_die 'archive root is not one canonical real directory'
jq -e --arg archive "$archive_root" '
    .schema_version == 1 and
    .kind == "archive-v2-pre-to-post-cohort" and
    .archive_root == $archive and
    (.reports | type == "array") and
    ([.reports[] | select(.classification == "legacy-pre") | .epoch] | length) > 0 and
    ([.reports[] | select(.classification == "legacy-pre") | .epoch] | unique | length) ==
        ([.reports[] | select(.classification == "legacy-pre") | .epoch] | length) and
    ([.reports[] | select(.classification == "legacy-pre") | .epoch] | sort) ==
        ((.conversion_epochs + .excluded_epochs) | sort)
' "$cohort" >/dev/null 2>&1 || fc_die 'cohort does not bind the exact LegacyPre set'

strict_lock=$conversion_state/.archive-v2-pre-to-post-manual.lock
[ ! -e "$strict_lock" ] && [ ! -L "$strict_lock" ] \
    || fc_die "strict converter is still locked: $strict_lock"

lock_dir=$archive_root/.archive-v2-pre-to-post-fast-candidate.lock
lock_held=0
worker_pid_0=
worker_pid_1=
fc_release_lock() {
    trap - 0 1 2 3 15
    if [ "$lock_held" -eq 1 ]; then
        rmdir "$lock_dir" 2>/dev/null \
            || echo "fast in-place runner: cannot remove archive-root lock: $lock_dir" >&2
        lock_held=0
    fi
}
fc_stop() {
    fc_status=$1
    trap - 0 1 2 3 15
    for fc_pid in "$worker_pid_0" "$worker_pid_1"; do
        [ -z "$fc_pid" ] || kill -TERM "$fc_pid" 2>/dev/null || :
    done
    for fc_pid in "$worker_pid_0" "$worker_pid_1"; do
        [ -z "$fc_pid" ] || wait "$fc_pid" 2>/dev/null || :
    done
    worker_pid_0=
    worker_pid_1=
    fc_release_lock
    exit "$fc_status"
}
trap 'fc_release_lock' 0
trap 'fc_stop 129' 1
trap 'fc_stop 130' 2
trap 'fc_stop 131' 3
trap 'fc_stop 143' 15
if ! mkdir "$lock_dir" 2>/dev/null; then
    fc_die "archive-root lock exists; another run or stopped run needs review: $lock_dir"
fi
lock_held=1

# The two file exchanges are not atomic to an uncoordinated reader. The
# operator authority is the safety condition for the full batch. These process
# checks are only start/end guards; they are not adversarial quiescence proof.
fc_check_no_named_archive_process() {
    fc_stage=$1
    fc_process_snapshot=$handoff_state_root/.fast-in-place-processes-$fc_stage-$$.txt
    [ ! -e "$fc_process_snapshot" ] && [ ! -L "$fc_process_snapshot" ] \
        || fc_die "process snapshot already exists at $fc_stage"
    # Run ps in this shell. A helper subshell inherits the coordinator argv;
    # when the state-root name starts with the archive-root name, that helper
    # can otherwise look like an unrelated archive reader.
    ps -eo pid=,args= >"$fc_process_snapshot" \
        || fc_die "cannot inspect running readers at $fc_stage"
    if awk -v self="$$" -v archive="$archive_root" '
        $1 != self && index($0, archive) {
            print > "/dev/stderr"; found = 1
        }
        END { exit found ? 0 : 1 }
    ' "$fc_process_snapshot"; then
        rm "$fc_process_snapshot" || :
        fc_die "a process still names the archive root at $fc_stage"
    fi
    rm "$fc_process_snapshot" \
        || fc_die "cannot remove the $fc_stage process snapshot"
}
fc_check_no_named_archive_process start

state_root=$handoff_state_root/fast-in-place-candidate
if [ -e "$state_root" ] || [ -L "$state_root" ]; then
    [ -d "$state_root" ] && [ ! -L "$state_root" ] \
        || fc_die 'fast in-place state is not one real directory'
else
    mkdir "$state_root" || fc_die 'cannot create fast in-place state'
fi
chmod 700 "$state_root" || fc_die 'cannot protect fast in-place state'
claims_dir=$state_root/claims
reports_dir=$state_root/reports
logs_dir=$state_root/logs
results_dir=$state_root/results
attempts_dir=$state_root/attempts
tools_dir=$state_root/tools
audit_dir=$state_root/source-audit-reports
for fc_dir in "$claims_dir" "$reports_dir" "$logs_dir" "$results_dir" \
    "$attempts_dir" "$tools_dir" "$audit_dir"; do
    if [ -e "$fc_dir" ] || [ -L "$fc_dir" ]; then
        [ -d "$fc_dir" ] && [ ! -L "$fc_dir" ] \
            || fc_die "state path is not one real directory: $fc_dir"
    else
        mkdir "$fc_dir" || fc_die "cannot create state directory: $fc_dir"
    fi
    chmod 700 "$fc_dir" || fc_die "cannot protect state directory: $fc_dir"
done

# Execute one private, read-only converter copy. Later changes to the source
# tool path cannot change a resumed batch.
converter=$tools_dir/archive-v2-pre-to-post
converter_binding=$tools_dir/archive-v2-pre-to-post.binding.json
converter_candidate=$converter.building-$$
if [ -e "$converter" ] || [ -L "$converter" ]; then
    [ -f "$converter" ] && [ ! -L "$converter" ] && [ -x "$converter" ] \
        && [ ! -w "$converter" ] \
        || fc_die 'pinned converter is not one read-only executable'
else
    [ ! -e "$converter_binding" ] && [ ! -L "$converter_binding" ] \
        || fc_die 'converter binding exists without its pinned executable'
    [ ! -e "$converter_candidate" ] && [ ! -L "$converter_candidate" ] \
        || fc_die 'unfinished pinned converter copy needs review'
    converter_source_sha_before=$(fc_sha256_file "$converter_source") \
        || fc_die 'cannot hash converter source before its private copy'
    fc_copy_file_exclusive "$converter_source" "$converter_candidate" \
        || fc_die 'cannot make the private converter copy'
    converter_source_sha_after=$(fc_sha256_file "$converter_source") \
        || fc_die 'cannot hash converter source after its private copy'
    converter_candidate_sha=$(fc_sha256_file "$converter_candidate") \
        || fc_die 'cannot hash the private converter copy'
    [ "$converter_source_sha_before" = "$converter_source_sha_after" ] \
        && [ "$converter_source_sha_before" = "$converter_candidate_sha" ] \
        || fc_die 'converter source changed while it was copied'
    chmod 500 "$converter_candidate" \
        || fc_die 'cannot make the private converter copy read-only'
    ln -n "$converter_candidate" "$converter" \
        || fc_die 'cannot publish the private converter without replacement'
    rm "$converter_candidate" || fc_die 'cannot remove converter copy candidate'
fi
converter_sha=$(fc_sha256_file "$converter") || fc_die 'cannot hash pinned converter'
converter_version=$("$converter" --version 2>/dev/null) \
    || fc_die 'pinned converter does not provide --version'
fc_require_nonempty_single_line CONVERTER_VERSION "$converter_version"
converter_binding_candidate=$converter_binding.building-$$
(
    set -C
    jq -cn --arg source "$converter_source" --arg pinned "$converter" \
        --arg sha "$converter_sha" --arg version "$converter_version" '
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-fast-pinned-converter",
            source_path:$source,
            pinned_path:$pinned,
            sha256:$sha,
            version:$version
        }
    ' >"$converter_binding_candidate"
) || fc_die 'cannot build pinned converter binding'
fc_publish_derived_file "$converter_binding_candidate" "$converter_binding" \
    || fc_die 'pinned converter binding differs; resume refused'
converter_exec=$converter
converter_execution_authority=read-only-private-path-with-immediate-pre-exec-sha256
converter_fd_open=0
if [ -d /proc/self/fd ]; then
    exec 9<"$converter" || fc_die 'cannot open pinned converter file descriptor'
    converter_fd_open=1
    [ "$(fc_sha256_file /proc/self/fd/9)" = "$converter_sha" ] \
        || fc_die 'open converter descriptor differs from its binding'
    [ "$(/proc/self/fd/9 --version 2>/dev/null)" = "$converter_version" ] \
        || fc_die 'open converter descriptor version differs from its binding'
    converter_exec=/proc/self/fd/9
    converter_execution_authority=read-only-private-inode-held-by-inherited-fd
fi

all_epoch_list=$state_root/all-legacy-pre-epochs.txt
report_table=$state_root/all-legacy-pre-reports.tsv
worker_0_list=$state_root/worker-0-epochs.txt
worker_1_list=$state_root/worker-1-epochs.txt
all_candidate=$all_epoch_list.building-$$
table_candidate=$report_table.building-$$
worker_0_candidate=$worker_0_list.building-$$
worker_1_candidate=$worker_1_list.building-$$
for fc_candidate in "$all_candidate" "$table_candidate" \
    "$worker_0_candidate" "$worker_1_candidate"; do
    [ ! -e "$fc_candidate" ] && [ ! -L "$fc_candidate" ] \
        || fc_die "unfinished list candidate needs review: $fc_candidate"
done
jq -r '[.reports[] | select(.classification == "legacy-pre") | .epoch] | sort[]' \
    "$cohort" >"$all_candidate" || fc_die 'cannot derive all LegacyPre epochs'
jq -r '[.reports[] | select(.classification == "legacy-pre")] | sort_by(.epoch)[] |
    [.epoch, .report_path, .report_sha256, .counts.blocks,
     .counts.compressed_block_bytes, .counts.uncompressed_block_bytes,
     .counts.typed_messages, .counts.pre_only, .counts.both_equivalent] | @tsv' \
    "$cohort" >"$table_candidate" \
    || fc_die 'cannot derive LegacyPre report table'
awk 'NR % 2 == 1 { print }' "$all_candidate" >"$worker_0_candidate" \
    || fc_die 'cannot derive worker-0 list'
awk 'NR % 2 == 0 { print }' "$all_candidate" >"$worker_1_candidate" \
    || fc_die 'cannot derive worker-1 list'
for fc_pair in "$all_candidate:$all_epoch_list" "$table_candidate:$report_table" \
    "$worker_0_candidate:$worker_0_list" "$worker_1_candidate:$worker_1_list"; do
    fc_candidate=${fc_pair%%:*}
    fc_final=${fc_pair#*:}
    fc_publish_derived_file "$fc_candidate" "$fc_final" \
        || fc_die "immutable admitted list changed: $fc_final"
done
epoch_count=$(fc_validate_epoch_list "$all_epoch_list") \
    || fc_die 'all-LegacyPre epoch list is not strictly ordered and unique'
[ "$(wc -l <"$report_table" | tr -d '[:space:]')" -eq "$epoch_count" ] \
    || fc_die 'LegacyPre report table count differs from epoch list'
awk -v fence="$fence_epoch" '$0 == fence { found++ } END { exit found == 1 ? 0 : 1 }' \
    "$all_epoch_list" || fc_die 'fence epoch is not in the exact LegacyPre list'

# Pin each small scanner report once. The converter also receives the admitted
# digest, so a changed path cannot authorize archive mutation.
while IFS= read -r fc_epoch || [ -n "$fc_epoch" ]; do
    fc_parse_report_binding "$fc_epoch" \
        || fc_die "cannot parse scanner-report binding for epoch $fc_epoch"
    pin_epoch_source=$archive_root/epoch-$fc_epoch
    fc_audit_candidate=$audit_report.building-$$
    if [ -e "$audit_report" ] || [ -L "$audit_report" ]; then
        [ -f "$audit_report" ] && [ ! -L "$audit_report" ] \
            && [ ! -w "$audit_report" ] \
            || fc_die "pinned scanner report is not read-only for epoch $fc_epoch"
    else
        [ -f "$audit_source_report" ] && [ ! -L "$audit_source_report" ] \
            || fc_die "source scanner report is absent for epoch $fc_epoch"
        [ ! -e "$fc_audit_candidate" ] && [ ! -L "$fc_audit_candidate" ] \
            || fc_die "unfinished scanner-report copy needs review for epoch $fc_epoch"
        [ "$(fc_sha256_file "$audit_source_report")" = "$audit_sha" ] \
            || fc_die "source scanner report changed for epoch $fc_epoch"
        fc_validate_scanner_report "$audit_source_report" "$fc_epoch" "$pin_epoch_source" "$audit_sha" \
            || fc_die "source scanner report is invalid for epoch $fc_epoch"
        fc_copy_file_exclusive "$audit_source_report" "$fc_audit_candidate" \
            || fc_die "cannot copy scanner report for epoch $fc_epoch"
        [ "$(fc_sha256_file "$audit_source_report")" = "$audit_sha" ] \
            && [ "$(fc_sha256_file "$fc_audit_candidate")" = "$audit_sha" ] \
            || fc_die "scanner report changed while copied for epoch $fc_epoch"
        fc_publish_derived_file "$fc_audit_candidate" "$audit_report" \
            || fc_die "cannot publish pinned scanner report for epoch $fc_epoch"
    fi
    fc_validate_scanner_report "$audit_report" "$fc_epoch" "$pin_epoch_source" "$audit_sha" \
        || fc_die "pinned scanner report is invalid for epoch $fc_epoch"
done <"$all_epoch_list"

config=$state_root/config.json
config_candidate=$config.building-$$
(
    set -C
    jq -cn --arg run_id "$run_id" --arg cluster "$cluster_id" \
        --arg archive "$archive_root" --arg state "$state_root" \
        --argjson fence "$fence_epoch" --argjson count "$epoch_count" \
        --arg converter "$converter" --arg version "$converter_version" \
        --arg converter_sha "$converter_sha" \
        --arg converter_source "$converter_source" \
        --arg converter_exec "$converter_exec" \
        --arg converter_authority "$converter_execution_authority" \
        --arg converter_binding "$converter_binding" \
        --arg audits "$audit_dir" \
        --arg maintenance "$operator_quiescence_authority_id" \
        --arg epochs "$all_epoch_list" --arg reports "$report_table" \
        --arg worker0 "$worker_0_list" --arg worker1 "$worker_1_list" '
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-fast-in-place-config",
            run_id:$run_id,
            cluster_id:$cluster,
            archive_root:$archive,
            state_root:$state,
            fence_epoch:$fence,
            worker_count:2,
            epoch_count:$count,
            converter:{
                source_path:$converter_source,pinned_path:$converter,
                execution_path:$converter_exec,
                execution_authority:$converter_authority,
                binding:$converter_binding,version:$version,sha256:$converter_sha,
                mode:"--fast-candidate"
            },
            epoch_list:$epochs,
            source_audit_table:$reports,
            pinned_source_audit_directory:$audits,
            worker_epoch_lists:[$worker0,$worker1],
            operator_maintenance_quiescence:{
                authority_id:$maintenance,
                required_for_entire_batch:true,
                scheduler_stopped:true,
                gateway_and_indexer_readers_stopped:true,
                other_archive_tools_stopped:true,
                cooperative_archive_root_lock:true,
                process_checks:["start","end"],
                adversarial_proof:false
            },
            backup_policy:"retain-old-pair-and-stale-controls",
            target_post_audit_deferred:true,
            canonical_manifest_deferred:true,
            canonical_publication:false
        }
    ' >"$config_candidate"
) || fc_die 'cannot create config candidate'
fc_publish_derived_file "$config_candidate" "$config" \
    || fc_die 'fast in-place config differs; resume refused'

batch_complete=$state_root/batch-complete.json
exec 3<"$worker_0_list" || fc_die 'cannot open worker-0 list'
exec 4<"$worker_1_list" || fc_die 'cannot open worker-1 list'
while :; do
    epoch_0=
    epoch_1=
    IFS= read -r epoch_0 <&3 || epoch_0=
    IFS= read -r epoch_1 <&4 || epoch_1=
    [ -n "$epoch_0" ] || [ -n "$epoch_1" ] || break
    [ ! -e "$strict_lock" ] && [ ! -L "$strict_lock" ] \
        || fc_die 'strict converter restarted during fast in-place run'
    fc_round=
    [ -z "$epoch_0" ] || fc_round="0:$epoch_0"
    if [ -n "$epoch_1" ]; then
        [ -z "$fc_round" ] && fc_round="1:$epoch_1" || fc_round="$fc_round 1:$epoch_1"
    fi
    # Word splitting is intentional for validated WORKER:EPOCH pairs.
    # shellcheck disable=SC2086
    fc_admit_round_space $fc_round
    if [ -n "$epoch_0" ]; then
        fc_epoch_job 0 "$epoch_0" &
        worker_pid_0=$!
    fi
    if [ -n "$epoch_1" ]; then
        fc_epoch_job 1 "$epoch_1" &
        worker_pid_1=$!
    fi
    status_0=0
    status_1=0
    if [ -n "$worker_pid_0" ]; then
        if wait "$worker_pid_0"; then status_0=0; else status_0=$?; fi
    fi
    if [ -n "$worker_pid_1" ]; then
        if wait "$worker_pid_1"; then status_1=0; else status_1=$?; fi
    fi
    worker_pid_0=
    worker_pid_1=
    if [ "$status_0" -ne 0 ] || [ "$status_1" -ne 0 ]; then
        fc_die "round failed (worker-0=$status_0 worker-1=$status_1); no later epoch was started"
    fi
done
exec 3<&-
exec 4<&-
if [ "$converter_fd_open" -eq 1 ]; then
    exec 9<&-
    converter_fd_open=0
fi

results_json='[]'
result_index=0
while IFS= read -r fc_epoch || [ -n "$fc_epoch" ]; do
    fc_worker=$((result_index % 2))
    fc_validate_completed_epoch "$fc_worker" "$fc_epoch" \
        || fc_die "final simple result is invalid for epoch $fc_epoch"
    results_json=$(jq -cn --argjson rows "$results_json" \
        --argjson epoch "$fc_epoch" --argjson worker "$fc_worker" \
        --arg result "$results_dir/epoch-$fc_epoch.json" \
        '$rows + [{epoch:$epoch,worker:$worker,result:$result}]') \
        || fc_die 'cannot assemble exact result list'
    result_index=$((result_index + 1))
done <"$all_epoch_list"
[ "$result_index" -eq "$epoch_count" ] || fc_die 'final result count differs'
fc_check_no_named_archive_process end

if [ -e "$batch_complete" ] || [ -L "$batch_complete" ]; then
    jq -e --arg run_id "$run_id" --arg cluster "$cluster_id" \
        --arg archive "$archive_root" --arg state "$state_root" \
        --arg maintenance "$operator_quiescence_authority_id" \
        --argjson fence "$fence_epoch" --argjson count "$epoch_count" \
        --argjson results "$results_json" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-fast-in-place-batch-complete" and
        .run_id == $run_id and .cluster_id == $cluster and
        .archive_root == $archive and .state_root == $state and
        .fence_epoch == $fence and .worker_count == 2 and
        .epoch_count == $count and .completed_epochs == $count and
        .results == $results and
        .backups_retained == true and
        .operator_maintenance_quiescence.authority_id == $maintenance and
        .operator_maintenance_quiescence.required_for_entire_batch == true and
        .operator_maintenance_quiescence.process_checks == ["start","end"] and
        .operator_maintenance_quiescence.adversarial_proof == false and
        .target_post_audit_deferred == true and
        .canonical_manifest_deferred == true and
        .canonical_publication == false and
        (.completed_at_utc | type == "string")
    ' "$batch_complete" >/dev/null 2>&1 \
        || fc_die 'batch-complete record differs; resume refused'
else
    completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) || fc_die 'cannot get completion time'
    complete_candidate=$batch_complete.building-$$
    (
        set -C
        jq -cn --arg run_id "$run_id" --arg cluster "$cluster_id" \
            --arg archive "$archive_root" --arg state "$state_root" \
            --arg maintenance "$operator_quiescence_authority_id" \
            --argjson fence "$fence_epoch" --argjson count "$epoch_count" \
            --argjson results "$results_json" --arg completed "$completed_at" '
            {
                schema_version:1,
                kind:"archive-v2-pre-to-post-fast-in-place-batch-complete",
                run_id:$run_id,
                cluster_id:$cluster,
                archive_root:$archive,
                state_root:$state,
                fence_epoch:$fence,
                worker_count:2,
                epoch_count:$count,
                completed_epochs:$count,
                results:$results,
                backups_retained:true,
                operator_maintenance_quiescence:{
                    authority_id:$maintenance,
                    required_for_entire_batch:true,
                    scheduler_stopped:true,
                    gateway_and_indexer_readers_stopped:true,
                    other_archive_tools_stopped:true,
                    cooperative_archive_root_lock:true,
                    process_checks:["start","end"],
                    adversarial_proof:false
                },
                target_post_audit_deferred:true,
                canonical_manifest_deferred:true,
                canonical_publication:false,
                completed_at_utc:$completed
            }
        ' >"$complete_candidate"
    ) || fc_die 'cannot create batch-complete record'
    fc_publish_derived_file "$complete_candidate" "$batch_complete" \
        || fc_die 'cannot publish batch-complete record'
fi

echo "all $epoch_count LegacyPre epochs are Post candidates in place; backups remain and canonical checks are deferred"
