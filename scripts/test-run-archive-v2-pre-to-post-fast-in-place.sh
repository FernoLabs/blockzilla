#!/bin/sh
# Hostile, payload-small tests for the two-worker fast in-place coordinator.
# A converter double models the durable pair switch; production format tests
# cover real wincode/zstd rewriting.

set -eu
umask 077

fail() {
    echo "fast in-place coordinator self-test: $*" >&2
    exit 1
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
runner=$script_dir/run-archive-v2-pre-to-post-fast-in-place.sh
runner_shell=${FAST_IN_PLACE_RUNNER_SHELL:-/bin/sh}
[ -x "$runner_shell" ] || fail "runner shell is not executable: $runner_shell"
test_root=$(mktemp -d "${TMPDIR:-/tmp}/archive-v2-fast-in-place-test.XXXXXX") \
    || fail 'cannot create test root'
test_root=$(CDPATH= cd -P "$test_root" && pwd -P)
case "${test_root##*/}" in archive-v2-fast-in-place-test.*) ;; *) fail 'unsafe test root' ;; esac
cleanup() {
    trap - 0 1 2 3 15
    if [ "${KEEP_FAST_IN_PLACE_TEST_ROOT:-0}" = 1 ]; then
        echo "kept test root: $test_root" >&2
        return
    fi
    chmod -R u+w "$test_root" 2>/dev/null || :
    rm -rf "$test_root"
}
trap 'cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

for command in jq awk sed wc tr dd; do
    command -v "$command" >/dev/null 2>&1 || fail "$command is required"
done

# Stable process and free-space doubles keep this test independent of desktop
# sandbox process-table and disk-size policy.
printf '%s\n' '#!/bin/sh' \
    'printf "  1 /sbin/init\\n"' \
    'if [ -n "${FAKE_PS_INHERITED_ARGV:-}" ]; then' \
    '  printf "  %s %s\\n" "$PPID" "$FAKE_PS_INHERITED_ARGV"' \
    'fi' >"$test_root/ps"
printf '#!/bin/sh\necho "Filesystem 1024-blocks Used Available Capacity Mounted on"\necho "fake 999999999 1 999999998 1%% /"\n' >"$test_root/df"
printf '%s\n' '#!/bin/sh' \
    'while [ "$#" -gt 0 ]; do' \
    '  case "$1" in -c|-n) shift 2 ;; *) break ;; esac' \
    'done' \
    'exec "$@"' >"$test_root/ionice"
chmod 700 "$test_root/ps" "$test_root/df" "$test_root/ionice"

fake_converter=$test_root/archive-v2-pre-to-post-fast-test
cat >"$fake_converter" <<'FAKE_CONVERTER'
#!/bin/sh
set -eu

if [ "$#" -eq 1 ] && [ "$1" = --version ]; then
    echo 'archive-v2-pre-to-post-fast-test 1.0.0'
    exit 0
fi

hash_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

fast=0
source_path=
target_path=
staging_path=
epoch=
cluster=
generation=
lease=
audit=
audit_expected_sha=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --fast-candidate) fast=1; shift ;;
        --source) source_path=$2; shift 2 ;;
        --source-lease-id) lease=$2; shift 2 ;;
        --target) target_path=$2; shift 2 ;;
        --staging) staging_path=$2; shift 2 ;;
        --epoch) epoch=$2; shift 2 ;;
        --cluster-id) cluster=$2; shift 2 ;;
        --generation-id) generation=$2; shift 2 ;;
        --source-audit-report) audit=$2; shift 2 ;;
        --source-audit-report-sha256) audit_expected_sha=$2; shift 2 ;;
        *) exit 64 ;;
    esac
done
[ "$fast" -eq 1 ] && [ "$target_path" = "$source_path" ] && [ -n "$lease" ]
control=${FAKE_CONTROL_ROOT:?}
mkdir -p "$control"
: >"$control/entered-$epoch"

stop() {
    trap - 1 2 3 15
    [ -z "${sleeper:-}" ] || kill -TERM "$sleeper" 2>/dev/null || :
    [ -z "${sleeper:-}" ] || wait "$sleeper" 2>/dev/null || :
    : >"$control/terminated-$epoch"
    exit 143
}
trap 'stop' 1 2 3 15
if [ -e "$control/sleep-all" ] || [ -e "$control/sleep-$epoch" ]; then
    sleep 30 &
    sleeper=$!
    wait "$sleeper"
    sleeper=
fi
if [ -e "$control/fail-$epoch" ] && [ ! -e "$control/failed-$epoch" ]; then
    : >"$control/failed-$epoch"
    exit 23
fi

parent=${source_path%/*}
backup_path=$parent/.epoch-$epoch.pre-to-post.backup
descriptor=$source_path/archive-v2-pre-to-post.candidate.v1.json
audit_sha=$(hash_file "$audit")
[ "$audit_sha" = "$audit_expected_sha" ]
audit_bytes=$(wc -c <"$audit" | tr -d '[:space:]')
counts=$(jq -c '.counts' "$audit")
source_blocks_sha=$(hash_file "$source_path/archive-v2-blocks.zstd")
source_index_sha=$(hash_file "$source_path/archive-v2-blocks.index")

write_descriptor() {
    candidate_blocks_bytes=$(wc -c <"$staging_path/archive-v2-blocks.zstd" | tr -d '[:space:]')
    candidate_index_bytes=$(wc -c <"$staging_path/archive-v2-blocks.index" | tr -d '[:space:]')
    candidate_blocks_sha=$(hash_file "$staging_path/archive-v2-blocks.zstd")
    candidate_index_sha=$(hash_file "$staging_path/archive-v2-blocks.index")
    jq -n --argjson epoch "$epoch" --arg cluster "$cluster" \
        --arg generation "$generation" --arg source "$source_path" \
        --arg backup "$backup_path" --arg audit "$audit" --arg audit_sha "$audit_sha" \
        --arg switch_lock "$parent/.archive-v2-pre-to-post.switch.lock" \
        --arg source_blocks_sha "$source_blocks_sha" --arg source_index_sha "$source_index_sha" \
        --arg candidate_blocks_sha "$candidate_blocks_sha" \
        --arg candidate_index_sha "$candidate_index_sha" \
        --argjson candidate_blocks_bytes "$candidate_blocks_bytes" \
        --argjson candidate_index_bytes "$candidate_index_bytes" \
        --argjson audit_bytes "$audit_bytes" --argjson counts "$counts" '
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-candidate",
            state:"unfinalized",
            canonical:false,
            epoch:$epoch,
            cluster_id:$cluster,
            prospective_generation_id:$generation,
            source:$source,
            candidate:$source,
            backup:$backup,
            source_profile_evidence:"external-whole-generation-scan-report",
            source_audit_report:{
                path:$audit,bytes:$audit_bytes,sha256:$audit_sha,
                completed_unix_seconds:1,workers:2,counts:$counts
            },
            expected_wire_profile_after_rewrite:"post-unknown-instruction-fallbacks-v1",
            source_full_audit_performed_in_this_run:false,
            source_audit_report_reused:true,
            single_decode_rewrite_pass:true,
            outer_block_bytes_preserved_verbatim_except_messages:true,
            sidecars_copied:false,
            sidecars_rewritten:false,
            pair_swap_requires_external_reader_quiescence:true,
            archive_root_switch_lock:$switch_lock,
            source_files:{
                "archive-v2-blocks.zstd":{bytes:10,sha256:$source_blocks_sha},
                "archive-v2-blocks.index":{bytes:88,sha256:$source_index_sha}
            },
            source_inventory:{
                "archive-v2-blocks.zstd":{bytes:10,disposition:"rewrite"},
                "archive-v2-blocks.index":{bytes:88,disposition:"rewrite"},
                "registry.bin":{bytes:11,disposition:"copy-durable"},
                "archive-v2-block-access.wincode":{bytes:9,disposition:"omit-edge"},
                "archive-v2-block-access.index":{bytes:15,disposition:"omit-edge"},
                "block-time-gaps.bin":{bytes:6,disposition:"copy-durable"},
                ".block-time-gaps.bin.lock":{bytes:1,disposition:"omit-control"},
                ".hivezilla-pipeline-owned.v1.json":{bytes:1,disposition:"omit-control"},
                ".complete-hot-v2-no-access-delete-car":{bytes:1,disposition:"omit-control"},
                ".complete-hot-v2-shredding-sidecar-v2":{bytes:1,disposition:"omit-control"},
                ".archive-v2-manifest-publish.lock":{bytes:1,disposition:"omit-control"},
                "archive-v2-generation.json":{bytes:11,disposition:"omit-control"},
                "archive-v2-registry-reprocess.receipt.json":{
                    bytes:1,disposition:"omit-control"
                },
                "archive-v2-pre-to-post.receipt.json":{bytes:1,disposition:"omit-control"},
                "archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker":{
                    bytes:1,disposition:"omit-control"
                },
                "archive-v2-get-block.index":{bytes:12,disposition:"omit-edge"},
                "archive-v2-block-access.index.pre-votehash-20260523T205501+0200":{
                    bytes:1,disposition:"omit-edge"
                },
                "archive-v2-get-block.index.pre-votehash-20260523T205501+0200":{
                    bytes:1,disposition:"omit-edge"
                },
                "archive-v2-blocks.wincode":{bytes:1,disposition:"omit-obsolete-block"},
                "archive-v2-blocks.wincode.zst":{bytes:1,disposition:"omit-obsolete-block"}
            },
            candidate_rewrite_files:{
                "archive-v2-blocks.zstd":{
                    bytes:$candidate_blocks_bytes,sha256:$candidate_blocks_sha
                },
                "archive-v2-blocks.index":{
                    bytes:$candidate_index_bytes,sha256:$candidate_index_sha
                }
            },
            retained_durable_files:["registry.bin"],
            retained_edge_files:[
                "archive-v2-block-access.wincode","archive-v2-block-access.index"
            ],
            retained_edge_files_authoritative:false,
            retained_edge_validation_deferred:true,
            get_block_index_rebuild_required:true,
            moved_to_backup:[
                ".archive-v2-manifest-publish.lock",
                ".block-time-gaps.bin.lock",
                ".complete-hot-v2-no-access-delete-car",
                ".complete-hot-v2-shredding-sidecar-v2",
                ".hivezilla-pipeline-owned.v1.json",
                "archive-v2-block-access.index.pre-votehash-20260523T205501+0200",
                "archive-v2-blocks.wincode",
                "archive-v2-blocks.wincode.zst",
                "archive-v2-generation.json",
                "archive-v2-get-block.index",
                "archive-v2-get-block.index.pre-votehash-20260523T205501+0200",
                "archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker",
                "archive-v2-pre-to-post.receipt.json",
                "archive-v2-registry-reprocess.receipt.json",
                "block-time-gaps.bin"
            ],
            rewrite:{
                blocks:$counts.blocks,
                typed_messages:$counts.typed_messages,
                message_input_bytes:10,
                message_output_bytes:10,
                message_mismatch_bytes:$counts.pre_only,
                source_instruction_data_tag_counts:[0,$counts.pre_only,0,0,0,0,0,0,0]
            },
            exact_message_length_preserved:true,
            exact_message_delta_proved:true,
            metadata_regions_copied_verbatim:true,
            canonical_publication_deferred:true,
            target_post_audit_performed:false,
            canonical_manifest_written:false,
            canonical_profile_marker_written:false,
            canonical_migration_receipt_written:false,
            source_provider_snapshot_required:false,
            source_linux_read_leases_required:true
        }
    ' >"$staging_path/archive-v2-pre-to-post.candidate.v1.json"
}

do_switch() {
    mkdir "$staging_path"
    printf 'post-blocks-%s\n' "$epoch" >"$staging_path/archive-v2-blocks.zstd"
    dd if=/dev/zero bs=88 count=1 of="$staging_path/archive-v2-blocks.index" 2>/dev/null
    write_descriptor
    descriptor_staged=$staging_path/archive-v2-pre-to-post.candidate.v1.json
    descriptor_bytes=$(wc -c <"$descriptor_staged" | tr -d '[:space:]')
    descriptor_sha=$(hash_file "$descriptor_staged")
    candidate_blocks_bytes=$(wc -c <"$staging_path/archive-v2-blocks.zstd" | tr -d '[:space:]')
    jq -n --argjson epoch "$epoch" --arg cluster "$cluster" \
        --arg generation "$generation" --arg source "$source_path" \
        --arg staging "$staging_path" --arg backup "$backup_path" \
        --arg descriptor_sha "$descriptor_sha" --argjson descriptor_bytes "$descriptor_bytes" \
        --arg audit "$audit" --arg audit_sha "$audit_sha" \
        --argjson audit_bytes "$audit_bytes" --argjson candidate_bytes "$candidate_blocks_bytes" \
        --slurpfile descriptor_doc "$descriptor_staged" '
        ($descriptor_doc[0]) as $d |
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-pair-swap-intent",
            epoch:$epoch,
            cluster_id:$cluster,
            prospective_generation_id:$generation,
            candidate:$source,
            staging:$staging,
            backup:$backup,
            source_blocks:{bytes:10,device_id:1,inode:11},
            source_blocks_binding:$d.source_files["archive-v2-blocks.zstd"],
            source_index:{bytes:88,device_id:1,inode:12},
            source_index_binding:$d.source_files["archive-v2-blocks.index"],
            candidate_blocks:{bytes:$candidate_bytes,device_id:1,inode:21},
            candidate_blocks_binding:$d.candidate_rewrite_files["archive-v2-blocks.zstd"],
            candidate_index:{bytes:88,device_id:1,inode:22},
            candidate_index_binding:$d.candidate_rewrite_files["archive-v2-blocks.index"],
            candidate_descriptor:{bytes:$descriptor_bytes,sha256:$descriptor_sha},
            moved_to_backup:$d.moved_to_backup,
            retained_edge_files:$d.retained_edge_files,
            source_audit_report_path:$audit,
            source_audit_report:{bytes:$audit_bytes,sha256:$audit_sha}
        }
    ' >"$staging_path/archive-v2-pre-to-post.switch-intent.v1.json"
    mkdir "$staging_path/disabled"
    mv "$source_path/archive-v2-blocks.zstd" "$source_path/.old-blocks"
    mv "$staging_path/archive-v2-blocks.zstd" "$source_path/archive-v2-blocks.zstd"
    mv "$source_path/.old-blocks" "$staging_path/archive-v2-blocks.zstd"
    mv "$source_path/archive-v2-blocks.index" "$source_path/.old-index"
    mv "$staging_path/archive-v2-blocks.index" "$source_path/archive-v2-blocks.index"
    mv "$source_path/.old-index" "$staging_path/archive-v2-blocks.index"
    for name in \
        block-time-gaps.bin .block-time-gaps.bin.lock \
        .hivezilla-pipeline-owned.v1.json \
        .complete-hot-v2-no-access-delete-car \
        .complete-hot-v2-shredding-sidecar-v2 \
        .archive-v2-manifest-publish.lock \
        archive-v2-generation.json archive-v2-registry-reprocess.receipt.json \
        archive-v2-pre-to-post.receipt.json \
        archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker \
        archive-v2-get-block.index \
        archive-v2-block-access.index.pre-votehash-20260523T205501+0200 \
        archive-v2-get-block.index.pre-votehash-20260523T205501+0200 \
        archive-v2-blocks.wincode archive-v2-blocks.wincode.zst; do
        [ ! -e "$source_path/$name" ] || mv "$source_path/$name" "$staging_path/disabled/$name"
    done
    mv "$staging_path/archive-v2-pre-to-post.candidate.v1.json" "$descriptor"
    mv "$staging_path" "$backup_path"
    intent_sha=$(hash_file "$backup_path/archive-v2-pre-to-post.switch-intent.v1.json")
    jq -n --argjson epoch "$epoch" --arg source "$source_path" \
        --arg backup "$backup_path" --arg descriptor_sha "$descriptor_sha" \
        --arg audit_sha "$audit_sha" --arg intent_sha "$intent_sha" \
        --slurpfile intent_doc "$backup_path/archive-v2-pre-to-post.switch-intent.v1.json" '
        ($intent_doc[0]) as $i |
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-pair-swap-complete",
            epoch:$epoch,
            canonical:false,
            candidate:$source,
            backup:$backup,
            intent_sha256:$intent_sha,
            candidate_descriptor_sha256:$descriptor_sha,
            source_audit_report_sha256:$audit_sha,
            source_blocks_sha256:$i.source_blocks_binding.sha256,
            source_index_sha256:$i.source_index_binding.sha256,
            candidate_blocks_sha256:$i.candidate_blocks_binding.sha256,
            candidate_index_sha256:$i.candidate_index_binding.sha256
        }
    ' >"$backup_path/archive-v2-pre-to-post.switch-complete.v1.json"
}

emit_report() {
    descriptor_bytes=$(wc -c <"$descriptor" | tr -d '[:space:]')
    descriptor_sha=$(hash_file "$descriptor")
    jq -n --argjson epoch "$epoch" --arg cluster "$cluster" \
        --arg generation "$generation" --arg source "$source_path" \
        --arg backup "$backup_path" --arg descriptor "$descriptor" \
        --arg switch_lock "$parent/.archive-v2-pre-to-post.switch.lock" \
        --arg audit "$audit" --arg audit_sha "$audit_sha" \
        --arg descriptor_sha "$descriptor_sha" --argjson descriptor_bytes "$descriptor_bytes" \
        --argjson audit_bytes "$audit_bytes" --argjson counts "$counts" \
        --slurpfile descriptor_doc "$descriptor" '
        ($descriptor_doc[0]) as $d |
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-candidate-report",
            state:"unfinalized",
            canonical:false,
            epoch:$epoch,
            cluster_id:$cluster,
            prospective_generation_id:$generation,
            source:$source,
            candidate:$source,
            backup:$backup,
            candidate_descriptor:$descriptor,
            candidate_descriptor_bytes:$descriptor_bytes,
            candidate_descriptor_sha256:$descriptor_sha,
            source_profile_evidence:"external-whole-generation-scan-report",
            source_audit_report:$audit,
            source_audit_report_bytes:$audit_bytes,
            source_audit_report_sha256:$audit_sha,
            expected_wire_profile_after_rewrite:"post-unknown-instruction-fallbacks-v1",
            source_scan_counts:$counts,
            zstd_level:3,
            source_full_audit_performed_in_this_run:false,
            source_audit_report_reused:true,
            single_decode_rewrite_pass:true,
            outer_block_bytes_preserved_verbatim_except_messages:true,
            sidecars_copied:false,
            sidecars_rewritten:false,
            pair_swap_requires_external_reader_quiescence:true,
            archive_root_switch_lock:$switch_lock,
            rewrite:$d.rewrite,
            retained_durable_files:$d.retained_durable_files,
            retained_edge_files:$d.retained_edge_files,
            moved_to_backup:$d.moved_to_backup,
            canonical_publication_deferred:true,
            target_post_audit_performed:false,
            canonical_manifest_written:false,
            canonical_profile_marker_written:false,
            canonical_migration_receipt_written:false,
            required_finalization:[],
            edge_rebuild_required:true,
            rewritten_files_read_only:true,
            source_provider_snapshot_required:false,
            source_linux_read_leases_required:true,
            elapsed_seconds:0.01
        }
    '
}

emit_recovery_report() {
    descriptor_bytes=$(wc -c <"$descriptor" | tr -d '[:space:]')
    descriptor_sha=$(hash_file "$descriptor")
    jq -n --argjson epoch "$epoch" --arg cluster "$cluster" \
        --arg generation "$generation" --arg source "$source_path" \
        --arg backup "$backup_path" --arg descriptor "$descriptor" \
        --arg descriptor_sha "$descriptor_sha" --argjson descriptor_bytes "$descriptor_bytes" \
        --arg audit "$audit" --arg audit_sha "$audit_sha" \
        --argjson audit_bytes "$audit_bytes" '
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-candidate-recovery-report",
            state:"unfinalized",
            canonical:false,
            epoch:$epoch,
            cluster_id:$cluster,
            prospective_generation_id:$generation,
            candidate:$source,
            backup:$backup,
            candidate_descriptor:$descriptor,
            candidate_descriptor_bytes:$descriptor_bytes,
            candidate_descriptor_sha256:$descriptor_sha,
            source_audit_report:$audit,
            source_audit_report_bytes:$audit_bytes,
            source_audit_report_sha256:$audit_sha,
            recovered_switch:true,
            already_complete:true
        }
    '
}

if [ -d "$backup_path" ] && [ -f "$descriptor" ]; then
    emit_recovery_report
    exit 0
fi
do_switch
if [ -e "$control/fail-after-swap-$epoch" ] \
    && [ ! -e "$control/failed-after-swap-$epoch" ]; then
    : >"$control/failed-after-swap-$epoch"
    exit 29
fi
emit_report
FAKE_CONVERTER
chmod 500 "$fake_converter"

make_scanner_report() {
    msr_path=$1
    msr_archive=$2
    msr_epoch=$3
    jq -n --argjson epoch "$msr_epoch" --arg archive "$msr_archive" '
        {
            schema_version:1,
            kind:"archive-v2-wire-profile-scan",
            archive:$archive,
            epoch:$epoch,
            workers:2,
            classification:"legacy-pre",
            action:"convert-to-post",
            counts:{
                blocks:1,
                compressed_block_bytes:10,
                uncompressed_block_bytes:20,
                typed_messages:3,
                owned_fallback_blocks:0,
                raw_transaction_fallbacks:0,
                post_only:0,
                pre_only:3,
                both_equivalent:0,
                both_divergent:0,
                invalid:0
            },
            first_evidence:{
                post_only:null,
                pre_only:{slot:1,transaction_index:0},
                both_divergent:null,
                invalid:null
            },
            error:null,
            elapsed_seconds:1.0,
            completed_unix_seconds:1
        }
    ' >"$msr_path"
}

make_fixture() {
    mf_root=$1
    mkdir -p "$mf_root/handoff/cohort" "$mf_root/handoff/conversion" \
        "$mf_root/archive" "$mf_root/strict-target" "$mf_root/reports" "$mf_root/control"
    mf_root=$(CDPATH= cd -P "$mf_root" && pwd -P)
    mf_handoff=$mf_root/handoff
    mf_archive=$mf_root/archive
    mf_target=$mf_root/strict-target
    mf_reports=$mf_root/reports
    mf_rows=$mf_root/report-rows.jsonl
    : >"$mf_rows"
    for mf_epoch in 2 4 6 8; do
        mf_epoch_dir=$mf_archive/epoch-$mf_epoch
        mkdir "$mf_epoch_dir"
        printf '0123456789' >"$mf_epoch_dir/archive-v2-blocks.zstd"
        dd if=/dev/zero bs=88 count=1 of="$mf_epoch_dir/archive-v2-blocks.index" 2>/dev/null
        printf 'registry %s\n' "$mf_epoch" >"$mf_epoch_dir/registry.bin"
        printf 'access %s\n' "$mf_epoch" >"$mf_epoch_dir/archive-v2-block-access.wincode"
        printf 'access-index %s\n' "$mf_epoch" >"$mf_epoch_dir/archive-v2-block-access.index"
        printf 'gap %s\n' "$mf_epoch" >"$mf_epoch_dir/block-time-gaps.bin"
        printf 'manifest %s\n' "$mf_epoch" >"$mf_epoch_dir/archive-v2-generation.json"
        printf 'get-block %s\n' "$mf_epoch" >"$mf_epoch_dir/archive-v2-get-block.index"
        for mf_stale in \
            .block-time-gaps.bin.lock \
            .hivezilla-pipeline-owned.v1.json \
            .complete-hot-v2-no-access-delete-car \
            .complete-hot-v2-shredding-sidecar-v2 \
            .archive-v2-manifest-publish.lock \
            archive-v2-registry-reprocess.receipt.json \
            archive-v2-pre-to-post.receipt.json \
            archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker \
            archive-v2-block-access.index.pre-votehash-20260523T205501+0200 \
            archive-v2-get-block.index.pre-votehash-20260523T205501+0200 \
            archive-v2-blocks.wincode archive-v2-blocks.wincode.zst; do
            printf x >"$mf_epoch_dir/$mf_stale"
        done
        mf_report=$mf_reports/epoch-$mf_epoch.json
        make_scanner_report "$mf_report" "$mf_epoch_dir" "$mf_epoch"
        mf_sha=$(sha256_file "$mf_report")
        jq -cn --argjson epoch "$mf_epoch" --arg path "$mf_report" --arg sha "$mf_sha" \
            --arg archive "$mf_epoch_dir" --slurpfile report "$mf_report" '
            {
                epoch:$epoch,report_set:"base",report_path:$path,report_sha256:$sha,
                archive:$archive,classification:"legacy-pre",action:"convert-to-post",
                counts:$report[0].counts
            }
        ' >>"$mf_rows"
    done
    jq -s --arg archive "$mf_archive" --arg target "$mf_target" '
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-cohort",
            archive_root:$archive,
            target_root:$target,
            reports:.,
            conversion_epochs:[4,6,8],
            excluded_epochs:[2]
        }
    ' "$mf_rows" >"$mf_handoff/cohort/cohort.json"
    jq -n --arg archive "$mf_archive" --arg target "$mf_target" \
        --arg state "$mf_handoff" '
        {
            schema_version:1,
            kind:"archive-v2-pre-to-post-manual-handoff-request",
            archive_root:$archive,
            target_root:$target,
            state_root:$state,
            cluster_id:"mainnet-beta",
            run_id:"test-run",
            excluded_epochs:[2]
        }
    ' >"$mf_handoff/request.json"
    rm "$mf_rows"
}

run_fixture() {
    rf_root=$1
    PATH="$test_root:$PATH" FAKE_CONTROL_ROOT="$rf_root/control" \
        "$runner_shell" "$runner" "$rf_root/handoff" 4 "$fake_converter" \
            test-maintenance-authority
}

# Exact coverage and deterministic partition.
basic=$test_root/basic
make_fixture "$basic"
run_fixture "$basic" >/dev/null || fail 'basic run failed'
for epoch in 2 4 6 8; do
    [ -d "$basic/archive/.epoch-$epoch.pre-to-post.backup" ] \
        && [ -f "$basic/archive/epoch-$epoch/archive-v2-pre-to-post.candidate.v1.json" ] \
        && [ -f "$basic/handoff/fast-in-place-candidate/results/epoch-$epoch.json" ] \
        || fail "basic result is absent for epoch $epoch"
    [ -f "$basic/archive/.epoch-$epoch.pre-to-post.backup/disabled/block-time-gaps.bin" ] \
        && [ -f "$basic/archive/epoch-$epoch/registry.bin" ] \
        || fail "backup/retained inventory is wrong for epoch $epoch"
done
for stale in \
    block-time-gaps.bin .block-time-gaps.bin.lock \
    .hivezilla-pipeline-owned.v1.json \
    .complete-hot-v2-no-access-delete-car \
    .complete-hot-v2-shredding-sidecar-v2 \
    .archive-v2-manifest-publish.lock \
    archive-v2-generation.json archive-v2-registry-reprocess.receipt.json \
    archive-v2-pre-to-post.receipt.json \
    archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker \
    archive-v2-get-block.index \
    archive-v2-block-access.index.pre-votehash-20260523T205501+0200 \
    archive-v2-get-block.index.pre-votehash-20260523T205501+0200 \
    archive-v2-blocks.wincode archive-v2-blocks.wincode.zst; do
    [ ! -e "$basic/archive/epoch-2/$stale" ] \
        && [ -f "$basic/archive/.epoch-2.pre-to-post.backup/disabled/$stale" ] \
        || fail "stale-object custody is wrong for $stale"
done
jq -e '.worker == 0' "$basic/handoff/fast-in-place-candidate/claims/epoch-2.json" >/dev/null \
    && jq -e '.worker == 1' "$basic/handoff/fast-in-place-candidate/claims/epoch-4.json" >/dev/null \
    && jq -e '.worker == 0' "$basic/handoff/fast-in-place-candidate/claims/epoch-6.json" >/dev/null \
    && jq -e '.worker == 1' "$basic/handoff/fast-in-place-candidate/claims/epoch-8.json" >/dev/null \
    || fail 'worker partition is not deterministic'
jq -e '.epoch_count == 4 and .completed_epochs == 4 and
    .backups_retained == true and .canonical_publication == false and
    .canonical_manifest_deferred == true and .target_post_audit_deferred == true' \
    "$basic/handoff/fast-in-place-candidate/batch-complete.json" >/dev/null \
    || fail 'non-canonical batch-complete record is invalid'

# A production-style state path can start with the archive-root text
# (`archive-v2-...` versus `archive`). The process guard must not classify its
# own process-table helper as an unrelated archive reader.
prefix_state=$test_root/prefix-state
make_fixture "$prefix_state"
mv "$prefix_state/handoff" "$prefix_state/archive-state"
jq --arg state "$prefix_state/archive-state" '.state_root = $state' \
    "$prefix_state/archive-state/request.json" \
    >"$prefix_state/archive-state/request.json.updated"
mv "$prefix_state/archive-state/request.json.updated" \
    "$prefix_state/archive-state/request.json"
PATH="$test_root:$PATH" \
    FAKE_PS_INHERITED_ARGV="$prefix_state/archive-state" \
    FAKE_CONTROL_ROOT="$prefix_state/control" \
    "$runner_shell" "$runner" "$prefix_state/archive-state" 4 "$fake_converter" \
        test-maintenance-authority >/dev/null \
    || fail 'archive-prefix state-root run failed'
[ -f "$prefix_state/archive-state/fast-in-place-candidate/batch-complete.json" ] \
    || fail 'archive-prefix state-root completion is absent'

# A duplicate cohort epoch cannot create a duplicate claim.
duplicate=$test_root/duplicate
make_fixture "$duplicate"
duplicate_cohort=$duplicate/handoff/cohort/cohort.json
jq '.reports += [.reports[0]]' "$duplicate_cohort" >"$duplicate_cohort.changed"
mv "$duplicate_cohort.changed" "$duplicate_cohort"
if run_fixture "$duplicate" >/dev/null 2>&1; then
    fail 'duplicate cohort epoch was accepted'
fi
[ ! -e "$duplicate/control/entered-2" ] \
    || fail 'duplicate cohort started a converter before rejection'

# Symlinked authority and recovery paths are rejected before conversion.
lock_symlink=$test_root/lock-symlink
make_fixture "$lock_symlink"
ln -s "$lock_symlink/control" \
    "$lock_symlink/archive/.archive-v2-pre-to-post-fast-candidate.lock"
if run_fixture "$lock_symlink" >/dev/null 2>&1; then
    fail 'symlinked archive-root lock was accepted'
fi
[ -L "$lock_symlink/archive/.archive-v2-pre-to-post-fast-candidate.lock" ] \
    && [ ! -e "$lock_symlink/control/entered-2" ] \
    || fail 'symlinked lock rejection changed its target or started work'

backup_symlink=$test_root/backup-symlink
make_fixture "$backup_symlink"
ln -s "$backup_symlink/control" \
    "$backup_symlink/archive/.epoch-2.pre-to-post.backup"
if run_fixture "$backup_symlink" >/dev/null 2>&1; then
    fail 'symlinked recovery backup was accepted'
fi
[ ! -e "$backup_symlink/control/entered-2" ] \
    || fail 'symlinked recovery backup reached the converter'

# One failure stops later starts, while the sibling in the same round finishes.
resume=$test_root/resume
make_fixture "$resume"
: >"$resume/control/fail-2"
if run_fixture "$resume" >/dev/null 2>&1; then
    fail 'worker failure unexpectedly succeeded'
fi
[ -f "$resume/control/entered-2" ] && [ -f "$resume/control/entered-4" ] \
    && [ -d "$resume/archive/.epoch-4.pre-to-post.backup" ] \
    && [ ! -e "$resume/control/entered-6" ] && [ ! -e "$resume/control/entered-8" ] \
    || fail 'failure did not stop new starts after the sibling finished'
# The private admitted copy, not the mutable source report path, controls the
# resumed conversion.
printf '{}\n' >"$resume/reports/epoch-2.json"
run_fixture "$resume" >/dev/null || fail 'exact-claim resume failed'
[ -f "$resume/handoff/fast-in-place-candidate/batch-complete.json" ] \
    || fail 'resume did not complete exact coverage'

# Recovery after the pair was switched but before a report was returned.
recovery=$test_root/recovery
make_fixture "$recovery"
: >"$recovery/control/fail-after-swap-2"
if run_fixture "$recovery" >/dev/null 2>&1; then
    fail 'post-swap failure unexpectedly succeeded'
fi
[ -d "$recovery/archive/.epoch-2.pre-to-post.backup" ] \
    && [ ! -f "$recovery/handoff/fast-in-place-candidate/results/epoch-2.json" ] \
    || fail 'post-swap failure fixture did not reach its recovery state'
run_fixture "$recovery" >/dev/null || fail 'journal-style recovery run failed'

# Missing moved custody and changed durable journal records are rejected.
moved_tamper=$test_root/moved-tamper
make_fixture "$moved_tamper"
run_fixture "$moved_tamper" >/dev/null || fail 'moved-tamper setup failed'
mv "$moved_tamper/archive/.epoch-2.pre-to-post.backup/disabled/block-time-gaps.bin" \
    "$moved_tamper/archive/.epoch-2.pre-to-post.backup/disabled/block-time-gaps.bin.changed"
if run_fixture "$moved_tamper" >/dev/null 2>&1; then
    fail 'missing moved stale object was accepted'
fi

journal_tamper=$test_root/journal-tamper
make_fixture "$journal_tamper"
run_fixture "$journal_tamper" >/dev/null || fail 'journal-tamper setup failed'
complete=$journal_tamper/archive/.epoch-2.pre-to-post.backup/archive-v2-pre-to-post.switch-complete.v1.json
chmod 600 "$complete"
jq '.intent_sha256 = ("0" * 64)' "$complete" >"$complete.changed"
mv "$complete.changed" "$complete"
if run_fixture "$journal_tamper" >/dev/null 2>&1; then
    fail 'changed pair-swap completion record was accepted'
fi

# A changed immutable claim is rejected.
claim_tamper=$test_root/claim-tamper
make_fixture "$claim_tamper"
: >"$claim_tamper/control/fail-2"
run_fixture "$claim_tamper" >/dev/null 2>&1 && fail 'claim setup failure succeeded'
claim=$claim_tamper/handoff/fast-in-place-candidate/claims/epoch-2.json
chmod 600 "$claim"
jq '.worker = 1' "$claim" >"$claim.changed"
mv "$claim.changed" "$claim"
if run_fixture "$claim_tamper" >/dev/null 2>&1; then
    fail 'changed claim was accepted'
fi

# A changed simple result is rejected without hashing archive payloads.
result_tamper=$basic/handoff/fast-in-place-candidate/results/epoch-2.json
chmod 600 "$result_tamper"
jq '.worker = 1' "$result_tamper" >"$result_tamper.changed"
mv "$result_tamper.changed" "$result_tamper"
if run_fixture "$basic" >/dev/null 2>&1; then
    fail 'changed result was accepted'
fi

# A second coordinator cannot enter, and TERM reaps both converter children.
signal_root=$test_root/signal
make_fixture "$signal_root"
: >"$signal_root/control/sleep-all"
PATH="$test_root:$PATH" FAKE_CONTROL_ROOT="$signal_root/control" \
    "$runner_shell" "$runner" "$signal_root/handoff" 4 "$fake_converter" \
        test-maintenance-authority >/dev/null 2>&1 &
signal_pid=$!
signal_ready=0
for unused in 1 2 3 4 5 6 7 8 9 10; do
    if [ -f "$signal_root/control/entered-2" ] && [ -f "$signal_root/control/entered-4" ]; then
        signal_ready=1
        break
    fi
    sleep 1
done
[ "$signal_ready" -eq 1 ] || fail 'signal workers did not start'
if run_fixture "$signal_root" >/dev/null 2>&1; then
    fail 'duplicate archive-root coordinator was accepted'
fi
kill -TERM "$signal_pid"
if wait "$signal_pid"; then
    fail 'signalled coordinator exited successfully'
fi
[ -f "$signal_root/control/terminated-2" ] \
    && [ -f "$signal_root/control/terminated-4" ] \
    && [ ! -e "$signal_root/archive/.archive-v2-pre-to-post-fast-candidate.lock" ] \
    || fail 'signal did not reap workers and release the root lock'

echo 'fast in-place coordinator self-test passed'
