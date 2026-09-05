#!/bin/sh
# Build and sample the one corrected epoch-900 Standalone V3 candidate.
# This fixed NAS runner does not publish or remove data. A failed run keeps
# its output and evidence for review.

set -eu
umask 077
LC_ALL=C
export LC_ALL

SOURCE=/volume1/blockzilla/archive-metadata-normalization/staging/epoch-900-current-typed-errors-v1-20260828T124710CEST
OUTPUT_PARENT=/volume1/blockzilla/index-archive-trial/foundation-optimized-split-v3-current-r1
OUTPUT=$OUTPUT_PARENT/epoch-900-full-2g-r2
STAGING=$OUTPUT_PARENT/.epoch-900-full-2g-r2.account-projection-staging
CAPACITY_ROOT=/volume1/blockzilla/index-archive-trial
CONVERTER=/volume1/blockzilla/scheduler-state/index-archive-bin/archive-v3-split-convert-2g-r2
READER=/volume1/blockzilla/scheduler-state/index-archive-bin/archive-v3-read-demo-network-r1
CONVERTER_BYTES=2552512
READER_BYTES=5805056
SPYX_PID=252572
WORKERS=12
MIN_LOGICAL_CPUS=12
MIN_MEM_AVAILABLE_KIB=3145728
MIN_FREE_BYTES=500000000000
MANIFEST=$SOURCE/archive-v2-generation.json
MANIFEST_BYTES=2408
MANIFEST_SHA256=afdfcc981f0dd182b21ff4e983e86700b4ae9b3188b1e567c9b2857ac1d50f67
GENERATION_DIGEST=5145e2e843f7e76d5de41e6433d7658d9dae02a50604c81a1373858195523097
MESSAGE_MARKER=$SOURCE/archive-v2-message-schema-post-unknown-fallbacks-v1.marker
MESSAGE_MARKER_BYTES=77
MESSAGE_MARKER_SHA256=c870c4b0940b05b7bd18a134fba496c5c376f539ef7668f137112526d5c61edd
METADATA_MARKER=$SOURCE/archive-v2-metadata-schema-current-typed-errors-v1.marker
METADATA_MARKER_BYTES=62
METADATA_MARKER_SHA256=f49c05f2021856a66542da4b84e31b2567b653a113acd05e0a0791cd620f0305
RUN_ID=epoch-900-full-2g-r2-current-typed-errors-v1
RUNS_DIR=$OUTPUT_PARENT/.runs
EVIDENCE=$RUNS_DIR/$RUN_ID
LOCK_FILE=$OUTPUT_PARENT/.epoch-900-full-2g-r2.lock

child_pid=

die() {
    trap - 1 2 3 15
    if [ -n "$child_pid" ] && kill -0 "$child_pid" 2>/dev/null; then
        kill -TERM "$child_pid" 2>/dev/null || :
        wait "$child_pid" 2>/dev/null || :
        echo "epoch-900 corrected V3: stopped only converter PID $child_pid; output and evidence were kept" >&2
    fi
    child_pid=
    echo "epoch-900 corrected V3: $*" >&2
    exit 1
}

usage() {
    echo "usage: $0 [--self-test]" >&2
    exit 2
}

on_signal() {
    os_status=$1
    trap - 1 2 3 15
    if [ -n "$child_pid" ] && kill -0 "$child_pid" 2>/dev/null; then
        kill -TERM "$child_pid" 2>/dev/null || :
        wait "$child_pid" 2>/dev/null || :
    fi
    echo "epoch-900 corrected V3: interrupted; output and evidence were kept" >&2
    exit "$os_status"
}

trap 'on_signal 129' 1
trap 'on_signal 130' 2
trap 'on_signal 131' 3
trap 'on_signal 143' 15

require_command() {
    command -v "$1" >/dev/null 2>&1 \
        || die "required command is not available: $1"
}

require_real_file_size() {
    rrfs_path=$1
    rrfs_size=$2
    rrfs_kind=$3
    [ -f "$rrfs_path" ] && [ ! -L "$rrfs_path" ] \
        || die "$rrfs_kind is not one real file: $rrfs_path"
    rrfs_actual=$(stat -c '%s' "$rrfs_path") \
        || die "cannot read $rrfs_kind size: $rrfs_path"
    [ "$rrfs_actual" = "$rrfs_size" ] \
        || die "$rrfs_kind size is $rrfs_actual bytes, expected $rrfs_size: $rrfs_path"
}

require_real_executable_size() {
    rres_path=$1
    rres_size=$2
    rres_kind=$3
    require_real_file_size "$rres_path" "$rres_size" "$rres_kind"
    [ -x "$rres_path" ] || die "$rres_kind is not executable: $rres_path"
    rres_parent=$(CDPATH= cd -P "$(dirname "$rres_path")" 2>/dev/null && pwd -P) \
        || die "cannot canonicalize $rres_kind parent"
    [ "$rres_parent/$(basename "$rres_path")" = "$rres_path" ] \
        || die "$rres_kind path is not canonical: $rres_path"
}

sha256_file() {
    sf_output=$(sha256sum "$1") || return 1
    printf '%s\n' "$sf_output" | awk '
        NR == 1 && length($1) == 64 && $1 !~ /[^0-9a-f]/ {
            print $1
            valid = 1
        }
        END { if (!valid || NR != 1) exit 1 }
    '
}

require_small_control() {
    rsc_path=$1
    rsc_size=$2
    rsc_sha=$3
    rsc_kind=$4
    require_real_file_size "$rsc_path" "$rsc_size" "$rsc_kind"
    rsc_actual_sha=$(sha256_file "$rsc_path") \
        || die "cannot hash $rsc_kind: $rsc_path"
    [ "$rsc_actual_sha" = "$rsc_sha" ] \
        || die "$rsc_kind SHA-256 does not match the sealed value: $rsc_path"
}

require_source_payload() {
    rsp_name=$1
    rsp_size=$2
    require_real_file_size "$SOURCE/$rsp_name" "$rsp_size" "sealed Compact object $rsp_name"
}

validate_manifest_json() {
    jq -e \
        --arg generation_digest "$GENERATION_DIGEST" '
        type == "object" and
        (keys | sort) == [
            "cluster_id", "complete", "epoch", "files",
            "generation_digest", "generation_id", "schema_version",
            "slots_per_epoch"
        ] and
        .schema_version == 1 and
        .cluster_id == "mainnet-beta" and
        .epoch == 900 and
        .generation_id == "epoch-900-current-typed-errors-v1-20260828T124710CEST" and
        .generation_digest == $generation_digest and
        .slots_per_epoch == 432000 and
        .complete == true and
        (.files | type) == "array" and
        (.files | length) == 13 and
        (all(.files[]; (keys | sort) == ["name", "sha256", "size"])) and
        (.files | sort_by(.name)) == [
            {"name":"archive-v2-blocks.index","size":22456652,"sha256":"4488c7cf2750c0a7fe11ae798b9d960ed72a0a9e904aa336a30c35198697d55d"},
            {"name":"archive-v2-blocks.zstd","size":59899113036,"sha256":"670b9261536eea30bf6a4ecdc521660fbc430216efaca5950569d62d9e8d915d"},
            {"name":"archive-v2-message-schema-post-unknown-fallbacks-v1.marker","size":77,"sha256":"c870c4b0940b05b7bd18a134fba496c5c376f539ef7668f137112526d5c61edd"},
            {"name":"archive-v2-meta.wincode","size":66,"sha256":"cf8015b66e8e85e132ec59caa004fcc0b5718f698d3fccb46723675ef6d59226"},
            {"name":"archive-v2-metadata-schema-current-typed-errors-v1.marker","size":62,"sha256":"f49c05f2021856a66542da4b84e31b2567b653a113acd05e0a0791cd620f0305"},
            {"name":"blockhash_registry.bin","size":13819456,"sha256":"78b5eeb27bf24eb6417c05ff813e21bd09e97c65384f45991f4a7d2d07a0483b"},
            {"name":"poh.wincode","size":9681441209,"sha256":"5ca21192fd38dfb47e6320790cfc98b0a5b6435aac8ec01e259d65c227da6d71"},
            {"name":"prev_blockhash_tail.bin","size":12000,"sha256":"fefc60774cbef04470aa9082f3d6eb77984898b7750daaf59c590d2a30a59dbd"},
            {"name":"registry.bin","size":889551808,"sha256":"d14bfd8e7c174eb59ce6559cbc35508c9c5ef5bfc6c3a27fde2b5201c0e179fc"},
            {"name":"registry.mphf","size":341082690,"sha256":"2bce73b90314548064183f17e8dc6c33efa9bcff5266f649377a559596778097"},
            {"name":"shredding.wincode","size":792857572,"sha256":"db2d09601597932f9ca4ba82924ad10ba6cd3433c40a42923aabd3cd8681a7f2"},
            {"name":"signatures.bin","size":32380385536,"sha256":"e2372e2b8f8277b222b828e10fc26541799c51db9f49d2fb152c4e3e26614adc"},
            {"name":"vote_hash_registry.bin","size":28070770,"sha256":"88dcfa6856bd0c24b602b4ac185bb28287490c4dcdbd4bab9aa90e23b505f5ae"}
        ] and
        ([.files[].size] | add) == 104048790934
    ' "$MANIFEST" >/dev/null \
        || die "sealed Compact generation manifest fields do not match the fixed inventory"
}

validate_source() {
    [ -d "$SOURCE" ] && [ ! -L "$SOURCE" ] \
        || die "corrected Compact source is not one real directory: $SOURCE"
    vs_source=$(CDPATH= cd -P "$SOURCE" 2>/dev/null && pwd -P) \
        || die "cannot canonicalize corrected Compact source"
    [ "$vs_source" = "$SOURCE" ] \
        || die "corrected Compact source path is not canonical: $vs_source"

    require_small_control "$MESSAGE_MARKER" "$MESSAGE_MARKER_BYTES" \
        "$MESSAGE_MARKER_SHA256" "sealed message-schema marker"
    require_small_control "$METADATA_MARKER" "$METADATA_MARKER_BYTES" \
        "$METADATA_MARKER_SHA256" "sealed metadata-schema marker"
    require_small_control "$MANIFEST" "$MANIFEST_BYTES" "$MANIFEST_SHA256" \
        "sealed Compact generation manifest"
    validate_manifest_json

    # The gateway sealing audit is the authority for payload hashes. Recheck
    # only the exact file types and sizes that the sealed manifest declares.
    require_source_payload archive-v2-blocks.zstd 59899113036
    require_source_payload archive-v2-blocks.index 22456652
    require_source_payload archive-v2-meta.wincode 66
    require_source_payload registry.bin 889551808
    require_source_payload registry.mphf 341082690
    require_source_payload signatures.bin 32380385536
    require_source_payload poh.wincode 9681441209
    require_source_payload shredding.wincode 792857572
    require_source_payload vote_hash_registry.bin 28070770
    require_source_payload blockhash_registry.bin 13819456
    require_source_payload prev_blockhash_tail.bin 12000
}

snapshot_source() {
    ss_output=$1
    : >"$ss_output"
    for ss_name in \
        archive-v2-blocks.zstd \
        archive-v2-blocks.index \
        archive-v2-meta.wincode \
        registry.bin \
        registry.mphf \
        signatures.bin \
        poh.wincode \
        shredding.wincode \
        vote_hash_registry.bin \
        blockhash_registry.bin \
        prev_blockhash_tail.bin \
        archive-v2-message-schema-post-unknown-fallbacks-v1.marker \
        archive-v2-metadata-schema-current-typed-errors-v1.marker \
        archive-v2-generation.json; do
        find "$SOURCE/$ss_name" -maxdepth 0 \
            -printf '%f\t%y\t%s\t%T@\t%C@\n' >>"$ss_output" \
            || die "cannot snapshot corrected Compact object: $ss_name"
    done
}

count_v3_converters() {
    cvc_count=0
    for cvc_proc in /proc/[0-9]*; do
        [ -d "$cvc_proc" ] || continue
        cvc_exe=$(readlink "$cvc_proc/exe" 2>/dev/null || :)
        case "$cvc_exe" in
            */archive-v3-split-convert-*) cvc_count=$((cvc_count + 1)) ;;
        esac
    done
    printf '%s\n' "$cvc_count"
}

process_state() {
    ps_path=/proc/$1/stat
    [ -r "$ps_path" ] || return 1
    awk 'NR == 1 { print $3; found = 1 } END { if (!found) exit 1 }' "$ps_path"
}

process_is_running() {
    pir_state=$(process_state "$1" 2>/dev/null) || return 1
    [ "$pir_state" != Z ]
}

wait_for_spyx() {
    wfs_announced=0
    while [ -e "/proc/$SPYX_PID" ]; do
        if [ "$wfs_announced" -eq 0 ]; then
            echo "epoch-900 corrected V3: waiting for SPYX PID $SPYX_PID to exit" >&2
            wfs_announced=1
        fi
        sleep 30
    done
    if [ "$wfs_announced" -eq 1 ]; then
        echo "epoch-900 corrected V3: SPYX PID $SPYX_PID is gone" >&2
    fi
}

validate_resource_gates() {
    vrg_cpus=$(nproc) || die "cannot read available logical CPU count"
    case "$vrg_cpus" in
        ''|*[!0-9]*) die "available logical CPU count is invalid: $vrg_cpus" ;;
    esac
    [ "$vrg_cpus" -ge "$MIN_LOGICAL_CPUS" ] \
        || die "only $vrg_cpus logical CPUs are available; at least $MIN_LOGICAL_CPUS are required"

    vrg_mem=$(awk '$1 == "MemAvailable:" { print $2; found = 1 } END { if (!found) exit 1 }' \
        /proc/meminfo) || die "cannot read MemAvailable from /proc/meminfo"
    case "$vrg_mem" in
        ''|*[!0-9]*) die "MemAvailable is invalid: $vrg_mem" ;;
    esac
    [ "$vrg_mem" -ge "$MIN_MEM_AVAILABLE_KIB" ] \
        || die "MemAvailable is $vrg_mem KiB; at least $MIN_MEM_AVAILABLE_KIB KiB is required"

    vrg_free=$(df -PB1 "$CAPACITY_ROOT" | awk 'NR == 2 { print $4; found = 1 } END { if (!found) exit 1 }') \
        || die "cannot read free bytes for $CAPACITY_ROOT"
    case "$vrg_free" in
        ''|*[!0-9]*) die "free-space result is invalid: $vrg_free" ;;
    esac
    [ "$vrg_free" -ge "$MIN_FREE_BYTES" ] \
        || die "free space is $vrg_free bytes; at least $MIN_FREE_BYTES bytes is required"

    AVAILABLE_CPUS=$vrg_cpus
    AVAILABLE_MEM_KIB=$vrg_mem
    AVAILABLE_BYTES=$vrg_free
}

write_expected_output_inventory() {
    cat >"$1" <<'EOF'
archive-v2-retained-sidecars.candidate.json
archive-v2-standalone-account-postings-adaptive-v3.control
archive-v2-standalone-account-postings-adaptive-v3.coverage
archive-v2-standalone-account-postings-adaptive-v3.pages
archive-v2-standalone-account-postings-adaptive-v3.report.json
archive-v2-standalone-balances.wincode
archive-v2-standalone-block-rewards.wincode
archive-v2-standalone-blocks.index
archive-v2-standalone-inner-instructions.wincode
archive-v2-standalone-loaded-addresses.wincode
archive-v2-standalone-logs.wincode
archive-v2-standalone-messages.wincode
archive-v2-standalone-outcomes.wincode
archive-v2-standalone-raw-metadata-fallbacks.wincode
archive-v2-standalone-token-balances.wincode
archive-v2-standalone-transaction-directory.wincode
archive-v2-standalone-transaction-rewards.wincode
benchmark-report.json
EOF
}

validate_output_inventory() {
    voi_non_files=$(find "$OUTPUT" -mindepth 1 -maxdepth 1 ! -type f -print \
        | wc -l | tr -d '[:space:]')
    [ "$voi_non_files" = 0 ] \
        || die "V3 output contains a directory, symlink, or non-regular object"
    find "$OUTPUT" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' \
        >"$EVIDENCE/output-inventory.unsorted"
    sort "$EVIDENCE/output-inventory.unsorted" >"$EVIDENCE/output-inventory.actual"
    write_expected_output_inventory "$EVIDENCE/output-inventory.expected"
    cmp -s "$EVIDENCE/output-inventory.expected" "$EVIDENCE/output-inventory.actual" \
        || die "V3 output inventory is not the exact 18-file converter inventory"
}

validate_candidate_binding() {
    vcb=$OUTPUT/archive-v2-retained-sidecars.candidate.json
    jq -e '
        type == "object" and
        (keys | sort) == [
            "complete_epoch", "epoch", "message_schema", "metadata_schema",
            "objects", "outer_schema", "schema_version", "selected_blocks",
            "selected_transactions", "slots_per_epoch",
            "source_generation_digest", "status"
        ] and
        .schema_version == 1 and
        .status == "unverified-nonpublishable" and
        .epoch == 900 and
        .slots_per_epoch == 432000 and
        .selected_blocks == 431858 and
        .selected_transactions == 476026811 and
        .complete_epoch == true and
        .outer_schema == "current" and
        .message_schema == "current" and
        .metadata_schema == "current-typed-error" and
        .source_generation_digest == null and
        .objects == [
            {"logical_name":"archive-v2-meta.wincode","role":"source-control","admitted_source_size":66},
            {"logical_name":"blockhash_registry.bin","role":"blockhash-registry","admitted_source_size":13819456},
            {"logical_name":"poh.wincode","role":"poh","admitted_source_size":9681441209},
            {"logical_name":"prev_blockhash_tail.bin","role":"previous-blockhash-tail","admitted_source_size":12000},
            {"logical_name":"registry.bin","role":"pubkey-registry","admitted_source_size":889551808},
            {"logical_name":"registry.mphf","role":"pubkey-registry-index","admitted_source_size":341082690},
            {"logical_name":"shredding.wincode","role":"shredding","admitted_source_size":792857572},
            {"logical_name":"signatures.bin","role":"transaction-signatures","admitted_source_size":32380385536},
            {"logical_name":"vote_hash_registry.bin","role":"vote-hash-registry","admitted_source_size":28070770}
        ] and
        ([.objects[].admitted_source_size] | add) == 44127221107
    ' "$vcb" >/dev/null \
        || die "V3 retained-sidecar candidate binding does not match the fixed source inventory"
}

validate_benchmark_report() {
    vbr=$OUTPUT/benchmark-report.json
    jq -e '
        def whole: type == "number" and . >= 0 and floor == .;
        type == "object" and
        .status == "unverified-nonpublishable" and
        .output_validation == "not-run" and
        .content_hashing == "none" and
        .epoch == 900 and
        .slots_per_epoch == 432000 and
        .message_schema == "current" and
        .metadata_schema == "current-typed-error" and
        .workers == 12 and
        .benchmark_prefix_blocks == null and
        .source_total_blocks == 431858 and
        .selected_blocks == 431858 and
        .transactions == 476026811 and
        .success_transactions == 455804664 and
        .failed_transactions == 20222147 and
        .unknown_transactions == 0 and
        (.success_transactions + .failed_transactions + .unknown_transactions) == .transactions and
        .source_unchanged == true and
        .signature_content_reads == 0 and
        .unrelated_source_content_reads == 0 and
        .account_raw_transaction_fallbacks == 0 and
        .account_raw_metadata_loaded_fallbacks == 0 and
        .cpi_raw_transaction_fallbacks == 0 and
        .cpi_raw_metadata_fallbacks == 0 and
        .source_raw_transaction_fallback_flags == 0 and
        .source_raw_metadata_fallback_flags == 0 and
        .optimized_split_v3.complete_epoch == true and
        .optimized_split_v3.publishable == false and
        .optimized_split_v3.output_validation == "not-run" and
        .optimized_split_v3.forward_projection == "not-created" and
        .optimized_split_v3.legacy_forward_files_created == false and
        .optimized_split_v3.source_block_projection_passes == 1 and
        .optimized_split_v3.adaptive_account_postings.blocks == 431858 and
        .optimized_split_v3.adaptive_account_postings.transactions == 476026811 and
        .optimized_split_v3.adaptive_account_postings.status == "unverified-nonpublishable" and
        .optimized_split_v3.adaptive_account_postings.postings == 3767061774 and
        .optimized_split_v3.adaptive_account_postings.distinct_accounts == 27494765 and
        .optimized_split_v3.adaptive_account_postings.coverage_records == 2901 and
        .optimized_split_v3.adaptive_account_postings.incomplete_account_transactions == 0 and
        .optimized_split_v3.adaptive_account_postings.incomplete_cpi_transactions == 2901 and
        .optimized_split_v3.adaptive_account_postings.sort_memory_bytes == 2147483648 and
        .standalone_ledger.format == "v3-varint-directory" and
        .standalone_ledger.output_reopens == 0 and
        .standalone_ledger.stats.directory_v3.blocks == 431858 and
        .standalone_ledger.stats.directory_v3.source_projection_passes == 431858 and
        .standalone_ledger.stats.directory_v3.varint_delta_checkpoint_blocks == 431858 and
        .standalone_ledger.stats.directory_v3.raw_fallback_records == 0 and
        ([.standalone_ledger.objects[] | select(.object == "raw-metadata-fallbacks")] | length) == 1 and
        ([.standalone_ledger.objects[] | select(.object == "raw-metadata-fallbacks")][0] |
            .decoded_payload_bytes == 0 and .stored_payload_bytes == 0 and .file_bytes == 64) and
        (.required_cloud_upload.required_cloud_bytes | whole) and
        (.required_cloud_upload.standalone_ledger_bytes | whole) and
        (.required_cloud_upload.retained_source_sidecar_bytes | whole) and
        (.required_cloud_upload.account_posting_index_bytes | whole) and
        (.required_cloud_upload.candidate_binding_bytes | whole) and
        .required_cloud_upload.retained_source_sidecar_bytes == 44127221107 and
        .required_cloud_upload.required_cloud_bytes ==
            (.required_cloud_upload.standalone_ledger_bytes +
             .required_cloud_upload.retained_source_sidecar_bytes +
             .required_cloud_upload.account_posting_index_bytes +
             .required_cloud_upload.candidate_binding_bytes) and
        .required_cloud_upload.omitted_forward_pages_bytes == 0 and
        .required_cloud_upload.omitted_forward_index_bytes == 0 and
        .required_cloud_upload.omitted_forward_total_bytes == 0 and
        .required_cloud_upload.final_bundle_must_omit_forward_projection == true
    ' "$vbr" >/dev/null || die "V3 benchmark report failed one fixed full-epoch gate"
}

validate_reader_json() {
    vrj_file=$1
    vrj_filter=$2
    jq -e -s "length == 1 and (.[0] | $vrj_filter)" "$vrj_file" >/dev/null \
        || die "reader sample failed its JSON gate: $(basename "$vrj_file")"
}

self_test() {
    [ "$OUTPUT" = "/volume1/blockzilla/index-archive-trial/foundation-optimized-split-v3-current-r1/epoch-900-full-2g-r2" ] \
        || die "self-test output identity failed"
    [ "$SOURCE" = "/volume1/blockzilla/archive-metadata-normalization/staging/epoch-900-current-typed-errors-v1-20260828T124710CEST" ] \
        || die "self-test source identity failed"
    [ "$WORKERS" -eq 12 ] || die "self-test worker count failed"
    [ "$MIN_LOGICAL_CPUS" -eq 12 ] || die "self-test CPU gate failed"
    [ "$MIN_MEM_AVAILABLE_KIB" -eq 3145728 ] || die "self-test memory gate failed"
    [ "$MIN_FREE_BYTES" -eq 500000000000 ] || die "self-test free-space gate failed"
    [ $((59899113036 + 22456652 + 66 + 889551808 + 341082690 + 32380385536 + 9681441209 + 792857572 + 28070770 + 13819456 + 12000)) -eq 104048790795 ] \
        || die "self-test Compact data-byte total failed"
    [ $((66 + 13819456 + 9681441209 + 12000 + 889551808 + 341082690 + 792857572 + 32380385536 + 28070770)) -eq 44127221107 ] \
        || die "self-test retained-sidecar byte total failed"
    echo "epoch-900 corrected V3 runner self-test: PASS"
}

case "${1:-}" in
    --self-test)
        [ "$#" -eq 1 ] || usage
        self_test
        exit 0
        ;;
    '') [ "$#" -eq 0 ] || usage ;;
    *) usage ;;
esac

for required in awk basename cat cmp date df dirname find flock jq kill mkdir nproc readlink sha256sum sleep sort stat tr wc; do
    require_command "$required"
done
[ -d /proc ] || die "/proc is required"
[ -r /proc/meminfo ] || die "/proc/meminfo is not readable"
[ -d "$CAPACITY_ROOT" ] && [ ! -L "$CAPACITY_ROOT" ] \
    || die "capacity root is not one real directory: $CAPACITY_ROOT"
capacity_canonical=$(CDPATH= cd -P "$CAPACITY_ROOT" 2>/dev/null && pwd -P) \
    || die "cannot canonicalize capacity root"
[ "$capacity_canonical" = "$CAPACITY_ROOT" ] \
    || die "capacity root path is not canonical: $capacity_canonical"

require_real_executable_size "$CONVERTER" "$CONVERTER_BYTES" "reviewed V3 converter"
require_real_executable_size "$READER" "$READER_BYTES" "reviewed V3 reader"
validate_source

[ ! -e "$OUTPUT" ] && [ ! -L "$OUTPUT" ] \
    || die "new V3 output already exists; refusing overwrite: $OUTPUT"
[ ! -e "$STAGING" ] && [ ! -L "$STAGING" ] \
    || die "converter staging path already exists; refusing reuse: $STAGING"

if [ ! -e "$OUTPUT_PARENT" ] && [ ! -L "$OUTPUT_PARENT" ]; then
    mkdir "$OUTPUT_PARENT" || die "cannot create fixed V3 output parent"
fi
[ -d "$OUTPUT_PARENT" ] && [ ! -L "$OUTPUT_PARENT" ] \
    || die "V3 output parent is not one real directory: $OUTPUT_PARENT"
output_parent_canonical=$(CDPATH= cd -P "$OUTPUT_PARENT" 2>/dev/null && pwd -P) \
    || die "cannot canonicalize V3 output parent"
[ "$output_parent_canonical" = "$OUTPUT_PARENT" ] \
    || die "V3 output parent path is not canonical: $output_parent_canonical"

[ ! -L "$LOCK_FILE" ] || die "runner lock is a symbolic link: $LOCK_FILE"
exec 9>>"$LOCK_FILE" || die "cannot open runner lock: $LOCK_FILE"
flock -n 9 || die "another corrected epoch-900 V3 runner holds $LOCK_FILE"
[ ! -e "$OUTPUT" ] && [ ! -L "$OUTPUT" ] \
    || die "new V3 output appeared while the runner acquired its lock"
[ ! -e "$STAGING" ] && [ ! -L "$STAGING" ] \
    || die "converter staging appeared while the runner acquired its lock"

wait_for_spyx
validate_resource_gates
[ "$(count_v3_converters)" -eq 0 ] \
    || die "another V3 converter is active; exactly zero are allowed before launch"
validate_source
[ ! -e "$OUTPUT" ] && [ ! -L "$OUTPUT" ] \
    || die "new V3 output appeared before launch"
[ ! -e "$STAGING" ] && [ ! -L "$STAGING" ] \
    || die "converter staging appeared before launch"

if [ ! -e "$RUNS_DIR" ] && [ ! -L "$RUNS_DIR" ]; then
    mkdir "$RUNS_DIR" || die "cannot create run-evidence parent"
fi
[ -d "$RUNS_DIR" ] && [ ! -L "$RUNS_DIR" ] \
    || die "run-evidence parent is not one real directory: $RUNS_DIR"
[ ! -e "$EVIDENCE" ] && [ ! -L "$EVIDENCE" ] \
    || die "fixed run-evidence directory already exists; refusing overwrite: $EVIDENCE"
mkdir "$EVIDENCE" || die "cannot create run-evidence directory"

date -u '+%Y-%m-%dT%H:%M:%SZ' >"$EVIDENCE/started-at-utc.txt"
snapshot_source "$EVIDENCE/source-before.tsv"
jq -n \
    --arg source "$SOURCE" \
    --arg output "$OUTPUT" \
    --arg converter "$CONVERTER" \
    --arg reader "$READER" \
    --arg generation_digest "$GENERATION_DIGEST" \
    --argjson spyx_pid "$SPYX_PID" \
    --argjson workers "$WORKERS" \
    --argjson logical_cpus "$AVAILABLE_CPUS" \
    --argjson mem_available_kib "$AVAILABLE_MEM_KIB" \
    --argjson free_bytes "$AVAILABLE_BYTES" '
    {
        schema_version: 1,
        kind: "epoch-900-corrected-v3-preflight",
        source: $source,
        source_generation_digest: $generation_digest,
        output: $output,
        converter: $converter,
        reader: $reader,
        waited_for_spyx_pid: $spyx_pid,
        workers: $workers,
        resource_gates: {
            logical_cpus_available: $logical_cpus,
            minimum_logical_cpus: 12,
            mem_available_kib: $mem_available_kib,
            minimum_mem_available_kib: 3145728,
            free_bytes: $free_bytes,
            minimum_free_bytes: 500000000000
        },
        converter_processes_before_launch: 0,
        output_absent_before_launch: true,
        staging_absent_before_launch: true
    }
' >"$EVIDENCE/preflight.json"

echo "epoch-900 corrected V3: starting the fixed full-epoch conversion" >&2
"$CONVERTER" "$SOURCE" "$OUTPUT" \
    --epoch 900 \
    --slots-per-epoch 432000 \
    --message-schema current \
    --metadata-schema current-typed-error \
    --workers "$WORKERS" \
    --optimized-split-v3 \
    >"$EVIDENCE/converter.stdout.json" \
    2>"$EVIDENCE/converter.stderr.log" &
child_pid=$!
printf '%s\n' "$child_pid" >"$EVIDENCE/converter.pid"

launch_checks=0
converter_seen=0
while [ "$launch_checks" -lt 30 ]; do
    if ! process_is_running "$child_pid"; then
        break
    fi
    launched_exe=$(readlink "/proc/$child_pid/exe" 2>/dev/null || :)
    if [ "$launched_exe" = "$CONVERTER" ]; then
        converter_seen=1
        break
    fi
    launch_checks=$((launch_checks + 1))
    sleep 1
done
if [ "$converter_seen" -ne 1 ]; then
    if wait "$child_pid"; then
        converter_status=0
    else
        converter_status=$?
    fi
    child_pid=
    printf '%s\n' "$converter_status" >"$EVIDENCE/converter.exit-status"
    die "converter did not become the exact reviewed process"
fi
launch_converter_count=$(count_v3_converters)
if [ "$launch_converter_count" -ne 1 ] && process_is_running "$child_pid"; then
    die "converter process count is not exactly one after launch"
fi

while process_is_running "$child_pid"; do
    active_converter_count=$(count_v3_converters)
    if [ "$active_converter_count" -ne 1 ]; then
        # The child can exit between the loop condition and the process scan.
        # Collect that normal exit below. Fail only while the child is live.
        if ! process_is_running "$child_pid"; then
            break
        fi
        die "converter process count changed while the build was active"
    fi
    sleep 15
done
if wait "$child_pid"; then
    converter_status=0
else
    converter_status=$?
fi
child_pid=
printf '%s\n' "$converter_status" >"$EVIDENCE/converter.exit-status"
[ "$converter_status" -eq 0 ] || die "converter exited with status $converter_status"
[ "$(count_v3_converters)" -eq 0 ] \
    || die "a V3 converter remains active after the fixed converter exited"
[ ! -s "$EVIDENCE/converter.stderr.log" ] \
    || die "converter wrote unexpected stderr; review $EVIDENCE/converter.stderr.log"
[ -d "$OUTPUT" ] && [ ! -L "$OUTPUT" ] \
    || die "converter did not publish one real output directory"
[ ! -e "$STAGING" ] && [ ! -L "$STAGING" ] \
    || die "converter staging path remains after successful exit"

validate_output_inventory
jq -e -s 'length == 1' "$EVIDENCE/converter.stdout.json" >/dev/null \
    || die "converter stdout is not exactly one JSON document"
jq -S -c . "$EVIDENCE/converter.stdout.json" >"$EVIDENCE/converter.stdout.normalized.json"
jq -S -c . "$OUTPUT/benchmark-report.json" >"$EVIDENCE/benchmark-report.normalized.json"
cmp -s "$EVIDENCE/converter.stdout.normalized.json" "$EVIDENCE/benchmark-report.normalized.json" \
    || die "converter stdout and benchmark-report.json differ"
validate_candidate_binding
validate_benchmark_report

"$READER" "$OUTPUT" transaction --block-id 2 --tx-index 0 \
    >"$EVIDENCE/read-transaction-block-2-tx-0.json" \
    2>"$EVIDENCE/read-transaction-block-2-tx-0.stderr.log" \
    || die "reader transaction sample block 2 transaction 0 failed"
validate_reader_json "$EVIDENCE/read-transaction-block-2-tx-0.json" '
    .candidate_status == "unverified-nonpublishable" and
    .archive_format == "v3-varint-directory" and
    .epoch == 900 and .block.block_id == 2 and .block.tx_count == 1597 and
    .transaction.tx_index == 0'

"$READER" "$OUTPUT" transaction --block-id 2 --tx-index 1151 \
    >"$EVIDENCE/read-transaction-block-2-tx-1151.json" \
    2>"$EVIDENCE/read-transaction-block-2-tx-1151.stderr.log" \
    || die "reader corrected-metadata transaction sample failed"
validate_reader_json "$EVIDENCE/read-transaction-block-2-tx-1151.json" '
    .candidate_status == "unverified-nonpublishable" and
    .archive_format == "v3-varint-directory" and
    .epoch == 900 and .block.block_id == 2 and .transaction.tx_index == 1151 and
    .transaction.metadata.kind == "decoded"'

"$READER" "$OUTPUT" account --account-id 1 --limit 10 \
    >"$EVIDENCE/read-account-1-limit-10.json" \
    2>"$EVIDENCE/read-account-1-limit-10.stderr.log" \
    || die "reader account sample failed"
validate_reader_json "$EVIDENCE/read-account-1-limit-10.json" '
    .candidate_status == "unverified-nonpublishable" and
    .archive_format == "v3-varint-directory" and
    .epoch == 900 and .returned_postings == 10 and .has_more == true and
    .total_postings == null and .incomplete_account_transactions == 0 and
    .incomplete_cpi_transactions == 2901 and (.postings | length) == 10'

"$READER" "$OUTPUT" coverage --limit 10 \
    >"$EVIDENCE/read-coverage-limit-10.json" \
    2>"$EVIDENCE/read-coverage-limit-10.stderr.log" \
    || die "reader coverage sample failed"
validate_reader_json "$EVIDENCE/read-coverage-limit-10.json" '
    .candidate_status == "unverified-nonpublishable" and
    .archive_format == "v3-varint-directory" and
    .epoch == 900 and .total_records == 2901 and .returned_records == 10 and
    .truncated == true'

validate_source
snapshot_source "$EVIDENCE/source-after.tsv"
cmp -s "$EVIDENCE/source-before.tsv" "$EVIDENCE/source-after.tsv" \
    || die "a sealed Compact source file changed during the V3 run"
validate_output_inventory
[ ! -e "$STAGING" ] && [ ! -L "$STAGING" ] \
    || die "converter staging path appeared during validation"

date -u '+%Y-%m-%dT%H:%M:%SZ' >"$EVIDENCE/completed-at-utc.txt"
jq -n \
    --arg source "$SOURCE" \
    --arg output "$OUTPUT" \
    --arg evidence "$EVIDENCE" \
    --arg generation_digest "$GENERATION_DIGEST" '
    {
        schema_version: 1,
        kind: "epoch-900-corrected-v3-build-and-sample-complete",
        status: "validated-local-candidate",
        publication_status: "unverified-nonpublishable",
        source: $source,
        source_generation_digest: $generation_digest,
        output: $output,
        evidence: $evidence,
        source_unchanged: true,
        converter_exit_status: 0,
        exact_output_file_count: 18,
        reader_samples_passed: 4,
        ready_for_fixed_r2_inventory: true,
        published: false
    }
' >"$EVIDENCE/complete.json"

echo "epoch-900 corrected V3: PASS; local candidate is ready for the fixed R2 inventory" >&2
echo "epoch-900 corrected V3: output: $OUTPUT" >&2
echo "epoch-900 corrected V3: evidence: $EVIDENCE" >&2
