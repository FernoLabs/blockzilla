#!/bin/sh

set -eu
umask 077

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
builder=$script_dir/build-archive-v2-pre-to-post-cohort.sh

die() {
    echo "cohort builder test: $*" >&2
    exit 1
}

if command -v sha256sum >/dev/null 2>&1; then
    sha256_file() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
    sha256_file() { shasum -a 256 "$1" | awk '{print $1}'; }
else
    die 'sha256sum or shasum is required'
fi

for required in jq awk dd mkdir cp mv rm sh; do
    command -v "$required" >/dev/null 2>&1 || die "missing command: $required"
done
sh -n "$builder" || die 'builder does not pass sh -n'

temporary_root=$(mktemp -d) || die 'cannot create temporary test root'
temporary_root=$(CDPATH= cd -P "$temporary_root" && pwd -P)
cleanup() {
    trap - 0 1 2 3 15
    rm -rf "$temporary_root"
}
trap 'cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

base=$temporary_root/base
rescan=$temporary_root/rescan
archive=$temporary_root/archive
target=$temporary_root/target
outputs=$temporary_root/outputs
mkdir "$base" "$rescan" "$archive" "$target" "$outputs"

make_source() {
    ms_epoch=$1
    ms_dir=$archive/epoch-$ms_epoch
    mkdir "$ms_dir"
    dd if=/dev/zero of="$ms_dir/archive-v2-blocks.index" bs=1 count=88 2>/dev/null
    dd if=/dev/zero of="$ms_dir/archive-v2-blocks.zstd" bs=1 count=4 2>/dev/null
}

make_report() {
    mr_root=$1
    mr_epoch=$2
    mr_classification=$3
    case "$mr_classification" in
        canonical-post)
            mr_action=none
            mr_post=1
            mr_pre=0
            mr_equivalent=0
            mr_post_evidence='{"slot":1,"transaction_index":0}'
            mr_pre_evidence=null
            ;;
        legacy-pre)
            mr_action=convert-to-post
            mr_post=0
            mr_pre=1
            mr_equivalent=0
            mr_post_evidence=null
            mr_pre_evidence='{"slot":1,"transaction_index":0}'
            ;;
        canonical-equivalent)
            mr_action=none
            mr_post=0
            mr_pre=0
            mr_equivalent=1
            mr_post_evidence=null
            mr_pre_evidence=null
            ;;
        *) die "unknown report class: $mr_classification" ;;
    esac
    jq -cn \
        --arg archive "$archive/epoch-$mr_epoch" \
        --argjson epoch "$mr_epoch" \
        --arg classification "$mr_classification" \
        --arg action "$mr_action" \
        --argjson post "$mr_post" \
        --argjson pre "$mr_pre" \
        --argjson equivalent "$mr_equivalent" \
        --argjson post_evidence "$mr_post_evidence" \
        --argjson pre_evidence "$mr_pre_evidence" '
        {
            schema_version: 1,
            kind: "archive-v2-wire-profile-scan",
            archive: $archive,
            epoch: $epoch,
            workers: 8,
            classification: $classification,
            action: $action,
            counts: {
                blocks: 1,
                owned_fallback_blocks: 0,
                compressed_block_bytes: 4,
                uncompressed_block_bytes: 8,
                typed_messages: 1,
                raw_transaction_fallbacks: 0,
                post_only: $post,
                pre_only: $pre,
                both_equivalent: $equivalent,
                both_divergent: 0,
                invalid: 0
            },
            first_evidence: {
                post_only: $post_evidence,
                pre_only: $pre_evidence,
                both_divergent: null,
                invalid: null
            },
            error: null,
            elapsed_seconds: 1,
            completed_unix_seconds: 1
        }
    ' >"$mr_root/epoch-$mr_epoch.json"
}

make_excluded_target() {
    met_epoch=$1
    met_source=$archive/epoch-$met_epoch
    met_target=$target/epoch-$met_epoch
    mkdir "$met_target"
    dd if=/dev/zero of="$met_target/archive-v2-blocks.index" bs=1 count=88 2>/dev/null
    dd if=/dev/zero of="$met_target/archive-v2-blocks.zstd" bs=1 count=4 2>/dev/null
    printf '%s\n' \
        'blockzilla/archive-v2-hot-message-schema/post-unknown-system-unknown-vote/v1' \
        >"$met_target/archive-v2-message-schema-post-unknown-fallbacks-v1.marker"

    met_index_sha=$(sha256_file "$met_target/archive-v2-blocks.index")
    met_blocks_sha=$(sha256_file "$met_target/archive-v2-blocks.zstd")
    jq -n \
        --argjson epoch "$met_epoch" \
        --arg source "$met_source" \
        --arg target "$met_target" \
        --arg index_sha "$met_index_sha" \
        --arg blocks_sha "$met_blocks_sha" '
        {
            schema_version: 1,
            kind: "archive-v2-pre-to-post-receipt",
            epoch: $epoch,
            cluster_id: "test-cluster",
            generation_id: ("test-post-epoch-" + ($epoch | tostring)),
            source: $source,
            source_authority_kind: "linux-kernel-read-leases",
            source_authority_id: ("test-lease-epoch-" + ($epoch | tostring)),
            source_authority_scope:
                "all-reviewed-source-inodes-pinned-and-read-leased-on-one-local-ext4-device",
            source_authority_filesystem: "linux-local-ext4",
            source_authority_device_id: 1,
            target: $target,
            source_profile: "pre-unknown-instruction-fallbacks-v1",
            target_profile: "post-unknown-instruction-fallbacks-v1",
            source_profile_decision: "unique-full-generation-decode",
            codec: "wincode-leb128-current-block+independent-zstd-frames",
            source_audit: {
                blocks: 1,
                typed_messages: 1,
                raw_transaction_fallbacks: 0,
                raw_metadata_fallbacks: 0,
                selected_only: 1,
                both_semantically_equivalent: 0,
                both_semantically_divergent: 0
            },
            source_files: {
                "archive-v2-blocks.index": {bytes: 88, sha256: $index_sha},
                "archive-v2-blocks.zstd": {bytes: 4, sha256: $blocks_sha}
            },
            target_files: {
                "archive-v2-blocks.index": {bytes: 88, sha256: $index_sha},
                "archive-v2-blocks.zstd": {bytes: 4, sha256: $blocks_sha}
            },
            exact_message_length_preserved: true,
            exact_message_delta_proved: true,
            metadata_regions_copied_verbatim: true,
            target_provider_immutability_required: true,
            source_provider_snapshot_required: false,
            source_linux_read_leases_required: true
        }
    ' >"$met_target/archive-v2-pre-to-post.receipt.json"

    met_receipt_sha=$(sha256_file "$met_target/archive-v2-pre-to-post.receipt.json")
    met_receipt_bytes=$(wc -c <"$met_target/archive-v2-pre-to-post.receipt.json" | tr -d '[:space:]')
    met_marker_sha=$(sha256_file \
        "$met_target/archive-v2-message-schema-post-unknown-fallbacks-v1.marker")
    jq -n \
        --argjson epoch "$met_epoch" \
        --arg index_sha "$met_index_sha" \
        --arg blocks_sha "$met_blocks_sha" \
        --arg receipt_sha "$met_receipt_sha" \
        --argjson receipt_bytes "$met_receipt_bytes" \
        --arg marker_sha "$met_marker_sha" '
        {
            schema_version: 1,
            cluster_id: "test-cluster",
            epoch: $epoch,
            generation_id: ("test-post-epoch-" + ($epoch | tostring)),
            generation_digest:
                "0000000000000000000000000000000000000000000000000000000000000000",
            slots_per_epoch: 432000,
            complete: true,
            files: [
                {name: "archive-v2-blocks.index", size: 88, sha256: $index_sha},
                {name: "archive-v2-blocks.zstd", size: 4, sha256: $blocks_sha},
                {
                    name: "archive-v2-pre-to-post.receipt.json",
                    size: $receipt_bytes,
                    sha256: $receipt_sha
                },
                {
                    name: "archive-v2-message-schema-post-unknown-fallbacks-v1.marker",
                    size: 77,
                    sha256: $marker_sha
                }
            ]
        }
    ' >"$met_target/archive-v2-generation.json"
}

epoch=0
while [ "$epoch" -le 4 ]; do
    make_source "$epoch"
    epoch=$((epoch + 1))
done
make_report "$base" 0 canonical-post
make_report "$rescan" 1 legacy-pre
make_report "$rescan" 2 canonical-equivalent
make_report "$base" 3 legacy-pre
make_excluded_target 1
exclude=$temporary_root/exclude.txt
printf '1\n' >"$exclude"

incomplete_output=$outputs/incomplete
if sh "$builder" "$base" "$rescan" "$archive" "$target" \
    "$incomplete_output" 0 4 1 2 "$exclude" >/dev/null 2>&1
then
    die 'incomplete selected range was accepted'
fi
[ ! -e "$incomplete_output" ] || die 'incomplete test published an output directory'

bad_base=$temporary_root/bad-base
cp -R "$base" "$bad_base"
jq '.classification = "canonical-post" | .action = "none"' \
    "$bad_base/epoch-3.json" >"$bad_base/epoch-3.json.bad"
mv "$bad_base/epoch-3.json.bad" "$bad_base/epoch-3.json"
bad_output=$outputs/bad-classification
if sh "$builder" "$bad_base" "$rescan" "$archive" "$target" \
    "$bad_output" 0 3 1 2 "$exclude" >/dev/null 2>&1
then
    die 'classification/count mismatch was accepted'
fi
[ ! -e "$bad_output" ] || die 'classification mismatch test published an output directory'

success_output=$outputs/success
sh "$builder" "$base" "$rescan" "$archive" "$target" \
    "$success_output" 0 3 1 2 "$exclude" >/dev/null
[ "$(wc -l <"$success_output/epochs.txt" | tr -d '[:space:]')" -eq 1 ] \
    || die 'successful cohort has the wrong epoch count'
[ "$(sed -n '1p' "$success_output/epochs.txt")" = 3 ] \
    || die 'successful cohort did not contain only epoch 3'
jq -e \
    --arg base "$base" \
    --arg rescan "$rescan" '
    .schema_version == 1 and
    .kind == "archive-v2-pre-to-post-cohort" and
    .selected_report_count == 4 and
    .selected_report_set_counts == {base: 2, rescan: 2} and
    .classification_totals == {
        canonical_post: 1,
        legacy_pre: 2,
        canonical_equivalent: 1
    } and
    .excluded_epochs == [1] and
    (.excluded_targets | length) == 1 and
    (.excluded_targets[0].source_report_sha256 ==
        ([.reports[] | select(.epoch == 1) | .report_sha256][0])) and
    .conversion_epochs == [3] and
    (.reports | length) == 4 and
    (all(.reports[]; (.report_sha256 | test("^[0-9a-f]{64}$")))) and
    (any(.reports[];
        .epoch == 1 and
        .report_set == "rescan" and
        .report_path == ($rescan + "/epoch-1.json"))) and
    (any(.reports[];
        .epoch == 3 and
        .report_set == "base" and
        .report_path == ($base + "/epoch-3.json")))
' "$success_output/cohort.json" >/dev/null \
    || die 'successful merged cohort metadata is wrong'
[ "$(sha256_file "$success_output/epochs.txt")" = \
    "$(jq -r '.epoch_list_sha256' "$success_output/cohort.json")" ] \
    || die 'epoch-list hash binding is wrong'

echo 'cohort builder tests passed'
