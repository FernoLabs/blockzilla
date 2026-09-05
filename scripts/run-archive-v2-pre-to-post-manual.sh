#!/bin/sh
# Convert an exact list of Compact Archive V2 epochs from the proven legacy
# Pre message grammar to the canonical Post grammar. This runner never changes
# a source generation, never removes staging data, and stops at the first
# incomplete or inconsistent epoch.

set -u
umask 077

POST_MARKER='archive-v2-message-schema-post-unknown-fallbacks-v1.marker'
POST_MARKER_BYTES=77
POST_MARKER_SHA256='c870c4b0940b05b7bd18a134fba496c5c376f539ef7668f137112526d5c61edd'
PRE_MARKER='archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker'
MANIFEST_FILE='archive-v2-generation.json'
RECEIPT_FILE='archive-v2-pre-to-post.receipt.json'
PUBLICATION_LOCK_FILE='.archive-v2-manifest-publish.lock'
BLOCKS_FILE='archive-v2-blocks.zstd'
BLOCK_INDEX_FILE='archive-v2-blocks.index'
META_FILE='archive-v2-meta.wincode'
REGISTRY_FILE='registry.bin'
REGISTRY_INDEX_FILE='registry.mphf'
SOURCE_PROFILE='pre-unknown-instruction-fallbacks-v1'
TARGET_PROFILE='post-unknown-instruction-fallbacks-v1'
AUTHORITY_KIND='linux-kernel-read-leases'
AUTHORITY_SCOPE='all-reviewed-source-inodes-pinned-and-read-leased-on-one-local-ext4-device'
AUTHORITY_FILESYSTEM='linux-local-ext4'

die() {
    echo "archive-v2-pre-to-post runner: $*" >&2
    exit 1
}

usage() {
    echo "usage: $0 CONVERTER SOURCE_ROOT TARGET_ROOT STATE_ROOT EPOCH_LIST CLUSTER_ID RUN_ID" >&2
    exit 2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is not available: $1"
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

canonical_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

sha256_file() {
    if [ "$sha256_program" = 'sha256sum' ]; then
        sf_output=$(sha256sum "$1") || return 1
    else
        sf_output=$(shasum -a 256 "$1") || return 1
    fi
    set -- $sf_output
    [ "$#" -ge 1 ] || return 1
    sf_digest=$1
    case "$sf_digest" in
        *[!0-9a-f]*|'') return 1 ;;
    esac
    [ "${#sf_digest}" -eq 64 ] || return 1
    printf '%s\n' "$sf_digest"
}

# Match blockzilla_read_sdk::manifest::compute_generation_digest exactly.
# Python is used only as a binary-safe encoding helper. jq below still applies
# the runner's complete manifest/receipt/report policy.
compute_generation_digest() {
    python3 -c '
import hashlib
import json
import struct
import sys

def reject_duplicates(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key: " + key)
        value[key] = item
    return value

with open(sys.argv[1], "rb") as stream:
    manifest = json.load(stream, object_pairs_hook=reject_duplicates)

def integer(name, value, limit):
    if type(value) is not int or value < 0 or value > limit:
        raise ValueError(name + " is not an unsigned integer in range")
    return value

def text(name, value):
    if type(value) is not str:
        raise ValueError(name + " is not text")
    encoded = value.encode("utf-8")
    integer(name + " byte length", len(encoded), 0xffffffff)
    return encoded

def length_prefixed(value):
    return struct.pack("<I", len(value)) + value

schema = integer("schema_version", manifest["schema_version"], 0xffffffff)
epoch = integer("epoch", manifest["epoch"], 0xffffffffffffffff)
slots = integer("slots_per_epoch", manifest["slots_per_epoch"], 0xffffffffffffffff)
if type(manifest["complete"]) is not bool:
    raise ValueError("complete is not a boolean")
cluster = text("cluster_id", manifest["cluster_id"])
generation = text("generation_id", manifest["generation_id"])
files = manifest["files"]
if type(files) is not list:
    raise ValueError("files is not an array")
integer("file count", len(files), 0xffffffff)

encoded_files = []
for entry in files:
    if type(entry) is not dict:
        raise ValueError("file entry is not an object")
    name = text("file name", entry["name"])
    size = integer("file size", entry["size"], 0xffffffffffffffff)
    digest_text = entry["sha256"]
    if type(digest_text) is not str or len(digest_text) != 64:
        raise ValueError("file digest is not lowercase SHA-256")
    if any(character not in "0123456789abcdef" for character in digest_text):
        raise ValueError("file digest is not lowercase SHA-256")
    encoded_files.append((name, size, bytes.fromhex(digest_text)))
encoded_files.sort(key=lambda item: item[0])

hasher = hashlib.sha256()
hasher.update(b"blockzilla/archive-v2-generation\0")
hasher.update(struct.pack("<I", schema))
hasher.update(length_prefixed(cluster))
hasher.update(struct.pack("<Q", epoch))
hasher.update(length_prefixed(generation))
hasher.update(struct.pack("<Q", slots))
hasher.update(bytes((1 if manifest["complete"] else 0,)))
hasher.update(struct.pack("<I", len(encoded_files)))
for name, size, digest in encoded_files:
    hasher.update(length_prefixed(name))
    hasher.update(struct.pack("<Q", size))
    hasher.update(digest)
print(hasher.hexdigest())
' "$1"
}

# The source and destination must be regular files on the same filesystem.
# ln creates the destination atomically and refuses an existing destination.
publish_file_no_replace() {
    pnr_source=$1
    pnr_target=$2
    [ -f "$pnr_source" ] && [ ! -L "$pnr_source" ] || return 1
    [ ! -e "$pnr_target" ] && [ ! -L "$pnr_target" ] || return 1
    ln -n "$pnr_source" "$pnr_target" || return 1
    rm "$pnr_source" || return 1
}

copy_file_exclusive() {
    cfe_source=$1
    cfe_target=$2
    (
        set -C
        dd if="$cfe_source" bs=1048576 >"$cfe_target" 2>/dev/null
    )
}

validate_epoch_list() {
    awk '
        BEGIN { count = 0 }
        !/^(0|[1-9][0-9]*)$/ {
            printf "invalid epoch-list line %d: %s\n", NR, $0 > "/dev/stderr"
            failed = 1
            next
        }
        length($0) > 14 {
            printf "epoch is too large on line %d: %s\n", NR, $0 > "/dev/stderr"
            failed = 1
            next
        }
        seen[$0]++ {
            printf "duplicate epoch on line %d: %s\n", NR, $0 > "/dev/stderr"
            failed = 1
            next
        }
        { count++ }
        END {
            if (count == 0) {
                print "epoch list is empty" > "/dev/stderr"
                failed = 1
            }
            if (failed) exit 1
            print count
        }
    ' "$1"
}

validate_report_file() (
    vr_report=$1
    vr_epoch=$2
    vr_source=$3
    vr_target=$4
    vr_staging=$5
    vr_generation_id=$6
    vr_lease_id=$7

    [ -f "$vr_report" ] && [ ! -L "$vr_report" ] || exit 1
    vr_lines=$(wc -l <"$vr_report" | tr -d '[:space:]') || exit 1
    [ "$vr_lines" = 1 ] || exit 1

    jq -e -s \
        --arg epoch "$vr_epoch" \
        --arg cluster "$cluster_id" \
        --arg generation "$vr_generation_id" \
        --arg source "$vr_source" \
        --arg target "$vr_target" \
        --arg staging "$vr_staging" \
        --arg lease "$vr_lease_id" \
        --arg source_profile "$SOURCE_PROFILE" \
        --arg target_profile "$TARGET_PROFILE" \
        --arg authority_kind "$AUTHORITY_KIND" \
        --arg authority_scope "$AUTHORITY_SCOPE" \
        --arg authority_filesystem "$AUTHORITY_FILESYSTEM" \
        --arg manifest "$vr_target/$MANIFEST_FILE" \
        --arg receipt "$vr_target/$RECEIPT_FILE" '
        length == 1 and
        (.[0] as $r |
            $r.schema_version == 1 and
            $r.kind == "archive-v2-pre-to-post-migration" and
            ($r.epoch | tostring) == $epoch and
            $r.cluster_id == $cluster and
            $r.generation_id == $generation and
            $r.source == $source and
            $r.target == $target and
            $r.staging == $staging and
            $r.source_authority_kind == $authority_kind and
            $r.source_authority_id == $lease and
            $r.source_authority_scope == $authority_scope and
            $r.source_authority_filesystem == $authority_filesystem and
            ($r.source_authority_device_id | type) == "number" and
            $r.source_authority_device_id >= 0 and
            $r.source_profile == $source_profile and
            $r.target_profile == $target_profile and
            $r.source_profile_decision == "unique-full-generation-decode" and
            $r.source_audit_blocks > 0 and
            $r.source_audit_typed_messages > 0 and
            $r.source_audit_selected_only > 0 and
            $r.source_audit_both_equivalent >= 0 and
            $r.source_audit_both_divergent == 0 and
            $r.source_audit_raw_transaction_fallbacks == 0 and
            $r.source_audit_raw_metadata_fallbacks == 0 and
            ($r.source_audit_selected_only + $r.source_audit_both_equivalent
                == $r.source_audit_typed_messages) and
            $r.rewrite.blocks == $r.source_audit_blocks and
            $r.rewrite.typed_messages == $r.source_audit_typed_messages and
            $r.rewrite.borrowed_current_blocks == $r.rewrite.blocks and
            $r.rewrite.owned_outer_fallbacks == 0 and
            $r.rewrite.raw_transaction_fallbacks == 0 and
            $r.rewrite.source_compressed_bytes > 0 and
            $r.rewrite.target_compressed_bytes > 0 and
            $r.rewrite.uncompressed_bytes > 0 and
            ($r.rewrite.target_blocks_sha256 | test("^[0-9a-f]{64}$")) and
            $r.rewrite.message_input_bytes > 0 and
            $r.rewrite.message_input_bytes == $r.rewrite.message_output_bytes and
            $r.rewrite.metadata_input_bytes == $r.rewrite.metadata_output_bytes and
            $r.rewrite.metadata_regions_byte_identical == true and
            ($r.rewrite.source_instruction_data_tag_counts | type) == "array" and
            ($r.rewrite.source_instruction_data_tag_counts | length) == 9 and
            all($r.rewrite.source_instruction_data_tag_counts[];
                type == "number" and . >= 0 and floor == .) and
            $r.rewrite.source_instruction_data_tag_counts[7] == 0 and
            $r.rewrite.source_instruction_data_tag_counts[8] == 0 and
            (($r.rewrite.source_instruction_data_tag_counts[1:7] | add)
                == $r.rewrite.message_mismatch_bytes) and
            $r.rewrite.message_mismatch_bytes > 0 and
            $r.target_post_audit_passed == true and
            $r.target_manifest == $manifest and
            ($r.target_manifest_digest | test("^[0-9a-f]{64}$")) and
            $r.migration_receipt == $receipt and
            $r.staged_files_read_only == true and
            $r.staged_directory_read_only == true and
            $r.target_provider_immutability_required == true and
            $r.source_provider_snapshot_required == false and
            $r.source_linux_read_leases_required == true
        )
    ' "$vr_report" >/dev/null 2>&1
)

validate_receipt_file() (
    vc_receipt=$1
    vc_report=$2
    vc_epoch=$3
    vc_source=$4
    vc_target=$5
    vc_generation_id=$6
    vc_lease_id=$7

    jq -e -s \
        --slurpfile report "$vc_report" \
        --arg epoch "$vc_epoch" \
        --arg cluster "$cluster_id" \
        --arg generation "$vc_generation_id" \
        --arg source "$vc_source" \
        --arg target "$vc_target" \
        --arg lease "$vc_lease_id" \
        --arg source_profile "$SOURCE_PROFILE" \
        --arg target_profile "$TARGET_PROFILE" \
        --arg authority_kind "$AUTHORITY_KIND" \
        --arg authority_scope "$AUTHORITY_SCOPE" \
        --arg authority_filesystem "$AUTHORITY_FILESYSTEM" \
        --arg blocks_file "$BLOCKS_FILE" \
        --arg block_index_file "$BLOCK_INDEX_FILE" \
        --arg meta_file "$META_FILE" \
        --arg registry_file "$REGISTRY_FILE" \
        --arg registry_index_file "$REGISTRY_INDEX_FILE" '
        length == 1 and ($report | length) == 1 and
        (.[0] as $x | $report[0] as $r |
            $x.schema_version == 1 and
            $x.kind == "archive-v2-pre-to-post-receipt" and
            ($x.epoch | tostring) == $epoch and
            $x.cluster_id == $cluster and
            $x.generation_id == $generation and
            $x.source == $source and
            $x.target == $target and
            $x.source_authority_kind == $authority_kind and
            $x.source_authority_id == $lease and
            $x.source_authority_scope == $authority_scope and
            $x.source_authority_filesystem == $authority_filesystem and
            ($x.source_authority_device_id | type) == "number" and
            $x.source_authority_device_id == $r.source_authority_device_id and
            $x.source_profile == $source_profile and
            $x.target_profile == $target_profile and
            $x.source_profile_decision == "unique-full-generation-decode" and
            $x.codec == "wincode-leb128-current-block+independent-zstd-frames" and
            $x.source_audit.blocks == $r.source_audit_blocks and
            $x.source_audit.typed_messages == $r.source_audit_typed_messages and
            $x.source_audit.raw_transaction_fallbacks == 0 and
            $x.source_audit.raw_metadata_fallbacks == 0 and
            $x.source_audit.selected_only == $r.source_audit_selected_only and
            $x.source_audit.both_semantically_equivalent
                == $r.source_audit_both_equivalent and
            $x.source_audit.both_semantically_divergent == 0 and
            $x.rewrite == $r.rewrite and
            $x.rewrite.owned_outer_fallbacks == 0 and
            $x.rewrite.raw_transaction_fallbacks == 0 and
            $x.rewrite.message_input_bytes == $x.rewrite.message_output_bytes and
            $x.rewrite.metadata_input_bytes == $x.rewrite.metadata_output_bytes and
            $x.rewrite.metadata_regions_byte_identical == true and
            ($x.rewrite.source_instruction_data_tag_counts | type) == "array" and
            ($x.rewrite.source_instruction_data_tag_counts | length) == 9 and
            $x.rewrite.source_instruction_data_tag_counts[7] == 0 and
            $x.rewrite.source_instruction_data_tag_counts[8] == 0 and
            (($x.rewrite.source_instruction_data_tag_counts[1:7] | add)
                == $x.rewrite.message_mismatch_bytes) and
            $x.exact_message_length_preserved == true and
            $x.exact_message_delta_proved == true and
            $x.metadata_regions_copied_verbatim == true and
            $x.target_provider_immutability_required == true and
            $x.source_provider_snapshot_required == false and
            $x.source_linux_read_leases_required == true and
            ($x.source_files | type) == "object" and
            ($x.target_files | type) == "object" and
            ($x.source_files | length) > 0 and
            ($x.target_files | length) > 0 and
            (($x.source_files | keys) == ($x.target_files | keys)) and
            all([$blocks_file, $block_index_file, $meta_file,
                $registry_file, $registry_index_file][];
                $x.target_files[.] | type == "object") and
            $x.source_files[$blocks_file].bytes == $x.rewrite.source_compressed_bytes and
            $x.target_files[$blocks_file].bytes == $x.rewrite.target_compressed_bytes and
            $x.target_files[$blocks_file].sha256 == $x.rewrite.target_blocks_sha256 and
            ($x.source_files[$block_index_file] | type) == "object" and
            ($x.target_files[$block_index_file] | type) == "object" and
            all($x.source_files | to_entries[];
                (.key | test("^[A-Za-z0-9._-]+$")) and
                (.value | keys) == ["bytes", "sha256"] and
                (.value.bytes | type) == "number" and .value.bytes >= 0 and
                (.value.sha256 | test("^[0-9a-f]{64}$"))) and
            all($x.target_files | to_entries[];
                (.key | test("^[A-Za-z0-9._-]+$")) and
                (.value | keys) == ["bytes", "sha256"] and
                (.value.bytes | type) == "number" and .value.bytes >= 0 and
                (.value.sha256 | test("^[0-9a-f]{64}$"))) and
            all($x.source_files | to_entries[];
                . as $source_binding |
                if .key == $blocks_file or .key == $block_index_file then
                    true
                else
                    $source_binding.value == $x.target_files[$source_binding.key]
                end)
        )
    ' "$vc_receipt" >/dev/null 2>&1
)

validate_manifest_file() (
    vm_manifest=$1
    vm_receipt=$2
    vm_report=$3
    vm_epoch=$4
    vm_generation_id=$5
    vm_receipt_bytes=$6
    vm_receipt_sha=$7
    vm_computed_digest=$8

    jq -e -s \
        --slurpfile receipt "$vm_receipt" \
        --slurpfile report "$vm_report" \
        --arg epoch "$vm_epoch" \
        --arg cluster "$cluster_id" \
        --arg generation "$vm_generation_id" \
        --arg receipt_name "$RECEIPT_FILE" \
        --arg post_marker "$POST_MARKER" \
        --arg pre_marker "$PRE_MARKER" \
        --arg post_marker_sha "$POST_MARKER_SHA256" \
        --argjson post_marker_bytes "$POST_MARKER_BYTES" \
        --arg receipt_sha "$vm_receipt_sha" \
        --arg computed_digest "$vm_computed_digest" \
        --argjson receipt_bytes "$vm_receipt_bytes" '
        length == 1 and ($receipt | length) == 1 and ($report | length) == 1 and
        (.[0] as $m | $receipt[0] as $x | $report[0] as $r |
            ($m | keys) == ["cluster_id", "complete", "epoch", "files",
                "generation_digest", "generation_id", "schema_version",
                "slots_per_epoch"] and
            $m.schema_version == 1 and
            ($m.epoch | tostring) == $epoch and
            $m.cluster_id == $cluster and
            $m.generation_id == $generation and
            $m.complete == true and
            ($m.generation_digest | test("^[0-9a-f]{64}$")) and
            $m.generation_digest == $computed_digest and
            $m.generation_digest == $r.target_manifest_digest and
            ($m.slots_per_epoch | type) == "number" and
            $m.slots_per_epoch > 0 and
            ($m.files | type) == "array" and
            all($m.files[];
                (keys == ["name", "sha256", "size"]) and
                (.name | test("^[A-Za-z0-9._-]+$")) and
                (.size | type) == "number" and .size >= 0 and
                (.size | floor) == .size and
                (.sha256 | test("^[0-9a-f]{64}$"))) and
            (($m.files | map(.name) | sort) ==
                ((($x.target_files | keys)
                    + [$receipt_name, $post_marker]) | sort)) and
            any($m.files[];
                .name == $post_marker and
                .size == $post_marker_bytes and
                .sha256 == $post_marker_sha) and
            (all($m.files[]; .name != $pre_marker)) and
            any($m.files[];
                .name == $receipt_name and
                .size == $receipt_bytes and
                .sha256 == $receipt_sha) and
            all($x.target_files | to_entries[];
                . as $binding |
                any($m.files[];
                    .name == $binding.key and
                    .size == $binding.value.bytes and
                    .sha256 == $binding.value.sha256))
        )
    ' "$vm_manifest" >/dev/null 2>&1
)

# The full-hash mode is used only for resume. A fresh converter success already
# hashed and audited the new target before it emitted its report. Both modes
# hash the receipt and the canonical marker and check every other bound size.
validate_target_generation() (
    vt_epoch=$1
    vt_source=$2
    vt_target=$3
    vt_staging=$4
    vt_generation_id=$5
    vt_lease_id=$6
    vt_report=$7
    vt_full_hash=$8

    [ -d "$vt_target" ] && [ ! -L "$vt_target" ] || exit 1
    [ ! -w "$vt_target" ] || exit 1
    [ ! -e "$vt_staging" ] && [ ! -L "$vt_staging" ] || exit 1
    validate_report_file "$vt_report" "$vt_epoch" "$vt_source" "$vt_target" \
        "$vt_staging" "$vt_generation_id" "$vt_lease_id" || exit 1

    vt_manifest=$vt_target/$MANIFEST_FILE
    vt_receipt=$vt_target/$RECEIPT_FILE
    vt_marker=$vt_target/$POST_MARKER
    vt_publication_lock=$vt_target/$PUBLICATION_LOCK_FILE
    for vt_control in "$vt_manifest" "$vt_receipt" "$vt_marker" "$vt_publication_lock"; do
        [ -f "$vt_control" ] && [ ! -L "$vt_control" ] || exit 1
        [ ! -w "$vt_control" ] || exit 1
    done
    vt_publication_lock_bytes=$(wc -c <"$vt_publication_lock" | tr -d '[:space:]') || exit 1
    [ "$vt_publication_lock_bytes" = 0 ] || exit 1

    validate_receipt_file "$vt_receipt" "$vt_report" "$vt_epoch" "$vt_source" \
        "$vt_target" "$vt_generation_id" "$vt_lease_id" || exit 1
    vt_receipt_bytes=$(wc -c <"$vt_receipt" | tr -d '[:space:]') || exit 1
    vt_receipt_sha=$(sha256_file "$vt_receipt") || exit 1
    vt_computed_manifest_digest=$(compute_generation_digest "$vt_manifest") || exit 1
    case "$vt_computed_manifest_digest" in
        *[!0-9a-f]*|'') exit 1 ;;
    esac
    [ "${#vt_computed_manifest_digest}" -eq 64 ] || exit 1
    validate_manifest_file "$vt_manifest" "$vt_receipt" "$vt_report" "$vt_epoch" \
        "$vt_generation_id" "$vt_receipt_bytes" "$vt_receipt_sha" \
        "$vt_computed_manifest_digest" || exit 1

    vt_marker_bytes=$(wc -c <"$vt_marker" | tr -d '[:space:]') || exit 1
    [ "$vt_marker_bytes" = "$POST_MARKER_BYTES" ] || exit 1
    vt_marker_sha=$(sha256_file "$vt_marker") || exit 1
    [ "$vt_marker_sha" = "$POST_MARKER_SHA256" ] || exit 1

    vt_tab=$(printf '\t')
    if ! jq -r '.files[] | [.name, (.size | tostring), .sha256] | @tsv' "$vt_manifest" |
        while IFS="$vt_tab" read -r vt_name vt_expected_bytes vt_expected_sha; do
            vt_path=$vt_target/$vt_name
            [ -f "$vt_path" ] && [ ! -L "$vt_path" ] || exit 1
            [ ! -w "$vt_path" ] || exit 1
            vt_actual_bytes=$(wc -c <"$vt_path" | tr -d '[:space:]') || exit 1
            [ "$vt_actual_bytes" = "$vt_expected_bytes" ] || exit 1
            if [ "$vt_full_hash" = 1 ] \
                || [ "$vt_name" = "$RECEIPT_FILE" ] \
                || [ "$vt_name" = "$POST_MARKER" ]; then
                vt_actual_sha=$(sha256_file "$vt_path") || exit 1
                [ "$vt_actual_sha" = "$vt_expected_sha" ] || exit 1
            fi
        done
    then
        exit 1
    fi

    vt_actual_count=0
    for vt_entry in "$vt_target"/* "$vt_target"/.[!.]*; do
        if [ ! -e "$vt_entry" ] && [ ! -L "$vt_entry" ]; then
            continue
        fi
        [ -f "$vt_entry" ] && [ ! -L "$vt_entry" ] || exit 1
        vt_name=${vt_entry##*/}
        if [ "$vt_name" != "$MANIFEST_FILE" ] \
            && [ "$vt_name" != "$PUBLICATION_LOCK_FILE" ]; then
            jq -e --arg name "$vt_name" \
                'any(.files[]; .name == $name)' "$vt_manifest" >/dev/null 2>&1 || exit 1
        fi
        vt_actual_count=$((vt_actual_count + 1))
    done
    # No converter-owned target name starts with two dots. Keep this check
    # separate so the inventory patterns are visibly disjoint.
    for vt_forbidden in "$vt_target"/..?*; do
        if [ -e "$vt_forbidden" ] || [ -L "$vt_forbidden" ]; then
            exit 1
        fi
    done
    vt_expected_count=$(jq '.files | length + 2' "$vt_manifest") || exit 1
    [ "$vt_actual_count" -eq "$vt_expected_count" ] || exit 1
)

write_epoch_attestation() (
    wea_path=$1
    wea_epoch=$2
    wea_source=$3
    wea_target=$4
    wea_staging=$5
    wea_generation_id=$6
    wea_lease_id=$7
    wea_report=$8
    wea_log=$9

    wea_manifest=$wea_target/$MANIFEST_FILE
    wea_receipt=$wea_target/$RECEIPT_FILE
    wea_report_bytes=$(wc -c <"$wea_report" | tr -d '[:space:]') || exit 1
    wea_report_sha=$(sha256_file "$wea_report") || exit 1
    wea_log_bytes=$(wc -c <"$wea_log" | tr -d '[:space:]') || exit 1
    wea_log_sha=$(sha256_file "$wea_log") || exit 1
    wea_manifest_bytes=$(wc -c <"$wea_manifest" | tr -d '[:space:]') || exit 1
    wea_manifest_sha=$(sha256_file "$wea_manifest") || exit 1
    wea_manifest_digest=$(compute_generation_digest "$wea_manifest") || exit 1
    wea_receipt_bytes=$(wc -c <"$wea_receipt" | tr -d '[:space:]') || exit 1
    wea_receipt_sha=$(sha256_file "$wea_receipt") || exit 1

    (
        set -C
        jq -cn \
            --arg run_id "$run_id" \
            --arg epoch "$wea_epoch" \
            --arg cluster "$cluster_id" \
            --arg converter_origin "$converter_origin" \
            --arg converter_pinned "$pinned_converter" \
            --arg converter_sha "$converter_sha" \
            --arg source "$wea_source" \
            --arg target "$wea_target" \
            --arg staging "$wea_staging" \
            --arg generation "$wea_generation_id" \
            --arg lease "$wea_lease_id" \
            --arg epoch_list "$epoch_list" \
            --arg epoch_list_snapshot "$epoch_list_snapshot" \
            --arg epoch_list_sha "$epoch_list_sha" \
            --arg report "$wea_report" \
            --argjson report_bytes "$wea_report_bytes" \
            --arg report_sha "$wea_report_sha" \
            --arg log "$wea_log" \
            --argjson log_bytes "$wea_log_bytes" \
            --arg log_sha "$wea_log_sha" \
            --arg manifest "$wea_manifest" \
            --argjson manifest_bytes "$wea_manifest_bytes" \
            --arg manifest_sha "$wea_manifest_sha" \
            --arg manifest_digest "$wea_manifest_digest" \
            --arg receipt "$wea_receipt" \
            --argjson receipt_bytes "$wea_receipt_bytes" \
            --arg receipt_sha "$wea_receipt_sha" '
            {
                schema_version: 1,
                kind: "archive-v2-pre-to-post-runner-epoch-attestation",
                run_id: $run_id,
                epoch: ($epoch | tonumber),
                cluster_id: $cluster,
                converter_origin: $converter_origin,
                converter_pinned: $converter_pinned,
                converter_sha256: $converter_sha,
                source: $source,
                target: $target,
                staging: $staging,
                generation_id: $generation,
                lease_id: $lease,
                epoch_list: $epoch_list,
                epoch_list_snapshot: $epoch_list_snapshot,
                epoch_list_sha256: $epoch_list_sha,
                report: {path: $report, bytes: $report_bytes, sha256: $report_sha},
                log: {path: $log, bytes: $log_bytes, sha256: $log_sha},
                manifest: {
                    path: $manifest,
                    bytes: $manifest_bytes,
                    sha256: $manifest_sha,
                    generation_digest: $manifest_digest
                },
                receipt: {path: $receipt, bytes: $receipt_bytes, sha256: $receipt_sha}
            }
        ' >"$wea_path"
    )
)

validate_epoch_attestation() (
    vea_path=$1
    vea_epoch=$2
    vea_source=$3
    vea_target=$4
    vea_staging=$5
    vea_generation_id=$6
    vea_lease_id=$7
    vea_report=$8
    vea_log=$9

    for vea_state in "$vea_path" "$vea_report" "$vea_log"; do
        [ -f "$vea_state" ] && [ ! -L "$vea_state" ] || exit 1
        [ ! -w "$vea_state" ] || exit 1
    done
    vea_lines=$(wc -l <"$vea_path" | tr -d '[:space:]') || exit 1
    [ "$vea_lines" = 1 ] || exit 1

    vea_manifest=$vea_target/$MANIFEST_FILE
    vea_receipt=$vea_target/$RECEIPT_FILE
    vea_report_bytes=$(wc -c <"$vea_report" | tr -d '[:space:]') || exit 1
    vea_report_sha=$(sha256_file "$vea_report") || exit 1
    vea_log_bytes=$(wc -c <"$vea_log" | tr -d '[:space:]') || exit 1
    vea_log_sha=$(sha256_file "$vea_log") || exit 1
    vea_manifest_bytes=$(wc -c <"$vea_manifest" | tr -d '[:space:]') || exit 1
    vea_manifest_sha=$(sha256_file "$vea_manifest") || exit 1
    vea_manifest_digest=$(compute_generation_digest "$vea_manifest") || exit 1
    vea_receipt_bytes=$(wc -c <"$vea_receipt" | tr -d '[:space:]') || exit 1
    vea_receipt_sha=$(sha256_file "$vea_receipt") || exit 1

    jq -e -s \
        --arg run_id "$run_id" \
        --arg epoch "$vea_epoch" \
        --arg cluster "$cluster_id" \
        --arg converter_origin "$converter_origin" \
        --arg converter_pinned "$pinned_converter" \
        --arg converter_sha "$converter_sha" \
        --arg source "$vea_source" \
        --arg target "$vea_target" \
        --arg staging "$vea_staging" \
        --arg generation "$vea_generation_id" \
        --arg lease "$vea_lease_id" \
        --arg epoch_list "$epoch_list" \
        --arg epoch_list_snapshot "$epoch_list_snapshot" \
        --arg epoch_list_sha "$epoch_list_sha" \
        --arg report "$vea_report" \
        --argjson report_bytes "$vea_report_bytes" \
        --arg report_sha "$vea_report_sha" \
        --arg log "$vea_log" \
        --argjson log_bytes "$vea_log_bytes" \
        --arg log_sha "$vea_log_sha" \
        --arg manifest "$vea_manifest" \
        --argjson manifest_bytes "$vea_manifest_bytes" \
        --arg manifest_sha "$vea_manifest_sha" \
        --arg manifest_digest "$vea_manifest_digest" \
        --arg receipt "$vea_receipt" \
        --argjson receipt_bytes "$vea_receipt_bytes" \
        --arg receipt_sha "$vea_receipt_sha" '
        length == 1 and
        (.[0] as $a |
            ($a | keys) == ["cluster_id", "converter_origin", "converter_pinned",
                "converter_sha256", "epoch", "epoch_list", "epoch_list_sha256",
                "epoch_list_snapshot", "generation_id", "kind", "lease_id", "log",
                "manifest", "receipt", "report", "run_id", "schema_version", "source",
                "staging", "target"] and
            $a.schema_version == 1 and
            $a.kind == "archive-v2-pre-to-post-runner-epoch-attestation" and
            ($a.epoch | tostring) == $epoch and
            $a.run_id == $run_id and
            $a.cluster_id == $cluster and
            $a.converter_origin == $converter_origin and
            $a.converter_pinned == $converter_pinned and
            $a.converter_sha256 == $converter_sha and
            $a.source == $source and
            $a.target == $target and
            $a.staging == $staging and
            $a.generation_id == $generation and
            $a.lease_id == $lease and
            $a.epoch_list == $epoch_list and
            $a.epoch_list_snapshot == $epoch_list_snapshot and
            $a.epoch_list_sha256 == $epoch_list_sha and
            ($a.report | keys) == ["bytes", "path", "sha256"] and
            $a.report == {path: $report, bytes: $report_bytes, sha256: $report_sha} and
            ($a.log | keys) == ["bytes", "path", "sha256"] and
            $a.log == {path: $log, bytes: $log_bytes, sha256: $log_sha} and
            ($a.manifest | keys) == ["bytes", "generation_digest", "path", "sha256"] and
            $a.manifest == {
                path: $manifest,
                bytes: $manifest_bytes,
                sha256: $manifest_sha,
                generation_digest: $manifest_digest
            } and
            ($a.receipt | keys) == ["bytes", "path", "sha256"] and
            $a.receipt == {path: $receipt, bytes: $receipt_bytes, sha256: $receipt_sha}
        )
    ' "$vea_path" >/dev/null 2>&1
)

validate_complete_file() (
    vcf_path=$1
    vcf_epochs=$2
    vcf_count=$3
    vcf_converter_sha=$4
    vcf_list_sha=$5
    vcf_attestations=$6
    [ -f "$vcf_path" ] && [ ! -L "$vcf_path" ] && [ ! -w "$vcf_path" ] || exit 1
    vcf_lines=$(wc -l <"$vcf_path" | tr -d '[:space:]') || exit 1
    [ "$vcf_lines" = 1 ] || exit 1
    jq -e -s \
        --arg run_id "$run_id" \
        --arg cluster "$cluster_id" \
        --arg converter_origin "$converter_origin" \
        --arg converter_pinned "$pinned_converter" \
        --arg converter_sha "$vcf_converter_sha" \
        --arg source_root "$source_root" \
        --arg target_root "$target_root" \
        --arg state_root "$state_root" \
        --arg epoch_list "$epoch_list" \
        --arg epoch_list_snapshot "$epoch_list_snapshot" \
        --arg epoch_list_sha "$vcf_list_sha" \
        --arg authority_kind "$AUTHORITY_KIND" \
        --argjson epoch_count "$vcf_count" \
        --argjson epochs "$vcf_epochs" \
        --argjson attestations "$vcf_attestations" '
        length == 1 and
        (.[0] as $c |
            ($c | keys) == ["cluster_id", "completed_at_utc", "completed_epochs",
                "converter_origin", "converter_pinned", "converter_sha256", "epoch_attestations",
                "epoch_count", "epoch_list", "epoch_list_sha256", "epoch_list_snapshot",
                "epochs", "kind", "run_id", "schema_version", "source_authority_kind",
                "source_root", "state_root", "target_root"] and
            $c.schema_version == 1 and
            $c.kind == "archive-v2-pre-to-post-manual-run-complete" and
            $c.run_id == $run_id and
            $c.cluster_id == $cluster and
            $c.converter_origin == $converter_origin and
            $c.converter_pinned == $converter_pinned and
            $c.converter_sha256 == $converter_sha and
            $c.source_root == $source_root and
            $c.target_root == $target_root and
            $c.state_root == $state_root and
            $c.epoch_list == $epoch_list and
            $c.epoch_list_snapshot == $epoch_list_snapshot and
            $c.epoch_list_sha256 == $epoch_list_sha and
            $c.source_authority_kind == $authority_kind and
            $c.epoch_count == $epoch_count and
            $c.completed_epochs == $epoch_count and
            $c.epochs == $epochs and
            $c.epoch_attestations == $attestations and
            ($c.completed_at_utc | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"))
        )
    ' "$vcf_path" >/dev/null 2>&1
)

[ "$#" -eq 7 ] || usage

converter_origin=$1
converter=$converter_origin
source_root=$2
target_root=$3
state_root=$4
epoch_list=$5
cluster_id=$6
run_id=$7

for required in jq awk sed wc tr date nice ionice chmod mkdir rmdir ln rm dd python3; do
    require_command "$required"
done
if command -v sha256sum >/dev/null 2>&1; then
    sha256_program=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    sha256_program=shasum
else
    die 'sha256sum or shasum is required'
fi

require_absolute_single_line CONVERTER "$converter"
require_absolute_single_line SOURCE_ROOT "$source_root"
require_absolute_single_line TARGET_ROOT "$target_root"
require_absolute_single_line STATE_ROOT "$state_root"
require_absolute_single_line EPOCH_LIST "$epoch_list"

case "$cluster_id" in
    ''|*[!A-Za-z0-9._-]*) die 'CLUSTER_ID must use only letters, digits, dot, underscore, or hyphen' ;;
esac
case "$run_id" in
    ''|*[!A-Za-z0-9._-]*) die 'RUN_ID must use only letters, digits, dot, underscore, or hyphen' ;;
esac
[ "$(printf '%s' "$cluster_id" | wc -c | tr -d '[:space:]')" -le 128 ] \
    || die 'CLUSTER_ID is longer than 128 bytes'
[ "$(printf '%s' "$run_id" | wc -c | tr -d '[:space:]')" -le 128 ] \
    || die 'RUN_ID is longer than 128 bytes'

[ -f "$converter" ] && [ ! -L "$converter" ] && [ -x "$converter" ] \
    || die "converter is not one real executable file: $converter"
converter_basename=${converter##*/}
expected_converter_sha=$(printf '%s\n' "$converter_basename" |
    sed -n 's/^archive-v2-pre-to-post-\([0-9a-f]\{64\}\)$/\1/p')
[ -n "$expected_converter_sha" ] \
    || die 'converter basename must be archive-v2-pre-to-post-SHA256'
converter_sha=$(sha256_file "$converter") || die 'cannot hash the converter'
[ "$converter_sha" = "$expected_converter_sha" ] \
    || die "converter digest does not match its content-addressed basename: $converter"

[ -d "$source_root" ] && [ ! -L "$source_root" ] \
    || die "source root is not one real directory: $source_root"
[ -d "$target_root" ] && [ ! -L "$target_root" ] \
    || die "target root is not one real directory: $target_root"
[ -f "$epoch_list" ] && [ ! -L "$epoch_list" ] \
    || die "epoch list is not one real file: $epoch_list"

source_canonical=$(canonical_directory "$source_root") \
    || die "cannot canonicalize source root: $source_root"
[ "$source_canonical" = "$source_root" ] \
    || die "SOURCE_ROOT must already be canonical: $source_canonical"
target_canonical=$(canonical_directory "$target_root") \
    || die "cannot canonicalize target root: $target_root"
[ "$target_canonical" = "$target_root" ] \
    || die "TARGET_ROOT must already be canonical: $target_canonical"
[ "$source_root" != "$target_root" ] || die 'source and target roots must differ'
chmod 700 "$target_root" || die "cannot set target root mode 0700: $target_root"

mkdir -p "$state_root" || die "cannot create state root: $state_root"
[ -d "$state_root" ] && [ ! -L "$state_root" ] \
    || die "state root is not one real directory: $state_root"
chmod 700 "$state_root" || die "cannot set state root mode 0700: $state_root"
state_canonical=$(canonical_directory "$state_root") \
    || die "cannot canonicalize state root: $state_root"
[ "$state_canonical" = "$state_root" ] \
    || die "STATE_ROOT must already be canonical: $state_canonical"

lock_dir=$state_root/.archive-v2-pre-to-post-manual.lock
if ! mkdir "$lock_dir" 2>/dev/null; then
    die "run lock already exists; another run or a stopped run needs review: $lock_dir"
fi
lock_held=1
converter_pid=
release_lock() {
    trap - 0 1 2 3 15
    if [ "${lock_held:-0}" -eq 1 ]; then
        rmdir "$lock_dir" 2>/dev/null \
            || echo "archive-v2-pre-to-post runner: cannot remove run lock: $lock_dir" >&2
        lock_held=0
    fi
}
stop_run() {
    sr_status=$1
    trap - 0 1 2 3 15
    if [ -n "${converter_pid:-}" ]; then
        kill -TERM "$converter_pid" 2>/dev/null || :
        wait "$converter_pid" 2>/dev/null || :
        converter_pid=
    fi
    release_lock
    exit "$sr_status"
}
trap 'release_lock' 0
trap 'stop_run 129' 1
trap 'stop_run 130' 2
trap 'stop_run 131' 3
trap 'stop_run 143' 15

for stale_state in "$state_root"/complete.json.building-* \
    "$state_root"/epoch-list.snapshot.building-*; do
    if [ -e "$stale_state" ] || [ -L "$stale_state" ]; then
        die "unfinished run state needs review: $stale_state"
    fi
done

pinned_converter_dir=$state_root/.archive-v2-pre-to-post-bin
if [ -e "$pinned_converter_dir" ] || [ -L "$pinned_converter_dir" ]; then
    [ -d "$pinned_converter_dir" ] && [ ! -L "$pinned_converter_dir" ] \
        || die "private converter path is not one real directory: $pinned_converter_dir"
else
    mkdir "$pinned_converter_dir" || die "cannot create private converter directory"
fi
chmod 700 "$pinned_converter_dir" \
    || die "cannot set private converter directory mode 0700: $pinned_converter_dir"
pinned_converter=$pinned_converter_dir/$converter_basename
for stale_converter in "$pinned_converter".building-*; do
    if [ -e "$stale_converter" ] || [ -L "$stale_converter" ]; then
        die "unfinished private converter copy needs review: $stale_converter"
    fi
done
if [ -e "$pinned_converter" ] || [ -L "$pinned_converter" ]; then
    [ -f "$pinned_converter" ] && [ ! -L "$pinned_converter" ] \
        || die "private converter is not one real file: $pinned_converter"
else
    pinned_converter_building=$pinned_converter.building-$$
    copy_file_exclusive "$converter_origin" "$pinned_converter_building" \
        || die "cannot make an exclusive private converter copy: $pinned_converter_building"
    copied_converter_sha=$(sha256_file "$pinned_converter_building") \
        || die 'cannot hash the private converter copy'
    [ "$copied_converter_sha" = "$converter_sha" ] \
        || die 'converter changed while its private copy was made'
    chmod 500 "$pinned_converter_building" \
        || die "cannot make private converter copy read-only"
    publish_file_no_replace "$pinned_converter_building" "$pinned_converter" \
        || die "cannot publish private converter without replacement"
fi
[ -x "$pinned_converter" ] && [ ! -w "$pinned_converter" ] \
    || die "private converter is not executable and read-only: $pinned_converter"
pinned_converter_sha=$(sha256_file "$pinned_converter") \
    || die 'cannot hash the private converter'
[ "$pinned_converter_sha" = "$converter_sha" ] \
    || die 'private converter digest differs from the admitted converter'
exec 4<"$pinned_converter" || die 'cannot pin the private converter file'
[ -e /dev/fd/4 ] || die '/dev/fd does not expose the pinned converter descriptor'
pinned_fd_sha=$(sha256_file /dev/fd/4) || die 'cannot hash the pinned converter descriptor'
[ "$pinned_fd_sha" = "$converter_sha" ] || die 'pinned converter descriptor digest differs'

epoch_list_snapshot=$state_root/epoch-list.snapshot
epoch_list_input_sha=$(sha256_file "$epoch_list") || die 'cannot hash the input epoch list'
if [ -e "$epoch_list_snapshot" ] || [ -L "$epoch_list_snapshot" ]; then
    [ -f "$epoch_list_snapshot" ] && [ ! -L "$epoch_list_snapshot" ] \
        || die "epoch-list snapshot is not one real file: $epoch_list_snapshot"
else
    epoch_list_snapshot_building=$epoch_list_snapshot.building-$$
    copy_file_exclusive "$epoch_list" "$epoch_list_snapshot_building" \
        || die "cannot make an exclusive private epoch-list snapshot"
    copied_epoch_list_sha=$(sha256_file "$epoch_list_snapshot_building") \
        || die 'cannot hash private epoch-list snapshot'
    epoch_list_input_sha_after=$(sha256_file "$epoch_list") \
        || die 'cannot rehash input epoch list after snapshot copy'
    [ "$copied_epoch_list_sha" = "$epoch_list_input_sha" ] \
        && [ "$copied_epoch_list_sha" = "$epoch_list_input_sha_after" ] \
        || die 'input epoch list changed while its private snapshot was made'
    validate_epoch_list "$epoch_list_snapshot_building" >/dev/null \
        || die "private epoch-list snapshot is invalid"
    chmod 400 "$epoch_list_snapshot_building" \
        || die 'cannot make private epoch-list snapshot read-only'
    publish_file_no_replace "$epoch_list_snapshot_building" "$epoch_list_snapshot" \
        || die 'cannot publish private epoch-list snapshot without replacement'
fi
[ -f "$epoch_list_snapshot" ] && [ ! -L "$epoch_list_snapshot" ] \
    && [ ! -w "$epoch_list_snapshot" ] \
    || die "private epoch-list snapshot is not one read-only real file"
epoch_list_sha=$(sha256_file "$epoch_list_snapshot") \
    || die 'cannot hash the private epoch-list snapshot'
[ "$epoch_list_input_sha" = "$epoch_list_sha" ] \
    || die 'input epoch list differs from the private run snapshot'
epoch_count=$(validate_epoch_list "$epoch_list_snapshot") \
    || die "private epoch-list snapshot is not an exact, unique decimal epoch list"
epochs_json=$(jq -Rsc 'split("\n") | map(select(length > 0) | tonumber)' "$epoch_list_snapshot") \
    || die 'cannot encode the epoch list as JSON'
[ "$(jq 'length' <<EOF
$epochs_json
EOF
)" -eq "$epoch_count" ] || die 'epoch-list JSON count is inconsistent'

completed_epochs=0
attestations_json='[]'
exec 3<"$epoch_list_snapshot" || die "cannot open private epoch-list snapshot"
while IFS= read -r epoch <&3 || [ -n "$epoch" ]; do
    source_epoch=$source_root/epoch-$epoch
    target_epoch=$target_root/epoch-$epoch
    staging_epoch=$target_root/.epoch-$epoch.pre-to-post.staging
    report=$state_root/epoch-$epoch.json
    log=$state_root/epoch-$epoch.log
    attestation=$state_root/epoch-$epoch.attestation.json
    generation_id=archive-v2-pre-to-post-$run_id-epoch-$epoch-post
    lease_id=archive-v2-pre-to-post-$run_id-epoch-$epoch-source-leases

    [ -d "$source_epoch" ] && [ ! -L "$source_epoch" ] \
        || die "epoch $epoch source directory is absent or is not a real directory: $source_epoch"
    [ ! -e "$staging_epoch" ] && [ ! -L "$staging_epoch" ] \
        || die "epoch $epoch staging exists; it will not be removed: $staging_epoch"

    for stale in "$report".building-* "$log".building-* "$attestation".building-*; do
        if [ -e "$stale" ] || [ -L "$stale" ]; then
            die "epoch $epoch has unfinished state that needs review: $stale"
        fi
    done

    if [ -e "$target_epoch" ] || [ -L "$target_epoch" ]; then
        [ -f "$report" ] && [ ! -L "$report" ] \
            || die "epoch $epoch target exists without its original valid report: $report"
        [ -f "$log" ] && [ ! -L "$log" ] \
            || die "epoch $epoch target exists without its original log: $log"
        [ -f "$attestation" ] && [ ! -L "$attestation" ] \
            || die "epoch $epoch target exists without its original runner attestation: $attestation"
        echo "epoch $epoch: validating existing target for resume"
        validate_target_generation "$epoch" "$source_epoch" "$target_epoch" \
            "$staging_epoch" "$generation_id" "$lease_id" "$report" 1 \
            || die "epoch $epoch existing target, report, manifest, or receipt is invalid"
        validate_epoch_attestation "$attestation" "$epoch" "$source_epoch" "$target_epoch" \
            "$staging_epoch" "$generation_id" "$lease_id" "$report" "$log" \
            || die "epoch $epoch existing runner attestation is invalid"
        attestation_sha=$(sha256_file "$attestation") \
            || die "epoch $epoch cannot hash its runner attestation"
        attestations_json=$(jq -cn \
            --argjson current "$attestations_json" \
            --argjson epoch "$epoch" \
            --arg path "$attestation" \
            --arg sha "$attestation_sha" \
            '$current + [{epoch: $epoch, path: $path, sha256: $sha}]') \
            || die "epoch $epoch cannot add its runner attestation to completion state"
        completed_epochs=$((completed_epochs + 1))
        echo "epoch $epoch: accepted existing canonical Post target"
        continue
    fi

    [ ! -e "$report" ] && [ ! -L "$report" ] \
        || die "epoch $epoch report exists but its target is absent: $report"
    [ ! -e "$log" ] && [ ! -L "$log" ] \
        || die "epoch $epoch log exists but its target is absent: $log"
    [ ! -e "$attestation" ] && [ ! -L "$attestation" ] \
        || die "epoch $epoch attestation exists but its target is absent: $attestation"

    current_converter_sha=$(sha256_file "$pinned_converter") \
        || die "epoch $epoch cannot hash private converter before execution"
    [ "$current_converter_sha" = "$converter_sha" ] \
        || die "epoch $epoch converter changed after run admission"

    building_report=$report.building-$$
    building_log=$log.building-$$
    building_attestation=$attestation.building-$$
    [ ! -e "$building_report" ] && [ ! -L "$building_report" ] \
        || die "epoch $epoch PID-specific report already exists: $building_report"
    [ ! -e "$building_log" ] && [ ! -L "$building_log" ] \
        || die "epoch $epoch PID-specific log already exists: $building_log"
    [ ! -e "$building_attestation" ] && [ ! -L "$building_attestation" ] \
        || die "epoch $epoch PID-specific attestation already exists: $building_attestation"

    echo "epoch $epoch: converting legacy Pre to canonical Post"
    started=$(date +%s)
    (
        set -C
        exec nice -n 19 ionice -c 3 /dev/fd/4 \
            --source "$source_epoch" \
            --source-lease-id "$lease_id" \
            --target "$target_epoch" \
            --staging "$staging_epoch" \
            --epoch "$epoch" \
            --cluster-id "$cluster_id" \
            --generation-id "$generation_id" \
            >"$building_report" 2>"$building_log"
    ) &
    converter_pid=$!
    if wait "$converter_pid"; then
        converter_status=0
    else
        converter_status=$?
    fi
    converter_pid=

    if [ "$converter_status" -ne 0 ]; then
        die "epoch $epoch converter exited with status $converter_status; private building output was kept for review"
    fi

    if ! validate_target_generation "$epoch" "$source_epoch" "$target_epoch" \
        "$staging_epoch" "$generation_id" "$lease_id" "$building_report" 0; then
        die "epoch $epoch converter result, target, manifest, or receipt is invalid; private building output was kept for review"
    fi

    [ ! -e "$log" ] && [ ! -L "$log" ] || die "epoch $epoch log appeared during conversion"
    [ ! -e "$report" ] && [ ! -L "$report" ] || die "epoch $epoch report appeared during conversion"
    [ ! -e "$attestation" ] && [ ! -L "$attestation" ] \
        || die "epoch $epoch attestation appeared during conversion"
    chmod 400 "$building_log" "$building_report" \
        || die "epoch $epoch cannot make its result state read-only"
    publish_file_no_replace "$building_log" "$log" \
        || die "epoch $epoch cannot publish its log without replacement"
    publish_file_no_replace "$building_report" "$report" \
        || die "epoch $epoch cannot publish its report without replacement"
    write_epoch_attestation "$building_attestation" "$epoch" "$source_epoch" "$target_epoch" \
        "$staging_epoch" "$generation_id" "$lease_id" "$report" "$log" \
        || die "epoch $epoch cannot create its runner attestation"
    chmod 400 "$building_attestation" \
        || die "epoch $epoch cannot make its runner attestation read-only"
    validate_epoch_attestation "$building_attestation" "$epoch" "$source_epoch" "$target_epoch" \
        "$staging_epoch" "$generation_id" "$lease_id" "$report" "$log" \
        || die "epoch $epoch generated runner attestation is invalid"
    publish_file_no_replace "$building_attestation" "$attestation" \
        || die "epoch $epoch cannot publish its runner attestation without replacement"
    attestation_sha=$(sha256_file "$attestation") \
        || die "epoch $epoch cannot hash its runner attestation"
    attestations_json=$(jq -cn \
        --argjson current "$attestations_json" \
        --argjson epoch "$epoch" \
        --arg path "$attestation" \
        --arg sha "$attestation_sha" \
        '$current + [{epoch: $epoch, path: $path, sha256: $sha}]') \
        || die "epoch $epoch cannot add its runner attestation to completion state"
    finished=$(date +%s)
    completed_epochs=$((completed_epochs + 1))
    echo "epoch $epoch: canonical Post target published in $((finished - started)) seconds"
done
exec 3<&-

[ "$completed_epochs" -eq "$epoch_count" ] \
    || die "completed epoch count $completed_epochs differs from list count $epoch_count"
final_pinned_path_sha=$(sha256_file "$pinned_converter") \
    || die 'cannot hash private converter path at completion'
[ "$final_pinned_path_sha" = "$converter_sha" ] \
    || die 'private converter path changed before completion'
final_epoch_list_sha=$(sha256_file "$epoch_list_snapshot") \
    || die 'cannot hash private epoch-list snapshot at completion'
[ "$final_epoch_list_sha" = "$epoch_list_sha" ] \
    || die 'private epoch-list snapshot changed during the run'
final_input_epoch_list_sha=$(sha256_file "$epoch_list") \
    || die 'cannot hash input epoch list at completion'
[ "$final_input_epoch_list_sha" = "$epoch_list_sha" ] \
    || die 'input epoch list changed during the run'

complete=$state_root/complete.json
if [ -e "$complete" ] || [ -L "$complete" ]; then
    [ -f "$complete" ] && [ ! -L "$complete" ] \
        || die "completion path is not one real file: $complete"
    validate_complete_file "$complete" "$epochs_json" "$epoch_count" \
        "$converter_sha" "$epoch_list_sha" "$attestations_json" \
        || die "existing completion record does not describe this exact run: $complete"
    echo "all $epoch_count listed epochs were already complete"
    exit 0
fi

complete_building=$complete.building-$$
completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) || die 'cannot get completion time'
(
    set -C
    jq -cn \
        --arg run_id "$run_id" \
        --arg cluster "$cluster_id" \
        --arg converter_origin "$converter_origin" \
        --arg converter_pinned "$pinned_converter" \
        --arg converter_sha "$converter_sha" \
        --arg source_root "$source_root" \
        --arg target_root "$target_root" \
        --arg state_root "$state_root" \
        --arg epoch_list "$epoch_list" \
        --arg epoch_list_snapshot "$epoch_list_snapshot" \
        --arg epoch_list_sha "$epoch_list_sha" \
        --arg authority_kind "$AUTHORITY_KIND" \
        --argjson epoch_count "$epoch_count" \
        --argjson epochs "$epochs_json" \
        --argjson attestations "$attestations_json" \
        --arg completed_at "$completed_at" '
        {
            schema_version: 1,
            kind: "archive-v2-pre-to-post-manual-run-complete",
            run_id: $run_id,
            cluster_id: $cluster,
            converter_origin: $converter_origin,
            converter_pinned: $converter_pinned,
            converter_sha256: $converter_sha,
            source_root: $source_root,
            target_root: $target_root,
            state_root: $state_root,
            epoch_list: $epoch_list,
            epoch_list_snapshot: $epoch_list_snapshot,
            epoch_list_sha256: $epoch_list_sha,
            source_authority_kind: $authority_kind,
            epoch_count: $epoch_count,
            completed_epochs: $epoch_count,
            epochs: $epochs,
            epoch_attestations: $attestations,
            completed_at_utc: $completed_at
        }
    ' >"$complete_building"
) || die 'cannot exclusively create completion record'
chmod 400 "$complete_building" || die 'cannot make completion record read-only'
validate_complete_file "$complete_building" "$epochs_json" "$epoch_count" \
    "$converter_sha" "$epoch_list_sha" "$attestations_json" \
    || die 'generated completion record is invalid'
[ ! -e "$complete" ] && [ ! -L "$complete" ] || die 'completion record appeared during publication'
publish_file_no_replace "$complete_building" "$complete" \
    || die 'cannot publish completion record without replacement'
echo "all $epoch_count listed epochs are canonical Post; completion record: $complete"
