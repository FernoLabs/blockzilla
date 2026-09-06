#!/bin/sh
# Validate the manual Pre-to-Post runner with a small, pre-published synthetic
# epoch. The content-addressed fake converter is /usr/bin/false and must never
# run. The target uses the real epoch-2 durable-file inventory from the NAS.

set -eu
umask 077

fail() {
    echo "manual Pre-to-Post runner self-test: $*" >&2
    exit 1
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

# Independent implementation of the SDK generation digest. The runner uses
# Python; this test uses Perl pack() so an encoding mistake is not mirrored.
sdk_generation_digest() {
    perl -MJSON::PP -MDigest::SHA=sha256_hex -0777 -e '
        my $manifest = decode_json(<STDIN>);
        sub text {
            my ($value) = @_;
            utf8::encode($value) if utf8::is_utf8($value);
            return pack("V", length($value)) . $value;
        }
        my @files = sort { $a->{name} cmp $b->{name} } @{$manifest->{files}};
        my $preimage = "blockzilla/archive-v2-generation\0";
        $preimage .= pack("V", $manifest->{schema_version});
        $preimage .= text($manifest->{cluster_id});
        $preimage .= pack("Q<", $manifest->{epoch});
        $preimage .= text($manifest->{generation_id});
        $preimage .= pack("Q<", $manifest->{slots_per_epoch});
        $preimage .= pack("C", $manifest->{complete} ? 1 : 0);
        $preimage .= pack("V", scalar @files);
        for my $file (@files) {
            $preimage .= text($file->{name});
            $preimage .= pack("Q<", $file->{size});
            $preimage .= pack("H*", $file->{sha256});
        }
        print sha256_hex($preimage), "\n";
    ' <"$1"
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
repo_root=$(CDPATH= cd -P "$script_dir/.." && pwd -P)
runner=$script_dir/run-archive-v2-pre-to-post-manual.sh
marker_asset=$repo_root/crates/compat/blockzilla-read-sdk-legacy/assets/archive-v2-message-schema-post-unknown-fallbacks-v1.marker

command -v jq >/dev/null 2>&1 || fail 'jq is required'
command -v awk >/dev/null 2>&1 || fail 'awk is required'
command -v dd >/dev/null 2>&1 || fail 'dd is required'
command -v perl >/dev/null 2>&1 || fail 'perl is required'
perl -MJSON::PP -MDigest::SHA -e 1 >/dev/null 2>&1 \
    || fail 'Perl JSON::PP and Digest::SHA are required'
if [ -x /usr/bin/true ] && [ -x /usr/bin/false ]; then
    true_program=/usr/bin/true
    false_program=/usr/bin/false
elif [ -x /bin/true ] && [ -x /bin/false ]; then
    true_program=/bin/true
    false_program=/bin/false
else
    fail 'real true and false executables are required'
fi

fixture_root=$(mktemp -d "${TMPDIR:-/tmp}/archive-v2-pre-to-post-runner-test.XXXXXX")
fixture_root=$(CDPATH= cd -P "$fixture_root" && pwd -P)
case "${fixture_root##*/}" in
    archive-v2-pre-to-post-runner-test.*) ;;
    *) fail "unexpected temporary directory: $fixture_root" ;;
esac
cleanup() {
    trap - 0 1 2 3 15
    if [ -n "${fixture_root:-}" ] && [ -d "$fixture_root" ]; then
        chmod -R u+w "$fixture_root" 2>/dev/null || :
        rm -rf "$fixture_root"
    fi
}
trap 'cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

printf 'new\n' >"$fixture_root/link-source"
printf 'old\n' >"$fixture_root/link-target"
if ln -n "$fixture_root/link-source" "$fixture_root/link-target" 2>/dev/null; then
    fail 'ln -n replaced an existing publication target'
fi
[ "$(sed -n '1p' "$fixture_root/link-target")" = old ] \
    || fail 'failed hard-link publication changed its destination'

source_root=$fixture_root/source
target_root=$fixture_root/target
state_root=$fixture_root/state
source_epoch=$source_root/epoch-2
target_epoch=$target_root/epoch-2
staging_epoch=$target_root/.epoch-2.pre-to-post.staging
epoch_list=$fixture_root/epochs.txt
report=$state_root/epoch-2.json
log=$state_root/epoch-2.log
attestation=$state_root/epoch-2.attestation.json
manifest=$target_epoch/archive-v2-generation.json
receipt=$target_epoch/archive-v2-pre-to-post.receipt.json
generation_id=archive-v2-pre-to-post-synthetic-epoch-2-post
lease_id=archive-v2-pre-to-post-synthetic-epoch-2-source-leases
generation_digest='0000000000000000000000000000000000000000000000000000000000000000'

mkdir -p "$source_epoch" "$target_epoch" "$state_root"
printf '2\n' >"$epoch_list"

durable_files='archive-v2-blocks.zstd
archive-v2-blocks.index
archive-v2-meta.wincode
blockhash_registry.bin
block-time-gaps.bin
poh.wincode
prev_blockhash_tail.bin
registry.bin
registry_counts.bin
registry.mphf
shredding.wincode
signatures.bin
vote_hash_registry.bin'
old_ifs=$IFS
IFS='
'
for name in $durable_files; do
    cp "$true_program" "$target_epoch/$name"
done
IFS=$old_ifs
cp "$marker_asset" "$target_epoch/archive-v2-message-schema-post-unknown-fallbacks-v1.marker"
: >"$target_epoch/.archive-v2-manifest-publish.lock"

data_bytes=$(wc -c <"$true_program" | tr -d '[:space:]')
data_sha=$(sha256_file "$true_program")
names_json='[
    "archive-v2-blocks.zstd",
    "archive-v2-blocks.index",
    "archive-v2-meta.wincode",
    "blockhash_registry.bin",
    "block-time-gaps.bin",
    "poh.wincode",
    "prev_blockhash_tail.bin",
    "registry.bin",
    "registry_counts.bin",
    "registry.mphf",
    "shredding.wincode",
    "signatures.bin",
    "vote_hash_registry.bin"
]'
bindings=$(jq -cn \
    --argjson names "$names_json" \
    --argjson bytes "$data_bytes" \
    --arg sha "$data_sha" '
    reduce $names[] as $name ({};
        .[$name] = {bytes: $bytes, sha256: $sha})
')
rewrite=$(jq -cn \
    --argjson bytes "$data_bytes" \
    --arg sha "$data_sha" '
    {
        blocks: 1,
        borrowed_current_blocks: 1,
        owned_outer_fallbacks: 0,
        typed_messages: 1,
        raw_transaction_fallbacks: 0,
        message_input_bytes: 10,
        message_output_bytes: 10,
        message_mismatch_bytes: 1,
        source_instruction_data_tag_counts: [0, 1, 0, 0, 0, 0, 0, 0, 0],
        metadata_input_bytes: 5,
        metadata_output_bytes: 5,
        metadata_regions_byte_identical: true,
        source_compressed_bytes: $bytes,
        target_compressed_bytes: $bytes,
        uncompressed_bytes: 20,
        target_blocks_sha256: $sha
    }
')

jq -n \
    --arg source "$source_epoch" \
    --arg target "$target_epoch" \
    --arg generation "$generation_id" \
    --arg lease "$lease_id" \
    --argjson bindings "$bindings" \
    --argjson rewrite "$rewrite" '
    {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-receipt",
        epoch: 2,
        cluster_id: "mainnet-beta",
        generation_id: $generation,
        source: $source,
        source_authority_kind: "linux-kernel-read-leases",
        source_authority_id: $lease,
        source_authority_scope: "all-reviewed-source-inodes-pinned-and-read-leased-on-one-local-ext4-device",
        source_authority_filesystem: "linux-local-ext4",
        source_authority_device_id: 1,
        target: $target,
        source_profile: "pre-unknown-instruction-fallbacks-v1",
        target_profile: "post-unknown-instruction-fallbacks-v1",
        source_profile_decision: "unique-full-generation-decode",
        codec: "wincode-leb128-current-block+independent-zstd-frames",
        source_zstd_level: 3,
        target_zstd_level: 3,
        source_audit: {
            blocks: 1,
            typed_messages: 1,
            raw_transaction_fallbacks: 0,
            raw_metadata_fallbacks: 0,
            selected_only: 1,
            both_semantically_equivalent: 0,
            both_semantically_divergent: 0
        },
        source_files: $bindings,
        target_files: $bindings,
        omitted_edge_files: [],
        omitted_control_files: [],
        omitted_obsolete_block_files: [],
        rewrite: $rewrite,
        exact_message_length_preserved: true,
        exact_message_delta_proved: true,
        metadata_regions_copied_verbatim: true,
        edge_rebuild_required: false,
        target_provider_immutability_required: true,
        source_provider_snapshot_required: false,
        source_linux_read_leases_required: true
    }
' >"$receipt"

receipt_bytes=$(wc -c <"$receipt" | tr -d '[:space:]')
receipt_sha=$(sha256_file "$receipt")
jq -cn \
    --arg generation "$generation_id" \
    --arg digest "$generation_digest" \
    --argjson bindings "$bindings" \
    --argjson receipt_bytes "$receipt_bytes" \
    --arg receipt_sha "$receipt_sha" '
    ($bindings | to_entries |
        map({name: .key, size: .value.bytes, sha256: .value.sha256})) as $data |
    {
        schema_version: 1,
        cluster_id: "mainnet-beta",
        epoch: 2,
        generation_id: $generation,
        generation_digest: $digest,
        slots_per_epoch: 432000,
        complete: true,
        files: (($data + [
            {
                name: "archive-v2-pre-to-post.receipt.json",
                size: $receipt_bytes,
                sha256: $receipt_sha
            },
            {
                name: "archive-v2-message-schema-post-unknown-fallbacks-v1.marker",
                size: 77,
                sha256: "c870c4b0940b05b7bd18a134fba496c5c376f539ef7668f137112526d5c61edd"
            }
        ]) | sort_by(.name))
    }
' >"$manifest"
generation_digest=$(sdk_generation_digest "$manifest") \
    || fail 'cannot compute independent SDK generation digest'
jq --arg digest "$generation_digest" '.generation_digest = $digest' \
    "$manifest" >"$manifest.with-digest"
mv "$manifest.with-digest" "$manifest"

jq -cn \
    --arg source "$source_epoch" \
    --arg target "$target_epoch" \
    --arg staging "$staging_epoch" \
    --arg generation "$generation_id" \
    --arg lease "$lease_id" \
    --arg manifest "$manifest" \
    --arg receipt "$receipt" \
    --arg digest "$generation_digest" \
    --argjson rewrite "$rewrite" '
    {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-migration",
        epoch: 2,
        cluster_id: "mainnet-beta",
        generation_id: $generation,
        source: $source,
        source_authority_kind: "linux-kernel-read-leases",
        source_authority_id: $lease,
        source_authority_scope: "all-reviewed-source-inodes-pinned-and-read-leased-on-one-local-ext4-device",
        source_authority_filesystem: "linux-local-ext4",
        source_authority_device_id: 1,
        target: $target,
        staging: $staging,
        source_profile: "pre-unknown-instruction-fallbacks-v1",
        target_profile: "post-unknown-instruction-fallbacks-v1",
        source_profile_decision: "unique-full-generation-decode",
        source_audit_blocks: 1,
        source_audit_typed_messages: 1,
        source_audit_selected_only: 1,
        source_audit_both_equivalent: 0,
        source_audit_both_divergent: 0,
        source_audit_raw_transaction_fallbacks: 0,
        source_audit_raw_metadata_fallbacks: 0,
        zstd_level: 3,
        rewrite: $rewrite,
        copied_durable_files: [],
        omitted_edge_files: [],
        omitted_control_files: [],
        omitted_obsolete_block_files: [],
        target_post_audit_passed: true,
        target_manifest: $manifest,
        target_manifest_digest: $digest,
        migration_receipt: $receipt,
        edge_rebuild_required: false,
        staged_files_read_only: true,
        staged_directory_read_only: true,
        target_provider_immutability_required: true,
        source_provider_snapshot_required: false,
        source_linux_read_leases_required: true,
        elapsed_seconds: 1
    }
' >"$report"
: >"$log"

chmod 444 "$target_epoch"/* "$target_epoch"/.[!.]*
chmod 555 "$target_epoch"
cp "$false_program" "$fixture_root/archive-v2-pre-to-post-placeholder"
converter_sha=$(sha256_file "$fixture_root/archive-v2-pre-to-post-placeholder")
converter=$fixture_root/archive-v2-pre-to-post-$converter_sha
mv "$fixture_root/archive-v2-pre-to-post-placeholder" "$converter"
pinned_converter=$state_root/.archive-v2-pre-to-post-bin/${converter##*/}
epoch_list_snapshot=$state_root/epoch-list.snapshot
epoch_list_sha=$(sha256_file "$epoch_list")
report_bytes=$(wc -c <"$report" | tr -d '[:space:]')
report_sha=$(sha256_file "$report")
log_bytes=$(wc -c <"$log" | tr -d '[:space:]')
log_sha=$(sha256_file "$log")
manifest_bytes=$(wc -c <"$manifest" | tr -d '[:space:]')
manifest_sha=$(sha256_file "$manifest")
receipt_bytes=$(wc -c <"$receipt" | tr -d '[:space:]')
receipt_sha=$(sha256_file "$receipt")
jq -cn \
    --arg source "$source_epoch" \
    --arg target "$target_epoch" \
    --arg staging "$staging_epoch" \
    --arg generation "$generation_id" \
    --arg lease "$lease_id" \
    --arg converter_origin "$converter" \
    --arg converter_pinned "$pinned_converter" \
    --arg converter_sha "$converter_sha" \
    --arg epoch_list "$epoch_list" \
    --arg epoch_list_snapshot "$epoch_list_snapshot" \
    --arg epoch_list_sha "$epoch_list_sha" \
    --arg report "$report" \
    --argjson report_bytes "$report_bytes" \
    --arg report_sha "$report_sha" \
    --arg log "$log" \
    --argjson log_bytes "$log_bytes" \
    --arg log_sha "$log_sha" \
    --arg manifest "$manifest" \
    --argjson manifest_bytes "$manifest_bytes" \
    --arg manifest_sha "$manifest_sha" \
    --arg manifest_digest "$generation_digest" \
    --arg receipt "$receipt" \
    --argjson receipt_bytes "$receipt_bytes" \
    --arg receipt_sha "$receipt_sha" '
    {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-runner-epoch-attestation",
        run_id: "synthetic",
        epoch: 2,
        cluster_id: "mainnet-beta",
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
' >"$attestation"
chmod 400 "$report" "$log" "$attestation"
if ! command -v ionice >/dev/null 2>&1; then
    ln -s "$true_program" "$fixture_root/ionice"
fi

PATH="$fixture_root:$PATH" "$runner" \
    "$converter" \
    "$source_root" \
    "$target_root" \
    "$state_root" \
    "$epoch_list" \
    mainnet-beta \
    synthetic >/dev/null
[ -s "$state_root/complete.json" ] || fail 'valid resume did not publish complete.json'
[ -f "$pinned_converter" ] && [ ! -L "$pinned_converter" ] && [ ! -w "$pinned_converter" ] \
    || fail 'runner did not publish one private read-only converter copy'
[ "$(sha256_file "$pinned_converter")" = "$converter_sha" ] \
    || fail 'private converter copy has the wrong digest'
[ -f "$epoch_list_snapshot" ] && [ ! -L "$epoch_list_snapshot" ] \
    && [ ! -w "$epoch_list_snapshot" ] \
    || fail 'runner did not publish one private read-only epoch-list snapshot'
[ "$(sha256_file "$epoch_list_snapshot")" = "$epoch_list_sha" ] \
    || fail 'private epoch-list snapshot has the wrong digest'
complete=$state_root/complete.json
complete_backup=$fixture_root/complete.valid
cp "$complete" "$complete_backup"

# A same-size change must fail the full-hash resume check.
blocks=$target_epoch/archive-v2-blocks.zstd
chmod 644 "$blocks"
dd if=/dev/zero of="$blocks" bs=1 count=1 conv=notrunc >/dev/null 2>&1
chmod 444 "$blocks"
if PATH="$fixture_root:$PATH" "$runner" \
    "$converter" \
    "$source_root" \
    "$target_root" \
    "$state_root" \
    "$epoch_list" \
    mainnet-beta \
    synthetic >/dev/null 2>&1; then
    fail 'same-size target tamper was accepted'
fi
chmod 644 "$blocks"
cp "$true_program" "$blocks"
chmod 444 "$blocks"

# A manifest/report pair with a matching but invalid generation digest must
# fail before the runner considers its attestation.
manifest_backup=$fixture_root/manifest.valid
report_backup=$fixture_root/report.valid
cp "$manifest" "$manifest_backup"
cp "$report" "$report_backup"
bad_digest='2222222222222222222222222222222222222222222222222222222222222222'
jq --arg digest "$bad_digest" '.generation_digest = $digest' \
    "$manifest" >"$fixture_root/manifest.bad"
jq --arg digest "$bad_digest" '.target_manifest_digest = $digest' \
    "$report" >"$fixture_root/report.bad"
chmod 644 "$manifest"
cp "$fixture_root/manifest.bad" "$manifest"
chmod 444 "$manifest"
chmod 600 "$report"
cp "$fixture_root/report.bad" "$report"
chmod 400 "$report"
if PATH="$fixture_root:$PATH" "$runner" \
    "$converter" \
    "$source_root" \
    "$target_root" \
    "$state_root" \
    "$epoch_list" \
    mainnet-beta \
    synthetic >/dev/null 2>&1; then
    fail 'invalid SDK generation digest was accepted'
fi
chmod 644 "$manifest"
cp "$manifest_backup" "$manifest"
chmod 444 "$manifest"
chmod 600 "$report"
cp "$report_backup" "$report"
chmod 400 "$report"

# The public list can change, but execution is bound to the private snapshot.
# A later invocation must refuse the different public list before processing it.
printf '3\n' >"$epoch_list"
if PATH="$fixture_root:$PATH" "$runner" \
    "$converter" \
    "$source_root" \
    "$target_root" \
    "$state_root" \
    "$epoch_list" \
    mainnet-beta \
    synthetic >"$fixture_root/list-change.out" 2>&1; then
    fail 'changed epoch list was accepted after snapshot publication'
fi
grep 'differs from the private run snapshot' "$fixture_root/list-change.out" >/dev/null 2>&1 \
    || fail 'changed epoch list did not fail at the private snapshot gate'
[ ! -e "$target_root/epoch-3" ] || fail 'changed list caused an unlisted conversion'
printf '2\n' >"$epoch_list"

# Without complete.json, a different content-addressed converter must not claim
# the existing epoch. The old per-epoch attestation binds its exact converter.
rm "$complete"
cp "$true_program" "$fixture_root/archive-v2-pre-to-post-second-placeholder"
second_converter_sha=$(sha256_file "$fixture_root/archive-v2-pre-to-post-second-placeholder")
second_converter=$fixture_root/archive-v2-pre-to-post-$second_converter_sha
mv "$fixture_root/archive-v2-pre-to-post-second-placeholder" "$second_converter"
if PATH="$fixture_root:$PATH" "$runner" \
    "$second_converter" \
    "$source_root" \
    "$target_root" \
    "$state_root" \
    "$epoch_list" \
    mainnet-beta \
    synthetic >"$fixture_root/converter-switch.out" 2>&1; then
    fail 'different converter claimed an existing unattested epoch'
fi
grep 'existing runner attestation is invalid' "$fixture_root/converter-switch.out" >/dev/null 2>&1 \
    || fail 'different converter did not fail at the epoch-attestation gate'
[ ! -e "$complete" ] || fail 'different converter published a false completion record'

# An existing invalid completion path must be preserved byte-for-byte. This
# also checks that publication never uses overwriting mv semantics.
cp "$complete_backup" "$complete"
chmod 600 "$complete"
jq '.kind = "do-not-overwrite-this-sentinel"' "$complete" >"$fixture_root/complete.invalid"
cp "$fixture_root/complete.invalid" "$complete"
chmod 400 "$complete"
invalid_complete_sha=$(sha256_file "$complete")
if PATH="$fixture_root:$PATH" "$runner" \
    "$converter" \
    "$source_root" \
    "$target_root" \
    "$state_root" \
    "$epoch_list" \
    mainnet-beta \
    synthetic >/dev/null 2>&1; then
    fail 'invalid existing completion record was accepted'
fi
[ "$(sha256_file "$complete")" = "$invalid_complete_sha" ] \
    || fail 'invalid existing completion record was overwritten'

# Exercise the fresh-conversion branch. The ionice test shim stands in for the
# converter process: it publishes the already validated target fixture and
# writes the exact converter report to stdout. The runner must publish its own
# report, log, attestation, list snapshot, private converter, and completion
# record without using an overwriting rename.
chmod -R u+w "$state_root"
fresh_target_template=$fixture_root/fresh-target-template
chmod 755 "$target_epoch"
mv "$target_epoch" "$fresh_target_template"
chmod 555 "$fresh_target_template"
rm -rf "$state_root"
mkdir "$state_root"
fresh_report_payload=$fixture_root/fresh-report.json
cp "$report_backup" "$fresh_report_payload"
if [ -e "$fixture_root/ionice" ] || [ -L "$fixture_root/ionice" ]; then
    rm "$fixture_root/ionice"
fi
{
    printf '%s\n' '#!/bin/sh'
    printf 'cp -R "%s" "%s" || exit 91\n' "$fresh_target_template" "$target_epoch"
    printf 'chmod 444 "%s"/* "%s"/.[!.]* || exit 92\n' "$target_epoch" "$target_epoch"
    printf 'chmod 555 "%s" || exit 93\n' "$target_epoch"
    printf 'exec dd if="%s" bs=1048576 2>/dev/null\n' "$fresh_report_payload"
} >"$fixture_root/ionice"
chmod 500 "$fixture_root/ionice"
PATH="$fixture_root:$PATH" "$runner" \
    "$second_converter" \
    "$source_root" \
    "$target_root" \
    "$state_root" \
    "$epoch_list" \
    mainnet-beta \
    synthetic >/dev/null
[ -s "$state_root/complete.json" ] || fail 'fresh run did not publish complete.json'
[ -s "$state_root/epoch-2.attestation.json" ] \
    || fail 'fresh run did not publish its epoch attestation'
[ ! -w "$state_root/epoch-2.json" ] && [ ! -w "$state_root/epoch-2.log" ] \
    && [ ! -w "$state_root/epoch-2.attestation.json" ] \
    || fail 'fresh run state was not made read-only'
jq -e --arg sha "$second_converter_sha" \
    '.converter_sha256 == $sha' "$state_root/epoch-2.attestation.json" >/dev/null \
    || fail 'fresh epoch attestation did not bind the executed converter'

# A direct signal to the runner must terminate and reap the active converter
# before it releases the run lock.
signal_state=$fixture_root/signal-state
signal_list=$fixture_root/signal-epochs.txt
signal_pid_file=$fixture_root/signal-converter.pid
mkdir "$source_root/epoch-3"
printf '3\n' >"$signal_list"
rm "$fixture_root/ionice"
{
    printf '%s\n' '#!/bin/sh'
    printf 'printf "%%s\\n" "$$" >"%s"\n' "$signal_pid_file"
    printf '%s\n' 'exec sleep 30'
} >"$fixture_root/ionice"
chmod 500 "$fixture_root/ionice"
PATH="$fixture_root:$PATH" "$runner" \
    "$second_converter" \
    "$source_root" \
    "$target_root" \
    "$signal_state" \
    "$signal_list" \
    mainnet-beta \
    signal-test >"$fixture_root/signal-run.out" 2>&1 &
signal_runner_pid=$!
signal_ready=0
signal_wait=0
while [ "$signal_wait" -lt 10 ]; do
    if [ -s "$signal_pid_file" ]; then
        signal_ready=1
        break
    fi
    sleep 1
    signal_wait=$((signal_wait + 1))
done
[ "$signal_ready" -eq 1 ] || fail 'signal test converter did not start'
signal_converter_pid=$(sed -n '1p' "$signal_pid_file")
kill -TERM "$signal_runner_pid"
if wait "$signal_runner_pid"; then
    fail 'signaled runner exited successfully'
fi
if kill -0 "$signal_converter_pid" 2>/dev/null; then
    fail 'runner released control while its converter was still active'
fi
[ ! -e "$signal_state/.archive-v2-pre-to-post-manual.lock" ] \
    || fail 'signaled runner did not release its lock after reaping the converter'

echo 'manual Pre-to-Post runner self-test passed'
